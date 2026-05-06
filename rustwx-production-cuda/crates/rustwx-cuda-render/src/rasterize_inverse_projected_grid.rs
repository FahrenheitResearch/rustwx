//! GPU port of `rustwx_render::rasterize::rasterize_inverse_projected_grid`.
//!
//! This is the hot path for regular global lat/lon grids rendered through a
//! projected map view: one CUDA thread per output pixel, inverse-project to
//! latitude/longitude, bilinear-sample the source grid, then apply the
//! discrete meteorology colormap.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result};

use crate::colormap::ColormapHostView;
use crate::sources::with_constants;

static UPLOAD_NS: AtomicU64 = AtomicU64::new(0);
static KERNEL_NS: AtomicU64 = AtomicU64::new(0);
static DOWNLOAD_NS: AtomicU64 = AtomicU64::new(0);
static MODULE_NS: AtomicU64 = AtomicU64::new(0);
static N_CALLS: AtomicU64 = AtomicU64::new(0);

fn timing_enabled() -> bool {
    std::env::var("RUSTWX_CUDA_RASTERIZE_TIMING")
        .ok()
        .as_deref()
        == Some("1")
}

pub fn print_timing_if_enabled() {
    if !timing_enabled() {
        return;
    }
    let n = N_CALLS.load(Ordering::Relaxed).max(1) as f64;
    let to_ms = |ns: u64| ns as f64 / 1_000_000.0;
    let to_ms_per = |ns: u64| ns as f64 / 1_000_000.0 / n;
    eprintln!(
        "[rasterize_inverse_projected_grid timing - N={} calls]",
        n as u64
    );
    eprintln!(
        "  module    : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(MODULE_NS.load(Ordering::Relaxed)),
        to_ms_per(MODULE_NS.load(Ordering::Relaxed))
    );
    eprintln!(
        "  upload    : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(UPLOAD_NS.load(Ordering::Relaxed)),
        to_ms_per(UPLOAD_NS.load(Ordering::Relaxed))
    );
    eprintln!(
        "  kernel    : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(KERNEL_NS.load(Ordering::Relaxed)),
        to_ms_per(KERNEL_NS.load(Ordering::Relaxed))
    );
    eprintln!(
        "  download  : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(DOWNLOAD_NS.load(Ordering::Relaxed)),
        to_ms_per(DOWNLOAD_NS.load(Ordering::Relaxed))
    );
}

#[derive(Debug, Clone, Copy)]
pub struct RegularLatLonAxesHost {
    pub nx: i32,
    pub ny: i32,
    pub lat0: f64,
    pub lat_step: f64,
    pub lon0: f64,
    pub lon_step: f64,
    pub periodic_lon: bool,
    pub period_points: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct InverseProjectionHost {
    pub kind: i32,
    pub p0: f64,
    pub p1: f64,
    pub p2: f64,
    pub p3: f64,
    pub p4: f64,
    pub p5: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct ClipBoundsHost {
    pub has_clip: bool,
    pub west_deg: f64,
    pub east_deg: f64,
    pub south_deg: f64,
    pub north_deg: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct MapExtentHost {
    pub x_min: f64,
    pub x_max: f64,
    pub y_min: f64,
    pub y_max: f64,
}

const KERNEL_SRC: &str =
    include_str!("../../../kernels/render/rasterize_inverse_projected_grid.cu");
const MODULE_KEY: &str = "render_rasterize_inverse_projected_grid";
const FUNCTION: &str = "rasterize_inverse_projected_grid_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

#[allow(clippy::too_many_arguments)]
pub fn host(
    ctx: &ContextHandle,
    data: &[f64],
    ny: usize,
    nx: usize,
    axes: RegularLatLonAxesHost,
    projection: InverseProjectionHost,
    clip: ClipBoundsHost,
    extent: MapExtentHost,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    host_on(
        ctx,
        ctx.stream(),
        data,
        ny,
        nx,
        axes,
        projection,
        clip,
        extent,
        cmap,
        img_w,
        img_h,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    axes: RegularLatLonAxesHost,
    projection: InverseProjectionHost,
    clip: ClipBoundsHost,
    extent: MapExtentHost,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    if ny < 2 || nx < 2 || data.len() != ny * nx {
        return Ok(vec![0u8; n_pixels * 4]);
    }

    let timing = timing_enabled();

    let t_up = if timing { Some(Instant::now()) } else { None };
    let data_d = DeviceVec::from_host_on(stream, data)?;
    let levels_d = DeviceVec::from_host_on(stream, cmap.levels)?;
    let colors_d = DeviceVec::from_host_on(stream, cmap.colors_packed)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, n_pixels)?;
    if let Some(t) = t_up {
        UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let n_levels = cmap.levels.len() as i32;
    let n_intervals = cmap.colors_packed.len() as i32;
    let has_under_i: i32 = if cmap.under_color.is_some() { 1 } else { 0 };
    let has_over_i: i32 = if cmap.over_color.is_some() { 1 } else { 0 };
    let under_color: u32 = cmap.under_color.unwrap_or(0);
    let over_color: u32 = cmap.over_color.unwrap_or(0);
    let has_mask_below_i: i32 = if cmap.mask_below.is_some() { 1 } else { 0 };
    let mask_below: f64 = cmap.mask_below.unwrap_or(0.0);
    let periodic_lon_i: i32 = if axes.periodic_lon { 1 } else { 0 };
    let has_clip_i: i32 = if clip.has_clip { 1 } else { 0 };

    let t_mod = if timing { Some(Instant::now()) } else { None };
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    if let Some(t) = t_mod {
        MODULE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((img_w + bx - 1) / bx, (img_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let t_k = if timing { Some(Instant::now()) } else { None };
    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&axes.ny)
        .arg(&axes.nx)
        .arg(&axes.lat0)
        .arg(&axes.lat_step)
        .arg(&axes.lon0)
        .arg(&axes.lon_step)
        .arg(&periodic_lon_i)
        .arg(&axes.period_points)
        .arg(&projection.kind)
        .arg(&projection.p0)
        .arg(&projection.p1)
        .arg(&projection.p2)
        .arg(&projection.p3)
        .arg(&projection.p4)
        .arg(&projection.p5)
        .arg(&has_clip_i)
        .arg(&clip.west_deg)
        .arg(&clip.east_deg)
        .arg(&clip.south_deg)
        .arg(&clip.north_deg)
        .arg(&extent.x_min)
        .arg(&extent.x_max)
        .arg(&extent.y_min)
        .arg(&extent.y_max)
        .arg(levels_d.slice())
        .arg(&n_levels)
        .arg(colors_d.slice())
        .arg(&n_intervals)
        .arg(&has_under_i)
        .arg(&under_color)
        .arg(&has_over_i)
        .arg(&over_color)
        .arg(&has_mask_below_i)
        .arg(&mask_below)
        .arg(out_d.slice_mut())
        .arg(&img_w_i)
        .arg(&img_h_i);
    unsafe { builder.launch(cfg)? };
    if let Some(t) = t_k {
        KERNEL_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let t_d = if timing { Some(Instant::now()) } else { None };
    let pixels_u32 = out_d.copy_to_host_on(stream)?;
    if let Some(t) = t_d {
        DOWNLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    if timing {
        N_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    Ok(u32_vec_to_rgba_bytes(pixels_u32))
}

fn u32_vec_to_rgba_bytes(mut v: Vec<u32>) -> Vec<u8> {
    let len_bytes = v.len() * 4;
    let cap_bytes = v.capacity() * 4;
    let ptr = v.as_mut_ptr() as *mut u8;
    std::mem::forget(v);
    unsafe { Vec::from_raw_parts(ptr, len_bytes, cap_bytes) }
}

/// Same as `host_on` but writes to a freshly-allocated device buffer
/// instead of returning host bytes — saves one PCIe download per call.
/// Used by the canvas-resident pipeline where the data layer is
/// composited GPU-to-GPU into the canvas immediately afterwards.
///
/// The returned `DeviceVec<u32>` has length `img_w * img_h` of packed RGBA
/// pixels (same byte layout as the existing `host_on` output).
#[allow(clippy::too_many_arguments)]
pub fn host_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    axes: RegularLatLonAxesHost,
    projection: InverseProjectionHost,
    clip: ClipBoundsHost,
    extent: MapExtentHost,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<DeviceVec<u32>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    if ny < 2 || nx < 2 || data.len() != ny * nx {
        return DeviceVec::zeros_on(stream, n_pixels);
    }

    let timing = timing_enabled();

    let t_up = if timing { Some(Instant::now()) } else { None };
    let data_d = DeviceVec::from_host_on(stream, data)?;
    let levels_d = DeviceVec::from_host_on(stream, cmap.levels)?;
    let colors_d = DeviceVec::from_host_on(stream, cmap.colors_packed)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, n_pixels)?;
    if let Some(t) = t_up {
        UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let n_levels = cmap.levels.len() as i32;
    let n_intervals = cmap.colors_packed.len() as i32;
    let has_under_i: i32 = if cmap.under_color.is_some() { 1 } else { 0 };
    let has_over_i: i32 = if cmap.over_color.is_some() { 1 } else { 0 };
    let under_color: u32 = cmap.under_color.unwrap_or(0);
    let over_color: u32 = cmap.over_color.unwrap_or(0);
    let has_mask_below_i: i32 = if cmap.mask_below.is_some() { 1 } else { 0 };
    let mask_below: f64 = cmap.mask_below.unwrap_or(0.0);
    let periodic_lon_i: i32 = if axes.periodic_lon { 1 } else { 0 };
    let has_clip_i: i32 = if clip.has_clip { 1 } else { 0 };

    let t_mod = if timing { Some(Instant::now()) } else { None };
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    if let Some(t) = t_mod {
        MODULE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((img_w + bx - 1) / bx, (img_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let t_k = if timing { Some(Instant::now()) } else { None };
    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&axes.ny)
        .arg(&axes.nx)
        .arg(&axes.lat0)
        .arg(&axes.lat_step)
        .arg(&axes.lon0)
        .arg(&axes.lon_step)
        .arg(&periodic_lon_i)
        .arg(&axes.period_points)
        .arg(&projection.kind)
        .arg(&projection.p0)
        .arg(&projection.p1)
        .arg(&projection.p2)
        .arg(&projection.p3)
        .arg(&projection.p4)
        .arg(&projection.p5)
        .arg(&has_clip_i)
        .arg(&clip.west_deg)
        .arg(&clip.east_deg)
        .arg(&clip.south_deg)
        .arg(&clip.north_deg)
        .arg(&extent.x_min)
        .arg(&extent.x_max)
        .arg(&extent.y_min)
        .arg(&extent.y_max)
        .arg(levels_d.slice())
        .arg(&n_levels)
        .arg(colors_d.slice())
        .arg(&n_intervals)
        .arg(&has_under_i)
        .arg(&under_color)
        .arg(&has_over_i)
        .arg(&over_color)
        .arg(&has_mask_below_i)
        .arg(&mask_below)
        .arg(out_d.slice_mut())
        .arg(&img_w_i)
        .arg(&img_h_i);
    unsafe { builder.launch(cfg)? };
    if let Some(t) = t_k {
        KERNEL_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    if timing {
        N_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    Ok(out_d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colormap::pack_rgba;

    // ---------- inlined CPU reference (port of upstream
    // `rustwx_render::rasterize::rasterize_inverse_projected_grid` for
    // the Geographic projection branch only) ----------

    fn bilinear_ref(v00: f64, v10: f64, v01: f64, v11: f64, fx: f64, fy: f64) -> f64 {
        if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
            let south = v00 * (1.0 - fx) + v10 * fx;
            let north = v01 * (1.0 - fx) + v11 * fx;
            return south * (1.0 - fy) + north * fy;
        }
        for v in [v00, v10, v01, v11] {
            if v.is_finite() {
                return v;
            }
        }
        f64::NAN
    }

    fn colormap_lookup_ref(
        value: f64,
        levels: &[f64],
        colors: &[u32],
        under_color: Option<u32>,
        over_color: Option<u32>,
        mask_below: Option<f64>,
    ) -> u32 {
        if value.is_nan() {
            return 0;
        }
        if let Some(mb) = mask_below {
            if value < mb {
                return 0;
            }
        }
        if levels.len() < 2 || colors.is_empty() {
            return 0;
        }
        if value < levels[0] {
            return under_color.unwrap_or(0);
        }
        let n_intervals = levels.len() - 1;
        let idx = levels.partition_point(|l| *l <= value);
        if idx <= n_intervals {
            let ci = idx.saturating_sub(1).min(colors.len() - 1);
            return colors[ci];
        }
        over_color.unwrap_or(colors[colors.len() - 1])
    }

    fn normalize_projection_lon(mut lon: f64) -> f64 {
        lon = lon % 360.0;
        if lon > 180.0 {
            lon -= 360.0;
        } else if lon <= -180.0 {
            lon += 360.0;
        }
        lon
    }

    fn normalize_grid_lon(mut lon: f64) -> f64 {
        while lon < -180.0 {
            lon += 360.0;
        }
        while lon >= 180.0 {
            lon -= 360.0;
        }
        lon
    }

    fn rem_euclid(value: f64, modulus: f64) -> f64 {
        let r = value % modulus;
        if r < 0.0 {
            r + modulus
        } else {
            r
        }
    }

    /// Geographic-projection unproject: identity in lat, shift+normalize in lon.
    fn unproject_geographic(central_meridian_deg: f64, x: f64, y: f64) -> Option<(f64, f64)> {
        if !x.is_finite() || !y.is_finite() {
            return None;
        }
        let lat = y.clamp(-89.999, 89.999);
        let lon = normalize_projection_lon(x + central_meridian_deg);
        Some((lat, lon))
    }

    fn grid_x_for_axis_lon(lon: f64, axes: &RegularLatLonAxesHost) -> Option<f64> {
        if axes.periodic_lon {
            return Some(rem_euclid(
                (lon - axes.lon0) / axes.lon_step,
                axes.period_points,
            ));
        }
        let mut adjusted = normalize_grid_lon(lon);
        let axis_center = axes.lon0 + axes.lon_step * (axes.nx as f64 - 1.0) / 2.0;
        while adjusted - axis_center > 180.0 {
            adjusted -= 360.0;
        }
        while adjusted - axis_center < -180.0 {
            adjusted += 360.0;
        }
        let gx = (adjusted - axes.lon0) / axes.lon_step;
        if gx >= 0.0 && gx <= (axes.nx as f64 - 1.0) {
            Some(gx)
        } else {
            None
        }
    }

    fn sample_regular_latlon_grid_ref(
        data: &[f64],
        axes: &RegularLatLonAxesHost,
        lat: f64,
        lon: f64,
    ) -> Option<f64> {
        if !lat.is_finite() || !lon.is_finite() {
            return None;
        }
        let gy = (lat - axes.lat0) / axes.lat_step;
        if gy < 0.0 || gy > (axes.ny as f64 - 1.0) {
            return None;
        }
        let gx = grid_x_for_axis_lon(lon, axes)?;

        let nx = axes.nx as usize;
        let ny = axes.ny as usize;
        let i0 = (gx.floor() as usize).min(nx - 1);
        let j0 = (gy.floor() as usize).min(ny - 1);
        let i1 = if axes.periodic_lon {
            (rem_euclid((i0 + 1) as f64, axes.period_points)) as usize
        } else {
            (i0 + 1).min(nx - 1)
        };
        let j1 = (j0 + 1).min(ny - 1);
        let fx = gx - i0 as f64;
        let fy = gy - j0 as f64;
        let idx = |j: usize, i: usize| j * nx + i;
        Some(bilinear_ref(
            data[idx(j0, i0)],
            data[idx(j0, i1)],
            data[idx(j1, i0)],
            data[idx(j1, i1)],
            fx,
            fy,
        ))
    }

    fn cpu_rasterize_inverse_projected_geographic_ref(
        data: &[f64],
        axes: &RegularLatLonAxesHost,
        central_meridian_deg: f64,
        clip: &ClipBoundsHost,
        extent: &MapExtentHost,
        levels: &[f64],
        colors: &[u32],
        under_color: Option<u32>,
        over_color: Option<u32>,
        mask_below: Option<f64>,
        img_w: u32,
        img_h: u32,
    ) -> Vec<u8> {
        let mut out = vec![0u8; (img_w as usize) * (img_h as usize) * 4];
        let x_den = img_w.saturating_sub(1).max(1) as f64;
        let y_den = img_h.saturating_sub(1).max(1) as f64;
        for py in 0..img_h {
            let y = extent.y_max - (py as f64 / y_den) * (extent.y_max - extent.y_min);
            for px in 0..img_w {
                let x = extent.x_min + (px as f64 / x_den) * (extent.x_max - extent.x_min);
                let Some((lat, lon)) = unproject_geographic(central_meridian_deg, x, y) else {
                    continue;
                };
                if clip.has_clip {
                    // mirror kernel inv_clip_contains for the simple case
                    if !lat.is_finite() || !lon.is_finite() {
                        continue;
                    }
                    if lat < clip.south_deg || lat > clip.north_deg {
                        continue;
                    }
                    let raw_span = (clip.east_deg - clip.west_deg).abs();
                    let span = if raw_span >= 359.0 {
                        raw_span.min(360.0)
                    } else {
                        let w = normalize_projection_lon(clip.west_deg);
                        let e = normalize_projection_lon(clip.east_deg);
                        if w <= e {
                            e - w
                        } else {
                            e + 360.0 - w
                        }
                    };
                    if span < 359.0 {
                        let w = normalize_projection_lon(clip.west_deg);
                        let e = normalize_projection_lon(clip.east_deg);
                        let l = normalize_projection_lon(lon);
                        let inside = if w <= e {
                            l >= w && l <= e
                        } else {
                            l >= w || l <= e
                        };
                        if !inside {
                            continue;
                        }
                    }
                }
                let Some(value) = sample_regular_latlon_grid_ref(data, axes, lat, lon) else {
                    continue;
                };
                let packed =
                    colormap_lookup_ref(value, levels, colors, under_color, over_color, mask_below);
                if ((packed >> 24) & 0xFF) == 0 {
                    continue;
                }
                let off = (py as usize * img_w as usize + px as usize) * 4;
                out[off] = packed as u8;
                out[off + 1] = (packed >> 8) as u8;
                out[off + 2] = (packed >> 16) as u8;
                out[off + 3] = (packed >> 24) as u8;
            }
        }
        out
    }

    // ---------- synthetic inputs ----------

    fn build_synthetic_latlon_field(ny: usize, nx: usize) -> Vec<f64> {
        let mut v = vec![0.0; ny * nx];
        for j in 0..ny {
            for i in 0..nx {
                let u = i as f64 / (nx as f64).max(1.0);
                let w = j as f64 / (ny as f64).max(1.0);
                v[j * nx + i] =
                    (u * 6.28).sin() * 50.0 + (w * 4.71).cos() * 30.0 + (u * w * 12.0).sin() * 20.0;
            }
        }
        // sprinkle NaNs across the grid to exercise the masked path
        v[0] = f64::NAN;
        v[7 * nx + 13] = f64::NAN;
        v[20 * nx + 41] = f64::NAN;
        v
    }

    fn build_synthetic_cmap() -> (Vec<f64>, Vec<u32>, Option<u32>, Option<u32>) {
        let levels: Vec<f64> = vec![
            -100.0, -75.0, -50.0, -30.0, -15.0, -5.0, 0.0, 5.0, 15.0, 30.0, 50.0, 75.0, 100.0,
        ];
        let palette: Vec<u32> = (0..(levels.len() - 1))
            .map(|k| {
                let t = k as f64 / ((levels.len() - 2) as f64);
                let r = (255.0 * t) as u8;
                let g = (255.0 * (1.0 - (2.0 * t - 1.0).abs())) as u8;
                let b = (255.0 * (1.0 - t)) as u8;
                pack_rgba(r, g, b, 255)
            })
            .collect();
        (
            levels,
            palette,
            Some(pack_rgba(10, 10, 10, 255)),
            Some(pack_rgba(255, 255, 255, 255)),
        )
    }

    /// Geographic projection (kind=0): GPU output must match the CPU
    /// reference within a per-channel delta of 1 LSB. With identical f64
    /// math on both sides we expect 0, but allow 1 to absorb any
    /// rounding-mode differences in the colormap quantization.
    #[test]
    fn inverse_projected_geographic_matches_cpu() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e}) — test requires GPU");
                return;
            }
        };

        let nx: usize = 60;
        let ny: usize = 40;
        let img_w: u32 = 360;
        let img_h: u32 = 180;

        let data = build_synthetic_latlon_field(ny, nx);
        let (levels, colors, under, over) = build_synthetic_cmap();

        // Periodic global grid: nx * lon_step == 360, no duplicate endpoint.
        let axes = RegularLatLonAxesHost {
            nx: nx as i32,
            ny: ny as i32,
            lat0: -90.0,
            lat_step: 180.0 / (ny as f64 - 1.0),
            lon0: -180.0,
            lon_step: 360.0 / nx as f64,
            periodic_lon: true,
            period_points: nx as f64,
        };
        let projection = InverseProjectionHost {
            kind: 0,
            p0: 0.0,
            p1: 0.0,
            p2: 0.0,
            p3: 0.0,
            p4: 0.0,
            p5: 0.0,
        };
        let clip = ClipBoundsHost {
            has_clip: false,
            west_deg: 0.0,
            east_deg: 0.0,
            south_deg: 0.0,
            north_deg: 0.0,
        };
        let extent = MapExtentHost {
            x_min: -180.0,
            x_max: 180.0,
            y_min: -90.0,
            y_max: 90.0,
        };
        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };

        let cpu = cpu_rasterize_inverse_projected_geographic_ref(
            &data,
            &axes,
            projection.p0,
            &clip,
            &extent,
            &levels,
            &colors,
            under,
            over,
            None,
            img_w,
            img_h,
        );
        let gpu = host(
            &ctx, &data, ny, nx, axes, projection, clip, extent, view, img_w, img_h,
        )
        .expect("CUDA rasterize_inverse_projected_grid host call failed");

        assert_eq!(cpu.len(), gpu.len(), "buffer length mismatch");

        let n_pix = (img_w as usize) * (img_h as usize);
        let mut max_delta: i32 = 0;
        let mut diff_pixels: usize = 0;
        let mut first_diff: Option<(usize, [u8; 4], [u8; 4])> = None;
        for p in 0..n_pix {
            let off = p * 4;
            let mut pixel_delta: i32 = 0;
            for k in 0..4 {
                let d = (cpu[off + k] as i32 - gpu[off + k] as i32).abs();
                pixel_delta = pixel_delta.max(d);
            }
            if pixel_delta > max_delta {
                max_delta = pixel_delta;
            }
            if pixel_delta > 0 {
                diff_pixels += 1;
                if first_diff.is_none() {
                    first_diff = Some((
                        p,
                        [cpu[off], cpu[off + 1], cpu[off + 2], cpu[off + 3]],
                        [gpu[off], gpu[off + 1], gpu[off + 2], gpu[off + 3]],
                    ));
                }
            }
        }

        eprintln!(
            "[inverse_projected_geographic] max_delta={} diff_pixels={}/{}",
            max_delta, diff_pixels, n_pix
        );
        if let Some((idx, c, g)) = first_diff {
            let py = idx / img_w as usize;
            let px = idx % img_w as usize;
            eprintln!("  first diff @ ({},{}) cpu={:?} gpu={:?}", px, py, c, g);
        }
        assert!(
            max_delta <= 1,
            "max channel delta {} > 1 between CPU and GPU geographic inverse-projection",
            max_delta
        );
    }

    /// Mercator (kind=4) sanity test: same approach but with the kernel's
    /// Mercator branch. The CPU reference inlines only the unproject math;
    /// everything else (clip / sample / colormap) is shared with the
    /// geographic test.
    #[test]
    fn inverse_projected_mercator_matches_cpu() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e}) — test requires GPU");
                return;
            }
        };

        const R_EARTH: f64 = 6370000.0;
        const RAD2DEG: f64 = 180.0 / std::f64::consts::PI;

        let nx: usize = 60;
        let ny: usize = 40;
        let img_w: u32 = 360;
        let img_h: u32 = 180;

        let data = build_synthetic_latlon_field(ny, nx);
        let (levels, colors, under, over) = build_synthetic_cmap();

        let axes = RegularLatLonAxesHost {
            nx: nx as i32,
            ny: ny as i32,
            lat0: -90.0,
            lat_step: 180.0 / (ny as f64 - 1.0),
            lon0: -180.0,
            lon_step: 360.0 / nx as f64,
            periodic_lon: true,
            period_points: nx as f64,
        };
        let central_meridian = 0.0_f64;
        let scale = 1.0_f64;
        let projection = InverseProjectionHost {
            kind: 4,
            p0: central_meridian,
            p1: scale,
            p2: 0.0,
            p3: 0.0,
            p4: 0.0,
            p5: 0.0,
        };
        let clip = ClipBoundsHost {
            has_clip: false,
            west_deg: 0.0,
            east_deg: 0.0,
            south_deg: 0.0,
            north_deg: 0.0,
        };
        // Mercator extent: world bounds at lat ~ ±~85°.
        // x range = ±π·R, y range matched at ±lat_max.
        let lat_max_deg: f64 = 80.0;
        let lat_max_rad = lat_max_deg.to_radians();
        let y_max = R_EARTH * scale * (lat_max_rad / 2.0 + std::f64::consts::FRAC_PI_4).tan().ln();
        let x_max = std::f64::consts::PI * R_EARTH * scale;
        let extent = MapExtentHost {
            x_min: -x_max,
            x_max,
            y_min: -y_max,
            y_max,
        };
        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };

        // CPU reference using Mercator unproject.
        let mut cpu = vec![0u8; (img_w as usize) * (img_h as usize) * 4];
        let x_den = img_w.saturating_sub(1).max(1) as f64;
        let y_den = img_h.saturating_sub(1).max(1) as f64;
        for py in 0..img_h {
            let y = extent.y_max - (py as f64 / y_den) * (extent.y_max - extent.y_min);
            for px in 0..img_w {
                let x = extent.x_min + (px as f64 / x_den) * (extent.x_max - extent.x_min);
                if !x.is_finite() || !y.is_finite() {
                    continue;
                }
                let lon_raw = central_meridian + x / (R_EARTH * scale) * RAD2DEG;
                let lat_raw = (2.0 * (y / (R_EARTH * scale)).exp().atan()
                    - std::f64::consts::FRAC_PI_2)
                    * RAD2DEG;
                let lat = lat_raw.clamp(-89.999, 89.999);
                let lon = normalize_projection_lon(lon_raw);
                let Some(value) = sample_regular_latlon_grid_ref(&data, &axes, lat, lon) else {
                    continue;
                };
                let packed = colormap_lookup_ref(value, &levels, &colors, under, over, None);
                if ((packed >> 24) & 0xFF) == 0 {
                    continue;
                }
                let off = (py as usize * img_w as usize + px as usize) * 4;
                cpu[off] = packed as u8;
                cpu[off + 1] = (packed >> 8) as u8;
                cpu[off + 2] = (packed >> 16) as u8;
                cpu[off + 3] = (packed >> 24) as u8;
            }
        }

        let gpu = host(
            &ctx, &data, ny, nx, axes, projection, clip, extent, view, img_w, img_h,
        )
        .expect("CUDA rasterize_inverse_projected_grid host call (Mercator) failed");

        assert_eq!(cpu.len(), gpu.len(), "buffer length mismatch (mercator)");

        let n_pix = (img_w as usize) * (img_h as usize);
        let mut max_delta: i32 = 0;
        let mut diff_pixels: usize = 0;
        for p in 0..n_pix {
            let off = p * 4;
            let mut pixel_delta: i32 = 0;
            for k in 0..4 {
                let d = (cpu[off + k] as i32 - gpu[off + k] as i32).abs();
                pixel_delta = pixel_delta.max(d);
            }
            if pixel_delta > max_delta {
                max_delta = pixel_delta;
            }
            if pixel_delta > 0 {
                diff_pixels += 1;
            }
        }
        eprintln!(
            "[inverse_projected_mercator] max_delta={} diff_pixels={}/{}",
            max_delta, diff_pixels, n_pix
        );
        assert!(
            max_delta <= 1,
            "max channel delta {} > 1 between CPU and GPU Mercator inverse-projection",
            max_delta
        );
    }
}
