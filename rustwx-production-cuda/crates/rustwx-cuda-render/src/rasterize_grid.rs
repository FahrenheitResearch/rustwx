//! GPU port of `rustwx_render::rasterize::rasterize_grid` — per-pixel
//! bilinear sample of an `[ny][nx]` `f64` grid + colormap lookup, returning
//! a packed-RGBA8 byte buffer ready for `image::RgbaImage::from_raw`.

use std::sync::Arc;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result,
};

use crate::colormap::ColormapHostView;
use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/render/rasterize_grid.cu");
const MODULE_KEY: &str = "render_rasterize_grid";
const FUNCTION: &str = "rasterize_grid_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Rasterize `data` (`[ny][nx]` row-major, south-up) into a packed-RGBA8
/// buffer of length `4 * img_w * img_h`. Byte order matches `image::Rgba<u8>`.
///
/// Uses the context's default stream — calls from multiple CPU threads
/// will serialize on the GPU. Use `host_on()` with a per-thread stream
/// for concurrent execution.
pub fn host(
    ctx: &ContextHandle,
    data: &[f64],
    ny: usize,
    nx: usize,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    host_on(ctx, ctx.stream(), data, ny, nx, cmap, img_w, img_h)
}

/// Same as `host()` but routes all device ops through the caller-supplied
/// stream. Run multiple non-default streams concurrently from different CPU
/// threads to actually overlap work on the GPU.
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    // CPU returns an empty image for ny==0 or nx==0 — match by short-circuiting
    // to a transparent buffer without launching the kernel.
    if ny == 0 || nx == 0 || data.is_empty() {
        return Ok(vec![0u8; n_pixels * 4]);
    }

    debug_assert_eq!(data.len(), ny * nx, "data length must equal ny * nx");

    // Upload field + colormap. Both are small relative to the launch and a
    // single render call is the unit of work — staying simple beats reuse.
    let data_d = DeviceVec::from_host_on(stream, data)?;
    let levels_d = DeviceVec::from_host_on(stream, cmap.levels)?;
    let colors_d = DeviceVec::from_host_on(stream, cmap.colors_packed)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, n_pixels)?;

    let n_levels = cmap.levels.len() as i32;
    let n_intervals = cmap.colors_packed.len() as i32;
    let has_under_i: i32 = if cmap.under_color.is_some() { 1 } else { 0 };
    let has_over_i: i32 = if cmap.over_color.is_some() { 1 } else { 0 };
    let under_color: u32 = cmap.under_color.unwrap_or(0);
    let over_color: u32 = cmap.over_color.unwrap_or(0);
    let has_mask_below_i: i32 = if cmap.mask_below.is_some() { 1 } else { 0 };
    let mask_below: f64 = cmap.mask_below.unwrap_or(0.0);

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    // 16x16 = 256 threads/block — typical for 2D image kernels on Ada/Blackwell.
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((img_w + bx - 1) / bx, (img_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
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

    // Copy back as Vec<u32>, reinterpret as Vec<u8>. RgbaImage::from_raw
    // wants a Vec<u8> of length 4 * w * h.
    let pixels_u32 = out_d.copy_to_host_on(stream)?;
    Ok(u32_vec_to_rgba_bytes(pixels_u32))
}

/// Same as `host_on` but writes to a freshly-allocated device buffer
/// instead of returning host bytes — saves one PCIe download per call.
/// Used by the canvas-resident pipeline.
pub fn host_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<DeviceVec<u32>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    if ny == 0 || nx == 0 || data.is_empty() {
        return DeviceVec::zeros_on(stream, n_pixels);
    }

    debug_assert_eq!(data.len(), ny * nx, "data length must equal ny * nx");

    let data_d = DeviceVec::from_host_on(stream, data)?;
    let levels_d = DeviceVec::from_host_on(stream, cmap.levels)?;
    let colors_d = DeviceVec::from_host_on(stream, cmap.colors_packed)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, n_pixels)?;

    let n_levels = cmap.levels.len() as i32;
    let n_intervals = cmap.colors_packed.len() as i32;
    let has_under_i: i32 = if cmap.under_color.is_some() { 1 } else { 0 };
    let has_over_i: i32 = if cmap.over_color.is_some() { 1 } else { 0 };
    let under_color: u32 = cmap.under_color.unwrap_or(0);
    let over_color: u32 = cmap.over_color.unwrap_or(0);
    let has_mask_below_i: i32 = if cmap.mask_below.is_some() { 1 } else { 0 };
    let mask_below: f64 = cmap.mask_below.unwrap_or(0.0);

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((img_w + bx - 1) / bx, (img_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
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

    Ok(out_d)
}

/// Reinterpret a `Vec<u32>` of packed-RGBA pixels as the `Vec<u8>` byte buffer
/// expected by `image::RgbaImage::from_raw`.
///
/// The packing is `u32 = R | (G << 8) | (B << 16) | (A << 24)`, which on a
/// little-endian host matches `[R, G, B, A]` byte order in memory.
fn u32_vec_to_rgba_bytes(mut v: Vec<u32>) -> Vec<u8> {
    let len_bytes = v.len() * 4;
    let cap_bytes = v.capacity() * 4;
    let ptr = v.as_mut_ptr() as *mut u8;
    std::mem::forget(v);
    unsafe { Vec::from_raw_parts(ptr, len_bytes, cap_bytes) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colormap::pack_rgba;

    /// Inlined reference of `rustwx_render::rasterize::rasterize_grid` —
    /// kept literal so the test fails loudly if upstream drifts. Output is
    /// the same `[R,G,B,A]` byte layout as `RgbaImage::from_raw`.
    fn cpu_rasterize_grid_ref(
        data: &[f64],
        ny: usize,
        nx: usize,
        levels: &[f64],
        colors_packed: &[u32],
        under_color: Option<u32>,
        over_color: Option<u32>,
        mask_below: Option<f64>,
        img_w: u32,
        img_h: u32,
    ) -> Vec<u8> {
        let mut out = vec![0u8; (img_w as usize) * (img_h as usize) * 4];
        if ny == 0 || nx == 0 {
            return out;
        }
        let x_den = img_w.saturating_sub(1).max(1) as f64;
        let y_den = img_h.saturating_sub(1).max(1) as f64;
        let gx_den = nx.saturating_sub(1).max(1) as f64;
        let gy_den = ny.saturating_sub(1).max(1) as f64;

        for py in 0..img_h {
            for px in 0..img_w {
                let gx = px as f64 / x_den * gx_den;
                let gy = (img_h.saturating_sub(1) - py) as f64 / y_den * gy_den;

                let i0 = gx.floor() as usize;
                let j0 = gy.floor() as usize;
                let i1 = (i0 + 1).min(nx - 1);
                let j1 = (j0 + 1).min(ny - 1);
                let fx = gx - i0 as f64;
                let fy = gy - j0 as f64;

                let v00 = data[j0 * nx + i0];
                let v10 = data[j0 * nx + i1];
                let v01 = data[j1 * nx + i0];
                let v11 = data[j1 * nx + i1];

                let value = bilinear_ref(v00, v10, v01, v11, fx, fy);
                let packed = colormap_lookup_ref(
                    value, levels, colors_packed,
                    under_color, over_color, mask_below,
                );
                let off = (py as usize * img_w as usize + px as usize) * 4;
                out[off    ] = (packed       ) as u8;
                out[off + 1] = (packed >>  8 ) as u8;
                out[off + 2] = (packed >> 16 ) as u8;
                out[off + 3] = (packed >> 24 ) as u8;
            }
        }
        out
    }

    fn bilinear_ref(v00: f64, v10: f64, v01: f64, v11: f64, fx: f64, fy: f64) -> f64 {
        if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
            let south = v00 * (1.0 - fx) + v10 * fx;
            let north = v01 * (1.0 - fx) + v11 * fx;
            south * (1.0 - fy) + north * fy
        } else {
            for v in [v00, v10, v01, v11] {
                if v.is_finite() { return v; }
            }
            f64::NAN
        }
    }

    fn colormap_lookup_ref(
        value: f64,
        levels: &[f64],
        colors: &[u32],
        under_color: Option<u32>,
        over_color: Option<u32>,
        mask_below: Option<f64>,
    ) -> u32 {
        if value.is_nan() { return 0; }
        if let Some(mb) = mask_below { if value < mb { return 0; } }
        if levels.is_empty() || colors.is_empty() { return 0; }
        if value < levels[0] { return under_color.unwrap_or(0); }
        let n_intervals = levels.len() - 1;
        let idx = levels.partition_point(|l| *l <= value);
        if idx <= n_intervals {
            return colors[idx.saturating_sub(1).min(colors.len() - 1)];
        }
        if value >= levels[n_intervals] {
            return over_color.unwrap_or(colors[colors.len() - 1]);
        }
        colors[colors.len() - 1]
    }

    fn build_synthetic_field(ny: usize, nx: usize) -> Vec<f64> {
        let mut v = vec![0.0; ny * nx];
        for j in 0..ny {
            for i in 0..nx {
                let x = i as f64 / (nx as f64).max(1.0);
                let y = j as f64 / (ny as f64).max(1.0);
                v[j * nx + i] = (x * 6.28).sin() * 50.0
                    + (y * 4.71).cos() * 30.0
                    + (x * y * 12.0).sin() * 20.0;
            }
        }
        // sprinkle a few NaNs to exercise the masked path
        v[0] = f64::NAN;
        if v.len() > 1234 { v[1234] = f64::NAN; }
        v
    }

    fn build_synthetic_cmap() -> (Vec<f64>, Vec<u32>, Option<u32>, Option<u32>) {
        // Asymmetric levels (irregular spacing) to exercise the binary search.
        let levels: Vec<f64> = vec![
            -100.0, -75.0, -50.0, -30.0, -15.0, -5.0,
              0.0,   5.0,  15.0,  30.0,  50.0,  75.0, 100.0,
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
            Some(pack_rgba(10, 10, 10, 255)),    // under = dark grey
            Some(pack_rgba(255, 255, 255, 255)), // over  = white
        )
    }

    /// End-to-end byte-for-byte verification: GPU output must equal the
    /// inlined CPU reference for a non-trivial synthetic field + irregular
    /// colormap with under/over/NaN paths exercised.
    #[test]
    fn rasterize_grid_matches_cpu_byte_for_byte() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e}) — test requires GPU");
                return;
            }
        };

        // Modest size — runs fast in tests but still spans many warps.
        let ny = 207;
        let nx = 311;
        let img_w: u32 = 640;
        let img_h: u32 = 480;

        let data = build_synthetic_field(ny, nx);
        let (levels, colors, under, over) = build_synthetic_cmap();

        let cpu = cpu_rasterize_grid_ref(
            &data, ny, nx, &levels, &colors, under, over, None, img_w, img_h,
        );

        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };
        let gpu = host(&ctx, &data, ny, nx, view, img_w, img_h)
            .expect("CUDA rasterize_grid host call failed");

        assert_eq!(cpu.len(), gpu.len(), "buffer length mismatch");

        // Count any divergent pixels for a useful diagnostic.
        let mut diff_pixels = 0usize;
        for p in 0..(img_w as usize * img_h as usize) {
            let off = p * 4;
            if cpu[off..off + 4] != gpu[off..off + 4] {
                diff_pixels += 1;
            }
        }
        assert_eq!(
            diff_pixels, 0,
            "{} of {} pixels diverged between CPU and GPU rasterize_grid",
            diff_pixels,
            img_w as usize * img_h as usize
        );
    }
}
