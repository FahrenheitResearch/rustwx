//! GPU-accelerated inverse-projected raster of a regular lat/lon data grid.
//!
//! Counterpart to `rustwx_render::rasterize::rasterize_inverse_projected_grid`
//! — the hot path for global-domain renders (GFS / GEFS / Euro on a regular
//! lat/lon axis, painted into a projected canvas like Robinson, Lambert,
//! Mercator, Polar Stereographic).
//!
//! One CUDA thread per output pixel:
//!   1) pixel -> projected (x, y)
//!   2) unproject (x, y) -> (lat, lon)
//!   3) (optional) geographic clip-bounds reject
//!   4) bilinear sample of the regular lat/lon grid at (lat, lon)
//!   5) colormap lookup (binary search on levels)
//!   6) packed-RGBA8 store
//!
//! All math is `f64` to match the CPU reference within a handful of ULPs.
//!
//! ## Supported projections
//!
//! | `ProjectionVariant` | Inverse | Notes |
//! |---|---|---|
//! | `Geographic`        | analytic | trivial: (lat, lon) = (y, x + central_meridian) |
//! | `Mercator`          | analytic |          |
//! | `LambertConformal`  | analytic |          |
//! | `Robinson`          | piecewise table   | matches CPU table inversion |
//! | `AlbersEqualArea`   | analytic |          |
//! | `PolarStereographic`| **unsupported** — host returns `Err`, caller falls back to CPU |
//!
//! The wrapper returns `Err(...)` for `PolarStereographic` rather than silently
//! producing wrong pixels — the integrator's swap site is expected to catch
//! the error and fall through to the CPU path.

use std::sync::Arc;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, Error, KernelModule, LaunchCfg, Result};

use crate::colormap::ColormapHostView;
use crate::sources::with_constants;

/// Wrap a string in `Error` via the `Io` variant — `rustwx_cuda_core::Error`
/// has no general-purpose string constructor, so we route surface errors
/// (unsupported projection, dimension mismatch sentinels) through a
/// `std::io::Error` carrier to keep the public `Result` type stable.
fn err_other(msg: impl Into<String>) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.into(),
    ))
}

const KERNEL_SRC: &str = include_str!(
    "../../../kernels/render/rasterize_inverse_projected_regular_latlon.cu"
);
const MODULE_KEY: &str = "render_rasterize_inverse_projected_regular_latlon";
const FUNCTION: &str = "rasterize_inverse_projected_regular_latlon_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Kernel-side `kind` codes used by `inv_unproject`. Mirrors the i32 values
/// baked into `rasterize_inverse_projected_regular_latlon.cu`.
const KIND_GEOGRAPHIC: i32 = 0;
const KIND_ROBINSON: i32 = 1;
const KIND_ALBERS_EQUAL_AREA: i32 = 2;
const KIND_LAMBERT_CONFORMAL: i32 = 3;
const KIND_MERCATOR: i32 = 4;

// -- Local mirrors of the upstream rustwx-render types ----------------------
//
// We deliberately don't pull `rustwx-render` in as a dep — the GPU crate must
// stay leaf-only. These structs carry just enough of the upstream shape for
// the kernel to consume.

/// Regular lat/lon axis description for the source grid. Mirrors
/// `rustwx_render::rasterize::RegularLatLonAxes`.
#[derive(Debug, Clone, Copy)]
pub struct RegularLatLonAxes {
    pub nx: usize,
    pub ny: usize,
    /// Latitude of grid row 0 (degrees). Row j is at `lat0 + lat_step * j`.
    pub lat0: f64,
    /// Per-row latitude step (degrees). Sign matches the source data.
    pub lat_step: f64,
    /// Longitude of grid column 0 (degrees).
    pub lon0: f64,
    /// Per-column longitude step (degrees).
    pub lon_step: f64,
    /// True when the longitude axis wraps around the globe (e.g. GFS 0.25°
    /// covers the full 360° in either `nx * |lon_step| ≈ 360` or
    /// `(nx-1) * |lon_step| ≈ 360` form).
    pub periodic_lon: bool,
    /// Number of *unique* longitude points in one period — `nx` for the
    /// no-duplicate case, `nx-1` when the last column duplicates the first.
    pub period_points: f64,
}

/// Canvas extent in projected coordinates. Mirrors `rustwx_render::overlay::MapExtent`.
#[derive(Debug, Clone, Copy)]
pub struct MapExtent {
    pub x_min: f64,
    pub x_max: f64,
    pub y_min: f64,
    pub y_max: f64,
}

/// Geographic clip bounds (degrees). Mirrors
/// `rustwx_render::request::GeographicClipBounds`.
#[derive(Debug, Clone, Copy)]
pub struct GeographicClipBounds {
    pub west_deg: f64,
    pub east_deg: f64,
    pub south_deg: f64,
    pub north_deg: f64,
}

/// Already-baked projection parameters — what the host sends to the kernel.
///
/// Field meanings depend on the variant; the kernel decodes them per `kind`:
///   - `Geographic { central_meridian_deg }`  -> p0 = central meridian
///   - `Mercator   { central_meridian_deg, scale }` -> p0=cm, p1=scale (cos(true_lat))
///   - `LambertConformal { n, f, rho0, stand_lon_deg }` -> p0=n, p1=f, p2=rho0, p3=stand_lon
///   - `AlbersEqualArea { n, c, rho0, central_meridian_deg }` -> p0=n, p1=c, p2=rho0, p3=cm
///   - `Robinson { central_meridian_deg }` -> p0=central meridian
#[derive(Debug, Clone, Copy)]
pub enum ProjectionVariant {
    Geographic {
        central_meridian_deg: f64,
    },
    Mercator {
        central_meridian_deg: f64,
        /// Pre-multiplied `cos(latitude_of_true_scale)` — must be > 0.
        scale: f64,
    },
    LambertConformal {
        n: f64,
        f: f64,
        rho0: f64,
        stand_lon_deg: f64,
    },
    AlbersEqualArea {
        n: f64,
        c: f64,
        rho0: f64,
        central_meridian_deg: f64,
    },
    Robinson {
        central_meridian_deg: f64,
    },
    /// Polar stereographic — kernel does not implement an analytic inverse.
    /// The wrapper returns `Err(...)` so the swap site can fall back to CPU.
    PolarStereographic,
}

impl ProjectionVariant {
    fn pack(self) -> Option<(i32, [f64; 6])> {
        let (kind, ps) = match self {
            Self::Geographic { central_meridian_deg } => {
                (KIND_GEOGRAPHIC, [central_meridian_deg, 0.0, 0.0, 0.0, 0.0, 0.0])
            }
            Self::Robinson { central_meridian_deg } => {
                (KIND_ROBINSON, [central_meridian_deg, 0.0, 0.0, 0.0, 0.0, 0.0])
            }
            Self::AlbersEqualArea { n, c, rho0, central_meridian_deg } => {
                (KIND_ALBERS_EQUAL_AREA, [n, c, rho0, central_meridian_deg, 0.0, 0.0])
            }
            Self::LambertConformal { n, f, rho0, stand_lon_deg } => {
                (KIND_LAMBERT_CONFORMAL, [n, f, rho0, stand_lon_deg, 0.0, 0.0])
            }
            Self::Mercator { central_meridian_deg, scale } => {
                (KIND_MERCATOR, [central_meridian_deg, scale, 0.0, 0.0, 0.0, 0.0])
            }
            Self::PolarStereographic => return None,
        };
        Some((kind, ps))
    }
}

/// Bundled inputs for `host_on`. Keeps the wrapper signature manageable.
#[derive(Debug, Clone, Copy)]
pub struct InverseProjArgs {
    pub projection: ProjectionVariant,
    /// Optional geographic clip — pixels whose unprojected (lat, lon) falls
    /// outside this box are skipped (matches CPU `clip_bounds`).
    pub clip: Option<GeographicClipBounds>,
    /// Canvas extent in projected coordinates.
    pub extent: MapExtent,
    /// Source-grid axis description.
    pub axes: RegularLatLonAxes,
}

/// Default-stream convenience wrapper. Use `host_on` for per-thread streams.
#[allow(clippy::too_many_arguments)]
pub fn host(
    ctx: &ContextHandle,
    data: &[f64],
    ny: usize,
    nx: usize,
    args: &InverseProjArgs,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    host_on(ctx, ctx.stream(), data, ny, nx, args, cmap, img_w, img_h)
}

/// Render `data` (`[ny][nx]` row-major, row j at `lat0 + j*lat_step`) into a
/// packed-RGBA8 canvas of size `img_w * img_h`. Output byte order is `R,G,B,A`
/// — drop-in for `image::RgbaImage::from_raw`.
///
/// Returns `Err(Error::Other(...))` for unsupported projections (currently
/// only `PolarStereographic`); the integrator's swap site is expected to
/// catch and fall back to the CPU path.
#[allow(clippy::too_many_arguments)]
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    args: &InverseProjArgs,
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    // Match the CPU's "produce an empty canvas" short-circuit on degenerate
    // inputs rather than panicking.
    if ny < 2 || nx < 2 || data.len() != ny * nx {
        return Ok(vec![0u8; n_pixels * 4]);
    }
    if args.axes.nx != nx || args.axes.ny != ny {
        return Err(err_other(format!(
            "axes.nx/ny ({}, {}) must match data nx/ny ({}, {})",
            args.axes.nx, args.axes.ny, nx, ny
        )));
    }

    let (kind, ps) = args.projection.pack().ok_or_else(|| {
        err_other(
            "rasterize_inverse_projected_regular_latlon: PolarStereographic \
             not implemented on GPU; caller should fall back to CPU",
        )
    })?;

    // Upload the source grid + colormap tables.
    let data_d = DeviceVec::from_host_on(stream, data)?;
    let levels_d = DeviceVec::from_host_on(stream, cmap.levels)?;
    let colors_d = DeviceVec::from_host_on(stream, cmap.colors_packed)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, n_pixels)?;

    // Pack scalar args.
    let n_levels = cmap.levels.len() as i32;
    let n_intervals = cmap.colors_packed.len() as i32;
    let has_under_i: i32 = if cmap.under_color.is_some() { 1 } else { 0 };
    let has_over_i: i32 = if cmap.over_color.is_some() { 1 } else { 0 };
    let under_color: u32 = cmap.under_color.unwrap_or(0);
    let over_color: u32 = cmap.over_color.unwrap_or(0);
    let has_mask_below_i: i32 = if cmap.mask_below.is_some() { 1 } else { 0 };
    let mask_below: f64 = cmap.mask_below.unwrap_or(0.0);

    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let periodic_lon_i: i32 = if args.axes.periodic_lon { 1 } else { 0 };

    let (clip_has, cw, ce, cs, cn) = match args.clip {
        Some(b) => (1i32, b.west_deg, b.east_deg, b.south_deg, b.north_deg),
        None => (0i32, 0.0, 0.0, 0.0, 0.0),
    };

    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    // p0..p5 unpacked into local stack slots — `arg(&ps[0])` doesn't pin the
    // slice through the launch builder lifetime.
    let p0 = ps[0];
    let p1 = ps[1];
    let p2 = ps[2];
    let p3 = ps[3];
    let p4 = ps[4];
    let p5 = ps[5];

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((img_w + bx - 1) / bx, (img_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let lat0 = args.axes.lat0;
    let lat_step = args.axes.lat_step;
    let lon0 = args.axes.lon0;
    let lon_step = args.axes.lon_step;
    let period_points = args.axes.period_points;
    let x_min = args.extent.x_min;
    let x_max = args.extent.x_max;
    let y_min = args.extent.y_min;
    let y_max = args.extent.y_max;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(&lat0)
        .arg(&lat_step)
        .arg(&lon0)
        .arg(&lon_step)
        .arg(&periodic_lon_i)
        .arg(&period_points)
        .arg(&kind)
        .arg(&p0)
        .arg(&p1)
        .arg(&p2)
        .arg(&p3)
        .arg(&p4)
        .arg(&p5)
        .arg(&clip_has)
        .arg(&cw)
        .arg(&ce)
        .arg(&cs)
        .arg(&cn)
        .arg(&x_min)
        .arg(&x_max)
        .arg(&y_min)
        .arg(&y_max)
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

    let pixels_u32 = out_d.copy_to_host_on(stream)?;
    Ok(u32_vec_to_rgba_bytes(pixels_u32))
}

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

    // ---------- CPU reference, inlined from rustwx-render -----------------

    fn cpu_normalize_projection_lon(lon: f64) -> f64 {
        let mut lon = lon % 360.0;
        if lon > 180.0 {
            lon -= 360.0;
        } else if lon <= -180.0 {
            lon += 360.0;
        }
        lon
    }

    fn cpu_normalize_axis_lon(mut lon: f64) -> f64 {
        while lon < -180.0 {
            lon += 360.0;
        }
        while lon >= 180.0 {
            lon -= 360.0;
        }
        lon
    }

    fn cpu_stabilize_lat(lat: f64) -> f64 {
        lat.clamp(-89.999, 89.999)
    }

    fn cpu_clip_contains(c: GeographicClipBounds, lat: f64, lon: f64) -> bool {
        if !lat.is_finite() || !lon.is_finite() {
            return false;
        }
        if lat < c.south_deg || lat > c.north_deg {
            return false;
        }
        let raw_span = (c.east_deg - c.west_deg).abs();
        let span = if raw_span >= 359.0 {
            raw_span.min(360.0)
        } else {
            let w = cpu_normalize_projection_lon(c.west_deg);
            let e = cpu_normalize_projection_lon(c.east_deg);
            if w <= e { e - w } else { e + 360.0 - w }
        };
        if span >= 359.0 {
            return true;
        }
        let w = cpu_normalize_projection_lon(c.west_deg);
        let e = cpu_normalize_projection_lon(c.east_deg);
        let lon = cpu_normalize_projection_lon(lon);
        if w <= e { lon >= w && lon <= e } else { lon >= w || lon <= e }
    }

    fn cpu_unproject(p: ProjectionVariant, x: f64, y: f64) -> Option<(f64, f64)> {
        if !x.is_finite() || !y.is_finite() {
            return None;
        }
        const R: f64 = 6_370_000.0;
        const D2R: f64 = std::f64::consts::PI / 180.0;
        const R2D: f64 = 180.0 / std::f64::consts::PI;
        match p {
            ProjectionVariant::Geographic { central_meridian_deg } => Some((
                cpu_stabilize_lat(y),
                cpu_normalize_projection_lon(x + central_meridian_deg),
            )),
            ProjectionVariant::Mercator { central_meridian_deg, scale } => {
                if scale <= 0.0 {
                    return None;
                }
                let lon = central_meridian_deg + x / (R * scale) * R2D;
                let lat = (2.0 * (y / (R * scale)).exp().atan() - std::f64::consts::PI / 2.0)
                    * R2D;
                Some((
                    cpu_stabilize_lat(lat),
                    cpu_normalize_projection_lon(lon),
                ))
            }
            ProjectionVariant::LambertConformal { n, f, rho0, stand_lon_deg } => {
                let dy = rho0 - y;
                let rho = (x * x + dy * dy).sqrt();
                if !rho.is_finite() || rho <= 0.0 || n.abs() < 1.0e-12 || f.abs() < 1.0e-12 {
                    return None;
                }
                let theta = x.atan2(dy);
                let ratio = R * f / rho;
                if ratio <= 0.0 || !ratio.is_finite() {
                    return None;
                }
                let phi = 2.0 * ratio.powf(1.0 / n).atan() - std::f64::consts::PI / 2.0;
                let lon = stand_lon_deg + theta / n * R2D;
                Some((phi * R2D, cpu_normalize_projection_lon(lon)))
            }
            ProjectionVariant::AlbersEqualArea { n, c, rho0, central_meridian_deg } => {
                let dy = rho0 - y;
                let rho = (x * x + dy * dy).sqrt();
                if !rho.is_finite() || rho <= 0.0 || n.abs() < 1.0e-12 {
                    return None;
                }
                let theta = x.atan2(dy);
                let rn = rho * n / R;
                let arg = (c - rn * rn) / (2.0 * n);
                if !arg.is_finite() || !(-1.0..=1.0).contains(&arg) {
                    return None;
                }
                let lat = arg.asin() * R2D;
                let lon = central_meridian_deg + theta / n * R2D;
                Some((lat, cpu_normalize_projection_lon(lon)))
            }
            ProjectionVariant::Robinson { .. } => {
                // Tests below don't exercise Robinson; leave unimplemented so a
                // misuse fails loudly rather than silently producing zeros.
                let _ = D2R;
                unimplemented!("CPU reference for Robinson not exercised in tests")
            }
            ProjectionVariant::PolarStereographic => None,
        }
    }

    fn cpu_grid_x_for_axis_lon(lon: f64, axes: &RegularLatLonAxes) -> Option<f64> {
        if axes.periodic_lon {
            return Some(((lon - axes.lon0) / axes.lon_step).rem_euclid(axes.period_points));
        }
        let mut adjusted = cpu_normalize_axis_lon(lon);
        let axis_center = axes.lon0 + axes.lon_step * (axes.nx as f64 - 1.0) / 2.0;
        while adjusted - axis_center > 180.0 {
            adjusted -= 360.0;
        }
        while adjusted - axis_center < -180.0 {
            adjusted += 360.0;
        }
        let gx = (adjusted - axes.lon0) / axes.lon_step;
        (gx >= 0.0 && gx <= (axes.nx as f64 - 1.0)).then_some(gx)
    }

    fn cpu_bilinear(v00: f64, v10: f64, v01: f64, v11: f64, fx: f64, fy: f64) -> f64 {
        if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
            let south = v00 * (1.0 - fx) + v10 * fx;
            let north = v01 * (1.0 - fx) + v11 * fx;
            south * (1.0 - fy) + north * fy
        } else {
            for v in [v00, v10, v01, v11] {
                if v.is_finite() {
                    return v;
                }
            }
            f64::NAN
        }
    }

    fn cpu_sample_regular_latlon(
        data: &[f64],
        axes: &RegularLatLonAxes,
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
        let gx = cpu_grid_x_for_axis_lon(lon, axes)?;
        let i0 = (gx.floor() as usize).min(axes.nx - 1);
        let j0 = gy.floor() as usize;
        let i1 = if axes.periodic_lon {
            ((i0 + 1) as f64).rem_euclid(axes.period_points) as usize
        } else {
            (i0 + 1).min(axes.nx - 1)
        };
        let j1 = (j0 + 1).min(axes.ny - 1);
        let fx = gx - i0 as f64;
        let fy = gy - j0 as f64;
        let idx = |j: usize, i: usize| j * axes.nx + i;
        Some(cpu_bilinear(
            data[idx(j0, i0)],
            data[idx(j0, i1)],
            data[idx(j1, i0)],
            data[idx(j1, i1)],
            fx,
            fy,
        ))
    }

    fn cpu_colormap_lookup(
        value: f64,
        levels: &[f64],
        colors: &[u32],
        under: Option<u32>,
        over: Option<u32>,
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
            return under.unwrap_or(0);
        }
        let n_intervals = levels.len() - 1;
        let idx = levels.partition_point(|l| *l <= value);
        if idx <= n_intervals {
            return colors[idx.saturating_sub(1).min(colors.len() - 1)];
        }
        over.unwrap_or(colors[colors.len() - 1])
    }

    fn cpu_render(
        data: &[f64],
        ny: usize,
        nx: usize,
        args: &InverseProjArgs,
        levels: &[f64],
        colors: &[u32],
        under: Option<u32>,
        over: Option<u32>,
        mask_below: Option<f64>,
        img_w: u32,
        img_h: u32,
    ) -> Vec<u8> {
        let mut out = vec![0u8; (img_w as usize) * (img_h as usize) * 4];
        if ny < 2 || nx < 2 {
            return out;
        }
        let x_den = img_w.saturating_sub(1).max(1) as f64;
        let y_den = img_h.saturating_sub(1).max(1) as f64;
        for py in 0..img_h {
            let y = args.extent.y_max
                - (py as f64 / y_den) * (args.extent.y_max - args.extent.y_min);
            for px in 0..img_w {
                let x = args.extent.x_min
                    + (px as f64 / x_den) * (args.extent.x_max - args.extent.x_min);
                let Some((lat, lon)) = cpu_unproject(args.projection, x, y) else {
                    continue;
                };
                if let Some(c) = args.clip {
                    if !cpu_clip_contains(c, lat, lon) {
                        continue;
                    }
                }
                let Some(value) = cpu_sample_regular_latlon(data, &args.axes, lat, lon) else {
                    continue;
                };
                let packed =
                    cpu_colormap_lookup(value, levels, colors, under, over, mask_below);
                let alpha = (packed >> 24) & 0xFF;
                if alpha == 0 {
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

    // ---------- Synthetic inputs -----------------------------------------

    fn synthetic_grid(ny: usize, nx: usize) -> Vec<f64> {
        // Smooth pattern + a couple of NaN holes to exercise the bilinear
        // first-finite fallback path.
        let mut v = vec![0.0; ny * nx];
        for j in 0..ny {
            for i in 0..nx {
                let u = i as f64 / (nx as f64 - 1.0).max(1.0);
                let w = j as f64 / (ny as f64 - 1.0).max(1.0);
                v[j * nx + i] = (u * 6.28).sin() * 50.0
                    + (w * 4.71).cos() * 30.0
                    + (u * w * 12.0).sin() * 20.0;
            }
        }
        v[0] = f64::NAN;
        if v.len() > 17 {
            v[17] = f64::NAN;
        }
        if v.len() > 233 {
            v[233] = f64::NAN;
        }
        v
    }

    fn synthetic_cmap() -> (Vec<f64>, Vec<u32>, Option<u32>, Option<u32>) {
        // Asymmetric levels exercise the binary search.
        let levels: Vec<f64> = vec![
            -100.0, -75.0, -50.0, -30.0, -15.0, -5.0, 0.0, 5.0, 15.0, 30.0, 50.0, 75.0, 100.0,
        ];
        let palette: Vec<u32> = (0..(levels.len() - 1))
            .map(|k| {
                let t = (k as f64) / ((levels.len() - 2) as f64);
                let r = (50.0 + 200.0 * t) as u8;
                let g = (200.0 - 150.0 * t) as u8;
                let b = (60.0 + 150.0 * (1.0 - t)) as u8;
                pack_rgba(r, g, b, 255)
            })
            .collect();
        let under = Some(pack_rgba(20, 20, 20, 255));
        let over = Some(pack_rgba(240, 240, 240, 255));
        (levels, palette, under, over)
    }

    fn max_channel_delta(a: &[u8], b: &[u8]) -> u8 {
        let mut m: u8 = 0;
        for (x, y) in a.iter().zip(b.iter()) {
            let d = if x > y { x - y } else { y - x };
            if d > m {
                m = d;
            }
        }
        m
    }

    fn build_args_geographic() -> InverseProjArgs {
        InverseProjArgs {
            projection: ProjectionVariant::Geographic { central_meridian_deg: 0.0 },
            clip: None,
            extent: MapExtent { x_min: -180.0, x_max: 180.0, y_min: -90.0, y_max: 90.0 },
            axes: RegularLatLonAxes {
                nx: 60,
                ny: 40,
                // Row 0 is south-most, row ny-1 is north-most.
                lat0: -90.0,
                lat_step: 180.0 / 39.0,
                lon0: -180.0,
                lon_step: 360.0 / 60.0,
                periodic_lon: true,
                period_points: 60.0,
            },
        }
    }

    fn build_args_mercator() -> InverseProjArgs {
        // Mercator centered on the prime meridian, true scale at the equator.
        // Pick a moderate canvas extent in meters that stays well clear of the
        // poles (Mercator blows up near ±90°).
        let scale = 1.0_f64; // cos(0°)
        InverseProjArgs {
            projection: ProjectionVariant::Mercator {
                central_meridian_deg: 0.0,
                scale,
            },
            clip: None,
            extent: MapExtent {
                x_min: -2.0e7,
                x_max: 2.0e7,
                y_min: -1.0e7,
                y_max: 1.0e7,
            },
            axes: RegularLatLonAxes {
                nx: 60,
                ny: 40,
                lat0: -90.0,
                lat_step: 180.0 / 39.0,
                lon0: -180.0,
                lon_step: 360.0 / 60.0,
                periodic_lon: true,
                period_points: 60.0,
            },
        }
    }

    #[test]
    fn geographic_matches_cpu_reference() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("skip: no CUDA context available");
                return;
            }
        };

        let ny = 40;
        let nx = 60;
        let data = synthetic_grid(ny, nx);
        let (levels, colors, under, over) = synthetic_cmap();
        let args = build_args_geographic();

        let img_w = 720u32;
        let img_h = 360u32;

        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };

        let gpu = host(&ctx, &data, ny, nx, &args, view, img_w, img_h)
            .expect("GPU rasterize_inverse_projected_regular_latlon (geographic)");
        let cpu =
            cpu_render(&data, ny, nx, &args, &levels, &colors, under, over, None, img_w, img_h);

        assert_eq!(gpu.len(), cpu.len(), "buffer sizes must match");
        let delta = max_channel_delta(&gpu, &cpu);
        eprintln!(
            "[inverse_projected geographic] img={}x{} max_channel_delta={}",
            img_w, img_h, delta
        );
        assert!(delta <= 1, "max channel delta too large: {}", delta);
    }

    #[test]
    fn mercator_matches_cpu_reference() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("skip: no CUDA context available");
                return;
            }
        };

        let ny = 40;
        let nx = 60;
        let data = synthetic_grid(ny, nx);
        let (levels, colors, under, over) = synthetic_cmap();
        let args = build_args_mercator();

        let img_w = 512u32;
        let img_h = 256u32;

        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };

        let gpu = host(&ctx, &data, ny, nx, &args, view, img_w, img_h)
            .expect("GPU rasterize_inverse_projected_regular_latlon (mercator)");
        let cpu =
            cpu_render(&data, ny, nx, &args, &levels, &colors, under, over, None, img_w, img_h);

        assert_eq!(gpu.len(), cpu.len(), "buffer sizes must match");
        let delta = max_channel_delta(&gpu, &cpu);
        eprintln!(
            "[inverse_projected mercator] img={}x{} max_channel_delta={}",
            img_w, img_h, delta
        );
        assert!(delta <= 1, "max channel delta too large: {}", delta);
    }

    #[test]
    fn polar_stereographic_returns_err_so_caller_falls_back_to_cpu() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("skip: no CUDA context available");
                return;
            }
        };

        let ny = 40;
        let nx = 60;
        let data = synthetic_grid(ny, nx);
        let (levels, colors, under, over) = synthetic_cmap();
        let mut args = build_args_geographic();
        args.projection = ProjectionVariant::PolarStereographic;

        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };

        let res = host(&ctx, &data, ny, nx, &args, view, 256, 256);
        assert!(
            res.is_err(),
            "polar stereographic must surface as Err so the swap site falls back to CPU"
        );
    }
}
