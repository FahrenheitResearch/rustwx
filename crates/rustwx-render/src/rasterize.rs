use crate::color::Rgba;
use crate::colormap::LeveledColormap;
use crate::overlay::MapExtent;
use crate::projection::ProjectionProjector;
use crate::request::GeographicClipBounds;
use image::RgbaImage;

/// Below this output-pixel count, the per-call CUDA upload + launch overhead
/// is not worth the saved CPU work. Tuned so a 224x224 panel still goes GPU.
#[cfg(feature = "cuda")]
const CUDA_MIN_PIXELS: usize = 50_000;

/// Hard counters proving whether the CUDA path actually ran. Set
/// `RUSTWX_CUDA_RASTERIZE_DEBUG=1` to have the binary print them at exit.
#[cfg(feature = "cuda")]
pub(crate) mod cuda_stats {
    use std::sync::atomic::{AtomicUsize, Ordering};
    pub static GRID_TRY: AtomicUsize = AtomicUsize::new(0);
    pub static GRID_OK: AtomicUsize = AtomicUsize::new(0);
    pub static GRID_FAIL: AtomicUsize = AtomicUsize::new(0);
    pub static GRID_BELOW_THRESHOLD: AtomicUsize = AtomicUsize::new(0);
    pub static PG_TRY: AtomicUsize = AtomicUsize::new(0);
    pub static PG_OK: AtomicUsize = AtomicUsize::new(0);
    pub static PG_FAIL: AtomicUsize = AtomicUsize::new(0);
    pub static PG_BELOW_THRESHOLD: AtomicUsize = AtomicUsize::new(0);
    pub static INV_TRY: AtomicUsize = AtomicUsize::new(0);
    pub static INV_OK: AtomicUsize = AtomicUsize::new(0);
    pub static INV_FAIL: AtomicUsize = AtomicUsize::new(0);
    pub static INV_BELOW_THRESHOLD: AtomicUsize = AtomicUsize::new(0);

    pub fn snapshot() -> [(&'static str, usize); 12] {
        [
            (
                "rasterize_grid: cuda attempts",
                GRID_TRY.load(Ordering::Relaxed),
            ),
            ("rasterize_grid: cuda OK", GRID_OK.load(Ordering::Relaxed)),
            (
                "rasterize_grid: cuda FAIL (fell back to CPU)",
                GRID_FAIL.load(Ordering::Relaxed),
            ),
            (
                "rasterize_grid: skipped (below threshold)",
                GRID_BELOW_THRESHOLD.load(Ordering::Relaxed),
            ),
            (
                "rasterize_projected_grid: cuda attempts",
                PG_TRY.load(Ordering::Relaxed),
            ),
            (
                "rasterize_projected_grid: cuda OK",
                PG_OK.load(Ordering::Relaxed),
            ),
            (
                "rasterize_projected_grid: cuda FAIL (fell back to CPU)",
                PG_FAIL.load(Ordering::Relaxed),
            ),
            (
                "rasterize_projected_grid: skipped (below threshold)",
                PG_BELOW_THRESHOLD.load(Ordering::Relaxed),
            ),
            (
                "rasterize_inverse_projected_grid: cuda attempts",
                INV_TRY.load(Ordering::Relaxed),
            ),
            (
                "rasterize_inverse_projected_grid: cuda OK",
                INV_OK.load(Ordering::Relaxed),
            ),
            (
                "rasterize_inverse_projected_grid: cuda FAIL (fell back to CPU)",
                INV_FAIL.load(Ordering::Relaxed),
            ),
            (
                "rasterize_inverse_projected_grid: skipped (below threshold)",
                INV_BELOW_THRESHOLD.load(Ordering::Relaxed),
            ),
        ]
    }

    pub fn print_if_enabled() {
        if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
            eprintln!("[rustwx-cuda rasterize counters]");
            for (label, value) in snapshot() {
                eprintln!("  {label}: {value}");
            }
            super::cuda_stats_ds::print_if_enabled();
            rustwx_cuda::render::print_phase_timing_if_enabled();
        }
    }
}

/// Counters for the GPU downsample+sharpen swap.
#[cfg(feature = "cuda")]
pub(crate) mod cuda_stats_ds {
    use std::sync::atomic::{AtomicUsize, Ordering};
    pub static TRY: AtomicUsize = AtomicUsize::new(0);
    pub static OK: AtomicUsize = AtomicUsize::new(0);
    pub static FAIL: AtomicUsize = AtomicUsize::new(0);

    pub fn print_if_enabled() {
        eprintln!(
            "  downsample+sharpen: cuda attempts={} OK={} FAIL={}",
            TRY.load(Ordering::Relaxed),
            OK.load(Ordering::Relaxed),
            FAIL.load(Ordering::Relaxed),
        );
        super::cuda_stats_lw::print_if_enabled();
    }
}

/// Counters for the GPU linework swap.
#[cfg(feature = "cuda")]
pub(crate) mod cuda_stats_lw {
    use std::sync::atomic::{AtomicUsize, Ordering};
    pub static TRY: AtomicUsize = AtomicUsize::new(0);
    pub static OK: AtomicUsize = AtomicUsize::new(0);
    pub static FAIL: AtomicUsize = AtomicUsize::new(0);

    pub fn print_if_enabled() {
        eprintln!(
            "  linework: cuda attempts={} OK={} FAIL={}",
            TRY.load(Ordering::Relaxed),
            OK.load(Ordering::Relaxed),
            FAIL.load(Ordering::Relaxed),
        );
    }
}

/// Re-export of the per-thread stream cache for use by the downsample
/// swap site in `render.rs`.
#[cfg(feature = "cuda")]
pub(crate) fn with_thread_stream_for_downsample<R>(
    f: impl FnOnce(
        &rustwx_cuda::core::ContextHandle,
        &std::sync::Arc<rustwx_cuda::core::cudarc::driver::CudaStream>,
    ) -> R,
) -> Option<R> {
    with_thread_stream(f)
}

/// Public counter snapshot — lets a CLI binary print the numbers at exit.
#[cfg(feature = "cuda")]
pub fn cuda_rasterize_stats() -> [(&'static str, usize); 12] {
    cuda_stats::snapshot()
}

/// Print the CUDA rasterize counters to stderr if `RUSTWX_CUDA_RASTERIZE_DEBUG=1`.
#[cfg(feature = "cuda")]
pub fn print_cuda_rasterize_stats_if_enabled() {
    cuda_stats::print_if_enabled();
}

#[cfg(not(feature = "cuda"))]
pub fn cuda_rasterize_stats() -> [(&'static str, usize); 0] {
    []
}

#[cfg(not(feature = "cuda"))]
pub fn print_cuda_rasterize_stats_if_enabled() {}

#[cfg(feature = "cuda")]
fn cuda_pack_cmap(cmap: &LeveledColormap) -> (Vec<u32>, Option<u32>, Option<u32>) {
    use rustwx_cuda::render::pack_rgba;
    let colors_packed: Vec<u32> = cmap
        .colors
        .iter()
        .map(|c| pack_rgba(c.r, c.g, c.b, c.a))
        .collect();
    let under_color = cmap.under_color.map(|c| pack_rgba(c.r, c.g, c.b, c.a));
    let over_color = cmap.over_color.map(|c| pack_rgba(c.r, c.g, c.b, c.a));
    (colors_packed, under_color, over_color)
}

/// Each rayon worker thread caches its own non-default CUDA stream the
/// first time it does GPU work. Multiple non-default streams run
/// concurrently on the device — the whole point of this cache is to keep
/// the GPU saturated when many threads issue kernels in parallel, instead
/// of all of them queuing on the shared default stream.
///
/// `new_stream()` calls `bind_to_thread()` internally; on rare timing the
/// driver returns NOT_INITIALIZED for newly-spawned rayon workers. We
/// explicitly bind first, retry once, and fall back to the shared default
/// stream rather than dropping the call to CPU. Falling back to default
/// keeps correctness; only the per-thread concurrency benefit is lost for
/// that specific call.
#[cfg(feature = "cuda")]
fn with_thread_stream<R>(
    f: impl FnOnce(
        &rustwx_cuda::core::ContextHandle,
        &std::sync::Arc<rustwx_cuda::core::cudarc::driver::CudaStream>,
    ) -> R,
) -> Option<R> {
    use rustwx_cuda::core::cudarc::driver::CudaStream;
    use std::cell::RefCell;
    use std::sync::Arc;

    thread_local! {
        static THREAD_STREAM: RefCell<Option<(rustwx_cuda::core::ContextHandle, Arc<CudaStream>)>> =
            const { RefCell::new(None) };
    }

    THREAD_STREAM.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let ctx = rustwx_cuda::core::global().ok()?;
            // Bind the primary context to this OS thread before any stream
            // creation. cudarc's `new_stream` does this internally too, but
            // doing it explicitly avoids a NOT_INITIALIZED race we've seen
            // when a rayon worker is spawned mid-pipeline.
            let _ = ctx.cuda().bind_to_thread();
            let stream = match ctx.new_stream() {
                Ok(s) => s,
                Err(_) => {
                    // One retry after explicit bind.
                    let _ = ctx.cuda().bind_to_thread();
                    match ctx.new_stream() {
                        Ok(s) => s,
                        // Last-resort fallback: default stream. Loses
                        // concurrency on this thread but the call still
                        // runs on the GPU.
                        Err(_) => Arc::clone(ctx.stream()),
                    }
                }
            };
            *slot = Some((ctx, stream));
        }
        let (ctx, stream) = slot.as_ref().unwrap();
        Some(f(ctx, stream))
    })
}

#[cfg(feature = "cuda")]
fn cuda_rasterize_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> Option<RgbaImage> {
    use rustwx_cuda::render::{ColormapHostView, rasterize_grid as cu_rg};
    use std::sync::atomic::Ordering;

    cuda_stats::GRID_TRY.fetch_add(1, Ordering::Relaxed);

    let bytes = with_thread_stream(|ctx, stream| {
        let (colors_packed, under_color, over_color) = cuda_pack_cmap(cmap);
        let view = ColormapHostView {
            levels: &cmap.levels,
            colors_packed: &colors_packed,
            under_color,
            over_color,
            mask_below: cmap.mask_below,
        };
        cu_rg::host_on(ctx, stream, data, ny, nx, view, img_w, img_h)
    });

    let bytes = match bytes {
        Some(Ok(b)) => b,
        _ => {
            cuda_stats::GRID_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    let img = RgbaImage::from_raw(img_w, img_h, bytes)?;
    cuda_stats::GRID_OK.fetch_add(1, Ordering::Relaxed);
    Some(img)
}

#[cfg(feature = "cuda")]
fn cuda_rasterize_projected_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> Option<RgbaImage> {
    use rustwx_cuda::render::{ColormapHostView, rasterize_projected_grid as cu_pg};
    use std::sync::atomic::Ordering;

    cuda_stats::PG_TRY.fetch_add(1, Ordering::Relaxed);

    let bytes = with_thread_stream(|ctx, stream| {
        let (colors_packed, under_color, over_color) = cuda_pack_cmap(cmap);
        let view = ColormapHostView {
            levels: &cmap.levels,
            colors_packed: &colors_packed,
            under_color,
            over_color,
            mask_below: cmap.mask_below,
        };
        cu_pg::host_on(ctx, stream, data, ny, nx, pixel_points, view, img_w, img_h)
    });

    let bytes = match bytes {
        Some(Ok(b)) => b,
        Some(Err(e)) => {
            if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
                eprintln!(
                    "[cuda_rasterize_projected_grid] host_on Err ny={ny} nx={nx} img={img_w}x{img_h} ppl={} datal={}: {e:?}",
                    pixel_points.len(),
                    data.len()
                );
            }
            cuda_stats::PG_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        None => {
            if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
                eprintln!("[cuda_rasterize_projected_grid] thread-stream init returned None");
            }
            cuda_stats::PG_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    let bytes_len = bytes.len();
    let img = match RgbaImage::from_raw(img_w, img_h, bytes) {
        Some(i) => i,
        None => {
            if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
                eprintln!(
                    "[cuda_rasterize_projected_grid] RgbaImage::from_raw None: img={img_w}x{img_h} bytes_len={bytes_len}"
                );
            }
            cuda_stats::PG_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    cuda_stats::PG_OK.fetch_add(1, Ordering::Relaxed);
    Some(img)
}

#[cfg(feature = "cuda")]
fn cuda_rasterize_inverse_projected_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    axes: RegularLatLonAxes,
    projector: ProjectionProjector,
    clip_bounds: Option<GeographicClipBounds>,
    extent: &MapExtent,
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> Option<RgbaImage> {
    use rustwx_cuda::render::{ColormapHostView, rasterize_inverse_projected_grid as cu_inv};
    use std::sync::atomic::Ordering;

    let projection_params = projector.inverse_kernel_params()?;
    cuda_stats::INV_TRY.fetch_add(1, Ordering::Relaxed);

    let bytes = with_thread_stream(|ctx, stream| {
        let (colors_packed, under_color, over_color) = cuda_pack_cmap(cmap);
        let view = ColormapHostView {
            levels: &cmap.levels,
            colors_packed: &colors_packed,
            under_color,
            over_color,
            mask_below: cmap.mask_below,
        };
        let cu_axes = cu_inv::RegularLatLonAxesHost {
            nx: axes.nx as i32,
            ny: axes.ny as i32,
            lat0: axes.lat0,
            lat_step: axes.lat_step,
            lon0: axes.lon0,
            lon_step: axes.lon_step,
            periodic_lon: axes.periodic_lon,
            period_points: axes.period_points,
        };
        let cu_projection = cu_inv::InverseProjectionHost {
            kind: projection_params.kind,
            p0: projection_params.p0,
            p1: projection_params.p1,
            p2: projection_params.p2,
            p3: projection_params.p3,
            p4: projection_params.p4,
            p5: projection_params.p5,
        };
        let cu_clip = clip_bounds.map_or(
            cu_inv::ClipBoundsHost {
                has_clip: false,
                west_deg: 0.0,
                east_deg: 0.0,
                south_deg: 0.0,
                north_deg: 0.0,
            },
            |bounds| cu_inv::ClipBoundsHost {
                has_clip: true,
                west_deg: bounds.west_deg,
                east_deg: bounds.east_deg,
                south_deg: bounds.south_deg,
                north_deg: bounds.north_deg,
            },
        );
        let cu_extent = cu_inv::MapExtentHost {
            x_min: extent.x_min,
            x_max: extent.x_max,
            y_min: extent.y_min,
            y_max: extent.y_max,
        };
        cu_inv::host_on(
            ctx,
            stream,
            data,
            ny,
            nx,
            cu_axes,
            cu_projection,
            cu_clip,
            cu_extent,
            view,
            img_w,
            img_h,
        )
    });

    let bytes = match bytes {
        Some(Ok(b)) => b,
        Some(Err(e)) => {
            if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
                eprintln!(
                    "[cuda_rasterize_inverse_projected_grid] host_on Err ny={ny} nx={nx} img={img_w}x{img_h} datal={}: {e:?}",
                    data.len()
                );
            }
            cuda_stats::INV_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        None => {
            if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
                eprintln!(
                    "[cuda_rasterize_inverse_projected_grid] thread-stream init returned None"
                );
            }
            cuda_stats::INV_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    let bytes_len = bytes.len();
    let img = match RgbaImage::from_raw(img_w, img_h, bytes) {
        Some(i) => i,
        None => {
            if std::env::var("RUSTWX_CUDA_RASTERIZE_DEBUG").ok().as_deref() == Some("1") {
                eprintln!(
                    "[cuda_rasterize_inverse_projected_grid] RgbaImage::from_raw None: img={img_w}x{img_h} bytes_len={bytes_len}"
                );
            }
            cuda_stats::INV_FAIL.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    cuda_stats::INV_OK.fetch_add(1, Ordering::Relaxed);
    Some(img)
}

/// Rasterize a 2D grid into an RGBA image using bilinear sampling.
///
/// `data` is row-major `[ny][nx]`. The image maps grid row 0 to the
/// bottom of the image (geographic convention: south at bottom).
pub fn rasterize_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    #[cfg(feature = "cuda")]
    {
        use std::sync::atomic::Ordering;
        let n_pix = (img_w as usize).saturating_mul(img_h as usize);
        if ny != 0 && nx != 0 && data.len() == ny * nx {
            if n_pix >= CUDA_MIN_PIXELS {
                if let Some(gpu_img) = cuda_rasterize_grid(data, ny, nx, cmap, img_w, img_h) {
                    return gpu_img;
                }
            } else {
                cuda_stats::GRID_BELOW_THRESHOLD.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let mut img = RgbaImage::new(img_w, img_h);

    if ny == 0 || nx == 0 {
        return img;
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

            let value = bilinear(v00, v10, v01, v11, fx, fy);
            let color = cmap.map(value);
            img.put_pixel(px, py, color.to_image_rgba());
        }
    }

    img
}

/// Rasterize a 2D grid on a projected mesh.
///
/// `pixel_points` contains local image coordinates in map space, one per grid
/// point, or `None` when the projected point falls outside the valid extent.
pub fn rasterize_projected_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    #[cfg(feature = "cuda")]
    {
        use std::sync::atomic::Ordering;
        let n_pix = (img_w as usize).saturating_mul(img_h as usize);
        if ny >= 2 && nx >= 2 && pixel_points.len() == ny * nx && data.len() == ny * nx {
            if n_pix >= CUDA_MIN_PIXELS {
                if let Some(mut gpu_img) =
                    cuda_rasterize_projected_grid(data, ny, nx, pixel_points, cmap, img_w, img_h)
                {
                    feather_projected_raster_edges(&mut gpu_img);
                    return gpu_img;
                }
            } else {
                cuda_stats::PG_BELOW_THRESHOLD.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let mut img = RgbaImage::new(img_w, img_h);

    if ny < 2 || nx < 2 || pixel_points.len() != ny * nx {
        return img;
    }

    for j in 0..(ny - 1) {
        for i in 0..(nx - 1) {
            let idx = |jj: usize, ii: usize| jj * nx + ii;
            let p00 = pixel_points[idx(j, i)];
            let p10 = pixel_points[idx(j, i + 1)];
            let p01 = pixel_points[idx(j + 1, i)];
            let p11 = pixel_points[idx(j + 1, i + 1)];

            let (p00, p10, p01, p11) = match (p00, p10, p01, p11) {
                (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
                _ => continue,
            };

            let v00 = data[idx(j, i)];
            let v10 = data[idx(j, i + 1)];
            let v01 = data[idx(j + 1, i)];
            let v11 = data[idx(j + 1, i + 1)];

            rasterize_triangle(&mut img, p00, v00, p10, v10, p11, v11, cmap);
            rasterize_triangle(&mut img, p00, v00, p11, v11, p01, v01, cmap);
        }
    }

    feather_projected_raster_edges(&mut img);
    img
}

pub(crate) fn rasterize_inverse_projected_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    lat_deg: &[f64],
    lon_deg: &[f64],
    projector: ProjectionProjector,
    clip_bounds: Option<GeographicClipBounds>,
    extent: &MapExtent,
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    let mut img = RgbaImage::new(img_w, img_h);
    if ny < 2
        || nx < 2
        || data.len() != ny * nx
        || lat_deg.len() != ny * nx
        || lon_deg.len() != ny * nx
    {
        return img;
    }

    let Some(axes) = RegularLatLonAxes::from_grid(lat_deg, lon_deg, ny, nx) else {
        return img;
    };

    #[cfg(feature = "cuda")]
    {
        use std::sync::atomic::Ordering;
        let n_pix = (img_w as usize).saturating_mul(img_h as usize);
        if n_pix >= CUDA_MIN_PIXELS {
            if let Some(gpu_img) = cuda_rasterize_inverse_projected_grid(
                data,
                ny,
                nx,
                axes,
                projector,
                clip_bounds,
                extent,
                cmap,
                img_w,
                img_h,
            ) {
                return gpu_img;
            }
        } else {
            cuda_stats::INV_BELOW_THRESHOLD.fetch_add(1, Ordering::Relaxed);
        }
    }

    let x_den = img_w.saturating_sub(1).max(1) as f64;
    let y_den = img_h.saturating_sub(1).max(1) as f64;
    for py in 0..img_h {
        let y = extent.y_max - (py as f64 / y_den) * (extent.y_max - extent.y_min);
        for px in 0..img_w {
            let x = extent.x_min + (px as f64 / x_den) * (extent.x_max - extent.x_min);
            let Some((lat, lon)) = projector.unproject(x, y) else {
                continue;
            };
            if clip_bounds.is_some_and(|bounds| !bounds.contains(lat, lon)) {
                continue;
            }
            let Some(value) = sample_regular_latlon_grid(data, &axes, lat, lon) else {
                continue;
            };
            let color = cmap.map(value);
            let rgba = color.to_image_rgba();
            if rgba.0[3] > 0 {
                img.put_pixel(px, py, rgba);
            }
        }
    }
    img
}

pub fn rasterize_rgba_grid(
    pixels: &[Rgba],
    ny: usize,
    nx: usize,
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    let mut img = RgbaImage::new(img_w, img_h);

    if ny == 0 || nx == 0 || pixels.len() != ny * nx {
        return img;
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

            let c00 = pixels[j0 * nx + i0];
            let c10 = pixels[j0 * nx + i1];
            let c01 = pixels[j1 * nx + i0];
            let c11 = pixels[j1 * nx + i1];
            let color = bilinear_rgba(c00, c10, c01, c11, fx, fy);
            if color.a > 0 {
                img.put_pixel(px, py, color.to_image_rgba());
            }
        }
    }

    img
}

#[derive(Clone, Copy)]
struct RegularLatLonAxes {
    nx: usize,
    ny: usize,
    lat0: f64,
    lat_step: f64,
    lon0: f64,
    lon_step: f64,
    periodic_lon: bool,
    period_points: f64,
}

impl RegularLatLonAxes {
    fn from_grid(lat_deg: &[f64], lon_deg: &[f64], ny: usize, nx: usize) -> Option<Self> {
        if nx < 2 || ny < 2 {
            return None;
        }
        let lat0 = lat_deg[0];
        let lat_last = lat_deg[(ny - 1) * nx];
        let lon0 = lon_deg[0];
        let lon1 = lon_deg[1];
        let lat_step = (lat_last - lat0) / (ny - 1) as f64;
        let lon_step = normalize_axis_lon_delta(lon1 - lon0);
        if !lat_step.is_finite()
            || !lon_step.is_finite()
            || lat_step.abs() < 1.0e-9
            || lon_step.abs() < 1.0e-9
        {
            return None;
        }
        let step = lon_step.abs();
        let tol = (step * 1.5).max(1.0e-6);
        let no_duplicate_full = ((nx as f64 * step) - 360.0).abs() <= tol;
        let duplicate_endpoint_full = (((nx - 1) as f64 * step) - 360.0).abs() <= tol;
        Some(Self {
            nx,
            ny,
            lat0,
            lat_step,
            lon0,
            lon_step,
            periodic_lon: no_duplicate_full || duplicate_endpoint_full,
            period_points: if duplicate_endpoint_full && !no_duplicate_full {
                (nx - 1) as f64
            } else {
                nx as f64
            },
        })
    }
}

fn sample_regular_latlon_grid(
    data: &[f64],
    axes: &RegularLatLonAxes,
    lat: f64,
    lon: f64,
) -> Option<f64> {
    if !lat.is_finite() || !lon.is_finite() {
        return None;
    }
    let gy = (lat - axes.lat0) / axes.lat_step;
    if gy < 0.0 || gy > (axes.ny - 1) as f64 {
        return None;
    }
    let gx = grid_x_for_axis_lon(lon, *axes)?;

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
    Some(bilinear(
        data[idx(j0, i0)],
        data[idx(j0, i1)],
        data[idx(j1, i0)],
        data[idx(j1, i1)],
        fx,
        fy,
    ))
}

fn grid_x_for_axis_lon(lon: f64, axes: RegularLatLonAxes) -> Option<f64> {
    if axes.periodic_lon {
        return Some(((lon - axes.lon0) / axes.lon_step).rem_euclid(axes.period_points));
    }

    let mut adjusted = normalize_longitude_deg(lon);
    let axis_center = axes.lon0 + axes.lon_step * (axes.nx - 1) as f64 / 2.0;
    while adjusted - axis_center > 180.0 {
        adjusted -= 360.0;
    }
    while adjusted - axis_center < -180.0 {
        adjusted += 360.0;
    }

    let gx = (adjusted - axes.lon0) / axes.lon_step;
    (gx >= 0.0 && gx <= (axes.nx - 1) as f64).then_some(gx)
}

fn normalize_axis_lon_delta(delta: f64) -> f64 {
    if delta > 180.0 {
        delta - 360.0
    } else if delta < -180.0 {
        delta + 360.0
    } else {
        delta
    }
}

fn normalize_longitude_deg(mut lon: f64) -> f64 {
    while lon < -180.0 {
        lon += 360.0;
    }
    while lon >= 180.0 {
        lon -= 360.0;
    }
    lon
}

pub fn rasterize_projected_rgba_grid(
    pixels: &[Rgba],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    let mut img = RgbaImage::new(img_w, img_h);

    if ny < 2 || nx < 2 || pixels.len() != ny * nx || pixel_points.len() != ny * nx {
        return img;
    }

    for j in 0..(ny - 1) {
        for i in 0..(nx - 1) {
            let idx = |jj: usize, ii: usize| jj * nx + ii;
            let p00 = pixel_points[idx(j, i)];
            let p10 = pixel_points[idx(j, i + 1)];
            let p01 = pixel_points[idx(j + 1, i)];
            let p11 = pixel_points[idx(j + 1, i + 1)];

            let (p00, p10, p01, p11) = match (p00, p10, p01, p11) {
                (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
                _ => continue,
            };

            let c00 = pixels[idx(j, i)];
            let c10 = pixels[idx(j, i + 1)];
            let c01 = pixels[idx(j + 1, i)];
            let c11 = pixels[idx(j + 1, i + 1)];

            rasterize_rgba_triangle(&mut img, p00, c00, p10, c10, p11, c11);
            rasterize_rgba_triangle(&mut img, p00, c00, p11, c11, p01, c01);
        }
    }

    feather_projected_raster_edges(&mut img);
    img
}

/// Rasterize projected grid coverage independent of fill values or colormap
/// alpha so frame calculations track the valid mesh footprint, not the weather
/// blob currently painted into it.
pub fn rasterize_projected_coverage_mask(
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    let mut img = RgbaImage::new(img_w, img_h);

    if ny < 2 || nx < 2 || pixel_points.len() != ny * nx {
        return img;
    }

    for j in 0..(ny - 1) {
        for i in 0..(nx - 1) {
            let idx = |jj: usize, ii: usize| jj * nx + ii;
            let p00 = pixel_points[idx(j, i)];
            let p10 = pixel_points[idx(j, i + 1)];
            let p01 = pixel_points[idx(j + 1, i)];
            let p11 = pixel_points[idx(j + 1, i + 1)];

            let (p00, p10, p01, p11) = match (p00, p10, p01, p11) {
                (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
                _ => continue,
            };

            rasterize_mask_triangle(&mut img, p00, p10, p11);
            rasterize_mask_triangle(&mut img, p00, p11, p01);
        }
    }

    img
}

fn bilinear(v00: f64, v10: f64, v01: f64, v11: f64, fx: f64, fy: f64) -> f64 {
    if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
        let south = v00 * (1.0 - fx) + v10 * fx;
        let north = v01 * (1.0 - fx) + v11 * fx;
        south * (1.0 - fy) + north * fy
    } else {
        for value in [v00, v10, v01, v11] {
            if value.is_finite() {
                return value;
            }
        }
        f64::NAN
    }
}

fn bilinear_rgba(c00: Rgba, c10: Rgba, c01: Rgba, c11: Rgba, fx: f64, fy: f64) -> Rgba {
    if c00.a == 0 && c10.a == 0 && c01.a == 0 && c11.a == 0 {
        return Rgba::TRANSPARENT;
    }
    let south = lerp_rgba_pair(c00, c10, fx);
    let north = lerp_rgba_pair(c01, c11, fx);
    lerp_rgba_pair(south, north, fy)
}

fn rasterize_triangle(
    img: &mut RgbaImage,
    p0: (f64, f64),
    v0: f64,
    p1: (f64, f64),
    v1: f64,
    p2: (f64, f64),
    v2: f64,
    cmap: &LeveledColormap,
) {
    if !v0.is_finite() || !v1.is_finite() || !v2.is_finite() {
        return;
    }

    let min_x = p0.0.min(p1.0).min(p2.0).floor().max(0.0) as i32;
    let max_x =
        p0.0.max(p1.0)
            .max(p2.0)
            .ceil()
            .min(img.width() as f64 - 1.0) as i32;
    let min_y = p0.1.min(p1.1).min(p2.1).floor().max(0.0) as i32;
    let max_y =
        p0.1.max(p1.1)
            .max(p2.1)
            .ceil()
            .min(img.height() as f64 - 1.0) as i32;

    if min_x > max_x || min_y > max_y {
        return;
    }

    let area = edge_fn(p0, p1, p2);
    if area.abs() < 1e-9 {
        return;
    }

    let inv_area = 1.0 / area;

    for py in min_y..=max_y {
        for px in min_x..=max_x {
            let p = (px as f64 + 0.5, py as f64 + 0.5);
            let w0 = edge_fn(p1, p2, p) * inv_area;
            let w1 = edge_fn(p2, p0, p) * inv_area;
            let w2 = edge_fn(p0, p1, p) * inv_area;

            if w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6 {
                continue;
            }

            let value = v0 * w0 + v1 * w1 + v2 * w2;
            let color = cmap.map(value).to_image_rgba();
            if color.0[3] > 0 {
                img.put_pixel(px as u32, py as u32, color);
            }
        }
    }
}

fn rasterize_rgba_triangle(
    img: &mut RgbaImage,
    p0: (f64, f64),
    c0: Rgba,
    p1: (f64, f64),
    c1: Rgba,
    p2: (f64, f64),
    c2: Rgba,
) {
    if c0.a == 0 && c1.a == 0 && c2.a == 0 {
        return;
    }

    let min_x = p0.0.min(p1.0).min(p2.0).floor().max(0.0) as i32;
    let max_x =
        p0.0.max(p1.0)
            .max(p2.0)
            .ceil()
            .min(img.width() as f64 - 1.0) as i32;
    let min_y = p0.1.min(p1.1).min(p2.1).floor().max(0.0) as i32;
    let max_y =
        p0.1.max(p1.1)
            .max(p2.1)
            .ceil()
            .min(img.height() as f64 - 1.0) as i32;

    if min_x > max_x || min_y > max_y {
        return;
    }

    let area = edge_fn(p0, p1, p2);
    if area.abs() < 1e-9 {
        return;
    }

    let inv_area = 1.0 / area;

    for py in min_y..=max_y {
        for px in min_x..=max_x {
            let p = (px as f64 + 0.5, py as f64 + 0.5);
            let w0 = edge_fn(p1, p2, p) * inv_area;
            let w1 = edge_fn(p2, p0, p) * inv_area;
            let w2 = edge_fn(p0, p1, p) * inv_area;

            if w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6 {
                continue;
            }

            let color = weighted_rgba(c0, w0, c1, w1, c2, w2);
            if color.a > 0 {
                img.put_pixel(px as u32, py as u32, color.to_image_rgba());
            }
        }
    }
}

fn lerp_rgba_pair(left: Rgba, right: Rgba, t: f64) -> Rgba {
    let t = t.clamp(0.0, 1.0);
    Rgba::with_alpha(
        lerp_u8(left.r, right.r, t),
        lerp_u8(left.g, right.g, t),
        lerp_u8(left.b, right.b, t),
        lerp_u8(left.a, right.a, t),
    )
}

fn weighted_rgba(c0: Rgba, w0: f64, c1: Rgba, w1: f64, c2: Rgba, w2: f64) -> Rgba {
    Rgba::with_alpha(
        weighted_u8(c0.r, w0, c1.r, w1, c2.r, w2),
        weighted_u8(c0.g, w0, c1.g, w1, c2.g, w2),
        weighted_u8(c0.b, w0, c1.b, w1, c2.b, w2),
        weighted_u8(c0.a, w0, c1.a, w1, c2.a, w2),
    )
}

fn lerp_u8(left: u8, right: u8, t: f64) -> u8 {
    (left as f64 + (right as f64 - left as f64) * t)
        .round()
        .clamp(0.0, 255.0) as u8
}

fn weighted_u8(v0: u8, w0: f64, v1: u8, w1: f64, v2: u8, w2: f64) -> u8 {
    (v0 as f64 * w0 + v1 as f64 * w1 + v2 as f64 * w2)
        .round()
        .clamp(0.0, 255.0) as u8
}

fn rasterize_mask_triangle(img: &mut RgbaImage, p0: (f64, f64), p1: (f64, f64), p2: (f64, f64)) {
    let min_x = p0.0.min(p1.0).min(p2.0).floor().max(0.0) as i32;
    let max_x =
        p0.0.max(p1.0)
            .max(p2.0)
            .ceil()
            .min(img.width() as f64 - 1.0) as i32;
    let min_y = p0.1.min(p1.1).min(p2.1).floor().max(0.0) as i32;
    let max_y =
        p0.1.max(p1.1)
            .max(p2.1)
            .ceil()
            .min(img.height() as f64 - 1.0) as i32;

    if min_x > max_x || min_y > max_y {
        return;
    }

    let area = edge_fn(p0, p1, p2);
    if area.abs() < 1e-9 {
        return;
    }

    let inv_area = 1.0 / area;
    let opaque = image::Rgba([255, 255, 255, 255]);

    for py in min_y..=max_y {
        for px in min_x..=max_x {
            let p = (px as f64 + 0.5, py as f64 + 0.5);
            let w0 = edge_fn(p1, p2, p) * inv_area;
            let w1 = edge_fn(p2, p0, p) * inv_area;
            let w2 = edge_fn(p0, p1, p) * inv_area;

            if w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6 {
                continue;
            }

            img.put_pixel(px as u32, py as u32, opaque);
        }
    }
}

fn edge_fn(a: (f64, f64), b: (f64, f64), p: (f64, f64)) -> f64 {
    (p.0 - a.0) * (b.1 - a.1) - (p.1 - a.1) * (b.0 - a.0)
}

fn feather_projected_raster_edges(img: &mut RgbaImage) {
    if img.width() < 2 || img.height() < 2 {
        return;
    }

    let src = img.clone();
    let width = src.width() as i32;
    let height = src.height() as i32;

    for py in 0..height {
        for px in 0..width {
            let center = src.get_pixel(px as u32, py as u32);
            let center_alpha = center.0[3];
            let mut transparent_neighbor = false;

            for dy in -1..=1 {
                for dx in -1..=1 {
                    if dx == 0 && dy == 0 {
                        continue;
                    }
                    let nx = px + dx;
                    let ny = py + dy;
                    if nx < 0 || ny < 0 || nx >= width || ny >= height {
                        transparent_neighbor = true;
                        continue;
                    }
                    let neighbor = src.get_pixel(nx as u32, ny as u32);
                    if neighbor.0[3] > 0 {
                    } else {
                        transparent_neighbor = true;
                    }
                }
            }

            if center_alpha > 0 && transparent_neighbor {
                let mut softened = *center;
                softened.0[3] = softened.0[3].min(216);
                img.put_pixel(px as u32, py as u32, softened);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RegularLatLonAxes, rasterize_projected_grid, sample_regular_latlon_grid};
    use crate::color::Rgba;
    use crate::colormap::{Extend, LeveledColormap};

    #[test]
    fn projected_raster_keeps_nan_mask_holes_transparent() {
        let data = [f64::NAN, 1.0, 1.0, 1.0];
        let pixel_points = [
            Some((0.0, 0.0)),
            Some((3.0, 0.0)),
            Some((0.0, 3.0)),
            Some((3.0, 3.0)),
        ];
        let cmap = LeveledColormap::from_palette(
            &[Rgba::new(255, 0, 0)],
            &[0.0, 2.0],
            Extend::Neither,
            None,
        );

        let image = rasterize_projected_grid(&data, 2, 2, &pixel_points, &cmap, 4, 4);

        assert!(
            image.pixels().all(|px| px.0[3] == 0),
            "mixed-validity projected cells should remain masked instead of bleeding a nearby finite value",
        );
    }

    #[test]
    fn partial_wide_longitude_crop_is_not_periodic() {
        let nx = 321;
        let ny = 2;
        let row_lon = (0..nx).map(|i| -140.0 + i as f64).collect::<Vec<_>>();
        let lon = row_lon
            .iter()
            .copied()
            .chain(row_lon.iter().copied())
            .collect::<Vec<_>>();
        let lat = vec![-10.0; nx]
            .into_iter()
            .chain(vec![0.0; nx])
            .collect::<Vec<_>>();
        let data = vec![1.0; nx * ny];

        let axes = RegularLatLonAxes::from_grid(&lat, &lon, ny, nx).unwrap();
        assert!(!axes.periodic_lon);
        assert!(sample_regular_latlon_grid(&data, &axes, -5.0, -170.0).is_none());
    }

    #[test]
    fn full_0_360_axis_samples_negative_longitudes_periodically() {
        let nx = 360;
        let ny = 2;
        let row_lon = (0..nx).map(|i| i as f64).collect::<Vec<_>>();
        let lon = row_lon
            .iter()
            .copied()
            .chain(row_lon.iter().copied())
            .collect::<Vec<_>>();
        let lat = vec![0.0; nx]
            .into_iter()
            .chain(vec![1.0; nx])
            .collect::<Vec<_>>();
        let data = lon.clone();

        let axes = RegularLatLonAxes::from_grid(&lat, &lon, ny, nx).unwrap();
        assert!(axes.periodic_lon);
        let value = sample_regular_latlon_grid(&data, &axes, 0.5, -170.0).unwrap();
        assert!((value - 190.0).abs() < 1.0);
    }
}
