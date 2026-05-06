//! GPU port of `rustwx_render::rasterize::rasterize_projected_grid` —
//! triangle-fill rasterization of a projected mesh onto an RGBA image.
//!
//! Layout: one CUDA thread per `(j, i)` input quad. Each thread fills both
//! triangles of its quad. Adjacent quads can race on shared-edge pixels —
//! we accept that since the field is smoothly-varying and the two
//! interpolated values differ by far less than 1/255 in well-behaved data.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result};

use crate::colormap::ColormapHostView;
use crate::sources::with_constants;

/// Aggregate ns spent in each phase. Set `RUSTWX_CUDA_RASTERIZE_TIMING=1`
/// at process start to enable; print with `print_timing_if_enabled`.
static FLATTEN_NS: AtomicU64 = AtomicU64::new(0);
static UPLOAD_NS: AtomicU64 = AtomicU64::new(0);
static KERNEL_NS: AtomicU64 = AtomicU64::new(0);
static DOWNLOAD_NS: AtomicU64 = AtomicU64::new(0);
static MODULE_NS: AtomicU64 = AtomicU64::new(0);
static N_CALLS: AtomicU64 = AtomicU64::new(0);
static MESH_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static MESH_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

fn timing_enabled() -> bool {
    std::env::var("RUSTWX_CUDA_RASTERIZE_TIMING")
        .ok()
        .as_deref()
        == Some("1")
}

/// Print accumulated per-phase timings + cache hit rate to stderr if enabled.
pub fn print_timing_if_enabled() {
    if !timing_enabled() {
        return;
    }
    let n = N_CALLS.load(Ordering::Relaxed).max(1) as f64;
    let to_ms = |ns: u64| (ns as f64 / 1_000_000.0);
    let to_ms_per = |ns: u64| (ns as f64 / 1_000_000.0 / n);
    eprintln!("[rasterize_projected_grid timing — N={} calls]", n as u64);
    eprintln!(
        "  flatten   : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(FLATTEN_NS.load(Ordering::Relaxed)),
        to_ms_per(FLATTEN_NS.load(Ordering::Relaxed))
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
    let hits = MESH_CACHE_HITS.load(Ordering::Relaxed);
    let miss = MESH_CACHE_MISSES.load(Ordering::Relaxed);
    eprintln!(
        "  mesh cache: {} hits / {} misses (hit rate {:.1}%)",
        hits,
        miss,
        100.0 * hits as f64 / (hits + miss).max(1) as f64
    );
}

/// Global cache for projected-mesh device buffers, shared across all rayon
/// workers. Within a single hour pass ~78+ recipes share one projected
/// mesh — caching globally instead of per-thread takes the hit rate from
/// ~44% (one miss per thread) to >99% (one miss total) and cuts ~960 MB
/// of redundant GPU residency to ~30 MB.
///
/// Cross-thread safety: after the uploading thread finishes the H2D copy
/// we call `stream.synchronize()` so the bytes are visible on the device
/// before any other thread's kernel reads them. CUDA allows multiple
/// streams to read the same device buffer concurrently as long as it's
/// already populated.
struct CachedMesh {
    /// Total point count — first part of the cache key. Cheap to compare.
    key_len: usize,
    /// Content hash sampled across the slice — second part of the cache key.
    /// Tuned for low collision risk across recipes that share a projection.
    fingerprint: u64,
    pix_x: Arc<DeviceVec<f64>>,
    pix_y: Arc<DeviceVec<f64>>,
    valid: Arc<DeviceVec<i32>>,
}

// `DeviceVec`'s inner `CudaSlice` carries an `Arc<CudaStream>` which is
// `Send + Sync`; the cache itself only ever reads device buffer pointers
// from the kernel, never from Rust, so global sharing is safe.
unsafe impl Send for CachedMesh {}
unsafe impl Sync for CachedMesh {}

static MESH_CACHE: once_cell::sync::Lazy<parking_lot::RwLock<Vec<Arc<CachedMesh>>>> =
    once_cell::sync::Lazy::new(|| parking_lot::RwLock::new(Vec::new()));

fn fingerprint(pp: &[Option<(f64, f64)>]) -> u64 {
    // Content-based hash: 32 sample points spread across the slice +
    // length + first/last. Identical-content meshes from different
    // recipes hash equal regardless of slice pointer; collision risk on
    // realistic projected meshes is negligible (lat/lon coords vary by
    // far more than fingerprint resolution).
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let n = pp.len();
    n.hash(&mut h);
    let probes = 32usize;
    for k in 0..probes {
        let i = if probes <= 1 {
            0
        } else {
            (k * (n.saturating_sub(1))) / (probes - 1)
        };
        if i < n {
            match &pp[i] {
                Some((x, y)) => {
                    1u8.hash(&mut h);
                    x.to_bits().hash(&mut h);
                    y.to_bits().hash(&mut h);
                }
                None => 0u8.hash(&mut h),
            }
        }
    }
    h.finish()
}

const KERNEL_SRC: &str = include_str!("../../../kernels/render/rasterize_projected_grid.cu");
const MODULE_KEY: &str = "render_rasterize_projected_grid";
const FUNCTION: &str = "rasterize_projected_grid_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Rasterize `data` onto a projected mesh. Default-stream variant; calls
/// from multiple CPU threads will serialize on the GPU. Use `host_on()` for
/// per-thread streams that actually overlap.
pub fn host(
    ctx: &ContextHandle,
    data: &[f64],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
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
        pixel_points,
        cmap,
        img_w,
        img_h,
    )
}

/// Same as `host()` but routes all device ops through the caller-supplied
/// stream. Pair with a thread-local stream cache so each rayon worker has
/// its own — multiple non-default streams run concurrently on the device.
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<Vec<u8>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    if ny < 2 || nx < 2 || pixel_points.len() != ny * nx || data.len() != ny * nx {
        return Ok(vec![0u8; n_pixels * 4]);
    }

    let timing = timing_enabled();
    let key_len = pixel_points.len();
    let want_fp = fingerprint(pixel_points);

    // Cheap read-locked lookup first.
    let cached_hit = {
        let r = MESH_CACHE.read();
        r.iter()
            .find(|c| c.key_len == key_len && c.fingerprint == want_fp)
            .cloned()
    };

    let cached = if let Some(c) = cached_hit {
        if timing {
            MESH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        }
        c
    } else {
        // Miss path: take the write lock, double-check (another thread may
        // have inserted while we waited), else flatten + upload + sync +
        // insert.
        let mut w = MESH_CACHE.write();
        if let Some(c) = w
            .iter()
            .find(|c| c.key_len == key_len && c.fingerprint == want_fp)
            .cloned()
        {
            if timing {
                MESH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            }
            c
        } else {
            if timing {
                MESH_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
            }
            let t0 = if timing { Some(Instant::now()) } else { None };
            let n_grid = ny * nx;
            let mut pix_x: Vec<f64> = Vec::with_capacity(n_grid);
            let mut pix_y: Vec<f64> = Vec::with_capacity(n_grid);
            let mut valid: Vec<i32> = Vec::with_capacity(n_grid);
            for pp in pixel_points {
                match pp {
                    Some((x, y)) => {
                        pix_x.push(*x);
                        pix_y.push(*y);
                        valid.push(1);
                    }
                    None => {
                        pix_x.push(0.0);
                        pix_y.push(0.0);
                        valid.push(0);
                    }
                }
            }
            if let Some(t) = t0 {
                FLATTEN_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            let t1 = if timing { Some(Instant::now()) } else { None };
            let pix_x_d = Arc::new(DeviceVec::from_host_on(stream, &pix_x)?);
            let pix_y_d = Arc::new(DeviceVec::from_host_on(stream, &pix_y)?);
            let valid_d = Arc::new(DeviceVec::from_host_on(stream, &valid)?);
            // Block until the mesh upload is visible on the device. Other
            // threads' kernels can then safely read these buffers from any
            // stream. Trivially fast (~1 ms for 30 MB on PCIe Gen5).
            stream.synchronize()?;
            if let Some(t) = t1 {
                UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            let arc = Arc::new(CachedMesh {
                key_len,
                fingerprint: want_fp,
                pix_x: pix_x_d,
                pix_y: pix_y_d,
                valid: valid_d,
            });
            // Bound cache size; keep the 16 most recent unique meshes
            // (typical pipeline only ever has 1-2 active at a time).
            if w.len() >= 16 {
                w.remove(0);
            }
            w.push(Arc::clone(&arc));
            arc
        }
    };

    let pix_x_arc = Arc::clone(&cached.pix_x);
    let pix_y_arc = Arc::clone(&cached.pix_y);
    let valid_arc = Arc::clone(&cached.valid);

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

    let t_mod = if timing { Some(Instant::now()) } else { None };
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    if let Some(t) = t_mod {
        MODULE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // Quad grid: (nx-1) × (ny-1). 16x16 blocks → ~256 threads/block.
    let qnx = (nx - 1) as u32;
    let qny = (ny - 1) as u32;
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((qnx + bx - 1) / bx, (qny + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let t_k = if timing { Some(Instant::now()) } else { None };
    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(pix_x_arc.slice())
        .arg(pix_y_arc.slice())
        .arg(valid_arc.slice())
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
        // Note: this measures launch dispatch, not kernel exec. Real exec
        // time is folded into the synchronous download below.
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
pub fn host_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &[f64],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<DeviceVec<u32>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    if ny < 2 || nx < 2 || pixel_points.len() != ny * nx || data.len() != ny * nx {
        return DeviceVec::zeros_on(stream, n_pixels);
    }

    let timing = timing_enabled();
    let key_len = pixel_points.len();
    let want_fp = fingerprint(pixel_points);

    let cached_hit = {
        let r = MESH_CACHE.read();
        r.iter()
            .find(|c| c.key_len == key_len && c.fingerprint == want_fp)
            .cloned()
    };

    let cached = if let Some(c) = cached_hit {
        if timing {
            MESH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        }
        c
    } else {
        let mut w = MESH_CACHE.write();
        if let Some(c) = w
            .iter()
            .find(|c| c.key_len == key_len && c.fingerprint == want_fp)
            .cloned()
        {
            if timing {
                MESH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            }
            c
        } else {
            if timing {
                MESH_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
            }
            let t0 = if timing { Some(Instant::now()) } else { None };
            let n_grid = ny * nx;
            let mut pix_x: Vec<f64> = Vec::with_capacity(n_grid);
            let mut pix_y: Vec<f64> = Vec::with_capacity(n_grid);
            let mut valid: Vec<i32> = Vec::with_capacity(n_grid);
            for pp in pixel_points {
                match pp {
                    Some((x, y)) => {
                        pix_x.push(*x);
                        pix_y.push(*y);
                        valid.push(1);
                    }
                    None => {
                        pix_x.push(0.0);
                        pix_y.push(0.0);
                        valid.push(0);
                    }
                }
            }
            if let Some(t) = t0 {
                FLATTEN_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            let t1 = if timing { Some(Instant::now()) } else { None };
            let pix_x_d = Arc::new(DeviceVec::from_host_on(stream, &pix_x)?);
            let pix_y_d = Arc::new(DeviceVec::from_host_on(stream, &pix_y)?);
            let valid_d = Arc::new(DeviceVec::from_host_on(stream, &valid)?);
            stream.synchronize()?;
            if let Some(t) = t1 {
                UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            let arc = Arc::new(CachedMesh {
                key_len,
                fingerprint: want_fp,
                pix_x: pix_x_d,
                pix_y: pix_y_d,
                valid: valid_d,
            });
            if w.len() >= 16 {
                w.remove(0);
            }
            w.push(Arc::clone(&arc));
            arc
        }
    };

    let pix_x_arc = Arc::clone(&cached.pix_x);
    let pix_y_arc = Arc::clone(&cached.pix_y);
    let valid_arc = Arc::clone(&cached.valid);

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

    let t_mod = if timing { Some(Instant::now()) } else { None };
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    if let Some(t) = t_mod {
        MODULE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let qnx = (nx - 1) as u32;
    let qny = (ny - 1) as u32;
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((qnx + bx - 1) / bx, (qny + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let t_k = if timing { Some(Instant::now()) } else { None };
    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(pix_x_arc.slice())
        .arg(pix_y_arc.slice())
        .arg(valid_arc.slice())
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

/// Device-resident sibling of `host_into_device_on`. Takes an already-on-
/// device `DeviceVec<f64>` instead of a host slice — the caller is presumed
/// to have produced it via the GPU GRIB decoder (`rustwx-cuda-grib`) and so
/// no PCIe upload is required for the data layer. Mesh + colormap upload
/// paths are unchanged (mesh is cached, colormap is small).
///
/// `data` length must equal `ny * nx`. Same `DeviceVec<u32>` output as
/// `host_into_device_on`.
pub fn device_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    data: &DeviceVec<f64>,
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    cmap: ColormapHostView<'_>,
    img_w: u32,
    img_h: u32,
) -> Result<DeviceVec<u32>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);

    if ny < 2 || nx < 2 || pixel_points.len() != ny * nx || data.len != ny * nx {
        return DeviceVec::zeros_on(stream, n_pixels);
    }

    let timing = timing_enabled();
    let key_len = pixel_points.len();
    let want_fp = fingerprint(pixel_points);

    let cached_hit = {
        let r = MESH_CACHE.read();
        r.iter()
            .find(|c| c.key_len == key_len && c.fingerprint == want_fp)
            .cloned()
    };

    let cached = if let Some(c) = cached_hit {
        if timing {
            MESH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        }
        c
    } else {
        let mut w = MESH_CACHE.write();
        if let Some(c) = w
            .iter()
            .find(|c| c.key_len == key_len && c.fingerprint == want_fp)
            .cloned()
        {
            if timing {
                MESH_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            }
            c
        } else {
            if timing {
                MESH_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
            }
            let t0 = if timing { Some(Instant::now()) } else { None };
            let n_grid = ny * nx;
            let mut pix_x: Vec<f64> = Vec::with_capacity(n_grid);
            let mut pix_y: Vec<f64> = Vec::with_capacity(n_grid);
            let mut valid: Vec<i32> = Vec::with_capacity(n_grid);
            for pp in pixel_points {
                match pp {
                    Some((x, y)) => {
                        pix_x.push(*x);
                        pix_y.push(*y);
                        valid.push(1);
                    }
                    None => {
                        pix_x.push(0.0);
                        pix_y.push(0.0);
                        valid.push(0);
                    }
                }
            }
            if let Some(t) = t0 {
                FLATTEN_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            let t1 = if timing { Some(Instant::now()) } else { None };
            let pix_x_d = Arc::new(DeviceVec::from_host_on(stream, &pix_x)?);
            let pix_y_d = Arc::new(DeviceVec::from_host_on(stream, &pix_y)?);
            let valid_d = Arc::new(DeviceVec::from_host_on(stream, &valid)?);
            stream.synchronize()?;
            if let Some(t) = t1 {
                UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            let arc = Arc::new(CachedMesh {
                key_len,
                fingerprint: want_fp,
                pix_x: pix_x_d,
                pix_y: pix_y_d,
                valid: valid_d,
            });
            if w.len() >= 16 {
                w.remove(0);
            }
            w.push(Arc::clone(&arc));
            arc
        }
    };

    let pix_x_arc = Arc::clone(&cached.pix_x);
    let pix_y_arc = Arc::clone(&cached.pix_y);
    let valid_arc = Arc::clone(&cached.valid);

    // No data upload — caller's `data` is already on device.
    let t_up = if timing { Some(Instant::now()) } else { None };
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

    let t_mod = if timing { Some(Instant::now()) } else { None };
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    if let Some(t) = t_mod {
        MODULE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let qnx = (nx - 1) as u32;
    let qny = (ny - 1) as u32;
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((qnx + bx - 1) / bx, (qny + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let ny_i: i32 = ny as i32;
    let nx_i: i32 = nx as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let t_k = if timing { Some(Instant::now()) } else { None };
    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data.slice())
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(pix_x_arc.slice())
        .arg(pix_y_arc.slice())
        .arg(valid_arc.slice())
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

    /// Inlined CPU reference of `rasterize_projected_grid` +
    /// `rasterize_triangle`. Drift-detector for upstream changes.
    fn cpu_ref(
        data: &[f64],
        ny: usize,
        nx: usize,
        pixel_points: &[Option<(f64, f64)>],
        levels: &[f64],
        colors: &[u32],
        under_color: Option<u32>,
        over_color: Option<u32>,
        mask_below: Option<f64>,
        img_w: u32,
        img_h: u32,
    ) -> Vec<u8> {
        let mut out = vec![0u8; (img_w as usize) * (img_h as usize) * 4];
        if ny < 2 || nx < 2 || pixel_points.len() != ny * nx {
            return out;
        }
        for j in 0..(ny - 1) {
            for i in 0..(nx - 1) {
                let p00 = pixel_points[j * nx + i];
                let p10 = pixel_points[j * nx + i + 1];
                let p01 = pixel_points[(j + 1) * nx + i];
                let p11 = pixel_points[(j + 1) * nx + i + 1];
                let (p00, p10, p01, p11) = match (p00, p10, p01, p11) {
                    (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
                    _ => continue,
                };
                let v00 = data[j * nx + i];
                let v10 = data[j * nx + i + 1];
                let v01 = data[(j + 1) * nx + i];
                let v11 = data[(j + 1) * nx + i + 1];
                tri_ref(
                    &mut out,
                    p00,
                    v00,
                    p10,
                    v10,
                    p11,
                    v11,
                    levels,
                    colors,
                    under_color,
                    over_color,
                    mask_below,
                    img_w,
                    img_h,
                );
                tri_ref(
                    &mut out,
                    p00,
                    v00,
                    p11,
                    v11,
                    p01,
                    v01,
                    levels,
                    colors,
                    under_color,
                    over_color,
                    mask_below,
                    img_w,
                    img_h,
                );
            }
        }
        out
    }

    fn tri_ref(
        out: &mut [u8],
        p0: (f64, f64),
        v0: f64,
        p1: (f64, f64),
        v1: f64,
        p2: (f64, f64),
        v2: f64,
        levels: &[f64],
        colors: &[u32],
        under_color: Option<u32>,
        over_color: Option<u32>,
        mask_below: Option<f64>,
        img_w: u32,
        img_h: u32,
    ) {
        if !v0.is_finite() || !v1.is_finite() || !v2.is_finite() {
            return;
        }
        let min_x = p0.0.min(p1.0).min(p2.0).floor().max(0.0) as i32;
        let max_x = p0.0.max(p1.0).max(p2.0).ceil().min(img_w as f64 - 1.0) as i32;
        let min_y = p0.1.min(p1.1).min(p2.1).floor().max(0.0) as i32;
        let max_y = p0.1.max(p1.1).max(p2.1).ceil().min(img_h as f64 - 1.0) as i32;
        if min_x > max_x || min_y > max_y {
            return;
        }
        let area = ef(p0, p1, p2);
        if area.abs() < 1e-9 {
            return;
        }
        let inv_area = 1.0 / area;
        for py in min_y..=max_y {
            for px in min_x..=max_x {
                let p = (px as f64 + 0.5, py as f64 + 0.5);
                let w0 = ef(p1, p2, p) * inv_area;
                let w1 = ef(p2, p0, p) * inv_area;
                let w2 = ef(p0, p1, p) * inv_area;
                if w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6 {
                    continue;
                }
                let value = v0 * w0 + v1 * w1 + v2 * w2;
                let packed = lookup(value, levels, colors, under_color, over_color, mask_below);
                if (packed >> 24) & 0xFF == 0 {
                    continue;
                }
                let off = (py as usize * img_w as usize + px as usize) * 4;
                out[off] = (packed) as u8;
                out[off + 1] = (packed >> 8) as u8;
                out[off + 2] = (packed >> 16) as u8;
                out[off + 3] = (packed >> 24) as u8;
            }
        }
    }

    fn ef(a: (f64, f64), b: (f64, f64), p: (f64, f64)) -> f64 {
        (p.0 - a.0) * (b.1 - a.1) - (p.1 - a.1) * (b.0 - a.0)
    }

    fn lookup(
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
        if levels.is_empty() || colors.is_empty() {
            return 0;
        }
        if value < levels[0] {
            return under_color.unwrap_or(0);
        }
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

    /// Verify on a synthetic warped mesh: roughly 80x60 grid with a smooth
    /// projection-like transform + a small irregular tilt, rendered at
    /// 480x360 pixels. Tolerate ≤ 1% disagreeing pixels and ≤ 8/255 max
    /// channel delta — the race-on-shared-edges effect described in the
    /// kernel comment.
    #[test]
    fn rasterize_projected_grid_matches_cpu_within_tolerance() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e})");
                return;
            }
        };

        let ny = 60;
        let nx = 80;
        let img_w: u32 = 480;
        let img_h: u32 = 360;

        // Smooth field
        let mut data = vec![0.0; ny * nx];
        for j in 0..ny {
            for i in 0..nx {
                let x = i as f64 / (nx - 1) as f64;
                let y = j as f64 / (ny - 1) as f64;
                data[j * nx + i] = (x * 6.28).sin() * 50.0 + (y * 4.71).cos() * 30.0;
            }
        }

        // Projected mesh: linear remap with slight pincushion warp +
        // a few `None` holes to exercise the valid-mask path.
        let mut pix: Vec<Option<(f64, f64)>> = Vec::with_capacity(ny * nx);
        for j in 0..ny {
            for i in 0..nx {
                let u = i as f64 / (nx - 1) as f64;
                let v = j as f64 / (ny - 1) as f64;
                let warp = 0.05 * ((u - 0.5).powi(2) + (v - 0.5).powi(2));
                let xp = (u + warp) * (img_w as f64 - 1.0);
                let yp = (v + warp) * (img_h as f64 - 1.0);
                if (i + j) == 17 || (i == 5 && j == 5) {
                    pix.push(None);
                } else {
                    pix.push(Some((xp, yp)));
                }
            }
        }

        let levels: Vec<f64> = vec![-100.0, -50.0, -25.0, 0.0, 25.0, 50.0, 100.0];
        let colors: Vec<u32> = (0..(levels.len() - 1))
            .map(|k| {
                let t = k as f64 / ((levels.len() - 2) as f64);
                pack_rgba((255.0 * t) as u8, 100, (255.0 * (1.0 - t)) as u8, 255)
            })
            .collect();
        let under = Some(pack_rgba(0, 0, 0, 255));
        let over = Some(pack_rgba(255, 255, 255, 255));

        let cpu = cpu_ref(
            &data, ny, nx, &pix, &levels, &colors, under, over, None, img_w, img_h,
        );
        let view = ColormapHostView {
            levels: &levels,
            colors_packed: &colors,
            under_color: under,
            over_color: over,
            mask_below: None,
        };
        let gpu = host(&ctx, &data, ny, nx, &pix, view, img_w, img_h)
            .expect("CUDA rasterize_projected_grid failed");

        assert_eq!(cpu.len(), gpu.len());
        let n_pixels = (img_w as usize) * (img_h as usize);

        let mut diff_pixels = 0usize;
        let mut max_chan_delta = 0u8;
        for p in 0..n_pixels {
            let off = p * 4;
            let same = cpu[off..off + 4] == gpu[off..off + 4];
            if !same {
                diff_pixels += 1;
                for c in 0..4 {
                    let d = (cpu[off + c] as i32 - gpu[off + c] as i32).unsigned_abs() as u8;
                    if d > max_chan_delta {
                        max_chan_delta = d;
                    }
                }
            }
        }

        let pct = (diff_pixels as f64) * 100.0 / (n_pixels as f64);
        eprintln!(
            "projected_grid: {} / {} pixels differ ({:.3}%), max channel delta = {}",
            diff_pixels, n_pixels, pct, max_chan_delta
        );

        // Bar:
        //   * <= 1% of pixels differ (only shared-edge race candidates)
        //   * <= 8/255 max channel delta (one colormap step in worst case)
        assert!(
            pct <= 1.0,
            "{:.3}% pixels differ — exceeds 1% tolerance",
            pct
        );
        assert!(
            max_chan_delta <= 8,
            "max channel delta {} > 8",
            max_chan_delta
        );
    }
}
