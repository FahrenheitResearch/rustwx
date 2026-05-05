//! GPU port of the `raster_blit` step in `rustwx-render`'s `render.rs`
//! (the per-pixel "source over" composite of a rasterized weather field
//! `map_img` onto the page canvas `img` at offset `(map_x, map_y)`,
//! optionally gated by a `draw_clip_mask`).
//!
//! Each thread = one source pixel. The thread bounds-checks its target
//! canvas coordinate, applies the clip-mask gate if present, then either
//! overwrites (opaque source) or alpha-blends (translucent source) into
//! the canvas. No cross-thread races: the source tile maps 1:1 onto a
//! non-overlapping canvas region.
//!
//! Pixel packing: `u32` little-endian → `R, G, B, A` bytes — same as
//! `image::RgbaImage::as_raw()` on x86_64.
//!
//! The host wrapper mutates `canvas` in place: it uploads the canvas
//! bytes, runs the kernel, then downloads the bytes back over the
//! caller's slice. (For very large canvases the upload/download cost
//! dominates — the kernel itself is trivially memory-bound.)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{
    ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result,
};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/render/raster_blit.cu");
const MODULE_KEY: &str = "render_raster_blit";
const FUNCTION: &str = "raster_blit_kernel";

static UPLOAD_NS: AtomicU64 = AtomicU64::new(0);
static KERNEL_NS: AtomicU64 = AtomicU64::new(0);
static DOWNLOAD_NS: AtomicU64 = AtomicU64::new(0);
static MODULE_NS: AtomicU64 = AtomicU64::new(0);
static N_CALLS: AtomicU64 = AtomicU64::new(0);

fn timing_enabled() -> bool {
    std::env::var("RUSTWX_CUDA_RASTERIZE_TIMING").ok().as_deref() == Some("1")
}

/// Print accumulated per-phase timings to stderr if enabled.
pub fn print_timing_if_enabled() {
    if !timing_enabled() {
        return;
    }
    let n = N_CALLS.load(Ordering::Relaxed).max(1) as f64;
    let to_ms = |ns: u64| (ns as f64 / 1_000_000.0);
    let to_per = |ns: u64| (ns as f64 / 1_000_000.0 / n);
    eprintln!("[raster_blit timing — N={} calls]", n as u64);
    eprintln!("  module    : {:>9.2} ms total ({:>6.2} ms/call)", to_ms(MODULE_NS.load(Ordering::Relaxed)), to_per(MODULE_NS.load(Ordering::Relaxed)));
    eprintln!("  upload    : {:>9.2} ms total ({:>6.2} ms/call)", to_ms(UPLOAD_NS.load(Ordering::Relaxed)), to_per(UPLOAD_NS.load(Ordering::Relaxed)));
    eprintln!("  kernel    : {:>9.2} ms total ({:>6.2} ms/call)", to_ms(KERNEL_NS.load(Ordering::Relaxed)), to_per(KERNEL_NS.load(Ordering::Relaxed)));
    eprintln!("  download  : {:>9.2} ms total ({:>6.2} ms/call)", to_ms(DOWNLOAD_NS.load(Ordering::Relaxed)), to_per(DOWNLOAD_NS.load(Ordering::Relaxed)));
}

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    KernelModule::load(ctx, MODULE_KEY, &with_constants(KERNEL_SRC))
}

/// Default-stream variant. Use `host_on()` from a worker pool to actually
/// overlap with other rasterize kernels.
pub fn host(
    ctx: &ContextHandle,
    src: &[u8],
    canvas: &mut [u8],
    canvas_w: u32,
    canvas_h: u32,
    src_w: u32,
    src_h: u32,
    dst_x: u32,
    dst_y: u32,
    clip_mask: Option<&[u8]>,
) -> Result<()> {
    host_on(
        ctx,
        ctx.stream(),
        src,
        canvas,
        canvas_w,
        canvas_h,
        src_w,
        src_h,
        dst_x,
        dst_y,
        clip_mask,
    )
}

/// Composite `src` onto `canvas` at `(dst_x, dst_y)`, optionally gated by
/// `clip_mask` (same dims as `src`; only the alpha channel is consulted).
/// Mutates `canvas` in place.
///
/// All buffers are packed RGBA8 in `R, G, B, A` byte order — i.e. the
/// layout of `image::RgbaImage::as_raw()`. `src.len()` must equal
/// `4 * src_w * src_h`, `canvas.len()` must equal `4 * canvas_w * canvas_h`,
/// and `clip_mask` (if present) must equal `src.len()`.
#[allow(clippy::too_many_arguments)]
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    src: &[u8],
    canvas: &mut [u8],
    canvas_w: u32,
    canvas_h: u32,
    src_w: u32,
    src_h: u32,
    dst_x: u32,
    dst_y: u32,
    clip_mask: Option<&[u8]>,
) -> Result<()> {
    let timing = timing_enabled();

    let src_pixels = (src_w as usize) * (src_h as usize);
    let canvas_pixels = (canvas_w as usize) * (canvas_h as usize);
    debug_assert_eq!(src.len(), src_pixels * 4);
    debug_assert_eq!(canvas.len(), canvas_pixels * 4);
    if let Some(m) = clip_mask {
        debug_assert_eq!(m.len(), src_pixels * 4);
    }

    if src_pixels == 0 || canvas_pixels == 0 {
        return Ok(());
    }

    // Reinterpret byte slices as packed u32 RGBA — same trick used in
    // `downsample.rs`. Safe as long as the underlying allocation is at
    // least 4-byte-aligned, which `Vec<u8>` and `image::RgbaImage::as_raw`
    // are in practice on all relevant targets. (We never write through
    // the byte slices while the u32 view is live.)
    let src_u32: &[u32] =
        unsafe { std::slice::from_raw_parts(src.as_ptr() as *const u32, src_pixels) };
    let canvas_u32: &[u32] =
        unsafe { std::slice::from_raw_parts(canvas.as_ptr() as *const u32, canvas_pixels) };
    let mask_u32: Option<&[u32]> = clip_mask.map(|m| unsafe {
        std::slice::from_raw_parts(m.as_ptr() as *const u32, src_pixels)
    });

    // ---- upload ----
    let t_up = if timing { Some(Instant::now()) } else { None };
    let src_d = DeviceVec::from_host_on(stream, src_u32)?;
    let mut canvas_d = DeviceVec::from_host_on(stream, canvas_u32)?;
    // For the no-mask case we still need *some* device pointer to pass —
    // a 1-element zero buffer is the cheapest valid choice and the kernel
    // ignores the contents because `has_clip_mask = 0`.
    let mask_d: DeviceVec<u32> = if let Some(m) = mask_u32 {
        DeviceVec::from_host_on(stream, m)?
    } else {
        DeviceVec::zeros_on(stream, 1)?
    };
    if let Some(t) = t_up {
        UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // ---- module / function ----
    let t_mod = if timing { Some(Instant::now()) } else { None };
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    if let Some(t) = t_mod {
        MODULE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // ---- launch ----
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((src_w + bx - 1) / bx, (src_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let has_clip_mask_i: i32 = if clip_mask.is_some() { 1 } else { 0 };
    let src_w_i: i32 = src_w as i32;
    let src_h_i: i32 = src_h as i32;
    let canvas_w_i: i32 = canvas_w as i32;
    let canvas_h_i: i32 = canvas_h as i32;
    let dst_x_i: i32 = dst_x as i32;
    let dst_y_i: i32 = dst_y as i32;

    let t_k = if timing { Some(Instant::now()) } else { None };
    let mut builder = stream.launch_builder(&func);
    builder
        .arg(src_d.slice())
        .arg(mask_d.slice())
        .arg(&has_clip_mask_i)
        .arg(canvas_d.slice_mut())
        .arg(&src_w_i)
        .arg(&src_h_i)
        .arg(&canvas_w_i)
        .arg(&canvas_h_i)
        .arg(&dst_x_i)
        .arg(&dst_y_i);
    unsafe { builder.launch(cfg)? };
    if let Some(t) = t_k {
        KERNEL_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // ---- download (back into the caller's `canvas` slice) ----
    let t_d = if timing { Some(Instant::now()) } else { None };
    let out_u32 = canvas_d.copy_to_host_on(stream)?;
    debug_assert_eq!(out_u32.len(), canvas_pixels);
    // SAFETY: same alignment caveat as above — we copy whole pixels back.
    let out_bytes: &[u8] =
        unsafe { std::slice::from_raw_parts(out_u32.as_ptr() as *const u8, canvas_pixels * 4) };
    canvas.copy_from_slice(out_bytes);
    if let Some(t) = t_d {
        DOWNLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    if timing {
        N_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

/// Same as `host_on` but composites a device-resident `src` onto a
/// device-resident `canvas` in place. No PCIe transfers — the caller has
/// already uploaded both buffers (and the optional clip mask). Used by
/// the canvas-resident pipeline (`draw_variable_layers_gpu`).
///
/// `src` length must equal `src_w * src_h`; `canvas` length must equal
/// `canvas_w * canvas_h`; `clip_mask` (if `Some`) must equal `src` length.
#[allow(clippy::too_many_arguments)]
pub fn host_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    src: &DeviceVec<u32>,
    canvas: &mut DeviceVec<u32>,
    canvas_w: u32,
    canvas_h: u32,
    src_w: u32,
    src_h: u32,
    dst_x: u32,
    dst_y: u32,
    clip_mask: Option<&DeviceVec<u32>>,
) -> Result<()> {
    let src_pixels = (src_w as usize) * (src_h as usize);
    let canvas_pixels = (canvas_w as usize) * (canvas_h as usize);
    if src_pixels == 0 || canvas_pixels == 0 {
        return Ok(());
    }

    // Need *some* device pointer to pass when the caller has no mask;
    // a 1-element zero buffer is the cheapest valid choice and the kernel
    // ignores its contents because `has_clip_mask = 0`.
    let scratch_mask: Option<DeviceVec<u32>> = if clip_mask.is_none() {
        Some(DeviceVec::zeros_on(stream, 1)?)
    } else {
        None
    };
    let mask_d: &DeviceVec<u32> = clip_mask.unwrap_or_else(|| scratch_mask.as_ref().unwrap());

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg = LaunchCfg {
        grid_dim: ((src_w + bx - 1) / bx, (src_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let has_clip_mask_i: i32 = if clip_mask.is_some() { 1 } else { 0 };
    let src_w_i: i32 = src_w as i32;
    let src_h_i: i32 = src_h as i32;
    let canvas_w_i: i32 = canvas_w as i32;
    let canvas_h_i: i32 = canvas_h as i32;
    let dst_x_i: i32 = dst_x as i32;
    let dst_y_i: i32 = dst_y as i32;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(src.slice())
        .arg(mask_d.slice())
        .arg(&has_clip_mask_i)
        .arg(canvas.slice_mut())
        .arg(&src_w_i)
        .arg(&src_h_i)
        .arg(&canvas_w_i)
        .arg(&canvas_h_i)
        .arg(&dst_x_i)
        .arg(&dst_y_i);
    unsafe { builder.launch(cfg)? };

    if timing_enabled() {
        N_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Inlined CPU reference of the raster_blit loop + `blend_pixel` body
    /// (rustwx-render/src/draw.rs). Drift detector for upstream changes.
    fn cpu_ref(
        src: &[u8],
        canvas: &mut [u8],
        canvas_w: u32,
        canvas_h: u32,
        src_w: u32,
        src_h: u32,
        dst_x: u32,
        dst_y: u32,
        clip_mask: Option<&[u8]>,
    ) {
        for sy in 0..src_h {
            for sx in 0..src_w {
                let s_idx = (sy as usize * src_w as usize + sx as usize) * 4;
                if let Some(mask) = clip_mask {
                    if mask[s_idx + 3] == 0 {
                        continue;
                    }
                }
                let sr = src[s_idx];
                let sg = src[s_idx + 1];
                let sb = src[s_idx + 2];
                let sa = src[s_idx + 3];
                if sa == 0 {
                    continue;
                }
                let cx = dst_x + sx;
                let cy = dst_y + sy;
                if cx >= canvas_w || cy >= canvas_h {
                    continue;
                }
                let c_idx = (cy as usize * canvas_w as usize + cx as usize) * 4;
                if sa == 255 {
                    canvas[c_idx] = sr;
                    canvas[c_idx + 1] = sg;
                    canvas[c_idx + 2] = sb;
                    canvas[c_idx + 3] = 255;
                } else {
                    let dr = canvas[c_idx];
                    let dg = canvas[c_idx + 1];
                    let db = canvas[c_idx + 2];
                    let alpha = sa as f64 / 255.0;
                    let inv = 1.0 - alpha;
                    canvas[c_idx] = (sr as f64 * alpha + dr as f64 * inv).round() as u8;
                    canvas[c_idx + 1] = (sg as f64 * alpha + dg as f64 * inv).round() as u8;
                    canvas[c_idx + 2] = (sb as f64 * alpha + db as f64 * inv).round() as u8;
                    canvas[c_idx + 3] = 255;
                }
            }
        }
    }

    #[test]
    fn raster_blit_matches_cpu_within_one_lsb() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e})");
                return;
            }
        };
        let stream = ctx.new_stream().expect("stream");

        let canvas_w: u32 = 320;
        let canvas_h: u32 = 240;
        let src_w: u32 = 200;
        let src_h: u32 = 150;
        let dst_x: u32 = 60;
        let dst_y: u32 = 40;

        // Synthetic canvas: deterministic gradient, fully opaque.
        let mut canvas_cpu = vec![0u8; (canvas_w as usize) * (canvas_h as usize) * 4];
        for y in 0..canvas_h {
            for x in 0..canvas_w {
                let off = (y as usize * canvas_w as usize + x as usize) * 4;
                canvas_cpu[off] = ((x * 3) % 256) as u8;
                canvas_cpu[off + 1] = ((y * 5) % 256) as u8;
                canvas_cpu[off + 2] = ((x ^ y) % 256) as u8;
                canvas_cpu[off + 3] = 255;
            }
        }
        let mut canvas_gpu = canvas_cpu.clone();

        // Synthetic source: varied alpha (covering 0, 1, 128, 200, 255 etc.)
        // and varied RGB. Exercises the skip-on-zero, fast-path-on-255,
        // and the float-blend code paths.
        let mut src = vec![0u8; (src_w as usize) * (src_h as usize) * 4];
        for y in 0..src_h {
            for x in 0..src_w {
                let off = (y as usize * src_w as usize + x as usize) * 4;
                src[off] = ((x * 7) % 256) as u8;
                src[off + 1] = ((y * 11) % 256) as u8;
                src[off + 2] = ((x + y) % 256) as u8;
                // Distribute alphas across the full range, including
                // the fully-transparent and fully-opaque fast-paths.
                let a = match (x + y) % 8 {
                    0 => 0,
                    1 => 1,
                    2 => 64,
                    3 => 128,
                    4 => 200,
                    5 => 254,
                    6 => 255,
                    _ => ((x * 13 + y * 17) % 256) as u8,
                };
                src[off + 3] = a;
            }
        }

        // Synthetic clip mask: punch a diagonal stripe of zero-alpha so a
        // chunk of source pixels are gated out. RGB values must not
        // matter — verify by setting them to garbage.
        let mut mask = vec![0u8; src.len()];
        for y in 0..src_h {
            for x in 0..src_w {
                let off = (y as usize * src_w as usize + x as usize) * 4;
                mask[off] = 99;
                mask[off + 1] = 33;
                mask[off + 2] = 7;
                let in_stripe = ((x as i32) - (y as i32)).rem_euclid(20) < 6;
                mask[off + 3] = if in_stripe { 0 } else { 255 };
            }
        }

        // CPU + GPU runs with the mask.
        cpu_ref(
            &src,
            &mut canvas_cpu,
            canvas_w,
            canvas_h,
            src_w,
            src_h,
            dst_x,
            dst_y,
            Some(&mask),
        );
        host_on(
            &ctx,
            &stream,
            &src,
            &mut canvas_gpu,
            canvas_w,
            canvas_h,
            src_w,
            src_h,
            dst_x,
            dst_y,
            Some(&mask),
        )
        .expect("CUDA raster_blit failed");

        assert_eq!(canvas_cpu.len(), canvas_gpu.len());
        let n = (canvas_w as usize) * (canvas_h as usize);
        let mut diff_pixels = 0usize;
        let mut max_chan_delta: u8 = 0;
        for p in 0..n {
            let off = p * 4;
            if canvas_cpu[off..off + 4] != canvas_gpu[off..off + 4] {
                diff_pixels += 1;
                for c in 0..4 {
                    let d = (canvas_cpu[off + c] as i32 - canvas_gpu[off + c] as i32)
                        .unsigned_abs() as u8;
                    if d > max_chan_delta {
                        max_chan_delta = d;
                    }
                }
            }
        }
        let pct = 100.0 * diff_pixels as f64 / n as f64;
        eprintln!(
            "raster_blit: {} / {} pixels differ ({:.3}%), max channel delta = {}",
            diff_pixels, n, pct, max_chan_delta
        );
        assert!(
            max_chan_delta <= 1,
            "max channel delta {} > 1 — blend math drift",
            max_chan_delta
        );
    }
}
