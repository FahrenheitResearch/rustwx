//! GPU port of `rustwx_render`'s post-render `downsample + sharpen` pass:
//!   1. Lanczos3 resize (matches `image::imageops::resize` w/ Lanczos3),
//!   2. 3x3 unsharp-mask (matches `sharpen_downsampled_image`).
//!
//! Operates on packed-RGBA8 byte buffers — same byte layout as
//! `image::RgbaImage::as_raw()` — so the swap site can lift a CPU canvas
//! to the GPU, run both passes back-to-back without round-tripping, and
//! download the final image once.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result};

use crate::sources::with_constants;

const DOWNSAMPLE_SRC: &str = include_str!("../../../kernels/render/downsample_lanczos3.cu");
const DOWNSAMPLE_KEY: &str = "render_downsample_lanczos3";
const DOWNSAMPLE_FN: &str = "downsample_lanczos3_kernel";

const SHARPEN_SRC: &str = include_str!("../../../kernels/render/sharpen_3x3.cu");
const SHARPEN_KEY: &str = "render_sharpen_3x3";
const SHARPEN_FN: &str = "sharpen_3x3_kernel";

static UPLOAD_NS: AtomicU64 = AtomicU64::new(0);
static DOWNSAMPLE_NS: AtomicU64 = AtomicU64::new(0);
static SHARPEN_NS: AtomicU64 = AtomicU64::new(0);
static DOWNLOAD_NS: AtomicU64 = AtomicU64::new(0);
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
    let to_ms = |ns: u64| (ns as f64 / 1_000_000.0);
    let to_per = |ns: u64| (ns as f64 / 1_000_000.0 / n);
    eprintln!("[downsample+sharpen timing — N={} calls]", n as u64);
    eprintln!(
        "  upload    : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(UPLOAD_NS.load(Ordering::Relaxed)),
        to_per(UPLOAD_NS.load(Ordering::Relaxed))
    );
    eprintln!(
        "  downsample: {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(DOWNSAMPLE_NS.load(Ordering::Relaxed)),
        to_per(DOWNSAMPLE_NS.load(Ordering::Relaxed))
    );
    eprintln!(
        "  sharpen   : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(SHARPEN_NS.load(Ordering::Relaxed)),
        to_per(SHARPEN_NS.load(Ordering::Relaxed))
    );
    eprintln!(
        "  download  : {:>9.2} ms total ({:>6.2} ms/call)",
        to_ms(DOWNLOAD_NS.load(Ordering::Relaxed)),
        to_per(DOWNLOAD_NS.load(Ordering::Relaxed))
    );
}

fn ds_module(ctx: &ContextHandle) -> Result<KernelModule> {
    KernelModule::load(ctx, DOWNSAMPLE_KEY, &with_constants(DOWNSAMPLE_SRC))
}

fn sharpen_module(ctx: &ContextHandle) -> Result<KernelModule> {
    KernelModule::load(ctx, SHARPEN_KEY, &with_constants(SHARPEN_SRC))
}

/// Downsample `src` (`src_w x src_h` packed RGBA bytes, length 4*w*h) to
/// `dst_w x dst_h` via Lanczos3, then apply a 3x3 unsharp-mask. Returns
/// the final packed-RGBA byte buffer. `sratio` = source/dest per-axis,
/// e.g. 2.0 for the production 2x supersample.
pub fn downsample_then_sharpen(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    src: &[u8],
    src_w: u32,
    src_h: u32,
    dst_w: u32,
    dst_h: u32,
    sratio: f32,
) -> Result<Vec<u8>> {
    let timing = timing_enabled();

    let src_pixels = (src_w as usize) * (src_h as usize);
    let dst_pixels = (dst_w as usize) * (dst_h as usize);
    debug_assert_eq!(src.len(), src_pixels * 4);

    // Treat the byte slice as packed u32 RGBA. Safe: image crate aligns
    // RgbaImage::as_raw() to 1 byte but we read whole pixels (4 bytes)
    // at u32 alignment via the kernel; the host upload is byte-wise.
    let src_u32: &[u32] =
        unsafe { std::slice::from_raw_parts(src.as_ptr() as *const u32, src_pixels) };

    let t_up = if timing { Some(Instant::now()) } else { None };
    let src_d = DeviceVec::from_host_on(stream, src_u32)?;
    let mut mid_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, dst_pixels)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, dst_pixels)?;
    if let Some(t) = t_up {
        UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // Pass 1: Lanczos3 downscale src → mid.
    let t_ds = if timing { Some(Instant::now()) } else { None };
    let m_ds = ds_module(ctx)?;
    let f_ds = m_ds.function(DOWNSAMPLE_FN)?;
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg_dst = LaunchCfg {
        grid_dim: ((dst_w + bx - 1) / bx, (dst_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };
    let src_w_i = src_w as i32;
    let src_h_i = src_h as i32;
    let dst_w_i = dst_w as i32;
    let dst_h_i = dst_h as i32;
    let mut b1 = stream.launch_builder(&f_ds);
    b1.arg(src_d.slice())
        .arg(&src_w_i)
        .arg(&src_h_i)
        .arg(mid_d.slice_mut())
        .arg(&dst_w_i)
        .arg(&dst_h_i)
        .arg(&sratio);
    unsafe { b1.launch(cfg_dst)? };
    if let Some(t) = t_ds {
        DOWNSAMPLE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // Pass 2: 3x3 sharpen mid → out.
    let t_sh = if timing { Some(Instant::now()) } else { None };
    let m_sh = sharpen_module(ctx)?;
    let f_sh = m_sh.function(SHARPEN_FN)?;
    let mut b2 = stream.launch_builder(&f_sh);
    b2.arg(mid_d.slice())
        .arg(out_d.slice_mut())
        .arg(&dst_w_i)
        .arg(&dst_h_i);
    unsafe { b2.launch(cfg_dst)? };
    if let Some(t) = t_sh {
        SHARPEN_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let t_dl = if timing { Some(Instant::now()) } else { None };
    let bytes_u32 = out_d.copy_to_host_on(stream)?;
    if let Some(t) = t_dl {
        DOWNLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    if timing {
        N_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    Ok(u32_vec_to_rgba_bytes(bytes_u32))
}

fn u32_vec_to_rgba_bytes(mut v: Vec<u32>) -> Vec<u8> {
    let len_bytes = v.len() * 4;
    let cap_bytes = v.capacity() * 4;
    let ptr = v.as_mut_ptr() as *mut u8;
    std::mem::forget(v);
    unsafe { Vec::from_raw_parts(ptr, len_bytes, cap_bytes) }
}

/// Same as `downsample_then_sharpen` but reads `src` from a `DeviceVec<u32>`
/// already on the GPU — skips the host upload entirely. Used by the
/// canvas-resident pipeline where the supersampled canvas is built and
/// kept GPU-resident across all draw phases, so the only transfer left is
/// the final dst-size download.
pub fn downsample_then_sharpen_from_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    src: &DeviceVec<u32>,
    src_w: u32,
    src_h: u32,
    dst_w: u32,
    dst_h: u32,
    sratio: f32,
) -> Result<Vec<u8>> {
    let timing = timing_enabled();

    let dst_pixels = (dst_w as usize) * (dst_h as usize);

    let t_up = if timing { Some(Instant::now()) } else { None };
    let mut mid_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, dst_pixels)?;
    let mut out_d: DeviceVec<u32> = DeviceVec::zeros_on(stream, dst_pixels)?;
    if let Some(t) = t_up {
        UPLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // Pass 1: Lanczos3 downscale src → mid.
    let t_ds = if timing { Some(Instant::now()) } else { None };
    let m_ds = ds_module(ctx)?;
    let f_ds = m_ds.function(DOWNSAMPLE_FN)?;
    let bx: u32 = 16;
    let by: u32 = 16;
    let cfg_dst = LaunchCfg {
        grid_dim: ((dst_w + bx - 1) / bx, (dst_h + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };
    let src_w_i = src_w as i32;
    let src_h_i = src_h as i32;
    let dst_w_i = dst_w as i32;
    let dst_h_i = dst_h as i32;
    let mut b1 = stream.launch_builder(&f_ds);
    b1.arg(src.slice())
        .arg(&src_w_i)
        .arg(&src_h_i)
        .arg(mid_d.slice_mut())
        .arg(&dst_w_i)
        .arg(&dst_h_i)
        .arg(&sratio);
    unsafe { b1.launch(cfg_dst)? };
    if let Some(t) = t_ds {
        DOWNSAMPLE_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // Pass 2: 3x3 sharpen mid → out.
    let t_sh = if timing { Some(Instant::now()) } else { None };
    let m_sh = sharpen_module(ctx)?;
    let f_sh = m_sh.function(SHARPEN_FN)?;
    let mut b2 = stream.launch_builder(&f_sh);
    b2.arg(mid_d.slice())
        .arg(out_d.slice_mut())
        .arg(&dst_w_i)
        .arg(&dst_h_i);
    unsafe { b2.launch(cfg_dst)? };
    if let Some(t) = t_sh {
        SHARPEN_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    let t_dl = if timing { Some(Instant::now()) } else { None };
    let bytes_u32 = out_d.copy_to_host_on(stream)?;
    if let Some(t) = t_dl {
        DOWNLOAD_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    if timing {
        N_CALLS.fetch_add(1, Ordering::Relaxed);
    }

    Ok(u32_vec_to_rgba_bytes(bytes_u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{imageops, ImageBuffer, Rgba};

    /// CPU reference for the sharpen pass — applied directly to a u8
    /// buffer to avoid `imageops::filter3x3`'s generic-pixel quirks
    /// (it returns f32-like output unsuitable for u8 ground truth).
    fn cpu_sharpen_3x3(src: &[u8], w: u32, h: u32) -> Vec<u8> {
        let w = w as i32;
        let h = h as i32;
        let mut out = vec![0u8; src.len()];
        let kc = 1.88f32;
        let ke = -0.22f32;
        let read = |xx: i32, yy: i32, c: usize| -> f32 {
            let xx = xx.clamp(0, w - 1);
            let yy = yy.clamp(0, h - 1);
            src[(yy * w + xx) as usize * 4 + c] as f32
        };
        let sat_round = |v: f32| -> u8 {
            if v < 0.0 {
                0
            } else if v > 255.0 {
                255
            } else {
                (v + 0.5) as u8
            }
        };
        for y in 0..h {
            for x in 0..w {
                for c in 0..4 {
                    let v = read(x, y, c) * kc
                        + read(x, y - 1, c) * ke
                        + read(x, y + 1, c) * ke
                        + read(x - 1, y, c) * ke
                        + read(x + 1, y, c) * ke;
                    out[(y * w + x) as usize * 4 + c] = sat_round(v);
                }
            }
        }
        out
    }

    #[test]
    fn downsample_sharpen_roughly_matches_cpu() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: {e}");
                return;
            }
        };
        let stream = ctx.new_stream().expect("stream");

        let sw = 320u32;
        let sh = 240u32;
        let dw = 160u32;
        let dh = 120u32;

        let mut img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::new(sw, sh);
        for y in 0..sh {
            for x in 0..sw {
                let r = (x % 256) as u8;
                let g = (y % 256) as u8;
                let b = ((x ^ y) % 256) as u8;
                img.put_pixel(x, y, Rgba([r, g, b, 255]));
            }
        }

        // CPU reference: Lanczos3 via image, then our matching sharpen ref.
        let cpu_resized = imageops::resize(&img, dw, dh, imageops::FilterType::Lanczos3);
        let cpu_sharpen = cpu_sharpen_3x3(cpu_resized.as_raw(), dw, dh);

        let gpu_bytes =
            downsample_then_sharpen(&ctx, &stream, img.as_raw(), sw, sh, dw, dh, 2.0).expect("gpu");

        assert_eq!(gpu_bytes.len(), cpu_sharpen.len());
        let n = (dw * dh) as usize;
        let mut diff_pixels = 0usize;
        let mut max_chan_delta = 0u8;
        for p in 0..n {
            let off = p * 4;
            if cpu_sharpen[off..off + 4] != gpu_bytes[off..off + 4] {
                diff_pixels += 1;
                for c in 0..4 {
                    let d = (cpu_sharpen[off + c] as i32 - gpu_bytes[off + c] as i32).unsigned_abs()
                        as u8;
                    if d > max_chan_delta {
                        max_chan_delta = d;
                    }
                }
            }
        }
        let pct = 100.0 * diff_pixels as f64 / n as f64;
        eprintln!(
            "downsample+sharpen: {} / {} pixels differ ({:.2}%), max channel delta = {}",
            diff_pixels, n, pct, max_chan_delta
        );
        // f32 + GPU rounding: tolerate ≤ 2/255 max channel delta.
        assert!(
            max_chan_delta <= 2,
            "max channel delta {} > 2",
            max_chan_delta
        );
    }
}
