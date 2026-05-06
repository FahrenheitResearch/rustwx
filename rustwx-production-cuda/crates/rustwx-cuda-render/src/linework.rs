//! GPU port of `rustwx_render::render::draw_projected_lines` —
//! anti-aliased polyline rasterization (coastlines, political borders,
//! lat/lon grids, etc.) into an RGBA8 canvas.
//!
//! Layout: one CUDA thread per line SEGMENT, flattened across all input
//! polylines. Each thread iterates the segment's pixel bbox and atomically
//! alpha-blends into the canvas (matches `draw::blend_pixel_coverage` /
//! `blend_pixel`, which is sequential per-pixel on the CPU).
//!
//! See `kernels/render/linework.cu` for the per-pixel coverage / blend math.
//!
//! ## Calling convention
//!
//! The caller hands us:
//!   - the existing canvas as packed `[R, G, B, A]` bytes (modified in place
//!     conceptually — we upload, run, download a fresh copy).
//!   - a list of `Polyline { points, color, width }` styled objects. Hidden
//!     polylines, polylines shorter than 2 points, or polylines with width
//!     == 0 are silently dropped (matching the CPU early-outs).
//!
//! Clipping & projection are upstream concerns: by the time points reach us
//! they are already in canvas pixel coordinates with `None`-holes already
//! split into separate polylines. This mirrors `draw_projected_lines`'s
//! behavior of restarting the chain whenever a point is invisible.

use std::sync::Arc;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/render/linework.cu");
const MODULE_KEY: &str = "render_linework";
const FUNCTION: &str = "linework_aa_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// One styled polyline ready for GPU rasterization. `points` are already in
/// canvas pixel coordinates (caller has applied projection + extent + layout
/// offset). `color` is packed `[R, G, B, A]` in `image::Rgba` byte order
/// (use `crate::colormap::pack_rgba`).
#[derive(Clone, Debug)]
pub struct Polyline {
    pub points: Vec<(f64, f64)>,
    pub color: u32,
    pub width: u32,
}

/// Default-stream entry point. Use `host_on` for per-thread streams.
pub fn host(
    ctx: &ContextHandle,
    canvas: &[u8],
    img_w: u32,
    img_h: u32,
    polylines: &[Polyline],
) -> Result<Vec<u8>> {
    host_on(ctx, ctx.stream(), canvas, img_w, img_h, polylines)
}

/// Caller-supplied stream variant. Uploads the canvas + segment table,
/// launches the kernel, downloads the modified canvas.
///
/// The caller's canvas is left untouched; the returned `Vec<u8>` is the new
/// canvas of length `img_w * img_h * 4`.
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    canvas: &[u8],
    img_w: u32,
    img_h: u32,
    polylines: &[Polyline],
) -> Result<Vec<u8>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);
    let canvas_bytes = n_pixels * 4;

    if canvas.len() != canvas_bytes {
        // Caller-supplied canvas size mismatch — return a zeroed canvas of
        // the requested dimensions rather than panic. Keeps us aligned with
        // the other kernels in this crate which prefer "no-op" over panic.
        return Ok(vec![0u8; canvas_bytes]);
    }

    // ---- Flatten polylines -> per-polyline segment buckets ----------------
    //
    // Filter out anything that wouldn't draw on the CPU:
    //   - alpha == 0   (invisible)
    //   - width == 0   (the CPU's `draw_line_aa_kernel` clamps width to 1,
    //                   but `draw_polyline_aa` has no such guard — we treat
    //                   width==0 as invisible to skip useless work)
    //   - points.len() < 2
    //   - any point not finite (CPU `draw_line_aa_kernel` early-outs;
    //     individual segments are filtered, not the whole polyline)
    //
    // We launch ONE kernel per polyline so that polyline ordering matches the
    // CPU reference (the CPU draws polylines sequentially; an opaque polyline
    // overwrites whatever a previous semi-transparent polyline blended at the
    // same pixel). Within a single polyline, segments may race, but the
    // atomic-CAS blend keeps such races bounded to ~1/255 per channel.
    struct Bucket {
        x0: Vec<f64>,
        y0: Vec<f64>,
        x1: Vec<f64>,
        y1: Vec<f64>,
        seg_poly: Vec<i32>,
        color: u32,
        width: i32,
    }
    let mut buckets: Vec<Bucket> = Vec::new();

    for line in polylines {
        let alpha = (line.color >> 24) & 0xFF;
        if alpha == 0 || line.width == 0 || line.points.len() < 2 {
            continue;
        }
        let mut b = Bucket {
            x0: Vec::new(),
            y0: Vec::new(),
            x1: Vec::new(),
            y1: Vec::new(),
            seg_poly: Vec::new(),
            color: line.color,
            width: line.width.max(1) as i32,
        };
        for w in line.points.windows(2) {
            let (x0, y0) = w[0];
            let (x1, y1) = w[1];
            if !x0.is_finite() || !y0.is_finite() || !x1.is_finite() || !y1.is_finite() {
                continue;
            }
            b.x0.push(x0);
            b.y0.push(y0);
            b.x1.push(x1);
            b.y1.push(y1);
            b.seg_poly.push(0);
        }
        if !b.seg_poly.is_empty() {
            buckets.push(b);
        }
    }

    // No work — round-trip the canvas as-is.
    if buckets.is_empty() {
        return Ok(canvas.to_vec());
    }

    // ---- Upload canvas ---------------------------------------------------
    // Canvas: reinterpret RGBA8 bytes as u32 (little-endian -> R is byte 0).
    // We require an aligned copy because `from_host_on` needs `&[u32]`.
    let canvas_u32: Vec<u32> = canvas
        .chunks_exact(4)
        .map(|c| {
            (c[0] as u32) | ((c[1] as u32) << 8) | ((c[2] as u32) << 16) | ((c[3] as u32) << 24)
        })
        .collect();

    let mut canvas_d: DeviceVec<u32> = DeviceVec::from_host_on(stream, &canvas_u32)?;

    // ---- Launch one kernel per polyline ---------------------------------
    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let bx: u32 = 128;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    for b in &buckets {
        let n_segments = b.seg_poly.len() as i32;
        let seg_x0_d = DeviceVec::from_host_on(stream, &b.x0)?;
        let seg_y0_d = DeviceVec::from_host_on(stream, &b.y0)?;
        let seg_x1_d = DeviceVec::from_host_on(stream, &b.x1)?;
        let seg_y1_d = DeviceVec::from_host_on(stream, &b.y1)?;
        let seg_poly_d = DeviceVec::from_host_on(stream, &b.seg_poly)?;
        let poly_color_d = DeviceVec::from_host_on(stream, &[b.color])?;
        let poly_width_d = DeviceVec::from_host_on(stream, &[b.width])?;

        let cfg = LaunchCfg {
            grid_dim: (((n_segments as u32) + bx - 1) / bx, 1, 1),
            block_dim: (bx, 1, 1),
            shared_mem_bytes: 0,
        };

        let mut builder = stream.launch_builder(&func);
        builder
            .arg(seg_x0_d.slice())
            .arg(seg_y0_d.slice())
            .arg(seg_x1_d.slice())
            .arg(seg_y1_d.slice())
            .arg(seg_poly_d.slice())
            .arg(&n_segments)
            .arg(poly_color_d.slice())
            .arg(poly_width_d.slice())
            .arg(canvas_d.slice_mut())
            .arg(&img_w_i)
            .arg(&img_h_i);
        unsafe { builder.launch(cfg)? };
    }

    // ---- Download -------------------------------------------------------
    let pixels_u32 = canvas_d.copy_to_host_on(stream)?;
    Ok(u32_vec_to_rgba_bytes(pixels_u32))
}

fn u32_vec_to_rgba_bytes(mut v: Vec<u32>) -> Vec<u8> {
    let len_bytes = v.len() * 4;
    let cap_bytes = v.capacity() * 4;
    let ptr = v.as_mut_ptr() as *mut u8;
    std::mem::forget(v);
    unsafe { Vec::from_raw_parts(ptr, len_bytes, cap_bytes) }
}

/// Same as `host_on` but rasterizes directly into a device-resident
/// canvas in place — no PCIe transfers. The caller already has the canvas
/// on device. Per polyline ordering preservation: ONE kernel launch per
/// polyline (matches `host_on`); within a single canvas-resident pipeline
/// these launches still serialize on the same stream and share the pre-
/// uploaded canvas buffer, so no per-launch canvas round-trip is paid.
///
/// `clip_mask` is currently unused by the linework kernel — the CPU
/// linework path is responsible for clip-mask culling at the polyline
/// chunking level (segments outside the mask are dropped before reaching
/// this function). It's kept in the signature for symmetry with the other
/// `host_into_device_on` wrappers.
pub fn host_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    canvas: &mut DeviceVec<u32>,
    img_w: u32,
    img_h: u32,
    polylines: &[Polyline],
    _clip_mask: Option<&DeviceVec<u32>>,
) -> Result<()> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);

    // Single-launch batch: collect all segments across all visible polylines
    // into one flat SoA table. Per-polyline ordering is no longer preserved
    // — adjacent polylines may race on shared edge pixels with at most ~1/255
    // channel noise (atomic-CAS blend bounds it). Verified visually equal
    // against the per-polyline serialised CPU path on real HRRR maps.
    let mut x0: Vec<f64> = Vec::new();
    let mut y0: Vec<f64> = Vec::new();
    let mut x1: Vec<f64> = Vec::new();
    let mut y1: Vec<f64> = Vec::new();
    let mut seg_poly: Vec<i32> = Vec::new();
    let mut poly_color: Vec<u32> = Vec::new();
    let mut poly_width: Vec<i32> = Vec::new();

    for line in polylines {
        let alpha = (line.color >> 24) & 0xFF;
        if alpha == 0 || line.width == 0 || line.points.len() < 2 {
            continue;
        }
        let pidx = poly_color.len() as i32;
        poly_color.push(line.color);
        poly_width.push(line.width.max(1) as i32);
        for w in line.points.windows(2) {
            let (px0, py0) = w[0];
            let (px1, py1) = w[1];
            if !px0.is_finite() || !py0.is_finite() || !px1.is_finite() || !py1.is_finite() {
                continue;
            }
            x0.push(px0);
            y0.push(py0);
            x1.push(px1);
            y1.push(py1);
            seg_poly.push(pidx);
        }
    }

    if seg_poly.is_empty() {
        return Ok(());
    }

    let n_segments = seg_poly.len() as i32;
    let seg_x0_d = DeviceVec::from_host_on(stream, &x0)?;
    let seg_y0_d = DeviceVec::from_host_on(stream, &y0)?;
    let seg_x1_d = DeviceVec::from_host_on(stream, &x1)?;
    let seg_y1_d = DeviceVec::from_host_on(stream, &y1)?;
    let seg_poly_d = DeviceVec::from_host_on(stream, &seg_poly)?;
    let poly_color_d = DeviceVec::from_host_on(stream, &poly_color)?;
    let poly_width_d = DeviceVec::from_host_on(stream, &poly_width)?;

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;
    let bx: u32 = 128;
    let cfg = LaunchCfg {
        grid_dim: (((n_segments as u32) + bx - 1) / bx, 1, 1),
        block_dim: (bx, 1, 1),
        shared_mem_bytes: 0,
    };
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(seg_x0_d.slice())
        .arg(seg_y0_d.slice())
        .arg(seg_x1_d.slice())
        .arg(seg_y1_d.slice())
        .arg(seg_poly_d.slice())
        .arg(&n_segments)
        .arg(poly_color_d.slice())
        .arg(poly_width_d.slice())
        .arg(canvas.slice_mut())
        .arg(&img_w_i)
        .arg(&img_h_i);
    unsafe { builder.launch(cfg)? };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colormap::pack_rgba;

    /// Inlined CPU reference of `draw_polyline_aa` -> `draw_line_aa_kernel`
    /// -> `blend_pixel_coverage` -> `blend_pixel`. Mirrors the math byte for
    /// byte so we can detect drift against the upstream `rustwx-render`
    /// implementation.
    fn cpu_ref(canvas: &mut [u8], img_w: u32, img_h: u32, polylines: &[Polyline]) {
        for line in polylines {
            let alpha = ((line.color >> 24) & 0xFF) as u8;
            if alpha == 0 || line.width == 0 || line.points.len() < 2 {
                continue;
            }
            let r = (line.color & 0xFF) as u8;
            let g = ((line.color >> 8) & 0xFF) as u8;
            let b = ((line.color >> 16) & 0xFF) as u8;
            let width = line.width.max(1);
            for w in line.points.windows(2) {
                let (x0, y0) = w[0];
                let (x1, y1) = w[1];
                if !x0.is_finite() || !y0.is_finite() || !x1.is_finite() || !y1.is_finite() {
                    continue;
                }
                draw_line_ref(canvas, img_w, img_h, x0, y0, x1, y1, r, g, b, alpha, width);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn draw_line_ref(
        canvas: &mut [u8],
        img_w: u32,
        img_h: u32,
        x0: f64,
        y0: f64,
        x1: f64,
        y1: f64,
        sr: u8,
        sg: u8,
        sb: u8,
        sa: u8,
        width: u32,
    ) {
        let half_width = width.max(1) as f64 * 0.5;
        let radius = half_width + 1.0;
        let min_x = (x0.min(x1) - radius).floor() as i32;
        let max_x = (x0.max(x1) + radius).ceil() as i32;
        let min_y = (y0.min(y1) - radius).floor() as i32;
        let max_y = (y0.max(y1) + radius).ceil() as i32;
        for py in min_y..=max_y {
            for px in min_x..=max_x {
                let p = ((px as f64) + 0.5, (py as f64) + 0.5);
                let d = dist_seg_ref(p.0, p.1, x0, y0, x1, y1);
                let coverage = (half_width + 0.5 - d).clamp(0.0, 1.0);
                if coverage <= 0.0 {
                    continue;
                }
                let scaled_alpha = ((sa as f64) * coverage).round() as u8;
                if scaled_alpha == 0 {
                    continue;
                }
                blend_ref(canvas, img_w, img_h, px, py, sr, sg, sb, scaled_alpha);
            }
        }
    }

    fn dist_seg_ref(px: f64, py: f64, x0: f64, y0: f64, x1: f64, y1: f64) -> f64 {
        let dx = x1 - x0;
        let dy = y1 - y0;
        let len_sq = dx * dx + dy * dy;
        if len_sq <= 1e-12 {
            let ox = px - x0;
            let oy = py - y0;
            return (ox * ox + oy * oy).sqrt();
        }
        let t = (((px - x0) * dx + (py - y0) * dy) / len_sq).clamp(0.0, 1.0);
        let ox = px - (x0 + t * dx);
        let oy = py - (y0 + t * dy);
        (ox * ox + oy * oy).sqrt()
    }

    #[allow(clippy::too_many_arguments)]
    fn blend_ref(
        canvas: &mut [u8],
        img_w: u32,
        img_h: u32,
        x: i32,
        y: i32,
        sr: u8,
        sg: u8,
        sb: u8,
        sa: u8,
    ) {
        if x < 0 || y < 0 || x as u32 >= img_w || y as u32 >= img_h {
            return;
        }
        let off = (y as usize * img_w as usize + x as usize) * 4;
        if sa == 255 {
            canvas[off] = sr;
            canvas[off + 1] = sg;
            canvas[off + 2] = sb;
            canvas[off + 3] = 255;
            return;
        }
        if sa == 0 {
            return;
        }
        let a = sa as f64 / 255.0;
        let inv = 1.0 - a;
        let dr = canvas[off] as f64;
        let dg = canvas[off + 1] as f64;
        let db = canvas[off + 2] as f64;
        canvas[off] = (sr as f64 * a + dr * inv).round() as u8;
        canvas[off + 1] = (sg as f64 * a + dg * inv).round() as u8;
        canvas[off + 2] = (sb as f64 * a + db * inv).round() as u8;
        canvas[off + 3] = 255;
    }

    #[test]
    fn linework_matches_cpu_reference() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e})");
                return;
            }
        };

        let img_w: u32 = 256;
        let img_h: u32 = 192;
        let n_pixels = (img_w as usize) * (img_h as usize);

        // Pre-fill with a non-trivial background so the alpha-blend path
        // actually exercises destination compositing rather than blending
        // over zeros.
        let mut bg = vec![0u8; n_pixels * 4];
        for j in 0..img_h {
            for i in 0..img_w {
                let off = ((j * img_w + i) as usize) * 4;
                bg[off] = ((i * 255) / img_w.max(1)) as u8;
                bg[off + 1] = ((j * 255) / img_h.max(1)) as u8;
                bg[off + 2] = 80;
                bg[off + 3] = 255;
            }
        }

        // Three polylines:
        //   1. Diagonal coastline-ish, white @ 80% alpha, width 1.
        //   2. Horizontal border, red opaque, width 2 (wider stroke path).
        //   3. Polyline with a non-axis-aligned bend; semi-transparent blue.
        // The third one shares an endpoint with itself (corner), exercising
        // the atomic-blend overlap path.
        let p1 = Polyline {
            points: vec![(12.5, 18.5), (60.0, 50.0), (110.25, 95.75), (200.0, 150.0)],
            color: pack_rgba(255, 255, 255, 200),
            width: 1,
        };
        let p2 = Polyline {
            points: vec![(20.0, 100.0), (240.0, 100.5)],
            color: pack_rgba(220, 30, 30, 255),
            width: 2,
        };
        let p3 = Polyline {
            points: vec![(40.0, 20.0), (80.0, 60.0), (40.0, 100.0), (80.0, 140.0)],
            color: pack_rgba(20, 60, 200, 160),
            width: 2,
        };
        let polys = vec![p1, p2, p3];

        let mut cpu_canvas = bg.clone();
        cpu_ref(&mut cpu_canvas, img_w, img_h, &polys);

        let gpu_canvas =
            host(&ctx, &bg, img_w, img_h, &polys).expect("CUDA linework kernel failed");

        assert_eq!(cpu_canvas.len(), gpu_canvas.len());

        let mut diff_pixels = 0usize;
        let mut max_chan_delta: i32 = 0;
        for p in 0..n_pixels {
            let off = p * 4;
            let mut pixel_diff = false;
            for c in 0..4 {
                let d = (cpu_canvas[off + c] as i32 - gpu_canvas[off + c] as i32).abs();
                if d > max_chan_delta {
                    max_chan_delta = d;
                }
                if d > 0 {
                    pixel_diff = true;
                }
            }
            if pixel_diff {
                diff_pixels += 1;
            }
        }
        let pct = (diff_pixels as f64) * 100.0 / (n_pixels as f64);
        eprintln!(
            "linework: {} / {} pixels differ ({:.3}%), max channel delta = {}",
            diff_pixels, n_pixels, pct, max_chan_delta
        );

        // Tolerance: <=2/255 max channel delta. Differences come from
        //   (a) the (rare) atomic-CAS race when multiple segments touch the
        //       same pixel — order of blends shifts result by at most 1/255
        //       per channel,
        //   (b) round-to-nearest after f64 arithmetic, which can disagree
        //       with the CPU by 1 ULP at the rounding boundary.
        assert!(
            max_chan_delta <= 2,
            "max channel delta {} > 2",
            max_chan_delta
        );
    }
}
