//! GPU port of `rustwx_render::render::draw_contours` — marching-squares
//! isoline rasterization for `ContourOverlay`.
//!
//! ## Inputs
//!
//! - A scalar field `data: [ny][nx]` (row-major, f64).
//! - Per-corner pixel coordinates `pix_x[ny*nx]`, `pix_y[ny*nx]`. The CPU
//!   accepts an `Option<&[Option<(f64, f64)>]>`; we materialize that into
//!   two dense `f64` arrays plus a `valid: i32` array (1 = corner usable,
//!   0 = hole). When the caller has no `pixel_points`, build the linear
//!   `grid_to_pixel` mapping host-side and pass `valid = all 1`.
//! - A list of `levels: f64`. The CPU caller filters `levels_are_sorted_finite`
//!   before bucketing; we accept any list (NaN levels collapse to no
//!   intersections via the `interp_point` finite check) but we recommend the
//!   caller still pre-filter.
//! - Style (`color`, `width`) — a single style applies to the whole overlay,
//!   matching `ContourOverlay`.
//!
//! ## Output
//!
//! The provided canvas (RGBA8 packed bytes) is copied to device, modified
//! in place by the kernel, and returned as a fresh `Vec<u8>`.
//!
//! ## What we DON'T port (and why)
//!
//! - `pixel_points: Option<…>` — collapsed at the call site into the
//!   `valid` mask (see above). The CPU's two branches (mask vs no-mask)
//!   reduce to "skip cell if any corner is None".
//! - `clip_mask: Option<&RgbaImage>` — drop. CPU `draw_contour_segments_masked`
//!   tests segment-vs-mask with raster sampling; that's a separate port.
//!   This kernel always renders unmasked. Callers that need clipping can
//!   composite the GPU output through the same mask afterwards (the
//!   contour line layer is overwritten where the mask is transparent).
//! - Contour LABELS — `maybe_draw_contour_label` calls into `text::draw_text`
//!   which is its own software rasterizer. Labels are sparse (one per
//!   level per render); leaving them on the CPU for now.
//! - H/L extrema labels (`draw_extrema_labels`) — same reason.
//! - Bucketed-vs-legacy fast path: GPU launches one thread per (cell,level)
//!   regardless. The CPU bucketing exists to avoid CPU overhead from
//!   touching cells that no level intersects; on the GPU, threads early-out
//!   when count<2 so the same culling happens implicitly with no host
//!   pre-pass.

use std::sync::Arc;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/render/contour_lines.cu");
const MODULE_KEY: &str = "render_contour_lines";
const FUNCTION: &str = "contour_lines_kernel";

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// Per-corner geometry for one contour overlay. `pix_x`/`pix_y` are the
/// pixel-space coordinates of every grid corner (already shifted by the
/// `Layout::map_x`/`map_y` offset on the CPU side); `valid` is a 0/1 mask
/// that mirrors the CPU's `Option<(f64, f64)>` semantics.
///
/// Lengths must all equal `nx * ny`.
#[derive(Clone, Debug)]
pub struct ContourGeometry<'a> {
    pub data: &'a [f64],
    pub nx: usize,
    pub ny: usize,
    pub pix_x: &'a [f64],
    pub pix_y: &'a [f64],
    pub valid: &'a [i32],
}

/// Default-stream entry point. Use `host_on` for per-thread streams.
pub fn host(
    ctx: &ContextHandle,
    canvas: &[u8],
    img_w: u32,
    img_h: u32,
    geom: &ContourGeometry<'_>,
    levels: &[f64],
    color: u32,
    width: u32,
) -> Result<Vec<u8>> {
    host_on(
        ctx,
        ctx.stream(),
        canvas,
        img_w,
        img_h,
        geom,
        levels,
        color,
        width,
    )
}

/// Caller-supplied stream variant.
#[allow(clippy::too_many_arguments)]
pub fn host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    canvas: &[u8],
    img_w: u32,
    img_h: u32,
    geom: &ContourGeometry<'_>,
    levels: &[f64],
    color: u32,
    width: u32,
) -> Result<Vec<u8>> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);
    let canvas_bytes = n_pixels * 4;

    if canvas.len() != canvas_bytes {
        return Ok(vec![0u8; canvas_bytes]);
    }

    // Mirror CPU early-outs: too-small grid, no levels, or no corners.
    let cells_per_row = geom.nx.saturating_sub(1);
    let cell_rows = geom.ny.saturating_sub(1);
    let total_cells = cells_per_row * cell_rows;
    let alpha = (color >> 24) & 0xFF;
    if total_cells == 0
        || levels.is_empty()
        || alpha == 0
        || width == 0
        || geom.nx < 2
        || geom.ny < 2
    {
        return Ok(canvas.to_vec());
    }

    let n_corners = geom.nx * geom.ny;
    debug_assert_eq!(geom.data.len(), n_corners);
    debug_assert_eq!(geom.pix_x.len(), n_corners);
    debug_assert_eq!(geom.pix_y.len(), n_corners);
    debug_assert_eq!(geom.valid.len(), n_corners);
    if geom.data.len() != n_corners
        || geom.pix_x.len() != n_corners
        || geom.pix_y.len() != n_corners
        || geom.valid.len() != n_corners
    {
        return Ok(canvas.to_vec());
    }

    // Reinterpret RGBA8 -> u32 (matches `image::Rgba` byte order).
    let canvas_u32: Vec<u32> = canvas
        .chunks_exact(4)
        .map(|c| {
            (c[0] as u32) | ((c[1] as u32) << 8) | ((c[2] as u32) << 16) | ((c[3] as u32) << 24)
        })
        .collect();

    let mut canvas_d: DeviceVec<u32> = DeviceVec::from_host_on(stream, &canvas_u32)?;
    let data_d = DeviceVec::from_host_on(stream, geom.data)?;
    let pix_x_d = DeviceVec::from_host_on(stream, geom.pix_x)?;
    let pix_y_d = DeviceVec::from_host_on(stream, geom.pix_y)?;
    let valid_d = DeviceVec::from_host_on(stream, geom.valid)?;
    let levels_d = DeviceVec::from_host_on(stream, levels)?;

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    // 2D launch: x = cell, y = level.
    let bx: u32 = 64;
    let by: u32 = 4;
    let n_cells = total_cells as u32;
    let n_levels = levels.len() as u32;
    let cfg = LaunchCfg {
        grid_dim: ((n_cells + bx - 1) / bx, (n_levels + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let nx_i: i32 = geom.nx as i32;
    let ny_i: i32 = geom.ny as i32;
    let n_levels_i: i32 = levels.len() as i32;
    let width_i: i32 = width.max(1) as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(pix_x_d.slice())
        .arg(pix_y_d.slice())
        .arg(valid_d.slice())
        .arg(levels_d.slice())
        .arg(&n_levels_i)
        .arg(&color)
        .arg(&width_i)
        .arg(canvas_d.slice_mut())
        .arg(&img_w_i)
        .arg(&img_h_i);
    unsafe { builder.launch(cfg)? };

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
/// on device. One kernel launch per call (i.e., one per `ContourOverlay`).
///
/// `_clip_mask` is unused by the contour kernel itself (matching the
/// `unmasked` variant on the CPU side). It's kept in the signature for
/// symmetry; callers should drop the mask when feeding contours to GPU.
#[allow(clippy::too_many_arguments)]
pub fn host_into_device_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    canvas: &mut DeviceVec<u32>,
    img_w: u32,
    img_h: u32,
    geom: &ContourGeometry<'_>,
    levels: &[f64],
    color: u32,
    width: u32,
    _clip_mask: Option<&DeviceVec<u32>>,
) -> Result<()> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);

    let cells_per_row = geom.nx.saturating_sub(1);
    let cell_rows = geom.ny.saturating_sub(1);
    let total_cells = cells_per_row * cell_rows;
    let alpha = (color >> 24) & 0xFF;
    if total_cells == 0
        || levels.is_empty()
        || alpha == 0
        || width == 0
        || geom.nx < 2
        || geom.ny < 2
    {
        return Ok(());
    }

    let n_corners = geom.nx * geom.ny;
    if geom.data.len() != n_corners
        || geom.pix_x.len() != n_corners
        || geom.pix_y.len() != n_corners
        || geom.valid.len() != n_corners
    {
        return Ok(());
    }

    let data_d = DeviceVec::from_host_on(stream, geom.data)?;
    let pix_x_d = DeviceVec::from_host_on(stream, geom.pix_x)?;
    let pix_y_d = DeviceVec::from_host_on(stream, geom.pix_y)?;
    let valid_d = DeviceVec::from_host_on(stream, geom.valid)?;
    let levels_d = DeviceVec::from_host_on(stream, levels)?;

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    let bx: u32 = 64;
    let by: u32 = 4;
    let n_cells = total_cells as u32;
    let n_levels = levels.len() as u32;
    let cfg = LaunchCfg {
        grid_dim: ((n_cells + bx - 1) / bx, (n_levels + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    };

    let nx_i: i32 = geom.nx as i32;
    let ny_i: i32 = geom.ny as i32;
    let n_levels_i: i32 = levels.len() as i32;
    let width_i: i32 = width.max(1) as i32;
    let img_w_i: i32 = img_w as i32;
    let img_h_i: i32 = img_h as i32;

    let mut builder = stream.launch_builder(&func);
    builder
        .arg(data_d.slice())
        .arg(&ny_i)
        .arg(&nx_i)
        .arg(pix_x_d.slice())
        .arg(pix_y_d.slice())
        .arg(valid_d.slice())
        .arg(levels_d.slice())
        .arg(&n_levels_i)
        .arg(&color)
        .arg(&width_i)
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

    // ---- CPU reference ---------------------------------------------------
    //
    // Inlined `interp_point` + `draw_contour_segments_unmasked` +
    // `draw_line_aa_kernel` + `blend_pixel(_coverage)` from
    // `crates/rustwx-render/src/{render.rs, draw.rs}`. Byte-for-byte mirror
    // so we can detect drift.

    fn interp_ref(
        x0: f64,
        y0: f64,
        v0: f64,
        x1: f64,
        y1: f64,
        v1: f64,
        level: f64,
    ) -> Option<(f64, f64)> {
        if !v0.is_finite() || !v1.is_finite() {
            return None;
        }
        let d0 = v0 - level;
        let d1 = v1 - level;
        if (d0 > 0.0 && d1 > 0.0) || (d0 < 0.0 && d1 < 0.0) {
            return None;
        }
        if (v1 - v0).abs() < 1e-12 {
            return Some(((x0 + x1) * 0.5, (y0 + y1) * 0.5));
        }
        let t = (level - v0) / (v1 - v0);
        Some((x0 + (x1 - x0) * t, y0 + (y1 - y0) * t))
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

    #[allow(clippy::too_many_arguments)]
    fn cpu_ref(
        canvas: &mut [u8],
        img_w: u32,
        img_h: u32,
        geom: &ContourGeometry<'_>,
        levels: &[f64],
        color: u32,
        width: u32,
    ) {
        let alpha = ((color >> 24) & 0xFF) as u8;
        if alpha == 0 || width == 0 || geom.nx < 2 || geom.ny < 2 {
            return;
        }
        let r = (color & 0xFF) as u8;
        let g = ((color >> 8) & 0xFF) as u8;
        let b = ((color >> 16) & 0xFF) as u8;
        for &level in levels {
            for j in 0..(geom.ny - 1) {
                for i in 0..(geom.nx - 1) {
                    let i00 = j * geom.nx + i;
                    let i10 = j * geom.nx + (i + 1);
                    let i11 = (j + 1) * geom.nx + (i + 1);
                    let i01 = (j + 1) * geom.nx + i;
                    if geom.valid[i00] == 0
                        || geom.valid[i10] == 0
                        || geom.valid[i11] == 0
                        || geom.valid[i01] == 0
                    {
                        continue;
                    }
                    let p0 = (geom.pix_x[i00], geom.pix_y[i00], geom.data[i00]);
                    let p1 = (geom.pix_x[i10], geom.pix_y[i10], geom.data[i10]);
                    let p2 = (geom.pix_x[i11], geom.pix_y[i11], geom.data[i11]);
                    let p3 = (geom.pix_x[i01], geom.pix_y[i01], geom.data[i01]);

                    let mut pts = [(0.0f64, 0.0f64); 4];
                    let mut count = 0usize;
                    for (a, bb) in [(p0, p1), (p1, p2), (p2, p3), (p3, p0)] {
                        if let Some(pt) = interp_ref(a.0, a.1, a.2, bb.0, bb.1, bb.2, level) {
                            pts[count] = pt;
                            count += 1;
                        }
                    }
                    if count < 2 {
                        continue;
                    }
                    let segs: &[(usize, usize)] = if count == 4 {
                        &[(0, 1), (2, 3)]
                    } else {
                        &[(0, 1)]
                    };
                    for &(ai, bi) in segs {
                        let (x0, y0) = pts[ai];
                        let (x1, y1) = pts[bi];
                        draw_line_ref(canvas, img_w, img_h, x0, y0, x1, y1, r, g, b, alpha, width);
                    }
                }
            }
        }
    }

    #[test]
    fn contour_lines_match_cpu_reference() {
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

        // Background — non-trivial gradient so the alpha-blend exercises the
        // destination compositing path.
        let mut bg = vec![0u8; n_pixels * 4];
        for j in 0..img_h {
            for i in 0..img_w {
                let off = ((j * img_w + i) as usize) * 4;
                bg[off] = ((i * 255) / img_w.max(1)) as u8;
                bg[off + 1] = ((j * 255) / img_h.max(1)) as u8;
                bg[off + 2] = 60;
                bg[off + 3] = 255;
            }
        }

        // Build a synthetic scalar field whose isolines form a nested set
        // of conic-section-like curves that intersect the cell grid in many
        // ways (both regular and saddle cells). 24x18 grid mapped onto a
        // 240x150 region inside the canvas with a 8-pixel inset so we
        // exercise positive pixel coords.
        let nx = 24usize;
        let ny = 18usize;
        let inset = 8.0f64;
        let map_w = (img_w as f64) - 2.0 * inset;
        let map_h = (img_h as f64) - 2.0 * inset;
        let mut data = vec![0.0f64; nx * ny];
        let mut pix_x = vec![0.0f64; nx * ny];
        let mut pix_y = vec![0.0f64; nx * ny];
        let valid = vec![1i32; nx * ny];
        for j in 0..ny {
            for i in 0..nx {
                // Pixel coords (linear `grid_to_pixel` analogue).
                let px = inset + (i as f64) / ((nx - 1) as f64) * (map_w - 1.0);
                let py = inset + (j as f64) / ((ny - 1) as f64) * (map_h - 1.0);
                pix_x[j * nx + i] = px;
                pix_y[j * nx + i] = py;
                // Field: shifted bowl + a saddle-creating cross term so we
                // get cells with 2 hits (regular) and 4 hits (saddle).
                let u = (i as f64) / ((nx - 1) as f64) * 4.0 - 2.0;
                let v = (j as f64) / ((ny - 1) as f64) * 3.0 - 1.5;
                data[j * nx + i] = u * u - v * v + 0.6 * u * v + 0.3;
            }
        }

        // 3 intersecting levels — one near the center of the bowl (no
        // hits), two that produce overlapping isolines.
        let levels = vec![-1.0f64, 0.0, 1.0];
        let color = pack_rgba(255, 255, 255, 220);
        let width = 2u32;

        let geom = ContourGeometry {
            data: &data,
            nx,
            ny,
            pix_x: &pix_x,
            pix_y: &pix_y,
            valid: &valid,
        };

        let mut cpu_canvas = bg.clone();
        cpu_ref(&mut cpu_canvas, img_w, img_h, &geom, &levels, color, width);

        let gpu_canvas = host(&ctx, &bg, img_w, img_h, &geom, &levels, color, width)
            .expect("CUDA contour-lines kernel failed");

        assert_eq!(cpu_canvas.len(), gpu_canvas.len());

        // Sanity: ensure we actually drew something (otherwise the test is
        // vacuously true).
        let touched = cpu_canvas
            .iter()
            .zip(bg.iter())
            .filter(|(c, b)| c != b)
            .count();
        assert!(
            touched > 100,
            "CPU reference produced almost no output ({touched} differing bytes) — \
             test inputs are degenerate"
        );

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
            "contour_lines: {} / {} pixels differ ({:.3}%), max channel delta = {}",
            diff_pixels, n_pixels, pct, max_chan_delta
        );

        // Tolerance: <=2/255. Sources of difference are the same as the
        // linework kernel:
        //   (a) atomic-CAS races between threads writing different LEVELS
        //       to the same pixel (intra-level only writes once per pixel
        //       per cell; inter-level overlaps are where the order shifts).
        //   (b) f64 round-to-nearest 1-ULP boundary disagreements.
        assert!(
            max_chan_delta <= 2,
            "max channel delta {} > 2",
            max_chan_delta
        );
    }

    #[test]
    fn contour_lines_skips_invalid_corners() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e})");
                return;
            }
        };

        // 3x3 grid where the top-left corner is invalid. Of the 4 cells,
        // only cell (0,0) touches that corner, so 3 cells still draw on
        // both CPU and GPU.
        let nx = 3usize;
        let ny = 3usize;
        let pix_x = vec![10.0, 50.0, 90.0, 10.0, 50.0, 90.0, 10.0, 50.0, 90.0];
        let pix_y = vec![10.0, 10.0, 10.0, 50.0, 50.0, 50.0, 90.0, 90.0, 90.0];
        let mut valid = vec![1i32; 9];
        valid[0] = 0;
        let data = vec![0.0, 1.0, 2.0, 1.0, 2.0, 3.0, 2.0, 3.0, 4.0];
        let levels = vec![1.5f64, 2.5];
        let color = pack_rgba(0, 0, 255, 255);

        let img_w: u32 = 128;
        let img_h: u32 = 128;
        let bg = vec![0u8; (img_w * img_h * 4) as usize];

        let geom = ContourGeometry {
            data: &data,
            nx,
            ny,
            pix_x: &pix_x,
            pix_y: &pix_y,
            valid: &valid,
        };

        let mut cpu_canvas = bg.clone();
        cpu_ref(&mut cpu_canvas, img_w, img_h, &geom, &levels, color, 1);
        let gpu_canvas = host(&ctx, &bg, img_w, img_h, &geom, &levels, color, 1)
            .expect("CUDA contour-lines kernel failed");

        let mut max_chan_delta = 0i32;
        for (cb, gb) in cpu_canvas.iter().zip(gpu_canvas.iter()) {
            let d = (*cb as i32 - *gb as i32).abs();
            if d > max_chan_delta {
                max_chan_delta = d;
            }
        }
        assert!(
            max_chan_delta <= 2,
            "max channel delta {} > 2",
            max_chan_delta
        );
    }
}
