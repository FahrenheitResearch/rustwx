//! GPU port of `rustwx_render::draw::fill_polygon` (the inner primitive
//! called by `draw_projected_polygons` in `crates/rustwx-render/src/render.rs`).
//!
//! CPU algorithm (see `crates/rustwx-render/src/draw.rs`):
//!   * Pre-extract every non-horizontal edge of every ring (multi-ring
//!     polygons get holes via even-odd winding for free).
//!   * For each scanline y in the polygon's y-range, evaluate yf = y + 0.5,
//!     collect x-intersections from edges whose half-open span
//!     `[y_min, y_max)` contains yf, sort, fill alternating pairs.
//!   * Per-pixel write goes through `blend_pixel`: opaque shortcut, alpha == 0
//!     no-op, otherwise rounded "over" composite with dst alpha forced to 255.
//!
//! GPU layout:
//!   * One CUDA thread per scanline. Each thread walks the flattened edge
//!     table for the polygon, sorts intersections in registers/local, fills
//!     its row. Threads are independent — no atomics, no race-on-write
//!     (a row is owned by exactly one thread).
//!   * One kernel launch per polygon. The canvas is uploaded once at the
//!     start of [`fill_polygons_host_on`], reused across launches on the same
//!     stream, and downloaded once at the end.
//!
//! Drift from CPU: identical edge math (same `(x_at_y_min, dx_per_dy)`
//! parameterisation, same `[y_min, y_max)` half-open inclusion). Identical
//! blend math (same f64 alpha, same `+ 0.5`-then-floor rounding). Should be
//! byte-for-byte equal to CPU `fill_polygon` modulo IEEE-754 reordering in
//! the intersection sum (both use `lo_x + (yf - lo_y) * dx`).
//!
//! Caveat: `POLYFILL_MAX_INTERSECTIONS` (256) caps the number of edges that
//! can intersect a single scanline. Real basemap polygons (Natural Earth land
//! / oceans / lakes) sit at <50, but a pathological input could overflow —
//! we warn at the Rust level when total edge count > 4096 so callers know.

use std::sync::Arc;

use cudarc::driver::{CudaStream, PushKernelArg};
use rustwx_cuda_core::{ContextHandle, DeviceVec, KernelModule, LaunchCfg, Result};

use crate::sources::with_constants;

const KERNEL_SRC: &str = include_str!("../../../kernels/render/polygon_fill.cu");
const MODULE_KEY: &str = "render_polygon_fill";
const FUNCTION: &str = "polygon_fill_scanline_kernel";
const MAX_INTERSECTIONS_PER_SCANLINE: usize = 256;

fn module(ctx: &ContextHandle) -> Result<KernelModule> {
    let src = with_constants(KERNEL_SRC);
    KernelModule::load(ctx, MODULE_KEY, &src)
}

/// One filled polygon for [`fill_polygons_host_on`].
///
/// `rings`: ring 0 is the outer boundary, additional rings are holes. Mirrors
/// `ProjectedPolygon::rings` (already projected to pixel space). `color` is
/// packed RGBA8 such that the bytes spell R,G,B,A in memory — use
/// `crate::pack_rgba(r, g, b, a)`.
#[derive(Clone, Debug)]
pub struct PolygonInput<'a> {
    pub rings: &'a [Vec<(f64, f64)>],
    pub color_packed: u32,
}

/// Composite a list of filled polygons over `canvas` (RGBA8, length =
/// `4 * img_w * img_h`). `canvas` is read into the GPU, polygons are filled
/// in order on `stream`, and the result is copied back. `clip` is the inclusive
/// pixel rectangle `(x0, y0, x1, y1)`; `None` defaults to the full image.
///
/// Polygon order matters when alpha < 255 (later polygons composite on top).
/// Empty `polygons` is a no-op (canvas unchanged).
pub fn fill_polygons_host_on(
    ctx: &ContextHandle,
    stream: &Arc<CudaStream>,
    canvas: &mut [u8],
    img_w: u32,
    img_h: u32,
    polygons: &[PolygonInput<'_>],
    clip: Option<(i32, i32, i32, i32)>,
) -> Result<()> {
    let img_w = img_w.max(1);
    let img_h = img_h.max(1);
    let n_pixels = (img_w as usize) * (img_h as usize);
    let want_bytes = n_pixels * 4;
    if canvas.len() != want_bytes {
        // Caller bug; return Ok early to keep parity with CPU which would
        // simply produce a wrong-sized image. We could return an error but
        // there isn't a public Error variant for this in rustwx-cuda-core,
        // so we mirror "no-op on shape mismatch" used elsewhere.
        return Ok(());
    }
    if polygons.is_empty() {
        return Ok(());
    }

    let img_w_i = img_w as i32;
    let img_h_i = img_h as i32;

    let (cx0_full, cy0_full, cx1_full, cy1_full) = match clip {
        Some((x0, y0, x1, y1)) => (
            x0.max(0),
            y0.max(0),
            x1.min(img_w_i - 1),
            y1.min(img_h_i - 1),
        ),
        None => (0, 0, img_w_i - 1, img_h_i - 1),
    };
    if cx1_full < cx0_full || cy1_full < cy0_full {
        return Ok(());
    }

    // Upload canvas as packed u32. The byte layout R,G,B,A matches little-
    // endian u32 readback, so we can transmute the &mut [u8] view.
    let canvas_u32 = unsafe {
        std::slice::from_raw_parts(canvas.as_ptr() as *const u32, n_pixels)
    };
    let mut canvas_d: DeviceVec<u32> = DeviceVec::from_host_on(stream, canvas_u32)?;

    let m = module(ctx)?;
    let func = m.function(FUNCTION)?;

    for poly in polygons {
        if poly.rings.is_empty() {
            continue;
        }
        // Source color fully transparent? CPU returns early; mirror that.
        let src_a = ((poly.color_packed >> 24) & 0xFF) as u8;
        if src_a == 0 {
            continue;
        }

        // Build edge table + per-polygon y-range, mirroring
        // `fill_polygon`'s pre-pass exactly.
        let mut y_min = f64::INFINITY;
        let mut y_max = f64::NEG_INFINITY;
        let mut ey_min: Vec<f64> = Vec::new();
        let mut ey_max: Vec<f64> = Vec::new();
        let mut ex: Vec<f64> = Vec::new();
        let mut edx: Vec<f64> = Vec::new();

        for ring in poly.rings {
            for &(_, y) in ring.iter() {
                if y.is_finite() {
                    if y < y_min { y_min = y; }
                    if y > y_max { y_max = y; }
                }
            }
            let n = ring.len();
            if n < 2 {
                continue;
            }
            for i in 0..n {
                let (ax, ay) = ring[i];
                let (bx, by) = ring[(i + 1) % n];
                if !ax.is_finite() || !ay.is_finite() || !bx.is_finite() || !by.is_finite() {
                    continue;
                }
                if (ay - by).abs() < 1e-9 {
                    // Horizontal — skip, matches CPU.
                    continue;
                }
                let (lo_y, hi_y, lo_x, hi_x) = if ay < by {
                    (ay, by, ax, bx)
                } else {
                    (by, ay, bx, ax)
                };
                let dx = (hi_x - lo_x) / (hi_y - lo_y);
                ey_min.push(lo_y);
                ey_max.push(hi_y);
                ex.push(lo_x);
                edx.push(dx);
            }
        }
        if ey_min.is_empty() {
            continue;
        }
        if !y_min.is_finite() || !y_max.is_finite() || y_max < cy0_full as f64 {
            continue;
        }
        let y0 = (y_min.floor() as i32).max(cy0_full);
        let y1 = (y_max.ceil() as i32).min(cy1_full);
        if y1 < y0 {
            continue;
        }

        // Heuristic warning: if the polygon has more edges than 16x our
        // per-scanline cap, log once. Real Natural Earth shapes are well
        // under this; this catches accidental dense data being routed in.
        if ey_min.len() > MAX_INTERSECTIONS_PER_SCANLINE * 16 {
            eprintln!(
                "[polygon_fill warn] polygon has {} edges; per-scanline cap is {} — \
                 fill may be incomplete on dense scanlines",
                ey_min.len(),
                MAX_INTERSECTIONS_PER_SCANLINE
            );
        }

        let n_edges_i = ey_min.len() as i32;
        let n_scanlines = (y1 - y0 + 1) as u32;

        let ey_min_d = DeviceVec::from_host_on(stream, &ey_min)?;
        let ey_max_d = DeviceVec::from_host_on(stream, &ey_max)?;
        let ex_d = DeviceVec::from_host_on(stream, &ex)?;
        let edx_d = DeviceVec::from_host_on(stream, &edx)?;

        let block: u32 = 128;
        let grid: u32 = (n_scanlines + block - 1) / block;
        let cfg = LaunchCfg {
            grid_dim: (grid, 1, 1),
            block_dim: (block, 1, 1),
            shared_mem_bytes: 0,
        };

        let cx0_i = cx0_full;
        let cy0_i = cy0_full;
        let cx1_i = cx1_full;
        let cy1_i = cy1_full;
        let color_u: u32 = poly.color_packed;

        let mut builder = stream.launch_builder(&func);
        builder
            .arg(&y0)
            .arg(&y1)
            .arg(&cx0_i)
            .arg(&cy0_i)
            .arg(&cx1_i)
            .arg(&cy1_i)
            .arg(ey_min_d.slice())
            .arg(ey_max_d.slice())
            .arg(ex_d.slice())
            .arg(edx_d.slice())
            .arg(&n_edges_i)
            .arg(&color_u)
            .arg(canvas_d.slice_mut())
            .arg(&img_w_i)
            .arg(&img_h_i);
        unsafe { builder.launch(cfg)? };
    }

    let pixels_u32 = canvas_d.copy_to_host_on(stream)?;
    // Copy back into the user buffer in-place.
    let bytes = unsafe {
        std::slice::from_raw_parts(pixels_u32.as_ptr() as *const u8, want_bytes)
    };
    canvas.copy_from_slice(bytes);
    Ok(())
}

/// Default-stream convenience wrapper. Calls from multiple CPU threads will
/// serialize on the GPU; use [`fill_polygons_host_on`] with per-thread streams
/// to overlap.
pub fn fill_polygons_host(
    ctx: &ContextHandle,
    canvas: &mut [u8],
    img_w: u32,
    img_h: u32,
    polygons: &[PolygonInput<'_>],
    clip: Option<(i32, i32, i32, i32)>,
) -> Result<()> {
    fill_polygons_host_on(ctx, ctx.stream(), canvas, img_w, img_h, polygons, clip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack_rgba;

    /// Inlined CPU reference — direct port of `fill_polygon` in
    /// `crates/rustwx-render/src/draw.rs`. Drift detector for upstream
    /// changes to the reference algorithm.
    fn cpu_fill(
        canvas: &mut [u8],
        img_w: u32,
        img_h: u32,
        rings: &[Vec<(f64, f64)>],
        color: u32,
        clip: Option<(i32, i32, i32, i32)>,
    ) {
        let src_a = ((color >> 24) & 0xFF) as u8;
        if rings.is_empty() || src_a == 0 {
            return;
        }
        let img_w_i = img_w as i32;
        let img_h_i = img_h as i32;
        let (cx0, cy0, cx1, cy1) = match clip {
            Some((x0, y0, x1, y1)) => (x0.max(0), y0.max(0), x1.min(img_w_i - 1), y1.min(img_h_i - 1)),
            None => (0, 0, img_w_i - 1, img_h_i - 1),
        };
        if cx1 < cx0 || cy1 < cy0 {
            return;
        }
        let mut y_min = f64::INFINITY;
        let mut y_max = f64::NEG_INFINITY;
        for ring in rings {
            for &(_, y) in ring {
                if y.is_finite() {
                    y_min = y_min.min(y);
                    y_max = y_max.max(y);
                }
            }
        }
        if !y_min.is_finite() || !y_max.is_finite() || y_max < cy0 as f64 {
            return;
        }
        let y0 = (y_min.floor() as i32).max(cy0);
        let y1 = (y_max.ceil() as i32).min(cy1);
        if y1 < y0 {
            return;
        }

        #[derive(Clone)]
        struct Edge { y_min: f64, y_max: f64, x: f64, dx: f64 }
        let mut edges: Vec<Edge> = Vec::new();
        for ring in rings {
            let n = ring.len();
            if n < 2 { continue; }
            for i in 0..n {
                let (ax, ay) = ring[i];
                let (bx, by) = ring[(i + 1) % n];
                if !ax.is_finite() || !ay.is_finite() || !bx.is_finite() || !by.is_finite() { continue; }
                if (ay - by).abs() < 1e-9 { continue; }
                let (lo_y, hi_y, lo_x, hi_x) = if ay < by { (ay, by, ax, bx) } else { (by, ay, bx, ax) };
                let dx = (hi_x - lo_x) / (hi_y - lo_y);
                edges.push(Edge { y_min: lo_y, y_max: hi_y, x: lo_x, dx });
            }
        }
        if edges.is_empty() { return; }

        let mut xs: Vec<f64> = Vec::with_capacity(edges.len());
        for y in y0..=y1 {
            let yf = y as f64 + 0.5;
            xs.clear();
            for e in &edges {
                if yf >= e.y_min && yf < e.y_max {
                    xs.push(e.x + (yf - e.y_min) * e.dx);
                }
            }
            if xs.len() < 2 { continue; }
            xs.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let mut i = 0;
            while i + 1 < xs.len() {
                let xa = (xs[i].max(cx0 as f64)).ceil() as i32;
                let xb = (xs[i + 1].min(cx1 as f64)).floor() as i32;
                if xb >= xa {
                    for x in xa..=xb {
                        let off = (y as usize * img_w as usize + x as usize) * 4;
                        blend_one(&mut canvas[off..off + 4], color);
                    }
                }
                i += 2;
            }
        }
    }

    fn blend_one(dst: &mut [u8], src: u32) {
        let sa = ((src >> 24) & 0xFF) as u8;
        if sa == 0 { return; }
        let sr = (src & 0xFF) as u8;
        let sg = ((src >> 8) & 0xFF) as u8;
        let sb = ((src >> 16) & 0xFF) as u8;
        if sa == 255 {
            dst[0] = sr;
            dst[1] = sg;
            dst[2] = sb;
            dst[3] = 255;
            return;
        }
        let a = sa as f64 / 255.0;
        let inv = 1.0 - a;
        dst[0] = (sr as f64 * a + dst[0] as f64 * inv).round() as u8;
        dst[1] = (sg as f64 * a + dst[1] as f64 * inv).round() as u8;
        dst[2] = (sb as f64 * a + dst[2] as f64 * inv).round() as u8;
        dst[3] = 255;
    }

    #[test]
    fn polygon_fill_matches_cpu() {
        let ctx = match rustwx_cuda_core::global() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skip: no CUDA context ({e})");
                return;
            }
        };

        let img_w: u32 = 128;
        let img_h: u32 = 96;
        let n_bytes = (img_w * img_h * 4) as usize;
        // Pre-fill background with a non-zero color so we can verify the
        // alpha-blend path keeps unfilled pixels and blends filled ones.
        let mut canvas_gpu = vec![0u8; n_bytes];
        for px in canvas_gpu.chunks_exact_mut(4) {
            px[0] = 32;
            px[1] = 64;
            px[2] = 96;
            px[3] = 255;
        }
        let mut canvas_cpu = canvas_gpu.clone();

        // Polygon 1: opaque triangle.
        let tri: Vec<Vec<(f64, f64)>> = vec![vec![
            (10.0, 10.0),
            (80.0, 20.0),
            (40.0, 70.0),
        ]];
        let tri_color = pack_rgba(220, 30, 50, 255);

        // Polygon 2: alpha quad with a hole — outer rectangle CCW, inner
        // rectangle CW. Even-odd fills the donut, leaving the hole.
        let quad_outer: Vec<(f64, f64)> = vec![
            (50.0, 30.0),
            (110.0, 30.0),
            (110.0, 80.0),
            (50.0, 80.0),
        ];
        let quad_hole: Vec<(f64, f64)> = vec![
            (70.0, 45.0),
            (70.0, 65.0),
            (95.0, 65.0),
            (95.0, 45.0),
        ];
        let quad: Vec<Vec<(f64, f64)>> = vec![quad_outer, quad_hole];
        let quad_color = pack_rgba(40, 200, 120, 128);

        // Polygon 3: clipped polygon — exercises the clip rect path.
        let clipped: Vec<Vec<(f64, f64)>> = vec![vec![
            (-20.0, 50.0),
            (40.0, 30.0),
            (40.0, 90.0),
            (-20.0, 90.0),
        ]];
        let clipped_color = pack_rgba(255, 255, 0, 200);

        let clip = Some((5_i32, 5_i32, (img_w - 6) as i32, (img_h - 6) as i32));

        // CPU reference
        cpu_fill(&mut canvas_cpu, img_w, img_h, &tri, tri_color, clip);
        cpu_fill(&mut canvas_cpu, img_w, img_h, &quad, quad_color, clip);
        cpu_fill(&mut canvas_cpu, img_w, img_h, &clipped, clipped_color, clip);

        // GPU
        let polys = vec![
            PolygonInput { rings: &tri,     color_packed: tri_color },
            PolygonInput { rings: &quad,    color_packed: quad_color },
            PolygonInput { rings: &clipped, color_packed: clipped_color },
        ];
        fill_polygons_host(&ctx, &mut canvas_gpu, img_w, img_h, &polys, clip)
            .expect("CUDA polygon fill failed");

        // Compare. Tolerate ≤ 1 LSB per channel (rounding diffs near edges).
        let mut diff_pixels = 0usize;
        let mut max_chan_delta = 0u8;
        let n_pixels = (img_w * img_h) as usize;
        for p in 0..n_pixels {
            let off = p * 4;
            let mut local_max = 0u8;
            for c in 0..4 {
                let d = (canvas_cpu[off + c] as i32 - canvas_gpu[off + c] as i32).unsigned_abs() as u8;
                if d > local_max { local_max = d; }
            }
            if local_max > 0 {
                diff_pixels += 1;
                if local_max > max_chan_delta { max_chan_delta = local_max; }
            }
        }
        eprintln!(
            "polygon_fill: {} / {} pixels differ; max channel delta = {}",
            diff_pixels, n_pixels, max_chan_delta
        );

        // The kernel intentionally mirrors CPU rounding exactly — we expect
        // 0 delta. Allow ≤ 1 LSB per channel for safety on non-deterministic
        // FP reordering by the device.
        assert!(
            max_chan_delta <= 1,
            "max channel delta {} > 1 (kernel diverges from CPU rounding)",
            max_chan_delta
        );
        // Bound the diff-pixel count too: if the algorithm itself drifted
        // (wrong pixels filled at all), most edge pixels would land in the
        // diff bucket. Even at ≤ 1 LSB, we expect this to be small.
        let pct = diff_pixels as f64 * 100.0 / n_pixels as f64;
        assert!(
            pct <= 2.0,
            "{:.3}% pixels differ — exceeds 2% tolerance",
            pct
        );
    }
}
