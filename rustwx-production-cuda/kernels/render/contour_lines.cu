// GPU port of `rustwx_render::render::draw_contours` — the marching-squares
// isoline rasterizer used by the contour-line overlay.
//
// CPU pipeline (see `crates/rustwx-render/src/render.rs`):
//   for each level L:
//     for each (j, i) cell:
//       1. Read 4 corner positions (`pixel_points` if available, else
//          linear `grid_to_pixel`) and 4 corner values from `overlay.data`.
//       2. Walk the 4 edges in order
//             (p0->p1), (p1->p2), (p2->p3), (p3->p0)
//          and call `interp_point` to get the linear-interp crossing of L on
//          each edge. Skip edges where both endpoints are on the same side
//          of L. Push hits into a length-4 buffer.
//       3. If at least 2 hits, draw segment[0]->segment[1]; if exactly 4
//          hits (saddle case), also draw segment[2]->segment[3].
//       4. Each segment is drawn with `draw::draw_line_aa_width` — same
//          coverage/blend math as `linework.cu`.
//
// GPU layout: ONE THREAD PER (cell, level) pair. Threads:
//   - Recompute the cell intersections (cheap; avoids a separate
//     compaction pass + a second kernel).
//   - For each emitted segment, run the same per-pixel coverage loop as
//     `linework_aa_kernel` and atomically blend into the canvas.
//
// Race ordering: marching squares emits at most one isoline per (cell,
// level), and isolines for different levels never share pixels at the
// "interior" of a cell — only at sub-pixel boundary crossings. We use the
// same atomic-CAS blend as linework so concurrent writes composite
// correctly; the only divergence vs CPU is that overlap order between
// different levels is not guaranteed (CPU iterates level-major). Pixels
// touched by a single level resolve identically; pixels touched by
// multiple levels can disagree by the same <=2/255 channel delta the
// linework kernel allows.

__device__ __forceinline__ unsigned int contour_pack_rgba8(
    unsigned int r, unsigned int g, unsigned int b, unsigned int a
) {
    return (r & 0xFFu)
         | ((g & 0xFFu) << 8)
         | ((b & 0xFFu) << 16)
         | ((a & 0xFFu) << 24);
}

__device__ __forceinline__ double contour_dist_to_segment(
    double px, double py,
    double x0, double y0,
    double x1, double y1
) {
    double dx = x1 - x0;
    double dy = y1 - y0;
    double len_sq = dx * dx + dy * dy;
    if (len_sq <= 1e-12) {
        double ox = px - x0;
        double oy = py - y0;
        return sqrt(ox * ox + oy * oy);
    }
    double t = ((px - x0) * dx + (py - y0) * dy) / len_sq;
    if (t < 0.0) t = 0.0;
    else if (t > 1.0) t = 1.0;
    double proj_x = x0 + t * dx;
    double proj_y = y0 + t * dy;
    double ox = px - proj_x;
    double oy = py - proj_y;
    return sqrt(ox * ox + oy * oy);
}

// Mirrors `blend_pixel(_coverage)` from rustwx-render/src/draw.rs and the
// helper in linework.cu. `scaled_alpha` is the per-pixel alpha after
// coverage scaling; the caller has already rounded.
__device__ __forceinline__ void contour_atomic_blend_pixel(
    unsigned int* canvas,
    int x, int y, int img_w, int img_h,
    unsigned int src_r, unsigned int src_g, unsigned int src_b,
    unsigned int scaled_alpha
) {
    if (x < 0 || y < 0 || x >= img_w || y >= img_h) return;
    if (scaled_alpha == 0u) return;

    size_t idx = (size_t)y * (size_t)img_w + (size_t)x;
    unsigned int* slot = canvas + idx;

    if (scaled_alpha >= 255u) {
        atomicExch(slot, contour_pack_rgba8(src_r, src_g, src_b, 255u));
        return;
    }

    double a = (double)scaled_alpha / 255.0;
    double inv = 1.0 - a;

    unsigned int old = *slot;
    while (true) {
        unsigned int dr = old & 0xFFu;
        unsigned int dg = (old >> 8) & 0xFFu;
        unsigned int db = (old >> 16) & 0xFFu;
        unsigned int nr = (unsigned int)(((double)src_r * a + (double)dr * inv) + 0.5);
        unsigned int ng = (unsigned int)(((double)src_g * a + (double)dg * inv) + 0.5);
        unsigned int nb = (unsigned int)(((double)src_b * a + (double)db * inv) + 0.5);
        if (nr > 255u) nr = 255u;
        if (ng > 255u) ng = 255u;
        if (nb > 255u) nb = 255u;
        unsigned int nv = contour_pack_rgba8(nr, ng, nb, 255u);
        unsigned int prev = atomicCAS(slot, old, nv);
        if (prev == old) break;
        old = prev;
    }
}

// Same per-pixel coverage loop as `linework_aa_kernel` but factored into a
// device helper so the contour kernel can call it twice per cell (one or
// two segments, depending on saddle/regular).
__device__ __forceinline__ void contour_draw_segment_aa(
    double x0, double y0,
    double x1, double y1,
    int width,
    unsigned int src_r, unsigned int src_g, unsigned int src_b, unsigned int src_a,
    unsigned int* canvas, int img_w, int img_h
) {
    if (!isfinite(x0) || !isfinite(y0) || !isfinite(x1) || !isfinite(y1)) return;
    if (src_a == 0u) return;
    if (width < 1) width = 1;

    double half_width = (double)width * 0.5;
    double radius = half_width + 1.0;

    double minx_d = (x0 < x1 ? x0 : x1) - radius;
    double maxx_d = (x0 > x1 ? x0 : x1) + radius;
    double miny_d = (y0 < y1 ? y0 : y1) - radius;
    double maxy_d = (y0 > y1 ? y0 : y1) + radius;

    int min_x = (int)floor(minx_d);
    int max_x = (int)ceil(maxx_d);
    int min_y = (int)floor(miny_d);
    int max_y = (int)ceil(maxy_d);

    if (min_x < 0) min_x = 0;
    if (min_y < 0) min_y = 0;
    if (max_x > img_w - 1) max_x = img_w - 1;
    if (max_y > img_h - 1) max_y = img_h - 1;
    if (min_x > max_x || min_y > max_y) return;

    double cov_max = half_width + 0.5;

    for (int py = min_y; py <= max_y; ++py) {
        for (int px = min_x; px <= max_x; ++px) {
            double sx = (double)px + 0.5;
            double sy = (double)py + 0.5;
            double d = contour_dist_to_segment(sx, sy, x0, y0, x1, y1);
            double coverage = cov_max - d;
            if (coverage <= 0.0) continue;
            if (coverage > 1.0) coverage = 1.0;

            unsigned int scaled_alpha =
                (unsigned int)((double)src_a * coverage + 0.5);
            if (scaled_alpha == 0u) continue;

            contour_atomic_blend_pixel(
                canvas, px, py, img_w, img_h,
                src_r, src_g, src_b, scaled_alpha
            );
        }
    }
}

// Mirrors `interp_point`: linear interp of `level` between the two
// endpoints. Returns 1 on hit, 0 on miss. `out_x/out_y` valid only on hit.
__device__ __forceinline__ int contour_interp_edge(
    double x0, double y0, double v0,
    double x1, double y1, double v1,
    double level,
    double* out_x, double* out_y
) {
    if (!isfinite(v0) || !isfinite(v1)) return 0;
    double d0 = v0 - level;
    double d1 = v1 - level;
    if ((d0 > 0.0 && d1 > 0.0) || (d0 < 0.0 && d1 < 0.0)) return 0;
    double dv = v1 - v0;
    if (fabs(dv) < 1e-12) {
        *out_x = (x0 + x1) * 0.5;
        *out_y = (y0 + y1) * 0.5;
        return 1;
    }
    double t = (level - v0) / dv;
    *out_x = x0 + (x1 - x0) * t;
    *out_y = y0 + (y1 - y0) * t;
    return 1;
}

// One thread per (cell, level). Cells flatten as `cell = j * (nx-1) + i`
// for `j in [0, ny-1)` and `i in [0, nx-1)`; `level` is `0..n_levels`.
//
// Inputs:
//   data            [ny][nx]   scalar field values (f64)
//   pix_x, pix_y    [ny][nx]   per-corner pixel coordinates. The CPU's
//                              `pixel_points: Option<&[Option<(f64,f64)>]>`
//                              is materialized host-side: when None, the
//                              caller fills pix_x/pix_y from the linear
//                              `grid_to_pixel` mapping; when Some, holes
//                              are signaled via valid[] = 0.
//   valid           [ny][nx]   1 = corner usable, 0 = hole. A cell is
//                              skipped if any of its 4 corners is invalid
//                              (matches `contour_cell_corners`).
//   levels          [n_levels] isoline levels (f64)
//   color, width               style (one per overlay, broadcast across
//                              all cells/levels)
//   canvas                     RGBA8 packed as u32, length img_w*img_h.
extern "C" __global__
void contour_lines_kernel(
    const double*       __restrict__ data,
    int ny, int nx,
    const double*       __restrict__ pix_x,
    const double*       __restrict__ pix_y,
    const int*          __restrict__ valid,
    const double*       __restrict__ levels,
    int n_levels,
    unsigned int        color,
    int                 width,
    unsigned int*       __restrict__ canvas,
    int img_w, int img_h
) {
    int cell = blockIdx.x * blockDim.x + threadIdx.x;
    int li   = blockIdx.y * blockDim.y + threadIdx.y;
    if (li >= n_levels) return;
    if (nx < 2 || ny < 2) return;

    int cells_per_row = nx - 1;
    int total_cells = cells_per_row * (ny - 1);
    if (cell >= total_cells) return;

    int i = cell % cells_per_row;
    int j = cell / cells_per_row;

    int idx00 = j * nx + i;
    int idx10 = j * nx + (i + 1);
    int idx11 = (j + 1) * nx + (i + 1);
    int idx01 = (j + 1) * nx + i;

    if (!valid[idx00] || !valid[idx10] || !valid[idx11] || !valid[idx01]) return;

    double x0 = pix_x[idx00], y0 = pix_y[idx00], v0 = data[idx00];
    double x1 = pix_x[idx10], y1 = pix_y[idx10], v1 = data[idx10];
    double x2 = pix_x[idx11], y2 = pix_y[idx11], v2 = data[idx11];
    double x3 = pix_x[idx01], y3 = pix_y[idx01], v3 = data[idx01];

    double level = levels[li];

    // Walk edges (p0->p1), (p1->p2), (p2->p3), (p3->p0) in CPU order.
    double pts_x[4];
    double pts_y[4];
    int count = 0;

    double hx, hy;
    if (contour_interp_edge(x0, y0, v0, x1, y1, v1, level, &hx, &hy)) {
        pts_x[count] = hx; pts_y[count] = hy; ++count;
    }
    if (contour_interp_edge(x1, y1, v1, x2, y2, v2, level, &hx, &hy)) {
        pts_x[count] = hx; pts_y[count] = hy; ++count;
    }
    if (contour_interp_edge(x2, y2, v2, x3, y3, v3, level, &hx, &hy)) {
        pts_x[count] = hx; pts_y[count] = hy; ++count;
    }
    if (contour_interp_edge(x3, y3, v3, x0, y0, v0, level, &hx, &hy)) {
        pts_x[count] = hx; pts_y[count] = hy; ++count;
    }

    if (count < 2) return;

    unsigned int src_r = color & 0xFFu;
    unsigned int src_g = (color >> 8) & 0xFFu;
    unsigned int src_b = (color >> 16) & 0xFFu;
    unsigned int src_a = (color >> 24) & 0xFFu;

    // CPU emits segment (0,1) always; if count==4 also (2,3). count==3 is
    // impossible for a closed marching-squares cell, but `interp_point`
    // can produce it on the degenerate boundary case (an exact-equal
    // corner at `level`); CPU still draws (0,1) only — match that.
    contour_draw_segment_aa(
        pts_x[0], pts_y[0], pts_x[1], pts_y[1],
        width,
        src_r, src_g, src_b, src_a,
        canvas, img_w, img_h
    );
    if (count == 4) {
        contour_draw_segment_aa(
            pts_x[2], pts_y[2], pts_x[3], pts_y[3],
            width,
            src_r, src_g, src_b, src_a,
            canvas, img_w, img_h
        );
    }
}
