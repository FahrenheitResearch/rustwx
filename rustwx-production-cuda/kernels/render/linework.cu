// GPU port of `rustwx_render::draw::draw_polyline_aa` / `draw_line_aa_kernel`,
// the inner loop driven by `render::draw_projected_lines`. Each polyline is
// a chain of segments rendered as an alpha-blended thick line over an RGBA8
// canvas.
//
// Layout: one CUDA thread per line SEGMENT. Each thread:
//   1. Loads the segment endpoints (already projected to canvas pixel space)
//      and looks up its polyline's `color` + `width`.
//   2. Computes the same pixel bbox as the CPU (`width/2 + 1` slack).
//   3. For each pixel center inside the bbox computes
//        coverage = clamp(width/2 + 0.5 - dist_to_segment, 0, 1)
//      then `scaled_alpha = round(color.a * coverage)`.
//   4. Blends into the canvas with an atomic-CAS loop so overlapping
//      segments composite correctly even though many threads target the
//      same pixel (the CPU equivalent is sequential `blend_pixel`).
//
// Blend math mirrors `blend_pixel` in `crates/rustwx-render/src/draw.rs`:
//   if alpha == 255 -> opaque overwrite (output alpha forced to 255)
//   else if alpha != 0 ->
//       a   = src_a / 255
//       inv = 1 - a
//       dst.r = round(src.r * a + dst.r * inv)
//       dst.g = round(src.g * a + dst.g * inv)
//       dst.b = round(src.b * a + dst.b * inv)
//       dst.a = 255
//
// Packing matches `image::Rgba` byte layout `[R, G, B, A]` reinterpreted
// little-endian as `u32` (same as `pack_rgba` on the Rust side and the byte
// layout used by `rasterize_projected_grid.cu`).

__device__ __forceinline__ unsigned int pack_rgba8(
    unsigned int r, unsigned int g, unsigned int b, unsigned int a
) {
    return (r & 0xFFu)
         | ((g & 0xFFu) << 8)
         | ((b & 0xFFu) << 16)
         | ((a & 0xFFu) << 24);
}

__device__ __forceinline__ double dist_to_segment(
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

// Mirrors `blend_pixel(_coverage)`. `scaled_alpha` is the per-pixel alpha
// AFTER multiplying by coverage (caller pre-scales). Atomic CAS loop ensures
// concurrent threads touching the same pixel composite serially without
// dropping writes.
__device__ __forceinline__ void atomic_blend_pixel(
    unsigned int* canvas,
    int x, int y, int img_w, int img_h,
    unsigned int src_r, unsigned int src_g, unsigned int src_b,
    unsigned int scaled_alpha
) {
    if (x < 0 || y < 0 || x >= img_w || y >= img_h) return;
    if (scaled_alpha == 0u) return;

    size_t idx = (size_t)y * (size_t)img_w + (size_t)x;
    unsigned int* slot = canvas + idx;

    // Always use the CAS loop (even for the opaque case) so concurrent
    // partial-alpha and opaque writes within the same polyline kernel launch
    // don't clobber each other via a non-CAS `atomicExch`. When
    // `scaled_alpha == 255` the blend math reduces to `src` exactly
    // (`a == 1.0`, `inv == 0.0`), matching `blend_pixel`'s opaque fast path.
    double a = (double)scaled_alpha / 255.0;
    double inv = 1.0 - a;

    unsigned int old = atomicAdd(slot, 0u); // atomic load
    while (true) {
        unsigned int dr = old & 0xFFu;
        unsigned int dg = (old >> 8) & 0xFFu;
        unsigned int db = (old >> 16) & 0xFFu;
        // Output alpha matches `blend_pixel`: forced to 255 once any
        // partial-alpha pixel is written. (CPU writes 255 unconditionally
        // in the partial-alpha branch.)
        unsigned int nr = (unsigned int)(((double)src_r * a + (double)dr * inv) + 0.5);
        unsigned int ng = (unsigned int)(((double)src_g * a + (double)dg * inv) + 0.5);
        unsigned int nb = (unsigned int)(((double)src_b * a + (double)db * inv) + 0.5);
        if (nr > 255u) nr = 255u;
        if (ng > 255u) ng = 255u;
        if (nb > 255u) nb = 255u;
        unsigned int nv = pack_rgba8(nr, ng, nb, 255u);
        unsigned int prev = atomicCAS(slot, old, nv);
        if (prev == old) break;
        old = prev;
    }
}

// One thread = one segment. Segments are stored as a flat SoA across all
// polylines: `seg_x0[i], seg_y0[i], seg_x1[i], seg_y1[i], seg_poly[i]` ==>
// polyline index. Per-polyline color/width are looked up via `seg_poly[i]`.
extern "C" __global__
void linework_aa_kernel(
    const double*       __restrict__ seg_x0,
    const double*       __restrict__ seg_y0,
    const double*       __restrict__ seg_x1,
    const double*       __restrict__ seg_y1,
    const int*          __restrict__ seg_poly,
    int n_segments,
    const unsigned int* __restrict__ poly_color,    // packed RGBA8, one per polyline
    const int*          __restrict__ poly_width,    // stroke width in pixels, one per polyline
    unsigned int*       __restrict__ canvas,
    int img_w, int img_h
) {
    int s = blockIdx.x * blockDim.x + threadIdx.x;
    if (s >= n_segments) return;

    double x0 = seg_x0[s];
    double y0 = seg_y0[s];
    double x1 = seg_x1[s];
    double y1 = seg_y1[s];

    if (!isfinite(x0) || !isfinite(y0) || !isfinite(x1) || !isfinite(y1)) return;

    int p = seg_poly[s];
    unsigned int col = poly_color[p];
    int width = poly_width[p];
    if (width < 1) width = 1;

    unsigned int src_r = col & 0xFFu;
    unsigned int src_g = (col >> 8) & 0xFFu;
    unsigned int src_b = (col >> 16) & 0xFFu;
    unsigned int src_a = (col >> 24) & 0xFFu;
    if (src_a == 0u) return;

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
            double d = dist_to_segment(sx, sy, x0, y0, x1, y1);
            double coverage = cov_max - d;
            if (coverage <= 0.0) continue;
            if (coverage > 1.0) coverage = 1.0;

            // Mirror `blend_pixel_coverage`: scaled_alpha = round(color.a * coverage)
            unsigned int scaled_alpha =
                (unsigned int)((double)src_a * coverage + 0.5);
            if (scaled_alpha == 0u) continue;

            atomic_blend_pixel(
                canvas, px, py, img_w, img_h,
                src_r, src_g, src_b, scaled_alpha
            );
        }
    }
}
