// Scanline polygon fill. Mirrors `rustwx_render::draw::fill_polygon`.
//
// CPU algorithm (see crates/rustwx-render/src/draw.rs `fill_polygon`):
//   1. Pre-extract every non-horizontal edge of every ring as
//      (y_min, y_max, x_at_y_min, dx_per_dy).
//   2. For each scanline y in [y0, y1], evaluate yf = y + 0.5,
//      collect x-intersections from edges whose half-open span
//      [y_min, y_max) contains yf.
//   3. Sort intersections ascending. Walk pairs (i, i+1):
//        xa = ceil(max(xs[i], cx0)),  xb = floor(min(xs[i+1], cx1))
//      and fill pixels in [xa, xb] (inclusive). Even-odd rule —
//      multi-ring polygons get holes for free when outer + inner
//      rings have opposite winding.
//   4. Alpha-blend the source color over each filled pixel
//      (opaque path = straight overwrite; partial alpha = "over"
//      compositing with rounded u8 result; a == 0 = no-op).
//
// GPU layout: one thread per scanline. Each thread walks the edge
// table, collects intersections into a thread-local stack array,
// sorts (insertion sort — typical polygon: <16 edges crossing any
// one scanline), and fills its row. Threads are independent; no
// races, no atomics, deterministic byte-for-byte output up to the
// floating-point edge-x calc, which is identical to CPU.
//
// Ringless / empty-edge / fully-clipped cases are handled by the
// Rust wrapper before launch.

#define POLYFILL_MAX_INTERSECTIONS 256

__device__ __forceinline__ unsigned int polyfill_blend(
    unsigned int dst, unsigned int src
) {
    // src/dst are packed RGBA8 (R = byte 0 .. A = byte 3), matching
    // image::Rgba layout. CPU `blend_pixel` shortcuts a == 255 to a
    // straight write and a == 0 to a no-op; we mirror that, with a
    // rounded `over` compositor for the in-between case.
    unsigned int sa = (src >> 24) & 0xFFu;
    if (sa == 0u) return dst;
    if (sa == 255u) return src;

    unsigned int sr = src & 0xFFu;
    unsigned int sg = (src >> 8) & 0xFFu;
    unsigned int sb = (src >> 16) & 0xFFu;

    unsigned int dr = dst & 0xFFu;
    unsigned int dg = (dst >> 8) & 0xFFu;
    unsigned int db = (dst >> 16) & 0xFFu;

    double a = (double)sa / 255.0;
    double inv = 1.0 - a;

    unsigned int br = (unsigned int)floor((double)sr * a + (double)dr * inv + 0.5);
    unsigned int bg = (unsigned int)floor((double)sg * a + (double)dg * inv + 0.5);
    unsigned int bb = (unsigned int)floor((double)sb * a + (double)db * inv + 0.5);
    if (br > 255u) br = 255u;
    if (bg > 255u) bg = 255u;
    if (bb > 255u) bb = 255u;

    // CPU forces dst alpha to 255 in the partial-alpha branch.
    return br | (bg << 8) | (bb << 16) | (255u << 24);
}

// In-place insertion sort. n is the live count, expected small (typically
// 2–16). Worst case bounded by POLYFILL_MAX_INTERSECTIONS so register-pressure
// is fine for an O(n^2) loop.
__device__ __forceinline__ void polyfill_sort(double* xs, int n) {
    for (int i = 1; i < n; ++i) {
        double v = xs[i];
        int j = i - 1;
        while (j >= 0 && xs[j] > v) {
            xs[j + 1] = xs[j];
            --j;
        }
        xs[j + 1] = v;
    }
}

// Each thread = one scanline `y` in [y0, y1]. Walks the flattened edge table,
// fills its row in `out`. Edge table layout (SoA, length n_edges):
//   ey_min[k]  ey_max[k]  ex[k]  edx[k]   for edge k
// describing the segment from (lo_x, lo_y) to (lo_x + dx*(hi_y-lo_y), hi_y).
extern "C" __global__
void polygon_fill_scanline_kernel(
    int y0, int y1,                         // scanline range (inclusive)
    int cx0, int cy0, int cx1, int cy1,     // clip rect (inclusive pixel bounds)
    const double* __restrict__ ey_min,
    const double* __restrict__ ey_max,
    const double* __restrict__ ex,
    const double* __restrict__ edx,
    int n_edges,
    unsigned int color_packed,              // packed RGBA8 src color
    unsigned int* __restrict__ out,
    int img_w, int img_h
) {
    int tid = blockIdx.x * blockDim.x + threadIdx.x;
    int y = y0 + tid;
    if (y > y1) return;
    if (y < cy0 || y > cy1) return;
    if (y < 0 || y >= img_h) return;

    double yf = (double)y + 0.5;

    // Collect intersections.
    double xs[POLYFILL_MAX_INTERSECTIONS];
    int n = 0;
    for (int k = 0; k < n_edges; ++k) {
        double y_lo = ey_min[k];
        double y_hi = ey_max[k];
        if (yf >= y_lo && yf < y_hi) {
            if (n < POLYFILL_MAX_INTERSECTIONS) {
                xs[n++] = ex[k] + (yf - y_lo) * edx[k];
            }
            // If we exceed the cap (extremely complex polygon — e.g. >256
            // edges crossing one scanline), we silently drop the overflow.
            // The Rust wrapper warns once per launch when n_edges suggests
            // this is possible (see polygon_fill.rs).
        }
    }
    if (n < 2) return;

    polyfill_sort(xs, n);

    double cx0d = (double)cx0;
    double cx1d = (double)cx1;

    int i = 0;
    while (i + 1 < n) {
        double a = xs[i];
        double b = xs[i + 1];
        if (a < cx0d) a = cx0d;
        if (b > cx1d) b = cx1d;
        int xa = (int)ceil(a);
        int xb = (int)floor(b);
        if (xb >= xa) {
            // Final clamp to image bounds (cx0/cx1 are pre-clipped by Rust
            // but be defensive in case image extents and clip differ).
            if (xa < 0) xa = 0;
            if (xb > img_w - 1) xb = img_w - 1;
            size_t row_off = (size_t)y * (size_t)img_w;
            for (int x = xa; x <= xb; ++x) {
                size_t off = row_off + (size_t)x;
                unsigned int dst = out[off];
                out[off] = polyfill_blend(dst, color_packed);
            }
        }
        i += 2;
    }
}
