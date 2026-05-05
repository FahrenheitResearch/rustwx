// Rasterize a 2D `f64` grid on a projected mesh. Mirrors
// `rustwx_render::rasterize::rasterize_projected_grid` + `rasterize_triangle`.
//
// Layout: one CUDA thread per (j, i) quad of the input grid. Each thread
// rasterizes both triangles of its quad in series. Quads are independent
// of each other except along shared edges, where two adjacent quads can
// both write the same output pixel — we accept the race because the field
// is smoothly varying so the two interp values differ by <1 LSB anywhere
// the field is well-behaved.
//
// Threads where any of the four corners has `valid == 0` (the CPU
// `Some(_)` filter) skip the quad entirely — same as CPU.
//
// Colormap helpers (`colormap_lookup`) are defined in the rasterize_grid.cu
// module already; this file is concatenated AFTER constants.cuh by the Rust
// loader, so we redefine the lookup here to keep the modules independent.

__device__ inline unsigned int proj_colormap_lookup(
    double value,
    const double* __restrict__ levels,
    int n_levels,
    const unsigned int* __restrict__ colors,
    int n_intervals,
    int has_under, unsigned int under_color,
    int has_over,  unsigned int over_color,
    int has_mask_below, double mask_below
) {
    if (isnan(value)) return 0u;
    if (has_mask_below && value < mask_below) return 0u;
    if (n_levels < 2 || n_intervals < 1) return 0u;
    if (value < levels[0]) {
        return has_under ? under_color : 0u;
    }
    int lo = 0, hi = n_levels;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        if (levels[mid] <= value) lo = mid + 1;
        else                       hi = mid;
    }
    int idx = lo;
    if (idx <= n_intervals) {
        int ci = idx - 1;
        if (ci < 0) ci = 0;
        if (ci > n_intervals - 1) ci = n_intervals - 1;
        return colors[ci];
    }
    return has_over ? over_color : colors[n_intervals - 1];
}

__device__ inline double edge_fn(
    double ax, double ay,
    double bx, double by,
    double pxv, double pyv
) {
    return (pxv - ax) * (by - ay) - (pyv - ay) * (bx - ax);
}

// Rasterize one triangle into `out` [img_h][img_w]. Mirrors `rasterize_triangle`
// — same bbox clamping, same edge-function barycentric, same `-1e-6` slack on
// inside-test, same alpha>0 gate before write.
__device__ inline void raster_triangle(
    double p0x, double p0y, double v0,
    double p1x, double p1y, double v1,
    double p2x, double p2y, double v2,
    const double* levels,        int n_levels,
    const unsigned int* colors,  int n_intervals,
    int has_under, unsigned int under_color,
    int has_over,  unsigned int over_color,
    int has_mask_below, double mask_below,
    unsigned int* out, int img_w, int img_h
) {
    if (!isfinite(v0) || !isfinite(v1) || !isfinite(v2)) return;

    double minx_d = fmin(fmin(p0x, p1x), p2x);
    double maxx_d = fmax(fmax(p0x, p1x), p2x);
    double miny_d = fmin(fmin(p0y, p1y), p2y);
    double maxy_d = fmax(fmax(p0y, p1y), p2y);

    int min_x = (int)floor(minx_d);
    int max_x = (int)ceil(maxx_d);
    int min_y = (int)floor(miny_d);
    int max_y = (int)ceil(maxy_d);
    if (min_x < 0) min_x = 0;
    if (min_y < 0) min_y = 0;
    if (max_x > img_w - 1) max_x = img_w - 1;
    if (max_y > img_h - 1) max_y = img_h - 1;
    if (min_x > max_x || min_y > max_y) return;

    double area = edge_fn(p0x, p0y, p1x, p1y, p2x, p2y);
    if (fabs(area) < 1e-9) return;
    double inv_area = 1.0 / area;

    for (int py = min_y; py <= max_y; ++py) {
        for (int px = min_x; px <= max_x; ++px) {
            double sx = (double)px + 0.5;
            double sy = (double)py + 0.5;
            double w0 = edge_fn(p1x, p1y, p2x, p2y, sx, sy) * inv_area;
            double w1 = edge_fn(p2x, p2y, p0x, p0y, sx, sy) * inv_area;
            double w2 = edge_fn(p0x, p0y, p1x, p1y, sx, sy) * inv_area;
            if (w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6) continue;

            double value = v0 * w0 + v1 * w1 + v2 * w2;
            unsigned int rgba = proj_colormap_lookup(
                value,
                levels, n_levels,
                colors, n_intervals,
                has_under, under_color,
                has_over,  over_color,
                has_mask_below, mask_below
            );
            // Match CPU: skip pixels with a fully-transparent colormap result.
            if (((rgba >> 24) & 0xFFu) == 0u) continue;
            out[(size_t)py * (size_t)img_w + (size_t)px] = rgba;
        }
    }
}

// Each thread = one quad `(j, i)`. Rasterizes both triangles in series.
extern "C" __global__
void rasterize_projected_grid_kernel(
    const double* __restrict__ data,    // [ny][nx]
    int ny, int nx,
    const double* __restrict__ pix_x,   // [ny][nx]  pixel-space X per grid point
    const double* __restrict__ pix_y,   // [ny][nx]  pixel-space Y per grid point
    const int*    __restrict__ valid,   // [ny][nx]  1 if Some(_), 0 if None
    const double* __restrict__ levels,
    int n_levels,
    const unsigned int* __restrict__ colors,
    int n_intervals,
    int has_under, unsigned int under_color,
    int has_over,  unsigned int over_color,
    int has_mask_below, double mask_below,
    unsigned int* __restrict__ out,
    int img_w, int img_h
) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;  // quad column
    int j = blockIdx.y * blockDim.y + threadIdx.y;  // quad row
    if (i >= nx - 1 || j >= ny - 1) return;
    if (ny < 2 || nx < 2) return;

    int idx00 = j * nx + i;
    int idx10 = j * nx + (i + 1);
    int idx01 = (j + 1) * nx + i;
    int idx11 = (j + 1) * nx + (i + 1);

    if (!valid[idx00] || !valid[idx10] || !valid[idx01] || !valid[idx11]) return;

    double p00x = pix_x[idx00], p00y = pix_y[idx00];
    double p10x = pix_x[idx10], p10y = pix_y[idx10];
    double p01x = pix_x[idx01], p01y = pix_y[idx01];
    double p11x = pix_x[idx11], p11y = pix_y[idx11];

    double v00 = data[idx00];
    double v10 = data[idx10];
    double v01 = data[idx01];
    double v11 = data[idx11];

    // CPU draws triangle (p00,p10,p11) then triangle (p00,p11,p01) — same
    // order here so that the (rare) overlap on shared edges follows the same
    // last-write-wins ordering inside a single thread.
    raster_triangle(
        p00x, p00y, v00,
        p10x, p10y, v10,
        p11x, p11y, v11,
        levels, n_levels, colors, n_intervals,
        has_under, under_color, has_over, over_color,
        has_mask_below, mask_below,
        out, img_w, img_h
    );
    raster_triangle(
        p00x, p00y, v00,
        p11x, p11y, v11,
        p01x, p01y, v01,
        levels, n_levels, colors, n_intervals,
        has_under, under_color, has_over, over_color,
        has_mask_below, mask_below,
        out, img_w, img_h
    );
}
