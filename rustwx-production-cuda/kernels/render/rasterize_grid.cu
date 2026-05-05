// Rasterize a 2D `f64` grid into a packed-RGBA8 image, mirroring
// `rustwx_render::rasterize::rasterize_grid` byte-for-byte where the inputs
// are finite, and `LeveledColormap::map` byte-for-byte for the colormap
// lookup.
//
// One thread per output pixel. Output is stored as `unsigned int` per pixel
// in little-endian RGBA byte layout (R = bits 0..8, G = 8..16, B = 16..24,
// A = 24..32) so reinterpreting the buffer as a byte array gives the same
// memory layout as `image::Rgba<u8>`.

__device__ inline unsigned int colormap_lookup(
    double value,
    const double* __restrict__ levels,
    int n_levels,                       // n_intervals + 1
    const unsigned int* __restrict__ colors,
    int n_intervals,                    // n_levels - 1
    int has_under, unsigned int under_color,
    int has_over,  unsigned int over_color,
    int has_mask_below, double mask_below
) {
    // NaN -> transparent. Matches `LeveledColormap::map` first branch.
    if (isnan(value)) return 0u;

    // mask_below cutoff -> transparent.
    if (has_mask_below && value < mask_below) return 0u;

    // Empty colormap is impossible here (caller guarantees), but guard anyway.
    if (n_levels < 2 || n_intervals < 1) return 0u;

    // Below first level boundary.
    if (value < levels[0]) {
        return has_under ? under_color : 0u;
    }

    // partition_point(|l| *l <= value): first index where levels[i] > value.
    int lo = 0;
    int hi = n_levels;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        if (levels[mid] <= value) lo = mid + 1;
        else                       hi = mid;
    }
    int idx = lo;

    // CPU code: `if idx <= n_intervals { colors[saturating_sub(1).min(...)] }`.
    // After the `value < levels[0]` early-out idx >= 1.
    if (idx <= n_intervals) {
        int ci = idx - 1;
        if (ci < 0) ci = 0;
        if (ci > n_intervals - 1) ci = n_intervals - 1;
        return colors[ci];
    }

    // value >= last level boundary.
    return has_over ? over_color : colors[n_intervals - 1];
}

__device__ inline double bilinear_sample(
    double v00, double v10, double v01, double v11,
    double fx, double fy
) {
    bool all_finite = isfinite(v00) && isfinite(v10)
                   && isfinite(v01) && isfinite(v11);
    if (all_finite) {
        double south = v00 * (1.0 - fx) + v10 * fx;
        double north = v01 * (1.0 - fx) + v11 * fx;
        return south * (1.0 - fy) + north * fy;
    }
    // CPU fallback: return the first finite corner, else NaN.
    if (isfinite(v00)) return v00;
    if (isfinite(v10)) return v10;
    if (isfinite(v01)) return v01;
    if (isfinite(v11)) return v11;
    return v00; // already NaN
}

// Each thread = one output pixel.
extern "C" __global__
void rasterize_grid_kernel(
    const double* __restrict__ data,        // [ny][nx] row-major, south-up
    int ny, int nx,
    const double* __restrict__ levels,      // [n_levels]
    int n_levels,
    const unsigned int* __restrict__ colors, // [n_intervals]
    int n_intervals,
    int has_under, unsigned int under_color,
    int has_over,  unsigned int over_color,
    int has_mask_below, double mask_below,
    unsigned int* __restrict__ out,          // [img_h][img_w] packed RGBA
    int img_w, int img_h
) {
    int px = blockIdx.x * blockDim.x + threadIdx.x;
    int py = blockIdx.y * blockDim.y + threadIdx.y;
    if (px >= img_w || py >= img_h) return;

    if (ny <= 0 || nx <= 0) {
        out[py * img_w + px] = 0u;
        return;
    }

    // Match CPU denominators exactly:
    //   x_den = max(img_w - 1, 1) as f64
    //   y_den = max(img_h - 1, 1) as f64
    //   gx_den = max(nx - 1, 1) as f64
    //   gy_den = max(ny - 1, 1) as f64
    double x_den  = (double)((img_w > 1) ? (img_w - 1) : 1);
    double y_den  = (double)((img_h > 1) ? (img_h - 1) : 1);
    double gx_den = (double)((nx    > 1) ? (nx    - 1) : 1);
    double gy_den = (double)((ny    > 1) ? (ny    - 1) : 1);

    double gx = (double)px / x_den * gx_den;
    // CPU does: (img_h.saturating_sub(1) - py) — i.e. flip vertically.
    int    py_flip_i = (img_h > 0 ? img_h - 1 : 0) - py;
    double gy = (double)py_flip_i / y_den * gy_den;

    // floor + clamp neighbour indices.
    int i0 = (int)floor(gx);
    int j0 = (int)floor(gy);
    if (i0 < 0) i0 = 0;
    if (j0 < 0) j0 = 0;
    if (i0 > nx - 1) i0 = nx - 1;
    if (j0 > ny - 1) j0 = ny - 1;
    int i1 = (i0 + 1 < nx) ? i0 + 1 : nx - 1;
    int j1 = (j0 + 1 < ny) ? j0 + 1 : ny - 1;
    double fx = gx - (double)i0;
    double fy = gy - (double)j0;

    double v00 = data[(size_t)j0 * (size_t)nx + (size_t)i0];
    double v10 = data[(size_t)j0 * (size_t)nx + (size_t)i1];
    double v01 = data[(size_t)j1 * (size_t)nx + (size_t)i0];
    double v11 = data[(size_t)j1 * (size_t)nx + (size_t)i1];

    double value = bilinear_sample(v00, v10, v01, v11, fx, fy);

    unsigned int rgba = colormap_lookup(
        value,
        levels, n_levels,
        colors, n_intervals,
        has_under, under_color,
        has_over,  over_color,
        has_mask_below, mask_below
    );

    out[(size_t)py * (size_t)img_w + (size_t)px] = rgba;
}
