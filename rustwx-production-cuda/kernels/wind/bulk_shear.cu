// Bulk wind shear (delta-u, delta-v) over a height layer.
// One thread = one column. Profile inputs are (ncols, nlevels) row-major:
// element (col, k) lives at index col*nlevels + k.
// Mirrors metrust::calc::wind::bulk_shear (linear interp at boundaries).

extern "C" __global__
void bulk_shear_kernel(
    const double* __restrict__ u,        // (ncols, nlevels) m/s
    const double* __restrict__ v,        // (ncols, nlevels) m/s
    const double* __restrict__ heights,  // (ncols, nlevels) meters AGL
    double bottom,                        // bottom of layer (m)
    double top,                           // top of layer (m)
    double* __restrict__ shear_u_out,    // (ncols,)
    double* __restrict__ shear_v_out,    // (ncols,)
    int ncols,
    int nlevels
) {
    int col = blockDim.x * blockIdx.x + threadIdx.x;
    if (col >= ncols) return;

    int off = col * nlevels;

    // Interpolate u, v at bottom
    double u_bot = u[off], v_bot = v[off];
    for (int k = 1; k < nlevels; k++) {
        if (heights[off + k] >= bottom) {
            double h0 = heights[off + k - 1];
            double h1 = heights[off + k];
            if (h1 - h0 > 1e-6) {
                double frac = (bottom - h0) / (h1 - h0);
                u_bot = u[off + k - 1] + frac * (u[off + k] - u[off + k - 1]);
                v_bot = v[off + k - 1] + frac * (v[off + k] - v[off + k - 1]);
            } else {
                u_bot = u[off + k];
                v_bot = v[off + k];
            }
            break;
        }
    }

    // Interpolate u, v at top
    double u_top = u[off + nlevels - 1], v_top = v[off + nlevels - 1];
    for (int k = 1; k < nlevels; k++) {
        if (heights[off + k] >= top) {
            double h0 = heights[off + k - 1];
            double h1 = heights[off + k];
            if (h1 - h0 > 1e-6) {
                double frac = (top - h0) / (h1 - h0);
                u_top = u[off + k - 1] + frac * (u[off + k] - u[off + k - 1]);
                v_top = v[off + k - 1] + frac * (v[off + k] - v[off + k - 1]);
            } else {
                u_top = u[off + k];
                v_top = v[off + k];
            }
            break;
        }
    }

    shear_u_out[col] = u_top - u_bot;
    shear_v_out[col] = v_top - v_bot;
}
