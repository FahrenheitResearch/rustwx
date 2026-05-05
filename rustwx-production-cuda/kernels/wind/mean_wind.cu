// Height-weighted mean wind in a layer. One thread = one column.
// Inputs are (ncols, nlevels) row-major: element (col, k) at col*nlevels + k.
//
// NOTE: This kernel uses centered "box" weights dh = (h[k+1] - h[k-1])/2
// rather than the trapezoidal-with-interpolated-endpoints integration in
// metrust::calc::wind::mean_wind. The wrapper marks this DEFER for parity
// testing — see DIVERGENT_KERNELS.md.

extern "C" __global__
void mean_wind_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ heights,
    double bottom,
    double top,
    double* __restrict__ mean_u_out,
    double* __restrict__ mean_v_out,
    int ncols,
    int nlevels
) {
    int col = blockDim.x * blockIdx.x + threadIdx.x;
    if (col >= ncols) return;

    int off = col * nlevels;
    double sum_u = 0.0, sum_v = 0.0, sum_dh = 0.0;

    for (int k = 0; k < nlevels; k++) {
        double h = heights[off + k];
        if (h < bottom) continue;
        if (h > top) break;

        double dh;
        if (k == 0 || heights[off + k - 1] < bottom) {
            // First level in layer
            if (k + 1 < nlevels && heights[off + k + 1] <= top) {
                dh = (heights[off + k + 1] - h) / 2.0;
            } else {
                dh = 1.0;
            }
        } else if (k + 1 >= nlevels || heights[off + k + 1] > top) {
            // Last level in layer
            dh = (h - heights[off + k - 1]) / 2.0;
        } else {
            dh = (heights[off + k + 1] - heights[off + k - 1]) / 2.0;
        }
        if (dh < 0.0) dh = 0.0;

        sum_u += u[off + k] * dh;
        sum_v += v[off + k] * dh;
        sum_dh += dh;
    }

    if (sum_dh > 0.0) {
        mean_u_out[col] = sum_u / sum_dh;
        mean_v_out[col] = sum_v / sum_dh;
    } else {
        mean_u_out[col] = 0.0;
        mean_v_out[col] = 0.0;
    }
}
