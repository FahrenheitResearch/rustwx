// Extract data within a pressure layer. One thread = one column.
// Inputs are (ncols, nlevels) row-major: element (col, k) at col*nlevels + k.
// Outputs p_out / v_out are also (ncols, nlevels) and padded with NaN beyond
// `count_out[col]` valid entries. Pressure axis is descending hPa.
//
// NOTE: This kernel does pure index-selection (p_top <= p[k] <= p_bottom).
// wx_math::thermo::get_layer additionally interpolates at the layer
// boundaries in log-pressure space. The two outputs differ at the endpoints
// — see DIVERGENT_KERNELS.md.

extern "C" __global__
void get_layer_kernel(
    const double* __restrict__ pressure,    // (ncols, nlevels) hPa, descending
    const double* __restrict__ values,      // (ncols, nlevels)
    double p_bottom,                          // hPa (higher pressure = lower altitude)
    double p_top,                             // hPa (lower pressure = higher altitude)
    double* __restrict__ p_out,             // (ncols, nlevels) — padded with NaN
    double* __restrict__ v_out,             // (ncols, nlevels) — padded with NaN
    int* __restrict__ count_out,            // (ncols,) — number of valid levels per col
    int ncols,
    int nlevels
) {
    int col = blockDim.x * blockIdx.x + threadIdx.x;
    if (col >= ncols) return;

    int off = col * nlevels;
    int cnt = 0;

    for (int k = 0; k < nlevels; k++) {
        double p = pressure[off + k];
        // Pressure is descending: p_bottom >= p >= p_top
        if (p <= p_bottom && p >= p_top) {
            p_out[off + cnt] = p;
            v_out[off + cnt] = values[off + k];
            cnt++;
        }
    }

    // Fill remaining with NaN
    for (int k = cnt; k < nlevels; k++) {
        p_out[off + k] = __longlong_as_double(0x7FF8000000000000LL);
        v_out[off + k] = __longlong_as_double(0x7FF8000000000000LL);
    }
    count_out[col] = cnt;
}
