// Corfidi MCS upwind / downwind propagation vectors. One thread = one column.
// Inputs are (ncols, nlevels) row-major: element (col, k) at col*nlevels + k.
//
// NOTE: 0-6 km mean wind here uses centered height-weights, while
// metrust::calc::wind::corfidi_storm_motion delegates to
// metrust::calc::wind::mean_wind which performs trapezoidal integration with
// interpolated endpoints. The two answers are close but not bit-equal; the
// wrapper test is deferred — see DIVERGENT_KERNELS.md.

extern "C" __global__
void corfidi_storm_motion_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ heights,
    double u_llj,           // low-level jet u (e.g. 850 hPa wind, m/s)
    double v_llj,           // low-level jet v
    double* __restrict__ upwind_u_out,
    double* __restrict__ upwind_v_out,
    double* __restrict__ downwind_u_out,
    double* __restrict__ downwind_v_out,
    int ncols,
    int nlevels
) {
    int col = blockDim.x * blockIdx.x + threadIdx.x;
    if (col >= ncols) return;

    int off = col * nlevels;

    // Mean wind 0-6 km (cloud-layer mean wind)
    double su = 0.0, sv = 0.0, sdh = 0.0;
    for (int k = 0; k < nlevels; k++) {
        double h = heights[off + k];
        if (h > 6000.0) break;
        double dh;
        if (k == 0) dh = (k + 1 < nlevels) ? (heights[off + k + 1] - h) / 2.0 : 1.0;
        else if (k + 1 >= nlevels || heights[off + k + 1] > 6000.0)
            dh = (h - heights[off + k - 1]) / 2.0;
        else
            dh = (heights[off + k + 1] - heights[off + k - 1]) / 2.0;
        if (dh < 0.0) dh = 0.0;
        su += u[off + k] * dh;
        sv += v[off + k] * dh;
        sdh += dh;
    }
    double mw_u = (sdh > 0.0) ? su / sdh : 0.0;
    double mw_v = (sdh > 0.0) ? sv / sdh : 0.0;

    // Propagation vector = mean_wind - LLJ (opposite of LLJ relative to mean)
    double prop_u = mw_u - u_llj;
    double prop_v = mw_v - v_llj;

    // Corfidi upwind = mean_wind - LLJ (propagation only)
    upwind_u_out[col] = prop_u;
    upwind_v_out[col] = prop_v;

    // Corfidi downwind = mean_wind + propagation = 2*mean_wind - LLJ
    downwind_u_out[col] = prop_u + mw_u;
    downwind_v_out[col] = prop_v + mw_v;
}
