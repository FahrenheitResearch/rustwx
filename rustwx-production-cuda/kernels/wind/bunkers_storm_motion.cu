// Bunkers right/left mover storm motion. One thread = one column.
// Inputs are (ncols, nlevels) row-major: element (col, k) at col*nlevels + k.
//
// NOTE: The kernel uses 0-6 km height-weighted mean wind and a simple
// (top - bottom) bulk shear vector. metrust::calc::wind::bunkers_storm_motion
// uses pressure-weighted mean wind plus mean(5.5-6 km) - mean(0-0.5 km) shear,
// so the two are not numerically identical. See DIVERGENT_KERNELS.md.

extern "C" __global__
void bunkers_storm_motion_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ heights,
    double* __restrict__ rm_u_out,       // right mover u
    double* __restrict__ rm_v_out,
    double* __restrict__ lm_u_out,       // left mover u
    double* __restrict__ lm_v_out,
    double* __restrict__ mw_u_out,       // mean wind u
    double* __restrict__ mw_v_out,
    int ncols,
    int nlevels
) {
    int col = blockDim.x * blockIdx.x + threadIdx.x;
    if (col >= ncols) return;

    int off = col * nlevels;
    double D = 7.5;  // deviation magnitude m/s

    // Mean wind 0-6 km
    double su6 = 0.0, sv6 = 0.0, sdh6 = 0.0;
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
        su6 += u[off + k] * dh;
        sv6 += v[off + k] * dh;
        sdh6 += dh;
    }
    double mu6 = (sdh6 > 0.0) ? su6 / sdh6 : 0.0;
    double mv6 = (sdh6 > 0.0) ? sv6 / sdh6 : 0.0;

    // Shear vector: 0-6 km bulk shear. Interpolate at 0 and 6000.
    double u_bot = u[off], v_bot = v[off];
    double u_top = u[off + nlevels - 1], v_top = v[off + nlevels - 1];
    for (int k = 1; k < nlevels; k++) {
        if (heights[off + k] >= 6000.0) {
            double h0 = heights[off + k - 1];
            double h1 = heights[off + k];
            if (h1 - h0 > 1e-6) {
                double frac = (6000.0 - h0) / (h1 - h0);
                u_top = u[off + k - 1] + frac * (u[off + k] - u[off + k - 1]);
                v_top = v[off + k - 1] + frac * (v[off + k] - v[off + k - 1]);
            } else {
                u_top = u[off + k];
                v_top = v[off + k];
            }
            break;
        }
    }
    double shear_u = u_top - u_bot;
    double shear_v = v_top - v_bot;

    // Normalize shear vector
    double shear_mag = sqrt(shear_u * shear_u + shear_v * shear_v);
    double shear_norm_u = 0.0, shear_norm_v = 0.0;
    if (shear_mag > 1e-6) {
        shear_norm_u = shear_u / shear_mag;
        shear_norm_v = shear_v / shear_mag;
    }

    // Perpendicular (cross-product in 2D): rotate 90 degrees CW for RM
    double perp_u = shear_norm_v;
    double perp_v = -shear_norm_u;

    // Right mover: mean + D * perp
    rm_u_out[col] = mu6 + D * perp_u;
    rm_v_out[col] = mv6 + D * perp_v;

    // Left mover: mean - D * perp
    lm_u_out[col] = mu6 - D * perp_u;
    lm_v_out[col] = mv6 - D * perp_v;

    // Mean wind
    mw_u_out[col] = mu6;
    mw_v_out[col] = mv6;
}
