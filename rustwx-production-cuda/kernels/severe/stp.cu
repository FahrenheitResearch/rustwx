// Significant Tornado Parameter (fixed-layer STP).
//
// Mirrors metrust::calc::severe::significant_tornado_parameter
// and met-cu's significant_tornado_parameter_kernel.
//
// Inputs:  sbcape (J/kg), lcl_height_m (m), srh_0_1km (m^2/s^2),
//          bulk_shear_0_6km (m/s).
// Output:  STP (dimensionless).

extern "C" __global__
void stp_kernel(
    const double* __restrict__ cape,
    const double* __restrict__ lcl,
    const double* __restrict__ srh,
    const double* __restrict__ shear,
    double* __restrict__ stp,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double c = cape[idx];
    double l = lcl[idx];
    double s = srh[idx];
    double sh = shear[idx];

    // CAPE term: SBCAPE / 1500, floored at 0
    double cape_term = (c / 1500.0);
    if (cape_term < 0.0) cape_term = 0.0;

    // LCL term
    double lcl_term;
    if (l <= 1000.0) {
        lcl_term = 1.0;
    } else {
        lcl_term = (2000.0 - l) / 1000.0;
        if (lcl_term < 0.0) lcl_term = 0.0;
        if (lcl_term > 1.0) lcl_term = 1.0;
    }

    // SRH term
    double srh_term = s / 150.0;
    if (srh_term < 0.0) srh_term = 0.0;

    // Shear term
    double shear_term;
    if (sh < 12.5) {
        shear_term = 0.0;
    } else {
        double sh_capped = sh > 30.0 ? 30.0 : sh;
        shear_term = sh_capped / 20.0;
        if (shear_term < 0.0) shear_term = 0.0;
    }

    stp[idx] = cape_term * lcl_term * srh_term * shear_term;
}
