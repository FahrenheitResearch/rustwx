// Supercell Composite Parameter (SCP).
//
// Mirrors metrust::calc::severe::supercell_composite_parameter
// and met-cu's supercell_composite_parameter_kernel.
//
// SCP = (mucape/1000) * (srh/50) * shear_term
//   shear_term = 0 if shear<10, else min(shear,20)/20
//
// Inputs:  mucape (J/kg), srh_eff (m^2/s^2), bulk_shear (m/s).
// Output:  SCP (dimensionless).

extern "C" __global__
void scp_kernel(
    const double* __restrict__ mucape,
    const double* __restrict__ srh,
    const double* __restrict__ shear,
    double* __restrict__ scp,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double c = mucape[idx];
    double s = srh[idx];
    double sh = shear[idx];

    double cape_term = c / 1000.0;
    if (cape_term < 0.0) cape_term = 0.0;

    double srh_term = s / 50.0;
    if (srh_term < 0.0) srh_term = 0.0;

    double shear_term;
    if (sh < 10.0) {
        shear_term = 0.0;
    } else {
        double sh_capped = sh > 20.0 ? 20.0 : sh;
        shear_term = sh_capped / 20.0;
        if (shear_term < 0.0) shear_term = 0.0;
    }

    scp[idx] = cape_term * srh_term * shear_term;
}
