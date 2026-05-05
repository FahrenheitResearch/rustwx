// Lifting Condensation Level via dry adiabatic ascent.
// Inputs:  p (hPa), t (Celsius), td (Celsius).
// Outputs: p_lcl (hPa), t_lcl (Celsius).
// Uses the `drylift` helper from thermo_helpers.cuh.

extern "C" __global__
void lcl_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    const double* __restrict__ dewpoint,
    double* __restrict__ p_lcl_out,
    double* __restrict__ t_lcl_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double p_lcl, t_lcl;
    drylift(pressure[i], temperature[i], dewpoint[i], &p_lcl, &t_lcl);
    p_lcl_out[i] = p_lcl;
    t_lcl_out[i] = t_lcl;
}
