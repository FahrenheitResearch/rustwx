// Dry adiabatic temperature at a new pressure level.
//   T_K_new = T_surface_K * (pressure / reference_pressure)^kappa
//   T_C_out = T_K_new - 273.15
// Inputs:  pressure (hPa), reference_pressure (hPa), t_surface (Celsius).
// Output:  temperature at `pressure` (Celsius).

extern "C" __global__
void dry_lapse_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ reference_pressure,
    const double* __restrict__ t_surface,
    double* __restrict__ t_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t_k = t_surface[idx] + 273.15;
    t_out[idx] = t_k * pow(pressure[idx] / reference_pressure[idx], ROCP) - 273.15;
}
