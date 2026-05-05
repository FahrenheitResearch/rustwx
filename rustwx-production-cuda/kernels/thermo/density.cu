// Air density (kg/m^3) using virtual temperature.
//   p_pa = p_hpa * 100
//   t_k  = t_c + 273.15
//   tv_k = t_k * (1 + 0.61 * w)
//   rho  = p_pa / (Rd * tv_k)
// Inputs: pressure (hPa), temperature (C), mixing ratio (kg/kg).

extern "C" __global__
void density_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    const double* __restrict__ mixing_ratio,
    double* __restrict__ rho,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double p_pa = pressure[idx] * 100.0;
    double t_k = temperature[idx] + 273.15;
    double w = mixing_ratio[idx];
    double tv_k = t_k * (1.0 + 0.61 * w);
    rho[idx] = p_pa / (287.04749097718457 * tv_k);
}
