// theta_v = theta * (1 + 0.61 * w)   with theta = T_K * (1000/p)^kappa
//
// Inputs:  pressure (hPa), temperature (Celsius), mixing ratio (kg/kg).
// Output:  theta_v (K).

extern "C" __global__
void virtual_potential_temperature_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    const double* __restrict__ mixing_ratio,
    double* __restrict__ theta_v,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t_k = temperature[idx] + ZEROCNK;
    double theta = t_k * pow(1000.0 / pressure[idx], ROCP);
    double w = mixing_ratio[idx];
    theta_v[idx] = theta * (1.0 + 0.61 * w);
}
