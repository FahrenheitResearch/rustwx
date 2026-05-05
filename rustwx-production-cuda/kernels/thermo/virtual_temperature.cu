// Virtual temperature (Celsius) from temperature (C) and mixing ratio (kg/kg).
// MetPy formula:  Tv = T_K * (1 + w/eps) / (1 + w) - 273.15
//
// NOTE: this is the dry-air-mass-fraction form used by MetPy and met-cu;
// it differs from the SHARPpy `virtual_temp(t, p, td)` helper.

extern "C" __global__
void virtual_temperature_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ mixing_ratio,
    double* __restrict__ tv,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t_k = temperature[idx] + ZEROCNK;
    double w = mixing_ratio[idx];
    tv[idx] = t_k * (1.0 + w / EPS) / (1.0 + w) - ZEROCNK;
}
