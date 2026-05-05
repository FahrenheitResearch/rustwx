// Mixing ratio (kg/kg) from vapor pressure (hPa) and total pressure (hPa).
//   w = eps * e / (p - e)

extern "C" __global__
void mixing_ratio_kernel(
    const double* __restrict__ vapor_pressure,
    const double* __restrict__ pressure,
    double* __restrict__ w,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double e = vapor_pressure[idx];
    w[idx] = EPS * e / (pressure[idx] - e);
}
