// Vapor pressure (hPa) from mixing ratio (kg/kg) and total pressure (hPa).
//   e = w * p / (eps + w)

extern "C" __global__
void vapor_pressure_from_mixing_ratio_kernel(
    const double* __restrict__ mixing_ratio,
    const double* __restrict__ pressure,
    double* __restrict__ e,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double w = mixing_ratio[idx];
    e[idx] = w * pressure[idx] / (EPS + w);
}
