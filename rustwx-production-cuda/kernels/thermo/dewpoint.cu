// Dewpoint (Celsius) from vapor pressure (hPa) -- inverse Bolton.
//   ln_ratio = ln(e/6.112)
//   td = 243.5 * ln_ratio / (17.67 - ln_ratio)

extern "C" __global__
void dewpoint_kernel(
    const double* __restrict__ vapor_pressure,
    double* __restrict__ td,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double ln_ratio = log(vapor_pressure[idx] / 6.112);
    td[idx] = 243.5 * ln_ratio / (17.67 - ln_ratio);
}
