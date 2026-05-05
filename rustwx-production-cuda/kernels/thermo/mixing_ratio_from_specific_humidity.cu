// Mixing ratio (kg/kg) from specific humidity (kg/kg).
//   w = q / (1 - q)

extern "C" __global__
void mixing_ratio_from_specific_humidity_kernel(
    const double* __restrict__ specific_humidity,
    double* __restrict__ w,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double q = specific_humidity[idx];
    w[idx] = q / (1.0 - q);
}
