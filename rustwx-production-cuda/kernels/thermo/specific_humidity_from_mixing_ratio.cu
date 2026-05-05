// Specific humidity (kg/kg) from mixing ratio (kg/kg).
//   q = w / (1 + w)

extern "C" __global__
void specific_humidity_from_mixing_ratio_kernel(
    const double* __restrict__ mixing_ratio,
    double* __restrict__ q,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double w = mixing_ratio[idx];
    q[idx] = w / (1.0 + w);
}
