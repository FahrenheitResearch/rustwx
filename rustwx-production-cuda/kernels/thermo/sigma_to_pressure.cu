// Sigma coordinate to pressure: p = sigma * (psfc - ptop) + ptop.

extern "C" __global__
void sigma_to_pressure_kernel(
    const double* __restrict__ sigma,
    const double* __restrict__ psfc,
    const double* __restrict__ ptop,
    double* __restrict__ pressure,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    pressure[idx] = sigma[idx] * (psfc[idx] - ptop[idx]) + ptop[idx];
}
