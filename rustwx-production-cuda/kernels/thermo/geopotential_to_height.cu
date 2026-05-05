// Geopotential (m^2/s^2) -> geopotential height (m): z = phi / g0.

extern "C" __global__
void geopotential_to_height_kernel(
    const double* __restrict__ geopotential,
    double* __restrict__ height,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    height[idx] = geopotential[idx] / G0;
}
