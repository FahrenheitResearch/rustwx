// Geopotential height (m) -> geopotential (m^2/s^2): phi = g0 * z.

extern "C" __global__
void height_to_geopotential_kernel(
    const double* __restrict__ height,
    double* __restrict__ geopotential,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    geopotential[idx] = G0 * height[idx];
}
