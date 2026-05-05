// Component of the wind vector along a unit normal (nx, ny).
// comp = u * nx + v * ny.

extern "C" __global__
void normal_component_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ nx,
    const double* __restrict__ ny,
    double* __restrict__ comp,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    comp[idx] = u[idx] * nx[idx] + v[idx] * ny[idx];
}
