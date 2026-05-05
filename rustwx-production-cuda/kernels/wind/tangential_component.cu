// Component of the wind vector along a unit tangent (tx, ty).
// comp = u * tx + v * ty.

extern "C" __global__
void tangential_component_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ tx,
    const double* __restrict__ ty,
    double* __restrict__ comp,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    comp[idx] = u[idx] * tx[idx] + v[idx] * ty[idx];
}
