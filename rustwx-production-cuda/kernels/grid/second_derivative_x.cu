// Second derivative along x:  d²f/dx² (boundary-aware).
//
// Boundary-aware d2dx2 is prepended at compile time by the Rust loader via
// `with_grid_helpers`. Do not add `#include` directives.

extern "C" __global__
void second_derivative_x_kernel(
    const double* __restrict__ f,
    const double* __restrict__ dx,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    out[j * nx + i] = d2dx2(f, dx, j, i, ny, nx);
}
