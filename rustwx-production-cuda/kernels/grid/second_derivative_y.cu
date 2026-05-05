// Second derivative along y:  d²f/dy² (boundary-aware).
//
// Boundary-aware d2dy2 is prepended at compile time by the Rust loader via
// `with_grid_helpers`. Do not add `#include` directives.

extern "C" __global__
void second_derivative_y_kernel(
    const double* __restrict__ f,
    const double* __restrict__ dy,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    out[j * nx + i] = d2dy2(f, dy, j, i, ny, nx);
}
