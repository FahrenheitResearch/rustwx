// df/dx via boundary-aware centered finite differences.
//
// Boundary-aware finite-difference helpers (ddx) are prepended at
// compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void first_derivative_x_kernel(
    const double* __restrict__ f,
    const double* __restrict__ dx,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    out[j * nx + i] = ddx(f, dx, j, i, ny, nx);
}
