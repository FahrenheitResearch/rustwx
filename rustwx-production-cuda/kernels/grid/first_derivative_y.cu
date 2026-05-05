// df/dy via boundary-aware centered finite differences.
//
// Boundary-aware finite-difference helpers (ddy) are prepended at
// compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void first_derivative_y_kernel(
    const double* __restrict__ f,
    const double* __restrict__ dy,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    out[j * nx + i] = ddy(f, dy, j, i, ny, nx);
}
