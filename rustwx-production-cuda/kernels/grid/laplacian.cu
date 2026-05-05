// Laplacian:  d2f/dx2 + d2f/dy2
//
// Boundary-aware second-derivative helpers (d2dx2, d2dy2) are prepended
// at compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void laplacian_kernel(
    const double* __restrict__ f,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    out[j * nx + i] = d2dx2(f, dx, j, i, ny, nx) + d2dy2(f, dy, j, i, ny, nx);
}
