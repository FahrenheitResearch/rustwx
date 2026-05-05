// Horizontal gradient:  (df/dx, df/dy)
//
// Boundary-aware finite-difference helpers (ddx, ddy) are prepended at
// compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void gradient_kernel(
    const double* __restrict__ f,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    double* __restrict__ dfdx,
    double* __restrict__ dfdy,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    int idx = j * nx + i;
    dfdx[idx] = ddx(f, dx, j, i, ny, nx);
    dfdy[idx] = ddy(f, dy, j, i, ny, nx);
}
