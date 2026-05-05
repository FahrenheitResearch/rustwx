// Horizontal advection:  -u*df/dx - v*df/dy
//
// Boundary-aware finite-difference helpers (ddx, ddy) are prepended at
// compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void advection_kernel(
    const double* __restrict__ field,
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    int idx = j * nx + i;
    double dfdx = ddx(field, dx, j, i, ny, nx);
    double dfdy = ddy(field, dy, j, i, ny, nx);
    out[idx] = -(u[idx] * dfdx + v[idx] * dfdy);
}
