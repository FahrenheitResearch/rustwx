// Relative vorticity:  zeta = dv/dx - du/dy
//
// Boundary-aware finite-difference helpers (ddx, ddy) and physical constants
// are prepended at compile time by the Rust loader via `with_grid_helpers`.
// Do not add `#include` directives — NVRTC cannot resolve filesystem paths.

extern "C" __global__
void vorticity_kernel(
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
    double dvdx = ddx(v, dx, j, i, ny, nx);
    double dudy = ddy(u, dy, j, i, ny, nx);
    out[idx] = dvdx - dudy;
}
