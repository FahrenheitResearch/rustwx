// Curvature vorticity component:
//   zeta_c = (u^2 dvdx - v^2 dudy + uv (dvdy - dudx)) / (u^2 + v^2)
//
// Boundary-aware ddx/ddy and physical constants are prepended at compile time
// by the Rust loader via `with_grid_helpers`. Do not add `#include` directives.

extern "C" __global__
void curvature_vorticity_kernel(
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
    double uc = u[idx], vc = v[idx];
    double spd2 = uc * uc + vc * vc;
    if (spd2 < 1e-20) { out[idx] = 0.0; return; }
    double dudx_v = ddx(u, dx, j, i, ny, nx);
    double dudy_v = ddy(u, dy, j, i, ny, nx);
    double dvdx_v = ddx(v, dx, j, i, ny, nx);
    double dvdy_v = ddy(v, dy, j, i, ny, nx);
    out[idx] = (uc * uc * dvdx_v - vc * vc * dudy_v
                + uc * vc * (dvdy_v - dudx_v)) / spd2;
}
