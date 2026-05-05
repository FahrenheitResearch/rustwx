// Geostrophic wind from geopotential height:
//   u_g = -(g / f) · ∂Z/∂y
//   v_g =  (g / f) · ∂Z/∂x
//
// Caller passes the Coriolis parameter `f` (per grid point). When |f| is
// below 1e-20 we set the geostrophic wind to zero (geostrophic balance breaks
// down near the equator).
//
// Boundary-aware ddx/ddy and physical constants are prepended at compile time
// by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void geostrophic_wind_kernel(
    const double* __restrict__ height,
    const double* __restrict__ f,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    double grav,
    double* __restrict__ ug,
    double* __restrict__ vg,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    int idx = j * nx + i;
    double fc = f[idx];
    if (fabs(fc) < 1e-20) { ug[idx] = 0.0; vg[idx] = 0.0; return; }
    double dZdx = ddx(height, dx, j, i, ny, nx);
    double dZdy = ddy(height, dy, j, i, ny, nx);
    ug[idx] = -(grav / fc) * dZdy;
    vg[idx] =  (grav / fc) * dZdx;
}
