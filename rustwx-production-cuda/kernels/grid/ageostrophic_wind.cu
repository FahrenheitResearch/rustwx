// Ageostrophic wind:  (ua, va) = (u - u_g, v - v_g)
// where u_g, v_g are derived from the geopotential height field Z and
// the Coriolis parameter f (computed host-side from latitude).
//
// Boundary-aware ddx/ddy and physical constants are prepended at compile time
// by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void ageostrophic_wind_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ height,
    const double* __restrict__ f,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    double grav,
    double* __restrict__ ua,
    double* __restrict__ va,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    int idx = j * nx + i;
    double fc = f[idx];
    if (fabs(fc) < 1e-20) { ua[idx] = u[idx]; va[idx] = v[idx]; return; }
    double dZdx = ddx(height, dx, j, i, ny, nx);
    double dZdy = ddy(height, dy, j, i, ny, nx);
    double ug = -(grav / fc) * dZdy;
    double vg =  (grav / fc) * dZdx;
    ua[idx] = u[idx] - ug;
    va[idx] = v[idx] - vg;
}
