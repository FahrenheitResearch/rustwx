// Absolute vorticity:  zeta_abs = (dv/dx - du/dy) + f
// where f = 2*OMEGA*sin(latitude_deg * pi/180).
//
// Boundary-aware finite-difference helpers (ddx, ddy) and physical constants
// (OMEGA) are prepended at compile time by the Rust loader.

extern "C" __global__
void absolute_vorticity_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    const double* __restrict__ latitude,
    double* __restrict__ out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    int idx = j * nx + i;
    double dvdx = ddx(v, dx, j, i, ny, nx);
    double dudy = ddy(u, dy, j, i, ny, nx);
    const double PI = 3.141592653589793238462643383279502884;
    double f = 2.0 * 7.2921159e-5 * sin(latitude[idx] * PI / 180.0);
    out[idx] = (dvdx - dudy) + f;
}
