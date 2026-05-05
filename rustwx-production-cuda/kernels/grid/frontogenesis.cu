// Petterssen 2D frontogenesis (scalar form):
//
//   F = -1/|grad(theta)| * [ (dtheta/dx)^2 * du/dx
//                          + (dtheta/dy)^2 * dv/dy
//                          + (dtheta/dx)*(dtheta/dy) * (dv/dx + du/dy) ]
//
// Sign convention: positive = frontogenesis (gradient sharpening).
//
// Boundary-aware finite-difference helpers (ddx, ddy) are prepended at
// compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void frontogenesis_kernel(
    const double* __restrict__ theta,
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

    double dtdx = ddx(theta, dx, j, i, ny, nx);
    double dtdy = ddy(theta, dy, j, i, ny, nx);
    double mag = sqrt(dtdx * dtdx + dtdy * dtdy);
    if (mag < 1e-20) { out[idx] = 0.0; return; }

    double dudx_v = ddx(u, dx, j, i, ny, nx);
    double dudy_v = ddy(u, dy, j, i, ny, nx);
    double dvdx_v = ddx(v, dx, j, i, ny, nx);
    double dvdy_v = ddy(v, dy, j, i, ny, nx);

    double F = (dtdx * dtdx * dudx_v
              + dtdy * dtdy * dvdy_v
              + dtdx * dtdy * (dvdx_v + dudy_v));
    out[idx] = -F / mag;
}
