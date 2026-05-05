// Q-vector components (Q1, Q2) on a constant pressure surface:
//   Q1 = -(Rd / p) * (du_g/dx · dT/dx + dv_g/dx · dT/dy)
//   Q2 = -(Rd / p) * (du_g/dy · dT/dx + dv_g/dy · dT/dy)
//
// Caller passes geostrophic wind for u/v. `pressure` is in Pa (scalar).
// Boundary-aware ddx/ddy and physical constants are prepended at compile time
// by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void q_vector_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    const double* __restrict__ temperature,
    const double* __restrict__ dx,
    const double* __restrict__ dy,
    double pressure,
    double rd,
    double* __restrict__ q1_out,
    double* __restrict__ q2_out,
    int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;
    int idx = j * nx + i;

    double dTdx = ddx(temperature, dx, j, i, ny, nx);
    double dTdy = ddy(temperature, dy, j, i, ny, nx);

    double dudx_v = ddx(u, dx, j, i, ny, nx);
    double dudy_v = ddy(u, dy, j, i, ny, nx);
    double dvdx_v = ddx(v, dx, j, i, ny, nx);
    double dvdy_v = ddy(v, dy, j, i, ny, nx);

    double coeff = -rd / pressure;
    q1_out[idx] = coeff * (dudx_v * dTdx + dvdx_v * dTdy);
    q2_out[idx] = coeff * (dudy_v * dTdx + dvdy_v * dTdy);
}
