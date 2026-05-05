// Total deformation:  sqrt(shearing^2 + stretching^2)
//
// Boundary-aware finite-difference helpers (ddx, ddy) are prepended at
// compile time by the Rust loader via `with_grid_helpers`.

extern "C" __global__
void total_deformation_kernel(
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
    double dudx = ddx(u, dx, j, i, ny, nx);
    double dvdy = ddy(v, dy, j, i, ny, nx);
    double dvdx = ddx(v, dx, j, i, ny, nx);
    double dudy = ddy(u, dy, j, i, ny, nx);
    double shear   = dvdx + dudy;
    double stretch = dudx - dvdy;
    out[idx] = sqrt(shear * shear + stretch * stretch);
}
