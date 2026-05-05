// Boundary-aware 2D finite difference helpers ported from met-cu
// (python/metcu/kernels/grid.py, _deriv_device_funcs block).
// Domain edges use one-sided second-order stencils.
//
// Field convention: row-major (C-contiguous) `f[j*nx + i]`.

#pragma once

__device__ inline double ddx(const double* f, const double* dx,
                             int j, int i, int ny, int nx) {
    int idx = j * nx + i;
    double h = dx[idx];
    if (i == 0)
        return (-3.0 * f[idx] + 4.0 * f[idx + 1] - f[idx + 2]) / (2.0 * h);
    if (i == nx - 1)
        return (3.0 * f[idx] - 4.0 * f[idx - 1] + f[idx - 2]) / (2.0 * h);
    return (f[idx + 1] - f[idx - 1]) / (2.0 * h);
}

__device__ inline double ddy(const double* f, const double* dy,
                             int j, int i, int ny, int nx) {
    int idx = j * nx + i;
    double h = dy[idx];
    if (j == 0)
        return (-3.0 * f[idx] + 4.0 * f[idx + nx] - f[idx + 2 * nx]) / (2.0 * h);
    if (j == ny - 1)
        return (3.0 * f[idx] - 4.0 * f[idx - nx] + f[idx - 2 * nx]) / (2.0 * h);
    return (f[idx + nx] - f[idx - nx]) / (2.0 * h);
}

__device__ inline double d2dx2(const double* f, const double* dx,
                               int j, int i, int ny, int nx) {
    int idx = j * nx + i;
    double h = dx[idx];
    if (i == 0)
        return (f[idx] - 2.0 * f[idx + 1] + f[idx + 2]) / (h * h);
    if (i == nx - 1)
        return (f[idx] - 2.0 * f[idx - 1] + f[idx - 2]) / (h * h);
    return (f[idx + 1] - 2.0 * f[idx] + f[idx - 1]) / (h * h);
}

__device__ inline double d2dy2(const double* f, const double* dy,
                               int j, int i, int ny, int nx) {
    int idx = j * nx + i;
    double h = dy[idx];
    if (j == 0)
        return (f[idx] - 2.0 * f[idx + nx] + f[idx + 2 * nx]) / (h * h);
    if (j == ny - 1)
        return (f[idx] - 2.0 * f[idx - nx] + f[idx - 2 * nx]) / (h * h);
    return (f[idx + nx] - 2.0 * f[idx] + f[idx - nx]) / (h * h);
}
