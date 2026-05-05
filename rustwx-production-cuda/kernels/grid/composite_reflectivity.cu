// Composite reflectivity: maximum value along the vertical axis per column.
//
// `field` is (nz, ny, nx) row-major. Output `out` is (ny, nx). Indexed as a
// single 1-D pass of `ncols = ny * nx` threads.

extern "C" __global__
void composite_reflectivity_kernel(
    const double* __restrict__ field,
    double* __restrict__ out,
    int ncols,
    int nz
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= ncols) return;

    double maxval = -1e308;
    for (int k = 0; k < nz; k++) {
        double v = field[k * ncols + idx];
        if (v > maxval) maxval = v;
    }
    out[idx] = maxval;
}
