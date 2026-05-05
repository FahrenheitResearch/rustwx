// Per-column 1-D log-pressure interpolation.
//
// `field`     : (nz_in, ny, nx) row-major (level-major)
// `pressure`  : (nz_in, ny, nx) input pressure at each level (Pa)
// `p_target`  : (nz_out,)       target pressures (Pa)
// `out`       : (nz_out, ny, nx)
//
// Pressure may be ascending or descending in z; the bracket test handles both.

extern "C" __global__
void log_interpolate_1d_kernel(
    const double* __restrict__ field,
    const double* __restrict__ pressure,
    const double* __restrict__ p_target,
    double* __restrict__ out,
    int nz_in, int nz_out, int ny, int nx
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;

    int nxy = ny * nx;
    int idx2d = j * nx + i;

    for (int ko = 0; ko < nz_out; ko++) {
        double lnpt = log(p_target[ko]);
        int found = 0;
        for (int k = 0; k < nz_in - 1; k++) {
            double lnp0 = log(pressure[k * nxy + idx2d]);
            double lnp1 = log(pressure[(k + 1) * nxy + idx2d]);
            if ((lnp0 >= lnpt && lnpt >= lnp1) || (lnp1 >= lnpt && lnpt >= lnp0)) {
                double denom = lnp1 - lnp0;
                double frac = (fabs(denom) > 1e-30) ? (lnpt - lnp0) / denom : 0.0;
                double f0 = field[k * nxy + idx2d];
                double f1 = field[(k + 1) * nxy + idx2d];
                out[ko * nxy + idx2d] = f0 + frac * (f1 - f0);
                found = 1;
                break;
            }
        }
        if (!found) out[ko * nxy + idx2d] = nan("");
    }
}
