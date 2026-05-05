// Per-column 1-D linear interpolation to new vertical levels.
//
// `field`     : (nz_in, ny, nx) row-major (level-major)
// `levels_in` : (nz_in, ny, nx) coordinate values at each input level
// `levels_out`: (nz_out,)       target levels (1-D)
// `out`       : (nz_out, ny, nx)
//
// `ascending != 0` indicates input coords increase with index. We accept
// either order via the bracket test below.
//
// No grid helpers needed; constants (none used) are prepended via
// `with_constants` for consistency.

extern "C" __global__
void interpolate_1d_kernel(
    const double* __restrict__ field,
    const double* __restrict__ levels_in,
    const double* __restrict__ levels_out,
    double* __restrict__ out,
    int nz_in, int nz_out, int ny, int nx,
    int ascending
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= nx || j >= ny) return;

    int nxy = ny * nx;
    int idx2d = j * nx + i;

    for (int ko = 0; ko < nz_out; ko++) {
        double target = levels_out[ko];
        int found = 0;
        for (int k = 0; k < nz_in - 1; k++) {
            double lo = levels_in[k * nxy + idx2d];
            double hi = levels_in[(k + 1) * nxy + idx2d];
            int bracket;
            if (ascending)
                bracket = (lo <= target && target <= hi) || (hi <= target && target <= lo);
            else
                bracket = (lo >= target && target >= hi) || (hi >= target && target >= lo);
            if (bracket) {
                double denom = hi - lo;
                double frac = (fabs(denom) > 1e-30) ? (target - lo) / denom : 0.0;
                double f0 = field[k * nxy + idx2d];
                double f1 = field[(k + 1) * nxy + idx2d];
                out[ko * nxy + idx2d] = f0 + frac * (f1 - f0);
                found = 1;
                break;
            }
        }
        if (!found) {
            out[ko * nxy + idx2d] = nan("");
        }
    }
}
