// Friction velocity u* = sqrt(|cov(u, w)|) over a time series of length n.
// Single-block, single-thread reduction (port of met-cu's friction_velocity_kernel).
// Output is a single scalar in `ustar_out[0]`.

extern "C" __global__
void friction_velocity_kernel(
    const double* __restrict__ u,
    const double* __restrict__ w,
    double* __restrict__ ustar_out,
    int n
) {
    int tid = blockDim.x * blockIdx.x + threadIdx.x;
    if (tid != 0) return;

    double mean_u = 0.0, mean_w = 0.0;
    for (int i = 0; i < n; i++) {
        mean_u += u[i];
        mean_w += w[i];
    }
    mean_u /= (double)n;
    mean_w /= (double)n;

    double cov = 0.0;
    for (int i = 0; i < n; i++) {
        cov += (u[i] - mean_u) * (w[i] - mean_w);
    }
    cov /= (double)n;
    ustar_out[0] = sqrt(fabs(cov));
}
