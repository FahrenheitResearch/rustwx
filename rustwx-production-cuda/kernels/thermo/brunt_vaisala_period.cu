// Brunt-Vaisala period (s) from frequency (s^-1).
// Returns 1e30 (sentinel "infinite") when bvf <= 0; for bvf > 0 returns 2*pi/bvf.

extern "C" __global__
void brunt_vaisala_period_kernel(
    const double* __restrict__ bvf,
    double* __restrict__ period,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double n_val = bvf[idx];
    period[idx] = (n_val > 0.0)
        ? (2.0 * 3.14159265358979323846 / n_val)
        : 1.0e30;
}
