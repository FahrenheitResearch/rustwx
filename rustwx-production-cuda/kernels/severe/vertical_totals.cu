// Vertical Totals: VT = T850 - T500
//
// Mirrors wx_math::composite::vertical_totals and met-cu's vertical_totals_kernel.

extern "C" __global__
void vertical_totals_kernel(
    const double* __restrict__ t850,
    const double* __restrict__ t500,
    double* __restrict__ vt,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    vt[idx] = t850[idx] - t500[idx];
}
