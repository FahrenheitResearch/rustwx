// Total Totals: TT = (T850 - T500) + (Td850 - T500)
//
// Mirrors wx_math::composite::total_totals and met-cu's total_totals_kernel.

extern "C" __global__
void total_totals_kernel(
    const double* __restrict__ t850,
    const double* __restrict__ t500,
    const double* __restrict__ td850,
    double* __restrict__ tt,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    tt[idx] = (t850[idx] - t500[idx]) + (td850[idx] - t500[idx]);
}
