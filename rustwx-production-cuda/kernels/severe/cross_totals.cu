// Cross Totals: CT = Td850 - T500
//
// Mirrors wx_math::composite::cross_totals and met-cu's cross_totals_kernel.

extern "C" __global__
void cross_totals_kernel(
    const double* __restrict__ td850,
    const double* __restrict__ t500,
    double* __restrict__ ct,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    ct[idx] = td850[idx] - t500[idx];
}
