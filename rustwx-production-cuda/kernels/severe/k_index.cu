// K-Index: KI = (T850 - T500) + Td850 - (T700 - Td700)
//
// Mirrors wx_math::composite::k_index and met-cu's k_index_kernel.

extern "C" __global__
void k_index_kernel(
    const double* __restrict__ t850,
    const double* __restrict__ t700,
    const double* __restrict__ t500,
    const double* __restrict__ td850,
    const double* __restrict__ td700,
    double* __restrict__ ki,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    ki[idx] = (t850[idx] - t500[idx]) + td850[idx] - (t700[idx] - td700[idx]);
}
