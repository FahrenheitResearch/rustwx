// Boyden Index: BI = (Z700 - Z1000)/10 - T700 - 200
//
// Mirrors wx_math::composite::boyden_index and met-cu's boyden_index_kernel.
//
// Inputs:  z1000 (m), z700 (m), t700 (deg C).

extern "C" __global__
void boyden_index_kernel(
    const double* __restrict__ z1000,
    const double* __restrict__ z700,
    const double* __restrict__ t700,
    double* __restrict__ bi,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    bi[idx] = (z700[idx] - z1000[idx]) / 10.0 - t700[idx] - 200.0;
}
