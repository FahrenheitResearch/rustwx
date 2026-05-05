// Energy-Helicity Index.
//
// EHI = (CAPE * SRH) / 160000
//
// Mirrors wx_math::composite::compute_ehi and met-cu's compute_ehi_kernel.

extern "C" __global__
void ehi_kernel(
    const double* __restrict__ cape,
    const double* __restrict__ srh,
    double* __restrict__ ehi,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    ehi[idx] = (cape[idx] * srh[idx]) / 160000.0;
}
