// Bulk Richardson Number.
//
// BRN = CAPE / (0.5 * shear^2)
//
// Mirrors wx_math::composite::bulk_richardson_number. Returns NaN when
// 0.5 * shear^2 < 0.1, matching the CPU reference.

extern "C" __global__
void brn_kernel(
    const double* __restrict__ cape,
    const double* __restrict__ shear,
    double* __restrict__ brn,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double s = shear[idx];
    double denom = 0.5 * s * s;
    if (denom < 0.1) {
        brn[idx] = nan("");
    } else {
        brn[idx] = cape[idx] / denom;
    }
}
