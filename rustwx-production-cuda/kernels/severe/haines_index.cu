// Haines Index (Low Elevation variant).
//
// Mirrors wx_math::composite::haines_index (which returns u8). The kernel
// returns the value as a double (always integer-valued in [2, 6]).
//
// Stability term A: T950 - T850
//   delta_t <= 3 -> 1; <= 7 -> 2; else 3
// Moisture term B: T850 - Td850
//   depression <= 5 -> 1; <= 9 -> 2; else 3
// Output: A + B.

extern "C" __global__
void haines_index_kernel(
    const double* __restrict__ t950,
    const double* __restrict__ t850,
    const double* __restrict__ td850,
    double* __restrict__ haines,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double dt = t950[idx] - t850[idx];
    double a;
    if (dt <= 3.0) a = 1.0;
    else if (dt <= 7.0) a = 2.0;
    else a = 3.0;

    double dd = t850[idx] - td850[idx];
    double b;
    if (dd <= 5.0) b = 1.0;
    else if (dd <= 9.0) b = 2.0;
    else b = 3.0;

    haines[idx] = a + b;
}
