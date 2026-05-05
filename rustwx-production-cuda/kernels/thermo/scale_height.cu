// Atmospheric scale height H = Rd * T / g0   (meters).
// Input: temperature (Kelvin).

extern "C" __global__
void scale_height_kernel(
    const double* __restrict__ temperature,
    double* __restrict__ H,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    H[idx] = 287.04749097718457 * temperature[idx] / 9.80665;
}
