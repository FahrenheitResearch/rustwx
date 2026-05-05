// Dry static energy:  DSE = Cp_d * T + g * z   (J/kg)
// Inputs are passed verbatim — the multiplication does not transform units;
// callers should pass T in Kelvin (the wx-math convention) to obtain
// thermodynamically meaningful values.

extern "C" __global__
void dry_static_energy_kernel(
    const double* __restrict__ height,
    const double* __restrict__ temperature,
    double* __restrict__ dse,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    dse[idx] = 1004.6662184201462 * temperature[idx]
             + 9.80665 * height[idx];
}
