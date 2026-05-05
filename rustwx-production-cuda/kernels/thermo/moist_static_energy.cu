// Moist static energy:  MSE = Cp_d*T + g*z + Lv0*q   (J/kg)
// Inputs: height (m), temperature (Kelvin to match wx-math),
//         specific humidity q in kg/kg.

extern "C" __global__
void moist_static_energy_kernel(
    const double* __restrict__ height,
    const double* __restrict__ temperature,
    const double* __restrict__ specific_humidity,
    double* __restrict__ mse,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    mse[idx] = 1004.6662184201462 * temperature[idx]
             + 9.80665 * height[idx]
             + 2500840.0 * specific_humidity[idx];
}
