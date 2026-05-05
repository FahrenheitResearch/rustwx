// Saturation mixing ratio (kg/kg) at p (hPa), T (Celsius).
// Uses the device helper `sat_mixing_ratio` from thermo_helpers.cuh.

extern "C" __global__
void saturation_mixing_ratio_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    double* __restrict__ ws_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    ws_out[idx] = sat_mixing_ratio(pressure[idx], temperature[idx]);
}
