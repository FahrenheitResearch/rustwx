// Saturation vapor pressure (hPa) over liquid water (Ambaum 2020).
// Uses the device helper `svp_hpa` from thermo_helpers.cuh.

extern "C" __global__
void saturation_vapor_pressure_kernel(
    const double* __restrict__ temperature,
    double* __restrict__ es_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    es_out[idx] = svp_hpa(temperature[idx]);
}
