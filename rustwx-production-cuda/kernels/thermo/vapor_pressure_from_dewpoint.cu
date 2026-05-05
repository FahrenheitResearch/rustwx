// Vapor pressure (hPa) from dewpoint (Celsius) = SVP at Td.
// Uses the device helper `svp_hpa` from thermo_helpers.cuh.

extern "C" __global__
void vapor_pressure_from_dewpoint_kernel(
    const double* __restrict__ dewpoint,
    double* __restrict__ e_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    e_out[idx] = svp_hpa(dewpoint[idx]);
}
