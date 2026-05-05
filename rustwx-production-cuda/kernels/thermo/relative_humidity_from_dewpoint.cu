// Relative humidity (%) from temperature and dewpoint (both Celsius).
//   RH = SVP(Td) / SVP(T) * 100
// Uses the device helper `svp_hpa` from thermo_helpers.cuh.

extern "C" __global__
void relative_humidity_from_dewpoint_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ dewpoint,
    double* __restrict__ rh_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double es_t = svp_hpa(temperature[idx]);
    double es_td = svp_hpa(dewpoint[idx]);
    rh_out[idx] = (es_td / es_t) * 100.0;
}
