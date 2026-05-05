// Specific humidity (kg/kg) from p (hPa) and dewpoint (Celsius).
//   e = svp_hpa(td)
//   w = EPS * e / (p - e)        (kg/kg)
//   q = w / (1 + w)

extern "C" __global__
void specific_humidity_from_dewpoint_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ dewpoint,
    double* __restrict__ q_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double e = svp_hpa(dewpoint[i]);
    double w = EPS * e / (pressure[i] - e);
    q_out[i] = w / (1.0 + w);
}
