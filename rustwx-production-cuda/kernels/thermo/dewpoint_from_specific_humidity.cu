// Dewpoint (Celsius) from pressure (hPa) and specific humidity (kg/kg).
//   w = q / (1 - q)
//   e = w * p / (EPS + w)
//   Td = dewpoint_from_vp(e)

extern "C" __global__
void dewpoint_from_specific_humidity_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ specific_humidity,
    double* __restrict__ td_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double q = specific_humidity[i];
    double p = pressure[i];
    double w = q / (1.0 - q);
    double e = w * p / (EPS + w);
    td_out[i] = dewpoint_from_vp(e);
}
