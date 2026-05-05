// Relative humidity (%) from p (hPa), T (Celsius), specific humidity (kg/kg).
//   w  = q / (1 - q)
//   ws = sat_mixing_ratio(p, T)         (kg/kg)
//   rh = (ws > 0) ? (w / ws) * 100 : 0

extern "C" __global__
void relative_humidity_from_specific_humidity_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    const double* __restrict__ specific_humidity,
    double* __restrict__ rh_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double q = specific_humidity[i];
    double w = q / (1.0 - q);
    double ws = sat_mixing_ratio(pressure[i], temperature[i]);
    rh_out[i] = (ws > 0.0) ? (w / ws) * 100.0 : 0.0;
}
