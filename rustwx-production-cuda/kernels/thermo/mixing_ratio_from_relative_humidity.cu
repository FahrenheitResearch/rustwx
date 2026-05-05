// Mixing ratio (kg/kg) from p (hPa), T (Celsius), RH (%).
//   ws (kg/kg) = sat_mixing_ratio(p, T)
//   w  = ws * rh / 100

extern "C" __global__
void mixing_ratio_from_relative_humidity_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    const double* __restrict__ rh,
    double* __restrict__ w_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double ws = sat_mixing_ratio(pressure[i], temperature[i]);
    w_out[i] = ws * rh[i] / 100.0;
}
