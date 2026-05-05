// Station pressure (hPa) -> altimeter setting (hPa) using met-cu's literal
// BARO_EXP = 0.190284 (Smithsonian inverse).
//   n     = 1 / BARO_EXP
//   alt   = ((p_stn - 0.3)^n + P0_STD^n * LAPSE_STD * elev / T0_STD) ^ (1/n)

extern "C" __global__
void station_to_altimeter_pressure_kernel(
    const double* __restrict__ station_pressure,
    const double* __restrict__ elevation,
    double* __restrict__ altimeter,
    int n_elem
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n_elem) return;
    const double BARO_EXP = 0.190284;
    double p_stn = station_pressure[idx];
    double elev = elevation[idx];
    double n = 1.0 / BARO_EXP;
    double term = pow(p_stn - 0.3, n) + pow(P0_STD, n) * LAPSE_STD * elev / T0_STD;
    altimeter[idx] = pow(term, 1.0 / n);
}
