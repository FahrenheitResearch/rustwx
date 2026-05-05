// Altimeter setting (hPa) -> station pressure (hPa) using the simple ratio
// formula (matches wx_math::thermo::altimeter_to_station_pressure).
//   ratio = 1 - L*elev / (T0_STD + L*elev)
//   p_stn = alt * ratio^(1/ROCP)

extern "C" __global__
void altimeter_to_station_pressure_kernel(
    const double* __restrict__ altimeter,
    const double* __restrict__ elevation,
    double* __restrict__ p_stn,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double alt = altimeter[idx];
    double elev = elevation[idx];
    double ratio = 1.0 - (LAPSE_STD * elev) / (T0_STD + LAPSE_STD * elev);
    p_stn[idx] = alt * pow(ratio, 1.0 / ROCP);
}
