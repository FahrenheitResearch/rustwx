// NWS / Environment Canada wind chill (FCM 2003).
// Inputs: T (Celsius), wind speed (m/s). Output: WC (Celsius).
//   v_kmh = ws * 3.6
//   spf   = v_kmh^0.16
//   wc    = (0.6215 + 0.3965 * spf) * t_c - 11.37 * spf + 13.12

extern "C" __global__
void windchill_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ wind_speed,
    double* __restrict__ wc,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double wind_kmh = wind_speed[idx] * 3.6;
    double spf = pow(wind_kmh, 0.16);
    wc[idx] = (0.6215 + 0.3965 * spf) * temperature[idx] - 11.37 * spf + 13.12;
}
