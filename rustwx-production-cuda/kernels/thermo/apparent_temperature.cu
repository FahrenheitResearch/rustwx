// Apparent temperature (Celsius) selecting between Rothfusz heat-index, NWS
// wind-chill, or pass-through based on the t_f/wind thresholds. Matches
// met-cu verbatim.

extern "C" __global__
void apparent_temperature_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ rh,
    const double* __restrict__ wind_speed,
    double* __restrict__ at_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double t_c = temperature[i];
    double rh_pct = rh[i];
    double ws = wind_speed[i];
    double t_f = t_c * 9.0 / 5.0 + 32.0;
    double wind_mph = ws * 2.23694;

    if (t_f >= 80.0) {
        // Heat index (raw Rothfusz, no Steadman threshold)
        double hi_f = -42.379
            + 2.04901523 * t_f
            + 10.14333127 * rh_pct
            - 0.22475541 * t_f * rh_pct
            - 0.00683783 * t_f * t_f
            - 0.05481717 * rh_pct * rh_pct
            + 0.00122874 * t_f * t_f * rh_pct
            + 0.00085282 * t_f * rh_pct * rh_pct
            - 0.00000199 * t_f * t_f * rh_pct * rh_pct;
        if (rh_pct < 13.0 && t_f >= 80.0 && t_f <= 112.0) {
            hi_f -= ((13.0 - rh_pct) / 4.0) * sqrt((17.0 - fabs(t_f - 95.0)) / 17.0);
        } else if (rh_pct > 85.0 && t_f >= 80.0 && t_f <= 87.0) {
            hi_f += ((rh_pct - 85.0) / 10.0) * ((87.0 - t_f) / 5.0);
        }
        at_out[i] = (hi_f - 32.0) * 5.0 / 9.0;
    } else if (t_f <= 50.0 && wind_mph > 3.0) {
        double wind_kmh = ws * 3.6;
        double spf = pow(wind_kmh, 0.16);
        at_out[i] = (0.6215 + 0.3965 * spf) * t_c - 11.37 * spf + 13.12;
    } else {
        at_out[i] = t_c;
    }
}
