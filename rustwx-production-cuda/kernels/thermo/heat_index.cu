// NWS heat index (Rothfusz regression). Input: T (Celsius), RH (%). Output: HI (Celsius).
// Note: the threshold logic in this kernel (`t_f < 80`) differs slightly from
// metrust's heat_index, which averages Steadman with T_F before the threshold check.

extern "C" __global__
void heat_index_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ relative_humidity,
    double* __restrict__ hi,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t_f = temperature[idx] * 9.0 / 5.0 + 32.0;
    double rh = relative_humidity[idx];
    double hi_f;
    double steadman = 0.5 * (t_f + 61.0 + (t_f - 68.0) * 1.2 + rh * 0.094);
    if (t_f < 80.0) {
        hi_f = steadman;
    } else {
        hi_f = -42.379
             + 2.04901523 * t_f
             + 10.14333127 * rh
             - 0.22475541 * t_f * rh
             - 0.00683783 * t_f * t_f
             - 0.05481717 * rh * rh
             + 0.00122874 * t_f * t_f * rh
             + 0.00085282 * t_f * rh * rh
             - 0.00000199 * t_f * t_f * rh * rh;
        if (rh < 13.0 && t_f >= 80.0 && t_f <= 112.0) {
            hi_f -= ((13.0 - rh) / 4.0) * sqrt((17.0 - fabs(t_f - 95.0)) / 17.0);
        } else if (rh > 85.0 && t_f >= 80.0 && t_f <= 87.0) {
            hi_f += ((rh - 85.0) / 10.0) * ((87.0 - t_f) / 5.0);
        }
    }
    hi[idx] = (hi_f - 32.0) * 5.0 / 9.0;
}
