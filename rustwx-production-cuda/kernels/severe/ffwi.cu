// Fosberg Fire Weather Index (FFWI).
//
// Mirrors wx_math::composite::fosberg_fire_weather_index exactly.
//
// Inputs:  t_f (deg F), rh (%, clamped to [0,100]), wspd_mph (mph).
// Output:  FFWI in [0, 100].

extern "C" __global__
void ffwi_kernel(
    const double* __restrict__ t_f,
    const double* __restrict__ rh,
    const double* __restrict__ wspd_mph,
    double* __restrict__ ffwi,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double tf = t_f[idx];
    double r = rh[idx];
    if (r < 0.0) r = 0.0;
    if (r > 100.0) r = 100.0;
    double w = wspd_mph[idx];

    double emc;
    if (r <= 10.0) {
        emc = 0.03229 + 0.281073 * r - 0.000578 * r * tf;
    } else if (r <= 50.0) {
        emc = 2.22749 + 0.160107 * r - 0.01478 * tf;
    } else {
        emc = 21.0606 + 0.005565 * r * r - 0.00035 * r * tf - 0.483199 * r;
    }

    double m = emc / 30.0;
    if (m < 0.0) m = 0.0;

    double eta = 1.0 - 2.0 * m + 1.5 * m * m - 0.5 * m * m * m;

    double fw = eta * sqrt(1.0 + w * w);
    double res = fw * 10.0 / 3.0;
    if (res < 0.0) res = 0.0;
    if (res > 100.0) res = 100.0;
    ffwi[idx] = res;
}
