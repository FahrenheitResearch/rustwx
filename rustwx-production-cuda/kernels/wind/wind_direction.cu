// Meteorological wind direction (degrees) from u, v.
// Mirrors met-cu's wind_direction kernel: rad = atan2(-u, -v); deg in [0, 360).
// Calm winds (u == 0 && v == 0) return 0.

extern "C" __global__
void wind_direction_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    double* __restrict__ wdir,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double ui = u[idx];
    double vi = v[idx];
    double rad = atan2(-ui, -vi);
    double d = rad * 180.0 / M_PI;
    if (d < 0.0) d += 360.0;
    if (ui == 0.0 && vi == 0.0) d = 0.0;
    wdir[idx] = d;
}
