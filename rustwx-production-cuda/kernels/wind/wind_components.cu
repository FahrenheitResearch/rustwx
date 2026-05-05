// (u, v) wind components from speed (m/s) and meteorological direction (deg).
// u = -speed * sin(rad); v = -speed * cos(rad).

extern "C" __global__
void wind_components_kernel(
    const double* __restrict__ speed,
    const double* __restrict__ direction,
    double* __restrict__ u_out,
    double* __restrict__ v_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double s = speed[idx];
    double rad = direction[idx] * M_PI / 180.0;
    u_out[idx] = -s * sin(rad);
    v_out[idx] = -s * cos(rad);
}
