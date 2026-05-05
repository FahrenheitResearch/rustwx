// speed = sqrt(u*u + v*v)
//
// Inputs:  u (m/s), v (m/s).
// Output:  speed (m/s).
// Mirrors wx-math::dynamics::wind_speed.

extern "C" __global__
void wind_speed_kernel(
    const double* __restrict__ u,
    const double* __restrict__ v,
    double* __restrict__ speed,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double ui = u[idx];
    double vi = v[idx];
    speed[idx] = sqrt(ui * ui + vi * vi);
}
