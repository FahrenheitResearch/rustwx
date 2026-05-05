// f = 2 * OMEGA * sin(latitude_rad)
// Inputs:  latitude (degrees).
// Output:  Coriolis parameter (s^-1).
// Mirrors wx-math::dynamics::coriolis_parameter.

extern "C" __global__
void coriolis_parameter_kernel(
    const double* __restrict__ latitude,
    double* __restrict__ f_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double lat = latitude[idx];
    f_out[idx] = 2.0 * OMEGA * sin(lat * M_PI / 180.0);
}
