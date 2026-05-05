// Standard atmosphere: pressure (hPa) -> height (m).
//   h = (T0_STD / LAPSE_STD) * (1 - (p / P0_STD) ^ (RD * LAPSE_STD / G0))
// Matches wx_math::thermo::pressure_to_height_std.

extern "C" __global__
void pressure_to_height_std_kernel(
    const double* __restrict__ pressure,
    double* __restrict__ height,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double p = pressure[idx];
    height[idx] = (T0_STD / LAPSE_STD) * (1.0 - pow(p / P0_STD, (RD * LAPSE_STD) / G0));
}
