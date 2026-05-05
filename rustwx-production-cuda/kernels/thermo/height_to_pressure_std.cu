// Standard atmosphere: height (m) -> pressure (hPa).
//   p = P0_STD * (1 - LAPSE_STD * h / T0_STD) ^ (G0 / (RD * LAPSE_STD))
// Matches wx_math::thermo::height_to_pressure_std.

extern "C" __global__
void height_to_pressure_std_kernel(
    const double* __restrict__ height,
    double* __restrict__ pressure,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double h = height[idx];
    pressure[idx] = P0_STD * pow(1.0 - LAPSE_STD * h / T0_STD, G0 / (RD * LAPSE_STD));
}
