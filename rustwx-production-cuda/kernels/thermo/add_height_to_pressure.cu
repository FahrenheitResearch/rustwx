// Add a height increment (m) to a pressure (hPa) via the standard atmosphere.
//   h     = (T0_STD / LAPSE_STD) * (1 - (p / P0_STD) ^ (RD*LAPSE/G0))
//   p_new = P0_STD * (1 - LAPSE_STD * (h + dh) / T0_STD) ^ (G0 / (RD * LAPSE_STD))

extern "C" __global__
void add_height_to_pressure_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ delta_height,
    double* __restrict__ p_new,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double p = pressure[idx];
    double dh = delta_height[idx];
    double h = (T0_STD / LAPSE_STD) * (1.0 - pow(p / P0_STD, (RD * LAPSE_STD) / G0));
    p_new[idx] = P0_STD * pow(1.0 - LAPSE_STD * (h + dh) / T0_STD, G0 / (RD * LAPSE_STD));
}
