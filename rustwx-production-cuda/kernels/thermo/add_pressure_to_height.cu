// Add a pressure increment (hPa) to a height (m) via the standard atmosphere.
//   p     = P0_STD * (1 - LAPSE_STD * h / T0_STD) ^ (G0 / (RD * LAPSE_STD))
//   h_new = (T0_STD / LAPSE_STD) * (1 - ((p + dp) / P0_STD) ^ (RD * LAPSE_STD / G0))

extern "C" __global__
void add_pressure_to_height_kernel(
    const double* __restrict__ height,
    const double* __restrict__ delta_pressure,
    double* __restrict__ h_new,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double h = height[idx];
    double dp = delta_pressure[idx];
    double p = P0_STD * pow(1.0 - LAPSE_STD * h / T0_STD, G0 / (RD * LAPSE_STD));
    h_new[idx] = (T0_STD / LAPSE_STD) * (1.0 - pow((p + dp) / P0_STD, (RD * LAPSE_STD) / G0));
}
