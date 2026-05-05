// Lv(T) = 2.501e6 - 2370.0 * t_c   (J/kg)

extern "C" __global__
void water_latent_heat_vaporization_kernel(
    const double* __restrict__ temperature,
    double* __restrict__ lv,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    lv[idx] = 2.501e6 - 2370.0 * temperature[idx];
}
