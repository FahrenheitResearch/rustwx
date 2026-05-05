// Lf(T) = 3.34e5 + 2106.0 * t_c   (J/kg)

extern "C" __global__
void water_latent_heat_melting_kernel(
    const double* __restrict__ temperature,
    double* __restrict__ lf,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    lf[idx] = 3.34e5 + 2106.0 * temperature[idx];
}
