// Ls(T) = Lv(T) + Lf(T)
//        = (2.501e6 - 2370 * t_c) + (3.34e5 + 2106 * t_c)        (J/kg)

extern "C" __global__
void water_latent_heat_sublimation_kernel(
    const double* __restrict__ temperature,
    double* __restrict__ ls,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t = temperature[idx];
    double lv = 2.501e6 - 2370.0 * t;
    double lf = 3.34e5  + 2106.0 * t;
    ls[idx] = lv + lf;
}
