// Montgomery streamfunction (J/kg): psi = Cp_d * T_K + g0 * z.
// Input temperature here is treated as Kelvin to match metrust/wx_math
// (which take t_k directly).

extern "C" __global__
void montgomery_streamfunction_kernel(
    const double* __restrict__ height,
    const double* __restrict__ temperature_k,
    double* __restrict__ psi,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    psi[idx] = CP_D * temperature_k[idx] + G0 * height[idx];
}
