// Moist-air Cp at constant pressure (J/(kg*K)):
//   Cp = Cp_d * (1 + (Cp_v/Cp_d)*w) / (1 + w)
// Cp_d=1005.7, Cp_v=1875.0, w in kg/kg.

extern "C" __global__
void moist_air_specific_heat_pressure_kernel(
    const double* __restrict__ mixing_ratio,
    double* __restrict__ cp_moist,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    const double Cp_d_local = 1005.7;
    const double Cp_v_local = 1875.0;
    double w = mixing_ratio[idx];
    cp_moist[idx] = Cp_d_local * (1.0 + (Cp_v_local / Cp_d_local) * w) / (1.0 + w);
}
