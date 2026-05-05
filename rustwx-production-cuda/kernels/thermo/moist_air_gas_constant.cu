// Moist-air gas constant (J/(kg*K)):
//   R = Rd * (1 + w/eps) / (1 + w),  Rd=287.058, eps=0.622  (mixing ratio in kg/kg)

extern "C" __global__
void moist_air_gas_constant_kernel(
    const double* __restrict__ mixing_ratio,
    double* __restrict__ r_moist,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    const double Rd_local = 287.058;
    const double eps = 0.622;
    double w = mixing_ratio[idx];
    r_moist[idx] = Rd_local * (1.0 + w / eps) / (1.0 + w);
}
