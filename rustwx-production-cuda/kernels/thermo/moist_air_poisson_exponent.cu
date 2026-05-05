// Moist-air Poisson exponent kappa = R_moist / Cp_moist (dimensionless).
//   Rd=287.058, eps=0.622, Cp_d=1005.7, Cp_v=1875.0, w in kg/kg.

extern "C" __global__
void moist_air_poisson_exponent_kernel(
    const double* __restrict__ mixing_ratio,
    double* __restrict__ kappa,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    const double Rd_local = 287.058;
    const double eps = 0.622;
    const double Cp_d_local = 1005.7;
    const double Cp_v_local = 1875.0;
    double w = mixing_ratio[idx];
    double r_m = Rd_local * (1.0 + w / eps) / (1.0 + w);
    double cp_m = Cp_d_local * (1.0 + (Cp_v_local / Cp_d_local) * w) / (1.0 + w);
    kappa[idx] = r_m / cp_m;
}
