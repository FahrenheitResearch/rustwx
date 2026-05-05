// Critical angle (degrees) between low-level shear vector and storm-relative
// inflow. Port of met-cu's critical_angle_kernel — six-input form
// (storm_u, storm_v, u_sfc, v_sfc, u_500, v_500).

extern "C" __global__
void critical_angle_kernel(
    const double* __restrict__ storm_u,
    const double* __restrict__ storm_v,
    const double* __restrict__ u_sfc,
    const double* __restrict__ v_sfc,
    const double* __restrict__ u_500,
    const double* __restrict__ v_500,
    double* __restrict__ angle,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;

    double shr_u = u_500[idx] - u_sfc[idx];
    double shr_v = v_500[idx] - v_sfc[idx];
    double inf_u = storm_u[idx] - u_sfc[idx];
    double inf_v = storm_v[idx] - v_sfc[idx];

    double mag_shr = sqrt(shr_u * shr_u + shr_v * shr_v);
    double mag_inf = sqrt(inf_u * inf_u + inf_v * inf_v);
    double denom = mag_shr * mag_inf;

    double a;
    if (denom < 1e-10) {
        a = 0.0;
    } else {
        double cosang = (shr_u * inf_u + shr_v * inf_v) / denom;
        if (cosang > 1.0) cosang = 1.0;
        if (cosang < -1.0) cosang = -1.0;
        a = acos(cosang) * 180.0 / M_PI;
    }
    angle[idx] = a;
}
