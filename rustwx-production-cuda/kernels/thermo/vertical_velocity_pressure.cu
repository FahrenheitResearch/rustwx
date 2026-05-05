// Omega (Pa/s) from w (m/s), pressure (hPa), temperature (Celsius).
//   t_k   = T_C + 273.15
//   rho   = (p * 100) / (RD * t_k)
//   omega = -rho * g0 * w

extern "C" __global__
void vertical_velocity_pressure_kernel(
    const double* __restrict__ w,
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    double* __restrict__ omega_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t_k = temperature[idx] + ZEROCNK;
    double p_pa = pressure[idx] * 100.0;
    double rho = p_pa / (RD * t_k);
    omega_out[idx] = -rho * G0 * w[idx];
}
