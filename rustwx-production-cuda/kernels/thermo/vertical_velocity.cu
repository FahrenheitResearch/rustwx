// Vertical velocity (m/s) from omega (Pa/s), pressure (hPa), temperature (Celsius).
//   t_k = T_C + 273.15
//   rho = (p * 100) / (RD * t_k)
//   w   = -omega / (rho * g0)

extern "C" __global__
void vertical_velocity_kernel(
    const double* __restrict__ omega,
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    double* __restrict__ w_out,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double t_k = temperature[idx] + ZEROCNK;
    double p_pa = pressure[idx] * 100.0;
    double rho = p_pa / (RD * t_k);
    w_out[idx] = -omega[idx] / (rho * G0);
}
