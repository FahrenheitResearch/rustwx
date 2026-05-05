// theta = T_K * (1000/p)^(Rd/Cp)
//
// Inputs:  pressure (hPa), temperature (Celsius).
// Output:  theta (Kelvin).
// Mirrors metrust::calc::thermo::potential_temperature.
//
// NOTE: helpers (ZEROCNK, ROCP) are prepended at compile time by the Rust
// loader — do not add an `#include` here, NVRTC won't resolve filesystem paths.

extern "C" __global__
void potential_temperature_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ temperature,
    double* __restrict__ theta,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double p = pressure[idx];
    double t_k = temperature[idx] + ZEROCNK;
    theta[idx] = t_k * pow(1000.0 / p, ROCP);
}
