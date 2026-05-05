// T = theta * (p/1000)^(Rd/Cp)
//
// Inputs:  pressure (hPa), theta (Kelvin).
// Output:  temperature (Kelvin).
// Mirrors wx_math::thermo::temperature_from_potential_temperature.

extern "C" __global__
void temperature_from_potential_temperature_kernel(
    const double* __restrict__ pressure,
    const double* __restrict__ theta,
    double* __restrict__ temperature,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    temperature[idx] = theta[idx] * pow(pressure[idx] / 1000.0, ROCP);
}
