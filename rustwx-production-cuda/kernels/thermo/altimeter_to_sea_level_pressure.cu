// Altimeter setting (hPa) -> sea level pressure (hPa) using the met-cu
// formulation: simple step-1 (no Smithsonian +0.3) followed by hypsometric
// SLP with Rd = 287.058 (matches met-cu, NOT metrust's Smithsonian variant).
//   ratio  = 1 - L*elev / (T0 + L*elev)
//   p_stn  = altimeter * ratio^(1/ROCP) + 0.3
//   T_mean = (t_c + 273.15) + 0.5 * L * elev
//   slp    = p_stn * exp(g * elev / (Rd * T_mean))

extern "C" __global__
void altimeter_to_sea_level_pressure_kernel(
    const double* __restrict__ altimeter,
    const double* __restrict__ elevation,
    const double* __restrict__ temperature,
    double* __restrict__ slp,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    const double Rd_alt = 287.058;
    double alt = altimeter[idx];
    double elev = elevation[idx];
    double t_c = temperature[idx];
    double ratio = 1.0 - (LAPSE_STD * elev) / (T0_STD + LAPSE_STD * elev);
    double p_stn = alt * pow(ratio, 1.0 / ROCP) + 0.3;
    double t_sfc_k = t_c + ZEROCNK;
    double t_mean_k = t_sfc_k + 0.5 * LAPSE_STD * elev;
    slp[idx] = p_stn * exp(G0 * elev / (Rd_alt * t_mean_k));
}
