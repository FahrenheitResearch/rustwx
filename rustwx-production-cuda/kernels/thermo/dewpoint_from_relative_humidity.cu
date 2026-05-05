// Dewpoint (Celsius) from temperature (Celsius) and relative humidity (%).
//   es = svp_hpa(T)        -- Ambaum (2020)
//   e  = (rh/100) * es
//   Td = 243.5 * ln(e/6.112) / (17.67 - ln(e/6.112))   -- inverse Bolton

extern "C" __global__
void dewpoint_from_relative_humidity_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ relative_humidity,
    double* __restrict__ td_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double es = svp_hpa(temperature[i]);
    double e = (relative_humidity[i] / 100.0) * es;
    double ln_ratio = log(e / 6.112);
    td_out[i] = 243.5 * ln_ratio / (17.67 - ln_ratio);
}
