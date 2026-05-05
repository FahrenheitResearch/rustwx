// Frost point (Celsius) from temperature (Celsius) and RH (%).
//   es_water = svp_hpa(T)
//   e        = (rh/100) * es_water
//   Tf       = 272.62 * ln(e/6.112) / (22.46 - ln(e/6.112))   (Magnus over ice)

extern "C" __global__
void frost_point_kernel(
    const double* __restrict__ temperature,
    const double* __restrict__ rh,
    double* __restrict__ fp_out,
    int n
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    if (i >= n) return;
    double es_water = svp_hpa(temperature[i]);
    double e = (rh[i] / 100.0) * es_water;
    double ln_ratio = log(e / 6.112);
    fp_out[i] = 272.62 * ln_ratio / (22.46 - ln_ratio);
}
