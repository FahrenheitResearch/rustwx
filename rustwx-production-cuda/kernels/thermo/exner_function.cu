// Exner function:  Pi = (p / 1000)^kappa
// Input: pressure (hPa).

extern "C" __global__
void exner_function_kernel(
    const double* __restrict__ pressure,
    double* __restrict__ exner,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    exner[idx] = pow(pressure[idx] / 1000.0, ROCP);
}
