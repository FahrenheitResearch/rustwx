// Convert an angle in degrees to a cardinal direction code in [0, n_directions).
// 0 = N, 1 = NNE, ..., (with n_directions = 16 → standard 16-point compass).
// code = floor((((deg mod 360) + 360 mod 360) + step/2) / step), wrapped to 0
// when result == n_directions.

extern "C" __global__
void angle_to_direction_kernel(
    const double* __restrict__ deg,
    const double* __restrict__ n_dirs,
    double* __restrict__ code,
    int n
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= n) return;
    double d = fmod(deg[idx], 360.0);
    if (d < 0.0) d += 360.0;
    double nd = n_dirs[idx];
    double step = 360.0 / nd;
    double c = floor((d + step / 2.0) / step);
    if (c >= nd) c = 0.0;
    code[idx] = c;
}
