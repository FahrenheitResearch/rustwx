// Mirrors wx-math::regrid::interpolate_vertical:
//   3D volume `values_3d[k*ny*nx + idx]`, vertical coord `levels[k]` (size nz),
//   produce a 2D slab interpolated to `target_level`.
//
// Bracketing levels (k0, k1) are computed host-side and passed in as scalars
// — that lets the kernel be a single elementwise pass over `slab_size = ny*nx`.
// `log_interp != 0` triggers log-pressure interpolation; in that case the
// host pre-computes the weight too, since it's level-only and constant per slab.

extern "C" __global__
void interpolate_vertical_kernel(
    const double* __restrict__ values_3d,
    double* __restrict__ result,
    int slab_size,
    int offset0,
    int offset1,
    double weight
) {
    int idx = blockDim.x * blockIdx.x + threadIdx.x;
    if (idx >= slab_size) return;
    double v0 = values_3d[offset0 + idx];
    double v1 = values_3d[offset1 + idx];
    if (isnan(v0) || isnan(v1)) {
        result[idx] = nan("");
    } else {
        result[idx] = v0 + weight * (v1 - v0);
    }
}
