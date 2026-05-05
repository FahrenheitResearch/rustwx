// Device-to-device 2D rectangular crop. Lets a sub-region view into a larger
// host-uploaded grid without re-uploading. Used by the benchmark to upload
// CONUS once and extract 7 regional crops on-device.
//
// Source:      [src_ny][src_nx]  row-major.
// Destination: [dst_ny][dst_nx]  row-major contiguous.
// off_x, off_y: top-left corner of the rectangle in source coords.

extern "C" __global__
void crop_2d_kernel(
    const double* __restrict__ src,
    double* __restrict__ dst,
    int src_nx,
    int dst_nx, int dst_ny,
    int off_x, int off_y
) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int j = blockDim.y * blockIdx.y + threadIdx.y;
    if (i >= dst_nx || j >= dst_ny) return;
    int src_idx = (j + off_y) * src_nx + (i + off_x);
    int dst_idx = j * dst_nx + i;
    dst[dst_idx] = src[src_idx];
}
