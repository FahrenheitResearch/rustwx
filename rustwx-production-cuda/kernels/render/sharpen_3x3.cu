// 3x3 unsharp-mask sharpen on packed-RGBA8 image. Matches CPU
// `sharpen_downsampled_image`:
//   kernel = [ 0   -0.22  0
//             -0.22 1.88 -0.22
//              0   -0.22  0 ]
// Edge pixels clamp to nearest neighbour.

__device__ __forceinline__ unsigned int sat_u8(float v) {
    if (v < 0.0f) return 0u;
    if (v > 255.0f) return 255u;
    return (unsigned int)(v + 0.5f);
}

__device__ __forceinline__ unsigned int rgba_pack_u8(float r, float g, float b, float a) {
    return sat_u8(r) | (sat_u8(g) << 8) | (sat_u8(b) << 16) | (sat_u8(a) << 24);
}

__device__ __forceinline__ unsigned int read_clamped(
    const unsigned int* __restrict__ src, int xx, int yy, int w, int h
) {
    if (xx < 0) xx = 0;
    if (yy < 0) yy = 0;
    if (xx > w - 1) xx = w - 1;
    if (yy > h - 1) yy = h - 1;
    return src[(size_t)yy * (size_t)w + (size_t)xx];
}

extern "C" __global__
void sharpen_3x3_kernel(
    const unsigned int* __restrict__ src,
    unsigned int* __restrict__ dst,
    int w, int h
) {
    int x = blockIdx.x * blockDim.x + threadIdx.x;
    int y = blockIdx.y * blockDim.y + threadIdx.y;
    if (x >= w || y >= h) return;

    const float k_center = 1.88f;
    const float k_edge   = -0.22f;

    unsigned int c  = read_clamped(src, x,     y,     w, h);
    unsigned int nN = read_clamped(src, x,     y - 1, w, h);
    unsigned int nS = read_clamped(src, x,     y + 1, w, h);
    unsigned int nW = read_clamped(src, x - 1, y,     w, h);
    unsigned int nE = read_clamped(src, x + 1, y,     w, h);

    float r = (float)( c  & 0xFFu)        * k_center
            + (float)(nN  & 0xFFu)        * k_edge
            + (float)(nS  & 0xFFu)        * k_edge
            + (float)(nW  & 0xFFu)        * k_edge
            + (float)(nE  & 0xFFu)        * k_edge;
    float g = (float)((c  >> 8) & 0xFFu)  * k_center
            + (float)((nN >> 8) & 0xFFu)  * k_edge
            + (float)((nS >> 8) & 0xFFu)  * k_edge
            + (float)((nW >> 8) & 0xFFu)  * k_edge
            + (float)((nE >> 8) & 0xFFu)  * k_edge;
    float b = (float)((c  >> 16) & 0xFFu) * k_center
            + (float)((nN >> 16) & 0xFFu) * k_edge
            + (float)((nS >> 16) & 0xFFu) * k_edge
            + (float)((nW >> 16) & 0xFFu) * k_edge
            + (float)((nE >> 16) & 0xFFu) * k_edge;
    float a = (float)((c  >> 24) & 0xFFu) * k_center
            + (float)((nN >> 24) & 0xFFu) * k_edge
            + (float)((nS >> 24) & 0xFFu) * k_edge
            + (float)((nW >> 24) & 0xFFu) * k_edge
            + (float)((nE >> 24) & 0xFFu) * k_edge;

    dst[(size_t)y * (size_t)w + (size_t)x] = rgba_pack_u8(r, g, b, a);
}
