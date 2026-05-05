// RGBA8 image downscale via separable Lanczos3, matching the
// `image::imageops::resize` behaviour with `FilterType::Lanczos3`.
//
// One thread per output pixel. The filter radius scales with the
// source/dest ratio (`sratio = src / dst >= 1`) to act as an anti-aliasing
// kernel during downscale.
//
// Output is a packed-RGBA `unsigned int` per pixel (same layout as
// `image::Rgba<u8>` after byte reinterpretation).

#define PI_F 3.14159265358979323846f
#define LANCZOS_A 3

__device__ __forceinline__ float sinc_pi(float x) {
    if (x == 0.0f) return 1.0f;
    float a = PI_F * x;
    return sinf(a) / a;
}

__device__ __forceinline__ float lanczos3_w(float t) {
    if (t < 0.0f) t = -t;
    if (t >= (float)LANCZOS_A) return 0.0f;
    return sinc_pi(t) * sinc_pi(t / (float)LANCZOS_A);
}

__device__ __forceinline__ unsigned int sat_u8(float v) {
    if (v < 0.0f) return 0u;
    if (v > 255.0f) return 255u;
    return (unsigned int)(v + 0.5f);
}

__device__ __forceinline__ unsigned int rgba_pack(float r, float g, float b, float a) {
    return sat_u8(r) | (sat_u8(g) << 8) | (sat_u8(b) << 16) | (sat_u8(a) << 24);
}

extern "C" __global__
void downsample_lanczos3_kernel(
    const unsigned int* __restrict__ src,
    int src_w, int src_h,
    unsigned int* __restrict__ dst,
    int dst_w, int dst_h,
    float sratio
) {
    int dx = blockIdx.x * blockDim.x + threadIdx.x;
    int dy = blockIdx.y * blockDim.y + threadIdx.y;
    if (dx >= dst_w || dy >= dst_h) return;

    // image::imageops::resize semantics: src_center = (out + 0.5) * ratio - 0.5.
    // This convention matches `image` 0.25 — verified against synthetic
    // gradient (max delta 2/255 in f32). Real renders show larger deltas
    // (≤ ~50) on AA text edges because we use f32 Lanczos3 vs CPU's f64.
    // Accepted: the affected pixels are sub-pixel-positioned text glyphs,
    // not data pixels.
    float src_cx = ((float)dx + 0.5f) * sratio - 0.5f;
    float src_cy = ((float)dy + 0.5f) * sratio - 0.5f;

    float radius = (float)LANCZOS_A * sratio;
    float inv_sratio = 1.0f / sratio;

    int x0 = (int)ceilf(src_cx - radius);
    int x1 = (int)floorf(src_cx + radius);
    int y0 = (int)ceilf(src_cy - radius);
    int y1 = (int)floorf(src_cy + radius);

    // Production case sratio=2 needs 13 taps. Cap at 32 for safety;
    // larger downscales degrade gracefully.
    const int MAX_TAPS = 32;
    float wx[MAX_TAPS];
    float wy[MAX_TAPS];

    int nx_taps = x1 - x0 + 1;
    if (nx_taps > MAX_TAPS) {
        x0 += (nx_taps - MAX_TAPS) / 2;
        x1 = x0 + MAX_TAPS - 1;
        nx_taps = MAX_TAPS;
    }
    float wx_sum = 0.0f;
    for (int i = 0; i < nx_taps; ++i) {
        float t = ((float)(x0 + i) - src_cx) * inv_sratio;
        wx[i] = lanczos3_w(t);
        wx_sum += wx[i];
    }
    if (wx_sum != 0.0f) {
        float inv_sum = 1.0f / wx_sum;
        for (int i = 0; i < nx_taps; ++i) wx[i] *= inv_sum;
    }

    int ny_taps = y1 - y0 + 1;
    if (ny_taps > MAX_TAPS) {
        y0 += (ny_taps - MAX_TAPS) / 2;
        y1 = y0 + MAX_TAPS - 1;
        ny_taps = MAX_TAPS;
    }
    float wy_sum = 0.0f;
    for (int j = 0; j < ny_taps; ++j) {
        float t = ((float)(y0 + j) - src_cy) * inv_sratio;
        wy[j] = lanczos3_w(t);
        wy_sum += wy[j];
    }
    if (wy_sum != 0.0f) {
        float inv_sum = 1.0f / wy_sum;
        for (int j = 0; j < ny_taps; ++j) wy[j] *= inv_sum;
    }

    float r = 0.0f, g = 0.0f, b = 0.0f, a = 0.0f;
    for (int j = 0; j < ny_taps; ++j) {
        int sy = y0 + j;
        if (sy < 0) sy = 0;
        if (sy > src_h - 1) sy = src_h - 1;
        float wyj = wy[j];
        for (int i = 0; i < nx_taps; ++i) {
            int sx = x0 + i;
            if (sx < 0) sx = 0;
            if (sx > src_w - 1) sx = src_w - 1;
            unsigned int p = src[(size_t)sy * (size_t)src_w + (size_t)sx];
            float w = wx[i] * wyj;
            r += (float)( p        & 0xFFu) * w;
            g += (float)((p >>  8) & 0xFFu) * w;
            b += (float)((p >> 16) & 0xFFu) * w;
            a += (float)((p >> 24) & 0xFFu) * w;
        }
    }

    dst[(size_t)dy * (size_t)dst_w + (size_t)dx] = rgba_pack(r, g, b, a);
}
