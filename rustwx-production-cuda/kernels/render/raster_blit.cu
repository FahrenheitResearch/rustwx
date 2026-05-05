// Per-pixel "source over" alpha composite of a small RGBA `src` image onto
// a larger RGBA `canvas` at a fixed `(dst_x, dst_y)` offset, optionally
// gated by a per-source-pixel `clip_mask` (alpha-only is read).
//
// Mirrors the CPU loop in `rustwx_render::render`'s raster_blit step:
//
//     for py in 0..src_h {
//         for px in 0..src_w {
//             if let Some(mask) = clip_mask {
//                 if mask[px,py].a == 0 { continue; }
//             }
//             let s = src[px,py];
//             if s.a == 0 { continue; }
//             if s.a == 255 { canvas[dst_x+px, dst_y+py] = s; }
//             else          { blend_pixel(canvas, dst_x+px, dst_y+py, s); }
//         }
//     }
//
// `blend_pixel` (see rustwx-render/src/draw.rs) does straight-alpha source
// over with an opaque destination assumption — final alpha is forced to 255:
//
//     alpha = src.a / 255.0
//     out.r = round(src.r * alpha + dst.r * (1 - alpha))   (likewise g, b)
//     out.a = 255
//
// We replicate the rounding using `nearbyint` in double precision so the
// kernel matches the CPU `.round() as u8` to the LSB.
//
// Layout: one CUDA thread per source pixel `(sx, sy)`. Each thread writes
// at most one canvas pixel — pixels are independent (no cross-thread races)
// because the source is tiled 1:1 onto a non-overlapping canvas region.
//
// Pixel packing: u32 with R = bits 0-7, G = 8-15, B = 16-23, A = 24-31
// (matches `image::RgbaImage::as_raw()` byte order on little-endian).

__device__ __forceinline__ unsigned int rb_pack(
    unsigned int r, unsigned int g, unsigned int b, unsigned int a
) {
    return (r & 0xFFu)
         | ((g & 0xFFu) << 8)
         | ((b & 0xFFu) << 16)
         | ((a & 0xFFu) << 24);
}

__device__ __forceinline__ unsigned int rb_round_u8(double v) {
    // Match Rust's `.round() as u8` (round half away from zero, then
    // saturate). `nearbyint` honors FE_TONEAREST (round-half-to-even),
    // so we hand-roll round-half-away-from-zero to match Rust exactly.
    double r = (v >= 0.0) ? floor(v + 0.5) : ceil(v - 0.5);
    if (r < 0.0)   r = 0.0;
    if (r > 255.0) r = 255.0;
    return (unsigned int)r;
}

extern "C" __global__
void raster_blit_kernel(
    const unsigned int* __restrict__ src,        // [src_h][src_w] packed RGBA
    const unsigned int* __restrict__ clip_mask,  // [src_h][src_w] packed RGBA, may be NULL
    int has_clip_mask,                           // 0/1 — if 0, clip_mask is ignored
    unsigned int* __restrict__ canvas,           // [canvas_h][canvas_w] packed RGBA, modified in place
    int src_w, int src_h,
    int canvas_w, int canvas_h,
    int dst_x, int dst_y
) {
    int sx = blockIdx.x * blockDim.x + threadIdx.x;
    int sy = blockIdx.y * blockDim.y + threadIdx.y;
    if (sx >= src_w || sy >= src_h) return;

    int s_idx = sy * src_w + sx;

    // Clip-mask gate — match CPU exactly: only the alpha channel is read.
    if (has_clip_mask) {
        unsigned int m = clip_mask[s_idx];
        unsigned int ma = (m >> 24) & 0xFFu;
        if (ma == 0u) return;
    }

    unsigned int s = src[s_idx];
    unsigned int sa = (s >> 24) & 0xFFu;
    if (sa == 0u) return;

    // Bounds-check the canvas write. CPU `blend_pixel` silently no-ops on
    // out-of-bounds writes (see draw.rs:5–7); the opaque path uses
    // `put_pixel` which panics on OOB, but the CPU caller guarantees
    // `dst_x + sx < canvas_w` for `sx in 0..src_w`, so OOB is normally
    // unreachable. We still gate to be safe for negative offsets in future.
    long long cx = (long long)dst_x + (long long)sx;
    long long cy = (long long)dst_y + (long long)sy;
    if (cx < 0 || cy < 0 || cx >= (long long)canvas_w || cy >= (long long)canvas_h) {
        return;
    }
    int c_idx = (int)cy * canvas_w + (int)cx;

    unsigned int sr = (s      ) & 0xFFu;
    unsigned int sg = (s >>  8) & 0xFFu;
    unsigned int sb = (s >> 16) & 0xFFu;

    if (sa == 255u) {
        canvas[c_idx] = rb_pack(sr, sg, sb, 255u);
        return;
    }

    unsigned int d = canvas[c_idx];
    unsigned int dr = (d      ) & 0xFFu;
    unsigned int dg = (d >>  8) & 0xFFu;
    unsigned int db = (d >> 16) & 0xFFu;

    double alpha = (double)sa / 255.0;
    double inv   = 1.0 - alpha;

    unsigned int or_ = rb_round_u8((double)sr * alpha + (double)dr * inv);
    unsigned int og  = rb_round_u8((double)sg * alpha + (double)dg * inv);
    unsigned int ob  = rb_round_u8((double)sb * alpha + (double)db * inv);

    canvas[c_idx] = rb_pack(or_, og, ob, 255u);
}
