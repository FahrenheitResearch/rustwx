//! Map-rasterization CUDA kernels.
//!
//! Ports the inner per-pixel loop of `rustwx-render`'s `rasterize.rs` to GPU:
//!   - bilinear sample of an `[ny][nx]` `f64` data grid at fractional
//!     `(gx, gy)` corresponding to each output pixel,
//!   - binary-search lookup of the value into the colormap level table,
//!   - packed RGBA8 store to the output image buffer.
//!
//! The output is a `Vec<u8>` of length `4 * img_w * img_h` in `R,G,B,A` byte
//! order — drop-in compatible with `image::RgbaImage::from_raw`.

mod sources;

pub mod colormap;
pub mod contour_lines;
pub mod downsample;
pub mod linework;
pub mod polygon_fill;
pub mod raster_blit;
pub mod rasterize_grid;
pub mod rasterize_inverse_projected_grid;
pub mod rasterize_projected_grid;

/// Print accumulated per-phase timings + cache hit rate to stderr if
/// `RUSTWX_CUDA_RASTERIZE_TIMING=1` is set. Call at process exit.
pub fn print_phase_timing_if_enabled() {
    rasterize_projected_grid::print_timing_if_enabled();
    rasterize_inverse_projected_grid::print_timing_if_enabled();
    downsample::print_timing_if_enabled();
}

pub use colormap::{pack_rgba, ColormapHostView};
pub use rustwx_cuda_core as core;
pub use rustwx_cuda_core::{Context, ContextHandle, DeviceVec, Error, Result};
