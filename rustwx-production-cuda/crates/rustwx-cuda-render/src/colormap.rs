//! FFI-shaped view of a rustwx-render `LeveledColormap` for GPU upload.
//!
//! The kernel does not need the legend tables or any render-density bits —
//! only the discrete `levels` boundaries, the per-interval `colors`,
//! optional under/over fallbacks, and the optional `mask_below` cutoff.

/// Pack an `(r, g, b, a)` byte tuple into a little-endian u32 such that
/// reinterpreting the resulting buffer as `[u8]` produces `R, G, B, A`
/// per-pixel — matches `image::Rgba` byte layout.
#[inline]
pub fn pack_rgba(r: u8, g: u8, b: u8, a: u8) -> u32 {
    (r as u32) | ((g as u32) << 8) | ((b as u32) << 16) | ((a as u32) << 24)
}

/// Borrowed view of a rustwx-render colormap, ready to upload to the device.
///
/// `colors_packed.len() == levels.len().saturating_sub(1)`: one color per
/// interval `[levels[i], levels[i+1])`. `under_color` / `over_color` are
/// fallbacks for values below `levels[0]` or at/above `levels.last()`.
#[derive(Clone, Copy)]
pub struct ColormapHostView<'a> {
    pub levels: &'a [f64],
    pub colors_packed: &'a [u32],
    pub under_color: Option<u32>,
    pub over_color: Option<u32>,
    pub mask_below: Option<f64>,
}
