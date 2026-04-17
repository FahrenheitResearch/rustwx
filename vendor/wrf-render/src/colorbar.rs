use crate::color::Rgba;
use crate::colormap::LeveledColormap;
use image::RgbaImage;

/// Thin cool-gray frame for the colorbar — reads as a subtle divider rather
/// than a hard black rule. Matches the "modern" look where the colorbar's
/// color swatches are the main signal and chrome recedes.
const COLORBAR_FRAME: Rgba = Rgba {
    r: 90,
    g: 96,
    b: 108,
    a: 255,
};

/// Hairline separator between adjacent color swatches. Near-transparent so it
/// only hints at the boundary — the user still sees swatches-as-blocks, not a
/// striped grid.
const COLORBAR_DIVIDER: Rgba = Rgba {
    r: 255,
    g: 255,
    b: 255,
    a: 70,
};

/// Draw a horizontal colorbar onto an existing image.
///
/// Fills the rectangle `(x, y, x+width, y+height)` with colour swatches
/// matching the levels in the colormap.  Each interval gets an equal-width
/// swatch.
pub fn draw_colorbar(
    img: &mut RgbaImage,
    cmap: &LeveledColormap,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
) {
    let n_intervals = if cmap.levels.len() > 1 {
        cmap.levels.len() - 1
    } else {
        return;
    };

    for px in x..x.saturating_add(width).min(img.width()) {
        let rel = (px - x) as f64 / width as f64;
        let interval = (rel * n_intervals as f64) as usize;
        let interval = interval.min(n_intervals - 1);
        let color = if interval < cmap.colors.len() {
            cmap.colors[interval]
        } else {
            Rgba::TRANSPARENT
        };
        for py in y..y.saturating_add(height).min(img.height()) {
            img.put_pixel(px, py, color.to_image_rgba());
        }
    }

    let x_end = (x + width).min(img.width());
    let y_end = (y + height).min(img.height());

    // Hairline separators between swatches — light, partial alpha so they
    // only suggest boundaries instead of chopping the bar into stripes.
    let divider = COLORBAR_DIVIDER.to_image_rgba();
    for i in 1..n_intervals {
        let tick_x = x + (i as u32 * width / n_intervals as u32);
        if tick_x < img.width() {
            for py in (y + 1)..y_end.saturating_sub(1) {
                // Alpha-composite onto the existing swatch so the divider
                // takes on a paler version of the underlying color.
                let dst = img.get_pixel(tick_x, py).0;
                let a = COLORBAR_DIVIDER.a as f64 / 255.0;
                let inv = 1.0 - a;
                let blended = image::Rgba([
                    (COLORBAR_DIVIDER.r as f64 * a + dst[0] as f64 * inv).round() as u8,
                    (COLORBAR_DIVIDER.g as f64 * a + dst[1] as f64 * inv).round() as u8,
                    (COLORBAR_DIVIDER.b as f64 * a + dst[2] as f64 * inv).round() as u8,
                    255,
                ]);
                img.put_pixel(tick_x, py, blended);
            }
        }
    }
    let _ = divider;

    // Thin cool-gray outer frame — one pixel, muted slate instead of solid black.
    let frame = COLORBAR_FRAME.to_image_rgba();
    for px in x..x_end {
        img.put_pixel(px, y, frame);
        if y_end > 0 {
            img.put_pixel(px, y_end - 1, frame);
        }
    }
    for py in y..y_end {
        img.put_pixel(x, py, frame);
        if x_end > 0 {
            img.put_pixel(x_end - 1, py, frame);
        }
    }
}

/// Draw short tick marks at specified relative positions (0..1) hanging above
/// the colorbar. Callers own the label placement; this just draws the line.
pub fn draw_colorbar_ticks(
    img: &mut RgbaImage,
    cbar_x: u32,
    cbar_y: u32,
    cbar_width: u32,
    positions: &[f64],
) {
    let frame = COLORBAR_FRAME.to_image_rgba();
    if cbar_y < 4 {
        return;
    }
    for &frac in positions {
        if !(0.0..=1.0).contains(&frac) {
            continue;
        }
        let px = cbar_x + (frac * cbar_width as f64).round() as u32;
        if px >= img.width() {
            continue;
        }
        for dy in 1..=3 {
            let py = cbar_y.saturating_sub(dy);
            if py < img.height() {
                img.put_pixel(px, py, frame);
            }
        }
    }
}
