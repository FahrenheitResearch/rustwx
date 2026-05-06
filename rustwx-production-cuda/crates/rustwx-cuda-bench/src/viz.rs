//! Render a `Vec<f64>` field as a PNG with a linear viridis-ish colormap.
//! Used by the benchmark to write maps under `bench_output/` so a human
//! can eyeball outputs and sanity-check the kernels.

use std::path::Path;

use image::{ImageBuffer, Rgb, RgbImage};

/// Save a 2D field as a PNG. NaN values render as black.
pub fn save_png<P: AsRef<Path>>(
    path: P,
    field: &[f64],
    nx: usize,
    ny: usize,
) -> std::io::Result<()> {
    let n = nx * ny;
    assert_eq!(field.len(), n, "field length must equal nx*ny");

    // Robust min/max ignoring NaN.
    let (vmin, vmax) = field
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .fold((f64::INFINITY, f64::NEG_INFINITY), |acc, v| {
            (acc.0.min(v), acc.1.max(v))
        });
    let range = (vmax - vmin).max(1e-12);

    let mut img: RgbImage = ImageBuffer::new(nx as u32, ny as u32);

    for j in 0..ny {
        // PNG y axis is top-down; meteorological grids are typically S→N.
        // Flip vertically so north shows up at the top of the image.
        let img_j = (ny - 1 - j) as u32;
        for i in 0..nx {
            let v = field[j * nx + i];
            let rgb = if v.is_finite() {
                viridis(((v - vmin) / range).clamp(0.0, 1.0))
            } else {
                Rgb([0, 0, 0])
            };
            img.put_pixel(i as u32, img_j, rgb);
        }
    }

    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }
    img.save(path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

/// 5-stop viridis-like map. Cheap to compute, perceptually reasonable.
fn viridis(t: f64) -> Rgb<u8> {
    // (t, r, g, b) stops sampled from matplotlib's viridis.
    const STOPS: [(f64, f64, f64, f64); 5] = [
        (0.00, 68.0, 1.0, 84.0),
        (0.25, 59.0, 82.0, 139.0),
        (0.50, 33.0, 144.0, 141.0),
        (0.75, 93.0, 201.0, 99.0),
        (1.00, 253.0, 231.0, 37.0),
    ];
    let mut i = 0;
    while i + 1 < STOPS.len() && t > STOPS[i + 1].0 {
        i += 1;
    }
    let (t0, r0, g0, b0) = STOPS[i];
    let (t1, r1, g1, b1) = STOPS[(i + 1).min(STOPS.len() - 1)];
    let f = if (t1 - t0).abs() < 1e-12 {
        0.0
    } else {
        (t - t0) / (t1 - t0)
    };
    Rgb([
        (r0 + f * (r1 - r0)).round().clamp(0.0, 255.0) as u8,
        (g0 + f * (g1 - g0)).round().clamp(0.0, 255.0) as u8,
        (b0 + f * (b1 - b0)).round().clamp(0.0, 255.0) as u8,
    ])
}
