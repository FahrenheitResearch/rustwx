use crate::colormap::LeveledColormap;
use image::RgbaImage;

/// Rasterize a 2D grid into an RGBA image using bilinear sampling.
///
/// `data` is row-major `[ny][nx]`. The image maps grid row 0 to the
/// bottom of the image (geographic convention: south at bottom).
pub fn rasterize_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    let mut img = RgbaImage::new(img_w, img_h);

    if ny == 0 || nx == 0 {
        return img;
    }

    let x_den = img_w.saturating_sub(1).max(1) as f64;
    let y_den = img_h.saturating_sub(1).max(1) as f64;
    let gx_den = nx.saturating_sub(1).max(1) as f64;
    let gy_den = ny.saturating_sub(1).max(1) as f64;

    for py in 0..img_h {
        for px in 0..img_w {
            let gx = px as f64 / x_den * gx_den;
            let gy = (img_h.saturating_sub(1) - py) as f64 / y_den * gy_den;

            let i0 = gx.floor() as usize;
            let j0 = gy.floor() as usize;
            let i1 = (i0 + 1).min(nx - 1);
            let j1 = (j0 + 1).min(ny - 1);
            let fx = gx - i0 as f64;
            let fy = gy - j0 as f64;

            let v00 = data[j0 * nx + i0];
            let v10 = data[j0 * nx + i1];
            let v01 = data[j1 * nx + i0];
            let v11 = data[j1 * nx + i1];

            let value = bilinear(v00, v10, v01, v11, fx, fy);
            let color = cmap.map(value);
            img.put_pixel(px, py, color.to_image_rgba());
        }
    }

    img
}

/// Rasterize a 2D grid on a projected mesh.
///
/// `pixel_points` contains local image coordinates in map space, one per grid
/// point, or `None` when the projected point falls outside the valid extent.
pub fn rasterize_projected_grid(
    data: &[f64],
    ny: usize,
    nx: usize,
    pixel_points: &[Option<(f64, f64)>],
    cmap: &LeveledColormap,
    img_w: u32,
    img_h: u32,
) -> RgbaImage {
    let mut img = RgbaImage::new(img_w, img_h);

    if ny < 2 || nx < 2 || pixel_points.len() != ny * nx {
        return img;
    }

    for j in 0..(ny - 1) {
        for i in 0..(nx - 1) {
            let idx = |jj: usize, ii: usize| jj * nx + ii;
            let p00 = pixel_points[idx(j, i)];
            let p10 = pixel_points[idx(j, i + 1)];
            let p01 = pixel_points[idx(j + 1, i)];
            let p11 = pixel_points[idx(j + 1, i + 1)];

            let (p00, p10, p01, p11) = match (p00, p10, p01, p11) {
                (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
                _ => continue,
            };

            let v00 = data[idx(j, i)];
            let v10 = data[idx(j, i + 1)];
            let v01 = data[idx(j + 1, i)];
            let v11 = data[idx(j + 1, i + 1)];

            rasterize_triangle(&mut img, p00, v00, p10, v10, p11, v11, cmap);
            rasterize_triangle(&mut img, p00, v00, p11, v11, p01, v01, cmap);
        }
    }

    img
}

fn bilinear(v00: f64, v10: f64, v01: f64, v11: f64, fx: f64, fy: f64) -> f64 {
    if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
        let south = v00 * (1.0 - fx) + v10 * fx;
        let north = v01 * (1.0 - fx) + v11 * fx;
        south * (1.0 - fy) + north * fy
    } else {
        for value in [v00, v10, v01, v11] {
            if value.is_finite() {
                return value;
            }
        }
        f64::NAN
    }
}

fn rasterize_triangle(
    img: &mut RgbaImage,
    p0: (f64, f64),
    v0: f64,
    p1: (f64, f64),
    v1: f64,
    p2: (f64, f64),
    v2: f64,
    cmap: &LeveledColormap,
) {
    let min_x = p0.0.min(p1.0).min(p2.0).floor().max(0.0) as i32;
    let max_x =
        p0.0.max(p1.0)
            .max(p2.0)
            .ceil()
            .min(img.width() as f64 - 1.0) as i32;
    let min_y = p0.1.min(p1.1).min(p2.1).floor().max(0.0) as i32;
    let max_y =
        p0.1.max(p1.1)
            .max(p2.1)
            .ceil()
            .min(img.height() as f64 - 1.0) as i32;

    if min_x > max_x || min_y > max_y {
        return;
    }

    let area = edge_fn(p0, p1, p2);
    if area.abs() < 1e-9 {
        return;
    }

    let inv_area = 1.0 / area;
    let fallback = nearest_finite(&[(p0, v0), (p1, v1), (p2, v2)]);

    for py in min_y..=max_y {
        for px in min_x..=max_x {
            let p = (px as f64 + 0.5, py as f64 + 0.5);
            let w0 = edge_fn(p1, p2, p) * inv_area;
            let w1 = edge_fn(p2, p0, p) * inv_area;
            let w2 = edge_fn(p0, p1, p) * inv_area;

            if w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6 {
                continue;
            }

            let value = if v0.is_finite() && v1.is_finite() && v2.is_finite() {
                v0 * w0 + v1 * w1 + v2 * w2
            } else {
                fallback.unwrap_or(f64::NAN)
            };
            let color = cmap.map(value).to_image_rgba();
            if color.0[3] > 0 {
                img.put_pixel(px as u32, py as u32, color);
            }
        }
    }
}

fn edge_fn(a: (f64, f64), b: (f64, f64), p: (f64, f64)) -> f64 {
    (p.0 - a.0) * (b.1 - a.1) - (p.1 - a.1) * (b.0 - a.0)
}

fn nearest_finite(points: &[((f64, f64), f64)]) -> Option<f64> {
    points.iter().find_map(|(_, v)| v.is_finite().then_some(*v))
}
