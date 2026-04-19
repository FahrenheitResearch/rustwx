use crate::color::Rgba;
use image::RgbaImage;

pub fn blend_pixel(img: &mut RgbaImage, x: i32, y: i32, color: Rgba) {
    if x < 0 || y < 0 || (x as u32) >= img.width() || (y as u32) >= img.height() {
        return;
    }

    if color.a == 255 {
        img.put_pixel(x as u32, y as u32, color.to_image_rgba());
        return;
    }
    if color.a == 0 {
        return;
    }

    let dst = img.get_pixel(x as u32, y as u32).0;
    let alpha = color.a as f64 / 255.0;
    let inv = 1.0 - alpha;
    let blended = image::Rgba([
        (color.r as f64 * alpha + dst[0] as f64 * inv).round() as u8,
        (color.g as f64 * alpha + dst[1] as f64 * inv).round() as u8,
        (color.b as f64 * alpha + dst[2] as f64 * inv).round() as u8,
        255,
    ]);
    img.put_pixel(x as u32, y as u32, blended);
}

fn blend_pixel_coverage(img: &mut RgbaImage, x: i32, y: i32, color: Rgba, coverage: f64) {
    if coverage <= 0.0 || color.a == 0 {
        return;
    }
    let scaled_alpha = ((color.a as f64) * coverage.clamp(0.0, 1.0)).round() as u8;
    if scaled_alpha == 0 {
        return;
    }
    blend_pixel(
        img,
        x,
        y,
        Rgba {
            a: scaled_alpha,
            ..color
        },
    );
}

fn draw_disc(img: &mut RgbaImage, x: i32, y: i32, radius: i32, color: Rgba) {
    let r = radius.max(0);
    for dy in -r..=r {
        for dx in -r..=r {
            if dx * dx + dy * dy <= r * r {
                blend_pixel(img, x + dx, y + dy, color);
            }
        }
    }
}

pub fn draw_line(img: &mut RgbaImage, x0: f64, y0: f64, x1: f64, y1: f64, color: Rgba, width: u32) {
    let dx = x1 - x0;
    let dy = y1 - y0;
    let steps = dx.abs().max(dy.abs()).ceil() as usize;
    if steps == 0 {
        draw_disc(
            img,
            x0.round() as i32,
            y0.round() as i32,
            (width / 2) as i32,
            color,
        );
        return;
    }

    let radius = (width as i32 - 1) / 2;
    for step in 0..=steps {
        let t = step as f64 / steps as f64;
        let x = x0 + dx * t;
        let y = y0 + dy * t;
        draw_disc(img, x.round() as i32, y.round() as i32, radius, color);
    }
}

fn ipart(x: f64) -> i32 {
    x.floor() as i32
}

fn roundi(x: f64) -> i32 {
    (x + 0.5).floor() as i32
}

fn fpart(x: f64) -> f64 {
    x - x.floor()
}

fn rfpart(x: f64) -> f64 {
    1.0 - fpart(x)
}

fn scale_alpha(color: Rgba, factor: f64) -> Rgba {
    let alpha = ((color.a as f64) * factor.clamp(0.0, 1.0)).round() as u8;
    Rgba { a: alpha, ..color }
}

fn draw_offset_aa_stroke(
    img: &mut RgbaImage,
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
    color: Rgba,
    offset: f64,
    alpha_scale: f64,
) {
    let dx = x1 - x0;
    let dy = y1 - y0;
    let len = (dx * dx + dy * dy).sqrt();
    if len < 1e-6 {
        blend_pixel(img, x0.round() as i32, y0.round() as i32, scale_alpha(color, alpha_scale));
        return;
    }

    let perp_x = -dy / len;
    let perp_y = dx / len;
    let stroke = scale_alpha(color, alpha_scale);
    draw_line_aa(
        img,
        x0 - perp_x * offset,
        y0 - perp_y * offset,
        x1 - perp_x * offset,
        y1 - perp_y * offset,
        stroke,
    );
    draw_line_aa(
        img,
        x0 + perp_x * offset,
        y0 + perp_y * offset,
        x1 + perp_x * offset,
        y1 + perp_y * offset,
        stroke,
    );
}

pub fn draw_line_aa(
    img: &mut RgbaImage,
    mut x0: f64,
    mut y0: f64,
    mut x1: f64,
    mut y1: f64,
    color: Rgba,
) {
    if !x0.is_finite() || !y0.is_finite() || !x1.is_finite() || !y1.is_finite() {
        return;
    }

    if (x1 - x0).abs() < 1e-6 && (y1 - y0).abs() < 1e-6 {
        blend_pixel(img, x0.round() as i32, y0.round() as i32, color);
        return;
    }

    let steep = (y1 - y0).abs() > (x1 - x0).abs();
    if steep {
        std::mem::swap(&mut x0, &mut y0);
        std::mem::swap(&mut x1, &mut y1);
    }
    if x0 > x1 {
        std::mem::swap(&mut x0, &mut x1);
        std::mem::swap(&mut y0, &mut y1);
    }

    let dx = x1 - x0;
    let dy = y1 - y0;
    if dx.abs() < 1e-6 {
        draw_line(img, x0, y0, x1, y1, color, 1);
        return;
    }
    let gradient = dy / dx;

    let plot = |img: &mut RgbaImage, steep: bool, x: i32, y: i32, alpha: f64| {
        if steep {
            blend_pixel_coverage(img, y, x, color, alpha);
        } else {
            blend_pixel_coverage(img, x, y, color, alpha);
        }
    };

    let xend = roundi(x0) as f64;
    let yend = y0 + gradient * (xend - x0);
    let xgap = rfpart(x0 + 0.5);
    let xpxl1 = xend as i32;
    let ypxl1 = ipart(yend);
    plot(img, steep, xpxl1, ypxl1, rfpart(yend) * xgap);
    plot(img, steep, xpxl1, ypxl1 + 1, fpart(yend) * xgap);
    let mut intery = yend + gradient;

    let xend = roundi(x1) as f64;
    let yend = y1 + gradient * (xend - x1);
    let xgap = fpart(x1 + 0.5);
    let xpxl2 = xend as i32;
    let ypxl2 = ipart(yend);
    plot(img, steep, xpxl2, ypxl2, rfpart(yend) * xgap);
    plot(img, steep, xpxl2, ypxl2 + 1, fpart(yend) * xgap);

    for x in (xpxl1 + 1)..xpxl2 {
        let y = ipart(intery);
        plot(img, steep, x, y, rfpart(intery));
        plot(img, steep, x, y + 1, fpart(intery));
        intery += gradient;
    }
}

pub fn draw_line_aa_width(
    img: &mut RgbaImage,
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
    color: Rgba,
    width: u32,
) {
    match width {
        0 | 1 => {
            draw_line_aa(img, x0, y0, x1, y1, color);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 0.35, 0.28);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 0.75, 0.12);
        }
        2 => {
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 0.35, 0.78);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 0.85, 0.24);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 1.25, 0.10);
        }
        3 => {
            draw_line_aa(img, x0, y0, x1, y1, color);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 0.7, 0.74);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 1.25, 0.24);
            draw_offset_aa_stroke(img, x0, y0, x1, y1, color, 1.8, 0.10);
        }
        _ => draw_line(img, x0, y0, x1, y1, color, width),
    }
}

pub fn draw_polyline(img: &mut RgbaImage, points: &[(f64, f64)], color: Rgba, width: u32) {
    if points.len() < 2 {
        return;
    }
    for segment in points.windows(2) {
        let (x0, y0) = segment[0];
        let (x1, y1) = segment[1];
        draw_line(img, x0, y0, x1, y1, color, width);
    }
}

pub fn draw_polyline_aa(img: &mut RgbaImage, points: &[(f64, f64)], color: Rgba, width: u32) {
    if points.len() < 2 {
        return;
    }
    for segment in points.windows(2) {
        let (x0, y0) = segment[0];
        let (x1, y1) = segment[1];
        draw_line_aa_width(img, x0, y0, x1, y1, color, width);
    }
}

/// Fill a polygon (optionally with holes) using an even-odd scanline rule.
///
/// `rings` is a list of closed rings in pixel coordinates — the first ring is
/// the outer boundary, any additional rings punch holes out of it. Rings are
/// auto-closed (the last vertex does not need to repeat the first).
///
/// Clipped to the image bounds. Uses alpha-blended pixel writes so fills with
/// partial alpha composite correctly over existing pixels.
/// Scanline-fills `rings` with even-odd winding, clipped to `clip` (inclusive
/// pixel rect: `(x0, y0, x1, y1)`) when supplied, otherwise clipped to the full
/// image. Callers drawing into a map panel must pass the panel rect so that
/// global polygons (world oceans / continents) don't bleed into the margins.
pub fn fill_polygon(
    img: &mut RgbaImage,
    rings: &[Vec<(f64, f64)>],
    color: Rgba,
    clip: Option<(i32, i32, i32, i32)>,
) {
    if rings.is_empty() || color.a == 0 {
        return;
    }

    let img_w = img.width() as i32;
    let img_h = img.height() as i32;
    let (cx0, cy0, cx1, cy1) = match clip {
        Some((x0, y0, x1, y1)) => (
            x0.max(0),
            y0.max(0),
            x1.min(img_w - 1),
            y1.min(img_h - 1),
        ),
        None => (0, 0, img_w - 1, img_h - 1),
    };
    if cx1 < cx0 || cy1 < cy0 {
        return;
    }

    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    for ring in rings {
        for &(_, y) in ring {
            if y.is_finite() {
                y_min = y_min.min(y);
                y_max = y_max.max(y);
            }
        }
    }
    if !y_min.is_finite() || !y_max.is_finite() || y_max < cy0 as f64 {
        return;
    }
    let y0 = y_min.floor().max(cy0 as f64) as i32;
    let y1 = (y_max.ceil() as i32).min(cy1);
    if y1 < y0 {
        return;
    }

    // Pre-extract edges once. Storing (y_min, y_max, x_at_y_min, dx_per_dy)
    // lets the scanline loop skip edges that don't span it.
    #[derive(Clone)]
    struct Edge {
        y_min: f64,
        y_max: f64,
        x: f64,
        dx: f64,
    }
    let mut edges: Vec<Edge> = Vec::new();
    for ring in rings {
        let n = ring.len();
        if n < 2 {
            continue;
        }
        for i in 0..n {
            let (ax, ay) = ring[i];
            let (bx, by) = ring[(i + 1) % n];
            if !ax.is_finite() || !ay.is_finite() || !bx.is_finite() || !by.is_finite() {
                continue;
            }
            if (ay - by).abs() < 1e-9 {
                continue; // horizontal edges contribute nothing to even-odd
            }
            let (lo_y, hi_y, lo_x, hi_x) = if ay < by {
                (ay, by, ax, bx)
            } else {
                (by, ay, bx, ax)
            };
            let dx = (hi_x - lo_x) / (hi_y - lo_y);
            edges.push(Edge {
                y_min: lo_y,
                y_max: hi_y,
                x: lo_x,
                dx,
            });
        }
    }
    if edges.is_empty() {
        return;
    }

    // Scanline loop. At pixel center (y + 0.5), collect edge x-intersections
    // for edges that straddle the scanline (using half-open [y_min, y_max)
    // avoids double-counting shared endpoints).
    let mut xs: Vec<f64> = Vec::with_capacity(edges.len());
    for y in y0..=y1 {
        let yf = y as f64 + 0.5;
        xs.clear();
        for edge in &edges {
            if yf >= edge.y_min && yf < edge.y_max {
                xs.push(edge.x + (yf - edge.y_min) * edge.dx);
            }
        }
        if xs.len() < 2 {
            continue;
        }
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mut i = 0;
        while i + 1 < xs.len() {
            let xa = xs[i].max(cx0 as f64).ceil() as i32;
            let xb = xs[i + 1].min(cx1 as f64).floor() as i32;
            if xb >= xa {
                for x in xa..=xb {
                    blend_pixel(img, x, y, color);
                }
            }
            i += 2;
        }
    }
}

pub fn draw_wind_barb(
    img: &mut RgbaImage,
    x_tip: f64,
    y_tip: f64,
    u: f64,
    v: f64,
    color: Rgba,
    shaft_len: f64,
    width: u32,
) {
    if !u.is_finite() || !v.is_finite() {
        return;
    }

    let speed = (u * u + v * v).sqrt();
    if speed < 2.5 {
        draw_disc(img, x_tip.round() as i32, y_tip.round() as i32, 2, color);
        return;
    }

    // Screen-space unit vector from barb tip toward the tail.
    let tail_dx = -u / speed;
    let tail_dy = v / speed;
    // Matplotlib's default barb side lands on the counterclockwise
    // perpendicular in screen space for the tip-anchored shaft.
    let perp_dx = -tail_dy;
    let perp_dy = tail_dx;

    let tail_x = x_tip + tail_dx * shaft_len;
    let tail_y = y_tip + tail_dy * shaft_len;
    draw_line(img, tail_x, tail_y, x_tip, y_tip, color, width);

    let mut remaining = ((speed + 2.5) / 5.0).floor() as i32 * 5;
    let mut offset = shaft_len;
    let spacing = (shaft_len * 0.16).max(2.0);
    let full_height = shaft_len * 0.40;
    let full_width = shaft_len * 0.25;

    while remaining >= 50 {
        draw_barb_flag(
            img,
            x_tip,
            y_tip,
            tail_dx,
            tail_dy,
            perp_dx,
            perp_dy,
            offset,
            full_height,
            full_width,
            color,
            width,
        );
        remaining -= 50;
        offset -= full_width + spacing;
    }

    while remaining >= 10 {
        draw_barb_segment(
            img,
            x_tip,
            y_tip,
            tail_dx,
            tail_dy,
            perp_dx,
            perp_dy,
            offset,
            full_height,
            full_width * 0.5,
            color,
            width,
        );
        remaining -= 10;
        offset -= spacing;
    }

    if remaining >= 5 {
        if (offset - shaft_len).abs() < 1e-6 {
            offset -= 1.5 * spacing;
        }
        draw_barb_segment(
            img,
            x_tip,
            y_tip,
            tail_dx,
            tail_dy,
            perp_dx,
            perp_dy,
            offset,
            full_height * 0.5,
            full_width * 0.25,
            color,
            width,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anti_aliased_line_blends_neighbor_pixels() {
        let mut img = RgbaImage::from_pixel(8, 8, image::Rgba([255, 255, 255, 255]));
        draw_line_aa(&mut img, 1.0, 1.0, 6.0, 4.0, Rgba::BLACK);

        let mut blended_neighbor_found = false;
        for pixel in img.pixels() {
            let rgb = &pixel.0[..3];
            if *rgb != [255, 255, 255] && *rgb != [0, 0, 0] {
                blended_neighbor_found = true;
                break;
            }
        }

        assert!(blended_neighbor_found);
    }
}

fn draw_barb_segment(
    img: &mut RgbaImage,
    x_tip: f64,
    y_tip: f64,
    tail_dx: f64,
    tail_dy: f64,
    perp_dx: f64,
    perp_dy: f64,
    offset: f64,
    height: f64,
    along_tail: f64,
    color: Rgba,
    width: u32,
) {
    let base_x = x_tip + tail_dx * offset;
    let base_y = y_tip + tail_dy * offset;
    let feather_x = base_x + perp_dx * height + tail_dx * along_tail;
    let feather_y = base_y + perp_dy * height + tail_dy * along_tail;
    draw_line(img, base_x, base_y, feather_x, feather_y, color, width);
}

fn draw_barb_flag(
    img: &mut RgbaImage,
    x_tip: f64,
    y_tip: f64,
    tail_dx: f64,
    tail_dy: f64,
    perp_dx: f64,
    perp_dy: f64,
    offset: f64,
    height: f64,
    width_along: f64,
    color: Rgba,
    width: u32,
) {
    let base_x = x_tip + tail_dx * offset;
    let base_y = y_tip + tail_dy * offset;
    let flag_tip_x = base_x + perp_dx * height - tail_dx * (width_along * 0.5);
    let flag_tip_y = base_y + perp_dy * height - tail_dy * (width_along * 0.5);
    let flag_tail_x = base_x - tail_dx * width_along;
    let flag_tail_y = base_y - tail_dy * width_along;
    draw_line(
        img,
        base_x,
        base_y,
        flag_tip_x,
        flag_tip_y,
        color,
        width + 1,
    );
    draw_line(
        img,
        flag_tip_x,
        flag_tip_y,
        flag_tail_x,
        flag_tail_y,
        color,
        width + 1,
    );
    draw_line(
        img,
        flag_tail_x,
        flag_tail_y,
        base_x,
        base_y,
        color,
        width + 1,
    );
}
