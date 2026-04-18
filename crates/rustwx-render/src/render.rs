use crate::color::Rgba;
use crate::colorbar;
use crate::colormap::LeveledColormap;
use crate::draw;
use crate::overlay::{
    BarbOverlay, ContourOverlay, MapExtent, ProjectedGrid, ProjectedPolygon, ProjectedPolyline,
};
use crate::presentation::{ProductVisualMode, RenderPresentation, TitleAnchor};
use crate::rasterize;
use crate::text;
use image::RgbaImage;
use std::cell::RefCell;
use std::io::Cursor;
use std::sync::Arc;

#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::sync::Mutex;

/// Full render configuration.
pub struct RenderOpts {
    pub width: u32,
    pub height: u32,
    pub cmap: LeveledColormap,
    pub background: Rgba,
    pub colorbar: bool,
    pub title: Option<String>,
    pub subtitle_left: Option<String>,
    pub subtitle_right: Option<String>,
    pub cbar_tick_step: Option<f64>,
    pub map_extent: Option<MapExtent>,
    pub projected_grid: Option<ProjectedGrid>,
    /// Filled polygons (lat/lon-derived). Drawn BEFORE the data raster so the
    /// data overlays on top; ordering within the list is bottom-to-top.
    /// Typical stack: ocean → land → lakes.
    pub projected_polygons: Vec<ProjectedPolygon>,
    pub projected_lines: Vec<ProjectedPolyline>,
    pub contours: Vec<ContourOverlay>,
    pub barbs: Vec<BarbOverlay>,
    pub presentation: RenderPresentation,
}

impl Default for RenderOpts {
    fn default() -> Self {
        Self {
            width: 1100,
            height: 850,
            cmap: LeveledColormap {
                levels: vec![],
                colors: vec![],
                under_color: None,
                over_color: None,
                mask_below: None,
            },
            background: Rgba::WHITE,
            colorbar: true,
            title: None,
            subtitle_left: None,
            subtitle_right: None,
            cbar_tick_step: None,
            map_extent: None,
            projected_grid: None,
            projected_polygons: vec![],
            projected_lines: vec![],
            contours: vec![],
            barbs: vec![],
            presentation: RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
        }
    }
}

struct Layout {
    map_x: u32,
    map_y: u32,
    map_w: u32,
    map_h: u32,
    cbar_x: u32,
    cbar_y: u32,
    cbar_w: u32,
    cbar_h: u32,
    title_y: u32,
    subtitle_y: u32,
}

#[derive(Clone)]
struct CachedProjectedPixels {
    grid_x: Vec<f64>,
    grid_y: Vec<f64>,
    nx: usize,
    ny: usize,
    map_w: u32,
    map_h: u32,
    extent_bits: [u64; 4],
    pixels: Arc<[Option<(f64, f64)>]>,
}

impl CachedProjectedPixels {
    fn new(
        grid: &ProjectedGrid,
        extent: &MapExtent,
        layout: &Layout,
        pixels: Arc<[Option<(f64, f64)>]>,
    ) -> Self {
        Self {
            grid_x: grid.x.clone(),
            grid_y: grid.y.clone(),
            nx: grid.nx,
            ny: grid.ny,
            map_w: layout.map_w,
            map_h: layout.map_h,
            extent_bits: extent_bits(extent),
            pixels,
        }
    }

    fn matches(&self, grid: &ProjectedGrid, extent: &MapExtent, layout: &Layout) -> bool {
        self.nx == grid.nx
            && self.ny == grid.ny
            && self.map_w == layout.map_w
            && self.map_h == layout.map_h
            && self.extent_bits == extent_bits(extent)
            && self.grid_x == grid.x
            && self.grid_y == grid.y
    }
}

thread_local! {
    static PROJECTED_PIXEL_CACHE: RefCell<Option<CachedProjectedPixels>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static PROJECTED_PIXEL_CACHE_MISSES: Cell<usize> = const { Cell::new(0) };
}
#[cfg(test)]
static PROJECTED_PIXEL_CACHE_TEST_LOCK: Mutex<()> = Mutex::new(());

fn compute_layout(
    total_w: u32,
    total_h: u32,
    has_cbar: bool,
    has_title: bool,
    presentation: RenderPresentation,
) -> Layout {
    let metrics = presentation.layout;
    let map_x = metrics.margin_x.min(total_w.saturating_sub(1));
    let title_h = if has_title { metrics.title_h } else { 0 };
    let footer_h = if has_cbar {
        metrics
            .footer_h
            .max(metrics.colorbar_h + metrics.colorbar_gap + 10)
    } else {
        metrics.footer_h.min(18)
    };
    let map_y = title_h.min(total_h.saturating_sub(1));
    let map_w = total_w.saturating_sub(map_x.saturating_mul(2)).max(1);
    let map_h = total_h
        .saturating_sub(map_y)
        .saturating_sub(footer_h)
        .max(1);
    let cbar_h = if has_cbar {
        metrics.colorbar_h.max(8)
    } else {
        0
    };
    let cbar_x = if has_cbar {
        metrics.colorbar_margin_x.min(total_w.saturating_sub(1))
    } else {
        0
    };
    let cbar_w = if has_cbar {
        total_w.saturating_sub(cbar_x.saturating_mul(2)).max(
            total_w
                .saturating_sub(metrics.margin_x.saturating_mul(2))
                .max(1),
        )
    } else {
        0
    };
    let cbar_y = if has_cbar {
        total_h
            .saturating_sub(metrics.colorbar_gap)
            .saturating_sub(cbar_h)
            .max(map_y + map_h)
    } else {
        0
    };

    Layout {
        map_x,
        map_y,
        map_w,
        map_h,
        cbar_x,
        cbar_y,
        cbar_w,
        cbar_h,
        title_y: 2,
        subtitle_y: title_h.saturating_sub(18),
    }
}

pub fn map_frame_aspect_ratio(total_w: u32, total_h: u32, has_cbar: bool, has_title: bool) -> f64 {
    map_frame_aspect_ratio_for_mode(
        ProductVisualMode::FilledMeteorology,
        total_w,
        total_h,
        has_cbar,
        has_title,
    )
}

pub fn map_frame_aspect_ratio_for_mode(
    mode: ProductVisualMode,
    total_w: u32,
    total_h: u32,
    has_cbar: bool,
    has_title: bool,
) -> f64 {
    let layout = compute_layout(
        total_w,
        total_h,
        has_cbar,
        has_title,
        RenderPresentation::for_mode(mode),
    );
    layout.map_w as f64 / (layout.map_h.max(1) as f64)
}

fn pick_ticks(levels: &[f64], step: Option<f64>) -> Vec<f64> {
    if levels.is_empty() {
        return vec![];
    }
    let lo = levels[0];
    let hi = levels[levels.len() - 1];

    if let Some(s) = step {
        let mut ticks = Vec::new();
        let mut v = lo;
        while v <= hi + s * 0.01 {
            ticks.push(v);
            v += s;
        }
        return ticks;
    }

    let range = hi - lo;
    if range <= 0.0 {
        return vec![lo];
    }
    let raw_step = range / 10.0;
    let mag = 10.0_f64.powf(raw_step.log10().floor());
    let nice = if raw_step / mag < 1.5 {
        mag
    } else if raw_step / mag < 3.5 {
        2.0 * mag
    } else if raw_step / mag < 7.5 {
        5.0 * mag
    } else {
        10.0 * mag
    };

    let mut ticks = Vec::new();
    let start = (lo / nice).ceil() * nice;
    let mut v = start;
    while v <= hi + nice * 0.01 {
        ticks.push(v);
        v += nice;
    }
    ticks
}

fn grid_to_pixel(i: f64, j: f64, nx: usize, ny: usize, layout: &Layout) -> (f64, f64) {
    let x = layout.map_x as f64
        + i / (nx.saturating_sub(1).max(1)) as f64 * (layout.map_w.saturating_sub(1)) as f64;
    let y = layout.map_y as f64
        + (1.0 - j / (ny.saturating_sub(1).max(1)) as f64)
            * (layout.map_h.saturating_sub(1)) as f64;
    (x, y)
}

fn mask_contains_local_pixel(mask: &RgbaImage, x: f64, y: f64) -> bool {
    if !x.is_finite() || !y.is_finite() {
        return false;
    }
    let px = x.round() as i32;
    let py = y.round() as i32;
    if px < 0 || py < 0 || px >= mask.width() as i32 || py >= mask.height() as i32 {
        return false;
    }
    mask.get_pixel(px as u32, py as u32).0[3] > 0
}

fn segment_intersects_mask(mask: &RgbaImage, x0: f64, y0: f64, x1: f64, y1: f64) -> bool {
    const SAMPLE_STEPS: [f64; 5] = [0.0, 0.25, 0.5, 0.75, 1.0];
    SAMPLE_STEPS.iter().any(|t| {
        let x = x0 + (x1 - x0) * t;
        let y = y0 + (y1 - y0) * t;
        mask_contains_local_pixel(mask, x, y)
    })
}

fn project_ring_unclipped(
    extent: &MapExtent,
    ring: &[(f64, f64)],
    layout: &Layout,
) -> Vec<(f64, f64)> {
    let dx = extent.x_max - extent.x_min;
    let dy = extent.y_max - extent.y_min;
    if dx.abs() < 1e-12 || dy.abs() < 1e-12 {
        return Vec::new();
    }
    let w = layout.map_w.saturating_sub(1) as f64;
    let h = layout.map_h.saturating_sub(1) as f64;
    ring.iter()
        .map(|&(x, y)| {
            let rx = (x - extent.x_min) / dx;
            let ry = 1.0 - (y - extent.y_min) / dy;
            (layout.map_x as f64 + rx * w, layout.map_y as f64 + ry * h)
        })
        .collect()
}

fn draw_projected_polygons(
    img: &mut RgbaImage,
    layout: &Layout,
    extent: &MapExtent,
    polygons: &[ProjectedPolygon],
    presentation: RenderPresentation,
) {
    // Polygon rings are projected without clipping (clipping the ring geometry
    // would need polygon-polygon intersection); instead we clip the scanline
    // fill to the map panel so global polygons (world oceans, continents)
    // can't paint outside the map rectangle.
    let map_right = layout.map_x.saturating_add(layout.map_w).saturating_sub(1) as i32;
    let map_bottom = layout.map_y.saturating_add(layout.map_h).saturating_sub(1) as i32;
    let clip = Some((
        layout.map_x as i32,
        layout.map_y as i32,
        map_right,
        map_bottom,
    ));

    for poly in polygons {
        if poly.rings.is_empty() {
            continue;
        }
        let style = presentation.polygon_style(poly.role, poly.color);
        if !style.visible {
            continue;
        }
        let rings: Vec<Vec<(f64, f64)>> = poly
            .rings
            .iter()
            .map(|ring| project_ring_unclipped(extent, ring, layout))
            .collect();
        draw::fill_polygon(img, &rings, style.color, clip);
    }
}

fn draw_projected_lines(
    img: &mut RgbaImage,
    layout: &Layout,
    extent: &MapExtent,
    lines: &[ProjectedPolyline],
    presentation: RenderPresentation,
) {
    for line in lines {
        let style = presentation.linework_style(line.role, line.color, line.width);
        if !style.visible {
            continue;
        }
        let mut current = Vec::with_capacity(line.points.len());
        for &(x, y) in &line.points {
            if let Some((px, py)) = extent.to_pixel(x, y, layout.map_w, layout.map_h) {
                current.push((layout.map_x as f64 + px, layout.map_y as f64 + py));
            } else if current.len() >= 2 {
                draw::draw_polyline(img, &current, style.color, style.width);
                current.clear();
            } else {
                current.clear();
            }
        }
        if current.len() >= 2 {
            draw::draw_polyline(img, &current, style.color, style.width);
        }
    }
}

fn projected_grid_to_pixels(
    grid: &ProjectedGrid,
    extent: &MapExtent,
    layout: &Layout,
) -> Vec<Option<(f64, f64)>> {
    grid.x
        .iter()
        .zip(grid.y.iter())
        .map(|(&x, &y)| {
            extent
                .to_pixel(x, y, layout.map_w, layout.map_h)
                .and_then(|(px, py)| {
                    if (0.0..layout.map_w as f64).contains(&px)
                        && (0.0..layout.map_h as f64).contains(&py)
                    {
                        Some((px, py))
                    } else {
                        None
                    }
                })
        })
        .collect()
}

fn projected_grid_to_pixels_cached(
    grid: &ProjectedGrid,
    extent: &MapExtent,
    layout: &Layout,
) -> Arc<[Option<(f64, f64)>]> {
    PROJECTED_PIXEL_CACHE.with(|cache_cell| {
        let mut cache = cache_cell.borrow_mut();
        if let Some(cached) = cache.as_ref() {
            if cached.matches(grid, extent, layout) {
                return Arc::clone(&cached.pixels);
            }
        }

        let pixels: Arc<[Option<(f64, f64)>]> =
            projected_grid_to_pixels(grid, extent, layout).into();
        *cache = Some(CachedProjectedPixels::new(
            grid,
            extent,
            layout,
            Arc::clone(&pixels),
        ));
        #[cfg(test)]
        PROJECTED_PIXEL_CACHE_MISSES.with(|count| count.set(count.get() + 1));
        pixels
    })
}

fn extent_bits(extent: &MapExtent) -> [u64; 4] {
    [
        extent.x_min.to_bits(),
        extent.x_max.to_bits(),
        extent.y_min.to_bits(),
        extent.y_max.to_bits(),
    ]
}

fn interp_point(a: (f64, f64, f64), b: (f64, f64, f64), level: f64) -> Option<(f64, f64)> {
    let (x0, y0, v0) = a;
    let (x1, y1, v1) = b;
    if !v0.is_finite() || !v1.is_finite() {
        return None;
    }
    let d0 = v0 - level;
    let d1 = v1 - level;
    if (d0 > 0.0 && d1 > 0.0) || (d0 < 0.0 && d1 < 0.0) {
        return None;
    }
    if (v1 - v0).abs() < 1e-12 {
        return Some(((x0 + x1) * 0.5, (y0 + y1) * 0.5));
    }
    let t = (level - v0) / (v1 - v0);
    Some((x0 + (x1 - x0) * t, y0 + (y1 - y0) * t))
}

fn draw_contours(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    clip_mask: Option<&RgbaImage>,
) {
    if overlay.nx < 2 || overlay.ny < 2 {
        return;
    }

    for &level in &overlay.levels {
        let mut label_drawn = !overlay.labels;
        for j in 0..(overlay.ny - 1) {
            for i in 0..(overlay.nx - 1) {
                let idx = |jj: usize, ii: usize| jj * overlay.nx + ii;
                let corners = if let Some(points) = pixel_points {
                    match (
                        points[idx(j, i)],
                        points[idx(j, i + 1)],
                        points[idx(j + 1, i + 1)],
                        points[idx(j + 1, i)],
                    ) {
                        (Some(a), Some(b), Some(c), Some(d)) => Some((a, b, c, d)),
                        _ => None,
                    }
                } else {
                    Some((
                        grid_to_pixel(i as f64, j as f64, overlay.nx, overlay.ny, layout),
                        grid_to_pixel((i + 1) as f64, j as f64, overlay.nx, overlay.ny, layout),
                        grid_to_pixel(
                            (i + 1) as f64,
                            (j + 1) as f64,
                            overlay.nx,
                            overlay.ny,
                            layout,
                        ),
                        grid_to_pixel(i as f64, (j + 1) as f64, overlay.nx, overlay.ny, layout),
                    ))
                };
                let Some((c0, c1, c2, c3)) = corners else {
                    continue;
                };

                let p0 = (c0.0, c0.1, overlay.data[idx(j, i)]);
                let p1 = (c1.0, c1.1, overlay.data[idx(j, i + 1)]);
                let p2 = (c2.0, c2.1, overlay.data[idx(j + 1, i + 1)]);
                let p3 = (c3.0, c3.1, overlay.data[idx(j + 1, i)]);

                let mut pts = Vec::with_capacity(4);
                if let Some(p) = interp_point(p0, p1, level) {
                    pts.push(p);
                }
                if let Some(p) = interp_point(p1, p2, level) {
                    pts.push(p);
                }
                if let Some(p) = interp_point(p2, p3, level) {
                    pts.push(p);
                }
                if let Some(p) = interp_point(p3, p0, level) {
                    pts.push(p);
                }

                if pts.len() < 2 {
                    continue;
                }

                let segments: &[(usize, usize)] = if pts.len() == 4 {
                    &[(0, 1), (2, 3)]
                } else {
                    &[(0, 1)]
                };

                for &(a, b) in segments {
                    let (x0, y0) = pts[a];
                    let (x1, y1) = pts[b];
                    if let Some(mask) = clip_mask {
                        if !segment_intersects_mask(mask, x0, y0, x1, y1) {
                            continue;
                        }
                    }
                    draw::draw_line(
                        img,
                        layout.map_x as f64 + x0,
                        layout.map_y as f64 + y0,
                        layout.map_x as f64 + x1,
                        layout.map_y as f64 + y1,
                        overlay.color,
                        overlay.width,
                    );

                    if !label_drawn && (x1 - x0).abs() + (y1 - y0).abs() > 18.0 {
                        let label = text::format_tick(level);
                        let tx = (layout.map_x as f64 + (x0 + x1) * 0.5) as i32
                            - text::text_width(&label, 1) as i32 / 2;
                        let ty = (layout.map_y as f64 + (y0 + y1) * 0.5) as i32 - 4;
                        text::draw_text(img, &label, tx, ty, overlay.color, 1);
                        label_drawn = true;
                    }
                }
            }
        }
    }

    // Draw H/L extrema labels if requested
    if overlay.show_extrema && overlay.nx >= 20 && overlay.ny >= 20 {
        draw_extrema_labels(img, layout, overlay, pixel_points, clip_mask);
    }
}

fn draw_extrema_labels(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    clip_mask: Option<&RgbaImage>,
) {
    let ny = overlay.ny;
    let nx = overlay.nx;
    let data = &overlay.data;

    // Box-blur smoothing (3 passes ≈ Gaussian sigma~3)
    let mut smoothed = data.clone();
    for _ in 0..3 {
        let mut tmp = smoothed.clone();
        let r = 5usize.min(ny / 4).min(nx / 4).max(1);
        for j in r..(ny - r) {
            for i in r..(nx - r) {
                let mut sum = 0.0;
                let mut cnt = 0.0;
                for dj in 0..=(2 * r) {
                    for di in 0..=(2 * r) {
                        let v = smoothed[(j - r + dj) * nx + (i - r + di)];
                        if v.is_finite() {
                            sum += v;
                            cnt += 1.0;
                        }
                    }
                }
                if cnt > 0.0 {
                    tmp[j * nx + i] = sum / cnt;
                }
            }
        }
        smoothed = tmp;
    }

    // Find local extrema
    let window = (ny / 10).max(10).min(30);
    let edge = (ny / 15).max(8);
    let mut highs: Vec<(usize, usize, f64)> = Vec::new();
    let mut lows: Vec<(usize, usize, f64)> = Vec::new();

    for j in edge..(ny - edge) {
        for i in edge..(nx - edge) {
            let val = smoothed[j * nx + i];
            if !val.is_finite() {
                continue;
            }
            let mut is_max = true;
            let mut is_min = true;
            let j0 = j.saturating_sub(window);
            let j1 = (j + window).min(ny - 1);
            let i0 = i.saturating_sub(window);
            let i1 = (i + window).min(nx - 1);
            'scan: for jj in j0..=j1 {
                for ii in i0..=i1 {
                    if jj == j && ii == i {
                        continue;
                    }
                    let v2 = smoothed[jj * nx + ii];
                    if v2 > val {
                        is_max = false;
                    }
                    if v2 < val {
                        is_min = false;
                    }
                    if !is_max && !is_min {
                        break 'scan;
                    }
                }
            }
            if is_max {
                highs.push((j, i, data[j * nx + i]));
            }
            if is_min {
                lows.push((j, i, data[j * nx + i]));
            }
        }
    }

    // Filter by percentile
    let mut sorted: Vec<f64> = data.iter().filter(|v| v.is_finite()).copied().collect();
    if sorted.is_empty() {
        return;
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p20 = sorted[sorted.len() * 20 / 100];
    let p90 = sorted[sorted.len() * 90 / 100];
    lows.retain(|&(_, _, v)| v < p20);
    highs.retain(|&(_, _, v)| v > p90);

    // Remove close neighbors
    let min_dist = 20.0f64;
    let dedup = |pts: &mut Vec<(usize, usize, f64)>| {
        let mut keep = Vec::new();
        for &p in pts.iter() {
            if keep.iter().all(|&(j2, i2, _): &(usize, usize, f64)| {
                ((p.0 as f64 - j2 as f64).powi(2) + (p.1 as f64 - i2 as f64).powi(2)).sqrt()
                    >= min_dist
            }) {
                keep.push(p);
            }
        }
        *pts = keep;
    };
    dedup(&mut highs);
    dedup(&mut lows);

    // Convert grid (j,i) to pixel coordinates
    let to_px = |j: usize, i: usize| -> Option<(i32, i32)> {
        if let Some(points) = pixel_points {
            let idx = j * nx + i;
            points.get(idx)?.map(|(px, py)| {
                (
                    layout.map_x as i32 + px as i32,
                    layout.map_y as i32 + py as i32,
                )
            })
        } else {
            let (px, py) = grid_to_pixel(i as f64, j as f64, nx, ny, layout);
            Some((px as i32, py as i32))
        }
    };

    // Deep royal blue for H, brick red for L — saturated enough to read as
    // labels but muted so they don't feel neon over colored data.
    let h_color = Rgba::new(24, 84, 168);
    let l_color = Rgba::new(176, 46, 42);
    // Outline uses a dark slate rather than pure black so the typographic
    // halo feels like a shadow instead of a hard stroke.
    let halo = Rgba::new(16, 20, 28);

    // Draw H labels
    for &(j, i, val) in &highs {
        if let Some((px, py)) = to_px(j, i) {
            if let Some(mask) = clip_mask {
                if !mask_contains_local_pixel(
                    mask,
                    (px - layout.map_x as i32) as f64,
                    (py - layout.map_y as i32) as f64,
                ) {
                    continue;
                }
            }
            for dx in -1..=1i32 {
                for dy in -1..=1i32 {
                    text::draw_text(img, "H", px + dx, py - 8 + dy, halo, 2);
                }
            }
            text::draw_text(img, "H", px, py - 8, h_color, 2);
            let vlabel = text::format_tick(val);
            for dx in -1..=1i32 {
                for dy in -1..=1i32 {
                    text::draw_text(img, &vlabel, px + dx - 8, py + 14 + dy, halo, 1);
                }
            }
            text::draw_text(img, &vlabel, px - 8, py + 14, h_color, 1);
        }
    }

    // Draw L labels
    for &(j, i, val) in &lows {
        if let Some((px, py)) = to_px(j, i) {
            if let Some(mask) = clip_mask {
                if !mask_contains_local_pixel(
                    mask,
                    (px - layout.map_x as i32) as f64,
                    (py - layout.map_y as i32) as f64,
                ) {
                    continue;
                }
            }
            for dx in -1..=1i32 {
                for dy in -1..=1i32 {
                    text::draw_text(img, "L", px + dx, py - 8 + dy, halo, 2);
                }
            }
            text::draw_text(img, "L", px, py - 8, l_color, 2);
            let vlabel = text::format_tick(val);
            for dx in -1..=1i32 {
                for dy in -1..=1i32 {
                    text::draw_text(img, &vlabel, px + dx - 8, py + 14 + dy, halo, 1);
                }
            }
            text::draw_text(img, &vlabel, px - 8, py + 14, l_color, 1);
        }
    }
}

fn draw_barbs(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &BarbOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    clip_mask: Option<&RgbaImage>,
) {
    if overlay.nx == 0 || overlay.ny == 0 {
        return;
    }
    let sx = overlay.stride_x.max(1);
    let sy = overlay.stride_y.max(1);

    for j in (0..overlay.ny).step_by(sy) {
        for i in (0..overlay.nx).step_by(sx) {
            let idx = j * overlay.nx + i;
            if idx >= overlay.u.len() || idx >= overlay.v.len() {
                continue;
            }
            let (x, y) = if let Some(points) = pixel_points {
                match points.get(idx).and_then(|p| *p) {
                    Some((px, py))
                        if (0.0..layout.map_w as f64).contains(&px)
                            && (0.0..layout.map_h as f64).contains(&py) =>
                    {
                        (layout.map_x as f64 + px, layout.map_y as f64 + py)
                    }
                    None => continue,
                    _ => continue,
                }
            } else {
                grid_to_pixel(i as f64, j as f64, overlay.nx, overlay.ny, layout)
            };
            if let Some(mask) = clip_mask {
                if !mask_contains_local_pixel(
                    mask,
                    x - layout.map_x as f64,
                    y - layout.map_y as f64,
                ) {
                    continue;
                }
            }
            draw::draw_wind_barb(
                img,
                x,
                y,
                overlay.u[idx],
                overlay.v[idx],
                overlay.color,
                overlay.length_px,
                overlay.width,
            );
        }
    }
}

pub fn render_to_image(data: &[f64], ny: usize, nx: usize, opts: &RenderOpts) -> RgbaImage {
    let has_title =
        opts.title.is_some() || opts.subtitle_left.is_some() || opts.subtitle_right.is_some();
    let layout = compute_layout(
        opts.width,
        opts.height,
        opts.colorbar,
        has_title,
        opts.presentation,
    );

    let canvas_background = if opts.background == Rgba::WHITE {
        opts.presentation.canvas_background
    } else {
        opts.background
    };
    let mut img = RgbaImage::from_pixel(opts.width, opts.height, canvas_background.to_image_rgba());
    let map_right = layout.map_x.saturating_add(layout.map_w).min(img.width());
    let map_bottom = layout.map_y.saturating_add(layout.map_h).min(img.height());
    for py in layout.map_y..map_bottom {
        for px in layout.map_x..map_right {
            img.put_pixel(px, py, opts.presentation.map_background.to_image_rgba());
        }
    }

    // Paint filled basemap polygons (ocean, land, lakes) under the data. Done
    // in main-canvas pixel space so we don't have to squeeze polygons through
    // the map_img scratch buffer's rasterize path.
    if let Some(ref extent) = opts.map_extent {
        draw_projected_polygons(
            &mut img,
            &layout,
            extent,
            &opts.projected_polygons,
            opts.presentation,
        );
    }

    let projected_pixels = match (&opts.projected_grid, &opts.map_extent) {
        (Some(grid), Some(extent)) if grid.nx == nx && grid.ny == ny => {
            Some(projected_grid_to_pixels_cached(grid, extent, &layout))
        }
        _ => None,
    };
    let map_img = if let Some(ref pixel_points) = projected_pixels {
        rasterize::rasterize_projected_grid(
            data,
            ny,
            nx,
            pixel_points.as_ref(),
            &opts.cmap,
            layout.map_w,
            layout.map_h,
        )
    } else {
        rasterize::rasterize_grid(data, ny, nx, &opts.cmap, layout.map_w, layout.map_h)
    };

    for py in 0..layout.map_h {
        for px in 0..layout.map_w {
            let src = map_img.get_pixel(px, py);
            let a = src.0[3];
            if a == 0 {
                continue;
            }
            if a == 255 {
                img.put_pixel(layout.map_x + px, layout.map_y + py, *src);
            } else {
                // Alpha-composite over whatever is already there (usually a
                // filled basemap polygon). Keeps the destination opaque.
                draw::blend_pixel(
                    &mut img,
                    (layout.map_x + px) as i32,
                    (layout.map_y + py) as i32,
                    Rgba {
                        r: src.0[0],
                        g: src.0[1],
                        b: src.0[2],
                        a,
                    },
                );
            }
        }
    }

    if let Some(ref extent) = opts.map_extent {
        draw_projected_lines(
            &mut img,
            &layout,
            extent,
            &opts.projected_lines,
            opts.presentation,
        );
    }
    let projected_pixels_ref = projected_pixels.as_deref();
    for contour in &opts.contours {
        // Contours self-clip via their own NaN data (interp_point skips non-finite
        // endpoints), so there's no need to clip to the fill raster. Using the
        // fill raster as a mask incorrectly suppressed contours wherever the fill
        // was masked_below — e.g. 500 mb height contours disappearing outside a
        // CAPE blob.
        draw_contours(&mut img, &layout, contour, projected_pixels_ref, None);
    }
    for barb in &opts.barbs {
        // Barbs have their own NaN handling in draw_wind_barb; same rationale as
        // contours, drop the fill-raster clip so a masked_below fill doesn't
        // suppress valid barbs outside the data blob.
        draw_barbs(&mut img, &layout, barb, projected_pixels_ref, None);
    }

    // Title is a muted near-black slate rather than pure black — reads as
    // editorial/quiet chrome instead of a hard headline. Subtitles shade even
    // lighter so the hierarchy (title > subtitle > attribution) is obvious.
    let title_color = opts.presentation.chrome.title_color;
    let subtitle_color = opts.presentation.chrome.subtitle_color;
    if let Some(ref t) = opts.title {
        match opts.presentation.chrome.title_anchor {
            TitleAnchor::Center => {
                text::draw_text_centered(&mut img, t, layout.title_y as i32, title_color, 1);
            }
            TitleAnchor::Left => {
                text::draw_text(
                    &mut img,
                    t,
                    layout.map_x as i32,
                    layout.title_y as i32,
                    title_color,
                    1,
                );
            }
        }
    }
    if let Some(ref t) = opts.subtitle_left {
        text::draw_text(
            &mut img,
            t,
            layout.map_x as i32,
            layout.subtitle_y as i32,
            subtitle_color,
            1,
        );
    }
    if let Some(ref t) = opts.subtitle_right {
        text::draw_text_right(
            &mut img,
            t,
            (layout.map_x + layout.map_w) as i32,
            layout.subtitle_y as i32,
            subtitle_color,
            1,
        );
    }
    if has_title && opts.subtitle_left.is_none() && opts.subtitle_right.is_none() {
        // Only show attribution when no explicit subtitles are set
        let made_by = "Color Tables: Solarpower07";
        text::draw_text_centered(
            &mut img,
            made_by,
            layout.subtitle_y as i32,
            Rgba::new(168, 174, 184),
            1,
        );
    }

    // Thin cool-gray frame around the map area — gives the plot a defined edge
    // without heavy rule lines. Only drawn when there is actual chrome
    // (title or colorbar); bare overlay-only renders stay chromeless so tests
    // that assert a fully transparent-input render is blank still pass.
    if let Some(frame) = opts.presentation.chrome.frame_color {
        let map_right = layout.map_x + layout.map_w.saturating_sub(1);
        let map_bottom = layout.map_y + layout.map_h.saturating_sub(1);
        for px in layout.map_x..=map_right.min(img.width().saturating_sub(1)) {
            if layout.map_y < img.height() {
                img.put_pixel(px, layout.map_y, frame.to_image_rgba());
            }
            if map_bottom < img.height() {
                img.put_pixel(px, map_bottom, frame.to_image_rgba());
            }
        }
        for py in layout.map_y..=map_bottom.min(img.height().saturating_sub(1)) {
            if layout.map_x < img.width() {
                img.put_pixel(layout.map_x, py, frame.to_image_rgba());
            }
            if map_right < img.width() {
                img.put_pixel(map_right, py, frame.to_image_rgba());
            }
        }
    }

    if let Some(domain_boundary) = opts.presentation.domain_boundary {
        if domain_boundary.visible {
            let map_right = layout.map_x + layout.map_w.saturating_sub(1);
            let map_bottom = layout.map_y + layout.map_h.saturating_sub(1);
            draw::draw_polyline(
                &mut img,
                &[
                    (layout.map_x as f64, layout.map_y as f64),
                    (map_right as f64, layout.map_y as f64),
                    (map_right as f64, map_bottom as f64),
                    (layout.map_x as f64, map_bottom as f64),
                    (layout.map_x as f64, layout.map_y as f64),
                ],
                domain_boundary.color,
                domain_boundary.width,
            );
        }
    }

    if opts.colorbar {
        colorbar::draw_colorbar(
            &mut img,
            &opts.cmap,
            layout.cbar_x,
            layout.cbar_y,
            layout.cbar_w,
            layout.cbar_h,
            opts.presentation.colorbar,
        );
        let ticks = pick_ticks(&opts.cmap.levels, opts.cbar_tick_step);
        let levels = &opts.cmap.levels;
        if levels.len() >= 2 {
            let lo = levels[0];
            let hi = levels[levels.len() - 1];
            let range = hi - lo;
            if range > 0.0 {
                let tick_positions: Vec<f64> = ticks.iter().map(|t| (t - lo) / range).collect();
                colorbar::draw_colorbar_ticks(
                    &mut img,
                    layout.cbar_x,
                    layout.cbar_y,
                    layout.cbar_w,
                    &tick_positions,
                    opts.presentation.colorbar.tick_color,
                );
                // Labels sit above the tick marks; text is a muted near-black so
                // it doesn't compete with the colorbar swatches.
                let tick_y = layout.cbar_y.saturating_sub(14) as i32;
                let label_color = opts.presentation.colorbar.label_color;
                for tick_val in &ticks {
                    let frac = (tick_val - lo) / range;
                    let px = layout.cbar_x as f64 + frac * layout.cbar_w as f64;
                    let label = text::format_tick(*tick_val);
                    let lw = text::text_width(&label, 1);
                    let lx = (px as i32) - (lw as i32 / 2);
                    text::draw_text(&mut img, &label, lx, tick_y, label_color, 1);
                }
            }
        }
    }

    img
}

pub fn render_to_png(data: &[f64], ny: usize, nx: usize, opts: &RenderOpts) -> Vec<u8> {
    let mut buf = Vec::new();
    render_to_image(data, ny, nx, opts)
        .write_to(&mut Cursor::new(&mut buf), image::ImageFormat::Png)
        .expect("PNG encoding failed");
    buf
}

#[cfg(test)]
fn reset_projected_pixel_cache_for_tests() {
    PROJECTED_PIXEL_CACHE.with(|cache_cell| {
        *cache_cell.borrow_mut() = None;
    });
    PROJECTED_PIXEL_CACHE_MISSES.with(|count| count.set(0));
}

#[cfg(test)]
fn projected_pixel_cache_miss_count_for_tests() -> usize {
    PROJECTED_PIXEL_CACHE_MISSES.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colormap::Extend;

    fn sample_cmap() -> LeveledColormap {
        LeveledColormap::from_palette(
            &[Rgba::new(0, 0, 255), Rgba::new(255, 0, 0)],
            &[0.0, 1.0, 2.0, 3.0],
            Extend::Neither,
            None,
        )
    }

    fn sample_projected_grid() -> ProjectedGrid {
        ProjectedGrid {
            x: vec![0.0, 1.0, 0.0, 1.0],
            y: vec![0.0, 0.0, 1.0, 1.0],
            ny: 2,
            nx: 2,
        }
    }

    fn sample_projected_opts() -> RenderOpts {
        RenderOpts {
            width: 240,
            height: 160,
            cmap: sample_cmap(),
            background: Rgba::WHITE,
            colorbar: false,
            title: Some("Projected".into()),
            subtitle_left: None,
            subtitle_right: None,
            cbar_tick_step: None,
            map_extent: Some(MapExtent {
                x_min: 0.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 1.0,
            }),
            projected_grid: Some(sample_projected_grid()),
            projected_polygons: Vec::new(),
            projected_lines: Vec::new(),
            contours: Vec::new(),
            barbs: Vec::new(),
            presentation: RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
        }
    }

    #[test]
    fn render_to_png_reuses_projected_pixel_cache_for_identical_meshes() {
        let _guard = PROJECTED_PIXEL_CACHE_TEST_LOCK.lock().unwrap();
        reset_projected_pixel_cache_for_tests();

        let data = [0.0, 1.0, 2.0, 3.0];
        let opts = sample_projected_opts();

        let first = render_to_png(&data, 2, 2, &opts);
        let second = render_to_png(&data, 2, 2, &opts);

        assert_eq!(first, second);
        assert_eq!(projected_pixel_cache_miss_count_for_tests(), 1);
    }

    #[test]
    fn render_to_png_recomputes_projected_pixels_when_extent_changes() {
        let _guard = PROJECTED_PIXEL_CACHE_TEST_LOCK.lock().unwrap();
        reset_projected_pixel_cache_for_tests();

        let data = [0.0, 1.0, 2.0, 3.0];
        let opts = sample_projected_opts();
        let mut shifted = sample_projected_opts();
        shifted.map_extent = Some(MapExtent {
            x_min: -0.25,
            x_max: 0.75,
            y_min: 0.0,
            y_max: 1.0,
        });

        render_to_png(&data, 2, 2, &opts);
        render_to_png(&data, 2, 2, &shifted);

        assert_eq!(projected_pixel_cache_miss_count_for_tests(), 2);
    }

    #[test]
    fn map_frame_aspect_ratio_matches_wide_render_layout() {
        let ratio = map_frame_aspect_ratio(1200, 900, true, true);
        assert!(ratio > 1.4);
        assert!(ratio < 1.7);
    }

    #[test]
    fn render_to_png_suppresses_barbs_when_overlay_data_is_nan() {
        // Updated expectation: barb overlays are no longer clipped to the fill
        // raster (that broke height-contour / wind-barb renders when the fill
        // used mask_below). Instead, barbs clip themselves via NaN u/v values.
        let _guard = PROJECTED_PIXEL_CACHE_TEST_LOCK.lock().unwrap();
        let mut opts = sample_projected_opts();
        opts.title = None;
        opts.barbs = vec![BarbOverlay {
            u: vec![f64::NAN; 4],
            v: vec![f64::NAN; 4],
            ny: 2,
            nx: 2,
            stride_x: 1,
            stride_y: 1,
            color: Rgba::BLACK,
            width: 1,
            length_px: 12.0,
        }];

        let data = [0.5f64; 4];
        let png = render_to_png(&data, 2, 2, &opts);
        let image = image::load_from_memory_with_format(&png, image::ImageFormat::Png)
            .unwrap()
            .to_rgba8();
        // NaN u/v means no barb glyphs are drawn.
        let non_fill = image.pixels().filter(|px| px.0 == [0, 0, 0, 255]).count();
        assert_eq!(non_fill, 0, "NaN barb vectors should produce no glyphs");
    }

    #[test]
    fn render_to_png_suppresses_contours_when_overlay_data_is_nan() {
        // Updated expectation: contour overlays self-clip via NaN data, not via
        // the fill raster. Lets height contours render across the whole frame
        // even when the paired CAPE fill uses mask_below.
        let _guard = PROJECTED_PIXEL_CACHE_TEST_LOCK.lock().unwrap();
        let mut opts = sample_projected_opts();
        opts.title = None;
        opts.contours = vec![ContourOverlay {
            data: vec![f64::NAN; 4],
            ny: 2,
            nx: 2,
            levels: vec![1.5],
            color: Rgba::BLACK,
            width: 1,
            labels: false,
            show_extrema: false,
        }];

        let data = [0.5f64; 4];
        let png = render_to_png(&data, 2, 2, &opts);
        let image = image::load_from_memory_with_format(&png, image::ImageFormat::Png)
            .unwrap()
            .to_rgba8();
        let contour_pixels = image.pixels().filter(|px| px.0 == [0, 0, 0, 255]).count();
        assert_eq!(
            contour_pixels, 0,
            "NaN contour data should produce no contour lines"
        );
    }
}
