use crate::color::Rgba;
use crate::colorbar;
use crate::colormap::LeveledColormap;
use crate::draw;
use crate::overlay::{
    BarbOverlay, ContourOverlay, MapExtent, ProjectedGrid, ProjectedPlaceLabelOverlay,
    ProjectedPointOverlay, ProjectedPolygon, ProjectedPolyline,
};
use crate::presentation::{ProductVisualMode, RenderPresentation, TitleAnchor};
use crate::rasterize;
use crate::request::{
    ChromeScale, DomainFrame, ProjectedLabelPlacement, ProjectedMarkerShape,
    ProjectedPlaceLabelPriority,
};
use crate::text;
use image::ExtendedColorType;
use image::ImageEncoder;
use image::RgbaImage;
use image::codecs::png::{CompressionType, FilterType as PngFilterType, PngEncoder};
use image::imageops::{FilterType, crop_imm, filter3x3, resize};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::sync::Mutex;

/// Full render configuration.
#[derive(Clone)]
pub struct RenderOpts {
    pub width: u32,
    pub height: u32,
    pub cmap: LeveledColormap,
    pub background: Rgba,
    pub colorbar: bool,
    pub title: Option<String>,
    pub subtitle_left: Option<String>,
    pub subtitle_center: Option<String>,
    pub subtitle_right: Option<String>,
    pub cbar_tick_step: Option<f64>,
    pub colorbar_mode: crate::colormap::LegendMode,
    pub chrome_scale: ChromeScale,
    pub supersample_factor: u32,
    pub domain_frame: Option<DomainFrame>,
    pub map_extent: Option<MapExtent>,
    pub projected_grid: Option<ProjectedGrid>,
    /// Filled polygons (lat/lon-derived). Drawn BEFORE the data raster so the
    /// data overlays on top; ordering within the list is bottom-to-top.
    /// Typical stack: ocean → land → lakes.
    pub projected_polygons: Vec<ProjectedPolygon>,
    pub projected_data_polygons: Vec<ProjectedPolygon>,
    pub projected_place_labels: Vec<ProjectedPlaceLabelOverlay>,
    pub projected_points: Vec<ProjectedPointOverlay>,
    pub projected_lines: Vec<ProjectedPolyline>,
    pub contours: Vec<ContourOverlay>,
    pub barbs: Vec<BarbOverlay>,
    pub presentation: RenderPresentation,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RenderImageTiming {
    pub layout_ms: u128,
    pub background_ms: u128,
    pub polygon_fill_ms: u128,
    pub projected_pixel_ms: u128,
    pub rasterize_ms: u128,
    pub raster_blit_ms: u128,
    pub linework_ms: u128,
    pub contour_ms: u128,
    pub barb_ms: u128,
    #[serde(default)]
    pub outside_frame_clear_ms: u128,
    pub chrome_ms: u128,
    pub colorbar_ms: u128,
    #[serde(default)]
    pub downsample_ms: u128,
    pub postprocess_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RenderPngTiming {
    pub image_timing: RenderImageTiming,
    #[serde(default)]
    pub render_to_image_ms: u128,
    pub png_encode_ms: u128,
    #[serde(default)]
    pub png_write_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PngCompressionMode {
    #[default]
    Default,
    Fast,
    Fastest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PngWriteOptions {
    #[serde(default)]
    pub compression: PngCompressionMode,
}

impl Default for PngWriteOptions {
    fn default() -> Self {
        Self {
            compression: PngCompressionMode::Default,
        }
    }
}

impl Default for RenderOpts {
    fn default() -> Self {
        Self {
            width: 1100,
            height: 850,
            cmap: LeveledColormap {
                levels: vec![],
                colors: vec![],
                legend_levels: vec![],
                legend_colors: vec![],
                under_color: None,
                over_color: None,
                mask_below: None,
            },
            background: Rgba::WHITE,
            colorbar: true,
            title: None,
            subtitle_left: None,
            subtitle_center: None,
            subtitle_right: None,
            cbar_tick_step: None,
            colorbar_mode: crate::colormap::LegendMode::Stepped,
            chrome_scale: ChromeScale::default(),
            supersample_factor: 1,
            domain_frame: None,
            map_extent: None,
            projected_grid: None,
            projected_polygons: vec![],
            projected_data_polygons: vec![],
            projected_place_labels: vec![],
            projected_points: vec![],
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
    text_scale: u32,
    label_gap: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalRect {
    min_x: u32,
    max_x: u32,
    min_y: u32,
    max_y: u32,
}

impl LocalRect {
    fn from_bounds(bounds: (u32, u32, u32, u32)) -> Self {
        let (min_x, max_x, min_y, max_y) = bounds;
        Self {
            min_x,
            max_x,
            min_y,
            max_y,
        }
    }

    fn width(self) -> u32 {
        self.max_x.saturating_sub(self.min_x).saturating_add(1)
    }
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

#[derive(Clone)]
struct CachedStaticBase {
    key: u64,
    image: RgbaImage,
}

struct VariableLayerTiming {
    rasterize_ms: u128,
    raster_blit_ms: u128,
    linework_ms: u128,
    contour_ms: u128,
    barb_ms: u128,
    outside_frame_clear_ms: u128,
    domain_clip_rect: Option<LocalRect>,
}

thread_local! {
    static PROJECTED_PIXEL_CACHE: RefCell<Option<CachedProjectedPixels>> = const { RefCell::new(None) };
    static STATIC_BASE_CACHE: RefCell<Option<CachedStaticBase>> = const { RefCell::new(None) };
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
    chrome_scale: ChromeScale,
) -> Layout {
    let chrome_scale = resolve_chrome_scale(total_w, total_h, chrome_scale);
    let metrics = scaled_layout_metrics(presentation.layout, chrome_scale);
    let text_scale = text_scale_from_chrome(chrome_scale);
    let title_line_h = match presentation.chrome.title_anchor {
        TitleAnchor::Center => text::bold_line_height(text_scale),
        TitleAnchor::Left => text::regular_line_height(text_scale),
    };
    let subtitle_line_h = text::regular_line_height(text_scale);
    let header_top_pad = scale_u32(5, chrome_scale);
    let header_bottom_pad = scale_u32(5, chrome_scale);
    let map_x = metrics.margin_x.min(total_w.saturating_sub(1));
    let title_h = if has_title {
        metrics.title_h.max(
            header_top_pad
                .saturating_add(title_line_h.max(subtitle_line_h))
                .saturating_add(header_bottom_pad),
        )
    } else {
        0
    };
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
        map_x
            .saturating_add(metrics.colorbar_margin_x)
            .min(total_w.saturating_sub(1))
    } else {
        0
    };
    let cbar_w = if has_cbar {
        map_w
            .saturating_sub(metrics.colorbar_margin_x.saturating_mul(2))
            .max(1)
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
        title_y: if has_title { header_top_pad } else { 0 },
        subtitle_y: if has_title { header_top_pad } else { 0 },
        text_scale,
        label_gap: scale_u32(12, chrome_scale),
    }
}

fn resolve_chrome_scale(total_w: u32, total_h: u32, chrome_scale: ChromeScale) -> f32 {
    match chrome_scale {
        ChromeScale::Fixed(value) => value.clamp(0.5, 4.0),
        ChromeScale::Auto {
            base_width,
            base_height,
            min,
            max,
        } => {
            let base_area = (base_width.max(1) as f64) * (base_height.max(1) as f64);
            let area = (total_w.max(1) as f64) * (total_h.max(1) as f64);
            ((area / base_area).sqrt() as f32).clamp(min, max)
        }
    }
}

fn scale_u32(value: u32, scale: f32) -> u32 {
    ((value as f32) * scale).round().max(1.0) as u32
}

fn scaled_layout_metrics(
    metrics: crate::presentation::LayoutMetrics,
    scale: f32,
) -> crate::presentation::LayoutMetrics {
    crate::presentation::LayoutMetrics {
        margin_x: scale_u32(metrics.margin_x, scale),
        title_h: scale_u32(metrics.title_h, scale),
        footer_h: scale_u32(metrics.footer_h, scale),
        colorbar_h: scale_u32(metrics.colorbar_h, scale),
        colorbar_gap: scale_u32(metrics.colorbar_gap, scale),
        colorbar_margin_x: scale_u32(metrics.colorbar_margin_x, scale),
    }
}

fn text_scale_from_chrome(chrome_scale: f32) -> u32 {
    ((chrome_scale * 3.0).ceil() as u32)
        .saturating_add(1)
        .clamp(3, 12)
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
        ChromeScale::default(),
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

fn colorbar_levels_for_ticks(cmap: &LeveledColormap) -> &[f64] {
    cmap.legend_levels_for_display()
}

fn measure_text_width(text: &str, scale: u32, bold: bool) -> u32 {
    if bold {
        text::text_width_bold(text, scale)
    } else {
        text::text_width(text, scale)
    }
}

fn measure_text_width_with_factor(text: &str, scale: u32, size_factor: f32, bold: bool) -> u32 {
    if bold {
        text::text_width_bold_with_factor(text, scale, size_factor)
    } else {
        text::text_width_with_factor(text, scale, size_factor)
    }
}

fn centered_text_left(text: &str, center_x: u32, scale: u32, bold: bool) -> i32 {
    let width = measure_text_width(text, scale, bold) as i32;
    center_x as i32 - width / 2
}

fn ellipsize_text_to_width(text: &str, max_width: u32, scale: u32, bold: bool) -> String {
    if measure_text_width(text, scale, bold) <= max_width {
        return text.to_string();
    }

    let ellipsis = "...";
    let ellipsis_w = measure_text_width(ellipsis, scale, bold);
    if ellipsis_w >= max_width {
        return ellipsis.to_string();
    }

    let mut kept = String::new();
    for ch in text.chars() {
        let mut candidate = kept.clone();
        candidate.push(ch);
        candidate.push_str(ellipsis);
        if measure_text_width(&candidate, scale, bold) > max_width {
            break;
        }
        kept.push(ch);
    }

    if kept.is_empty() {
        ellipsis.to_string()
    } else {
        format!("{kept}{ellipsis}")
    }
}

fn ellipsize_text_to_width_with_factor(
    text: &str,
    max_width: u32,
    scale: u32,
    size_factor: f32,
    bold: bool,
) -> String {
    if measure_text_width_with_factor(text, scale, size_factor, bold) <= max_width {
        return text.to_string();
    }

    let ellipsis = "...";
    let ellipsis_w = measure_text_width_with_factor(ellipsis, scale, size_factor, bold);
    if ellipsis_w >= max_width {
        return ellipsis.to_string();
    }

    let mut kept = String::new();
    for ch in text.chars() {
        let mut candidate = kept.clone();
        candidate.push(ch);
        candidate.push_str(ellipsis);
        if measure_text_width_with_factor(&candidate, scale, size_factor, bold) > max_width {
            break;
        }
        kept.push(ch);
    }

    if kept.is_empty() {
        ellipsis.to_string()
    } else {
        format!("{kept}{ellipsis}")
    }
}

fn filter_tick_labels_to_fit(
    ticks: &[f64],
    lo: f64,
    range: f64,
    cbar_x: u32,
    cbar_w: u32,
    label_left_bound: u32,
    label_right_bound: u32,
    img_w: u32,
    text_scale: u32,
) -> Vec<(f64, i32, String)> {
    let min_gap_px = (6 * text_scale.max(1)) as i32;
    let min_x = label_left_bound.min(img_w.saturating_sub(1)) as i32;
    let max_x = label_right_bound.min(img_w) as i32;
    if max_x <= min_x {
        return Vec::new();
    }
    let mut labels = Vec::with_capacity(ticks.len());
    let mut last_right = i32::MIN / 4;

    for tick_val in ticks {
        let frac = (tick_val - lo) / range;
        let px = cbar_x as f64 + frac * cbar_w as f64;
        let label = text::format_tick(*tick_val);
        let lw = text::text_width(&label, text_scale) as i32;
        if lw >= max_x.saturating_sub(min_x) {
            continue;
        }
        let centered_lx = (px as i32) - (lw / 2);
        let max_lx = max_x.saturating_sub(lw);
        let lx = centered_lx.clamp(min_x, max_lx.max(min_x));
        if !labels.is_empty() && lx <= last_right.saturating_add(min_gap_px) {
            continue;
        }
        last_right = lx.saturating_add(lw);
        labels.push((*tick_val, lx, label));
    }

    labels
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
    clip_rect: Option<(i32, i32, i32, i32)>,
) {
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
        draw::fill_polygon(img, &rings, style.color, clip_rect);
    }
}

fn draw_projected_lines(
    img: &mut RgbaImage,
    layout: &Layout,
    extent: &MapExtent,
    lines: &[ProjectedPolyline],
    presentation: RenderPresentation,
    clip_mask: Option<&RgbaImage>,
) {
    for line in lines {
        let style = presentation.linework_style(line.role, line.color, line.width);
        if !style.visible {
            continue;
        }
        let mut current: Vec<(f64, f64)> = Vec::with_capacity(line.points.len());
        let mut previous_local: Option<(f64, f64)> = None;
        for &(x, y) in &line.points {
            if let Some((px, py)) = extent.to_pixel(x, y, layout.map_w, layout.map_h) {
                let visible = clip_mask
                    .map(|mask| {
                        previous_local
                            .map(|(prev_x, prev_y)| {
                                segment_intersects_mask(mask, prev_x, prev_y, px, py)
                            })
                            .unwrap_or_else(|| mask_contains_local_pixel(mask, px, py))
                    })
                    .unwrap_or(true);
                if visible {
                    current.push((layout.map_x as f64 + px, layout.map_y as f64 + py));
                } else if current.len() >= 2 {
                    draw::draw_polyline_aa(img, &current, style.color, style.width);
                    current.clear();
                } else {
                    current.clear();
                }
                previous_local = Some((px, py));
            } else if current.len() >= 2 {
                draw::draw_polyline_aa(img, &current, style.color, style.width);
                current.clear();
                previous_local = None;
            } else {
                current.clear();
                previous_local = None;
            }
        }
        if current.len() >= 2 {
            draw::draw_polyline_aa(img, &current, style.color, style.width);
        }
    }
}

fn draw_projected_points(
    img: &mut RgbaImage,
    layout: &Layout,
    extent: &MapExtent,
    points: &[ProjectedPointOverlay],
    clip_mask: Option<&RgbaImage>,
) {
    for point in points {
        let Some((px, py)) = extent.to_pixel(point.x, point.y, layout.map_w, layout.map_h) else {
            continue;
        };
        if let Some(mask) = clip_mask {
            if !mask_contains_local_pixel(mask, px, py) {
                continue;
            }
        }

        let x = layout.map_x as f64 + px.clamp(0.0, layout.map_w.saturating_sub(1) as f64);
        let y = layout.map_y as f64 + py.clamp(0.0, layout.map_h.saturating_sub(1) as f64);
        let radius = point.radius_px.max(1) as f64;
        let width = point.width_px.max(1);
        match point.shape {
            ProjectedMarkerShape::Circle => {
                draw::draw_circle_stroke_aa(img, x, y, radius, point.color, width);
            }
            ProjectedMarkerShape::Plus => {
                draw::draw_plus_marker_aa(img, x, y, radius, point.color, width);
            }
            ProjectedMarkerShape::Cross => {
                draw::draw_cross_marker_aa(img, x, y, radius, point.color, width);
            }
        }
    }
}

fn label_bounds(layout: &Layout, clip_rect: Option<LocalRect>) -> (i32, i32, i32, i32) {
    if let Some(rect) = clip_rect {
        (
            (layout.map_x + rect.min_x) as i32,
            (layout.map_x + rect.max_x) as i32,
            (layout.map_y + rect.min_y) as i32,
            (layout.map_y + rect.max_y) as i32,
        )
    } else {
        let max_x = layout.map_x.saturating_add(layout.map_w).saturating_sub(1) as i32;
        let max_y = layout.map_y.saturating_add(layout.map_h).saturating_sub(1) as i32;
        (layout.map_x as i32, max_x, layout.map_y as i32, max_y)
    }
}

fn label_line_height_with_factor(scale: u32, size_factor: f32, bold: bool) -> u32 {
    if bold {
        text::bold_line_height_with_factor(scale, size_factor)
    } else {
        text::regular_line_height_with_factor(scale, size_factor)
    }
}

fn draw_styled_text(
    img: &mut RgbaImage,
    text_value: &str,
    x: i32,
    y: i32,
    color: Rgba,
    scale: u32,
    size_factor: f32,
    bold: bool,
) {
    if bold {
        text::draw_text_bold_with_factor(img, text_value, x, y, color, scale, size_factor);
    } else {
        text::draw_text_with_factor(img, text_value, x, y, color, scale, size_factor);
    }
}

fn label_top_left(
    placement: ProjectedLabelPlacement,
    anchor_x: i32,
    anchor_y: i32,
    label_width: u32,
    label_height: u32,
) -> (i32, i32) {
    let width = label_width as i32;
    let height = label_height as i32;
    match placement {
        ProjectedLabelPlacement::Center => (anchor_x - width / 2, anchor_y - height / 2),
        ProjectedLabelPlacement::Left => (anchor_x - width, anchor_y - height / 2),
        ProjectedLabelPlacement::Right => (anchor_x, anchor_y - height / 2),
        ProjectedLabelPlacement::Above => (anchor_x - width / 2, anchor_y - height),
        ProjectedLabelPlacement::Below => (anchor_x - width / 2, anchor_y),
        ProjectedLabelPlacement::AboveLeft => (anchor_x - width, anchor_y - height),
        ProjectedLabelPlacement::AboveRight => (anchor_x, anchor_y - height),
        ProjectedLabelPlacement::BelowLeft => (anchor_x - width, anchor_y),
        ProjectedLabelPlacement::BelowRight => (anchor_x, anchor_y),
    }
}

fn draw_text_halo(
    img: &mut RgbaImage,
    text_value: &str,
    x: i32,
    y: i32,
    label_color: Rgba,
    halo_color: Rgba,
    halo_width_px: u32,
    scale: u32,
    size_factor: f32,
    bold: bool,
) {
    let halo_width_px = halo_width_px as i32;
    if halo_color.a > 0 && halo_width_px > 0 {
        for dy in -halo_width_px..=halo_width_px {
            for dx in -halo_width_px..=halo_width_px {
                if dx == 0 && dy == 0 {
                    continue;
                }
                if dx.abs().max(dy.abs()) > halo_width_px {
                    continue;
                }
                draw_styled_text(
                    img,
                    text_value,
                    x + dx,
                    y + dy,
                    halo_color,
                    scale,
                    size_factor,
                    bold,
                );
            }
        }
    }
    draw_styled_text(img, text_value, x, y, label_color, scale, size_factor, bold);
}

#[derive(Debug, Clone, Copy)]
struct PlaceLabelRenderAdjustments {
    text_size_factor: f32,
    text_alpha_factor: f32,
    halo_alpha_factor: f32,
    marker_scale_factor: f32,
    marker_alpha_factor: f32,
    outline_width_factor: f32,
    halo_width_factor: f32,
    offset_factor: f32,
}

fn place_label_render_adjustments(
    priority: ProjectedPlaceLabelPriority,
) -> PlaceLabelRenderAdjustments {
    match priority {
        ProjectedPlaceLabelPriority::Primary => PlaceLabelRenderAdjustments {
            text_size_factor: 1.0,
            text_alpha_factor: 1.0,
            halo_alpha_factor: 1.0,
            marker_scale_factor: 1.0,
            marker_alpha_factor: 1.0,
            outline_width_factor: 1.0,
            halo_width_factor: 1.0,
            offset_factor: 1.0,
        },
        ProjectedPlaceLabelPriority::Auxiliary => PlaceLabelRenderAdjustments {
            text_size_factor: 0.90,
            text_alpha_factor: 0.84,
            halo_alpha_factor: 0.78,
            marker_scale_factor: 0.82,
            marker_alpha_factor: 0.84,
            outline_width_factor: 0.75,
            halo_width_factor: 0.75,
            offset_factor: 0.92,
        },
        ProjectedPlaceLabelPriority::Micro => PlaceLabelRenderAdjustments {
            text_size_factor: 0.82,
            text_alpha_factor: 0.72,
            halo_alpha_factor: 0.62,
            marker_scale_factor: 0.68,
            marker_alpha_factor: 0.72,
            outline_width_factor: 0.50,
            halo_width_factor: 0.50,
            offset_factor: 0.85,
        },
    }
}

fn scale_alpha(color: Rgba, factor: f32) -> Rgba {
    Rgba::with_alpha(
        color.r,
        color.g,
        color.b,
        ((color.a as f32) * factor.clamp(0.0, 1.0)).round() as u8,
    )
}

fn scale_nonzero_u32(value: u32, factor: f32) -> u32 {
    if value == 0 {
        0
    } else {
        ((value as f32) * factor).round().max(1.0) as u32
    }
}

fn scale_i32(value: i32, factor: f32) -> i32 {
    ((value as f32) * factor).round() as i32
}

fn draw_projected_place_labels(
    img: &mut RgbaImage,
    layout: &Layout,
    extent: &MapExtent,
    place_labels: &[ProjectedPlaceLabelOverlay],
    clip_mask: Option<&RgbaImage>,
    clip_rect: Option<LocalRect>,
) {
    let (min_x, max_x, min_y, max_y) = label_bounds(layout, clip_rect);
    let available_width = max_x.saturating_sub(min_x).saturating_add(1) as u32;
    let available_height = max_y.saturating_sub(min_y).saturating_add(1) as u32;

    for place_label in place_labels {
        let adjustments = place_label_render_adjustments(place_label.priority);
        let Some((px, py)) =
            extent.to_pixel(place_label.x, place_label.y, layout.map_w, layout.map_h)
        else {
            continue;
        };

        if let Some(mask) = clip_mask {
            if !mask_contains_local_pixel(mask, px, py) {
                continue;
            }
        }

        let marker_x = layout.map_x as f64 + px.clamp(0.0, layout.map_w.saturating_sub(1) as f64);
        let marker_y = layout.map_y as f64 + py.clamp(0.0, layout.map_h.saturating_sub(1) as f64);
        let marker_radius = scale_nonzero_u32(
            place_label.style.marker_radius_px,
            adjustments.marker_scale_factor,
        );
        let marker_outline_width = scale_nonzero_u32(
            place_label.style.marker_outline_width,
            adjustments.outline_width_factor,
        );
        if marker_radius > 0 {
            draw::draw_circle_fill_aa(
                img,
                marker_x,
                marker_y,
                marker_radius as f64,
                scale_alpha(
                    place_label.style.marker_fill,
                    adjustments.marker_alpha_factor,
                ),
            );
            let marker_outline = scale_alpha(
                place_label.style.marker_outline,
                adjustments.marker_alpha_factor,
            );
            if marker_outline_width > 0 && marker_outline.a > 0 {
                draw::draw_circle_stroke_aa(
                    img,
                    marker_x,
                    marker_y,
                    marker_radius as f64,
                    marker_outline,
                    marker_outline_width,
                );
            }
        }

        let Some(label) = place_label
            .label
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };

        let bold = place_label.style.label_bold;
        let scale = place_label.style.label_scale.max(1);
        let fitted_label = ellipsize_text_to_width_with_factor(
            label,
            available_width.max(1),
            scale,
            adjustments.text_size_factor,
            bold,
        );
        let label_width = measure_text_width_with_factor(
            &fitted_label,
            scale,
            adjustments.text_size_factor,
            bold,
        );
        let label_height = label_line_height_with_factor(scale, adjustments.text_size_factor, bold);
        if label_width == 0 || label_height == 0 || label_height > available_height {
            continue;
        }

        let anchor_x = marker_x.round() as i32
            + scale_i32(
                place_label.style.label_offset_x_px,
                adjustments.offset_factor,
            );
        let anchor_y = marker_y.round() as i32
            + scale_i32(
                place_label.style.label_offset_y_px,
                adjustments.offset_factor,
            );
        let (mut text_x, mut text_y) = label_top_left(
            place_label.style.label_placement,
            anchor_x,
            anchor_y,
            label_width,
            label_height,
        );
        let max_label_x = max_x.saturating_sub(label_width as i32).saturating_add(1);
        let max_label_y = max_y.saturating_sub(label_height as i32).saturating_add(1);
        text_x = text_x.clamp(min_x, max_label_x.max(min_x));
        text_y = text_y.clamp(min_y, max_label_y.max(min_y));

        draw_text_halo(
            img,
            &fitted_label,
            text_x,
            text_y,
            scale_alpha(place_label.style.label_color, adjustments.text_alpha_factor),
            scale_alpha(place_label.style.label_halo, adjustments.halo_alpha_factor),
            scale_nonzero_u32(
                place_label.style.label_halo_width_px,
                adjustments.halo_width_factor,
            ),
            scale,
            adjustments.text_size_factor,
            bold,
        );
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

fn hash_rgba(hasher: &mut impl Hasher, color: Rgba) {
    color.r.hash(hasher);
    color.g.hash(hasher);
    color.b.hash(hasher);
    color.a.hash(hasher);
}

fn hash_extent(hasher: &mut impl Hasher, extent: &MapExtent) {
    extent.x_min.to_bits().hash(hasher);
    extent.x_max.to_bits().hash(hasher);
    extent.y_min.to_bits().hash(hasher);
    extent.y_max.to_bits().hash(hasher);
}

fn hash_projected_polygons(hasher: &mut impl Hasher, polygons: &[ProjectedPolygon]) {
    polygons.len().hash(hasher);
    for polygon in polygons {
        hash_rgba(hasher, polygon.color);
        std::mem::discriminant(&polygon.role).hash(hasher);
        polygon.rings.len().hash(hasher);
        for ring in &polygon.rings {
            ring.len().hash(hasher);
            for &(x, y) in ring {
                x.to_bits().hash(hasher);
                y.to_bits().hash(hasher);
            }
        }
    }
}

fn static_base_cache_key(
    opts: &RenderOpts,
    layout: &Layout,
    extent: Option<&MapExtent>,
    domain_frame_rect: Option<LocalRect>,
    canvas_background: Rgba,
    map_background: Rgba,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    opts.width.hash(&mut hasher);
    opts.height.hash(&mut hasher);
    layout.map_x.hash(&mut hasher);
    layout.map_y.hash(&mut hasher);
    layout.map_w.hash(&mut hasher);
    layout.map_h.hash(&mut hasher);
    hash_rgba(&mut hasher, canvas_background);
    hash_rgba(&mut hasher, map_background);
    opts.domain_frame.is_some().hash(&mut hasher);
    if let Some(frame) = opts.domain_frame {
        frame.clear_outside.hash(&mut hasher);
    }
    domain_frame_rect
        .map(|rect| (rect.min_x, rect.max_x, rect.min_y, rect.max_y).hash(&mut hasher));
    if let Some(extent) = extent {
        hash_extent(&mut hasher, extent);
    } else {
        0u8.hash(&mut hasher);
    }
    hash_projected_polygons(&mut hasher, &opts.projected_polygons);
    hasher.finish()
}

fn build_static_base_image(
    opts: &RenderOpts,
    layout: &Layout,
    extent: Option<&MapExtent>,
    domain_frame_rect: Option<LocalRect>,
    canvas_background: Rgba,
    map_background: Rgba,
    polygon_clip_rect: (i32, i32, i32, i32),
) -> (RgbaImage, u128, u128) {
    let background_start = Instant::now();
    let mut img = RgbaImage::from_pixel(opts.width, opts.height, canvas_background.to_image_rgba());
    if matches!(opts.domain_frame, Some(frame) if frame.clear_outside)
        && domain_frame_rect.is_some()
    {
        let rect = domain_frame_rect.expect("checked is_some above");
        for py in rect.min_y..=rect.max_y.min(layout.map_h.saturating_sub(1)) {
            for px in rect.min_x..=rect.max_x.min(layout.map_w.saturating_sub(1)) {
                img.put_pixel(
                    layout.map_x + px,
                    layout.map_y + py,
                    map_background.to_image_rgba(),
                );
            }
        }
    } else {
        let map_right = layout.map_x.saturating_add(layout.map_w).min(img.width());
        let map_bottom = layout.map_y.saturating_add(layout.map_h).min(img.height());
        for py in layout.map_y..map_bottom {
            for px in layout.map_x..map_right {
                img.put_pixel(px, py, map_background.to_image_rgba());
            }
        }
    }
    let background_ms = background_start.elapsed().as_millis();

    let polygon_start = Instant::now();
    if let Some(extent) = extent {
        draw_projected_polygons(
            &mut img,
            layout,
            extent,
            &opts.projected_polygons,
            opts.presentation,
            Some(polygon_clip_rect),
        );
    }
    let polygon_fill_ms = polygon_start.elapsed().as_millis();
    (img, background_ms, polygon_fill_ms)
}

fn cached_static_base_image(
    opts: &RenderOpts,
    layout: &Layout,
    extent: Option<&MapExtent>,
    domain_frame_rect: Option<LocalRect>,
    canvas_background: Rgba,
    map_background: Rgba,
    polygon_clip_rect: (i32, i32, i32, i32),
) -> (RgbaImage, u128, u128) {
    let static_base_key = static_base_cache_key(
        opts,
        layout,
        extent,
        domain_frame_rect,
        canvas_background,
        map_background,
    );
    STATIC_BASE_CACHE.with(|cache_cell| {
        let mut cache = cache_cell.borrow_mut();
        if let Some(cached) = cache.as_ref() {
            if cached.key == static_base_key {
                return (cached.image.clone(), 0, 0);
            }
        }

        let (image, background_ms, polygon_fill_ms) = build_static_base_image(
            opts,
            layout,
            extent,
            domain_frame_rect,
            canvas_background,
            map_background,
            polygon_clip_rect,
        );
        *cache = Some(CachedStaticBase {
            key: static_base_key,
            image: image.clone(),
        });
        (image, background_ms, polygon_fill_ms)
    })
}

fn draw_projected_grid_boundary(
    img: &mut RgbaImage,
    layout: &Layout,
    grid: &ProjectedGrid,
    pixel_points: &[Option<(f64, f64)>],
    color: Rgba,
    width: u32,
) -> bool {
    if grid.nx < 2 || grid.ny < 2 || pixel_points.len() != grid.nx * grid.ny {
        return false;
    }

    let idx = |j: usize, i: usize| j * grid.nx + i;
    let mut boundary = Vec::with_capacity((grid.nx + grid.ny) * 2 + 1);
    let mut visible_min_x = f64::INFINITY;
    let mut visible_max_x = f64::NEG_INFINITY;
    let mut visible_min_y = f64::INFINITY;
    let mut visible_max_y = f64::NEG_INFINITY;

    for &(px, py) in pixel_points.iter().flatten() {
        let x = layout.map_x as f64 + px;
        let y = layout.map_y as f64 + py;
        visible_min_x = visible_min_x.min(x);
        visible_max_x = visible_max_x.max(x);
        visible_min_y = visible_min_y.min(y);
        visible_max_y = visible_max_y.max(y);
    }

    for i in 0..grid.nx {
        let Some((px, py)) = pixel_points[idx(0, i)] else {
            return draw_visible_projected_grid_bounds(
                img,
                visible_min_x,
                visible_max_x,
                visible_min_y,
                visible_max_y,
                color,
                width,
            );
        };
        boundary.push((layout.map_x as f64 + px, layout.map_y as f64 + py));
    }
    for j in 1..grid.ny {
        let Some((px, py)) = pixel_points[idx(j, grid.nx - 1)] else {
            return draw_visible_projected_grid_bounds(
                img,
                visible_min_x,
                visible_max_x,
                visible_min_y,
                visible_max_y,
                color,
                width,
            );
        };
        boundary.push((layout.map_x as f64 + px, layout.map_y as f64 + py));
    }
    for i in (0..grid.nx.saturating_sub(1)).rev() {
        let Some((px, py)) = pixel_points[idx(grid.ny - 1, i)] else {
            return draw_visible_projected_grid_bounds(
                img,
                visible_min_x,
                visible_max_x,
                visible_min_y,
                visible_max_y,
                color,
                width,
            );
        };
        boundary.push((layout.map_x as f64 + px, layout.map_y as f64 + py));
    }
    for j in (1..grid.ny.saturating_sub(1)).rev() {
        let Some((px, py)) = pixel_points[idx(j, 0)] else {
            return draw_visible_projected_grid_bounds(
                img,
                visible_min_x,
                visible_max_x,
                visible_min_y,
                visible_max_y,
                color,
                width,
            );
        };
        boundary.push((layout.map_x as f64 + px, layout.map_y as f64 + py));
    }

    if let Some(first) = boundary.first().copied() {
        boundary.push(first);
    }

    if boundary.len() >= 2 {
        draw::draw_polyline_aa(img, &boundary, color, width);
        true
    } else {
        false
    }
}

fn draw_visible_projected_grid_bounds(
    img: &mut RgbaImage,
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
    color: Rgba,
    width: u32,
) -> bool {
    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        return false;
    }
    draw::draw_polyline_aa(
        img,
        &[
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        ],
        color,
        width,
    );
    true
}

fn raster_alpha_bounds(map_img: &RgbaImage) -> Option<(u32, u32, u32, u32)> {
    const OUTLINE_ALPHA_THRESHOLD: u8 = 128;
    let mut min_x: Option<u32> = None;
    let mut max_x: Option<u32> = None;
    let mut min_y: Option<u32> = None;
    let mut max_y: Option<u32> = None;

    for py in 0..map_img.height() {
        for px in 0..map_img.width() {
            if map_img.get_pixel(px, py).0[3] < OUTLINE_ALPHA_THRESHOLD {
                continue;
            }
            min_x = Some(min_x.map_or(px, |v| v.min(px)));
            max_x = Some(max_x.map_or(px, |v| v.max(px)));
            min_y = Some(min_y.map_or(py, |v| v.min(py)));
            max_y = Some(max_y.map_or(py, |v| v.max(py)));
        }
    }

    match (min_x, max_x, min_y, max_y) {
        (Some(min_x), Some(max_x), Some(min_y), Some(max_y)) => Some((min_x, max_x, min_y, max_y)),
        _ => None,
    }
}

fn inset_rect(bounds: LocalRect, inset: u32) -> Option<LocalRect> {
    let LocalRect {
        min_x,
        max_x,
        min_y,
        max_y,
    } = bounds;
    if max_x <= min_x.saturating_add(inset.saturating_mul(2))
        || max_y <= min_y.saturating_add(inset.saturating_mul(2))
    {
        return None;
    }
    Some(LocalRect {
        min_x: min_x + inset,
        max_x: max_x - inset,
        min_y: min_y + inset,
        max_y: max_y - inset,
    })
}

fn build_rect_clip_mask(map_w: u32, map_h: u32, rect: LocalRect) -> RgbaImage {
    let mut mask = RgbaImage::new(map_w, map_h);
    for py in rect.min_y..=rect.max_y.min(map_h.saturating_sub(1)) {
        for px in rect.min_x..=rect.max_x.min(map_w.saturating_sub(1)) {
            mask.put_pixel(px, py, Rgba::WHITE.to_image_rgba());
        }
    }
    mask
}

fn draw_local_rect_outline(
    img: &mut RgbaImage,
    layout: &Layout,
    rect: LocalRect,
    color: Rgba,
    width: u32,
) {
    draw::draw_polyline_aa(
        img,
        &[
            (
                layout.map_x as f64 + rect.min_x as f64,
                layout.map_y as f64 + rect.min_y as f64,
            ),
            (
                layout.map_x as f64 + rect.max_x as f64,
                layout.map_y as f64 + rect.min_y as f64,
            ),
            (
                layout.map_x as f64 + rect.max_x as f64,
                layout.map_y as f64 + rect.max_y as f64,
            ),
            (
                layout.map_x as f64 + rect.min_x as f64,
                layout.map_y as f64 + rect.max_y as f64,
            ),
            (
                layout.map_x as f64 + rect.min_x as f64,
                layout.map_y as f64 + rect.min_y as f64,
            ),
        ],
        color,
        width,
    );
}

fn covered(mask: &RgbaImage, x: u32, y: u32) -> bool {
    mask.get_pixel(x, y).0[3] > 0
}

fn row_coverage_count(mask: &RgbaImage, y: u32, x0: u32, x1: u32) -> u32 {
    (x0..=x1).filter(|&x| covered(mask, x, y)).count() as u32
}

fn col_coverage_count(mask: &RgbaImage, x: u32, y0: u32, y1: u32) -> u32 {
    (y0..=y1).filter(|&y| covered(mask, x, y)).count() as u32
}

fn inner_rect_from_coverage(mask: &RgbaImage, inset: u32) -> Option<LocalRect> {
    let (bx0, bx1, by0, by1) = raster_alpha_bounds(mask)?;
    let mut rect = LocalRect::from_bounds((bx0, bx1, by0, by1));
    const EDGE_COVERAGE_NUM: u32 = 9;
    const EDGE_COVERAGE_DEN: u32 = 10;

    for _ in 0..3 {
        let width = rect.width();
        let min_row_coverage = ((width * EDGE_COVERAGE_NUM) / EDGE_COVERAGE_DEN).max(1);
        let top = (rect.min_y..=rect.max_y)
            .find(|&y| row_coverage_count(mask, y, rect.min_x, rect.max_x) >= min_row_coverage)?;
        let bottom = (rect.min_y..=rect.max_y)
            .rev()
            .find(|&y| row_coverage_count(mask, y, rect.min_x, rect.max_x) >= min_row_coverage)?;
        rect.min_y = top;
        rect.max_y = bottom;
        if rect.min_y >= rect.max_y {
            return None;
        }

        let height = rect.max_y.saturating_sub(rect.min_y).saturating_add(1);
        let min_col_coverage = ((height * EDGE_COVERAGE_NUM) / EDGE_COVERAGE_DEN).max(1);
        let left = (rect.min_x..=rect.max_x)
            .find(|&x| col_coverage_count(mask, x, rect.min_y, rect.max_y) >= min_col_coverage)?;
        let right = (rect.min_x..=rect.max_x)
            .rev()
            .find(|&x| col_coverage_count(mask, x, rect.min_y, rect.max_y) >= min_col_coverage)?;
        rect.min_x = left;
        rect.max_x = right;
        if rect.min_x >= rect.max_x {
            return None;
        }
    }

    inset_rect(rect, inset)
}

fn compute_projected_domain_frame_rect(
    frame: DomainFrame,
    grid: &ProjectedGrid,
    pixel_points: &[Option<(f64, f64)>],
    map_w: u32,
    map_h: u32,
) -> Option<LocalRect> {
    let mask =
        rasterize::rasterize_projected_coverage_mask(grid.ny, grid.nx, pixel_points, map_w, map_h);
    inner_rect_from_coverage(&mask, frame.inset_px)
}

fn scale_render_opts_for_supersample(opts: &RenderOpts, factor: u32) -> RenderOpts {
    let factor = factor.max(1);
    let mut scaled = opts.clone();
    scaled.width = scaled.width.saturating_mul(factor);
    scaled.height = scaled.height.saturating_mul(factor);
    if let Some(frame) = scaled.domain_frame.as_mut() {
        frame.inset_px = frame.inset_px.saturating_mul(factor);
        frame.outline_width = frame.outline_width.max(1).saturating_mul(factor);
    }
    match &mut scaled.chrome_scale {
        ChromeScale::Fixed(scale) => *scale *= factor as f32,
        ChromeScale::Auto { .. } => {}
    }
    for line in &mut scaled.projected_lines {
        line.width = line.width.max(1).saturating_mul(factor);
    }
    for point in &mut scaled.projected_points {
        point.radius_px = point.radius_px.max(1).saturating_mul(factor);
        point.width_px = point.width_px.max(1).saturating_mul(factor);
    }
    for place_label in &mut scaled.projected_place_labels {
        place_label.style.marker_radius_px =
            place_label.style.marker_radius_px.saturating_mul(factor);
        place_label.style.marker_outline_width = place_label
            .style
            .marker_outline_width
            .saturating_mul(factor);
        place_label.style.label_halo_width_px =
            place_label.style.label_halo_width_px.saturating_mul(factor);
        place_label.style.label_scale = place_label.style.label_scale.max(1).saturating_mul(factor);
        place_label.style.label_offset_x_px = place_label
            .style
            .label_offset_x_px
            .saturating_mul(factor as i32);
        place_label.style.label_offset_y_px = place_label
            .style
            .label_offset_y_px
            .saturating_mul(factor as i32);
    }
    for contour in &mut scaled.contours {
        contour.width = contour.width.max(1).saturating_mul(factor);
    }
    for barb in &mut scaled.barbs {
        barb.width = barb.width.max(1).saturating_mul(factor);
        barb.length_px *= factor as f64;
    }
    scaled.supersample_factor = 1;
    scaled
}

fn clear_map_outside_local_rect(
    img: &mut RgbaImage,
    layout: &Layout,
    rect: LocalRect,
    clear_color: Rgba,
) {
    let x0 = layout.map_x + rect.min_x;
    let x1 = layout.map_x + rect.max_x;
    let y0 = layout.map_y + rect.min_y;
    let y1 = layout.map_y + rect.max_y;

    let clear = clear_color.to_image_rgba();
    let map_right = layout.map_x.saturating_add(layout.map_w).min(img.width());
    let map_bottom = layout.map_y.saturating_add(layout.map_h).min(img.height());

    for py in layout.map_y..map_bottom {
        for px in layout.map_x..map_right {
            if px < x0 || px > x1 || py < y0 || py > y1 {
                img.put_pixel(px, py, clear);
            }
        }
    }
}

fn chrome_anchor_bounds(
    layout: &Layout,
    frame: Option<DomainFrame>,
    frame_rect: Option<LocalRect>,
) -> (u32, u32, u32) {
    if matches!(frame, Some(frame) if frame.chrome_follows_frame) {
        if let Some(rect) = frame_rect {
            let left = layout.map_x + rect.min_x;
            let right = layout.map_x + rect.max_x;
            let center = left + right.saturating_sub(left) / 2;
            return (left, right, center);
        }
    }

    let left = layout.map_x;
    let right = layout.map_x + layout.map_w;
    let center = left + right.saturating_sub(left) / 2;
    (left, right, center)
}

fn chrome_anchor_rows(
    layout: &Layout,
    frame: Option<DomainFrame>,
    frame_rect: Option<LocalRect>,
) -> (u32, u32) {
    if matches!(frame, Some(frame) if frame.chrome_follows_frame) {
        if let Some(rect) = frame_rect {
            let frame_top = layout.map_y + rect.min_y;
            let row_h = text::bold_line_height(layout.text_scale)
                .max(text::regular_line_height(layout.text_scale));
            let row_gap = 8u32.saturating_mul(layout.text_scale.max(1));
            let row_y = frame_top.saturating_sub(row_h.saturating_add(row_gap));
            return (row_y, row_y);
        }
    }

    (layout.title_y, layout.subtitle_y)
}

fn joined_subtitle_metadata(opts: &RenderOpts) -> Option<String> {
    let mut parts = Vec::with_capacity(3);
    if let Some(text) = opts
        .subtitle_left
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        parts.push(text);
    }
    if let Some(text) = opts
        .subtitle_center
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        parts.push(text);
    }
    if let Some(text) = opts
        .subtitle_right
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        parts.push(text);
    }

    (!parts.is_empty()).then(|| parts.join(" | "))
}

fn colorbar_anchor_rect(
    layout: &Layout,
    frame: Option<DomainFrame>,
    frame_rect: Option<LocalRect>,
) -> (u32, u32, u32) {
    let mut cbar_x = layout.cbar_x;
    let mut cbar_y = layout.cbar_y;
    let mut cbar_w = layout.cbar_w;

    if matches!(frame, Some(frame) if frame.legend_follows_frame) {
        if let Some(rect) = frame_rect {
            let domain_left = layout.map_x + rect.min_x;
            let domain_right = layout.map_x + rect.max_x;
            let domain_bottom = layout.map_y + rect.max_y;
            let baseline_margin = layout.cbar_x.saturating_sub(layout.map_x);
            let margin = baseline_margin.min(rect.width().saturating_sub(1) / 4);

            cbar_x = domain_left.saturating_add(margin);
            cbar_w = domain_right
                .saturating_sub(domain_left)
                .saturating_sub(margin.saturating_mul(2))
                .max(1);
            cbar_y = domain_bottom
                .saturating_add(layout.label_gap)
                .saturating_add(8)
                .min(layout.cbar_y);
        }
    }

    (cbar_x, cbar_y, cbar_w)
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

fn levels_are_sorted_finite(levels: &[f64]) -> bool {
    levels.iter().all(|value| value.is_finite()) && levels.windows(2).all(|w| w[0] <= w[1])
}

fn lower_bound(levels: &[f64], target: f64) -> usize {
    let mut lo = 0usize;
    let mut hi = levels.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if levels[mid] < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn upper_bound(levels: &[f64], target: f64) -> usize {
    let mut lo = 0usize;
    let mut hi = levels.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if levels[mid] <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn finite_minmax_4(v0: f64, v1: f64, v2: f64, v3: f64) -> Option<(f64, f64)> {
    let mut min_v = f64::INFINITY;
    let mut max_v = f64::NEG_INFINITY;
    let mut finite_count = 0usize;
    for value in [v0, v1, v2, v3] {
        if value.is_finite() {
            min_v = min_v.min(value);
            max_v = max_v.max(value);
            finite_count += 1;
        }
    }
    if finite_count >= 2 {
        Some((min_v, max_v))
    } else {
        None
    }
}

fn contour_cell_corners(
    layout: &Layout,
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    base: usize,
) -> Option<((f64, f64), (f64, f64), (f64, f64), (f64, f64))> {
    if let Some(points) = pixel_points {
        match (
            points[base],
            points[base + 1],
            points[base + overlay.nx + 1],
            points[base + overlay.nx],
        ) {
            (Some(a), Some(b), Some(c), Some(d)) => Some((a, b, c, d)),
            _ => None,
        }
    } else {
        let i = base % overlay.nx;
        let j = base / overlay.nx;
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
    }
}

fn emit_interp_point(
    pts: &mut [(f64, f64); 4],
    count: &mut usize,
    a: (f64, f64, f64),
    b: (f64, f64, f64),
    level: f64,
) {
    if let Some(point) = interp_point(a, b, level) {
        pts[*count] = point;
        *count += 1;
    }
}

fn contour_cell_intersections(
    layout: &Layout,
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    base: usize,
    level: f64,
) -> Option<([(f64, f64); 4], usize)> {
    let (c0, c1, c2, c3) = contour_cell_corners(layout, overlay, pixel_points, base)?;
    let p0 = (c0.0, c0.1, overlay.data[base]);
    let p1 = (c1.0, c1.1, overlay.data[base + 1]);
    let p2 = (c2.0, c2.1, overlay.data[base + overlay.nx + 1]);
    let p3 = (c3.0, c3.1, overlay.data[base + overlay.nx]);

    let mut pts = [(0.0, 0.0); 4];
    let mut count = 0usize;
    emit_interp_point(&mut pts, &mut count, p0, p1, level);
    emit_interp_point(&mut pts, &mut count, p1, p2, level);
    emit_interp_point(&mut pts, &mut count, p2, p3, level);
    emit_interp_point(&mut pts, &mut count, p3, p0, level);

    if count >= 2 { Some((pts, count)) } else { None }
}

fn maybe_draw_contour_label(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    level: f64,
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
    label_drawn: &mut bool,
) {
    if *label_drawn || (x1 - x0).abs() + (y1 - y0).abs() <= 18.0 {
        return;
    }

    let label = text::format_tick(level);
    let tx =
        (layout.map_x as f64 + (x0 + x1) * 0.5) as i32 - text::text_width(&label, 1) as i32 / 2;
    let ty = (layout.map_y as f64 + (y0 + y1) * 0.5) as i32 - 4;
    text::draw_text(img, &label, tx, ty, overlay.color, 1);
    *label_drawn = true;
}

fn draw_contour_segments_unmasked(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    level: f64,
    pts: &[(f64, f64); 4],
    count: usize,
    label_drawn: &mut bool,
) {
    let segments: &[(usize, usize)] = if count == 4 {
        &[(0, 1), (2, 3)]
    } else {
        &[(0, 1)]
    };

    for &(a, b) in segments {
        let (x0, y0) = pts[a];
        let (x1, y1) = pts[b];
        draw::draw_line_aa_width(
            img,
            layout.map_x as f64 + x0,
            layout.map_y as f64 + y0,
            layout.map_x as f64 + x1,
            layout.map_y as f64 + y1,
            overlay.color,
            overlay.width,
        );
        maybe_draw_contour_label(img, layout, overlay, level, x0, y0, x1, y1, label_drawn);
    }
}

fn draw_contour_segments_masked(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    level: f64,
    pts: &[(f64, f64); 4],
    count: usize,
    mask: &RgbaImage,
    label_drawn: &mut bool,
) {
    let segments: &[(usize, usize)] = if count == 4 {
        &[(0, 1), (2, 3)]
    } else {
        &[(0, 1)]
    };

    for &(a, b) in segments {
        let (x0, y0) = pts[a];
        let (x1, y1) = pts[b];
        if !segment_intersects_mask(mask, x0, y0, x1, y1) {
            continue;
        }
        draw::draw_line_aa_width(
            img,
            layout.map_x as f64 + x0,
            layout.map_y as f64 + y0,
            layout.map_x as f64 + x1,
            layout.map_y as f64 + y1,
            overlay.color,
            overlay.width,
        );
        maybe_draw_contour_label(img, layout, overlay, level, x0, y0, x1, y1, label_drawn);
    }
}

fn build_contour_buckets(
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
) -> Vec<Vec<u32>> {
    let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); overlay.levels.len()];
    if overlay.levels.is_empty() {
        return buckets;
    }

    for j in 0..(overlay.ny - 1) {
        let row_base = j * overlay.nx;
        for i in 0..(overlay.nx - 1) {
            let base = row_base + i;
            if let Some(points) = pixel_points {
                if !matches!(
                    (
                        points[base],
                        points[base + 1],
                        points[base + overlay.nx + 1],
                        points[base + overlay.nx]
                    ),
                    (Some(_), Some(_), Some(_), Some(_))
                ) {
                    continue;
                }
            }

            let Some((min_v, max_v)) = finite_minmax_4(
                overlay.data[base],
                overlay.data[base + 1],
                overlay.data[base + overlay.nx + 1],
                overlay.data[base + overlay.nx],
            ) else {
                continue;
            };

            let lo = lower_bound(&overlay.levels, min_v);
            let hi = upper_bound(&overlay.levels, max_v);
            if lo >= hi {
                continue;
            }

            let cell_id = base as u32;
            for bucket in &mut buckets[lo..hi] {
                bucket.push(cell_id);
            }
        }
    }

    buckets
}

fn draw_contours_bucketed(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    clip_mask: Option<&RgbaImage>,
) {
    let buckets = build_contour_buckets(overlay, pixel_points);

    if let Some(mask) = clip_mask {
        for (level_index, &level) in overlay.levels.iter().enumerate() {
            let mut label_drawn = !overlay.labels;
            for &base in &buckets[level_index] {
                let Some((pts, count)) =
                    contour_cell_intersections(layout, overlay, pixel_points, base as usize, level)
                else {
                    continue;
                };
                draw_contour_segments_masked(
                    img,
                    layout,
                    overlay,
                    level,
                    &pts,
                    count,
                    mask,
                    &mut label_drawn,
                );
            }
        }
    } else {
        for (level_index, &level) in overlay.levels.iter().enumerate() {
            let mut label_drawn = !overlay.labels;
            for &base in &buckets[level_index] {
                let Some((pts, count)) =
                    contour_cell_intersections(layout, overlay, pixel_points, base as usize, level)
                else {
                    continue;
                };
                draw_contour_segments_unmasked(
                    img,
                    layout,
                    overlay,
                    level,
                    &pts,
                    count,
                    &mut label_drawn,
                );
            }
        }
    }
}

fn draw_contours_legacy(
    img: &mut RgbaImage,
    layout: &Layout,
    overlay: &ContourOverlay,
    pixel_points: Option<&[Option<(f64, f64)>]>,
    clip_mask: Option<&RgbaImage>,
) {
    for &level in &overlay.levels {
        let mut label_drawn = !overlay.labels;
        for j in 0..(overlay.ny - 1) {
            let row_base = j * overlay.nx;
            for i in 0..(overlay.nx - 1) {
                let base = row_base + i;
                let Some((pts, count)) =
                    contour_cell_intersections(layout, overlay, pixel_points, base, level)
                else {
                    continue;
                };

                if let Some(mask) = clip_mask {
                    draw_contour_segments_masked(
                        img,
                        layout,
                        overlay,
                        level,
                        &pts,
                        count,
                        mask,
                        &mut label_drawn,
                    );
                } else {
                    draw_contour_segments_unmasked(
                        img,
                        layout,
                        overlay,
                        level,
                        &pts,
                        count,
                        &mut label_drawn,
                    );
                }
            }
        }
    }
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

    if levels_are_sorted_finite(&overlay.levels) && overlay.data.len() <= u32::MAX as usize {
        draw_contours_bucketed(img, layout, overlay, pixel_points, clip_mask);
    } else {
        draw_contours_legacy(img, layout, overlay, pixel_points, clip_mask);
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

fn draw_variable_layers(
    img: &mut RgbaImage,
    data: &[f64],
    ny: usize,
    nx: usize,
    opts: &RenderOpts,
    layout: &Layout,
    projected_pixels: Option<&[Option<(f64, f64)>]>,
    domain_frame_rect: Option<LocalRect>,
    polygon_clip_rect: (i32, i32, i32, i32),
    canvas_background: Rgba,
) -> VariableLayerTiming {
    if let Some(ref extent) = opts.map_extent {
        draw_projected_polygons(
            img,
            layout,
            extent,
            &opts.projected_data_polygons,
            opts.presentation,
            Some(polygon_clip_rect),
        );
    }

    let rasterize_start = Instant::now();
    let map_img = if let Some(pixel_points) = projected_pixels {
        rasterize::rasterize_projected_grid(
            data,
            ny,
            nx,
            pixel_points,
            &opts.cmap,
            layout.map_w,
            layout.map_h,
        )
    } else {
        rasterize::rasterize_grid(data, ny, nx, &opts.cmap, layout.map_w, layout.map_h)
    };
    let rasterize_ms = rasterize_start.elapsed().as_millis();

    let domain_clip_rect = domain_frame_rect.or_else(|| {
        opts.presentation
            .domain_boundary
            .and_then(|domain_boundary| {
                if !domain_boundary.visible {
                    return None;
                }
                let inset = domain_boundary.width.saturating_add(3);
                raster_alpha_bounds(&map_img)
                    .map(LocalRect::from_bounds)
                    .and_then(|bounds| inset_rect(bounds, inset))
            })
    });
    let domain_clip_mask =
        domain_clip_rect.map(|rect| build_rect_clip_mask(layout.map_w, layout.map_h, rect));

    let raster_blit_start = Instant::now();
    for py in 0..layout.map_h {
        for px in 0..layout.map_w {
            if let Some(mask) = domain_clip_mask.as_ref() {
                if mask.get_pixel(px, py).0[3] == 0 {
                    continue;
                }
            }
            let src = map_img.get_pixel(px, py);
            let a = src.0[3];
            if a == 0 {
                continue;
            }
            if a == 255 {
                img.put_pixel(layout.map_x + px, layout.map_y + py, *src);
            } else {
                draw::blend_pixel(
                    img,
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
    let raster_blit_ms = raster_blit_start.elapsed().as_millis();

    let linework_start = Instant::now();
    if let Some(ref extent) = opts.map_extent {
        draw_projected_lines(
            img,
            layout,
            extent,
            &opts.projected_lines,
            opts.presentation,
            domain_clip_mask.as_ref(),
        );
    }
    let linework_ms = linework_start.elapsed().as_millis();

    let point_start = Instant::now();
    if let Some(ref extent) = opts.map_extent {
        draw_projected_points(
            img,
            layout,
            extent,
            &opts.projected_points,
            domain_clip_mask.as_ref(),
        );
    }
    let point_ms = point_start.elapsed().as_millis();

    let contour_start = Instant::now();
    for contour in &opts.contours {
        draw_contours(
            img,
            layout,
            contour,
            projected_pixels,
            domain_clip_mask.as_ref(),
        );
    }
    let contour_ms = contour_start.elapsed().as_millis();

    let barb_start = Instant::now();
    for barb in &opts.barbs {
        draw_barbs(
            img,
            layout,
            barb,
            projected_pixels,
            domain_clip_mask.as_ref(),
        );
    }
    let barb_ms = barb_start.elapsed().as_millis();

    let label_start = Instant::now();
    if let Some(ref extent) = opts.map_extent {
        draw_projected_place_labels(
            img,
            layout,
            extent,
            &opts.projected_place_labels,
            domain_clip_mask.as_ref(),
            domain_clip_rect,
        );
    }
    let label_ms = label_start.elapsed().as_millis();

    let outside_frame_clear_start = Instant::now();
    if let (Some(frame), Some(rect)) = (opts.domain_frame, domain_frame_rect) {
        if frame.clear_outside {
            clear_map_outside_local_rect(img, layout, rect, canvas_background);
        }
    }
    let outside_frame_clear_ms = outside_frame_clear_start.elapsed().as_millis();

    VariableLayerTiming {
        rasterize_ms,
        raster_blit_ms,
        linework_ms: linework_ms
            .saturating_add(point_ms)
            .saturating_add(label_ms),
        contour_ms,
        barb_ms,
        outside_frame_clear_ms,
        domain_clip_rect,
    }
}

fn draw_chrome_and_colorbar(
    img: &mut RgbaImage,
    layout: &Layout,
    opts: &RenderOpts,
    projected_pixels_ref: Option<&[Option<(f64, f64)>]>,
    domain_frame_rect: Option<LocalRect>,
    domain_clip_rect: Option<LocalRect>,
    _has_title: bool,
) -> (u128, u128) {
    let chrome_start = Instant::now();
    let (chrome_left, chrome_right, chrome_center) =
        chrome_anchor_bounds(layout, opts.domain_frame, domain_frame_rect);
    let (title_y, subtitle_y) = chrome_anchor_rows(layout, opts.domain_frame, domain_frame_rect);
    let title_color = opts.presentation.chrome.title_color;
    let subtitle_color = opts.presentation.chrome.subtitle_color;
    let metadata = joined_subtitle_metadata(opts);
    let row_gap = 14u32.saturating_mul(layout.text_scale.max(1));
    let row_width = chrome_right.saturating_sub(chrome_left).max(1);
    let metadata_width_limit = if opts.title.is_some() {
        row_width.saturating_mul(2).checked_div(5).unwrap_or(1)
    } else {
        row_width
    }
    .max(1);
    let fitted_metadata = metadata
        .map(|text| ellipsize_text_to_width(&text, metadata_width_limit, layout.text_scale, false));
    let metadata_width = fitted_metadata
        .as_ref()
        .map(|text| text::text_width(text, layout.text_scale) as u32)
        .unwrap_or(0);
    let title_width_limit = row_width
        .saturating_sub(metadata_width)
        .saturating_sub(if metadata_width > 0 { row_gap } else { 0 })
        .max(1);

    if let Some(ref t) = opts.title {
        let title = ellipsize_text_to_width(t, title_width_limit, layout.text_scale, true);
        let title_x = if fitted_metadata.is_none()
            && matches!(opts.presentation.chrome.title_anchor, TitleAnchor::Center)
        {
            centered_text_left(&title, chrome_center, layout.text_scale, true)
        } else {
            chrome_left as i32
        };
        text::draw_text_bold(
            img,
            &title,
            title_x,
            title_y as i32,
            title_color,
            layout.text_scale,
        );
    }
    if let Some(ref metadata) = fitted_metadata {
        text::draw_text_right(
            img,
            metadata,
            chrome_right as i32,
            subtitle_y as i32,
            subtitle_color,
            layout.text_scale,
        );
    }
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

    if let Some(frame) = opts.domain_frame {
        if let Some(rect) = domain_frame_rect {
            draw_local_rect_outline(
                img,
                layout,
                rect,
                frame.outline_color.into(),
                frame.outline_width,
            );
        }
    }

    if let Some(domain_boundary) = opts.presentation.domain_boundary {
        if domain_boundary.visible {
            if let Some(rect) = domain_clip_rect.filter(|_| domain_frame_rect.is_none()) {
                draw_local_rect_outline(
                    img,
                    layout,
                    rect,
                    domain_boundary.color,
                    domain_boundary.width,
                );
            } else {
                let drew_grid_boundary = match (&opts.projected_grid, projected_pixels_ref) {
                    (Some(grid), Some(pixel_points)) => draw_projected_grid_boundary(
                        img,
                        layout,
                        grid,
                        pixel_points,
                        domain_boundary.color,
                        domain_boundary.width,
                    ),
                    _ => false,
                };
                if !drew_grid_boundary {
                    let map_right = layout.map_x + layout.map_w.saturating_sub(1);
                    let map_bottom = layout.map_y + layout.map_h.saturating_sub(1);
                    draw::draw_polyline_aa(
                        img,
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
        }
    }
    let chrome_ms = chrome_start.elapsed().as_millis();

    let colorbar_start = Instant::now();
    if opts.colorbar {
        let (cbar_x, cbar_y, cbar_w) =
            colorbar_anchor_rect(layout, opts.domain_frame, domain_frame_rect);
        colorbar::draw_colorbar(
            img,
            &opts.cmap,
            cbar_x,
            cbar_y,
            cbar_w,
            layout.cbar_h,
            opts.colorbar_mode,
            opts.presentation.colorbar,
        );
        let levels = colorbar_levels_for_ticks(&opts.cmap);
        let ticks = pick_ticks(levels, opts.cbar_tick_step);
        if levels.len() >= 2 {
            let lo = levels[0];
            let hi = levels[levels.len() - 1];
            let range = hi - lo;
            if range > 0.0 {
                let tick_positions: Vec<f64> = ticks.iter().map(|t| (t - lo) / range).collect();
                colorbar::draw_colorbar_ticks(
                    img,
                    cbar_x,
                    cbar_y,
                    cbar_w,
                    &tick_positions,
                    opts.presentation.colorbar.tick_color,
                );
                let tick_y = cbar_y.saturating_sub(layout.label_gap) as i32;
                let label_color = opts.presentation.colorbar.label_color;
                for (_, lx, label) in filter_tick_labels_to_fit(
                    &ticks,
                    lo,
                    range,
                    cbar_x,
                    cbar_w,
                    cbar_x,
                    cbar_x.saturating_add(cbar_w),
                    img.width(),
                    layout.text_scale,
                ) {
                    text::draw_text(img, &label, lx, tick_y, label_color, layout.text_scale);
                }
            }
        }
    }
    let colorbar_ms = colorbar_start.elapsed().as_millis();

    (chrome_ms, colorbar_ms)
}

fn render_to_image_profile_inner(
    data: &[f64],
    ny: usize,
    nx: usize,
    opts: &RenderOpts,
) -> (RgbaImage, RenderImageTiming) {
    let total_start = Instant::now();
    let layout_start = Instant::now();
    let has_title = opts.title.is_some()
        || opts.subtitle_left.is_some()
        || opts.subtitle_center.is_some()
        || opts.subtitle_right.is_some();
    let layout = compute_layout(
        opts.width,
        opts.height,
        opts.colorbar,
        has_title,
        opts.presentation,
        opts.chrome_scale,
    );
    let layout_ms = layout_start.elapsed().as_millis();

    let projected_pixel_start = Instant::now();
    let projected_pixels = match (&opts.projected_grid, &opts.map_extent) {
        (Some(grid), Some(extent)) if grid.nx == nx && grid.ny == ny => {
            Some(projected_grid_to_pixels_cached(grid, extent, &layout))
        }
        _ => None,
    };
    let projected_pixel_ms = projected_pixel_start.elapsed().as_millis();
    let domain_frame_rect = match (
        opts.domain_frame,
        opts.projected_grid.as_ref(),
        projected_pixels.as_deref(),
    ) {
        (Some(frame), Some(grid), Some(pixel_points)) => compute_projected_domain_frame_rect(
            frame,
            grid,
            pixel_points,
            layout.map_w,
            layout.map_h,
        ),
        _ => None,
    };
    let polygon_clip_rect = domain_frame_rect
        .filter(|_| matches!(opts.domain_frame, Some(frame) if frame.clear_outside))
        .map(|rect| {
            (
                (layout.map_x + rect.min_x) as i32,
                (layout.map_y + rect.min_y) as i32,
                (layout.map_x + rect.max_x) as i32,
                (layout.map_y + rect.max_y) as i32,
            )
        })
        .unwrap_or_else(|| {
            let map_right = layout.map_x.saturating_add(layout.map_w).saturating_sub(1) as i32;
            let map_bottom = layout.map_y.saturating_add(layout.map_h).saturating_sub(1) as i32;
            (
                layout.map_x as i32,
                layout.map_y as i32,
                map_right,
                map_bottom,
            )
        });

    let canvas_background = if opts.background == Rgba::WHITE {
        opts.presentation.canvas_background
    } else {
        opts.background
    };
    let map_background = if opts.background == Rgba::WHITE {
        opts.presentation.map_background
    } else {
        opts.background
    };
    let (mut img, background_ms, polygon_fill_ms) = cached_static_base_image(
        opts,
        &layout,
        opts.map_extent.as_ref(),
        domain_frame_rect,
        canvas_background,
        map_background,
        polygon_clip_rect,
    );
    let variable_timing = draw_variable_layers(
        &mut img,
        data,
        ny,
        nx,
        opts,
        &layout,
        projected_pixels.as_deref(),
        domain_frame_rect,
        polygon_clip_rect,
        canvas_background,
    );
    let (chrome_ms, colorbar_ms) = draw_chrome_and_colorbar(
        &mut img,
        &layout,
        opts,
        projected_pixels.as_deref(),
        domain_frame_rect,
        variable_timing.domain_clip_rect,
        has_title,
    );

    let timing = RenderImageTiming {
        layout_ms,
        background_ms,
        polygon_fill_ms,
        projected_pixel_ms,
        rasterize_ms: variable_timing.rasterize_ms,
        raster_blit_ms: variable_timing.raster_blit_ms,
        linework_ms: variable_timing.linework_ms,
        contour_ms: variable_timing.contour_ms,
        barb_ms: variable_timing.barb_ms,
        outside_frame_clear_ms: variable_timing.outside_frame_clear_ms,
        chrome_ms,
        colorbar_ms,
        downsample_ms: 0,
        postprocess_ms: 0,
        total_ms: total_start.elapsed().as_millis(),
    };

    (img, timing)
}

pub fn render_to_image_profile(
    data: &[f64],
    ny: usize,
    nx: usize,
    opts: &RenderOpts,
) -> (RgbaImage, RenderImageTiming) {
    let factor = opts.supersample_factor.max(1);
    if factor == 1 {
        return render_to_image_profile_inner(data, ny, nx, opts);
    }

    let total_start = Instant::now();
    let scaled_opts = scale_render_opts_for_supersample(opts, factor);
    let (hires, mut timing) = render_to_image_profile_inner(data, ny, nx, &scaled_opts);
    let downsample_start = Instant::now();
    let image = resize(&hires, opts.width, opts.height, FilterType::Lanczos3);
    let image = sharpen_downsampled_image(&image);
    timing.downsample_ms = downsample_start.elapsed().as_millis();
    timing.postprocess_ms = timing.downsample_ms;
    timing.total_ms = total_start.elapsed().as_millis();
    (image, timing)
}

fn sharpen_downsampled_image(image: &RgbaImage) -> RgbaImage {
    filter3x3(
        image,
        &[0.0, -0.22, 0.0, -0.22, 1.88, -0.22, 0.0, -0.22, 0.0],
    )
}

pub fn render_to_image(data: &[f64], ny: usize, nx: usize, opts: &RenderOpts) -> RgbaImage {
    render_to_image_profile(data, ny, nx, opts).0
}

fn row_is_canvas_background(img: &RgbaImage, y: u32, background: Rgba) -> bool {
    let bg = background.to_image_rgba().0;
    (0..img.width()).all(|x| {
        let px = img.get_pixel(x, y).0;
        let diff = px[0].abs_diff(bg[0]) as u16
            + px[1].abs_diff(bg[1]) as u16
            + px[2].abs_diff(bg[2]) as u16
            + px[3].abs_diff(bg[3]) as u16;
        diff <= 6
    })
}

pub(crate) fn trim_vertical_canvas_whitespace(img: &RgbaImage, background: Rgba) -> RgbaImage {
    if img.height() <= 2 {
        return img.clone();
    }

    let first_non_bg = (0..img.height()).find(|&y| !row_is_canvas_background(img, y, background));
    let last_non_bg = (0..img.height()).rfind(|&y| !row_is_canvas_background(img, y, background));

    let (Some(first), Some(last)) = (first_non_bg, last_non_bg) else {
        return img.clone();
    };

    let top_pad = 2u32;
    let bottom_pad = 2u32;
    let crop_top = first.saturating_sub(top_pad);
    let crop_bottom = (last.saturating_add(bottom_pad)).min(img.height().saturating_sub(1));
    let crop_h = crop_bottom.saturating_sub(crop_top).saturating_add(1);
    if crop_top == 0 && crop_h == img.height() {
        return img.clone();
    }

    crop_imm(img, 0, crop_top, img.width(), crop_h).to_image()
}

pub fn encode_rgba_png_profile_with_options(
    image: &RgbaImage,
    options: &PngWriteOptions,
) -> (Vec<u8>, u128) {
    let encode_start = Instant::now();
    let compression = match options.compression {
        PngCompressionMode::Default => CompressionType::Default,
        PngCompressionMode::Fast => CompressionType::Fast,
        PngCompressionMode::Fastest => CompressionType::Best,
    };
    let mut buf = Vec::new();
    let encoder = PngEncoder::new_with_quality(&mut buf, compression, PngFilterType::Adaptive);
    encoder
        .write_image(
            image.as_raw(),
            image.width(),
            image.height(),
            ExtendedColorType::Rgba8,
        )
        .expect("PNG encoding failed");
    (buf, encode_start.elapsed().as_millis())
}

pub fn render_to_png_profile_with_options(
    data: &[f64],
    ny: usize,
    nx: usize,
    opts: &RenderOpts,
    png_options: &PngWriteOptions,
) -> (Vec<u8>, RenderPngTiming) {
    let total_start = Instant::now();
    let (image, image_timing) = render_to_image_profile(data, ny, nx, opts);
    let render_to_image_ms = image_timing.total_ms;
    let (buf, png_encode_ms) = encode_rgba_png_profile_with_options(&image, png_options);
    let timing = RenderPngTiming {
        image_timing,
        render_to_image_ms,
        png_encode_ms,
        png_write_ms: 0,
        total_ms: total_start.elapsed().as_millis(),
    };
    (buf, timing)
}

pub fn render_to_png_profile(
    data: &[f64],
    ny: usize,
    nx: usize,
    opts: &RenderOpts,
) -> (Vec<u8>, RenderPngTiming) {
    render_to_png_profile_with_options(data, ny, nx, opts, &PngWriteOptions::default())
}

pub fn render_to_png(data: &[f64], ny: usize, nx: usize, opts: &RenderOpts) -> Vec<u8> {
    render_to_png_profile(data, ny, nx, opts).0
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
    use crate::colormap::{ColormapBuildOptions, Extend, LevelDensity};

    fn sample_cmap() -> LeveledColormap {
        LeveledColormap::from_palette(
            &[Rgba::new(0, 0, 255), Rgba::new(255, 0, 0)],
            &[0.0, 1.0, 2.0, 3.0],
            Extend::Neither,
            None,
        )
    }

    fn sample_masked_cmap() -> LeveledColormap {
        LeveledColormap::from_palette(
            &[Rgba::new(0, 0, 255), Rgba::new(255, 0, 0)],
            &[10.0, 20.0, 30.0],
            Extend::Neither,
            Some(10.0),
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
            subtitle_center: None,
            subtitle_right: None,
            cbar_tick_step: None,
            colorbar_mode: crate::colormap::LegendMode::Stepped,
            chrome_scale: ChromeScale::default(),
            supersample_factor: 1,
            domain_frame: None,
            map_extent: Some(MapExtent {
                x_min: 0.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 1.0,
            }),
            projected_grid: Some(sample_projected_grid()),
            projected_polygons: Vec::new(),
            projected_data_polygons: Vec::new(),
            projected_place_labels: Vec::new(),
            projected_points: Vec::new(),
            projected_lines: Vec::new(),
            contours: Vec::new(),
            barbs: Vec::new(),
            presentation: RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
        }
    }

    fn sample_place_label() -> ProjectedPlaceLabelOverlay {
        ProjectedPlaceLabelOverlay {
            x: 0.52,
            y: 0.48,
            label: Some("Sacramento".into()),
            priority: ProjectedPlaceLabelPriority::Primary,
            style: crate::overlay::ProjectedPlaceLabelStyle {
                marker_radius_px: 4,
                marker_fill: Rgba::with_alpha(255, 255, 255, 235),
                marker_outline: Rgba::with_alpha(24, 28, 34, 240),
                marker_outline_width: 1,
                label_color: Rgba::BLACK,
                label_halo: Rgba::with_alpha(255, 255, 255, 235),
                label_halo_width_px: 2,
                label_scale: 1,
                label_offset_x_px: 6,
                label_offset_y_px: -2,
                label_placement: ProjectedLabelPlacement::AboveRight,
                label_bold: true,
            },
        }
    }

    fn contour_test_layout() -> Layout {
        Layout {
            map_x: 0,
            map_y: 0,
            map_w: 64,
            map_h: 64,
            cbar_x: 0,
            cbar_y: 0,
            cbar_w: 0,
            cbar_h: 0,
            title_y: 0,
            subtitle_y: 0,
            text_scale: 1,
            label_gap: 14,
        }
    }

    fn blank_test_image() -> RgbaImage {
        RgbaImage::from_pixel(80, 80, Rgba::WHITE.to_image_rgba())
    }

    fn non_white_bounds(img: &RgbaImage) -> Option<(u32, u32, u32, u32)> {
        let mut min_x = u32::MAX;
        let mut max_x = 0u32;
        let mut min_y = u32::MAX;
        let mut max_y = 0u32;
        let mut found = false;

        for (x, y, pixel) in img.enumerate_pixels() {
            if pixel.0 == [255, 255, 255, 255] {
                continue;
            }
            found = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }

        found.then_some((min_x, max_x, min_y, max_y))
    }

    fn sample_domain_frame(outline_color: crate::request::Color) -> DomainFrame {
        DomainFrame {
            inset_px: 5,
            outline_color,
            outline_width: 2,
            clear_outside: true,
            legend_follows_frame: true,
            chrome_follows_frame: true,
        }
    }

    fn visit_rs_files(
        root: &std::path::Path,
        visitor: &mut impl FnMut(&std::path::Path),
    ) -> std::io::Result<()> {
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_rs_files(&path, visitor)?;
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                visitor(&path);
            }
        }
        Ok(())
    }

    #[test]
    fn supersample_scaling_expands_overlay_dimensions() {
        let mut opts = sample_projected_opts();
        opts.projected_lines = vec![ProjectedPolyline {
            points: vec![(0.0, 0.0), (1.0, 1.0)],
            color: Rgba::BLACK,
            width: 2,
            role: crate::presentation::LineworkRole::Generic,
        }];
        opts.projected_points = vec![ProjectedPointOverlay {
            x: 0.50,
            y: 0.50,
            color: Rgba::new(255, 80, 40),
            radius_px: 5,
            width_px: 2,
            shape: ProjectedMarkerShape::Plus,
        }];
        opts.contours = vec![ContourOverlay {
            data: vec![500.0, 504.0, 508.0, 512.0],
            ny: 2,
            nx: 2,
            levels: vec![504.0],
            color: Rgba::BLACK,
            width: 1,
            labels: false,
            show_extrema: false,
        }];
        opts.barbs = vec![BarbOverlay {
            u: vec![10.0, 10.0, 10.0, 10.0],
            v: vec![0.0, 0.0, 0.0, 0.0],
            ny: 2,
            nx: 2,
            stride_x: 1,
            stride_y: 1,
            color: Rgba::BLACK,
            width: 1,
            length_px: 18.0,
        }];
        opts.projected_place_labels = vec![sample_place_label()];
        opts.domain_frame = Some(sample_domain_frame(crate::request::Color::BLACK));

        let scaled = scale_render_opts_for_supersample(&opts, 2);
        assert_eq!(scaled.width, opts.width * 2);
        assert_eq!(scaled.height, opts.height * 2);
        assert_eq!(scaled.projected_lines[0].width, 4);
        assert_eq!(scaled.projected_points[0].radius_px, 10);
        assert_eq!(scaled.projected_points[0].width_px, 4);
        assert_eq!(scaled.projected_place_labels[0].style.marker_radius_px, 8);
        assert_eq!(scaled.projected_place_labels[0].style.label_scale, 2);
        assert_eq!(scaled.projected_place_labels[0].style.label_offset_x_px, 12);
        assert_eq!(scaled.contours[0].width, 2);
        assert_eq!(scaled.barbs[0].width, 2);
        assert_eq!(scaled.barbs[0].length_px, 36.0);
        assert_eq!(scaled.domain_frame.unwrap().outline_width, 4);
        assert_eq!(scaled.supersample_factor, 1);
    }

    #[test]
    fn render_to_image_supersample_preserves_requested_dimensions() {
        let mut opts = sample_projected_opts();
        opts.supersample_factor = 2;
        let data = vec![10.0, 20.0, 30.0, 25.0];
        let (image, timing) = render_to_image_profile(&data, 2, 2, &opts);
        assert_eq!(image.width(), opts.width);
        assert_eq!(image.height(), opts.height);
        assert!(timing.postprocess_ms <= timing.total_ms);
    }

    #[test]
    fn projected_place_labels_render_visible_marker_and_text() {
        let mut opts = sample_projected_opts();
        opts.projected_place_labels = vec![sample_place_label()];
        let data = vec![0.5, 1.0, 1.5, 2.0];

        let image = render_to_image(&data, 2, 2, &opts);
        let dark_pixels = image
            .pixels()
            .filter(|pixel| pixel.0[0] < 80 && pixel.0[1] < 80 && pixel.0[2] < 80)
            .count();
        let bright_pixels = image
            .pixels()
            .filter(|pixel| pixel.0[0] > 220 && pixel.0[1] > 220 && pixel.0[2] > 220)
            .count();

        assert!(
            dark_pixels > 50,
            "label text and marker outline should be visible"
        );
        assert!(
            bright_pixels > 200,
            "marker fill and halo should be visible"
        );
    }

    #[test]
    fn projected_points_render_visible_marker() {
        let mut opts = sample_projected_opts();
        opts.projected_points = vec![ProjectedPointOverlay {
            x: 0.50,
            y: 0.50,
            color: Rgba::new(255, 50, 20),
            radius_px: 8,
            width_px: 2,
            shape: ProjectedMarkerShape::Plus,
        }];
        let data = vec![0.5, 1.0, 1.5, 2.0];

        let image = render_to_image(&data, 2, 2, &opts);
        let red_pixels = image
            .pixels()
            .filter(|pixel| pixel.0[0] > 200 && pixel.0[1] < 100 && pixel.0[2] < 100)
            .count();

        assert!(red_pixels > 15, "projected point marker should be visible");
    }

    #[test]
    fn projected_place_labels_clamp_text_inside_requested_clip_rect() {
        let presentation = RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology);
        let layout = compute_layout(240, 160, false, false, presentation, ChromeScale::default());
        let extent = MapExtent {
            x_min: 0.0,
            x_max: 1.0,
            y_min: 0.0,
            y_max: 1.0,
        };
        let clip_rect = LocalRect {
            min_x: 20,
            max_x: 80,
            min_y: 20,
            max_y: 60,
        };
        let local_x = clip_rect.max_x.saturating_sub(2) as f64;
        let local_y = clip_rect.max_y.saturating_sub(2) as f64;
        let mut style = sample_place_label().style;
        style.marker_radius_px = 0;
        style.marker_outline_width = 0;
        style.label_halo = Rgba::TRANSPARENT;
        style.label_halo_width_px = 0;
        style.label_offset_x_px = 28;
        style.label_offset_y_px = 14;
        style.label_placement = ProjectedLabelPlacement::BelowRight;
        let label = ProjectedPlaceLabelOverlay {
            x: local_x / layout.map_w.saturating_sub(1) as f64,
            y: 1.0 - (local_y / layout.map_h.saturating_sub(1) as f64),
            label: Some("Sacramento Valley".into()),
            priority: ProjectedPlaceLabelPriority::Primary,
            style,
        };
        let mut img = RgbaImage::from_pixel(240, 160, Rgba::WHITE.to_image_rgba());

        draw_projected_place_labels(&mut img, &layout, &extent, &[label], None, Some(clip_rect));

        let (min_x, max_x, min_y, max_y) =
            non_white_bounds(&img).expect("clipped place label should still render");
        assert!(min_x >= layout.map_x + clip_rect.min_x);
        assert!(max_x <= layout.map_x + clip_rect.max_x);
        assert!(min_y >= layout.map_y + clip_rect.min_y);
        assert!(max_y <= layout.map_y + clip_rect.max_y);
    }

    #[test]
    fn projected_place_labels_skip_marker_and_text_outside_requested_clip_mask() {
        let presentation = RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology);
        let layout = compute_layout(240, 160, false, false, presentation, ChromeScale::default());
        let extent = MapExtent {
            x_min: 0.0,
            x_max: 1.0,
            y_min: 0.0,
            y_max: 1.0,
        };
        let clip_mask = RgbaImage::from_pixel(
            layout.map_w,
            layout.map_h,
            Rgba::TRANSPARENT.to_image_rgba(),
        );
        let mut img = RgbaImage::from_pixel(240, 160, Rgba::WHITE.to_image_rgba());

        draw_projected_place_labels(
            &mut img,
            &layout,
            &extent,
            &[sample_place_label()],
            Some(&clip_mask),
            None,
        );

        assert!(
            non_white_bounds(&img).is_none(),
            "place labels whose marker falls outside the clip mask should not render"
        );
    }

    #[test]
    fn projected_place_label_priorities_reduce_auxiliary_and_micro_visual_weight() {
        let primary = place_label_render_adjustments(ProjectedPlaceLabelPriority::Primary);
        let auxiliary = place_label_render_adjustments(ProjectedPlaceLabelPriority::Auxiliary);
        let micro = place_label_render_adjustments(ProjectedPlaceLabelPriority::Micro);

        assert_eq!(primary.text_size_factor, 1.0);
        assert_eq!(primary.marker_scale_factor, 1.0);
        assert!(auxiliary.text_size_factor < primary.text_size_factor);
        assert!(auxiliary.text_alpha_factor < primary.text_alpha_factor);
        assert!(micro.text_size_factor < auxiliary.text_size_factor);
        assert!(micro.text_alpha_factor < auxiliary.text_alpha_factor);
        assert!(micro.marker_scale_factor < auxiliary.marker_scale_factor);
        assert!(micro.halo_width_factor < auxiliary.halo_width_factor);
    }

    fn slanted_projected_fixture() -> (Layout, ProjectedGrid, Arc<[Option<(f64, f64)>]>, LocalRect)
    {
        let layout = compute_layout(
            320,
            240,
            true,
            true,
            RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
            ChromeScale::default(),
        );
        let nx = 14usize;
        let ny = 10usize;
        let grid = ProjectedGrid {
            x: vec![0.0; nx * ny],
            y: vec![0.0; nx * ny],
            ny,
            nx,
        };
        let mut pixel_points = Vec::with_capacity(nx * ny);
        for j in 0..ny {
            for i in 0..nx {
                pixel_points.push(Some((
                    28.0 + i as f64 * 12.0 + j as f64 * 0.5,
                    10.0 + j as f64 * 8.0,
                )));
            }
        }
        let pixel_points: Arc<[Option<(f64, f64)>]> = pixel_points.into();
        let rect = compute_projected_domain_frame_rect(
            sample_domain_frame(crate::request::Color::BLACK),
            &grid,
            pixel_points.as_ref(),
            layout.map_w,
            layout.map_h,
        )
        .expect("slanted test grid should produce a frame rect");
        (layout, grid, pixel_points, rect)
    }

    #[test]
    fn bucketed_contours_match_legacy_for_sorted_levels() {
        let layout = contour_test_layout();
        let overlay = ContourOverlay {
            data: vec![0.0, 1.0, 2.0, 1.0, 2.0, 3.0, 2.0, 3.0, 4.0],
            ny: 3,
            nx: 3,
            levels: vec![1.5, 2.5],
            color: Rgba::BLACK,
            width: 1,
            labels: false,
            show_extrema: false,
        };

        let mut legacy = blank_test_image();
        let mut bucketed = blank_test_image();
        draw_contours_legacy(&mut legacy, &layout, &overlay, None, None);
        draw_contours_bucketed(&mut bucketed, &layout, &overlay, None, None);

        assert_eq!(legacy, bucketed);
    }

    #[test]
    fn bucketed_contours_match_legacy_with_nan_corner() {
        let layout = contour_test_layout();
        let overlay = ContourOverlay {
            data: vec![0.0, 1.0, f64::NAN, 3.0],
            ny: 2,
            nx: 2,
            levels: vec![0.5, 1.5, 2.5],
            color: Rgba::BLACK,
            width: 1,
            labels: false,
            show_extrema: false,
        };

        let mut legacy = blank_test_image();
        let mut bucketed = blank_test_image();
        draw_contours_legacy(&mut legacy, &layout, &overlay, None, None);
        draw_contours_bucketed(&mut bucketed, &layout, &overlay, None, None);

        assert_eq!(legacy, bucketed);
    }

    #[test]
    fn domain_frame_uses_projected_coverage_when_fill_is_fully_masked() {
        let mut opts = sample_projected_opts();
        opts.cmap = sample_masked_cmap();
        opts.title = None;
        opts.domain_frame = Some(sample_domain_frame(crate::request::Color::rgba(
            250, 10, 10, 255,
        )));

        let data = [0.0f64; 4];
        let image = render_to_image(&data, 2, 2, &opts);
        let outline_pixels = image
            .pixels()
            .filter(|px| px.0[0] > 180 && px.0[1] < 120 && px.0[2] < 120)
            .count();

        assert!(
            outline_pixels > 0,
            "domain frame should still render from projected coverage even when fill alpha is empty"
        );
    }

    #[test]
    fn domain_frame_clears_map_outside_rect() {
        let (layout, _, _, rect) = slanted_projected_fixture();
        let presentation = RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology);
        let mut img =
            RgbaImage::from_pixel(320, 240, presentation.canvas_background.to_image_rgba());

        for py in layout.map_y..layout.map_y + layout.map_h {
            for px in layout.map_x..layout.map_x + layout.map_w {
                img.put_pixel(px, py, presentation.map_background.to_image_rgba());
            }
        }

        let outside_x = layout.map_x + rect.min_x.saturating_sub(1);
        let outside_y = layout.map_y + rect.min_y;
        let inside_x = layout.map_x + rect.min_x + 1;
        let inside_y = layout.map_y + rect.min_y + 1;
        img.put_pixel(outside_x, outside_y, Rgba::BLACK.to_image_rgba());

        clear_map_outside_local_rect(&mut img, &layout, rect, presentation.canvas_background);

        assert_eq!(
            img.get_pixel(outside_x, outside_y).0,
            presentation.canvas_background.to_image_rgba().0
        );
        assert_eq!(
            img.get_pixel(inside_x, inside_y).0,
            presentation.map_background.to_image_rgba().0
        );
    }

    #[test]
    fn domain_frame_moves_colorbar_under_frame() {
        let (layout, _, _, rect) = slanted_projected_fixture();
        let frame = sample_domain_frame(crate::request::Color::BLACK);

        let (_, cbar_y, _) = colorbar_anchor_rect(&layout, Some(frame), Some(rect));

        assert!(cbar_y < layout.cbar_y);
    }

    #[test]
    fn domain_frame_text_anchors_to_rect() {
        let (layout, _, _, rect) = slanted_projected_fixture();
        let frame = sample_domain_frame(crate::request::Color::BLACK);

        let (left, right, center) = chrome_anchor_bounds(&layout, Some(frame), Some(rect));

        assert_eq!(left, layout.map_x + rect.min_x);
        assert_eq!(right, layout.map_x + rect.max_x);
        assert_eq!(center, left + right.saturating_sub(left) / 2);
        assert_ne!(left, layout.map_x);
        assert_ne!(right, layout.map_x + layout.map_w);
    }

    #[test]
    fn domain_frame_text_rows_anchor_just_above_rect() {
        let (layout, _, _, rect) = slanted_projected_fixture();
        let frame = sample_domain_frame(crate::request::Color::BLACK);

        let (title_y, subtitle_y) = chrome_anchor_rows(&layout, Some(frame), Some(rect));
        let frame_top = layout.map_y + rect.min_y;
        let max_gap = text::bold_line_height(layout.text_scale)
            .max(text::regular_line_height(layout.text_scale))
            .saturating_add(8u32.saturating_mul(layout.text_scale.max(1)));

        assert_eq!(title_y, subtitle_y);
        assert!(title_y < frame_top);
        assert!(frame_top.saturating_sub(title_y) <= max_gap);
    }

    #[test]
    fn trim_vertical_canvas_whitespace_crops_outer_blank_rows() {
        let mut img = RgbaImage::from_pixel(
            6,
            10,
            RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology)
                .canvas_background
                .to_image_rgba(),
        );
        for y in 3..7 {
            for x in 0..6 {
                img.put_pixel(x, y, Rgba::BLACK.to_image_rgba());
            }
        }

        let trimmed = trim_vertical_canvas_whitespace(
            &img,
            RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology).canvas_background,
        );

        assert_eq!(trimmed.width(), 6);
        assert!(trimmed.height() < 10);
        assert!(trimmed.height() >= 4);
    }

    #[test]
    fn bucketed_contours_match_legacy_when_projected_corner_is_missing() {
        let layout = contour_test_layout();
        let overlay = ContourOverlay {
            data: vec![0.0, 1.0, 2.0, 3.0],
            ny: 2,
            nx: 2,
            levels: vec![1.5],
            color: Rgba::BLACK,
            width: 1,
            labels: false,
            show_extrema: false,
        };
        let pixel_points = vec![
            Some((0.0, 0.0)),
            None,
            Some((64.0, 64.0)),
            Some((0.0, 64.0)),
        ];

        let mut legacy = blank_test_image();
        let mut bucketed = blank_test_image();
        draw_contours_legacy(&mut legacy, &layout, &overlay, Some(&pixel_points), None);
        draw_contours_bucketed(&mut bucketed, &layout, &overlay, Some(&pixel_points), None);

        assert_eq!(legacy, bucketed);
    }

    #[test]
    fn levels_are_sorted_finite_rejects_unsorted_or_nan_levels() {
        assert!(levels_are_sorted_finite(&[1.0, 2.0, 3.0]));
        assert!(!levels_are_sorted_finite(&[2.0, 1.0]));
        assert!(!levels_are_sorted_finite(&[1.0, f64::NAN, 3.0]));
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
        assert!(ratio > 1.35);
        assert!(ratio < 1.7);
    }

    #[test]
    fn colorbar_tick_levels_follow_legend_levels_when_fill_is_densified() {
        let cmap = LeveledColormap::from_palette_with_options(
            &[Rgba::new(0, 0, 255), Rgba::new(255, 0, 0)],
            &[0.0, 10.0, 20.0, 30.0, 40.0],
            Extend::Neither,
            None,
            ColormapBuildOptions {
                render_density: crate::colormap::RenderDensity::default(),
                legend: crate::colormap::LegendControls {
                    density: LevelDensity::default(),
                    mode: crate::colormap::LegendMode::Stepped,
                },
            },
        );

        assert!(cmap.levels.len() > cmap.legend_levels.len());
        assert_eq!(
            colorbar_levels_for_ticks(&cmap),
            cmap.legend_levels.as_slice()
        );
    }

    #[test]
    fn colorbar_tick_labels_clamp_to_requested_bounds() {
        let labels =
            filter_tick_labels_to_fit(&[0.0, 50.0, 100.0], 0.0, 100.0, 80, 120, 80, 200, 400, 1);
        assert!(!labels.is_empty());
        for (_, lx, label) in labels {
            let width = text::text_width(&label, 1) as i32;
            assert!(lx >= 80);
            assert!(lx + width <= 200);
        }
    }

    #[test]
    fn chrome_scale_grows_layout_for_larger_outputs() {
        let base = compute_layout(
            1200,
            900,
            true,
            true,
            RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
            ChromeScale::default(),
        );
        let bigger = compute_layout(
            2400,
            1800,
            true,
            true,
            RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
            ChromeScale::default(),
        );

        assert!(bigger.cbar_h > base.cbar_h);
        assert!(bigger.text_scale > base.text_scale);
        assert!(bigger.label_gap > base.label_gap);
    }

    #[test]
    fn filled_layout_keeps_header_and_legend_tight_to_map() {
        let layout = compute_layout(
            1200,
            900,
            true,
            true,
            RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology),
            ChromeScale::Fixed(1.0),
        );

        assert_eq!(layout.map_y, 42);
        assert_eq!(layout.title_y, 5);
        assert_eq!(layout.subtitle_y, 5);
        assert_eq!(layout.cbar_y + layout.cbar_h, 892);
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

    #[test]
    fn crates_do_not_reintroduce_legacy_credit_footers() {
        let crates_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..");
        let forbidden = [
            String::from_utf8(vec![
                67, 111, 108, 111, 114, 32, 84, 97, 98, 108, 101, 115, 58, 32, 83, 111, 108, 97,
                114, 112, 111, 119, 101, 114, 48, 55,
            ])
            .expect("legacy footer bytes should be valid utf-8"),
            ["Pivotal", " Weather"].concat(),
            ["Weather", "Bell"].concat(),
        ];
        let mut offenders = Vec::<String>::new();
        visit_rs_files(&crates_root, &mut |path| {
            if let Ok(contents) = std::fs::read_to_string(path) {
                for term in &forbidden {
                    if contents.contains(term) {
                        offenders.push(format!("{} => {}", path.display(), term));
                    }
                }
            }
        })
        .expect("crate source tree should be readable");
        assert!(
            offenders.is_empty(),
            "legacy credit/footer strings remain in crates/: {offenders:?}"
        );
    }
}
