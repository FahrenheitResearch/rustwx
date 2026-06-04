use rusttype::{Font, Scale, point};
use rustwx_contour::{ContourEngine, ContourLevels, RectilinearGrid, ScalarField2D};
use std::sync::OnceLock;
use std::time::Instant;

use crate::data::{ScalarSection, SectionMetadata};
use crate::error::CrossSectionError;
use crate::palette::CrossSectionPalette;
use crate::style::{CrossSectionProduct, CrossSectionStyle};
use crate::vertical::{VerticalAxis, VerticalKind, VerticalUnits};
use crate::wind::DecomposedWindGrid;

const SOURCE_SANS_3_REGULAR: &[u8] =
    include_bytes!("../../rustwx-render/assets/fonts/SourceSans3-Regular.ttf");
const SOURCE_SANS_3_SEMIBOLD: &[u8] =
    include_bytes!("../../rustwx-render/assets/fonts/SourceSans3-Semibold.ttf");
const MS_TO_KT_F32: f32 = 1.943_844_5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrossSectionFontKind {
    Regular,
    Semibold,
}

struct CrossSectionFontSet {
    regular: Option<Font<'static>>,
    semibold: Option<Font<'static>>,
}

static CROSS_SECTION_FONTS: OnceLock<CrossSectionFontSet> = OnceLock::new();

/// Simple RGBA color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
    pub a: u8,
}

impl Color {
    pub const BLACK: Self = Self::rgb(0, 0, 0);
    pub const WHITE: Self = Self::rgb(255, 255, 255);
    pub const TRANSPARENT: Self = Self::rgba(0, 0, 0, 0);

    pub const fn rgb(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b, a: 255 }
    }

    pub const fn rgba(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }

    fn with_alpha(self, alpha: u8) -> Self {
        Self { a: alpha, ..self }
    }

    fn luminance(self) -> f32 {
        (0.2126 * self.r as f32 + 0.7152 * self.g as f32 + 0.0722 * self.b as f32) / 255.0
    }

    fn lerp(self, other: Self, fraction: f32) -> Self {
        let fraction = fraction.clamp(0.0, 1.0);
        let mix = |start: u8, end: u8| -> u8 {
            let start = start as f32;
            let end = end as f32;
            (start + (end - start) * fraction).round() as u8
        };
        Self {
            r: mix(self.r, other.r),
            g: mix(self.g, other.g),
            b: mix(self.b, other.b),
            a: mix(self.a, other.a),
        }
    }
}

/// Margins around the drawable plot area.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Insets {
    pub left: u32,
    pub right: u32,
    pub top: u32,
    pub bottom: u32,
}

impl Default for Insets {
    fn default() -> Self {
        Self {
            left: 82,
            right: 128,
            top: 64,
            bottom: 86,
        }
    }
}

/// Render options for [`render_scalar_section`].
#[derive(Debug, Clone, PartialEq)]
pub struct CrossSectionRenderRequest {
    pub width: u32,
    pub height: u32,
    pub margins: Insets,
    pub page_background_top: Color,
    pub page_background_bottom: Color,
    pub plot_background_top: Color,
    pub plot_background_bottom: Color,
    pub frame_color: Color,
    pub axis_color: Color,
    pub text_color: Color,
    pub grid_major_color: Color,
    pub grid_minor_color: Color,
    pub terrain_fill_top: Color,
    pub terrain_fill_bottom: Color,
    pub terrain_stroke: Color,
    pub terrain_highlight: Color,
    pub palette: Vec<Color>,
    pub value_range: Option<(f32, f32)>,
    pub value_ticks: Vec<f32>,
    pub colorbar_label: Option<String>,
    pub show_axes: bool,
    pub show_grid: bool,
    pub show_colorbar: bool,
    pub isotherms_c: Vec<f32>,
    pub highlight_isotherm_c: Option<f32>,
    pub isotherm_color: Color,
    pub highlight_isotherm_color: Color,
    pub wind_overlay: Option<WindOverlayBundle>,
    pub contour_overlays: Vec<ScalarContourOverlayBundle>,
}

impl CrossSectionRenderRequest {
    pub fn with_dimensions(mut self, width: u32, height: u32) -> Self {
        self.width = width;
        self.height = height;
        self
    }

    pub fn with_palette(mut self, palette: Vec<Color>) -> Self {
        self.palette = palette;
        self
    }

    pub fn with_value_range(mut self, min_value: f32, max_value: f32) -> Self {
        self.value_range = Some((min_value, max_value));
        self
    }

    pub fn with_value_ticks(mut self, ticks: Vec<f32>) -> Self {
        self.value_ticks = ticks;
        self
    }

    pub fn with_colorbar_label(mut self, label: impl Into<String>) -> Self {
        self.colorbar_label = Some(label.into());
        self
    }

    pub fn with_isotherms(mut self, levels_c: Vec<f32>, highlight_c: Option<f32>) -> Self {
        self.isotherms_c = levels_c;
        self.highlight_isotherm_c = highlight_c;
        self
    }

    pub fn with_margins(mut self, margins: Insets) -> Self {
        self.margins = margins;
        self
    }

    pub fn with_wind_overlay(mut self, overlay: WindOverlayBundle) -> Self {
        self.wind_overlay = Some(overlay);
        self
    }

    pub fn with_contour_overlay(mut self, overlay: ScalarContourOverlayBundle) -> Self {
        self.contour_overlays.push(overlay);
        self
    }

    pub fn with_contour_overlays(mut self, overlays: Vec<ScalarContourOverlayBundle>) -> Self {
        self.contour_overlays = overlays;
        self
    }
}

impl Default for CrossSectionRenderRequest {
    fn default() -> Self {
        Self {
            width: 960,
            height: 560,
            margins: Insets::default(),
            page_background_top: Color::WHITE,
            page_background_bottom: Color::WHITE,
            plot_background_top: Color::rgb(248, 250, 252),
            plot_background_bottom: Color::rgb(248, 250, 252),
            frame_color: Color::rgb(18, 22, 27),
            axis_color: Color::rgb(38, 45, 53),
            text_color: Color::rgb(20, 24, 29),
            grid_major_color: Color::rgba(0, 0, 0, 50),
            grid_minor_color: Color::rgba(0, 0, 0, 22),
            terrain_fill_top: Color::rgb(151, 96, 48),
            terrain_fill_bottom: Color::rgb(151, 96, 48),
            terrain_stroke: Color::rgb(18, 14, 10),
            terrain_highlight: Color::TRANSPARENT,
            palette: CrossSectionPalette::default().build(),
            value_range: None,
            value_ticks: Vec::new(),
            colorbar_label: None,
            show_axes: true,
            show_grid: true,
            show_colorbar: true,
            isotherms_c: CrossSectionStyle::default().isotherms_c().to_vec(),
            highlight_isotherm_c: CrossSectionStyle::default().highlight_isotherm_c(),
            isotherm_color: Color::rgba(20, 24, 29, 205),
            highlight_isotherm_color: Color::rgb(214, 34, 190),
            wind_overlay: None,
            contour_overlays: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarContourOverlayBundle {
    pub section: ScalarSection,
    pub levels: Vec<f32>,
    pub highlight_level: Option<f32>,
    pub label: Option<String>,
    pub units: Option<String>,
    pub color: Color,
    pub highlight_color: Color,
}

impl ScalarContourOverlayBundle {
    pub fn new(section: ScalarSection, levels: Vec<f32>) -> Self {
        Self {
            section,
            levels,
            highlight_level: None,
            label: None,
            units: None,
            color: Color::rgba(20, 24, 29, 205),
            highlight_color: Color::rgb(214, 34, 190),
        }
    }

    pub fn with_highlight(mut self, level: f32) -> Self {
        self.highlight_level = Some(level);
        self
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn with_units(mut self, units: impl Into<String>) -> Self {
        self.units = Some(units.into());
        self
    }

    pub fn with_color(mut self, color: Color) -> Self {
        self.color = color;
        self
    }

    pub fn with_highlight_color(mut self, color: Color) -> Self {
        self.highlight_color = color;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindOverlayBundle {
    pub grid: DecomposedWindGrid,
    pub style: WindOverlayStyle,
    pub label: Option<String>,
}

impl WindOverlayBundle {
    pub fn new(grid: DecomposedWindGrid, style: WindOverlayStyle) -> Self {
        Self {
            grid,
            style,
            label: None,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WindOverlayStyle {
    pub stride_points: usize,
    pub stride_levels: usize,
    pub target_columns: usize,
    pub min_speed_ms: f32,
    pub max_speed_ms: f32,
    pub base_length_px: f32,
    pub max_length_px: f32,
    pub arrow_head_px: f32,
    pub cross_tick_px: f32,
    pub line_width: u32,
    pub color: Color,
}

impl Default for WindOverlayStyle {
    fn default() -> Self {
        Self {
            stride_points: 6,
            stride_levels: 2,
            target_columns: 10,
            min_speed_ms: 6.0,
            max_speed_ms: 35.0,
            base_length_px: 18.0,
            max_length_px: 18.0,
            arrow_head_px: 4.0,
            cross_tick_px: 5.0,
            line_width: 1,
            color: Color::rgba(0, 0, 0, 220),
        }
    }
}

/// Raw RGBA output from the lightweight rasterizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedCrossSection {
    width: u32,
    height: u32,
    rgba: Vec<u8>,
}

impl RenderedCrossSection {
    pub fn width(&self) -> u32 {
        self.width
    }

    pub fn height(&self) -> u32 {
        self.height
    }

    pub fn rgba(&self) -> &[u8] {
        &self.rgba
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CrossSectionRenderTiming {
    pub plot_layout_ms: u128,
    pub terrain_mask_ms: u128,
    pub scene_resolve_ms: u128,
    pub canvas_init_ms: u128,
    pub scalar_field_ms: u128,
    pub grid_ms: u128,
    pub contour_topology_ms: u128,
    pub contour_draw_ms: u128,
    pub wind_overlay_ms: u128,
    pub terrain_ms: u128,
    pub axes_ms: u128,
    pub header_ms: u128,
    pub footer_ms: u128,
    pub colorbar_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct OverlayContourTiming {
    topology_ms: u128,
    draw_ms: u128,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq)]
struct WindVectorGeometry {
    start: (f64, f64),
    end: (f64, f64),
    head_left: (f64, f64),
    head_right: (f64, f64),
}

#[derive(Debug, Clone, PartialEq)]
struct ResolvedRenderScene {
    palette: Vec<Color>,
    min_value: f32,
    max_value: f32,
    value_ticks: Vec<f32>,
    colorbar_label: String,
    overlay_levels: Vec<f32>,
    highlight_overlay: Option<f32>,
    field_label: String,
    field_units: Option<String>,
}

impl ResolvedRenderScene {
    fn resolve(
        section: &ScalarSection,
        masked: &ScalarSection,
        request: &CrossSectionRenderRequest,
    ) -> Result<Self, CrossSectionError> {
        let declared_style = detect_declared_style(section.metadata());
        let declared_palette = detect_declared_palette(section.metadata());
        let finite_range = masked.finite_range();
        let (min_value, max_value) = request
            .value_range
            .or_else(|| {
                declared_style
                    .as_ref()
                    .and_then(CrossSectionStyle::value_range)
            })
            .or(finite_range)
            .ok_or(CrossSectionError::NoFiniteData)?;

        let palette = if request_uses_default_palette(request) {
            declared_style
                .as_ref()
                .map(|style| style.palette().build())
                .or_else(|| declared_palette.map(CrossSectionPalette::build))
                .unwrap_or_else(|| request.palette.clone())
        } else {
            request.palette.clone()
        };
        if palette.len() < 2 {
            return Err(CrossSectionError::EmptyColorRamp);
        }

        let value_ticks = if request.value_ticks.is_empty() {
            declared_style
                .as_ref()
                .map(|style| style.value_ticks().to_vec())
                .filter(|ticks| !ticks.is_empty())
                .unwrap_or_else(|| nice_value_ticks(min_value, max_value, 7))
        } else {
            request.value_ticks.clone()
        };

        let uses_default_overlay = request_uses_default_overlays(request);
        let overlay_levels = if uses_default_overlay {
            declared_style
                .as_ref()
                .map(|style| style.isotherms_c().to_vec())
                .unwrap_or_else(|| request.isotherms_c.clone())
        } else {
            request.isotherms_c.clone()
        };
        let mut overlay_levels = overlay_levels;
        normalize_levels(&mut overlay_levels);
        let highlight_overlay = if overlay_levels.is_empty() {
            None
        } else if uses_default_overlay {
            declared_style
                .as_ref()
                .and_then(CrossSectionStyle::highlight_isotherm_c)
                .filter(|value| {
                    overlay_levels
                        .iter()
                        .any(|level| (*level - *value).abs() <= 0.001)
                })
                .or(request.highlight_isotherm_c)
        } else {
            request.highlight_isotherm_c
        };

        let declared_product = declared_style.as_ref().map(CrossSectionStyle::product);
        let field_label = resolve_field_label(section.metadata(), declared_product);
        let field_units = section
            .metadata()
            .field_units
            .as_deref()
            .map(normalize_units_label)
            .or_else(|| {
                declared_product
                    .map(CrossSectionProduct::units)
                    .map(normalize_units_label)
            })
            .filter(|units| !units.is_empty());
        let colorbar_label = request
            .colorbar_label
            .clone()
            .or_else(|| {
                declared_style
                    .as_ref()
                    .and_then(|style| style.colorbar_label().map(str::to_string))
            })
            .unwrap_or_else(|| compose_label_with_units(&field_label, field_units.as_deref()));

        Ok(Self {
            palette,
            min_value,
            max_value,
            value_ticks,
            colorbar_label,
            overlay_levels,
            highlight_overlay,
            field_label,
            field_units,
        })
    }
}

/// Renders a scalar cross-section to a simple RGBA buffer.
pub fn render_scalar_section(
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
) -> Result<RenderedCrossSection, CrossSectionError> {
    render_scalar_section_profile(section, request).map(|(rendered, _)| rendered)
}

pub fn render_scalar_section_profile(
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
) -> Result<(RenderedCrossSection, CrossSectionRenderTiming), CrossSectionError> {
    let total_start = Instant::now();
    if request.width < 2 || request.height < 2 {
        return Err(CrossSectionError::InvalidRenderDimensions);
    }

    let plot_layout_start = Instant::now();
    let plot = PlotRect::from_request(request)?;
    let plot_layout_ms = plot_layout_start.elapsed().as_millis();
    let terrain_mask_start = Instant::now();
    let masked = section.masked_with_terrain();
    let terrain_mask_ms = terrain_mask_start.elapsed().as_millis();
    let scene_resolve_start = Instant::now();
    let scene = ResolvedRenderScene::resolve(section, &masked, request)?;
    let scene_resolve_ms = scene_resolve_start.elapsed().as_millis();

    let canvas_init_start = Instant::now();
    let mut canvas = Canvas::new(
        request.width,
        request.height,
        request.page_background_top,
        request.page_background_bottom,
    );
    canvas.fill_rect_gradient(
        plot,
        request.plot_background_top,
        request.plot_background_bottom,
    );
    let canvas_init_ms = canvas_init_start.elapsed().as_millis();
    let scalar_field_start = Instant::now();
    render_scalar_field(&mut canvas, &plot, section, &scene);
    let scalar_field_ms = scalar_field_start.elapsed().as_millis();

    let mut grid_ms = 0;
    if request.show_grid {
        let grid_start = Instant::now();
        draw_grid(&mut canvas, &plot, &masked, request);
        grid_ms = grid_start.elapsed().as_millis();
    }

    let contour_timing =
        draw_overlay_contours_profile(&mut canvas, &plot, &masked, request, &scene);
    let wind_overlay_start = Instant::now();
    draw_wind_overlay(&mut canvas, &plot, &masked, request)?;
    let wind_overlay_ms = wind_overlay_start.elapsed().as_millis();
    let terrain_start = Instant::now();
    draw_terrain(&mut canvas, &plot, section, request);
    let terrain_ms = terrain_start.elapsed().as_millis();

    let mut axes_ms = 0;
    if request.show_axes {
        let axes_start = Instant::now();
        draw_axes(&mut canvas, &plot, &masked, request);
        axes_ms = axes_start.elapsed().as_millis();
    }

    let header_start = Instant::now();
    draw_header(&mut canvas, &plot, &masked, request, &scene);
    let header_ms = header_start.elapsed().as_millis();
    let footer_start = Instant::now();
    draw_footer(&mut canvas, &plot, &masked, request, &scene);
    let footer_ms = footer_start.elapsed().as_millis();

    let mut colorbar_ms = 0;
    if request.show_colorbar {
        let colorbar_start = Instant::now();
        draw_colorbar(&mut canvas, &plot, request, &scene);
        colorbar_ms = colorbar_start.elapsed().as_millis();
    }

    let rendered = RenderedCrossSection {
        width: request.width,
        height: request.height,
        rgba: canvas.rgba,
    };
    Ok((
        rendered,
        CrossSectionRenderTiming {
            plot_layout_ms,
            terrain_mask_ms,
            scene_resolve_ms,
            canvas_init_ms,
            scalar_field_ms,
            grid_ms,
            contour_topology_ms: contour_timing.topology_ms,
            contour_draw_ms: contour_timing.draw_ms,
            wind_overlay_ms,
            terrain_ms,
            axes_ms,
            header_ms,
            footer_ms,
            colorbar_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    ))
}

#[derive(Debug, Clone, Copy)]
struct PlotRect {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

impl PlotRect {
    fn from_request(request: &CrossSectionRenderRequest) -> Result<Self, CrossSectionError> {
        let width = request
            .width
            .checked_sub(request.margins.left + request.margins.right)
            .ok_or(CrossSectionError::InvalidPlotMargins)?;
        let height = request
            .height
            .checked_sub(request.margins.top + request.margins.bottom)
            .ok_or(CrossSectionError::InvalidPlotMargins)?;

        if width < 2 || height < 2 {
            return Err(CrossSectionError::InvalidPlotMargins);
        }

        Ok(Self {
            x: request.margins.left,
            y: request.margins.top,
            width,
            height,
        })
    }

    fn right(&self) -> u32 {
        self.x + self.width - 1
    }

    fn bottom(&self) -> u32 {
        self.y + self.height - 1
    }

    fn contains(&self, x: i32, y: i32) -> bool {
        x >= self.x as i32
            && x <= self.right() as i32
            && y >= self.y as i32
            && y <= self.bottom() as i32
    }
}

struct Canvas {
    width: u32,
    height: u32,
    rgba: Vec<u8>,
}

impl Canvas {
    fn new(width: u32, height: u32, top: Color, bottom: Color) -> Self {
        let mut canvas = Self {
            width,
            height,
            rgba: vec![0u8; (width * height * 4) as usize],
        };
        canvas.fill_vertical_gradient(0, 0, width, height, top, bottom);
        canvas
    }

    fn fill_vertical_gradient(
        &mut self,
        x: u32,
        y: u32,
        width: u32,
        height: u32,
        top: Color,
        bottom: Color,
    ) {
        if width == 0 || height == 0 {
            return;
        }

        for dy in 0..height {
            let fraction = if height == 1 {
                0.0
            } else {
                dy as f32 / (height - 1) as f32
            };
            let color = top.lerp(bottom, fraction);
            for dx in 0..width {
                self.set_pixel(x + dx, y + dy, color);
            }
        }
    }

    fn fill_rect_gradient(&mut self, rect: PlotRect, top: Color, bottom: Color) {
        self.fill_vertical_gradient(rect.x, rect.y, rect.width, rect.height, top, bottom);
    }

    fn fill_rect(&mut self, x: u32, y: u32, width: u32, height: u32, color: Color) {
        for dy in 0..height {
            for dx in 0..width {
                self.blend_pixel((x + dx) as i32, (y + dy) as i32, color);
            }
        }
    }

    fn draw_rect(&mut self, rect: PlotRect, color: Color, thickness: u32) {
        for offset in 0..thickness {
            let x0 = rect.x.saturating_sub(offset);
            let y0 = rect.y.saturating_sub(offset);
            let x1 = (rect.right() + offset).min(self.width.saturating_sub(1));
            let y1 = (rect.bottom() + offset).min(self.height.saturating_sub(1));
            for x in x0..=x1 {
                self.blend_pixel(x as i32, y0 as i32, color);
                self.blend_pixel(x as i32, y1 as i32, color);
            }
            for y in y0..=y1 {
                self.blend_pixel(x0 as i32, y as i32, color);
                self.blend_pixel(x1 as i32, y as i32, color);
            }
        }
    }

    fn draw_line(
        &mut self,
        start: (f64, f64),
        end: (f64, f64),
        color: Color,
        thickness: u32,
        clip: Option<&PlotRect>,
    ) {
        let dx = end.0 - start.0;
        let dy = end.1 - start.1;
        let steps = dx.abs().max(dy.abs()).ceil().max(1.0) as usize;
        let radius = thickness.saturating_sub(1) as i32 / 2;

        for step in 0..=steps {
            let t = step as f64 / steps as f64;
            let x = start.0 + dx * t;
            let y = start.1 + dy * t;
            let xi = x.round() as i32;
            let yi = y.round() as i32;
            for oy in -radius..=radius {
                for ox in -radius..=radius {
                    if clip.is_none_or(|rect| rect.contains(xi + ox, yi + oy)) {
                        self.blend_pixel(xi + ox, yi + oy, color);
                    }
                }
            }
        }
    }

    fn set_pixel(&mut self, x: u32, y: u32, color: Color) {
        if x >= self.width || y >= self.height {
            return;
        }
        let idx = ((y * self.width + x) * 4) as usize;
        self.rgba[idx] = color.r;
        self.rgba[idx + 1] = color.g;
        self.rgba[idx + 2] = color.b;
        self.rgba[idx + 3] = color.a;
    }

    fn blend_pixel(&mut self, x: i32, y: i32, color: Color) {
        if x < 0 || y < 0 || x >= self.width as i32 || y >= self.height as i32 {
            return;
        }
        let idx = ((y as u32 * self.width + x as u32) * 4) as usize;
        if color.a == 255 {
            self.rgba[idx] = color.r;
            self.rgba[idx + 1] = color.g;
            self.rgba[idx + 2] = color.b;
            self.rgba[idx + 3] = 255;
            return;
        }
        if color.a == 0 {
            return;
        }

        let alpha = color.a as f32 / 255.0;
        let inv = 1.0 - alpha;
        self.rgba[idx] = (color.r as f32 * alpha + self.rgba[idx] as f32 * inv).round() as u8;
        self.rgba[idx + 1] =
            (color.g as f32 * alpha + self.rgba[idx + 1] as f32 * inv).round() as u8;
        self.rgba[idx + 2] =
            (color.b as f32 * alpha + self.rgba[idx + 2] as f32 * inv).round() as u8;
        self.rgba[idx + 3] = 255;
    }

    fn draw_text(
        &mut self,
        x: i32,
        y: i32,
        text: &str,
        color: Color,
        scale: u32,
        shadow: Option<Color>,
    ) {
        if let Some(shadow) = shadow {
            self.draw_text(
                x + scale as i32,
                y + scale as i32,
                text,
                shadow,
                scale,
                None,
            );
        }

        if let Some(font) = cross_section_font(scale) {
            self.draw_ttf_text(x, y, text, color, scale, font);
            return;
        }

        let mut cursor_x = x;
        let mut cursor_y = y;
        let bitmap_scale = scale.max(1) as i32;
        for ch in text.chars() {
            if ch == '\n' {
                cursor_x = x;
                cursor_y += 8 * bitmap_scale;
                continue;
            }

            let glyph = glyph_rows(ch);
            for (row_index, row) in glyph.iter().enumerate() {
                for col in 0..5 {
                    if (row >> (4 - col)) & 1 == 1 {
                        for sy in 0..bitmap_scale {
                            for sx in 0..bitmap_scale {
                                self.blend_pixel(
                                    cursor_x + col * bitmap_scale + sx,
                                    cursor_y + row_index as i32 * bitmap_scale + sy,
                                    color,
                                );
                            }
                        }
                    }
                }
            }
            cursor_x += 6 * bitmap_scale;
        }
    }

    fn draw_ttf_text(
        &mut self,
        x: i32,
        y: i32,
        text: &str,
        color: Color,
        scale: u32,
        font: &Font<'static>,
    ) {
        let kind = font_kind_for_scale(scale);
        let scale = Scale::uniform(cross_section_font_size_px(scale, kind));
        let v_metrics = font.v_metrics(scale);
        let line_height = cross_section_line_height_px(scale, font).max(1.0).ceil() as i32;

        for (line_index, line) in text.split('\n').enumerate() {
            let baseline_y = y + line_index as i32 * line_height;
            let glyphs = font.layout(
                line,
                scale,
                point(x as f32, baseline_y as f32 + v_metrics.ascent),
            );

            for glyph in glyphs {
                if let Some(bb) = glyph.pixel_bounding_box() {
                    glyph.draw(|gx, gy, coverage| {
                        let px = bb.min.x + gx as i32;
                        let py = bb.min.y + gy as i32;
                        let alpha = ((color.a as f32) * coverage).round().clamp(0.0, 255.0) as u8;
                        self.blend_pixel(
                            px,
                            py,
                            Color {
                                r: color.r,
                                g: color.g,
                                b: color.b,
                                a: alpha,
                            },
                        );
                    });
                }
            }
        }
    }
}

fn render_scalar_field(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    scene: &ResolvedRenderScene,
) {
    let start_distance = section.distances_km()[0];
    let end_distance = section.distances_km()[section.n_points() - 1];

    for plot_y in 0..plot.height {
        let y_fraction = if plot.height == 1 {
            0.0
        } else {
            plot_y as f64 / (plot.height as f64 - 1.0)
        };
        let axis_value = section.vertical_axis().value_at_plot_fraction(y_fraction);

        for plot_x in 0..plot.width {
            let x_fraction = if plot.width == 1 {
                0.0
            } else {
                plot_x as f64 / (plot.width as f64 - 1.0)
            };
            let distance_km = start_distance + x_fraction * (end_distance - start_distance);

            let Some(value) = section.bilinear_sample(distance_km, axis_value) else {
                continue;
            };
            let color = map_value_to_color(value, scene.min_value, scene.max_value, &scene.palette);
            canvas.set_pixel(plot.x + plot_x, plot.y + plot_y, color);
        }
    }
}

fn draw_grid(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
) {
    for tick in vertical_ticks(section.vertical_axis()) {
        if let Some(y) = axis_value_to_pixel(section.vertical_axis(), plot, tick) {
            canvas.draw_line(
                (plot.x as f64, y),
                (plot.right() as f64, y),
                request.grid_major_color,
                1,
                Some(plot),
            );
        }
    }

    for tick in intermediate_vertical_ticks(section.vertical_axis()) {
        if let Some(y) = axis_value_to_pixel(section.vertical_axis(), plot, tick) {
            canvas.draw_line(
                (plot.x as f64, y),
                (plot.right() as f64, y),
                request.grid_minor_color,
                1,
                Some(plot),
            );
        }
    }

    for tick in distance_ticks(section.distances_km(), 6) {
        if let Some(x) = distance_to_pixel(section, plot, tick) {
            canvas.draw_line(
                (x, plot.y as f64),
                (x, plot.bottom() as f64),
                request.grid_major_color,
                1,
                Some(plot),
            );
        }
    }

    for tick in intermediate_distance_ticks(section.distances_km(), 6) {
        if let Some(x) = distance_to_pixel(section, plot, tick) {
            canvas.draw_line(
                (x, plot.y as f64),
                (x, plot.bottom() as f64),
                request.grid_minor_color,
                1,
                Some(plot),
            );
        }
    }
}

fn draw_overlay_contours_profile(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
    scene: &ResolvedRenderScene,
) -> OverlayContourTiming {
    let mut timing = OverlayContourTiming::default();
    if !scene.overlay_levels.is_empty() {
        let fallback = draw_single_contour_overlay(
            canvas,
            plot,
            section,
            section,
            &scene.overlay_levels,
            scene.highlight_overlay,
            request.isotherm_color,
            request.highlight_isotherm_color,
            scene.field_units.as_deref(),
        );
        timing.topology_ms += fallback.topology_ms;
        timing.draw_ms += fallback.draw_ms;
    }

    for overlay in &request.contour_overlays {
        let masked_overlay = overlay.section.masked_with_terrain();
        let overlay_timing = draw_single_contour_overlay(
            canvas,
            plot,
            section,
            &masked_overlay,
            &overlay.levels,
            overlay.highlight_level,
            overlay.color,
            overlay.highlight_color,
            overlay.units.as_deref(),
        );
        timing.topology_ms += overlay_timing.topology_ms;
        timing.draw_ms += overlay_timing.draw_ms;
    }

    timing
}

fn draw_single_contour_overlay(
    canvas: &mut Canvas,
    plot: &PlotRect,
    plot_section: &ScalarSection,
    contour_section: &ScalarSection,
    levels: &[f32],
    highlight_overlay: Option<f32>,
    contour_color: Color,
    highlight_color: Color,
    label_units: Option<&str>,
) -> OverlayContourTiming {
    let topology_start = Instant::now();
    if contour_section.n_points() != plot_section.n_points()
        || contour_section.n_levels() != plot_section.n_levels()
    {
        return OverlayContourTiming::default();
    }

    let mut levels = levels.to_vec();
    if let Some(highlight) = highlight_overlay {
        levels.push(highlight);
    }
    normalize_levels(&mut levels);
    if levels.is_empty() {
        return OverlayContourTiming::default();
    }

    let Ok(grid) = RectilinearGrid::new(
        contour_section.distances_km().to_vec(),
        contour_section.vertical_axis().levels().to_vec(),
    ) else {
        return OverlayContourTiming::default();
    };
    let values = contour_section
        .values()
        .iter()
        .map(|value| *value as f64)
        .collect::<Vec<_>>();
    let Ok(field) = ScalarField2D::new(grid, values) else {
        return OverlayContourTiming::default();
    };
    let contour_levels = levels.iter().map(|value| *value as f64).collect::<Vec<_>>();
    let Ok(levels) = ContourLevels::new(contour_levels) else {
        return OverlayContourTiming::default();
    };

    let topology = ContourEngine::new().extract_isolines(&field, &levels);
    let topology_ms = topology_start.elapsed().as_millis();
    let draw_start = Instant::now();

    for layer in &topology.layers {
        let level = layer.level as f32;
        let highlighted =
            highlight_overlay.is_some_and(|candidate| (candidate - level).abs() <= 0.001);
        let color = if highlighted {
            highlight_color
        } else {
            contour_color
        };
        let thickness = if highlighted { 3 } else { 1 };
        let halo = contour_halo_color(color).with_alpha(if highlighted { 126 } else { 58 });

        for segment in &layer.segments {
            let Some(start) = data_point_to_pixel(
                plot_section,
                plot,
                segment.geometry.start.x,
                segment.geometry.start.y,
            ) else {
                continue;
            };
            let Some(end) = data_point_to_pixel(
                plot_section,
                plot,
                segment.geometry.end.x,
                segment.geometry.end.y,
            ) else {
                continue;
            };
            canvas.draw_line(start, end, halo, thickness + 2, Some(plot));
            canvas.draw_line(start, end, color, thickness, Some(plot));
        }

        if !highlighted {
            let label = contour_label_for_level(level, label_units);
            draw_contour_label(canvas, plot, plot_section, layer, color, &label);
        }
    }
    OverlayContourTiming {
        topology_ms,
        draw_ms: draw_start.elapsed().as_millis(),
    }
}

fn draw_wind_overlay(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
) -> Result<(), CrossSectionError> {
    let Some(overlay) = request.wind_overlay.as_ref() else {
        return Ok(());
    };

    if overlay.grid.n_levels() != section.n_levels()
        || overlay.grid.n_points() != section.n_points()
    {
        return Err(CrossSectionError::ShapeMismatch {
            context: "wind overlay",
            expected: section.n_levels() * section.n_points(),
            actual: overlay.grid.n_levels() * overlay.grid.n_points(),
        });
    }

    let stride_levels = overlay.style.stride_levels.max(1);
    let point_indices = wind_barb_point_indices(
        section.n_points(),
        overlay.style.target_columns,
        overlay.style.stride_points.max(1),
    );
    let axis_levels = section.vertical_axis().levels();
    let terrain = section.terrain();

    for level_index in (0..section.n_levels()).step_by(stride_levels) {
        for &point_index in &point_indices {
            let Some(speed_ms) = overlay.grid.speed_value(level_index, point_index) else {
                continue;
            };
            let Some(along_ms) = overlay.grid.along_section_value(level_index, point_index) else {
                continue;
            };
            let Some(left_ms) = overlay.grid.left_of_section_value(level_index, point_index) else {
                continue;
            };
            if !(speed_ms.is_finite() && along_ms.is_finite() && left_ms.is_finite()) {
                continue;
            }
            if speed_ms < overlay.style.min_speed_ms {
                continue;
            }

            let distance_km = section.distances_km()[point_index];
            let axis_value = axis_levels[level_index];
            if terrain.is_some_and(|terrain| {
                terrain
                    .below_surface(section.vertical_axis(), distance_km, axis_value)
                    .unwrap_or(false)
            }) {
                continue;
            }

            let Some((center_x, center_y)) =
                data_point_to_pixel(section, plot, distance_km, axis_value)
            else {
                continue;
            };

            draw_section_wind_barb(
                canvas,
                (center_x, center_y),
                along_ms,
                left_ms,
                speed_ms,
                overlay.style,
                plot,
            );
        }
    }

    Ok(())
}

fn wind_barb_point_indices(
    n_points: usize,
    target_columns: usize,
    fallback_stride: usize,
) -> Vec<usize> {
    if n_points == 0 {
        return Vec::new();
    }
    if target_columns == 0 {
        return (0..n_points).step_by(fallback_stride.max(1)).collect();
    }
    let count = target_columns.min(n_points);
    if count <= 1 {
        return vec![0];
    }
    (0..count)
        .map(|index| ((index * (n_points - 1) + (count - 1) / 2) / (count - 1)).min(n_points - 1))
        .collect()
}

#[cfg(test)]
fn draw_section_wind_vector(
    canvas: &mut Canvas,
    center: (f64, f64),
    along_ms: f32,
    left_ms: f32,
    speed_ms: f32,
    style: WindOverlayStyle,
    plot: &PlotRect,
) {
    let Some(geometry) =
        resolve_section_wind_vector_geometry(center, along_ms, left_ms, speed_ms, style)
    else {
        return;
    };
    let halo = contour_halo_color(style.color).with_alpha(90);
    canvas.draw_line(
        geometry.start,
        geometry.end,
        halo,
        style.line_width + 2,
        Some(plot),
    );
    canvas.draw_line(
        geometry.start,
        geometry.end,
        style.color,
        style.line_width,
        Some(plot),
    );
    canvas.draw_line(
        geometry.end,
        geometry.head_left,
        halo,
        style.line_width + 2,
        Some(plot),
    );
    canvas.draw_line(
        geometry.end,
        geometry.head_left,
        style.color,
        style.line_width,
        Some(plot),
    );
    canvas.draw_line(
        geometry.end,
        geometry.head_right,
        halo,
        style.line_width + 2,
        Some(plot),
    );
    canvas.draw_line(
        geometry.end,
        geometry.head_right,
        style.color,
        style.line_width,
        Some(plot),
    );
}

fn draw_section_wind_barb(
    canvas: &mut Canvas,
    center: (f64, f64),
    along_ms: f32,
    left_ms: f32,
    speed_ms: f32,
    style: WindOverlayStyle,
    plot: &PlotRect,
) {
    if !(along_ms.is_finite() && left_ms.is_finite() && speed_ms.is_finite()) {
        return;
    }

    let vector_norm = f64::from(along_ms).hypot(f64::from(left_ms));
    if vector_norm < 0.001 {
        return;
    }

    let shaft_length = style.max_length_px.max(style.base_length_px + 4.0) as f64;
    let unit_x = f64::from(along_ms) / vector_norm;
    let unit_y = -f64::from(left_ms) / vector_norm;
    let half_length = shaft_length * 0.5;
    let start = (
        center.0 - unit_x * half_length,
        center.1 - unit_y * half_length,
    );
    let end = (
        center.0 + unit_x * half_length,
        center.1 + unit_y * half_length,
    );
    draw_wind_line(canvas, start, end, style.color, style.line_width, plot);

    let barb_len = style.cross_tick_px.max(5.0) as f64;
    let spacing = (style.cross_tick_px.max(5.0) * 0.82) as f64;
    let perp_x = -unit_y;
    let perp_y = unit_x;
    let mut speed_kt = (speed_ms * MS_TO_KT_F32 / 5.0).round() * 5.0;
    let mut offset = 1.5f64;

    while speed_kt >= 47.5 {
        let base = (end.0 - unit_x * offset, end.1 - unit_y * offset);
        let next = (
            end.0 - unit_x * (offset + spacing),
            end.1 - unit_y * (offset + spacing),
        );
        let tip = (base.0 + perp_x * barb_len, base.1 + perp_y * barb_len);
        draw_wind_line(canvas, base, tip, style.color, style.line_width, plot);
        draw_wind_line(canvas, next, tip, style.color, style.line_width, plot);
        speed_kt -= 50.0;
        offset += spacing * 1.35;
    }

    while speed_kt >= 7.5 {
        let base = (end.0 - unit_x * offset, end.1 - unit_y * offset);
        let tip = (base.0 + perp_x * barb_len, base.1 + perp_y * barb_len);
        draw_wind_line(canvas, base, tip, style.color, style.line_width, plot);
        speed_kt -= 10.0;
        offset += spacing;
    }

    if speed_kt >= 2.5 {
        let base = (end.0 - unit_x * offset, end.1 - unit_y * offset);
        let tip = (
            base.0 + perp_x * barb_len * 0.55,
            base.1 + perp_y * barb_len * 0.55,
        );
        draw_wind_line(canvas, base, tip, style.color, style.line_width, plot);
    }
}

fn draw_wind_line(
    canvas: &mut Canvas,
    start: (f64, f64),
    end: (f64, f64),
    color: Color,
    thickness: u32,
    plot: &PlotRect,
) {
    canvas.draw_line(start, end, color, thickness, Some(plot));
}

#[cfg(test)]
fn resolve_section_wind_vector_geometry(
    center: (f64, f64),
    along_ms: f32,
    left_ms: f32,
    speed_ms: f32,
    style: WindOverlayStyle,
) -> Option<WindVectorGeometry> {
    if !(along_ms.is_finite() && left_ms.is_finite() && speed_ms.is_finite()) {
        return None;
    }

    let vector_norm = f64::from(along_ms).hypot(f64::from(left_ms));
    if vector_norm < 0.001 {
        return None;
    }

    let normalized_speed =
        (speed_ms / style.max_speed_ms.max(style.min_speed_ms + 1.0)).clamp(0.0, 1.0);
    let shaft_length =
        style.base_length_px + (style.max_length_px - style.base_length_px) * normalized_speed;
    let unit_x = f64::from(along_ms) / vector_norm;
    let unit_y = -f64::from(left_ms) / vector_norm;
    let half_length = shaft_length as f64 * 0.5;
    let start = (
        center.0 - unit_x * half_length,
        center.1 - unit_y * half_length,
    );
    let end = (
        center.0 + unit_x * half_length,
        center.1 + unit_y * half_length,
    );

    let head_length = style.arrow_head_px.max(2.0) as f64;
    let head_half_width = (style.cross_tick_px.max(2.0) * 0.5) as f64;
    let back_x = -unit_x * head_length;
    let back_y = -unit_y * head_length;
    let perp_x = -unit_y;
    let perp_y = unit_x;
    let head_left = (
        end.0 + back_x + perp_x * head_half_width,
        end.1 + back_y + perp_y * head_half_width,
    );
    let head_right = (
        end.0 + back_x - perp_x * head_half_width,
        end.1 + back_y - perp_y * head_half_width,
    );

    Some(WindVectorGeometry {
        start,
        end,
        head_left,
        head_right,
    })
}

fn draw_contour_label(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    layer: &rustwx_contour::ContourLayer,
    color: Color,
    label: &str,
) {
    let Some(segment) = layer.segments.iter().max_by(|left, right| {
        left.geometry
            .length_squared()
            .partial_cmp(&right.geometry.length_squared())
            .unwrap_or(std::cmp::Ordering::Equal)
    }) else {
        return;
    };

    let Some(start) = data_point_to_pixel(
        section,
        plot,
        segment.geometry.start.x,
        segment.geometry.start.y,
    ) else {
        return;
    };
    let Some(end) = data_point_to_pixel(
        section,
        plot,
        segment.geometry.end.x,
        segment.geometry.end.y,
    ) else {
        return;
    };
    let text_width = measure_text_width(&label, 1) as i32;
    let mid_x = ((start.0 + end.0) * 0.5).round() as i32;
    let mid_y = ((start.1 + end.1) * 0.5).round() as i32;
    let label_x =
        (mid_x - text_width / 2).clamp(plot.x as i32 + 4, plot.right() as i32 - text_width - 4);
    let label_y = (mid_y - 6).clamp(plot.y as i32 + 2, plot.bottom() as i32 - 14);

    canvas.draw_text(
        label_x + 1,
        label_y + 1,
        label,
        Color::WHITE.with_alpha(210),
        1,
        None,
    );
    canvas.draw_text(label_x, label_y, label, color, 1, None);
}

fn draw_terrain(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
) {
    let Some(terrain) = section.terrain() else {
        return;
    };

    let axis = section.vertical_axis();
    let mut surface_ys = Vec::with_capacity(plot.width as usize);
    for plot_x in 0..plot.width {
        let fraction = if plot.width == 1 {
            0.0
        } else {
            plot_x as f64 / (plot.width as f64 - 1.0)
        };
        let distance_km = section.distances_km()[0]
            + fraction
                * (section.distances_km()[section.n_points() - 1] - section.distances_km()[0]);

        let Some(surface_value) = terrain.surface_value_on_axis(axis, distance_km) else {
            continue;
        };

        let Some(surface_y) = terrain_surface_y(axis, plot, surface_value) else {
            surface_ys.push(None);
            continue;
        };

        surface_ys.push(Some(surface_y.min(plot.bottom())));
    }

    for plot_x in 0..plot.width {
        let Some(surface_y) = surface_ys[plot_x as usize] else {
            continue;
        };
        let column_height = plot.bottom().saturating_sub(surface_y) + 1;

        for offset in 0..column_height {
            canvas.blend_pixel(
                (plot.x + plot_x) as i32,
                (surface_y + offset) as i32,
                request.terrain_fill_top,
            );
        }
        canvas.blend_pixel(
            (plot.x + plot_x) as i32,
            surface_y as i32,
            request.terrain_stroke,
        );
        if surface_y > plot.y {
            canvas.blend_pixel(
                (plot.x + plot_x) as i32,
                surface_y as i32 - 1,
                request.terrain_stroke.with_alpha(155),
            );
        }
    }
}

fn draw_axes(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
) {
    canvas.draw_rect(*plot, request.frame_color, 2);

    for tick in vertical_ticks(section.vertical_axis()) {
        let Some(y) = axis_value_to_pixel(section.vertical_axis(), plot, tick) else {
            continue;
        };
        canvas.draw_line(
            (plot.x as f64 - 7.0, y),
            (plot.x as f64, y),
            request.axis_color,
            1,
            None,
        );
        let label = format_axis_tick(section.vertical_axis(), tick);
        let label_x = plot.x as i32 - measure_text_width(&label, 1) as i32 - 10;
        canvas.draw_text(
            label_x,
            y.round() as i32 - 3,
            &label,
            request.text_color,
            1,
            Some(Color::rgba(255, 255, 255, 175)),
        );
    }

    for tick in distance_ticks(section.distances_km(), 6) {
        let Some(x) = distance_to_pixel(section, plot, tick) else {
            continue;
        };
        canvas.draw_line(
            (x, plot.bottom() as f64),
            (x, plot.bottom() as f64 + 7.0),
            request.axis_color,
            1,
            None,
        );
        let label = format_distance_tick(tick);
        let label_x = (x.round() as i32 - measure_text_width(&label, 1) as i32 / 2).clamp(
            plot.x as i32,
            plot.right() as i32 - measure_text_width(&label, 1) as i32,
        );
        canvas.draw_text(
            label_x,
            plot.bottom() as i32 + 10,
            &label,
            request.text_color,
            1,
            Some(Color::rgba(255, 255, 255, 175)),
        );
    }

    let axis_title = axis_label(section.vertical_axis());
    canvas.draw_text(
        plot.x as i32 - measure_text_width(&axis_title, 1) as i32 - 12,
        plot.y as i32 - 22,
        &axis_title,
        request.text_color,
        1,
        Some(Color::rgba(255, 255, 255, 175)),
    );
}

fn draw_header(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
    scene: &ResolvedRenderScene,
) {
    let title = section
        .metadata()
        .title
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "Cross-Section: {}",
                compose_label_with_units(&scene.field_label, scene.field_units.as_deref())
            )
        });
    let title_scale = if measure_text_width(&title, 2) + plot.x + 28 <= canvas.width {
        2
    } else {
        1
    };
    let title_y = plot.y as i32 - 52;

    canvas.draw_text(
        plot.x as i32,
        title_y.max(8),
        &title,
        request.text_color,
        title_scale,
        None,
    );

    let timing = format_header_timing(section.metadata());
    if !timing.is_empty() {
        let timing_width = measure_text_width(&timing, 1) as i32;
        canvas.draw_text(
            plot.right() as i32 - timing_width,
            (title_y + 7).max(10),
            &timing,
            Color::rgb(82, 88, 96),
            1,
            None,
        );
    }

    draw_reference_legend(canvas, plot, request, scene);
}

fn draw_footer(
    canvas: &mut Canvas,
    plot: &PlotRect,
    section: &ScalarSection,
    request: &CrossSectionRenderRequest,
    _scene: &ResolvedRenderScene,
) {
    let center_label = "Distance (km)";
    let center_x =
        plot.x as i32 + (plot.width as i32 - measure_text_width(center_label, 1) as i32) / 2;
    canvas.draw_text(
        center_x,
        plot.bottom() as i32 + 36,
        center_label,
        request.text_color,
        1,
        None,
    );

    let start_label = section
        .metadata()
        .attribute("start_label")
        .unwrap_or("Start");
    let end_label = section.metadata().attribute("end_label").unwrap_or("End");
    let route_label = section.metadata().attribute("route_label").unwrap_or("");
    let route_y = plot.bottom() as i32 + 62;
    let start_text = format!("A  {start_label}");
    let end_text = format!("B  {end_label}");
    canvas.draw_text(
        plot.x as i32,
        route_y,
        &start_text,
        request.text_color,
        1,
        None,
    );
    let end_width = measure_text_width(&end_text, 1) as i32;
    canvas.draw_text(
        plot.right() as i32 - end_width,
        route_y,
        &end_text,
        request.text_color,
        1,
        None,
    );

    if !route_label.is_empty() {
        let route_width = measure_text_width(route_label, 1) as i32;
        if route_width + 40 < plot.width as i32 {
            canvas.draw_text(
                plot.x as i32 + (plot.width as i32 - route_width) / 2,
                route_y,
                route_label,
                Color::rgb(82, 88, 96),
                1,
                None,
            );
        }
    }
}

fn draw_colorbar(
    canvas: &mut Canvas,
    plot: &PlotRect,
    request: &CrossSectionRenderRequest,
    scene: &ResolvedRenderScene,
) {
    let bar_x = plot.right() + 28;
    let bar_width = 20u32;
    let label_x = bar_x + bar_width + 9;
    if label_x + 20 >= canvas.width {
        return;
    }
    let bar_y = plot.y;
    let bar_height = plot.height.max(40);

    for offset in 0..bar_height {
        let fraction = if bar_height == 1 {
            0.0
        } else {
            1.0 - offset as f32 / (bar_height - 1) as f32
        };
        let value = scene.min_value + fraction * (scene.max_value - scene.min_value);
        let color = map_value_to_color(value, scene.min_value, scene.max_value, &scene.palette);
        for dx in 0..bar_width {
            canvas.set_pixel(bar_x + dx, bar_y + offset, color);
        }
    }

    canvas.draw_rect(
        PlotRect {
            x: bar_x,
            y: bar_y,
            width: bar_width,
            height: bar_height,
        },
        request.frame_color,
        1,
    );

    let label = scene.colorbar_label.as_str();
    let label_width = measure_text_width(label, 1) as i32;
    let label_left = (canvas.width as i32 - label_width - 4)
        .max(4)
        .min(bar_x as i32);
    canvas.draw_text(
        label_left,
        plot.y as i32 - 23,
        label,
        request.text_color,
        1,
        None,
    );

    for &tick in &scene.value_ticks {
        if tick < scene.min_value || tick > scene.max_value {
            continue;
        }
        let fraction = if (scene.max_value - scene.min_value).abs() <= f32::EPSILON {
            0.5
        } else {
            1.0 - ((tick - scene.min_value) / (scene.max_value - scene.min_value)).clamp(0.0, 1.0)
        };
        let y = bar_y as f32 + fraction * (bar_height.saturating_sub(1) as f32);
        canvas.draw_line(
            (bar_x as f64 + bar_width as f64 + 1.0, y as f64),
            (label_x as f64 - 3.0, y as f64),
            request.axis_color.with_alpha(110),
            1,
            None,
        );
        let tick_label = format_scalar_value(tick);
        canvas.draw_text(
            label_x as i32,
            y.round() as i32 - 3,
            &tick_label,
            request.text_color,
            1,
            None,
        );
    }

    if let Some(highlight) = scene.highlight_overlay {
        if highlight >= scene.min_value && highlight <= scene.max_value {
            let fraction = if (scene.max_value - scene.min_value).abs() <= f32::EPSILON {
                0.5
            } else {
                1.0 - ((highlight - scene.min_value) / (scene.max_value - scene.min_value))
                    .clamp(0.0, 1.0)
            };
            let y = bar_y as f32 + fraction * (bar_height.saturating_sub(1) as f32);
            canvas.draw_line(
                (bar_x as f64 - 2.0, y as f64),
                (label_x as f64 + 20.0, y as f64),
                contour_halo_color(request.highlight_isotherm_color).with_alpha(86),
                4,
                None,
            );
            canvas.draw_line(
                (bar_x as f64 - 2.0, y as f64),
                (label_x as f64 + 20.0, y as f64),
                request.highlight_isotherm_color,
                2,
                None,
            );
        }
    }
}

enum LegendSymbol {
    Line(Color),
    Barb(Color),
}

struct LegendEntry {
    label: String,
    symbol: LegendSymbol,
}

fn draw_reference_legend(
    canvas: &mut Canvas,
    plot: &PlotRect,
    request: &CrossSectionRenderRequest,
    scene: &ResolvedRenderScene,
) {
    let mut entries = Vec::new();
    if request.contour_overlays.is_empty() && !scene.overlay_levels.is_empty() {
        entries.push(LegendEntry {
            label: "Contours".to_string(),
            symbol: LegendSymbol::Line(request.isotherm_color),
        });
        if scene.highlight_overlay.is_some() {
            entries.push(LegendEntry {
                label: "0 C Isotherm".to_string(),
                symbol: LegendSymbol::Line(request.highlight_isotherm_color),
            });
        }
    }

    for overlay in &request.contour_overlays {
        if !overlay.levels.is_empty() {
            entries.push(LegendEntry {
                label: overlay
                    .label
                    .clone()
                    .unwrap_or_else(|| "Contours".to_string()),
                symbol: LegendSymbol::Line(overlay.color),
            });
        }
        if overlay.highlight_level.is_some() {
            entries.push(LegendEntry {
                label: overlay
                    .label
                    .clone()
                    .unwrap_or_else(|| "Highlighted Contour".to_string()),
                symbol: LegendSymbol::Line(overlay.highlight_color),
            });
        }
    }

    if let Some(overlay) = request.wind_overlay.as_ref() {
        entries.push(LegendEntry {
            label: "Wind Barbs (kt)".to_string(),
            symbol: LegendSymbol::Barb(overlay.style.color),
        });
    }
    if entries.is_empty() {
        return;
    }

    let max_label_width = entries
        .iter()
        .map(|entry| measure_text_width(&entry.label, 1))
        .max()
        .unwrap_or(0);
    let desired_width = (max_label_width + 64).max(160);
    let max_width = plot.width.saturating_sub(16).max(96);
    let width = desired_width.min(max_width);
    let height = 10 + entries.len() as u32 * 21;
    if height + 16 >= plot.height {
        return;
    }

    let x = plot.x + 10;
    let y = plot.y + 10;
    let rect = PlotRect {
        x,
        y,
        width,
        height,
    };
    canvas.fill_rect(x, y, width, height, Color::rgba(255, 255, 255, 232));
    canvas.draw_rect(rect, request.frame_color.with_alpha(145), 1);

    let legend_clip = PlotRect {
        x,
        y,
        width,
        height,
    };
    for (index, entry) in entries.iter().enumerate() {
        let row_y = y as i32 + 9 + index as i32 * 21;
        match entry.symbol {
            LegendSymbol::Line(color) => {
                canvas.draw_line(
                    (x as f64 + 11.0, row_y as f64 + 5.0),
                    (x as f64 + 34.0, row_y as f64 + 5.0),
                    Color::WHITE.with_alpha(180),
                    3,
                    None,
                );
                canvas.draw_line(
                    (x as f64 + 11.0, row_y as f64 + 5.0),
                    (x as f64 + 34.0, row_y as f64 + 5.0),
                    color,
                    2,
                    None,
                );
            }
            LegendSymbol::Barb(color) => {
                draw_section_wind_barb(
                    canvas,
                    (x as f64 + 22.0, row_y as f64 + 6.0),
                    12.0,
                    0.0,
                    12.0,
                    WindOverlayStyle {
                        min_speed_ms: 0.0,
                        max_speed_ms: 24.0,
                        base_length_px: 18.0,
                        max_length_px: 18.0,
                        cross_tick_px: 6.0,
                        color,
                        ..WindOverlayStyle::default()
                    },
                    &legend_clip,
                );
            }
        }
        canvas.draw_text(
            x as i32 + 44,
            row_y,
            &entry.label,
            request.text_color,
            1,
            None,
        );
    }
}

fn format_header_timing(metadata: &SectionMetadata) -> String {
    let mut parts = Vec::new();
    if let Some(init) = metadata
        .attribute("init_label")
        .or_else(|| metadata.attribute("store_cycle"))
    {
        parts.push(format!("Init: {}", compact_datetime_label(init)));
    }
    if let Some(forecast_hour) = metadata.attribute("forecast_hour") {
        parts.push(forecast_hour.to_string());
    }
    if let Some(valid) = metadata
        .attribute("valid_time")
        .or(metadata.valid_label.as_deref())
    {
        parts.push(format!("Valid: {}", compact_datetime_label(valid)));
    }
    parts.join("   ")
}

fn compact_datetime_label(value: &str) -> String {
    value
        .split_whitespace()
        .map(compact_datetime_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn compact_datetime_token(value: &str) -> String {
    let trimmed = value.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 13
        && bytes[0..8].iter().all(u8::is_ascii_digit)
        && bytes[8] == b'T'
        && bytes[9..11].iter().all(u8::is_ascii_digit)
    {
        return format!(
            "{}-{}-{} {}Z",
            &trimmed[0..4],
            &trimmed[4..6],
            &trimmed[6..8],
            &trimmed[9..11]
        );
    }
    trimmed.to_string()
}

fn terrain_surface_y(axis: &VerticalAxis, plot: &PlotRect, surface_value: f64) -> Option<u32> {
    let maybe_surface_y = if let Some(frac) = axis.plot_fraction_of_value(surface_value) {
        Some(plot.y + (frac * (plot.height as f64 - 1.0)).round() as u32)
    } else {
        match axis.kind() {
            VerticalKind::Pressure if surface_value <= axis.plot_top() => Some(plot.y),
            VerticalKind::Pressure if surface_value >= axis.plot_bottom() => None,
            VerticalKind::Height if surface_value >= axis.plot_top() => Some(plot.y),
            VerticalKind::Height if surface_value <= axis.plot_bottom() => None,
            _ => None,
        }
    };
    maybe_surface_y
}

fn map_value_to_color(value: f32, min_value: f32, max_value: f32, palette: &[Color]) -> Color {
    let fraction = if (max_value - min_value).abs() <= f32::EPSILON {
        0.5
    } else {
        ((value - min_value) / (max_value - min_value)).clamp(0.0, 1.0)
    };

    let scaled = fraction * (palette.len() as f32 - 1.0);
    let left = scaled.floor() as usize;
    let right = scaled.ceil().min((palette.len() - 1) as f32) as usize;
    if left == right {
        palette[left]
    } else {
        palette[left].lerp(palette[right], scaled - left as f32)
    }
}

fn distance_to_pixel(section: &ScalarSection, plot: &PlotRect, distance_km: f64) -> Option<f64> {
    let start = section.distances_km()[0];
    let end = section.distances_km()[section.n_points() - 1];
    if distance_km < start || distance_km > end {
        return None;
    }
    let fraction = if (end - start).abs() <= f64::EPSILON {
        0.0
    } else {
        (distance_km - start) / (end - start)
    };
    Some(plot.x as f64 + fraction * (plot.width as f64 - 1.0))
}

fn axis_value_to_pixel(axis: &VerticalAxis, plot: &PlotRect, value: f64) -> Option<f64> {
    let fraction = axis.plot_fraction_of_value(value)?;
    Some(plot.y as f64 + fraction * (plot.height as f64 - 1.0))
}

fn data_point_to_pixel(
    section: &ScalarSection,
    plot: &PlotRect,
    distance_km: f64,
    axis_value: f64,
) -> Option<(f64, f64)> {
    Some((
        distance_to_pixel(section, plot, distance_km)?,
        axis_value_to_pixel(section.vertical_axis(), plot, axis_value)?,
    ))
}

fn axis_label(axis: &VerticalAxis) -> String {
    match (axis.kind(), axis.units()) {
        (VerticalKind::Pressure, _) => "Pressure (hPa)".to_string(),
        (VerticalKind::Height, VerticalUnits::Meters) => "Height (m)".to_string(),
        (VerticalKind::Height, VerticalUnits::Kilometers) => "Height (km)".to_string(),
        (VerticalKind::Height, VerticalUnits::Hectopascals) => "Height".to_string(),
    }
}

fn vertical_ticks(axis: &VerticalAxis) -> Vec<f64> {
    match axis.kind() {
        VerticalKind::Pressure => {
            let preferred = [
                1000.0, 925.0, 850.0, 700.0, 600.0, 500.0, 400.0, 300.0, 250.0, 200.0, 150.0, 100.0,
            ];
            let min = axis.plot_top().min(axis.plot_bottom());
            let max = axis.plot_top().max(axis.plot_bottom());
            let mut ticks = preferred
                .into_iter()
                .filter(|tick| *tick >= min && *tick <= max)
                .collect::<Vec<_>>();
            if ticks.len() < 3 {
                ticks = axis.levels().to_vec();
            }
            ticks
        }
        VerticalKind::Height => {
            let min = axis.plot_bottom();
            let max = axis.plot_top();
            let step = nice_step((max - min).abs() / 6.0).max(0.5);
            ranged_ticks(min, max, step)
        }
    }
}

fn intermediate_vertical_ticks(axis: &VerticalAxis) -> Vec<f64> {
    let major = vertical_ticks(axis);
    major
        .windows(2)
        .filter_map(|pair| {
            let midpoint = (pair[0] + pair[1]) * 0.5;
            axis.plot_fraction_of_value(midpoint).map(|_| midpoint)
        })
        .collect()
}

fn distance_ticks(distances_km: &[f64], desired_count: usize) -> Vec<f64> {
    let start = distances_km[0];
    let end = distances_km[distances_km.len() - 1];
    let step = nice_step((end - start).abs() / desired_count.max(1) as f64).max(1.0);
    ranged_ticks(start, end, step)
}

fn intermediate_distance_ticks(distances_km: &[f64], desired_count: usize) -> Vec<f64> {
    let major = distance_ticks(distances_km, desired_count);
    major
        .windows(2)
        .map(|pair| (pair[0] + pair[1]) * 0.5)
        .collect()
}

fn nice_value_ticks(min: f32, max: f32, desired_count: usize) -> Vec<f32> {
    let min = min as f64;
    let max = max as f64;
    let step = nice_step((max - min).abs() / desired_count.max(1) as f64).max(1.0);
    ranged_ticks(min, max, step)
        .into_iter()
        .map(|tick| tick as f32)
        .collect()
}

fn ranged_ticks(start: f64, end: f64, step: f64) -> Vec<f64> {
    if !start.is_finite() || !end.is_finite() || !step.is_finite() || step <= 0.0 {
        return Vec::new();
    }

    let min = start.min(end);
    let max = start.max(end);
    let mut ticks = Vec::new();
    let mut tick = (min / step).ceil() * step;
    while tick <= max + step * 0.25 {
        ticks.push((tick * 1000.0).round() / 1000.0);
        tick += step;
    }
    if ticks
        .first()
        .is_none_or(|first| (*first - min).abs() > step * 0.35)
    {
        ticks.insert(0, min);
    }
    if ticks
        .last()
        .is_none_or(|last| (*last - max).abs() > step * 0.35)
    {
        ticks.push(max);
    }
    ticks
}

fn nice_step(raw: f64) -> f64 {
    if !raw.is_finite() || raw <= 0.0 {
        return 1.0;
    }
    let exponent = raw.log10().floor();
    let base = 10f64.powf(exponent);
    let fraction = raw / base;
    let nice = if fraction <= 1.0 {
        1.0
    } else if fraction <= 2.0 {
        2.0
    } else if fraction <= 2.5 {
        2.5
    } else if fraction <= 5.0 {
        5.0
    } else {
        10.0
    };
    nice * base
}

fn normalize_levels(levels: &mut Vec<f32>) {
    levels.retain(|value| value.is_finite());
    levels.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    levels.dedup_by(|left, right| (*left - *right).abs() <= 0.001);
}

fn detect_declared_style(metadata: &SectionMetadata) -> Option<CrossSectionStyle> {
    let mut style = [
        metadata.attribute("product_style"),
        metadata.attribute("render_style"),
        metadata.attribute("product_key"),
        metadata.attribute("field_key"),
        metadata.field_name.as_deref(),
    ]
    .into_iter()
    .flatten()
    .find_map(CrossSectionStyle::from_name)?;
    if let Some(palette) = detect_declared_palette(metadata) {
        style = style.with_palette(palette);
    }
    if let Some(label) = metadata.attribute("colorbar_label") {
        style = style.with_colorbar_label(label);
    }
    Some(style)
}

fn detect_declared_palette(metadata: &SectionMetadata) -> Option<CrossSectionPalette> {
    metadata
        .attribute("palette")
        .or_else(|| metadata.attribute("palette_name"))
        .and_then(CrossSectionPalette::from_name)
}

fn resolve_field_label(
    metadata: &SectionMetadata,
    declared_product: Option<CrossSectionProduct>,
) -> String {
    declared_product
        .map(product_badge_label)
        .map(str::to_string)
        .or_else(|| metadata.field_name.as_deref().map(normalize_field_label))
        .unwrap_or_else(|| "FIELD".to_string())
}

fn product_badge_label(product: CrossSectionProduct) -> &'static str {
    match product {
        CrossSectionProduct::Temperature => "TEMP",
        CrossSectionProduct::WindSpeed => "WIND",
        CrossSectionProduct::ThetaE => "THETA-E",
        CrossSectionProduct::RelativeHumidity => "RH",
        CrossSectionProduct::SpecificHumidity => "Q",
        CrossSectionProduct::Omega => "OMEGA",
        CrossSectionProduct::Vorticity => "VORT",
        CrossSectionProduct::Shear => "SHEAR",
        CrossSectionProduct::LapseRate => "LAPSE RATE",
        CrossSectionProduct::CloudWater => "CLOUD WATER",
        CrossSectionProduct::TotalCondensate => "CONDENSATE",
        CrossSectionProduct::WetBulb => "WET BULB",
        CrossSectionProduct::Icing => "ICING",
        CrossSectionProduct::Frontogenesis => "FRONTO",
        CrossSectionProduct::Smoke => "SMOKE",
        CrossSectionProduct::VaporPressureDeficit => "VPD",
        CrossSectionProduct::DewpointDepression => "DPD",
        CrossSectionProduct::MoistureTransport => "MOISTURE XPORT",
        CrossSectionProduct::PotentialVorticity => "PV",
        CrossSectionProduct::FireWeather => "FIRE WX",
    }
}

fn normalize_field_label(value: &str) -> String {
    CrossSectionProduct::from_name(value)
        .map(product_badge_label)
        .map(str::to_string)
        .unwrap_or_else(|| value.replace(['_', '-'], " ").to_ascii_uppercase())
}

fn normalize_units_label(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "c" | "degc" | "celsius" => "C".to_string(),
        "f" | "degf" | "fahrenheit" => "F".to_string(),
        "k" | "degk" | "kelvin" => "K".to_string(),
        "hpa" => "HPA".to_string(),
        "m" | "meter" | "meters" => "M".to_string(),
        "km" => "KM".to_string(),
        "m/s" | "ms-1" | "mps" | "ms^-1" => "M/S".to_string(),
        "kt" | "kts" | "knot" | "knots" => "KT".to_string(),
        "dbz" => "DBZ".to_string(),
        "%" => "%".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

fn compose_label_with_units(label: &str, units: Option<&str>) -> String {
    match units {
        Some("%") => format!("{label} %"),
        Some(units) if !units.is_empty() => format!("{label} {units}"),
        _ => label.to_string(),
    }
}

fn request_uses_default_palette(request: &CrossSectionRenderRequest) -> bool {
    request.palette == CrossSectionPalette::default().build()
}

fn request_uses_default_overlays(request: &CrossSectionRenderRequest) -> bool {
    let default_style = CrossSectionStyle::default();
    request.isotherms_c.as_slice() == default_style.isotherms_c()
        && request.highlight_isotherm_c == default_style.highlight_isotherm_c()
}

fn contour_halo_color(color: Color) -> Color {
    if color.luminance() >= 0.6 {
        Color::BLACK
    } else {
        Color::WHITE
    }
}

fn format_axis_tick(axis: &VerticalAxis, value: f64) -> String {
    match axis.units() {
        VerticalUnits::Kilometers => format_scalar_value(value as f32),
        _ => format!("{value:.0}"),
    }
}

fn format_distance_tick(value: f64) -> String {
    format!("{value:.0}")
}

fn format_scalar_value(value: f32) -> String {
    if (value - value.round()).abs() <= 0.05 {
        format!("{value:.0}")
    } else if value.abs() >= 100.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn contour_label_for_level(value: f32, units: Option<&str>) -> String {
    match units.map(str::trim).filter(|units| !units.is_empty()) {
        Some(units) if units.eq_ignore_ascii_case("c") => {
            format!("{}C", format_scalar_value(value))
        }
        _ => format_scalar_value(value),
    }
}

#[cfg(test)]
fn badge_rect(x: i32, y: i32, text: &str, scale: u32) -> (u32, u32, u32, u32) {
    let pad_x = 4 * scale.max(1);
    let width = measure_text_width(text, scale) + pad_x * 2;
    let height = text_line_height(scale) + 4;
    (x.max(0) as u32, y.max(0) as u32, width, height)
}

#[cfg(test)]
fn measure_badge_width(text: &str, scale: u32) -> u32 {
    badge_rect(0, 0, text, scale).2
}

fn measure_text_width(text: &str, scale: u32) -> u32 {
    if let Some(font) = cross_section_font(scale) {
        let font_kind = font_kind_for_scale(scale);
        let scale = Scale::uniform(cross_section_font_size_px(scale, font_kind));
        let v_metrics = font.v_metrics(scale);
        return text
            .split('\n')
            .map(|line| {
                let glyphs: Vec<_> = font
                    .layout(line, scale, point(0.0, v_metrics.ascent))
                    .collect();
                glyphs
                    .iter()
                    .rev()
                    .find_map(|glyph| glyph.pixel_bounding_box().map(|bb| bb.max.x.max(0) as u32))
                    .or_else(|| {
                        glyphs.last().map(|glyph| {
                            let end =
                                glyph.position().x + glyph.unpositioned().h_metrics().advance_width;
                            end.max(0.0).ceil() as u32
                        })
                    })
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0);
    }

    text.chars().count() as u32 * 6 * scale.max(1)
}

#[cfg(test)]
fn text_line_height(scale: u32) -> u32 {
    if let Some(font) = cross_section_font(scale) {
        let font_kind = font_kind_for_scale(scale);
        let scale = Scale::uniform(cross_section_font_size_px(scale, font_kind));
        return cross_section_line_height_px(scale, font).ceil() as u32;
    }

    8 * scale.max(1)
}

fn font_kind_for_scale(scale: u32) -> CrossSectionFontKind {
    if scale >= 2 {
        CrossSectionFontKind::Semibold
    } else {
        CrossSectionFontKind::Regular
    }
}

fn cross_section_font(scale: u32) -> Option<&'static Font<'static>> {
    let fonts = CROSS_SECTION_FONTS.get_or_init(load_cross_section_fonts);
    match font_kind_for_scale(scale) {
        CrossSectionFontKind::Regular => fonts.regular.as_ref(),
        CrossSectionFontKind::Semibold => fonts.semibold.as_ref().or(fonts.regular.as_ref()),
    }
}

fn load_cross_section_fonts() -> CrossSectionFontSet {
    CrossSectionFontSet {
        regular: Font::try_from_bytes(SOURCE_SANS_3_REGULAR),
        semibold: Font::try_from_bytes(SOURCE_SANS_3_SEMIBOLD),
    }
}

fn cross_section_font_size_px(scale: u32, kind: CrossSectionFontKind) -> f32 {
    match (scale.max(1), kind) {
        (1, CrossSectionFontKind::Regular) => 14.0,
        (1, CrossSectionFontKind::Semibold) => 16.0,
        (2, CrossSectionFontKind::Regular) => 18.0,
        (2, CrossSectionFontKind::Semibold) => 22.0,
        (level, CrossSectionFontKind::Regular) => 14.0 + (level as f32 - 1.0) * 4.0,
        (level, CrossSectionFontKind::Semibold) => 16.0 + (level as f32 - 1.0) * 4.5,
    }
}

fn cross_section_line_height_px(scale: Scale, font: &Font<'static>) -> f32 {
    let v_metrics = font.v_metrics(scale);
    (v_metrics.ascent - v_metrics.descent + v_metrics.line_gap).max(scale.y + 2.0)
}

fn glyph_rows(ch: char) -> [u8; 7] {
    match ch.to_ascii_uppercase() {
        'A' => [0x0E, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        'B' => [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        'C' => [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        'D' => [0x1E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x1E],
        'E' => [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        'F' => [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        'G' => [0x0F, 0x10, 0x10, 0x17, 0x11, 0x11, 0x0F],
        'H' => [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        'I' => [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        'J' => [0x01, 0x01, 0x01, 0x01, 0x11, 0x11, 0x0E],
        'K' => [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        'L' => [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        'M' => [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        'N' => [0x11, 0x11, 0x19, 0x15, 0x13, 0x11, 0x11],
        'O' => [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        'P' => [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        'Q' => [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        'R' => [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        'S' => [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        'T' => [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        'U' => [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        'V' => [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        'W' => [0x11, 0x11, 0x11, 0x15, 0x15, 0x15, 0x0A],
        'X' => [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        'Y' => [0x11, 0x11, 0x0A, 0x04, 0x04, 0x04, 0x04],
        'Z' => [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        '0' => [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        '1' => [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        '2' => [0x0E, 0x11, 0x01, 0x02, 0x04, 0x08, 0x1F],
        '3' => [0x1E, 0x01, 0x01, 0x0E, 0x01, 0x01, 0x1E],
        '4' => [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        '5' => [0x1F, 0x10, 0x10, 0x1E, 0x01, 0x01, 0x1E],
        '6' => [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        '7' => [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        '8' => [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        '9' => [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        '-' => [0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00],
        '.' => [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        ':' => [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x0C, 0x00],
        ',' => [0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C, 0x08],
        '/' => [0x01, 0x02, 0x04, 0x04, 0x08, 0x10, 0x10],
        '|' => [0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        ' ' => [0x00; 7],
        _ => [0x1F, 0x01, 0x02, 0x04, 0x04, 0x00, 0x04],
    }
}

#[cfg(test)]
mod tests;
