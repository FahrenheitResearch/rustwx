mod color;
mod colorbar;
mod colormap;
mod colormaps;
mod draw;
mod error;
mod features;
mod overlay;
mod panel;
mod projected_map;
mod projection;
mod rasterize;
mod render;
mod request;
mod text;
pub mod solar07;

pub use error::RustwxRenderError;
pub use image::RgbaImage;
pub use panel::{PanelGridLayout, PanelPadding, compose_panel_images, render_panel_grid};
pub use projected_map::{ProjectedMap, build_projected_map};
pub use features::{
    BasemapStyle, StyledLonLatLayer, StyledLonLatPolygonLayer, load_styled_conus_features_for,
    load_styled_conus_polygons_for,
};
pub use projection::LambertConformal;
pub use request::{
    Color, ColorScale, ContourLayer, ContourStyle, DiscreteColorScale, ExtendMode, Field2D,
    GridShape, LatLonGrid, MapRenderRequest, ProductKey, ProductMaturity, ProductSemanticFlag,
    ProductSemantics, ProjectedDomain, ProjectedExtent, ProjectedLineOverlay,
    ProjectedPolygonFill, WindBarbLayer, WindBarbStyle,
};
pub use render::map_frame_aspect_ratio;
pub use rustwx_core::{
    Field2D as CoreField2D, GridShape as CoreGridShape, LatLonGrid as CoreLatLonGrid,
    ProductKey as CoreProductKey,
};
pub use solar07::{
    DerivedProductStyle, DerivedScalePreset, ECAPE_SEVERE_PANEL_PRODUCTS,
    SEVERE_CLASSIC_PANEL_PRODUCTS, Solar07Palette, Solar07Preset, Solar07Product, palette_scale,
};

use std::cell::RefCell;
use std::path::Path;
use std::sync::OnceLock;
use crate::color::Rgba;
use crate::colormap::{Extend, LeveledColormap};
use crate::overlay::{
    BarbOverlay, ContourOverlay, MapExtent, ProjectedGrid, ProjectedPolygon, ProjectedPolyline,
};
use crate::render::{RenderOpts, render_to_image as native_render_to_image, render_to_png};

#[derive(Debug, Default, Clone, Copy)]
pub struct RustRenderer;

#[derive(Default)]
struct RenderScratch {
    f64_buffers: Vec<Vec<f64>>,
    point_buffers: Vec<Vec<(f64, f64)>>,
}

impl RenderScratch {
    fn take_f64_buffer(&mut self, len: usize) -> Vec<f64> {
        let mut buffer = self.f64_buffers.pop().unwrap_or_default();
        buffer.clear();
        if buffer.capacity() < len {
            buffer.reserve(len - buffer.capacity());
        }
        buffer
    }

    fn fill_f64_from_f32(&mut self, src: &[f32]) -> Vec<f64> {
        let mut buffer = self.take_f64_buffer(src.len());
        buffer.extend(src.iter().map(|&value| value as f64));
        buffer
    }

    fn fill_f64_from_f64(&mut self, src: &[f64]) -> Vec<f64> {
        let mut buffer = self.take_f64_buffer(src.len());
        buffer.extend_from_slice(src);
        buffer
    }

    fn fill_f64_constant(&mut self, len: usize, value: f64) -> Vec<f64> {
        let mut buffer = self.take_f64_buffer(len);
        buffer.resize(len, value);
        buffer
    }

    fn reclaim_f64_buffer(&mut self, mut buffer: Vec<f64>) {
        buffer.clear();
        self.f64_buffers.push(buffer);
    }

    fn take_point_buffer(&mut self, len: usize) -> Vec<(f64, f64)> {
        let mut buffer = self.point_buffers.pop().unwrap_or_default();
        buffer.clear();
        if buffer.capacity() < len {
            buffer.reserve(len - buffer.capacity());
        }
        buffer
    }

    fn fill_point_buffer(&mut self, src: &[(f64, f64)]) -> Vec<(f64, f64)> {
        let mut buffer = self.take_point_buffer(src.len());
        buffer.extend_from_slice(src);
        buffer
    }

    fn reclaim_point_buffer(&mut self, mut buffer: Vec<(f64, f64)>) {
        buffer.clear();
        self.point_buffers.push(buffer);
    }

    fn reclaim_render_opts(&mut self, mut opts: RenderOpts, data: Vec<f64>) {
        self.reclaim_f64_buffer(data);

        if let Some(grid) = opts.projected_grid.take() {
            self.reclaim_f64_buffer(grid.x);
            self.reclaim_f64_buffer(grid.y);
        }

        for line in opts.projected_lines.drain(..) {
            self.reclaim_point_buffer(line.points);
        }

        for poly in opts.projected_polygons.drain(..) {
            for ring in poly.rings {
                self.reclaim_point_buffer(ring);
            }
        }

        for contour in opts.contours.drain(..) {
            self.reclaim_f64_buffer(contour.data);
            self.reclaim_f64_buffer(contour.levels);
        }

        for barb in opts.barbs.drain(..) {
            self.reclaim_f64_buffer(barb.u);
            self.reclaim_f64_buffer(barb.v);
        }
    }
}

thread_local! {
    static RENDER_SCRATCH: RefCell<RenderScratch> = RefCell::new(RenderScratch::default());
}

impl RustRenderer {
    pub fn render_png(self, request: &MapRenderRequest) -> Result<Vec<u8>, RustwxRenderError> {
        with_render_state(request, |data, ny, nx, opts| {
            Ok(render_to_png(data, ny, nx, opts))
        })
    }

    pub fn render_image(self, request: &MapRenderRequest) -> Result<RgbaImage, RustwxRenderError> {
        with_render_state(request, |data, ny, nx, opts| {
            Ok(native_render_to_image(data, ny, nx, opts))
        })
    }

    pub fn save_png<P: AsRef<Path>>(
        self,
        request: &MapRenderRequest,
        output_path: P,
    ) -> Result<(), RustwxRenderError> {
        let bytes = self.render_png(request)?;
        let path = output_path.as_ref();
        std::fs::write(path, bytes).map_err(|source| RustwxRenderError::WriteFile {
            path: path.display().to_string(),
            source,
        })
    }
}

pub fn render_png(request: &MapRenderRequest) -> Result<Vec<u8>, RustwxRenderError> {
    RustRenderer.render_png(request)
}

pub fn render_image(request: &MapRenderRequest) -> Result<RgbaImage, RustwxRenderError> {
    RustRenderer.render_image(request)
}

pub fn save_png<P: AsRef<Path>>(
    request: &MapRenderRequest,
    output_path: P,
) -> Result<(), RustwxRenderError> {
    RustRenderer.save_png(request, output_path)
}

fn with_render_state<T>(
    request: &MapRenderRequest,
    render: impl FnOnce(&[f64], usize, usize, &RenderOpts) -> Result<T, RustwxRenderError>,
) -> Result<T, RustwxRenderError> {
    validate_request(request)?;

    let shape = request.field.grid.shape;
    let overlay_only = request.is_overlay_only();
    let cmap = if overlay_only {
        blank_fill_colormap()
    } else {
        build_colormap(&request.scale)
    };
    let projected_domain = request.projected_domain.as_ref();
    let default_title = default_title(&request.field);

    RENDER_SCRATCH.with(|scratch_cell| {
        let mut scratch = scratch_cell.borrow_mut();

        let data = if overlay_only {
            scratch.fill_f64_constant(shape.len(), OVERLAY_ONLY_FILL_VALUE)
        } else {
            scratch.fill_f64_from_f32(&request.field.values)
        };

        let projected_grid = projected_domain.map(|domain| ProjectedGrid {
            x: scratch.fill_f64_from_f64(&domain.x),
            y: scratch.fill_f64_from_f64(&domain.y),
            ny: shape.ny,
            nx: shape.nx,
        });

        let mut projected_lines = Vec::with_capacity(request.projected_lines.len());
        for line in &request.projected_lines {
            projected_lines.push(ProjectedPolyline {
                points: scratch.fill_point_buffer(&line.points),
                color: line.color.into(),
                width: line.width,
            });
        }

        let mut projected_polygons = Vec::with_capacity(request.projected_polygons.len());
        for poly in &request.projected_polygons {
            let rings = poly
                .rings
                .iter()
                .map(|ring| scratch.fill_point_buffer(ring))
                .collect();
            projected_polygons.push(ProjectedPolygon {
                rings,
                color: poly.color.into(),
            });
        }

        let mut contours = Vec::with_capacity(request.contours.len());
        for layer in &request.contours {
            contours.push(ContourOverlay {
                data: scratch.fill_f64_from_f32(&layer.data),
                ny: shape.ny,
                nx: shape.nx,
                levels: scratch.fill_f64_from_f64(&layer.levels),
                color: layer.color.into(),
                width: layer.width,
                labels: layer.labels,
                show_extrema: layer.show_extrema,
            });
        }

        let mut barbs = Vec::with_capacity(request.wind_barbs.len());
        for layer in &request.wind_barbs {
            barbs.push(BarbOverlay {
                u: scratch.fill_f64_from_f32(&layer.u),
                v: scratch.fill_f64_from_f32(&layer.v),
                ny: shape.ny,
                nx: shape.nx,
                stride_x: layer.stride_x,
                stride_y: layer.stride_y,
                color: layer.color.into(),
                width: layer.width,
                length_px: layer.length_px,
            });
        }

        let opts = RenderOpts {
            width: request.width,
            height: request.height,
            cmap,
            background: request.background.into(),
            colorbar: request.colorbar,
            title: request.title.clone().or(default_title),
            subtitle_left: request.subtitle_left.clone(),
            subtitle_right: request.subtitle_right.clone(),
            cbar_tick_step: request.cbar_tick_step,
            map_extent: projected_domain.map(|domain| MapExtent {
                x_min: domain.extent.x_min,
                x_max: domain.extent.x_max,
                y_min: domain.extent.y_min,
                y_max: domain.extent.y_max,
            }),
            projected_grid,
            projected_polygons,
            projected_lines,
            contours,
            barbs,
        };

        let result = render(&data, shape.ny, shape.nx, &opts);
        scratch.reclaim_render_opts(opts, data);
        result
    })
}

fn build_colormap(scale: &ColorScale) -> LeveledColormap {
    let discrete = match scale {
        ColorScale::Solar07(preset) => preset.scale(),
        ColorScale::Discrete(scale) => scale.clone(),
    };

    let colors: Vec<Rgba> = discrete.colors.into_iter().map(Into::into).collect();
    LeveledColormap::from_palette(
        &colors,
        &discrete.levels,
        discrete.extend.into(),
        discrete.mask_below,
    )
}

const OVERLAY_ONLY_FILL_VALUE: f64 = 0.5;

fn blank_fill_colormap() -> LeveledColormap {
    static BLANK_FILL_COLORMAP: OnceLock<LeveledColormap> = OnceLock::new();
    BLANK_FILL_COLORMAP
        .get_or_init(|| {
            LeveledColormap::from_palette(&[Rgba::WHITE], &[0.0, 1.0], Extend::Neither, None)
        })
        .clone()
}

fn default_title(field: &Field2D) -> Option<String> {
    match &field.product {
        ProductKey::Named(name) if !name.is_empty() => Some(name.clone()),
        _ => None,
    }
}

fn validate_request(request: &MapRenderRequest) -> Result<(), RustwxRenderError> {
    let expected = request.field.grid.shape.len();

    if let Some(domain) = &request.projected_domain {
        if request.field.grid.shape.nx < 2 || request.field.grid.shape.ny < 2 {
            return Err(RustwxRenderError::DegenerateProjectedGrid);
        }
        if domain.x.len() != domain.y.len() {
            return Err(RustwxRenderError::InvalidProjectedGrid);
        }
        if domain.x.len() != expected {
            return Err(RustwxRenderError::LayerShapeMismatch {
                layer: "projected_domain",
                expected,
                actual: domain.x.len(),
            });
        }
    }

    for layer in &request.contours {
        if layer.data.len() != expected {
            return Err(RustwxRenderError::LayerShapeMismatch {
                layer: "contour",
                expected,
                actual: layer.data.len(),
            });
        }
    }

    for layer in &request.wind_barbs {
        if layer.u.len() != expected {
            return Err(RustwxRenderError::LayerShapeMismatch {
                layer: "wind_barb_u",
                expected,
                actual: layer.u.len(),
            });
        }
        if layer.v.len() != expected {
            return Err(RustwxRenderError::LayerShapeMismatch {
                layer: "wind_barb_v",
                expected,
                actual: layer.v.len(),
            });
        }
    }

    Ok(())
}

impl From<Color> for Rgba {
    fn from(value: Color) -> Self {
        Self {
            r: value.r,
            g: value.g,
            b: value.b,
            a: value.a,
        }
    }
}

impl From<Rgba> for Color {
    fn from(value: Rgba) -> Self {
        Self {
            r: value.r,
            g: value.g,
            b: value.b,
            a: value.a,
        }
    }
}

impl From<ExtendMode> for Extend {
    fn from(value: ExtendMode) -> Self {
        match value {
            ExtendMode::Neither => Self::Neither,
            ExtendMode::Min => Self::Min,
            ExtendMode::Max => Self::Max,
            ExtendMode::Both => Self::Both,
        }
    }
}

pub fn draw_centered_text_line(
    img: &mut RgbaImage,
    text: &str,
    y: i32,
    color: Color,
    scale: u32,
) {
    text::draw_text_centered(img, text, y, color.into(), scale);
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::ImageFormat;

    fn sample_field(product: &str) -> Field2D {
        let shape = GridShape::new(4, 3).unwrap();
        let lat = vec![35.0; shape.len()];
        let lon = vec![-97.0; shape.len()];
        let grid = LatLonGrid::new(shape, lat, lon).unwrap();
        let values = vec![
            0.0, 250.0, 750.0, 1500.0, 2000.0, 2400.0, 2600.0, 2800.0, 3000.0, 3200.0, 3400.0,
            3600.0,
        ];
        Field2D::new(ProductKey::named(product), "J/kg", grid, values).unwrap()
    }

    #[test]
    fn solar07_product_mapping_covers_ecape_and_severe_aliases() {
        assert_eq!(
            Solar07Product::from_product_name("sbecape"),
            Some(Solar07Product::Sbecape)
        );
        assert_eq!(
            Solar07Product::from_product_name("mlecin"),
            Some(Solar07Product::Mlecin)
        );
        assert_eq!(
            Solar07Product::from_product_name("ecape_scp"),
            Some(Solar07Product::EcapeScpExperimental)
        );
        assert_eq!(
            Solar07Product::from_product_name("ecape_ehi"),
            Some(Solar07Product::EcapeEhiExperimental)
        );
    }

    #[test]
    fn render_png_emits_valid_nonempty_image() {
        let request = MapRenderRequest {
            field: sample_field("sbecape"),
            product_metadata: None,
            width: 320,
            height: 240,
            scale: ColorScale::Solar07(crate::solar07::Solar07Preset::Cape),
            background: Color::WHITE,
            colorbar: true,
            title: Some("SBECAPE".into()),
            subtitle_left: Some("HRRR 2026-04-14 20Z F00".into()),
            subtitle_right: Some("rustwx-render".into()),
            cbar_tick_step: Some(500.0),
            projected_domain: None,
            projected_polygons: Vec::new(),
            projected_lines: Vec::new(),
            contours: Vec::new(),
            wind_barbs: Vec::new(),
            semantics: None,
        };

        let png = render_png(&request).unwrap();
        assert!(png.starts_with(&[137, 80, 78, 71, 13, 10, 26, 10]));

        let image = image::load_from_memory_with_format(&png, ImageFormat::Png)
            .unwrap()
            .to_rgba8();
        assert_eq!(image.width(), 320);
        assert_eq!(image.height(), 240);

        let non_white = image
            .pixels()
            .filter(|px| px.0 != [255, 255, 255, 255])
            .count();
        assert!(non_white > 1000, "image should contain rendered content");
    }

    #[test]
    fn save_png_writes_file() {
        let request =
            MapRenderRequest::for_solar07_product(sample_field("scp"), Solar07Product::Scp);

        let path = std::env::temp_dir().join(format!("rustwx-render-{}.png", std::process::id()));
        save_png(&request, &path).unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(&[137, 80, 78, 71, 13, 10, 26, 10]));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn render_image_emits_rgba_canvas_without_png_decode_in_callers() {
        let request = MapRenderRequest {
            field: sample_field("mucape"),
            product_metadata: None,
            width: 320,
            height: 240,
            scale: ColorScale::Solar07(crate::solar07::Solar07Preset::Cape),
            background: Color::WHITE,
            colorbar: false,
            title: Some("MUCAPE".into()),
            subtitle_left: None,
            subtitle_right: None,
            cbar_tick_step: Some(500.0),
            projected_domain: None,
            projected_polygons: Vec::new(),
            projected_lines: Vec::new(),
            contours: Vec::new(),
            wind_barbs: Vec::new(),
            semantics: None,
        };

        let image = render_image(&request).unwrap();
        assert_eq!(image.width(), 320);
        assert_eq!(image.height(), 240);

        let non_white = image
            .pixels()
            .filter(|px| px.0 != [255, 255, 255, 255])
            .count();
        assert!(non_white > 1000, "image should contain rendered content");
    }

    #[test]
    fn for_solar07_product_sets_expected_titles_for_experimental_fields() {
        let request = MapRenderRequest::for_solar07_product(
            sample_field("ecape_scp"),
            Solar07Product::EcapeScpExperimental,
        );

        assert_eq!(request.title.as_deref(), Some("ECAPE SCP (EXP)"));
        assert_eq!(request.cbar_tick_step, Some(1.0));
        assert!(matches!(
            request.scale,
            ColorScale::Solar07(Solar07Preset::Scp)
        ));
    }

    #[test]
    fn derived_product_builder_renders_signed_field_with_builtin_scale() {
        let shape = GridShape::new(4, 3).unwrap();
        let lat = vec![35.0; shape.len()];
        let lon = vec![-97.0; shape.len()];
        let grid = LatLonGrid::new(shape, lat, lon).unwrap();
        let field = Field2D::new(
            ProductKey::named("temperature_advection_850mb"),
            "K/hr",
            grid,
            vec![
                -10.0, -8.0, -6.0, -4.0, -2.0, 0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0,
            ],
        )
        .unwrap();

        let request = MapRenderRequest::for_derived_product(
            field,
            DerivedProductStyle::TemperatureAdvection850mb,
        );
        let image = render_image(&request).unwrap();

        let non_white = image
            .pixels()
            .filter(|px| px.0 != [255, 255, 255, 255])
            .count();
        assert!(non_white > 1000, "derived render should contain content");
    }

    #[test]
    fn contour_only_map_with_height_contours_and_barbs_renders_visible_overlays() {
        let base = sample_field("height");
        let contours = sample_field("height_contours");
        let u = sample_field("u_wind");
        let mut v = sample_field("v_wind");
        v.values.iter_mut().for_each(|value| *value = 10.0);

        let request = MapRenderRequest::contour_only(base)
            .with_contour_field(
                &contours,
                vec![500.0, 1500.0, 2500.0, 3500.0],
                ContourStyle {
                    labels: true,
                    ..Default::default()
                },
            )
            .unwrap()
            .with_wind_barbs(
                &u,
                &v,
                WindBarbStyle {
                    stride_x: 2,
                    stride_y: 2,
                    ..Default::default()
                },
            )
            .unwrap();

        let image = render_image(&request).unwrap();
        let non_white = image
            .pixels()
            .filter(|px| px.0 != [255, 255, 255, 255])
            .count();
        assert!(
            non_white > 1000,
            "overlay-only render should remain visible"
        );
    }
}
