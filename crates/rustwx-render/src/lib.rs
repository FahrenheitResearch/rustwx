mod error;
mod panel;
mod request;
pub mod solar07;

pub use error::RustwxRenderError;
pub use image::RgbaImage;
pub use panel::{PanelGridLayout, PanelPadding, compose_panel_images, render_panel_grid};
pub use request::{
    Color, ColorScale, ContourLayer, ContourStyle, DiscreteColorScale, ExtendMode, Field2D,
    GridShape, LatLonGrid, MapRenderRequest, ProductKey, ProjectedDomain, ProjectedExtent,
    ProjectedLineOverlay, WindBarbLayer, WindBarbStyle,
};
pub use rustwx_core::{
    Field2D as CoreField2D, GridShape as CoreGridShape, LatLonGrid as CoreLatLonGrid,
    ProductKey as CoreProductKey,
};
pub use solar07::{
    DerivedProductStyle, DerivedScalePreset, ECAPE_SEVERE_PANEL_PRODUCTS,
    SEVERE_CLASSIC_PANEL_PRODUCTS, Solar07Palette, Solar07Preset, Solar07Product, palette_scale,
};

use image::ImageFormat;
use std::path::Path;
use wrf_render::{
    BarbOverlay, ContourOverlay, Extend, LeveledColormap, MapExtent, ProjectedGrid,
    ProjectedPolyline, RenderOpts, Rgba, render_to_png,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct RustRenderer;

impl RustRenderer {
    pub fn render_png(self, request: &MapRenderRequest) -> Result<Vec<u8>, RustwxRenderError> {
        validate_request(request)?;

        let shape = request.field.grid.shape;
        let cmap = build_colormap(&request.scale);
        let projected_domain = request.projected_domain.as_ref();

        let opts = RenderOpts {
            width: request.width,
            height: request.height,
            cmap,
            background: request.background.into(),
            colorbar: request.colorbar,
            title: request
                .title
                .clone()
                .or_else(|| default_title(&request.field)),
            subtitle_left: request.subtitle_left.clone(),
            subtitle_right: request.subtitle_right.clone(),
            cbar_tick_step: request.cbar_tick_step,
            map_extent: projected_domain.map(|domain| MapExtent {
                x_min: domain.extent.x_min,
                x_max: domain.extent.x_max,
                y_min: domain.extent.y_min,
                y_max: domain.extent.y_max,
            }),
            projected_grid: projected_domain.map(|domain| ProjectedGrid {
                x: domain.x.clone(),
                y: domain.y.clone(),
                ny: shape.ny,
                nx: shape.nx,
            }),
            projected_lines: request
                .projected_lines
                .iter()
                .map(|line| ProjectedPolyline {
                    points: line.points.clone(),
                    color: line.color.into(),
                    width: line.width,
                })
                .collect(),
            contours: request
                .contours
                .iter()
                .map(|layer| ContourOverlay {
                    data: layer.data.iter().map(|v| *v as f64).collect(),
                    ny: shape.ny,
                    nx: shape.nx,
                    levels: layer.levels.clone(),
                    color: layer.color.into(),
                    width: layer.width,
                    labels: layer.labels,
                    show_extrema: layer.show_extrema,
                })
                .collect(),
            barbs: request
                .wind_barbs
                .iter()
                .map(|layer| BarbOverlay {
                    u: layer.u.iter().map(|v| *v as f64).collect(),
                    v: layer.v.iter().map(|v| *v as f64).collect(),
                    ny: shape.ny,
                    nx: shape.nx,
                    stride_x: layer.stride_x,
                    stride_y: layer.stride_y,
                    color: layer.color.into(),
                    width: layer.width,
                    length_px: layer.length_px,
                })
                .collect(),
        };

        let data: Vec<f64> = request.field.values.iter().map(|v| *v as f64).collect();
        Ok(render_to_png(&data, shape.ny, shape.nx, &opts))
    }

    pub fn render_image(self, request: &MapRenderRequest) -> Result<RgbaImage, RustwxRenderError> {
        let png = self.render_png(request)?;
        image::load_from_memory_with_format(&png, ImageFormat::Png)
            .map(|image| image.to_rgba8())
            .map_err(|source| RustwxRenderError::DecodeRenderedPng { source })
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

#[cfg(test)]
mod tests {
    use super::*;

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
            projected_lines: Vec::new(),
            contours: Vec::new(),
            wind_barbs: Vec::new(),
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
            projected_lines: Vec::new(),
            contours: Vec::new(),
            wind_barbs: Vec::new(),
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
