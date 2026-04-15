#[path = "../../src/error.rs"]
mod error;
#[path = "../../src/request.rs"]
mod request;
#[path = "../../src/solar07.rs"]
pub mod solar07;

pub use error::RustwxRenderError;
pub use request::{
    Color, ColorScale, ContourLayer, ContourStyle, DiscreteColorScale, ExtendMode, Field2D,
    GridShape, LatLonGrid, MapRenderRequest, ProductKey, ProjectedDomain, ProjectedExtent,
    ProjectedLineOverlay, WindBarbLayer, WindBarbStyle,
};

use std::path::Path;
use wrf_render::{
    render_to_png, BarbOverlay, ContourOverlay, Extend, LeveledColormap, MapExtent, ProjectedGrid,
    ProjectedPolyline, RenderOpts, Rgba,
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
        use crate::solar07::Solar07Preset;

        assert_eq!(
            Solar07Preset::from_product_name("sbecape"),
            Some(Solar07Preset::Cape)
        );
        assert_eq!(
            Solar07Preset::from_product_name("mlecin"),
            Some(Solar07Preset::Cin)
        );
        assert_eq!(
            Solar07Preset::from_product_name("ecape_scp"),
            Some(Solar07Preset::Scp)
        );
        assert_eq!(
            Solar07Preset::from_product_name("ecape_ehi"),
            Some(Solar07Preset::Ehi)
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
        let request = MapRenderRequest::new(
            sample_field("scp"),
            ColorScale::Solar07(crate::solar07::Solar07Preset::Scp),
        );

        let path = std::env::temp_dir().join(format!("rustwx-render-{}.png", std::process::id()));
        save_png(&request, &path).unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(&[137, 80, 78, 71, 13, 10, 26, 10]));

        let _ = std::fs::remove_file(path);
    }
}
