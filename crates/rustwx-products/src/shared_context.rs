use image::DynamicImage;
use rustwx_core::{Field2D, LatLonGrid, ProductKey};
pub use rustwx_render::ProjectedMap;
use rustwx_render::{
    Color, DomainFrame, MapRenderRequest, PanelGridLayout, PanelPadding, ProductVisualMode,
    ProjectedDomain, WeatherProduct, draw_centered_text_line, map_frame_aspect_ratio_for_mode,
    render_panel_grid,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DomainSpec {
    pub slug: String,
    pub bounds: (f64, f64, f64, f64),
}

impl DomainSpec {
    pub fn new<S: Into<String>>(slug: S, bounds: (f64, f64, f64, f64)) -> Self {
        Self {
            slug: slug.into(),
            bounds,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PreparedProjectedContext {
    projected_maps: HashMap<(u32, u32), ProjectedMap>,
}

pub trait ProjectedMapProvider: Sync {
    fn projected_map(&self, width: u32, height: u32) -> Option<&ProjectedMap>;
}

impl PreparedProjectedContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn projected_map(&self, width: u32, height: u32) -> Option<&ProjectedMap> {
        self.projected_maps.get(&(width, height))
    }

    pub fn insert(&mut self, width: u32, height: u32, projected: ProjectedMap) {
        self.projected_maps.insert((width, height), projected);
    }

    pub fn contains_size(&self, width: u32, height: u32) -> bool {
        self.projected_maps.contains_key(&(width, height))
    }
}

impl ProjectedMapProvider for PreparedProjectedContext {
    fn projected_map(&self, width: u32, height: u32) -> Option<&ProjectedMap> {
        self.projected_map(width, height)
    }
}

#[derive(Debug, Clone)]
pub struct WeatherPanelField {
    pub product: WeatherProduct,
    pub artifact_slug: Option<String>,
    pub title_override: Option<String>,
    pub units: String,
    pub values: Vec<f64>,
}

impl WeatherPanelField {
    pub fn new<S: Into<String>>(product: WeatherProduct, units: S, values: Vec<f64>) -> Self {
        Self {
            product,
            artifact_slug: None,
            title_override: None,
            units: units.into(),
            values,
        }
    }

    pub fn with_artifact_slug<S: Into<String>>(mut self, slug: S) -> Self {
        self.artifact_slug = Some(slug.into());
        self
    }

    pub fn with_title_override<S: Into<String>>(mut self, title: S) -> Self {
        self.title_override = Some(title.into());
        self
    }

    pub fn artifact_slug(&self) -> &str {
        self.artifact_slug
            .as_deref()
            .unwrap_or_else(|| self.product.slug())
    }

    pub fn display_title(&self) -> &str {
        self.title_override
            .as_deref()
            .unwrap_or_else(|| self.product.display_title())
    }
}

#[derive(Debug, Clone, Default)]
pub struct WeatherPanelHeader {
    pub title: String,
    pub subtitle_lines: Vec<String>,
}

impl WeatherPanelHeader {
    pub fn new<S: Into<String>>(title: S) -> Self {
        Self {
            title: title.into(),
            subtitle_lines: Vec::new(),
        }
    }

    pub fn with_subtitle_line<S: Into<String>>(mut self, line: S) -> Self {
        self.subtitle_lines.push(line.into());
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WeatherPanelLayout {
    pub panel_width: u32,
    pub panel_height: u32,
    pub top_padding: u32,
}

impl Default for WeatherPanelLayout {
    fn default() -> Self {
        Self {
            panel_width: 700,
            panel_height: 520,
            top_padding: 70,
        }
    }
}

impl WeatherPanelLayout {
    pub fn target_aspect_ratio(self) -> f64 {
        map_frame_aspect_ratio_for_mode(
            ProductVisualMode::PanelMember,
            self.panel_width,
            self.panel_height,
            true,
            true,
        )
    }
}

pub fn layout_key(layout: WeatherPanelLayout) -> (u32, u32, u32) {
    (layout.panel_width, layout.panel_height, layout.top_padding)
}

pub fn build_weather_map_request(
    grid: &LatLonGrid,
    projected: &ProjectedMap,
    field_spec: &WeatherPanelField,
    width: u32,
    height: u32,
    subtitle_left: Option<String>,
    subtitle_right: Option<String>,
) -> Result<MapRenderRequest, Box<dyn std::error::Error>> {
    let field = Field2D::new(
        ProductKey::named(field_spec.product.slug()),
        field_spec.units.clone(),
        grid.clone(),
        field_spec.values.iter().map(|&v| v as f32).collect(),
    )?;
    let mut request = MapRenderRequest::for_core_weather_product(field, field_spec.product);
    request.width = width;
    request.height = height;
    request.supersample_factor = 2;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.visual_mode = ProductVisualMode::SevereDiagnostic;
    request.title = Some(field_spec.display_title().to_string());
    request.subtitle_left = subtitle_left;
    request.subtitle_right = subtitle_right;
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x.clone(),
        y: projected.projected_y.clone(),
        extent: projected.extent.clone(),
    });
    request.projected_lines = projected.lines.clone();
    request.projected_polygons = projected.polygons.clone();
    Ok(request)
}

pub fn render_two_by_four_weather_panel(
    output_path: &Path,
    grid: &LatLonGrid,
    projected: &ProjectedMap,
    fields: &[WeatherPanelField],
    header: &WeatherPanelHeader,
    layout: WeatherPanelLayout,
) -> Result<(), Box<dyn std::error::Error>> {
    let grid_layout = PanelGridLayout::two_by_four(layout.panel_width, layout.panel_height)?
        .with_padding(PanelPadding {
            top: layout.top_padding,
            ..Default::default()
        });
    let mut requests = Vec::with_capacity(fields.len());

    for field_spec in fields {
        let field = Field2D::new(
            ProductKey::named(field_spec.product.slug()),
            field_spec.units.clone(),
            grid.clone(),
            field_spec.values.iter().map(|&v| v as f32).collect(),
        )?;
        let mut request = MapRenderRequest::for_core_weather_product(field, field_spec.product);
        request.width = layout.panel_width;
        request.height = layout.panel_height;
        request.visual_mode = ProductVisualMode::PanelMember;
        if let Some(title) = &field_spec.title_override {
            request.title = Some(title.clone());
        }
        request.projected_domain = Some(ProjectedDomain {
            x: projected.projected_x.clone(),
            y: projected.projected_y.clone(),
            extent: projected.extent.clone(),
        });
        request.projected_lines = projected.lines.clone();
        request.projected_polygons = projected.polygons.clone();
        requests.push(request);
    }

    let mut canvas = render_panel_grid(&grid_layout, &requests)?;
    draw_centered_text_line(&mut canvas, &header.title, 10, Color::BLACK, 2);
    for (idx, line) in header.subtitle_lines.iter().enumerate() {
        draw_centered_text_line(&mut canvas, line, 35 + (idx as i32 * 20), Color::BLACK, 1);
    }

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    DynamicImage::ImageRgba8(canvas).save(output_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_render::{ProjectedExtent, ProjectedLineOverlay, ProjectedPolygonFill};

    #[test]
    fn projected_context_tracks_sizes() {
        let mut context = PreparedProjectedContext::new();
        assert!(!context.contains_size(700, 520));
        context.insert(
            700,
            520,
            ProjectedMap {
                projected_x: vec![0.0],
                projected_y: vec![0.0],
                extent: ProjectedExtent {
                    x_min: 0.0,
                    x_max: 1.0,
                    y_min: 0.0,
                    y_max: 1.0,
                },
                lines: Vec::<ProjectedLineOverlay>::new(),
                polygons: Vec::<ProjectedPolygonFill>::new(),
            },
        );
        assert!(context.contains_size(700, 520));
        assert!(context.projected_map(700, 520).is_some());
    }

    #[test]
    fn panel_field_keeps_title_override() {
        let field = WeatherPanelField::new(WeatherProduct::StpFixed, "dimensionless", vec![1.0])
            .with_title_override("STP (FIXED)");
        assert_eq!(field.title_override.as_deref(), Some("STP (FIXED)"));
    }

    #[test]
    fn panel_field_keeps_artifact_slug_override() {
        let field = WeatherPanelField::new(WeatherProduct::Scp, "dimensionless", vec![1.0])
            .with_artifact_slug("scp_mu_0_3km_0_6km_proxy");
        assert_eq!(field.artifact_slug(), "scp_mu_0_3km_0_6km_proxy");
    }

    #[test]
    fn panel_field_default_artifact_slug_stays_on_product_slug() {
        let field = WeatherPanelField::new(WeatherProduct::StpFixed, "dimensionless", vec![1.0])
            .with_title_override("STP (fixed layer)");
        assert_eq!(field.artifact_slug(), WeatherProduct::StpFixed.slug());
    }
}
