use image::DynamicImage;
use rustwx_core::{Field2D, LatLonGrid, ProductKey};
use rustwx_render::{
    MapRenderRequest, PanelGridLayout, PanelPadding, ProjectedDomain, ProjectedExtent,
    ProjectedLineOverlay, Solar07Product, render_panel_grid,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use wrf_render::render::map_frame_aspect_ratio;
use wrf_render::text;

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

#[derive(Debug, Clone)]
pub struct ProjectedMap {
    pub projected_x: Vec<f64>,
    pub projected_y: Vec<f64>,
    pub extent: ProjectedExtent,
    pub lines: Vec<ProjectedLineOverlay>,
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
pub struct Solar07PanelField {
    pub product: Solar07Product,
    pub title_override: Option<String>,
    pub units: String,
    pub values: Vec<f64>,
}

impl Solar07PanelField {
    pub fn new<S: Into<String>>(product: Solar07Product, units: S, values: Vec<f64>) -> Self {
        Self {
            product,
            title_override: None,
            units: units.into(),
            values,
        }
    }

    pub fn with_title_override<S: Into<String>>(mut self, title: S) -> Self {
        self.title_override = Some(title.into());
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct Solar07PanelHeader {
    pub title: String,
    pub subtitle_lines: Vec<String>,
}

impl Solar07PanelHeader {
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
pub struct Solar07PanelLayout {
    pub panel_width: u32,
    pub panel_height: u32,
    pub top_padding: u32,
}

impl Default for Solar07PanelLayout {
    fn default() -> Self {
        Self {
            panel_width: 700,
            panel_height: 520,
            top_padding: 70,
        }
    }
}

impl Solar07PanelLayout {
    pub fn target_aspect_ratio(self) -> f64 {
        map_frame_aspect_ratio(self.panel_width, self.panel_height, true, true)
    }
}

pub fn layout_key(layout: Solar07PanelLayout) -> (u32, u32, u32) {
    (layout.panel_width, layout.panel_height, layout.top_padding)
}

pub fn render_two_by_four_solar07_panel(
    output_path: &Path,
    grid: &LatLonGrid,
    projected: &ProjectedMap,
    fields: &[Solar07PanelField],
    header: &Solar07PanelHeader,
    layout: Solar07PanelLayout,
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
        let mut request = MapRenderRequest::for_core_solar07_product(field, field_spec.product);
        request.width = layout.panel_width;
        request.height = layout.panel_height;
        if let Some(title) = &field_spec.title_override {
            request.title = Some(title.clone());
        }
        request.projected_domain = Some(ProjectedDomain {
            x: projected.projected_x.clone(),
            y: projected.projected_y.clone(),
            extent: projected.extent.clone(),
        });
        request.projected_lines = projected.lines.clone();
        requests.push(request);
    }

    let mut canvas = render_panel_grid(&grid_layout, &requests)?;
    text::draw_text_centered(&mut canvas, &header.title, 10, wrf_render::Rgba::BLACK, 2);
    for (idx, line) in header.subtitle_lines.iter().enumerate() {
        text::draw_text_centered(
            &mut canvas,
            line,
            35 + (idx as i32 * 20),
            wrf_render::Rgba::BLACK,
            1,
        );
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
    use rustwx_render::Solar07Product;

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
                lines: Vec::new(),
            },
        );
        assert!(context.contains_size(700, 520));
        assert!(context.projected_map(700, 520).is_some());
    }

    #[test]
    fn panel_field_keeps_title_override() {
        let field = Solar07PanelField::new(Solar07Product::StpFixed, "dimensionless", vec![1.0])
            .with_title_override("STP (FIXED)");
        assert_eq!(field.title_override.as_deref(), Some("STP (FIXED)"));
    }
}
