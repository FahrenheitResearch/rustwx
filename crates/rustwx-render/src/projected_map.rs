use std::error::Error;

use crate::MapExtent;
use crate::features::{
    BasemapStyle, load_styled_basemap_features_for, load_styled_basemap_polygons_for,
};
use crate::projection::{ProjectionProjector, ProjectionSpec};
use crate::request::{
    Color, ProjectedDomain, ProjectedExtent, ProjectedLineOverlay, ProjectedPolygonFill,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedMap {
    pub projected_x: Vec<f64>,
    pub projected_y: Vec<f64>,
    pub extent: ProjectedExtent,
    pub lines: Vec<ProjectedLineOverlay>,
    pub polygons: Vec<ProjectedPolygonFill>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProjectedBasemap {
    pub lines: Vec<ProjectedLineOverlay>,
    pub polygons: Vec<ProjectedPolygonFill>,
}

impl ProjectedMap {
    pub fn domain(&self) -> ProjectedDomain {
        ProjectedDomain {
            x: self.projected_x.clone(),
            y: self.projected_y.clone(),
            extent: self.extent.clone(),
        }
    }

    pub fn basemap(&self) -> ProjectedBasemap {
        ProjectedBasemap {
            lines: self.lines.clone(),
            polygons: self.polygons.clone(),
        }
    }

    pub fn split(self) -> (ProjectedDomain, ProjectedBasemap) {
        let domain = ProjectedDomain {
            x: self.projected_x,
            y: self.projected_y,
            extent: self.extent,
        };
        let basemap = ProjectedBasemap {
            lines: self.lines,
            polygons: self.polygons,
        };
        (domain, basemap)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeographicBounds {
    pub west_deg: f64,
    pub east_deg: f64,
    pub south_deg: f64,
    pub north_deg: f64,
}

impl GeographicBounds {
    pub fn new(west_deg: f64, east_deg: f64, south_deg: f64, north_deg: f64) -> Self {
        Self {
            west_deg,
            east_deg,
            south_deg: south_deg.min(north_deg),
            north_deg: south_deg.max(north_deg),
        }
    }

    fn contains(self, lat_deg: f64, lon_deg: f64) -> bool {
        if !lat_deg.is_finite() || !lon_deg.is_finite() {
            return false;
        }
        if lat_deg < self.south_deg || lat_deg > self.north_deg {
            return false;
        }
        let west = normalize_longitude_deg(self.west_deg);
        let east = normalize_longitude_deg(self.east_deg);
        let lon = normalize_longitude_deg(lon_deg);
        if west <= east {
            lon >= west && lon <= east
        } else {
            lon >= west || lon <= east
        }
    }
}

impl From<(f64, f64, f64, f64)> for GeographicBounds {
    fn from(value: (f64, f64, f64, f64)) -> Self {
        Self::new(value.0, value.1, value.2, value.3)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProjectedFrameSource {
    FullDomain,
    GeographicBounds(GeographicBounds),
}

impl ProjectedFrameSource {
    fn matches(self, lat_deg: f64, lon_deg: f64) -> bool {
        match self {
            Self::FullDomain => true,
            Self::GeographicBounds(bounds) => bounds.contains(lat_deg, lon_deg),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedDomainBuildOptions {
    pub projection: Option<ProjectionSpec>,
    /// Optional latitude of origin for projection families that benefit from a
    /// caller-provided reference latitude. When absent, the builder uses the
    /// lat/lon mesh midpoint.
    pub reference_latitude_deg: Option<f64>,
    pub frame_source: ProjectedFrameSource,
    pub target_aspect_ratio: f64,
    pub pad_fraction: f64,
}

impl ProjectedDomainBuildOptions {
    pub fn from_bounds(bounds: (f64, f64, f64, f64), target_aspect_ratio: f64) -> Self {
        Self {
            projection: None,
            reference_latitude_deg: None,
            frame_source: ProjectedFrameSource::GeographicBounds(bounds.into()),
            target_aspect_ratio,
            pad_fraction: 0.0,
        }
    }

    pub fn full_domain(target_aspect_ratio: f64) -> Self {
        Self {
            projection: None,
            reference_latitude_deg: None,
            frame_source: ProjectedFrameSource::FullDomain,
            target_aspect_ratio,
            pad_fraction: 0.0,
        }
    }

    pub fn with_projection(mut self, projection: impl Into<ProjectionSpec>) -> Self {
        self.projection = Some(projection.into());
        self
    }

    pub fn with_reference_latitude(mut self, reference_latitude_deg: f64) -> Self {
        self.reference_latitude_deg = Some(reference_latitude_deg);
        self
    }

    pub fn with_padding(mut self, pad_fraction: f64) -> Self {
        self.pad_fraction = pad_fraction.max(0.0);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectedBasemapBuildOptions {
    pub style: BasemapStyle,
    pub polygon_pad_fraction: f64,
    pub line_pad_fraction: f64,
}

impl Default for ProjectedBasemapBuildOptions {
    fn default() -> Self {
        Self {
            style: BasemapStyle::Filled,
            polygon_pad_fraction: 0.50,
            line_pad_fraction: 0.10,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedMapBuildOptions {
    pub domain: ProjectedDomainBuildOptions,
    pub basemap: Option<ProjectedBasemapBuildOptions>,
}

impl ProjectedMapBuildOptions {
    pub fn from_bounds(bounds: (f64, f64, f64, f64), target_aspect_ratio: f64) -> Self {
        Self {
            domain: ProjectedDomainBuildOptions::from_bounds(bounds, target_aspect_ratio),
            basemap: Some(ProjectedBasemapBuildOptions::default()),
        }
    }

    pub fn full_domain(target_aspect_ratio: f64) -> Self {
        Self {
            domain: ProjectedDomainBuildOptions::full_domain(target_aspect_ratio),
            basemap: Some(ProjectedBasemapBuildOptions::default()),
        }
    }

    pub fn with_projection(mut self, projection: impl Into<ProjectionSpec>) -> Self {
        self.domain = self.domain.with_projection(projection);
        self
    }

    pub fn without_basemap(mut self) -> Self {
        self.basemap = None;
        self
    }

    pub fn with_basemap_style(mut self, style: BasemapStyle) -> Self {
        let mut basemap = self.basemap.unwrap_or_default();
        basemap.style = style;
        self.basemap = Some(basemap);
        self
    }
}

pub fn build_projected_domain(
    lat_deg: &[f32],
    lon_deg: &[f32],
    options: &ProjectedDomainBuildOptions,
) -> Result<ProjectedDomain, Box<dyn Error>> {
    validate_lat_lon_mesh(lat_deg, lon_deg)?;
    let projector = resolved_projector(lat_deg, lon_deg, options)?;
    let (projected_x, projected_y, extent) = project_domain(
        lat_deg,
        lon_deg,
        projector,
        options.frame_source,
        options.pad_fraction,
        options.target_aspect_ratio,
    )?;

    Ok(ProjectedDomain {
        x: projected_x,
        y: projected_y,
        extent,
    })
}

pub fn build_projected_map_with_options(
    lat_deg: &[f32],
    lon_deg: &[f32],
    options: &ProjectedMapBuildOptions,
) -> Result<ProjectedMap, Box<dyn Error>> {
    validate_lat_lon_mesh(lat_deg, lon_deg)?;
    let projector = resolved_projector(lat_deg, lon_deg, &options.domain)?;
    let (projected_x, projected_y, extent) = project_domain(
        lat_deg,
        lon_deg,
        projector,
        options.domain.frame_source,
        options.domain.pad_fraction,
        options.domain.target_aspect_ratio,
    )?;

    let basemap = options
        .basemap
        .as_ref()
        .map(|basemap| build_projected_basemap(projector, &extent, *basemap))
        .transpose()?
        .unwrap_or_default();

    Ok(ProjectedMap {
        projected_x,
        projected_y,
        extent,
        lines: basemap.lines,
        polygons: basemap.polygons,
    })
}

pub fn build_projected_map(
    lat_deg: &[f32],
    lon_deg: &[f32],
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn Error>> {
    build_projected_map_with_options(
        lat_deg,
        lon_deg,
        &ProjectedMapBuildOptions::from_bounds(bounds, target_ratio),
    )
}

fn resolved_projector(
    lat_deg: &[f32],
    lon_deg: &[f32],
    options: &ProjectedDomainBuildOptions,
) -> Result<ProjectionProjector, Box<dyn Error>> {
    let projection = options
        .projection
        .clone()
        .or_else(|| ProjectionSpec::infer_from_latlon_grid(lat_deg, lon_deg))
        .ok_or("projected map builder requires at least one finite lat/lon point")?;
    projection
        .build_projector(options.reference_latitude_deg, lat_deg, lon_deg)
        .map_err(Into::into)
}

fn validate_lat_lon_mesh(lat_deg: &[f32], lon_deg: &[f32]) -> Result<(), Box<dyn Error>> {
    if lat_deg.len() != lon_deg.len() {
        return Err("lat/lon arrays must have the same length".into());
    }
    if lat_deg.is_empty() {
        return Err("lat/lon arrays must not be empty".into());
    }
    Ok(())
}

fn project_domain(
    lat_deg: &[f32],
    lon_deg: &[f32],
    projector: ProjectionProjector,
    frame_source: ProjectedFrameSource,
    pad_fraction: f64,
    target_aspect_ratio: f64,
) -> Result<(Vec<f64>, Vec<f64>, ProjectedExtent), Box<dyn Error>> {
    let mut projected_x = Vec::with_capacity(lat_deg.len());
    let mut projected_y = Vec::with_capacity(lat_deg.len());
    let mut full_bounds = ProjectedBounds::default();
    let mut framed_bounds = ProjectedBounds::default();

    for (&lat, &lon) in lat_deg.iter().zip(lon_deg.iter()) {
        let lat = lat as f64;
        let lon = lon as f64;
        let (x, y) = projector.project(lat, lon);
        projected_x.push(x);
        projected_y.push(y);
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        full_bounds.include(x, y);
        if frame_source.matches(lat, lon) {
            framed_bounds.include(x, y);
        }
    }

    let bounds = if framed_bounds.is_valid() {
        framed_bounds
    } else {
        full_bounds
    };
    if !bounds.is_valid() {
        return Err("projected extent produced no finite coordinates".into());
    }

    let padded = bounds.expanded(pad_fraction.max(0.0));
    let extent = MapExtent::from_bounds(
        padded.min_x,
        padded.max_x,
        padded.min_y,
        padded.max_y,
        target_aspect_ratio,
    );

    Ok((
        projected_x,
        projected_y,
        ProjectedExtent {
            x_min: extent.x_min,
            x_max: extent.x_max,
            y_min: extent.y_min,
            y_max: extent.y_max,
        },
    ))
}

fn build_projected_basemap(
    projector: ProjectionProjector,
    extent: &ProjectedExtent,
    options: ProjectedBasemapBuildOptions,
) -> Result<ProjectedBasemap, Box<dyn Error>> {
    let line_bbox = expanded_bbox(extent, options.line_pad_fraction.max(0.0));
    let polygon_bbox = expanded_bbox(extent, options.polygon_pad_fraction.max(0.0));

    let mut lines = Vec::new();
    for layer in load_styled_basemap_features_for(options.style) {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for line in layer.lines {
            let mut current = Vec::<(f64, f64)>::with_capacity(line.len());
            for (lon, lat) in line {
                let point = projector.project(lat, lon);
                if point_in_bbox(point, line_bbox) {
                    current.push(point);
                } else if current.len() >= 2 {
                    lines.push(ProjectedLineOverlay {
                        points: std::mem::take(&mut current),
                        color,
                        width: layer.width,
                        role: layer.role,
                    });
                } else {
                    current.clear();
                }
            }
            if current.len() >= 2 {
                lines.push(ProjectedLineOverlay {
                    points: current,
                    color,
                    width: layer.width,
                    role: layer.role,
                });
            }
        }
    }

    let mut polygons = Vec::new();
    for layer in load_styled_basemap_polygons_for(options.style) {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for polygon in layer.polygons {
            let rings: Vec<Vec<(f64, f64)>> = polygon
                .into_iter()
                .map(|ring| {
                    ring.into_iter()
                        .map(|(lon, lat)| projector.project(lat, lon))
                        .collect::<Vec<(f64, f64)>>()
                })
                .filter(|ring| ring_overlaps_bbox(ring, polygon_bbox))
                .collect();
            if !rings.is_empty() {
                polygons.push(ProjectedPolygonFill {
                    rings,
                    color,
                    role: layer.role,
                });
            }
        }
    }

    Ok(ProjectedBasemap { lines, polygons })
}

fn point_in_bbox(point: (f64, f64), bbox: (f64, f64, f64, f64)) -> bool {
    point.0 >= bbox.0 && point.0 <= bbox.1 && point.1 >= bbox.2 && point.1 <= bbox.3
}

fn expanded_bbox(extent: &ProjectedExtent, pad_fraction: f64) -> (f64, f64, f64, f64) {
    let pad_x = 0.5 * pad_fraction * (extent.x_max - extent.x_min);
    let pad_y = 0.5 * pad_fraction * (extent.y_max - extent.y_min);
    (
        extent.x_min - pad_x,
        extent.x_max + pad_x,
        extent.y_min - pad_y,
        extent.y_max + pad_y,
    )
}

fn ring_overlaps_bbox(ring: &[(f64, f64)], bbox: (f64, f64, f64, f64)) -> bool {
    let mut bounds = ProjectedBounds::default();
    for &(x, y) in ring {
        bounds.include(x, y);
    }
    bounds.is_valid()
        && !(bounds.max_x < bbox.0
            || bounds.min_x > bbox.1
            || bounds.max_y < bbox.2
            || bounds.min_y > bbox.3)
}

fn normalize_longitude_deg(lon_deg: f64) -> f64 {
    let mut lon = lon_deg % 360.0;
    if lon > 180.0 {
        lon -= 360.0;
    } else if lon <= -180.0 {
        lon += 360.0;
    }
    lon
}

#[derive(Debug, Clone, Copy)]
struct ProjectedBounds {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
}

impl Default for ProjectedBounds {
    fn default() -> Self {
        Self {
            min_x: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            min_y: f64::INFINITY,
            max_y: f64::NEG_INFINITY,
        }
    }
}

impl ProjectedBounds {
    fn include(&mut self, x: f64, y: f64) {
        self.min_x = self.min_x.min(x);
        self.max_x = self.max_x.max(x);
        self.min_y = self.min_y.min(y);
        self.max_y = self.max_y.max(y);
    }

    fn is_valid(self) -> bool {
        self.min_x.is_finite()
            && self.max_x.is_finite()
            && self.min_y.is_finite()
            && self.max_y.is_finite()
    }

    fn expanded(self, pad_fraction: f64) -> Self {
        let width = self.max_x - self.min_x;
        let height = self.max_y - self.min_y;
        let pad_x = width * pad_fraction / 2.0;
        let pad_y = height * pad_fraction / 2.0;
        Self {
            min_x: self.min_x - pad_x,
            max_x: self.max_x + pad_x,
            min_y: self.min_y - pad_y,
            max_y: self.max_y + pad_y,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::ProjectionSpec;

    fn sample_lat_lon() -> (Vec<f32>, Vec<f32>) {
        (
            vec![35.0, 35.0, 35.0, 36.0, 36.0, 36.0],
            vec![-100.0, -99.0, -98.0, -100.0, -99.0, -98.0],
        )
    }

    #[test]
    fn projected_domain_builder_supports_full_domain_geographic_projection() {
        let (lat, lon) = sample_lat_lon();
        let domain = build_projected_domain(
            &lat,
            &lon,
            &ProjectedDomainBuildOptions::full_domain(2.0)
                .with_projection(ProjectionSpec::Geographic),
        )
        .expect("domain should build");

        assert_eq!(domain.x.len(), lat.len());
        assert_eq!(domain.y.len(), lat.len());
        assert!(domain.extent.x_min < 0.0);
        assert!(domain.extent.x_max > 0.0);
        assert!(domain.extent.y_max > domain.extent.y_min);
    }

    #[test]
    fn projected_domain_builder_respects_geographic_crop_bounds() {
        let (lat, lon) = sample_lat_lon();
        let full = build_projected_domain(
            &lat,
            &lon,
            &ProjectedDomainBuildOptions::full_domain(1.5)
                .with_projection(ProjectionSpec::Geographic),
        )
        .expect("full domain");
        let cropped = build_projected_domain(
            &lat,
            &lon,
            &ProjectedDomainBuildOptions::from_bounds((-99.25, -98.25, 35.0, 36.0), 1.5)
                .with_projection(ProjectionSpec::Geographic),
        )
        .expect("cropped domain");

        assert!(
            cropped.extent.x_max - cropped.extent.x_min < full.extent.x_max - full.extent.x_min
        );
    }

    #[test]
    fn projected_map_builder_can_skip_basemap_for_reusable_domain_scaffolds() {
        let (lat, lon) = sample_lat_lon();
        let projected = build_projected_map_with_options(
            &lat,
            &lon,
            &ProjectedMapBuildOptions::full_domain(1.4)
                .with_projection(ProjectionSpec::Geographic)
                .without_basemap(),
        )
        .expect("projected map");

        assert!(projected.lines.is_empty());
        assert!(projected.polygons.is_empty());
    }

    #[test]
    fn projected_map_split_preserves_domain_and_basemap_layers() {
        let projected = ProjectedMap {
            projected_x: vec![0.0, 1.0],
            projected_y: vec![0.0, 1.0],
            extent: ProjectedExtent {
                x_min: 0.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 1.0,
            },
            lines: vec![ProjectedLineOverlay {
                points: vec![(0.0, 0.0), (1.0, 1.0)],
                color: Color::BLACK,
                width: 2,
                role: crate::presentation::LineworkRole::Generic,
            }],
            polygons: vec![ProjectedPolygonFill {
                rings: vec![vec![(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]],
                color: Color::WHITE,
                role: crate::presentation::PolygonRole::Generic,
            }],
        };

        let (domain, basemap) = projected.split();
        assert_eq!(domain.x, vec![0.0, 1.0]);
        assert_eq!(basemap.lines.len(), 1);
        assert_eq!(basemap.polygons.len(), 1);
    }
}
