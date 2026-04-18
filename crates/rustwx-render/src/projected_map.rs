use std::error::Error;

use crate::features::{load_styled_conus_features, load_styled_conus_polygons};
use crate::projection::LambertConformal;
use crate::request::{Color, ProjectedExtent, ProjectedLineOverlay, ProjectedPolygonFill};
use crate::MapExtent;

#[derive(Debug, Clone)]
pub struct ProjectedMap {
    pub projected_x: Vec<f64>,
    pub projected_y: Vec<f64>,
    pub extent: ProjectedExtent,
    pub lines: Vec<ProjectedLineOverlay>,
    pub polygons: Vec<ProjectedPolygonFill>,
}

pub fn build_projected_map(
    lat_deg: &[f32],
    lon_deg: &[f32],
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn Error>> {
    let proj = LambertConformal::new(33.0, 45.0, -97.0, 39.0);
    let mut projected_x = Vec::with_capacity(lat_deg.len());
    let mut projected_y = Vec::with_capacity(lat_deg.len());
    let mut full_min_x = f64::INFINITY;
    let mut full_max_x = f64::NEG_INFINITY;
    let mut full_min_y = f64::INFINITY;
    let mut full_max_y = f64::NEG_INFINITY;
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    for (&lat, &lon) in lat_deg.iter().zip(lon_deg.iter()) {
        let lat = lat as f64;
        let lon = lon as f64;
        let (x, y) = proj.project(lat, lon);
        projected_x.push(x);
        projected_y.push(y);
        if x.is_finite() && y.is_finite() {
            full_min_x = full_min_x.min(x);
            full_max_x = full_max_x.max(x);
            full_min_y = full_min_y.min(y);
            full_max_y = full_max_y.max(y);
        }
        if lon >= bounds.0 && lon <= bounds.1 && lat >= bounds.2 && lat <= bounds.3 {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    }

    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        min_x = full_min_x;
        max_x = full_max_x;
        min_y = full_min_y;
        max_y = full_max_y;
    }

    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        return Err("projected extent produced no finite coordinates".into());
    }

    let extent = MapExtent::from_bounds(min_x, max_x, min_y, max_y, target_ratio);
    let mut lines = Vec::new();
    for layer in load_styled_conus_features() {
        for line in layer.lines {
            lines.push(ProjectedLineOverlay {
                points: line
                    .into_iter()
                    .map(|(lon, lat)| proj.project(lat, lon))
                    .collect(),
                color: Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a),
                width: layer.width,
                role: layer.role,
            });
        }
    }

    let pad_x = 0.50 * (extent.x_max - extent.x_min);
    let pad_y = 0.50 * (extent.y_max - extent.y_min);
    let accept_bbox = (
        extent.x_min - pad_x,
        extent.x_max + pad_x,
        extent.y_min - pad_y,
        extent.y_max + pad_y,
    );
    let mut polygons: Vec<ProjectedPolygonFill> = Vec::new();
    for layer in load_styled_conus_polygons() {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for polygon in layer.polygons {
            let rings: Vec<Vec<(f64, f64)>> = polygon
                .into_iter()
                .map(|ring| {
                    ring.into_iter()
                        .map(|(lon, lat)| proj.project(lat, lon))
                        .collect::<Vec<(f64, f64)>>()
                })
                .filter(|ring| ring_overlaps_bbox(ring, accept_bbox))
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

    Ok(ProjectedMap {
        projected_x,
        projected_y,
        extent: ProjectedExtent {
            x_min: extent.x_min,
            x_max: extent.x_max,
            y_min: extent.y_min,
            y_max: extent.y_max,
        },
        lines,
        polygons,
    })
}

fn ring_overlaps_bbox(ring: &[(f64, f64)], bbox: (f64, f64, f64, f64)) -> bool {
    let (mut rx_min, mut rx_max) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut ry_min, mut ry_max) = (f64::INFINITY, f64::NEG_INFINITY);
    for &(x, y) in ring {
        if x < rx_min {
            rx_min = x;
        }
        if x > rx_max {
            rx_max = x;
        }
        if y < ry_min {
            ry_min = y;
        }
        if y > ry_max {
            ry_max = y;
        }
    }
    !(rx_max < bbox.0 || rx_min > bbox.1 || ry_max < bbox.2 || ry_min > bbox.3)
}
