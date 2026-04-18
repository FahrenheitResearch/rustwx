use crate::color::Rgba;
use crate::presentation::{LineworkRole, PolygonRole};

#[derive(Clone, Debug)]
pub struct MapExtent {
    pub x_min: f64,
    pub x_max: f64,
    pub y_min: f64,
    pub y_max: f64,
}

#[derive(Clone, Debug)]
pub struct ProjectedGrid {
    pub x: Vec<f64>,
    pub y: Vec<f64>,
    pub ny: usize,
    pub nx: usize,
}

impl MapExtent {
    pub fn to_pixel(&self, x: f64, y: f64, img_w: u32, img_h: u32) -> Option<(f64, f64)> {
        let dx = self.x_max - self.x_min;
        let dy = self.y_max - self.y_min;
        if dx.abs() < 1e-12 || dy.abs() < 1e-12 {
            return None;
        }
        let rx = (x - self.x_min) / dx;
        let ry = 1.0 - (y - self.y_min) / dy;
        if !(-0.1..=1.1).contains(&rx) || !(-0.1..=1.1).contains(&ry) {
            return None;
        }
        Some((
            rx * (img_w.saturating_sub(1)) as f64,
            ry * (img_h.saturating_sub(1)) as f64,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct ProjectedPolyline {
    pub points: Vec<(f64, f64)>,
    pub color: Rgba,
    pub width: u32,
    pub role: LineworkRole,
}

/// A filled polygon in projected map coordinates. The first ring is the outer
/// boundary; additional rings punch holes (inlets/lakes). Typically sourced
/// from Natural Earth land/ocean/lake shapefiles.
#[derive(Clone, Debug)]
pub struct ProjectedPolygon {
    pub rings: Vec<Vec<(f64, f64)>>,
    pub color: Rgba,
    pub role: PolygonRole,
}

#[derive(Clone, Debug)]
pub struct ContourOverlay {
    pub data: Vec<f64>,
    pub ny: usize,
    pub nx: usize,
    pub levels: Vec<f64>,
    pub color: Rgba,
    pub width: u32,
    pub labels: bool,
    /// When true, find and draw H/L extrema labels on the contour field.
    pub show_extrema: bool,
}

#[derive(Clone, Debug)]
pub struct BarbOverlay {
    pub u: Vec<f64>,
    pub v: Vec<f64>,
    pub ny: usize,
    pub nx: usize,
    pub stride_x: usize,
    pub stride_y: usize,
    pub color: Rgba,
    pub width: u32,
    pub length_px: f64,
}
