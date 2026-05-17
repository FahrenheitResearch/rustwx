use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PointSample {
    pub variable: String,
    pub forecast_hour: u16,
    pub level_hpa: u16,
    pub value: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PointProfile {
    pub lat_deg: f64,
    pub lon_deg: f64,
    pub samples: Vec<PointSample>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoxProfile {
    pub center_lat_deg: f64,
    pub center_lon_deg: f64,
    pub min_lat_deg: f64,
    pub max_lat_deg: f64,
    pub min_lon_deg: f64,
    pub max_lon_deg: f64,
    pub x0: usize,
    pub x1: usize,
    pub y0: usize,
    pub y1: usize,
    pub cell_count: usize,
    pub samples: Vec<PointSample>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteDef {
    pub id: String,
    pub name: String,
    pub points: Vec<(f64, f64)>,
    pub sample_spacing_km: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteSample {
    pub distance_km: f32,
    pub lat_deg: f64,
    pub lon_deg: f64,
    pub grid_x: f32,
    pub grid_y: f32,
    pub x0: usize,
    pub y0: usize,
    pub wx: f32,
    pub wy: f32,
    pub route_unit_u: f32,
    pub route_unit_v: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteValue {
    pub sample_index: usize,
    pub variable: String,
    pub forecast_hour: u16,
    pub level_hpa: u16,
    pub value: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteSectionPrimitives {
    pub route_id: String,
    pub route_name: String,
    pub forecast_hour: u16,
    pub route_samples: Vec<RouteSample>,
    pub values: Vec<RouteValue>,
}

pub fn haversine_km(a: (f64, f64), b: (f64, f64)) -> f64 {
    let radius_km = 6371.0;
    let dlat = (b.0 - a.0).to_radians();
    let dlon = (b.1 - a.1).to_radians();
    let lat1 = a.0.to_radians();
    let lat2 = b.0.to_radians();
    let h = (dlat * 0.5).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon * 0.5).sin().powi(2);
    2.0 * radius_km * h.sqrt().asin()
}

pub fn route_unit_components(start: (f64, f64), end: (f64, f64)) -> (f32, f32) {
    let mean_lat = ((start.0 + end.0) * 0.5).to_radians();
    let dx = (end.1 - start.1) * mean_lat.cos();
    let dy = end.0 - start.0;
    let mag = (dx * dx + dy * dy).sqrt();
    if mag <= f64::EPSILON {
        (0.0, 0.0)
    } else {
        ((dx / mag) as f32, (dy / mag) as f32)
    }
}
