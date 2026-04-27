use numpy::PyReadonlyArray2;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rustwx_render::{
    BasemapStyle, Color, ProductVisualMode, ProjectedDomain, ProjectedExtent, ProjectedLineOverlay,
    ProjectedPolygonFill, RenderPresentation, load_styled_conus_features_for,
    load_styled_conus_polygons_for, map_frame_aspect_ratio_for_mode,
};
use serde_json::{Map, Value, json};
use std::f64::consts::PI;

use super::spec::{
    ExtentsMetadata, GridShapeMetadata, LayoutMetadata, PixelBoundsMetadata, RenderSpec,
};
use super::spec::{
    ProjectedBasemapOverlayMetadata, ProjectedCornerMetadata, ProjectedExtentMetadata,
    ProjectedGeometryMetadata, ProjectedOverlayCounts, ProjectedProjectionDescription,
    ProjectedSurfaceSpec, ProjectionMetadata, ProjectionSpec,
};

pub(crate) const WRF_EARTH_RADIUS_M: f64 = 6_370_000.0;
pub(crate) const DEG_TO_RAD: f64 = PI / 180.0;

#[derive(Debug, Clone)]
pub(crate) struct Array2Data {
    pub(crate) ny: usize,
    pub(crate) nx: usize,
    pub(crate) values: Vec<f64>,
}

impl Array2Data {
    pub(crate) fn to_f32(&self) -> Vec<f32> {
        self.values.iter().map(|value| *value as f32).collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Layout {
    pub(crate) map_x: u32,
    pub(crate) map_y: u32,
    pub(crate) map_w: u32,
    pub(crate) map_h: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectedCorner {
    pub(crate) index: usize,
    pub(crate) grid_corner: &'static str,
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    pub(crate) x: f64,
    pub(crate) y: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct Geometry {
    pub(crate) x: Vec<f64>,
    pub(crate) y: Vec<f64>,
    pub(crate) valid_extent: ProjectedExtent,
    pub(crate) padded_extent: ProjectedExtent,
    pub(crate) projected_corners: Vec<ProjectedCorner>,
}

#[derive(Debug, Clone, Copy)]
struct ProjectedBounds {
    x_min: f64,
    x_max: f64,
    y_min: f64,
    y_max: f64,
}

impl ProjectedBounds {
    fn empty() -> Self {
        Self {
            x_min: f64::INFINITY,
            x_max: f64::NEG_INFINITY,
            y_min: f64::INFINITY,
            y_max: f64::NEG_INFINITY,
        }
    }

    fn include(&mut self, x: f64, y: f64) {
        if !x.is_finite() || !y.is_finite() {
            return;
        }
        self.x_min = self.x_min.min(x);
        self.x_max = self.x_max.max(x);
        self.y_min = self.y_min.min(y);
        self.y_max = self.y_max.max(y);
    }

    fn is_valid(self) -> bool {
        self.x_min.is_finite()
            && self.x_max.is_finite()
            && self.y_min.is_finite()
            && self.y_max.is_finite()
            && self.x_min <= self.x_max
            && self.y_min <= self.y_max
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SurfaceContext {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) colorbar: bool,
    pub(crate) has_title: bool,
    pub(crate) visual_mode: ProductVisualMode,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectedRenderArrays {
    pub(crate) lat: Array2Data,
    pub(crate) lon: Array2Data,
    pub(crate) field: Array2Data,
    pub(crate) contour_field: Option<Array2Data>,
    pub(crate) overlay_field: Option<Array2Data>,
    pub(crate) wind_u: Option<Array2Data>,
    pub(crate) wind_v: Option<Array2Data>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LambertProjector {
    n: f64,
    f: f64,
    rho0: f64,
    lambda0: f64,
}

impl LambertProjector {
    fn new(truelat1: f64, truelat2: f64, stand_lon: f64, ref_lat: f64) -> Self {
        let phi1 = truelat1 * DEG_TO_RAD;
        let phi2 = truelat2 * DEG_TO_RAD;
        let phi0 = ref_lat * DEG_TO_RAD;
        let lambda0 = stand_lon * DEG_TO_RAD;

        let n = if (truelat1 - truelat2).abs() < 1.0e-10 {
            phi1.sin()
        } else {
            let num = phi1.cos().ln() - phi2.cos().ln();
            let den = (PI / 4.0 + phi2 / 2.0).tan().ln() - (PI / 4.0 + phi1 / 2.0).tan().ln();
            num / den
        };
        let f = phi1.cos() * (PI / 4.0 + phi1 / 2.0).tan().powf(n) / n;
        let rho0 = WRF_EARTH_RADIUS_M * f / (PI / 4.0 + phi0 / 2.0).tan().powf(n);

        Self {
            n,
            f,
            rho0,
            lambda0,
        }
    }

    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        let phi = lat * DEG_TO_RAD;
        let lambda = lon * DEG_TO_RAD;
        let rho = WRF_EARTH_RADIUS_M * self.f / (PI / 4.0 + phi / 2.0).tan().powf(self.n);
        let theta = self.n * (lambda - self.lambda0);
        let x = rho * theta.sin();
        let y = self.rho0 - rho * theta.cos();
        (x, y)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PolarProjector {
    stand_lon: f64,
    truelat1: f64,
    north_pole: bool,
}

impl PolarProjector {
    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        let theta = (lon - self.stand_lon) * DEG_TO_RAD;
        let lat_ts = self.truelat1.abs() * DEG_TO_RAD;
        let k = (1.0 + lat_ts.sin()) / 2.0;
        if self.north_pole {
            let rho = 2.0 * WRF_EARTH_RADIUS_M * k * (PI / 4.0 - (lat * DEG_TO_RAD) / 2.0).tan();
            (rho * theta.sin(), -rho * theta.cos())
        } else {
            let rho = 2.0 * WRF_EARTH_RADIUS_M * k * (PI / 4.0 + (lat * DEG_TO_RAD) / 2.0).tan();
            (rho * theta.sin(), rho * theta.cos())
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MercatorProjector {
    truelat1: f64,
    cen_lon: f64,
}

impl MercatorProjector {
    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        let lat_ts = self.truelat1 * DEG_TO_RAD;
        let lon0 = self.cen_lon * DEG_TO_RAD;
        let lambda = lon * DEG_TO_RAD;
        let phi = lat * DEG_TO_RAD;
        let scale = lat_ts.cos();
        let x = WRF_EARTH_RADIUS_M * scale * (lambda - lon0);
        let y = WRF_EARTH_RADIUS_M * scale * (PI / 4.0 + phi / 2.0).tan().ln();
        (x, y)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Projector {
    Lambert {
        inner: LambertProjector,
        truelat1: f64,
        truelat2: f64,
        stand_lon: f64,
        cen_lat: f64,
    },
    Polar {
        inner: PolarProjector,
        truelat1: f64,
        stand_lon: f64,
        north_pole: bool,
    },
    Mercator {
        inner: MercatorProjector,
        truelat1: f64,
        cen_lon: f64,
    },
    LatLon,
}

impl Projector {
    fn from_spec(spec: &ProjectionSpec) -> PyResult<Self> {
        match spec.map_proj {
            1 => {
                let truelat1 = required_param(spec.truelat1, "TRUELAT1")?;
                let truelat2 = required_param(spec.truelat2, "TRUELAT2")?;
                let stand_lon = required_param(spec.stand_lon, "STAND_LON")?;
                let cen_lat = required_param(spec.cen_lat, "CEN_LAT")?;
                Ok(Self::Lambert {
                    inner: LambertProjector::new(truelat1, truelat2, stand_lon, cen_lat),
                    truelat1,
                    truelat2,
                    stand_lon,
                    cen_lat,
                })
            }
            2 => {
                let truelat1 = required_param(spec.truelat1, "TRUELAT1")?;
                let stand_lon = required_param(spec.stand_lon, "STAND_LON")?;
                let cen_lat = required_param(spec.cen_lat, "CEN_LAT")?;
                Ok(Self::Polar {
                    inner: PolarProjector {
                        stand_lon,
                        truelat1,
                        north_pole: cen_lat >= 0.0,
                    },
                    truelat1,
                    stand_lon,
                    north_pole: cen_lat >= 0.0,
                })
            }
            3 => {
                let truelat1 = required_param(spec.truelat1, "TRUELAT1")?;
                let cen_lon = required_param(spec.cen_lon, "CEN_LON")?;
                Ok(Self::Mercator {
                    inner: MercatorProjector { truelat1, cen_lon },
                    truelat1,
                    cen_lon,
                })
            }
            6 => Ok(Self::LatLon),
            other => Err(PyValueError::new_err(format!(
                "Unsupported MAP_PROJ value {other}; expected one of 1, 2, 3, 6"
            ))),
        }
    }

    pub(crate) fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        match self {
            Self::Lambert { inner, .. } => inner.project(lat, lon),
            Self::Polar { inner, .. } => inner.project(lat, lon),
            Self::Mercator { inner, .. } => inner.project(lat, lon),
            Self::LatLon => (lon, lat),
        }
    }

    pub(crate) fn projection_info(self) -> Map<String, Value> {
        match self {
            Self::Lambert {
                truelat1,
                truelat2,
                stand_lon,
                cen_lat,
                ..
            } => json_object([
                ("proj", json!("lcc")),
                ("lat_1", json!(truelat1)),
                ("lat_2", json!(truelat2)),
                ("lat_0", json!(cen_lat)),
                ("lon_0", json!(stand_lon)),
                ("a", json!(WRF_EARTH_RADIUS_M)),
                ("b", json!(WRF_EARTH_RADIUS_M)),
                ("units", json!("m")),
            ]),
            Self::Polar {
                truelat1,
                stand_lon,
                north_pole,
                ..
            } => json_object([
                ("proj", json!("stere")),
                ("lat_0", json!(if north_pole { 90.0 } else { -90.0 })),
                ("lat_ts", json!(truelat1)),
                ("lon_0", json!(stand_lon)),
                ("a", json!(WRF_EARTH_RADIUS_M)),
                ("b", json!(WRF_EARTH_RADIUS_M)),
                ("units", json!("m")),
            ]),
            Self::Mercator {
                truelat1, cen_lon, ..
            } => json_object([
                ("proj", json!("merc")),
                ("lat_ts", json!(truelat1)),
                ("lon_0", json!(cen_lon)),
                ("a", json!(WRF_EARTH_RADIUS_M)),
                ("b", json!(WRF_EARTH_RADIUS_M)),
                ("units", json!("m")),
            ]),
            Self::LatLon => json_object([
                ("proj", json!("longlat")),
                ("a", json!(WRF_EARTH_RADIUS_M)),
                ("b", json!(WRF_EARTH_RADIUS_M)),
            ]),
        }
    }

    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Lambert { .. } => "lambert_conformal_conic",
            Self::Polar { north_pole, .. } => {
                if north_pole {
                    "polar_stereographic_north"
                } else {
                    "polar_stereographic_south"
                }
            }
            Self::Mercator { .. } => "mercator",
            Self::LatLon => "latitude_longitude",
        }
    }

    pub(crate) fn map_proj(self) -> i32 {
        match self {
            Self::Lambert { .. } => 1,
            Self::Polar { .. } => 2,
            Self::Mercator { .. } => 3,
            Self::LatLon => 6,
        }
    }
}

fn json_object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Map<String, Value> {
    entries
        .into_iter()
        .filter(|(_, value)| !value.is_null())
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn required_param(value: Option<f64>, name: &str) -> PyResult<f64> {
    value.ok_or_else(|| PyValueError::new_err(format!("Missing projection parameter {name}")))
}

fn surface_context(spec: &ProjectedSurfaceSpec) -> SurfaceContext {
    SurfaceContext {
        width: spec.width(),
        height: spec.height(),
        colorbar: spec.colorbar(),
        has_title: spec.has_title(),
        visual_mode: spec.visual_mode(),
    }
}

pub(crate) fn projection_metadata(
    spec: &ProjectionSpec,
    projector: Projector,
) -> ProjectionMetadata {
    ProjectionMetadata {
        map_proj: projector.map_proj(),
        kind: projector.kind(),
        earth_radius_m: WRF_EARTH_RADIUS_M,
        parameters: spec.clone(),
        projected_crs: projector.projection_info(),
    }
}

pub(crate) fn pixel_bounds(layout: Layout, crop_top: u32) -> PixelBoundsMetadata {
    PixelBoundsMetadata {
        x_start: layout.map_x,
        y_start: layout.map_y.saturating_sub(crop_top),
        x_end: layout.map_x.saturating_add(layout.map_w),
        y_end: layout
            .map_y
            .saturating_add(layout.map_h)
            .saturating_sub(crop_top),
    }
}

pub(crate) fn extent_to_array(extent: &ProjectedExtent) -> [f64; 4] {
    [extent.x_min, extent.x_max, extent.y_min, extent.y_max]
}

fn projected_corners_metadata(corners: &[ProjectedCorner]) -> Vec<ProjectedCornerMetadata> {
    corners
        .iter()
        .map(|corner| ProjectedCornerMetadata {
            index: corner.index,
            grid_corner: corner.grid_corner,
            lat: corner.lat,
            lon: corner.lon,
            x: corner.x,
            y: corner.y,
        })
        .collect()
}

fn layout_metadata(context: SurfaceContext, layout: Layout, crop_top: u32) -> LayoutMetadata {
    LayoutMetadata {
        width: context.width,
        height: context.height,
        colorbar: context.colorbar,
        has_title: context.has_title,
        visual_mode: context.visual_mode,
        crop_top,
        pixel_bounds: pixel_bounds(layout, crop_top),
    }
}

pub(crate) fn geometry_metadata(
    kind: &'static str,
    spec: &ProjectedSurfaceSpec,
    projector: Projector,
    geometry: &Geometry,
    layout: Layout,
    crop_top: u32,
    lat: &Array2Data,
    include_projected_domain: bool,
) -> ProjectedGeometryMetadata {
    let context = surface_context(spec);
    ProjectedGeometryMetadata {
        kind,
        schema_version: 1,
        grid_shape: GridShapeMetadata {
            ny: lat.ny,
            nx: lat.nx,
        },
        pixel_bounds: pixel_bounds(layout, crop_top),
        data_extent: extent_to_array(&geometry.padded_extent),
        valid_data_extent: extent_to_array(&geometry.valid_extent),
        projection_info: projector.projection_info(),
        projection: projection_metadata(&spec.projection, projector),
        extents: ExtentsMetadata {
            padded: ProjectedExtentMetadata::from(&geometry.padded_extent),
            valid: ProjectedExtentMetadata::from(&geometry.valid_extent),
        },
        layout: layout_metadata(context, layout, crop_top),
        projected_corners: projected_corners_metadata(&geometry.projected_corners),
        projected_domain: include_projected_domain.then(|| ProjectedDomain {
            x: geometry.x.clone(),
            y: geometry.y.clone(),
            extent: geometry.padded_extent.clone(),
        }),
    }
}

pub(crate) fn extract_array(name: &str, array: PyReadonlyArray2<'_, f64>) -> PyResult<Array2Data> {
    let view = array.as_array();
    let shape = view.shape();
    let ny = shape[0];
    let nx = shape[1];
    if ny == 0 || nx == 0 {
        return Err(PyValueError::new_err(format!(
            "{name} must be a non-empty 2-D array"
        )));
    }
    Ok(Array2Data {
        ny,
        nx,
        values: view.iter().copied().collect(),
    })
}

fn ensure_same_shape(reference: &Array2Data, other: &Array2Data, name: &str) -> PyResult<()> {
    if reference.nx != other.nx || reference.ny != other.ny {
        return Err(PyValueError::new_err(format!(
            "{name} shape mismatch: expected ({}, {}), got ({}, {})",
            reference.ny, reference.nx, other.ny, other.nx
        )));
    }
    Ok(())
}

pub(crate) fn extract_lat_lon_arrays(
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
) -> PyResult<(Array2Data, Array2Data)> {
    let lat = extract_array("lat", lat)?;
    let lon = extract_array("lon", lon)?;
    ensure_same_shape(&lat, &lon, "lon")?;
    Ok((lat, lon))
}

pub(crate) fn extract_projected_render_arrays(
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<ProjectedRenderArrays> {
    let (lat, lon) = extract_lat_lon_arrays(lat, lon)?;
    let field = extract_array("field", field)?;
    ensure_same_shape(&field, &lat, "field")?;
    Ok(ProjectedRenderArrays {
        lat,
        lon,
        field,
        contour_field: contour_field
            .map(|array| extract_array("contour_field", array))
            .transpose()?,
        overlay_field: overlay_field
            .map(|array| extract_array("overlay_field", array))
            .transpose()?,
        wind_u: wind_u
            .map(|array| extract_array("wind_u", array))
            .transpose()?,
        wind_v: wind_v
            .map(|array| extract_array("wind_v", array))
            .transpose()?,
    })
}

pub(crate) fn validate_projected_render_arrays(
    spec: &RenderSpec,
    arrays: &ProjectedRenderArrays,
) -> PyResult<()> {
    if let Some(ref contour_field) = arrays.contour_field {
        ensure_same_shape(&arrays.field, contour_field, "contour_field")?;
    }
    if let Some(ref overlay_field) = arrays.overlay_field {
        ensure_same_shape(&arrays.field, overlay_field, "overlay_field")?;
        if spec.overlay.is_none() {
            return Err(PyValueError::new_err(
                "Overlay field provided without overlay spec",
            ));
        }
    }
    match (&arrays.wind_u, &arrays.wind_v) {
        (Some(u), Some(v)) => {
            ensure_same_shape(&arrays.field, u, "wind_u")?;
            ensure_same_shape(&arrays.field, v, "wind_v")?;
        }
        (None, None) => {}
        _ => {
            return Err(PyValueError::new_err(
                "wind_u and wind_v must both be provided or both be omitted",
            ));
        }
    }
    Ok(())
}

fn build_geometry(
    projector: Projector,
    lat: &Array2Data,
    lon: &Array2Data,
    width: u32,
    height: u32,
    colorbar: bool,
    has_title: bool,
    visual_mode: ProductVisualMode,
) -> Geometry {
    let target_ratio =
        map_frame_aspect_ratio_for_mode(visual_mode, width, height, colorbar, has_title);
    let mut x = Vec::with_capacity(lat.values.len());
    let mut y = Vec::with_capacity(lat.values.len());
    let mut data_bounds = ProjectedBounds::empty();

    for (&lat_deg, &lon_deg) in lat.values.iter().zip(lon.values.iter()) {
        let (px, py) = projector.project(lat_deg, lon_deg);
        x.push(px);
        y.push(py);
        data_bounds.include(px, py);
    }

    let nx = lat.nx;
    let ny = lat.ny;
    let idx = |row: usize, col: usize| row * nx + col;
    let corners = [
        (0, "top_left", lat.values[idx(0, 0)], lon.values[idx(0, 0)]),
        (
            1,
            "bottom_right",
            lat.values[idx(ny.saturating_sub(1), nx.saturating_sub(1))],
            lon.values[idx(ny.saturating_sub(1), nx.saturating_sub(1))],
        ),
        (
            2,
            "bottom_left",
            lat.values[idx(ny.saturating_sub(1), 0)],
            lon.values[idx(ny.saturating_sub(1), 0)],
        ),
        (
            3,
            "top_right",
            lat.values[idx(0, nx.saturating_sub(1))],
            lon.values[idx(0, nx.saturating_sub(1))],
        ),
    ];

    let projected_corners: Vec<ProjectedCorner> = corners
        .into_iter()
        .map(|(index, grid_corner, lat_deg, lon_deg)| {
            let (x, y) = projector.project(lat_deg, lon_deg);
            ProjectedCorner {
                index,
                grid_corner,
                lat: lat_deg,
                lon: lon_deg,
                x,
                y,
            }
        })
        .collect();
    if !data_bounds.is_valid() {
        data_bounds = ProjectedBounds::empty();
        for corner in &projected_corners {
            data_bounds.include(corner.x, corner.y);
        }
    }

    let data_x_min = data_bounds.x_min;
    let data_x_max = data_bounds.x_max;
    let data_y_min = data_bounds.y_min;
    let data_y_max = data_bounds.y_max;

    let valid_extent = ProjectedExtent {
        x_min: data_x_min,
        x_max: data_x_max,
        y_min: data_y_min,
        y_max: data_y_max,
    };

    let data_width = (data_x_max - data_x_min).max(1.0e-12);
    let data_height = (data_y_max - data_y_min).max(1.0e-12);
    let data_ratio = data_width / data_height;

    let padded_extent = if data_ratio > target_ratio {
        let new_height = data_width / target_ratio;
        let pad_y = (new_height - data_height) / 2.0;
        ProjectedExtent {
            x_min: data_x_min,
            x_max: data_x_max,
            y_min: data_y_min - pad_y,
            y_max: data_y_max + pad_y,
        }
    } else {
        let new_width = data_height * target_ratio;
        let pad_x = (new_width - data_width) / 2.0;
        ProjectedExtent {
            x_min: data_x_min - pad_x,
            x_max: data_x_max + pad_x,
            y_min: data_y_min,
            y_max: data_y_max,
        }
    };

    Geometry {
        x,
        y,
        valid_extent,
        padded_extent,
        projected_corners,
    }
}

pub(crate) fn project_lines(
    projector: Projector,
    extent: &ProjectedExtent,
    style: BasemapStyle,
) -> Vec<ProjectedLineOverlay> {
    let layers = load_styled_conus_features_for(style);
    let mut overlays = Vec::new();
    let pad_x = 0.10 * (extent.x_max - extent.x_min);
    let pad_y = 0.10 * (extent.y_max - extent.y_min);
    let x_lo = extent.x_min - pad_x;
    let x_hi = extent.x_max + pad_x;
    let y_lo = extent.y_min - pad_y;
    let y_hi = extent.y_max + pad_y;

    for layer in layers {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for line in layer.lines {
            let mut current: Vec<(f64, f64)> = Vec::with_capacity(line.len());
            for (lon, lat) in line {
                let (x, y) = projector.project(lat, lon);
                if x < x_lo || x > x_hi || y < y_lo || y > y_hi {
                    if current.len() >= 2 {
                        overlays.push(ProjectedLineOverlay {
                            points: std::mem::take(&mut current),
                            color,
                            width: layer.width,
                            role: layer.role,
                        });
                    } else {
                        current.clear();
                    }
                    continue;
                }
                current.push((x, y));
            }
            if current.len() >= 2 {
                overlays.push(ProjectedLineOverlay {
                    points: current,
                    color,
                    width: layer.width,
                    role: layer.role,
                });
            }
        }
    }

    overlays
}

pub(crate) fn project_polygons(
    projector: Projector,
    extent: &ProjectedExtent,
    style: BasemapStyle,
) -> Vec<ProjectedPolygonFill> {
    let layers = load_styled_conus_polygons_for(style);
    let pad_x = 0.50 * (extent.x_max - extent.x_min);
    let pad_y = 0.50 * (extent.y_max - extent.y_min);
    let bbox = (
        extent.x_min - pad_x,
        extent.x_max + pad_x,
        extent.y_min - pad_y,
        extent.y_max + pad_y,
    );
    let mut out = Vec::new();

    for layer in layers {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for polygon in layer.polygons {
            let rings: Vec<Vec<(f64, f64)>> = polygon
                .into_iter()
                .map(|ring| {
                    ring.into_iter()
                        .map(|(lon, lat)| projector.project(lat, lon))
                        .collect::<Vec<(f64, f64)>>()
                })
                .filter(|ring| ring_overlaps_bbox(ring, bbox))
                .collect();
            if !rings.is_empty() {
                out.push(ProjectedPolygonFill {
                    rings,
                    color,
                    role: layer.role,
                });
            }
        }
    }

    out
}

fn ring_overlaps_bbox(ring: &[(f64, f64)], bbox: (f64, f64, f64, f64)) -> bool {
    let (mut rx_min, mut rx_max) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut ry_min, mut ry_max) = (f64::INFINITY, f64::NEG_INFINITY);
    for &(x, y) in ring {
        rx_min = rx_min.min(x);
        rx_max = rx_max.max(x);
        ry_min = ry_min.min(y);
        ry_max = ry_max.max(y);
    }
    !(rx_max < bbox.0 || rx_min > bbox.1 || ry_max < bbox.2 || ry_min > bbox.3)
}

pub(crate) fn overlay_counts(
    lines: &[ProjectedLineOverlay],
    polygons: &[ProjectedPolygonFill],
) -> ProjectedOverlayCounts {
    ProjectedOverlayCounts {
        line_overlays: lines.len(),
        line_points: lines.iter().map(|line| line.points.len()).sum(),
        polygon_fills: polygons.len(),
        polygon_rings: polygons.iter().map(|polygon| polygon.rings.len()).sum(),
        polygon_points: polygons
            .iter()
            .flat_map(|polygon| polygon.rings.iter())
            .map(Vec::len)
            .sum(),
    }
}

fn build_layout(
    width: u32,
    height: u32,
    colorbar: bool,
    has_title: bool,
    visual_mode: ProductVisualMode,
) -> Layout {
    let presentation = RenderPresentation::for_mode(visual_mode);
    let chrome_scale = resolve_chrome_scale(width, height);
    let metrics = presentation.layout;
    let margin_x = scale_u32(metrics.margin_x, chrome_scale);
    let text_scale = (chrome_scale.round().clamp(1.0, 4.0) as u32).max(1);
    let title_line_h = line_height_for_scale(text_scale, false);
    let subtitle_line_h = line_height_for_scale(text_scale, false);
    let title_h_raw = scale_u32(metrics.title_h, chrome_scale).max(
        scale_u32(3, chrome_scale)
            .saturating_add(title_line_h)
            .saturating_add(scale_u32(2, chrome_scale))
            .saturating_add(subtitle_line_h)
            .saturating_add(scale_u32(2, chrome_scale)),
    );
    let footer_h_raw = scale_u32(metrics.footer_h, chrome_scale);
    let colorbar_h = scale_u32(metrics.colorbar_h, chrome_scale);
    let colorbar_gap = scale_u32(metrics.colorbar_gap, chrome_scale);
    let map_x = margin_x.min(width.saturating_sub(1));
    let title_h = if has_title { title_h_raw } else { 0 };
    let footer_h = if colorbar {
        footer_h_raw.max(colorbar_h + colorbar_gap + 10)
    } else {
        footer_h_raw.min(18)
    };
    let map_y = title_h.min(height.saturating_sub(1));
    let map_w = width.saturating_sub(map_x.saturating_mul(2)).max(1);
    let map_h = height.saturating_sub(map_y).saturating_sub(footer_h).max(1);
    Layout {
        map_x,
        map_y,
        map_w,
        map_h,
    }
}

pub(crate) fn prepare_projected_surface(
    spec: &ProjectedSurfaceSpec,
    lat: &Array2Data,
    lon: &Array2Data,
) -> PyResult<(Projector, Geometry, Layout, SurfaceContext)> {
    let context = surface_context(spec);
    let projector = projector_from_spec(&spec.projection)?;
    let geometry = build_geometry(
        projector,
        lat,
        lon,
        context.width,
        context.height,
        context.colorbar,
        context.has_title,
        context.visual_mode,
    );
    let layout = build_layout(
        context.width,
        context.height,
        context.colorbar,
        context.has_title,
        context.visual_mode,
    );
    Ok((projector, geometry, layout, context))
}

fn resolve_chrome_scale(width: u32, height: u32) -> f32 {
    let base_area = 1200.0_f64 * 900.0_f64;
    let area = (width.max(1) as f64) * (height.max(1) as f64);
    (area / base_area).sqrt().clamp(1.0, 3.0) as f32
}

fn scale_u32(value: u32, scale: f32) -> u32 {
    ((value as f32) * scale).round().max(1.0) as u32
}

fn line_height_for_scale(scale: u32, bold: bool) -> u32 {
    let base = match (scale.max(1), bold) {
        (1, false) => 12.0,
        (1, true) => 15.0,
        (2, false) => 16.0,
        (2, true) => 19.0,
        (s, false) => 12.0 + (s as f32 - 1.0) * 4.0,
        (s, true) => 15.0 + (s as f32 - 1.0) * 4.0,
    };
    base.ceil() as u32
}

pub(crate) fn projector_from_spec(spec: &ProjectionSpec) -> PyResult<Projector> {
    Projector::from_spec(spec)
}

pub(crate) fn geometry_description(
    spec: &ProjectedSurfaceSpec,
    lat: &Array2Data,
    lon: &Array2Data,
    include_projected_domain: bool,
) -> PyResult<ProjectedGeometryMetadata> {
    let (projector, geometry, layout, _) = prepare_projected_surface(spec, lat, lon)?;
    Ok(geometry_metadata(
        "projected_geometry",
        spec,
        projector,
        &geometry,
        layout,
        0,
        lat,
        include_projected_domain,
    ))
}

pub(crate) fn projected_overlay_description(
    spec: &ProjectedSurfaceSpec,
    lat: &Array2Data,
    lon: &Array2Data,
    include_geometry: bool,
) -> PyResult<ProjectedBasemapOverlayMetadata> {
    let (projector, geometry, layout, _) = prepare_projected_surface(spec, lat, lon)?;
    let base = geometry_metadata(
        "projected_basemap_overlays",
        spec,
        projector,
        &geometry,
        layout,
        0,
        lat,
        false,
    );
    let basemap_style = spec.basemap_style();
    let (line_overlays, polygon_fills) = if let Some(style) = basemap_style.to_option() {
        (
            project_lines(projector, &geometry.padded_extent, style),
            project_polygons(projector, &geometry.padded_extent, style),
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let counts = overlay_counts(&line_overlays, &polygon_fills);
    Ok(ProjectedBasemapOverlayMetadata {
        kind: "projected_basemap_overlays",
        schema_version: 1,
        basemap_style,
        grid_shape: base.grid_shape,
        pixel_bounds: base.pixel_bounds,
        data_extent: base.data_extent,
        valid_data_extent: base.valid_data_extent,
        projection_info: base.projection_info,
        projection: base.projection,
        extents: base.extents,
        layout: base.layout,
        projected_corners: base.projected_corners,
        counts,
        line_overlays: include_geometry.then_some(line_overlays),
        polygon_fills: include_geometry.then_some(polygon_fills),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_geometry_extent_uses_all_grid_points_not_only_corners() {
        let lat = Array2Data {
            ny: 3,
            nx: 3,
            values: vec![0.0, 0.0, 0.0, 0.0, 5.0, 0.0, 1.0, 1.0, 1.0],
        };
        let lon = Array2Data {
            ny: 3,
            nx: 3,
            values: vec![0.0, 1.0, 2.0, 0.0, 5.0, 2.0, 0.0, 1.0, 2.0],
        };

        let geometry = build_geometry(
            Projector::LatLon,
            &lat,
            &lon,
            600,
            600,
            false,
            false,
            ProductVisualMode::FilledMeteorology,
        );

        assert_eq!(geometry.valid_extent.x_min, 0.0);
        assert_eq!(geometry.valid_extent.y_min, 0.0);
        assert_eq!(geometry.valid_extent.x_max, 5.0);
        assert_eq!(geometry.valid_extent.y_max, 5.0);
        assert!(geometry.padded_extent.x_max >= geometry.valid_extent.x_max);
        assert!(geometry.padded_extent.y_max >= geometry.valid_extent.y_max);
    }
}

pub(crate) fn projection_description(
    spec: &ProjectionSpec,
) -> PyResult<ProjectedProjectionDescription> {
    let projector = projector_from_spec(spec)?;
    Ok(ProjectedProjectionDescription {
        kind: "projected_projection",
        schema_version: 1,
        projection_info: projector.projection_info(),
        projection: projection_metadata(spec, projector),
    })
}
