use image::ExtendedColorType;
use image::ImageEncoder;
use image::RgbaImage;
use image::codecs::png::{CompressionType, FilterType as PngFilterType, PngEncoder};
use image::imageops::crop_imm;
use numpy::PyReadonlyArray2;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use rustwx_render::{
    BasemapStyle, Color, ColorScale, ContourStyle, DiscreteColorScale, DomainFrame, ExtendMode,
    Field2D, GridShape, LatLonGrid, LevelDensity, MapRenderRequest, ProductKey, ProductVisualMode,
    ProjectedDomain, ProjectedExtent, ProjectedLineOverlay, ProjectedPolygonFill, RenderDensity,
    RenderPresentation, WindBarbStyle, load_styled_conus_features_for,
    load_styled_conus_polygons_for, map_frame_aspect_ratio_for_mode, render_image, solar07,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::f64::consts::PI;
use std::fs;
use std::path::Path;

const WRF_EARTH_RADIUS_M: f64 = 6_370_000.0;
const DEG_TO_RAD: f64 = PI / 180.0;

fn default_projected_render_density() -> RenderDensity {
    RenderDensity {
        fill: LevelDensity::default(),
        palette_multiplier: 1,
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BasemapStyleSpec {
    Filled,
    White,
    None,
}

impl Default for BasemapStyleSpec {
    fn default() -> Self {
        Self::None
    }
}

impl BasemapStyleSpec {
    fn to_option(self) -> Option<BasemapStyle> {
        match self {
            Self::Filled => Some(BasemapStyle::Filled),
            Self::White => Some(BasemapStyle::White),
            Self::None => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PaletteSpec {
    Cape,
    ThreeCape,
    Ehi,
    Srh,
    Stp,
    LapseRate,
    Uh,
    MlMetric,
    Reflectivity,
    Winds,
    Temperature,
    Dewpoint,
    Rh,
    RelVort,
    SimIr,
    GeopotAnomaly,
    Precip,
    ShadedOverlay,
}

impl From<PaletteSpec> for solar07::Solar07Palette {
    fn from(value: PaletteSpec) -> Self {
        match value {
            PaletteSpec::Cape => Self::Cape,
            PaletteSpec::ThreeCape => Self::ThreeCape,
            PaletteSpec::Ehi => Self::Ehi,
            PaletteSpec::Srh => Self::Srh,
            PaletteSpec::Stp => Self::Stp,
            PaletteSpec::LapseRate => Self::LapseRate,
            PaletteSpec::Uh => Self::Uh,
            PaletteSpec::MlMetric => Self::MlMetric,
            PaletteSpec::Reflectivity => Self::Reflectivity,
            PaletteSpec::Winds => Self::Winds,
            PaletteSpec::Temperature => Self::Temperature,
            PaletteSpec::Dewpoint => Self::Dewpoint,
            PaletteSpec::Rh => Self::Rh,
            PaletteSpec::RelVort => Self::RelVort,
            PaletteSpec::SimIr => Self::SimIr,
            PaletteSpec::GeopotAnomaly => Self::GeopotAnomaly,
            PaletteSpec::Precip => Self::Precip,
            PaletteSpec::ShadedOverlay => Self::ShadedOverlay,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ScaleSpec {
    Palette {
        palette: PaletteSpec,
        levels: Vec<f64>,
        #[serde(default = "default_extend_both")]
        extend: ExtendMode,
        mask_below: Option<f64>,
    },
    Discrete {
        levels: Vec<f64>,
        colors: Vec<Color>,
        #[serde(default = "default_extend_both")]
        extend: ExtendMode,
        mask_below: Option<f64>,
    },
}

impl ScaleSpec {
    fn into_color_scale(self) -> ColorScale {
        match self {
            Self::Palette {
                palette,
                levels,
                extend,
                mask_below,
            } => ColorScale::Discrete(solar07::palette_scale(
                palette.into(),
                levels,
                extend,
                mask_below,
            )),
            Self::Discrete {
                levels,
                colors,
                extend,
                mask_below,
            } => ColorScale::Discrete(DiscreteColorScale {
                levels,
                colors,
                extend,
                mask_below,
            }),
        }
    }
}

fn default_extend_both() -> ExtendMode {
    ExtendMode::Both
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectionSpec {
    map_proj: i32,
    truelat1: Option<f64>,
    truelat2: Option<f64>,
    stand_lon: Option<f64>,
    cen_lat: Option<f64>,
    cen_lon: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ContourSpec {
    levels: Vec<f64>,
    #[serde(default)]
    style: Option<ContourStyle>,
}

#[derive(Debug, Clone, Deserialize)]
struct OverlaySpec {
    scale: ScaleSpec,
    #[serde(default)]
    visual_mode: Option<ProductVisualMode>,
}

#[derive(Debug, Clone, Deserialize)]
struct RenderSpec {
    output_path: String,
    product_key: String,
    field_units: String,
    scale: ScaleSpec,
    projection: ProjectionSpec,
    #[serde(default)]
    width: Option<u32>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    colorbar: Option<bool>,
    #[serde(default)]
    tick_step: Option<f64>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    subtitle_left: Option<String>,
    #[serde(default)]
    subtitle_center: Option<String>,
    #[serde(default)]
    subtitle_right: Option<String>,
    #[serde(default)]
    visual_mode: Option<ProductVisualMode>,
    #[serde(default)]
    basemap_style: Option<BasemapStyleSpec>,
    #[serde(default)]
    domain_frame: Option<bool>,
    #[serde(default)]
    contour: Option<ContourSpec>,
    #[serde(default)]
    overlay: Option<OverlaySpec>,
    #[serde(default)]
    wind_barbs: Option<WindBarbStyle>,
}

#[derive(Debug, Clone)]
struct Array2Data {
    ny: usize,
    nx: usize,
    values: Vec<f64>,
}

impl Array2Data {
    fn to_f32(&self) -> Vec<f32> {
        self.values.iter().map(|value| *value as f32).collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct Layout {
    map_x: u32,
    map_y: u32,
    map_w: u32,
    map_h: u32,
}

#[derive(Debug, Clone)]
struct Geometry {
    x: Vec<f64>,
    y: Vec<f64>,
    valid_extent: ProjectedExtent,
    padded_extent: ProjectedExtent,
}

#[derive(Debug, Clone, Copy)]
struct LambertProjector {
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
struct PolarProjector {
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
struct MercatorProjector {
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
enum Projector {
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

    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        match self {
            Self::Lambert { inner, .. } => inner.project(lat, lon),
            Self::Polar { inner, .. } => inner.project(lat, lon),
            Self::Mercator { inner, .. } => inner.project(lat, lon),
            Self::LatLon => (lon, lat),
        }
    }

    fn projection_info(self) -> Map<String, Value> {
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

fn extract_array(name: &str, array: PyReadonlyArray2<'_, f64>) -> PyResult<Array2Data> {
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

    for (&lat_deg, &lon_deg) in lat.values.iter().zip(lon.values.iter()) {
        let (px, py) = projector.project(lat_deg, lon_deg);
        x.push(px);
        y.push(py);
    }

    let nx = lat.nx;
    let ny = lat.ny;
    let idx = |row: usize, col: usize| row * nx + col;
    let corners = [
        (lat.values[idx(0, 0)], lon.values[idx(0, 0)]),
        (
            lat.values[idx(ny.saturating_sub(1), nx.saturating_sub(1))],
            lon.values[idx(ny.saturating_sub(1), nx.saturating_sub(1))],
        ),
        (
            lat.values[idx(ny.saturating_sub(1), 0)],
            lon.values[idx(ny.saturating_sub(1), 0)],
        ),
        (
            lat.values[idx(0, nx.saturating_sub(1))],
            lon.values[idx(0, nx.saturating_sub(1))],
        ),
    ];

    let projected_corners: Vec<(f64, f64)> = corners
        .into_iter()
        .map(|(lat_deg, lon_deg)| projector.project(lat_deg, lon_deg))
        .collect();
    let data_x_min = projected_corners
        .iter()
        .map(|(px, _)| *px)
        .fold(f64::INFINITY, f64::min);
    let data_x_max = projected_corners
        .iter()
        .map(|(px, _)| *px)
        .fold(f64::NEG_INFINITY, f64::max);
    let data_y_min = projected_corners
        .iter()
        .map(|(_, py)| *py)
        .fold(f64::INFINITY, f64::min);
    let data_y_max = projected_corners
        .iter()
        .map(|(_, py)| *py)
        .fold(f64::NEG_INFINITY, f64::max);

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
    }
}

fn project_lines(
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

fn project_polygons(
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

fn build_request(
    spec: &RenderSpec,
    grid: &LatLonGrid,
    field_values: Vec<f32>,
    geometry: &Geometry,
    visual_mode: ProductVisualMode,
) -> PyResult<MapRenderRequest> {
    let field = Field2D::new(
        ProductKey::named(spec.product_key.clone()),
        spec.field_units.clone(),
        grid.clone(),
        field_values,
    )
    .map_err(to_runtime_error)?;
    let mut request = MapRenderRequest::new(field, spec.scale.clone().into_color_scale());
    request.width = spec.width.unwrap_or(1100);
    request.height = spec.height.unwrap_or(850);
    request.render_density = default_projected_render_density();
    request.colorbar = spec.colorbar.unwrap_or(true);
    request.title = spec.title.clone();
    request.subtitle_left = spec.subtitle_left.clone();
    request.subtitle_center = spec.subtitle_center.clone();
    request.subtitle_right = spec.subtitle_right.clone();
    request.cbar_tick_step = spec.tick_step;
    request.visual_mode = visual_mode;
    request.domain_frame = if spec.domain_frame.unwrap_or(true) {
        Some(DomainFrame::model_data_default())
    } else {
        None
    };
    request.projected_domain = Some(ProjectedDomain {
        x: geometry.x.clone(),
        y: geometry.y.clone(),
        extent: geometry.padded_extent.clone(),
    });
    if let Some(style) = spec.basemap_style.unwrap_or_default().to_option() {
        request.projected_polygons = project_polygons(
            projector_from_spec(&spec.projection)?,
            &geometry.padded_extent,
            style,
        );
        request.projected_lines = project_lines(
            projector_from_spec(&spec.projection)?,
            &geometry.padded_extent,
            style,
        );
    }
    Ok(request)
}

fn projector_from_spec(spec: &ProjectionSpec) -> PyResult<Projector> {
    Projector::from_spec(spec)
}

fn build_overlay_request(
    spec: &RenderSpec,
    grid: &LatLonGrid,
    field_values: Vec<f32>,
    geometry: &Geometry,
    overlay: &OverlaySpec,
) -> PyResult<MapRenderRequest> {
    let field = Field2D::new(
        ProductKey::named(format!("{} Overlay", spec.product_key)),
        spec.field_units.clone(),
        grid.clone(),
        field_values,
    )
    .map_err(to_runtime_error)?;
    let visual_mode = overlay
        .visual_mode
        .unwrap_or(ProductVisualMode::OverlayAnalysis);
    let mut request = MapRenderRequest::new(field, overlay.scale.clone().into_color_scale());
    request.width = spec.width.unwrap_or(1100);
    request.height = spec.height.unwrap_or(850);
    request.render_density = default_projected_render_density();
    request.background = Color::TRANSPARENT;
    request.colorbar = false;
    request.title = Some(String::new());
    request.subtitle_left = Some(String::new());
    request.subtitle_center = Some(String::new());
    request.subtitle_right = Some(String::new());
    request.visual_mode = visual_mode;
    request.domain_frame = if spec.domain_frame.unwrap_or(true) {
        Some(DomainFrame::model_data_default())
    } else {
        None
    };
    request.projected_domain = Some(ProjectedDomain {
        x: geometry.x.clone(),
        y: geometry.y.clone(),
        extent: geometry.padded_extent.clone(),
    });
    Ok(request)
}

fn add_contour_layer(
    request: &mut MapRenderRequest,
    grid: &LatLonGrid,
    spec: &RenderSpec,
    contour_field: &Array2Data,
) -> PyResult<()> {
    let contour_spec = spec
        .contour
        .as_ref()
        .ok_or_else(|| PyValueError::new_err("Contour field provided without contour spec"))?;
    let field = Field2D::new(
        ProductKey::named(format!("{} Contours", spec.product_key)),
        spec.field_units.clone(),
        grid.clone(),
        contour_field.to_f32(),
    )
    .map_err(to_runtime_error)?;
    request
        .add_contour_field(
            &field,
            contour_spec.levels.clone(),
            contour_spec.style.unwrap_or_default(),
        )
        .map_err(to_runtime_error)?;
    Ok(())
}

fn add_wind_barbs(
    request: &mut MapRenderRequest,
    grid: &LatLonGrid,
    spec: &RenderSpec,
    wind_u: &Array2Data,
    wind_v: &Array2Data,
) -> PyResult<()> {
    let style = spec.wind_barbs.unwrap_or_default();
    let u_field = Field2D::new(
        ProductKey::named(format!("{} Wind U", spec.product_key)),
        "kt",
        grid.clone(),
        wind_u.to_f32(),
    )
    .map_err(to_runtime_error)?;
    let v_field = Field2D::new(
        ProductKey::named(format!("{} Wind V", spec.product_key)),
        "kt",
        grid.clone(),
        wind_v.to_f32(),
    )
    .map_err(to_runtime_error)?;
    request
        .add_wind_barbs(&u_field, &v_field, style)
        .map_err(to_runtime_error)?;
    Ok(())
}

fn alpha_composite(base: &mut RgbaImage, overlay: &RgbaImage) {
    let width = base.width().min(overlay.width());
    let height = base.height().min(overlay.height());
    for y in 0..height {
        for x in 0..width {
            let dst = *base.get_pixel(x, y);
            let src = *overlay.get_pixel(x, y);
            let src_a = src.0[3] as f32 / 255.0;
            if src_a <= 0.0 {
                continue;
            }
            let dst_a = dst.0[3] as f32 / 255.0;
            let out_a = src_a + dst_a * (1.0 - src_a);
            let mut out = [0_u8; 4];
            for channel in 0..3 {
                let src_c = src.0[channel] as f32 / 255.0;
                let dst_c = dst.0[channel] as f32 / 255.0;
                let value = if out_a <= 0.0 {
                    0.0
                } else {
                    (src_c * src_a + dst_c * dst_a * (1.0 - src_a)) / out_a
                };
                out[channel] = (value * 255.0).round().clamp(0.0, 255.0) as u8;
            }
            out[3] = (out_a * 255.0).round().clamp(0.0, 255.0) as u8;
            base.put_pixel(x, y, image::Rgba(out));
        }
    }
}

fn strip_overlay_background(image: &mut RgbaImage) {
    for pixel in image.pixels_mut() {
        let [r, g, b, _] = pixel.0;
        if u16::from(r.abs_diff(255)) + u16::from(g.abs_diff(255)) + u16::from(b.abs_diff(255)) <= 6
        {
            *pixel = image::Rgba([0, 0, 0, 0]);
        }
    }
}

fn row_is_background(image: &RgbaImage, y: u32, background: [u8; 4]) -> bool {
    (0..image.width()).all(|x| {
        let px = image.get_pixel(x, y).0;
        let diff = u16::from(px[0].abs_diff(background[0]))
            + u16::from(px[1].abs_diff(background[1]))
            + u16::from(px[2].abs_diff(background[2]))
            + u16::from(px[3].abs_diff(background[3]));
        diff <= 6
    })
}

fn trim_vertical_canvas_whitespace(image: &RgbaImage, background: [u8; 4]) -> (RgbaImage, u32) {
    if image.height() <= 2 {
        return (image.clone(), 0);
    }
    let first_non_bg = (0..image.height()).find(|&y| !row_is_background(image, y, background));
    let last_non_bg = (0..image.height()).rfind(|&y| !row_is_background(image, y, background));
    let (Some(first), Some(last)) = (first_non_bg, last_non_bg) else {
        return (image.clone(), 0);
    };
    let crop_top = first.saturating_sub(2);
    let crop_bottom = last.saturating_add(2).min(image.height().saturating_sub(1));
    let crop_h = crop_bottom.saturating_sub(crop_top).saturating_add(1);
    if crop_top == 0 && crop_h == image.height() {
        return (image.clone(), 0);
    }
    (
        crop_imm(image, 0, crop_top, image.width(), crop_h).to_image(),
        crop_top,
    )
}

fn write_png(path: &Path, image: &RgbaImage) -> PyResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    let mut bytes = Vec::new();
    let encoder = PngEncoder::new_with_quality(
        &mut bytes,
        CompressionType::Default,
        PngFilterType::Adaptive,
    );
    encoder
        .write_image(
            image.as_raw(),
            image.width(),
            image.height(),
            ExtendedColorType::Rgba8,
        )
        .map_err(io_error)?;
    fs::write(path, bytes).map_err(io_error)
}

fn io_error<E: std::fmt::Display>(error: E) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn to_runtime_error<E: std::fmt::Display>(error: E) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pyfunction]
#[pyo3(signature = (spec_json, lat, lon, field, contour_field=None, overlay_field=None, wind_u=None, wind_v=None))]
pub fn render_projected_map_json(
    spec_json: &str,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<String> {
    let spec: RenderSpec = serde_json::from_str(spec_json)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let lat = extract_array("lat", lat)?;
    let lon = extract_array("lon", lon)?;
    let field = extract_array("field", field)?;
    ensure_same_shape(&lat, &lon, "lon")?;
    ensure_same_shape(&field, &lat, "field")?;

    let contour_field = contour_field
        .map(|array| extract_array("contour_field", array))
        .transpose()?;
    let overlay_field = overlay_field
        .map(|array| extract_array("overlay_field", array))
        .transpose()?;
    let wind_u = wind_u
        .map(|array| extract_array("wind_u", array))
        .transpose()?;
    let wind_v = wind_v
        .map(|array| extract_array("wind_v", array))
        .transpose()?;

    if let Some(ref contour_field) = contour_field {
        ensure_same_shape(&field, contour_field, "contour_field")?;
    }
    if let Some(ref overlay_field) = overlay_field {
        ensure_same_shape(&field, overlay_field, "overlay_field")?;
        if spec.overlay.is_none() {
            return Err(PyValueError::new_err(
                "Overlay field provided without overlay spec",
            ));
        }
    }
    match (&wind_u, &wind_v) {
        (Some(u), Some(v)) => {
            ensure_same_shape(&field, u, "wind_u")?;
            ensure_same_shape(&field, v, "wind_v")?;
        }
        (None, None) => {}
        _ => {
            return Err(PyValueError::new_err(
                "wind_u and wind_v must both be provided or both be omitted",
            ));
        }
    }

    let width = spec.width.unwrap_or(1100);
    let height = spec.height.unwrap_or(850);
    let colorbar = spec.colorbar.unwrap_or(true);
    let has_title = spec.title.is_some()
        || spec.subtitle_left.is_some()
        || spec.subtitle_center.is_some()
        || spec.subtitle_right.is_some();
    let visual_mode = spec
        .visual_mode
        .unwrap_or(ProductVisualMode::FilledMeteorology);
    let projector = projector_from_spec(&spec.projection)?;
    let geometry = build_geometry(
        projector,
        &lat,
        &lon,
        width,
        height,
        colorbar,
        has_title,
        visual_mode,
    );

    let shape = GridShape::new(field.nx, field.ny).map_err(to_runtime_error)?;
    let grid = LatLonGrid::new(shape, lat.to_f32(), lon.to_f32()).map_err(to_runtime_error)?;

    let mut request = build_request(&spec, &grid, field.to_f32(), &geometry, visual_mode)?;
    if spec.overlay.is_none() {
        if let Some(ref contour_field) = contour_field {
            add_contour_layer(&mut request, &grid, &spec, contour_field)?;
        }
    }
    if let (Some(wind_u), Some(wind_v)) = (&wind_u, &wind_v) {
        add_wind_barbs(&mut request, &grid, &spec, wind_u, wind_v)?;
    }

    let mut image = render_image(&request).map_err(to_runtime_error)?;
    if let Some(ref overlay_spec) = spec.overlay {
        let overlay_field = overlay_field.as_ref().ok_or_else(|| {
            PyValueError::new_err("Overlay spec provided without overlay field array")
        })?;
        let overlay_request = build_overlay_request(
            &spec,
            &grid,
            overlay_field.to_f32(),
            &geometry,
            overlay_spec,
        )?;
        let mut overlay_request = overlay_request;
        if let Some(ref contour_field) = contour_field {
            add_contour_layer(&mut overlay_request, &grid, &spec, contour_field)?;
        }
        let mut overlay_image = render_image(&overlay_request).map_err(to_runtime_error)?;
        strip_overlay_background(&mut overlay_image);
        alpha_composite(&mut image, &overlay_image);
    }

    let background = RenderPresentation::for_mode(visual_mode)
        .canvas_background
        .to_image_rgba()
        .0;
    let (trimmed, crop_top) = trim_vertical_canvas_whitespace(&image, background);
    write_png(Path::new(&spec.output_path), &trimmed)?;

    let layout = build_layout(width, height, colorbar, has_title, visual_mode);
    let projection_info = projector.projection_info();
    let metadata = json!({
        "pixel_bounds": {
            "x_start": layout.map_x,
            "y_start": layout.map_y.saturating_sub(crop_top),
            "x_end": layout.map_x.saturating_add(layout.map_w),
            "y_end": layout.map_y.saturating_add(layout.map_h).saturating_sub(crop_top),
        },
        "data_extent": [
            geometry.padded_extent.x_min,
            geometry.padded_extent.x_max,
            geometry.padded_extent.y_min,
            geometry.padded_extent.y_max,
        ],
        "valid_data_extent": [
            geometry.valid_extent.x_min,
            geometry.valid_extent.x_max,
            geometry.valid_extent.y_min,
            geometry.valid_extent.y_max,
        ],
        "projection_info": projection_info,
    });
    serde_json::to_string_pretty(&metadata).map_err(to_runtime_error)
}

#[pyfunction]
#[pyo3(signature = (spec_json, lat, lon, field, contour_field=None, overlay_field=None, wind_u=None, wind_v=None))]
pub fn render_wrf_map_json(
    spec_json: &str,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<String> {
    render_projected_map_json(
        spec_json,
        lat,
        lon,
        field,
        contour_field,
        overlay_field,
        wind_u,
        wind_v,
    )
}
