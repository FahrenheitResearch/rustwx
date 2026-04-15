use crate::cache::{load_bincode, store_bincode};
use grib_core::grib2::{
    Grib2File, Grib2Message, flip_rows, grid_latlon, unpack_message_normalized,
};
use image::DynamicImage;
use rustwx_core::{
    CycleSpec, Field2D, GridShape, LatLonGrid, ModelId, ModelRunRequest, ProductKey, RustwxError,
    SourceId,
};
use rustwx_io::{CachedFetchResult, FetchRequest, artifact_cache_dir, fetch_bytes_with_cache};
use rustwx_models::{LatestRun, latest_available_run};
use rustwx_render::{
    Color, MapRenderRequest, PanelGridLayout, PanelPadding, ProjectedDomain, ProjectedExtent,
    ProjectedLineOverlay, Solar07Product, render_panel_grid,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use wrf_render::features::load_styled_conus_features;
use wrf_render::projection::LambertConformal;
use wrf_render::render::map_frame_aspect_ratio;
use wrf_render::text;

pub const SURFACE_PATTERNS: &[&str] = &[
    "PRES:surface",
    "HGT:surface",
    "TMP:2 m above ground",
    "SPFH:2 m above ground",
    "UGRD:10 m above ground",
    "VGRD:10 m above ground",
];

pub const PRESSURE_PATTERNS: &[&str] = &["HGT:", "TMP:", "SPFH:", "UGRD:", "VGRD:"];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrSurfaceFields {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
    pub nx: usize,
    pub ny: usize,
    pub psfc_pa: Vec<f64>,
    pub orog_m: Vec<f64>,
    pub t2_k: Vec<f64>,
    pub q2_kgkg: Vec<f64>,
    pub u10_ms: Vec<f64>,
    pub v10_ms: Vec<f64>,
    pub lambert_latin1: f64,
    pub lambert_latin2: f64,
    pub lambert_lov: f64,
}

impl HrrrSurfaceFields {
    pub fn core_grid(&self) -> Result<LatLonGrid, RustwxError> {
        LatLonGrid::new(
            GridShape::new(self.nx, self.ny)?,
            self.lat.iter().map(|&v| v as f32).collect(),
            self.lon.iter().map(|&v| v as f32).collect(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrPressureFields {
    pub pressure_levels_hpa: Vec<f64>,
    pub temperature_c_3d: Vec<f64>,
    pub qvapor_kgkg_3d: Vec<f64>,
    pub u_ms_3d: Vec<f64>,
    pub v_ms_3d: Vec<f64>,
    pub gh_m_3d: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct HrrrFetchedSubset {
    pub request: FetchRequest,
    pub fetched: CachedFetchResult,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CachedDecode<T> {
    pub value: T,
    pub cache_hit: bool,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ProjectedMap {
    pub projected_x: Vec<f64>,
    pub projected_y: Vec<f64>,
    pub extent: ProjectedExtent,
    pub lines: Vec<ProjectedLineOverlay>,
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

pub fn resolve_hrrr_run(
    date: &str,
    cycle_override: Option<u8>,
    source: SourceId,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model: ModelId::Hrrr,
            cycle: CycleSpec::new(date, hour)?,
            source,
        }),
        None => Ok(latest_available_run(ModelId::Hrrr, Some(source), date)?),
    }
}

pub fn fetch_hrrr_subset(
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    product: &str,
    patterns: &[&str],
    cache_root: &Path,
    use_cache: bool,
) -> Result<HrrrFetchedSubset, Box<dyn std::error::Error>> {
    let request = ModelRunRequest::new(ModelId::Hrrr, cycle, forecast_hour, product)?;
    let fetch_request = FetchRequest {
        request,
        source_override: Some(source),
        variable_patterns: patterns.iter().map(|s| s.to_string()).collect(),
    };
    let fetched = fetch_bytes_with_cache(&fetch_request, cache_root, use_cache)?;
    let bytes = fetched.result.bytes.clone();
    Ok(HrrrFetchedSubset {
        request: fetch_request,
        fetched,
        bytes,
    })
}

pub fn decode_cache_path(cache_root: &Path, fetch: &FetchRequest, name: &str) -> PathBuf {
    artifact_cache_dir(cache_root, fetch)
        .join("decoded")
        .join(format!("{name}.bin"))
}

pub fn load_or_decode_surface(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<CachedDecode<HrrrSurfaceFields>, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<HrrrSurfaceFields>(path)? {
            return Ok(CachedDecode {
                value: cached,
                cache_hit: true,
                path: path.to_path_buf(),
            });
        }
    }
    let decoded = decode_surface(bytes)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok(CachedDecode {
        value: decoded,
        cache_hit: false,
        path: path.to_path_buf(),
    })
}

pub fn load_or_decode_pressure(
    path: &Path,
    bytes: &[u8],
    nx: usize,
    ny: usize,
    use_cache: bool,
) -> Result<CachedDecode<HrrrPressureFields>, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<HrrrPressureFields>(path)? {
            return Ok(CachedDecode {
                value: cached,
                cache_hit: true,
                path: path.to_path_buf(),
            });
        }
    }
    let decoded = decode_pressure(bytes, nx, ny)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok(CachedDecode {
        value: decoded,
        cache_hit: false,
        path: path.to_path_buf(),
    })
}

pub fn decode_surface(bytes: &[u8]) -> Result<HrrrSurfaceFields, Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let sample = file
        .messages
        .first()
        .ok_or("surface subset had no GRIB messages")?;
    let (mut lat, mut lon_raw) = grid_latlon(&sample.grid);
    if sample.grid.scan_mode & 0x40 != 0 {
        flip_rows(&mut lat, sample.grid.nx as usize, sample.grid.ny as usize);
        flip_rows(
            &mut lon_raw,
            sample.grid.nx as usize,
            sample.grid.ny as usize,
        );
    }
    let lon = lon_raw
        .into_iter()
        .map(normalize_longitude)
        .collect::<Vec<_>>();
    let nx = sample.grid.nx as usize;
    let ny = sample.grid.ny as usize;

    let psfc_pa = unpack_message_normalized(find_message(&file.messages, 0, 3, 0, 1, Some(0.0))?)?;
    let orog_m = unpack_message_normalized(find_message(&file.messages, 0, 3, 5, 1, Some(0.0))?)?;
    let t2_k = unpack_message_normalized(find_message(&file.messages, 0, 0, 0, 103, Some(2.0))?)?;
    let q2_specific =
        unpack_message_normalized(find_message(&file.messages, 0, 1, 0, 103, Some(2.0))?)?;
    let u10_ms =
        unpack_message_normalized(find_message(&file.messages, 0, 2, 2, 103, Some(10.0))?)?;
    let v10_ms =
        unpack_message_normalized(find_message(&file.messages, 0, 2, 3, 103, Some(10.0))?)?;

    Ok(HrrrSurfaceFields {
        lat,
        lon,
        nx,
        ny,
        psfc_pa,
        orog_m,
        t2_k,
        q2_kgkg: q_to_mixing_ratio(&q2_specific),
        u10_ms,
        v10_ms,
        lambert_latin1: sample.grid.latin1,
        lambert_latin2: sample.grid.latin2,
        lambert_lov: sample.grid.lov,
    })
}

pub fn decode_pressure(
    bytes: &[u8],
    nx: usize,
    ny: usize,
) -> Result<HrrrPressureFields, Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let temperature = collect_levels(&file.messages, 0, 0, 0, 100)?;
    let specific_humidity = collect_levels(&file.messages, 0, 1, 0, 100)?;
    let u_wind = collect_levels(&file.messages, 0, 2, 2, 100)?;
    let v_wind = collect_levels(&file.messages, 0, 2, 3, 100)?;
    let gh = collect_levels(&file.messages, 0, 3, 5, 100)?;

    let levels = temperature
        .iter()
        .map(|(level, _)| *level)
        .collect::<Vec<_>>();
    for dataset in [&specific_humidity, &u_wind, &v_wind, &gh] {
        let candidate = dataset.iter().map(|(level, _)| *level).collect::<Vec<_>>();
        if candidate != levels {
            return Err("pressure subset levels did not line up across variables".into());
        }
    }

    let expected = nx * ny;
    let flatten = |records: &Vec<(f64, Vec<f64>)>| -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        let mut out = Vec::with_capacity(records.len() * expected);
        for (_, values) in records {
            if values.len() != expected {
                return Err("decoded pressure field had unexpected grid size".into());
            }
            out.extend_from_slice(values);
        }
        Ok(out)
    };

    Ok(HrrrPressureFields {
        pressure_levels_hpa: levels
            .into_iter()
            .map(normalize_pressure_level_hpa)
            .collect(),
        temperature_c_3d: flatten(&temperature)?
            .into_iter()
            .map(|value| value - 273.15)
            .collect(),
        qvapor_kgkg_3d: q_to_mixing_ratio(&flatten(&specific_humidity)?),
        u_ms_3d: flatten(&u_wind)?,
        v_ms_3d: flatten(&v_wind)?,
        gh_m_3d: flatten(&gh)?,
    })
}

pub fn build_projected_map(
    surface: &HrrrSurfaceFields,
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    let (lon_min, lon_max, lat_min, lat_max) = bounds;
    let center_lat = surface
        .lat
        .iter()
        .zip(surface.lon.iter())
        .filter(|(_, lon)| lon.is_finite())
        .map(|(lat, _)| *lat)
        .sum::<f64>()
        / surface.lat.len() as f64;
    let proj = LambertConformal::new(
        surface.lambert_latin1,
        surface.lambert_latin2,
        normalize_longitude(surface.lambert_lov),
        center_lat,
    );

    let mut projected_x = Vec::with_capacity(surface.lat.len());
    let mut projected_y = Vec::with_capacity(surface.lat.len());
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    for (&lat, &lon) in surface.lat.iter().zip(surface.lon.iter()) {
        let (x, y) = proj.project(lat, lon);
        projected_x.push(x);
        projected_y.push(y);
        if lon >= lon_min && lon <= lon_max && lat >= lat_min && lat <= lat_max {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    }

    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        return Err("requested crop produced an empty projected extent".into());
    }

    let extent =
        wrf_render::overlay::MapExtent::from_bounds(min_x, max_x, min_y, max_y, target_ratio);
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
            });
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
    })
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

pub fn broadcast_levels_pa(levels_hpa: &[f64], n2d: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(levels_hpa.len() * n2d);
    for level in levels_hpa {
        out.extend(std::iter::repeat_n(*level * 100.0, n2d));
    }
    out
}

fn collect_levels(
    messages: &[Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    let mut records = messages
        .iter()
        .filter(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
        })
        .map(|msg| Ok((msg.product.level_value, unpack_message_normalized(msg)?)))
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    records.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(records)
}

fn find_message<'a>(
    messages: &'a [Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
    level_value: Option<f64>,
) -> Result<&'a Grib2Message, Box<dyn std::error::Error>> {
    messages
        .iter()
        .find(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
                && level_value
                    .map(|level| (msg.product.level_value - level).abs() < 0.25)
                    .unwrap_or(true)
        })
        .ok_or_else(|| {
            format!(
                "missing GRIB message for discipline={discipline} category={category} number={number} level_type={level_type} level={level_value:?}"
            )
            .into()
        })
}

fn q_to_mixing_ratio(values: &[f64]) -> Vec<f64> {
    values
        .iter()
        .map(|&q| (q / (1.0 - q).max(1.0e-12)).max(1.0e-10))
        .collect()
}

fn normalize_pressure_level_hpa(level: f64) -> f64 {
    if level > 2_000.0 {
        level / 100.0
    } else {
        level
    }
}

fn normalize_longitude(lon: f64) -> f64 {
    if lon > 180.0 { lon - 360.0 } else { lon }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_hrrr_cycle_avoids_latest_probe() {
        let latest = resolve_hrrr_run("20260414", Some(19), SourceId::Aws).unwrap();
        assert_eq!(latest.model, ModelId::Hrrr);
        assert_eq!(latest.cycle.date_yyyymmdd, "20260414");
        assert_eq!(latest.cycle.hour_utc, 19);
        assert_eq!(latest.source, SourceId::Aws);
    }

    #[test]
    fn broadcast_levels_builds_pa_volume() {
        assert_eq!(
            broadcast_levels_pa(&[1000.0, 850.0], 3),
            vec![100000.0, 100000.0, 100000.0, 85000.0, 85000.0, 85000.0]
        );
    }

    #[test]
    fn panel_field_keeps_title_override() {
        let field = Solar07PanelField::new(Solar07Product::StpFixed, "dimensionless", vec![1.0])
            .with_title_override("STP (FIXED)");
        assert_eq!(field.title_override.as_deref(), Some("STP (FIXED)"));
    }

    #[test]
    fn surface_core_grid_preserves_shape() {
        let surface = HrrrSurfaceFields {
            lat: vec![35.0, 35.0, 36.0, 36.0],
            lon: vec![-100.0, -99.0, -100.0, -99.0],
            nx: 2,
            ny: 2,
            psfc_pa: vec![100000.0; 4],
            orog_m: vec![0.0; 4],
            t2_k: vec![290.0; 4],
            q2_kgkg: vec![0.01; 4],
            u10_ms: vec![5.0; 4],
            v10_ms: vec![2.0; 4],
            lambert_latin1: 33.0,
            lambert_latin2: 45.0,
            lambert_lov: -97.0,
        };
        let grid = surface.core_grid().unwrap();
        assert_eq!(grid.shape.nx, 2);
        assert_eq!(grid.shape.ny, 2);
    }
}
