use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use rustwx_render::{
    build_projected_map_with_options, save_png_profile_with_options, ChromeScale, Color,
    ColorScale, DiscreteColorScale, ExtendMode, Field2D, GridShape, LambertConformal, LatLonGrid,
    LegendControls, LegendMode, LevelDensity, MapRenderRequest, PngCompressionMode,
    PngWriteOptions, ProductKey, ProductVisualMode, ProjectedDomain, ProjectedMapBuildOptions,
    ProjectedMarkerShape, ProjectedPointOverlay, ProjectionSpec, RenderDensity,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlmLightningRenderRequest {
    pub data_dir: PathBuf,
    pub domain_slug: String,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub out_dir: PathBuf,
    #[serde(default = "default_width")]
    pub width: u32,
    #[serde(default = "default_height")]
    pub height: u32,
    #[serde(default = "default_max_age_min")]
    pub max_age_min: f64,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
}

impl GlmLightningRenderRequest {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        domain_slug: impl Into<String>,
        domain_label: impl Into<String>,
        bounds: (f64, f64, f64, f64),
        out_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            domain_slug: domain_slug.into(),
            domain_label: domain_label.into(),
            bounds,
            out_dir: out_dir.into(),
            width: default_width(),
            height: default_height(),
            max_age_min: default_max_age_min(),
            png_compression: default_png_compression(),
        }
    }

    fn png_write_options(&self) -> PngWriteOptions {
        PngWriteOptions {
            compression: self.png_compression,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlmFlash {
    pub lat: f64,
    pub lon: f64,
    pub energy_j: Option<f64>,
    pub area_m2: Option<f64>,
    pub time_utc: DateTime<Utc>,
    pub source_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlmLightningRenderReport {
    pub ok: bool,
    pub domain: String,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub png_path: PathBuf,
    pub data_json_path: PathBuf,
    pub flash_count_total: usize,
    pub flash_count_in_domain: usize,
    pub flash_count_drawn: usize,
    pub n_files: usize,
    pub time_window: GlmLightningTimeWindow,
    pub data_dir: PathBuf,
    pub timing: GlmLightningTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlmLightningTimeWindow {
    pub first: Option<DateTime<Utc>>,
    pub last: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlmLightningTiming {
    pub ingest_ms: u128,
    pub render_request_ms: u128,
    pub render_ms: u128,
    pub write_json_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlmLightningDataArtifact {
    domain: String,
    domain_label: String,
    bounds: (f64, f64, f64, f64),
    time_window: GlmLightningTimeWindow,
    flash_count_total: usize,
    flash_count_in_domain: usize,
    flashes: Vec<GlmFlash>,
}

#[derive(Debug)]
struct CollectedGlmFlashes {
    flashes: Vec<GlmFlash>,
    n_files: usize,
    first: Option<DateTime<Utc>>,
    last: Option<DateTime<Utc>>,
}

const MESH_NX: usize = 48;
const MESH_NY: usize = 48;

pub fn default_glm_data_dir() -> PathBuf {
    std::env::var_os("RUSTWX_GLM_DIR")
        .or_else(|| std::env::var_os("CWT_GLM_DIR"))
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("USERPROFILE")
                .or_else(|| std::env::var_os("HOME"))
                .map(PathBuf::from)
                .map(|home| home.join("lightning-test").join("data").join("glm"))
        })
        .unwrap_or_else(|| PathBuf::from("glm"))
}

pub fn render_glm_lightning_map(
    request: &GlmLightningRenderRequest,
) -> Result<GlmLightningRenderReport, Box<dyn Error>> {
    let total_start = Instant::now();
    if !request.data_dir.exists() {
        return Err(boxed_error(format!(
            "GLM data dir not found: {}",
            request.data_dir.display()
        )));
    }

    let ingest_start = Instant::now();
    let collected = collect_glm_flashes(&request.data_dir)?;
    let ingest_ms = ingest_start.elapsed().as_millis();

    let domain_flashes = collected
        .flashes
        .iter()
        .filter(|flash| point_in_bounds(flash.lon, flash.lat, request.bounds))
        .cloned()
        .collect::<Vec<_>>();
    let time_window = GlmLightningTimeWindow {
        first: collected.first,
        last: collected.last,
    };

    let iso_leaf = time_window
        .last
        .unwrap_or_else(Utc::now)
        .format("%Y%m%dT%H%MZ")
        .to_string();
    let raw_dir = request
        .out_dir
        .join("lightning")
        .join(sanitize_path_component(&request.domain_slug))
        .join(iso_leaf)
        .join("raw");
    fs::create_dir_all(&raw_dir)?;
    let png_path = raw_dir.join("glm_flashes.png");
    let data_json_path = raw_dir.join("glm_flashes.json");

    let render_request_start = Instant::now();
    let render_request = build_glm_render_request(request, &domain_flashes, time_window.last)?;
    let render_request_ms = render_request_start.elapsed().as_millis();

    let render_start = Instant::now();
    save_png_profile_with_options(&render_request, &png_path, &request.png_write_options())?;
    let render_ms = render_start.elapsed().as_millis();

    let write_json_start = Instant::now();
    let artifact = GlmLightningDataArtifact {
        domain: request.domain_slug.clone(),
        domain_label: request.domain_label.clone(),
        bounds: request.bounds,
        time_window: time_window.clone(),
        flash_count_total: collected.flashes.len(),
        flash_count_in_domain: domain_flashes.len(),
        flashes: domain_flashes.clone(),
    };
    fs::write(&data_json_path, serde_json::to_vec_pretty(&artifact)?)?;
    let write_json_ms = write_json_start.elapsed().as_millis();

    Ok(GlmLightningRenderReport {
        ok: true,
        domain: request.domain_slug.clone(),
        domain_label: request.domain_label.clone(),
        bounds: request.bounds,
        png_path,
        data_json_path,
        flash_count_total: collected.flashes.len(),
        flash_count_in_domain: domain_flashes.len(),
        flash_count_drawn: domain_flashes.len(),
        n_files: collected.n_files,
        time_window,
        data_dir: request.data_dir.clone(),
        timing: GlmLightningTiming {
            ingest_ms,
            render_request_ms,
            render_ms,
            write_json_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    })
}

fn build_glm_render_request(
    request: &GlmLightningRenderRequest,
    flashes: &[GlmFlash],
    reference_time: Option<DateTime<Utc>>,
) -> Result<MapRenderRequest, Box<dyn Error>> {
    let bounds = normalized_bounds(request.bounds)?;
    let (lat, lon) = lat_lon_mesh(bounds, MESH_NX, MESH_NY);
    let target_ratio = rustwx_render::map_frame_aspect_ratio_for_mode(
        ProductVisualMode::OverlayAnalysis,
        request.width,
        request.height,
        false,
        true,
    );
    let center_lon = bounds_center_lon(bounds);
    let center_lat = (bounds.2 + bounds.3) * 0.5;
    let projection = ProjectionSpec::LambertConformal {
        standard_parallel_1_deg: 30.0,
        standard_parallel_2_deg: 60.0,
        central_meridian_deg: center_lon,
    };
    let mut map_options = ProjectedMapBuildOptions::from_bounds(bounds, target_ratio)
        .with_projection(projection.clone());
    map_options.domain.reference_latitude_deg = Some(center_lat);
    map_options.domain.pad_fraction = 0.02;
    let projected = build_projected_map_with_options(&lat, &lon, &map_options)?;
    let projector = LambertConformal::new(30.0, 60.0, center_lon, center_lat);

    let grid = LatLonGrid::new(GridShape::new(MESH_NX, MESH_NY)?, lat, lon)?;
    let field = Field2D::new(
        ProductKey::named("glm_lightning_flashes"),
        "flashes",
        grid,
        vec![0.0; MESH_NX * MESH_NY],
    )?;
    let mut render_request = MapRenderRequest::new(field, transparent_scale());
    render_request.width = request.width;
    render_request.height = request.height;
    render_request.colorbar = false;
    render_request.visual_mode = ProductVisualMode::OverlayAnalysis;
    render_request.domain_frame = None;
    render_request.supersample_factor = 2;
    render_request.render_density = RenderDensity {
        fill: LevelDensity::default(),
        palette_multiplier: 1,
    };
    render_request.legend = LegendControls {
        density: LevelDensity::default(),
        mode: LegendMode::SmoothRamp,
    };
    render_request.chrome_scale = ChromeScale::Auto {
        base_width: 1500,
        base_height: 1300,
        min: 1.0,
        max: 2.4,
    };
    render_request.title = Some("GOES GLM Lightning".to_string());
    render_request.subtitle_left = Some(format!(
        "{} | {}",
        request.domain_label,
        format_time_window(reference_time, flashes)
    ));
    render_request.subtitle_right = Some(format!(
        "{} flashes | recency <= {:.0} min",
        flashes.len(),
        request.max_age_min
    ));
    render_request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x,
        y: projected.projected_y,
        extent: projected.extent,
    });
    render_request.projected_lines = projected.lines;
    render_request.projected_polygons = projected.polygons;
    render_request.projected_points = lightning_point_overlays(
        flashes,
        reference_time.unwrap_or_else(|| newest_flash_time(flashes).unwrap_or_else(Utc::now)),
        request.max_age_min,
        projector,
    );
    Ok(render_request)
}

fn collect_glm_flashes(data_dir: &Path) -> Result<CollectedGlmFlashes, Box<dyn Error>> {
    let mut files = fs::read_dir(data_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("nc"))
        })
        .collect::<Vec<_>>();
    files.sort();
    if files.is_empty() {
        return Err(boxed_error(format!(
            "no .nc GLM files found in {}",
            data_dir.display()
        )));
    }

    let mut flashes = Vec::new();
    let mut first = None;
    let mut last = None;
    let mut n_files = 0usize;
    for path in files {
        let file_flashes = read_glm_file(&path)?;
        if file_flashes.is_empty() {
            continue;
        }
        n_files += 1;
        for flash in file_flashes {
            first = Some(first.map_or(flash.time_utc, |current: DateTime<Utc>| {
                current.min(flash.time_utc)
            }));
            last = Some(last.map_or(flash.time_utc, |current: DateTime<Utc>| {
                current.max(flash.time_utc)
            }));
            flashes.push(flash);
        }
    }

    if flashes.is_empty() {
        return Err(boxed_error(format!(
            "no GLM flashes parsed from {}",
            data_dir.display()
        )));
    }

    Ok(CollectedGlmFlashes {
        flashes,
        n_files,
        first,
        last,
    })
}

fn read_glm_file(path: &Path) -> Result<Vec<GlmFlash>, Box<dyn Error>> {
    let options = netcrust::NcOpenOptions {
        metadata_mode: netcrust::NcMetadataMode::Lossy,
        ..Default::default()
    };
    let file = netcrust::File::open_with_options(path, options)?;
    let lat = read_scaled_f64(&file, "flash_lat")?;
    let lon = read_scaled_f64(&file, "flash_lon")?;
    let time_offset = read_scaled_f64(&file, "flash_frame_time_offset_of_first_event")?;
    let n = lat.len().min(lon.len()).min(time_offset.len());
    if n == 0 {
        return Ok(Vec::new());
    }

    let energy = read_optional_f64(&file, "flash_energy", n)?;
    let area = read_optional_f64(&file, "flash_area", n)?;
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let file_start = parse_goes_filename_time(&filename, 's')
        .or_else(|| parse_time_coverage_attr(&file, "time_coverage_start"))
        .ok_or_else(|| {
            boxed_error(format!(
                "could not resolve GLM file start time from {}",
                path.display()
            ))
        })?;

    let mut flashes = Vec::with_capacity(n);
    for idx in 0..n {
        let lat_value = lat[idx];
        let lon_value = lon[idx];
        if !lat_value.is_finite() || !lon_value.is_finite() {
            continue;
        }
        flashes.push(GlmFlash {
            lat: lat_value,
            lon: normalize_longitude_deg(lon_value),
            energy_j: finite_option(energy[idx]),
            area_m2: finite_option(area[idx]),
            time_utc: add_seconds(file_start, time_offset[idx]),
            source_file: filename.clone(),
        });
    }
    Ok(flashes)
}

fn read_optional_f64(
    file: &netcrust::File,
    name: &str,
    expected_len: usize,
) -> Result<Vec<f64>, Box<dyn Error>> {
    match read_scaled_f64(file, name) {
        Ok(values) => {
            if values.len() >= expected_len {
                Ok(values)
            } else {
                let mut padded = values;
                padded.resize(expected_len, f64::NAN);
                Ok(padded)
            }
        }
        Err(err) if err.to_string().contains("variable not found") => {
            Ok(vec![f64::NAN; expected_len])
        }
        Err(err) => Err(err),
    }
}

fn read_scaled_f64(file: &netcrust::File, name: &str) -> Result<Vec<f64>, Box<dyn Error>> {
    let variable = file
        .variable(name)
        .ok_or_else(|| boxed_error(format!("variable not found: {name}")))?;
    let scale = variable
        .attribute("scale_factor")
        .and_then(|attr| attr.as_f64())
        .unwrap_or(1.0);
    let offset = variable
        .attribute("add_offset")
        .and_then(|attr| attr.as_f64())
        .unwrap_or(0.0);
    let fill = variable
        .attribute("_FillValue")
        .and_then(|attr| attr.as_f64());
    let values = variable.values_f64()?;
    Ok(values
        .into_iter()
        .map(|value| {
            if fill.is_some_and(|fill| (value - fill).abs() < 0.5) {
                f64::NAN
            } else {
                value * scale + offset
            }
        })
        .collect())
}

fn lightning_point_overlays(
    flashes: &[GlmFlash],
    reference_time: DateTime<Utc>,
    max_age_min: f64,
    projector: LambertConformal,
) -> Vec<ProjectedPointOverlay> {
    let max_age_min = max_age_min.max(1.0);
    let mut ordered = flashes.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        let left_age = age_minutes(left.time_utc, reference_time);
        let right_age = age_minutes(right.time_utc, reference_time);
        right_age
            .partial_cmp(&left_age)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    ordered
        .into_iter()
        .map(|flash| {
            let (x, y) = projector.project(flash.lat, flash.lon);
            ProjectedPointOverlay {
                x,
                y,
                color: lightning_recency_color(
                    age_minutes(flash.time_utc, reference_time),
                    max_age_min,
                ),
                radius_px: lightning_radius_px(flash.area_m2),
                width_px: 2,
                shape: ProjectedMarkerShape::Plus,
            }
        })
        .collect()
}

fn lightning_recency_color(age_min: f64, max_age_min: f64) -> Color {
    const ANCHORS: &[(f64, Color)] = &[
        (0.0, Color::rgba(255, 255, 255, 255)),
        (0.25, Color::rgba(255, 230, 128, 255)),
        (0.50, Color::rgba(255, 160, 48, 255)),
        (0.75, Color::rgba(255, 58, 31, 255)),
        (1.0, Color::rgba(163, 8, 21, 255)),
    ];
    let t = (age_min / max_age_min).clamp(0.0, 1.0);
    for pair in ANCHORS.windows(2) {
        let (t0, c0) = pair[0];
        let (t1, c1) = pair[1];
        if t <= t1 {
            let local = if (t1 - t0).abs() < 1e-9 {
                0.0
            } else {
                (t - t0) / (t1 - t0)
            };
            return lerp_color(c0, c1, local);
        }
    }
    ANCHORS
        .last()
        .map(|(_, color)| *color)
        .unwrap_or(Color::WHITE)
}

fn lightning_radius_px(area_m2: Option<f64>) -> u32 {
    let area = area_m2.unwrap_or(1.0).max(1.0);
    let scaled = ((area.log10() - 6.0) / 3.0).clamp(0.0, 1.0);
    (4.0 + scaled * 8.0).round() as u32
}

fn lerp_color(left: Color, right: Color, t: f64) -> Color {
    let t = t.clamp(0.0, 1.0);
    Color::rgba(
        lerp_u8(left.r, right.r, t),
        lerp_u8(left.g, right.g, t),
        lerp_u8(left.b, right.b, t),
        lerp_u8(left.a, right.a, t),
    )
}

fn lerp_u8(left: u8, right: u8, t: f64) -> u8 {
    (left as f64 + (right as f64 - left as f64) * t).round() as u8
}

fn transparent_scale() -> ColorScale {
    ColorScale::Discrete(DiscreteColorScale {
        levels: vec![0.0, 1.0],
        colors: vec![Color::TRANSPARENT],
        extend: ExtendMode::Neither,
        mask_below: Some(0.5),
    })
}

fn lat_lon_mesh(bounds: (f64, f64, f64, f64), nx: usize, ny: usize) -> (Vec<f32>, Vec<f32>) {
    let (west, _east, south, north) = bounds;
    let west360 = normalize_longitude_360(west);
    let span = longitude_span_deg(bounds);
    let mut lat = Vec::with_capacity(nx * ny);
    let mut lon = Vec::with_capacity(nx * ny);
    for y in 0..ny {
        let fy = if ny <= 1 {
            0.0
        } else {
            y as f64 / (ny - 1) as f64
        };
        let lat_value = south + (north - south) * fy;
        for x in 0..nx {
            let fx = if nx <= 1 {
                0.0
            } else {
                x as f64 / (nx - 1) as f64
            };
            let lon_value = normalize_longitude_deg(west360 + span * fx);
            lat.push(lat_value as f32);
            lon.push(lon_value as f32);
        }
    }
    (lat, lon)
}

fn normalized_bounds(bounds: (f64, f64, f64, f64)) -> Result<(f64, f64, f64, f64), Box<dyn Error>> {
    let (west, east, south, north) = bounds;
    if !west.is_finite()
        || !east.is_finite()
        || !south.is_finite()
        || !north.is_finite()
        || !(-90.0..=90.0).contains(&south)
        || !(-90.0..=90.0).contains(&north)
        || south >= north
    {
        return Err(boxed_error(
            "bounds must be finite [west,east,south,north] values with south < north and valid latitudes",
        ));
    }
    Ok((
        normalize_longitude_deg(west),
        normalize_longitude_deg(east),
        south,
        north,
    ))
}

fn point_in_bounds(lon: f64, lat: f64, bounds: (f64, f64, f64, f64)) -> bool {
    if !lon.is_finite() || !lat.is_finite() {
        return false;
    }
    let Ok(bounds) = normalized_bounds(bounds) else {
        return false;
    };
    let (_, _, south, north) = bounds;
    if lat < south || lat > north {
        return false;
    }
    let span = longitude_span_deg(bounds);
    if span >= 359.0 {
        return true;
    }
    let west = normalize_longitude_360(bounds.0);
    let east = normalize_longitude_360(bounds.1);
    let lon = normalize_longitude_360(lon);
    if west <= east {
        lon >= west && lon <= east
    } else {
        lon >= west || lon <= east
    }
}

fn bounds_center_lon(bounds: (f64, f64, f64, f64)) -> f64 {
    if longitude_span_deg(bounds) >= 359.0 {
        return 0.0;
    }
    normalize_longitude_deg(normalize_longitude_360(bounds.0) + longitude_span_deg(bounds) * 0.5)
}

fn longitude_span_deg(bounds: (f64, f64, f64, f64)) -> f64 {
    let west = normalize_longitude_360(bounds.0);
    let east = normalize_longitude_360(bounds.1);
    if west <= east {
        east - west
    } else {
        east + 360.0 - west
    }
}

fn normalize_longitude_360(lon: f64) -> f64 {
    lon.rem_euclid(360.0)
}

fn normalize_longitude_deg(lon: f64) -> f64 {
    let mut value = (lon + 180.0).rem_euclid(360.0) - 180.0;
    if value == -180.0 {
        value = 180.0;
    }
    value
}

fn parse_goes_filename_time(filename: &str, marker: char) -> Option<DateTime<Utc>> {
    let token = filename
        .split('_')
        .find(|part| part.starts_with(marker) && part.len() >= 14)?;
    parse_goes_timestamp_token(&token[1..])
}

fn parse_goes_timestamp_token(token: &str) -> Option<DateTime<Utc>> {
    let stamp = token.get(..13)?;
    let year = stamp.get(0..4)?.parse::<i32>().ok()?;
    let doy = stamp.get(4..7)?.parse::<u32>().ok()?;
    let hour = stamp.get(7..9)?.parse::<u32>().ok()?;
    let minute = stamp.get(9..11)?.parse::<u32>().ok()?;
    let second = stamp.get(11..13)?.parse::<u32>().ok()?;
    let date = NaiveDate::from_yo_opt(year, doy)?;
    let dt = date.and_hms_opt(hour, minute, second)?;
    Some(Utc.from_utc_datetime(&dt))
}

fn parse_time_coverage_attr(file: &netcrust::File, attr_name: &str) -> Option<DateTime<Utc>> {
    let attr = file.attribute(attr_name)?;
    let raw = attr.as_string()?.trim();
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn add_seconds(base: DateTime<Utc>, offset_seconds: f64) -> DateTime<Utc> {
    let nanos = (offset_seconds * 1_000_000_000.0).round();
    if !nanos.is_finite() {
        return base;
    }
    base + Duration::nanoseconds(nanos.clamp(i64::MIN as f64, i64::MAX as f64) as i64)
}

fn newest_flash_time(flashes: &[GlmFlash]) -> Option<DateTime<Utc>> {
    flashes.iter().map(|flash| flash.time_utc).max()
}

fn age_minutes(time: DateTime<Utc>, reference_time: DateTime<Utc>) -> f64 {
    reference_time
        .signed_duration_since(time)
        .num_milliseconds()
        .max(0) as f64
        / 60_000.0
}

fn format_time_window(reference_time: Option<DateTime<Utc>>, flashes: &[GlmFlash]) -> String {
    let Some(last) = reference_time.or_else(|| newest_flash_time(flashes)) else {
        return "no flashes in domain".to_string();
    };
    let first = flashes
        .iter()
        .map(|flash| flash.time_utc)
        .min()
        .unwrap_or(last);
    format!("{}-{}Z", first.format("%H:%M"), last.format("%H:%M"))
}

fn finite_option(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn sanitize_path_component(value: &str) -> String {
    let slug = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    slug.trim_matches('_').to_string()
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

fn default_width() -> u32 {
    1500
}

fn default_height() -> u32 {
    1300
}

fn default_max_age_min() -> f64 {
    30.0
}

fn default_png_compression() -> PngCompressionMode {
    PngCompressionMode::Default
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_goes_filename_timestamp() {
        let parsed = parse_goes_filename_time(
            "OR_GLM-L2-LCFA_G18_s20261170234200_e20261170234400_c20261170234415.nc",
            's',
        )
        .expect("timestamp should parse");
        assert_eq!(parsed.to_rfc3339(), "2026-04-27T02:34:20+00:00");
    }

    #[test]
    fn point_in_bounds_supports_wrapped_longitude_ranges() {
        let fiji_like = (170.0, -175.0, -25.0, -10.0);
        assert!(point_in_bounds(178.0, -18.0, fiji_like));
        assert!(point_in_bounds(-178.0, -18.0, fiji_like));
        assert!(!point_in_bounds(-120.0, -18.0, fiji_like));
    }

    #[test]
    fn lightning_recency_color_moves_from_white_to_red() {
        let recent = lightning_recency_color(0.0, 30.0);
        let old = lightning_recency_color(30.0, 30.0);
        assert!(recent.r >= old.r);
        assert!(recent.g > old.g);
        assert!(old.r > old.g);
    }

    #[test]
    fn mesh_uses_requested_shape_and_bounds() {
        let (lat, lon) = lat_lon_mesh((-125.0, -114.0, 32.0, 43.0), 4, 3);
        assert_eq!(lat.len(), 12);
        assert_eq!(lon.len(), 12);
        assert!((lat[0] - 32.0).abs() < 0.01);
        assert!((lat[8] - 43.0).abs() < 0.01);
        assert!((lon[0] + 125.0).abs() < 0.01);
        assert!((lon[3] + 114.0).abs() < 0.01);
    }

    #[test]
    #[ignore]
    fn renders_default_glm_dir_when_available() {
        let data_dir = default_glm_data_dir();
        if !data_dir.exists() {
            eprintln!(
                "skipping local GLM smoke test; missing {}",
                data_dir.display()
            );
            return;
        }
        let request = GlmLightningRenderRequest::new(
            data_dir,
            "california",
            "California",
            (-125.0, -114.0, 32.0, 43.0),
            "target/glm-lightning-smoke",
        );
        let report = render_glm_lightning_map(&request).expect("local GLM render should succeed");
        assert!(report.png_path.exists());
        assert!(report.data_json_path.exists());
        assert!(report.flash_count_total > 0);
    }
}
