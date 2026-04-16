use crate::cache::{load_bincode, store_bincode};
use crate::orchestrator::{PreparedRunContext, PreparedRunMetadata};
pub use crate::shared_context::{
    DomainSpec, PreparedProjectedContext, ProjectedMap, Solar07PanelField, Solar07PanelHeader,
    Solar07PanelLayout, layout_key, render_two_by_four_solar07_panel,
};
use grib_core::grib2::{
    Grib2File, Grib2Message, flip_rows, grid_latlon, unpack_message_normalized,
};
use rustwx_calc::{
    EcapeTripletOptions, EcapeVolumeInputs, GridShape as CalcGridShape, ScpEhiInputs,
    SupportedSevereFields, SurfaceInputs, VolumeShape, WindGridInputs,
    compute_ecape_triplet_with_failure_mask_from_parts, compute_scp_ehi,
    compute_supported_severe_fields, compute_wind_diagnostics_bundle,
};
use rustwx_core::{
    CycleSpec, GridShape, LatLonGrid, ModelId, ModelRunRequest, RustwxError, SourceId,
};
use rustwx_io::{CachedFetchResult, FetchRequest, artifact_cache_dir, fetch_bytes_with_cache};
use rustwx_models::{LatestRun, latest_available_run};
use rustwx_render::{Color, ProjectedExtent, ProjectedLineOverlay, Solar07Product};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;
use wrf_render::features::load_styled_conus_features;
use wrf_render::projection::LambertConformal;
use wrf_render::render::map_frame_aspect_ratio;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum HrrrBatchProduct {
    SevereProofPanel,
    Ecape8Panel,
}

impl HrrrBatchProduct {
    pub fn slug(self) -> &'static str {
        match self {
            Self::SevereProofPanel => "severe_proof_panel",
            Self::Ecape8Panel => "ecape8_panel",
        }
    }

    pub fn layout(self) -> Solar07PanelLayout {
        match self {
            Self::SevereProofPanel => Solar07PanelLayout {
                top_padding: 86,
                ..Default::default()
            },
            Self::Ecape8Panel => Solar07PanelLayout::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrBatchRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub products: Vec<HrrrBatchProduct>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrFetchRuntimeInfo {
    pub planned_product: String,
    pub fetched_product: String,
    pub requested_source: SourceId,
    pub resolved_source: SourceId,
    pub resolved_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrSharedTiming {
    pub fetch_surface_ms: u128,
    pub fetch_pressure_ms: u128,
    pub decode_surface_ms: u128,
    pub decode_pressure_ms: u128,
    pub fetch_surface_cache_hit: bool,
    pub fetch_pressure_cache_hit: bool,
    pub decode_surface_cache_hit: bool,
    pub decode_pressure_cache_hit: bool,
    pub surface_fetch: HrrrFetchRuntimeInfo,
    pub pressure_fetch: HrrrFetchRuntimeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrProductTiming {
    pub project_ms: u128,
    pub compute_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrRenderedProduct {
    pub product: HrrrBatchProduct,
    pub output_path: PathBuf,
    pub timing: HrrrProductTiming,
    pub metadata: HrrrProductMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HrrrProductMetadata {
    pub failure_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrBatchReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub products: Vec<HrrrRenderedProduct>,
    pub shared_timing: HrrrSharedTiming,
    pub total_ms: u128,
}

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

impl HrrrFetchedSubset {
    pub fn runtime_info(&self, planned_product: &str) -> HrrrFetchRuntimeInfo {
        HrrrFetchRuntimeInfo {
            planned_product: planned_product.to_string(),
            fetched_product: self.request.request.product.clone(),
            requested_source: self
                .request
                .source_override
                .unwrap_or(self.fetched.result.source),
            resolved_source: self.fetched.result.source,
            resolved_url: self.fetched.result.url.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedDecode<T> {
    pub value: T,
    pub cache_hit: bool,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
struct PreparedHrrrHeavyVolume {
    grid: CalcGridShape,
    shape: VolumeShape,
    pressure_levels_pa: Vec<f64>,
    pressure_3d_pa: Option<Vec<f64>>,
    height_agl_3d: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GridCrop {
    x_start: usize,
    x_end: usize,
    y_start: usize,
    y_end: usize,
}

impl GridCrop {
    fn width(self) -> usize {
        self.x_end - self.x_start
    }

    fn height(self) -> usize {
        self.y_end - self.y_start
    }
}

#[derive(Debug, Clone)]
struct CroppedHrrrHeavyDomain {
    surface: HrrrSurfaceFields,
    pressure: HrrrPressureFields,
    grid: LatLonGrid,
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
    fetch_hrrr_file(
        cycle,
        forecast_hour,
        source,
        product,
        patterns.iter().map(|s| s.to_string()).collect(),
        cache_root,
        use_cache,
    )
}

pub fn fetch_hrrr_family_file(
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    product: &str,
    cache_root: &Path,
    use_cache: bool,
) -> Result<HrrrFetchedSubset, Box<dyn std::error::Error>> {
    fetch_hrrr_file(
        cycle,
        forecast_hour,
        source,
        product,
        Vec::new(),
        cache_root,
        use_cache,
    )
}

fn fetch_hrrr_file(
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    product: &str,
    variable_patterns: Vec<String>,
    cache_root: &Path,
    use_cache: bool,
) -> Result<HrrrFetchedSubset, Box<dyn std::error::Error>> {
    let fetch_request =
        hrrr_fetch_request(cycle, forecast_hour, source, product, variable_patterns)?;
    let fetched = fetch_bytes_with_cache(&fetch_request, cache_root, use_cache)?;
    let bytes = fetched.result.bytes.clone();
    Ok(HrrrFetchedSubset {
        request: fetch_request,
        fetched,
        bytes,
    })
}

fn hrrr_fetch_request(
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    product: &str,
    variable_patterns: Vec<String>,
) -> Result<FetchRequest, RustwxError> {
    Ok(FetchRequest {
        request: ModelRunRequest::new(ModelId::Hrrr, cycle, forecast_hour, product)?,
        source_override: Some(source),
        variable_patterns,
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
    if let Some(cached) = try_load_cached_decode::<HrrrSurfaceFields>(path, use_cache)? {
        return Ok(CachedDecode {
            value: cached,
            cache_hit: true,
            path: path.to_path_buf(),
        });
    }
    let decoded = decode_surface(bytes)?;
    store_decoded_value(path, &decoded, use_cache)?;
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
    let (decoded, decoded_shape) = load_or_decode_pressure_with_shape(path, bytes, use_cache)?;
    validate_pressure_decode_against_surface(&decoded, decoded_shape, nx, ny)?;
    Ok(decoded)
}

fn load_or_decode_pressure_with_shape(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<(CachedDecode<HrrrPressureFields>, Option<(usize, usize)>), Box<dyn std::error::Error>>
{
    if let Some(cached) = try_load_cached_decode::<HrrrPressureFields>(path, use_cache)? {
        return Ok((
            CachedDecode {
                value: cached,
                cache_hit: true,
                path: path.to_path_buf(),
            },
            None,
        ));
    }
    decode_pressure_cache_miss_with_shape(path, bytes, use_cache)
}

fn decode_pressure_cache_miss_with_shape(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<(CachedDecode<HrrrPressureFields>, Option<(usize, usize)>), Box<dyn std::error::Error>>
{
    let (decoded, nx, ny) = decode_pressure_with_shape(bytes)?;
    store_decoded_value(path, &decoded, use_cache)?;
    Ok((
        CachedDecode {
            value: decoded,
            cache_hit: false,
            path: path.to_path_buf(),
        },
        Some((nx, ny)),
    ))
}

fn try_load_cached_decode<T>(
    path: &Path,
    use_cache: bool,
) -> Result<Option<T>, Box<dyn std::error::Error>>
where
    T: DeserializeOwned,
{
    if use_cache {
        load_bincode::<T>(path)
    } else {
        Ok(None)
    }
}

fn store_decoded_value<T>(
    path: &Path,
    value: &T,
    use_cache: bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: Serialize,
{
    if use_cache {
        store_bincode(path, value)?;
    }
    Ok(())
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
    let (decoded, found_nx, found_ny) = decode_pressure_with_shape(bytes)?;
    validate_pressure_shape(found_nx, found_ny, nx, ny)?;
    Ok(decoded)
}

fn decode_pressure_with_shape(
    bytes: &[u8],
) -> Result<(HrrrPressureFields, usize, usize), Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let (nx, ny) = pressure_grid_shape_from_messages(&file.messages)?;
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

    Ok((
        HrrrPressureFields {
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
        },
        nx,
        ny,
    ))
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

#[derive(Debug)]
pub struct LoadedHrrrTimestep {
    pub(crate) latest: LatestRun,
    pub(crate) surface_subset: HrrrFetchedSubset,
    pub(crate) pressure_subset: HrrrFetchedSubset,
    pub(crate) surface_decode: CachedDecode<HrrrSurfaceFields>,
    pub(crate) pressure_decode: CachedDecode<HrrrPressureFields>,
    pub(crate) grid: LatLonGrid,
    pub(crate) shared_timing: HrrrSharedTiming,
}

#[derive(Debug)]
pub(crate) struct PreparedHrrrHourContext {
    pub(crate) timestep: LoadedHrrrTimestep,
    projected: PreparedProjectedContext,
}

impl PreparedHrrrHourContext {
    pub(crate) fn projected_map(&self, width: u32, height: u32) -> Option<&ProjectedMap> {
        self.projected.projected_map(width, height)
    }

    pub fn timestep(&self) -> &LoadedHrrrTimestep {
        &self.timestep
    }
}

impl LoadedHrrrTimestep {
    pub fn latest(&self) -> &LatestRun {
        &self.latest
    }

    pub fn surface_subset(&self) -> &HrrrFetchedSubset {
        &self.surface_subset
    }

    pub fn pressure_subset(&self) -> &HrrrFetchedSubset {
        &self.pressure_subset
    }

    pub fn surface_decode(&self) -> &CachedDecode<HrrrSurfaceFields> {
        &self.surface_decode
    }

    pub fn pressure_decode(&self) -> &CachedDecode<HrrrPressureFields> {
        &self.pressure_decode
    }

    pub fn grid(&self) -> &LatLonGrid {
        &self.grid
    }

    pub fn shared_timing(&self) -> &HrrrSharedTiming {
        &self.shared_timing
    }
}

pub fn run_hrrr_batch(
    request: &HrrrBatchRequest,
) -> Result<HrrrBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let projection_sizes = required_batch_projection_sizes(&request.products);
    let context = prepare_hrrr_hour_context(
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
        request.domain.bounds,
        &projection_sizes,
        &request.cache_root,
        request.use_cache,
    )?;
    let prepared = PreparedRunContext::new(
        PreparedRunMetadata::from_latest(context.timestep().latest(), request.forecast_hour),
        context,
    );
    run_hrrr_batch_with_context(request, &prepared)
}

pub(crate) fn run_hrrr_batch_with_context(
    request: &HrrrBatchRequest,
    prepared: &PreparedRunContext<PreparedHrrrHourContext>,
) -> Result<HrrrBatchReport, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let context = prepared.context();
    let metadata = prepared.metadata();
    let timestep = context.timestep();
    let cropped_heavy_domain = crop_hrrr_heavy_domain(
        &timestep.surface_decode.value,
        &timestep.pressure_decode.value,
        request.domain.bounds,
    )?;
    let (surface, pressure, grid) = match cropped_heavy_domain.as_ref() {
        Some(cropped) => (&cropped.surface, &cropped.pressure, &cropped.grid),
        None => (
            &timestep.surface_decode.value,
            &timestep.pressure_decode.value,
            &timestep.grid,
        ),
    };
    let unique_products = dedupe_products(&request.products);
    let render_parallelism = self::png_render_parallelism(unique_products.len());
    let mut projected_maps = HashMap::<(u32, u32, u32), ProjectedMap>::new();
    let mut project_timings = Vec::with_capacity(unique_products.len());
    let can_reuse_shared_projection = cropped_heavy_domain.is_none();

    for product in &unique_products {
        let layout = product.layout();
        let key = self::layout_key(layout);
        let project_start = Instant::now();
        if !projected_maps.contains_key(&key) {
            let built = if can_reuse_shared_projection {
                context
                    .projected_map(layout.panel_width, layout.panel_height)
                    .cloned()
                    .unwrap_or(build_projected_map(
                        surface,
                        request.domain.bounds,
                        layout.target_aspect_ratio(),
                    )?)
            } else {
                build_projected_map(surface, request.domain.bounds, layout.target_aspect_ratio())?
            };
            projected_maps.insert(key, built);
        }
        project_timings.push(project_start.elapsed().as_millis());
    }

    let date_yyyymmdd = metadata.date_yyyymmdd.as_str();
    let cycle_utc = metadata.cycle_utc;
    let forecast_hour = metadata.forecast_hour;
    let domain_slug = request.domain.slug.as_str();
    let needs_heavy = unique_products.iter().any(|product| {
        matches!(
            product,
            HrrrBatchProduct::SevereProofPanel | HrrrBatchProduct::Ecape8Panel
        )
    });
    let needs_pressure_3d = unique_products
        .iter()
        .any(|product| matches!(product, HrrrBatchProduct::SevereProofPanel));
    let prepared_heavy_volume = if needs_heavy {
        Some(prepare_hrrr_heavy_volume(
            surface,
            pressure,
            needs_pressure_3d,
        )?)
    } else {
        None
    };
    let products = thread::scope(|scope| -> Result<Vec<HrrrRenderedProduct>, io::Error> {
        let mut products = Vec::with_capacity(unique_products.len());
        let mut pending = VecDeque::new();

        for (idx, product) in unique_products.iter().copied().enumerate() {
            let product_start = Instant::now();
            let project_ms = project_timings[idx];
            let layout = product.layout();

            let compute_start = Instant::now();
            let computed = compute_hrrr_batch_product(
                product,
                date_yyyymmdd,
                cycle_utc,
                forecast_hour,
                surface,
                pressure,
                prepared_heavy_volume.as_ref(),
            )
            .map_err(self::thread_render_error)?;
            let compute_ms = compute_start.elapsed().as_millis();

            let output_path = request.out_dir.join(format!(
                "rustwx_hrrr_{}_{}z_f{:02}_{}_{}.png",
                date_yyyymmdd,
                cycle_utc,
                forecast_hour,
                domain_slug,
                product.slug()
            ));
            let projected = projected_maps
                .get(&self::layout_key(layout))
                .ok_or_else(|| io::Error::other("missing projected map for HRRR batch render"))?;

            pending.push_back(
                scope.spawn(move || -> Result<HrrrRenderedProduct, io::Error> {
                    let render_start = Instant::now();
                    render_two_by_four_solar07_panel(
                        &output_path,
                        grid,
                        projected,
                        &computed.fields,
                        &computed.header,
                        layout,
                    )
                    .map_err(self::thread_render_error)?;
                    let render_ms = render_start.elapsed().as_millis();

                    Ok(HrrrRenderedProduct {
                        product,
                        output_path,
                        timing: HrrrProductTiming {
                            project_ms,
                            compute_ms,
                            render_ms,
                            total_ms: product_start.elapsed().as_millis(),
                        },
                        metadata: computed.metadata,
                    })
                }),
            );

            if pending.len() >= render_parallelism {
                products.push(self::join_render_job(pending.pop_front().unwrap())?);
            }
        }

        while let Some(handle) = pending.pop_front() {
            products.push(self::join_render_job(handle)?);
        }

        Ok(products)
    })
    .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    Ok(HrrrBatchReport {
        date_yyyymmdd: metadata.date_yyyymmdd.clone(),
        cycle_utc,
        forecast_hour,
        source: metadata.source,
        domain: request.domain.clone(),
        products,
        shared_timing: timestep.shared_timing.clone(),
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn required_batch_projection_sizes(products: &[HrrrBatchProduct]) -> Vec<(u32, u32)> {
    dedupe_products(products)
        .into_iter()
        .map(|product| {
            let layout = product.layout();
            (layout.panel_width, layout.panel_height)
        })
        .collect()
}

pub fn load_hrrr_timestep_from_parts(
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    cache_root: &Path,
    use_cache: bool,
) -> Result<LoadedHrrrTimestep, Box<dyn std::error::Error>> {
    let latest = resolve_hrrr_run(date_yyyymmdd, cycle_override_utc, source)?;
    load_hrrr_timestep_from_latest(latest, forecast_hour, cache_root, use_cache)
}

pub fn load_hrrr_timestep_from_latest(
    latest: LatestRun,
    forecast_hour: u16,
    cache_root: &Path,
    use_cache: bool,
) -> Result<LoadedHrrrTimestep, Box<dyn std::error::Error>> {
    let ((surface_subset, fetch_surface_ms), (pressure_subset, fetch_pressure_ms)) =
        thread::scope(|scope| -> Result<_, io::Error> {
            let surface_cycle = latest.cycle.clone();
            let pressure_cycle = latest.cycle.clone();
            let source = latest.source;
            let surface_handle = scope.spawn(move || -> Result<_, io::Error> {
                let fetch_surface_start = Instant::now();
                let surface_subset = fetch_hrrr_family_file(
                    surface_cycle,
                    forecast_hour,
                    source,
                    "sfc",
                    cache_root,
                    use_cache,
                )
                .map_err(thread_render_error)?;
                Ok((surface_subset, fetch_surface_start.elapsed().as_millis()))
            });
            let pressure_handle = scope.spawn(move || -> Result<_, io::Error> {
                let fetch_pressure_start = Instant::now();
                let pressure_subset = fetch_hrrr_family_file(
                    pressure_cycle,
                    forecast_hour,
                    source,
                    "prs",
                    cache_root,
                    use_cache,
                )
                .map_err(thread_render_error)?;
                Ok((pressure_subset, fetch_pressure_start.elapsed().as_millis()))
            });
            let surface = join_scoped_job(surface_handle)?;
            let pressure = join_scoped_job(pressure_handle)?;
            Ok((surface, pressure))
        })
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    let surface_cache_path = decode_cache_path(cache_root, &surface_subset.request, "surface");
    let pressure_cache_path = decode_cache_path(cache_root, &pressure_subset.request, "pressure");
    let surface_bytes = surface_subset.bytes.as_slice();
    let pressure_bytes = pressure_subset.bytes.as_slice();
    let (
        (surface_decode, decode_surface_ms),
        (pressure_decode, pressure_shape, decode_pressure_ms),
    ) = thread::scope(|scope| -> Result<_, io::Error> {
        let surface_handle = scope.spawn(move || -> Result<_, io::Error> {
            let decode_surface_start = Instant::now();
            let surface_decode =
                load_or_decode_surface(&surface_cache_path, surface_bytes, use_cache)
                    .map_err(thread_render_error)?;
            Ok((surface_decode, decode_surface_start.elapsed().as_millis()))
        });
        let pressure_handle = scope.spawn(move || -> Result<_, io::Error> {
            let decode_pressure_start = Instant::now();
            let (pressure_decode, shape) =
                load_or_decode_pressure_with_shape(&pressure_cache_path, pressure_bytes, use_cache)
                    .map_err(thread_render_error)?;
            Ok((
                pressure_decode,
                shape,
                decode_pressure_start.elapsed().as_millis(),
            ))
        });
        let surface = join_scoped_job(surface_handle)?;
        let pressure = join_scoped_job(pressure_handle)?;
        Ok((surface, pressure))
    })
    .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
    validate_pressure_decode_against_surface(
        &pressure_decode,
        pressure_shape,
        surface_decode.value.nx,
        surface_decode.value.ny,
    )?;
    let grid = surface_decode.value.core_grid()?;
    let surface_fetch = surface_subset.runtime_info("sfc");
    let pressure_fetch = pressure_subset.runtime_info("prs");

    Ok(LoadedHrrrTimestep {
        latest,
        surface_subset,
        pressure_subset,
        surface_decode,
        pressure_decode,
        grid,
        shared_timing: HrrrSharedTiming {
            fetch_surface_ms,
            fetch_pressure_ms,
            decode_surface_ms,
            decode_pressure_ms,
            fetch_surface_cache_hit: false,
            fetch_pressure_cache_hit: false,
            decode_surface_cache_hit: false,
            decode_pressure_cache_hit: false,
            surface_fetch,
            pressure_fetch,
        },
    }
    .with_cache_flags())
}

pub(crate) fn build_projected_maps_for_sizes(
    surface: &HrrrSurfaceFields,
    bounds: (f64, f64, f64, f64),
    sizes: &[(u32, u32)],
) -> Result<PreparedProjectedContext, Box<dyn std::error::Error>> {
    let mut maps = PreparedProjectedContext::new();
    for &(width, height) in sizes {
        if width == 0 || height == 0 || maps.contains_size(width, height) {
            continue;
        }
        let projected = build_projected_map(
            surface,
            bounds,
            map_frame_aspect_ratio(width, height, true, true),
        )?;
        maps.insert(width, height, projected);
    }
    Ok(maps)
}

pub(crate) fn prepare_hrrr_hour_context(
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    bounds: (f64, f64, f64, f64),
    projection_sizes: &[(u32, u32)],
    cache_root: &Path,
    use_cache: bool,
) -> Result<PreparedHrrrHourContext, Box<dyn std::error::Error>> {
    let latest = resolve_hrrr_run(date_yyyymmdd, cycle_override_utc, source)?;
    let timestep = load_hrrr_timestep_from_latest(latest, forecast_hour, cache_root, use_cache)?;
    let projected =
        build_projected_maps_for_sizes(&timestep.surface_decode.value, bounds, projection_sizes)?;
    Ok(PreparedHrrrHourContext {
        timestep,
        projected,
    })
}

impl LoadedHrrrTimestep {
    fn with_cache_flags(mut self) -> Self {
        self.shared_timing.fetch_surface_cache_hit = self.surface_subset.fetched.cache_hit;
        self.shared_timing.fetch_pressure_cache_hit = self.pressure_subset.fetched.cache_hit;
        self.shared_timing.decode_surface_cache_hit = self.surface_decode.cache_hit;
        self.shared_timing.decode_pressure_cache_hit = self.pressure_decode.cache_hit;
        self
    }
}

#[derive(Debug)]
struct ComputedHrrrProduct {
    fields: Vec<Solar07PanelField>,
    header: Solar07PanelHeader,
    metadata: HrrrProductMetadata,
}

fn compute_hrrr_batch_product(
    product: HrrrBatchProduct,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
    prepared_heavy_volume: Option<&PreparedHrrrHeavyVolume>,
) -> Result<ComputedHrrrProduct, Box<dyn std::error::Error>> {
    match product {
        HrrrBatchProduct::SevereProofPanel => {
            let fields = match prepared_heavy_volume {
                Some(prepared) => {
                    compute_severe_panel_fields_with_prepared_volume(surface, pressure, prepared)?
                }
                None => compute_severe_panel_fields(surface, pressure)?,
            };
            let header = Solar07PanelHeader::new(format!(
                "HRRR Severe Proof Panel  Run: {} {:02}:00 UTC  Forecast Hour: F{:02}",
                date_yyyymmdd, cycle_utc, forecast_hour
            ))
            .with_subtitle_line(
                "STP is fixed-layer only: sbCAPE + sbLCL + 0-1 km SRH + 0-6 km bulk shear.",
            )
            .with_subtitle_line(
                "SCP stays a fixed-depth proxy here: muCAPE + 0-3 km SRH + 0-6 km shear. EHI 0-1 km uses sbCAPE + 0-1 km SRH. Effective-layer derivation is not wired yet.",
            );
            Ok(ComputedHrrrProduct {
                fields,
                header,
                metadata: HrrrProductMetadata::default(),
            })
        }
        HrrrBatchProduct::Ecape8Panel => {
            let (fields, failure_count) = match prepared_heavy_volume {
                Some(prepared) => {
                    compute_ecape8_panel_fields_with_prepared_volume(surface, pressure, prepared)?
                }
                None => compute_ecape8_panel_fields(surface, pressure)?,
            };
            let header = Solar07PanelHeader::new(format!(
                "HRRR ECAPE Product Panel  Run: {} {:02}:00 UTC  Forecast Hour: F{:02}  zero-fill columns: {}",
                date_yyyymmdd, cycle_utc, forecast_hour, failure_count
            ))
            .with_subtitle_line(
                "Parcel-specific ECAPE shown for SB, ML, and MU. Single NCAPE context plus SBECIN and MLECIN. Experimental SCP/EHI shown.",
            );
            Ok(ComputedHrrrProduct {
                fields,
                header,
                metadata: HrrrProductMetadata {
                    failure_count: Some(failure_count),
                },
            })
        }
    }
}

fn dedupe_products(products: &[HrrrBatchProduct]) -> Vec<HrrrBatchProduct> {
    let mut seen = std::collections::HashSet::new();
    let mut unique = Vec::new();
    for product in products {
        if seen.insert(*product) {
            unique.push(*product);
        }
    }
    unique
}

pub fn severe_panel_fields_from_supported(fields: SupportedSevereFields) -> Vec<Solar07PanelField> {
    vec![
        Solar07PanelField::new(Solar07Product::Sbcape, "J/kg", fields.sbcape_jkg),
        Solar07PanelField::new(Solar07Product::Mlcin, "J/kg", fields.mlcin_jkg),
        Solar07PanelField::new(Solar07Product::Mucape, "J/kg", fields.mucape_jkg),
        Solar07PanelField::new(Solar07Product::Srh01km, "m^2/s^2", fields.srh_01km_m2s2),
        Solar07PanelField::new(Solar07Product::Srh03km, "m^2/s^2", fields.srh_03km_m2s2),
        Solar07PanelField::new(Solar07Product::StpFixed, "dimensionless", fields.stp_fixed),
        Solar07PanelField::new(
            Solar07Product::Scp,
            "dimensionless",
            fields.scp_mu_03km_06km_proxy,
        )
        .with_title_override("SCP (MU / 0-3 KM / 0-6 KM PROXY)"),
        Solar07PanelField::new(
            Solar07Product::Ehi,
            "dimensionless",
            fields.ehi_sb_01km_proxy,
        )
        .with_title_override("EHI 0-1 KM"),
    ]
}

pub fn compute_severe_panel_fields(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
) -> Result<Vec<Solar07PanelField>, Box<dyn std::error::Error>> {
    let prepared = prepare_hrrr_heavy_volume(surface, pressure, true)?;
    compute_severe_panel_fields_with_prepared_volume(surface, pressure, &prepared)
}

fn compute_severe_panel_fields_with_prepared_volume(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
    prepared: &PreparedHrrrHeavyVolume,
) -> Result<Vec<Solar07PanelField>, Box<dyn std::error::Error>> {
    let pressure_3d_pa = prepared
        .pressure_3d_pa
        .as_deref()
        .ok_or("prepared severe volume was missing broadcast pressure data")?;
    let fields = compute_supported_severe_fields(
        prepared.grid,
        EcapeVolumeInputs {
            pressure_pa: pressure_3d_pa,
            temperature_c: &pressure.temperature_c_3d,
            qvapor_kgkg: &pressure.qvapor_kgkg_3d,
            height_agl_m: &prepared.height_agl_3d,
            u_ms: &pressure.u_ms_3d,
            v_ms: &pressure.v_ms_3d,
            nz: prepared.shape.nz,
        },
        SurfaceInputs {
            psfc_pa: &surface.psfc_pa,
            t2_k: &surface.t2_k,
            q2_kgkg: &surface.q2_kgkg,
            u10_ms: &surface.u10_ms,
            v10_ms: &surface.v10_ms,
        },
    )?;
    Ok(severe_panel_fields_from_supported(fields))
}

pub fn compute_ecape8_panel_fields(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
) -> Result<(Vec<Solar07PanelField>, usize), Box<dyn std::error::Error>> {
    let prepared = prepare_hrrr_heavy_volume(surface, pressure, false)?;
    compute_ecape8_panel_fields_with_prepared_volume(surface, pressure, &prepared)
}

fn compute_ecape8_panel_fields_with_prepared_volume(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
    prepared: &PreparedHrrrHeavyVolume,
) -> Result<(Vec<Solar07PanelField>, usize), Box<dyn std::error::Error>> {
    let triplet = compute_ecape_triplet_with_failure_mask_from_parts(
        prepared.grid,
        EcapeVolumeInputs {
            pressure_pa: &prepared.pressure_levels_pa,
            temperature_c: &pressure.temperature_c_3d,
            qvapor_kgkg: &pressure.qvapor_kgkg_3d,
            height_agl_m: &prepared.height_agl_3d,
            u_ms: &pressure.u_ms_3d,
            v_ms: &pressure.v_ms_3d,
            nz: prepared.shape.nz,
        },
        SurfaceInputs {
            psfc_pa: &surface.psfc_pa,
            t2_k: &surface.t2_k,
            q2_kgkg: &surface.q2_kgkg,
            u10_ms: &surface.u10_ms,
            v10_ms: &surface.v10_ms,
        },
        EcapeTripletOptions::new("right_moving"),
    )?;
    let wind = WindGridInputs {
        shape: prepared.shape,
        u_3d_ms: &pressure.u_ms_3d,
        v_3d_ms: &pressure.v_ms_3d,
        height_agl_3d_m: &prepared.height_agl_3d,
    };
    let wind_diagnostics = compute_wind_diagnostics_bundle(wind)?;
    let experimental = compute_scp_ehi(ScpEhiInputs {
        grid: prepared.grid,
        scp_cape_jkg: &triplet.mu.fields.ecape_jkg,
        scp_srh_m2s2: &wind_diagnostics.srh_03km_m2s2,
        scp_bulk_wind_difference_ms: &wind_diagnostics.shear_06km_ms,
        ehi_cape_jkg: &triplet.sb.fields.ecape_jkg,
        ehi_srh_m2s2: &wind_diagnostics.srh_01km_m2s2,
    })?;
    let failure_count = triplet.total_failure_count();

    let fields = vec![
        Solar07PanelField::new(Solar07Product::Sbecape, "J/kg", triplet.sb.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Mlecape, "J/kg", triplet.ml.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Muecape, "J/kg", triplet.mu.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Sbncape, "J/kg", triplet.sb.fields.ncape_jkg),
        Solar07PanelField::new(Solar07Product::Sbecin, "J/kg", triplet.sb.fields.cin_jkg),
        Solar07PanelField::new(Solar07Product::Mlecin, "J/kg", triplet.ml.fields.cin_jkg),
        Solar07PanelField::new(
            Solar07Product::EcapeScpExperimental,
            "dimensionless",
            experimental.scp,
        ),
        Solar07PanelField::new(
            Solar07Product::EcapeEhiExperimental,
            "dimensionless",
            experimental.ehi,
        ),
    ];
    Ok((fields, failure_count))
}

fn prepare_hrrr_heavy_volume(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
    include_pressure_3d: bool,
) -> Result<PreparedHrrrHeavyVolume, Box<dyn std::error::Error>> {
    let grid = CalcGridShape::new(surface.nx, surface.ny)?;
    let shape = VolumeShape::new(grid, pressure.pressure_levels_hpa.len())?;
    let pressure_levels_pa = pressure
        .pressure_levels_hpa
        .iter()
        .map(|level_hpa| level_hpa * 100.0)
        .collect::<Vec<_>>();
    Ok(PreparedHrrrHeavyVolume {
        grid,
        shape,
        pressure_levels_pa,
        pressure_3d_pa: include_pressure_3d
            .then(|| broadcast_levels_pa(&pressure.pressure_levels_hpa, grid.len())),
        height_agl_3d: compute_height_agl_3d(surface, pressure, grid, shape),
    })
}

pub(crate) fn compute_height_agl_3d(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
    grid: CalcGridShape,
    shape: VolumeShape,
) -> Vec<f64> {
    let mut height_agl_3d = pressure
        .gh_m_3d
        .iter()
        .enumerate()
        .map(|(idx, &value)| {
            let ij = idx % grid.len();
            (value - surface.orog_m[ij]).max(0.0)
        })
        .collect::<Vec<_>>();

    for k in 1..shape.nz {
        let level_offset = k * grid.len();
        let prev_offset = (k - 1) * grid.len();
        for ij in 0..grid.len() {
            let min_height = height_agl_3d[prev_offset + ij] + 1.0;
            if height_agl_3d[level_offset + ij] < min_height {
                height_agl_3d[level_offset + ij] = min_height;
            }
        }
    }

    height_agl_3d
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

fn pressure_grid_shape_from_messages(
    messages: &[Grib2Message],
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let mut matching = messages.iter().filter(|msg| msg.product.level_type == 100);
    let sample = matching
        .next()
        .ok_or("pressure subset had no isobaric GRIB messages")?;
    let nx = sample.grid.nx as usize;
    let ny = sample.grid.ny as usize;
    for message in matching {
        let message_nx = message.grid.nx as usize;
        let message_ny = message.grid.ny as usize;
        if message_nx != nx || message_ny != ny {
            return Err("pressure subset contained inconsistent grid shapes".into());
        }
    }
    Ok((nx, ny))
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

fn crop_hrrr_heavy_domain(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
    bounds: (f64, f64, f64, f64),
) -> Result<Option<CroppedHrrrHeavyDomain>, Box<dyn std::error::Error>> {
    let Some(crop) = crop_rect_for_bounds(surface, bounds)? else {
        return Ok(None);
    };
    let cropped_surface = crop_surface_fields(surface, crop);
    let cropped_pressure = crop_pressure_fields(pressure, surface.nx, surface.ny, crop)?;
    let grid = cropped_surface.core_grid()?;
    Ok(Some(CroppedHrrrHeavyDomain {
        surface: cropped_surface,
        pressure: cropped_pressure,
        grid,
    }))
}

fn crop_rect_for_bounds(
    surface: &HrrrSurfaceFields,
    bounds: (f64, f64, f64, f64),
) -> Result<Option<GridCrop>, Box<dyn std::error::Error>> {
    let mut min_x = surface.nx;
    let mut max_x = 0usize;
    let mut min_y = surface.ny;
    let mut max_y = 0usize;
    let mut found = false;

    for y in 0..surface.ny {
        let row_offset = y * surface.nx;
        for x in 0..surface.nx {
            let idx = row_offset + x;
            let lat = surface.lat[idx];
            let lon = surface.lon[idx];
            if lon >= bounds.0 && lon <= bounds.1 && lat >= bounds.2 && lat <= bounds.3 {
                min_x = min_x.min(x);
                max_x = max_x.max(x);
                min_y = min_y.min(y);
                max_y = max_y.max(y);
                found = true;
            }
        }
    }

    if !found {
        return Err("requested crop produced an empty heavy-compute domain".into());
    }

    let crop = GridCrop {
        x_start: min_x,
        x_end: max_x + 1,
        y_start: min_y,
        y_end: max_y + 1,
    };

    if crop.x_start == 0
        && crop.x_end == surface.nx
        && crop.y_start == 0
        && crop.y_end == surface.ny
    {
        Ok(None)
    } else {
        Ok(Some(crop))
    }
}

fn crop_surface_fields(surface: &HrrrSurfaceFields, crop: GridCrop) -> HrrrSurfaceFields {
    HrrrSurfaceFields {
        lat: crop_2d_values(&surface.lat, surface.nx, crop),
        lon: crop_2d_values(&surface.lon, surface.nx, crop),
        nx: crop.width(),
        ny: crop.height(),
        psfc_pa: crop_2d_values(&surface.psfc_pa, surface.nx, crop),
        orog_m: crop_2d_values(&surface.orog_m, surface.nx, crop),
        t2_k: crop_2d_values(&surface.t2_k, surface.nx, crop),
        q2_kgkg: crop_2d_values(&surface.q2_kgkg, surface.nx, crop),
        u10_ms: crop_2d_values(&surface.u10_ms, surface.nx, crop),
        v10_ms: crop_2d_values(&surface.v10_ms, surface.nx, crop),
        lambert_latin1: surface.lambert_latin1,
        lambert_latin2: surface.lambert_latin2,
        lambert_lov: surface.lambert_lov,
    }
}

fn crop_pressure_fields(
    pressure: &HrrrPressureFields,
    source_nx: usize,
    source_ny: usize,
    crop: GridCrop,
) -> Result<HrrrPressureFields, Box<dyn std::error::Error>> {
    let level_count = pressure.pressure_levels_hpa.len();
    let expected_len = source_nx
        .checked_mul(source_ny)
        .and_then(|n2d| n2d.checked_mul(level_count))
        .ok_or("pressure crop expected length overflowed")?;
    for (name, values) in [
        ("temperature_c_3d", &pressure.temperature_c_3d),
        ("qvapor_kgkg_3d", &pressure.qvapor_kgkg_3d),
        ("u_ms_3d", &pressure.u_ms_3d),
        ("v_ms_3d", &pressure.v_ms_3d),
        ("gh_m_3d", &pressure.gh_m_3d),
    ] {
        if values.len() != expected_len {
            return Err(format!(
                "pressure field {name} length {} did not match expected source volume length {expected_len}",
                values.len()
            )
            .into());
        }
    }

    Ok(HrrrPressureFields {
        pressure_levels_hpa: pressure.pressure_levels_hpa.clone(),
        temperature_c_3d: crop_3d_values(
            &pressure.temperature_c_3d,
            source_nx,
            source_ny,
            level_count,
            crop,
        ),
        qvapor_kgkg_3d: crop_3d_values(
            &pressure.qvapor_kgkg_3d,
            source_nx,
            source_ny,
            level_count,
            crop,
        ),
        u_ms_3d: crop_3d_values(&pressure.u_ms_3d, source_nx, source_ny, level_count, crop),
        v_ms_3d: crop_3d_values(&pressure.v_ms_3d, source_nx, source_ny, level_count, crop),
        gh_m_3d: crop_3d_values(&pressure.gh_m_3d, source_nx, source_ny, level_count, crop),
    })
}

fn crop_2d_values(values: &[f64], source_nx: usize, crop: GridCrop) -> Vec<f64> {
    let mut out = Vec::with_capacity(crop.width() * crop.height());
    for y in crop.y_start..crop.y_end {
        let row_start = y * source_nx + crop.x_start;
        let row_end = row_start + crop.width();
        out.extend_from_slice(&values[row_start..row_end]);
    }
    out
}

fn crop_3d_values(
    values: &[f64],
    source_nx: usize,
    source_ny: usize,
    level_count: usize,
    crop: GridCrop,
) -> Vec<f64> {
    let source_n2d = source_nx * source_ny;
    let mut out = Vec::with_capacity(crop.width() * crop.height() * level_count);
    for level in 0..level_count {
        let level_offset = level * source_n2d;
        for y in crop.y_start..crop.y_end {
            let row_start = level_offset + y * source_nx + crop.x_start;
            let row_end = row_start + crop.width();
            out.extend_from_slice(&values[row_start..row_end]);
        }
    }
    out
}

fn validate_pressure_shape(
    found_nx: usize,
    found_ny: usize,
    expected_nx: usize,
    expected_ny: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if found_nx != expected_nx || found_ny != expected_ny {
        return Err(format!(
            "pressure subset grid shape {found_nx}x{found_ny} did not match expected {expected_nx}x{expected_ny}"
        )
        .into());
    }
    Ok(())
}

fn validate_pressure_decode_against_surface(
    decoded: &CachedDecode<HrrrPressureFields>,
    decoded_shape: Option<(usize, usize)>,
    expected_nx: usize,
    expected_ny: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((found_nx, found_ny)) = decoded_shape {
        return validate_pressure_shape(found_nx, found_ny, expected_nx, expected_ny);
    }
    validate_cached_pressure_point_count(&decoded.value, expected_nx, expected_ny)
}

fn validate_cached_pressure_point_count(
    pressure: &HrrrPressureFields,
    expected_nx: usize,
    expected_ny: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let level_count = pressure.pressure_levels_hpa.len();
    if level_count == 0 {
        return Err("cached pressure decode had no pressure levels".into());
    }
    let expected_points = expected_nx
        .checked_mul(expected_ny)
        .ok_or("expected surface grid shape overflowed point-count validation")?;
    validate_cached_pressure_volume_len(
        "temperature_c_3d",
        pressure.temperature_c_3d.len(),
        level_count,
        expected_points,
    )?;
    validate_cached_pressure_volume_len(
        "qvapor_kgkg_3d",
        pressure.qvapor_kgkg_3d.len(),
        level_count,
        expected_points,
    )?;
    validate_cached_pressure_volume_len(
        "u_ms_3d",
        pressure.u_ms_3d.len(),
        level_count,
        expected_points,
    )?;
    validate_cached_pressure_volume_len(
        "v_ms_3d",
        pressure.v_ms_3d.len(),
        level_count,
        expected_points,
    )?;
    validate_cached_pressure_volume_len(
        "gh_m_3d",
        pressure.gh_m_3d.len(),
        level_count,
        expected_points,
    )?;
    Ok(())
}

fn validate_cached_pressure_volume_len(
    field_name: &str,
    len: usize,
    level_count: usize,
    expected_points: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if len % level_count != 0 {
        return Err(format!(
            "cached pressure field {field_name} length {len} was not divisible by level count {level_count}"
        )
        .into());
    }
    let found_points = len / level_count;
    if found_points != expected_points {
        return Err(format!(
            "cached pressure field {field_name} had {found_points} horizontal points, expected {expected_points}"
        )
        .into());
    }
    Ok(())
}

fn png_render_parallelism(job_count: usize) -> usize {
    thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(job_count.max(1))
}

fn thread_render_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

fn join_scoped_job<T>(
    handle: thread::ScopedJoinHandle<'_, Result<T, io::Error>>,
) -> Result<T, io::Error> {
    match handle.join() {
        Ok(result) => result,
        Err(panic) => Err(io::Error::other(format!(
            "worker panicked: {}",
            panic_message(panic)
        ))),
    }
}

fn join_render_job<T>(
    handle: thread::ScopedJoinHandle<'_, Result<T, io::Error>>,
) -> Result<T, io::Error> {
    join_scoped_job(handle).map_err(|err| io::Error::other(format!("render worker failed: {err}")))
}

fn panic_message(panic: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_calc::SupportedSevereFields;

    #[test]
    fn explicit_hrrr_cycle_avoids_latest_probe() {
        let latest = resolve_hrrr_run("20260414", Some(19), SourceId::Aws).unwrap();
        assert_eq!(latest.model, ModelId::Hrrr);
        assert_eq!(latest.cycle.date_yyyymmdd, "20260414");
        assert_eq!(latest.cycle.hour_utc, 19);
        assert_eq!(latest.source, SourceId::Aws);
    }

    #[test]
    fn family_file_fetch_request_uses_empty_variable_patterns() {
        let request = hrrr_fetch_request(
            CycleSpec::new("20260414", 23).unwrap(),
            0,
            SourceId::Aws,
            "sfc",
            Vec::new(),
        )
        .unwrap();
        assert!(request.variable_patterns.is_empty());
    }

    #[test]
    fn fetched_subset_runtime_info_keeps_planned_and_actual_fetch_truth() {
        let subset = HrrrFetchedSubset {
            request: hrrr_fetch_request(
                CycleSpec::new("20260414", 23).unwrap(),
                6,
                SourceId::Nomads,
                "sfc",
                Vec::new(),
            )
            .unwrap(),
            fetched: CachedFetchResult {
                result: rustwx_io::FetchResult {
                    source: SourceId::Nomads,
                    url: "https://example.test/hrrr.t23z.wrfsfcf06.grib2".into(),
                    bytes: vec![1, 2, 3],
                },
                cache_hit: false,
                bytes_path: PathBuf::from("fetch.grib2"),
                metadata_path: PathBuf::from("fetch_meta.json"),
            },
            bytes: vec![1, 2, 3],
        };

        let runtime = subset.runtime_info("nat");
        assert_eq!(runtime.planned_product, "nat");
        assert_eq!(runtime.fetched_product, "sfc");
        assert_eq!(runtime.requested_source, SourceId::Nomads);
        assert_eq!(runtime.resolved_source, SourceId::Nomads);
        assert!(runtime.resolved_url.contains("wrfsfc"));
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

    #[test]
    fn batch_product_dedupe_preserves_first_seen_order() {
        let products = dedupe_products(&[
            HrrrBatchProduct::Ecape8Panel,
            HrrrBatchProduct::SevereProofPanel,
            HrrrBatchProduct::Ecape8Panel,
        ]);
        assert_eq!(
            products,
            vec![
                HrrrBatchProduct::Ecape8Panel,
                HrrrBatchProduct::SevereProofPanel
            ]
        );
    }

    #[test]
    fn severe_field_titles_keep_current_labels_explicit() {
        let fields = severe_panel_fields_from_supported(SupportedSevereFields {
            sbcape_jkg: vec![1.0],
            mlcin_jkg: vec![-25.0],
            mucape_jkg: vec![2.0],
            srh_01km_m2s2: vec![100.0],
            srh_03km_m2s2: vec![200.0],
            shear_06km_ms: vec![20.0],
            stp_fixed: vec![1.5],
            scp_mu_03km_06km_proxy: vec![5.0],
            ehi_sb_01km_proxy: vec![2.0],
        });

        assert_eq!(fields.len(), 8);
        assert_eq!(fields[5].product, Solar07Product::StpFixed);
        assert_eq!(
            fields[6].title_override.as_deref(),
            Some("SCP (MU / 0-3 KM / 0-6 KM PROXY)")
        );
        assert_eq!(fields[7].title_override.as_deref(), Some("EHI 0-1 KM"));
    }

    #[test]
    fn cached_pressure_decode_validates_against_expected_surface_point_count() {
        let decoded = CachedDecode {
            value: HrrrPressureFields {
                pressure_levels_hpa: vec![1000.0, 850.0],
                temperature_c_3d: vec![0.0; 8],
                qvapor_kgkg_3d: vec![0.0; 8],
                u_ms_3d: vec![0.0; 8],
                v_ms_3d: vec![0.0; 8],
                gh_m_3d: vec![0.0; 8],
            },
            cache_hit: true,
            path: PathBuf::from("cached_pressure.bin"),
        };

        validate_pressure_decode_against_surface(&decoded, None, 2, 2).unwrap();
        assert!(validate_pressure_decode_against_surface(&decoded, None, 3, 2).is_err());
    }

    #[test]
    fn crop_rect_for_bounds_returns_none_for_full_domain() {
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
        assert!(
            crop_rect_for_bounds(&surface, (-101.0, -98.0, 34.5, 36.5))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn crop_hrrr_heavy_domain_reduces_surface_and_pressure_shapes() {
        let surface = HrrrSurfaceFields {
            lat: vec![
                40.0, 40.0, 40.0, 40.0, //
                41.0, 41.0, 41.0, 41.0, //
                42.0, 42.0, 42.0, 42.0,
            ],
            lon: vec![
                -102.0, -101.0, -100.0, -99.0, //
                -102.0, -101.0, -100.0, -99.0, //
                -102.0, -101.0, -100.0, -99.0,
            ],
            nx: 4,
            ny: 3,
            psfc_pa: (0..12).map(|v| v as f64).collect(),
            orog_m: (100..112).map(|v| v as f64).collect(),
            t2_k: (200..212).map(|v| v as f64).collect(),
            q2_kgkg: (300..312).map(|v| v as f64).collect(),
            u10_ms: (400..412).map(|v| v as f64).collect(),
            v10_ms: (500..512).map(|v| v as f64).collect(),
            lambert_latin1: 33.0,
            lambert_latin2: 45.0,
            lambert_lov: -97.0,
        };
        let pressure = HrrrPressureFields {
            pressure_levels_hpa: vec![1000.0, 850.0],
            temperature_c_3d: (0..24).map(|v| v as f64).collect(),
            qvapor_kgkg_3d: (100..124).map(|v| v as f64).collect(),
            u_ms_3d: (200..224).map(|v| v as f64).collect(),
            v_ms_3d: (300..324).map(|v| v as f64).collect(),
            gh_m_3d: (400..424).map(|v| v as f64).collect(),
        };

        let cropped = crop_hrrr_heavy_domain(&surface, &pressure, (-101.1, -99.9, 40.5, 42.5))
            .unwrap()
            .unwrap();

        assert_eq!(cropped.surface.nx, 2);
        assert_eq!(cropped.surface.ny, 2);
        assert_eq!(cropped.surface.psfc_pa, vec![5.0, 6.0, 9.0, 10.0]);
        assert_eq!(
            cropped.pressure.temperature_c_3d,
            vec![5.0, 6.0, 9.0, 10.0, 17.0, 18.0, 21.0, 22.0]
        );
        assert_eq!(cropped.grid.shape.nx, 2);
        assert_eq!(cropped.grid.shape.ny, 2);
    }
}
