use crate::cache::{load_bincode, store_bincode};
use crate::direct::build_projected_map;
use crate::hrrr::ProjectedMap;
use grib_core::grib2::{
    Grib2File, Grib2Message, flip_rows, grid_latlon, unpack_message_normalized,
};
use rustwx_calc::{GridShape as CalcGridShape, VolumeShape};
use rustwx_core::{
    CanonicalBundleDescriptor, CanonicalDataFamily, CycleSpec, GridShape, LatLonGrid, ModelId,
    ModelRunRequest, RustwxError, SourceId,
};
use rustwx_io::{CachedFetchResult, FetchRequest, artifact_cache_dir, fetch_bytes_with_cache};
use rustwx_models::{
    LatestRun, ResolvedCanonicalBundleProduct, latest_available_run,
    resolve_canonical_bundle_product,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use wrf_render::render::map_frame_aspect_ratio;

const GEOPOTENTIAL_M2S2_TO_M: f64 = 1.0 / 9.806_65;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceFields {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
    pub nx: usize,
    pub ny: usize,
    pub psfc_pa: Vec<f64>,
    pub orog_m: Vec<f64>,
    pub orog_is_proxy: bool,
    pub t2_k: Vec<f64>,
    pub q2_kgkg: Vec<f64>,
    pub u10_ms: Vec<f64>,
    pub v10_ms: Vec<f64>,
}

impl SurfaceFields {
    pub fn core_grid(&self) -> Result<LatLonGrid, RustwxError> {
        LatLonGrid::new(
            GridShape::new(self.nx, self.ny)?,
            self.lat.iter().map(|&v| v as f32).collect(),
            self.lon.iter().map(|&v| v as f32).collect(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PressureFields {
    pub pressure_levels_hpa: Vec<f64>,
    pub temperature_c_3d: Vec<f64>,
    pub qvapor_kgkg_3d: Vec<f64>,
    pub u_ms_3d: Vec<f64>,
    pub v_ms_3d: Vec<f64>,
    pub gh_m_3d: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRuntimeInfo {
    pub planned_bundle: CanonicalBundleDescriptor,
    pub planned_family: CanonicalDataFamily,
    pub planned_product: String,
    pub resolved_native_product: String,
    pub fetched_product: String,
    pub requested_source: SourceId,
    pub resolved_source: SourceId,
    pub resolved_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedTiming {
    pub fetch_surface_ms: u128,
    pub fetch_pressure_ms: u128,
    pub decode_surface_ms: u128,
    pub decode_pressure_ms: u128,
    pub fetch_surface_cache_hit: bool,
    pub fetch_pressure_cache_hit: bool,
    pub decode_surface_cache_hit: bool,
    pub decode_pressure_cache_hit: bool,
    pub surface_fetch: FetchRuntimeInfo,
    pub pressure_fetch: FetchRuntimeInfo,
}

#[derive(Debug, Clone)]
pub struct CachedDecode<T> {
    pub value: T,
    pub cache_hit: bool,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FetchedModelFile {
    pub request: FetchRequest,
    pub fetched: CachedFetchResult,
    pub bytes: Vec<u8>,
}

impl FetchedModelFile {
    pub fn runtime_info(
        &self,
        planned_bundle: &ResolvedCanonicalBundleProduct,
    ) -> FetchRuntimeInfo {
        FetchRuntimeInfo {
            planned_bundle: planned_bundle.bundle,
            planned_family: planned_bundle.family,
            planned_product: planned_bundle.native_product.clone(),
            resolved_native_product: planned_bundle.native_product.clone(),
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

#[derive(Debug)]
pub struct LoadedModelTimestep {
    pub latest: LatestRun,
    pub model: ModelId,
    pub surface_file: FetchedModelFile,
    pub pressure_file: FetchedModelFile,
    pub surface_decode: CachedDecode<SurfaceFields>,
    pub pressure_decode: CachedDecode<PressureFields>,
    pub grid: LatLonGrid,
    pub shared_timing: SharedTiming,
}

#[derive(Debug, Clone)]
pub struct PreparedProjectedContext {
    projected_maps: HashMap<(u32, u32), ProjectedMap>,
}

impl PreparedProjectedContext {
    pub fn projected_map(&self, width: u32, height: u32) -> Option<&ProjectedMap> {
        self.projected_maps.get(&(width, height))
    }
}

#[derive(Debug, Clone)]
pub struct PreparedHeavyVolume {
    pub grid: CalcGridShape,
    pub shape: VolumeShape,
    pub pressure_levels_pa: Vec<f64>,
    pub pressure_3d_pa: Option<Vec<f64>>,
    pub height_agl_3d: Vec<f64>,
}

pub fn resolve_model_run(
    model: ModelId,
    date: &str,
    cycle_override: Option<u8>,
    source: SourceId,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model,
            cycle: CycleSpec::new(date, hour)?,
            source,
        }),
        None => Ok(latest_available_run(model, Some(source), date)?),
    }
}

pub fn load_model_timestep_from_parts(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
    cache_root: &Path,
    use_cache: bool,
) -> Result<LoadedModelTimestep, Box<dyn std::error::Error>> {
    let latest = resolve_model_run(model, date_yyyymmdd, cycle_override_utc, source)?;
    load_model_timestep_from_latest(
        latest,
        forecast_hour,
        surface_product_override,
        pressure_product_override,
        cache_root,
        use_cache,
    )
}

pub fn load_model_timestep_from_latest(
    latest: LatestRun,
    forecast_hour: u16,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
    cache_root: &Path,
    use_cache: bool,
) -> Result<LoadedModelTimestep, Box<dyn std::error::Error>> {
    let model = latest.model;
    let (surface_bundle, pressure_bundle) =
        thermo_bundles(model, surface_product_override, pressure_product_override);

    let ((surface_file, fetch_surface_ms), (pressure_file, fetch_pressure_ms)) =
        if surface_bundle.native_product == pressure_bundle.native_product {
            let fetch_start = Instant::now();
            let fetched = fetch_family_file(
                model,
                latest.cycle.clone(),
                forecast_hour,
                latest.source,
                &surface_bundle,
                cache_root,
                use_cache,
            )?;
            let elapsed = fetch_start.elapsed().as_millis();
            ((fetched.clone(), elapsed), (fetched, elapsed))
        } else {
            let surface_start = Instant::now();
            let surface = fetch_family_file(
                model,
                latest.cycle.clone(),
                forecast_hour,
                latest.source,
                &surface_bundle,
                cache_root,
                use_cache,
            )?;
            let pressure_start = Instant::now();
            let pressure = fetch_family_file(
                model,
                latest.cycle.clone(),
                forecast_hour,
                latest.source,
                &pressure_bundle,
                cache_root,
                use_cache,
            )?;
            (
                (surface, surface_start.elapsed().as_millis()),
                (pressure, pressure_start.elapsed().as_millis()),
            )
        };

    let surface_cache_path = decode_cache_path(cache_root, &surface_file.request, "surface");
    let pressure_cache_path = decode_cache_path(cache_root, &pressure_file.request, "pressure");
    let surface_bytes = surface_file.bytes.as_slice();
    let pressure_bytes = pressure_file.bytes.as_slice();
    let decode_surface_start = Instant::now();
    let surface_decode = load_or_decode_surface(&surface_cache_path, surface_bytes, use_cache)?;
    let decode_surface_ms = decode_surface_start.elapsed().as_millis();
    let decode_pressure_start = Instant::now();
    let (pressure_decode, pressure_shape) =
        load_or_decode_pressure_with_shape(&pressure_cache_path, pressure_bytes, use_cache)?;
    let decode_pressure_ms = decode_pressure_start.elapsed().as_millis();

    validate_pressure_decode_against_surface(
        &pressure_decode,
        pressure_shape,
        surface_decode.value.nx,
        surface_decode.value.ny,
    )?;
    let grid = surface_decode.value.core_grid()?;
    let surface_fetch = surface_file.runtime_info(&surface_bundle);
    let pressure_fetch = pressure_file.runtime_info(&pressure_bundle);

    Ok(LoadedModelTimestep {
        latest,
        model,
        surface_file,
        pressure_file,
        surface_decode,
        pressure_decode,
        grid,
        shared_timing: SharedTiming {
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

pub fn build_projected_maps_for_sizes(
    surface: &SurfaceFields,
    bounds: (f64, f64, f64, f64),
    sizes: &[(u32, u32)],
) -> Result<PreparedProjectedContext, Box<dyn std::error::Error>> {
    let mut projected_maps = HashMap::new();
    for &(width, height) in sizes {
        if width == 0 || height == 0 || projected_maps.contains_key(&(width, height)) {
            continue;
        }
        let projected = build_projected_map(
            &surface
                .lat
                .iter()
                .copied()
                .map(|v| v as f32)
                .collect::<Vec<_>>(),
            &surface
                .lon
                .iter()
                .copied()
                .map(|v| v as f32)
                .collect::<Vec<_>>(),
            bounds,
            map_frame_aspect_ratio(width, height, true, true),
        )?;
        projected_maps.insert((width, height), projected);
    }
    Ok(PreparedProjectedContext { projected_maps })
}

pub fn prepare_heavy_volume(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    include_pressure_3d: bool,
) -> Result<PreparedHeavyVolume, Box<dyn std::error::Error>> {
    let grid = CalcGridShape::new(surface.nx, surface.ny)?;
    let shape = VolumeShape::new(grid, pressure.pressure_levels_hpa.len())?;
    let pressure_levels_pa = pressure
        .pressure_levels_hpa
        .iter()
        .map(|level_hpa| level_hpa * 100.0)
        .collect::<Vec<_>>();
    Ok(PreparedHeavyVolume {
        grid,
        shape,
        pressure_levels_pa,
        pressure_3d_pa: include_pressure_3d
            .then(|| broadcast_levels_pa(&pressure.pressure_levels_hpa, grid.len())),
        height_agl_3d: compute_height_agl_3d(surface, pressure, grid, shape),
    })
}

pub fn compute_height_agl_3d(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    grid: CalcGridShape,
    shape: VolumeShape,
) -> Vec<f64> {
    let fallback_orog = surface
        .orog_is_proxy
        .then(|| proxy_orography_from_pressure(pressure, grid, shape));
    let mut height_agl_3d = pressure
        .gh_m_3d
        .iter()
        .enumerate()
        .map(|(idx, &value)| {
            let ij = idx % grid.len();
            let orog = fallback_orog
                .as_ref()
                .map(|values| values[ij])
                .unwrap_or(surface.orog_m[ij]);
            (value - orog).max(0.0)
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

fn proxy_orography_from_pressure(
    pressure: &PressureFields,
    grid: CalcGridShape,
    shape: VolumeShape,
) -> Vec<f64> {
    let mut proxy = vec![f64::INFINITY; grid.len()];
    for k in 0..shape.nz {
        let level_offset = k * grid.len();
        for ij in 0..grid.len() {
            proxy[ij] = proxy[ij].min(pressure.gh_m_3d[level_offset + ij]);
        }
    }
    proxy
        .into_iter()
        .map(|value| if value.is_finite() { value } else { 0.0 })
        .collect()
}

pub fn broadcast_levels_pa(levels_hpa: &[f64], n2d: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(levels_hpa.len() * n2d);
    for level in levels_hpa {
        out.extend(std::iter::repeat_n(*level * 100.0, n2d));
    }
    out
}

fn thermo_bundles(
    model: ModelId,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
) -> (
    ResolvedCanonicalBundleProduct,
    ResolvedCanonicalBundleProduct,
) {
    (
        resolve_canonical_bundle_product(
            model,
            CanonicalBundleDescriptor::SurfaceAnalysis,
            surface_product_override,
        ),
        resolve_canonical_bundle_product(
            model,
            CanonicalBundleDescriptor::PressureAnalysis,
            pressure_product_override,
        ),
    )
}

fn fetch_family_file(
    model: ModelId,
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    bundle: &ResolvedCanonicalBundleProduct,
    cache_root: &Path,
    use_cache: bool,
) -> Result<FetchedModelFile, Box<dyn std::error::Error>> {
    let request = FetchRequest {
        request: ModelRunRequest::new(model, cycle, forecast_hour, &bundle.native_product)?,
        source_override: Some(source),
        variable_patterns: Vec::new(),
    };
    let fetched = fetch_bytes_with_cache(&request, cache_root, use_cache)?;
    Ok(FetchedModelFile {
        request,
        bytes: fetched.result.bytes.clone(),
        fetched,
    })
}

fn decode_cache_path(cache_root: &Path, fetch: &FetchRequest, name: &str) -> PathBuf {
    artifact_cache_dir(cache_root, fetch)
        .join("decoded")
        .join(format!("{name}.bin"))
}

fn load_or_decode_surface(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<CachedDecode<SurfaceFields>, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<SurfaceFields>(path)? {
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

fn load_or_decode_pressure_with_shape(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<(CachedDecode<PressureFields>, Option<(usize, usize)>), Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<PressureFields>(path)? {
            return Ok((
                CachedDecode {
                    value: cached,
                    cache_hit: true,
                    path: path.to_path_buf(),
                },
                None,
            ));
        }
    }
    let (decoded, nx, ny) = decode_pressure_with_shape(bytes)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok((
        CachedDecode {
            value: decoded,
            cache_hit: false,
            path: path.to_path_buf(),
        },
        Some((nx, ny)),
    ))
}

fn decode_surface(bytes: &[u8]) -> Result<SurfaceFields, Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let sample = file
        .messages
        .first()
        .ok_or("surface family GRIB had no messages")?;
    let (mut lat_raw, mut lon_raw) = grid_latlon(&sample.grid);
    if sample.grid.scan_mode & 0x40 != 0 {
        flip_rows(
            &mut lat_raw,
            sample.grid.nx as usize,
            sample.grid.ny as usize,
        );
        flip_rows(
            &mut lon_raw,
            sample.grid.nx as usize,
            sample.grid.ny as usize,
        );
    }
    normalize_longitude_rows(
        &mut lat_raw,
        &mut lon_raw,
        sample.grid.nx as usize,
        sample.grid.ny as usize,
    );

    let lat = lat_raw;
    let lon = lon_raw
        .into_iter()
        .map(normalize_longitude)
        .collect::<Vec<_>>();
    let nx = sample.grid.nx as usize;
    let ny = sample.grid.ny as usize;

    let psfc_pa = unpack_message_normalized(find_message(
        &file.messages,
        &[(0, 3, 0, 1, Some(0.0)), (0, 3, 0, 1, None)],
    )?)?;
    let (orog_m, orog_is_proxy) = match decode_orography(&file.messages) {
        Ok(values) => (values, false),
        Err(_) => (vec![0.0; nx * ny], true),
    };
    let t2_k =
        unpack_message_normalized(find_message(&file.messages, &[(0, 0, 0, 103, Some(2.0))])?)?;
    let q2_kgkg = decode_surface_mixing_ratio(&file.messages, &psfc_pa, &t2_k)?;
    let u10_ms =
        unpack_message_normalized(find_message(&file.messages, &[(0, 2, 2, 103, Some(10.0))])?)?;
    let v10_ms =
        unpack_message_normalized(find_message(&file.messages, &[(0, 2, 3, 103, Some(10.0))])?)?;

    Ok(SurfaceFields {
        lat,
        lon,
        nx,
        ny,
        psfc_pa,
        orog_m,
        orog_is_proxy,
        t2_k,
        q2_kgkg,
        u10_ms,
        v10_ms,
    })
}

fn decode_pressure_with_shape(
    bytes: &[u8],
) -> Result<(PressureFields, usize, usize), Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let (nx, ny) = pressure_grid_shape_from_messages(&file.messages)?;
    let temperature = collect_levels(&file.messages, 0, 0, 0, 100)?;
    let u_wind = collect_levels(&file.messages, 0, 2, 2, 100)?;
    let v_wind = collect_levels(&file.messages, 0, 2, 3, 100)?;
    let gh = decode_height_levels(&file.messages)?;
    let moisture = decode_pressure_mixing_ratio_levels(&file.messages, &temperature)?;

    let levels = common_isobaric_levels(&temperature, &[&moisture, &u_wind, &v_wind, &gh]);
    if levels.is_empty() {
        return Err("pressure family had no common thermodynamic levels".into());
    }
    let aligned_levels = levels.clone();

    let expected = nx * ny;
    let flatten = |records: &Vec<(f64, Vec<f64>)>| -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        let mut out = Vec::with_capacity(levels.len() * expected);
        for &level in &levels {
            let values = level_values(records, level)
                .ok_or_else(|| format!("missing aligned pressure level {level}"))?;
            if values.len() != expected {
                return Err("decoded pressure field had unexpected grid size".into());
            }
            out.extend_from_slice(values);
        }
        Ok(out)
    };

    Ok((
        PressureFields {
            pressure_levels_hpa: aligned_levels
                .into_iter()
                .map(normalize_pressure_level_hpa)
                .collect(),
            temperature_c_3d: flatten(&temperature)?
                .into_iter()
                .map(|value| value - 273.15)
                .collect(),
            qvapor_kgkg_3d: flatten(&moisture)?,
            u_ms_3d: flatten(&u_wind)?,
            v_ms_3d: flatten(&v_wind)?,
            gh_m_3d: flatten(&gh)?,
        },
        nx,
        ny,
    ))
}

fn decode_orography(messages: &[Grib2Message]) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    if let Ok(message) = find_message(messages, &[(0, 3, 5, 1, Some(0.0)), (0, 3, 5, 1, None)]) {
        return Ok(unpack_message_normalized(message)?);
    }
    if let Ok(message) = find_message(messages, &[(0, 3, 4, 1, Some(0.0)), (0, 3, 4, 1, None)]) {
        return Ok(unpack_message_normalized(message)?
            .into_iter()
            .map(|value| value * GEOPOTENTIAL_M2S2_TO_M)
            .collect());
    }
    Err("missing surface orography/geopotential-height field".into())
}

fn decode_surface_mixing_ratio(
    messages: &[Grib2Message],
    psfc_pa: &[f64],
    t2_k: &[f64],
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    if let Ok(message) = find_message(messages, &[(0, 1, 0, 103, Some(2.0))]) {
        return Ok(q_to_mixing_ratio(&unpack_message_normalized(message)?));
    }
    if let Ok(message) = find_message(messages, &[(0, 0, 6, 103, Some(2.0))]) {
        let dewpoint_k = unpack_message_normalized(message)?;
        return Ok(psfc_pa
            .iter()
            .zip(dewpoint_k.iter())
            .map(|(&psfc, &td_k)| mixing_ratio_from_dewpoint_k(psfc / 100.0, td_k))
            .collect());
    }
    if let Ok(message) = find_message(messages, &[(0, 1, 1, 103, Some(2.0))]) {
        let rh_pct = unpack_message_normalized(message)?;
        return Ok(psfc_pa
            .iter()
            .zip(t2_k.iter())
            .zip(rh_pct.iter())
            .map(|((&psfc, &t_k), &rh)| mixing_ratio_from_relative_humidity(psfc / 100.0, t_k, rh))
            .collect());
    }
    Err("missing 2m specific humidity/dewpoint/RH field for surface thermodynamics".into())
}

fn decode_pressure_mixing_ratio_levels(
    messages: &[Grib2Message],
    temperature: &Vec<(f64, Vec<f64>)>,
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    if let Ok(levels) = collect_levels(messages, 0, 1, 0, 100) {
        return Ok(levels
            .into_iter()
            .map(|(level, values)| (level, q_to_mixing_ratio(&values)))
            .collect());
    }
    if let Ok(dewpoint) = collect_levels(messages, 0, 0, 6, 100) {
        let mut out = Vec::with_capacity(dewpoint.len());
        for (level, td_k) in dewpoint {
            out.push((
                level,
                td_k.into_iter()
                    .map(|td_k| {
                        mixing_ratio_from_dewpoint_k(normalize_pressure_level_hpa(level), td_k)
                    })
                    .collect(),
            ));
        }
        return Ok(out);
    }
    if let Ok(rh) = collect_levels(messages, 0, 1, 1, 100) {
        let mut out = Vec::with_capacity(rh.len());
        for (level, rh_pct) in rh {
            let temperature_k = level_values(temperature, level)
                .ok_or_else(|| format!("missing temperature level {level} for RH fallback"))?;
            out.push((
                level,
                temperature_k
                    .iter()
                    .zip(rh_pct.iter())
                    .map(|(&t_k, &rh)| {
                        mixing_ratio_from_relative_humidity(
                            normalize_pressure_level_hpa(level),
                            t_k,
                            rh,
                        )
                    })
                    .collect(),
            ));
        }
        return Ok(out);
    }
    Err("missing pressure-level specific humidity/dewpoint/RH field for thermodynamics".into())
}

fn decode_height_levels(
    messages: &[Grib2Message],
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    if let Ok(levels) = collect_levels(messages, 0, 3, 5, 100) {
        return Ok(levels);
    }
    if let Ok(levels) = collect_levels(messages, 0, 3, 4, 100) {
        return Ok(levels
            .into_iter()
            .map(|(level, values)| {
                (
                    level,
                    values
                        .into_iter()
                        .map(|value| value * GEOPOTENTIAL_M2S2_TO_M)
                        .collect(),
                )
            })
            .collect());
    }
    Err("missing pressure-level height/geopotential field".into())
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

    if records.is_empty() {
        return Err(format!(
            "missing GRIB records for discipline={discipline} category={category} number={number} level_type={level_type}"
        )
        .into());
    }
    records.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(records)
}

fn common_isobaric_levels(
    base: &Vec<(f64, Vec<f64>)>,
    others: &[&Vec<(f64, Vec<f64>)>],
) -> Vec<f64> {
    base.iter()
        .map(|(level, _)| *level)
        .filter(|&level| {
            others
                .iter()
                .all(|records| level_values(records, level).is_some())
        })
        .collect()
}

fn level_values<'a>(records: &'a [(f64, Vec<f64>)], level: f64) -> Option<&'a [f64]> {
    records
        .iter()
        .find(|(candidate, _)| (*candidate - level).abs() < 0.25)
        .map(|(_, values)| values.as_slice())
}

fn pressure_grid_shape_from_messages(
    messages: &[Grib2Message],
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let mut matching = messages.iter().filter(|msg| msg.product.level_type == 100);
    let sample = matching
        .next()
        .ok_or("pressure family had no isobaric GRIB messages")?;
    let nx = sample.grid.nx as usize;
    let ny = sample.grid.ny as usize;
    for message in matching {
        let message_nx = message.grid.nx as usize;
        let message_ny = message.grid.ny as usize;
        if message_nx != nx || message_ny != ny {
            return Err("pressure family contained inconsistent grid shapes".into());
        }
    }
    Ok((nx, ny))
}

fn find_message<'a>(
    messages: &'a [Grib2Message],
    candidates: &[(u8, u8, u8, u8, Option<f64>)],
) -> Result<&'a Grib2Message, Box<dyn std::error::Error>> {
    for &(discipline, category, number, level_type, level_value) in candidates {
        if let Some(message) = messages.iter().find(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
                && level_value
                    .map(|level| (msg.product.level_value - level).abs() < 0.25)
                    .unwrap_or(true)
        }) {
            return Ok(message);
        }
    }
    Err("missing GRIB message for requested candidates".into())
}

fn q_to_mixing_ratio(values: &[f64]) -> Vec<f64> {
    values
        .iter()
        .map(|&q| (q / (1.0 - q).max(1.0e-12)).max(1.0e-10))
        .collect()
}

fn mixing_ratio_from_dewpoint_k(pressure_hpa: f64, dewpoint_k: f64) -> f64 {
    let td_c = dewpoint_k - 273.15;
    let vapor_pressure_hpa = 6.112 * ((17.67 * td_c) / (td_c + 243.5)).exp();
    mixing_ratio_from_vapor_pressure(pressure_hpa, vapor_pressure_hpa)
}

fn mixing_ratio_from_relative_humidity(pressure_hpa: f64, temperature_k: f64, rh_pct: f64) -> f64 {
    let t_c = temperature_k - 273.15;
    let saturation_vapor_pressure_hpa = 6.112 * ((17.67 * t_c) / (t_c + 243.5)).exp();
    let vapor_pressure_hpa = (rh_pct / 100.0).clamp(0.0, 1.5) * saturation_vapor_pressure_hpa;
    mixing_ratio_from_vapor_pressure(pressure_hpa, vapor_pressure_hpa)
}

fn mixing_ratio_from_vapor_pressure(pressure_hpa: f64, vapor_pressure_hpa: f64) -> f64 {
    let epsilon = 0.622;
    let e = vapor_pressure_hpa
        .max(0.0)
        .min((pressure_hpa - 1.0).max(0.0));
    (epsilon * e / (pressure_hpa - e).max(1.0e-6)).max(1.0e-10)
}

fn normalize_pressure_level_hpa(level: f64) -> f64 {
    if level > 2_000.0 {
        level / 100.0
    } else {
        level
    }
}

fn validate_pressure_decode_against_surface(
    decoded: &CachedDecode<PressureFields>,
    decoded_shape: Option<(usize, usize)>,
    nx: usize,
    ny: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((found_nx, found_ny)) = decoded_shape {
        if found_nx != nx || found_ny != ny {
            return Err(format!(
                "pressure decode shape {found_nx}x{found_ny} did not match surface shape {nx}x{ny}"
            )
            .into());
        }
    }
    let expected = nx * ny * decoded.value.pressure_levels_hpa.len();
    if decoded.value.temperature_c_3d.len() != expected
        || decoded.value.qvapor_kgkg_3d.len() != expected
        || decoded.value.u_ms_3d.len() != expected
        || decoded.value.v_ms_3d.len() != expected
        || decoded.value.gh_m_3d.len() != expected
    {
        return Err("pressure decode fields did not match the surface grid shape".into());
    }
    Ok(())
}

fn normalize_longitude(lon: f64) -> f64 {
    if lon > 180.0 { lon - 360.0 } else { lon }
}

fn normalize_longitude_rows(lat: &mut [f64], lon: &mut [f64], nx: usize, ny: usize) {
    if nx == 0 || ny == 0 {
        return;
    }

    for row in 0..ny {
        let start = row * nx;
        let end = start + nx;
        let lat_row = &mut lat[start..end];
        let lon_row = &mut lon[start..end];
        for lon_value in lon_row.iter_mut() {
            *lon_value = normalize_longitude(*lon_value);
        }
        if let Some(wrap_idx) = first_longitude_wrap(lon_row) {
            lat_row.rotate_left(wrap_idx);
            lon_row.rotate_left(wrap_idx);
        }
    }
}

fn first_longitude_wrap(lon_row: &[f64]) -> Option<usize> {
    lon_row
        .windows(2)
        .position(|pair| pair[1] < pair[0])
        .map(|idx| idx + 1)
}

impl LoadedModelTimestep {
    pub fn grid(&self) -> &LatLonGrid {
        &self.grid
    }

    pub fn shared_timing(&self) -> &SharedTiming {
        &self.shared_timing
    }

    fn with_cache_flags(mut self) -> Self {
        self.shared_timing.fetch_surface_cache_hit = self.surface_file.fetched.cache_hit;
        self.shared_timing.fetch_pressure_cache_hit = self.pressure_file.fetched.cache_hit;
        self.shared_timing.decode_surface_cache_hit = self.surface_decode.cache_hit;
        self.shared_timing.decode_pressure_cache_hit = self.pressure_decode.cache_hit;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hrrr_defaults_to_split_surface_and_pressure_products() {
        let (surface, pressure) = thermo_bundles(ModelId::Hrrr, None, None);
        assert_eq!(surface.bundle, CanonicalBundleDescriptor::SurfaceAnalysis);
        assert_eq!(surface.family, CanonicalDataFamily::Surface);
        assert_eq!(surface.native_product, "sfc");
        assert_eq!(pressure.bundle, CanonicalBundleDescriptor::PressureAnalysis);
        assert_eq!(pressure.family, CanonicalDataFamily::Pressure);
        assert_eq!(pressure.native_product, "prs");
    }

    #[test]
    fn global_models_default_to_single_full_family_product() {
        let (gfs_surface, gfs_pressure) = thermo_bundles(ModelId::Gfs, None, None);
        assert_eq!(gfs_surface.native_product, "pgrb2.0p25");
        assert_eq!(gfs_pressure.native_product, "pgrb2.0p25");

        let (ecmwf_surface, ecmwf_pressure) = thermo_bundles(ModelId::EcmwfOpenData, None, None);
        assert_eq!(ecmwf_surface.native_product, "oper");
        assert_eq!(ecmwf_pressure.native_product, "oper");

        let (rrfs_surface, rrfs_pressure) = thermo_bundles(ModelId::RrfsA, None, None);
        assert_eq!(rrfs_surface.native_product, "prs-conus");
        assert_eq!(rrfs_pressure.native_product, "prs-conus");
    }

    #[test]
    fn product_overrides_replace_defaults() {
        let (surface, pressure) = thermo_bundles(ModelId::RrfsA, Some("prs-na"), Some("prs-na"));
        assert_eq!(surface.native_product, "prs-na");
        assert_eq!(pressure.native_product, "prs-na");
    }

    #[test]
    fn mixing_ratio_fallbacks_produce_positive_values() {
        let dewpoint = mixing_ratio_from_dewpoint_k(1000.0, 293.15);
        let rh = mixing_ratio_from_relative_humidity(1000.0, 298.15, 65.0);
        assert!(dewpoint > 0.0);
        assert!(rh > 0.0);
    }
}
