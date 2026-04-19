use crate::cache::{load_bincode, store_bincode};
use crate::direct::build_projected_map;
use crate::shared_context::PreparedProjectedContext;
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
    LatestRun, ResolvedCanonicalBundleProduct, latest_available_run_at_forecast_hour,
    latest_available_run_for_products_at_forecast_hour, resolve_canonical_bundle_product,
};
use rustwx_render::{ProjectedExtent, map_frame_aspect_ratio};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;

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
pub struct PreparedHeavyVolume {
    pub grid: CalcGridShape,
    pub shape: VolumeShape,
    pub pressure_levels_pa: Vec<f64>,
    pub pressure_3d_pa: Option<Vec<f64>>,
    pub height_agl_3d: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct LoadedSurfaceGeometry {
    pub latest: LatestRun,
    pub model: ModelId,
    pub surface_bundle: ResolvedCanonicalBundleProduct,
    pub surface_file: FetchedModelFile,
    pub surface_decode: CachedDecode<SurfaceFields>,
    pub grid: LatLonGrid,
    pub fetch_ms: u128,
    pub decode_ms: u128,
}

pub fn resolve_model_run(
    model: ModelId,
    date: &str,
    cycle_override: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model,
            cycle: CycleSpec::new(date, hour)?,
            source,
        }),
        None => Ok(latest_available_run_at_forecast_hour(
            model,
            Some(source),
            date,
            forecast_hour,
        )?),
    }
}

pub fn resolve_thermo_pair_run(
    model: ModelId,
    date: &str,
    cycle_override: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model,
            cycle: CycleSpec::new(date, hour)?,
            source,
        }),
        None => {
            let (surface_bundle, pressure_bundle) =
                thermo_bundles(model, surface_product_override, pressure_product_override);
            let required_products = [
                surface_bundle.native_product.as_str(),
                pressure_bundle.native_product.as_str(),
            ];
            Ok(latest_available_run_for_products_at_forecast_hour(
                model,
                Some(source),
                date,
                &required_products,
                forecast_hour,
            )?)
        }
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
    let latest = resolve_model_run(
        model,
        date_yyyymmdd,
        cycle_override_utc,
        forecast_hour,
        source,
    )?;
    load_model_timestep_from_latest(
        latest,
        forecast_hour,
        surface_product_override,
        pressure_product_override,
        cache_root,
        use_cache,
    )
}

pub fn load_surface_geometry_from_latest(
    latest: LatestRun,
    forecast_hour: u16,
    surface_product_override: Option<&str>,
    cache_root: &Path,
    use_cache: bool,
) -> Result<LoadedSurfaceGeometry, Box<dyn std::error::Error>> {
    let surface_bundle = resolve_canonical_bundle_product(
        latest.model,
        CanonicalBundleDescriptor::SurfaceAnalysis,
        surface_product_override,
    );
    let fetch_start = Instant::now();
    let surface_file = fetch_family_file(
        latest.model,
        latest.cycle.clone(),
        forecast_hour,
        latest.source,
        &surface_bundle,
        cache_root,
        use_cache,
    )?;
    let fetch_ms = fetch_start.elapsed().as_millis();
    let decode_start = Instant::now();
    let surface_decode = load_or_decode_surface(
        &decode_cache_path(cache_root, &surface_file.request, "surface"),
        surface_file.bytes.as_slice(),
        use_cache,
    )?;
    let decode_ms = decode_start.elapsed().as_millis();
    let grid = surface_decode.value.core_grid()?;
    let model = latest.model;
    Ok(LoadedSurfaceGeometry {
        latest,
        model,
        surface_bundle,
        surface_file,
        surface_decode,
        grid,
        fetch_ms,
        decode_ms,
    })
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
            let fetched = fetch_family_file_with_patterns(
                model,
                latest.cycle.clone(),
                forecast_hour,
                latest.source,
                &surface_bundle,
                merge_variable_patterns([
                    bundle_fetch_variable_patterns(
                        model,
                        surface_bundle.bundle,
                        &surface_bundle.native_product,
                    ),
                    bundle_fetch_variable_patterns(
                        model,
                        pressure_bundle.bundle,
                        &pressure_bundle.native_product,
                    ),
                ]),
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
    let mut context = PreparedProjectedContext::new();
    for &(width, height) in sizes {
        if width == 0 || height == 0 || context.contains_size(width, height) {
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
        context.insert(width, height, projected);
    }
    Ok(context)
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

/// Subrectangle of a generic surface/pressure grid that the heavy
/// compute kernels can operate on. Used to keep ECAPE/severe runs fast
/// on regional renders that only need a fraction of the source domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridCrop {
    pub x_start: usize,
    pub x_end: usize,
    pub y_start: usize,
    pub y_end: usize,
}

impl GridCrop {
    pub fn width(self) -> usize {
        self.x_end - self.x_start
    }

    pub fn height(self) -> usize {
        self.y_end - self.y_start
    }
}

/// Cropped surface+pressure pair plus the recomputed `LatLonGrid`. The
/// generic loader returns full-domain decoded fields; lane runners that
/// know they only need a regional slice (mainly `hrrr_batch` for
/// severe/ECAPE on a small bounding box) crop with this helper.
#[derive(Debug, Clone)]
pub struct CroppedHeavyDomain {
    pub surface: SurfaceFields,
    pub pressure: PressureFields,
    pub grid: LatLonGrid,
}

pub fn crop_heavy_domain(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    bounds: (f64, f64, f64, f64),
) -> Result<Option<CroppedHeavyDomain>, Box<dyn std::error::Error>> {
    let Some(crop) = crop_rect_for_bounds(surface, bounds)? else {
        return Ok(None);
    };
    let cropped_surface = crop_surface_fields(surface, crop);
    let cropped_pressure = crop_pressure_fields(pressure, surface.nx, surface.ny, crop)?;
    let grid = cropped_surface.core_grid()?;
    Ok(Some(CroppedHeavyDomain {
        surface: cropped_surface,
        pressure: cropped_pressure,
        grid,
    }))
}

pub fn crop_heavy_domain_for_projected_extent(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    projected_x: &[f64],
    projected_y: &[f64],
    extent: &ProjectedExtent,
    pad_cells: usize,
) -> Result<Option<CroppedHeavyDomain>, Box<dyn std::error::Error>> {
    let Some(crop) =
        crop_rect_for_projected_extent(surface, projected_x, projected_y, extent, pad_cells)?
    else {
        return Ok(None);
    };
    let cropped_surface = crop_surface_fields(surface, crop);
    let cropped_pressure = crop_pressure_fields(pressure, surface.nx, surface.ny, crop)?;
    let grid = cropped_surface.core_grid()?;
    Ok(Some(CroppedHeavyDomain {
        surface: cropped_surface,
        pressure: cropped_pressure,
        grid,
    }))
}

fn crop_rect_for_bounds(
    surface: &SurfaceFields,
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

fn crop_rect_for_projected_extent(
    surface: &SurfaceFields,
    projected_x: &[f64],
    projected_y: &[f64],
    extent: &ProjectedExtent,
    pad_cells: usize,
) -> Result<Option<GridCrop>, Box<dyn std::error::Error>> {
    let expected_len = surface.nx * surface.ny;
    if projected_x.len() != expected_len || projected_y.len() != expected_len {
        return Err("projected crop inputs did not match surface grid size".into());
    }

    let mut min_x = surface.nx;
    let mut max_x = 0usize;
    let mut min_y = surface.ny;
    let mut max_y = 0usize;
    let mut found = false;

    for y in 0..surface.ny {
        let row_offset = y * surface.nx;
        for x in 0..surface.nx {
            let idx = row_offset + x;
            let px = projected_x[idx];
            let py = projected_y[idx];
            if px >= extent.x_min && px <= extent.x_max && py >= extent.y_min && py <= extent.y_max
            {
                min_x = min_x.min(x);
                max_x = max_x.max(x);
                min_y = min_y.min(y);
                max_y = max_y.max(y);
                found = true;
            }
        }
    }

    if !found {
        return Err("requested projected crop produced an empty heavy-compute domain".into());
    }

    let crop = GridCrop {
        x_start: min_x.saturating_sub(pad_cells),
        x_end: (max_x + 1 + pad_cells).min(surface.nx),
        y_start: min_y.saturating_sub(pad_cells),
        y_end: (max_y + 1 + pad_cells).min(surface.ny),
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

fn crop_surface_fields(surface: &SurfaceFields, crop: GridCrop) -> SurfaceFields {
    SurfaceFields {
        lat: crop_2d_values(&surface.lat, surface.nx, crop),
        lon: crop_2d_values(&surface.lon, surface.nx, crop),
        nx: crop.width(),
        ny: crop.height(),
        psfc_pa: crop_2d_values(&surface.psfc_pa, surface.nx, crop),
        orog_m: crop_2d_values(&surface.orog_m, surface.nx, crop),
        orog_is_proxy: surface.orog_is_proxy,
        t2_k: crop_2d_values(&surface.t2_k, surface.nx, crop),
        q2_kgkg: crop_2d_values(&surface.q2_kgkg, surface.nx, crop),
        u10_ms: crop_2d_values(&surface.u10_ms, surface.nx, crop),
        v10_ms: crop_2d_values(&surface.v10_ms, surface.nx, crop),
    }
}

fn crop_pressure_fields(
    pressure: &PressureFields,
    source_nx: usize,
    source_ny: usize,
    crop: GridCrop,
) -> Result<PressureFields, Box<dyn std::error::Error>> {
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

    Ok(PressureFields {
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

pub(crate) fn fetch_family_file(
    model: ModelId,
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    bundle: &ResolvedCanonicalBundleProduct,
    cache_root: &Path,
    use_cache: bool,
) -> Result<FetchedModelFile, Box<dyn std::error::Error>> {
    fetch_family_file_with_patterns(
        model,
        cycle,
        forecast_hour,
        source,
        bundle,
        bundle_fetch_variable_patterns(model, bundle.bundle, &bundle.native_product),
        cache_root,
        use_cache,
    )
}

pub(crate) fn fetch_family_file_with_patterns(
    model: ModelId,
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    bundle: &ResolvedCanonicalBundleProduct,
    variable_patterns: Vec<String>,
    cache_root: &Path,
    use_cache: bool,
) -> Result<FetchedModelFile, Box<dyn std::error::Error>> {
    let request = FetchRequest {
        request: ModelRunRequest::new(model, cycle, forecast_hour, &bundle.native_product)?,
        source_override: Some(source),
        variable_patterns,
    };
    let fetched = fetch_bytes_with_cache(&request, cache_root, use_cache)?;
    Ok(FetchedModelFile {
        request,
        bytes: fetched.result.bytes.clone(),
        fetched,
    })
}

pub(crate) fn bundle_fetch_variable_patterns(
    model: ModelId,
    bundle: CanonicalBundleDescriptor,
    native_product: &str,
) -> Vec<String> {
    if model != ModelId::RrfsA {
        return Vec::new();
    }

    match (bundle, native_product) {
        (CanonicalBundleDescriptor::SurfaceAnalysis, "nat-na") => vec![
            "PRES:surface",
            "HGT:surface",
            "GP:surface",
            "TMP:2 m above ground",
            "SPFH:2 m above ground",
            "DPT:2 m above ground",
            "RH:2 m above ground",
            "UGRD:10 m above ground",
            "VGRD:10 m above ground",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        (CanonicalBundleDescriptor::PressureAnalysis, "prs-na") => {
            vec!["HGT", "GP", "TMP", "SPFH", "DPT", "RH", "UGRD", "VGRD"]
                .into_iter()
                .map(str::to_string)
                .collect()
        }
        _ => Vec::new(),
    }
}

fn merge_variable_patterns(pattern_groups: impl IntoIterator<Item = Vec<String>>) -> Vec<String> {
    let mut merged = Vec::new();
    for group in pattern_groups {
        for pattern in group {
            if !merged.contains(&pattern) {
                merged.push(pattern);
            }
        }
    }
    merged
}

pub(crate) fn decode_cache_path(cache_root: &Path, fetch: &FetchRequest, name: &str) -> PathBuf {
    artifact_cache_dir(cache_root, fetch)
        .join("decoded")
        .join(format!("{name}.bin"))
}

pub(crate) fn load_or_decode_surface(
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

pub(crate) fn load_or_decode_pressure_with_shape(
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

// GRIB2 level type 100 (isobaric surface) values are always pascals; see the
// matching note in rustwx_io. The old "only divide when > 2000" heuristic
// aliased stratospheric Pa levels onto tropospheric hectopascal numbers, so
// GFS/RRFS-A moisture columns picked up bogus mid-level RH values.
fn normalize_pressure_level_hpa(level_value_pa: f64) -> f64 {
    level_value_pa / 100.0
}

pub(crate) fn validate_pressure_decode_against_surface(
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
        assert_eq!(rrfs_surface.native_product, "nat-na");
        assert_eq!(rrfs_pressure.native_product, "prs-na");
    }

    #[test]
    fn rrfs_thermo_bundle_fetch_patterns_use_idx_subsetting() {
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::RrfsA,
                CanonicalBundleDescriptor::SurfaceAnalysis,
                "nat-na"
            ),
            vec![
                "PRES:surface".to_string(),
                "HGT:surface".to_string(),
                "GP:surface".to_string(),
                "TMP:2 m above ground".to_string(),
                "SPFH:2 m above ground".to_string(),
                "DPT:2 m above ground".to_string(),
                "RH:2 m above ground".to_string(),
                "UGRD:10 m above ground".to_string(),
                "VGRD:10 m above ground".to_string(),
            ]
        );
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::RrfsA,
                CanonicalBundleDescriptor::PressureAnalysis,
                "prs-na"
            ),
            vec![
                "HGT".to_string(),
                "GP".to_string(),
                "TMP".to_string(),
                "SPFH".to_string(),
                "DPT".to_string(),
                "RH".to_string(),
                "UGRD".to_string(),
                "VGRD".to_string(),
            ]
        );
        assert!(
            bundle_fetch_variable_patterns(
                ModelId::Hrrr,
                CanonicalBundleDescriptor::SurfaceAnalysis,
                "sfc"
            )
            .is_empty()
        );
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

    #[test]
    fn projected_crop_uses_projected_extent_with_padding() {
        let nx = 4usize;
        let ny = 4usize;
        let len = nx * ny;
        let surface = SurfaceFields {
            lat: vec![35.0; len],
            lon: vec![-97.0; len],
            nx,
            ny,
            psfc_pa: vec![100000.0; len],
            orog_m: vec![300.0; len],
            orog_is_proxy: false,
            t2_k: vec![295.0; len],
            q2_kgkg: vec![0.012; len],
            u10_ms: vec![10.0; len],
            v10_ms: vec![5.0; len],
        };
        let pressure = PressureFields {
            pressure_levels_hpa: vec![1000.0],
            temperature_c_3d: vec![20.0; len],
            qvapor_kgkg_3d: vec![0.010; len],
            u_ms_3d: vec![10.0; len],
            v_ms_3d: vec![5.0; len],
            gh_m_3d: vec![1500.0; len],
        };
        let projected_x = vec![
            0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0,
        ];
        let projected_y = vec![
            0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0,
        ];
        let extent = ProjectedExtent {
            x_min: 1.0,
            x_max: 1.0,
            y_min: 1.0,
            y_max: 1.0,
        };

        let cropped = crop_heavy_domain_for_projected_extent(
            &surface,
            &pressure,
            &projected_x,
            &projected_y,
            &extent,
            1,
        )
        .expect("crop should succeed")
        .expect("crop should reduce to a padded subset");

        assert_eq!(cropped.surface.nx, 3);
        assert_eq!(cropped.surface.ny, 3);
        assert_eq!(cropped.grid.shape.nx, 3);
        assert_eq!(cropped.grid.shape.ny, 3);
    }
}
