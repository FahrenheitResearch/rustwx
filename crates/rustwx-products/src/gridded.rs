use crate::cache::{load_bincode, store_bincode};
use crate::direct::build_projected_map_with_projection;
use crate::shared_context::PreparedProjectedContext;
use grib_core::grib2::{
    Grib2File, Grib2Message, flip_rows, grid_latlon,
    unpack_message_normalized as unpack_message_scan_normalized,
};
use rayon::prelude::*;
use rustwx_calc::{GridShape as CalcGridShape, VolumeShape};
use rustwx_core::{
    CanonicalBundleDescriptor, CanonicalDataFamily, CycleSpec, GridProjection, GridShape,
    LatLonGrid, ModelId, ModelRunRequest, RustwxError, SourceId,
};
use rustwx_io::{
    CachedFetchResult, FetchRequest, artifact_cache_dir, earth2_archive, fetch_bytes_with_cache,
    grid_projection_from_grib2_grid,
};
use rustwx_models::{
    LatestRun, ResolvedCanonicalBundleProduct, latest_available_run_at_forecast_hour,
    latest_available_run_for_products_at_forecast_hour, resolve_canonical_bundle_product,
};
#[cfg(test)]
use rustwx_render::ProjectedExtent;
use rustwx_render::map_frame_aspect_ratio;
#[cfg(feature = "wrf")]
use rustwx_wrf as wrf;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;

const GEOPOTENTIAL_M2S2_TO_M: f64 = 1.0 / 9.806_65;
const MAX_DECODE_CACHE_WRITE_BYTES: usize = 512 * 1024 * 1024;
const PRESSURE_OPTIONAL_FIELDS_ENV: &str = "RUSTWX_PRESSURE_OPTIONAL_FIELDS";

mod crop;

pub use crop::{
    CroppedHeavyDomain, GridCrop, ProjectedGridIntersection, classify_projected_grid_intersection,
    crop_heavy_domain, crop_heavy_domain_for_projected_extent, crop_latlon_grid, crop_values_f32,
    crop_values_f64,
};
use crop::{
    crop_2d_values, crop_optional_2d_values, crop_rect_for_layout, cropped_decode_cache_path,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceFields {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
    pub nx: usize,
    pub ny: usize,
    pub projection: Option<GridProjection>,
    pub psfc_pa: Vec<f64>,
    pub orog_m: Vec<f64>,
    pub orog_is_proxy: bool,
    pub t2_k: Vec<f64>,
    pub q2_kgkg: Vec<f64>,
    pub u10_ms: Vec<f64>,
    pub v10_ms: Vec<f64>,
    pub native_sbcape_jkg: Option<Vec<f64>>,
    pub native_mlcape_jkg: Option<Vec<f64>>,
    pub native_mucape_jkg: Option<Vec<f64>>,
    pub native_pblh_m: Option<Vec<f64>>,
}

impl SurfaceFields {
    pub fn core_grid(&self) -> Result<LatLonGrid, RustwxError> {
        LatLonGrid::new(
            GridShape::new(self.nx, self.ny)?,
            self.lat.iter().map(|&v| v as f32).collect(),
            self.lon.iter().map(|&v| v as f32).collect(),
        )
    }

    pub fn decoded_bytes_estimate(&self) -> usize {
        let len = self.lat.len();
        let required_f64_fields = 8usize;
        let optional_f64_fields = [
            self.native_sbcape_jkg.as_ref(),
            self.native_mlcape_jkg.as_ref(),
            self.native_mucape_jkg.as_ref(),
            self.native_pblh_m.as_ref(),
        ]
        .into_iter()
        .filter(|field| field.is_some())
        .count();
        len * (required_f64_fields + optional_f64_fields) * std::mem::size_of::<f64>()
            + std::mem::size_of::<bool>()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SurfaceGridLayout {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
    pub nx: usize,
    pub ny: usize,
    pub projection: Option<GridProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PressureFields {
    pub pressure_levels_hpa: Vec<f64>,
    pub pressure_3d_pa: Option<Vec<f64>>,
    pub temperature_c_3d: Vec<f64>,
    pub qvapor_kgkg_3d: Vec<f64>,
    pub u_ms_3d: Vec<f64>,
    pub v_ms_3d: Vec<f64>,
    pub gh_m_3d: Vec<f64>,
    #[serde(default)]
    pub omega_pa_s_3d: Option<Vec<f64>>,
    #[serde(default)]
    pub absolute_vorticity_s_3d: Option<Vec<f64>>,
    #[serde(default)]
    pub cloud_liquid_kgkg_3d: Option<Vec<f64>>,
    #[serde(default)]
    pub cloud_ice_kgkg_3d: Option<Vec<f64>>,
    #[serde(default)]
    pub rain_kgkg_3d: Option<Vec<f64>>,
    #[serde(default)]
    pub snow_kgkg_3d: Option<Vec<f64>>,
    #[serde(default)]
    pub graupel_kgkg_3d: Option<Vec<f64>>,
}

impl PressureFields {
    pub fn decoded_bytes_estimate(&self) -> usize {
        let level_count = self.pressure_levels_hpa.len();
        let volume_len = self.temperature_c_3d.len();
        let pressure_3d_len = self
            .pressure_3d_pa
            .as_ref()
            .map(|values| values.len())
            .unwrap_or(0);
        let optional_volume_len: usize = [
            self.omega_pa_s_3d.as_ref(),
            self.absolute_vorticity_s_3d.as_ref(),
            self.cloud_liquid_kgkg_3d.as_ref(),
            self.cloud_ice_kgkg_3d.as_ref(),
            self.rain_kgkg_3d.as_ref(),
            self.snow_kgkg_3d.as_ref(),
            self.graupel_kgkg_3d.as_ref(),
        ]
        .into_iter()
        .map(|values| values.map(Vec::len).unwrap_or(0))
        .sum();
        level_count * std::mem::size_of::<f64>()
            + (volume_len * 5usize + pressure_3d_len + optional_volume_len)
                * std::mem::size_of::<f64>()
    }
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PreparedHeavyVolumeTiming {
    pub prepare_height_agl_ms: u128,
    pub broadcast_pressure_ms: u128,
    pub pressure_3d_bytes: usize,
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

pub fn load_model_timestep_from_parts_cropped(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
    cache_root: &Path,
    use_cache: bool,
    bounds: (f64, f64, f64, f64),
) -> Result<LoadedModelTimestep, Box<dyn std::error::Error>> {
    let latest = resolve_model_run(
        model,
        date_yyyymmdd,
        cycle_override_utc,
        forecast_hour,
        source,
    )?;
    load_model_timestep_from_latest_cropped(
        latest,
        forecast_hour,
        surface_product_override,
        pressure_product_override,
        cache_root,
        use_cache,
        bounds,
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

    let ((mut surface_file, fetch_surface_ms), (mut pressure_file, fetch_pressure_ms)) =
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
            let ((surface_result, fetch_surface_ms), (pressure_result, fetch_pressure_ms)) =
                fetch_surface_pressure_files_parallel(
                    model,
                    latest.cycle.clone(),
                    forecast_hour,
                    latest.source,
                    &surface_bundle,
                    &pressure_bundle,
                    cache_root,
                    use_cache,
                );
            let surface = surface_result?;
            let pressure = pressure_result?;
            ((surface, fetch_surface_ms), (pressure, fetch_pressure_ms))
        };

    let surface_cache_path = decode_cache_path(cache_root, &surface_file.request, "surface");
    let pressure_cache_path = decode_cache_path(
        cache_root,
        &pressure_file.request,
        pressure_decode_cache_name(),
    );
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
    surface_file.bytes.clear();
    surface_file.bytes.shrink_to_fit();
    pressure_file.bytes.clear();
    pressure_file.bytes.shrink_to_fit();
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

pub fn load_model_timestep_from_latest_cropped(
    latest: LatestRun,
    forecast_hour: u16,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
    cache_root: &Path,
    use_cache: bool,
    bounds: (f64, f64, f64, f64),
) -> Result<LoadedModelTimestep, Box<dyn std::error::Error>> {
    let model = latest.model;
    let (surface_bundle, pressure_bundle) =
        thermo_bundles(model, surface_product_override, pressure_product_override);

    let ((mut surface_file, fetch_surface_ms), (mut pressure_file, fetch_pressure_ms)) =
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
            let ((surface_result, fetch_surface_ms), (pressure_result, fetch_pressure_ms)) =
                fetch_surface_pressure_files_parallel(
                    model,
                    latest.cycle.clone(),
                    forecast_hour,
                    latest.source,
                    &surface_bundle,
                    &pressure_bundle,
                    cache_root,
                    use_cache,
                );
            let surface = surface_result?;
            let pressure = pressure_result?;
            ((surface, fetch_surface_ms), (pressure, fetch_pressure_ms))
        };

    let surface_layout = decode_surface_grid(surface_file.bytes.as_slice())?;
    let crop = crop_rect_for_layout(&surface_layout, bounds)?
        .ok_or("requested cropped load produced an empty domain")?;
    let surface_cache_path =
        cropped_decode_cache_path(cache_root, &surface_file.request, "surface", crop);
    let pressure_cache_path = cropped_decode_cache_path(
        cache_root,
        &pressure_file.request,
        pressure_decode_cache_name(),
        crop,
    );

    let decode_surface_start = Instant::now();
    let surface_decode = load_or_decode_surface_cropped(
        &surface_cache_path,
        surface_file.bytes.as_slice(),
        use_cache,
        crop,
    )?;
    let decode_surface_ms = decode_surface_start.elapsed().as_millis();

    let decode_pressure_start = Instant::now();
    let (pressure_decode, pressure_shape) = load_or_decode_pressure_cropped_with_shape(
        &pressure_cache_path,
        pressure_file.bytes.as_slice(),
        use_cache,
        crop,
    )?;
    let decode_pressure_ms = decode_pressure_start.elapsed().as_millis();

    validate_pressure_decode_against_surface(
        &pressure_decode,
        pressure_shape,
        surface_decode.value.nx,
        surface_decode.value.ny,
    )?;
    surface_file.bytes.clear();
    surface_file.bytes.shrink_to_fit();
    pressure_file.bytes.clear();
    pressure_file.bytes.shrink_to_fit();
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
        let projected = build_projected_map_with_projection(
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
            surface.projection.as_ref(),
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
    let (prepared, _) = prepare_heavy_volume_timed(surface, pressure, include_pressure_3d)?;
    Ok(prepared)
}

pub fn prepare_heavy_volume_timed(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    include_pressure_3d: bool,
) -> Result<(PreparedHeavyVolume, PreparedHeavyVolumeTiming), Box<dyn std::error::Error>> {
    let grid = CalcGridShape::new(surface.nx, surface.ny)?;
    let shape = VolumeShape::new(grid, pressure.pressure_levels_hpa.len())?;
    let pressure_levels_pa = pressure
        .pressure_levels_hpa
        .iter()
        .map(|level_hpa| level_hpa * 100.0)
        .collect::<Vec<_>>();
    let height_agl_start = Instant::now();
    let height_agl_3d = compute_height_agl_3d(surface, pressure, grid, shape);
    let prepare_height_agl_ms = height_agl_start.elapsed().as_millis();
    let broadcast_start = Instant::now();
    let pressure_3d_pa = include_pressure_3d.then(|| {
        pressure
            .pressure_3d_pa
            .clone()
            .unwrap_or_else(|| broadcast_levels_pa(&pressure.pressure_levels_hpa, grid.len()))
    });
    let broadcast_pressure_ms = if include_pressure_3d {
        broadcast_start.elapsed().as_millis()
    } else {
        0
    };
    let pressure_3d_bytes = pressure_3d_pa
        .as_ref()
        .map(|values| values.len() * std::mem::size_of::<f64>())
        .unwrap_or(0);
    Ok((
        PreparedHeavyVolume {
            grid,
            shape,
            pressure_levels_pa,
            pressure_3d_pa,
            height_agl_3d,
        },
        PreparedHeavyVolumeTiming {
            prepare_height_agl_ms,
            broadcast_pressure_ms,
            pressure_3d_bytes,
        },
    ))
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

fn fetch_surface_pressure_files_parallel(
    model: ModelId,
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    surface_bundle: &ResolvedCanonicalBundleProduct,
    pressure_bundle: &ResolvedCanonicalBundleProduct,
    cache_root: &Path,
    use_cache: bool,
) -> (
    (Result<FetchedModelFile, std::io::Error>, u128),
    (Result<FetchedModelFile, std::io::Error>, u128),
) {
    let surface_cycle = cycle.clone();
    let pressure_cycle = cycle;
    rayon::join(
        || {
            let start = Instant::now();
            let result = fetch_family_file(
                model,
                surface_cycle,
                forecast_hour,
                source,
                surface_bundle,
                cache_root,
                use_cache,
            )
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()));
            (result, start.elapsed().as_millis())
        },
        || {
            let start = Instant::now();
            let result = fetch_family_file(
                model,
                pressure_cycle,
                forecast_hour,
                source,
                pressure_bundle,
                cache_root,
                use_cache,
            )
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()));
            (result, start.elapsed().as_millis())
        },
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
        earth2_ensemble: None,
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
    match (bundle, native_product) {
        (CanonicalBundleDescriptor::SurfaceAnalysis, _) if model_has_grib_surface_bundle(model) => {
            surface_analysis_fetch_patterns(model)
        }
        (CanonicalBundleDescriptor::PressureAnalysis, _)
            if model_has_grib_pressure_bundle(model) =>
        {
            pressure_analysis_fetch_patterns(model)
        }
        (CanonicalBundleDescriptor::NativeAnalysis, "nat-na") => vec![
            "CAPE:surface",
            "CIN:surface",
            "LFTX:500-1000 mb",
            "CAPE:90-0 mb above ground",
            "CIN:90-0 mb above ground",
            "CAPE:255-0 mb above ground",
            "CIN:255-0 mb above ground",
            "HGT:cloud base",
            "PRES:cloud base",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        (CanonicalBundleDescriptor::NativeAnalysis, "sfc") if matches!(model, ModelId::Hrrr) => {
            vec![
                "PRES:surface",
                "APCP:surface",
                "TMP:2 m above ground",
                "DPT:2 m above ground",
                "RH:2 m above ground",
                "UGRD:10 m above ground",
                "VGRD:10 m above ground",
                "GUST:surface",
                "GUST:10 m above ground",
                "MSLMA:mean sea level",
                "PRMSL:mean sea level",
                "MSLET:mean sea level",
                "LCDC:low cloud layer",
                "MCDC:middle cloud layer",
                "HCDC:high cloud layer",
                "MXUPHL:5000-2000 m above ground",
                "WIND:10 m above ground",
                "VIS:surface",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        }
        _ => Vec::new(),
    }
}

fn model_has_grib_surface_bundle(model: ModelId) -> bool {
    matches!(
        model,
        ModelId::Hrrr
            | ModelId::HrrrAk
            | ModelId::Gfs
            | ModelId::Gdas
            | ModelId::Gefs
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Sref
            | ModelId::Rtma
            | ModelId::Urma
            | ModelId::Nbm
            | ModelId::RrfsA
    )
}

fn model_has_grib_pressure_bundle(model: ModelId) -> bool {
    matches!(
        model,
        ModelId::Hrrr
            | ModelId::HrrrAk
            | ModelId::Gfs
            | ModelId::Gdas
            | ModelId::Gefs
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Sref
            | ModelId::RrfsA
    )
}

fn surface_analysis_fetch_patterns(model: ModelId) -> Vec<String> {
    let mut patterns = vec![
        "PRES:surface",
        "HGT:surface",
        "GP:surface",
        "TMP:2 m above ground",
        "SPFH:2 m above ground",
        "UGRD:10 m above ground",
        "VGRD:10 m above ground",
    ];
    if matches!(
        model,
        ModelId::Gfs
            | ModelId::Gdas
            | ModelId::Gefs
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Sref
            | ModelId::Rtma
            | ModelId::Urma
            | ModelId::Nbm
            | ModelId::RrfsA
    ) {
        patterns.extend(["DPT:2 m above ground", "RH:2 m above ground"]);
    }
    patterns.into_iter().map(str::to_string).collect()
}

fn pressure_analysis_fetch_patterns(model: ModelId) -> Vec<String> {
    let patterns = match model {
        ModelId::Hrrr | ModelId::HrrrAk => {
            hrrr_pressure_analysis_fetch_patterns(pressure_optional_decode_enabled())
        }
        // RAP pressure products stay on the full family fetch in production;
        // old subset attempts could omit pressure-level wind records required
        // by the generic pressure decoder.
        ModelId::Rap => Vec::new(),
        ModelId::Gfs
        | ModelId::Gdas
        | ModelId::Gefs
        | ModelId::Aigfs
        | ModelId::Aigefs
        | ModelId::Nam
        | ModelId::Hiresw
        | ModelId::Sref => vec!["HGT", "TMP", "RH", "UGRD", "VGRD"],
        ModelId::RrfsA => vec!["HGT", "GP", "TMP", "SPFH", "DPT", "RH", "UGRD", "VGRD"],
        _ => Vec::new(),
    };
    patterns.into_iter().map(str::to_string).collect()
}

fn hrrr_pressure_analysis_fetch_patterns(include_optional: bool) -> Vec<&'static str> {
    let mut patterns = vec!["HGT", "TMP", "SPFH", "UGRD", "VGRD"];
    if include_optional {
        patterns.extend([
            "VVEL", "ABSV", "CLWMR", "CIMIXR", "ICMR", "RWMR", "SNMR", "GRLE",
        ]);
    }
    patterns
}

fn merge_variable_patterns(pattern_groups: impl IntoIterator<Item = Vec<String>>) -> Vec<String> {
    let mut merged = Vec::new();
    for group in pattern_groups {
        if group.is_empty() {
            return Vec::new();
        }
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
    if use_cache && decoded.decoded_bytes_estimate() <= MAX_DECODE_CACHE_WRITE_BYTES {
        store_bincode(path, &decoded)?;
    }
    Ok(CachedDecode {
        value: decoded,
        cache_hit: false,
        path: path.to_path_buf(),
    })
}

pub(crate) fn load_or_decode_surface_from_file(
    path: &Path,
    file: &FetchedModelFile,
    use_cache: bool,
) -> Result<CachedDecode<SurfaceFields>, Box<dyn std::error::Error>> {
    if !earth2_archive::is_earth2_archive_fetch(&file.request) {
        return load_or_decode_surface(path, file.bytes.as_slice(), use_cache);
    }
    if use_cache {
        if let Some(cached) = load_bincode::<SurfaceFields>(path)? {
            return Ok(CachedDecode {
                value: cached,
                cache_hit: true,
                path: path.to_path_buf(),
            });
        }
    }
    let decoded = decode_surface_from_earth2_file(file)?;
    if use_cache && decoded.decoded_bytes_estimate() <= MAX_DECODE_CACHE_WRITE_BYTES {
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
    if use_cache && decoded.decoded_bytes_estimate() <= MAX_DECODE_CACHE_WRITE_BYTES {
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

pub(crate) fn load_or_decode_pressure_from_file_with_shape(
    path: &Path,
    file: &FetchedModelFile,
    use_cache: bool,
) -> Result<(CachedDecode<PressureFields>, Option<(usize, usize)>), Box<dyn std::error::Error>> {
    if !earth2_archive::is_earth2_archive_fetch(&file.request) {
        return load_or_decode_pressure_with_shape(path, file.bytes.as_slice(), use_cache);
    }
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
    let (decoded, nx, ny) = decode_pressure_from_earth2_file(file)?;
    if use_cache && decoded.decoded_bytes_estimate() <= MAX_DECODE_CACHE_WRITE_BYTES {
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

pub(crate) fn decode_surface_grid(
    bytes: &[u8],
) -> Result<SurfaceGridLayout, Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let sample = file
        .messages
        .first()
        .ok_or("surface family GRIB had no messages")?;
    Ok(decode_surface_grid_from_sample(sample))
}

pub(crate) fn load_or_decode_surface_cropped(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
    crop: GridCrop,
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
    let decoded = decode_surface_cropped(bytes, crop)?;
    if use_cache && decoded.decoded_bytes_estimate() <= MAX_DECODE_CACHE_WRITE_BYTES {
        store_bincode(path, &decoded)?;
    }
    Ok(CachedDecode {
        value: decoded,
        cache_hit: false,
        path: path.to_path_buf(),
    })
}

pub(crate) fn load_or_decode_pressure_cropped_with_shape(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
    crop: GridCrop,
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
    let (decoded, nx, ny) = decode_pressure_cropped_with_shape(bytes, crop)?;
    if use_cache && decoded.decoded_bytes_estimate() <= MAX_DECODE_CACHE_WRITE_BYTES {
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

fn decode_surface_from_earth2_file(
    file: &FetchedModelFile,
) -> Result<SurfaceFields, Box<dyn std::error::Error>> {
    let decoded = if file.fetched.bytes_path.is_file() {
        earth2_archive::decode_surface_from_path(&file.fetched.bytes_path)?
    } else {
        earth2_archive::decode_surface_from_bytes(&file.bytes)?
    };
    Ok(surface_fields_from_earth2(decoded))
}

fn surface_fields_from_earth2(decoded: earth2_archive::Earth2SurfaceFields) -> SurfaceFields {
    SurfaceFields {
        lat: decoded.lat,
        lon: decoded.lon,
        nx: decoded.nx,
        ny: decoded.ny,
        projection: Some(GridProjection::Geographic),
        psfc_pa: decoded.psfc_pa,
        orog_m: decoded.orog_m,
        orog_is_proxy: true,
        t2_k: decoded.t2_k,
        q2_kgkg: decoded.q2_kgkg,
        u10_ms: decoded.u10_ms,
        v10_ms: decoded.v10_ms,
        native_sbcape_jkg: None,
        native_mlcape_jkg: None,
        native_mucape_jkg: None,
        native_pblh_m: None,
    }
}

fn decode_pressure_from_earth2_file(
    file: &FetchedModelFile,
) -> Result<(PressureFields, usize, usize), Box<dyn std::error::Error>> {
    let decoded = if file.fetched.bytes_path.is_file() {
        earth2_archive::decode_pressure_from_path(&file.fetched.bytes_path)?
    } else {
        earth2_archive::decode_pressure_from_bytes(&file.bytes)?
    };
    Ok(pressure_fields_from_earth2(decoded))
}

fn pressure_fields_from_earth2(
    decoded: earth2_archive::Earth2PressureFields,
) -> (PressureFields, usize, usize) {
    (
        PressureFields {
            pressure_levels_hpa: decoded.pressure_levels_hpa,
            pressure_3d_pa: None,
            temperature_c_3d: decoded.temperature_c_3d,
            qvapor_kgkg_3d: decoded.qvapor_kgkg_3d,
            u_ms_3d: decoded.u_ms_3d,
            v_ms_3d: decoded.v_ms_3d,
            gh_m_3d: decoded.gh_m_3d,
            omega_pa_s_3d: decoded.omega_pa_s_3d,
            absolute_vorticity_s_3d: None,
            cloud_liquid_kgkg_3d: None,
            cloud_ice_kgkg_3d: None,
            rain_kgkg_3d: None,
            snow_kgkg_3d: None,
            graupel_kgkg_3d: None,
        },
        decoded.nx,
        decoded.ny,
    )
}

fn decode_surface(bytes: &[u8]) -> Result<SurfaceFields, Box<dyn std::error::Error>> {
    #[cfg(feature = "wrf")]
    if wrf::looks_like_wrf(bytes) {
        let decoded = wrf::decode_surface_from_bytes(bytes, None)?;
        return Ok(SurfaceFields {
            lat: decoded.lat,
            lon: decoded.lon,
            nx: decoded.nx,
            ny: decoded.ny,
            projection: None,
            psfc_pa: decoded.psfc_pa,
            orog_m: decoded.orog_m,
            orog_is_proxy: false,
            t2_k: decoded.t2_k,
            q2_kgkg: decoded.q2_kgkg,
            u10_ms: decoded.u10_ms,
            v10_ms: decoded.v10_ms,
            native_sbcape_jkg: None,
            native_mlcape_jkg: None,
            native_mucape_jkg: None,
            native_pblh_m: None,
        });
    }
    if earth2_archive::looks_like_earth2_archive(bytes) {
        let decoded = earth2_archive::decode_surface_from_bytes(bytes)?;
        return Ok(surface_fields_from_earth2(decoded));
    }
    let file = Grib2File::from_bytes(bytes)?;
    let sample = file
        .messages
        .first()
        .ok_or("surface family GRIB had no messages")?;
    let SurfaceGridLayout {
        lat,
        lon,
        nx,
        ny,
        projection,
    } = decode_surface_grid_from_sample(sample);

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
    let native_sbcape_jkg = decode_optional_native_cape(&file.messages, NativeCapeLayer::Surface)?;
    let native_mlcape_jkg =
        decode_optional_native_cape(&file.messages, NativeCapeLayer::MixedLayer)?;
    let native_mucape_jkg =
        decode_optional_native_cape(&file.messages, NativeCapeLayer::MostUnstable)?;
    let native_pblh_m = decode_optional_native_pblh(&file.messages)?;

    Ok(SurfaceFields {
        lat,
        lon,
        nx,
        ny,
        projection,
        psfc_pa,
        orog_m,
        orog_is_proxy,
        t2_k,
        q2_kgkg,
        u10_ms,
        v10_ms,
        native_sbcape_jkg,
        native_mlcape_jkg,
        native_mucape_jkg,
        native_pblh_m,
    })
}

fn decode_surface_cropped(
    bytes: &[u8],
    crop: GridCrop,
) -> Result<SurfaceFields, Box<dyn std::error::Error>> {
    #[cfg(feature = "wrf")]
    if wrf::looks_like_wrf(bytes) {
        return Ok(crop::crop_surface_fields(&decode_surface(bytes)?, crop));
    }
    let file = Grib2File::from_bytes(bytes)?;
    let sample = file
        .messages
        .first()
        .ok_or("surface family GRIB had no messages")?;
    let SurfaceGridLayout {
        lat,
        lon,
        nx,
        ny: _,
        projection,
    } = decode_surface_grid_from_sample(sample);

    let psfc_pa = crop_2d_values(
        &unpack_message_normalized(find_message(
            &file.messages,
            &[(0, 3, 0, 1, Some(0.0)), (0, 3, 0, 1, None)],
        )?)?,
        nx,
        crop,
    );
    let (orog_m, orog_is_proxy) = match decode_orography(&file.messages) {
        Ok(values) => (crop_2d_values(&values, nx, crop), false),
        Err(_) => (vec![0.0; crop.width() * crop.height()], true),
    };
    let t2_k = crop_2d_values(
        &unpack_message_normalized(find_message(&file.messages, &[(0, 0, 0, 103, Some(2.0))])?)?,
        nx,
        crop,
    );
    let q2_kgkg = decode_surface_mixing_ratio_cropped(&file.messages, &psfc_pa, &t2_k, nx, crop)?;
    let u10_ms = crop_2d_values(
        &unpack_message_normalized(find_message(&file.messages, &[(0, 2, 2, 103, Some(10.0))])?)?,
        nx,
        crop,
    );
    let v10_ms = crop_2d_values(
        &unpack_message_normalized(find_message(&file.messages, &[(0, 2, 3, 103, Some(10.0))])?)?,
        nx,
        crop,
    );
    let native_sbcape_jkg = crop_optional_2d_values(
        &decode_optional_native_cape(&file.messages, NativeCapeLayer::Surface)?,
        nx,
        crop,
    );
    let native_mlcape_jkg = crop_optional_2d_values(
        &decode_optional_native_cape(&file.messages, NativeCapeLayer::MixedLayer)?,
        nx,
        crop,
    );
    let native_mucape_jkg = crop_optional_2d_values(
        &decode_optional_native_cape(&file.messages, NativeCapeLayer::MostUnstable)?,
        nx,
        crop,
    );
    let native_pblh_m =
        crop_optional_2d_values(&decode_optional_native_pblh(&file.messages)?, nx, crop);

    Ok(SurfaceFields {
        lat: crop_2d_values(&lat, nx, crop),
        lon: crop_2d_values(&lon, nx, crop),
        nx: crop.width(),
        ny: crop.height(),
        projection,
        psfc_pa,
        orog_m,
        orog_is_proxy,
        t2_k,
        q2_kgkg,
        u10_ms,
        v10_ms,
        native_sbcape_jkg,
        native_mlcape_jkg,
        native_mucape_jkg,
        native_pblh_m,
    })
}

fn decode_pressure_with_shape(
    bytes: &[u8],
) -> Result<(PressureFields, usize, usize), Box<dyn std::error::Error>> {
    #[cfg(feature = "wrf")]
    if wrf::looks_like_wrf(bytes) {
        let decoded = wrf::decode_pressure_from_bytes(bytes, None)?;
        return Ok((
            PressureFields {
                pressure_levels_hpa: decoded.pressure_levels_hpa,
                pressure_3d_pa: Some(decoded.pressure_3d_pa),
                temperature_c_3d: decoded.temperature_c_3d,
                qvapor_kgkg_3d: decoded.qvapor_kgkg_3d,
                u_ms_3d: decoded.u_ms_3d,
                v_ms_3d: decoded.v_ms_3d,
                gh_m_3d: decoded.gh_m_3d,
                omega_pa_s_3d: None,
                absolute_vorticity_s_3d: None,
                cloud_liquid_kgkg_3d: None,
                cloud_ice_kgkg_3d: None,
                rain_kgkg_3d: None,
                snow_kgkg_3d: None,
                graupel_kgkg_3d: None,
            },
            decoded.nx,
            decoded.ny,
        ));
    }
    if earth2_archive::looks_like_earth2_archive(bytes) {
        let decoded = earth2_archive::decode_pressure_from_bytes(bytes)?;
        return Ok(pressure_fields_from_earth2(decoded));
    }
    let file = Grib2File::from_bytes(bytes)?;
    let (nx, ny) = pressure_grid_shape_from_messages(&file.messages)?;
    let temperature = collect_levels(&file.messages, 0, 0, 0, 100)?;
    let u_wind = collect_levels(&file.messages, 0, 2, 2, 100)?;
    let v_wind = collect_levels(&file.messages, 0, 2, 3, 100)?;
    let gh = decode_height_levels(&file.messages)?;
    let moisture = decode_pressure_mixing_ratio_levels(&file.messages, &temperature)?;
    let include_optional = pressure_optional_decode_enabled();
    let omega = if include_optional {
        collect_optional_levels(&file.messages, 0, 2, 8, 100)?
    } else {
        None
    };
    let absolute_vorticity = if include_optional {
        collect_optional_levels(&file.messages, 0, 2, 10, 100)?
    } else {
        None
    };
    let cloud_liquid = if include_optional {
        collect_optional_levels(&file.messages, 0, 1, 22, 100)?
    } else {
        None
    };
    let cloud_ice = if include_optional {
        collect_optional_levels_any(&file.messages, &[(0, 1, 82), (0, 1, 23)], 100)?
    } else {
        None
    };
    let rain = if include_optional {
        collect_optional_levels(&file.messages, 0, 1, 24, 100)?
    } else {
        None
    };
    let snow = if include_optional {
        collect_optional_levels(&file.messages, 0, 1, 25, 100)?
    } else {
        None
    };
    let graupel = if include_optional {
        collect_optional_levels_any(&file.messages, &[(0, 1, 32), (0, 1, 74)], 100)?
    } else {
        None
    };

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
    let flatten_optional =
        |records: &Option<Vec<(f64, Vec<f64>)>>| -> Result<Option<Vec<f64>>, Box<dyn std::error::Error>> {
            let Some(records) = records.as_ref() else {
                return Ok(None);
            };
            let mut out = Vec::with_capacity(levels.len() * expected);
            for &level in &levels {
                let Some(values) = level_values(records, level) else {
                    return Ok(None);
                };
                if values.len() != expected {
                    return Ok(None);
                }
                out.extend_from_slice(values);
            }
            Ok(Some(out))
        };

    Ok((
        PressureFields {
            pressure_levels_hpa: aligned_levels
                .into_iter()
                .map(normalize_pressure_level_hpa)
                .collect(),
            pressure_3d_pa: None,
            temperature_c_3d: flatten(&temperature)?
                .into_iter()
                .map(|value| value - 273.15)
                .collect(),
            qvapor_kgkg_3d: flatten(&moisture)?,
            u_ms_3d: flatten(&u_wind)?,
            v_ms_3d: flatten(&v_wind)?,
            gh_m_3d: flatten(&gh)?,
            omega_pa_s_3d: flatten_optional(&omega)?,
            absolute_vorticity_s_3d: flatten_optional(&absolute_vorticity)?,
            cloud_liquid_kgkg_3d: flatten_optional(&cloud_liquid)?,
            cloud_ice_kgkg_3d: flatten_optional(&cloud_ice)?,
            rain_kgkg_3d: flatten_optional(&rain)?,
            snow_kgkg_3d: flatten_optional(&snow)?,
            graupel_kgkg_3d: flatten_optional(&graupel)?,
        },
        nx,
        ny,
    ))
}

fn decode_pressure_cropped_with_shape(
    bytes: &[u8],
    crop: GridCrop,
) -> Result<(PressureFields, usize, usize), Box<dyn std::error::Error>> {
    #[cfg(feature = "wrf")]
    if wrf::looks_like_wrf(bytes) {
        let (decoded, nx, ny) = decode_pressure_with_shape(bytes)?;
        return Ok((
            crop::crop_pressure_fields(&decoded, nx, ny, crop)?,
            crop.width(),
            crop.height(),
        ));
    }
    let file = Grib2File::from_bytes(bytes)?;
    let (nx, _ny) = pressure_grid_shape_from_messages(&file.messages)?;
    let temperature = collect_levels_cropped(&file.messages, 0, 0, 0, 100, nx, crop)?;
    let u_wind = collect_levels_cropped(&file.messages, 0, 2, 2, 100, nx, crop)?;
    let v_wind = collect_levels_cropped(&file.messages, 0, 2, 3, 100, nx, crop)?;
    let gh = decode_height_levels_cropped(&file.messages, nx, crop)?;
    let moisture =
        decode_pressure_mixing_ratio_levels_cropped(&file.messages, &temperature, nx, crop)?;
    let include_optional = pressure_optional_decode_enabled();
    let omega = if include_optional {
        collect_optional_levels_cropped(&file.messages, 0, 2, 8, 100, nx, crop)?
    } else {
        None
    };
    let absolute_vorticity = if include_optional {
        collect_optional_levels_cropped(&file.messages, 0, 2, 10, 100, nx, crop)?
    } else {
        None
    };
    let cloud_liquid = if include_optional {
        collect_optional_levels_cropped(&file.messages, 0, 1, 22, 100, nx, crop)?
    } else {
        None
    };
    let cloud_ice = if include_optional {
        collect_optional_levels_any_cropped(
            &file.messages,
            &[(0, 1, 82), (0, 1, 23)],
            100,
            nx,
            crop,
        )?
    } else {
        None
    };
    let rain = if include_optional {
        collect_optional_levels_cropped(&file.messages, 0, 1, 24, 100, nx, crop)?
    } else {
        None
    };
    let snow = if include_optional {
        collect_optional_levels_cropped(&file.messages, 0, 1, 25, 100, nx, crop)?
    } else {
        None
    };
    let graupel = if include_optional {
        collect_optional_levels_any_cropped(
            &file.messages,
            &[(0, 1, 32), (0, 1, 74)],
            100,
            nx,
            crop,
        )?
    } else {
        None
    };

    let levels = common_isobaric_levels(&temperature, &[&moisture, &u_wind, &v_wind, &gh]);
    if levels.is_empty() {
        return Err("pressure family had no common thermodynamic levels".into());
    }

    let expected = crop.width() * crop.height();
    let flatten = |records: &Vec<(f64, Vec<f64>)>| -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        let mut out = Vec::with_capacity(levels.len() * expected);
        for &level in &levels {
            let values = level_values(records, level)
                .ok_or_else(|| format!("missing aligned pressure level {level}"))?;
            if values.len() != expected {
                return Err("decoded cropped pressure field had unexpected grid size".into());
            }
            out.extend_from_slice(values);
        }
        Ok(out)
    };
    let flatten_optional =
        |records: &Option<Vec<(f64, Vec<f64>)>>| -> Result<Option<Vec<f64>>, Box<dyn std::error::Error>> {
            let Some(records) = records.as_ref() else {
                return Ok(None);
            };
            let mut out = Vec::with_capacity(levels.len() * expected);
            for &level in &levels {
                let Some(values) = level_values(records, level) else {
                    return Ok(None);
                };
                if values.len() != expected {
                    return Ok(None);
                }
                out.extend_from_slice(values);
            }
            Ok(Some(out))
        };

    let pressure_levels_hpa = levels
        .iter()
        .copied()
        .map(normalize_pressure_level_hpa)
        .collect();

    Ok((
        PressureFields {
            pressure_levels_hpa,
            pressure_3d_pa: None,
            temperature_c_3d: flatten(&temperature)?
                .into_iter()
                .map(|value| value - 273.15)
                .collect(),
            qvapor_kgkg_3d: flatten(&moisture)?,
            u_ms_3d: flatten(&u_wind)?,
            v_ms_3d: flatten(&v_wind)?,
            gh_m_3d: flatten(&gh)?,
            omega_pa_s_3d: flatten_optional(&omega)?,
            absolute_vorticity_s_3d: flatten_optional(&absolute_vorticity)?,
            cloud_liquid_kgkg_3d: flatten_optional(&cloud_liquid)?,
            cloud_ice_kgkg_3d: flatten_optional(&cloud_ice)?,
            rain_kgkg_3d: flatten_optional(&rain)?,
            snow_kgkg_3d: flatten_optional(&snow)?,
            graupel_kgkg_3d: flatten_optional(&graupel)?,
        },
        crop.width(),
        crop.height(),
    ))
}

fn decode_surface_grid_from_sample(sample: &Grib2Message) -> SurfaceGridLayout {
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
    SurfaceGridLayout {
        lat: lat_raw,
        lon: lon_raw
            .into_iter()
            .map(normalize_longitude)
            .collect::<Vec<_>>(),
        nx: sample.grid.nx as usize,
        ny: sample.grid.ny as usize,
        projection: grid_projection_from_grib2_grid(&sample.grid),
    }
}

fn unpack_message_normalized(
    message: &Grib2Message,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let mut values = unpack_message_scan_normalized(message)?;
    rotate_values_to_normalized_longitude_rows(message, &mut values);
    Ok(values)
}

fn rotate_values_to_normalized_longitude_rows(message: &Grib2Message, values: &mut [f64]) {
    let nx = message.grid.nx as usize;
    let ny = message.grid.ny as usize;
    if nx == 0 || ny == 0 || values.len() != nx * ny {
        return;
    }

    let (_lat_raw, mut lon_raw) = grid_latlon(&message.grid);
    if message.grid.scan_mode & 0x40 != 0 {
        flip_rows(&mut lon_raw, nx, ny);
    }
    for row in 0..ny {
        let start = row * nx;
        let end = start + nx;
        let lon_row = &mut lon_raw[start..end];
        for lon_value in lon_row.iter_mut() {
            *lon_value = normalize_longitude(*lon_value);
        }
        if let Some(wrap_idx) = first_longitude_wrap(lon_row) {
            values[start..end].rotate_left(wrap_idx);
        }
    }
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

fn decode_surface_mixing_ratio_cropped(
    messages: &[Grib2Message],
    psfc_pa: &[f64],
    t2_k: &[f64],
    source_nx: usize,
    crop: GridCrop,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    if let Ok(message) = find_message(messages, &[(0, 1, 0, 103, Some(2.0))]) {
        let values = unpack_message_normalized(message)?;
        return Ok(q_to_mixing_ratio(&crop_2d_values(&values, source_nx, crop)));
    }
    if let Ok(message) = find_message(messages, &[(0, 0, 6, 103, Some(2.0))]) {
        let dewpoint_k = crop_2d_values(&unpack_message_normalized(message)?, source_nx, crop);
        return Ok(psfc_pa
            .iter()
            .zip(dewpoint_k.iter())
            .map(|(&psfc, &td_k)| mixing_ratio_from_dewpoint_k(psfc / 100.0, td_k))
            .collect());
    }
    if let Ok(message) = find_message(messages, &[(0, 1, 1, 103, Some(2.0))]) {
        let rh_pct = crop_2d_values(&unpack_message_normalized(message)?, source_nx, crop);
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

fn decode_pressure_mixing_ratio_levels_cropped(
    messages: &[Grib2Message],
    temperature: &Vec<(f64, Vec<f64>)>,
    source_nx: usize,
    crop: GridCrop,
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    if let Ok(levels) = collect_levels_cropped(messages, 0, 1, 0, 100, source_nx, crop) {
        return Ok(levels
            .into_iter()
            .map(|(level, values)| (level, q_to_mixing_ratio(&values)))
            .collect());
    }
    if let Ok(dewpoint) = collect_levels_cropped(messages, 0, 0, 6, 100, source_nx, crop) {
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
    if let Ok(rh) = collect_levels_cropped(messages, 0, 1, 1, 100, source_nx, crop) {
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

fn decode_height_levels_cropped(
    messages: &[Grib2Message],
    source_nx: usize,
    crop: GridCrop,
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    if let Ok(levels) = collect_levels_cropped(messages, 0, 3, 5, 100, source_nx, crop) {
        return Ok(levels);
    }
    if let Ok(levels) = collect_levels_cropped(messages, 0, 3, 4, 100, source_nx, crop) {
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
        .par_iter()
        .filter(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
        })
        .map(|msg| {
            unpack_message_normalized(msg)
                .map(|values| (msg.product.level_value, values))
                .map_err(|err| err.to_string())
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(|err| std::io::Error::other(err))?;

    if records.is_empty() {
        return Err(format!(
            "missing GRIB records for discipline={discipline} category={category} number={number} level_type={level_type}"
        )
        .into());
    }
    records.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(records)
}

fn collect_optional_levels(
    messages: &[Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
) -> Result<Option<Vec<(f64, Vec<f64>)>>, Box<dyn std::error::Error>> {
    match collect_levels(messages, discipline, category, number, level_type) {
        Ok(records) => Ok(Some(records)),
        Err(_) => Ok(None),
    }
}

fn collect_optional_levels_any(
    messages: &[Grib2Message],
    candidates: &[(u8, u8, u8)],
    level_type: u8,
) -> Result<Option<Vec<(f64, Vec<f64>)>>, Box<dyn std::error::Error>> {
    for &(discipline, category, number) in candidates {
        if let Some(records) =
            collect_optional_levels(messages, discipline, category, number, level_type)?
        {
            return Ok(Some(records));
        }
    }
    Ok(None)
}

fn collect_levels_cropped(
    messages: &[Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
    source_nx: usize,
    crop: GridCrop,
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    let mut records = messages
        .par_iter()
        .filter(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
        })
        .map(|msg| {
            unpack_message_normalized(msg)
                .map(|values| {
                    (
                        msg.product.level_value,
                        crop_2d_values(&values, source_nx, crop),
                    )
                })
                .map_err(|err| err.to_string())
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(|err| std::io::Error::other(err))?;

    if records.is_empty() {
        return Err(format!(
            "missing GRIB records for discipline={discipline} category={category} number={number} level_type={level_type}"
        )
        .into());
    }
    records.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(records)
}

fn collect_optional_levels_cropped(
    messages: &[Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
    source_nx: usize,
    crop: GridCrop,
) -> Result<Option<Vec<(f64, Vec<f64>)>>, Box<dyn std::error::Error>> {
    match collect_levels_cropped(
        messages, discipline, category, number, level_type, source_nx, crop,
    ) {
        Ok(records) => Ok(Some(records)),
        Err(_) => Ok(None),
    }
}

fn collect_optional_levels_any_cropped(
    messages: &[Grib2Message],
    candidates: &[(u8, u8, u8)],
    level_type: u8,
    source_nx: usize,
    crop: GridCrop,
) -> Result<Option<Vec<(f64, Vec<f64>)>>, Box<dyn std::error::Error>> {
    for &(discipline, category, number) in candidates {
        if let Some(records) = collect_optional_levels_cropped(
            messages, discipline, category, number, level_type, source_nx, crop,
        )? {
            return Ok(Some(records));
        }
    }
    Ok(None)
}

fn pressure_optional_decode_enabled() -> bool {
    pressure_optional_decode_enabled_from_env_value(
        std::env::var(PRESSURE_OPTIONAL_FIELDS_ENV).ok(),
    )
}

fn pressure_decode_cache_name() -> &'static str {
    pressure_decode_cache_name_from_optional_enabled(pressure_optional_decode_enabled())
}

fn pressure_decode_cache_name_from_optional_enabled(include_optional: bool) -> &'static str {
    if include_optional {
        "pressure_optional"
    } else {
        "pressure_core"
    }
}

fn pressure_optional_decode_enabled_from_env_value(value: Option<String>) -> bool {
    let Some(value) = value else {
        return true;
    };
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    )
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

#[derive(Debug, Clone, Copy)]
enum NativeCapeLayer {
    Surface,
    MixedLayer,
    MostUnstable,
}

impl NativeCapeLayer {
    fn candidates(self) -> &'static [(u8, u8, u8, u8, Option<f64>)] {
        match self {
            Self::Surface => &[(0, 7, 6, 1, Some(0.0))],
            Self::MixedLayer => &[(0, 7, 6, 108, Some(9000.0))],
            Self::MostUnstable => &[(0, 7, 6, 108, Some(25500.0))],
        }
    }
}

fn decode_optional_native_cape(
    messages: &[Grib2Message],
    layer: NativeCapeLayer,
) -> Result<Option<Vec<f64>>, Box<dyn std::error::Error>> {
    let Some(message) = find_optional_message(messages, layer.candidates()) else {
        return Ok(None);
    };
    Ok(Some(unpack_message_normalized(message)?))
}

fn decode_optional_native_pblh(
    messages: &[Grib2Message],
) -> Result<Option<Vec<f64>>, Box<dyn std::error::Error>> {
    let candidates = &[
        (0, 3, 18, 1, Some(0.0)),
        (0, 3, 18, 1, None),
        (0, 3, 196, 1, Some(0.0)),
        (0, 3, 196, 1, None),
    ];
    let Some(message) = find_optional_message(messages, candidates) else {
        return Ok(None);
    };
    Ok(Some(unpack_message_normalized(message)?))
}

fn find_optional_message<'a>(
    messages: &'a [Grib2Message],
    candidates: &[(u8, u8, u8, u8, Option<f64>)],
) -> Option<&'a Grib2Message> {
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
            return Some(message);
        }
    }
    None
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
    for (name, values) in [
        ("omega_pa_s_3d", decoded.value.omega_pa_s_3d.as_ref()),
        (
            "absolute_vorticity_s_3d",
            decoded.value.absolute_vorticity_s_3d.as_ref(),
        ),
        (
            "cloud_liquid_kgkg_3d",
            decoded.value.cloud_liquid_kgkg_3d.as_ref(),
        ),
        (
            "cloud_ice_kgkg_3d",
            decoded.value.cloud_ice_kgkg_3d.as_ref(),
        ),
        ("rain_kgkg_3d", decoded.value.rain_kgkg_3d.as_ref()),
        ("snow_kgkg_3d", decoded.value.snow_kgkg_3d.as_ref()),
        ("graupel_kgkg_3d", decoded.value.graupel_kgkg_3d.as_ref()),
    ] {
        if let Some(values) = values {
            if values.len() != expected {
                return Err(format!(
                    "pressure decode optional field {name} did not match the surface grid shape"
                )
                .into());
            }
        }
    }
    Ok(())
}

fn normalize_longitude(lon: f64) -> f64 {
    if lon > 180.0 { lon - 360.0 } else { lon }
}

fn point_in_geographic_bounds(lon: f64, lat: f64, bounds: (f64, f64, f64, f64)) -> bool {
    if !lon.is_finite() || !lat.is_finite() || lat < bounds.2 || lat > bounds.3 {
        return false;
    }
    let west = normalize_longitude_for_bounds(bounds.0);
    let east = normalize_longitude_for_bounds(bounds.1);
    let lon = normalize_longitude_for_bounds(lon);
    if west <= east {
        lon >= west && lon <= east
    } else {
        lon >= west || lon <= east
    }
}

fn normalize_longitude_for_bounds(lon: f64) -> f64 {
    let mut lon = lon % 360.0;
    if lon > 180.0 {
        lon -= 360.0;
    } else if lon <= -180.0 {
        lon += 360.0;
    }
    lon
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
    fn thermo_bundle_fetch_patterns_use_idx_subsetting() {
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::RrfsA,
                CanonicalBundleDescriptor::SurfaceAnalysis,
                "nat-na"
            ),
            surface_analysis_fetch_patterns(ModelId::RrfsA)
        );
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::RrfsA,
                CanonicalBundleDescriptor::PressureAnalysis,
                "prs-na"
            ),
            pressure_analysis_fetch_patterns(ModelId::RrfsA)
        );
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::Hrrr,
                CanonicalBundleDescriptor::SurfaceAnalysis,
                "sfc"
            ),
            surface_analysis_fetch_patterns(ModelId::Hrrr)
        );
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::Hrrr,
                CanonicalBundleDescriptor::PressureAnalysis,
                "prs"
            ),
            pressure_analysis_fetch_patterns(ModelId::Hrrr)
        );
        assert_eq!(
            bundle_fetch_variable_patterns(
                ModelId::Gfs,
                CanonicalBundleDescriptor::PressureAnalysis,
                "pgrb2.0p25"
            ),
            pressure_analysis_fetch_patterns(ModelId::Gfs)
        );
    }

    #[test]
    fn hrrr_native_surface_patterns_include_visibility() {
        let patterns = bundle_fetch_variable_patterns(
            ModelId::Hrrr,
            CanonicalBundleDescriptor::NativeAnalysis,
            "sfc",
        );
        assert!(patterns.contains(&"VIS:surface".to_string()));
    }

    #[test]
    fn rap_pressure_bundle_uses_full_family_fetch() {
        assert!(
            bundle_fetch_variable_patterns(
                ModelId::Rap,
                CanonicalBundleDescriptor::PressureAnalysis,
                "awp130pgrb",
            )
            .is_empty()
        );
    }

    #[test]
    fn rap_same_file_surface_pressure_merge_uses_full_fetch() {
        assert!(
            merge_variable_patterns([
                surface_analysis_fetch_patterns(ModelId::Rap),
                pressure_analysis_fetch_patterns(ModelId::Rap),
            ])
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
    fn pressure_optional_decode_env_parser_defaults_on() {
        assert!(pressure_optional_decode_enabled_from_env_value(None));
        assert!(pressure_optional_decode_enabled_from_env_value(Some(
            "1".to_string()
        )));
        assert!(pressure_optional_decode_enabled_from_env_value(Some(
            "true".to_string()
        )));
        assert!(!pressure_optional_decode_enabled_from_env_value(Some(
            "0".to_string()
        )));
        assert!(!pressure_optional_decode_enabled_from_env_value(Some(
            "false".to_string()
        )));
        assert!(!pressure_optional_decode_enabled_from_env_value(Some(
            " off ".to_string()
        )));
    }

    #[test]
    fn hrrr_core_pressure_fetch_patterns_omit_optional_volumes() {
        let core = hrrr_pressure_analysis_fetch_patterns(false);
        assert_eq!(core, vec!["HGT", "TMP", "SPFH", "UGRD", "VGRD"]);

        let full = hrrr_pressure_analysis_fetch_patterns(true);
        assert!(full.contains(&"VVEL"));
        assert!(full.contains(&"ABSV"));
        assert!(full.contains(&"CLWMR"));
        assert!(full.len() > core.len());
    }

    #[test]
    fn pressure_decode_cache_name_includes_optional_policy() {
        assert_eq!(
            pressure_decode_cache_name_from_optional_enabled(false),
            "pressure_core"
        );
        assert_eq!(
            pressure_decode_cache_name_from_optional_enabled(true),
            "pressure_optional"
        );
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
            projection: Some(GridProjection::LambertConformal {
                standard_parallel_1_deg: 38.5,
                standard_parallel_2_deg: 38.5,
                central_meridian_deg: -97.5,
            }),
            psfc_pa: vec![100000.0; len],
            orog_m: vec![300.0; len],
            orog_is_proxy: false,
            t2_k: vec![295.0; len],
            q2_kgkg: vec![0.012; len],
            u10_ms: vec![10.0; len],
            v10_ms: vec![5.0; len],
            native_sbcape_jkg: None,
            native_mlcape_jkg: None,
            native_mucape_jkg: None,
            native_pblh_m: None,
        };
        let pressure = PressureFields {
            pressure_levels_hpa: vec![1000.0],
            pressure_3d_pa: None,
            temperature_c_3d: vec![20.0; len],
            qvapor_kgkg_3d: vec![0.010; len],
            u_ms_3d: vec![10.0; len],
            v_ms_3d: vec![5.0; len],
            gh_m_3d: vec![1500.0; len],
            omega_pa_s_3d: None,
            absolute_vorticity_s_3d: None,
            cloud_liquid_kgkg_3d: None,
            cloud_ice_kgkg_3d: None,
            rain_kgkg_3d: None,
            snow_kgkg_3d: None,
            graupel_kgkg_3d: None,
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
