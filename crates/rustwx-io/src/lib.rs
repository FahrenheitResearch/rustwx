mod cache;

pub use cache::{
    CachedFetchMetadata, CachedFetchResult, CachedFieldResult, artifact_cache_dir,
    fetch_cache_paths, field_cache_path, load_cached_fetch, load_cached_selected_field,
    store_cached_fetch, store_cached_selected_field,
};

use grib_core::grib2::{
    Grib2File, Grib2Message, GridDefinition, flip_rows, grid_latlon, unpack_message,
};
use rayon::prelude::*;
use rustwx_core::{
    CanonicalField, FieldSelector, GridProjection, GridShape, LatLonGrid, ModelId, ModelRunRequest,
    ModelTimestep, ResolvedUrl, SelectedField2D, SelectedHybridLevelVolume, SourceId,
    VerticalSelector,
};
use rustwx_models::{latest_available_run, model_summary, resolve_urls};
#[cfg(feature = "wrf")]
use rustwx_wrf as wrf;
use serde::Serialize;
use std::collections::HashSet;
use std::path::Path;
use thiserror::Error;
use wx_core::download::{DownloadClient, byte_ranges, find_entries, parse_idx};

#[derive(Debug, Error)]
pub enum IoError {
    #[error(transparent)]
    Core(#[from] rustwx_core::RustwxError),
    #[error(transparent)]
    Model(#[from] rustwx_models::ModelError),
    #[error("download client error: {0}")]
    Download(String),
    #[error("cache error: {0}")]
    Cache(String),
    #[error("grib error: {0}")]
    Grib(String),
    #[error("field '{selector}' was not found in GRIB data")]
    FieldNotFound { selector: FieldSelector },
    #[error("selector '{selector}' is not supported by structured GRIB extraction")]
    UnsupportedStructuredSelector { selector: FieldSelector },
    #[error("grid coordinates could not be derived for selector '{selector}'")]
    MissingGridCoordinates { selector: FieldSelector },
    #[error("wrf error: {0}")]
    Wrf(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProbeResult {
    pub source: SourceId,
    pub available: bool,
    pub grib_url: String,
    pub idx_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct FetchRequest {
    pub request: ModelRunRequest,
    pub source_override: Option<SourceId>,
    pub variable_patterns: Vec<String>,
}

impl FetchRequest {
    pub fn from_timestep<S, I, P>(
        timestep: &ModelTimestep,
        product: S,
        source_override: Option<SourceId>,
        variable_patterns: I,
    ) -> Result<Self, rustwx_core::RustwxError>
    where
        S: Into<String>,
        I: IntoIterator<Item = P>,
        P: Into<String>,
    {
        Ok(Self {
            request: timestep.request(product)?,
            source_override,
            variable_patterns: variable_patterns.into_iter().map(Into::into).collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct FetchResult {
    pub source: SourceId,
    pub url: String,
    pub bytes: Vec<u8>,
}

pub fn grid_projection_from_grib2_grid(grid: &GridDefinition) -> Option<GridProjection> {
    match grid.template {
        0 | 1 | 40 => Some(GridProjection::Geographic),
        10 => Some(GridProjection::Mercator {
            latitude_of_true_scale_deg: grid.latin1,
            central_meridian_deg: normalize_longitude(longitude_midpoint(grid.lon1, grid.lon2)),
        }),
        20 => Some(GridProjection::PolarStereographic {
            true_latitude_deg: if grid.lad != 0.0 {
                grid.lad
            } else {
                grid.latin1
            },
            central_meridian_deg: normalize_longitude(grid.lov),
            south_pole_on_projection_plane: (grid.projection_center_flag & 1) != 0,
        }),
        30 => Some(GridProjection::LambertConformal {
            standard_parallel_1_deg: grid.latin1,
            standard_parallel_2_deg: if grid.latin2 != 0.0 {
                grid.latin2
            } else {
                grid.latin1
            },
            central_meridian_deg: normalize_longitude(grid.lov),
        }),
        template => Some(GridProjection::Other { template }),
    }
}

pub fn client() -> Result<DownloadClient, IoError> {
    // rustwx owns fetch/decode caching through the explicit cache_root passed
    // into fetch_bytes_with_cache. Enabling wx-core's default cache here writes
    // duplicate GRIB bytes to platform locations such as ~/.cache/metrust, which
    // bypasses callers' storage controls on research nodes.
    DownloadClient::new().map_err(|err| IoError::Download(err.to_string()))
}

pub fn latest_run(
    model: ModelId,
    date_yyyymmdd: &str,
) -> Result<rustwx_models::LatestRun, IoError> {
    latest_available_run(model, None, date_yyyymmdd).map_err(Into::into)
}

pub fn probe_sources(fetch: &FetchRequest) -> Result<Vec<ProbeResult>, IoError> {
    let client = client()?;
    let urls = filtered_urls(fetch)?;
    Ok(urls
        .into_iter()
        .map(|resolved| {
            let available = probe_availability(&client, &resolved);
            ProbeResult {
                source: resolved.source,
                available,
                grib_url: resolved.grib_url,
                idx_url: resolved.idx_url,
            }
        })
        .collect())
}

pub fn available_forecast_hours(
    model: ModelId,
    date_yyyymmdd: &str,
    hour_utc: u8,
    product: &str,
    source_override: Option<SourceId>,
) -> Result<Vec<u16>, IoError> {
    let client = client()?;
    let candidates = candidate_hours(model, hour_utc);
    let summary = model_summary(model);

    let available = if should_parallelize_hour_availability_probes(source_override, summary) {
        candidates
            .par_iter()
            .filter_map(|&forecast_hour| {
                let cycle = rustwx_core::CycleSpec::new(date_yyyymmdd, hour_utc).ok()?;
                let fetch = FetchRequest {
                    request: ModelRunRequest::new(model, cycle, forecast_hour, product).ok()?,
                    source_override,
                    variable_patterns: Vec::new(),
                };
                if fetch_request_is_available(&client, &fetch).ok()? {
                    Some(forecast_hour)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    } else {
        candidates
            .iter()
            .filter_map(|&forecast_hour| {
                let cycle = rustwx_core::CycleSpec::new(date_yyyymmdd, hour_utc).ok()?;
                let fetch = FetchRequest {
                    request: ModelRunRequest::new(model, cycle, forecast_hour, product).ok()?,
                    source_override,
                    variable_patterns: Vec::new(),
                };
                if fetch_request_is_available(&client, &fetch).ok()? {
                    Some(forecast_hour)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };

    let mut available = available;
    available.sort_unstable();
    Ok(available)
}

pub fn fetch_bytes(fetch: &FetchRequest) -> Result<FetchResult, IoError> {
    let client = client()?;
    let urls = filtered_urls(fetch)?;
    let patterns = fetch
        .variable_patterns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let mut errors = Vec::new();
    for resolved in urls {
        match try_fetch_one(&client, &resolved, &patterns) {
            Ok(bytes) => {
                return Ok(FetchResult {
                    source: resolved.source,
                    url: resolved.grib_url,
                    bytes,
                });
            }
            Err(err) => errors.push(format!("{}: {}", resolved.source, err)),
        }
    }

    Err(IoError::Download(format!(
        "all sources failed for {} f{:03}: {}",
        fetch.request.model,
        fetch.request.forecast_hour,
        errors.join(" | ")
    )))
}

pub fn fetch_bytes_with_cache(
    fetch: &FetchRequest,
    cache_root: &std::path::Path,
    use_cache: bool,
) -> Result<CachedFetchResult, IoError> {
    if use_cache {
        if let Some(cached) = load_cached_fetch(cache_root, fetch)? {
            return Ok(cached);
        }
    }
    let result = fetch_bytes(fetch)?;
    if use_cache {
        store_cached_fetch(cache_root, fetch, &result)
    } else {
        let (bytes_path, metadata_path) = fetch_cache_paths(cache_root, fetch);
        Ok(CachedFetchResult {
            result,
            cache_hit: false,
            bytes_path,
            metadata_path,
        })
    }
}

pub fn extract_field_from_bytes(
    bytes: &[u8],
    selector: FieldSelector,
) -> Result<SelectedField2D, IoError> {
    let mut fields = extract_fields_from_bytes(bytes, &[selector])?;
    debug_assert_eq!(fields.len(), 1);
    Ok(fields.swap_remove(0))
}

pub fn extract_fields_from_bytes(
    bytes: &[u8],
    selectors: &[FieldSelector],
) -> Result<Vec<SelectedField2D>, IoError> {
    let grib = Grib2File::from_bytes(bytes).map_err(|err| IoError::Grib(err.to_string()))?;
    extract_fields_from_grib2(&grib, selectors)
}

pub fn extract_field_from_grib2(
    grib: &Grib2File,
    selector: FieldSelector,
) -> Result<SelectedField2D, IoError> {
    let mut fields = extract_fields_from_grib2(grib, &[selector])?;
    debug_assert_eq!(fields.len(), 1);
    Ok(fields.swap_remove(0))
}

pub fn extract_fields_from_grib2(
    grib: &Grib2File,
    selectors: &[FieldSelector],
) -> Result<Vec<SelectedField2D>, IoError> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let prepared = selectors
        .iter()
        .copied()
        .map(PreparedSelector::new)
        .collect::<Result<Vec<_>, _>>()?;
    let mut matched = vec![None; prepared.len()];
    let mut remaining = prepared.len();

    for message in &grib.messages {
        for (index, prepared_selector) in prepared.iter().enumerate() {
            if matched[index].is_none() && prepared_selector.message.matches(message) {
                matched[index] = Some(message);
                remaining -= 1;
            }
        }
        if remaining == 0 {
            break;
        }
    }

    let mut out = Vec::with_capacity(prepared.len());
    for (prepared_selector, message) in prepared.iter().zip(matched.into_iter()) {
        let message = message.ok_or(IoError::FieldNotFound {
            selector: prepared_selector.selector,
        })?;
        out.push(build_selected_field(
            message,
            prepared_selector.selector,
            prepared_selector.message.units,
        )?);
    }

    Ok(out)
}

/// Partial-success variant of `extract_fields_from_grib2`: selectors
/// whose GRIB message is absent from the file are returned in the
/// `missing` vector instead of erroring out. Callers that want per-
/// selector soft-fail (e.g. direct_batch, which renders many recipes
/// from one fetch and shouldn't abort the whole batch when one
/// selector is missing) opt into this variant; everyone else keeps
/// getting strict all-or-nothing semantics from the original function.
///
/// The only `Err` path here is a genuinely malformed selector or a
/// decode error on a matched message — neither of which is the "this
/// model doesn't expose that field at init time" case that the strict
/// variant treats identically.
pub fn extract_fields_from_grib2_partial(
    grib: &Grib2File,
    selectors: &[FieldSelector],
) -> Result<PartialExtraction, IoError> {
    let mut extracted = Vec::new();
    let mut missing = Vec::new();

    if selectors.is_empty() {
        return Ok(PartialExtraction { extracted, missing });
    }

    let prepared = selectors
        .iter()
        .copied()
        .map(PreparedSelector::new)
        .collect::<Result<Vec<_>, _>>()?;
    let mut matched = vec![None; prepared.len()];
    let mut remaining = prepared.len();

    for message in &grib.messages {
        for (index, prepared_selector) in prepared.iter().enumerate() {
            if matched[index].is_none() && prepared_selector.message.matches(message) {
                matched[index] = Some(message);
                remaining -= 1;
            }
        }
        if remaining == 0 {
            break;
        }
    }

    for (prepared_selector, message) in prepared.iter().zip(matched.into_iter()) {
        match message {
            Some(message) => extracted.push(build_selected_field(
                message,
                prepared_selector.selector,
                prepared_selector.message.units,
            )?),
            None => missing.push(prepared_selector.selector),
        }
    }

    Ok(PartialExtraction { extracted, missing })
}

/// Result of a partial extraction: every selector the GRIB file served
/// in `extracted`, every selector whose message was absent in `missing`.
#[derive(Debug, Clone)]
pub struct PartialExtraction {
    pub extracted: Vec<SelectedField2D>,
    pub missing: Vec<FieldSelector>,
}

pub fn extract_fields_partial_from_model_bytes(
    model: ModelId,
    bytes: &[u8],
    preferred_path: Option<&Path>,
    selectors: &[FieldSelector],
) -> Result<PartialExtraction, IoError> {
    match model {
        ModelId::WrfGdex => extract_wrf_gdex_fields_partial(bytes, preferred_path, selectors),
        _ => {
            let grib =
                Grib2File::from_bytes(bytes).map_err(|err| IoError::Grib(err.to_string()))?;
            extract_fields_from_grib2_partial(&grib, selectors)
        }
    }
}

#[cfg(feature = "wrf")]
fn extract_wrf_gdex_fields_partial(
    bytes: &[u8],
    preferred_path: Option<&Path>,
    selectors: &[FieldSelector],
) -> Result<PartialExtraction, IoError> {
    let partial = wrf::extract_selectors_partial_from_bytes(bytes, preferred_path, selectors)
        .map_err(|err| IoError::Wrf(err.to_string()))?;
    Ok(PartialExtraction {
        extracted: partial.extracted,
        missing: partial.missing,
    })
}

#[cfg(not(feature = "wrf"))]
fn extract_wrf_gdex_fields_partial(
    _bytes: &[u8],
    _preferred_path: Option<&Path>,
    _selectors: &[FieldSelector],
) -> Result<PartialExtraction, IoError> {
    Err(IoError::Wrf(
        "WRF/GDEX NetCDF support is not compiled; rebuild with --features wrf".to_string(),
    ))
}

pub fn extract_pressure_field_from_bytes(
    bytes: &[u8],
    field: CanonicalField,
    level_hpa: u16,
) -> Result<SelectedField2D, IoError> {
    extract_field_from_bytes(bytes, FieldSelector::isobaric(field, level_hpa))
}

pub fn extract_pressure_field_from_grib2(
    grib: &Grib2File,
    field: CanonicalField,
    level_hpa: u16,
) -> Result<SelectedField2D, IoError> {
    extract_field_from_grib2(grib, FieldSelector::isobaric(field, level_hpa))
}

pub const HRRR_WRFNAT_HYBRID_LEVEL_COUNT: u16 = 50;

#[derive(Debug, Clone, PartialEq)]
pub struct HrrrWrfnatSmokeExtraction {
    pub hybrid_smoke: SelectedHybridLevelVolume,
    pub hybrid_pressure: SelectedHybridLevelVolume,
    pub near_surface_smoke: SelectedField2D,
    pub column_smoke: SelectedField2D,
}

pub fn hrrr_wrfnat_hybrid_levels() -> Vec<u16> {
    (1..=HRRR_WRFNAT_HYBRID_LEVEL_COUNT).collect()
}

pub fn extract_hybrid_level_volume_from_bytes(
    bytes: &[u8],
    field: CanonicalField,
    levels_hybrid: &[u16],
) -> Result<SelectedHybridLevelVolume, IoError> {
    let grib = Grib2File::from_bytes(bytes).map_err(|err| IoError::Grib(err.to_string()))?;
    extract_hybrid_level_volume_from_grib2(&grib, field, levels_hybrid)
}

pub fn extract_hybrid_level_volume_from_grib2(
    grib: &Grib2File,
    field: CanonicalField,
    levels_hybrid: &[u16],
) -> Result<SelectedHybridLevelVolume, IoError> {
    let selectors = levels_hybrid
        .iter()
        .copied()
        .map(|level| FieldSelector::hybrid_level(field, level))
        .collect::<Vec<_>>();
    let slices = extract_fields_from_grib2(grib, &selectors)?;
    build_hybrid_level_volume(field, levels_hybrid, slices)
}

pub fn extract_hrrr_wrfnat_smoke_fields_from_bytes(
    bytes: &[u8],
) -> Result<HrrrWrfnatSmokeExtraction, IoError> {
    let grib = Grib2File::from_bytes(bytes).map_err(|err| IoError::Grib(err.to_string()))?;
    extract_hrrr_wrfnat_smoke_fields_from_grib2(&grib)
}

pub fn extract_hrrr_wrfnat_smoke_fields_from_grib2(
    grib: &Grib2File,
) -> Result<HrrrWrfnatSmokeExtraction, IoError> {
    let levels = hrrr_wrfnat_hybrid_levels();
    let hybrid_smoke =
        extract_hybrid_level_volume_from_grib2(grib, CanonicalField::SmokeMassDensity, &levels)?;
    let hybrid_pressure =
        extract_hybrid_level_volume_from_grib2(grib, CanonicalField::Pressure, &levels)?;
    let mut smoke_maps = extract_fields_from_grib2(
        grib,
        &[
            FieldSelector::height_agl(CanonicalField::SmokeMassDensity, 8),
            FieldSelector::entire_atmosphere(CanonicalField::ColumnIntegratedSmoke),
        ],
    )?;
    debug_assert_eq!(smoke_maps.len(), 2);
    let column_smoke = smoke_maps
        .pop()
        .expect("column smoke selector should be present after successful extraction");
    let near_surface_smoke = smoke_maps
        .pop()
        .expect("near-surface smoke selector should be present after successful extraction");

    Ok(HrrrWrfnatSmokeExtraction {
        hybrid_smoke,
        hybrid_pressure,
        near_surface_smoke,
        column_smoke,
    })
}

fn build_hybrid_level_volume(
    field: CanonicalField,
    levels_hybrid: &[u16],
    slices: Vec<SelectedField2D>,
) -> Result<SelectedHybridLevelVolume, IoError> {
    let Some(first) = slices.first() else {
        return Err(rustwx_core::RustwxError::EmptyHybridLevels.into());
    };

    let expected_grid = first.grid.clone();
    let expected_units = first.units.clone();
    let expected_projection = first.projection.clone();

    for slice in &slices {
        if slice.grid != expected_grid {
            return Err(IoError::Grib(format!(
                "hybrid volume for field '{field}' used inconsistent grids across levels"
            )));
        }
        if slice.units != expected_units {
            return Err(IoError::Grib(format!(
                "hybrid volume for field '{field}' used inconsistent units across levels"
            )));
        }
        if slice.projection != expected_projection {
            return Err(IoError::Grib(format!(
                "hybrid volume for field '{field}' used inconsistent projections across levels"
            )));
        }
    }

    let values = slices
        .into_iter()
        .flat_map(|slice| slice.values)
        .collect::<Vec<_>>();
    let mut volume = SelectedHybridLevelVolume::new(
        field,
        levels_hybrid.to_vec(),
        expected_units,
        expected_grid,
        values,
    )?;
    if let Some(projection) = expected_projection {
        volume = volume.with_projection(projection);
    }
    Ok(volume)
}

fn filtered_urls(fetch: &FetchRequest) -> Result<Vec<ResolvedUrl>, IoError> {
    let urls = resolve_urls(&fetch.request)?;
    Ok(match fetch.source_override {
        Some(source) => urls
            .into_iter()
            .filter(|url| url.source == source)
            .collect(),
        None => urls,
    })
}

fn fetch_request_is_available(
    client: &DownloadClient,
    fetch: &FetchRequest,
) -> Result<bool, IoError> {
    let urls = filtered_urls(fetch)?;
    Ok(any_source_available(&urls, |resolved| {
        probe_availability(client, resolved)
    }))
}

fn probe_availability(client: &DownloadClient, resolved: &ResolvedUrl) -> bool {
    if matches!(resolved.source, SourceId::Nomads) {
        client.get_range(&resolved.grib_url, 0, 0).is_ok()
    } else {
        client.head_ok(resolved.availability_probe_url())
    }
}

fn any_source_available<F>(resolved: &[ResolvedUrl], mut probe: F) -> bool
where
    F: FnMut(&ResolvedUrl) -> bool,
{
    resolved.iter().any(&mut probe)
}

fn should_parallelize_hour_availability_probes(
    source_override: Option<SourceId>,
    summary: &rustwx_models::ModelSummary,
) -> bool {
    match source_override {
        Some(source) => !matches!(source, SourceId::Nomads),
        None => summary
            .sources
            .iter()
            .all(|source| source.id != SourceId::Nomads),
    }
}

fn try_fetch_one(
    client: &DownloadClient,
    resolved: &ResolvedUrl,
    variable_patterns: &[&str],
) -> Result<Vec<u8>, String> {
    if !variable_patterns.is_empty() {
        if let Some(idx_url) = &resolved.idx_url {
            if let Ok(idx_text) = client.get_text(idx_url) {
                if let Some(ranges) = idx_subset_ranges(&idx_text, variable_patterns)? {
                    return client
                        .get_ranges(&resolved.grib_url, &ranges)
                        .map_err(|err| err.to_string());
                }
            }
        }
    }
    client
        .get_bytes(&resolved.grib_url)
        .map_err(|err| err.to_string())
}

fn idx_subset_ranges(idx_text: &str, patterns: &[&str]) -> Result<Option<Vec<(u64, u64)>>, String> {
    let entries = parse_idx(idx_text);
    if entries.is_empty() {
        return Ok(None);
    }

    let mut selected = Vec::new();
    let mut seen_offsets = HashSet::new();
    for pattern in patterns {
        for entry in find_entries(&entries, pattern) {
            if seen_offsets.insert(entry.byte_offset) {
                selected.push(entry);
            }
        }
    }

    if selected.is_empty() {
        return Ok(None);
    }
    Ok(Some(coalesce_contiguous_ranges(byte_ranges(
        &entries, &selected,
    ))))
}

fn coalesce_contiguous_ranges(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    if ranges.len() <= 1 {
        return ranges;
    }
    ranges.sort_unstable_by_key(|range| range.0);

    let mut merged = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        let Some((_, last_end)) = merged.last_mut() else {
            merged.push((start, end));
            continue;
        };
        if *last_end != u64::MAX && start <= last_end.saturating_add(1) {
            *last_end = (*last_end).max(end);
        } else {
            merged.push((start, end));
        }
    }
    merged
}

fn candidate_hours(model: ModelId, cycle_hour: u8) -> Vec<u16> {
    // Delegate to the canonical schedule in rustwx-models so availability
    // probes match the cycle-aware horizons that the catalog and fetch
    // plan already encode (e.g. ECMWF 00/12z goes to 360h, 06/18z to 144h).
    rustwx_models::supported_forecast_hours(model, cycle_hour)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParameterCode {
    discipline: u8,
    category: u8,
    number: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LevelMatch {
    Surface,
    MeanSeaLevel,
    IsobaricHpa(u16),
    HybridLevel(u16),
    EntireAtmosphere,
    NominalTop,
    ExactLevelType(u8),
    HeightAboveGroundMeters(u16),
    SurfaceOrHeightAboveGroundMeters(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StructuredMessageSelector {
    parameters: &'static [ParameterCode],
    level: LevelMatch,
    units: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct PreparedSelector {
    selector: FieldSelector,
    message: StructuredMessageSelector,
}

const PARAMETER_HGT: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 3,
    number: 5,
}];
const PARAMETER_PRESSURE: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 3,
    number: 0,
}];
const PARAMETER_TMP: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 0,
    number: 0,
}];
const PARAMETER_DPT: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 0,
    number: 6,
}];
const PARAMETER_RH: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 1,
    number: 1,
}];
const PARAMETER_PWAT: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 1,
    number: 3,
}];
const PARAMETER_TOTAL_PRECIPITATION: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 1,
    number: 8,
}];
const PARAMETER_CATEGORICAL_RAIN: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 192,
    },
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 33,
    },
];
const PARAMETER_CATEGORICAL_FREEZING_RAIN: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 193,
    },
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 34,
    },
];
const PARAMETER_CATEGORICAL_ICE_PELLETS: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 194,
    },
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 35,
    },
];
const PARAMETER_CATEGORICAL_SNOW: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 195,
    },
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 36,
    },
];
const PARAMETER_UGRD: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 2,
    number: 2,
}];
const PARAMETER_VGRD: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 2,
    number: 3,
}];
const PARAMETER_WIND_GUST: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 2,
    number: 22,
}];
// Only absolute vorticity is wired right now. Relative vorticity needs its own
// explicit selector and GRIB parameter mapping before it should be exposed.
const PARAMETER_ABSOLUTE_VORTICITY: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 2,
    number: 10,
}];
const PARAMETER_MSLP: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 3,
        number: 1,
    },
    ParameterCode {
        discipline: 0,
        category: 3,
        number: 192,
    },
    ParameterCode {
        discipline: 0,
        category: 3,
        number: 198,
    },
];
const PARAMETER_LANDSEA_MASK: &[ParameterCode] = &[ParameterCode {
    discipline: 2,
    category: 0,
    number: 0,
}];
const PARAMETER_TOTAL_CLOUD_COVER: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 6,
    number: 1,
}];
const PARAMETER_LOW_CLOUD_COVER: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 6,
    number: 3,
}];
const PARAMETER_MIDDLE_CLOUD_COVER: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 6,
    number: 4,
}];
const PARAMETER_HIGH_CLOUD_COVER: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 6,
    number: 5,
}];
const PARAMETER_VISIBILITY: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 19,
    number: 0,
}];
const PARAMETER_SIMULATED_IR: &[ParameterCode] = &[ParameterCode {
    discipline: 3,
    category: 192,
    number: 7,
}];
const PARAMETER_RADAR_REFLECTIVITY: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 16,
        number: 4,
    },
    ParameterCode {
        discipline: 0,
        category: 16,
        number: 195,
    },
];
const PARAMETER_COMPOSITE_REFLECTIVITY: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 16,
        number: 196,
    },
    ParameterCode {
        discipline: 0,
        category: 16,
        number: 5,
    },
    ParameterCode {
        discipline: 0,
        category: 1,
        number: 209,
    },
];
const PARAMETER_UPDRAFT_HELICITY: &[ParameterCode] = &[
    ParameterCode {
        discipline: 0,
        category: 7,
        number: 199,
    },
    ParameterCode {
        discipline: 0,
        category: 7,
        number: 15,
    },
];
const PARAMETER_SMOKE_MASS_DENSITY: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 20,
    number: 0,
}];
const PARAMETER_COLUMN_INTEGRATED_SMOKE: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 20,
    number: 1,
}];

impl StructuredMessageSelector {
    fn matches(self, message: &Grib2Message) -> bool {
        self.parameters.iter().any(|parameter| {
            message.discipline == parameter.discipline
                && message.product.parameter_category == parameter.category
                && message.product.parameter_number == parameter.number
        }) && self.level.matches(message)
    }
}

impl PreparedSelector {
    fn new(selector: FieldSelector) -> Result<Self, IoError> {
        Ok(Self {
            selector,
            message: StructuredMessageSelector::try_from(selector)?,
        })
    }
}

impl TryFrom<FieldSelector> for StructuredMessageSelector {
    type Error = IoError;

    fn try_from(selector: FieldSelector) -> Result<Self, Self::Error> {
        match selector {
            FieldSelector {
                field: CanonicalField::Pressure,
                vertical: VerticalSelector::HybridLevel(level),
            } if is_supported_hrrr_smoke_hybrid_level(level) => Ok(Self {
                parameters: PARAMETER_PRESSURE,
                level: LevelMatch::HybridLevel(level),
                units: "Pa",
            }),
            FieldSelector {
                field: CanonicalField::GeopotentialHeight,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if is_supported_upper_air_level(level_hpa) => Ok(Self {
                parameters: PARAMETER_HGT,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "gpm",
            }),
            FieldSelector {
                field: CanonicalField::Temperature,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if is_supported_upper_air_level(level_hpa) => Ok(Self {
                parameters: PARAMETER_TMP,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "K",
            }),
            FieldSelector {
                field: CanonicalField::RelativeHumidity,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if is_supported_upper_air_level(level_hpa) => Ok(Self {
                parameters: PARAMETER_RH,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "%",
            }),
            FieldSelector {
                field: CanonicalField::Dewpoint,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if matches!(level_hpa, 700 | 850) => Ok(Self {
                parameters: PARAMETER_DPT,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "K",
            }),
            FieldSelector {
                field: CanonicalField::Temperature,
                vertical: VerticalSelector::HeightAboveGroundMeters(2),
            } => Ok(Self {
                parameters: PARAMETER_TMP,
                level: LevelMatch::HeightAboveGroundMeters(2),
                units: "K",
            }),
            FieldSelector {
                field: CanonicalField::Dewpoint,
                vertical: VerticalSelector::HeightAboveGroundMeters(2),
            } => Ok(Self {
                parameters: PARAMETER_DPT,
                level: LevelMatch::HeightAboveGroundMeters(2),
                units: "K",
            }),
            FieldSelector {
                field: CanonicalField::RelativeHumidity,
                vertical: VerticalSelector::HeightAboveGroundMeters(2),
            } => Ok(Self {
                parameters: PARAMETER_RH,
                level: LevelMatch::HeightAboveGroundMeters(2),
                units: "%",
            }),
            FieldSelector {
                field: CanonicalField::SmokeMassDensity,
                vertical: VerticalSelector::HybridLevel(level),
            } if is_supported_hrrr_smoke_hybrid_level(level) => Ok(Self {
                parameters: PARAMETER_SMOKE_MASS_DENSITY,
                level: LevelMatch::HybridLevel(level),
                units: "kg/m^3",
            }),
            FieldSelector {
                field: CanonicalField::AbsoluteVorticity,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if is_supported_upper_air_level(level_hpa) => Ok(Self {
                parameters: PARAMETER_ABSOLUTE_VORTICITY,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "s^-1",
            }),
            FieldSelector {
                field: CanonicalField::UWind,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if is_supported_upper_air_level(level_hpa) => Ok(Self {
                parameters: PARAMETER_UGRD,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "m/s",
            }),
            FieldSelector {
                field: CanonicalField::VWind,
                vertical: VerticalSelector::IsobaricHpa(level_hpa),
            } if is_supported_upper_air_level(level_hpa) => Ok(Self {
                parameters: PARAMETER_VGRD,
                level: LevelMatch::IsobaricHpa(level_hpa),
                units: "m/s",
            }),
            FieldSelector {
                field: CanonicalField::UWind,
                vertical: VerticalSelector::HeightAboveGroundMeters(10),
            } => Ok(Self {
                parameters: PARAMETER_UGRD,
                level: LevelMatch::HeightAboveGroundMeters(10),
                units: "m/s",
            }),
            FieldSelector {
                field: CanonicalField::VWind,
                vertical: VerticalSelector::HeightAboveGroundMeters(10),
            } => Ok(Self {
                parameters: PARAMETER_VGRD,
                level: LevelMatch::HeightAboveGroundMeters(10),
                units: "m/s",
            }),
            FieldSelector {
                field: CanonicalField::WindGust,
                vertical: VerticalSelector::HeightAboveGroundMeters(10),
            } => Ok(Self {
                parameters: PARAMETER_WIND_GUST,
                // Operational gust products are often keyed as 10 m AGL in
                // product catalogs even when the GRIB metadata carries a
                // surface level type.
                level: LevelMatch::SurfaceOrHeightAboveGroundMeters(10),
                units: "m/s",
            }),
            FieldSelector {
                field: CanonicalField::SmokeMassDensity,
                vertical: VerticalSelector::HeightAboveGroundMeters(8),
            } => Ok(Self {
                parameters: PARAMETER_SMOKE_MASS_DENSITY,
                level: LevelMatch::HeightAboveGroundMeters(8),
                units: "kg/m^3",
            }),
            FieldSelector {
                field: CanonicalField::PressureReducedToMeanSeaLevel,
                vertical: VerticalSelector::MeanSeaLevel,
            } => Ok(Self {
                parameters: PARAMETER_MSLP,
                level: LevelMatch::MeanSeaLevel,
                units: "Pa",
            }),
            FieldSelector {
                field: CanonicalField::PrecipitableWater,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_PWAT,
                level: LevelMatch::EntireAtmosphere,
                units: "kg/m^2",
            }),
            FieldSelector {
                field: CanonicalField::ColumnIntegratedSmoke,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_COLUMN_INTEGRATED_SMOKE,
                level: LevelMatch::EntireAtmosphere,
                units: "kg/m^2",
            }),
            FieldSelector {
                field: CanonicalField::TotalPrecipitation,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_TOTAL_PRECIPITATION,
                level: LevelMatch::Surface,
                units: "kg/m^2",
            }),
            FieldSelector {
                field: CanonicalField::TotalCloudCover,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_TOTAL_CLOUD_COVER,
                level: LevelMatch::EntireAtmosphere,
                units: "%",
            }),
            FieldSelector {
                field: CanonicalField::LowCloudCover,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_LOW_CLOUD_COVER,
                level: LevelMatch::ExactLevelType(214),
                units: "%",
            }),
            FieldSelector {
                field: CanonicalField::MiddleCloudCover,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_MIDDLE_CLOUD_COVER,
                level: LevelMatch::ExactLevelType(224),
                units: "%",
            }),
            FieldSelector {
                field: CanonicalField::HighCloudCover,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_HIGH_CLOUD_COVER,
                level: LevelMatch::ExactLevelType(234),
                units: "%",
            }),
            FieldSelector {
                field: CanonicalField::Visibility,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_VISIBILITY,
                level: LevelMatch::Surface,
                units: "m",
            }),
            FieldSelector {
                field: CanonicalField::SimulatedInfraredBrightnessTemperature,
                vertical: VerticalSelector::NominalTop,
            } => Ok(Self {
                parameters: PARAMETER_SIMULATED_IR,
                level: LevelMatch::NominalTop,
                units: "K",
            }),
            FieldSelector {
                field: CanonicalField::CategoricalRain,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_CATEGORICAL_RAIN,
                level: LevelMatch::Surface,
                units: "0/1",
            }),
            FieldSelector {
                field: CanonicalField::CategoricalFreezingRain,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_CATEGORICAL_FREEZING_RAIN,
                level: LevelMatch::Surface,
                units: "0/1",
            }),
            FieldSelector {
                field: CanonicalField::CategoricalIcePellets,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_CATEGORICAL_ICE_PELLETS,
                level: LevelMatch::Surface,
                units: "0/1",
            }),
            FieldSelector {
                field: CanonicalField::CategoricalSnow,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_CATEGORICAL_SNOW,
                level: LevelMatch::Surface,
                units: "0/1",
            }),
            FieldSelector {
                field: CanonicalField::RadarReflectivity,
                vertical: VerticalSelector::HeightAboveGroundMeters(1000),
            } => Ok(Self {
                parameters: PARAMETER_RADAR_REFLECTIVITY,
                level: LevelMatch::HeightAboveGroundMeters(1000),
                units: "dBZ",
            }),
            FieldSelector {
                field: CanonicalField::LandSeaMask,
                vertical: VerticalSelector::Surface,
            } => Ok(Self {
                parameters: PARAMETER_LANDSEA_MASK,
                level: LevelMatch::Surface,
                units: "fraction",
            }),
            FieldSelector {
                field: CanonicalField::CompositeReflectivity,
                vertical: VerticalSelector::EntireAtmosphere,
            } => Ok(Self {
                parameters: PARAMETER_COMPOSITE_REFLECTIVITY,
                level: LevelMatch::EntireAtmosphere,
                units: "dBZ",
            }),
            FieldSelector {
                field: CanonicalField::UpdraftHelicity,
                vertical:
                    VerticalSelector::HeightAboveGroundLayerMeters {
                        bottom_m: 2000,
                        top_m: 5000,
                    },
            } => Ok(Self {
                parameters: PARAMETER_UPDRAFT_HELICITY,
                // HRRR/RRFS native UH fields surface the top of the AGL layer
                // in GRIB metadata; the operational 2-5 km UH product is the
                // 5000 m entry.
                level: LevelMatch::HeightAboveGroundMeters(5000),
                units: "m^2/s^2",
            }),
            _ => Err(IoError::UnsupportedStructuredSelector { selector }),
        }
    }
}

fn is_supported_upper_air_level(level_hpa: u16) -> bool {
    matches!(level_hpa, 200 | 300 | 500 | 700 | 850)
}

impl LevelMatch {
    fn matches(self, message: &Grib2Message) -> bool {
        match self {
            Self::Surface => message.product.level_type == 1,
            Self::MeanSeaLevel => message.product.level_type == 101,
            Self::IsobaricHpa(level_hpa) => {
                message.product.level_type == 100
                    && (normalize_pressure_level_hpa(message.product.level_value)
                        - f64::from(level_hpa))
                    .abs()
                        < 0.25
            }
            Self::HybridLevel(level) => {
                message.product.level_type == 105
                    && (message.product.level_value - f64::from(level)).abs() < 0.25
            }
            Self::EntireAtmosphere => matches!(message.product.level_type, 10 | 200),
            Self::NominalTop => message.product.level_type == 8,
            Self::ExactLevelType(level_type) => message.product.level_type == level_type,
            Self::HeightAboveGroundMeters(level_m) => {
                matches!(message.product.level_type, 103 | 118)
                    && (message.product.level_value - f64::from(level_m)).abs() < 0.25
            }
            Self::SurfaceOrHeightAboveGroundMeters(level_m) => {
                message.product.level_type == 1
                    || (matches!(message.product.level_type, 103 | 118)
                        && (message.product.level_value - f64::from(level_m)).abs() < 0.25)
            }
        }
    }
}

fn build_selected_field(
    message: &Grib2Message,
    selector: FieldSelector,
    units: &str,
) -> Result<SelectedField2D, IoError> {
    let nx = message.grid.nx as usize;
    let ny = message.grid.ny as usize;
    let shape = GridShape::new(nx, ny)?;
    let (mut lat, mut lon) = grid_latlon(&message.grid);
    if lat.is_empty() || lon.is_empty() {
        return Err(IoError::MissingGridCoordinates { selector });
    }
    let mut values = unpack_message(message).map_err(|err| IoError::Grib(err.to_string()))?;
    if message.grid.scan_mode & 0x40 != 0 {
        flip_rows(&mut lat, nx, ny);
        flip_rows(&mut lon, nx, ny);
        flip_rows(&mut values, nx, ny);
    }
    normalize_and_rotate_longitude_rows(&mut lat, &mut lon, &mut values, nx, ny);

    let grid = LatLonGrid::new(
        shape,
        lat.into_iter().map(|value| value as f32).collect(),
        lon.into_iter().map(|value| value as f32).collect(),
    )?;
    let values = values.into_iter().map(|value| value as f32).collect();
    let mut field = SelectedField2D::new(selector, units, grid, values)?;
    if let Some(projection) = grid_projection_from_grib2_grid(&message.grid) {
        field = field.with_projection(projection);
    }
    Ok(field)
}

// GRIB2 Code Table 4.5 level type 100 (isobaric surface) always encodes the
// pressure value in pascals. Converting to hectopascals is a plain /100. The
// old heuristic "only divide when > 2000" collapsed stratospheric levels
// (e.g. 700 Pa = 7 hPa) onto tropospheric hectopascal numbers (e.g. 700 hPa),
// which made GFS and RRFS-A pick the wrong 700 mb RH message (flat brown).
fn normalize_pressure_level_hpa(level_value_pa: f64) -> f64 {
    level_value_pa / 100.0
}

fn is_supported_hrrr_smoke_hybrid_level(level: u16) -> bool {
    (1..=HRRR_WRFNAT_HYBRID_LEVEL_COUNT).contains(&level)
}

fn longitude_midpoint(west_deg: f64, east_deg: f64) -> f64 {
    let west = normalize_longitude(west_deg);
    let mut east = normalize_longitude(east_deg);
    if east < west {
        east += 360.0;
    }
    west + (east - west) / 2.0
}

fn normalize_longitude(lon: f64) -> f64 {
    if lon > 180.0 { lon - 360.0 } else { lon }
}

fn normalize_and_rotate_longitude_rows(
    lat: &mut [f64],
    lon: &mut [f64],
    values: &mut [f64],
    nx: usize,
    ny: usize,
) {
    if nx == 0 || ny == 0 {
        return;
    }

    for row in 0..ny {
        let start = row * nx;
        let end = start + nx;
        let lat_row = &mut lat[start..end];
        let lon_row = &mut lon[start..end];
        let value_row = &mut values[start..end];

        for lon_value in lon_row.iter_mut() {
            *lon_value = normalize_longitude(*lon_value);
        }

        if let Some(wrap_idx) = first_longitude_wrap(lon_row) {
            lat_row.rotate_left(wrap_idx);
            lon_row.rotate_left(wrap_idx);
            value_row.rotate_left(wrap_idx);
        }
    }
}

fn first_longitude_wrap(lon_row: &[f64]) -> Option<usize> {
    lon_row
        .windows(2)
        .position(|pair| pair[1] < pair[0])
        .map(|idx| idx + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use grib_core::grib2::{DataRepresentation, GridDefinition, ProductDefinition};
    use std::path::PathBuf;

    const SAMPLE_IDX: &str = "\
1:0:d=2026041420:TMP:2 m above ground:anl:
2:47843:d=2026041420:SPFH:2 m above ground:anl:
3:96542:d=2026041420:CAPE:surface:anl:
4:143210:d=2026041420:UGRD:10 m above ground:anl:
5:200000:d=2026041420:VGRD:10 m above ground:anl:
";

    fn ieee_f32_message(
        parameter: ParameterCode,
        level_type: u8,
        level_value: f64,
        values: &[f32],
        lon1: f64,
        lon2: f64,
    ) -> Grib2Message {
        let raw_data = values
            .iter()
            .flat_map(|value| value.to_be_bytes())
            .collect::<Vec<_>>();
        Grib2Message {
            discipline: parameter.discipline,
            reference_time: chrono::NaiveDate::from_ymd_opt(2026, 4, 14)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            grid: GridDefinition {
                template: 0,
                nx: values.len() as u32,
                ny: 1,
                lat1: 35.0,
                lon1,
                lat2: 35.0,
                lon2,
                dx: 1.0,
                dy: 0.0,
                scan_mode: 0,
                num_data_points: values.len() as u32,
                ..Default::default()
            },
            product: ProductDefinition {
                template: 0,
                parameter_category: parameter.category,
                parameter_number: parameter.number,
                level_type,
                level_value,
                ..Default::default()
            },
            data_rep: DataRepresentation {
                template: 4,
                bits_per_value: 32,
                section5_num_data_points: values.len() as u32,
                ..Default::default()
            },
            bitmap: None,
            raw_data,
        }
    }

    #[test]
    fn projection_metadata_is_inferred_from_grib_grid_templates() {
        let lambert = GridDefinition {
            template: 30,
            latin1: 38.5,
            latin2: 38.5,
            lov: 262.5,
            ..Default::default()
        };
        assert_eq!(
            grid_projection_from_grib2_grid(&lambert),
            Some(GridProjection::LambertConformal {
                standard_parallel_1_deg: 38.5,
                standard_parallel_2_deg: 38.5,
                central_meridian_deg: -97.5,
            })
        );

        let polar = GridDefinition {
            template: 20,
            lad: 60.0,
            lov: 210.0,
            projection_center_flag: 1,
            ..Default::default()
        };
        assert_eq!(
            grid_projection_from_grib2_grid(&polar),
            Some(GridProjection::PolarStereographic {
                true_latitude_deg: 60.0,
                central_meridian_deg: -150.0,
                south_pole_on_projection_plane: true,
            })
        );
    }

    fn sample_pressure_subset_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("proof")
            .join("rustwx_hrrr_20260414_22z_f00_prs_subset.grib2")
    }

    #[test]
    fn candidate_hours_match_model_rules() {
        assert_eq!(candidate_hours(ModelId::Hrrr, 20).last().copied(), Some(18));
        assert_eq!(candidate_hours(ModelId::Hrrr, 18).last().copied(), Some(48));
        assert_eq!(
            candidate_hours(ModelId::RrfsA, 20).last().copied(),
            Some(60)
        );
        // ECMWF open-data 00/12z stream reaches f360; 06/18z stops at f144.
        assert_eq!(
            candidate_hours(ModelId::EcmwfOpenData, 0).last().copied(),
            Some(360)
        );
        assert_eq!(
            candidate_hours(ModelId::EcmwfOpenData, 12).last().copied(),
            Some(360)
        );
        assert_eq!(
            candidate_hours(ModelId::EcmwfOpenData, 6).last().copied(),
            Some(144)
        );
        assert_eq!(
            candidate_hours(ModelId::EcmwfOpenData, 18).last().copied(),
            Some(144)
        );
    }

    #[test]
    fn nomads_hour_probes_are_serialized() {
        assert!(!should_parallelize_hour_availability_probes(
            Some(SourceId::Nomads),
            model_summary(ModelId::Hrrr)
        ));
        assert!(should_parallelize_hour_availability_probes(
            Some(SourceId::Aws),
            model_summary(ModelId::Hrrr)
        ));
        assert!(!should_parallelize_hour_availability_probes(
            None,
            model_summary(ModelId::Hrrr)
        ));
    }

    #[test]
    fn nomads_probe_uses_grib_url_for_availability() {
        let resolved = ResolvedUrl {
            source: SourceId::Nomads,
            grib_url: "https://nomads.ncep.noaa.gov/file.grib2".to_string(),
            idx_url: Some("https://nomads.ncep.noaa.gov/file.grib2.idx".to_string()),
        };
        assert_eq!(
            resolved.availability_probe_url(),
            "https://nomads.ncep.noaa.gov/file.grib2.idx"
        );
        assert_eq!(resolved.grib_url, "https://nomads.ncep.noaa.gov/file.grib2");
    }

    #[test]
    fn source_probe_uses_fallback_sources_in_registry_order() {
        let urls = vec![
            ResolvedUrl {
                source: SourceId::Nomads,
                grib_url: "https://nomads.ncep.noaa.gov/primary.grib2".to_string(),
                idx_url: None,
            },
            ResolvedUrl {
                source: SourceId::Aws,
                grib_url: "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/fallback.grib2".to_string(),
                idx_url: None,
            },
        ];
        let seen = std::sync::Mutex::new(Vec::new());
        let available = any_source_available(&urls, |resolved| {
            seen.lock().unwrap().push(resolved.source);
            matches!(resolved.source, SourceId::Aws)
        });
        assert!(available);
        assert_eq!(*seen.lock().unwrap(), vec![SourceId::Nomads, SourceId::Aws]);
    }

    #[test]
    fn matching_ranges_uses_idx_patterns() {
        let ranges = idx_subset_ranges(SAMPLE_IDX, &["TMP:2 m above ground", "CAPE:surface"])
            .unwrap()
            .expect("idx subset ranges should exist");
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges[1].0, 96542);
    }

    #[test]
    fn matching_ranges_dedupes_duplicate_selector_hits() {
        let ranges = idx_subset_ranges(
            SAMPLE_IDX,
            &["TMP:2 m above ground", "TMP:2 m above ground"],
        )
        .unwrap()
        .expect("idx subset ranges should exist");
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, 0);
    }

    #[test]
    fn idx_subset_ranges_coalesces_contiguous_messages_only() {
        let ranges = idx_subset_ranges(
            SAMPLE_IDX,
            &["TMP:2 m above ground", "SPFH:2 m above ground"],
        )
        .unwrap()
        .expect("idx subset ranges should exist");
        assert_eq!(ranges, vec![(0, 96541)]);
    }

    #[test]
    fn idx_subset_ranges_falls_back_when_patterns_do_not_match() {
        assert_eq!(
            idx_subset_ranges(SAMPLE_IDX, &["TMP:850 mb"]).unwrap(),
            None
        );
    }

    #[test]
    fn idx_subset_ranges_falls_back_when_idx_is_unparseable() {
        assert_eq!(
            idx_subset_ranges("not an idx", &["TMP:2 m above ground"]).unwrap(),
            None
        );
    }

    #[test]
    fn resolve_fetch_urls_uses_registry_order() {
        let request = ModelRunRequest::new(
            ModelId::RrfsA,
            rustwx_core::CycleSpec::new("20260414", 20).unwrap(),
            2,
            "prs-conus",
        )
        .unwrap();
        let fetch = FetchRequest {
            request,
            source_override: None,
            variable_patterns: Vec::new(),
        };
        let urls = filtered_urls(&fetch).unwrap();
        assert_eq!(urls.len(), 1);
        assert!(urls[0].grib_url.contains("noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.20260414/20/rrfs.t20z.prslev.3km.f002.conus.grib2"));
    }

    #[test]
    fn fetch_request_from_timestep_builds_request() {
        let timestep = ModelTimestep::with_source(
            ModelId::Hrrr,
            rustwx_core::CycleSpec::new("20260414", 18).unwrap(),
            3,
            rustwx_core::TimeStamp::new("2026-04-14T21:00:00Z").unwrap(),
            Some(SourceId::Nomads),
        )
        .unwrap();

        let fetch = FetchRequest::from_timestep(
            &timestep,
            "prs",
            timestep.source,
            ["TMP:500 mb", "RH:500 mb"],
        )
        .unwrap();

        assert_eq!(fetch.request.model, ModelId::Hrrr);
        assert_eq!(fetch.request.forecast_hour, 3);
        assert_eq!(fetch.request.product, "prs");
        assert_eq!(fetch.source_override, Some(SourceId::Nomads));
        assert_eq!(
            fetch.variable_patterns,
            vec!["TMP:500 mb".to_string(), "RH:500 mb".to_string()]
        );
    }

    #[test]
    fn structured_selector_matches_supported_upper_air_subset() {
        let height_200 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::GeopotentialHeight,
            200,
        ))
        .unwrap();
        let height_200_message =
            ieee_f32_message(PARAMETER_HGT[0], 100, 20_000.0, &[12_040.0], -99.0, -99.0);
        assert!(height_200.matches(&height_200_message));

        let wind_300 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::VWind,
            300,
        ))
        .unwrap();
        let wind_300_message =
            ieee_f32_message(PARAMETER_VGRD[0], 100, 30_000.0, &[36.0], -99.0, -99.0);
        assert!(wind_300.matches(&wind_300_message));

        let wind_selector = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::UWind,
            850,
        ))
        .unwrap();
        let wind_message = ieee_f32_message(
            PARAMETER_UGRD[0],
            100,
            85_000.0,
            &[12.0, 15.0],
            -99.0,
            -98.0,
        );
        assert!(wind_selector.matches(&wind_message));

        let temp_700 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::Temperature,
            700,
        ))
        .unwrap();
        let temp_message =
            ieee_f32_message(PARAMETER_TMP[0], 100, 70_000.0, &[274.0], -99.0, -99.0);
        assert!(temp_700.matches(&temp_message));
        // Stratospheric 7 hPa (level_value=700 Pa) must NOT alias onto 700 hPa.
        let stratospheric_tmp_message =
            ieee_f32_message(PARAMETER_TMP[0], 100, 700.0, &[210.0], -99.0, -99.0);
        assert!(!temp_700.matches(&stratospheric_tmp_message));

        let rh_700 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::RelativeHumidity,
            700,
        ))
        .unwrap();
        let rh_message = ieee_f32_message(PARAMETER_RH[0], 100, 70_000.0, &[61.0], -99.0, -99.0);
        assert!(rh_700.matches(&rh_message));
        // GFS/RRFS carry stratospheric RH at level_value=700 Pa (7 hPa). With the
        // old "divide by 100 only when > 2000" heuristic this collided with 700
        // hPa and the first-match extraction picked up the near-zero
        // stratospheric RH, producing a flat-brown 700 mb render.
        let stratospheric_rh_message =
            ieee_f32_message(PARAMETER_RH[0], 100, 700.0, &[0.1], -99.0, -99.0);
        assert!(!rh_700.matches(&stratospheric_rh_message));

        let dewpoint_850 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::Dewpoint,
            850,
        ))
        .unwrap();
        let dewpoint_message =
            ieee_f32_message(PARAMETER_DPT[0], 100, 85_000.0, &[281.0], -99.0, -99.0);
        assert!(dewpoint_850.matches(&dewpoint_message));

        let dewpoint_700 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::Dewpoint,
            700,
        ))
        .unwrap();
        let dewpoint_700_message =
            ieee_f32_message(PARAMETER_DPT[0], 100, 70_000.0, &[270.0], -99.0, -99.0);
        assert!(dewpoint_700.matches(&dewpoint_700_message));

        let vorticity_500 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::AbsoluteVorticity,
            500,
        ))
        .unwrap();
        let vorticity_message = ieee_f32_message(
            PARAMETER_ABSOLUTE_VORTICITY[0],
            100,
            50_000.0,
            &[0.00012],
            -99.0,
            -99.0,
        );
        assert!(vorticity_500.matches(&vorticity_message));

        let lsm_surface = StructuredMessageSelector::try_from(FieldSelector::surface(
            CanonicalField::LandSeaMask,
        ))
        .unwrap();
        let lsm_message = ieee_f32_message(PARAMETER_LANDSEA_MASK[0], 1, 0.0, &[1.0], -99.0, -99.0);
        assert!(lsm_surface.matches(&lsm_message));

        let temp_2m = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::Temperature,
            2,
        ))
        .unwrap();
        let temp_2m_message = ieee_f32_message(PARAMETER_TMP[0], 103, 2.0, &[293.2], -99.0, -99.0);
        assert!(temp_2m.matches(&temp_2m_message));

        let dewpoint_2m = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::Dewpoint,
            2,
        ))
        .unwrap();
        let dewpoint_2m_message =
            ieee_f32_message(PARAMETER_DPT[0], 103, 2.0, &[286.4], -99.0, -99.0);
        assert!(dewpoint_2m.matches(&dewpoint_2m_message));

        let rh_2m = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::RelativeHumidity,
            2,
        ))
        .unwrap();
        let rh_2m_message = ieee_f32_message(PARAMETER_RH[0], 103, 2.0, &[64.0], -99.0, -99.0);
        assert!(rh_2m.matches(&rh_2m_message));

        let hybrid_pressure = StructuredMessageSelector::try_from(FieldSelector::hybrid_level(
            CanonicalField::Pressure,
            7,
        ))
        .unwrap();
        let hybrid_pressure_message =
            ieee_f32_message(PARAMETER_PRESSURE[0], 105, 7.0, &[81_500.0], -99.0, -99.0);
        assert!(hybrid_pressure.matches(&hybrid_pressure_message));

        let hybrid_smoke = StructuredMessageSelector::try_from(FieldSelector::hybrid_level(
            CanonicalField::SmokeMassDensity,
            7,
        ))
        .unwrap();
        let hybrid_smoke_message = ieee_f32_message(
            PARAMETER_SMOKE_MASS_DENSITY[0],
            105,
            7.0,
            &[0.000_012],
            -99.0,
            -99.0,
        );
        assert!(hybrid_smoke.matches(&hybrid_smoke_message));
        let wrong_hybrid_smoke_message = ieee_f32_message(
            PARAMETER_SMOKE_MASS_DENSITY[0],
            105,
            8.0,
            &[0.000_012],
            -99.0,
            -99.0,
        );
        assert!(!hybrid_smoke.matches(&wrong_hybrid_smoke_message));

        let smoke_8m = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::SmokeMassDensity,
            8,
        ))
        .unwrap();
        let smoke_8m_message = ieee_f32_message(
            PARAMETER_SMOKE_MASS_DENSITY[0],
            103,
            8.0,
            &[0.000_025],
            -99.0,
            -99.0,
        );
        assert!(smoke_8m.matches(&smoke_8m_message));

        let smoke_column = StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
            CanonicalField::ColumnIntegratedSmoke,
        ))
        .unwrap();
        let smoke_column_message = ieee_f32_message(
            PARAMETER_COLUMN_INTEGRATED_SMOKE[0],
            200,
            0.0,
            &[0.003],
            -99.0,
            -99.0,
        );
        assert!(smoke_column.matches(&smoke_column_message));

        let u_10m = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::UWind,
            10,
        ))
        .unwrap();
        let u_10m_message = ieee_f32_message(PARAMETER_UGRD[0], 103, 10.0, &[8.0], -99.0, -99.0);
        assert!(u_10m.matches(&u_10m_message));

        let gust_10m = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::WindGust,
            10,
        ))
        .unwrap();
        let gust_surface_message =
            ieee_f32_message(PARAMETER_WIND_GUST[0], 1, 0.0, &[18.0], -99.0, -99.0);
        assert!(gust_10m.matches(&gust_surface_message));
        let gust_10m_message =
            ieee_f32_message(PARAMETER_WIND_GUST[0], 103, 10.0, &[18.0], -99.0, -99.0);
        assert!(gust_10m.matches(&gust_10m_message));

        let mslp = StructuredMessageSelector::try_from(FieldSelector::mean_sea_level(
            CanonicalField::PressureReducedToMeanSeaLevel,
        ))
        .unwrap();
        let mslp_message =
            ieee_f32_message(PARAMETER_MSLP[0], 101, 0.0, &[100_925.0], -99.0, -99.0);
        assert!(mslp.matches(&mslp_message));
        let mslma_message =
            ieee_f32_message(PARAMETER_MSLP[2], 101, 0.0, &[100_830.0], -99.0, -99.0);
        assert!(mslp.matches(&mslma_message));

        let pwat = StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
            CanonicalField::PrecipitableWater,
        ))
        .unwrap();
        let pwat_message = ieee_f32_message(PARAMETER_PWAT[0], 200, 0.0, &[31.0], -99.0, -99.0);
        assert!(pwat.matches(&pwat_message));

        let qpf = StructuredMessageSelector::try_from(FieldSelector::surface(
            CanonicalField::TotalPrecipitation,
        ))
        .unwrap();
        let qpf_message = ieee_f32_message(
            PARAMETER_TOTAL_PRECIPITATION[0],
            1,
            0.0,
            &[12.0],
            -99.0,
            -99.0,
        );
        assert!(qpf.matches(&qpf_message));

        let tcdc = StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
            CanonicalField::TotalCloudCover,
        ))
        .unwrap();
        let tcdc_message = ieee_f32_message(
            PARAMETER_TOTAL_CLOUD_COVER[0],
            200,
            0.0,
            &[84.0],
            -99.0,
            -99.0,
        );
        assert!(tcdc.matches(&tcdc_message));

        let lcdc = StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
            CanonicalField::LowCloudCover,
        ))
        .unwrap();
        let lcdc_message = ieee_f32_message(
            PARAMETER_LOW_CLOUD_COVER[0],
            214,
            0.0,
            &[40.0],
            -99.0,
            -99.0,
        );
        assert!(lcdc.matches(&lcdc_message));

        let mcdc = StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
            CanonicalField::MiddleCloudCover,
        ))
        .unwrap();
        let mcdc_message = ieee_f32_message(
            PARAMETER_MIDDLE_CLOUD_COVER[0],
            224,
            0.0,
            &[55.0],
            -99.0,
            -99.0,
        );
        assert!(mcdc.matches(&mcdc_message));

        let hcdc = StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
            CanonicalField::HighCloudCover,
        ))
        .unwrap();
        let hcdc_message = ieee_f32_message(
            PARAMETER_HIGH_CLOUD_COVER[0],
            234,
            0.0,
            &[70.0],
            -99.0,
            -99.0,
        );
        assert!(hcdc.matches(&hcdc_message));

        let visibility =
            StructuredMessageSelector::try_from(FieldSelector::surface(CanonicalField::Visibility))
                .unwrap();
        let visibility_message =
            ieee_f32_message(PARAMETER_VISIBILITY[0], 1, 0.0, &[16_000.0], -99.0, -99.0);
        assert!(visibility.matches(&visibility_message));

        let simulated_ir = StructuredMessageSelector::try_from(FieldSelector::nominal_top(
            CanonicalField::SimulatedInfraredBrightnessTemperature,
        ))
        .unwrap();
        let simulated_ir_message =
            ieee_f32_message(PARAMETER_SIMULATED_IR[0], 8, 0.0, &[234.5], -99.0, -99.0);
        let simulated_ir_wrong_level =
            ieee_f32_message(PARAMETER_SIMULATED_IR[0], 10, 0.0, &[234.5], -99.0, -99.0);
        assert!(simulated_ir.matches(&simulated_ir_message));
        assert!(!simulated_ir.matches(&simulated_ir_wrong_level));

        let categorical_rain = StructuredMessageSelector::try_from(FieldSelector::surface(
            CanonicalField::CategoricalRain,
        ))
        .unwrap();
        let categorical_rain_message =
            ieee_f32_message(PARAMETER_CATEGORICAL_RAIN[0], 1, 0.0, &[1.0], -99.0, -99.0);
        assert!(categorical_rain.matches(&categorical_rain_message));
        let categorical_rain_hrrr_message =
            ieee_f32_message(PARAMETER_CATEGORICAL_RAIN[1], 1, 0.0, &[1.0], -99.0, -99.0);
        assert!(categorical_rain.matches(&categorical_rain_hrrr_message));

        let categorical_freezing_rain = StructuredMessageSelector::try_from(
            FieldSelector::surface(CanonicalField::CategoricalFreezingRain),
        )
        .unwrap();
        let categorical_freezing_rain_message = ieee_f32_message(
            PARAMETER_CATEGORICAL_FREEZING_RAIN[0],
            1,
            0.0,
            &[1.0],
            -99.0,
            -99.0,
        );
        assert!(categorical_freezing_rain.matches(&categorical_freezing_rain_message));
        let categorical_freezing_rain_hrrr_message = ieee_f32_message(
            PARAMETER_CATEGORICAL_FREEZING_RAIN[1],
            1,
            0.0,
            &[1.0],
            -99.0,
            -99.0,
        );
        assert!(categorical_freezing_rain.matches(&categorical_freezing_rain_hrrr_message));

        let categorical_ice_pellets = StructuredMessageSelector::try_from(FieldSelector::surface(
            CanonicalField::CategoricalIcePellets,
        ))
        .unwrap();
        let categorical_ice_pellets_message = ieee_f32_message(
            PARAMETER_CATEGORICAL_ICE_PELLETS[0],
            1,
            0.0,
            &[1.0],
            -99.0,
            -99.0,
        );
        assert!(categorical_ice_pellets.matches(&categorical_ice_pellets_message));
        let categorical_ice_pellets_hrrr_message = ieee_f32_message(
            PARAMETER_CATEGORICAL_ICE_PELLETS[1],
            1,
            0.0,
            &[1.0],
            -99.0,
            -99.0,
        );
        assert!(categorical_ice_pellets.matches(&categorical_ice_pellets_hrrr_message));

        let categorical_snow = StructuredMessageSelector::try_from(FieldSelector::surface(
            CanonicalField::CategoricalSnow,
        ))
        .unwrap();
        let categorical_snow_message =
            ieee_f32_message(PARAMETER_CATEGORICAL_SNOW[0], 1, 0.0, &[1.0], -99.0, -99.0);
        assert!(categorical_snow.matches(&categorical_snow_message));
        let categorical_snow_hrrr_message =
            ieee_f32_message(PARAMETER_CATEGORICAL_SNOW[1], 1, 0.0, &[1.0], -99.0, -99.0);
        assert!(categorical_snow.matches(&categorical_snow_hrrr_message));

        let reflectivity_1km = StructuredMessageSelector::try_from(FieldSelector::height_agl(
            CanonicalField::RadarReflectivity,
            1000,
        ))
        .unwrap();
        let reflectivity_message = ieee_f32_message(
            PARAMETER_RADAR_REFLECTIVITY[0],
            103,
            1000.0,
            &[42.0],
            -99.0,
            -99.0,
        );
        assert!(reflectivity_1km.matches(&reflectivity_message));

        let uh_2_5km = StructuredMessageSelector::try_from(FieldSelector::height_layer_agl(
            CanonicalField::UpdraftHelicity,
            2000,
            5000,
        ))
        .unwrap();
        let uh_message = ieee_f32_message(
            PARAMETER_UPDRAFT_HELICITY[0],
            103,
            5000.0,
            &[125.0],
            -99.0,
            -99.0,
        );
        assert!(uh_2_5km.matches(&uh_message));

        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::isobaric(
                CanonicalField::Dewpoint,
                500
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::isobaric(
                CanonicalField::AbsoluteVorticity,
                925
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::isobaric(
                CanonicalField::RelativeVorticity,
                500
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::height_layer_agl(
                CanonicalField::UpdraftHelicity,
                0,
                3000
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::hybrid_level(
                CanonicalField::SmokeMassDensity,
                51
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::height_agl(
                CanonicalField::SmokeMassDensity,
                2
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
        assert!(matches!(
            StructuredMessageSelector::try_from(FieldSelector::entire_atmosphere(
                CanonicalField::SimulatedInfraredBrightnessTemperature
            )),
            Err(IoError::UnsupportedStructuredSelector { .. })
        ));
    }

    #[test]
    fn extract_ignores_stratospheric_pa_alias_of_tropospheric_level() {
        // GFS/RRFS-A carry both 7 hPa (level_value = 700 Pa) and 700 hPa
        // (level_value = 70_000 Pa) messages in the same file. The 7 hPa one
        // appears first. The extractor must return the 700 hPa message.
        let stratospheric =
            ieee_f32_message(PARAMETER_RH[0], 100, 700.0, &[0.1, 0.2], 261.0, 262.0);
        let tropospheric =
            ieee_f32_message(PARAMETER_RH[0], 100, 70_000.0, &[55.0, 65.0], 261.0, 262.0);
        let grib = Grib2File {
            messages: vec![stratospheric, tropospheric],
        };

        let field = extract_pressure_field_from_grib2(&grib, CanonicalField::RelativeHumidity, 700)
            .unwrap();

        assert_eq!(field.values, vec![55.0, 65.0]);
    }

    #[test]
    fn extract_field_from_grib2_returns_selector_backed_field() {
        // 500 hPa is encoded as 50_000 Pa per GRIB2 Code Table 4.5 level 100.
        let message = ieee_f32_message(
            PARAMETER_TMP[0],
            100,
            50_000.0,
            &[255.0, 256.5],
            261.0,
            262.0,
        );
        let grib = Grib2File {
            messages: vec![message],
        };

        let field =
            extract_pressure_field_from_grib2(&grib, CanonicalField::Temperature, 500).unwrap();

        assert_eq!(
            field.selector,
            FieldSelector::isobaric(CanonicalField::Temperature, 500)
        );
        assert_eq!(field.units, "K");
        assert_eq!(field.grid.shape.nx, 2);
        assert_eq!(field.grid.shape.ny, 1);
        assert_eq!(field.grid.lon_deg, vec![-99.0, -98.0]);
        assert_eq!(field.values, vec![255.0, 256.5]);
    }

    #[test]
    fn extract_hybrid_level_volume_from_grib2_stacks_requested_levels() {
        let smoke_level_2 = ieee_f32_message(
            PARAMETER_SMOKE_MASS_DENSITY[0],
            105,
            2.0,
            &[0.3, 0.4],
            -99.0,
            -98.0,
        );
        let smoke_level_1 = ieee_f32_message(
            PARAMETER_SMOKE_MASS_DENSITY[0],
            105,
            1.0,
            &[0.1, 0.2],
            -99.0,
            -98.0,
        );
        let grib = Grib2File {
            messages: vec![smoke_level_2, smoke_level_1],
        };

        let volume = extract_hybrid_level_volume_from_grib2(
            &grib,
            CanonicalField::SmokeMassDensity,
            &[1, 2],
        )
        .unwrap();

        assert_eq!(volume.field, CanonicalField::SmokeMassDensity);
        assert_eq!(volume.levels_hybrid, vec![1, 2]);
        assert_eq!(volume.units, "kg/m^3");
        assert_eq!(volume.level_slice(0), Some(&[0.1, 0.2][..]));
        assert_eq!(volume.level_slice(1), Some(&[0.3, 0.4][..]));
        assert_eq!(
            volume.selector_at(0),
            Some(FieldSelector::hybrid_level(
                CanonicalField::SmokeMassDensity,
                1
            ))
        );
    }

    #[test]
    fn extract_hrrr_wrfnat_smoke_fields_returns_surface_column_and_hybrid_pairs() {
        let mut messages = Vec::new();
        for level in 1..=HRRR_WRFNAT_HYBRID_LEVEL_COUNT {
            messages.push(ieee_f32_message(
                PARAMETER_PRESSURE[0],
                105,
                f64::from(level),
                &[80_000.0 - level as f32, 79_000.0 - level as f32],
                -99.0,
                -98.0,
            ));

            let smoke_values = match level {
                1 => vec![0.1, 0.2],
                2 => vec![0.3, 0.4],
                _ => vec![level as f32, level as f32 + 0.5],
            };
            messages.push(ieee_f32_message(
                PARAMETER_SMOKE_MASS_DENSITY[0],
                105,
                f64::from(level),
                &smoke_values,
                -99.0,
                -98.0,
            ));
        }
        messages.push(ieee_f32_message(
            PARAMETER_SMOKE_MASS_DENSITY[0],
            103,
            8.0,
            &[1.5, 2.5],
            -99.0,
            -98.0,
        ));
        messages.push(ieee_f32_message(
            PARAMETER_COLUMN_INTEGRATED_SMOKE[0],
            200,
            0.0,
            &[3.5, 4.5],
            -99.0,
            -98.0,
        ));
        let grib = Grib2File { messages };

        let extracted = extract_hrrr_wrfnat_smoke_fields_from_grib2(&grib).unwrap();

        assert_eq!(extracted.hybrid_smoke.level_count(), 50);
        assert_eq!(extracted.hybrid_pressure.level_count(), 50);
        assert_eq!(
            extracted.near_surface_smoke.selector,
            FieldSelector::height_agl(CanonicalField::SmokeMassDensity, 8)
        );
        assert_eq!(
            extracted.column_smoke.selector,
            FieldSelector::entire_atmosphere(CanonicalField::ColumnIntegratedSmoke)
        );
        assert_eq!(extracted.hybrid_smoke.level_slice(0), Some(&[0.1, 0.2][..]));
        assert_eq!(extracted.hybrid_smoke.level_slice(1), Some(&[0.3, 0.4][..]));
        assert_eq!(
            extracted.hybrid_pressure.selector_at(49),
            Some(FieldSelector::hybrid_level(CanonicalField::Pressure, 50))
        );
        assert_eq!(extracted.near_surface_smoke.values, vec![1.5, 2.5]);
        assert_eq!(extracted.column_smoke.values, vec![3.5, 4.5]);
    }

    #[test]
    fn extract_field_from_real_pressure_bytes_uses_structured_matching() {
        let path = sample_pressure_subset_path();
        if !path.exists() {
            eprintln!(
                "skipping real pressure subset test; fixture is not present at {}",
                path.display()
            );
            return;
        }
        let bytes = std::fs::read(&path).unwrap();

        let temp_500 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::Temperature, 500).unwrap();
        let temp_700 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::Temperature, 700).unwrap();
        let hgt_700 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::GeopotentialHeight, 700)
                .unwrap();
        let hgt_850 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::GeopotentialHeight, 850)
                .unwrap();
        let u_700 = extract_pressure_field_from_bytes(&bytes, CanonicalField::UWind, 700).unwrap();
        let v_700 = extract_pressure_field_from_bytes(&bytes, CanonicalField::VWind, 700).unwrap();

        assert_eq!(
            temp_500.selector,
            FieldSelector::isobaric(CanonicalField::Temperature, 500)
        );
        assert_eq!(
            temp_700.selector,
            FieldSelector::isobaric(CanonicalField::Temperature, 700)
        );
        assert_eq!(temp_500.units, "K");
        assert_eq!(temp_700.units, "K");
        assert_eq!(hgt_700.units, "gpm");
        assert_eq!(hgt_850.units, "gpm");
        assert_eq!(u_700.units, "m/s");
        assert_eq!(v_700.units, "m/s");
        assert_eq!(temp_700.grid.shape, hgt_700.grid.shape);
        assert_eq!(temp_700.grid.shape, u_700.grid.shape);
        assert_eq!(u_700.grid.shape, v_700.grid.shape);
        assert_eq!(temp_500.grid.shape, hgt_850.grid.shape);
        assert_eq!(temp_500.values.len(), temp_500.grid.shape.len());
        assert_eq!(temp_700.values.len(), temp_700.grid.shape.len());
        assert_eq!(hgt_700.values.len(), hgt_700.grid.shape.len());
        assert_eq!(hgt_850.values.len(), hgt_850.grid.shape.len());
        assert_eq!(u_700.values.len(), u_700.grid.shape.len());
        assert_eq!(v_700.values.len(), v_700.grid.shape.len());
        assert!(temp_500.values.iter().any(|value| value.is_finite()));
        assert!(temp_700.values.iter().any(|value| value.is_finite()));
        assert!(hgt_700.values.iter().any(|value| value.is_finite()));
        assert!(hgt_850.values.iter().any(|value| value.is_finite()));
        assert!(u_700.values.iter().any(|value| value.is_finite()));
        assert!(v_700.values.iter().any(|value| value.is_finite()));
    }

    #[test]
    fn extract_fields_from_real_pressure_bytes_batches_parse_and_matching() {
        let path = sample_pressure_subset_path();
        if !path.exists() {
            eprintln!(
                "skipping real pressure subset batch test; fixture is not present at {}",
                path.display()
            );
            return;
        }
        let bytes = std::fs::read(&path).unwrap();
        let selectors = [
            FieldSelector::isobaric(CanonicalField::Temperature, 500),
            FieldSelector::isobaric(CanonicalField::Temperature, 700),
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 700),
            FieldSelector::isobaric(CanonicalField::UWind, 700),
            FieldSelector::isobaric(CanonicalField::VWind, 700),
        ];

        let batched = extract_fields_from_bytes(&bytes, &selectors).unwrap();

        assert_eq!(batched.len(), selectors.len());
        for (selector, field) in selectors.iter().zip(batched.iter()) {
            assert_eq!(&field.selector, selector);
        }

        let single_temp_500 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::Temperature, 500).unwrap();
        let single_hgt_700 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::GeopotentialHeight, 700)
                .unwrap();
        let single_u_700 =
            extract_pressure_field_from_bytes(&bytes, CanonicalField::UWind, 700).unwrap();

        assert_eq!(batched[0], single_temp_500);
        assert_eq!(batched[2], single_hgt_700);
        assert_eq!(batched[3], single_u_700);
    }

    #[test]
    fn normalize_and_rotate_longitude_rows_keeps_rows_monotone() {
        let mut lat = vec![40.0, 40.0, 40.0, 40.0, 39.0, 39.0, 39.0, 39.0];
        let mut lon = vec![0.0, 90.0, 180.0, 270.0, 0.0, 90.0, 180.0, 270.0];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 11.0, 12.0, 13.0, 14.0];

        normalize_and_rotate_longitude_rows(&mut lat, &mut lon, &mut values, 4, 2);

        assert_eq!(lon[..4], [-90.0, 0.0, 90.0, 180.0]);
        assert_eq!(lon[4..], [-90.0, 0.0, 90.0, 180.0]);
        assert_eq!(values[..4], [4.0, 1.0, 2.0, 3.0]);
        assert_eq!(values[4..], [14.0, 11.0, 12.0, 13.0]);
        assert_eq!(lat[..4], [40.0, 40.0, 40.0, 40.0]);
        assert_eq!(lat[4..], [39.0, 39.0, 39.0, 39.0]);
    }
}
