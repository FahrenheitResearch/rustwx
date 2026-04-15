mod cache;

pub use cache::{
    CachedFetchMetadata, CachedFetchResult, CachedFieldResult, artifact_cache_dir,
    fetch_cache_paths, field_cache_path, load_cached_fetch, load_cached_selected_field,
    store_cached_fetch, store_cached_selected_field,
};

use grib_core::grib2::{Grib2File, Grib2Message, flip_rows, grid_latlon, unpack_message};
use rayon::prelude::*;
use rustwx_core::{
    CanonicalField, FieldSelector, GridShape, LatLonGrid, ModelId, ModelRunRequest, ModelTimestep,
    ResolvedUrl, SelectedField2D, SourceId, VerticalSelector,
};
use rustwx_models::{latest_available_run, model_summary, resolve_urls};
use serde::Serialize;
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

pub fn client() -> Result<DownloadClient, IoError> {
    DownloadClient::new_with_cache(None).map_err(|err| IoError::Download(err.to_string()))
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
            let available = client.head_ok(resolved.availability_probe_url());
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
    let source = source_override.unwrap_or(summary.sources[0].id);

    let available = candidates
        .par_iter()
        .filter_map(|&forecast_hour| {
            let cycle = rustwx_core::CycleSpec::new(date_yyyymmdd, hour_utc).ok()?;
            let request = ModelRunRequest::new(model, cycle, forecast_hour, product).ok()?;
            let resolved = resolve_urls(&request).ok()?;
            let target = resolved.into_iter().find(|url| url.source == source)?;
            if client.head_ok(target.availability_probe_url()) {
                Some(forecast_hour)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

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
    let grib = Grib2File::from_bytes(bytes).map_err(|err| IoError::Grib(err.to_string()))?;
    extract_field_from_grib2(&grib, selector)
}

pub fn extract_field_from_grib2(
    grib: &Grib2File,
    selector: FieldSelector,
) -> Result<SelectedField2D, IoError> {
    let message_selector = StructuredMessageSelector::try_from(selector)?;
    let message = grib
        .messages
        .iter()
        .find(|message| message_selector.matches(message))
        .ok_or(IoError::FieldNotFound { selector })?;
    build_selected_field(message, selector, message_selector.units)
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

fn try_fetch_one(
    client: &DownloadClient,
    resolved: &ResolvedUrl,
    variable_patterns: &[&str],
) -> Result<Vec<u8>, String> {
    if !variable_patterns.is_empty() {
        if let Some(idx_url) = &resolved.idx_url {
            let idx_text = client.get_text(idx_url).map_err(|err| err.to_string())?;
            let ranges = matching_ranges(&idx_text, variable_patterns)?;
            if !ranges.is_empty() {
                return client
                    .get_ranges(&resolved.grib_url, &ranges)
                    .map_err(|err| err.to_string());
            }
        }
    }
    client
        .get_bytes(&resolved.grib_url)
        .map_err(|err| err.to_string())
}

fn matching_ranges(idx_text: &str, patterns: &[&str]) -> Result<Vec<(u64, u64)>, String> {
    let entries = parse_idx(idx_text);
    if entries.is_empty() {
        return Err("idx file was empty or unparseable".to_string());
    }

    let mut selected = Vec::new();
    for pattern in patterns {
        for entry in find_entries(&entries, pattern) {
            if !selected
                .iter()
                .any(|existing: &&wx_core::download::IdxEntry| {
                    existing.byte_offset == entry.byte_offset
                })
            {
                selected.push(entry);
            }
        }
    }

    if selected.is_empty() {
        return Err(format!("no idx entries matched patterns {patterns:?}"));
    }
    Ok(byte_ranges(&entries, &selected))
}

fn candidate_hours(model: ModelId, cycle_hour: u8) -> Vec<u16> {
    match model {
        ModelId::Hrrr => {
            if cycle_hour % 6 == 0 {
                (0..=48).collect()
            } else {
                (0..=18).collect()
            }
        }
        ModelId::Gfs => {
            let mut hours = (0..=120).collect::<Vec<u16>>();
            hours.extend((123..=384).step_by(3));
            hours
        }
        ModelId::EcmwfOpenData => (0..=240).step_by(3).collect(),
        ModelId::RrfsA => (0..=60).collect(),
    }
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
    IsobaricHpa(u16),
    EntireAtmosphere,
    HeightAboveGroundMeters(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StructuredMessageSelector {
    parameters: &'static [ParameterCode],
    level: LevelMatch,
    units: &'static str,
}

const PARAMETER_HGT: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 3,
    number: 5,
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
// Only absolute vorticity is wired right now. Relative vorticity needs its own
// explicit selector and GRIB parameter mapping before it should be exposed.
const PARAMETER_ABSOLUTE_VORTICITY: &[ParameterCode] = &[ParameterCode {
    discipline: 0,
    category: 2,
    number: 10,
}];
const PARAMETER_LANDSEA_MASK: &[ParameterCode] = &[ParameterCode {
    discipline: 2,
    category: 0,
    number: 0,
}];
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

impl StructuredMessageSelector {
    fn matches(self, message: &Grib2Message) -> bool {
        self.parameters.iter().any(|parameter| {
            message.discipline == parameter.discipline
                && message.product.parameter_category == parameter.category
                && message.product.parameter_number == parameter.number
        }) && self.level.matches(message)
    }
}

impl TryFrom<FieldSelector> for StructuredMessageSelector {
    type Error = IoError;

    fn try_from(selector: FieldSelector) -> Result<Self, Self::Error> {
        match selector {
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
    matches!(level_hpa, 500 | 700 | 850)
}

impl LevelMatch {
    fn matches(self, message: &Grib2Message) -> bool {
        match self {
            Self::Surface => message.product.level_type == 1,
            Self::IsobaricHpa(level_hpa) => {
                message.product.level_type == 100
                    && (normalize_pressure_level_hpa(message.product.level_value)
                        - f64::from(level_hpa))
                    .abs()
                        < 0.25
            }
            Self::EntireAtmosphere => matches!(message.product.level_type, 10 | 200),
            Self::HeightAboveGroundMeters(level_m) => {
                matches!(message.product.level_type, 103 | 118)
                    && (message.product.level_value - f64::from(level_m)).abs() < 0.25
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

    SelectedField2D::new(selector, units, grid, values).map_err(Into::into)
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
    }

    #[test]
    fn matching_ranges_uses_idx_patterns() {
        let ranges =
            matching_ranges(SAMPLE_IDX, &["TMP:2 m above ground", "CAPE:surface"]).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges[1].0, 96542);
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
            rustwx_core::TimeStamp::new("2026-04-14T21:00:00Z"),
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
        let wind_selector = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::UWind,
            850,
        ))
        .unwrap();
        let wind_message =
            ieee_f32_message(PARAMETER_UGRD[0], 100, 850.0, &[12.0, 15.0], -99.0, -98.0);
        assert!(wind_selector.matches(&wind_message));

        let temp_700 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::Temperature,
            700,
        ))
        .unwrap();
        let temp_message =
            ieee_f32_message(PARAMETER_TMP[0], 100, 70_000.0, &[274.0], -99.0, -99.0);
        assert!(temp_700.matches(&temp_message));

        let rh_700 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::RelativeHumidity,
            700,
        ))
        .unwrap();
        let rh_message = ieee_f32_message(PARAMETER_RH[0], 100, 700.0, &[61.0], -99.0, -99.0);
        assert!(rh_700.matches(&rh_message));

        let dewpoint_850 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::Dewpoint,
            850,
        ))
        .unwrap();
        let dewpoint_message =
            ieee_f32_message(PARAMETER_DPT[0], 100, 850.0, &[281.0], -99.0, -99.0);
        assert!(dewpoint_850.matches(&dewpoint_message));

        let dewpoint_700 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::Dewpoint,
            700,
        ))
        .unwrap();
        let dewpoint_700_message =
            ieee_f32_message(PARAMETER_DPT[0], 100, 700.0, &[270.0], -99.0, -99.0);
        assert!(dewpoint_700.matches(&dewpoint_700_message));

        let vorticity_500 = StructuredMessageSelector::try_from(FieldSelector::isobaric(
            CanonicalField::AbsoluteVorticity,
            500,
        ))
        .unwrap();
        let vorticity_message = ieee_f32_message(
            PARAMETER_ABSOLUTE_VORTICITY[0],
            100,
            500.0,
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
    }

    #[test]
    fn extract_field_from_grib2_returns_selector_backed_field() {
        let message = ieee_f32_message(PARAMETER_TMP[0], 100, 500.0, &[255.0, 256.5], 261.0, 262.0);
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
    fn extract_field_from_real_pressure_bytes_uses_structured_matching() {
        let path = sample_pressure_subset_path();
        assert!(
            path.exists(),
            "expected sample pressure subset at {}",
            path.display()
        );
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
