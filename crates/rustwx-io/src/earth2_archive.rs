use crate::{FetchRequest, FetchResult, IoError, PartialExtraction};
use hdf5_reader::{Datatype, Hdf5File, SliceInfo, SliceInfoElem};
use rustwx_core::{
    CanonicalField, CycleSpec, FieldSelector, GridProjection, GridShape, LatLonGrid, ModelId,
    ModelRunRequest, SelectedField2D, SourceId, VerticalSelector,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

const AIFS_INFERENCE_ARCHIVE_ENV: &str = "RUSTWX_AIFS_INFERENCE_ARCHIVE";
const ARCHIVE_ENV: &str = "RUSTWX_EARTH2_ARCHIVE";
const GEOPOTENTIAL_M2S2_TO_M: f64 = 1.0 / 9.806_65;
const EPSILON: f64 = 0.622;
const EARTH2_LEVELS_HPA: &[u16] = &[
    1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50,
];

thread_local! {
    static HDF5_PATH_CACHE: RefCell<HashMap<PathBuf, Rc<Hdf5File>>> = RefCell::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub struct Earth2SurfaceFields {
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
}

#[derive(Debug, Clone)]
pub struct Earth2PressureFields {
    pub pressure_levels_hpa: Vec<f64>,
    pub temperature_c_3d: Vec<f64>,
    pub qvapor_kgkg_3d: Vec<f64>,
    pub u_ms_3d: Vec<f64>,
    pub v_ms_3d: Vec<f64>,
    pub gh_m_3d: Vec<f64>,
    pub omega_pa_s_3d: Option<Vec<f64>>,
    pub nx: usize,
    pub ny: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Earth2EnsembleSelector {
    /// Backward-compatible deterministic path. If a variable has a leading
    /// member axis, this intentionally selects member 0.
    Deterministic,
    Member(u16),
    Statistic(Earth2EnsembleStat),
}

impl Default for Earth2EnsembleSelector {
    fn default() -> Self {
        Self::Deterministic
    }
}

impl Earth2EnsembleSelector {
    pub fn cache_slug(self) -> String {
        match self {
            Self::Deterministic => "deterministic".to_string(),
            Self::Member(member) => format!("member{member:03}"),
            Self::Statistic(stat) => stat.cache_slug().to_string(),
        }
    }

    pub fn filename_slug(self) -> String {
        match self {
            Self::Deterministic => "deterministic".to_string(),
            Self::Member(member) => format!("m{member}"),
            Self::Statistic(stat) => stat.cache_slug().to_string(),
        }
    }

    pub fn label(self) -> String {
        match self {
            Self::Deterministic => "deterministic".to_string(),
            Self::Member(member) => format!("member {member}"),
            Self::Statistic(stat) => stat.label().to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Earth2EnsembleStat {
    Mean,
    Std,
    Min,
    Max,
    P10,
    P50,
    P90,
}

impl Earth2EnsembleStat {
    pub fn cache_slug(self) -> &'static str {
        match self {
            Self::Mean => "mean",
            Self::Std => "std",
            Self::Min => "min",
            Self::Max => "max",
            Self::P10 => "p10",
            Self::P50 => "p50",
            Self::P90 => "p90",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Mean => "ensemble mean",
            Self::Std => "ensemble std",
            Self::Min => "ensemble min",
            Self::Max => "ensemble max",
            Self::P10 => "ensemble p10",
            Self::P50 => "ensemble p50",
            Self::P90 => "ensemble p90",
        }
    }

    fn aggregate_suffix(self) -> &'static str {
        self.cache_slug()
    }
}

#[derive(Debug, Clone)]
struct Earth2Grid {
    nx: usize,
    ny: usize,
    lat: Vec<f64>,
    lon: Vec<f64>,
    lon_order: Vec<usize>,
}

enum Earth2DataFile<'a> {
    Netcdf(&'a netcrust::File),
    Hdf5(&'a Hdf5File),
}

#[derive(Debug, Clone, Copy)]
enum Transform {
    Identity,
    FractionToPercent,
    GeopotentialToHeight,
    KelvinToCelsius,
    MetersToMillimeters,
}

impl Transform {
    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Identity => value,
            Self::FractionToPercent => value * 100.0,
            Self::GeopotentialToHeight => value * GEOPOTENTIAL_M2S2_TO_M,
            Self::KelvinToCelsius => value - 273.15,
            Self::MetersToMillimeters => value * 1000.0,
        }
    }
}

pub fn is_earth2_archive_fetch(fetch: &FetchRequest) -> bool {
    matches!(
        fetch.source_override,
        Some(SourceId::AifsInference | SourceId::Earth2Archive)
    )
}

pub fn archive_root() -> Result<PathBuf, IoError> {
    std::env::var_os(ARCHIVE_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| IoError::Earth2Archive(format!("{ARCHIVE_ENV} is not set")))
}

pub fn archive_root_for_source(source: SourceId) -> Result<PathBuf, IoError> {
    match source {
        SourceId::AifsInference => std::env::var_os(AIFS_INFERENCE_ARCHIVE_ENV)
            .or_else(|| std::env::var_os(ARCHIVE_ENV))
            .map(PathBuf::from)
            .ok_or_else(|| {
                IoError::Earth2Archive(format!(
                    "{AIFS_INFERENCE_ARCHIVE_ENV} is not set for aifs-inference source"
                ))
            }),
        SourceId::Earth2Archive => archive_root(),
        other => Err(IoError::Earth2Archive(format!(
            "{other} is not a local AIFS NetCDF archive source"
        ))),
    }
}

pub fn archive_path_for_request(request: &ModelRunRequest) -> Result<PathBuf, IoError> {
    archive_path_with_root(archive_root()?, request)
}

pub fn archive_path_for_fetch(fetch: &FetchRequest) -> Result<PathBuf, IoError> {
    let source = local_archive_source(fetch)?;
    archive_path_with_root_and_selector(
        archive_root_for_source(source)?,
        &fetch.request,
        fetch.earth2_ensemble,
    )
}

pub fn archive_path_with_root(
    root: impl AsRef<Path>,
    request: &ModelRunRequest,
) -> Result<PathBuf, IoError> {
    Ok(canonical_archive_path(root.as_ref(), request))
}

fn archive_path_with_root_and_selector(
    root: impl AsRef<Path>,
    request: &ModelRunRequest,
    selector: Option<Earth2EnsembleSelector>,
) -> Result<PathBuf, IoError> {
    let root = root.as_ref();
    let candidates = archive_path_candidates(root, request, selector);
    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }
    if let Some(member) = requested_member(request, selector) {
        if let Some(path) = find_flat_member_file(root, request, member) {
            return Ok(path);
        }
    }
    candidates.into_iter().next().ok_or_else(|| {
        IoError::Earth2Archive(format!(
            "no archive path candidates could be built for {} f{:03}",
            request.model, request.forecast_hour
        ))
    })
}

fn canonical_archive_path(root: &Path, request: &ModelRunRequest) -> PathBuf {
    let init = format!(
        "{}T{:02}Z",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc
    );
    root.join(request.model.as_str())
        .join(init)
        .join(format!("lead{:03}.nc", request.forecast_hour))
}

fn archive_path_candidates(
    root: &Path,
    request: &ModelRunRequest,
    selector: Option<Earth2EnsembleSelector>,
) -> Vec<PathBuf> {
    let init = format!(
        "{}T{:02}Z",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc
    );
    let canonical = canonical_archive_path(root, request);
    let Some(member) = requested_member(request, selector) else {
        return vec![canonical];
    };
    let flat_name = flat_member_file_name(request, member);
    vec![
        root.join(request.model.as_str())
            .join(&init)
            .join(format!("m{member:02}"))
            .join(format!("lead{:03}.nc", request.forecast_hour)),
        root.join(request.model.as_str())
            .join(&init)
            .join(format!("m{member:02}_lead{:03}.nc", request.forecast_hour)),
        root.join(&flat_name),
        root.join(request.model.as_str())
            .join(&init)
            .join(&flat_name),
        canonical,
    ]
}

fn flat_member_file_name(request: &ModelRunRequest, member: u16) -> String {
    format!(
        "aifs_long_{}T{:02}0000Z_m{member:02}_lead{:05}.nc",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, request.forecast_hour
    )
}

fn requested_member(
    request: &ModelRunRequest,
    selector: Option<Earth2EnsembleSelector>,
) -> Option<u16> {
    match selector {
        Some(Earth2EnsembleSelector::Member(member)) => Some(member),
        _ => parse_member_product(&request.product),
    }
}

fn parse_member_product(product: &str) -> Option<u16> {
    let normalized = product
        .trim()
        .trim_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(product)
        .trim()
        .to_ascii_lowercase();
    let digits = normalized
        .strip_prefix('m')
        .or_else(|| normalized.strip_prefix("mem"))
        .or_else(|| normalized.strip_prefix("member"))?;
    if digits.is_empty() || digits.len() > 3 || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u16>().ok()
}

fn find_flat_member_file(root: &Path, request: &ModelRunRequest, member: u16) -> Option<PathBuf> {
    let cycle_token = format!(
        "{}T{:02}0000Z",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc
    );
    let member_token = format!("_m{member:02}_");
    let lead_token = format!("_lead{:05}.nc", request.forecast_hour);
    let mut matches = std::fs::read_dir(root)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_type()
                .map(|file_type| file_type.is_file())
                .unwrap_or(false)
        })
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.contains(&cycle_token)
                && name.contains(&member_token)
                && name.ends_with(&lead_token)
            {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    matches.sort();
    matches.into_iter().next()
}

fn cycle_dir(root: &Path, model: ModelId, cycle: &CycleSpec) -> PathBuf {
    root.join(model.as_str())
        .join(format!("{}T{:02}Z", cycle.date_yyyymmdd, cycle.hour_utc))
}

fn local_archive_source(fetch: &FetchRequest) -> Result<SourceId, IoError> {
    match fetch.source_override {
        Some(SourceId::AifsInference | SourceId::Earth2Archive) => {
            Ok(fetch.source_override.expect("checked source override"))
        }
        Some(other) => Err(IoError::Earth2Archive(format!(
            "{} is local-archive only; requested source {other}",
            fetch.request.model
        ))),
        None => Err(IoError::Earth2Archive(format!(
            "{} local AIFS NetCDF reads require --source aifs-inference or --source earth2-archive",
            fetch.request.model
        ))),
    }
}

pub fn available_leads_for_cycle(model: ModelId, cycle: &CycleSpec) -> Result<Vec<u16>, IoError> {
    let root = archive_root()?;
    Ok(available_leads_for_cycle_with_root(root, model, cycle))
}

pub fn default_forecast_hour_for_archive(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
) -> Result<Option<u16>, IoError> {
    let root = archive_root()?;
    Ok(default_forecast_hour_for_archive_with_root(
        root,
        model,
        date_yyyymmdd,
        cycle_override_utc,
    ))
}

fn default_forecast_hour_for_archive_with_root(
    root: impl AsRef<Path>,
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_override_utc: Option<u8>,
) -> Option<u16> {
    let root = root.as_ref();
    let model_dir = root.join(model.as_str());
    let candidate_dates = cycle_date_rollback_candidates(date_yyyymmdd);
    let mut candidates = Vec::<(String, u8, u16)>::new();
    if let Ok(entries) = std::fs::read_dir(&model_dir) {
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            if !entry
                .file_type()
                .map(|file_type| file_type.is_dir())
                .unwrap_or(false)
            {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(cycle) = parse_archive_cycle_dir(&name) else {
                continue;
            };
            if !candidate_dates
                .iter()
                .any(|candidate| candidate == &cycle.date_yyyymmdd)
            {
                continue;
            }
            if cycle_override_utc.is_some_and(|hour| hour != cycle.hour_utc) {
                continue;
            }
            let Some(first_lead) = available_leads_in_dir(&entry.path()).into_iter().min() else {
                continue;
            };
            candidates.push((cycle.date_yyyymmdd, cycle.hour_utc, first_lead));
        }
    }
    for (cycle, lead) in flat_member_cycles_and_leads(root, model) {
        if !candidate_dates
            .iter()
            .any(|candidate| candidate == &cycle.date_yyyymmdd)
        {
            continue;
        }
        if cycle_override_utc.is_some_and(|hour| hour != cycle.hour_utc) {
            continue;
        }
        candidates.push((cycle.date_yyyymmdd, cycle.hour_utc, lead));
    }
    let (latest_date, latest_hour) = candidates
        .iter()
        .map(|(date, hour, _)| (date.as_str(), *hour))
        .max()?;
    candidates
        .iter()
        .filter(|(date, hour, _)| date == latest_date && *hour == latest_hour)
        .map(|(_, _, lead)| *lead)
        .min()
}

fn available_leads_for_cycle_with_root(
    root: impl AsRef<Path>,
    model: ModelId,
    cycle: &CycleSpec,
) -> Vec<u16> {
    let root = root.as_ref();
    let mut leads = available_leads_in_dir(&cycle_dir(root, model, cycle));
    leads.extend(available_flat_member_leads(root, model, cycle));
    leads.sort_unstable();
    leads.dedup();
    leads
}

fn available_leads_in_dir(cycle_dir: &Path) -> Vec<u16> {
    let Ok(entries) = std::fs::read_dir(cycle_dir) else {
        return Vec::new();
    };
    let mut leads = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_type()
                .map(|file_type| file_type.is_file())
                .unwrap_or(false)
        })
        .filter_map(|entry| {
            let name = entry.file_name();
            parse_lead_file_name(&name.to_string_lossy())
        })
        .collect::<Vec<_>>();
    leads.sort_unstable();
    leads.dedup();
    leads
}

fn parse_lead_file_name(name: &str) -> Option<u16> {
    let lead = if let Some(lead) = name
        .strip_prefix("lead")
        .and_then(|value| value.strip_suffix(".nc"))
    {
        lead
    } else {
        name.rsplit_once("_lead")?.1.strip_suffix(".nc")?
    };
    if lead.len() < 3 || !lead.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    lead.parse::<u16>().ok()
}

fn parse_flat_member_file_name(name: &str) -> Option<(CycleSpec, u16, u16)> {
    let name = name.strip_suffix(".nc")?;
    let (before_lead, lead) = name.rsplit_once("_lead")?;
    let lead = lead.parse::<u16>().ok()?;
    let (before_member, member) = before_lead.rsplit_once("_m")?;
    let member = member.parse::<u16>().ok()?;
    let timestamp = before_member.rsplit('_').next()?;
    if timestamp.len() != 16
        || &timestamp[8..9] != "T"
        || !timestamp.ends_with('Z')
        || &timestamp[11..15] != "0000"
    {
        return None;
    }
    let date = &timestamp[..8];
    let hour = timestamp[9..11].parse::<u8>().ok()?;
    let cycle = CycleSpec::new(date.to_string(), hour).ok()?;
    Some((cycle, member, lead))
}

fn available_flat_member_leads(root: &Path, model: ModelId, cycle: &CycleSpec) -> Vec<u16> {
    flat_member_cycles_and_leads(root, model)
        .into_iter()
        .filter_map(|(found_cycle, lead)| {
            (found_cycle.date_yyyymmdd == cycle.date_yyyymmdd
                && found_cycle.hour_utc == cycle.hour_utc)
                .then_some(lead)
        })
        .collect()
}

fn flat_member_cycles_and_leads(root: &Path, model: ModelId) -> Vec<(CycleSpec, u16)> {
    if model != ModelId::Aifs {
        return Vec::new();
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut values = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_type()
                .map(|file_type| file_type.is_file())
                .unwrap_or(false)
        })
        .filter_map(|entry| {
            let name = entry.file_name();
            parse_flat_member_file_name(&name.to_string_lossy())
                .map(|(cycle, _, lead)| (cycle, lead))
        })
        .collect::<Vec<_>>();
    values.sort_unstable_by(|a, b| {
        a.0.date_yyyymmdd
            .cmp(&b.0.date_yyyymmdd)
            .then(a.0.hour_utc.cmp(&b.0.hour_utc))
            .then(a.1.cmp(&b.1))
    });
    values.dedup();
    values
}

fn parse_archive_cycle_dir(name: &str) -> Option<CycleSpec> {
    if name.len() != 12 || !name.ends_with('Z') || &name[8..9] != "T" {
        return None;
    }
    let date = &name[..8];
    let hour = name[9..11].parse::<u8>().ok()?;
    CycleSpec::new(date.to_string(), hour).ok()
}

fn cycle_date_rollback_candidates(date_yyyymmdd: &str) -> Vec<String> {
    let mut dates = Vec::with_capacity(2);
    dates.push(date_yyyymmdd.to_string());
    if let Some(previous) = previous_day_yyyymmdd(date_yyyymmdd) {
        dates.push(previous);
    }
    dates
}

fn previous_day_yyyymmdd(date_yyyymmdd: &str) -> Option<String> {
    if date_yyyymmdd.len() != 8 {
        return None;
    }
    let year = date_yyyymmdd[0..4].parse::<i32>().ok()?;
    let month = date_yyyymmdd[4..6].parse::<u8>().ok()?;
    let day = date_yyyymmdd[6..8].parse::<u8>().ok()?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let (year, month, day) = if day > 1 {
        (year, month, day - 1)
    } else if month > 1 {
        let prev_month = month - 1;
        (year, prev_month, days_in_month(year, prev_month))
    } else {
        (year - 1, 12, 31)
    };
    Some(format!("{year:04}{month:02}{day:02}"))
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub fn probe_archive(fetch: &FetchRequest) -> Result<crate::ProbeResult, IoError> {
    let source = local_archive_source(fetch)?;
    let path = archive_path_for_fetch(fetch)?;
    Ok(crate::ProbeResult {
        source,
        available: path.is_file(),
        grib_url: file_url(&path),
        idx_url: None,
    })
}

pub fn archive_fetch_available(fetch: &FetchRequest) -> bool {
    archive_path_for_fetch(fetch)
        .map(|path| path.is_file())
        .unwrap_or(false)
}

pub fn fetch_archive_bytes(fetch: &FetchRequest) -> Result<FetchResult, IoError> {
    let source = local_archive_source(fetch)?;
    let path = archive_path_for_fetch(fetch)?;
    if !path.is_file() {
        return Err(IoError::Earth2Archive(format!(
            "archive file is missing: {}",
            path.display()
        )));
    }
    if let Some(selector) = fetch.earth2_ensemble {
        validate_ensemble_selector_for_path(&path, selector)?;
    }
    Ok(FetchResult {
        source,
        url: file_url(&path),
        // Earth2 archives are local NetCDF files. Downstream readers use
        // `CachedFetchResult.bytes_path` to open the file selectively, so
        // keep this empty instead of slurping multi-GB ensemble archives.
        bytes: Vec::new(),
    })
}

pub fn looks_like_earth2_archive(bytes: &[u8]) -> bool {
    netcrust::looks_like_netcdf(bytes)
        && contains_ascii(bytes, b"lat")
        && contains_ascii(bytes, b"lon")
        && (contains_ascii(bytes, b"t2m")
            || contains_ascii(bytes, b"t1000")
            || contains_ascii(bytes, b"member"))
}

pub fn decode_surface_from_bytes(bytes: &[u8]) -> Result<Earth2SurfaceFields, IoError> {
    let file = open_file_from_bytes(bytes)?;
    let file = Earth2DataFile::Netcdf(&file);
    let grid = Earth2Grid::from_data_file(&file)?;
    let sp = read_2d(&file, &grid, "sp")?;
    let d2m = read_2d(&file, &grid, "d2m")?;
    let q2 = sp
        .iter()
        .zip(d2m.iter())
        .map(|(&pressure_pa, &dewpoint_k)| specific_humidity_from_dewpoint(pressure_pa, dewpoint_k))
        .collect::<Vec<_>>();
    Ok(Earth2SurfaceFields {
        lat: grid.lat.clone(),
        lon: grid.lon.clone(),
        nx: grid.nx,
        ny: grid.ny,
        psfc_pa: sp,
        orog_m: vec![0.0; grid.nx * grid.ny],
        t2_k: read_2d(&file, &grid, "t2m")?,
        q2_kgkg: q2,
        u10_ms: read_2d(&file, &grid, "u10m")?,
        v10_ms: read_2d(&file, &grid, "v10m")?,
    })
}

pub fn decode_surface_from_path(path: &Path) -> Result<Earth2SurfaceFields, IoError> {
    let file = open_hdf5_mmap(path)?;
    let file = Earth2DataFile::Hdf5(&file);
    let grid = Earth2Grid::from_data_file(&file)?;
    let sp = read_2d(&file, &grid, "sp")?;
    let d2m = read_2d(&file, &grid, "d2m")?;
    let q2 = sp
        .iter()
        .zip(d2m.iter())
        .map(|(&pressure_pa, &dewpoint_k)| specific_humidity_from_dewpoint(pressure_pa, dewpoint_k))
        .collect::<Vec<_>>();
    Ok(Earth2SurfaceFields {
        lat: grid.lat.clone(),
        lon: grid.lon.clone(),
        nx: grid.nx,
        ny: grid.ny,
        psfc_pa: sp,
        orog_m: vec![0.0; grid.nx * grid.ny],
        t2_k: read_2d(&file, &grid, "t2m")?,
        q2_kgkg: q2,
        u10_ms: read_2d(&file, &grid, "u10m")?,
        v10_ms: read_2d(&file, &grid, "v10m")?,
    })
}

pub fn decode_pressure_from_bytes(bytes: &[u8]) -> Result<Earth2PressureFields, IoError> {
    let file = open_file_from_bytes(bytes)?;
    let file = Earth2DataFile::Netcdf(&file);
    let grid = Earth2Grid::from_data_file(&file)?;
    decode_pressure_from_file(&file, &grid)
}

pub fn decode_pressure_from_path(path: &Path) -> Result<Earth2PressureFields, IoError> {
    let file = open_hdf5_mmap(path)?;
    let file = Earth2DataFile::Hdf5(&file);
    let grid = Earth2Grid::from_data_file(&file)?;
    decode_pressure_from_file(&file, &grid)
}

pub fn ensemble_member_count_for_path(path: &Path) -> Result<Option<usize>, IoError> {
    if let Ok(file) = open_hdf5_mmap(path) {
        if let Some(count) = ensemble_member_count_from_hdf5(&file)? {
            return Ok(Some(count));
        }
    }
    let file = open_file_from_path(path)?;
    Ok(ensemble_member_count_from_file(&file))
}

pub fn validate_ensemble_selector_for_path(
    path: &Path,
    selector: Earth2EnsembleSelector,
) -> Result<(), IoError> {
    let Earth2EnsembleSelector::Member(member) = selector else {
        return Ok(());
    };
    let Some(member_count) = ensemble_member_count_for_path(path)? else {
        if path_matches_member(path, member) {
            return Ok(());
        }
        return Err(IoError::Earth2Archive(format!(
            "member {member} requested for {}, but no member dimension, ensemble_size attribute, or member-specific file name was found",
            path.display()
        )));
    };
    if usize::from(member) >= member_count {
        return Err(IoError::Earth2Archive(format!(
            "member {member} requested for {}, but file has {member_count} members",
            path.display()
        )));
    }
    Ok(())
}

fn path_matches_member(path: &Path, member: u16) -> bool {
    path_member(path).is_some_and(|path_member| path_member == member)
}

fn path_member(path: &Path) -> Option<u16> {
    if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
        if let Some((_, member, _)) = parse_flat_member_file_name(name) {
            return Some(member);
        }
        if let Some((before_lead, _)) = name.strip_suffix(".nc")?.rsplit_once("_lead") {
            if let Some(member_text) = before_lead.rsplit_once("_m").map(|(_, value)| value) {
                if member_text.chars().all(|ch| ch.is_ascii_digit()) {
                    return member_text.parse::<u16>().ok();
                }
            }
        }
    }
    path.parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .and_then(parse_member_product)
}

pub fn extract_fields_partial_from_bytes(
    bytes: &[u8],
    preferred_path: Option<&Path>,
    selectors: &[FieldSelector],
) -> Result<PartialExtraction, IoError> {
    extract_fields_partial_from_bytes_with_selector(
        bytes,
        preferred_path,
        selectors,
        Earth2EnsembleSelector::Deterministic,
    )
}

pub fn extract_fields_partial_from_bytes_with_selector(
    bytes: &[u8],
    preferred_path: Option<&Path>,
    selectors: &[FieldSelector],
    ensemble_selector: Earth2EnsembleSelector,
) -> Result<PartialExtraction, IoError> {
    if let Some(path) = preferred_path.filter(|path| path.is_file()) {
        if let Ok(file) = open_hdf5_mmap(path) {
            return extract_fields_partial_from_data_file(
                &Earth2DataFile::Hdf5(&file),
                selectors,
                ensemble_selector,
            );
        }
    }

    let file = open_file(bytes, preferred_path)?;
    extract_fields_partial_from_data_file(
        &Earth2DataFile::Netcdf(&file),
        selectors,
        ensemble_selector,
    )
}

fn extract_fields_partial_from_data_file(
    file: &Earth2DataFile<'_>,
    selectors: &[FieldSelector],
    ensemble_selector: Earth2EnsembleSelector,
) -> Result<PartialExtraction, IoError> {
    let grid = Earth2Grid::from_data_file(file)?;
    let latlon = grid.latlon_grid()?;
    let mut extracted = Vec::new();
    let mut missing = Vec::new();

    for &selector in selectors {
        match read_selector(file, &grid, selector, ensemble_selector)? {
            Some((values, units)) => {
                extracted.push(
                    SelectedField2D::new(
                        selector,
                        units,
                        latlon.clone(),
                        values.into_iter().map(|value| value as f32).collect(),
                    )
                    .map_err(IoError::Core)?
                    .with_projection(GridProjection::Geographic),
                );
            }
            None => missing.push(selector),
        }
    }

    Ok(PartialExtraction { extracted, missing })
}

fn open_file(bytes: &[u8], preferred_path: Option<&Path>) -> Result<netcrust::File, IoError> {
    let options = netcrust::NcOpenOptions {
        metadata_mode: netcrust::NcMetadataMode::Lossy,
        ..Default::default()
    };
    if let Some(path) = preferred_path {
        if path.is_file() {
            return netcrust::File::open_with_options(path, options)
                .map_err(|err| IoError::Earth2Archive(err.to_string()));
        }
    }
    netcrust::File::from_bytes_with_options(bytes, options)
        .map_err(|err| IoError::Earth2Archive(err.to_string()))
}

fn open_file_from_path(path: &Path) -> Result<netcrust::File, IoError> {
    let options = netcrust::NcOpenOptions {
        metadata_mode: netcrust::NcMetadataMode::Lossy,
        ..Default::default()
    };
    netcrust::File::open_with_options(path, options)
        .map_err(|err| IoError::Earth2Archive(err.to_string()))
}

fn open_file_from_bytes(bytes: &[u8]) -> Result<netcrust::File, IoError> {
    open_file(bytes, None)
}

fn ensemble_member_count_from_file(file: &netcrust::File) -> Option<usize> {
    file.dimension("member")
        .map(|dimension| dimension.len())
        .filter(|len| *len > 0)
        .or_else(|| {
            file.attribute("ensemble_size")
                .and_then(|attribute| attribute.as_f64())
                .filter(|value| value.is_finite() && *value > 0.0)
                .map(|value| value.round() as usize)
        })
}

fn ensemble_member_count_from_hdf5(file: &Hdf5File) -> Result<Option<usize>, IoError> {
    if let Ok(member) = file.dataset("member") {
        let shape = hdf5_dataset_shape(&member, "member")?;
        if let Some(len) = shape.first().copied().filter(|len| *len > 0) {
            return Ok(Some(len));
        }
    }
    for name in ["t2m", "u10m", "v10m", "sp", "msl"] {
        let Ok(dataset) = file.dataset(name) else {
            continue;
        };
        let shape = hdf5_dataset_shape(&dataset, name)?;
        if let [members, _, _] = shape.as_slice() {
            if *members > 0 {
                return Ok(Some(*members));
            }
        }
    }
    Ok(None)
}

impl Earth2Grid {
    fn from_data_file(file: &Earth2DataFile<'_>) -> Result<Self, IoError> {
        match file {
            Earth2DataFile::Netcdf(file) => Self::from_file(file),
            Earth2DataFile::Hdf5(file) => Self::from_hdf5(file),
        }
    }

    fn from_file(file: &netcrust::File) -> Result<Self, IoError> {
        let lat_raw = file
            .read_f64("lat")
            .map_err(|err| IoError::Earth2Archive(format!("failed to read lat: {err}")))?;
        let lon_raw = file
            .read_f64("lon")
            .map_err(|err| IoError::Earth2Archive(format!("failed to read lon: {err}")))?;
        if lat_raw.is_empty() || lon_raw.is_empty() {
            return Err(IoError::Earth2Archive(
                "lat/lon coordinates are empty".to_string(),
            ));
        }
        let nx = lon_raw.len();
        let ny = lat_raw.len();
        let mut lon_order = lon_raw
            .iter()
            .enumerate()
            .map(|(index, &lon)| (index, normalize_lon(lon)))
            .collect::<Vec<_>>();
        lon_order.sort_by(|a, b| a.1.total_cmp(&b.1));
        let lon_order = lon_order
            .into_iter()
            .map(|(index, _)| index)
            .collect::<Vec<_>>();

        let mut lat = Vec::with_capacity(nx * ny);
        let mut lon = Vec::with_capacity(nx * ny);
        for &lat_value in &lat_raw {
            for &source_x in &lon_order {
                lat.push(lat_value);
                lon.push(normalize_lon(lon_raw[source_x]));
            }
        }

        Ok(Self {
            nx,
            ny,
            lat,
            lon,
            lon_order,
        })
    }

    fn from_hdf5(file: &Hdf5File) -> Result<Self, IoError> {
        let lat_raw = read_hdf5_named_values_f64(file, "lat")?;
        let lon_raw = read_hdf5_named_values_f64(file, "lon")?;
        Self::from_lat_lon(lat_raw, lon_raw)
    }

    fn from_lat_lon(lat_raw: Vec<f64>, lon_raw: Vec<f64>) -> Result<Self, IoError> {
        if lat_raw.is_empty() || lon_raw.is_empty() {
            return Err(IoError::Earth2Archive(
                "lat/lon coordinates are empty".to_string(),
            ));
        }
        let nx = lon_raw.len();
        let ny = lat_raw.len();
        let mut lon_order = lon_raw
            .iter()
            .enumerate()
            .map(|(index, &lon)| (index, normalize_lon(lon)))
            .collect::<Vec<_>>();
        lon_order.sort_by(|a, b| a.1.total_cmp(&b.1));
        let lon_order = lon_order
            .into_iter()
            .map(|(index, _)| index)
            .collect::<Vec<_>>();

        let mut lat = Vec::with_capacity(nx * ny);
        let mut lon = Vec::with_capacity(nx * ny);
        for &lat_value in &lat_raw {
            for &source_x in &lon_order {
                lat.push(lat_value);
                lon.push(normalize_lon(lon_raw[source_x]));
            }
        }

        Ok(Self {
            nx,
            ny,
            lat,
            lon,
            lon_order,
        })
    }

    fn latlon_grid(&self) -> Result<LatLonGrid, IoError> {
        LatLonGrid::new(
            GridShape::new(self.nx, self.ny)?,
            self.lat.iter().map(|&value| value as f32).collect(),
            self.lon.iter().map(|&value| value as f32).collect(),
        )
        .map_err(IoError::Core)
    }
}

fn read_selector(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
    selector: FieldSelector,
    ensemble_selector: Earth2EnsembleSelector,
) -> Result<Option<(Vec<f64>, &'static str)>, IoError> {
    match (selector.field, selector.vertical) {
        (CanonicalField::Temperature, VerticalSelector::HeightAboveGroundMeters(2)) => {
            read_optional_transformed(file, grid, "t2m", Transform::Identity, ensemble_selector)
                .map_units("K")
        }
        (CanonicalField::Dewpoint, VerticalSelector::HeightAboveGroundMeters(2)) => {
            read_optional_transformed(file, grid, "d2m", Transform::Identity, ensemble_selector)
                .map_units("K")
        }
        (CanonicalField::RelativeHumidity, VerticalSelector::HeightAboveGroundMeters(2)) => {
            if !has_var(file, "t2m") || !has_var(file, "d2m") {
                return Ok(None);
            }
            let t = read_2d_selected(file, grid, "t2m", ensemble_selector)?;
            let td = read_2d_selected(file, grid, "d2m", ensemble_selector)?;
            Ok(Some((
                t.iter()
                    .zip(td.iter())
                    .map(|(&t, &td)| relative_humidity_from_t_td(t, td))
                    .collect(),
                "%",
            )))
        }
        (CanonicalField::UWind, VerticalSelector::HeightAboveGroundMeters(10)) => {
            read_optional_transformed(file, grid, "u10m", Transform::Identity, ensemble_selector)
                .map_units("m/s")
        }
        (CanonicalField::VWind, VerticalSelector::HeightAboveGroundMeters(10)) => {
            read_optional_transformed(file, grid, "v10m", Transform::Identity, ensemble_selector)
                .map_units("m/s")
        }
        (CanonicalField::Pressure, VerticalSelector::Surface) => {
            read_optional_transformed(file, grid, "sp", Transform::Identity, ensemble_selector)
                .map_units("Pa")
        }
        (CanonicalField::PressureReducedToMeanSeaLevel, VerticalSelector::MeanSeaLevel) => {
            read_optional_transformed(file, grid, "msl", Transform::Identity, ensemble_selector)
                .map_units("Pa")
        }
        (CanonicalField::PrecipitableWater, VerticalSelector::EntireAtmosphere) => {
            read_optional_transformed(file, grid, "tcw", Transform::Identity, ensemble_selector)
                .map_units("kg/m^2")
        }
        (CanonicalField::TotalCloudCover, VerticalSelector::EntireAtmosphere) => {
            read_optional_transformed(
                file,
                grid,
                "tcc",
                Transform::FractionToPercent,
                ensemble_selector,
            )
            .map_units("%")
        }
        (CanonicalField::LowCloudCover, VerticalSelector::EntireAtmosphere) => {
            read_optional_transformed(
                file,
                grid,
                "lcc",
                Transform::FractionToPercent,
                ensemble_selector,
            )
            .map_units("%")
        }
        (CanonicalField::MiddleCloudCover, VerticalSelector::EntireAtmosphere) => {
            read_optional_transformed(
                file,
                grid,
                "mcc",
                Transform::FractionToPercent,
                ensemble_selector,
            )
            .map_units("%")
        }
        (CanonicalField::HighCloudCover, VerticalSelector::EntireAtmosphere) => {
            read_optional_transformed(
                file,
                grid,
                "hcc",
                Transform::FractionToPercent,
                ensemble_selector,
            )
            .map_units("%")
        }
        (CanonicalField::TotalPrecipitation, VerticalSelector::Surface) => {
            if let Some(values) = read_optional_transformed(
                file,
                grid,
                "tp06",
                Transform::Identity,
                ensemble_selector,
            )? {
                Ok(Some((values, "kg/m^2")))
            } else {
                read_optional_transformed(
                    file,
                    grid,
                    "tp",
                    Transform::MetersToMillimeters,
                    ensemble_selector,
                )
                .map_units("kg/m^2")
            }
        }
        (field, VerticalSelector::IsobaricHpa(level)) => {
            read_pressure_selector(file, grid, field, level, ensemble_selector)
        }
        _ => Ok(None),
    }
}

fn read_pressure_selector(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
    field: CanonicalField,
    level: u16,
    ensemble_selector: Earth2EnsembleSelector,
) -> Result<Option<(Vec<f64>, &'static str)>, IoError> {
    let level_suffix = level.to_string();
    match field {
        CanonicalField::Temperature => read_optional_transformed(
            file,
            grid,
            &format!("t{level_suffix}"),
            Transform::Identity,
            ensemble_selector,
        )
        .map_units("K"),
        CanonicalField::UWind => read_optional_transformed(
            file,
            grid,
            &format!("u{level_suffix}"),
            Transform::Identity,
            ensemble_selector,
        )
        .map_units("m/s"),
        CanonicalField::VWind => read_optional_transformed(
            file,
            grid,
            &format!("v{level_suffix}"),
            Transform::Identity,
            ensemble_selector,
        )
        .map_units("m/s"),
        CanonicalField::GeopotentialHeight => read_optional_transformed(
            file,
            grid,
            &format!("z{level_suffix}"),
            Transform::GeopotentialToHeight,
            ensemble_selector,
        )
        .map_units("gpm"),
        CanonicalField::Dewpoint => {
            let q_name = format!("q{level_suffix}");
            if !has_var(file, &q_name) {
                return Ok(None);
            }
            Ok(Some((
                read_2d_selected(file, grid, &q_name, ensemble_selector)?
                    .into_iter()
                    .map(|q| dewpoint_from_specific_humidity(level as f64 * 100.0, q))
                    .collect(),
                "K",
            )))
        }
        CanonicalField::RelativeHumidity => {
            let t_name = format!("t{level_suffix}");
            let q_name = format!("q{level_suffix}");
            if !has_var(file, &t_name) || !has_var(file, &q_name) {
                return Ok(None);
            }
            let t = read_2d_selected(file, grid, &t_name, ensemble_selector)?;
            let q = read_2d_selected(file, grid, &q_name, ensemble_selector)?;
            Ok(Some((
                t.iter()
                    .zip(q.iter())
                    .map(|(&t, &q)| {
                        relative_humidity_from_t_td(
                            t,
                            dewpoint_from_specific_humidity(level as f64 * 100.0, q),
                        )
                    })
                    .collect(),
                "%",
            )))
        }
        _ => Ok(None),
    }
}

fn decode_pressure_from_file(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
) -> Result<Earth2PressureFields, IoError> {
    let expected = grid.nx * grid.ny;
    let mut pressure_levels_hpa = Vec::new();
    let mut temperature_c_3d = Vec::new();
    let mut qvapor_kgkg_3d = Vec::new();
    let mut u_ms_3d = Vec::new();
    let mut v_ms_3d = Vec::new();
    let mut gh_m_3d = Vec::new();
    let mut omega_levels = Vec::new();
    let mut have_all_omega = true;

    for &level in EARTH2_LEVELS_HPA {
        let suffix = level.to_string();
        let required = [
            format!("t{suffix}"),
            format!("q{suffix}"),
            format!("u{suffix}"),
            format!("v{suffix}"),
            format!("z{suffix}"),
        ];
        if required.iter().any(|name| !has_var(file, name)) {
            continue;
        }
        pressure_levels_hpa.push(level as f64);
        temperature_c_3d.extend(
            read_2d(file, grid, &required[0])?
                .into_iter()
                .map(|value| Transform::KelvinToCelsius.apply(value)),
        );
        qvapor_kgkg_3d.extend(read_2d(file, grid, &required[1])?);
        u_ms_3d.extend(read_2d(file, grid, &required[2])?);
        v_ms_3d.extend(read_2d(file, grid, &required[3])?);
        gh_m_3d.extend(
            read_2d(file, grid, &required[4])?
                .into_iter()
                .map(|value| Transform::GeopotentialToHeight.apply(value)),
        );
        let omega_name = format!("w{suffix}");
        if has_var(file, &omega_name) {
            omega_levels.extend(read_2d(file, grid, &omega_name)?);
        } else {
            have_all_omega = false;
        }
    }

    if pressure_levels_hpa.is_empty() {
        return Err(IoError::Earth2Archive(
            "no complete pressure levels found in Earth2 archive file".to_string(),
        ));
    }
    let expected_volume_len = pressure_levels_hpa.len() * expected;
    if temperature_c_3d.len() != expected_volume_len
        || qvapor_kgkg_3d.len() != expected_volume_len
        || u_ms_3d.len() != expected_volume_len
        || v_ms_3d.len() != expected_volume_len
        || gh_m_3d.len() != expected_volume_len
    {
        return Err(IoError::Earth2Archive(
            "decoded pressure volume had inconsistent sizes".to_string(),
        ));
    }

    Ok(Earth2PressureFields {
        pressure_levels_hpa,
        temperature_c_3d,
        qvapor_kgkg_3d,
        u_ms_3d,
        v_ms_3d,
        gh_m_3d,
        omega_pa_s_3d: (have_all_omega && omega_levels.len() == expected_volume_len)
            .then_some(omega_levels),
        nx: grid.nx,
        ny: grid.ny,
    })
}

trait OptionalUnits {
    fn map_units(self, units: &'static str) -> Result<Option<(Vec<f64>, &'static str)>, IoError>;
}

impl OptionalUnits for Result<Option<Vec<f64>>, IoError> {
    fn map_units(self, units: &'static str) -> Result<Option<(Vec<f64>, &'static str)>, IoError> {
        self.map(|values| values.map(|values| (values, units)))
    }
}

fn read_optional_transformed(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
    name: &str,
    transform: Transform,
    ensemble_selector: Earth2EnsembleSelector,
) -> Result<Option<Vec<f64>>, IoError> {
    if !has_var(file, name) {
        return Ok(None);
    }
    Ok(Some(
        read_2d_selected(file, grid, name, ensemble_selector)?
            .into_iter()
            .map(|value| transform.apply(value))
            .collect(),
    ))
}

fn read_2d(file: &Earth2DataFile<'_>, grid: &Earth2Grid, name: &str) -> Result<Vec<f64>, IoError> {
    match file {
        Earth2DataFile::Hdf5(file) => read_hdf5_2d(file, grid, name, None)?.ok_or_else(|| {
            IoError::Earth2Archive(format!("variable {name} not found in Earth2 HDF5 file"))
        }),
        Earth2DataFile::Netcdf(file) => {
            if let Some(values) = read_hdf5_path_2d(file, grid, name, None)? {
                return Ok(values);
            }

            let array = file
                .read_array_f64_first_record_or_all(name)
                .map_err(|err| IoError::Earth2Archive(format!("failed to read {name}: {err}")))?;
            let shape = array.shape().to_vec();
            let values = array.into_values();
            let expected = grid.nx * grid.ny;
            if values.len() != expected {
                return Err(IoError::Earth2Archive(format!(
                    "variable {name} had {} values, expected {expected}",
                    values.len()
                )));
            }
            match shape.as_slice() {
                [ny, nx] if *ny == grid.ny && *nx == grid.nx => Ok(reorder_lon(values, grid)),
                [len] if *len == expected => Ok(reorder_lon(values, grid)),
                _ => Err(IoError::Earth2Archive(format!(
                    "variable {name} had unsupported shape {shape:?}; expected [{}, {}]",
                    grid.ny, grid.nx
                ))),
            }
        }
    }
}

fn read_2d_selected(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
    name: &str,
    selector: Earth2EnsembleSelector,
) -> Result<Vec<f64>, IoError> {
    if let Earth2EnsembleSelector::Statistic(stat) = selector {
        let aggregate_name = format!("{}_{}", name, stat.aggregate_suffix());
        if has_var(file, &aggregate_name) {
            return read_2d(file, grid, &aggregate_name);
        }
    }
    if let Some(values) = read_member_axis_2d_slice(file, grid, name, selector)? {
        return Ok(values);
    }

    match file {
        Earth2DataFile::Hdf5(file) => read_2d_selected_hdf5(file, grid, name, selector),
        Earth2DataFile::Netcdf(file) => {
            let array = file
                .read_array_f64(name)
                .map_err(|err| IoError::Earth2Archive(format!("failed to read {name}: {err}")))?;
            let shape = array.shape().to_vec();
            let values = array.into_values();
            let expected = grid.nx * grid.ny;
            match shape.as_slice() {
                [ny, nx] if *ny == grid.ny && *nx == grid.nx => match selector {
                    Earth2EnsembleSelector::Deterministic | Earth2EnsembleSelector::Member(_) => {
                        Ok(reorder_lon(values, grid))
                    }
                    Earth2EnsembleSelector::Statistic(stat) => {
                        Err(IoError::Earth2Archive(format!(
                            "{} selector requested for deterministic variable {name}; no aggregate variable {}_{} was present",
                            stat.label(),
                            name,
                            stat.aggregate_suffix()
                        )))
                    }
                },
                [len] if *len == expected => match selector {
                    Earth2EnsembleSelector::Deterministic | Earth2EnsembleSelector::Member(_) => {
                        Ok(reorder_lon(values, grid))
                    }
                    Earth2EnsembleSelector::Statistic(stat) => {
                        Err(IoError::Earth2Archive(format!(
                            "{} selector requested for deterministic variable {name}; no aggregate variable {}_{} was present",
                            stat.label(),
                            name,
                            stat.aggregate_suffix()
                        )))
                    }
                },
                [members, ny, nx] if *ny == grid.ny && *nx == grid.nx => {
                    read_member_shaped_values(name, values, *members, expected, grid, selector)
                }
                _ => Err(IoError::Earth2Archive(format!(
                    "variable {name} had unsupported shape {shape:?}; expected [{}, {}] or [member, {}, {}]",
                    grid.ny, grid.nx, grid.ny, grid.nx
                ))),
            }
        }
    }
}

fn read_member_axis_2d_slice(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
    name: &str,
    selector: Earth2EnsembleSelector,
) -> Result<Option<Vec<f64>>, IoError> {
    let Some(shape) = variable_shape(file, name)? else {
        return Ok(None);
    };
    let [members, ny, nx] = shape.as_slice() else {
        return Ok(None);
    };
    if *ny != grid.ny || *nx != grid.nx {
        return Ok(None);
    }
    let member_index = match selector {
        Earth2EnsembleSelector::Deterministic => 0,
        Earth2EnsembleSelector::Member(member) => usize::from(member),
        Earth2EnsembleSelector::Statistic(_) => return Ok(None),
    };
    if member_index >= *members {
        return Err(IoError::Earth2Archive(format!(
            "member {member_index} requested for variable {name}, but file has {members} members"
        )));
    }
    read_member_2d_slice(file, grid, name, member_index).map(Some)
}

fn read_member_2d_slice(
    file: &Earth2DataFile<'_>,
    grid: &Earth2Grid,
    name: &str,
    member_index: usize,
) -> Result<Vec<f64>, IoError> {
    match file {
        Earth2DataFile::Hdf5(file) => {
            return read_hdf5_2d(file, grid, name, Some(member_index))?.ok_or_else(|| {
                IoError::Earth2Archive(format!("variable {name} not found in Earth2 HDF5 file"))
            });
        }
        Earth2DataFile::Netcdf(file) => {
            if let Some(values) = read_hdf5_path_2d(file, grid, name, Some(member_index))? {
                return Ok(values);
            }

            let selection = netcrust::NcSliceInfo {
                selections: vec![
                    netcrust::NcSliceInfoElem::Index(member_index as u64),
                    netcrust::NcSliceInfoElem::Slice {
                        start: 0,
                        end: u64::MAX,
                        step: 1,
                    },
                    netcrust::NcSliceInfoElem::Slice {
                        start: 0,
                        end: u64::MAX,
                        step: 1,
                    },
                ],
            };
            let array = file.read_array_f64_slice(name, &selection).map_err(|err| {
                IoError::Earth2Archive(format!(
                    "failed to read member {member_index} slice for {name}: {err}"
                ))
            })?;
            let shape = array.shape().to_vec();
            let values = array.into_values();
            let expected = grid.nx * grid.ny;
            if values.len() != expected {
                return Err(IoError::Earth2Archive(format!(
                    "member {member_index} slice for variable {name} had {} values, expected {expected}",
                    values.len()
                )));
            }
            match shape.as_slice() {
                [ny, nx] if *ny == grid.ny && *nx == grid.nx => Ok(reorder_lon(values, grid)),
                [len] if *len == expected => Ok(reorder_lon(values, grid)),
                _ => Err(IoError::Earth2Archive(format!(
                    "member {member_index} slice for variable {name} had unsupported shape {shape:?}; expected [{}, {}]",
                    grid.ny, grid.nx
                ))),
            }
        }
    }
}

fn read_2d_selected_hdf5(
    file: &Hdf5File,
    grid: &Earth2Grid,
    name: &str,
    selector: Earth2EnsembleSelector,
) -> Result<Vec<f64>, IoError> {
    let dataset = file
        .dataset(name)
        .map_err(|err| IoError::Earth2Archive(format!("failed to open {name}: {err}")))?;
    let shape = hdf5_dataset_shape(&dataset, name)?;
    let values = read_hdf5_values_f64(&dataset)?;
    let expected = grid.nx * grid.ny;
    match shape.as_slice() {
        [ny, nx] if *ny == grid.ny && *nx == grid.nx => match selector {
            Earth2EnsembleSelector::Deterministic | Earth2EnsembleSelector::Member(_) => {
                Ok(reorder_lon(values, grid))
            }
            Earth2EnsembleSelector::Statistic(stat) => Err(IoError::Earth2Archive(format!(
                "{} selector requested for deterministic variable {name}; no aggregate variable {}_{} was present",
                stat.label(),
                name,
                stat.aggregate_suffix()
            ))),
        },
        [len] if *len == expected => match selector {
            Earth2EnsembleSelector::Deterministic | Earth2EnsembleSelector::Member(_) => {
                Ok(reorder_lon(values, grid))
            }
            Earth2EnsembleSelector::Statistic(stat) => Err(IoError::Earth2Archive(format!(
                "{} selector requested for deterministic variable {name}; no aggregate variable {}_{} was present",
                stat.label(),
                name,
                stat.aggregate_suffix()
            ))),
        },
        [members, ny, nx] if *ny == grid.ny && *nx == grid.nx => {
            read_member_shaped_values(name, values, *members, expected, grid, selector)
        }
        _ => Err(IoError::Earth2Archive(format!(
            "variable {name} had unsupported shape {shape:?}; expected [{}, {}] or [member, {}, {}]",
            grid.ny, grid.nx, grid.ny, grid.nx
        ))),
    }
}

fn read_hdf5_2d(
    file: &Hdf5File,
    grid: &Earth2Grid,
    name: &str,
    member_index: Option<usize>,
) -> Result<Option<Vec<f64>>, IoError> {
    let dataset = match file.dataset(name) {
        Ok(dataset) => dataset,
        Err(_) => return Ok(None),
    };
    let shape = hdf5_dataset_shape(&dataset, name)?;
    let expected = grid.nx * grid.ny;

    let values = match (member_index, shape.as_slice()) {
        (Some(member), [members, ny, nx]) if *ny == grid.ny && *nx == grid.nx => {
            if member >= *members {
                return Err(IoError::Earth2Archive(format!(
                    "member {member} requested for variable {name}, but file has {members} members"
                )));
            }
            let selection = SliceInfo {
                selections: vec![
                    SliceInfoElem::Index(member as u64),
                    SliceInfoElem::Slice {
                        start: 0,
                        end: u64::MAX,
                        step: 1,
                    },
                    SliceInfoElem::Slice {
                        start: 0,
                        end: u64::MAX,
                        step: 1,
                    },
                ],
            };
            read_hdf5_values_f64_slice(&dataset, &selection)?
        }
        (None, [members, ny, nx]) if *ny == grid.ny && *nx == grid.nx => {
            if *members == 0 {
                return Err(IoError::Earth2Archive(format!(
                    "variable {name} has an empty member dimension"
                )));
            }
            let selection = SliceInfo {
                selections: vec![
                    SliceInfoElem::Index(0),
                    SliceInfoElem::Slice {
                        start: 0,
                        end: u64::MAX,
                        step: 1,
                    },
                    SliceInfoElem::Slice {
                        start: 0,
                        end: u64::MAX,
                        step: 1,
                    },
                ],
            };
            read_hdf5_values_f64_slice(&dataset, &selection)?
        }
        (None, [ny, nx]) if *ny == grid.ny && *nx == grid.nx => read_hdf5_values_f64(&dataset)?,
        (None, [len]) if *len == expected => read_hdf5_values_f64(&dataset)?,
        (Some(member), _) => {
            return Err(IoError::Earth2Archive(format!(
                "member {member} selector requested for variable {name} with unsupported shape {shape:?}; expected [member, {}, {}]",
                grid.ny, grid.nx
            )));
        }
        _ => return Ok(None),
    };

    if values.len() != expected {
        return Err(IoError::Earth2Archive(format!(
            "variable {name} had {} values, expected {expected}",
            values.len()
        )));
    }
    Ok(Some(reorder_lon(values, grid)))
}

fn variable_shape(file: &Earth2DataFile<'_>, name: &str) -> Result<Option<Vec<usize>>, IoError> {
    match file {
        Earth2DataFile::Netcdf(file) => Ok(file.variable(name).map(|variable| variable.shape())),
        Earth2DataFile::Hdf5(file) => match file.dataset(name) {
            Ok(dataset) => hdf5_dataset_shape(&dataset, name).map(Some),
            Err(_) => Ok(None),
        },
    }
}

fn hdf5_dataset_shape(dataset: &hdf5_reader::Dataset, name: &str) -> Result<Vec<usize>, IoError> {
    dataset
        .shape()
        .iter()
        .map(|&value| usize::try_from(value).map_err(|err| err.to_string()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            IoError::Earth2Archive(format!("shape for variable {name} was too large: {err}"))
        })
}

fn read_hdf5_named_values_f64(file: &Hdf5File, name: &str) -> Result<Vec<f64>, IoError> {
    let dataset = file
        .dataset(name)
        .map_err(|err| IoError::Earth2Archive(format!("failed to open {name}: {err}")))?;
    read_hdf5_values_f64(&dataset)
}

fn read_hdf5_path_2d(
    file: &netcrust::File,
    grid: &Earth2Grid,
    name: &str,
    member_index: Option<usize>,
) -> Result<Option<Vec<f64>>, IoError> {
    let Some(path) = file.path() else {
        return Ok(None);
    };
    let expected = grid.nx * grid.ny;
    let values = with_hdf5_path(path, |hdf5| {
        let dataset = match hdf5.dataset(name) {
            Ok(dataset) => dataset,
            Err(_) => return Ok(None),
        };
        let shape = dataset
            .shape()
            .iter()
            .map(|&value| usize::try_from(value).map_err(|err| err.to_string()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                IoError::Earth2Archive(format!("shape for variable {name} was too large: {err}"))
            })?;

        let values = match (member_index, shape.as_slice()) {
            (Some(member), [members, ny, nx]) if *ny == grid.ny && *nx == grid.nx => {
                if member >= *members {
                    return Err(IoError::Earth2Archive(format!(
                        "member {member} requested for variable {name}, but file has {members} members"
                    )));
                }
                let selection = SliceInfo {
                    selections: vec![
                        SliceInfoElem::Index(member as u64),
                        SliceInfoElem::Slice {
                            start: 0,
                            end: u64::MAX,
                            step: 1,
                        },
                        SliceInfoElem::Slice {
                            start: 0,
                            end: u64::MAX,
                            step: 1,
                        },
                    ],
                };
                read_hdf5_values_f64_slice(&dataset, &selection)?
            }
            (None, [members, ny, nx]) if *ny == grid.ny && *nx == grid.nx => {
                if *members == 0 {
                    return Err(IoError::Earth2Archive(format!(
                        "variable {name} has an empty member dimension"
                    )));
                }
                let selection = SliceInfo {
                    selections: vec![
                        SliceInfoElem::Index(0),
                        SliceInfoElem::Slice {
                            start: 0,
                            end: u64::MAX,
                            step: 1,
                        },
                        SliceInfoElem::Slice {
                            start: 0,
                            end: u64::MAX,
                            step: 1,
                        },
                    ],
                };
                read_hdf5_values_f64_slice(&dataset, &selection)?
            }
            (None, [ny, nx]) if *ny == grid.ny && *nx == grid.nx => read_hdf5_values_f64(&dataset)?,
            (None, [len]) if *len == expected => read_hdf5_values_f64(&dataset)?,
            (Some(member), _) => {
                return Err(IoError::Earth2Archive(format!(
                    "member {member} selector requested for variable {name} with unsupported shape {shape:?}; expected [member, {}, {}]",
                    grid.ny, grid.nx
                )));
            }
            _ => return Ok(None),
        };
        Ok(Some(values))
    })?;

    let Some(values) = values else {
        return Ok(None);
    };

    if values.len() != expected {
        return Err(IoError::Earth2Archive(format!(
            "variable {name} had {} values, expected {expected}",
            values.len()
        )));
    }
    Ok(Some(reorder_lon(values, grid)))
}

fn with_hdf5_path<T>(
    path: &Path,
    f: impl FnOnce(&Hdf5File) -> Result<T, IoError>,
) -> Result<T, IoError> {
    let path = path.to_path_buf();
    let hdf5 = HDF5_PATH_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(file) = cache.get(&path) {
            return Ok::<Rc<Hdf5File>, IoError>(Rc::clone(file));
        }
        let file = Rc::new(open_hdf5_mmap(&path)?);
        cache.insert(path.clone(), Rc::clone(&file));
        Ok(file)
    })?;
    f(hdf5.as_ref())
}

fn open_hdf5_mmap(path: &Path) -> Result<Hdf5File, IoError> {
    let file = std::fs::File::open(path).map_err(|err| {
        IoError::Earth2Archive(format!(
            "failed to open {} for HDF5 mmap: {err}",
            path.display()
        ))
    })?;
    // SAFETY: the map is read-only, lives inside the HDF5 reader, and rustwx
    // does not mutate local Earth2 archive files while rendering from them.
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file) }.map_err(|err| {
        IoError::Earth2Archive(format!("failed to mmap {}: {err}", path.display()))
    })?;
    Hdf5File::from_mmap_with_options(mmap, hdf5_reader::OpenOptions::default()).map_err(|err| {
        IoError::Earth2Archive(format!(
            "failed to open Earth2 HDF5 path {}: {err}",
            path.display()
        ))
    })
}

fn read_hdf5_values_f64(dataset: &hdf5_reader::Dataset) -> Result<Vec<f64>, IoError> {
    match dataset.dtype() {
        Datatype::FloatingPoint { size: 4, .. } => Ok(dataset
            .read_array::<f32>()
            .map_err(hdf5_read_error)?
            .iter()
            .map(|&value| f64::from(value))
            .collect()),
        Datatype::FloatingPoint { size: 8, .. } => Ok(dataset
            .read_array::<f64>()
            .map_err(hdf5_read_error)?
            .iter()
            .copied()
            .collect()),
        dtype => Err(IoError::Earth2Archive(format!(
            "unsupported Earth2 HDF5 dtype for {}: {dtype:?}",
            dataset.name()
        ))),
    }
}

fn read_hdf5_values_f64_slice(
    dataset: &hdf5_reader::Dataset,
    selection: &SliceInfo,
) -> Result<Vec<f64>, IoError> {
    match dataset.dtype() {
        Datatype::FloatingPoint { size: 4, .. } => Ok(dataset
            .read_slice::<f32>(selection)
            .map_err(hdf5_read_error)?
            .iter()
            .map(|&value| f64::from(value))
            .collect()),
        Datatype::FloatingPoint { size: 8, .. } => Ok(dataset
            .read_slice::<f64>(selection)
            .map_err(hdf5_read_error)?
            .iter()
            .copied()
            .collect()),
        dtype => Err(IoError::Earth2Archive(format!(
            "unsupported Earth2 HDF5 dtype for {}: {dtype:?}",
            dataset.name()
        ))),
    }
}

fn hdf5_read_error(err: hdf5_reader::error::Error) -> IoError {
    IoError::Earth2Archive(err.to_string())
}

fn read_member_shaped_values(
    name: &str,
    values: Vec<f64>,
    member_count: usize,
    grid_len: usize,
    grid: &Earth2Grid,
    selector: Earth2EnsembleSelector,
) -> Result<Vec<f64>, IoError> {
    if member_count == 0 {
        return Err(IoError::Earth2Archive(format!(
            "variable {name} has an empty member dimension"
        )));
    }
    let expected = member_count * grid_len;
    if values.len() != expected {
        return Err(IoError::Earth2Archive(format!(
            "variable {name} had {} values, expected {expected} for {member_count} members",
            values.len()
        )));
    }
    match selector {
        Earth2EnsembleSelector::Deterministic => member_values(name, &values, 0, grid_len, grid),
        Earth2EnsembleSelector::Member(member) => {
            let index = usize::from(member);
            if index >= member_count {
                return Err(IoError::Earth2Archive(format!(
                    "member {member} requested for variable {name}, but file has {member_count} members"
                )));
            }
            member_values(name, &values, index, grid_len, grid)
        }
        Earth2EnsembleSelector::Statistic(stat) => Ok(reorder_lon(
            compute_member_stat(&values, member_count, grid_len, stat),
            grid,
        )),
    }
}

fn member_values(
    name: &str,
    values: &[f64],
    member_index: usize,
    grid_len: usize,
    grid: &Earth2Grid,
) -> Result<Vec<f64>, IoError> {
    let start = member_index * grid_len;
    let end = start + grid_len;
    values
        .get(start..end)
        .map(|slice| reorder_lon(slice.to_vec(), grid))
        .ok_or_else(|| {
            IoError::Earth2Archive(format!(
                "member {member_index} slice for variable {name} is out of bounds"
            ))
        })
}

fn compute_member_stat(
    values: &[f64],
    member_count: usize,
    grid_len: usize,
    stat: Earth2EnsembleStat,
) -> Vec<f64> {
    let mut out = Vec::with_capacity(grid_len);
    for cell in 0..grid_len {
        let mut members = Vec::with_capacity(member_count);
        for member in 0..member_count {
            members.push(values[member * grid_len + cell]);
        }
        out.push(stat_for_members(&mut members, stat));
    }
    out
}

fn stat_for_members(members: &mut [f64], stat: Earth2EnsembleStat) -> f64 {
    match stat {
        Earth2EnsembleStat::Mean => {
            members.iter().copied().sum::<f64>() / (members.len().max(1) as f64)
        }
        Earth2EnsembleStat::Std => {
            let mean = members.iter().copied().sum::<f64>() / (members.len().max(1) as f64);
            let variance = members
                .iter()
                .map(|value| {
                    let delta = value - mean;
                    delta * delta
                })
                .sum::<f64>()
                / (members.len().max(1) as f64);
            variance.sqrt()
        }
        Earth2EnsembleStat::Min => members
            .iter()
            .copied()
            .fold(f64::INFINITY, |left, right| left.min(right)),
        Earth2EnsembleStat::Max => members
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, |left, right| left.max(right)),
        Earth2EnsembleStat::P10 => percentile_nearest_rank(members, 0.10),
        Earth2EnsembleStat::P50 => percentile_nearest_rank(members, 0.50),
        Earth2EnsembleStat::P90 => percentile_nearest_rank(members, 0.90),
    }
}

fn percentile_nearest_rank(values: &mut [f64], quantile: f64) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    let last = values.len().saturating_sub(1);
    let index = ((last as f64) * quantile).round().clamp(0.0, last as f64) as usize;
    values[index]
}

fn reorder_lon(values: Vec<f64>, grid: &Earth2Grid) -> Vec<f64> {
    let mut out = Vec::with_capacity(values.len());
    for y in 0..grid.ny {
        let row_start = y * grid.nx;
        for &x in &grid.lon_order {
            out.push(values[row_start + x]);
        }
    }
    out
}

fn has_var(file: &Earth2DataFile<'_>, name: &str) -> bool {
    match file {
        Earth2DataFile::Netcdf(file) => file.variable(name).is_some(),
        Earth2DataFile::Hdf5(file) => file.dataset(name).is_ok(),
    }
}

fn normalize_lon(lon: f64) -> f64 {
    let mut normalized = lon;
    while normalized > 180.0 {
        normalized -= 360.0;
    }
    while normalized <= -180.0 {
        normalized += 360.0;
    }
    normalized
}

fn specific_humidity_from_dewpoint(pressure_pa: f64, dewpoint_k: f64) -> f64 {
    let e = vapor_pressure_from_dewpoint(dewpoint_k).min(pressure_pa * 0.99);
    (EPSILON * e / (pressure_pa - (1.0 - EPSILON) * e)).clamp(0.0, 0.1)
}

fn dewpoint_from_specific_humidity(pressure_pa: f64, q: f64) -> f64 {
    let q = q.clamp(0.0, 0.1);
    let e = (q * pressure_pa) / (EPSILON + (1.0 - EPSILON) * q);
    dewpoint_from_vapor_pressure(e)
}

fn relative_humidity_from_t_td(temperature_k: f64, dewpoint_k: f64) -> f64 {
    let e = vapor_pressure_from_dewpoint(dewpoint_k);
    let es = vapor_pressure_from_dewpoint(temperature_k).max(1.0);
    (100.0 * e / es).clamp(0.0, 100.0)
}

fn vapor_pressure_from_dewpoint(dewpoint_k: f64) -> f64 {
    let dewpoint_c = dewpoint_k - 273.15;
    611.2 * ((17.67 * dewpoint_c) / (dewpoint_c + 243.5)).exp()
}

fn dewpoint_from_vapor_pressure(vapor_pressure_pa: f64) -> f64 {
    let e_hpa = (vapor_pressure_pa / 100.0).max(0.001);
    let ln = (e_hpa / 6.112).ln();
    273.15 + (243.5 * ln) / (17.67 - ln)
}

fn contains_ascii(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}

#[cfg(test)]
mod tests;
