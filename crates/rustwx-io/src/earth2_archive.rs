use crate::{FetchRequest, FetchResult, IoError, PartialExtraction};
use rustwx_core::{
    CanonicalField, FieldSelector, GridProjection, GridShape, LatLonGrid, ModelId, ModelRunRequest,
    SelectedField2D, SourceId, VerticalSelector,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const ARCHIVE_ENV: &str = "RUSTWX_EARTH2_ARCHIVE";
const GEOPOTENTIAL_M2S2_TO_M: f64 = 1.0 / 9.806_65;
const EPSILON: f64 = 0.622;
const EARTH2_LEVELS_HPA: &[u16] = &[
    1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50,
];

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

#[derive(Debug, Clone, Copy)]
enum Transform {
    Identity,
    FractionToPercent,
    GeopotentialToHeight,
    KelvinToCelsius,
}

impl Transform {
    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Identity => value,
            Self::FractionToPercent => value * 100.0,
            Self::GeopotentialToHeight => value * GEOPOTENTIAL_M2S2_TO_M,
            Self::KelvinToCelsius => value - 273.15,
        }
    }
}

pub fn is_earth2_archive_fetch(fetch: &FetchRequest) -> bool {
    fetch.request.model == ModelId::Aifs || fetch.source_override == Some(SourceId::Earth2Archive)
}

pub fn archive_root() -> Result<PathBuf, IoError> {
    std::env::var_os(ARCHIVE_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| IoError::Earth2Archive(format!("{ARCHIVE_ENV} is not set")))
}

pub fn archive_path_for_request(request: &ModelRunRequest) -> Result<PathBuf, IoError> {
    archive_path_with_root(archive_root()?, request)
}

pub fn archive_path_with_root(
    root: impl AsRef<Path>,
    request: &ModelRunRequest,
) -> Result<PathBuf, IoError> {
    let init = format!(
        "{}T{:02}Z",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc
    );
    Ok(root
        .as_ref()
        .join(request.model.as_str())
        .join(init)
        .join(format!("lead{:03}.nc", request.forecast_hour)))
}

pub fn probe_archive(fetch: &FetchRequest) -> Result<crate::ProbeResult, IoError> {
    let path = archive_path_for_request(&fetch.request)?;
    Ok(crate::ProbeResult {
        source: SourceId::Earth2Archive,
        available: path.is_file(),
        grib_url: file_url(&path),
        idx_url: None,
    })
}

pub fn archive_fetch_available(fetch: &FetchRequest) -> bool {
    archive_path_for_request(&fetch.request)
        .map(|path| path.is_file())
        .unwrap_or(false)
}

pub fn fetch_archive_bytes(fetch: &FetchRequest) -> Result<FetchResult, IoError> {
    if let Some(source) = fetch.source_override {
        if source != SourceId::Earth2Archive {
            return Err(IoError::Earth2Archive(format!(
                "{} is local-archive only; requested source {source}",
                fetch.request.model
            )));
        }
    }
    let path = archive_path_for_request(&fetch.request)?;
    let bytes = std::fs::read(&path).map_err(|err| {
        IoError::Earth2Archive(format!("failed to read {}: {err}", path.display()))
    })?;
    Ok(FetchResult {
        source: SourceId::Earth2Archive,
        url: file_url(&path),
        bytes,
    })
}

pub fn looks_like_earth2_archive(bytes: &[u8]) -> bool {
    netcrust::looks_like_netcdf(bytes)
        && contains_ascii(bytes, b"lat")
        && contains_ascii(bytes, b"lon")
        && (contains_ascii(bytes, b"t2m") || contains_ascii(bytes, b"t1000"))
}

pub fn decode_surface_from_bytes(bytes: &[u8]) -> Result<Earth2SurfaceFields, IoError> {
    let file = open_file_from_bytes(bytes)?;
    let grid = Earth2Grid::from_file(&file)?;
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
    let grid = Earth2Grid::from_file(&file)?;
    decode_pressure_from_file(&file, &grid)
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
    let file = open_file(bytes, preferred_path)?;
    let grid = Earth2Grid::from_file(&file)?;
    let latlon = grid.latlon_grid()?;
    let mut extracted = Vec::new();
    let mut missing = Vec::new();

    for &selector in selectors {
        match read_selector(&file, &grid, selector, ensemble_selector)? {
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

fn open_file_from_bytes(bytes: &[u8]) -> Result<netcrust::File, IoError> {
    open_file(bytes, None)
}

impl Earth2Grid {
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
    file: &netcrust::File,
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
            read_optional_transformed(file, grid, "tp06", Transform::Identity, ensemble_selector)
                .map_units("kg/m^2")
        }
        (field, VerticalSelector::IsobaricHpa(level)) => {
            read_pressure_selector(file, grid, field, level, ensemble_selector)
        }
        _ => Ok(None),
    }
}

fn read_pressure_selector(
    file: &netcrust::File,
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
    file: &netcrust::File,
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
    file: &netcrust::File,
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

fn read_2d(file: &netcrust::File, grid: &Earth2Grid, name: &str) -> Result<Vec<f64>, IoError> {
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

fn read_2d_selected(
    file: &netcrust::File,
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

    let array = file
        .read_array_f64(name)
        .map_err(|err| IoError::Earth2Archive(format!("failed to read {name}: {err}")))?;
    let shape = array.shape().to_vec();
    let values = array.into_values();
    let expected = grid.nx * grid.ny;
    match shape.as_slice() {
        [ny, nx] if *ny == grid.ny && *nx == grid.nx => match selector {
            Earth2EnsembleSelector::Deterministic => Ok(reorder_lon(values, grid)),
            Earth2EnsembleSelector::Member(_) => Err(IoError::Earth2Archive(format!(
                "member selector requested for deterministic variable {name}"
            ))),
            Earth2EnsembleSelector::Statistic(stat) => Err(IoError::Earth2Archive(format!(
                "{} selector requested for deterministic variable {name}; no aggregate variable {}_{} was present",
                stat.label(),
                name,
                stat.aggregate_suffix()
            ))),
        },
        [len] if *len == expected => match selector {
            Earth2EnsembleSelector::Deterministic => Ok(reorder_lon(values, grid)),
            Earth2EnsembleSelector::Member(_) => Err(IoError::Earth2Archive(format!(
                "member selector requested for deterministic variable {name}"
            ))),
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

fn has_var(file: &netcrust::File, name: &str) -> bool {
    file.variable(name).is_some()
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
mod tests {
    use super::*;
    use rustwx_core::{CycleSpec, ModelRunRequest};

    #[test]
    fn archive_path_uses_canonical_layout() {
        let request = ModelRunRequest::new(
            ModelId::Aifs,
            CycleSpec::new("20160822", 0).unwrap(),
            24,
            "oper",
        )
        .unwrap();
        let path = archive_path_with_root("X:/archive", &request).unwrap();
        let as_text = path.to_string_lossy().replace('\\', "/");
        assert!(as_text.ends_with("X:/archive/aifs/20160822T00Z/lead024.nc"));
    }

    #[test]
    fn dewpoint_q_roundtrip_is_reasonable() {
        let pressure = 100_000.0;
        let dewpoint = 293.15;
        let q = specific_humidity_from_dewpoint(pressure, dewpoint);
        let decoded = dewpoint_from_specific_humidity(pressure, q);
        assert!((decoded - dewpoint).abs() < 0.01);
    }

    #[test]
    fn ensemble_member_stats_are_computed_cellwise() {
        let values = vec![
            1.0, 10.0, 100.0, //
            3.0, 14.0, 106.0,
        ];
        assert_eq!(
            compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Mean),
            vec![2.0, 12.0, 103.0]
        );
        assert_eq!(
            compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Std),
            vec![1.0, 2.0, 3.0]
        );
        assert_eq!(
            compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Min),
            vec![1.0, 10.0, 100.0]
        );
        assert_eq!(
            compute_member_stat(&values, 2, 3, Earth2EnsembleStat::Max),
            vec![3.0, 14.0, 106.0]
        );
    }

    #[test]
    fn percentile_stat_uses_nearest_rank_on_sorted_members() {
        let mut values = vec![30.0, 10.0, 20.0, 40.0, 50.0];
        assert_eq!(percentile_nearest_rank(&mut values, 0.10), 10.0);
        let mut values = vec![30.0, 10.0, 20.0, 40.0, 50.0];
        assert_eq!(percentile_nearest_rank(&mut values, 0.50), 30.0);
        let mut values = vec![30.0, 10.0, 20.0, 40.0, 50.0];
        assert_eq!(percentile_nearest_rank(&mut values, 0.90), 50.0);
    }
}
