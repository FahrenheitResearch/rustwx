use crate::{FetchRequest, FetchResult, IoError};
use rustwx_core::{FieldSelector, GridShape, LatLonGrid, SelectedField2D};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

const FETCH_METADATA_SCHEMA_VERSION: u32 = 1;
const GRID_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const FIELD_PAYLOAD_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedFetchMetadata {
    pub request: rustwx_core::ModelRunRequest,
    pub source_override: Option<rustwx_core::SourceId>,
    pub variable_patterns: Vec<String>,
    pub resolved_source: rustwx_core::SourceId,
    pub resolved_url: String,
    pub bytes_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedFetchResult {
    pub result: FetchResult,
    pub cache_hit: bool,
    pub bytes_path: PathBuf,
    pub metadata_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CachedFieldResult {
    pub field: SelectedField2D,
    pub cache_hit: bool,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CachedGridPayload {
    shape: GridShape,
    lat_deg: Vec<f32>,
    lon_deg: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CachedFieldPayload {
    selector: FieldSelector,
    units: String,
    values: Vec<f32>,
    grid_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct VersionedJsonPayload<T> {
    schema_version: u32,
    payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct VersionedBinaryPayload<T> {
    schema_version: u32,
    payload: T,
}

pub fn artifact_cache_dir(cache_root: &Path, fetch: &FetchRequest) -> PathBuf {
    let product = sanitize_component(&fetch.request.product);
    let source = sanitize_component(
        fetch
            .source_override
            .map(|source| source.as_str())
            .unwrap_or("auto"),
    );
    let variable_slug = variable_patterns_slug(&fetch.variable_patterns);
    cache_root
        .join(sanitize_component(fetch.request.model.as_str()))
        .join(&fetch.request.cycle.date_yyyymmdd)
        .join(format!("{:02}z", fetch.request.cycle.hour_utc))
        .join(format!("f{:03}", fetch.request.forecast_hour))
        .join(product)
        .join(source)
        .join(variable_slug)
}

pub fn fetch_cache_paths(cache_root: &Path, fetch: &FetchRequest) -> (PathBuf, PathBuf) {
    let root = artifact_cache_dir(cache_root, fetch);
    (root.join("fetch.grib2"), root.join("fetch_meta.json"))
}

pub fn field_cache_path(
    cache_root: &Path,
    fetch: &FetchRequest,
    selector: FieldSelector,
) -> PathBuf {
    artifact_cache_dir(cache_root, fetch)
        .join("fields")
        .join(format!("{}.bin", sanitize_component(&selector.key())))
}

fn grid_cache_path(cache_root: &Path, fetch: &FetchRequest, grid_key: &str) -> PathBuf {
    artifact_cache_dir(cache_root, fetch)
        .join("fields")
        .join("grids")
        .join(format!("{grid_key}.bin"))
}

pub fn load_cached_fetch(
    cache_root: &Path,
    fetch: &FetchRequest,
) -> Result<Option<CachedFetchResult>, IoError> {
    let (bytes_path, metadata_path) = fetch_cache_paths(cache_root, fetch);
    if !bytes_path.exists() || !metadata_path.exists() {
        if bytes_path.exists() || metadata_path.exists() {
            quarantine_cache_paths(&[&bytes_path, &metadata_path], "incomplete_fetch_cache");
        }
        return Ok(None);
    }
    let bytes = match fs::read(&bytes_path) {
        Ok(bytes) => bytes,
        Err(_) => {
            quarantine_cache_paths(&[&bytes_path, &metadata_path], "fetch_bytes_read_error");
            return Ok(None);
        }
    };
    let metadata_bytes = match fs::read(&metadata_path) {
        Ok(bytes) => bytes,
        Err(_) => {
            quarantine_cache_paths(&[&bytes_path, &metadata_path], "fetch_metadata_read_error");
            return Ok(None);
        }
    };
    let Some(metadata) = load_cached_fetch_metadata(&metadata_bytes) else {
        quarantine_cache_paths(
            &[&bytes_path, &metadata_path],
            "fetch_metadata_decode_error",
        );
        return Ok(None);
    };
    if metadata.bytes_len != bytes.len()
        || metadata.request != fetch.request
        || metadata.source_override != fetch.source_override
        || metadata.variable_patterns != fetch.variable_patterns
    {
        quarantine_cache_paths(&[&bytes_path, &metadata_path], "fetch_metadata_mismatch");
        return Ok(None);
    }
    Ok(Some(CachedFetchResult {
        result: FetchResult {
            source: metadata.resolved_source,
            url: metadata.resolved_url,
            bytes,
        },
        cache_hit: true,
        bytes_path,
        metadata_path,
    }))
}

pub fn store_cached_fetch(
    cache_root: &Path,
    fetch: &FetchRequest,
    result: &FetchResult,
) -> Result<CachedFetchResult, IoError> {
    let (bytes_path, metadata_path) = fetch_cache_paths(cache_root, fetch);
    if let Some(parent) = bytes_path.parent() {
        fs::create_dir_all(parent).map_err(cache_error)?;
    }
    atomic_write_bytes(&bytes_path, &result.bytes)?;
    let metadata = CachedFetchMetadata {
        request: fetch.request.clone(),
        source_override: fetch.source_override,
        variable_patterns: fetch.variable_patterns.clone(),
        resolved_source: result.source,
        resolved_url: result.url.clone(),
        bytes_len: result.bytes.len(),
    };
    let metadata_bytes = serde_json::to_vec_pretty(&VersionedJsonPayload {
        schema_version: FETCH_METADATA_SCHEMA_VERSION,
        payload: metadata,
    })
    .map_err(|err| IoError::Cache(err.to_string()))?;
    atomic_write_bytes(&metadata_path, &metadata_bytes)?;

    Ok(CachedFetchResult {
        result: result.clone(),
        cache_hit: false,
        bytes_path,
        metadata_path,
    })
}

pub fn load_cached_selected_field(
    cache_root: &Path,
    fetch: &FetchRequest,
    selector: FieldSelector,
) -> Result<Option<CachedFieldResult>, IoError> {
    let path = field_cache_path(cache_root, fetch, selector);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(_) => {
            quarantine_cache_paths(&[&path], "selected_field_read_error");
            return Ok(None);
        }
    };
    let Some(field) =
        load_cached_selected_field_payload(cache_root, fetch, selector, &path, &bytes)?
    else {
        return Ok(None);
    };
    Ok(Some(CachedFieldResult {
        field,
        cache_hit: true,
        path,
    }))
}

pub fn store_cached_selected_field(
    cache_root: &Path,
    fetch: &FetchRequest,
    field: &SelectedField2D,
) -> Result<CachedFieldResult, IoError> {
    let path = field_cache_path(cache_root, fetch, field.selector);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(cache_error)?;
    }
    let grid_key = grid_cache_key(&field.grid);
    let grid_path = grid_cache_path(cache_root, fetch, &grid_key);
    if !grid_path.exists() {
        if let Some(parent) = grid_path.parent() {
            fs::create_dir_all(parent).map_err(cache_error)?;
        }
        let grid_payload = CachedGridPayload {
            shape: field.grid.shape,
            lat_deg: field.grid.lat_deg.clone(),
            lon_deg: field.grid.lon_deg.clone(),
        };
        let grid_bytes = serialize_binary_payload(GRID_PAYLOAD_SCHEMA_VERSION, &grid_payload)?;
        atomic_write_bytes(&grid_path, &grid_bytes)?;
    }

    let field_payload = CachedFieldPayload {
        selector: field.selector,
        units: field.units.clone(),
        values: field.values.clone(),
        grid_key,
    };
    let field_bytes = serialize_binary_payload(FIELD_PAYLOAD_SCHEMA_VERSION, &field_payload)?;
    atomic_write_bytes(&path, &field_bytes)?;
    Ok(CachedFieldResult {
        field: field.clone(),
        cache_hit: false,
        path,
    })
}

fn variable_patterns_slug(patterns: &[String]) -> String {
    if patterns.is_empty() {
        return "full".to_string();
    }
    let joined = patterns.join("__");
    let sanitized = sanitize_component(&joined);
    if sanitized.len() <= 120 {
        sanitized
    } else {
        format!("{}__{}vars", &sanitized[..120], patterns.len())
    }
}

fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_was_sep = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if !last_was_sep {
            out.push('_');
            last_was_sep = true;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

fn load_cached_selected_field_payload(
    cache_root: &Path,
    fetch: &FetchRequest,
    expected_selector: FieldSelector,
    field_path: &Path,
    bytes: &[u8],
) -> Result<Option<SelectedField2D>, IoError> {
    if let Some(payload) =
        load_binary_payload::<CachedFieldPayload>(bytes, FIELD_PAYLOAD_SCHEMA_VERSION)
    {
        if payload.selector != expected_selector {
            quarantine_cache_paths(&[field_path], "selected_field_selector_mismatch");
            return Ok(None);
        }
        let grid_path = grid_cache_path(cache_root, fetch, &payload.grid_key);
        let grid_bytes = match fs::read(&grid_path) {
            Ok(bytes) => bytes,
            Err(_) => {
                quarantine_cache_paths(&[field_path, &grid_path], "selected_field_grid_read_error");
                return Ok(None);
            }
        };
        let Some(grid_payload) =
            load_binary_payload::<CachedGridPayload>(&grid_bytes, GRID_PAYLOAD_SCHEMA_VERSION)
        else {
            quarantine_cache_paths(
                &[field_path, &grid_path],
                "selected_field_grid_decode_error",
            );
            return Ok(None);
        };
        let grid = match LatLonGrid::new(
            grid_payload.shape,
            grid_payload.lat_deg,
            grid_payload.lon_deg,
        ) {
            Ok(grid) => grid,
            Err(_) => {
                quarantine_cache_paths(&[field_path, &grid_path], "selected_field_grid_invalid");
                return Ok(None);
            }
        };
        let field =
            match SelectedField2D::new(payload.selector, payload.units, grid, payload.values) {
                Ok(field) => field,
                Err(_) => {
                    quarantine_cache_paths(&[field_path, &grid_path], "selected_field_invalid");
                    return Ok(None);
                }
            };
        if !selected_field_grid_is_canonical(&field) {
            quarantine_cache_paths(
                &[field_path, &grid_path],
                "selected_field_grid_noncanonical",
            );
            return Ok(None);
        }
        return Ok(Some(field));
    }

    if let Ok(field) = bincode::deserialize::<SelectedField2D>(bytes) {
        if field.selector != expected_selector {
            quarantine_cache_paths(&[field_path], "legacy_selected_field_selector_mismatch");
            return Ok(None);
        }
        if !selected_field_grid_is_canonical(&field) {
            quarantine_cache_paths(&[field_path], "legacy_selected_field_grid_noncanonical");
            return Ok(None);
        }
        return Ok(Some(field));
    }

    quarantine_cache_paths(&[field_path], "selected_field_decode_error");
    Ok(None)
}

fn load_cached_fetch_metadata(bytes: &[u8]) -> Option<CachedFetchMetadata> {
    if let Ok(wrapper) = serde_json::from_slice::<VersionedJsonPayload<CachedFetchMetadata>>(bytes)
    {
        if wrapper.schema_version == FETCH_METADATA_SCHEMA_VERSION {
            return Some(wrapper.payload);
        }
        return None;
    }
    serde_json::from_slice::<CachedFetchMetadata>(bytes).ok()
}

fn selected_field_grid_is_canonical(field: &SelectedField2D) -> bool {
    let nx = field.grid.shape.nx;
    let ny = field.grid.shape.ny;
    if nx == 0 || ny == 0 {
        return false;
    }
    if field.grid.lat_deg.len() != nx * ny || field.grid.lon_deg.len() != nx * ny {
        return false;
    }

    for row in 0..ny {
        let start = row * nx;
        let end = start + nx;
        let lat_row = &field.grid.lat_deg[start..end];
        let lon_row = &field.grid.lon_deg[start..end];

        if lat_row.iter().any(|value| !value.is_finite()) {
            return false;
        }
        if lon_row
            .iter()
            .any(|value| !value.is_finite() || *value < -180.0 || *value > 180.0)
        {
            return false;
        }
        if lon_row.windows(2).any(|pair| pair[1] < pair[0]) {
            return false;
        }
    }
    true
}

fn serialize_binary_payload<T: Serialize>(
    schema_version: u32,
    payload: &T,
) -> Result<Vec<u8>, IoError> {
    bincode::serialize(&VersionedBinaryPayload {
        schema_version,
        payload,
    })
    .map_err(|err| IoError::Cache(err.to_string()))
}

fn load_binary_payload<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    expected_schema_version: u32,
) -> Option<T> {
    if let Ok(wrapper) = bincode::deserialize::<VersionedBinaryPayload<T>>(bytes) {
        if wrapper.schema_version == expected_schema_version {
            return Some(wrapper.payload);
        }
    }
    None
}

fn grid_cache_key(grid: &LatLonGrid) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    hash = fnv1a_mix(hash, grid.shape.nx as u64);
    hash = fnv1a_mix(hash, grid.shape.ny as u64);
    for value in &grid.lat_deg {
        hash = fnv1a_mix(hash, value.to_bits() as u64);
    }
    for value in &grid.lon_deg {
        hash = fnv1a_mix(hash, value.to_bits() as u64);
    }
    format!("{hash:016x}")
}

fn fnv1a_mix(hash: u64, value: u64) -> u64 {
    let mut out = hash;
    for byte in value.to_le_bytes() {
        out ^= u64::from(byte);
        out = out.wrapping_mul(0x100000001b3);
    }
    out
}

fn cache_error(err: std::io::Error) -> IoError {
    IoError::Cache(err.to_string())
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), IoError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(cache_error)?;
    }
    let tmp_path = temp_path_for(path);
    let write_result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(cache_error)?;
        file.write_all(bytes).map_err(cache_error)?;
        file.sync_all().map_err(cache_error)?;
        Ok::<(), IoError>(())
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    fs::rename(&tmp_path, path).map_err(|err| {
        let _ = fs::remove_file(&tmp_path);
        cache_error(err)
    })
}

fn quarantine_cache_paths(paths: &[&Path], reason: &str) {
    for path in paths {
        quarantine_cache_path(path, reason);
    }
}

fn quarantine_cache_path(path: &Path, reason: &str) {
    if !path.exists() {
        return;
    }
    let quarantine_path = quarantine_path_for(path, reason);
    if let Some(parent) = quarantine_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if fs::rename(path, &quarantine_path).is_err() {
        let _ = fs::remove_file(path);
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cache");
    path.with_file_name(format!(
        ".{file_name}.tmp-{}-{}",
        process::id(),
        unique_suffix()
    ))
}

fn quarantine_path_for(path: &Path, reason: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cache");
    path.with_file_name(format!(
        "{file_name}.corrupt-{reason}-{}-{}",
        process::id(),
        unique_suffix()
    ))
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{
        CanonicalField, CycleSpec, FieldSelector, GridShape, LatLonGrid, ModelId, ModelRunRequest,
        SourceId, VerticalSelector,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_fetch_request() -> FetchRequest {
        FetchRequest {
            request: ModelRunRequest::new(
                ModelId::Hrrr,
                CycleSpec::new("20260414", 23).unwrap(),
                0,
                "prs",
            )
            .unwrap(),
            source_override: Some(SourceId::Aws),
            variable_patterns: vec!["TMP:500 mb".to_string(), "UGRD:500 mb".to_string()],
        }
    }

    fn sample_field() -> SelectedField2D {
        let grid = LatLonGrid::new(
            GridShape::new(2, 2).unwrap(),
            vec![40.0, 40.0, 39.0, 39.0],
            vec![-100.0, -99.0, -100.0, -99.0],
        )
        .unwrap();
        SelectedField2D::new(
            FieldSelector::new(
                CanonicalField::Temperature,
                VerticalSelector::IsobaricHpa(500),
            ),
            "K",
            grid,
            vec![255.0, 256.0, 257.0, 258.0],
        )
        .unwrap()
    }

    fn temp_cache_root() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rustwx_io_cache_test_{unique}"))
    }

    #[test]
    fn cached_fetch_and_field_round_trip() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let result = FetchResult {
            source: SourceId::Aws,
            url: "https://example.test/sample.grib2".to_string(),
            bytes: vec![1, 2, 3, 4, 5],
        };
        let stored = store_cached_fetch(&cache_root, &fetch, &result).unwrap();
        assert!(!stored.cache_hit);
        let loaded = load_cached_fetch(&cache_root, &fetch).unwrap().unwrap();
        assert!(loaded.cache_hit);
        assert_eq!(loaded.result, result);

        let field = sample_field();
        let stored_field = store_cached_selected_field(&cache_root, &fetch, &field).unwrap();
        assert!(!stored_field.cache_hit);
        let grid_path = grid_cache_path(&cache_root, &fetch, &grid_cache_key(&field.grid));
        assert!(grid_path.exists());
        let loaded_field = load_cached_selected_field(&cache_root, &fetch, field.selector)
            .unwrap()
            .unwrap();
        assert!(loaded_field.cache_hit);
        assert_eq!(loaded_field.field, field);

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn field_cache_reuses_shared_grid_payload() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let first = sample_field();
        let second = SelectedField2D::new(
            FieldSelector::new(CanonicalField::UWind, VerticalSelector::IsobaricHpa(500)),
            "m/s",
            first.grid.clone(),
            vec![10.0, 11.0, 12.0, 13.0],
        )
        .unwrap();

        store_cached_selected_field(&cache_root, &fetch, &first).unwrap();
        store_cached_selected_field(&cache_root, &fetch, &second).unwrap();

        let grids_dir = artifact_cache_dir(&cache_root, &fetch)
            .join("fields")
            .join("grids");
        let grid_files = fs::read_dir(&grids_dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(grid_files.len(), 1);

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn load_cached_selected_field_reads_legacy_embedded_field_payload() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let field = sample_field();
        let path = field_cache_path(&cache_root, &fetch, field.selector);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let legacy_bytes = bincode::serialize(&field).unwrap();
        fs::write(&path, legacy_bytes).unwrap();

        let loaded = load_cached_selected_field(&cache_root, &fetch, field.selector)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.field, field);

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn corrupt_fetch_metadata_is_quarantined_and_treated_as_cache_miss() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let (bytes_path, metadata_path) = fetch_cache_paths(&cache_root, &fetch);
        fs::create_dir_all(bytes_path.parent().unwrap()).unwrap();
        fs::write(&bytes_path, [1_u8, 2, 3, 4]).unwrap();
        fs::write(&metadata_path, b"{not-json").unwrap();

        let loaded = load_cached_fetch(&cache_root, &fetch).unwrap();
        assert!(loaded.is_none());
        assert!(!bytes_path.exists());
        assert!(!metadata_path.exists());
        let quarantined = fs::read_dir(bytes_path.parent().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            quarantined
                .iter()
                .any(|entry| entry.file_name().to_string_lossy().contains("corrupt"))
        );

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn store_cached_fetch_writes_versioned_metadata() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let result = FetchResult {
            source: SourceId::Aws,
            url: "https://example.test/sample.grib2".to_string(),
            bytes: vec![9, 8, 7, 6],
        };

        store_cached_fetch(&cache_root, &fetch, &result).unwrap();
        let (_, metadata_path) = fetch_cache_paths(&cache_root, &fetch);
        let wrapper: VersionedJsonPayload<CachedFetchMetadata> =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        assert_eq!(wrapper.schema_version, FETCH_METADATA_SCHEMA_VERSION);
        assert_eq!(wrapper.payload.resolved_source, SourceId::Aws);

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn corrupt_field_cache_is_quarantined_and_treated_as_cache_miss() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let field = sample_field();
        let path = field_cache_path(&cache_root, &fetch, field.selector);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"definitely-not-bincode").unwrap();

        let loaded = load_cached_selected_field(&cache_root, &fetch, field.selector).unwrap();
        assert!(loaded.is_none());
        assert!(!path.exists());
        let quarantined = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            quarantined
                .iter()
                .any(|entry| entry.file_name().to_string_lossy().contains("corrupt"))
        );

        fs::remove_dir_all(cache_root).ok();
    }

    #[test]
    fn legacy_noncanonical_field_cache_is_quarantined_and_treated_as_cache_miss() {
        let cache_root = temp_cache_root();
        let fetch = sample_fetch_request();
        let mut field = sample_field();
        field.grid.lon_deg = vec![260.0, 261.0, 260.0, 261.0];
        let path = field_cache_path(&cache_root, &fetch, field.selector);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bincode::serialize(&field).unwrap()).unwrap();

        let loaded = load_cached_selected_field(&cache_root, &fetch, field.selector).unwrap();
        assert!(loaded.is_none());
        assert!(!path.exists());
        let quarantined = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            quarantined
                .iter()
                .any(|entry| entry.file_name().to_string_lossy().contains("corrupt"))
        );

        fs::remove_dir_all(cache_root).ok();
    }
}
