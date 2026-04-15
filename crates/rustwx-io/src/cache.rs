use crate::{FetchRequest, FetchResult, IoError};
use rustwx_core::{FieldSelector, GridShape, LatLonGrid, SelectedField2D};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

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
        return Ok(None);
    }
    let Ok(bytes) = fs::read(&bytes_path) else {
        return Ok(None);
    };
    let Ok(metadata_bytes) = fs::read(&metadata_path) else {
        return Ok(None);
    };
    let Ok(metadata) = serde_json::from_slice::<CachedFetchMetadata>(&metadata_bytes) else {
        return Ok(None);
    };
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
    fs::write(&bytes_path, &result.bytes).map_err(cache_error)?;
    let metadata = CachedFetchMetadata {
        request: fetch.request.clone(),
        source_override: fetch.source_override,
        variable_patterns: fetch.variable_patterns.clone(),
        resolved_source: result.source,
        resolved_url: result.url.clone(),
        bytes_len: result.bytes.len(),
    };
    fs::write(
        &metadata_path,
        serde_json::to_vec_pretty(&metadata).map_err(|err| IoError::Cache(err.to_string()))?,
    )
    .map_err(cache_error)?;

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
    let Ok(bytes) = fs::read(&path) else {
        return Ok(None);
    };
    let Some(field) = load_cached_selected_field_payload(cache_root, fetch, &bytes)? else {
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
        let grid_bytes =
            bincode::serialize(&grid_payload).map_err(|err| IoError::Cache(err.to_string()))?;
        fs::write(&grid_path, grid_bytes).map_err(cache_error)?;
    }

    let field_payload = CachedFieldPayload {
        selector: field.selector,
        units: field.units.clone(),
        values: field.values.clone(),
        grid_key,
    };
    let field_bytes =
        bincode::serialize(&field_payload).map_err(|err| IoError::Cache(err.to_string()))?;
    fs::write(&path, field_bytes).map_err(cache_error)?;
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
    bytes: &[u8],
) -> Result<Option<SelectedField2D>, IoError> {
    if let Ok(payload) = bincode::deserialize::<CachedFieldPayload>(bytes) {
        let grid_path = grid_cache_path(cache_root, fetch, &payload.grid_key);
        if let Ok(grid_bytes) = fs::read(&grid_path) {
            if let Ok(grid_payload) = bincode::deserialize::<CachedGridPayload>(&grid_bytes) {
                let grid = LatLonGrid::new(
                    grid_payload.shape,
                    grid_payload.lat_deg,
                    grid_payload.lon_deg,
                )?;
                let field =
                    SelectedField2D::new(payload.selector, payload.units, grid, payload.values)?;
                return Ok(Some(field));
            }
        }
    }

    if let Ok(field) = bincode::deserialize::<SelectedField2D>(bytes) {
        return Ok(Some(field));
    }

    Ok(None)
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
}
