use crate::{FetchRequest, FetchResult, IoError};
use rustwx_core::FieldSelector;
use rustwx_core::SelectedField2D;
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
    let Ok(field) = bincode::deserialize::<SelectedField2D>(&bytes) else {
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
    let bytes = bincode::serialize(field).map_err(|err| IoError::Cache(err.to_string()))?;
    fs::write(&path, bytes).map_err(cache_error)?;
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
        let loaded_field = load_cached_selected_field(&cache_root, &fetch, field.selector)
            .unwrap()
            .unwrap();
        assert!(loaded_field.cache_hit);
        assert_eq!(loaded_field.field, field);

        fs::remove_dir_all(cache_root).ok();
    }
}
