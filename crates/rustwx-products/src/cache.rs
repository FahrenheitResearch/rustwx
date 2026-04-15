use crate::publication::atomic_write_bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

const BINCODE_CACHE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
struct VersionedCachePayload<T> {
    schema_version: u32,
    payload: T,
}

pub fn default_proof_cache_dir(out_dir: &Path) -> PathBuf {
    out_dir.join("cache")
}

pub fn load_bincode<T: DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    if let Ok(wrapper) = bincode::deserialize::<VersionedCachePayload<T>>(&bytes) {
        if wrapper.schema_version == BINCODE_CACHE_SCHEMA_VERSION {
            return Ok(Some(wrapper.payload));
        }
        quarantine_cache_file(path, "schema_mismatch");
        return Ok(None);
    }
    if let Ok(value) = bincode::deserialize::<T>(&bytes) {
        return Ok(Some(value));
    }
    quarantine_cache_file(path, "decode_error");
    Ok(None)
}

pub fn store_bincode<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = bincode::serialize(&VersionedCachePayload {
        schema_version: BINCODE_CACHE_SCHEMA_VERSION,
        payload: value,
    })?;
    atomic_write_bytes(path, &bytes)?;
    Ok(())
}

pub fn ensure_dir(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
    struct Fixture {
        name: String,
        value: u16,
    }

    #[test]
    fn bincode_round_trip_works() {
        let root =
            std::env::temp_dir().join(format!("rustwx_products_cache_{}", std::process::id()));
        let path = root.join("fixture.bin");
        let fixture = Fixture {
            name: "demo".into(),
            value: 7,
        };

        store_bincode(&path, &fixture).unwrap();
        let loaded = load_bincode::<Fixture>(&path).unwrap().unwrap();
        assert_eq!(loaded, fixture);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_bincode_payload_still_loads() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_products_cache_legacy_{}",
            std::process::id()
        ));
        let path = root.join("fixture.bin");
        let fixture = Fixture {
            name: "legacy".into(),
            value: 9,
        };

        fs::create_dir_all(&root).unwrap();
        fs::write(&path, bincode::serialize(&fixture).unwrap()).unwrap();
        let loaded = load_bincode::<Fixture>(&path).unwrap().unwrap();
        assert_eq!(loaded, fixture);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_bincode_payload_is_quarantined_and_treated_as_cache_miss() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_products_cache_corrupt_{}",
            std::process::id()
        ));
        let path = root.join("fixture.bin");

        fs::create_dir_all(&root).unwrap();
        fs::write(&path, b"not-bincode").unwrap();
        let loaded = load_bincode::<Fixture>(&path).unwrap();
        assert!(loaded.is_none());
        assert!(!path.exists());
        let quarantined = fs::read_dir(&root)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            quarantined
                .iter()
                .any(|entry| entry.file_name().to_string_lossy().contains("corrupt"))
        );

        let _ = fs::remove_dir_all(root);
    }
}

fn quarantine_cache_file(path: &Path, reason: &str) {
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
