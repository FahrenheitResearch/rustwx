use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};

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
    Ok(Some(bincode::deserialize(&bytes)?))
}

pub fn store_bincode<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bincode::serialize(value)?)?;
    Ok(())
}

pub fn ensure_dir(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(path)?;
    Ok(())
}
