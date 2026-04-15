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
}
