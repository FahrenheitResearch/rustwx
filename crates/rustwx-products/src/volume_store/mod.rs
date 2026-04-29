//! Chunked primitive weather volume storage.
//!
//! This module is intentionally small and model-neutral. The first production
//! writer will target CA HRRR pressure-level fields, but the reader/index/codec
//! here do not know what HRRR is.

pub mod bundle;
mod chunk_payload;
pub mod codec;
pub mod grid;
pub mod index;
pub mod manifest;
pub mod pressure;
pub mod reader;
pub mod sampling;
pub mod terrain;
pub mod writer;

pub use bundle::{
    FORECAST_BUNDLE_FILE, FORECAST_BUNDLE_FORMAT, FORECAST_GROUP_FORMAT,
    FORECAST_GROUP_MANIFEST_FILE, ForecastAxis, ForecastAxisKind, ForecastAxisValue, ForecastBlob,
    ForecastBundle, ForecastBundleReader, ForecastGroupKind, ForecastGroupManifest,
    ForecastGroupRef, ForecastIndex, ForecastVariable,
};
pub use codec::{ChunkCodec, DecodedChunk, EncodedChunk};
pub use grid::GridSpec;
pub use index::{ChunkExtent, ChunkIndex, ChunkIndexRecord};
pub use manifest::{ChunkShape, VolumeManifest, VolumeVariable};
pub use pressure::{
    PressureTimestepProvider, PressureVolumeTimestep, pressure_volume_variables_for_fields,
    write_pressure_volume_from_provider, write_pressure_volume_from_timesteps,
};
pub use reader::VolumeStore;
pub use sampling::{
    PointProfile, PointSample, RouteDef, RouteSample, RouteSectionPrimitives, RouteValue,
};
pub use terrain::{
    SURFACE_TERRAIN_FORMAT, SURFACE_TERRAIN_MANIFEST_FILE, SURFACE_TERRAIN_PAYLOAD_FILE,
    SurfaceTerrainBuildStats, SurfaceTerrainManifest, SurfaceTerrainStore, SurfaceTerrainTimestep,
    write_surface_terrain_store,
};
pub use writer::{BuildStats, VolumeFieldProvider, write_volume_store};

pub type VolumeResult<T> = Result<T, VolumeStoreError>;

#[derive(Debug)]
pub enum VolumeStoreError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidManifest(String),
    InvalidIndex(String),
    InvalidChunk(String),
    MissingVariable(String),
    MissingHour(u8),
    MissingLevel(u16),
    OutOfBounds(String),
    Provider(String),
}

impl std::fmt::Display for VolumeStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Json(err) => write!(f, "JSON error: {err}"),
            Self::InvalidManifest(message) => write!(f, "invalid volume manifest: {message}"),
            Self::InvalidIndex(message) => write!(f, "invalid volume index: {message}"),
            Self::InvalidChunk(message) => write!(f, "invalid volume chunk: {message}"),
            Self::MissingVariable(name) => write!(f, "missing variable: {name}"),
            Self::MissingHour(hour) => write!(f, "missing forecast hour: f{hour:03}"),
            Self::MissingLevel(level) => write!(f, "missing pressure level: {level} hPa"),
            Self::OutOfBounds(message) => write!(f, "volume request out of bounds: {message}"),
            Self::Provider(message) => write!(f, "volume field provider failed: {message}"),
        }
    }
}

impl std::error::Error for VolumeStoreError {}

impl From<std::io::Error> for VolumeStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for VolumeStoreError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn temp_store_dir(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rustwx_volume_store_{name}_{}_{}",
            std::process::id(),
            id
        ))
    }

    fn synthetic_manifest() -> VolumeManifest {
        VolumeManifest {
            format: "rustwx-volume-store-v0".to_string(),
            model: "synthetic".to_string(),
            domain: "unit-test".to_string(),
            product: "pressure".to_string(),
            cycle: "2026-04-28T00:00:00Z".to_string(),
            forecast_hours: vec![0, 1],
            variables: vec![
                VolumeVariable::new("TMP", "Temperature", "K"),
                VolumeVariable::new("UGRD", "U wind", "m/s"),
            ],
            levels_hpa: vec![1000, 850],
            chunk_shape: ChunkShape {
                t: 1,
                z: 2,
                y: 4,
                x: 4,
            },
            codec: ChunkCodec::AffineI16RawV0.name().to_string(),
            grid: GridSpec::RegularLatLon {
                nx: 8,
                ny: 6,
                west_lon_deg: -124.0,
                east_lon_deg: -116.0,
                south_lat_deg: 32.0,
                north_lat_deg: 38.0,
            },
        }
    }

    fn synthetic_value(var: &str, hour: u8, level: u16, y: usize, x: usize) -> f32 {
        let var_base = if var == "TMP" { 250.0 } else { -20.0 };
        let level_base = if level == 1000 { 0.0 } else { 100.0 };
        var_base + level_base + f32::from(hour) * 10.0 + y as f32 + x as f32 * 0.25
    }

    #[test]
    fn writer_reader_round_trip_exact_grid_point() {
        let root = temp_store_dir("round_trip");
        let manifest = synthetic_manifest();
        let grid_len = manifest.grid.grid_len();
        let nx = manifest.grid.nx();
        let provider = |var: &str, hour: u8, level: u16| -> VolumeResult<Vec<f32>> {
            let mut values = Vec::with_capacity(grid_len);
            for y in 0..manifest.grid.ny() {
                for x in 0..nx {
                    values.push(synthetic_value(var, hour, level, y, x));
                }
            }
            Ok(values)
        };

        let stats = write_volume_store(&root, &manifest, provider).expect("write store");
        assert!(stats.raw_i16_bytes > 0);
        assert!(stats.chunk_count > 0);

        let store = VolumeStore::open(&root).expect("open store");
        let lon = -124.0 + 4.0 * (8.0 / 7.0);
        let lat = 35.6;
        let (grid_x, grid_y) = store.manifest().grid.grid_xy(lat, lon).unwrap();
        assert!((grid_x - 4.0).abs() < 1.0e-5);
        assert!((grid_y - 3.0).abs() < 1.0e-5);

        let profile = store
            .sample_point_3d(lat, lon, &["TMP"], &[1], &[850])
            .expect("sample point");
        assert_eq!(profile.samples.len(), 1);
        let expected = synthetic_value("TMP", 1, 850, 3, 4);
        assert!((profile.samples[0].value - expected).abs() <= 0.01);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn route_sampling_crosses_multiple_tiles() {
        let root = temp_store_dir("route");
        let manifest = synthetic_manifest();
        let nx = manifest.grid.nx();
        let provider = |var: &str, hour: u8, level: u16| -> VolumeResult<Vec<f32>> {
            let mut values = Vec::with_capacity(manifest.grid.grid_len());
            for y in 0..manifest.grid.ny() {
                for x in 0..nx {
                    values.push(synthetic_value(var, hour, level, y, x));
                }
            }
            Ok(values)
        };
        write_volume_store(&root, &manifest, provider).expect("write store");
        let store = VolumeStore::open(&root).expect("open store");
        let route = RouteDef {
            id: "test_route".to_string(),
            name: "Test route".to_string(),
            points: vec![(32.0, -124.0), (38.0, -116.0)],
            sample_spacing_km: 180.0,
        };
        let section = store
            .sample_route_3d(&route, &["TMP", "UGRD"], 0, &[1000, 850])
            .expect("sample route");

        assert!(section.route_samples.len() >= 5);
        assert_eq!(section.values.len(), section.route_samples.len() * 2 * 2);
        assert!(section.values.iter().all(|value| value.value.is_finite()));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn curvilinear_grid_samples_nearest_model_cell() {
        let grid = GridSpec::CurvilinearLatLon {
            nx: 3,
            ny: 2,
            lat_deg: vec![34.0, 34.1, 34.2, 35.0, 35.1, 35.2],
            lon_deg: vec![-121.0, -120.0, -119.0, -121.2, -120.2, -119.2],
            description: "test mesh".to_string(),
        };
        grid.validate().unwrap();
        let (x, y) = grid.grid_xy(35.08, -120.18).unwrap();
        assert_eq!((x.round() as usize, y.round() as usize), (1, 1));
    }
}
