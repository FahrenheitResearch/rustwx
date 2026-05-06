use super::sampling::RouteSample;
use super::{VolumeResult, VolumeStoreError};
use crate::gridded::SurfaceFields;
use rustwx_cross_section::TerrainProfile;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

pub const SURFACE_TERRAIN_FORMAT: &str = "rustwx-volume-surface-terrain-v0";
pub const SURFACE_TERRAIN_MANIFEST_FILE: &str = "surface_terrain.json";
pub const SURFACE_TERRAIN_PAYLOAD_FILE: &str = "surface_terrain.bin";

#[derive(Debug, Clone)]
pub struct SurfaceTerrainTimestep {
    pub forecast_hour: u8,
    pub psfc_pa: Vec<f32>,
    pub orog_m: Vec<f32>,
}

impl SurfaceTerrainTimestep {
    pub fn from_surface(forecast_hour: u8, surface: &SurfaceFields) -> Self {
        Self {
            forecast_hour,
            psfc_pa: surface.psfc_pa.iter().map(|value| *value as f32).collect(),
            orog_m: surface.orog_m.iter().map(|value| *value as f32).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceTerrainManifest {
    pub format: String,
    pub forecast_hours: Vec<u8>,
    pub grid_cells: usize,
    pub payload_file: String,
    pub orog_offset_bytes: u64,
    pub psfc_offsets_bytes: BTreeMap<u8, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceTerrainBuildStats {
    pub format: String,
    pub forecast_hours: Vec<u8>,
    pub grid_cells: usize,
    pub payload_bytes: u64,
    pub manifest_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SurfaceTerrainStore {
    manifest: SurfaceTerrainManifest,
    orog_m: Vec<f32>,
    psfc_by_hour: BTreeMap<u8, Vec<f32>>,
}

impl SurfaceTerrainStore {
    pub fn open(root: &Path) -> VolumeResult<Self> {
        let manifest_path = root.join(SURFACE_TERRAIN_MANIFEST_FILE);
        let manifest_bytes = fs::read(&manifest_path)?;
        let manifest: SurfaceTerrainManifest = serde_json::from_slice(&manifest_bytes)?;
        manifest.validate()?;

        let mut payload = Vec::new();
        File::open(root.join(&manifest.payload_file))?.read_to_end(&mut payload)?;
        let expected_len = (1 + manifest.forecast_hours.len())
            .checked_mul(manifest.grid_cells)
            .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| {
                VolumeStoreError::InvalidManifest(
                    "surface terrain payload byte count overflow".to_string(),
                )
            })?;
        if payload.len() != expected_len {
            return Err(VolumeStoreError::InvalidChunk(format!(
                "surface terrain payload had {} bytes, expected {expected_len}",
                payload.len()
            )));
        }

        let orog_m = read_f32_slice(&payload, manifest.orog_offset_bytes, manifest.grid_cells)?;
        let mut psfc_by_hour = BTreeMap::new();
        for hour in &manifest.forecast_hours {
            let offset = manifest
                .psfc_offsets_bytes
                .get(hour)
                .copied()
                .ok_or_else(|| {
                    VolumeStoreError::InvalidManifest(format!(
                        "surface terrain missing f{hour:03} psfc offset"
                    ))
                })?;
            psfc_by_hour.insert(
                *hour,
                read_f32_slice(&payload, offset, manifest.grid_cells)?,
            );
        }

        Ok(Self {
            manifest,
            orog_m,
            psfc_by_hour,
        })
    }

    pub fn open_optional(root: &Path) -> VolumeResult<Option<Self>> {
        if !root.join(SURFACE_TERRAIN_MANIFEST_FILE).exists() {
            return Ok(None);
        }
        Self::open(root).map(Some)
    }

    pub fn manifest(&self) -> &SurfaceTerrainManifest {
        &self.manifest
    }

    pub fn terrain_profile(
        &self,
        forecast_hour: u8,
        route_samples: &[RouteSample],
        distances_km: Vec<f64>,
        nx: usize,
        ny: usize,
    ) -> VolumeResult<TerrainProfile> {
        let psfc_pa = self
            .psfc_by_hour
            .get(&forecast_hour)
            .ok_or(VolumeStoreError::MissingHour(forecast_hour))?;
        let mut surface_pressure_hpa = Vec::with_capacity(route_samples.len());
        let mut surface_height_m = Vec::with_capacity(route_samples.len());
        for sample in route_samples {
            surface_pressure_hpa.push(
                bilinear(psfc_pa, nx, ny, sample.grid_x, sample.grid_y).max(0.0) as f64 / 100.0,
            );
            surface_height_m
                .push(bilinear(&self.orog_m, nx, ny, sample.grid_x, sample.grid_y) as f64);
        }
        TerrainProfile::new(distances_km)
            .and_then(|terrain| terrain.with_surface_pressure_hpa(surface_pressure_hpa))
            .and_then(|terrain| terrain.with_surface_height_m(surface_height_m))
            .map_err(|err| VolumeStoreError::InvalidManifest(err.to_string()))
    }

    pub fn sample_grid_point(
        &self,
        forecast_hour: u8,
        grid_x: f32,
        grid_y: f32,
        nx: usize,
        ny: usize,
    ) -> VolumeResult<SurfaceTerrainPoint> {
        let psfc_pa = self
            .psfc_by_hour
            .get(&forecast_hour)
            .ok_or(VolumeStoreError::MissingHour(forecast_hour))?;
        Ok(SurfaceTerrainPoint {
            surface_pressure_hpa: bilinear(psfc_pa, nx, ny, grid_x, grid_y).max(0.0) as f64 / 100.0,
            surface_height_m_msl: bilinear(&self.orog_m, nx, ny, grid_x, grid_y) as f64,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SurfaceTerrainPoint {
    pub surface_pressure_hpa: f64,
    pub surface_height_m_msl: f64,
}

impl SurfaceTerrainManifest {
    fn validate(&self) -> VolumeResult<()> {
        if self.format != SURFACE_TERRAIN_FORMAT {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "unsupported surface terrain format '{}'",
                self.format
            )));
        }
        if self.forecast_hours.is_empty() {
            return Err(VolumeStoreError::InvalidManifest(
                "surface terrain requires at least one forecast hour".to_string(),
            ));
        }
        if self.grid_cells == 0 {
            return Err(VolumeStoreError::InvalidManifest(
                "surface terrain grid_cells must be positive".to_string(),
            ));
        }
        if self
            .forecast_hours
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(VolumeStoreError::InvalidManifest(
                "surface terrain forecast hours must be strictly increasing".to_string(),
            ));
        }
        for hour in &self.forecast_hours {
            if !self.psfc_offsets_bytes.contains_key(hour) {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "surface terrain missing f{hour:03} psfc offset"
                )));
            }
        }
        Ok(())
    }
}

pub fn write_surface_terrain_store(
    root: &Path,
    mut timesteps: Vec<SurfaceTerrainTimestep>,
    grid_cells: usize,
) -> VolumeResult<SurfaceTerrainBuildStats> {
    if timesteps.is_empty() {
        return Err(VolumeStoreError::InvalidManifest(
            "at least one surface terrain timestep is required".to_string(),
        ));
    }
    timesteps.sort_by_key(|timestep| timestep.forecast_hour);
    if timesteps
        .windows(2)
        .any(|pair| pair[0].forecast_hour == pair[1].forecast_hour)
    {
        return Err(VolumeStoreError::InvalidManifest(
            "duplicate surface terrain forecast hour".to_string(),
        ));
    }
    for timestep in &timesteps {
        if timestep.psfc_pa.len() != grid_cells || timestep.orog_m.len() != grid_cells {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "surface terrain f{:03} grid length mismatch: psfc={} orog={} expected {grid_cells}",
                timestep.forecast_hour,
                timestep.psfc_pa.len(),
                timestep.orog_m.len()
            )));
        }
    }

    fs::create_dir_all(root)?;
    let payload_path = root.join(SURFACE_TERRAIN_PAYLOAD_FILE);
    let mut payload = BufWriter::new(File::create(&payload_path)?);
    let mut offset = 0u64;
    let orog_offset_bytes = offset;
    write_f32_values(&mut payload, &timesteps[0].orog_m)?;
    offset += byte_len_f32(grid_cells);

    let mut psfc_offsets_bytes = BTreeMap::new();
    for timestep in &timesteps {
        psfc_offsets_bytes.insert(timestep.forecast_hour, offset);
        write_f32_values(&mut payload, &timestep.psfc_pa)?;
        offset += byte_len_f32(grid_cells);
    }
    payload.flush()?;

    let manifest = SurfaceTerrainManifest {
        format: SURFACE_TERRAIN_FORMAT.to_string(),
        forecast_hours: timesteps
            .iter()
            .map(|timestep| timestep.forecast_hour)
            .collect(),
        grid_cells,
        payload_file: SURFACE_TERRAIN_PAYLOAD_FILE.to_string(),
        orog_offset_bytes,
        psfc_offsets_bytes,
    };
    manifest.validate()?;
    let manifest_path = root.join(SURFACE_TERRAIN_MANIFEST_FILE);
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(SurfaceTerrainBuildStats {
        format: SURFACE_TERRAIN_FORMAT.to_string(),
        forecast_hours: manifest.forecast_hours,
        grid_cells,
        payload_bytes: fs::metadata(payload_path)?.len(),
        manifest_bytes: fs::metadata(manifest_path)?.len(),
    })
}

fn write_f32_values<W: Write>(writer: &mut W, values: &[f32]) -> VolumeResult<()> {
    for value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

fn read_f32_slice(payload: &[u8], offset: u64, count: usize) -> VolumeResult<Vec<f32>> {
    let start = usize::try_from(offset).map_err(|_| {
        VolumeStoreError::InvalidChunk("surface terrain offset does not fit platform".to_string())
    })?;
    let byte_len = count
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| {
            VolumeStoreError::InvalidChunk("surface terrain byte count overflow".to_string())
        })?;
    let end = start.checked_add(byte_len).ok_or_else(|| {
        VolumeStoreError::InvalidChunk("surface terrain range overflow".to_string())
    })?;
    if end > payload.len() {
        return Err(VolumeStoreError::InvalidChunk(format!(
            "surface terrain range {start}..{end} exceeds payload length {}",
            payload.len()
        )));
    }
    let mut values = Vec::with_capacity(count);
    for chunk in payload[start..end].chunks_exact(4) {
        values.push(f32::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(values)
}

fn byte_len_f32(count: usize) -> u64 {
    (count * std::mem::size_of::<f32>()) as u64
}

fn bilinear(values: &[f32], nx: usize, ny: usize, grid_x: f32, grid_y: f32) -> f32 {
    if nx == 0 || ny == 0 {
        return f32::NAN;
    }
    let x0 = grid_x.floor().clamp(0.0, (nx - 1) as f32) as usize;
    let y0 = grid_y.floor().clamp(0.0, (ny - 1) as f32) as usize;
    let x1 = (x0 + 1).min(nx - 1);
    let y1 = (y0 + 1).min(ny - 1);
    let wx = grid_x - x0 as f32;
    let wy = grid_y - y0 as f32;
    let v00 = values[y0 * nx + x0];
    let v10 = values[y0 * nx + x1];
    let v01 = values[y1 * nx + x0];
    let v11 = values[y1 * nx + x1];
    if !(v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite()) {
        return f32::NAN;
    }
    let top = v00 * (1.0 - wx) + v10 * wx;
    let bottom = v01 * (1.0 - wx) + v11 * wx;
    top * (1.0 - wy) + bottom * wy
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
            "rustwx_surface_terrain_{name}_{}_{}",
            std::process::id(),
            id
        ))
    }

    #[test]
    fn surface_terrain_store_round_trips_profile() {
        let root = temp_store_dir("round_trip");
        write_surface_terrain_store(
            &root,
            vec![SurfaceTerrainTimestep {
                forecast_hour: 0,
                psfc_pa: vec![100000.0, 95000.0, 90000.0, 85000.0],
                orog_m: vec![0.0, 500.0, 1000.0, 1500.0],
            }],
            4,
        )
        .expect("write terrain");
        let store = SurfaceTerrainStore::open(&root).expect("open terrain");
        let route_samples = vec![
            RouteSample {
                distance_km: 0.0,
                lat_deg: 0.0,
                lon_deg: 0.0,
                grid_x: 0.0,
                grid_y: 0.0,
                x0: 0,
                y0: 0,
                wx: 0.0,
                wy: 0.0,
                route_unit_u: 1.0,
                route_unit_v: 0.0,
            },
            RouteSample {
                distance_km: 10.0,
                lat_deg: 0.0,
                lon_deg: 1.0,
                grid_x: 1.0,
                grid_y: 1.0,
                x0: 1,
                y0: 1,
                wx: 0.0,
                wy: 0.0,
                route_unit_u: 1.0,
                route_unit_v: 0.0,
            },
        ];
        let terrain = store
            .terrain_profile(0, &route_samples, vec![0.0, 10.0], 2, 2)
            .expect("sample terrain");
        assert_eq!(terrain.surface_pressure_hpa().unwrap(), &[1000.0, 850.0]);
        assert_eq!(terrain.surface_height_m().unwrap(), &[0.0, 1500.0]);
        let _ = fs::remove_dir_all(root);
    }
}
