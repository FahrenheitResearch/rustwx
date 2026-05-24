use super::codec::ChunkCodec;
use super::grid::GridSpec;
use super::index::{read_index_records, write_index_records, ChunkIndex};
use super::manifest::{ChunkShape, VolumeManifest, VolumeVariable};
use super::writer::{write_volume_store, BuildStats, VolumeFieldProvider};
use super::{VolumeResult, VolumeStoreError};
use crate::gridded::PressureFields;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct PressureVolumeTimestep<'a> {
    pub forecast_hour: u16,
    pub pressure: &'a PressureFields,
}

pub trait PressureTimestepProvider {
    fn pressure_fields(&mut self, forecast_hour: u16) -> VolumeResult<PressureFields>;
}

impl<F> PressureTimestepProvider for F
where
    F: FnMut(u16) -> VolumeResult<PressureFields>,
{
    fn pressure_fields(&mut self, forecast_hour: u16) -> VolumeResult<PressureFields> {
        self(forecast_hour)
    }
}

pub fn write_pressure_volume_from_timesteps(
    root: &Path,
    model: impl Into<String>,
    domain: impl Into<String>,
    cycle: impl Into<String>,
    grid: GridSpec,
    chunk_shape: ChunkShape,
    timesteps: &[PressureVolumeTimestep<'_>],
) -> VolumeResult<BuildStats> {
    if timesteps.is_empty() {
        return Err(VolumeStoreError::InvalidManifest(
            "at least one pressure timestep is required".to_string(),
        ));
    }

    let grid_len = grid.grid_len();
    let levels_hpa = levels_from_pressure(timesteps[0].pressure)?;
    for timestep in timesteps {
        validate_pressure_fields(timestep.pressure, grid_len)?;
        if levels_from_pressure(timestep.pressure)? != levels_hpa {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "f{:03} pressure levels do not match the first timestep",
                timestep.forecast_hour
            )));
        }
    }

    let mut by_hour = BTreeMap::new();
    for timestep in timesteps {
        if by_hour
            .insert(timestep.forecast_hour, timestep.pressure)
            .is_some()
        {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "duplicate pressure timestep f{:03}",
                timestep.forecast_hour
            )));
        }
    }

    let forecast_hours = by_hour.keys().copied().collect::<Vec<_>>();
    let manifest = VolumeManifest {
        format: "rustwx-volume-store-v0".to_string(),
        model: model.into(),
        domain: domain.into(),
        product: "pressure".to_string(),
        cycle: cycle.into(),
        forecast_hours,
        variables: pressure_volume_variables_for_fields(timesteps[0].pressure),
        levels_hpa,
        chunk_shape,
        codec: ChunkCodec::AffineI16RawV0.name().to_string(),
        grid,
    };

    write_volume_store(
        root,
        &manifest,
        LoadedPressureProvider { by_hour, grid_len },
    )
}

pub fn write_pressure_volume_from_provider<P: PressureTimestepProvider>(
    root: &Path,
    model: impl Into<String>,
    domain: impl Into<String>,
    cycle: impl Into<String>,
    grid: GridSpec,
    chunk_shape: ChunkShape,
    forecast_hours: Vec<u16>,
    levels_hpa: Vec<u16>,
    variables: Vec<VolumeVariable>,
    mut provider: P,
) -> VolumeResult<BuildStats> {
    if chunk_shape.t != 1 {
        return Err(VolumeStoreError::InvalidManifest(
            "streamed pressure writer currently requires chunk_shape.t = 1".to_string(),
        ));
    }
    let manifest = VolumeManifest {
        format: "rustwx-volume-store-v0".to_string(),
        model: model.into(),
        domain: domain.into(),
        product: "pressure".to_string(),
        cycle: cycle.into(),
        forecast_hours,
        variables,
        levels_hpa,
        chunk_shape,
        codec: ChunkCodec::AffineI16RawV0.name().to_string(),
        grid,
    };
    write_streamed_pressure_volume(root, &manifest, &mut provider)
}

fn write_streamed_pressure_volume<P: PressureTimestepProvider>(
    root: &Path,
    manifest: &VolumeManifest,
    provider: &mut P,
) -> VolumeResult<BuildStats> {
    let started = Instant::now();
    manifest.validate()?;
    if manifest.chunk_shape.t != 1 {
        return Err(VolumeStoreError::InvalidManifest(
            "streamed pressure writer currently requires chunk_shape.t = 1".to_string(),
        ));
    }
    let codec = ChunkCodec::from_name(&manifest.codec)?;
    fs::create_dir_all(root)?;

    let manifest_path = root.join("manifest.json");
    let index_path = root.join("index.bin");
    let chunks_path = root.join("chunks.bin");
    let stats_path = root.join("build_stats.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(manifest)?)?;

    let mut records = ChunkIndex::empty_records(manifest);
    let chunk_index = ChunkIndex::new(manifest, records.clone())?;
    let mut chunks = BufWriter::new(File::create(&chunks_path)?);
    let mut payload_bytes = 0u64;

    let grid_len = manifest.grid.grid_len();
    let nz_blocks = manifest.levels_hpa.len().div_ceil(manifest.chunk_shape.z);
    let ny_tiles = manifest.grid.ny().div_ceil(manifest.chunk_shape.y);
    let nx_tiles = manifest.grid.nx().div_ceil(manifest.chunk_shape.x);

    for (hour_index, &hour) in manifest.forecast_hours.iter().enumerate() {
        let pressure = provider.pressure_fields(hour).map_err(|err| {
            VolumeStoreError::Provider(format!("load pressure f{hour:03}: {err}"))
        })?;
        validate_pressure_fields(&pressure, grid_len)?;
        if levels_from_pressure(&pressure)? != manifest.levels_hpa {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "f{hour:03} pressure levels do not match manifest"
            )));
        }

        for (var_index, variable) in manifest.variables.iter().enumerate() {
            for z_block in 0..nz_blocks {
                let z0 = z_block * manifest.chunk_shape.z;
                let nz = manifest.chunk_shape.z.min(manifest.levels_hpa.len() - z0);
                let mut slab = Vec::with_capacity(nz);
                for local_z in 0..nz {
                    let level = manifest.levels_hpa[z0 + local_z];
                    slab.push(pressure_plane(&pressure, &variable.name, level, grid_len)?);
                }

                for y_tile in 0..ny_tiles {
                    for x_tile in 0..nx_tiles {
                        let chunk_id =
                            chunk_index.chunk_id(var_index, hour_index, z_block, y_tile, x_tile)?;
                        let extent = chunk_index.chunk_extent(chunk_id)?;
                        let mut values = Vec::with_capacity(extent.value_count());
                        for local_z in 0..extent.nz {
                            let plane = &slab[local_z];
                            for local_y in 0..extent.ny {
                                let global_y = extent.y0 + local_y;
                                let row_offset = global_y * manifest.grid.nx();
                                for local_x in 0..extent.nx {
                                    values.push(plane[row_offset + extent.x0 + local_x]);
                                }
                            }
                        }
                        let mut encoded = codec.encode(&values)?;
                        encoded.record.offset = payload_bytes;
                        chunks.write_all(&encoded.payload)?;
                        payload_bytes += encoded.payload.len() as u64;
                        records[chunk_id] = encoded.record;
                    }
                }
            }
        }
    }
    chunks.flush()?;
    write_index_records(File::create(&index_path)?, &records)?;

    let verified_records = read_index_records(File::open(&index_path)?)?;
    if verified_records.len() != records.len() {
        return Err(VolumeStoreError::InvalidIndex(
            "index verification record count mismatch".to_string(),
        ));
    }

    let raw_values = manifest.variables.len()
        * manifest.forecast_hours.len()
        * manifest.levels_hpa.len()
        * manifest.grid.grid_len();
    let stats = BuildStats {
        format: manifest.format.clone(),
        chunk_count: records.len(),
        grid_cells: manifest.grid.grid_len(),
        raw_f32_bytes: (raw_values * 4) as u64,
        raw_i16_bytes: (raw_values * 2) as u64,
        payload_bytes,
        manifest_bytes: fs::metadata(&manifest_path)?.len(),
        index_bytes: fs::metadata(&index_path)?.len(),
        elapsed_ms: started.elapsed().as_millis() as u64,
    };
    fs::write(stats_path, serde_json::to_vec_pretty(&stats)?)?;
    Ok(stats)
}

pub fn pressure_volume_variables_for_fields(pressure: &PressureFields) -> Vec<VolumeVariable> {
    let mut variables = vec![
        VolumeVariable::new("TMP", "Temperature", "degC"),
        VolumeVariable::new("SPFH", "Specific humidity", "kg/kg"),
        VolumeVariable::new("UGRD", "U wind", "m/s"),
        VolumeVariable::new("VGRD", "V wind", "m/s"),
        VolumeVariable::new("HGT", "Geopotential height", "m"),
    ];
    if pressure.omega_pa_s_3d.is_some() {
        variables.push(VolumeVariable::new("VVEL", "Vertical velocity", "Pa/s"));
    }
    if pressure.absolute_vorticity_s_3d.is_some() {
        variables.push(VolumeVariable::new("ABSV", "Absolute vorticity", "s^-1"));
    }
    if pressure.cloud_liquid_kgkg_3d.is_some() {
        variables.push(VolumeVariable::new("CLWMR", "Cloud liquid water", "kg/kg"));
    }
    if pressure.cloud_ice_kgkg_3d.is_some() {
        variables.push(VolumeVariable::new(
            "ICMR",
            "Cloud ice mixing ratio",
            "kg/kg",
        ));
    }
    if pressure.rain_kgkg_3d.is_some() {
        variables.push(VolumeVariable::new("RWMR", "Rain mixing ratio", "kg/kg"));
    }
    if pressure.snow_kgkg_3d.is_some() {
        variables.push(VolumeVariable::new("SNMR", "Snow mixing ratio", "kg/kg"));
    }
    if pressure.graupel_kgkg_3d.is_some() {
        variables.push(VolumeVariable::new("GRLE", "Graupel mixing ratio", "kg/kg"));
    }
    variables
}

struct LoadedPressureProvider<'a> {
    by_hour: BTreeMap<u16, &'a PressureFields>,
    grid_len: usize,
}

impl VolumeFieldProvider for LoadedPressureProvider<'_> {
    fn field_plane(
        &mut self,
        variable: &str,
        forecast_hour: u16,
        level_hpa: u16,
    ) -> VolumeResult<Vec<f32>> {
        let pressure = self
            .by_hour
            .get(&forecast_hour)
            .ok_or(VolumeStoreError::MissingHour(forecast_hour))?;
        pressure_plane(pressure, variable, level_hpa, self.grid_len)
    }
}

fn validate_pressure_fields(pressure: &PressureFields, grid_len: usize) -> VolumeResult<()> {
    let level_count = pressure.pressure_levels_hpa.len();
    let expected = level_count * grid_len;
    for (name, values) in [
        ("temperature_c_3d", &pressure.temperature_c_3d),
        ("qvapor_kgkg_3d", &pressure.qvapor_kgkg_3d),
        ("u_ms_3d", &pressure.u_ms_3d),
        ("v_ms_3d", &pressure.v_ms_3d),
        ("gh_m_3d", &pressure.gh_m_3d),
    ] {
        if values.len() != expected {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "{name} has {} values, expected {expected}",
                values.len()
            )));
        }
    }
    for (name, values) in [
        ("omega_pa_s_3d", pressure.omega_pa_s_3d.as_ref()),
        (
            "absolute_vorticity_s_3d",
            pressure.absolute_vorticity_s_3d.as_ref(),
        ),
        (
            "cloud_liquid_kgkg_3d",
            pressure.cloud_liquid_kgkg_3d.as_ref(),
        ),
        ("cloud_ice_kgkg_3d", pressure.cloud_ice_kgkg_3d.as_ref()),
        ("rain_kgkg_3d", pressure.rain_kgkg_3d.as_ref()),
        ("snow_kgkg_3d", pressure.snow_kgkg_3d.as_ref()),
        ("graupel_kgkg_3d", pressure.graupel_kgkg_3d.as_ref()),
    ] {
        if let Some(values) = values {
            if values.len() != expected {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "{name} has {} values, expected {expected}",
                    values.len()
                )));
            }
        }
    }
    Ok(())
}

fn levels_from_pressure(pressure: &PressureFields) -> VolumeResult<Vec<u16>> {
    let mut levels = Vec::with_capacity(pressure.pressure_levels_hpa.len());
    for level in &pressure.pressure_levels_hpa {
        if !level.is_finite() || *level <= 0.0 || *level > f64::from(u16::MAX) {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "invalid pressure level {level}"
            )));
        }
        let rounded = level.round() as u16;
        if rounded > 0 {
            levels.push(rounded);
        }
    }
    levels.sort_unstable_by(|left, right| right.cmp(left));
    levels.dedup();
    if levels.is_empty() {
        return Err(VolumeStoreError::InvalidManifest(
            "no integer pressure levels remain after rounding".to_string(),
        ));
    }
    Ok(levels)
}

fn pressure_plane(
    pressure: &PressureFields,
    variable: &str,
    level_hpa: u16,
    grid_len: usize,
) -> VolumeResult<Vec<f32>> {
    let level_index = pressure
        .pressure_levels_hpa
        .iter()
        .position(|level| (*level - f64::from(level_hpa)).abs() < 0.5)
        .ok_or(VolumeStoreError::MissingLevel(level_hpa))?;
    let values = match variable {
        "TMP" => &pressure.temperature_c_3d,
        "SPFH" => &pressure.qvapor_kgkg_3d,
        "UGRD" => &pressure.u_ms_3d,
        "VGRD" => &pressure.v_ms_3d,
        "HGT" => &pressure.gh_m_3d,
        "VVEL" => pressure
            .omega_pa_s_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        "ABSV" => pressure
            .absolute_vorticity_s_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        "CLWMR" => pressure
            .cloud_liquid_kgkg_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        "ICMR" | "CIMIXR" => pressure
            .cloud_ice_kgkg_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        "RWMR" => pressure
            .rain_kgkg_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        "SNMR" => pressure
            .snow_kgkg_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        "GRLE" => pressure
            .graupel_kgkg_3d
            .as_ref()
            .ok_or_else(|| VolumeStoreError::MissingVariable(variable.to_string()))?,
        other => return Err(VolumeStoreError::MissingVariable(other.to_string())),
    };
    let start = level_index * grid_len;
    let end = start + grid_len;
    if end > values.len() {
        return Err(VolumeStoreError::InvalidChunk(format!(
            "{variable} {level_hpa} hPa slice {start}..{end} exceeds {} values",
            values.len()
        )));
    }
    Ok(values[start..end]
        .iter()
        .map(|value| *value as f32)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_store::reader::VolumeStore;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn temp_store_dir(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rustwx_pressure_volume_{name}_{}_{}",
            std::process::id(),
            id
        ))
    }

    fn pressure(hour: u16, nx: usize, ny: usize) -> PressureFields {
        let levels = vec![1000.0, 850.0];
        let mut temperature = Vec::new();
        let mut qvapor = Vec::new();
        let mut u = Vec::new();
        let mut v = Vec::new();
        let mut hgt = Vec::new();
        for (level_index, _) in levels.iter().enumerate() {
            for y in 0..ny {
                for x in 0..nx {
                    let base = f64::from(hour) * 10.0 + level_index as f64 * 100.0;
                    temperature.push(base + y as f64 + x as f64 * 0.25);
                    qvapor.push(0.001 + base * 0.00001);
                    u.push(5.0 + x as f64);
                    v.push(-3.0 + y as f64);
                    hgt.push(100.0 + base);
                }
            }
        }
        PressureFields {
            pressure_levels_hpa: levels,
            pressure_3d_pa: None,
            temperature_c_3d: temperature,
            qvapor_kgkg_3d: qvapor,
            u_ms_3d: u,
            v_ms_3d: v,
            gh_m_3d: hgt,
            omega_pa_s_3d: None,
            absolute_vorticity_s_3d: None,
            cloud_liquid_kgkg_3d: None,
            cloud_ice_kgkg_3d: None,
            rain_kgkg_3d: None,
            snow_kgkg_3d: None,
            graupel_kgkg_3d: None,
        }
    }

    fn pressure_with_levels(hour: u16, nx: usize, ny: usize, levels: Vec<f64>) -> PressureFields {
        let mut pressure = pressure(hour, nx, ny);
        pressure.pressure_levels_hpa = levels.clone();
        let grid_len = nx * ny;
        let rewrite = |values: &mut Vec<f64>| {
            values.clear();
            for (level_index, _) in levels.iter().enumerate() {
                for index in 0..grid_len {
                    values.push(f64::from(hour) * 10.0 + level_index as f64 * 100.0 + index as f64);
                }
            }
        };
        rewrite(&mut pressure.temperature_c_3d);
        rewrite(&mut pressure.qvapor_kgkg_3d);
        rewrite(&mut pressure.u_ms_3d);
        rewrite(&mut pressure.v_ms_3d);
        rewrite(&mut pressure.gh_m_3d);
        pressure
    }

    #[test]
    fn levels_from_pressure_normalizes_ascending_model_order() {
        let pressure =
            pressure_with_levels(0, 2, 2, vec![0.4, 100.0, 500.0, 850.0, 1000.0, 1000.0]);
        assert_eq!(
            levels_from_pressure(&pressure).unwrap(),
            vec![1000, 850, 500, 100]
        );
        let plane = pressure_plane(&pressure, "TMP", 1000, 4).unwrap();
        assert_eq!(plane[0], 400.0);
    }

    #[test]
    fn pressure_timesteps_write_volume_store() {
        let root = temp_store_dir("adapter");
        let grid = GridSpec::RegularLatLon {
            nx: 6,
            ny: 5,
            west_lon_deg: -124.0,
            east_lon_deg: -118.0,
            south_lat_deg: 32.0,
            north_lat_deg: 37.0,
        };
        let p0 = pressure(0, 6, 5);
        let p1 = pressure(1, 6, 5);
        let stats = write_pressure_volume_from_timesteps(
            &root,
            "hrrr",
            "california-test",
            "2026-04-28T00:00:00Z",
            grid,
            ChunkShape {
                t: 1,
                z: 2,
                y: 3,
                x: 3,
            },
            &[
                PressureVolumeTimestep {
                    forecast_hour: 0,
                    pressure: &p0,
                },
                PressureVolumeTimestep {
                    forecast_hour: 1,
                    pressure: &p1,
                },
            ],
        )
        .expect("write pressure store");
        assert_eq!(stats.grid_cells, 30);
        let store = VolumeStore::open(&root).expect("open pressure volume");
        let profile = store
            .sample_point_3d(34.5, -121.6, &["TMP", "UGRD"], &[1], &[850])
            .expect("sample pressure volume");
        assert_eq!(profile.samples.len(), 2);
        assert!(profile
            .samples
            .iter()
            .all(|sample| sample.value.is_finite()));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn streamed_provider_writes_each_pressure_hour_once() {
        let root = temp_store_dir("streamed_provider");
        let grid = GridSpec::RegularLatLon {
            nx: 6,
            ny: 5,
            west_lon_deg: -124.0,
            east_lon_deg: -118.0,
            south_lat_deg: 32.0,
            north_lat_deg: 37.0,
        };
        let mut loaded_hours = Vec::new();
        let stats = write_pressure_volume_from_provider(
            &root,
            "hrrr",
            "california-test",
            "2026-04-28T00:00:00Z",
            grid,
            ChunkShape {
                t: 1,
                z: 2,
                y: 3,
                x: 3,
            },
            vec![0, 1],
            vec![1000, 850],
            pressure_volume_variables_for_fields(&pressure(0, 6, 5)),
            |hour| {
                loaded_hours.push(hour);
                Ok(pressure(hour, 6, 5))
            },
        )
        .expect("stream pressure volume");
        assert_eq!(stats.grid_cells, 30);
        assert_eq!(loaded_hours, vec![0, 1]);

        let store = VolumeStore::open(&root).expect("open pressure volume");
        let profile = store
            .sample_point_3d(34.5, -121.6, &["TMP"], &[1], &[850])
            .expect("sample pressure volume");
        assert_eq!(profile.samples.len(), 1);
        assert!(profile.samples[0].value.is_finite());
        let _ = std::fs::remove_dir_all(root);
    }
}
