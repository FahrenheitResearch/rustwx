use super::codec::ChunkCodec;
use super::index::{ChunkIndex, read_index_records, write_index_records};
use super::manifest::VolumeManifest;
use super::{VolumeResult, VolumeStoreError};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

pub trait VolumeFieldProvider {
    fn field_plane(
        &mut self,
        variable: &str,
        forecast_hour: u16,
        level_hpa: u16,
    ) -> VolumeResult<Vec<f32>>;
}

impl<F> VolumeFieldProvider for F
where
    F: for<'a> FnMut(&'a str, u16, u16) -> VolumeResult<Vec<f32>>,
{
    fn field_plane(
        &mut self,
        variable: &str,
        forecast_hour: u16,
        level_hpa: u16,
    ) -> VolumeResult<Vec<f32>> {
        self(variable, forecast_hour, level_hpa)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BuildStats {
    pub format: String,
    pub chunk_count: usize,
    pub grid_cells: usize,
    pub raw_f32_bytes: u64,
    pub raw_i16_bytes: u64,
    pub payload_bytes: u64,
    pub manifest_bytes: u64,
    pub index_bytes: u64,
    pub elapsed_ms: u64,
}

pub fn write_volume_store<P: VolumeFieldProvider>(
    root: &Path,
    manifest: &VolumeManifest,
    mut provider: P,
) -> VolumeResult<BuildStats> {
    let started = Instant::now();
    manifest.validate()?;
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

    let nt_blocks = manifest
        .forecast_hours
        .len()
        .div_ceil(manifest.chunk_shape.t);
    let nz_blocks = manifest.levels_hpa.len().div_ceil(manifest.chunk_shape.z);
    let ny_tiles = manifest.grid.ny().div_ceil(manifest.chunk_shape.y);
    let nx_tiles = manifest.grid.nx().div_ceil(manifest.chunk_shape.x);
    let grid_len = manifest.grid.grid_len();

    for (var_index, variable) in manifest.variables.iter().enumerate() {
        for t_block in 0..nt_blocks {
            let t0 = t_block * manifest.chunk_shape.t;
            let nt = manifest
                .chunk_shape
                .t
                .min(manifest.forecast_hours.len() - t0);
            for z_block in 0..nz_blocks {
                let z0 = z_block * manifest.chunk_shape.z;
                let nz = manifest.chunk_shape.z.min(manifest.levels_hpa.len() - z0);
                let mut slab = Vec::with_capacity(nt * nz);
                for local_t in 0..nt {
                    let hour = manifest.forecast_hours[t0 + local_t];
                    for local_z in 0..nz {
                        let level = manifest.levels_hpa[z0 + local_z];
                        let plane =
                            provider
                                .field_plane(&variable.name, hour, level)
                                .map_err(|err| {
                                    VolumeStoreError::Provider(format!(
                                        "{} f{hour:03} {level} hPa: {err}",
                                        variable.name
                                    ))
                                })?;
                        if plane.len() != grid_len {
                            return Err(VolumeStoreError::InvalidChunk(format!(
                                "field plane {} f{hour:03} {level} hPa has {} values, expected {}",
                                variable.name,
                                plane.len(),
                                grid_len
                            )));
                        }
                        slab.push(plane);
                    }
                }
                for y_tile in 0..ny_tiles {
                    for x_tile in 0..nx_tiles {
                        let chunk_id =
                            chunk_index.chunk_id(var_index, t_block, z_block, y_tile, x_tile)?;
                        let extent = chunk_index.chunk_extent(chunk_id)?;
                        let mut values = Vec::with_capacity(extent.value_count());
                        for local_t in 0..extent.nt {
                            for local_z in 0..extent.nz {
                                let plane = &slab[local_t * extent.nz + local_z];
                                for local_y in 0..extent.ny {
                                    let global_y = extent.y0 + local_y;
                                    let row_offset = global_y * manifest.grid.nx();
                                    for local_x in 0..extent.nx {
                                        values.push(plane[row_offset + extent.x0 + local_x]);
                                    }
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
