use super::manifest::VolumeManifest;
use super::{VolumeResult, VolumeStoreError};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

pub const CHUNK_INDEX_RECORD_BYTES: usize = 36;

pub const FLAG_EMPTY: u32 = 1 << 0;
pub const FLAG_CONSTANT: u32 = 1 << 1;
pub const FLAG_DENSE_I16: u32 = 1 << 2;
pub const FLAG_HAS_MISSING_SENTINEL: u32 = 1 << 3;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ChunkIndexRecord {
    pub offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub center: f32,
    pub scale: f32,
    pub valid_min: f32,
    pub valid_max: f32,
    pub flags: u32,
}

impl ChunkIndexRecord {
    pub fn empty() -> Self {
        Self {
            offset: 0,
            compressed_len: 0,
            uncompressed_len: 0,
            center: f32::NAN,
            scale: f32::NAN,
            valid_min: f32::NAN,
            valid_max: f32::NAN,
            flags: FLAG_EMPTY,
        }
    }

    pub fn constant(value: f32) -> Self {
        Self {
            offset: 0,
            compressed_len: 0,
            uncompressed_len: 0,
            center: value,
            scale: 0.0,
            valid_min: value,
            valid_max: value,
            flags: FLAG_CONSTANT,
        }
    }

    pub fn to_le_bytes(self) -> [u8; CHUNK_INDEX_RECORD_BYTES] {
        let mut bytes = [0u8; CHUNK_INDEX_RECORD_BYTES];
        bytes[0..8].copy_from_slice(&self.offset.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.compressed_len.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.center.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.scale.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.valid_min.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.valid_max.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.flags.to_le_bytes());
        bytes
    }

    pub fn from_le_bytes(bytes: [u8; CHUNK_INDEX_RECORD_BYTES]) -> Self {
        Self {
            offset: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            center: f32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            scale: f32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            valid_min: f32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            valid_max: f32::from_le_bytes(bytes[28..32].try_into().unwrap()),
            flags: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkExtent {
    pub var_index: usize,
    pub t0: usize,
    pub nt: usize,
    pub z0: usize,
    pub nz: usize,
    pub y0: usize,
    pub ny: usize,
    pub x0: usize,
    pub nx: usize,
}

impl ChunkExtent {
    pub fn value_count(self) -> usize {
        self.nt * self.nz * self.ny * self.nx
    }

    pub fn linear_index(self, t: usize, z: usize, y: usize, x: usize) -> usize {
        (((t * self.nz + z) * self.ny + y) * self.nx) + x
    }
}

#[derive(Debug, Clone)]
pub struct ChunkIndex {
    manifest: VolumeManifest,
    records: Vec<ChunkIndexRecord>,
    nt_blocks: usize,
    nz_blocks: usize,
    ny_tiles: usize,
    nx_tiles: usize,
}

impl ChunkIndex {
    pub fn new(manifest: &VolumeManifest, records: Vec<ChunkIndexRecord>) -> VolumeResult<Self> {
        manifest.validate()?;
        let nt_blocks = manifest
            .forecast_hours
            .len()
            .div_ceil(manifest.chunk_shape.t);
        let nz_blocks = manifest.levels_hpa.len().div_ceil(manifest.chunk_shape.z);
        let ny_tiles = manifest.grid.ny().div_ceil(manifest.chunk_shape.y);
        let nx_tiles = manifest.grid.nx().div_ceil(manifest.chunk_shape.x);
        let expected = manifest.variables.len() * nt_blocks * nz_blocks * ny_tiles * nx_tiles;
        if records.len() != expected {
            return Err(VolumeStoreError::InvalidIndex(format!(
                "record count {} does not match expected {}",
                records.len(),
                expected
            )));
        }
        Ok(Self {
            manifest: manifest.clone(),
            records,
            nt_blocks,
            nz_blocks,
            ny_tiles,
            nx_tiles,
        })
    }

    pub fn empty_records(manifest: &VolumeManifest) -> Vec<ChunkIndexRecord> {
        vec![ChunkIndexRecord::empty(); manifest.chunk_count()]
    }

    pub fn records(&self) -> &[ChunkIndexRecord] {
        &self.records
    }

    pub fn record(&self, chunk_id: usize) -> VolumeResult<ChunkIndexRecord> {
        self.records.get(chunk_id).copied().ok_or_else(|| {
            VolumeStoreError::InvalidIndex(format!("chunk id {chunk_id} out of range"))
        })
    }

    pub fn chunk_id(
        &self,
        var_index: usize,
        t_block: usize,
        z_block: usize,
        y_tile: usize,
        x_tile: usize,
    ) -> VolumeResult<usize> {
        if var_index >= self.manifest.variables.len()
            || t_block >= self.nt_blocks
            || z_block >= self.nz_blocks
            || y_tile >= self.ny_tiles
            || x_tile >= self.nx_tiles
        {
            return Err(VolumeStoreError::InvalidIndex(format!(
                "chunk coordinates out of range: var={var_index} t={t_block} z={z_block} y={y_tile} x={x_tile}"
            )));
        }
        Ok(
            ((((var_index * self.nt_blocks + t_block) * self.nz_blocks + z_block) * self.ny_tiles
                + y_tile)
                * self.nx_tiles)
                + x_tile,
        )
    }

    pub fn chunk_extent(&self, chunk_id: usize) -> VolumeResult<ChunkExtent> {
        if chunk_id >= self.records.len() {
            return Err(VolumeStoreError::InvalidIndex(format!(
                "chunk id {chunk_id} out of range"
            )));
        }
        let mut remaining = chunk_id;
        let x_tile = remaining % self.nx_tiles;
        remaining /= self.nx_tiles;
        let y_tile = remaining % self.ny_tiles;
        remaining /= self.ny_tiles;
        let z_block = remaining % self.nz_blocks;
        remaining /= self.nz_blocks;
        let t_block = remaining % self.nt_blocks;
        remaining /= self.nt_blocks;
        let var_index = remaining;
        let t0 = t_block * self.manifest.chunk_shape.t;
        let z0 = z_block * self.manifest.chunk_shape.z;
        let y0 = y_tile * self.manifest.chunk_shape.y;
        let x0 = x_tile * self.manifest.chunk_shape.x;
        Ok(ChunkExtent {
            var_index,
            t0,
            nt: self
                .manifest
                .chunk_shape
                .t
                .min(self.manifest.forecast_hours.len() - t0),
            z0,
            nz: self
                .manifest
                .chunk_shape
                .z
                .min(self.manifest.levels_hpa.len() - z0),
            y0,
            ny: self
                .manifest
                .chunk_shape
                .y
                .min(self.manifest.grid.ny() - y0),
            x0,
            nx: self
                .manifest
                .chunk_shape
                .x
                .min(self.manifest.grid.nx() - x0),
        })
    }

    pub fn chunk_for_indices(
        &self,
        var_index: usize,
        hour_index: usize,
        level_index: usize,
        y: usize,
        x: usize,
    ) -> VolumeResult<(usize, ChunkExtent)> {
        let t_block = hour_index / self.manifest.chunk_shape.t;
        let z_block = level_index / self.manifest.chunk_shape.z;
        let y_tile = y / self.manifest.chunk_shape.y;
        let x_tile = x / self.manifest.chunk_shape.x;
        let chunk_id = self.chunk_id(var_index, t_block, z_block, y_tile, x_tile)?;
        let extent = self.chunk_extent(chunk_id)?;
        Ok((chunk_id, extent))
    }
}

pub fn write_index_records<W: Write>(
    mut writer: W,
    records: &[ChunkIndexRecord],
) -> VolumeResult<()> {
    for record in records {
        writer.write_all(&record.to_le_bytes())?;
    }
    Ok(())
}

pub fn read_index_records<R: Read>(mut reader: R) -> VolumeResult<Vec<ChunkIndexRecord>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    if bytes.len() % CHUNK_INDEX_RECORD_BYTES != 0 {
        return Err(VolumeStoreError::InvalidIndex(format!(
            "index byte length {} is not divisible by {}",
            bytes.len(),
            CHUNK_INDEX_RECORD_BYTES
        )));
    }
    let mut records = Vec::with_capacity(bytes.len() / CHUNK_INDEX_RECORD_BYTES);
    for chunk in bytes.chunks_exact(CHUNK_INDEX_RECORD_BYTES) {
        records.push(ChunkIndexRecord::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_store::codec::ChunkCodec;
    use crate::volume_store::grid::GridSpec;
    use crate::volume_store::manifest::{ChunkShape, VolumeManifest, VolumeVariable};

    fn manifest() -> VolumeManifest {
        VolumeManifest {
            format: "rustwx-volume-store-v0".to_string(),
            model: "synthetic".to_string(),
            domain: "test".to_string(),
            product: "pressure".to_string(),
            cycle: "2026-04-28T00:00:00Z".to_string(),
            forecast_hours: vec![0, 1, 2],
            variables: vec![VolumeVariable::new("TMP", "Temperature", "K")],
            levels_hpa: vec![1000, 850, 700, 500, 300],
            chunk_shape: ChunkShape {
                t: 1,
                z: 4,
                y: 4,
                x: 4,
            },
            codec: ChunkCodec::AffineI16RawV0.name().to_string(),
            grid: GridSpec::RegularLatLon {
                nx: 9,
                ny: 7,
                west_lon_deg: -1.0,
                east_lon_deg: 1.0,
                south_lat_deg: -1.0,
                north_lat_deg: 1.0,
            },
        }
    }

    #[test]
    fn chunk_id_is_direct_and_reversible() {
        let manifest = manifest();
        let index = ChunkIndex::new(&manifest, ChunkIndex::empty_records(&manifest)).unwrap();
        let chunk_id = index.chunk_id(0, 2, 1, 1, 2).unwrap();
        let extent = index.chunk_extent(chunk_id).unwrap();
        assert_eq!(extent.var_index, 0);
        assert_eq!(extent.t0, 2);
        assert_eq!(extent.z0, 4);
        assert_eq!(extent.y0, 4);
        assert_eq!(extent.x0, 8);
        assert_eq!(extent.nt, 1);
        assert_eq!(extent.nz, 1);
        assert_eq!(extent.ny, 3);
        assert_eq!(extent.nx, 1);
    }

    #[test]
    fn fixed_width_index_records_round_trip() {
        let records = vec![ChunkIndexRecord::empty(), ChunkIndexRecord::constant(42.0)];
        let mut bytes = Vec::new();
        write_index_records(&mut bytes, &records).unwrap();
        assert_eq!(bytes.len(), CHUNK_INDEX_RECORD_BYTES * records.len());
        let round_trip = read_index_records(bytes.as_slice()).unwrap();
        assert_eq!(round_trip[0].flags, records[0].flags);
        assert!(round_trip[0].center.is_nan());
        assert_eq!(round_trip[1], records[1]);
    }
}
