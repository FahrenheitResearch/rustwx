use super::chunk_payload::ChunkPayload;
use super::codec::ChunkCodec;
use super::index::{ChunkExtent, ChunkIndex, read_index_records};
use super::manifest::VolumeManifest;
use super::sampling::{
    PointProfile, PointSample, RouteDef, RouteSample, RouteSectionPrimitives, RouteValue,
    haversine_km, route_unit_components,
};
use super::{VolumeResult, VolumeStoreError};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

type DecodeCache = HashMap<usize, super::DecodedChunk>;

#[derive(Debug, Clone)]
pub struct VolumeStore {
    manifest: VolumeManifest,
    index: ChunkIndex,
    payload: ChunkPayload,
    codec: ChunkCodec,
}

impl VolumeStore {
    pub fn open(root: &Path) -> VolumeResult<Self> {
        let manifest_bytes = fs::read(root.join("manifest.json"))?;
        let manifest: VolumeManifest = serde_json::from_slice(&manifest_bytes)?;
        manifest.validate()?;
        let codec = ChunkCodec::from_name(&manifest.codec)?;
        let records = read_index_records(fs::File::open(root.join("index.bin"))?)?;
        let index = ChunkIndex::new(&manifest, records)?;
        let payload = ChunkPayload::open(&root.join("chunks.bin"))?;
        Ok(Self {
            manifest,
            index,
            payload,
            codec,
        })
    }

    pub fn manifest(&self) -> &VolumeManifest {
        &self.manifest
    }

    pub fn read_tile_f32(
        &self,
        variable: &str,
        forecast_hour: u8,
        level_hpa: u16,
        y_tile: usize,
        x_tile: usize,
        dst: &mut Vec<f32>,
    ) -> VolumeResult<ChunkExtent> {
        let var_index = self.manifest.variable_index(variable)?;
        let hour_index = self.manifest.hour_index(forecast_hour)?;
        let level_index = self.manifest.level_index(level_hpa)?;
        let t_block = hour_index / self.manifest.chunk_shape.t;
        let z_block = level_index / self.manifest.chunk_shape.z;
        let chunk_id = self
            .index
            .chunk_id(var_index, t_block, z_block, y_tile, x_tile)?;
        let extent = self.index.chunk_extent(chunk_id)?;
        let decoded = self.decode_chunk(chunk_id, extent)?;
        let local_t = hour_index - extent.t0;
        let local_z = level_index - extent.z0;
        dst.clear();
        dst.reserve(extent.ny * extent.nx);
        for local_y in 0..extent.ny {
            for local_x in 0..extent.nx {
                let idx = extent.linear_index(local_t, local_z, local_y, local_x);
                dst.push(decoded.values[idx]);
            }
        }
        Ok(extent)
    }

    pub fn sample_point_3d(
        &self,
        lat_deg: f64,
        lon_deg: f64,
        variables: &[&str],
        forecast_hours: &[u8],
        levels_hpa: &[u16],
    ) -> VolumeResult<PointProfile> {
        let (grid_x, grid_y) = self.manifest.grid.grid_xy(lat_deg, lon_deg)?;
        let mut cache = DecodeCache::new();
        let mut samples =
            Vec::with_capacity(variables.len() * forecast_hours.len() * levels_hpa.len());
        for variable in variables {
            for hour in forecast_hours {
                for level in levels_hpa {
                    samples.push(PointSample {
                        variable: (*variable).to_string(),
                        forecast_hour: *hour,
                        level_hpa: *level,
                        value: self.sample_grid_point_cached(
                            variable, *hour, *level, grid_x, grid_y, &mut cache,
                        )?,
                    });
                }
            }
        }
        Ok(PointProfile {
            lat_deg,
            lon_deg,
            samples,
        })
    }

    pub fn sample_route_3d(
        &self,
        route: &RouteDef,
        variables: &[&str],
        forecast_hour: u8,
        levels_hpa: &[u16],
    ) -> VolumeResult<RouteSectionPrimitives> {
        let route_samples = self.precompute_route(route)?;
        let mut cache = DecodeCache::new();
        let mut values =
            Vec::with_capacity(route_samples.len() * variables.len() * levels_hpa.len());
        for (sample_index, sample) in route_samples.iter().enumerate() {
            for variable in variables {
                for level in levels_hpa {
                    values.push(RouteValue {
                        sample_index,
                        variable: (*variable).to_string(),
                        forecast_hour,
                        level_hpa: *level,
                        value: self.sample_grid_point_cached(
                            variable,
                            forecast_hour,
                            *level,
                            sample.grid_x,
                            sample.grid_y,
                            &mut cache,
                        )?,
                    });
                }
            }
        }
        Ok(RouteSectionPrimitives {
            route_id: route.id.clone(),
            route_name: route.name.clone(),
            forecast_hour,
            route_samples,
            values,
        })
    }

    fn decode_chunk(
        &self,
        chunk_id: usize,
        extent: ChunkExtent,
    ) -> VolumeResult<super::DecodedChunk> {
        let record = self.index.record(chunk_id)?;
        let payload = self
            .payload
            .bytes(chunk_id, record.offset, record.compressed_len)?;
        self.codec
            .decode(record, payload.as_ref(), extent.value_count())
    }

    fn sample_grid_point_cached(
        &self,
        variable: &str,
        forecast_hour: u8,
        level_hpa: u16,
        grid_x: f32,
        grid_y: f32,
        cache: &mut DecodeCache,
    ) -> VolumeResult<f32> {
        let nx = self.manifest.grid.nx();
        let ny = self.manifest.grid.ny();
        if grid_x < 0.0 || grid_y < 0.0 || grid_x > (nx - 1) as f32 || grid_y > (ny - 1) as f32 {
            return Err(VolumeStoreError::OutOfBounds(format!(
                "grid point ({grid_x:.3}, {grid_y:.3}) outside {nx}x{ny}"
            )));
        }
        let x0 = grid_x.floor().clamp(0.0, (nx - 1) as f32) as usize;
        let y0 = grid_y.floor().clamp(0.0, (ny - 1) as f32) as usize;
        let x1 = (x0 + 1).min(nx - 1);
        let y1 = (y0 + 1).min(ny - 1);
        let wx = grid_x - x0 as f32;
        let wy = grid_y - y0 as f32;
        let v00 =
            self.sample_grid_cell_cached(variable, forecast_hour, level_hpa, y0, x0, cache)?;
        let v10 =
            self.sample_grid_cell_cached(variable, forecast_hour, level_hpa, y0, x1, cache)?;
        let v01 =
            self.sample_grid_cell_cached(variable, forecast_hour, level_hpa, y1, x0, cache)?;
        let v11 =
            self.sample_grid_cell_cached(variable, forecast_hour, level_hpa, y1, x1, cache)?;
        Ok(bilinear(v00, v10, v01, v11, wx, wy))
    }

    fn sample_grid_cell_cached(
        &self,
        variable: &str,
        forecast_hour: u8,
        level_hpa: u16,
        y: usize,
        x: usize,
        cache: &mut DecodeCache,
    ) -> VolumeResult<f32> {
        let var_index = self.manifest.variable_index(variable)?;
        let hour_index = self.manifest.hour_index(forecast_hour)?;
        let level_index = self.manifest.level_index(level_hpa)?;
        let (chunk_id, extent) =
            self.index
                .chunk_for_indices(var_index, hour_index, level_index, y, x)?;
        if !cache.contains_key(&chunk_id) {
            cache.insert(chunk_id, self.decode_chunk(chunk_id, extent)?);
        }
        let decoded = cache.get(&chunk_id).ok_or_else(|| {
            VolumeStoreError::InvalidChunk(format!("chunk {chunk_id} cache miss"))
        })?;
        let local_t = hour_index - extent.t0;
        let local_z = level_index - extent.z0;
        let local_y = y - extent.y0;
        let local_x = x - extent.x0;
        Ok(decoded.values[extent.linear_index(local_t, local_z, local_y, local_x)])
    }

    fn precompute_route(&self, route: &RouteDef) -> VolumeResult<Vec<RouteSample>> {
        if route.points.len() < 2 {
            return Err(VolumeStoreError::InvalidManifest(
                "route must have at least two points".to_string(),
            ));
        }
        let spacing_km = f64::from(route.sample_spacing_km.max(0.1));
        let mut samples = Vec::new();
        let mut cumulative_km = 0.0;
        for segment in route.points.windows(2) {
            let start = segment[0];
            let end = segment[1];
            let length_km = haversine_km(start, end).max(0.0);
            let steps = (length_km / spacing_km).ceil().max(1.0) as usize;
            let (route_unit_u, route_unit_v) = route_unit_components(start, end);
            for step in 0..=steps {
                if !samples.is_empty() && step == 0 {
                    continue;
                }
                let fraction = step as f64 / steps as f64;
                let (lat, lon) = self
                    .manifest
                    .grid
                    .lat_lon_for_fraction(fraction, start, end);
                let (grid_x, grid_y) = self.manifest.grid.grid_xy(lat, lon)?;
                let x0 = grid_x.floor() as usize;
                let y0 = grid_y.floor() as usize;
                samples.push(RouteSample {
                    distance_km: (cumulative_km + length_km * fraction) as f32,
                    lat_deg: lat,
                    lon_deg: lon,
                    grid_x,
                    grid_y,
                    x0,
                    y0,
                    wx: grid_x - x0 as f32,
                    wy: grid_y - y0 as f32,
                    route_unit_u,
                    route_unit_v,
                });
            }
            cumulative_km += length_km;
        }
        Ok(samples)
    }
}

fn bilinear(v00: f32, v10: f32, v01: f32, v11: f32, wx: f32, wy: f32) -> f32 {
    if !(v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite()) {
        return f32::NAN;
    }
    let top = v00 * (1.0 - wx) + v10 * wx;
    let bottom = v01 * (1.0 - wx) + v11 * wx;
    top * (1.0 - wy) + bottom * wy
}
