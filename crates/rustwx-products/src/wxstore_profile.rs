use crate::gridded::{PressureFields, SurfaceFields};
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;

const FORMAT: &str = "rustwx-wx-profile-store-v0";
const MANIFEST_FILE: &str = "manifest.json";
const WXP_MAGIC: &[u8; 8] = b"ORWXWXP0";
const WXP_VERSION: u32 = 1;
const WXP_HEADER_LEN: usize = 64;
const WXP_INDEX_RECORD_LEN: usize = 16;
const MISSING_I16: i16 = i16::MIN;

#[derive(Debug, Clone)]
pub struct WxProfileTimestep<'a> {
    pub forecast_hour: u16,
    pub pressure: &'a PressureFields,
    pub surface: &'a SurfaceFields,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxProfileManifest {
    pub format: String,
    pub model: String,
    pub domain: String,
    pub product: String,
    pub cycle: String,
    pub run_id: String,
    pub forecast_hours: Vec<u16>,
    pub variables: Vec<WxProfileVariable>,
    pub levels_hpa: Vec<u16>,
    pub nx: usize,
    pub ny: usize,
    pub dimensions: Vec<String>,
    pub chunk_y: usize,
    pub chunk_x: usize,
    pub chunk_levels: usize,
    pub chunk_hours: usize,
    pub compression: String,
    pub surface: WxSurfaceManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxProfileVariable {
    pub name: String,
    pub label: String,
    pub units: String,
    pub path: String,
    pub codec: String,
    pub scale_factor: f32,
    pub add_offset: f32,
    pub missing: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxSurfaceManifest {
    pub path_prefix: String,
    pub variables: Vec<WxSurfaceVariable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxSurfaceVariable {
    pub name: String,
    pub units: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BuildWxProfileReport {
    pub out_dir: PathBuf,
    pub model: String,
    pub domain: String,
    pub cycle: String,
    pub run_id: String,
    pub variables: Vec<String>,
    pub levels_hpa: Vec<u16>,
    pub forecast_hours: Vec<u16>,
    pub files_written: usize,
    pub bytes_written: u64,
    pub chunk_x: usize,
    pub chunk_y: usize,
    pub chunk_count_per_variable: usize,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct WxProfileGridPoint {
    pub x: usize,
    pub y: usize,
    pub index: usize,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct WxSurfacePoint {
    pub psfc_hpa: f64,
    pub orog_m: f64,
    pub t2_c: f64,
    pub q2_kgkg: f64,
    pub u10_ms: f64,
    pub v10_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WxProfileBoxSummary {
    pub requested_lat: f64,
    pub requested_lon: f64,
    pub radius_lat_deg: f64,
    pub radius_lon_deg: f64,
    pub point_count: usize,
    pub mean_lat: f64,
    pub mean_lon: f64,
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
    pub min_x: usize,
    pub max_x: usize,
    pub min_y: usize,
    pub max_y: usize,
}

#[derive(Debug, Clone)]
pub struct WxProfileBoxMean {
    pub summary: WxProfileBoxSummary,
    pub surface: WxSurfacePoint,
    pub variables: HashMap<String, Vec<Option<f64>>>,
}

pub struct WxProfileStore {
    root: PathBuf,
    manifest: WxProfileManifest,
    file_cache: RwLock<HashMap<String, Arc<WxProfileFile>>>,
    surface_cache: RwLock<HashMap<String, Arc<Mmap>>>,
}

#[derive(Debug)]
struct WxProfileFile {
    mmap: Mmap,
    header: WxpHeader,
    index: Vec<WxpIndexRecord>,
}

#[derive(Debug, Clone, Copy)]
struct WxpHeader {
    nx: usize,
    ny: usize,
    levels_len: usize,
    hours_len: usize,
    chunk_x: usize,
    scale_factor: f32,
    add_offset: f32,
    chunk_count: usize,
    index_offset: usize,
    data_offset: usize,
}

#[derive(Debug, Clone, Copy)]
struct WxpIndexRecord {
    offset: usize,
    len: usize,
    x_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct BoxCandidate {
    index: usize,
    x: usize,
    y: usize,
}

#[derive(Debug, Clone, Copy)]
struct WxpChunkSpec {
    y0: usize,
    y_count: usize,
    x0: usize,
    x_count: usize,
}

struct EncodedWxpChunk {
    x_count: usize,
    compressed: Vec<u8>,
}

pub fn write_wx_profile_store_from_timesteps(
    root: &Path,
    model: impl Into<String>,
    domain: impl Into<String>,
    cycle: impl Into<String>,
    run_id: impl Into<String>,
    timesteps: &[WxProfileTimestep<'_>],
    chunk_x: usize,
    chunk_y: usize,
    include_vvel: bool,
) -> Result<BuildWxProfileReport, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let first = timesteps
        .first()
        .ok_or("at least one profile timestep is required")?;
    let nx = first.surface.nx;
    let ny = first.surface.ny;
    let grid_len = nx * ny;
    let levels_hpa = levels_from_pressure(first.pressure)?;
    let forecast_hours = timesteps
        .iter()
        .map(|timestep| timestep.forecast_hour)
        .collect::<Vec<_>>();
    for timestep in timesteps {
        validate_timestep(timestep, nx, ny, grid_len, &levels_hpa)?;
    }

    fs::create_dir_all(root)?;
    let chunk_x = chunk_x.clamp(1, nx.max(1));
    let chunk_y = chunk_y.clamp(1, ny.max(1));
    let variables = profile_variables_for_pressure(first.pressure, include_vvel);
    let model = model.into();
    let domain = domain.into();
    let cycle = cycle.into();
    let run_id = run_id.into();
    let manifest = WxProfileManifest {
        format: FORMAT.to_string(),
        model: model.clone(),
        domain: domain.clone(),
        product: "wx_profile_point_temporal".to_string(),
        cycle: cycle.clone(),
        run_id: run_id.clone(),
        forecast_hours: forecast_hours.clone(),
        variables: variables.clone(),
        levels_hpa: levels_hpa.clone(),
        nx,
        ny,
        dimensions: ["y", "x_chunk", "pressure_hpa", "forecast_hour"]
            .iter()
            .map(|value| value.to_string())
            .collect(),
        chunk_y,
        chunk_x,
        chunk_levels: levels_hpa.len(),
        chunk_hours: forecast_hours.len(),
        compression: "zstd(level=1)+i16(variable-specific-scale)".to_string(),
        surface: WxSurfaceManifest {
            path_prefix: "surface_wx".to_string(),
            variables: surface_variables(),
        },
    };

    if let Err(err) = variables.par_iter().try_for_each(|variable| {
        write_variable_wxp_file(
            root,
            variable,
            &levels_hpa,
            timesteps,
            nx,
            ny,
            chunk_x,
            chunk_y,
        )
        .map_err(|err| err.to_string())
    }) {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, err).into());
    }
    write_surface_files(root, &manifest.surface, timesteps, nx, ny)?;
    write_manifest(root, &manifest)?;

    let bytes_written = variables
        .iter()
        .map(|variable| {
            fs::metadata(root.join(&variable.path))
                .map(|meta| meta.len())
                .unwrap_or(0)
        })
        .sum::<u64>()
        + manifest
            .surface
            .variables
            .iter()
            .map(|variable| {
                fs::metadata(root.join(&variable.path))
                    .map(|meta| meta.len())
                    .unwrap_or(0)
            })
            .sum::<u64>();
    Ok(BuildWxProfileReport {
        out_dir: root.to_path_buf(),
        model,
        domain,
        cycle,
        run_id,
        variables: variables
            .iter()
            .map(|variable| variable.name.clone())
            .collect(),
        levels_hpa,
        forecast_hours,
        files_written: variables.len() + manifest.surface.variables.len() + 1,
        bytes_written,
        chunk_x,
        chunk_y,
        chunk_count_per_variable: ny.div_ceil(chunk_y) * nx.div_ceil(chunk_x),
        elapsed_ms: started.elapsed().as_millis(),
    })
}

impl WxProfileStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let root = root.as_ref().to_path_buf();
        let manifest: WxProfileManifest =
            serde_json::from_slice(&fs::read(root.join(MANIFEST_FILE))?)?;
        if manifest.format != FORMAT {
            return Err(format!("unsupported wx profile format '{}'", manifest.format).into());
        }
        if manifest.nx == 0 || manifest.ny == 0 {
            return Err("wx profile manifest has empty grid".into());
        }
        Ok(Self {
            root,
            manifest,
            file_cache: RwLock::new(HashMap::new()),
            surface_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn manifest(&self) -> &WxProfileManifest {
        &self.manifest
    }

    pub fn variable_names(&self) -> Vec<String> {
        self.manifest
            .variables
            .iter()
            .map(|variable| variable.name.clone())
            .collect()
    }

    pub fn locate_nearest_grid_point(
        &self,
        lat: f64,
        lon: f64,
    ) -> Result<WxProfileGridPoint, Box<dyn std::error::Error>> {
        if !lat.is_finite() || !lon.is_finite() {
            return Err("lat/lon must be finite".into());
        }
        let lats = self.surface_mmap("LAT")?;
        let lons = self.surface_mmap("LON")?;
        let nxy = self.manifest.nx * self.manifest.ny;
        let mut best_index = 0usize;
        let mut best_score = f64::INFINITY;
        for index in 0..nxy {
            let plat = f32_at(&lats, index) as f64;
            let plon = f32_at(&lons, index) as f64;
            if !plat.is_finite() || !plon.is_finite() {
                continue;
            }
            let dlat = plat - lat;
            let dlon = normalized_lon_delta(plon - lon);
            let score = dlat * dlat + dlon * dlon;
            if score < best_score {
                best_score = score;
                best_index = index;
            }
        }
        Ok(WxProfileGridPoint {
            x: best_index % self.manifest.nx,
            y: best_index / self.manifest.nx,
            index: best_index,
            lat: f32_at(&lats, best_index) as f64,
            lon: normalize_lon(f32_at(&lons, best_index) as f64),
        })
    }

    pub fn read_variable_point(
        &self,
        variable: &str,
        forecast_hour: u16,
        point: &WxProfileGridPoint,
    ) -> Result<Vec<Option<f64>>, Box<dyn std::error::Error>> {
        let hour_index = self
            .manifest
            .forecast_hours
            .iter()
            .position(|hour| *hour == forecast_hour)
            .ok_or_else(|| format!("forecast hour f{forecast_hour:03} is not available"))?;
        let file = self.file_for_variable(variable)?;
        let chunk_y = self.manifest.chunk_y.max(1);
        let chunks_per_row = self.manifest.nx.div_ceil(file.header.chunk_x);
        let chunk_row = point.y / chunk_y;
        let chunk_id = chunk_row * chunks_per_row + point.x / file.header.chunk_x;
        let record = *file
            .index
            .get(chunk_id)
            .ok_or_else(|| format!("wx chunk {chunk_id} missing for variable '{variable}'"))?;
        let local_x = point.x % file.header.chunk_x;
        let local_y = point.y % chunk_y;
        let y_count = chunk_y_count(chunk_row, chunk_y, self.manifest.ny);
        if local_x >= record.x_count {
            return Err(
                format!("local x {local_x} outside chunk x_count {}", record.x_count).into(),
            );
        }
        let decoded = decode_wxp_chunk(&file, record, variable, chunk_id, y_count)?;
        let mut values = Vec::with_capacity(file.header.levels_len);
        for level_index in 0..file.header.levels_len {
            let value_index = (((local_y * record.x_count + local_x) * file.header.levels_len
                + level_index)
                * file.header.hours_len)
                + hour_index;
            let byte_offset = value_index * 2;
            let q = i16::from_le_bytes([decoded[byte_offset], decoded[byte_offset + 1]]);
            if q == MISSING_I16 {
                values.push(None);
            } else {
                values.push(Some(
                    f64::from(q) / f64::from(file.header.scale_factor)
                        - f64::from(file.header.add_offset),
                ));
            }
        }
        Ok(values)
    }

    pub fn read_surface_point(
        &self,
        forecast_hour: u16,
        point: &WxProfileGridPoint,
    ) -> Result<WxSurfacePoint, Box<dyn std::error::Error>> {
        let hour_index = self
            .manifest
            .forecast_hours
            .iter()
            .position(|hour| *hour == forecast_hour)
            .ok_or_else(|| format!("forecast hour f{forecast_hour:03} is not available"))?;
        let nxy = self.manifest.nx * self.manifest.ny;
        let offset = hour_index * nxy + point.index;
        Ok(WxSurfacePoint {
            psfc_hpa: f32_at(&self.surface_mmap("PSFC")?, offset) as f64 / 100.0,
            orog_m: f32_at(&self.surface_mmap("OROG")?, offset) as f64,
            t2_c: f32_at(&self.surface_mmap("T2")?, offset) as f64 - 273.15,
            q2_kgkg: f32_at(&self.surface_mmap("Q2")?, offset) as f64,
            u10_ms: f32_at(&self.surface_mmap("U10")?, offset) as f64,
            v10_ms: f32_at(&self.surface_mmap("V10")?, offset) as f64,
        })
    }

    pub fn read_box_mean(
        &self,
        forecast_hour: u16,
        lat: f64,
        lon: f64,
        radius_lat_deg: f64,
        radius_lon_deg: f64,
        variables: &[&str],
    ) -> Result<WxProfileBoxMean, Box<dyn std::error::Error>> {
        if !lat.is_finite() || !lon.is_finite() {
            return Err("lat/lon must be finite".into());
        }
        let radius_lat_deg = radius_lat_deg.abs().max(0.0001);
        let radius_lon_deg = radius_lon_deg.abs().max(0.0001);
        let hour_index = self
            .manifest
            .forecast_hours
            .iter()
            .position(|hour| *hour == forecast_hour)
            .ok_or_else(|| format!("forecast hour f{forecast_hour:03} is not available"))?;
        let candidates = self.box_candidates(lat, lon, radius_lat_deg, radius_lon_deg)?;
        if candidates.is_empty() {
            return Err("box does not include any profile grid points".into());
        }
        let summary = self.box_summary(lat, lon, radius_lat_deg, radius_lon_deg, &candidates)?;
        let surface = self.mean_surface_point(hour_index, &candidates)?;
        let mut means = HashMap::new();
        for variable in variables {
            means.insert(
                (*variable).to_string(),
                self.mean_variable_box(variable, hour_index, &candidates)?,
            );
        }
        Ok(WxProfileBoxMean {
            summary,
            surface,
            variables: means,
        })
    }

    fn file_for_variable(
        &self,
        variable: &str,
    ) -> Result<Arc<WxProfileFile>, Box<dyn std::error::Error>> {
        {
            let cache = self
                .file_cache
                .read()
                .map_err(|_| "profile cache poisoned")?;
            if let Some(file) = cache.get(variable) {
                return Ok(file.clone());
            }
        }
        let descriptor = self
            .manifest
            .variables
            .iter()
            .find(|candidate| candidate.name == variable)
            .ok_or_else(|| format!("wx profile variable '{variable}' is not available"))?;
        let path = self.root.join(&descriptor.path);
        let file = File::open(&path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let header = parse_wxp_header(&mmap)?;
        let index = parse_wxp_index(&mmap, header)?;
        let file = Arc::new(WxProfileFile {
            mmap,
            header,
            index,
        });
        self.file_cache
            .write()
            .map_err(|_| "profile cache poisoned")?
            .insert(variable.to_string(), file.clone());
        Ok(file)
    }

    fn surface_mmap(&self, variable: &str) -> Result<Arc<Mmap>, Box<dyn std::error::Error>> {
        {
            let cache = self
                .surface_cache
                .read()
                .map_err(|_| "surface cache poisoned")?;
            if let Some(mmap) = cache.get(variable) {
                return Ok(mmap.clone());
            }
        }
        let descriptor = self
            .manifest
            .surface
            .variables
            .iter()
            .find(|candidate| candidate.name == variable)
            .ok_or_else(|| format!("surface variable '{variable}' is not available"))?;
        let file = File::open(self.root.join(&descriptor.path))?;
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file)? });
        self.surface_cache
            .write()
            .map_err(|_| "surface cache poisoned")?
            .insert(variable.to_string(), mmap.clone());
        Ok(mmap)
    }

    fn box_candidates(
        &self,
        lat: f64,
        lon: f64,
        radius_lat_deg: f64,
        radius_lon_deg: f64,
    ) -> Result<Vec<BoxCandidate>, Box<dyn std::error::Error>> {
        let lats = self.surface_mmap("LAT")?;
        let lons = self.surface_mmap("LON")?;
        let nxy = self.manifest.nx * self.manifest.ny;
        let min_lat = lat - radius_lat_deg;
        let max_lat = lat + radius_lat_deg;
        let lon = normalize_lon(lon);
        let mut candidates = Vec::new();
        for index in 0..nxy {
            let plat = f32_at(&lats, index) as f64;
            let plon = normalize_lon(f32_at(&lons, index) as f64);
            if !plat.is_finite() || !plon.is_finite() {
                continue;
            }
            if plat < min_lat || plat > max_lat {
                continue;
            }
            if normalized_lon_delta(plon - lon).abs() > radius_lon_deg {
                continue;
            }
            candidates.push(BoxCandidate {
                index,
                x: index % self.manifest.nx,
                y: index / self.manifest.nx,
            });
        }
        Ok(candidates)
    }

    fn box_summary(
        &self,
        lat: f64,
        lon: f64,
        radius_lat_deg: f64,
        radius_lon_deg: f64,
        candidates: &[BoxCandidate],
    ) -> Result<WxProfileBoxSummary, Box<dyn std::error::Error>> {
        let lats = self.surface_mmap("LAT")?;
        let lons = self.surface_mmap("LON")?;
        let center_lon = normalize_lon(lon);
        let mut sum_lat = 0.0;
        let mut sum_lon_delta = 0.0;
        let mut min_lat = f64::INFINITY;
        let mut max_lat = f64::NEG_INFINITY;
        let mut min_delta = f64::INFINITY;
        let mut max_delta = f64::NEG_INFINITY;
        let mut min_x = usize::MAX;
        let mut max_x = 0usize;
        let mut min_y = usize::MAX;
        let mut max_y = 0usize;
        for candidate in candidates {
            let plat = f32_at(&lats, candidate.index) as f64;
            let plon = normalize_lon(f32_at(&lons, candidate.index) as f64);
            let dlon = normalized_lon_delta(plon - center_lon);
            sum_lat += plat;
            sum_lon_delta += dlon;
            min_lat = min_lat.min(plat);
            max_lat = max_lat.max(plat);
            min_delta = min_delta.min(dlon);
            max_delta = max_delta.max(dlon);
            min_x = min_x.min(candidate.x);
            max_x = max_x.max(candidate.x);
            min_y = min_y.min(candidate.y);
            max_y = max_y.max(candidate.y);
        }
        let count = candidates.len() as f64;
        Ok(WxProfileBoxSummary {
            requested_lat: lat,
            requested_lon: center_lon,
            radius_lat_deg,
            radius_lon_deg,
            point_count: candidates.len(),
            mean_lat: sum_lat / count,
            mean_lon: normalize_lon(center_lon + sum_lon_delta / count),
            min_lat,
            max_lat,
            min_lon: normalize_lon(center_lon + min_delta),
            max_lon: normalize_lon(center_lon + max_delta),
            min_x,
            max_x,
            min_y,
            max_y,
        })
    }

    fn mean_surface_point(
        &self,
        hour_index: usize,
        candidates: &[BoxCandidate],
    ) -> Result<WxSurfacePoint, Box<dyn std::error::Error>> {
        let nxy = self.manifest.nx * self.manifest.ny;
        Ok(WxSurfacePoint {
            psfc_hpa: self.mean_surface_variable("PSFC", hour_index, nxy, candidates)? / 100.0,
            orog_m: self.mean_surface_variable("OROG", hour_index, nxy, candidates)?,
            t2_c: self.mean_surface_variable("T2", hour_index, nxy, candidates)? - 273.15,
            q2_kgkg: self.mean_surface_variable("Q2", hour_index, nxy, candidates)?,
            u10_ms: self.mean_surface_variable("U10", hour_index, nxy, candidates)?,
            v10_ms: self.mean_surface_variable("V10", hour_index, nxy, candidates)?,
        })
    }

    fn mean_surface_variable(
        &self,
        variable: &str,
        hour_index: usize,
        nxy: usize,
        candidates: &[BoxCandidate],
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mmap = self.surface_mmap(variable)?;
        let mut sum = 0.0;
        let mut count = 0usize;
        let base = hour_index * nxy;
        for candidate in candidates {
            let value = f32_at(&mmap, base + candidate.index) as f64;
            if value.is_finite() {
                sum += value;
                count += 1;
            }
        }
        if count == 0 {
            return Err(format!("no finite {variable} surface values in box").into());
        }
        Ok(sum / count as f64)
    }

    fn mean_variable_box(
        &self,
        variable: &str,
        hour_index: usize,
        candidates: &[BoxCandidate],
    ) -> Result<Vec<Option<f64>>, Box<dyn std::error::Error>> {
        let file = self.file_for_variable(variable)?;
        if file.header.nx != self.manifest.nx || file.header.ny != self.manifest.ny {
            return Err(
                format!("wx profile variable '{variable}' grid differs from manifest").into(),
            );
        }
        if hour_index >= file.header.hours_len {
            return Err(format!("hour index {hour_index} outside variable '{variable}'").into());
        }
        let chunk_y = self.manifest.chunk_y.max(1);
        let chunks_per_row = self.manifest.nx.div_ceil(file.header.chunk_x);
        let mut chunk_points = vec![Vec::<(usize, usize)>::new(); file.header.chunk_count];
        for candidate in candidates {
            let chunk_row = candidate.y / chunk_y;
            let chunk_id = chunk_row * chunks_per_row + candidate.x / file.header.chunk_x;
            if let Some(local_points) = chunk_points.get_mut(chunk_id) {
                local_points.push((candidate.y % chunk_y, candidate.x % file.header.chunk_x));
            }
        }
        let mut sums = vec![0.0; file.header.levels_len];
        let mut counts = vec![0usize; file.header.levels_len];
        for (chunk_id, local_points) in chunk_points.iter().enumerate() {
            if local_points.is_empty() {
                continue;
            }
            let record = *file
                .index
                .get(chunk_id)
                .ok_or_else(|| format!("wx chunk {chunk_id} missing for variable '{variable}'"))?;
            let chunk_row = chunk_id / chunks_per_row;
            let y_count = chunk_y_count(chunk_row, chunk_y, self.manifest.ny);
            let decoded = decode_wxp_chunk(&file, record, variable, chunk_id, y_count)?;
            for (local_y, local_x) in local_points {
                if *local_x >= record.x_count {
                    continue;
                }
                for level_index in 0..file.header.levels_len {
                    let value_index = (((*local_y * record.x_count + *local_x)
                        * file.header.levels_len
                        + level_index)
                        * file.header.hours_len)
                        + hour_index;
                    let byte_offset = value_index * 2;
                    let q = i16::from_le_bytes([decoded[byte_offset], decoded[byte_offset + 1]]);
                    if let Some(value) = decode_quantized_value(q, file.header) {
                        sums[level_index] += value;
                        counts[level_index] += 1;
                    }
                }
            }
        }
        Ok(sums
            .into_iter()
            .zip(counts)
            .map(|(sum, count)| (count > 0).then_some(sum / count as f64))
            .collect())
    }
}

fn write_variable_wxp_file(
    root: &Path,
    variable: &WxProfileVariable,
    levels_hpa: &[u16],
    timesteps: &[WxProfileTimestep<'_>],
    nx: usize,
    ny: usize,
    chunk_x: usize,
    chunk_y: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = root.join(&variable.path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("wxp.tmp");
    let _ = fs::remove_file(&tmp_path);
    let mut file = File::create(&tmp_path)?;
    let chunks_per_row = nx.div_ceil(chunk_x);
    let chunk_count = ny.div_ceil(chunk_y) * chunks_per_row;
    let placeholder = WxpHeader {
        nx,
        ny,
        levels_len: levels_hpa.len(),
        hours_len: timesteps.len(),
        chunk_x,
        scale_factor: variable.scale_factor,
        add_offset: variable.add_offset,
        chunk_count,
        index_offset: 0,
        data_offset: WXP_HEADER_LEN,
    };
    write_wxp_header(&mut file, placeholder)?;
    file.seek(SeekFrom::Start(WXP_HEADER_LEN as u64))?;

    let sources = timesteps
        .iter()
        .map(|timestep| pressure_values_for_variable(timestep.pressure, &variable.name))
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_specs = wxp_chunk_specs(nx, ny, chunk_x, chunk_y);
    let encoded_chunks = chunk_specs
        .par_iter()
        .map(|spec| {
            encode_wxp_chunk(
                &sources,
                levels_hpa.len(),
                variable.scale_factor,
                variable.add_offset,
                nx,
                ny,
                *spec,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;

    let mut index = Vec::with_capacity(chunk_count);
    for encoded in encoded_chunks {
        let offset = file.stream_position()? as usize;
        file.write_all(&encoded.compressed)?;
        index.push(WxpIndexRecord {
            offset,
            len: encoded.compressed.len(),
            x_count: encoded.x_count,
        });
    }

    let index_offset = file.stream_position()? as usize;
    for record in &index {
        write_index_record(&mut file, *record)?;
    }
    file.seek(SeekFrom::Start(0))?;
    write_wxp_header(
        &mut file,
        WxpHeader {
            index_offset,
            ..placeholder
        },
    )?;
    file.flush()?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn wxp_chunk_specs(nx: usize, ny: usize, chunk_x: usize, chunk_y: usize) -> Vec<WxpChunkSpec> {
    let mut specs = Vec::with_capacity(nx.div_ceil(chunk_x) * ny.div_ceil(chunk_y));
    let mut y0 = 0usize;
    while y0 < ny {
        let y_count = (ny - y0).min(chunk_y);
        let mut x0 = 0usize;
        while x0 < nx {
            let x_count = (nx - x0).min(chunk_x);
            specs.push(WxpChunkSpec {
                y0,
                y_count,
                x0,
                x_count,
            });
            x0 += x_count;
        }
        y0 += y_count;
    }
    specs
}

fn encode_wxp_chunk(
    sources: &[&[f64]],
    levels_len: usize,
    scale_factor: f32,
    add_offset: f32,
    nx: usize,
    ny: usize,
    spec: WxpChunkSpec,
) -> Result<EncodedWxpChunk, String> {
    let hours_len = sources.len();
    let min_q = f32::from(MISSING_I16 + 1);
    let max_q = f32::from(i16::MAX);
    let mut quantized = vec![MISSING_I16; spec.y_count * spec.x_count * levels_len * hours_len];
    for (hour_index, source) in sources.iter().enumerate() {
        for local_y in 0..spec.y_count {
            let y = spec.y0 + local_y;
            for level_index in 0..levels_len {
                let row_offset = level_index * nx * ny + y * nx + spec.x0;
                for local_x in 0..spec.x_count {
                    let dst = (((local_y * spec.x_count + local_x) * levels_len + level_index)
                        * hours_len)
                        + hour_index;
                    quantized[dst] = quantize_value_scaled(
                        source[row_offset + local_x] as f32,
                        scale_factor,
                        add_offset,
                        min_q,
                        max_q,
                    );
                }
            }
        }
    }
    let mut bytes = Vec::with_capacity(quantized.len() * 2);
    for value in &quantized {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    let compressed =
        zstd::stream::encode_all(bytes.as_slice(), 1).map_err(|err| err.to_string())?;
    Ok(EncodedWxpChunk {
        x_count: spec.x_count,
        compressed,
    })
}

fn write_surface_files(
    root: &Path,
    manifest: &WxSurfaceManifest,
    timesteps: &[WxProfileTimestep<'_>],
    nx: usize,
    ny: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(root.join(&manifest.path_prefix))?;
    let nxy = nx * ny;
    for variable in &manifest.variables {
        let path = root.join(&variable.path);
        let mut file = File::create(path)?;
        if variable.name == "LAT" || variable.name == "LON" {
            let source = if variable.name == "LAT" {
                &timesteps[0].surface.lat
            } else {
                &timesteps[0].surface.lon
            };
            if source.len() != nxy {
                return Err(format!("surface {} length does not match grid", variable.name).into());
            }
            for value in source {
                file.write_all(&(*value as f32).to_le_bytes())?;
            }
            continue;
        }
        for timestep in timesteps {
            let values = surface_values_for_variable(timestep.surface, &variable.name)?;
            if values.len() != nxy {
                return Err(format!("surface {} length does not match grid", variable.name).into());
            }
            for value in values {
                file.write_all(&(*value as f32).to_le_bytes())?;
            }
        }
    }
    Ok(())
}

fn write_manifest(
    root: &Path,
    manifest: &WxProfileManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = root.join(MANIFEST_FILE);
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_vec_pretty(manifest)?)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn validate_timestep(
    timestep: &WxProfileTimestep<'_>,
    nx: usize,
    ny: usize,
    grid_len: usize,
    levels_hpa: &[u16],
) -> Result<(), Box<dyn std::error::Error>> {
    if timestep.surface.nx != nx || timestep.surface.ny != ny {
        return Err(format!("f{:03} surface grid shape differs", timestep.forecast_hour).into());
    }
    let timestep_levels = levels_from_pressure(timestep.pressure)?;
    if timestep_levels != levels_hpa {
        return Err(format!("f{:03} pressure levels differ", timestep.forecast_hour).into());
    }
    let expected_3d = grid_len * levels_hpa.len();
    for (name, values) in [
        ("TMP", &timestep.pressure.temperature_c_3d),
        ("SPFH", &timestep.pressure.qvapor_kgkg_3d),
        ("UGRD", &timestep.pressure.u_ms_3d),
        ("VGRD", &timestep.pressure.v_ms_3d),
        ("HGT", &timestep.pressure.gh_m_3d),
    ] {
        if values.len() != expected_3d {
            return Err(format!("{name} length {} != {expected_3d}", values.len()).into());
        }
    }
    Ok(())
}

fn profile_variables_for_pressure(
    pressure: &PressureFields,
    include_vvel: bool,
) -> Vec<WxProfileVariable> {
    let mut names = vec!["TMP", "SPFH", "UGRD", "VGRD", "HGT"];
    if include_vvel && pressure.omega_pa_s_3d.is_some() {
        names.push("VVEL");
    }
    names.into_iter().map(profile_variable).collect()
}

fn profile_variable(name: &str) -> WxProfileVariable {
    let (label, units) = match name {
        "TMP" => ("Temperature", "degC"),
        "SPFH" => ("Specific humidity", "kg/kg"),
        "UGRD" => ("U wind", "m/s"),
        "VGRD" => ("V wind", "m/s"),
        "HGT" => ("Geopotential height", "m"),
        "VVEL" => ("Vertical velocity", "Pa/s"),
        _ => (name, "unknown"),
    };
    let encoding = wx_encoding_for_variable(name);
    WxProfileVariable {
        name: name.to_string(),
        label: label.to_string(),
        units: units.to_string(),
        path: format!("profile_wx/{name}.wxp"),
        codec: "zstd-i16-le-v0".to_string(),
        scale_factor: encoding.scale_factor,
        add_offset: encoding.add_offset,
        missing: MISSING_I16,
    }
}

fn surface_variables() -> Vec<WxSurfaceVariable> {
    [
        ("LAT", "deg", "surface_wx/LAT.f32"),
        ("LON", "deg", "surface_wx/LON.f32"),
        ("PSFC", "Pa", "surface_wx/PSFC.f32"),
        ("OROG", "m", "surface_wx/OROG.f32"),
        ("T2", "K", "surface_wx/T2.f32"),
        ("Q2", "kg/kg", "surface_wx/Q2.f32"),
        ("U10", "m/s", "surface_wx/U10.f32"),
        ("V10", "m/s", "surface_wx/V10.f32"),
    ]
    .into_iter()
    .map(|(name, units, path)| WxSurfaceVariable {
        name: name.to_string(),
        units: units.to_string(),
        path: path.to_string(),
    })
    .collect()
}

fn pressure_values_for_variable<'a>(
    pressure: &'a PressureFields,
    variable: &str,
) -> Result<&'a [f64], Box<dyn std::error::Error>> {
    match variable {
        "TMP" => Ok(&pressure.temperature_c_3d),
        "SPFH" => Ok(&pressure.qvapor_kgkg_3d),
        "UGRD" => Ok(&pressure.u_ms_3d),
        "VGRD" => Ok(&pressure.v_ms_3d),
        "HGT" => Ok(&pressure.gh_m_3d),
        "VVEL" => pressure
            .omega_pa_s_3d
            .as_deref()
            .ok_or_else(|| "VVEL is not available".into()),
        _ => Err(format!("unsupported pressure variable {variable}").into()),
    }
}

fn surface_values_for_variable<'a>(
    surface: &'a SurfaceFields,
    variable: &str,
) -> Result<&'a [f64], Box<dyn std::error::Error>> {
    match variable {
        "PSFC" => Ok(&surface.psfc_pa),
        "OROG" => Ok(&surface.orog_m),
        "T2" => Ok(&surface.t2_k),
        "Q2" => Ok(&surface.q2_kgkg),
        "U10" => Ok(&surface.u10_ms),
        "V10" => Ok(&surface.v10_ms),
        _ => Err(format!("unsupported surface variable {variable}").into()),
    }
}

fn levels_from_pressure(pressure: &PressureFields) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
    pressure
        .pressure_levels_hpa
        .iter()
        .map(|level| {
            if !level.is_finite() || *level < 0.0 || *level > f64::from(u16::MAX) {
                Err(format!("invalid pressure level {level}").into())
            } else {
                Ok(level.round() as u16)
            }
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct WxVariableEncoding {
    scale_factor: f32,
    add_offset: f32,
}

fn wx_encoding_for_variable(variable: &str) -> WxVariableEncoding {
    let scale_factor = match variable {
        "TMP" => 20.0,
        "SPFH" => 1_000_000.0,
        "UGRD" | "VGRD" => 10.0,
        "HGT" => 1.0,
        "VVEL" => 100.0,
        _ => 10.0,
    };
    WxVariableEncoding {
        scale_factor,
        add_offset: 0.0,
    }
}

#[inline(always)]
fn quantize_value_scaled(
    value: f32,
    scale_factor: f32,
    add_offset: f32,
    min_q: f32,
    max_q: f32,
) -> i16 {
    if !value.is_finite() {
        return MISSING_I16;
    }
    let scaled = ((value + add_offset) * scale_factor).round();
    if !scaled.is_finite() {
        return MISSING_I16;
    }
    scaled.clamp(min_q, max_q) as i16
}

fn write_wxp_header(file: &mut File, header: WxpHeader) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = [0u8; WXP_HEADER_LEN];
    bytes[0..8].copy_from_slice(WXP_MAGIC);
    bytes[8..12].copy_from_slice(&WXP_VERSION.to_le_bytes());
    bytes[12..16].copy_from_slice(&(header.nx as u32).to_le_bytes());
    bytes[16..20].copy_from_slice(&(header.ny as u32).to_le_bytes());
    bytes[20..24].copy_from_slice(&(header.levels_len as u32).to_le_bytes());
    bytes[24..28].copy_from_slice(&(header.hours_len as u32).to_le_bytes());
    bytes[28..32].copy_from_slice(&(header.chunk_x as u32).to_le_bytes());
    bytes[32..36].copy_from_slice(&header.scale_factor.to_le_bytes());
    bytes[36..40].copy_from_slice(&header.add_offset.to_le_bytes());
    bytes[40..48].copy_from_slice(&(header.chunk_count as u64).to_le_bytes());
    bytes[48..56].copy_from_slice(&(header.index_offset as u64).to_le_bytes());
    bytes[56..64].copy_from_slice(&(header.data_offset as u64).to_le_bytes());
    file.write_all(&bytes)?;
    Ok(())
}

fn write_index_record(
    file: &mut File,
    record: WxpIndexRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    file.write_all(&(record.offset as u64).to_le_bytes())?;
    file.write_all(&(record.len as u32).to_le_bytes())?;
    file.write_all(&(record.x_count as u32).to_le_bytes())?;
    Ok(())
}

fn parse_wxp_header(mmap: &[u8]) -> Result<WxpHeader, Box<dyn std::error::Error>> {
    if mmap.len() < WXP_HEADER_LEN {
        return Err("file too short for wxp header".into());
    }
    if &mmap[0..8] != WXP_MAGIC {
        return Err("bad wxp magic".into());
    }
    let version = u32_from(&mmap[8..12])?;
    if version != WXP_VERSION {
        return Err(format!("unsupported wxp version {version}").into());
    }
    let header = WxpHeader {
        nx: u32_from(&mmap[12..16])? as usize,
        ny: u32_from(&mmap[16..20])? as usize,
        levels_len: u32_from(&mmap[20..24])? as usize,
        hours_len: u32_from(&mmap[24..28])? as usize,
        chunk_x: u32_from(&mmap[28..32])? as usize,
        scale_factor: f32_from(&mmap[32..36])?,
        add_offset: f32_from(&mmap[36..40])?,
        chunk_count: u64_from(&mmap[40..48])? as usize,
        index_offset: u64_from(&mmap[48..56])? as usize,
        data_offset: u64_from(&mmap[56..64])? as usize,
    };
    if header.chunk_x == 0 || header.levels_len == 0 || header.hours_len == 0 {
        return Err("wxp header has empty chunk/axes".into());
    }
    if header.index_offset < WXP_HEADER_LEN || header.index_offset > mmap.len() {
        return Err("invalid wxp index offset".into());
    }
    Ok(header)
}

fn parse_wxp_index(
    mmap: &[u8],
    header: WxpHeader,
) -> Result<Vec<WxpIndexRecord>, Box<dyn std::error::Error>> {
    let bytes_len = header.chunk_count * WXP_INDEX_RECORD_LEN;
    let end = header.index_offset + bytes_len;
    if end > mmap.len() {
        return Err("wxp index exceeds file length".into());
    }
    let mut records = Vec::with_capacity(header.chunk_count);
    let mut offset = header.index_offset;
    for _ in 0..header.chunk_count {
        records.push(WxpIndexRecord {
            offset: u64_from(&mmap[offset..offset + 8])? as usize,
            len: u32_from(&mmap[offset + 8..offset + 12])? as usize,
            x_count: u32_from(&mmap[offset + 12..offset + 16])? as usize,
        });
        offset += WXP_INDEX_RECORD_LEN;
    }
    Ok(records)
}

fn decode_wxp_chunk(
    file: &WxProfileFile,
    record: WxpIndexRecord,
    variable: &str,
    chunk_id: usize,
    y_count: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let end = record.offset + record.len;
    if end > file.mmap.len() {
        return Err(format!("wx chunk {chunk_id} exceeds file length").into());
    }
    let decoded = zstd::stream::decode_all(&file.mmap[record.offset..end])?;
    let expected_len =
        y_count * record.x_count * file.header.levels_len * file.header.hours_len * 2;
    if decoded.len() != expected_len {
        return Err(format!(
            "wx decoded chunk {chunk_id} for {variable} has {} bytes, expected {expected_len}",
            decoded.len()
        )
        .into());
    }
    Ok(decoded)
}

fn chunk_y_count(chunk_row: usize, chunk_y: usize, ny: usize) -> usize {
    let y0 = chunk_row * chunk_y;
    ny.saturating_sub(y0).min(chunk_y)
}

fn decode_quantized_value(q: i16, header: WxpHeader) -> Option<f64> {
    (q != MISSING_I16)
        .then(|| f64::from(q) / f64::from(header.scale_factor) - f64::from(header.add_offset))
}

fn f32_at(bytes: &[u8], index: usize) -> f32 {
    let offset = index * 4;
    f32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn u32_from(bytes: &[u8]) -> Result<u32, Box<dyn std::error::Error>> {
    Ok(u32::from_le_bytes(bytes.try_into()?))
}

fn u64_from(bytes: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(u64::from_le_bytes(bytes.try_into()?))
}

fn f32_from(bytes: &[u8]) -> Result<f32, Box<dyn std::error::Error>> {
    Ok(f32::from_le_bytes(bytes.try_into()?))
}

fn normalize_lon(lon: f64) -> f64 {
    ((lon + 180.0).rem_euclid(360.0)) - 180.0
}

fn normalized_lon_delta(mut delta: f64) -> f64 {
    while delta > 180.0 {
        delta -= 360.0;
    }
    while delta < -180.0 {
        delta += 360.0;
    }
    delta
}
