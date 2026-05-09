//! Concrete native dataset materializer.
//!
//! This module wires the hour-major scheduler to the source-specific decode
//! scaffolds and the hot training shard writer. It is intentionally conservative:
//! local raw-cache files are preferred for observation sources, HRRR can use the
//! existing rustwx model fetch cache, and missing sources can either fail the run
//! or be written as NaN tensors for plumbing/profiling tests.

use chrono::{DateTime, Datelike, Utc};
use rayon::prelude::*;
use rustwx_core::{CycleSpec, GridShape, LatLonGrid, ModelId, ModelRunRequest, SourceId};
use rustwx_io::{FetchRequest, fetch_bytes_with_cache};
use rustwx_radar::batch::{
    CartesianGridSpec, Level2TensorProduct, build_level2_cartesian_tensors,
    parse_level2_object_name_scan_time,
};
use rustwx_radar::nexrad::sites::{find_nearest_site, find_site};
use rustwx_radar::{Level2File, aws as nexrad_aws};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::native_dataset::{
    NativeDatasetHourJob, NativeDatasetPlan, NativeDatasetSourceKind, NativeDatasetTile,
    NativeHourOutput, NativeHourProcessor,
};
use crate::native_dataset_hrrr::{
    HrrrHourCache, NativeDatasetTileGrid, RemapMethod, decode_hrrr_hour_once,
    precompute_tile_remap_with_projection, remap_hrrr_hour_to_tile,
};
use crate::native_dataset_obs::{
    GOES_MCMIPC_CHANNELS, GoesAbiChannelSpec, NativeObsTileGrid, read_goes_multiband_hour,
    read_mrms_product_hour, remap_goes_hour_to_tile, remap_mrms_hour_to_tile,
};
use crate::native_dataset_shard_store::{
    TrainingShardManifest, TrainingShardSampleTensor, TrainingShardTensorSpec, TrainingShardWriter,
};
use crate::satellite::parse_goes_abi_filename;

const TARGET_MRMS_CHANNEL_COUNT: usize = 2;
const TARGET_GOES_CHANNEL_COUNT: usize = 1;
const TARGET_INITIATION_CHANNEL_COUNT: usize = 1;
const TARGET_REFC_THRESHOLD_CHANNEL_COUNT: usize = 1;
const CURRENT_REFC_CHANNEL_COUNT: usize = 1;
const REFLECTIVITY_INITIATION_THRESHOLD_DBZ: f32 = 35.0;
const QUIET_REFLECTIVITY_DBZ: f32 = -20.0;
const GOES_MAX_LOCAL_DELTA_MS: i64 = 30 * 60 * 1000;
const MRMS_MAX_LOCAL_DELTA_MS: i64 = 20 * 60 * 1000;
const LEVEL2_MAX_LOCAL_DELTA_MS: i64 = 10 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeMaterializerMissingPolicy {
    Fail,
    FillNan,
}

impl Default for NativeMaterializerMissingPolicy {
    fn default() -> Self {
        Self::Fail
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeDatasetMaterializerConfig {
    pub source_root: Option<PathBuf>,
    pub cache_root: PathBuf,
    pub shard_out_dir: PathBuf,
    #[serde(default = "default_hrrr_source")]
    pub hrrr_source: SourceId,
    #[serde(default = "default_true")]
    pub use_cache: bool,
    #[serde(default)]
    pub fetch_hrrr_when_missing: bool,
    #[serde(default)]
    pub fetch_obs_when_missing: bool,
    #[serde(default)]
    pub fetch_level2_when_missing: bool,
    #[serde(default)]
    pub missing_policy: NativeMaterializerMissingPolicy,
}

impl NativeDatasetMaterializerConfig {
    pub fn new(cache_root: impl Into<PathBuf>, shard_out_dir: impl Into<PathBuf>) -> Self {
        Self {
            source_root: None,
            cache_root: cache_root.into(),
            shard_out_dir: shard_out_dir.into(),
            hrrr_source: default_hrrr_source(),
            use_cache: true,
            fetch_hrrr_when_missing: false,
            fetch_obs_when_missing: false,
            fetch_level2_when_missing: false,
            missing_policy: NativeMaterializerMissingPolicy::Fail,
        }
    }

    pub fn with_source_root(mut self, source_root: impl Into<PathBuf>) -> Self {
        self.source_root = Some(source_root.into());
        self
    }

    pub fn with_missing_policy(mut self, missing_policy: NativeMaterializerMissingPolicy) -> Self {
        self.missing_policy = missing_policy;
        self
    }

    pub fn with_fetch_hrrr_when_missing(mut self, fetch_hrrr_when_missing: bool) -> Self {
        self.fetch_hrrr_when_missing = fetch_hrrr_when_missing;
        self
    }

    pub fn with_fetch_obs_when_missing(mut self, fetch_obs_when_missing: bool) -> Self {
        self.fetch_obs_when_missing = fetch_obs_when_missing;
        self
    }

    pub fn with_fetch_level2_when_missing(mut self, fetch_level2_when_missing: bool) -> Self {
        self.fetch_level2_when_missing = fetch_level2_when_missing;
        self
    }
}

fn default_hrrr_source() -> SourceId {
    SourceId::Aws
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeDatasetMaterializerLayout {
    pub hrrr_field_ids: Vec<String>,
    pub mrms_field_ids: Vec<String>,
    pub goes_product_family: String,
    pub goes_channel_ids: Vec<String>,
    pub level2_product_ids: Vec<String>,
    pub grid_size: usize,
    pub history_steps: usize,
}

pub struct NativeDatasetMaterializer {
    config: NativeDatasetMaterializerConfig,
    layout: NativeDatasetMaterializerLayout,
    writer: Option<TrainingShardWriter>,
    frames: BTreeMap<FrameKey, MaterializedFrame>,
}

#[derive(Debug, Clone, PartialEq)]
struct MaterializedFrame {
    hrrr_fields: Vec<f32>,
    mrms_fields: Vec<f32>,
    goes_fields: Vec<f32>,
    level2_fields: Vec<f32>,
    hrrr_valid_mask: Vec<f32>,
    mrms_valid_mask: Vec<f32>,
    goes_valid_mask: Vec<f32>,
    level2_valid_mask: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FrameKey {
    tile_id: String,
    valid_time_utc: DateTime<Utc>,
}

impl NativeDatasetMaterializer {
    pub fn create(
        plan: &NativeDatasetPlan,
        config: NativeDatasetMaterializerConfig,
    ) -> Result<Self, Box<dyn Error>> {
        let layout = NativeDatasetMaterializerLayout::from_plan(plan)?;
        let manifest = training_shard_manifest_for_plan(plan, &layout)?;
        let writer = TrainingShardWriter::create(&config.shard_out_dir, manifest)?;
        Ok(Self {
            config,
            layout,
            writer: Some(writer),
            frames: BTreeMap::new(),
        })
    }

    pub fn finish(mut self) -> Result<TrainingShardManifest, Box<dyn Error>> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| boxed_error("native materializer shard writer already finished"))?;
        Ok(writer.finish()?)
    }

    fn materialize_hour(
        &mut self,
        plan: &NativeDatasetPlan,
        job: &NativeDatasetHourJob,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let artifacts = Vec::new();
        let hrrr = self.load_hrrr(job.valid_time_utc)?;
        let goes = self.load_goes(job.valid_time_utc)?;
        let mrms = self.load_mrms(job.valid_time_utc)?;

        let tile_frames = job
            .tile_jobs
            .par_iter()
            .map(|tile_job| {
                let tile = plan
                    .shard
                    .tiles
                    .iter()
                    .find(|tile| tile.tile_id == tile_job.tile_id)
                    .ok_or_else(|| format!("tile '{}' not found in shard", tile_job.tile_id))?;
                let frame = self
                    .materialize_tile_frame(job.valid_time_utc, tile, &hrrr, goes.as_ref(), &mrms)
                    .map_err(|err| err.to_string())?;
                Ok::<_, String>((
                    FrameKey {
                        tile_id: tile.tile_id.clone(),
                        valid_time_utc: job.valid_time_utc,
                    },
                    frame,
                ))
            })
            .collect::<Vec<_>>();

        for result in tile_frames {
            let (key, frame) = result.map_err(boxed_error)?;
            self.frames.insert(key, frame);
        }

        Ok(artifacts)
    }

    fn emit_ready_samples(
        &mut self,
        plan: &NativeDatasetPlan,
        job: &NativeDatasetHourJob,
    ) -> Result<usize, Box<dyn Error>> {
        let ready = plan
            .shard
            .sample_windows
            .iter()
            .filter(|sample| {
                sample.case_id == job.case_id && sample.target_time_utc == job.valid_time_utc
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut emitted = 0usize;
        for sample in ready {
            let Some(target_frame) = self.frames.get(&FrameKey {
                tile_id: sample.tile_id.clone(),
                valid_time_utc: sample.target_time_utc,
            }) else {
                continue;
            };
            let Some(current_frame) = self.frames.get(&FrameKey {
                tile_id: sample.tile_id.clone(),
                valid_time_utc: sample.valid_time_utc,
            }) else {
                continue;
            };
            let mut history = Vec::with_capacity(sample.history_frame_times_utc.len());
            let mut all_history_available = true;
            for history_time in &sample.history_frame_times_utc {
                let key = FrameKey {
                    tile_id: sample.tile_id.clone(),
                    valid_time_utc: *history_time,
                };
                match self.frames.get(&key) {
                    Some(frame) => history.push(frame),
                    None => {
                        all_history_available = false;
                        break;
                    }
                }
            }
            if !all_history_available {
                continue;
            }

            let mrms_hist = stack_history(&history, |frame| &frame.mrms_fields);
            let goes_hist = stack_history(&history, |frame| &frame.goes_fields);
            let mrms_hist_valid_mask = stack_history(&history, |frame| &frame.mrms_valid_mask);
            let goes_hist_valid_mask = stack_history(&history, |frame| &frame.goes_valid_mask);
            let target_mrms = target_mrms_from_frame(target_frame, &self.layout);
            let target_goes_c13 = target_goes_c13_from_frame(target_frame, &self.layout);
            let current_refc = current_refc_from_frame(current_frame, &self.layout);
            let target_initiation =
                initiation_from_refc(&current_refc, &target_mrms, self.layout.grid_size);
            let target_refc_ge35 = threshold_from_refc(
                &target_mrms,
                self.layout.grid_size,
                REFLECTIVITY_INITIATION_THRESHOLD_DBZ,
            );

            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| boxed_error("native materializer shard writer is closed"))?;
            writer.append_sample(
                sample.sample_id,
                &[
                    TrainingShardSampleTensor {
                        name: "hrrr_fields",
                        values: &current_frame.hrrr_fields,
                    },
                    TrainingShardSampleTensor {
                        name: "mrms_hist",
                        values: &mrms_hist,
                    },
                    TrainingShardSampleTensor {
                        name: "goes_hist",
                        values: &goes_hist,
                    },
                    TrainingShardSampleTensor {
                        name: "level2_fields",
                        values: &current_frame.level2_fields,
                    },
                    TrainingShardSampleTensor {
                        name: "target_mrms",
                        values: &target_mrms,
                    },
                    TrainingShardSampleTensor {
                        name: "target_goes_c13",
                        values: &target_goes_c13,
                    },
                    TrainingShardSampleTensor {
                        name: "target_initiation",
                        values: &target_initiation,
                    },
                    TrainingShardSampleTensor {
                        name: "current_refc",
                        values: &current_refc,
                    },
                    TrainingShardSampleTensor {
                        name: "target_refc_ge35",
                        values: &target_refc_ge35,
                    },
                    TrainingShardSampleTensor {
                        name: "hrrr_valid_mask",
                        values: &current_frame.hrrr_valid_mask,
                    },
                    TrainingShardSampleTensor {
                        name: "mrms_hist_valid_mask",
                        values: &mrms_hist_valid_mask,
                    },
                    TrainingShardSampleTensor {
                        name: "goes_hist_valid_mask",
                        values: &goes_hist_valid_mask,
                    },
                    TrainingShardSampleTensor {
                        name: "level2_valid_mask",
                        values: &current_frame.level2_valid_mask,
                    },
                    TrainingShardSampleTensor {
                        name: "target_valid_mask",
                        values: &target_frame.mrms_valid_mask,
                    },
                ],
            )?;
            emitted += 1;
        }
        self.prune_old_frames(plan, job.valid_time_utc);
        Ok(emitted)
    }

    fn prune_old_frames(&mut self, plan: &NativeDatasetPlan, latest_time: DateTime<Utc>) {
        let keep = plan
            .shard
            .sample_windows
            .iter()
            .filter(|sample| sample.target_time_utc >= latest_time)
            .flat_map(|sample| {
                sample
                    .history_frame_times_utc
                    .iter()
                    .copied()
                    .chain(std::iter::once(sample.target_time_utc))
            })
            .collect::<BTreeSet<_>>();
        self.frames
            .retain(|key, _| keep.contains(&key.valid_time_utc));
    }

    fn materialize_tile_frame(
        &self,
        valid_time: DateTime<Utc>,
        tile: &NativeDatasetTile,
        hrrr: &Option<HrrrHourCache>,
        goes: Option<&crate::native_dataset_obs::DecodedGoesHour>,
        mrms: &[Option<crate::native_dataset_obs::DecodedMrmsHour>],
    ) -> Result<MaterializedFrame, Box<dyn Error>> {
        let grid_size = self.layout.grid_size;
        let tile_grid = regular_tile_latlon_grid(tile, grid_size)?;
        let obs_tile = NativeObsTileGrid::new(tile.bounds, grid_size, grid_size)?;
        let hrrr_fields = match hrrr {
            Some(hour) => {
                let remap = precompute_tile_remap_with_projection(
                    hour.grid
                        .as_ref()
                        .ok_or_else(|| boxed_error("decoded HRRR hour has no grid"))?,
                    hour.projection.as_ref(),
                    &NativeDatasetTileGrid::new(tile.tile_id.clone(), tile_grid),
                    RemapMethod::Bilinear,
                )?;
                let remapped = remap_hrrr_hour_to_tile(hour, &remap)?;
                stack_hrrr_fields(&self.layout.hrrr_field_ids, &remapped.fields, grid_size)
            }
            None => nan_vec(self.layout.hrrr_field_ids.len() * grid_size * grid_size),
        };
        let hrrr_valid_mask =
            valid_mask_all_channels(&hrrr_fields, self.layout.hrrr_field_ids.len(), grid_size);

        let goes_fields = match goes {
            Some(hour) => {
                let remapped = remap_goes_hour_to_tile(hour, obs_tile)?;
                stack_obs_bands(&self.layout.goes_channel_ids, &remapped.bands, grid_size)
            }
            None => nan_vec(self.layout.goes_channel_ids.len() * grid_size * grid_size),
        };
        let goes_valid_mask =
            valid_mask_all_channels(&goes_fields, self.layout.goes_channel_ids.len(), grid_size);

        let mut mrms_fields =
            Vec::with_capacity(self.layout.mrms_field_ids.len() * grid_size * grid_size);
        for (field_index, decoded) in mrms.iter().enumerate() {
            match decoded {
                Some(hour) => {
                    let remapped = remap_mrms_hour_to_tile(hour, obs_tile)?;
                    let field_id = self
                        .layout
                        .mrms_field_ids
                        .get(field_index)
                        .map(String::as_str)
                        .unwrap_or_default();
                    mrms_fields.extend(sanitize_mrms_channel(field_id, &remapped.values));
                }
                None => mrms_fields.extend(nan_vec(grid_size * grid_size)),
            }
        }
        let mrms_valid_mask =
            valid_mask_all_channels(&mrms_fields, self.layout.mrms_field_ids.len(), grid_size);

        let level2_fields = self.materialize_level2(valid_time, tile)?;
        let level2_valid_mask = valid_mask_any_channel(
            &level2_fields,
            self.layout.level2_product_ids.len(),
            grid_size,
        );
        Ok(MaterializedFrame {
            hrrr_fields,
            mrms_fields,
            goes_fields,
            level2_fields,
            hrrr_valid_mask,
            mrms_valid_mask,
            goes_valid_mask,
            level2_valid_mask,
        })
    }

    fn materialize_level2(
        &self,
        valid_time: DateTime<Utc>,
        tile: &NativeDatasetTile,
    ) -> Result<Vec<f32>, Box<dyn Error>> {
        let site = match tile
            .radar_site
            .as_deref()
            .and_then(find_site)
            .or_else(|| find_nearest_site(tile.center_lat, tile.center_lon))
        {
            Some(site) => site,
            None if self.config.missing_policy == NativeMaterializerMissingPolicy::FillNan => {
                return Ok(nan_vec(
                    self.layout.level2_product_ids.len()
                        * self.layout.grid_size
                        * self.layout.grid_size,
                ));
            }
            None => {
                return Err(boxed_error(format!(
                    "no Level-II radar site found for tile '{}'",
                    tile.tile_id
                )));
            }
        };

        let (bytes, source_key_or_url) = match self.load_level2_bytes(valid_time, site.id)? {
            Some(result) => result,
            None => {
                return Ok(nan_vec(
                    self.layout.level2_product_ids.len()
                        * self.layout.grid_size
                        * self.layout.grid_size,
                ));
            }
        };
        let file = match Level2File::parse(&bytes) {
            Ok(file) => file,
            Err(_err) if self.config.missing_policy == NativeMaterializerMissingPolicy::FillNan => {
                return Ok(nan_vec(
                    self.layout.level2_product_ids.len()
                        * self.layout.grid_size
                        * self.layout.grid_size,
                ));
            }
            Err(err) => return Err(boxed_error(format!("failed to parse Level-II file: {err}"))),
        };
        let products = self.layout.level2_products()?;
        let grid_spec = radar_grid_spec_for_tile(tile, self.layout.grid_size)?;
        let tensors =
            build_level2_cartesian_tensors(&file, site, source_key_or_url, &products, &grid_spec);
        let mut out = Vec::with_capacity(
            self.layout.level2_product_ids.len() * self.layout.grid_size * self.layout.grid_size,
        );
        let g2 = self.layout.grid_size * self.layout.grid_size;
        for product in products {
            match tensors
                .iter()
                .find(|tensor| tensor.metadata.product == product)
            {
                Some(tensor) if tensor.values.len() == g2 => out.extend_from_slice(&tensor.values),
                _ => out.extend(nan_vec(g2)),
            }
        }
        Ok(out)
    }

    fn load_hrrr(
        &self,
        valid_time: DateTime<Utc>,
    ) -> Result<Option<HrrrHourCache>, Box<dyn Error>> {
        let hrrr_path = self.config.source_root.as_ref().map(|root| {
            root.join("hrrr")
                .join(format!("hrrr_{}.grib2", valid_time.format("%Y%m%d_%H")))
        });
        let bytes = match hrrr_path {
            Some(path) if path.exists() => fs::read(path)?,
            Some(path) if !self.config.fetch_hrrr_when_missing => {
                return self.handle_missing_source(
                    "hrrr_wrfsfc",
                    valid_time,
                    boxed_error(format!(
                        "local HRRR file does not exist: {}",
                        path.display()
                    )),
                );
            }
            Some(path) => {
                debug_assert!(self.config.fetch_hrrr_when_missing);
                match self.fetch_hrrr(valid_time) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        return self.handle_missing_source(
                            "hrrr_wrfsfc",
                            valid_time,
                            boxed_error(format!(
                                "local HRRR file missing at {} and fetch failed: {err}",
                                path.display()
                            )),
                        );
                    }
                }
            }
            None if self.config.fetch_hrrr_when_missing => match self.fetch_hrrr(valid_time) {
                Ok(bytes) => bytes,
                Err(err) => return self.handle_missing_source("hrrr_wrfsfc", valid_time, err),
            },
            None => {
                return self.handle_missing_source(
                    "hrrr_wrfsfc",
                    valid_time,
                    boxed_error("no --source-root HRRR cache and HRRR network fetch is disabled"),
                );
            }
        };
        match decode_hrrr_hour_once(
            valid_time,
            "hrrr_wrfsfc",
            &bytes,
            self.layout.hrrr_field_ids.iter().map(String::as_str),
        ) {
            Ok(hour) => Ok(Some(hour)),
            Err(err) if self.config.missing_policy == NativeMaterializerMissingPolicy::FillNan => {
                let _ = err;
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn fetch_hrrr(&self, valid_time: DateTime<Utc>) -> Result<Vec<u8>, Box<dyn Error>> {
        let cycle = CycleSpec::new(
            valid_time.format("%Y%m%d").to_string(),
            valid_time.hour() as u8,
        )?;
        let request = ModelRunRequest::new(ModelId::Hrrr, cycle, 0, "sfc")?;
        let fetch = FetchRequest {
            request,
            source_override: Some(self.config.hrrr_source),
            variable_patterns: Vec::new(),
            earth2_ensemble: None,
        };
        Ok(
            fetch_bytes_with_cache(&fetch, &self.config.cache_root, self.config.use_cache)?
                .result
                .bytes,
        )
    }

    fn load_goes(
        &self,
        valid_time: DateTime<Utc>,
    ) -> Result<Option<crate::native_dataset_obs::DecodedGoesHour>, Box<dyn Error>> {
        let root = self.obs_raw_root();
        let goes_dir = root.join("goes");
        let path = match find_nearest_goes_file(
            &goes_dir,
            valid_time,
            GOES_MAX_LOCAL_DELTA_MS,
            &self.layout.goes_product_family,
        )? {
            Some(path) => path,
            None if self.config.fetch_obs_when_missing => {
                match self.fetch_goes(valid_time, &goes_dir) {
                    Ok(path) => path,
                    Err(err) => {
                        return self.handle_missing_source(
                            "goes_abi",
                            valid_time,
                            boxed_error(format!(
                                "no local GOES ABI file found and fetch failed: {err}"
                            )),
                        );
                    }
                }
            }
            None => {
                return self.handle_missing_source(
                    "goes_abi",
                    valid_time,
                    boxed_error("no local GOES ABI file found"),
                );
            }
        };
        let channels = self
            .layout
            .goes_channel_ids
            .iter()
            .filter_map(|id| GOES_MCMIPC_CHANNELS.iter().find(|channel| channel.id == id))
            .copied()
            .collect::<Vec<GoesAbiChannelSpec>>();
        match read_goes_multiband_hour(path, &channels) {
            Ok(hour) => Ok(Some(hour)),
            Err(err) if self.config.missing_policy == NativeMaterializerMissingPolicy::FillNan => {
                let _ = err;
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn load_mrms(
        &self,
        valid_time: DateTime<Utc>,
    ) -> Result<Vec<Option<crate::native_dataset_obs::DecodedMrmsHour>>, Box<dyn Error>> {
        let source_root = self.obs_raw_root();
        let mut out = Vec::with_capacity(self.layout.mrms_field_ids.len());
        for field_id in &self.layout.mrms_field_ids {
            let dir = source_root.join("mrms").join(field_id);
            match find_nearest_mrms_file(&dir, valid_time, MRMS_MAX_LOCAL_DELTA_MS)? {
                Some(path) => match read_mrms_product_hour(path) {
                    Ok(hour) => out.push(Some(hour)),
                    Err(err)
                        if self.config.missing_policy
                            == NativeMaterializerMissingPolicy::FillNan =>
                    {
                        let _ = err;
                        out.push(None);
                    }
                    Err(err) => return Err(err),
                },
                None if self.config.fetch_obs_when_missing => {
                    match self.fetch_mrms(field_id, valid_time, &dir) {
                        Ok(path) => match read_mrms_product_hour(path) {
                            Ok(hour) => out.push(Some(hour)),
                            Err(err)
                                if self.config.missing_policy
                                    == NativeMaterializerMissingPolicy::FillNan =>
                            {
                                let _ = err;
                                out.push(None);
                            }
                            Err(err) => return Err(err),
                        },
                        Err(_err)
                            if self.config.missing_policy
                                == NativeMaterializerMissingPolicy::FillNan =>
                        {
                            out.push(None);
                        }
                        Err(err) => {
                            return Err(boxed_error(format!(
                                "no local MRMS file for field '{field_id}' near {} and fetch failed: {err}",
                                valid_time.to_rfc3339()
                            )));
                        }
                    }
                }
                None if self.config.missing_policy == NativeMaterializerMissingPolicy::FillNan => {
                    out.push(None)
                }
                None => {
                    return Err(boxed_error(format!(
                        "no local MRMS file for field '{field_id}' near {}",
                        valid_time.to_rfc3339()
                    )));
                }
            }
        }
        Ok(out)
    }

    fn load_level2_bytes(
        &self,
        valid_time: DateTime<Utc>,
        site_id: &str,
    ) -> Result<Option<(Vec<u8>, String)>, Box<dyn Error>> {
        let root = self.obs_raw_root();
        for dir in [
            root.join("level2").join(site_id),
            root.join("nexrad").join(site_id),
            root.join("radar").join(site_id),
        ] {
            if let Some(path) =
                find_nearest_level2_file(&dir, valid_time, LEVEL2_MAX_LOCAL_DELTA_MS)?
            {
                let bytes = fs::read(&path).map(maybe_decompress_gzip)?;
                return Ok(Some((bytes, path.display().to_string())));
            }
        }

        if self.config.fetch_level2_when_missing {
            let resolved =
                match rustwx_radar::batch::resolve_nearest_volume(site_id, valid_time, 10) {
                    Ok(resolved) => resolved,
                    Err(_err)
                        if self.config.missing_policy
                            == NativeMaterializerMissingPolicy::FillNan =>
                    {
                        return Ok(None);
                    }
                    Err(err) => return Err(boxed_error(format!("Level-II resolve failed: {err}"))),
                };
            let bytes = match nexrad_aws::fetch_object(&resolved.s3_key) {
                Ok(bytes) => bytes,
                Err(_err)
                    if self.config.missing_policy == NativeMaterializerMissingPolicy::FillNan =>
                {
                    return Ok(None);
                }
                Err(err) => return Err(boxed_error(format!("Level-II fetch failed: {err}"))),
            };
            let cache_dir = root.join("level2").join(site_id);
            fs::create_dir_all(&cache_dir)?;
            let filename = resolved
                .s3_key
                .rsplit('/')
                .next()
                .filter(|value| !value.is_empty())
                .unwrap_or("level2_volume");
            let path = cache_dir.join(filename);
            if !path.exists() {
                fs::write(&path, &bytes)?;
            }
            return Ok(Some((bytes, resolved.s3_key)));
        }

        match self.config.missing_policy {
            NativeMaterializerMissingPolicy::FillNan => Ok(None),
            NativeMaterializerMissingPolicy::Fail => Err(boxed_error(format!(
                "no local Level-II file for site {site_id} near {} and fetch is disabled",
                valid_time.to_rfc3339()
            ))),
        }
    }

    fn obs_raw_root(&self) -> PathBuf {
        self.config
            .source_root
            .clone()
            .unwrap_or_else(|| self.config.cache_root.join("raw"))
    }

    fn fetch_goes(
        &self,
        valid_time: DateTime<Utc>,
        out_dir: &Path,
    ) -> Result<PathBuf, Box<dyn Error>> {
        let product_prefix = goes_s3_prefix_product(&self.layout.goes_product_family);
        let prefix = format!(
            "{}/{:04}/{:03}/{:02}/",
            product_prefix,
            valid_time.year(),
            valid_time.ordinal(),
            valid_time.hour()
        );
        let objects = list_public_s3("https://noaa-goes16.s3.amazonaws.com", &prefix)?;
        let object = objects
            .into_iter()
            .filter_map(|object| {
                let name = object.key.rsplit('/').next()?;
                let parsed = parse_goes_abi_filename(name).ok()?;
                if !goes_filename_product_matches_request(
                    &parsed.product,
                    &self.layout.goes_product_family,
                ) {
                    return None;
                }
                let delta = (parsed.start_time_utc.timestamp_millis()
                    - valid_time.timestamp_millis())
                .abs();
                Some((delta, object))
            })
            .min_by_key(|(delta, _)| *delta)
            .map(|(_, object)| object)
            .ok_or_else(|| {
                boxed_error(format!("no GOES ABI object listed under prefix {prefix}"))
            })?;
        fs::create_dir_all(out_dir)?;
        let filename = object
            .key
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| boxed_error("GOES object key has no filename"))?;
        let path = out_dir.join(filename);
        download_public_s3_object("https://noaa-goes16.s3.amazonaws.com", &object.key, &path)?;
        Ok(path)
    }

    fn fetch_mrms(
        &self,
        field_id: &str,
        valid_time: DateTime<Utc>,
        out_dir: &Path,
    ) -> Result<PathBuf, Box<dyn Error>> {
        let product_prefix = mrms_product_prefix(field_id).ok_or_else(|| {
            boxed_error(format!("no MRMS product mapping for field '{field_id}'"))
        })?;
        let prefix = format!("{product_prefix}/{}/", valid_time.format("%Y%m%d"));
        let objects = list_public_s3("https://noaa-mrms-pds.s3.amazonaws.com", &prefix)?;
        let object = objects
            .into_iter()
            .filter_map(|object| {
                let name = object.key.rsplit('/').next()?;
                let time = parse_mrms_filename_time(name)?;
                let delta = (time.timestamp_millis() - valid_time.timestamp_millis()).abs();
                Some((delta, object))
            })
            .min_by_key(|(delta, _)| *delta)
            .map(|(_, object)| object)
            .ok_or_else(|| boxed_error(format!("no MRMS object listed under prefix {prefix}")))?;
        fs::create_dir_all(out_dir)?;
        let filename = object
            .key
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| boxed_error("MRMS object key has no filename"))?;
        let path = out_dir.join(filename);
        download_public_s3_object("https://noaa-mrms-pds.s3.amazonaws.com", &object.key, &path)?;
        Ok(path)
    }

    fn handle_missing_source<T>(
        &self,
        source_id: &str,
        valid_time: DateTime<Utc>,
        err: Box<dyn Error>,
    ) -> Result<Option<T>, Box<dyn Error>> {
        match self.config.missing_policy {
            NativeMaterializerMissingPolicy::FillNan => Ok(None),
            NativeMaterializerMissingPolicy::Fail => Err(boxed_error(format!(
                "missing source {source_id} for {}: {err}",
                valid_time.to_rfc3339()
            ))),
        }
    }
}

impl NativeHourProcessor for NativeDatasetMaterializer {
    fn process_hour(
        &mut self,
        plan: &NativeDatasetPlan,
        job: &NativeDatasetHourJob,
    ) -> Result<NativeHourOutput, String> {
        let artifacts = self
            .materialize_hour(plan, job)
            .map_err(|err| err.to_string())?;
        let samples_emitted = self
            .emit_ready_samples(plan, job)
            .map_err(|err| err.to_string())?;
        Ok(NativeHourOutput {
            samples_emitted,
            artifacts,
        })
    }
}

impl NativeDatasetMaterializerLayout {
    pub fn from_plan(plan: &NativeDatasetPlan) -> Result<Self, Box<dyn Error>> {
        let grid_size = usize::from(plan.config.grid_size);
        let history_steps = usize::from(plan.config.history_steps);
        let source = |kind: NativeDatasetSourceKind| {
            plan.config
                .sources
                .iter()
                .find(|source| source.kind == kind)
                .map(|source| source.fields.clone())
                .unwrap_or_default()
        };
        let goes_product_family = plan
            .config
            .sources
            .iter()
            .find(|source| source.kind == NativeDatasetSourceKind::GoesNetcdf)
            .and_then(|source| source.product_family.clone())
            .unwrap_or_else(|| "ABI-L2-MCMIPC".to_string());
        Ok(Self {
            hrrr_field_ids: source(NativeDatasetSourceKind::ModelGrib),
            mrms_field_ids: source(NativeDatasetSourceKind::MrmsGrib),
            goes_product_family,
            goes_channel_ids: source(NativeDatasetSourceKind::GoesNetcdf),
            level2_product_ids: source(NativeDatasetSourceKind::RadarLevel2),
            grid_size,
            history_steps,
        })
    }

    pub fn level2_products(&self) -> Result<Vec<Level2TensorProduct>, Box<dyn Error>> {
        self.level2_product_ids
            .iter()
            .map(|id| parse_level2_tensor_product(id))
            .collect()
    }
}

fn parse_level2_tensor_product(value: &str) -> Result<Level2TensorProduct, Box<dyn Error>> {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_");
    let product = match normalized.as_str() {
        "reflectivity" | "ref" | "refl" | "dbz" => Level2TensorProduct::Reflectivity,
        "velocity" | "vel" => Level2TensorProduct::Velocity,
        "spectrum_width" | "sw" => Level2TensorProduct::SpectrumWidth,
        "differential_reflectivity" | "zdr" => Level2TensorProduct::DifferentialReflectivity,
        "correlation_coefficient" | "cc" | "rho" | "rhohv" => {
            Level2TensorProduct::CorrelationCoefficient
        }
        "differential_phase" | "phi" | "phidp" => Level2TensorProduct::DifferentialPhase,
        "specific_diff_phase" | "kdp" => Level2TensorProduct::SpecificDiffPhase,
        "hydrometeor_class" | "hca" | "hhc" => Level2TensorProduct::HydrometeorClass,
        "storm_relative_velocity" | "srv" => Level2TensorProduct::StormRelativeVelocity,
        "vil" => Level2TensorProduct::Vil,
        "echo_tops" | "et" => Level2TensorProduct::EchoTops,
        _ => {
            return Err(boxed_error(format!(
                "unknown Level-II native dataset product '{value}'"
            )));
        }
    };
    Ok(product)
}

pub fn training_shard_manifest_for_plan(
    plan: &NativeDatasetPlan,
    layout: &NativeDatasetMaterializerLayout,
) -> Result<TrainingShardManifest, Box<dyn Error>> {
    let shard_id = format!("{}-shard-{:05}", plan.dataset_name, plan.shard.shard_index);
    let g = layout.grid_size;
    let h = layout.history_steps;
    Ok(TrainingShardManifest::new(
        shard_id,
        vec![
            TrainingShardTensorSpec::f32_raw(
                "hrrr_fields",
                "hrrr",
                vec![layout.hrrr_field_ids.len(), g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "mrms_hist",
                "mrms",
                vec![h, layout.mrms_field_ids.len(), g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "goes_hist",
                "goes",
                vec![h, layout.goes_channel_ids.len(), g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "level2_fields",
                "radar",
                vec![layout.level2_product_ids.len(), g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "target_mrms",
                "target",
                vec![TARGET_MRMS_CHANNEL_COUNT, g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "target_goes_c13",
                "target",
                vec![TARGET_GOES_CHANNEL_COUNT, g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "target_initiation",
                "target",
                vec![TARGET_INITIATION_CHANNEL_COUNT, g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "current_refc",
                "target",
                vec![CURRENT_REFC_CHANNEL_COUNT, g, g],
            )?,
            TrainingShardTensorSpec::f32_raw(
                "target_refc_ge35",
                "target",
                vec![TARGET_REFC_THRESHOLD_CHANNEL_COUNT, g, g],
            )?,
            TrainingShardTensorSpec::f32_raw("hrrr_valid_mask", "mask", vec![1, g, g])?,
            TrainingShardTensorSpec::f32_raw("mrms_hist_valid_mask", "mask", vec![h, 1, g, g])?,
            TrainingShardTensorSpec::f32_raw("goes_hist_valid_mask", "mask", vec![h, 1, g, g])?,
            TrainingShardTensorSpec::f32_raw("level2_valid_mask", "mask", vec![1, g, g])?,
            TrainingShardTensorSpec::f32_raw("target_valid_mask", "mask", vec![1, g, g])?,
        ],
    )?)
}

fn regular_tile_latlon_grid(
    tile: &NativeDatasetTile,
    grid_size: usize,
) -> Result<LatLonGrid, Box<dyn Error>> {
    let shape = GridShape::new(grid_size, grid_size)?;
    let obs_grid = NativeObsTileGrid::new(tile.bounds, grid_size, grid_size)?;
    let mut lat = Vec::with_capacity(shape.len());
    let mut lon = Vec::with_capacity(shape.len());
    for row in 0..grid_size {
        for col in 0..grid_size {
            let (lat_value, lon_value) = obs_grid.lat_lon_at(row, col);
            lat.push(lat_value as f32);
            lon.push(lon_value as f32);
        }
    }
    Ok(LatLonGrid::new(shape, lat, lon)?)
}

fn stack_hrrr_fields(
    field_ids: &[String],
    fields: &BTreeMap<String, crate::native_dataset_hrrr::HrrrCachedField>,
    grid_size: usize,
) -> Vec<f32> {
    let mut out = Vec::with_capacity(field_ids.len() * grid_size * grid_size);
    for field_id in field_ids {
        match fields.get(field_id) {
            Some(field) => out.extend_from_slice(&field.values),
            None => out.extend(nan_vec(grid_size * grid_size)),
        }
    }
    out
}

fn stack_obs_bands(
    field_ids: &[String],
    bands: &[crate::native_dataset_obs::RemappedObsBand],
    grid_size: usize,
) -> Vec<f32> {
    let mut out = Vec::with_capacity(field_ids.len() * grid_size * grid_size);
    for field_id in field_ids {
        match bands.iter().find(|band| &band.field_id == field_id) {
            Some(band) => out.extend_from_slice(&band.values),
            None => out.extend(nan_vec(grid_size * grid_size)),
        }
    }
    out
}

fn stack_history<'a, F>(history: &'a [&'a MaterializedFrame], field: F) -> Vec<f32>
where
    F: Fn(&'a MaterializedFrame) -> &'a [f32],
{
    let element_count = history.iter().map(|frame| field(frame).len()).sum();
    let mut out = Vec::with_capacity(element_count);
    for frame in history {
        out.extend_from_slice(field(frame));
    }
    out
}

fn target_mrms_from_frame(
    frame: &MaterializedFrame,
    layout: &NativeDatasetMaterializerLayout,
) -> Vec<f32> {
    let g2 = layout.grid_size * layout.grid_size;
    let mut out = Vec::with_capacity(TARGET_MRMS_CHANNEL_COUNT * g2);
    append_channel_or_nan(&mut out, &frame.mrms_fields, 0, g2);
    let prate_index = if layout.mrms_field_ids.len() >= 3 {
        2
    } else {
        1
    };
    append_channel_or_nan(&mut out, &frame.mrms_fields, prate_index, g2);
    out
}

fn target_goes_c13_from_frame(
    frame: &MaterializedFrame,
    layout: &NativeDatasetMaterializerLayout,
) -> Vec<f32> {
    let g2 = layout.grid_size * layout.grid_size;
    let c13_index = layout
        .goes_channel_ids
        .iter()
        .position(|id| id.eq_ignore_ascii_case("C13"))
        .unwrap_or_else(|| layout.goes_channel_ids.len().saturating_sub(1));
    let mut out = Vec::with_capacity(g2);
    append_channel_or_nan(&mut out, &frame.goes_fields, c13_index, g2);
    out
}

fn current_refc_from_frame(
    frame: &MaterializedFrame,
    layout: &NativeDatasetMaterializerLayout,
) -> Vec<f32> {
    let g2 = layout.grid_size * layout.grid_size;
    let mut out = Vec::with_capacity(g2);
    append_channel_or_nan(&mut out, &frame.mrms_fields, 0, g2);
    out
}

fn initiation_from_refc(current_refc: &[f32], target_mrms: &[f32], grid_size: usize) -> Vec<f32> {
    let g2 = grid_size * grid_size;
    let target_refc = &target_mrms[..target_mrms.len().min(g2)];
    (0..g2)
        .map(|idx| {
            let current = current_refc.get(idx).copied().unwrap_or(f32::NAN);
            let target = target_refc.get(idx).copied().unwrap_or(f32::NAN);
            if current.is_finite()
                && target.is_finite()
                && current < REFLECTIVITY_INITIATION_THRESHOLD_DBZ
                && target >= REFLECTIVITY_INITIATION_THRESHOLD_DBZ
            {
                1.0
            } else {
                0.0
            }
        })
        .collect()
}

fn threshold_from_refc(target_mrms: &[f32], grid_size: usize, threshold: f32) -> Vec<f32> {
    let g2 = grid_size * grid_size;
    let target_refc = &target_mrms[..target_mrms.len().min(g2)];
    (0..g2)
        .map(|idx| {
            let target = target_refc.get(idx).copied().unwrap_or(f32::NAN);
            if target.is_finite() && target >= threshold {
                1.0
            } else {
                0.0
            }
        })
        .collect()
}

fn valid_mask_all_channels(values: &[f32], channel_count: usize, grid_size: usize) -> Vec<f32> {
    let g2 = grid_size * grid_size;
    if channel_count == 0 {
        return vec![0.0; g2];
    }
    (0..g2)
        .map(|idx| {
            let valid = (0..channel_count).all(|channel| {
                values
                    .get(channel * g2 + idx)
                    .copied()
                    .is_some_and(f32::is_finite)
            });
            if valid { 1.0 } else { 0.0 }
        })
        .collect()
}

fn valid_mask_any_channel(values: &[f32], channel_count: usize, grid_size: usize) -> Vec<f32> {
    let g2 = grid_size * grid_size;
    if channel_count == 0 {
        return vec![0.0; g2];
    }
    (0..g2)
        .map(|idx| {
            let valid = (0..channel_count).any(|channel| {
                values
                    .get(channel * g2 + idx)
                    .copied()
                    .is_some_and(f32::is_finite)
            });
            if valid { 1.0 } else { 0.0 }
        })
        .collect()
}

fn sanitize_mrms_channel(field_id: &str, values: &[f32]) -> Vec<f32> {
    let id = field_id.to_ascii_lowercase();
    values
        .iter()
        .map(|value| {
            if !value.is_finite() {
                return f32::NAN;
            }
            if id.contains("refc") || id.contains("reflect") || id.contains("llz") {
                if *value <= QUIET_REFLECTIVITY_DBZ {
                    QUIET_REFLECTIVITY_DBZ
                } else {
                    *value
                }
            } else if id.contains("az") || id.contains("rotation") || id.contains("mesh") {
                if *value <= -900.0 { 0.0 } else { *value }
            } else if id.contains("prate") || id.contains("precip") {
                if *value < 0.0 { 0.0 } else { *value }
            } else {
                *value
            }
        })
        .collect()
}

fn append_channel_or_nan(out: &mut Vec<f32>, values: &[f32], channel: usize, g2: usize) {
    let start = channel.saturating_mul(g2);
    let end = start.saturating_add(g2);
    if end <= values.len() {
        out.extend_from_slice(&values[start..end]);
    } else {
        out.extend(nan_vec(g2));
    }
}

fn nan_vec(len: usize) -> Vec<f32> {
    vec![f32::NAN; len]
}

fn find_nearest_goes_file(
    dir: &Path,
    target_time: DateTime<Utc>,
    max_delta_ms: i64,
    product_family: &str,
) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let expected_product = product_family.trim().to_ascii_uppercase();
    let mut best: Option<(i64, PathBuf)> = None;
    for path in list_regular_files(dir)? {
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Ok(parsed) = parse_goes_abi_filename(name) else {
            continue;
        };
        if !expected_product.is_empty()
            && !goes_filename_product_matches_request(&parsed.product, &expected_product)
        {
            continue;
        }
        let delta =
            (parsed.start_time_utc.timestamp_millis() - target_time.timestamp_millis()).abs();
        if delta > max_delta_ms {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|(best_delta, _)| delta < *best_delta)
        {
            best = Some((delta, path));
        }
    }
    Ok(best.map(|(_, path)| path))
}

fn goes_s3_prefix_product(product: &str) -> String {
    let trimmed = product.trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.ends_with("M1") || upper.ends_with("M2") {
        trimmed[..trimmed.len().saturating_sub(1)].to_string()
    } else {
        trimmed.to_string()
    }
}

fn goes_filename_product_matches_request(actual_product: &str, requested_product: &str) -> bool {
    let actual = actual_product.trim().to_ascii_uppercase();
    let requested = requested_product.trim().to_ascii_uppercase();
    actual == requested
        || (requested.ends_with('M')
            && (actual == format!("{requested}1") || actual == format!("{requested}2")))
}

fn find_nearest_mrms_file(
    dir: &Path,
    target_time: DateTime<Utc>,
    max_delta_ms: i64,
) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let mut best: Option<(i64, PathBuf)> = None;
    for path in list_regular_files(dir)? {
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(time) = parse_mrms_filename_time(name) else {
            continue;
        };
        let delta = (time.timestamp_millis() - target_time.timestamp_millis()).abs();
        if delta > max_delta_ms {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|(best_delta, _)| delta < *best_delta)
        {
            best = Some((delta, path));
        }
    }
    Ok(best.map(|(_, path)| path))
}

fn find_nearest_level2_file(
    dir: &Path,
    target_time: DateTime<Utc>,
    max_delta_ms: i64,
) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let mut best: Option<(i64, PathBuf)> = None;
    for path in list_regular_files(dir)? {
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(time) = parse_level2_object_name_scan_time(name) else {
            continue;
        };
        let delta = (time.timestamp_millis() - target_time.timestamp_millis()).abs();
        if delta > max_delta_ms {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|(best_delta, _)| delta < *best_delta)
        {
            best = Some((delta, path));
        }
    }
    Ok(best.map(|(_, path)| path))
}

fn list_regular_files(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() {
            out.push(path);
        }
    }
    Ok(out)
}

fn parse_mrms_filename_time(name: &str) -> Option<DateTime<Utc>> {
    let bytes = name.as_bytes();
    for idx in 0..bytes.len().saturating_sub(15) {
        if !bytes[idx..idx + 8].iter().all(u8::is_ascii_digit) {
            continue;
        }
        if bytes.get(idx + 8) != Some(&b'-') {
            continue;
        }
        if !bytes[idx + 9..idx + 15].iter().all(u8::is_ascii_digit) {
            continue;
        }
        let stamp = &name[idx..idx + 15];
        let parsed = chrono::NaiveDateTime::parse_from_str(stamp, "%Y%m%d-%H%M%S").ok()?;
        return Some(parsed.and_utc());
    }
    None
}

fn radar_grid_spec_for_tile(
    tile: &NativeDatasetTile,
    grid_size: usize,
) -> Result<CartesianGridSpec, Box<dyn Error>> {
    if grid_size == 0 {
        return Err(boxed_error("radar tile grid size must be non-zero"));
    }
    let cos_lat = tile.center_lat.to_radians().cos().abs().max(0.01);
    let width_m = (tile.bounds.east - tile.bounds.west).abs() * 111_139.0 * cos_lat;
    let height_m = (tile.bounds.north - tile.bounds.south).abs() * 111_139.0;
    let span_m = width_m.max(height_m).max(1.0);
    let resolution_m = if grid_size > 1 {
        span_m / (grid_size - 1) as f64
    } else {
        span_m
    };
    let origin = -0.5 * resolution_m * grid_size.saturating_sub(1) as f64;
    Ok(CartesianGridSpec {
        nx: grid_size as u32,
        ny: grid_size as u32,
        center_lat: tile.center_lat,
        center_lon: tile.center_lon,
        resolution_m,
        x_origin_m: origin,
        y_origin_m: origin,
        projection: "local_tangent_cartesian_m".to_string(),
    })
}

fn mrms_product_prefix(field_id: &str) -> Option<&'static str> {
    match field_id {
        "refc" | "reflectivity" => Some("CONUS/MergedReflectivityQCComposite_00.50"),
        "llz" | "low_level_reflectivity" => Some("CONUS/MergedReflectivityQC_00.50"),
        "prate" | "precip_rate" => Some("CONUS/PrecipRate_00.00"),
        "mesh" => Some("CONUS/MESH"),
        "azshear_0_2km" | "rotation" => Some("CONUS/MergedAzShear_0-2kmAGL"),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct PublicS3Object {
    key: String,
}

fn list_public_s3(base_url: &str, prefix: &str) -> Result<Vec<PublicS3Object>, Box<dyn Error>> {
    let agent = build_agent();
    let url = format!("{base_url}?list-type=2&prefix={prefix}");
    let mut response = agent.get(&url).call()?;
    let xml = response.body_mut().read_to_string()?;
    Ok(parse_s3_list_xml(&xml))
}

fn download_public_s3_object(base_url: &str, key: &str, path: &Path) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        return Ok(());
    }
    let agent = build_agent();
    let url = format!("{base_url}/{key}");
    let mut response = agent.get(&url).call()?;
    let bytes = response
        .body_mut()
        .with_config()
        .limit(2 * 1024 * 1024 * 1024)
        .read_to_vec()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    Ok(())
}

fn parse_s3_list_xml(xml: &str) -> Vec<PublicS3Object> {
    let mut objects = Vec::new();
    for contents in xml.split("<Contents>").skip(1) {
        let end = contents.find("</Contents>").unwrap_or(contents.len());
        let block = &contents[..end];
        let Some(key) = extract_xml_tag(block, "Key") else {
            continue;
        };
        if key.is_empty() || key.ends_with('/') {
            continue;
        }
        objects.push(PublicS3Object { key });
    }
    objects
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn maybe_decompress_gzip(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() < 2 || bytes[0] != 0x1f || bytes[1] != 0x8b {
        return bytes;
    }
    let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
    let mut out = Vec::new();
    match decoder.read_to_end(&mut out) {
        Ok(_) if !out.is_empty() => out,
        _ => bytes,
    }
}

fn build_agent() -> ureq::Agent {
    rustls::crypto::CryptoProvider::install_default(rustls_rustcrypto::provider()).ok();
    let crypto = std::sync::Arc::new(rustls_rustcrypto::provider());
    ureq::Agent::config_builder()
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .provider(ureq::tls::TlsProvider::Rustls)
                .root_certs(ureq::tls::RootCerts::WebPki)
                .unversioned_rustls_crypto_provider(crypto)
                .build(),
        )
        .build()
        .new_agent()
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        message.into(),
    ))
}

trait DateTimeHourExt {
    fn hour(&self) -> u32;
}

impl DateTimeHourExt for DateTime<Utc> {
    fn hour(&self) -> u32 {
        chrono::Timelike::hour(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_dataset::NativeHourProcessor;
    use crate::native_dataset::{
        NativeDatasetBounds, NativeDatasetBuildConfig, NativeDatasetCase, NativeDatasetShardSpec,
        NativeDatasetTile, plan_native_dataset,
    };

    fn test_plan() -> NativeDatasetPlan {
        let case = NativeDatasetCase::new(
            "case",
            DateTime::parse_from_rfc3339("2024-05-06T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            4,
        );
        let tile = NativeDatasetTile::new(
            "tile",
            35.0,
            -97.0,
            NativeDatasetBounds::new(-97.1, -96.9, 34.9, 35.1),
        );
        let mut config = NativeDatasetBuildConfig::hrrr_multisource_v1(
            "materializer_test",
            vec![case],
            vec![tile],
        );
        config.grid_size = 4;
        plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap()
    }

    #[test]
    fn materializer_manifest_matches_plan_shapes() {
        let plan = test_plan();
        let layout = NativeDatasetMaterializerLayout::from_plan(&plan).unwrap();
        assert_eq!(layout.goes_product_family, "ABI-L2-MCMIPC");
        let manifest = training_shard_manifest_for_plan(&plan, &layout).unwrap();
        assert_eq!(
            manifest.tensor("hrrr_fields").unwrap().shape,
            vec![10, 4, 4]
        );
        assert_eq!(
            manifest.tensor("mrms_hist").unwrap().shape,
            vec![3, 3, 4, 4]
        );
        assert_eq!(
            manifest.tensor("goes_hist").unwrap().shape,
            vec![3, 8, 4, 4]
        );
        assert_eq!(
            manifest.tensor("target_initiation").unwrap().shape,
            vec![1, 4, 4]
        );
        assert_eq!(
            manifest.tensor("hrrr_valid_mask").unwrap().shape,
            vec![1, 4, 4]
        );
        assert_eq!(
            manifest.tensor("mrms_hist_valid_mask").unwrap().shape,
            vec![3, 1, 4, 4]
        );
        assert_eq!(
            manifest.tensor("target_refc_ge35").unwrap().shape,
            vec![1, 4, 4]
        );
    }

    #[test]
    fn materializer_layout_preserves_goes_product_family() {
        let case = NativeDatasetCase::new(
            "case",
            DateTime::parse_from_rfc3339("2024-05-06T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            4,
        );
        let tile = NativeDatasetTile::new(
            "tile",
            35.0,
            -97.0,
            NativeDatasetBounds::new(-97.1, -96.9, 34.9, 35.1),
        );
        let mut config =
            NativeDatasetBuildConfig::hrrr_multisource_v1("goes_meso", vec![case], vec![tile]);
        for source in &mut config.sources {
            if source.kind == NativeDatasetSourceKind::GoesNetcdf {
                source.product_family = Some("ABI-L2-MCMIPM1".to_string());
            }
        }
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let layout = NativeDatasetMaterializerLayout::from_plan(&plan).unwrap();
        assert_eq!(layout.goes_product_family, "ABI-L2-MCMIPM1");
    }

    #[test]
    fn goes_mesoscale_product_family_uses_shared_s3_prefix() {
        assert_eq!(goes_s3_prefix_product("ABI-L2-MCMIPM1"), "ABI-L2-MCMIPM");
        assert!(goes_filename_product_matches_request(
            "ABI-L2-MCMIPM1",
            "ABI-L2-MCMIPM1"
        ));
        assert!(!goes_filename_product_matches_request(
            "ABI-L2-MCMIPM2",
            "ABI-L2-MCMIPM1"
        ));
        assert!(goes_filename_product_matches_request(
            "ABI-L2-MCMIPM2",
            "ABI-L2-MCMIPM"
        ));
    }

    #[test]
    fn materializer_can_emit_nan_shard_when_sources_are_missing() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_native_materializer_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let plan = test_plan();
        let config = NativeDatasetMaterializerConfig::new(root.join("cache"), root.join("shard"))
            .with_source_root(root.join("raw"))
            .with_missing_policy(NativeMaterializerMissingPolicy::FillNan);
        let mut materializer = NativeDatasetMaterializer::create(&plan, config).unwrap();
        let hour_jobs = crate::native_dataset::build_native_dataset_hour_jobs(&plan).unwrap();
        let mut emitted = 0usize;
        for job in &hour_jobs {
            emitted += materializer
                .process_hour(&plan, job)
                .unwrap()
                .samples_emitted;
        }
        let manifest = materializer.finish().unwrap();
        assert_eq!(emitted, plan.expected_samples);
        assert_eq!(manifest.sample_count, plan.expected_samples as u64);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nearest_level2_file_respects_time_window() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_level2_window_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("KFDR20240506_180007_V06"), b"not-a-real-volume").unwrap();
        let target = DateTime::parse_from_rfc3339("2024-05-06T19:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let stale = find_nearest_level2_file(&root, target, 10 * 60 * 1000).unwrap();
        let loose = find_nearest_level2_file(&root, target, 70 * 60 * 1000).unwrap();

        assert!(stale.is_none());
        assert_eq!(
            loose.unwrap().file_name().unwrap(),
            "KFDR20240506_180007_V06"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn mrms_sentinel_sanitization_preserves_quiet_weather_semantics() {
        let refc = sanitize_mrms_channel("refc", &[-99.0, -20.0, 36.0, f32::NAN]);
        assert_eq!(refc[0], QUIET_REFLECTIVITY_DBZ);
        assert_eq!(refc[1], -20.0);
        assert_eq!(refc[2], 36.0);
        assert!(refc[3].is_nan());

        let low_level_reflectivity = sanitize_mrms_channel("llz", &[-999.0, -99.0, 2.5]);
        assert_eq!(
            low_level_reflectivity,
            vec![QUIET_REFLECTIVITY_DBZ, QUIET_REFLECTIVITY_DBZ, 2.5]
        );

        let rotation = sanitize_mrms_channel("azshear_0_2km", &[-999.0, -99.0, 2.5]);
        assert_eq!(rotation, vec![0.0, -99.0, 2.5]);

        let prate = sanitize_mrms_channel("prate", &[-1.0, 0.0, 4.0]);
        assert_eq!(prate, vec![0.0, 0.0, 4.0]);
    }

    #[test]
    fn validity_masks_track_finite_source_coverage() {
        let values = vec![1.0, 2.0, f32::NAN, 4.0, 10.0, f32::NAN, 30.0, 40.0];
        assert_eq!(
            valid_mask_all_channels(&values, 2, 2),
            vec![1.0, 0.0, 0.0, 1.0]
        );
        assert_eq!(
            valid_mask_any_channel(&values, 2, 2),
            vec![1.0, 1.0, 1.0, 1.0]
        );
    }

    #[test]
    fn reflectivity_threshold_target_uses_sanitized_quiet_values() {
        let target = vec![QUIET_REFLECTIVITY_DBZ, 34.9, 35.0, 60.0];
        assert_eq!(
            threshold_from_refc(&target, 2, REFLECTIVITY_INITIATION_THRESHOLD_DBZ),
            vec![0.0, 0.0, 1.0, 1.0]
        );
    }
}
