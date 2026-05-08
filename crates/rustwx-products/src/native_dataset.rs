//! Rust-native dataset orchestration primitives.
//!
//! This module is intentionally source-agnostic: it describes the case/tile/time
//! job graph, shard ownership, source families, cache policy, and expected row
//! counts without depending on Python-side orchestration. Dataset runners can
//! plug rustwx GRIB, NetCDF/HDF5, radar, and volume-store decoders into the
//! generated jobs.

use chrono::{DateTime, Duration, Utc};
use rustwx_core::ModelId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

pub const NATIVE_DATASET_PLAN_SCHEMA: &str = "rustwx.native_dataset_plan.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeDatasetSourceKind {
    ModelGrib,
    MrmsGrib,
    GoesNetcdf,
    RadarLevel2,
    LocalNetcdf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetSource {
    pub id: String,
    pub kind: NativeDatasetSourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_family: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
}

impl NativeDatasetSource {
    pub fn hrrr_surface(fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            id: "hrrr_wrfsfc".to_string(),
            kind: NativeDatasetSourceKind::ModelGrib,
            model: Some(ModelId::Hrrr),
            product_family: Some("wrfsfc".to_string()),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }

    pub fn mrms(fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            id: "mrms".to_string(),
            kind: NativeDatasetSourceKind::MrmsGrib,
            model: None,
            product_family: Some("mrms".to_string()),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }

    pub fn goes_abi(channels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            id: "goes_abi".to_string(),
            kind: NativeDatasetSourceKind::GoesNetcdf,
            model: None,
            product_family: Some("ABI-L2-MCMIPC".to_string()),
            fields: channels.into_iter().map(Into::into).collect(),
        }
    }

    pub fn nexrad_level2() -> Self {
        Self::nexrad_level2_products(["reflectivity", "velocity"])
    }

    pub fn nexrad_level2_products(products: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            id: "nexrad_level2".to_string(),
            kind: NativeDatasetSourceKind::RadarLevel2,
            model: None,
            product_family: Some("level2".to_string()),
            fields: products.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NativeDatasetBounds {
    pub west: f64,
    pub east: f64,
    pub south: f64,
    pub north: f64,
}

impl NativeDatasetBounds {
    pub fn new(west: f64, east: f64, south: f64, north: f64) -> Self {
        Self {
            west,
            east,
            south,
            north,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeDatasetTile {
    pub tile_id: String,
    pub center_lat: f64,
    pub center_lon: f64,
    pub bounds: NativeDatasetBounds,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub radar_site: Option<String>,
}

impl NativeDatasetTile {
    pub fn new(
        tile_id: impl Into<String>,
        center_lat: f64,
        center_lon: f64,
        bounds: NativeDatasetBounds,
    ) -> Self {
        Self {
            tile_id: tile_id.into(),
            center_lat,
            center_lon,
            bounds,
            radar_site: None,
        }
    }

    pub fn with_radar_site(mut self, radar_site: impl Into<String>) -> Self {
        self.radar_site = Some(radar_site.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetCase {
    pub case_id: String,
    pub start_utc: DateTime<Utc>,
    pub hours: u16,
}

impl NativeDatasetCase {
    pub fn new(case_id: impl Into<String>, start_utc: DateTime<Utc>, hours: u16) -> Self {
        Self {
            case_id: case_id.into(),
            start_utc,
            hours,
        }
    }

    pub fn frame_times(&self, stride_hours: u16) -> Vec<DateTime<Utc>> {
        let stride = stride_hours.max(1);
        let mut times = Vec::new();
        let mut current = self.start_utc;
        let end = self.start_utc + Duration::hours(i64::from(self.hours));
        while current <= end {
            times.push(current);
            current += Duration::hours(i64::from(stride));
        }
        times
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeDatasetOutputFormat {
    Npz,
    VolumeStore,
    Arrow,
    RawF32TrainingShard,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeDatasetBuildConfig {
    pub dataset_name: String,
    pub cases: Vec<NativeDatasetCase>,
    pub tiles: Vec<NativeDatasetTile>,
    pub sources: Vec<NativeDatasetSource>,
    pub stride_hours: u16,
    pub history_steps: u16,
    pub forecast_step_frames: u16,
    pub grid_size: u16,
    pub output_format: NativeDatasetOutputFormat,
}

impl NativeDatasetBuildConfig {
    pub fn hrrr_multisource_v1(
        dataset_name: impl Into<String>,
        cases: Vec<NativeDatasetCase>,
        tiles: Vec<NativeDatasetTile>,
    ) -> Self {
        Self {
            dataset_name: dataset_name.into(),
            cases,
            tiles,
            sources: vec![
                NativeDatasetSource::hrrr_surface([
                    "t2m", "d2m", "u10", "v10", "cape", "cin", "refc", "mslp", "terrain", "pwat",
                ]),
                NativeDatasetSource::mrms(["refc", "llz", "prate"]),
                NativeDatasetSource::goes_abi([
                    "C01", "C02", "C03", "C07", "C08", "C09", "C10", "C13",
                ]),
                NativeDatasetSource::nexrad_level2(),
            ],
            stride_hours: 1,
            history_steps: 3,
            forecast_step_frames: 1,
            grid_size: 512,
            output_format: NativeDatasetOutputFormat::RawF32TrainingShard,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.dataset_name.trim().is_empty() {
            return Err("dataset_name cannot be empty".to_string());
        }
        if self.cases.is_empty() {
            return Err("at least one case is required".to_string());
        }
        if self.tiles.is_empty() {
            return Err("at least one tile is required".to_string());
        }
        if self.sources.is_empty() {
            return Err("at least one source is required".to_string());
        }
        if self.stride_hours == 0 {
            return Err("stride_hours must be >= 1".to_string());
        }
        if self.history_steps == 0 {
            return Err("history_steps must be >= 1".to_string());
        }
        if self.forecast_step_frames == 0 {
            return Err("forecast_step_frames must be >= 1".to_string());
        }
        Ok(())
    }

    pub fn samples_per_complete_tile_case(&self, case: &NativeDatasetCase) -> usize {
        let frame_count = case.frame_times(self.stride_hours).len();
        let required_window = usize::from(self.history_steps.saturating_sub(1))
            + usize::from(self.forecast_step_frames);
        frame_count.saturating_sub(required_window)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetShardSpec {
    pub shard_index: usize,
    pub shard_count: usize,
}

impl NativeDatasetShardSpec {
    pub fn new(shard_index: usize, shard_count: usize) -> Result<Self, String> {
        if shard_count == 0 {
            return Err("shard_count must be >= 1".to_string());
        }
        if shard_index >= shard_count {
            return Err(format!(
                "shard_index {shard_index} must be less than shard_count {shard_count}"
            ));
        }
        Ok(Self {
            shard_index,
            shard_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetFrameJob {
    pub job_id: String,
    pub case_id: String,
    pub tile_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub source_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSourceKey {
    pub source_id: String,
    pub kind: NativeDatasetSourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_family: Option<String>,
}

impl From<&NativeDatasetSource> for NativeSourceKey {
    fn from(source: &NativeDatasetSource) -> Self {
        Self {
            source_id: source.id.clone(),
            kind: source.kind,
            model: source.model.clone(),
            product_family: source.product_family.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeHourBundle {
    pub case_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub source_keys: Vec<NativeSourceKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeTileBatchJob {
    pub frame_job_id: String,
    pub case_id: String,
    pub tile_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub source_ids: Vec<String>,
}

impl From<&NativeDatasetFrameJob> for NativeTileBatchJob {
    fn from(job: &NativeDatasetFrameJob) -> Self {
        Self {
            frame_job_id: job.job_id.clone(),
            case_id: job.case_id.clone(),
            tile_id: job.tile_id.clone(),
            valid_time_utc: job.valid_time_utc,
            source_ids: job.source_ids.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetHourJob {
    pub job_id: String,
    pub case_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub bundle: NativeHourBundle,
    pub tile_jobs: Vec<NativeTileBatchJob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetSampleWindow {
    pub sample_id: String,
    pub case_id: String,
    pub tile_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub history_frame_times_utc: Vec<DateTime<Utc>>,
    pub target_time_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeDatasetShardPlan {
    pub shard_index: usize,
    pub shard_count: usize,
    pub tiles: Vec<NativeDatasetTile>,
    pub frame_jobs: Vec<NativeDatasetFrameJob>,
    pub sample_windows: Vec<NativeDatasetSampleWindow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeDatasetPlan {
    pub schema_version: String,
    pub dataset_name: String,
    pub generated_at_utc: DateTime<Utc>,
    pub config: NativeDatasetBuildConfig,
    pub shard: NativeDatasetShardPlan,
    pub expected_frame_jobs: usize,
    pub expected_samples: usize,
}

impl NativeDatasetPlan {
    pub fn write_json(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    pub fn required_source_ids(&self) -> BTreeSet<&str> {
        self.config
            .sources
            .iter()
            .map(|source| source.id.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetRunnerConfig {
    pub max_attempts: u16,
    pub continue_on_error: bool,
}

impl Default for NativeDatasetRunnerConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            continue_on_error: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeFrameOutput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
}

impl NativeFrameOutput {
    pub fn empty() -> Self {
        Self {
            artifacts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeHourOutput {
    pub samples_emitted: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
}

impl NativeHourOutput {
    pub fn empty() -> Self {
        Self {
            samples_emitted: 0,
            artifacts: Vec::new(),
        }
    }
}

pub trait NativeFrameProcessor {
    fn process_frame(
        &mut self,
        plan: &NativeDatasetPlan,
        job: &NativeDatasetFrameJob,
    ) -> Result<NativeFrameOutput, String>;
}

pub trait NativeHourProcessor {
    fn process_hour(
        &mut self,
        plan: &NativeDatasetPlan,
        job: &NativeDatasetHourJob,
    ) -> Result<NativeHourOutput, String>;
}

#[derive(Debug, Default)]
pub struct NativeDryRunHourProcessor;

impl NativeHourProcessor for NativeDryRunHourProcessor {
    fn process_hour(
        &mut self,
        plan: &NativeDatasetPlan,
        job: &NativeDatasetHourJob,
    ) -> Result<NativeHourOutput, String> {
        let samples_emitted = plan
            .shard
            .sample_windows
            .iter()
            .filter(|sample| {
                sample.case_id == job.case_id && sample.valid_time_utc == job.valid_time_utc
            })
            .count();
        Ok(NativeHourOutput {
            samples_emitted,
            artifacts: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeDatasetJobStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetJobReport {
    pub job_id: String,
    pub case_id: String,
    pub tile_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub attempts: u16,
    pub status: NativeDatasetJobStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetRunReport {
    pub schema_version: String,
    pub dataset_name: String,
    pub started_at_utc: DateTime<Utc>,
    pub finished_at_utc: DateTime<Utc>,
    pub attempted_frame_jobs: usize,
    pub succeeded_frame_jobs: usize,
    pub failed_frame_jobs: usize,
    pub jobs: Vec<NativeDatasetJobReport>,
}

impl NativeDatasetRunReport {
    pub fn write_json(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(path, bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetProgress {
    pub schema_version: String,
    pub dataset_name: String,
    pub started_at_utc: DateTime<Utc>,
    pub updated_at_utc: DateTime<Utc>,
    pub elapsed_ms: u64,
    pub hours_total: usize,
    pub hours_completed: usize,
    pub tile_frames_total: usize,
    pub tile_frames_completed: usize,
    pub samples_emitted: usize,
    pub failed_hours: usize,
    pub failed_tile_frames: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetHourJobReport {
    pub job_id: String,
    pub case_id: String,
    pub valid_time_utc: DateTime<Utc>,
    pub attempts: u16,
    pub status: NativeDatasetJobStatus,
    pub tile_frames: usize,
    pub samples_emitted: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDatasetHourRunReport {
    pub schema_version: String,
    pub dataset_name: String,
    pub started_at_utc: DateTime<Utc>,
    pub finished_at_utc: DateTime<Utc>,
    pub progress: NativeDatasetProgress,
    pub jobs: Vec<NativeDatasetHourJobReport>,
}

impl NativeDatasetHourRunReport {
    pub fn write_json(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(path, bytes)?;
        Ok(())
    }
}

pub fn run_native_dataset_plan<P: NativeFrameProcessor>(
    plan: &NativeDatasetPlan,
    config: &NativeDatasetRunnerConfig,
    processor: &mut P,
) -> Result<NativeDatasetRunReport, String> {
    let started_at_utc = Utc::now();
    let max_attempts = config.max_attempts.max(1);
    let mut jobs = Vec::with_capacity(plan.shard.frame_jobs.len());
    let mut succeeded = 0usize;
    let mut failed = 0usize;

    for job in &plan.shard.frame_jobs {
        let mut last_error = None;
        let mut final_output = None;
        let mut attempts = 0u16;
        for attempt in 1..=max_attempts {
            attempts = attempt;
            match processor.process_frame(plan, job) {
                Ok(output) => {
                    final_output = Some(output);
                    break;
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        if let Some(output) = final_output {
            succeeded += 1;
            jobs.push(NativeDatasetJobReport {
                job_id: job.job_id.clone(),
                case_id: job.case_id.clone(),
                tile_id: job.tile_id.clone(),
                valid_time_utc: job.valid_time_utc,
                attempts,
                status: NativeDatasetJobStatus::Succeeded,
                artifacts: output.artifacts,
                error: None,
            });
        } else {
            failed += 1;
            let error = last_error.unwrap_or_else(|| "frame processor failed".to_string());
            jobs.push(NativeDatasetJobReport {
                job_id: job.job_id.clone(),
                case_id: job.case_id.clone(),
                tile_id: job.tile_id.clone(),
                valid_time_utc: job.valid_time_utc,
                attempts,
                status: NativeDatasetJobStatus::Failed,
                artifacts: Vec::new(),
                error: Some(error.clone()),
            });
            if !config.continue_on_error {
                return Err(format!(
                    "native dataset frame job {} failed: {error}",
                    job.job_id
                ));
            }
        }
    }

    Ok(NativeDatasetRunReport {
        schema_version: "rustwx.native_dataset_run_report.v1".to_string(),
        dataset_name: plan.dataset_name.clone(),
        started_at_utc,
        finished_at_utc: Utc::now(),
        attempted_frame_jobs: plan.shard.frame_jobs.len(),
        succeeded_frame_jobs: succeeded,
        failed_frame_jobs: failed,
        jobs,
    })
}

pub fn build_native_dataset_hour_jobs(
    plan: &NativeDatasetPlan,
) -> Result<Vec<NativeDatasetHourJob>, String> {
    let source_by_id = plan
        .config
        .sources
        .iter()
        .map(|source| (source.id.clone(), NativeSourceKey::from(source)))
        .collect::<BTreeMap<_, _>>();
    let mut grouped = BTreeMap::<(String, DateTime<Utc>), Vec<&NativeDatasetFrameJob>>::new();
    for job in &plan.shard.frame_jobs {
        grouped
            .entry((job.case_id.clone(), job.valid_time_utc))
            .or_default()
            .push(job);
    }

    let mut hour_jobs = Vec::with_capacity(grouped.len());
    for ((case_id, valid_time_utc), frame_jobs) in grouped {
        let mut source_ids = BTreeSet::new();
        let mut tile_jobs = Vec::with_capacity(frame_jobs.len());
        for frame_job in frame_jobs {
            for source_id in &frame_job.source_ids {
                source_ids.insert(source_id.clone());
            }
            tile_jobs.push(NativeTileBatchJob::from(frame_job));
        }

        let mut source_keys = Vec::with_capacity(source_ids.len());
        for source_id in source_ids {
            let source_key = source_by_id.get(&source_id).cloned().ok_or_else(|| {
                format!(
                    "frame jobs reference source {source_id}, but it is absent from plan config"
                )
            })?;
            source_keys.push(source_key);
        }

        let job_id = format!("{}_{}", case_id, valid_time_utc.format("%Y%m%d_%H%M"));
        let bundle = NativeHourBundle {
            case_id: case_id.clone(),
            valid_time_utc,
            source_keys,
        };
        hour_jobs.push(NativeDatasetHourJob {
            job_id,
            case_id,
            valid_time_utc,
            bundle,
            tile_jobs,
        });
    }
    Ok(hour_jobs)
}

pub fn run_native_dataset_hour_plan<P: NativeHourProcessor>(
    plan: &NativeDatasetPlan,
    config: &NativeDatasetRunnerConfig,
    processor: &mut P,
) -> Result<NativeDatasetHourRunReport, String> {
    run_native_dataset_hour_plan_with_progress(plan, config, processor, |_| {})
}

pub fn run_native_dataset_hour_plan_with_progress<P, F>(
    plan: &NativeDatasetPlan,
    config: &NativeDatasetRunnerConfig,
    processor: &mut P,
    mut on_progress: F,
) -> Result<NativeDatasetHourRunReport, String>
where
    P: NativeHourProcessor,
    F: FnMut(&NativeDatasetProgress),
{
    let started_at_utc = Utc::now();
    let max_attempts = config.max_attempts.max(1);
    let hour_jobs = build_native_dataset_hour_jobs(plan)?;
    let mut progress = NativeDatasetProgress {
        schema_version: "rustwx.native_dataset_progress.v1".to_string(),
        dataset_name: plan.dataset_name.clone(),
        started_at_utc,
        updated_at_utc: started_at_utc,
        elapsed_ms: 0,
        hours_total: hour_jobs.len(),
        hours_completed: 0,
        tile_frames_total: plan.shard.frame_jobs.len(),
        tile_frames_completed: 0,
        samples_emitted: 0,
        failed_hours: 0,
        failed_tile_frames: 0,
    };
    on_progress(&progress);

    let mut reports = Vec::with_capacity(hour_jobs.len());
    for hour_job in &hour_jobs {
        let mut last_error = None;
        let mut final_output = None;
        let mut attempts = 0u16;
        for attempt in 1..=max_attempts {
            attempts = attempt;
            match processor.process_hour(plan, hour_job) {
                Ok(output) => {
                    final_output = Some(output);
                    break;
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        if let Some(output) = final_output {
            progress.hours_completed += 1;
            progress.tile_frames_completed += hour_job.tile_jobs.len();
            progress.samples_emitted += output.samples_emitted;
            reports.push(NativeDatasetHourJobReport {
                job_id: hour_job.job_id.clone(),
                case_id: hour_job.case_id.clone(),
                valid_time_utc: hour_job.valid_time_utc,
                attempts,
                status: NativeDatasetJobStatus::Succeeded,
                tile_frames: hour_job.tile_jobs.len(),
                samples_emitted: output.samples_emitted,
                artifacts: output.artifacts,
                error: None,
            });
        } else {
            let error = last_error.unwrap_or_else(|| "hour processor failed".to_string());
            progress.failed_hours += 1;
            progress.failed_tile_frames += hour_job.tile_jobs.len();
            reports.push(NativeDatasetHourJobReport {
                job_id: hour_job.job_id.clone(),
                case_id: hour_job.case_id.clone(),
                valid_time_utc: hour_job.valid_time_utc,
                attempts,
                status: NativeDatasetJobStatus::Failed,
                tile_frames: hour_job.tile_jobs.len(),
                samples_emitted: 0,
                artifacts: Vec::new(),
                error: Some(error.clone()),
            });
            update_native_dataset_progress_timing(&mut progress, started_at_utc);
            on_progress(&progress);
            if !config.continue_on_error {
                return Err(format!(
                    "native dataset hour job {} failed: {error}",
                    hour_job.job_id
                ));
            }
            continue;
        }

        update_native_dataset_progress_timing(&mut progress, started_at_utc);
        on_progress(&progress);
    }

    let finished_at_utc = Utc::now();
    progress.updated_at_utc = finished_at_utc;
    progress.elapsed_ms = finished_at_utc
        .signed_duration_since(started_at_utc)
        .num_milliseconds()
        .max(0) as u64;

    Ok(NativeDatasetHourRunReport {
        schema_version: "rustwx.native_dataset_hour_run_report.v1".to_string(),
        dataset_name: plan.dataset_name.clone(),
        started_at_utc,
        finished_at_utc,
        progress,
        jobs: reports,
    })
}

fn update_native_dataset_progress_timing(
    progress: &mut NativeDatasetProgress,
    started_at_utc: DateTime<Utc>,
) {
    let updated_at_utc = Utc::now();
    progress.updated_at_utc = updated_at_utc;
    progress.elapsed_ms = updated_at_utc
        .signed_duration_since(started_at_utc)
        .num_milliseconds()
        .max(0) as u64;
}

pub fn plan_native_dataset(
    config: NativeDatasetBuildConfig,
    shard: NativeDatasetShardSpec,
) -> Result<NativeDatasetPlan, String> {
    config.validate()?;
    let source_ids = config
        .sources
        .iter()
        .map(|source| source.id.clone())
        .collect::<Vec<_>>();
    let tiles = config
        .tiles
        .iter()
        .enumerate()
        .filter(|(idx, _)| idx % shard.shard_count == shard.shard_index)
        .map(|(_, tile)| tile.clone())
        .collect::<Vec<_>>();

    let mut frame_jobs = Vec::new();
    let mut sample_windows = Vec::new();
    for case in &config.cases {
        let frame_times = case.frame_times(config.stride_hours);
        for tile in &tiles {
            for valid_time in &frame_times {
                frame_jobs.push(NativeDatasetFrameJob {
                    job_id: format!(
                        "{}_{}_{}",
                        case.case_id,
                        tile.tile_id,
                        valid_time.format("%Y%m%d_%H%M")
                    ),
                    case_id: case.case_id.clone(),
                    tile_id: tile.tile_id.clone(),
                    valid_time_utc: *valid_time,
                    source_ids: source_ids.clone(),
                });
            }

            let history_steps = usize::from(config.history_steps);
            let target_step = usize::from(config.forecast_step_frames);
            if frame_times.len() >= history_steps + target_step {
                for idx in history_steps - 1..frame_times.len() - target_step {
                    let valid_time = frame_times[idx];
                    let history_start = idx + 1 - history_steps;
                    sample_windows.push(NativeDatasetSampleWindow {
                        sample_id: format!(
                            "{}_{}_{}",
                            case.case_id,
                            tile.tile_id,
                            valid_time.format("%Y%m%d_%H%M")
                        ),
                        case_id: case.case_id.clone(),
                        tile_id: tile.tile_id.clone(),
                        valid_time_utc: valid_time,
                        history_frame_times_utc: frame_times[history_start..=idx].to_vec(),
                        target_time_utc: frame_times[idx + target_step],
                    });
                }
            }
        }
    }

    let expected_frame_jobs = frame_jobs.len();
    let expected_samples = sample_windows.len();
    Ok(NativeDatasetPlan {
        schema_version: NATIVE_DATASET_PLAN_SCHEMA.to_string(),
        dataset_name: config.dataset_name.clone(),
        generated_at_utc: Utc::now(),
        config,
        shard: NativeDatasetShardPlan {
            shard_index: shard.shard_index,
            shard_count: shard.shard_count,
            tiles,
            frame_jobs,
            sample_windows,
        },
        expected_frame_jobs,
        expected_samples,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case() -> NativeDatasetCase {
        NativeDatasetCase::new(
            "case",
            DateTime::parse_from_rfc3339("2024-05-06T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            24,
        )
    }

    fn tiles(n: usize) -> Vec<NativeDatasetTile> {
        (0..n)
            .map(|idx| {
                NativeDatasetTile::new(
                    format!("tile_{idx:02}"),
                    35.0,
                    -97.0 + idx as f64,
                    NativeDatasetBounds::new(-98.0, -96.0, 34.0, 36.0),
                )
            })
            .collect()
    }

    #[test]
    fn case_frame_times_include_both_endpoints() {
        let times = case().frame_times(1);
        assert_eq!(times.len(), 25);
        assert_eq!(
            times.first().unwrap().to_rfc3339(),
            "2024-05-06T12:00:00+00:00"
        );
        assert_eq!(
            times.last().unwrap().to_rfc3339(),
            "2024-05-07T12:00:00+00:00"
        );
    }

    #[test]
    fn one_day_three_history_one_target_yields_twenty_two_samples_per_tile() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
        assert_eq!(config.samples_per_complete_tile_case(&config.cases[0]), 22);
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        assert_eq!(plan.expected_frame_jobs, 25);
        assert_eq!(plan.expected_samples, 22);
        assert_eq!(
            plan.shard.sample_windows[0].history_frame_times_utc.len(),
            3
        );
        assert_eq!(
            plan.shard.sample_windows[0].valid_time_utc.to_rfc3339(),
            "2024-05-06T14:00:00+00:00"
        );
        assert_eq!(
            plan.shard.sample_windows[0].target_time_utc.to_rfc3339(),
            "2024-05-06T15:00:00+00:00"
        );
    }

    #[test]
    fn shard_spec_assigns_disjoint_tile_subsets() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(5));
        let shard0 =
            plan_native_dataset(config.clone(), NativeDatasetShardSpec::new(0, 2).unwrap())
                .unwrap();
        let shard1 =
            plan_native_dataset(config, NativeDatasetShardSpec::new(1, 2).unwrap()).unwrap();
        let ids0 = shard0
            .shard
            .tiles
            .iter()
            .map(|tile| tile.tile_id.as_str())
            .collect::<Vec<_>>();
        let ids1 = shard1
            .shard
            .tiles
            .iter()
            .map(|tile| tile.tile_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids0, vec!["tile_00", "tile_02", "tile_04"]);
        assert_eq!(ids1, vec!["tile_01", "tile_03"]);
        assert_eq!(shard0.expected_samples + shard1.expected_samples, 5 * 22);
    }

    #[test]
    fn default_hrrr_multisource_plan_declares_real_source_families() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let sources = plan.required_source_ids();
        assert!(sources.contains("hrrr_wrfsfc"));
        assert!(sources.contains("mrms"));
        assert!(sources.contains("goes_abi"));
        assert!(sources.contains("nexrad_level2"));
    }

    #[test]
    fn hour_jobs_group_frame_jobs_by_case_and_valid_time() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(2));
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let hour_jobs = build_native_dataset_hour_jobs(&plan).unwrap();

        assert_eq!(hour_jobs.len(), 25);
        assert_eq!(
            hour_jobs[0].valid_time_utc.to_rfc3339(),
            "2024-05-06T12:00:00+00:00"
        );
        assert_eq!(hour_jobs[0].tile_jobs.len(), 2);
        assert_eq!(hour_jobs[0].bundle.source_keys.len(), 4);
        let tile_ids = hour_jobs[0]
            .tile_jobs
            .iter()
            .map(|job| job.tile_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(tile_ids, vec!["tile_00", "tile_01"]);
    }

    #[test]
    fn dry_run_hour_runner_reports_progress_counters() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(2));
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let mut processor = NativeDryRunHourProcessor;
        let mut progress_events = Vec::new();
        let report = run_native_dataset_hour_plan_with_progress(
            &plan,
            &NativeDatasetRunnerConfig {
                max_attempts: 1,
                continue_on_error: true,
            },
            &mut processor,
            |progress| progress_events.push(progress.clone()),
        )
        .unwrap();

        assert_eq!(progress_events.len(), 26);
        assert_eq!(progress_events[0].hours_completed, 0);
        assert_eq!(report.progress.hours_total, 25);
        assert_eq!(report.progress.hours_completed, 25);
        assert_eq!(report.progress.tile_frames_total, 50);
        assert_eq!(report.progress.tile_frames_completed, 50);
        assert_eq!(report.progress.samples_emitted, 44);
        assert_eq!(report.progress.failed_hours, 0);
        assert_eq!(report.progress.failed_tile_frames, 0);
    }

    #[derive(Default)]
    struct FlakyHourProcessor {
        fail_first_hour_once: bool,
        calls: usize,
    }

    impl NativeHourProcessor for FlakyHourProcessor {
        fn process_hour(
            &mut self,
            _plan: &NativeDatasetPlan,
            job: &NativeDatasetHourJob,
        ) -> Result<NativeHourOutput, String> {
            self.calls += 1;
            if self.fail_first_hour_once && job.job_id.ends_with("20240506_1200") {
                self.fail_first_hour_once = false;
                return Err("transient hour decode error".to_string());
            }
            Ok(NativeHourOutput::empty())
        }
    }

    #[test]
    fn hour_runner_retries_transient_processor_failures() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let mut processor = FlakyHourProcessor {
            fail_first_hour_once: true,
            calls: 0,
        };
        let report = run_native_dataset_hour_plan(
            &plan,
            &NativeDatasetRunnerConfig {
                max_attempts: 2,
                continue_on_error: true,
            },
            &mut processor,
        )
        .unwrap();

        assert_eq!(report.progress.hours_completed, 25);
        assert_eq!(report.progress.failed_hours, 0);
        assert_eq!(report.jobs[0].attempts, 2);
        assert_eq!(processor.calls, 26);
    }

    #[derive(Default)]
    struct FlakyProcessor {
        fail_first_job_once: bool,
        calls: usize,
    }

    impl NativeFrameProcessor for FlakyProcessor {
        fn process_frame(
            &mut self,
            _plan: &NativeDatasetPlan,
            job: &NativeDatasetFrameJob,
        ) -> Result<NativeFrameOutput, String> {
            self.calls += 1;
            if self.fail_first_job_once && job.job_id.ends_with("20240506_1200") {
                self.fail_first_job_once = false;
                return Err("transient source error".to_string());
            }
            Ok(NativeFrameOutput {
                artifacts: vec![format!("{}.npz", job.job_id)],
            })
        }
    }

    #[test]
    fn runner_retries_transient_processor_failures() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let mut processor = FlakyProcessor {
            fail_first_job_once: true,
            calls: 0,
        };
        let report = run_native_dataset_plan(
            &plan,
            &NativeDatasetRunnerConfig {
                max_attempts: 2,
                continue_on_error: true,
            },
            &mut processor,
        )
        .unwrap();
        assert_eq!(report.attempted_frame_jobs, 25);
        assert_eq!(report.succeeded_frame_jobs, 25);
        assert_eq!(report.failed_frame_jobs, 0);
        assert_eq!(report.jobs[0].attempts, 2);
        assert_eq!(processor.calls, 26);
    }

    struct AlwaysFail;

    impl NativeFrameProcessor for AlwaysFail {
        fn process_frame(
            &mut self,
            _plan: &NativeDatasetPlan,
            _job: &NativeDatasetFrameJob,
        ) -> Result<NativeFrameOutput, String> {
            Err("permanent decode failure".to_string())
        }
    }

    #[test]
    fn runner_can_abort_on_first_failure() {
        let config = NativeDatasetBuildConfig::hrrr_multisource_v1("test", vec![case()], tiles(1));
        let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
        let mut processor = AlwaysFail;
        let err = run_native_dataset_plan(
            &plan,
            &NativeDatasetRunnerConfig {
                max_attempts: 1,
                continue_on_error: false,
            },
            &mut processor,
        )
        .unwrap_err();
        assert!(err.contains("permanent decode failure"));
    }
}
