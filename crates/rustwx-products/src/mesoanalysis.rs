use crate::gridded::SurfaceFields;
use chrono::{DateTime, Utc};
use rustwx_calc::{
    compute_dewpoint_from_pressure_and_mixing_ratio, compute_surface_mesoanalysis, MesoObservation,
    MesoanalysisConfig, MesoanalysisFields, MesoanalysisVariableDiagnostics, SurfaceMesoBackground,
};
use rustwx_core::GridShape;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

const F_TO_C_OFFSET: f64 = 32.0;
const F_TO_C_SCALE: f64 = 5.0 / 9.0;
const KTS_TO_MS: f64 = 0.514_444;
const EARTH_RADIUS_KM: f64 = 6371.0;
const VALIDATION_GRID_BIN_DEG: f64 = 1.0;
const VALIDATION_GRID_BIN_RADIUS: i32 = 2;
const VALIDATION_MAX_NEAREST_GRID_DISTANCE_KM: f64 = 150.0;
const HOLDOUT_SPATIAL_BLOCK_DEG: f64 = 2.0;
const MIN_MSLP_HPA: f64 = 800.0;
const MAX_MSLP_HPA: f64 = 1100.0;
const MIN_STATION_PRESSURE_HPA: f64 = 450.0;
const MAX_STATION_PRESSURE_HPA: f64 = 1100.0;
const MIN_ALTIMETER_INHG: f64 = 24.0;
const MAX_ALTIMETER_INHG: f64 = 33.5;
const DEFAULT_TIME_WEIGHT_HALF_LIFE_MINUTES: f64 = 60.0;
const DEFAULT_MAX_TIME_ERROR_INFLATION_FACTOR: f64 = 2.0;
pub const CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS: usize = 10;
pub const CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE: f64 = 0.0;

#[derive(Debug, Clone, Copy, PartialEq)]
struct RunnerObservationErrorProfile {
    temperature_c: f64,
    dewpoint_c: f64,
    wind_ms: f64,
    mean_sea_level_pressure_hpa: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RunnerObservationSourceQualityProfile {
    source_quality_class: &'static str,
    representativeness_class: &'static str,
    correction_role: &'static str,
    quality_weight: f64,
    error_profile: RunnerObservationErrorProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerObservationSourceSummary {
    pub path: PathBuf,
    pub source: String,
    pub source_name: String,
    pub kind: String,
    pub source_quality_class: String,
    pub representativeness_class: String,
    pub correction_role: String,
    pub quality_weight: f64,
    pub default_temperature_error_c: f64,
    pub default_dewpoint_error_c: f64,
    pub default_wind_error_ms: f64,
    pub default_mean_sea_level_pressure_error_hpa: f64,
    pub observation_count: usize,
    pub accepted_for_mesoanalysis: usize,
    pub accepted_mean_sea_level_pressure_count: usize,
    pub accepted_station_pressure_count: usize,
    pub accepted_altimeter_count: usize,
    pub accepted_min_observation_age_minutes: Option<f64>,
    pub accepted_mean_observation_age_minutes: Option<f64>,
    pub accepted_max_observation_age_minutes: Option<f64>,
    pub mean_time_weight: Option<f64>,
    pub min_time_weight: Option<f64>,
    pub duplicate_filtered_count: usize,
    pub profile_filtered_count: usize,
    pub time_filtered_count: usize,
    pub missing_or_invalid_time_count: usize,
    pub skipped_for_kind: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerMesoObservationLoad {
    pub observations: Vec<MesoObservation>,
    pub sources: Vec<RunnerObservationSourceSummary>,
}

#[derive(Debug, Clone)]
pub struct RunnerMesoObservationLoadOptions {
    pub reference_time_utc: Option<DateTime<Utc>>,
    pub max_age_minutes: Option<i64>,
    pub allow_future_minutes: i64,
    pub time_weight_half_life_minutes: Option<f64>,
    pub max_time_error_inflation_factor: f64,
    pub profile: RunnerMesoObservationProfile,
}

impl Default for RunnerMesoObservationLoadOptions {
    fn default() -> Self {
        Self {
            reference_time_utc: None,
            max_age_minutes: None,
            allow_future_minutes: 5,
            time_weight_half_life_minutes: Some(DEFAULT_TIME_WEIGHT_HALF_LIFE_MINUTES),
            max_time_error_inflation_factor: DEFAULT_MAX_TIME_ERROR_INFLATION_FACTOR,
            profile: RunnerMesoObservationProfile::AllCurrentSurface,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMesoObservationProfile {
    AllCurrentSurface,
    SurfaceMesoConus,
}

impl RunnerMesoObservationProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllCurrentSurface => "all_current_surface",
            Self::SurfaceMesoConus => "surface_meso_conus",
        }
    }
}

impl Default for RunnerMesoObservationProfile {
    fn default() -> Self {
        Self::AllCurrentSurface
    }
}

impl FromStr for RunnerMesoObservationProfile {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "all" | "all_current_surface" | "permissive" => Ok(Self::AllCurrentSurface),
            "surface_meso_conus" | "conus_surface_meso" | "surface_analysis_conus" => {
                Ok(Self::SurfaceMesoConus)
            }
            other => Err(format!(
                "unknown observation profile '{other}'; expected all_current_surface or surface_meso_conus"
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceMesoanalysisReport {
    pub schema: String,
    pub grid_cells: usize,
    pub source_count: usize,
    pub observation_count: usize,
    pub contributing_sources: Vec<String>,
    pub diagnostics: Vec<MesoanalysisVariableDiagnostics>,
    pub fields: SurfaceMesoanalysisFieldSummaries,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<SurfaceMesoanalysisValidationSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holdout_validation: Option<SurfaceMesoanalysisHoldoutValidationSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeated_holdout_validation: Option<SurfaceMesoanalysisRepeatedHoldoutValidationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceMesoanalysisFieldSummaries {
    pub temperature_2m_c: FieldSummary,
    pub dewpoint_2m_c: FieldSummary,
    pub q2_kgkg: FieldSummary,
    pub wind_speed_10m_ms: FieldSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<FieldSummary>,
    pub temperature_increment_c: FieldSummary,
    pub dewpoint_increment_c: FieldSummary,
    pub u10_increment_ms: FieldSummary,
    pub v10_increment_ms: FieldSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_increment_hpa: Option<FieldSummary>,
    pub temperature_confidence: FieldSummary,
    pub dewpoint_confidence: FieldSummary,
    pub u10_confidence: FieldSummary,
    pub v10_confidence: FieldSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_confidence: Option<FieldSummary>,
    pub neighbor_count: CountSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldSummary {
    pub finite_count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CountSummary {
    pub grid_cells: usize,
    pub covered_grid_cells: usize,
    pub max: u16,
    pub mean: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisValidationSummary {
    pub observation_count: usize,
    pub sampled_observation_count: usize,
    pub skipped_observation_count: usize,
    pub max_nearest_grid_distance_km: Option<f64>,
    pub temperature_c: VariableValidationSummary,
    pub dewpoint_c: VariableValidationSummary,
    pub wind_speed_ms: VariableValidationSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableValidationSummary>,
    pub source_summaries: Vec<SourceMesoanalysisValidationSummary>,
    #[serde(default)]
    pub strata_summaries: Vec<StratifiedMesoanalysisValidationSummary>,
    pub samples: Vec<StationMesoanalysisValidationSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceMesoanalysisValidationSummary {
    pub source: String,
    pub sampled_observation_count: usize,
    pub temperature_c: VariableValidationSummary,
    pub dewpoint_c: VariableValidationSummary,
    pub wind_speed_ms: VariableValidationSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableValidationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StratifiedMesoanalysisValidationSummary {
    pub stratum_type: String,
    pub stratum_value: String,
    pub sampled_observation_count: usize,
    pub temperature_c: VariableValidationSummary,
    pub dewpoint_c: VariableValidationSummary,
    pub wind_speed_ms: VariableValidationSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableValidationSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceMesoanalysisHoldoutStrategy {
    StationHash,
    SpatialBlock,
    SourceHash,
}

impl Default for SurfaceMesoanalysisHoldoutStrategy {
    fn default() -> Self {
        Self::StationHash
    }
}

impl SurfaceMesoanalysisHoldoutStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StationHash => "station_hash",
            Self::SpatialBlock => "spatial_block",
            Self::SourceHash => "source_hash",
        }
    }

    fn selection_rule(self) -> &'static str {
        match self {
            Self::StationHash => {
                "stable hash rank of source/station/time, adjusted to keep training non-empty"
            }
            Self::SpatialBlock => {
                "stable hash rank of 2 degree latitude/longitude blocks, adjusted to keep training non-empty"
            }
            Self::SourceHash => {
                "stable hash rank of source/provider groups, adjusted to keep training non-empty"
            }
        }
    }
}

impl FromStr for SurfaceMesoanalysisHoldoutStrategy {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "station_hash" | "station" | "random" | "station_random" => Ok(Self::StationHash),
            "spatial_block" | "spatial" | "block" | "spatial_blocks" => Ok(Self::SpatialBlock),
            "source_hash" | "source" | "provider" | "provider_holdout" => Ok(Self::SourceHash),
            other => Err(format!(
                "unknown holdout strategy '{other}'; expected station_hash, spatial_block, or source_hash"
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisHoldoutValidationSummary {
    pub schema: String,
    pub requested_fraction: f64,
    pub seed: u64,
    pub strategy: SurfaceMesoanalysisHoldoutStrategy,
    pub min_holdout_observations: usize,
    pub training_observation_count: usize,
    pub holdout_observation_count: usize,
    pub selection_rule: String,
    pub validation: SurfaceMesoanalysisValidationSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisRepeatedHoldoutValidationSummary {
    pub schema: String,
    pub requested_fraction: f64,
    pub seed: u64,
    pub repeat_count: usize,
    pub completed_fold_count: usize,
    pub strategy: SurfaceMesoanalysisHoldoutStrategy,
    pub min_holdout_observations: usize,
    pub selection_rule: String,
    pub folds: Vec<SurfaceMesoanalysisHoldoutValidationSummary>,
    pub temperature_c: VariableRepeatedValidationSummary,
    pub dewpoint_c: VariableRepeatedValidationSummary,
    pub wind_speed_ms: VariableRepeatedValidationSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableRepeatedValidationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableRepeatedValidationSummary {
    pub fold_count: usize,
    pub total_observation_count: usize,
    pub mean_observation_count: f64,
    pub mean_abs_background_error: Option<f64>,
    pub mean_abs_analysis_error: Option<f64>,
    pub mean_abs_error_improvement: Option<f64>,
    pub background_rmse: Option<f64>,
    pub analysis_rmse: Option<f64>,
    pub analysis_beats_background_mae_fold_count: usize,
    pub analysis_beats_background_rmse_fold_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisValidationGateThresholds {
    pub min_sampled_observations: usize,
    pub max_skipped_observations: usize,
    pub max_nearest_grid_distance_km: f64,
    pub max_temperature_mean_abs_error_c: f64,
    pub max_dewpoint_mean_abs_error_c: f64,
    pub max_wind_speed_mean_abs_error_ms: f64,
}

impl Default for SurfaceMesoanalysisValidationGateThresholds {
    fn default() -> Self {
        Self {
            min_sampled_observations: 10,
            max_skipped_observations: 0,
            max_nearest_grid_distance_km: 10.0,
            max_temperature_mean_abs_error_c: 2.0,
            max_dewpoint_mean_abs_error_c: 2.5,
            max_wind_speed_mean_abs_error_ms: 2.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisValidationGate {
    pub schema: String,
    pub passed: bool,
    pub thresholds: SurfaceMesoanalysisValidationGateThresholds,
    pub checks: Vec<SurfaceMesoanalysisValidationGateCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisValidationGateCheck {
    pub name: String,
    pub passed: bool,
    pub observed: Option<f64>,
    pub threshold: f64,
    pub comparator: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VariableValidationSummary {
    pub observation_count: usize,
    pub improved_count: usize,
    pub worsened_count: usize,
    pub unchanged_count: usize,
    pub mean_background_error: Option<f64>,
    pub mean_analysis_error: Option<f64>,
    pub mean_abs_background_error: Option<f64>,
    pub mean_abs_analysis_error: Option<f64>,
    pub mean_abs_error_improvement: Option<f64>,
    pub background_rmse: Option<f64>,
    pub analysis_rmse: Option<f64>,
    pub max_abs_background_error: Option<f64>,
    pub max_abs_analysis_error: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<VariableConfidenceValidationSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VariableConfidenceValidationSummary {
    pub observation_count: usize,
    pub mean_confidence: Option<f64>,
    pub low_confidence_observation_count: usize,
    pub low_confidence_mean_abs_analysis_error: Option<f64>,
    pub medium_confidence_observation_count: usize,
    pub medium_confidence_mean_abs_analysis_error: Option<f64>,
    pub high_confidence_observation_count: usize,
    pub high_confidence_mean_abs_analysis_error: Option<f64>,
    pub high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub confidence_abs_error_correlation: Option<f64>,
    pub ranked_low_confidence_observation_count: usize,
    pub ranked_low_confidence_mean_abs_analysis_error: Option<f64>,
    pub ranked_high_confidence_observation_count: usize,
    pub ranked_high_confidence_mean_abs_analysis_error: Option<f64>,
    pub ranked_high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub reliability: ConfidenceReliabilityContract,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfidenceReliabilityContract {
    pub schema: String,
    pub semantic_label: String,
    pub status: String,
    pub bucket_coverage_sufficient: bool,
    pub ranked_low_confidence_observation_count: usize,
    pub ranked_high_confidence_observation_count: usize,
    pub min_ranked_bucket_observation_count: usize,
    pub ranked_high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub max_ranked_high_minus_low_mean_abs_analysis_error: f64,
    pub message: String,
}

impl Default for ConfidenceReliabilityContract {
    fn default() -> Self {
        confidence_reliability_contract_from_ranked_buckets(0, 0, None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisValidationComparison {
    pub schema: String,
    pub candidate_label: String,
    pub baseline_label: String,
    pub temperature_c: VariableValidationComparison,
    pub dewpoint_c: VariableValidationComparison,
    pub wind_speed_ms: VariableValidationComparison,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableValidationComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableValidationComparison {
    pub candidate_observation_count: usize,
    pub baseline_observation_count: usize,
    pub candidate_mean_abs_analysis_error: Option<f64>,
    pub baseline_mean_abs_analysis_error: Option<f64>,
    pub mean_abs_analysis_error_delta: Option<f64>,
    pub candidate_analysis_rmse: Option<f64>,
    pub baseline_analysis_rmse: Option<f64>,
    pub analysis_rmse_delta: Option<f64>,
    pub candidate_mean_analysis_error: Option<f64>,
    pub baseline_mean_analysis_error: Option<f64>,
    pub mean_analysis_error_delta: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisBenchmarkSummary {
    pub schema: String,
    pub candidate_label: String,
    pub baseline_label: String,
    pub validation_mode: String,
    pub temperature_c: VariableBenchmarkSummary,
    pub dewpoint_c: VariableBenchmarkSummary,
    pub wind_speed_ms: VariableBenchmarkSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableBenchmarkSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableBenchmarkSummary {
    pub candidate_observation_count: usize,
    pub baseline_observation_count: usize,
    pub background_mean_abs_error: Option<f64>,
    pub candidate_mean_abs_error: Option<f64>,
    pub baseline_mean_abs_error: Option<f64>,
    pub candidate_minus_background_mae: Option<f64>,
    pub baseline_minus_background_mae: Option<f64>,
    pub candidate_minus_baseline_mae: Option<f64>,
    pub background_rmse: Option<f64>,
    pub candidate_rmse: Option<f64>,
    pub baseline_rmse: Option<f64>,
    pub candidate_minus_background_rmse: Option<f64>,
    pub baseline_minus_background_rmse: Option<f64>,
    pub candidate_minus_baseline_rmse: Option<f64>,
    pub mae_winner: String,
    pub rmse_winner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisRepeatedHoldoutBenchmarkSummary {
    pub schema: String,
    pub candidate_label: String,
    pub baseline_label: String,
    pub validation_mode: String,
    pub fold_count: usize,
    pub temperature_c: VariableRepeatedBenchmarkSummary,
    pub dewpoint_c: VariableRepeatedBenchmarkSummary,
    pub wind_speed_ms: VariableRepeatedBenchmarkSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<VariableRepeatedBenchmarkSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableRepeatedBenchmarkSummary {
    pub fold_count: usize,
    pub background_mean_abs_error: Option<f64>,
    pub candidate_mean_abs_error: Option<f64>,
    pub baseline_mean_abs_error: Option<f64>,
    pub candidate_minus_background_mae: Option<f64>,
    pub candidate_minus_baseline_mae: Option<f64>,
    pub background_rmse: Option<f64>,
    pub candidate_rmse: Option<f64>,
    pub baseline_rmse: Option<f64>,
    pub candidate_minus_background_rmse: Option<f64>,
    pub candidate_minus_baseline_rmse: Option<f64>,
    pub candidate_beats_background_mae_fold_count: usize,
    pub candidate_beats_baseline_mae_fold_count: usize,
    pub candidate_beats_background_rmse_fold_count: usize,
    pub candidate_beats_baseline_rmse_fold_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisExternalReferenceDescriptor {
    pub reference_label: String,
    pub reference_model: String,
    pub reference_source: String,
    pub reference_cycle: String,
    pub reference_forecast_hour: u16,
    pub reference_product: String,
    pub candidate_label: String,
    pub background_label: String,
    pub validation_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisExternalReferenceComparison {
    pub schema: String,
    pub reference_label: String,
    pub reference_model: String,
    pub reference_source: String,
    pub reference_cycle: String,
    pub reference_forecast_hour: u16,
    pub reference_product: String,
    pub candidate_label: String,
    pub background_label: String,
    pub validation_mode: String,
    pub sampled_observation_count: usize,
    pub skipped_observation_count: usize,
    pub max_nearest_grid_distance_km: Option<f64>,
    pub temperature_c: SurfaceMesoanalysisExternalReferenceVariableComparison,
    pub dewpoint_c: SurfaceMesoanalysisExternalReferenceVariableComparison,
    pub wind_speed_ms: SurfaceMesoanalysisExternalReferenceVariableComparison,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<SurfaceMesoanalysisExternalReferenceVariableComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisExternalReferenceVariableComparison {
    pub observation_count: usize,
    pub candidate_observation_count: usize,
    pub reference_observation_count: usize,
    pub background_mean_abs_error: Option<f64>,
    pub candidate_mean_abs_error: Option<f64>,
    pub reference_mean_abs_error: Option<f64>,
    pub candidate_minus_background_mae: Option<f64>,
    pub candidate_minus_reference_mae: Option<f64>,
    pub background_rmse: Option<f64>,
    pub candidate_rmse: Option<f64>,
    pub reference_rmse: Option<f64>,
    pub candidate_minus_background_rmse: Option<f64>,
    pub candidate_minus_reference_rmse: Option<f64>,
    pub mae_winner: String,
    pub rmse_winner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StationMesoanalysisValidationSample {
    pub station_id: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_quality_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub representativeness_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correction_role: Option<String>,
    pub timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_age_minutes: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_age_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_weight: Option<f64>,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub grid_index: usize,
    pub grid_latitude_deg: f64,
    pub grid_longitude_deg: f64,
    pub nearest_grid_distance_km: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terrain_pressure_hpa: Option<f64>,
    #[serde(default)]
    pub terrain_pressure_class: String,
    pub temperature_c: Option<StationVariableValidation>,
    pub dewpoint_c: Option<StationVariableValidation>,
    pub wind_speed_ms: Option<StationVariableValidation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_sea_level_pressure_hpa: Option<StationVariableValidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StationVariableValidation {
    pub observed: f64,
    pub background: f64,
    pub analysis: f64,
    pub background_error: f64,
    pub analysis_error: f64,
    pub abs_error_improvement: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct SurfaceMesoanalysisGridExportRequest {
    pub model: String,
    pub run_id: String,
    pub member: String,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub source: String,
    pub forecast_hour: u16,
    pub valid_time: String,
    pub out_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceMesoanalysisGridExportManifest {
    pub schema: String,
    pub model: String,
    pub run_id: String,
    pub member: String,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub source: String,
    pub forecast_hours: Vec<u16>,
    pub generated_at: String,
    pub manifest_path: PathBuf,
    pub fields: Vec<SurfaceMesoanalysisGridExportRecord>,
    pub blockers: Vec<Value>,
    pub timing: SurfaceMesoanalysisGridExportTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurfaceMesoanalysisGridExportRecord {
    pub product_slug: String,
    pub title: String,
    pub units: String,
    pub model: String,
    pub run_id: String,
    pub member: String,
    pub forecast_hour: u16,
    pub valid_time: String,
    pub nx: usize,
    pub ny: usize,
    pub crop: Option<SurfaceMesoanalysisGridExportCrop>,
    pub bounds: Option<[f64; 4]>,
    pub values_path: PathBuf,
    pub lat_path: PathBuf,
    pub lon_path: PathBuf,
    pub no_data: SurfaceMesoanalysisGridNoDataInfo,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SurfaceMesoanalysisGridExportCrop {
    pub x_start: usize,
    pub x_end: usize,
    pub y_start: usize,
    pub y_end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SurfaceMesoanalysisGridNoDataInfo {
    pub encoding: String,
    pub finite_count: usize,
    pub nan_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct SurfaceMesoanalysisGridExportTiming {
    pub total_ms: u128,
    pub write_ms: u128,
}

#[derive(Debug, Deserialize)]
struct RunnerObservationFile {
    #[serde(default)]
    source: String,
    #[serde(default)]
    source_name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    observation_count: usize,
    #[serde(default)]
    observations: Vec<RunnerDirectObservation>,
}

#[derive(Debug, Deserialize)]
struct RunnerDirectObservation {
    #[serde(default)]
    station_id: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    timestamp: Option<String>,
    latitude: f64,
    longitude: f64,
    temperature_f: Option<f64>,
    dewpoint_f: Option<f64>,
    wind_direction_deg: Option<f64>,
    wind_speed_kts: Option<f64>,
    altimeter_inhg: Option<f64>,
    station_pressure_mb: Option<f64>,
    sea_level_pressure_mb: Option<f64>,
}

pub fn load_runner_meso_observations(
    paths: &[PathBuf],
) -> Result<RunnerMesoObservationLoad, Box<dyn std::error::Error>> {
    load_runner_meso_observations_with_options(paths, &RunnerMesoObservationLoadOptions::default())
}

pub fn load_runner_meso_observations_with_options(
    paths: &[PathBuf],
    options: &RunnerMesoObservationLoadOptions,
) -> Result<RunnerMesoObservationLoad, Box<dyn std::error::Error>> {
    let mut observations = Vec::new();
    let mut sources = Vec::new();

    for path in paths {
        let bytes = fs::read(path)?;
        let file: RunnerObservationFile = serde_json::from_slice(&bytes)?;
        let skip_kind = !runner_kind_is_current_surface_candidate(file.kind.as_str());
        let source_quality =
            runner_observation_source_quality_profile(file.source.as_str(), file.kind.as_str());
        let before = observations.len();
        let mut accepted_mean_sea_level_pressure_count = 0usize;
        let mut accepted_station_pressure_count = 0usize;
        let mut accepted_altimeter_count = 0usize;
        let mut accepted_age_count = 0usize;
        let mut accepted_age_sum = 0.0;
        let mut accepted_min_age = None::<f64>;
        let mut accepted_max_age = None::<f64>;
        let mut time_weight_count = 0usize;
        let mut time_weight_sum = 0.0;
        let mut min_time_weight = None::<f64>;
        let mut profile_filtered_count = 0usize;
        let mut time_filtered_count = 0usize;
        let mut missing_or_invalid_time_count = 0usize;
        if !skip_kind {
            for observation in &file.observations {
                let Some(mut meso) = runner_observation_to_meso(
                    file.source.as_str(),
                    file.kind.as_str(),
                    observation,
                ) else {
                    continue;
                };
                if !observation_passes_profile(&meso, file.kind.as_str(), options.profile) {
                    profile_filtered_count += 1;
                    continue;
                }
                match observation_passes_time_filter(&meso, options) {
                    TimeFilterDecision::Pass { age_minutes } => {
                        if let Some(age_minutes) = age_minutes {
                            accepted_age_count += 1;
                            accepted_age_sum += age_minutes;
                            accepted_min_age = Some(
                                accepted_min_age
                                    .map(|current| current.min(age_minutes))
                                    .unwrap_or(age_minutes),
                            );
                            accepted_max_age = Some(
                                accepted_max_age
                                    .map(|current| current.max(age_minutes))
                                    .unwrap_or(age_minutes),
                            );
                        }
                        if let Some(time_weight) =
                            apply_time_representativeness(&mut meso, age_minutes, options)
                        {
                            time_weight_count += 1;
                            time_weight_sum += time_weight;
                            min_time_weight = Some(
                                min_time_weight
                                    .map(|current| current.min(time_weight))
                                    .unwrap_or(time_weight),
                            );
                        }
                        if meso.mean_sea_level_pressure_hpa.is_some() {
                            accepted_mean_sea_level_pressure_count += 1;
                        }
                        if normalized_station_pressure_hpa(observation).is_some() {
                            accepted_station_pressure_count += 1;
                        }
                        if normalized_altimeter_inhg(observation).is_some() {
                            accepted_altimeter_count += 1;
                        }
                        observations.push(meso);
                    }
                    TimeFilterDecision::Filtered => time_filtered_count += 1,
                    TimeFilterDecision::MissingOrInvalidTime => missing_or_invalid_time_count += 1,
                }
            }
        }
        let accepted = observations.len() - before;
        sources.push(RunnerObservationSourceSummary {
            path: path.clone(),
            source: file.source,
            source_name: file.source_name,
            kind: file.kind,
            source_quality_class: source_quality.source_quality_class.to_string(),
            representativeness_class: source_quality.representativeness_class.to_string(),
            correction_role: source_quality.correction_role.to_string(),
            quality_weight: source_quality.quality_weight,
            default_temperature_error_c: source_quality.error_profile.temperature_c,
            default_dewpoint_error_c: source_quality.error_profile.dewpoint_c,
            default_wind_error_ms: source_quality.error_profile.wind_ms,
            default_mean_sea_level_pressure_error_hpa: source_quality
                .error_profile
                .mean_sea_level_pressure_hpa,
            observation_count: file.observation_count,
            accepted_for_mesoanalysis: accepted,
            accepted_mean_sea_level_pressure_count,
            accepted_station_pressure_count,
            accepted_altimeter_count,
            accepted_min_observation_age_minutes: accepted_min_age,
            accepted_mean_observation_age_minutes: if accepted_age_count > 0 {
                Some(accepted_age_sum / accepted_age_count as f64)
            } else {
                None
            },
            accepted_max_observation_age_minutes: accepted_max_age,
            mean_time_weight: if time_weight_count > 0 {
                Some(time_weight_sum / time_weight_count as f64)
            } else {
                None
            },
            min_time_weight,
            duplicate_filtered_count: 0,
            profile_filtered_count,
            time_filtered_count,
            missing_or_invalid_time_count,
            skipped_for_kind: skip_kind,
        });
    }

    let (deduplicated_observations, duplicate_observations) =
        deduplicate_meso_observations(observations);
    for duplicate in duplicate_observations {
        if let Some(source) = sources
            .iter_mut()
            .find(|source| source.source == duplicate.source)
        {
            source.duplicate_filtered_count += 1;
            source.accepted_for_mesoanalysis = source.accepted_for_mesoanalysis.saturating_sub(1);
        }
    }

    Ok(RunnerMesoObservationLoad {
        observations: deduplicated_observations,
        sources,
    })
}

fn deduplicate_meso_observations(
    observations: Vec<MesoObservation>,
) -> (Vec<MesoObservation>, Vec<MesoObservation>) {
    let mut key_to_index = HashMap::<String, usize>::new();
    let mut retained = Vec::<MesoObservation>::with_capacity(observations.len());
    let mut duplicates = Vec::<MesoObservation>::new();

    for observation in observations {
        let key = meso_observation_dedup_key(&observation);
        if let Some(&retained_index) = key_to_index.get(&key) {
            if meso_observation_is_better_duplicate(&observation, &retained[retained_index]) {
                duplicates.push(std::mem::replace(
                    &mut retained[retained_index],
                    observation,
                ));
            } else {
                duplicates.push(observation);
            }
        } else {
            key_to_index.insert(key, retained.len());
            retained.push(observation);
        }
    }

    (retained, duplicates)
}

fn meso_observation_dedup_key(observation: &MesoObservation) -> String {
    let station = observation.station_id.trim().to_ascii_lowercase();
    if !station.is_empty() && station != "unknown" {
        format!("station:{station}")
    } else {
        format!(
            "geo:{:.3}:{:.3}",
            observation.latitude_deg, observation.longitude_deg
        )
    }
}

fn meso_observation_is_better_duplicate(
    candidate: &MesoObservation,
    retained: &MesoObservation,
) -> bool {
    meso_observation_duplicate_score(candidate) > meso_observation_duplicate_score(retained)
}

fn meso_observation_duplicate_score(observation: &MesoObservation) -> (i64, i64, i64, i64) {
    let quality = (observation.quality_weight.clamp(0.0, 100.0) * 1_000_000.0).round() as i64;
    let field_count = [
        observation.temperature_c,
        observation.dewpoint_c,
        observation.wind_speed_ms,
        observation.mean_sea_level_pressure_hpa,
    ]
    .into_iter()
    .filter(Option::is_some)
    .count() as i64;
    let timestamp = observation
        .timestamp
        .as_deref()
        .and_then(parse_observation_time_utc)
        .map(|time| time.timestamp())
        .unwrap_or(i64::MIN);
    let precision = -[
        observation.temperature_error_c,
        observation.dewpoint_error_c,
        observation.wind_error_ms,
        observation.mean_sea_level_pressure_error_hpa,
    ]
    .into_iter()
    .flatten()
    .filter(|value| value.is_finite())
    .map(|value| (value * 1_000.0).round() as i64)
    .sum::<i64>();

    (quality, field_count, timestamp, precision)
}

pub fn compute_surface_mesoanalysis_from_fields(
    surface: &SurfaceFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
) -> Result<MesoanalysisFields, Box<dyn std::error::Error>> {
    let grid = GridShape::new(surface.nx, surface.ny)?;
    Ok(compute_surface_mesoanalysis(
        SurfaceMesoBackground {
            grid,
            lat_deg: &surface.lat,
            lon_deg: &surface.lon,
            psfc_pa: &surface.psfc_pa,
            t2_k: &surface.t2_k,
            q2_kgkg: &surface.q2_kgkg,
            u10_ms: &surface.u10_ms,
            v10_ms: &surface.v10_ms,
        },
        observations,
        config,
    )?)
}

pub fn summarize_surface_mesoanalysis(
    fields: &MesoanalysisFields,
    observations: &[MesoObservation],
) -> SurfaceMesoanalysisReport {
    let mut contributing_sources = BTreeSet::new();
    for observation in observations {
        if !observation.source.is_empty() {
            contributing_sources.insert(observation.source.clone());
        }
    }
    let wind_speed_10m_ms: Vec<f64> = fields
        .u10_ms
        .iter()
        .zip(fields.v10_ms.iter())
        .map(|(&u, &v)| (u * u + v * v).sqrt())
        .collect();
    SurfaceMesoanalysisReport {
        schema: "rustwx.surface_mesoanalysis.report.v1".to_string(),
        grid_cells: fields.temperature_2m_c.len(),
        source_count: contributing_sources.len(),
        observation_count: observations.len(),
        contributing_sources: contributing_sources.into_iter().collect(),
        diagnostics: fields.diagnostics.clone(),
        fields: SurfaceMesoanalysisFieldSummaries {
            temperature_2m_c: summarize_field(&fields.temperature_2m_c),
            dewpoint_2m_c: summarize_field(&fields.dewpoint_2m_c),
            q2_kgkg: summarize_field(&fields.q2_kgkg),
            wind_speed_10m_ms: summarize_field(&wind_speed_10m_ms),
            mean_sea_level_pressure_hpa: fields
                .mean_sea_level_pressure_hpa
                .as_ref()
                .map(|values| summarize_field(values)),
            temperature_increment_c: summarize_field(&fields.temperature_increment_c),
            dewpoint_increment_c: summarize_field(&fields.dewpoint_increment_c),
            u10_increment_ms: summarize_field(&fields.u10_increment_ms),
            v10_increment_ms: summarize_field(&fields.v10_increment_ms),
            mean_sea_level_pressure_increment_hpa: fields
                .mean_sea_level_pressure_increment_hpa
                .as_ref()
                .map(|values| summarize_field(values)),
            temperature_confidence: summarize_field(&fields.temperature_confidence),
            dewpoint_confidence: summarize_field(&fields.dewpoint_confidence),
            u10_confidence: summarize_field(&fields.u10_confidence),
            v10_confidence: summarize_field(&fields.v10_confidence),
            mean_sea_level_pressure_confidence: fields
                .mean_sea_level_pressure_confidence
                .as_ref()
                .map(|values| summarize_field(values)),
            neighbor_count: summarize_counts(&fields.neighbor_count),
        },
        validation: None,
        holdout_validation: None,
        repeated_holdout_validation: None,
    }
}

pub fn summarize_surface_mesoanalysis_with_validation(
    surface: &SurfaceFields,
    fields: &MesoanalysisFields,
    observations: &[MesoObservation],
) -> Result<SurfaceMesoanalysisReport, Box<dyn std::error::Error>> {
    let mut report = summarize_surface_mesoanalysis(fields, observations);
    report.validation = Some(validate_surface_mesoanalysis_at_observations(
        surface,
        fields,
        observations,
    )?);
    Ok(report)
}

pub fn summarize_surface_mesoanalysis_with_validation_and_holdout(
    surface: &SurfaceFields,
    fields: &MesoanalysisFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
    requested_holdout_fraction: f64,
    holdout_seed: u64,
    min_holdout_observations: usize,
) -> Result<SurfaceMesoanalysisReport, Box<dyn std::error::Error>> {
    summarize_surface_mesoanalysis_with_validation_and_holdout_strategy(
        surface,
        fields,
        observations,
        config,
        requested_holdout_fraction,
        holdout_seed,
        min_holdout_observations,
        SurfaceMesoanalysisHoldoutStrategy::StationHash,
    )
}

pub fn summarize_surface_mesoanalysis_with_validation_and_holdout_strategy(
    surface: &SurfaceFields,
    fields: &MesoanalysisFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
    requested_holdout_fraction: f64,
    holdout_seed: u64,
    min_holdout_observations: usize,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
) -> Result<SurfaceMesoanalysisReport, Box<dyn std::error::Error>> {
    let mut report = summarize_surface_mesoanalysis_with_validation(surface, fields, observations)?;
    report.holdout_validation = compute_surface_mesoanalysis_holdout_validation_with_strategy(
        surface,
        observations,
        config,
        requested_holdout_fraction,
        holdout_seed,
        min_holdout_observations,
        holdout_strategy,
    )?;
    Ok(report)
}

pub fn compute_surface_mesoanalysis_holdout_validation(
    surface: &SurfaceFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
) -> Result<Option<SurfaceMesoanalysisHoldoutValidationSummary>, Box<dyn std::error::Error>> {
    compute_surface_mesoanalysis_holdout_validation_with_strategy(
        surface,
        observations,
        config,
        requested_fraction,
        seed,
        min_holdout_observations,
        SurfaceMesoanalysisHoldoutStrategy::StationHash,
    )
}

pub fn compute_surface_mesoanalysis_holdout_validation_with_strategy(
    surface: &SurfaceFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
) -> Result<Option<SurfaceMesoanalysisHoldoutValidationSummary>, Box<dyn std::error::Error>> {
    if !(requested_fraction.is_finite() && requested_fraction > 0.0) || observations.len() < 2 {
        return Ok(None);
    }
    let Some((training, holdout)) = surface_mesoanalysis_holdout_split_with_strategy(
        observations,
        requested_fraction,
        seed,
        min_holdout_observations,
        holdout_strategy,
    ) else {
        return Ok(None);
    };
    let holdout_fields = compute_surface_mesoanalysis_from_fields(surface, &training, config)?;
    let validation =
        validate_surface_mesoanalysis_at_observations(surface, &holdout_fields, &holdout)?;
    Ok(Some(SurfaceMesoanalysisHoldoutValidationSummary {
        schema: "rustwx.surface_mesoanalysis.holdout_validation.v1".to_string(),
        requested_fraction,
        seed,
        strategy: holdout_strategy,
        min_holdout_observations,
        training_observation_count: training.len(),
        holdout_observation_count: holdout.len(),
        selection_rule: holdout_strategy.selection_rule().to_string(),
        validation,
    }))
}

pub fn surface_mesoanalysis_holdout_observations_with_strategy(
    observations: &[MesoObservation],
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
) -> Option<Vec<MesoObservation>> {
    surface_mesoanalysis_holdout_split_with_strategy(
        observations,
        requested_fraction,
        seed,
        min_holdout_observations,
        holdout_strategy,
    )
    .map(|(_, holdout)| holdout)
}

pub fn surface_mesoanalysis_holdout_split_with_strategy(
    observations: &[MesoObservation],
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
) -> Option<(Vec<MesoObservation>, Vec<MesoObservation>)> {
    if !(requested_fraction.is_finite() && requested_fraction > 0.0) || observations.len() < 2 {
        return None;
    }
    let (training, holdout) = deterministic_holdout_split(
        observations,
        requested_fraction,
        seed,
        min_holdout_observations,
        holdout_strategy,
    );
    if training.is_empty() || holdout.is_empty() {
        None
    } else {
        Some((training, holdout))
    }
}

pub fn compute_surface_mesoanalysis_repeated_holdout_validation(
    surface: &SurfaceFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
    requested_fraction: f64,
    seed: u64,
    repeat_count: usize,
    min_holdout_observations: usize,
) -> Result<Option<SurfaceMesoanalysisRepeatedHoldoutValidationSummary>, Box<dyn std::error::Error>>
{
    compute_surface_mesoanalysis_repeated_holdout_validation_with_strategy(
        surface,
        observations,
        config,
        requested_fraction,
        seed,
        repeat_count,
        min_holdout_observations,
        SurfaceMesoanalysisHoldoutStrategy::StationHash,
    )
}

pub fn compute_surface_mesoanalysis_repeated_holdout_validation_with_strategy(
    surface: &SurfaceFields,
    observations: &[MesoObservation],
    config: MesoanalysisConfig,
    requested_fraction: f64,
    seed: u64,
    repeat_count: usize,
    min_holdout_observations: usize,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
) -> Result<Option<SurfaceMesoanalysisRepeatedHoldoutValidationSummary>, Box<dyn std::error::Error>>
{
    if repeat_count == 0 {
        return Ok(None);
    }
    let mut folds = Vec::new();
    for fold_index in 0..repeat_count {
        let fold_seed = seed.wrapping_add(fold_index as u64);
        if let Some(holdout) = compute_surface_mesoanalysis_holdout_validation_with_strategy(
            surface,
            observations,
            config,
            requested_fraction,
            fold_seed,
            min_holdout_observations,
            holdout_strategy,
        )? {
            folds.push(holdout);
        }
    }
    if folds.is_empty() {
        return Ok(None);
    }

    Ok(Some(SurfaceMesoanalysisRepeatedHoldoutValidationSummary {
        schema: "rustwx.surface_mesoanalysis.repeated_holdout_validation.v1".to_string(),
        requested_fraction,
        seed,
        repeat_count,
        completed_fold_count: folds.len(),
        strategy: holdout_strategy,
        min_holdout_observations,
        selection_rule: format!(
            "{}, repeated with consecutive seeds",
            holdout_strategy.selection_rule()
        ),
        temperature_c: summarize_repeated_variable_validation(
            folds.iter().map(|fold| &fold.validation.temperature_c),
        ),
        dewpoint_c: summarize_repeated_variable_validation(
            folds.iter().map(|fold| &fold.validation.dewpoint_c),
        ),
        wind_speed_ms: summarize_repeated_variable_validation(
            folds.iter().map(|fold| &fold.validation.wind_speed_ms),
        ),
        mean_sea_level_pressure_hpa: summarize_repeated_optional_variable_validation(
            folds
                .iter()
                .map(|fold| fold.validation.mean_sea_level_pressure_hpa.as_ref()),
        ),
        folds,
    }))
}

fn summarize_repeated_optional_variable_validation<'a>(
    variables: impl Iterator<Item = Option<&'a VariableValidationSummary>>,
) -> Option<VariableRepeatedValidationSummary> {
    let variables = variables.flatten().collect::<Vec<_>>();
    if variables.is_empty() {
        None
    } else {
        Some(summarize_repeated_variable_validation(
            variables.into_iter(),
        ))
    }
}

fn summarize_repeated_variable_validation<'a>(
    variables: impl Iterator<Item = &'a VariableValidationSummary>,
) -> VariableRepeatedValidationSummary {
    let variables = variables.collect::<Vec<_>>();
    let fold_count = variables.len();
    let total_observation_count = variables
        .iter()
        .map(|variable| variable.observation_count)
        .sum::<usize>();
    VariableRepeatedValidationSummary {
        fold_count,
        total_observation_count,
        mean_observation_count: if fold_count > 0 {
            total_observation_count as f64 / fold_count as f64
        } else {
            0.0
        },
        mean_abs_background_error: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.mean_abs_background_error),
        ),
        mean_abs_analysis_error: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.mean_abs_analysis_error),
        ),
        mean_abs_error_improvement: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.mean_abs_error_improvement),
        ),
        background_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.background_rmse),
        ),
        analysis_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.analysis_rmse),
        ),
        analysis_beats_background_mae_fold_count: variables
            .iter()
            .filter(|variable| {
                option_less_than(
                    variable.mean_abs_analysis_error,
                    variable.mean_abs_background_error,
                )
            })
            .count(),
        analysis_beats_background_rmse_fold_count: variables
            .iter()
            .filter(|variable| option_less_than(variable.analysis_rmse, variable.background_rmse))
            .count(),
    }
}

fn deterministic_holdout_split(
    observations: &[MesoObservation],
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
) -> (Vec<MesoObservation>, Vec<MesoObservation>) {
    match holdout_strategy {
        SurfaceMesoanalysisHoldoutStrategy::StationHash => station_hash_holdout_split(
            observations,
            requested_fraction,
            seed,
            min_holdout_observations,
        ),
        SurfaceMesoanalysisHoldoutStrategy::SpatialBlock => grouped_holdout_split(
            observations,
            requested_fraction,
            seed,
            min_holdout_observations,
            spatial_holdout_key,
        ),
        SurfaceMesoanalysisHoldoutStrategy::SourceHash => grouped_holdout_split(
            observations,
            requested_fraction,
            seed,
            min_holdout_observations,
            source_holdout_key,
        ),
    }
}

fn station_hash_holdout_split(
    observations: &[MesoObservation],
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
) -> (Vec<MesoObservation>, Vec<MesoObservation>) {
    let fraction = requested_fraction.clamp(0.0, 0.95);
    let target_holdout_count =
        target_holdout_count(observations.len(), fraction, min_holdout_observations);
    if target_holdout_count == 0 {
        return (observations.to_vec(), Vec::new());
    }

    let mut scored = observations
        .iter()
        .cloned()
        .map(|observation| {
            let score = holdout_score(&observation, seed);
            (score, observation)
        })
        .collect::<Vec<_>>();
    scored.sort_by_key(|(score, observation)| {
        (
            *score,
            observation.source.clone(),
            observation.station_id.clone(),
            observation.timestamp.clone(),
        )
    });

    let mut training = Vec::with_capacity(observations.len() - target_holdout_count);
    let mut holdout = Vec::with_capacity(target_holdout_count);
    for (index, (_, observation)) in scored.into_iter().enumerate() {
        if index < target_holdout_count {
            holdout.push(observation);
        } else {
            training.push(observation);
        }
    }
    (training, holdout)
}

fn grouped_holdout_split(
    observations: &[MesoObservation],
    requested_fraction: f64,
    seed: u64,
    min_holdout_observations: usize,
    key_fn: fn(&MesoObservation) -> String,
) -> (Vec<MesoObservation>, Vec<MesoObservation>) {
    let fraction = requested_fraction.clamp(0.0, 0.95);
    let target_holdout_count =
        target_holdout_count(observations.len(), fraction, min_holdout_observations);
    if target_holdout_count == 0 {
        return (observations.to_vec(), Vec::new());
    }

    let mut groups = BTreeMap::<String, Vec<MesoObservation>>::new();
    for observation in observations {
        groups
            .entry(key_fn(observation))
            .or_default()
            .push(observation.clone());
    }
    if groups.len() < 2 {
        return station_hash_holdout_split(
            observations,
            requested_fraction,
            seed,
            min_holdout_observations,
        );
    }

    let mut scored = groups
        .into_iter()
        .map(|(key, observations)| {
            let score = holdout_group_score(key.as_str(), seed);
            (score, key, observations)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.len().cmp(&right.2.len()))
    });

    let mut holdout_group_indexes = BTreeSet::new();
    let mut holdout_count = 0usize;
    for (index, (_, _, group)) in scored.iter().enumerate() {
        if holdout_count >= target_holdout_count {
            break;
        }
        if holdout_count + group.len() >= observations.len() {
            continue;
        }
        holdout_group_indexes.insert(index);
        holdout_count += group.len();
    }
    if holdout_group_indexes.is_empty() {
        return station_hash_holdout_split(
            observations,
            requested_fraction,
            seed,
            min_holdout_observations,
        );
    }

    let mut training = Vec::with_capacity(observations.len().saturating_sub(holdout_count));
    let mut holdout = Vec::with_capacity(holdout_count);
    for (index, (_, _, group)) in scored.into_iter().enumerate() {
        if holdout_group_indexes.contains(&index) {
            holdout.extend(group);
        } else {
            training.extend(group);
        }
    }
    (training, holdout)
}

fn target_holdout_count(
    observation_count: usize,
    requested_fraction: f64,
    min_holdout_observations: usize,
) -> usize {
    let target_holdout_count = ((observation_count as f64) * requested_fraction).round() as usize;
    target_holdout_count
        .max(min_holdout_observations.min(observation_count.saturating_sub(1)))
        .min(observation_count.saturating_sub(1))
}

fn holdout_score(observation: &MesoObservation, seed: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    observation.source.hash(&mut hasher);
    observation.station_id.hash(&mut hasher);
    observation.timestamp.hash(&mut hasher);
    observation.latitude_deg.to_bits().hash(&mut hasher);
    observation.longitude_deg.to_bits().hash(&mut hasher);
    hasher.finish()
}

fn holdout_group_score(key: &str, seed: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    hasher.finish()
}

fn spatial_holdout_key(observation: &MesoObservation) -> String {
    let lat_bin = ((observation.latitude_deg + 90.0) / HOLDOUT_SPATIAL_BLOCK_DEG).floor() as i32;
    let lon_bin = ((observation.longitude_deg + 180.0) / HOLDOUT_SPATIAL_BLOCK_DEG).floor() as i32;
    format!("{lat_bin}:{lon_bin}")
}

fn source_holdout_key(observation: &MesoObservation) -> String {
    validation_source_key(observation.source.as_str())
}

pub fn validate_surface_mesoanalysis_at_observations(
    surface: &SurfaceFields,
    fields: &MesoanalysisFields,
    observations: &[MesoObservation],
) -> Result<SurfaceMesoanalysisValidationSummary, Box<dyn std::error::Error>> {
    validate_validation_lengths(surface, fields)?;
    let pressure_hpa = surface
        .psfc_pa
        .iter()
        .map(|value| value * 0.01)
        .collect::<Vec<_>>();
    let background_dewpoint_c =
        compute_dewpoint_from_pressure_and_mixing_ratio(&pressure_hpa, &surface.q2_kgkg)?;
    let grid_index = SurfaceValidationGridIndex::new(&surface.lat, &surface.lon);

    let mut temperature = ValidationAccumulator::default();
    let mut dewpoint = ValidationAccumulator::default();
    let mut wind_speed = ValidationAccumulator::default();
    let mut source_accumulators = BTreeMap::<String, SourceValidationAccumulator>::new();
    let mut strata_accumulators = BTreeMap::<(String, String), SourceValidationAccumulator>::new();
    let mut samples = Vec::new();
    let mut max_nearest_grid_distance_km = None::<f64>;
    let mut skipped_observation_count = 0usize;

    for observation in observations {
        let Some(nearest) = grid_index.nearest(
            observation.latitude_deg,
            observation.longitude_deg,
            VALIDATION_MAX_NEAREST_GRID_DISTANCE_KM,
        ) else {
            skipped_observation_count += 1;
            continue;
        };
        max_nearest_grid_distance_km = Some(
            max_nearest_grid_distance_km
                .map(|current| current.max(nearest.distance_km))
                .unwrap_or(nearest.distance_km),
        );
        let index = nearest.index;
        let temperature_c = station_variable_validation(
            observation.temperature_c,
            Some(surface.t2_k[index] - 273.15),
            Some(fields.temperature_2m_c[index]),
            Some(fields.temperature_confidence[index]),
        );
        let dewpoint_c = station_variable_validation(
            observation.dewpoint_c,
            Some(background_dewpoint_c[index]),
            Some(fields.dewpoint_2m_c[index]),
            Some(fields.dewpoint_confidence[index]),
        );
        let background_wind_speed_ms =
            (surface.u10_ms[index].powi(2) + surface.v10_ms[index].powi(2)).sqrt();
        let analysis_wind_speed_ms =
            (fields.u10_ms[index].powi(2) + fields.v10_ms[index].powi(2)).sqrt();
        let wind_speed_confidence = fields.u10_confidence[index].min(fields.v10_confidence[index]);
        let wind_speed_ms = station_variable_validation(
            observation.wind_speed_ms,
            Some(background_wind_speed_ms),
            Some(analysis_wind_speed_ms),
            Some(wind_speed_confidence),
        );
        temperature.push(temperature_c.as_ref());
        dewpoint.push(dewpoint_c.as_ref());
        wind_speed.push(wind_speed_ms.as_ref());
        let source_key = validation_source_key(observation.source.as_str());
        let source_accumulator = source_accumulators.entry(source_key).or_default();
        source_accumulator.push(
            temperature_c.as_ref(),
            dewpoint_c.as_ref(),
            wind_speed_ms.as_ref(),
        );
        let terrain_pressure_hpa = finite_option(surface.psfc_pa[index] * 0.01);
        let terrain_pressure_class = validation_terrain_pressure_class(terrain_pressure_hpa);
        let observation_age_bucket = observation
            .observation_age_minutes
            .map(validation_observation_age_bucket);
        push_validation_strata(
            &mut strata_accumulators,
            observation,
            terrain_pressure_class,
            observation_age_bucket.as_deref(),
            temperature_c.as_ref(),
            dewpoint_c.as_ref(),
            wind_speed_ms.as_ref(),
        );
        samples.push(StationMesoanalysisValidationSample {
            station_id: observation.station_id.clone(),
            source: observation.source.clone(),
            source_quality_class: observation.source_quality_class.clone(),
            representativeness_class: observation.representativeness_class.clone(),
            correction_role: observation.correction_role.clone(),
            timestamp: observation.timestamp.clone(),
            observation_age_minutes: observation.observation_age_minutes,
            observation_age_bucket,
            time_weight: observation.time_weight,
            latitude_deg: observation.latitude_deg,
            longitude_deg: observation.longitude_deg,
            grid_index: index,
            grid_latitude_deg: surface.lat[index],
            grid_longitude_deg: surface.lon[index],
            nearest_grid_distance_km: nearest.distance_km,
            terrain_pressure_hpa,
            terrain_pressure_class: terrain_pressure_class.to_string(),
            temperature_c,
            dewpoint_c,
            wind_speed_ms,
            mean_sea_level_pressure_hpa: None,
        });
    }

    Ok(SurfaceMesoanalysisValidationSummary {
        observation_count: observations.len(),
        sampled_observation_count: samples.len(),
        skipped_observation_count,
        max_nearest_grid_distance_km,
        temperature_c: temperature.finish(),
        dewpoint_c: dewpoint.finish(),
        wind_speed_ms: wind_speed.finish(),
        mean_sea_level_pressure_hpa: None,
        source_summaries: source_accumulators
            .into_iter()
            .map(|(source, accumulator)| accumulator.finish(source))
            .collect(),
        strata_summaries: strata_accumulators
            .into_iter()
            .map(|((stratum_type, stratum_value), accumulator)| {
                accumulator.finish_stratum(stratum_type, stratum_value)
            })
            .collect(),
        samples,
    })
}

pub fn validate_surface_reference_at_observations(
    surface: &SurfaceFields,
    observations: &[MesoObservation],
) -> Result<SurfaceMesoanalysisValidationSummary, Box<dyn std::error::Error>> {
    let fields = surface_fields_as_mesoanalysis_fields(surface)?;
    validate_surface_mesoanalysis_at_observations(surface, &fields, observations)
}

pub fn compare_surface_mesoanalysis_to_external_reference(
    descriptor: SurfaceMesoanalysisExternalReferenceDescriptor,
    candidate: &SurfaceMesoanalysisValidationSummary,
    reference: &SurfaceMesoanalysisValidationSummary,
) -> SurfaceMesoanalysisExternalReferenceComparison {
    let reference_samples = reference
        .samples
        .iter()
        .map(|sample| (validation_sample_key(sample), sample))
        .collect::<BTreeMap<_, _>>();

    let mut temperature = ExternalReferenceVariableAccumulator::default();
    let mut dewpoint = ExternalReferenceVariableAccumulator::default();
    let mut wind_speed = ExternalReferenceVariableAccumulator::default();
    let mut mean_sea_level_pressure = ExternalReferenceVariableAccumulator::default();
    let mut sampled_observation_count = 0usize;
    let mut max_nearest_grid_distance_km = None::<f64>;

    for candidate_sample in &candidate.samples {
        let Some(reference_sample) =
            reference_samples.get(&validation_sample_key(candidate_sample))
        else {
            continue;
        };
        sampled_observation_count += 1;
        let sample_distance = candidate_sample
            .nearest_grid_distance_km
            .max(reference_sample.nearest_grid_distance_km);
        max_nearest_grid_distance_km = Some(
            max_nearest_grid_distance_km
                .map(|current| current.max(sample_distance))
                .unwrap_or(sample_distance),
        );
        temperature.push(
            candidate_sample.temperature_c.as_ref(),
            reference_sample.temperature_c.as_ref(),
        );
        dewpoint.push(
            candidate_sample.dewpoint_c.as_ref(),
            reference_sample.dewpoint_c.as_ref(),
        );
        wind_speed.push(
            candidate_sample.wind_speed_ms.as_ref(),
            reference_sample.wind_speed_ms.as_ref(),
        );
        mean_sea_level_pressure.push(
            candidate_sample.mean_sea_level_pressure_hpa.as_ref(),
            reference_sample.mean_sea_level_pressure_hpa.as_ref(),
        );
    }

    let skipped_observation_count = candidate
        .observation_count
        .max(reference.observation_count)
        .saturating_sub(sampled_observation_count);

    let mean_sea_level_pressure_hpa = mean_sea_level_pressure.finish();
    SurfaceMesoanalysisExternalReferenceComparison {
        schema: "rustwx.surface_mesoanalysis.external_reference_comparison.v1".to_string(),
        reference_label: descriptor.reference_label,
        reference_model: descriptor.reference_model,
        reference_source: descriptor.reference_source,
        reference_cycle: descriptor.reference_cycle,
        reference_forecast_hour: descriptor.reference_forecast_hour,
        reference_product: descriptor.reference_product,
        candidate_label: descriptor.candidate_label,
        background_label: descriptor.background_label,
        validation_mode: descriptor.validation_mode,
        sampled_observation_count,
        skipped_observation_count,
        max_nearest_grid_distance_km,
        temperature_c: temperature
            .finish()
            .unwrap_or_else(empty_external_reference_variable),
        dewpoint_c: dewpoint
            .finish()
            .unwrap_or_else(empty_external_reference_variable),
        wind_speed_ms: wind_speed
            .finish()
            .unwrap_or_else(empty_external_reference_variable),
        mean_sea_level_pressure_hpa,
    }
}

pub fn evaluate_surface_mesoanalysis_validation_gate(
    validation: &SurfaceMesoanalysisValidationSummary,
    thresholds: SurfaceMesoanalysisValidationGateThresholds,
) -> SurfaceMesoanalysisValidationGate {
    let mut checks = Vec::new();
    push_min_gate_check(
        &mut checks,
        "sampled_observation_count",
        validation.sampled_observation_count as f64,
        thresholds.min_sampled_observations as f64,
        format!(
            "{} sampled observations; minimum required is {}",
            validation.sampled_observation_count, thresholds.min_sampled_observations
        ),
    );
    push_max_gate_check(
        &mut checks,
        "skipped_observation_count",
        Some(validation.skipped_observation_count as f64),
        thresholds.max_skipped_observations as f64,
        format!(
            "{} observations skipped during validation; maximum allowed is {}",
            validation.skipped_observation_count, thresholds.max_skipped_observations
        ),
    );
    push_max_gate_check(
        &mut checks,
        "max_nearest_grid_distance_km",
        validation.max_nearest_grid_distance_km,
        thresholds.max_nearest_grid_distance_km,
        format!(
            "maximum nearest-grid distance must be <= {:.3} km",
            thresholds.max_nearest_grid_distance_km
        ),
    );
    push_max_gate_check(
        &mut checks,
        "temperature_c_mean_abs_analysis_error",
        validation.temperature_c.mean_abs_analysis_error,
        thresholds.max_temperature_mean_abs_error_c,
        format!(
            "2 m temperature analysis MAE must be <= {:.3} C",
            thresholds.max_temperature_mean_abs_error_c
        ),
    );
    push_max_gate_check(
        &mut checks,
        "dewpoint_c_mean_abs_analysis_error",
        validation.dewpoint_c.mean_abs_analysis_error,
        thresholds.max_dewpoint_mean_abs_error_c,
        format!(
            "2 m dew point analysis MAE must be <= {:.3} C",
            thresholds.max_dewpoint_mean_abs_error_c
        ),
    );
    push_max_gate_check(
        &mut checks,
        "wind_speed_ms_mean_abs_analysis_error",
        validation.wind_speed_ms.mean_abs_analysis_error,
        thresholds.max_wind_speed_mean_abs_error_ms,
        format!(
            "10 m wind-speed analysis MAE must be <= {:.3} m/s",
            thresholds.max_wind_speed_mean_abs_error_ms
        ),
    );

    SurfaceMesoanalysisValidationGate {
        schema: "rustwx.surface_mesoanalysis.validation_gate.v1".to_string(),
        passed: checks.iter().all(|check| check.passed),
        thresholds,
        checks,
    }
}

pub fn compare_surface_mesoanalysis_validations(
    candidate_label: impl Into<String>,
    candidate: &SurfaceMesoanalysisValidationSummary,
    baseline_label: impl Into<String>,
    baseline: &SurfaceMesoanalysisValidationSummary,
) -> SurfaceMesoanalysisValidationComparison {
    SurfaceMesoanalysisValidationComparison {
        schema: "rustwx.surface_mesoanalysis.validation_comparison.v1".to_string(),
        candidate_label: candidate_label.into(),
        baseline_label: baseline_label.into(),
        temperature_c: compare_variable_validations(
            &candidate.temperature_c,
            &baseline.temperature_c,
        ),
        dewpoint_c: compare_variable_validations(&candidate.dewpoint_c, &baseline.dewpoint_c),
        wind_speed_ms: compare_variable_validations(
            &candidate.wind_speed_ms,
            &baseline.wind_speed_ms,
        ),
        mean_sea_level_pressure_hpa: candidate
            .mean_sea_level_pressure_hpa
            .as_ref()
            .zip(baseline.mean_sea_level_pressure_hpa.as_ref())
            .map(|(candidate, baseline)| compare_variable_validations(candidate, baseline)),
    }
}

pub fn benchmark_surface_mesoanalysis_validations(
    candidate_label: impl Into<String>,
    candidate: &SurfaceMesoanalysisValidationSummary,
    baseline_label: impl Into<String>,
    baseline: &SurfaceMesoanalysisValidationSummary,
    validation_mode: impl Into<String>,
) -> SurfaceMesoanalysisBenchmarkSummary {
    SurfaceMesoanalysisBenchmarkSummary {
        schema: "rustwx.surface_mesoanalysis.benchmark_summary.v1".to_string(),
        candidate_label: candidate_label.into(),
        baseline_label: baseline_label.into(),
        validation_mode: validation_mode.into(),
        temperature_c: benchmark_variable_validations(
            &candidate.temperature_c,
            &baseline.temperature_c,
        ),
        dewpoint_c: benchmark_variable_validations(&candidate.dewpoint_c, &baseline.dewpoint_c),
        wind_speed_ms: benchmark_variable_validations(
            &candidate.wind_speed_ms,
            &baseline.wind_speed_ms,
        ),
        mean_sea_level_pressure_hpa: candidate
            .mean_sea_level_pressure_hpa
            .as_ref()
            .zip(baseline.mean_sea_level_pressure_hpa.as_ref())
            .map(|(candidate, baseline)| benchmark_variable_validations(candidate, baseline)),
    }
}

pub fn benchmark_surface_mesoanalysis_repeated_holdout_validations(
    candidate_label: impl Into<String>,
    candidate: &SurfaceMesoanalysisRepeatedHoldoutValidationSummary,
    baseline_label: impl Into<String>,
    baseline: &SurfaceMesoanalysisRepeatedHoldoutValidationSummary,
    validation_mode: impl Into<String>,
) -> SurfaceMesoanalysisRepeatedHoldoutBenchmarkSummary {
    let candidate_label = candidate_label.into();
    let baseline_label = baseline_label.into();
    let validation_mode = validation_mode.into();
    let fold_count = candidate.folds.len().min(baseline.folds.len());
    let fold_benchmarks = candidate
        .folds
        .iter()
        .zip(baseline.folds.iter())
        .map(|(candidate, baseline)| {
            benchmark_surface_mesoanalysis_validations(
                candidate_label.clone(),
                &candidate.validation,
                baseline_label.clone(),
                &baseline.validation,
                validation_mode.clone(),
            )
        })
        .collect::<Vec<_>>();

    SurfaceMesoanalysisRepeatedHoldoutBenchmarkSummary {
        schema: "rustwx.surface_mesoanalysis.repeated_holdout_benchmark_summary.v1".to_string(),
        candidate_label,
        baseline_label,
        validation_mode,
        fold_count,
        temperature_c: summarize_repeated_variable_benchmarks(
            fold_benchmarks
                .iter()
                .map(|benchmark| &benchmark.temperature_c),
        ),
        dewpoint_c: summarize_repeated_variable_benchmarks(
            fold_benchmarks
                .iter()
                .map(|benchmark| &benchmark.dewpoint_c),
        ),
        wind_speed_ms: summarize_repeated_variable_benchmarks(
            fold_benchmarks
                .iter()
                .map(|benchmark| &benchmark.wind_speed_ms),
        ),
        mean_sea_level_pressure_hpa: summarize_repeated_optional_variable_benchmarks(
            fold_benchmarks
                .iter()
                .map(|benchmark| benchmark.mean_sea_level_pressure_hpa.as_ref()),
        ),
    }
}

fn compare_variable_validations(
    candidate: &VariableValidationSummary,
    baseline: &VariableValidationSummary,
) -> VariableValidationComparison {
    VariableValidationComparison {
        candidate_observation_count: candidate.observation_count,
        baseline_observation_count: baseline.observation_count,
        candidate_mean_abs_analysis_error: candidate.mean_abs_analysis_error,
        baseline_mean_abs_analysis_error: baseline.mean_abs_analysis_error,
        mean_abs_analysis_error_delta: option_delta(
            candidate.mean_abs_analysis_error,
            baseline.mean_abs_analysis_error,
        ),
        candidate_analysis_rmse: candidate.analysis_rmse,
        baseline_analysis_rmse: baseline.analysis_rmse,
        analysis_rmse_delta: option_delta(candidate.analysis_rmse, baseline.analysis_rmse),
        candidate_mean_analysis_error: candidate.mean_analysis_error,
        baseline_mean_analysis_error: baseline.mean_analysis_error,
        mean_analysis_error_delta: option_delta(
            candidate.mean_analysis_error,
            baseline.mean_analysis_error,
        ),
    }
}

fn summarize_repeated_optional_variable_benchmarks<'a>(
    variables: impl Iterator<Item = Option<&'a VariableBenchmarkSummary>>,
) -> Option<VariableRepeatedBenchmarkSummary> {
    let variables = variables.flatten().collect::<Vec<_>>();
    if variables.is_empty() {
        None
    } else {
        Some(summarize_repeated_variable_benchmarks(
            variables.into_iter(),
        ))
    }
}

fn summarize_repeated_variable_benchmarks<'a>(
    variables: impl Iterator<Item = &'a VariableBenchmarkSummary>,
) -> VariableRepeatedBenchmarkSummary {
    let variables = variables.collect::<Vec<_>>();
    VariableRepeatedBenchmarkSummary {
        fold_count: variables.len(),
        background_mean_abs_error: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.background_mean_abs_error),
        ),
        candidate_mean_abs_error: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.candidate_mean_abs_error),
        ),
        baseline_mean_abs_error: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.baseline_mean_abs_error),
        ),
        candidate_minus_background_mae: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.candidate_minus_background_mae),
        ),
        candidate_minus_baseline_mae: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.candidate_minus_baseline_mae),
        ),
        background_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.background_rmse),
        ),
        candidate_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.candidate_rmse),
        ),
        baseline_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.baseline_rmse),
        ),
        candidate_minus_background_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.candidate_minus_background_rmse),
        ),
        candidate_minus_baseline_rmse: mean_present_values(
            variables
                .iter()
                .filter_map(|variable| variable.candidate_minus_baseline_rmse),
        ),
        candidate_beats_background_mae_fold_count: variables
            .iter()
            .filter(|variable| {
                option_less_than(
                    variable.candidate_mean_abs_error,
                    variable.background_mean_abs_error,
                )
            })
            .count(),
        candidate_beats_baseline_mae_fold_count: variables
            .iter()
            .filter(|variable| {
                option_less_than(
                    variable.candidate_mean_abs_error,
                    variable.baseline_mean_abs_error,
                )
            })
            .count(),
        candidate_beats_background_rmse_fold_count: variables
            .iter()
            .filter(|variable| option_less_than(variable.candidate_rmse, variable.background_rmse))
            .count(),
        candidate_beats_baseline_rmse_fold_count: variables
            .iter()
            .filter(|variable| option_less_than(variable.candidate_rmse, variable.baseline_rmse))
            .count(),
    }
}

fn benchmark_variable_validations(
    candidate: &VariableValidationSummary,
    baseline: &VariableValidationSummary,
) -> VariableBenchmarkSummary {
    let background_mean_abs_error = candidate
        .mean_abs_background_error
        .or(baseline.mean_abs_background_error);
    let background_rmse = candidate.background_rmse.or(baseline.background_rmse);

    VariableBenchmarkSummary {
        candidate_observation_count: candidate.observation_count,
        baseline_observation_count: baseline.observation_count,
        background_mean_abs_error,
        candidate_mean_abs_error: candidate.mean_abs_analysis_error,
        baseline_mean_abs_error: baseline.mean_abs_analysis_error,
        candidate_minus_background_mae: option_delta(
            candidate.mean_abs_analysis_error,
            background_mean_abs_error,
        ),
        baseline_minus_background_mae: option_delta(
            baseline.mean_abs_analysis_error,
            background_mean_abs_error,
        ),
        candidate_minus_baseline_mae: option_delta(
            candidate.mean_abs_analysis_error,
            baseline.mean_abs_analysis_error,
        ),
        background_rmse,
        candidate_rmse: candidate.analysis_rmse,
        baseline_rmse: baseline.analysis_rmse,
        candidate_minus_background_rmse: option_delta(candidate.analysis_rmse, background_rmse),
        baseline_minus_background_rmse: option_delta(baseline.analysis_rmse, background_rmse),
        candidate_minus_baseline_rmse: option_delta(
            candidate.analysis_rmse,
            baseline.analysis_rmse,
        ),
        mae_winner: lower_error_winner(
            ("background", background_mean_abs_error),
            ("candidate", candidate.mean_abs_analysis_error),
            ("baseline", baseline.mean_abs_analysis_error),
        ),
        rmse_winner: lower_error_winner(
            ("background", background_rmse),
            ("candidate", candidate.analysis_rmse),
            ("baseline", baseline.analysis_rmse),
        ),
    }
}

fn lower_error_winner(
    background: (&'static str, Option<f64>),
    candidate: (&'static str, Option<f64>),
    baseline: (&'static str, Option<f64>),
) -> String {
    let mut values = [background, candidate, baseline]
        .into_iter()
        .filter_map(|(label, value)| value.map(|value| (label, value)));
    let Some(mut best) = values.next() else {
        return "unavailable".to_string();
    };
    let mut tied = false;
    for value in values {
        if value.1 < best.1 - 1.0e-12 {
            best = value;
            tied = false;
        } else if (value.1 - best.1).abs() <= 1.0e-12 {
            tied = true;
        }
    }
    if tied {
        "tie".to_string()
    } else {
        best.0.to_string()
    }
}

fn option_delta(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    candidate
        .zip(baseline)
        .map(|(candidate, baseline)| candidate - baseline)
}

fn option_less_than(left: Option<f64>, right: Option<f64>) -> bool {
    left.zip(right)
        .map(|(left, right)| left < right)
        .unwrap_or(false)
}

fn mean_present_values(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut count = 0usize;
    let mut sum = 0.0;
    for value in values {
        if value.is_finite() {
            count += 1;
            sum += value;
        }
    }
    if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    }
}

pub fn write_surface_mesoanalysis_report(
    path: &Path,
    report: &SurfaceMesoanalysisReport,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

pub fn write_surface_mesoanalysis_grid_export(
    request: &SurfaceMesoanalysisGridExportRequest,
    surface: &SurfaceFields,
    fields: &MesoanalysisFields,
) -> Result<SurfaceMesoanalysisGridExportManifest, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    validate_validation_lengths(surface, fields)?;
    fs::create_dir_all(&request.out_dir)?;
    let nx = surface.nx;
    let ny = surface.ny;
    let bounds = bounds_from_surface_latlon(&surface.lat, &surface.lon);
    let lat_path = PathBuf::from("grid_lat.f32");
    let lon_path = PathBuf::from("grid_lon.f32");
    let write_start = Instant::now();
    write_f64_as_f32_file(&request.out_dir.join(&lat_path), &surface.lat)?;
    write_lon_f64_as_f32_file(&request.out_dir.join(&lon_path), &surface.lon)?;

    let mut records = Vec::new();
    write_meso_export_record(
        request,
        surface,
        "meso_temperature_2m_c",
        "Mesoanalysis 2 m temperature",
        "degC",
        &fields.temperature_2m_c,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_dewpoint_2m_c",
        "Mesoanalysis 2 m dew point",
        "degC",
        &fields.dewpoint_2m_c,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_q2_kgkg",
        "Mesoanalysis 2 m water-vapor mixing ratio",
        "kg kg-1",
        &fields.q2_kgkg,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_u10_ms",
        "Mesoanalysis 10 m u wind",
        "m s-1",
        &fields.u10_ms,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_v10_ms",
        "Mesoanalysis 10 m v wind",
        "m s-1",
        &fields.v10_ms,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    let wind_speed_10m_ms = fields
        .u10_ms
        .iter()
        .zip(fields.v10_ms.iter())
        .map(|(&u, &v)| (u * u + v * v).sqrt())
        .collect::<Vec<_>>();
    write_meso_export_record(
        request,
        surface,
        "meso_wind_speed_10m_ms",
        "Mesoanalysis 10 m wind speed",
        "m s-1",
        &wind_speed_10m_ms,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    if let Some(values) = fields.mean_sea_level_pressure_hpa.as_ref() {
        write_meso_export_record(
            request,
            surface,
            "meso_mean_sea_level_pressure_hpa",
            "Mesoanalysis mean sea-level pressure",
            "hPa",
            values,
            &lat_path,
            &lon_path,
            bounds,
            &mut records,
        )?;
    }
    write_meso_export_record(
        request,
        surface,
        "meso_temperature_increment_c",
        "Mesoanalysis 2 m temperature increment",
        "degC",
        &fields.temperature_increment_c,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_dewpoint_increment_c",
        "Mesoanalysis 2 m dew point increment",
        "degC",
        &fields.dewpoint_increment_c,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_u10_increment_ms",
        "Mesoanalysis 10 m u-wind increment",
        "m s-1",
        &fields.u10_increment_ms,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_v10_increment_ms",
        "Mesoanalysis 10 m v-wind increment",
        "m s-1",
        &fields.v10_increment_ms,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    if let Some(values) = fields.mean_sea_level_pressure_increment_hpa.as_ref() {
        write_meso_export_record(
            request,
            surface,
            "meso_mean_sea_level_pressure_increment_hpa",
            "Mesoanalysis mean sea-level pressure increment",
            "hPa",
            values,
            &lat_path,
            &lon_path,
            bounds,
            &mut records,
        )?;
    }
    write_meso_export_record(
        request,
        surface,
        "meso_temperature_confidence",
        "Mesoanalysis 2 m temperature confidence",
        "1",
        &fields.temperature_confidence,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_dewpoint_confidence",
        "Mesoanalysis 2 m dew point confidence",
        "1",
        &fields.dewpoint_confidence,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_u10_confidence",
        "Mesoanalysis 10 m u-wind confidence",
        "1",
        &fields.u10_confidence,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    write_meso_export_record(
        request,
        surface,
        "meso_v10_confidence",
        "Mesoanalysis 10 m v-wind confidence",
        "1",
        &fields.v10_confidence,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;
    if let Some(values) = fields.mean_sea_level_pressure_confidence.as_ref() {
        write_meso_export_record(
            request,
            surface,
            "meso_mean_sea_level_pressure_confidence",
            "Mesoanalysis mean sea-level pressure confidence",
            "1",
            values,
            &lat_path,
            &lon_path,
            bounds,
            &mut records,
        )?;
    }
    let neighbor_count = fields
        .neighbor_count
        .iter()
        .map(|&value| f64::from(value))
        .collect::<Vec<_>>();
    write_meso_export_record(
        request,
        surface,
        "meso_neighbor_count",
        "Mesoanalysis objective-analysis neighbor count",
        "count",
        &neighbor_count,
        &lat_path,
        &lon_path,
        bounds,
        &mut records,
    )?;

    let manifest_path = request.out_dir.join("manifest.json");
    let manifest = SurfaceMesoanalysisGridExportManifest {
        schema: "rustwx.surface_mesoanalysis.grid_export.v1".to_string(),
        model: request.model.clone(),
        run_id: request.run_id.clone(),
        member: request.member.clone(),
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: request.cycle_utc,
        source: request.source.clone(),
        forecast_hours: vec![request.forecast_hour],
        generated_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        manifest_path: manifest_path.clone(),
        fields: records,
        blockers: Vec::new(),
        timing: SurfaceMesoanalysisGridExportTiming {
            total_ms: total_start.elapsed().as_millis(),
            write_ms: write_start.elapsed().as_millis(),
        },
    };
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    debug_assert_eq!(nx * ny, surface.lat.len());
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
fn write_meso_export_record(
    request: &SurfaceMesoanalysisGridExportRequest,
    surface: &SurfaceFields,
    product_slug: &str,
    title: &str,
    units: &str,
    values: &[f64],
    lat_path: &Path,
    lon_path: &Path,
    bounds: Option<[f64; 4]>,
    records: &mut Vec<SurfaceMesoanalysisGridExportRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = surface.nx * surface.ny;
    if values.len() != expected {
        return Err(format!(
            "{product_slug} length {} did not match surface grid length {expected}",
            values.len()
        )
        .into());
    }
    let values_path = PathBuf::from(format!(
        "{product_slug}_f{:03}_values.f32",
        request.forecast_hour
    ));
    let no_data = write_f64_as_f32_file(&request.out_dir.join(&values_path), values)?;
    records.push(SurfaceMesoanalysisGridExportRecord {
        product_slug: product_slug.to_string(),
        title: title.to_string(),
        units: units.to_string(),
        model: request.model.clone(),
        run_id: request.run_id.clone(),
        member: request.member.clone(),
        forecast_hour: request.forecast_hour,
        valid_time: request.valid_time.clone(),
        nx: surface.nx,
        ny: surface.ny,
        crop: None,
        bounds,
        values_path,
        lat_path: lat_path.to_path_buf(),
        lon_path: lon_path.to_path_buf(),
        no_data,
    });
    Ok(())
}

fn write_f64_as_f32_file(
    path: &Path,
    values: &[f64],
) -> Result<SurfaceMesoanalysisGridNoDataInfo, Box<dyn std::error::Error>> {
    let mut file = BufWriter::new(File::create(path)?);
    let mut bytes = Vec::with_capacity(64 * 1024 * 4);
    let mut finite_count = 0usize;
    for chunk in values.chunks(64 * 1024) {
        bytes.clear();
        for &value in chunk {
            let value = if value.is_finite() {
                finite_count += 1;
                value as f32
            } else {
                f32::NAN
            };
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        file.write_all(&bytes)?;
    }
    file.flush()?;
    Ok(SurfaceMesoanalysisGridNoDataInfo {
        encoding: "nan".to_string(),
        finite_count,
        nan_count: values.len().saturating_sub(finite_count),
    })
}

fn write_lon_f64_as_f32_file(
    path: &Path,
    values: &[f64],
) -> Result<SurfaceMesoanalysisGridNoDataInfo, Box<dyn std::error::Error>> {
    let normalized = values
        .iter()
        .map(|&value| normalize_lon(value))
        .collect::<Vec<_>>();
    write_f64_as_f32_file(path, &normalized)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TimeFilterDecision {
    Pass { age_minutes: Option<f64> },
    Filtered,
    MissingOrInvalidTime,
}

fn observation_passes_time_filter(
    observation: &MesoObservation,
    options: &RunnerMesoObservationLoadOptions,
) -> TimeFilterDecision {
    let (Some(reference_time), Some(max_age_minutes)) =
        (options.reference_time_utc.as_ref(), options.max_age_minutes)
    else {
        return TimeFilterDecision::Pass { age_minutes: None };
    };
    let Some(timestamp) = observation.timestamp.as_deref() else {
        return TimeFilterDecision::MissingOrInvalidTime;
    };
    let Some(observation_time) = parse_observation_time_utc(timestamp) else {
        return TimeFilterDecision::MissingOrInvalidTime;
    };
    let delta_seconds = reference_time
        .signed_duration_since(observation_time)
        .num_seconds();
    if delta_seconds < -(options.allow_future_minutes * 60) || delta_seconds > max_age_minutes * 60
    {
        TimeFilterDecision::Filtered
    } else {
        TimeFilterDecision::Pass {
            age_minutes: Some(delta_seconds as f64 / 60.0),
        }
    }
}

fn apply_time_representativeness(
    observation: &mut MesoObservation,
    age_minutes: Option<f64>,
    options: &RunnerMesoObservationLoadOptions,
) -> Option<f64> {
    let age_minutes = age_minutes.filter(|value| value.is_finite())?;
    observation.observation_age_minutes = Some(age_minutes);
    let half_life = options
        .time_weight_half_life_minutes
        .filter(|value| value.is_finite() && *value > 0.0)?;
    let max_inflation = if options.max_time_error_inflation_factor.is_finite()
        && options.max_time_error_inflation_factor >= 1.0
    {
        options.max_time_error_inflation_factor
    } else {
        DEFAULT_MAX_TIME_ERROR_INFLATION_FACTOR
    };
    let age_for_weight = age_minutes.max(0.0);
    let min_weight = (1.0 / (max_inflation * max_inflation)).clamp(0.01, 1.0);
    let time_weight = (-(std::f64::consts::LN_2 * age_for_weight) / half_life)
        .exp()
        .clamp(min_weight, 1.0);
    let error_inflation = (1.0 / time_weight.sqrt()).min(max_inflation);
    observation.quality_weight *= time_weight;
    observation.time_weight = Some(time_weight);
    if let Some(value) = observation.temperature_error_c.as_mut() {
        *value *= error_inflation;
    }
    if let Some(value) = observation.dewpoint_error_c.as_mut() {
        *value *= error_inflation;
    }
    if let Some(value) = observation.wind_error_ms.as_mut() {
        *value *= error_inflation;
    }
    if let Some(value) = observation.mean_sea_level_pressure_error_hpa.as_mut() {
        *value *= error_inflation;
    }
    Some(time_weight)
}

fn parse_observation_time_utc(timestamp: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn runner_observation_to_meso(
    fallback_source: &str,
    fallback_kind: &str,
    observation: &RunnerDirectObservation,
) -> Option<MesoObservation> {
    if !(observation.latitude.is_finite() && observation.longitude.is_finite()) {
        return None;
    }
    let station_id = if observation.station_id.is_empty() {
        "unknown".to_string()
    } else {
        observation.station_id.clone()
    };
    let source = if observation.source.is_empty() {
        fallback_source.to_string()
    } else {
        observation.source.clone()
    };
    let source_quality = runner_observation_source_quality_profile(source.as_str(), fallback_kind);
    let mut meso = MesoObservation::new(station_id, observation.latitude, observation.longitude)
        .with_source(&source);
    meso.timestamp = observation.timestamp.clone();
    meso.source_quality_class = Some(source_quality.source_quality_class.to_string());
    meso.representativeness_class = Some(source_quality.representativeness_class.to_string());
    meso.correction_role = Some(source_quality.correction_role.to_string());
    if let Some(value) = observation.temperature_f.and_then(f_to_c) {
        meso.temperature_c = Some(value);
    }
    if let Some(value) = observation.dewpoint_f.and_then(f_to_c) {
        meso.dewpoint_c = Some(value);
    }
    if let (Some(direction), Some(speed_kts)) =
        (observation.wind_direction_deg, observation.wind_speed_kts)
    {
        if direction.is_finite() && speed_kts.is_finite() && speed_kts >= 0.0 {
            meso.wind_direction_deg = Some(direction);
            meso.wind_speed_ms = Some(speed_kts * KTS_TO_MS);
        }
    }
    if let Some(value) = normalized_mean_sea_level_pressure_hpa(observation) {
        meso.mean_sea_level_pressure_hpa = Some(value);
    }
    meso.quality_weight = source_quality.quality_weight;
    let error_profile = source_quality.error_profile;
    if meso.temperature_c.is_some() {
        meso.temperature_error_c = Some(error_profile.temperature_c);
    }
    if meso.dewpoint_c.is_some() {
        meso.dewpoint_error_c = Some(error_profile.dewpoint_c);
    }
    if meso.wind_speed_ms.is_some() {
        meso.wind_error_ms = Some(error_profile.wind_ms);
    }
    if meso.mean_sea_level_pressure_hpa.is_some() {
        meso.mean_sea_level_pressure_error_hpa = Some(error_profile.mean_sea_level_pressure_hpa);
    }
    if meso.temperature_c.is_none() && meso.dewpoint_c.is_none() && meso.wind_speed_ms.is_none() {
        None
    } else {
        Some(meso)
    }
}

fn observation_passes_profile(
    observation: &MesoObservation,
    kind: &str,
    profile: RunnerMesoObservationProfile,
) -> bool {
    match profile {
        RunnerMesoObservationProfile::AllCurrentSurface => true,
        RunnerMesoObservationProfile::SurfaceMesoConus => {
            observation_in_conus(observation.latitude_deg, observation.longitude_deg)
                && runner_kind_is_surface_meso_conus_candidate(kind)
        }
    }
}

fn observation_in_conus(latitude: f64, longitude: f64) -> bool {
    latitude.is_finite()
        && longitude.is_finite()
        && (24.0..=50.5).contains(&latitude)
        && (-125.5..=-66.0).contains(&longitude)
}

fn runner_kind_is_surface_meso_conus_candidate(kind: &str) -> bool {
    matches!(
        kind,
        "asos_awos_metar"
            | "mesonet_observation"
            | "mesonet_5min"
            | "mesonet_current_5min"
            | "mesonet_current_15min"
            | "mesonet_hourly"
            | "mesonet_hourly_ag_weather"
            | "marine_current_observation"
            | "rwis_weather"
            | "rwis_current"
            | "raws_current_weather"
            | "coastal_meteorology_current"
    )
}

fn runner_observation_quality_weight(source: &str, kind: &str) -> f64 {
    match kind {
        "asos_awos_metar" => 1.0,
        "mesonet_observation"
        | "mesonet_5min"
        | "mesonet_current_5min"
        | "mesonet_current_15min" => 1.15,
        "mesonet_hourly" | "mesonet_hourly_ag_weather" => 1.05,
        "marine_current_observation" | "coastal_meteorology_current" => 0.85,
        "raws_current_weather" | "rwis_weather" | "rwis_current" => 0.65,
        "snotel_station_observation"
        | "snotel_hourly"
        | "scan_station_observation"
        | "scan_hourly" => 0.55,
        _ if source.contains("raws") || source.contains("rwis") => 0.65,
        _ => 1.0,
    }
}

fn runner_observation_error_profile(source: &str, kind: &str) -> RunnerObservationErrorProfile {
    match kind {
        "asos_awos_metar" => RunnerObservationErrorProfile {
            temperature_c: 0.8,
            dewpoint_c: 1.0,
            wind_ms: 1.5,
            mean_sea_level_pressure_hpa: 0.7,
        },
        "mesonet_observation"
        | "mesonet_5min"
        | "mesonet_current_5min"
        | "mesonet_current_15min" => RunnerObservationErrorProfile {
            temperature_c: 1.0,
            dewpoint_c: 1.3,
            wind_ms: 2.0,
            mean_sea_level_pressure_hpa: 1.0,
        },
        "mesonet_hourly" | "mesonet_hourly_ag_weather" => RunnerObservationErrorProfile {
            temperature_c: 1.2,
            dewpoint_c: 1.5,
            wind_ms: 2.2,
            mean_sea_level_pressure_hpa: 1.1,
        },
        "marine_current_observation" | "coastal_meteorology_current" => {
            RunnerObservationErrorProfile {
                temperature_c: 1.2,
                dewpoint_c: 1.5,
                wind_ms: 2.0,
                mean_sea_level_pressure_hpa: 1.0,
            }
        }
        "raws_current_weather" => RunnerObservationErrorProfile {
            temperature_c: 1.5,
            dewpoint_c: 2.0,
            wind_ms: 2.5,
            mean_sea_level_pressure_hpa: 1.5,
        },
        "rwis_weather" | "rwis_current" => RunnerObservationErrorProfile {
            temperature_c: 2.0,
            dewpoint_c: 2.5,
            wind_ms: 3.0,
            mean_sea_level_pressure_hpa: 2.0,
        },
        "snotel_station_observation"
        | "snotel_hourly"
        | "scan_station_observation"
        | "scan_hourly" => RunnerObservationErrorProfile {
            temperature_c: 2.0,
            dewpoint_c: 2.5,
            wind_ms: 3.5,
            mean_sea_level_pressure_hpa: 2.0,
        },
        _ if source.contains("raws") => RunnerObservationErrorProfile {
            temperature_c: 1.5,
            dewpoint_c: 2.0,
            wind_ms: 2.5,
            mean_sea_level_pressure_hpa: 1.5,
        },
        _ if source.contains("rwis") => RunnerObservationErrorProfile {
            temperature_c: 2.0,
            dewpoint_c: 2.5,
            wind_ms: 3.0,
            mean_sea_level_pressure_hpa: 2.0,
        },
        _ => RunnerObservationErrorProfile {
            temperature_c: 2.5,
            dewpoint_c: 3.0,
            wind_ms: 3.5,
            mean_sea_level_pressure_hpa: 2.5,
        },
    }
}

fn runner_observation_source_quality_profile(
    source: &str,
    kind: &str,
) -> RunnerObservationSourceQualityProfile {
    let source_lc = source.to_ascii_lowercase();
    let (source_quality_class, representativeness_class, correction_role) = match kind {
        "asos_awos_metar" => (
            "aviation_reference",
            "synoptic_airport_surface",
            "primary_correction_and_verification",
        ),
        "mesonet_observation"
        | "mesonet_5min"
        | "mesonet_current_5min"
        | "mesonet_current_15min" => (
            "high_frequency_mesonet",
            "near_surface_mesonet",
            "primary_correction_with_source_error",
        ),
        "mesonet_hourly" | "mesonet_hourly_ag_weather" => (
            "hourly_or_ag_mesonet",
            "near_surface_agricultural_mesonet",
            "supplemental_correction_with_source_error",
        ),
        "marine_current_observation" | "coastal_meteorology_current" => (
            "marine_or_coastal",
            "marine_coastal_exposure",
            "marine_coastal_correction",
        ),
        "raws_current_weather" => (
            "fire_weather_raws",
            "fire_weather_exposure",
            "supplemental_correction_with_representativeness_inflation",
        ),
        "rwis_weather" | "rwis_current" => (
            "road_weather_rwis",
            "road_microclimate",
            "supplemental_or_context_with_representativeness_inflation",
        ),
        "snotel_station_observation" | "snotel_hourly" => (
            "snow_telemetry",
            "mountain_hydrology_exposure",
            "context_or_sparse_correction",
        ),
        "scan_station_observation" | "scan_hourly" => (
            "soil_climate_network",
            "agricultural_soil_climate_exposure",
            "context_or_sparse_correction",
        ),
        _ if source_lc.contains("raws") => (
            "fire_weather_raws",
            "fire_weather_exposure",
            "supplemental_correction_with_representativeness_inflation",
        ),
        _ if source_lc.contains("rwis") => (
            "road_weather_rwis",
            "road_microclimate",
            "supplemental_or_context_with_representativeness_inflation",
        ),
        _ => (
            "generic_surface",
            "unknown_or_mixed_exposure",
            "supplemental_context_until_calibrated",
        ),
    };
    RunnerObservationSourceQualityProfile {
        source_quality_class,
        representativeness_class,
        correction_role,
        quality_weight: runner_observation_quality_weight(source, kind),
        error_profile: runner_observation_error_profile(source, kind),
    }
}

fn normalized_mean_sea_level_pressure_hpa(observation: &RunnerDirectObservation) -> Option<f64> {
    observation
        .sea_level_pressure_mb
        .filter(|value| (MIN_MSLP_HPA..=MAX_MSLP_HPA).contains(value))
}

fn normalized_station_pressure_hpa(observation: &RunnerDirectObservation) -> Option<f64> {
    observation
        .station_pressure_mb
        .filter(|value| (MIN_STATION_PRESSURE_HPA..=MAX_STATION_PRESSURE_HPA).contains(value))
}

fn normalized_altimeter_inhg(observation: &RunnerDirectObservation) -> Option<f64> {
    observation
        .altimeter_inhg
        .filter(|value| (MIN_ALTIMETER_INHG..=MAX_ALTIMETER_INHG).contains(value))
}

fn runner_kind_is_current_surface_candidate(kind: &str) -> bool {
    matches!(
        kind,
        "asos_awos_metar"
            | "mesonet_observation"
            | "mesonet_5min"
            | "mesonet_current_5min"
            | "mesonet_current_15min"
            | "mesonet_hourly"
            | "mesonet_hourly_ag_weather"
            | "marine_current_observation"
            | "rwis_weather"
            | "rwis_current"
            | "raws_current_weather"
            | "coastal_meteorology_current"
            | "snotel_station_observation"
            | "snotel_hourly"
            | "scan_station_observation"
            | "scan_hourly"
    )
}

fn f_to_c(value_f: f64) -> Option<f64> {
    if value_f.is_finite() {
        Some((value_f - F_TO_C_OFFSET) * F_TO_C_SCALE)
    } else {
        None
    }
}

fn summarize_field(values: &[f64]) -> FieldSummary {
    let mut finite_count = 0usize;
    let mut min = None::<f64>;
    let mut max = None::<f64>;
    let mut sum = 0.0;
    for &value in values {
        if !value.is_finite() {
            continue;
        }
        finite_count += 1;
        min = Some(min.map(|current| current.min(value)).unwrap_or(value));
        max = Some(max.map(|current| current.max(value)).unwrap_or(value));
        sum += value;
    }
    FieldSummary {
        finite_count,
        min,
        max,
        mean: if finite_count > 0 {
            Some(sum / finite_count as f64)
        } else {
            None
        },
    }
}

fn summarize_counts(values: &[u16]) -> CountSummary {
    let grid_cells = values.len();
    let covered_grid_cells = values.iter().filter(|&&value| value > 0).count();
    let max = values.iter().copied().max().unwrap_or(0);
    let sum: usize = values.iter().map(|&value| value as usize).sum();
    CountSummary {
        grid_cells,
        covered_grid_cells,
        max,
        mean: if grid_cells > 0 {
            sum as f64 / grid_cells as f64
        } else {
            0.0
        },
    }
}

fn validate_validation_lengths(
    surface: &SurfaceFields,
    fields: &MesoanalysisFields,
) -> Result<(), Box<dyn std::error::Error>> {
    let len = surface.nx * surface.ny;
    for (name, found) in [
        ("surface.lat", surface.lat.len()),
        ("surface.lon", surface.lon.len()),
        ("surface.psfc_pa", surface.psfc_pa.len()),
        ("surface.t2_k", surface.t2_k.len()),
        ("surface.q2_kgkg", surface.q2_kgkg.len()),
        ("surface.u10_ms", surface.u10_ms.len()),
        ("surface.v10_ms", surface.v10_ms.len()),
        ("fields.temperature_2m_c", fields.temperature_2m_c.len()),
        ("fields.dewpoint_2m_c", fields.dewpoint_2m_c.len()),
        ("fields.q2_kgkg", fields.q2_kgkg.len()),
        ("fields.u10_ms", fields.u10_ms.len()),
        ("fields.v10_ms", fields.v10_ms.len()),
        (
            "fields.temperature_increment_c",
            fields.temperature_increment_c.len(),
        ),
        (
            "fields.dewpoint_increment_c",
            fields.dewpoint_increment_c.len(),
        ),
        ("fields.u10_increment_ms", fields.u10_increment_ms.len()),
        ("fields.v10_increment_ms", fields.v10_increment_ms.len()),
        (
            "fields.temperature_confidence",
            fields.temperature_confidence.len(),
        ),
        (
            "fields.dewpoint_confidence",
            fields.dewpoint_confidence.len(),
        ),
        ("fields.u10_confidence", fields.u10_confidence.len()),
        ("fields.v10_confidence", fields.v10_confidence.len()),
        ("fields.neighbor_count", fields.neighbor_count.len()),
    ] {
        if found != len {
            return Err(
                format!("{name} length {found} did not match surface grid length {len}").into(),
            );
        }
    }
    for (name, values) in [
        (
            "fields.mean_sea_level_pressure_hpa",
            fields.mean_sea_level_pressure_hpa.as_ref(),
        ),
        (
            "fields.mean_sea_level_pressure_increment_hpa",
            fields.mean_sea_level_pressure_increment_hpa.as_ref(),
        ),
        (
            "fields.mean_sea_level_pressure_confidence",
            fields.mean_sea_level_pressure_confidence.as_ref(),
        ),
    ] {
        if let Some(values) = values {
            if values.len() != len {
                return Err(format!(
                    "{name} length {} did not match surface grid length {len}",
                    values.len()
                )
                .into());
            }
        }
    }
    Ok(())
}

fn push_min_gate_check(
    checks: &mut Vec<SurfaceMesoanalysisValidationGateCheck>,
    name: &str,
    observed: f64,
    threshold: f64,
    message: String,
) {
    checks.push(SurfaceMesoanalysisValidationGateCheck {
        name: name.to_string(),
        passed: observed >= threshold,
        observed: Some(observed),
        threshold,
        comparator: ">=".to_string(),
        message,
    });
}

fn push_max_gate_check(
    checks: &mut Vec<SurfaceMesoanalysisValidationGateCheck>,
    name: &str,
    observed: Option<f64>,
    threshold: f64,
    message: String,
) {
    let passed = observed
        .map(|value| value.is_finite() && value <= threshold)
        .unwrap_or(false);
    checks.push(SurfaceMesoanalysisValidationGateCheck {
        name: name.to_string(),
        passed,
        observed,
        threshold,
        comparator: "<=".to_string(),
        message,
    });
}

fn surface_fields_as_mesoanalysis_fields(
    surface: &SurfaceFields,
) -> Result<MesoanalysisFields, Box<dyn std::error::Error>> {
    let len = surface.lat.len();
    validate_surface_field_lengths(surface)?;
    let pressure_hpa = surface
        .psfc_pa
        .iter()
        .map(|value| value * 0.01)
        .collect::<Vec<_>>();
    let dewpoint_2m_c =
        compute_dewpoint_from_pressure_and_mixing_ratio(&pressure_hpa, &surface.q2_kgkg)?;
    Ok(MesoanalysisFields {
        temperature_2m_c: surface.t2_k.iter().map(|value| value - 273.15).collect(),
        dewpoint_2m_c,
        q2_kgkg: surface.q2_kgkg.clone(),
        u10_ms: surface.u10_ms.clone(),
        v10_ms: surface.v10_ms.clone(),
        mean_sea_level_pressure_hpa: None,
        temperature_increment_c: vec![0.0; len],
        dewpoint_increment_c: vec![0.0; len],
        u10_increment_ms: vec![0.0; len],
        v10_increment_ms: vec![0.0; len],
        mean_sea_level_pressure_increment_hpa: None,
        neighbor_count: vec![0; len],
        temperature_confidence: vec![1.0; len],
        dewpoint_confidence: vec![1.0; len],
        u10_confidence: vec![1.0; len],
        v10_confidence: vec![1.0; len],
        mean_sea_level_pressure_confidence: None,
        diagnostics: Vec::new(),
    })
}

fn validate_surface_field_lengths(
    surface: &SurfaceFields,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = surface.nx * surface.ny;
    for (name, len) in [
        ("lat", surface.lat.len()),
        ("lon", surface.lon.len()),
        ("psfc_pa", surface.psfc_pa.len()),
        ("t2_k", surface.t2_k.len()),
        ("q2_kgkg", surface.q2_kgkg.len()),
        ("u10_ms", surface.u10_ms.len()),
        ("v10_ms", surface.v10_ms.len()),
    ] {
        if len != expected {
            return Err(format!(
                "surface field {name} length {len} did not match grid size {expected}"
            )
            .into());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct NearestGridCell {
    index: usize,
    distance_km: f64,
}

struct SurfaceValidationGridIndex<'a> {
    lat: &'a [f64],
    lon: &'a [f64],
    bins: HashMap<(i32, i32), Vec<usize>>,
}

impl<'a> SurfaceValidationGridIndex<'a> {
    fn new(lat: &'a [f64], lon: &'a [f64]) -> Self {
        let mut bins = HashMap::<(i32, i32), Vec<usize>>::new();
        for (index, (&lat, &lon)) in lat.iter().zip(lon.iter()).enumerate() {
            if lat.is_finite() && lon.is_finite() {
                bins.entry(validation_bin_key(lat, lon))
                    .or_default()
                    .push(index);
            }
        }
        Self { lat, lon, bins }
    }

    fn nearest(&self, lat: f64, lon: f64, max_distance_km: f64) -> Option<NearestGridCell> {
        if !(lat.is_finite() && lon.is_finite()) {
            return None;
        }
        let (lat_bin, lon_bin) = validation_bin_key(lat, lon);
        let mut nearest = None::<NearestGridCell>;
        for dy in -VALIDATION_GRID_BIN_RADIUS..=VALIDATION_GRID_BIN_RADIUS {
            for dx in -VALIDATION_GRID_BIN_RADIUS..=VALIDATION_GRID_BIN_RADIUS {
                let Some(candidates) = self.bins.get(&(lat_bin + dy, lon_bin + dx)) else {
                    continue;
                };
                for &index in candidates {
                    let distance_km =
                        haversine_distance_km(lat, lon, self.lat[index], self.lon[index]);
                    if distance_km <= max_distance_km
                        && nearest
                            .map(|current| distance_km < current.distance_km)
                            .unwrap_or(true)
                    {
                        nearest = Some(NearestGridCell { index, distance_km });
                    }
                }
            }
        }
        nearest
    }
}

#[derive(Default)]
struct ValidationAccumulator {
    count: usize,
    improved_count: usize,
    worsened_count: usize,
    unchanged_count: usize,
    sum_background_error: f64,
    sum_analysis_error: f64,
    sum_abs_background_error: f64,
    sum_abs_analysis_error: f64,
    sum_abs_error_improvement: f64,
    sum_square_background_error: f64,
    sum_square_analysis_error: f64,
    max_abs_background_error: Option<f64>,
    max_abs_analysis_error: Option<f64>,
    confidence: ConfidenceValidationAccumulator,
}

impl ValidationAccumulator {
    fn push(&mut self, sample: Option<&StationVariableValidation>) {
        let Some(sample) = sample else {
            return;
        };
        self.count += 1;
        let background_abs = sample.background_error.abs();
        let analysis_abs = sample.analysis_error.abs();
        self.sum_background_error += sample.background_error;
        self.sum_analysis_error += sample.analysis_error;
        self.sum_abs_background_error += background_abs;
        self.sum_abs_analysis_error += analysis_abs;
        self.sum_abs_error_improvement += sample.abs_error_improvement;
        self.sum_square_background_error += sample.background_error * sample.background_error;
        self.sum_square_analysis_error += sample.analysis_error * sample.analysis_error;
        self.confidence
            .push(sample.confidence, sample.analysis_error.abs());
        self.max_abs_background_error = Some(
            self.max_abs_background_error
                .map(|current| current.max(background_abs))
                .unwrap_or(background_abs),
        );
        self.max_abs_analysis_error = Some(
            self.max_abs_analysis_error
                .map(|current| current.max(analysis_abs))
                .unwrap_or(analysis_abs),
        );
        if sample.abs_error_improvement > 1.0e-9 {
            self.improved_count += 1;
        } else if sample.abs_error_improvement < -1.0e-9 {
            self.worsened_count += 1;
        } else {
            self.unchanged_count += 1;
        }
    }

    fn finish(self) -> VariableValidationSummary {
        VariableValidationSummary {
            observation_count: self.count,
            improved_count: self.improved_count,
            worsened_count: self.worsened_count,
            unchanged_count: self.unchanged_count,
            mean_background_error: mean_if_nonzero(self.sum_background_error, self.count),
            mean_analysis_error: mean_if_nonzero(self.sum_analysis_error, self.count),
            mean_abs_background_error: mean_if_nonzero(self.sum_abs_background_error, self.count),
            mean_abs_analysis_error: mean_if_nonzero(self.sum_abs_analysis_error, self.count),
            mean_abs_error_improvement: mean_if_nonzero(self.sum_abs_error_improvement, self.count),
            background_rmse: rmse_if_nonzero(self.sum_square_background_error, self.count),
            analysis_rmse: rmse_if_nonzero(self.sum_square_analysis_error, self.count),
            max_abs_background_error: self.max_abs_background_error,
            max_abs_analysis_error: self.max_abs_analysis_error,
            confidence: self.confidence.finish(),
        }
    }
}

#[derive(Default)]
struct ConfidenceValidationAccumulator {
    count: usize,
    sum_confidence: f64,
    sum_abs_analysis_error: f64,
    sum_confidence_squared: f64,
    sum_abs_analysis_error_squared: f64,
    sum_confidence_abs_analysis_error: f64,
    low: ConfidenceValidationBucket,
    medium: ConfidenceValidationBucket,
    high: ConfidenceValidationBucket,
    samples: Vec<(f64, f64)>,
}

impl ConfidenceValidationAccumulator {
    fn push(&mut self, confidence: Option<f64>, abs_analysis_error: f64) {
        let Some(confidence) = finite_confidence(confidence) else {
            return;
        };
        if !abs_analysis_error.is_finite() {
            return;
        }

        self.count += 1;
        self.sum_confidence += confidence;
        self.sum_abs_analysis_error += abs_analysis_error;
        self.sum_confidence_squared += confidence * confidence;
        self.sum_abs_analysis_error_squared += abs_analysis_error * abs_analysis_error;
        self.sum_confidence_abs_analysis_error += confidence * abs_analysis_error;
        self.samples.push((confidence, abs_analysis_error));

        if confidence < 1.0 / 3.0 {
            self.low.push(abs_analysis_error);
        } else if confidence < 2.0 / 3.0 {
            self.medium.push(abs_analysis_error);
        } else {
            self.high.push(abs_analysis_error);
        }
    }

    fn finish(self) -> Option<VariableConfidenceValidationSummary> {
        if self.count == 0 {
            return None;
        }

        let low_mean_abs_analysis_error = self.low.mean_abs_analysis_error();
        let high_mean_abs_analysis_error = self.high.mean_abs_analysis_error();
        let confidence_abs_error_correlation = self.confidence_abs_error_correlation();
        let ranked = ranked_confidence_validation_summary(self.samples);
        let reliability = confidence_reliability_contract_from_ranked_buckets(
            ranked.low_count,
            ranked.high_count,
            ranked.high_minus_low_mean_abs_analysis_error,
        );
        Some(VariableConfidenceValidationSummary {
            observation_count: self.count,
            mean_confidence: mean_if_nonzero(self.sum_confidence, self.count),
            low_confidence_observation_count: self.low.count,
            low_confidence_mean_abs_analysis_error: low_mean_abs_analysis_error,
            medium_confidence_observation_count: self.medium.count,
            medium_confidence_mean_abs_analysis_error: self.medium.mean_abs_analysis_error(),
            high_confidence_observation_count: self.high.count,
            high_confidence_mean_abs_analysis_error: high_mean_abs_analysis_error,
            high_minus_low_mean_abs_analysis_error: match (
                high_mean_abs_analysis_error,
                low_mean_abs_analysis_error,
            ) {
                (Some(high), Some(low)) => Some(high - low),
                _ => None,
            },
            confidence_abs_error_correlation,
            ranked_low_confidence_observation_count: ranked.low_count,
            ranked_low_confidence_mean_abs_analysis_error: ranked.low_mean_abs_analysis_error,
            ranked_high_confidence_observation_count: ranked.high_count,
            ranked_high_confidence_mean_abs_analysis_error: ranked.high_mean_abs_analysis_error,
            ranked_high_minus_low_mean_abs_analysis_error: ranked
                .high_minus_low_mean_abs_analysis_error,
            reliability,
        })
    }

    fn confidence_abs_error_correlation(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let count = self.count as f64;
        let confidence_mean = self.sum_confidence / count;
        let abs_error_mean = self.sum_abs_analysis_error / count;
        let confidence_variance =
            self.sum_confidence_squared / count - confidence_mean * confidence_mean;
        let abs_error_variance =
            self.sum_abs_analysis_error_squared / count - abs_error_mean * abs_error_mean;
        if confidence_variance <= 1.0e-12 || abs_error_variance <= 1.0e-12 {
            return None;
        }

        let covariance =
            self.sum_confidence_abs_analysis_error / count - confidence_mean * abs_error_mean;
        Some(covariance / (confidence_variance.sqrt() * abs_error_variance.sqrt()))
    }
}

#[derive(Default)]
struct RankedConfidenceValidationSummary {
    low_count: usize,
    low_mean_abs_analysis_error: Option<f64>,
    high_count: usize,
    high_mean_abs_analysis_error: Option<f64>,
    high_minus_low_mean_abs_analysis_error: Option<f64>,
}

fn ranked_confidence_validation_summary(
    mut samples: Vec<(f64, f64)>,
) -> RankedConfidenceValidationSummary {
    if samples.len() < 3 {
        return RankedConfidenceValidationSummary::default();
    }
    samples.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    let bucket_count = (samples.len() / 3).max(1);
    let low_sum = samples
        .iter()
        .take(bucket_count)
        .map(|(_, abs_error)| abs_error)
        .sum::<f64>();
    let high_sum = samples
        .iter()
        .rev()
        .take(bucket_count)
        .map(|(_, abs_error)| abs_error)
        .sum::<f64>();
    let low_mean = mean_if_nonzero(low_sum, bucket_count);
    let high_mean = mean_if_nonzero(high_sum, bucket_count);
    RankedConfidenceValidationSummary {
        low_count: bucket_count,
        low_mean_abs_analysis_error: low_mean,
        high_count: bucket_count,
        high_mean_abs_analysis_error: high_mean,
        high_minus_low_mean_abs_analysis_error: match (high_mean, low_mean) {
            (Some(high), Some(low)) => Some(high - low),
            _ => None,
        },
    }
}

fn confidence_reliability_contract_from_ranked_buckets(
    ranked_low_count: usize,
    ranked_high_count: usize,
    ranked_high_minus_low_mae: Option<f64>,
) -> ConfidenceReliabilityContract {
    let bucket_coverage_sufficient = ranked_low_count
        >= CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS
        && ranked_high_count >= CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS
        && ranked_high_minus_low_mae.is_some();
    let passed = bucket_coverage_sufficient
        && ranked_high_minus_low_mae
            .map(|value| value <= CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE)
            .unwrap_or(false);
    let (status, semantic_label, message) = if passed {
        (
            "passed",
            "calibrated_reliability",
            "ranked high-confidence samples had lower or equal MAE than ranked low-confidence samples",
        )
    } else if bucket_coverage_sufficient {
        (
            "failed",
            "uncalibrated_support",
            "ranked high-confidence samples had higher MAE than ranked low-confidence samples",
        )
    } else {
        (
            "untestable",
            "support_index",
            "ranked confidence buckets did not have enough coverage to test reliability",
        )
    };
    ConfidenceReliabilityContract {
        schema: "rustwx.surface_mesoanalysis.confidence_reliability.v1".to_string(),
        semantic_label: semantic_label.to_string(),
        status: status.to_string(),
        bucket_coverage_sufficient,
        ranked_low_confidence_observation_count: ranked_low_count,
        ranked_high_confidence_observation_count: ranked_high_count,
        min_ranked_bucket_observation_count: CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
        ranked_high_minus_low_mean_abs_analysis_error: ranked_high_minus_low_mae,
        max_ranked_high_minus_low_mean_abs_analysis_error:
            CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
        message: message.to_string(),
    }
}

#[derive(Default)]
struct ConfidenceValidationBucket {
    count: usize,
    sum_abs_analysis_error: f64,
}

impl ConfidenceValidationBucket {
    fn push(&mut self, abs_analysis_error: f64) {
        self.count += 1;
        self.sum_abs_analysis_error += abs_analysis_error;
    }

    fn mean_abs_analysis_error(&self) -> Option<f64> {
        mean_if_nonzero(self.sum_abs_analysis_error, self.count)
    }
}

fn finite_confidence(confidence: Option<f64>) -> Option<f64> {
    let confidence = confidence?;
    if confidence.is_finite() {
        Some(confidence.clamp(0.0, 1.0))
    } else {
        None
    }
}

#[derive(Default)]
struct SourceValidationAccumulator {
    sampled_observation_count: usize,
    temperature: ValidationAccumulator,
    dewpoint: ValidationAccumulator,
    wind_speed: ValidationAccumulator,
}

impl SourceValidationAccumulator {
    fn push(
        &mut self,
        temperature_c: Option<&StationVariableValidation>,
        dewpoint_c: Option<&StationVariableValidation>,
        wind_speed_ms: Option<&StationVariableValidation>,
    ) {
        self.sampled_observation_count += 1;
        self.temperature.push(temperature_c);
        self.dewpoint.push(dewpoint_c);
        self.wind_speed.push(wind_speed_ms);
    }

    fn finish(self, source: String) -> SourceMesoanalysisValidationSummary {
        SourceMesoanalysisValidationSummary {
            source,
            sampled_observation_count: self.sampled_observation_count,
            temperature_c: self.temperature.finish(),
            dewpoint_c: self.dewpoint.finish(),
            wind_speed_ms: self.wind_speed.finish(),
            mean_sea_level_pressure_hpa: None,
        }
    }

    fn finish_stratum(
        self,
        stratum_type: String,
        stratum_value: String,
    ) -> StratifiedMesoanalysisValidationSummary {
        StratifiedMesoanalysisValidationSummary {
            stratum_type,
            stratum_value,
            sampled_observation_count: self.sampled_observation_count,
            temperature_c: self.temperature.finish(),
            dewpoint_c: self.dewpoint.finish(),
            wind_speed_ms: self.wind_speed.finish(),
            mean_sea_level_pressure_hpa: None,
        }
    }
}

fn push_validation_strata(
    strata: &mut BTreeMap<(String, String), SourceValidationAccumulator>,
    observation: &MesoObservation,
    terrain_pressure_class: &str,
    observation_age_bucket: Option<&str>,
    temperature_c: Option<&StationVariableValidation>,
    dewpoint_c: Option<&StationVariableValidation>,
    wind_speed_ms: Option<&StationVariableValidation>,
) {
    for (stratum_type, stratum_value) in [
        (
            "source_quality_class",
            observation
                .source_quality_class
                .as_deref()
                .unwrap_or("unknown_source_quality"),
        ),
        (
            "representativeness_class",
            observation
                .representativeness_class
                .as_deref()
                .unwrap_or("unknown_representativeness"),
        ),
        (
            "correction_role",
            observation
                .correction_role
                .as_deref()
                .unwrap_or("unknown_correction_role"),
        ),
        ("terrain_pressure_class", terrain_pressure_class),
        (
            "observation_age_bucket",
            observation_age_bucket.unwrap_or("unknown_age"),
        ),
    ] {
        strata
            .entry((stratum_type.to_string(), stratum_value.to_string()))
            .or_default()
            .push(temperature_c, dewpoint_c, wind_speed_ms);
    }
}

fn validation_terrain_pressure_class(pressure_hpa: Option<f64>) -> &'static str {
    let Some(pressure_hpa) = pressure_hpa else {
        return "unknown_terrain_pressure";
    };
    if pressure_hpa >= 950.0 {
        "lowland_high_pressure"
    } else if pressure_hpa >= 850.0 {
        "elevated_terrain"
    } else if pressure_hpa >= 700.0 {
        "mountain_terrain"
    } else {
        "high_mountain_terrain"
    }
}

fn validation_observation_age_bucket(age_minutes: f64) -> String {
    if !age_minutes.is_finite() {
        return "unknown_age".to_string();
    }
    let age_minutes = age_minutes.max(0.0);
    if age_minutes <= 15.0 {
        "age_000_015_min".to_string()
    } else if age_minutes <= 30.0 {
        "age_015_030_min".to_string()
    } else if age_minutes <= 60.0 {
        "age_030_060_min".to_string()
    } else if age_minutes <= 90.0 {
        "age_060_090_min".to_string()
    } else {
        "age_over_090_min".to_string()
    }
}

fn finite_option(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn station_variable_validation(
    observed: Option<f64>,
    background: Option<f64>,
    analysis: Option<f64>,
    confidence: Option<f64>,
) -> Option<StationVariableValidation> {
    let (Some(observed), Some(background), Some(analysis)) = (observed, background, analysis)
    else {
        return None;
    };
    if !(observed.is_finite() && background.is_finite() && analysis.is_finite()) {
        return None;
    }
    let background_error = background - observed;
    let analysis_error = analysis - observed;
    Some(StationVariableValidation {
        observed,
        background,
        analysis,
        background_error,
        analysis_error,
        abs_error_improvement: background_error.abs() - analysis_error.abs(),
        confidence: finite_confidence(confidence),
    })
}

#[derive(Default)]
struct ExternalReferenceVariableAccumulator {
    count: usize,
    sum_abs_background_error: f64,
    sum_abs_candidate_error: f64,
    sum_abs_reference_error: f64,
    sum_square_background_error: f64,
    sum_square_candidate_error: f64,
    sum_square_reference_error: f64,
}

impl ExternalReferenceVariableAccumulator {
    fn push(
        &mut self,
        candidate: Option<&StationVariableValidation>,
        reference: Option<&StationVariableValidation>,
    ) {
        let (Some(candidate), Some(reference)) = (candidate, reference) else {
            return;
        };
        if !(candidate.observed.is_finite()
            && candidate.background_error.is_finite()
            && candidate.analysis_error.is_finite()
            && reference.analysis_error.is_finite())
        {
            return;
        }
        self.count += 1;
        self.sum_abs_background_error += candidate.background_error.abs();
        self.sum_abs_candidate_error += candidate.analysis_error.abs();
        self.sum_abs_reference_error += reference.analysis_error.abs();
        self.sum_square_background_error += candidate.background_error * candidate.background_error;
        self.sum_square_candidate_error += candidate.analysis_error * candidate.analysis_error;
        self.sum_square_reference_error += reference.analysis_error * reference.analysis_error;
    }

    fn finish(self) -> Option<SurfaceMesoanalysisExternalReferenceVariableComparison> {
        if self.count == 0 {
            return None;
        }
        let background_mean_abs_error = mean_if_nonzero(self.sum_abs_background_error, self.count);
        let candidate_mean_abs_error = mean_if_nonzero(self.sum_abs_candidate_error, self.count);
        let reference_mean_abs_error = mean_if_nonzero(self.sum_abs_reference_error, self.count);
        let background_rmse = rmse_if_nonzero(self.sum_square_background_error, self.count);
        let candidate_rmse = rmse_if_nonzero(self.sum_square_candidate_error, self.count);
        let reference_rmse = rmse_if_nonzero(self.sum_square_reference_error, self.count);

        Some(SurfaceMesoanalysisExternalReferenceVariableComparison {
            observation_count: self.count,
            candidate_observation_count: self.count,
            reference_observation_count: self.count,
            background_mean_abs_error,
            candidate_mean_abs_error,
            reference_mean_abs_error,
            candidate_minus_background_mae: option_delta(
                candidate_mean_abs_error,
                background_mean_abs_error,
            ),
            candidate_minus_reference_mae: option_delta(
                candidate_mean_abs_error,
                reference_mean_abs_error,
            ),
            background_rmse,
            candidate_rmse,
            reference_rmse,
            candidate_minus_background_rmse: option_delta(candidate_rmse, background_rmse),
            candidate_minus_reference_rmse: option_delta(candidate_rmse, reference_rmse),
            mae_winner: lowest_external_error_winner([
                ("background", background_mean_abs_error),
                ("candidate", candidate_mean_abs_error),
                ("reference", reference_mean_abs_error),
            ]),
            rmse_winner: lowest_external_error_winner([
                ("background", background_rmse),
                ("candidate", candidate_rmse),
                ("reference", reference_rmse),
            ]),
        })
    }
}

fn empty_external_reference_variable() -> SurfaceMesoanalysisExternalReferenceVariableComparison {
    SurfaceMesoanalysisExternalReferenceVariableComparison {
        observation_count: 0,
        candidate_observation_count: 0,
        reference_observation_count: 0,
        background_mean_abs_error: None,
        candidate_mean_abs_error: None,
        reference_mean_abs_error: None,
        candidate_minus_background_mae: None,
        candidate_minus_reference_mae: None,
        background_rmse: None,
        candidate_rmse: None,
        reference_rmse: None,
        candidate_minus_background_rmse: None,
        candidate_minus_reference_rmse: None,
        mae_winner: "unavailable".to_string(),
        rmse_winner: "unavailable".to_string(),
    }
}

fn lowest_external_error_winner(values: [(&'static str, Option<f64>); 3]) -> String {
    let mut values = values
        .into_iter()
        .filter_map(|(label, value)| value.map(|value| (label, value)));
    let Some(mut best) = values.next() else {
        return "unavailable".to_string();
    };
    let mut tied = false;
    for value in values {
        if value.1 < best.1 - 1.0e-12 {
            best = value;
            tied = false;
        } else if (value.1 - best.1).abs() <= 1.0e-12 {
            tied = true;
        }
    }
    if tied {
        "tie".to_string()
    } else {
        best.0.to_string()
    }
}

fn mean_if_nonzero(sum: f64, count: usize) -> Option<f64> {
    if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    }
}

fn rmse_if_nonzero(sum_square_error: f64, count: usize) -> Option<f64> {
    if count > 0 {
        Some((sum_square_error / count as f64).sqrt())
    } else {
        None
    }
}

fn validation_source_key(source: &str) -> String {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

fn validation_sample_key(sample: &StationMesoanalysisValidationSample) -> String {
    format!(
        "{}|{}|{}|{:.6}|{:.6}",
        sample.source,
        sample.station_id,
        sample.timestamp.as_deref().unwrap_or(""),
        sample.latitude_deg,
        normalize_lon(sample.longitude_deg)
    )
}

fn bounds_from_surface_latlon(lat: &[f64], lon: &[f64]) -> Option<[f64; 4]> {
    if lat.len() != lon.len() || lat.is_empty() {
        return None;
    }
    let mut west = f64::INFINITY;
    let mut south = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut north = f64::NEG_INFINITY;
    for (&lat, &lon) in lat.iter().zip(lon.iter()) {
        let lon = normalize_lon(lon);
        if !(lat.is_finite() && lon.is_finite()) {
            continue;
        }
        west = west.min(lon);
        east = east.max(lon);
        south = south.min(lat);
        north = north.max(lat);
    }
    if west.is_finite() && south.is_finite() && east.is_finite() && north.is_finite() {
        Some([west, south, east, north])
    } else {
        None
    }
}

fn validation_bin_key(lat: f64, lon: f64) -> (i32, i32) {
    (
        (lat / VALIDATION_GRID_BIN_DEG).floor() as i32,
        (normalize_lon(lon) / VALIDATION_GRID_BIN_DEG).floor() as i32,
    )
}

fn normalize_lon(lon: f64) -> f64 {
    (lon + 180.0).rem_euclid(360.0) - 180.0
}

fn haversine_distance_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (normalize_lon(lon2) - normalize_lon(lon1)).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat * 0.5).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon * 0.5).sin().powi(2);
    2.0 * EARTH_RADIUS_KM * a.sqrt().atan2((1.0 - a).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_surface() -> SurfaceFields {
        SurfaceFields {
            lat: vec![35.0, 35.0, 35.0],
            lon: vec![-98.2, -98.0, -97.8],
            nx: 3,
            ny: 1,
            projection: None,
            psfc_pa: vec![100000.0; 3],
            orog_m: vec![300.0; 3],
            orog_is_proxy: false,
            t2_k: vec![293.15; 3],
            q2_kgkg: vec![0.010; 3],
            u10_ms: vec![0.0; 3],
            v10_ms: vec![0.0; 3],
            native_sbcape_jkg: None,
            native_mlcape_jkg: None,
            native_mucape_jkg: None,
            native_pblh_m: None,
        }
    }

    #[test]
    fn runner_observation_loader_maps_current_weather_feed() {
        let path = std::env::temp_dir().join(format!(
            "rustwx_runner_obs_{}_{}.json",
            std::process::id(),
            "current"
        ));
        fs::write(
            &path,
            r#"{
                "source": "calfire_raws_current",
                "source_name": "CAL FIRE RAWS 10-Minute Weather",
                "kind": "raws_current_weather",
                "observation_count": 1,
                "observations": [{
                    "station_id": "CF019",
                    "source": "calfire_raws_current",
                    "timestamp": "2026-05-12T08:20:00Z",
                    "latitude": 35.0,
                    "longitude": -98.0,
                    "temperature_f": 68.0,
                    "dewpoint_f": 59.0,
                    "wind_direction_deg": 270.0,
                    "wind_speed_kts": 10.0
                }]
            }"#,
        )
        .unwrap();

        let loaded = load_runner_meso_observations(&[path.clone()]).unwrap();
        let _ = fs::remove_file(path);

        assert_eq!(loaded.sources.len(), 1);
        assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 1);
        assert_eq!(loaded.sources[0].source_quality_class, "fire_weather_raws");
        assert_eq!(
            loaded.sources[0].representativeness_class,
            "fire_weather_exposure"
        );
        assert_eq!(
            loaded.sources[0].correction_role,
            "supplemental_correction_with_representativeness_inflation"
        );
        assert_eq!(loaded.sources[0].quality_weight, 0.65);
        assert_eq!(loaded.sources[0].default_temperature_error_c, 1.5);
        assert_eq!(loaded.sources[0].default_wind_error_ms, 2.5);
        assert_eq!(loaded.sources[0].time_filtered_count, 0);
        assert_eq!(loaded.sources[0].missing_or_invalid_time_count, 0);
        assert_eq!(loaded.observations.len(), 1);
        assert!((loaded.observations[0].temperature_c.unwrap() - 20.0).abs() < 1.0e-6);
        assert!((loaded.observations[0].wind_speed_ms.unwrap() - 5.14444).abs() < 1.0e-5);
        assert_eq!(loaded.observations[0].temperature_error_c, Some(1.5));
        assert_eq!(loaded.observations[0].dewpoint_error_c, Some(2.0));
        assert_eq!(loaded.observations[0].wind_error_ms, Some(2.5));
    }

    #[test]
    fn runner_observation_loader_skips_daily_fire_danger_feed() {
        let path = std::env::temp_dir().join(format!(
            "rustwx_runner_obs_{}_{}.json",
            std::process::id(),
            "daily"
        ));
        fs::write(
            &path,
            r#"{
                "source": "nifc_raws_fire_danger",
                "kind": "raws_fire_danger_daily",
                "observation_count": 1,
                "observations": [{
                    "station_id": "102004",
                    "latitude": 44.0,
                    "longitude": -116.0,
                    "temperature_f": 68.0
                }]
            }"#,
        )
        .unwrap();

        let loaded = load_runner_meso_observations(&[path.clone()]).unwrap();
        let _ = fs::remove_file(path);

        assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 0);
        assert!(loaded.sources[0].skipped_for_kind);
        assert!(loaded.observations.is_empty());
    }

    #[test]
    fn runner_observation_loader_filters_by_reference_time() {
        let path = std::env::temp_dir().join(format!(
            "rustwx_runner_obs_{}_{}.json",
            std::process::id(),
            "time_filter"
        ));
        fs::write(
            &path,
            r#"{
                "source": "oklahoma_mesonet",
                "kind": "mesonet_current_5min",
                "observation_count": 3,
                "observations": [
                    {
                        "station_id": "GOOD",
                        "timestamp": "2026-05-12T08:20:00Z",
                        "latitude": 35.0,
                        "longitude": -98.0,
                        "temperature_f": 68.0
                    },
                    {
                        "station_id": "OLD",
                        "timestamp": "2026-05-12T06:00:00Z",
                        "latitude": 35.1,
                        "longitude": -98.1,
                        "temperature_f": 69.0
                    },
                    {
                        "station_id": "MISSING_TIME",
                        "latitude": 35.2,
                        "longitude": -98.2,
                        "temperature_f": 70.0
                    }
                ]
            }"#,
        )
        .unwrap();

        let loaded = load_runner_meso_observations_with_options(
            &[path.clone()],
            &RunnerMesoObservationLoadOptions {
                reference_time_utc: Some(
                    DateTime::parse_from_rfc3339("2026-05-12T08:30:00Z")
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                max_age_minutes: Some(30),
                allow_future_minutes: 5,
                time_weight_half_life_minutes: Some(60.0),
                max_time_error_inflation_factor: 2.0,
                profile: RunnerMesoObservationProfile::AllCurrentSurface,
            },
        )
        .unwrap();
        let _ = fs::remove_file(path);

        assert_eq!(loaded.observations.len(), 1);
        assert_eq!(loaded.observations[0].station_id, "GOOD");
        assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 1);
        assert_eq!(
            loaded.sources[0].accepted_min_observation_age_minutes,
            Some(10.0)
        );
        assert_eq!(
            loaded.sources[0].accepted_mean_observation_age_minutes,
            Some(10.0)
        );
        assert_eq!(
            loaded.sources[0].accepted_max_observation_age_minutes,
            Some(10.0)
        );
        let expected_time_weight = (-(std::f64::consts::LN_2 * 10.0) / 60.0).exp();
        assert!(
            (loaded.sources[0].mean_time_weight.unwrap() - expected_time_weight).abs() < 1.0e-12
        );
        assert!(loaded.observations[0].quality_weight < 1.15);
        assert!(loaded.observations[0].temperature_error_c.unwrap() > 1.0);
        assert_eq!(loaded.sources[0].time_filtered_count, 1);
        assert_eq!(loaded.sources[0].missing_or_invalid_time_count, 1);
    }

    #[test]
    fn runner_observation_loader_deduplicates_station_across_sources() {
        let low_path = std::env::temp_dir().join(format!(
            "rustwx_runner_obs_{}_{}.json",
            std::process::id(),
            "duplicate_low"
        ));
        let high_path = std::env::temp_dir().join(format!(
            "rustwx_runner_obs_{}_{}.json",
            std::process::id(),
            "duplicate_high"
        ));
        fs::write(
            &low_path,
            r#"{
                "source": "rwis_duplicate",
                "kind": "rwis_current",
                "observation_count": 1,
                "observations": [{
                    "station_id": "DUP",
                    "source": "rwis_duplicate",
                    "timestamp": "2026-05-12T08:10:00Z",
                    "latitude": 35.0,
                    "longitude": -98.0,
                    "temperature_f": 70.0
                }]
            }"#,
        )
        .unwrap();
        fs::write(
            &high_path,
            r#"{
                "source": "metar_duplicate",
                "kind": "asos_awos_metar",
                "observation_count": 1,
                "observations": [{
                    "station_id": "DUP",
                    "source": "metar_duplicate",
                    "timestamp": "2026-05-12T08:20:00Z",
                    "latitude": 35.0,
                    "longitude": -98.0,
                    "temperature_f": 68.0
                }]
            }"#,
        )
        .unwrap();

        let loaded = load_runner_meso_observations(&[low_path.clone(), high_path.clone()]).unwrap();
        let _ = fs::remove_file(low_path);
        let _ = fs::remove_file(high_path);

        assert_eq!(loaded.observations.len(), 1);
        assert_eq!(loaded.observations[0].station_id, "DUP");
        assert_eq!(loaded.observations[0].source, "metar_duplicate");
        assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 0);
        assert_eq!(loaded.sources[0].duplicate_filtered_count, 1);
        assert_eq!(loaded.sources[1].accepted_for_mesoanalysis, 1);
        assert_eq!(loaded.sources[1].duplicate_filtered_count, 0);
    }

    #[test]
    fn surface_meso_conus_profile_filters_global_and_representativeness_noise() {
        let path = std::env::temp_dir().join(format!(
            "rustwx_runner_obs_{}_{}.json",
            std::process::id(),
            "surface_profile"
        ));
        fs::write(
            &path,
            r#"{
                "source": "aviation_weather_metar_conus",
                "kind": "asos_awos_metar",
                "observation_count": 3,
                "observations": [
                    {
                        "station_id": "KOUN",
                        "timestamp": "2026-05-12T08:20:00Z",
                        "latitude": 35.24,
                        "longitude": -97.47,
                        "temperature_f": 68.0,
                        "sea_level_pressure_mb": 1014.2
                    },
                    {
                        "station_id": "EEPU",
                        "timestamp": "2026-05-12T08:20:00Z",
                        "latitude": 58.42,
                        "longitude": 24.47,
                        "temperature_f": 57.0
                    },
                    {
                        "station_id": "PHNL",
                        "timestamp": "2026-05-12T08:20:00Z",
                        "latitude": 21.32,
                        "longitude": -157.93,
                        "temperature_f": 79.0
                    }
                ]
            }"#,
        )
        .unwrap();

        let loaded = load_runner_meso_observations_with_options(
            &[path.clone()],
            &RunnerMesoObservationLoadOptions {
                profile: RunnerMesoObservationProfile::SurfaceMesoConus,
                ..RunnerMesoObservationLoadOptions::default()
            },
        )
        .unwrap();
        let _ = fs::remove_file(path);

        assert_eq!(loaded.observations.len(), 1);
        assert_eq!(loaded.observations[0].station_id, "KOUN");
        assert_eq!(
            loaded.observations[0].mean_sea_level_pressure_hpa,
            Some(1014.2)
        );
        assert_eq!(loaded.observations[0].temperature_error_c, Some(0.8));
        assert_eq!(
            loaded.observations[0].mean_sea_level_pressure_error_hpa,
            Some(0.7)
        );
        assert_eq!(
            loaded.observations[0].source_quality_class.as_deref(),
            Some("aviation_reference")
        );
        assert_eq!(
            loaded.observations[0].representativeness_class.as_deref(),
            Some("synoptic_airport_surface")
        );
        assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 1);
        assert_eq!(loaded.sources[0].profile_filtered_count, 2);
        assert_eq!(loaded.sources[0].accepted_mean_sea_level_pressure_count, 1);
        assert_eq!(loaded.sources[0].source_quality_class, "aviation_reference");
        assert_eq!(
            loaded.sources[0].representativeness_class,
            "synoptic_airport_surface"
        );
        assert_eq!(
            loaded.sources[0].correction_role,
            "primary_correction_and_verification"
        );
        assert_eq!(loaded.sources[0].quality_weight, 1.0);
        assert_eq!(loaded.sources[0].default_temperature_error_c, 0.8);
        assert_eq!(
            loaded.sources[0].default_mean_sea_level_pressure_error_hpa,
            0.7
        );
    }

    #[test]
    fn runner_kind_allowlist_covers_current_runner_sources() {
        for kind in [
            "asos_awos_metar",
            "mesonet_5min",
            "mesonet_current_5min",
            "mesonet_current_15min",
            "mesonet_hourly",
            "mesonet_hourly_ag_weather",
            "marine_current_observation",
            "rwis_current",
            "raws_current_weather",
            "coastal_meteorology_current",
            "snotel_hourly",
            "scan_hourly",
        ] {
            assert!(
                runner_kind_is_current_surface_candidate(kind),
                "{kind} should be usable for surface mesoanalysis"
            );
        }
        assert!(!runner_kind_is_current_surface_candidate(
            "raws_fire_danger_daily"
        ));
        assert!(!runner_kind_is_current_surface_candidate(
            "hydro_current_observation"
        ));
        assert!(!runner_kind_is_current_surface_candidate(
            "hydro_forecast_status"
        ));
        assert!(!runner_kind_is_current_surface_candidate(
            "flash_flood_current_observation"
        ));
        assert!(!runner_kind_is_current_surface_candidate(
            "coastal_water_current"
        ));
        assert!(!runner_kind_is_current_surface_candidate(
            "air_quality_current_observation"
        ));
        assert!(!runner_kind_is_current_surface_candidate(
            "coop_daily_climate"
        ));
    }

    #[test]
    fn surface_fields_mesoanalysis_consumes_runner_observations() {
        let surface = sample_surface();
        let mut observation = MesoObservation::new("OKC", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(25.0)
            .with_dewpoint_c(18.0)
            .with_wind(270.0, 10.0);
        observation.source_quality_class = Some("aviation_reference".to_string());
        observation.representativeness_class = Some("synoptic_airport_surface".to_string());
        observation.correction_role = Some("primary_correction_and_verification".to_string());
        observation.observation_age_minutes = Some(12.0);
        observation.time_weight = Some(0.87);
        let observations = vec![observation];

        let fields = compute_surface_mesoanalysis_from_fields(
            &surface,
            &observations,
            MesoanalysisConfig {
                barnes_radius_km: 30.0,
                barnes_kappa_km2: 100.0,
                ..MesoanalysisConfig::default()
            },
        )
        .unwrap();
        let report = summarize_surface_mesoanalysis(&fields, &observations);

        assert_eq!(report.schema, "rustwx.surface_mesoanalysis.report.v1");
        assert_eq!(report.observation_count, 1);
        assert_eq!(report.fields.temperature_2m_c.finite_count, 3);
        assert!(report.fields.temperature_increment_c.finite_count > 0);
        assert!(report.fields.temperature_confidence.finite_count > 0);
        assert!(fields.u10_ms[1] > 9.9);
    }

    #[test]
    fn station_validation_reports_background_to_analysis_improvement() {
        let surface = sample_surface();
        let mut observation = MesoObservation::new("OKC", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(25.0)
            .with_dewpoint_c(18.0)
            .with_wind(270.0, 10.0);
        observation.source_quality_class = Some("aviation_reference".to_string());
        observation.representativeness_class = Some("synoptic_airport_surface".to_string());
        observation.correction_role = Some("primary_correction_and_verification".to_string());
        observation.observation_age_minutes = Some(12.0);
        observation.time_weight = Some(0.87);
        let observations = vec![observation];

        let fields = compute_surface_mesoanalysis_from_fields(
            &surface,
            &observations,
            MesoanalysisConfig {
                barnes_radius_km: 30.0,
                barnes_kappa_km2: 100.0,
                ..MesoanalysisConfig::default()
            },
        )
        .unwrap();
        let report =
            summarize_surface_mesoanalysis_with_validation(&surface, &fields, &observations)
                .unwrap();
        let validation = report.validation.unwrap();
        let sample = validation.samples.first().unwrap();
        let temperature = sample.temperature_c.as_ref().unwrap();

        assert_eq!(validation.sampled_observation_count, 1);
        assert!(sample.nearest_grid_distance_km < 1.0e-6);
        assert_eq!(
            sample.source_quality_class.as_deref(),
            Some("aviation_reference")
        );
        assert_eq!(
            sample.observation_age_bucket.as_deref(),
            Some("age_000_015_min")
        );
        assert_eq!(sample.terrain_pressure_class, "lowland_high_pressure");
        assert!(temperature.abs_error_improvement > 0.0);
        assert!(temperature.confidence.is_some());
        assert!(validation.temperature_c.mean_abs_error_improvement.unwrap() > 0.0);
        assert!(validation.temperature_c.analysis_rmse.unwrap() < 0.1);
        assert_eq!(
            validation
                .temperature_c
                .confidence
                .as_ref()
                .unwrap()
                .observation_count,
            1
        );
        assert_eq!(
            validation.temperature_c.max_abs_background_error,
            validation.temperature_c.mean_abs_background_error
        );
        assert_eq!(validation.source_summaries.len(), 1);
        assert_eq!(validation.source_summaries[0].source, "unit");
        assert_eq!(
            validation.source_summaries[0]
                .temperature_c
                .observation_count,
            1
        );
        assert_eq!(
            validation.source_summaries[0]
                .temperature_c
                .confidence
                .as_ref()
                .unwrap()
                .observation_count,
            1
        );
        assert!(validation.strata_summaries.iter().any(|summary| {
            summary.stratum_type == "source_quality_class"
                && summary.stratum_value == "aviation_reference"
                && summary.temperature_c.observation_count == 1
        }));
        assert!(validation.strata_summaries.iter().any(|summary| {
            summary.stratum_type == "observation_age_bucket"
                && summary.stratum_value == "age_000_015_min"
                && summary.temperature_c.observation_count == 1
        }));
    }

    #[test]
    fn validation_confidence_summary_bins_station_errors() {
        let surface = sample_surface();
        let observations = vec![
            MesoObservation::new("LOW", 35.0, -98.2)
                .with_source("unit")
                .with_temperature_c(20.0),
            MesoObservation::new("MID", 35.0, -98.0)
                .with_source("unit")
                .with_temperature_c(20.0),
            MesoObservation::new("HIGH", 35.0, -97.8)
                .with_source("unit")
                .with_temperature_c(20.0),
        ];
        let fields = MesoanalysisFields {
            temperature_2m_c: vec![15.0, 18.0, 20.0],
            dewpoint_2m_c: vec![10.0; 3],
            q2_kgkg: surface.q2_kgkg.clone(),
            u10_ms: surface.u10_ms.clone(),
            v10_ms: surface.v10_ms.clone(),
            mean_sea_level_pressure_hpa: None,
            temperature_increment_c: vec![-5.0, -2.0, 0.0],
            dewpoint_increment_c: vec![0.0; 3],
            u10_increment_ms: vec![0.0; 3],
            v10_increment_ms: vec![0.0; 3],
            mean_sea_level_pressure_increment_hpa: None,
            neighbor_count: vec![1; 3],
            temperature_confidence: vec![0.1, 0.5, 0.9],
            dewpoint_confidence: vec![1.0; 3],
            u10_confidence: vec![1.0; 3],
            v10_confidence: vec![1.0; 3],
            mean_sea_level_pressure_confidence: None,
            diagnostics: Vec::new(),
        };

        let validation =
            validate_surface_mesoanalysis_at_observations(&surface, &fields, &observations)
                .unwrap();
        let confidence = validation.temperature_c.confidence.as_ref().unwrap();

        assert_eq!(confidence.observation_count, 3);
        assert_eq!(confidence.low_confidence_observation_count, 1);
        assert_eq!(confidence.medium_confidence_observation_count, 1);
        assert_eq!(confidence.high_confidence_observation_count, 1);
        assert_eq!(confidence.low_confidence_mean_abs_analysis_error, Some(5.0));
        assert_eq!(
            confidence.medium_confidence_mean_abs_analysis_error,
            Some(2.0)
        );
        assert_eq!(
            confidence.high_confidence_mean_abs_analysis_error,
            Some(0.0)
        );
        assert_eq!(
            confidence.high_minus_low_mean_abs_analysis_error,
            Some(-5.0)
        );
        assert!(confidence.confidence_abs_error_correlation.unwrap() < -0.9);
        assert_eq!(confidence.ranked_low_confidence_observation_count, 1);
        assert_eq!(confidence.ranked_high_confidence_observation_count, 1);
        assert_eq!(
            confidence.ranked_high_minus_low_mean_abs_analysis_error,
            Some(-5.0)
        );
        assert_eq!(confidence.reliability.status, "untestable");
        assert_eq!(confidence.reliability.semantic_label, "support_index");
        assert!(!confidence.reliability.bucket_coverage_sufficient);
        assert_eq!(
            confidence.reliability.min_ranked_bucket_observation_count,
            CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS
        );
        assert_eq!(
            validation.samples[0]
                .temperature_c
                .as_ref()
                .unwrap()
                .confidence,
            Some(0.1)
        );
    }

    #[test]
    fn confidence_reliability_requires_professional_ranked_bucket_coverage() {
        let undercovered = confidence_reliability_contract_from_ranked_buckets(9, 9, Some(-1.0));
        assert_eq!(undercovered.status, "untestable");
        assert_eq!(undercovered.semantic_label, "support_index");
        assert!(!undercovered.bucket_coverage_sufficient);
        assert_eq!(
            undercovered.min_ranked_bucket_observation_count,
            CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS
        );

        let passing = confidence_reliability_contract_from_ranked_buckets(10, 10, Some(-0.1));
        assert_eq!(passing.status, "passed");
        assert_eq!(passing.semantic_label, "calibrated_reliability");
        assert!(passing.bucket_coverage_sufficient);

        let failing = confidence_reliability_contract_from_ranked_buckets(10, 10, Some(0.1));
        assert_eq!(failing.status, "failed");
        assert_eq!(failing.semantic_label, "uncalibrated_support");
        assert!(failing.bucket_coverage_sufficient);
    }

    #[test]
    fn external_reference_comparison_uses_same_validation_samples() {
        let background_surface = sample_surface();
        let mut reference_surface = sample_surface();
        reference_surface.t2_k = vec![294.65; 3];
        let observations = vec![MesoObservation::new("OKC", 35.0, -98.0)
            .with_source("unit")
            .with_timestamp("2026-05-13T01:00:00Z")
            .with_temperature_c(22.0)];
        let candidate_fields = MesoanalysisFields {
            temperature_2m_c: vec![21.0; 3],
            dewpoint_2m_c: vec![15.0; 3],
            q2_kgkg: background_surface.q2_kgkg.clone(),
            u10_ms: background_surface.u10_ms.clone(),
            v10_ms: background_surface.v10_ms.clone(),
            mean_sea_level_pressure_hpa: None,
            temperature_increment_c: vec![1.0; 3],
            dewpoint_increment_c: vec![0.0; 3],
            u10_increment_ms: vec![0.0; 3],
            v10_increment_ms: vec![0.0; 3],
            mean_sea_level_pressure_increment_hpa: None,
            neighbor_count: vec![1; 3],
            temperature_confidence: vec![1.0; 3],
            dewpoint_confidence: vec![1.0; 3],
            u10_confidence: vec![1.0; 3],
            v10_confidence: vec![1.0; 3],
            mean_sea_level_pressure_confidence: None,
            diagnostics: Vec::new(),
        };

        let candidate_validation = validate_surface_mesoanalysis_at_observations(
            &background_surface,
            &candidate_fields,
            &observations,
        )
        .unwrap();
        let reference_validation =
            validate_surface_reference_at_observations(&reference_surface, &observations).unwrap();
        let comparison = compare_surface_mesoanalysis_to_external_reference(
            SurfaceMesoanalysisExternalReferenceDescriptor {
                reference_label: "rtma".to_string(),
                reference_model: "rtma".to_string(),
                reference_source: "nomads".to_string(),
                reference_cycle: "2026051301z".to_string(),
                reference_forecast_hour: 0,
                reference_product: "2dvaranl_ndfd".to_string(),
                candidate_label: "OptimalInterpolation".to_string(),
                background_label: "hrrr".to_string(),
                validation_mode: "holdout_validation".to_string(),
            },
            &candidate_validation,
            &reference_validation,
        );

        assert_eq!(comparison.reference_label, "rtma");
        assert_eq!(comparison.sampled_observation_count, 1);
        assert_eq!(
            comparison.temperature_c.background_mean_abs_error,
            Some(2.0)
        );
        assert_eq!(comparison.temperature_c.candidate_mean_abs_error, Some(1.0));
        assert_eq!(comparison.temperature_c.reference_mean_abs_error, Some(0.5));
        assert_eq!(
            comparison.temperature_c.candidate_minus_background_mae,
            Some(-1.0)
        );
        assert_eq!(
            comparison.temperature_c.candidate_minus_reference_mae,
            Some(0.5)
        );
        assert_eq!(comparison.temperature_c.mae_winner, "reference");
    }

    #[test]
    fn holdout_validation_recomputes_analysis_without_withheld_observations() {
        let surface = sample_surface();
        let observations = vec![
            MesoObservation::new("WEST", 35.0, -98.2)
                .with_source("unit")
                .with_temperature_c(24.0),
            MesoObservation::new("CENTER", 35.0, -98.0)
                .with_source("unit")
                .with_temperature_c(25.0),
            MesoObservation::new("EAST", 35.0, -97.8)
                .with_source("unit")
                .with_temperature_c(16.0),
        ];
        let config = MesoanalysisConfig {
            barnes_radius_km: 35.0,
            barnes_kappa_km2: 100.0,
            ..MesoanalysisConfig::default()
        };
        let fields =
            compute_surface_mesoanalysis_from_fields(&surface, &observations, config).unwrap();
        let report = summarize_surface_mesoanalysis_with_validation_and_holdout(
            &surface,
            &fields,
            &observations,
            config,
            0.34,
            7,
            1,
        )
        .unwrap();
        let holdout = report.holdout_validation.unwrap();

        assert_eq!(holdout.holdout_observation_count, 1);
        assert_eq!(holdout.training_observation_count, 2);
        assert_eq!(holdout.validation.observation_count, 1);
        assert_eq!(holdout.validation.sampled_observation_count, 1);
    }

    #[test]
    fn repeated_holdout_validation_aggregates_multiple_splits() {
        let surface = sample_surface();
        let observations = vec![
            MesoObservation::new("WEST", 35.0, -98.2)
                .with_source("unit")
                .with_temperature_c(24.0),
            MesoObservation::new("CENTER", 35.0, -98.0)
                .with_source("unit")
                .with_temperature_c(25.0),
            MesoObservation::new("EAST", 35.0, -97.8)
                .with_source("unit")
                .with_temperature_c(16.0),
        ];
        let config = MesoanalysisConfig {
            barnes_radius_km: 35.0,
            barnes_kappa_km2: 100.0,
            ..MesoanalysisConfig::default()
        };

        let repeated = compute_surface_mesoanalysis_repeated_holdout_validation(
            &surface,
            &observations,
            config,
            0.34,
            7,
            3,
            1,
        )
        .unwrap()
        .unwrap();

        assert_eq!(repeated.repeat_count, 3);
        assert_eq!(repeated.completed_fold_count, 3);
        assert_eq!(repeated.folds.len(), 3);
        assert_eq!(repeated.temperature_c.fold_count, 3);
        assert_eq!(repeated.temperature_c.total_observation_count, 3);
        assert_eq!(repeated.temperature_c.mean_observation_count, 1.0);
        assert!(repeated.temperature_c.mean_abs_analysis_error.is_some());
    }

    #[test]
    fn spatial_block_holdout_withholds_whole_spatial_groups() {
        let observations = vec![
            MesoObservation::new("A1", 34.2, -99.2).with_source("unit"),
            MesoObservation::new("A2", 34.4, -99.4).with_source("unit"),
            MesoObservation::new("B1", 38.2, -95.2).with_source("unit"),
            MesoObservation::new("B2", 38.4, -95.4).with_source("unit"),
        ];

        let (training, holdout) = deterministic_holdout_split(
            &observations,
            0.50,
            11,
            1,
            SurfaceMesoanalysisHoldoutStrategy::SpatialBlock,
        );

        let held_blocks = holdout
            .iter()
            .map(spatial_holdout_key)
            .collect::<BTreeSet<_>>();
        assert_eq!(training.len(), 2);
        assert_eq!(holdout.len(), 2);
        assert_eq!(held_blocks.len(), 1);
    }

    #[test]
    fn source_hash_holdout_withholds_whole_provider_groups() {
        let observations = vec![
            MesoObservation::new("A1", 35.0, -98.2).with_source("provider_a"),
            MesoObservation::new("A2", 35.0, -98.0).with_source("provider_a"),
            MesoObservation::new("B1", 35.0, -97.8).with_source("provider_b"),
            MesoObservation::new("B2", 35.0, -97.6).with_source("provider_b"),
        ];

        let (training, holdout) = deterministic_holdout_split(
            &observations,
            0.50,
            13,
            1,
            SurfaceMesoanalysisHoldoutStrategy::SourceHash,
        );

        let held_sources = holdout
            .iter()
            .map(|observation| observation.source.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(training.len(), 2);
        assert_eq!(holdout.len(), 2);
        assert_eq!(held_sources.len(), 1);
    }

    #[test]
    fn validation_gate_flags_pass_and_fail_thresholds() {
        let validation = SurfaceMesoanalysisValidationSummary {
            observation_count: 2,
            sampled_observation_count: 2,
            skipped_observation_count: 0,
            max_nearest_grid_distance_km: Some(1.0),
            temperature_c: VariableValidationSummary {
                observation_count: 2,
                improved_count: 2,
                worsened_count: 0,
                unchanged_count: 0,
                mean_abs_background_error: Some(5.0),
                mean_abs_analysis_error: Some(0.5),
                mean_abs_error_improvement: Some(4.5),
                ..VariableValidationSummary::default()
            },
            dewpoint_c: VariableValidationSummary {
                observation_count: 2,
                improved_count: 2,
                worsened_count: 0,
                unchanged_count: 0,
                mean_abs_background_error: Some(4.0),
                mean_abs_analysis_error: Some(0.4),
                mean_abs_error_improvement: Some(3.6),
                ..VariableValidationSummary::default()
            },
            wind_speed_ms: VariableValidationSummary {
                observation_count: 2,
                improved_count: 2,
                worsened_count: 0,
                unchanged_count: 0,
                mean_abs_background_error: Some(3.0),
                mean_abs_analysis_error: Some(0.3),
                mean_abs_error_improvement: Some(2.7),
                ..VariableValidationSummary::default()
            },
            mean_sea_level_pressure_hpa: None,
            source_summaries: Vec::new(),
            strata_summaries: Vec::new(),
            samples: Vec::new(),
        };

        let pass = evaluate_surface_mesoanalysis_validation_gate(
            &validation,
            SurfaceMesoanalysisValidationGateThresholds {
                min_sampled_observations: 2,
                max_skipped_observations: 0,
                max_nearest_grid_distance_km: 2.0,
                max_temperature_mean_abs_error_c: 1.0,
                max_dewpoint_mean_abs_error_c: 1.0,
                max_wind_speed_mean_abs_error_ms: 1.0,
            },
        );
        assert!(pass.passed);

        let fail = evaluate_surface_mesoanalysis_validation_gate(
            &validation,
            SurfaceMesoanalysisValidationGateThresholds {
                max_temperature_mean_abs_error_c: 0.1,
                ..pass.thresholds
            },
        );
        assert!(!fail.passed);
        assert!(fail
            .checks
            .iter()
            .any(|check| check.name == "temperature_c_mean_abs_analysis_error" && !check.passed));
    }

    #[test]
    fn validation_comparison_reports_candidate_minus_baseline_error_deltas() {
        let candidate = SurfaceMesoanalysisValidationSummary {
            observation_count: 2,
            sampled_observation_count: 2,
            skipped_observation_count: 0,
            max_nearest_grid_distance_km: Some(1.0),
            temperature_c: VariableValidationSummary {
                observation_count: 2,
                mean_analysis_error: Some(-0.25),
                mean_abs_analysis_error: Some(0.8),
                analysis_rmse: Some(1.0),
                ..VariableValidationSummary::default()
            },
            dewpoint_c: VariableValidationSummary::default(),
            wind_speed_ms: VariableValidationSummary::default(),
            mean_sea_level_pressure_hpa: None,
            source_summaries: Vec::new(),
            strata_summaries: Vec::new(),
            samples: Vec::new(),
        };
        let baseline = SurfaceMesoanalysisValidationSummary {
            observation_count: 2,
            sampled_observation_count: 2,
            skipped_observation_count: 0,
            max_nearest_grid_distance_km: Some(1.0),
            temperature_c: VariableValidationSummary {
                observation_count: 2,
                mean_analysis_error: Some(0.50),
                mean_abs_analysis_error: Some(1.2),
                analysis_rmse: Some(1.5),
                ..VariableValidationSummary::default()
            },
            dewpoint_c: VariableValidationSummary::default(),
            wind_speed_ms: VariableValidationSummary::default(),
            mean_sea_level_pressure_hpa: None,
            source_summaries: Vec::new(),
            strata_summaries: Vec::new(),
            samples: Vec::new(),
        };

        let comparison =
            compare_surface_mesoanalysis_validations("oi", &candidate, "barnes", &baseline);

        assert_eq!(comparison.candidate_label, "oi");
        assert_eq!(comparison.baseline_label, "barnes");
        assert!(
            (comparison
                .temperature_c
                .mean_abs_analysis_error_delta
                .unwrap()
                + 0.4)
                .abs()
                < 1.0e-12
        );
        assert_eq!(comparison.temperature_c.analysis_rmse_delta, Some(-0.5));
        assert_eq!(
            comparison.temperature_c.mean_analysis_error_delta,
            Some(-0.75)
        );
    }

    #[test]
    fn benchmark_summary_distills_raw_candidate_and_baseline_skill() {
        let candidate = SurfaceMesoanalysisValidationSummary {
            observation_count: 3,
            sampled_observation_count: 3,
            skipped_observation_count: 0,
            max_nearest_grid_distance_km: Some(1.0),
            temperature_c: VariableValidationSummary {
                observation_count: 3,
                mean_abs_background_error: Some(2.0),
                mean_abs_analysis_error: Some(0.7),
                background_rmse: Some(2.4),
                analysis_rmse: Some(0.9),
                ..VariableValidationSummary::default()
            },
            dewpoint_c: VariableValidationSummary::default(),
            wind_speed_ms: VariableValidationSummary::default(),
            mean_sea_level_pressure_hpa: None,
            source_summaries: Vec::new(),
            strata_summaries: Vec::new(),
            samples: Vec::new(),
        };
        let baseline = SurfaceMesoanalysisValidationSummary {
            observation_count: 3,
            sampled_observation_count: 3,
            skipped_observation_count: 0,
            max_nearest_grid_distance_km: Some(1.0),
            temperature_c: VariableValidationSummary {
                observation_count: 3,
                mean_abs_background_error: Some(2.0),
                mean_abs_analysis_error: Some(1.1),
                background_rmse: Some(2.4),
                analysis_rmse: Some(1.3),
                ..VariableValidationSummary::default()
            },
            dewpoint_c: VariableValidationSummary::default(),
            wind_speed_ms: VariableValidationSummary::default(),
            mean_sea_level_pressure_hpa: None,
            source_summaries: Vec::new(),
            strata_summaries: Vec::new(),
            samples: Vec::new(),
        };

        let benchmark = benchmark_surface_mesoanalysis_validations(
            "oi",
            &candidate,
            "barnes",
            &baseline,
            "same_observation_validation",
        );

        assert_eq!(benchmark.candidate_label, "oi");
        assert_eq!(benchmark.baseline_label, "barnes");
        assert!(
            (benchmark
                .temperature_c
                .candidate_minus_background_mae
                .unwrap()
                + 1.3)
                .abs()
                < 1.0e-12
        );
        assert!(
            (benchmark
                .temperature_c
                .candidate_minus_baseline_mae
                .unwrap()
                + 0.4)
                .abs()
                < 1.0e-12
        );
        assert!(
            (benchmark
                .temperature_c
                .baseline_minus_background_rmse
                .unwrap()
                + 1.1)
                .abs()
                < 1.0e-12
        );
        assert!(
            (benchmark
                .temperature_c
                .candidate_minus_baseline_rmse
                .unwrap()
                + 0.4)
                .abs()
                < 1.0e-12
        );
        assert_eq!(benchmark.temperature_c.mae_winner, "candidate");
        assert_eq!(benchmark.temperature_c.rmse_winner, "candidate");
    }

    #[test]
    fn repeated_holdout_benchmark_counts_fold_wins() {
        fn validation(raw_mae: f64, analysis_mae: f64) -> SurfaceMesoanalysisValidationSummary {
            SurfaceMesoanalysisValidationSummary {
                observation_count: 2,
                sampled_observation_count: 2,
                skipped_observation_count: 0,
                max_nearest_grid_distance_km: Some(1.0),
                temperature_c: VariableValidationSummary {
                    observation_count: 2,
                    mean_abs_background_error: Some(raw_mae),
                    mean_abs_analysis_error: Some(analysis_mae),
                    background_rmse: Some(raw_mae + 0.2),
                    analysis_rmse: Some(analysis_mae + 0.2),
                    ..VariableValidationSummary::default()
                },
                dewpoint_c: VariableValidationSummary::default(),
                wind_speed_ms: VariableValidationSummary::default(),
                mean_sea_level_pressure_hpa: None,
                source_summaries: Vec::new(),
                strata_summaries: Vec::new(),
                samples: Vec::new(),
            }
        }

        fn repeated(
            validations: Vec<SurfaceMesoanalysisValidationSummary>,
        ) -> SurfaceMesoanalysisRepeatedHoldoutValidationSummary {
            let folds = validations
                .into_iter()
                .enumerate()
                .map(
                    |(index, validation)| SurfaceMesoanalysisHoldoutValidationSummary {
                        schema: "rustwx.surface_mesoanalysis.holdout_validation.v1".to_string(),
                        requested_fraction: 0.5,
                        seed: index as u64,
                        strategy: SurfaceMesoanalysisHoldoutStrategy::StationHash,
                        min_holdout_observations: 1,
                        training_observation_count: 2,
                        holdout_observation_count: 2,
                        selection_rule: "unit".to_string(),
                        validation,
                    },
                )
                .collect::<Vec<_>>();
            SurfaceMesoanalysisRepeatedHoldoutValidationSummary {
                schema: "rustwx.surface_mesoanalysis.repeated_holdout_validation.v1".to_string(),
                requested_fraction: 0.5,
                seed: 0,
                repeat_count: folds.len(),
                completed_fold_count: folds.len(),
                strategy: SurfaceMesoanalysisHoldoutStrategy::StationHash,
                min_holdout_observations: 1,
                selection_rule: "unit".to_string(),
                temperature_c: summarize_repeated_variable_validation(
                    folds.iter().map(|fold| &fold.validation.temperature_c),
                ),
                dewpoint_c: summarize_repeated_variable_validation(
                    folds.iter().map(|fold| &fold.validation.dewpoint_c),
                ),
                wind_speed_ms: summarize_repeated_variable_validation(
                    folds.iter().map(|fold| &fold.validation.wind_speed_ms),
                ),
                mean_sea_level_pressure_hpa: None,
                folds,
            }
        }

        let candidate = repeated(vec![validation(2.0, 0.8), validation(2.0, 1.4)]);
        let baseline = repeated(vec![validation(2.0, 1.2), validation(2.0, 1.1)]);

        let benchmark = benchmark_surface_mesoanalysis_repeated_holdout_validations(
            "oi",
            &candidate,
            "barnes",
            &baseline,
            "repeated_holdout_validation",
        );

        assert_eq!(benchmark.fold_count, 2);
        assert_eq!(
            benchmark
                .temperature_c
                .candidate_beats_background_mae_fold_count,
            2
        );
        assert_eq!(
            benchmark
                .temperature_c
                .candidate_beats_baseline_mae_fold_count,
            1
        );
        assert!(
            (benchmark
                .temperature_c
                .candidate_minus_baseline_mae
                .unwrap()
                + 0.05)
                .abs()
                < 1.0e-12
        );
    }

    #[test]
    fn grid_export_writes_wxstore_compatible_manifest() {
        let surface = sample_surface();
        let observations = vec![MesoObservation::new("OKC", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(25.0)
            .with_dewpoint_c(18.0)
            .with_wind(270.0, 10.0)];
        let fields = compute_surface_mesoanalysis_from_fields(
            &surface,
            &observations,
            MesoanalysisConfig {
                barnes_radius_km: 30.0,
                barnes_kappa_km2: 100.0,
                ..MesoanalysisConfig::default()
            },
        )
        .unwrap();
        let out_dir =
            std::env::temp_dir().join(format!("rustwx_meso_grid_export_{}", std::process::id()));
        let _ = fs::remove_dir_all(&out_dir);

        let manifest = write_surface_mesoanalysis_grid_export(
            &SurfaceMesoanalysisGridExportRequest {
                model: "hrrr".to_string(),
                run_id: "unit_run".to_string(),
                member: "control".to_string(),
                date_yyyymmdd: "20260512".to_string(),
                cycle_utc: 0,
                source: "unit".to_string(),
                forecast_hour: 0,
                valid_time: "2026-05-12T00:00:00Z".to_string(),
                out_dir: out_dir.clone(),
            },
            &surface,
            &fields,
        )
        .unwrap();

        assert_eq!(
            manifest.schema,
            "rustwx.surface_mesoanalysis.grid_export.v1"
        );
        assert_eq!(manifest.fields.len(), 15);
        assert!(out_dir.join("manifest.json").is_file());
        assert!(out_dir.join("grid_lat.f32").is_file());
        assert!(out_dir.join("grid_lon.f32").is_file());
        let temperature = manifest
            .fields
            .iter()
            .find(|field| field.product_slug == "meso_temperature_2m_c")
            .unwrap();
        assert_eq!(temperature.nx, 3);
        assert_eq!(temperature.ny, 1);
        assert_eq!(
            fs::metadata(out_dir.join(&temperature.values_path))
                .unwrap()
                .len(),
            12
        );
        let confidence = manifest
            .fields
            .iter()
            .find(|field| field.product_slug == "meso_temperature_confidence")
            .unwrap();
        assert!(out_dir.join(&confidence.values_path).is_file());

        let _ = fs::remove_dir_all(out_dir);
    }
}
