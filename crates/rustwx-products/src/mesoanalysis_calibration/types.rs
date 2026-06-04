use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationReport {
    pub schema: String,
    pub generated_at: String,
    pub requested_report_count: usize,
    pub loaded_case_count: usize,
    pub skipped_reports: Vec<SurfaceMesoanalysisCalibrationSkippedReport>,
    pub quality_flags: Vec<String>,
    pub cases: Vec<SurfaceMesoanalysisCalibrationCase>,
    pub aggregate: SurfaceMesoanalysisCalibrationAggregate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calibration_gate: Option<SurfaceMesoanalysisCalibrationGate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationSkippedReport {
    pub path: PathBuf,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationHistory {
    pub schema: String,
    pub generated_at: String,
    pub calibration_schema: String,
    pub calibration_generated_at: String,
    pub case_count: usize,
    pub station_series: BTreeMap<String, SurfaceMesoanalysisStationInnovationHistorySeries>,
    pub source_series: BTreeMap<String, SurfaceMesoanalysisSourceInnovationHistorySeries>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub station_watchlist: Vec<SurfaceMesoanalysisStationInnovationWatchItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_watchlist: Vec<SurfaceMesoanalysisSourceInnovationWatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisStationInnovationWatchItem {
    pub station_key: String,
    pub station_id: String,
    pub source: String,
    pub variable: String,
    pub case_count: usize,
    pub observation_count: usize,
    pub mean_abs_analysis_error: Option<f64>,
    pub abs_analysis_bias: Option<f64>,
    pub mean_abs_error_improvement: Option<f64>,
    pub max_abs_analysis_error: Option<f64>,
    pub severity_score: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisSourceInnovationWatchItem {
    pub source: String,
    pub variable: String,
    pub case_count: usize,
    pub mean_observation_count: Option<f64>,
    pub mean_candidate_mae: Option<f64>,
    pub mean_candidate_minus_background_mae: Option<f64>,
    pub worst_candidate_minus_background_mae: Option<f64>,
    pub severity_score: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationQueryRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variables: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_case_count: Option<usize>,
    pub top: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationQueryReport {
    pub schema: String,
    pub generated_at: String,
    pub history_schema: String,
    pub history_generated_at: String,
    pub history_case_count: usize,
    pub request: SurfaceMesoanalysisInnovationQueryRequest,
    pub matched_station_watchlist_count: usize,
    pub matched_source_watchlist_count: usize,
    pub station_watchlist: Vec<SurfaceMesoanalysisStationInnovationWatchItem>,
    pub source_watchlist: Vec<SurfaceMesoanalysisSourceInnovationWatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationWxStoreIndexManifest {
    pub schema: String,
    pub generated_at: String,
    pub history_schema: String,
    pub history_generated_at: String,
    pub history_case_count: usize,
    pub station_series_count: usize,
    pub source_series_count: usize,
    pub station_index_path: PathBuf,
    pub source_index_path: PathBuf,
    pub station_watchlist_path: PathBuf,
    pub source_watchlist_path: PathBuf,
    pub query_policy: SurfaceMesoanalysisInnovationWxStoreQueryPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationWxStoreQueryPolicy {
    pub station_keys: Vec<String>,
    pub source_keys: Vec<String>,
    pub variable_key: String,
    pub sortable_fields: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationWxStoreStationRecord {
    pub station_key: String,
    pub station_id: String,
    pub source: String,
    pub variable: String,
    pub case_count: usize,
    pub sample_count: usize,
    pub observation_count: usize,
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
    pub watchlist: Option<SurfaceMesoanalysisStationInnovationWatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationWxStoreSourceRecord {
    pub source: String,
    pub variable: String,
    pub case_count: usize,
    pub mean_sampled_observation_count: Option<f64>,
    pub mean_observation_count: Option<f64>,
    pub mean_background_mae: Option<f64>,
    pub mean_candidate_mae: Option<f64>,
    pub mean_candidate_minus_background_mae: Option<f64>,
    pub mean_background_rmse: Option<f64>,
    pub mean_candidate_rmse: Option<f64>,
    pub mean_candidate_minus_background_rmse: Option<f64>,
    pub candidate_beats_background_mae_case_count: usize,
    pub candidate_loses_background_mae_case_count: usize,
    pub worst_candidate_minus_background_mae: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watchlist: Option<SurfaceMesoanalysisSourceInnovationWatchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisStationInnovationHistorySeries {
    pub station_id: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<SurfaceMesoanalysisCalibrationStationAggregate>,
    pub entries: Vec<SurfaceMesoanalysisStationInnovationHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisStationInnovationHistoryEntry {
    pub case: SurfaceMesoanalysisInnovationHistoryCase,
    pub sample_count: usize,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationStationVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisSourceInnovationHistorySeries {
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<SurfaceMesoanalysisCalibrationSourceAggregate>,
    pub entries: Vec<SurfaceMesoanalysisSourceInnovationHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisSourceInnovationHistoryEntry {
    pub case: SurfaceMesoanalysisInnovationHistoryCase,
    pub sampled_observation_count: usize,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisInnovationHistoryCase {
    pub source_path: PathBuf,
    pub case_signature: String,
    pub model: String,
    pub model_source: String,
    pub model_cycle: String,
    pub date: String,
    pub cycle: Option<u8>,
    pub forecast_hour: u16,
    pub benchmark_mode: String,
    pub holdout_strategy: Option<String>,
    #[serde(default)]
    pub case_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationCase {
    pub source_path: PathBuf,
    pub run_schema: String,
    pub model: String,
    pub model_source: String,
    pub model_cycle: String,
    pub date: String,
    pub cycle: Option<u8>,
    pub forecast_hour: u16,
    pub model_load_mode: String,
    #[serde(default)]
    pub case_tags: Vec<String>,
    pub analysis_method: String,
    pub covariance_kernel: Option<String>,
    pub holdout_strategy: Option<String>,
    pub repeated_fold_count: Option<usize>,
    pub validation_gate_passed: Option<bool>,
    pub observation_count: Option<usize>,
    pub source_count: Option<usize>,
    pub grid_export_field_count: Option<usize>,
    pub mesoanalysis_compute_ms: Option<u128>,
    pub benchmark_mode: String,
    #[serde(default)]
    pub diagnostics: BTreeMap<String, SurfaceMesoanalysisCalibrationDiagnosticCase>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase>,
    pub sources: BTreeMap<String, SurfaceMesoanalysisCalibrationSourceCase>,
    #[serde(default)]
    pub strata: BTreeMap<String, SurfaceMesoanalysisCalibrationStratumCase>,
    #[serde(default)]
    pub stations: BTreeMap<String, SurfaceMesoanalysisCalibrationStationCase>,
    pub external_references: BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceCase>,
    #[serde(default)]
    pub covariance_ablations: BTreeMap<String, SurfaceMesoanalysisCalibrationAblationCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationDiagnosticCase {
    pub candidate_observations: usize,
    pub accepted_observations: usize,
    pub rejected_observations: usize,
    #[serde(default)]
    pub gross_error_rescued_observations: usize,
    pub covered_grid_cells: usize,
    pub solver_failed_grid_cells: usize,
    pub truncated_neighbor_grid_cells: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationVariableCase {
    pub observation_count: Option<usize>,
    pub fold_count: Option<usize>,
    pub background_mean_abs_error: Option<f64>,
    pub candidate_mean_abs_error: Option<f64>,
    pub barnes_mean_abs_error: Option<f64>,
    pub candidate_minus_background_mae: Option<f64>,
    pub candidate_minus_barnes_mae: Option<f64>,
    pub background_rmse: Option<f64>,
    pub candidate_rmse: Option<f64>,
    pub barnes_rmse: Option<f64>,
    pub candidate_minus_background_rmse: Option<f64>,
    pub candidate_minus_barnes_rmse: Option<f64>,
    pub mae_winner: Option<String>,
    pub rmse_winner: Option<String>,
    pub candidate_beats_background_mae_fold_count: Option<usize>,
    pub candidate_beats_barnes_mae_fold_count: Option<usize>,
    pub candidate_beats_background_rmse_fold_count: Option<usize>,
    pub candidate_beats_barnes_rmse_fold_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<SurfaceMesoanalysisCalibrationConfidenceCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationConfidenceCase {
    pub observation_count: Option<usize>,
    pub mean_confidence: Option<f64>,
    pub low_confidence_observation_count: Option<usize>,
    pub low_confidence_mean_abs_analysis_error: Option<f64>,
    pub medium_confidence_observation_count: Option<usize>,
    pub medium_confidence_mean_abs_analysis_error: Option<f64>,
    pub high_confidence_observation_count: Option<usize>,
    pub high_confidence_mean_abs_analysis_error: Option<f64>,
    pub high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub confidence_abs_error_correlation: Option<f64>,
    pub ranked_low_confidence_observation_count: Option<usize>,
    pub ranked_low_confidence_mean_abs_analysis_error: Option<f64>,
    pub ranked_high_confidence_observation_count: Option<usize>,
    pub ranked_high_confidence_mean_abs_analysis_error: Option<f64>,
    pub ranked_high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub reliability: SurfaceMesoanalysisCalibrationConfidenceReliabilityCase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationConfidenceReliabilityCase {
    pub schema: String,
    pub semantic_label: String,
    pub status: String,
    pub bucket_coverage_sufficient: bool,
    pub ranked_low_confidence_observation_count: Option<usize>,
    pub ranked_high_confidence_observation_count: Option<usize>,
    pub min_ranked_bucket_observation_count: usize,
    pub ranked_high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub max_ranked_high_minus_low_mean_abs_analysis_error: f64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationSourceCase {
    pub sampled_observation_count: usize,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationStratumCase {
    pub stratum_type: String,
    pub stratum_value: String,
    pub sampled_observation_count: usize,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationStationCase {
    pub station_id: String,
    pub source: String,
    pub sample_count: usize,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationStationVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationStationVariableCase {
    pub observation_count: usize,
    pub mean_background_error: Option<f64>,
    pub mean_analysis_error: Option<f64>,
    pub mean_abs_background_error: Option<f64>,
    pub mean_abs_analysis_error: Option<f64>,
    pub mean_abs_error_improvement: Option<f64>,
    pub background_rmse: Option<f64>,
    pub analysis_rmse: Option<f64>,
    pub max_abs_background_error: Option<f64>,
    pub max_abs_analysis_error: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationReferenceCase {
    pub reference_label: String,
    pub validation_mode: String,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationReferenceVariableCase {
    pub observation_count: Option<usize>,
    pub candidate_mean_abs_error: Option<f64>,
    pub reference_mean_abs_error: Option<f64>,
    pub candidate_minus_reference_mae: Option<f64>,
    pub candidate_rmse: Option<f64>,
    pub reference_rmse: Option<f64>,
    pub candidate_minus_reference_rmse: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationAblationCase {
    pub baseline_label: String,
    pub candidate_label: String,
    pub validation_mode: String,
    pub baseline_compute_ms: Option<u128>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationAblationVariableCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationAblationVariableCase {
    pub candidate_observation_count: Option<usize>,
    pub baseline_observation_count: Option<usize>,
    pub candidate_mean_abs_error: Option<f64>,
    pub baseline_mean_abs_error: Option<f64>,
    pub candidate_minus_baseline_mae: Option<f64>,
    pub candidate_rmse: Option<f64>,
    pub baseline_rmse: Option<f64>,
    pub candidate_minus_baseline_rmse: Option<f64>,
    pub mae_winner: Option<String>,
    pub rmse_winner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationAggregate {
    pub case_count: usize,
    pub validation_gate_passed_count: usize,
    pub validation_gate_failed_count: usize,
    pub mean_mesoanalysis_compute_ms: Option<f64>,
    pub max_mesoanalysis_compute_ms: Option<f64>,
    pub model_counts: BTreeMap<String, usize>,
    pub model_source_counts: BTreeMap<String, usize>,
    pub date_counts: BTreeMap<String, usize>,
    pub cycle_counts: BTreeMap<String, usize>,
    pub forecast_hour_counts: BTreeMap<String, usize>,
    pub case_signature_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub case_tag_counts: BTreeMap<String, usize>,
    pub method_counts: BTreeMap<String, usize>,
    pub benchmark_mode_counts: BTreeMap<String, usize>,
    pub holdout_strategy_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub diagnostics: BTreeMap<String, SurfaceMesoanalysisCalibrationDiagnosticAggregate>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
    pub sources: BTreeMap<String, SurfaceMesoanalysisCalibrationSourceAggregate>,
    #[serde(default)]
    pub strata: BTreeMap<String, SurfaceMesoanalysisCalibrationStratumAggregate>,
    #[serde(default)]
    pub stations: BTreeMap<String, SurfaceMesoanalysisCalibrationStationAggregate>,
    pub external_references: BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceAggregate>,
    #[serde(default)]
    pub covariance_ablations: BTreeMap<String, SurfaceMesoanalysisCalibrationAblationAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationDiagnosticAggregate {
    pub case_count: usize,
    pub mean_candidate_observations: Option<f64>,
    pub mean_accepted_observations: Option<f64>,
    pub mean_rejected_observations: Option<f64>,
    pub total_gross_error_rescued_observations: usize,
    pub max_gross_error_rescued_observations: usize,
    pub mean_covered_grid_cells: Option<f64>,
    pub total_solver_failed_grid_cells: usize,
    pub total_truncated_neighbor_grid_cells: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationVariableAggregate {
    pub case_count: usize,
    pub mean_observation_count: Option<f64>,
    pub mean_fold_count: Option<f64>,
    pub mean_background_mae: Option<f64>,
    pub mean_candidate_mae: Option<f64>,
    pub mean_barnes_mae: Option<f64>,
    pub mean_candidate_minus_background_mae: Option<f64>,
    pub mean_candidate_minus_barnes_mae: Option<f64>,
    pub mean_background_rmse: Option<f64>,
    pub mean_candidate_rmse: Option<f64>,
    pub mean_barnes_rmse: Option<f64>,
    pub mean_candidate_minus_background_rmse: Option<f64>,
    pub mean_candidate_minus_barnes_rmse: Option<f64>,
    pub candidate_beats_background_mae_case_count: usize,
    pub candidate_beats_barnes_mae_case_count: usize,
    pub candidate_beats_background_rmse_case_count: usize,
    pub candidate_beats_barnes_rmse_case_count: usize,
    pub candidate_loses_background_mae_case_count: usize,
    pub candidate_loses_barnes_mae_case_count: usize,
    pub worst_candidate_minus_background_mae: Option<f64>,
    pub worst_candidate_minus_barnes_mae: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<SurfaceMesoanalysisCalibrationConfidenceAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationConfidenceAggregate {
    pub case_count: usize,
    pub mean_observation_count: Option<f64>,
    pub mean_confidence: Option<f64>,
    pub mean_low_confidence_observation_count: Option<f64>,
    pub min_low_confidence_observation_count: Option<f64>,
    pub mean_low_confidence_mae: Option<f64>,
    pub mean_medium_confidence_observation_count: Option<f64>,
    pub min_medium_confidence_observation_count: Option<f64>,
    pub mean_medium_confidence_mae: Option<f64>,
    pub mean_high_confidence_observation_count: Option<f64>,
    pub min_high_confidence_observation_count: Option<f64>,
    pub mean_high_confidence_mae: Option<f64>,
    pub mean_high_minus_low_confidence_mae: Option<f64>,
    pub worst_high_minus_low_confidence_mae: Option<f64>,
    pub mean_confidence_abs_error_correlation: Option<f64>,
    pub worst_confidence_abs_error_correlation: Option<f64>,
    pub mean_ranked_low_confidence_observation_count: Option<f64>,
    pub mean_ranked_low_confidence_mae: Option<f64>,
    pub mean_ranked_high_confidence_observation_count: Option<f64>,
    pub mean_ranked_high_confidence_mae: Option<f64>,
    pub mean_ranked_high_minus_low_confidence_mae: Option<f64>,
    pub worst_ranked_high_minus_low_confidence_mae: Option<f64>,
    pub high_confidence_beats_low_confidence_mae_case_count: usize,
    pub high_confidence_loses_low_confidence_mae_case_count: usize,
    pub ranked_high_confidence_beats_low_confidence_mae_case_count: usize,
    pub ranked_high_confidence_loses_low_confidence_mae_case_count: usize,
    pub negative_confidence_abs_error_correlation_case_count: usize,
    pub positive_confidence_abs_error_correlation_case_count: usize,
    pub reliability: SurfaceMesoanalysisCalibrationConfidenceReliabilityAggregate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationConfidenceReliabilityAggregate {
    pub schema: String,
    pub semantic_label: String,
    pub status: String,
    pub bucket_coverage_sufficient: bool,
    pub case_count: usize,
    pub passed_case_count: usize,
    pub failed_case_count: usize,
    pub untestable_case_count: usize,
    pub min_ranked_low_confidence_observation_count: Option<f64>,
    pub min_ranked_high_confidence_observation_count: Option<f64>,
    pub min_ranked_bucket_observation_count: usize,
    pub worst_ranked_high_minus_low_mean_abs_analysis_error: Option<f64>,
    pub max_ranked_high_minus_low_mean_abs_analysis_error: f64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationSourceAggregate {
    pub case_count: usize,
    pub mean_sampled_observation_count: Option<f64>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationStratumAggregate {
    pub stratum_type: String,
    pub stratum_value: String,
    pub case_count: usize,
    pub mean_sampled_observation_count: Option<f64>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationStationAggregate {
    pub station_id: String,
    pub source: String,
    pub case_count: usize,
    pub sample_count: usize,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationStationVariableAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationStationVariableAggregate {
    pub case_count: usize,
    pub observation_count: usize,
    pub mean_background_error: Option<f64>,
    pub mean_analysis_error: Option<f64>,
    pub mean_abs_background_error: Option<f64>,
    pub mean_abs_analysis_error: Option<f64>,
    pub mean_abs_error_improvement: Option<f64>,
    pub background_rmse: Option<f64>,
    pub analysis_rmse: Option<f64>,
    pub max_abs_background_error: Option<f64>,
    pub max_abs_analysis_error: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationReferenceAggregate {
    pub case_count: usize,
    pub validation_mode_counts: BTreeMap<String, usize>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceVariableAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationReferenceVariableAggregate {
    pub case_count: usize,
    pub mean_observation_count: Option<f64>,
    pub mean_candidate_mae: Option<f64>,
    pub mean_reference_mae: Option<f64>,
    pub mean_candidate_minus_reference_mae: Option<f64>,
    pub mean_candidate_rmse: Option<f64>,
    pub mean_reference_rmse: Option<f64>,
    pub mean_candidate_minus_reference_rmse: Option<f64>,
    pub candidate_beats_reference_mae_case_count: usize,
    pub candidate_beats_reference_rmse_case_count: usize,
    pub candidate_loses_reference_mae_case_count: usize,
    pub worst_candidate_minus_reference_mae: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationAblationAggregate {
    pub case_count: usize,
    pub validation_mode_counts: BTreeMap<String, usize>,
    pub mean_baseline_compute_ms: Option<f64>,
    pub variables: BTreeMap<String, SurfaceMesoanalysisCalibrationAblationVariableAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationAblationVariableAggregate {
    pub case_count: usize,
    pub mean_candidate_observation_count: Option<f64>,
    pub mean_baseline_observation_count: Option<f64>,
    pub mean_candidate_mae: Option<f64>,
    pub mean_baseline_mae: Option<f64>,
    pub mean_candidate_minus_baseline_mae: Option<f64>,
    pub mean_candidate_rmse: Option<f64>,
    pub mean_baseline_rmse: Option<f64>,
    pub mean_candidate_minus_baseline_rmse: Option<f64>,
    pub candidate_beats_baseline_mae_case_count: usize,
    pub candidate_beats_baseline_rmse_case_count: usize,
    pub candidate_loses_baseline_mae_case_count: usize,
    pub worst_candidate_minus_baseline_mae: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationGateThresholds {
    pub min_case_count: usize,
    pub allow_skipped_reports: bool,
    pub allowed_quality_flags: Vec<String>,
    pub required_benchmark_modes: Vec<String>,
    pub required_holdout_strategies: Vec<String>,
    pub required_external_references: Vec<String>,
    #[serde(default)]
    pub required_covariance_ablations: Vec<String>,
    #[serde(default)]
    pub required_case_tags: Vec<String>,
    pub variables: Vec<String>,
    pub sources: Vec<String>,
    #[serde(default)]
    pub strata: Vec<String>,
    #[serde(default)]
    pub stations: Vec<String>,
    pub max_domain_candidate_minus_background_mae: Option<f64>,
    pub max_domain_candidate_minus_barnes_mae: Option<f64>,
    pub max_domain_candidate_minus_reference_mae: Option<f64>,
    pub max_covariance_ablation_candidate_minus_baseline_mae: Option<f64>,
    pub max_case_mesoanalysis_compute_ms: Option<f64>,
    pub min_unique_case_signatures: Option<usize>,
    pub min_unique_dates: Option<usize>,
    pub min_unique_cycles: Option<usize>,
    pub min_unique_forecast_hours: Option<usize>,
    pub min_unique_case_tags: Option<usize>,
    pub min_domain_low_confidence_observation_count: Option<usize>,
    pub min_domain_high_confidence_observation_count: Option<usize>,
    pub max_domain_high_minus_low_confidence_mae: Option<f64>,
    pub max_domain_ranked_high_minus_low_confidence_mae: Option<f64>,
    pub max_domain_confidence_abs_error_correlation: Option<f64>,
    pub max_source_candidate_minus_background_mae: Option<f64>,
    pub max_source_candidate_minus_barnes_mae: Option<f64>,
    pub min_source_low_confidence_observation_count: Option<usize>,
    pub min_source_high_confidence_observation_count: Option<usize>,
    pub max_source_high_minus_low_confidence_mae: Option<f64>,
    pub max_source_ranked_high_minus_low_confidence_mae: Option<f64>,
    pub max_source_confidence_abs_error_correlation: Option<f64>,
    pub max_stratum_candidate_minus_background_mae: Option<f64>,
    pub max_stratum_candidate_minus_barnes_mae: Option<f64>,
    pub min_stratum_low_confidence_observation_count: Option<usize>,
    pub min_stratum_high_confidence_observation_count: Option<usize>,
    pub max_stratum_high_minus_low_confidence_mae: Option<f64>,
    pub max_stratum_ranked_high_minus_low_confidence_mae: Option<f64>,
    pub max_stratum_confidence_abs_error_correlation: Option<f64>,
    pub min_station_observation_count: Option<usize>,
    pub max_station_candidate_minus_background_mae: Option<f64>,
    pub max_station_analysis_mae: Option<f64>,
    pub max_station_abs_analysis_bias: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationGate {
    pub schema: String,
    pub passed: bool,
    pub thresholds: SurfaceMesoanalysisCalibrationGateThresholds,
    pub checks: Vec<SurfaceMesoanalysisCalibrationGateCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SurfaceMesoanalysisCalibrationGateCheck {
    pub name: String,
    pub scope: String,
    pub passed: bool,
    pub observed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    pub comparator: String,
    pub message: String,
}
