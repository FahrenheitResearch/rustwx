use crate::mesoanalysis::{
    CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
    CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

mod types;
pub use types::*;
mod gates;
pub use gates::evaluate_surface_mesoanalysis_calibration_gate;
mod history;
mod parsing;
#[cfg(test)]
use gates::push_max_confidence_calibration_gate_check;
pub use history::{
    build_surface_mesoanalysis_innovation_history, merge_surface_mesoanalysis_innovation_history,
    query_surface_mesoanalysis_innovation_history, read_surface_mesoanalysis_innovation_history,
    write_surface_mesoanalysis_innovation_history,
    write_surface_mesoanalysis_innovation_query_report,
    write_surface_mesoanalysis_innovation_wxstore_index,
};
#[cfg(test)]
use history::{source_wxstore_index_records, station_wxstore_index_records};
use parsing::build_surface_mesoanalysis_calibration_report_from_values;

pub fn discover_surface_mesoanalysis_run_reports(roots: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
    let mut reports = Vec::new();
    for root in roots {
        discover_surface_mesoanalysis_run_reports_one(root, &mut reports)?;
    }
    reports.sort();
    reports.dedup();
    Ok(reports)
}

pub fn build_surface_mesoanalysis_calibration_report(
    paths: &[PathBuf],
) -> SurfaceMesoanalysisCalibrationReport {
    let values = paths.iter().map(|path| {
        let value = fs::read(path)
            .map_err(|error| format!("read failed: {error}"))
            .and_then(|bytes| {
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|error| format!("JSON parse failed: {error}"))
            });
        (path.clone(), value)
    });
    build_surface_mesoanalysis_calibration_report_from_values(values)
}

pub fn write_surface_mesoanalysis_calibration_report(
    path: &Path,
    report: &SurfaceMesoanalysisCalibrationReport,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

fn discover_surface_mesoanalysis_run_reports_one(
    root: &Path,
    reports: &mut Vec<PathBuf>,
) -> io::Result<()> {
    let metadata = fs::metadata(root)?;
    if metadata.is_file() {
        if root.file_name().and_then(|name| name.to_str()) == Some("run_report.json") {
            reports.push(root.to_path_buf());
        }
        return Ok(());
    }
    if !metadata.is_dir() {
        return Ok(());
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                stack.push(path);
            } else if path.file_name().and_then(|name| name.to_str()) == Some("run_report.json") {
                reports.push(path);
            }
        }
    }
    Ok(())
}

fn aggregate_calibration_cases(
    cases: &[SurfaceMesoanalysisCalibrationCase],
) -> SurfaceMesoanalysisCalibrationAggregate {
    let mut method_counts = BTreeMap::new();
    let mut benchmark_mode_counts = BTreeMap::new();
    let mut holdout_strategy_counts = BTreeMap::new();
    let mut model_counts = BTreeMap::new();
    let mut model_source_counts = BTreeMap::new();
    let mut date_counts = BTreeMap::new();
    let mut cycle_counts = BTreeMap::new();
    let mut forecast_hour_counts = BTreeMap::new();
    let mut case_signature_counts = BTreeMap::new();
    let mut case_tag_counts = BTreeMap::new();
    let mut diagnostic_accumulators = BTreeMap::<String, DiagnosticAggregateAccumulator>::new();
    let mut variable_accumulators = BTreeMap::<String, VariableAggregateAccumulator>::new();
    let mut source_accumulators = BTreeMap::<String, SourceAggregateAccumulator>::new();
    let mut stratum_accumulators = BTreeMap::<String, StratumAggregateAccumulator>::new();
    let mut station_accumulators = BTreeMap::<String, StationAggregateAccumulator>::new();
    let mut reference_accumulators = BTreeMap::<String, ReferenceAggregateAccumulator>::new();
    let mut ablation_accumulators = BTreeMap::<String, AblationAggregateAccumulator>::new();
    let mut mesoanalysis_compute_ms = Vec::new();
    let mut validation_gate_passed_count = 0usize;
    let mut validation_gate_failed_count = 0usize;

    for case in cases {
        push_u128_as_f64(&mut mesoanalysis_compute_ms, case.mesoanalysis_compute_ms);
        increment_count(&mut model_counts, non_empty_or_missing(&case.model));
        increment_count(
            &mut model_source_counts,
            non_empty_or_missing(&case.model_source),
        );
        increment_count(&mut date_counts, non_empty_or_missing(&case.date));
        increment_count(&mut cycle_counts, cycle_key(case.cycle));
        increment_count(
            &mut forecast_hour_counts,
            forecast_hour_key(case.forecast_hour),
        );
        increment_count(&mut case_signature_counts, case_signature(case));
        for tag in &case.case_tags {
            increment_count(&mut case_tag_counts, tag.clone());
        }
        increment_count(
            &mut method_counts,
            non_empty_or_missing(&case.analysis_method),
        );
        increment_count(
            &mut benchmark_mode_counts,
            non_empty_or_missing(&case.benchmark_mode),
        );
        if let Some(strategy) = case.holdout_strategy.as_ref() {
            increment_count(&mut holdout_strategy_counts, strategy.clone());
        }
        match case.validation_gate_passed {
            Some(true) => validation_gate_passed_count += 1,
            Some(false) => validation_gate_failed_count += 1,
            None => {}
        }
        for (name, diagnostic) in &case.diagnostics {
            diagnostic_accumulators
                .entry(name.clone())
                .or_default()
                .push(diagnostic);
        }
        for (name, variable) in &case.variables {
            variable_accumulators
                .entry(name.clone())
                .or_default()
                .push(variable);
        }
        for (source, summary) in &case.sources {
            source_accumulators
                .entry(source.clone())
                .or_default()
                .push(summary);
        }
        for (stratum, summary) in &case.strata {
            stratum_accumulators
                .entry(stratum.clone())
                .or_insert_with(|| {
                    StratumAggregateAccumulator::new(
                        summary.stratum_type.clone(),
                        summary.stratum_value.clone(),
                    )
                })
                .push(summary);
        }
        for (station, summary) in &case.stations {
            station_accumulators
                .entry(station.clone())
                .or_insert_with(|| {
                    StationAggregateAccumulator::new(
                        summary.station_id.clone(),
                        summary.source.clone(),
                    )
                })
                .push(summary);
        }
        for (label, reference) in &case.external_references {
            reference_accumulators
                .entry(label.clone())
                .or_default()
                .push(reference);
        }
        for (label, ablation) in &case.covariance_ablations {
            ablation_accumulators
                .entry(label.clone())
                .or_default()
                .push(ablation);
        }
    }

    SurfaceMesoanalysisCalibrationAggregate {
        case_count: cases.len(),
        validation_gate_passed_count,
        validation_gate_failed_count,
        mean_mesoanalysis_compute_ms: mean(&mesoanalysis_compute_ms),
        max_mesoanalysis_compute_ms: max(&mesoanalysis_compute_ms),
        model_counts,
        model_source_counts,
        date_counts,
        cycle_counts,
        forecast_hour_counts,
        case_signature_counts,
        case_tag_counts,
        method_counts,
        benchmark_mode_counts,
        holdout_strategy_counts,
        diagnostics: diagnostic_accumulators
            .into_iter()
            .map(|(name, accumulator)| (name, accumulator.finish()))
            .collect(),
        variables: variable_accumulators
            .into_iter()
            .map(|(name, accumulator)| (name, accumulator.finish()))
            .collect(),
        sources: source_accumulators
            .into_iter()
            .map(|(source, accumulator)| (source, accumulator.finish()))
            .collect(),
        strata: stratum_accumulators
            .into_iter()
            .map(|(stratum, accumulator)| (stratum, accumulator.finish()))
            .collect(),
        stations: station_accumulators
            .into_iter()
            .map(|(station, accumulator)| (station, accumulator.finish()))
            .collect(),
        external_references: reference_accumulators
            .into_iter()
            .map(|(label, accumulator)| (label, accumulator.finish()))
            .collect(),
        covariance_ablations: ablation_accumulators
            .into_iter()
            .map(|(label, accumulator)| (label, accumulator.finish()))
            .collect(),
    }
}

fn calibration_quality_flags(
    cases: &[SurfaceMesoanalysisCalibrationCase],
    skipped_reports: &[SurfaceMesoanalysisCalibrationSkippedReport],
    aggregate: &SurfaceMesoanalysisCalibrationAggregate,
) -> Vec<String> {
    let mut flags = Vec::new();
    if cases.is_empty() {
        flags.push("empty_calibration_matrix".to_string());
    }
    if !skipped_reports.is_empty() {
        flags.push("skipped_reports_present".to_string());
    }
    if aggregate.benchmark_mode_counts.len() > 1 {
        flags.push("mixed_benchmark_modes".to_string());
    }
    if aggregate
        .benchmark_mode_counts
        .contains_key("same_observation_validation")
    {
        flags.push("contains_same_observation_validation".to_string());
    }
    if aggregate.validation_gate_failed_count > 0 {
        flags.push("validation_gate_failures".to_string());
    }
    if cases.len() > 1 && aggregate.case_signature_counts.len() <= 1 {
        flags.push("single_case_signature_matrix".to_string());
    }
    flags
}

#[derive(Debug, Clone, Default)]
struct DiagnosticAggregateAccumulator {
    candidate_observations: Vec<f64>,
    accepted_observations: Vec<f64>,
    rejected_observations: Vec<f64>,
    gross_error_rescued_observations: Vec<usize>,
    covered_grid_cells: Vec<f64>,
    total_solver_failed_grid_cells: usize,
    total_truncated_neighbor_grid_cells: usize,
}

impl DiagnosticAggregateAccumulator {
    fn push(&mut self, diagnostic: &SurfaceMesoanalysisCalibrationDiagnosticCase) {
        self.candidate_observations
            .push(diagnostic.candidate_observations as f64);
        self.accepted_observations
            .push(diagnostic.accepted_observations as f64);
        self.rejected_observations
            .push(diagnostic.rejected_observations as f64);
        self.gross_error_rescued_observations
            .push(diagnostic.gross_error_rescued_observations);
        self.covered_grid_cells
            .push(diagnostic.covered_grid_cells as f64);
        self.total_solver_failed_grid_cells += diagnostic.solver_failed_grid_cells;
        self.total_truncated_neighbor_grid_cells += diagnostic.truncated_neighbor_grid_cells;
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationDiagnosticAggregate {
        SurfaceMesoanalysisCalibrationDiagnosticAggregate {
            case_count: self.candidate_observations.len(),
            mean_candidate_observations: mean(&self.candidate_observations),
            mean_accepted_observations: mean(&self.accepted_observations),
            mean_rejected_observations: mean(&self.rejected_observations),
            total_gross_error_rescued_observations: self
                .gross_error_rescued_observations
                .iter()
                .sum(),
            max_gross_error_rescued_observations: self
                .gross_error_rescued_observations
                .into_iter()
                .max()
                .unwrap_or(0),
            mean_covered_grid_cells: mean(&self.covered_grid_cells),
            total_solver_failed_grid_cells: self.total_solver_failed_grid_cells,
            total_truncated_neighbor_grid_cells: self.total_truncated_neighbor_grid_cells,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SourceSummaryAccumulator {
    sampled_observation_count: usize,
    variables: BTreeMap<String, SourceVariableStatsAccumulator>,
}

#[derive(Debug, Clone, Default)]
struct StratumSummaryAccumulator {
    stratum_type: String,
    stratum_value: String,
    summary: SourceSummaryAccumulator,
}

impl StratumSummaryAccumulator {
    fn new(stratum_type: String, stratum_value: String) -> Self {
        Self {
            stratum_type,
            stratum_value,
            summary: SourceSummaryAccumulator::default(),
        }
    }

    fn push_stratum_summary(&mut self, summary: &Value) {
        self.summary.push_source_summary(summary);
    }
}

#[derive(Debug, Clone, Default)]
struct StationSummaryAccumulator {
    station_id: String,
    source: String,
    sample_count: usize,
    variables: BTreeMap<String, StationVariableStatsAccumulator>,
}

impl StationSummaryAccumulator {
    fn new(station_id: String, source: String) -> Self {
        Self {
            station_id,
            source,
            sample_count: 0,
            variables: BTreeMap::new(),
        }
    }

    fn push_sample(&mut self, sample: &Value) {
        self.sample_count += 1;
        for variable in [
            "temperature_c",
            "dewpoint_c",
            "wind_speed_ms",
            "mean_sea_level_pressure_hpa",
        ] {
            if let Some(value) = value_at(sample, &[variable]) {
                self.variables
                    .entry(variable.to_string())
                    .or_default()
                    .push_variable(value);
            }
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationStationCase {
        SurfaceMesoanalysisCalibrationStationCase {
            station_id: self.station_id,
            source: self.source,
            sample_count: self.sample_count,
            variables: self
                .variables
                .into_iter()
                .map(|(name, accumulator)| (name, accumulator.finish_case()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct StationAggregateAccumulator {
    station_id: String,
    source: String,
    case_count: usize,
    sample_count: usize,
    variables: BTreeMap<String, StationVariableStatsAccumulator>,
}

impl StationAggregateAccumulator {
    fn new(station_id: String, source: String) -> Self {
        Self {
            station_id,
            source,
            case_count: 0,
            sample_count: 0,
            variables: BTreeMap::new(),
        }
    }

    fn push(&mut self, station: &SurfaceMesoanalysisCalibrationStationCase) {
        self.case_count += 1;
        self.sample_count += station.sample_count;
        for (name, variable) in &station.variables {
            self.variables
                .entry(name.clone())
                .or_default()
                .push_variable_case(variable);
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationStationAggregate {
        SurfaceMesoanalysisCalibrationStationAggregate {
            station_id: self.station_id,
            source: self.source,
            case_count: self.case_count,
            sample_count: self.sample_count,
            variables: self
                .variables
                .into_iter()
                .map(|(name, accumulator)| (name, accumulator.finish_aggregate()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct StationVariableStatsAccumulator {
    observation_count: usize,
    case_count: usize,
    background_error_sum: f64,
    analysis_error_sum: f64,
    abs_background_error_sum: f64,
    abs_analysis_error_sum: f64,
    abs_error_improvement_sum: f64,
    background_square_error_sum: f64,
    analysis_square_error_sum: f64,
    max_abs_background_error: Option<f64>,
    max_abs_analysis_error: Option<f64>,
}

impl StationVariableStatsAccumulator {
    fn push_variable(&mut self, value: &Value) {
        let Some(background_error) = f64_at(value, &["background_error"]) else {
            return;
        };
        let Some(analysis_error) = f64_at(value, &["analysis_error"]) else {
            return;
        };
        let improvement = f64_at(value, &["abs_error_improvement"])
            .unwrap_or_else(|| background_error.abs() - analysis_error.abs());
        self.observation_count += 1;
        self.background_error_sum += background_error;
        self.analysis_error_sum += analysis_error;
        self.abs_background_error_sum += background_error.abs();
        self.abs_analysis_error_sum += analysis_error.abs();
        self.abs_error_improvement_sum += improvement;
        self.background_square_error_sum += background_error * background_error;
        self.analysis_square_error_sum += analysis_error * analysis_error;
        self.max_abs_background_error = Some(
            self.max_abs_background_error
                .unwrap_or(0.0)
                .max(background_error.abs()),
        );
        self.max_abs_analysis_error = Some(
            self.max_abs_analysis_error
                .unwrap_or(0.0)
                .max(analysis_error.abs()),
        );
    }

    fn push_variable_case(&mut self, variable: &SurfaceMesoanalysisCalibrationStationVariableCase) {
        let count = variable.observation_count;
        if count == 0 {
            return;
        }
        self.case_count += 1;
        self.observation_count += count;
        self.background_error_sum += variable.mean_background_error.unwrap_or(0.0) * count as f64;
        self.analysis_error_sum += variable.mean_analysis_error.unwrap_or(0.0) * count as f64;
        self.abs_background_error_sum +=
            variable.mean_abs_background_error.unwrap_or(0.0) * count as f64;
        self.abs_analysis_error_sum +=
            variable.mean_abs_analysis_error.unwrap_or(0.0) * count as f64;
        self.abs_error_improvement_sum +=
            variable.mean_abs_error_improvement.unwrap_or(0.0) * count as f64;
        if let Some(rmse) = variable.background_rmse {
            self.background_square_error_sum += rmse * rmse * count as f64;
        }
        if let Some(rmse) = variable.analysis_rmse {
            self.analysis_square_error_sum += rmse * rmse * count as f64;
        }
        if let Some(value) = variable.max_abs_background_error {
            self.max_abs_background_error =
                Some(self.max_abs_background_error.unwrap_or(0.0).max(value));
        }
        if let Some(value) = variable.max_abs_analysis_error {
            self.max_abs_analysis_error =
                Some(self.max_abs_analysis_error.unwrap_or(0.0).max(value));
        }
    }

    fn finish_case(self) -> SurfaceMesoanalysisCalibrationStationVariableCase {
        SurfaceMesoanalysisCalibrationStationVariableCase {
            observation_count: self.observation_count,
            mean_background_error: mean_if_count(self.background_error_sum, self.observation_count),
            mean_analysis_error: mean_if_count(self.analysis_error_sum, self.observation_count),
            mean_abs_background_error: mean_if_count(
                self.abs_background_error_sum,
                self.observation_count,
            ),
            mean_abs_analysis_error: mean_if_count(
                self.abs_analysis_error_sum,
                self.observation_count,
            ),
            mean_abs_error_improvement: mean_if_count(
                self.abs_error_improvement_sum,
                self.observation_count,
            ),
            background_rmse: rmse_if_count(
                self.background_square_error_sum,
                self.observation_count,
            ),
            analysis_rmse: rmse_if_count(self.analysis_square_error_sum, self.observation_count),
            max_abs_background_error: self.max_abs_background_error,
            max_abs_analysis_error: self.max_abs_analysis_error,
        }
    }

    fn finish_aggregate(self) -> SurfaceMesoanalysisCalibrationStationVariableAggregate {
        SurfaceMesoanalysisCalibrationStationVariableAggregate {
            case_count: self.case_count,
            observation_count: self.observation_count,
            mean_background_error: mean_if_count(self.background_error_sum, self.observation_count),
            mean_analysis_error: mean_if_count(self.analysis_error_sum, self.observation_count),
            mean_abs_background_error: mean_if_count(
                self.abs_background_error_sum,
                self.observation_count,
            ),
            mean_abs_analysis_error: mean_if_count(
                self.abs_analysis_error_sum,
                self.observation_count,
            ),
            mean_abs_error_improvement: mean_if_count(
                self.abs_error_improvement_sum,
                self.observation_count,
            ),
            background_rmse: rmse_if_count(
                self.background_square_error_sum,
                self.observation_count,
            ),
            analysis_rmse: rmse_if_count(self.analysis_square_error_sum, self.observation_count),
            max_abs_background_error: self.max_abs_background_error,
            max_abs_analysis_error: self.max_abs_analysis_error,
        }
    }
}

#[derive(Debug, Clone)]
struct FinishedSourceSummary {
    sampled_observation_count: usize,
    variables: BTreeMap<String, SourceVariableStats>,
}

#[derive(Debug, Clone)]
struct SourceVariableStats {
    observation_count: usize,
    mean_abs_background_error: Option<f64>,
    mean_abs_analysis_error: Option<f64>,
    background_rmse: Option<f64>,
    analysis_rmse: Option<f64>,
    confidence: Option<SurfaceMesoanalysisCalibrationConfidenceCase>,
}

impl SourceSummaryAccumulator {
    fn push_source_summary(&mut self, summary: &Value) {
        self.sampled_observation_count +=
            usize_at(summary, &["sampled_observation_count"]).unwrap_or(0);
        for variable in [
            "temperature_c",
            "dewpoint_c",
            "wind_speed_ms",
            "mean_sea_level_pressure_hpa",
        ] {
            if let Some(value) = value_at(summary, &[variable]) {
                self.variables
                    .entry(variable.to_string())
                    .or_default()
                    .push_variable_summary(value);
            }
        }
    }

    fn finish(&self) -> FinishedSourceSummary {
        FinishedSourceSummary {
            sampled_observation_count: self.sampled_observation_count,
            variables: self
                .variables
                .iter()
                .map(|(name, accumulator)| (name.clone(), accumulator.finish()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SourceVariableStatsAccumulator {
    observation_count: usize,
    background_mae_weighted_sum: f64,
    background_mae_weight: usize,
    analysis_mae_weighted_sum: f64,
    analysis_mae_weight: usize,
    background_rmse_square_weighted_sum: f64,
    background_rmse_weight: usize,
    analysis_rmse_square_weighted_sum: f64,
    analysis_rmse_weight: usize,
    confidence: ConfidenceCaseAccumulator,
}

impl SourceVariableStatsAccumulator {
    fn push_variable_summary(&mut self, summary: &Value) {
        let observation_count = usize_at(summary, &["observation_count"]).unwrap_or(0);
        if observation_count == 0 {
            return;
        }
        self.observation_count += observation_count;
        self.push_mean_abs_background(summary, observation_count);
        self.push_mean_abs_analysis(summary, observation_count);
        self.push_background_rmse(summary, observation_count);
        self.push_analysis_rmse(summary, observation_count);
        if let Some(confidence) = value_at(summary, &["confidence"]) {
            self.confidence.push_confidence_summary(confidence);
        }
    }

    fn push_mean_abs_background(&mut self, summary: &Value, observation_count: usize) {
        if let Some(value) = f64_at(summary, &["mean_abs_background_error"]) {
            self.background_mae_weighted_sum += value * observation_count as f64;
            self.background_mae_weight += observation_count;
        }
    }

    fn push_mean_abs_analysis(&mut self, summary: &Value, observation_count: usize) {
        if let Some(value) = f64_at(summary, &["mean_abs_analysis_error"]) {
            self.analysis_mae_weighted_sum += value * observation_count as f64;
            self.analysis_mae_weight += observation_count;
        }
    }

    fn push_background_rmse(&mut self, summary: &Value, observation_count: usize) {
        if let Some(value) = f64_at(summary, &["background_rmse"]) {
            self.background_rmse_square_weighted_sum += value * value * observation_count as f64;
            self.background_rmse_weight += observation_count;
        }
    }

    fn push_analysis_rmse(&mut self, summary: &Value, observation_count: usize) {
        if let Some(value) = f64_at(summary, &["analysis_rmse"]) {
            self.analysis_rmse_square_weighted_sum += value * value * observation_count as f64;
            self.analysis_rmse_weight += observation_count;
        }
    }

    fn finish(&self) -> SourceVariableStats {
        SourceVariableStats {
            observation_count: self.observation_count,
            mean_abs_background_error: weighted_mean(
                self.background_mae_weighted_sum,
                self.background_mae_weight,
            ),
            mean_abs_analysis_error: weighted_mean(
                self.analysis_mae_weighted_sum,
                self.analysis_mae_weight,
            ),
            background_rmse: weighted_rmse(
                self.background_rmse_square_weighted_sum,
                self.background_rmse_weight,
            ),
            analysis_rmse: weighted_rmse(
                self.analysis_rmse_square_weighted_sum,
                self.analysis_rmse_weight,
            ),
            confidence: self.confidence.finish(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ConfidenceCaseAccumulator {
    observation_counts: Vec<f64>,
    confidence_weighted_sum: f64,
    confidence_weight: usize,
    low_counts: Vec<f64>,
    low_mae_weighted_sum: f64,
    low_mae_weight: usize,
    medium_counts: Vec<f64>,
    medium_mae_weighted_sum: f64,
    medium_mae_weight: usize,
    high_counts: Vec<f64>,
    high_mae_weighted_sum: f64,
    high_mae_weight: usize,
    correlation_weighted_sum: f64,
    correlation_weight: usize,
    ranked_low_counts: Vec<f64>,
    ranked_low_mae_weighted_sum: f64,
    ranked_low_mae_weight: usize,
    ranked_high_counts: Vec<f64>,
    ranked_high_mae_weighted_sum: f64,
    ranked_high_mae_weight: usize,
}

impl ConfidenceCaseAccumulator {
    fn push_confidence_summary(&mut self, summary: &Value) {
        let Some(confidence) = parse_confidence_case(summary) else {
            return;
        };
        self.push(&confidence);
    }

    fn push(&mut self, confidence: &SurfaceMesoanalysisCalibrationConfidenceCase) {
        push_usize_as_f64(&mut self.observation_counts, confidence.observation_count);
        if let (Some(mean_confidence), Some(observation_count)) =
            (confidence.mean_confidence, confidence.observation_count)
        {
            self.confidence_weighted_sum += mean_confidence * observation_count as f64;
            self.confidence_weight += observation_count;
        }
        self.push_bucket(
            confidence.low_confidence_observation_count,
            confidence.low_confidence_mean_abs_analysis_error,
            ConfidenceBucketKind::Low,
        );
        self.push_bucket(
            confidence.medium_confidence_observation_count,
            confidence.medium_confidence_mean_abs_analysis_error,
            ConfidenceBucketKind::Medium,
        );
        self.push_bucket(
            confidence.high_confidence_observation_count,
            confidence.high_confidence_mean_abs_analysis_error,
            ConfidenceBucketKind::High,
        );
        if let Some(correlation) = confidence.confidence_abs_error_correlation {
            let weight = confidence.observation_count.unwrap_or(1).max(1);
            self.correlation_weighted_sum += correlation * weight as f64;
            self.correlation_weight += weight;
        }
        self.push_ranked_bucket(
            confidence.ranked_low_confidence_observation_count,
            confidence.ranked_low_confidence_mean_abs_analysis_error,
            RankedConfidenceBucketKind::Low,
        );
        self.push_ranked_bucket(
            confidence.ranked_high_confidence_observation_count,
            confidence.ranked_high_confidence_mean_abs_analysis_error,
            RankedConfidenceBucketKind::High,
        );
    }

    fn push_bucket(
        &mut self,
        count: Option<usize>,
        mean_abs_analysis_error: Option<f64>,
        bucket: ConfidenceBucketKind,
    ) {
        if let Some(count) = count {
            match bucket {
                ConfidenceBucketKind::Low => self.low_counts.push(count as f64),
                ConfidenceBucketKind::Medium => self.medium_counts.push(count as f64),
                ConfidenceBucketKind::High => self.high_counts.push(count as f64),
            }
            if let Some(mean_abs_analysis_error) = mean_abs_analysis_error {
                match bucket {
                    ConfidenceBucketKind::Low => {
                        self.low_mae_weighted_sum += mean_abs_analysis_error * count as f64;
                        self.low_mae_weight += count;
                    }
                    ConfidenceBucketKind::Medium => {
                        self.medium_mae_weighted_sum += mean_abs_analysis_error * count as f64;
                        self.medium_mae_weight += count;
                    }
                    ConfidenceBucketKind::High => {
                        self.high_mae_weighted_sum += mean_abs_analysis_error * count as f64;
                        self.high_mae_weight += count;
                    }
                }
            }
        }
    }

    fn push_ranked_bucket(
        &mut self,
        count: Option<usize>,
        mean_abs_analysis_error: Option<f64>,
        bucket: RankedConfidenceBucketKind,
    ) {
        if let Some(count) = count {
            match bucket {
                RankedConfidenceBucketKind::Low => self.ranked_low_counts.push(count as f64),
                RankedConfidenceBucketKind::High => self.ranked_high_counts.push(count as f64),
            }
            if let Some(mean_abs_analysis_error) = mean_abs_analysis_error {
                match bucket {
                    RankedConfidenceBucketKind::Low => {
                        self.ranked_low_mae_weighted_sum += mean_abs_analysis_error * count as f64;
                        self.ranked_low_mae_weight += count;
                    }
                    RankedConfidenceBucketKind::High => {
                        self.ranked_high_mae_weighted_sum += mean_abs_analysis_error * count as f64;
                        self.ranked_high_mae_weight += count;
                    }
                }
            }
        }
    }

    fn finish(&self) -> Option<SurfaceMesoanalysisCalibrationConfidenceCase> {
        if self.observation_counts.is_empty() {
            return None;
        }
        let observation_count = Some(self.observation_counts.iter().sum::<f64>() as usize);
        let low_mean = weighted_mean(self.low_mae_weighted_sum, self.low_mae_weight);
        let medium_mean = weighted_mean(self.medium_mae_weighted_sum, self.medium_mae_weight);
        let high_mean = weighted_mean(self.high_mae_weighted_sum, self.high_mae_weight);
        let ranked_low_mean =
            weighted_mean(self.ranked_low_mae_weighted_sum, self.ranked_low_mae_weight);
        let ranked_high_mean = weighted_mean(
            self.ranked_high_mae_weighted_sum,
            self.ranked_high_mae_weight,
        );
        let ranked_low_count = Some(self.ranked_low_counts.iter().sum::<f64>() as usize);
        let ranked_high_count = Some(self.ranked_high_counts.iter().sum::<f64>() as usize);
        let ranked_high_minus_low_mean = option_delta(ranked_high_mean, ranked_low_mean);
        let reliability = confidence_reliability_case_from_ranked_buckets(
            ranked_low_count,
            ranked_high_count,
            ranked_high_minus_low_mean,
        );
        Some(SurfaceMesoanalysisCalibrationConfidenceCase {
            observation_count,
            mean_confidence: weighted_mean(self.confidence_weighted_sum, self.confidence_weight),
            low_confidence_observation_count: Some(self.low_counts.iter().sum::<f64>() as usize),
            low_confidence_mean_abs_analysis_error: low_mean,
            medium_confidence_observation_count: Some(
                self.medium_counts.iter().sum::<f64>() as usize
            ),
            medium_confidence_mean_abs_analysis_error: medium_mean,
            high_confidence_observation_count: Some(self.high_counts.iter().sum::<f64>() as usize),
            high_confidence_mean_abs_analysis_error: high_mean,
            high_minus_low_mean_abs_analysis_error: option_delta(high_mean, low_mean),
            confidence_abs_error_correlation: weighted_mean(
                self.correlation_weighted_sum,
                self.correlation_weight,
            ),
            ranked_low_confidence_observation_count: ranked_low_count,
            ranked_low_confidence_mean_abs_analysis_error: ranked_low_mean,
            ranked_high_confidence_observation_count: ranked_high_count,
            ranked_high_confidence_mean_abs_analysis_error: ranked_high_mean,
            ranked_high_minus_low_mean_abs_analysis_error: ranked_high_minus_low_mean,
            reliability,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum ConfidenceBucketKind {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy)]
enum RankedConfidenceBucketKind {
    Low,
    High,
}

fn parse_confidence_case(value: &Value) -> Option<SurfaceMesoanalysisCalibrationConfidenceCase> {
    let low_mean = f64_at(value, &["low_confidence_mean_abs_analysis_error"]);
    let medium_mean = f64_at(value, &["medium_confidence_mean_abs_analysis_error"])
        .or_else(|| f64_at(value, &["mid_confidence_mean_abs_analysis_error"]));
    let high_mean = f64_at(value, &["high_confidence_mean_abs_analysis_error"]);
    let ranked_low_count = usize_at(value, &["ranked_low_confidence_observation_count"]);
    let ranked_high_count = usize_at(value, &["ranked_high_confidence_observation_count"]);
    let ranked_high_minus_low = f64_at(value, &["ranked_high_minus_low_mean_abs_analysis_error"]);
    let confidence = SurfaceMesoanalysisCalibrationConfidenceCase {
        observation_count: usize_at(value, &["observation_count"]),
        mean_confidence: f64_at(value, &["mean_confidence"]),
        low_confidence_observation_count: usize_at(value, &["low_confidence_observation_count"]),
        low_confidence_mean_abs_analysis_error: low_mean,
        medium_confidence_observation_count: usize_at(
            value,
            &["medium_confidence_observation_count"],
        )
        .or_else(|| usize_at(value, &["mid_confidence_observation_count"])),
        medium_confidence_mean_abs_analysis_error: medium_mean,
        high_confidence_observation_count: usize_at(value, &["high_confidence_observation_count"]),
        high_confidence_mean_abs_analysis_error: high_mean,
        high_minus_low_mean_abs_analysis_error: f64_at(
            value,
            &["high_minus_low_mean_abs_analysis_error"],
        )
        .or_else(|| option_delta(high_mean, low_mean)),
        confidence_abs_error_correlation: f64_at(value, &["confidence_abs_error_correlation"]),
        ranked_low_confidence_observation_count: ranked_low_count,
        ranked_low_confidence_mean_abs_analysis_error: f64_at(
            value,
            &["ranked_low_confidence_mean_abs_analysis_error"],
        ),
        ranked_high_confidence_observation_count: ranked_high_count,
        ranked_high_confidence_mean_abs_analysis_error: f64_at(
            value,
            &["ranked_high_confidence_mean_abs_analysis_error"],
        ),
        ranked_high_minus_low_mean_abs_analysis_error: ranked_high_minus_low,
        reliability: parse_or_build_confidence_reliability_case(
            value,
            ranked_low_count,
            ranked_high_count,
            ranked_high_minus_low,
        ),
    };
    if confidence.observation_count.is_some()
        || confidence.mean_confidence.is_some()
        || confidence.low_confidence_observation_count.is_some()
        || confidence.low_confidence_mean_abs_analysis_error.is_some()
        || confidence.medium_confidence_observation_count.is_some()
        || confidence
            .medium_confidence_mean_abs_analysis_error
            .is_some()
        || confidence.high_confidence_observation_count.is_some()
        || confidence.high_confidence_mean_abs_analysis_error.is_some()
        || confidence.high_minus_low_mean_abs_analysis_error.is_some()
        || confidence.confidence_abs_error_correlation.is_some()
        || confidence.ranked_low_confidence_observation_count.is_some()
        || confidence
            .ranked_low_confidence_mean_abs_analysis_error
            .is_some()
        || confidence
            .ranked_high_confidence_observation_count
            .is_some()
        || confidence
            .ranked_high_confidence_mean_abs_analysis_error
            .is_some()
        || confidence
            .ranked_high_minus_low_mean_abs_analysis_error
            .is_some()
    {
        Some(confidence)
    } else {
        None
    }
}

fn parse_or_build_confidence_reliability_case(
    value: &Value,
    fallback_ranked_low_count: Option<usize>,
    fallback_ranked_high_count: Option<usize>,
    fallback_ranked_high_minus_low_mae: Option<f64>,
) -> SurfaceMesoanalysisCalibrationConfidenceReliabilityCase {
    let reliability = value_at(value, &["reliability"]);
    let ranked_low_count = reliability
        .and_then(|value| usize_at(value, &["ranked_low_confidence_observation_count"]))
        .or(fallback_ranked_low_count);
    let ranked_high_count = reliability
        .and_then(|value| usize_at(value, &["ranked_high_confidence_observation_count"]))
        .or(fallback_ranked_high_count);
    let ranked_high_minus_low_mae = reliability
        .and_then(|value| f64_at(value, &["ranked_high_minus_low_mean_abs_analysis_error"]))
        .or(fallback_ranked_high_minus_low_mae);
    let fallback = confidence_reliability_case_from_ranked_buckets(
        ranked_low_count,
        ranked_high_count,
        ranked_high_minus_low_mae,
    );
    let Some(reliability) = reliability else {
        return fallback;
    };
    SurfaceMesoanalysisCalibrationConfidenceReliabilityCase {
        schema: string_at(reliability, &["schema"]).unwrap_or(fallback.schema),
        semantic_label: string_at(reliability, &["semantic_label"])
            .unwrap_or(fallback.semantic_label),
        status: string_at(reliability, &["status"]).unwrap_or(fallback.status),
        bucket_coverage_sufficient: bool_at(reliability, &["bucket_coverage_sufficient"])
            .unwrap_or(fallback.bucket_coverage_sufficient),
        ranked_low_confidence_observation_count: ranked_low_count,
        ranked_high_confidence_observation_count: ranked_high_count,
        min_ranked_bucket_observation_count: usize_at(
            reliability,
            &["min_ranked_bucket_observation_count"],
        )
        .unwrap_or(fallback.min_ranked_bucket_observation_count),
        ranked_high_minus_low_mean_abs_analysis_error: ranked_high_minus_low_mae,
        max_ranked_high_minus_low_mean_abs_analysis_error: f64_at(
            reliability,
            &["max_ranked_high_minus_low_mean_abs_analysis_error"],
        )
        .unwrap_or(fallback.max_ranked_high_minus_low_mean_abs_analysis_error),
        message: string_at(reliability, &["message"]).unwrap_or(fallback.message),
    }
}

fn confidence_reliability_case_from_ranked_buckets(
    ranked_low_count: Option<usize>,
    ranked_high_count: Option<usize>,
    ranked_high_minus_low_mae: Option<f64>,
) -> SurfaceMesoanalysisCalibrationConfidenceReliabilityCase {
    let bucket_coverage_sufficient = ranked_low_count
        .map(|count| count >= CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS)
        .unwrap_or(false)
        && ranked_high_count
            .map(|count| count >= CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS)
            .unwrap_or(false)
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
    SurfaceMesoanalysisCalibrationConfidenceReliabilityCase {
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

#[derive(Debug, Default)]
struct SourceAggregateAccumulator {
    case_count: usize,
    sampled_observation_counts: Vec<f64>,
    variables: BTreeMap<String, VariableAggregateAccumulator>,
}

impl SourceAggregateAccumulator {
    fn push(&mut self, source: &SurfaceMesoanalysisCalibrationSourceCase) {
        self.case_count += 1;
        self.sampled_observation_counts
            .push(source.sampled_observation_count as f64);
        for (name, variable) in &source.variables {
            self.variables
                .entry(name.clone())
                .or_default()
                .push(variable);
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationSourceAggregate {
        SurfaceMesoanalysisCalibrationSourceAggregate {
            case_count: self.case_count,
            mean_sampled_observation_count: mean(&self.sampled_observation_counts),
            variables: self
                .variables
                .into_iter()
                .map(|(name, accumulator)| (name, accumulator.finish()))
                .collect(),
        }
    }
}

#[derive(Debug, Default)]
struct StratumAggregateAccumulator {
    stratum_type: String,
    stratum_value: String,
    case_count: usize,
    sampled_observation_counts: Vec<f64>,
    variables: BTreeMap<String, VariableAggregateAccumulator>,
}

impl StratumAggregateAccumulator {
    fn new(stratum_type: String, stratum_value: String) -> Self {
        Self {
            stratum_type,
            stratum_value,
            ..Self::default()
        }
    }

    fn push(&mut self, stratum: &SurfaceMesoanalysisCalibrationStratumCase) {
        self.case_count += 1;
        self.sampled_observation_counts
            .push(stratum.sampled_observation_count as f64);
        for (name, variable) in &stratum.variables {
            self.variables
                .entry(name.clone())
                .or_default()
                .push(variable);
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationStratumAggregate {
        SurfaceMesoanalysisCalibrationStratumAggregate {
            stratum_type: self.stratum_type,
            stratum_value: self.stratum_value,
            case_count: self.case_count,
            mean_sampled_observation_count: mean(&self.sampled_observation_counts),
            variables: self
                .variables
                .into_iter()
                .map(|(name, accumulator)| (name, accumulator.finish()))
                .collect(),
        }
    }
}

#[derive(Debug, Default)]
struct ReferenceAggregateAccumulator {
    case_count: usize,
    validation_mode_counts: BTreeMap<String, usize>,
    variables: BTreeMap<String, ReferenceVariableAggregateAccumulator>,
}

impl ReferenceAggregateAccumulator {
    fn push(&mut self, reference: &SurfaceMesoanalysisCalibrationReferenceCase) {
        self.case_count += 1;
        *self
            .validation_mode_counts
            .entry(reference.validation_mode.clone())
            .or_insert(0usize) += 1;
        for (name, variable) in &reference.variables {
            self.variables
                .entry(name.clone())
                .or_default()
                .push(variable);
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationReferenceAggregate {
        SurfaceMesoanalysisCalibrationReferenceAggregate {
            case_count: self.case_count,
            validation_mode_counts: self.validation_mode_counts,
            variables: self
                .variables
                .into_iter()
                .map(|(name, accumulator)| (name, accumulator.finish()))
                .collect(),
        }
    }
}

#[derive(Debug, Default)]
struct ReferenceVariableAggregateAccumulator {
    case_count: usize,
    observation_counts: Vec<f64>,
    candidate_mae: Vec<f64>,
    reference_mae: Vec<f64>,
    candidate_minus_reference_mae: Vec<f64>,
    candidate_rmse: Vec<f64>,
    reference_rmse: Vec<f64>,
    candidate_minus_reference_rmse: Vec<f64>,
    candidate_beats_reference_mae_case_count: usize,
    candidate_beats_reference_rmse_case_count: usize,
    candidate_loses_reference_mae_case_count: usize,
}

impl ReferenceVariableAggregateAccumulator {
    fn push(&mut self, variable: &SurfaceMesoanalysisCalibrationReferenceVariableCase) {
        self.case_count += 1;
        push_usize_as_f64(&mut self.observation_counts, variable.observation_count);
        push_f64(&mut self.candidate_mae, variable.candidate_mean_abs_error);
        push_f64(&mut self.reference_mae, variable.reference_mean_abs_error);
        push_f64(
            &mut self.candidate_minus_reference_mae,
            variable.candidate_minus_reference_mae,
        );
        push_f64(&mut self.candidate_rmse, variable.candidate_rmse);
        push_f64(&mut self.reference_rmse, variable.reference_rmse);
        push_f64(
            &mut self.candidate_minus_reference_rmse,
            variable.candidate_minus_reference_rmse,
        );
        if option_less_than_zero(variable.candidate_minus_reference_mae) {
            self.candidate_beats_reference_mae_case_count += 1;
        }
        if option_less_than_zero(variable.candidate_minus_reference_rmse) {
            self.candidate_beats_reference_rmse_case_count += 1;
        }
        if option_greater_than_zero(variable.candidate_minus_reference_mae) {
            self.candidate_loses_reference_mae_case_count += 1;
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationReferenceVariableAggregate {
        SurfaceMesoanalysisCalibrationReferenceVariableAggregate {
            case_count: self.case_count,
            mean_observation_count: mean(&self.observation_counts),
            mean_candidate_mae: mean(&self.candidate_mae),
            mean_reference_mae: mean(&self.reference_mae),
            mean_candidate_minus_reference_mae: mean(&self.candidate_minus_reference_mae),
            mean_candidate_rmse: mean(&self.candidate_rmse),
            mean_reference_rmse: mean(&self.reference_rmse),
            mean_candidate_minus_reference_rmse: mean(&self.candidate_minus_reference_rmse),
            candidate_beats_reference_mae_case_count: self.candidate_beats_reference_mae_case_count,
            candidate_beats_reference_rmse_case_count: self
                .candidate_beats_reference_rmse_case_count,
            candidate_loses_reference_mae_case_count: self.candidate_loses_reference_mae_case_count,
            worst_candidate_minus_reference_mae: max(&self.candidate_minus_reference_mae),
        }
    }
}

#[derive(Debug, Default)]
struct AblationAggregateAccumulator {
    case_count: usize,
    validation_mode_counts: BTreeMap<String, usize>,
    baseline_compute_ms: Vec<f64>,
    variables: BTreeMap<String, AblationVariableAggregateAccumulator>,
}

impl AblationAggregateAccumulator {
    fn push(&mut self, ablation: &SurfaceMesoanalysisCalibrationAblationCase) {
        self.case_count += 1;
        *self
            .validation_mode_counts
            .entry(ablation.validation_mode.clone())
            .or_insert(0usize) += 1;
        push_u128_as_f64(&mut self.baseline_compute_ms, ablation.baseline_compute_ms);
        for (name, variable) in &ablation.variables {
            self.variables
                .entry(name.clone())
                .or_default()
                .push(variable);
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationAblationAggregate {
        SurfaceMesoanalysisCalibrationAblationAggregate {
            case_count: self.case_count,
            validation_mode_counts: self.validation_mode_counts,
            mean_baseline_compute_ms: mean(&self.baseline_compute_ms),
            variables: self
                .variables
                .into_iter()
                .map(|(name, accumulator)| (name, accumulator.finish()))
                .collect(),
        }
    }
}

#[derive(Debug, Default)]
struct AblationVariableAggregateAccumulator {
    case_count: usize,
    candidate_observation_counts: Vec<f64>,
    baseline_observation_counts: Vec<f64>,
    candidate_mae: Vec<f64>,
    baseline_mae: Vec<f64>,
    candidate_minus_baseline_mae: Vec<f64>,
    candidate_rmse: Vec<f64>,
    baseline_rmse: Vec<f64>,
    candidate_minus_baseline_rmse: Vec<f64>,
    candidate_beats_baseline_mae_case_count: usize,
    candidate_beats_baseline_rmse_case_count: usize,
    candidate_loses_baseline_mae_case_count: usize,
}

impl AblationVariableAggregateAccumulator {
    fn push(&mut self, variable: &SurfaceMesoanalysisCalibrationAblationVariableCase) {
        self.case_count += 1;
        push_usize_as_f64(
            &mut self.candidate_observation_counts,
            variable.candidate_observation_count,
        );
        push_usize_as_f64(
            &mut self.baseline_observation_counts,
            variable.baseline_observation_count,
        );
        push_f64(&mut self.candidate_mae, variable.candidate_mean_abs_error);
        push_f64(&mut self.baseline_mae, variable.baseline_mean_abs_error);
        push_f64(
            &mut self.candidate_minus_baseline_mae,
            variable.candidate_minus_baseline_mae,
        );
        push_f64(&mut self.candidate_rmse, variable.candidate_rmse);
        push_f64(&mut self.baseline_rmse, variable.baseline_rmse);
        push_f64(
            &mut self.candidate_minus_baseline_rmse,
            variable.candidate_minus_baseline_rmse,
        );
        if option_less_than_zero(variable.candidate_minus_baseline_mae) {
            self.candidate_beats_baseline_mae_case_count += 1;
        }
        if option_less_than_zero(variable.candidate_minus_baseline_rmse) {
            self.candidate_beats_baseline_rmse_case_count += 1;
        }
        if option_greater_than_zero(variable.candidate_minus_baseline_mae) {
            self.candidate_loses_baseline_mae_case_count += 1;
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationAblationVariableAggregate {
        SurfaceMesoanalysisCalibrationAblationVariableAggregate {
            case_count: self.case_count,
            mean_candidate_observation_count: mean(&self.candidate_observation_counts),
            mean_baseline_observation_count: mean(&self.baseline_observation_counts),
            mean_candidate_mae: mean(&self.candidate_mae),
            mean_baseline_mae: mean(&self.baseline_mae),
            mean_candidate_minus_baseline_mae: mean(&self.candidate_minus_baseline_mae),
            mean_candidate_rmse: mean(&self.candidate_rmse),
            mean_baseline_rmse: mean(&self.baseline_rmse),
            mean_candidate_minus_baseline_rmse: mean(&self.candidate_minus_baseline_rmse),
            candidate_beats_baseline_mae_case_count: self.candidate_beats_baseline_mae_case_count,
            candidate_beats_baseline_rmse_case_count: self.candidate_beats_baseline_rmse_case_count,
            candidate_loses_baseline_mae_case_count: self.candidate_loses_baseline_mae_case_count,
            worst_candidate_minus_baseline_mae: max(&self.candidate_minus_baseline_mae),
        }
    }
}

#[derive(Debug, Default)]
struct VariableAggregateAccumulator {
    case_count: usize,
    observation_counts: Vec<f64>,
    fold_counts: Vec<f64>,
    background_mae: Vec<f64>,
    candidate_mae: Vec<f64>,
    barnes_mae: Vec<f64>,
    candidate_minus_background_mae: Vec<f64>,
    candidate_minus_barnes_mae: Vec<f64>,
    background_rmse: Vec<f64>,
    candidate_rmse: Vec<f64>,
    barnes_rmse: Vec<f64>,
    candidate_minus_background_rmse: Vec<f64>,
    candidate_minus_barnes_rmse: Vec<f64>,
    candidate_beats_background_mae_case_count: usize,
    candidate_beats_barnes_mae_case_count: usize,
    candidate_beats_background_rmse_case_count: usize,
    candidate_beats_barnes_rmse_case_count: usize,
    candidate_loses_background_mae_case_count: usize,
    candidate_loses_barnes_mae_case_count: usize,
    confidence: ConfidenceAggregateAccumulator,
}

impl VariableAggregateAccumulator {
    fn push(&mut self, variable: &SurfaceMesoanalysisCalibrationVariableCase) {
        self.case_count += 1;
        push_usize_as_f64(&mut self.observation_counts, variable.observation_count);
        push_usize_as_f64(&mut self.fold_counts, variable.fold_count);
        push_f64(&mut self.background_mae, variable.background_mean_abs_error);
        push_f64(&mut self.candidate_mae, variable.candidate_mean_abs_error);
        push_f64(&mut self.barnes_mae, variable.barnes_mean_abs_error);
        push_f64(
            &mut self.candidate_minus_background_mae,
            variable.candidate_minus_background_mae,
        );
        push_f64(
            &mut self.candidate_minus_barnes_mae,
            variable.candidate_minus_barnes_mae,
        );
        push_f64(&mut self.background_rmse, variable.background_rmse);
        push_f64(&mut self.candidate_rmse, variable.candidate_rmse);
        push_f64(&mut self.barnes_rmse, variable.barnes_rmse);
        push_f64(
            &mut self.candidate_minus_background_rmse,
            variable.candidate_minus_background_rmse,
        );
        push_f64(
            &mut self.candidate_minus_barnes_rmse,
            variable.candidate_minus_barnes_rmse,
        );
        if option_less_than_zero(variable.candidate_minus_background_mae) {
            self.candidate_beats_background_mae_case_count += 1;
        }
        if option_less_than_zero(variable.candidate_minus_barnes_mae) {
            self.candidate_beats_barnes_mae_case_count += 1;
        }
        if option_less_than_zero(variable.candidate_minus_background_rmse) {
            self.candidate_beats_background_rmse_case_count += 1;
        }
        if option_less_than_zero(variable.candidate_minus_barnes_rmse) {
            self.candidate_beats_barnes_rmse_case_count += 1;
        }
        if option_greater_than_zero(variable.candidate_minus_background_mae) {
            self.candidate_loses_background_mae_case_count += 1;
        }
        if option_greater_than_zero(variable.candidate_minus_barnes_mae) {
            self.candidate_loses_barnes_mae_case_count += 1;
        }
        if let Some(confidence) = variable.confidence.as_ref() {
            self.confidence.push(confidence);
        }
    }

    fn finish(self) -> SurfaceMesoanalysisCalibrationVariableAggregate {
        SurfaceMesoanalysisCalibrationVariableAggregate {
            case_count: self.case_count,
            mean_observation_count: mean(&self.observation_counts),
            mean_fold_count: mean(&self.fold_counts),
            mean_background_mae: mean(&self.background_mae),
            mean_candidate_mae: mean(&self.candidate_mae),
            mean_barnes_mae: mean(&self.barnes_mae),
            mean_candidate_minus_background_mae: mean(&self.candidate_minus_background_mae),
            mean_candidate_minus_barnes_mae: mean(&self.candidate_minus_barnes_mae),
            mean_background_rmse: mean(&self.background_rmse),
            mean_candidate_rmse: mean(&self.candidate_rmse),
            mean_barnes_rmse: mean(&self.barnes_rmse),
            mean_candidate_minus_background_rmse: mean(&self.candidate_minus_background_rmse),
            mean_candidate_minus_barnes_rmse: mean(&self.candidate_minus_barnes_rmse),
            candidate_beats_background_mae_case_count: self
                .candidate_beats_background_mae_case_count,
            candidate_beats_barnes_mae_case_count: self.candidate_beats_barnes_mae_case_count,
            candidate_beats_background_rmse_case_count: self
                .candidate_beats_background_rmse_case_count,
            candidate_beats_barnes_rmse_case_count: self.candidate_beats_barnes_rmse_case_count,
            candidate_loses_background_mae_case_count: self
                .candidate_loses_background_mae_case_count,
            candidate_loses_barnes_mae_case_count: self.candidate_loses_barnes_mae_case_count,
            worst_candidate_minus_background_mae: max(&self.candidate_minus_background_mae),
            worst_candidate_minus_barnes_mae: max(&self.candidate_minus_barnes_mae),
            confidence: self.confidence.finish(),
        }
    }
}

#[derive(Debug, Default)]
struct ConfidenceAggregateAccumulator {
    case_count: usize,
    observation_counts: Vec<f64>,
    mean_confidences: Vec<f64>,
    low_counts: Vec<f64>,
    low_mae: Vec<f64>,
    medium_counts: Vec<f64>,
    medium_mae: Vec<f64>,
    high_counts: Vec<f64>,
    high_mae: Vec<f64>,
    high_minus_low_mae: Vec<f64>,
    confidence_abs_error_correlations: Vec<f64>,
    ranked_low_counts: Vec<f64>,
    ranked_low_mae: Vec<f64>,
    ranked_high_counts: Vec<f64>,
    ranked_high_mae: Vec<f64>,
    ranked_high_minus_low_mae: Vec<f64>,
    high_confidence_beats_low_confidence_mae_case_count: usize,
    high_confidence_loses_low_confidence_mae_case_count: usize,
    ranked_high_confidence_beats_low_confidence_mae_case_count: usize,
    ranked_high_confidence_loses_low_confidence_mae_case_count: usize,
    negative_confidence_abs_error_correlation_case_count: usize,
    positive_confidence_abs_error_correlation_case_count: usize,
    confidence_reliability_passed_case_count: usize,
    confidence_reliability_failed_case_count: usize,
    confidence_reliability_untestable_case_count: usize,
}

impl ConfidenceAggregateAccumulator {
    fn push(&mut self, confidence: &SurfaceMesoanalysisCalibrationConfidenceCase) {
        self.case_count += 1;
        push_usize_as_f64(&mut self.observation_counts, confidence.observation_count);
        push_f64(&mut self.mean_confidences, confidence.mean_confidence);
        push_usize_as_f64(
            &mut self.low_counts,
            confidence.low_confidence_observation_count,
        );
        push_f64(
            &mut self.low_mae,
            confidence.low_confidence_mean_abs_analysis_error,
        );
        push_usize_as_f64(
            &mut self.medium_counts,
            confidence.medium_confidence_observation_count,
        );
        push_f64(
            &mut self.medium_mae,
            confidence.medium_confidence_mean_abs_analysis_error,
        );
        push_usize_as_f64(
            &mut self.high_counts,
            confidence.high_confidence_observation_count,
        );
        push_f64(
            &mut self.high_mae,
            confidence.high_confidence_mean_abs_analysis_error,
        );
        push_f64(
            &mut self.high_minus_low_mae,
            confidence.high_minus_low_mean_abs_analysis_error,
        );
        push_f64(
            &mut self.confidence_abs_error_correlations,
            confidence.confidence_abs_error_correlation,
        );
        push_usize_as_f64(
            &mut self.ranked_low_counts,
            confidence.ranked_low_confidence_observation_count,
        );
        push_f64(
            &mut self.ranked_low_mae,
            confidence.ranked_low_confidence_mean_abs_analysis_error,
        );
        push_usize_as_f64(
            &mut self.ranked_high_counts,
            confidence.ranked_high_confidence_observation_count,
        );
        push_f64(
            &mut self.ranked_high_mae,
            confidence.ranked_high_confidence_mean_abs_analysis_error,
        );
        push_f64(
            &mut self.ranked_high_minus_low_mae,
            confidence.ranked_high_minus_low_mean_abs_analysis_error,
        );
        if option_less_than_zero(confidence.high_minus_low_mean_abs_analysis_error) {
            self.high_confidence_beats_low_confidence_mae_case_count += 1;
        }
        if option_greater_than_zero(confidence.high_minus_low_mean_abs_analysis_error) {
            self.high_confidence_loses_low_confidence_mae_case_count += 1;
        }
        if option_less_than_zero(confidence.ranked_high_minus_low_mean_abs_analysis_error) {
            self.ranked_high_confidence_beats_low_confidence_mae_case_count += 1;
        }
        if option_greater_than_zero(confidence.ranked_high_minus_low_mean_abs_analysis_error) {
            self.ranked_high_confidence_loses_low_confidence_mae_case_count += 1;
        }
        if option_less_than_zero(confidence.confidence_abs_error_correlation) {
            self.negative_confidence_abs_error_correlation_case_count += 1;
        }
        if option_greater_than_zero(confidence.confidence_abs_error_correlation) {
            self.positive_confidence_abs_error_correlation_case_count += 1;
        }
        match confidence.reliability.status.as_str() {
            "passed" => self.confidence_reliability_passed_case_count += 1,
            "failed" => self.confidence_reliability_failed_case_count += 1,
            _ => self.confidence_reliability_untestable_case_count += 1,
        }
    }

    fn finish(self) -> Option<SurfaceMesoanalysisCalibrationConfidenceAggregate> {
        if self.case_count == 0 {
            return None;
        }
        let reliability = confidence_reliability_aggregate_from_cases(
            self.case_count,
            self.confidence_reliability_passed_case_count,
            self.confidence_reliability_failed_case_count,
            self.confidence_reliability_untestable_case_count,
            min(&self.ranked_low_counts),
            min(&self.ranked_high_counts),
            max(&self.ranked_high_minus_low_mae),
        );
        Some(SurfaceMesoanalysisCalibrationConfidenceAggregate {
            case_count: self.case_count,
            mean_observation_count: mean(&self.observation_counts),
            mean_confidence: mean(&self.mean_confidences),
            mean_low_confidence_observation_count: mean(&self.low_counts),
            min_low_confidence_observation_count: min(&self.low_counts),
            mean_low_confidence_mae: mean(&self.low_mae),
            mean_medium_confidence_observation_count: mean(&self.medium_counts),
            min_medium_confidence_observation_count: min(&self.medium_counts),
            mean_medium_confidence_mae: mean(&self.medium_mae),
            mean_high_confidence_observation_count: mean(&self.high_counts),
            min_high_confidence_observation_count: min(&self.high_counts),
            mean_high_confidence_mae: mean(&self.high_mae),
            mean_high_minus_low_confidence_mae: mean(&self.high_minus_low_mae),
            worst_high_minus_low_confidence_mae: max(&self.high_minus_low_mae),
            mean_confidence_abs_error_correlation: mean(&self.confidence_abs_error_correlations),
            worst_confidence_abs_error_correlation: max(&self.confidence_abs_error_correlations),
            mean_ranked_low_confidence_observation_count: mean(&self.ranked_low_counts),
            mean_ranked_low_confidence_mae: mean(&self.ranked_low_mae),
            mean_ranked_high_confidence_observation_count: mean(&self.ranked_high_counts),
            mean_ranked_high_confidence_mae: mean(&self.ranked_high_mae),
            mean_ranked_high_minus_low_confidence_mae: mean(&self.ranked_high_minus_low_mae),
            worst_ranked_high_minus_low_confidence_mae: max(&self.ranked_high_minus_low_mae),
            high_confidence_beats_low_confidence_mae_case_count: self
                .high_confidence_beats_low_confidence_mae_case_count,
            high_confidence_loses_low_confidence_mae_case_count: self
                .high_confidence_loses_low_confidence_mae_case_count,
            ranked_high_confidence_beats_low_confidence_mae_case_count: self
                .ranked_high_confidence_beats_low_confidence_mae_case_count,
            ranked_high_confidence_loses_low_confidence_mae_case_count: self
                .ranked_high_confidence_loses_low_confidence_mae_case_count,
            negative_confidence_abs_error_correlation_case_count: self
                .negative_confidence_abs_error_correlation_case_count,
            positive_confidence_abs_error_correlation_case_count: self
                .positive_confidence_abs_error_correlation_case_count,
            reliability,
        })
    }
}

fn confidence_reliability_aggregate_from_cases(
    case_count: usize,
    passed_case_count: usize,
    failed_case_count: usize,
    untestable_case_count: usize,
    min_ranked_low_count: Option<f64>,
    min_ranked_high_count: Option<f64>,
    worst_ranked_high_minus_low_mae: Option<f64>,
) -> SurfaceMesoanalysisCalibrationConfidenceReliabilityAggregate {
    let bucket_coverage_sufficient =
        case_count > 0 && passed_case_count + failed_case_count == case_count;
    let (status, semantic_label, message) = if failed_case_count > 0 {
        (
            "failed",
            "uncalibrated_support",
            "one or more cases failed ranked confidence reliability",
        )
    } else if passed_case_count == case_count && case_count > 0 {
        (
            "passed",
            "calibrated_reliability",
            "all cases passed ranked confidence reliability",
        )
    } else {
        (
            "untestable",
            "support_index",
            "one or more cases lacked enough ranked confidence bucket coverage to test reliability",
        )
    };
    SurfaceMesoanalysisCalibrationConfidenceReliabilityAggregate {
        schema: "rustwx.surface_mesoanalysis.confidence_reliability_aggregate.v1".to_string(),
        semantic_label: semantic_label.to_string(),
        status: status.to_string(),
        bucket_coverage_sufficient,
        case_count,
        passed_case_count,
        failed_case_count,
        untestable_case_count,
        min_ranked_low_confidence_observation_count: min_ranked_low_count,
        min_ranked_high_confidence_observation_count: min_ranked_high_count,
        min_ranked_bucket_observation_count: CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
        worst_ranked_high_minus_low_mean_abs_analysis_error: worst_ranked_high_minus_low_mae,
        max_ranked_high_minus_low_mean_abs_analysis_error:
            CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
        message: message.to_string(),
    }
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    value_at(value, path)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn string_vec_at(value: &Value, path: &[&str]) -> Vec<String> {
    let Some(value) = value_at(value, path) else {
        return Vec::new();
    };
    if let Some(raw) = value.as_str() {
        return vec![raw.to_string()];
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn normalized_case_tags(raw_tags: &[String]) -> Vec<String> {
    let mut tags = raw_tags
        .iter()
        .map(|tag| tag.trim())
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    tags.sort();
    tags.dedup();
    tags
}

fn bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    value_at(value, path).and_then(Value::as_bool)
}

fn usize_at(value: &Value, path: &[&str]) -> Option<usize> {
    u64_at(value, path).and_then(|value| usize::try_from(value).ok())
}

fn u64_at(value: &Value, path: &[&str]) -> Option<u64> {
    value_at(value, path).and_then(Value::as_u64)
}

fn f64_at(value: &Value, path: &[&str]) -> Option<f64> {
    value_at(value, path)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
}

fn value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    if current.is_null() {
        None
    } else {
        Some(current)
    }
}

fn push_f64(values: &mut Vec<f64>, value: Option<f64>) {
    if let Some(value) = value.filter(|value| value.is_finite()) {
        values.push(value);
    }
}

fn push_usize_as_f64(values: &mut Vec<f64>, value: Option<usize>) {
    if let Some(value) = value {
        values.push(value as f64);
    }
}

fn push_u128_as_f64(values: &mut Vec<f64>, value: Option<u128>) {
    if let Some(value) = value {
        values.push(value as f64);
    }
}

fn increment_count(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_insert(0usize) += 1;
}

fn non_empty_or_missing(value: &str) -> String {
    if value.is_empty() {
        "<missing>".to_string()
    } else {
        value.to_string()
    }
}

fn cycle_key(cycle: Option<u8>) -> String {
    cycle
        .map(|cycle| format!("{cycle:02}"))
        .unwrap_or_else(|| "<missing>".to_string())
}

fn forecast_hour_key(forecast_hour: u16) -> String {
    format!("f{forecast_hour:03}")
}

fn case_signature(case: &SurfaceMesoanalysisCalibrationCase) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        non_empty_or_missing(&case.model),
        non_empty_or_missing(&case.model_source),
        non_empty_or_missing(&case.date),
        cycle_key(case.cycle),
        forecast_hour_key(case.forecast_hour)
    )
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn weighted_mean(weighted_sum: f64, weight: usize) -> Option<f64> {
    if weight == 0 {
        None
    } else {
        Some(weighted_sum / weight as f64)
    }
}

fn weighted_rmse(square_weighted_sum: f64, weight: usize) -> Option<f64> {
    weighted_mean(square_weighted_sum, weight).map(f64::sqrt)
}

fn mean_if_count(sum: f64, count: usize) -> Option<f64> {
    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}

fn rmse_if_count(square_sum: f64, count: usize) -> Option<f64> {
    mean_if_count(square_sum, count).map(f64::sqrt)
}

fn max(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::max)
}

fn min(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::min)
}

fn option_delta(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    candidate
        .zip(baseline)
        .map(|(candidate, baseline)| candidate - baseline)
}

fn option_less_than_zero(value: Option<f64>) -> bool {
    value.map(|value| value < 0.0).unwrap_or(false)
}

fn option_greater_than_zero(value: Option<f64>) -> bool {
    value.map(|value| value > 0.0).unwrap_or(false)
}

#[cfg(test)]
mod contract_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn empty_gate_thresholds() -> SurfaceMesoanalysisCalibrationGateThresholds {
        SurfaceMesoanalysisCalibrationGateThresholds {
            min_case_count: 0,
            allow_skipped_reports: true,
            allowed_quality_flags: Vec::new(),
            required_benchmark_modes: Vec::new(),
            required_holdout_strategies: Vec::new(),
            required_external_references: Vec::new(),
            required_covariance_ablations: Vec::new(),
            required_case_tags: Vec::new(),
            variables: Vec::new(),
            sources: Vec::new(),
            strata: Vec::new(),
            stations: Vec::new(),
            max_domain_candidate_minus_background_mae: None,
            max_domain_candidate_minus_barnes_mae: None,
            max_domain_candidate_minus_reference_mae: None,
            max_covariance_ablation_candidate_minus_baseline_mae: None,
            max_case_mesoanalysis_compute_ms: None,
            min_unique_case_signatures: None,
            min_unique_dates: None,
            min_unique_cycles: None,
            min_unique_forecast_hours: None,
            min_unique_case_tags: None,
            min_domain_low_confidence_observation_count: None,
            min_domain_high_confidence_observation_count: None,
            max_domain_high_minus_low_confidence_mae: None,
            max_domain_ranked_high_minus_low_confidence_mae: None,
            max_domain_confidence_abs_error_correlation: None,
            max_source_candidate_minus_background_mae: None,
            max_source_candidate_minus_barnes_mae: None,
            min_source_low_confidence_observation_count: None,
            min_source_high_confidence_observation_count: None,
            max_source_high_minus_low_confidence_mae: None,
            max_source_ranked_high_minus_low_confidence_mae: None,
            max_source_confidence_abs_error_correlation: None,
            max_stratum_candidate_minus_background_mae: None,
            max_stratum_candidate_minus_barnes_mae: None,
            min_stratum_low_confidence_observation_count: None,
            min_stratum_high_confidence_observation_count: None,
            max_stratum_high_minus_low_confidence_mae: None,
            max_stratum_ranked_high_minus_low_confidence_mae: None,
            max_stratum_confidence_abs_error_correlation: None,
            min_station_observation_count: None,
            max_station_candidate_minus_background_mae: None,
            max_station_analysis_mae: None,
            max_station_abs_analysis_bias: None,
        }
    }

    #[test]
    fn calibration_report_prefers_repeated_holdout_benchmark() {
        let report = build_surface_mesoanalysis_calibration_report_from_values([(
            PathBuf::from("case/run_report.json"),
            Ok(json!({
                "schema": "rustwx.surface_mesoanalysis.run_report.v1",
                "model": "hrrr",
                "model_source": "nomads",
                "model_cycle": "2026051300z",
                "date": "20260513",
                "cycle": 0,
                "forecast_hour": 1,
                "model_load_mode": "surface_only",
                "case_tags": ["regime=dryline", "hazard=severe", "regime=dryline"],
                "grid_export_field_count": 15,
                "mesoanalysis_compute_ms": 1337,
                "mesoanalysis_config": {
                    "method": "optimal_interpolation",
                    "oi_covariance_kernel": "exponential"
                },
                "validation_gate": {
                    "passed": true
                },
                "mesoanalysis": {
                    "observation_count": 2933,
                    "source_count": 5,
                    "diagnostics": [
                        {
                            "variable": "temperature_2m_c",
                            "candidate_observations": 40,
                            "accepted_observations": 38,
                            "rejected_observations": 2,
                            "gross_error_rescued_observations": 3,
                            "covered_grid_cells": 1234,
                            "solver_failed_grid_cells": 1,
                            "truncated_neighbor_grid_cells": 4
                        }
                    ],
                    "repeated_holdout_validation": {
                        "strategy": "spatial_block",
                        "completed_fold_count": 2,
                        "folds": [
                            {
                                "validation": {
                                    "source_summaries": [
                                        {
                                            "source": "metar",
                                            "sampled_observation_count": 10,
                                            "temperature_c": {
                                                "observation_count": 10,
                                                "mean_abs_background_error": 1.0,
                                                "mean_abs_analysis_error": 0.8,
                                                "background_rmse": 1.2,
                                                "analysis_rmse": 1.0
                                            }
                                        }
                                    ],
                                    "samples": [
                                        {
                                            "station_id": "KOUN",
                                            "source": "metar",
                                            "temperature_c": {
                                                "background_error": 1.0,
                                                "analysis_error": 0.5,
                                                "abs_error_improvement": 0.5
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "validation": {
                                    "source_summaries": [
                                        {
                                            "source": "metar",
                                            "sampled_observation_count": 20,
                                            "temperature_c": {
                                                "observation_count": 20,
                                                "mean_abs_background_error": 2.0,
                                                "mean_abs_analysis_error": 1.4,
                                                "background_rmse": 2.2,
                                                "analysis_rmse": 1.6
                                            }
                                        }
                                    ],
                                    "samples": [
                                        {
                                            "station_id": "KOUN",
                                            "source": "metar",
                                            "temperature_c": {
                                                "background_error": -1.0,
                                                "analysis_error": -0.25,
                                                "abs_error_improvement": 0.75
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                "barnes_baseline_comparison": {
                    "holdout_benchmark_summary": {
                        "validation_mode": "holdout_validation",
                        "temperature_c": {
                            "candidate_minus_background_mae": 9.0
                        }
                    },
                    "repeated_holdout_benchmark_summary": {
                        "validation_mode": "repeated_holdout_validation",
                        "fold_count": 2,
                        "temperature_c": {
                            "fold_count": 2,
                            "background_mean_abs_error": 0.899,
                            "candidate_mean_abs_error": 0.858,
                            "baseline_mean_abs_error": 0.936,
                            "candidate_minus_background_mae": -0.041,
                            "candidate_minus_baseline_mae": -0.078,
                            "background_rmse": 1.2,
                            "candidate_rmse": 1.1,
                            "baseline_rmse": 1.3,
                            "candidate_minus_background_rmse": -0.1,
                            "candidate_minus_baseline_rmse": -0.2,
                            "candidate_beats_background_mae_fold_count": 2,
                            "candidate_beats_baseline_mae_fold_count": 2,
                            "candidate_beats_background_rmse_fold_count": 1,
                            "candidate_beats_baseline_rmse_fold_count": 2
                        },
                        "dewpoint_c": {
                            "fold_count": 2,
                            "candidate_minus_background_mae": 0.2,
                            "candidate_minus_baseline_mae": -0.1
                        },
                        "wind_speed_ms": {
                            "fold_count": 2,
                            "candidate_minus_background_mae": -0.02,
                            "candidate_minus_baseline_mae": -0.25
                        }
                    },
                    "baseline_repeated_holdout_validation": {
                        "folds": [
                            {
                                "validation": {
                                    "source_summaries": [
                                        {
                                            "source": "metar",
                                            "sampled_observation_count": 10,
                                            "temperature_c": {
                                                "observation_count": 10,
                                                "mean_abs_background_error": 1.0,
                                                "mean_abs_analysis_error": 0.9,
                                                "background_rmse": 1.2,
                                                "analysis_rmse": 1.1
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "validation": {
                                    "source_summaries": [
                                        {
                                            "source": "metar",
                                            "sampled_observation_count": 20,
                                            "temperature_c": {
                                                "observation_count": 20,
                                                "mean_abs_background_error": 2.0,
                                                "mean_abs_analysis_error": 1.8,
                                                "background_rmse": 2.2,
                                                "analysis_rmse": 2.0
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                "external_reference_comparison": {
                    "reference_label": "rtma",
                    "validation_mode": "holdout_validation",
                    "temperature_c": {
                        "candidate_observation_count": 30,
                        "candidate_mean_abs_error": 0.858,
                        "reference_mean_abs_error": 0.9,
                        "candidate_rmse": 1.1,
                        "reference_rmse": 1.2
                    },
                    "dewpoint_c": {
                        "candidate_observation_count": 20,
                        "candidate_mean_abs_error": 1.0,
                        "reference_mean_abs_error": 0.8,
                        "candidate_rmse": 1.4,
                        "reference_rmse": 1.1
                    }
                }
            })),
        )]);

        assert_eq!(report.loaded_case_count, 1);
        assert!(report.quality_flags.is_empty());
        assert_eq!(report.aggregate.mean_mesoanalysis_compute_ms, Some(1337.0));
        assert_eq!(report.aggregate.max_mesoanalysis_compute_ms, Some(1337.0));
        assert_eq!(report.aggregate.model_counts.get("hrrr"), Some(&1));
        assert_eq!(report.aggregate.date_counts.get("20260513"), Some(&1));
        assert_eq!(report.aggregate.cycle_counts.get("00"), Some(&1));
        assert_eq!(report.aggregate.forecast_hour_counts.get("f001"), Some(&1));
        assert_eq!(report.aggregate.case_signature_counts.len(), 1);
        assert_eq!(
            report.cases[0].case_tags,
            vec!["hazard=severe".to_string(), "regime=dryline".to_string()]
        );
        assert_eq!(
            report.aggregate.case_tag_counts.get("regime=dryline"),
            Some(&1)
        );
        assert_eq!(
            report.aggregate.case_tag_counts.get("hazard=severe"),
            Some(&1)
        );
        let station = report
            .aggregate
            .stations
            .get("metar::KOUN")
            .expect("station innovation aggregate");
        assert_eq!(station.station_id, "KOUN");
        assert_eq!(station.source, "metar");
        assert_eq!(station.case_count, 1);
        assert_eq!(station.sample_count, 2);
        let station_temperature = station
            .variables
            .get("temperature_c")
            .expect("station temperature aggregate");
        assert_eq!(station_temperature.observation_count, 2);
        assert_close(station_temperature.mean_abs_analysis_error.unwrap(), 0.375);
        assert_close(
            station_temperature.mean_abs_error_improvement.unwrap(),
            0.625,
        );
        let history = build_surface_mesoanalysis_innovation_history(&report);
        assert_eq!(
            history.schema,
            "rustwx.surface_mesoanalysis.innovation_history.v1"
        );
        assert_eq!(history.station_watchlist.len(), 1);
        assert_eq!(history.station_watchlist[0].station_key, "metar::KOUN");
        assert_eq!(history.station_watchlist[0].variable, "temperature_c");
        assert_eq!(
            history.station_watchlist[0].reason,
            "high_station_analysis_error"
        );
        assert_eq!(history.source_watchlist.len(), 1);
        assert_eq!(history.source_watchlist[0].source, "metar");
        let station_index_records = station_wxstore_index_records(&history);
        assert_eq!(station_index_records.len(), 1);
        assert_eq!(station_index_records[0].station_key, "metar::KOUN");
        assert_eq!(
            station_index_records[0]
                .watchlist
                .as_ref()
                .expect("station watchlist on index record")
                .reason,
            "high_station_analysis_error"
        );
        let source_index_records = source_wxstore_index_records(&history);
        assert_eq!(source_index_records.len(), 1);
        assert_eq!(source_index_records[0].source, "metar");
        assert!(
            source_index_records[0].watchlist.is_some(),
            "source watchlist should be denormalized into source index record"
        );
        let station_query = query_surface_mesoanalysis_innovation_history(
            &history,
            SurfaceMesoanalysisInnovationQueryRequest {
                stations: vec!["KOUN".to_string()],
                sources: Vec::new(),
                variables: vec!["temperature_c".to_string()],
                min_case_count: Some(1),
                top: 1,
            },
        );
        assert_eq!(
            station_query.schema,
            "rustwx.surface_mesoanalysis.innovation_query.v1"
        );
        assert_eq!(station_query.matched_station_watchlist_count, 1);
        assert_eq!(station_query.station_watchlist.len(), 1);
        assert_eq!(
            station_query.station_watchlist[0].station_key,
            "metar::KOUN"
        );
        assert!(station_query.source_watchlist.is_empty());
        let source_query = query_surface_mesoanalysis_innovation_history(
            &history,
            SurfaceMesoanalysisInnovationQueryRequest {
                stations: Vec::new(),
                sources: vec!["metar".to_string()],
                variables: vec!["temperature_c".to_string()],
                min_case_count: Some(2),
                top: 1,
            },
        );
        assert!(source_query.station_watchlist.is_empty());
        assert!(source_query.source_watchlist.is_empty());
        let source_query = query_surface_mesoanalysis_innovation_history(
            &history,
            SurfaceMesoanalysisInnovationQueryRequest {
                stations: Vec::new(),
                sources: vec!["metar".to_string()],
                variables: vec!["temperature_c".to_string()],
                min_case_count: Some(1),
                top: 1,
            },
        );
        assert_eq!(source_query.matched_source_watchlist_count, 1);
        assert_eq!(source_query.source_watchlist.len(), 1);
        assert_eq!(source_query.source_watchlist[0].source, "metar");
        let station_history = history
            .station_series
            .get("metar::KOUN")
            .expect("station innovation history");
        assert_eq!(station_history.entries.len(), 1);
        assert_eq!(station_history.entries[0].sample_count, 2);
        assert_eq!(
            station_history.entries[0].case.case_signature,
            "hrrr|nomads|20260513|00|f001"
        );
        assert_eq!(
            station_history.entries[0]
                .variables
                .get("temperature_c")
                .expect("station history temperature")
                .mean_abs_analysis_error,
            Some(0.375)
        );
        assert_eq!(
            report.cases[0].benchmark_mode,
            "repeated_holdout_validation"
        );
        assert_eq!(
            report.cases[0].holdout_strategy.as_deref(),
            Some("spatial_block")
        );
        assert_eq!(report.cases[0].repeated_fold_count, Some(2));
        let temperature_diagnostics = report
            .aggregate
            .diagnostics
            .get("temperature_2m_c")
            .expect("temperature diagnostics");
        assert_eq!(temperature_diagnostics.case_count, 1);
        assert_eq!(
            temperature_diagnostics.total_gross_error_rescued_observations,
            3
        );
        assert_eq!(temperature_diagnostics.total_solver_failed_grid_cells, 1);
        assert_eq!(
            temperature_diagnostics.total_truncated_neighbor_grid_cells,
            4
        );
        let temperature = report
            .aggregate
            .variables
            .get("temperature_c")
            .expect("temperature aggregate");
        assert_eq!(temperature.case_count, 1);
        assert_eq!(
            temperature.mean_candidate_minus_background_mae,
            Some(-0.041)
        );
        assert_eq!(temperature.candidate_beats_background_mae_case_count, 1);
        let dewpoint = report
            .aggregate
            .variables
            .get("dewpoint_c")
            .expect("dewpoint aggregate");
        assert_eq!(dewpoint.candidate_loses_background_mae_case_count, 1);
        let metar = report.cases[0].sources.get("metar").expect("metar source");
        assert_eq!(metar.sampled_observation_count, 30);
        let metar_history = history
            .source_series
            .get("metar")
            .expect("source innovation history");
        assert_eq!(metar_history.entries.len(), 1);
        assert_eq!(metar_history.entries[0].sampled_observation_count, 30);
        let deduped_history = merge_surface_mesoanalysis_innovation_history(
            Some(history.clone()),
            history.clone(),
            None,
        );
        assert_eq!(deduped_history.case_count, 1);
        assert_eq!(
            deduped_history
                .station_series
                .get("metar::KOUN")
                .expect("deduped station history")
                .entries
                .len(),
            1
        );
        let mut next_history = history.clone();
        for series in next_history.station_series.values_mut() {
            for entry in &mut series.entries {
                entry.case.date = "20260514".to_string();
                entry.case.model_cycle = "2026051400z".to_string();
                entry.case.case_signature = "hrrr|nomads|20260514|00|f001".to_string();
            }
        }
        for series in next_history.source_series.values_mut() {
            for entry in &mut series.entries {
                entry.case.date = "20260514".to_string();
                entry.case.model_cycle = "2026051400z".to_string();
                entry.case.case_signature = "hrrr|nomads|20260514|00|f001".to_string();
            }
        }
        let merged_history = merge_surface_mesoanalysis_innovation_history(
            Some(history.clone()),
            next_history.clone(),
            None,
        );
        assert_eq!(merged_history.case_count, 2);
        assert_eq!(
            merged_history
                .station_series
                .get("metar::KOUN")
                .expect("merged station history")
                .entries
                .len(),
            2
        );
        let retained_history =
            merge_surface_mesoanalysis_innovation_history(Some(history), next_history, Some(1));
        let retained_station = retained_history
            .station_series
            .get("metar::KOUN")
            .expect("retained station history");
        assert_eq!(retained_history.case_count, 1);
        assert_eq!(retained_station.entries.len(), 1);
        assert_eq!(
            retained_station.entries[0].case.case_signature,
            "hrrr|nomads|20260514|00|f001"
        );
        assert_eq!(
            retained_station
                .aggregate
                .as_ref()
                .expect("retained station aggregate")
                .case_count,
            1
        );
        assert_eq!(
            retained_history.station_watchlist[0].station_key,
            "metar::KOUN"
        );
        let metar_temperature = metar
            .variables
            .get("temperature_c")
            .expect("metar temperature");
        assert_eq!(metar_temperature.observation_count, Some(30));
        assert_close(
            metar_temperature.background_mean_abs_error.unwrap(),
            1.666_666_666_666_666_7,
        );
        assert_close(metar_temperature.candidate_mean_abs_error.unwrap(), 1.2);
        assert_close(metar_temperature.barnes_mean_abs_error.unwrap(), 1.5);
        assert_close(
            metar_temperature.candidate_minus_background_mae.unwrap(),
            -0.466_666_666_666_666_8,
        );
        assert_close(metar_temperature.candidate_minus_barnes_mae.unwrap(), -0.3);
        let metar_aggregate = report
            .aggregate
            .sources
            .get("metar")
            .expect("metar aggregate");
        assert_eq!(metar_aggregate.case_count, 1);
        assert_eq!(metar_aggregate.mean_sampled_observation_count, Some(30.0));
        assert_eq!(
            metar_aggregate
                .variables
                .get("temperature_c")
                .expect("metar aggregate temperature")
                .candidate_beats_barnes_mae_case_count,
            1
        );
        let rtma = report
            .aggregate
            .external_references
            .get("rtma")
            .expect("rtma reference aggregate");
        let rtma_temperature = rtma
            .variables
            .get("temperature_c")
            .expect("rtma temperature");
        assert_eq!(rtma.case_count, 1);
        assert_close(
            rtma_temperature.mean_candidate_minus_reference_mae.unwrap(),
            -0.042,
        );
        assert_eq!(rtma_temperature.candidate_beats_reference_mae_case_count, 1);
        let rtma_dewpoint = rtma.variables.get("dewpoint_c").expect("rtma dewpoint");
        assert_close(
            rtma_dewpoint.mean_candidate_minus_reference_mae.unwrap(),
            0.2,
        );
        assert_eq!(rtma_dewpoint.candidate_loses_reference_mae_case_count, 1);
        let pass_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: vec!["contains_same_observation_validation".to_string()],
                required_benchmark_modes: vec!["repeated_holdout_validation".to_string()],
                required_holdout_strategies: vec!["spatial_block".to_string()],
                required_external_references: vec!["rtma".to_string()],
                required_covariance_ablations: Vec::new(),
                required_case_tags: Vec::new(),
                variables: vec!["temperature_c".to_string()],
                sources: vec!["metar".to_string()],
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: Some(0.0),
                max_domain_candidate_minus_barnes_mae: Some(0.0),
                max_domain_candidate_minus_reference_mae: Some(0.0),
                max_covariance_ablation_candidate_minus_baseline_mae: None,
                max_case_mesoanalysis_compute_ms: None,
                min_unique_case_signatures: Some(1),
                min_unique_dates: Some(1),
                min_unique_cycles: Some(1),
                min_unique_forecast_hours: Some(1),
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: Some(0.0),
                max_source_candidate_minus_barnes_mae: Some(0.0),
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(pass_gate.passed, "{:#?}", pass_gate.checks);
        assert!(
            pass_gate
                .checks
                .iter()
                .any(|check| check.name == "required_holdout_strategies" && check.passed)
        );
        let fail_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: Vec::new(),
                required_benchmark_modes: Vec::new(),
                required_holdout_strategies: Vec::new(),
                required_external_references: vec!["rtma".to_string()],
                required_covariance_ablations: Vec::new(),
                required_case_tags: Vec::new(),
                variables: vec!["dewpoint_c".to_string()],
                sources: Vec::new(),
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: Some(0.0),
                max_domain_candidate_minus_barnes_mae: None,
                max_domain_candidate_minus_reference_mae: Some(0.0),
                max_covariance_ablation_candidate_minus_baseline_mae: None,
                max_case_mesoanalysis_compute_ms: None,
                min_unique_case_signatures: None,
                min_unique_dates: None,
                min_unique_cycles: None,
                min_unique_forecast_hours: None,
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(!fail_gate.passed);
        assert!(
            fail_gate.checks.iter().any(|check| check.name
                == "domain_candidate_minus_background_mae"
                && !check.passed)
        );

        let station_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: vec!["contains_same_observation_validation".to_string()],
                required_benchmark_modes: vec!["repeated_holdout_validation".to_string()],
                required_holdout_strategies: vec!["spatial_block".to_string()],
                variables: vec!["temperature_c".to_string()],
                stations: vec!["metar::KOUN".to_string()],
                min_station_observation_count: Some(2),
                max_station_candidate_minus_background_mae: Some(0.0),
                max_station_analysis_mae: Some(0.5),
                max_station_abs_analysis_bias: Some(0.2),
                ..empty_gate_thresholds()
            },
        );
        assert!(station_gate.passed, "{:#?}", station_gate.checks);
        assert!(
            station_gate
                .checks
                .iter()
                .any(|check| check.name == "station_analysis_mae" && check.passed)
        );

        let mut failing_station_thresholds = station_gate.thresholds.clone();
        failing_station_thresholds.max_station_analysis_mae = Some(0.1);
        let failing_station_gate =
            evaluate_surface_mesoanalysis_calibration_gate(&report, failing_station_thresholds);
        assert!(!failing_station_gate.passed);
        assert!(
            failing_station_gate
                .checks
                .iter()
                .any(|check| check.name == "station_analysis_mae" && !check.passed)
        );
    }

    #[test]
    fn calibration_report_aggregates_validation_strata() {
        let report = build_surface_mesoanalysis_calibration_report_from_values([(
            PathBuf::from("strata/run_report.json"),
            Ok(json!({
                "schema": "rustwx.surface_mesoanalysis.run_report.v1",
                "model": "hrrr",
                "model_source": "nomads",
                "model_cycle": "2026051300z",
                "date": "20260513",
                "cycle": 0,
                "forecast_hour": 1,
                "model_load_mode": "surface_only",
                "mesoanalysis_config": {
                    "method": "optimal_interpolation"
                },
                "mesoanalysis": {
                    "observation_count": 2,
                    "source_count": 1,
                    "validation": {
                        "strata_summaries": [
                            {
                                "stratum_type": "terrain_pressure_class",
                                "stratum_value": "lowland_high_pressure",
                                "sampled_observation_count": 2,
                                "temperature_c": {
                                    "observation_count": 2,
                                    "mean_abs_background_error": 2.0,
                                    "mean_abs_analysis_error": 0.8,
                                    "background_rmse": 2.2,
                                    "analysis_rmse": 1.0
                                }
                            }
                        ]
                    }
                },
                "barnes_baseline_comparison": {
                    "benchmark_summary": {
                        "validation_mode": "same_observation_validation",
                        "temperature_c": {
                            "candidate_observation_count": 2,
                            "background_mean_abs_error": 2.0,
                            "candidate_mean_abs_error": 0.8,
                            "baseline_mean_abs_error": 1.4,
                            "candidate_minus_background_mae": -1.2,
                            "candidate_minus_baseline_mae": -0.6,
                            "background_rmse": 2.2,
                            "candidate_rmse": 1.0,
                            "baseline_rmse": 1.6,
                            "candidate_minus_background_rmse": -1.2,
                            "candidate_minus_baseline_rmse": -0.6
                        }
                    },
                    "baseline_validation": {
                        "strata_summaries": [
                            {
                                "stratum_type": "terrain_pressure_class",
                                "stratum_value": "lowland_high_pressure",
                                "sampled_observation_count": 2,
                                "temperature_c": {
                                    "observation_count": 2,
                                    "mean_abs_background_error": 2.0,
                                    "mean_abs_analysis_error": 1.4,
                                    "background_rmse": 2.2,
                                    "analysis_rmse": 1.6
                                }
                            }
                        ]
                    }
                }
            })),
        )]);

        assert_eq!(report.loaded_case_count, 1);
        let key = "terrain_pressure_class=lowland_high_pressure";
        let case_stratum = report.cases[0].strata.get(key).expect("case stratum");
        assert_eq!(case_stratum.stratum_type, "terrain_pressure_class");
        assert_eq!(case_stratum.stratum_value, "lowland_high_pressure");
        assert_eq!(case_stratum.sampled_observation_count, 2);
        let case_temperature = case_stratum
            .variables
            .get("temperature_c")
            .expect("case stratum temperature");
        assert_eq!(case_temperature.observation_count, Some(2));
        assert_close(case_temperature.candidate_minus_barnes_mae.unwrap(), -0.6);

        let aggregate_stratum = report.aggregate.strata.get(key).expect("aggregate stratum");
        assert_eq!(aggregate_stratum.case_count, 1);
        assert_eq!(aggregate_stratum.mean_sampled_observation_count, Some(2.0));
        assert_eq!(
            aggregate_stratum
                .variables
                .get("temperature_c")
                .expect("aggregate stratum temperature")
                .candidate_beats_barnes_mae_case_count,
            1
        );

        let pass_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: vec!["contains_same_observation_validation".to_string()],
                required_benchmark_modes: vec!["same_observation_validation".to_string()],
                required_holdout_strategies: Vec::new(),
                required_external_references: Vec::new(),
                required_covariance_ablations: Vec::new(),
                required_case_tags: Vec::new(),
                variables: vec!["temperature_c".to_string()],
                sources: Vec::new(),
                strata: vec![key.to_string()],
                max_domain_candidate_minus_background_mae: None,
                max_domain_candidate_minus_barnes_mae: None,
                max_domain_candidate_minus_reference_mae: None,
                max_covariance_ablation_candidate_minus_baseline_mae: None,
                max_case_mesoanalysis_compute_ms: None,
                min_unique_case_signatures: None,
                min_unique_dates: None,
                min_unique_cycles: None,
                min_unique_forecast_hours: None,
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: Some(0.0),
                max_stratum_candidate_minus_barnes_mae: Some(0.0),
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(pass_gate.passed, "{:#?}", pass_gate.checks);
        assert!(
            pass_gate
                .checks
                .iter()
                .any(|check| check.name == "stratum_candidate_minus_barnes_mae" && check.passed)
        );

        let fail_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                max_stratum_candidate_minus_barnes_mae: Some(-0.7),
                ..pass_gate.thresholds
            },
        );
        assert!(!fail_gate.passed);
        assert!(
            fail_gate
                .checks
                .iter()
                .any(|check| check.name == "stratum_candidate_minus_barnes_mae" && !check.passed)
        );
    }

    #[test]
    fn calibration_gate_can_require_confidence_reliability() {
        let confidence = json!({
            "observation_count": 30,
            "mean_confidence": 0.5,
            "low_confidence_observation_count": 10,
            "low_confidence_mean_abs_analysis_error": 2.0,
            "medium_confidence_observation_count": 10,
            "medium_confidence_mean_abs_analysis_error": 1.0,
            "high_confidence_observation_count": 10,
            "high_confidence_mean_abs_analysis_error": 0.5,
            "high_minus_low_mean_abs_analysis_error": -1.5,
            "ranked_low_confidence_observation_count": 10,
            "ranked_low_confidence_mean_abs_analysis_error": 2.0,
            "ranked_high_confidence_observation_count": 10,
            "ranked_high_confidence_mean_abs_analysis_error": 0.5,
            "ranked_high_minus_low_mean_abs_analysis_error": -1.5,
            "confidence_abs_error_correlation": -0.7
        });
        let report = build_surface_mesoanalysis_calibration_report_from_values([(
            PathBuf::from("confidence/run_report.json"),
            Ok(json!({
                "schema": "rustwx.surface_mesoanalysis.run_report.v1",
                "model": "hrrr",
                "model_source": "nomads",
                "model_cycle": "2026051300z",
                "date": "20260513",
                "cycle": 0,
                "forecast_hour": 1,
                "model_load_mode": "surface_only",
                "mesoanalysis_config": {
                    "method": "optimal_interpolation"
                },
                "mesoanalysis": {
                    "observation_count": 30,
                    "source_count": 1,
                    "holdout_validation": {
                        "strategy": "spatial_block",
                        "validation": {
                            "temperature_c": {
                                "observation_count": 30,
                                "mean_abs_background_error": 1.2,
                                "mean_abs_analysis_error": 1.0,
                                "background_rmse": 1.4,
                                "analysis_rmse": 1.2,
                                "confidence": confidence.clone()
                            },
                            "source_summaries": [
                                {
                                    "source": "metar",
                                    "sampled_observation_count": 30,
                                    "temperature_c": {
                                        "observation_count": 30,
                                        "mean_abs_background_error": 1.2,
                                        "mean_abs_analysis_error": 1.0,
                                        "background_rmse": 1.4,
                                        "analysis_rmse": 1.2,
                                        "confidence": confidence
                                    }
                                }
                            ]
                        }
                    }
                },
                "barnes_baseline_comparison": {
                    "holdout_benchmark_summary": {
                        "validation_mode": "holdout_validation",
                        "temperature_c": {
                            "candidate_observation_count": 30,
                            "background_mean_abs_error": 1.2,
                            "candidate_mean_abs_error": 1.0,
                            "baseline_mean_abs_error": 1.1,
                            "candidate_minus_background_mae": -0.2,
                            "candidate_minus_baseline_mae": -0.1
                        }
                    }
                }
            })),
        )]);

        let temperature = report
            .aggregate
            .variables
            .get("temperature_c")
            .expect("temperature aggregate");
        let temperature_confidence = temperature.confidence.as_ref().unwrap();
        assert_eq!(temperature_confidence.case_count, 1);
        assert_close(
            temperature_confidence
                .worst_high_minus_low_confidence_mae
                .unwrap(),
            -1.5,
        );
        assert_close(
            temperature_confidence
                .worst_ranked_high_minus_low_confidence_mae
                .unwrap(),
            -1.5,
        );
        assert_close(
            temperature_confidence
                .worst_confidence_abs_error_correlation
                .unwrap(),
            -0.7,
        );
        assert_eq!(
            temperature_confidence.min_low_confidence_observation_count,
            Some(10.0)
        );
        assert_eq!(
            temperature_confidence.min_high_confidence_observation_count,
            Some(10.0)
        );
        assert_eq!(temperature_confidence.reliability.status, "passed");
        assert_eq!(
            temperature_confidence.reliability.semantic_label,
            "calibrated_reliability"
        );

        let thresholds = SurfaceMesoanalysisCalibrationGateThresholds {
            min_case_count: 1,
            allow_skipped_reports: false,
            allowed_quality_flags: Vec::new(),
            required_benchmark_modes: vec!["holdout_validation".to_string()],
            required_holdout_strategies: vec!["spatial_block".to_string()],
            required_external_references: Vec::new(),
            required_covariance_ablations: Vec::new(),
            required_case_tags: Vec::new(),
            variables: vec!["temperature_c".to_string()],
            sources: vec!["metar".to_string()],
            strata: Vec::new(),
            max_domain_candidate_minus_background_mae: None,
            max_domain_candidate_minus_barnes_mae: None,
            max_domain_candidate_minus_reference_mae: None,
            max_covariance_ablation_candidate_minus_baseline_mae: None,
            max_case_mesoanalysis_compute_ms: None,
            min_unique_case_signatures: None,
            min_unique_dates: None,
            min_unique_cycles: None,
            min_unique_forecast_hours: None,
            min_unique_case_tags: None,
            min_domain_low_confidence_observation_count: Some(10),
            min_domain_high_confidence_observation_count: Some(10),
            max_domain_high_minus_low_confidence_mae: Some(0.0),
            max_domain_ranked_high_minus_low_confidence_mae: Some(0.0),
            max_domain_confidence_abs_error_correlation: Some(0.0),
            max_source_candidate_minus_background_mae: None,
            max_source_candidate_minus_barnes_mae: None,
            min_source_low_confidence_observation_count: Some(10),
            min_source_high_confidence_observation_count: Some(10),
            max_source_high_minus_low_confidence_mae: Some(0.0),
            max_source_ranked_high_minus_low_confidence_mae: Some(0.0),
            max_source_confidence_abs_error_correlation: Some(0.0),
            max_stratum_candidate_minus_background_mae: None,
            max_stratum_candidate_minus_barnes_mae: None,
            min_stratum_low_confidence_observation_count: None,
            min_stratum_high_confidence_observation_count: None,
            max_stratum_high_minus_low_confidence_mae: None,
            max_stratum_ranked_high_minus_low_confidence_mae: None,
            max_stratum_confidence_abs_error_correlation: None,
            ..empty_gate_thresholds()
        };
        let pass_gate = evaluate_surface_mesoanalysis_calibration_gate(&report, thresholds.clone());
        assert!(pass_gate.passed);
        assert!(
            pass_gate
                .checks
                .iter()
                .any(|check| check.name == "domain_high_minus_low_confidence_mae" && check.passed)
        );
        assert!(pass_gate.checks.iter().any(|check| check.name
            == "domain_ranked_high_minus_low_confidence_mae"
            && check.passed
            && check.comparator == "<= and reliability=passed"));
        assert!(
            pass_gate.checks.iter().any(|check| check.name
                == "domain_high_confidence_observation_count"
                && check.passed)
        );

        let mut fail_thresholds = thresholds;
        fail_thresholds.max_domain_confidence_abs_error_correlation = Some(-0.8);
        let fail_gate = evaluate_surface_mesoanalysis_calibration_gate(&report, fail_thresholds);
        assert!(!fail_gate.passed);
        assert!(
            fail_gate.checks.iter().any(|check| check.name
                == "domain_confidence_abs_error_correlation"
                && !check.passed)
        );
    }

    #[test]
    fn ranked_confidence_gate_rejects_undercovered_positive_signal() {
        let confidence = SurfaceMesoanalysisCalibrationConfidenceAggregate {
            case_count: 1,
            mean_observation_count: Some(3.0),
            mean_confidence: Some(0.5),
            mean_low_confidence_observation_count: Some(1.0),
            min_low_confidence_observation_count: Some(1.0),
            mean_low_confidence_mae: Some(2.0),
            mean_medium_confidence_observation_count: Some(1.0),
            min_medium_confidence_observation_count: Some(1.0),
            mean_medium_confidence_mae: Some(1.0),
            mean_high_confidence_observation_count: Some(1.0),
            min_high_confidence_observation_count: Some(1.0),
            mean_high_confidence_mae: Some(0.5),
            mean_high_minus_low_confidence_mae: Some(-1.5),
            worst_high_minus_low_confidence_mae: Some(-1.5),
            mean_confidence_abs_error_correlation: Some(-0.7),
            worst_confidence_abs_error_correlation: Some(-0.7),
            mean_ranked_low_confidence_observation_count: Some(1.0),
            mean_ranked_low_confidence_mae: Some(2.0),
            mean_ranked_high_confidence_observation_count: Some(1.0),
            mean_ranked_high_confidence_mae: Some(0.5),
            mean_ranked_high_minus_low_confidence_mae: Some(-1.5),
            worst_ranked_high_minus_low_confidence_mae: Some(-1.5),
            high_confidence_beats_low_confidence_mae_case_count: 1,
            high_confidence_loses_low_confidence_mae_case_count: 0,
            ranked_high_confidence_beats_low_confidence_mae_case_count: 1,
            ranked_high_confidence_loses_low_confidence_mae_case_count: 0,
            negative_confidence_abs_error_correlation_case_count: 1,
            positive_confidence_abs_error_correlation_case_count: 0,
            reliability: SurfaceMesoanalysisCalibrationConfidenceReliabilityAggregate {
                schema: "rustwx.surface_mesoanalysis.confidence_reliability_aggregate.v1"
                    .to_string(),
                semantic_label: "support_index".to_string(),
                status: "untestable".to_string(),
                bucket_coverage_sufficient: false,
                case_count: 1,
                passed_case_count: 0,
                failed_case_count: 0,
                untestable_case_count: 1,
                min_ranked_low_confidence_observation_count: Some(1.0),
                min_ranked_high_confidence_observation_count: Some(1.0),
                min_ranked_bucket_observation_count:
                    CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
                worst_ranked_high_minus_low_mean_abs_analysis_error: Some(-1.5),
                max_ranked_high_minus_low_mean_abs_analysis_error:
                    CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
                message: "one or more cases lacked enough ranked confidence bucket coverage to test reliability"
                    .to_string(),
            },
        };
        let mut checks = Vec::new();
        push_max_confidence_calibration_gate_check(
            &mut checks,
            "domain_ranked_high_minus_low_confidence_mae",
            "domain/temperature_c",
            Some(-1.5),
            0.0,
            "domain temperature_c worst ranked high-minus-low confidence MAE must be <= 0.000"
                .to_string(),
            Some(&confidence),
            true,
        );

        assert_eq!(checks.len(), 1);
        assert!(!checks[0].passed);
        assert_eq!(checks[0].comparator, "<= and reliability=passed");
        assert!(checks[0].message.contains("status=untestable"));
    }

    #[test]
    fn calibration_report_loads_external_reference_only_case() {
        let report = build_surface_mesoanalysis_calibration_report_from_values([(
            PathBuf::from("rtma_only/run_report.json"),
            Ok(json!({
                "schema": "rustwx.surface_mesoanalysis.run_report.v1",
                "model": "hrrr",
                "model_source": "nomads",
                "model_cycle": "2026051300z",
                "date": "20260513",
                "cycle": 0,
                "forecast_hour": 1,
                "model_load_mode": "surface_only",
                "mesoanalysis_config": {
                    "method": "optimal_interpolation"
                },
                "validation_gate": {
                    "passed": true
                },
                "mesoanalysis": {
                    "observation_count": 100,
                    "source_count": 2,
                    "holdout_validation": {
                        "strategy": "spatial_block"
                    }
                },
                "external_reference_comparisons": [
                    {
                        "reference_label": "rtma",
                        "validation_mode": "holdout_validation",
                        "temperature_c": {
                            "candidate_observation_count": 25,
                            "candidate_mean_abs_error": 0.9,
                            "reference_mean_abs_error": 0.7,
                            "candidate_rmse": 1.1,
                            "reference_rmse": 0.9
                        }
                    }
                ]
            })),
        )]);

        assert_eq!(report.loaded_case_count, 1);
        assert!(report.skipped_reports.is_empty());
        assert!(report.aggregate.variables.is_empty());
        assert_eq!(report.cases[0].benchmark_mode, "holdout_validation");
        assert_eq!(
            report.cases[0].holdout_strategy.as_deref(),
            Some("spatial_block")
        );
        let rtma = report
            .aggregate
            .external_references
            .get("rtma")
            .expect("rtma aggregate");
        assert_eq!(rtma.case_count, 1);
        assert_close(
            rtma.variables
                .get("temperature_c")
                .expect("temperature")
                .mean_candidate_minus_reference_mae
                .unwrap(),
            0.2,
        );

        let gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: Vec::new(),
                required_benchmark_modes: vec!["holdout_validation".to_string()],
                required_holdout_strategies: Vec::new(),
                required_external_references: vec!["rtma".to_string()],
                required_covariance_ablations: Vec::new(),
                required_case_tags: Vec::new(),
                variables: Vec::new(),
                sources: Vec::new(),
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: None,
                max_domain_candidate_minus_barnes_mae: None,
                max_domain_candidate_minus_reference_mae: Some(0.25),
                max_covariance_ablation_candidate_minus_baseline_mae: None,
                max_case_mesoanalysis_compute_ms: None,
                min_unique_case_signatures: None,
                min_unique_dates: None,
                min_unique_cycles: None,
                min_unique_forecast_hours: None,
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(gate.passed);
    }

    #[test]
    fn calibration_report_loads_covariance_ablation_case() {
        let report = build_surface_mesoanalysis_calibration_report_from_values([(
            PathBuf::from("ablation_only/run_report.json"),
            Ok(json!({
                "schema": "rustwx.surface_mesoanalysis.run_report.v1",
                "model": "hrrr",
                "model_source": "nomads",
                "model_cycle": "2026051302z",
                "date": "20260513",
                "cycle": 2,
                "forecast_hour": 1,
                "model_load_mode": "surface_only",
                "mesoanalysis_compute_ms": 1500,
                "mesoanalysis_config": {
                    "method": "optimal_interpolation",
                    "oi_covariance_kernel": "exponential"
                },
                "validation_gate": {
                    "passed": true
                },
                "mesoanalysis": {
                    "observation_count": 100,
                    "source_count": 2,
                    "holdout_validation": {
                        "strategy": "spatial_block"
                    }
                },
                "covariance_ablation_comparison": {
                    "baseline_label": "IsotropicOiNoTerrain",
                    "candidate_label": "OptimalInterpolation",
                    "validation_mode": "same_observation_validation",
                    "baseline_compute_ms": 1418,
                    "holdout_benchmark_summary": {
                        "validation_mode": "holdout_validation",
                        "temperature_c": {
                            "candidate_observation_count": 30,
                            "baseline_observation_count": 30,
                            "candidate_mean_abs_error": 1.10,
                            "baseline_mean_abs_error": 1.12,
                            "candidate_minus_baseline_mae": -0.02,
                            "candidate_rmse": 1.37,
                            "baseline_rmse": 1.39,
                            "candidate_minus_baseline_rmse": -0.02,
                            "mae_winner": "candidate",
                            "rmse_winner": "candidate"
                        },
                        "dewpoint_c": {
                            "candidate_observation_count": 30,
                            "baseline_observation_count": 30,
                            "candidate_mean_abs_error": 1.20,
                            "baseline_mean_abs_error": 1.15,
                            "candidate_rmse": 1.45,
                            "baseline_rmse": 1.44,
                            "mae_winner": "baseline",
                            "rmse_winner": "baseline"
                        }
                    }
                }
            })),
        )]);

        assert_eq!(report.loaded_case_count, 1);
        assert!(report.aggregate.variables.is_empty());
        assert_eq!(report.aggregate.mean_mesoanalysis_compute_ms, Some(1500.0));
        assert_eq!(report.aggregate.max_mesoanalysis_compute_ms, Some(1500.0));
        assert_eq!(report.aggregate.model_counts.get("hrrr"), Some(&1));
        assert_eq!(report.aggregate.cycle_counts.get("02"), Some(&1));
        assert_eq!(report.aggregate.forecast_hour_counts.get("f001"), Some(&1));
        assert_eq!(report.aggregate.case_signature_counts.len(), 1);
        assert_eq!(report.cases[0].benchmark_mode, "holdout_validation");
        assert_eq!(
            report.cases[0].holdout_strategy.as_deref(),
            Some("spatial_block")
        );
        let ablation = report
            .aggregate
            .covariance_ablations
            .get("IsotropicOiNoTerrain")
            .expect("covariance ablation aggregate");
        assert_eq!(ablation.case_count, 1);
        assert_eq!(ablation.mean_baseline_compute_ms, Some(1418.0));
        let temperature = ablation
            .variables
            .get("temperature_c")
            .expect("temperature ablation");
        assert_close(
            temperature.mean_candidate_minus_baseline_mae.unwrap(),
            -0.02,
        );
        assert_eq!(temperature.candidate_beats_baseline_mae_case_count, 1);
        let dewpoint = ablation
            .variables
            .get("dewpoint_c")
            .expect("dewpoint ablation");
        assert_close(dewpoint.mean_candidate_minus_baseline_mae.unwrap(), 0.05);
        assert_eq!(dewpoint.candidate_loses_baseline_mae_case_count, 1);

        let pass_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: Vec::new(),
                required_benchmark_modes: vec!["holdout_validation".to_string()],
                required_holdout_strategies: vec!["spatial_block".to_string()],
                required_external_references: Vec::new(),
                required_covariance_ablations: vec!["IsotropicOiNoTerrain".to_string()],
                required_case_tags: Vec::new(),
                variables: vec!["temperature_c".to_string()],
                sources: Vec::new(),
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: None,
                max_domain_candidate_minus_barnes_mae: None,
                max_domain_candidate_minus_reference_mae: None,
                max_covariance_ablation_candidate_minus_baseline_mae: Some(0.0),
                max_case_mesoanalysis_compute_ms: Some(2_000.0),
                min_unique_case_signatures: Some(1),
                min_unique_dates: Some(1),
                min_unique_cycles: Some(1),
                min_unique_forecast_hours: Some(1),
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(pass_gate.passed);
        assert!(
            pass_gate
                .checks
                .iter()
                .any(|check| check.name == "max_case_mesoanalysis_compute_ms" && check.passed)
        );

        let fail_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 1,
                allow_skipped_reports: false,
                allowed_quality_flags: Vec::new(),
                required_benchmark_modes: Vec::new(),
                required_holdout_strategies: Vec::new(),
                required_external_references: Vec::new(),
                required_covariance_ablations: vec!["IsotropicOiNoTerrain".to_string()],
                required_case_tags: Vec::new(),
                variables: vec!["dewpoint_c".to_string()],
                sources: Vec::new(),
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: None,
                max_domain_candidate_minus_barnes_mae: None,
                max_domain_candidate_minus_reference_mae: None,
                max_covariance_ablation_candidate_minus_baseline_mae: Some(0.0),
                max_case_mesoanalysis_compute_ms: None,
                min_unique_case_signatures: None,
                min_unique_dates: None,
                min_unique_cycles: None,
                min_unique_forecast_hours: None,
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(!fail_gate.passed);
        assert!(fail_gate.checks.iter().any(|check| check.name
            == "covariance_ablation_candidate_minus_baseline_mae"
            && !check.passed));
    }

    #[test]
    fn calibration_gate_can_require_case_diversity() {
        let duplicate_signature_report =
            build_surface_mesoanalysis_calibration_report_from_values([
                (
                    PathBuf::from("case_a/run_report.json"),
                    Ok(diversity_case_json(0, 1, "spatial_block")),
                ),
                (
                    PathBuf::from("case_b_variant/run_report.json"),
                    Ok(diversity_case_json(0, 1, "source_hash")),
                ),
            ]);

        assert_eq!(duplicate_signature_report.loaded_case_count, 2);
        assert_eq!(
            duplicate_signature_report
                .aggregate
                .case_signature_counts
                .len(),
            1
        );
        assert!(
            duplicate_signature_report
                .quality_flags
                .contains(&"single_case_signature_matrix".to_string())
        );
        let duplicate_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &duplicate_signature_report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 2,
                allow_skipped_reports: false,
                allowed_quality_flags: vec!["single_case_signature_matrix".to_string()],
                required_benchmark_modes: vec!["holdout_validation".to_string()],
                required_holdout_strategies: vec![
                    "spatial_block".to_string(),
                    "source_hash".to_string(),
                ],
                required_external_references: Vec::new(),
                required_covariance_ablations: Vec::new(),
                required_case_tags: Vec::new(),
                variables: Vec::new(),
                sources: Vec::new(),
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: Some(0.0),
                max_domain_candidate_minus_barnes_mae: Some(0.0),
                max_domain_candidate_minus_reference_mae: None,
                max_covariance_ablation_candidate_minus_baseline_mae: None,
                max_case_mesoanalysis_compute_ms: Some(2_000.0),
                min_unique_case_signatures: Some(2),
                min_unique_dates: Some(1),
                min_unique_cycles: Some(1),
                min_unique_forecast_hours: Some(1),
                min_unique_case_tags: None,
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(!duplicate_gate.passed);
        assert!(
            duplicate_gate
                .checks
                .iter()
                .any(|check| check.name == "unique_case_signature_count" && !check.passed)
        );
        assert!(
            duplicate_gate
                .checks
                .iter()
                .any(|check| check.name == "required_holdout_strategies" && check.passed)
        );

        let diverse_report = build_surface_mesoanalysis_calibration_report_from_values([
            (
                PathBuf::from("case_a/run_report.json"),
                Ok(diversity_case_json(0, 1, "spatial_block")),
            ),
            (
                PathBuf::from("case_b/run_report.json"),
                Ok(diversity_case_json(2, 1, "source_hash")),
            ),
        ]);
        assert_eq!(diverse_report.aggregate.case_signature_counts.len(), 2);
        assert_eq!(diverse_report.aggregate.cycle_counts.len(), 2);
        assert_eq!(
            diverse_report
                .aggregate
                .case_tag_counts
                .get("regime=dryline"),
            Some(&1)
        );
        assert_eq!(
            diverse_report
                .aggregate
                .case_tag_counts
                .get("regime=nocturnal_llj"),
            Some(&1)
        );
        assert!(
            !diverse_report
                .quality_flags
                .contains(&"single_case_signature_matrix".to_string())
        );
        let diverse_gate = evaluate_surface_mesoanalysis_calibration_gate(
            &diverse_report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: 2,
                allow_skipped_reports: false,
                allowed_quality_flags: Vec::new(),
                required_benchmark_modes: vec!["holdout_validation".to_string()],
                required_holdout_strategies: vec![
                    "spatial_block".to_string(),
                    "source_hash".to_string(),
                ],
                required_external_references: Vec::new(),
                required_covariance_ablations: Vec::new(),
                required_case_tags: vec![
                    "regime=dryline".to_string(),
                    "regime=nocturnal_llj".to_string(),
                ],
                variables: Vec::new(),
                sources: Vec::new(),
                strata: Vec::new(),
                max_domain_candidate_minus_background_mae: Some(0.0),
                max_domain_candidate_minus_barnes_mae: Some(0.0),
                max_domain_candidate_minus_reference_mae: None,
                max_covariance_ablation_candidate_minus_baseline_mae: None,
                max_case_mesoanalysis_compute_ms: Some(2_000.0),
                min_unique_case_signatures: Some(2),
                min_unique_dates: Some(1),
                min_unique_cycles: Some(2),
                min_unique_forecast_hours: Some(1),
                min_unique_case_tags: Some(4),
                min_domain_low_confidence_observation_count: None,
                min_domain_high_confidence_observation_count: None,
                max_domain_high_minus_low_confidence_mae: None,
                max_domain_ranked_high_minus_low_confidence_mae: None,
                max_domain_confidence_abs_error_correlation: None,
                max_source_candidate_minus_background_mae: None,
                max_source_candidate_minus_barnes_mae: None,
                min_source_low_confidence_observation_count: None,
                min_source_high_confidence_observation_count: None,
                max_source_high_minus_low_confidence_mae: None,
                max_source_ranked_high_minus_low_confidence_mae: None,
                max_source_confidence_abs_error_correlation: None,
                max_stratum_candidate_minus_background_mae: None,
                max_stratum_candidate_minus_barnes_mae: None,
                min_stratum_low_confidence_observation_count: None,
                min_stratum_high_confidence_observation_count: None,
                max_stratum_high_minus_low_confidence_mae: None,
                max_stratum_ranked_high_minus_low_confidence_mae: None,
                max_stratum_confidence_abs_error_correlation: None,
                ..empty_gate_thresholds()
            },
        );
        assert!(diverse_gate.passed);
        assert!(
            diverse_gate
                .checks
                .iter()
                .any(|check| check.name == "required_case_tags" && check.passed)
        );
        assert!(
            diverse_gate
                .checks
                .iter()
                .any(|check| check.name == "unique_case_tag_count" && check.passed)
        );

        let mut missing_tag_thresholds = diverse_gate.thresholds.clone();
        missing_tag_thresholds.required_case_tags = vec!["regime=cold_pool".to_string()];
        missing_tag_thresholds.min_unique_case_tags = Some(5);
        let missing_tag_gate =
            evaluate_surface_mesoanalysis_calibration_gate(&diverse_report, missing_tag_thresholds);
        assert!(!missing_tag_gate.passed);
        assert!(
            missing_tag_gate
                .checks
                .iter()
                .any(|check| check.name == "required_case_tags" && !check.passed)
        );
        assert!(
            missing_tag_gate
                .checks
                .iter()
                .any(|check| check.name == "unique_case_tag_count" && !check.passed)
        );
    }

    fn diversity_case_json(cycle: u8, forecast_hour: u16, holdout_strategy: &str) -> Value {
        json!({
            "schema": "rustwx.surface_mesoanalysis.run_report.v1",
            "model": "hrrr",
            "model_source": "nomads",
            "model_cycle": format!("20260513{cycle:02}z"),
            "date": "20260513",
            "cycle": cycle,
            "forecast_hour": forecast_hour,
            "model_load_mode": "surface_only",
            "case_tags": [
                format!("holdout={holdout_strategy}"),
                if cycle == 0 { "regime=dryline" } else { "regime=nocturnal_llj" }
            ],
            "mesoanalysis_compute_ms": 1200,
            "mesoanalysis_config": {
                "method": "optimal_interpolation"
            },
            "validation_gate": {
                "passed": true
            },
            "mesoanalysis": {
                "observation_count": 100,
                "source_count": 2,
                "holdout_validation": {
                    "strategy": holdout_strategy,
                    "validation": {
                        "source_summaries": []
                    }
                }
            },
            "barnes_baseline_comparison": {
                "holdout_benchmark_summary": {
                    "validation_mode": "holdout_validation",
                    "temperature_c": {
                        "candidate_observation_count": 10,
                        "baseline_observation_count": 10,
                        "background_mean_abs_error": 1.0,
                        "candidate_mean_abs_error": 0.8,
                        "baseline_mean_abs_error": 0.9,
                        "candidate_minus_background_mae": -0.2,
                        "candidate_minus_baseline_mae": -0.1,
                        "background_rmse": 1.2,
                        "candidate_rmse": 1.0,
                        "baseline_rmse": 1.1,
                        "candidate_minus_background_rmse": -0.2,
                        "candidate_minus_baseline_rmse": -0.1
                    }
                }
            }
        })
    }

    #[test]
    fn calibration_report_records_unsupported_or_missing_benchmarks() {
        let report = build_surface_mesoanalysis_calibration_report_from_values([
            (
                PathBuf::from("bad_schema/run_report.json"),
                Ok(json!({
                    "schema": "something_else"
                })),
            ),
            (
                PathBuf::from("missing_benchmark/run_report.json"),
                Ok(json!({
                    "schema": "rustwx.surface_mesoanalysis.run_report.v1"
                })),
            ),
        ]);

        assert_eq!(report.loaded_case_count, 0);
        assert_eq!(report.skipped_reports.len(), 2);
        assert_eq!(
            report.quality_flags,
            vec!["empty_calibration_matrix", "skipped_reports_present"]
        );
        assert!(
            report.skipped_reports[0]
                .reason
                .contains("unsupported run schema")
        );
        assert!(
            report.skipped_reports[1]
                .reason
                .contains("no Barnes baseline")
        );
    }

    fn assert_close(left: f64, right: f64) {
        assert!(
            (left - right).abs() < 1.0e-12,
            "expected {left} to be close to {right}"
        );
    }
}
