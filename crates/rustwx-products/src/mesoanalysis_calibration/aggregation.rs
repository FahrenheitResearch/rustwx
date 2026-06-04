use super::confidence::ConfidenceAggregateAccumulator;
use super::*;
use std::collections::BTreeMap;

pub(super) fn aggregate_calibration_cases(
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

pub(super) fn calibration_quality_flags(
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
pub(super) struct StationAggregateAccumulator {
    station_id: String,
    source: String,
    case_count: usize,
    sample_count: usize,
    variables: BTreeMap<String, StationVariableStatsAccumulator>,
}

impl StationAggregateAccumulator {
    pub(super) fn new(station_id: String, source: String) -> Self {
        Self {
            station_id,
            source,
            case_count: 0,
            sample_count: 0,
            variables: BTreeMap::new(),
        }
    }

    pub(super) fn push(&mut self, station: &SurfaceMesoanalysisCalibrationStationCase) {
        self.case_count += 1;
        self.sample_count += station.sample_count;
        for (name, variable) in &station.variables {
            self.variables
                .entry(name.clone())
                .or_default()
                .push_variable_case(variable);
        }
    }

    pub(super) fn finish(self) -> SurfaceMesoanalysisCalibrationStationAggregate {
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

#[derive(Debug, Default)]
pub(super) struct SourceAggregateAccumulator {
    case_count: usize,
    sampled_observation_counts: Vec<f64>,
    variables: BTreeMap<String, VariableAggregateAccumulator>,
}

impl SourceAggregateAccumulator {
    pub(super) fn push(&mut self, source: &SurfaceMesoanalysisCalibrationSourceCase) {
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

    pub(super) fn finish(self) -> SurfaceMesoanalysisCalibrationSourceAggregate {
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
