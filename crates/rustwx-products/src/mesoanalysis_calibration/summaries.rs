use super::confidence::ConfidenceCaseAccumulator;
use super::helpers::{
    f64_at, mean_if_count, rmse_if_count, usize_at, value_at, weighted_mean, weighted_rmse,
};
use super::*;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub(super) struct SourceSummaryAccumulator {
    sampled_observation_count: usize,
    variables: BTreeMap<String, SourceVariableStatsAccumulator>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct StratumSummaryAccumulator {
    pub(super) stratum_type: String,
    pub(super) stratum_value: String,
    pub(super) summary: SourceSummaryAccumulator,
}

impl StratumSummaryAccumulator {
    pub(super) fn new(stratum_type: String, stratum_value: String) -> Self {
        Self {
            stratum_type,
            stratum_value,
            summary: SourceSummaryAccumulator::default(),
        }
    }

    pub(super) fn push_stratum_summary(&mut self, summary: &Value) {
        self.summary.push_source_summary(summary);
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct StationSummaryAccumulator {
    station_id: String,
    source: String,
    sample_count: usize,
    variables: BTreeMap<String, StationVariableStatsAccumulator>,
}

impl StationSummaryAccumulator {
    pub(super) fn new(station_id: String, source: String) -> Self {
        Self {
            station_id,
            source,
            sample_count: 0,
            variables: BTreeMap::new(),
        }
    }

    pub(super) fn push_sample(&mut self, sample: &Value) {
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

    pub(super) fn finish(self) -> SurfaceMesoanalysisCalibrationStationCase {
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
pub(super) struct StationVariableStatsAccumulator {
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

    pub(super) fn push_variable_case(
        &mut self,
        variable: &SurfaceMesoanalysisCalibrationStationVariableCase,
    ) {
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

    pub(super) fn finish_aggregate(self) -> SurfaceMesoanalysisCalibrationStationVariableAggregate {
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
pub(super) struct FinishedSourceSummary {
    pub(super) sampled_observation_count: usize,
    pub(super) variables: BTreeMap<String, SourceVariableStats>,
}

#[derive(Debug, Clone)]
pub(super) struct SourceVariableStats {
    pub(super) observation_count: usize,
    pub(super) mean_abs_background_error: Option<f64>,
    pub(super) mean_abs_analysis_error: Option<f64>,
    pub(super) background_rmse: Option<f64>,
    pub(super) analysis_rmse: Option<f64>,
    pub(super) confidence: Option<SurfaceMesoanalysisCalibrationConfidenceCase>,
}

impl SourceSummaryAccumulator {
    pub(super) fn push_source_summary(&mut self, summary: &Value) {
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

    pub(super) fn finish(&self) -> FinishedSourceSummary {
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
pub(super) struct SourceVariableStatsAccumulator {
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
    pub(super) fn push_variable_summary(&mut self, summary: &Value) {
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

    pub(super) fn finish(&self) -> SourceVariableStats {
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
