use super::helpers::{
    bool_at, f64_at, max, mean, min, option_delta, option_greater_than_zero, option_less_than_zero,
    push_f64, push_usize_as_f64, string_at, usize_at, value_at, weighted_mean,
};
use super::*;
use crate::mesoanalysis::{
    CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
    CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
};
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub(super) struct ConfidenceCaseAccumulator {
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
    pub(super) fn push_confidence_summary(&mut self, summary: &Value) {
        let Some(confidence) = parse_confidence_case(summary) else {
            return;
        };
        self.push(&confidence);
    }

    pub(super) fn push(&mut self, confidence: &SurfaceMesoanalysisCalibrationConfidenceCase) {
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

    pub(super) fn finish(&self) -> Option<SurfaceMesoanalysisCalibrationConfidenceCase> {
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

pub(super) fn parse_confidence_case(
    value: &Value,
) -> Option<SurfaceMesoanalysisCalibrationConfidenceCase> {
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
pub(super) struct ConfidenceAggregateAccumulator {
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
    pub(super) fn push(&mut self, confidence: &SurfaceMesoanalysisCalibrationConfidenceCase) {
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

    pub(super) fn finish(self) -> Option<SurfaceMesoanalysisCalibrationConfidenceAggregate> {
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
