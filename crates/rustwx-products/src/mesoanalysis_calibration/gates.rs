use super::*;
use std::collections::BTreeMap;

pub fn evaluate_surface_mesoanalysis_calibration_gate(
    report: &SurfaceMesoanalysisCalibrationReport,
    thresholds: SurfaceMesoanalysisCalibrationGateThresholds,
) -> SurfaceMesoanalysisCalibrationGate {
    let mut checks = Vec::new();
    push_min_calibration_gate_check(
        &mut checks,
        "loaded_case_count",
        "matrix",
        report.loaded_case_count as f64,
        thresholds.min_case_count as f64,
        format!(
            "{} loaded cases; minimum required is {}",
            report.loaded_case_count, thresholds.min_case_count
        ),
    );
    if !thresholds.allow_skipped_reports {
        push_max_calibration_gate_check(
            &mut checks,
            "skipped_report_count",
            "matrix",
            Some(report.skipped_reports.len() as f64),
            0.0,
            format!(
                "{} skipped reports; none allowed by this gate",
                report.skipped_reports.len()
            ),
        );
    }
    let disallowed_quality_flags = report
        .quality_flags
        .iter()
        .filter(|flag| !thresholds.allowed_quality_flags.contains(flag))
        .cloned()
        .collect::<Vec<_>>();
    checks.push(SurfaceMesoanalysisCalibrationGateCheck {
        name: "quality_flags".to_string(),
        scope: "matrix".to_string(),
        passed: disallowed_quality_flags.is_empty(),
        observed: Some(disallowed_quality_flags.len() as f64),
        threshold: Some(0.0),
        comparator: "<=".to_string(),
        message: if disallowed_quality_flags.is_empty() {
            "no disallowed quality flags".to_string()
        } else {
            format!(
                "disallowed quality flags present: {}",
                disallowed_quality_flags.join(", ")
            )
        },
    });
    if !thresholds.required_benchmark_modes.is_empty() {
        let missing_modes = thresholds
            .required_benchmark_modes
            .iter()
            .filter(|mode| !report.aggregate.benchmark_mode_counts.contains_key(*mode))
            .cloned()
            .collect::<Vec<_>>();
        checks.push(SurfaceMesoanalysisCalibrationGateCheck {
            name: "required_benchmark_modes".to_string(),
            scope: "matrix".to_string(),
            passed: missing_modes.is_empty(),
            observed: Some(missing_modes.len() as f64),
            threshold: Some(0.0),
            comparator: "<=".to_string(),
            message: if missing_modes.is_empty() {
                "all required benchmark modes are present".to_string()
            } else {
                format!(
                    "missing required benchmark modes: {}",
                    missing_modes.join(", ")
                )
            },
        });
    }
    if !thresholds.required_holdout_strategies.is_empty() {
        let missing_strategies = thresholds
            .required_holdout_strategies
            .iter()
            .filter(|strategy| {
                !report
                    .aggregate
                    .holdout_strategy_counts
                    .contains_key(*strategy)
            })
            .cloned()
            .collect::<Vec<_>>();
        checks.push(SurfaceMesoanalysisCalibrationGateCheck {
            name: "required_holdout_strategies".to_string(),
            scope: "matrix".to_string(),
            passed: missing_strategies.is_empty(),
            observed: Some(missing_strategies.len() as f64),
            threshold: Some(0.0),
            comparator: "<=".to_string(),
            message: if missing_strategies.is_empty() {
                "all required holdout strategies are present".to_string()
            } else {
                format!(
                    "missing required holdout strategies: {}",
                    missing_strategies.join(", ")
                )
            },
        });
    }
    if !thresholds.required_external_references.is_empty() {
        let missing_references = thresholds
            .required_external_references
            .iter()
            .filter(|label| !report.aggregate.external_references.contains_key(*label))
            .cloned()
            .collect::<Vec<_>>();
        checks.push(SurfaceMesoanalysisCalibrationGateCheck {
            name: "required_external_references".to_string(),
            scope: "matrix".to_string(),
            passed: missing_references.is_empty(),
            observed: Some(missing_references.len() as f64),
            threshold: Some(0.0),
            comparator: "<=".to_string(),
            message: if missing_references.is_empty() {
                "all required external references are present".to_string()
            } else {
                format!(
                    "missing required external references: {}",
                    missing_references.join(", ")
                )
            },
        });
    }
    if !thresholds.required_covariance_ablations.is_empty() {
        let missing_ablations = thresholds
            .required_covariance_ablations
            .iter()
            .filter(|label| !report.aggregate.covariance_ablations.contains_key(*label))
            .cloned()
            .collect::<Vec<_>>();
        checks.push(SurfaceMesoanalysisCalibrationGateCheck {
            name: "required_covariance_ablations".to_string(),
            scope: "matrix".to_string(),
            passed: missing_ablations.is_empty(),
            observed: Some(missing_ablations.len() as f64),
            threshold: Some(0.0),
            comparator: "<=".to_string(),
            message: if missing_ablations.is_empty() {
                "all required covariance ablations are present".to_string()
            } else {
                format!(
                    "missing required covariance ablations: {}",
                    missing_ablations.join(", ")
                )
            },
        });
    }
    if !thresholds.required_case_tags.is_empty() {
        let missing_tags = normalized_case_tags(&thresholds.required_case_tags)
            .into_iter()
            .filter(|tag| !report.aggregate.case_tag_counts.contains_key(tag))
            .collect::<Vec<_>>();
        checks.push(SurfaceMesoanalysisCalibrationGateCheck {
            name: "required_case_tags".to_string(),
            scope: "matrix".to_string(),
            passed: missing_tags.is_empty(),
            observed: Some(missing_tags.len() as f64),
            threshold: Some(0.0),
            comparator: "<=".to_string(),
            message: if missing_tags.is_empty() {
                "all required case tags are present".to_string()
            } else {
                format!("missing required case tags: {}", missing_tags.join(", "))
            },
        });
    }

    let variables =
        selected_gate_variable_names(&report.aggregate.variables, thresholds.variables.as_slice());
    if let Some(threshold) = thresholds.max_domain_candidate_minus_background_mae {
        push_domain_variable_threshold_checks(
            &mut checks,
            "domain_candidate_minus_background_mae",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |variable| variable.mean_candidate_minus_background_mae,
        );
    }
    if let Some(threshold) = thresholds.max_domain_candidate_minus_barnes_mae {
        push_domain_variable_threshold_checks(
            &mut checks,
            "domain_candidate_minus_barnes_mae",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |variable| variable.mean_candidate_minus_barnes_mae,
        );
    }
    if let Some(threshold) = thresholds.max_domain_candidate_minus_reference_mae {
        let reference_variables = selected_gate_reference_variable_names(
            &report.aggregate.external_references,
            thresholds.variables.as_slice(),
        );
        push_reference_variable_threshold_checks(
            &mut checks,
            "domain_candidate_minus_reference_mae",
            &report.aggregate.external_references,
            thresholds.required_external_references.as_slice(),
            reference_variables.as_slice(),
            threshold,
        );
    }
    if let Some(threshold) = thresholds.max_covariance_ablation_candidate_minus_baseline_mae {
        let ablation_variables = selected_gate_ablation_variable_names(
            &report.aggregate.covariance_ablations,
            thresholds.variables.as_slice(),
        );
        push_ablation_variable_threshold_checks(
            &mut checks,
            "covariance_ablation_candidate_minus_baseline_mae",
            &report.aggregate.covariance_ablations,
            thresholds.required_covariance_ablations.as_slice(),
            ablation_variables.as_slice(),
            threshold,
        );
    }
    if let Some(threshold) = thresholds.max_case_mesoanalysis_compute_ms {
        push_max_calibration_gate_check(
            &mut checks,
            "max_case_mesoanalysis_compute_ms",
            "matrix",
            report.aggregate.max_mesoanalysis_compute_ms,
            threshold,
            format!("maximum per-case mesoanalysis compute time must be <= {threshold:.0} ms"),
        );
    }
    if let Some(threshold) = thresholds.min_unique_case_signatures {
        push_min_calibration_gate_check(
            &mut checks,
            "unique_case_signature_count",
            "matrix",
            report.aggregate.case_signature_counts.len() as f64,
            threshold as f64,
            format!(
                "{} unique case signatures; minimum required is {threshold}",
                report.aggregate.case_signature_counts.len()
            ),
        );
    }
    if let Some(threshold) = thresholds.min_unique_dates {
        push_min_calibration_gate_check(
            &mut checks,
            "unique_date_count",
            "matrix",
            report.aggregate.date_counts.len() as f64,
            threshold as f64,
            format!(
                "{} unique dates; minimum required is {threshold}",
                report.aggregate.date_counts.len()
            ),
        );
    }
    if let Some(threshold) = thresholds.min_unique_cycles {
        push_min_calibration_gate_check(
            &mut checks,
            "unique_cycle_count",
            "matrix",
            report.aggregate.cycle_counts.len() as f64,
            threshold as f64,
            format!(
                "{} unique cycles; minimum required is {threshold}",
                report.aggregate.cycle_counts.len()
            ),
        );
    }
    if let Some(threshold) = thresholds.min_unique_forecast_hours {
        push_min_calibration_gate_check(
            &mut checks,
            "unique_forecast_hour_count",
            "matrix",
            report.aggregate.forecast_hour_counts.len() as f64,
            threshold as f64,
            format!(
                "{} unique forecast hours; minimum required is {threshold}",
                report.aggregate.forecast_hour_counts.len()
            ),
        );
    }
    if let Some(threshold) = thresholds.min_unique_case_tags {
        push_min_calibration_gate_check(
            &mut checks,
            "unique_case_tag_count",
            "matrix",
            report.aggregate.case_tag_counts.len() as f64,
            threshold as f64,
            format!(
                "{} unique case tags; minimum required is {threshold}",
                report.aggregate.case_tag_counts.len()
            ),
        );
    }
    if let Some(threshold) = thresholds.min_domain_low_confidence_observation_count {
        push_domain_confidence_min_threshold_checks(
            &mut checks,
            "domain_low_confidence_observation_count",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |confidence| confidence.min_low_confidence_observation_count,
            "minimum low-confidence observation count",
        );
    }
    if let Some(threshold) = thresholds.min_domain_high_confidence_observation_count {
        push_domain_confidence_min_threshold_checks(
            &mut checks,
            "domain_high_confidence_observation_count",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |confidence| confidence.min_high_confidence_observation_count,
            "minimum high-confidence observation count",
        );
    }
    if let Some(threshold) = thresholds.max_domain_high_minus_low_confidence_mae {
        push_domain_confidence_threshold_checks(
            &mut checks,
            "domain_high_minus_low_confidence_mae",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |confidence| confidence.worst_high_minus_low_confidence_mae,
            "worst high-minus-low confidence MAE",
            false,
        );
    }
    if let Some(threshold) = thresholds.max_domain_ranked_high_minus_low_confidence_mae {
        push_domain_confidence_threshold_checks(
            &mut checks,
            "domain_ranked_high_minus_low_confidence_mae",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |confidence| confidence.worst_ranked_high_minus_low_confidence_mae,
            "worst ranked high-minus-low confidence MAE",
            true,
        );
    }
    if let Some(threshold) = thresholds.max_domain_confidence_abs_error_correlation {
        push_domain_confidence_threshold_checks(
            &mut checks,
            "domain_confidence_abs_error_correlation",
            &report.aggregate.variables,
            variables.as_slice(),
            threshold,
            |confidence| confidence.worst_confidence_abs_error_correlation,
            "worst confidence-to-absolute-error correlation",
            false,
        );
    }
    let sources =
        selected_gate_source_names(&report.aggregate.sources, thresholds.sources.as_slice());
    if let Some(threshold) = thresholds.max_source_candidate_minus_background_mae {
        push_source_variable_threshold_checks(
            &mut checks,
            "source_candidate_minus_background_mae",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |variable| variable.mean_candidate_minus_background_mae,
        );
    }
    if let Some(threshold) = thresholds.max_source_candidate_minus_barnes_mae {
        push_source_variable_threshold_checks(
            &mut checks,
            "source_candidate_minus_barnes_mae",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |variable| variable.mean_candidate_minus_barnes_mae,
        );
    }
    if let Some(threshold) = thresholds.min_source_low_confidence_observation_count {
        push_source_confidence_min_threshold_checks(
            &mut checks,
            "source_low_confidence_observation_count",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |confidence| confidence.min_low_confidence_observation_count,
            "minimum low-confidence observation count",
        );
    }
    if let Some(threshold) = thresholds.min_source_high_confidence_observation_count {
        push_source_confidence_min_threshold_checks(
            &mut checks,
            "source_high_confidence_observation_count",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |confidence| confidence.min_high_confidence_observation_count,
            "minimum high-confidence observation count",
        );
    }
    if let Some(threshold) = thresholds.max_source_high_minus_low_confidence_mae {
        push_source_confidence_threshold_checks(
            &mut checks,
            "source_high_minus_low_confidence_mae",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |confidence| confidence.worst_high_minus_low_confidence_mae,
            "worst high-minus-low confidence MAE",
            false,
        );
    }
    if let Some(threshold) = thresholds.max_source_ranked_high_minus_low_confidence_mae {
        push_source_confidence_threshold_checks(
            &mut checks,
            "source_ranked_high_minus_low_confidence_mae",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |confidence| confidence.worst_ranked_high_minus_low_confidence_mae,
            "worst ranked high-minus-low confidence MAE",
            true,
        );
    }
    if let Some(threshold) = thresholds.max_source_confidence_abs_error_correlation {
        push_source_confidence_threshold_checks(
            &mut checks,
            "source_confidence_abs_error_correlation",
            &report.aggregate.sources,
            sources.as_slice(),
            variables.as_slice(),
            threshold,
            |confidence| confidence.worst_confidence_abs_error_correlation,
            "worst confidence-to-absolute-error correlation",
            false,
        );
    }
    let strata =
        selected_gate_stratum_names(&report.aggregate.strata, thresholds.strata.as_slice());
    let stratum_variables = selected_gate_stratum_variable_names(
        &report.aggregate.strata,
        thresholds.variables.as_slice(),
    );
    if let Some(threshold) = thresholds.max_stratum_candidate_minus_background_mae {
        push_stratum_variable_threshold_checks(
            &mut checks,
            "stratum_candidate_minus_background_mae",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |variable| variable.mean_candidate_minus_background_mae,
        );
    }
    if let Some(threshold) = thresholds.max_stratum_candidate_minus_barnes_mae {
        push_stratum_variable_threshold_checks(
            &mut checks,
            "stratum_candidate_minus_barnes_mae",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |variable| variable.mean_candidate_minus_barnes_mae,
        );
    }
    if let Some(threshold) = thresholds.min_stratum_low_confidence_observation_count {
        push_stratum_confidence_min_threshold_checks(
            &mut checks,
            "stratum_low_confidence_observation_count",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |confidence| confidence.min_low_confidence_observation_count,
            "minimum low-confidence observation count",
        );
    }
    if let Some(threshold) = thresholds.min_stratum_high_confidence_observation_count {
        push_stratum_confidence_min_threshold_checks(
            &mut checks,
            "stratum_high_confidence_observation_count",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |confidence| confidence.min_high_confidence_observation_count,
            "minimum high-confidence observation count",
        );
    }
    if let Some(threshold) = thresholds.max_stratum_high_minus_low_confidence_mae {
        push_stratum_confidence_threshold_checks(
            &mut checks,
            "stratum_high_minus_low_confidence_mae",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |confidence| confidence.worst_high_minus_low_confidence_mae,
            "worst high-minus-low confidence MAE",
            false,
        );
    }
    if let Some(threshold) = thresholds.max_stratum_ranked_high_minus_low_confidence_mae {
        push_stratum_confidence_threshold_checks(
            &mut checks,
            "stratum_ranked_high_minus_low_confidence_mae",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |confidence| confidence.worst_ranked_high_minus_low_confidence_mae,
            "worst ranked high-minus-low confidence MAE",
            true,
        );
    }
    if let Some(threshold) = thresholds.max_stratum_confidence_abs_error_correlation {
        push_stratum_confidence_threshold_checks(
            &mut checks,
            "stratum_confidence_abs_error_correlation",
            &report.aggregate.strata,
            strata.as_slice(),
            stratum_variables.as_slice(),
            threshold,
            |confidence| confidence.worst_confidence_abs_error_correlation,
            "worst confidence-to-absolute-error correlation",
            false,
        );
    }

    let stations =
        selected_gate_station_names(&report.aggregate.stations, thresholds.stations.as_slice());
    let station_variables = selected_gate_station_variable_names(
        &report.aggregate.stations,
        thresholds.variables.as_slice(),
    );
    if let Some(threshold) = thresholds.min_station_observation_count {
        push_station_variable_min_threshold_checks(
            &mut checks,
            "station_observation_count",
            &report.aggregate.stations,
            stations.as_slice(),
            station_variables.as_slice(),
            threshold,
            |variable| Some(variable.observation_count as f64),
            "observation count",
        );
    }
    if let Some(threshold) = thresholds.max_station_candidate_minus_background_mae {
        push_station_variable_threshold_checks(
            &mut checks,
            "station_candidate_minus_background_mae",
            &report.aggregate.stations,
            stations.as_slice(),
            station_variables.as_slice(),
            threshold,
            |variable| {
                option_delta(
                    variable.mean_abs_analysis_error,
                    variable.mean_abs_background_error,
                )
            },
            "candidate-minus-background MAE",
        );
    }
    if let Some(threshold) = thresholds.max_station_analysis_mae {
        push_station_variable_threshold_checks(
            &mut checks,
            "station_analysis_mae",
            &report.aggregate.stations,
            stations.as_slice(),
            station_variables.as_slice(),
            threshold,
            |variable| variable.mean_abs_analysis_error,
            "analysis MAE",
        );
    }
    if let Some(threshold) = thresholds.max_station_abs_analysis_bias {
        push_station_variable_threshold_checks(
            &mut checks,
            "station_abs_analysis_bias",
            &report.aggregate.stations,
            stations.as_slice(),
            station_variables.as_slice(),
            threshold,
            |variable| variable.mean_analysis_error.map(f64::abs),
            "absolute analysis bias",
        );
    }

    SurfaceMesoanalysisCalibrationGate {
        schema: "rustwx.surface_mesoanalysis.calibration_gate.v1".to_string(),
        passed: checks.iter().all(|check| check.passed),
        thresholds,
        checks,
    }
}

fn selected_gate_variable_names(
    available: &BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
    requested: &[String],
) -> Vec<String> {
    if requested.is_empty() {
        available.keys().cloned().collect()
    } else {
        requested.to_vec()
    }
}

fn selected_gate_source_names(
    available: &BTreeMap<String, SurfaceMesoanalysisCalibrationSourceAggregate>,
    requested: &[String],
) -> Vec<String> {
    if requested.is_empty() {
        available.keys().cloned().collect()
    } else {
        requested.to_vec()
    }
}

fn selected_gate_stratum_names(
    available: &BTreeMap<String, SurfaceMesoanalysisCalibrationStratumAggregate>,
    requested: &[String],
) -> Vec<String> {
    if requested.is_empty() {
        available.keys().cloned().collect()
    } else {
        requested.to_vec()
    }
}

fn selected_gate_stratum_variable_names(
    strata: &BTreeMap<String, SurfaceMesoanalysisCalibrationStratumAggregate>,
    requested: &[String],
) -> Vec<String> {
    if !requested.is_empty() {
        return requested.to_vec();
    }
    let mut names = strata
        .values()
        .flat_map(|stratum| stratum.variables.keys().cloned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn selected_gate_station_names(
    available: &BTreeMap<String, SurfaceMesoanalysisCalibrationStationAggregate>,
    requested: &[String],
) -> Vec<String> {
    if requested.is_empty() {
        available.keys().cloned().collect()
    } else {
        requested.to_vec()
    }
}

fn selected_gate_station_variable_names(
    stations: &BTreeMap<String, SurfaceMesoanalysisCalibrationStationAggregate>,
    requested: &[String],
) -> Vec<String> {
    if !requested.is_empty() {
        return requested.to_vec();
    }
    let mut names = stations
        .values()
        .flat_map(|station| station.variables.keys().cloned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn selected_gate_reference_variable_names(
    references: &BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceAggregate>,
    requested: &[String],
) -> Vec<String> {
    if !requested.is_empty() {
        return requested.to_vec();
    }
    let mut names = references
        .values()
        .flat_map(|reference| reference.variables.keys().cloned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn selected_gate_ablation_variable_names(
    ablations: &BTreeMap<String, SurfaceMesoanalysisCalibrationAblationAggregate>,
    requested: &[String],
) -> Vec<String> {
    if !requested.is_empty() {
        return requested.to_vec();
    }
    let mut names = ablations
        .values()
        .flat_map(|ablation| ablation.variables.keys().cloned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn push_domain_variable_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    variables: &BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationVariableAggregate) -> Option<f64>,
) {
    for variable_name in selected_variables {
        let Some(variable) = variables.get(variable_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("domain/{variable_name}"),
            ));
            continue;
        };
        let observed = selector(variable);
        push_max_calibration_gate_check(
            checks,
            name,
            format!("domain/{variable_name}"),
            observed,
            threshold,
            format!(
                "domain {variable_name} candidate-minus-comparator MAE must be <= {threshold:.3}"
            ),
        );
    }
}

fn push_source_variable_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    sources: &BTreeMap<String, SurfaceMesoanalysisCalibrationSourceAggregate>,
    selected_sources: &[String],
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationVariableAggregate) -> Option<f64>,
) {
    for source_name in selected_sources {
        let Some(source) = sources.get(source_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("source/{source_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("source/{source_name}/{variable_name}");
            let Some(variable) = source.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let observed = selector(variable);
            push_max_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold,
                format!(
                    "source {source_name} {variable_name} candidate-minus-comparator MAE must be <= {threshold:.3}"
                ),
            );
        }
    }
}

fn push_stratum_variable_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    strata: &BTreeMap<String, SurfaceMesoanalysisCalibrationStratumAggregate>,
    selected_strata: &[String],
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationVariableAggregate) -> Option<f64>,
) {
    for stratum_name in selected_strata {
        let Some(stratum) = strata.get(stratum_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("stratum/{stratum_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("stratum/{stratum_name}/{variable_name}");
            let Some(variable) = stratum.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let observed = selector(variable);
            push_max_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold,
                format!(
                    "stratum {stratum_name} {variable_name} candidate-minus-comparator MAE must be <= {threshold:.3}"
                ),
            );
        }
    }
}

fn push_station_variable_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    stations: &BTreeMap<String, SurfaceMesoanalysisCalibrationStationAggregate>,
    selected_stations: &[String],
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationStationVariableAggregate) -> Option<f64>,
    metric_label: &str,
) {
    for station_name in selected_stations {
        let Some(station) = stations.get(station_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("station/{station_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("station/{station_name}/{variable_name}");
            let Some(variable) = station.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let observed = selector(variable);
            push_max_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold,
                format!(
                    "station {station_name} {variable_name} {metric_label} must be <= {threshold:.3}"
                ),
            );
        }
    }
}

fn push_station_variable_min_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    stations: &BTreeMap<String, SurfaceMesoanalysisCalibrationStationAggregate>,
    selected_stations: &[String],
    selected_variables: &[String],
    threshold: usize,
    selector: fn(&SurfaceMesoanalysisCalibrationStationVariableAggregate) -> Option<f64>,
    metric_label: &str,
) {
    for station_name in selected_stations {
        let Some(station) = stations.get(station_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("station/{station_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("station/{station_name}/{variable_name}");
            let Some(variable) = station.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let observed = selector(variable);
            push_optional_min_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold as f64,
                format!(
                    "station {station_name} {variable_name} {metric_label} must be >= {threshold}"
                ),
            );
        }
    }
}

fn push_domain_confidence_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    variables: &BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationConfidenceAggregate) -> Option<f64>,
    metric_label: &str,
    require_reliability_passed: bool,
) {
    for variable_name in selected_variables {
        let scope = format!("domain/{variable_name}");
        let Some(variable) = variables.get(variable_name) else {
            checks.push(missing_calibration_gate_check(name, scope));
            continue;
        };
        let confidence = variable.confidence.as_ref();
        let observed = confidence.and_then(selector);
        push_max_confidence_calibration_gate_check(
            checks,
            name,
            scope,
            observed,
            threshold,
            format!("domain {variable_name} {metric_label} must be <= {threshold:.3}"),
            confidence,
            require_reliability_passed,
        );
    }
}

fn push_domain_confidence_min_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    variables: &BTreeMap<String, SurfaceMesoanalysisCalibrationVariableAggregate>,
    selected_variables: &[String],
    threshold: usize,
    selector: fn(&SurfaceMesoanalysisCalibrationConfidenceAggregate) -> Option<f64>,
    metric_label: &str,
) {
    for variable_name in selected_variables {
        let scope = format!("domain/{variable_name}");
        let Some(variable) = variables.get(variable_name) else {
            checks.push(missing_calibration_gate_check(name, scope));
            continue;
        };
        let observed = variable.confidence.as_ref().and_then(selector);
        push_optional_min_calibration_gate_check(
            checks,
            name,
            scope,
            observed,
            threshold as f64,
            format!("domain {variable_name} {metric_label} must be >= {threshold}"),
        );
    }
}

fn push_source_confidence_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    sources: &BTreeMap<String, SurfaceMesoanalysisCalibrationSourceAggregate>,
    selected_sources: &[String],
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationConfidenceAggregate) -> Option<f64>,
    metric_label: &str,
    require_reliability_passed: bool,
) {
    for source_name in selected_sources {
        let Some(source) = sources.get(source_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("source/{source_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("source/{source_name}/{variable_name}");
            let Some(variable) = source.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let confidence = variable.confidence.as_ref();
            let observed = confidence.and_then(selector);
            push_max_confidence_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold,
                format!(
                    "source {source_name} {variable_name} {metric_label} must be <= {threshold:.3}"
                ),
                confidence,
                require_reliability_passed,
            );
        }
    }
}

fn push_stratum_confidence_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    strata: &BTreeMap<String, SurfaceMesoanalysisCalibrationStratumAggregate>,
    selected_strata: &[String],
    selected_variables: &[String],
    threshold: f64,
    selector: fn(&SurfaceMesoanalysisCalibrationConfidenceAggregate) -> Option<f64>,
    metric_label: &str,
    require_reliability_passed: bool,
) {
    for stratum_name in selected_strata {
        let Some(stratum) = strata.get(stratum_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("stratum/{stratum_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("stratum/{stratum_name}/{variable_name}");
            let Some(variable) = stratum.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let confidence = variable.confidence.as_ref();
            let observed = confidence.and_then(selector);
            push_max_confidence_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold,
                format!(
                    "stratum {stratum_name} {variable_name} {metric_label} must be <= {threshold:.3}"
                ),
                confidence,
                require_reliability_passed,
            );
        }
    }
}

fn push_source_confidence_min_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    sources: &BTreeMap<String, SurfaceMesoanalysisCalibrationSourceAggregate>,
    selected_sources: &[String],
    selected_variables: &[String],
    threshold: usize,
    selector: fn(&SurfaceMesoanalysisCalibrationConfidenceAggregate) -> Option<f64>,
    metric_label: &str,
) {
    for source_name in selected_sources {
        let Some(source) = sources.get(source_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("source/{source_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("source/{source_name}/{variable_name}");
            let Some(variable) = source.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let observed = variable.confidence.as_ref().and_then(selector);
            push_optional_min_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold as f64,
                format!(
                    "source {source_name} {variable_name} {metric_label} must be >= {threshold}"
                ),
            );
        }
    }
}

fn push_stratum_confidence_min_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    strata: &BTreeMap<String, SurfaceMesoanalysisCalibrationStratumAggregate>,
    selected_strata: &[String],
    selected_variables: &[String],
    threshold: usize,
    selector: fn(&SurfaceMesoanalysisCalibrationConfidenceAggregate) -> Option<f64>,
    metric_label: &str,
) {
    for stratum_name in selected_strata {
        let Some(stratum) = strata.get(stratum_name) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("stratum/{stratum_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("stratum/{stratum_name}/{variable_name}");
            let Some(variable) = stratum.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            let observed = variable.confidence.as_ref().and_then(selector);
            push_optional_min_calibration_gate_check(
                checks,
                name,
                scope,
                observed,
                threshold as f64,
                format!(
                    "stratum {stratum_name} {variable_name} {metric_label} must be >= {threshold}"
                ),
            );
        }
    }
}

fn push_ablation_variable_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    ablations: &BTreeMap<String, SurfaceMesoanalysisCalibrationAblationAggregate>,
    requested_ablations: &[String],
    selected_variables: &[String],
    threshold: f64,
) {
    let ablation_names = if requested_ablations.is_empty() {
        ablations.keys().cloned().collect::<Vec<_>>()
    } else {
        requested_ablations.to_vec()
    };
    if ablation_names.is_empty() {
        checks.push(missing_calibration_gate_check(name, "covariance_ablation"));
        return;
    }
    for ablation_name in ablation_names {
        let Some(ablation) = ablations.get(ablation_name.as_str()) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("covariance_ablation/{ablation_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("covariance_ablation/{ablation_name}/{variable_name}");
            let Some(variable) = ablation.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            push_max_calibration_gate_check(
                checks,
                name,
                scope,
                variable.mean_candidate_minus_baseline_mae,
                threshold,
                format!(
                    "covariance ablation {ablation_name} {variable_name} candidate-minus-baseline MAE must be <= {threshold:.3}"
                ),
            );
        }
    }
}

fn push_reference_variable_threshold_checks(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: &str,
    references: &BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceAggregate>,
    requested_references: &[String],
    selected_variables: &[String],
    threshold: f64,
) {
    let reference_names = if requested_references.is_empty() {
        references.keys().cloned().collect::<Vec<_>>()
    } else {
        requested_references.to_vec()
    };
    if reference_names.is_empty() {
        checks.push(missing_calibration_gate_check(name, "external_reference"));
        return;
    }
    for reference_name in reference_names {
        let Some(reference) = references.get(reference_name.as_str()) else {
            checks.push(missing_calibration_gate_check(
                name,
                format!("external_reference/{reference_name}"),
            ));
            continue;
        };
        for variable_name in selected_variables {
            let scope = format!("external_reference/{reference_name}/{variable_name}");
            let Some(variable) = reference.variables.get(variable_name) else {
                checks.push(missing_calibration_gate_check(name, scope));
                continue;
            };
            push_max_calibration_gate_check(
                checks,
                name,
                scope,
                variable.mean_candidate_minus_reference_mae,
                threshold,
                format!(
                    "external reference {reference_name} {variable_name} candidate-minus-reference MAE must be <= {threshold:.3}"
                ),
            );
        }
    }
}

fn push_min_calibration_gate_check(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: impl Into<String>,
    scope: impl Into<String>,
    observed: f64,
    threshold: f64,
    message: String,
) {
    checks.push(SurfaceMesoanalysisCalibrationGateCheck {
        name: name.into(),
        scope: scope.into(),
        passed: observed >= threshold,
        observed: Some(observed),
        threshold: Some(threshold),
        comparator: ">=".to_string(),
        message,
    });
}

fn push_optional_min_calibration_gate_check(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: impl Into<String>,
    scope: impl Into<String>,
    observed: Option<f64>,
    threshold: f64,
    message: String,
) {
    checks.push(SurfaceMesoanalysisCalibrationGateCheck {
        name: name.into(),
        scope: scope.into(),
        passed: observed.map(|value| value >= threshold).unwrap_or(false),
        observed,
        threshold: Some(threshold),
        comparator: ">=".to_string(),
        message,
    });
}

fn push_max_calibration_gate_check(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: impl Into<String>,
    scope: impl Into<String>,
    observed: Option<f64>,
    threshold: f64,
    message: String,
) {
    checks.push(SurfaceMesoanalysisCalibrationGateCheck {
        name: name.into(),
        scope: scope.into(),
        passed: observed.map(|value| value <= threshold).unwrap_or(false),
        observed,
        threshold: Some(threshold),
        comparator: "<=".to_string(),
        message,
    });
}

pub(super) fn push_max_confidence_calibration_gate_check(
    checks: &mut Vec<SurfaceMesoanalysisCalibrationGateCheck>,
    name: impl Into<String>,
    scope: impl Into<String>,
    observed: Option<f64>,
    threshold: f64,
    mut message: String,
    confidence: Option<&SurfaceMesoanalysisCalibrationConfidenceAggregate>,
    require_reliability_passed: bool,
) {
    let reliability_passed = !require_reliability_passed
        || confidence
            .map(|confidence| confidence_reliability_aggregate_passed(&confidence.reliability))
            .unwrap_or(false);
    if require_reliability_passed {
        if let Some(confidence) = confidence {
            let reliability = &confidence.reliability;
            message.push_str(&format!(
                "; confidence reliability status={}, semantic_label={}, bucket_coverage_sufficient={}, passed_cases={}, failed_cases={}, untestable_cases={}",
                reliability.status,
                reliability.semantic_label,
                reliability.bucket_coverage_sufficient,
                reliability.passed_case_count,
                reliability.failed_case_count,
                reliability.untestable_case_count
            ));
        } else {
            message.push_str("; confidence reliability missing");
        }
    }
    checks.push(SurfaceMesoanalysisCalibrationGateCheck {
        name: name.into(),
        scope: scope.into(),
        passed: observed.map(|value| value <= threshold).unwrap_or(false) && reliability_passed,
        observed,
        threshold: Some(threshold),
        comparator: if require_reliability_passed {
            "<= and reliability=passed".to_string()
        } else {
            "<=".to_string()
        },
        message,
    });
}

fn confidence_reliability_aggregate_passed(
    reliability: &SurfaceMesoanalysisCalibrationConfidenceReliabilityAggregate,
) -> bool {
    reliability.status == "passed"
        && reliability.semantic_label == "calibrated_reliability"
        && reliability.bucket_coverage_sufficient
}

fn missing_calibration_gate_check(
    name: impl Into<String>,
    scope: impl Into<String>,
) -> SurfaceMesoanalysisCalibrationGateCheck {
    SurfaceMesoanalysisCalibrationGateCheck {
        name: name.into(),
        scope: scope.into(),
        passed: false,
        observed: None,
        threshold: None,
        comparator: "present".to_string(),
        message: "required calibration metric was not available".to_string(),
    }
}
