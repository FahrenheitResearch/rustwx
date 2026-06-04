use super::confidence::parse_confidence_case;
use super::*;
use chrono::Utc;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub(super) fn build_surface_mesoanalysis_calibration_report_from_values(
    values: impl IntoIterator<Item = (PathBuf, Result<Value, String>)>,
) -> SurfaceMesoanalysisCalibrationReport {
    let mut requested_report_count = 0usize;
    let mut cases = Vec::new();
    let mut skipped_reports = Vec::new();

    for (path, value) in values {
        requested_report_count += 1;
        match value.and_then(|value| parse_calibration_case(path.clone(), &value)) {
            Ok(case) => cases.push(case),
            Err(reason) => {
                skipped_reports.push(SurfaceMesoanalysisCalibrationSkippedReport { path, reason })
            }
        }
    }

    let aggregate = aggregate_calibration_cases(&cases);
    let quality_flags = calibration_quality_flags(&cases, &skipped_reports, &aggregate);
    SurfaceMesoanalysisCalibrationReport {
        schema: "rustwx.surface_mesoanalysis.calibration_matrix.v1".to_string(),
        generated_at: Utc::now().to_rfc3339(),
        requested_report_count,
        loaded_case_count: cases.len(),
        skipped_reports,
        quality_flags,
        cases,
        aggregate,
        calibration_gate: None,
    }
}

fn parse_calibration_case(
    path: PathBuf,
    value: &Value,
) -> Result<SurfaceMesoanalysisCalibrationCase, String> {
    let run_schema = string_at(value, &["schema"]).unwrap_or_default();
    if run_schema != "rustwx.surface_mesoanalysis.run_report.v1" {
        return Err(format!(
            "unsupported run schema '{}'; expected rustwx.surface_mesoanalysis.run_report.v1",
            if run_schema.is_empty() {
                "<missing>"
            } else {
                run_schema.as_str()
            }
        ));
    }
    let external_references = extract_external_reference_cases(value);
    let covariance_ablations = extract_covariance_ablation_cases(value);
    let (benchmark_mode, variables, sources, strata, stations, benchmark_fold_count) =
        match select_benchmark(value) {
            Ok((benchmark_mode, benchmark)) => {
                let mut variables = extract_benchmark_variables(benchmark);
                if variables.is_empty() {
                    return Err("selected benchmark contained no variable summaries".to_string());
                }
                merge_candidate_validation_confidence(
                    value,
                    benchmark_mode.as_str(),
                    &mut variables,
                );
                let sources = extract_source_cases(value, benchmark_mode.as_str());
                let strata = extract_strata_cases(value, benchmark_mode.as_str());
                let stations = extract_station_cases(value, benchmark_mode.as_str());
                (
                    benchmark_mode,
                    variables,
                    sources,
                    strata,
                    stations,
                    usize_at(benchmark, &["fold_count"]),
                )
            }
            Err(_reason) if !external_references.is_empty() || !covariance_ablations.is_empty() => {
                let benchmark_mode =
                    auxiliary_validation_mode(&external_references, &covariance_ablations);
                let variables =
                    extract_candidate_validation_variables(value, benchmark_mode.as_str());
                let sources = extract_source_cases(value, benchmark_mode.as_str());
                let strata = extract_strata_cases(value, benchmark_mode.as_str());
                let stations = extract_station_cases(value, benchmark_mode.as_str());
                (benchmark_mode, variables, sources, strata, stations, None)
            }
            Err(reason) => return Err(reason),
        };

    Ok(SurfaceMesoanalysisCalibrationCase {
        source_path: path,
        run_schema,
        model: string_at(value, &["model"]).unwrap_or_default(),
        model_source: string_at(value, &["model_source"]).unwrap_or_default(),
        model_cycle: string_at(value, &["model_cycle"]).unwrap_or_default(),
        date: string_at(value, &["date"]).unwrap_or_default(),
        cycle: u64_at(value, &["cycle"]).and_then(|value| u8::try_from(value).ok()),
        forecast_hour: u64_at(value, &["forecast_hour"])
            .and_then(|value| u16::try_from(value).ok())
            .unwrap_or(0),
        model_load_mode: string_at(value, &["model_load_mode"]).unwrap_or_default(),
        case_tags: normalized_case_tags(&string_vec_at(value, &["case_tags"])),
        analysis_method: string_at(value, &["mesoanalysis_config", "method"]).unwrap_or_default(),
        covariance_kernel: string_at(value, &["mesoanalysis_config", "oi_covariance_kernel"]),
        holdout_strategy: string_at(
            value,
            &["mesoanalysis", "repeated_holdout_validation", "strategy"],
        )
        .or_else(|| string_at(value, &["mesoanalysis", "holdout_validation", "strategy"])),
        repeated_fold_count: usize_at(
            value,
            &[
                "mesoanalysis",
                "repeated_holdout_validation",
                "completed_fold_count",
            ],
        )
        .or(benchmark_fold_count),
        validation_gate_passed: bool_at(value, &["validation_gate", "passed"]),
        observation_count: usize_at(value, &["mesoanalysis", "observation_count"]),
        source_count: usize_at(value, &["mesoanalysis", "source_count"]),
        grid_export_field_count: usize_at(value, &["grid_export_field_count"]),
        mesoanalysis_compute_ms: u64_at(value, &["mesoanalysis_compute_ms"]).map(u128::from),
        benchmark_mode,
        diagnostics: extract_diagnostic_cases(value),
        variables,
        sources,
        strata,
        stations,
        external_references,
        covariance_ablations,
    })
}

fn extract_diagnostic_cases(
    value: &Value,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationDiagnosticCase> {
    let Some(diagnostics) =
        value_at(value, &["mesoanalysis", "diagnostics"]).and_then(Value::as_array)
    else {
        return BTreeMap::new();
    };
    diagnostics
        .iter()
        .filter_map(|diagnostic| {
            let variable = string_at(diagnostic, &["variable"])?;
            Some((
                variable,
                SurfaceMesoanalysisCalibrationDiagnosticCase {
                    candidate_observations: usize_at(diagnostic, &["candidate_observations"])
                        .unwrap_or(0),
                    accepted_observations: usize_at(diagnostic, &["accepted_observations"])
                        .unwrap_or(0),
                    rejected_observations: usize_at(diagnostic, &["rejected_observations"])
                        .unwrap_or(0),
                    gross_error_rescued_observations: usize_at(
                        diagnostic,
                        &["gross_error_rescued_observations"],
                    )
                    .unwrap_or(0),
                    covered_grid_cells: usize_at(diagnostic, &["covered_grid_cells"]).unwrap_or(0),
                    solver_failed_grid_cells: usize_at(diagnostic, &["solver_failed_grid_cells"])
                        .unwrap_or(0),
                    truncated_neighbor_grid_cells: usize_at(
                        diagnostic,
                        &["truncated_neighbor_grid_cells"],
                    )
                    .unwrap_or(0),
                },
            ))
        })
        .collect()
}

fn select_benchmark<'a>(value: &'a Value) -> Result<(String, &'a Value), String> {
    let repeated = value
        .get("barnes_baseline_comparison")
        .and_then(|comparison| comparison.get("repeated_holdout_benchmark_summary"));
    if let Some(benchmark) = non_null_object(repeated) {
        return Ok((
            string_at(benchmark, &["validation_mode"])
                .unwrap_or_else(|| "repeated_holdout_validation".to_string()),
            benchmark,
        ));
    }

    let holdout = value
        .get("barnes_baseline_comparison")
        .and_then(|comparison| comparison.get("holdout_benchmark_summary"));
    if let Some(benchmark) = non_null_object(holdout) {
        return Ok((
            string_at(benchmark, &["validation_mode"])
                .unwrap_or_else(|| "holdout_validation".to_string()),
            benchmark,
        ));
    }

    let same_observation = value
        .get("barnes_baseline_comparison")
        .and_then(|comparison| comparison.get("benchmark_summary"));
    if let Some(benchmark) = non_null_object(same_observation) {
        return Ok((
            string_at(benchmark, &["validation_mode"])
                .unwrap_or_else(|| "same_observation_validation".to_string()),
            benchmark,
        ));
    }

    Err("run report has no Barnes baseline benchmark summary".to_string())
}

fn external_reference_validation_mode(
    references: &BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceCase>,
) -> String {
    let mut modes = references
        .values()
        .map(|reference| reference.validation_mode.as_str())
        .filter(|mode| !mode.is_empty());
    let Some(first) = modes.next() else {
        return "external_reference_validation".to_string();
    };
    if modes.all(|mode| mode == first) {
        first.to_string()
    } else {
        "mixed_external_reference_validation".to_string()
    }
}

fn auxiliary_validation_mode(
    references: &BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceCase>,
    ablations: &BTreeMap<String, SurfaceMesoanalysisCalibrationAblationCase>,
) -> String {
    if !references.is_empty() && ablations.is_empty() {
        return external_reference_validation_mode(references);
    }
    let mut modes = references
        .values()
        .map(|reference| reference.validation_mode.as_str())
        .chain(
            ablations
                .values()
                .map(|ablation| ablation.validation_mode.as_str()),
        )
        .filter(|mode| !mode.is_empty());
    let Some(first) = modes.next() else {
        return "auxiliary_validation".to_string();
    };
    if modes.all(|mode| mode == first) {
        first.to_string()
    } else {
        "mixed_auxiliary_validation".to_string()
    }
}

fn non_null_object(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| value.is_object())
}

fn extract_benchmark_variables(
    benchmark: &Value,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase> {
    let mut variables = BTreeMap::new();
    for name in [
        "temperature_c",
        "dewpoint_c",
        "wind_speed_ms",
        "mean_sea_level_pressure_hpa",
    ] {
        let Some(value) = benchmark.get(name).filter(|value| value.is_object()) else {
            continue;
        };
        variables.insert(name.to_string(), parse_variable_case(value));
    }
    variables
}

fn parse_variable_case(value: &Value) -> SurfaceMesoanalysisCalibrationVariableCase {
    SurfaceMesoanalysisCalibrationVariableCase {
        observation_count: usize_at(value, &["candidate_observation_count"]),
        fold_count: usize_at(value, &["fold_count"]),
        background_mean_abs_error: f64_at(value, &["background_mean_abs_error"]),
        candidate_mean_abs_error: f64_at(value, &["candidate_mean_abs_error"]),
        barnes_mean_abs_error: f64_at(value, &["baseline_mean_abs_error"]),
        candidate_minus_background_mae: f64_at(value, &["candidate_minus_background_mae"]),
        candidate_minus_barnes_mae: f64_at(value, &["candidate_minus_baseline_mae"]),
        background_rmse: f64_at(value, &["background_rmse"]),
        candidate_rmse: f64_at(value, &["candidate_rmse"]),
        barnes_rmse: f64_at(value, &["baseline_rmse"]),
        candidate_minus_background_rmse: f64_at(value, &["candidate_minus_background_rmse"]),
        candidate_minus_barnes_rmse: f64_at(value, &["candidate_minus_baseline_rmse"]),
        mae_winner: string_at(value, &["mae_winner"]),
        rmse_winner: string_at(value, &["rmse_winner"]),
        candidate_beats_background_mae_fold_count: usize_at(
            value,
            &["candidate_beats_background_mae_fold_count"],
        ),
        candidate_beats_barnes_mae_fold_count: usize_at(
            value,
            &["candidate_beats_baseline_mae_fold_count"],
        ),
        candidate_beats_background_rmse_fold_count: usize_at(
            value,
            &["candidate_beats_background_rmse_fold_count"],
        ),
        candidate_beats_barnes_rmse_fold_count: usize_at(
            value,
            &["candidate_beats_baseline_rmse_fold_count"],
        ),
        confidence: value_at(value, &["confidence"]).and_then(parse_confidence_case),
    }
}

fn merge_candidate_validation_confidence(
    value: &Value,
    benchmark_mode: &str,
    variables: &mut BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase>,
) {
    let validation_variables = selected_candidate_variable_stats(value, benchmark_mode);
    for (name, stats) in validation_variables {
        let Some(variable) = variables.get_mut(name.as_str()) else {
            continue;
        };
        if variable.confidence.is_none() {
            variable.confidence = stats.confidence;
        }
    }
}

fn extract_candidate_validation_variables(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationVariableCase> {
    selected_candidate_variable_stats(value, benchmark_mode)
        .into_iter()
        .map(|(name, stats)| {
            let variable = SurfaceMesoanalysisCalibrationVariableCase {
                observation_count: Some(stats.observation_count),
                fold_count: None,
                background_mean_abs_error: stats.mean_abs_background_error,
                candidate_mean_abs_error: stats.mean_abs_analysis_error,
                barnes_mean_abs_error: None,
                candidate_minus_background_mae: option_delta(
                    stats.mean_abs_analysis_error,
                    stats.mean_abs_background_error,
                ),
                candidate_minus_barnes_mae: None,
                background_rmse: stats.background_rmse,
                candidate_rmse: stats.analysis_rmse,
                barnes_rmse: None,
                candidate_minus_background_rmse: option_delta(
                    stats.analysis_rmse,
                    stats.background_rmse,
                ),
                candidate_minus_barnes_rmse: None,
                mae_winner: None,
                rmse_winner: None,
                candidate_beats_background_mae_fold_count: None,
                candidate_beats_barnes_mae_fold_count: None,
                candidate_beats_background_rmse_fold_count: None,
                candidate_beats_barnes_rmse_fold_count: None,
                confidence: stats.confidence,
            };
            (name, variable)
        })
        .collect()
}

fn extract_external_reference_cases(
    value: &Value,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceCase> {
    let mut references = BTreeMap::new();
    if let Some(reference) = value.get("external_reference_comparison") {
        push_external_reference_case(reference, &mut references);
    }
    if let Some(reference) = value.get("external_reference") {
        push_external_reference_case(reference, &mut references);
    }
    if let Some(reference_array) = value
        .get("external_reference_comparisons")
        .and_then(Value::as_array)
    {
        for reference in reference_array {
            push_external_reference_case(reference, &mut references);
        }
    }
    if let Some(reference_map) = value
        .get("external_reference_comparisons")
        .and_then(Value::as_object)
    {
        for (label, reference) in reference_map {
            let mut case = parse_external_reference_case(reference);
            if case.reference_label.is_empty() {
                case.reference_label = label.clone();
            }
            if !case.variables.is_empty() {
                references.insert(case.reference_label.clone(), case);
            }
        }
    }
    references
}

fn push_external_reference_case(
    value: &Value,
    references: &mut BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceCase>,
) {
    let case = parse_external_reference_case(value);
    if !case.reference_label.is_empty() && !case.variables.is_empty() {
        references.insert(case.reference_label.clone(), case);
    }
}

fn parse_external_reference_case(value: &Value) -> SurfaceMesoanalysisCalibrationReferenceCase {
    let reference_label = string_at(value, &["reference_label"])
        .or_else(|| string_at(value, &["baseline_label"]))
        .or_else(|| string_at(value, &["label"]))
        .unwrap_or_default();
    let validation_mode = string_at(value, &["validation_mode"])
        .or_else(|| string_at(value, &["mode"]))
        .unwrap_or_else(|| "external_reference_validation".to_string());
    let variable_root = value.get("variables").unwrap_or(value);
    SurfaceMesoanalysisCalibrationReferenceCase {
        reference_label,
        validation_mode,
        variables: extract_external_reference_variables(variable_root),
    }
}

fn extract_external_reference_variables(
    value: &Value,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationReferenceVariableCase> {
    let mut variables = BTreeMap::new();
    for name in [
        "temperature_c",
        "dewpoint_c",
        "wind_speed_ms",
        "mean_sea_level_pressure_hpa",
    ] {
        let Some(variable) = value.get(name).filter(|value| value.is_object()) else {
            continue;
        };
        variables.insert(
            name.to_string(),
            parse_external_reference_variable(variable),
        );
    }
    variables
}

fn parse_external_reference_variable(
    value: &Value,
) -> SurfaceMesoanalysisCalibrationReferenceVariableCase {
    let candidate_mean_abs_error = f64_at(value, &["candidate_mean_abs_error"])
        .or_else(|| f64_at(value, &["candidate_mean_abs_analysis_error"]));
    let reference_mean_abs_error = f64_at(value, &["reference_mean_abs_error"])
        .or_else(|| f64_at(value, &["reference_mean_abs_analysis_error"]))
        .or_else(|| f64_at(value, &["baseline_mean_abs_error"]));
    let candidate_rmse =
        f64_at(value, &["candidate_rmse"]).or_else(|| f64_at(value, &["candidate_analysis_rmse"]));
    let reference_rmse = f64_at(value, &["reference_rmse"])
        .or_else(|| f64_at(value, &["reference_analysis_rmse"]))
        .or_else(|| f64_at(value, &["baseline_rmse"]));
    SurfaceMesoanalysisCalibrationReferenceVariableCase {
        observation_count: usize_at(value, &["candidate_observation_count"])
            .or_else(|| usize_at(value, &["observation_count"])),
        candidate_mean_abs_error,
        reference_mean_abs_error,
        candidate_minus_reference_mae: f64_at(value, &["candidate_minus_reference_mae"])
            .or_else(|| f64_at(value, &["candidate_minus_baseline_mae"]))
            .or_else(|| option_delta(candidate_mean_abs_error, reference_mean_abs_error)),
        candidate_rmse,
        reference_rmse,
        candidate_minus_reference_rmse: f64_at(value, &["candidate_minus_reference_rmse"])
            .or_else(|| f64_at(value, &["candidate_minus_baseline_rmse"]))
            .or_else(|| option_delta(candidate_rmse, reference_rmse)),
    }
}

fn extract_covariance_ablation_cases(
    value: &Value,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationAblationCase> {
    let mut ablations = BTreeMap::new();
    if let Some(ablation) = value.get("covariance_ablation_comparison") {
        push_covariance_ablation_case(ablation, &mut ablations);
    }
    if let Some(ablation_array) = value
        .get("covariance_ablation_comparisons")
        .and_then(Value::as_array)
    {
        for ablation in ablation_array {
            push_covariance_ablation_case(ablation, &mut ablations);
        }
    }
    if let Some(ablation_map) = value
        .get("covariance_ablation_comparisons")
        .and_then(Value::as_object)
    {
        for (label, ablation) in ablation_map {
            let mut case = parse_covariance_ablation_case(ablation);
            if case.baseline_label.is_empty() {
                case.baseline_label = label.clone();
            }
            if !case.variables.is_empty() {
                ablations.insert(case.baseline_label.clone(), case);
            }
        }
    }
    ablations
}

fn push_covariance_ablation_case(
    value: &Value,
    ablations: &mut BTreeMap<String, SurfaceMesoanalysisCalibrationAblationCase>,
) {
    let case = parse_covariance_ablation_case(value);
    if !case.baseline_label.is_empty() && !case.variables.is_empty() {
        ablations.insert(case.baseline_label.clone(), case);
    }
}

fn parse_covariance_ablation_case(value: &Value) -> SurfaceMesoanalysisCalibrationAblationCase {
    let baseline_label = string_at(value, &["baseline_label"])
        .or_else(|| string_at(value, &["label"]))
        .unwrap_or_default();
    let candidate_label = string_at(value, &["candidate_label"]).unwrap_or_default();
    let (validation_mode, variable_root) = select_comparison_benchmark(value)
        .map(|(mode, benchmark)| (mode, benchmark))
        .unwrap_or_else(|| {
            (
                string_at(value, &["validation_mode"])
                    .unwrap_or_else(|| "covariance_ablation_validation".to_string()),
                value,
            )
        });
    SurfaceMesoanalysisCalibrationAblationCase {
        baseline_label,
        candidate_label,
        validation_mode,
        baseline_compute_ms: u64_at(value, &["baseline_compute_ms"]).map(u128::from),
        variables: extract_covariance_ablation_variables(variable_root),
    }
}

fn select_comparison_benchmark(value: &Value) -> Option<(String, &Value)> {
    let repeated = value.get("repeated_holdout_benchmark_summary");
    if let Some(benchmark) = non_null_object(repeated) {
        return Some((
            string_at(benchmark, &["validation_mode"])
                .unwrap_or_else(|| "repeated_holdout_validation".to_string()),
            benchmark,
        ));
    }
    let holdout = value.get("holdout_benchmark_summary");
    if let Some(benchmark) = non_null_object(holdout) {
        return Some((
            string_at(benchmark, &["validation_mode"])
                .unwrap_or_else(|| "holdout_validation".to_string()),
            benchmark,
        ));
    }
    let same_observation = value.get("benchmark_summary");
    if let Some(benchmark) = non_null_object(same_observation) {
        return Some((
            string_at(benchmark, &["validation_mode"])
                .unwrap_or_else(|| "same_observation_validation".to_string()),
            benchmark,
        ));
    }
    None
}

fn extract_covariance_ablation_variables(
    value: &Value,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationAblationVariableCase> {
    let mut variables = BTreeMap::new();
    for name in [
        "temperature_c",
        "dewpoint_c",
        "wind_speed_ms",
        "mean_sea_level_pressure_hpa",
    ] {
        let Some(variable) = value.get(name).filter(|value| value.is_object()) else {
            continue;
        };
        variables.insert(
            name.to_string(),
            parse_covariance_ablation_variable(variable),
        );
    }
    variables
}

fn parse_covariance_ablation_variable(
    value: &Value,
) -> SurfaceMesoanalysisCalibrationAblationVariableCase {
    let candidate_mean_abs_error = f64_at(value, &["candidate_mean_abs_error"])
        .or_else(|| f64_at(value, &["candidate_mean_abs_analysis_error"]));
    let baseline_mean_abs_error = f64_at(value, &["baseline_mean_abs_error"])
        .or_else(|| f64_at(value, &["baseline_mean_abs_analysis_error"]));
    let candidate_rmse =
        f64_at(value, &["candidate_rmse"]).or_else(|| f64_at(value, &["candidate_analysis_rmse"]));
    let baseline_rmse =
        f64_at(value, &["baseline_rmse"]).or_else(|| f64_at(value, &["baseline_analysis_rmse"]));
    SurfaceMesoanalysisCalibrationAblationVariableCase {
        candidate_observation_count: usize_at(value, &["candidate_observation_count"]),
        baseline_observation_count: usize_at(value, &["baseline_observation_count"]),
        candidate_mean_abs_error,
        baseline_mean_abs_error,
        candidate_minus_baseline_mae: f64_at(value, &["candidate_minus_baseline_mae"])
            .or_else(|| option_delta(candidate_mean_abs_error, baseline_mean_abs_error)),
        candidate_rmse,
        baseline_rmse,
        candidate_minus_baseline_rmse: f64_at(value, &["candidate_minus_baseline_rmse"])
            .or_else(|| option_delta(candidate_rmse, baseline_rmse)),
        mae_winner: string_at(value, &["mae_winner"]),
        rmse_winner: string_at(value, &["rmse_winner"]),
    }
}

fn extract_source_cases(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationSourceCase> {
    let candidate_sources = selected_candidate_source_accumulators(value, benchmark_mode);
    let baseline_sources = selected_baseline_source_accumulators(value, benchmark_mode);
    candidate_sources
        .into_iter()
        .map(|(source, candidate)| {
            let baseline = baseline_sources.get(source.as_str());
            let case = finish_source_case(candidate, baseline);
            (source, case)
        })
        .collect()
}

fn extract_strata_cases(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationStratumCase> {
    let candidate_strata = selected_candidate_stratum_accumulators(value, benchmark_mode);
    let baseline_strata = selected_baseline_stratum_accumulators(value, benchmark_mode);
    candidate_strata
        .into_iter()
        .map(|(key, candidate)| {
            let baseline = baseline_strata.get(key.as_str());
            let case = finish_stratum_case(candidate, baseline);
            (key, case)
        })
        .collect()
}

fn extract_station_cases(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SurfaceMesoanalysisCalibrationStationCase> {
    selected_candidate_station_accumulators(value, benchmark_mode)
        .into_iter()
        .map(|(key, accumulator)| (key, accumulator.finish()))
        .collect()
}

fn selected_candidate_source_accumulators(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SourceSummaryAccumulator> {
    if benchmark_mode == "repeated_holdout_validation" {
        return source_accumulators_from_folds_at(
            value,
            &["mesoanalysis", "repeated_holdout_validation", "folds"],
        );
    }
    if benchmark_mode == "holdout_validation" {
        return source_accumulators_from_source_summaries_at(
            value,
            &[
                "mesoanalysis",
                "holdout_validation",
                "validation",
                "source_summaries",
            ],
        );
    }
    source_accumulators_from_source_summaries_at(
        value,
        &["mesoanalysis", "validation", "source_summaries"],
    )
}

fn selected_baseline_source_accumulators(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SourceSummaryAccumulator> {
    if benchmark_mode == "repeated_holdout_validation" {
        return source_accumulators_from_folds_at(
            value,
            &[
                "barnes_baseline_comparison",
                "baseline_repeated_holdout_validation",
                "folds",
            ],
        );
    }
    if benchmark_mode == "holdout_validation" {
        return source_accumulators_from_source_summaries_at(
            value,
            &[
                "barnes_baseline_comparison",
                "baseline_holdout_validation",
                "validation",
                "source_summaries",
            ],
        );
    }
    source_accumulators_from_source_summaries_at(
        value,
        &[
            "barnes_baseline_comparison",
            "baseline_validation",
            "source_summaries",
        ],
    )
}

fn selected_candidate_stratum_accumulators(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, StratumSummaryAccumulator> {
    if benchmark_mode == "repeated_holdout_validation" {
        return stratum_accumulators_from_folds_at(
            value,
            &["mesoanalysis", "repeated_holdout_validation", "folds"],
        );
    }
    if benchmark_mode == "holdout_validation" {
        return stratum_accumulators_from_strata_summaries_at(
            value,
            &[
                "mesoanalysis",
                "holdout_validation",
                "validation",
                "strata_summaries",
            ],
        );
    }
    stratum_accumulators_from_strata_summaries_at(
        value,
        &["mesoanalysis", "validation", "strata_summaries"],
    )
}

fn selected_baseline_stratum_accumulators(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, StratumSummaryAccumulator> {
    if benchmark_mode == "repeated_holdout_validation" {
        return stratum_accumulators_from_folds_at(
            value,
            &[
                "barnes_baseline_comparison",
                "baseline_repeated_holdout_validation",
                "folds",
            ],
        );
    }
    if benchmark_mode == "holdout_validation" {
        return stratum_accumulators_from_strata_summaries_at(
            value,
            &[
                "barnes_baseline_comparison",
                "baseline_holdout_validation",
                "validation",
                "strata_summaries",
            ],
        );
    }
    stratum_accumulators_from_strata_summaries_at(
        value,
        &[
            "barnes_baseline_comparison",
            "baseline_validation",
            "strata_summaries",
        ],
    )
}

fn selected_candidate_station_accumulators(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, StationSummaryAccumulator> {
    let mut accumulators = BTreeMap::new();
    if benchmark_mode == "repeated_holdout_validation" {
        push_station_samples_from_folds_at(
            value,
            &["mesoanalysis", "repeated_holdout_validation", "folds"],
            &mut accumulators,
        );
    } else if benchmark_mode == "holdout_validation" {
        push_station_samples_at(
            value,
            &[
                "mesoanalysis",
                "holdout_validation",
                "validation",
                "samples",
            ],
            &mut accumulators,
        );
    } else {
        push_station_samples_at(
            value,
            &["mesoanalysis", "validation", "samples"],
            &mut accumulators,
        );
    }
    accumulators
}

fn selected_candidate_variable_stats(
    value: &Value,
    benchmark_mode: &str,
) -> BTreeMap<String, SourceVariableStats> {
    let mut accumulators = BTreeMap::new();
    if benchmark_mode == "repeated_holdout_validation" {
        push_variable_summaries_from_folds_at(
            value,
            &["mesoanalysis", "repeated_holdout_validation", "folds"],
            &mut accumulators,
        );
    } else if benchmark_mode == "holdout_validation" {
        push_variable_summaries_at(
            value,
            &["mesoanalysis", "holdout_validation", "validation"],
            &mut accumulators,
        );
    } else {
        push_variable_summaries_at(value, &["mesoanalysis", "validation"], &mut accumulators);
    }
    accumulators
        .into_iter()
        .map(|(name, accumulator)| (name, accumulator.finish()))
        .collect()
}

fn push_station_samples_from_folds_at(
    value: &Value,
    path: &[&str],
    accumulators: &mut BTreeMap<String, StationSummaryAccumulator>,
) {
    let Some(folds) = value_at(value, path).and_then(Value::as_array) else {
        return;
    };
    for fold in folds {
        push_station_samples_at(fold, &["validation", "samples"], accumulators);
    }
}

fn push_station_samples_at(
    value: &Value,
    path: &[&str],
    accumulators: &mut BTreeMap<String, StationSummaryAccumulator>,
) {
    let Some(samples) = value_at(value, path).and_then(Value::as_array) else {
        return;
    };
    for sample in samples {
        let station_id =
            string_at(sample, &["station_id"]).unwrap_or_else(|| "<missing>".to_string());
        let source = string_at(sample, &["source"]).unwrap_or_else(|| "<missing>".to_string());
        let key = station_key(source.as_str(), station_id.as_str());
        accumulators
            .entry(key)
            .or_insert_with(|| StationSummaryAccumulator::new(station_id, source))
            .push_sample(sample);
    }
}

fn push_variable_summaries_from_folds_at(
    value: &Value,
    path: &[&str],
    accumulators: &mut BTreeMap<String, SourceVariableStatsAccumulator>,
) {
    let Some(folds) = value_at(value, path).and_then(Value::as_array) else {
        return;
    };
    for fold in folds {
        push_variable_summaries_at(fold, &["validation"], accumulators);
    }
}

fn push_variable_summaries_at(
    value: &Value,
    path: &[&str],
    accumulators: &mut BTreeMap<String, SourceVariableStatsAccumulator>,
) {
    let Some(summary) = value_at(value, path).filter(|value| value.is_object()) else {
        return;
    };
    for variable in [
        "temperature_c",
        "dewpoint_c",
        "wind_speed_ms",
        "mean_sea_level_pressure_hpa",
    ] {
        if let Some(value) = value_at(summary, &[variable]) {
            accumulators
                .entry(variable.to_string())
                .or_default()
                .push_variable_summary(value);
        }
    }
}

fn source_accumulators_from_folds_at(
    value: &Value,
    path: &[&str],
) -> BTreeMap<String, SourceSummaryAccumulator> {
    let mut accumulators = BTreeMap::new();
    let Some(folds) = value_at(value, path).and_then(Value::as_array) else {
        return accumulators;
    };
    for fold in folds {
        push_source_summaries_at(fold, &["validation", "source_summaries"], &mut accumulators);
    }
    accumulators
}

fn source_accumulators_from_source_summaries_at(
    value: &Value,
    path: &[&str],
) -> BTreeMap<String, SourceSummaryAccumulator> {
    let mut accumulators = BTreeMap::new();
    push_source_summaries_at(value, path, &mut accumulators);
    accumulators
}

fn stratum_accumulators_from_folds_at(
    value: &Value,
    path: &[&str],
) -> BTreeMap<String, StratumSummaryAccumulator> {
    let mut accumulators = BTreeMap::new();
    let Some(folds) = value_at(value, path).and_then(Value::as_array) else {
        return accumulators;
    };
    for fold in folds {
        push_strata_summaries_at(fold, &["validation", "strata_summaries"], &mut accumulators);
    }
    accumulators
}

fn stratum_accumulators_from_strata_summaries_at(
    value: &Value,
    path: &[&str],
) -> BTreeMap<String, StratumSummaryAccumulator> {
    let mut accumulators = BTreeMap::new();
    push_strata_summaries_at(value, path, &mut accumulators);
    accumulators
}

fn push_source_summaries_at(
    value: &Value,
    path: &[&str],
    accumulators: &mut BTreeMap<String, SourceSummaryAccumulator>,
) {
    let Some(summaries) = value_at(value, path).and_then(Value::as_array) else {
        return;
    };
    for summary in summaries {
        let Some(source) = string_at(summary, &["source"]) else {
            continue;
        };
        accumulators
            .entry(source)
            .or_default()
            .push_source_summary(summary);
    }
}

fn push_strata_summaries_at(
    value: &Value,
    path: &[&str],
    accumulators: &mut BTreeMap<String, StratumSummaryAccumulator>,
) {
    let Some(summaries) = value_at(value, path).and_then(Value::as_array) else {
        return;
    };
    for summary in summaries {
        let Some(stratum_type) = string_at(summary, &["stratum_type"]) else {
            continue;
        };
        let Some(stratum_value) = string_at(summary, &["stratum_value"]) else {
            continue;
        };
        let key = calibration_stratum_key(stratum_type.as_str(), stratum_value.as_str());
        accumulators
            .entry(key)
            .or_insert_with(|| StratumSummaryAccumulator::new(stratum_type, stratum_value))
            .push_stratum_summary(summary);
    }
}

fn finish_source_case(
    candidate: SourceSummaryAccumulator,
    baseline: Option<&SourceSummaryAccumulator>,
) -> SurfaceMesoanalysisCalibrationSourceCase {
    let candidate = candidate.finish();
    let baseline_variables = baseline
        .map(SourceSummaryAccumulator::finish)
        .map(|source| source.variables)
        .unwrap_or_default();
    let variables = candidate
        .variables
        .into_iter()
        .map(|(name, candidate_variable)| {
            let baseline_variable = baseline_variables.get(name.as_str());
            let variable = SurfaceMesoanalysisCalibrationVariableCase {
                observation_count: Some(candidate_variable.observation_count),
                fold_count: None,
                background_mean_abs_error: candidate_variable.mean_abs_background_error,
                candidate_mean_abs_error: candidate_variable.mean_abs_analysis_error,
                barnes_mean_abs_error: baseline_variable
                    .and_then(|variable| variable.mean_abs_analysis_error),
                candidate_minus_background_mae: option_delta(
                    candidate_variable.mean_abs_analysis_error,
                    candidate_variable.mean_abs_background_error,
                ),
                candidate_minus_barnes_mae: option_delta(
                    candidate_variable.mean_abs_analysis_error,
                    baseline_variable.and_then(|variable| variable.mean_abs_analysis_error),
                ),
                background_rmse: candidate_variable.background_rmse,
                candidate_rmse: candidate_variable.analysis_rmse,
                barnes_rmse: baseline_variable.and_then(|variable| variable.analysis_rmse),
                candidate_minus_background_rmse: option_delta(
                    candidate_variable.analysis_rmse,
                    candidate_variable.background_rmse,
                ),
                candidate_minus_barnes_rmse: option_delta(
                    candidate_variable.analysis_rmse,
                    baseline_variable.and_then(|variable| variable.analysis_rmse),
                ),
                mae_winner: None,
                rmse_winner: None,
                candidate_beats_background_mae_fold_count: None,
                candidate_beats_barnes_mae_fold_count: None,
                candidate_beats_background_rmse_fold_count: None,
                candidate_beats_barnes_rmse_fold_count: None,
                confidence: candidate_variable.confidence.clone(),
            };
            (name, variable)
        })
        .collect();

    SurfaceMesoanalysisCalibrationSourceCase {
        sampled_observation_count: candidate.sampled_observation_count,
        variables,
    }
}

fn finish_stratum_case(
    candidate: StratumSummaryAccumulator,
    baseline: Option<&StratumSummaryAccumulator>,
) -> SurfaceMesoanalysisCalibrationStratumCase {
    let stratum_type = candidate.stratum_type.clone();
    let stratum_value = candidate.stratum_value.clone();
    let source_case = finish_source_case(
        candidate.summary,
        baseline.map(|baseline| &baseline.summary),
    );
    SurfaceMesoanalysisCalibrationStratumCase {
        stratum_type,
        stratum_value,
        sampled_observation_count: source_case.sampled_observation_count,
        variables: source_case.variables,
    }
}

fn calibration_stratum_key(stratum_type: &str, stratum_value: &str) -> String {
    format!("{stratum_type}={stratum_value}")
}

fn station_key(source: &str, station_id: &str) -> String {
    format!(
        "{}::{}",
        non_empty_or_missing(source),
        non_empty_or_missing(station_id)
    )
}
