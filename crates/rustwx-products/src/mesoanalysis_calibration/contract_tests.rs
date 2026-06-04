use super::*;
use serde_json::{Value, json};

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

fn calibration_contract_report() -> SurfaceMesoanalysisCalibrationReport {
    build_surface_mesoanalysis_calibration_report_from_values([(
        PathBuf::from("contract/run_report.json"),
        Ok(json!({
            "schema": "rustwx.surface_mesoanalysis.run_report.v1",
            "model": "hrrr",
            "model_source": "nomads",
            "model_cycle": "2026051300z",
            "date": "20260513",
            "cycle": 0,
            "forecast_hour": 1,
            "model_load_mode": "surface_only",
            "case_tags": ["regime=dryline"],
            "mesoanalysis_config": {
                "method": "optimal_interpolation"
            },
            "validation_gate": {
                "passed": true
            },
            "mesoanalysis": {
                "observation_count": 1,
                "source_count": 1,
                "validation": {
                    "source_summaries": [
                        {
                            "source": "metar",
                            "sampled_observation_count": 1,
                            "temperature_c": {
                                "observation_count": 1,
                                "mean_abs_background_error": 2.0,
                                "mean_abs_analysis_error": 0.5,
                                "background_rmse": 2.0,
                                "analysis_rmse": 0.5
                            }
                        }
                    ],
                    "samples": [
                        {
                            "station_id": "KOUN",
                            "source": "metar",
                            "temperature_c": {
                                "background_error": 2.0,
                                "analysis_error": 0.5,
                                "abs_error_improvement": 1.5
                            }
                        }
                    ]
                }
            },
            "barnes_baseline_comparison": {
                "benchmark_summary": {
                    "validation_mode": "same_observation_validation",
                    "temperature_c": {
                        "candidate_observation_count": 1,
                        "background_mean_abs_error": 2.0,
                        "candidate_mean_abs_error": 0.5,
                        "baseline_mean_abs_error": 1.0,
                        "candidate_minus_background_mae": -1.5,
                        "candidate_minus_baseline_mae": -0.5,
                        "background_rmse": 2.0,
                        "candidate_rmse": 0.5,
                        "baseline_rmse": 1.0,
                        "candidate_minus_background_rmse": -1.5,
                        "candidate_minus_baseline_rmse": -0.5
                    }
                },
                "baseline_validation": {
                    "source_summaries": [
                        {
                            "source": "metar",
                            "sampled_observation_count": 1,
                            "temperature_c": {
                                "observation_count": 1,
                                "mean_abs_background_error": 2.0,
                                "mean_abs_analysis_error": 1.0,
                                "background_rmse": 2.0,
                                "analysis_rmse": 1.0
                            }
                        }
                    ]
                }
            }
        })),
    )])
}

fn contract_gate_thresholds() -> SurfaceMesoanalysisCalibrationGateThresholds {
    SurfaceMesoanalysisCalibrationGateThresholds {
        min_case_count: 1,
        allow_skipped_reports: false,
        allowed_quality_flags: vec!["contains_same_observation_validation".to_string()],
        required_benchmark_modes: vec!["same_observation_validation".to_string()],
        variables: vec!["temperature_c".to_string()],
        sources: vec!["metar".to_string()],
        stations: vec!["metar::KOUN".to_string()],
        max_domain_candidate_minus_background_mae: Some(0.0),
        max_source_candidate_minus_background_mae: Some(0.0),
        min_station_observation_count: Some(1),
        max_station_analysis_mae: Some(0.5),
        ..empty_gate_thresholds()
    }
}

fn gate_check_value(gate: &SurfaceMesoanalysisCalibrationGate, name: &str, scope: &str) -> Value {
    let check = gate
        .checks
        .iter()
        .find(|check| check.name == name && check.scope == scope)
        .unwrap_or_else(|| panic!("missing calibration gate check {name}/{scope}"));
    serde_json::to_value(check).expect("gate check serializes")
}

#[test]
fn calibration_contract_serializes_gate_history_and_index_shapes() {
    let mut report = calibration_contract_report();
    report.generated_at = "2026-05-13T00:00:00Z".to_string();
    assert_eq!(
        serde_json::to_value(&report)
            .expect("report serializes")
            .get("schema"),
        Some(&json!("rustwx.surface_mesoanalysis.calibration_matrix.v1"))
    );

    let gate = evaluate_surface_mesoanalysis_calibration_gate(&report, contract_gate_thresholds());
    let gate_json = serde_json::to_value(&gate).expect("gate serializes");
    assert_eq!(
        gate_json.get("schema"),
        Some(&json!("rustwx.surface_mesoanalysis.calibration_gate.v1"))
    );
    assert_eq!(gate_json.get("passed"), Some(&json!(true)));
    assert_eq!(gate_json["thresholds"]["min_case_count"], json!(1));
    assert_eq!(
        gate_json["thresholds"]["variables"],
        json!(["temperature_c"])
    );
    assert_eq!(
        gate_check_value(&gate, "loaded_case_count", "matrix"),
        json!({
            "name": "loaded_case_count",
            "scope": "matrix",
            "passed": true,
            "observed": 1.0,
            "threshold": 1.0,
            "comparator": ">=",
            "message": "1 loaded cases; minimum required is 1"
        })
    );
    assert_eq!(
        gate_check_value(
            &gate,
            "domain_candidate_minus_background_mae",
            "domain/temperature_c",
        ),
        json!({
            "name": "domain_candidate_minus_background_mae",
            "scope": "domain/temperature_c",
            "passed": true,
            "observed": -1.5,
            "threshold": 0.0,
            "comparator": "<=",
            "message": "domain temperature_c candidate-minus-comparator MAE must be <= 0.000"
        })
    );
    assert_eq!(
        gate_check_value(
            &gate,
            "source_candidate_minus_background_mae",
            "source/metar/temperature_c",
        ),
        json!({
            "name": "source_candidate_minus_background_mae",
            "scope": "source/metar/temperature_c",
            "passed": true,
            "observed": -1.5,
            "threshold": 0.0,
            "comparator": "<=",
            "message": "source metar temperature_c candidate-minus-comparator MAE must be <= 0.000"
        })
    );
    assert_eq!(
        gate_check_value(
            &gate,
            "station_observation_count",
            "station/metar::KOUN/temperature_c",
        ),
        json!({
            "name": "station_observation_count",
            "scope": "station/metar::KOUN/temperature_c",
            "passed": true,
            "observed": 1.0,
            "threshold": 1.0,
            "comparator": ">=",
            "message": "station metar::KOUN temperature_c observation count must be >= 1"
        })
    );

    let mut history = build_surface_mesoanalysis_innovation_history(&report);
    history.generated_at = "2026-05-13T00:05:00Z".to_string();
    let history_json = serde_json::to_value(&history).expect("history serializes");
    assert_eq!(
        history_json["schema"],
        json!("rustwx.surface_mesoanalysis.innovation_history.v1")
    );
    assert_eq!(
        history_json["calibration_schema"],
        json!("rustwx.surface_mesoanalysis.calibration_matrix.v1")
    );
    assert_eq!(
        history_json["calibration_generated_at"],
        json!(report.generated_at)
    );
    assert_eq!(history_json["case_count"], json!(1));
    assert_eq!(
        history_json["station_watchlist"][0],
        json!({
            "station_key": "metar::KOUN",
            "station_id": "KOUN",
            "source": "metar",
            "variable": "temperature_c",
            "case_count": 1,
            "observation_count": 1,
            "mean_abs_analysis_error": 0.5,
            "abs_analysis_bias": 0.5,
            "mean_abs_error_improvement": 1.5,
            "max_abs_analysis_error": 0.5,
            "severity_score": 0.675,
            "reason": "persistent_station_bias"
        })
    );
    assert_eq!(
        history_json["source_watchlist"][0],
        json!({
            "source": "metar",
            "variable": "temperature_c",
            "case_count": 1,
            "mean_observation_count": 1.0,
            "mean_candidate_mae": 0.5,
            "mean_candidate_minus_background_mae": -1.5,
            "worst_candidate_minus_background_mae": -1.5,
            "severity_score": 0.5,
            "reason": "high_source_analysis_error"
        })
    );

    let station_index_records = station_wxstore_index_records(&history);
    assert_eq!(
        serde_json::to_value(&station_index_records[0]).expect("station index serializes"),
        json!({
            "station_key": "metar::KOUN",
            "station_id": "KOUN",
            "source": "metar",
            "variable": "temperature_c",
            "case_count": 1,
            "sample_count": 1,
            "observation_count": 1,
            "mean_background_error": 2.0,
            "mean_analysis_error": 0.5,
            "mean_abs_background_error": 2.0,
            "mean_abs_analysis_error": 0.5,
            "mean_abs_error_improvement": 1.5,
            "background_rmse": 2.0,
            "analysis_rmse": 0.5,
            "max_abs_background_error": 2.0,
            "max_abs_analysis_error": 0.5,
            "watchlist": history_json["station_watchlist"][0].clone()
        })
    );
    let source_index_records = source_wxstore_index_records(&history);
    assert_eq!(
        serde_json::to_value(&source_index_records[0]).expect("source index serializes"),
        json!({
            "source": "metar",
            "variable": "temperature_c",
            "case_count": 1,
            "mean_sampled_observation_count": 1.0,
            "mean_observation_count": 1.0,
            "mean_background_mae": 2.0,
            "mean_candidate_mae": 0.5,
            "mean_candidate_minus_background_mae": -1.5,
            "mean_background_rmse": 2.0,
            "mean_candidate_rmse": 0.5,
            "mean_candidate_minus_background_rmse": -1.5,
            "candidate_beats_background_mae_case_count": 1,
            "candidate_loses_background_mae_case_count": 0,
            "worst_candidate_minus_background_mae": -1.5,
            "watchlist": history_json["source_watchlist"][0].clone()
        })
    );
}
