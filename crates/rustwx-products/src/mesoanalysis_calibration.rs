use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

mod types;
pub use types::*;
mod gates;
pub use gates::evaluate_surface_mesoanalysis_calibration_gate;
mod aggregation;
mod confidence;
mod helpers;
mod history;
mod parsing;
mod summaries;
#[cfg(test)]
use crate::mesoanalysis::{
    CONFIDENCE_RELIABILITY_MAX_RANKED_HIGH_MINUS_LOW_MAE,
    CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
};
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
