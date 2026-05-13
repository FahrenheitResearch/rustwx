use clap::Parser;
use rustwx_products::mesoanalysis_calibration::{
    SurfaceMesoanalysisCalibrationGateThresholds, SurfaceMesoanalysisInnovationQueryRequest,
    build_surface_mesoanalysis_calibration_report, build_surface_mesoanalysis_innovation_history,
    discover_surface_mesoanalysis_run_reports, evaluate_surface_mesoanalysis_calibration_gate,
    merge_surface_mesoanalysis_innovation_history, query_surface_mesoanalysis_innovation_history,
    read_surface_mesoanalysis_innovation_history, write_surface_mesoanalysis_calibration_report,
    write_surface_mesoanalysis_innovation_history,
    write_surface_mesoanalysis_innovation_query_report,
    write_surface_mesoanalysis_innovation_wxstore_index,
};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "surface-mesoanalysis-calibration",
    about = "Aggregate RustWX surface mesoanalysis run reports into a calibration skill matrix"
)]
struct Cli {
    #[arg(long = "run-report")]
    run_reports: Vec<PathBuf>,
    #[arg(long = "reports-root")]
    reports_roots: Vec<PathBuf>,
    #[arg(
        long,
        default_value = "target\\surface_mesoanalysis_calibration\\calibration_matrix.json"
    )]
    out: PathBuf,
    #[arg(long)]
    innovation_history_in: Option<PathBuf>,
    #[arg(long)]
    innovation_history_out: Option<PathBuf>,
    #[arg(long)]
    innovation_history_max_entries_per_series: Option<usize>,
    #[arg(long)]
    innovation_wxstore_index_dir: Option<PathBuf>,
    #[arg(long)]
    innovation_query_history: Option<PathBuf>,
    #[arg(long)]
    innovation_query_out: Option<PathBuf>,
    #[arg(long = "innovation-query-station", value_delimiter = ',', num_args = 1..)]
    innovation_query_stations: Vec<String>,
    #[arg(long = "innovation-query-source", value_delimiter = ',', num_args = 1..)]
    innovation_query_sources: Vec<String>,
    #[arg(long = "innovation-query-variable", value_delimiter = ',', num_args = 1..)]
    innovation_query_variables: Vec<String>,
    #[arg(long)]
    innovation_query_min_case_count: Option<usize>,
    #[arg(long, default_value_t = 10)]
    innovation_query_top: usize,
    #[arg(long, default_value_t = 1)]
    min_case_count: usize,
    #[arg(long, default_value_t = false)]
    fail_on_skipped: bool,
    #[arg(long, default_value_t = false)]
    fail_on_calibration_gate: bool,
    #[arg(long = "allow-quality-flag", value_delimiter = ',', num_args = 1..)]
    allowed_quality_flags: Vec<String>,
    #[arg(long = "require-benchmark-mode", value_delimiter = ',', num_args = 1..)]
    required_benchmark_modes: Vec<String>,
    #[arg(long = "require-holdout-strategy", value_delimiter = ',', num_args = 1..)]
    required_holdout_strategies: Vec<String>,
    #[arg(long = "require-external-reference", value_delimiter = ',', num_args = 1..)]
    required_external_references: Vec<String>,
    #[arg(long = "require-covariance-ablation", value_delimiter = ',', num_args = 1..)]
    required_covariance_ablations: Vec<String>,
    #[arg(long = "require-case-tag", value_delimiter = ',', num_args = 1..)]
    required_case_tags: Vec<String>,
    #[arg(long = "gate-variable", value_delimiter = ',', num_args = 1..)]
    gate_variables: Vec<String>,
    #[arg(long = "gate-source", value_delimiter = ',', num_args = 1..)]
    gate_sources: Vec<String>,
    #[arg(long = "gate-stratum", value_delimiter = ',', num_args = 1..)]
    gate_strata: Vec<String>,
    #[arg(long = "gate-station", value_delimiter = ',', num_args = 1..)]
    gate_stations: Vec<String>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_domain_oi_minus_raw_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_domain_oi_minus_barnes_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_domain_oi_minus_reference_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_covariance_ablation_oi_minus_baseline_mae: Option<f64>,
    #[arg(long)]
    gate_max_case_mesoanalysis_compute_ms: Option<f64>,
    #[arg(long)]
    gate_min_unique_case_signatures: Option<usize>,
    #[arg(long)]
    gate_min_unique_dates: Option<usize>,
    #[arg(long)]
    gate_min_unique_cycles: Option<usize>,
    #[arg(long)]
    gate_min_unique_forecast_hours: Option<usize>,
    #[arg(long)]
    gate_min_unique_case_tags: Option<usize>,
    #[arg(long)]
    gate_min_domain_low_confidence_observation_count: Option<usize>,
    #[arg(long)]
    gate_min_domain_high_confidence_observation_count: Option<usize>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_domain_high_minus_low_confidence_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_domain_ranked_high_minus_low_confidence_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_domain_confidence_abs_error_correlation: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_source_oi_minus_raw_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_source_oi_minus_barnes_mae: Option<f64>,
    #[arg(long)]
    gate_min_source_low_confidence_observation_count: Option<usize>,
    #[arg(long)]
    gate_min_source_high_confidence_observation_count: Option<usize>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_source_high_minus_low_confidence_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_source_ranked_high_minus_low_confidence_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_source_confidence_abs_error_correlation: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_stratum_oi_minus_raw_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_stratum_oi_minus_barnes_mae: Option<f64>,
    #[arg(long)]
    gate_min_stratum_low_confidence_observation_count: Option<usize>,
    #[arg(long)]
    gate_min_stratum_high_confidence_observation_count: Option<usize>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_stratum_high_minus_low_confidence_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_stratum_ranked_high_minus_low_confidence_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_stratum_confidence_abs_error_correlation: Option<f64>,
    #[arg(long)]
    gate_min_station_observation_count: Option<usize>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_station_oi_minus_raw_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_station_analysis_mae: Option<f64>,
    #[arg(long, allow_negative_numbers = true)]
    gate_max_station_abs_analysis_bias: Option<f64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    if args.innovation_query_history.is_some()
        && args.run_reports.is_empty()
        && args.reports_roots.is_empty()
    {
        emit_innovation_history_query(&args)?;
        return Ok(());
    }

    let mut reports = args.run_reports.clone();
    reports.extend(discover_surface_mesoanalysis_run_reports(
        &args.reports_roots,
    )?);
    reports.sort();
    reports.dedup();
    if reports.is_empty() {
        return Err("no run reports were provided or discovered".into());
    }

    let mut report = build_surface_mesoanalysis_calibration_report(&reports);
    let gate_requested = args.fail_on_calibration_gate
        || args.fail_on_skipped
        || !args.allowed_quality_flags.is_empty()
        || !args.required_benchmark_modes.is_empty()
        || !args.required_holdout_strategies.is_empty()
        || !args.required_external_references.is_empty()
        || !args.required_covariance_ablations.is_empty()
        || !args.required_case_tags.is_empty()
        || !args.gate_variables.is_empty()
        || !args.gate_sources.is_empty()
        || !args.gate_strata.is_empty()
        || !args.gate_stations.is_empty()
        || args.gate_max_domain_oi_minus_raw_mae.is_some()
        || args.gate_max_domain_oi_minus_barnes_mae.is_some()
        || args.gate_max_domain_oi_minus_reference_mae.is_some()
        || args
            .gate_max_covariance_ablation_oi_minus_baseline_mae
            .is_some()
        || args.gate_max_case_mesoanalysis_compute_ms.is_some()
        || args.gate_min_unique_case_signatures.is_some()
        || args.gate_min_unique_dates.is_some()
        || args.gate_min_unique_cycles.is_some()
        || args.gate_min_unique_forecast_hours.is_some()
        || args.gate_min_unique_case_tags.is_some()
        || args
            .gate_min_domain_low_confidence_observation_count
            .is_some()
        || args
            .gate_min_domain_high_confidence_observation_count
            .is_some()
        || args.gate_max_domain_high_minus_low_confidence_mae.is_some()
        || args
            .gate_max_domain_ranked_high_minus_low_confidence_mae
            .is_some()
        || args
            .gate_max_domain_confidence_abs_error_correlation
            .is_some()
        || args.gate_max_source_oi_minus_raw_mae.is_some()
        || args.gate_max_source_oi_minus_barnes_mae.is_some()
        || args
            .gate_min_source_low_confidence_observation_count
            .is_some()
        || args
            .gate_min_source_high_confidence_observation_count
            .is_some()
        || args.gate_max_source_high_minus_low_confidence_mae.is_some()
        || args
            .gate_max_source_ranked_high_minus_low_confidence_mae
            .is_some()
        || args
            .gate_max_source_confidence_abs_error_correlation
            .is_some()
        || args.gate_max_stratum_oi_minus_raw_mae.is_some()
        || args.gate_max_stratum_oi_minus_barnes_mae.is_some()
        || args
            .gate_min_stratum_low_confidence_observation_count
            .is_some()
        || args
            .gate_min_stratum_high_confidence_observation_count
            .is_some()
        || args
            .gate_max_stratum_high_minus_low_confidence_mae
            .is_some()
        || args
            .gate_max_stratum_ranked_high_minus_low_confidence_mae
            .is_some()
        || args
            .gate_max_stratum_confidence_abs_error_correlation
            .is_some();
    let gate_requested = gate_requested
        || args.gate_min_station_observation_count.is_some()
        || args.gate_max_station_oi_minus_raw_mae.is_some()
        || args.gate_max_station_analysis_mae.is_some()
        || args.gate_max_station_abs_analysis_bias.is_some();
    if gate_requested {
        report.calibration_gate = Some(evaluate_surface_mesoanalysis_calibration_gate(
            &report,
            SurfaceMesoanalysisCalibrationGateThresholds {
                min_case_count: args.min_case_count,
                allow_skipped_reports: !args.fail_on_skipped,
                allowed_quality_flags: args.allowed_quality_flags.clone(),
                required_benchmark_modes: args.required_benchmark_modes.clone(),
                required_holdout_strategies: args.required_holdout_strategies.clone(),
                required_external_references: args.required_external_references.clone(),
                required_covariance_ablations: args.required_covariance_ablations.clone(),
                required_case_tags: args.required_case_tags.clone(),
                variables: args.gate_variables.clone(),
                sources: args.gate_sources.clone(),
                strata: args.gate_strata.clone(),
                stations: args.gate_stations.clone(),
                max_domain_candidate_minus_background_mae: args.gate_max_domain_oi_minus_raw_mae,
                max_domain_candidate_minus_barnes_mae: args.gate_max_domain_oi_minus_barnes_mae,
                max_domain_candidate_minus_reference_mae: args
                    .gate_max_domain_oi_minus_reference_mae,
                max_covariance_ablation_candidate_minus_baseline_mae: args
                    .gate_max_covariance_ablation_oi_minus_baseline_mae,
                max_case_mesoanalysis_compute_ms: args.gate_max_case_mesoanalysis_compute_ms,
                min_unique_case_signatures: args.gate_min_unique_case_signatures,
                min_unique_dates: args.gate_min_unique_dates,
                min_unique_cycles: args.gate_min_unique_cycles,
                min_unique_forecast_hours: args.gate_min_unique_forecast_hours,
                min_unique_case_tags: args.gate_min_unique_case_tags,
                min_domain_low_confidence_observation_count: args
                    .gate_min_domain_low_confidence_observation_count,
                min_domain_high_confidence_observation_count: args
                    .gate_min_domain_high_confidence_observation_count,
                max_domain_high_minus_low_confidence_mae: args
                    .gate_max_domain_high_minus_low_confidence_mae,
                max_domain_ranked_high_minus_low_confidence_mae: args
                    .gate_max_domain_ranked_high_minus_low_confidence_mae,
                max_domain_confidence_abs_error_correlation: args
                    .gate_max_domain_confidence_abs_error_correlation,
                max_source_candidate_minus_background_mae: args.gate_max_source_oi_minus_raw_mae,
                max_source_candidate_minus_barnes_mae: args.gate_max_source_oi_minus_barnes_mae,
                min_source_low_confidence_observation_count: args
                    .gate_min_source_low_confidence_observation_count,
                min_source_high_confidence_observation_count: args
                    .gate_min_source_high_confidence_observation_count,
                max_source_high_minus_low_confidence_mae: args
                    .gate_max_source_high_minus_low_confidence_mae,
                max_source_ranked_high_minus_low_confidence_mae: args
                    .gate_max_source_ranked_high_minus_low_confidence_mae,
                max_source_confidence_abs_error_correlation: args
                    .gate_max_source_confidence_abs_error_correlation,
                max_stratum_candidate_minus_background_mae: args.gate_max_stratum_oi_minus_raw_mae,
                max_stratum_candidate_minus_barnes_mae: args.gate_max_stratum_oi_minus_barnes_mae,
                min_stratum_low_confidence_observation_count: args
                    .gate_min_stratum_low_confidence_observation_count,
                min_stratum_high_confidence_observation_count: args
                    .gate_min_stratum_high_confidence_observation_count,
                max_stratum_high_minus_low_confidence_mae: args
                    .gate_max_stratum_high_minus_low_confidence_mae,
                max_stratum_ranked_high_minus_low_confidence_mae: args
                    .gate_max_stratum_ranked_high_minus_low_confidence_mae,
                max_stratum_confidence_abs_error_correlation: args
                    .gate_max_stratum_confidence_abs_error_correlation,
                min_station_observation_count: args.gate_min_station_observation_count,
                max_station_candidate_minus_background_mae: args.gate_max_station_oi_minus_raw_mae,
                max_station_analysis_mae: args.gate_max_station_analysis_mae,
                max_station_abs_analysis_bias: args.gate_max_station_abs_analysis_bias,
            },
        ));
    }
    write_surface_mesoanalysis_calibration_report(&args.out, &report)?;
    if let Some(path) = args.innovation_history_out.as_deref() {
        let incoming_history = build_surface_mesoanalysis_innovation_history(&report);
        let existing_history = args
            .innovation_history_in
            .as_deref()
            .filter(|path| path.exists())
            .map(read_surface_mesoanalysis_innovation_history)
            .transpose()?;
        let history = merge_surface_mesoanalysis_innovation_history(
            existing_history,
            incoming_history,
            args.innovation_history_max_entries_per_series,
        );
        write_surface_mesoanalysis_innovation_history(path, &history)?;
        if let Some(index_dir) = args.innovation_wxstore_index_dir.as_deref() {
            write_surface_mesoanalysis_innovation_wxstore_index(index_dir, &history)?;
        }
    }
    println!("{}", serde_json::to_string_pretty(&report)?);

    if report.loaded_case_count < args.min_case_count {
        return Err(format!(
            "calibration matrix loaded {} cases, below required minimum {}",
            report.loaded_case_count, args.min_case_count
        )
        .into());
    }
    if args.fail_on_skipped && !report.skipped_reports.is_empty() {
        return Err(format!(
            "calibration matrix skipped {} reports",
            report.skipped_reports.len()
        )
        .into());
    }
    if args.fail_on_calibration_gate
        && report
            .calibration_gate
            .as_ref()
            .map(|gate| !gate.passed)
            .unwrap_or(false)
    {
        return Err("surface mesoanalysis calibration gate failed".into());
    }
    if args.innovation_query_history.is_some() {
        emit_innovation_history_query(&args)?;
    }
    Ok(())
}

fn emit_innovation_history_query(args: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let path = args
        .innovation_query_history
        .as_deref()
        .ok_or("--innovation-query-history is required for innovation queries")?;
    let history = read_surface_mesoanalysis_innovation_history(path)?;
    if let Some(index_dir) = args.innovation_wxstore_index_dir.as_deref() {
        write_surface_mesoanalysis_innovation_wxstore_index(index_dir, &history)?;
    }
    let report = query_surface_mesoanalysis_innovation_history(
        &history,
        SurfaceMesoanalysisInnovationQueryRequest {
            stations: args.innovation_query_stations.clone(),
            sources: args.innovation_query_sources.clone(),
            variables: args.innovation_query_variables.clone(),
            min_case_count: args.innovation_query_min_case_count,
            top: args.innovation_query_top,
        },
    );
    if let Some(path) = args.innovation_query_out.as_deref() {
        write_surface_mesoanalysis_innovation_query_report(path, &report)?;
    } else {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_negative_gate_thresholds_as_values() {
        let args = Cli::try_parse_from([
            "surface-mesoanalysis-calibration",
            "--run-report",
            "target/run_report.json",
            "--innovation-history-in",
            "target/rolling_innovation_history.json",
            "--innovation-history-out",
            "target/innovation_history.json",
            "--innovation-history-max-entries-per-series",
            "365",
            "--innovation-wxstore-index-dir",
            "target/innovation_wxstore_index",
            "--innovation-query-history",
            "target/innovation_history.json",
            "--innovation-query-out",
            "target/innovation_query.json",
            "--innovation-query-station",
            "aviation_weather_metar_conus::KP69,KOUN",
            "--innovation-query-source",
            "aviation_weather_metar_conus",
            "--innovation-query-variable",
            "temperature_c,wind_speed_ms",
            "--innovation-query-min-case-count",
            "2",
            "--innovation-query-top",
            "5",
            "--gate-max-domain-oi-minus-raw-mae",
            "-0.1",
            "--gate-max-domain-ranked-high-minus-low-confidence-mae",
            "-0.2",
            "--gate-max-domain-confidence-abs-error-correlation",
            "-0.3",
            "--gate-max-covariance-ablation-oi-minus-baseline-mae",
            "-0.4",
            "--gate-max-source-oi-minus-barnes-mae",
            "-0.5",
            "--gate-max-stratum-oi-minus-raw-mae",
            "-0.6",
            "--gate-max-stratum-ranked-high-minus-low-confidence-mae",
            "-0.7",
            "--gate-station",
            "aviation_weather_metar_conus::KP69",
            "--gate-min-station-observation-count",
            "1",
            "--gate-max-station-oi-minus-raw-mae",
            "-0.8",
            "--gate-max-station-analysis-mae",
            "2.0",
            "--gate-max-station-abs-analysis-bias",
            "1.5",
            "--require-case-tag",
            "regime=dryline,regime=nocturnal_llj",
            "--gate-min-unique-case-tags",
            "2",
        ])
        .expect("negative calibration thresholds should parse as option values");

        assert_eq!(args.gate_max_domain_oi_minus_raw_mae, Some(-0.1));
        assert_eq!(
            args.innovation_history_in,
            Some(PathBuf::from("target/rolling_innovation_history.json"))
        );
        assert_eq!(args.innovation_history_max_entries_per_series, Some(365));
        assert_eq!(
            args.innovation_wxstore_index_dir,
            Some(PathBuf::from("target/innovation_wxstore_index"))
        );
        assert_eq!(
            args.innovation_history_out,
            Some(PathBuf::from("target/innovation_history.json"))
        );
        assert_eq!(
            args.innovation_query_history,
            Some(PathBuf::from("target/innovation_history.json"))
        );
        assert_eq!(
            args.innovation_query_out,
            Some(PathBuf::from("target/innovation_query.json"))
        );
        assert_eq!(
            args.innovation_query_stations,
            vec![
                "aviation_weather_metar_conus::KP69".to_string(),
                "KOUN".to_string()
            ]
        );
        assert_eq!(
            args.innovation_query_sources,
            vec!["aviation_weather_metar_conus".to_string()]
        );
        assert_eq!(
            args.innovation_query_variables,
            vec!["temperature_c".to_string(), "wind_speed_ms".to_string()]
        );
        assert_eq!(args.innovation_query_min_case_count, Some(2));
        assert_eq!(args.innovation_query_top, 5);
        assert_eq!(
            args.gate_max_domain_ranked_high_minus_low_confidence_mae,
            Some(-0.2)
        );
        assert_eq!(
            args.gate_max_domain_confidence_abs_error_correlation,
            Some(-0.3)
        );
        assert_eq!(
            args.gate_max_covariance_ablation_oi_minus_baseline_mae,
            Some(-0.4)
        );
        assert_eq!(args.gate_max_source_oi_minus_barnes_mae, Some(-0.5));
        assert_eq!(args.gate_max_stratum_oi_minus_raw_mae, Some(-0.6));
        assert_eq!(
            args.gate_max_stratum_ranked_high_minus_low_confidence_mae,
            Some(-0.7)
        );
        assert_eq!(
            args.gate_stations,
            vec!["aviation_weather_metar_conus::KP69".to_string()]
        );
        assert_eq!(args.gate_min_station_observation_count, Some(1));
        assert_eq!(args.gate_max_station_oi_minus_raw_mae, Some(-0.8));
        assert_eq!(args.gate_max_station_analysis_mae, Some(2.0));
        assert_eq!(args.gate_max_station_abs_analysis_bias, Some(1.5));
        assert_eq!(
            args.required_case_tags,
            vec![
                "regime=dryline".to_string(),
                "regime=nocturnal_llj".to_string()
            ]
        );
        assert_eq!(args.gate_min_unique_case_tags, Some(2));
    }
}
