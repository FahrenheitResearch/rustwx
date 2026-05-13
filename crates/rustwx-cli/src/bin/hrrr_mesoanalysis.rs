use chrono::{DateTime, Duration, NaiveDate, Timelike, Utc};
use clap::Parser;
use rustwx_calc::{MesoanalysisConfig, MesoanalysisCovarianceKernel, MesoanalysisMethod};
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::default_proof_cache_dir;
use rustwx_products::gridded::{load_surface_geometry_from_latest, resolve_model_run};
use rustwx_products::mesoanalysis::{
    CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS, RunnerMesoObservationLoadOptions,
    RunnerMesoObservationProfile, RunnerObservationSourceSummary,
    SurfaceMesoanalysisBenchmarkSummary, SurfaceMesoanalysisExternalReferenceComparison,
    SurfaceMesoanalysisExternalReferenceDescriptor, SurfaceMesoanalysisGridExportRequest,
    SurfaceMesoanalysisHoldoutStrategy, SurfaceMesoanalysisHoldoutValidationSummary,
    SurfaceMesoanalysisRepeatedHoldoutBenchmarkSummary,
    SurfaceMesoanalysisRepeatedHoldoutValidationSummary, SurfaceMesoanalysisValidationComparison,
    SurfaceMesoanalysisValidationGate, SurfaceMesoanalysisValidationGateThresholds,
    SurfaceMesoanalysisValidationSummary, VariableValidationSummary,
    benchmark_surface_mesoanalysis_repeated_holdout_validations,
    benchmark_surface_mesoanalysis_validations, compare_surface_mesoanalysis_to_external_reference,
    compare_surface_mesoanalysis_validations, compute_surface_mesoanalysis_from_fields,
    compute_surface_mesoanalysis_holdout_validation_with_strategy,
    compute_surface_mesoanalysis_repeated_holdout_validation_with_strategy,
    evaluate_surface_mesoanalysis_validation_gate, load_runner_meso_observations_with_options,
    summarize_surface_mesoanalysis_with_validation_and_holdout_strategy,
    surface_mesoanalysis_holdout_observations_with_strategy,
    validate_surface_mesoanalysis_at_observations, validate_surface_reference_at_observations,
    write_surface_mesoanalysis_grid_export, write_surface_mesoanalysis_report,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "surface-mesoanalysis",
    about = "Native Rust model-agnostic surface objective analysis from rustwx-runner direct observations"
)]
struct Cli {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long)]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    model_source: SourceId,
    #[arg(long = "observations-json")]
    observations_json: Vec<PathBuf>,
    #[arg(long)]
    observations_root: Option<PathBuf>,
    #[arg(long = "obs-source", value_delimiter = ',', num_args = 1..)]
    obs_sources: Vec<String>,
    #[arg(long, default_value = "surface_meso_conus")]
    obs_profile: RunnerMesoObservationProfile,
    #[arg(long = "case-tag", value_delimiter = ',', num_args = 1..)]
    case_tags: Vec<String>,
    #[arg(long, default_value = "target\\surface_mesoanalysis")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = 75.0)]
    radius_km: f64,
    #[arg(long, default_value_t = 2500.0)]
    kappa_km2: f64,
    #[arg(long, default_value_t = 2)]
    barnes_passes: u8,
    #[arg(long, default_value_t = 0.3)]
    second_pass_gamma: f64,
    #[arg(long, default_value_t = 1)]
    min_neighbors: usize,
    #[arg(long, default_value_t = 35.0)]
    background_radius_km: f64,
    #[arg(long, default_value = "barnes")]
    analysis_method: MesoanalysisMethod,
    #[arg(long, default_value_t = 15.0)]
    oi_length_scale_km: f64,
    #[arg(long, default_value_t = 1.0)]
    oi_background_error_temperature_c: f64,
    #[arg(long, default_value_t = 1.2)]
    oi_background_error_dewpoint_c: f64,
    #[arg(long, default_value_t = 1.5)]
    oi_background_error_wind_ms: f64,
    #[arg(long, default_value_t = 3.0)]
    oi_background_error_mslp_hpa: f64,
    #[arg(long, default_value = "exponential")]
    oi_covariance_kernel: MesoanalysisCovarianceKernel,
    #[arg(long, default_value_t = 1.2)]
    oi_observation_error_temperature_c: f64,
    #[arg(long, default_value_t = 1.6)]
    oi_observation_error_dewpoint_c: f64,
    #[arg(long, default_value_t = 2.0)]
    oi_observation_error_wind_ms: f64,
    #[arg(long, default_value_t = 1.5)]
    oi_observation_error_mslp_hpa: f64,
    #[arg(long, default_value_t = 2.5)]
    oi_flow_anisotropy_ratio: f64,
    #[arg(long, default_value_t = 75.0)]
    oi_terrain_pressure_scale_hpa: f64,
    #[arg(long, default_value_t = 32)]
    oi_max_observations_per_grid_cell: usize,
    #[arg(long, default_value_t = 1.0e-4)]
    oi_min_target_correlation: f64,
    #[arg(long, default_value_t = 1.0e-8)]
    oi_matrix_jitter_fraction: f64,
    #[arg(long, default_value_t = 6.0)]
    oi_gross_error_sigma: f64,
    #[arg(long, default_value_t = 25.0)]
    oi_gross_error_buddy_radius_km: f64,
    #[arg(long, default_value_t = 1)]
    oi_gross_error_buddy_min_neighbors: usize,
    #[arg(long, default_value_t = 2.5)]
    oi_gross_error_buddy_agreement_sigma: f64,
    #[arg(long, default_value_t = 1.0)]
    oi_max_local_innovation_factor: f64,
    #[arg(long, default_value_t = 0.10)]
    holdout_fraction: f64,
    #[arg(long, default_value_t = 20260512)]
    holdout_seed: u64,
    #[arg(long, default_value = "station_hash")]
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
    #[arg(long, default_value_t = 1)]
    holdout_repeat_count: usize,
    #[arg(long, default_value_t = 10)]
    min_holdout_observations: usize,
    #[arg(long, default_value_t = false)]
    no_holdout_validation: bool,
    #[arg(long)]
    obs_reference_time: Option<String>,
    #[arg(long, default_value_t = 90)]
    max_obs_age_minutes: i64,
    #[arg(long, default_value_t = 5)]
    allow_future_obs_minutes: i64,
    #[arg(long, default_value_t = 60.0)]
    obs_time_weight_half_life_minutes: f64,
    #[arg(long, default_value_t = 2.0)]
    obs_max_time_error_inflation_factor: f64,
    #[arg(long, default_value_t = false)]
    no_time_filter: bool,
    #[arg(long)]
    grid_export_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_grid_export: bool,
    #[arg(long, default_value_t = false)]
    compare_barnes_baseline: bool,
    #[arg(long, default_value_t = false)]
    compare_isotropic_oi_baseline: bool,
    #[arg(long = "external-reference-model", value_delimiter = ',', num_args = 1..)]
    external_reference_models: Vec<ModelId>,
    #[arg(long, default_value = "nomads")]
    external_reference_source: SourceId,
    #[arg(long)]
    external_reference_product: Option<String>,
    #[arg(long, default_value_t = 10)]
    gate_min_sampled_observations: usize,
    #[arg(long, default_value_t = 0)]
    gate_max_skipped_observations: usize,
    #[arg(long, default_value_t = 10.0)]
    gate_max_nearest_grid_distance_km: f64,
    #[arg(long, default_value_t = 2.0)]
    gate_max_temperature_mae_c: f64,
    #[arg(long, default_value_t = 2.5)]
    gate_max_dewpoint_mae_c: f64,
    #[arg(long, default_value_t = 2.0)]
    gate_max_wind_speed_mae_ms: f64,
    #[arg(long, default_value_t = false)]
    fail_on_validation_gate: bool,
}

#[derive(Debug, Serialize)]
struct SurfaceMesoanalysisRunReport {
    schema: String,
    model: String,
    date: String,
    cycle: Option<u8>,
    forecast_hour: u16,
    model_source: String,
    model_cycle: String,
    model_load_mode: String,
    surface_fetch_ms: u128,
    surface_decode_ms: u128,
    output_report_path: PathBuf,
    agent_packet_path: PathBuf,
    grid_export_manifest_path: Option<PathBuf>,
    grid_export_field_count: usize,
    observation_paths: Vec<PathBuf>,
    observation_profile: RunnerMesoObservationProfile,
    case_tags: Vec<String>,
    obs_reference_time: Option<String>,
    max_obs_age_minutes: Option<i64>,
    allow_future_obs_minutes: i64,
    obs_time_weight_half_life_minutes: Option<f64>,
    obs_max_time_error_inflation_factor: f64,
    mesoanalysis_config: MesoanalysisConfig,
    mesoanalysis_compute_ms: u128,
    validation_gate: SurfaceMesoanalysisValidationGate,
    #[serde(skip_serializing_if = "Option::is_none")]
    barnes_baseline_comparison: Option<SurfaceMesoanalysisBaselineComparison>,
    #[serde(skip_serializing_if = "Option::is_none")]
    covariance_ablation_comparison: Option<SurfaceMesoanalysisBaselineComparison>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    external_reference_comparisons: Vec<SurfaceMesoanalysisExternalReferenceComparison>,
    observation_sources: Vec<RunnerObservationSourceSummary>,
    mesoanalysis: rustwx_products::mesoanalysis::SurfaceMesoanalysisReport,
}

#[derive(Debug, Serialize)]
struct SurfaceMesoanalysisBaselineComparison {
    schema: String,
    baseline_label: String,
    candidate_label: String,
    validation_mode: String,
    baseline_config: MesoanalysisConfig,
    baseline_compute_ms: u128,
    baseline_validation: SurfaceMesoanalysisValidationSummary,
    validation_comparison: SurfaceMesoanalysisValidationComparison,
    benchmark_summary: SurfaceMesoanalysisBenchmarkSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    baseline_holdout_validation: Option<SurfaceMesoanalysisHoldoutValidationSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    holdout_validation_comparison: Option<SurfaceMesoanalysisValidationComparison>,
    #[serde(skip_serializing_if = "Option::is_none")]
    holdout_benchmark_summary: Option<SurfaceMesoanalysisBenchmarkSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    baseline_repeated_holdout_validation:
        Option<SurfaceMesoanalysisRepeatedHoldoutValidationSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repeated_holdout_benchmark_summary: Option<SurfaceMesoanalysisRepeatedHoldoutBenchmarkSummary>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    fs::create_dir_all(&args.out_dir)?;
    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    let observation_paths = resolve_observation_paths(&args)?;
    if observation_paths.is_empty() {
        return Err("no observation files were found".into());
    }

    let latest = resolve_model_run(
        args.model,
        args.date.as_str(),
        args.cycle,
        args.forecast_hour,
        args.model_source,
    )?;
    let loaded = load_surface_geometry_from_latest(
        latest,
        args.forecast_hour,
        None,
        &cache_dir,
        !args.no_cache,
    )?;
    let model_valid_time = model_valid_time_utc(
        loaded.latest.cycle.date_yyyymmdd.as_str(),
        loaded.latest.cycle.hour_utc,
        args.forecast_hour,
    )?;
    let obs_reference_time = if args.no_time_filter {
        None
    } else if let Some(raw) = args.obs_reference_time.as_deref() {
        Some(parse_utc_timestamp(raw)?)
    } else {
        Some(model_valid_time.clone())
    };
    let observation_options = RunnerMesoObservationLoadOptions {
        reference_time_utc: obs_reference_time.clone(),
        max_age_minutes: if args.no_time_filter {
            None
        } else {
            Some(args.max_obs_age_minutes)
        },
        allow_future_minutes: args.allow_future_obs_minutes,
        time_weight_half_life_minutes: if args.obs_time_weight_half_life_minutes > 0.0 {
            Some(args.obs_time_weight_half_life_minutes)
        } else {
            None
        },
        max_time_error_inflation_factor: args.obs_max_time_error_inflation_factor,
        profile: args.obs_profile,
    };
    let observations =
        load_runner_meso_observations_with_options(&observation_paths, &observation_options)?;
    let config = MesoanalysisConfig {
        method: args.analysis_method,
        barnes_radius_km: args.radius_km,
        barnes_kappa_km2: args.kappa_km2,
        barnes_passes: args.barnes_passes,
        barnes_second_pass_gamma: args.second_pass_gamma,
        min_neighbors: args.min_neighbors,
        background_search_radius_km: args.background_radius_km,
        oi_length_scale_km: args.oi_length_scale_km,
        oi_background_error_temperature_c: args.oi_background_error_temperature_c,
        oi_background_error_dewpoint_c: args.oi_background_error_dewpoint_c,
        oi_background_error_wind_ms: args.oi_background_error_wind_ms,
        oi_background_error_mslp_hpa: args.oi_background_error_mslp_hpa,
        oi_covariance_kernel: args.oi_covariance_kernel,
        oi_observation_error_temperature_c: args.oi_observation_error_temperature_c,
        oi_observation_error_dewpoint_c: args.oi_observation_error_dewpoint_c,
        oi_observation_error_wind_ms: args.oi_observation_error_wind_ms,
        oi_observation_error_mslp_hpa: args.oi_observation_error_mslp_hpa,
        oi_flow_anisotropy_ratio: args.oi_flow_anisotropy_ratio,
        oi_terrain_pressure_scale_hpa: args.oi_terrain_pressure_scale_hpa,
        oi_max_observations_per_grid_cell: args.oi_max_observations_per_grid_cell,
        oi_min_target_correlation: args.oi_min_target_correlation,
        oi_matrix_jitter_fraction: args.oi_matrix_jitter_fraction,
        oi_gross_error_sigma: args.oi_gross_error_sigma,
        oi_gross_error_buddy_radius_km: args.oi_gross_error_buddy_radius_km,
        oi_gross_error_buddy_min_neighbors: args.oi_gross_error_buddy_min_neighbors,
        oi_gross_error_buddy_agreement_sigma: args.oi_gross_error_buddy_agreement_sigma,
        oi_max_local_innovation_factor: args.oi_max_local_innovation_factor,
        ..MesoanalysisConfig::default()
    };
    let mesoanalysis_compute_start = Instant::now();
    let fields = compute_surface_mesoanalysis_from_fields(
        &loaded.surface_decode.value,
        &observations.observations,
        config,
    )?;
    let mesoanalysis_compute_ms = mesoanalysis_compute_start.elapsed().as_millis();
    let mut mesoanalysis = summarize_surface_mesoanalysis_with_validation_and_holdout_strategy(
        &loaded.surface_decode.value,
        &fields,
        &observations.observations,
        config,
        if args.no_holdout_validation {
            0.0
        } else {
            args.holdout_fraction
        },
        args.holdout_seed,
        args.min_holdout_observations,
        args.holdout_strategy,
    )?;
    if !args.no_holdout_validation && args.holdout_repeat_count > 1 {
        mesoanalysis.repeated_holdout_validation =
            compute_surface_mesoanalysis_repeated_holdout_validation_with_strategy(
                &loaded.surface_decode.value,
                &observations.observations,
                config,
                args.holdout_fraction,
                args.holdout_seed,
                args.holdout_repeat_count,
                args.min_holdout_observations,
                args.holdout_strategy,
            )?;
    }
    let validation = mesoanalysis
        .validation
        .as_ref()
        .ok_or("mesoanalysis validation summary was not generated")?;
    let validation_gate = evaluate_surface_mesoanalysis_validation_gate(
        validation,
        SurfaceMesoanalysisValidationGateThresholds {
            min_sampled_observations: args.gate_min_sampled_observations,
            max_skipped_observations: args.gate_max_skipped_observations,
            max_nearest_grid_distance_km: args.gate_max_nearest_grid_distance_km,
            max_temperature_mean_abs_error_c: args.gate_max_temperature_mae_c,
            max_dewpoint_mean_abs_error_c: args.gate_max_dewpoint_mae_c,
            max_wind_speed_mean_abs_error_ms: args.gate_max_wind_speed_mae_ms,
        },
    );
    let barnes_baseline_comparison = if args.compare_barnes_baseline {
        Some(compute_barnes_baseline_comparison(
            &loaded.surface_decode.value,
            &observations.observations,
            config,
            validation,
            mesoanalysis.holdout_validation.as_ref(),
            mesoanalysis.repeated_holdout_validation.as_ref(),
            if args.no_holdout_validation {
                0.0
            } else {
                args.holdout_fraction
            },
            args.holdout_seed,
            args.holdout_strategy,
            args.holdout_repeat_count,
            args.min_holdout_observations,
        )?)
    } else {
        None
    };
    let covariance_ablation_comparison = if args.compare_isotropic_oi_baseline
        && args.analysis_method == MesoanalysisMethod::OptimalInterpolation
    {
        Some(compute_isotropic_oi_baseline_comparison(
            &loaded.surface_decode.value,
            &observations.observations,
            config,
            validation,
            mesoanalysis.holdout_validation.as_ref(),
            mesoanalysis.repeated_holdout_validation.as_ref(),
            if args.no_holdout_validation {
                0.0
            } else {
                args.holdout_fraction
            },
            args.holdout_seed,
            args.holdout_strategy,
            args.holdout_repeat_count,
            args.min_holdout_observations,
        )?)
    } else {
        None
    };
    let external_reference_comparisons = compute_external_reference_comparisons(
        &args,
        &cache_dir,
        &observations.observations,
        config,
        &model_valid_time,
        validation,
        mesoanalysis.holdout_validation.as_ref(),
    )?;
    let output_report_path = args.out_dir.join("surface_mesoanalysis_report.json");
    write_surface_mesoanalysis_report(&output_report_path, &mesoanalysis)?;
    let grid_export = if args.no_grid_export {
        None
    } else {
        let grid_export_dir = args
            .grid_export_dir
            .clone()
            .unwrap_or_else(|| args.out_dir.join("wxstore_grid_export"));
        Some(write_surface_mesoanalysis_grid_export(
            &SurfaceMesoanalysisGridExportRequest {
                model: args.model.to_string(),
                run_id: run_id_for_latest(
                    loaded.latest.model.as_str(),
                    loaded.latest.cycle.date_yyyymmdd.as_str(),
                    loaded.latest.cycle.hour_utc,
                ),
                member: "control".to_string(),
                date_yyyymmdd: loaded.latest.cycle.date_yyyymmdd.clone(),
                cycle_utc: loaded.latest.cycle.hour_utc,
                source: loaded.latest.source.to_string(),
                forecast_hour: args.forecast_hour,
                valid_time: model_valid_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                out_dir: grid_export_dir,
            },
            &loaded.surface_decode.value,
            &fields,
        )?)
    };
    let agent_packet_path = args.out_dir.join("mesoanalysis_agent_packet.json");

    let run_report = SurfaceMesoanalysisRunReport {
        schema: "rustwx.surface_mesoanalysis.run_report.v1".to_string(),
        model: args.model.to_string(),
        date: args.date,
        cycle: args.cycle,
        forecast_hour: args.forecast_hour,
        model_source: args.model_source.to_string(),
        model_cycle: format!(
            "{}{:02}z",
            loaded.latest.cycle.date_yyyymmdd, loaded.latest.cycle.hour_utc
        ),
        model_load_mode: "surface_only".to_string(),
        surface_fetch_ms: loaded.fetch_ms,
        surface_decode_ms: loaded.decode_ms,
        output_report_path,
        agent_packet_path,
        grid_export_manifest_path: grid_export
            .as_ref()
            .map(|manifest| manifest.manifest_path.clone()),
        grid_export_field_count: grid_export
            .as_ref()
            .map(|manifest| manifest.fields.len())
            .unwrap_or(0),
        observation_paths,
        observation_profile: args.obs_profile,
        case_tags: normalized_case_tags(&args.case_tags),
        obs_reference_time: obs_reference_time.map(|value| value.to_rfc3339()),
        max_obs_age_minutes: observation_options.max_age_minutes,
        allow_future_obs_minutes: observation_options.allow_future_minutes,
        obs_time_weight_half_life_minutes: observation_options.time_weight_half_life_minutes,
        obs_max_time_error_inflation_factor: observation_options.max_time_error_inflation_factor,
        mesoanalysis_config: config,
        mesoanalysis_compute_ms,
        validation_gate,
        barnes_baseline_comparison,
        covariance_ablation_comparison,
        external_reference_comparisons,
        observation_sources: observations.sources,
        mesoanalysis,
    };
    let run_report_path = args.out_dir.join("run_report.json");
    write_surface_mesoanalysis_agent_packet(&run_report.agent_packet_path, &run_report)?;
    fs::write(&run_report_path, serde_json::to_vec_pretty(&run_report)?)?;
    println!("{}", serde_json::to_string_pretty(&run_report)?);
    if args.fail_on_validation_gate && !run_report.validation_gate.passed {
        return Err("mesoanalysis validation gate failed".into());
    }
    Ok(())
}

fn compute_isotropic_oi_baseline_comparison(
    surface: &rustwx_products::gridded::SurfaceFields,
    observations: &[rustwx_calc::MesoObservation],
    candidate_config: MesoanalysisConfig,
    candidate_validation: &SurfaceMesoanalysisValidationSummary,
    candidate_holdout_validation: Option<&SurfaceMesoanalysisHoldoutValidationSummary>,
    candidate_repeated_holdout_validation: Option<
        &SurfaceMesoanalysisRepeatedHoldoutValidationSummary,
    >,
    holdout_fraction: f64,
    holdout_seed: u64,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
    holdout_repeat_count: usize,
    min_holdout_observations: usize,
) -> Result<SurfaceMesoanalysisBaselineComparison, Box<dyn std::error::Error>> {
    let baseline_config = MesoanalysisConfig {
        method: MesoanalysisMethod::OptimalInterpolation,
        oi_flow_anisotropy_ratio: 1.0,
        oi_terrain_pressure_scale_hpa: 1.0e9,
        ..candidate_config
    };
    compute_config_baseline_comparison(
        surface,
        observations,
        baseline_config,
        "OptimalInterpolation".to_string(),
        "IsotropicOiNoTerrain".to_string(),
        candidate_validation,
        candidate_holdout_validation,
        candidate_repeated_holdout_validation,
        holdout_fraction,
        holdout_seed,
        holdout_strategy,
        holdout_repeat_count,
        min_holdout_observations,
    )
}

fn compute_external_reference_comparisons(
    args: &Cli,
    cache_dir: &Path,
    observations: &[rustwx_calc::MesoObservation],
    config: MesoanalysisConfig,
    model_valid_time: &DateTime<Utc>,
    same_observation_validation: &SurfaceMesoanalysisValidationSummary,
    holdout_validation: Option<&SurfaceMesoanalysisHoldoutValidationSummary>,
) -> Result<Vec<SurfaceMesoanalysisExternalReferenceComparison>, Box<dyn std::error::Error>> {
    if args.external_reference_models.is_empty() {
        return Ok(Vec::new());
    }

    let (validation_mode, validation_observations, candidate_validation) =
        if let Some(holdout) = holdout_validation {
            let holdout_observations = surface_mesoanalysis_holdout_observations_with_strategy(
                observations,
                args.holdout_fraction,
                args.holdout_seed,
                args.min_holdout_observations,
                args.holdout_strategy,
            )
            .ok_or("could not reproduce holdout observations for external reference validation")?;
            (
                "holdout_validation".to_string(),
                holdout_observations,
                &holdout.validation,
            )
        } else {
            (
                "same_observation_validation".to_string(),
                observations.to_vec(),
                same_observation_validation,
            )
        };

    let reference_date = model_valid_time.format("%Y%m%d").to_string();
    let reference_cycle = model_valid_time.hour() as u8;
    let candidate_label = format!("{:?}", config.method);
    let background_label = args.model.to_string();
    let mut comparisons = Vec::new();

    for reference_model in &args.external_reference_models {
        let latest = resolve_model_run(
            *reference_model,
            reference_date.as_str(),
            Some(reference_cycle),
            0,
            args.external_reference_source,
        )?;
        let reference_loaded = load_surface_geometry_from_latest(
            latest,
            0,
            args.external_reference_product.as_deref(),
            cache_dir,
            !args.no_cache,
        )?;
        let reference_validation = validate_surface_reference_at_observations(
            &reference_loaded.surface_decode.value,
            &validation_observations,
        )?;
        let descriptor = SurfaceMesoanalysisExternalReferenceDescriptor {
            reference_label: reference_model.to_string(),
            reference_model: reference_model.to_string(),
            reference_source: reference_loaded.latest.source.to_string(),
            reference_cycle: format!(
                "{}{:02}z",
                reference_loaded.latest.cycle.date_yyyymmdd, reference_loaded.latest.cycle.hour_utc
            ),
            reference_forecast_hour: 0,
            reference_product: reference_loaded.surface_bundle.native_product.clone(),
            candidate_label: candidate_label.clone(),
            background_label: background_label.clone(),
            validation_mode: validation_mode.clone(),
        };
        comparisons.push(compare_surface_mesoanalysis_to_external_reference(
            descriptor,
            candidate_validation,
            &reference_validation,
        ));
    }

    Ok(comparisons)
}

fn parse_utc_timestamp(raw: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn compute_barnes_baseline_comparison(
    surface: &rustwx_products::gridded::SurfaceFields,
    observations: &[rustwx_calc::MesoObservation],
    candidate_config: MesoanalysisConfig,
    candidate_validation: &SurfaceMesoanalysisValidationSummary,
    candidate_holdout_validation: Option<&SurfaceMesoanalysisHoldoutValidationSummary>,
    candidate_repeated_holdout_validation: Option<
        &SurfaceMesoanalysisRepeatedHoldoutValidationSummary,
    >,
    holdout_fraction: f64,
    holdout_seed: u64,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
    holdout_repeat_count: usize,
    min_holdout_observations: usize,
) -> Result<SurfaceMesoanalysisBaselineComparison, Box<dyn std::error::Error>> {
    let baseline_config = MesoanalysisConfig {
        method: MesoanalysisMethod::Barnes,
        ..candidate_config
    };
    compute_config_baseline_comparison(
        surface,
        observations,
        baseline_config,
        format!("{:?}", candidate_config.method),
        "Barnes".to_string(),
        candidate_validation,
        candidate_holdout_validation,
        candidate_repeated_holdout_validation,
        holdout_fraction,
        holdout_seed,
        holdout_strategy,
        holdout_repeat_count,
        min_holdout_observations,
    )
}

fn compute_config_baseline_comparison(
    surface: &rustwx_products::gridded::SurfaceFields,
    observations: &[rustwx_calc::MesoObservation],
    baseline_config: MesoanalysisConfig,
    candidate_label: String,
    baseline_label: String,
    candidate_validation: &SurfaceMesoanalysisValidationSummary,
    candidate_holdout_validation: Option<&SurfaceMesoanalysisHoldoutValidationSummary>,
    candidate_repeated_holdout_validation: Option<
        &SurfaceMesoanalysisRepeatedHoldoutValidationSummary,
    >,
    holdout_fraction: f64,
    holdout_seed: u64,
    holdout_strategy: SurfaceMesoanalysisHoldoutStrategy,
    holdout_repeat_count: usize,
    min_holdout_observations: usize,
) -> Result<SurfaceMesoanalysisBaselineComparison, Box<dyn std::error::Error>> {
    let baseline_start = Instant::now();
    let baseline_fields =
        compute_surface_mesoanalysis_from_fields(surface, observations, baseline_config)?;
    let baseline_compute_ms = baseline_start.elapsed().as_millis();
    let baseline_validation =
        validate_surface_mesoanalysis_at_observations(surface, &baseline_fields, observations)?;
    let validation_mode = "same_observation_validation".to_string();
    let validation_comparison = compare_surface_mesoanalysis_validations(
        candidate_label.clone(),
        candidate_validation,
        baseline_label.clone(),
        &baseline_validation,
    );
    let benchmark_summary = benchmark_surface_mesoanalysis_validations(
        candidate_label.clone(),
        candidate_validation,
        baseline_label.clone(),
        &baseline_validation,
        validation_mode.clone(),
    );
    let baseline_holdout_validation =
        compute_surface_mesoanalysis_holdout_validation_with_strategy(
            surface,
            observations,
            baseline_config,
            holdout_fraction,
            holdout_seed,
            min_holdout_observations,
            holdout_strategy,
        )?;
    let holdout_validation_comparison = candidate_holdout_validation
        .zip(baseline_holdout_validation.as_ref())
        .map(|(candidate, baseline)| {
            compare_surface_mesoanalysis_validations(
                candidate_label.clone(),
                &candidate.validation,
                baseline_label.clone(),
                &baseline.validation,
            )
        });
    let holdout_benchmark_summary = candidate_holdout_validation
        .zip(baseline_holdout_validation.as_ref())
        .map(|(candidate, baseline)| {
            benchmark_surface_mesoanalysis_validations(
                candidate_label.clone(),
                &candidate.validation,
                baseline_label.clone(),
                &baseline.validation,
                "holdout_validation",
            )
        });
    let baseline_repeated_holdout_validation =
        if candidate_repeated_holdout_validation.is_some() && holdout_repeat_count > 1 {
            compute_surface_mesoanalysis_repeated_holdout_validation_with_strategy(
                surface,
                observations,
                baseline_config,
                holdout_fraction,
                holdout_seed,
                holdout_repeat_count,
                min_holdout_observations,
                holdout_strategy,
            )?
        } else {
            None
        };
    let repeated_holdout_benchmark_summary = candidate_repeated_holdout_validation
        .zip(baseline_repeated_holdout_validation.as_ref())
        .map(|(candidate, baseline)| {
            benchmark_surface_mesoanalysis_repeated_holdout_validations(
                candidate_label.clone(),
                candidate,
                baseline_label.clone(),
                baseline,
                "repeated_holdout_validation",
            )
        });

    Ok(SurfaceMesoanalysisBaselineComparison {
        schema: "rustwx.surface_mesoanalysis.baseline_comparison.v1".to_string(),
        baseline_label,
        candidate_label,
        validation_mode,
        baseline_config,
        baseline_compute_ms,
        baseline_validation,
        validation_comparison,
        benchmark_summary,
        baseline_holdout_validation,
        holdout_validation_comparison,
        holdout_benchmark_summary,
        baseline_repeated_holdout_validation,
        repeated_holdout_benchmark_summary,
    })
}

fn write_surface_mesoanalysis_agent_packet(
    path: &Path,
    report: &SurfaceMesoanalysisRunReport,
) -> Result<(), Box<dyn std::error::Error>> {
    let packet = build_surface_mesoanalysis_agent_packet(path, report);
    fs::write(path, serde_json::to_vec_pretty(&packet)?)?;
    Ok(())
}

fn build_surface_mesoanalysis_agent_packet(
    packet_path: &Path,
    report: &SurfaceMesoanalysisRunReport,
) -> Value {
    let run_report_path = packet_path
        .parent()
        .map(|parent| parent.join("run_report.json"))
        .unwrap_or_else(|| PathBuf::from("run_report.json"));
    let validation = report
        .mesoanalysis
        .validation
        .as_ref()
        .map(compact_validation_summary)
        .unwrap_or(Value::Null);
    let holdout_validation = report
        .mesoanalysis
        .holdout_validation
        .as_ref()
        .map(compact_holdout_validation)
        .unwrap_or(Value::Null);
    let repeated_holdout_validation = report
        .mesoanalysis
        .repeated_holdout_validation
        .as_ref()
        .map(compact_repeated_holdout_validation)
        .unwrap_or(Value::Null);
    let baseline = report
        .barnes_baseline_comparison
        .as_ref()
        .map(compact_baseline_comparison)
        .unwrap_or(Value::Null);
    let covariance_ablation = report
        .covariance_ablation_comparison
        .as_ref()
        .map(compact_baseline_comparison)
        .unwrap_or(Value::Null);
    let external_references = if report.external_reference_comparisons.is_empty() {
        Value::Null
    } else {
        json!(report.external_reference_comparisons)
    };
    let confidence_reliability = compact_confidence_reliability_contract(report);

    json!({
        "schema": "rustwx.surface_mesoanalysis.agent_packet.v1",
        "packet_role": "compact LLM-readable surface mesoanalysis decision packet",
        "packet_path": packet_path,
        "contract": {
            "model_agnostic": true,
            "analysis_kind": "surface_adjusted_diagnostic",
            "analysis_semantics": "post-processed near-surface objective analysis; not a dynamically balanced model state",
            "background_model_role": "native dynamically assimilated model background",
            "agent_use": "Use holdout scorecards, source-quality profiles, support/confidence diagnostics, and artifact refs before trusting corrected fields.",
            "confidence_semantics": {
                "grid_confidence_field_kind": "oi_variance_reduction_support_proxy",
                "skill_calibrated_by_default": false,
                "interpretation": "Higher values mean stronger local OI support and lower posterior variance under the configured covariance assumptions, not guaranteed lower held-out error.",
                "trust_requirement": "Prefer corrected fields only where holdout validation, source-quality metadata, and confidence-reliability gates support the case."
            }
        },
        "run": {
            "model": report.model,
            "model_source": report.model_source,
            "model_cycle": report.model_cycle,
            "forecast_hour": report.forecast_hour,
            "date": report.date,
            "cycle": report.cycle,
            "case_tags": report.case_tags,
            "model_load_mode": report.model_load_mode,
            "obs_reference_time": report.obs_reference_time,
            "max_obs_age_minutes": report.max_obs_age_minutes,
            "allow_future_obs_minutes": report.allow_future_obs_minutes,
            "obs_time_weight_half_life_minutes": report.obs_time_weight_half_life_minutes,
            "obs_max_time_error_inflation_factor": report.obs_max_time_error_inflation_factor,
            "surface_fetch_ms": report.surface_fetch_ms,
            "surface_decode_ms": report.surface_decode_ms,
            "mesoanalysis_compute_ms": report.mesoanalysis_compute_ms
        },
        "method": {
            "config": report.mesoanalysis_config,
            "analysis_method": report.mesoanalysis_config.method,
            "covariance_kernel": report.mesoanalysis_config.oi_covariance_kernel,
            "oi_length_scale_km": report.mesoanalysis_config.oi_length_scale_km,
            "terrain_pressure_scale_hpa": report.mesoanalysis_config.oi_terrain_pressure_scale_hpa,
            "flow_anisotropy_ratio": report.mesoanalysis_config.oi_flow_anisotropy_ratio,
            "gross_error_sigma": report.mesoanalysis_config.oi_gross_error_sigma,
            "gross_error_buddy_radius_km": report.mesoanalysis_config.oi_gross_error_buddy_radius_km,
            "gross_error_buddy_min_neighbors": report.mesoanalysis_config.oi_gross_error_buddy_min_neighbors,
            "gross_error_buddy_agreement_sigma": report.mesoanalysis_config.oi_gross_error_buddy_agreement_sigma,
            "max_local_observations": report.mesoanalysis_config.oi_max_observations_per_grid_cell
        },
        "observations": {
            "profile": report.observation_profile,
            "observation_count": report.mesoanalysis.observation_count,
            "source_count": report.mesoanalysis.source_count,
            "contributing_sources": report.mesoanalysis.contributing_sources,
            "paths": report.observation_paths,
            "source_summaries": compact_observation_sources(&report.observation_sources),
            "source_quality_note": "Observation-error defaults are source-class and representativeness priors; accepted observations may be time-weighted by model-valid-time age before correction."
        },
        "validation": {
            "gate": report.validation_gate,
            "confidence_reliability": confidence_reliability,
            "same_observation": validation,
            "holdout": holdout_validation,
            "repeated_holdout": repeated_holdout_validation,
            "barnes_baseline": baseline,
            "covariance_ablation": covariance_ablation,
            "external_references": external_references
        },
        "fields": {
            "grid_cells": report.mesoanalysis.grid_cells,
            "field_summaries": report.mesoanalysis.fields,
            "diagnostics": report.mesoanalysis.diagnostics
        },
        "artifacts": [
            {
                "id": "surface_mesoanalysis_agent_packet",
                "kind": "compact_packet",
                "path": packet_path,
                "description": "Compact LLM-readable packet derived from the full run report."
            },
            {
                "id": "surface_mesoanalysis_run_report",
                "kind": "full_json_report",
                "path": run_report_path,
                "description": "Full run report; includes full validation samples and nested diagnostics."
            },
            {
                "id": "surface_mesoanalysis_report",
                "kind": "analysis_report",
                "path": report.output_report_path,
                "description": "Surface mesoanalysis field summaries, diagnostics, validation, and holdout records."
            },
            {
                "id": "surface_mesoanalysis_grid_manifest",
                "kind": "wxstore_grid_export",
                "path": report.grid_export_manifest_path,
                "field_count": report.grid_export_field_count,
                "description": "WxStore-style grid manifest for corrected fields, increments, confidence, and neighbor counts."
            }
        ],
        "agent_notes": [
            "Prefer repeated or spatial/source holdout scorecards over same-observation fit when judging analysis skill.",
            "Barnes same-observation error can look better because it strongly fits stations used in the correction.",
            "Use confidence fields as local OI support diagnostics unless a confidence-reliability gate has passed for the case.",
            "Use source_quality_class, representativeness_class, quality_weight, time-weight summaries, and duplicate_filtered_count to understand why a source was trusted or damped.",
            "Use strata_summaries to check whether skill holds by source quality, exposure/representativeness, terrain-pressure class, and observation-age bucket.",
            "Use covariance_ablation when present to verify whether flow anisotropy and terrain-pressure damping helped the case."
        ],
        "caveats": [
            "This packet describes a near-surface diagnostic analysis, not a full 3D variational model analysis.",
            "Corrected surface fields should not be fed back into balanced 3D derived fields without a separate consistency step.",
            "Single-case skill is not universal calibration; use multi-case and source/spatial holdouts before changing defaults."
        ]
    })
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

fn compact_observation_sources(sources: &[RunnerObservationSourceSummary]) -> Vec<Value> {
    sources
        .iter()
        .map(|source| {
            json!({
                "source": source.source,
                "source_name": source.source_name,
                "kind": source.kind,
                "source_quality_class": source.source_quality_class,
                "representativeness_class": source.representativeness_class,
                "correction_role": source.correction_role,
                "quality_weight": source.quality_weight,
                "default_observation_error": {
                    "temperature_c": source.default_temperature_error_c,
                    "dewpoint_c": source.default_dewpoint_error_c,
                    "wind_ms": source.default_wind_error_ms,
                    "mean_sea_level_pressure_hpa": source.default_mean_sea_level_pressure_error_hpa
                },
                "observation_count": source.observation_count,
                "accepted_for_mesoanalysis": source.accepted_for_mesoanalysis,
                "accepted_mean_sea_level_pressure_count": source.accepted_mean_sea_level_pressure_count,
                "accepted_station_pressure_count": source.accepted_station_pressure_count,
                "accepted_altimeter_count": source.accepted_altimeter_count,
                "accepted_min_observation_age_minutes": source.accepted_min_observation_age_minutes,
                "accepted_mean_observation_age_minutes": source.accepted_mean_observation_age_minutes,
                "accepted_max_observation_age_minutes": source.accepted_max_observation_age_minutes,
                "mean_time_weight": source.mean_time_weight,
                "min_time_weight": source.min_time_weight,
                "duplicate_filtered_count": source.duplicate_filtered_count,
                "profile_filtered_count": source.profile_filtered_count,
                "time_filtered_count": source.time_filtered_count,
                "missing_or_invalid_time_count": source.missing_or_invalid_time_count,
                "skipped_for_kind": source.skipped_for_kind,
                "path": source.path
            })
        })
        .collect()
}

fn compact_baseline_comparison(comparison: &SurfaceMesoanalysisBaselineComparison) -> Value {
    json!({
        "schema": comparison.schema,
        "candidate_label": comparison.candidate_label,
        "baseline_label": comparison.baseline_label,
        "validation_mode": comparison.validation_mode,
        "baseline_compute_ms": comparison.baseline_compute_ms,
        "same_observation_benchmark": comparison.benchmark_summary,
        "same_observation_comparison": comparison.validation_comparison,
        "holdout_benchmark": comparison.holdout_benchmark_summary,
        "holdout_comparison": comparison.holdout_validation_comparison,
        "baseline_holdout": comparison
            .baseline_holdout_validation
            .as_ref()
            .map(compact_holdout_validation),
        "baseline_repeated_holdout": comparison
            .baseline_repeated_holdout_validation
            .as_ref()
            .map(compact_repeated_holdout_validation),
        "repeated_holdout_benchmark": comparison.repeated_holdout_benchmark_summary
    })
}

fn compact_holdout_validation(validation: &SurfaceMesoanalysisHoldoutValidationSummary) -> Value {
    json!({
        "schema": validation.schema,
        "requested_fraction": validation.requested_fraction,
        "seed": validation.seed,
        "strategy": validation.strategy,
        "min_holdout_observations": validation.min_holdout_observations,
        "training_observation_count": validation.training_observation_count,
        "holdout_observation_count": validation.holdout_observation_count,
        "selection_rule": validation.selection_rule,
        "validation": compact_validation_summary(&validation.validation)
    })
}

fn compact_repeated_holdout_validation(
    validation: &SurfaceMesoanalysisRepeatedHoldoutValidationSummary,
) -> Value {
    json!({
        "schema": validation.schema,
        "requested_fraction": validation.requested_fraction,
        "seed": validation.seed,
        "repeat_count": validation.repeat_count,
        "completed_fold_count": validation.completed_fold_count,
        "strategy": validation.strategy,
        "min_holdout_observations": validation.min_holdout_observations,
        "selection_rule": validation.selection_rule,
        "temperature_c": validation.temperature_c,
        "dewpoint_c": validation.dewpoint_c,
        "wind_speed_ms": validation.wind_speed_ms,
        "mean_sea_level_pressure_hpa": validation.mean_sea_level_pressure_hpa,
        "folds": validation
            .folds
            .iter()
            .map(compact_holdout_validation)
            .collect::<Vec<_>>()
    })
}

fn compact_validation_summary(validation: &SurfaceMesoanalysisValidationSummary) -> Value {
    json!({
        "observation_count": validation.observation_count,
        "sampled_observation_count": validation.sampled_observation_count,
        "skipped_observation_count": validation.skipped_observation_count,
        "max_nearest_grid_distance_km": validation.max_nearest_grid_distance_km,
        "temperature_c": validation.temperature_c,
        "dewpoint_c": validation.dewpoint_c,
        "wind_speed_ms": validation.wind_speed_ms,
        "mean_sea_level_pressure_hpa": validation.mean_sea_level_pressure_hpa,
        "source_summaries": validation.source_summaries,
        "strata_summaries": validation.strata_summaries
    })
}

fn compact_confidence_reliability_contract(report: &SurfaceMesoanalysisRunReport) -> Value {
    let (validation_mode, validation) =
        if let Some(holdout) = report.mesoanalysis.holdout_validation.as_ref() {
            ("holdout_validation", Some(&holdout.validation))
        } else if let Some(validation) = report.mesoanalysis.validation.as_ref() {
            ("same_observation_validation", Some(validation))
        } else {
            ("none", None)
        };
    let Some(validation) = validation else {
        return json!({
            "schema": "rustwx.surface_mesoanalysis.confidence_reliability_packet.v1",
            "validation_mode": validation_mode,
            "status": "untestable",
            "semantic_label": "support_index",
            "fields": {}
        });
    };
    let fields = vec![
        (
            "temperature_c",
            compact_variable_confidence_reliability(&validation.temperature_c),
        ),
        (
            "dewpoint_c",
            compact_variable_confidence_reliability(&validation.dewpoint_c),
        ),
        (
            "wind_speed_ms",
            compact_variable_confidence_reliability(&validation.wind_speed_ms),
        ),
    ];
    let failed_count = fields
        .iter()
        .filter(|(_, field)| field.get("status").and_then(Value::as_str) == Some("failed"))
        .count();
    let passed_count = fields
        .iter()
        .filter(|(_, field)| field.get("status").and_then(Value::as_str) == Some("passed"))
        .count();
    let (status, semantic_label) = if failed_count > 0 {
        ("failed", "uncalibrated_support")
    } else if passed_count == fields.len() {
        ("passed", "calibrated_reliability")
    } else {
        ("untestable", "support_index")
    };
    json!({
        "schema": "rustwx.surface_mesoanalysis.confidence_reliability_packet.v1",
        "validation_mode": validation_mode,
        "status": status,
        "semantic_label": semantic_label,
        "field_count": fields.len(),
        "passed_field_count": passed_count,
        "failed_field_count": failed_count,
        "untestable_field_count": fields.len().saturating_sub(passed_count + failed_count),
        "fields": fields
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<serde_json::Map<_, _>>()
    })
}

fn compact_variable_confidence_reliability(variable: &VariableValidationSummary) -> Value {
    if let Some(confidence) = variable.confidence.as_ref() {
        json!(confidence.reliability)
    } else {
        json!({
            "schema": "rustwx.surface_mesoanalysis.confidence_reliability.v1",
            "semantic_label": "support_index",
            "status": "untestable",
            "bucket_coverage_sufficient": false,
            "ranked_low_confidence_observation_count": 0,
            "ranked_high_confidence_observation_count": 0,
            "min_ranked_bucket_observation_count": CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS,
            "ranked_high_minus_low_mean_abs_analysis_error": null,
            "max_ranked_high_minus_low_mean_abs_analysis_error": 0.0,
            "message": "confidence summary was not available for this field"
        })
    }
}

fn model_valid_time_utc(
    date_yyyymmdd: &str,
    cycle_hour_utc: u8,
    forecast_hour: u16,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    let date = NaiveDate::parse_from_str(date_yyyymmdd, "%Y%m%d")?;
    let cycle_time = date
        .and_hms_opt(cycle_hour_utc as u32, 0, 0)
        .ok_or("invalid model cycle time")?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(cycle_time, Utc)
        + Duration::hours(forecast_hour as i64))
}

fn run_id_for_latest(model: &str, date_yyyymmdd: &str, cycle_hour_utc: u8) -> String {
    format!(
        "{}_{}_{:02}z",
        date_yyyymmdd,
        model.replace('-', "_"),
        cycle_hour_utc
    )
}

fn resolve_observation_paths(args: &Cli) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if !args.observations_json.is_empty() {
        return Ok(args.observations_json.clone());
    }
    let root = args
        .observations_root
        .clone()
        .unwrap_or_else(default_runner_observations_root);
    if !args.obs_sources.is_empty() {
        return Ok(args
            .obs_sources
            .iter()
            .map(|source| {
                root.join("sources")
                    .join(source)
                    .join("latest_observations.json")
            })
            .filter(|path| path.exists())
            .collect());
    }
    discover_latest_observation_paths(&root)
}

fn discover_latest_observation_paths(
    root: &Path,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let sources_dir = root.join("sources");
    let mut paths = Vec::new();
    if !sources_dir.exists() {
        return Ok(paths);
    }
    for entry in fs::read_dir(sources_dir)? {
        let entry = entry?;
        let path = entry.path().join("latest_observations.json");
        if path.exists() {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn default_runner_observations_root() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rustwx-runner")
        .join("data")
        .join("observations")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_and_normalizes_case_tags() {
        let args = Cli::try_parse_from([
            "surface-mesoanalysis",
            "--date",
            "20260513",
            "--case-tag",
            "regime=dryline,hazard=severe",
            "--case-tag",
            "regime=dryline",
        ])
        .expect("case tags should parse");

        assert_eq!(
            normalized_case_tags(&args.case_tags),
            vec!["hazard=severe".to_string(), "regime=dryline".to_string()]
        );
    }
}
