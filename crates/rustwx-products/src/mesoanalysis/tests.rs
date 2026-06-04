use super::*;

fn sample_surface() -> SurfaceFields {
    SurfaceFields {
        lat: vec![35.0, 35.0, 35.0],
        lon: vec![-98.2, -98.0, -97.8],
        nx: 3,
        ny: 1,
        projection: None,
        psfc_pa: vec![100000.0; 3],
        orog_m: vec![300.0; 3],
        orog_is_proxy: false,
        t2_k: vec![293.15; 3],
        q2_kgkg: vec![0.010; 3],
        u10_ms: vec![0.0; 3],
        v10_ms: vec![0.0; 3],
        native_sbcape_jkg: None,
        native_mlcape_jkg: None,
        native_mucape_jkg: None,
        native_pblh_m: None,
    }
}

#[test]
fn runner_observation_loader_maps_current_weather_feed() {
    let path = std::env::temp_dir().join(format!(
        "rustwx_runner_obs_{}_{}.json",
        std::process::id(),
        "current"
    ));
    fs::write(
        &path,
        r#"{
            "source": "calfire_raws_current",
            "source_name": "CAL FIRE RAWS 10-Minute Weather",
            "kind": "raws_current_weather",
            "observation_count": 1,
            "observations": [{
                "station_id": "CF019",
                "source": "calfire_raws_current",
                "timestamp": "2026-05-12T08:20:00Z",
                "latitude": 35.0,
                "longitude": -98.0,
                "temperature_f": 68.0,
                "dewpoint_f": 59.0,
                "wind_direction_deg": 270.0,
                "wind_speed_kts": 10.0
            }]
        }"#,
    )
    .unwrap();

    let loaded = load_runner_meso_observations(&[path.clone()]).unwrap();
    let _ = fs::remove_file(path);

    assert_eq!(loaded.sources.len(), 1);
    assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 1);
    assert_eq!(loaded.sources[0].source_quality_class, "fire_weather_raws");
    assert_eq!(
        loaded.sources[0].representativeness_class,
        "fire_weather_exposure"
    );
    assert_eq!(
        loaded.sources[0].correction_role,
        "supplemental_correction_with_representativeness_inflation"
    );
    assert_eq!(loaded.sources[0].quality_weight, 0.65);
    assert_eq!(loaded.sources[0].default_temperature_error_c, 1.5);
    assert_eq!(loaded.sources[0].default_wind_error_ms, 2.5);
    assert_eq!(loaded.sources[0].time_filtered_count, 0);
    assert_eq!(loaded.sources[0].missing_or_invalid_time_count, 0);
    assert_eq!(loaded.observations.len(), 1);
    assert!((loaded.observations[0].temperature_c.unwrap() - 20.0).abs() < 1.0e-6);
    assert!((loaded.observations[0].wind_speed_ms.unwrap() - 5.14444).abs() < 1.0e-5);
    assert_eq!(loaded.observations[0].temperature_error_c, Some(1.5));
    assert_eq!(loaded.observations[0].dewpoint_error_c, Some(2.0));
    assert_eq!(loaded.observations[0].wind_error_ms, Some(2.5));
}

#[test]
fn runner_observation_loader_skips_daily_fire_danger_feed() {
    let path = std::env::temp_dir().join(format!(
        "rustwx_runner_obs_{}_{}.json",
        std::process::id(),
        "daily"
    ));
    fs::write(
        &path,
        r#"{
            "source": "nifc_raws_fire_danger",
            "kind": "raws_fire_danger_daily",
            "observation_count": 1,
            "observations": [{
                "station_id": "102004",
                "latitude": 44.0,
                "longitude": -116.0,
                "temperature_f": 68.0
            }]
        }"#,
    )
    .unwrap();

    let loaded = load_runner_meso_observations(&[path.clone()]).unwrap();
    let _ = fs::remove_file(path);

    assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 0);
    assert!(loaded.sources[0].skipped_for_kind);
    assert!(loaded.observations.is_empty());
}

#[test]
fn runner_observation_loader_filters_by_reference_time() {
    let path = std::env::temp_dir().join(format!(
        "rustwx_runner_obs_{}_{}.json",
        std::process::id(),
        "time_filter"
    ));
    fs::write(
        &path,
        r#"{
            "source": "oklahoma_mesonet",
            "kind": "mesonet_current_5min",
            "observation_count": 3,
            "observations": [
                {
                    "station_id": "GOOD",
                    "timestamp": "2026-05-12T08:20:00Z",
                    "latitude": 35.0,
                    "longitude": -98.0,
                    "temperature_f": 68.0
                },
                {
                    "station_id": "OLD",
                    "timestamp": "2026-05-12T06:00:00Z",
                    "latitude": 35.1,
                    "longitude": -98.1,
                    "temperature_f": 69.0
                },
                {
                    "station_id": "MISSING_TIME",
                    "latitude": 35.2,
                    "longitude": -98.2,
                    "temperature_f": 70.0
                }
            ]
        }"#,
    )
    .unwrap();

    let loaded = load_runner_meso_observations_with_options(
        &[path.clone()],
        &RunnerMesoObservationLoadOptions {
            reference_time_utc: Some(
                DateTime::parse_from_rfc3339("2026-05-12T08:30:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            max_age_minutes: Some(30),
            allow_future_minutes: 5,
            time_weight_half_life_minutes: Some(60.0),
            max_time_error_inflation_factor: 2.0,
            profile: RunnerMesoObservationProfile::AllCurrentSurface,
        },
    )
    .unwrap();
    let _ = fs::remove_file(path);

    assert_eq!(loaded.observations.len(), 1);
    assert_eq!(loaded.observations[0].station_id, "GOOD");
    assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 1);
    assert_eq!(
        loaded.sources[0].accepted_min_observation_age_minutes,
        Some(10.0)
    );
    assert_eq!(
        loaded.sources[0].accepted_mean_observation_age_minutes,
        Some(10.0)
    );
    assert_eq!(
        loaded.sources[0].accepted_max_observation_age_minutes,
        Some(10.0)
    );
    let expected_time_weight = (-(std::f64::consts::LN_2 * 10.0) / 60.0).exp();
    assert!((loaded.sources[0].mean_time_weight.unwrap() - expected_time_weight).abs() < 1.0e-12);
    assert!(loaded.observations[0].quality_weight < 1.15);
    assert!(loaded.observations[0].temperature_error_c.unwrap() > 1.0);
    assert_eq!(loaded.sources[0].time_filtered_count, 1);
    assert_eq!(loaded.sources[0].missing_or_invalid_time_count, 1);
}

#[test]
fn runner_observation_loader_deduplicates_station_across_sources() {
    let low_path = std::env::temp_dir().join(format!(
        "rustwx_runner_obs_{}_{}.json",
        std::process::id(),
        "duplicate_low"
    ));
    let high_path = std::env::temp_dir().join(format!(
        "rustwx_runner_obs_{}_{}.json",
        std::process::id(),
        "duplicate_high"
    ));
    fs::write(
        &low_path,
        r#"{
            "source": "rwis_duplicate",
            "kind": "rwis_current",
            "observation_count": 1,
            "observations": [{
                "station_id": "DUP",
                "source": "rwis_duplicate",
                "timestamp": "2026-05-12T08:10:00Z",
                "latitude": 35.0,
                "longitude": -98.0,
                "temperature_f": 70.0
            }]
        }"#,
    )
    .unwrap();
    fs::write(
        &high_path,
        r#"{
            "source": "metar_duplicate",
            "kind": "asos_awos_metar",
            "observation_count": 1,
            "observations": [{
                "station_id": "DUP",
                "source": "metar_duplicate",
                "timestamp": "2026-05-12T08:20:00Z",
                "latitude": 35.0,
                "longitude": -98.0,
                "temperature_f": 68.0
            }]
        }"#,
    )
    .unwrap();

    let loaded = load_runner_meso_observations(&[low_path.clone(), high_path.clone()]).unwrap();
    let _ = fs::remove_file(low_path);
    let _ = fs::remove_file(high_path);

    assert_eq!(loaded.observations.len(), 1);
    assert_eq!(loaded.observations[0].station_id, "DUP");
    assert_eq!(loaded.observations[0].source, "metar_duplicate");
    assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 0);
    assert_eq!(loaded.sources[0].duplicate_filtered_count, 1);
    assert_eq!(loaded.sources[1].accepted_for_mesoanalysis, 1);
    assert_eq!(loaded.sources[1].duplicate_filtered_count, 0);
}

#[test]
fn surface_meso_conus_profile_filters_global_and_representativeness_noise() {
    let path = std::env::temp_dir().join(format!(
        "rustwx_runner_obs_{}_{}.json",
        std::process::id(),
        "surface_profile"
    ));
    fs::write(
        &path,
        r#"{
            "source": "aviation_weather_metar_conus",
            "kind": "asos_awos_metar",
            "observation_count": 3,
            "observations": [
                {
                    "station_id": "KOUN",
                    "timestamp": "2026-05-12T08:20:00Z",
                    "latitude": 35.24,
                    "longitude": -97.47,
                    "temperature_f": 68.0,
                    "sea_level_pressure_mb": 1014.2
                },
                {
                    "station_id": "EEPU",
                    "timestamp": "2026-05-12T08:20:00Z",
                    "latitude": 58.42,
                    "longitude": 24.47,
                    "temperature_f": 57.0
                },
                {
                    "station_id": "PHNL",
                    "timestamp": "2026-05-12T08:20:00Z",
                    "latitude": 21.32,
                    "longitude": -157.93,
                    "temperature_f": 79.0
                }
            ]
        }"#,
    )
    .unwrap();

    let loaded = load_runner_meso_observations_with_options(
        &[path.clone()],
        &RunnerMesoObservationLoadOptions {
            profile: RunnerMesoObservationProfile::SurfaceMesoConus,
            ..RunnerMesoObservationLoadOptions::default()
        },
    )
    .unwrap();
    let _ = fs::remove_file(path);

    assert_eq!(loaded.observations.len(), 1);
    assert_eq!(loaded.observations[0].station_id, "KOUN");
    assert_eq!(
        loaded.observations[0].mean_sea_level_pressure_hpa,
        Some(1014.2)
    );
    assert_eq!(loaded.observations[0].temperature_error_c, Some(0.8));
    assert_eq!(
        loaded.observations[0].mean_sea_level_pressure_error_hpa,
        Some(0.7)
    );
    assert_eq!(
        loaded.observations[0].source_quality_class.as_deref(),
        Some("aviation_reference")
    );
    assert_eq!(
        loaded.observations[0].representativeness_class.as_deref(),
        Some("synoptic_airport_surface")
    );
    assert_eq!(loaded.sources[0].accepted_for_mesoanalysis, 1);
    assert_eq!(loaded.sources[0].profile_filtered_count, 2);
    assert_eq!(loaded.sources[0].accepted_mean_sea_level_pressure_count, 1);
    assert_eq!(loaded.sources[0].source_quality_class, "aviation_reference");
    assert_eq!(
        loaded.sources[0].representativeness_class,
        "synoptic_airport_surface"
    );
    assert_eq!(
        loaded.sources[0].correction_role,
        "primary_correction_and_verification"
    );
    assert_eq!(loaded.sources[0].quality_weight, 1.0);
    assert_eq!(loaded.sources[0].default_temperature_error_c, 0.8);
    assert_eq!(
        loaded.sources[0].default_mean_sea_level_pressure_error_hpa,
        0.7
    );
}

#[test]
fn runner_kind_allowlist_covers_current_runner_sources() {
    for kind in [
        "asos_awos_metar",
        "mesonet_5min",
        "mesonet_current_5min",
        "mesonet_current_15min",
        "mesonet_hourly",
        "mesonet_hourly_ag_weather",
        "marine_current_observation",
        "rwis_current",
        "raws_current_weather",
        "coastal_meteorology_current",
        "snotel_hourly",
        "scan_hourly",
    ] {
        assert!(
            runner_kind_is_current_surface_candidate(kind),
            "{kind} should be usable for surface mesoanalysis"
        );
    }
    assert!(!runner_kind_is_current_surface_candidate(
        "raws_fire_danger_daily"
    ));
    assert!(!runner_kind_is_current_surface_candidate(
        "hydro_current_observation"
    ));
    assert!(!runner_kind_is_current_surface_candidate(
        "hydro_forecast_status"
    ));
    assert!(!runner_kind_is_current_surface_candidate(
        "flash_flood_current_observation"
    ));
    assert!(!runner_kind_is_current_surface_candidate(
        "coastal_water_current"
    ));
    assert!(!runner_kind_is_current_surface_candidate(
        "air_quality_current_observation"
    ));
    assert!(!runner_kind_is_current_surface_candidate(
        "coop_daily_climate"
    ));
}

#[test]
fn surface_fields_mesoanalysis_consumes_runner_observations() {
    let surface = sample_surface();
    let mut observation = MesoObservation::new("OKC", 35.0, -98.0)
        .with_source("unit")
        .with_temperature_c(25.0)
        .with_dewpoint_c(18.0)
        .with_wind(270.0, 10.0);
    observation.source_quality_class = Some("aviation_reference".to_string());
    observation.representativeness_class = Some("synoptic_airport_surface".to_string());
    observation.correction_role = Some("primary_correction_and_verification".to_string());
    observation.observation_age_minutes = Some(12.0);
    observation.time_weight = Some(0.87);
    let observations = vec![observation];

    let fields = compute_surface_mesoanalysis_from_fields(
        &surface,
        &observations,
        MesoanalysisConfig {
            barnes_radius_km: 30.0,
            barnes_kappa_km2: 100.0,
            ..MesoanalysisConfig::default()
        },
    )
    .unwrap();
    let report = summarize_surface_mesoanalysis(&fields, &observations);

    assert_eq!(report.schema, "rustwx.surface_mesoanalysis.report.v1");
    assert_eq!(report.observation_count, 1);
    assert_eq!(report.fields.temperature_2m_c.finite_count, 3);
    assert!(report.fields.temperature_increment_c.finite_count > 0);
    assert!(report.fields.temperature_confidence.finite_count > 0);
    assert!(fields.u10_ms[1] > 9.9);
}

#[test]
fn station_validation_reports_background_to_analysis_improvement() {
    let surface = sample_surface();
    let mut observation = MesoObservation::new("OKC", 35.0, -98.0)
        .with_source("unit")
        .with_temperature_c(25.0)
        .with_dewpoint_c(18.0)
        .with_wind(270.0, 10.0);
    observation.source_quality_class = Some("aviation_reference".to_string());
    observation.representativeness_class = Some("synoptic_airport_surface".to_string());
    observation.correction_role = Some("primary_correction_and_verification".to_string());
    observation.observation_age_minutes = Some(12.0);
    observation.time_weight = Some(0.87);
    let observations = vec![observation];

    let fields = compute_surface_mesoanalysis_from_fields(
        &surface,
        &observations,
        MesoanalysisConfig {
            barnes_radius_km: 30.0,
            barnes_kappa_km2: 100.0,
            ..MesoanalysisConfig::default()
        },
    )
    .unwrap();
    let report =
        summarize_surface_mesoanalysis_with_validation(&surface, &fields, &observations).unwrap();
    let validation = report.validation.unwrap();
    let sample = validation.samples.first().unwrap();
    let temperature = sample.temperature_c.as_ref().unwrap();

    assert_eq!(validation.sampled_observation_count, 1);
    assert!(sample.nearest_grid_distance_km < 1.0e-6);
    assert_eq!(
        sample.source_quality_class.as_deref(),
        Some("aviation_reference")
    );
    assert_eq!(
        sample.observation_age_bucket.as_deref(),
        Some("age_000_015_min")
    );
    assert_eq!(sample.terrain_pressure_class, "lowland_high_pressure");
    assert!(temperature.abs_error_improvement > 0.0);
    assert!(temperature.confidence.is_some());
    assert!(validation.temperature_c.mean_abs_error_improvement.unwrap() > 0.0);
    assert!(validation.temperature_c.analysis_rmse.unwrap() < 0.1);
    assert_eq!(
        validation
            .temperature_c
            .confidence
            .as_ref()
            .unwrap()
            .observation_count,
        1
    );
    assert_eq!(
        validation.temperature_c.max_abs_background_error,
        validation.temperature_c.mean_abs_background_error
    );
    assert_eq!(validation.source_summaries.len(), 1);
    assert_eq!(validation.source_summaries[0].source, "unit");
    assert_eq!(
        validation.source_summaries[0]
            .temperature_c
            .observation_count,
        1
    );
    assert_eq!(
        validation.source_summaries[0]
            .temperature_c
            .confidence
            .as_ref()
            .unwrap()
            .observation_count,
        1
    );
    assert!(validation.strata_summaries.iter().any(|summary| {
        summary.stratum_type == "source_quality_class"
            && summary.stratum_value == "aviation_reference"
            && summary.temperature_c.observation_count == 1
    }));
    assert!(validation.strata_summaries.iter().any(|summary| {
        summary.stratum_type == "observation_age_bucket"
            && summary.stratum_value == "age_000_015_min"
            && summary.temperature_c.observation_count == 1
    }));
}

#[test]
fn validation_confidence_summary_bins_station_errors() {
    let surface = sample_surface();
    let observations = vec![
        MesoObservation::new("LOW", 35.0, -98.2)
            .with_source("unit")
            .with_temperature_c(20.0),
        MesoObservation::new("MID", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(20.0),
        MesoObservation::new("HIGH", 35.0, -97.8)
            .with_source("unit")
            .with_temperature_c(20.0),
    ];
    let fields = MesoanalysisFields {
        temperature_2m_c: vec![15.0, 18.0, 20.0],
        dewpoint_2m_c: vec![10.0; 3],
        q2_kgkg: surface.q2_kgkg.clone(),
        u10_ms: surface.u10_ms.clone(),
        v10_ms: surface.v10_ms.clone(),
        mean_sea_level_pressure_hpa: None,
        temperature_increment_c: vec![-5.0, -2.0, 0.0],
        dewpoint_increment_c: vec![0.0; 3],
        u10_increment_ms: vec![0.0; 3],
        v10_increment_ms: vec![0.0; 3],
        mean_sea_level_pressure_increment_hpa: None,
        neighbor_count: vec![1; 3],
        temperature_confidence: vec![0.1, 0.5, 0.9],
        dewpoint_confidence: vec![1.0; 3],
        u10_confidence: vec![1.0; 3],
        v10_confidence: vec![1.0; 3],
        mean_sea_level_pressure_confidence: None,
        diagnostics: Vec::new(),
    };

    let validation =
        validate_surface_mesoanalysis_at_observations(&surface, &fields, &observations).unwrap();
    let confidence = validation.temperature_c.confidence.as_ref().unwrap();

    assert_eq!(confidence.observation_count, 3);
    assert_eq!(confidence.low_confidence_observation_count, 1);
    assert_eq!(confidence.medium_confidence_observation_count, 1);
    assert_eq!(confidence.high_confidence_observation_count, 1);
    assert_eq!(confidence.low_confidence_mean_abs_analysis_error, Some(5.0));
    assert_eq!(
        confidence.medium_confidence_mean_abs_analysis_error,
        Some(2.0)
    );
    assert_eq!(
        confidence.high_confidence_mean_abs_analysis_error,
        Some(0.0)
    );
    assert_eq!(
        confidence.high_minus_low_mean_abs_analysis_error,
        Some(-5.0)
    );
    assert!(confidence.confidence_abs_error_correlation.unwrap() < -0.9);
    assert_eq!(confidence.ranked_low_confidence_observation_count, 1);
    assert_eq!(confidence.ranked_high_confidence_observation_count, 1);
    assert_eq!(
        confidence.ranked_high_minus_low_mean_abs_analysis_error,
        Some(-5.0)
    );
    assert_eq!(confidence.reliability.status, "untestable");
    assert_eq!(confidence.reliability.semantic_label, "support_index");
    assert!(!confidence.reliability.bucket_coverage_sufficient);
    assert_eq!(
        confidence.reliability.min_ranked_bucket_observation_count,
        CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS
    );
    assert_eq!(
        validation.samples[0]
            .temperature_c
            .as_ref()
            .unwrap()
            .confidence,
        Some(0.1)
    );
}

#[test]
fn confidence_reliability_requires_professional_ranked_bucket_coverage() {
    let undercovered = confidence_reliability_contract_from_ranked_buckets(9, 9, Some(-1.0));
    assert_eq!(undercovered.status, "untestable");
    assert_eq!(undercovered.semantic_label, "support_index");
    assert!(!undercovered.bucket_coverage_sufficient);
    assert_eq!(
        undercovered.min_ranked_bucket_observation_count,
        CONFIDENCE_RELIABILITY_MIN_RANKED_BUCKET_OBSERVATIONS
    );

    let passing = confidence_reliability_contract_from_ranked_buckets(10, 10, Some(-0.1));
    assert_eq!(passing.status, "passed");
    assert_eq!(passing.semantic_label, "calibrated_reliability");
    assert!(passing.bucket_coverage_sufficient);

    let failing = confidence_reliability_contract_from_ranked_buckets(10, 10, Some(0.1));
    assert_eq!(failing.status, "failed");
    assert_eq!(failing.semantic_label, "uncalibrated_support");
    assert!(failing.bucket_coverage_sufficient);
}

#[test]
fn external_reference_comparison_uses_same_validation_samples() {
    let background_surface = sample_surface();
    let mut reference_surface = sample_surface();
    reference_surface.t2_k = vec![294.65; 3];
    let observations = vec![
        MesoObservation::new("OKC", 35.0, -98.0)
            .with_source("unit")
            .with_timestamp("2026-05-13T01:00:00Z")
            .with_temperature_c(22.0),
    ];
    let candidate_fields = MesoanalysisFields {
        temperature_2m_c: vec![21.0; 3],
        dewpoint_2m_c: vec![15.0; 3],
        q2_kgkg: background_surface.q2_kgkg.clone(),
        u10_ms: background_surface.u10_ms.clone(),
        v10_ms: background_surface.v10_ms.clone(),
        mean_sea_level_pressure_hpa: None,
        temperature_increment_c: vec![1.0; 3],
        dewpoint_increment_c: vec![0.0; 3],
        u10_increment_ms: vec![0.0; 3],
        v10_increment_ms: vec![0.0; 3],
        mean_sea_level_pressure_increment_hpa: None,
        neighbor_count: vec![1; 3],
        temperature_confidence: vec![1.0; 3],
        dewpoint_confidence: vec![1.0; 3],
        u10_confidence: vec![1.0; 3],
        v10_confidence: vec![1.0; 3],
        mean_sea_level_pressure_confidence: None,
        diagnostics: Vec::new(),
    };

    let candidate_validation = validate_surface_mesoanalysis_at_observations(
        &background_surface,
        &candidate_fields,
        &observations,
    )
    .unwrap();
    let reference_validation =
        validate_surface_reference_at_observations(&reference_surface, &observations).unwrap();
    let comparison = compare_surface_mesoanalysis_to_external_reference(
        SurfaceMesoanalysisExternalReferenceDescriptor {
            reference_label: "rtma".to_string(),
            reference_model: "rtma".to_string(),
            reference_source: "nomads".to_string(),
            reference_cycle: "2026051301z".to_string(),
            reference_forecast_hour: 0,
            reference_product: "2dvaranl_ndfd".to_string(),
            candidate_label: "OptimalInterpolation".to_string(),
            background_label: "hrrr".to_string(),
            validation_mode: "holdout_validation".to_string(),
        },
        &candidate_validation,
        &reference_validation,
    );

    assert_eq!(comparison.reference_label, "rtma");
    assert_eq!(comparison.sampled_observation_count, 1);
    assert_eq!(
        comparison.temperature_c.background_mean_abs_error,
        Some(2.0)
    );
    assert_eq!(comparison.temperature_c.candidate_mean_abs_error, Some(1.0));
    assert_eq!(comparison.temperature_c.reference_mean_abs_error, Some(0.5));
    assert_eq!(
        comparison.temperature_c.candidate_minus_background_mae,
        Some(-1.0)
    );
    assert_eq!(
        comparison.temperature_c.candidate_minus_reference_mae,
        Some(0.5)
    );
    assert_eq!(comparison.temperature_c.mae_winner, "reference");
}

#[test]
fn holdout_validation_recomputes_analysis_without_withheld_observations() {
    let surface = sample_surface();
    let observations = vec![
        MesoObservation::new("WEST", 35.0, -98.2)
            .with_source("unit")
            .with_temperature_c(24.0),
        MesoObservation::new("CENTER", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(25.0),
        MesoObservation::new("EAST", 35.0, -97.8)
            .with_source("unit")
            .with_temperature_c(16.0),
    ];
    let config = MesoanalysisConfig {
        barnes_radius_km: 35.0,
        barnes_kappa_km2: 100.0,
        ..MesoanalysisConfig::default()
    };
    let fields = compute_surface_mesoanalysis_from_fields(&surface, &observations, config).unwrap();
    let report = summarize_surface_mesoanalysis_with_validation_and_holdout(
        &surface,
        &fields,
        &observations,
        config,
        0.34,
        7,
        1,
    )
    .unwrap();
    let holdout = report.holdout_validation.unwrap();

    assert_eq!(holdout.holdout_observation_count, 1);
    assert_eq!(holdout.training_observation_count, 2);
    assert_eq!(holdout.validation.observation_count, 1);
    assert_eq!(holdout.validation.sampled_observation_count, 1);
}

#[test]
fn repeated_holdout_validation_aggregates_multiple_splits() {
    let surface = sample_surface();
    let observations = vec![
        MesoObservation::new("WEST", 35.0, -98.2)
            .with_source("unit")
            .with_temperature_c(24.0),
        MesoObservation::new("CENTER", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(25.0),
        MesoObservation::new("EAST", 35.0, -97.8)
            .with_source("unit")
            .with_temperature_c(16.0),
    ];
    let config = MesoanalysisConfig {
        barnes_radius_km: 35.0,
        barnes_kappa_km2: 100.0,
        ..MesoanalysisConfig::default()
    };

    let repeated = compute_surface_mesoanalysis_repeated_holdout_validation(
        &surface,
        &observations,
        config,
        0.34,
        7,
        3,
        1,
    )
    .unwrap()
    .unwrap();

    assert_eq!(repeated.repeat_count, 3);
    assert_eq!(repeated.completed_fold_count, 3);
    assert_eq!(repeated.folds.len(), 3);
    assert_eq!(repeated.temperature_c.fold_count, 3);
    assert_eq!(repeated.temperature_c.total_observation_count, 3);
    assert_eq!(repeated.temperature_c.mean_observation_count, 1.0);
    assert!(repeated.temperature_c.mean_abs_analysis_error.is_some());
}

#[test]
fn spatial_block_holdout_withholds_whole_spatial_groups() {
    let observations = vec![
        MesoObservation::new("A1", 34.2, -99.2).with_source("unit"),
        MesoObservation::new("A2", 34.4, -99.4).with_source("unit"),
        MesoObservation::new("B1", 38.2, -95.2).with_source("unit"),
        MesoObservation::new("B2", 38.4, -95.4).with_source("unit"),
    ];

    let (training, holdout) = deterministic_holdout_split(
        &observations,
        0.50,
        11,
        1,
        SurfaceMesoanalysisHoldoutStrategy::SpatialBlock,
    );

    let held_blocks = holdout
        .iter()
        .map(spatial_holdout_key)
        .collect::<BTreeSet<_>>();
    assert_eq!(training.len(), 2);
    assert_eq!(holdout.len(), 2);
    assert_eq!(held_blocks.len(), 1);
}

#[test]
fn source_hash_holdout_withholds_whole_provider_groups() {
    let observations = vec![
        MesoObservation::new("A1", 35.0, -98.2).with_source("provider_a"),
        MesoObservation::new("A2", 35.0, -98.0).with_source("provider_a"),
        MesoObservation::new("B1", 35.0, -97.8).with_source("provider_b"),
        MesoObservation::new("B2", 35.0, -97.6).with_source("provider_b"),
    ];

    let (training, holdout) = deterministic_holdout_split(
        &observations,
        0.50,
        13,
        1,
        SurfaceMesoanalysisHoldoutStrategy::SourceHash,
    );

    let held_sources = holdout
        .iter()
        .map(|observation| observation.source.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(training.len(), 2);
    assert_eq!(holdout.len(), 2);
    assert_eq!(held_sources.len(), 1);
}

#[test]
fn validation_gate_flags_pass_and_fail_thresholds() {
    let validation = SurfaceMesoanalysisValidationSummary {
        observation_count: 2,
        sampled_observation_count: 2,
        skipped_observation_count: 0,
        max_nearest_grid_distance_km: Some(1.0),
        temperature_c: VariableValidationSummary {
            observation_count: 2,
            improved_count: 2,
            worsened_count: 0,
            unchanged_count: 0,
            mean_abs_background_error: Some(5.0),
            mean_abs_analysis_error: Some(0.5),
            mean_abs_error_improvement: Some(4.5),
            ..VariableValidationSummary::default()
        },
        dewpoint_c: VariableValidationSummary {
            observation_count: 2,
            improved_count: 2,
            worsened_count: 0,
            unchanged_count: 0,
            mean_abs_background_error: Some(4.0),
            mean_abs_analysis_error: Some(0.4),
            mean_abs_error_improvement: Some(3.6),
            ..VariableValidationSummary::default()
        },
        wind_speed_ms: VariableValidationSummary {
            observation_count: 2,
            improved_count: 2,
            worsened_count: 0,
            unchanged_count: 0,
            mean_abs_background_error: Some(3.0),
            mean_abs_analysis_error: Some(0.3),
            mean_abs_error_improvement: Some(2.7),
            ..VariableValidationSummary::default()
        },
        mean_sea_level_pressure_hpa: None,
        source_summaries: Vec::new(),
        strata_summaries: Vec::new(),
        samples: Vec::new(),
    };

    let pass = evaluate_surface_mesoanalysis_validation_gate(
        &validation,
        SurfaceMesoanalysisValidationGateThresholds {
            min_sampled_observations: 2,
            max_skipped_observations: 0,
            max_nearest_grid_distance_km: 2.0,
            max_temperature_mean_abs_error_c: 1.0,
            max_dewpoint_mean_abs_error_c: 1.0,
            max_wind_speed_mean_abs_error_ms: 1.0,
        },
    );
    assert!(pass.passed);

    let fail = evaluate_surface_mesoanalysis_validation_gate(
        &validation,
        SurfaceMesoanalysisValidationGateThresholds {
            max_temperature_mean_abs_error_c: 0.1,
            ..pass.thresholds
        },
    );
    assert!(!fail.passed);
    assert!(
        fail.checks
            .iter()
            .any(|check| check.name == "temperature_c_mean_abs_analysis_error" && !check.passed)
    );
}

#[test]
fn validation_comparison_reports_candidate_minus_baseline_error_deltas() {
    let candidate = SurfaceMesoanalysisValidationSummary {
        observation_count: 2,
        sampled_observation_count: 2,
        skipped_observation_count: 0,
        max_nearest_grid_distance_km: Some(1.0),
        temperature_c: VariableValidationSummary {
            observation_count: 2,
            mean_analysis_error: Some(-0.25),
            mean_abs_analysis_error: Some(0.8),
            analysis_rmse: Some(1.0),
            ..VariableValidationSummary::default()
        },
        dewpoint_c: VariableValidationSummary::default(),
        wind_speed_ms: VariableValidationSummary::default(),
        mean_sea_level_pressure_hpa: None,
        source_summaries: Vec::new(),
        strata_summaries: Vec::new(),
        samples: Vec::new(),
    };
    let baseline = SurfaceMesoanalysisValidationSummary {
        observation_count: 2,
        sampled_observation_count: 2,
        skipped_observation_count: 0,
        max_nearest_grid_distance_km: Some(1.0),
        temperature_c: VariableValidationSummary {
            observation_count: 2,
            mean_analysis_error: Some(0.50),
            mean_abs_analysis_error: Some(1.2),
            analysis_rmse: Some(1.5),
            ..VariableValidationSummary::default()
        },
        dewpoint_c: VariableValidationSummary::default(),
        wind_speed_ms: VariableValidationSummary::default(),
        mean_sea_level_pressure_hpa: None,
        source_summaries: Vec::new(),
        strata_summaries: Vec::new(),
        samples: Vec::new(),
    };

    let comparison =
        compare_surface_mesoanalysis_validations("oi", &candidate, "barnes", &baseline);

    assert_eq!(comparison.candidate_label, "oi");
    assert_eq!(comparison.baseline_label, "barnes");
    assert!(
        (comparison
            .temperature_c
            .mean_abs_analysis_error_delta
            .unwrap()
            + 0.4)
            .abs()
            < 1.0e-12
    );
    assert_eq!(comparison.temperature_c.analysis_rmse_delta, Some(-0.5));
    assert_eq!(
        comparison.temperature_c.mean_analysis_error_delta,
        Some(-0.75)
    );
}

#[test]
fn benchmark_summary_distills_raw_candidate_and_baseline_skill() {
    let candidate = SurfaceMesoanalysisValidationSummary {
        observation_count: 3,
        sampled_observation_count: 3,
        skipped_observation_count: 0,
        max_nearest_grid_distance_km: Some(1.0),
        temperature_c: VariableValidationSummary {
            observation_count: 3,
            mean_abs_background_error: Some(2.0),
            mean_abs_analysis_error: Some(0.7),
            background_rmse: Some(2.4),
            analysis_rmse: Some(0.9),
            ..VariableValidationSummary::default()
        },
        dewpoint_c: VariableValidationSummary::default(),
        wind_speed_ms: VariableValidationSummary::default(),
        mean_sea_level_pressure_hpa: None,
        source_summaries: Vec::new(),
        strata_summaries: Vec::new(),
        samples: Vec::new(),
    };
    let baseline = SurfaceMesoanalysisValidationSummary {
        observation_count: 3,
        sampled_observation_count: 3,
        skipped_observation_count: 0,
        max_nearest_grid_distance_km: Some(1.0),
        temperature_c: VariableValidationSummary {
            observation_count: 3,
            mean_abs_background_error: Some(2.0),
            mean_abs_analysis_error: Some(1.1),
            background_rmse: Some(2.4),
            analysis_rmse: Some(1.3),
            ..VariableValidationSummary::default()
        },
        dewpoint_c: VariableValidationSummary::default(),
        wind_speed_ms: VariableValidationSummary::default(),
        mean_sea_level_pressure_hpa: None,
        source_summaries: Vec::new(),
        strata_summaries: Vec::new(),
        samples: Vec::new(),
    };

    let benchmark = benchmark_surface_mesoanalysis_validations(
        "oi",
        &candidate,
        "barnes",
        &baseline,
        "same_observation_validation",
    );

    assert_eq!(benchmark.candidate_label, "oi");
    assert_eq!(benchmark.baseline_label, "barnes");
    assert!(
        (benchmark
            .temperature_c
            .candidate_minus_background_mae
            .unwrap()
            + 1.3)
            .abs()
            < 1.0e-12
    );
    assert!(
        (benchmark
            .temperature_c
            .candidate_minus_baseline_mae
            .unwrap()
            + 0.4)
            .abs()
            < 1.0e-12
    );
    assert!(
        (benchmark
            .temperature_c
            .baseline_minus_background_rmse
            .unwrap()
            + 1.1)
            .abs()
            < 1.0e-12
    );
    assert!(
        (benchmark
            .temperature_c
            .candidate_minus_baseline_rmse
            .unwrap()
            + 0.4)
            .abs()
            < 1.0e-12
    );
    assert_eq!(benchmark.temperature_c.mae_winner, "candidate");
    assert_eq!(benchmark.temperature_c.rmse_winner, "candidate");
}

#[test]
fn repeated_holdout_benchmark_counts_fold_wins() {
    fn validation(raw_mae: f64, analysis_mae: f64) -> SurfaceMesoanalysisValidationSummary {
        SurfaceMesoanalysisValidationSummary {
            observation_count: 2,
            sampled_observation_count: 2,
            skipped_observation_count: 0,
            max_nearest_grid_distance_km: Some(1.0),
            temperature_c: VariableValidationSummary {
                observation_count: 2,
                mean_abs_background_error: Some(raw_mae),
                mean_abs_analysis_error: Some(analysis_mae),
                background_rmse: Some(raw_mae + 0.2),
                analysis_rmse: Some(analysis_mae + 0.2),
                ..VariableValidationSummary::default()
            },
            dewpoint_c: VariableValidationSummary::default(),
            wind_speed_ms: VariableValidationSummary::default(),
            mean_sea_level_pressure_hpa: None,
            source_summaries: Vec::new(),
            strata_summaries: Vec::new(),
            samples: Vec::new(),
        }
    }

    fn repeated(
        validations: Vec<SurfaceMesoanalysisValidationSummary>,
    ) -> SurfaceMesoanalysisRepeatedHoldoutValidationSummary {
        let folds = validations
            .into_iter()
            .enumerate()
            .map(
                |(index, validation)| SurfaceMesoanalysisHoldoutValidationSummary {
                    schema: "rustwx.surface_mesoanalysis.holdout_validation.v1".to_string(),
                    requested_fraction: 0.5,
                    seed: index as u64,
                    strategy: SurfaceMesoanalysisHoldoutStrategy::StationHash,
                    min_holdout_observations: 1,
                    training_observation_count: 2,
                    holdout_observation_count: 2,
                    selection_rule: "unit".to_string(),
                    validation,
                },
            )
            .collect::<Vec<_>>();
        SurfaceMesoanalysisRepeatedHoldoutValidationSummary {
            schema: "rustwx.surface_mesoanalysis.repeated_holdout_validation.v1".to_string(),
            requested_fraction: 0.5,
            seed: 0,
            repeat_count: folds.len(),
            completed_fold_count: folds.len(),
            strategy: SurfaceMesoanalysisHoldoutStrategy::StationHash,
            min_holdout_observations: 1,
            selection_rule: "unit".to_string(),
            temperature_c: summarize_repeated_variable_validation(
                folds.iter().map(|fold| &fold.validation.temperature_c),
            ),
            dewpoint_c: summarize_repeated_variable_validation(
                folds.iter().map(|fold| &fold.validation.dewpoint_c),
            ),
            wind_speed_ms: summarize_repeated_variable_validation(
                folds.iter().map(|fold| &fold.validation.wind_speed_ms),
            ),
            mean_sea_level_pressure_hpa: None,
            folds,
        }
    }

    let candidate = repeated(vec![validation(2.0, 0.8), validation(2.0, 1.4)]);
    let baseline = repeated(vec![validation(2.0, 1.2), validation(2.0, 1.1)]);

    let benchmark = benchmark_surface_mesoanalysis_repeated_holdout_validations(
        "oi",
        &candidate,
        "barnes",
        &baseline,
        "repeated_holdout_validation",
    );

    assert_eq!(benchmark.fold_count, 2);
    assert_eq!(
        benchmark
            .temperature_c
            .candidate_beats_background_mae_fold_count,
        2
    );
    assert_eq!(
        benchmark
            .temperature_c
            .candidate_beats_baseline_mae_fold_count,
        1
    );
    assert!(
        (benchmark
            .temperature_c
            .candidate_minus_baseline_mae
            .unwrap()
            + 0.05)
            .abs()
            < 1.0e-12
    );
}

#[test]
fn grid_export_writes_wxstore_compatible_manifest() {
    let surface = sample_surface();
    let observations = vec![
        MesoObservation::new("OKC", 35.0, -98.0)
            .with_source("unit")
            .with_temperature_c(25.0)
            .with_dewpoint_c(18.0)
            .with_wind(270.0, 10.0),
    ];
    let fields = compute_surface_mesoanalysis_from_fields(
        &surface,
        &observations,
        MesoanalysisConfig {
            barnes_radius_km: 30.0,
            barnes_kappa_km2: 100.0,
            ..MesoanalysisConfig::default()
        },
    )
    .unwrap();
    let out_dir =
        std::env::temp_dir().join(format!("rustwx_meso_grid_export_{}", std::process::id()));
    let _ = fs::remove_dir_all(&out_dir);

    let manifest = write_surface_mesoanalysis_grid_export(
        &SurfaceMesoanalysisGridExportRequest {
            model: "hrrr".to_string(),
            run_id: "unit_run".to_string(),
            member: "control".to_string(),
            date_yyyymmdd: "20260512".to_string(),
            cycle_utc: 0,
            source: "unit".to_string(),
            forecast_hour: 0,
            valid_time: "2026-05-12T00:00:00Z".to_string(),
            out_dir: out_dir.clone(),
        },
        &surface,
        &fields,
    )
    .unwrap();

    assert_eq!(
        manifest.schema,
        "rustwx.surface_mesoanalysis.grid_export.v1"
    );
    assert_eq!(manifest.fields.len(), 15);
    assert!(out_dir.join("manifest.json").is_file());
    assert!(out_dir.join("grid_lat.f32").is_file());
    assert!(out_dir.join("grid_lon.f32").is_file());
    let temperature = manifest
        .fields
        .iter()
        .find(|field| field.product_slug == "meso_temperature_2m_c")
        .unwrap();
    assert_eq!(temperature.nx, 3);
    assert_eq!(temperature.ny, 1);
    assert_eq!(
        fs::metadata(out_dir.join(&temperature.values_path))
            .unwrap()
            .len(),
        12
    );
    let confidence = manifest
        .fields
        .iter()
        .find(|field| field.product_slug == "meso_temperature_confidence")
        .unwrap();
    assert!(out_dir.join(&confidence.values_path).is_file());

    let _ = fs::remove_dir_all(out_dir);
}
