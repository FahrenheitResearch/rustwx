use super::*;
use crate::native_dataset::NativeHourProcessor;
use crate::native_dataset::{
    NativeDatasetBounds, NativeDatasetBuildConfig, NativeDatasetCase, NativeDatasetShardSpec,
    NativeDatasetTile, plan_native_dataset,
};

fn test_plan() -> NativeDatasetPlan {
    let case = NativeDatasetCase::new(
        "case",
        DateTime::parse_from_rfc3339("2024-05-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        4,
    );
    let tile = NativeDatasetTile::new(
        "tile",
        35.0,
        -97.0,
        NativeDatasetBounds::new(-97.1, -96.9, 34.9, 35.1),
    );
    let mut config =
        NativeDatasetBuildConfig::hrrr_multisource_v1("materializer_test", vec![case], vec![tile]);
    config.grid_size = 4;
    plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap()
}

#[test]
fn materializer_manifest_matches_plan_shapes() {
    let plan = test_plan();
    let layout = NativeDatasetMaterializerLayout::from_plan(&plan).unwrap();
    assert_eq!(layout.goes_product_family, "ABI-L2-MCMIPC");
    let manifest = training_shard_manifest_for_plan(&plan, &layout).unwrap();
    assert_eq!(
        manifest.tensor("hrrr_fields").unwrap().shape,
        vec![10, 4, 4]
    );
    assert_eq!(
        manifest.tensor("mrms_hist").unwrap().shape,
        vec![3, 3, 4, 4]
    );
    assert_eq!(
        manifest.tensor("goes_hist").unwrap().shape,
        vec![3, 8, 4, 4]
    );
    assert_eq!(
        manifest.tensor("target_initiation").unwrap().shape,
        vec![1, 4, 4]
    );
    assert_eq!(
        manifest.tensor("hrrr_valid_mask").unwrap().shape,
        vec![1, 4, 4]
    );
    assert_eq!(
        manifest.tensor("mrms_hist_valid_mask").unwrap().shape,
        vec![3, 1, 4, 4]
    );
    assert_eq!(
        manifest.tensor("target_refc_ge35").unwrap().shape,
        vec![1, 4, 4]
    );
}

#[test]
fn materializer_layout_preserves_goes_product_family() {
    let case = NativeDatasetCase::new(
        "case",
        DateTime::parse_from_rfc3339("2024-05-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        4,
    );
    let tile = NativeDatasetTile::new(
        "tile",
        35.0,
        -97.0,
        NativeDatasetBounds::new(-97.1, -96.9, 34.9, 35.1),
    );
    let mut config =
        NativeDatasetBuildConfig::hrrr_multisource_v1("goes_meso", vec![case], vec![tile]);
    for source in &mut config.sources {
        if source.kind == NativeDatasetSourceKind::GoesNetcdf {
            source.product_family = Some("ABI-L2-MCMIPM1".to_string());
        }
    }
    let plan = plan_native_dataset(config, NativeDatasetShardSpec::new(0, 1).unwrap()).unwrap();
    let layout = NativeDatasetMaterializerLayout::from_plan(&plan).unwrap();
    assert_eq!(layout.goes_product_family, "ABI-L2-MCMIPM1");
}

#[test]
fn goes_mesoscale_product_family_uses_shared_s3_prefix() {
    assert_eq!(goes_s3_prefix_product("ABI-L2-MCMIPM1"), "ABI-L2-MCMIPM");
    assert!(goes_filename_product_matches_request(
        "ABI-L2-MCMIPM1",
        "ABI-L2-MCMIPM1"
    ));
    assert!(!goes_filename_product_matches_request(
        "ABI-L2-MCMIPM2",
        "ABI-L2-MCMIPM1"
    ));
    assert!(goes_filename_product_matches_request(
        "ABI-L2-MCMIPM2",
        "ABI-L2-MCMIPM"
    ));
}

#[test]
fn materializer_can_emit_nan_shard_when_sources_are_missing() {
    let root = std::env::temp_dir().join(format!(
        "rustwx_native_materializer_{}_{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let plan = test_plan();
    let config = NativeDatasetMaterializerConfig::new(root.join("cache"), root.join("shard"))
        .with_source_root(root.join("raw"))
        .with_missing_policy(NativeMaterializerMissingPolicy::FillNan);
    let mut materializer = NativeDatasetMaterializer::create(&plan, config).unwrap();
    let hour_jobs = crate::native_dataset::build_native_dataset_hour_jobs(&plan).unwrap();
    let mut emitted = 0usize;
    for job in &hour_jobs {
        emitted += materializer
            .process_hour(&plan, job)
            .unwrap()
            .samples_emitted;
    }
    let manifest = materializer.finish().unwrap();
    assert_eq!(emitted, plan.expected_samples);
    assert_eq!(manifest.sample_count, plan.expected_samples as u64);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn nearest_level2_file_respects_time_window() {
    let root = std::env::temp_dir().join(format!(
        "rustwx_level2_window_{}_{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("KFDR20240506_180007_V06"), b"not-a-real-volume").unwrap();
    let target = DateTime::parse_from_rfc3339("2024-05-06T19:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let stale = find_nearest_level2_file(&root, target, 10 * 60 * 1000).unwrap();
    let loose = find_nearest_level2_file(&root, target, 70 * 60 * 1000).unwrap();

    assert!(stale.is_none());
    assert_eq!(
        loose.unwrap().file_name().unwrap(),
        "KFDR20240506_180007_V06"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn mrms_sentinel_sanitization_preserves_quiet_weather_semantics() {
    let refc = sanitize_mrms_channel("refc", &[-99.0, -20.0, 36.0, f32::NAN]);
    assert_eq!(refc[0], QUIET_REFLECTIVITY_DBZ);
    assert_eq!(refc[1], -20.0);
    assert_eq!(refc[2], 36.0);
    assert!(refc[3].is_nan());

    let low_level_reflectivity = sanitize_mrms_channel("llz", &[-999.0, -99.0, 2.5]);
    assert_eq!(
        low_level_reflectivity,
        vec![QUIET_REFLECTIVITY_DBZ, QUIET_REFLECTIVITY_DBZ, 2.5]
    );

    let rotation = sanitize_mrms_channel("azshear_0_2km", &[-999.0, -99.0, 2.5]);
    assert_eq!(rotation, vec![0.0, -99.0, 2.5]);

    let prate = sanitize_mrms_channel("prate", &[-1.0, 0.0, 4.0]);
    assert_eq!(prate, vec![0.0, 0.0, 4.0]);
}

#[test]
fn validity_masks_track_finite_source_coverage() {
    let values = vec![1.0, 2.0, f32::NAN, 4.0, 10.0, f32::NAN, 30.0, 40.0];
    assert_eq!(
        valid_mask_all_channels(&values, 2, 2),
        vec![1.0, 0.0, 0.0, 1.0]
    );
    assert_eq!(
        valid_mask_any_channel(&values, 2, 2),
        vec![1.0, 1.0, 1.0, 1.0]
    );
}

#[test]
fn derived_goes_fields_expand_to_raw_channel_dependencies() {
    let fields = vec![
        "btd_c13_c15".to_string(),
        "C02".to_string(),
        "ndiff_c02_c01".to_string(),
    ];
    let channels = required_goes_channels(&fields);
    let ids = channels
        .iter()
        .map(|channel| channel.id)
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["C13", "C15", "C02", "C01"]);
}

#[test]
fn stack_obs_bands_computes_derived_goes_fields() {
    let bands = vec![
        crate::native_dataset_obs::RemappedObsBand {
            field_id: "C13".to_string(),
            units: Some("K".to_string()),
            values: vec![250.0, 252.0, f32::NAN, 260.0],
        },
        crate::native_dataset_obs::RemappedObsBand {
            field_id: "C15".to_string(),
            units: Some("K".to_string()),
            values: vec![240.0, 247.0, 240.0, f32::NAN],
        },
        crate::native_dataset_obs::RemappedObsBand {
            field_id: "C02".to_string(),
            units: None,
            values: vec![0.6, 0.5, 0.0, f32::NAN],
        },
        crate::native_dataset_obs::RemappedObsBand {
            field_id: "C01".to_string(),
            units: None,
            values: vec![0.2, 0.5, 0.0, 0.2],
        },
    ];
    let fields = vec![
        "C13".to_string(),
        "btd_c13_c15".to_string(),
        "ndiff_c02_c01".to_string(),
        "unknown".to_string(),
    ];

    let stacked = stack_obs_bands(&fields, &bands, 2);

    assert_eq!(stacked[0], 250.0);
    assert_eq!(stacked[1], 252.0);
    assert!(stacked[2].is_nan());
    assert_eq!(stacked[3], 260.0);
    assert_eq!(stacked[4], 10.0);
    assert_eq!(stacked[5], 5.0);
    assert!(stacked[6].is_nan());
    assert!(stacked[7].is_nan());
    assert!((stacked[8] - 0.5).abs() < 1.0e-6);
    assert_eq!(stacked[9], 0.0);
    assert!(stacked[10].is_nan());
    assert!(stacked[11].is_nan());
    assert!(stacked[12..16].iter().all(|value| value.is_nan()));
}

#[test]
fn reflectivity_threshold_target_uses_sanitized_quiet_values() {
    let target = vec![QUIET_REFLECTIVITY_DBZ, 34.9, 35.0, 60.0];
    assert_eq!(
        threshold_from_refc(&target, 2, REFLECTIVITY_INITIATION_THRESHOLD_DBZ),
        vec![0.0, 0.0, 1.0, 1.0]
    );
}
