use super::*;

#[test]
fn mesoconvective_v1_channel_order_matches_julia_expectation() {
    let channels = MlChannelPreset::MesoconvectiveV1
        .channels_for_model(ModelId::Hrrr)
        .iter()
        .map(|channel| channel.name())
        .collect::<Vec<_>>();
    assert_eq!(
        channels,
        vec![
            "t2m",
            "d2m",
            "q2m",
            "u10",
            "v10",
            "wind_speed",
            "wind_direction",
            "relative_humidity",
            "cape",
            "refc"
        ]
    );
}

#[test]
fn hybrid_column_v1_uses_exact_channel_names() {
    let channels = MlChannelPreset::HybridColumnV1
        .channels_for_model(ModelId::Hrrr)
        .iter()
        .map(|channel| channel.name())
        .collect::<Vec<_>>();
    assert!(channels.contains(&"mslp"));
    assert!(channels.contains(&"refc"));
    assert!(channels.contains(&"sbcape"));
    assert!(channels.contains(&"mlcape"));
    assert!(channels.contains(&"mucape"));
    assert!(!channels.contains(&"cape"));
}

#[test]
fn hybrid_column_v1_can_exclude_ecape_channels() {
    let request = MlDatasetExportRequest {
        model: ModelId::Hrrr,
        dataset_name: "test".to_string(),
        date_yyyymmdd: "20260422".to_string(),
        cycle_utc: 7,
        forecast_hours: vec![0],
        source: SourceId::Nomads,
        split: MlDatasetSplit::Train,
        out_dir: PathBuf::from("target/test"),
        cache_root: PathBuf::from("target/test-cache"),
        use_cache: true,
        preset: MlChannelPreset::HybridColumnV1,
        include_ecape: false,
        requested_domain_id: None,
        crop_bounds: None,
    };
    let channels = resolved_channels_for_request(&request)
        .iter()
        .map(|channel| channel.name())
        .collect::<Vec<_>>();
    assert!(!channels.contains(&"sbecape"));
    assert!(!channels.contains(&"mlecape"));
    assert!(!channels.contains(&"muecape"));
    assert_eq!(
        excluded_optional_groups(&request),
        vec![MlOptionalChannelGroup::Ecape]
    );
}

#[test]
fn mesoconvective_v1_declares_explicit_compatibility_mode() {
    assert_eq!(
        MlChannelPreset::MesoconvectiveV1.compatibility_mode(),
        Some("wxtrain_legacy_cape_alias")
    );
    assert_eq!(
        MlExportChannel::CapeCompat.compatibility_alias_of(),
        Some("sbcape")
    );
    assert_eq!(MlExportChannel::CapeCompat.canonical_name(), "sbcape");
}

#[test]
fn npy_header_uses_standard_shape_contract() {
    let bytes = build_npy_f32_grid_bytes(2, 3, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).unwrap();
    assert!(bytes.starts_with(b"\x93NUMPY"));
    let header_len = u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
    let header = String::from_utf8(bytes[10..10 + header_len].to_vec()).unwrap();
    assert!(header.contains("'shape': (2, 3)"));
    assert_eq!(
        bytes.len(),
        10 + header_len + 6 * std::mem::size_of::<f32>()
    );
}

#[test]
fn cycle_and_valid_time_metadata_are_explicit() {
    assert_eq!(
        cycle_init_utc("20260422", 7).unwrap(),
        "2026-04-22T07:00:00+00:00"
    );
    assert_eq!(
        valid_time_utc("20260422", 7, 3).unwrap(),
        "2026-04-22T10:00:00+00:00"
    );
}

#[test]
fn split_counts_accumulate_expected_bucket() {
    let mut counts = MlDatasetSplitCounts::default();
    increment_split_count(&mut counts, MlDatasetSplit::Train);
    increment_split_count(&mut counts, MlDatasetSplit::Validation);
    increment_split_count(&mut counts, MlDatasetSplit::Validation);
    increment_split_count(&mut counts, MlDatasetSplit::Test);
    assert_eq!(counts.train, 1);
    assert_eq!(counts.validation, 2);
    assert_eq!(counts.test, 1);
}

#[test]
fn wind_direction_matches_meteorological_from_direction_convention() {
    let directions = compute_wind_direction_deg(&[-10.0, 0.0, 10.0, 0.0], &[0.0, -10.0, 0.0, 10.0]);
    assert_eq!(directions, vec![90.0, 0.0, 270.0, 180.0]);
}

#[test]
fn rrfs_hybrid_profile_is_enabled_but_gfs_is_not() {
    assert!(
        !MlChannelPreset::HybridColumnV1
            .channels_for_model(ModelId::Gfs)
            .is_empty()
            == false
    );
    assert!(
        !MlChannelPreset::HybridColumnV1
            .channels_for_model(ModelId::RrfsA)
            .is_empty()
    );
}

#[test]
fn rrfs_hybrid_profile_excludes_unverified_direct_channels() {
    let channels = MlChannelPreset::HybridColumnV1
        .channels_for_model(ModelId::RrfsA)
        .iter()
        .map(|channel| channel.name())
        .collect::<Vec<_>>();
    assert!(!channels.contains(&"mslp"));
    assert!(!channels.contains(&"refc"));
    assert!(channels.contains(&"terrain"));
    assert!(channels.contains(&"sbcape"));
}

#[test]
fn pressure_level_metadata_is_attached_to_hybrid_channels() {
    assert_eq!(MlExportChannel::T925.level_hpa(), Some(925.0));
    assert_eq!(MlExportChannel::Rh850.level_hpa(), Some(850.0));
    assert_eq!(MlExportChannel::V700.level_hpa(), Some(700.0));
}
