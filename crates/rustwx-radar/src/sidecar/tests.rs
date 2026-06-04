use super::*;
use crate::nexrad::level2::{MomentData, RadialData};

#[test]
fn polar_sidecar_round_trips_values_masks_and_sampling() {
    let root = std::env::temp_dir().join(format!("rustwx-radar-sidecar-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let site = RadarSite {
        id: "KTLX",
        name: "Oklahoma City",
        state: "OK",
        lat: 35.0,
        lon: -97.0,
    };
    let sweep = Level2Sweep {
        elevation_number: 1,
        elevation_angle: 0.0,
        nyquist_velocity: None,
        radials: vec![RadialData {
            azimuth: 0.0,
            elevation: 0.0,
            azimuth_spacing: 1.0,
            nyquist_velocity: None,
            radial_status: 1,
            moments: vec![MomentData {
                product: RadarProduct::Reflectivity,
                gate_count: 4,
                first_gate_range: 0,
                gate_size: 250,
                data_word_size: Some(8),
                scale: Some(2.0),
                offset: Some(66.0),
                raw_data: Some(vec![2, 0, 1, 106]),
                data: vec![10.0, f32::NAN, f32::NAN, 20.0],
            }],
        }],
    };

    let record = write_polar_sidecar(
        &sweep,
        &site,
        RadarProduct::Reflectivity,
        &root,
        RadarPolarSidecarOptions {
            name: "test".to_string(),
            source_key_or_url: Some("s3://nexrad/KTLX".to_string()),
            scan_time_utc: "2026-05-11T00:00:00Z".to_string(),
            site_lat: None,
            site_lon: None,
            site_elevation_m: None,
            site_feedhorn_height_m: None,
            sweep_index: 0,
            processing_state: "raw".to_string(),
            product_provenance: serde_json::json!({"source": "native", "derived": false}),
            product_qc: None,
            velocity_qc: None,
            dealias_qc: None,
            velocity_quality_qc: None,
            reflectivity_qc: None,
        },
    )
    .unwrap();

    let sidecar = RadarPolarSidecar::open(&record.manifest_path).unwrap();
    assert_eq!(sidecar.manifest.radial_count, 1);
    assert_eq!(sidecar.manifest.site.elevation_m, Some(389.4));
    assert_eq!(sidecar.manifest.radials[0].scale, Some(2.0));
    assert_eq!(sidecar.manifest.radials[0].offset, Some(66.0));
    assert!(sidecar.manifest.value_meanings.is_empty());
    assert!(
        sidecar
            .manifest
            .gate_flag_meanings
            .iter()
            .any(|meaning| meaning.name == "range_folded"
                && meaning.mask == GATE_FLAG_RANGE_FOLDED)
    );
    assert_eq!(sidecar.gate_flags[1] & GATE_FLAG_MISSING, GATE_FLAG_MISSING);
    assert_eq!(
        sidecar.gate_flags[2] & GATE_FLAG_RANGE_FOLDED,
        GATE_FLAG_RANGE_FOLDED
    );

    let (lat, lon) = radar_polar_to_lat_lon(site.lat, site.lon, 0.0, 750.0);
    let sample = sidecar
        .sample_lat_lon(lat, lon, RadarPolarSampleMethod::Nearest)
        .unwrap();
    assert_eq!(sample.value, Some(20.0));
    assert_eq!(sample.value_label, None);
    assert_eq!(sample.gate_index, 3);
    assert_eq!(sample.radial_index, 0);
    assert_eq!(sample.units, "dBZ");
    assert!(sample.gate_flags.iter().any(|flag| flag == "valid"));
    assert_eq!(sample.lat, lat);
    assert_eq!(sample.lon, lon);
    assert_eq!(sample.first_gate_range_m, 0);
    assert_eq!(sample.gate_spacing_m, 250);
    assert_eq!(sample.azimuth_spacing_deg, 1.0);
    assert_eq!(sample.raw, true);
    assert_eq!(sample.dealiased, false);
    assert_eq!(sample.filtered, false);
    assert_eq!(sample.derived, false);

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn polar_sidecar_labels_categorical_hca_values() {
    let root =
        std::env::temp_dir().join(format!("rustwx-radar-sidecar-hca-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let site = RadarSite {
        id: "KTLX",
        name: "Oklahoma City",
        lat: 35.333,
        lon: -97.277,
        state: "OK",
    };
    let sweep = Level2Sweep {
        elevation_number: 1,
        elevation_angle: 0.5,
        nyquist_velocity: None,
        radials: vec![RadialData {
            azimuth: 0.0,
            elevation: 0.5,
            azimuth_spacing: 1.0,
            nyquist_velocity: None,
            radial_status: 1,
            moments: vec![MomentData {
                product: RadarProduct::HydrometeorClass,
                gate_count: 1,
                first_gate_range: 0,
                gate_size: 250,
                data_word_size: None,
                scale: None,
                offset: None,
                raw_data: None,
                data: vec![7.0],
            }],
        }],
    };

    let record = write_polar_sidecar(
        &sweep,
        &site,
        RadarProduct::HydrometeorClass,
        &root,
        RadarPolarSidecarOptions {
            name: "hca".to_string(),
            source_key_or_url: Some("s3://nexrad/KTLX".to_string()),
            scan_time_utc: "2026-05-11T00:00:00Z".to_string(),
            site_lat: None,
            site_lon: None,
            site_elevation_m: None,
            site_feedhorn_height_m: None,
            sweep_index: 0,
            processing_state: "derived".to_string(),
            product_provenance: serde_json::json!({
                "source": "derived",
                "derived": true,
                "inputs": ["ref", "zdr", "cc", "phi"],
                "method": "dual_pol_rule_hca_v1"
            }),
            product_qc: None,
            velocity_qc: None,
            dealias_qc: None,
            velocity_quality_qc: None,
            reflectivity_qc: None,
        },
    )
    .unwrap();

    let sidecar = RadarPolarSidecar::open(&record.manifest_path).unwrap();
    assert!(
        sidecar
            .manifest
            .value_meanings
            .iter()
            .any(|meaning| meaning.value == 7.0 && meaning.label == "Heavy Rain")
    );

    let (lat, lon) = radar_polar_to_lat_lon(site.lat, site.lon, 0.0, 0.0);
    let sample = sidecar
        .sample_lat_lon(lat, lon, RadarPolarSampleMethod::Interpolated)
        .unwrap();
    assert_eq!(sample.method, "nearest");
    assert_eq!(sample.value, Some(7.0));
    assert_eq!(sample.value_label.as_deref(), Some("Heavy Rain"));
    assert!(sample.gate_flags.iter().any(|flag| flag == "derived"));

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn radar_relative_coordinates_round_trip_for_sidecar_queries() {
    let site_lat = 35.333;
    let site_lon = -97.277;
    let azimuth_deg = 42.0;
    let range_m = 87_500.0;

    let (lat, lon) = radar_polar_to_lat_lon(site_lat, site_lon, azimuth_deg, range_m);
    let polar = radar_lat_lon_to_polar(site_lat, site_lon, lat, lon);

    assert!((polar.azimuth_deg - azimuth_deg).abs() < 0.001);
    assert!((polar.ground_range_m - range_m).abs() < 0.1);
}

#[test]
fn radar_relative_coordinates_use_great_circle_geometry_at_long_range() {
    let site_lat = 35.333;
    let site_lon = -97.277;
    let azimuth_deg = 90.0;
    let range_m = 460_000.0;

    let (lat, lon) = radar_polar_to_lat_lon(site_lat, site_lon, azimuth_deg, range_m);
    let polar = radar_lat_lon_to_polar(site_lat, site_lon, lat, lon);

    assert!(lat < site_lat - 0.05);
    assert!((polar.azimuth_deg - azimuth_deg).abs() < 0.001);
    assert!((polar.ground_range_m - range_m).abs() < 0.1);
}

#[test]
fn radar_relative_coordinates_wrap_antimeridian() {
    let (lat, lon) = radar_polar_to_lat_lon(20.0, 179.8, 90.0, 80_000.0);
    let polar = radar_lat_lon_to_polar(20.0, 179.8, lat, lon);

    assert!(lon < -179.0);
    assert!((polar.azimuth_deg - 90.0).abs() < 0.001);
    assert!((polar.ground_range_m - 80_000.0).abs() < 0.1);
}

#[test]
fn polar_sidecar_rejects_invalid_manifest_shape_and_escaping_paths() {
    let root = std::env::temp_dir().join(format!(
        "rustwx-radar-sidecar-invalid-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join(VALUES_FILE_NAME), []).unwrap();
    fs::write(root.join(GATE_FLAGS_FILE_NAME), []).unwrap();
    let manifest_path = root.join("polar_sidecar_manifest.json");
    let mut manifest = serde_json::json!({
        "schema": RADAR_POLAR_SIDECAR_SCHEMA,
        "sidecar_version": 1,
        "ok": true,
        "name": "bad",
        "site": {
            "id": "KTLX",
            "name": "Oklahoma City",
            "state": "OK",
            "lat": 35.0,
            "lon": -97.0,
            "elevation_m": 370.0,
            "feedhorn_height_m": 20.0,
            "antenna_elevation_m": 390.0
        },
        "product": "ref",
        "product_name": "Reflectivity",
        "units": "dBZ",
        "product_provenance": {"source": "native", "derived": false},
        "source_key_or_url": "s3://nexrad/KTLX",
        "scan_time_utc": "2026-05-11T00:00:00Z",
        "sweep_index": 0,
        "elevation_deg": 0.0,
        "nyquist_velocity_ms": null,
        "processing_state": "raw",
        "radial_count": 0,
        "max_gate_count": 0,
        "gate_count": 0,
        "values_path": VALUES_FILE_NAME,
        "values_encoding": "f32_le_row_major_radial_gate_nan_missing",
        "gate_flags_path": GATE_FLAGS_FILE_NAME,
        "gate_flags_encoding": "u8_bitmask_row_major_radial_gate",
        "radials": [],
        "qc": {}
    });
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
    assert!(
        err.to_string()
            .contains("unsupported radar sidecar version")
    );

    manifest["sidecar_version"] = serde_json::json!(2);
    manifest["ok"] = serde_json::json!(false);
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
    assert!(err.to_string().contains("manifest is not ok"));

    manifest["ok"] = serde_json::json!(true);
    manifest["radial_count"] = serde_json::json!(1);
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
    assert!(err.to_string().contains("radial metadata mismatch"));

    let outside_values = root
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(
            "rustwx-radar-sidecar-outside-values-{}",
            std::process::id()
        ));
    fs::write(&outside_values, []).unwrap();
    manifest["radial_count"] = serde_json::json!(0);
    manifest["values_path"] = serde_json::json!(format!(
        "../{}",
        outside_values.file_name().unwrap().to_string_lossy()
    ));
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let err = RadarPolarSidecar::open(&manifest_path).unwrap_err();
    assert!(err.to_string().contains("escapes sidecar root"));

    let _ = fs::remove_file(outside_values);
    let _ = fs::remove_dir_all(&root);
}
