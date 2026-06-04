use super::*;
use serde_json::json;

#[test]
fn accepts_conservative_reflectivity_and_improved_dealias() {
    let value = json!({
        "ok": true,
        "name": "ktlx_vel",
        "product": "vel",
        "reflectivity_qc": {
            "finite_gate_count": 1000,
            "removed_gate_count": 10,
            "removed_gate_fraction": 0.01
        },
        "velocity_qc": {
            "finite_gate_count": 1000,
            "fold_like_jump_count": 5,
            "severe_jump_count": 2,
            "fold_like_jump_fraction": 0.002,
            "max_abs_jump_ms": 55.0
        },
        "dealias_qc": {
            "attempted": true,
            "accepted": true,
            "forced": false,
            "decision": "candidate_accepted",
            "changed_gate_count": 100,
            "original_score": {
                "fold_like_jumps": 100,
                "severe_jumps": 50
            },
            "candidate_score": {
                "fold_like_jumps": 20,
                "severe_jumps": 5
            }
        }
    });
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: Some(0.05),
        max_velocity_fold_fraction: Some(0.005),
        max_velocity_severe_jumps: Some(5),
        max_velocity_max_jump_ms: Some(80.0),
        min_product_finite_gates: None,
        min_product_min_value: None,
        min_product_max_value: None,
        max_product_max_value: None,
        require_product_source: None,
        require_product_input: Vec::new(),
        require_product_method: None,
        require_numeric_sidecar: false,
        require_sidecar_value_meaning: Vec::new(),
        require_unclipped_bounds: false,
    };

    let summary = evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

    assert!(summary.ok);
    assert_eq!(summary.entries[0].failures.len(), 0);
}

#[test]
fn rejects_overclean_reflectivity_and_worse_accepted_dealias() {
    let value = json!({
        "ok": true,
        "name": "bad_vel",
        "product": "vel",
        "reflectivity_qc": {
            "finite_gate_count": 1000,
            "removed_gate_count": 120,
            "removed_gate_fraction": 0.12
        },
        "velocity_qc": {
            "finite_gate_count": 1000,
            "fold_like_jump_count": 20,
            "severe_jump_count": 9,
            "fold_like_jump_fraction": 0.02,
            "max_abs_jump_ms": 150.0
        },
        "dealias_qc": {
            "attempted": true,
            "accepted": true,
            "forced": false,
            "decision": "candidate_accepted",
            "changed_gate_count": 100,
            "original_score": {
                "fold_like_jumps": 10,
                "severe_jumps": 5
            },
            "candidate_score": {
                "fold_like_jumps": 12,
                "severe_jumps": 7
            }
        }
    });
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: Some(0.05),
        max_velocity_fold_fraction: Some(0.005),
        max_velocity_severe_jumps: Some(5),
        max_velocity_max_jump_ms: Some(80.0),
        min_product_finite_gates: None,
        min_product_min_value: None,
        min_product_max_value: None,
        max_product_max_value: None,
        require_product_source: None,
        require_product_input: Vec::new(),
        require_product_method: None,
        require_numeric_sidecar: false,
        require_sidecar_value_meaning: Vec::new(),
        require_unclipped_bounds: false,
    };

    let summary = evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

    assert!(!summary.ok);
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("reflectivity removed fraction") })
    );
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("accepted dealias increased fold-like jumps") })
    );
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("accepted dealias increased severe jumps") })
    );
}

#[test]
fn gates_generic_product_qc_ranges() {
    let value = json!({
        "ok": true,
        "name": "ksjt_cc",
        "product": "cc",
        "product_qc": {
            "product": "cc",
            "finite_gate_count": 2000,
            "min_value": 0.2,
            "max_value": 1.0,
            "mean_value": 0.91
        }
    });
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: None,
        max_velocity_fold_fraction: None,
        max_velocity_severe_jumps: None,
        max_velocity_max_jump_ms: None,
        min_product_finite_gates: Some(1000),
        min_product_min_value: Some(0.0),
        min_product_max_value: Some(0.95),
        max_product_max_value: Some(1.05),
        require_product_source: None,
        require_product_input: Vec::new(),
        require_product_method: None,
        require_numeric_sidecar: false,
        require_sidecar_value_meaning: Vec::new(),
        require_unclipped_bounds: false,
    };

    let summary = evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

    assert!(summary.ok);

    let too_low = json!({
        "ok": true,
        "name": "bad_cc",
        "product": "cc",
        "product_qc": {
            "product": "cc",
            "finite_gate_count": 20,
            "min_value": -0.2,
            "max_value": 1.4,
            "mean_value": 0.5
        }
    });
    let summary =
        evaluate_manifest_value(Path::new("manifest.json"), &too_low, &thresholds).unwrap();

    assert!(!summary.ok);
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("product finite gates 20 below 1000") })
    );
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("product min value -0.2000 below 0.0000") })
    );
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("product max value 1.4000 exceeds 1.0500") })
    );
}

#[test]
fn gates_numeric_sidecar_presence_and_manifest_shape() {
    let root = std::env::temp_dir().join(format!(
        "rustwx-radar-quality-sidecar-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let sidecar_manifest = root.join("polar_sidecar_manifest.json");
    let values_path = root.join("polar_values_f32le.bin");
    let flags_path = root.join("polar_gate_flags_u8.bin");
    fs::write(
        &sidecar_manifest,
        serde_json::to_vec_pretty(&json!({
            "schema": RADAR_POLAR_SIDECAR_SCHEMA,
            "sidecar_version": 2,
            "ok": true,
            "name": "ktlx_ref",
            "site": {
                "id": "KTLX",
                "name": "Oklahoma City",
                "state": "OK",
                "lat": 35.333,
                "lon": -97.277,
                "elevation_m": 389.4
            },
            "product": "ref",
            "product_name": "Reflectivity",
            "units": "dBZ",
            "value_meanings": [
                {
                    "value": 7.0,
                    "name": "heavy_rain",
                    "label": "Heavy Rain",
                    "description": "High-reflectivity rain or positive-KDP heavy rain."
                }
            ],
            "product_provenance": {
                "source": "native",
                "derived": false
            },
            "source_key_or_url": "s3://nexrad/KTLX",
            "scan_time_utc": "2026-05-11T00:00:00Z",
            "sweep_index": 0,
            "elevation_deg": 0.5,
            "nyquist_velocity_ms": null,
            "processing_state": "raw",
            "radial_count": 1,
            "max_gate_count": 4,
            "gate_count": 4,
            "values_path": values_path.display().to_string(),
            "values_encoding": "f32_le_row_major_radial_gate_nan_missing",
            "gate_flags_path": flags_path.display().to_string(),
            "gate_flags_encoding": "u8_bitmask_row_major_radial_gate",
            "gate_flag_meanings": [
                {"bit": 0, "mask": 1, "name": "valid", "description": "finite value"},
                {"bit": 1, "mask": 2, "name": "missing", "description": "missing value"},
                {"bit": 2, "mask": 4, "name": "range_folded", "description": "range folded"},
                {"bit": 3, "mask": 8, "name": "filtered", "description": "filtered by QC"},
                {"bit": 4, "mask": 16, "name": "derived", "description": "derived product"},
                {"bit": 5, "mask": 32, "name": "dealiased", "description": "dealiased velocity"}
            ],
            "radials": [{
                "radial_index": 0,
                "azimuth_deg": 0.0,
                "elevation_deg": 0.5,
                "azimuth_spacing_deg": 1.0,
                "gate_count": 4,
                "first_gate_range_m": 0,
                "gate_spacing_m": 250,
                "nyquist_velocity_ms": null,
                "data_word_size_bits": 8,
                "scale": 2.0,
                "offset": 66.0
            }],
            "qc": {}
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(&values_path, [0u8; 16]).unwrap();
    fs::write(&flags_path, [1u8; 4]).unwrap();
    let value = json!({
        "ok": true,
        "name": "ktlx_ref",
        "product": "ref",
        "numeric_sidecar": {
            "schema": RADAR_POLAR_SIDECAR_SCHEMA,
            "manifest_path": sidecar_manifest.display().to_string(),
            "values_path": values_path.display().to_string(),
            "gate_flags_path": flags_path.display().to_string(),
            "radial_count": 1,
            "max_gate_count": 4,
            "gate_count": 4,
            "processing_state": "raw"
        }
    });
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: None,
        max_velocity_fold_fraction: None,
        max_velocity_severe_jumps: None,
        max_velocity_max_jump_ms: None,
        min_product_finite_gates: None,
        min_product_min_value: None,
        min_product_max_value: None,
        max_product_max_value: None,
        require_product_source: None,
        require_product_input: Vec::new(),
        require_product_method: None,
        require_numeric_sidecar: true,
        require_sidecar_value_meaning: vec![
            "heavy_rain".to_string(),
            "Heavy Rain".to_string(),
            "7".to_string(),
        ],
        require_unclipped_bounds: false,
    };

    let summary =
        evaluate_manifest_value(&root.join("tiles_manifest.json"), &value, &thresholds).unwrap();

    assert!(summary.ok);
    assert_eq!(
        summary.entries[0]
            .numeric_sidecar
            .as_ref()
            .map(|sidecar| sidecar.processing_state.as_str()),
        Some("raw")
    );

    let mut missing_meaning_thresholds = thresholds.clone();
    missing_meaning_thresholds.require_sidecar_value_meaning = vec!["large_hail".to_string()];
    let missing = evaluate_manifest_value(
        &root.join("tiles_manifest.json"),
        &value,
        &missing_meaning_thresholds,
    )
    .unwrap();
    assert!(!missing.ok);
    assert!(
        missing
            .failures
            .iter()
            .any(|failure| { failure.contains("value_meanings do not include large_hail") })
    );

    fs::remove_dir_all(root).ok();
}

#[test]
fn gates_unclipped_tile_bounds_metadata() {
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: None,
        max_velocity_fold_fraction: None,
        max_velocity_severe_jumps: None,
        max_velocity_max_jump_ms: None,
        min_product_finite_gates: None,
        min_product_min_value: None,
        min_product_max_value: None,
        max_product_max_value: None,
        require_product_source: None,
        require_product_input: Vec::new(),
        require_product_method: None,
        require_numeric_sidecar: false,
        require_sidecar_value_meaning: Vec::new(),
        require_unclipped_bounds: true,
    };
    let value = json!({
        "ok": true,
        "name": "ksjt_ref_z12",
        "product": "ref",
        "bounds": [-100.9, 31.0, -100.2, 31.7],
        "clip_to_bounds": false,
        "sampling_bounds": [-105.3, 27.2, -95.6, 35.5]
    });

    let summary = evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

    assert!(summary.ok);
    assert_eq!(summary.entries[0].clip_to_bounds, Some(false));
    assert_eq!(
        summary.entries[0].sampling_bounds,
        Some([-105.3, 27.2, -95.6, 35.5])
    );

    let clipped = json!({
        "ok": true,
        "name": "bad_clip",
        "product": "ref",
        "bounds": [-100.9, 31.0, -100.2, 31.7],
        "clip_to_bounds": true,
        "sampling_bounds": [-100.9, 31.0, -100.2, 31.7]
    });
    let summary =
        evaluate_manifest_value(Path::new("manifest.json"), &clipped, &thresholds).unwrap();

    assert!(!summary.ok);
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("clip_to_bounds=Some(true)") })
    );

    let too_small = json!({
        "ok": true,
        "name": "bad_sampling_bounds",
        "product": "ref",
        "bounds": [-100.9, 31.0, -100.2, 31.7],
        "clip_to_bounds": false,
        "sampling_bounds": [-100.8, 31.1, -100.3, 31.6]
    });
    let summary =
        evaluate_manifest_value(Path::new("manifest.json"), &too_small, &thresholds).unwrap();

    assert!(!summary.ok);
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("do not cover tile bounds") })
    );
}

#[test]
fn gates_product_provenance() {
    let value = json!({
        "ok": true,
        "name": "ksjt_kdp",
        "product": "kdp",
        "product_provenance": {
            "source": "derived",
            "derived": true,
            "inputs": ["phi"],
            "method": "centered_phi_range_derivative"
        }
    });
    let thresholds = QualityThresholds {
        max_reflectivity_removed_fraction: None,
        max_velocity_fold_fraction: None,
        max_velocity_severe_jumps: None,
        max_velocity_max_jump_ms: None,
        min_product_finite_gates: None,
        min_product_min_value: None,
        min_product_max_value: None,
        max_product_max_value: None,
        require_product_source: Some("derived".to_string()),
        require_product_input: vec!["phi".to_string()],
        require_product_method: Some("centered_phi_range_derivative".to_string()),
        require_numeric_sidecar: false,
        require_sidecar_value_meaning: Vec::new(),
        require_unclipped_bounds: false,
    };

    let summary = evaluate_manifest_value(Path::new("manifest.json"), &value, &thresholds).unwrap();

    assert!(summary.ok);
    assert_eq!(
        summary.entries[0]
            .product_provenance
            .as_ref()
            .and_then(|provenance| provenance.method.as_deref()),
        Some("centered_phi_range_derivative")
    );

    let missing = json!({
        "ok": true,
        "name": "bad_kdp",
        "product": "kdp"
    });
    let summary =
        evaluate_manifest_value(Path::new("manifest.json"), &missing, &thresholds).unwrap();

    assert!(!summary.ok);
    assert!(
        summary
            .failures
            .iter()
            .any(|failure| { failure.contains("missing product provenance") })
    );
}
