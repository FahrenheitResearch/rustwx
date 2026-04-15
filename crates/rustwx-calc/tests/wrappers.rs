use rustwx_calc::{
    BulkRichardsonInputs, CalcError, EcapeGridInputs, EcapeOptions, EcapeTripletOptions,
    EffectiveScpInputs, EffectiveSevereInputs, EffectiveStpInputs, FixedStpInputs, GridShape,
    ScpEhiInputs, ShipInputs, VolumeShape, WindGridInputs, compute_bri, compute_ecape,
    compute_ecape_triplet, compute_ecape_triplet_with_failure_mask,
    compute_ecape_with_failure_mask, compute_effective_severe, compute_ehi, compute_scp,
    compute_scp_effective, compute_scp_ehi, compute_shear, compute_ship, compute_srh, compute_stp,
    compute_stp_effective, compute_stp_fixed, compute_supported_severe_fields,
};

fn sample_volume_shape() -> VolumeShape {
    VolumeShape::new(GridShape::new(1, 1).unwrap(), 6).unwrap()
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1.0e-9,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn ecape_wrapper_matches_single_column_output_shape() {
    let shape = sample_volume_shape();
    let inputs = EcapeGridInputs {
        shape,
        pressure_3d_pa: &[95000.0, 90000.0, 85000.0, 70000.0, 50000.0, 30000.0],
        temperature_3d_c: &[26.0, 22.0, 18.0, 8.0, -10.0, -38.0],
        qvapor_3d_kgkg: &[0.016, 0.013, 0.010, 0.005, 0.0015, 0.0003],
        height_agl_3d_m: &[150.0, 800.0, 1500.0, 3000.0, 5600.0, 9200.0],
        u_3d_ms: &[6.0, 9.0, 12.0, 18.0, 26.0, 33.0],
        v_3d_ms: &[2.0, 5.0, 8.0, 13.0, 20.0, 28.0],
        psfc_pa: &[100000.0],
        t2_k: &[303.15],
        q2_kgkg: &[0.018],
        u10_ms: &[5.0],
        v10_ms: &[1.5],
    };
    let options = EcapeOptions::new("ml", "bunkers_rm").with_pseudoadiabatic(true);

    let result = compute_ecape(inputs, &options).unwrap();

    assert_eq!(result.ecape_jkg.len(), 1);
    assert_eq!(result.ncape_jkg.len(), 1);
    assert_eq!(result.cape_jkg.len(), 1);
    assert_eq!(result.cin_jkg.len(), 1);
    assert_eq!(result.lfc_m.len(), 1);
    assert_eq!(result.el_m.len(), 1);
    assert!(result.ecape_jkg[0].is_finite());
    assert!(result.cape_jkg[0].is_finite());
}

#[test]
fn supported_severe_fields_match_component_wrappers_on_single_column_fixture() {
    let shape = sample_volume_shape();
    let inputs = EcapeGridInputs {
        shape,
        pressure_3d_pa: &[95000.0, 90000.0, 85000.0, 70000.0, 50000.0, 30000.0],
        temperature_3d_c: &[26.0, 22.0, 18.0, 8.0, -10.0, -38.0],
        qvapor_3d_kgkg: &[0.016, 0.013, 0.010, 0.005, 0.0015, 0.0003],
        height_agl_3d_m: &[150.0, 800.0, 1500.0, 3000.0, 5600.0, 9200.0],
        u_3d_ms: &[6.0, 9.0, 12.0, 18.0, 26.0, 33.0],
        v_3d_ms: &[2.0, 5.0, 8.0, 13.0, 20.0, 28.0],
        psfc_pa: &[100000.0],
        t2_k: &[303.15],
        q2_kgkg: &[0.018],
        u10_ms: &[5.0],
        v10_ms: &[1.5],
    };

    let supported = compute_supported_severe_fields(
        shape.grid,
        rustwx_calc::EcapeVolumeInputs {
            pressure_pa: inputs.pressure_3d_pa,
            temperature_c: inputs.temperature_3d_c,
            qvapor_kgkg: inputs.qvapor_3d_kgkg,
            height_agl_m: inputs.height_agl_3d_m,
            u_ms: inputs.u_3d_ms,
            v_ms: inputs.v_3d_ms,
            nz: shape.nz,
        },
        rustwx_calc::SurfaceInputs {
            psfc_pa: inputs.psfc_pa,
            t2_k: inputs.t2_k,
            q2_kgkg: inputs.q2_kgkg,
            u10_ms: inputs.u10_ms,
            v10_ms: inputs.v10_ms,
        },
    )
    .unwrap();

    assert_eq!(supported.sbcape_jkg.len(), 1);
    assert_eq!(supported.mlcin_jkg.len(), 1);
    assert_eq!(supported.mucape_jkg.len(), 1);
    assert_eq!(supported.srh_01km_m2s2.len(), 1);
    assert_eq!(supported.srh_03km_m2s2.len(), 1);
    assert_eq!(supported.shear_06km_ms.len(), 1);
    assert_eq!(supported.stp_fixed.len(), 1);
    assert_eq!(supported.scp_mu_03km_06km_proxy.len(), 1);
    assert_eq!(supported.ehi_sb_01km_proxy.len(), 1);
    assert!(supported.stp_fixed[0].is_finite());
    assert!(supported.scp_mu_03km_06km_proxy[0].is_finite());
    assert!(supported.ehi_sb_01km_proxy[0].is_finite());
}

#[test]
fn ecape_failure_mask_exposes_zero_fill_columns() {
    let shape = VolumeShape::new(GridShape::new(1, 1).unwrap(), 2).unwrap();
    let inputs = EcapeGridInputs {
        shape,
        pressure_3d_pa: &[f64::NAN, f64::NAN],
        temperature_3d_c: &[f64::NAN, f64::NAN],
        qvapor_3d_kgkg: &[f64::NAN, f64::NAN],
        height_agl_3d_m: &[f64::NAN, f64::NAN],
        u_3d_ms: &[f64::NAN, f64::NAN],
        v_3d_ms: &[f64::NAN, f64::NAN],
        psfc_pa: &[100000.0],
        t2_k: &[300.0],
        q2_kgkg: &[0.014],
        u10_ms: &[4.0],
        v10_ms: &[1.0],
    };
    let options = EcapeOptions::new("sb", "mean_wind").with_pseudoadiabatic(true);

    let result = compute_ecape_with_failure_mask(inputs, &options).unwrap();

    assert_eq!(result.failure_mask, vec![1]);
    assert_eq!(result.failure_count(), 1);
    assert_eq!(result.fields.ecape_jkg, vec![0.0]);
    assert_eq!(result.fields.ncape_jkg, vec![0.0]);
    assert_eq!(result.fields.cape_jkg, vec![0.0]);
    assert_eq!(result.fields.cin_jkg, vec![0.0]);
}

#[test]
fn severe_wrappers_match_underlying_grid_math() {
    let shape = VolumeShape::new(GridShape::new(1, 1).unwrap(), 3).unwrap();
    let wind = WindGridInputs {
        shape,
        u_3d_ms: &[0.0, 10.0, 20.0],
        v_3d_ms: &[0.0, 0.0, 0.0],
        height_agl_3d_m: &[0.0, 3000.0, 6000.0],
    };

    let shear = compute_shear(wind, 0.0, 6000.0).unwrap();
    let srh = compute_srh(wind, 3000.0).unwrap();
    let grid = GridShape::new(1, 1).unwrap();
    let stp = compute_stp(grid, &[1500.0], &[1000.0], &[150.0], &[20.0]).unwrap();
    let ehi = compute_ehi(grid, &[2000.0], &[200.0]).unwrap();
    let scp = compute_scp(grid, &[3000.0], &[150.0], &[20.0]).unwrap();

    assert_eq!(shear, vec![20.0]);
    assert_eq!(
        srh,
        metrust::calc::severe::grid::compute_srh(
            wind.u_3d_ms,
            wind.v_3d_ms,
            wind.height_agl_3d_m,
            1,
            1,
            3,
            3000.0,
        )
    );
    assert_eq!(stp, vec![1.0]);
    assert_eq!(ehi, vec![2.5]);
    assert_eq!(scp, vec![9.0]);
}

#[test]
fn ecape_triplet_wrapper_matches_masked_fields() {
    let shape = sample_volume_shape();
    let inputs = EcapeGridInputs {
        shape,
        pressure_3d_pa: &[95000.0, 90000.0, 85000.0, 70000.0, 50000.0, 30000.0],
        temperature_3d_c: &[26.0, 22.0, 18.0, 8.0, -10.0, -38.0],
        qvapor_3d_kgkg: &[0.016, 0.013, 0.010, 0.005, 0.0015, 0.0003],
        height_agl_3d_m: &[150.0, 800.0, 1500.0, 3000.0, 5600.0, 9200.0],
        u_3d_ms: &[6.0, 9.0, 12.0, 18.0, 26.0, 33.0],
        v_3d_ms: &[2.0, 5.0, 8.0, 13.0, 20.0, 28.0],
        psfc_pa: &[100000.0],
        t2_k: &[303.15],
        q2_kgkg: &[0.018],
        u10_ms: &[5.0],
        v10_ms: &[1.5],
    };
    let options = EcapeTripletOptions::new("bunkers_rm").with_pseudoadiabatic(true);

    let masked = compute_ecape_triplet_with_failure_mask(inputs, &options).unwrap();
    let unmasked = compute_ecape_triplet(inputs, &options).unwrap();

    assert_eq!(unmasked.sb, masked.sb.fields);
    assert_eq!(unmasked.ml, masked.ml.fields);
    assert_eq!(unmasked.mu, masked.mu.fields);
    assert_eq!(masked.total_failure_count(), 0);
}

#[test]
fn fixed_stp_wrapper_uses_operational_thresholds() {
    let grid = GridShape::new(3, 1).unwrap();

    let stp = compute_stp_fixed(FixedStpInputs {
        grid,
        sbcape_jkg: &[1500.0, 1500.0, 1500.0],
        lcl_m: &[500.0, 1000.0, 1000.0],
        srh_1km_m2s2: &[150.0, 150.0, 150.0],
        shear_6km_ms: &[20.0, 12.0, 40.0],
    })
    .unwrap();

    assert_eq!(stp, vec![1.0, 0.0, 1.5]);
}

#[test]
fn effective_stp_wrapper_matches_source_formula() {
    let grid = GridShape::new(4, 1).unwrap();

    let stp = compute_stp_effective(EffectiveStpInputs {
        grid,
        mlcape_jkg: &[1500.0, 1500.0, 1500.0, 1500.0],
        mlcin_jkg: &[-50.0, -50.0, -250.0, -50.0],
        ml_lcl_m: &[1000.0, 1000.0, 1000.0, 1000.0],
        effective_srh_m2s2: &[150.0, 150.0, 150.0, 150.0],
        effective_bulk_wind_difference_ms: &[10.0, 20.0, 20.0, 40.0],
    })
    .unwrap();

    assert_eq!(stp, vec![0.0, 1.0, 0.0, 1.5]);
}

#[test]
fn compatibility_stp_wrapper_routes_to_fixed_formula() {
    let grid = GridShape::new(1, 1).unwrap();

    let explicit = compute_stp_fixed(FixedStpInputs {
        grid,
        sbcape_jkg: &[1500.0],
        lcl_m: &[500.0],
        srh_1km_m2s2: &[150.0],
        shear_6km_ms: &[20.0],
    })
    .unwrap();
    let compat = compute_stp(grid, &[1500.0], &[500.0], &[150.0], &[20.0]).unwrap();

    assert_eq!(compat, explicit);
    assert_eq!(compat, vec![1.0]);
}

#[test]
fn effective_scp_wrapper_uses_effective_bulk_wind_difference_thresholds() {
    let grid = GridShape::new(3, 1).unwrap();

    let scp = compute_scp_effective(EffectiveScpInputs {
        grid,
        mucape_jkg: &[3000.0, 3000.0, 3000.0],
        effective_srh_m2s2: &[150.0, 150.0, 150.0],
        effective_bulk_wind_difference_ms: &[8.0, 20.0, 30.0],
    })
    .unwrap();

    assert_eq!(scp, vec![0.0, 9.0, 9.0]);
}

#[test]
fn effective_stp_wrapper_rejects_bad_lengths() {
    let grid = GridShape::new(2, 1).unwrap();

    let err = compute_stp_effective(EffectiveStpInputs {
        grid,
        mlcape_jkg: &[1500.0],
        mlcin_jkg: &[-50.0, -50.0],
        ml_lcl_m: &[1000.0, 1000.0],
        effective_srh_m2s2: &[150.0, 150.0],
        effective_bulk_wind_difference_ms: &[20.0, 20.0],
    })
    .unwrap_err();

    assert!(matches!(
        err,
        CalcError::LengthMismatch {
            field: "mlcape_jkg",
            ..
        }
    ));
}

#[test]
fn effective_severe_wrapper_reuses_effective_inputs_for_stp_and_scp() {
    let grid = GridShape::new(4, 1).unwrap();

    let outputs = compute_effective_severe(EffectiveSevereInputs {
        grid,
        mlcape_jkg: &[1500.0, 1500.0, 1500.0, 1500.0],
        mlcin_jkg: &[-50.0, -50.0, -250.0, -50.0],
        ml_lcl_m: &[1000.0, 1000.0, 1000.0, 1000.0],
        mucape_jkg: &[3000.0, 3000.0, 3000.0, 3000.0],
        effective_srh_m2s2: &[150.0, 150.0, 150.0, 150.0],
        effective_bulk_wind_difference_ms: &[8.0, 20.0, 20.0, 40.0],
    })
    .unwrap();

    assert_eq!(outputs.stp_effective, vec![0.0, 1.0, 0.0, 1.5]);
    assert_eq!(outputs.scp_effective, vec![0.0, 9.0, 9.0, 9.0]);
}

#[test]
fn effective_severe_wrapper_rejects_bad_lengths() {
    let grid = GridShape::new(2, 1).unwrap();

    let err = compute_effective_severe(EffectiveSevereInputs {
        grid,
        mlcape_jkg: &[1500.0, 1500.0],
        mlcin_jkg: &[-50.0, -50.0],
        ml_lcl_m: &[1000.0, 1000.0],
        mucape_jkg: &[3000.0],
        effective_srh_m2s2: &[150.0, 150.0],
        effective_bulk_wind_difference_ms: &[20.0, 20.0],
    })
    .unwrap_err();

    assert!(matches!(
        err,
        CalcError::LengthMismatch {
            field: "mucape_jkg",
            ..
        }
    ));
}

#[test]
fn scp_ehi_wrapper_matches_cached_proof_style_inputs() {
    let grid = GridShape::new(3, 1).unwrap();

    let outputs = compute_scp_ehi(ScpEhiInputs {
        grid,
        scp_cape_jkg: &[3000.0, 3000.0, 3000.0],
        scp_srh_m2s2: &[150.0, 150.0, 150.0],
        scp_bulk_wind_difference_ms: &[8.0, 20.0, 30.0],
        ehi_cape_jkg: &[2000.0, 1600.0, 800.0],
        ehi_srh_m2s2: &[200.0, 100.0, 50.0],
    })
    .unwrap();

    assert_eq!(outputs.scp, vec![0.0, 9.0, 9.0]);
    assert_eq!(outputs.ehi, vec![2.5, 1.0, 0.25]);
}

#[test]
fn scp_ehi_wrapper_rejects_bad_lengths() {
    let grid = GridShape::new(2, 1).unwrap();

    let err = compute_scp_ehi(ScpEhiInputs {
        grid,
        scp_cape_jkg: &[3000.0, 3000.0],
        scp_srh_m2s2: &[150.0, 150.0],
        scp_bulk_wind_difference_ms: &[20.0, 20.0],
        ehi_cape_jkg: &[2000.0],
        ehi_srh_m2s2: &[200.0, 200.0],
    })
    .unwrap_err();

    assert!(matches!(
        err,
        CalcError::LengthMismatch {
            field: "ehi_cape_jkg",
            ..
        }
    ));
}

#[test]
fn ship_wrapper_matches_local_component_formula() {
    let grid = GridShape::new(3, 1).unwrap();

    let ship = compute_ship(ShipInputs {
        grid,
        mucape_jkg: &[2000.0, 1000.0, 2000.0],
        shear_6km_ms: &[20.0, 20.0, 20.0],
        temperature_500c: &[-15.0, -15.0, 5.0],
        lapse_rate_700_500_cpkm: &[7.0, 7.0, 7.0],
        mixing_ratio_500_gkg: &[10.0, 10.0, 10.0],
    })
    .unwrap();

    assert_close(ship[0], 1.0);
    assert_close(ship[1], 0.38461538461538464);
    assert_close(ship[2], 0.0);
}

#[test]
fn ship_wrapper_rejects_bad_lengths() {
    let grid = GridShape::new(2, 1).unwrap();

    let err = compute_ship(ShipInputs {
        grid,
        mucape_jkg: &[2000.0],
        shear_6km_ms: &[20.0, 20.0],
        temperature_500c: &[-15.0, -15.0],
        lapse_rate_700_500_cpkm: &[7.0, 7.0],
        mixing_ratio_500_gkg: &[10.0, 10.0],
    })
    .unwrap_err();

    assert!(matches!(
        err,
        CalcError::LengthMismatch {
            field: "mucape_jkg",
            ..
        }
    ));
}

#[test]
fn bri_wrapper_matches_local_brn_shear_behavior() {
    let grid = GridShape::new(3, 1).unwrap();

    let bri = compute_bri(BulkRichardsonInputs {
        grid,
        cape_jkg: &[2000.0, 500.0, 1000.0],
        brn_shear_ms: &[20.0, 30.0, 0.1],
    })
    .unwrap();

    assert_close(bri[0], 10.0);
    assert_close(bri[1], 1.1111111111111112);
    assert_close(bri[2], 0.0);
}

#[test]
fn bri_wrapper_rejects_bad_lengths() {
    let grid = GridShape::new(2, 1).unwrap();

    let err = compute_bri(BulkRichardsonInputs {
        grid,
        cape_jkg: &[2000.0],
        brn_shear_ms: &[20.0, 20.0],
    })
    .unwrap_err();

    assert!(matches!(
        err,
        CalcError::LengthMismatch {
            field: "cape_jkg",
            ..
        }
    ));
}
