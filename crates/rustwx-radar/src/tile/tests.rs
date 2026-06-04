use super::*;

#[test]
fn web_mercator_round_trip_puts_zero_zero_in_world_center() {
    assert_eq!(lon_to_tile_x(0.0, 1), 1);
    assert_eq!(lat_to_tile_y(0.0, 1), 1);
    let (lon, lat) = tile_pixel_lon_lat(1, 1, 1, 0, 0, 256);
    assert!(lon > 0.0);
    assert!(lat < 0.0);
    let lons = tile_column_longitudes(1, 1, 256);
    let lats = tile_row_latitudes(1, 1, 256);
    assert!((lons[0] - lon).abs() < 1e-10);
    assert!((lats[0] - lat).abs() < 1e-10);
}

#[test]
fn coverage_bounds_contain_site() {
    let site = RadarSite {
        id: "KTLX",
        name: "Oklahoma City",
        lat: 35.0,
        lon: -97.0,
        state: "OK",
    };
    let bounds = radar_coverage_bounds(&site, 230_000.0);
    assert!(bounds[0] < site.lon && site.lon < bounds[2]);
    assert!(bounds[1] < site.lat && site.lat < bounds[3]);
}

#[test]
fn prepared_sweep_reports_native_resolution_metadata() {
    let sweep = Level2Sweep {
        elevation_number: 1,
        elevation_angle: 0.5,
        nyquist_velocity: None,
        radials: vec![
            qc_reflectivity_radial(0.0, vec![10.0, 20.0, 30.0]),
            qc_reflectivity_radial(1.0, vec![11.0, 21.0, 31.0]),
            qc_reflectivity_radial(2.0, vec![12.0, 22.0, 32.0]),
        ],
    };
    let prepared = PreparedSweep::new(
        &sweep,
        RadarProduct::Reflectivity,
        None,
        ColorTablePreset::Default,
        1.0,
    )
    .unwrap();

    assert_eq!(prepared.native_gate_size_m(), Some(250));
    assert_eq!(prepared.native_azimuth_spacing_deg(), Some(1.0));
    let z9_resolution = web_mercator_meters_per_pixel(31.3711, 9);
    assert!(z9_resolution > 250.0 && z9_resolution < 270.0);
}

#[test]
fn intersect_bounds_rejects_disjoint_boxes() {
    assert!(intersect_bounds([-100.0, 30.0, -90.0, 40.0], [-80.0, 30.0, -70.0, 40.0]).is_none());
    assert_eq!(
        intersect_bounds([-100.0, 30.0, -90.0, 40.0], [-95.0, 32.0, -85.0, 45.0]),
        Some([-95.0, 32.0, -90.0, 40.0])
    );
}

#[test]
fn requested_bounds_select_tiles_without_forcing_pixel_crop_by_default() {
    let requested = [-100.0, 34.0, -99.5, 34.5];
    let coverage = [-101.0, 33.0, -98.0, 36.0];
    let tile_bounds = intersect_bounds(requested, coverage).unwrap();

    assert_eq!(
        radar_sampling_bounds(tile_bounds, coverage, false),
        coverage
    );
    assert_eq!(
        radar_sampling_bounds(tile_bounds, coverage, true),
        tile_bounds
    );
}

#[test]
fn tile_options_validate_supersample_factor() {
    let mut options = RadarTileOptions::default();
    options.sample_factor = 0;
    assert!(validate_options(&options).is_err());

    options.sample_factor = 2;
    assert!(validate_options(&options).is_ok());

    options.sample_factor = 5;
    assert!(validate_options(&options).is_err());
}

#[test]
fn velocity_qc_counts_fold_like_neighbor_jumps() {
    let sweep = Level2Sweep {
        elevation_number: 1,
        elevation_angle: 0.5,
        nyquist_velocity: Some(15.0),
        radials: vec![
            qc_velocity_radial(0.0, vec![10.0, -14.0, 12.0]),
            qc_velocity_radial(1.0, vec![11.0, -13.0, 13.0]),
        ],
    };

    let summary = radar_velocity_qc_summary(&sweep, RadarProduct::Velocity).unwrap();

    assert_eq!(summary.finite_gate_count, 6);
    assert_eq!(summary.radial_pair_count, 4);
    assert_eq!(summary.azimuth_pair_count, 3);
    assert_eq!(summary.fold_like_jump_count, 4);
    assert_eq!(summary.severe_jump_count, 4);
    assert!(summary.fold_like_jump_fraction > 0.5);
    assert!(summary.max_abs_jump_ms > 25.0);
}

#[test]
fn product_qc_reports_generic_moment_value_range() {
    let sweep = Level2Sweep {
        elevation_number: 1,
        elevation_angle: 0.5,
        nyquist_velocity: None,
        radials: vec![
            qc_product_radial(
                0.0,
                RadarProduct::CorrelationCoefficient,
                vec![0.5, 0.95, f32::NAN],
            ),
            qc_product_radial(
                1.0,
                RadarProduct::CorrelationCoefficient,
                vec![0.7, 0.8, 1.0],
            ),
        ],
    };

    let summary = radar_product_qc_summary(&sweep, RadarProduct::CorrelationCoefficient).unwrap();

    assert_eq!(summary.product, "cc");
    assert_eq!(summary.finite_gate_count, 5);
    assert!((summary.min_value - 0.5).abs() < f32::EPSILON);
    assert!((summary.max_value - 1.0).abs() < f32::EPSILON);
    assert!((summary.mean_value - 0.79).abs() < 0.001);
}

#[test]
fn product_provenance_reports_phi_derived_kdp() {
    let file = Level2File {
        station_id: "KXXX".to_string(),
        volume_date: 1,
        volume_time: 0,
        vcp: None,
        site_metadata: None,
        partial: false,
        sweeps: vec![Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: None,
            radials: vec![qc_product_radial(
                0.0,
                RadarProduct::DifferentialPhase,
                vec![0.0, 1.0, 2.0],
            )],
        }],
    };

    let provenance = radar_product_provenance(
        &file,
        RadarProduct::SpecificDiffPhase,
        RadarSweepSelection::Lowest,
    );

    assert_eq!(provenance.source, "derived");
    assert!(provenance.derived);
    assert_eq!(provenance.inputs, vec!["phi"]);
    assert_eq!(
        provenance.method.as_deref(),
        Some("centered_phi_range_derivative")
    );
}

#[test]
fn native_kdp_is_preferred_over_phi_derived_kdp() {
    let file = Level2File {
        station_id: "KXXX".to_string(),
        volume_date: 1,
        volume_time: 0,
        vcp: None,
        site_metadata: None,
        partial: false,
        sweeps: vec![Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: None,
            radials: vec![RadialData {
                azimuth: 0.0,
                elevation: 0.5,
                azimuth_spacing: 1.0,
                nyquist_velocity: None,
                radial_status: 1,
                moments: vec![
                    MomentData {
                        product: RadarProduct::DifferentialPhase,
                        gate_count: 3,
                        first_gate_range: 0,
                        gate_size: 250,
                        data_word_size: None,
                        scale: None,
                        offset: None,
                        raw_data: None,
                        data: vec![0.0, 1.0, 2.0],
                    },
                    MomentData {
                        product: RadarProduct::SpecificDiffPhase,
                        gate_count: 3,
                        first_gate_range: 0,
                        gate_size: 250,
                        data_word_size: None,
                        scale: None,
                        offset: None,
                        raw_data: None,
                        data: vec![0.5, 1.5, 2.5],
                    },
                ],
            }],
        }],
    };

    let provenance = radar_product_provenance(
        &file,
        RadarProduct::SpecificDiffPhase,
        RadarSweepSelection::Lowest,
    );

    assert_eq!(provenance.source, "native");
    assert!(!provenance.derived);
    assert!(provenance.inputs.is_empty());
    assert!(provenance.method.is_none());

    let resolved = resolve_tile_sweep(
        &file,
        RadarProduct::SpecificDiffPhase,
        RadarSweepSelection::Lowest,
        false,
        DealiasMethod::Off,
        false,
        false,
        false,
        1,
    )
    .unwrap();
    let kdp = radial_moment_for_product(
        &resolved.sweep().radials[0],
        RadarProduct::SpecificDiffPhase,
    )
    .unwrap();

    assert_eq!(resolved.sweep_index(), 0);
    assert_eq!(kdp.data, vec![0.5, 1.5, 2.5]);
}

#[test]
fn hydrometeor_class_provenance_reports_dual_pol_derived() {
    let file = Level2File {
        station_id: "KXXX".to_string(),
        volume_date: 1,
        volume_time: 0,
        vcp: None,
        site_metadata: None,
        partial: false,
        sweeps: vec![Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: None,
            radials: vec![dual_pol_hca_radial(0.0)],
        }],
    };

    let provenance = radar_product_provenance(
        &file,
        RadarProduct::HydrometeorClass,
        RadarSweepSelection::Lowest,
    );

    assert_eq!(provenance.source, "derived");
    assert!(provenance.derived);
    assert_eq!(provenance.inputs, vec!["ref", "zdr", "cc", "phi"]);
    assert_eq!(provenance.method.as_deref(), Some("dual_pol_rule_hca_v1"));

    let resolved = resolve_tile_sweep(
        &file,
        RadarProduct::HydrometeorClass,
        RadarSweepSelection::Lowest,
        false,
        DealiasMethod::Off,
        false,
        false,
        false,
        1,
    )
    .unwrap();
    let hca =
        radial_moment_for_product(&resolved.sweep().radials[0], RadarProduct::HydrometeorClass)
            .unwrap();

    assert_eq!(resolved.sweep_index(), 0);
    assert_eq!(hca.data[8], 7.0);
}

#[test]
fn reflectivity_despeckle_removes_isolated_gate_only() {
    let sweep = Level2Sweep {
        elevation_number: 1,
        elevation_angle: 0.5,
        nyquist_velocity: None,
        radials: vec![
            qc_reflectivity_radial(0.0, vec![f32::NAN, f32::NAN, 35.0, f32::NAN]),
            qc_reflectivity_radial(1.0, vec![20.0, f32::NAN, 36.0, f32::NAN]),
            qc_reflectivity_radial(2.0, vec![f32::NAN, f32::NAN, f32::NAN, f32::NAN]),
        ],
    };

    let (filtered, summary) = despeckle_reflectivity_sweep(&sweep, RadarProduct::Reflectivity, 1);
    let row0 = radial_moment_for_product(&filtered.radials[0], RadarProduct::Reflectivity).unwrap();
    let row1 = radial_moment_for_product(&filtered.radials[1], RadarProduct::Reflectivity).unwrap();

    assert_eq!(summary.finite_gate_count, 3);
    assert_eq!(summary.removed_gate_count, 1);
    assert!(row1.data[0].is_nan());
    assert_eq!(row0.data[2], 35.0);
    assert_eq!(row1.data[2], 36.0);
}

fn qc_velocity_radial(azimuth: f32, data: Vec<f32>) -> RadialData {
    RadialData {
        azimuth,
        elevation: 0.5,
        azimuth_spacing: 1.0,
        nyquist_velocity: Some(15.0),
        radial_status: 1,
        moments: vec![MomentData {
            product: RadarProduct::Velocity,
            gate_count: data.len() as u16,
            first_gate_range: 0,
            gate_size: 250,
            data_word_size: None,
            scale: None,
            offset: None,
            raw_data: None,
            data,
        }],
    }
}

fn qc_reflectivity_radial(azimuth: f32, data: Vec<f32>) -> RadialData {
    qc_product_radial(azimuth, RadarProduct::Reflectivity, data)
}

fn qc_product_radial(azimuth: f32, product: RadarProduct, data: Vec<f32>) -> RadialData {
    RadialData {
        azimuth,
        elevation: 0.5,
        azimuth_spacing: 1.0,
        nyquist_velocity: None,
        radial_status: 1,
        moments: vec![MomentData {
            product,
            gate_count: data.len() as u16,
            first_gate_range: 0,
            gate_size: 250,
            data_word_size: None,
            scale: None,
            offset: None,
            raw_data: None,
            data,
        }],
    }
}

fn dual_pol_hca_radial(azimuth: f32) -> RadialData {
    RadialData {
        azimuth,
        elevation: 0.5,
        azimuth_spacing: 1.0,
        nyquist_velocity: None,
        radial_status: 1,
        moments: vec![
            qc_moment(RadarProduct::Reflectivity, vec![55.0; 16]),
            qc_moment(RadarProduct::DifferentialReflectivity, vec![0.8; 16]),
            qc_moment(RadarProduct::CorrelationCoefficient, vec![0.97; 16]),
            qc_moment(
                RadarProduct::DifferentialPhase,
                (0..16).map(|idx| idx as f32 * 4.0).collect(),
            ),
        ],
    }
}

fn qc_moment(product: RadarProduct, data: Vec<f32>) -> MomentData {
    MomentData {
        product,
        gate_count: data.len() as u16,
        first_gate_range: 0,
        gate_size: 250,
        data_word_size: None,
        scale: None,
        offset: None,
        raw_data: None,
        data,
    }
}
