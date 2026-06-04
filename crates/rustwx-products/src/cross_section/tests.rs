use super::*;
use rustwx_cross_section::{
    CrossSectionRequest, CrossSectionStyle, SamplingStrategy, ScalarSection, SectionPath,
    TerrainProfile, VerticalAxis, WindOverlayBundle, WindOverlayStyle, decompose_wind_grid,
};

fn sample_surface_fields() -> SurfaceFields {
    SurfaceFields {
        lat: vec![35.0, 35.0, 36.0, 36.0],
        lon: vec![-100.0, -99.0, -100.0, -99.0],
        nx: 2,
        ny: 2,
        projection: None,
        psfc_pa: vec![100000.0, 99500.0, 99000.0, 98500.0],
        orog_m: vec![400.0, 450.0, 600.0, 650.0],
        orog_is_proxy: false,
        t2_k: vec![298.0; 4],
        q2_kgkg: vec![0.012; 4],
        u10_ms: vec![8.0; 4],
        v10_ms: vec![4.0; 4],
        native_sbcape_jkg: None,
        native_mlcape_jkg: None,
        native_mucape_jkg: None,
        native_pblh_m: None,
    }
}

fn sample_pressure_fields() -> PressureFields {
    PressureFields {
        pressure_levels_hpa: vec![1000.0, 850.0],
        pressure_3d_pa: None,
        temperature_c_3d: vec![24.0, 26.0, 22.0, 24.0, 12.0, 14.0, 10.0, 12.0],
        qvapor_kgkg_3d: vec![0.014, 0.013, 0.012, 0.011, 0.010, 0.009, 0.008, 0.007],
        u_ms_3d: vec![12.0, 16.0, 14.0, 18.0, 20.0, 24.0, 22.0, 26.0],
        v_ms_3d: vec![2.0, 4.0, 3.0, 5.0, 6.0, 8.0, 7.0, 9.0],
        gh_m_3d: vec![100.0; 8],
        omega_pa_s_3d: None,
        absolute_vorticity_s_3d: None,
        cloud_liquid_kgkg_3d: None,
        cloud_ice_kgkg_3d: None,
        rain_kgkg_3d: None,
        snow_kgkg_3d: None,
        graupel_kgkg_3d: None,
    }
}

fn sample_layout() -> SectionLayout {
    CrossSectionRequest::new(
        SectionPath::endpoints(
            GeoPoint::new(35.0, -100.0).unwrap(),
            GeoPoint::new(36.0, -99.0).unwrap(),
        )
        .unwrap(),
    )
    .with_sampling(SamplingStrategy::Count(3))
    .with_metadata(
        SectionMetadata::new()
            .with_attribute("route_label", "TEST ROUTE")
            .with_attribute("start_label", "35.00N 100.00W")
            .with_attribute("end_label", "36.00N 99.00W"),
    )
    .build_layout()
    .unwrap()
}

fn below_ground_extrema_fixture() -> (SectionLayout, PressureCrossSectionArtifact) {
    let layout = CrossSectionRequest::new(
        SectionPath::endpoints(
            GeoPoint::new(35.0, -100.0).unwrap(),
            GeoPoint::new(36.0, -99.0).unwrap(),
        )
        .unwrap(),
    )
    .with_sampling(SamplingStrategy::Count(2))
    .build_layout()
    .unwrap();
    let terrain = TerrainProfile::new(layout.sampled_path.distances_km())
        .unwrap()
        .with_surface_pressure_hpa(vec![950.0, 950.0])
        .unwrap()
        .with_surface_height_m(vec![150.0, 250.0])
        .unwrap();
    let section = ScalarSection::new(
        layout.sampled_path.distances_km(),
        VerticalAxis::pressure_hpa(vec![1000.0, 900.0]).unwrap(),
        vec![999.0, 999.0, 10.0, 20.0],
    )
    .unwrap()
    .with_metadata(
        SectionMetadata::new()
            .field("temperature", "C")
            .with_attribute("route_label", "MASK TEST"),
    )
    .with_terrain(terrain)
    .unwrap();
    let wind_overlay = WindOverlayBundle::new(
        decompose_wind_grid(
            &[0.0, 0.0, 5.0, 7.0],
            &[0.0, 0.0, 0.0, 0.0],
            2,
            2,
            &[45.0, 45.0],
        )
        .unwrap(),
        WindOverlayStyle::default(),
    );
    (
        layout,
        PressureCrossSectionArtifact {
            section,
            style: CrossSectionStyle::new(CrossSectionProduct::Temperature),
            wind_overlay,
        },
    )
}

#[test]
fn supported_product_list_matches_current_pressure_section_lane() {
    assert_eq!(
        SUPPORTED_PRESSURE_CROSS_SECTION_PRODUCTS.len(),
        ALL_CROSS_SECTION_PRODUCTS.len() - 1
    );
    for product in ALL_CROSS_SECTION_PRODUCTS {
        assert_eq!(
            supports_pressure_cross_section_product(product),
            product != CrossSectionProduct::Smoke,
            "{product:?}"
        );
    }
}

#[test]
fn pressure_cross_section_facts_capture_route_and_extrema_metadata() {
    let surface = sample_surface_fields();
    let pressure = sample_pressure_fields();
    let layout = sample_layout();
    let artifact = build_pressure_cross_section_from_parts_profiled(
        &surface,
        &pressure,
        ModelId::Hrrr,
        SourceId::Nomads,
        &CycleSpec::new("20260414", 23).unwrap(),
        0,
        &layout,
        CrossSectionProduct::Temperature,
    )
    .unwrap()
    .artifact;

    let facts = summarize_pressure_cross_section_artifact(&layout, &artifact);

    assert_eq!(facts.route.sample_count, 3);
    assert_eq!(facts.route.start.sample_index, 0);
    assert_eq!(facts.route.midpoint.sample_index, 1);
    assert_eq!(facts.route.end.sample_index, 2);
    assert!(facts.route.total_distance_km > 100.0);
    assert_eq!(facts.metadata.field_name.as_deref(), Some("temperature"));
    assert_eq!(facts.metadata.field_units.as_deref(), Some("C"));
    assert_eq!(
        facts
            .metadata
            .attributes
            .get("route_label")
            .map(String::as_str),
        Some("TEST ROUTE")
    );
    assert_eq!(facts.scalar.vertical_kind, "pressure");
    assert_eq!(facts.scalar.vertical_units, "hpa");
    assert_eq!(facts.scalar.level_count, 2);
    assert!(facts.global_minimum().is_some());
    assert!(facts.global_maximum().is_some());
    assert!(facts.global_maximum().unwrap().value >= facts.global_minimum().unwrap().value);
    assert!(facts.lowest_visible_level_minimum().is_some());
    assert!(facts.lowest_visible_level_maximum().is_some());
    let terrain = facts.terrain.as_ref().expect("terrain facts should exist");
    assert!(terrain.surface_pressure_minimum().is_some());
    assert!(terrain.surface_height_maximum_m().is_some());
    assert_eq!(facts.wind.units, "m/s");
    assert!(facts.strongest_wind_speed().is_some());
}

#[test]
fn pressure_cross_section_facts_ignore_below_ground_extrema() {
    let (layout, artifact) = below_ground_extrema_fixture();

    let facts = PressureCrossSectionFacts::from_artifact(&layout, &artifact);

    assert_eq!(facts.global_minimum().unwrap().value, 10.0);
    assert_eq!(facts.global_maximum().unwrap().value, 20.0);
    assert_eq!(facts.lowest_visible_level_maximum().unwrap().value, 20.0);
    assert_eq!(facts.wind.strongest_speed().unwrap().value, 7.0);
    assert_eq!(
        facts
            .terrain
            .as_ref()
            .and_then(|terrain| terrain.surface_pressure_minimum())
            .unwrap()
            .value,
        950.0
    );
}

#[test]
fn pressure_cross_section_builder_returns_finite_theta_e_and_wind_overlay() {
    let surface = sample_surface_fields();
    let pressure = sample_pressure_fields();
    let layout = sample_layout();

    let artifact = build_pressure_cross_section_from_parts_profiled(
        &surface,
        &pressure,
        ModelId::Hrrr,
        SourceId::Nomads,
        &CycleSpec::new("20260414", 23).unwrap(),
        0,
        &layout,
        CrossSectionProduct::ThetaE,
    )
    .unwrap()
    .artifact;

    assert_eq!(artifact.style.product(), CrossSectionProduct::ThetaE);
    assert_eq!(artifact.section.n_points(), 3);
    assert_eq!(artifact.section.n_levels(), 2);
    assert!(
        artifact
            .section
            .values()
            .iter()
            .all(|value| value.is_finite())
    );
    assert_eq!(
        artifact.section.metadata().attribute("product_key"),
        Some("theta_e")
    );
    assert_eq!(artifact.wind_overlay.grid.n_levels(), 2);
    assert_eq!(artifact.wind_overlay.grid.n_points(), 3);
}

#[test]
fn pressure_cross_section_builder_returns_finite_moisture_transport() {
    let surface = sample_surface_fields();
    let pressure = sample_pressure_fields();
    let layout = sample_layout();

    let artifact = build_pressure_cross_section_from_parts_profiled(
        &surface,
        &pressure,
        ModelId::Hrrr,
        SourceId::Nomads,
        &CycleSpec::new("20260414", 23).unwrap(),
        0,
        &layout,
        CrossSectionProduct::MoistureTransport,
    )
    .unwrap()
    .artifact;

    assert_eq!(
        artifact.style.product(),
        CrossSectionProduct::MoistureTransport
    );
    assert_eq!(
        artifact.section.metadata().attribute("product_key"),
        Some("moisture_transport")
    );
    assert_eq!(
        artifact.section.metadata().field_units.as_deref(),
        Some("g*m/kg/s")
    );
    assert!(
        artifact
            .section
            .values()
            .iter()
            .all(|value| value.is_finite() && *value > 0.0)
    );
    assert_eq!(artifact.wind_overlay.grid.n_levels(), 2);
    assert_eq!(artifact.wind_overlay.grid.n_points(), 3);
}

#[test]
fn wind_speed_sections_are_converted_to_knots() {
    let pressure_hpa = [1000.0];
    let temperature_c = [20.0];
    let mixing_ratio_kgkg = [0.010];
    let u_ms = [10.0];
    let v_ms = [0.0];
    let values = build_pressure_cross_section_product_values(
        CrossSectionProduct::WindSpeed,
        PressureCrossSectionProductInputs {
            pressure_hpa: &pressure_hpa,
            temperature_c: &temperature_c,
            mixing_ratio_kgkg: &mixing_ratio_kgkg,
            u_ms: &u_ms,
            v_ms: &v_ms,
            optional: PressureCrossSectionOptionalProductFields::default(),
        },
    )
    .unwrap();

    assert_eq!(values.len(), 1);
    assert!((values[0] - 19.438_444_924_406_05).abs() < 1.0e-6);
}

#[test]
fn specific_humidity_sections_convert_mixing_ratio_to_g_per_kg() {
    let pressure_hpa = [1000.0];
    let temperature_c = [20.0];
    let mixing_ratio_kgkg = [0.010];
    let u_ms = [0.0];
    let v_ms = [0.0];
    let values = build_pressure_cross_section_product_values(
        CrossSectionProduct::SpecificHumidity,
        PressureCrossSectionProductInputs {
            pressure_hpa: &pressure_hpa,
            temperature_c: &temperature_c,
            mixing_ratio_kgkg: &mixing_ratio_kgkg,
            u_ms: &u_ms,
            v_ms: &v_ms,
            optional: PressureCrossSectionOptionalProductFields::default(),
        },
    )
    .unwrap();

    assert_eq!(values.len(), 1);
    assert!((values[0] - 9.900_990_099_009_901).abs() < 1.0e-9);
}

#[test]
fn moisture_and_fire_products_use_shared_pressure_inputs_consistently() {
    let pressure_hpa = [1000.0];
    let temperature_c = [20.0];
    let mixing_ratio_kgkg = [0.010];
    let u_ms = [6.0];
    let v_ms = [8.0];
    let inputs = PressureCrossSectionProductInputs {
        pressure_hpa: &pressure_hpa,
        temperature_c: &temperature_c,
        mixing_ratio_kgkg: &mixing_ratio_kgkg,
        u_ms: &u_ms,
        v_ms: &v_ms,
        optional: PressureCrossSectionOptionalProductFields::default(),
    };

    let relative_humidity =
        build_pressure_cross_section_product_values(CrossSectionProduct::RelativeHumidity, inputs)
            .unwrap();
    let wet_bulb =
        build_pressure_cross_section_product_values(CrossSectionProduct::WetBulb, inputs).unwrap();
    let vapor_pressure_deficit = build_pressure_cross_section_product_values(
        CrossSectionProduct::VaporPressureDeficit,
        inputs,
    )
    .unwrap();
    let dewpoint_depression = build_pressure_cross_section_product_values(
        CrossSectionProduct::DewpointDepression,
        inputs,
    )
    .unwrap();
    let moisture_transport =
        build_pressure_cross_section_product_values(CrossSectionProduct::MoistureTransport, inputs)
            .unwrap();
    let fire_weather =
        build_pressure_cross_section_product_values(CrossSectionProduct::FireWeather, inputs)
            .unwrap();

    let expected_dewpoint_c =
        compute_dewpoint_from_pressure_and_mixing_ratio(&pressure_hpa, &mixing_ratio_kgkg).unwrap();
    let expected_specific_humidity_gkg = mixing_ratio_to_specific_humidity_gkg(&mixing_ratio_kgkg);
    let expected_wind_speed_ms = compute_wind_speed_ms(&u_ms, &v_ms).unwrap();

    assert_eq!(fire_weather, relative_humidity);
    assert!(
        (wet_bulb[0] - approximate_wet_bulb_temperature_c(temperature_c[0], relative_humidity[0]))
            .abs()
            < 1.0e-9
    );
    assert!(
        (vapor_pressure_deficit[0]
            - tetens_saturation_vapor_pressure_hpa(temperature_c[0])
                * (1.0 - (relative_humidity[0] / 100.0).clamp(0.0, 1.0)))
        .abs()
            < 1.0e-9
    );
    assert!((dewpoint_depression[0] - (temperature_c[0] - expected_dewpoint_c[0])).abs() < 1.0e-9);
    assert!(
        (moisture_transport[0] - expected_specific_humidity_gkg[0] * expected_wind_speed_ms[0])
            .abs()
            < 1.0e-9
    );
}

#[test]
fn wxsection_parity_registry_marks_current_and_future_volume_products() {
    assert_eq!(WXSECTION_PARITY_CROSS_SECTION_PRODUCTS.len(), 20);
    assert!(supports_pressure_cross_section_product(
        CrossSectionProduct::Frontogenesis
    ));
    assert!(supports_pressure_cross_section_product(
        CrossSectionProduct::LapseRate
    ));
    assert!(supports_pressure_cross_section_product(
        CrossSectionProduct::Shear
    ));

    let current_vars = ["TMP", "SPFH", "UGRD", "VGRD", "HGT"];
    assert!(
        missing_pressure_volume_requirements(CrossSectionProduct::Frontogenesis, &current_vars)
            .is_empty()
    );
    assert!(
        missing_pressure_volume_requirements(CrossSectionProduct::Shear, &current_vars).is_empty()
    );

    let omega_missing =
        missing_pressure_volume_requirements(CrossSectionProduct::Omega, &current_vars);
    assert!(omega_missing.iter().any(|item| item.contains("VVEL")));

    let smoke_missing =
        missing_pressure_volume_requirements(CrossSectionProduct::Smoke, &current_vars);
    assert!(
        smoke_missing
            .iter()
            .any(|item| item.contains("hybrid:MASSDEN"))
    );
}

#[test]
fn current_pressure_inputs_compute_shear_lapse_rate_and_frontogenesis() {
    let pressure_hpa = [1000.0, 1000.0, 850.0, 850.0];
    let temperature_c = [20.0, 18.0, 10.0, 8.0];
    let mixing_ratio_kgkg = [0.010; 4];
    let u_ms = [5.0, 10.0, 15.0, 25.0];
    let v_ms = [0.0; 4];
    let height_m = [100.0, 100.0, 1500.0, 1500.0];
    let distance_km = [0.0, 100.0];
    let section_wind_ms = u_ms;
    let inputs = PressureCrossSectionProductInputs {
        pressure_hpa: &pressure_hpa,
        temperature_c: &temperature_c,
        mixing_ratio_kgkg: &mixing_ratio_kgkg,
        u_ms: &u_ms,
        v_ms: &v_ms,
        optional: PressureCrossSectionOptionalProductFields {
            height_m: Some(&height_m),
            distance_km: Some(&distance_km),
            section_wind_ms: Some(&section_wind_ms),
            point_count: Some(2),
            level_count: Some(2),
            ..PressureCrossSectionOptionalProductFields::default()
        },
    };

    let shear =
        build_pressure_cross_section_product_values(CrossSectionProduct::Shear, inputs).unwrap();
    let lapse = build_pressure_cross_section_product_values(CrossSectionProduct::LapseRate, inputs)
        .unwrap();
    let frontogenesis =
        build_pressure_cross_section_product_values(CrossSectionProduct::Frontogenesis, inputs)
            .unwrap();

    assert_eq!(shear.len(), 4);
    assert_eq!(lapse.len(), 4);
    assert_eq!(frontogenesis.len(), 4);
    assert!(shear.iter().all(|value| value.is_finite()));
    assert!(lapse.iter().all(|value| value.is_finite()));
    assert!(frontogenesis.iter().all(|value| value.is_finite()));
}

#[test]
fn omega_and_smoke_products_require_optional_upstream_inputs() {
    let pressure_hpa = [1000.0];
    let temperature_c = [20.0];
    let mixing_ratio_kgkg = [0.010];
    let u_ms = [5.0];
    let v_ms = [0.0];
    let inputs = PressureCrossSectionProductInputs {
        pressure_hpa: &pressure_hpa,
        temperature_c: &temperature_c,
        mixing_ratio_kgkg: &mixing_ratio_kgkg,
        u_ms: &u_ms,
        v_ms: &v_ms,
        optional: PressureCrossSectionOptionalProductFields::default(),
    };

    let omega_err = build_pressure_cross_section_product_values(CrossSectionProduct::Omega, inputs)
        .unwrap_err();
    let smoke_err = build_pressure_cross_section_product_values(CrossSectionProduct::Smoke, inputs)
        .unwrap_err();

    assert!(
        omega_err
            .to_string()
            .contains("requires sampled omega input")
    );
    assert!(
        smoke_err
            .to_string()
            .contains("requires sampled smoke input")
    );

    let optional_inputs = PressureCrossSectionProductInputs {
        optional: PressureCrossSectionOptionalProductFields {
            omega_pa_s: Some(&[0.5]),
            smoke_ugm3: Some(&[12.0]),
            ..PressureCrossSectionOptionalProductFields::default()
        },
        ..inputs
    };
    let omega =
        build_pressure_cross_section_product_values(CrossSectionProduct::Omega, optional_inputs)
            .unwrap();
    let smoke =
        build_pressure_cross_section_product_values(CrossSectionProduct::Smoke, optional_inputs)
            .unwrap();

    assert_eq!(omega, vec![18.0]);
    assert_eq!(smoke, vec![12.0]);
}

#[test]
fn sample_stencil_keeps_four_best_candidates_in_distance_order() {
    let surface = SurfaceFields {
        lat: vec![35.0, 35.0, 35.0, 36.0, 36.0, 36.0],
        lon: vec![-101.0, -100.0, -99.0, -101.0, -100.0, -99.0],
        nx: 3,
        ny: 2,
        projection: None,
        psfc_pa: vec![100000.0; 6],
        orog_m: vec![0.0; 6],
        orog_is_proxy: false,
        t2_k: vec![290.0; 6],
        q2_kgkg: vec![0.010; 6],
        u10_ms: vec![5.0; 6],
        v10_ms: vec![2.0; 6],
        native_sbcape_jkg: None,
        native_mlcape_jkg: None,
        native_mucape_jkg: None,
        native_pblh_m: None,
    };
    let point = GeoPoint::new(35.2, -100.1).unwrap();
    let stencil = sample_stencil_for_point(
        &surface,
        &[0usize, 1, 2, 3, 4, 5],
        point,
        HorizontalInterpolation::Bilinear,
    );

    assert_eq!(stencil.len, 4);
    assert_eq!(stencil.indices[0], 1);
    assert!(
        stencil.weights[..stencil.len as usize]
            .iter()
            .all(|weight| weight.is_finite() && *weight > 0.0)
    );
    let weight_sum = stencil.weights[..stencil.len as usize].iter().sum::<f64>();
    assert!((weight_sum - 1.0).abs() < 1.0e-9);
}
