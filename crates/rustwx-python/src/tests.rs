use super::*;

#[test]
fn render_maps_router_splits_heavy_ecape_from_non_ecape_work() {
    let request = RenderMapsRequestJson {
        products: Some(vec![
            "mlecape".to_string(),
            "mlcape".to_string(),
            "srh_0_3km".to_string(),
            "qpf_1h".to_string(),
        ]),
        ..RenderMapsRequestJson::default()
    };

    let routed = route_requested_products(ModelId::Hrrr, &request).unwrap();

    assert_eq!(routed.heavy_derived_recipe_slugs, vec!["mlecape"]);
    assert!(routed.direct_recipe_slugs.is_empty());
    assert_eq!(routed.derived_recipe_slugs, vec!["mlcape", "srh_0_3km"]);
    assert_eq!(routed.windowed_products, vec![HrrrWindowedProduct::Qpf1h]);
}

#[test]
fn render_maps_router_accepts_diurnal_temp_aliases_as_windowed_products() {
    let request = RenderMapsRequestJson {
        products: Some(vec![
            "diurnal_temp_day1_max".to_string(),
            "tmin_24_48h".to_string(),
            "dtr_0_48h".to_string(),
            "diurnal_rh_day1_min".to_string(),
            "2m_dewpoint_24_48h_max".to_string(),
            "vpd2m_0_48h_range".to_string(),
        ]),
        ..RenderMapsRequestJson::default()
    };

    let routed = route_requested_products(ModelId::Hrrr, &request).unwrap();

    assert!(routed.direct_recipe_slugs.is_empty());
    assert!(routed.derived_recipe_slugs.is_empty());
    assert_eq!(
        routed.windowed_products,
        vec![
            HrrrWindowedProduct::Temp2m0to24hMax,
            HrrrWindowedProduct::Temp2m24to48hMin,
            HrrrWindowedProduct::Temp2m0to48hRange,
            HrrrWindowedProduct::Rh2m0to24hMin,
            HrrrWindowedProduct::Dewpoint2m24to48hMax,
            HrrrWindowedProduct::Vpd2m0to48hRange
        ]
    );
}

#[test]
fn render_maps_router_allows_cross_model_total_qpf_windowed_product() {
    let request = RenderMapsRequestJson {
        windowed_products: Some(vec!["qpf_total".to_string()]),
        ..RenderMapsRequestJson::default()
    };

    let routed = route_requested_products(ModelId::Gfs, &request).unwrap();

    assert!(routed.direct_recipe_slugs.is_empty());
    assert!(routed.derived_recipe_slugs.is_empty());
    assert_eq!(
        routed.windowed_products,
        vec![HrrrWindowedProduct::QpfTotal]
    );
}

#[test]
fn prepare_data_skips_derived_recipes_without_precise_fetch_plan() {
    let specs = prepare_fetch_specs_for_product(ModelId::Hrrr, "sbcape");

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].plan_kind, "derived_recipe_no_fetch_plan");
    assert!(specs[0].skip_fetch);
    assert!(specs[0].variable_patterns.is_empty());
}

#[test]
fn prepare_data_keeps_direct_recipe_fetch_plans_enabled() {
    let specs = prepare_fetch_specs_for_product(ModelId::Hrrr, "2m_temperature_10m_winds");

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].plan_kind, "plot_recipe_fetch_plan");
    assert!(!specs[0].skip_fetch);
    assert!(!specs[0].variable_patterns.is_empty());
}

#[test]
fn render_maps_router_rejects_cross_model_hrrr_specific_windowed_product() {
    pyo3::prepare_freethreaded_python();
    let request = RenderMapsRequestJson {
        windowed_products: Some(vec!["qpf_6h".to_string()]),
        ..RenderMapsRequestJson::default()
    };

    let err = route_requested_products(ModelId::Gfs, &request)
        .expect_err("GFS qpf_6h should remain blocked until validated");

    assert!(err.to_string().contains("qpf_total only"));
}

#[test]
fn render_maps_default_cache_is_shared_across_bounds_and_output_dirs() {
    let redding = RenderMapsRequestJson {
        date_yyyymmdd: Some("20260424".to_string()),
        cycle_utc: Some(0),
        forecast_hour: Some(1),
        domain: Some("redding".to_string()),
        bounds: Some(vec![-123.0, -121.5, 39.8, 41.2]),
        out_dir: Some(PathBuf::from("target/render-cache/redding")),
        direct_recipes: Some(vec!["2m_temperature_10m_winds".to_string()]),
        ..RenderMapsRequestJson::default()
    };
    let bakersfield = RenderMapsRequestJson {
        date_yyyymmdd: Some("20260424".to_string()),
        cycle_utc: Some(0),
        forecast_hour: Some(1),
        domain: Some("bakersfield".to_string()),
        bounds: Some(vec![-120.2, -117.8, 34.5, 36.2]),
        out_dir: Some(PathBuf::from("target/render-cache/bakersfield")),
        direct_recipes: Some(vec!["2m_temperature_10m_winds".to_string()]),
        ..RenderMapsRequestJson::default()
    };

    let redding_plan = build_render_maps_plan(redding).unwrap();
    let bakersfield_plan = build_render_maps_plan(bakersfield).unwrap();

    assert_ne!(
        redding_plan.request.out_dir,
        bakersfield_plan.request.out_dir
    );
    assert_ne!(
        redding_plan.request.domains[0].bounds,
        bakersfield_plan.request.domains[0].bounds
    );
    assert_eq!(
        redding_plan.request.cache_root,
        bakersfield_plan.request.cache_root
    );
    assert_eq!(
        redding_plan.request.cache_root,
        default_render_maps_cache_dir()
    );
}

#[test]
fn render_maps_explicit_cache_dir_is_honored() {
    let request = RenderMapsRequestJson {
        date_yyyymmdd: Some("20260424".to_string()),
        cycle_utc: Some(0),
        forecast_hour: Some(1),
        bounds: Some(vec![-123.0, -121.5, 39.8, 41.2]),
        out_dir: Some(PathBuf::from("target/render-cache/redding")),
        cache_dir: Some(PathBuf::from("target/render-cache/shared")),
        direct_recipes: Some(vec!["2m_temperature_10m_winds".to_string()]),
        ..RenderMapsRequestJson::default()
    };

    let plan = build_render_maps_plan(request).unwrap();

    assert_eq!(
        plan.request.cache_root,
        PathBuf::from("target/render-cache/shared")
    );
}

#[test]
fn render_maps_plan_keeps_heavy_only_requests_out_of_non_ecape_runner() {
    let request = RenderMapsRequestJson {
        date_yyyymmdd: Some("20260424".to_string()),
        cycle_utc: Some(22),
        forecast_hour: Some(1),
        domain: Some("heavy-smoke".to_string()),
        bounds: Some(vec![-102.0, -94.0, 33.0, 38.0]),
        products: Some(vec![
            "sbecape".to_string(),
            "mlecape".to_string(),
            "muecape".to_string(),
        ]),
        ..RenderMapsRequestJson::default()
    };

    let plan = build_render_maps_plan(request).unwrap();

    assert!(plan.request.direct_recipe_slugs.is_empty());
    assert!(plan.request.derived_recipe_slugs.is_empty());
    assert!(plan.request.windowed_products.is_empty());
    assert_eq!(
        plan.heavy_derived_recipe_slugs,
        vec!["sbecape", "mlecape", "muecape"]
    );
    assert_eq!(plan.request.domains[0].slug, "heavy_smoke");
}
