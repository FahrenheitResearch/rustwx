use super::*;
use std::path::Path;

#[test]
fn regular_latlon_geometry_reconstructs_mesh() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "gfs".to_string(),
        run: "20260506_gfs_18z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 2,
        ny: 2,
        forecast_hours: vec![0],
        chunk_y: 2,
        chunk_x: 2,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "regular_latlon",
            "lat_start": 40.0,
            "lat_step": -1.0,
            "lon_start": -101.0,
            "lon_step": 1.0,
            "bounds": [-101.0, 39.0, -100.0, 40.0]
        }),
    };
    let geometry = geometry_from_wxa_meta(&meta).unwrap();
    assert_eq!(geometry.grid.lat_deg, vec![40.0, 40.0, 39.0, 39.0]);
    assert_eq!(geometry.grid.lon_deg, vec![-101.0, -100.0, -101.0, -100.0]);
}

#[test]
fn wxa_grid_meta_records_requested_domain() {
    let domain = DomainSpec::new("conus", LEGACY_CONUS_WXA_FRAME_BOUNDS);
    let grid_meta = wxa_grid_meta_from_latlon(
        "nbm",
        2,
        2,
        &[52.0, 52.0, 23.0, 23.0],
        &[-127.0, -66.0, -127.0, -66.0],
        None,
        Some([-127.0, 23.0, -66.0, 52.0]),
        Some(&domain),
    );

    let requested = grid_meta
        .get("requested_domain")
        .expect("requested domain should be written");
    assert_eq!(requested.get("slug").and_then(Value::as_str), Some("conus"));
    assert_eq!(
        requested.get("west").and_then(Value::as_f64),
        Some(LEGACY_CONUS_WXA_FRAME_BOUNDS.0)
    );
    assert_eq!(
        requested.get("east").and_then(Value::as_f64),
        Some(LEGACY_CONUS_WXA_FRAME_BOUNDS.1)
    );
}

#[test]
fn requested_domain_metadata_drives_wxa_plot_bounds() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "gfs".to_string(),
        run: "20260506_gfs_18z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 2,
        ny: 2,
        forecast_hours: vec![0],
        chunk_y: 2,
        chunk_x: 2,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "regular_latlon",
            "lat_start": 80.0,
            "lat_step": -160.0,
            "lon_start": -180.0,
            "lon_step": 359.0,
            "bounds": [-180.0, -80.0, 179.0, 80.0],
            "requested_domain": {
                "slug": "conus",
                "west": -127.0,
                "east": -66.0,
                "south": 23.0,
                "north": 51.5
            }
        }),
    };
    let geometry = geometry_from_wxa_meta(&meta).unwrap();

    assert_eq!(
        default_wxa_plot_bounds(&meta, &geometry),
        LEGACY_CONUS_WXA_FRAME_BOUNDS
    );
}

#[test]
fn broad_legacy_rrfs_wxa_bounds_frame_as_conus() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "rrfs-a".to_string(),
        run: "20260604_rrfs_a_00z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 3,
        ny: 3,
        forecast_hours: vec![0],
        chunk_y: 3,
        chunk_x: 3,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "curvilinear_latlon_sampled",
            "bounds": [-134.7235, 10.9553, -37.03, 60.775],
            "sample": {
                "nx": 3,
                "ny": 3,
                "x": [0, 1, 2],
                "y": [0, 1, 2],
                "lat": [
                    60.7, 56.0, 51.5,
                    42.0, 38.0, 34.0,
                    23.0, 18.0, 10.9
                ],
                "lon": [
                    -134.7, -93.0, -37.0,
                    -130.0, -96.0, -66.0,
                    -127.0, -101.0, -60.0
                ]
            }
        }),
    };
    let geometry = geometry_from_wxa_meta(&meta).unwrap();

    assert_eq!(
        default_wxa_plot_bounds(&meta, &geometry),
        LEGACY_CONUS_WXA_FRAME_BOUNDS
    );
}

#[test]
fn broad_legacy_nbm_wxa_bounds_frame_as_conus() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "nbm".to_string(),
        run: "20260604_nbm_00z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 2,
        ny: 2,
        forecast_hours: vec![6],
        chunk_y: 2,
        chunk_x: 2,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "regular_latlon",
            "lat_start": 53.79964447021485,
            "lat_step": -34.14250183105469,
            "lon_start": -136.36680603027344,
            "lon_step": 75.88330841064453,
            "bounds": [-136.36680603027344, 19.657142639160156, -60.483497619628906, 53.79964447021485]
        }),
    };
    let geometry = geometry_from_wxa_meta(&meta).unwrap();

    assert_eq!(
        default_wxa_plot_bounds(&meta, &geometry),
        LEGACY_CONUS_WXA_FRAME_BOUNDS
    );
}

#[test]
fn global_legacy_nbm_wxa_bounds_keep_global_frame() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "nbm".to_string(),
        run: "20260604_nbm_00z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 2,
        ny: 2,
        forecast_hours: vec![6],
        chunk_y: 2,
        chunk_x: 2,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "regular_latlon",
            "lat_start": 90.0,
            "lat_step": -180.0,
            "lon_start": -180.0,
            "lon_step": 359.999,
            "bounds": [-180.0, -90.0, 179.999, 90.0]
        }),
    };
    let geometry = geometry_from_wxa_meta(&meta).unwrap();

    assert_eq!(
        default_wxa_plot_bounds(&meta, &geometry),
        (-180.0, 179.999, -90.0, 90.0)
    );
}

#[test]
fn sampled_curvilinear_wxa_static_maps_use_raster_alpha_frame() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "rrfs-a".to_string(),
        run: "20260604_rrfs_a_00z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 3,
        ny: 3,
        forecast_hours: vec![0],
        chunk_y: 3,
        chunk_x: 3,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "curvilinear_latlon_sampled",
            "bounds": [-126.0, 22.0, -66.0, 52.0],
            "sample": {
                "nx": 3,
                "ny": 3,
                "x": [0, 1, 2],
                "y": [0, 1, 2],
                "lat": [
                    52.0, 50.0, 47.0,
                    38.0, 37.0, 34.0,
                    24.0, 23.0, 22.0
                ],
                "lon": [
                    -118.0, -93.0, -66.0,
                    -124.0, -96.0, -73.0,
                    -126.0, -101.0, -82.0
                ]
            }
        }),
    };
    let wxa = WxaDense2dGrid {
        meta,
        forecast_hour: 0,
        values: vec![70.0; 9],
    };
    let geometry = geometry_from_wxa_meta(&wxa.meta).unwrap();
    let request = build_wxa_map_request(
        &wxa,
        &geometry,
        (
            geometry.bounds[0],
            geometry.bounds[2],
            geometry.bounds[1],
            geometry.bounds[3],
        ),
        800,
        450,
        "2m AGL Temperature",
        Path::new("2m_temperature.wxa"),
        None,
    )
    .unwrap();

    assert_eq!(
        request.domain_frame.unwrap().source,
        DomainFrameSource::RasterAlpha
    );
}

#[test]
fn sampled_curvilinear_sparse_categorical_wxa_maps_use_viewport_frame() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "rrfs-a".to_string(),
        run: "20260604_rrfs_a_00z".to_string(),
        member: Some("control".to_string()),
        variable: "categorical_freezing_rain".to_string(),
        units: "category".to_string(),
        nx: 3,
        ny: 3,
        forecast_hours: vec![0],
        chunk_y: 3,
        chunk_x: 3,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "curvilinear_latlon_sampled",
            "bounds": [-126.0, 22.0, -66.0, 52.0],
            "sample": {
                "nx": 3,
                "ny": 3,
                "x": [0, 1, 2],
                "y": [0, 1, 2],
                "lat": [
                    52.0, 50.0, 47.0,
                    38.0, 37.0, 34.0,
                    24.0, 23.0, 22.0
                ],
                "lon": [
                    -118.0, -93.0, -66.0,
                    -124.0, -96.0, -73.0,
                    -126.0, -101.0, -82.0
                ]
            }
        }),
    };
    let wxa = WxaDense2dGrid {
        meta,
        forecast_hour: 0,
        values: vec![0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
    };
    let geometry = geometry_from_wxa_meta(&wxa.meta).unwrap();
    let request = build_wxa_map_request(
        &wxa,
        &geometry,
        (
            geometry.bounds[0],
            geometry.bounds[2],
            geometry.bounds[1],
            geometry.bounds[3],
        ),
        800,
        450,
        "Categorical Freezing Rain",
        Path::new("categorical_freezing_rain.wxa"),
        None,
    )
    .unwrap();

    assert_eq!(
        request.domain_frame.unwrap().source,
        DomainFrameSource::MapViewport
    );
}

#[test]
fn regular_wxa_static_maps_keep_projected_grid_frame() {
    let meta = WxaDense2dMeta {
        schema: "wxstore.wxa.dense2d.v1".to_string(),
        model: "gfs".to_string(),
        run: "20260506_gfs_18z".to_string(),
        member: Some("control".to_string()),
        variable: "2m_temperature".to_string(),
        units: "degF".to_string(),
        nx: 2,
        ny: 2,
        forecast_hours: vec![0],
        chunk_y: 2,
        chunk_x: 2,
        dtype: "f32_le".to_string(),
        codec: "zstd_level_1".to_string(),
        grid: serde_json::json!({
            "type": "regular_latlon",
            "lat_start": 40.0,
            "lat_step": -1.0,
            "lon_start": -101.0,
            "lon_step": 1.0,
            "bounds": [-101.0, 39.0, -100.0, 40.0]
        }),
    };
    let wxa = WxaDense2dGrid {
        meta,
        forecast_hour: 0,
        values: vec![70.0; 4],
    };
    let geometry = geometry_from_wxa_meta(&wxa.meta).unwrap();
    let request = build_wxa_map_request(
        &wxa,
        &geometry,
        (
            geometry.bounds[0],
            geometry.bounds[2],
            geometry.bounds[1],
            geometry.bounds[3],
        ),
        800,
        450,
        "2m AGL Temperature",
        Path::new("2m_temperature.wxa"),
        None,
    )
    .unwrap();

    assert_eq!(
        request.domain_frame.unwrap().source,
        DomainFrameSource::ProjectedGrid
    );
}

#[test]
fn run_time_subtitle_parses_wxstore_run() {
    let subtitle = subtitle_for_wxa_time(Some(ModelId::Gfs), "20260506_gfs_18z", 3).unwrap();
    assert!(subtitle.contains("Init 05/06 18Z"));
    assert!(subtitle.contains("F003"));
}

#[test]
fn wxa_model_id_parser_covers_extended_store_models() {
    for (raw, expected) in [
        ("nbm", ModelId::Nbm),
        ("rrfs-a", ModelId::RrfsA),
        ("rrfs_a", ModelId::RrfsA),
        ("refs", ModelId::Refs),
    ] {
        assert_eq!(model_id_from_wxa(raw), Some(expected));
    }
}

#[test]
fn run_time_subtitle_parses_rrfs_and_nbm_wxstore_runs() {
    let rrfs =
        subtitle_for_wxa_time(model_id_from_wxa("rrfs-a"), "20260604_rrfs_a_00z", 0).unwrap();
    assert!(rrfs.contains("Init 06/04 00Z"));
    assert!(rrfs.contains("F000"));
    assert!(rrfs.contains("RRFS-A"));

    let nbm = subtitle_for_wxa_time(model_id_from_wxa("nbm"), "20260604_nbm_00z", 6).unwrap();
    assert!(nbm.contains("Init 06/04 00Z"));
    assert!(nbm.contains("F006"));
    assert!(nbm.contains("NBM"));
}

#[test]
fn wxa_direct_styles_use_plot_semantics_and_dense_surface_scales() {
    let (scale, mode, tick_step) = plot_style_for_wxa_product("mslp_10m_winds", "kt");
    let ColorScale::Discrete(wind_scale) = scale else {
        panic!("expected mslp/10m winds to use a discrete 10m wind scale");
    };
    assert_eq!(mode, ProductVisualMode::FilledMeteorology);
    assert_eq!(tick_step, Some(5.0));
    assert_eq!(wind_scale.levels.first().copied(), Some(10.0));
    assert_eq!(wind_scale.levels.last().copied(), Some(60.0));
    assert_eq!(wind_scale.mask_below, Some(10.0));

    let (scale, _, tick_step) = plot_style_for_wxa_product("2m_temperature_10m_winds", "degF");
    let ColorScale::Discrete(temp_scale) = scale else {
        panic!("expected 2m temperature to use a discrete operational scale");
    };
    assert_eq!(tick_step, Some(10.0));
    assert_eq!(temp_scale.levels.first().copied(), Some(-60.0));
    assert_eq!(temp_scale.levels.get(1).copied(), Some(-59.0));
    assert_eq!(temp_scale.levels.last().copied(), Some(120.0));
}

#[test]
fn wxa_derived_styles_do_not_fall_back_to_generic_scale() {
    let (scale, mode, tick_step) = plot_style_for_wxa_product("ehi_0_1km", "dimensionless");
    assert!(matches!(scale, ColorScale::Weather(WeatherPreset::Ehi)));
    assert_eq!(mode, ProductVisualMode::SevereDiagnostic);
    assert_eq!(tick_step, Some(1.0));

    let (scale, mode, tick_step) = plot_style_for_wxa_product("lapse_rate_700_500", "degC/km");
    let ColorScale::Discrete(lapse_scale) = scale else {
        panic!("expected lapse rate preset scale");
    };
    assert_eq!(mode, ProductVisualMode::SevereDiagnostic);
    assert_eq!(tick_step, Some(1.0));
    assert_eq!(lapse_scale.levels.first().copied(), Some(2.0));

    let (scale, mode, tick_step) = plot_style_for_wxa_product("fire_weather_composite", "index");
    let ColorScale::Discrete(fire_scale) = scale else {
        panic!("expected fire weather composite custom scale");
    };
    assert_eq!(mode, ProductVisualMode::SevereDiagnostic);
    assert_eq!(tick_step, Some(20.0));
    assert_eq!(fire_scale.levels.first().copied(), Some(0.0));
    assert_eq!(
        fire_scale.colors.first().copied(),
        Some(Color::rgba(250, 250, 247, 255))
    );
}

#[test]
fn wxa_component_products_are_hidden_from_showcase_selection() {
    assert!(is_wxa_component_product("mslp_10m_winds__contour"));
    assert!(is_wxa_component_product(
        "500mb_temperature_height_winds__wind_u"
    ));
    assert!(is_wxa_component_product(
        "cloud_cover_levels__low_cloud_cover"
    ));
    assert!(is_wxa_component_product(
        "precipitation_type__categorical_snow"
    ));
    assert!(!is_wxa_component_product("mslp_10m_winds"));
    assert!(!is_wxa_component_product("cloud_cover_levels"));
}

#[test]
fn wxa_composite_panel_component_products_match_direct_component_storage() {
    assert_eq!(
        wxa_composite_panel_component_products("cloud_cover_levels").unwrap(),
        vec![
            "cloud_cover_levels__low_cloud_cover".to_string(),
            "cloud_cover_levels__middle_cloud_cover".to_string(),
            "cloud_cover_levels__high_cloud_cover".to_string(),
        ]
    );
    assert!(wxa_composite_panel_component_products("2m_temperature").is_none());
}
