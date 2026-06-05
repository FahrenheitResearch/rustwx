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
