use super::*;
use rustwx_core::{GridShape, LatLonGrid, ProductKey};

fn sample_field() -> Field2D {
    let grid = LatLonGrid::new(
        GridShape::new(4, 3).unwrap(),
        vec![
            20.0, 20.0, 20.0, 20.0, 30.0, 30.0, 30.0, 30.0, 40.0, 40.0, 40.0, 40.0,
        ],
        vec![
            -130.0, -120.0, -110.0, -100.0, -130.0, -120.0, -110.0, -100.0, -130.0, -120.0, -110.0,
            -100.0,
        ],
    )
    .unwrap();
    Field2D::new(
        ProductKey::named("sample"),
        "unit",
        grid,
        (0..12).map(|value| value as f32).collect(),
    )
    .unwrap()
}

#[test]
fn crop_field_to_domain_uses_exclusive_crop_bounds() {
    let cropped = crop_field_to_domain(&sample_field(), (-121.0, -109.0, 25.0, 41.0)).unwrap();
    assert_eq!(cropped.nx, 2);
    assert_eq!(cropped.ny, 2);
    assert_eq!(
        cropped.crop,
        Some(WxStoreGridExportCrop {
            x_start: 1,
            x_end: 3,
            y_start: 1,
            y_end: 3
        })
    );
    assert_eq!(cropped.values, vec![5.0, 6.0, 9.0, 10.0]);
}

#[test]
fn valid_time_uses_cycle_plus_forecast_hour() {
    let latest = LatestRun {
        model: ModelId::Gfs,
        cycle: rustwx_core::CycleSpec::new("20260430", 18).unwrap(),
        source: SourceId::Aws,
    };
    assert_eq!(valid_time_utc(&latest, 9).unwrap(), "2026-05-01T03:00:00Z");
}

#[test]
fn default_products_include_all_supported_non_ecape_grid_lanes() {
    let products = default_wxstore_export_product_slugs(ModelId::Hrrr);
    assert!(products.contains(&"2m_temperature".to_string()));
    assert!(products.contains(&"mlcape".to_string()));
    assert!(products.contains(&"qpf_1h".to_string()));
    assert!(products.contains(&"total_qpf".to_string()));
    assert!(products.contains(&"scp_mu_0_3km_0_6km_proxy".to_string()));
    assert!(!products.contains(&"sbecape".to_string()));
    assert!(!products.contains(&"severe_proof_panel".to_string()));
    assert!(!products.contains(&"cloud_cover_levels".to_string()));
    assert!(products.iter().all(|slug| !slug.contains("ecape")));
}

#[test]
fn classification_routes_aliases_and_blocks_ecape() {
    let products = classify_wxstore_export_products(
        ModelId::Hrrr,
        &[
            "1h_qpf".to_string(),
            "2m_theta_e_10m_winds".to_string(),
            "sbecape".to_string(),
        ],
    );
    assert!(products.direct_slugs.is_empty());
    assert_eq!(products.derived_slugs, vec!["theta_e_2m_10m_winds"]);
    assert_eq!(
        products
            .windowed_products
            .iter()
            .map(|product| product.slug())
            .collect::<Vec<_>>(),
        vec!["qpf_1h"]
    );
    assert!(
        products
            .blockers
            .iter()
            .any(|blocker| blocker.product_slug == "sbecape")
    );
}

#[test]
fn hour_range_slug_stays_short_for_full_runs() {
    let hrrr = (0..=48).collect::<Vec<_>>();
    assert_eq!(hour_range_slug(&hrrr), "f000_f048");

    let mut gefs = (0..=240).step_by(3).collect::<Vec<_>>();
    gefs.extend((246..=384).step_by(6));
    assert_eq!(hour_range_slug(&gefs), "f000_f384_n105");

    let mut gfs = (0..=120).collect::<Vec<_>>();
    gfs.extend((123..=384).step_by(3));
    assert_eq!(hour_range_slug(&gfs), "f000_f384_n209");
}
