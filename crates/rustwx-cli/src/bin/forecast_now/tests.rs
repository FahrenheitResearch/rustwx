use super::{
    Args, PinResolution, PinnedRunRequest, RoutePolicyArg, RouteSelection,
    default_windowed_products, filter_heavy_derived_recipes, forecast_now_required_products,
    parse_windowed_products, select_non_hrrr_non_ecape_route, supports_unified_non_hrrr_non_ecape,
};
use clap::Parser;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::windowed::HrrrWindowedProduct;

#[test]
fn pinned_request_uses_resolved_cycle_date() {
    let pinned = PinnedRunRequest {
        date_yyyymmdd: "20260417".to_string(),
        cycle_override_utc: Some(12),
        source: SourceId::Aws,
        resolution: PinResolution::AutoLatest,
    };
    assert_eq!(pinned.date_yyyymmdd, "20260417");
    assert_eq!(pinned.cycle_override_utc, Some(12));
}

#[test]
fn skip_ecape_filters_heavy_derived_recipes() {
    let recipes = vec![
        "sbcape".to_string(),
        "sbecape".to_string(),
        "stp_fixed".to_string(),
    ];
    let filtered = filter_heavy_derived_recipes(recipes, true);
    assert_eq!(
        filtered,
        vec!["sbcape".to_string(), "stp_fixed".to_string()]
    );
}

#[test]
fn default_windowed_products_are_short_non_vpd_operational_products() {
    let products = default_windowed_products();

    assert!(products.contains(&HrrrWindowedProduct::Qpf1h));
    assert!(products.contains(&HrrrWindowedProduct::QpfTotal));
    assert!(products.contains(&HrrrWindowedProduct::Uh25km1h));
    assert!(products.contains(&HrrrWindowedProduct::Wind10mRunMax));
    assert!(
        products
            .iter()
            .all(|product| !product.slug().contains("_vpd_"))
    );
}

#[test]
fn windowed_product_parser_supports_presets_and_slugs() {
    assert_eq!(
        parse_windowed_products(None).unwrap(),
        default_windowed_products()
    );
    assert_eq!(
        parse_windowed_products(Some(&["qpf-1h".to_string()])).unwrap(),
        vec![HrrrWindowedProduct::Qpf1h]
    );
    assert!(
        parse_windowed_products(Some(&["all-normal".to_string()]))
            .unwrap()
            .iter()
            .all(|product| !product.slug().contains("_vpd_"))
    );
}

#[test]
fn auto_route_uses_unified_non_hrrr_path() {
    assert_eq!(
        select_non_hrrr_non_ecape_route(ModelId::Gfs, RoutePolicyArg::Auto),
        RouteSelection::Unified
    );
    assert_eq!(
        select_non_hrrr_non_ecape_route(ModelId::EcmwfOpenData, RoutePolicyArg::Auto),
        RouteSelection::Unified
    );
    assert_eq!(
        select_non_hrrr_non_ecape_route(ModelId::WrfGdex, RoutePolicyArg::Auto),
        RouteSelection::Unified
    );
}

#[test]
fn wrf_gdex_supports_unified_non_ecape_runner() {
    assert!(supports_unified_non_hrrr_non_ecape(ModelId::WrfGdex));
    assert!(!supports_unified_non_hrrr_non_ecape(ModelId::Hrrr));
}

#[test]
fn refs_direct_recipes_probe_their_required_products() {
    let prob_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "refs_prob_2m_temperature_below_273p15k",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Refs, &prob_args),
        vec!["prob-conus".to_string()]
    );

    let spread_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "refs_sprd_2m_temperature",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Refs, &spread_args),
        vec!["sprd-conus".to_string()]
    );

    let both_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "refs_prob_2m_temperature_below_273p15k,refs_sprd_2m_temperature",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Refs, &both_args),
        vec!["prob-conus".to_string(), "sprd-conus".to_string()]
    );
}

#[test]
fn href_direct_recipes_probe_their_required_products() {
    let mean_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "href_mean_2m_temperature",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Href, &mean_args),
        vec!["ensprod/conus/mean".to_string()]
    );

    let prob_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "href_prob_2m_temperature_below_273p15k",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Href, &prob_args),
        vec!["ensprod/conus/prob".to_string()]
    );

    let spread_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "href_sprd_2m_temperature",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Href, &spread_args),
        vec!["ensprod/conus/sprd".to_string()]
    );

    let both_args = Args::parse_from([
        "forecast-now",
        "--out-dir",
        "out",
        "--cache-dir",
        "cache",
        "--direct-recipes",
        "href_mean_2m_temperature,href_prob_2m_temperature_below_273p15k,href_sprd_2m_temperature",
    ]);
    assert_eq!(
        forecast_now_required_products(ModelId::Href, &both_args),
        vec![
            "ensprod/conus/mean".to_string(),
            "ensprod/conus/prob".to_string(),
            "ensprod/conus/sprd".to_string()
        ]
    );
}
