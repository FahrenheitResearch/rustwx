use super::*;
use rustwx_render::PngCompressionMode;

#[test]
fn skip_ecape_filters_heavy_derived_recipes_in_maps() {
    let filtered = filter_heavy_derived_recipes(
        vec![
            "sbcape".to_string(),
            "sbecape".to_string(),
            "stp_fixed".to_string(),
        ],
        true,
    );
    assert_eq!(
        filtered,
        vec!["sbcape".to_string(), "stp_fixed".to_string()]
    );
}

#[test]
fn non_hrrr_build_jobs_collapse_direct_and_derived_into_unified_lane() {
    let mut direct_recipes = BTreeMap::new();
    direct_recipes.insert(ModelId::Gfs, vec!["composite_reflectivity".to_string()]);
    let mut derived_recipes = BTreeMap::new();
    derived_recipes.insert(ModelId::Gfs, vec!["sbcape".to_string()]);
    let config = RunnerConfig {
        date_yyyymmdd: "20260414".to_string(),
        cycle_override_utc: Some(12),
        source_override: Some(SourceId::Nomads),
        out_dir: PathBuf::from("out"),
        cache_dir: PathBuf::from("cache"),
        use_cache: false,
        source_mode: ProductSourceMode::Canonical,
        png_compression: PngCompressionMode::Default,
        output_width: 1200,
        output_height: 900,
        skip_severe: false,
        skip_ecape: false,
        skip_direct: false,
        skip_derived: false,
        direct_recipes,
        derived_recipes,
        surface_product_override: None,
        pressure_product_override: None,
        direct_product_overrides: HashMap::new(),
        token_budget: TokenBudget {
            light: 1,
            warm: 1,
            heavy: 1,
        },
        failure_cooldown_seconds: 900,
    };

    let jobs = build_jobs(&config, &[ModelId::Gfs], &[RegionPreset::Conus], &[0]);
    assert_eq!(jobs.len(), 3);
    assert!(
        jobs.iter()
            .any(|job| job.key.lane == ProductionLane::NonHrrrNonEcape)
    );
    assert!(!jobs.iter().any(|job| matches!(
        job.key.lane,
        ProductionLane::Direct | ProductionLane::Derived
    )));
}
