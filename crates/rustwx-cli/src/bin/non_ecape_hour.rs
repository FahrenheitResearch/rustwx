use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_io::earth2_archive::Earth2EnsembleSelector;
use rustwx_models::model_summary;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::catalog::{ProductTargetStatus, build_supported_products_catalog};
use rustwx_products::derived::is_heavy_derived_recipe_slug;
use rustwx_products::non_ecape::{
    NonEcapeHourRequest, NonEcapeMultiDomainRequest, run_model_non_ecape_hour,
    run_model_non_ecape_hour_build, run_model_non_ecape_hour_multi_domain,
    run_model_non_ecape_hour_wxstore_only,
};
use rustwx_products::places::{PlaceLabelDensityTier, default_place_label_overlay_for_domain};
use rustwx_products::publication::{
    atomic_write_json, canonical_run_slug, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use rustwx_products::windowed::{HrrrWindowedProduct, windowed_product_available_at_forecast_hour};
use rustwx_products::wxstore_export::{
    WxStoreGridExportRequest, default_wxstore_export_product_slugs,
};
use rustwx_render::{PngCompressionMode, ProductMaturity, ProductSemanticFlag};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum WindowedProductArg {
    Qpf1h,
    Qpf6h,
    Qpf12h,
    Qpf24h,
    QpfTotal,
    Uh25km1h,
    Uh25km3h,
    Uh25kmRunMax,
    Wind10m1hMax,
    Wind10mRunMax,
    Wind10m0to24hMax,
    Wind10m24to48hMax,
    Wind10m0to48hMax,
    Temp2m0to24hMax,
    Temp2m24to48hMax,
    Temp2m0to48hMax,
    Temp2m0to24hMin,
    Temp2m24to48hMin,
    Temp2m0to48hMin,
    Temp2m0to24hRange,
    Temp2m24to48hRange,
    Temp2m0to48hRange,
    Rh2m0to24hMax,
    Rh2m24to48hMax,
    Rh2m0to48hMax,
    Rh2m0to24hMin,
    Rh2m24to48hMin,
    Rh2m0to48hMin,
    Rh2m0to24hRange,
    Rh2m24to48hRange,
    Rh2m0to48hRange,
    Dewpoint2m0to24hMax,
    Dewpoint2m24to48hMax,
    Dewpoint2m0to48hMax,
    Dewpoint2m0to24hMin,
    Dewpoint2m24to48hMin,
    Dewpoint2m0to48hMin,
    Dewpoint2m0to24hRange,
    Dewpoint2m24to48hRange,
    Dewpoint2m0to48hRange,
    Vpd2m0to24hMax,
    Vpd2m24to48hMax,
    Vpd2m0to48hMax,
    Vpd2m0to24hMin,
    Vpd2m24to48hMin,
    Vpd2m0to48hMin,
    Vpd2m0to24hRange,
    Vpd2m24to48hRange,
    Vpd2m0to48hRange,
}

impl From<WindowedProductArg> for HrrrWindowedProduct {
    fn from(value: WindowedProductArg) -> Self {
        match value {
            WindowedProductArg::Qpf1h => HrrrWindowedProduct::Qpf1h,
            WindowedProductArg::Qpf6h => HrrrWindowedProduct::Qpf6h,
            WindowedProductArg::Qpf12h => HrrrWindowedProduct::Qpf12h,
            WindowedProductArg::Qpf24h => HrrrWindowedProduct::Qpf24h,
            WindowedProductArg::QpfTotal => HrrrWindowedProduct::QpfTotal,
            WindowedProductArg::Uh25km1h => HrrrWindowedProduct::Uh25km1h,
            WindowedProductArg::Uh25km3h => HrrrWindowedProduct::Uh25km3h,
            WindowedProductArg::Uh25kmRunMax => HrrrWindowedProduct::Uh25kmRunMax,
            WindowedProductArg::Wind10m1hMax => HrrrWindowedProduct::Wind10m1hMax,
            WindowedProductArg::Wind10mRunMax => HrrrWindowedProduct::Wind10mRunMax,
            WindowedProductArg::Wind10m0to24hMax => HrrrWindowedProduct::Wind10m0to24hMax,
            WindowedProductArg::Wind10m24to48hMax => HrrrWindowedProduct::Wind10m24to48hMax,
            WindowedProductArg::Wind10m0to48hMax => HrrrWindowedProduct::Wind10m0to48hMax,
            WindowedProductArg::Temp2m0to24hMax => HrrrWindowedProduct::Temp2m0to24hMax,
            WindowedProductArg::Temp2m24to48hMax => HrrrWindowedProduct::Temp2m24to48hMax,
            WindowedProductArg::Temp2m0to48hMax => HrrrWindowedProduct::Temp2m0to48hMax,
            WindowedProductArg::Temp2m0to24hMin => HrrrWindowedProduct::Temp2m0to24hMin,
            WindowedProductArg::Temp2m24to48hMin => HrrrWindowedProduct::Temp2m24to48hMin,
            WindowedProductArg::Temp2m0to48hMin => HrrrWindowedProduct::Temp2m0to48hMin,
            WindowedProductArg::Temp2m0to24hRange => HrrrWindowedProduct::Temp2m0to24hRange,
            WindowedProductArg::Temp2m24to48hRange => HrrrWindowedProduct::Temp2m24to48hRange,
            WindowedProductArg::Temp2m0to48hRange => HrrrWindowedProduct::Temp2m0to48hRange,
            WindowedProductArg::Rh2m0to24hMax => HrrrWindowedProduct::Rh2m0to24hMax,
            WindowedProductArg::Rh2m24to48hMax => HrrrWindowedProduct::Rh2m24to48hMax,
            WindowedProductArg::Rh2m0to48hMax => HrrrWindowedProduct::Rh2m0to48hMax,
            WindowedProductArg::Rh2m0to24hMin => HrrrWindowedProduct::Rh2m0to24hMin,
            WindowedProductArg::Rh2m24to48hMin => HrrrWindowedProduct::Rh2m24to48hMin,
            WindowedProductArg::Rh2m0to48hMin => HrrrWindowedProduct::Rh2m0to48hMin,
            WindowedProductArg::Rh2m0to24hRange => HrrrWindowedProduct::Rh2m0to24hRange,
            WindowedProductArg::Rh2m24to48hRange => HrrrWindowedProduct::Rh2m24to48hRange,
            WindowedProductArg::Rh2m0to48hRange => HrrrWindowedProduct::Rh2m0to48hRange,
            WindowedProductArg::Dewpoint2m0to24hMax => HrrrWindowedProduct::Dewpoint2m0to24hMax,
            WindowedProductArg::Dewpoint2m24to48hMax => HrrrWindowedProduct::Dewpoint2m24to48hMax,
            WindowedProductArg::Dewpoint2m0to48hMax => HrrrWindowedProduct::Dewpoint2m0to48hMax,
            WindowedProductArg::Dewpoint2m0to24hMin => HrrrWindowedProduct::Dewpoint2m0to24hMin,
            WindowedProductArg::Dewpoint2m24to48hMin => HrrrWindowedProduct::Dewpoint2m24to48hMin,
            WindowedProductArg::Dewpoint2m0to48hMin => HrrrWindowedProduct::Dewpoint2m0to48hMin,
            WindowedProductArg::Dewpoint2m0to24hRange => HrrrWindowedProduct::Dewpoint2m0to24hRange,
            WindowedProductArg::Dewpoint2m24to48hRange => {
                HrrrWindowedProduct::Dewpoint2m24to48hRange
            }
            WindowedProductArg::Dewpoint2m0to48hRange => HrrrWindowedProduct::Dewpoint2m0to48hRange,
            WindowedProductArg::Vpd2m0to24hMax => HrrrWindowedProduct::Vpd2m0to24hMax,
            WindowedProductArg::Vpd2m24to48hMax => HrrrWindowedProduct::Vpd2m24to48hMax,
            WindowedProductArg::Vpd2m0to48hMax => HrrrWindowedProduct::Vpd2m0to48hMax,
            WindowedProductArg::Vpd2m0to24hMin => HrrrWindowedProduct::Vpd2m0to24hMin,
            WindowedProductArg::Vpd2m24to48hMin => HrrrWindowedProduct::Vpd2m24to48hMin,
            WindowedProductArg::Vpd2m0to48hMin => HrrrWindowedProduct::Vpd2m0to48hMin,
            WindowedProductArg::Vpd2m0to24hRange => HrrrWindowedProduct::Vpd2m0to24hRange,
            WindowedProductArg::Vpd2m24to48hRange => HrrrWindowedProduct::Vpd2m24to48hRange,
            WindowedProductArg::Vpd2m0to48hRange => HrrrWindowedProduct::Vpd2m0to48hRange,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceModeArg {
    Canonical,
    Fastest,
}

impl From<SourceModeArg> for ProductSourceMode {
    fn from(value: SourceModeArg) -> Self {
        match value {
            SourceModeArg::Canonical => Self::Canonical,
            SourceModeArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DomainSetArg {
    Single,
    GlobalModel,
    HrrrRapRegions,
    UsRegions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
}

impl From<PngCompressionArg> for PngCompressionMode {
    fn from(value: PngCompressionArg) -> Self {
        match value {
            PngCompressionArg::Default => Self::Default,
            PngCompressionArg::Fast => Self::Fast,
            PngCompressionArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "non-ecape-hour",
    about = "Generate one unified all-model non-ECAPE hour pass across direct, derived, and supported windowed products"
)]
struct Args {
    #[arg(long, default_value = "gfs")]
    model: ModelId,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Conus)]
    region: RegionPreset,
    #[arg(
        long = "regions",
        value_enum,
        value_delimiter = ',',
        num_args = 1..,
        help = "Explicit multi-domain region list, e.g. --regions california,pacific-northwest,midwest,northeast"
    )]
    regions: Vec<RegionPreset>,
    #[arg(long, value_enum, default_value_t = DomainSetArg::Single)]
    domain_set: DomainSetArg,
    #[arg(long, default_value_t = 1)]
    domain_jobs: usize,
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(long, default_value_t = false)]
    skip_derived: bool,
    #[arg(long, default_value_t = false)]
    skip_windowed: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Run only direct recipe products; disables derived and windowed products"
    )]
    direct_only: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Use every supported non-ECAPE direct/derived recipe for this model instead of the operational default set"
    )]
    all_supported: bool,
    #[arg(long = "product-override", value_delimiter = ',', num_args = 0..)]
    product_overrides: Vec<String>,
    #[arg(long)]
    surface_product: Option<String>,
    #[arg(long)]
    pressure_product: Option<String>,
    #[arg(
        long = "windowed-product",
        value_enum,
        value_delimiter = ',',
        num_args = 1..,
        help = "Windowed products are currently HRRR-only; other models will report blockers"
    )]
    windowed_products: Vec<WindowedProductArg>,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(
        long,
        help = "Also export the same hour as a WxStore grid manifest from the prepared full-GRIB bundle"
    )]
    wxstore_out_dir: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        help = "WxStore export region. Defaults to CONUS for HRRR/RAP and global for global models."
    )]
    wxstore_region: Option<RegionPreset>,
    #[arg(long = "wxstore-product", value_delimiter = ',', num_args = 0..)]
    wxstore_products: Vec<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "When exporting WxStore grids, skip static PNG rendering and write only the WxStore grid manifest"
    )]
    skip_static_plots: bool,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = false)]
    allow_large_heavy_domain: bool,
    #[arg(long = "source-mode", alias = "thermo-path", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,
    #[arg(long = "place-label-density", default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=3))]
    place_label_density: u8,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
    #[arg(
        long,
        help = "AIFS Earth2Archive/aifs-inference member index to render"
    )]
    member: Option<u16>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_domain_slug = if args.regions.is_empty() {
        match args.domain_set {
            DomainSetArg::Single => args.region.slug().to_string(),
            _ => domain_set_slug(args.domain_set).to_string(),
        }
    } else {
        region_list_slug(&args.regions)
    };
    let failure_slug = canonical_run_slug(
        &args.model.as_str().replace('-', "_"),
        &args.date,
        args.cycle,
        args.forecast_hour,
        &failure_domain_slug,
        "non_ecape_hour",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "non_ecape_hour",
            &failure_slug,
            &failure_out_dir,
            &failure_slug,
            err.to_string(),
        );
        return Err(err);
    }
    Ok(())
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let source = args
        .source
        .unwrap_or(model_summary(args.model).sources[0].id);
    let earth2_ensemble = args.member.map(Earth2EnsembleSelector::Member);
    if earth2_ensemble.is_some() && args.model != ModelId::Aifs {
        return Err("--member currently applies only to --model aifs".into());
    }
    let png_compression: PngCompressionMode = args.png_compression.into();
    let (default_direct, default_derived, default_windowed) = if args.all_supported {
        all_supported_non_ecape_product_sets(args.model, args.forecast_hour)
    } else {
        default_non_ecape_product_sets(args.model, args.forecast_hour)
    };
    let direct_recipe_slugs = if args.direct_recipes.is_empty() {
        default_direct
    } else if recipe_list_disabled(&args.direct_recipes) {
        Vec::new()
    } else {
        args.direct_recipes.clone()
    };
    let derived_recipe_slugs =
        if args.direct_only || args.skip_derived || recipe_list_disabled(&args.derived_recipes) {
            Vec::new()
        } else if args.derived_recipes.is_empty() {
            default_derived
        } else {
            args.derived_recipes.clone()
        };
    let domains = domains_for_request(args.model, args.region, &args.regions, args.domain_set)?;
    let domain = domains
        .first()
        .cloned()
        .ok_or("domain selection produced zero domains")?;
    let direct_product_overrides = build_direct_product_overrides(args)?;
    let windowed_products = if args.direct_only || args.skip_windowed {
        Vec::new()
    } else if args.windowed_products.is_empty() {
        default_windowed
    } else {
        args.windowed_products
            .iter()
            .copied()
            .map(Into::into)
            .collect()
    };
    let wxstore_request = wxstore_request_for_args(args, &cache_root, source)?;
    let (output_width, output_height) = static_output_size_for_model(args.model);

    if wxstore_request.is_some() {
        let build_domain_slug = selected_domains_slug(args, &domains);
        let request = NonEcapeMultiDomainRequest {
            model: args.model,
            date_yyyymmdd: args.date.clone(),
            cycle_override_utc: args.cycle,
            forecast_hour: args.forecast_hour,
            source,
            domains,
            out_dir: args.out_dir.clone(),
            cache_root: cache_root.clone(),
            use_cache: !args.no_cache,
            source_mode: args.source_mode.into(),
            direct_recipe_slugs,
            derived_recipe_slugs,
            direct_product_overrides,
            surface_product_override: args.surface_product.clone(),
            pressure_product_override: args.pressure_product.clone(),
            allow_large_heavy_domain: args.allow_large_heavy_domain,
            windowed_products,
            output_width,
            output_height,
            png_compression,
            custom_poi_overlay: None,
            place_label_overlay: default_place_label_overlay_for_domain(
                &domain,
                PlaceLabelDensityTier::from_numeric(args.place_label_density),
            ),
            earth2_ensemble,
            domain_jobs: Some(args.domain_jobs),
        };
        let report = if args.skip_static_plots {
            let wxstore_request = wxstore_request
                .as_ref()
                .ok_or("--skip-static-plots requires --wxstore-out-dir")?;
            run_model_non_ecape_hour_wxstore_only(&request, wxstore_request)?
        } else {
            run_model_non_ecape_hour_build(&request, wxstore_request.as_ref())?
        };
        let model_slug = report.static_report.model.as_str().replace('-', "_");
        let report_path = args.out_dir.join(format!(
            "rustwx_{}_{}_{}z_f{:03}_{}_non_ecape_hour_build_report.json",
            model_slug,
            report.static_report.date_yyyymmdd,
            report.static_report.cycle_utc,
            report.static_report.forecast_hour,
            build_domain_slug
        ));
        atomic_write_json(&report_path, &report)?;
        print_multi_report(&report.static_report);
        if let Some(wxstore_report) = &report.wxstore_report {
            println!("{}", wxstore_report.manifest_path.display());
        }
        println!("{}", report_path.display());
        return Ok(());
    }

    if domains.len() == 1 {
        let request = NonEcapeHourRequest {
            model: args.model,
            date_yyyymmdd: args.date.clone(),
            cycle_override_utc: args.cycle,
            forecast_hour: args.forecast_hour,
            source,
            domain: domain.clone(),
            out_dir: args.out_dir.clone(),
            cache_root,
            use_cache: !args.no_cache,
            source_mode: args.source_mode.into(),
            direct_recipe_slugs,
            derived_recipe_slugs,
            direct_product_overrides,
            surface_product_override: args.surface_product.clone(),
            pressure_product_override: args.pressure_product.clone(),
            allow_large_heavy_domain: args.allow_large_heavy_domain,
            windowed_products,
            output_width,
            output_height,
            png_compression,
            custom_poi_overlay: None,
            place_label_overlay: default_place_label_overlay_for_domain(
                &domain,
                PlaceLabelDensityTier::from_numeric(args.place_label_density),
            ),
            earth2_ensemble,
        };
        let report = run_model_non_ecape_hour(&request)?;
        let model_slug = report.model.as_str().replace('-', "_");
        let report_path = args.out_dir.join(format!(
            "rustwx_{}_{}_{}z_f{:03}_{}_non_ecape_hour_report.json",
            model_slug,
            report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            report.domain.slug
        ));
        atomic_write_json(&report_path, &report)?;
        print_single_report(&report, &report_path);
    } else {
        let request = NonEcapeMultiDomainRequest {
            model: args.model,
            date_yyyymmdd: args.date.clone(),
            cycle_override_utc: args.cycle,
            forecast_hour: args.forecast_hour,
            source,
            domains,
            out_dir: args.out_dir.clone(),
            cache_root,
            use_cache: !args.no_cache,
            source_mode: args.source_mode.into(),
            direct_recipe_slugs,
            derived_recipe_slugs,
            direct_product_overrides,
            surface_product_override: args.surface_product.clone(),
            pressure_product_override: args.pressure_product.clone(),
            allow_large_heavy_domain: args.allow_large_heavy_domain,
            windowed_products,
            output_width,
            output_height,
            png_compression,
            custom_poi_overlay: None,
            place_label_overlay: default_place_label_overlay_for_domain(
                &domain,
                PlaceLabelDensityTier::from_numeric(args.place_label_density),
            ),
            earth2_ensemble,
            domain_jobs: Some(args.domain_jobs),
        };
        let report = run_model_non_ecape_hour_multi_domain(&request)?;
        let model_slug = report.model.as_str().replace('-', "_");
        let report_path = args.out_dir.join(format!(
            "rustwx_{}_{}_{}z_f{:03}_{}_multi_domain_non_ecape_hour_report.json",
            model_slug,
            report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            selected_domains_slug(
                args,
                &report
                    .domains
                    .iter()
                    .map(|domain| domain.domain.clone())
                    .collect::<Vec<_>>()
            )
        ));
        atomic_write_json(&report_path, &report)?;
        print_multi_report(&report);
        println!("{}", report_path.display());
    }
    Ok(())
}

fn recipe_list_disabled(values: &[String]) -> bool {
    values.len() == 1
        && matches!(
            values[0].trim().to_ascii_lowercase().as_str(),
            "none" | "off" | "false" | "skip" | "disabled" | "empty"
        )
}

fn wxstore_request_for_args(
    args: &Args,
    cache_root: &PathBuf,
    source: SourceId,
) -> Result<Option<WxStoreGridExportRequest>, Box<dyn std::error::Error>> {
    let Some(out_dir) = args.wxstore_out_dir.clone() else {
        return Ok(None);
    };
    let product_slugs = if args.wxstore_products.is_empty() {
        default_wxstore_export_product_slugs(args.model)
    } else {
        args.wxstore_products.clone()
    };
    if product_slugs.is_empty() {
        return Err(format!(
            "no WxStore grid products are supported for model {} after filtering",
            args.model.as_str()
        )
        .into());
    }
    let wxstore_region = args.wxstore_region.unwrap_or_else(|| {
        if matches!(args.model, ModelId::Hrrr | ModelId::Rap) {
            RegionPreset::Conus
        } else {
            RegionPreset::Global
        }
    });
    Ok(Some(WxStoreGridExportRequest {
        model: args.model,
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hours: vec![args.forecast_hour],
        source,
        domain: domain(wxstore_region),
        product_slugs,
        out_dir,
        cache_root: cache_root.clone(),
        use_cache: !args.no_cache,
        direct_wxa_root: None,
        publish_wxa_latest: false,
    }))
}

fn print_multi_report(report: &rustwx_products::non_ecape::NonEcapeMultiDomainReport) {
    for domain_report in &report.domains {
        for output_path in &domain_report.summary.output_paths {
            println!("{}", output_path.display());
        }
        println!("{}", domain_report.publication_manifest_path.display());
        if let Some(attempt_path) = &domain_report.attempt_manifest_path {
            println!("{}", attempt_path.display());
        }
    }
}

fn print_single_report(
    report: &rustwx_products::non_ecape::NonEcapeHourReport,
    report_path: &PathBuf,
) {
    for output_path in &report.summary.output_paths {
        println!("{}", output_path.display());
    }
    if let Some(windowed) = &report.windowed {
        if !windowed.blockers.is_empty() {
            eprintln!("blocked windowed products:");
            for blocker in &windowed.blockers {
                eprintln!("  {}: {}", blocker.product.slug(), blocker.reason);
            }
        }
    }
    if let Some(derived) = &report.derived {
        if !derived.blockers.is_empty() {
            eprintln!("blocked derived products:");
            for blocker in &derived.blockers {
                eprintln!(
                    "  {} [{}]: {}",
                    blocker.recipe_slug,
                    blocker.source_route.as_str(),
                    blocker.reason
                );
            }
        }
    }
    println!("{}", report.publication_manifest_path.display());
    if let Some(attempt_path) = &report.attempt_manifest_path {
        println!("{}", attempt_path.display());
    }
    println!("{}", report_path.display());
}

fn domain_set_slug(domain_set: DomainSetArg) -> &'static str {
    match domain_set {
        DomainSetArg::Single => "single",
        DomainSetArg::GlobalModel => "global_model",
        DomainSetArg::HrrrRapRegions => "hrrr_rap_regions",
        DomainSetArg::UsRegions => "us_regions",
    }
}

fn region_list_slug(regions: &[RegionPreset]) -> String {
    regions
        .iter()
        .map(|region| region.slug())
        .collect::<Vec<_>>()
        .join("_")
}

fn selected_domains_slug(args: &Args, domains: &[DomainSpec]) -> String {
    if !args.regions.is_empty() {
        region_list_slug(&args.regions)
    } else if domains.len() == 1 {
        domains
            .first()
            .map(|domain| domain.slug.clone())
            .unwrap_or_else(|| "empty".to_string())
    } else {
        domain_set_slug(args.domain_set).to_string()
    }
}

fn static_output_dimension(name: &str, fallback: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value >= 320)
        .unwrap_or(fallback)
}

fn static_output_size_for_model(model: ModelId) -> (u32, u32) {
    let (default_width, default_height) = match model {
        ModelId::Hrrr | ModelId::Rap => (1200, 900),
        _ => (1600, 900),
    };
    (
        static_output_dimension("RUSTWX_STATIC_OUTPUT_WIDTH", default_width),
        static_output_dimension("RUSTWX_STATIC_OUTPUT_HEIGHT", default_height),
    )
}

fn domain(region: RegionPreset) -> DomainSpec {
    DomainSpec::new(region.slug(), region.bounds())
}

fn supports_regional_domain_set(model: ModelId) -> bool {
    matches!(
        model,
        ModelId::Hrrr
            | ModelId::HrrrAk
            | ModelId::Rap
            | ModelId::RrfsA
            | ModelId::RrfsPublic
            | ModelId::RrfsFireWx
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Nbm
    )
}

fn domains_for_request(
    model: ModelId,
    region: RegionPreset,
    regions: &[RegionPreset],
    domain_set: DomainSetArg,
) -> Result<Vec<DomainSpec>, Box<dyn std::error::Error>> {
    if !regions.is_empty() {
        if !matches!(domain_set, DomainSetArg::Single) {
            return Err("--regions cannot be combined with --domain-set other than single".into());
        }
        return Ok(regions.iter().copied().map(domain).collect());
    }

    let regional_model = supports_regional_domain_set(model);
    if regional_model && matches!(domain_set, DomainSetArg::GlobalModel) {
        return Err(format!(
            "{} should use --domain-set hrrr-rap-regions or --domain-set us-regions, not global-model",
            model.as_str()
        )
        .into());
    }
    if !regional_model
        && matches!(
            domain_set,
            DomainSetArg::HrrrRapRegions | DomainSetArg::UsRegions
        )
    {
        return Err(format!(
            "{} should use --domain-set global-model for multi-domain static plots",
            model.as_str()
        )
        .into());
    }

    Ok(match domain_set {
        DomainSetArg::Single => vec![domain(region)],
        DomainSetArg::GlobalModel => vec![
            domain(RegionPreset::Global),
            domain(RegionPreset::Conus),
            domain(RegionPreset::NorthAmerica),
            domain(RegionPreset::SouthAmerica),
            domain(RegionPreset::Europe),
            domain(RegionPreset::Africa),
            domain(RegionPreset::Asia),
            domain(RegionPreset::Australia),
            domain(RegionPreset::Antarctica),
        ],
        DomainSetArg::UsRegions => vec![
            domain(RegionPreset::Conus),
            domain(RegionPreset::Midwest),
            domain(RegionPreset::PacificNorthwest),
            domain(RegionPreset::CaliforniaSouthwest),
            domain(RegionPreset::RockiesHighPlains),
            domain(RegionPreset::SouthernPlains),
            domain(RegionPreset::Oklahoma),
            domain(RegionPreset::GreatLakes),
            domain(RegionPreset::Southeast),
            domain(RegionPreset::Northeast),
        ],
        DomainSetArg::HrrrRapRegions => vec![
            domain(RegionPreset::Conus),
            domain(RegionPreset::Midwest),
            domain(RegionPreset::California),
            domain(RegionPreset::CaliforniaSquare),
            domain(RegionPreset::RenoSquare),
            domain(RegionPreset::PacificNorthwest),
            domain(RegionPreset::CaliforniaSouthwest),
            domain(RegionPreset::RockiesHighPlains),
            domain(RegionPreset::Southeast),
            domain(RegionPreset::SouthernPlains),
            domain(RegionPreset::Oklahoma),
            domain(RegionPreset::IllinoisToKansas),
            domain(RegionPreset::GulfToKansas),
            domain(RegionPreset::Northeast),
            domain(RegionPreset::GreatLakes),
        ],
    })
}

fn default_non_ecape_product_sets(
    model: ModelId,
    forecast_hour: u16,
) -> (Vec<String>, Vec<String>, Vec<HrrrWindowedProduct>) {
    let catalog = build_supported_products_catalog();
    let supported_for_model = |entry: &rustwx_products::catalog::ProductCatalogEntry| {
        entry.support.iter().any(|target| {
            target.model == Some(model) && matches!(target.status, ProductTargetStatus::Supported)
        })
    };
    let direct = catalog
        .direct
        .iter()
        .filter(|entry| supported_for_model(entry))
        .filter(|entry| include_in_operational_default(entry))
        .map(|entry| entry.slug.clone())
        .collect::<Vec<_>>();
    let derived = catalog
        .derived
        .iter()
        .filter(|entry| supported_for_model(entry))
        .filter(|entry| include_in_operational_default(entry))
        .filter(|entry| {
            let slug = entry.slug.to_ascii_lowercase();
            !slug.contains("ecape") && !is_heavy_derived_recipe_slug(&entry.slug)
        })
        .map(|entry| entry.slug.clone())
        .collect::<Vec<_>>();
    let windowed = catalog
        .windowed
        .iter()
        .filter(|entry| supported_for_model(entry))
        .filter_map(|entry| windowed_product_from_slug(&entry.slug))
        .filter(|product| windowed_product_available_at_forecast_hour(*product, forecast_hour))
        .collect::<Vec<_>>();
    (direct, derived, windowed)
}

fn all_supported_non_ecape_product_sets(
    model: ModelId,
    forecast_hour: u16,
) -> (Vec<String>, Vec<String>, Vec<HrrrWindowedProduct>) {
    let catalog = build_supported_products_catalog();
    let supported_for_model = |entry: &rustwx_products::catalog::ProductCatalogEntry| {
        entry.support.iter().any(|target| {
            target.model == Some(model) && matches!(target.status, ProductTargetStatus::Supported)
        })
    };
    let direct = catalog
        .direct
        .iter()
        .filter(|entry| supported_for_model(entry))
        .filter(|entry| !direct_recipe_requires_explicit_opt_in(&entry.slug))
        .map(|entry| entry.slug.clone())
        .collect::<Vec<_>>();
    let derived = catalog
        .derived
        .iter()
        .filter(|entry| supported_for_model(entry))
        .filter(|entry| {
            let slug = entry.slug.to_ascii_lowercase();
            !slug.contains("ecape") && !is_heavy_derived_recipe_slug(&entry.slug)
        })
        .map(|entry| entry.slug.clone())
        .collect::<Vec<_>>();
    let windowed = catalog
        .windowed
        .iter()
        .filter(|entry| supported_for_model(entry))
        .filter_map(|entry| windowed_product_from_slug(&entry.slug))
        .filter(|product| windowed_product_available_at_forecast_hour(*product, forecast_hour))
        .collect::<Vec<_>>();
    (direct, derived, windowed)
}

fn direct_recipe_requires_explicit_opt_in(slug: &str) -> bool {
    let slug = slug.to_ascii_lowercase();
    slug.starts_with("nbm_qmd_")
        || slug.starts_with("sref_prob_")
        || slug.starts_with("href_")
        || slug.starts_with("refs_")
        || slug.starts_with("aigefs_spr_")
        || slug.starts_with("hgefs_spr_")
        || slug.starts_with("gefs_spr_")
}

fn include_in_operational_default(entry: &rustwx_products::catalog::ProductCatalogEntry) -> bool {
    entry.maturity == ProductMaturity::Operational
        && !entry.slug.starts_with("nbm_qmd_")
        && !entry.slug.starts_with("sref_prob_")
        && !entry
            .flags
            .iter()
            .any(|flag| matches!(flag, ProductSemanticFlag::Proxy))
        && entry.slug != "total_qpf"
}

fn windowed_product_from_slug(slug: &str) -> Option<HrrrWindowedProduct> {
    HrrrWindowedProduct::supported_products()
        .iter()
        .copied()
        .find(|product| product.slug() == slug)
}

fn build_direct_product_overrides(
    args: &Args,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let mut parsed = parse_product_overrides(&args.product_overrides)?;
    if args.model == ModelId::WrfGdex {
        if let Some(product) = &args.surface_product {
            parsed.insert("d612005-hist2d".to_string(), product.clone());
        }
        if let Some(product) = &args.pressure_product {
            parsed.insert("d612005-hist3d".to_string(), product.clone());
        }
    }
    Ok(parsed)
}

fn parse_product_overrides(
    values: &[String],
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let mut parsed = HashMap::new();
    for value in values {
        let (planned, actual) = value.split_once('=').ok_or_else(|| {
            format!("invalid --product-override '{value}', expected planned=actual")
        })?;
        let planned = planned.trim();
        let actual = actual.trim();
        if planned.is_empty() || actual.is_empty() {
            return Err(format!(
                "invalid --product-override '{value}', expected non-empty planned=actual"
            )
            .into());
        }
        parsed.insert(planned.to_string(), actual.to_string());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regional_models_accept_regional_domain_sets() {
        for model in [
            ModelId::Hrrr,
            ModelId::Rap,
            ModelId::RrfsA,
            ModelId::RrfsPublic,
            ModelId::RrfsFireWx,
            ModelId::Nam,
            ModelId::Hiresw,
        ] {
            let domains =
                domains_for_request(model, RegionPreset::Conus, &[], DomainSetArg::UsRegions)
                    .unwrap();
            assert!(domains.iter().any(|domain| domain.slug == "conus"));
            assert!(
                domains_for_request(model, RegionPreset::Conus, &[], DomainSetArg::GlobalModel)
                    .is_err()
            );
        }
    }

    #[test]
    fn global_models_accept_global_domain_set_only() {
        let domains = domains_for_request(
            ModelId::Gfs,
            RegionPreset::Global,
            &[],
            DomainSetArg::GlobalModel,
        )
        .unwrap();
        assert!(domains.iter().any(|domain| domain.slug == "global"));
        assert!(
            domains_for_request(
                ModelId::Gfs,
                RegionPreset::Global,
                &[],
                DomainSetArg::UsRegions
            )
            .is_err()
        );
    }

    #[test]
    fn explicit_regions_work_for_any_model() {
        let regions = [RegionPreset::Conus, RegionPreset::Europe];
        let domains = domains_for_request(
            ModelId::Gfs,
            RegionPreset::Global,
            &regions,
            DomainSetArg::Single,
        )
        .unwrap();
        assert_eq!(
            domains
                .iter()
                .map(|domain| domain.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["conus", "europe"]
        );
    }

    #[test]
    fn all_supported_hrrr_f000_has_no_auto_windowed_products() {
        let (_, _, windowed) = all_supported_non_ecape_product_sets(ModelId::Hrrr, 0);
        assert!(windowed.is_empty());
    }

    #[test]
    fn all_supported_hrrr_f006_keeps_only_available_windowed_products() {
        let (_, _, windowed) = all_supported_non_ecape_product_sets(ModelId::Hrrr, 6);
        assert!(windowed.contains(&HrrrWindowedProduct::Qpf1h));
        assert!(windowed.contains(&HrrrWindowedProduct::Qpf6h));
        assert!(windowed.contains(&HrrrWindowedProduct::QpfTotal));
        assert!(windowed.contains(&HrrrWindowedProduct::Uh25km3h));
        assert!(windowed.contains(&HrrrWindowedProduct::Wind10mRunMax));
        assert!(!windowed.contains(&HrrrWindowedProduct::Qpf12h));
        assert!(!windowed.contains(&HrrrWindowedProduct::Wind10m0to24hMax));
        assert!(!windowed.contains(&HrrrWindowedProduct::Temp2m0to24hMax));
    }
}
