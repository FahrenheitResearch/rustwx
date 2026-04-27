use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::model_summary;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::supported_derived_recipe_slugs;
use rustwx_products::direct::supported_direct_recipe_slugs;
use rustwx_products::non_ecape::{NonEcapeHourRequest, run_model_non_ecape_hour};
use rustwx_products::places::{PlaceLabelDensityTier, default_place_label_overlay_for_domain};
use rustwx_products::publication::{
    atomic_write_json, canonical_run_slug, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use rustwx_products::windowed::HrrrWindowedProduct;

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

fn default_direct_recipes() -> Vec<String> {
    vec![
        "composite_reflectivity",
        "2m_temperature_10m_winds",
        "2m_dewpoint_10m_winds",
        "2m_relative_humidity_10m_winds",
        "250mb_height_winds",
        "500mb_height_winds",
        "250mb_temperature_height_winds",
        "700mb_height_winds",
        "850mb_height_winds",
        "500mb_rh_height_winds",
        "700mb_temperature_height_winds",
        "850mb_temperature_height_winds",
        "mslp_10m_winds",
        "precipitable_water",
        "10m_wind_gusts",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_derived_recipes() -> Vec<String> {
    vec![
        "sbcape",
        "mlcape",
        "mucape",
        "sbcin",
        "mlcin",
        "bulk_shear_0_6km",
        "bulk_shear_0_1km",
        "srh_0_1km",
        "srh_0_3km",
        "ehi_0_1km",
        "ehi_0_3km",
        "stp_fixed",
        "scp_mu_0_3km_0_6km_proxy",
        "lapse_rate_700_500",
        "lapse_rate_0_3km",
        "theta_e_2m_10m_winds",
        "lifted_index",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
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
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_slug = canonical_run_slug(
        &args.model.as_str().replace('-', "_"),
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.region.slug(),
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
    let direct_recipe_slugs = if args.direct_recipes.is_empty() {
        let supported = supported_direct_recipe_slugs(args.model);
        default_direct_recipes()
            .into_iter()
            .filter(|slug| supported.contains(slug))
            .collect()
    } else {
        args.direct_recipes.clone()
    };
    let derived_recipe_slugs = if args.derived_recipes.is_empty() {
        let supported = supported_derived_recipe_slugs(args.model);
        default_derived_recipes()
            .into_iter()
            .filter(|slug| supported.contains(slug))
            .collect()
    } else {
        args.derived_recipes.clone()
    };
    let domain = DomainSpec::new(args.region.slug(), args.region.bounds());
    let direct_product_overrides = build_direct_product_overrides(args)?;
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
        windowed_products: args
            .windowed_products
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        output_width: 1200,
        output_height: 900,
        png_compression: rustwx_render::PngCompressionMode::Default,
        custom_poi_overlay: None,
        place_label_overlay: default_place_label_overlay_for_domain(
            &domain,
            PlaceLabelDensityTier::from_numeric(args.place_label_density),
        ),
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
    Ok(())
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
