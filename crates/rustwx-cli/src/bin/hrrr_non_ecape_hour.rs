use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::supported_derived_recipe_slugs;
use rustwx_products::direct::supported_direct_recipe_slugs;
use rustwx_products::non_ecape::{HrrrNonEcapeHourRequest, run_hrrr_non_ecape_hour};
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[value(rename_all = "kebab-case")]
enum PlaceLabelDensityArg {
    /// Disable place labels.
    #[value(alias("0"), alias("off"))]
    None,
    /// Major anchor labels only.
    #[default]
    #[value(alias("1"))]
    Major,
    /// Major anchors plus nearby auxiliary labels.
    #[value(alias("2"))]
    MajorAndAux,
    /// The densest supported label set.
    #[value(alias("3"), alias("full"))]
    Dense,
}

impl From<PlaceLabelDensityArg> for PlaceLabelDensityTier {
    fn from(value: PlaceLabelDensityArg) -> Self {
        match value {
            PlaceLabelDensityArg::None => Self::None,
            PlaceLabelDensityArg::Major => Self::Major,
            PlaceLabelDensityArg::MajorAndAux => Self::MajorAndAux,
            PlaceLabelDensityArg::Dense => Self::Dense,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-non-ecape-hour",
    about = "Generate one unified CONUS-first HRRR hour pass across direct, derived, and windowed non-ECAPE products"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(
        long,
        default_value = "nomads",
        help = "HRRR source for the main operator path; defaults to NOMADS full-family ingest"
    )]
    source: rustwx_core::SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::Conus)]
    region: RegionPreset,
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(
        long = "windowed-product",
        value_enum,
        value_delimiter = ',',
        num_args = 1..
    )]
    windowed_products: Vec<WindowedProductArg>,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Disable caches for an honest cold-run ingest benchmark"
    )]
    no_cache: bool,
    #[arg(long = "source-mode", alias = "thermo-path", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,
    #[arg(
        long = "place-label-density",
        value_enum,
        default_value_t = PlaceLabelDensityArg::Major,
        help = "Place-label density: none, major, major-and-aux, or dense. Numeric aliases 0-3 also work."
    )]
    place_label_density: PlaceLabelDensityArg,
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
        "2m_relative_humidity",
        "500mb_height_winds",
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_slug = canonical_run_slug(
        "hrrr",
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.region.slug(),
        "non_ecape_hour",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        // The products layer publishes its own failure manifest once the
        // run manifest has been staged, but we still need a belt-and-
        // braces failure manifest for errors that happen before that
        // (argument validation, cache-dir creation, early ingest
        // failures). This helper is idempotent for reruns so there's no
        // harm if the products layer also wrote one.
        let _ = publish_failure_manifest(
            "hrrr_non_ecape_hour",
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

    let direct_recipe_slugs = if args.direct_recipes.is_empty() {
        let supported = supported_direct_recipe_slugs(rustwx_core::ModelId::Hrrr);
        default_direct_recipes()
            .into_iter()
            .filter(|slug| supported.contains(slug))
            .collect()
    } else {
        args.direct_recipes.clone()
    };
    let derived_recipe_slugs = if args.derived_recipes.is_empty() {
        let supported = supported_derived_recipe_slugs(rustwx_core::ModelId::Hrrr);
        default_derived_recipes()
            .into_iter()
            .filter(|slug| supported.contains(slug))
            .collect()
    } else {
        args.derived_recipes.clone()
    };

    let domain = DomainSpec::new(args.region.slug(), args.region.bounds());
    let request = HrrrNonEcapeHourRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: domain.clone(),
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        direct_recipe_slugs,
        derived_recipe_slugs,
        windowed_products: args
            .windowed_products
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        source_mode: args.source_mode.into(),
        output_width: 1200,
        output_height: 900,
        png_compression: rustwx_render::PngCompressionMode::Default,
        custom_poi_overlay: None,
        place_label_overlay: default_place_label_overlay_for_domain(
            &domain,
            args.place_label_density.into(),
        ),
    };
    let report = run_hrrr_non_ecape_hour(&request)?;
    let report_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_non_ecape_hour_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
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
