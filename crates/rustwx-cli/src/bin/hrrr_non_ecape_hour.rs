use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::non_ecape::{HrrrNonEcapeHourRequest, run_hrrr_non_ecape_hour};
use rustwx_products::publication::{
    atomic_write_json, canonical_run_slug, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::thermo_native::ThermoPathMode;
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
    #[arg(long, value_enum, default_value_t = ThermoPathArg::PreferNativeExact)]
    thermo_path: ThermoPathArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ThermoPathArg {
    CanonicalDerived,
    PreferNativeExact,
    CompareNativeVsDerived,
    NativeOnly,
}

impl From<ThermoPathArg> for ThermoPathMode {
    fn from(value: ThermoPathArg) -> Self {
        match value {
            ThermoPathArg::CanonicalDerived => Self::CanonicalDerived,
            ThermoPathArg::PreferNativeExact => Self::PreferNativeExact,
            ThermoPathArg::CompareNativeVsDerived => Self::CompareNativeVsDerived,
            ThermoPathArg::NativeOnly => Self::NativeOnly,
        }
    }
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

    let request = HrrrNonEcapeHourRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        direct_recipe_slugs: args.direct_recipes.clone(),
        derived_recipe_slugs: args.derived_recipes.clone(),
        windowed_products: args.windowed_products.iter().copied().map(Into::into).collect(),
        thermo_path_mode: args.thermo_path.into(),
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
    println!("{}", report.publication_manifest_path.display());
    if let Some(attempt_path) = &report.attempt_manifest_path {
        println!("{}", attempt_path.display());
    }
    println!("{}", report_path.display());
    Ok(())
}
