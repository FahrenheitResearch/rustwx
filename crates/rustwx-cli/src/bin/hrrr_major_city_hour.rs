use std::fs;
use std::path::PathBuf;

#[path = "../metro.rs"]
mod metro;
#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use metro::major_us_city_domains;
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::supported_derived_recipe_inventory;
use rustwx_products::direct::supported_direct_recipe_slugs;
use rustwx_products::non_ecape::{
    HrrrNonEcapeMultiDomainRequest, run_hrrr_non_ecape_hour_multi_domain,
};
use rustwx_products::publication::atomic_write_json;
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use rustwx_products::windowed::HrrrWindowedProduct;
use rustwx_render::PngCompressionMode;

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
    name = "hrrr-major-city-hour",
    about = "Generate one HRRR non-ECAPE hour for CONUS plus major-city crops using one shared load"
)]
struct Args {
    #[arg(long, default_value = "20260419")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: rustwx_core::SourceId,
    #[arg(
        long,
        default_value = "C:\\Users\\drew\\rustwx\\proof\\hrrr_major_city_hour"
    )]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long = "source-mode", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(long = "windowed-product", value_enum, value_delimiter = ',', num_args = 1..)]
    windowed_products: Vec<WindowedProductArg>,
    #[arg(long, default_value_t = false)]
    skip_conus: bool,
    #[arg(long)]
    max_cities: Option<usize>,
    #[arg(long, default_value_t = 1)]
    domain_jobs: usize,
    #[arg(long)]
    render_threads: Option<usize>,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Default)]
    png_compression: PngCompressionArg,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run(&args)
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

    let render_threads = args
        .render_threads
        .or_else(|| if args.domain_jobs > 1 { Some(1) } else { None });
    match render_threads {
        Some(value) if value > 0 => unsafe {
            std::env::set_var("RUSTWX_RENDER_THREADS", value.to_string());
        },
        _ => unsafe {
            std::env::remove_var("RUSTWX_RENDER_THREADS");
        },
    }

    let mut domains = Vec::<DomainSpec>::new();
    if !args.skip_conus {
        domains.push(DomainSpec::new(
            RegionPreset::Conus.slug(),
            RegionPreset::Conus.bounds(),
        ));
    }
    let mut city_domains = major_us_city_domains();
    if let Some(limit) = args.max_cities {
        city_domains.truncate(limit);
    }
    domains.extend(city_domains);

    let direct_recipe_slugs = if args.direct_recipes.is_empty() {
        supported_direct_recipe_slugs(rustwx_core::ModelId::Hrrr)
    } else {
        args.direct_recipes.clone()
    };
    let derived_recipe_slugs = if args.derived_recipes.is_empty() {
        supported_derived_recipe_inventory()
            .iter()
            .map(|recipe| recipe.slug.to_string())
            .collect()
    } else {
        args.derived_recipes.clone()
    };

    let request = HrrrNonEcapeMultiDomainRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domains,
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        source_mode: args.source_mode.into(),
        direct_recipe_slugs,
        derived_recipe_slugs,
        windowed_products: args
            .windowed_products
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        output_width: 1200,
        output_height: 900,
        png_compression: args.png_compression.into(),
        domain_jobs: Some(args.domain_jobs),
    };

    let report = run_hrrr_non_ecape_hour_multi_domain(&request)?;
    let report_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_major_city_hour_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour
    ));
    atomic_write_json(&report_path, &report)?;

    for domain in &report.domains {
        println!("{} {}", domain.domain.slug, domain.summary.output_count);
    }
    println!("{}", report_path.display());
    Ok(())
}
