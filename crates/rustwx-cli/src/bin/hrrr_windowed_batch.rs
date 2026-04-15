use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::hrrr::DomainSpec;
use rustwx_products::windowed::{
    HrrrWindowedBatchRequest, HrrrWindowedProduct, run_hrrr_windowed_batch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProductArg {
    Qpf1h,
    Qpf6h,
    Qpf12h,
    Qpf24h,
    QpfTotal,
    Uh25km1h,
    Uh25km3h,
    Uh25kmRunMax,
}

impl From<ProductArg> for HrrrWindowedProduct {
    fn from(value: ProductArg) -> Self {
        match value {
            ProductArg::Qpf1h => HrrrWindowedProduct::Qpf1h,
            ProductArg::Qpf6h => HrrrWindowedProduct::Qpf6h,
            ProductArg::Qpf12h => HrrrWindowedProduct::Qpf12h,
            ProductArg::Qpf24h => HrrrWindowedProduct::Qpf24h,
            ProductArg::QpfTotal => HrrrWindowedProduct::QpfTotal,
            ProductArg::Uh25km1h => HrrrWindowedProduct::Uh25km1h,
            ProductArg::Uh25km3h => HrrrWindowedProduct::Uh25km3h,
            ProductArg::Uh25kmRunMax => HrrrWindowedProduct::Uh25kmRunMax,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-windowed-batch",
    about = "Generate conservative multi-hour HRRR QPF and UH window products"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 6)]
    forecast_hour: u16,
    #[arg(long, default_value = "aws")]
    source: rustwx_core::SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(
        long = "product",
        value_enum,
        value_delimiter = ',',
        num_args = 1..,
        required = true
    )]
    products: Vec<ProductArg>,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let request = HrrrWindowedBatchRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        products: args.products.into_iter().map(Into::into).collect(),
    };
    let report = run_hrrr_windowed_batch(&request)?;
    let report_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_windowed_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    ));
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;

    for product in &report.products {
        println!("{}", product.output_path.display());
    }
    if !report.blockers.is_empty() {
        eprintln!("blocked products:");
        for blocker in &report.blockers {
            eprintln!("  {}: {}", blocker.product.slug(), blocker.reason);
        }
    }
    println!("{}", report_path.display());
    Ok(())
}
