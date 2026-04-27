use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::ecape::{EcapeBatchRequest, run_ecape_ratio_display_batch};
use rustwx_products::publication::{
    atomic_write_json, canonical_run_slug, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-ecape-ratio-display",
    about = "Generate HRRR ECAPE/CAPE derived-ratio display comparison plots"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::SouthernPlains)]
    region: RegionPreset,
    #[arg(long, default_value = "proof\\ecape_ratio_display\\plots")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(
        long,
        alias = "allow-conus-heavy",
        default_value_t = false,
        help = "Allow very large heavy ECAPE domains instead of refusing the run"
    )]
    allow_large_heavy_domain: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_slug = canonical_run_slug(
        "hrrr",
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.region.slug(),
        "ecape_ratio_display",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "hrrr_ecape_ratio_display",
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

    let request = EcapeBatchRequest {
        model: ModelId::Hrrr,
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: args.allow_large_heavy_domain,
    };
    let report = run_ecape_ratio_display_batch(&request)?;
    let stem = canonical_run_slug(
        "hrrr",
        &report.date_yyyymmdd,
        Some(report.cycle_utc),
        report.forecast_hour,
        &report.domain.slug,
        "ecape_ratio_display",
    );
    let report_path = args.out_dir.join(format!("{stem}_report.json"));
    atomic_write_json(&report_path, &report)?;

    for output in &report.outputs {
        println!("{}", output.output_path.display());
    }
    println!("{}", report_path.display());
    Ok(())
}
