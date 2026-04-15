use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::direct::{HrrrDirectBatchRequest, run_hrrr_direct_batch};
use rustwx_products::hrrr::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-direct-batch",
    about = "Generate multiple direct/native RustWX HRRR plots from one shared timestep fetch/extract pass"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: rustwx_core::SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long = "recipe", value_delimiter = ',', num_args = 1.., required = true)]
    recipes: Vec<String>,
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

    let request = HrrrDirectBatchRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.clone(),
        use_cache: !args.no_cache,
        recipe_slugs: args.recipes,
    };
    let report = run_hrrr_direct_batch(&request)?;

    let stem = format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_direct",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    );
    let manifest_path = args.out_dir.join(format!("{stem}_manifest.json"));
    let timing_path = args.out_dir.join(format!("{stem}_timing.json"));
    fs::write(&manifest_path, serde_json::to_vec_pretty(&report)?)?;
    fs::write(
        &timing_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "date": report.date_yyyymmdd,
            "cycle_utc": report.cycle_utc,
            "forecast_hour": report.forecast_hour,
            "source": report.source,
            "domain": report.domain,
            "fetches": report.fetches,
            "recipes": report.recipes.iter().map(|recipe| {
                serde_json::json!({
                    "recipe_slug": recipe.recipe_slug,
                    "output_path": recipe.output_path,
                    "timing_ms": recipe.timing,
                })
            }).collect::<Vec<_>>(),
            "total_ms": report.total_ms,
        }))?,
    )?;

    for recipe in &report.recipes {
        println!("{}", recipe.output_path.display());
    }
    println!("{}", manifest_path.display());
    println!("{}", timing_path.display());
    Ok(())
}
