use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::direct::{HrrrDirectBatchRequest, run_hrrr_direct_batch};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    finalize_and_publish_run_manifest,
};
use rustwx_products::shared_context::DomainSpec;

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
    atomic_write_json(&manifest_path, &report)?;
    atomic_write_json(
        &timing_path,
        &serde_json::json!({
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
        }),
    )?;
    let mut run_manifest =
        RunPublicationManifest::new("hrrr_direct_batch", stem.clone(), args.out_dir.clone())
            .with_run_metadata(
                "hrrr",
                report.date_yyyymmdd.clone(),
                report.cycle_utc,
                report.forecast_hour,
                report.source.as_str(),
                report.domain.slug.clone(),
            )
            .with_input_fetches(
                report
                    .fetches
                    .iter()
                    .map(|fetch| fetch.input_fetch.clone())
                    .collect(),
            )
            .with_artifacts(
                report
                    .recipes
                    .iter()
                    .map(|recipe| {
                        PublishedArtifactRecord::planned(
                            recipe.recipe_slug.clone(),
                            relative_output_path(&args.out_dir, &recipe.output_path),
                        )
                        .with_state(ArtifactPublicationState::Complete)
                        .with_content_identity(recipe.content_identity.clone())
                        .with_input_fetch_keys(recipe.input_fetch_keys.clone())
                    })
                    .collect(),
            );
    let (canonical_manifest, attempt_manifest) =
        finalize_and_publish_run_manifest(&mut run_manifest, &args.out_dir, &stem)?;

    for recipe in &report.recipes {
        println!("{}", recipe.output_path.display());
    }
    println!("{}", manifest_path.display());
    println!("{}", timing_path.display());
    println!("{}", canonical_manifest.display());
    println!("{}", attempt_manifest.display());
    Ok(())
}

fn relative_output_path(root: &std::path::Path, output_path: &std::path::Path) -> PathBuf {
    output_path
        .strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| output_path.to_path_buf())
}
