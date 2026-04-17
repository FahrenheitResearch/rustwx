use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::model_summary;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    finalize_and_publish_run_manifest,
};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::severe::{SevereBatchRequest, run_severe_batch};

#[derive(Debug, Parser)]
#[command(
    name = "severe-batch",
    about = "Generate a severe proof panel from one shared full-file thermodynamic load"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long)]
    surface_product: Option<String>,
    #[arg(long)]
    pressure_product: Option<String>,
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

    let source = args
        .source
        .unwrap_or(model_summary(args.model).sources[0].id);
    let request = SevereBatchRequest {
        model: args.model,
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.clone(),
        use_cache: !args.no_cache,
        surface_product_override: args.surface_product.clone(),
        pressure_product_override: args.pressure_product.clone(),
    };
    let report = run_severe_batch(&request)?;

    let model_slug = report.model.as_str().replace('-', "_");
    let stem = format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_severe_proof_panel",
        model_slug,
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.domain.slug
    );
    let manifest_path = args.out_dir.join(format!("{stem}_manifest.json"));
    let timing_path = args.out_dir.join(format!("{stem}_timing.json"));
    atomic_write_json(&manifest_path, &report)?;
    atomic_write_json(
        &timing_path,
        &serde_json::json!({
            "model": report.model,
            "date": report.date_yyyymmdd,
            "cycle_utc": report.cycle_utc,
            "forecast_hour": report.forecast_hour,
            "source": report.source,
            "domain": report.domain,
            "input_fetches": report.input_fetches,
            "shared_timing": report.shared_timing,
            "project_ms": report.project_ms,
            "compute_ms": report.compute_ms,
            "render_ms": report.render_ms,
            "total_ms": report.total_ms,
        }),
    )?;
    let mut run_manifest =
        RunPublicationManifest::new("severe_batch", stem.clone(), args.out_dir.clone())
            .with_run_metadata(
                report.model.as_str(),
                report.date_yyyymmdd.clone(),
                report.cycle_utc,
                report.forecast_hour,
                report.source.as_str(),
                report.domain.slug.clone(),
            )
            .with_input_fetches(report.input_fetches.clone())
            .with_artifacts(vec![
                PublishedArtifactRecord::planned(
                    "severe_proof_panel",
                    relative_output_path(&args.out_dir, &report.output_path),
                )
                .with_state(ArtifactPublicationState::Complete)
                .with_content_identity(report.output_identity.clone())
                .with_input_fetch_keys(
                    report
                        .input_fetches
                        .iter()
                        .map(|fetch| fetch.fetch_key.clone())
                        .collect(),
                ),
            ]);
    let (canonical_manifest, attempt_manifest) =
        finalize_and_publish_run_manifest(&mut run_manifest, &args.out_dir, &stem)?;

    println!("{}", report.output_path.display());
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
