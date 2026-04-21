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
    canonical_run_slug, finalize_and_publish_run_manifest, publish_failure_manifest,
};
use rustwx_products::severe::{SevereBatchRequest, run_severe_batch};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "severe-batch",
    about = "Generate severe map products from one shared cropped heavy thermodynamic load"
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
    #[arg(
        long,
        alias = "allow-conus-heavy",
        default_value_t = false,
        help = "Allow very large heavy severe domains instead of refusing the run"
    )]
    allow_large_heavy_domain: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_slug = canonical_run_slug(
        &args.model.as_str().replace('-', "_"),
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.region.slug(),
        "severe",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "severe_batch",
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
        allow_large_heavy_domain: args.allow_large_heavy_domain,
    };
    let report = run_severe_batch(&request)?;

    let model_slug = report.model.as_str().replace('-', "_");
    let stem = format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_severe",
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
            "heavy_timing": report.heavy_timing,
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
            .with_artifacts(build_artifacts(&args.out_dir, &report));
    let (canonical_manifest, attempt_manifest) =
        finalize_and_publish_run_manifest(&mut run_manifest, &args.out_dir, &stem)?;

    for output in &report.outputs {
        println!("{}", output.output_path.display());
    }
    println!("{}", manifest_path.display());
    println!("{}", timing_path.display());
    println!("{}", canonical_manifest.display());
    println!("{}", attempt_manifest.display());
    Ok(())
}

fn build_artifacts(
    out_dir: &std::path::Path,
    report: &rustwx_products::severe::SevereBatchReport,
) -> Vec<PublishedArtifactRecord> {
    let input_fetch_keys = report
        .input_fetches
        .iter()
        .map(|fetch| fetch.fetch_key.clone())
        .collect::<Vec<_>>();
    report
        .outputs
        .iter()
        .map(|output| {
            PublishedArtifactRecord::planned(
                &output.product,
                relative_output_path(out_dir, &output.output_path),
            )
            .with_state(ArtifactPublicationState::Complete)
            .with_content_identity(output.output_identity.clone())
            .with_input_fetch_keys(input_fetch_keys.clone())
        })
        .collect()
}

fn relative_output_path(root: &std::path::Path, output_path: &std::path::Path) -> PathBuf {
    output_path
        .strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| output_path.to_path_buf())
}
