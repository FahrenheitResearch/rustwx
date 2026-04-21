use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::heavy::{HeavyPanelHourRequest, run_heavy_panel_hour};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    canonical_run_slug, finalize_and_publish_run_manifest, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProductArg {
    SevereProof,
    Ecape8,
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-batch",
    about = "Generate multiple RustWX HRRR heavy map products from one shared cropped heavy load"
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
    #[arg(
        long,
        alias = "allow-conus-heavy",
        default_value_t = false,
        help = "Allow very large heavy ECAPE/severe domains instead of refusing the run"
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
        "batch",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "hrrr_batch",
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

    let request = HeavyPanelHourRequest {
        model: rustwx_core::ModelId::Hrrr,
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.clone(),
        use_cache: !args.no_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: args.allow_large_heavy_domain,
    };
    let report = run_heavy_panel_hour(&request)?;
    let report_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_batch_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    ));
    let run_slug = format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_batch",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    );
    atomic_write_json(&report_path, &report)?;
    let selected_severe = args
        .products
        .iter()
        .any(|product| matches!(product, ProductArg::SevereProof));
    let selected_ecape = args
        .products
        .iter()
        .any(|product| matches!(product, ProductArg::Ecape8));
    let input_fetch_keys = report
        .input_fetches
        .iter()
        .map(|fetch| fetch.fetch_key.clone())
        .collect::<Vec<_>>();
    let mut artifacts = Vec::new();
    if selected_severe {
        artifacts.extend(report.severe.outputs.iter().map(|output| {
            PublishedArtifactRecord::planned(
                &output.product,
                relative_output_path(&args.out_dir, &output.output_path),
            )
            .with_state(ArtifactPublicationState::Complete)
            .with_content_identity(output.output_identity.clone())
            .with_input_fetch_keys(input_fetch_keys.clone())
        }));
    }
    if selected_ecape {
        artifacts.extend(report.ecape.outputs.iter().map(|output| {
            PublishedArtifactRecord::planned(
                &output.product,
                relative_output_path(&args.out_dir, &output.output_path),
            )
            .with_state(ArtifactPublicationState::Complete)
            .with_detail(format!(
                "failure_count={}",
                report.ecape.failure_count.unwrap_or(0)
            ))
            .with_content_identity(output.output_identity.clone())
            .with_input_fetch_keys(input_fetch_keys.clone())
        }));
    }
    let mut run_manifest =
        RunPublicationManifest::new("hrrr_batch", run_slug.clone(), args.out_dir.clone())
            .with_run_metadata(
                "hrrr",
                report.date_yyyymmdd.clone(),
                report.cycle_utc,
                report.forecast_hour,
                report.source.as_str(),
                report.domain.slug.clone(),
            )
            .with_input_fetches(report.input_fetches.clone())
            .with_artifacts(artifacts);
    let (canonical_manifest, attempt_manifest) =
        finalize_and_publish_run_manifest(&mut run_manifest, &args.out_dir, &run_slug)?;

    if selected_severe {
        for product in &report.severe.outputs {
            println!("{}", product.output_path.display());
        }
    }
    if selected_ecape {
        for product in &report.ecape.outputs {
            println!("{}", product.output_path.display());
        }
    }
    println!("{}", report_path.display());
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
