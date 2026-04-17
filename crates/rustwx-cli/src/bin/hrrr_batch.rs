use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::hrrr::{HrrrBatchProduct, HrrrBatchRequest, run_hrrr_batch};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest,
    artifact_identity_from_path, atomic_write_json, finalize_and_publish_run_manifest,
};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProductArg {
    SevereProof,
    Ecape8,
}

impl From<ProductArg> for HrrrBatchProduct {
    fn from(value: ProductArg) -> Self {
        match value {
            ProductArg::SevereProof => HrrrBatchProduct::SevereProofPanel,
            ProductArg::Ecape8 => HrrrBatchProduct::Ecape8Panel,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-batch",
    about = "Generate multiple RustWX HRRR proof products from one fetched/decoded timestep"
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

    let request = HrrrBatchRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.clone(),
        use_cache: !args.no_cache,
        products: args.products.into_iter().map(Into::into).collect(),
    };
    let report = run_hrrr_batch(&request)?;
    let report_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_batch_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    ));
    let run_slug = format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_batch",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    );
    atomic_write_json(&report_path, &report)?;
    let mut artifacts = Vec::with_capacity(report.products.len());
    for product in &report.products {
        let mut record = PublishedArtifactRecord::planned(
            product.product.slug(),
            relative_output_path(&args.out_dir, &product.output_path),
        )
        .with_state(ArtifactPublicationState::Complete)
        .with_input_fetch_keys(product.input_fetch_keys.clone());
        let content_identity = match &product.content_identity {
            Some(identity) => identity.clone(),
            None => artifact_identity_from_path(&product.output_path)?,
        };
        record = record.with_content_identity(content_identity);
        if let Some(failure_count) = product.metadata.failure_count {
            record = record.with_detail(format!("failure_count={failure_count}"));
        }
        artifacts.push(record);
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

    for product in &report.products {
        println!("{}", product.output_path.display());
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
