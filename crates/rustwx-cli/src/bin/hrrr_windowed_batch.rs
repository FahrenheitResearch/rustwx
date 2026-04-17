use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest,
    artifact_identity_from_path, atomic_write_json, finalize_and_publish_run_manifest,
};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::windowed::{
    HrrrWindowedBatchRequest, HrrrWindowedProduct, collect_windowed_input_fetches,
    run_hrrr_windowed_batch, windowed_product_input_fetch_keys,
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
    let run_slug = format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_windowed",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour, report.domain.slug
    );
    atomic_write_json(&report_path, &report)?;
    let mut artifacts = Vec::with_capacity(report.products.len() + report.blockers.len());
    for product in &report.products {
        artifacts.push(
            PublishedArtifactRecord::planned(
                product.product.slug(),
                relative_output_path(&args.out_dir, &product.output_path),
            )
            .with_state(ArtifactPublicationState::Complete)
            .with_content_identity(artifact_identity_from_path(&product.output_path)?)
            .with_input_fetch_keys(windowed_product_input_fetch_keys(
                product,
                &report.shared_timing,
            )),
        );
    }
    artifacts.extend(report.blockers.iter().map(|blocker| {
        PublishedArtifactRecord::planned(
            blocker.product.slug(),
            PathBuf::from(format!("{}_blocked.txt", blocker.product.slug())),
        )
        .with_state(ArtifactPublicationState::Blocked)
        .with_detail(blocker.reason.clone())
    }));
    let mut run_manifest =
        RunPublicationManifest::new("hrrr_windowed_batch", run_slug.clone(), args.out_dir.clone())
            .with_run_metadata(
                "hrrr",
                report.date_yyyymmdd.clone(),
                report.cycle_utc,
                report.forecast_hour,
                report.source.as_str(),
                report.domain.slug.clone(),
            )
            .with_input_fetches(collect_windowed_input_fetches(&report))
            .with_artifacts(artifacts);
    let (canonical_manifest, attempt_manifest) =
        finalize_and_publish_run_manifest(&mut run_manifest, &args.out_dir, &run_slug)?;

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
