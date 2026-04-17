//! HRRR-pinned severe-proof panel runner.
//!
//! This was the original severe-panel CLI before the generic
//! `severe_batch` binary existed. The old version hard-coded the HRRR
//! fetch/decoder surface (`fetch_hrrr_subset`, `PRESSURE_PATTERNS`,
//! `SURFACE_PATTERNS`, `load_or_decode_surface`, etc.), all of which
//! were removed when `rustwx_products::severe` went generic. That left
//! `hrrr_severe_proof` broken on main.
//!
//! We keep the bin because it's still the operator-friendly entry point
//! for "HRRR severe proof panel right now, no cross-model flags" — now
//! it's a thin wrapper around `run_severe_batch` with the model pinned
//! to HRRR. It shares the same manifest contract, provenance capture,
//! and failure-path publication as every other operational runner.

use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    canonical_run_slug, finalize_and_publish_run_manifest, publish_failure_manifest,
};
use rustwx_products::severe::{SevereBatchRequest, run_severe_batch};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-severe-proof",
    about = "Generate a RustWX HRRR severe proof panel from supported fixed-depth diagnostics"
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
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_slug = canonical_run_slug(
        "hrrr",
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.region.slug(),
        "severe_proof_panel",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "hrrr_severe_proof",
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

    let request = SevereBatchRequest {
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
    };
    let report = run_severe_batch(&request)?;

    let stem = canonical_run_slug(
        "hrrr",
        &report.date_yyyymmdd,
        Some(report.cycle_utc),
        report.forecast_hour,
        &report.domain.slug,
        "severe_proof_panel",
    );
    let report_path = args.out_dir.join(format!("{stem}_report.json"));
    let timing_path = args.out_dir.join(format!("{stem}_timing.json"));
    atomic_write_json(&report_path, &report)?;
    atomic_write_json(
        &timing_path,
        &serde_json::json!({
            "model": report.model,
            "date": report.date_yyyymmdd,
            "cycle_utc": report.cycle_utc,
            "forecast_hour": report.forecast_hour,
            "region": report.domain.slug,
            "source": report.source.as_str(),
            "assumptions": {
                "stp": "fixed-layer Thompson-style STP using sbCAPE, sbLCL, 0-1 km SRH, and 0-6 km bulk shear",
                "scp": "fixed-depth proxy using muCAPE, 0-3 km SRH, and 0-6 km bulk shear",
                "ehi": "0-1 km EHI using sbCAPE and 0-1 km SRH",
                "effective_layer": "not derived in this proof path"
            },
            "timing_ms": {
                "project": report.project_ms,
                "compute": report.compute_ms,
                "render": report.render_ms,
                "total": report.total_ms,
            }
        }),
    )?;
    let mut run_manifest =
        RunPublicationManifest::new("hrrr_severe_proof", stem.clone(), args.out_dir.clone())
            .with_run_metadata(
                "hrrr",
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
    println!("{}", report_path.display());
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
