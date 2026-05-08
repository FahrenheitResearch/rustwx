use anyhow::Context;
use clap::Parser;
use rustwx_products::native_dataset::{
    NativeDatasetPlan, NativeDatasetRunnerConfig, NativeDryRunHourProcessor,
    run_native_dataset_hour_plan_with_progress,
};
use rustwx_products::native_dataset_materializer::{
    NativeDatasetMaterializer, NativeDatasetMaterializerConfig, NativeMaterializerMissingPolicy,
};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "native-dataset-runner",
    about = "Run a Rust-native dataset plan in hour-major dry-run or materialization mode"
)]
struct Args {
    #[arg(long, default_value = "target/native_dataset_plan/dataset_plan.json")]
    plan: PathBuf,
    #[arg(long, value_name = "PATH")]
    progress_out: Option<PathBuf>,
    #[arg(long, value_name = "PATH")]
    report_out: Option<PathBuf>,
    #[arg(long, value_name = "DIR")]
    source_root: Option<PathBuf>,
    #[arg(
        long,
        value_name = "DIR",
        default_value = "target/native_dataset_cache"
    )]
    cache_root: PathBuf,
    #[arg(long, value_name = "DIR")]
    shard_out: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    allow_missing_sources: bool,
    #[arg(long, default_value_t = false)]
    fetch_hrrr: bool,
    #[arg(long, default_value_t = false)]
    fetch_obs: bool,
    #[arg(long, default_value_t = false)]
    fetch_radar: bool,
    #[arg(long, default_value_t = 3)]
    max_attempts: u16,
    #[arg(long, default_value_t = true)]
    continue_on_error: bool,
    #[arg(long, default_value_t = 0)]
    rayon_threads: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.rayon_threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.rayon_threads)
            .build_global()
            .context("failed to configure global Rayon thread pool")?;
    }
    let plan_bytes = fs::read(&args.plan)
        .with_context(|| format!("failed to read plan: {}", args.plan.display()))?;
    let plan: NativeDatasetPlan = serde_json::from_slice(&plan_bytes)
        .with_context(|| format!("failed to parse plan JSON: {}", args.plan.display()))?;

    let config = NativeDatasetRunnerConfig {
        max_attempts: args.max_attempts,
        continue_on_error: args.continue_on_error,
    };
    let progress_out = args.progress_out.clone();
    if let Some(path) = &progress_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create progress directory: {}", parent.display())
            })?;
        }
        fs::write(path, b"")
            .with_context(|| format!("failed to initialize progress JSONL: {}", path.display()))?;
    }
    let mut progress_lines = Vec::new();

    let materializing = args.shard_out.is_some();
    let report = if let Some(shard_out) = args.shard_out.clone() {
        let mut materializer_config =
            NativeDatasetMaterializerConfig::new(&args.cache_root, shard_out)
                .with_missing_policy(if args.allow_missing_sources {
                    NativeMaterializerMissingPolicy::FillNan
                } else {
                    NativeMaterializerMissingPolicy::Fail
                })
                .with_fetch_hrrr_when_missing(args.fetch_hrrr)
                .with_fetch_obs_when_missing(args.fetch_obs)
                .with_fetch_level2_when_missing(args.fetch_radar);
        if let Some(source_root) = args.source_root.clone() {
            materializer_config = materializer_config.with_source_root(source_root);
        }
        let mut processor = NativeDatasetMaterializer::create(&plan, materializer_config)
            .map_err(|err| anyhow::anyhow!("initialize native dataset materializer: {err}"))?;
        let report = run_native_dataset_hour_plan_with_progress(
            &plan,
            &config,
            &mut processor,
            |progress| {
                record_progress(&progress_out, &mut progress_lines, progress);
            },
        )
        .map_err(anyhow::Error::msg)?;
        let manifest = processor
            .finish()
            .map_err(|err| anyhow::anyhow!("finish native dataset shard: {err}"))?;
        eprintln!(
            "{}",
            serde_json::json!({
                "mode": "materialize",
                "shard_out": args.shard_out,
                "sample_count": manifest.sample_count,
                "completed": manifest.completed,
            })
        );
        report
    } else {
        let mut processor = NativeDryRunHourProcessor;
        run_native_dataset_hour_plan_with_progress(&plan, &config, &mut processor, |progress| {
            record_progress(&progress_out, &mut progress_lines, progress);
        })
        .map_err(anyhow::Error::msg)?
    };

    if progress_out.is_none() {
        for line in progress_lines {
            println!("{line}");
        }
    }

    if let Some(path) = args.report_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create report directory: {}", parent.display())
            })?;
        }
        let bytes = serde_json::to_vec_pretty(&report)
            .context("failed to serialize native dataset run report")?;
        fs::write(&path, bytes)
            .with_context(|| format!("failed to write run report: {}", path.display()))?;
    }

    eprintln!(
        "{}",
        serde_json::json!({
            "mode": if materializing { "materialize" } else { "dry_run" },
            "schema_version": report.schema_version,
            "dataset_name": report.dataset_name,
            "hours_total": report.progress.hours_total,
            "hours_completed": report.progress.hours_completed,
            "tile_frames_total": report.progress.tile_frames_total,
            "tile_frames_completed": report.progress.tile_frames_completed,
            "samples_emitted": report.progress.samples_emitted,
            "failed_hours": report.progress.failed_hours,
            "failed_tile_frames": report.progress.failed_tile_frames,
        })
    );
    Ok(())
}

fn record_progress(
    progress_out: &Option<PathBuf>,
    progress_lines: &mut Vec<String>,
    progress: &rustwx_products::native_dataset::NativeDatasetProgress,
) {
    let line =
        serde_json::to_string(progress).expect("native dataset progress should be serializable");
    if let Some(path) = progress_out {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("progress JSONL should be appendable");
        writeln!(file, "{line}").expect("progress JSONL should be writable");
    } else {
        progress_lines.push(line);
    }
}
