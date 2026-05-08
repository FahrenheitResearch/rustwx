use anyhow::{Context, Result, bail};
use clap::Parser;
use rustwx_products::native_dataset_shard_store::{
    TRAINING_SHARD_INDEX_FILE, TRAINING_SHARD_MANIFEST_FILE, TrainingShardManifest,
    TrainingShardReader, TrainingShardSampleTensor, TrainingShardTensorSpec, TrainingShardWriter,
};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "native-dataset-shard-export",
    about = "Initialize a RustWX native training shard store"
)]
struct Args {
    #[arg(long, default_value = "target/native_dataset_shard")]
    out_dir: PathBuf,
    #[arg(long, default_value = "shard-00000")]
    shard_id: String,
    #[arg(long, default_value_t = false)]
    init: bool,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false)]
    append_synthetic_sample: bool,
}

#[derive(Debug, Serialize)]
struct ShardExportReport {
    shard_id: String,
    out_dir: PathBuf,
    dry_run: bool,
    initialized: bool,
    appended_synthetic_sample: bool,
    sample_count: u64,
    layout: Vec<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.dry_run && !args.init {
        bail!("use --dry-run to inspect the layout or --init to create it");
    }

    let manifest = default_manifest(&args.shard_id)?;
    let layout = shard_layout(&manifest);
    if args.dry_run {
        let report = ShardExportReport {
            shard_id: args.shard_id,
            out_dir: args.out_dir,
            dry_run: true,
            initialized: false,
            appended_synthetic_sample: false,
            sample_count: 0,
            layout,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let mut writer = TrainingShardWriter::create(&args.out_dir, manifest)
        .with_context(|| format!("initialize shard at {}", args.out_dir.display()))?;
    if args.append_synthetic_sample {
        append_synthetic_sample(&mut writer).context("append synthetic sample")?;
    }
    let manifest = writer.finish().context("finish shard")?;

    if args.append_synthetic_sample {
        let reader = TrainingShardReader::open(&args.out_dir).context("smoke-open shard")?;
        if reader.index().len() != 1 {
            bail!("smoke-open expected one index record");
        }
    }

    let report = ShardExportReport {
        shard_id: manifest.shard_id,
        out_dir: args.out_dir,
        dry_run: false,
        initialized: true,
        appended_synthetic_sample: args.append_synthetic_sample,
        sample_count: manifest.sample_count,
        layout,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn default_manifest(shard_id: &str) -> Result<TrainingShardManifest> {
    let tensors = vec![
        TrainingShardTensorSpec::f32_raw("hrrr_fields", "hrrr", vec![2, 4, 4])?,
        TrainingShardTensorSpec::f32_raw("mrms_fields", "mrms", vec![1, 4, 4])?,
        TrainingShardTensorSpec::f32_raw("goes_fields", "goes", vec![3, 4, 4])?,
        TrainingShardTensorSpec::f32_raw("radar_fields", "radar", vec![1, 4, 4])?,
    ];
    Ok(TrainingShardManifest::new(shard_id, tensors)?)
}

fn shard_layout(manifest: &TrainingShardManifest) -> Vec<String> {
    let mut layout = vec![
        TRAINING_SHARD_MANIFEST_FILE.to_string(),
        TRAINING_SHARD_INDEX_FILE.to_string(),
    ];
    for group in &manifest.source_groups {
        layout.push(group.blob_path.clone());
    }
    layout
}

fn append_synthetic_sample(writer: &mut TrainingShardWriter) -> Result<()> {
    let hrrr = synthetic_values(32, 100.0);
    let mrms = synthetic_values(16, 200.0);
    let goes = synthetic_values(48, 300.0);
    let radar = synthetic_values(16, 400.0);
    writer.append_sample(
        "synthetic-000",
        &[
            TrainingShardSampleTensor {
                name: "hrrr_fields",
                values: &hrrr,
            },
            TrainingShardSampleTensor {
                name: "mrms_fields",
                values: &mrms,
            },
            TrainingShardSampleTensor {
                name: "goes_fields",
                values: &goes,
            },
            TrainingShardSampleTensor {
                name: "radar_fields",
                values: &radar,
            },
        ],
    )?;
    Ok(())
}

fn synthetic_values(count: usize, base: f32) -> Vec<f32> {
    (0..count).map(|index| base + index as f32).collect()
}
