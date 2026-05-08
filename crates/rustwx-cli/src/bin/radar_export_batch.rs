use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use rustwx_radar::{RadarBatchRequest, plan_batch_requests};

#[derive(Parser)]
#[command(
    name = "radar_export_batch",
    about = "Plan batched NEXRAD Level-II tensor exports with volume/product dedupe"
)]
struct Cli {
    #[arg(long)]
    requests_jsonl: PathBuf,

    #[arg(long)]
    out_dir: PathBuf,

    #[arg(long)]
    cache_dir: PathBuf,

    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if !cli.dry_run {
        anyhow::bail!("radar_export_batch currently supports planning only; rerun with --dry-run");
    }

    std::fs::create_dir_all(&cli.out_dir)
        .with_context(|| format!("create {}", cli.out_dir.display()))?;
    std::fs::create_dir_all(&cli.cache_dir)
        .with_context(|| format!("create {}", cli.cache_dir.display()))?;

    let requests = read_requests_jsonl(&cli.requests_jsonl)?;
    let manifest = plan_batch_requests(&requests)?;
    let manifest_path = cli.out_dir.join("radar_export_batch_manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
        .with_context(|| format!("write {}", manifest_path.display()))?;

    eprintln!(
        "planned {} requests into {} volume/product/grid groups",
        manifest.resolved_product_request_count, manifest.group_count
    );
    println!("{}", manifest_path.display());
    Ok(())
}

fn read_requests_jsonl(path: &PathBuf) -> anyhow::Result<Vec<RadarBatchRequest>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut requests = Vec::new();
    for (line_index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read line {}", line_index + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request = serde_json::from_str::<RadarBatchRequest>(trimmed)
            .with_context(|| format!("parse {} line {}", path.display(), line_index + 1))?;
        requests.push(request);
    }
    if requests.is_empty() {
        anyhow::bail!("{} did not contain any batch requests", path.display());
    }
    Ok(requests)
}
