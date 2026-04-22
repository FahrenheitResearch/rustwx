use std::fs;
use std::path::PathBuf;

use clap::Parser;
use rustwx_products::comparison::load_and_compare_product_runs;
use rustwx_products::publication::atomic_write_json;

#[derive(Debug, Parser)]
#[command(
    name = "product-compare",
    about = "Compare two rustwx weather-product manifests/reports and emit machine-readable JSON deltas"
)]
struct Args {
    #[arg(long)]
    left: PathBuf,
    #[arg(long)]
    right: PathBuf,
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let comparison = load_and_compare_product_runs(&args.left, &args.right)?;

    if let Some(path) = args.out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        atomic_write_json(&path, &comparison)?;
        println!("{}", path.display());
    } else {
        println!("{}", serde_json::to_string_pretty(&comparison)?);
    }

    Ok(())
}
