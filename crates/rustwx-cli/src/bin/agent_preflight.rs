use std::fs;
use std::path::PathBuf;

use clap::Parser;
use rustwx_core::ModelId;
use rustwx_products::agent_backend::{AgentPreflightRequest, build_agent_preflight};

#[derive(Debug, Parser)]
#[command(
    name = "agent-preflight",
    about = "Emit a machine-readable RustWx product capability and execution-lane preflight for agent apps"
)]
struct Args {
    #[arg(long, help = "Optional model filter, e.g. hrrr, gfs, nbm, rrfs-a")]
    model: Option<ModelId>,
    #[arg(long = "product", value_delimiter = ',', num_args = 0..)]
    products: Vec<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Emit every catalog entry even when --product filters are supplied"
    )]
    all: bool,
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let report = build_agent_preflight(AgentPreflightRequest {
        model: args.model,
        include_all_catalog_entries: args.all || args.products.is_empty(),
        products: args.products,
    });
    let bytes = serde_json::to_vec_pretty(&report)?;

    if let Some(path) = args.out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::write(&path, &bytes)?;
        println!("{}", path.display());
    } else {
        println!("{}", String::from_utf8(bytes)?);
    }

    Ok(())
}
