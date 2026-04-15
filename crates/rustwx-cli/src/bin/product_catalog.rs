use std::fs;
use std::path::PathBuf;

use clap::Parser;
use rustwx_products::catalog::build_supported_products_catalog;

#[derive(Debug, Parser)]
#[command(
    name = "product-catalog",
    about = "Emit a JSON inventory of currently supported rustwx direct/derived/heavy/windowed products"
)]
struct Args {
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let catalog = build_supported_products_catalog();
    let bytes = serde_json::to_vec_pretty(&catalog)?;

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
