use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use rustwx_products::gallery::{
    build_proof_gallery_index, load_gallery_catalog, load_proof_manifest, render_gallery_html,
};

#[derive(Debug, Parser)]
#[command(
    name = "proof-gallery",
    about = "Generate a static RustWX proof gallery from existing manifests and the product catalog"
)]
struct Args {
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    proof_root: PathBuf,
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(long)]
    catalog: Option<PathBuf>,
    #[arg(long, default_value = "RustWX Proof Gallery")]
    title: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| args.proof_root.join("viewer"));
    fs::create_dir_all(&out_dir)?;

    let catalog_path = args
        .catalog
        .clone()
        .unwrap_or_else(|| args.proof_root.join("product_catalog.json"));
    let catalog = if catalog_path.exists() {
        Some(load_gallery_catalog(&catalog_path)?)
    } else {
        None
    };

    let mut manifests = collect_manifest_paths(&args.proof_root)?;
    manifests.sort();

    let mut records = Vec::new();
    for path in manifests {
        if let Ok(record) = load_proof_manifest(&path) {
            records.push(record);
        }
    }

    let index = build_proof_gallery_index(
        &args.title,
        &args.proof_root,
        &out_dir,
        catalog.as_ref(),
        &records,
    );

    let index_json_path = out_dir.join("index.json");
    let index_html_path = out_dir.join("index.html");
    fs::write(&index_json_path, serde_json::to_vec_pretty(&index)?)?;
    fs::write(&index_html_path, render_gallery_html(&index))?;

    println!("{}", index_html_path.display());
    println!("{}", index_json_path.display());
    Ok(())
}

fn collect_manifest_paths(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut manifests = Vec::new();
    collect_manifest_paths_recursive(root, &mut manifests)?;
    let preferred_run_manifests = manifests
        .iter()
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?;
            name.strip_suffix("_run_manifest.json")
                .map(|stem| stem.to_string())
        })
        .collect::<HashSet<_>>();
    manifests.retain(|path| {
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            return false;
        };
        if name.ends_with("_run_manifest.json") {
            return true;
        }
        !preferred_run_manifests.iter().any(|stem| {
            name == format!("{stem}_manifest.json")
                || name == format!("{stem}_report.json")
                || name == format!("{stem}_batch_report.json")
                || name == format!("{stem}_windowed_report.json")
        })
    });
    Ok(manifests)
}

fn collect_manifest_paths_recursive(
    dir: &Path,
    manifests: &mut Vec<PathBuf>,
) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_manifest_paths_recursive(&path, manifests)?;
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let is_manifest = name.ends_with("_manifest.json")
            || name.ends_with("_run_manifest.json")
            || name.ends_with("_batch_report.json")
            || name.ends_with("_windowed_report.json");
        if is_manifest {
            manifests.push(path);
        }
    }
    Ok(())
}
