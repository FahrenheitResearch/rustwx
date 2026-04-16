use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::model_summary;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::{
    DerivedBatchRequest, run_derived_batch, supported_derived_recipe_slugs,
};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    default_run_manifest_path, publish_run_manifest,
};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "derived-batch",
    about = "Generate multiple derived RustWX plots from one shared full-file thermodynamic load"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long = "recipe", value_delimiter = ',', num_args = 1..)]
    recipes: Vec<String>,
    #[arg(long, default_value_t = false)]
    all_supported: bool,
    #[arg(long)]
    surface_product: Option<String>,
    #[arg(long)]
    pressure_product: Option<String>,
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

    let source = args
        .source
        .unwrap_or(model_summary(args.model).sources[0].id);
    let recipes = if args.all_supported {
        let supported = supported_derived_recipe_slugs(args.model);
        if supported.is_empty() {
            return Err(format!(
                "no derived products are currently supported for {}",
                args.model
            )
            .into());
        }
        supported
    } else if args.recipes.is_empty() {
        return Err("pass at least one --recipe or use --all-supported".into());
    } else {
        args.recipes
    };

    let request = DerivedBatchRequest {
        model: args.model,
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.clone(),
        use_cache: !args.no_cache,
        recipe_slugs: recipes,
        surface_product_override: args.surface_product.clone(),
        pressure_product_override: args.pressure_product.clone(),
    };
    let report = run_derived_batch(&request)?;

    let model_slug = report.model.as_str().replace('-', "_");
    let stem = format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_derived",
        model_slug,
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.domain.slug
    );
    let manifest_path = args.out_dir.join(format!("{stem}_manifest.json"));
    let timing_path = args.out_dir.join(format!("{stem}_timing.json"));
    let run_manifest_path = default_run_manifest_path(&args.out_dir, &stem);
    atomic_write_json(&manifest_path, &report)?;
    atomic_write_json(&timing_path, &report)?;
    let mut run_manifest =
        RunPublicationManifest::new("derived_batch", stem.clone(), args.out_dir.clone())
            .with_run_metadata(
                report.model.as_str(),
                report.date_yyyymmdd.clone(),
                report.cycle_utc,
                report.forecast_hour,
                format!("{:?}", report.source),
                report.domain.slug.clone(),
            )
            .with_input_fetches(report.input_fetches.clone())
            .with_artifacts(
                report
                    .recipes
                    .iter()
                    .map(|recipe| {
                        PublishedArtifactRecord::planned(
                            recipe.recipe_slug.clone(),
                            relative_output_path(&args.out_dir, &recipe.output_path),
                        )
                        .with_state(ArtifactPublicationState::Complete)
                        .with_content_identity(recipe.content_identity.clone())
                        .with_input_fetch_keys(recipe.input_fetch_keys.clone())
                    })
                    .collect(),
            );
    run_manifest.mark_complete();
    publish_run_manifest(&run_manifest_path, &run_manifest)?;

    for recipe in &report.recipes {
        println!("{}", recipe.output_path.display());
    }
    println!("{}", manifest_path.display());
    println!("{}", timing_path.display());
    println!("{}", run_manifest_path.display());
    Ok(())
}

fn relative_output_path(root: &std::path::Path, output_path: &std::path::Path) -> PathBuf {
    output_path
        .strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| output_path.to_path_buf())
}
