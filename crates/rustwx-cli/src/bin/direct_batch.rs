use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::model_summary;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::direct::{
    DirectBatchRequest, run_direct_batch, supported_direct_recipe_slugs,
};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    canonical_run_slug, finalize_and_publish_run_manifest, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "direct-batch",
    about = "Generate multiple direct/native RustWX plots from one shared full-file fetch/extract pass"
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
    #[arg(long = "product-override", value_delimiter = ',', num_args = 0..)]
    product_overrides: Vec<String>,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Provisional slug used only if the run fails before a resolved
    // report is available; matches the success slug bit-for-bit when
    // `args.cycle` is already pinned, and falls back to a placeholder
    // cycle ("XX") when the latest run hasn't been resolved yet.
    let failure_slug = canonical_run_slug(
        &args.model.as_str().replace('-', "_"),
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.region.slug(),
        "direct",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "direct_batch",
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

    let source = args
        .source
        .unwrap_or(model_summary(args.model).sources[0].id);
    let recipes = if args.all_supported {
        let supported = supported_direct_recipe_slugs(args.model);
        if supported.is_empty() {
            return Err(format!(
                "no direct products are currently supported for {}",
                args.model
            )
            .into());
        }
        supported
    } else if args.recipes.is_empty() {
        return Err("pass at least one --recipe or use --all-supported".into());
    } else {
        args.recipes.clone()
    };
    let request = DirectBatchRequest {
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
        product_overrides: parse_product_overrides(&args.product_overrides)?,
    };
    let report = run_direct_batch(&request)?;

    let model_slug = report.model.as_str().replace('-', "_");
    let stem = format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_direct",
        model_slug,
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.domain.slug
    );
    let manifest_path = args.out_dir.join(format!("{stem}_manifest.json"));
    let timing_path = args.out_dir.join(format!("{stem}_timing.json"));
    atomic_write_json(&manifest_path, &report)?;
    atomic_write_json(
        &timing_path,
        &serde_json::json!({
            "model": report.model,
            "date": report.date_yyyymmdd,
            "cycle_utc": report.cycle_utc,
            "forecast_hour": report.forecast_hour,
            "source": report.source,
            "domain": report.domain,
            "fetches": report.fetches,
            "recipes": report.recipes.iter().map(|recipe| {
                serde_json::json!({
                    "recipe_slug": recipe.recipe_slug,
                    "planned_grib_product": recipe.grib_product,
                    "fetched_grib_product": recipe.fetched_grib_product,
                    "resolved_source": recipe.resolved_source,
                    "resolved_url": recipe.resolved_url,
                    "output_path": recipe.output_path,
                    "timing_ms": recipe.timing,
                })
            }).collect::<Vec<_>>(),
            "total_ms": report.total_ms,
        }),
    )?;
    let mut run_manifest =
        RunPublicationManifest::new("direct_batch", stem.clone(), args.out_dir.clone())
            .with_run_metadata(
                report.model.as_str(),
                report.date_yyyymmdd.clone(),
                report.cycle_utc,
                report.forecast_hour,
                report.source.as_str(),
                report.domain.slug.clone(),
            )
            .with_input_fetches(
                report
                    .fetches
                    .iter()
                    .map(|fetch| fetch.input_fetch.clone())
                    .collect(),
            )
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
    let (canonical_manifest, attempt_manifest) =
        finalize_and_publish_run_manifest(&mut run_manifest, &args.out_dir, &stem)?;

    for recipe in &report.recipes {
        println!("{}", recipe.output_path.display());
    }
    println!("{}", manifest_path.display());
    println!("{}", timing_path.display());
    println!("{}", canonical_manifest.display());
    println!("{}", attempt_manifest.display());
    Ok(())
}

fn parse_product_overrides(
    values: &[String],
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let mut parsed = HashMap::new();
    for value in values {
        let (planned, actual) = value.split_once('=').ok_or_else(|| {
            format!("invalid --product-override '{value}', expected planned=actual")
        })?;
        let planned = planned.trim();
        let actual = actual.trim();
        if planned.is_empty() || actual.is_empty() {
            return Err(format!(
                "invalid --product-override '{value}', expected non-empty planned=actual"
            )
            .into());
        }
        parsed.insert(planned.to_string(), actual.to_string());
    }
    Ok(parsed)
}

fn relative_output_path(root: &std::path::Path, output_path: &std::path::Path) -> PathBuf {
    output_path
        .strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| output_path.to_path_buf())
}
