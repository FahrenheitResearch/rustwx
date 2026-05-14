use std::fs;
use std::path::PathBuf;

#[path = "../domain.rs"]
mod domain;
#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use domain::domain_from_region_or_country;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::grib_ensemble::{
    CompareOp, GribEnsembleRenderRequest, GribEnsembleStat, default_grib_member_products,
    expand_member_template, run_grib_ensemble_render,
};
use rustwx_products::places::{PlaceLabelDensityTier, default_place_label_overlay_for_domain};

#[cfg(feature = "cuda")]
struct CudaStatsGuard;

#[cfg(feature = "cuda")]
impl Drop for CudaStatsGuard {
    fn drop(&mut self) {
        rustwx_render::print_cuda_rasterize_stats_if_enabled();
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StatArg {
    Mean,
    Std,
    Min,
    Max,
    P10,
    P50,
    P90,
    ProbExceed,
}

impl From<StatArg> for GribEnsembleStat {
    fn from(value: StatArg) -> Self {
        match value {
            StatArg::Mean => Self::Mean,
            StatArg::Std => Self::Std,
            StatArg::Min => Self::Min,
            StatArg::Max => Self::Max,
            StatArg::P10 => Self::P10,
            StatArg::P50 => Self::P50,
            StatArg::P90 => Self::P90,
            StatArg::ProbExceed => Self::ProbExceed,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CompareArg {
    Gt,
    Ge,
    Lt,
    Le,
}

impl From<CompareArg> for CompareOp {
    fn from(value: CompareArg) -> Self {
        match value {
            CompareArg::Gt => Self::Gt,
            CompareArg::Ge => Self::Ge,
            CompareArg::Lt => Self::Lt,
            CompareArg::Le => Self::Le,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
}

impl From<PngCompressionArg> for rustwx_render::PngCompressionMode {
    fn from(value: PngCompressionArg) -> Self {
        match value {
            PngCompressionArg::Default => Self::Default,
            PngCompressionArg::Fast => Self::Fast,
            PngCompressionArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "grib-ensemble-reduce",
    about = "Fetch member GRIB files, reduce a direct map recipe, and render an ensemble-stat PNG"
)]
struct Args {
    #[arg(long, default_value = "gefs")]
    model: ModelId,
    #[arg(long, default_value = "20260502")]
    date: String,
    #[arg(long, default_value_t = 0)]
    cycle: u8,
    #[arg(long, default_value_t = 24)]
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, default_value = "2m_temperature_10m_winds")]
    recipe: String,
    #[arg(long, value_enum, default_value_t = StatArg::Mean)]
    stat: StatArg,
    #[arg(long)]
    threshold: Option<f32>,
    #[arg(long = "threshold-op", value_enum, default_value_t = CompareArg::Gt)]
    threshold_op: CompareArg,
    #[arg(long = "member-product", value_delimiter = ',', num_args = 0..)]
    member_products: Vec<String>,
    #[arg(long)]
    member_template: Option<String>,
    #[arg(long = "member", value_delimiter = ',', num_args = 0..)]
    members: Vec<String>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Conus)]
    region: RegionPreset,
    #[arg(long)]
    country: Option<String>,
    #[arg(long, default_value = "target\\grib_ensemble_reduce")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long = "place-label-density", default_value_t = 1, value_parser = clap::value_parser!(u8).range(0..=3))]
    place_label_density: u8,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "cuda")]
    let _cuda_stats_guard = CudaStatsGuard;

    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let member_products = resolve_member_products(&args)?;
    let domain = domain_from_region_or_country(args.region, args.country.as_deref())?;
    let request = GribEnsembleRenderRequest {
        model: args.model,
        date_yyyymmdd: args.date.clone(),
        cycle_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args
            .source
            .unwrap_or(rustwx_models::model_summary(args.model).sources[0].id),
        recipe_slug: args.recipe.clone(),
        member_products,
        stat: args.stat.into(),
        threshold: args.threshold,
        threshold_op: Some(args.threshold_op.into()),
        domain: domain.clone(),
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        output_width: static_output_dimension("RUSTWX_STATIC_OUTPUT_WIDTH", 1600),
        output_height: static_output_dimension("RUSTWX_STATIC_OUTPUT_HEIGHT", 900),
        png_compression: args.png_compression.into(),
        place_label_overlay: default_place_label_overlay_for_domain(
            &domain,
            PlaceLabelDensityTier::from_numeric(args.place_label_density),
        ),
    };
    let report = run_grib_ensemble_render(&request)?;
    let manifest_path = args.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_ensemble_{}_manifest.json",
        args.model.as_str().replace('-', "_"),
        args.date,
        args.cycle,
        args.forecast_hour,
        request.domain.slug,
        report.stat.slug()
    ));
    fs::write(&manifest_path, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", report.output_path.display());
    println!("{}", manifest_path.display());
    Ok(())
}

fn resolve_member_products(args: &Args) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if !args.member_products.is_empty() {
        return Ok(args.member_products.clone());
    }
    if let Some(template) = args.member_template.as_deref() {
        if args.members.is_empty() {
            return Err("--member-template requires at least one --member".into());
        }
        return Ok(expand_member_template(template, &args.members));
    }
    default_grib_member_products(args.model).ok_or_else(|| {
        format!(
            "no default member list is registered for {}; pass --member-product or --member-template",
            args.model
        )
        .into()
    })
}

fn static_output_dimension(name: &str, fallback: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value >= 320)
        .unwrap_or(fallback)
}
