use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use rustwx_core::{BundleRequirement, CanonicalBundleDescriptor, CycleSpec, ModelId, SourceId};
use rustwx_models::{
    LatestRun, latest_available_run_at_forecast_hour,
    latest_available_run_for_products_at_forecast_hour, model_summary,
};
use rustwx_products::cache::ensure_dir;
use rustwx_products::planner::{ExecutionPlan, ExecutionPlanBuilder};
use rustwx_products::runtime::{
    BundleLoaderConfig, decode_loaded_execution_plan, fetch_execution_plan,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "cache-warm",
    about = "Warm planner/runtime fetch+decode caches for a model run without rendering products"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,

    #[arg(long)]
    date: String,

    #[arg(long)]
    cycle: Option<u8>,

    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,

    #[arg(long)]
    source: Option<SourceId>,

    #[arg(long, value_enum, default_value_t = WarmPreset::Pair)]
    preset: WarmPreset,

    /// Explicit bundles to warm. If provided, overrides --preset.
    #[arg(long = "bundle", value_enum, value_delimiter = ',', num_args = 1..)]
    bundles: Vec<BundleArg>,

    /// Override the native product for the surface canonical bundle.
    #[arg(long)]
    surface_override: Option<String>,

    /// Override the native product for the pressure canonical bundle.
    #[arg(long)]
    pressure_override: Option<String>,

    /// Override the native product for the native-analysis canonical bundle.
    #[arg(long)]
    native_override: Option<String>,

    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof\\cache")]
    cache_dir: PathBuf,

    #[arg(long, default_value_t = false)]
    no_cache: bool,

    /// Emit machine-readable JSON to stdout instead of a human summary.
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum WarmPreset {
    Pair,
    Native,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BundleArg {
    Surface,
    Pressure,
    Native,
}

impl BundleArg {
    fn descriptor(self) -> CanonicalBundleDescriptor {
        match self {
            Self::Surface => CanonicalBundleDescriptor::SurfaceAnalysis,
            Self::Pressure => CanonicalBundleDescriptor::PressureAnalysis,
            Self::Native => CanonicalBundleDescriptor::NativeAnalysis,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct WarmBundleResult {
    bundle: String,
    native_product: String,
    fetched: bool,
    decoded: bool,
    fetch_failure: Option<String>,
    bundle_failure: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WarmReport {
    strategy: &'static str,
    model: ModelId,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    use_cache: bool,
    cache_dir: PathBuf,
    fetch_keys: usize,
    fetched_ok: usize,
    fetch_failures: usize,
    decoded_surface: usize,
    decoded_pressure: usize,
    total_ms: u128,
    fetch_ms_total: u128,
    decode_surface_ms_total: u128,
    decode_pressure_ms_total: u128,
    bundles: Vec<WarmBundleResult>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if !args.no_cache {
        ensure_dir(&args.cache_dir)?;
    }

    let latest = resolve_run(&args)?;
    let plan = build_plan(&args, &latest);
    let total_start = Instant::now();
    let report = warm_plan_with_loader(plan, &args.cache_dir, !args.no_cache)?;
    let total_ms = total_start.elapsed().as_millis();
    let report = WarmReport { total_ms, ..report };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&report);
    }
    Ok(())
}

fn resolve_run(args: &Args) -> Result<LatestRun, Box<dyn std::error::Error>> {
    let source = args
        .source
        .unwrap_or(model_summary(args.model).sources[0].id);

    if let Some(cycle_utc) = args.cycle {
        return Ok(LatestRun {
            model: args.model,
            cycle: CycleSpec::new(&args.date, cycle_utc)?,
            source,
        });
    }

    let required_products = required_products_for_latest_probe(args);
    if required_products.is_empty() {
        Ok(latest_available_run_at_forecast_hour(
            args.model,
            Some(source),
            &args.date,
            args.forecast_hour,
        )?)
    } else {
        let required_refs: Vec<&str> = required_products.iter().map(String::as_str).collect();
        Ok(latest_available_run_for_products_at_forecast_hour(
            args.model,
            Some(source),
            &args.date,
            &required_refs,
            args.forecast_hour,
        )?)
    }
}

fn required_products_for_latest_probe(args: &Args) -> Vec<String> {
    bundle_requests(args)
        .into_iter()
        .map(|bundle| match bundle {
            CanonicalBundleDescriptor::SurfaceAnalysis => override_for_bundle(args, bundle)
                .unwrap_or_else(|| default_product_for_bundle(args.model, bundle).to_string()),
            CanonicalBundleDescriptor::PressureAnalysis => override_for_bundle(args, bundle)
                .unwrap_or_else(|| default_product_for_bundle(args.model, bundle).to_string()),
            CanonicalBundleDescriptor::NativeAnalysis => override_for_bundle(args, bundle)
                .unwrap_or_else(|| default_product_for_bundle(args.model, bundle).to_string()),
        })
        .collect()
}

fn bundle_requests(args: &Args) -> Vec<CanonicalBundleDescriptor> {
    if !args.bundles.is_empty() {
        return args
            .bundles
            .iter()
            .map(|bundle| bundle.descriptor())
            .collect();
    }

    match args.preset {
        WarmPreset::Pair => vec![
            CanonicalBundleDescriptor::SurfaceAnalysis,
            CanonicalBundleDescriptor::PressureAnalysis,
        ],
        WarmPreset::Native => vec![CanonicalBundleDescriptor::NativeAnalysis],
        WarmPreset::All => vec![
            CanonicalBundleDescriptor::SurfaceAnalysis,
            CanonicalBundleDescriptor::PressureAnalysis,
            CanonicalBundleDescriptor::NativeAnalysis,
        ],
    }
}

fn build_plan(args: &Args, latest: &LatestRun) -> ExecutionPlan {
    let mut builder = ExecutionPlanBuilder::new(latest, args.forecast_hour);
    for bundle in bundle_requests(args) {
        let mut requirement = BundleRequirement::new(bundle, args.forecast_hour);
        if let Some(native_override) = override_for_bundle(args, bundle) {
            requirement = requirement.with_native_override(native_override);
        }
        builder.require(&requirement);
    }
    builder.build()
}

fn override_for_bundle(args: &Args, bundle: CanonicalBundleDescriptor) -> Option<String> {
    match bundle {
        CanonicalBundleDescriptor::SurfaceAnalysis => args.surface_override.clone(),
        CanonicalBundleDescriptor::PressureAnalysis => args.pressure_override.clone(),
        CanonicalBundleDescriptor::NativeAnalysis => args.native_override.clone(),
    }
}

fn default_product_for_bundle(model: ModelId, bundle: CanonicalBundleDescriptor) -> &'static str {
    match (model, bundle) {
        (ModelId::Hrrr, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc",
        (ModelId::Hrrr, CanonicalBundleDescriptor::PressureAnalysis) => "prs",
        (ModelId::Hrrr, CanonicalBundleDescriptor::NativeAnalysis) => "nat",
        (ModelId::Gfs, _) => "pgrb2.0p25",
        (ModelId::EcmwfOpenData, _) => "oper",
        (ModelId::RrfsA, CanonicalBundleDescriptor::SurfaceAnalysis) => "nat-na",
        (ModelId::RrfsA, CanonicalBundleDescriptor::PressureAnalysis) => "prs-na",
        (ModelId::RrfsA, CanonicalBundleDescriptor::NativeAnalysis) => "nat-na",
    }
}

/// Cache warm strategy: use the staged planner/runtime loader path to
/// materialize fetch + decode caches without rendering any products.
fn warm_plan_with_loader(
    plan: ExecutionPlan,
    cache_dir: &std::path::Path,
    use_cache: bool,
) -> Result<WarmReport, Box<dyn std::error::Error>> {
    let config = BundleLoaderConfig::new(cache_dir.to_path_buf(), use_cache);
    let fetched = fetch_execution_plan(plan, &config)?;
    let loaded = decode_loaded_execution_plan(fetched, &config)?;

    let mut bundles = Vec::with_capacity(loaded.plan.bundles.len());
    for bundle in &loaded.plan.bundles {
        let fetch_key = bundle.fetch_key();
        let fetch_failure = loaded.fetch_failure(&fetch_key).map(str::to_string);
        let bundle_failure = loaded.bundle_failure(&bundle.id).map(str::to_string);
        let decoded = match bundle.id.bundle {
            CanonicalBundleDescriptor::SurfaceAnalysis => {
                loaded.surface_decodes.contains_key(&bundle.id)
            }
            CanonicalBundleDescriptor::PressureAnalysis => {
                loaded.pressure_decodes.contains_key(&bundle.id)
            }
            CanonicalBundleDescriptor::NativeAnalysis => false,
        };
        bundles.push(WarmBundleResult {
            bundle: bundle.id.bundle.as_str().to_string(),
            native_product: bundle.id.native_product.clone(),
            fetched: loaded.fetched_for(bundle).is_some(),
            decoded,
            fetch_failure,
            bundle_failure,
        });
    }

    Ok(WarmReport {
        strategy: "planner_staged_loader",
        model: loaded.plan.model,
        date_yyyymmdd: loaded.plan.cycle.date_yyyymmdd.clone(),
        cycle_utc: loaded.plan.cycle.hour_utc,
        forecast_hour: loaded.forecast_hour,
        source: loaded.plan.source,
        use_cache,
        cache_dir: cache_dir.to_path_buf(),
        fetch_keys: loaded.plan.fetch_keys().len(),
        fetched_ok: loaded.fetched.len(),
        fetch_failures: loaded.fetch_failures.len(),
        decoded_surface: loaded.surface_decodes.len(),
        decoded_pressure: loaded.pressure_decodes.len(),
        total_ms: 0,
        fetch_ms_total: loaded.timing.fetch_ms_total,
        decode_surface_ms_total: loaded.timing.decode_surface_ms_total,
        decode_pressure_ms_total: loaded.timing.decode_pressure_ms_total,
        bundles,
    })
}

fn print_report(report: &WarmReport) {
    println!(
        "[cache-warm] strategy={} model={} date={} cycle={:02}z fh={:03} source={} cache={} use_cache={}",
        report.strategy,
        report.model,
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.source,
        report.cache_dir.display(),
        report.use_cache
    );
    println!(
        "[cache-warm] fetch_keys={} fetched_ok={} fetch_failures={} decoded_surface={} decoded_pressure={} total_ms={} fetch_ms_total={} decode_surface_ms_total={} decode_pressure_ms_total={}",
        report.fetch_keys,
        report.fetched_ok,
        report.fetch_failures,
        report.decoded_surface,
        report.decoded_pressure,
        report.total_ms,
        report.fetch_ms_total,
        report.decode_surface_ms_total,
        report.decode_pressure_ms_total
    );
    for bundle in &report.bundles {
        println!(
            "[bundle] {} native_product={} fetched={} decoded={} fetch_failure={} bundle_failure={}",
            bundle.bundle,
            bundle.native_product,
            bundle.fetched,
            bundle.decoded,
            bundle.fetch_failure.as_deref().unwrap_or("-"),
            bundle.bundle_failure.as_deref().unwrap_or("-"),
        );
    }
}
