//! One-shot multi-model, multi-hour orchestrator.
//!
//! Calls every planner-driven lane (severe, ECAPE, direct, derived) for
//! each (model, forecast_hour) in the requested range, soft-failing
//! per-lane so one model's unavailability doesn't kill the others. The
//! goal is a single command that says "give me everything a severe
//! weather forecaster wants, for today, across every available model,
//! cropped to the midwest, going out 6 hours."
//!
//! Design intent:
//! - Every lane independently resolves its own latest run — if GFS is
//!   late publishing and HRRR is fresh, GFS skips and HRRR keeps going.
//! - Directly invokes the crate's lane entry points (`run_severe_batch`,
//!   etc.), so it shares cache + planner + partial-success behavior with
//!   the per-lane bins.
//! - Writes PNGs and a single summary JSON. The summary lists every
//!   attempted (model, fh, lane) with outcome + reason.

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::model_summary;
use rustwx_products::cache::ensure_dir;
use rustwx_products::derived::is_heavy_derived_recipe_slug;
use rustwx_products::ecape::{EcapeBatchRequest, run_ecape_batch};
use rustwx_products::heavy::{HeavyPanelHourRequest, run_heavy_panel_hour};
use rustwx_products::non_ecape::{
    HrrrNonEcapeHourRequest, NonEcapeHourRequest, run_hrrr_non_ecape_hour, run_model_non_ecape_hour,
};
use rustwx_products::severe::{SevereBatchRequest, run_severe_batch};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "forecast-now",
    about = "One-shot multi-model multi-hour orchestrator with per-lane soft-fail"
)]
struct Args {
    /// Comma-separated list of models (hrrr, gfs, ecmwf-open-data, rrfs-a).
    #[arg(long, value_delimiter = ',', default_value = "hrrr")]
    models: Vec<ModelId>,

    /// Forecast hours to request. Accepts either a single range "0-6" or a
    /// comma-separated list "0,3,6".
    #[arg(long, default_value = "0-6")]
    hours: String,

    /// Region crop(s). Comma-separated for multi-region runs
    /// (--regions=midwest,southeast,great_lakes).
    #[arg(long, value_delimiter = ',', default_value = "midwest")]
    regions: Vec<RegionPreset>,

    /// Date of the run in YYYYMMDD. Defaults to today (UTC).
    #[arg(long)]
    date: Option<String>,

    /// Optional cycle override (UTC hour). Defaults to per-model latest.
    #[arg(long)]
    cycle: Option<u8>,

    /// Source override (aws, nomads, etc.). Defaults to the model's
    /// primary source.
    #[arg(long)]
    source: Option<SourceId>,

    /// Output root. PNGs and summary JSON go here.
    #[arg(long)]
    out_dir: PathBuf,

    /// Shared cache root.
    #[arg(long)]
    cache_dir: PathBuf,

    /// Disable caching (forces re-fetch).
    #[arg(long, default_value_t = false)]
    no_cache: bool,

    /// Allow large-domain heavy diagnostics on wide crops like CONUS.
    #[arg(long, default_value_t = false)]
    allow_large_heavy_domain: bool,

    /// Number of outer forecast jobs to run concurrently. A job is one
    /// independent (region, model, forecast_hour) execution bundle.
    #[arg(long, default_value_t = 1)]
    job_concurrency: usize,

    /// Inner render thread cap used by the product runners. When set, this
    /// writes `RUSTWX_RENDER_THREADS` once before any worker threads start.
    #[arg(long)]
    render_threads: Option<usize>,

    /// Output image width for direct/native/non-ECAPE renders.
    #[arg(long, default_value_t = 1200)]
    width: u32,

    /// Output image height for direct/native/non-ECAPE renders.
    #[arg(long, default_value_t = 900)]
    height: u32,

    /// Skip direct lane.
    #[arg(long, default_value_t = false)]
    skip_direct: bool,
    /// Skip derived lane.
    #[arg(long, default_value_t = false)]
    skip_derived: bool,
    /// Skip severe lane.
    #[arg(long, default_value_t = false)]
    skip_severe: bool,
    /// Skip ECAPE lane.
    #[arg(long, default_value_t = false)]
    skip_ecape: bool,

    /// Comma-separated recipe slugs for the direct lane. Defaults to a
    /// curated severe-weather set.
    #[arg(long, value_delimiter = ',')]
    direct_recipes: Option<Vec<String>>,

    /// Comma-separated recipe slugs for the derived lane. Defaults to a
    /// curated severe-weather set.
    #[arg(long, value_delimiter = ',')]
    derived_recipes: Option<Vec<String>>,

    /// Use every supported direct + derived slug from the product
    /// catalog, ignoring --direct-recipes / --derived-recipes. Intended
    /// for benchmark runs ("every product of every model").
    #[arg(long, default_value_t = false)]
    all_supported: bool,

    /// Product source mode for derived/non-ECAPE execution.
    #[arg(long = "source-mode", alias = "thermo-path", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,

    /// Route policy for direct+derived non-ECAPE work.
    ///
    /// `auto` uses the unified non-ECAPE path for every supported model.
    /// `unified` forces the generic non-ECAPE path for non-HRRR models.
    /// `split` forces per-lane direct+derived execution for non-HRRR models.
    #[arg(long, value_enum, default_value_t = RoutePolicyArg::Auto)]
    route_policy: RoutePolicyArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceModeArg {
    Canonical,
    Fastest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum RoutePolicyArg {
    Auto,
    Unified,
    Split,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RouteSelection {
    HrrrUnified,
    Unified,
    Split,
}

impl From<SourceModeArg> for ProductSourceMode {
    fn from(value: SourceModeArg) -> Self {
        match value {
            SourceModeArg::Canonical => Self::Canonical,
            SourceModeArg::Fastest => Self::Fastest,
        }
    }
}

fn default_direct_recipes() -> Vec<String> {
    // Curated severe-weather forecaster set. Slugs match the
    // product_catalog. Recipes that aren't supported on a given model
    // (e.g. composite_reflectivity on GFS) degrade per-recipe via the
    // DirectRecipeBlocker path — the rest of the recipes still render.
    vec![
        "composite_reflectivity",
        "2m_temperature_10m_winds",
        "2m_dewpoint_10m_winds",
        "2m_relative_humidity",
        "500mb_height_winds",
        "700mb_height_winds",
        "850mb_height_winds",
        "500mb_rh_height_winds",
        "700mb_temperature_height_winds",
        "850mb_temperature_height_winds",
        "mslp_10m_winds",
        "precipitable_water",
        "10m_wind_gusts",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_derived_recipes() -> Vec<String> {
    // Severe-weather forecaster staples derived from the surface +
    // pressure bundle. Every slug here is 'supported' in the
    // product_catalog; derived_batch currently errors out on the first
    // unsupported slug (will be softened in a later pass), so keep
    // this list to genuinely-available products.
    vec![
        "sbcape",
        "mlcape",
        "mucape",
        "sbcin",
        "mlcin",
        "bulk_shear_0_6km",
        "bulk_shear_0_1km",
        "srh_0_1km",
        "srh_0_3km",
        "ehi_0_1km",
        "ehi_0_3km",
        "stp_fixed",
        "scp_mu_0_3km_0_6km_proxy",
        "lapse_rate_700_500",
        "lapse_rate_0_3km",
        "theta_e_2m_10m_winds",
        "lifted_index",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn forecast_now_required_products(model: ModelId, args: &Args) -> Vec<&'static str> {
    if !matches!(model, ModelId::RrfsA) {
        return Vec::new();
    }
    let mut products = Vec::new();
    if !args.skip_direct {
        products.push("prs-conus");
    }
    if !args.skip_severe || !args.skip_ecape || !args.skip_derived {
        products.push("nat-na");
        products.push("prs-na");
    }
    products
}

#[derive(Debug, Clone, Copy, Serialize)]
enum Lane {
    Severe,
    Ecape,
    Direct,
    Derived,
}

impl Lane {
    fn slug(self) -> &'static str {
        match self {
            Lane::Severe => "severe",
            Lane::Ecape => "ecape",
            Lane::Direct => "direct",
            Lane::Derived => "derived",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct LaneOutcome {
    #[serde(default)]
    region: String,
    model: ModelId,
    forecast_hour: u16,
    lane: String,
    route_selected: RouteSelection,
    run_date_yyyymmdd: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_cycle_utc: Option<u8>,
    run_source: SourceId,
    pin_resolution: PinResolution,
    ok: bool,
    duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    outputs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    blockers: Vec<String>,
}

fn lane_outcome_from_pinned(
    pinned: &PinnedRunRequest,
    model: ModelId,
    forecast_hour: u16,
    lane: &str,
    route_selected: RouteSelection,
    ok: bool,
    duration_ms: u128,
    error: Option<String>,
    outputs: Vec<String>,
    blockers: Vec<String>,
) -> LaneOutcome {
    LaneOutcome {
        region: String::new(),
        model,
        forecast_hour,
        lane: lane.to_string(),
        route_selected,
        run_date_yyyymmdd: pinned.date_yyyymmdd.clone(),
        run_cycle_utc: pinned.cycle_override_utc,
        run_source: pinned.source,
        pin_resolution: pinned.resolution,
        ok,
        duration_ms,
        error,
        outputs,
        blockers,
    }
}

fn merge_counts(dst: &mut ModelCounts, src: &ModelCounts) {
    dst.succeeded += src.succeeded;
    dst.failed += src.failed;
    dst.blocked_recipes += src.blocked_recipes;
    dst.outputs += src.outputs;
}

fn run_forecast_job(job: ForecastJob, config: &ExecConfig) -> ForecastJobResult {
    let mut counts = ModelCounts::default();
    let mut outcomes = Vec::new();

    if matches!(job.model, ModelId::Hrrr) {
        let mut hrrr_outcomes = run_hrrr_unified(
            &job.pinned,
            job.forecast_hour,
            &job.domain,
            config,
            &job.direct_recipes,
            &job.derived_recipes,
            &mut counts,
        );
        for outcome in &mut hrrr_outcomes {
            outcome.region = job.region_slug.clone();
        }
        outcomes.extend(hrrr_outcomes);
    } else {
        if !config.skip_severe {
            let mut outcome = run_severe_lane(
                job.model,
                &job.pinned,
                job.forecast_hour,
                &job.domain,
                config,
                &mut counts,
            );
            outcome.region = job.region_slug.clone();
            outcomes.push(outcome);
        }
        if !config.skip_ecape {
            let mut outcome = run_ecape_lane(
                job.model,
                &job.pinned,
                job.forecast_hour,
                &job.domain,
                config,
                &mut counts,
            );
            outcome.region = job.region_slug.clone();
            outcomes.push(outcome);
        }
        let want_non_hrrr_non_ecape = (!config.skip_direct && !job.direct_recipes.is_empty())
            || (!config.skip_derived && !job.derived_recipes.is_empty());
        let non_hrrr_route = select_non_hrrr_non_ecape_route(job.model, config.route_policy);
        if want_non_hrrr_non_ecape
            && matches!(non_hrrr_route, RouteSelection::Unified)
            && supports_unified_non_hrrr_non_ecape(job.model)
        {
            let mut outcome = run_non_hrrr_non_ecape_hour(
                job.model,
                &job.pinned,
                job.forecast_hour,
                &job.domain,
                config,
                &job.direct_recipes,
                &job.derived_recipes,
                &mut counts,
            );
            outcome.region = job.region_slug.clone();
            outcomes.push(outcome);
        } else {
            if !config.skip_direct {
                let mut outcome = run_direct_lane(
                    job.model,
                    &job.pinned,
                    job.forecast_hour,
                    &job.domain,
                    config,
                    &job.direct_recipes,
                    &mut counts,
                );
                outcome.region = job.region_slug.clone();
                outcomes.push(outcome);
            }
            if !config.skip_derived {
                let mut outcome = run_derived_lane(
                    job.model,
                    &job.pinned,
                    job.forecast_hour,
                    &job.domain,
                    config,
                    &job.derived_recipes,
                    &mut counts,
                );
                outcome.region = job.region_slug.clone();
                outcomes.push(outcome);
            }
        }
    }

    ForecastJobResult {
        model_key: format!("{}:{}", job.region_slug, job.model),
        counts,
        outcomes,
    }
}

/// Global union of every 'supported' or 'partial' direct + derived
/// slug in the catalog. Used by --all-supported when we want one list
/// for summary display; per-model filtering happens in
/// `model_supported_recipe_lists`.
fn all_supported_recipe_lists() -> (Vec<String>, Vec<String>) {
    use rustwx_products::catalog::{ProductCatalogStatus, build_supported_products_catalog};
    let catalog = build_supported_products_catalog();
    let include = |status: ProductCatalogStatus| {
        matches!(
            status,
            ProductCatalogStatus::Supported | ProductCatalogStatus::Partial
        )
    };
    let direct: Vec<String> = catalog
        .direct
        .iter()
        .filter(|e| include(e.status))
        .map(|e| e.slug.clone())
        .collect();
    let derived: Vec<String> = catalog
        .derived
        .iter()
        .filter(|e| include(e.status))
        .map(|e| e.slug.clone())
        .collect();
    (direct, derived)
}

/// Per-model supported recipe lists. The product catalog records a
/// `ProductTargetSupport` per (recipe, model), so we can ask "which
/// slugs does HRRR actually render today?" and hand derived_batch a
/// list it won't reject. Without this filter, --all-supported would
/// include slugs that are supported by *some* model (via the rollup
/// status) but not the specific model we're invoking — derived_batch
/// currently errors on the first unsupported slug, so per-model
/// filtering keeps the benchmark honest.
fn model_supported_recipe_lists(model: ModelId) -> (Vec<String>, Vec<String>) {
    use rustwx_products::catalog::{ProductTargetStatus, build_supported_products_catalog};
    let catalog = build_supported_products_catalog();
    let supported_for_model = |support: &[rustwx_products::catalog::ProductTargetSupport]| {
        support
            .iter()
            .any(|s| s.model == Some(model) && matches!(s.status, ProductTargetStatus::Supported))
    };
    let direct: Vec<String> = catalog
        .direct
        .iter()
        .filter(|e| supported_for_model(&e.support))
        .map(|e| e.slug.clone())
        .collect();
    let derived: Vec<String> = catalog
        .derived
        .iter()
        .filter(|e| supported_for_model(&e.support))
        .map(|e| e.slug.clone())
        .collect();
    (direct, derived)
}

fn filter_recipes_for_model(requested: &[String], supported: &[String]) -> Vec<String> {
    requested
        .iter()
        .filter(|slug| supported.iter().any(|candidate| candidate == *slug))
        .cloned()
        .collect()
}

fn filter_heavy_derived_recipes(recipes: Vec<String>, skip_ecape: bool) -> Vec<String> {
    if !skip_ecape {
        return recipes;
    }
    recipes
        .into_iter()
        .filter(|slug| !is_heavy_derived_recipe_slug(slug))
        .collect()
}

fn select_non_hrrr_non_ecape_route(model: ModelId, policy: RoutePolicyArg) -> RouteSelection {
    if matches!(model, ModelId::Hrrr) {
        return RouteSelection::HrrrUnified;
    }
    match policy {
        RoutePolicyArg::Auto | RoutePolicyArg::Unified => RouteSelection::Unified,
        RoutePolicyArg::Split => RouteSelection::Split,
    }
}

fn supports_unified_non_hrrr_non_ecape(model: ModelId) -> bool {
    matches!(
        model,
        ModelId::RrfsA | ModelId::Gfs | ModelId::EcmwfOpenData | ModelId::WrfGdex
    )
}

#[derive(Debug, Serialize)]
struct RunSummary {
    started_utc: String,
    finished_utc: String,
    wall_clock_ms: u128,
    regions: Vec<String>,
    date_yyyymmdd: String,
    cycle_override_utc: Option<u8>,
    models: Vec<ModelId>,
    hours: Vec<u16>,
    allow_large_heavy_domain: bool,
    direct_recipes: Vec<String>,
    derived_recipes: Vec<String>,
    route_policy: RoutePolicyArg,
    outcomes: Vec<LaneOutcome>,
    counts_by_model: BTreeMap<String, ModelCounts>,
    resolved_runs_by_model: BTreeMap<String, ResolvedRunSummary>,
}

#[derive(Debug, Default, Serialize)]
struct ModelCounts {
    succeeded: usize,
    failed: usize,
    blocked_recipes: usize,
    outputs: usize,
}

#[derive(Debug, Clone)]
struct PinnedRunRequest {
    date_yyyymmdd: String,
    cycle_override_utc: Option<u8>,
    source: SourceId,
    resolution: PinResolution,
}

#[derive(Debug, Clone)]
struct ExecConfig {
    out_dir: PathBuf,
    cache_dir: PathBuf,
    no_cache: bool,
    allow_large_heavy_domain: bool,
    skip_severe: bool,
    skip_ecape: bool,
    skip_direct: bool,
    skip_derived: bool,
    source_mode: ProductSourceMode,
    route_policy: RoutePolicyArg,
    output_width: u32,
    output_height: u32,
}

#[derive(Debug, Clone)]
struct ForecastJob {
    region_slug: String,
    domain: DomainSpec,
    model: ModelId,
    forecast_hour: u16,
    pinned: PinnedRunRequest,
    direct_recipes: Vec<String>,
    derived_recipes: Vec<String>,
}

#[derive(Debug)]
struct ForecastJobResult {
    model_key: String,
    counts: ModelCounts,
    outcomes: Vec<LaneOutcome>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum PinResolution {
    RequestedOverride,
    AutoLatest,
    UnresolvedFallback,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedRunSummary {
    run_date_yyyymmdd: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_cycle_utc: Option<u8>,
    run_source: SourceId,
    pin_resolution: PinResolution,
}

impl From<&PinnedRunRequest> for ResolvedRunSummary {
    fn from(value: &PinnedRunRequest) -> Self {
        Self {
            run_date_yyyymmdd: value.date_yyyymmdd.clone(),
            run_cycle_utc: value.cycle_override_utc,
            run_source: value.source,
            pin_resolution: value.resolution,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let run_start = Instant::now();
    let started_utc = utc_timestamp();

    let date = args.date.clone().unwrap_or_else(today_utc_yyyymmdd);
    let hours = parse_hours(&args.hours)?;

    fs::create_dir_all(&args.out_dir)?;
    if !args.no_cache {
        ensure_dir(&args.cache_dir)?;
    }
    if let Some(render_threads) = args.render_threads.filter(|&value| value > 0) {
        // Set once before any worker threads start so the inner runners can
        // pick up a fixed render-thread budget.
        unsafe {
            std::env::set_var("RUSTWX_RENDER_THREADS", render_threads.to_string());
        }
    }

    let (direct_recipes, derived_recipes) = if args.all_supported {
        all_supported_recipe_lists()
    } else {
        (
            args.direct_recipes
                .clone()
                .unwrap_or_else(default_direct_recipes),
            args.derived_recipes
                .clone()
                .unwrap_or_else(default_derived_recipes),
        )
    };
    let derived_recipes = filter_heavy_derived_recipes(derived_recipes, args.skip_ecape);

    println!(
        "[forecast-now] date={date} regions={:?} hours={:?} models={:?} direct={} derived={} route_policy={:?} allow_large_heavy_domain={} size={}x{} job_concurrency={} render_threads={:?}",
        args.regions.iter().map(|r| r.slug()).collect::<Vec<_>>(),
        hours,
        args.models,
        direct_recipes.len(),
        derived_recipes.len(),
        args.route_policy,
        args.allow_large_heavy_domain,
        args.width,
        args.height,
        args.job_concurrency,
        args.render_threads,
    );

    let pin_forecast_hour = hours.iter().copied().max().unwrap_or(0);
    let mut pinned_runs_by_model = BTreeMap::<String, PinnedRunRequest>::new();
    for &model in &args.models {
        let source = args.source.unwrap_or(model_summary(model).sources[0].id);
        let pinned_request = if let Some(cycle_override_utc) = args.cycle {
            PinnedRunRequest {
                date_yyyymmdd: date.clone(),
                cycle_override_utc: Some(cycle_override_utc),
                source,
                resolution: PinResolution::RequestedOverride,
            }
        } else {
            let required_products = forecast_now_required_products(model, &args);
            let latest = if required_products.is_empty() {
                rustwx_models::latest_available_run_at_forecast_hour(
                    model,
                    Some(source),
                    &date,
                    pin_forecast_hour,
                )
            } else {
                rustwx_models::latest_available_run_for_products_at_forecast_hour(
                    model,
                    Some(source),
                    &date,
                    &required_products,
                    pin_forecast_hour,
                )
            };
            match latest {
                Ok(run) => {
                    println!(
                        "[cycle] {model}: pinned to {:02}z ({}) via {:?}",
                        run.cycle.hour_utc, run.cycle.date_yyyymmdd, run.source
                    );
                    PinnedRunRequest {
                        date_yyyymmdd: run.cycle.date_yyyymmdd,
                        cycle_override_utc: Some(run.cycle.hour_utc),
                        source: run.source,
                        resolution: PinResolution::AutoLatest,
                    }
                }
                Err(err) => {
                    eprintln!("[cycle] {model}: latest-run resolve failed: {err}");
                    PinnedRunRequest {
                        date_yyyymmdd: date.clone(),
                        cycle_override_utc: None,
                        source,
                        resolution: PinResolution::UnresolvedFallback,
                    }
                }
            }
        };
        pinned_runs_by_model.insert(model.to_string(), pinned_request);
    }

    let config = ExecConfig {
        out_dir: args.out_dir.clone(),
        cache_dir: args.cache_dir.clone(),
        no_cache: args.no_cache,
        allow_large_heavy_domain: args.allow_large_heavy_domain,
        skip_severe: args.skip_severe,
        skip_ecape: args.skip_ecape,
        skip_direct: args.skip_direct,
        skip_derived: args.skip_derived,
        source_mode: args.source_mode.into(),
        route_policy: args.route_policy,
        output_width: args.width,
        output_height: args.height,
    };

    let mut jobs = Vec::<ForecastJob>::new();
    for &region in &args.regions {
        let domain = DomainSpec::new(region.slug(), region.bounds());
        println!("\n=== region: {} ===", region.slug());
        for &model in &args.models {
            let pinned_request = pinned_runs_by_model
                .get(&model.to_string())
                .expect("model pin should be computed before execution");

            let (direct_for_model, derived_for_model) = if args.all_supported {
                let (supported_direct, supported_derived) = model_supported_recipe_lists(model);
                (
                    supported_direct,
                    filter_heavy_derived_recipes(supported_derived, args.skip_ecape),
                )
            } else {
                let (supported_direct, supported_derived) = model_supported_recipe_lists(model);
                let direct_for_model = if args.direct_recipes.is_some() {
                    direct_recipes.clone()
                } else {
                    filter_recipes_for_model(&direct_recipes, &supported_direct)
                };
                let derived_for_model = if args.derived_recipes.is_some() {
                    derived_recipes.clone()
                } else {
                    filter_recipes_for_model(&derived_recipes, &supported_derived)
                };
                (direct_for_model, derived_for_model)
            };

            for &fh in &hours {
                jobs.push(ForecastJob {
                    region_slug: region.slug().to_string(),
                    domain: domain.clone(),
                    model,
                    forecast_hour: fh,
                    pinned: pinned_request.clone(),
                    direct_recipes: direct_for_model.clone(),
                    derived_recipes: derived_for_model.clone(),
                });
            }
        }
    }

    let mut outcomes = Vec::<LaneOutcome>::new();
    let mut counts_by_model: BTreeMap<String, ModelCounts> = BTreeMap::new();
    let job_count = jobs.len();
    let worker_count = args.job_concurrency.max(1).min(job_count.max(1));
    if worker_count <= 1 || job_count <= 1 {
        for job in jobs {
            let result = run_forecast_job(job, &config);
            merge_counts(
                counts_by_model.entry(result.model_key).or_default(),
                &result.counts,
            );
            outcomes.extend(result.outcomes);
        }
    } else {
        let queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
        let config = Arc::new(config);
        let (tx, rx) = mpsc::channel::<ForecastJobResult>();
        let mut handles = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let queue = Arc::clone(&queue);
            let config = Arc::clone(&config);
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                loop {
                    let job = {
                        let mut queue = queue.lock().expect("forecast job queue poisoned");
                        queue.pop_front()
                    };
                    let Some(job) = job else {
                        break;
                    };
                    let result = run_forecast_job(job, &config);
                    if tx.send(result).is_err() {
                        break;
                    }
                }
            }));
        }
        drop(tx);

        for result in rx {
            merge_counts(
                counts_by_model.entry(result.model_key).or_default(),
                &result.counts,
            );
            outcomes.extend(result.outcomes);
        }

        for handle in handles {
            handle
                .join()
                .map_err(|_| std::io::Error::other("forecast_now worker thread panicked"))?;
        }
    }
    outcomes.sort_by(|a, b| {
        a.region
            .cmp(&b.region)
            .then_with(|| a.model.to_string().cmp(&b.model.to_string()))
            .then_with(|| a.forecast_hour.cmp(&b.forecast_hour))
            .then_with(|| a.lane.cmp(&b.lane))
    });

    let finished_utc = utc_timestamp();
    let wall_clock_ms = run_start.elapsed().as_millis();

    let summary = RunSummary {
        started_utc,
        finished_utc,
        wall_clock_ms,
        regions: args.regions.iter().map(|r| r.slug().to_string()).collect(),
        date_yyyymmdd: date.clone(),
        cycle_override_utc: args.cycle,
        models: args.models.clone(),
        hours: hours.clone(),
        allow_large_heavy_domain: args.allow_large_heavy_domain,
        direct_recipes,
        derived_recipes,
        route_policy: args.route_policy,
        outcomes,
        counts_by_model,
        resolved_runs_by_model: pinned_runs_by_model
            .iter()
            .map(|(model, pinned)| (model.clone(), ResolvedRunSummary::from(pinned)))
            .collect(),
    };

    let summary_path = args
        .out_dir
        .join(format!("forecast_now_summary_{date}.json"));
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;

    let ok_count = summary.outcomes.iter().filter(|o| o.ok).count();
    let fail_count = summary.outcomes.len() - ok_count;
    let total_outputs: usize = summary.outcomes.iter().map(|o| o.outputs.len()).sum();
    println!(
        "\n[forecast-now] done in {} ms — {} ok, {} failed, {} png(s), summary: {}",
        wall_clock_ms,
        ok_count,
        fail_count,
        total_outputs,
        summary_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        PinResolution, PinnedRunRequest, RoutePolicyArg, RouteSelection,
        filter_heavy_derived_recipes, select_non_hrrr_non_ecape_route,
        supports_unified_non_hrrr_non_ecape,
    };
    use rustwx_core::{ModelId, SourceId};

    #[test]
    fn pinned_request_uses_resolved_cycle_date() {
        let pinned = PinnedRunRequest {
            date_yyyymmdd: "20260417".to_string(),
            cycle_override_utc: Some(12),
            source: SourceId::Aws,
            resolution: PinResolution::AutoLatest,
        };
        assert_eq!(pinned.date_yyyymmdd, "20260417");
        assert_eq!(pinned.cycle_override_utc, Some(12));
    }

    #[test]
    fn skip_ecape_filters_heavy_derived_recipes() {
        let recipes = vec![
            "sbcape".to_string(),
            "sbecape".to_string(),
            "stp_fixed".to_string(),
        ];
        let filtered = filter_heavy_derived_recipes(recipes, true);
        assert_eq!(
            filtered,
            vec!["sbcape".to_string(), "stp_fixed".to_string()]
        );
    }

    #[test]
    fn auto_route_uses_unified_non_hrrr_path() {
        assert_eq!(
            select_non_hrrr_non_ecape_route(ModelId::Gfs, RoutePolicyArg::Auto),
            RouteSelection::Unified
        );
        assert_eq!(
            select_non_hrrr_non_ecape_route(ModelId::EcmwfOpenData, RoutePolicyArg::Auto),
            RouteSelection::Unified
        );
        assert_eq!(
            select_non_hrrr_non_ecape_route(ModelId::WrfGdex, RoutePolicyArg::Auto),
            RouteSelection::Unified
        );
    }

    #[test]
    fn wrf_gdex_supports_unified_non_ecape_runner() {
        assert!(supports_unified_non_hrrr_non_ecape(ModelId::WrfGdex));
        assert!(!supports_unified_non_hrrr_non_ecape(ModelId::Hrrr));
    }
}

fn parse_hours(spec: &str) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
    let trimmed = spec.trim();
    if let Some((lo, hi)) = trimmed.split_once('-') {
        let lo: u16 = lo.trim().parse()?;
        let hi: u16 = hi.trim().parse()?;
        if hi < lo {
            return Err(format!("hours range hi < lo: {spec}").into());
        }
        return Ok((lo..=hi).collect());
    }
    let mut hours = Vec::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        hours.push(part.parse::<u16>()?);
    }
    if hours.is_empty() {
        return Err(format!("no hours parsed from '{spec}'").into());
    }
    hours.sort();
    hours.dedup();
    Ok(hours)
}

fn today_utc_yyyymmdd() -> String {
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days_since_epoch = secs / 86_400;
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    format!("{year:04}{month:02}{day:02}")
}

fn utc_timestamp() -> String {
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days_since_epoch = secs / 86_400;
    let seconds_in_day = secs % 86_400;
    let hour = seconds_in_day / 3600;
    let minute = (seconds_in_day % 3600) / 60;
    let second = seconds_in_day % 60;
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Convert days since 1970-01-01 to (year, month, day) using Howard
/// Hinnant's civil_from_days algorithm. No chrono dependency.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y } as i32;
    (year, m, d)
}

fn run_severe_lane(
    model: ModelId,
    pinned: &PinnedRunRequest,
    fh: u16,
    domain: &DomainSpec,
    config: &ExecConfig,
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = SevereBatchRequest {
        model,
        date_yyyymmdd: pinned.date_yyyymmdd.clone(),
        cycle_override_utc: pinned.cycle_override_utc,
        forecast_hour: fh,
        source: pinned.source,
        domain: domain.clone(),
        out_dir: config.out_dir.clone(),
        cache_root: config.cache_dir.clone(),
        use_cache: !config.no_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: config.allow_large_heavy_domain,
    };
    let slug = Lane::Severe.slug();
    match run_severe_batch(&request) {
        Ok(report) => {
            let outputs: Vec<String> = report
                .outputs
                .iter()
                .map(|output| output.output_path.to_string_lossy().to_string())
                .collect();
            println!("[ok  ] {model} f{fh:03} {slug}: {} png", outputs.len());
            counts.succeeded += 1;
            counts.outputs += outputs.len();
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                true,
                start.elapsed().as_millis(),
                None,
                outputs,
                Vec::new(),
            )
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                false,
                start.elapsed().as_millis(),
                Some(err.to_string()),
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

fn run_ecape_lane(
    model: ModelId,
    pinned: &PinnedRunRequest,
    fh: u16,
    domain: &DomainSpec,
    config: &ExecConfig,
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = EcapeBatchRequest {
        model,
        date_yyyymmdd: pinned.date_yyyymmdd.clone(),
        cycle_override_utc: pinned.cycle_override_utc,
        forecast_hour: fh,
        source: pinned.source,
        domain: domain.clone(),
        out_dir: config.out_dir.clone(),
        cache_root: config.cache_dir.clone(),
        use_cache: !config.no_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: config.allow_large_heavy_domain,
    };
    let slug = Lane::Ecape.slug();
    match run_ecape_batch(&request) {
        Ok(report) => {
            let outputs: Vec<String> = report
                .outputs
                .iter()
                .map(|output| output.output_path.to_string_lossy().to_string())
                .collect();
            println!("[ok  ] {model} f{fh:03} {slug}: {} png", outputs.len());
            counts.succeeded += 1;
            counts.outputs += outputs.len();
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                true,
                start.elapsed().as_millis(),
                None,
                outputs,
                Vec::new(),
            )
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                false,
                start.elapsed().as_millis(),
                Some(err.to_string()),
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

fn run_non_hrrr_non_ecape_hour(
    model: ModelId,
    pinned: &PinnedRunRequest,
    fh: u16,
    domain: &DomainSpec,
    config: &ExecConfig,
    direct_recipes: &[String],
    derived_recipes: &[String],
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let direct_recipe_slugs = if config.skip_direct {
        Vec::new()
    } else {
        direct_recipes.to_vec()
    };
    let derived_recipe_slugs = if config.skip_derived {
        Vec::new()
    } else {
        derived_recipes.to_vec()
    };
    let slug = format!("{}_non_ecape_hour", model.as_str().replace('-', "_"));

    let request = NonEcapeHourRequest {
        model,
        date_yyyymmdd: pinned.date_yyyymmdd.clone(),
        cycle_override_utc: pinned.cycle_override_utc,
        forecast_hour: fh,
        source: pinned.source,
        domain: domain.clone(),
        out_dir: config.out_dir.clone(),
        cache_root: config.cache_dir.clone(),
        use_cache: !config.no_cache,
        source_mode: config.source_mode,
        direct_recipe_slugs,
        derived_recipe_slugs,
        allow_large_heavy_domain: config.allow_large_heavy_domain,
        windowed_products: Vec::new(),
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: rustwx_render::PngCompressionMode::Default,
    };

    match run_model_non_ecape_hour(&request) {
        Ok(report) => {
            let outputs: Vec<String> = report
                .summary
                .output_paths
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect();
            let mut blockers = Vec::new();
            if let Some(direct) = &report.direct {
                blockers.extend(
                    direct
                        .blockers
                        .iter()
                        .map(|b| format!("{}: {}", b.recipe_slug, b.reason)),
                );
            }
            if let Some(derived) = &report.derived {
                blockers.extend(derived.blockers.iter().map(|b| {
                    format!(
                        "{} [{}]: {}",
                        b.recipe_slug,
                        b.source_route.as_str(),
                        b.reason
                    )
                }));
            }
            let dur = start.elapsed().as_millis();
            println!(
                "[ok  ] {model} f{fh:03} {slug}: {} png, {} blocker(s) in {:.2}s",
                outputs.len(),
                blockers.len(),
                dur as f64 / 1000.0
            );
            counts.outputs += outputs.len();
            counts.blocked_recipes += blockers.len();
            if blockers.is_empty() || !outputs.is_empty() {
                counts.succeeded += 1;
            } else {
                counts.failed += 1;
            }
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                &slug,
                RouteSelection::Unified,
                !outputs.is_empty() || blockers.is_empty(),
                dur,
                None,
                outputs,
                blockers,
            )
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                &slug,
                RouteSelection::Unified,
                false,
                start.elapsed().as_millis(),
                Some(err.to_string()),
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

fn run_direct_lane(
    model: ModelId,
    pinned: &PinnedRunRequest,
    fh: u16,
    domain: &DomainSpec,
    config: &ExecConfig,
    recipes: &[String],
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = rustwx_products::direct::DirectBatchRequest {
        model,
        date_yyyymmdd: pinned.date_yyyymmdd.clone(),
        cycle_override_utc: pinned.cycle_override_utc,
        forecast_hour: fh,
        source: pinned.source,
        domain: domain.clone(),
        out_dir: config.out_dir.clone(),
        cache_root: config.cache_dir.clone(),
        use_cache: !config.no_cache,
        recipe_slugs: recipes.to_vec(),
        product_overrides: std::collections::HashMap::new(),
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: rustwx_render::PngCompressionMode::Default,
    };
    let slug = Lane::Direct.slug();
    match rustwx_products::direct::run_direct_batch(&request) {
        Ok(report) => {
            let outputs: Vec<String> = report
                .recipes
                .iter()
                .map(|r| r.output_path.to_string_lossy().to_string())
                .collect();
            let blockers: Vec<String> = report
                .blockers
                .iter()
                .map(|b| format!("{}: {}", b.recipe_slug, b.reason))
                .collect();
            counts.outputs += outputs.len();
            counts.blocked_recipes += blockers.len();
            if blockers.is_empty() {
                counts.succeeded += 1;
            } else if outputs.is_empty() {
                counts.failed += 1;
            } else {
                counts.succeeded += 1;
            }
            println!(
                "[ok  ] {model} f{fh:03} {slug}: {} png, {} blocker(s)",
                outputs.len(),
                blockers.len()
            );
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                !outputs.is_empty() || blockers.is_empty(),
                start.elapsed().as_millis(),
                None,
                outputs,
                blockers,
            )
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                false,
                start.elapsed().as_millis(),
                Some(err.to_string()),
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

fn run_derived_lane(
    model: ModelId,
    pinned: &PinnedRunRequest,
    fh: u16,
    domain: &DomainSpec,
    config: &ExecConfig,
    recipes: &[String],
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = rustwx_products::derived::DerivedBatchRequest {
        model,
        date_yyyymmdd: pinned.date_yyyymmdd.clone(),
        cycle_override_utc: pinned.cycle_override_utc,
        forecast_hour: fh,
        source: pinned.source,
        domain: domain.clone(),
        out_dir: config.out_dir.clone(),
        cache_root: config.cache_dir.clone(),
        use_cache: !config.no_cache,
        recipe_slugs: recipes.to_vec(),
        surface_product_override: None,
        pressure_product_override: None,
        source_mode: config.source_mode,
        allow_large_heavy_domain: config.allow_large_heavy_domain,
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: rustwx_render::PngCompressionMode::Default,
    };
    let slug = Lane::Derived.slug();
    match rustwx_products::derived::run_derived_batch(&request) {
        Ok(report) => {
            let outputs: Vec<String> = report
                .recipes
                .iter()
                .map(|r| r.output_path.to_string_lossy().to_string())
                .collect();
            let blockers: Vec<String> = report
                .blockers
                .iter()
                .map(|b| {
                    format!(
                        "{} [{}]: {}",
                        b.recipe_slug,
                        b.source_route.as_str(),
                        b.reason
                    )
                })
                .collect();
            counts.outputs += outputs.len();
            counts.blocked_recipes += blockers.len();
            if blockers.is_empty() || !outputs.is_empty() {
                counts.succeeded += 1;
            } else {
                counts.failed += 1;
            }
            println!(
                "[ok  ] {model} f{fh:03} {slug}: {} png, {} blocker(s)",
                outputs.len(),
                blockers.len()
            );
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                !outputs.is_empty() || blockers.is_empty(),
                start.elapsed().as_millis(),
                None,
                outputs,
                blockers,
            )
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            lane_outcome_from_pinned(
                pinned,
                model,
                fh,
                slug,
                RouteSelection::Split,
                false,
                start.elapsed().as_millis(),
                Some(err.to_string()),
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

/// HRRR-specific unified runner that reuses the optimized wrappers:
///   * `run_heavy_panel_hour` shares one surface+pressure bundle load +
///     one `prepare_heavy_volume` pass across the severe and ECAPE map
///     families.
///   * `run_hrrr_non_ecape_hour` shares one bundle load across direct,
///     derived, and windowed (windowed is skipped at f000 because the
///     accumulation windows aren't populated yet).
///
/// Falling back to the generic per-lane runners for HRRR forces four
/// separate `load_execution_plan` calls and three redundant
/// `prepare_heavy_volume` passes, which is why forecast_now was ~10×
/// slower than the checked-in HRRR baselines. Non-HRRR models follow
/// the explicit route policy handled in `run_forecast_job`.
fn run_hrrr_unified(
    pinned: &PinnedRunRequest,
    fh: u16,
    domain: &DomainSpec,
    config: &ExecConfig,
    direct_recipes: &[String],
    derived_recipes: &[String],
    counts: &mut ModelCounts,
) -> Vec<LaneOutcome> {
    let mut outcomes = Vec::new();

    // severe + ECAPE via one shared heavy-hour pass
    if !config.skip_severe || !config.skip_ecape {
        let start = Instant::now();
        let request = HeavyPanelHourRequest {
            model: ModelId::Hrrr,
            date_yyyymmdd: pinned.date_yyyymmdd.clone(),
            cycle_override_utc: pinned.cycle_override_utc,
            forecast_hour: fh,
            source: pinned.source,
            domain: domain.clone(),
            out_dir: config.out_dir.clone(),
            cache_root: config.cache_dir.clone(),
            use_cache: !config.no_cache,
            surface_product_override: None,
            pressure_product_override: None,
            allow_large_heavy_domain: config.allow_large_heavy_domain,
        };
        let slug = if !config.skip_severe && !config.skip_ecape {
            "hrrr_heavy_hour"
        } else if !config.skip_severe {
            "hrrr_heavy_hour_severe"
        } else {
            "hrrr_heavy_hour_ecape"
        };
        match run_heavy_panel_hour(&request) {
            Ok(report) => {
                let mut outputs = Vec::new();
                if !config.skip_severe {
                    outputs.extend(
                        report
                            .severe
                            .outputs
                            .iter()
                            .map(|p| p.output_path.to_string_lossy().to_string()),
                    );
                }
                if !config.skip_ecape {
                    outputs.extend(
                        report
                            .ecape
                            .outputs
                            .iter()
                            .map(|p| p.output_path.to_string_lossy().to_string()),
                    );
                }
                let dur = start.elapsed().as_millis();
                println!(
                    "[ok  ] hrrr f{fh:03} {slug}: {} png in {:.2}s",
                    outputs.len(),
                    dur as f64 / 1000.0
                );
                counts.succeeded += 1;
                counts.outputs += outputs.len();
                outcomes.push(lane_outcome_from_pinned(
                    pinned,
                    ModelId::Hrrr,
                    fh,
                    slug,
                    RouteSelection::HrrrUnified,
                    true,
                    dur,
                    None,
                    outputs,
                    Vec::new(),
                ));
            }
            Err(err) => {
                eprintln!("[fail] hrrr f{fh:03} {slug}: {err}");
                counts.failed += 1;
                outcomes.push(lane_outcome_from_pinned(
                    pinned,
                    ModelId::Hrrr,
                    fh,
                    slug,
                    RouteSelection::HrrrUnified,
                    false,
                    start.elapsed().as_millis(),
                    Some(err.to_string()),
                    Vec::new(),
                    Vec::new(),
                ));
            }
        }
    }

    // direct + derived via run_hrrr_non_ecape_hour (shared bundle load)
    let want_direct = !config.skip_direct && !direct_recipes.is_empty();
    let want_derived = !config.skip_derived && !derived_recipes.is_empty();
    if want_direct || want_derived {
        let start = Instant::now();
        let request = HrrrNonEcapeHourRequest {
            date_yyyymmdd: pinned.date_yyyymmdd.clone(),
            cycle_override_utc: pinned.cycle_override_utc,
            forecast_hour: fh,
            source: pinned.source,
            domain: domain.clone(),
            out_dir: config.out_dir.clone(),
            cache_root: config.cache_dir.clone(),
            use_cache: !config.no_cache,
            direct_recipe_slugs: if want_direct {
                direct_recipes.to_vec()
            } else {
                Vec::new()
            },
            derived_recipe_slugs: if want_derived {
                derived_recipes.to_vec()
            } else {
                Vec::new()
            },
            windowed_products: Vec::new(),
            source_mode: config.source_mode,
            output_width: config.output_width,
            output_height: config.output_height,
            png_compression: rustwx_render::PngCompressionMode::Default,
        };
        match run_hrrr_non_ecape_hour(&request) {
            Ok(report) => {
                let outputs: Vec<String> = report
                    .summary
                    .output_paths
                    .iter()
                    .map(|p| p.to_string_lossy().to_string())
                    .collect();
                let mut blockers = Vec::new();
                if let Some(direct) = &report.direct {
                    blockers.extend(
                        direct
                            .blockers
                            .iter()
                            .map(|b| format!("{}: {}", b.recipe_slug, b.reason)),
                    );
                }
                if let Some(derived) = &report.derived {
                    blockers.extend(derived.blockers.iter().map(|b| {
                        format!(
                            "{} [{}]: {}",
                            b.recipe_slug,
                            b.source_route.as_str(),
                            b.reason
                        )
                    }));
                }
                if let Some(windowed) = &report.windowed {
                    blockers.extend(
                        windowed
                            .blockers
                            .iter()
                            .map(|b| format!("{}: {}", b.product.slug(), b.reason)),
                    );
                }
                let dur = start.elapsed().as_millis();
                println!(
                    "[ok  ] hrrr f{fh:03} hrrr_non_ecape_hour: {} png, {} blocker(s) in {:.2}s",
                    outputs.len(),
                    blockers.len(),
                    dur as f64 / 1000.0
                );
                counts.outputs += outputs.len();
                counts.blocked_recipes += blockers.len();
                if blockers.is_empty() || !outputs.is_empty() {
                    counts.succeeded += 1;
                } else {
                    counts.failed += 1;
                }
                outcomes.push(lane_outcome_from_pinned(
                    pinned,
                    ModelId::Hrrr,
                    fh,
                    "hrrr_non_ecape_hour",
                    RouteSelection::HrrrUnified,
                    !outputs.is_empty() || blockers.is_empty(),
                    dur,
                    None,
                    outputs,
                    blockers,
                ));
            }
            Err(err) => {
                eprintln!("[fail] hrrr f{fh:03} hrrr_non_ecape_hour: {err}");
                counts.failed += 1;
                outcomes.push(lane_outcome_from_pinned(
                    pinned,
                    ModelId::Hrrr,
                    fh,
                    "hrrr_non_ecape_hour",
                    RouteSelection::HrrrUnified,
                    false,
                    start.elapsed().as_millis(),
                    Some(err.to_string()),
                    Vec::new(),
                    Vec::new(),
                ));
            }
        }
    }

    outcomes
}
