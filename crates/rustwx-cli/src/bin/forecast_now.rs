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

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::model_summary;
use rustwx_products::cache::ensure_dir;
use rustwx_products::derived::{DerivedBatchRequest, run_derived_batch};
use rustwx_products::direct::{DirectBatchRequest, run_direct_batch};
use rustwx_products::ecape::{EcapeBatchRequest, run_ecape_batch};
use rustwx_products::hrrr::{HrrrBatchProduct, HrrrBatchRequest, run_hrrr_batch};
use rustwx_products::non_ecape::{HrrrNonEcapeHourRequest, run_hrrr_non_ecape_hour};
use rustwx_products::severe::{SevereBatchRequest, run_severe_batch};
use rustwx_products::shared_context::DomainSpec;
use serde::Serialize;
use std::collections::HashMap;

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
    ok: bool,
    duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    outputs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    blockers: Vec<String>,
}

fn annotate_region(outcomes: &mut Vec<LaneOutcome>, mut outcome: LaneOutcome, region: RegionPreset) {
    outcome.region = region.slug().to_string();
    outcomes.push(outcome);
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
    use rustwx_products::catalog::{
        ProductTargetStatus, build_supported_products_catalog,
    };
    let catalog = build_supported_products_catalog();
    let supported_for_model = |support: &[rustwx_products::catalog::ProductTargetSupport]| {
        support.iter().any(|s| {
            s.model == Some(model) && matches!(s.status, ProductTargetStatus::Supported)
        })
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
    direct_recipes: Vec<String>,
    derived_recipes: Vec<String>,
    outcomes: Vec<LaneOutcome>,
    counts_by_model: BTreeMap<String, ModelCounts>,
}

#[derive(Debug, Default, Serialize)]
struct ModelCounts {
    succeeded: usize,
    failed: usize,
    blocked_recipes: usize,
    outputs: usize,
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

    println!(
        "[forecast-now] date={date} regions={:?} hours={:?} models={:?} direct={} derived={}",
        args.regions.iter().map(|r| r.slug()).collect::<Vec<_>>(),
        hours,
        args.models,
        direct_recipes.len(),
        derived_recipes.len(),
    );

    let mut outcomes = Vec::<LaneOutcome>::new();
    let mut counts_by_model: BTreeMap<String, ModelCounts> = BTreeMap::new();

    for &region in &args.regions {
        let domain = DomainSpec::new(region.slug(), region.bounds());
        println!("\n=== region: {} ===", region.slug());
        for &model in &args.models {
            let counts = counts_by_model
                .entry(format!("{}:{}", region.slug(), model))
                .or_default();
            let source = args
                .source
                .unwrap_or(model_summary(model).sources[0].id);

            // Per-model recipe selection for --all-supported so derived
            // doesn't abort on a slug that's Supported in the catalog's
            // rollup but Blocked for this specific model.
            let (direct_for_model, derived_for_model) = if args.all_supported {
                model_supported_recipe_lists(model)
            } else {
                (direct_recipes.clone(), derived_recipes.clone())
            };

            // Pin the cycle ONCE per model so severe/ECAPE/direct/derived
            // all use the same run. Without this each per-lane batch
            // re-runs latest_available_run(), and if a new cycle publishes
            // mid-run (e.g. 18z severe, 19z ECAPE) every subsequent lane
            // invalidates the fetch cache and does a full wrfsfc+wrfprs
            // re-download + re-decode — the HRRR midwest bench was taking
            // 30-40 min per model because of this.
            let pinned_cycle: Option<u8> = args.cycle.or_else(|| {
                match rustwx_models::latest_available_run(model, Some(source), &date) {
                    Ok(run) => {
                        println!("[cycle] {model}: pinned to {:02}z ({})", run.cycle.hour_utc, run.cycle.date_yyyymmdd);
                        Some(run.cycle.hour_utc)
                    }
                    Err(err) => {
                        eprintln!("[cycle] {model}: latest-run resolve failed: {err}");
                        None
                    }
                }
            });

            for &fh in &hours {
                // HRRR has optimized unified runners that share the
                // planner-loaded surface+pressure bundle and the
                // prepare_heavy_volume pass across severe+ECAPE, and
                // share a single bundle load across direct+derived.
                // Falling back to the generic per-lane runners for HRRR
                // forces 4 separate bundle loads and 3 redundant
                // prepare_heavy_volume passes, which is the original
                // "full CONUS HRRR used to run in ~60s, now takes
                // minutes" regression. For HRRR we call the unified
                // runners; for GFS/ECMWF/RRFS-A we still use the
                // generic per-lane runners (no unified runner exists
                // for those yet).
                if matches!(model, ModelId::Hrrr) {
                    let hrrr_outcomes = run_hrrr_unified(
                        &date,
                        pinned_cycle,
                        fh,
                        source,
                        &domain,
                        &args,
                        &direct_for_model,
                        &derived_for_model,
                        counts,
                    );
                    for outcome in hrrr_outcomes {
                        annotate_region(&mut outcomes, outcome, region);
                    }
                    continue;
                }

                if !args.skip_severe {
                    let outcome = run_severe_lane(
                        model, &date, pinned_cycle, fh, source, &domain, &args, counts,
                    );
                    annotate_region(&mut outcomes, outcome, region);
                }
                if !args.skip_ecape {
                    let outcome = run_ecape_lane(
                        model, &date, pinned_cycle, fh, source, &domain, &args, counts,
                    );
                    annotate_region(&mut outcomes, outcome, region);
                }
                if !args.skip_direct {
                    let outcome = run_direct_lane(
                        model,
                        &date,
                        pinned_cycle,
                        fh,
                        source,
                        &domain,
                        &args,
                        &direct_for_model,
                        counts,
                    );
                    annotate_region(&mut outcomes, outcome, region);
                }
                if !args.skip_derived {
                    let outcome = run_derived_lane(
                        model,
                        &date,
                        pinned_cycle,
                        fh,
                        source,
                        &domain,
                        &args,
                        &derived_for_model,
                        counts,
                    );
                    annotate_region(&mut outcomes, outcome, region);
                }
            }
        }
    }

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
        direct_recipes,
        derived_recipes,
        outcomes,
        counts_by_model,
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
    date: &str,
    cycle: Option<u8>,
    fh: u16,
    source: SourceId,
    domain: &DomainSpec,
    args: &Args,
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = SevereBatchRequest {
        model,
        date_yyyymmdd: date.to_string(),
        cycle_override_utc: cycle,
        forecast_hour: fh,
        source,
        domain: domain.clone(),
        out_dir: args.out_dir.clone(),
        cache_root: args.cache_dir.clone(),
        use_cache: !args.no_cache,
        surface_product_override: None,
        pressure_product_override: None,
    };
    let slug = Lane::Severe.slug();
    match run_severe_batch(&request) {
        Ok(report) => {
            let png = report.output_path.to_string_lossy().to_string();
            println!("[ok  ] {model} f{fh:03} {slug}: {png}");
            counts.succeeded += 1;
            counts.outputs += 1;
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: true,
                duration_ms: start.elapsed().as_millis(),
                error: None,
                outputs: vec![png],
                blockers: Vec::new(),
            }
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: false,
                duration_ms: start.elapsed().as_millis(),
                error: Some(err.to_string()),
                outputs: Vec::new(),
                blockers: Vec::new(),
            }
        }
    }
}

fn run_ecape_lane(
    model: ModelId,
    date: &str,
    cycle: Option<u8>,
    fh: u16,
    source: SourceId,
    domain: &DomainSpec,
    args: &Args,
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = EcapeBatchRequest {
        model,
        date_yyyymmdd: date.to_string(),
        cycle_override_utc: cycle,
        forecast_hour: fh,
        source,
        domain: domain.clone(),
        out_dir: args.out_dir.clone(),
        cache_root: args.cache_dir.clone(),
        use_cache: !args.no_cache,
        surface_product_override: None,
        pressure_product_override: None,
    };
    let slug = Lane::Ecape.slug();
    match run_ecape_batch(&request) {
        Ok(report) => {
            let png = report.output_path.to_string_lossy().to_string();
            println!("[ok  ] {model} f{fh:03} {slug}: {png}");
            counts.succeeded += 1;
            counts.outputs += 1;
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: true,
                duration_ms: start.elapsed().as_millis(),
                error: None,
                outputs: vec![png],
                blockers: Vec::new(),
            }
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: false,
                duration_ms: start.elapsed().as_millis(),
                error: Some(err.to_string()),
                outputs: Vec::new(),
                blockers: Vec::new(),
            }
        }
    }
}

fn run_direct_lane(
    model: ModelId,
    date: &str,
    cycle: Option<u8>,
    fh: u16,
    source: SourceId,
    domain: &DomainSpec,
    args: &Args,
    recipes: &[String],
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = DirectBatchRequest {
        model,
        date_yyyymmdd: date.to_string(),
        cycle_override_utc: cycle,
        forecast_hour: fh,
        source,
        domain: domain.clone(),
        out_dir: args.out_dir.clone(),
        cache_root: args.cache_dir.clone(),
        use_cache: !args.no_cache,
        recipe_slugs: recipes.to_vec(),
        product_overrides: HashMap::new(),
    };
    let slug = Lane::Direct.slug();
    match run_direct_batch(&request) {
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
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: !outputs.is_empty() || blockers.is_empty(),
                duration_ms: start.elapsed().as_millis(),
                error: None,
                outputs,
                blockers,
            }
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: false,
                duration_ms: start.elapsed().as_millis(),
                error: Some(err.to_string()),
                outputs: Vec::new(),
                blockers: Vec::new(),
            }
        }
    }
}

fn run_derived_lane(
    model: ModelId,
    date: &str,
    cycle: Option<u8>,
    fh: u16,
    source: SourceId,
    domain: &DomainSpec,
    args: &Args,
    recipes: &[String],
    counts: &mut ModelCounts,
) -> LaneOutcome {
    let start = Instant::now();
    let request = DerivedBatchRequest {
        model,
        date_yyyymmdd: date.to_string(),
        cycle_override_utc: cycle,
        forecast_hour: fh,
        source,
        domain: domain.clone(),
        out_dir: args.out_dir.clone(),
        cache_root: args.cache_dir.clone(),
        use_cache: !args.no_cache,
        recipe_slugs: recipes.to_vec(),
        surface_product_override: None,
        pressure_product_override: None,
    };
    let slug = Lane::Derived.slug();
    match run_derived_batch(&request) {
        Ok(report) => {
            let outputs: Vec<String> = report
                .recipes
                .iter()
                .map(|r| r.output_path.to_string_lossy().to_string())
                .collect();
            counts.outputs += outputs.len();
            counts.succeeded += 1;
            println!(
                "[ok  ] {model} f{fh:03} {slug}: {} png",
                outputs.len()
            );
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: true,
                duration_ms: start.elapsed().as_millis(),
                error: None,
                outputs,
                blockers: Vec::new(),
            }
        }
        Err(err) => {
            eprintln!("[fail] {model} f{fh:03} {slug}: {err}");
            counts.failed += 1;
            LaneOutcome {
                region: String::new(),
                model,
                forecast_hour: fh,
                lane: slug.to_string(),
                ok: false,
                duration_ms: start.elapsed().as_millis(),
                error: Some(err.to_string()),
                outputs: Vec::new(),
                blockers: Vec::new(),
            }
        }
    }
}

/// HRRR-specific unified runner that reuses the existing optimized
/// wrappers:
///   * `run_hrrr_batch` shares one surface+pressure bundle load + one
///     `prepare_heavy_volume` pass across the severe panel and the
///     ECAPE8 panel.
///   * `run_hrrr_non_ecape_hour` shares one bundle load across direct,
///     derived, and windowed (windowed is skipped at f000 because the
///     accumulation windows aren't populated yet).
///
/// Falling back to the generic per-lane runners for HRRR forces four
/// separate `load_execution_plan` calls and three redundant
/// `prepare_heavy_volume` passes, which is why forecast_now was ~10×
/// slower than the checked-in HRRR baselines. GFS/ECMWF/RRFS-A still
/// route through the per-lane runners (no unified variant exists for
/// them yet).
fn run_hrrr_unified(
    date: &str,
    cycle: Option<u8>,
    fh: u16,
    source: SourceId,
    domain: &DomainSpec,
    args: &Args,
    direct_recipes: &[String],
    derived_recipes: &[String],
    counts: &mut ModelCounts,
) -> Vec<LaneOutcome> {
    let mut outcomes = Vec::new();

    // severe + ECAPE via run_hrrr_batch (shared bundle + shared heavy volume)
    let mut products = Vec::<HrrrBatchProduct>::new();
    if !args.skip_severe {
        products.push(HrrrBatchProduct::SevereProofPanel);
    }
    if !args.skip_ecape {
        products.push(HrrrBatchProduct::Ecape8Panel);
    }
    if !products.is_empty() {
        let start = Instant::now();
        let request = HrrrBatchRequest {
            date_yyyymmdd: date.to_string(),
            cycle_override_utc: cycle,
            forecast_hour: fh,
            source,
            domain: domain.clone(),
            out_dir: args.out_dir.clone(),
            cache_root: args.cache_dir.clone(),
            use_cache: !args.no_cache,
            products,
        };
        let slug = if !args.skip_severe && !args.skip_ecape {
            "hrrr_batch_severe_ecape"
        } else if !args.skip_severe {
            "hrrr_batch_severe"
        } else {
            "hrrr_batch_ecape"
        };
        match run_hrrr_batch(&request) {
            Ok(report) => {
                let outputs: Vec<String> = report
                    .products
                    .iter()
                    .map(|p| p.output_path.to_string_lossy().to_string())
                    .collect();
                let dur = start.elapsed().as_millis();
                println!(
                    "[ok  ] hrrr f{fh:03} {slug}: {} png in {:.2}s",
                    outputs.len(),
                    dur as f64 / 1000.0
                );
                counts.succeeded += 1;
                counts.outputs += outputs.len();
                outcomes.push(LaneOutcome {
                    region: String::new(),
                    model: ModelId::Hrrr,
                    forecast_hour: fh,
                    lane: slug.to_string(),
                    ok: true,
                    duration_ms: dur,
                    error: None,
                    outputs,
                    blockers: Vec::new(),
                });
            }
            Err(err) => {
                eprintln!("[fail] hrrr f{fh:03} {slug}: {err}");
                counts.failed += 1;
                outcomes.push(LaneOutcome {
                    region: String::new(),
                    model: ModelId::Hrrr,
                    forecast_hour: fh,
                    lane: slug.to_string(),
                    ok: false,
                    duration_ms: start.elapsed().as_millis(),
                    error: Some(err.to_string()),
                    outputs: Vec::new(),
                    blockers: Vec::new(),
                });
            }
        }
    }

    // direct + derived via run_hrrr_non_ecape_hour (shared bundle load)
    let want_direct = !args.skip_direct && !direct_recipes.is_empty();
    let want_derived = !args.skip_derived && !derived_recipes.is_empty();
    if want_direct || want_derived {
        let start = Instant::now();
        let request = HrrrNonEcapeHourRequest {
            date_yyyymmdd: date.to_string(),
            cycle_override_utc: cycle,
            forecast_hour: fh,
            source,
            domain: domain.clone(),
            out_dir: args.out_dir.clone(),
            cache_root: args.cache_dir.clone(),
            use_cache: !args.no_cache,
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
        };
        match run_hrrr_non_ecape_hour(&request) {
            Ok(report) => {
                let outputs: Vec<String> = report
                    .summary
                    .output_paths
                    .iter()
                    .map(|p| p.to_string_lossy().to_string())
                    .collect();
                let dur = start.elapsed().as_millis();
                println!(
                    "[ok  ] hrrr f{fh:03} hrrr_non_ecape_hour: {} png in {:.2}s",
                    outputs.len(),
                    dur as f64 / 1000.0
                );
                counts.succeeded += 1;
                counts.outputs += outputs.len();
                outcomes.push(LaneOutcome {
                    region: String::new(),
                    model: ModelId::Hrrr,
                    forecast_hour: fh,
                    lane: "hrrr_non_ecape_hour".to_string(),
                    ok: true,
                    duration_ms: dur,
                    error: None,
                    outputs,
                    blockers: Vec::new(),
                });
            }
            Err(err) => {
                eprintln!("[fail] hrrr f{fh:03} hrrr_non_ecape_hour: {err}");
                counts.failed += 1;
                outcomes.push(LaneOutcome {
                    region: String::new(),
                    model: ModelId::Hrrr,
                    forecast_hour: fh,
                    lane: "hrrr_non_ecape_hour".to_string(),
                    ok: false,
                    duration_ms: start.elapsed().as_millis(),
                    error: Some(err.to_string()),
                    outputs: Vec::new(),
                    blockers: Vec::new(),
                });
            }
        }
    }

    outcomes
}
