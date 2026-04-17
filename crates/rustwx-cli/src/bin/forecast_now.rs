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

    /// Region crop.
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,

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
}

fn default_direct_recipes() -> Vec<String> {
    vec![
        "radar_reflectivity",
        "2m_temperature",
        "2m_dewpoint",
        "2m_relative_humidity",
        "2m_temperature_10m_winds",
        "500mb_height_winds",
        "700mb_height_winds",
        "850mb_height_winds",
        "precipitable_water",
        "mean_sea_level_pressure",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_derived_recipes() -> Vec<String> {
    vec![
        "2m_dewpoint_depression",
        "2m_theta_e_10m_winds",
        "bulk_shear_0_6km",
        "500mb_absolute_vorticity",
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

#[derive(Debug, Serialize)]
struct RunSummary {
    started_utc: String,
    finished_utc: String,
    wall_clock_ms: u128,
    region: String,
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
    let domain = DomainSpec::new(args.region.slug(), args.region.bounds());

    fs::create_dir_all(&args.out_dir)?;
    if !args.no_cache {
        ensure_dir(&args.cache_dir)?;
    }

    let direct_recipes = args
        .direct_recipes
        .clone()
        .unwrap_or_else(default_direct_recipes);
    let derived_recipes = args
        .derived_recipes
        .clone()
        .unwrap_or_else(default_derived_recipes);

    println!(
        "[forecast-now] date={date} region={} hours={:?} models={:?}",
        args.region.slug(),
        hours,
        args.models
    );

    let mut outcomes = Vec::<LaneOutcome>::new();
    let mut counts_by_model: BTreeMap<String, ModelCounts> = BTreeMap::new();

    for &model in &args.models {
        let counts = counts_by_model
            .entry(model.to_string())
            .or_default();
        let source = args
            .source
            .unwrap_or(model_summary(model).sources[0].id);

        for &fh in &hours {
            if !args.skip_severe {
                let outcome = run_severe_lane(
                    model, &date, args.cycle, fh, source, &domain, &args, counts,
                );
                outcomes.push(outcome);
            }
            if !args.skip_ecape {
                let outcome = run_ecape_lane(
                    model, &date, args.cycle, fh, source, &domain, &args, counts,
                );
                outcomes.push(outcome);
            }
            if !args.skip_direct {
                let outcome = run_direct_lane(
                    model,
                    &date,
                    args.cycle,
                    fh,
                    source,
                    &domain,
                    &args,
                    &direct_recipes,
                    counts,
                );
                outcomes.push(outcome);
            }
            if !args.skip_derived {
                let outcome = run_derived_lane(
                    model,
                    &date,
                    args.cycle,
                    fh,
                    source,
                    &domain,
                    &args,
                    &derived_recipes,
                    counts,
                );
                outcomes.push(outcome);
            }
        }
    }

    let finished_utc = utc_timestamp();
    let wall_clock_ms = run_start.elapsed().as_millis();

    let summary = RunSummary {
        started_utc,
        finished_utc,
        wall_clock_ms,
        region: args.region.slug().to_string(),
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
