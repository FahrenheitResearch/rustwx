use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::conus_plus_us_split_region_domains;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::supported_derived_recipe_inventory;
use rustwx_products::direct::supported_direct_recipe_slugs;
use rustwx_products::non_ecape::{
    HrrrNonEcapeFanoutTiming, HrrrNonEcapeMultiDomainReport, HrrrNonEcapeMultiDomainRequest,
    HrrrNonEcapeSharedTiming, run_hrrr_non_ecape_hour_multi_domain,
};
use rustwx_products::publication::atomic_write_json;
use rustwx_products::source::ProductSourceMode;
use rustwx_render::PngCompressionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceModeArg {
    Canonical,
    Fastest,
}

impl From<SourceModeArg> for ProductSourceMode {
    fn from(value: SourceModeArg) -> Self {
        match value {
            SourceModeArg::Canonical => Self::Canonical,
            SourceModeArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
}

impl From<PngCompressionArg> for PngCompressionMode {
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
    name = "hrrr-us-region-hours",
    about = "Generate HRRR non-ECAPE direct+derived outputs for CONUS plus a fixed US region split across multiple forecast hours"
)]
struct Args {
    #[arg(long, default_value = "20260419")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = [0u16, 1u16, 2u16])]
    hours: Vec<u16>,
    #[arg(long, default_value = "nomads")]
    source: rustwx_core::SourceId,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof\\hrrr_us_region_hours")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long = "source-mode", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(long, default_value_t = 2)]
    hour_jobs: usize,
    #[arg(long, default_value_t = 8)]
    domain_jobs: usize,
    #[arg(long)]
    render_threads: Option<usize>,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HourRunSummary {
    forecast_hour: u16,
    cycle_utc: u8,
    source: rustwx_core::SourceId,
    report_path: PathBuf,
    domain_count: usize,
    output_count: usize,
    direct_rendered_count: usize,
    derived_rendered_count: usize,
    shared_timing: HrrrNonEcapeSharedTiming,
    fanout_timing: HrrrNonEcapeFanoutTiming,
    total_ms: u128,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UsRegionHoursSummary {
    hour_count: usize,
    domain_count: usize,
    output_count: usize,
    direct_rendered_count: usize,
    derived_rendered_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HrrrUsRegionHoursReport {
    date_yyyymmdd: String,
    cycle_override_utc: Option<u8>,
    source: rustwx_core::SourceId,
    out_dir: PathBuf,
    cache_root: PathBuf,
    use_cache: bool,
    source_mode: ProductSourceMode,
    hours: Vec<u16>,
    region_slugs: Vec<String>,
    hour_jobs: usize,
    domain_jobs: usize,
    render_threads: Option<usize>,
    png_compression: PngCompressionMode,
    direct_recipe_count: usize,
    derived_recipe_count: usize,
    runs: Vec<HourRunSummary>,
    summary: UsRegionHoursSummary,
    total_ms: u128,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run(&args)
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let mut hours = args.hours.clone();
    hours.sort_unstable();
    hours.dedup();
    if hours.is_empty() {
        return Err("at least one forecast hour is required".into());
    }

    let domains = conus_plus_us_split_region_domains();
    let region_slugs = domains.iter().map(|domain| domain.slug.clone()).collect::<Vec<_>>();
    let domain_jobs = args.domain_jobs.max(1).min(domains.len().max(1));
    let hour_jobs = args.hour_jobs.max(1).min(hours.len().max(1));
    let render_threads = args.render_threads.or_else(|| {
        if hour_jobs > 1 || domain_jobs > 1 {
            Some(1)
        } else {
            None
        }
    });
    match render_threads {
        Some(value) if value > 0 => {
            unsafe {
                std::env::set_var("RUSTWX_RENDER_THREADS", value.to_string());
            }
        }
        _ => {
            unsafe {
                std::env::remove_var("RUSTWX_RENDER_THREADS");
            }
        }
    }

    let direct_recipe_slugs = if args.direct_recipes.is_empty() {
        supported_direct_recipe_slugs(rustwx_core::ModelId::Hrrr)
    } else {
        args.direct_recipes.clone()
    };
    let derived_recipe_slugs = if args.derived_recipes.is_empty() {
        supported_derived_recipe_inventory()
            .iter()
            .map(|recipe| recipe.slug.to_string())
            .collect()
    } else {
        args.derived_recipes.clone()
    };

    let queue = Arc::new(Mutex::new(VecDeque::from(hours.clone())));
    let (tx, rx) = mpsc::channel::<Result<HourRunSummary, String>>();
    let out_dir = args.out_dir.clone();
    let cache_root = cache_root.clone();
    let source_mode: ProductSourceMode = args.source_mode.into();
    let png_compression: PngCompressionMode = args.png_compression.into();

    thread::scope(|scope| {
        for _ in 0..hour_jobs {
            let queue = Arc::clone(&queue);
            let tx = tx.clone();
            let out_dir = out_dir.clone();
            let cache_root = cache_root.clone();
            let domains = domains.clone();
            let date = args.date.clone();
            let cycle = args.cycle;
            let source = args.source;
            let direct_recipe_slugs = direct_recipe_slugs.clone();
            let derived_recipe_slugs = derived_recipe_slugs.clone();
            scope.spawn(move || loop {
                let next_hour = {
                    let mut queue = queue.lock().expect("hour queue poisoned");
                    queue.pop_front()
                };
                let Some(forecast_hour) = next_hour else {
                    break;
                };

                let request = HrrrNonEcapeMultiDomainRequest {
                    date_yyyymmdd: date.clone(),
                    cycle_override_utc: cycle,
                    forecast_hour,
                    source,
                    domains: domains.clone(),
                    out_dir: out_dir.clone(),
                    cache_root: cache_root.clone(),
                    use_cache: !args.no_cache,
                    source_mode,
                    direct_recipe_slugs: direct_recipe_slugs.clone(),
                    derived_recipe_slugs: derived_recipe_slugs.clone(),
                    windowed_products: Vec::new(),
                    output_width: 1200,
                    output_height: 900,
                    png_compression,
                    domain_jobs: Some(domain_jobs),
                };

                let result = run_hrrr_non_ecape_hour_multi_domain(&request)
                    .and_then(|report| write_hour_report(&out_dir, &report))
                    .map_err(|err| err.to_string());
                if tx.send(result).is_err() {
                    break;
                }
            });
        }
        drop(tx);
    });

    let mut runs = Vec::new();
    for result in rx {
        runs.push(result.map_err(|err| -> Box<dyn std::error::Error> { err.into() })?);
    }
    runs.sort_by_key(|run| run.forecast_hour);

    let summary = UsRegionHoursSummary {
        hour_count: runs.len(),
        domain_count: runs.iter().map(|run| run.domain_count).sum(),
        output_count: runs.iter().map(|run| run.output_count).sum(),
        direct_rendered_count: runs.iter().map(|run| run.direct_rendered_count).sum(),
        derived_rendered_count: runs.iter().map(|run| run.derived_rendered_count).sum(),
    };

    let top_level_report = HrrrUsRegionHoursReport {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        source: args.source,
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        source_mode,
        hours,
        region_slugs,
        hour_jobs,
        domain_jobs,
        render_threads,
        png_compression,
        direct_recipe_count: direct_recipe_slugs.len(),
        derived_recipe_count: derived_recipe_slugs.len(),
        runs,
        summary,
        total_ms: total_start.elapsed().as_millis(),
    };

    let report_path = top_level_report_path(args);
    atomic_write_json(&report_path, &top_level_report)?;

    println!(
        "summary  hours={}  domains={}  outputs={}  direct={}  derived={}  total={} ({})",
        top_level_report.summary.hour_count,
        top_level_report.summary.domain_count,
        top_level_report.summary.output_count,
        top_level_report.summary.direct_rendered_count,
        top_level_report.summary.derived_rendered_count,
        top_level_report.total_ms,
        format_elapsed_ms(top_level_report.total_ms),
    );
    for run in &top_level_report.runs {
        println!(
            "f{:03} cycle {:02}z  domains={}  outputs={}  direct={}  derived={}  total={} ({})",
            run.forecast_hour,
            run.cycle_utc,
            run.domain_count,
            run.output_count,
            run.direct_rendered_count,
            run.derived_rendered_count,
            run.total_ms,
            format_elapsed_ms(run.total_ms),
        );
    }
    println!("{}", report_path.display());

    Ok(())
}

fn write_hour_report(
    out_dir: &std::path::Path,
    report: &HrrrNonEcapeMultiDomainReport,
) -> Result<HourRunSummary, Box<dyn std::error::Error>> {
    let report_path = out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_us_region_hour_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour
    ));
    atomic_write_json(&report_path, report)?;

    Ok(HourRunSummary {
        forecast_hour: report.forecast_hour,
        cycle_utc: report.cycle_utc,
        source: report.source,
        report_path,
        domain_count: report.domains.len(),
        output_count: report
            .domains
            .iter()
            .map(|domain| domain.summary.output_count)
            .sum(),
        direct_rendered_count: report
            .domains
            .iter()
            .map(|domain| domain.summary.direct_rendered_count)
            .sum(),
        derived_rendered_count: report
            .domains
            .iter()
            .map(|domain| domain.summary.derived_rendered_count)
            .sum(),
        shared_timing: report.shared_timing.clone(),
        fanout_timing: report.fanout_timing.clone(),
        total_ms: report.total_ms,
    })
}

fn top_level_report_path(args: &Args) -> PathBuf {
    match args.cycle {
        Some(cycle) => args.out_dir.join(format!(
            "rustwx_hrrr_{}_{}z_us_region_hours_report.json",
            args.date, cycle
        )),
        None => args
            .out_dir
            .join(format!("rustwx_hrrr_{}_us_region_hours_report.json", args.date)),
    }
}

fn format_elapsed_ms(total_ms: u128) -> String {
    let total_seconds = total_ms / 1000;
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    let tenths = (total_ms % 1000) / 100;

    if minutes > 0 {
        format!("{minutes}m {seconds}.{tenths}s")
    } else {
        format!("{seconds}.{tenths}s")
    }
}
