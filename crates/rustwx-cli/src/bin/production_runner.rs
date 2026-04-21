use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_core::{CycleSpec, ModelId, SourceId};
use rustwx_models::{LatestRun, latest_available_run_at_forecast_hour, model_summary};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::{
    DerivedBatchRequest, run_derived_batch, supported_derived_recipe_slugs,
};
use rustwx_products::direct::{
    DirectBatchRequest, run_direct_batch, supported_direct_recipe_slugs,
};
use rustwx_products::ecape::{EcapeBatchRequest, run_ecape_batch};
use rustwx_products::heavy::{HeavyPanelHourRequest, run_heavy_panel_hour};
use rustwx_products::non_ecape::{
    HrrrNonEcapeHourRequest, NonEcapeHourRequest, run_hrrr_non_ecape_hour, run_model_non_ecape_hour,
};
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, RunPublicationManifest, atomic_write_json,
    finalize_and_publish_run_manifest, publish_failure_manifest,
};
use rustwx_products::severe::{SevereBatchRequest, run_severe_batch};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use rustwx_render::PngCompressionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(
    name = "production-runner",
    about = "Persistent-ish production scheduler skeleton for rustwx operational lanes"
)]
struct Args {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "hrrr,gfs,ecmwf-open-data,rrfs-a"
    )]
    models: Vec<ModelId>,
    #[arg(long, default_value = "0-6")]
    hours: String,
    #[arg(long, value_delimiter = ',', default_value = "conus")]
    regions: Vec<RegionPreset>,
    #[arg(long)]
    date: Option<String>,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = 300)]
    poll_seconds: u64,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long)]
    max_iterations: Option<usize>,
    #[arg(long)]
    render_threads: Option<usize>,
    #[arg(long, default_value_t = 1200)]
    width: u32,
    #[arg(long, default_value_t = 900)]
    height: u32,
    #[arg(long, default_value_t = false)]
    skip_severe: bool,
    #[arg(long, default_value_t = false)]
    skip_ecape: bool,
    #[arg(long, default_value_t = false)]
    skip_direct: bool,
    #[arg(long, default_value_t = false)]
    skip_derived: bool,
    #[arg(long, default_value_t = false)]
    all_supported: bool,
    #[arg(long, value_delimiter = ',')]
    direct_recipes: Option<Vec<String>>,
    #[arg(long, value_delimiter = ',')]
    derived_recipes: Option<Vec<String>>,
    #[arg(long = "source-mode", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Default)]
    png_compression: PngCompressionArg,
    #[arg(long, default_value_t = 1)]
    light_tokens: usize,
    #[arg(long, default_value_t = 1)]
    warm_tokens: usize,
    #[arg(long, default_value_t = 1)]
    heavy_tokens: usize,
    #[arg(long, default_value_t = 900)]
    failure_cooldown_seconds: u64,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum ProductionLane {
    HrrrHeavy,
    HrrrNonEcape,
    Severe,
    Ecape,
    NonHrrrNonEcape,
    Direct,
    Derived,
}

impl ProductionLane {
    fn slug(self) -> &'static str {
        match self {
            Self::HrrrHeavy => "hrrr_heavy",
            Self::HrrrNonEcape => "hrrr_non_ecape",
            Self::Severe => "severe",
            Self::Ecape => "ecape",
            Self::NonHrrrNonEcape => "non_hrrr_non_ecape",
            Self::Direct => "direct",
            Self::Derived => "derived",
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::HrrrHeavy => 0,
            Self::HrrrNonEcape => 1,
            Self::Severe => 2,
            Self::Ecape => 3,
            Self::NonHrrrNonEcape => 4,
            Self::Direct => 5,
            Self::Derived => 6,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum MemoryClass {
    Light,
    Warm,
    Heavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum JobLifecycleState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Deferred,
    SkippedFresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum FreshnessState {
    Unknown,
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductionJobKey {
    model: ModelId,
    region_slug: String,
    forecast_hour: u16,
    lane: ProductionLane,
}

impl ProductionJobKey {
    fn slug(&self) -> String {
        format!(
            "{}:{}:f{:03}:{}",
            self.model,
            self.region_slug,
            self.forecast_hour,
            self.lane.slug()
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductionJob {
    key: ProductionJobKey,
    memory_class: MemoryClass,
    priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobFreshness {
    state: FreshnessState,
    desired_date_yyyymmdd: String,
    desired_cycle_utc: Option<u8>,
    desired_source: SourceId,
    observed_at_utc: String,
    detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobPersistentState {
    lifecycle: JobLifecycleState,
    freshness: FreshnessState,
    desired_date_yyyymmdd: String,
    desired_cycle_utc: Option<u8>,
    desired_source: SourceId,
    attempt_count: u32,
    success_count: u32,
    failure_count: u32,
    last_observed_run_slug: Option<String>,
    last_attempt_started_utc: Option<String>,
    last_attempt_finished_utc: Option<String>,
    last_success_utc: Option<String>,
    last_error: Option<String>,
    last_outputs: Vec<String>,
}

impl JobPersistentState {
    fn new(default_date_yyyymmdd: String, default_source: SourceId) -> Self {
        Self {
            lifecycle: JobLifecycleState::Pending,
            freshness: FreshnessState::Unknown,
            desired_date_yyyymmdd: default_date_yyyymmdd,
            desired_cycle_utc: None,
            desired_source: default_source,
            attempt_count: 0,
            success_count: 0,
            failure_count: 0,
            last_observed_run_slug: None,
            last_attempt_started_utc: None,
            last_attempt_finished_utc: None,
            last_success_utc: None,
            last_error: None,
            last_outputs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IterationEvent {
    timestamp_utc: String,
    job_key: String,
    model: ModelId,
    region_slug: String,
    forecast_hour: u16,
    lane: ProductionLane,
    lifecycle: JobLifecycleState,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductionRunnerStateFile {
    schema_version: u32,
    runner_label: String,
    started_utc: String,
    updated_utc: String,
    iteration: usize,
    jobs: BTreeMap<String, JobPersistentState>,
    recent_events: Vec<IterationEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductionRunnerIterationReport {
    iteration: usize,
    started_utc: String,
    finished_utc: String,
    planned_jobs: usize,
    queued_jobs: usize,
    executed_jobs: usize,
    succeeded_jobs: usize,
    failed_jobs: usize,
    deferred_jobs: usize,
    skipped_fresh_jobs: usize,
    queue_order: Vec<String>,
    events: Vec<IterationEvent>,
    total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokenBudget {
    light: usize,
    warm: usize,
    heavy: usize,
}

#[derive(Debug, Clone)]
struct ResourceTokens {
    available_light: usize,
    available_warm: usize,
    available_heavy: usize,
}

#[derive(Debug, Clone, Copy)]
struct ResourceLease {
    memory_class: MemoryClass,
}

impl ResourceTokens {
    fn new(budget: &TokenBudget) -> Self {
        Self {
            available_light: budget.light.max(1),
            available_warm: budget.warm.max(1),
            available_heavy: budget.heavy.max(1),
        }
    }

    fn try_reserve(&mut self, memory_class: MemoryClass) -> Option<ResourceLease> {
        let slot = match memory_class {
            MemoryClass::Light => &mut self.available_light,
            MemoryClass::Warm => &mut self.available_warm,
            MemoryClass::Heavy => &mut self.available_heavy,
        };
        if *slot == 0 {
            return None;
        }
        *slot -= 1;
        Some(ResourceLease { memory_class })
    }

    fn release(&mut self, lease: ResourceLease) {
        match lease.memory_class {
            MemoryClass::Light => self.available_light += 1,
            MemoryClass::Warm => self.available_warm += 1,
            MemoryClass::Heavy => self.available_heavy += 1,
        }
    }
}

#[derive(Debug, Clone)]
struct RunnerConfig {
    date_yyyymmdd: String,
    cycle_override_utc: Option<u8>,
    source_override: Option<SourceId>,
    out_dir: PathBuf,
    cache_dir: PathBuf,
    use_cache: bool,
    source_mode: ProductSourceMode,
    png_compression: PngCompressionMode,
    output_width: u32,
    output_height: u32,
    skip_severe: bool,
    skip_ecape: bool,
    skip_direct: bool,
    skip_derived: bool,
    direct_recipes: BTreeMap<ModelId, Vec<String>>,
    derived_recipes: BTreeMap<ModelId, Vec<String>>,
    token_budget: TokenBudget,
    failure_cooldown_seconds: u64,
}

#[derive(Debug, Clone)]
struct JobExecutionContext {
    latest: LatestRun,
    direct_recipes: Vec<String>,
    derived_recipes: Vec<String>,
}

#[derive(Debug, Clone)]
struct JobExecutionResult {
    lifecycle: JobLifecycleState,
    outputs: Vec<String>,
    detail: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let date = args.date.clone().unwrap_or_else(today_utc_yyyymmdd);
    let failure_slug = format!("production_runner_{date}");
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args, &date) {
        let _ = publish_failure_manifest(
            "production_runner",
            &failure_slug,
            &failure_out_dir,
            &failure_slug,
            err.to_string(),
        );
        return Err(err);
    }
    Ok(())
}

fn run(args: &Args, date_yyyymmdd: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(&args.out_dir)?;
    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_dir)?;
    }
    configure_render_threads(args.render_threads);

    let hours = parse_hours(&args.hours)?;
    if hours.is_empty() {
        return Err("at least one forecast hour is required".into());
    }

    let config = RunnerConfig {
        date_yyyymmdd: date_yyyymmdd.to_string(),
        cycle_override_utc: args.cycle,
        source_override: args.source,
        out_dir: args.out_dir.clone(),
        cache_dir: cache_dir.clone(),
        use_cache: !args.no_cache,
        source_mode: args.source_mode.into(),
        png_compression: args.png_compression.into(),
        output_width: args.width,
        output_height: args.height,
        skip_severe: args.skip_severe,
        skip_ecape: args.skip_ecape,
        skip_direct: args.skip_direct,
        skip_derived: args.skip_derived,
        direct_recipes: build_direct_recipe_map(args),
        derived_recipes: build_derived_recipe_map(args),
        token_budget: TokenBudget {
            light: args.light_tokens,
            warm: args.warm_tokens,
            heavy: args.heavy_tokens,
        },
        failure_cooldown_seconds: args.failure_cooldown_seconds,
    };

    let runner_label = format!("production_runner_{}", utc_timestamp().replace(':', "-"));
    let state_path = config.out_dir.join("production_runner_state.json");
    let iteration_report_path = config
        .out_dir
        .join("production_runner_iteration_latest.json");
    let mut state = load_or_initialize_state(&state_path, &runner_label);

    let mut iteration = state.iteration;
    loop {
        if args.once && iteration > 0 {
            break;
        }
        if let Some(max_iterations) = args.max_iterations {
            if iteration >= max_iterations {
                break;
            }
        }
        iteration += 1;
        let report = run_iteration(
            iteration,
            &config,
            &args.models,
            &args.regions,
            &hours,
            &mut state,
        )?;
        atomic_write_json(&iteration_report_path, &report)?;
        persist_state(&state_path, &mut state, iteration, &report.events)?;
        publish_iteration_manifest(
            &config.out_dir,
            &config.date_yyyymmdd,
            iteration,
            &state_path,
            &iteration_report_path,
            &report,
        )?;
        if args.once {
            break;
        }
        thread::sleep(Duration::from_secs(args.poll_seconds.max(1)));
    }

    Ok(())
}

fn run_iteration(
    iteration: usize,
    config: &RunnerConfig,
    models: &[ModelId],
    regions: &[RegionPreset],
    hours: &[u16],
    state: &mut ProductionRunnerStateFile,
) -> Result<ProductionRunnerIterationReport, Box<dyn std::error::Error>> {
    let iteration_start = Instant::now();
    let started_utc = utc_timestamp();
    let planned_jobs = build_jobs(config, models, regions, hours);
    let mut events = Vec::<IterationEvent>::new();
    let mut queue = VecDeque::<ProductionJob>::new();

    for job in planned_jobs.iter().cloned() {
        let freshness = probe_freshness(&job, config);
        let key_slug = job.key.slug();
        let entry = state.jobs.entry(key_slug.clone()).or_insert_with(|| {
            JobPersistentState::new(config.date_yyyymmdd.clone(), freshness.desired_source)
        });
        entry.freshness = freshness.state;
        entry.desired_date_yyyymmdd = freshness.desired_date_yyyymmdd.clone();
        entry.desired_cycle_utc = freshness.desired_cycle_utc;
        entry.desired_source = freshness.desired_source;
        if freshness.state == FreshnessState::Unavailable {
            entry.last_error = freshness.detail.clone();
        }

        let desired_run_slug = desired_run_slug(&job, &freshness);
        if should_queue_job(
            entry,
            &freshness,
            desired_run_slug.as_deref(),
            config.failure_cooldown_seconds,
        ) {
            queue.push_back(job);
        } else {
            let lifecycle = if freshness.state == FreshnessState::Available {
                JobLifecycleState::SkippedFresh
            } else {
                JobLifecycleState::Deferred
            };
            events.push(IterationEvent {
                timestamp_utc: utc_timestamp(),
                job_key: key_slug,
                model: job.key.model,
                region_slug: job.key.region_slug.clone(),
                forecast_hour: job.key.forecast_hour,
                lane: job.key.lane,
                lifecycle,
                message: freshness.detail.clone().unwrap_or_else(|| {
                    "job already satisfied for the latest observed run".to_string()
                }),
            });
        }
    }

    let queue_order = queue.iter().map(|job| job.key.slug()).collect::<Vec<_>>();
    let mut tokens = ResourceTokens::new(&config.token_budget);
    let mut executed_jobs = 0usize;
    let mut succeeded_jobs = 0usize;
    let mut failed_jobs = 0usize;
    let mut deferred_jobs = 0usize;
    let mut skipped_fresh_jobs = events
        .iter()
        .filter(|event| event.lifecycle == JobLifecycleState::SkippedFresh)
        .count();

    while let Some(job) = queue.pop_front() {
        let Some(lease) = tokens.try_reserve(job.memory_class) else {
            deferred_jobs += 1;
            events.push(IterationEvent {
                timestamp_utc: utc_timestamp(),
                job_key: job.key.slug(),
                model: job.key.model,
                region_slug: job.key.region_slug.clone(),
                forecast_hour: job.key.forecast_hour,
                lane: job.key.lane,
                lifecycle: JobLifecycleState::Deferred,
                message: format!("no {:?} tokens available", job.memory_class),
            });
            continue;
        };

        let key_slug = job.key.slug();
        let job_state = state
            .jobs
            .get_mut(&key_slug)
            .expect("queued job must already have persistent state");
        job_state.lifecycle = JobLifecycleState::Running;
        job_state.attempt_count += 1;
        job_state.last_attempt_started_utc = Some(utc_timestamp());

        let freshness = JobFreshness {
            state: job_state.freshness,
            desired_date_yyyymmdd: job_state.desired_date_yyyymmdd.clone(),
            desired_cycle_utc: job_state.desired_cycle_utc,
            desired_source: job_state.desired_source,
            observed_at_utc: utc_timestamp(),
            detail: None,
        };
        let context = match build_execution_context(&job, &freshness, config) {
            Ok(context) => context,
            Err(err) => {
                tokens.release(lease);
                executed_jobs += 1;
                failed_jobs += 1;
                job_state.lifecycle = JobLifecycleState::Failed;
                job_state.failure_count += 1;
                job_state.last_attempt_finished_utc = Some(utc_timestamp());
                job_state.last_error = Some(err.to_string());
                events.push(IterationEvent {
                    timestamp_utc: utc_timestamp(),
                    job_key: key_slug,
                    model: job.key.model,
                    region_slug: job.key.region_slug.clone(),
                    forecast_hour: job.key.forecast_hour,
                    lane: job.key.lane,
                    lifecycle: JobLifecycleState::Failed,
                    message: err.to_string(),
                });
                continue;
            }
        };

        let result = execute_job(&job, &context, config);
        tokens.release(lease);
        executed_jobs += 1;
        job_state.last_attempt_finished_utc = Some(utc_timestamp());
        job_state.last_observed_run_slug = Some(format!(
            "{}:{}:{:02}z:f{:03}",
            context.latest.model,
            context.latest.cycle.date_yyyymmdd,
            context.latest.cycle.hour_utc,
            job.key.forecast_hour
        ));
        job_state.last_error = result.detail.clone();
        if !result.outputs.is_empty() {
            job_state.last_outputs = result.outputs.clone();
        }

        match result.lifecycle {
            JobLifecycleState::Succeeded => {
                succeeded_jobs += 1;
                job_state.lifecycle = JobLifecycleState::Succeeded;
                job_state.success_count += 1;
                job_state.last_success_utc = job_state.last_attempt_finished_utc.clone();
            }
            JobLifecycleState::Deferred => {
                deferred_jobs += 1;
                job_state.lifecycle = JobLifecycleState::Deferred;
            }
            JobLifecycleState::SkippedFresh => {
                skipped_fresh_jobs += 1;
                job_state.lifecycle = JobLifecycleState::SkippedFresh;
            }
            _ => {
                failed_jobs += 1;
                job_state.lifecycle = JobLifecycleState::Failed;
                job_state.failure_count += 1;
            }
        }

        events.push(IterationEvent {
            timestamp_utc: utc_timestamp(),
            job_key: job.key.slug(),
            model: job.key.model,
            region_slug: job.key.region_slug.clone(),
            forecast_hour: job.key.forecast_hour,
            lane: job.key.lane,
            lifecycle: result.lifecycle,
            message: result
                .detail
                .unwrap_or_else(|| format!("{} output(s)", result.outputs.len())),
        });
    }

    Ok(ProductionRunnerIterationReport {
        iteration,
        started_utc,
        finished_utc: utc_timestamp(),
        planned_jobs: planned_jobs.len(),
        queued_jobs: queue_order.len(),
        executed_jobs,
        succeeded_jobs,
        failed_jobs,
        deferred_jobs,
        skipped_fresh_jobs,
        queue_order,
        events,
        total_ms: iteration_start.elapsed().as_millis(),
    })
}

fn build_jobs(
    config: &RunnerConfig,
    models: &[ModelId],
    regions: &[RegionPreset],
    hours: &[u16],
) -> Vec<ProductionJob> {
    let mut jobs = Vec::new();
    for &region in regions {
        for &model in models {
            for &forecast_hour in hours {
                let region_slug = region.slug().to_string();
                if model == ModelId::Hrrr {
                    let has_non_ecape_work = (!config.skip_direct
                        && config
                            .direct_recipes
                            .get(&model)
                            .map(|recipes| !recipes.is_empty())
                            .unwrap_or(false))
                        || (!config.skip_derived
                            && config
                                .derived_recipes
                                .get(&model)
                                .map(|recipes| !recipes.is_empty())
                                .unwrap_or(false));
                    if !config.skip_severe || !config.skip_ecape {
                        jobs.push(ProductionJob {
                            key: ProductionJobKey {
                                model,
                                region_slug: region_slug.clone(),
                                forecast_hour,
                                lane: ProductionLane::HrrrHeavy,
                            },
                            memory_class: MemoryClass::Heavy,
                            priority: ProductionLane::HrrrHeavy.priority(),
                        });
                    }
                    if has_non_ecape_work {
                        jobs.push(ProductionJob {
                            key: ProductionJobKey {
                                model,
                                region_slug: region_slug.clone(),
                                forecast_hour,
                                lane: ProductionLane::HrrrNonEcape,
                            },
                            memory_class: MemoryClass::Warm,
                            priority: ProductionLane::HrrrNonEcape.priority(),
                        });
                    }
                    continue;
                }

                let has_non_ecape_work = (!config.skip_direct
                    && config
                        .direct_recipes
                        .get(&model)
                        .map(|recipes| !recipes.is_empty())
                        .unwrap_or(false))
                    || (!config.skip_derived
                        && config
                            .derived_recipes
                            .get(&model)
                            .map(|recipes| !recipes.is_empty())
                            .unwrap_or(false));
                if !config.skip_severe {
                    jobs.push(simple_job(
                        model,
                        &region_slug,
                        forecast_hour,
                        ProductionLane::Severe,
                    ));
                }
                if !config.skip_ecape {
                    jobs.push(simple_job(
                        model,
                        &region_slug,
                        forecast_hour,
                        ProductionLane::Ecape,
                    ));
                }
                if has_non_ecape_work {
                    jobs.push(simple_job(
                        model,
                        &region_slug,
                        forecast_hour,
                        ProductionLane::NonHrrrNonEcape,
                    ));
                }
            }
        }
    }
    jobs.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.key.model.to_string().cmp(&right.key.model.to_string()))
            .then_with(|| left.key.forecast_hour.cmp(&right.key.forecast_hour))
            .then_with(|| left.key.region_slug.cmp(&right.key.region_slug))
    });
    jobs
}

fn simple_job(
    model: ModelId,
    region_slug: &str,
    forecast_hour: u16,
    lane: ProductionLane,
) -> ProductionJob {
    ProductionJob {
        key: ProductionJobKey {
            model,
            region_slug: region_slug.to_string(),
            forecast_hour,
            lane,
        },
        memory_class: match lane {
            ProductionLane::Severe | ProductionLane::Ecape | ProductionLane::HrrrHeavy => {
                MemoryClass::Heavy
            }
            ProductionLane::HrrrNonEcape => MemoryClass::Warm,
            ProductionLane::NonHrrrNonEcape => MemoryClass::Warm,
            ProductionLane::Direct | ProductionLane::Derived => MemoryClass::Light,
        },
        priority: lane.priority(),
    }
}

fn probe_freshness(job: &ProductionJob, config: &RunnerConfig) -> JobFreshness {
    let desired_source = config
        .source_override
        .unwrap_or_else(|| model_summary(job.key.model).sources[0].id);
    let observed_at_utc = utc_timestamp();
    if let Some(cycle_utc) = config.cycle_override_utc {
        return JobFreshness {
            state: FreshnessState::Available,
            desired_date_yyyymmdd: config.date_yyyymmdd.clone(),
            desired_cycle_utc: Some(cycle_utc),
            desired_source,
            observed_at_utc,
            detail: Some("cycle override pinned; latest-run probe skipped".to_string()),
        };
    }
    match latest_available_run_at_forecast_hour(
        job.key.model,
        Some(desired_source),
        &config.date_yyyymmdd,
        job.key.forecast_hour,
    ) {
        Ok(latest) => JobFreshness {
            state: FreshnessState::Available,
            desired_date_yyyymmdd: latest.cycle.date_yyyymmdd,
            desired_cycle_utc: Some(latest.cycle.hour_utc),
            desired_source: latest.source,
            observed_at_utc,
            detail: None,
        },
        Err(err) => JobFreshness {
            state: FreshnessState::Unavailable,
            desired_date_yyyymmdd: config.date_yyyymmdd.clone(),
            desired_cycle_utc: None,
            desired_source,
            observed_at_utc,
            detail: Some(format!("latest-run probe unavailable: {err}")),
        },
    }
}

fn should_queue_job(
    state: &JobPersistentState,
    freshness: &JobFreshness,
    desired_run_slug: Option<&str>,
    failure_cooldown_seconds: u64,
) -> bool {
    if freshness.state != FreshnessState::Available {
        return false;
    }
    if let Some(desired_run_slug) = desired_run_slug {
        if state.last_observed_run_slug.as_deref() != Some(desired_run_slug) {
            return true;
        }
        if state.lifecycle != JobLifecycleState::Succeeded {
            return !recently_failed(state, failure_cooldown_seconds);
        }
    }
    false
}

fn recently_failed(state: &JobPersistentState, failure_cooldown_seconds: u64) -> bool {
    if state.lifecycle != JobLifecycleState::Failed {
        return false;
    }
    let Some(finished_utc) = &state.last_attempt_finished_utc else {
        return false;
    };
    let Some(last_secs) = parse_utc_timestamp_seconds(finished_utc) else {
        return false;
    };
    unix_now_secs().saturating_sub(last_secs) < failure_cooldown_seconds
}

fn build_execution_context(
    job: &ProductionJob,
    freshness: &JobFreshness,
    config: &RunnerConfig,
) -> Result<JobExecutionContext, Box<dyn std::error::Error>> {
    let cycle_utc = freshness
        .desired_cycle_utc
        .ok_or("freshness probe did not resolve a cycle")?;
    let latest = LatestRun {
        model: job.key.model,
        cycle: CycleSpec::new(&freshness.desired_date_yyyymmdd, cycle_utc)?,
        source: freshness.desired_source,
    };
    Ok(JobExecutionContext {
        latest,
        direct_recipes: config
            .direct_recipes
            .get(&job.key.model)
            .cloned()
            .unwrap_or_default(),
        derived_recipes: config
            .derived_recipes
            .get(&job.key.model)
            .cloned()
            .unwrap_or_default(),
    })
}

fn execute_job(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    match job.key.lane {
        ProductionLane::HrrrHeavy => execute_hrrr_heavy(job, context, config),
        ProductionLane::HrrrNonEcape => execute_hrrr_non_ecape(job, context, config),
        ProductionLane::NonHrrrNonEcape => execute_non_hrrr_non_ecape(job, context, config),
        ProductionLane::Severe => execute_severe(job, context, config),
        ProductionLane::Ecape => execute_ecape(job, context, config),
        ProductionLane::Direct => execute_direct(job, context, config),
        ProductionLane::Derived => execute_derived(job, context, config),
    }
}

fn execute_hrrr_heavy(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    if config.skip_severe && config.skip_ecape {
        return JobExecutionResult {
            lifecycle: JobLifecycleState::SkippedFresh,
            outputs: Vec::new(),
            detail: Some("heavy lane disabled by skip flags".to_string()),
        };
    }

    let request = HeavyPanelHourRequest {
        model: ModelId::Hrrr,
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: false,
    };
    match run_heavy_panel_hour(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: JobLifecycleState::Succeeded,
            outputs: {
                let mut outputs = Vec::new();
                if !config.skip_severe {
                    outputs.extend(
                        report
                            .severe
                            .outputs
                            .iter()
                            .map(|item| item.output_path.to_string_lossy().to_string()),
                    );
                }
                if !config.skip_ecape {
                    outputs.extend(
                        report
                            .ecape
                            .outputs
                            .iter()
                            .map(|item| item.output_path.to_string_lossy().to_string()),
                    );
                }
                outputs
            },
            detail: Some(format!(
                "{} heavy map(s)",
                if config.skip_severe {
                    report.ecape.outputs.len()
                } else if config.skip_ecape {
                    report.severe.outputs.len()
                } else {
                    report.severe.outputs.len() + report.ecape.outputs.len()
                }
            )),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn execute_hrrr_non_ecape(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    let request = HrrrNonEcapeHourRequest {
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        source_mode: config.source_mode,
        direct_recipe_slugs: if config.skip_direct {
            Vec::new()
        } else {
            context.direct_recipes.clone()
        },
        derived_recipe_slugs: if config.skip_derived {
            Vec::new()
        } else {
            context.derived_recipes.clone()
        },
        windowed_products: Vec::new(),
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: config.png_compression,
    };
    match run_hrrr_non_ecape_hour(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: if report.summary.output_count > 0 {
                JobLifecycleState::Succeeded
            } else {
                JobLifecycleState::Deferred
            },
            outputs: report
                .summary
                .output_paths
                .iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect(),
            detail: Some(format!(
                "{} output(s), {} blocker(s)",
                report.summary.output_count,
                report
                    .windowed
                    .as_ref()
                    .map(|windowed| windowed.blockers.len())
                    .unwrap_or(0)
                    + report
                        .direct
                        .as_ref()
                        .map(|direct| direct.blockers.len())
                        .unwrap_or(0)
                    + report
                        .derived
                        .as_ref()
                        .map(|derived| derived.blockers.len())
                        .unwrap_or(0)
            )),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn execute_non_hrrr_non_ecape(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    let request = NonEcapeHourRequest {
        model: job.key.model,
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        source_mode: config.source_mode,
        direct_recipe_slugs: if config.skip_direct {
            Vec::new()
        } else {
            context.direct_recipes.clone()
        },
        derived_recipe_slugs: if config.skip_derived {
            Vec::new()
        } else {
            context.derived_recipes.clone()
        },
        allow_large_heavy_domain: false,
        windowed_products: Vec::new(),
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: config.png_compression,
    };
    match run_model_non_ecape_hour(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: if report.summary.output_count > 0 {
                JobLifecycleState::Succeeded
            } else {
                JobLifecycleState::Deferred
            },
            outputs: report
                .summary
                .output_paths
                .iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect(),
            detail: Some(format!(
                "{} output(s), {} blocker(s)",
                report.summary.output_count,
                report
                    .windowed
                    .as_ref()
                    .map(|windowed| windowed.blockers.len())
                    .unwrap_or(0)
                    + report
                        .direct
                        .as_ref()
                        .map(|direct| direct.blockers.len())
                        .unwrap_or(0)
                    + report
                        .derived
                        .as_ref()
                        .map(|derived| derived.blockers.len())
                        .unwrap_or(0)
            )),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn execute_severe(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    let request = SevereBatchRequest {
        model: job.key.model,
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: false,
    };
    match run_severe_batch(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: JobLifecycleState::Succeeded,
            outputs: report
                .outputs
                .iter()
                .map(|output| output.output_path.to_string_lossy().to_string())
                .collect(),
            detail: Some(format!("{} severe map(s)", report.outputs.len())),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn execute_ecape(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    let request = EcapeBatchRequest {
        model: job.key.model,
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: false,
    };
    match run_ecape_batch(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: JobLifecycleState::Succeeded,
            outputs: report
                .outputs
                .iter()
                .map(|output| output.output_path.to_string_lossy().to_string())
                .collect(),
            detail: Some(format!("{} ecape map(s)", report.outputs.len())),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn execute_direct(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    if context.direct_recipes.is_empty() {
        return JobExecutionResult {
            lifecycle: JobLifecycleState::Deferred,
            outputs: Vec::new(),
            detail: Some("direct lane has no configured recipes".to_string()),
        };
    }
    let request = DirectBatchRequest {
        model: job.key.model,
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        recipe_slugs: context.direct_recipes.clone(),
        product_overrides: HashMap::new(),
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: config.png_compression,
    };
    match run_direct_batch(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: if report.recipes.is_empty() && !report.blockers.is_empty() {
                JobLifecycleState::Deferred
            } else {
                JobLifecycleState::Succeeded
            },
            outputs: report
                .recipes
                .iter()
                .map(|item| item.output_path.to_string_lossy().to_string())
                .collect(),
            detail: Some(format!(
                "{} rendered, {} blockers",
                report.recipes.len(),
                report.blockers.len()
            )),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn execute_derived(
    job: &ProductionJob,
    context: &JobExecutionContext,
    config: &RunnerConfig,
) -> JobExecutionResult {
    if context.derived_recipes.is_empty() {
        return JobExecutionResult {
            lifecycle: JobLifecycleState::Deferred,
            outputs: Vec::new(),
            detail: Some("derived lane has no configured recipes".to_string()),
        };
    }
    let request = DerivedBatchRequest {
        model: job.key.model,
        date_yyyymmdd: context.latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(context.latest.cycle.hour_utc),
        forecast_hour: job.key.forecast_hour,
        source: context.latest.source,
        domain: domain_from_region_slug(&job.key.region_slug),
        out_dir: config.out_dir.join(&job.key.region_slug),
        cache_root: config.cache_dir.clone(),
        use_cache: config.use_cache,
        recipe_slugs: context.derived_recipes.clone(),
        surface_product_override: None,
        pressure_product_override: None,
        source_mode: config.source_mode,
        allow_large_heavy_domain: false,
        output_width: config.output_width,
        output_height: config.output_height,
        png_compression: config.png_compression,
    };
    match run_derived_batch(&request) {
        Ok(report) => JobExecutionResult {
            lifecycle: if report.recipes.is_empty() && !report.blockers.is_empty() {
                JobLifecycleState::Deferred
            } else {
                JobLifecycleState::Succeeded
            },
            outputs: report
                .recipes
                .iter()
                .map(|item| item.output_path.to_string_lossy().to_string())
                .collect(),
            detail: Some(format!(
                "{} rendered, {} blockers",
                report.recipes.len(),
                report.blockers.len()
            )),
        },
        Err(err) => JobExecutionResult {
            lifecycle: JobLifecycleState::Failed,
            outputs: Vec::new(),
            detail: Some(err.to_string()),
        },
    }
}

fn load_or_initialize_state(path: &Path, runner_label: &str) -> ProductionRunnerStateFile {
    if let Ok(bytes) = fs::read(path) {
        if let Ok(state) = serde_json::from_slice::<ProductionRunnerStateFile>(&bytes) {
            return state;
        }
    }
    let now = utc_timestamp();
    ProductionRunnerStateFile {
        schema_version: 1,
        runner_label: runner_label.to_string(),
        started_utc: now.clone(),
        updated_utc: now,
        iteration: 0,
        jobs: BTreeMap::new(),
        recent_events: Vec::new(),
    }
}

fn persist_state(
    state_path: &Path,
    state: &mut ProductionRunnerStateFile,
    iteration: usize,
    new_events: &[IterationEvent],
) -> Result<(), Box<dyn std::error::Error>> {
    state.iteration = iteration;
    state.updated_utc = utc_timestamp();
    state.recent_events.extend_from_slice(new_events);
    const MAX_EVENTS: usize = 512;
    if state.recent_events.len() > MAX_EVENTS {
        let drop_count = state.recent_events.len() - MAX_EVENTS;
        state.recent_events.drain(0..drop_count);
    }
    atomic_write_json(state_path, state)
}

fn publish_iteration_manifest(
    out_dir: &Path,
    date_yyyymmdd: &str,
    iteration: usize,
    state_path: &Path,
    report_path: &Path,
    report: &ProductionRunnerIterationReport,
) -> Result<(), Box<dyn std::error::Error>> {
    let run_slug = format!("production_runner_{date_yyyymmdd}_iter{iteration:04}");
    let mut manifest = RunPublicationManifest::new("production_runner", run_slug.clone(), out_dir)
        .with_artifacts(vec![
            PublishedArtifactRecord::planned("runner_state", relative_path(out_dir, state_path))
                .with_state(ArtifactPublicationState::Complete),
            PublishedArtifactRecord::planned(
                "iteration_report",
                relative_path(out_dir, report_path),
            )
            .with_state(ArtifactPublicationState::Complete),
        ]);
    manifest.mark_running();
    if report.failed_jobs > 0 {
        manifest.mark_partial(format!(
            "{} failed job(s), {} deferred job(s)",
            report.failed_jobs, report.deferred_jobs
        ));
    } else {
        manifest.mark_complete();
    }
    let _ = finalize_and_publish_run_manifest(&mut manifest, out_dir, &run_slug)?;
    Ok(())
}

fn build_direct_recipe_map(args: &Args) -> BTreeMap<ModelId, Vec<String>> {
    let mut map = BTreeMap::new();
    for &model in &args.models {
        let supported = supported_direct_recipe_slugs(model);
        let recipes = if args.all_supported {
            supported
        } else if let Some(explicit) = &args.direct_recipes {
            explicit
                .iter()
                .filter(|slug| supported.contains(*slug))
                .cloned()
                .collect()
        } else {
            default_direct_recipes()
                .into_iter()
                .filter(|slug| supported.contains(slug))
                .collect()
        };
        map.insert(model, unique_vec(recipes));
    }
    map
}

fn build_derived_recipe_map(args: &Args) -> BTreeMap<ModelId, Vec<String>> {
    let mut map = BTreeMap::new();
    for &model in &args.models {
        let supported = supported_derived_recipe_slugs(model);
        let recipes = if args.all_supported {
            supported
        } else if let Some(explicit) = &args.derived_recipes {
            explicit
                .iter()
                .filter(|slug| supported.contains(*slug))
                .cloned()
                .collect()
        } else {
            default_derived_recipes()
                .into_iter()
                .filter(|slug| supported.contains(slug))
                .collect()
        };
        map.insert(
            model,
            filter_heavy_derived_recipes(unique_vec(recipes), args.skip_ecape),
        );
    }
    map
}

fn filter_heavy_derived_recipes(recipes: Vec<String>, skip_ecape: bool) -> Vec<String> {
    if !skip_ecape {
        return recipes;
    }
    recipes
        .into_iter()
        .filter(|slug| !rustwx_products::derived::is_heavy_derived_recipe_slug(slug))
        .collect()
}

fn default_direct_recipes() -> Vec<String> {
    vec![
        "composite_reflectivity",
        "2m_temperature_10m_winds",
        "2m_dewpoint_10m_winds",
        "2m_relative_humidity",
        "500mb_height_winds",
        "700mb_height_winds",
        "850mb_height_winds",
        "mslp_10m_winds",
        "precipitable_water",
        "10m_wind_gusts",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_derived_recipes() -> Vec<String> {
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
        "stp_fixed",
        "lifted_index",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn domain_from_region_slug(region_slug: &str) -> DomainSpec {
    for region in [
        RegionPreset::Midwest,
        RegionPreset::Conus,
        RegionPreset::California,
        RegionPreset::CaliforniaSquare,
        RegionPreset::RenoSquare,
        RegionPreset::Southeast,
        RegionPreset::SouthernPlains,
        RegionPreset::Northeast,
        RegionPreset::GreatLakes,
    ] {
        if region.slug() == region_slug {
            return DomainSpec::new(region.slug(), region.bounds());
        }
    }
    DomainSpec::new(region_slug, RegionPreset::Conus.bounds())
}

fn parse_hours(spec: &str) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
    if let Some((start, end)) = spec.split_once('-') {
        let start: u16 = start.parse()?;
        let end: u16 = end.parse()?;
        if start > end {
            return Err(format!("invalid hour range '{spec}'").into());
        }
        return Ok((start..=end).collect());
    }
    let mut hours = BTreeSet::<u16>::new();
    for token in spec.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        hours.insert(token.parse()?);
    }
    Ok(hours.into_iter().collect())
}

fn configure_render_threads(render_threads: Option<usize>) {
    match render_threads {
        Some(value) if value > 0 => unsafe {
            std::env::set_var("RUSTWX_RENDER_THREADS", value.to_string());
        },
        _ => unsafe {
            std::env::remove_var("RUSTWX_RENDER_THREADS");
        },
    }
}

fn desired_run_slug(job: &ProductionJob, freshness: &JobFreshness) -> Option<String> {
    freshness.desired_cycle_utc.map(|cycle_utc| {
        format!(
            "{}:{}:{:02}z:f{:03}",
            job.key.model, freshness.desired_date_yyyymmdd, cycle_utc, job.key.forecast_hour
        )
    })
}

fn relative_path(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root)
        .map(Path::to_path_buf)
        .unwrap_or_else(|_| path.to_path_buf())
}

fn unique_vec(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn parse_utc_timestamp_seconds(value: &str) -> Option<u64> {
    if value.len() < 19 {
        return None;
    }
    let year: i32 = value.get(0..4)?.parse().ok()?;
    let month: u32 = value.get(5..7)?.parse().ok()?;
    let day: u32 = value.get(8..10)?.parse().ok()?;
    let hour: u32 = value.get(11..13)?.parse().ok()?;
    let minute: u32 = value.get(14..16)?.parse().ok()?;
    let second: u32 = value.get(17..19)?.parse().ok()?;
    let days = days_from_civil(year, month, day)?;
    Some(days as u64 * 86_400 + hour as u64 * 3600 + minute as u64 * 60 + second as u64)
}

fn today_utc_yyyymmdd() -> String {
    let secs = unix_now_secs();
    let days_since_epoch = secs / 86_400;
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    format!("{year:04}{month:02}{day:02}")
}

fn utc_timestamp() -> String {
    let secs = unix_now_secs();
    let days_since_epoch = secs / 86_400;
    let seconds_in_day = secs % 86_400;
    let hour = seconds_in_day / 3600;
    let minute = (seconds_in_day % 3600) / 60;
    let second = seconds_in_day % 60;
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = ((5 * doy + 2) / 153) as i64;
    let d = doy - ((153 * mp as u64 + 2) / 5) + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = year as i64 - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_render::PngCompressionMode;

    #[test]
    fn skip_ecape_filters_heavy_derived_recipes_in_maps() {
        let filtered = filter_heavy_derived_recipes(
            vec![
                "sbcape".to_string(),
                "sbecape".to_string(),
                "stp_fixed".to_string(),
            ],
            true,
        );
        assert_eq!(
            filtered,
            vec!["sbcape".to_string(), "stp_fixed".to_string()]
        );
    }

    #[test]
    fn non_hrrr_build_jobs_collapse_direct_and_derived_into_unified_lane() {
        let mut direct_recipes = BTreeMap::new();
        direct_recipes.insert(ModelId::Gfs, vec!["composite_reflectivity".to_string()]);
        let mut derived_recipes = BTreeMap::new();
        derived_recipes.insert(ModelId::Gfs, vec!["sbcape".to_string()]);
        let config = RunnerConfig {
            date_yyyymmdd: "20260414".to_string(),
            cycle_override_utc: Some(12),
            source_override: Some(SourceId::Nomads),
            out_dir: PathBuf::from("out"),
            cache_dir: PathBuf::from("cache"),
            use_cache: false,
            source_mode: ProductSourceMode::Canonical,
            png_compression: PngCompressionMode::Default,
            output_width: 1200,
            output_height: 900,
            skip_severe: false,
            skip_ecape: false,
            skip_direct: false,
            skip_derived: false,
            direct_recipes,
            derived_recipes,
            token_budget: TokenBudget {
                light: 1,
                warm: 1,
                heavy: 1,
            },
            failure_cooldown_seconds: 900,
        };

        let jobs = build_jobs(&config, &[ModelId::Gfs], &[RegionPreset::Conus], &[0]);
        assert_eq!(jobs.len(), 3);
        assert!(
            jobs.iter()
                .any(|job| job.key.lane == ProductionLane::NonHrrrNonEcape)
        );
        assert!(!jobs.iter().any(|job| matches!(
            job.key.lane,
            ProductionLane::Direct | ProductionLane::Derived
        )));
    }
}
