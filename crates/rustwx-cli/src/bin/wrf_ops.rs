use clap::{Parser, Subcommand, ValueEnum};
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};
use rustwx_io::{FetchRequest, fetch_bytes_with_cache};
use rustwx_prep::{
    WrfDomainBounds, WrfInitSource, WrfNestedResolution, WrfOpsPlan, WrfOpsRequest,
    WrfPhysicsPreset,
};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use chrono::{NaiveDate, NaiveDateTime, TimeDelta};

const DEFAULT_NON_ECAPE_SEVERE_DERIVED_RECIPES: &str = "sbcape,sbcin,sblcl,mlcape,mlcin,mucape,mucin,dcape,lifted_index,lapse_rate_700_500,lapse_rate_0_3km,bulk_shear_0_1km,bulk_shear_0_6km,srh_0_1km,srh_0_3km,ehi_0_1km,ehi_0_3km,stp_fixed,scp_mu_0_3km_0_6km_proxy";
const DEFAULT_DIRECT_RECIPES: &str = "2m_temperature_10m_winds,2m_dewpoint_10m_winds,mslp_10m_winds,10m_wind_gusts,total_qpf,composite_reflectivity,composite_reflectivity_uh,uh_2to5km";

#[derive(Debug, Parser)]
#[command(
    name = "wrf-ops",
    about = "Rust-first WRF operational planning, namelist generation, and node bootstrap helpers"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the WRF initialization sources that RustWX knows how to plan.
    Sources,
    /// Generate a WRF project plan and optionally write namelist.wps/namelist.input.
    Plan(PlanArgs),
    /// Create a runnable WRF project directory from domain/init arguments.
    Create(CreateArgs),
    /// Create a project and launch its pipeline in one step.
    Run(RunArgs),
    /// Stage full GRIB files for a generated WRF project through RustWX fetch/cache.
    StageGribs(StageGribsArgs),
    /// Resolve the latest available RustWX-backed cycle for a WRF init source.
    Latest(LatestArgs),
    /// Launch a generated WRF project pipeline, usually inside tmux.
    Launch(LaunchArgs),
    /// Print a concise status report for a generated WRF project.
    Status(StatusArgs),
    /// Write static human/agent status files for a generated WRF project.
    Dashboard(DashboardArgs),
    /// Audit whether this node is ready to create, launch, and plot WRF projects.
    Doctor(DoctorArgs),
    /// Emit copy-paste commands for bootstrapping or launching a WRF ops project.
    Recipe(RecipeArgs),
    /// Emit a node bootstrap script for Intel oneAPI WRF/WPS builds.
    BootstrapScript(BootstrapArgs),
}

#[derive(Debug, Parser)]
struct WrfPlanCommonArgs {
    #[arg(long)]
    project_name: String,
    #[arg(long, default_value = "hrrr")]
    init: WrfInitSource,
    #[arg(long)]
    start_utc: String,
    #[arg(long)]
    end_utc: String,
    #[arg(long, allow_hyphen_values = true)]
    west: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    east: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    south: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    north: Option<f64>,
    /// Domain center latitude. Use with --center-lon and --radius-km or --width-km/--height-km.
    #[arg(long)]
    center_lat: Option<f64>,
    /// Domain center longitude. Use with --center-lat and --radius-km or --width-km/--height-km.
    #[arg(long, allow_hyphen_values = true)]
    center_lon: Option<f64>,
    /// Square inner-domain size helper, in kilometers from center to each edge.
    #[arg(long)]
    radius_km: Option<f64>,
    /// Inner-domain width helper, in kilometers.
    #[arg(long)]
    width_km: Option<f64>,
    /// Inner-domain height helper, in kilometers.
    #[arg(long)]
    height_km: Option<f64>,
    #[arg(long, value_enum, default_value_t = ResolutionArg::Default3km)]
    resolution: ResolutionArg,
    #[arg(long)]
    inner_dx_m: Option<u32>,
    #[arg(long, default_value_t = 3)]
    parent_ratio: u16,
    #[arg(long, default_value_t = true)]
    nested: bool,
    #[arg(long, default_value_t = 6)]
    history_interval_minutes: u32,
    #[arg(long)]
    output_3d_interval_minutes: Option<u32>,
    #[arg(long, default_value_t = 20)]
    num_cores: u32,
    #[arg(long, value_enum, default_value_t = PhysicsArg::SevereConvection)]
    physics: PhysicsArg,
    #[arg(long, value_enum, default_value_t = PlotPresetArg::FullDerived)]
    plot_preset: PlotPresetArg,
    #[arg(long)]
    num_metgrid_levels: Option<u16>,
    #[arg(long)]
    num_metgrid_soil_levels: Option<u16>,
    #[arg(long, value_delimiter = ',')]
    wps_products: Option<Vec<String>>,
    #[arg(long, default_value = "/home/drew/weather/wrf/WRF_BUILD/WPS_GEOG")]
    geog_data_path: String,
    #[arg(long, default_value = "/home/drew/weather/wrf/WRF_BUILD")]
    wrf_build_path: String,
    #[arg(long, default_value = "/home/drew/weather/apps/rustwx/target/release")]
    rustwx_bin_dir: String,
}

#[derive(Debug, Parser)]
struct PlanArgs {
    #[command(flatten)]
    common: WrfPlanCommonArgs,
    #[arg(long)]
    write_dir: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct CreateArgs {
    #[command(flatten)]
    common: WrfPlanCommonArgs,
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long, default_value_t = false)]
    stage_dry_run: bool,
}

#[derive(Debug, Parser)]
struct RunArgs {
    #[command(flatten)]
    common: WrfPlanCommonArgs,
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long)]
    tmux_session: Option<String>,
    #[arg(long, default_value_t = false)]
    foreground: bool,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false)]
    overwrite_project: bool,
    #[arg(long, default_value_t = false)]
    skip_stage_gribs: bool,
    #[arg(long, default_value_t = false)]
    skip_wps: bool,
    #[arg(long, default_value_t = false)]
    skip_real: bool,
    #[arg(long, default_value_t = false)]
    skip_wrf: bool,
    #[arg(long, default_value_t = false)]
    plot: bool,
    #[arg(long, default_value_t = false)]
    overwrite_gribs: bool,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    forecast_hours: Vec<u16>,
    #[arg(long)]
    cycle_utc: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ResolutionArg {
    Default3km,
    Extra1p5km,
    Special500m,
}

impl From<ResolutionArg> for WrfNestedResolution {
    fn from(value: ResolutionArg) -> Self {
        match value {
            ResolutionArg::Default3km => Self::Default3Km,
            ResolutionArg::Extra1p5km => Self::Extra1p5Km,
            ResolutionArg::Special500m => Self::Special500M,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum PlotPresetArg {
    SevereNoEcape,
    DirectOnly,
    ReflOnly,
    FullDerived,
    Custom,
}

impl PlotPresetArg {
    fn slug(self) -> &'static str {
        match self {
            Self::SevereNoEcape => "severe-no-ecape",
            Self::DirectOnly => "direct-only",
            Self::ReflOnly => "refl-only",
            Self::FullDerived => "full-derived",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PhysicsArg {
    SevereConvection,
    SevereConvectionNoahMp,
}

impl From<PhysicsArg> for WrfPhysicsPreset {
    fn from(value: PhysicsArg) -> Self {
        match value {
            PhysicsArg::SevereConvection => Self::SevereConvection,
            PhysicsArg::SevereConvectionNoahMp => Self::SevereConvectionNoahMp,
        }
    }
}

#[derive(Debug, Parser)]
struct BootstrapArgs {
    #[arg(long, default_value = "/home/drew/weather/wrf")]
    base: String,
    #[arg(long, default_value = "4.6.1")]
    wrf_version: String,
    #[arg(long, default_value = "4.6.0")]
    wps_version: String,
    #[arg(long)]
    write_path: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct RecipeArgs {
    #[command(flatten)]
    common: WrfPlanCommonArgs,
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long)]
    tmux_session: Option<String>,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long)]
    cycle_utc: Option<String>,
    #[arg(long, default_value_t = false)]
    overwrite_gribs: bool,
    #[arg(long, default_value_t = false)]
    include_bootstrap: bool,
}

#[derive(Debug, Parser)]
struct StageGribsArgs {
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    forecast_hours: Vec<u16>,
    #[arg(long)]
    cycle_utc: Option<String>,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false)]
    overwrite: bool,
}

#[derive(Debug, Parser)]
struct LatestArgs {
    #[arg(long, default_value = "hrrr")]
    init: WrfInitSource,
    #[arg(long)]
    date_yyyymmdd: String,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    /// Resolve a common newest cycle for a full forecast-hour set, e.g. 2,3,4,5,6,7,8,9,10.
    #[arg(long, value_delimiter = ',')]
    forecast_hours: Vec<u16>,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
}

#[derive(Debug, Parser)]
struct LaunchArgs {
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long)]
    tmux_session: Option<String>,
    #[arg(long, default_value_t = false)]
    foreground: bool,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false)]
    skip_stage_gribs: bool,
    #[arg(long, default_value_t = false)]
    skip_wps: bool,
    #[arg(long, default_value_t = false)]
    skip_real: bool,
    #[arg(long, default_value_t = false)]
    skip_wrf: bool,
    #[arg(long, default_value_t = false)]
    plot: bool,
    #[arg(long, value_enum)]
    plot_preset: Option<PlotPresetArg>,
    #[arg(long, default_value_t = false)]
    overwrite_gribs: bool,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    forecast_hours: Vec<u16>,
    #[arg(long)]
    cycle_utc: Option<String>,
}

#[derive(Debug, Parser)]
struct StatusArgs {
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long)]
    tmux_session: Option<String>,
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Debug, Parser)]
struct DashboardArgs {
    #[arg(long)]
    project_dir: PathBuf,
    #[arg(long)]
    tmux_session: Option<String>,
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    watch: bool,
    #[arg(long, default_value_t = 30)]
    interval_seconds: u64,
}

#[derive(Debug, Parser)]
struct DoctorArgs {
    #[arg(long, default_value = "/home/drew/weather/wrf/WRF_BUILD")]
    wrf_build_path: PathBuf,
    #[arg(long, default_value = "/home/drew/weather/wrf/WRF_BUILD/WPS_GEOG")]
    geog_data_path: PathBuf,
    #[arg(long, default_value = "/home/drew/weather/apps/rustwx/target/release")]
    rustwx_bin_dir: PathBuf,
    #[arg(long, default_value = "/home/drew/weather/wrf/projects")]
    projects_dir: PathBuf,
    #[arg(long, default_value_t = false)]
    json: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Sources => {
            println!(
                "{}",
                serde_json::to_string_pretty(&WrfInitSource::supported_sources())?
            );
        }
        Command::Plan(args) => run_plan(args)?,
        Command::Create(args) => run_create(args)?,
        Command::Run(args) => run_run(args)?,
        Command::StageGribs(args) => run_stage_gribs(args)?,
        Command::Latest(args) => run_latest(args)?,
        Command::Launch(args) => run_launch(args)?,
        Command::Status(args) => run_status(args)?,
        Command::Dashboard(args) => run_dashboard(args)?,
        Command::Doctor(args) => run_doctor(args)?,
        Command::Recipe(args) => run_recipe(args)?,
        Command::BootstrapScript(args) => run_bootstrap_script(args)?,
    }
    Ok(())
}

fn run_plan(args: PlanArgs) -> anyhow::Result<()> {
    let plan = build_plan(&args.common)?;
    let plan_json = serde_json::to_string_pretty(&plan)?;
    println!("{plan_json}");

    if let Some(write_dir) = args.write_dir {
        write_project_files(&write_dir, &plan, &args.common)?;
    }
    Ok(())
}

fn run_create(args: CreateArgs) -> anyhow::Result<()> {
    let plan = build_plan(&args.common)?;
    write_project_files(&args.project_dir, &plan, &args.common)?;
    println!(
        "created {} for {} {}",
        args.project_dir.display(),
        plan.request.init_source,
        plan.request.nested_resolution.label()
    );
    if args.stage_dry_run {
        run_stage_gribs(StageGribsArgs {
            project_dir: args.project_dir,
            cache_dir: None,
            source: None,
            products: Vec::new(),
            forecast_hours: Vec::new(),
            cycle_utc: None,
            dry_run: true,
            overwrite: false,
        })?;
    }
    Ok(())
}

fn run_run(args: RunArgs) -> anyhow::Result<()> {
    if args.project_dir.exists()
        && !args.overwrite_project
        && fs::read_dir(&args.project_dir)
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false)
    {
        anyhow::bail!(
            "{} already exists and is not empty; pass --overwrite-project to refresh generated files",
            args.project_dir.display()
        );
    }

    let plan = build_plan(&args.common)?;
    write_project_files(&args.project_dir, &plan, &args.common)?;
    let session = args
        .tmux_session
        .clone()
        .unwrap_or_else(|| default_tmux_session(&args.project_dir));
    println!(
        "created {} for {} {}",
        args.project_dir.display(),
        plan.request.init_source,
        plan.request.nested_resolution.label()
    );

    run_launch(LaunchArgs {
        project_dir: args.project_dir.clone(),
        tmux_session: Some(session.clone()),
        foreground: args.foreground,
        dry_run: args.dry_run,
        skip_stage_gribs: args.skip_stage_gribs,
        skip_wps: args.skip_wps,
        skip_real: args.skip_real,
        skip_wrf: args.skip_wrf,
        plot: args.plot,
        plot_preset: args.plot.then_some(args.common.plot_preset),
        overwrite_gribs: args.overwrite_gribs,
        source: args.source,
        cache_dir: args.cache_dir,
        products: args.products,
        forecast_hours: args.forecast_hours,
        cycle_utc: args.cycle_utc,
    })?;

    println!(
        "status json: wrf_ops status --project-dir {} --tmux-session {} --json",
        shell_quote(&args.project_dir.display().to_string()),
        shell_quote(&session)
    );
    println!(
        "dashboard: wrf_ops dashboard --project-dir {} --tmux-session {}",
        shell_quote(&args.project_dir.display().to_string()),
        shell_quote(&session)
    );
    Ok(())
}

fn build_plan(args: &WrfPlanCommonArgs) -> anyhow::Result<WrfOpsPlan> {
    let bounds = resolve_domain_bounds(args)?;
    let mut request = WrfOpsRequest::severe_default(
        args.project_name.clone(),
        args.init,
        args.start_utc.clone(),
        args.end_utc.clone(),
        bounds,
    );
    request.nested = args.nested;
    request.nested_resolution = if let Some(inner_dx_m) = args.inner_dx_m {
        WrfNestedResolution::custom(inner_dx_m, args.parent_ratio)?
    } else {
        args.resolution.into()
    };
    request.history_interval_minutes = args.history_interval_minutes;
    request.output_3d_interval_minutes = args.output_3d_interval_minutes;
    request.num_cores = args.num_cores;
    request.physics = args.physics.into();
    request.num_metgrid_levels = args.num_metgrid_levels;
    request.num_metgrid_soil_levels = args.num_metgrid_soil_levels;
    request.wps_products = args.wps_products.clone();

    Ok(WrfOpsPlan::plan(request)?)
}

fn resolve_domain_bounds(args: &WrfPlanCommonArgs) -> anyhow::Result<WrfDomainBounds> {
    match (args.west, args.east, args.south, args.north) {
        (Some(west), Some(east), Some(south), Some(north)) => {
            return Ok(WrfDomainBounds::new(west, east, south, north)?);
        }
        (None, None, None, None) => {}
        _ => anyhow::bail!("domain bounds require all four of --west --east --south --north"),
    }

    let Some(center_lat) = args.center_lat else {
        anyhow::bail!(
            "domain requires either --west/--east/--south/--north or --center-lat/--center-lon with --radius-km or --width-km/--height-km"
        );
    };
    let Some(center_lon) = args.center_lon else {
        anyhow::bail!("domain center requires both --center-lat and --center-lon");
    };
    let (width_km, height_km) = if let Some(radius_km) = args.radius_km {
        if args.width_km.is_some() || args.height_km.is_some() {
            anyhow::bail!("use either --radius-km or --width-km/--height-km, not both");
        }
        (radius_km * 2.0, radius_km * 2.0)
    } else {
        let Some(width_km) = args.width_km else {
            anyhow::bail!("centered domains require --radius-km or --width-km");
        };
        let Some(height_km) = args.height_km else {
            anyhow::bail!("centered domains require --radius-km or --height-km");
        };
        (width_km, height_km)
    };
    Ok(WrfDomainBounds::from_center_km(
        center_lat, center_lon, width_km, height_km,
    )?)
}

fn write_project_files(
    write_dir: &Path,
    plan: &WrfOpsPlan,
    args: &WrfPlanCommonArgs,
) -> anyhow::Result<()> {
    let plan_json = serde_json::to_string_pretty(&plan)?;

    fs::create_dir_all(write_dir)?;
    fs::write(write_dir.join("wrf_ops_plan.json"), plan_json)?;
    fs::write(
        write_dir.join("namelist.wps"),
        plan.render_namelist_wps(&args.geog_data_path),
    )?;
    fs::write(
        write_dir.join("namelist.input"),
        plan.render_namelist_input(),
    )?;
    write_project_scripts(
        write_dir,
        plan,
        &args.wrf_build_path,
        &args.rustwx_bin_dir,
        args.plot_preset,
    )?;
    Ok(())
}

fn run_launch(args: LaunchArgs) -> anyhow::Result<()> {
    let project_dir = fs::canonicalize(&args.project_dir)?;
    let script = project_dir.join("run_pipeline.sh");
    if !script.exists() {
        anyhow::bail!(
            "{} does not exist; create the project with `wrf_ops create` first",
            script.display()
        );
    }

    if args.foreground {
        let mut command = ProcessCommand::new(&script);
        command.current_dir(&project_dir);
        set_pipeline_env(&mut command, &args);
        let status = command.status()?;
        if !status.success() {
            anyhow::bail!("WRF pipeline failed with status {status}");
        }
        return Ok(());
    }

    let session = args
        .tmux_session
        .clone()
        .unwrap_or_else(|| default_tmux_session(&project_dir));
    let env_prefix = pipeline_env_pairs(&args)
        .into_iter()
        .map(|(key, value)| format!("{key}={}", shell_quote(&value)))
        .collect::<Vec<_>>()
        .join(" ");
    let command_line = format!(
        "cd {} && {} ./run_pipeline.sh",
        shell_quote(&project_dir.display().to_string()),
        env_prefix
    );
    let status = ProcessCommand::new("tmux")
        .args(["new-session", "-d", "-s", &session, &command_line])
        .status()?;
    if !status.success() {
        anyhow::bail!("tmux launch failed with status {status}");
    }

    println!(
        "launched {} in tmux session {}",
        project_dir.display(),
        session
    );
    println!("attach with: tmux attach -t {session}");
    Ok(())
}

fn run_status(args: StatusArgs) -> anyhow::Result<()> {
    let project_dir = fs::canonicalize(&args.project_dir)?;
    let status = project_status(&project_dir, args.tmux_session.as_deref())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }
    let plan = &status.plan;

    println!("project: {}", plan.request.project_name);
    println!("path: {}", project_dir.display());
    println!(
        "init: {} products={}",
        plan.request.init_source,
        plan.wps_products.join(",")
    );
    println!(
        "time: {} -> {} every {}s",
        plan.request.start_utc, plan.request.end_utc, plan.interval_seconds
    );
    println!(
        "domains: max_dom={} inner={} {:.0}m {}x{}",
        plan.max_dom,
        plan.inner_domain().id,
        plan.inner_domain().dx_m,
        plan.inner_domain().e_we,
        plan.inner_domain().e_sn
    );

    println!("gribs: {}", status.counts.gribs);
    println!("geo_em: {}", status.counts.geo_em);
    println!("met_em: {}", status.counts.met_em);
    println!("wrfinput: {}", status.counts.wrfinput);
    println!("wrfout: {}", status.counts.wrfout);
    if let Some(latest) = &status.latest_wrfout {
        println!("latest_wrfout: {latest}");
    }
    if let Some(progress) = status.progress_percent {
        println!("progress: {progress:.1}%");
    }
    if let Some(valid) = &status.latest_valid_utc {
        println!("latest_valid: {valid}");
    }
    if let Some(eta) = &status.eta {
        println!("eta: {}", eta.label);
    }

    if !status.pipeline_status.is_empty() {
        println!("pipeline_status:");
        for line in &status.pipeline_status {
            println!("  {line}");
        }
    }

    if let Some(tmux) = &status.tmux {
        match tmux.active {
            Some(true) => println!("tmux: {} active", tmux.session),
            Some(false) => println!("tmux: {} not found", tmux.session),
            None => println!("tmux: unable to check {}", tmux.session),
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct ProjectStatus {
    project_dir: String,
    plan: WrfOpsPlan,
    counts: ProjectCounts,
    latest_wrfout: Option<String>,
    latest_valid_utc: Option<String>,
    progress_percent: Option<f64>,
    remaining_sim_seconds: Option<i64>,
    eta: Option<ProjectEta>,
    pipeline_status: Vec<String>,
    tmux: Option<TmuxStatus>,
}

#[derive(Debug, Serialize)]
struct ProjectCounts {
    gribs: usize,
    geo_em: usize,
    met_em: usize,
    wrfinput: usize,
    wrfout: usize,
    plots: usize,
}

#[derive(Debug, Serialize)]
struct ProjectEta {
    label: String,
    wall_seconds_per_sim_hour: Option<f64>,
}

#[derive(Debug, Serialize)]
struct TmuxStatus {
    session: String,
    active: Option<bool>,
}

fn project_status(project_dir: &Path, tmux_session: Option<&str>) -> anyhow::Result<ProjectStatus> {
    let plan = read_project_plan(project_dir)?;
    let grib_dir = project_dir.join("data").join("grib");
    let wps_dir = project_dir.join("WPS_work");
    let wrf_dir = project_dir.join("WRF_run");
    let plot_dir = project_dir.join("plots");
    let latest_wrfout_path = latest_matching(&wrf_dir, |name| name.starts_with("wrfout_"));
    let latest_valid_utc = latest_wrfout_path
        .as_deref()
        .and_then(wrfout_valid_from_path)
        .map(|dt| format!("{}Z", dt.format("%Y-%m-%dT%H:%M:%S")));
    let (progress_percent, remaining_sim_seconds) = progress_from_latest_valid(
        &plan,
        latest_wrfout_path
            .as_deref()
            .and_then(wrfout_valid_from_path),
    );
    let eta = eta_from_progress(project_dir, progress_percent, remaining_sim_seconds);
    let pipeline_status = {
        let path = project_dir.join("logs").join("pipeline.status");
        if path.exists() {
            tail_lines(&path, 8)?
        } else {
            Vec::new()
        }
    };
    let tmux = tmux_session.map(|session| TmuxStatus {
        session: session.to_string(),
        active: tmux_active(session),
    });

    Ok(ProjectStatus {
        project_dir: project_dir.display().to_string(),
        plan,
        counts: ProjectCounts {
            gribs: count_matching(&grib_dir, |_| true),
            geo_em: count_matching(&wps_dir, |name| name.starts_with("geo_em.")),
            met_em: count_matching(&wps_dir, |name| name.starts_with("met_em.")),
            wrfinput: count_matching(&wrf_dir, |name| name.starts_with("wrfinput_")),
            wrfout: count_matching(&wrf_dir, |name| name.starts_with("wrfout_")),
            plots: count_matching_recursive(&plot_dir, |name| name.ends_with(".png")),
        },
        latest_wrfout: latest_wrfout_path.map(|path| path.display().to_string()),
        latest_valid_utc,
        progress_percent,
        remaining_sim_seconds,
        eta,
        pipeline_status,
        tmux,
    })
}

fn tmux_active(session: &str) -> Option<bool> {
    ProcessCommand::new("tmux")
        .args(["has-session", "-t", session])
        .status()
        .ok()
        .map(|status| status.success())
}

fn run_dashboard(args: DashboardArgs) -> anyhow::Result<()> {
    let project_dir = fs::canonicalize(&args.project_dir)?;
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| project_dir.join("dashboard"));
    fs::create_dir_all(&out_dir)?;
    fs::write(out_dir.join("index.html"), dashboard_html())?;
    loop {
        write_dashboard_status(&project_dir, args.tmux_session.as_deref(), &out_dir)?;
        if !args.watch {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(args.interval_seconds.max(1)));
    }
    println!("{}", out_dir.join("index.html").display());
    Ok(())
}

fn write_dashboard_status(
    project_dir: &Path,
    tmux_session: Option<&str>,
    out_dir: &Path,
) -> anyhow::Result<()> {
    let status = project_status(project_dir, tmux_session)?;
    fs::write(
        out_dir.join("status.json"),
        serde_json::to_vec_pretty(&status)?,
    )?;
    Ok(())
}

fn dashboard_html() -> &'static str {
    r#"<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RustWX WRF Ops Dashboard</title>
<style>
  :root { color-scheme: dark; font-family: system-ui, sans-serif; background: #101214; color: #eef2f4; }
  body { margin: 0; }
  header { padding: 18px 22px; border-bottom: 1px solid #30363d; background: #15191d; }
  h1 { margin: 0; font-size: 22px; font-weight: 650; }
  main { padding: 18px; display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
  section { border: 1px solid #30363d; border-radius: 8px; padding: 14px; background: #171b20; }
  h2 { margin: 0 0 10px; font-size: 15px; color: #b8c7d4; }
  .big { font-size: 28px; font-weight: 700; }
  dl { margin: 0; display: grid; grid-template-columns: auto 1fr; gap: 7px 12px; }
  dt { color: #93a4b2; }
  dd { margin: 0; overflow-wrap: anywhere; }
  progress { width: 100%; height: 16px; accent-color: #75d3ff; }
  pre { margin: 0; white-space: pre-wrap; color: #d5dde5; font-size: 13px; }
</style>
<header><h1>RustWX WRF Ops Dashboard</h1></header>
<main>
  <section><h2>Run</h2><dl id="run"></dl></section>
  <section><h2>Progress</h2><div class="big" id="progressText">--</div><progress id="progress" max="100" value="0"></progress><p id="eta"></p></section>
  <section><h2>Counts</h2><dl id="counts"></dl></section>
  <section><h2>Control Commands</h2><pre id="commands"></pre></section>
  <section style="grid-column:1/-1"><h2>Pipeline</h2><pre id="pipeline"></pre></section>
</main>
<script>
const dl = (el, rows) => { el.innerHTML = rows.map(([k,v]) => `<dt>${k}</dt><dd>${v ?? ""}</dd>`).join(""); };
async function refresh() {
  const res = await fetch("status.json?ts=" + Date.now());
  const s = await res.json();
  const plan = s.plan;
  dl(document.getElementById("run"), [
    ["project", plan.request.project_name],
    ["init", plan.request.init_source],
    ["time", `${plan.request.start_utc} to ${plan.request.end_utc}`],
    ["domain", `d${String(plan.max_dom).padStart(2,"0")} ${plan.domains[plan.domains.length-1].dx_m} m ${plan.domains[plan.domains.length-1].e_we}x${plan.domains[plan.domains.length-1].e_sn}`],
    ["latest", s.latest_valid_utc],
    ["tmux", s.tmux ? `${s.tmux.session}: ${s.tmux.active ? "active" : "not active"}` : ""]
  ]);
  const p = s.progress_percent ?? 0;
  document.getElementById("progress").value = p;
  document.getElementById("progressText").textContent = p ? `${p.toFixed(1)}%` : "--";
  document.getElementById("eta").textContent = s.eta ? s.eta.label : "";
  dl(document.getElementById("counts"), Object.entries(s.counts));
  document.getElementById("pipeline").textContent = (s.pipeline_status || []).join("\n");
  document.getElementById("commands").textContent =
    `wrf_ops status --project-dir '${s.project_dir}' --json\n` +
    `wrf_ops launch --project-dir '${s.project_dir}' --foreground\n` +
    `tmux attach -t '${s.tmux ? s.tmux.session : "SESSION"}'`;
}
refresh();
setInterval(refresh, 30000);
</script>
"#
}

fn read_project_plan(project_dir: &Path) -> anyhow::Result<WrfOpsPlan> {
    let plan_path = project_dir.join("wrf_ops_plan.json");
    let plan_json = fs::read_to_string(&plan_path)?;
    Ok(serde_json::from_str(&plan_json)?)
}

fn set_pipeline_env(command: &mut ProcessCommand, args: &LaunchArgs) {
    for (key, value) in pipeline_env_pairs(args) {
        command.env(key, value);
    }
}

fn pipeline_env_pairs(args: &LaunchArgs) -> Vec<(&'static str, String)> {
    vec![
        ("STAGE_GRIBS", flag(!args.skip_stage_gribs).to_string()),
        ("RUN_WPS", flag(!args.skip_wps).to_string()),
        ("RUN_REAL", flag(!args.skip_real).to_string()),
        ("RUN_WRF", flag(!args.skip_wrf).to_string()),
        ("PLOT", flag(args.plot).to_string()),
        (
            "PLOT_PRESET",
            args.plot_preset
                .map(|preset| preset.slug().to_string())
                .unwrap_or_default(),
        ),
        ("DRY_RUN", flag(args.dry_run).to_string()),
        ("OVERWRITE_GRIBS", flag(args.overwrite_gribs).to_string()),
        (
            "STAGE_SOURCE",
            args.source
                .map(|source| source.as_str().to_string())
                .unwrap_or_default(),
        ),
        (
            "STAGE_CACHE_DIR",
            args.cache_dir
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        ),
        ("STAGE_PRODUCTS", args.products.join(",")),
        (
            "STAGE_FORECAST_HOURS",
            args.forecast_hours
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "STAGE_CYCLE_UTC",
            args.cycle_utc.clone().unwrap_or_default(),
        ),
    ]
}

fn flag(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

fn default_tmux_session(project_dir: &Path) -> String {
    let name = project_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("wrf_project");
    let clean = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("wrf_{clean}")
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn count_matching(dir: &Path, matches: impl Fn(&str) -> bool) -> usize {
    fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_str().is_some_and(|name| matches(name)))
        .count()
}

fn latest_matching(dir: &Path, matches: impl Fn(&str) -> bool) -> Option<PathBuf> {
    fs::read_dir(dir)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_str().is_some_and(|name| matches(name)))
        .filter_map(|entry| {
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((modified, entry.path()))
        })
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path)
}

fn count_matching_recursive(dir: &Path, matches: impl Fn(&str) -> bool + Copy) -> usize {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                count_matching_recursive(&path, matches)
            } else if entry.file_name().to_str().is_some_and(matches) {
                1
            } else {
                0
            }
        })
        .sum()
}

fn tail_lines(path: &Path, count: usize) -> anyhow::Result<Vec<String>> {
    let contents = fs::read_to_string(path)?;
    let mut lines = contents
        .lines()
        .rev()
        .take(count)
        .map(str::to_string)
        .collect::<Vec<_>>();
    lines.reverse();
    Ok(lines)
}

fn wrfout_valid_from_path(path: &Path) -> Option<NaiveDateTime> {
    let name = path.file_name()?.to_str()?;
    let stamp = name.split_once("wrfout_d")?.1.split_once('_')?.1;
    NaiveDateTime::parse_from_str(stamp, "%Y-%m-%d_%H_%M_%S").ok()
}

fn parse_utc_naive(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value.trim_end_matches('Z'), "%Y-%m-%dT%H:%M:%S").ok()
}

fn progress_from_latest_valid(
    plan: &WrfOpsPlan,
    latest_valid: Option<NaiveDateTime>,
) -> (Option<f64>, Option<i64>) {
    let Some(latest_valid) = latest_valid else {
        return (None, None);
    };
    let Some(start) = parse_utc_naive(&plan.request.start_utc) else {
        return (None, None);
    };
    let Some(end) = parse_utc_naive(&plan.request.end_utc) else {
        return (None, None);
    };
    let total = end.signed_duration_since(start).num_seconds();
    let done = latest_valid.signed_duration_since(start).num_seconds();
    if total <= 0 {
        return (None, None);
    }
    let progress = (done.max(0) as f64 / total as f64 * 100.0).clamp(0.0, 100.0);
    let remaining = (total - done).max(0);
    (Some(progress), Some(remaining))
}

fn eta_from_progress(
    project_dir: &Path,
    _progress_percent: Option<f64>,
    remaining_sim_seconds: Option<i64>,
) -> Option<ProjectEta> {
    let remaining_sim_seconds = remaining_sim_seconds?;
    let wall_seconds_per_sim_second = wrfout_wall_rate(project_dir)?;
    let remaining_wall = (remaining_sim_seconds as f64 * wall_seconds_per_sim_second).max(0.0);
    Some(ProjectEta {
        label: format!(
            "~{} remaining",
            human_duration(remaining_wall.round() as i64)
        ),
        wall_seconds_per_sim_hour: Some(wall_seconds_per_sim_second * 3_600.0),
    })
}

fn wrfout_wall_rate(project_dir: &Path) -> Option<f64> {
    let wrf_dir = project_dir.join("WRF_run");
    let mut samples = fs::read_dir(wrf_dir)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let valid = wrfout_valid_from_path(&path)?;
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((valid, modified))
        })
        .collect::<Vec<_>>();
    samples.sort_by_key(|(valid, _)| *valid);
    let first = samples.first()?;
    let last = samples.last()?;
    let sim_seconds = last.0.signed_duration_since(first.0).num_seconds();
    let wall_seconds = last.1.duration_since(first.1).ok()?.as_secs_f64();
    (sim_seconds > 0 && wall_seconds > 0.0).then_some(wall_seconds / sim_seconds as f64)
}

fn human_duration(seconds: i64) -> String {
    let seconds = seconds.max(0);
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m {secs:02}s")
    } else {
        format!("{secs}s")
    }
}

fn run_stage_gribs(args: StageGribsArgs) -> anyhow::Result<()> {
    let plan_path = args.project_dir.join("wrf_ops_plan.json");
    let plan_json = fs::read_to_string(&plan_path)?;
    let plan: WrfOpsPlan = serde_json::from_str(&plan_json)?;
    let model = plan
        .request
        .init_source
        .rustwx_model_id()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "init source '{}' is planned, but RustWX does not have a direct GRIB staging adapter for it yet",
                plan.request.init_source
            )
        })?;
    let products = if args.products.is_empty() {
        plan.wps_products.clone()
    } else {
        args.products.clone()
    };
    if products.is_empty() {
        anyhow::bail!(
            "no WPS products configured for {}",
            plan.request.init_source
        );
    }

    let cycle_utc = args.cycle_utc.as_deref().unwrap_or(&plan.request.start_utc);
    let cycle = CycleSpec::new(cycle_date_yyyymmdd(cycle_utc), cycle_hour(cycle_utc))?;
    let forecast_hours = if args.forecast_hours.is_empty() {
        plan_forecast_hours(&plan)
    } else {
        args.forecast_hours.clone()
    };
    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| args.project_dir.join(".rustwx_fetch_cache"));
    let grib_dir = args.project_dir.join("data").join("grib");
    fs::create_dir_all(&grib_dir)?;

    let mut staged = 0usize;
    for forecast_hour in forecast_hours {
        for product in &products {
            let request = ModelRunRequest::new(model, cycle.clone(), forecast_hour, product)?;
            let fetch = FetchRequest {
                request,
                source_override: args.source,
                variable_patterns: Vec::new(),
                earth2_ensemble: None,
            };

            if args.dry_run {
                let urls = rustwx_models::resolve_urls(&fetch.request)?;
                for resolved in urls.into_iter().filter(|resolved| match args.source {
                    Some(source) => source == resolved.source,
                    None => true,
                }) {
                    println!(
                        "dry-run {} f{:03} {} {}",
                        model, forecast_hour, resolved.source, resolved.grib_url
                    );
                }
                continue;
            }

            let cached = fetch_bytes_with_cache(&fetch, &cache_dir, true)?;
            let target = grib_dir.join(grib_filename(
                model,
                &cycle,
                forecast_hour,
                product,
                cached.result.source,
            ));
            if target.exists() {
                if args.overwrite {
                    fs::remove_file(&target)?;
                } else {
                    println!("skip existing {}", target.display());
                    continue;
                }
            }
            if let Err(err) = fs::hard_link(&cached.bytes_path, &target) {
                eprintln!(
                    "hardlink failed for {} ({err}); copying instead",
                    target.display()
                );
                fs::copy(&cached.bytes_path, &target)?;
            }
            println!(
                "staged {} f{:03} {} {}",
                model,
                forecast_hour,
                cached.result.source,
                target.display()
            );
            staged += 1;
        }
    }

    println!("staged {staged} GRIB file(s) into {}", grib_dir.display());
    Ok(())
}

fn run_latest(args: LatestArgs) -> anyhow::Result<()> {
    let model = args.init.rustwx_model_id().ok_or_else(|| {
        anyhow::anyhow!(
            "init source '{}' is planned, but RustWX does not have a direct latest-cycle adapter for it yet",
            args.init
        )
    })?;
    let products = if args.products.is_empty() {
        args.init
            .default_wps_products()
            .iter()
            .map(|product| (*product).to_string())
            .collect::<Vec<_>>()
    } else {
        args.products.clone()
    };
    let product_refs = products.iter().map(String::as_str).collect::<Vec<_>>();
    let forecast_hours = if args.forecast_hours.is_empty() {
        vec![args.forecast_hour]
    } else {
        args.forecast_hours.clone()
    };
    let mut resolved = Vec::with_capacity(forecast_hours.len());
    for forecast_hour in &forecast_hours {
        resolved.push(
            rustwx_models::latest_available_run_for_products_at_forecast_hour(
                model,
                args.source,
                &args.date_yyyymmdd,
                &product_refs,
                *forecast_hour,
            )?,
        );
    }
    let common = resolved
        .iter()
        .min_by_key(|latest| {
            (
                latest.cycle.date_yyyymmdd.clone(),
                latest.cycle.hour_utc,
                latest.source.as_str().to_string(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("no forecast hours requested"))?;
    let cycle_utc = cycle_utc_from_parts(&common.cycle.date_yyyymmdd, common.cycle.hour_utc);
    let min_hour = forecast_hours.iter().copied().min().unwrap_or(0);
    let max_hour = forecast_hours.iter().copied().max().unwrap_or(min_hour);
    let cycle_dt = NaiveDate::parse_from_str(&common.cycle.date_yyyymmdd, "%Y%m%d")?
        .and_hms_opt(u32::from(common.cycle.hour_utc), 0, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid cycle hour {}", common.cycle.hour_utc))?;
    let start_utc = format!(
        "{}Z",
        (cycle_dt + TimeDelta::hours(i64::from(min_hour))).format("%Y-%m-%dT%H:%M:%S")
    );
    let end_utc = format!(
        "{}Z",
        (cycle_dt + TimeDelta::hours(i64::from(max_hour))).format("%Y-%m-%dT%H:%M:%S")
    );
    let payload = serde_json::json!({
        "init": args.init,
        "model": common.model,
        "source": common.source,
        "cycle": common.cycle,
        "cycle_utc": cycle_utc,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "forecast_hour": args.forecast_hour,
        "forecast_hours": forecast_hours,
        "products": products,
        "resolved_forecast_cycles": resolved,
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn cycle_utc_from_parts(date_yyyymmdd: &str, hour: u8) -> String {
    format!(
        "{}-{}-{}T{:02}:00:00Z",
        &date_yyyymmdd[0..4],
        &date_yyyymmdd[4..6],
        &date_yyyymmdd[6..8],
        hour
    )
}

fn plan_forecast_hours(plan: &WrfOpsPlan) -> Vec<u16> {
    let total_hours = plan.run_hours + u32::from(plan.run_minutes > 0);
    let step_hours = (plan.interval_seconds / 3_600).max(1);
    let mut hours = Vec::new();
    let mut hour = 0u32;
    while hour <= total_hours {
        hours.push(hour.min(u32::from(u16::MAX)) as u16);
        hour += step_hours;
    }
    hours
}

fn grib_filename(
    model: ModelId,
    cycle: &CycleSpec,
    forecast_hour: u16,
    product: &str,
    source: SourceId,
) -> String {
    let product = product
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!(
        "{}_{}_{:02}z_f{:03}_{}_{}.grib2",
        model.as_str(),
        cycle.date_yyyymmdd,
        cycle.hour_utc,
        forecast_hour,
        source.as_str(),
        product
    )
}

fn write_project_scripts(
    dir: &std::path::Path,
    plan: &WrfOpsPlan,
    wrf_build_path: &str,
    rustwx_bin_dir: &str,
    plot_preset: PlotPresetArg,
) -> anyhow::Result<()> {
    fs::create_dir_all(dir.join("data/grib"))?;
    fs::create_dir_all(dir.join("WPS_work"))?;
    fs::create_dir_all(dir.join("WRF_run"))?;
    fs::create_dir_all(dir.join("plots"))?;
    fs::create_dir_all(dir.join("logs"))?;

    let wps_ungrib = if matches!(
        plan.request.init_source,
        WrfInitSource::Hrrr | WrfInitSource::Rap
    ) {
        r#"rm -f GRIBFILE.* FILE:* PRES:* SOIL:* met_em.d*.nc
cp "$PROJECT_DIR/namelist.wps" namelist.wps
./geogrid.exe 2>&1 | tee "$PROJECT_DIR/logs/geogrid.log"
mapfile -t gribs < <(find "$GRIB_DIR" -maxdepth 1 -type f \( -name '*_prs.grib2' -o -name '*_prs.grb2' -o -name '*_awp130pgrb.grib2' -o -name '*_awp130pgrb.grb2' \) | sort)
if [ "${#gribs[@]}" -eq 0 ]; then
  mapfile -t gribs < <(find "$GRIB_DIR" -maxdepth 1 -type f \( -name '*.grb' -o -name '*.grb2' -o -name '*.grib' -o -name '*.grib2' \) | sort)
fi
if [ "${#gribs[@]}" -eq 0 ]; then
  echo "No GRIB files found in $GRIB_DIR" >&2
  exit 2
fi
sed "s/prefix                 = 'FILE'/prefix                 = 'PRES'/" "$PROJECT_DIR/namelist.wps" > namelist.wps
ln -sfn "$WPS_BUILD/ungrib/Variable_Tables/Vtable.GFS" Vtable
"$WPS_BUILD/link_grib.csh" "${gribs[@]}"
./ungrib.exe 2>&1 | tee "$PROJECT_DIR/logs/ungrib_pres.log"
rm -f GRIBFILE.*
sed "s/prefix                 = 'FILE'/prefix                 = 'SOIL'/" "$PROJECT_DIR/namelist.wps" > namelist.wps
ln -sfn "$WPS_BUILD/ungrib/Variable_Tables/Vtable.raphrrr" Vtable
"$WPS_BUILD/link_grib.csh" "${gribs[@]}"
./ungrib.exe 2>&1 | tee "$PROJECT_DIR/logs/ungrib_soil.log"
cat "$PROJECT_DIR/logs/ungrib_pres.log" "$PROJECT_DIR/logs/ungrib_soil.log" > "$PROJECT_DIR/logs/ungrib.log"
rm -f GRIBFILE.*
sed -e "s/prefix                 = 'FILE'/prefix                 = 'PRES'/" -e "s/fg_name                = 'FILE'/fg_name                = 'PRES', 'SOIL'/" "$PROJECT_DIR/namelist.wps" > namelist.wps
./metgrid.exe 2>&1 | tee "$PROJECT_DIR/logs/metgrid.log"
"#
        .to_string()
    } else {
        format!(
            r#"ln -sfn "$WPS_BUILD/ungrib/Variable_Tables/{vtable}" Vtable
cp "$PROJECT_DIR/namelist.wps" namelist.wps
rm -f GRIBFILE.* FILE:* met_em.d*.nc
mapfile -t gribs < <(find "$GRIB_DIR" -maxdepth 1 -type f \( -name '*.grb' -o -name '*.grb2' -o -name '*.grib' -o -name '*.grib2' \) | sort)
if [ "${{#gribs[@]}}" -eq 0 ]; then
  echo "No GRIB files found in $GRIB_DIR" >&2
  exit 2
fi
"$WPS_BUILD/link_grib.csh" "${{gribs[@]}}"
./geogrid.exe 2>&1 | tee "$PROJECT_DIR/logs/geogrid.log"
./ungrib.exe 2>&1 | tee "$PROJECT_DIR/logs/ungrib.log"
./metgrid.exe 2>&1 | tee "$PROJECT_DIR/logs/metgrid.log"
"#,
            vtable = plan.vtable
        )
    };

    let wps = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
WPS_BUILD="{wrf_build_path}/WPS"
NETCDF="{wrf_build_path}/LIBRARIES/netcdf"
WPS_WORK="$PROJECT_DIR/WPS_work"
GRIB_DIR="$PROJECT_DIR/data/grib"
set +u
source /opt/intel/oneapi/setvars.sh --force >/dev/null 2>&1
set -u
export NETCDF
export PATH="$NETCDF/bin:/opt/intel/oneapi/mpi/latest/bin:$PATH"
export LD_LIBRARY_PATH="$NETCDF/lib:/opt/intel/oneapi/mpi/latest/lib:${{LD_LIBRARY_PATH:-}}"
mkdir -p "$WPS_WORK" "$PROJECT_DIR/logs"
cd "$WPS_WORK"
ln -sfn "$WPS_BUILD/geogrid.exe" geogrid.exe
ln -sfn "$WPS_BUILD/ungrib.exe" ungrib.exe
ln -sfn "$WPS_BUILD/metgrid.exe" metgrid.exe
mkdir -p geogrid metgrid
ln -sfn "$WPS_BUILD/geogrid/GEOGRID.TBL.ARW" geogrid/GEOGRID.TBL
ln -sfn "$WPS_BUILD/metgrid/METGRID.TBL.ARW" metgrid/METGRID.TBL
{wps_ungrib}
"#,
        wrf_build_path = wrf_build_path,
        wps_ungrib = wps_ungrib,
    );

    let real = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
WRF_BUILD="{wrf_build_path}/WRF"
NETCDF="{wrf_build_path}/LIBRARIES/netcdf"
WPS_WORK="$PROJECT_DIR/WPS_work"
WRF_RUN="$PROJECT_DIR/WRF_run"
set +u
source /opt/intel/oneapi/setvars.sh --force >/dev/null 2>&1
set -u
export NETCDF
export PATH="$NETCDF/bin:/opt/intel/oneapi/mpi/latest/bin:$PATH"
export LD_LIBRARY_PATH="$NETCDF/lib:/opt/intel/oneapi/mpi/latest/lib:${{LD_LIBRARY_PATH:-}}"
ulimit -s unlimited || true
export OMP_STACKSIZE="${{OMP_STACKSIZE:-1G}}"
export KMP_STACKSIZE="${{KMP_STACKSIZE:-1G}}"
mkdir -p "$WRF_RUN" "$PROJECT_DIR/logs"
cd "$WRF_RUN"
ln -sfn "$WRF_BUILD/main/real.exe" real.exe
find "$WRF_BUILD/run" -maxdepth 1 \( -type f -o -type l \) -exec ln -sfn {{}} . \;
rm -f namelist.input
cp "$PROJECT_DIR/namelist.input" namelist.input
find "$WPS_WORK" -maxdepth 1 -type f -name 'met_em.d*.nc' -exec ln -sfn {{}} . \;
rm -f rsl.out.* rsl.error.* wrfinput_d* wrfbdy_d*
mpirun -np {cores} ./real.exe
cp rsl.out.0000 "$PROJECT_DIR/logs/real.log"
"#,
        wrf_build_path = wrf_build_path,
        cores = plan.request.num_cores,
    );

    let wrf = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
WRF_BUILD="{wrf_build_path}/WRF"
NETCDF="{wrf_build_path}/LIBRARIES/netcdf"
WRF_RUN="$PROJECT_DIR/WRF_run"
set +u
source /opt/intel/oneapi/setvars.sh --force >/dev/null 2>&1
set -u
export NETCDF
export PATH="$NETCDF/bin:/opt/intel/oneapi/mpi/latest/bin:$PATH"
export LD_LIBRARY_PATH="$NETCDF/lib:/opt/intel/oneapi/mpi/latest/lib:${{LD_LIBRARY_PATH:-}}"
ulimit -s unlimited || true
export OMP_STACKSIZE="${{OMP_STACKSIZE:-1G}}"
export KMP_STACKSIZE="${{KMP_STACKSIZE:-1G}}"
mkdir -p "$WRF_RUN" "$PROJECT_DIR/logs"
cd "$WRF_RUN"
ln -sfn "$WRF_BUILD/main/wrf.exe" wrf.exe
find "$WRF_BUILD/run" -maxdepth 1 \( -type f -o -type l \) -exec ln -sfn {{}} . \;
rm -f namelist.input
cp "$PROJECT_DIR/namelist.input" namelist.input
rm -f rsl.out.* rsl.error.*
mpirun -np {cores} ./wrf.exe
cp rsl.out.0000 "$PROJECT_DIR/logs/wrf.log"
"#,
        wrf_build_path = wrf_build_path,
        cores = plan.request.num_cores,
    );

    let plot_domain = format!("d{:02}", plan.inner_domain().id);
    let plot = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
RUSTWX_BIN="{rustwx_bin_dir}"
export RUSTWX_PLOT_STYLE="${{RUSTWX_PLOT_STYLE:-operational_fast}}"
PLOT_PRESET="${{PLOT_PRESET:-{plot_preset}}}"
PLOT_ALL_SUPPORTED="${{PLOT_ALL_SUPPORTED:-}}"
PLOT_ALL_DERIVED_SUPPORTED="${{PLOT_ALL_DERIVED_SUPPORTED:-}}"
PLOT_DIRECT_RECIPES="${{PLOT_DIRECT_RECIPES:-}}"
PLOT_DERIVED_RECIPES="${{PLOT_DERIVED_RECIPES:-}}"
case "$PLOT_PRESET" in
  severe-no-ecape)
    PLOT_ALL_SUPPORTED="${{PLOT_ALL_SUPPORTED:-1}}"
    PLOT_DERIVED_RECIPES="${{PLOT_DERIVED_RECIPES:-{derived_recipes}}}"
    ;;
  direct-only)
    PLOT_ALL_SUPPORTED="${{PLOT_ALL_SUPPORTED:-1}}"
    PLOT_DERIVED_RECIPES="${{PLOT_DERIVED_RECIPES:-}}"
    ;;
  refl-only)
    PLOT_ALL_SUPPORTED="${{PLOT_ALL_SUPPORTED:-0}}"
    PLOT_DIRECT_RECIPES="${{PLOT_DIRECT_RECIPES:-composite_reflectivity,composite_reflectivity_uh,uh_2to5km}}"
    PLOT_DERIVED_RECIPES="${{PLOT_DERIVED_RECIPES:-}}"
    ;;
  full-derived)
    PLOT_ALL_SUPPORTED="${{PLOT_ALL_SUPPORTED:-1}}"
    PLOT_ALL_DERIVED_SUPPORTED="${{PLOT_ALL_DERIVED_SUPPORTED:-1}}"
    ;;
  custom)
    PLOT_ALL_SUPPORTED="${{PLOT_ALL_SUPPORTED:-0}}"
    PLOT_DIRECT_RECIPES="${{PLOT_DIRECT_RECIPES:-{direct_recipes}}}"
    ;;
  *)
    echo "unknown PLOT_PRESET=$PLOT_PRESET" >&2
    exit 2
    ;;
esac
cmd=("$RUSTWX_BIN/wrf_local_showcase"
  --plot-style "${{PLOT_STYLE:-operational-fast}}"
  --input-dir "$PROJECT_DIR/WRF_run"
  --out-dir "$PROJECT_DIR/plots"
  --cycle-date "{cycle_date}"
  --cycle {cycle_hour}
  --domains {plot_domain}
  --kinds wrfout
  --domain-slug-prefix "{project_slug}"
  --width 2400
  --height 1600
  --place-label-density 1
  --png-compression fast
  --jobs "${{PLOT_JOBS:-1}}")
if [ "${{PLOT_ALL_SUPPORTED:-0}}" = "1" ]; then
  cmd+=(--all-supported)
else
  cmd+=(--recipes "${{PLOT_DIRECT_RECIPES:-{direct_recipes}}}")
fi
if [ "${{PLOT_ALL_DERIVED_SUPPORTED:-0}}" = "1" ]; then
  cmd+=(--all-derived-supported)
elif [ -n "${{PLOT_DERIVED_RECIPES:-}}" ]; then
  cmd+=(--derived-recipes "$PLOT_DERIVED_RECIPES")
fi
"${{cmd[@]}}"
"#,
        rustwx_bin_dir = rustwx_bin_dir,
        cycle_date = cycle_date_yyyymmdd(&plan.request.start_utc),
        cycle_hour = cycle_hour(&plan.request.start_utc),
        plot_domain = plot_domain,
        project_slug = plan.request.project_name.replace('-', "_"),
        derived_recipes = DEFAULT_NON_ECAPE_SEVERE_DERIVED_RECIPES,
        direct_recipes = DEFAULT_DIRECT_RECIPES,
        plot_preset = plot_preset.slug(),
    );

    let pipeline = r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUSTWX_BIN="@RUSTWX_BIN@"
: "${STAGE_GRIBS:=1}"
: "${RUN_WPS:=1}"
: "${RUN_REAL:=1}"
: "${RUN_WRF:=1}"
: "${PLOT:=0}"
: "${DRY_RUN:=0}"
: "${OVERWRITE_GRIBS:=0}"
: "${STAGE_SOURCE:=}"
: "${STAGE_CACHE_DIR:=}"
: "${STAGE_PRODUCTS:=}"
: "${STAGE_FORECAST_HOURS:=}"
: "${STAGE_CYCLE_UTC:=}"
mkdir -p "$PROJECT_DIR/logs"
status="$PROJECT_DIR/logs/pipeline.status"
stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
phase() { echo "$(stamp) $1" | tee -a "$status"; }

phase start
if [ "$STAGE_GRIBS" = "1" ]; then
  phase stage-gribs
  cmd=("$RUSTWX_BIN/wrf_ops" stage-gribs --project-dir "$PROJECT_DIR")
  if [ -n "$STAGE_SOURCE" ]; then
    cmd+=(--source "$STAGE_SOURCE")
  fi
  if [ -n "$STAGE_CACHE_DIR" ]; then
    cmd+=(--cache-dir "$STAGE_CACHE_DIR")
  fi
  if [ -n "$STAGE_PRODUCTS" ]; then
    cmd+=(--products "$STAGE_PRODUCTS")
  fi
  if [ -n "$STAGE_FORECAST_HOURS" ]; then
    cmd+=(--forecast-hours "$STAGE_FORECAST_HOURS")
  fi
  if [ -n "$STAGE_CYCLE_UTC" ]; then
    cmd+=(--cycle-utc "$STAGE_CYCLE_UTC")
  fi
  if [ "$DRY_RUN" = "1" ]; then
    cmd+=(--dry-run)
  fi
  if [ "$OVERWRITE_GRIBS" = "1" ]; then
    cmd+=(--overwrite)
  fi
  "${cmd[@]}" 2>&1 | tee "$PROJECT_DIR/logs/stage_gribs.log"
fi

if [ "$DRY_RUN" = "1" ]; then
  phase dry-run-complete
  exit 0
fi

if [ "$RUN_WPS" = "1" ]; then
  phase wps
  "$PROJECT_DIR/run_wps.sh"
fi
if [ "$RUN_REAL" = "1" ]; then
  phase real
  "$PROJECT_DIR/run_real.sh"
fi
if [ "$RUN_WRF" = "1" ]; then
  phase wrf
  "$PROJECT_DIR/run_wrf.sh"
fi
if [ "$PLOT" = "1" ]; then
  phase plot
  "$PROJECT_DIR/plot_wrfout.sh"
fi
phase complete
"#
    .replace("@RUSTWX_BIN@", rustwx_bin_dir);

    let status = r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUSTWX_BIN="@RUSTWX_BIN@"
if [ "${JSON:-0}" = "1" ] && [ -x "$RUSTWX_BIN/wrf_ops" ]; then
  exec "$RUSTWX_BIN/wrf_ops" status --project-dir "$PROJECT_DIR" --json
fi
echo "project: $PROJECT_DIR"
if command -v jq >/dev/null 2>&1 && [ -f "$PROJECT_DIR/wrf_ops_plan.json" ]; then
  jq -r '"init: \(.request.init_source) products=\(.wps_products|join(",")) max_dom=\(.max_dom) run=\(.run_hours)h\(.run_minutes)m"' "$PROJECT_DIR/wrf_ops_plan.json"
fi
count() {
  local dir="$1"
  local pattern="$2"
  if [ -d "$dir" ]; then
    find "$dir" -maxdepth 1 -type f -name "$pattern" | wc -l
  else
    printf '0\n'
  fi
}
printf 'gribs: %s\n' "$(count "$PROJECT_DIR/data/grib" '*')"
printf 'geo_em: %s\n' "$(count "$PROJECT_DIR/WPS_work" 'geo_em.*.nc')"
printf 'met_em: %s\n' "$(count "$PROJECT_DIR/WPS_work" 'met_em.d*.nc')"
printf 'wrfinput: %s\n' "$(count "$PROJECT_DIR/WRF_run" 'wrfinput_d*')"
printf 'wrfout: %s\n' "$(count "$PROJECT_DIR/WRF_run" 'wrfout_*')"
latest="$(find "$PROJECT_DIR/WRF_run" -maxdepth 1 -type f -name 'wrfout_*' 2>/dev/null | sort | tail -1 || true)"
if [ -n "$latest" ]; then
  echo "latest_wrfout: $latest"
fi
if [ -f "$PROJECT_DIR/logs/pipeline.status" ]; then
  echo "pipeline_status:"
  tail -8 "$PROJECT_DIR/logs/pipeline.status"
fi
df -h "$PROJECT_DIR" | tail -1
"#
    .replace("@RUSTWX_BIN@", rustwx_bin_dir);

    let dashboard = r#"#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUSTWX_BIN="@RUSTWX_BIN@"
exec "$RUSTWX_BIN/wrf_ops" dashboard --project-dir "$PROJECT_DIR" "$@"
"#
    .replace("@RUSTWX_BIN@", rustwx_bin_dir);

    fs::write(dir.join("run_wps.sh"), wps)?;
    fs::write(dir.join("run_real.sh"), real)?;
    fs::write(dir.join("run_wrf.sh"), wrf)?;
    fs::write(dir.join("plot_wrfout.sh"), plot)?;
    fs::write(dir.join("run_pipeline.sh"), pipeline)?;
    fs::write(dir.join("status.sh"), status)?;
    fs::write(dir.join("dashboard.sh"), dashboard)?;
    make_executable(&dir.join("run_wps.sh"))?;
    make_executable(&dir.join("run_real.sh"))?;
    make_executable(&dir.join("run_wrf.sh"))?;
    make_executable(&dir.join("plot_wrfout.sh"))?;
    make_executable(&dir.join("run_pipeline.sh"))?;
    make_executable(&dir.join("status.sh"))?;
    make_executable(&dir.join("dashboard.sh"))?;
    Ok(())
}

#[cfg(unix)]
fn make_executable(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &std::path::Path) -> anyhow::Result<()> {
    Ok(())
}

fn cycle_date_yyyymmdd(value: &str) -> String {
    let clean = value.trim_end_matches('Z');
    let date = clean.split('T').next().unwrap_or(clean);
    date.replace('-', "")
}

fn cycle_hour(value: &str) -> u8 {
    value
        .split('T')
        .nth(1)
        .and_then(|time| time.get(0..2))
        .and_then(|hour| hour.parse::<u8>().ok())
        .unwrap_or(0)
}

fn run_doctor(args: DoctorArgs) -> anyhow::Result<()> {
    let checks = doctor_checks(&args);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&checks)?);
    } else {
        for check in &checks {
            let status = if check.ok { "OK" } else { "MISSING" };
            let required = if check.required {
                "required"
            } else {
                "optional"
            };
            println!("{status:7} {required:8} {:18} {}", check.kind, check.target);
        }
        run_passthrough("df", &["-h", path_str(&args.projects_dir)]);
    }

    let failed = checks.iter().any(|check| check.required && !check.ok);
    if failed {
        anyhow::bail!("node doctor found missing required WRF ops pieces");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DoctorCheck {
    kind: &'static str,
    target: String,
    required: bool,
    ok: bool,
}

fn doctor_checks(args: &DoctorArgs) -> Vec<DoctorCheck> {
    let wrf_build = &args.wrf_build_path;
    let wps = wrf_build.join("WPS");
    let wrf = wrf_build.join("WRF");
    let geog = &args.geog_data_path;
    let bin = &args.rustwx_bin_dir;
    let setvars = PathBuf::from("/opt/intel/oneapi/setvars.sh");

    let mut checks = vec![
        path_check("wrf-binary", wrf.join("main/wrf.exe"), true),
        path_check("wrf-binary", wrf.join("main/real.exe"), true),
        path_check("wrf-binary", wrf.join("main/ndown.exe"), true),
        path_check("wps-binary", wps.join("geogrid.exe"), true),
        path_check("wps-binary", wps.join("ungrib.exe"), true),
        path_check("wps-binary", wps.join("metgrid.exe"), true),
        path_check("wps-script", wps.join("link_grib.csh"), true),
        path_check(
            "wps-vtable",
            wps.join("ungrib/Variable_Tables/Vtable.GFS"),
            true,
        ),
        path_check(
            "wps-vtable",
            wps.join("ungrib/Variable_Tables/Vtable.raphrrr"),
            true,
        ),
        path_check(
            "wps-vtable",
            wps.join("ungrib/Variable_Tables/Vtable.ECMWF"),
            true,
        ),
        path_check("geog", geog.join("topo_gmted2010_30s/index"), true),
        path_check("intel-oneapi", &setvars, true),
        path_check("intel-mpi", "/opt/intel/oneapi/mpi/latest/bin/mpirun", true),
        path_check("rustwx-bin", bin.join("wrf_ops"), true),
        path_check("rustwx-bin", bin.join("wrf_local_showcase"), false),
        path_check("projects", &args.projects_dir, true),
        command_check("command", "ldd", &["--version"], true),
        command_check("command", "tmux", &["-V"], true),
    ];

    checks.extend([
        ldd_contains_check(
            "wps-ungrib-runtime",
            wps.join("ungrib.exe"),
            "libgfortran",
            true,
        ),
        ldd_with_oneapi_contains_check(
            "wps-metgrid-runtime",
            &setvars,
            wps.join("metgrid.exe"),
            "libimf",
            true,
        ),
        ldd_with_oneapi_contains_check(
            "wrf-runtime",
            &setvars,
            wrf.join("main/wrf.exe"),
            "libimf",
            true,
        ),
    ]);

    checks
}

fn path_check(kind: &'static str, path: impl AsRef<Path>, required: bool) -> DoctorCheck {
    let path = path.as_ref();
    DoctorCheck {
        kind,
        target: path.display().to_string(),
        required,
        ok: path.exists(),
    }
}

fn command_check(kind: &'static str, command: &str, args: &[&str], required: bool) -> DoctorCheck {
    let ok = ProcessCommand::new(command)
        .args(args)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false);
    DoctorCheck {
        kind,
        target: command.to_string(),
        required,
        ok,
    }
}

fn ldd_contains_check(
    kind: &'static str,
    path: impl AsRef<Path>,
    needle: &str,
    required: bool,
) -> DoctorCheck {
    let path = path.as_ref();
    let target = format!("{} contains {}", path.display(), needle);
    let ok = path.exists()
        && ProcessCommand::new("ldd")
            .arg(path)
            .output()
            .map(|output| {
                output.status.success() && String::from_utf8_lossy(&output.stdout).contains(needle)
            })
            .unwrap_or(false);
    DoctorCheck {
        kind,
        target,
        required,
        ok,
    }
}

fn ldd_with_oneapi_contains_check(
    kind: &'static str,
    setvars: &Path,
    path: impl AsRef<Path>,
    needle: &str,
    required: bool,
) -> DoctorCheck {
    let path = path.as_ref();
    let target = format!("{} contains {} after oneAPI env", path.display(), needle);
    let script = format!(
        "set +u; source {} --force >/dev/null 2>&1; set -u; ldd {}",
        shell_quote(&setvars.display().to_string()),
        shell_quote(&path.display().to_string())
    );
    let ok = setvars.exists()
        && path.exists()
        && ProcessCommand::new("bash")
            .args(["-lc", &script])
            .output()
            .map(|output| {
                output.status.success() && String::from_utf8_lossy(&output.stdout).contains(needle)
            })
            .unwrap_or(false);
    DoctorCheck {
        kind,
        target,
        required,
        ok,
    }
}

fn run_passthrough(command: &str, args: &[&str]) {
    let _ = ProcessCommand::new(command).args(args).status();
}

fn path_str(path: &Path) -> &str {
    path.to_str().unwrap_or(".")
}

fn run_bootstrap_script(args: BootstrapArgs) -> anyhow::Result<()> {
    let script = normalize_script_lf(&intel_bootstrap_script(
        &args.base,
        &args.wrf_version,
        &args.wps_version,
    ));
    if let Some(write_path) = args.write_path {
        if let Some(parent) = write_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&write_path, script)?;
        println!("{}", write_path.display());
    } else {
        print!("{script}");
    }
    Ok(())
}

fn normalize_script_lf(script: &str) -> String {
    script.replace("\r\n", "\n").replace('\r', "\n")
}

fn run_recipe(args: RecipeArgs) -> anyhow::Result<()> {
    let plan = build_plan(&args.common)?;
    let bin = format!(
        "{}/wrf_ops",
        args.common.rustwx_bin_dir.trim_end_matches('/')
    );
    let projects_dir = args
        .project_dir
        .parent()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| ".".to_string());
    let session = args
        .tmux_session
        .clone()
        .unwrap_or_else(|| default_tmux_session(&args.project_dir));

    println!("# RustWX WRF ops recipe");
    println!(
        "# init={} products={}",
        plan.request.init_source,
        plan.wps_products.join(",")
    );
    println!(
        "# inner_domain=d{:02} dx={:.0}m grid={}x{} run={}h{}m",
        plan.inner_domain().id,
        plan.inner_domain().dx_m,
        plan.inner_domain().e_we,
        plan.inner_domain().e_sn,
        plan.run_hours,
        plan.run_minutes
    );
    println!();

    if args.include_bootstrap {
        println!("# Bootstrap WRF/WPS on a fresh node from a RustWX checkout:");
        println!(
            "cargo run -p rustwx-cli --bin wrf_ops -- bootstrap-script --base {} > /tmp/bootstrap-wrf.sh",
            shell_quote("/home/drew/weather/wrf")
        );
        println!("bash /tmp/bootstrap-wrf.sh");
        println!();
    }

    println!("# 1) Audit node readiness");
    println!(
        "{} doctor --wrf-build-path {} --geog-data-path {} --rustwx-bin-dir {} --projects-dir {}",
        shell_quote(&bin),
        shell_quote(&args.common.wrf_build_path),
        shell_quote(&args.common.geog_data_path),
        shell_quote(&args.common.rustwx_bin_dir),
        shell_quote(&projects_dir)
    );
    println!();

    println!("# 2) Create the project");
    println!(
        "{}",
        recipe_create_command(&bin, &args.common, &args.project_dir)?
    );
    println!();

    println!("# 3) Launch and monitor");
    let mut launch = vec![
        shell_quote(&bin),
        "launch".to_string(),
        "--project-dir".to_string(),
        shell_quote(&args.project_dir.display().to_string()),
        "--tmux-session".to_string(),
        shell_quote(&session),
    ];
    if let Some(source) = args.source {
        launch.push("--source".to_string());
        launch.push(shell_quote(source.as_str()));
    }
    if let Some(cache_dir) = &args.cache_dir {
        launch.push("--cache-dir".to_string());
        launch.push(shell_quote(&cache_dir.display().to_string()));
    }
    if let Some(cycle_utc) = &args.cycle_utc {
        launch.push("--cycle-utc".to_string());
        launch.push(shell_quote(cycle_utc));
    }
    if args.overwrite_gribs {
        launch.push("--overwrite-gribs".to_string());
    }
    if plan.request.init_source.rustwx_model_id().is_none() {
        launch.push("--skip-stage-gribs".to_string());
        println!("# This init source is external-GRIB mode; put GRIBs in data/grib first.");
    }
    println!("{}", launch.join(" "));
    println!(
        "{} status --project-dir {} --tmux-session {}",
        shell_quote(&bin),
        shell_quote(&args.project_dir.display().to_string()),
        shell_quote(&session)
    );
    println!("tmux attach -t {}", shell_quote(&session));
    Ok(())
}

fn recipe_create_command(
    bin: &str,
    args: &WrfPlanCommonArgs,
    project_dir: &Path,
) -> anyhow::Result<String> {
    let bounds = resolve_domain_bounds(args)?;
    let mut parts = vec![
        shell_quote(bin),
        "create".to_string(),
        "--project-name".to_string(),
        shell_quote(&args.project_name),
        "--init".to_string(),
        shell_quote(&args.init.to_string()),
        "--start-utc".to_string(),
        shell_quote(&args.start_utc),
        "--end-utc".to_string(),
        shell_quote(&args.end_utc),
        "--west".to_string(),
        bounds.west_lon_deg.to_string(),
        "--east".to_string(),
        bounds.east_lon_deg.to_string(),
        "--south".to_string(),
        bounds.south_lat_deg.to_string(),
        "--north".to_string(),
        bounds.north_lat_deg.to_string(),
        "--resolution".to_string(),
        args.resolution
            .to_possible_value()
            .map(|value| value.get_name().to_string())
            .unwrap_or_else(|| "default3km".to_string()),
        "--parent-ratio".to_string(),
        args.parent_ratio.to_string(),
        "--history-interval-minutes".to_string(),
        args.history_interval_minutes.to_string(),
        "--num-cores".to_string(),
        args.num_cores.to_string(),
        "--physics".to_string(),
        args.physics
            .to_possible_value()
            .map(|value| value.get_name().to_string())
            .unwrap_or_else(|| "severe-convection".to_string()),
        "--plot-preset".to_string(),
        args.plot_preset.slug().to_string(),
        "--geog-data-path".to_string(),
        shell_quote(&args.geog_data_path),
        "--wrf-build-path".to_string(),
        shell_quote(&args.wrf_build_path),
        "--rustwx-bin-dir".to_string(),
        shell_quote(&args.rustwx_bin_dir),
        "--project-dir".to_string(),
        shell_quote(&project_dir.display().to_string()),
    ];
    if let Some(inner_dx_m) = args.inner_dx_m {
        parts.push("--inner-dx-m".to_string());
        parts.push(inner_dx_m.to_string());
    }
    if !args.nested {
        parts.push("--nested=false".to_string());
    }
    if let Some(output_interval) = args.output_3d_interval_minutes {
        parts.push("--output-3d-interval-minutes".to_string());
        parts.push(output_interval.to_string());
    }
    if let Some(levels) = args.num_metgrid_levels {
        parts.push("--num-metgrid-levels".to_string());
        parts.push(levels.to_string());
    }
    if let Some(levels) = args.num_metgrid_soil_levels {
        parts.push("--num-metgrid-soil-levels".to_string());
        parts.push(levels.to_string());
    }
    if let Some(products) = &args.wps_products {
        if !products.is_empty() {
            parts.push("--wps-products".to_string());
            parts.push(shell_quote(&products.join(",")));
        }
    }
    Ok(parts.join(" "))
}

#[cfg(test)]
#[path = "wrf_ops/tests.rs"]
mod tests;

fn intel_bootstrap_script(base: &str, wrf_version: &str, wps_version: &str) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
BASE="{base}"
WEATHER_ROOT="$(cd "$BASE/.." && pwd)"
BUILD="$BASE/WRF_BUILD"
LIBS="$BUILD/LIBRARIES"
SRC="$BUILD/src"
NETCDF="$LIBS/netcdf"
WRF_DIR="$BUILD/WRF"
WPS_DIR="$BUILD/WPS"
RUSTWX_DIR="${{RUSTWX_DIR:-$WEATHER_ROOT/apps/rustwx}}"
LOG_DIR="$BASE/logs"
mkdir -p "$LIBS" "$SRC" "$LOG_DIR" "$BASE/projects"
exec > >(tee -a "$LOG_DIR/bootstrap-intel-wrf-$(date -u +%Y%m%dT%H%M%SZ).log") 2>&1

echo "== apt/dpkg audit =="
pgrep -af 'apt|dpkg' || true
sudo dpkg --audit
df -h /
ip -br addr

echo "== base packages =="
sudo apt-get update
sudo apt-get install -y \
  ca-certificates gpg-agent wget curl git make m4 perl csh tcsh \
  build-essential gfortran pkg-config cmake time file unzip \
  libhdf5-dev zlib1g-dev libpng-dev libxml2-dev

echo "== Rust toolchain =="
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal
fi
if [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi
cargo --version

echo "== Intel oneAPI apt repo =="
wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
  | gpg --dearmor \
  | sudo tee /usr/share/keyrings/oneapi-archive-keyring.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" \
  | sudo tee /etc/apt/sources.list.d/oneAPI.list >/dev/null
sudo apt-get update

choose_pkg() {{
  local candidate
  for candidate in "$@"; do
    if apt-cache show "$candidate" >/dev/null 2>&1; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}}

FORTRAN_PKG="$(choose_pkg intel-oneapi-compiler-fortran intel-fortran-essentials)"
CPP_PKG="$(choose_pkg intel-oneapi-compiler-dpcpp-cpp intel-cpp-essentials)"
MPI_PKG="$(choose_pkg intel-oneapi-mpi-devel intel-mpi-devel)"
sudo apt-get install -y "$FORTRAN_PKG" "$CPP_PKG" "$MPI_PKG"

set +u
source /opt/intel/oneapi/setvars.sh --force
set -u

export CC=icx
export CXX=icpx
export FC=ifx
export F77=ifx
export I_MPI_F90=ifx
export I_MPI_F77=ifx
export I_MPI_CC=icx
export I_MPI_CXX=icpx
export WRFIO_NCD_LARGE_FILE_SUPPORT=1
export NETCDF="$NETCDF"
export PATH="$NETCDF/bin:/opt/intel/oneapi/mpi/latest/bin:$PATH"
export LD_LIBRARY_PATH="$NETCDF/lib:/opt/intel/oneapi/mpi/latest/lib:${{LD_LIBRARY_PATH:-}}"

echo "== compiler versions =="
ifx --version | head -2
icx --version | head -2
mpiifx --version | head -2 || true
ln -sfn /opt/intel/oneapi/mpi/latest "$LIBS/mpich"

download() {{
  local url=$1
  local out=$2
  if [ ! -f "$out" ]; then
    wget -O "$out" "$url"
  fi
}}

echo "== source trees =="
cd "$SRC"
if [ ! -d "$WRF_DIR/.git" ]; then
  git clone --branch "v{wrf_version}" --depth 1 https://github.com/wrf-model/WRF.git "$WRF_DIR"
fi
if [ ! -d "$WPS_DIR/.git" ]; then
  git clone --branch "v{wps_version}" --depth 1 https://github.com/wrf-model/WPS.git "$WPS_DIR"
fi

echo "== static NetCDF C/Fortran with Intel compilers =="
download "https://downloads.unidata.ucar.edu/netcdf-c/4.9.3/netcdf-c-4.9.3.tar.gz" "netcdf-c-4.9.3.tar.gz"
if [ ! -x "$NETCDF/bin/nc-config" ]; then
  rm -rf "$NETCDF" netcdf-c-4.9.3
  tar -xzf netcdf-c-4.9.3.tar.gz
  cd netcdf-c-4.9.3
  ./configure --prefix="$NETCDF" --disable-netcdf-4 --disable-dap --disable-quantize \
    --disable-shared --enable-static CC=icx CXX=icpx CFLAGS="-O3 -fPIC" CXXFLAGS="-O3 -fPIC"
  make -j "$(nproc)"
  make install
fi

cd "$SRC"
if [ ! -d netcdf-fortran-4.6.2 ]; then
  git clone --depth 1 --branch v4.6.2 https://github.com/Unidata/netcdf-fortran.git netcdf-fortran-4.6.2
fi
if [ ! -f "$NETCDF/include/netcdf.inc" ] || [ ! -f "$NETCDF/lib/libnetcdff.a" ]; then
  cd netcdf-fortran-4.6.2
  git reset --hard
  git clean -fdx
  for f in fortran/*.F90; do ln -sfn "$(basename "$f")" "${{f%.F90}}.f90"; done
  CPPFLAGS="-I$NETCDF/include" LDFLAGS="-L$NETCDF/lib" LIBS="-lnetcdf" \
    ./configure --prefix="$NETCDF" --disable-shared --enable-static \
    CC=icx FC=ifx F77=ifx FCFLAGS="-O2 -fPIC" FFLAGS="-O2 -fPIC"
  make -j "$(nproc)"
  make install
fi

echo "== WRF em_real Intel oneAPI dmpar =="
cd "$WRF_DIR"
./clean -a
rm -f configure.wrf
printf "78\n1\n" | ./configure
grep -q "^SFC[[:space:]]*=[[:space:]]*ifx" configure.wrf
./compile -j "$(nproc)" em_real
test -x main/wrf.exe
test -x main/real.exe
test -x main/ndown.exe

echo "== WPS Intel oneAPI dmpar =="
cd "$WPS_DIR"
./clean -a
rm -f configure.wps
export WRF_DIR="$WRF_DIR"
if printf "10\n" | ./configure --build-grib2-libs; then
  :
else
  printf "19\n" | ./configure --build-grib2-libs
fi
grep -q "^SFC[[:space:]]*=[[:space:]]*ifx" configure.wps
./compile

build_wps_grib2_with_gcc() {{
  local prefix="$WPS_DIR/grib2"
  (
    cd "$WPS_DIR/external/zlib-1.2.11"
    make clean || true
    CC=gcc CFLAGS="-O2 -fPIC" ./configure --static --prefix="$prefix"
    make -j "$(nproc)"
    make install
  )
  (
    cd "$WPS_DIR/external/libpng-1.6.37"
    make clean || true
    CPPFLAGS="-I$prefix/include" LDFLAGS="-L$prefix/lib" \
      CC=gcc CFLAGS="-O2 -fPIC" \
      ./configure --prefix="$prefix" --disable-shared --enable-static
    make -j "$(nproc)"
    make install
  )
  (
    cd "$WPS_DIR/external/jasper-1.900.29"
    make clean || true
    CPPFLAGS="-I$prefix/include" LDFLAGS="-L$prefix/lib" \
      CC=gcc CFLAGS="-O2 -fPIC" \
      ./configure --prefix="$prefix" --disable-shared
    find . -name Makefile.am -exec touch -d 2020-01-01 {{}} +
    touch configure.ac ac_m4/*.m4 aclocal.m4 configure config.status || true
    sleep 1
    find . -name "*.h.in" -exec touch {{}} +
    find . -name Makefile.in -exec touch {{}} +
    sleep 1
    find . -name "*.h" -exec touch {{}} +
    find . -name Makefile -exec touch {{}} +
    make -j "$(nproc)"
    make install
  )
}}

build_wps_ungrib_with_gfortran() {{
  (
    cd "$WPS_DIR"
    cp configure.wps "configure.wps.ifx-before-gnu-ungrib"
    sed -i -E 's|^RANLIB[[:space:]]*=.*|RANLIB              = ranlib|' configure.wps
    sed -i -E 's|^SFC[[:space:]]*=.*|SFC                 = gfortran|' configure.wps
    sed -i -E 's|^SCC[[:space:]]*=.*|SCC                 = gcc|' configure.wps
    sed -i -E 's|^FC[[:space:]]*=.*|FC                  = $(SFC)|' configure.wps
    sed -i -E 's|^CC[[:space:]]*=.*|CC                  = $(SCC)|' configure.wps
    sed -i -E 's|^LD[[:space:]]*=.*|LD                  = $(FC)|' configure.wps
    sed -i -E 's|^FFLAGS[[:space:]]*=.*|FFLAGS              = $(FORMAT_FREE) -O -fconvert=big-endian -frecord-marker=4|' configure.wps
    sed -i -E 's|^F77FLAGS[[:space:]]*=.*|F77FLAGS            = $(FORMAT_FIXED) -O -fconvert=big-endian -frecord-marker=4|' configure.wps
    sed -i -E 's|^FCSUFFIX[[:space:]]*=.*|FCSUFFIX            =|' configure.wps
    sed -i -E 's|^FNGFLAGS[[:space:]]*=.*|FNGFLAGS            = $(FFLAGS)|' configure.wps
    sed -i -E 's|^LDFLAGS[[:space:]]*=.*|LDFLAGS             = -no-pie|' configure.wps
    sed -i -E 's|^CFLAGS[[:space:]]*=.*|CFLAGS              =|' configure.wps
    sed -i -E 's|^CPP[[:space:]]*=.*|CPP                 = /usr/bin/cpp -P -traditional|' configure.wps
    sed -i -E 's|^CPPFLAGS[[:space:]]*=.*|CPPFLAGS            = -D_UNDERSCORE -DBYTESWAP -DLINUX -DIO_NETCDF -DBIT32 -DNO_SIGNAL|' configure.wps
    grep -q '^FORMAT_FREE' configure.wps || \
      sed -i '/^F77FLAGS/a FORMAT_FREE         = -ffree-form\nFORMAT_FIXED        = -ffixed-form\nFCCOMPAT            = -fallow-argument-mismatch' configure.wps
    sed -i -E 's|^FCCOMPAT[[:space:]]*=.*|FCCOMPAT            = -fallow-argument-mismatch|' configure.wps
    cd ungrib/src/ngl
    make DEV_TOP="$WPS_DIR" clean || true
    make DEV_TOP="$WPS_DIR" all
    cd ..
    make DEV_TOP="$WPS_DIR" clean || true
    make DEV_TOP="$WPS_DIR" ungrib.exe
    cd "$WPS_DIR"
    ln -sfn ungrib/src/ungrib.exe ungrib.exe
  )
}}

echo "== WPS GNU ungrib against GCC GRIB2 libraries =="
build_wps_grib2_with_gcc
build_wps_ungrib_with_gfortran

test -x geogrid.exe
test -x ungrib.exe
test -x metgrid.exe
ln -sfn "$WPS_DIR/grib2" "$LIBS/grib2"

echo "== WPS geog high-res mandatory static data =="
GEOG_DIR="$BUILD/WPS_GEOG"
GEOG_TAR="$SRC/geog_high_res_mandatory.tar.gz"
mkdir -p "$GEOG_DIR"
if [ ! -d "$GEOG_DIR/topo_gmted2010_30s" ] && [ ! -d "$GEOG_DIR/geog/topo_gmted2010_30s" ]; then
  download "https://www2.mmm.ucar.edu/wrf/src/wps_files/geog_high_res_mandatory.tar.gz" "$GEOG_TAR"
  tar -xzf "$GEOG_TAR" -C "$GEOG_DIR" --strip-components=1
fi

echo "== RustWX WRF ops binaries =="
if [ -f "$RUSTWX_DIR/Cargo.toml" ]; then
  cd "$RUSTWX_DIR"
  cargo build --release -p rustwx-cli --bin wrf_ops
  cargo build --release -p rustwx-cli --bin wrf_local_showcase || true
  "$RUSTWX_DIR/target/release/wrf_ops" doctor \
    --wrf-build-path "$BUILD" \
    --geog-data-path "$GEOG_DIR" \
    --rustwx-bin-dir "$RUSTWX_DIR/target/release" \
    --projects-dir "$BASE/projects"
else
  echo "RustWX checkout not found at $RUSTWX_DIR; copy or clone it there, then build wrf_ops."
fi

echo "WRF/WPS Intel oneAPI build complete at $BUILD"
"#
    )
}
