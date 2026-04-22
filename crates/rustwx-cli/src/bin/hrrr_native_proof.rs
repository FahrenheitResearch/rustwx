use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use region::RegionPreset;
use rustwx_cli::cross_section_proof::{
    CrossSectionRunOutput, CrossSectionSummary as CrossSectionProofSummary,
    PressureCrossSectionRequest, default_native_cross_section_requests, run_pressure_cross_section,
};
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::supported_derived_recipe_slugs;
use rustwx_products::direct::supported_direct_recipe_slugs;
use rustwx_products::non_ecape::{
    HrrrNonEcapeHourReport, HrrrNonEcapeHourRequest, run_hrrr_non_ecape_hour,
};
use rustwx_products::publication::{
    atomic_write_json, canonical_run_slug, publish_failure_manifest,
};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceModeArg {
    Canonical,
    Fastest,
}

impl SourceModeArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::Fastest => "fastest",
        }
    }
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
enum RunnerModeArg {
    Suite,
    Custom,
}

impl RunnerModeArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Suite => "suite",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
enum ProofCaseArg {
    MidwestCore,
    ConusContour,
    SouthernPlainsSevere,
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-native-proof",
    about = "Generate a tight HRRR proof suite of representative weather-native plots"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = RunnerModeArg::Suite)]
    mode: RunnerModeArg,
    #[arg(long, value_enum, value_delimiter = ',', num_args = 1..)]
    case: Vec<ProofCaseArg>,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(long, hide = true)]
    suite_child_output: Option<PathBuf>,
    #[arg(long, default_value = "proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long = "source-mode", alias = "thermo-path", value_enum, default_value_t = SourceModeArg::Canonical)]
    source_mode: SourceModeArg,
}

#[derive(Debug, Clone, Copy)]
struct ProofCaseDefinition {
    arg: ProofCaseArg,
    slug: &'static str,
    label: &'static str,
    theme: &'static str,
    region: RegionPreset,
    direct_recipes: &'static [&'static str],
    derived_recipes: &'static [&'static str],
    notes: &'static [&'static str],
    projected: bool,
    contour_candidate: bool,
}

const SUITE_CASES: &[ProofCaseDefinition] = &[
    ProofCaseDefinition {
        arg: ProofCaseArg::MidwestCore,
        slug: "midwest_core",
        label: "Midwest Convective Core",
        theme: "local convective weather-native proof",
        region: RegionPreset::Midwest,
        direct_recipes: &["composite_reflectivity"],
        derived_recipes: &["sbcape"],
        notes: &[
            "Compact warm-sector proof for basic HRRR native field rendering.",
            "Keeps one direct reflectivity panel and one derived thermodynamic panel in the suite.",
        ],
        projected: true,
        contour_candidate: false,
    },
    ProofCaseDefinition {
        arg: ProofCaseArg::ConusContour,
        slug: "conus_contour",
        label: "CONUS Projected Contour / Synoptic",
        theme: "projected contour-sensitive synoptic proof",
        region: RegionPreset::Conus,
        direct_recipes: &["mslp_10m_winds", "500mb_temperature_height_winds"],
        derived_recipes: &[],
        notes: &[
            "MSLP / 10m Winds is the contour-sensitive projected map to rerun as contour integration improves.",
            "500mb Temperature / Height / Winds is the projected synoptic companion proof.",
        ],
        projected: true,
        contour_candidate: true,
    },
    ProofCaseDefinition {
        arg: ProofCaseArg::SouthernPlainsSevere,
        slug: "southern_plains_severe",
        label: "Southern Plains Projected Severe",
        theme: "projected severe diagnostic proof",
        region: RegionPreset::SouthernPlains,
        direct_recipes: &[],
        derived_recipes: &["stp_fixed"],
        notes: &[
            "Projected severe marker that exercises fixed-layer composite diagnostics in a classic warm-season domain.",
        ],
        projected: true,
        contour_candidate: false,
    },
];

#[derive(Debug, Serialize, Deserialize)]
struct ProofArtifactRecord {
    case_slug: String,
    lane: String,
    recipe_slug: String,
    title: String,
    source_route: Option<String>,
    output_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProofBlockerRecord {
    case_slug: String,
    lane: String,
    recipe_slug: String,
    source_route: Option<String>,
    reason: String,
}

#[derive(Debug, Serialize)]
struct CrossSectionHookSummary {
    status: &'static str,
    kind: &'static str,
    rationale: &'static str,
    proof_count: usize,
    proofs: Vec<CrossSectionProofSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NativeProofCaseSummary {
    slug: String,
    label: String,
    theme: String,
    region: String,
    cycle_utc: u8,
    projected: bool,
    contour_candidate: bool,
    notes: Vec<String>,
    direct_recipes: Vec<String>,
    derived_recipes: Vec<String>,
    output_count: usize,
    blocker_count: usize,
    outputs: Vec<ProofArtifactRecord>,
    blockers: Vec<ProofBlockerRecord>,
    shared_timing: rustwx_products::non_ecape::HrrrNonEcapeSharedTiming,
    total_ms: u128,
    publication_manifest_path: PathBuf,
    attempt_manifest_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct NativeProofSummary {
    runner: &'static str,
    model: &'static str,
    mode: String,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    out_dir: PathBuf,
    cache_root: PathBuf,
    use_cache: bool,
    source_mode: String,
    selected_region: Option<String>,
    selected_suite_cases: Vec<String>,
    case_count: usize,
    output_count: usize,
    cross_section_proof_count: usize,
    blocker_count: usize,
    cases: Vec<NativeProofCaseSummary>,
    cross_section_hook_path: PathBuf,
}

struct CrossSectionLaneRun {
    hook: CrossSectionHookSummary,
    proofs: Vec<CrossSectionRunOutput>,
}

fn default_custom_direct_recipes() -> Vec<String> {
    vec!["composite_reflectivity", "500mb_temperature_height_winds"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_custom_derived_recipes() -> Vec<String> {
    vec!["sbcape", "stp_fixed"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let failure_region = failure_region_slug(&args);
    let failure_slug = canonical_run_slug(
        "hrrr",
        &args.date,
        args.cycle,
        args.forecast_hour,
        failure_region,
        "native_proof",
    );
    let failure_out_dir = args.out_dir.clone();
    if let Err(err) = run(&args) {
        let _ = publish_failure_manifest(
            "hrrr_native_proof",
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
    validate_args(args)?;

    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    match args.mode {
        RunnerModeArg::Suite => run_suite(args, &cache_root),
        RunnerModeArg::Custom => run_custom(args, &cache_root),
    }
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.suite_child_output.is_some() && args.mode != RunnerModeArg::Custom {
        return Err("`--suite-child-output` is only valid in custom mode".into());
    }
    match args.mode {
        RunnerModeArg::Suite => {
            if !args.direct_recipes.is_empty() || !args.derived_recipes.is_empty() {
                return Err(
                    "suite mode uses the built-in proof cases; switch to --mode custom for ad hoc recipe lists"
                        .into(),
                );
            }
        }
        RunnerModeArg::Custom => {
            if !args.case.is_empty() {
                return Err("`--case` is only valid in suite mode".into());
            }
        }
    }
    Ok(())
}

fn run_suite(args: &Args, cache_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let case_summaries = selected_suite_case_definitions(args)
        .iter()
        .map(|definition| run_suite_case_subprocess(args, cache_root, definition))
        .collect::<Result<Vec<_>, _>>()?;
    if case_summaries.is_empty() {
        return Err("native proof runner did not emit any proof cases".into());
    }

    let first_case = case_summaries
        .first()
        .ok_or("native proof runner did not emit a first proof case")?;
    let cross_section_lane = run_cross_section_lane(args, cache_root, first_case.cycle_utc)?;
    let stem = canonical_run_slug(
        "hrrr",
        &args.date,
        Some(first_case.cycle_utc),
        args.forecast_hour,
        failure_region_slug(args),
        "native_proof",
    );
    let cross_section_hook_path = args.out_dir.join(format!("{stem}_cross_section_hook.json"));
    let summary_path = args.out_dir.join(format!("{stem}_summary.json"));
    atomic_write_json(&cross_section_hook_path, &cross_section_lane.hook)?;

    let output_count = case_summaries
        .iter()
        .map(|case| case.output_count)
        .sum::<usize>()
        + cross_section_lane.hook.proof_count;
    let blocker_count = case_summaries.iter().map(|case| case.blocker_count).sum();
    let summary = NativeProofSummary {
        runner: "hrrr_native_proof",
        model: "hrrr",
        mode: args.mode.as_str().to_string(),
        date_yyyymmdd: args.date.clone(),
        cycle_utc: first_case.cycle_utc,
        forecast_hour: args.forecast_hour,
        source: args.source,
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.to_path_buf(),
        use_cache: !args.no_cache,
        source_mode: args.source_mode.as_str().to_string(),
        selected_region: None,
        selected_suite_cases: selected_suite_case_definitions(args)
            .iter()
            .map(|case| case.slug.to_string())
            .collect(),
        case_count: case_summaries.len(),
        output_count,
        cross_section_proof_count: cross_section_lane.hook.proof_count,
        blocker_count,
        cases: case_summaries,
        cross_section_hook_path: relative_output_path(&args.out_dir, &cross_section_hook_path),
    };
    atomic_write_json(&summary_path, &summary)?;

    for case in &summary.cases {
        for output in &case.outputs {
            println!("{}", args.out_dir.join(&output.output_path).display());
        }
    }
    print_cross_section_artifacts(&cross_section_lane.proofs);
    println!("{}", cross_section_hook_path.display());
    println!("{}", summary_path.display());
    Ok(())
}

fn run_custom(args: &Args, cache_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let case_summary = run_custom_case(args, cache_root)?;
    if let Some(path) = &args.suite_child_output {
        atomic_write_json(path, &case_summary)?;
        return Ok(());
    }

    let cross_section_lane = run_cross_section_lane(args, cache_root, case_summary.cycle_utc)?;
    let stem = canonical_run_slug(
        "hrrr",
        &args.date,
        Some(case_summary.cycle_utc),
        args.forecast_hour,
        failure_region_slug(args),
        "native_proof",
    );
    let cross_section_hook_path = args.out_dir.join(format!("{stem}_cross_section_hook.json"));
    let summary_path = args.out_dir.join(format!("{stem}_summary.json"));
    atomic_write_json(&cross_section_hook_path, &cross_section_lane.hook)?;

    let summary = NativeProofSummary {
        runner: "hrrr_native_proof",
        model: "hrrr",
        mode: args.mode.as_str().to_string(),
        date_yyyymmdd: args.date.clone(),
        cycle_utc: case_summary.cycle_utc,
        forecast_hour: args.forecast_hour,
        source: args.source,
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.to_path_buf(),
        use_cache: !args.no_cache,
        source_mode: args.source_mode.as_str().to_string(),
        selected_region: Some(args.region.slug().to_string()),
        selected_suite_cases: Vec::new(),
        case_count: 1,
        output_count: case_summary.output_count + cross_section_lane.hook.proof_count,
        cross_section_proof_count: cross_section_lane.hook.proof_count,
        blocker_count: case_summary.blocker_count,
        cases: vec![case_summary],
        cross_section_hook_path: relative_output_path(&args.out_dir, &cross_section_hook_path),
    };
    atomic_write_json(&summary_path, &summary)?;

    for case in &summary.cases {
        for output in &case.outputs {
            println!("{}", args.out_dir.join(&output.output_path).display());
        }
    }
    print_cross_section_artifacts(&cross_section_lane.proofs);
    println!("{}", cross_section_hook_path.display());
    println!("{}", summary_path.display());
    Ok(())
}

fn run_suite_case_subprocess(
    args: &Args,
    cache_root: &Path,
    definition: &ProofCaseDefinition,
) -> Result<NativeProofCaseSummary, Box<dyn std::error::Error>> {
    let supported_direct = supported_direct_recipe_slugs(ModelId::Hrrr);
    let supported_derived = supported_derived_recipe_slugs(ModelId::Hrrr);

    for recipe in definition.direct_recipes {
        if !supported_direct.iter().any(|supported| supported == recipe) {
            return Err(format!(
                "suite case '{}' expects unsupported HRRR direct recipe '{}'",
                definition.slug, recipe
            )
            .into());
        }
    }
    for recipe in definition.derived_recipes {
        if !supported_derived
            .iter()
            .any(|supported| supported == recipe)
        {
            return Err(format!(
                "suite case '{}' expects unsupported HRRR derived recipe '{}'",
                definition.slug, recipe
            )
            .into());
        }
    }

    let cycle = args.cycle.unwrap_or(255);
    let child_output_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_native_proof_case.json",
        args.date, cycle, args.forecast_hour, definition.slug
    ));
    let region_arg = definition
        .region
        .to_possible_value()
        .map(|value| value.get_name().to_string())
        .unwrap_or_else(|| definition.region.slug().replace('_', "-"));
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--mode")
        .arg("custom")
        .arg("--date")
        .arg(&args.date)
        .arg("--forecast-hour")
        .arg(args.forecast_hour.to_string())
        .arg("--source")
        .arg(args.source.as_str())
        .arg("--region")
        .arg(&region_arg)
        .arg("--out-dir")
        .arg(&args.out_dir)
        .arg("--source-mode")
        .arg(args.source_mode.as_str())
        .arg("--suite-child-output")
        .arg(&child_output_path);
    if let Some(cycle) = args.cycle {
        command.arg("--cycle").arg(cycle.to_string());
    }
    if args.no_cache {
        command.arg("--no-cache");
    }
    if cache_root != default_proof_cache_dir(&args.out_dir).as_path() {
        command.arg("--cache-dir").arg(cache_root);
    }
    for recipe in definition.direct_recipes {
        command.arg("--direct-recipe").arg(recipe);
    }
    for recipe in definition.derived_recipes {
        command.arg("--derived-recipe").arg(recipe);
    }

    let status = command.status()?;
    if !status.success() {
        return Err(format!(
            "suite child for '{}' exited with status {}",
            definition.slug, status
        )
        .into());
    }

    let bytes = fs::read(&child_output_path)?;
    let mut case_summary = serde_json::from_slice::<NativeProofCaseSummary>(&bytes)?;
    fs::remove_file(&child_output_path)?;
    case_summary.slug = definition.slug.to_string();
    case_summary.label = definition.label.to_string();
    case_summary.theme = definition.theme.to_string();
    case_summary.region = definition.region.slug().to_string();
    case_summary.projected = definition.projected;
    case_summary.contour_candidate = definition.contour_candidate;
    case_summary.notes = definition
        .notes
        .iter()
        .map(|note| (*note).to_string())
        .collect();
    case_summary.direct_recipes = definition
        .direct_recipes
        .iter()
        .map(|recipe| (*recipe).to_string())
        .collect();
    case_summary.derived_recipes = definition
        .derived_recipes
        .iter()
        .map(|recipe| (*recipe).to_string())
        .collect();
    for output in &mut case_summary.outputs {
        output.case_slug = definition.slug.to_string();
    }
    for blocker in &mut case_summary.blockers {
        blocker.case_slug = definition.slug.to_string();
    }
    Ok(case_summary)
}

fn run_custom_case(
    args: &Args,
    cache_root: &Path,
) -> Result<NativeProofCaseSummary, Box<dyn std::error::Error>> {
    let supported_direct = supported_direct_recipe_slugs(ModelId::Hrrr);
    let supported_derived = supported_derived_recipe_slugs(ModelId::Hrrr);
    let (direct_recipes, derived_recipes) =
        resolve_custom_recipe_slugs(args, &supported_direct, &supported_derived);

    run_case(
        args,
        cache_root,
        args.region.slug(),
        "Custom HRRR Proof",
        "single-region ad hoc proof",
        args.region,
        true,
        false,
        vec!["Custom mode keeps the older single-region behavior for focused reruns.".to_string()],
        direct_recipes,
        derived_recipes,
    )
}

fn resolve_custom_recipe_slugs(
    args: &Args,
    supported_direct: &[String],
    supported_derived: &[String],
) -> (Vec<String>, Vec<String>) {
    let direct_recipes = if args.direct_recipes.is_empty() {
        if should_use_custom_default_recipes(args) {
            default_custom_direct_recipes()
                .into_iter()
                .filter(|recipe| supported_direct.contains(recipe))
                .collect()
        } else {
            Vec::new()
        }
    } else {
        args.direct_recipes.clone()
    };
    let derived_recipes = if args.derived_recipes.is_empty() {
        if should_use_custom_default_recipes(args) {
            default_custom_derived_recipes()
                .into_iter()
                .filter(|recipe| supported_derived.contains(recipe))
                .collect()
        } else {
            Vec::new()
        }
    } else {
        args.derived_recipes.clone()
    };
    (direct_recipes, derived_recipes)
}

fn should_use_custom_default_recipes(args: &Args) -> bool {
    args.suite_child_output.is_none()
}

#[allow(clippy::too_many_arguments)]
fn run_case(
    args: &Args,
    cache_root: &Path,
    case_slug: &str,
    label: &str,
    theme: &str,
    region: RegionPreset,
    projected: bool,
    contour_candidate: bool,
    notes: Vec<String>,
    direct_recipe_slugs: Vec<String>,
    derived_recipe_slugs: Vec<String>,
) -> Result<NativeProofCaseSummary, Box<dyn std::error::Error>> {
    let request = HrrrNonEcapeHourRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(region.slug(), region.bounds()),
        out_dir: args.out_dir.clone(),
        cache_root: cache_root.to_path_buf(),
        use_cache: !args.no_cache,
        source_mode: args.source_mode.into(),
        direct_recipe_slugs: direct_recipe_slugs.clone(),
        derived_recipe_slugs: derived_recipe_slugs.clone(),
        windowed_products: Vec::new(),
        output_width: 1200,
        output_height: 900,
        png_compression: rustwx_render::PngCompressionMode::Default,
    };
    let report = run_hrrr_non_ecape_hour(&request)?;

    Ok(build_case_summary(
        &args.out_dir,
        case_slug,
        label,
        theme,
        region,
        projected,
        contour_candidate,
        notes,
        direct_recipe_slugs,
        derived_recipe_slugs,
        &report,
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_case_summary(
    out_dir: &Path,
    case_slug: &str,
    label: &str,
    theme: &str,
    region: RegionPreset,
    projected: bool,
    contour_candidate: bool,
    notes: Vec<String>,
    direct_recipes: Vec<String>,
    derived_recipes: Vec<String>,
    report: &HrrrNonEcapeHourReport,
) -> NativeProofCaseSummary {
    let outputs = collect_outputs(out_dir, case_slug, report);
    let blockers = collect_blockers(case_slug, report);
    NativeProofCaseSummary {
        slug: case_slug.to_string(),
        label: label.to_string(),
        theme: theme.to_string(),
        region: region.slug().to_string(),
        cycle_utc: report.cycle_utc,
        projected,
        contour_candidate,
        notes,
        direct_recipes,
        derived_recipes,
        output_count: outputs.len(),
        blocker_count: blockers.len(),
        outputs,
        blockers,
        shared_timing: report.shared_timing.clone(),
        total_ms: report.total_ms,
        publication_manifest_path: relative_output_path(out_dir, &report.publication_manifest_path),
        attempt_manifest_path: report
            .attempt_manifest_path
            .as_ref()
            .map(|path| relative_output_path(out_dir, path)),
    }
}

fn collect_outputs(
    out_dir: &Path,
    case_slug: &str,
    report: &HrrrNonEcapeHourReport,
) -> Vec<ProofArtifactRecord> {
    let mut outputs = Vec::new();
    if let Some(direct) = &report.direct {
        for recipe in &direct.recipes {
            outputs.push(ProofArtifactRecord {
                case_slug: case_slug.to_string(),
                lane: "direct".to_string(),
                recipe_slug: recipe.recipe_slug.clone(),
                title: recipe.title.clone(),
                source_route: Some(recipe.source_route.as_str().to_string()),
                output_path: relative_output_path(out_dir, &recipe.output_path),
            });
        }
    }
    if let Some(derived) = &report.derived {
        for recipe in &derived.recipes {
            outputs.push(ProofArtifactRecord {
                case_slug: case_slug.to_string(),
                lane: "derived".to_string(),
                recipe_slug: recipe.recipe_slug.clone(),
                title: recipe.title.clone(),
                source_route: Some(recipe.source_route.as_str().to_string()),
                output_path: relative_output_path(out_dir, &recipe.output_path),
            });
        }
    }
    outputs
}

fn collect_blockers(case_slug: &str, report: &HrrrNonEcapeHourReport) -> Vec<ProofBlockerRecord> {
    let mut blockers = Vec::new();
    if let Some(direct) = &report.direct {
        for blocker in &direct.blockers {
            blockers.push(ProofBlockerRecord {
                case_slug: case_slug.to_string(),
                lane: "direct".to_string(),
                recipe_slug: blocker.recipe_slug.clone(),
                source_route: None,
                reason: blocker.reason.clone(),
            });
        }
    }
    if let Some(derived) = &report.derived {
        for blocker in &derived.blockers {
            blockers.push(ProofBlockerRecord {
                case_slug: case_slug.to_string(),
                lane: "derived".to_string(),
                recipe_slug: blocker.recipe_slug.clone(),
                source_route: Some(blocker.source_route.as_str().to_string()),
                reason: blocker.reason.clone(),
            });
        }
    }
    blockers
}

fn run_cross_section_lane(
    args: &Args,
    cache_root: &Path,
    cycle_utc: u8,
) -> Result<CrossSectionLaneRun, Box<dyn std::error::Error>> {
    let proofs = planned_cross_section_requests(args, cache_root, cycle_utc)
        .into_iter()
        .map(|request| run_pressure_cross_section(&request))
        .collect::<Result<Vec<_>, _>>()?;
    let hook = build_cross_section_hook(proofs.iter().map(|proof| proof.summary.clone()));
    Ok(CrossSectionLaneRun { hook, proofs })
}

fn planned_cross_section_requests(
    args: &Args,
    cache_root: &Path,
    cycle_utc: u8,
) -> Vec<PressureCrossSectionRequest> {
    default_native_cross_section_requests(
        &args.date,
        cycle_utc,
        args.forecast_hour,
        args.source,
        &args.out_dir,
        Some(cache_root.to_path_buf()),
        !args.no_cache,
    )
}

fn build_cross_section_hook(
    proofs: impl IntoIterator<Item = CrossSectionProofSummary>,
) -> CrossSectionHookSummary {
    let proofs = proofs.into_iter().collect::<Vec<_>>();
    CrossSectionHookSummary {
        status: "complete",
        kind: "cross_section_proof_lane",
        rationale: "Native proof now pins a lightweight set of real-data HRRR cross sections so cross-section regressions stay visible without widening the suite.",
        proof_count: proofs.len(),
        proofs,
    }
}

fn print_cross_section_artifacts(proofs: &[CrossSectionRunOutput]) {
    for path in collect_cross_section_artifact_paths(proofs) {
        println!("{}", path.display());
    }
}

fn collect_cross_section_artifact_paths(proofs: &[CrossSectionRunOutput]) -> Vec<PathBuf> {
    proofs
        .iter()
        .flat_map(|proof| [proof.output_path.clone(), proof.summary_path.clone()])
        .collect()
}

fn selected_suite_case_definitions(args: &Args) -> Vec<&'static ProofCaseDefinition> {
    let selected = if args.case.is_empty() {
        vec![ProofCaseArg::ConusContour]
    } else {
        args.case.clone()
    };

    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    for arg in selected {
        if seen.insert(arg) {
            ordered.push(suite_case_definition(arg));
        }
    }
    ordered
}

fn suite_case_definition(arg: ProofCaseArg) -> &'static ProofCaseDefinition {
    SUITE_CASES
        .iter()
        .find(|case| case.arg == arg)
        .expect("suite case definition should exist")
}

fn failure_region_slug(args: &Args) -> &'static str {
    match args.mode {
        RunnerModeArg::Suite => "suite",
        RunnerModeArg::Custom => args.region.slug(),
    }
}

fn relative_output_path(root: &Path, output_path: &Path) -> PathBuf {
    output_path
        .strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| output_path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::{
        Args, CrossSectionProofSummary, CrossSectionRunOutput, ProofCaseArg, RunnerModeArg,
        SourceModeArg, build_cross_section_hook, collect_cross_section_artifact_paths,
        default_custom_derived_recipes, default_custom_direct_recipes, resolve_custom_recipe_slugs,
        selected_suite_case_definitions, validate_args,
    };
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn base_args() -> Args {
        Args {
            mode: RunnerModeArg::Suite,
            case: Vec::new(),
            date: "20260414".to_string(),
            cycle: Some(23),
            forecast_hour: 0,
            source: rustwx_core::SourceId::Nomads,
            region: super::region::RegionPreset::Midwest,
            direct_recipes: Vec::new(),
            derived_recipes: Vec::new(),
            suite_child_output: None,
            out_dir: PathBuf::from("proof"),
            cache_dir: None,
            no_cache: false,
            source_mode: SourceModeArg::Canonical,
        }
    }

    fn cross_section_proof(route_slug: &str, route_label: &str) -> CrossSectionProofSummary {
        CrossSectionProofSummary {
            model: "hrrr",
            route_slug: route_slug.to_string(),
            route_label: route_label.to_string(),
            product_slug: "temperature".to_string(),
            product_label: "Temperature".to_string(),
            palette_slug: "temperature_white_zero".to_string(),
            date_yyyymmdd: "20260414".to_string(),
            cycle_utc: 23,
            forecast_hour: 0,
            source: "nomads".to_string(),
            output_path: PathBuf::from(format!("{route_slug}.png")),
            summary_path: PathBuf::from(format!("{route_slug}.json")),
            route_distance_km: 1234.0,
            sample_count: 181,
            pressure_levels: 39,
            start_lat: 35.2220,
            start_lon: -101.8313,
            end_lat: 41.8781,
            end_lon: -87.6298,
        }
    }

    fn cross_section_run_output(route_slug: &str, route_label: &str) -> CrossSectionRunOutput {
        let summary = cross_section_proof(route_slug, route_label);
        CrossSectionRunOutput {
            output_path: summary.output_path.clone(),
            summary_path: summary.summary_path.clone(),
            summary,
        }
    }

    #[test]
    fn custom_recipe_lists_are_unique() {
        let direct = default_custom_direct_recipes();
        let derived = default_custom_derived_recipes();
        assert_eq!(
            direct.iter().collect::<HashSet<_>>().len(),
            direct.len(),
            "duplicate custom direct proof recipes"
        );
        assert_eq!(
            derived.iter().collect::<HashSet<_>>().len(),
            derived.len(),
            "duplicate custom derived proof recipes"
        );
    }

    #[test]
    fn cross_section_hook_tracks_multiple_real_proofs() {
        let hook = build_cross_section_hook(vec![
            cross_section_proof("amarillo_chicago", "Amarillo to Chicago"),
            cross_section_proof("kansas_city_chicago", "Kansas City to Chicago"),
        ]);
        assert_eq!(hook.status, "complete");
        assert_eq!(hook.kind, "cross_section_proof_lane");
        assert_eq!(hook.proof_count, 2);
        assert_eq!(
            hook.proofs
                .iter()
                .map(|proof| proof.route_slug.as_str())
                .collect::<Vec<_>>(),
            vec!["amarillo_chicago", "kansas_city_chicago"]
        );
        assert!(hook.rationale.contains("cross sections"));
    }

    #[test]
    fn cross_section_reporting_keeps_output_summary_pairs() {
        let proofs = vec![
            cross_section_run_output("amarillo_chicago", "Amarillo to Chicago"),
            cross_section_run_output("kansas_city_chicago", "Kansas City to Chicago"),
        ];
        assert_eq!(
            collect_cross_section_artifact_paths(&proofs),
            vec![
                PathBuf::from("amarillo_chicago.png"),
                PathBuf::from("amarillo_chicago.json"),
                PathBuf::from("kansas_city_chicago.png"),
                PathBuf::from("kansas_city_chicago.json"),
            ]
        );
    }

    #[test]
    fn suite_defaults_cover_required_cases() {
        let args = base_args();
        let cases = selected_suite_case_definitions(&args);
        let slugs = cases.iter().map(|case| case.slug).collect::<Vec<_>>();
        assert_eq!(slugs, vec!["conus_contour"]);
        assert!(cases.iter().all(|case| case.contour_candidate));
    }

    #[test]
    fn suite_case_selection_is_deduped_and_ordered() {
        let mut args = base_args();
        args.case = vec![
            ProofCaseArg::ConusContour,
            ProofCaseArg::MidwestCore,
            ProofCaseArg::ConusContour,
        ];
        let slugs = selected_suite_case_definitions(&args)
            .iter()
            .map(|case| case.slug)
            .collect::<Vec<_>>();
        assert_eq!(slugs, vec!["conus_contour", "midwest_core"]);
    }

    #[test]
    fn validate_args_rejects_recipe_overrides_in_suite_mode() {
        let mut args = base_args();
        args.direct_recipes.push("mslp_10m_winds".to_string());
        let err = validate_args(&args).unwrap_err().to_string();
        assert!(err.contains("suite mode"));
    }

    #[test]
    fn validate_args_rejects_case_selection_in_custom_mode() {
        let mut args = base_args();
        args.mode = RunnerModeArg::Custom;
        args.case.push(ProofCaseArg::ConusContour);
        let err = validate_args(&args).unwrap_err().to_string();
        assert!(err.contains("only valid in suite mode"));
    }

    #[test]
    fn standalone_custom_mode_uses_default_recipe_lists() {
        let mut args = base_args();
        args.mode = RunnerModeArg::Custom;
        let supported_direct = vec![
            "composite_reflectivity".to_string(),
            "500mb_temperature_height_winds".to_string(),
        ];
        let supported_derived = vec!["sbcape".to_string(), "stp_fixed".to_string()];
        let (direct, derived) =
            resolve_custom_recipe_slugs(&args, &supported_direct, &supported_derived);
        assert_eq!(direct, default_custom_direct_recipes());
        assert_eq!(derived, default_custom_derived_recipes());
    }

    #[test]
    fn suite_child_custom_mode_keeps_declared_recipe_lists_exact() {
        let mut args = base_args();
        args.mode = RunnerModeArg::Custom;
        args.suite_child_output = Some(PathBuf::from("child.json"));
        let supported_direct = vec![
            "composite_reflectivity".to_string(),
            "500mb_temperature_height_winds".to_string(),
        ];
        let supported_derived = vec!["sbcape".to_string(), "stp_fixed".to_string()];
        let (direct, derived) =
            resolve_custom_recipe_slugs(&args, &supported_direct, &supported_derived);
        assert!(direct.is_empty());
        assert!(derived.is_empty());
    }
}
