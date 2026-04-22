use rustwx_core::{GridProjection, ModelId, SourceId};
use rustwx_products::cache::ensure_dir;
use rustwx_products::derived::{
    NativeContourRenderMode, build_hrrr_live_derived_artifact_with_render_mode,
    native_contour_line_levels_for_recipe_slug,
};
use rustwx_products::direct::build_projected_map_with_projection;
use rustwx_products::gridded::load_model_timestep_from_parts;
use rustwx_products::shared_context::DomainSpec;
use rustwx_render::{
    DiscreteColorScale, PngCompressionMode, PngWriteOptions, RenderImageTiming, RenderSaveTiming,
    RenderStateTiming, save_png_profile_with_options,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

const DEFAULT_OUTPUT_WIDTH: u32 = 1200;
const DEFAULT_OUTPUT_HEIGHT: u32 = 900;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherNativeBenchmarkRequest {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub product_slugs: Vec<String>,
    #[serde(default = "default_native_fill_level_multiplier")]
    pub native_fill_level_multiplier: usize,
    pub rust_runs: usize,
    pub python_runs: usize,
    pub python_executable: String,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkStageTiming {
    pub data_load_ms: u128,
    pub projected_map_build_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RustRenderRunRecord {
    pub total_ms: u128,
    pub render_to_image_ms: u128,
    pub state_timing: RenderStateTiming,
    pub image_timing: RenderImageTiming,
    pub png_encode_ms: u128,
    pub file_write_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RustRenderProfileSummary {
    pub run_count: usize,
    pub runs: Vec<RustRenderRunRecord>,
    pub median_total_ms: u128,
    pub median_run: RustRenderRunRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonBenchmarkRunRecord {
    pub render_save_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonBenchmarkSummary {
    pub python_executable: String,
    pub setup_ms: f64,
    pub process_wall_ms: u128,
    pub run_count: usize,
    pub runs: Vec<PythonBenchmarkRunRecord>,
    pub median_render_save_ms: f64,
    pub output_png: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRatioSummary {
    pub native_speedup_over_legacy: f64,
    pub native_speedup_over_python: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherNativeBenchmarkCaseSummary {
    pub recipe_slug: String,
    pub title: String,
    pub units: String,
    pub native_request_build_ms: u128,
    pub legacy_request_build_ms: u128,
    pub native_output_png: PathBuf,
    pub legacy_output_png: PathBuf,
    pub python_output_png: PathBuf,
    pub native_profile: RustRenderProfileSummary,
    pub legacy_profile: RustRenderProfileSummary,
    pub python_profile: PythonBenchmarkSummary,
    pub ratios: BenchmarkRatioSummary,
    pub payload_path: PathBuf,
    pub python_summary_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherNativeBenchmarkSummary {
    pub runner: &'static str,
    pub model: ModelId,
    pub request: WeatherNativeBenchmarkRequest,
    pub stage_timing: BenchmarkStageTiming,
    pub cases: Vec<WeatherNativeBenchmarkCaseSummary>,
    pub summary_json: PathBuf,
    pub summary_markdown: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MatplotlibContourPayload {
    recipe_slug: String,
    title: String,
    units: String,
    width: u32,
    height: u32,
    bounds: (f64, f64, f64, f64),
    projection: Option<GridProjection>,
    nx: usize,
    ny: usize,
    lat_deg: Vec<f32>,
    lon_deg: Vec<f32>,
    values: Vec<f32>,
    scale: DiscreteColorScale,
    line_levels: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PythonSummaryFile {
    setup_ms: f64,
    render_save_ms_runs: Vec<f64>,
    median_render_save_ms: f64,
    output_png: PathBuf,
}

fn default_output_width() -> u32 {
    DEFAULT_OUTPUT_WIDTH
}

fn default_output_height() -> u32 {
    DEFAULT_OUTPUT_HEIGHT
}

fn default_png_compression() -> PngCompressionMode {
    PngCompressionMode::Default
}

fn default_native_fill_level_multiplier() -> usize {
    1
}

pub fn default_benchmark_products() -> Vec<String> {
    vec![
        "stp_fixed".to_string(),
        "sbcape".to_string(),
        "srh_0_1km".to_string(),
    ]
}

pub fn run_weather_native_benchmark(
    request: &WeatherNativeBenchmarkRequest,
) -> Result<WeatherNativeBenchmarkSummary, Box<dyn Error>> {
    ensure_dir(&request.out_dir)?;
    let benchmark_root = request.out_dir.join("bench");
    ensure_dir(&benchmark_root)?;

    let data_load_start = Instant::now();
    let loaded = load_model_timestep_from_parts(
        ModelId::Hrrr,
        &request.date_yyyymmdd,
        Some(request.cycle_utc),
        request.forecast_hour,
        request.source,
        None,
        None,
        &request.cache_root,
        request.use_cache,
    )?;
    let data_load_ms = data_load_start.elapsed().as_millis();

    let projected_start = Instant::now();
    let projected = build_projected_map_with_projection(
        &loaded.grid.lat_deg,
        &loaded.grid.lon_deg,
        loaded.surface_decode.value.projection.as_ref(),
        request.domain.bounds,
        rustwx_render::map_frame_aspect_ratio(
            request.output_width,
            request.output_height,
            true,
            true,
        ),
    )?;
    let projected_map_build_ms = projected_start.elapsed().as_millis();

    let mut cases = Vec::with_capacity(request.product_slugs.len());
    for recipe_slug in &request.product_slugs {
        cases.push(run_benchmark_case(
            request,
            &benchmark_root,
            recipe_slug,
            &loaded,
            &projected,
        )?);
    }

    let summary_json = benchmark_root.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_weather_native_benchmark_summary.json",
        request.date_yyyymmdd, request.cycle_utc, request.forecast_hour, request.domain.slug
    ));
    let summary_markdown = benchmark_root.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_weather_native_benchmark_summary.md",
        request.date_yyyymmdd, request.cycle_utc, request.forecast_hour, request.domain.slug
    ));

    let summary = WeatherNativeBenchmarkSummary {
        runner: "weather_native_benchmark",
        model: ModelId::Hrrr,
        request: request.clone(),
        stage_timing: BenchmarkStageTiming {
            data_load_ms,
            projected_map_build_ms,
        },
        cases,
        summary_json: summary_json.clone(),
        summary_markdown: summary_markdown.clone(),
    };
    fs::write(&summary_json, serde_json::to_vec_pretty(&summary)?)?;
    fs::write(&summary_markdown, render_summary_markdown(&summary))?;
    Ok(summary)
}

fn run_benchmark_case(
    request: &WeatherNativeBenchmarkRequest,
    benchmark_root: &Path,
    recipe_slug: &str,
    loaded: &rustwx_products::gridded::LoadedModelTimestep,
    projected: &rustwx_products::shared_context::ProjectedMap,
) -> Result<WeatherNativeBenchmarkCaseSummary, Box<dyn Error>> {
    let native_build_start = Instant::now();
    let native = build_hrrr_live_derived_artifact_with_render_mode(
        recipe_slug,
        &loaded.surface_decode.value,
        &loaded.pressure_decode.value,
        &loaded.grid,
        projected,
        request.domain.bounds,
        &request.date_yyyymmdd,
        request.cycle_utc,
        request.forecast_hour,
        request.source,
        NativeContourRenderMode::Automatic,
        request.native_fill_level_multiplier,
    )?;
    let native_request_build_ms = native_build_start.elapsed().as_millis();

    let legacy_build_start = Instant::now();
    let legacy = build_hrrr_live_derived_artifact_with_render_mode(
        recipe_slug,
        &loaded.surface_decode.value,
        &loaded.pressure_decode.value,
        &loaded.grid,
        projected,
        request.domain.bounds,
        &request.date_yyyymmdd,
        request.cycle_utc,
        request.forecast_hour,
        request.source,
        NativeContourRenderMode::LegacyRaster,
        1,
    )?;
    let legacy_request_build_ms = legacy_build_start.elapsed().as_millis();

    let line_levels = native_contour_line_levels_for_recipe_slug(recipe_slug)?.unwrap_or_default();
    let png_options = PngWriteOptions {
        compression: request.png_compression,
    };
    let native_output_png = benchmark_root.join(format!("{recipe_slug}_rust_native.png"));
    let legacy_output_png = benchmark_root.join(format!("{recipe_slug}_rust_legacy.png"));
    let python_output_png = benchmark_root.join(format!("{recipe_slug}_python_matplotlib.png"));
    let native_profile = profile_rust_request(
        &native.request,
        &native_output_png,
        request.rust_runs,
        &png_options,
    )?;
    let legacy_profile = profile_rust_request(
        &legacy.request,
        &legacy_output_png,
        request.rust_runs,
        &png_options,
    )?;

    let payload_path = benchmark_root.join(format!("{recipe_slug}_python_payload.json"));
    let python_summary_path = benchmark_root.join(format!("{recipe_slug}_python_summary.json"));
    let payload = MatplotlibContourPayload {
        recipe_slug: recipe_slug.to_string(),
        title: native.title.clone(),
        units: native.field.units.clone(),
        width: request.output_width,
        height: request.output_height,
        bounds: request.domain.bounds,
        projection: loaded.surface_decode.value.projection.clone(),
        nx: native.field.grid.shape.nx,
        ny: native.field.grid.shape.ny,
        lat_deg: native.field.grid.lat_deg.clone(),
        lon_deg: native.field.grid.lon_deg.clone(),
        values: native.field.values.clone(),
        scale: native.request.scale.resolved_discrete(),
        line_levels,
    };
    fs::write(&payload_path, serde_json::to_vec(&payload)?)?;
    let python_profile = run_python_benchmark(
        request,
        &payload_path,
        &python_output_png,
        &python_summary_path,
    )?;

    Ok(WeatherNativeBenchmarkCaseSummary {
        recipe_slug: recipe_slug.to_string(),
        title: native.title.clone(),
        units: native.field.units.clone(),
        native_request_build_ms,
        legacy_request_build_ms,
        native_output_png,
        legacy_output_png,
        python_output_png,
        native_profile: native_profile.clone(),
        legacy_profile: legacy_profile.clone(),
        python_profile: python_profile.clone(),
        ratios: BenchmarkRatioSummary {
            native_speedup_over_legacy: ratio(
                legacy_profile.median_total_ms as f64,
                native_profile.median_total_ms as f64,
            ),
            native_speedup_over_python: ratio(
                python_profile.median_render_save_ms,
                native_profile.median_total_ms as f64,
            ),
        },
        payload_path,
        python_summary_path,
    })
}

fn profile_rust_request(
    request: &rustwx_render::MapRenderRequest,
    output_png: &Path,
    run_count: usize,
    png_options: &PngWriteOptions,
) -> Result<RustRenderProfileSummary, Box<dyn Error>> {
    let run_count = run_count.max(1);
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        let timing = save_png_profile_with_options(request, output_png, png_options)?;
        runs.push(rust_run_record(&timing));
    }
    let median_run = median_rust_run(&runs);
    Ok(RustRenderProfileSummary {
        run_count,
        runs,
        median_total_ms: median_run.total_ms,
        median_run,
    })
}

fn rust_run_record(timing: &RenderSaveTiming) -> RustRenderRunRecord {
    RustRenderRunRecord {
        total_ms: timing.total_ms,
        render_to_image_ms: timing.png_timing.render_to_image_ms,
        state_timing: timing.state_timing.clone(),
        image_timing: timing.png_timing.image_timing.clone(),
        png_encode_ms: timing.png_timing.png_encode_ms,
        file_write_ms: timing.file_write_ms,
    }
}

fn median_rust_run(runs: &[RustRenderRunRecord]) -> RustRenderRunRecord {
    let mut sorted = runs.to_vec();
    sorted.sort_by_key(|run| run.total_ms);
    sorted[sorted.len() / 2].clone()
}

fn run_python_benchmark(
    request: &WeatherNativeBenchmarkRequest,
    payload_path: &Path,
    output_png: &Path,
    summary_path: &Path,
) -> Result<PythonBenchmarkSummary, Box<dyn Error>> {
    let script_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("matplotlib_weather_native_bench.py");
    let process_start = Instant::now();
    let output = Command::new(&request.python_executable)
        .arg(&script_path)
        .arg("--payload")
        .arg(payload_path)
        .arg("--output")
        .arg(output_png)
        .arg("--summary")
        .arg(summary_path)
        .arg("--runs")
        .arg(request.python_runs.max(1).to_string())
        .output()?;
    let process_wall_ms = process_start.elapsed().as_millis();
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "python benchmark failed for {}: {}\n{}",
            payload_path.display(),
            stderr.trim(),
            stdout.trim()
        )
        .into());
    }

    let summary_file: PythonSummaryFile = serde_json::from_slice(&fs::read(summary_path)?)?;
    let runs = summary_file
        .render_save_ms_runs
        .into_iter()
        .map(|render_save_ms| PythonBenchmarkRunRecord { render_save_ms })
        .collect::<Vec<_>>();
    Ok(PythonBenchmarkSummary {
        python_executable: request.python_executable.clone(),
        setup_ms: summary_file.setup_ms,
        process_wall_ms,
        run_count: runs.len(),
        runs,
        median_render_save_ms: summary_file.median_render_save_ms,
        output_png: summary_file.output_png,
    })
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if numerator.is_finite() && denominator.is_finite() && denominator > 0.0 {
        numerator / denominator
    } else {
        f64::NAN
    }
}

fn render_summary_markdown(summary: &WeatherNativeBenchmarkSummary) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Weather-native benchmark summary\n\n");
    markdown.push_str(&format!(
        "- model: `{}`\n- date/cycle: `{}` `{}`Z f{:03}\n- domain: `{}`\n- native fill level multiplier: `{}`\n- stage timing: load={} ms, projected_map={} ms\n\n",
        summary.model,
        summary.request.date_yyyymmdd,
        summary.request.cycle_utc,
        summary.request.forecast_hour,
        summary.request.domain.slug,
        summary.request.native_fill_level_multiplier,
        summary.stage_timing.data_load_ms,
        summary.stage_timing.projected_map_build_ms,
    ));
    markdown.push_str("| Product | Rust native median (ms) | Rust legacy median (ms) | Python median (ms) | Native speedup vs legacy | Native speedup vs Python |\n");
    markdown.push_str("|---|---:|---:|---:|---:|---:|\n");
    for case in &summary.cases {
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {:.1} | {:.2}x | {:.2}x |\n",
            case.recipe_slug,
            case.native_profile.median_total_ms,
            case.legacy_profile.median_total_ms,
            case.python_profile.median_render_save_ms,
            case.ratios.native_speedup_over_legacy,
            case.ratios.native_speedup_over_python,
        ));
    }
    markdown.push_str("\n## Artifacts\n\n");
    for case in &summary.cases {
        markdown.push_str(&format!(
            "- `{}` native: `{}`\n- `{}` legacy: `{}`\n- `{}` python: `{}`\n",
            case.recipe_slug,
            case.native_output_png.display(),
            case.recipe_slug,
            case.legacy_output_png.display(),
            case.recipe_slug,
            case.python_output_png.display(),
        ));
    }
    markdown
}
