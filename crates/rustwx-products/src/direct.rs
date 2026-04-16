use grib_core::grib2::Grib2File;
use image::DynamicImage;
use rustwx_core::{
    CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, SelectedField2D, SourceId,
};
use rustwx_io::{
    CachedFetchResult, FetchRequest, extract_fields_from_grib2, fetch_bytes,
    fetch_bytes_with_cache, load_cached_selected_field, store_cached_selected_field,
};
use rustwx_models::{
    LatestRun, ModelError, PlotRecipe, PlotRecipeFetchMode, PlotRecipeFetchPlan, RenderStyle,
    latest_available_run, plot_recipe, plot_recipe_fetch_plan,
};
use rustwx_render::{
    Color, ColorScale, ContourLayer, DiscreteColorScale, ExtendMode, MapRenderRequest,
    PanelGridLayout, PanelPadding, ProjectedDomain, ProjectedExtent, ProjectedLineOverlay,
    WindBarbLayer, render_panel_grid, save_png,
    solar07::{Solar07Palette, solar07_palette},
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use wrf_render::features::load_styled_conus_features;
use wrf_render::overlay::MapExtent;
use wrf_render::projection::LambertConformal;
use wrf_render::render::map_frame_aspect_ratio;
use wrf_render::text;

use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
    fetch_identity_from_cached_result,
};
use crate::shared_context::{DomainSpec, ProjectedMap, ProjectedMapProvider};
use crate::spec::direct_product_specs;

const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;
const CLOUD_LEVEL_COMPONENT_SLUGS: &[&str] =
    &["low_cloud_cover", "middle_cloud_cover", "high_cloud_cover"];
const PRECIPITATION_TYPE_COMPONENT_SLUGS: &[&str] = &[
    "categorical_rain",
    "categorical_freezing_rain",
    "categorical_ice_pellets",
    "categorical_snow",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectBatchRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub recipe_slugs: Vec<String>,
    pub product_overrides: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDirectBatchRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub recipe_slugs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectFetchRuntimeInfo {
    pub fetch_key: String,
    pub planned_product: String,
    pub fetched_product: String,
    pub requested_source: SourceId,
    pub resolved_source: SourceId,
    pub resolved_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRecipeTiming {
    pub project_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectFetchTiming {
    pub product: String,
    pub fetch_mode: PlotRecipeFetchMode,
    pub fetch_ms: u128,
    pub parse_ms: u128,
    pub extract_ms: u128,
    pub total_ms: u128,
    pub fetch_cache_hit: bool,
    pub extract_cache_hits: usize,
    pub extract_cache_misses: usize,
    pub runtime_fetch: DirectFetchRuntimeInfo,
    pub input_fetch: PublishedFetchIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRenderedRecipe {
    pub recipe_slug: String,
    pub title: String,
    pub grib_product: String,
    pub fetched_grib_product: String,
    pub resolved_source: SourceId,
    pub resolved_url: String,
    pub output_path: PathBuf,
    pub content_identity: ArtifactContentIdentity,
    pub input_fetch_keys: Vec<String>,
    pub timing: DirectRecipeTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectBatchReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub fetches: Vec<DirectFetchTiming>,
    pub recipes: Vec<DirectRenderedRecipe>,
    pub total_ms: u128,
}

pub type HrrrDirectFetchRuntimeInfo = DirectFetchRuntimeInfo;
pub type HrrrDirectRecipeTiming = DirectRecipeTiming;
pub type HrrrDirectFetchTiming = DirectFetchTiming;
pub type HrrrDirectRenderedRecipe = DirectRenderedRecipe;
pub type HrrrDirectBatchReport = DirectBatchReport;

#[derive(Debug, Clone)]
struct PlannedDirectRecipe {
    recipe: &'static PlotRecipe,
    plan: PlotRecipeFetchPlan,
}

#[derive(Debug, Clone)]
struct FetchGroup {
    product: String,
    fetch_mode: PlotRecipeFetchMode,
    // Retained for recipe-level coverage/debugging; the direct/native batch
    // path intentionally pulls full family GRIB bytes and extracts grouped
    // selectors from the parsed full file.
    variable_patterns: Vec<String>,
    selectors: Vec<FieldSelector>,
}

#[derive(Debug, Clone, Copy)]
struct CompositePanelSpec {
    rows: u32,
    columns: u32,
    panel_width: u32,
    panel_height: u32,
    top_padding: u32,
    component_slugs: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BarbStrideCacheKey {
    u_selector: FieldSelector,
    v_selector: FieldSelector,
    bounds_bits: [u64; 4],
}

type SharedBarbStrideCache = Arc<Mutex<HashMap<BarbStrideCacheKey, (usize, usize)>>>;

impl DirectBatchRequest {
    fn from_hrrr(request: &HrrrDirectBatchRequest) -> Self {
        Self {
            model: ModelId::Hrrr,
            date_yyyymmdd: request.date_yyyymmdd.clone(),
            cycle_override_utc: request.cycle_override_utc,
            forecast_hour: request.forecast_hour,
            source: request.source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: request.recipe_slugs.clone(),
            product_overrides: HashMap::new(),
        }
    }
}

fn resolve_direct_run(
    model: ModelId,
    date: &str,
    cycle_override: Option<u8>,
    source: SourceId,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model,
            cycle: CycleSpec::new(date, hour)?,
            source,
        }),
        None => Ok(latest_available_run(model, Some(source), date)?),
    }
}

pub fn run_direct_batch(
    request: &DirectBatchRequest,
) -> Result<DirectBatchReport, Box<dyn std::error::Error>> {
    let latest = resolve_direct_run(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.source,
    )?;
    run_direct_batch_with_context(request, &latest, None)
}

pub fn run_hrrr_direct_batch(
    request: &HrrrDirectBatchRequest,
) -> Result<HrrrDirectBatchReport, Box<dyn std::error::Error>> {
    run_direct_batch(&DirectBatchRequest::from_hrrr(request))
}

pub(crate) fn run_hrrr_direct_batch_with_context(
    request: &HrrrDirectBatchRequest,
    latest: &LatestRun,
    shared_context: Option<&crate::hrrr::PreparedHrrrHourContext>,
) -> Result<HrrrDirectBatchReport, Box<dyn std::error::Error>> {
    let generic = DirectBatchRequest::from_hrrr(request);
    run_direct_batch_with_context(
        &generic,
        latest,
        shared_context.map(|ctx| ctx as &dyn ProjectedMapProvider),
    )
}

fn run_direct_batch_with_context(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    shared_context: Option<&dyn ProjectedMapProvider>,
) -> Result<DirectBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let total_start = Instant::now();
    let planned = plan_direct_recipes(request.model, &request.recipe_slugs)?;
    let groups = group_direct_fetches(request, &planned);
    let mut extracted = HashMap::<FieldSelector, SelectedField2D>::new();
    let mut fetches = Vec::with_capacity(groups.len());
    let mut fetch_truth_by_actual_product = HashMap::<String, DirectFetchRuntimeInfo>::new();

    for group in &groups {
        let (fields, timing) = load_direct_fetch_group(
            request,
            latest,
            request.forecast_hour,
            group,
            &request.cache_root,
            request.use_cache,
        )?;
        extracted.extend(fields.into_iter().map(|field| (field.selector, field)));
        fetch_truth_by_actual_product.insert(group.product.clone(), timing.runtime_fetch.clone());
        fetches.push(timing);
    }

    let rendered = render_direct_recipes(
        request,
        latest,
        &planned,
        &extracted,
        &fetch_truth_by_actual_product,
        shared_context,
    )?;

    Ok(DirectBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: latest.source,
        domain: request.domain.clone(),
        fetches,
        recipes: rendered,
        total_ms: total_start.elapsed().as_millis(),
    })
}

pub(crate) fn required_direct_projection_sizes(recipe_slugs: &[String]) -> Vec<(u32, u32)> {
    let mut sizes = vec![(OUTPUT_WIDTH, OUTPUT_HEIGHT)];
    let mut seen = HashSet::<(u32, u32)>::from_iter(sizes.iter().copied());
    for slug in recipe_slugs {
        let normalized = plot_recipe(slug)
            .map(|recipe| recipe.slug)
            .unwrap_or(slug.as_str());
        if let Some(spec) = composite_panel_spec(normalized) {
            let size = (spec.panel_width, spec.panel_height);
            if seen.insert(size) {
                sizes.push(size);
            }
        }
    }
    sizes
}

pub fn supported_direct_recipe_slugs(model: ModelId) -> Vec<String> {
    direct_product_specs()
        .into_iter()
        .filter(|spec| plot_recipe_fetch_plan(&spec.slug, model).is_ok())
        .map(|spec| spec.slug)
        .collect()
}

fn plan_direct_recipes(
    model: ModelId,
    recipe_slugs: &[String],
) -> Result<Vec<PlannedDirectRecipe>, Box<dyn std::error::Error>> {
    let mut planned = Vec::new();
    let mut seen = HashSet::<String>::new();
    for slug in recipe_slugs {
        let recipe = plot_recipe(slug).ok_or_else(|| format!("unknown recipe '{slug}'"))?;
        if !seen.insert(recipe.slug.to_string()) {
            continue;
        }
        let plan = match plot_recipe_fetch_plan(recipe.slug, model) {
            Ok(plan) => plan,
            Err(ModelError::UnsupportedPlotRecipeModel { reason, .. }) => {
                return Err(format!(
                    "plot recipe '{}' is not supported for {}: {}",
                    recipe.slug, model, reason
                )
                .into());
            }
            Err(err) => return Err(err.into()),
        };
        planned.push(PlannedDirectRecipe { recipe, plan });
    }
    Ok(planned)
}

fn group_direct_fetches(request: &DirectBatchRequest, recipes: &[PlannedDirectRecipe]) -> Vec<FetchGroup> {
    let mut grouped = HashMap::<String, FetchGroup>::new();
    for item in recipes {
        let key = canonical_fetch_product(request, item.plan.product.as_ref());
        let entry = grouped.entry(key.clone()).or_insert_with(|| FetchGroup {
            product: key,
            fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
            variable_patterns: Vec::new(),
            selectors: Vec::new(),
        });
        for pattern in item.plan.variable_patterns() {
            if !entry.variable_patterns.iter().any(|value| value == pattern) {
                entry.variable_patterns.push(pattern.to_string());
            }
        }
        for selector in item.plan.selectors() {
            if !entry.selectors.contains(&selector) {
                entry.selectors.push(selector);
            }
        }
    }
    let mut groups = grouped.into_values().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.product.cmp(&right.product));
    groups
}

fn canonical_fetch_product(request: &DirectBatchRequest, planned_product: &str) -> String {
    if let Some(overridden) = request.product_overrides.get(planned_product) {
        return overridden.clone();
    }

    match (request.model, planned_product) {
        (ModelId::Hrrr, "nat") => "sfc".to_string(),
        _ => planned_product.to_string(),
    }
}

fn build_direct_fetch_request(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    forecast_hour: u16,
    group: &FetchGroup,
) -> Result<FetchRequest, rustwx_core::RustwxError> {
    Ok(FetchRequest {
        request: ModelRunRequest::new(
            request.model,
            latest.cycle.clone(),
            forecast_hour,
            group.product.as_str(),
        )?,
        source_override: Some(latest.source),
        // Force full-family GRIB fetches for the direct/native lane. Grouped
        // extraction remains efficient because we still union selectors per
        // family and extract them from one parsed full GRIB.
        variable_patterns: Vec::new(),
    })
}

fn load_direct_fetch_group(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    forecast_hour: u16,
    group: &FetchGroup,
    cache_root: &std::path::Path,
    use_cache: bool,
) -> Result<(Vec<SelectedField2D>, DirectFetchTiming), Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let fetch_request = build_direct_fetch_request(request, latest, forecast_hour, group)?;

    let fetch_start = Instant::now();
    let fetched = if use_cache {
        fetch_bytes_with_cache(&fetch_request, cache_root, true)?
    } else {
        CachedFetchResult {
            result: fetch_bytes(&fetch_request)?,
            cache_hit: false,
            bytes_path: rustwx_io::fetch_cache_paths(cache_root, &fetch_request).0,
            metadata_path: rustwx_io::fetch_cache_paths(cache_root, &fetch_request).1,
        }
    };
    let fetch_ms = fetch_start.elapsed().as_millis();

    let extract_start = Instant::now();
    let mut extracted = Vec::<SelectedField2D>::new();
    let mut missing = Vec::<FieldSelector>::new();
    let mut extract_cache_hits = 0usize;
    if use_cache {
        for selector in &group.selectors {
            if let Some(cached) = load_cached_selected_field(cache_root, &fetch_request, *selector)?
            {
                extracted.push(cached.field);
                extract_cache_hits += 1;
            } else {
                missing.push(*selector);
            }
        }
    } else {
        missing.extend(group.selectors.iter().copied());
    }

    let parse_start = Instant::now();
    let grib = if missing.is_empty() {
        None
    } else {
        Some(Grib2File::from_bytes(&fetched.result.bytes)?)
    };
    let parse_ms = parse_start.elapsed().as_millis();

    if let Some(grib) = grib.as_ref() {
        let decoded = extract_fields_from_grib2(grib, &missing)?;
        if use_cache {
            for field in &decoded {
                store_cached_selected_field(cache_root, &fetch_request, field)?;
            }
        }
        extracted.extend(decoded);
    }
    let extract_ms = extract_start.elapsed().as_millis();

    Ok((
        extracted,
        DirectFetchTiming {
            product: group.product.clone(),
            fetch_mode: group.fetch_mode,
            fetch_ms,
            parse_ms,
            extract_ms,
            total_ms: total_start.elapsed().as_millis(),
            fetch_cache_hit: fetched.cache_hit,
            extract_cache_hits,
            extract_cache_misses: missing.len(),
            runtime_fetch: DirectFetchRuntimeInfo {
                fetch_key: crate::publication::fetch_key(
                    group.product.as_str(),
                    &fetch_request.request,
                ),
                planned_product: group.product.clone(),
                fetched_product: fetch_request.request.product.clone(),
                requested_source: fetch_request
                    .source_override
                    .unwrap_or(fetched.result.source),
                resolved_source: fetched.result.source,
                resolved_url: fetched.result.url.clone(),
            },
            input_fetch: fetch_identity_from_cached_result(
                group.product.as_str(),
                &fetch_request,
                &fetched,
            ),
        },
    ))
}

fn render_direct_recipes(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    planned: &[PlannedDirectRecipe],
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    fetch_truth_by_actual_product: &HashMap<String, DirectFetchRuntimeInfo>,
    shared_context: Option<&dyn ProjectedMapProvider>,
) -> Result<Vec<DirectRenderedRecipe>, Box<dyn std::error::Error>> {
    let barb_stride_cache = Arc::new(Mutex::new(HashMap::new()));
    let worker_count = render_worker_count(planned.len());
    if worker_count <= 1 {
        return planned
            .iter()
            .map(|item| {
                render_direct_recipe(
                    request,
                    latest,
                    item,
                    extracted,
                    fetch_truth_by_actual_product,
                    shared_context,
                    &barb_stride_cache,
                )
            })
            .collect();
    }

    let chunk_size = (planned.len() + worker_count - 1) / worker_count;
    let mut rendered = vec![None; planned.len()];

    thread::scope(|scope| -> Result<(), std::io::Error> {
        let mut handles = Vec::new();
        for (chunk_index, chunk) in planned.chunks(chunk_size).enumerate() {
            let barb_stride_cache = Arc::clone(&barb_stride_cache);
            let start_index = chunk_index * chunk_size;
            handles.push(scope.spawn(
                move || -> Result<Vec<(usize, DirectRenderedRecipe)>, std::io::Error> {
                    let mut chunk_rendered = Vec::with_capacity(chunk.len());
                    for (offset, item) in chunk.iter().enumerate() {
                        let rendered = render_direct_recipe(
                            request,
                            latest,
                            item,
                            extracted,
                            fetch_truth_by_actual_product,
                            shared_context,
                            &barb_stride_cache,
                        )
                        .map_err(|err| {
                            std::io::Error::other(format!(
                                "failed rendering recipe '{}': {err}",
                                item.recipe.slug
                            ))
                        })?;
                        chunk_rendered.push((start_index + offset, rendered));
                    }
                    Ok(chunk_rendered)
                },
            ));
        }

        for handle in handles {
            let chunk_rendered = handle
                .join()
                .map_err(|_| std::io::Error::other("parallel direct render worker panicked"))??;
            for (index, recipe) in chunk_rendered {
                rendered[index] = Some(recipe);
            }
        }
        Ok(())
    })?;

    let mut completed = Vec::with_capacity(planned.len());
    for recipe in rendered {
        completed.push(recipe.ok_or_else(|| {
            std::io::Error::other("parallel direct render worker dropped a recipe result")
        })?);
    }
    Ok(completed)
}

fn render_worker_count(recipe_count: usize) -> usize {
    if recipe_count <= 1 {
        return 1;
    }

    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .min(recipe_count)
}

fn composite_panel_spec(slug: &str) -> Option<CompositePanelSpec> {
    match slug {
        "cloud_cover_levels" => Some(CompositePanelSpec {
            rows: 1,
            columns: 3,
            panel_width: 420,
            panel_height: 320,
            top_padding: 64,
            component_slugs: CLOUD_LEVEL_COMPONENT_SLUGS,
        }),
        "precipitation_type" => Some(CompositePanelSpec {
            rows: 2,
            columns: 2,
            panel_width: 600,
            panel_height: 415,
            top_padding: 70,
            component_slugs: PRECIPITATION_TYPE_COMPONENT_SLUGS,
        }),
        _ => None,
    }
}

fn render_direct_recipe(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    item: &PlannedDirectRecipe,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    fetch_truth_by_actual_product: &HashMap<String, DirectFetchRuntimeInfo>,
    shared_context: Option<&dyn ProjectedMapProvider>,
    barb_stride_cache: &SharedBarbStrideCache,
) -> Result<DirectRenderedRecipe, Box<dyn std::error::Error>> {
    let render_start = Instant::now();
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}.png",
        request.model.as_str().replace('-', "_"),
        request.date_yyyymmdd,
        latest.cycle.hour_utc,
        request.forecast_hour,
        request.domain.slug,
        item.recipe.slug
    ));
    let canonical_product = canonical_fetch_product(request, item.plan.product.as_ref());
    let runtime_fetch = fetch_truth_by_actual_product
        .get::<str>(canonical_product.as_str())
        .ok_or_else(|| {
            format!(
                "missing direct fetch runtime truth for canonical family '{}'",
                canonical_product
            )
        })?;
    let project_ms = if let Some(spec) = composite_panel_spec(item.recipe.slug) {
        render_direct_composite_panel(
            item.recipe,
            spec,
            request,
            latest,
            extracted,
            &output_path,
            shared_context,
            barb_stride_cache,
        )?
    } else {
        let filled_selector = item
            .recipe
            .filled
            .selector
            .ok_or("recipe filled field missing selector binding")?;
        let filled = extracted
            .get(&filled_selector)
            .ok_or_else(|| format!("missing filled selector {:?}", filled_selector))?;

        let project_start = Instant::now();
        let projected = if let Some(projected) =
            shared_context.and_then(|ctx| ctx.projected_map(OUTPUT_WIDTH, OUTPUT_HEIGHT).cloned())
        {
            projected
        } else {
            build_projected_map(
                &filled.grid.lat_deg,
                &filled.grid.lon_deg,
                request.domain.bounds,
                map_frame_aspect_ratio(OUTPUT_WIDTH, OUTPUT_HEIGHT, true, true),
            )?
        };
        let project_ms = project_start.elapsed().as_millis();

        let mut render_request = build_render_request(
            item.recipe,
            filled,
            extracted,
            projected,
            request.domain.bounds,
            barb_stride_cache,
        )?;
        render_request.subtitle_left = Some(format!(
            "{} {}Z F{:03}  {}",
            request.date_yyyymmdd, latest.cycle.hour_utc, request.forecast_hour, request.model
        ));
        render_request.subtitle_right = Some(format!("source: {}", latest.source));
        save_png(&render_request, &output_path)?;
        project_ms
    };
    let content_identity = artifact_identity_from_path(&output_path)?;
    let total_ms = render_start.elapsed().as_millis();

    Ok(DirectRenderedRecipe {
        recipe_slug: item.recipe.slug.to_string(),
        title: item.recipe.title.to_string(),
        grib_product: item.plan.product.to_string(),
        fetched_grib_product: runtime_fetch.fetched_product.clone(),
        resolved_source: runtime_fetch.resolved_source,
        resolved_url: runtime_fetch.resolved_url.clone(),
        output_path,
        content_identity,
        input_fetch_keys: vec![runtime_fetch.fetch_key.clone()],
        timing: DirectRecipeTiming {
            project_ms,
            render_ms: total_ms.saturating_sub(project_ms),
            total_ms,
        },
    })
}

fn render_direct_composite_panel(
    recipe: &PlotRecipe,
    spec: CompositePanelSpec,
    request: &DirectBatchRequest,
    latest: &LatestRun,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    output_path: &std::path::Path,
    shared_context: Option<&dyn ProjectedMapProvider>,
    barb_stride_cache: &SharedBarbStrideCache,
) -> Result<u128, Box<dyn std::error::Error>> {
    let first_component = plot_recipe(spec.component_slugs[0])
        .ok_or_else(|| format!("missing component recipe '{}'", spec.component_slugs[0]))?;
    let first_selector = first_component
        .filled
        .selector
        .ok_or("component recipe filled field missing selector binding")?;
    let first_field = extracted
        .get(&first_selector)
        .ok_or_else(|| format!("missing component selector {:?}", first_selector))?;

    let project_start = Instant::now();
    let projected = if let Some(projected) = shared_context.and_then(|ctx| {
        ctx.projected_map(spec.panel_width, spec.panel_height)
            .cloned()
    }) {
        projected
    } else {
        build_projected_map(
            &first_field.grid.lat_deg,
            &first_field.grid.lon_deg,
            request.domain.bounds,
            map_frame_aspect_ratio(spec.panel_width, spec.panel_height, true, true),
        )?
    };
    let project_ms = project_start.elapsed().as_millis();

    let mut panel_requests = Vec::with_capacity(spec.component_slugs.len());
    for component_slug in spec.component_slugs {
        let component_recipe = plot_recipe(component_slug)
            .ok_or_else(|| format!("missing component recipe '{component_slug}'"))?;
        let selector = component_recipe
            .filled
            .selector
            .ok_or("component recipe filled field missing selector binding")?;
        let filled = extracted
            .get(&selector)
            .ok_or_else(|| format!("missing component selector {:?}", selector))?;
        let mut panel_request = build_render_request(
            component_recipe,
            filled,
            extracted,
            projected.clone(),
            request.domain.bounds,
            barb_stride_cache,
        )?;
        panel_request.width = spec.panel_width;
        panel_request.height = spec.panel_height;
        panel_request.subtitle_left = None;
        panel_request.subtitle_right = None;
        panel_requests.push(panel_request);
    }

    let layout =
        PanelGridLayout::new(spec.rows, spec.columns, spec.panel_width, spec.panel_height)?
            .with_padding(PanelPadding {
                top: spec.top_padding,
                ..Default::default()
            });
    let mut canvas = render_panel_grid(&layout, &panel_requests)?;
    text::draw_text_centered(&mut canvas, recipe.title, 10, wrf_render::Rgba::BLACK, 2);
    text::draw_text_centered(
        &mut canvas,
        &format!(
            "{} {}Z F{:03}  {} | source: {}",
            request.date_yyyymmdd,
            latest.cycle.hour_utc,
            request.forecast_hour,
            request.model,
            latest.source
        ),
        35,
        wrf_render::Rgba::BLACK,
        1,
    );
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    DynamicImage::ImageRgba8(canvas).save(output_path)?;
    Ok(project_ms)
}

fn build_render_request(
    recipe: &PlotRecipe,
    filled: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    projected: ProjectedMap,
    bounds: (f64, f64, f64, f64),
    barb_stride_cache: &SharedBarbStrideCache,
) -> Result<MapRenderRequest, Box<dyn std::error::Error>> {
    let filled_field = convert_filled_field(recipe, filled);
    let overlay_only = should_render_overlay_only(filled.selector, recipe.contours.is_some());
    let mut request = if overlay_only {
        let mut request = MapRenderRequest::contour_only(filled_field.clone().into());
        if let Some(layer) = contour_layer_for_values(filled.selector, &filled.values) {
            request.contours.push(layer);
        }
        request
    } else {
        MapRenderRequest::new(
            filled_field.clone().into(),
            scale_for_recipe(recipe, filled.selector),
        )
    };
    request.title = Some(recipe.title.to_string());
    request.width = OUTPUT_WIDTH;
    request.height = OUTPUT_HEIGHT;
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x,
        y: projected.projected_y,
        extent: projected.extent,
    });
    request.projected_lines = projected.lines;
    if overlay_only {
        request
            .contours
            .extend(build_contour_layers(recipe, extracted));
    } else {
        request.contours = build_contour_layers(recipe, extracted);
    }
    request.wind_barbs = build_barb_layers(recipe, extracted, bounds, barb_stride_cache);
    Ok(request)
}

fn convert_filled_field(recipe: &PlotRecipe, field: &SelectedField2D) -> rustwx_core::Field2D {
    let mut core = field.clone().into_field2d();
    if matches!(
        recipe.style,
        RenderStyle::Solar07Temperature | RenderStyle::Solar07Dewpoint
    ) {
        for value in &mut core.values {
            *value -= 273.15;
        }
        core.units = "degC".to_string();
    } else if field.selector.field == CanonicalField::PressureReducedToMeanSeaLevel {
        for value in &mut core.values {
            *value *= 0.01;
        }
        core.units = "hPa".to_string();
    } else if field.selector.field == CanonicalField::PrecipitableWater {
        for value in &mut core.values {
            *value /= 25.4;
        }
        core.units = "in".to_string();
    } else if field.selector.field == CanonicalField::Visibility {
        for value in &mut core.values {
            *value *= 0.000_621_371_2;
        }
        core.units = "mi".to_string();
    } else if field.selector.field == CanonicalField::AbsoluteVorticity {
        for value in &mut core.values {
            *value *= 100_000.0;
        }
        core.units = "10^-5 s^-1".to_string();
    } else if field.selector.field == CanonicalField::WindGust {
        for value in &mut core.values {
            *value *= 1.943_844_5;
        }
        core.units = "kt".to_string();
    } else if field.selector.field == CanonicalField::TotalPrecipitation {
        core.units = "mm".to_string();
    }
    core
}

fn should_render_overlay_only(selector: FieldSelector, has_explicit_contours: bool) -> bool {
    if has_explicit_contours {
        return false;
    }
    matches!(
        selector.field,
        CanonicalField::GeopotentialHeight | CanonicalField::PressureReducedToMeanSeaLevel
    )
}

fn scale_for_recipe(recipe: &PlotRecipe, filled_selector: FieldSelector) -> ColorScale {
    let discrete = match recipe.style {
        RenderStyle::Solar07Temperature => {
            let (lo, hi) = match filled_selector.vertical {
                rustwx_core::VerticalSelector::IsobaricHpa(500) => (-50.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(850) => (-40.0, 40.0),
                _ => (-60.0, 40.0),
            };
            DiscreteColorScale {
                levels: range_step(lo, hi, 1.0),
                colors: solar07_palette(Solar07Palette::Temperature),
                extend: ExtendMode::Both,
                mask_below: None,
            }
        }
        RenderStyle::Solar07Reflectivity | RenderStyle::Solar07RadarReflectivity => {
            DiscreteColorScale {
                levels: range_step(5.0, 80.0, 5.0),
                colors: solar07_palette(Solar07Palette::Reflectivity),
                extend: ExtendMode::Both,
                mask_below: None,
            }
        }
        RenderStyle::Solar07Rh => DiscreteColorScale {
            levels: range_step(0.0, 105.0, 5.0),
            colors: solar07_palette(Solar07Palette::Rh),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07Vorticity => DiscreteColorScale {
            levels: range_step(0.0, 48.0, 2.0),
            colors: solar07_palette(Solar07Palette::RelVort),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07Dewpoint => DiscreteColorScale {
            levels: range_step(-40.0, 30.0, 2.0),
            colors: solar07_palette(Solar07Palette::Dewpoint),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07Pressure => DiscreteColorScale {
            levels: range_step(960.0, 1045.0, 2.0),
            colors: solar07_palette(Solar07Palette::Winds),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07WindGust | RenderStyle::Solar07Winds => DiscreteColorScale {
            levels: range_step(0.0, 85.0, 5.0),
            colors: solar07_palette(Solar07Palette::Winds),
            extend: ExtendMode::Max,
            mask_below: None,
        },
        RenderStyle::Solar07CloudCover => DiscreteColorScale {
            levels: range_step(0.0, 110.0, 10.0),
            colors: solar07_palette(Solar07Palette::Rh),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07PrecipitableWater => DiscreteColorScale {
            levels: range_step(0.0, 2.6, 0.1),
            colors: solar07_palette(Solar07Palette::Precip),
            extend: ExtendMode::Max,
            mask_below: None,
        },
        RenderStyle::Solar07Qpf => DiscreteColorScale {
            levels: range_step(0.0, 100.0, 5.0),
            colors: solar07_palette(Solar07Palette::Precip),
            extend: ExtendMode::Max,
            mask_below: None,
        },
        RenderStyle::Solar07Categorical => DiscreteColorScale {
            levels: vec![0.0, 0.5, 1.0],
            colors: vec![
                Color::rgba(242, 242, 242, 255),
                Color::rgba(216, 34, 34, 255),
            ],
            extend: ExtendMode::Neither,
            mask_below: None,
        },
        RenderStyle::Solar07Visibility => DiscreteColorScale {
            levels: range_step(0.0, 10.5, 0.5),
            colors: solar07_palette(Solar07Palette::MlMetric),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07Satellite => DiscreteColorScale {
            levels: range_step(170.0, 321.0, 2.0),
            colors: solar07_palette(Solar07Palette::SimIr),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        RenderStyle::Solar07Lightning => DiscreteColorScale {
            levels: range_step(0.0, 20.5, 0.5),
            colors: solar07_palette(Solar07Palette::Uh),
            extend: ExtendMode::Max,
            mask_below: None,
        },
        _ => DiscreteColorScale {
            levels: range_step(-50.0, 5.0, 1.0),
            colors: solar07_palette(Solar07Palette::Temperature),
            extend: ExtendMode::Both,
            mask_below: None,
        },
    };
    ColorScale::Discrete(discrete)
}

fn build_contour_layers(
    recipe: &PlotRecipe,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> Vec<ContourLayer> {
    let Some(spec) = &recipe.contours else {
        return Vec::new();
    };
    let Some(selector) = spec.selector else {
        return Vec::new();
    };
    let Some(field) = extracted.get(&selector) else {
        return Vec::new();
    };

    contour_layer_for_values(selector, &field.values)
        .into_iter()
        .collect()
}

fn contour_layer_for_values(selector: FieldSelector, values: &[f32]) -> Option<ContourLayer> {
    let data = if selector.field == CanonicalField::GeopotentialHeight {
        values.iter().map(|value| value * 0.1).collect()
    } else if selector.field == CanonicalField::PressureReducedToMeanSeaLevel {
        values.iter().map(|value| value * 0.01).collect()
    } else {
        values.to_vec()
    };
    let (levels, color, width, labels) = match selector {
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(200),
        } => (range_step(1020.0, 1290.0, 6.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(300),
        } => (range_step(780.0, 1020.0, 6.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(500),
        } => (range_step(450.0, 651.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(700),
        } => (range_step(180.0, 361.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(850),
        } => (range_step(0.0, 201.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::PressureReducedToMeanSeaLevel,
            ..
        } => (range_step(960.0, 1045.0, 2.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::UpdraftHelicity,
            vertical:
                rustwx_core::VerticalSelector::HeightAboveGroundLayerMeters {
                    bottom_m: 2000,
                    top_m: 5000,
                },
        } => (
            vec![25.0, 50.0, 75.0, 100.0, 150.0, 200.0],
            Color::rgba(166, 0, 255, 255),
            2,
            false,
        ),
        _ => (range_step(0.0, 200.0, 10.0), Color::BLACK, 1, true),
    };

    Some(ContourLayer {
        data,
        levels,
        color,
        width,
        labels,
        show_extrema: false,
    })
}

fn build_barb_layers(
    recipe: &PlotRecipe,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    bounds: (f64, f64, f64, f64),
    barb_stride_cache: &SharedBarbStrideCache,
) -> Vec<WindBarbLayer> {
    let (Some(u_spec), Some(v_spec)) = (&recipe.barbs_u, &recipe.barbs_v) else {
        return Vec::new();
    };
    let (Some(u_selector), Some(v_selector)) = (u_spec.selector, v_spec.selector) else {
        return Vec::new();
    };
    let (Some(u), Some(v)) = (extracted.get(&u_selector), extracted.get(&v_selector)) else {
        return Vec::new();
    };
    let (stride_x, stride_y) =
        cached_barb_strides(u_selector, v_selector, &u.grid, bounds, barb_stride_cache);
    vec![WindBarbLayer {
        u: u.values.iter().map(|value| value * 1.943_844_5).collect(),
        v: v.values.iter().map(|value| value * 1.943_844_5).collect(),
        stride_x,
        stride_y,
        color: Color::BLACK,
        width: 1,
        length_px: 20.0,
    }]
}

fn cached_barb_strides(
    u_selector: FieldSelector,
    v_selector: FieldSelector,
    grid: &rustwx_core::LatLonGrid,
    bounds: (f64, f64, f64, f64),
    barb_stride_cache: &SharedBarbStrideCache,
) -> (usize, usize) {
    let key = BarbStrideCacheKey {
        u_selector,
        v_selector,
        bounds_bits: [
            bounds.0.to_bits(),
            bounds.1.to_bits(),
            bounds.2.to_bits(),
            bounds.3.to_bits(),
        ],
    };

    {
        let cache = barb_stride_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(&strides) = cache.get(&key) {
            return strides;
        }
    }

    let (visible_nx, visible_ny) = visible_grid_span(grid, bounds);
    let strides = (
        ((visible_nx as f64 / 24.0).round() as usize).clamp(3, 128),
        ((visible_ny as f64 / 14.0).round() as usize).clamp(3, 96),
    );

    let mut cache = barb_stride_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cache.entry(key).or_insert(strides)
}

pub(crate) fn build_projected_map(
    lat_deg: &[f32],
    lon_deg: &[f32],
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    let proj = LambertConformal::new(33.0, 45.0, -97.0, 39.0);
    let mut projected_x = Vec::with_capacity(lat_deg.len());
    let mut projected_y = Vec::with_capacity(lat_deg.len());
    let mut full_min_x = f64::INFINITY;
    let mut full_max_x = f64::NEG_INFINITY;
    let mut full_min_y = f64::INFINITY;
    let mut full_max_y = f64::NEG_INFINITY;
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    for (&lat, &lon) in lat_deg.iter().zip(lon_deg.iter()) {
        let lat = lat as f64;
        let lon = lon as f64;
        let (x, y) = proj.project(lat, lon);
        projected_x.push(x);
        projected_y.push(y);
        if x.is_finite() && y.is_finite() {
            full_min_x = full_min_x.min(x);
            full_max_x = full_max_x.max(x);
            full_min_y = full_min_y.min(y);
            full_max_y = full_max_y.max(y);
        }
        if lon >= bounds.0 && lon <= bounds.1 && lat >= bounds.2 && lat <= bounds.3 {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    }

    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        min_x = full_min_x;
        max_x = full_max_x;
        min_y = full_min_y;
        max_y = full_max_y;
    }

    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        return Err("projected extent produced no finite coordinates".into());
    }

    let extent = MapExtent::from_bounds(min_x, max_x, min_y, max_y, target_ratio);
    let mut lines = Vec::new();
    for layer in load_styled_conus_features() {
        for line in layer.lines {
            lines.push(ProjectedLineOverlay {
                points: line
                    .into_iter()
                    .map(|(lon, lat)| proj.project(lat, lon))
                    .collect(),
                color: Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a),
                width: layer.width,
            });
        }
    }

    Ok(ProjectedMap {
        projected_x,
        projected_y,
        extent: ProjectedExtent {
            x_min: extent.x_min,
            x_max: extent.x_max,
            y_min: extent.y_min,
            y_max: extent.y_max,
        },
        lines,
    })
}

fn range_step(start: f64, stop: f64, step: f64) -> Vec<f64> {
    let mut values = Vec::new();
    let mut current = start;
    while current < stop - step * 1.0e-9 {
        values.push(current);
        current += step;
    }
    values
}

fn visible_grid_span(
    grid: &rustwx_core::LatLonGrid,
    bounds: (f64, f64, f64, f64),
) -> (usize, usize) {
    let mut min_i = usize::MAX;
    let mut max_i = 0usize;
    let mut min_j = usize::MAX;
    let mut max_j = 0usize;

    for j in 0..grid.shape.ny {
        for i in 0..grid.shape.nx {
            let idx = j * grid.shape.nx + i;
            let lat = grid.lat_deg[idx] as f64;
            let lon = grid.lon_deg[idx] as f64;
            if lon >= bounds.0 && lon <= bounds.1 && lat >= bounds.2 && lat <= bounds.3 {
                min_i = min_i.min(i);
                max_i = max_i.max(i);
                min_j = min_j.min(j);
                max_j = max_j.max(j);
            }
        }
    }

    if min_i == usize::MAX || min_j == usize::MAX {
        return (grid.shape.nx.max(1), grid.shape.ny.max(1));
    }

    (max_i - min_i + 1, max_j - min_j + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{GridShape, LatLonGrid, SelectedField2D};

    fn sample_grid() -> LatLonGrid {
        LatLonGrid::new(
            GridShape::new(2, 2).unwrap(),
            vec![35.0, 35.0, 36.0, 36.0],
            vec![-100.0, -99.0, -100.0, -99.0],
        )
        .unwrap()
    }

    fn sample_selected_field(
        selector: FieldSelector,
        units: &str,
        values: Vec<f32>,
    ) -> SelectedField2D {
        SelectedField2D::new(selector, units, sample_grid(), values).unwrap()
    }

    fn sample_direct_request(model: ModelId) -> DirectBatchRequest {
        DirectBatchRequest {
            model,
            date_yyyymmdd: "20260414".to_string(),
            cycle_override_utc: Some(23),
            forecast_hour: 6,
            source: rustwx_models::model_summary(model).sources[0].id,
            domain: DomainSpec::new("midwest", (-105.0, -80.0, 30.0, 50.0)),
            out_dir: PathBuf::from("C:\\temp\\rustwx-tests"),
            cache_root: PathBuf::from("C:\\temp\\rustwx-tests-cache"),
            use_cache: false,
            recipe_slugs: Vec::new(),
            product_overrides: HashMap::new(),
        }
    }

    #[test]
    fn planning_hrrr_direct_batch_dedupes_recipe_aliases() {
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "500mb_temperature_height_winds".to_string(),
                "500mb temperature height winds".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].recipe.slug, "500mb_temperature_height_winds");
        assert_eq!(planned[0].plan.product, "prs");
    }

    #[test]
    fn grouping_keeps_shared_prs_selector_union_under_whole_file_fetches() {
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "500mb_temperature_height_winds".to_string(),
                "700mb_temperature_height_winds".to_string(),
            ],
        )
        .unwrap();
        let request = sample_direct_request(ModelId::Hrrr);
        let groups = group_direct_fetches(&request, &planned);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].product, "prs");
        assert_eq!(
            groups[0].fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert!(
            groups[0]
                .selectors
                .contains(&FieldSelector::isobaric(CanonicalField::Temperature, 500))
        );
        assert!(
            groups[0]
                .selectors
                .contains(&FieldSelector::isobaric(CanonicalField::Temperature, 700))
        );
        assert!(groups[0].variable_patterns.is_empty());
    }

    #[test]
    fn direct_fetch_request_uses_full_family_bytes() {
        let request = sample_direct_request(ModelId::Hrrr);
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Nomads,
        };
        let group = FetchGroup {
            product: "prs".to_string(),
            fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
            variable_patterns: vec!["TMP:500 mb".to_string()],
            selectors: vec![FieldSelector::isobaric(CanonicalField::Temperature, 500)],
        };
        let fetch = build_direct_fetch_request(&request, &latest, 6, &group).unwrap();
        assert_eq!(fetch.request.product, "prs");
        assert_eq!(fetch.source_override, Some(SourceId::Nomads));
        assert!(fetch.variable_patterns.is_empty());
    }

    #[test]
    fn native_fetches_share_surface_family_file() {
        let request = sample_direct_request(ModelId::Hrrr);
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Aws,
        };
        let group = FetchGroup {
            product: canonical_fetch_product(&request, "nat"),
            fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
            variable_patterns: Vec::new(),
            selectors: vec![FieldSelector::entire_atmosphere(
                CanonicalField::CompositeReflectivity,
            )],
        };
        let fetch = build_direct_fetch_request(&request, &latest, 6, &group).unwrap();
        assert_eq!(fetch.request.product, "sfc");
    }

    #[test]
    fn direct_fetch_timing_keeps_planned_vs_actual_family_truth() {
        let request = sample_direct_request(ModelId::Hrrr);
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Nomads,
        };
        let planned_product = "nat";
        let group = FetchGroup {
            product: canonical_fetch_product(&request, planned_product),
            fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
            variable_patterns: Vec::new(),
            selectors: vec![FieldSelector::entire_atmosphere(
                CanonicalField::CompositeReflectivity,
            )],
        };
        let fetch = build_direct_fetch_request(&request, &latest, 6, &group).unwrap();
        let runtime = HrrrDirectFetchRuntimeInfo {
            fetch_key: crate::publication::fetch_key(planned_product, &fetch.request),
            planned_product: planned_product.into(),
            fetched_product: fetch.request.product.clone(),
            requested_source: fetch.source_override.unwrap(),
            resolved_source: SourceId::Nomads,
            resolved_url: "https://example.test/hrrr.t23z.wrfsfcf06.grib2".into(),
        };
        assert_eq!(runtime.planned_product, "nat");
        assert_eq!(runtime.fetched_product, "sfc");
        assert_eq!(runtime.resolved_source, SourceId::Nomads);
        assert!(runtime.resolved_url.contains("wrfsfc"));
    }

    #[test]
    fn all_hrrr_direct_fetch_requests_strip_idx_patterns_before_fetch() {
        let request = sample_direct_request(ModelId::Hrrr);
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Aws,
        };
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "500mb_temperature_height_winds".to_string(),
                "2m_temperature_10m_winds".to_string(),
                "composite_reflectivity".to_string(),
            ],
        )
        .unwrap();
        let groups = group_direct_fetches(&request, &planned);
        assert_eq!(groups.len(), 2);

        for group in &groups {
            let fetch = build_direct_fetch_request(&request, &latest, 6, group).unwrap();
            assert_eq!(
                group.fetch_mode,
                PlotRecipeFetchMode::WholeFileStructuredExtract
            );
            assert!(
                fetch.variable_patterns.is_empty(),
                "full-family HRRR direct fetches should not send idx subset patterns"
            );
        }
    }

    #[test]
    fn grouping_splits_prs_and_nat_recipes() {
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "500mb_temperature_height_winds".to_string(),
                "composite_reflectivity".to_string(),
            ],
        )
        .unwrap();
        let request = sample_direct_request(ModelId::Hrrr);
        let groups = group_direct_fetches(&request, &planned);
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().any(|group| group.product == "prs"));
        assert!(groups.iter().any(|group| group.product == "sfc"));
    }

    #[test]
    fn planning_supports_hrrr_direct_composite_layout_recipes() {
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "cloud_cover_levels".to_string(),
                "precipitation_type".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(planned.len(), 2);

        let request = sample_direct_request(ModelId::Hrrr);
        let groups = group_direct_fetches(&request, &planned);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].product, "sfc");
        assert!(
            groups[0]
                .selectors
                .contains(&FieldSelector::entire_atmosphere(
                    CanonicalField::LowCloudCover
                ))
        );
        assert!(
            groups[0]
                .selectors
                .contains(&FieldSelector::surface(CanonicalField::CategoricalSnow))
        );
    }

    #[test]
    fn unsupported_recipe_error_stays_explicit() {
        let err = plan_direct_recipes(ModelId::Hrrr, &["1h_qpf".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("windowed lane") || err.contains("not supported"));
    }

    #[test]
    fn gfs_direct_fetches_are_now_whole_file() {
        let planned = plan_direct_recipes(
            ModelId::Gfs,
            &["500mb_temperature_height_winds".to_string()],
        )
        .unwrap();
        let request = sample_direct_request(ModelId::Gfs);
        let groups = group_direct_fetches(&request, &planned);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        let request = sample_direct_request(ModelId::Gfs);
        let latest = LatestRun {
            model: ModelId::Gfs,
            cycle: rustwx_core::CycleSpec::new("20260414", 18).unwrap(),
            source: SourceId::Nomads,
        };
        let fetch = build_direct_fetch_request(&request, &latest, 6, &groups[0]).unwrap();
        assert_eq!(fetch.request.product, "pgrb2.0p25");
        assert!(fetch.variable_patterns.is_empty());
    }

    #[test]
    fn rrfs_direct_product_overrides_can_select_na_family() {
        let mut request = sample_direct_request(ModelId::RrfsA);
        request
            .product_overrides
            .insert("prs-conus".to_string(), "prs-na".to_string());
        let latest = LatestRun {
            model: ModelId::RrfsA,
            cycle: rustwx_core::CycleSpec::new("20260414", 20).unwrap(),
            source: SourceId::Aws,
        };
        let group = FetchGroup {
            product: canonical_fetch_product(&request, "prs-conus"),
            fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
            variable_patterns: Vec::new(),
            selectors: vec![FieldSelector::isobaric(CanonicalField::Temperature, 500)],
        };
        let fetch = build_direct_fetch_request(&request, &latest, 2, &group).unwrap();
        assert_eq!(fetch.request.product, "prs-na");
    }

    #[test]
    fn convert_filled_field_applies_operational_unit_transforms() {
        let pressure_recipe = plot_recipe("mslp_10m_winds").unwrap();
        let pressure_field = sample_selected_field(
            FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel),
            "Pa",
            vec![100000.0; 4],
        );
        let converted_pressure = convert_filled_field(pressure_recipe, &pressure_field);
        assert_eq!(converted_pressure.units, "hPa");
        assert_eq!(converted_pressure.values[0], 1000.0);

        let pwat_recipe = plot_recipe("precipitable_water").unwrap();
        let pwat_field = sample_selected_field(
            FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater),
            "kg/m^2",
            vec![25.4; 4],
        );
        let converted_pwat = convert_filled_field(pwat_recipe, &pwat_field);
        assert_eq!(converted_pwat.units, "in");
        assert!((converted_pwat.values[0] - 1.0).abs() < 1.0e-6);

        let vis_recipe = plot_recipe("visibility").unwrap();
        let vis_field = sample_selected_field(
            FieldSelector::surface(CanonicalField::Visibility),
            "m",
            vec![1609.344; 4],
        );
        let converted_vis = convert_filled_field(vis_recipe, &vis_field);
        assert_eq!(converted_vis.units, "mi");
        assert!((converted_vis.values[0] - 1.0).abs() < 1.0e-4);

        let vort_recipe = plot_recipe("500mb_absolute_vorticity_height_winds").unwrap();
        let vort_field = sample_selected_field(
            FieldSelector::isobaric(CanonicalField::AbsoluteVorticity, 500),
            "s^-1",
            vec![0.0002; 4],
        );
        let converted_vort = convert_filled_field(vort_recipe, &vort_field);
        assert_eq!(converted_vort.units, "10^-5 s^-1");
        assert!((converted_vort.values[0] - 20.0).abs() < 1.0e-6);
    }

    #[test]
    fn overlay_only_rule_catches_height_and_mslp_products() {
        assert!(should_render_overlay_only(
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
            false
        ));
        assert!(should_render_overlay_only(
            FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel),
            false
        ));
        assert!(!should_render_overlay_only(
            FieldSelector::isobaric(CanonicalField::Temperature, 500),
            true
        ));
        assert!(!should_render_overlay_only(
            FieldSelector::surface(CanonicalField::Visibility),
            false
        ));
    }
}
