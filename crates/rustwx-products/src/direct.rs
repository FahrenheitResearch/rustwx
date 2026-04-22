use crate::derived::NativeContourRenderMode;
use grib_core::grib2::Grib2File;
use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, CanonicalField, CycleSpec, FieldSelector,
    ModelId, SelectedField2D, SourceId, VerticalSelector,
};
use rustwx_io::{
    extract_fields_from_grib2_partial, load_cached_selected_field, store_cached_selected_field,
};
use rustwx_models::{
    LatestRun, ModelError, PlotRecipe, PlotRecipeFetchMode, PlotRecipeFetchPlan, RenderStyle,
    latest_available_run_at_forecast_hour, plot_recipe, plot_recipe_fetch_plan,
};
use rustwx_render::{
    Color, ColorScale, ContourLayer, DiscreteColorScale, DomainFrame, ExtendMode, LevelDensity,
    MapRenderRequest, PanelGridLayout, PanelPadding, PngCompressionMode, PngWriteOptions,
    ProductVisualMode, ProjectedContourLineStyle, ProjectedDomain, ProjectedMap, RenderImageTiming,
    RenderStateTiming, WindBarbLayer, build_projected_contour_geometry_profile,
    densify_discrete_scale, draw_centered_text_line, map_frame_aspect_ratio_for_mode,
    render_panel_grid, save_png_profile_with_options, save_rgba_png_profile_with_options,
    solar07::{Solar07Palette, solar07_palette},
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use crate::planner::{ExecutionPlan, ExecutionPlanBuilder};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
    fetch_identity_from_cached_result_with_aliases,
};
use crate::runtime::{
    BundleLoaderConfig, FetchedBundleBytes, LoadedBundleSet, load_execution_plan,
};
use crate::shared_context::{DomainSpec, ProjectedMapProvider};
use crate::source::{ProductSourceRoute, direct_route_for_recipe_slug};
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

fn default_output_width() -> u32 {
    OUTPUT_WIDTH
}

fn default_output_height() -> u32 {
    OUTPUT_HEIGHT
}

fn default_png_compression() -> PngCompressionMode {
    PngCompressionMode::Default
}

fn default_native_fill_level_multiplier() -> usize {
    1
}

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
    #[serde(default)]
    pub contour_mode: NativeContourRenderMode,
    #[serde(default = "default_native_fill_level_multiplier")]
    pub native_fill_level_multiplier: usize,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
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
    #[serde(default)]
    pub contour_mode: NativeContourRenderMode,
    #[serde(default = "default_native_fill_level_multiplier")]
    pub native_fill_level_multiplier: usize,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectFetchRuntimeInfo {
    pub fetch_key: String,
    /// Canonical (physical) family name that was actually fetched.
    ///
    /// Kept equal to `fetched_product` for backward-compatibility with
    /// existing manifest consumers; the logical families that contributed
    /// to this canonical fetch are surfaced separately in
    /// `planned_family_aliases` so audit tooling can tell which recipes
    /// rerouted (e.g. HRRR "nat" → "sfc").
    pub planned_product: String,
    pub fetched_product: String,
    /// Sorted de-duplicated set of logical planned families (before
    /// canonicalization) that were merged into this fetch. For non-HRRR
    /// models this equals `[planned_product]`; for HRRR it can include
    /// "nat" alongside "sfc" when composite/native-family recipes share
    /// the wrfsfc file with surface recipes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub planned_family_aliases: Vec<String>,
    pub requested_source: SourceId,
    pub resolved_source: SourceId,
    pub resolved_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRecipeTiming {
    pub project_ms: u128,
    #[serde(default)]
    pub field_prepare_ms: u128,
    #[serde(default)]
    pub contour_prepare_ms: u128,
    #[serde(default)]
    pub barb_prepare_ms: u128,
    #[serde(default)]
    pub render_to_image_ms: u128,
    #[serde(default)]
    pub data_layer_draw_ms: u128,
    #[serde(default)]
    pub overlay_draw_ms: u128,
    #[serde(default)]
    pub panel_compose_ms: u128,
    pub request_build_ms: u128,
    pub render_state_prep_ms: u128,
    pub png_encode_ms: u128,
    pub file_write_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
    pub state_timing: RenderStateTiming,
    pub image_timing: RenderImageTiming,
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
    pub source_route: ProductSourceRoute,
    pub grib_product: String,
    pub fetched_grib_product: String,
    pub resolved_source: SourceId,
    pub resolved_url: String,
    pub output_path: PathBuf,
    pub content_identity: ArtifactContentIdentity,
    pub input_fetch_keys: Vec<String>,
    pub timing: DirectRecipeTiming,
}

/// Per-recipe failure that doesn't abort the whole batch. Emitted when
/// a recipe's required GRIB message isn't present in the file (e.g.,
/// GFS f000 doesn't publish accumulated APCP, ECMWF doesn't expose 2 m
/// RH) or when a render-time error hits just that recipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRecipeBlocker {
    pub recipe_slug: String,
    pub reason: String,
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
    /// Recipes that couldn't render — missing GRIB messages or render
    /// errors. Populated instead of short-circuiting the batch, so
    /// orchestration callers get per-recipe signal rather than a single
    /// hard error on the first problem.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<DirectRecipeBlocker>,
    pub total_ms: u128,
}

pub type HrrrDirectFetchRuntimeInfo = DirectFetchRuntimeInfo;
pub type HrrrDirectRecipeTiming = DirectRecipeTiming;
pub type HrrrDirectFetchTiming = DirectFetchTiming;
pub type HrrrDirectRenderedRecipe = DirectRenderedRecipe;
pub type HrrrDirectRecipeBlocker = DirectRecipeBlocker;
pub type HrrrDirectBatchReport = DirectBatchReport;

#[derive(Debug, Clone, Copy, Default)]
struct DirectRequestBuildTiming {
    field_prepare_ms: u128,
    contour_prepare_ms: u128,
    barb_prepare_ms: u128,
}

fn direct_data_layer_draw_ms(image_timing: &RenderImageTiming) -> u128 {
    image_timing.polygon_fill_ms
        + image_timing.projected_pixel_ms
        + image_timing.rasterize_ms
        + image_timing.raster_blit_ms
}

fn direct_overlay_draw_ms(image_timing: &RenderImageTiming) -> u128 {
    image_timing.linework_ms + image_timing.contour_ms + image_timing.barb_ms
}

#[derive(Debug, Clone)]
struct PlannedDirectRecipe {
    recipe: &'static PlotRecipe,
    plan: PlotRecipeFetchPlan,
}

#[derive(Debug, Clone)]
pub struct FetchGroup {
    pub product: String,
    pub fetch_mode: PlotRecipeFetchMode,
    // Retained for recipe-level coverage/debugging; the direct/native batch
    // path intentionally pulls full family GRIB bytes and extracts grouped
    // selectors from the parsed full file.
    pub variable_patterns: Vec<String>,
    pub selectors: Vec<FieldSelector>,
    /// Sorted set of logical planned-family names (as requested by the
    /// recipes' fetch plans) that collapsed into this canonical fetch. For
    /// HRRR this is how we preserve the "nat" logical identity even when
    /// it reroutes to the physical "sfc" file.
    pub planned_family_aliases: std::collections::BTreeSet<String>,
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

impl CompositePanelSpec {
    fn scaled_for_request(self, request: &DirectBatchRequest) -> Self {
        let scale_x = request.output_width as f64 / OUTPUT_WIDTH as f64;
        let scale_y = request.output_height as f64 / OUTPUT_HEIGHT as f64;
        Self {
            rows: self.rows,
            columns: self.columns,
            panel_width: ((self.panel_width as f64) * scale_x).round().max(1.0) as u32,
            panel_height: ((self.panel_height as f64) * scale_y).round().max(1.0) as u32,
            top_padding: ((self.top_padding as f64) * scale_y).round().max(1.0) as u32,
            component_slugs: self.component_slugs,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BarbStrideCacheKey {
    u_selector: FieldSelector,
    v_selector: FieldSelector,
    bounds_bits: [u64; 4],
}

type SharedContourLayerCache = Arc<Mutex<HashMap<FieldSelector, Option<ContourLayer>>>>;
type SharedBarbStrideCache = Arc<Mutex<HashMap<BarbStrideCacheKey, (usize, usize)>>>;
type SharedBarbLayerCache = Arc<Mutex<HashMap<BarbStrideCacheKey, Vec<WindBarbLayer>>>>;
type SharedProjectedMapCache = Arc<Mutex<HashMap<(u32, u32, u8), ProjectedMap>>>;
type PreparedProjectedMaps = Arc<HashMap<(u32, u32, u8), ProjectedMap>>;

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
            contour_mode: request.contour_mode,
            native_fill_level_multiplier: request.native_fill_level_multiplier,
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
        }
    }

    /// Public planner-side conversion: lets the unified non-ECAPE-hour
    /// runner build a `DirectBatchRequest` from the HRRR-pinned variant
    /// so it can ask the direct lane to plan its fetch groups before
    /// loading bundles.
    pub fn from_hrrr_for_planner(request: &HrrrDirectBatchRequest) -> Self {
        Self::from_hrrr(request)
    }
}

impl DirectBatchRequest {
    fn png_write_options(&self) -> PngWriteOptions {
        PngWriteOptions {
            compression: self.png_compression,
        }
    }
}

/// Plan the direct lane's fetch groups without running the loader. The
/// unified non-ECAPE-hour runner uses this to build a single execution
/// plan that covers direct + derived (+ severe/ECAPE if requested).
pub fn plan_direct_fetch_groups(
    request: &DirectBatchRequest,
) -> Result<Vec<FetchGroup>, Box<dyn std::error::Error>> {
    let planned = plan_direct_recipes(request.model, &request.recipe_slugs)?;
    Ok(group_direct_fetches(request, &planned))
}

fn resolve_direct_run(
    model: ModelId,
    date: &str,
    cycle_override: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model,
            cycle: CycleSpec::new(date, hour)?,
            source,
        }),
        None => Ok(latest_available_run_at_forecast_hour(
            model,
            Some(source),
            date,
            forecast_hour,
        )?),
    }
}

pub fn run_direct_batch(
    request: &DirectBatchRequest,
) -> Result<DirectBatchReport, Box<dyn std::error::Error>> {
    let latest = resolve_direct_run(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
    )?;
    run_direct_batch_with_context(request, &latest, None)
}

pub fn run_hrrr_direct_batch(
    request: &HrrrDirectBatchRequest,
) -> Result<HrrrDirectBatchReport, Box<dyn std::error::Error>> {
    run_direct_batch(&DirectBatchRequest::from_hrrr(request))
}

/// Planner-loaded entry point used by `hrrr_non_ecape_hour`. Direct
/// shares the unified `LoadedBundleSet` with the derived/severe lanes
/// when they co-run.
pub(crate) fn run_hrrr_direct_batch_from_loaded(
    request: &HrrrDirectBatchRequest,
    loaded: &LoadedBundleSet,
) -> Result<HrrrDirectBatchReport, Box<dyn std::error::Error>> {
    let generic = DirectBatchRequest::from_hrrr(request);
    run_direct_batch_from_loaded(
        &generic,
        loaded,
        &generic.cache_root,
        generic.use_cache,
        None,
    )
}

pub(crate) fn run_direct_batch_from_loaded(
    request: &DirectBatchRequest,
    loaded: &LoadedBundleSet,
    cache_root: &std::path::Path,
    use_cache: bool,
    shared_context: Option<&dyn ProjectedMapProvider>,
) -> Result<DirectBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if use_cache {
        fs::create_dir_all(cache_root)?;
    }
    let total_start = Instant::now();
    let planned = plan_direct_recipes(request.model, &request.recipe_slugs)?;
    let groups = group_direct_fetches(request, &planned);
    let mut extracted = HashMap::<FieldSelector, SelectedField2D>::new();
    let mut fetches = Vec::with_capacity(groups.len());
    let mut fetch_truth_by_actual_product = HashMap::<String, DirectFetchRuntimeInfo>::new();
    let mut missing_selectors = HashSet::<FieldSelector>::new();
    let mut blockers = Vec::<DirectRecipeBlocker>::new();

    for group in &groups {
        let fetched = match find_loaded_bytes_for_group(loaded, group) {
            Ok(bytes) => bytes,
            Err(err) => {
                // The whole fetch for this group is gone (upstream
                // planner fetch failure). Every recipe pointing at this
                // group becomes a blocker instead of crashing the batch.
                let reason = err.to_string();
                for selector in &group.selectors {
                    missing_selectors.insert(*selector);
                }
                for recipe_slug in recipe_slugs_depending_on_group(&planned, group) {
                    blockers.push(DirectRecipeBlocker {
                        recipe_slug,
                        reason: reason.clone(),
                    });
                }
                continue;
            }
        };
        let (fields, unmatched, timing) =
            extract_direct_fetch_group_from_loaded(request, group, fetched, use_cache)?;
        extracted.extend(fields.into_iter().map(|field| (field.selector, field)));
        for selector in unmatched {
            missing_selectors.insert(selector);
        }
        fetch_truth_by_actual_product.insert(group.product.clone(), timing.runtime_fetch.clone());
        fetches.push(timing);
    }

    let (renderable, selector_blockers) =
        partition_recipes_by_selector_availability(&planned, &missing_selectors);
    blockers.extend(selector_blockers);

    let rendered = render_direct_recipes(
        request,
        &loaded.latest,
        &renderable,
        &extracted,
        &fetch_truth_by_actual_product,
        shared_context,
    )?;

    Ok(DirectBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: loaded.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: loaded.latest.source,
        domain: request.domain.clone(),
        fetches,
        recipes: rendered,
        blockers,
        total_ms: total_start.elapsed().as_millis(),
    })
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
    // Build the typed execution plan from the recipe fetch groups. Each
    // group becomes a NativeAnalysis bundle whose native_override is the
    // canonical fetched product — the planner merges direct groups that
    // share a physical file with other lanes (severe/ECAPE). The direct
    // lane still runs its own per-selector extract out of the bytes the
    // loader fetched.
    let plan = build_direct_execution_plan(latest, request.forecast_hour, &groups);
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig {
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
        },
    )?;

    let mut extracted = HashMap::<FieldSelector, SelectedField2D>::new();
    let mut fetches = Vec::with_capacity(groups.len());
    let mut fetch_truth_by_actual_product = HashMap::<String, DirectFetchRuntimeInfo>::new();
    let mut missing_selectors = HashSet::<FieldSelector>::new();
    let mut blockers = Vec::<DirectRecipeBlocker>::new();

    for group in &groups {
        let fetched = match find_loaded_bytes_for_group(&loaded, group) {
            Ok(bytes) => bytes,
            Err(err) => {
                let reason = err.to_string();
                for selector in &group.selectors {
                    missing_selectors.insert(*selector);
                }
                for recipe_slug in recipe_slugs_depending_on_group(&planned, group) {
                    blockers.push(DirectRecipeBlocker {
                        recipe_slug,
                        reason: reason.clone(),
                    });
                }
                continue;
            }
        };
        let (fields, unmatched, timing) =
            extract_direct_fetch_group_from_loaded(request, group, fetched, request.use_cache)?;
        extracted.extend(fields.into_iter().map(|field| (field.selector, field)));
        for selector in unmatched {
            missing_selectors.insert(selector);
        }
        fetch_truth_by_actual_product.insert(group.product.clone(), timing.runtime_fetch.clone());
        fetches.push(timing);
    }

    let (renderable, selector_blockers) =
        partition_recipes_by_selector_availability(&planned, &missing_selectors);
    blockers.extend(selector_blockers);

    let rendered = render_direct_recipes(
        request,
        latest,
        &renderable,
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
        blockers,
        total_ms: total_start.elapsed().as_millis(),
    })
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

/// Which planned recipe slugs route their fetches through this group?
/// Used when the group's underlying fetch failed upstream so every
/// dependent recipe becomes a blocker with the fetch's error reason.
fn recipe_slugs_depending_on_group(
    planned: &[PlannedDirectRecipe],
    group: &FetchGroup,
) -> Vec<String> {
    planned
        .iter()
        .filter(|item| {
            // A recipe routes through this group iff the group's
            // selectors contain any of the recipe's plan selectors.
            item.plan
                .selectors()
                .into_iter()
                .any(|sel| group.selectors.contains(&sel))
        })
        .map(|item| item.recipe.slug.to_string())
        .collect()
}

/// Split the planned list into (renderable, blockers) based on which
/// selectors the extraction pass could actually produce. A recipe is
/// blocked when its filled selector (or, for composite panels, any
/// component recipe's filled selector) is missing from the GRIB file.
/// Everything else passes through to the render pipeline unchanged.
fn partition_recipes_by_selector_availability(
    planned: &[PlannedDirectRecipe],
    missing: &HashSet<FieldSelector>,
) -> (Vec<PlannedDirectRecipe>, Vec<DirectRecipeBlocker>) {
    let mut renderable = Vec::with_capacity(planned.len());
    let mut blockers = Vec::new();
    for item in planned {
        let reason = recipe_block_reason(item.recipe, missing);
        match reason {
            Some(reason) => blockers.push(DirectRecipeBlocker {
                recipe_slug: item.recipe.slug.to_string(),
                reason,
            }),
            None => renderable.push(item.clone()),
        }
    }
    (renderable, blockers)
}

/// If any selector required to render `recipe` is missing, return a
/// human-readable blocker reason. Otherwise `None`.
fn recipe_block_reason(recipe: &PlotRecipe, missing: &HashSet<FieldSelector>) -> Option<String> {
    if let Some(spec) = composite_panel_spec(recipe.slug) {
        for component_slug in spec.component_slugs {
            let Some(component) = plot_recipe(component_slug) else {
                continue;
            };
            if let Some(selector) = component.filled.selector {
                if missing.contains(&selector) {
                    return Some(format!(
                        "composite component '{}' missing selector {}",
                        component_slug,
                        selector.key()
                    ));
                }
            }
        }
        return None;
    }
    if let Some(selector) = recipe.filled.selector {
        if missing.contains(&selector) {
            return Some(format!(
                "missing GRIB message for filled selector {}",
                selector.key()
            ));
        }
    }
    None
}

fn group_direct_fetches(
    request: &DirectBatchRequest,
    recipes: &[PlannedDirectRecipe],
) -> Vec<FetchGroup> {
    let mut grouped = HashMap::<String, FetchGroup>::new();
    for item in recipes {
        let planned_family = item.plan.product.to_string();
        let key = canonical_fetch_product(request, planned_family.as_str());
        let entry = grouped.entry(key.clone()).or_insert_with(|| FetchGroup {
            product: key.clone(),
            fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
            variable_patterns: Vec::new(),
            selectors: Vec::new(),
            planned_family_aliases: std::collections::BTreeSet::new(),
        });
        entry.planned_family_aliases.insert(planned_family);
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

fn build_direct_execution_plan(
    latest: &LatestRun,
    forecast_hour: u16,
    groups: &[FetchGroup],
) -> ExecutionPlan {
    let mut builder = ExecutionPlanBuilder::new(latest, forecast_hour);
    for group in groups {
        // Each direct fetch group corresponds to one unique physical
        // GRIB file. Express it as a NativeAnalysis bundle with the
        // canonical fetched product as native_override; record every
        // logical planned family (e.g. "nat", "sfc") so manifests can
        // surface the aliases.
        let requirement =
            BundleRequirement::new(CanonicalBundleDescriptor::NativeAnalysis, forecast_hour)
                .with_native_override(group.product.clone());
        for alias in &group.planned_family_aliases {
            builder.require_with_logical_family(&requirement, Some(alias));
        }
    }
    builder.build()
}

fn find_loaded_bytes_for_group<'a>(
    loaded: &'a LoadedBundleSet,
    group: &FetchGroup,
) -> Result<&'a FetchedBundleBytes, Box<dyn std::error::Error>> {
    loaded
        .fetched
        .values()
        .find(|bundle| bundle.key.native_product == group.product)
        .ok_or_else(|| {
            format!(
                "direct planner missed fetch for canonical family '{}'",
                group.product
            )
            .into()
        })
}

fn extract_direct_fetch_group_from_loaded(
    request: &DirectBatchRequest,
    group: &FetchGroup,
    fetched: &FetchedBundleBytes,
    use_cache: bool,
) -> Result<(Vec<SelectedField2D>, Vec<FieldSelector>, DirectFetchTiming), Box<dyn std::error::Error>>
{
    let total_start = Instant::now();
    let fetch_request = &fetched.file.request;
    let cached_result = &fetched.file.fetched;
    let fetch_ms = fetched.fetch_ms;

    let extract_start = Instant::now();
    let mut extracted = Vec::<SelectedField2D>::new();
    let mut missing = Vec::<FieldSelector>::new();
    let mut extract_cache_hits = 0usize;
    if use_cache {
        for selector in &group.selectors {
            if let Some(cached) =
                load_cached_selected_field(&request.cache_root, fetch_request, *selector)?
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
        Some(Grib2File::from_bytes(&fetched.file.bytes)?)
    };
    let parse_ms = parse_start.elapsed().as_millis();

    // Selectors whose GRIB message wasn't present in the file go here;
    // the caller uses them to mark dependent recipes as blockers
    // instead of the whole batch tripping on the first missing message.
    let mut unmatched = Vec::<FieldSelector>::new();
    if let Some(grib) = grib.as_ref() {
        let partial = extract_fields_from_grib2_partial(grib, &missing)?;
        if use_cache {
            for field in &partial.extracted {
                store_cached_selected_field(&request.cache_root, fetch_request, field)?;
            }
        }
        let fetched_count = partial.extracted.len();
        extracted.extend(partial.extracted);
        unmatched = partial.missing;
        // extract_cache_misses was previously "count of selectors we
        // had to decode from GRIB"; keep that meaning by subtracting
        // truly-unmatched selectors from the count we actually pulled.
        let _ = fetched_count;
    }
    let extract_ms = extract_start.elapsed().as_millis();

    let extract_cache_misses = missing.len().saturating_sub(unmatched.len());

    Ok((
        extracted,
        unmatched,
        DirectFetchTiming {
            product: group.product.clone(),
            fetch_mode: group.fetch_mode,
            fetch_ms,
            parse_ms,
            extract_ms,
            total_ms: total_start.elapsed().as_millis(),
            fetch_cache_hit: cached_result.cache_hit,
            extract_cache_hits,
            extract_cache_misses,
            runtime_fetch: DirectFetchRuntimeInfo {
                fetch_key: crate::publication::fetch_key(
                    group.product.as_str(),
                    &fetch_request.request,
                ),
                planned_product: group.product.clone(),
                fetched_product: fetch_request.request.product.clone(),
                planned_family_aliases: group.planned_family_aliases.iter().cloned().collect(),
                requested_source: fetch_request
                    .source_override
                    .unwrap_or(cached_result.result.source),
                resolved_source: cached_result.result.source,
                resolved_url: cached_result.result.url.clone(),
            },
            input_fetch: fetch_identity_from_cached_result_with_aliases(
                group.product.as_str(),
                group
                    .planned_family_aliases
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>(),
                fetch_request,
                cached_result,
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
    if planned.is_empty() {
        return Ok(Vec::new());
    }

    let contour_layer_cache = Arc::new(Mutex::new(HashMap::new()));
    let barb_layer_cache = Arc::new(Mutex::new(HashMap::new()));
    let barb_stride_cache = Arc::new(Mutex::new(HashMap::new()));
    let projected_map_cache = Arc::new(Mutex::new(HashMap::new()));
    let prepared_projected_maps = build_prepared_projected_maps(request, planned, extracted)?;
    if prepared_projected_maps.is_empty() {
        return Ok(Vec::new());
    }
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
                    &contour_layer_cache,
                    &barb_layer_cache,
                    &barb_stride_cache,
                    &projected_map_cache,
                    &prepared_projected_maps,
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
            let contour_layer_cache = Arc::clone(&contour_layer_cache);
            let barb_layer_cache = Arc::clone(&barb_layer_cache);
            let projected_map_cache = Arc::clone(&projected_map_cache);
            let prepared_projected_maps = Arc::clone(&prepared_projected_maps);
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
                            &contour_layer_cache,
                            &barb_layer_cache,
                            &barb_stride_cache,
                            &projected_map_cache,
                            &prepared_projected_maps,
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

    let override_threads = std::env::var("RUSTWX_RENDER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0);

    thread::available_parallelism()
        .map(|count| override_threads.unwrap_or((count.get() / 2).max(1)))
        .unwrap_or(1)
        .min(recipe_count)
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

fn visual_mode_cache_key(mode: ProductVisualMode) -> u8 {
    match mode {
        ProductVisualMode::FilledMeteorology => 0,
        ProductVisualMode::UpperAirAnalysis => 1,
        ProductVisualMode::OverlayAnalysis => 2,
        ProductVisualMode::SevereDiagnostic => 3,
        ProductVisualMode::PanelMember => 4,
        ProductVisualMode::ComparisonPanel => 5,
    }
}

fn standard_projected_key(
    request: &DirectBatchRequest,
    recipe: &PlotRecipe,
) -> Option<(u32, u32, u8)> {
    let filled_selector = recipe.filled.selector?;
    let overlay_only = should_render_overlay_only(filled_selector, recipe.contours.is_some());
    let visual_mode = visual_mode_for_direct_recipe(recipe, filled_selector, overlay_only);
    Some((
        request.output_width,
        request.output_height,
        visual_mode_cache_key(visual_mode),
    ))
}

fn build_prepared_projected_maps(
    request: &DirectBatchRequest,
    planned: &[PlannedDirectRecipe],
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> Result<PreparedProjectedMaps, Box<dyn std::error::Error>> {
    let Some(sample_field) = planned.iter().find_map(|item| {
        item.recipe
            .filled
            .selector
            .and_then(|selector| extracted.get(&selector))
    }) else {
        return Ok(Arc::new(HashMap::new()));
    };

    let mut keys = std::collections::BTreeSet::<(u32, u32, u8)>::new();
    for item in planned {
        if let Some(spec) = composite_panel_spec(item.recipe.slug) {
            let spec = spec.scaled_for_request(request);
            keys.insert((
                spec.panel_width,
                spec.panel_height,
                visual_mode_cache_key(ProductVisualMode::PanelMember),
            ));
        } else if let Some(key) = standard_projected_key(request, item.recipe) {
            keys.insert(key);
        }
    }

    let mut prepared = HashMap::new();
    for (width, height, mode_key) in keys {
        let visual_mode = match mode_key {
            0 => ProductVisualMode::FilledMeteorology,
            1 => ProductVisualMode::UpperAirAnalysis,
            2 => ProductVisualMode::OverlayAnalysis,
            3 => ProductVisualMode::SevereDiagnostic,
            4 => ProductVisualMode::PanelMember,
            5 => ProductVisualMode::ComparisonPanel,
            _ => ProductVisualMode::FilledMeteorology,
        };
        let projected = build_projected_map_with_projection(
            &sample_field.grid.lat_deg,
            &sample_field.grid.lon_deg,
            sample_field.projection.as_ref(),
            request.domain.bounds,
            map_frame_aspect_ratio_for_mode(visual_mode, width, height, true, true),
        )?;
        prepared.insert((width, height, mode_key), projected);
    }
    Ok(Arc::new(prepared))
}

fn render_direct_recipe(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    item: &PlannedDirectRecipe,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    fetch_truth_by_actual_product: &HashMap<String, DirectFetchRuntimeInfo>,
    shared_context: Option<&dyn ProjectedMapProvider>,
    contour_layer_cache: &SharedContourLayerCache,
    barb_layer_cache: &SharedBarbLayerCache,
    barb_stride_cache: &SharedBarbStrideCache,
    projected_map_cache: &SharedProjectedMapCache,
    prepared_projected_maps: &PreparedProjectedMaps,
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
    let (
        project_ms,
        field_prepare_ms,
        contour_prepare_ms,
        barb_prepare_ms,
        request_build_ms,
        render_state_prep_ms,
        png_encode_ms,
        file_write_ms,
        state_timing,
        image_timing,
    ) = if let Some(spec) = composite_panel_spec(item.recipe.slug) {
        render_direct_composite_panel(
            item.recipe,
            spec.scaled_for_request(request),
            request,
            latest,
            extracted,
            &output_path,
            shared_context,
            contour_layer_cache,
            barb_layer_cache,
            barb_stride_cache,
            projected_map_cache,
            prepared_projected_maps,
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
        let visual_mode = visual_mode_for_direct_recipe(
            item.recipe,
            filled_selector,
            should_render_overlay_only(filled_selector, item.recipe.contours.is_some()),
        );
        let cache_key = (
            request.output_width,
            request.output_height,
            visual_mode_cache_key(visual_mode),
        );
        let projected = if let Some(projected) = shared_context.and_then(|ctx| {
            ctx.projected_map(request.output_width, request.output_height)
                .cloned()
        }) {
            projected
        } else if let Some(projected) = prepared_projected_maps.get(&cache_key).cloned() {
            projected
        } else if let Some(projected) = projected_map_cache
            .lock()
            .expect("projected map cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            projected
        } else {
            let projected = build_projected_map_with_projection(
                &filled.grid.lat_deg,
                &filled.grid.lon_deg,
                filled.projection.as_ref(),
                request.domain.bounds,
                map_frame_aspect_ratio_for_mode(
                    visual_mode,
                    request.output_width,
                    request.output_height,
                    true,
                    true,
                ),
            )?;
            projected_map_cache
                .lock()
                .expect("projected map cache poisoned")
                .insert(cache_key, projected.clone());
            projected
        };
        let project_ms = project_start.elapsed().as_millis();

        let request_build_start = Instant::now();
        let (mut render_request, build_timing) = build_render_request(
            item.recipe,
            filled,
            extracted,
            projected,
            request.domain.bounds,
            request.output_width,
            request.output_height,
            contour_layer_cache,
            barb_layer_cache,
            barb_stride_cache,
            request.contour_mode,
            request.native_fill_level_multiplier,
        )?;
        let request_build_ms = request_build_start.elapsed().as_millis();
        render_request.subtitle_left = Some(format!(
            "{} {}Z F{:03}  {}",
            request.date_yyyymmdd, latest.cycle.hour_utc, request.forecast_hour, request.model
        ));
        render_request.subtitle_right = Some(format!("source: {}", latest.source));
        let save_timing = save_png_profile_with_options(
            &render_request,
            &output_path,
            &request.png_write_options(),
        )?;
        (
            project_ms,
            build_timing.field_prepare_ms,
            build_timing.contour_prepare_ms,
            build_timing.barb_prepare_ms,
            request_build_ms,
            save_timing.state_timing.state_prep_ms,
            save_timing.png_timing.png_encode_ms,
            save_timing.file_write_ms,
            save_timing.state_timing,
            save_timing.png_timing.image_timing,
        )
    };
    let content_identity = artifact_identity_from_path(&output_path)?;
    let total_ms = render_start.elapsed().as_millis();

    let panel_compose_ms = if composite_panel_spec(item.recipe.slug).is_some() {
        image_timing.total_ms
    } else {
        0
    };

    Ok(DirectRenderedRecipe {
        recipe_slug: item.recipe.slug.to_string(),
        title: item.recipe.title.to_string(),
        source_route: direct_route_for_recipe_slug(item.recipe.slug),
        grib_product: item.plan.product.to_string(),
        fetched_grib_product: runtime_fetch.fetched_product.clone(),
        resolved_source: runtime_fetch.resolved_source,
        resolved_url: runtime_fetch.resolved_url.clone(),
        output_path,
        content_identity,
        input_fetch_keys: vec![runtime_fetch.fetch_key.clone()],
        timing: DirectRecipeTiming {
            render_to_image_ms: image_timing.total_ms,
            data_layer_draw_ms: direct_data_layer_draw_ms(&image_timing),
            overlay_draw_ms: direct_overlay_draw_ms(&image_timing),
            panel_compose_ms,
            project_ms,
            field_prepare_ms,
            contour_prepare_ms,
            barb_prepare_ms,
            request_build_ms,
            render_state_prep_ms,
            png_encode_ms,
            file_write_ms,
            render_ms: total_ms.saturating_sub(project_ms),
            total_ms,
            state_timing,
            image_timing,
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
    contour_layer_cache: &SharedContourLayerCache,
    barb_layer_cache: &SharedBarbLayerCache,
    barb_stride_cache: &SharedBarbStrideCache,
    projected_map_cache: &SharedProjectedMapCache,
    prepared_projected_maps: &PreparedProjectedMaps,
) -> Result<
    (
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        RenderStateTiming,
        RenderImageTiming,
    ),
    Box<dyn std::error::Error>,
> {
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
    let cache_key = (
        spec.panel_width,
        spec.panel_height,
        visual_mode_cache_key(ProductVisualMode::PanelMember),
    );
    let projected = if let Some(projected) = shared_context.and_then(|ctx| {
        ctx.projected_map(spec.panel_width, spec.panel_height)
            .cloned()
    }) {
        projected
    } else if let Some(projected) = prepared_projected_maps.get(&cache_key).cloned() {
        projected
    } else if let Some(projected) = projected_map_cache
        .lock()
        .expect("projected map cache poisoned")
        .get(&cache_key)
        .cloned()
    {
        projected
    } else {
        let projected = build_projected_map_with_projection(
            &first_field.grid.lat_deg,
            &first_field.grid.lon_deg,
            first_field.projection.as_ref(),
            request.domain.bounds,
            map_frame_aspect_ratio_for_mode(
                ProductVisualMode::PanelMember,
                spec.panel_width,
                spec.panel_height,
                true,
                true,
            ),
        )?;
        projected_map_cache
            .lock()
            .expect("projected map cache poisoned")
            .insert(cache_key, projected.clone());
        projected
    };
    let project_ms = project_start.elapsed().as_millis();

    let request_build_start = Instant::now();
    let mut build_timing = DirectRequestBuildTiming::default();
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
        let (mut panel_request, panel_timing) = build_render_request(
            component_recipe,
            filled,
            extracted,
            projected.clone(),
            request.domain.bounds,
            spec.panel_width,
            spec.panel_height,
            contour_layer_cache,
            barb_layer_cache,
            barb_stride_cache,
            request.contour_mode,
            request.native_fill_level_multiplier,
        )?;
        build_timing.field_prepare_ms += panel_timing.field_prepare_ms;
        build_timing.contour_prepare_ms += panel_timing.contour_prepare_ms;
        build_timing.barb_prepare_ms += panel_timing.barb_prepare_ms;
        panel_request.width = spec.panel_width;
        panel_request.height = spec.panel_height;
        panel_request.visual_mode = ProductVisualMode::PanelMember;
        panel_request.subtitle_left = None;
        panel_request.subtitle_right = None;
        panel_requests.push(panel_request);
    }
    let request_build_ms = request_build_start.elapsed().as_millis();

    let layout =
        PanelGridLayout::new(spec.rows, spec.columns, spec.panel_width, spec.panel_height)?
            .with_padding(PanelPadding {
                top: spec.top_padding,
                ..Default::default()
            });
    let render_start = Instant::now();
    let mut canvas = render_panel_grid(&layout, &panel_requests)?;
    let render_ms = render_start.elapsed().as_millis();
    draw_centered_text_line(&mut canvas, recipe.title, 10, Color::BLACK, 2);
    draw_centered_text_line(
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
        Color::BLACK,
        1,
    );
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let save_timing =
        save_rgba_png_profile_with_options(&canvas, output_path, &request.png_write_options())?;
    Ok((
        project_ms,
        build_timing.field_prepare_ms,
        build_timing.contour_prepare_ms,
        build_timing.barb_prepare_ms,
        request_build_ms,
        save_timing.state_timing.state_prep_ms,
        save_timing.png_timing.png_encode_ms,
        save_timing.file_write_ms,
        save_timing.state_timing,
        RenderImageTiming {
            total_ms: render_ms,
            ..RenderImageTiming::default()
        },
    ))
}

fn build_render_request(
    recipe: &PlotRecipe,
    filled: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    projected: ProjectedMap,
    bounds: (f64, f64, f64, f64),
    output_width: u32,
    output_height: u32,
    contour_layer_cache: &SharedContourLayerCache,
    barb_layer_cache: &SharedBarbLayerCache,
    barb_stride_cache: &SharedBarbStrideCache,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<(MapRenderRequest, DirectRequestBuildTiming), Box<dyn std::error::Error>> {
    let mut timing = DirectRequestBuildTiming::default();
    let field_prepare_start = Instant::now();
    let filled_field = render_filled_field(recipe, filled, extracted)?;
    timing.field_prepare_ms = field_prepare_start.elapsed().as_millis();
    let overlay_only = should_render_overlay_only(filled.selector, recipe.contours.is_some());
    let visual_mode = visual_mode_for_direct_recipe(recipe, filled.selector, overlay_only);
    let mut request = if overlay_only {
        let mut request = MapRenderRequest::contour_only(filled_field.clone().into());
        let contour_prepare_start = Instant::now();
        if let Some(layer) =
            cached_contour_layer(filled.selector, &filled.values, contour_layer_cache)
        {
            request.contours.push(layer);
        }
        timing.contour_prepare_ms += contour_prepare_start.elapsed().as_millis();
        request
    } else {
        MapRenderRequest::new(
            filled_field.clone().into(),
            scale_for_recipe(recipe, filled.selector),
        )
    };
    request.visual_mode = visual_mode;
    request.title = Some(recipe.title.to_string());
    request.width = output_width;
    request.height = output_height;
    request.supersample_factor = 2;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x,
        y: projected.projected_y,
        extent: projected.extent,
    });
    request.projected_lines = projected.lines;
    request.projected_polygons = projected.polygons;
    let contour_prepare_start = Instant::now();
    if overlay_only {
        request
            .contours
            .extend(build_contour_layers(recipe, extracted, contour_layer_cache));
    } else {
        request.contours = build_contour_layers(recipe, extracted, contour_layer_cache);
    }
    timing.contour_prepare_ms += contour_prepare_start.elapsed().as_millis();
    let barb_prepare_start = Instant::now();
    request.wind_barbs = build_barb_layers(
        recipe,
        extracted,
        bounds,
        barb_layer_cache,
        barb_stride_cache,
    );
    timing.barb_prepare_ms = barb_prepare_start.elapsed().as_millis();
    if !overlay_only {
        let contour_fill_start = Instant::now();
        maybe_apply_experimental_projected_contours(
            recipe,
            &mut request,
            contour_mode,
            native_fill_level_multiplier,
        )?;
        timing.contour_prepare_ms += contour_fill_start.elapsed().as_millis();
    }
    Ok((request, timing))
}

fn maybe_apply_experimental_projected_contours(
    recipe: &PlotRecipe,
    request: &mut MapRenderRequest,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let enabled = match contour_mode {
        NativeContourRenderMode::Automatic | NativeContourRenderMode::LegacyRaster => false,
        NativeContourRenderMode::ExperimentalAllProjected => true,
        NativeContourRenderMode::Signature => signature_contour_direct_recipe_enabled(recipe),
    };
    if !enabled {
        return Ok(());
    }
    let Some(projected_domain) = request.projected_domain.as_ref() else {
        return Ok(());
    };
    request.scale =
        densify_direct_native_contour_scale(request.scale.clone(), native_fill_level_multiplier);
    let (geometry, _) = build_projected_contour_geometry_profile(
        &request.field,
        projected_domain,
        &request.scale,
        &[],
        ProjectedContourLineStyle::default(),
    )?;
    request.projected_data_polygons.extend(geometry.fills);
    request.projected_lines.extend(geometry.lines);
    request.field.values.fill(f32::NAN);
    Ok(())
}

fn signature_contour_direct_recipe_enabled(recipe: &PlotRecipe) -> bool {
    matches!(
        recipe.slug,
        "mslp_10m_winds"
            | "200mb_height_winds"
            | "200mb_absolute_vorticity_height_winds"
            | "300mb_temperature_height_winds"
            | "700mb_height_winds"
    )
}

fn densify_direct_native_contour_scale(
    scale: ColorScale,
    native_fill_level_multiplier: usize,
) -> ColorScale {
    if native_fill_level_multiplier <= 1 {
        return scale;
    }
    let discrete = scale.resolved_discrete();
    ColorScale::Discrete(densify_discrete_scale(
        &discrete,
        LevelDensity {
            multiplier: native_fill_level_multiplier,
            min_source_level_count: 2,
        },
    ))
}

fn visual_mode_for_direct_recipe(
    recipe: &PlotRecipe,
    selector: FieldSelector,
    overlay_only: bool,
) -> ProductVisualMode {
    if overlay_only {
        return ProductVisualMode::OverlayAnalysis;
    }

    if matches!(recipe.style, RenderStyle::Solar07Height)
        || matches!(selector.vertical, VerticalSelector::IsobaricHpa(_))
    {
        return ProductVisualMode::UpperAirAnalysis;
    }

    let slug = recipe.slug.to_ascii_lowercase();
    if [
        "cape", "cin", "stp", "scp", "ehi", "srh", "shear", "lapse", "uh", "helicity",
    ]
    .iter()
    .any(|token| slug.contains(token))
    {
        return ProductVisualMode::SevereDiagnostic;
    }

    ProductVisualMode::FilledMeteorology
}

fn render_filled_field(
    recipe: &PlotRecipe,
    field: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> Result<rustwx_core::Field2D, Box<dyn std::error::Error>> {
    if let Some(wind_speed) = derived_height_winds_fill(recipe, field, extracted)? {
        return Ok(wind_speed);
    }
    Ok(convert_filled_field(recipe, field))
}

fn derived_height_winds_fill(
    recipe: &PlotRecipe,
    field: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> Result<Option<rustwx_core::Field2D>, Box<dyn std::error::Error>> {
    if recipe.style != RenderStyle::Solar07Height
        || field.selector.field != CanonicalField::GeopotentialHeight
    {
        return Ok(None);
    }

    let (Some(u_spec), Some(v_spec)) = (&recipe.barbs_u, &recipe.barbs_v) else {
        return Ok(None);
    };
    let (Some(u_selector), Some(v_selector)) = (u_spec.selector, v_spec.selector) else {
        return Ok(None);
    };
    let (Some(u), Some(v)) = (extracted.get(&u_selector), extracted.get(&v_selector)) else {
        return Ok(None);
    };

    let values: Vec<f32> = u
        .values
        .iter()
        .zip(&v.values)
        .map(|(u_value, v_value)| {
            let speed_ms = ((*u_value as f64).powi(2) + (*v_value as f64).powi(2)).sqrt();
            (speed_ms * 1.943_844_5) as f32
        })
        .collect();

    let field = rustwx_core::Field2D::new(
        rustwx_core::ProductKey::named(format!("{}_wind_speed", recipe.slug)),
        "kt",
        u.grid.clone(),
        values,
    )?;
    Ok(Some(field))
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
                // Mask clear-air cells (<5 dBZ) so the basemap shows through —
                // matches how NWS/NOAA radar products render.
                mask_below: Some(5.0),
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
        RenderStyle::Solar07Height => DiscreteColorScale {
            levels: match filled_selector.vertical {
                rustwx_core::VerticalSelector::IsobaricHpa(200)
                | rustwx_core::VerticalSelector::IsobaricHpa(300) => range_step(50.0, 170.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(500) => range_step(20.0, 150.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(700) => range_step(10.0, 90.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(850) => range_step(10.0, 70.0, 5.0),
                _ => range_step(10.0, 120.0, 5.0),
            },
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
            // Reveal basemap where no precipitation has accumulated.
            mask_below: Some(0.25),
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
            // Pragmatic near-zero cutoff (units: flashes km^-2 day^-1) so
            // cells with no meaningful flash activity reveal basemap. Not
            // an NWS operational threshold — unlike reflectivity's 5 dBZ
            // minimum detectable or QPF's 0.01 in trace, there's no
            // standard display cutoff for lightning flash density. Also
            // note: lightning_flash_density is currently blocked as a
            // native recipe (HRRR exposes LTNGSD/LTNG, not the flash-
            // density parameters), so this scale isn't hit in practice
            // today; the value matches the scale's level step for
            // consistency with how the first bin is drawn.
            mask_below: Some(0.5),
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
    contour_layer_cache: &SharedContourLayerCache,
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

    cached_contour_layer(selector, &field.values, contour_layer_cache)
        .into_iter()
        .collect()
}

fn cached_contour_layer(
    selector: FieldSelector,
    values: &[f32],
    contour_layer_cache: &SharedContourLayerCache,
) -> Option<ContourLayer> {
    {
        let cache = contour_layer_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(layer) = cache.get(&selector) {
            return layer.clone();
        }
    }

    let layer = contour_layer_for_values(selector, values);
    let mut cache = contour_layer_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache
        .entry(selector)
        .or_insert_with(|| layer.clone())
        .clone()
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
    barb_layer_cache: &SharedBarbLayerCache,
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
        let cache = barb_layer_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(layers) = cache.get(&key) {
            return layers.clone();
        }
    }

    let (stride_x, stride_y) =
        cached_barb_strides(u_selector, v_selector, &u.grid, bounds, barb_stride_cache);
    let layers = vec![WindBarbLayer {
        u: u.values.iter().map(|value| value * 1.943_844_5).collect(),
        v: v.values.iter().map(|value| value * 1.943_844_5).collect(),
        stride_x,
        stride_y,
        color: Color::BLACK,
        width: 1,
        length_px: 20.0,
    }];
    let mut cache = barb_layer_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.entry(key).or_insert_with(|| layers.clone()).clone()
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

pub fn build_projected_map(
    lat_deg: &[f32],
    lon_deg: &[f32],
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    rustwx_render::build_projected_map(lat_deg, lon_deg, bounds, target_ratio)
}

pub fn build_projected_map_with_projection(
    lat_deg: &[f32],
    lon_deg: &[f32],
    projection: Option<&rustwx_core::GridProjection>,
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    let mut options = rustwx_render::ProjectedMapBuildOptions::from_bounds(bounds, target_ratio);
    if let Some(projection) = projection.cloned() {
        options = options.with_projection(projection);
    }
    rustwx_render::build_projected_map_with_options(lat_deg, lon_deg, &options)
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

    #[test]
    fn partition_blocks_recipe_whose_filled_selector_is_missing() {
        // Partial-success regression: direct_batch used to crash the
        // whole batch on the first missing GRIB message (GFS f000
        // missing APCP@Surface, ECMWF f000 missing RH@2m_agl). Now a
        // missing selector produces a per-recipe blocker and the rest
        // of the recipes still render.
        let rh_recipe = plot_recipe("2m_relative_humidity").expect("2m RH recipe should exist");
        let tmp_recipe = plot_recipe("2m_temperature").expect("2m temperature recipe should exist");

        let planned = vec![
            PlannedDirectRecipe {
                recipe: rh_recipe,
                plan: plot_recipe_fetch_plan(rh_recipe.slug, ModelId::Hrrr).unwrap(),
            },
            PlannedDirectRecipe {
                recipe: tmp_recipe,
                plan: plot_recipe_fetch_plan(tmp_recipe.slug, ModelId::Hrrr).unwrap(),
            },
        ];
        let mut missing = HashSet::new();
        missing.insert(
            rh_recipe
                .filled
                .selector
                .expect("2m RH recipe has a filled selector"),
        );

        let (renderable, blockers) = partition_recipes_by_selector_availability(&planned, &missing);
        assert_eq!(renderable.len(), 1);
        assert_eq!(renderable[0].recipe.slug, tmp_recipe.slug);
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].recipe_slug, rh_recipe.slug);
        assert!(
            blockers[0].reason.contains("filled selector"),
            "blocker reason should mention the missing filled selector; got: {}",
            blockers[0].reason
        );
    }

    #[test]
    fn empty_renderable_batch_returns_without_projected_map_failure() {
        let request = sample_direct_request(ModelId::Hrrr);
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Nomads,
        };

        let rendered = render_direct_recipes(
            &request,
            &latest,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .expect("empty renderable batches should not fail projected-map prep");

        assert!(rendered.is_empty());
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
            contour_mode: NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: OUTPUT_WIDTH,
            output_height: OUTPUT_HEIGHT,
            png_compression: PngCompressionMode::Default,
        }
    }

    #[test]
    fn signature_contour_direct_recipe_list_is_curated() {
        let mslp_recipe = plot_recipe("mslp_10m_winds").unwrap();
        let temperature_recipe = plot_recipe("2m_temperature").unwrap();
        assert!(signature_contour_direct_recipe_enabled(mslp_recipe));
        assert!(!signature_contour_direct_recipe_enabled(temperature_recipe));
    }

    /// Test-only equivalent of the legacy `build_direct_fetch_request`
    /// helper. Tests still want to assert that direct's fetch identity
    /// stays consistent across HRRR's nat→sfc routing and product
    /// overrides; the production path now builds requests inside the
    /// loader, but the same routing logic lives in the planner so this
    /// thin shim stays honest.
    fn build_direct_fetch_request(
        request: &DirectBatchRequest,
        latest: &LatestRun,
        forecast_hour: u16,
        group: &FetchGroup,
    ) -> Result<rustwx_io::FetchRequest, rustwx_core::RustwxError> {
        Ok(rustwx_io::FetchRequest {
            request: rustwx_core::ModelRunRequest::new(
                request.model,
                latest.cycle.clone(),
                forecast_hour,
                group.product.as_str(),
            )?,
            source_override: Some(latest.source),
            variable_patterns: Vec::new(),
        })
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
    fn grouping_preserves_logical_family_aliases_when_nat_reroutes_to_sfc() {
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "composite_reflectivity".to_string(),
                "2m_temperature_10m_winds".to_string(),
            ],
        )
        .unwrap();
        let request = sample_direct_request(ModelId::Hrrr);
        let groups = group_direct_fetches(&request, &planned);
        // Both recipes share the canonical sfc fetch, but the logical
        // planning recorded "nat" for composite_reflectivity; the alias
        // set must retain both "nat" and "sfc" for provenance.
        let sfc_group = groups
            .iter()
            .find(|group| group.product == "sfc")
            .expect("expected a canonical sfc fetch group");
        assert!(sfc_group.planned_family_aliases.contains("nat"));
        assert!(sfc_group.planned_family_aliases.contains("sfc"));
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
            planned_family_aliases: std::collections::BTreeSet::from(["prs".to_string()]),
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
            planned_family_aliases: std::collections::BTreeSet::from(["nat".to_string()]),
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
            planned_family_aliases: std::collections::BTreeSet::from([planned_product.to_string()]),
        };
        let fetch = build_direct_fetch_request(&request, &latest, 6, &group).unwrap();
        let runtime = HrrrDirectFetchRuntimeInfo {
            fetch_key: crate::publication::fetch_key(planned_product, &fetch.request),
            planned_product: planned_product.into(),
            fetched_product: fetch.request.product.clone(),
            planned_family_aliases: vec![planned_product.into()],
            requested_source: fetch.source_override.unwrap(),
            resolved_source: SourceId::Nomads,
            resolved_url: "https://example.test/hrrr.t23z.wrfsfcf06.grib2".into(),
        };
        assert_eq!(runtime.planned_product, "nat");
        assert_eq!(runtime.fetched_product, "sfc");
        assert_eq!(runtime.planned_family_aliases, vec!["nat".to_string()]);
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
            planned_family_aliases: std::collections::BTreeSet::from(["prs-conus".to_string()]),
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

    #[test]
    fn height_winds_fill_uses_derived_wind_speed_in_knots() {
        let recipe = plot_recipe("500mb_height_winds").unwrap();
        let filled = sample_selected_field(
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
            "gpm",
            vec![540.0, 543.0, 546.0, 549.0],
        );
        let u = sample_selected_field(
            FieldSelector::isobaric(CanonicalField::UWind, 500),
            "m/s",
            vec![10.0, 0.0, 3.0, 4.0],
        );
        let v = sample_selected_field(
            FieldSelector::isobaric(CanonicalField::VWind, 500),
            "m/s",
            vec![0.0, 10.0, 4.0, 3.0],
        );
        let mut extracted = HashMap::new();
        extracted.insert(filled.selector, filled.clone());
        extracted.insert(u.selector, u);
        extracted.insert(v.selector, v);

        let render_field = render_filled_field(recipe, &filled, &extracted).unwrap();

        assert_eq!(render_field.units, "kt");
        assert_eq!(
            render_field.product.as_named(),
            Some("500mb_height_winds_wind_speed")
        );
        assert!((render_field.values[0] - 19.438_445).abs() < 0.01);
        assert!((render_field.values[1] - 19.438_445).abs() < 0.01);
        assert!((render_field.values[2] - 9.719_223).abs() < 0.01);
        assert!((render_field.values[3] - 9.719_223).abs() < 0.01);
    }
}
