use crate::derived::NativeContourRenderMode;
use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, CanonicalField, CycleSpec, FieldProduct,
    FieldSelector, GridProjection, ModelId, SelectedField2D, SourceId, VerticalSelector,
};
use rustwx_io::{
    earth2_archive::{Earth2EnsembleSelector, Earth2EnsembleStat},
    extract_fields_partial_from_model_bytes_with_earth2_selector_at_forecast_hour,
    load_cached_selected_field, store_cached_selected_field,
};
use rustwx_models::{
    LatestRun, ModelError, PlotRecipe, PlotRecipeFetchMode, PlotRecipeFetchPlan, RenderStyle,
    latest_available_run_at_forecast_hour, plot_recipe, plot_recipe_fetch_plan,
};
use rustwx_render::{
    BasemapDetail, Color, ColorScale, ContourLayer, DiscreteColorScale, DomainFrame, ExtendMode,
    GeographicClipBounds, InverseRasterProjection, LevelDensity, MapRenderRequest, PanelGridLayout,
    PanelPadding, PngCompressionMode, PngWriteOptions, ProductVisualMode,
    ProjectedContourLineStyle, ProjectedDomain, ProjectedMap, ProjectedMapBuildOptions,
    RasterSampleMode, RenderImageTiming, RenderStateTiming, WindBarbLayer, WindStreamlineLayer,
    build_projected_contour_geometry_profile, densify_discrete_scale, draw_centered_text_line,
    render_panel_grid, save_png_profile_with_options, save_rgba_png_profile_with_options,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::Instant;

use crate::custom_poi::{CustomPoiOverlay, apply_custom_poi_overlay};
use crate::gridded::{GridCrop, crop_latlon_grid, crop_values_f32};
use crate::places::PlaceLabelOverlay;
use crate::planner::{ExecutionPlan, ExecutionPlanBuilder};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
    fetch_identity_from_cached_result_with_aliases,
};
use crate::runtime::{
    BundleLoaderConfig, FetchedBundleBytes, LoadedBundleSet, load_execution_plan,
};
use crate::shared_context::{
    DomainSpec, ProjectedMapProvider, model_time_subtitle, source_subtitle, static_chrome_scale,
    static_supersample_factor, static_supersample_sharpen, static_title_with_suffix,
};
use crate::source::{ProductSourceRoute, direct_route_for_recipe_slug};
use crate::spec::direct_product_specs;

const OUTPUT_WIDTH: u32 = 1600;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle_left_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle_right_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earth2_ensemble: Option<rustwx_io::earth2_archive::Earth2EnsembleSelector>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earth2_ensemble: Option<rustwx_io::earth2_archive::Earth2EnsembleSelector>,
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

#[derive(Debug, Clone)]
pub(crate) struct DirectSampledProductField {
    pub recipe_slug: String,
    pub title: String,
    pub source_route: ProductSourceRoute,
    pub field_selector: Option<FieldSelector>,
    pub field: rustwx_core::Field2D,
    pub input_fetches: Vec<PublishedFetchIdentity>,
}

#[derive(Debug, Clone)]
pub(crate) struct DirectSampledProductSet {
    pub latest: LatestRun,
    pub fields: Vec<DirectSampledProductField>,
    pub blockers: Vec<DirectRecipeBlocker>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedDirectBatch {
    latest: LatestRun,
    renderable: Vec<PlannedDirectRecipe>,
    extracted: HashMap<FieldSelector, SelectedField2D>,
    fetches: Vec<DirectFetchTiming>,
    fetch_truth_by_actual_product: HashMap<String, DirectFetchRuntimeInfo>,
    blockers: Vec<DirectRecipeBlocker>,
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
type SharedStreamlineLayerCache = Arc<Mutex<HashMap<BarbStrideCacheKey, Vec<WindStreamlineLayer>>>>;
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
            custom_poi_overlay: request.custom_poi_overlay.clone(),
            place_label_overlay: request.place_label_overlay.clone(),
            output_suffix: None,
            subtitle_left_override: None,
            subtitle_right_override: None,
            earth2_ensemble: None,
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

fn sampling_direct_request(
    model: ModelId,
    source: SourceId,
    forecast_hour: u16,
    cache_root: &std::path::Path,
    use_cache: bool,
) -> DirectBatchRequest {
    DirectBatchRequest {
        model,
        date_yyyymmdd: String::new(),
        cycle_override_utc: None,
        forecast_hour,
        source,
        domain: DomainSpec::new("sampling", (-180.0, 180.0, -90.0, 90.0)),
        out_dir: PathBuf::new(),
        cache_root: cache_root.to_path_buf(),
        use_cache,
        recipe_slugs: Vec::new(),
        product_overrides: HashMap::new(),
        contour_mode: NativeContourRenderMode::Automatic,
        native_fill_level_multiplier: 1,
        output_width: OUTPUT_WIDTH,
        output_height: OUTPUT_HEIGHT,
        png_compression: PngCompressionMode::Default,
        custom_poi_overlay: None,
        place_label_overlay: None,
        output_suffix: None,
        subtitle_left_override: None,
        subtitle_right_override: None,
        earth2_ensemble: None,
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

pub fn render_direct_recipe_from_selected_fields(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    recipe_slug: &str,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    fetched_product: impl Into<String>,
    resolved_url: impl Into<String>,
    fetch_key: impl Into<String>,
) -> Result<DirectRenderedRecipe, Box<dyn std::error::Error>> {
    let mut rendered = render_direct_recipes_from_selected_fields(
        request,
        latest,
        &[recipe_slug.to_string()],
        extracted,
        fetched_product,
        resolved_url,
        fetch_key,
    )?;
    rendered
        .pop()
        .ok_or_else(|| "direct recipe rendered no outputs".into())
}

pub fn render_direct_recipes_from_selected_fields(
    request: &DirectBatchRequest,
    latest: &LatestRun,
    recipe_slugs: &[String],
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    fetched_product: impl Into<String>,
    resolved_url: impl Into<String>,
    fetch_key: impl Into<String>,
) -> Result<Vec<DirectRenderedRecipe>, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    let planned = plan_direct_recipes(request.model, recipe_slugs)?;
    let groups = group_direct_fetches(request, &planned);
    let fetched_product = fetched_product.into();
    let resolved_url = resolved_url.into();
    let fetch_key = fetch_key.into();
    let mut fetch_truth_by_actual_product = HashMap::<String, DirectFetchRuntimeInfo>::new();
    for group in &groups {
        fetch_truth_by_actual_product.insert(
            group.product.clone(),
            DirectFetchRuntimeInfo {
                fetch_key: fetch_key.clone(),
                planned_product: group.product.clone(),
                fetched_product: fetched_product.clone(),
                planned_family_aliases: group.planned_family_aliases.iter().cloned().collect(),
                requested_source: request.source,
                resolved_source: latest.source,
                resolved_url: resolved_url.clone(),
                earth2_ensemble: request.earth2_ensemble,
            },
        );
    }

    let missing = planned
        .iter()
        .flat_map(|item| item.plan.selectors())
        .filter(|selector| !extracted.contains_key(selector))
        .collect::<HashSet<_>>();
    if !missing.is_empty() {
        return Err(format!("missing selected fields for direct render: {:?}", missing).into());
    }

    render_direct_recipes(
        request,
        latest,
        &planned,
        extracted,
        &fetch_truth_by_actual_product,
        None,
    )
}

pub fn run_hrrr_direct_batch(
    request: &HrrrDirectBatchRequest,
) -> Result<HrrrDirectBatchReport, Box<dyn std::error::Error>> {
    run_direct_batch(&DirectBatchRequest::from_hrrr(request))
}

pub(crate) fn required_direct_fetch_products(
    model: ModelId,
    recipe_slugs: &[String],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let planned = plan_direct_recipes(model, recipe_slugs)?;
    let request =
        sampling_direct_request(model, SourceId::Aws, 0, std::path::Path::new("."), false);
    Ok(group_direct_fetches(&request, &planned)
        .into_iter()
        .map(|group| group.product)
        .collect())
}

pub(crate) fn load_direct_sampled_fields_from_latest(
    latest: &LatestRun,
    forecast_hour: u16,
    cache_root: &std::path::Path,
    use_cache: bool,
    recipe_slugs: &[String],
) -> Result<DirectSampledProductSet, Box<dyn std::error::Error>> {
    let request = sampling_direct_request(
        latest.model,
        latest.source,
        forecast_hour,
        cache_root,
        use_cache,
    );
    let planned = plan_direct_recipes(latest.model, recipe_slugs)?;
    if planned.is_empty() {
        return Ok(DirectSampledProductSet {
            latest: latest.clone(),
            fields: Vec::new(),
            blockers: Vec::new(),
        });
    }

    let groups = group_direct_fetches(&request, &planned);
    let plan = build_direct_execution_plan(latest, forecast_hour, &groups);
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig::new(cache_root.to_path_buf(), use_cache)
            .with_earth2_ensemble(request.earth2_ensemble),
    )?;
    load_direct_sampled_fields_from_loaded_request(&request, &loaded, recipe_slugs)
}

pub(crate) fn build_direct_sampled_execution_plan(
    latest: &LatestRun,
    forecast_hour: u16,
    cache_root: &std::path::Path,
    use_cache: bool,
    recipe_slugs: &[String],
) -> Result<ExecutionPlan, Box<dyn std::error::Error>> {
    let request = sampling_direct_request(
        latest.model,
        latest.source,
        forecast_hour,
        cache_root,
        use_cache,
    );
    let planned = plan_direct_recipes(latest.model, recipe_slugs)?;
    let groups = group_direct_fetches(&request, &planned);
    Ok(build_direct_execution_plan(latest, forecast_hour, &groups))
}

pub(crate) fn load_direct_sampled_fields_from_loaded(
    latest: &LatestRun,
    forecast_hour: u16,
    cache_root: &std::path::Path,
    use_cache: bool,
    recipe_slugs: &[String],
    loaded: &LoadedBundleSet,
) -> Result<DirectSampledProductSet, Box<dyn std::error::Error>> {
    let request = sampling_direct_request(
        latest.model,
        latest.source,
        forecast_hour,
        cache_root,
        use_cache,
    );
    load_direct_sampled_fields_from_loaded_request(&request, loaded, recipe_slugs)
}

fn load_direct_sampled_fields_from_loaded_request(
    request: &DirectBatchRequest,
    loaded: &LoadedBundleSet,
    recipe_slugs: &[String],
) -> Result<DirectSampledProductSet, Box<dyn std::error::Error>> {
    let planned = plan_direct_recipes(request.model, recipe_slugs)?;
    if planned.is_empty() {
        return Ok(DirectSampledProductSet {
            latest: loaded.latest.clone(),
            fields: Vec::new(),
            blockers: Vec::new(),
        });
    }

    let groups = group_direct_fetches(request, &planned);

    let mut extracted = HashMap::<FieldSelector, SelectedField2D>::new();
    let mut missing_selectors = HashSet::<FieldSelector>::new();
    let mut blockers = Vec::<DirectRecipeBlocker>::new();
    let mut fetches_by_product = HashMap::<String, PublishedFetchIdentity>::new();

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
        fetches_by_product.insert(group.product.clone(), timing.input_fetch.clone());
    }

    let (renderable, selector_blockers) =
        partition_recipes_by_selector_availability(&planned, &missing_selectors);
    blockers.extend(selector_blockers);

    let mut fields = Vec::new();
    for item in renderable {
        if composite_panel_spec(item.recipe.slug).is_some() {
            blockers.push(DirectRecipeBlocker {
                recipe_slug: item.recipe.slug.to_string(),
                reason: "composite direct recipe does not expose a single sampled filled field"
                    .to_string(),
            });
            continue;
        }
        let Some(filled_selector) = item.recipe.filled.selector else {
            blockers.push(DirectRecipeBlocker {
                recipe_slug: item.recipe.slug.to_string(),
                reason: "direct recipe is missing a filled selector binding".to_string(),
            });
            continue;
        };
        let Some(filled) = extracted.get(&filled_selector) else {
            blockers.push(DirectRecipeBlocker {
                recipe_slug: item.recipe.slug.to_string(),
                reason: format!(
                    "direct recipe '{}' was renderable but missing selector {}",
                    item.recipe.slug,
                    filled_selector.key()
                ),
            });
            continue;
        };
        let field = render_filled_field(item.recipe, filled, &extracted)?;
        let canonical_product = canonical_fetch_product_for_selectors(
            &request,
            item.plan.product.as_ref(),
            &item.plan.selectors(),
        );
        let input_fetches = fetches_by_product
            .get(&canonical_product)
            .cloned()
            .into_iter()
            .collect();
        fields.push(DirectSampledProductField {
            recipe_slug: item.recipe.slug.to_string(),
            title: item.recipe.title.to_string(),
            source_route: direct_route_for_recipe_slug(item.recipe.slug),
            field_selector: Some(filled_selector),
            field,
            input_fetches,
        });
    }

    Ok(DirectSampledProductSet {
        latest: loaded.latest.clone(),
        fields,
        blockers,
    })
}

pub(crate) fn load_single_direct_sampled_field_from_latest(
    latest: &LatestRun,
    forecast_hour: u16,
    cache_root: &std::path::Path,
    use_cache: bool,
    recipe_slug: &str,
    allow_composite_filled_field: bool,
) -> Result<DirectSampledProductField, Box<dyn std::error::Error>> {
    let request = sampling_direct_request(
        latest.model,
        latest.source,
        forecast_hour,
        cache_root,
        use_cache,
    );
    let planned = plan_direct_recipes(latest.model, &[recipe_slug.to_string()])?;
    let planned_item = planned
        .first()
        .ok_or_else(|| format!("direct recipe '{recipe_slug}' did not plan"))?;

    let groups = group_direct_fetches(&request, &planned);
    let plan = build_direct_execution_plan(latest, forecast_hour, &groups);
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig::new(cache_root.to_path_buf(), use_cache)
            .with_earth2_ensemble(request.earth2_ensemble),
    )?;

    let mut extracted = HashMap::<FieldSelector, SelectedField2D>::new();
    let mut missing_selectors = HashSet::<FieldSelector>::new();
    let mut fetches_by_product = HashMap::<String, PublishedFetchIdentity>::new();

    for group in &groups {
        let fetched = match find_loaded_bytes_for_group(&loaded, group) {
            Ok(bytes) => bytes,
            Err(err) => {
                for selector in &group.selectors {
                    missing_selectors.insert(*selector);
                }
                return Err(format!(
                    "direct recipe '{}' fetch group '{}' failed: {}",
                    recipe_slug, group.product, err
                )
                .into());
            }
        };
        let (fields, unmatched, timing) =
            extract_direct_fetch_group_from_loaded(&request, group, fetched, use_cache)?;
        extracted.extend(fields.into_iter().map(|field| (field.selector, field)));
        for selector in unmatched {
            missing_selectors.insert(selector);
        }
        fetches_by_product.insert(group.product.clone(), timing.input_fetch.clone());
    }

    if let Some(reason) = recipe_block_reason(planned_item.recipe, &missing_selectors) {
        return Err(format!(
            "direct sampled field '{}' is blocked: {}",
            recipe_slug, reason
        )
        .into());
    }

    if composite_panel_spec(planned_item.recipe.slug).is_some() && !allow_composite_filled_field {
        return Err(format!(
            "direct recipe '{}' is composite and does not expose a single sampled filled field by default",
            recipe_slug
        )
        .into());
    }

    let Some(filled_selector) = planned_item.recipe.filled.selector else {
        return Err(format!(
            "direct recipe '{}' is missing a filled selector binding",
            recipe_slug
        )
        .into());
    };
    let Some(filled) = extracted.get(&filled_selector) else {
        return Err(format!(
            "direct recipe '{}' did not resolve filled selector {}",
            recipe_slug,
            filled_selector.key()
        )
        .into());
    };
    let field = render_filled_field(planned_item.recipe, filled, &extracted)?;
    let canonical_product = canonical_fetch_product_for_selectors(
        &request,
        planned_item.plan.product.as_ref(),
        &planned_item.plan.selectors(),
    );
    let input_fetches = fetches_by_product
        .get(&canonical_product)
        .cloned()
        .into_iter()
        .collect();
    Ok(DirectSampledProductField {
        recipe_slug: planned_item.recipe.slug.to_string(),
        title: planned_item.recipe.title.to_string(),
        source_route: direct_route_for_recipe_slug(planned_item.recipe.slug),
        field_selector: Some(filled_selector),
        field,
        input_fetches,
    })
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
    let total_start = Instant::now();
    let prepared = prepare_direct_batch_from_loaded(request, loaded, cache_root, use_cache)?;
    run_direct_batch_from_prepared_with_total_start(request, &prepared, shared_context, total_start)
}

pub(crate) fn prepare_direct_batch_from_loaded(
    request: &DirectBatchRequest,
    loaded: &LoadedBundleSet,
    cache_root: &std::path::Path,
    use_cache: bool,
) -> Result<PreparedDirectBatch, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if use_cache {
        fs::create_dir_all(cache_root)?;
    }
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
            match extract_direct_fetch_group_from_loaded(request, group, fetched, use_cache) {
                Ok(result) => result,
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

    Ok(PreparedDirectBatch {
        latest: loaded.latest.clone(),
        renderable,
        extracted,
        fetches,
        fetch_truth_by_actual_product,
        blockers,
    })
}

pub(crate) fn run_direct_batch_from_prepared(
    request: &DirectBatchRequest,
    prepared: &PreparedDirectBatch,
    shared_context: Option<&dyn ProjectedMapProvider>,
) -> Result<DirectBatchReport, Box<dyn std::error::Error>> {
    run_direct_batch_from_prepared_with_total_start(
        request,
        prepared,
        shared_context,
        Instant::now(),
    )
}

fn run_direct_batch_from_prepared_with_total_start(
    request: &DirectBatchRequest,
    prepared: &PreparedDirectBatch,
    shared_context: Option<&dyn ProjectedMapProvider>,
    total_start: Instant,
) -> Result<DirectBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;

    let rendered = render_direct_recipes(
        request,
        &prepared.latest,
        &prepared.renderable,
        &prepared.extracted,
        &prepared.fetch_truth_by_actual_product,
        shared_context,
    )?;

    Ok(DirectBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: prepared.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: prepared.latest.source,
        domain: request.domain.clone(),
        fetches: prepared.fetches.clone(),
        recipes: rendered,
        blockers: prepared.blockers.clone(),
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
            earth2_ensemble: request.earth2_ensemble,
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
        let (fields, unmatched, timing) = match extract_direct_fetch_group_from_loaded(
            request,
            group,
            fetched,
            request.use_cache,
        ) {
            Ok(result) => result,
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
        .filter(|spec| !direct_recipe_requires_explicit_opt_in(&spec.slug))
        .filter(|spec| plot_recipe_fetch_plan(&spec.slug, model).is_ok())
        .map(|spec| spec.slug)
        .collect()
}

fn direct_recipe_requires_explicit_opt_in(slug: &str) -> bool {
    slug.starts_with("nbm_qmd_")
        || slug.starts_with("sref_prob_")
        || slug.starts_with("gefs_avg_")
        || slug.starts_with("gefs_spr_")
        || slug.starts_with("aigefs_spr_")
        || slug.starts_with("hgefs_spr_")
        || slug.starts_with("href_sprd_")
        || slug.starts_with("href_prob_")
        || slug.starts_with("href_mean_")
        || slug.starts_with("refs_sprd_")
        || slug.starts_with("refs_prob_")
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
        let selectors = item.plan.selectors();
        let key =
            canonical_fetch_product_for_selectors(request, planned_family.as_str(), &selectors);
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
        for selector in selectors {
            if !entry.selectors.contains(&selector) {
                entry.selectors.push(selector);
            }
        }
        for (product, selector) in
            extra_direct_selectors(request, item.plan.product.as_ref(), item.recipe)
        {
            let extra_key = canonical_fetch_product_for_selectors(request, &product, &[selector]);
            let extra_entry = grouped
                .entry(extra_key.clone())
                .or_insert_with(|| FetchGroup {
                    product: extra_key.clone(),
                    fetch_mode: PlotRecipeFetchMode::WholeFileStructuredExtract,
                    variable_patterns: Vec::new(),
                    selectors: Vec::new(),
                    planned_family_aliases: std::collections::BTreeSet::new(),
                });
            extra_entry.planned_family_aliases.insert(product);
            if !extra_entry.selectors.contains(&selector) {
                extra_entry.selectors.push(selector);
            }
        }
    }
    let mut groups = grouped.into_values().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.product.cmp(&right.product));
    groups
}

fn extra_direct_selectors(
    request: &DirectBatchRequest,
    planned_product: &str,
    recipe: &PlotRecipe,
) -> Vec<(String, FieldSelector)> {
    if request.model == ModelId::WrfGdex {
        if let Some(FieldSelector {
            vertical: VerticalSelector::IsobaricHpa(_),
            ..
        }) = recipe.filled.selector
        {
            return vec![(
                wrf_gdex_surface_pressure_product(request, planned_product),
                FieldSelector::surface(CanonicalField::Pressure),
            )];
        }
    }
    Vec::new()
}

fn canonical_fetch_product(request: &DirectBatchRequest, planned_product: &str) -> String {
    canonical_fetch_product_for_selectors(request, planned_product, &[])
}

fn wrf_gdex_surface_pressure_product(
    request: &DirectBatchRequest,
    planned_product: &str,
) -> String {
    let product = canonical_fetch_product(request, planned_product);
    let normalized = product.replace('_', "-").to_ascii_lowercase();
    let Some((dataset, suffix)) = normalized.split_once('-') else {
        return product;
    };
    if !is_gdex_dataset_token(dataset) {
        return product;
    }
    match suffix {
        "hist3d" => format!("{dataset}-hist2d"),
        "future3d" => format!("{dataset}-future2d"),
        _ => product,
    }
}

fn canonical_fetch_product_for_selectors(
    request: &DirectBatchRequest,
    planned_product: &str,
    selectors: &[FieldSelector],
) -> String {
    if let Some(overridden) = request.product_overrides.get(planned_product) {
        return overridden.clone();
    }

    match (request.model, planned_product) {
        (ModelId::Hrrr, "nat") if hrrr_native_selectors_require_wrfnat(selectors) => {
            "nat".to_string()
        }
        (ModelId::Hrrr, "nat") => "sfc".to_string(),
        _ => planned_product.to_string(),
    }
}

fn hrrr_native_selectors_require_wrfnat(selectors: &[FieldSelector]) -> bool {
    selectors.iter().any(|selector| {
        matches!(
            selector.field,
            CanonicalField::SmokeMassDensity | CanonicalField::ColumnIntegratedSmoke
        )
    })
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
            if should_attach_direct_idx_patterns(latest.source) {
                builder.require_with_logical_family_and_patterns(
                    &requirement,
                    Some(alias),
                    group.variable_patterns.clone(),
                );
            } else {
                builder.require_with_logical_family(&requirement, Some(alias));
            }
        }
    }
    builder.build()
}

fn should_attach_direct_idx_patterns(source: SourceId) -> bool {
    matches!(source, SourceId::Aws | SourceId::Google)
}

fn dataset_token_from_product(product: &str) -> Option<&str> {
    let token = product.split(['-', '_']).next().unwrap_or(product);
    if is_gdex_dataset_token(token) {
        Some(token)
    } else {
        None
    }
}

fn is_gdex_dataset_token(token: &str) -> bool {
    token.len() > 1 && token.starts_with('d') && token[1..].chars().all(|ch| ch.is_ascii_digit())
}

fn native_stat_label_from_product(product: &str) -> Option<String> {
    let token = product
        .rsplit('/')
        .next()
        .unwrap_or(product)
        .trim()
        .to_ascii_lowercase();
    let mut token = token
        .strip_suffix("_3hrly")
        .or_else(|| token.strip_suffix("_hourly"))
        .or_else(|| token.strip_suffix("_1hrly"))
        .unwrap_or(token.as_str())
        .to_string();
    token = token.replace('-', "_");
    for suffix in ["_conus", "_ak", "_hi", "_pr"] {
        if let Some(stripped) = token.strip_suffix(suffix) {
            token = stripped.to_string();
            break;
        }
    }
    let label = match token.as_str() {
        "mean" => "Mean".to_string(),
        "avg" | "avrg" => "Average".to_string(),
        "spread" | "sprd" => "Spread".to_string(),
        "std" | "stddev" | "stdev" => "Std Dev".to_string(),
        "min" | "minimum" => "Min".to_string(),
        "max" | "maximum" => "Max".to_string(),
        "prob" | "probability" => "Probability".to_string(),
        "eas" => "EAS".to_string(),
        "lpmm" => "Localized PMM".to_string(),
        "pmmn" | "pmm" => "Probability-Matched Mean".to_string(),
        "ffri" => "FFRI".to_string(),
        value if value.len() >= 2 && value.starts_with('p') => {
            let digits = &value[1..];
            if digits.chars().all(|ch| ch.is_ascii_digit()) {
                value.to_ascii_uppercase()
            } else {
                return None;
            }
        }
        _ => return None,
    };
    Some(label)
}

fn native_stat_label_for_request(
    request: &DirectBatchRequest,
    planned_product: Option<&str>,
) -> Option<String> {
    planned_product
        .and_then(|planned| {
            request
                .product_overrides
                .get(planned)
                .and_then(|product| native_stat_label_from_product(product))
                .or_else(|| native_stat_label_from_product(planned))
        })
        .or_else(|| {
            request
                .product_overrides
                .values()
                .find_map(|product| native_stat_label_from_product(product))
        })
}

fn model_title_prefix(model: ModelId) -> String {
    model.as_str().replace('-', " ").to_ascii_uppercase()
}

fn apply_native_stat_title_prefix(model: ModelId, stat_label: &str, base_title: &str) -> String {
    let model_label = model_title_prefix(model);
    let stat_prefix = format!("{model_label} {stat_label} ");
    if base_title.starts_with(&stat_prefix) {
        return base_title.to_string();
    }
    let model_prefix = format!("{model_label} ");
    if let Some(without_model) = base_title.strip_prefix(&model_prefix) {
        return format!("{model_label} {stat_label} {without_model}");
    }
    format!("{model_label} {stat_label} {base_title}")
}

fn direct_title_for_request(
    request: &DirectBatchRequest,
    planned_product: Option<&str>,
    base_title: &str,
) -> String {
    let mut title = base_title.to_string();
    if request.model == ModelId::Aifs {
        if let Some(selector) = request.earth2_ensemble {
            title = format!("{title} ({})", selector.label());
        }
    }
    if let Some(stat_label) = native_stat_label_for_request(request, planned_product) {
        title = apply_native_stat_title_prefix(request.model, &stat_label, &title);
    }
    if request.model != ModelId::WrfGdex {
        return static_title_with_suffix(title);
    }

    let dataset = planned_product
        .and_then(|planned| {
            request
                .product_overrides
                .get(planned)
                .and_then(|product| dataset_token_from_product(product))
                .or_else(|| dataset_token_from_product(planned))
        })
        .or_else(|| {
            request
                .product_overrides
                .values()
                .find_map(|product| dataset_token_from_product(product))
        })
        .unwrap_or("d612005");
    static_title_with_suffix(format!("{title} ({dataset})"))
}

fn direct_title_for_planned_product(
    request: &DirectBatchRequest,
    planned_product: &str,
    base_title: &str,
) -> String {
    direct_title_for_request(request, Some(planned_product), base_title)
}

fn direct_panel_title_for_request(request: &DirectBatchRequest, base_title: &str) -> String {
    direct_title_for_request(request, None, base_title)
}

fn find_loaded_bytes_for_group<'a>(
    loaded: &'a LoadedBundleSet,
    group: &FetchGroup,
) -> Result<&'a FetchedBundleBytes, Box<dyn std::error::Error>> {
    if let Some(bundle) = loaded
        .fetched
        .values()
        .find(|bundle| bundle.key.native_product == group.product)
    {
        return Ok(bundle);
    }
    if let Some((key, reason)) = loaded
        .fetch_failures
        .iter()
        .find(|(key, _)| key.native_product == group.product)
    {
        return Err(format!(
            "direct fetch failed for canonical family '{}' from {:?}: {}",
            group.product, key.source, reason
        )
        .into());
    }
    Err(format!(
        "direct planner missed fetch for canonical family '{}'",
        group.product
    )
    .into())
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

    // Selectors whose GRIB message wasn't present in the file go here;
    // the caller uses them to mark dependent recipes as blockers
    // instead of the whole batch tripping on the first missing message.
    let mut unmatched = Vec::<FieldSelector>::new();
    let parse_start = Instant::now();
    if !missing.is_empty() {
        let partial =
            extract_fields_partial_from_model_bytes_with_earth2_selector_at_forecast_hour(
                fetch_request.request.model,
                &fetched.file.bytes,
                Some(cached_result.bytes_path.as_path()),
                &missing,
                fetch_request.earth2_ensemble,
                Some(fetch_request.request.forecast_hour),
            )?;
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
    let parse_ms = parse_start.elapsed().as_millis();
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
                earth2_ensemble: fetch_request.earth2_ensemble,
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

    let crop_bounds = crop_bounds_for_direct_request(request, planned, extracted);
    let domain_extracted = crop_direct_fields_for_domain(extracted, crop_bounds)?;
    let extracted = &domain_extracted;
    let contour_layer_cache = Arc::new(Mutex::new(HashMap::new()));
    let barb_layer_cache = Arc::new(Mutex::new(HashMap::new()));
    let streamline_layer_cache = Arc::new(Mutex::new(HashMap::new()));
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
                    &streamline_layer_cache,
                    &barb_stride_cache,
                    &projected_map_cache,
                    &prepared_projected_maps,
                )
            })
            .collect();
    }

    let next_index = AtomicUsize::new(0);
    let mut rendered = vec![None; planned.len()];

    thread::scope(|scope| -> Result<(), std::io::Error> {
        let mut handles = Vec::new();
        for _ in 0..worker_count {
            let barb_stride_cache = Arc::clone(&barb_stride_cache);
            let contour_layer_cache = Arc::clone(&contour_layer_cache);
            let barb_layer_cache = Arc::clone(&barb_layer_cache);
            let streamline_layer_cache = Arc::clone(&streamline_layer_cache);
            let projected_map_cache = Arc::clone(&projected_map_cache);
            let prepared_projected_maps = Arc::clone(&prepared_projected_maps);
            let next_index = &next_index;
            handles.push(scope.spawn(
                move || -> Result<Vec<(usize, DirectRenderedRecipe)>, std::io::Error> {
                    let mut worker_rendered = Vec::new();
                    loop {
                        let index = next_index.fetch_add(1, Ordering::Relaxed);
                        let Some(item) = planned.get(index) else {
                            break;
                        };
                        let rendered = render_direct_recipe(
                            request,
                            latest,
                            item,
                            extracted,
                            fetch_truth_by_actual_product,
                            shared_context,
                            &contour_layer_cache,
                            &barb_layer_cache,
                            &streamline_layer_cache,
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
                        worker_rendered.push((index, rendered));
                    }
                    Ok(worker_rendered)
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

fn crop_bounds_for_direct_request(
    request: &DirectBatchRequest,
    planned: &[PlannedDirectRecipe],
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> (f64, f64, f64, f64) {
    let Some((recipe, field)) = planned.iter().find_map(|item| {
        let selector = item.recipe.filled.selector?;
        extracted.get(&selector).map(|field| (item.recipe, field))
    }) else {
        return request.domain.bounds;
    };
    let overlay_only = should_render_overlay_only(field.selector, recipe.contours.is_some());
    let visual_mode = visual_mode_for_direct_recipe(recipe, field.selector, overlay_only);
    render_bounds_for_direct_field(
        request.domain.bounds,
        field,
        visual_mode,
        request.output_width,
        request.output_height,
    )
}

fn render_bounds_for_direct_field(
    bounds: (f64, f64, f64, f64),
    field: &SelectedField2D,
    visual_mode: ProductVisualMode,
    width: u32,
    height: u32,
) -> (f64, f64, f64, f64) {
    let target_ratio =
        direct_map_frame_aspect_ratio(visual_mode, width, height, field.projection.as_ref());
    presentation_frame_bounds_for_grid(
        field.projection.as_ref(),
        bounds,
        projection_presentation_variant(),
        target_ratio,
    )
}

fn direct_domain_crop_pad_cells_override() -> Option<usize> {
    std::env::var("RUSTWX_DOMAIN_CROP_PAD_CELLS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn direct_domain_crop_pad_cells_for_field(field: &SelectedField2D) -> usize {
    let base = direct_domain_crop_pad_cells_override().unwrap_or(6);
    if !matches!(field.projection.as_ref(), Some(GridProjection::Geographic)) {
        return base;
    }

    let variant = projection_presentation_variant();
    let pad_deg = std::env::var("RUSTWX_GEOGRAPHIC_DOMAIN_CROP_PAD_DEG")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(match variant {
            ProjectionPresentationVariant::PivotalLambert => PIVOTAL_GEOGRAPHIC_CROP_PAD_DEG,
            _ => 12.0,
        });
    let Some(spacing_deg) = estimate_geographic_grid_spacing_deg(&field.grid) else {
        return base.max(24);
    };
    let cells = (pad_deg / spacing_deg.max(1.0e-6)).ceil() as usize;
    let max_cells = match variant {
        ProjectionPresentationVariant::PivotalLambert => 128,
        _ => 96,
    };
    base.max(cells.clamp(12, max_cells))
}

fn crop_direct_fields_for_domain(
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    bounds: (f64, f64, f64, f64),
) -> Result<HashMap<FieldSelector, SelectedField2D>, Box<dyn std::error::Error>> {
    let mut cropped = HashMap::with_capacity(extracted.len());
    for (&selector, field) in extracted {
        let mut pad_cells = direct_domain_crop_pad_cells_for_field(field);
        let uses_inverse_raster =
            inverse_raster_projection_for_grid(field.projection.as_ref(), bounds, &field.grid)
                .is_some();
        if uses_inverse_raster {
            pad_cells = pad_cells.max(inverse_raster_crop_pad_cells());
        }
        let preserve_full_longitude_axis = uses_inverse_raster
            && matches!(field.projection.as_ref(), Some(GridProjection::Geographic))
            && grid_has_full_periodic_longitude_axis(&field.grid);
        cropped.insert(
            selector,
            crop_selected_field_for_domain(field, bounds, pad_cells, preserve_full_longitude_axis)?,
        );
    }
    Ok(cropped)
}

fn inverse_raster_crop_pad_cells() -> usize {
    std::env::var("RUSTWX_INVERSE_RASTER_CROP_PAD_CELLS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(1000)
}

fn estimate_geographic_grid_spacing_deg(grid: &rustwx_core::LatLonGrid) -> Option<f64> {
    let nx = grid.shape.nx;
    let ny = grid.shape.ny;
    if nx < 2 && ny < 2 {
        return None;
    }

    let mut best = f64::INFINITY;
    let row_candidates = [0usize, ny / 2, ny.saturating_sub(1)];
    for y in row_candidates {
        if y >= ny || nx < 2 {
            continue;
        }
        let offset = y * nx;
        for x in 0..nx - 1 {
            let a = grid.lon_deg[offset + x] as f64;
            let b = grid.lon_deg[offset + x + 1] as f64;
            let delta = longitude_delta_deg(a, b);
            if delta.is_finite() && delta > 0.0 && delta < best {
                best = delta;
            }
        }
    }

    let col_candidates = [0usize, nx / 2, nx.saturating_sub(1)];
    for x in col_candidates {
        if x >= nx || ny < 2 {
            continue;
        }
        for y in 0..ny - 1 {
            let a = grid.lat_deg[y * nx + x] as f64;
            let b = grid.lat_deg[(y + 1) * nx + x] as f64;
            let delta = (b - a).abs();
            if delta.is_finite() && delta > 0.0 && delta < best {
                best = delta;
            }
        }
    }

    best.is_finite().then_some(best)
}

fn grid_has_full_periodic_longitude_axis(grid: &rustwx_core::LatLonGrid) -> bool {
    let nx = grid.shape.nx;
    let ny = grid.shape.ny;
    if nx < 2 || ny == 0 || grid.lon_deg.len() < nx {
        return false;
    }

    let lon0 = grid.lon_deg[0] as f64;
    let lon1 = grid.lon_deg[1] as f64;
    let mut step = lon1 - lon0;
    if step > 180.0 {
        step -= 360.0;
    } else if step < -180.0 {
        step += 360.0;
    }
    let step = step.abs();
    if !step.is_finite() || step < 1.0e-9 {
        return false;
    }

    let tol = (step * 1.5).max(1.0e-6);
    ((nx as f64 * step) - 360.0).abs() <= tol || (((nx - 1) as f64 * step) - 360.0).abs() <= tol
}

fn longitude_delta_deg(a: f64, b: f64) -> f64 {
    let mut delta = (normalize_longitude_for_bounds(b) - normalize_longitude_for_bounds(a)).abs();
    if delta > 180.0 {
        delta = 360.0 - delta;
    }
    delta
}

fn crop_selected_field_for_domain(
    field: &SelectedField2D,
    bounds: (f64, f64, f64, f64),
    pad_cells: usize,
    preserve_full_longitude_axis: bool,
) -> Result<SelectedField2D, Box<dyn std::error::Error>> {
    let Some(crop) =
        crop_for_direct_grid(&field.grid, bounds, pad_cells, preserve_full_longitude_axis)?
    else {
        return Ok(field.clone());
    };
    let mut cropped = SelectedField2D::new(
        field.selector,
        field.units.clone(),
        crop_latlon_grid(&field.grid, crop)?,
        crop_values_f32(&field.values, field.grid.shape.nx, crop),
    )?;
    if let Some(projection) = field.projection.clone() {
        cropped = cropped.with_projection(projection);
    }
    Ok(cropped)
}

fn crop_for_direct_grid(
    grid: &rustwx_core::LatLonGrid,
    bounds: (f64, f64, f64, f64),
    pad_cells: usize,
    preserve_full_longitude_axis: bool,
) -> Result<Option<GridCrop>, Box<dyn std::error::Error>> {
    let nx = grid.shape.nx;
    let ny = grid.shape.ny;
    if nx == 0 || ny == 0 {
        return Ok(None);
    }

    let mut min_x = nx;
    let mut max_x = 0usize;
    let mut min_y = ny;
    let mut max_y = 0usize;
    let mut found = false;

    for y in 0..ny {
        let row_offset = y * nx;
        for x in 0..nx {
            let idx = row_offset + x;
            let lat = grid.lat_deg[idx] as f64;
            let lon = grid.lon_deg[idx] as f64;
            if point_in_geographic_bounds(lon, lat, bounds) {
                min_x = min_x.min(x);
                max_x = max_x.max(x);
                min_y = min_y.min(y);
                max_y = max_y.max(y);
                found = true;
            }
        }
    }

    if !found {
        return Ok(None);
    }

    let crop = GridCrop {
        x_start: if preserve_full_longitude_axis {
            0
        } else {
            min_x.saturating_sub(pad_cells)
        },
        x_end: if preserve_full_longitude_axis {
            nx
        } else {
            (max_x + 1 + pad_cells).min(nx)
        },
        y_start: min_y.saturating_sub(pad_cells),
        y_end: (max_y + 1 + pad_cells).min(ny),
    };

    if crop.x_start == 0 && crop.x_end == nx && crop.y_start == 0 && crop.y_end == ny {
        Ok(None)
    } else {
        Ok(Some(crop))
    }
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
            if point_in_geographic_bounds(lon, lat, bounds) {
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

fn point_in_geographic_bounds(lon: f64, lat: f64, bounds: (f64, f64, f64, f64)) -> bool {
    if !lon.is_finite() || !lat.is_finite() || lat < bounds.2 || lat > bounds.3 {
        return false;
    }
    if longitude_bounds_span_deg(bounds) >= 359.0 {
        return true;
    }
    let west = normalize_longitude_for_bounds(bounds.0);
    let east = normalize_longitude_for_bounds(bounds.1);
    let lon = normalize_longitude_for_bounds(lon);
    if west <= east {
        lon >= west && lon <= east
    } else {
        lon >= west || lon <= east
    }
}

fn normalize_longitude_for_bounds(lon: f64) -> f64 {
    let mut lon = lon % 360.0;
    if lon > 180.0 {
        lon -= 360.0;
    } else if lon <= -180.0 {
        lon += 360.0;
    }
    lon
}

pub(crate) fn is_global_scale_domain(bounds: (f64, f64, f64, f64)) -> bool {
    crate::plot_design::is_global_scale_domain(bounds)
}

fn longitude_bounds_span_deg(bounds: (f64, f64, f64, f64)) -> f64 {
    let raw_span = (bounds.1 - bounds.0).abs();
    if raw_span >= 359.0 {
        return raw_span.min(360.0);
    }

    let west = normalize_longitude_for_bounds(bounds.0);
    let east = normalize_longitude_for_bounds(bounds.1);
    if west <= east {
        east - west
    } else {
        east + 360.0 - west
    }
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
    let mut by_geometry = HashMap::<(u32, u32, u64), ProjectedMap>::new();
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
        let target_ratio = direct_map_frame_aspect_ratio(
            visual_mode,
            width,
            height,
            sample_field.projection.as_ref(),
        );
        let geometry_key = (width, height, target_ratio.to_bits());
        let projected = if let Some(projected) = by_geometry.get(&geometry_key) {
            projected.clone()
        } else {
            let projected = build_projected_map_with_projection(
                &sample_field.grid.lat_deg,
                &sample_field.grid.lon_deg,
                sample_field.projection.as_ref(),
                request.domain.bounds,
                target_ratio,
            )?;
            by_geometry.insert(geometry_key, projected.clone());
            projected
        };
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
    streamline_layer_cache: &SharedStreamlineLayerCache,
    barb_stride_cache: &SharedBarbStrideCache,
    projected_map_cache: &SharedProjectedMapCache,
    prepared_projected_maps: &PreparedProjectedMaps,
) -> Result<DirectRenderedRecipe, Box<dyn std::error::Error>> {
    let render_start = Instant::now();
    let suffix = request
        .output_suffix
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("_{}", sanitize_output_suffix(value)))
        .unwrap_or_default();
    let earth2_suffix = earth2_filename_suffix(request.earth2_ensemble);
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}{}.png",
        request.model.as_str().replace('-', "_"),
        request.date_yyyymmdd,
        latest.cycle.hour_utc,
        request.forecast_hour,
        request.domain.slug,
        item.recipe.slug,
        format!("{suffix}{earth2_suffix}")
    ));
    let canonical_product = canonical_fetch_product_for_selectors(
        request,
        item.plan.product.as_ref(),
        &item.plan.selectors(),
    );
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
            streamline_layer_cache,
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
        let overlay_only = !earth2_suppresses_companion_overlays(request.earth2_ensemble)
            && should_render_overlay_only(filled_selector, item.recipe.contours.is_some());
        let visual_mode = visual_mode_for_direct_recipe(item.recipe, filled_selector, overlay_only);
        let target_ratio = direct_map_frame_aspect_ratio(
            visual_mode,
            request.output_width,
            request.output_height,
            filled.projection.as_ref(),
        );
        let render_bounds = render_bounds_for_direct_field(
            request.domain.bounds,
            filled,
            visual_mode,
            request.output_width,
            request.output_height,
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
                target_ratio,
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
            render_bounds,
            request.output_width,
            request.output_height,
            contour_layer_cache,
            barb_layer_cache,
            streamline_layer_cache,
            barb_stride_cache,
            request.contour_mode,
            request.native_fill_level_multiplier,
            request.earth2_ensemble,
        )?;
        let request_build_ms = request_build_start.elapsed().as_millis();
        apply_source_raster_policy(latest.source, &mut render_request);
        render_request.title = Some(direct_title_for_planned_product(
            request,
            item.plan.product.as_ref(),
            item.recipe.title,
        ));
        render_request.subtitle_left =
            Some(request.subtitle_left_override.clone().unwrap_or_else(|| {
                model_time_subtitle(
                    request.model,
                    &request.date_yyyymmdd,
                    latest.cycle.hour_utc,
                    request.forecast_hour,
                )
            }));
        render_request.subtitle_right = Some(
            request
                .subtitle_right_override
                .clone()
                .unwrap_or_else(|| source_subtitle(latest.source)),
        );
        if let Some(overlay) = request.custom_poi_overlay.as_ref() {
            apply_custom_poi_overlay(
                &mut render_request,
                overlay,
                render_bounds,
                &filled.grid.lat_deg,
                &filled.grid.lon_deg,
                filled.projection.as_ref(),
            )?;
        }
        if let Some(overlay) = request.place_label_overlay.as_ref() {
            crate::apply_place_label_overlay_with_density_styling(
                &mut render_request,
                overlay,
                &request.domain,
                &filled.grid.lat_deg,
                &filled.grid.lon_deg,
                filled.projection.as_ref(),
            )?;
        }
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
        title: direct_title_for_planned_product(
            request,
            item.plan.product.as_ref(),
            item.recipe.title,
        ),
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
    streamline_layer_cache: &SharedStreamlineLayerCache,
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
    let panel_target_ratio = direct_map_frame_aspect_ratio(
        ProductVisualMode::PanelMember,
        spec.panel_width,
        spec.panel_height,
        first_field.projection.as_ref(),
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
            panel_target_ratio,
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
        let panel_render_bounds = render_bounds_for_direct_field(
            request.domain.bounds,
            filled,
            ProductVisualMode::PanelMember,
            spec.panel_width,
            spec.panel_height,
        );
        let (mut panel_request, panel_timing) = build_render_request(
            component_recipe,
            filled,
            extracted,
            projected.clone(),
            panel_render_bounds,
            spec.panel_width,
            spec.panel_height,
            contour_layer_cache,
            barb_layer_cache,
            streamline_layer_cache,
            barb_stride_cache,
            request.contour_mode,
            request.native_fill_level_multiplier,
            request.earth2_ensemble,
        )?;
        build_timing.field_prepare_ms += panel_timing.field_prepare_ms;
        build_timing.contour_prepare_ms += panel_timing.contour_prepare_ms;
        build_timing.barb_prepare_ms += panel_timing.barb_prepare_ms;
        apply_source_raster_policy(latest.source, &mut panel_request);
        panel_request.width = spec.panel_width;
        panel_request.height = spec.panel_height;
        panel_request.visual_mode = ProductVisualMode::PanelMember;
        panel_request.subtitle_left = None;
        panel_request.subtitle_right = None;
        if let Some(overlay) = request.custom_poi_overlay.as_ref() {
            apply_custom_poi_overlay(
                &mut panel_request,
                overlay,
                panel_render_bounds,
                &filled.grid.lat_deg,
                &filled.grid.lon_deg,
                filled.projection.as_ref(),
            )?;
        }
        if let Some(overlay) = request.place_label_overlay.as_ref() {
            crate::apply_place_label_overlay_with_density_styling(
                &mut panel_request,
                overlay,
                &request.domain,
                &filled.grid.lat_deg,
                &filled.grid.lon_deg,
                filled.projection.as_ref(),
            )?;
        }
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
    let title = direct_panel_title_for_request(request, recipe.title);
    draw_centered_text_line(&mut canvas, &title, 10, Color::BLACK, 2);
    draw_centered_text_line(
        &mut canvas,
        &format!(
            "{} | {}",
            request.subtitle_left_override.clone().unwrap_or_else(|| {
                model_time_subtitle(
                    request.model,
                    &request.date_yyyymmdd,
                    latest.cycle.hour_utc,
                    request.forecast_hour,
                )
            }),
            request
                .subtitle_right_override
                .clone()
                .unwrap_or_else(|| source_subtitle(latest.source))
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
    streamline_layer_cache: &SharedStreamlineLayerCache,
    barb_stride_cache: &SharedBarbStrideCache,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
    earth2_ensemble: Option<Earth2EnsembleSelector>,
) -> Result<(MapRenderRequest, DirectRequestBuildTiming), Box<dyn std::error::Error>> {
    let mut timing = DirectRequestBuildTiming::default();
    let field_prepare_start = Instant::now();
    let filled_field =
        render_filled_field_with_ensemble(recipe, filled, extracted, earth2_ensemble)?;
    timing.field_prepare_ms = field_prepare_start.elapsed().as_millis();
    let suppress_companion_overlays = earth2_suppresses_companion_overlays(earth2_ensemble);
    let overlay_only = !suppress_companion_overlays
        && should_render_overlay_only(filled.selector, recipe.contours.is_some());
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
            scale_for_earth2_selector(
                recipe,
                filled.selector,
                &filled_field.values,
                earth2_ensemble,
            ),
        )
    };
    crate::plot_design::StaticPlotDesign::new(bounds, visual_mode)
        .overlay_only(overlay_only)
        .apply_to_request(&mut request);
    request.title = Some(static_title_with_suffix(recipe.title));
    request.width = output_width;
    request.height = output_height;
    request.chrome_scale = static_chrome_scale();
    request.supersample_factor = static_supersample_factor();
    request.supersample_sharpen = static_supersample_sharpen();
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x,
        y: projected.projected_y,
        extent: projected.extent,
    });
    request.projected_lines = projected.lines;
    request.projected_polygons = projected.polygons;
    request.inverse_raster_projection = projected.inverse_raster_projection;
    let contour_prepare_start = Instant::now();
    if !suppress_companion_overlays {
        if overlay_only {
            request
                .contours
                .extend(build_contour_layers(recipe, extracted, contour_layer_cache));
        } else {
            request.contours = build_contour_layers(recipe, extracted, contour_layer_cache);
        }
    }
    timing.contour_prepare_ms += contour_prepare_start.elapsed().as_millis();
    let barb_prepare_start = Instant::now();
    if !suppress_companion_overlays {
        request.wind_streamlines = build_streamline_layers(
            recipe,
            extracted,
            bounds,
            streamline_layer_cache,
            barb_stride_cache,
        );
        request.wind_barbs = build_barb_layers(
            recipe,
            extracted,
            bounds,
            barb_layer_cache,
            barb_stride_cache,
        );
    }
    timing.barb_prepare_ms = barb_prepare_start.elapsed().as_millis();
    if !overlay_only && !suppress_companion_overlays {
        let contour_fill_start = Instant::now();
        maybe_apply_below_ground_mask_overlay(filled.selector, extracted, &mut request)?;
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

fn apply_source_raster_policy(source: SourceId, request: &mut MapRenderRequest) {
    if matches!(source, SourceId::AifsInference)
        && request.raster_sample_mode == RasterSampleMode::Nearest
    {
        request.raster_sample_mode = RasterSampleMode::Linear;
    }
}

fn maybe_apply_below_ground_mask_overlay(
    filled_selector: FieldSelector,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    request: &mut MapRenderRequest,
) -> Result<(), Box<dyn std::error::Error>> {
    let VerticalSelector::IsobaricHpa(level_hpa) = filled_selector.vertical else {
        return Ok(());
    };
    let Some(projected_domain) = request.projected_domain.as_ref() else {
        return Ok(());
    };
    let Some(surface_pressure) = extracted.get(&FieldSelector::surface(CanonicalField::Pressure))
    else {
        return Ok(());
    };

    let nx = surface_pressure.grid.shape.nx;
    let ny = surface_pressure.grid.shape.ny;
    if nx < 2 || ny < 2 {
        return Ok(());
    }
    if projected_domain.x.len() != nx * ny || projected_domain.y.len() != nx * ny {
        return Ok(());
    }

    let target_pa = level_hpa as f32 * 100.0;
    let masked: Vec<bool> = surface_pressure
        .values
        .iter()
        .map(|value| value.is_finite() && *value < target_pa)
        .collect();
    if !masked.iter().any(|value| *value) {
        return Ok(());
    }

    let render_mask = dilate_mask(&masked, nx, ny);
    apply_below_ground_nan_mask(&render_mask, &mut request.field.values);
    for contour in &mut request.contours {
        apply_below_ground_nan_mask(&render_mask, &mut contour.data);
    }
    for barb in &mut request.wind_barbs {
        apply_below_ground_nan_mask(&render_mask, &mut barb.u);
        apply_below_ground_nan_mask(&render_mask, &mut barb.v);
    }

    let idx = |j: usize, i: usize| j * nx + i;
    let cell_masked = |j: usize, i: usize| {
        render_mask[idx(j, i)]
            && render_mask[idx(j, i + 1)]
            && render_mask[idx(j + 1, i)]
            && render_mask[idx(j + 1, i + 1)]
    };

    for j in 0..(ny - 1) {
        let mut i = 0usize;
        while i < nx - 1 {
            if !cell_masked(j, i) {
                i += 1;
                continue;
            }
            let start = i;
            let mut end = i;
            while end + 1 < nx - 1 && cell_masked(j, end + 1) {
                end += 1;
            }

            let mut ring = Vec::with_capacity(((end - start + 2) * 2) + 1);
            for col in start..=end + 1 {
                ring.push((
                    projected_domain.x[idx(j, col)],
                    projected_domain.y[idx(j, col)],
                ));
            }
            for col in (start..=end + 1).rev() {
                ring.push((
                    projected_domain.x[idx(j + 1, col)],
                    projected_domain.y[idx(j + 1, col)],
                ));
            }
            if let Some(first) = ring.first().copied() {
                ring.push(first);
            }
            if ring.iter().all(|(x, y)| x.is_finite() && y.is_finite()) {
                request
                    .projected_data_polygons
                    .push(rustwx_render::ProjectedPolygonFill {
                        rings: vec![ring],
                        color: Color::rgba(210, 200, 181, 255),
                        role: rustwx_render::PolygonRole::Generic,
                    });
            }
            i = end + 1;
        }
    }
    Ok(())
}

fn dilate_mask(mask: &[bool], nx: usize, ny: usize) -> Vec<bool> {
    let mut dilated = vec![false; mask.len()];
    for j in 0..ny {
        let j0 = j.saturating_sub(1);
        let j1 = (j + 1).min(ny - 1);
        for i in 0..nx {
            let i0 = i.saturating_sub(1);
            let i1 = (i + 1).min(nx - 1);
            let masked = (j0..=j1).any(|jj| (i0..=i1).any(|ii| mask[jj * nx + ii]));
            dilated[j * nx + i] = masked;
        }
    }
    dilated
}

fn apply_below_ground_nan_mask(mask: &[bool], values: &mut [f32]) {
    if values.len() != mask.len() {
        return;
    }
    for (value, masked) in values.iter_mut().zip(mask.iter().copied()) {
        if masked {
            *value = f32::NAN;
        }
    }
}

fn maybe_apply_experimental_projected_contours(
    _recipe: &PlotRecipe,
    request: &mut MapRenderRequest,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let enabled = match contour_mode {
        NativeContourRenderMode::Automatic
        | NativeContourRenderMode::LegacyRaster
        | NativeContourRenderMode::Signature => false,
        NativeContourRenderMode::ExperimentalAllProjected => true,
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

    if matches!(recipe.style, RenderStyle::WeatherHeight)
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

fn earth2_filename_suffix(selector: Option<Earth2EnsembleSelector>) -> String {
    selector
        .map(|selector| format!("_{}", selector.filename_slug()))
        .unwrap_or_default()
}

fn sanitize_output_suffix(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '_' | '-' | '.') {
            out.push(ch);
        } else if ch.is_whitespace() {
            out.push('_');
        }
    }
    out.trim_matches(['_', '-', '.']).to_string()
}

fn earth2_suppresses_companion_overlays(selector: Option<Earth2EnsembleSelector>) -> bool {
    matches!(
        selector,
        Some(Earth2EnsembleSelector::Statistic(
            Earth2EnsembleStat::Std | Earth2EnsembleStat::Min | Earth2EnsembleStat::Max
        ))
    )
}

fn earth2_is_std_selector(selector: Option<Earth2EnsembleSelector>) -> bool {
    matches!(
        selector,
        Some(Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Std))
    )
}

fn selector_is_spread_product(selector: FieldSelector) -> bool {
    matches!(
        selector.product,
        FieldProduct::EnsembleStandardDeviation | FieldProduct::EnsembleSpread
    )
}

fn render_filled_field(
    recipe: &PlotRecipe,
    field: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> Result<rustwx_core::Field2D, Box<dyn std::error::Error>> {
    render_filled_field_with_ensemble(recipe, field, extracted, None)
}

fn render_filled_field_with_ensemble(
    recipe: &PlotRecipe,
    field: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    earth2_ensemble: Option<Earth2EnsembleSelector>,
) -> Result<rustwx_core::Field2D, Box<dyn std::error::Error>> {
    if earth2_suppresses_companion_overlays(earth2_ensemble) {
        return Ok(convert_filled_field_with_ensemble(
            recipe,
            field,
            earth2_ensemble,
        ));
    }
    if let Some(wind_speed) = derived_height_winds_fill(recipe, field, extracted)? {
        return Ok(wind_speed);
    }
    Ok(convert_filled_field_with_ensemble(
        recipe,
        field,
        earth2_ensemble,
    ))
}

fn derived_height_winds_fill(
    recipe: &PlotRecipe,
    field: &SelectedField2D,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
) -> Result<Option<rustwx_core::Field2D>, Box<dyn std::error::Error>> {
    if recipe.style != RenderStyle::WeatherHeight
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

fn convert_filled_field_with_ensemble(
    recipe: &PlotRecipe,
    field: &SelectedField2D,
    earth2_ensemble: Option<Earth2EnsembleSelector>,
) -> rustwx_core::Field2D {
    let mut core = field.clone().into_field2d();
    if field.selector.field == CanonicalField::SmokeMassDensity {
        for value in &mut core.values {
            *value *= 1_000_000_000.0;
        }
        core.units = "ug/m^3".to_string();
    } else if field.selector.field == CanonicalField::ColumnIntegratedSmoke {
        for value in &mut core.values {
            *value *= 1_000_000.0;
        }
        core.units = "mg/m^2".to_string();
    } else if matches!(
        recipe.style,
        RenderStyle::WeatherTemperature | RenderStyle::WeatherDewpoint
    ) {
        if earth2_is_std_selector(earth2_ensemble) || selector_is_spread_product(field.selector) {
            core.units = "K".to_string();
        } else if matches!(
            field.selector.vertical,
            VerticalSelector::HeightAboveGroundMeters(2)
        ) {
            for value in &mut core.values {
                *value = (*value - 273.15) * 9.0 / 5.0 + 32.0;
            }
            core.units = "degF".to_string();
        } else {
            for value in &mut core.values {
                *value -= 273.15;
            }
            core.units = "degC".to_string();
        }
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
    } else if matches!(
        field.selector.field,
        CanonicalField::WindSpeed | CanonicalField::WindGust
    ) {
        for value in &mut core.values {
            *value *= 1.943_844_5;
        }
        core.units = "kt".to_string();
    } else if field.selector.field == CanonicalField::TotalPrecipitation {
        if matches!(recipe.style, RenderStyle::WeatherQpf) {
            for value in &mut core.values {
                *value /= 25.4;
            }
            core.units = "in".to_string();
        } else {
            core.units = "mm".to_string();
        }
    }
    core
}

fn should_render_overlay_only(selector: FieldSelector, has_explicit_contours: bool) -> bool {
    if has_explicit_contours {
        return false;
    }
    matches!(selector.field, CanonicalField::GeopotentialHeight)
}

fn scale_for_earth2_selector(
    recipe: &PlotRecipe,
    filled_selector: FieldSelector,
    values: &[f32],
    earth2_ensemble: Option<Earth2EnsembleSelector>,
) -> ColorScale {
    if earth2_is_std_selector(earth2_ensemble) || selector_is_spread_product(filled_selector) {
        return earth2_spread_scale(values);
    }
    scale_for_recipe(recipe, filled_selector)
}

fn earth2_spread_scale(values: &[f32]) -> ColorScale {
    let mut finite = values
        .iter()
        .filter_map(|value| {
            let value = *value as f64;
            value.is_finite().then_some(value.max(0.0))
        })
        .collect::<Vec<_>>();
    finite.sort_by(|a, b| a.total_cmp(b));
    let p99 = percentile_sorted(&finite, 0.99).unwrap_or(1.0);
    let upper = nice_spread_upper_bound(p99);
    let step = nice_spread_step(upper / 16.0);
    ColorScale::Discrete(DiscreteColorScale {
        levels: range_step(0.0, upper + step * 0.5, step),
        colors: earth2_spread_colors(),
        extend: ExtendMode::Max,
        mask_below: None,
    })
}

fn percentile_sorted(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() - 1) as f64 * percentile.clamp(0.0, 1.0)).round() as usize;
    values.get(index).copied()
}

fn nice_spread_upper_bound(value: f64) -> f64 {
    let value = if value.is_finite() && value > 0.0 {
        value
    } else {
        1.0
    };
    let magnitude = 10_f64.powf(value.log10().floor());
    for multiple in [1.0, 2.0, 2.5, 5.0, 10.0] {
        let candidate = multiple * magnitude;
        if candidate >= value {
            return candidate.max(0.1);
        }
    }
    (10.0 * magnitude).max(0.1)
}

fn nice_spread_step(value: f64) -> f64 {
    let value = if value.is_finite() && value > 0.0 {
        value
    } else {
        0.1
    };
    let magnitude = 10_f64.powf(value.log10().floor());
    for multiple in [1.0, 2.0, 2.5, 5.0, 10.0] {
        let candidate = multiple * magnitude;
        if candidate >= value {
            return candidate.max(0.01);
        }
    }
    (10.0 * magnitude).max(0.01)
}

fn earth2_spread_colors() -> Vec<Color> {
    vec![
        Color::rgba(247, 251, 255, 255),
        Color::rgba(222, 235, 247, 255),
        Color::rgba(198, 219, 239, 255),
        Color::rgba(158, 202, 225, 255),
        Color::rgba(107, 174, 214, 255),
        Color::rgba(49, 130, 189, 255),
        Color::rgba(8, 81, 156, 255),
        Color::rgba(8, 48, 107, 255),
    ]
}

fn scale_for_recipe(recipe: &PlotRecipe, filled_selector: FieldSelector) -> ColorScale {
    crate::plot_design::operational_fill_scale_for_recipe(recipe, filled_selector)
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

    let layer = crate::plot_design::operational_contour_layer_for_values(selector, values);
    let mut cache = contour_layer_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache
        .entry(selector)
        .or_insert_with(|| layer.clone())
        .clone()
}

fn build_streamline_layers(
    recipe: &PlotRecipe,
    extracted: &HashMap<FieldSelector, SelectedField2D>,
    bounds: (f64, f64, f64, f64),
    streamline_layer_cache: &SharedStreamlineLayerCache,
    barb_stride_cache: &SharedBarbStrideCache,
) -> Vec<WindStreamlineLayer> {
    if !static_streamlines_enabled() {
        return Vec::new();
    }
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
        let cache = streamline_layer_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(layers) = cache.get(&key) {
            return layers.clone();
        }
    }

    let (stride_x, stride_y) =
        cached_streamline_strides(u_selector, v_selector, &u.grid, bounds, barb_stride_cache);
    let style = crate::plot_design::operational_wind_streamline_style(stride_x, stride_y);
    let layers = vec![WindStreamlineLayer {
        u: u.values.iter().map(|value| value * 1.943_844_5).collect(),
        v: v.values.iter().map(|value| value * 1.943_844_5).collect(),
        stride_x: style.stride_x,
        stride_y: style.stride_y,
        color: style.color,
        width: style.width,
        max_steps: style.max_steps,
        step_cells: style.step_cells,
        min_speed: style.min_speed,
    }];
    let mut cache = streamline_layer_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.entry(key).or_insert_with(|| layers.clone()).clone()
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
        width: static_barb_width(),
        length_px: static_barb_length_px(),
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
    let density = static_barb_density_scale();
    let (target_columns, target_rows) = barb_target_columns_rows(bounds);
    let strides = (
        ((visible_nx as f64 / (target_columns * density)).round() as usize).clamp(2, 128),
        ((visible_ny as f64 / (target_rows * density)).round() as usize).clamp(2, 96),
    );

    let mut cache = barb_stride_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cache.entry(key).or_insert(strides)
}

fn barb_target_columns_rows(bounds: (f64, f64, f64, f64)) -> (f64, f64) {
    let lat_span = (bounds.3 - bounds.2).abs();
    let lon_span = longitude_bounds_span_deg(bounds);
    if is_global_scale_domain(bounds) {
        (34.0, 16.0)
    } else if is_broad_continent_scale_domain(bounds) {
        (26.0, 13.0)
    } else if lat_span <= 12.0 && lon_span <= 20.0 {
        (28.0, 18.0)
    } else {
        (23.0, 14.0)
    }
}

fn cached_streamline_strides(
    u_selector: FieldSelector,
    v_selector: FieldSelector,
    grid: &rustwx_core::LatLonGrid,
    bounds: (f64, f64, f64, f64),
    barb_stride_cache: &SharedBarbStrideCache,
) -> (usize, usize) {
    let barb_strides = cached_barb_strides(u_selector, v_selector, grid, bounds, barb_stride_cache);
    let density = static_streamline_density_scale();
    (
        ((barb_strides.0 as f64 / density).round() as usize).clamp(2, 96),
        ((barb_strides.1 as f64 / density).round() as usize).clamp(2, 64),
    )
}

fn static_barb_width() -> u32 {
    std::env::var("RUSTWX_BARB_WIDTH")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(1)
        .clamp(1, 8)
}

fn static_barb_length_px() -> f64 {
    std::env::var("RUSTWX_BARB_LENGTH_PX")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(17.0)
        .clamp(6.0, 48.0)
}

fn static_barb_density_scale() -> f64 {
    std::env::var("RUSTWX_BARB_DENSITY")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0)
        .clamp(0.25, 4.0)
}

fn static_streamlines_enabled() -> bool {
    std::env::var("RUSTWX_WIND_STREAMLINES")
        .ok()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(true)
}

fn static_streamline_density_scale() -> f64 {
    std::env::var("RUSTWX_STREAMLINE_DENSITY")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0)
        .clamp(0.25, 4.0)
}

pub fn build_projected_map(
    lat_deg: &[f32],
    lon_deg: &[f32],
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    build_projected_map_with_projection(lat_deg, lon_deg, None, bounds, target_ratio)
}

pub fn build_projected_map_with_projection(
    lat_deg: &[f32],
    lon_deg: &[f32],
    projection: Option<&rustwx_core::GridProjection>,
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    if full_domain_projected_frame_enabled(projection) {
        return build_full_domain_projected_map_with_projection(
            lat_deg,
            lon_deg,
            projection,
            bounds,
            target_ratio,
        );
    }

    let variant = projection_presentation_variant();
    let presentation_projection = presentation_projection_for_bounds(projection, bounds, variant);
    let frame_bounds = presentation_frame_bounds_for_projection(
        bounds,
        presentation_projection.as_ref(),
        target_ratio,
    );
    let mut options =
        rustwx_render::ProjectedMapBuildOptions::from_bounds(frame_bounds, target_ratio);
    if let Some(presentation_projection) = presentation_projection {
        let reference_latitude =
            reference_latitude_for_projection_variant(variant, projection, frame_bounds);
        options = options.with_projection(presentation_projection);
        if let Some(reference_latitude) = reference_latitude {
            options.domain.reference_latitude_deg = Some(reference_latitude);
        }
    }
    options = options.with_basemap_detail(basemap_detail_for_bounds(frame_bounds));
    options.domain.pad_fraction = presentation_pad_fraction_for_bounds(frame_bounds);
    let mut projected =
        rustwx_render::build_projected_map_with_options(lat_deg, lon_deg, &options)?;
    projected.inverse_raster_projection =
        inverse_raster_projection_for_latlon_mesh(projection, frame_bounds, lat_deg, lon_deg);
    Ok(projected)
}

fn build_full_domain_projected_map_with_projection(
    lat_deg: &[f32],
    lon_deg: &[f32],
    projection: Option<&rustwx_core::GridProjection>,
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    let mut options = ProjectedMapBuildOptions::full_domain(target_ratio);
    if let Some(projection) = projection {
        options = options.with_projection(projection.clone());
    }
    let basemap_bounds = latlon_mesh_bounds(lat_deg, lon_deg).unwrap_or(bounds);
    options = options.with_basemap_detail(basemap_detail_for_bounds(basemap_bounds));
    options.domain.pad_fraction = full_domain_projected_frame_pad_fraction();
    let mut projected =
        rustwx_render::build_projected_map_with_options(lat_deg, lon_deg, &options)?;
    projected.inverse_raster_projection =
        inverse_raster_projection_for_latlon_mesh(projection, basemap_bounds, lat_deg, lon_deg);
    Ok(projected)
}

fn full_domain_projected_frame_enabled(projection: Option<&GridProjection>) -> bool {
    let auto = full_domain_projected_frame_default(projection);
    std::env::var("RUSTWX_PROJECTED_FRAME_SOURCE")
        .ok()
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "full-domain" | "full_domain" | "native" | "native-domain" | "native_domain" => true,
            "requested" | "request" | "bounds" | "domain" | "map-bounds" | "map_bounds" => false,
            "auto" | "" => auto,
            other => matches!(other, "1" | "true" | "yes" | "on"),
        })
        .unwrap_or(auto)
}

fn full_domain_projected_frame_default(projection: Option<&GridProjection>) -> bool {
    projection
        .map(GridProjection::is_projected)
        .unwrap_or(false)
}

fn full_domain_projected_frame_pad_fraction() -> f64 {
    std::env::var("RUSTWX_PROJECTED_FRAME_PAD_FRACTION")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(0.02)
        .clamp(0.0, 0.25)
}

fn latlon_mesh_bounds(lat_deg: &[f32], lon_deg: &[f32]) -> Option<(f64, f64, f64, f64)> {
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for (&lat, &lon) in lat_deg.iter().zip(lon_deg.iter()) {
        let lat = lat as f64;
        let lon = lon as f64;
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        south = south.min(lat);
        north = north.max(lat);
        west = west.min(lon);
        east = east.max(lon);
    }
    (west.is_finite() && east.is_finite() && south.is_finite() && north.is_finite())
        .then_some((west, east, south, north))
}

pub(crate) fn inverse_raster_projection_for_grid(
    projection: Option<&GridProjection>,
    bounds: (f64, f64, f64, f64),
    grid: &rustwx_core::LatLonGrid,
) -> Option<InverseRasterProjection> {
    inverse_raster_projection_for_latlon_mesh(projection, bounds, &grid.lat_deg, &grid.lon_deg)
}

fn inverse_raster_projection_for_latlon_mesh(
    projection: Option<&GridProjection>,
    bounds: (f64, f64, f64, f64),
    lat_deg: &[f32],
    lon_deg: &[f32],
) -> Option<InverseRasterProjection> {
    let regular_latlon = matches!(projection, Some(GridProjection::Geographic))
        || (projection.is_none() && rectilinear_latlon_mesh_for_inverse(lat_deg, lon_deg));
    if !regular_latlon {
        return None;
    }
    let variant = projection_presentation_variant();
    let projection =
        presentation_projection_for_bounds(Some(&GridProjection::Geographic), bounds, variant)?;
    let reference_longitude_deg = match projection {
        rustwx_render::ProjectionSpec::Geographic => Some(center_longitude_for_bounds(bounds)),
        _ => None,
    };
    match projection {
        rustwx_render::ProjectionSpec::AlbersEqualArea { .. }
        | rustwx_render::ProjectionSpec::Geographic
        | rustwx_render::ProjectionSpec::LambertConformal { .. }
        | rustwx_render::ProjectionSpec::Mercator { .. }
        | rustwx_render::ProjectionSpec::Robinson { .. } => {
            let clip_bounds = inverse_raster_clip_bounds(bounds, &projection);
            Some(InverseRasterProjection {
                projection,
                reference_latitude_deg: reference_latitude_for_projection_variant(
                    variant,
                    Some(&GridProjection::Geographic),
                    bounds,
                ),
                reference_longitude_deg,
                clip_bounds,
            })
        }
        _ => None,
    }
}

fn inverse_raster_clip_bounds(
    bounds: (f64, f64, f64, f64),
    projection: &rustwx_render::ProjectionSpec,
) -> Option<GeographicClipBounds> {
    if !env_flag_enabled("RUSTWX_INVERSE_RASTER_GEO_CLIP", true) {
        return None;
    }
    if !matches!(projection, rustwx_render::ProjectionSpec::Geographic) {
        return None;
    }
    Some(GeographicClipBounds::new(
        bounds.0, bounds.1, bounds.2, bounds.3,
    ))
}

fn env_flag_enabled(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn rectilinear_latlon_mesh_for_inverse(lat_deg: &[f32], lon_deg: &[f32]) -> bool {
    if lat_deg.len() != lon_deg.len() || lat_deg.len() < 9 {
        return false;
    }
    let len = lat_deg.len();
    let mut nx = 0usize;
    for idx in 1..len {
        if (lat_deg[idx] - lat_deg[0]).abs() > 1.0e-4 {
            nx = idx;
            break;
        }
    }
    if nx < 2 || len % nx != 0 {
        return false;
    }
    let ny = len / nx;
    if ny < 2 {
        return false;
    }
    let sample_rows = [0, ny / 2, ny - 1];
    let sample_cols = [0, nx / 2, nx - 1];
    for &row in &sample_rows {
        let row_offset = row * nx;
        let row_lat = lat_deg[row_offset];
        for &col in &sample_cols {
            if (lat_deg[row_offset + col] - row_lat).abs() > 1.0e-3 {
                return false;
            }
        }
    }
    for &col in &sample_cols {
        let col_lon = lon_deg[col];
        for &row in &sample_rows {
            if longitude_delta_abs_deg(lon_deg[row * nx + col], col_lon) > 1.0e-3 {
                return false;
            }
        }
    }
    true
}

fn longitude_delta_abs_deg(a: f32, b: f32) -> f32 {
    let mut delta = (a - b).abs();
    while delta > 180.0 {
        delta = (delta - 360.0).abs();
    }
    delta
}

pub fn model_data_domain_frame_for_projection(
    _projection: Option<&GridProjection>,
) -> Option<DomainFrame> {
    Some(DomainFrame::map_viewport_default())
}

fn direct_map_frame_aspect_ratio(
    visual_mode: ProductVisualMode,
    width: u32,
    height: u32,
    projection: Option<&GridProjection>,
) -> f64 {
    rustwx_render::map_frame_aspect_ratio_for_mode_with_domain_frame_and_chrome_scale(
        visual_mode,
        width,
        height,
        true,
        true,
        model_data_domain_frame_for_projection(projection).is_some(),
        static_chrome_scale(),
    )
}

fn basemap_detail_for_bounds(bounds: (f64, f64, f64, f64)) -> BasemapDetail {
    let lat_span = (bounds.3 - bounds.2).abs();
    let lon_span = longitude_bounds_span_deg(bounds);
    if is_global_scale_domain(bounds) {
        BasemapDetail::Global
    } else if lat_span >= 45.0 || lon_span >= 65.0 {
        BasemapDetail::Broad
    } else {
        BasemapDetail::Regional
    }
}

fn presentation_pad_fraction_for_bounds(bounds: (f64, f64, f64, f64)) -> f64 {
    if let Ok(value) = std::env::var("RUSTWX_PRESENTATION_PAD_FRACTION") {
        if let Ok(parsed) = value.trim().parse::<f64>() {
            return parsed.clamp(0.0, 0.25);
        }
    }
    let lat_span = (bounds.3 - bounds.2).abs();
    let lon_span = longitude_bounds_span_deg(bounds);
    if is_global_scale_domain(bounds) {
        0.06
    } else if lat_span >= 45.0 || lon_span >= 65.0 {
        0.045
    } else {
        0.025
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionPresentationVariant {
    Adaptive,
    AlbersEqualArea,
    RectangularGeographic,
    Mercator,
    PivotalLambert,
    Robinson,
}

const PIVOTAL_CONUS_STANDARD_PARALLEL_1_DEG: f64 = 33.0;
const PIVOTAL_CONUS_STANDARD_PARALLEL_2_DEG: f64 = 45.0;
const PIVOTAL_CONUS_CENTRAL_MERIDIAN_DEG: f64 = -96.0;
const PIVOTAL_CONUS_REFERENCE_LATITUDE_DEG: f64 = 39.0;
const NORTH_AMERICA_LAMBERT_REFERENCE_LATITUDE_DEG: f64 = 45.0;
const PIVOTAL_GEOGRAPHIC_CROP_PAD_DEG: f64 = 18.0;

fn projection_presentation_variant() -> ProjectionPresentationVariant {
    std::env::var("RUSTWX_PROJECTION_VARIANT")
        .ok()
        .map(
            |value| match normalize_projection_variant_name(&value).as_str() {
                "albers" | "albersequalarea" | "aea" => {
                    ProjectionPresentationVariant::AlbersEqualArea
                }
                "rectangular" | "geographic" | "platecarree" | "crop" => {
                    ProjectionPresentationVariant::RectangularGeographic
                }
                "mercator" | "webmap" | "webmercator" => ProjectionPresentationVariant::Mercator,
                "pivotallambert" | "pivotal" => ProjectionPresentationVariant::PivotalLambert,
                "robinson" | "atlas" => ProjectionPresentationVariant::Robinson,
                _ => ProjectionPresentationVariant::Adaptive,
            },
        )
        .unwrap_or(ProjectionPresentationVariant::Adaptive)
}

fn normalize_projection_variant_name(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', '_'], "")
}

fn presentation_projection_for_bounds(
    native_projection: Option<&GridProjection>,
    bounds: (f64, f64, f64, f64),
    variant: ProjectionPresentationVariant,
) -> Option<rustwx_render::ProjectionSpec> {
    if is_global_scale_domain(bounds) {
        return Some(rustwx_render::ProjectionSpec::Robinson {
            central_meridian_deg: center_longitude_for_bounds(bounds),
        });
    }

    match native_projection {
        Some(GridProjection::Geographic) | None => {
            Some(regional_latlon_presentation_projection(bounds, variant))
        }
        Some(projection) => Some(projection.clone().into()),
    }
}

fn regional_latlon_presentation_projection(
    bounds: (f64, f64, f64, f64),
    variant: ProjectionPresentationVariant,
) -> rustwx_render::ProjectionSpec {
    match variant {
        ProjectionPresentationVariant::AlbersEqualArea => conus_albers_presentation_projection(),
        ProjectionPresentationVariant::RectangularGeographic => {
            rustwx_render::ProjectionSpec::Geographic
        }
        ProjectionPresentationVariant::Mercator => {
            regional_mercator_presentation_projection(bounds)
        }
        ProjectionPresentationVariant::PivotalLambert if is_conus_lambert_candidate(bounds) => {
            pivotal_lambert_conus_projection()
        }
        ProjectionPresentationVariant::PivotalLambert
            if is_north_america_projection_candidate(bounds) =>
        {
            north_america_lambert_presentation_projection()
        }
        ProjectionPresentationVariant::Robinson => robinson_presentation_projection(bounds),
        _ => regional_presentation_projection(bounds),
    }
}

fn presentation_frame_bounds_for_projection(
    bounds: (f64, f64, f64, f64),
    projection: Option<&rustwx_render::ProjectionSpec>,
    target_ratio: f64,
) -> (f64, f64, f64, f64) {
    if !matches!(projection, Some(rustwx_render::ProjectionSpec::Geographic))
        || is_global_scale_domain(bounds)
    {
        return bounds;
    }
    expand_geographic_bounds_to_aspect(bounds, target_ratio)
}

fn presentation_frame_bounds_for_grid(
    native_projection: Option<&GridProjection>,
    bounds: (f64, f64, f64, f64),
    variant: ProjectionPresentationVariant,
    target_ratio: f64,
) -> (f64, f64, f64, f64) {
    let presentation_projection =
        presentation_projection_for_bounds(native_projection, bounds, variant);
    presentation_frame_bounds_for_projection(bounds, presentation_projection.as_ref(), target_ratio)
}

fn expand_geographic_bounds_to_aspect(
    bounds: (f64, f64, f64, f64),
    target_ratio: f64,
) -> (f64, f64, f64, f64) {
    let safe_ratio = target_ratio.max(1.0e-6);
    let mut south = bounds.2.min(bounds.3).clamp(-89.5, 89.5);
    let mut north = bounds.2.max(bounds.3).clamp(-89.5, 89.5);
    if north <= south {
        south = (south - 0.5).clamp(-89.5, 89.0);
        north = (north + 0.5).clamp(-89.0, 89.5);
    }
    let lat_span = (north - south).max(1.0e-6);
    let lon_span = longitude_bounds_span_deg(bounds).max(1.0e-6);
    let current_ratio = lon_span / lat_span;
    if current_ratio < safe_ratio {
        let wanted_lon_span = (lat_span * safe_ratio).min(360.0);
        let center = center_longitude_for_bounds(bounds);
        let west = normalize_longitude_for_bounds(center - wanted_lon_span / 2.0);
        let east_unwrapped = center + wanted_lon_span / 2.0;
        let east = if east_unwrapped > 180.0 {
            east_unwrapped
        } else {
            normalize_longitude_for_bounds(east_unwrapped)
        };
        (west, east, south, north)
    } else {
        let wanted_lat_span = lon_span / safe_ratio;
        let center = ((south + north) / 2.0).clamp(-89.0, 89.0);
        south = (center - wanted_lat_span / 2.0).clamp(-89.5, 89.5);
        north = (center + wanted_lat_span / 2.0).clamp(-89.5, 89.5);
        if north - south < wanted_lat_span {
            if south <= -89.5 {
                north = (south + wanted_lat_span).clamp(-89.5, 89.5);
            } else if north >= 89.5 {
                south = (north - wanted_lat_span).clamp(-89.5, 89.5);
            }
        }
        (bounds.0, bounds.1, south, north)
    }
}

fn conus_albers_presentation_projection() -> rustwx_render::ProjectionSpec {
    rustwx_render::ProjectionSpec::AlbersEqualArea {
        standard_parallel_1_deg: 29.5,
        standard_parallel_2_deg: 45.5,
        central_meridian_deg: -96.0,
        latitude_of_origin_deg: 23.0,
    }
}

fn pivotal_lambert_conus_projection() -> rustwx_render::ProjectionSpec {
    rustwx_render::ProjectionSpec::LambertConformal {
        standard_parallel_1_deg: PIVOTAL_CONUS_STANDARD_PARALLEL_1_DEG,
        standard_parallel_2_deg: PIVOTAL_CONUS_STANDARD_PARALLEL_2_DEG,
        central_meridian_deg: PIVOTAL_CONUS_CENTRAL_MERIDIAN_DEG,
    }
}

fn north_america_lambert_presentation_projection() -> rustwx_render::ProjectionSpec {
    rustwx_render::ProjectionSpec::LambertConformal {
        standard_parallel_1_deg: 25.0,
        standard_parallel_2_deg: 60.0,
        central_meridian_deg: -100.0,
    }
}

fn regional_mercator_presentation_projection(
    bounds: (f64, f64, f64, f64),
) -> rustwx_render::ProjectionSpec {
    if bounds.3 <= -55.0 || bounds.2 >= 55.0 {
        return regional_presentation_projection(bounds);
    }

    rustwx_render::ProjectionSpec::Mercator {
        latitude_of_true_scale_deg: ((bounds.2 + bounds.3) / 2.0).clamp(-85.0, 85.0),
        central_meridian_deg: center_longitude_for_bounds(bounds),
    }
}

fn reference_latitude_for_projection_variant(
    variant: ProjectionPresentationVariant,
    native_projection: Option<&GridProjection>,
    bounds: (f64, f64, f64, f64),
) -> Option<f64> {
    let _ = variant;
    match native_projection {
        Some(GridProjection::Geographic) | None if !is_global_scale_domain(bounds) => {
            if is_conus_lambert_candidate(bounds) {
                Some(PIVOTAL_CONUS_REFERENCE_LATITUDE_DEG)
            } else if is_north_america_projection_candidate(bounds) {
                Some(NORTH_AMERICA_LAMBERT_REFERENCE_LATITUDE_DEG)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_conus_lambert_candidate(bounds: (f64, f64, f64, f64)) -> bool {
    let west = normalize_longitude_for_bounds(bounds.0);
    let east = normalize_longitude_for_bounds(bounds.1);
    if west > east {
        return false;
    }

    let lat_span = (bounds.3 - bounds.2).abs();
    let lon_span = longitude_bounds_span_deg(bounds);
    bounds.2 >= 20.0
        && bounds.3 <= 56.0
        && west >= -132.0
        && east <= -60.0
        && lat_span >= 5.0
        && lat_span <= 38.0
        && lon_span >= 8.0
        && lon_span <= 75.0
}

fn is_north_america_projection_candidate(bounds: (f64, f64, f64, f64)) -> bool {
    let west = normalize_longitude_for_bounds(bounds.0);
    let east = normalize_longitude_for_bounds(bounds.1);
    if west > east {
        return false;
    }

    let lat_span = (bounds.3 - bounds.2).abs();
    let lon_span = longitude_bounds_span_deg(bounds);
    bounds.2 >= -5.0
        && bounds.3 <= 88.0
        && west >= -180.0
        && east <= -35.0
        && lat_span >= 45.0
        && lon_span >= 80.0
        && lon_span <= 155.0
}

fn regional_presentation_projection(bounds: (f64, f64, f64, f64)) -> rustwx_render::ProjectionSpec {
    let center_lat = ((bounds.2 + bounds.3) / 2.0).clamp(-85.0, 85.0);
    let center_lon = center_longitude_for_bounds(bounds);
    let lat_span = (bounds.3 - bounds.2).abs();

    if bounds.3 <= -55.0 {
        return rustwx_render::ProjectionSpec::PolarStereographic {
            true_latitude_deg: -71.0,
            central_meridian_deg: center_lon,
            south_pole_on_projection_plane: true,
        };
    }
    if bounds.2 >= 55.0 {
        return rustwx_render::ProjectionSpec::PolarStereographic {
            true_latitude_deg: 71.0,
            central_meridian_deg: center_lon,
            south_pole_on_projection_plane: false,
        };
    }
    if is_broad_continent_scale_domain(bounds) {
        return rustwx_render::ProjectionSpec::Geographic;
    }
    if bounds.2 < -25.0 && bounds.3 > 25.0 {
        return rustwx_render::ProjectionSpec::Mercator {
            latitude_of_true_scale_deg: center_lat,
            central_meridian_deg: center_lon,
        };
    }

    let inset = (lat_span / 6.0).clamp(2.0, 12.0);
    let sp1 = stabilize_presentation_parallel(bounds.2 + inset);
    let sp2 = stabilize_presentation_parallel(bounds.3 - inset);
    rustwx_render::ProjectionSpec::LambertConformal {
        standard_parallel_1_deg: sp1,
        standard_parallel_2_deg: if (sp2 - sp1).abs() < 0.25 { sp1 } else { sp2 },
        central_meridian_deg: center_lon,
    }
}

fn is_broad_continent_scale_domain(bounds: (f64, f64, f64, f64)) -> bool {
    let lat_span = (bounds.3 - bounds.2).abs();
    let lon_span = longitude_bounds_span_deg(bounds);
    !is_conus_lambert_candidate(bounds) && (lat_span >= 50.0 || lon_span >= 90.0)
}

fn robinson_presentation_projection(bounds: (f64, f64, f64, f64)) -> rustwx_render::ProjectionSpec {
    rustwx_render::ProjectionSpec::Robinson {
        central_meridian_deg: center_longitude_for_bounds(bounds),
    }
}

fn center_longitude_for_bounds(bounds: (f64, f64, f64, f64)) -> f64 {
    if longitude_bounds_span_deg(bounds) >= 359.0 {
        return 0.0;
    }
    let west = normalize_longitude_for_bounds(bounds.0);
    let mut east = normalize_longitude_for_bounds(bounds.1);
    if east < west {
        east += 360.0;
    }
    normalize_longitude_for_bounds((west + east) / 2.0)
}

fn stabilize_presentation_parallel(lat_deg: f64) -> f64 {
    let lat = lat_deg.clamp(-80.0, 80.0);
    if lat.abs() < 1.0 {
        10.0_f64.copysign(if lat < 0.0 { -1.0 } else { 1.0 })
    } else {
        lat
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{GridProjection, GridShape, LatLonGrid, SelectedField2D};

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
    fn barb_density_targets_thin_operational_synoptic_domains() {
        assert_eq!(
            barb_target_columns_rows((-127.0, -66.0, 23.0, 51.5)),
            (23.0, 14.0)
        );
        assert_eq!(
            barb_target_columns_rows((-170.0, -50.0, 5.0, 84.0)),
            (26.0, 13.0)
        );
        assert_eq!(
            barb_target_columns_rows((-180.0, 179.999, -90.0, 90.0)),
            (34.0, 16.0)
        );
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
            custom_poi_overlay: None,
            place_label_overlay: None,
            output_suffix: None,
            subtitle_left_override: None,
            subtitle_right_override: None,
            earth2_ensemble: None,
        }
    }

    #[test]
    fn native_stat_product_overrides_promote_stat_to_static_titles() {
        let mut request = sample_direct_request(ModelId::Sref);
        request.product_overrides.insert(
            "ensprod/pgrb212/mean_3hrly".to_string(),
            "ensprod/pgrb212/p50_3hrly".to_string(),
        );

        assert_eq!(
            native_stat_label_for_request(&request, Some("ensprod/pgrb212/mean_3hrly")).as_deref(),
            Some("P50")
        );
        let title = direct_title_for_planned_product(
            &request,
            "ensprod/pgrb212/mean_3hrly",
            "2m Temperature + 10m Winds",
        );
        assert!(
            title.starts_with("SREF P50 2m Temperature + 10m Winds"),
            "{title}"
        );
    }

    #[test]
    fn native_stat_title_prefix_keeps_existing_model_prefix_first() {
        assert_eq!(
            apply_native_stat_title_prefix(ModelId::Sref, "Spread", "SREF 2m Dewpoint"),
            "SREF Spread 2m Dewpoint"
        );
        assert_eq!(
            apply_native_stat_title_prefix(ModelId::Sref, "Mean", "SREF Mean 2m Dewpoint"),
            "SREF Mean 2m Dewpoint"
        );
    }

    #[test]
    fn global_scale_domain_detection_handles_dateline_bounds() {
        assert!(is_global_scale_domain((-180.0, 179.999, -90.0, 90.0)));
        assert!(!is_global_scale_domain((-125.0, -66.0, 24.0, 50.0)));
    }

    #[test]
    fn full_world_geographic_bounds_include_all_longitudes() {
        let bounds = (-180.0, 180.0, -90.0, 90.0);

        assert!(point_in_geographic_bounds(-179.5, 0.0, bounds));
        assert!(point_in_geographic_bounds(-90.0, 0.0, bounds));
        assert!(point_in_geographic_bounds(0.0, 0.0, bounds));
        assert!(point_in_geographic_bounds(90.0, 0.0, bounds));
        assert!(point_in_geographic_bounds(179.5, 0.0, bounds));
    }

    #[test]
    fn inverse_raster_latlon_maps_clip_regional_bounds() {
        let bounds = (110.0, 180.0, -50.0, 0.0);
        let inverse = inverse_raster_projection_for_grid(
            Some(&GridProjection::Geographic),
            bounds,
            &sample_grid(),
        )
        .expect("regional regular lat/lon maps should use inverse raster");

        assert!(inverse.clip_bounds.is_some());
        assert_eq!(
            inverse.reference_longitude_deg,
            Some(center_longitude_for_bounds(bounds))
        );
    }

    #[test]
    fn inverse_raster_does_not_geo_clip_projected_conus_frames() {
        let clip = inverse_raster_clip_bounds(
            (-127.0, -66.0, 23.0, 51.5),
            &rustwx_render::ProjectionSpec::LambertConformal {
                standard_parallel_1_deg: PIVOTAL_CONUS_STANDARD_PARALLEL_1_DEG,
                standard_parallel_2_deg: PIVOTAL_CONUS_STANDARD_PARALLEL_2_DEG,
                central_meridian_deg: PIVOTAL_CONUS_CENTRAL_MERIDIAN_DEG,
            },
        );

        assert!(clip.is_none());
    }

    #[test]
    fn native_projected_grids_use_full_domain_frame_by_default() {
        let lambert = GridProjection::LambertConformal {
            standard_parallel_1_deg: 33.0,
            standard_parallel_2_deg: 45.0,
            central_meridian_deg: -96.0,
        };

        assert!(full_domain_projected_frame_default(Some(&lambert)));
        assert!(!full_domain_projected_frame_default(Some(
            &GridProjection::Geographic
        )));
        assert!(!full_domain_projected_frame_default(None));
    }

    #[test]
    fn pivotal_lambert_variant_uses_fixed_conus_projection_for_geographic_grids() {
        let bounds = (-127.0, -66.0, 23.0, 51.5);
        let projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            bounds,
            ProjectionPresentationVariant::PivotalLambert,
        )
        .expect("CONUS geographic grids should get a presentation projection");

        assert_eq!(
            projection,
            rustwx_render::ProjectionSpec::LambertConformal {
                standard_parallel_1_deg: PIVOTAL_CONUS_STANDARD_PARALLEL_1_DEG,
                standard_parallel_2_deg: PIVOTAL_CONUS_STANDARD_PARALLEL_2_DEG,
                central_meridian_deg: PIVOTAL_CONUS_CENTRAL_MERIDIAN_DEG,
            }
        );
        assert_eq!(
            reference_latitude_for_projection_variant(
                ProjectionPresentationVariant::PivotalLambert,
                Some(&GridProjection::Geographic),
                bounds,
            ),
            Some(PIVOTAL_CONUS_REFERENCE_LATITUDE_DEG)
        );
    }

    #[test]
    fn pivotal_lambert_variant_keeps_global_geographic_grids_on_robinson() {
        let bounds = (-180.0, 179.999, -90.0, 90.0);
        let projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            bounds,
            ProjectionPresentationVariant::PivotalLambert,
        )
        .expect("global geographic grids should get a presentation projection");

        assert!(matches!(
            projection,
            rustwx_render::ProjectionSpec::Robinson {
                central_meridian_deg
            } if central_meridian_deg == 0.0
        ));
        assert_eq!(
            reference_latitude_for_projection_variant(
                ProjectionPresentationVariant::PivotalLambert,
                Some(&GridProjection::Geographic),
                bounds,
            ),
            None
        );
    }

    #[test]
    fn albers_variant_uses_conus_equal_area_regionally_and_robinson_globally() {
        let conus_bounds = (-127.0, -66.0, 23.0, 51.5);
        let conus_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            conus_bounds,
            ProjectionPresentationVariant::AlbersEqualArea,
        )
        .expect("CONUS geographic grids should get a presentation projection");

        assert_eq!(
            conus_projection,
            rustwx_render::ProjectionSpec::AlbersEqualArea {
                standard_parallel_1_deg: 29.5,
                standard_parallel_2_deg: 45.5,
                central_meridian_deg: -96.0,
                latitude_of_origin_deg: 23.0,
            }
        );

        let global_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            (-180.0, 179.999, -90.0, 90.0),
            ProjectionPresentationVariant::AlbersEqualArea,
        )
        .expect("global geographic grids should get a presentation projection");

        assert!(matches!(
            global_projection,
            rustwx_render::ProjectionSpec::Robinson {
                central_meridian_deg
            } if central_meridian_deg == 0.0
        ));
    }

    #[test]
    fn mercator_variant_uses_mercator_regionally_and_robinson_globally() {
        let conus_bounds = (-127.0, -66.0, 23.0, 51.5);
        let conus_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            conus_bounds,
            ProjectionPresentationVariant::Mercator,
        )
        .expect("CONUS geographic grids should get a presentation projection");

        assert!(matches!(
            conus_projection,
            rustwx_render::ProjectionSpec::Mercator { .. }
        ));

        let global_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            (-180.0, 179.999, -90.0, 90.0),
            ProjectionPresentationVariant::Mercator,
        )
        .expect("global geographic grids should get a presentation projection");

        assert!(matches!(
            global_projection,
            rustwx_render::ProjectionSpec::Robinson {
                central_meridian_deg
            } if central_meridian_deg == 0.0
        ));
    }

    #[test]
    fn rectangular_variant_uses_geographic_regionally_and_robinson_globally() {
        let europe_bounds = (-25.0, 45.0, 34.0, 72.0);
        let europe_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            europe_bounds,
            ProjectionPresentationVariant::RectangularGeographic,
        )
        .expect("regional geographic grids should get a presentation projection");
        assert_eq!(europe_projection, rustwx_render::ProjectionSpec::Geographic);

        let global_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            (-180.0, 179.999, -90.0, 90.0),
            ProjectionPresentationVariant::RectangularGeographic,
        )
        .expect("global geographic grids should get a presentation projection");
        assert!(matches!(
            global_projection,
            rustwx_render::ProjectionSpec::Robinson {
                central_meridian_deg
            } if central_meridian_deg == 0.0
        ));
    }

    #[test]
    fn adaptive_geographic_regions_use_presentation_projections() {
        assert!(matches!(
            presentation_projection_for_bounds(
                Some(&GridProjection::Geographic),
                (-180.0, 179.999, -90.0, 90.0),
                ProjectionPresentationVariant::Adaptive,
            )
            .unwrap(),
            rustwx_render::ProjectionSpec::Robinson { .. }
        ));

        let conus_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            (-127.0, -66.0, 23.0, 51.5),
            ProjectionPresentationVariant::Adaptive,
        )
        .unwrap();
        assert!(matches!(
            conus_projection,
            rustwx_render::ProjectionSpec::LambertConformal { .. }
        ));

        let europe_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            (-25.0, 45.0, 34.0, 72.0),
            ProjectionPresentationVariant::Adaptive,
        )
        .unwrap();
        assert!(matches!(
            europe_projection,
            rustwx_render::ProjectionSpec::LambertConformal { .. }
        ));

        let north_america_projection = presentation_projection_for_bounds(
            Some(&GridProjection::Geographic),
            (-170.0, -50.0, 5.0, 84.0),
            ProjectionPresentationVariant::Adaptive,
        )
        .unwrap();
        assert_eq!(
            north_america_projection,
            rustwx_render::ProjectionSpec::Geographic
        );

        assert!(matches!(
            presentation_projection_for_bounds(
                Some(&GridProjection::Geographic),
                (-180.0, 179.999, -90.0, -60.0),
                ProjectionPresentationVariant::Adaptive,
            )
            .unwrap(),
            rustwx_render::ProjectionSpec::PolarStereographic { .. }
        ));
    }

    #[test]
    fn rectangular_variant_expands_tall_bounds_to_target_aspect() {
        let bounds = (110.0, 180.0, -50.0, 0.0);
        let expanded = presentation_frame_bounds_for_grid(
            Some(&GridProjection::Geographic),
            bounds,
            ProjectionPresentationVariant::RectangularGeographic,
            16.0 / 9.0,
        );

        assert!((expanded.3 - expanded.2 - 50.0).abs() < 1.0e-6);
        assert!(
            longitude_bounds_span_deg(expanded) > longitude_bounds_span_deg(bounds),
            "expanded bounds should widen the crop for a 16:9 rectangular map"
        );
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
            variable_patterns: if should_attach_direct_idx_patterns(latest.source) {
                group.variable_patterns.clone()
            } else {
                Vec::new()
            },
            earth2_ensemble: request.earth2_ensemble,
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
    fn grouping_keeps_shared_prs_selector_union_under_structured_fetches() {
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
        assert!(
            groups[0]
                .variable_patterns
                .iter()
                .any(|pattern| pattern.contains("500 mb"))
        );
        assert!(
            groups[0]
                .variable_patterns
                .iter()
                .any(|pattern| pattern.contains("700 mb"))
        );
    }

    #[test]
    fn direct_fetch_request_strips_nomads_subset_patterns() {
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
    fn smoke_fetches_stay_on_hrrr_wrfnat_family() {
        let planned = plan_direct_recipes(
            ModelId::Hrrr,
            &[
                "smoke_pm25_native".to_string(),
                "2m_temperature_10m_winds".to_string(),
            ],
        )
        .unwrap();
        let request = sample_direct_request(ModelId::Hrrr);
        let groups = group_direct_fetches(&request, &planned);

        assert!(groups.iter().any(|group| group.product == "nat"));
        assert!(groups.iter().any(|group| group.product == "sfc"));

        let smoke_group = groups
            .iter()
            .find(|group| {
                group.selectors.contains(&FieldSelector::height_agl(
                    CanonicalField::SmokeMassDensity,
                    8,
                ))
            })
            .expect("expected a dedicated smoke wrfnat group");
        assert_eq!(smoke_group.product, "nat");
        assert_eq!(
            smoke_group.planned_family_aliases,
            std::collections::BTreeSet::from(["nat".to_string()])
        );
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
            earth2_ensemble: None,
        };
        assert_eq!(runtime.planned_product, "nat");
        assert_eq!(runtime.fetched_product, "sfc");
        assert_eq!(runtime.planned_family_aliases, vec!["nat".to_string()]);
        assert_eq!(runtime.resolved_source, SourceId::Nomads);
        assert!(runtime.resolved_url.contains("wrfsfc"));
    }

    #[test]
    fn nomads_hrrr_direct_fetch_requests_use_full_grib_files() {
        let request = sample_direct_request(ModelId::Hrrr);
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Nomads,
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
                "NOMADS production direct fetches should not carry .idx subset patterns"
            );
        }
    }

    #[test]
    fn aws_hrrr_direct_fetch_requests_keep_idx_patterns_for_fallback() {
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
            ],
        )
        .unwrap();
        let groups = group_direct_fetches(&request, &planned);
        let prs_group = groups
            .iter()
            .find(|group| group.product == "prs")
            .expect("expected a pressure fetch group");
        assert!(!prs_group.variable_patterns.is_empty());

        let fetch = build_direct_fetch_request(&request, &latest, 6, prs_group).unwrap();
        assert_eq!(fetch.source_override, Some(SourceId::Aws));
        assert_eq!(fetch.variable_patterns, prs_group.variable_patterns);
    }

    #[test]
    fn direct_execution_plan_strips_group_subset_patterns_for_nomads() {
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
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Nomads,
        };
        let plan = build_direct_execution_plan(&latest, 6, &groups);
        let prs_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.id.native_product == "prs")
            .expect("expected a planned HRRR pressure bundle");
        let patterns = prs_bundle
            .aliases
            .iter()
            .flat_map(|alias| alias.variable_patterns.iter())
            .collect::<Vec<_>>();
        assert!(
            patterns.is_empty(),
            "NOMADS production execution should use full GRIB files without .idx subset patterns"
        );
    }

    #[test]
    fn direct_execution_plan_keeps_group_subset_patterns_for_aws_fallback() {
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
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: rustwx_core::CycleSpec::new("20260414", 23).unwrap(),
            source: SourceId::Aws,
        };
        let plan = build_direct_execution_plan(&latest, 6, &groups);
        let prs_bundle = plan
            .bundles
            .iter()
            .find(|bundle| bundle.id.native_product == "prs")
            .expect("expected a planned HRRR pressure bundle");
        let patterns = prs_bundle
            .aliases
            .iter()
            .flat_map(|alias| alias.variable_patterns.iter())
            .collect::<Vec<_>>();
        assert!(patterns.iter().any(|pattern| pattern.contains("500 mb")));
        assert!(patterns.iter().any(|pattern| pattern.contains("700 mb")));
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
    fn nbm_pop_direct_recipe_groups_to_core_surface_product() {
        let planned =
            plan_direct_recipes(ModelId::Nbm, &["probability_of_precipitation".to_string()])
                .unwrap();
        let request = sample_direct_request(ModelId::Nbm);
        let groups = group_direct_fetches(&request, &planned);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].product, "core/co");
        assert!(groups[0].selectors.contains(&FieldSelector::surface(
            CanonicalField::ProbabilityOfPrecipitation
        )));
    }

    #[test]
    fn nbm_qmd_direct_recipes_are_explicit_only_for_all_supported() {
        let supported = supported_direct_recipe_slugs(ModelId::Nbm);
        assert!(!supported.iter().any(|slug| slug.starts_with("nbm_qmd_")));
        let sref_supported = supported_direct_recipe_slugs(ModelId::Sref);
        assert!(
            !sref_supported
                .iter()
                .any(|slug| slug.starts_with("sref_prob_"))
        );
        let gefs_supported = supported_direct_recipe_slugs(ModelId::Gefs);
        assert!(
            !gefs_supported
                .iter()
                .any(|slug| slug.starts_with("gefs_avg_") || slug.starts_with("gefs_spr_"))
        );
        let aigefs_supported = supported_direct_recipe_slugs(ModelId::Aigefs);
        assert!(
            !aigefs_supported
                .iter()
                .any(|slug| slug.starts_with("aigefs_spr_"))
        );
        let hgefs_supported = supported_direct_recipe_slugs(ModelId::Hgefs);
        assert!(
            !hgefs_supported
                .iter()
                .any(|slug| slug.starts_with("hgefs_spr_"))
        );
        let href_supported = supported_direct_recipe_slugs(ModelId::Href);
        assert!(!href_supported.iter().any(|slug| {
            slug.starts_with("href_sprd_")
                || slug.starts_with("href_prob_")
                || slug.starts_with("href_mean_")
        }));
        let refs_supported = supported_direct_recipe_slugs(ModelId::Refs);
        assert!(
            !refs_supported
                .iter()
                .any(|slug| { slug.starts_with("refs_sprd_") || slug.starts_with("refs_prob_") })
        );

        let planned =
            plan_direct_recipes(ModelId::Nbm, &["nbm_qmd_2m_temperature_p50".to_string()]).unwrap();
        assert_eq!(planned[0].plan.product, "qmd/co");

        let sref_planned = plan_direct_recipes(
            ModelId::Sref,
            &["sref_prob_2m_temperature_below_273k".to_string()],
        )
        .unwrap();
        assert_eq!(sref_planned[0].plan.product, "ensprod/pgrb212/prob_3hrly");

        let gefs_planned = plan_direct_recipes(
            ModelId::Gefs,
            &["gefs_spr_2m_temperature_stddev".to_string()],
        )
        .unwrap();
        assert_eq!(gefs_planned[0].plan.product, "pgrb2ap5/gespr");

        let aigefs_planned = plan_direct_recipes(
            ModelId::Aigefs,
            &["aigefs_spr_2m_temperature_stddev".to_string()],
        )
        .unwrap();
        assert_eq!(aigefs_planned[0].plan.product, "sfc/spr");

        let hgefs_planned = plan_direct_recipes(
            ModelId::Hgefs,
            &["hgefs_spr_2m_temperature_stddev".to_string()],
        )
        .unwrap();
        assert_eq!(hgefs_planned[0].plan.product, "sfc/spr");

        let href_planned =
            plan_direct_recipes(ModelId::Href, &["href_sprd_2m_temperature".to_string()]).unwrap();
        assert_eq!(href_planned[0].plan.product, "ensprod/conus/sprd");

        let href_prob_planned = plan_direct_recipes(
            ModelId::Href,
            &["href_prob_2m_temperature_below_273p15k".to_string()],
        )
        .unwrap();
        assert_eq!(href_prob_planned[0].plan.product, "ensprod/conus/prob");

        let href_mean_planned =
            plan_direct_recipes(ModelId::Href, &["href_mean_2m_temperature".to_string()]).unwrap();
        assert_eq!(href_mean_planned[0].plan.product, "ensprod/conus/mean");

        let refs_spread_planned =
            plan_direct_recipes(ModelId::Refs, &["refs_sprd_2m_temperature".to_string()]).unwrap();
        assert_eq!(refs_spread_planned[0].plan.product, "sprd-conus");

        let refs_prob_planned = plan_direct_recipes(
            ModelId::Refs,
            &["refs_prob_2m_temperature_below_273p15k".to_string()],
        )
        .unwrap();
        assert_eq!(refs_prob_planned[0].plan.product, "prob-conus");
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
        let converted_pressure =
            convert_filled_field_with_ensemble(pressure_recipe, &pressure_field, None);
        assert_eq!(converted_pressure.units, "hPa");
        assert_eq!(converted_pressure.values[0], 1000.0);

        let pwat_recipe = plot_recipe("precipitable_water").unwrap();
        let pwat_field = sample_selected_field(
            FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater),
            "kg/m^2",
            vec![25.4; 4],
        );
        let converted_pwat = convert_filled_field_with_ensemble(pwat_recipe, &pwat_field, None);
        assert_eq!(converted_pwat.units, "in");
        assert!((converted_pwat.values[0] - 1.0).abs() < 1.0e-6);

        let vis_recipe = plot_recipe("visibility").unwrap();
        let vis_field = sample_selected_field(
            FieldSelector::surface(CanonicalField::Visibility),
            "m",
            vec![1609.344; 4],
        );
        let converted_vis = convert_filled_field_with_ensemble(vis_recipe, &vis_field, None);
        assert_eq!(converted_vis.units, "mi");
        assert!((converted_vis.values[0] - 1.0).abs() < 1.0e-4);

        let vort_recipe = plot_recipe("500mb_absolute_vorticity_height_winds").unwrap();
        let vort_field = sample_selected_field(
            FieldSelector::isobaric(CanonicalField::AbsoluteVorticity, 500),
            "s^-1",
            vec![0.0002; 4],
        );
        let converted_vort = convert_filled_field_with_ensemble(vort_recipe, &vort_field, None);
        assert_eq!(converted_vort.units, "10^-5 s^-1");
        assert!((converted_vort.values[0] - 20.0).abs() < 1.0e-6);

        let temp_recipe = plot_recipe("2m_temperature").unwrap();
        let temp_field = sample_selected_field(
            FieldSelector::height_agl(CanonicalField::Temperature, 2),
            "K",
            vec![273.15; 4],
        );
        let converted_temp = convert_filled_field_with_ensemble(temp_recipe, &temp_field, None);
        assert_eq!(converted_temp.units, "degF");
        assert!((converted_temp.values[0] - 32.0).abs() < 1.0e-5);

        let upper_temp_recipe = plot_recipe("500mb_temperature_height_winds").unwrap();
        let upper_temp_field = sample_selected_field(
            FieldSelector::isobaric(CanonicalField::Temperature, 500),
            "K",
            vec![253.15; 4],
        );
        let converted_upper_temp =
            convert_filled_field_with_ensemble(upper_temp_recipe, &upper_temp_field, None);
        assert_eq!(converted_upper_temp.units, "degC");
        assert!((converted_upper_temp.values[0] + 20.0).abs() < 1.0e-5);

        let wind_speed_recipe = plot_recipe("nbm_qmd_10m_wind_speed_p50").unwrap();
        let wind_speed_field = sample_selected_field(
            FieldSelector::height_agl(CanonicalField::WindSpeed, 10).with_percentile(50),
            "m/s",
            vec![10.0; 4],
        );
        let converted_wind_speed =
            convert_filled_field_with_ensemble(wind_speed_recipe, &wind_speed_field, None);
        assert_eq!(converted_wind_speed.units, "kt");
        assert!((converted_wind_speed.values[0] - 19.438_445).abs() < 1.0e-5);
    }

    #[test]
    fn overlay_only_rule_only_catches_height_products() {
        assert!(should_render_overlay_only(
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
            false
        ));
        assert!(!should_render_overlay_only(
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
    fn direct_synoptic_contours_use_operational_emphasis() {
        let pressure = crate::plot_design::operational_contour_layer_for_values(
            FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel),
            &[100000.0, 100200.0, 100400.0, 100600.0],
        )
        .expect("pressure contour layer");
        assert_eq!(pressure.levels.first().copied(), Some(960.0));
        assert_eq!(pressure.width, 1);
        assert_eq!(pressure.major_every, Some(2));
        assert_eq!(pressure.major_width, Some(2));
        assert!(pressure.labels);
        assert!(pressure.show_extrema);
        assert_eq!(pressure.pattern, rustwx_render::ContourLinePattern::Solid);
        assert_eq!(pressure.data[0], 1000.0);

        let height = crate::plot_design::operational_contour_layer_for_values(
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
            &[5400.0, 5460.0, 5520.0, 5580.0],
        )
        .expect("height contour layer");
        assert_eq!(height.major_every, Some(2));
        assert_eq!(height.major_width, Some(2));
        assert!(height.labels);
        assert!(!height.show_extrema);
        assert_eq!(height.data[0], 540.0);
    }

    #[test]
    fn earth2_selector_suffixes_are_filename_friendly() {
        assert_eq!(
            earth2_filename_suffix(Some(Earth2EnsembleSelector::Member(3))),
            "_m3"
        );
        assert_eq!(
            earth2_filename_suffix(Some(Earth2EnsembleSelector::Statistic(
                Earth2EnsembleStat::Mean
            ))),
            "_mean"
        );
        assert_eq!(earth2_filename_suffix(None), "");
    }

    #[test]
    fn earth2_std_temperature_keeps_spread_units_and_scale() {
        let recipe = plot_recipe("2m_temperature").unwrap();
        let field = sample_selected_field(
            FieldSelector::height_agl(CanonicalField::Temperature, 2),
            "K",
            vec![0.0, 2.0, 4.0, 8.0],
        );
        let converted = convert_filled_field_with_ensemble(
            recipe,
            &field,
            Some(Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Std)),
        );
        assert_eq!(converted.units, "K");
        assert_eq!(converted.values, vec![0.0, 2.0, 4.0, 8.0]);

        let ColorScale::Discrete(scale) = scale_for_earth2_selector(
            recipe,
            field.selector,
            &converted.values,
            Some(Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Std)),
        ) else {
            panic!("expected discrete spread scale");
        };
        assert_eq!(scale.levels.first().copied(), Some(0.0));
        assert!(scale.levels.last().copied().unwrap_or(0.0) >= 8.0);
        assert_eq!(scale.extend, ExtendMode::Max);
    }

    #[test]
    fn qmd_stddev_temperature_keeps_spread_units_and_scale() {
        let recipe = plot_recipe("nbm_qmd_2m_temperature_stddev").unwrap();
        let field = sample_selected_field(
            FieldSelector::height_agl(CanonicalField::Temperature, 2)
                .with_ensemble_standard_deviation(),
            "K",
            vec![0.0, 1.5, 3.0, 6.0],
        );
        let converted = convert_filled_field_with_ensemble(recipe, &field, None);
        assert_eq!(converted.units, "K");
        assert_eq!(converted.values, vec![0.0, 1.5, 3.0, 6.0]);

        let ColorScale::Discrete(scale) =
            scale_for_earth2_selector(recipe, field.selector, &converted.values, None)
        else {
            panic!("expected discrete spread scale");
        };
        assert_eq!(scale.levels.first().copied(), Some(0.0));
        assert!(scale.levels.last().copied().unwrap_or(0.0) >= 6.0);
        assert_eq!(scale.extend, ExtendMode::Max);
    }

    #[test]
    fn earth2_std_min_max_suppress_companion_overlays() {
        assert!(earth2_suppresses_companion_overlays(Some(
            Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Std)
        )));
        assert!(earth2_suppresses_companion_overlays(Some(
            Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Min)
        )));
        assert!(earth2_suppresses_companion_overlays(Some(
            Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Max)
        )));
        assert!(!earth2_suppresses_companion_overlays(Some(
            Earth2EnsembleSelector::Statistic(Earth2EnsembleStat::Mean)
        )));
        assert!(!earth2_suppresses_companion_overlays(Some(
            Earth2EnsembleSelector::Member(0)
        )));
    }

    #[test]
    fn weather_uh_scale_uses_operational_levels_and_masks_negative_noise() {
        let recipe = plot_recipe("uh_2to5km").unwrap();
        let scale = scale_for_recipe(
            recipe,
            FieldSelector::height_layer_agl(CanonicalField::UpdraftHelicity, 2000, 5000),
        );
        let ColorScale::Discrete(discrete) = scale else {
            panic!("expected discrete UH scale");
        };
        assert_eq!(discrete.levels.first().copied(), Some(0.0));
        assert_eq!(discrete.levels.last().copied(), Some(400.0));
        assert_eq!(discrete.mask_below, Some(0.0));
    }

    #[test]
    fn reflectivity_scale_masks_no_return_values() {
        let recipe = plot_recipe("composite_reflectivity").unwrap();
        let scale = scale_for_recipe(
            recipe,
            FieldSelector::surface(CanonicalField::CompositeReflectivity),
        );
        let ColorScale::Discrete(discrete) = scale else {
            panic!("expected discrete reflectivity scale");
        };
        assert_eq!(discrete.levels.first().copied(), Some(10.0));
        assert_eq!(discrete.levels.last().copied(), Some(70.0));
        assert_eq!(discrete.extend, ExtendMode::Max);
        assert_eq!(discrete.mask_below, Some(10.0));
    }

    #[test]
    fn categorical_precip_scale_masks_false_flags() {
        let recipe = plot_recipe("categorical_snow").unwrap();
        let scale = scale_for_recipe(
            recipe,
            FieldSelector::surface(CanonicalField::CategoricalSnow),
        );
        let ColorScale::Discrete(discrete) = scale else {
            panic!("expected discrete categorical scale");
        };
        assert_eq!(discrete.levels, vec![0.0, 0.5, 1.0]);
        assert_eq!(discrete.extend, ExtendMode::Neither);
        assert_eq!(discrete.mask_below, Some(0.5));
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

    #[test]
    fn aifs_inference_direct_policy_uses_interpolated_raster_sampling() {
        let field = sample_selected_field(
            FieldSelector::height_agl(CanonicalField::Temperature, 2),
            "K",
            vec![290.0; 4],
        )
        .into_field2d();
        let mut request = MapRenderRequest::contour_only(field.into());
        assert_eq!(request.raster_sample_mode, RasterSampleMode::Linear);

        apply_source_raster_policy(SourceId::AifsInference, &mut request);

        assert_eq!(request.raster_sample_mode, RasterSampleMode::Linear);
    }
}
