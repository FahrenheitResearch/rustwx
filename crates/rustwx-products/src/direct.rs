use crate::derived::NativeContourRenderMode;
#[cfg(test)]
use rustwx_core::CanonicalField;
use rustwx_core::{FieldSelector, ModelId, SelectedField2D, SourceId};
#[cfg(test)]
use rustwx_io::earth2_archive::{Earth2EnsembleSelector, Earth2EnsembleStat};
use rustwx_models::{LatestRun, PlotRecipe, plot_recipe};
#[cfg(test)]
use rustwx_models::{PlotRecipeFetchMode, plot_recipe_fetch_plan};
use rustwx_render::{
    Color, ContourLayer, PanelGridLayout, PanelPadding, PngCompressionMode, PngWriteOptions,
    ProductVisualMode, ProjectedMap, RenderImageTiming, RenderStateTiming, WindBarbLayer,
    WindStreamlineLayer, draw_centered_text_line, render_panel_grid, save_png_profile_with_options,
    save_rgba_png_profile_with_options,
};
#[cfg(test)]
use rustwx_render::{ColorScale, ExtendMode, MapRenderRequest, RasterSampleMode};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::Instant;

use crate::custom_poi::apply_custom_poi_overlay;
use crate::publication::artifact_identity_from_path;
use crate::shared_context::{
    DomainSpec, ProjectedMapProvider, model_time_subtitle, source_subtitle,
};
use crate::source::direct_route_for_recipe_slug;

mod batch;
mod composite;
mod domain;
mod fetch;
mod planning;
mod projection;
mod query;
mod rendering;
mod titles;
mod types;
pub(crate) use batch::{
    prepare_direct_batch_from_loaded, run_direct_batch_from_loaded, run_direct_batch_from_prepared,
};
pub use batch::{run_direct_batch, run_hrrr_direct_batch};
use composite::{CompositePanelSpec, composite_panel_spec};
#[cfg(test)]
use domain::{
    DirectGridCrop, crop_for_direct_grid, crop_latlon_grid_for_direct,
    crop_selected_field_for_domain, is_global_scale_domain, longitude_bounds_span_deg,
    point_in_geographic_bounds,
};
use domain::{
    crop_bounds_for_direct_request, crop_direct_fields_for_domain, render_bounds_for_direct_field,
};
use fetch::{extract_direct_fetch_group_from_loaded, find_loaded_bytes_for_group};
pub use planning::{FetchGroup, supported_direct_recipe_slugs};
use planning::{
    PlannedDirectRecipe, canonical_fetch_product_for_selectors, group_direct_fetches,
    plan_direct_recipes,
};
#[cfg(test)]
use planning::{canonical_fetch_product, should_attach_direct_idx_patterns};
use projection::direct_map_frame_aspect_ratio;
pub(crate) use projection::inverse_raster_projection_for_grid;
#[cfg(test)]
use projection::{
    PIVOTAL_CONUS_CENTRAL_MERIDIAN_DEG, PIVOTAL_CONUS_REFERENCE_LATITUDE_DEG,
    PIVOTAL_CONUS_STANDARD_PARALLEL_1_DEG, PIVOTAL_CONUS_STANDARD_PARALLEL_2_DEG,
    ProjectionPresentationVariant, center_longitude_for_bounds,
    full_domain_projected_frame_default, inverse_raster_clip_bounds,
    presentation_frame_bounds_for_grid, presentation_projection_for_bounds,
    reference_latitude_for_projection_variant,
};
pub use projection::{
    build_projected_map, build_projected_map_with_projection,
    model_data_domain_frame_for_projection,
};
pub(crate) use query::{
    build_direct_sampled_execution_plan, direct_component_slug,
    load_direct_sampled_fields_from_latest, load_direct_sampled_fields_from_loaded,
    load_single_direct_sampled_field_from_latest, required_direct_fetch_products,
};
#[cfg(test)]
use rendering::{
    StreamlineSetting, barb_target_columns_rows, convert_filled_field_with_ensemble,
    render_filled_field, scale_for_earth2_selector, scale_for_recipe, streamlines_enabled_for_grid,
};
use rendering::{
    apply_source_raster_policy, build_render_request, earth2_filename_suffix,
    earth2_suppresses_companion_overlays, sanitize_output_suffix, should_render_overlay_only,
    visual_mode_for_direct_recipe,
};
#[cfg(test)]
use titles::{apply_native_stat_title_prefix, native_stat_label_for_request};
use titles::{direct_panel_title_for_request, direct_title_for_planned_product};
pub(crate) use types::PreparedDirectBatch;
pub use types::{
    DirectBatchReport, DirectBatchRequest, DirectFetchRuntimeInfo, DirectFetchTiming,
    DirectRecipeBlocker, DirectRecipeTiming, DirectRenderedRecipe, HrrrDirectBatchReport,
    HrrrDirectBatchRequest, HrrrDirectFetchRuntimeInfo, HrrrDirectFetchTiming,
    HrrrDirectRecipeBlocker, HrrrDirectRecipeTiming, HrrrDirectRenderedRecipe,
};
use types::{DirectRequestBuildTiming, OUTPUT_HEIGHT, OUTPUT_WIDTH};
fn direct_data_layer_draw_ms(image_timing: &RenderImageTiming) -> u128 {
    image_timing.polygon_fill_ms
        + image_timing.projected_pixel_ms
        + image_timing.rasterize_ms
        + image_timing.raster_blit_ms
}

fn direct_overlay_draw_ms(image_timing: &RenderImageTiming) -> u128 {
    image_timing.linework_ms + image_timing.contour_ms + image_timing.barb_ms
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

#[cfg(test)]
mod tests {
    use super::planning::{
        build_direct_execution_plan, partition_recipes_by_selector_availability,
    };
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
    fn local_wrf_netcdf_titles_omit_gdex_dataset_token() {
        let mut request = sample_direct_request(ModelId::WrfGdex);
        request.subtitle_right_override = Some("source: local WRF NetCDF".to_string());

        let title = direct_title_for_planned_product(
            &request,
            "d612005-hist2d",
            "Composite Reflectivity / UH",
        );

        assert!(title.starts_with("Composite Reflectivity / UH"), "{title}");
        assert!(!title.contains("d612005"), "{title}");
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

    fn periodic_global_grid() -> rustwx_core::LatLonGrid {
        let nx = 36usize;
        let ny = 3usize;
        let mut lat = Vec::with_capacity(nx * ny);
        let mut lon = Vec::with_capacity(nx * ny);
        for row_lat in [-10.0_f32, 0.0, 10.0] {
            for x in 0..nx {
                lat.push(row_lat);
                lon.push((x as f32) * 10.0);
            }
        }
        rustwx_core::LatLonGrid::new(rustwx_core::GridShape::new(nx, ny).unwrap(), lat, lon)
            .unwrap()
    }

    #[test]
    fn periodic_global_crop_wraps_regional_domains_across_greenwich() {
        let grid = periodic_global_grid();

        let crop = crop_for_direct_grid(&grid, (-12.0, 12.0, -2.0, 2.0), 1, true)
            .unwrap()
            .expect("regional Greenwich crop should trim the periodic axis");

        assert_eq!(
            crop,
            DirectGridCrop::Wrapped {
                x_start: 34,
                x_end: 3,
                y_start: 0,
                y_end: 3,
            }
        );

        let cropped = crop_latlon_grid_for_direct(&grid, crop).unwrap();
        assert_eq!(cropped.shape.nx, 5);
        assert_eq!(cropped.shape.ny, 3);
        assert_eq!(&cropped.lon_deg[0..5], &[340.0, 350.0, 0.0, 10.0, 20.0]);
    }

    #[test]
    fn periodic_global_direct_crop_normalizes_longitudes_near_domain_center() {
        let grid = periodic_global_grid();
        let values = (0..grid.shape.nx * grid.shape.ny)
            .map(|value| value as f32)
            .collect::<Vec<_>>();
        let field = SelectedField2D::new(
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 300),
            "m",
            grid,
            values,
        )
        .unwrap();

        let cropped =
            crop_selected_field_for_domain(&field, (-12.0, 12.0, -2.0, 2.0), 1, true).unwrap();

        assert_eq!(cropped.grid.shape.nx, 5);
        assert_eq!(cropped.grid.shape.ny, 3);
        assert_eq!(
            &cropped.grid.lon_deg[0..5],
            &[-20.0, -10.0, 0.0, 10.0, 20.0]
        );
    }

    #[test]
    fn streamline_auto_mode_disables_regular_latlon_grids() {
        assert!(!streamlines_enabled_for_grid(
            StreamlineSetting::Auto,
            &periodic_global_grid()
        ));
        assert!(streamlines_enabled_for_grid(
            StreamlineSetting::Enabled,
            &periodic_global_grid()
        ));
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
