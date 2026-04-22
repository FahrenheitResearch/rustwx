use crate::benchmark::default_benchmark_products;
use crate::cross_section_proof::{
    PreparedPressureCrossSectionScene, PressureCrossSectionRequest,
    prepare_pressure_cross_section_scene,
};
use image::RgbaImage;
use rustwx_core::{ModelId, SourceId};
use rustwx_cross_section::{RenderedCrossSection, render_scalar_section_profile};
use rustwx_products::artifact_bundle::{
    ArtifactBundleArtifact, ArtifactBundleAuxiliaryOutput, ArtifactBundleManifest,
    ArtifactBundleRole, ArtifactBundleRunContext, default_artifact_bundle_manifest_path,
    publish_artifact_bundle_manifest,
};
use rustwx_products::cache::ensure_dir;
use rustwx_products::cross_section::PressureCrossSectionFacts;
use rustwx_products::derived::{
    DerivedLiveArtifactBuildTiming, NativeContourRenderMode,
    build_hrrr_live_derived_artifact_profiled,
};
use rustwx_products::direct::build_projected_map_with_projection;
use rustwx_products::gridded::load_model_timestep_from_parts;
use rustwx_products::shared_context::{DomainSpec, ProjectedMap};
use rustwx_render::{
    PngCompressionMode, PngWriteOptions, RenderSaveTiming, save_png_profile_with_options,
    save_rgba_png_profile_with_options,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct WeatherNativeProfileRequest {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub map_products: Vec<String>,
    pub cross_section_requests: Vec<PressureCrossSectionRequest>,
    pub runs: usize,
    pub output_width: u32,
    pub output_height: u32,
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherNativeProfileRequestSummary {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub map_products: Vec<String>,
    pub cross_section_products: Vec<String>,
    pub runs: usize,
    pub output_width: u32,
    pub output_height: u32,
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileStageTiming {
    pub data_load_ms: u128,
    pub projected_map_build_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentTimingStat {
    pub component: String,
    pub median_ms: u128,
    pub mean_ms: f64,
    pub share_of_total_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapProfileRunRecord {
    pub total_ms: u128,
    pub build_timing: DerivedLiveArtifactBuildTiming,
    pub render_save_timing: RenderSaveTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapRenderModeProfileSummary {
    pub mode: String,
    pub run_count: usize,
    pub output_png: PathBuf,
    pub runs: Vec<MapProfileRunRecord>,
    pub median_total_ms: u128,
    pub component_hotspots: Vec<ComponentTimingStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapProfileCaseSummary {
    pub recipe_slug: String,
    pub title: String,
    pub units: String,
    pub native: MapRenderModeProfileSummary,
    pub legacy: MapRenderModeProfileSummary,
    pub native_speedup_over_legacy: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSectionProfileRunRecord {
    pub total_ms: u128,
    pub path_layout_ms: u128,
    pub artifact_build_ms: u128,
    pub artifact_stencil_build_ms: u128,
    pub artifact_terrain_profile_ms: u128,
    pub artifact_pressure_sampling_ms: u128,
    pub artifact_product_compute_ms: u128,
    pub artifact_metadata_ms: u128,
    pub artifact_section_assembly_ms: u128,
    pub artifact_wind_overlay_ms: u128,
    pub render_request_build_ms: u128,
    pub plot_layout_ms: u128,
    pub terrain_mask_ms: u128,
    pub scene_resolve_ms: u128,
    pub canvas_init_ms: u128,
    pub scalar_field_ms: u128,
    pub grid_ms: u128,
    pub contour_topology_ms: u128,
    pub contour_draw_ms: u128,
    pub wind_overlay_ms: u128,
    pub terrain_ms: u128,
    pub axes_ms: u128,
    pub header_ms: u128,
    pub footer_ms: u128,
    pub colorbar_ms: u128,
    pub rgba_wrap_ms: u128,
    pub png_encode_ms: u128,
    pub file_write_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSectionProfileSummary {
    pub route_slug: String,
    pub route_label: String,
    pub route_distance_km: f64,
    pub product_slug: String,
    pub product_label: String,
    pub palette_slug: String,
    pub facts: PressureCrossSectionFacts,
    pub run_count: usize,
    pub output_png: PathBuf,
    pub runs: Vec<CrossSectionProfileRunRecord>,
    pub median_total_ms: u128,
    pub component_hotspots: Vec<ComponentTimingStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherNativeProfileSummary {
    pub runner: &'static str,
    pub model: ModelId,
    pub request: WeatherNativeProfileRequestSummary,
    pub stage_timing: ProfileStageTiming,
    pub map_cases: Vec<MapProfileCaseSummary>,
    pub cross_sections: Vec<CrossSectionProfileSummary>,
    pub summary_json: PathBuf,
    pub summary_markdown: PathBuf,
    pub bundle_manifest: PathBuf,
}

impl WeatherNativeProfileRequest {
    pub fn normalized_map_products(&self) -> Vec<String> {
        if self.map_products.is_empty() {
            default_benchmark_products()
        } else {
            self.map_products.clone()
        }
    }
}

pub fn default_cross_section_profile_products() -> Vec<String> {
    vec![
        "temperature".to_string(),
        "rh".to_string(),
        "theta_e".to_string(),
        "wind_speed".to_string(),
    ]
}

pub fn run_weather_native_profile(
    request: &WeatherNativeProfileRequest,
) -> Result<WeatherNativeProfileSummary, Box<dyn Error>> {
    ensure_dir(&request.out_dir)?;
    let profile_root = request.out_dir.join("profile");
    ensure_dir(&profile_root)?;

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

    let map_products = request.normalized_map_products();
    let mut map_cases = Vec::with_capacity(map_products.len());
    for recipe_slug in &map_products {
        map_cases.push(run_map_profile_case(
            request,
            &profile_root,
            recipe_slug,
            &loaded.surface_decode.value,
            &loaded.pressure_decode.value,
            &loaded.grid,
            &projected,
        )?);
    }

    let mut cross_sections = Vec::with_capacity(request.cross_section_requests.len());
    for cross_request in &request.cross_section_requests {
        cross_sections.push(run_cross_section_profile_case(
            request,
            &profile_root,
            cross_request,
            &loaded,
        )?);
    }

    let profile_stem = format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_weather_native_profile",
        request.date_yyyymmdd, request.cycle_utc, request.forecast_hour, request.domain.slug
    );
    let summary_json = profile_root.join(format!("{profile_stem}_summary.json"));
    let summary_markdown = profile_root.join(format!("{profile_stem}_summary.md"));
    let bundle_manifest = default_artifact_bundle_manifest_path(&profile_root, &profile_stem);

    let summary = WeatherNativeProfileSummary {
        runner: "weather_native_profile",
        model: ModelId::Hrrr,
        request: WeatherNativeProfileRequestSummary {
            date_yyyymmdd: request.date_yyyymmdd.clone(),
            cycle_utc: request.cycle_utc,
            forecast_hour: request.forecast_hour,
            source: request.source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            map_products,
            cross_section_products: request
                .cross_section_requests
                .iter()
                .map(|item| item.product.slug().to_string())
                .collect(),
            runs: request.runs.max(1),
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
        },
        stage_timing: ProfileStageTiming {
            data_load_ms,
            projected_map_build_ms,
        },
        map_cases,
        cross_sections,
        summary_json: relative_path(&request.out_dir, &summary_json),
        summary_markdown: relative_path(&request.out_dir, &summary_markdown),
        bundle_manifest: relative_path(&request.out_dir, &bundle_manifest),
    };
    fs::write(&summary_json, serde_json::to_vec_pretty(&summary)?)?;
    fs::write(&summary_markdown, render_summary_markdown(&summary))?;
    publish_artifact_bundle_manifest(
        &bundle_manifest,
        &build_profile_bundle_manifest(request, &summary, &summary_json, &summary_markdown)?,
    )?;
    Ok(summary)
}

fn build_profile_bundle_manifest(
    request: &WeatherNativeProfileRequest,
    summary: &WeatherNativeProfileSummary,
    summary_json: &Path,
    summary_markdown: &Path,
) -> Result<ArtifactBundleManifest, Box<dyn Error>> {
    let mut manifest = ArtifactBundleManifest::new(
        "weather_native_profile",
        format!(
            "{} {}z f{:03} {}",
            summary.request.date_yyyymmdd.as_str(),
            summary.request.cycle_utc,
            summary.request.forecast_hour,
            summary.request.domain.slug.as_str()
        ),
        &request.out_dir,
    )
    .with_build_provenance(
        rustwx_products::publication_provenance::capture_default_build_provenance(),
    )
    .with_run_context(
        ArtifactBundleRunContext::new(summary.runner)
            .with_model(summary.model.to_string())
            .with_cycle_metadata(
                summary.request.date_yyyymmdd.clone(),
                summary.request.cycle_utc,
                summary.request.forecast_hour,
            )
            .with_source(summary.request.source.to_string())
            .with_domain_slug(summary.request.domain.slug.clone()),
    );
    manifest.insert_metadata_value("stage_timing", serde_json::to_value(&summary.stage_timing)?);
    manifest.insert_metadata_value("map_case_count", json!(summary.map_cases.len()));
    manifest.insert_metadata_value("cross_section_count", json!(summary.cross_sections.len()));
    manifest.insert_metadata_value(
        "map_products",
        serde_json::to_value(&summary.request.map_products)?,
    );
    manifest.insert_metadata_value(
        "cross_section_products",
        serde_json::to_value(&summary.request.cross_section_products)?,
    );
    manifest.insert_metadata_value(
        "output_size",
        json!({
            "width": summary.request.output_width,
            "height": summary.request.output_height,
        }),
    );

    let summary_json_aux = ArtifactBundleAuxiliaryOutput::new(
        "profile_summary_json",
        relative_path(&request.out_dir, summary_json),
    )
    .with_media_type("application/json");
    let summary_markdown_aux = ArtifactBundleAuxiliaryOutput::new(
        "profile_summary_markdown",
        relative_path(&request.out_dir, summary_markdown),
    )
    .with_media_type("text/markdown");

    let mut summary_json_artifact = ArtifactBundleArtifact::from_existing_path(
        "profile_summary_json",
        ArtifactBundleRole::Stats,
        "application/json",
        &request.out_dir,
        summary_json,
    )?;
    summary_json_artifact.insert_metadata_value("flow", json!("weather_native_profile"));
    summary_json_artifact.insert_stat_value("map_case_count", json!(summary.map_cases.len()));
    summary_json_artifact
        .insert_stat_value("cross_section_count", json!(summary.cross_sections.len()));
    manifest.push_artifact(summary_json_artifact);

    let mut summary_markdown_artifact = ArtifactBundleArtifact::from_existing_path(
        "profile_summary_markdown",
        ArtifactBundleRole::Metadata,
        "text/markdown",
        &request.out_dir,
        summary_markdown,
    )?;
    summary_markdown_artifact.insert_metadata_value("flow", json!("weather_native_profile"));
    manifest.push_artifact(summary_markdown_artifact);

    for case in &summary.map_cases {
        manifest.push_artifact(build_map_profile_bundle_artifact(
            &request.out_dir,
            case,
            &case.native,
            "native",
            Some(case.native_speedup_over_legacy),
            &summary_json_aux,
            &summary_markdown_aux,
        )?);
        manifest.push_artifact(build_map_profile_bundle_artifact(
            &request.out_dir,
            case,
            &case.legacy,
            "legacy",
            None,
            &summary_json_aux,
            &summary_markdown_aux,
        )?);
    }

    for case in &summary.cross_sections {
        manifest.push_artifact(build_cross_section_profile_bundle_artifact(
            &request.out_dir,
            case,
            &summary_json_aux,
            &summary_markdown_aux,
        )?);
    }

    Ok(manifest)
}

fn run_map_profile_case(
    request: &WeatherNativeProfileRequest,
    profile_root: &Path,
    recipe_slug: &str,
    surface: &rustwx_products::gridded::SurfaceFields,
    pressure: &rustwx_products::gridded::PressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
) -> Result<MapProfileCaseSummary, Box<dyn Error>> {
    let native = profile_map_mode(
        request,
        profile_root,
        recipe_slug,
        surface,
        pressure,
        grid,
        projected,
        NativeContourRenderMode::Automatic,
        "native",
    )?;
    let legacy = profile_map_mode(
        request,
        profile_root,
        recipe_slug,
        surface,
        pressure,
        grid,
        projected,
        NativeContourRenderMode::LegacyRaster,
        "legacy",
    )?;
    let native_speedup_over_legacy = ratio(
        legacy.2.median_total_ms as f64,
        native.2.median_total_ms as f64,
    );
    Ok(MapProfileCaseSummary {
        recipe_slug: recipe_slug.to_string(),
        title: native.0.clone(),
        units: native.1.clone(),
        native: native.2,
        legacy: legacy.2.clone(),
        native_speedup_over_legacy,
    })
}

fn profile_map_mode(
    request: &WeatherNativeProfileRequest,
    profile_root: &Path,
    recipe_slug: &str,
    surface: &rustwx_products::gridded::SurfaceFields,
    pressure: &rustwx_products::gridded::PressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    contour_mode: NativeContourRenderMode,
    mode_slug: &str,
) -> Result<(String, String, MapRenderModeProfileSummary), Box<dyn Error>> {
    let run_count = request.runs.max(1);
    let output_png = profile_root.join(format!("{recipe_slug}_rust_{mode_slug}_profile.png"));
    let png_options = PngWriteOptions {
        compression: request.png_compression,
    };
    let mut title = None;
    let mut units = None;
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        let run_start = Instant::now();
        let profiled = build_hrrr_live_derived_artifact_profiled(
            recipe_slug,
            surface,
            pressure,
            grid,
            projected,
            request.domain.bounds,
            &request.date_yyyymmdd,
            request.cycle_utc,
            request.forecast_hour,
            request.source,
            contour_mode,
        )?;
        title.get_or_insert_with(|| profiled.artifact.title.clone());
        units.get_or_insert_with(|| profiled.artifact.field.units.clone());
        let render_save_timing =
            save_png_profile_with_options(&profiled.artifact.request, &output_png, &png_options)?;
        runs.push(MapProfileRunRecord {
            total_ms: run_start.elapsed().as_millis(),
            build_timing: profiled.timing,
            render_save_timing,
        });
    }

    let median_total_ms = median_u128(runs.iter().map(|run| run.total_ms));
    let hotspots = component_stats(&runs, median_total_ms, |run| {
        let state = &run.render_save_timing.state_timing;
        let image = &run.render_save_timing.png_timing.image_timing;
        vec![
            ("compute_fields", run.build_timing.compute_fields_ms),
            ("request_base_build", run.build_timing.request_base_build_ms),
            (
                "native_contour_projected_points",
                run.build_timing.native_contour_projected_points_ms,
            ),
            (
                "native_contour_scalar_field",
                run.build_timing.native_contour_scalar_field_ms,
            ),
            (
                "native_contour_fill_topology",
                run.build_timing.native_contour_fill_topology_ms,
            ),
            (
                "native_contour_fill_geometry",
                run.build_timing.native_contour_fill_geometry_ms,
            ),
            (
                "native_contour_line_topology",
                run.build_timing.native_contour_line_topology_ms,
            ),
            (
                "native_contour_line_geometry",
                run.build_timing.native_contour_line_geometry_ms,
            ),
            ("wind_overlay_build", run.build_timing.wind_overlay_build_ms),
            ("validate", state.validate_ms),
            ("data_buffer", state.data_buffer_ms),
            ("projected_grid", state.projected_grid_ms),
            ("projected_lines", state.projected_lines_ms),
            ("projected_polygons", state.projected_polygons_ms),
            ("contour_prep", state.contour_prep_ms),
            ("barb_prep", state.barb_prep_ms),
            ("layout", image.layout_ms),
            ("background", image.background_ms),
            ("polygon_fill", image.polygon_fill_ms),
            ("projected_pixel", image.projected_pixel_ms),
            ("rasterize", image.rasterize_ms),
            ("raster_blit", image.raster_blit_ms),
            ("linework", image.linework_ms),
            ("contour_draw", image.contour_ms),
            ("barb_draw", image.barb_ms),
            ("outside_frame_clear", image.outside_frame_clear_ms),
            ("chrome", image.chrome_ms),
            ("colorbar", image.colorbar_ms),
            ("downsample", image.downsample_ms),
            ("postprocess", image.postprocess_ms),
            (
                "png_encode",
                run.render_save_timing.png_timing.png_encode_ms,
            ),
            ("file_write", run.render_save_timing.file_write_ms),
        ]
    });

    Ok((
        title.unwrap_or_else(|| recipe_slug.to_string()),
        units.unwrap_or_default(),
        MapRenderModeProfileSummary {
            mode: mode_slug.to_string(),
            run_count,
            output_png: relative_path(&request.out_dir, &output_png),
            runs,
            median_total_ms,
            component_hotspots: hotspots,
        },
    ))
}

fn run_cross_section_profile_case(
    request: &WeatherNativeProfileRequest,
    profile_root: &Path,
    cross_request: &PressureCrossSectionRequest,
    loaded: &rustwx_products::gridded::LoadedModelTimestep,
) -> Result<CrossSectionProfileSummary, Box<dyn Error>> {
    let run_count = request.runs.max(1);
    let mut scene_info = None::<PreparedPressureCrossSectionScene>;
    let mut runs = Vec::with_capacity(run_count);
    let png_options = PngWriteOptions {
        compression: request.png_compression,
    };
    let mut output_png = None::<PathBuf>;

    for _ in 0..run_count {
        let run_start = Instant::now();
        let scene = prepare_pressure_cross_section_scene(cross_request, loaded)?;
        let output_path = profile_root.join(format!(
            "{}_{}_cross_section_profile.png",
            scene.route_slug,
            cross_request.product.slug()
        ));
        let (rendered, render_timing) =
            render_scalar_section_profile(&scene.artifact.section, &scene.render_request)?;
        let rgba_wrap_start = Instant::now();
        let image = rendered_cross_section_to_image(&rendered)?;
        let rgba_wrap_ms = rgba_wrap_start.elapsed().as_millis();
        let png_timing = save_rgba_png_profile_with_options(&image, &output_path, &png_options)?;

        output_png = Some(output_path);
        if scene_info.is_none() {
            scene_info = Some(scene.clone());
        }
        runs.push(CrossSectionProfileRunRecord {
            total_ms: run_start.elapsed().as_millis(),
            path_layout_ms: scene.timing.path_layout_ms,
            artifact_build_ms: scene.timing.artifact_build_ms,
            artifact_stencil_build_ms: scene.timing.artifact_stencil_build_ms,
            artifact_terrain_profile_ms: scene.timing.artifact_terrain_profile_ms,
            artifact_pressure_sampling_ms: scene.timing.artifact_pressure_sampling_ms,
            artifact_product_compute_ms: scene.timing.artifact_product_compute_ms,
            artifact_metadata_ms: scene.timing.artifact_metadata_ms,
            artifact_section_assembly_ms: scene.timing.artifact_section_assembly_ms,
            artifact_wind_overlay_ms: scene.timing.artifact_wind_overlay_ms,
            render_request_build_ms: scene.timing.render_request_build_ms,
            plot_layout_ms: render_timing.plot_layout_ms,
            terrain_mask_ms: render_timing.terrain_mask_ms,
            scene_resolve_ms: render_timing.scene_resolve_ms,
            canvas_init_ms: render_timing.canvas_init_ms,
            scalar_field_ms: render_timing.scalar_field_ms,
            grid_ms: render_timing.grid_ms,
            contour_topology_ms: render_timing.contour_topology_ms,
            contour_draw_ms: render_timing.contour_draw_ms,
            wind_overlay_ms: render_timing.wind_overlay_ms,
            terrain_ms: render_timing.terrain_ms,
            axes_ms: render_timing.axes_ms,
            header_ms: render_timing.header_ms,
            footer_ms: render_timing.footer_ms,
            colorbar_ms: render_timing.colorbar_ms,
            rgba_wrap_ms,
            png_encode_ms: png_timing.png_timing.png_encode_ms,
            file_write_ms: png_timing.file_write_ms,
        });
    }

    let scene = scene_info.ok_or("cross-section profiler recorded no scene metadata")?;
    let output_png = output_png.ok_or("cross-section profiler recorded no output path")?;
    let median_total_ms = median_u128(runs.iter().map(|run| run.total_ms));
    let hotspots = component_stats(&runs, median_total_ms, |run| {
        vec![
            ("path_layout", run.path_layout_ms),
            ("artifact_stencil_build", run.artifact_stencil_build_ms),
            ("artifact_terrain_profile", run.artifact_terrain_profile_ms),
            (
                "artifact_pressure_sampling",
                run.artifact_pressure_sampling_ms,
            ),
            ("artifact_product_compute", run.artifact_product_compute_ms),
            ("artifact_metadata", run.artifact_metadata_ms),
            (
                "artifact_section_assembly",
                run.artifact_section_assembly_ms,
            ),
            ("artifact_wind_overlay", run.artifact_wind_overlay_ms),
            ("render_request_build", run.render_request_build_ms),
            ("plot_layout", run.plot_layout_ms),
            ("terrain_mask", run.terrain_mask_ms),
            ("scene_resolve", run.scene_resolve_ms),
            ("canvas_init", run.canvas_init_ms),
            ("scalar_field", run.scalar_field_ms),
            ("grid", run.grid_ms),
            ("contour_topology", run.contour_topology_ms),
            ("contour_draw", run.contour_draw_ms),
            ("wind_overlay", run.wind_overlay_ms),
            ("terrain", run.terrain_ms),
            ("axes", run.axes_ms),
            ("header", run.header_ms),
            ("footer", run.footer_ms),
            ("colorbar", run.colorbar_ms),
            ("rgba_wrap", run.rgba_wrap_ms),
            ("png_encode", run.png_encode_ms),
            ("file_write", run.file_write_ms),
        ]
    });

    Ok(CrossSectionProfileSummary {
        route_slug: scene.route_slug,
        route_label: scene.route_label,
        route_distance_km: scene.route_distance_km,
        product_slug: cross_request.product.slug().to_string(),
        product_label: cross_request.product.display_name().to_string(),
        palette_slug: scene.palette_slug,
        facts: scene.facts,
        run_count,
        output_png: relative_path(&request.out_dir, &output_png),
        runs,
        median_total_ms,
        component_hotspots: hotspots,
    })
}

fn rendered_cross_section_to_image(
    rendered: &RenderedCrossSection,
) -> Result<RgbaImage, Box<dyn Error>> {
    RgbaImage::from_raw(
        rendered.width(),
        rendered.height(),
        rendered.rgba().to_vec(),
    )
    .ok_or_else(|| "cross-section renderer returned an invalid RGBA buffer length".into())
}

fn component_stats<T>(
    runs: &[T],
    median_total_ms: u128,
    component_fn: impl Fn(&T) -> Vec<(&'static str, u128)>,
) -> Vec<ComponentTimingStat> {
    let mut samples = BTreeMap::<String, Vec<u128>>::new();
    for run in runs {
        for (name, value) in component_fn(run) {
            samples.entry(name.to_string()).or_default().push(value);
        }
    }
    let mut stats = samples
        .into_iter()
        .map(|(component, values)| {
            let median_ms = median_u128(values.iter().copied());
            let mean_ms = mean_u128(&values);
            let share_of_total_pct = if median_total_ms > 0 {
                (median_ms as f64 / median_total_ms as f64) * 100.0
            } else {
                0.0
            };
            ComponentTimingStat {
                component,
                median_ms,
                mean_ms,
                share_of_total_pct,
            }
        })
        .filter(|item| item.median_ms > 0)
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        right
            .median_ms
            .cmp(&left.median_ms)
            .then_with(|| left.component.cmp(&right.component))
    });
    stats
}

fn median_u128(values: impl IntoIterator<Item = u128>) -> u128 {
    let mut values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

fn mean_u128(values: &[u128]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let sum = values.iter().copied().sum::<u128>() as f64;
    sum / values.len() as f64
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if numerator.is_finite() && denominator.is_finite() && denominator > 0.0 {
        numerator / denominator
    } else {
        f64::NAN
    }
}

fn relative_path(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| path.to_path_buf())
}

fn build_map_profile_bundle_artifact(
    output_root: &Path,
    case: &MapProfileCaseSummary,
    mode: &MapRenderModeProfileSummary,
    mode_slug: &str,
    speedup_over_legacy: Option<f64>,
    summary_json_aux: &ArtifactBundleAuxiliaryOutput,
    summary_markdown_aux: &ArtifactBundleAuxiliaryOutput,
) -> Result<ArtifactBundleArtifact, Box<dyn Error>> {
    let output_path = output_root.join(&mode.output_png);
    let mut artifact = ArtifactBundleArtifact::from_existing_path(
        format!("map:{}:{mode_slug}", case.recipe_slug),
        ArtifactBundleRole::PrimaryImage,
        "image/png",
        output_root,
        &output_path,
    )?;
    artifact.insert_metadata_value("flow", json!("map_profile"));
    artifact.insert_metadata_value("recipe_slug", json!(case.recipe_slug.as_str()));
    artifact.insert_metadata_value("title", json!(case.title.as_str()));
    artifact.insert_metadata_value("units", json!(case.units.as_str()));
    artifact.insert_metadata_value("render_mode", json!(mode.mode.as_str()));
    artifact.insert_stat_value("run_count", json!(mode.run_count));
    artifact.insert_stat_value("median_total_ms", json!(mode.median_total_ms));
    artifact.insert_stat_value(
        "component_hotspots",
        serde_json::to_value(&mode.component_hotspots)?,
    );
    if let Some(speedup) = speedup_over_legacy {
        artifact.insert_stat_value("speedup_over_legacy", json!(speedup));
    }
    artifact.push_auxiliary_output(summary_json_aux.clone());
    artifact.push_auxiliary_output(summary_markdown_aux.clone());
    Ok(artifact)
}

fn build_cross_section_profile_bundle_artifact(
    output_root: &Path,
    case: &CrossSectionProfileSummary,
    summary_json_aux: &ArtifactBundleAuxiliaryOutput,
    summary_markdown_aux: &ArtifactBundleAuxiliaryOutput,
) -> Result<ArtifactBundleArtifact, Box<dyn Error>> {
    let output_path = output_root.join(&case.output_png);
    let mut artifact = ArtifactBundleArtifact::from_existing_path(
        format!("cross_section:{}:{}", case.route_slug, case.product_slug),
        ArtifactBundleRole::PrimaryImage,
        "image/png",
        output_root,
        &output_path,
    )?;
    artifact.insert_metadata_value("flow", json!("cross_section_profile"));
    artifact.insert_metadata_value("route_slug", json!(case.route_slug.as_str()));
    artifact.insert_metadata_value("route_label", json!(case.route_label.as_str()));
    artifact.insert_metadata_value("product_slug", json!(case.product_slug.as_str()));
    artifact.insert_metadata_value("product_label", json!(case.product_label.as_str()));
    artifact.insert_metadata_value("palette_slug", json!(case.palette_slug.as_str()));
    artifact.insert_stat_value("run_count", json!(case.run_count));
    artifact.insert_stat_value("median_total_ms", json!(case.median_total_ms));
    artifact.insert_stat_value("route_distance_km", json!(case.route_distance_km));
    artifact.insert_stat_value("facts", serde_json::to_value(&case.facts)?);
    artifact.insert_stat_value(
        "component_hotspots",
        serde_json::to_value(&case.component_hotspots)?,
    );
    artifact.push_auxiliary_output(summary_json_aux.clone());
    artifact.push_auxiliary_output(summary_markdown_aux.clone());
    Ok(artifact)
}

fn render_summary_markdown(summary: &WeatherNativeProfileSummary) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Weather-native component profile summary\n\n");
    markdown.push_str(&format!(
        "- model: `{}`\n- date/cycle: `{}` `{}`Z f{:03}\n- domain: `{}`\n- runs per case: `{}`\n- stage timing: load={} ms, projected_map={} ms\n\n",
        summary.model,
        summary.request.date_yyyymmdd,
        summary.request.cycle_utc,
        summary.request.forecast_hour,
        summary.request.domain.slug,
        summary.request.runs,
        summary.stage_timing.data_load_ms,
        summary.stage_timing.projected_map_build_ms,
    ));

    markdown.push_str("## Map Profiles\n\n");
    markdown.push_str("| Product | Native total (ms) | Legacy total (ms) | Native vs legacy |\n");
    markdown.push_str("|---|---:|---:|---:|\n");
    for case in &summary.map_cases {
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {:.2}x |\n",
            case.recipe_slug,
            case.native.median_total_ms,
            case.legacy.median_total_ms,
            case.native_speedup_over_legacy,
        ));
    }
    markdown.push('\n');
    for case in &summary.map_cases {
        markdown.push_str(&format!("### `{}`\n\n", case.recipe_slug));
        markdown.push_str(&format!(
            "- native proof: `{}`\n- legacy proof: `{}`\n",
            case.native.output_png.display(),
            case.legacy.output_png.display(),
        ));
        markdown.push_str("- native top components:\n");
        for component in case.native.component_hotspots.iter().take(8) {
            markdown.push_str(&format!(
                "  - `{}`: {} ms ({:.1}%)\n",
                component.component, component.median_ms, component.share_of_total_pct
            ));
        }
        markdown.push_str("- legacy top components:\n");
        for component in case.legacy.component_hotspots.iter().take(8) {
            markdown.push_str(&format!(
                "  - `{}`: {} ms ({:.1}%)\n",
                component.component, component.median_ms, component.share_of_total_pct
            ));
        }
        markdown.push('\n');
    }

    markdown.push_str("## Cross-Section Profiles\n\n");
    markdown.push_str("| Product | Route | Total (ms) | Palette |\n");
    markdown.push_str("|---|---|---:|---|\n");
    for case in &summary.cross_sections {
        markdown.push_str(&format!(
            "| `{}` | `{}` | {} | `{}` |\n",
            case.product_slug, case.route_slug, case.median_total_ms, case.palette_slug
        ));
    }
    markdown.push('\n');
    for case in &summary.cross_sections {
        markdown.push_str(&format!("### `{}`\n\n", case.product_slug));
        markdown.push_str(&format!(
            "- proof: `{}`\n- route: `{}` ({:.0} km)\n",
            case.output_png.display(),
            case.route_label,
            case.route_distance_km,
        ));
        markdown.push_str("- top components:\n");
        for component in case.component_hotspots.iter().take(8) {
            markdown.push_str(&format!(
                "  - `{}`: {} ms ({:.1}%)\n",
                component.component, component.median_ms, component.share_of_total_pct
            ));
        }
        markdown.push('\n');
    }

    markdown
}

#[cfg(test)]
mod tests {
    use super::{
        ComponentTimingStat, CrossSectionProfileSummary, MapProfileCaseSummary,
        MapRenderModeProfileSummary, ProfileStageTiming, WeatherNativeProfileRequest,
        WeatherNativeProfileRequestSummary, WeatherNativeProfileSummary,
        build_profile_bundle_manifest, component_stats, median_u128, relative_path,
    };
    use rustwx_core::SourceId;
    use rustwx_products::shared_context::DomainSpec;
    use std::fs;
    use std::path::PathBuf;
    use std::process;

    fn sample_profile_request(out_dir: PathBuf) -> WeatherNativeProfileRequest {
        WeatherNativeProfileRequest {
            date_yyyymmdd: "20260422".to_string(),
            cycle_utc: 18,
            forecast_hour: 6,
            source: SourceId::Nomads,
            domain: DomainSpec::new("southern_plains", (-107.0, -91.0, 30.0, 40.0)),
            out_dir: out_dir.clone(),
            cache_root: out_dir.join("cache"),
            use_cache: true,
            map_products: vec!["mlcape".to_string()],
            cross_section_requests: Vec::new(),
            runs: 3,
            output_width: 1200,
            output_height: 900,
            png_compression: rustwx_render::PngCompressionMode::Default,
        }
    }

    #[test]
    fn median_u128_picks_middle_value() {
        assert_eq!(median_u128([9, 2, 5]), 5);
    }

    #[test]
    fn component_stats_sort_by_median_desc() {
        let runs = vec![(10, 3), (12, 2), (11, 4)];
        let stats = component_stats(&runs, 20, |run| {
            vec![("artifact_build", run.0), ("png_encode", run.1)]
        });
        assert_eq!(stats[0].component, "artifact_build");
        assert_eq!(stats[0].median_ms, 11);
        assert_eq!(stats[1].component, "png_encode");
        assert_eq!(stats[1].median_ms, 3);
    }

    #[test]
    fn profile_bundle_manifest_collects_map_cross_section_and_summary_outputs() {
        let root = std::env::temp_dir().join(format!("rustwx_profile_bundle_{}", process::id()));
        let profile_root = root.join("profile");
        fs::create_dir_all(&profile_root).unwrap();

        let native_png = profile_root.join("mlcape_native.png");
        let legacy_png = profile_root.join("mlcape_legacy.png");
        let cross_png = profile_root.join("amarillo_temperature_cross.png");
        let summary_json = profile_root.join("summary.json");
        let summary_markdown = profile_root.join("summary.md");
        fs::write(&native_png, b"native-png").unwrap();
        fs::write(&legacy_png, b"legacy-png").unwrap();
        fs::write(&cross_png, b"cross-png").unwrap();

        let request = sample_profile_request(root.clone());
        let summary = WeatherNativeProfileSummary {
            runner: "weather_native_profile",
            model: rustwx_core::ModelId::Hrrr,
            request: WeatherNativeProfileRequestSummary {
                date_yyyymmdd: "20260422".to_string(),
                cycle_utc: 18,
                forecast_hour: 6,
                source: SourceId::Nomads,
                domain: DomainSpec::new("southern_plains", (-107.0, -91.0, 30.0, 40.0)),
                out_dir: root.clone(),
                cache_root: root.join("cache"),
                use_cache: true,
                map_products: vec!["mlcape".to_string()],
                cross_section_products: vec!["temperature".to_string()],
                runs: 3,
                output_width: 1200,
                output_height: 900,
                png_compression: rustwx_render::PngCompressionMode::Default,
            },
            stage_timing: ProfileStageTiming {
                data_load_ms: 12,
                projected_map_build_ms: 8,
            },
            map_cases: vec![MapProfileCaseSummary {
                recipe_slug: "mlcape".to_string(),
                title: "MLCAPE".to_string(),
                units: "J/kg".to_string(),
                native: MapRenderModeProfileSummary {
                    mode: "native".to_string(),
                    run_count: 3,
                    output_png: relative_path(&root, &native_png),
                    runs: Vec::new(),
                    median_total_ms: 55,
                    component_hotspots: vec![ComponentTimingStat {
                        component: "contour_draw".to_string(),
                        median_ms: 14,
                        mean_ms: 14.0,
                        share_of_total_pct: 25.0,
                    }],
                },
                legacy: MapRenderModeProfileSummary {
                    mode: "legacy".to_string(),
                    run_count: 3,
                    output_png: relative_path(&root, &legacy_png),
                    runs: Vec::new(),
                    median_total_ms: 70,
                    component_hotspots: vec![ComponentTimingStat {
                        component: "rasterize".to_string(),
                        median_ms: 18,
                        mean_ms: 18.0,
                        share_of_total_pct: 25.7,
                    }],
                },
                native_speedup_over_legacy: 70.0 / 55.0,
            }],
            cross_sections: vec![CrossSectionProfileSummary {
                route_slug: "amarillo_chicago".to_string(),
                route_label: "Amarillo to Chicago".to_string(),
                route_distance_km: 1160.0,
                product_slug: "temperature".to_string(),
                product_label: "Temperature".to_string(),
                palette_slug: "temperature".to_string(),
                facts: rustwx_products::cross_section::PressureCrossSectionFacts::default(),
                run_count: 3,
                output_png: relative_path(&root, &cross_png),
                runs: Vec::new(),
                median_total_ms: 42,
                component_hotspots: vec![ComponentTimingStat {
                    component: "scalar_field".to_string(),
                    median_ms: 11,
                    mean_ms: 11.0,
                    share_of_total_pct: 26.2,
                }],
            }],
            summary_json: relative_path(&root, &summary_json),
            summary_markdown: relative_path(&root, &summary_markdown),
            bundle_manifest: PathBuf::from("profile/bundle.json"),
        };
        fs::write(&summary_json, serde_json::to_vec_pretty(&summary).unwrap()).unwrap();
        fs::write(&summary_markdown, "# profile summary\n").unwrap();

        let manifest =
            build_profile_bundle_manifest(&request, &summary, &summary_json, &summary_markdown)
                .unwrap();

        assert_eq!(manifest.bundle_kind, "weather_native_profile");
        assert_eq!(manifest.artifacts.len(), 5);
        assert_eq!(
            manifest
                .artifacts
                .iter()
                .find(|artifact| artifact.artifact_key == "map:mlcape:native")
                .unwrap()
                .auxiliary_outputs
                .len(),
            2
        );
        assert!(manifest.artifacts.iter().any(|artifact| {
            artifact.artifact_key == "cross_section:amarillo_chicago:temperature"
                && artifact.relative_path == PathBuf::from("profile/amarillo_temperature_cross.png")
        }));
        assert!(manifest.artifacts.iter().any(|artifact| {
            artifact.artifact_key == "profile_summary_json"
                && artifact.role == rustwx_products::artifact_bundle::ArtifactBundleRole::Stats
        }));

        let _ = fs::remove_dir_all(root);
    }
}
