use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use grib_core::grib2::Grib2File;
use image::imageops::{FilterType, filter3x3, resize};
use image::{DynamicImage, RgbaImage};
use region::RegionPreset;
use rustwx_core::VerticalSelector;
use rustwx_core::{CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, SourceId};
use rustwx_io::{
    FetchRequest, extract_field_from_grib2, fetch_bytes, fetch_bytes_with_cache,
    load_cached_selected_field, store_cached_selected_field,
};
use rustwx_models::{ModelError, PlotRecipe, plot_recipe, plot_recipe_fetch_plan};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::shared_context::model_time_subtitle;
use rustwx_render::{
    Color, ColorScale, ContourLayer, DiscreteColorScale, DomainFrame, ExtendMode, LevelDensity,
    LineworkRole, MapRenderRequest, ProductVisualMode, ProjectedDomain, ProjectedMap,
    RenderDensity, StaticPlotStyle, WindBarbLayer,
    build_projected_map as build_projected_map_from_latlon, map_frame_aspect_ratio_for_mode,
    render_image_with_style, weather::WeatherPalette, weather::weather_palette,
};
use serde::Serialize;

const DEFAULT_RECIPE: &str = "2m_relative_humidity";
const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;

#[derive(Debug, Parser)]
#[command(
    name = "style-proof",
    about = "Generate a small style comparison set for one real RustWX map request"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, default_value = DEFAULT_RECIPE)]
    recipe: String,
    #[arg(long, default_value = "20260419")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long)]
    product: Option<String>,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Conus)]
    region: RegionPreset,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof\\style_proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = false)]
    no_html: bool,
}

#[derive(Debug, Clone, Copy)]
enum ProofVariant {
    Baseline,
    CleanAtlas,
    DenseColors,
    CleanAtlasDense,
    LineHierarchy,
    CleanAtlasFast,
    Supersample2x,
    LineHierarchySupersample2x,
    CleanAtlasQuality2x,
    Sharpen,
    Combined,
    CleanAtlasCombined,
}

impl ProofVariant {
    fn slug(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::CleanAtlas => "clean_atlas",
            Self::DenseColors => "dense_colors",
            Self::CleanAtlasDense => "clean_atlas_dense",
            Self::LineHierarchy => "line_hierarchy",
            Self::CleanAtlasFast => "clean_atlas_fast",
            Self::Supersample2x => "supersample_2x",
            Self::LineHierarchySupersample2x => "line_hierarchy_supersample_2x",
            Self::CleanAtlasQuality2x => "clean_atlas_quality_2x",
            Self::Sharpen => "sharpen",
            Self::Combined => "combined",
            Self::CleanAtlasCombined => "clean_atlas_combined",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Baseline => "Current request as rendered by production defaults.",
            Self::CleanAtlas => "Current request through the Clean Atlas presentation style.",
            Self::DenseColors => {
                "Increase stepped fill density only; no smoothing, no line changes."
            }
            Self::CleanAtlasDense => {
                "Clean Atlas chrome plus denser stepped fill levels for smoother gradients."
            }
            Self::LineHierarchy => {
                "Make coast/state/frame linework more deliberate via request-side width/color tuning."
            }
            Self::CleanAtlasFast => {
                "Renderer-level Clean Atlas Fast style; intended as the fast production candidate."
            }
            Self::Supersample2x => "Render the full map at 2x and downsample back to 1200x900.",
            Self::LineHierarchySupersample2x => {
                "Tuned map line hierarchy plus 2x supersampling and Lanczos downsampling; no sharpen pass."
            }
            Self::CleanAtlasQuality2x => {
                "Clean Atlas Fast plus tuned line hierarchy and 2x supersampling; no sharpen pass."
            }
            Self::Sharpen => "Apply a mild post-render sharpen kernel to the final PNG.",
            Self::Combined => {
                "Dense stepped fills + tuned line hierarchy + 2x supersample + mild sharpen."
            }
            Self::CleanAtlasCombined => {
                "Clean Atlas + dense fills + tuned line hierarchy + 2x supersample + mild sharpen."
            }
        }
    }

    fn plot_style(self) -> StaticPlotStyle {
        match self {
            Self::CleanAtlas | Self::CleanAtlasDense => StaticPlotStyle::CleanAtlas,
            Self::CleanAtlasFast => StaticPlotStyle::CleanAtlasFast,
            Self::CleanAtlasQuality2x => StaticPlotStyle::CleanAtlasQuality2x,
            Self::CleanAtlasCombined => StaticPlotStyle::CleanAtlasCombined,
            _ => StaticPlotStyle::Default,
        }
    }
}

#[derive(Debug, Serialize)]
struct VariantRecord {
    variant: String,
    description: String,
    output_path: PathBuf,
    output_file: String,
    width: u32,
    height: u32,
    bytes: u64,
    render_ms: u128,
    save_ms: u128,
    total_ms: u128,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let recipe =
        plot_recipe(&args.recipe).ok_or_else(|| format!("unknown recipe '{}'", args.recipe))?;
    let (default_product, selectors, variable_patterns) = fetch_recipe_inputs(recipe, args.model)?;
    let product = args.product.as_deref().unwrap_or(default_product);

    let latest = match args.cycle {
        Some(hour) => rustwx_models::LatestRun {
            model: args.model,
            cycle: CycleSpec::new(&args.date, hour)?,
            source: args
                .source
                .unwrap_or(rustwx_models::model_summary(args.model).sources[0].id),
        },
        None => rustwx_models::latest_available_run(args.model, args.source, &args.date)?,
    };

    let request = build_request(
        &args,
        recipe,
        product,
        selectors,
        variable_patterns,
        &cache_root,
        &latest,
    )?;
    let product_slug = filename_slug(product);

    let variants = [
        ProofVariant::Baseline,
        ProofVariant::CleanAtlas,
        ProofVariant::DenseColors,
        ProofVariant::CleanAtlasDense,
        ProofVariant::LineHierarchy,
        ProofVariant::CleanAtlasFast,
        ProofVariant::Supersample2x,
        ProofVariant::LineHierarchySupersample2x,
        ProofVariant::CleanAtlasQuality2x,
        ProofVariant::Sharpen,
        ProofVariant::Combined,
        ProofVariant::CleanAtlasCombined,
    ];
    let mut records = Vec::with_capacity(variants.len());
    for variant in variants {
        let render_start = Instant::now();
        let image = render_variant(&request, variant)?;
        let render_ms = render_start.elapsed().as_millis();
        let width = image.width();
        let height = image.height();
        let output_path = args.out_dir.join(format!(
            "rustwx_{}_{}_{}z_f{:03}_{}_{}_{}_{}.png",
            args.model.as_str().replace('-', "_"),
            args.date,
            latest.cycle.hour_utc,
            args.forecast_hour,
            args.region.slug(),
            recipe.slug,
            product_slug,
            variant.slug()
        ));
        let save_start = Instant::now();
        DynamicImage::ImageRgba8(image).save(&output_path)?;
        let save_ms = save_start.elapsed().as_millis();
        let bytes = fs::metadata(&output_path)?.len();
        let output_file = output_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        records.push(VariantRecord {
            variant: variant.slug().to_string(),
            description: variant.description().to_string(),
            output_path,
            output_file,
            width,
            height,
            bytes,
            render_ms,
            save_ms,
            total_ms: render_ms.saturating_add(save_ms),
        });
    }

    let manifest_path = args.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}_{}_variants.json",
        args.model.as_str().replace('-', "_"),
        args.date,
        latest.cycle.hour_utc,
        args.forecast_hour,
        args.region.slug(),
        recipe.slug,
        product_slug
    ));
    fs::write(&manifest_path, serde_json::to_vec_pretty(&records)?)?;

    if !args.no_html {
        let html_path = write_lab_html(&args, recipe, &latest, product, &manifest_path, &records)?;
        println!("{}", html_path.display());
    }

    for record in &records {
        println!("{}", record.output_path.display());
    }
    println!("{}", manifest_path.display());
    Ok(())
}

fn build_request(
    args: &Args,
    recipe: &PlotRecipe,
    product: &str,
    selectors: Vec<FieldSelector>,
    variable_patterns: Vec<&'static str>,
    cache_root: &std::path::Path,
    latest: &rustwx_models::LatestRun,
) -> Result<MapRenderRequest, Box<dyn std::error::Error>> {
    let request = ModelRunRequest::new(
        args.model,
        latest.cycle.clone(),
        args.forecast_hour,
        product,
    )?;
    let fetch = FetchRequest {
        request,
        source_override: Some(latest.source),
        variable_patterns: variable_patterns.into_iter().map(str::to_string).collect(),
        earth2_ensemble: None,
    };

    let fetched = if args.no_cache {
        rustwx_io::CachedFetchResult {
            result: fetch_bytes(&fetch)?,
            cache_hit: false,
            bytes_path: rustwx_io::fetch_cache_paths(cache_root, &fetch).0,
            metadata_path: rustwx_io::fetch_cache_paths(cache_root, &fetch).1,
        }
    } else {
        fetch_bytes_with_cache(&fetch, cache_root, true)?
    };

    let mut extracted = HashMap::new();
    let mut missing_selectors = Vec::new();
    for selector in selectors {
        if !args.no_cache {
            if let Some(cached) = load_cached_selected_field(cache_root, &fetch, selector)? {
                extracted.insert(selector, cached.field);
                continue;
            }
        }
        missing_selectors.push(selector);
    }

    let grib = if missing_selectors.is_empty() {
        None
    } else {
        Some(Grib2File::from_bytes(&fetched.result.bytes)?)
    };
    if let Some(grib) = grib.as_ref() {
        for selector in &missing_selectors {
            let field = extract_field_from_grib2(grib, *selector)?;
            if !args.no_cache {
                store_cached_selected_field(cache_root, &fetch, &field)?;
            }
            extracted.insert(*selector, field);
        }
    }

    let filled_selector = recipe
        .filled
        .selector
        .ok_or("recipe filled field missing selector binding")?;
    let filled = extracted
        .get(&filled_selector)
        .ok_or("missing filled selector after extraction")?
        .clone();

    let overlay_only = should_render_overlay_only(filled.selector, recipe.contours.is_some());
    let projected = build_projected_map(
        &filled.grid.lat_deg,
        &filled.grid.lon_deg,
        args.region,
        map_frame_aspect_ratio_for_mode(
            visual_mode_for_recipe(recipe, filled.selector, overlay_only),
            OUTPUT_WIDTH,
            OUTPUT_HEIGHT,
            true,
            true,
        ),
    )?;

    let mut render_request =
        build_render_request(recipe, &filled, &extracted, projected, args.region);
    render_request.subtitle_left = Some(model_time_subtitle(
        args.model,
        &args.date,
        latest.cycle.hour_utc,
        args.forecast_hour,
    ));
    render_request.subtitle_right = Some(format!("{} | source: {}", product, latest.source));
    Ok(render_request)
}

fn render_variant(
    base_request: &MapRenderRequest,
    variant: ProofVariant,
) -> Result<RgbaImage, Box<dyn std::error::Error>> {
    match variant {
        ProofVariant::Baseline | ProofVariant::CleanAtlas => {
            render_styled_image(base_request, variant)
        }
        ProofVariant::DenseColors | ProofVariant::CleanAtlasDense => {
            let mut request = base_request.clone();
            apply_dense_colors(&mut request);
            render_styled_image(&request, variant)
        }
        ProofVariant::LineHierarchy => {
            let mut request = base_request.clone();
            apply_line_hierarchy(&mut request);
            render_styled_image(&request, variant)
        }
        ProofVariant::CleanAtlasFast
        | ProofVariant::CleanAtlasQuality2x
        | ProofVariant::CleanAtlasCombined => render_styled_image(base_request, variant),
        ProofVariant::Supersample2x => render_supersampled(base_request, 2, variant),
        ProofVariant::LineHierarchySupersample2x => {
            let mut request = base_request.clone();
            apply_line_hierarchy(&mut request);
            render_supersampled(&request, 2, variant)
        }
        ProofVariant::Sharpen => {
            let image = render_styled_image(base_request, variant)?;
            Ok(sharpen_image(&image))
        }
        ProofVariant::Combined => {
            let mut request = base_request.clone();
            apply_dense_colors(&mut request);
            apply_line_hierarchy(&mut request);
            let image = render_supersampled(&request, 2, variant)?;
            Ok(sharpen_image(&image))
        }
    }
}

fn render_styled_image(
    request: &MapRenderRequest,
    variant: ProofVariant,
) -> Result<RgbaImage, Box<dyn std::error::Error>> {
    Ok(render_image_with_style(request, variant.plot_style())?)
}

fn apply_dense_colors(request: &mut MapRenderRequest) {
    request.render_density = RenderDensity {
        fill: LevelDensity {
            multiplier: 16,
            min_source_level_count: 5,
        },
        palette_multiplier: 16,
    };
}

fn render_supersampled(
    request: &MapRenderRequest,
    factor: u32,
    variant: ProofVariant,
) -> Result<RgbaImage, Box<dyn std::error::Error>> {
    let mut hires = request.clone();
    hires.width = request.width.saturating_mul(factor);
    hires.height = request.height.saturating_mul(factor);
    scale_overlay_dimensions(&mut hires, factor);
    let rendered = render_styled_image(&hires, variant)?;
    Ok(resize(
        &rendered,
        request.width,
        request.height,
        FilterType::Lanczos3,
    ))
}

fn sharpen_image(image: &RgbaImage) -> RgbaImage {
    filter3x3(
        image,
        &[0.0, -0.35, 0.0, -0.35, 2.4, -0.35, 0.0, -0.35, 0.0],
    )
}

fn apply_line_hierarchy(request: &mut MapRenderRequest) {
    for line in &mut request.projected_lines {
        let (color, width) = match line.role {
            LineworkRole::Coast => (Color::rgba(12, 16, 22, 255), 2),
            LineworkRole::State => (Color::rgba(48, 54, 64, 235), 1),
            LineworkRole::International => (Color::rgba(82, 88, 100, 220), 1),
            LineworkRole::Lake => (Color::rgba(34, 82, 145, 240), 1),
            LineworkRole::County => (Color::rgba(150, 158, 168, 120), 1),
            LineworkRole::Generic => (line.color, line.width.max(1)),
        };
        line.color = color;
        line.width = width;
        line.role = LineworkRole::Generic;
    }

    if let Some(frame) = request.domain_frame.as_mut() {
        frame.outline_color = Color::rgba(14, 16, 18, 255);
        frame.outline_width = 2;
    } else {
        request.domain_frame = Some(DomainFrame::model_data_default());
    }
}

fn scale_overlay_dimensions(request: &mut MapRenderRequest, factor: u32) {
    let factor = factor.max(1);
    if factor == 1 {
        return;
    }

    for line in &mut request.projected_lines {
        line.width = line.width.max(1).saturating_mul(factor);
    }
    for contour in &mut request.contours {
        contour.width = contour.width.max(1).saturating_mul(factor);
    }
    for barb in &mut request.wind_barbs {
        barb.width = barb.width.max(1).saturating_mul(factor);
        barb.length_px *= factor as f64;
    }
    if let Some(frame) = request.domain_frame.as_mut() {
        frame.outline_width = frame.outline_width.max(1).saturating_mul(factor);
    }
}

fn build_render_request(
    recipe: &PlotRecipe,
    filled: &rustwx_core::SelectedField2D,
    extracted: &HashMap<FieldSelector, rustwx_core::SelectedField2D>,
    projected: ProjectedMap,
    region: RegionPreset,
) -> MapRenderRequest {
    let filled_field = render_filled_field(recipe, filled, extracted);
    let overlay_only = should_render_overlay_only(filled.selector, recipe.contours.is_some());
    let mut request = if overlay_only {
        MapRenderRequest::contour_only(filled_field.into())
    } else {
        MapRenderRequest::new(
            filled_field.into(),
            scale_for_recipe(recipe, filled.selector),
        )
    };
    request.visual_mode = visual_mode_for_recipe(recipe, filled.selector, overlay_only);
    request.title = Some(recipe.title.to_string());
    request.width = OUTPUT_WIDTH;
    request.height = OUTPUT_HEIGHT;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x,
        y: projected.projected_y,
        extent: projected.extent,
    });
    request.projected_lines = projected.lines;
    request.projected_polygons = projected.polygons;
    request.contours = build_contour_layers(recipe, extracted);
    request.wind_barbs = build_barb_layers(recipe, extracted, region);
    request
}

fn visual_mode_for_recipe(
    recipe: &PlotRecipe,
    selector: FieldSelector,
    overlay_only: bool,
) -> ProductVisualMode {
    if overlay_only {
        return ProductVisualMode::OverlayAnalysis;
    }
    if matches!(recipe.style, rustwx_models::RenderStyle::WeatherHeight)
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

fn fetch_recipe_inputs(
    recipe: &PlotRecipe,
    model: ModelId,
) -> Result<(&'static str, Vec<FieldSelector>, Vec<&'static str>), Box<dyn std::error::Error>> {
    match plot_recipe_fetch_plan(recipe.slug, model) {
        Ok(plan) => Ok((plan.product, plan.selectors(), plan.variable_patterns())),
        Err(ModelError::UnsupportedPlotRecipeModel { reason, .. }) => Err(format!(
            "plot recipe '{}' is not yet supported for {}: {}",
            recipe.slug, model, reason
        )
        .into()),
        Err(err) => Err(err.into()),
    }
}

fn convert_filled_field(
    recipe: &PlotRecipe,
    field: &rustwx_core::SelectedField2D,
) -> rustwx_core::Field2D {
    let mut core = field.clone().into_field2d();
    if matches!(
        recipe.style,
        rustwx_models::RenderStyle::WeatherTemperature
            | rustwx_models::RenderStyle::WeatherDewpoint
    ) {
        for value in &mut core.values {
            *value -= 273.15;
        }
        core.units = "degC".to_string();
    }
    core
}

fn render_filled_field(
    recipe: &PlotRecipe,
    field: &rustwx_core::SelectedField2D,
    extracted: &HashMap<FieldSelector, rustwx_core::SelectedField2D>,
) -> rustwx_core::Field2D {
    derived_height_winds_fill(recipe, field, extracted)
        .unwrap_or_else(|| convert_filled_field(recipe, field))
}

fn derived_height_winds_fill(
    recipe: &PlotRecipe,
    field: &rustwx_core::SelectedField2D,
    extracted: &HashMap<FieldSelector, rustwx_core::SelectedField2D>,
) -> Option<rustwx_core::Field2D> {
    if recipe.style != rustwx_models::RenderStyle::WeatherHeight
        || field.selector.field != CanonicalField::GeopotentialHeight
    {
        return None;
    }

    let (Some(u_spec), Some(v_spec)) = (&recipe.barbs_u, &recipe.barbs_v) else {
        return None;
    };
    let (Some(u_selector), Some(v_selector)) = (u_spec.selector, v_spec.selector) else {
        return None;
    };
    let (Some(u), Some(v)) = (extracted.get(&u_selector), extracted.get(&v_selector)) else {
        return None;
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

    rustwx_core::Field2D::new(
        rustwx_core::ProductKey::named(format!("{}_wind_speed", recipe.slug)),
        "kt",
        u.grid.clone(),
        values,
    )
    .ok()
}

fn should_render_overlay_only(selector: FieldSelector, has_contours: bool) -> bool {
    matches!(
        selector.field,
        CanonicalField::GeopotentialHeight | CanonicalField::PressureReducedToMeanSeaLevel
    ) && !has_contours
}

fn scale_for_recipe(recipe: &PlotRecipe, filled_selector: FieldSelector) -> ColorScale {
    let discrete = match recipe.style {
        rustwx_models::RenderStyle::WeatherTemperature => {
            let (lo, hi) = match filled_selector.vertical {
                rustwx_core::VerticalSelector::IsobaricHpa(500) => (-50.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(850) => (-40.0, 40.0),
                _ => (-60.0, 40.0),
            };
            DiscreteColorScale {
                levels: range_step(lo, hi, 1.0),
                colors: weather_palette(WeatherPalette::Temperature),
                extend: ExtendMode::Both,
                mask_below: None,
            }
        }
        rustwx_models::RenderStyle::WeatherReflectivity => DiscreteColorScale {
            levels: range_step(5.0, 80.0, 5.0),
            colors: weather_palette(WeatherPalette::Reflectivity),
            extend: ExtendMode::Both,
            mask_below: Some(5.0),
        },
        rustwx_models::RenderStyle::WeatherRh => DiscreteColorScale {
            levels: range_step(0.0, 105.0, 5.0),
            colors: weather_palette(WeatherPalette::Rh),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        rustwx_models::RenderStyle::WeatherVorticity => DiscreteColorScale {
            levels: range_step(0.0, 48.0, 2.0),
            colors: weather_palette(WeatherPalette::RelVort),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        rustwx_models::RenderStyle::WeatherDewpoint => DiscreteColorScale {
            levels: range_step(-40.0, 30.0, 2.0),
            colors: weather_palette(WeatherPalette::Dewpoint),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        rustwx_models::RenderStyle::WeatherHeight => DiscreteColorScale {
            levels: match filled_selector.vertical {
                rustwx_core::VerticalSelector::IsobaricHpa(200)
                | rustwx_core::VerticalSelector::IsobaricHpa(250)
                | rustwx_core::VerticalSelector::IsobaricHpa(300) => range_step(50.0, 170.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(500) => range_step(20.0, 150.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(700) => range_step(10.0, 90.0, 5.0),
                rustwx_core::VerticalSelector::IsobaricHpa(850) => range_step(10.0, 70.0, 5.0),
                _ => range_step(10.0, 120.0, 5.0),
            },
            colors: weather_palette(WeatherPalette::Winds),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        _ => DiscreteColorScale {
            levels: range_step(-50.0, 5.0, 1.0),
            colors: weather_palette(WeatherPalette::Temperature),
            extend: ExtendMode::Both,
            mask_below: None,
        },
    };
    ColorScale::Discrete(discrete)
}

fn build_contour_layers(
    recipe: &PlotRecipe,
    extracted: &HashMap<FieldSelector, rustwx_core::SelectedField2D>,
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

    let data = if selector.field == CanonicalField::GeopotentialHeight {
        field.values.iter().map(|value| value * 0.1).collect()
    } else {
        field.values.clone()
    };
    let (levels, color, width, labels) = match selector {
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(200),
            ..
        } => (range_step(1020.0, 1290.0, 6.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(300),
            ..
        } => (range_step(780.0, 1020.0, 6.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(500),
            ..
        } => (range_step(450.0, 650.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(700),
            ..
        } => (range_step(180.0, 360.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(850),
            ..
        } => (range_step(0.0, 200.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::UpdraftHelicity,
            vertical:
                rustwx_core::VerticalSelector::HeightAboveGroundLayerMeters {
                    bottom_m: 2000,
                    top_m: 5000,
                },
            ..
        } => (
            vec![25.0, 50.0, 75.0, 100.0, 150.0, 200.0],
            Color::rgba(166, 0, 255, 255),
            2,
            false,
        ),
        _ => (range_step(0.0, 200.0, 10.0), Color::BLACK, 1, true),
    };

    vec![ContourLayer {
        data,
        levels,
        color,
        width,
        labels,
        show_extrema: false,
    }]
}

fn build_barb_layers(
    recipe: &PlotRecipe,
    extracted: &HashMap<FieldSelector, rustwx_core::SelectedField2D>,
    region: RegionPreset,
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
    let (visible_nx, visible_ny) = visible_grid_span(&u.grid, region.bounds());
    let stride_x = ((visible_nx as f64 / 24.0).round() as usize).clamp(3, 128);
    let stride_y = ((visible_ny as f64 / 14.0).round() as usize).clamp(3, 96);
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

fn build_projected_map(
    lat_deg: &[f32],
    lon_deg: &[f32],
    region: RegionPreset,
    target_ratio: f64,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    build_projected_map_from_latlon(lat_deg, lon_deg, region.bounds(), target_ratio)
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

fn write_lab_html(
    args: &Args,
    recipe: &PlotRecipe,
    latest: &rustwx_models::LatestRun,
    product: &str,
    manifest_path: &Path,
    records: &[VariantRecord],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let html_path = args.out_dir.join("index.html");
    let manifest_file = manifest_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("variants.json");
    let run_label = format!(
        "{} {} {:02}z f{:03} {} {} {}",
        args.model.as_str(),
        args.date,
        latest.cycle.hour_utc,
        args.forecast_hour,
        args.region.slug(),
        recipe.slug,
        product
    );

    let baseline = records
        .iter()
        .find(|record| record.variant == "baseline")
        .or_else(|| records.first());
    let mut cards = String::new();
    for record in records {
        let is_baseline = record.variant == "baseline";
        let baseline_src = baseline
            .map(|record| record.output_file.as_str())
            .unwrap_or(record.output_file.as_str());
        cards.push_str(&format!(
            r#"
<article class="card {baseline_class}" data-variant="{variant}">
  <div class="card-head">
    <div>
      <h2>{variant_title}</h2>
      <p>{description}</p>
    </div>
    <div class="bench">
      <span>{render_ms} ms render</span>
      <span>{save_ms} ms save</span>
      <span>{total_ms} ms total</span>
      <span>{bytes_kb} KB</span>
    </div>
  </div>
  <div class="compare">
    <figure>
      <figcaption>Baseline</figcaption>
      <img src="{baseline_src}" alt="baseline plot">
    </figure>
    <figure>
      <figcaption>{variant_title}</figcaption>
      <img src="{src}" alt="{variant_title} plot">
    </figure>
  </div>
  <div class="vote-row">
    <button type="button" onclick="vote('{variant}', 1)">Upvote this style</button>
    <button type="button" onclick="vote('{variant}', -1)">Downvote</button>
    <strong id="score-{variant}">0</strong>
  </div>
  <textarea id="note-{variant}" placeholder="What looks better or worse? Label density, colors, contrast, map clarity, artifacts..." oninput="saveNote('{variant}')"></textarea>
</article>
"#,
            baseline_class = if is_baseline { "baseline" } else { "" },
            variant = html_attr(&record.variant),
            variant_title = html_text(&record.variant.replace('_', " ")),
            description = html_text(&record.description),
            render_ms = record.render_ms,
            save_ms = record.save_ms,
            total_ms = record.total_ms,
            bytes_kb = (record.bytes + 1023) / 1024,
            baseline_src = html_attr(baseline_src),
            src = html_attr(&record.output_file),
        ));
    }

    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>rustwx Plot Style Lab</title>
  <style>
    :root {{ color-scheme: light; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    body {{ margin: 0; background: #f4f6f8; color: #121820; }}
    header {{ padding: 20px 24px 14px; border-bottom: 1px solid #d8dde3; background: #ffffff; position: sticky; top: 0; z-index: 2; }}
    h1 {{ margin: 0 0 6px; font-size: 22px; letter-spacing: 0; }}
    p {{ margin: 0; color: #4d5967; }}
    main {{ padding: 18px 24px 36px; display: grid; gap: 18px; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
    .toolbar a, button {{ border: 1px solid #aeb7c2; background: #fff; color: #17202a; padding: 8px 10px; border-radius: 6px; cursor: pointer; text-decoration: none; font-weight: 600; }}
    .toolbar button:hover, .toolbar a:hover {{ background: #edf2f7; }}
    .card {{ background: #fff; border: 1px solid #d7dde5; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06); }}
    .card.baseline {{ border-color: #7c8a9a; }}
    .card-head {{ display: flex; justify-content: space-between; gap: 18px; padding: 14px 16px; border-bottom: 1px solid #e2e7ee; }}
    h2 {{ margin: 0 0 4px; font-size: 18px; text-transform: capitalize; }}
    .bench {{ min-width: 150px; display: grid; gap: 4px; font-variant-numeric: tabular-nums; color: #26313d; }}
    .bench span {{ background: #eef2f6; border-radius: 5px; padding: 3px 7px; text-align: right; }}
    .compare {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0; background: #101418; }}
    figure {{ margin: 0; padding: 10px; border-right: 1px solid #2a333d; }}
    figure:last-child {{ border-right: 0; }}
    figcaption {{ color: #e9eef5; font-size: 13px; margin-bottom: 8px; text-transform: capitalize; }}
    img {{ width: 100%; display: block; background: #fff; }}
    .vote-row {{ display: flex; gap: 10px; align-items: center; padding: 12px 16px; border-top: 1px solid #e2e7ee; }}
    textarea {{ box-sizing: border-box; width: calc(100% - 32px); min-height: 72px; margin: 0 16px 16px; padding: 10px; border: 1px solid #cad2dc; border-radius: 6px; font: inherit; resize: vertical; }}
    pre {{ white-space: pre-wrap; background: #101418; color: #e6edf3; padding: 12px; border-radius: 8px; }}
    @media (max-width: 900px) {{ .compare {{ grid-template-columns: 1fr; }} figure {{ border-right: 0; border-bottom: 1px solid #2a333d; }} .card-head {{ flex-direction: column; }} }}
  </style>
</head>
<body data-run="{run_key}">
  <header>
    <h1>rustwx Plot Style Lab</h1>
    <p>{run_label}</p>
    <div class="toolbar">
      <a href="{manifest_file}">Manifest JSON</a>
      <button type="button" onclick="exportVotes()">Export votes/notes</button>
      <button type="button" onclick="resetVotes()">Reset local votes</button>
    </div>
  </header>
  <main>
    {cards}
    <section>
      <h2>Preference export</h2>
      <pre id="export-box">No export yet.</pre>
    </section>
  </main>
  <script>
    const runKey = document.body.dataset.run;
    const key = (name) => `rustwx-style-lab:${{runKey}}:${{name}}`;
    function scoreId(variant) {{ return `score-${{variant}}`; }}
    function noteId(variant) {{ return `note-${{variant}}`; }}
    function loadAll() {{
      document.querySelectorAll('[data-variant]').forEach(card => {{
        const variant = card.dataset.variant;
        const score = Number(localStorage.getItem(key(`score:${{variant}}`)) || '0');
        const note = localStorage.getItem(key(`note:${{variant}}`)) || '';
        document.getElementById(scoreId(variant)).textContent = String(score);
        document.getElementById(noteId(variant)).value = note;
      }});
    }}
    function vote(variant, delta) {{
      const scoreKey = key(`score:${{variant}}`);
      const next = Number(localStorage.getItem(scoreKey) || '0') + delta;
      localStorage.setItem(scoreKey, String(next));
      document.getElementById(scoreId(variant)).textContent = String(next);
    }}
    function saveNote(variant) {{
      localStorage.setItem(key(`note:${{variant}}`), document.getElementById(noteId(variant)).value);
    }}
    function exportVotes() {{
      const result = {{ run: runKey, exported_at: new Date().toISOString(), preferences: [] }};
      document.querySelectorAll('[data-variant]').forEach(card => {{
        const variant = card.dataset.variant;
        result.preferences.push({{
          variant,
          score: Number(localStorage.getItem(key(`score:${{variant}}`)) || '0'),
          note: localStorage.getItem(key(`note:${{variant}}`)) || ''
        }});
      }});
      document.getElementById('export-box').textContent = JSON.stringify(result, null, 2);
    }}
    function resetVotes() {{
      document.querySelectorAll('[data-variant]').forEach(card => {{
        const variant = card.dataset.variant;
        localStorage.removeItem(key(`score:${{variant}}`));
        localStorage.removeItem(key(`note:${{variant}}`));
      }});
      loadAll();
      exportVotes();
    }}
    loadAll();
    exportVotes();
  </script>
</body>
</html>
"#,
        run_key = html_attr(&run_label.replace(' ', "_")),
        run_label = html_text(&run_label),
        manifest_file = html_attr(manifest_file),
        cards = cards,
    );

    fs::write(&html_path, html)?;
    Ok(html_path)
}

fn html_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn html_attr(value: &str) -> String {
    html_text(value).replace('"', "&quot;")
}

fn filename_slug(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
        } else if !slug.ends_with('_') {
            slug.push('_');
        }
    }
    slug.trim_matches('_').to_string()
}
