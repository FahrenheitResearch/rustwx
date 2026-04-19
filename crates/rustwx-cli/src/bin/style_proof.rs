use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

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
use rustwx_render::{
    Color, ColorScale, ContourLayer, DiscreteColorScale, DomainFrame, ExtendMode, LevelDensity,
    LineworkRole, MapRenderRequest, ProductVisualMode, ProjectedDomain, ProjectedMap,
    RenderDensity, WindBarbLayer, build_projected_map as build_projected_map_from_latlon,
    map_frame_aspect_ratio_for_mode, render_image, solar07::Solar07Palette,
    solar07::solar07_palette,
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
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Conus)]
    region: RegionPreset,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof\\style_proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

#[derive(Debug, Clone, Copy)]
enum ProofVariant {
    Baseline,
    DenseColors,
    LineHierarchy,
    Supersample2x,
    Sharpen,
    Combined,
}

impl ProofVariant {
    fn slug(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::DenseColors => "dense_colors",
            Self::LineHierarchy => "line_hierarchy",
            Self::Supersample2x => "supersample_2x",
            Self::Sharpen => "sharpen",
            Self::Combined => "combined",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Baseline => "Current request as rendered by production defaults.",
            Self::DenseColors => {
                "Increase stepped fill density only; no smoothing, no line changes."
            }
            Self::LineHierarchy => {
                "Make coast/state/frame linework more deliberate via request-side width/color tuning."
            }
            Self::Supersample2x => "Render the full map at 2x and downsample back to 1200x900.",
            Self::Sharpen => "Apply a mild post-render sharpen kernel to the final PNG.",
            Self::Combined => {
                "Dense stepped fills + tuned line hierarchy + 2x supersample + mild sharpen."
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct VariantRecord {
    variant: String,
    description: String,
    output_path: PathBuf,
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
    let (product, selectors, variable_patterns) = fetch_recipe_inputs(recipe, args.model)?;

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

    let variants = [
        ProofVariant::Baseline,
        ProofVariant::DenseColors,
        ProofVariant::LineHierarchy,
        ProofVariant::Supersample2x,
        ProofVariant::Sharpen,
        ProofVariant::Combined,
    ];
    let mut records = Vec::with_capacity(variants.len());
    for variant in variants {
        let image = render_variant(&request, variant)?;
        let output_path = args.out_dir.join(format!(
            "rustwx_{}_{}_{}z_f{:03}_{}_{}_{}.png",
            args.model.as_str().replace('-', "_"),
            args.date,
            latest.cycle.hour_utc,
            args.forecast_hour,
            args.region.slug(),
            recipe.slug,
            variant.slug()
        ));
        DynamicImage::ImageRgba8(image).save(&output_path)?;
        records.push(VariantRecord {
            variant: variant.slug().to_string(),
            description: variant.description().to_string(),
            output_path,
        });
    }

    let manifest_path = args.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}_variants.json",
        args.model.as_str().replace('-', "_"),
        args.date,
        latest.cycle.hour_utc,
        args.forecast_hour,
        args.region.slug(),
        recipe.slug
    ));
    fs::write(&manifest_path, serde_json::to_vec_pretty(&records)?)?;

    for record in &records {
        println!("{}", record.output_path.display());
    }
    println!("{}", manifest_path.display());
    Ok(())
}

fn build_request(
    args: &Args,
    recipe: &PlotRecipe,
    product: &'static str,
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
    render_request.subtitle_left = Some(format!(
        "{} {}Z F{:03}  {}",
        args.date, latest.cycle.hour_utc, args.forecast_hour, args.model
    ));
    render_request.subtitle_right = Some(format!("source: {}", latest.source));
    Ok(render_request)
}

fn render_variant(
    base_request: &MapRenderRequest,
    variant: ProofVariant,
) -> Result<RgbaImage, Box<dyn std::error::Error>> {
    match variant {
        ProofVariant::Baseline => Ok(render_image(base_request)?),
        ProofVariant::DenseColors => {
            let mut request = base_request.clone();
            request.render_density = RenderDensity {
                fill: LevelDensity {
                    multiplier: 16,
                    min_source_level_count: 5,
                },
                palette_multiplier: 16,
            };
            Ok(render_image(&request)?)
        }
        ProofVariant::LineHierarchy => {
            let mut request = base_request.clone();
            apply_line_hierarchy(&mut request);
            Ok(render_image(&request)?)
        }
        ProofVariant::Supersample2x => render_supersampled(base_request, 2),
        ProofVariant::Sharpen => {
            let image = render_image(base_request)?;
            Ok(sharpen_image(&image))
        }
        ProofVariant::Combined => {
            let mut request = base_request.clone();
            request.render_density = RenderDensity {
                fill: LevelDensity {
                    multiplier: 16,
                    min_source_level_count: 5,
                },
                palette_multiplier: 16,
            };
            apply_line_hierarchy(&mut request);
            let image = render_supersampled(&request, 2)?;
            Ok(sharpen_image(&image))
        }
    }
}

fn render_supersampled(
    request: &MapRenderRequest,
    factor: u32,
) -> Result<RgbaImage, Box<dyn std::error::Error>> {
    let mut hires = request.clone();
    hires.width = request.width.saturating_mul(factor);
    hires.height = request.height.saturating_mul(factor);
    scale_overlay_dimensions(&mut hires, factor);
    let rendered = render_image(&hires)?;
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
    if matches!(recipe.style, rustwx_models::RenderStyle::Solar07Height)
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
        rustwx_models::RenderStyle::Solar07Temperature
            | rustwx_models::RenderStyle::Solar07Dewpoint
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
    if recipe.style != rustwx_models::RenderStyle::Solar07Height
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
        rustwx_models::RenderStyle::Solar07Temperature => {
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
        rustwx_models::RenderStyle::Solar07Reflectivity => DiscreteColorScale {
            levels: range_step(5.0, 80.0, 5.0),
            colors: solar07_palette(Solar07Palette::Reflectivity),
            extend: ExtendMode::Both,
            mask_below: Some(5.0),
        },
        rustwx_models::RenderStyle::Solar07Rh => DiscreteColorScale {
            levels: range_step(0.0, 105.0, 5.0),
            colors: solar07_palette(Solar07Palette::Rh),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        rustwx_models::RenderStyle::Solar07Vorticity => DiscreteColorScale {
            levels: range_step(0.0, 48.0, 2.0),
            colors: solar07_palette(Solar07Palette::RelVort),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        rustwx_models::RenderStyle::Solar07Dewpoint => DiscreteColorScale {
            levels: range_step(-40.0, 30.0, 2.0),
            colors: solar07_palette(Solar07Palette::Dewpoint),
            extend: ExtendMode::Both,
            mask_below: None,
        },
        rustwx_models::RenderStyle::Solar07Height => DiscreteColorScale {
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
        } => (range_step(1020.0, 1290.0, 6.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(300),
        } => (range_step(780.0, 1020.0, 6.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(500),
        } => (range_step(450.0, 650.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(700),
        } => (range_step(180.0, 360.0, 3.0), Color::BLACK, 1, true),
        FieldSelector {
            field: CanonicalField::GeopotentialHeight,
            vertical: rustwx_core::VerticalSelector::IsobaricHpa(850),
        } => (range_step(0.0, 200.0, 3.0), Color::BLACK, 1, true),
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
