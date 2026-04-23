use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use grib_core::grib2::Grib2File;
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
    Color, ColorScale, ContourLayer, DiscreteColorScale, ExtendMode, MapRenderRequest,
    ProductVisualMode, ProjectedDomain, ProjectedMap, WindBarbLayer,
    build_projected_map as build_projected_map_from_latlon, map_frame_aspect_ratio_for_mode,
    save_png, weather::WeatherPalette, weather::weather_palette,
};
use serde_json::json;

const DEFAULT_RECIPE: &str = "500mb_temperature_height_winds";
const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;

#[derive(Debug, Parser)]
#[command(
    name = "plot-recipe-proof",
    about = "Generate a selector-backed RustWX atmospheric proof plot"
)]
struct Args {
    #[arg(long, default_value = "gfs")]
    model: ModelId,
    #[arg(long, default_value = DEFAULT_RECIPE)]
    recipe: String,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

#[derive(Debug, Clone)]
struct Timing {
    fetch_ms: u128,
    parse_ms: u128,
    extract_ms: u128,
    project_ms: u128,
    render_ms: u128,
    total_ms: u128,
    fetch_cache_hit: bool,
    extract_cache_hits: usize,
    extract_cache_misses: usize,
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

    let total_start = Instant::now();
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

    let fetch_start = Instant::now();
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
            bytes_path: rustwx_io::fetch_cache_paths(&cache_root, &fetch).0,
            metadata_path: rustwx_io::fetch_cache_paths(&cache_root, &fetch).1,
        }
    } else {
        fetch_bytes_with_cache(&fetch, &cache_root, true)?
    };
    let fetch_ms = fetch_start.elapsed().as_millis();

    let extract_start = Instant::now();
    let mut extracted = HashMap::new();
    let mut missing_selectors = Vec::new();
    let mut extract_cache_hits = 0usize;
    for selector in selectors {
        if !args.no_cache {
            if let Some(cached) = load_cached_selected_field(&cache_root, &fetch, selector)? {
                extracted.insert(selector, cached.field);
                extract_cache_hits += 1;
                continue;
            }
        }
        missing_selectors.push(selector);
    }

    let parse_start = Instant::now();
    let grib = if missing_selectors.is_empty() {
        None
    } else {
        Some(Grib2File::from_bytes(&fetched.result.bytes)?)
    };
    let parse_ms = parse_start.elapsed().as_millis();

    if let Some(grib) = grib.as_ref() {
        for selector in &missing_selectors {
            let field = extract_field_from_grib2(grib, *selector)?;
            if !args.no_cache {
                store_cached_selected_field(&cache_root, &fetch, &field)?;
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
    let extract_ms = extract_start.elapsed().as_millis();

    let project_start = Instant::now();
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
    let project_ms = project_start.elapsed().as_millis();

    let render_start = Instant::now();
    let mut request = build_render_request(recipe, &filled, &extracted, projected, args.region);
    request.subtitle_left = Some(format!(
        "{} {}Z F{:03}  {}",
        args.date, latest.cycle.hour_utc, args.forecast_hour, args.model
    ));
    request.subtitle_right = Some(format!("source: {}", latest.source));

    let output_path = args.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}.png",
        args.model.as_str().replace('-', "_"),
        args.date,
        latest.cycle.hour_utc,
        args.forecast_hour,
        format!("{}_{}", args.region.slug(), recipe.slug)
    ));
    save_png(&request, &output_path)?;
    let render_ms = render_start.elapsed().as_millis();

    let timing = Timing {
        fetch_ms,
        parse_ms,
        extract_ms,
        project_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
        fetch_cache_hit: fetched.cache_hit,
        extract_cache_hits,
        extract_cache_misses: missing_selectors.len(),
    };
    let timing_path = args.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_timing.json",
        args.model.as_str().replace('-', "_"),
        args.date,
        latest.cycle.hour_utc,
        args.forecast_hour,
        format!("{}_{}", args.region.slug(), recipe.slug)
    ));
    fs::write(
        &timing_path,
        serde_json::to_vec_pretty(&json!({
            "model": args.model.as_str(),
            "recipe": recipe.slug,
            "region": args.region.slug(),
            "cycle": latest.cycle,
            "forecast_hour": args.forecast_hour,
            "source": latest.source.as_str(),
            "grib_url": fetched.result.url,
            "cache": {
                "root": cache_root,
                "fetch_hit": timing.fetch_cache_hit,
                "extract_hits": timing.extract_cache_hits,
                "extract_misses": timing.extract_cache_misses,
            },
            "panel_path": output_path,
            "timing_ms": {
                "fetch": timing.fetch_ms,
                "parse": timing.parse_ms,
                "extract": timing.extract_ms,
                "project": timing.project_ms,
                "render": timing.render_ms,
                "total": timing.total_ms,
            }
        }))?,
    )?;

    println!("{}", output_path.display());
    println!("{}", timing_path.display());
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_recipe_inputs_uses_whole_file_plan_for_ecmwf_upper_air_recipe() {
        let recipe = plot_recipe("500mb_temperature_height_winds").unwrap();
        let (product, selectors, variable_patterns) =
            fetch_recipe_inputs(recipe, ModelId::EcmwfOpenData).unwrap();

        assert_eq!(product, "oper");
        assert!(variable_patterns.is_empty());
        assert_eq!(
            selectors,
            vec![
                FieldSelector::isobaric(CanonicalField::Temperature, 500),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
                FieldSelector::isobaric(CanonicalField::UWind, 500),
                FieldSelector::isobaric(CanonicalField::VWind, 500),
            ]
        );
    }

    #[test]
    fn fetch_recipe_inputs_surfaces_specific_recipe_gate_reason() {
        let recipe = plot_recipe("700mb_dewpoint_height_winds").unwrap();
        let err = fetch_recipe_inputs(recipe, ModelId::EcmwfOpenData)
            .unwrap_err()
            .to_string();

        assert!(err.contains("700mb Dewpoint"));
        assert!(err.contains("dewpoint"));
    }
}
