use anyhow::{anyhow, Context, Result};
use clap::Parser;
use image::{DynamicImage, ImageFormat, RgbaImage};
use rustwx_cross_section::{
    decompose_wind_grid, render_scalar_section_profile, Color, CrossSectionProduct,
    CrossSectionStyle, Insets, ScalarContourOverlayBundle, SectionMetadata, VerticalAxis,
    WindOverlayBundle, WindOverlayStyle, ALL_CROSS_SECTION_PRODUCTS,
};
use rustwx_products::cross_section::{
    build_pressure_cross_section_product_values, missing_pressure_volume_requirements,
    PressureCrossSectionOptionalProductFields, PressureCrossSectionProductInputs,
};
use rustwx_products::volume_store::{
    RouteDef, RouteSectionPrimitives, SurfaceTerrainStore, VolumeStore,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const DEFAULT_PRODUCTS: &str = "all";
#[derive(Debug, Parser)]
#[command(
    name = "volume-store-cross-section-render",
    about = "Render rustwx-cross-section PNGs directly from a pressure VolumeStore"
)]
struct Args {
    #[arg(
        long,
        default_value = "proof/hrrr_pressure_volume_store_latest_f000/store"
    )]
    store: PathBuf,
    #[arg(
        long,
        default_value = "proof/cafire_local_artifacts/proof_wall_8800/volume_cross_sections"
    )]
    out_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_PRODUCTS)]
    products: String,
    #[arg(long, default_value_t = 0)]
    hour: u8,
    #[arg(
        long,
        help = "Forecast hours to render: all, a comma list like 0,1,2, or ranges like 0-48"
    )]
    hours: Option<String>,
    #[arg(long, default_value_t = 5.0)]
    spacing_km: f32,
    #[arg(
        long,
        default_value_t = 100.0,
        help = "Top pressure in hPa to include in the section; lower pressure levels above this are omitted"
    )]
    top_pressure_hpa: f64,
    #[arg(long, default_value_t = 1400)]
    width: u32,
    #[arg(long, default_value_t = 820)]
    height: u32,
    #[arg(long)]
    route_id: Option<String>,
    #[arg(long)]
    route_name: Option<String>,
    #[arg(long, allow_hyphen_values = true)]
    start_lat: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    start_lon: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    end_lat: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    end_lon: Option<f64>,
}

#[derive(Debug, Clone)]
struct RouteSpec {
    id: String,
    name: String,
    start: (f64, f64),
    end: (f64, f64),
}

struct SampledRouteInputs {
    primitives: RouteSectionPrimitives,
    levels: Vec<u16>,
    n_points: usize,
    n_levels: usize,
    temperature_c: Vec<f64>,
    mixing_ratio_kgkg: Vec<f64>,
    u_ms: Vec<f64>,
    v_ms: Vec<f64>,
    height_m: Vec<f64>,
    omega_pa_s: Option<Vec<f64>>,
    absolute_vorticity_s: Option<Vec<f64>>,
    cloud_liquid_kgkg: Option<Vec<f64>>,
    cloud_ice_kgkg: Option<Vec<f64>>,
    rain_kgkg: Option<Vec<f64>>,
    snow_kgkg: Option<Vec<f64>>,
    graupel_kgkg: Option<Vec<f64>>,
    section_wind_ms: Vec<f64>,
    pressure_hpa: Vec<f64>,
    distances: Vec<f64>,
    terrain: Option<rustwx_cross_section::TerrainProfile>,
    sample_ms: u128,
    terrain_ms: u128,
}

#[derive(Debug, Serialize)]
struct RenderSummary {
    generated_at_store_cycle: String,
    store: String,
    out_dir: String,
    hour: u8,
    forecast_hours: Vec<u8>,
    spacing_km: f32,
    top_pressure_hpa: f64,
    route_count: usize,
    product_count: usize,
    rendered_count: usize,
    skipped_count: usize,
    total_ms: u128,
    outputs: Vec<RenderedOutput>,
    skipped: Vec<SkippedProduct>,
}

#[derive(Debug, Serialize)]
struct RenderedOutput {
    route_id: String,
    route_name: String,
    hour: u8,
    product: String,
    product_label: String,
    png_path: String,
    webp_path: String,
    summary_path: String,
    samples: usize,
    levels: usize,
    top_pressure_hpa: f64,
    values: usize,
    min_value: f32,
    max_value: f32,
    sample_ms: u128,
    terrain_ms: u128,
    terrain_mask: bool,
    product_ms: u128,
    render_ms: u128,
    total_ms: u128,
}

#[derive(Debug, Serialize)]
struct SkippedProduct {
    product: String,
    product_label: String,
    missing_requirements: Vec<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let total_start = Instant::now();
    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create {}", args.out_dir.display()))?;
    let store = VolumeStore::open(&args.store).map_err(|err| anyhow!(err.to_string()))?;
    let hours = parse_hours(
        args.hours.as_deref(),
        args.hour,
        &store.manifest().forecast_hours,
    )?;
    let products = parse_products(&args.products)?;
    let terrain_store =
        SurfaceTerrainStore::open_optional(&args.store).map_err(|err| anyhow!(err.to_string()))?;
    let available_variables = store
        .manifest()
        .variables
        .iter()
        .map(|variable| variable.name.as_str())
        .collect::<Vec<_>>();
    let mut render_products = Vec::new();
    let mut skipped = Vec::new();
    for product in products {
        let missing = missing_pressure_volume_requirements(product, &available_variables);
        if missing.is_empty() {
            render_products.push(product);
        } else {
            skipped.push(SkippedProduct {
                product: product.slug().to_string(),
                product_label: product.display_name().to_string(),
                missing_requirements: missing,
            });
        }
    }
    let routes = routes_from_args(&args)?;
    let mut outputs = Vec::new();
    if !render_products.is_empty() {
        for route in &routes {
            for hour in &hours {
                let sampled =
                    sample_route_inputs(&args, &store, terrain_store.as_ref(), route, *hour)?;
                for product in &render_products {
                    outputs.push(render_one(&args, &store, route, &sampled, *product, *hour)?);
                }
            }
        }
    }
    let report = RenderSummary {
        generated_at_store_cycle: store.manifest().cycle.clone(),
        store: args.store.display().to_string(),
        out_dir: args.out_dir.display().to_string(),
        hour: *hours.first().unwrap_or(&args.hour),
        forecast_hours: hours,
        spacing_km: args.spacing_km,
        top_pressure_hpa: args.top_pressure_hpa,
        route_count: routes.len(),
        product_count: render_products.len() + skipped.len(),
        rendered_count: outputs.len(),
        skipped_count: skipped.len(),
        total_ms: total_start.elapsed().as_millis(),
        outputs,
        skipped,
    };
    let report_path = args.out_dir.join("volume_cross_section_render_report.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn sample_route_inputs(
    args: &Args,
    store: &VolumeStore,
    terrain_store: Option<&SurfaceTerrainStore>,
    route: &RouteSpec,
    hour: u8,
) -> Result<SampledRouteInputs> {
    let levels =
        filter_levels_by_top_pressure(&store.manifest().levels_hpa, args.top_pressure_hpa)?;
    let route_def = RouteDef {
        id: route.id.clone(),
        name: route.name.clone(),
        points: vec![route.start, route.end],
        sample_spacing_km: args.spacing_km,
    };
    let sample_start = Instant::now();
    let manifest_variables = store
        .manifest()
        .variables
        .iter()
        .map(|variable| variable.name.clone())
        .collect::<Vec<_>>();
    let has_var = |name: &str| {
        manifest_variables
            .iter()
            .any(|variable| variable.eq_ignore_ascii_case(name))
    };
    let mut sample_variables = vec![
        "TMP".to_string(),
        "SPFH".to_string(),
        "UGRD".to_string(),
        "VGRD".to_string(),
        "HGT".to_string(),
    ];
    for variable in ["VVEL", "ABSV", "CLWMR", "ICMR", "RWMR", "SNMR", "GRLE"] {
        if has_var(variable) {
            sample_variables.push(variable.to_string());
        }
    }
    let sample_variable_refs = sample_variables
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let primitives = store
        .sample_route_3d(&route_def, &sample_variable_refs, hour, &levels)
        .map_err(|err| anyhow!(err.to_string()))?;
    let sample_ms = sample_start.elapsed().as_millis();

    let n_points = primitives.route_samples.len();
    let n_levels = levels.len();
    let mut temperature_c = vec![f64::NAN; n_levels * n_points];
    let mut mixing_ratio_kgkg = vec![f64::NAN; n_levels * n_points];
    let mut u_ms = vec![f64::NAN; n_levels * n_points];
    let mut v_ms = vec![f64::NAN; n_levels * n_points];
    let mut height_m = vec![f64::NAN; n_levels * n_points];
    let mut omega_pa_s = has_var("VVEL").then(|| vec![f64::NAN; n_levels * n_points]);
    let mut absolute_vorticity_s = has_var("ABSV").then(|| vec![f64::NAN; n_levels * n_points]);
    let mut cloud_liquid_kgkg = has_var("CLWMR").then(|| vec![f64::NAN; n_levels * n_points]);
    let mut cloud_ice_kgkg = has_var("ICMR").then(|| vec![f64::NAN; n_levels * n_points]);
    let mut rain_kgkg = has_var("RWMR").then(|| vec![f64::NAN; n_levels * n_points]);
    let mut snow_kgkg = has_var("SNMR").then(|| vec![f64::NAN; n_levels * n_points]);
    let mut graupel_kgkg = has_var("GRLE").then(|| vec![f64::NAN; n_levels * n_points]);
    fill_inputs(
        &primitives,
        &levels,
        n_points,
        &mut temperature_c,
        &mut mixing_ratio_kgkg,
        &mut u_ms,
        &mut v_ms,
        &mut height_m,
        omega_pa_s.as_deref_mut(),
        absolute_vorticity_s.as_deref_mut(),
        cloud_liquid_kgkg.as_deref_mut(),
        cloud_ice_kgkg.as_deref_mut(),
        rain_kgkg.as_deref_mut(),
        snow_kgkg.as_deref_mut(),
        graupel_kgkg.as_deref_mut(),
    )?;
    let section_wind_ms = section_wind_from_route(&primitives, n_levels, n_points, &u_ms, &v_ms)?;
    let pressure_hpa = levels
        .iter()
        .flat_map(|level| std::iter::repeat(f64::from(*level)).take(n_points))
        .collect::<Vec<_>>();
    let distances = primitives
        .route_samples
        .iter()
        .map(|sample| f64::from(sample.distance_km))
        .collect::<Vec<_>>();
    let terrain_start = Instant::now();
    let terrain = terrain_store
        .map(|terrain_store| {
            terrain_store.terrain_profile(
                hour,
                &primitives.route_samples,
                distances.clone(),
                store.manifest().grid.nx(),
                store.manifest().grid.ny(),
            )
        })
        .transpose()
        .map_err(|err| anyhow!(err.to_string()))?;
    let terrain_ms = terrain_start.elapsed().as_millis();
    Ok(SampledRouteInputs {
        primitives,
        levels,
        n_points,
        n_levels,
        temperature_c,
        mixing_ratio_kgkg,
        u_ms,
        v_ms,
        height_m,
        omega_pa_s,
        absolute_vorticity_s,
        cloud_liquid_kgkg,
        cloud_ice_kgkg,
        rain_kgkg,
        snow_kgkg,
        graupel_kgkg,
        section_wind_ms,
        pressure_hpa,
        distances,
        terrain,
        sample_ms,
        terrain_ms,
    })
}

fn filter_levels_by_top_pressure(levels: &[u16], top_pressure_hpa: f64) -> Result<Vec<u16>> {
    if !top_pressure_hpa.is_finite() || top_pressure_hpa <= 0.0 {
        return Err(anyhow!(
            "top pressure must be a positive finite hPa value, got {top_pressure_hpa}"
        ));
    }
    let filtered = levels
        .iter()
        .copied()
        .filter(|level| f64::from(*level) >= top_pressure_hpa)
        .collect::<Vec<_>>();
    if filtered.len() < 2 {
        return Err(anyhow!(
            "top pressure {} hPa leaves fewer than two pressure levels from store levels {:?}",
            top_pressure_hpa,
            levels
        ));
    }
    Ok(filtered)
}

fn render_one(
    args: &Args,
    store: &VolumeStore,
    route: &RouteSpec,
    sampled: &SampledRouteInputs,
    product: CrossSectionProduct,
    hour: u8,
) -> Result<RenderedOutput> {
    let total_start = Instant::now();
    let product_start = Instant::now();
    let section_values = build_pressure_cross_section_product_values(
        product,
        PressureCrossSectionProductInputs {
            pressure_hpa: &sampled.pressure_hpa,
            temperature_c: &sampled.temperature_c,
            mixing_ratio_kgkg: &sampled.mixing_ratio_kgkg,
            u_ms: &sampled.u_ms,
            v_ms: &sampled.v_ms,
            optional: PressureCrossSectionOptionalProductFields {
                height_m: Some(&sampled.height_m),
                distance_km: Some(&sampled.distances),
                section_wind_ms: Some(&sampled.section_wind_ms),
                omega_pa_s: sampled.omega_pa_s.as_deref(),
                absolute_vorticity_s: sampled.absolute_vorticity_s.as_deref(),
                cloud_liquid_kgkg: sampled.cloud_liquid_kgkg.as_deref(),
                cloud_ice_kgkg: sampled.cloud_ice_kgkg.as_deref(),
                rain_kgkg: sampled.rain_kgkg.as_deref(),
                snow_kgkg: sampled.snow_kgkg.as_deref(),
                graupel_kgkg: sampled.graupel_kgkg.as_deref(),
                point_count: Some(sampled.n_points),
                level_count: Some(sampled.n_levels),
                ..PressureCrossSectionOptionalProductFields::default()
            },
        },
    )
    .map_err(|err| anyhow!(err.to_string()))?
    .into_iter()
    .map(|value| value as f32)
    .collect::<Vec<_>>();
    let product_ms = product_start.elapsed().as_millis();

    let metadata = SectionMetadata::new()
        .titled(format!("HRRR Cross-Section: {}", product.display_name()))
        .field(product.slug(), product.units())
        .sourced_from("rustwx pressure VolumeStore")
        .valid_at(format!("{} F{:03}", store.manifest().cycle, hour))
        .with_attribute("route_label", route.name.as_str())
        .with_attribute("start_label", coord_label(route.start))
        .with_attribute("end_label", coord_label(route.end))
        .with_attribute("product_key", product.slug())
        .with_attribute("store_cycle", store.manifest().cycle.clone())
        .with_attribute("init_label", store.manifest().cycle.clone())
        .with_attribute("forecast_hour", format!("F{:03}", hour))
        .with_attribute("top_pressure_hpa", format!("{:.0}", args.top_pressure_hpa))
        .with_attribute(
            "valid_time",
            format!("{} +{:03}h", store.manifest().cycle, hour),
        );
    let mut section = rustwx_cross_section::ScalarSection::new(
        sampled.distances.clone(),
        VerticalAxis::pressure_hpa(
            sampled
                .levels
                .iter()
                .map(|level| f64::from(*level))
                .collect(),
        )?,
        section_values,
    )?
    .with_metadata(metadata);
    if let Some(terrain) = sampled.terrain.clone() {
        section = section.with_terrain(terrain)?;
    }

    let style = style_for_product(product);
    let render_start = Instant::now();
    let wind_overlay = wind_overlay_from_inputs(
        &sampled.primitives,
        sampled.n_levels,
        sampled.n_points,
        &sampled.u_ms,
        &sampled.v_ms,
    )?;
    let contour_overlays = reference_contour_overlays(sampled)?;
    let request = style
        .to_render_request()
        .with_dimensions(args.width, args.height)
        .with_margins(Insets {
            left: 96,
            right: 130,
            top: 78,
            bottom: 112,
        })
        .with_isotherms(Vec::new(), None)
        .with_contour_overlays(contour_overlays)
        .with_wind_overlay(wind_overlay);
    let (rendered, _timing) = render_scalar_section_profile(&section, &request)?;
    let render_ms = render_start.elapsed().as_millis();

    let stem = format!(
        "volume_store_{}_f{:03}_{}_cross_section",
        route.id,
        hour,
        product.slug()
    );
    let png_path = args.out_dir.join(format!("{stem}.png"));
    let webp_path = args.out_dir.join(format!("{stem}.webp"));
    save_rgba_images(&png_path, &webp_path, &rendered)?;

    let finite = section
        .values()
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    let min_value = finite.iter().copied().fold(f32::INFINITY, f32::min);
    let max_value = finite.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let output = RenderedOutput {
        route_id: route.id.clone(),
        route_name: route.name.clone(),
        hour,
        product: product.slug().to_string(),
        product_label: product.display_name().to_string(),
        png_path: png_path.display().to_string(),
        webp_path: webp_path.display().to_string(),
        summary_path: args
            .out_dir
            .join(format!("{stem}.json"))
            .display()
            .to_string(),
        samples: sampled.n_points,
        levels: sampled.n_levels,
        top_pressure_hpa: args.top_pressure_hpa,
        values: section.values().len(),
        min_value,
        max_value,
        sample_ms: sampled.sample_ms,
        terrain_ms: sampled.terrain_ms,
        terrain_mask: sampled.terrain.is_some(),
        product_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
    };
    fs::write(&output.summary_path, serde_json::to_vec_pretty(&output)?)?;
    Ok(output)
}

fn fill_inputs(
    primitives: &RouteSectionPrimitives,
    levels: &[u16],
    n_points: usize,
    temperature_c: &mut [f64],
    mixing_ratio_kgkg: &mut [f64],
    u_ms: &mut [f64],
    v_ms: &mut [f64],
    height_m: &mut [f64],
    mut omega_pa_s: Option<&mut [f64]>,
    mut absolute_vorticity_s: Option<&mut [f64]>,
    mut cloud_liquid_kgkg: Option<&mut [f64]>,
    mut cloud_ice_kgkg: Option<&mut [f64]>,
    mut rain_kgkg: Option<&mut [f64]>,
    mut snow_kgkg: Option<&mut [f64]>,
    mut graupel_kgkg: Option<&mut [f64]>,
) -> Result<()> {
    let level_index = levels
        .iter()
        .enumerate()
        .map(|(index, level)| (*level, index))
        .collect::<BTreeMap<_, _>>();
    for value in &primitives.values {
        let Some(level_index) = level_index.get(&value.level_hpa).copied() else {
            continue;
        };
        let flat = level_index * n_points + value.sample_index;
        match value.variable.as_str() {
            "TMP" => temperature_c[flat] = f64::from(value.value),
            "SPFH" => mixing_ratio_kgkg[flat] = f64::from(value.value),
            "UGRD" => u_ms[flat] = f64::from(value.value),
            "VGRD" => v_ms[flat] = f64::from(value.value),
            "HGT" => height_m[flat] = f64::from(value.value),
            "VVEL" => {
                if let Some(values) = omega_pa_s.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            "ABSV" => {
                if let Some(values) = absolute_vorticity_s.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            "CLWMR" => {
                if let Some(values) = cloud_liquid_kgkg.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            "ICMR" | "CIMIXR" => {
                if let Some(values) = cloud_ice_kgkg.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            "RWMR" => {
                if let Some(values) = rain_kgkg.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            "SNMR" => {
                if let Some(values) = snow_kgkg.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            "GRLE" => {
                if let Some(values) = graupel_kgkg.as_deref_mut() {
                    values[flat] = f64::from(value.value);
                }
            }
            _ => {}
        }
    }
    if temperature_c.iter().any(|value| !value.is_finite())
        || mixing_ratio_kgkg.iter().any(|value| !value.is_finite())
        || u_ms.iter().any(|value| !value.is_finite())
        || v_ms.iter().any(|value| !value.is_finite())
        || height_m.iter().any(|value| !value.is_finite())
    {
        return Err(anyhow!(
            "VolumeStore route sample did not return a complete TMP/SPFH/UGRD/VGRD/HGT section"
        ));
    }
    for (name, values) in [
        ("VVEL", omega_pa_s.as_deref()),
        ("ABSV", absolute_vorticity_s.as_deref()),
        ("CLWMR", cloud_liquid_kgkg.as_deref()),
        ("ICMR", cloud_ice_kgkg.as_deref()),
        ("RWMR", rain_kgkg.as_deref()),
        ("SNMR", snow_kgkg.as_deref()),
        ("GRLE", graupel_kgkg.as_deref()),
    ] {
        if let Some(values) = values {
            if values.iter().any(|value| !value.is_finite()) {
                return Err(anyhow!(
                    "VolumeStore route sample did not return a complete {name} section"
                ));
            }
        }
    }
    Ok(())
}

fn section_wind_from_route(
    primitives: &RouteSectionPrimitives,
    n_levels: usize,
    n_points: usize,
    u_ms: &[f64],
    v_ms: &[f64],
) -> Result<Vec<f64>> {
    let expected = n_levels * n_points;
    if u_ms.len() != expected || v_ms.len() != expected {
        return Err(anyhow!(
            "wind arrays have invalid shape: expected {expected}, got u={} v={}",
            u_ms.len(),
            v_ms.len()
        ));
    }
    if primitives.route_samples.len() != n_points {
        return Err(anyhow!(
            "route sample count mismatch: expected {n_points}, got {}",
            primitives.route_samples.len()
        ));
    }
    let mut section_wind = Vec::with_capacity(expected);
    for level_index in 0..n_levels {
        for point_index in 0..n_points {
            let flat = level_index * n_points + point_index;
            let sample = &primitives.route_samples[point_index];
            section_wind.push(
                u_ms[flat] * f64::from(sample.route_unit_u)
                    + v_ms[flat] * f64::from(sample.route_unit_v),
            );
        }
    }
    Ok(section_wind)
}

fn reference_contour_overlays(
    sampled: &SampledRouteInputs,
) -> Result<Vec<ScalarContourOverlayBundle>> {
    let axis = VerticalAxis::pressure_hpa(
        sampled
            .levels
            .iter()
            .map(|level| f64::from(*level))
            .collect(),
    )?;
    let theta_values = potential_temperature_k(&sampled.pressure_hpa, &sampled.temperature_c)?;
    let theta_levels = potential_temperature_levels(&theta_values);
    let mut theta_section = rustwx_cross_section::ScalarSection::new(
        sampled.distances.clone(),
        axis.clone(),
        theta_values,
    )?
    .with_metadata(SectionMetadata::new().field("potential_temperature", "K"));
    if let Some(terrain) = sampled.terrain.clone() {
        theta_section = theta_section.with_terrain(terrain)?;
    }

    let mut temperature_section = rustwx_cross_section::ScalarSection::new(
        sampled.distances.clone(),
        axis,
        sampled
            .temperature_c
            .iter()
            .map(|value| *value as f32)
            .collect(),
    )?
    .with_metadata(SectionMetadata::new().field("temperature", "C"));
    if let Some(terrain) = sampled.terrain.clone() {
        temperature_section = temperature_section.with_terrain(terrain)?;
    }

    Ok(vec![
        ScalarContourOverlayBundle::new(theta_section, theta_levels)
            .with_label("Potential Temp (K)")
            .with_units("K")
            .with_color(Color::rgba(20, 24, 29, 205)),
        ScalarContourOverlayBundle::new(temperature_section, Vec::new())
            .with_highlight(0.0)
            .with_label("0 C Isotherm")
            .with_units("C")
            .with_highlight_color(Color::rgb(214, 34, 190)),
    ])
}

fn potential_temperature_k(pressure_hpa: &[f64], temperature_c: &[f64]) -> Result<Vec<f32>> {
    if pressure_hpa.len() != temperature_c.len() {
        return Err(anyhow!(
            "potential temperature input length mismatch: pressure={} temperature={}",
            pressure_hpa.len(),
            temperature_c.len()
        ));
    }
    Ok(pressure_hpa
        .iter()
        .zip(temperature_c.iter())
        .map(|(&pressure_hpa, &temperature_c)| {
            if pressure_hpa > 0.0 {
                ((temperature_c + 273.15) * (1000.0 / pressure_hpa).powf(0.2854)) as f32
            } else {
                f32::NAN
            }
        })
        .collect())
}

fn potential_temperature_levels(values: &[f32]) -> Vec<f32> {
    let finite = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return Vec::new();
    }
    let min_value = finite.iter().copied().fold(f32::INFINITY, f32::min);
    let max_value = finite.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let mut step = 4.0;
    let range = max_value - min_value;
    if range / step > 28.0 {
        step = 8.0;
    }
    let start = ((min_value / step).floor() * step).max(240.0);
    let end = ((max_value / step).ceil() * step).min(440.0);
    let mut levels = Vec::new();
    let mut level = start;
    while level <= end + step * 0.25 {
        levels.push(level);
        level += step;
    }
    levels
}

fn wind_overlay_from_inputs(
    primitives: &RouteSectionPrimitives,
    n_levels: usize,
    n_points: usize,
    u_ms: &[f64],
    v_ms: &[f64],
) -> Result<WindOverlayBundle> {
    let u = u_ms.iter().map(|value| *value as f32).collect::<Vec<_>>();
    let v = v_ms.iter().map(|value| *value as f32).collect::<Vec<_>>();
    let bearings = primitives
        .route_samples
        .iter()
        .map(|sample| {
            let bearing = f64::from(sample.route_unit_u)
                .atan2(f64::from(sample.route_unit_v))
                .to_degrees();
            if bearing < 0.0 {
                bearing + 360.0
            } else {
                bearing
            }
        })
        .collect::<Vec<_>>();
    Ok(WindOverlayBundle::new(
        decompose_wind_grid(&u, &v, n_levels, n_points, &bearings)?,
        WindOverlayStyle {
            stride_points: 6,
            stride_levels: 2,
            min_speed_ms: 6.0,
            max_speed_ms: 40.0,
            base_length_px: 18.0,
            max_length_px: 18.0,
            cross_tick_px: 6.0,
            ..WindOverlayStyle::default()
        },
    )
    .with_label("VolumeStore Section-Relative Wind"))
}

fn style_for_product(product: CrossSectionProduct) -> CrossSectionStyle {
    let mut style = CrossSectionStyle::new(product);
    match product {
        CrossSectionProduct::Temperature => {
            style = style.with_value_range(-36.0, 30.0);
        }
        CrossSectionProduct::SpecificHumidity => {
            style = style.with_value_ticks(vec![0.0, 2.0, 4.0, 8.0, 12.0, 16.0]);
        }
        CrossSectionProduct::WindSpeed => {
            style = style.with_value_ticks(vec![0.0, 20.0, 40.0, 60.0, 80.0, 100.0]);
        }
        CrossSectionProduct::ThetaE => {
            style = style.with_value_range(284.0, 356.0);
        }
        CrossSectionProduct::VaporPressureDeficit => {
            style = style.with_value_ticks(vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);
        }
        CrossSectionProduct::MoistureTransport => {
            style = style.with_value_ticks(vec![0.0, 25.0, 50.0, 100.0, 150.0, 200.0]);
        }
        CrossSectionProduct::FireWeather => {
            style = style.with_value_ticks(vec![0.0, 15.0, 25.0, 40.0, 60.0, 80.0, 100.0]);
        }
        _ => {}
    }
    style
}

fn parse_products(value: &str) -> Result<Vec<CrossSectionProduct>> {
    let normalized = value.trim();
    if normalized.eq_ignore_ascii_case("all") || normalized.eq_ignore_ascii_case("wxsection") {
        return Ok(ALL_CROSS_SECTION_PRODUCTS.to_vec());
    }
    normalized
        .split(',')
        .filter_map(|raw| {
            let trimmed = raw.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        })
        .map(|name| {
            CrossSectionProduct::from_name(name)
                .ok_or_else(|| anyhow!("unknown cross-section product '{name}'"))
        })
        .collect()
}

fn parse_hours(value: Option<&str>, default_hour: u8, available_hours: &[u8]) -> Result<Vec<u8>> {
    let available = available_hours.iter().copied().collect::<BTreeSet<_>>();
    if available.is_empty() {
        return Err(anyhow!(
            "VolumeStore manifest does not list any forecast hours"
        ));
    }
    let requested = match value.map(str::trim).filter(|item| !item.is_empty()) {
        None => vec![default_hour],
        Some(raw) if raw.eq_ignore_ascii_case("all") => available.iter().copied().collect(),
        Some(raw) => {
            let mut hours = BTreeSet::new();
            for part in raw.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                if let Some((start, end)) = part.split_once('-') {
                    let start = parse_hour_token(start.trim())?;
                    let end = parse_hour_token(end.trim())?;
                    if end < start {
                        return Err(anyhow!("invalid hour range '{part}': end is before start"));
                    }
                    for hour in start..=end {
                        hours.insert(hour);
                    }
                } else {
                    hours.insert(parse_hour_token(part)?);
                }
            }
            hours.into_iter().collect()
        }
    };
    if requested.is_empty() {
        return Err(anyhow!("no forecast hours were requested"));
    }
    let unsupported = requested
        .iter()
        .copied()
        .filter(|hour| !available.contains(hour))
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        let supported = available
            .iter()
            .map(|hour| format!("f{hour:03}"))
            .collect::<Vec<_>>()
            .join(", ");
        let unsupported = unsupported
            .iter()
            .map(|hour| format!("f{hour:03}"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow!(
            "requested unsupported forecast hours {unsupported}; store supports {supported}"
        ));
    }
    Ok(requested)
}

fn parse_hour_token(value: &str) -> Result<u8> {
    value
        .trim_start_matches(['f', 'F'])
        .parse::<u8>()
        .with_context(|| format!("invalid forecast hour '{value}'"))
}

fn routes_from_args(args: &Args) -> Result<Vec<RouteSpec>> {
    let custom_requested = args.start_lat.is_some()
        || args.start_lon.is_some()
        || args.end_lat.is_some()
        || args.end_lon.is_some()
        || args.route_id.is_some()
        || args.route_name.is_some();
    if !custom_requested {
        return Ok(ca_routes());
    }

    let start_lat = required_arg(args.start_lat, "--start-lat")?;
    let start_lon = required_arg(args.start_lon, "--start-lon")?;
    let end_lat = required_arg(args.end_lat, "--end-lat")?;
    let end_lon = required_arg(args.end_lon, "--end-lon")?;
    validate_coordinate(start_lat, start_lon, "start")?;
    validate_coordinate(end_lat, end_lon, "end")?;
    if (start_lat - end_lat).abs() < 1.0e-6 && (start_lon - end_lon).abs() < 1.0e-6 {
        return Err(anyhow!("custom route start and end points are identical"));
    }

    let id = sanitize_route_id(args.route_id.as_deref().unwrap_or("custom_route"));
    let name = args.route_name.clone().unwrap_or_else(|| {
        format!(
            "Custom {:.3},{:.3} to {:.3},{:.3}",
            start_lat, start_lon, end_lat, end_lon
        )
    });
    Ok(vec![RouteSpec {
        id,
        name,
        start: (start_lat, start_lon),
        end: (end_lat, end_lon),
    }])
}

fn required_arg(value: Option<f64>, name: &str) -> Result<f64> {
    value.ok_or_else(|| anyhow!("{name} is required when rendering a custom route"))
}

fn validate_coordinate(lat: f64, lon: f64, label: &str) -> Result<()> {
    if !lat.is_finite() || !lon.is_finite() {
        return Err(anyhow!("{label} coordinate must be finite"));
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(anyhow!("{label} latitude {lat} is outside [-90, 90]"));
    }
    if !(-180.0..=180.0).contains(&lon) {
        return Err(anyhow!("{label} longitude {lon} is outside [-180, 180]"));
    }
    Ok(())
}

fn sanitize_route_id(value: &str) -> String {
    let mut sanitized = String::new();
    let mut last_was_underscore = false;
    for ch in value.chars() {
        let next = if ch.is_ascii_alphanumeric() {
            Some(ch.to_ascii_lowercase())
        } else if ch == '-' || ch == '_' || ch.is_whitespace() {
            Some('_')
        } else {
            None
        };
        let Some(next) = next else {
            continue;
        };
        if next == '_' {
            if last_was_underscore || sanitized.is_empty() {
                continue;
            }
            last_was_underscore = true;
        } else {
            last_was_underscore = false;
        }
        sanitized.push(next);
        if sanitized.len() >= 80 {
            break;
        }
    }
    let trimmed = sanitized.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "custom_route".to_string()
    } else {
        trimmed
    }
}

fn ca_routes() -> Vec<RouteSpec> {
    vec![
        RouteSpec {
            id: "bay_sierra".to_string(),
            name: "Bay to Sierra".to_string(),
            start: (37.7749, -122.4194),
            end: (38.5788, -119.7513),
        },
        RouteSpec {
            id: "redding_tahoe".to_string(),
            name: "Redding to Tahoe".to_string(),
            start: (40.5865, -122.3917),
            end: (39.0968, -120.0324),
        },
        RouteSpec {
            id: "la_southern_sierra".to_string(),
            name: "LA Basin to Southern Sierra".to_string(),
            start: (34.0522, -118.2437),
            end: (36.5786, -118.2923),
        },
        RouteSpec {
            id: "north_coast_valley".to_string(),
            name: "North Coast to Central Valley".to_string(),
            start: (40.8021, -124.1637),
            end: (39.7285, -121.8375),
        },
        RouteSpec {
            id: "san_diego_inland".to_string(),
            name: "San Diego to Inland Empire".to_string(),
            start: (32.7157, -117.1611),
            end: (34.1083, -117.2898),
        },
    ]
}

fn coord_label(point: (f64, f64)) -> String {
    let lat_hemisphere = if point.0 < 0.0 { 'S' } else { 'N' };
    let lon_hemisphere = if point.1 < 0.0 { 'W' } else { 'E' };
    format!(
        "{:.2}{} {:.2}{}",
        point.0.abs(),
        lat_hemisphere,
        point.1.abs(),
        lon_hemisphere
    )
}

fn save_rgba_images(
    png_path: &Path,
    webp_path: &Path,
    rendered: &rustwx_cross_section::RenderedCrossSection,
) -> Result<()> {
    let image = RgbaImage::from_raw(
        rendered.width(),
        rendered.height(),
        rendered.rgba().to_vec(),
    )
    .ok_or_else(|| anyhow!("cross-section renderer returned invalid RGBA buffer length"))?;
    image.save_with_format(png_path, ImageFormat::Png)?;
    DynamicImage::ImageRgba8(image).save_with_format(webp_path, ImageFormat::WebP)?;
    Ok(())
}
