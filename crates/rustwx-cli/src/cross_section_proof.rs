use std::fs;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use image::RgbaImage;
use rustwx_core::{ModelId, SourceId};
use rustwx_cross_section::{
    CrossSectionPalette, CrossSectionProduct, CrossSectionRenderRequest, CrossSectionRequest,
    CrossSectionStyle, GeoPoint, Insets, RenderedCrossSection, SamplingStrategy, SectionMetadata,
    SectionPath, WindOverlayBundle, WindOverlayStyle, render_scalar_section,
};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::cross_section::{
    PressureCrossSectionArtifact, build_pressure_cross_section_profiled,
};
use rustwx_products::gridded::{LoadedModelTimestep, load_model_timestep_from_parts};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct PressureCrossSectionRequest {
    pub date: String,
    pub cycle: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub route: RoutePreset,
    pub product: CrossSectionProduct,
    pub palette: Option<CrossSectionPalette>,
    pub sample_count: usize,
    pub out_dir: PathBuf,
    pub cache_dir: Option<PathBuf>,
    pub use_cache: bool,
    pub show_wind_overlay: bool,
}

#[derive(Debug, Clone)]
pub struct CrossSectionRunOutput {
    pub output_path: PathBuf,
    pub summary_path: PathBuf,
    pub summary: CrossSectionSummary,
}

#[derive(Debug, Clone)]
pub struct PreparedPressureCrossSectionScene {
    pub route_slug: String,
    pub route_label: String,
    pub route_distance_km: f64,
    pub palette_slug: String,
    pub artifact: PressureCrossSectionArtifact,
    pub render_request: CrossSectionRenderRequest,
    pub timing: PreparedPressureCrossSectionTiming,
}

#[derive(Debug, Clone, Copy)]
pub struct PreparedPressureCrossSectionTiming {
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
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct CrossSectionSummary {
    pub model: &'static str,
    pub route_slug: String,
    pub route_label: String,
    pub product_slug: String,
    pub product_label: String,
    pub palette_slug: String,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: String,
    pub output_path: PathBuf,
    pub summary_path: PathBuf,
    pub route_distance_km: f64,
    pub sample_count: usize,
    pub pressure_levels: usize,
    pub start_lat: f64,
    pub start_lon: f64,
    pub end_lat: f64,
    pub end_lon: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProofProductArg {
    Temperature,
    RelativeHumidity,
    ThetaE,
    WindSpeed,
}

impl ProofProductArg {
    pub const fn product(self) -> CrossSectionProduct {
        match self {
            Self::Temperature => CrossSectionProduct::Temperature,
            Self::RelativeHumidity => CrossSectionProduct::RelativeHumidity,
            Self::ThetaE => CrossSectionProduct::ThetaE,
            Self::WindSpeed => CrossSectionProduct::WindSpeed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutePresetArg {
    AmarilloChicago,
    KansasCityChicago,
}

#[derive(Debug, Clone, Copy)]
pub struct RoutePreset {
    slug: &'static str,
    label: &'static str,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
}

impl RoutePresetArg {
    pub fn preset(self) -> RoutePreset {
        match self {
            Self::AmarilloChicago => RoutePreset {
                slug: "amarillo_chicago",
                label: "Amarillo to Chicago",
                start_lat: 35.2220,
                start_lon: -101.8313,
                end_lat: 41.8781,
                end_lon: -87.6298,
            },
            Self::KansasCityChicago => RoutePreset {
                slug: "kansas_city_chicago",
                label: "Kansas City to Chicago",
                start_lat: 39.0997,
                start_lon: -94.5786,
                end_lat: 41.8781,
                end_lon: -87.6298,
            },
        }
    }
}

pub fn default_cross_section_proof_route() -> RoutePreset {
    RoutePresetArg::AmarilloChicago.preset()
}

pub fn default_temperature_proof_route() -> RoutePreset {
    default_cross_section_proof_route()
}

pub fn default_native_cross_section_requests(
    date: &str,
    cycle: u8,
    forecast_hour: u16,
    source: SourceId,
    out_dir: &Path,
    cache_dir: Option<PathBuf>,
    use_cache: bool,
) -> Vec<PressureCrossSectionRequest> {
    let route = default_cross_section_proof_route();
    [
        (
            CrossSectionProduct::Temperature,
            Some(CrossSectionPalette::TemperatureWhiteZero),
        ),
        (CrossSectionProduct::RelativeHumidity, None),
        (CrossSectionProduct::ThetaE, None),
        (CrossSectionProduct::WindSpeed, None),
    ]
    .into_iter()
    .map(|(product, palette)| PressureCrossSectionRequest {
        date: date.to_string(),
        cycle,
        forecast_hour,
        source,
        route,
        product,
        palette,
        sample_count: 181,
        out_dir: out_dir.to_path_buf(),
        cache_dir: cache_dir.clone(),
        use_cache,
        show_wind_overlay: true,
    })
    .collect()
}

pub fn resolve_route(
    route: RoutePresetArg,
    start_lat: Option<f64>,
    start_lon: Option<f64>,
    end_lat: Option<f64>,
    end_lon: Option<f64>,
) -> Result<RoutePreset, Box<dyn std::error::Error>> {
    let mut route = route.preset();
    match (start_lat, start_lon, end_lat, end_lon) {
        (None, None, None, None) => Ok(route),
        (Some(start_lat), Some(start_lon), Some(end_lat), Some(end_lon)) => {
            route.slug = "custom";
            route.label = "Custom Route";
            route.start_lat = start_lat;
            route.start_lon = start_lon;
            route.end_lat = end_lat;
            route.end_lon = end_lon;
            Ok(route)
        }
        _ => Err(
            "custom coordinates require all of --start-lat --start-lon --end-lat --end-lon".into(),
        ),
    }
}

pub fn run_pressure_cross_section(
    request: &PressureCrossSectionRequest,
) -> Result<CrossSectionRunOutput, Box<dyn std::error::Error>> {
    let cache_root = request
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&request.out_dir));
    if request.use_cache {
        ensure_dir(&cache_root)?;
    }

    let loaded = load_model_timestep_from_parts(
        ModelId::Hrrr,
        &request.date,
        Some(request.cycle),
        request.forecast_hour,
        request.source,
        None,
        None,
        &cache_root,
        request.use_cache,
    )?;

    let scene = prepare_pressure_cross_section_scene(request, &loaded)?;
    let rendered = render_scalar_section(&scene.artifact.section, &scene.render_request)?;

    let output_path = request.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_{}_cross_section.png",
        request.date,
        request.cycle,
        request.forecast_hour,
        scene.route_slug,
        request.product.slug()
    ));
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    save_rgba_image(&output_path, &rendered)?;

    let summary_path = request.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_{}_cross_section.json",
        request.date,
        request.cycle,
        request.forecast_hour,
        scene.route_slug,
        request.product.slug()
    ));
    let summary = CrossSectionSummary {
        model: "hrrr",
        route_slug: scene.route_slug.clone(),
        route_label: scene.route_label.clone(),
        product_slug: request.product.slug().to_string(),
        product_label: request.product.display_name().to_string(),
        palette_slug: scene.palette_slug.clone(),
        date_yyyymmdd: request.date.clone(),
        cycle_utc: request.cycle,
        forecast_hour: request.forecast_hour,
        source: request.source.as_str().to_string(),
        output_path: relative_path(&request.out_dir, &output_path),
        summary_path: relative_path(&request.out_dir, &summary_path),
        route_distance_km: scene.route_distance_km,
        sample_count: scene.artifact.section.n_points(),
        pressure_levels: scene.artifact.section.n_levels(),
        start_lat: request.route.start_lat,
        start_lon: request.route.start_lon,
        end_lat: request.route.end_lat,
        end_lon: request.route.end_lon,
    };
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;

    Ok(CrossSectionRunOutput {
        output_path,
        summary_path,
        summary,
    })
}

pub fn prepare_pressure_cross_section_scene(
    request: &PressureCrossSectionRequest,
    loaded: &LoadedModelTimestep,
) -> Result<PreparedPressureCrossSectionScene, Box<dyn std::error::Error>> {
    let total_start = std::time::Instant::now();
    let path_layout_start = std::time::Instant::now();
    let route = request.route;
    let path = SectionPath::endpoints(
        GeoPoint::new(route.start_lat, route.start_lon)?,
        GeoPoint::new(route.end_lat, route.end_lon)?,
    )?;
    let route_distance_km = path.total_distance_km();
    let layout = CrossSectionRequest::new(path)
        .with_sampling(SamplingStrategy::Count(request.sample_count.max(2)))
        .with_field_key(request.product.slug())
        .with_metadata(
            SectionMetadata::new()
                .titled(format!(
                    "HRRR {} Cross Section",
                    request.product.display_name()
                ))
                .field(request.product.slug(), request.product.units())
                .sourced_from(request.source.as_str())
                .valid_at(format!(
                    "{} {:02}Z F{:03}",
                    request.date, request.cycle, request.forecast_hour
                ))
                .with_attribute(
                    "start_label",
                    format_coord_label(route.start_lat, route.start_lon),
                )
                .with_attribute(
                    "end_label",
                    format_coord_label(route.end_lat, route.end_lon),
                )
                .with_attribute(
                    "route_label",
                    format!(
                        "{}  {:.0} KM",
                        route.label.to_uppercase(),
                        route_distance_km
                    ),
                )
                .with_attribute("product_key", request.product.slug())
                .with_attribute("render_style", request.product.style_key()),
        )
        .build_layout()?;
    let path_layout_ms = path_layout_start.elapsed().as_millis();

    let artifact_build_start = std::time::Instant::now();
    let profiled = build_pressure_cross_section_profiled(loaded, &layout, request.product)?;
    let mut artifact = profiled.artifact;
    artifact.wind_overlay = tuned_wind_overlay(&artifact.wind_overlay);
    let artifact_build_ms = artifact_build_start.elapsed().as_millis();
    let render_request_build_start = std::time::Instant::now();
    let style = proof_style_for_request(request, &artifact);
    let render_request = render_request_for_request(request, &artifact, &style);
    let render_request_build_ms = render_request_build_start.elapsed().as_millis();
    Ok(PreparedPressureCrossSectionScene {
        route_slug: route.slug.to_string(),
        route_label: route.label.to_string(),
        route_distance_km,
        palette_slug: style.palette().slug().to_string(),
        artifact,
        render_request,
        timing: PreparedPressureCrossSectionTiming {
            path_layout_ms,
            artifact_build_ms,
            artifact_stencil_build_ms: profiled.timing.stencil_build_ms,
            artifact_terrain_profile_ms: profiled.timing.terrain_profile_ms,
            artifact_pressure_sampling_ms: profiled.timing.pressure_sampling_ms,
            artifact_product_compute_ms: profiled.timing.product_compute_ms,
            artifact_metadata_ms: profiled.timing.metadata_ms,
            artifact_section_assembly_ms: profiled.timing.section_assembly_ms,
            artifact_wind_overlay_ms: profiled.timing.wind_overlay_ms,
            render_request_build_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    })
}

fn proof_style_for_request(
    request: &PressureCrossSectionRequest,
    artifact: &PressureCrossSectionArtifact,
) -> CrossSectionStyle {
    let mut style = artifact.style.clone();
    match request.product {
        CrossSectionProduct::Temperature => {
            style = style
                .with_palette(CrossSectionPalette::TemperatureWhiteZero)
                .with_value_range(-36.0, 30.0)
                .with_value_ticks(vec![-35.0, -30.0, -20.0, -10.0, 0.0, 10.0, 20.0, 30.0]);
        }
        CrossSectionProduct::ThetaE => {
            style = style
                .with_value_range(284.0, 356.0)
                .with_value_ticks(vec![284.0, 296.0, 308.0, 320.0, 332.0, 344.0, 356.0]);
        }
        CrossSectionProduct::WindSpeed => {
            style = style.with_value_ticks(vec![0.0, 20.0, 40.0, 60.0, 80.0, 100.0]);
        }
        _ => {}
    }
    if let Some(palette) = request.palette {
        style = style.with_palette(palette);
    }
    style
}

fn render_request_for_request(
    request: &PressureCrossSectionRequest,
    artifact: &PressureCrossSectionArtifact,
    style: &CrossSectionStyle,
) -> CrossSectionRenderRequest {
    let mut render_request = style
        .to_render_request()
        .with_dimensions(1400, 820)
        .with_margins(Insets {
            left: 90,
            right: 126,
            top: 78,
            bottom: 82,
        });
    if request.show_wind_overlay {
        render_request = render_request.with_wind_overlay(artifact.wind_overlay.clone());
    }
    render_request
}

fn tuned_wind_overlay(overlay: &WindOverlayBundle) -> WindOverlayBundle {
    WindOverlayBundle::new(
        overlay.grid.clone(),
        WindOverlayStyle {
            stride_points: 10,
            stride_levels: 3,
            min_speed_ms: 8.0,
            max_speed_ms: 40.0,
            base_length_px: 8.0,
            max_length_px: 24.0,
            arrow_head_px: 4.5,
            cross_tick_px: 6.0,
            ..overlay.style
        },
    )
    .with_label(
        overlay
            .label
            .clone()
            .unwrap_or_else(|| "Section Relative Wind".to_string()),
    )
}

fn save_rgba_image(
    output_path: &Path,
    rendered: &RenderedCrossSection,
) -> Result<(), Box<dyn std::error::Error>> {
    let image = RgbaImage::from_raw(
        rendered.width(),
        rendered.height(),
        rendered.rgba().to_vec(),
    )
    .ok_or("cross-section renderer returned an invalid RGBA buffer length")?;
    image.save(output_path)?;
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| path.to_path_buf())
}

fn format_coord_label(lat_deg: f64, lon_deg: f64) -> String {
    let lat_hemisphere = if lat_deg < 0.0 { 'S' } else { 'N' };
    let lon_hemisphere = if lon_deg < 0.0 { 'W' } else { 'E' };
    format!(
        "{:.2}{} {:.2}{}",
        lat_deg.abs(),
        lat_hemisphere,
        lon_deg.abs(),
        lon_hemisphere
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_coord_label_uses_cardinal_suffixes() {
        assert_eq!(format_coord_label(39.0997, -94.5786), "39.10N 94.58W");
        assert_eq!(format_coord_label(-33.8688, 151.2093), "33.87S 151.21E");
    }

    #[test]
    fn default_route_prefers_amarillo_to_chicago() {
        let route = RoutePresetArg::AmarilloChicago.preset();
        assert_eq!(route.slug, "amarillo_chicago");
        assert!((route.start_lat - 35.2220).abs() < 1.0e-6);
        assert!((route.end_lon + 87.6298).abs() < 1.0e-6);
    }

    #[test]
    fn resolve_route_supports_custom_coordinates() {
        let route = resolve_route(
            RoutePresetArg::AmarilloChicago,
            Some(39.0997),
            Some(-94.5786),
            Some(41.8781),
            Some(-87.6298),
        )
        .unwrap();
        assert_eq!(route.slug, "custom");
        assert_eq!(route.label, "Custom Route");
    }

    #[test]
    fn native_proof_request_set_covers_multiple_products() {
        let requests = default_native_cross_section_requests(
            "20260414",
            23,
            0,
            SourceId::Nomads,
            Path::new("proof"),
            Some(PathBuf::from("cache")),
            true,
        );

        assert_eq!(
            requests
                .iter()
                .map(|request| request.product.slug())
                .collect::<Vec<_>>(),
            vec!["temperature", "rh", "theta_e", "wind_speed"]
        );
        assert_eq!(
            requests[0].palette,
            Some(CrossSectionPalette::TemperatureWhiteZero)
        );
        assert!(requests.iter().all(|request| request.show_wind_overlay));
    }

    #[test]
    fn proof_product_arg_maps_to_cross_section_product() {
        assert_eq!(
            ProofProductArg::RelativeHumidity.product(),
            CrossSectionProduct::RelativeHumidity
        );
        assert_eq!(
            ProofProductArg::ThetaE.product(),
            CrossSectionProduct::ThetaE
        );
    }
}
