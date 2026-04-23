use std::fs;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use image::RgbaImage;
use rustwx_core::{ModelId, SourceId};
use rustwx_cross_section::{
    CrossSectionPalette, CrossSectionProduct, CrossSectionRenderRequest, CrossSectionRequest,
    CrossSectionStyle, GeoBounds, GeoPoint, Insets, RenderedCrossSection,
    RepresentativeRouteStrategy, SamplingStrategy, SectionMetadata, SectionPath, WindOverlayBundle,
    WindOverlayStyle, render_scalar_section, representative_route_for_bounds,
    representative_route_for_cluster,
};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::cross_section::{
    PressureCrossSectionArtifact, PressureCrossSectionFacts, build_pressure_cross_section_profiled,
    summarize_pressure_cross_section_artifact,
};
use rustwx_products::gridded::{LoadedModelTimestep, load_model_timestep_from_parts};
use rustwx_products::shared_context::DomainSpec;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct PressureCrossSectionRequest {
    pub model: ModelId,
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
    pub surface_product_override: Option<String>,
    pub pressure_product_override: Option<String>,
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
    pub facts: PressureCrossSectionFacts,
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
    pub facts: PressureCrossSectionFacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProofProductArg {
    Temperature,
    RelativeHumidity,
    SpecificHumidity,
    ThetaE,
    WindSpeed,
    WetBulb,
    VaporPressureDeficit,
    DewpointDepression,
    MoistureTransport,
    FireWeather,
}

impl ProofProductArg {
    pub const fn product(self) -> CrossSectionProduct {
        match self {
            Self::Temperature => CrossSectionProduct::Temperature,
            Self::RelativeHumidity => CrossSectionProduct::RelativeHumidity,
            Self::SpecificHumidity => CrossSectionProduct::SpecificHumidity,
            Self::ThetaE => CrossSectionProduct::ThetaE,
            Self::WindSpeed => CrossSectionProduct::WindSpeed,
            Self::WetBulb => CrossSectionProduct::WetBulb,
            Self::VaporPressureDeficit => CrossSectionProduct::VaporPressureDeficit,
            Self::DewpointDepression => CrossSectionProduct::DewpointDepression,
            Self::MoistureTransport => CrossSectionProduct::MoistureTransport,
            Self::FireWeather => CrossSectionProduct::FireWeather,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoutePresetArg {
    AmarilloChicago,
    KansasCityChicago,
    SanFranciscoTahoe,
    SacramentoReno,
    LosAngelesMojave,
    SanDiegoImperial,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoutePreset {
    slug: String,
    label: String,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
}

impl RoutePreset {
    pub fn new<S1: Into<String>, S2: Into<String>>(
        slug: S1,
        label: S2,
        start: GeoPoint,
        end: GeoPoint,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        SectionPath::endpoints(start, end)?;
        Ok(Self {
            slug: slug.into(),
            label: label.into(),
            start_lat: start.lat_deg,
            start_lon: start.lon_deg,
            end_lat: end.lat_deg,
            end_lon: end.lon_deg,
        })
    }

    pub fn slug(&self) -> &str {
        &self.slug
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn start_lat(&self) -> f64 {
        self.start_lat
    }

    pub fn start_lon(&self) -> f64 {
        self.start_lon
    }

    pub fn end_lat(&self) -> f64 {
        self.end_lat
    }

    pub fn end_lon(&self) -> f64 {
        self.end_lon
    }

    pub fn start_point(&self) -> Result<GeoPoint, rustwx_cross_section::CrossSectionError> {
        GeoPoint::new(self.start_lat, self.start_lon)
    }

    pub fn end_point(&self) -> Result<GeoPoint, rustwx_cross_section::CrossSectionError> {
        GeoPoint::new(self.end_lat, self.end_lon)
    }

    pub fn path(&self) -> Result<SectionPath, rustwx_cross_section::CrossSectionError> {
        SectionPath::endpoints(self.start_point()?, self.end_point()?)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CameraAreaRouteSpec {
    pub slug: String,
    pub label: String,
    pub bounds: GeoBounds,
    pub strategy: RepresentativeRouteStrategy,
}

impl CameraAreaRouteSpec {
    pub fn new<S1: Into<String>, S2: Into<String>>(
        slug: S1,
        label: S2,
        bounds: (f64, f64, f64, f64),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            slug: slug.into(),
            label: label.into(),
            bounds: GeoBounds::new(bounds.0, bounds.1, bounds.2, bounds.3)?,
            strategy: RepresentativeRouteStrategy::LongestAxisMidline,
        })
    }

    pub fn from_domain<S: Into<String>>(
        domain: &DomainSpec,
        label: S,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(domain.slug.clone(), label, domain.bounds)
    }

    pub fn with_strategy(mut self, strategy: RepresentativeRouteStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn derive_route(&self) -> Result<RoutePreset, Box<dyn std::error::Error>> {
        let route = representative_route_for_bounds(self.bounds, self.strategy)?;
        RoutePreset::new(
            format!("{}_representative", self.slug),
            format!("{} Representative Route", self.label),
            route.start,
            route.end,
        )
    }
}

pub fn derive_camera_cluster_route<S1: Into<String>, S2: Into<String>>(
    slug: S1,
    label: S2,
    points: &[GeoPoint],
    strategy: RepresentativeRouteStrategy,
) -> Result<RoutePreset, Box<dyn std::error::Error>> {
    let slug = slug.into();
    let label = label.into();
    let route = representative_route_for_cluster(points, strategy)?;
    RoutePreset::new(
        format!("{}_representative", slug),
        format!("{} Representative Route", label),
        route.start,
        route.end,
    )
}

impl RoutePresetArg {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::AmarilloChicago => "amarillo_chicago",
            Self::KansasCityChicago => "kansas_city_chicago",
            Self::SanFranciscoTahoe => "san_francisco_tahoe",
            Self::SacramentoReno => "sacramento_reno",
            Self::LosAngelesMojave => "los_angeles_mojave",
            Self::SanDiegoImperial => "san_diego_imperial",
        }
    }

    pub fn preset(self) -> RoutePreset {
        match self {
            Self::AmarilloChicago => RoutePreset::new(
                self.slug(),
                "Amarillo to Chicago",
                GeoPoint::new(35.2220, -101.8313).expect("fixed route should be valid"),
                GeoPoint::new(41.8781, -87.6298).expect("fixed route should be valid"),
            )
            .expect("fixed route should be valid"),
            Self::KansasCityChicago => RoutePreset::new(
                self.slug(),
                "Kansas City to Chicago",
                GeoPoint::new(39.0997, -94.5786).expect("fixed route should be valid"),
                GeoPoint::new(41.8781, -87.6298).expect("fixed route should be valid"),
            )
            .expect("fixed route should be valid"),
            Self::SanFranciscoTahoe => RoutePreset::new(
                self.slug(),
                "San Francisco to Tahoe",
                GeoPoint::new(37.8044, -122.2712).expect("fixed route should be valid"),
                GeoPoint::new(38.9399, -119.9772).expect("fixed route should be valid"),
            )
            .expect("fixed route should be valid"),
            Self::SacramentoReno => RoutePreset::new(
                self.slug(),
                "Sacramento to Reno",
                GeoPoint::new(38.5816, -121.4944).expect("fixed route should be valid"),
                GeoPoint::new(39.5296, -119.8138).expect("fixed route should be valid"),
            )
            .expect("fixed route should be valid"),
            Self::LosAngelesMojave => RoutePreset::new(
                self.slug(),
                "Los Angeles to Mojave",
                GeoPoint::new(34.0522, -118.2437).expect("fixed route should be valid"),
                GeoPoint::new(35.0525, -118.1739).expect("fixed route should be valid"),
            )
            .expect("fixed route should be valid"),
            Self::SanDiegoImperial => RoutePreset::new(
                self.slug(),
                "San Diego to Imperial",
                GeoPoint::new(32.7157, -117.1611).expect("fixed route should be valid"),
                GeoPoint::new(32.7920, -115.5631).expect("fixed route should be valid"),
            )
            .expect("fixed route should be valid"),
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
    let routes = [default_cross_section_proof_route()];
    default_native_cross_section_requests_for_routes(
        date,
        cycle,
        forecast_hour,
        source,
        &routes,
        out_dir,
        cache_dir,
        use_cache,
    )
}

pub fn default_native_cross_section_requests_for_routes(
    date: &str,
    cycle: u8,
    forecast_hour: u16,
    source: SourceId,
    routes: &[RoutePreset],
    out_dir: &Path,
    cache_dir: Option<PathBuf>,
    use_cache: bool,
) -> Vec<PressureCrossSectionRequest> {
    let product_specs = [
        (
            CrossSectionProduct::Temperature,
            Some(CrossSectionPalette::TemperatureWhiteZero),
        ),
        (CrossSectionProduct::RelativeHumidity, None),
        (CrossSectionProduct::ThetaE, None),
        (CrossSectionProduct::WindSpeed, None),
    ];
    let mut requests = Vec::with_capacity(routes.len() * product_specs.len());
    for route in routes {
        for &(product, palette) in &product_specs {
            requests.push(PressureCrossSectionRequest {
                model: ModelId::Hrrr,
                date: date.to_string(),
                cycle,
                forecast_hour,
                source,
                route: route.clone(),
                product,
                palette,
                sample_count: 181,
                out_dir: out_dir.to_path_buf(),
                cache_dir: cache_dir.clone(),
                use_cache,
                show_wind_overlay: true,
                surface_product_override: None,
                pressure_product_override: None,
            });
        }
    }
    requests
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
            route.slug = "custom".to_string();
            route.label = "Custom Route".to_string();
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

pub fn custom_route_preset<S1: Into<String>, S2: Into<String>>(
    slug: S1,
    label: S2,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
) -> Result<RoutePreset, Box<dyn std::error::Error>> {
    if !start_lat.is_finite()
        || !start_lon.is_finite()
        || !end_lat.is_finite()
        || !end_lon.is_finite()
    {
        return Err("custom cross-section route requires finite coordinates".into());
    }
    RoutePreset::new(
        slug,
        label,
        GeoPoint::new(start_lat, start_lon)?,
        GeoPoint::new(end_lat, end_lon)?,
    )
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
        request.model,
        &request.date,
        Some(request.cycle),
        request.forecast_hour,
        request.source,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
        &cache_root,
        request.use_cache,
    )?;

    run_pressure_cross_section_with_loaded(request, &loaded)
}

pub fn run_pressure_cross_section_with_loaded(
    request: &PressureCrossSectionRequest,
    loaded: &LoadedModelTimestep,
) -> Result<CrossSectionRunOutput, Box<dyn std::error::Error>> {
    let scene = prepare_pressure_cross_section_scene(request, &loaded)?;
    let rendered = render_scalar_section(&scene.artifact.section, &scene.render_request)?;

    write_cross_section_outputs(request, &scene, &rendered)
}

fn write_cross_section_outputs(
    request: &PressureCrossSectionRequest,
    scene: &PreparedPressureCrossSectionScene,
    rendered: &RenderedCrossSection,
) -> Result<CrossSectionRunOutput, Box<dyn std::error::Error>> {
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}_cross_section.png",
        request.model.as_str(),
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
        "rustwx_{}_{}_{}z_f{:03}_{}_{}_cross_section.json",
        request.model.as_str(),
        request.date,
        request.cycle,
        request.forecast_hour,
        scene.route_slug,
        request.product.slug()
    ));
    let summary = CrossSectionSummary {
        model: request.model.as_str(),
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
        start_lat: request.route.start_lat(),
        start_lon: request.route.start_lon(),
        end_lat: request.route.end_lat(),
        end_lon: request.route.end_lon(),
        facts: scene.facts.clone(),
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
    let route = request.route.clone();
    let path = route.path()?;
    let route_distance_km = path.total_distance_km();
    let layout = CrossSectionRequest::new(path)
        .with_sampling(SamplingStrategy::Count(request.sample_count.max(2)))
        .with_field_key(request.product.slug())
        .with_metadata(
            SectionMetadata::new()
                .titled(format!(
                    "{} {} Cross Section",
                    request.model.as_str().to_ascii_uppercase(),
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
                    format_coord_label(route.start_lat(), route.start_lon()),
                )
                .with_attribute(
                    "end_label",
                    format_coord_label(route.end_lat(), route.end_lon()),
                )
                .with_attribute(
                    "route_label",
                    format!(
                        "{}  {:.0} KM",
                        route.label().to_uppercase(),
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
    let facts = summarize_pressure_cross_section_artifact(&layout, &artifact);
    let artifact_build_ms = artifact_build_start.elapsed().as_millis();
    let render_request_build_start = std::time::Instant::now();
    let style = proof_style_for_request(request, &artifact);
    let render_request = render_request_for_request(request, &artifact, &style);
    let render_request_build_ms = render_request_build_start.elapsed().as_millis();
    Ok(PreparedPressureCrossSectionScene {
        route_slug: route.slug().to_string(),
        route_label: route.label().to_string(),
        route_distance_km,
        palette_slug: style.palette().slug().to_string(),
        facts,
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
        CrossSectionProduct::SpecificHumidity => {
            style = style.with_value_ticks(vec![0.0, 2.0, 4.0, 8.0, 12.0, 16.0]);
        }
        CrossSectionProduct::ThetaE => {
            style = style
                .with_value_range(284.0, 356.0)
                .with_value_ticks(vec![284.0, 296.0, 308.0, 320.0, 332.0, 344.0, 356.0]);
        }
        CrossSectionProduct::WindSpeed => {
            style = style.with_value_ticks(vec![0.0, 20.0, 40.0, 60.0, 80.0, 100.0]);
        }
        CrossSectionProduct::WetBulb => {
            style = style.with_value_ticks(vec![-40.0, -20.0, -10.0, 0.0, 10.0, 20.0, 30.0]);
        }
        CrossSectionProduct::VaporPressureDeficit => {
            style = style.with_value_ticks(vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);
        }
        CrossSectionProduct::DewpointDepression => {
            style = style.with_value_ticks(vec![0.0, 5.0, 10.0, 20.0, 30.0, 40.0]);
        }
        CrossSectionProduct::MoistureTransport => {
            style = style.with_value_ticks(vec![0.0, 25.0, 50.0, 100.0, 150.0, 200.0]);
        }
        CrossSectionProduct::FireWeather => {
            style = style.with_value_ticks(vec![0.0, 15.0, 25.0, 40.0, 60.0, 80.0, 100.0]);
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
    fn california_route_presets_have_stable_slugs() {
        assert_eq!(
            RoutePresetArg::SanFranciscoTahoe.preset().slug,
            "san_francisco_tahoe"
        );
        assert_eq!(
            RoutePresetArg::SacramentoReno.preset().slug,
            "sacramento_reno"
        );
        assert_eq!(
            RoutePresetArg::LosAngelesMojave.preset().slug,
            "los_angeles_mojave"
        );
        assert_eq!(
            RoutePresetArg::SanDiegoImperial.preset().slug,
            "san_diego_imperial"
        );
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
    fn camera_area_route_derives_long_axis_midline_from_domain_bounds() {
        let area = CameraAreaRouteSpec::from_domain(
            &DomainSpec::new("southern_plains", (-109.0, -90.0, 25.0, 40.5)),
            "Southern Plains",
        )
        .unwrap();
        let route = area.derive_route().unwrap();

        assert_eq!(route.slug(), "southern_plains_representative");
        assert_eq!(route.label(), "Southern Plains Representative Route");
        assert!((route.start_lat() - route.end_lat()).abs() < 1.0e-6);
        assert!(route.start_lon() < route.end_lon());
    }

    #[test]
    fn camera_cluster_route_uses_farthest_pair_strategy() {
        let points = [
            GeoPoint::new(37.8044, -122.2712).unwrap(),
            GeoPoint::new(38.5816, -121.4944).unwrap(),
            GeoPoint::new(39.5296, -119.8138).unwrap(),
        ];
        let route = derive_camera_cluster_route(
            "sierra_camera_cluster",
            "Sierra Camera Cluster",
            &points,
            RepresentativeRouteStrategy::FarthestPair,
        )
        .unwrap();

        assert_eq!(route.slug(), "sierra_camera_cluster_representative");
        assert_eq!(route.label(), "Sierra Camera Cluster Representative Route");
        assert!((route.start_lat() - points[0].lat_deg).abs() < 1.0e-6);
        assert!((route.end_lon() - points[2].lon_deg).abs() < 1.0e-6);
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
    fn native_proof_request_helper_expands_each_route_across_default_products() {
        let routes = vec![
            CameraAreaRouteSpec::from_domain(
                &DomainSpec::new("conus", (-127.0, -66.0, 23.0, 51.5)),
                "CONUS",
            )
            .unwrap()
            .derive_route()
            .unwrap(),
            CameraAreaRouteSpec::from_domain(
                &DomainSpec::new("midwest", (-104.0, -74.0, 28.0, 49.0)),
                "Midwest",
            )
            .unwrap()
            .derive_route()
            .unwrap(),
        ];
        let requests = default_native_cross_section_requests_for_routes(
            "20260414",
            23,
            0,
            SourceId::Nomads,
            &routes,
            Path::new("proof"),
            Some(PathBuf::from("cache")),
            true,
        );

        assert_eq!(requests.len(), 8);
        assert_eq!(requests[0].route.slug(), "conus_representative");
        assert_eq!(requests[4].route.slug(), "midwest_representative");
        assert_eq!(
            requests
                .iter()
                .map(|request| request.product.slug())
                .collect::<Vec<_>>(),
            vec![
                "temperature",
                "rh",
                "theta_e",
                "wind_speed",
                "temperature",
                "rh",
                "theta_e",
                "wind_speed",
            ]
        );
    }

    #[test]
    fn proof_product_arg_maps_to_cross_section_product() {
        assert_eq!(
            ProofProductArg::RelativeHumidity.product(),
            CrossSectionProduct::RelativeHumidity
        );
        assert_eq!(
            ProofProductArg::SpecificHumidity.product(),
            CrossSectionProduct::SpecificHumidity
        );
        assert_eq!(
            ProofProductArg::ThetaE.product(),
            CrossSectionProduct::ThetaE
        );
        assert_eq!(
            ProofProductArg::WetBulb.product(),
            CrossSectionProduct::WetBulb
        );
        assert_eq!(
            ProofProductArg::VaporPressureDeficit.product(),
            CrossSectionProduct::VaporPressureDeficit
        );
        assert_eq!(
            ProofProductArg::DewpointDepression.product(),
            CrossSectionProduct::DewpointDepression
        );
        assert_eq!(
            ProofProductArg::MoistureTransport.product(),
            CrossSectionProduct::MoistureTransport
        );
        assert_eq!(
            ProofProductArg::FireWeather.product(),
            CrossSectionProduct::FireWeather
        );
    }
}
