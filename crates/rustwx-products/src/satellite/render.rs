use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::PathBuf;

use rustwx_render::{
    ChromeScale, Color, ColorScale, DiscreteColorScale, DomainFrame, ExtendMode, Field2D,
    GridShape, LatLonGrid, LegendControls, LegendMode, LevelDensity, MapRenderRequest, ProductKey,
    ProductVisualMode, ProjectedMapBuildOptions, ProjectionSpec, RenderDensity,
    build_projected_map_with_options, map_frame_aspect_ratio_for_mode_with_domain_frame,
};

use super::abi::read_goes_abi_field;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoesAbiLayerStyle {
    VisibleRed,
    CleanIr,
    ShortwaveIr,
    WaterVapor,
}

impl GoesAbiLayerStyle {
    pub fn product_slug(self) -> &'static str {
        match self {
            Self::VisibleRed => "goes_visible_red",
            Self::CleanIr => "goes_clean_ir",
            Self::ShortwaveIr => "goes_shortwave_ir",
            Self::WaterVapor => "goes_water_vapor",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::VisibleRed => "GOES Visible Red",
            Self::CleanIr => "GOES Clean IR",
            Self::ShortwaveIr => "GOES Shortwave IR",
            Self::WaterVapor => "GOES Water Vapor",
        }
    }

    pub fn scale(self) -> ColorScale {
        match self {
            Self::VisibleRed => grayscale_visible_scale(0.0, 1.0),
            Self::CleanIr => clean_window_ir_scale(),
            Self::ShortwaveIr => shortwave_window_ir_scale(),
            Self::WaterVapor => upper_water_vapor_scale(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesAbiMapRequest {
    pub abi_path: PathBuf,
    #[serde(default = "default_variable_name")]
    pub variable_name: String,
    pub layer_style: GoesAbiLayerStyle,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    #[serde(default = "default_width")]
    pub width: u32,
    #[serde(default = "default_height")]
    pub height: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesAbiBandMapRequest {
    pub abi_path: PathBuf,
    #[serde(default = "default_variable_name")]
    pub variable_name: String,
    pub channel: u8,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    #[serde(default = "default_width")]
    pub width: u32,
    #[serde(default = "default_height")]
    pub height: u32,
}

pub fn build_goes_abi_map_render_request(
    request: &GoesAbiMapRequest,
) -> Result<MapRenderRequest, Box<dyn Error>> {
    build_goes_abi_scalar_render_request(ScalarRenderSpec {
        abi_path: &request.abi_path,
        variable_name: &request.variable_name,
        product_slug: request.layer_style.product_slug().to_string(),
        title: request.layer_style.title().to_string(),
        scale: request.layer_style.scale(),
        domain_label: &request.domain_label,
        bounds: request.bounds,
        width: request.width,
        height: request.height,
    })
}

pub fn build_goes_abi_band_render_request(
    request: &GoesAbiBandMapRequest,
) -> Result<MapRenderRequest, Box<dyn Error>> {
    if !(1..=16).contains(&request.channel) {
        return Err(format!("GOES ABI channel out of range: {}", request.channel).into());
    }
    build_goes_abi_scalar_render_request(ScalarRenderSpec {
        abi_path: &request.abi_path,
        variable_name: &request.variable_name,
        product_slug: abi_band_product_slug(request.channel),
        title: abi_band_title(request.channel),
        scale: abi_band_scale(request.channel),
        domain_label: &request.domain_label,
        bounds: request.bounds,
        width: request.width,
        height: request.height,
    })
}

struct ScalarRenderSpec<'a> {
    abi_path: &'a PathBuf,
    variable_name: &'a str,
    product_slug: String,
    title: String,
    scale: ColorScale,
    domain_label: &'a str,
    bounds: (f64, f64, f64, f64),
    width: u32,
    height: u32,
}

fn build_goes_abi_scalar_render_request(
    spec: ScalarRenderSpec<'_>,
) -> Result<MapRenderRequest, Box<dyn Error>> {
    let field = read_goes_abi_field(spec.abi_path, spec.variable_name)?;
    let (lat, lon) = field.scene.lat_lon_mesh();
    let grid = LatLonGrid::new(
        GridShape::new(field.scene.fixed_grid.nx, field.scene.fixed_grid.ny)?,
        lat.clone(),
        lon.clone(),
    )?;
    let units = field.units.as_deref().unwrap_or("");
    let render_field = Field2D::new(
        ProductKey::named(spec.product_slug.as_str()),
        units,
        grid,
        field.values,
    )?;

    let target_ratio = map_frame_aspect_ratio_for_mode_with_domain_frame(
        ProductVisualMode::OverlayAnalysis,
        spec.width,
        spec.height,
        true,
        true,
        true,
    );
    let center_lon = bounds_center_lon(spec.bounds);
    let center_lat = (spec.bounds.2 + spec.bounds.3) * 0.5;
    let projection = ProjectionSpec::LambertConformal {
        standard_parallel_1_deg: 30.0,
        standard_parallel_2_deg: 60.0,
        central_meridian_deg: center_lon,
    };
    let mut map_options = ProjectedMapBuildOptions::from_bounds(spec.bounds, target_ratio)
        .with_projection(projection);
    map_options.domain.reference_latitude_deg = Some(center_lat);
    map_options.domain.pad_fraction = 0.02;
    let projected = build_projected_map_with_options(&lat, &lon, &map_options)?;

    let mut render_request = MapRenderRequest::new(render_field, spec.scale);
    render_request.width = spec.width;
    render_request.height = spec.height;
    render_request.visual_mode = ProductVisualMode::OverlayAnalysis;
    render_request.supersample_factor = 2;
    render_request.domain_frame = Some(DomainFrame::map_viewport_default());
    render_request.render_density = RenderDensity {
        fill: LevelDensity {
            multiplier: 4,
            min_source_level_count: 6,
        },
        palette_multiplier: 16,
    };
    render_request.legend = LegendControls {
        density: LevelDensity::default(),
        mode: LegendMode::SmoothRamp,
    };
    render_request.chrome_scale = ChromeScale::Auto {
        base_width: 1500,
        base_height: 1300,
        min: 1.0,
        max: 2.3,
    };
    render_request.title = Some(spec.title);
    render_request.subtitle_left = Some(format!(
        "{} | {}",
        spec.domain_label,
        field.scene.start_time_utc.format("%Y-%m-%d %H:%MZ")
    ));
    render_request.subtitle_right = Some(format!(
        "{} {}",
        field.scene.satellite.as_str(),
        field.scene.product
    ));
    render_request.apply_projected_map(&projected);
    Ok(render_request)
}

fn discrete_scale(levels: &[f64], colors: &[Color], extend: ExtendMode) -> ColorScale {
    ColorScale::Discrete(DiscreteColorScale {
        levels: levels.to_vec(),
        colors: colors.to_vec(),
        extend,
        mask_below: None,
    })
}

fn smooth_scale(
    min: f64,
    max: f64,
    step: f64,
    anchors: &[(f64, Color)],
    extend: ExtendMode,
) -> ColorScale {
    let mut levels = Vec::new();
    let mut value = min;
    while value < max {
        levels.push((value * 1000.0).round() / 1000.0);
        value += step;
    }
    levels.push(max);

    let colors = levels
        .windows(2)
        .map(|window| color_at((window[0] + window[1]) * 0.5, anchors))
        .collect::<Vec<_>>();

    discrete_scale(&levels, &colors, extend)
}

fn color_at(value: f64, anchors: &[(f64, Color)]) -> Color {
    if anchors.is_empty() {
        return Color::TRANSPARENT;
    }
    if value <= anchors[0].0 {
        return anchors[0].1;
    }

    for window in anchors.windows(2) {
        let (lo_value, lo_color) = window[0];
        let (hi_value, hi_color) = window[1];
        if value <= hi_value {
            let span = hi_value - lo_value;
            let t = if span > 0.0 {
                ((value - lo_value) / span).clamp(0.0, 1.0)
            } else {
                0.0
            };
            return lerp_color(lo_color, hi_color, t);
        }
    }

    anchors[anchors.len() - 1].1
}

fn lerp_color(a: Color, b: Color, t: f64) -> Color {
    let channel = |lo: u8, hi: u8| -> u8 {
        (lo as f64 + (hi as f64 - lo as f64) * t)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    Color::rgba(
        channel(a.r, b.r),
        channel(a.g, b.g),
        channel(a.b, b.b),
        channel(a.a, b.a),
    )
}

fn grayscale_visible_scale(min: f64, max: f64) -> ColorScale {
    smooth_scale(
        min,
        max,
        (max - min) / 80.0,
        &[
            (min, Color::rgba(3, 5, 8, 255)),
            (min + (max - min) * 0.18, Color::rgba(24, 29, 35, 255)),
            (min + (max - min) * 0.36, Color::rgba(61, 69, 78, 255)),
            (min + (max - min) * 0.56, Color::rgba(118, 128, 138, 255)),
            (min + (max - min) * 0.76, Color::rgba(190, 197, 204, 255)),
            (max, Color::rgba(252, 253, 254, 255)),
        ],
        ExtendMode::Both,
    )
}

fn shortwave_window_ir_scale() -> ColorScale {
    smooth_scale(
        240.0,
        430.0,
        2.0,
        &[
            (240.0, Color::rgba(5, 9, 18, 255)),
            (270.0, Color::rgba(28, 44, 78, 255)),
            (295.0, Color::rgba(58, 85, 103, 255)),
            (315.0, Color::rgba(98, 104, 88, 255)),
            (335.0, Color::rgba(156, 130, 58, 255)),
            (355.0, Color::rgba(219, 135, 34, 255)),
            (375.0, Color::rgba(224, 61, 31, 255)),
            (395.0, Color::rgba(196, 28, 72, 255)),
            (415.0, Color::rgba(255, 236, 180, 255)),
            (430.0, Color::rgba(255, 255, 255, 255)),
        ],
        ExtendMode::Both,
    )
}

fn upper_water_vapor_scale() -> ColorScale {
    water_vapor_scale(184.0, 268.0, true)
}

fn mid_water_vapor_scale() -> ColorScale {
    water_vapor_scale(188.0, 276.0, false)
}

fn lower_water_vapor_scale() -> ColorScale {
    smooth_scale(
        196.0,
        286.0,
        1.0,
        &[
            (196.0, Color::rgba(252, 252, 255, 255)),
            (208.0, Color::rgba(194, 224, 239, 255)),
            (222.0, Color::rgba(97, 158, 205, 255)),
            (236.0, Color::rgba(70, 91, 160, 255)),
            (250.0, Color::rgba(116, 79, 135, 255)),
            (264.0, Color::rgba(159, 117, 84, 255)),
            (276.0, Color::rgba(98, 85, 68, 255)),
            (286.0, Color::rgba(43, 42, 39, 255)),
        ],
        ExtendMode::Both,
    )
}

fn water_vapor_scale(min: f64, max: f64, upper_channel: bool) -> ColorScale {
    let blue = if upper_channel {
        Color::rgba(66, 129, 195, 255)
    } else {
        Color::rgba(78, 145, 198, 255)
    };
    smooth_scale(
        min,
        max,
        1.0,
        &[
            (min, Color::rgba(252, 253, 255, 255)),
            (min + 12.0, Color::rgba(200, 229, 242, 255)),
            (min + 26.0, blue),
            (min + 42.0, Color::rgba(68, 80, 151, 255)),
            (min + 56.0, Color::rgba(112, 78, 128, 255)),
            (min + 70.0, Color::rgba(151, 113, 82, 255)),
            (max - 8.0, Color::rgba(90, 80, 65, 255)),
            (max, Color::rgba(38, 38, 36, 255)),
        ],
        ExtendMode::Both,
    )
}

fn cloud_top_ir_scale() -> ColorScale {
    smooth_scale(
        188.0,
        325.0,
        1.5,
        &[
            (188.0, Color::rgba(255, 255, 255, 255)),
            (202.0, Color::rgba(207, 234, 252, 255)),
            (216.0, Color::rgba(122, 191, 231, 255)),
            (230.0, Color::rgba(76, 124, 190, 255)),
            (244.0, Color::rgba(84, 78, 139, 255)),
            (258.0, Color::rgba(103, 93, 111, 255)),
            (274.0, Color::rgba(95, 95, 95, 255)),
            (292.0, Color::rgba(61, 61, 61, 255)),
            (310.0, Color::rgba(31, 31, 31, 255)),
            (325.0, Color::rgba(8, 8, 8, 255)),
        ],
        ExtendMode::Both,
    )
}

fn ozone_ir_scale() -> ColorScale {
    smooth_scale(
        190.0,
        320.0,
        1.5,
        &[
            (190.0, Color::rgba(252, 251, 255, 255)),
            (205.0, Color::rgba(201, 223, 245, 255)),
            (220.0, Color::rgba(129, 169, 218, 255)),
            (235.0, Color::rgba(94, 104, 174, 255)),
            (250.0, Color::rgba(119, 82, 139, 255)),
            (265.0, Color::rgba(153, 112, 91, 255)),
            (282.0, Color::rgba(107, 100, 91, 255)),
            (300.0, Color::rgba(59, 59, 59, 255)),
            (320.0, Color::rgba(12, 12, 12, 255)),
        ],
        ExtendMode::Both,
    )
}

fn clean_window_ir_scale() -> ColorScale {
    smooth_scale(
        188.0,
        328.0,
        1.5,
        &[
            (188.0, Color::rgba(255, 255, 255, 255)),
            (202.0, Color::rgba(218, 239, 254, 255)),
            (216.0, Color::rgba(143, 204, 235, 255)),
            (230.0, Color::rgba(83, 146, 202, 255)),
            (244.0, Color::rgba(67, 91, 154, 255)),
            (258.0, Color::rgba(87, 76, 122, 255)),
            (272.0, Color::rgba(99, 95, 102, 255)),
            (288.0, Color::rgba(72, 72, 72, 255)),
            (306.0, Color::rgba(36, 36, 36, 255)),
            (328.0, Color::rgba(4, 4, 4, 255)),
        ],
        ExtendMode::Both,
    )
}

fn longwave_ir_scale() -> ColorScale {
    smooth_scale(
        188.0,
        330.0,
        1.5,
        &[
            (188.0, Color::rgba(255, 255, 255, 255)),
            (204.0, Color::rgba(225, 238, 250, 255)),
            (220.0, Color::rgba(157, 196, 222, 255)),
            (236.0, Color::rgba(91, 137, 185, 255)),
            (252.0, Color::rgba(86, 85, 132, 255)),
            (268.0, Color::rgba(102, 96, 101, 255)),
            (286.0, Color::rgba(76, 76, 76, 255)),
            (306.0, Color::rgba(37, 37, 37, 255)),
            (330.0, Color::rgba(5, 5, 5, 255)),
        ],
        ExtendMode::Both,
    )
}

fn dirty_window_ir_scale() -> ColorScale {
    smooth_scale(
        188.0,
        330.0,
        1.5,
        &[
            (188.0, Color::rgba(255, 255, 255, 255)),
            (204.0, Color::rgba(224, 237, 248, 255)),
            (220.0, Color::rgba(158, 193, 216, 255)),
            (236.0, Color::rgba(104, 137, 178, 255)),
            (252.0, Color::rgba(96, 86, 132, 255)),
            (268.0, Color::rgba(117, 97, 94, 255)),
            (286.0, Color::rgba(83, 78, 72, 255)),
            (306.0, Color::rgba(39, 39, 37, 255)),
            (330.0, Color::rgba(5, 5, 5, 255)),
        ],
        ExtendMode::Both,
    )
}

fn co2_longwave_ir_scale() -> ColorScale {
    smooth_scale(
        188.0,
        315.0,
        1.5,
        &[
            (188.0, Color::rgba(255, 255, 255, 255)),
            (202.0, Color::rgba(218, 232, 253, 255)),
            (216.0, Color::rgba(148, 188, 231, 255)),
            (230.0, Color::rgba(91, 120, 194, 255)),
            (244.0, Color::rgba(95, 80, 151, 255)),
            (258.0, Color::rgba(125, 89, 124, 255)),
            (274.0, Color::rgba(96, 91, 88, 255)),
            (294.0, Color::rgba(48, 48, 48, 255)),
            (315.0, Color::rgba(7, 7, 7, 255)),
        ],
        ExtendMode::Both,
    )
}

fn abi_band_product_slug(channel: u8) -> String {
    format!("goes_abi_band_{channel:02}")
}

fn abi_band_title(channel: u8) -> String {
    format!("Band {channel}")
}

fn abi_band_scale(channel: u8) -> ColorScale {
    match channel {
        1..=6 => visible_reflectance_scale(),
        7 => shortwave_window_ir_scale(),
        8 => upper_water_vapor_scale(),
        9 => mid_water_vapor_scale(),
        10 => lower_water_vapor_scale(),
        11 => cloud_top_ir_scale(),
        12 => ozone_ir_scale(),
        13 => clean_window_ir_scale(),
        14 => longwave_ir_scale(),
        15 => dirty_window_ir_scale(),
        16 => co2_longwave_ir_scale(),
        _ => clean_window_ir_scale(),
    }
}

fn visible_reflectance_scale() -> ColorScale {
    grayscale_visible_scale(0.0, 1.0)
}

fn bounds_center_lon(bounds: (f64, f64, f64, f64)) -> f64 {
    let west = normalize_lon_360(bounds.0);
    let east = normalize_lon_360(bounds.1);
    let center = if east < west {
        (west + east + 360.0) * 0.5
    } else {
        (west + east) * 0.5
    };
    normalize_longitude_deg(center)
}

fn normalize_lon_360(lon: f64) -> f64 {
    lon.rem_euclid(360.0)
}

fn normalize_longitude_deg(lon: f64) -> f64 {
    let mut value = (lon + 180.0).rem_euclid(360.0) - 180.0;
    if value == -180.0 {
        value = 180.0;
    }
    value
}

fn default_variable_name() -> String {
    "CMI".to_string()
}

fn default_width() -> u32 {
    1400
}

fn default_height() -> u32 {
    1100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn satellite_styles_have_matching_level_and_color_counts() {
        for style in [
            GoesAbiLayerStyle::VisibleRed,
            GoesAbiLayerStyle::CleanIr,
            GoesAbiLayerStyle::ShortwaveIr,
            GoesAbiLayerStyle::WaterVapor,
        ] {
            let ColorScale::Discrete(scale) = style.scale() else {
                panic!("satellite styles should use discrete scales");
            };
            assert_eq!(scale.colors.len() + 1, scale.levels.len());
        }
    }

    #[test]
    fn abi_band_products_have_stable_slugs_and_titles() {
        assert_eq!(abi_band_product_slug(13), "goes_abi_band_13");
        assert_eq!(abi_band_title(7), "Band 7");
        let ColorScale::Discrete(scale) = abi_band_scale(2) else {
            panic!("ABI band scales should be discrete");
        };
        assert_eq!(scale.colors.len() + 1, scale.levels.len());
    }
}
