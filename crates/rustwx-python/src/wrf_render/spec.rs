use rustwx_render::{
    BasemapStyle, Color, ColorScale, ContourStyle, DiscreteColorScale, ExtendMode,
    ProductVisualMode, ProjectedDomain, ProjectedExtent, ProjectedLineOverlay,
    ProjectedPolygonFill, WindBarbStyle, solar07,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

fn default_extend_both() -> ExtendMode {
    ExtendMode::Both
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BasemapStyleSpec {
    Filled,
    White,
    None,
}

impl Default for BasemapStyleSpec {
    fn default() -> Self {
        Self::None
    }
}

impl BasemapStyleSpec {
    pub(crate) fn to_option(self) -> Option<BasemapStyle> {
        match self {
            Self::Filled => Some(BasemapStyle::Filled),
            Self::White => Some(BasemapStyle::White),
            Self::None => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PaletteSpec {
    Cape,
    ThreeCape,
    Ehi,
    Srh,
    Stp,
    LapseRate,
    Uh,
    MlMetric,
    Reflectivity,
    Winds,
    Temperature,
    Dewpoint,
    Rh,
    RelVort,
    SimIr,
    GeopotAnomaly,
    Precip,
    ShadedOverlay,
}

impl From<PaletteSpec> for solar07::Solar07Palette {
    fn from(value: PaletteSpec) -> Self {
        match value {
            PaletteSpec::Cape => Self::Cape,
            PaletteSpec::ThreeCape => Self::ThreeCape,
            PaletteSpec::Ehi => Self::Ehi,
            PaletteSpec::Srh => Self::Srh,
            PaletteSpec::Stp => Self::Stp,
            PaletteSpec::LapseRate => Self::LapseRate,
            PaletteSpec::Uh => Self::Uh,
            PaletteSpec::MlMetric => Self::MlMetric,
            PaletteSpec::Reflectivity => Self::Reflectivity,
            PaletteSpec::Winds => Self::Winds,
            PaletteSpec::Temperature => Self::Temperature,
            PaletteSpec::Dewpoint => Self::Dewpoint,
            PaletteSpec::Rh => Self::Rh,
            PaletteSpec::RelVort => Self::RelVort,
            PaletteSpec::SimIr => Self::SimIr,
            PaletteSpec::GeopotAnomaly => Self::GeopotAnomaly,
            PaletteSpec::Precip => Self::Precip,
            PaletteSpec::ShadedOverlay => Self::ShadedOverlay,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ScaleSpec {
    Palette {
        palette: PaletteSpec,
        levels: Vec<f64>,
        #[serde(default = "default_extend_both")]
        extend: ExtendMode,
        mask_below: Option<f64>,
    },
    Discrete {
        levels: Vec<f64>,
        colors: Vec<Color>,
        #[serde(default = "default_extend_both")]
        extend: ExtendMode,
        mask_below: Option<f64>,
    },
}

impl ScaleSpec {
    pub(crate) fn into_color_scale(self) -> ColorScale {
        match self {
            Self::Palette {
                palette,
                levels,
                extend,
                mask_below,
            } => ColorScale::Discrete(solar07::palette_scale(
                palette.into(),
                levels,
                extend,
                mask_below,
            )),
            Self::Discrete {
                levels,
                colors,
                extend,
                mask_below,
            } => ColorScale::Discrete(DiscreteColorScale {
                levels,
                colors,
                extend,
                mask_below,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionSpec {
    pub(crate) map_proj: i32,
    pub(crate) truelat1: Option<f64>,
    pub(crate) truelat2: Option<f64>,
    pub(crate) stand_lon: Option<f64>,
    pub(crate) cen_lat: Option<f64>,
    pub(crate) cen_lon: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ContourSpec {
    pub(crate) levels: Vec<f64>,
    #[serde(default)]
    pub(crate) style: Option<ContourStyle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OverlaySpec {
    pub(crate) scale: ScaleSpec,
    #[serde(default)]
    pub(crate) visual_mode: Option<ProductVisualMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectedSurfaceSpec {
    pub(crate) projection: ProjectionSpec,
    #[serde(default)]
    pub(crate) width: Option<u32>,
    #[serde(default)]
    pub(crate) height: Option<u32>,
    #[serde(default)]
    pub(crate) colorbar: Option<bool>,
    #[serde(default)]
    pub(crate) tick_step: Option<f64>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_left: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_center: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_right: Option<String>,
    #[serde(default)]
    pub(crate) visual_mode: Option<ProductVisualMode>,
    #[serde(default)]
    pub(crate) basemap_style: Option<BasemapStyleSpec>,
    #[serde(default)]
    pub(crate) domain_frame: Option<bool>,
}

impl ProjectedSurfaceSpec {
    pub(crate) fn width(&self) -> u32 {
        self.width.unwrap_or(1100)
    }

    pub(crate) fn height(&self) -> u32 {
        self.height.unwrap_or(850)
    }

    pub(crate) fn colorbar(&self) -> bool {
        self.colorbar.unwrap_or(true)
    }

    pub(crate) fn has_title(&self) -> bool {
        self.title.is_some()
            || self.subtitle_left.is_some()
            || self.subtitle_center.is_some()
            || self.subtitle_right.is_some()
    }

    pub(crate) fn visual_mode(&self) -> ProductVisualMode {
        self.visual_mode
            .unwrap_or(ProductVisualMode::FilledMeteorology)
    }

    pub(crate) fn basemap_style(&self) -> BasemapStyleSpec {
        self.basemap_style.unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RenderSpec {
    pub(crate) output_path: String,
    pub(crate) product_key: String,
    pub(crate) field_units: String,
    pub(crate) scale: ScaleSpec,
    pub(crate) projection: ProjectionSpec,
    #[serde(default)]
    pub(crate) width: Option<u32>,
    #[serde(default)]
    pub(crate) height: Option<u32>,
    #[serde(default)]
    pub(crate) colorbar: Option<bool>,
    #[serde(default)]
    pub(crate) tick_step: Option<f64>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_left: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_center: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_right: Option<String>,
    #[serde(default)]
    pub(crate) visual_mode: Option<ProductVisualMode>,
    #[serde(default)]
    pub(crate) basemap_style: Option<BasemapStyleSpec>,
    #[serde(default)]
    pub(crate) domain_frame: Option<bool>,
    #[serde(default)]
    pub(crate) contour: Option<ContourSpec>,
    #[serde(default)]
    pub(crate) overlay: Option<OverlaySpec>,
    #[serde(default)]
    pub(crate) wind_barbs: Option<WindBarbStyle>,
}

impl From<&RenderSpec> for ProjectedSurfaceSpec {
    fn from(value: &RenderSpec) -> Self {
        Self {
            projection: value.projection.clone(),
            width: value.width,
            height: value.height,
            colorbar: value.colorbar,
            tick_step: value.tick_step,
            title: value.title.clone(),
            subtitle_left: value.subtitle_left.clone(),
            subtitle_center: value.subtitle_center.clone(),
            subtitle_right: value.subtitle_right.clone(),
            visual_mode: value.visual_mode,
            basemap_style: value.basemap_style,
            domain_frame: value.domain_frame,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GridShapeMetadata {
    pub(crate) ny: usize,
    pub(crate) nx: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PixelBoundsMetadata {
    pub(crate) x_start: u32,
    pub(crate) y_start: u32,
    pub(crate) x_end: u32,
    pub(crate) y_end: u32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedExtentMetadata {
    pub(crate) x_min: f64,
    pub(crate) x_max: f64,
    pub(crate) y_min: f64,
    pub(crate) y_max: f64,
}

impl From<&ProjectedExtent> for ProjectedExtentMetadata {
    fn from(value: &ProjectedExtent) -> Self {
        Self {
            x_min: value.x_min,
            x_max: value.x_max,
            y_min: value.y_min,
            y_max: value.y_max,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExtentsMetadata {
    pub(crate) padded: ProjectedExtentMetadata,
    pub(crate) valid: ProjectedExtentMetadata,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectionMetadata {
    pub(crate) map_proj: i32,
    pub(crate) kind: &'static str,
    pub(crate) earth_radius_m: f64,
    pub(crate) parameters: ProjectionSpec,
    pub(crate) projected_crs: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedCornerMetadata {
    pub(crate) index: usize,
    pub(crate) grid_corner: &'static str,
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    pub(crate) x: f64,
    pub(crate) y: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LayoutMetadata {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) colorbar: bool,
    pub(crate) has_title: bool,
    pub(crate) visual_mode: ProductVisualMode,
    pub(crate) crop_top: u32,
    pub(crate) pixel_bounds: PixelBoundsMetadata,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RenderLayersMetadata {
    pub(crate) basemap_style: BasemapStyleSpec,
    pub(crate) contours: bool,
    pub(crate) overlay_fill: bool,
    pub(crate) wind_barbs: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedGeometryMetadata {
    pub(crate) kind: &'static str,
    pub(crate) schema_version: u8,
    pub(crate) grid_shape: GridShapeMetadata,
    pub(crate) pixel_bounds: PixelBoundsMetadata,
    pub(crate) data_extent: [f64; 4],
    pub(crate) valid_data_extent: [f64; 4],
    pub(crate) projection_info: Map<String, Value>,
    pub(crate) projection: ProjectionMetadata,
    pub(crate) extents: ExtentsMetadata,
    pub(crate) layout: LayoutMetadata,
    pub(crate) projected_corners: Vec<ProjectedCornerMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projected_domain: Option<ProjectedDomain>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedMapRenderMetadata {
    pub(crate) kind: &'static str,
    pub(crate) schema_version: u8,
    pub(crate) output_path: String,
    pub(crate) grid_shape: GridShapeMetadata,
    pub(crate) pixel_bounds: PixelBoundsMetadata,
    pub(crate) data_extent: [f64; 4],
    pub(crate) valid_data_extent: [f64; 4],
    pub(crate) projection_info: Map<String, Value>,
    pub(crate) projection: ProjectionMetadata,
    pub(crate) extents: ExtentsMetadata,
    pub(crate) layout: LayoutMetadata,
    pub(crate) projected_corners: Vec<ProjectedCornerMetadata>,
    pub(crate) layers: RenderLayersMetadata,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedOverlayCounts {
    pub(crate) line_overlays: usize,
    pub(crate) line_points: usize,
    pub(crate) polygon_fills: usize,
    pub(crate) polygon_rings: usize,
    pub(crate) polygon_points: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedBasemapOverlayMetadata {
    pub(crate) kind: &'static str,
    pub(crate) schema_version: u8,
    pub(crate) basemap_style: BasemapStyleSpec,
    pub(crate) grid_shape: GridShapeMetadata,
    pub(crate) pixel_bounds: PixelBoundsMetadata,
    pub(crate) data_extent: [f64; 4],
    pub(crate) valid_data_extent: [f64; 4],
    pub(crate) projection_info: Map<String, Value>,
    pub(crate) projection: ProjectionMetadata,
    pub(crate) extents: ExtentsMetadata,
    pub(crate) layout: LayoutMetadata,
    pub(crate) projected_corners: Vec<ProjectedCornerMetadata>,
    pub(crate) counts: ProjectedOverlayCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) line_overlays: Option<Vec<ProjectedLineOverlay>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) polygon_fills: Option<Vec<ProjectedPolygonFill>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProjectedProjectionDescription {
    pub(crate) kind: &'static str,
    pub(crate) schema_version: u8,
    pub(crate) projection_info: Map<String, Value>,
    pub(crate) projection: ProjectionMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionPointSpec {
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    #[serde(default)]
    pub(crate) label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionPathSpec {
    pub(crate) start: CrossSectionPointSpec,
    pub(crate) end: CrossSectionPointSpec,
    #[serde(default)]
    pub(crate) sample_count: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CrossSectionVerticalCoordinate {
    Pressure,
    Height,
    Altitude,
}

impl Default for CrossSectionVerticalCoordinate {
    fn default() -> Self {
        Self::Pressure
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionAxisSpec {
    #[serde(default)]
    pub(crate) coordinate: CrossSectionVerticalCoordinate,
    #[serde(default)]
    pub(crate) top: Option<f64>,
    #[serde(default)]
    pub(crate) bottom: Option<f64>,
    #[serde(default)]
    pub(crate) units: Option<String>,
    #[serde(default)]
    pub(crate) vertical_scale: Option<f64>,
}

impl Default for CrossSectionAxisSpec {
    fn default() -> Self {
        Self {
            coordinate: CrossSectionVerticalCoordinate::Pressure,
            top: None,
            bottom: None,
            units: None,
            vertical_scale: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionFieldSpec {
    pub(crate) product_key: String,
    #[serde(default)]
    pub(crate) field_units: Option<String>,
    #[serde(default)]
    pub(crate) scale: Option<ScaleSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionContourFieldSpec {
    pub(crate) product_key: String,
    pub(crate) levels: Vec<f64>,
    #[serde(default)]
    pub(crate) field_units: Option<String>,
    #[serde(default)]
    pub(crate) style: Option<ContourStyle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionOverlayFieldSpec {
    pub(crate) product_key: String,
    pub(crate) scale: ScaleSpec,
    #[serde(default)]
    pub(crate) field_units: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionWindSpec {
    pub(crate) u_product_key: String,
    pub(crate) v_product_key: String,
    #[serde(default)]
    pub(crate) field_units: Option<String>,
    #[serde(default)]
    pub(crate) style: Option<WindBarbStyle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionRenderSpec {
    #[serde(default)]
    pub(crate) output_path: Option<String>,
    #[serde(default)]
    pub(crate) width: Option<u32>,
    #[serde(default)]
    pub(crate) height: Option<u32>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_left: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_center: Option<String>,
    #[serde(default)]
    pub(crate) subtitle_right: Option<String>,
}

impl Default for CrossSectionRenderSpec {
    fn default() -> Self {
        Self {
            output_path: None,
            width: None,
            height: None,
            title: None,
            subtitle_left: None,
            subtitle_center: None,
            subtitle_right: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrossSectionRequestSpec {
    pub(crate) path: CrossSectionPathSpec,
    pub(crate) field: CrossSectionFieldSpec,
    #[serde(default)]
    pub(crate) contours: Vec<CrossSectionContourFieldSpec>,
    #[serde(default)]
    pub(crate) overlay: Option<CrossSectionOverlayFieldSpec>,
    #[serde(default)]
    pub(crate) wind: Option<CrossSectionWindSpec>,
    #[serde(default)]
    pub(crate) axis: CrossSectionAxisSpec,
    #[serde(default)]
    pub(crate) render: CrossSectionRenderSpec,
}
