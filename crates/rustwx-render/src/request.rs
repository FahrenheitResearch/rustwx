use crate::RustwxRenderError;
use rustwx_core as core;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GridShape {
    pub nx: usize,
    pub ny: usize,
}

impl GridShape {
    pub fn new(nx: usize, ny: usize) -> Result<Self, RustwxRenderError> {
        if nx == 0 || ny == 0 {
            return Err(RustwxRenderError::InvalidGridShape { nx, ny });
        }
        Ok(Self { nx, ny })
    }

    pub fn len(self) -> usize {
        self.nx * self.ny
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatLonGrid {
    pub shape: GridShape,
    pub lat_deg: Vec<f32>,
    pub lon_deg: Vec<f32>,
}

impl LatLonGrid {
    pub fn new(
        shape: GridShape,
        lat_deg: Vec<f32>,
        lon_deg: Vec<f32>,
    ) -> Result<Self, RustwxRenderError> {
        if lat_deg.len() != shape.len() || lon_deg.len() != shape.len() {
            return Err(RustwxRenderError::InvalidGridShape {
                nx: shape.nx,
                ny: shape.ny,
            });
        }
        Ok(Self {
            shape,
            lat_deg,
            lon_deg,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProductKey {
    Named(String),
}

impl ProductKey {
    pub fn named<S: Into<String>>(name: S) -> Self {
        Self::Named(name.into())
    }

    pub fn as_named(&self) -> Option<&str> {
        match self {
            Self::Named(name) => Some(name.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field2D {
    pub product: ProductKey,
    pub units: String,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
}

impl Field2D {
    pub fn new<S: Into<String>>(
        product: ProductKey,
        units: S,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<Self, RustwxRenderError> {
        if values.len() != grid.shape.len() {
            return Err(RustwxRenderError::InvalidGridShape {
                nx: grid.shape.nx,
                ny: grid.shape.ny,
            });
        }
        Ok(Self {
            product,
            units: units.into(),
            grid,
            values,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
    pub a: u8,
}

impl Color {
    pub const TRANSPARENT: Self = Self {
        r: 0,
        g: 0,
        b: 0,
        a: 0,
    };
    pub const WHITE: Self = Self {
        r: 255,
        g: 255,
        b: 255,
        a: 255,
    };
    pub const BLACK: Self = Self {
        r: 0,
        g: 0,
        b: 0,
        a: 255,
    };

    pub const fn rgba(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtendMode {
    Neither,
    Min,
    Max,
    Both,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscreteColorScale {
    pub levels: Vec<f64>,
    pub colors: Vec<Color>,
    pub extend: ExtendMode,
    pub mask_below: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColorScale {
    Solar07(crate::solar07::Solar07Preset),
    Discrete(DiscreteColorScale),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedExtent {
    pub x_min: f64,
    pub x_max: f64,
    pub y_min: f64,
    pub y_max: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedDomain {
    pub x: Vec<f64>,
    pub y: Vec<f64>,
    pub extent: ProjectedExtent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedLineOverlay {
    pub points: Vec<(f64, f64)>,
    pub color: Color,
    pub width: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContourLayer {
    pub data: Vec<f32>,
    pub levels: Vec<f64>,
    pub color: Color,
    pub width: u32,
    pub labels: bool,
    pub show_extrema: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindBarbLayer {
    pub u: Vec<f32>,
    pub v: Vec<f32>,
    pub stride_x: usize,
    pub stride_y: usize,
    pub color: Color,
    pub width: u32,
    pub length_px: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapRenderRequest {
    pub field: Field2D,
    pub width: u32,
    pub height: u32,
    pub scale: ColorScale,
    pub background: Color,
    pub colorbar: bool,
    pub title: Option<String>,
    pub subtitle_left: Option<String>,
    pub subtitle_right: Option<String>,
    pub cbar_tick_step: Option<f64>,
    pub projected_domain: Option<ProjectedDomain>,
    pub projected_lines: Vec<ProjectedLineOverlay>,
    pub contours: Vec<ContourLayer>,
    pub wind_barbs: Vec<WindBarbLayer>,
}

impl MapRenderRequest {
    pub fn new(field: Field2D, scale: ColorScale) -> Self {
        Self {
            field,
            width: 1100,
            height: 850,
            scale,
            background: Color::WHITE,
            colorbar: true,
            title: None,
            subtitle_left: None,
            subtitle_right: None,
            cbar_tick_step: None,
            projected_domain: None,
            projected_lines: Vec::new(),
            contours: Vec::new(),
            wind_barbs: Vec::new(),
        }
    }

    pub fn from_core_field(field: core::Field2D, scale: ColorScale) -> Self {
        Self::new(field.into(), scale)
    }

    pub fn for_solar07_product(field: Field2D, product: crate::solar07::Solar07Product) -> Self {
        let mut request = Self::new(field, ColorScale::Solar07(product.scale_preset()));
        request.title = Some(product.display_title().to_string());
        request.cbar_tick_step = product.default_tick_step();
        request
    }

    pub fn for_core_solar07_product(
        field: core::Field2D,
        product: crate::solar07::Solar07Product,
    ) -> Self {
        Self::for_solar07_product(field.into(), product)
    }
}

impl From<core::GridShape> for GridShape {
    fn from(value: core::GridShape) -> Self {
        Self {
            nx: value.nx,
            ny: value.ny,
        }
    }
}

impl From<GridShape> for core::GridShape {
    fn from(value: GridShape) -> Self {
        Self {
            nx: value.nx,
            ny: value.ny,
        }
    }
}

impl From<core::LatLonGrid> for LatLonGrid {
    fn from(value: core::LatLonGrid) -> Self {
        Self {
            shape: value.shape.into(),
            lat_deg: value.lat_deg,
            lon_deg: value.lon_deg,
        }
    }
}

impl From<LatLonGrid> for core::LatLonGrid {
    fn from(value: LatLonGrid) -> Self {
        Self {
            shape: value.shape.into(),
            lat_deg: value.lat_deg,
            lon_deg: value.lon_deg,
        }
    }
}

impl From<core::ProductKey> for ProductKey {
    fn from(value: core::ProductKey) -> Self {
        match value {
            core::ProductKey::Named(name) => Self::Named(name),
        }
    }
}

impl From<ProductKey> for core::ProductKey {
    fn from(value: ProductKey) -> Self {
        match value {
            ProductKey::Named(name) => Self::Named(name),
        }
    }
}

impl From<core::Field2D> for Field2D {
    fn from(value: core::Field2D) -> Self {
        Self {
            product: value.product.into(),
            units: value.units,
            grid: value.grid.into(),
            values: value.values,
        }
    }
}

impl From<Field2D> for core::Field2D {
    fn from(value: Field2D) -> Self {
        Self {
            product: value.product.into(),
            units: value.units,
            grid: value.grid.into(),
            values: value.values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_render_field() -> Field2D {
        let shape = GridShape::new(3, 2).unwrap();
        let grid = LatLonGrid::new(
            shape,
            vec![35.0, 35.0, 35.0, 36.0, 36.0, 36.0],
            vec![-99.0, -98.0, -97.0, -99.0, -98.0, -97.0],
        )
        .unwrap();
        Field2D::new(
            ProductKey::named("sbecape"),
            "J/kg",
            grid,
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        )
        .unwrap()
    }

    #[test]
    fn field2d_round_trips_through_rustwx_core() {
        let render_field = sample_render_field();
        let core_field: core::Field2D = render_field.clone().into();
        let round_trip = Field2D::from(core_field);

        assert_eq!(round_trip, render_field);
    }

    #[test]
    fn solar07_builder_accepts_core_field() {
        let core_field: core::Field2D = sample_render_field().into();
        let request = MapRenderRequest::for_core_solar07_product(
            core_field,
            crate::solar07::Solar07Product::Mlecape,
        );

        assert!(matches!(
            request.scale,
            ColorScale::Solar07(crate::solar07::Solar07Preset::Cape)
        ));
        assert_eq!(request.title.as_deref(), Some("MLECAPE"));
        assert_eq!(request.cbar_tick_step, Some(500.0));
    }
}
