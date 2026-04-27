use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RustwxError {
    #[error("invalid grid shape: nx={nx}, ny={ny}")]
    InvalidGridShape { nx: usize, ny: usize },
    #[error("invalid field data length: expected {expected}, got {actual}")]
    InvalidFieldDataLength { expected: usize, actual: usize },
    #[error("unknown model '{0}'")]
    UnknownModel(String),
    #[error("unknown source '{0}'")]
    UnknownSource(String),
    #[error("invalid cycle date '{0}', expected YYYYMMDD")]
    InvalidCycleDate(String),
    #[error("invalid cycle hour {0}, expected 0..23")]
    InvalidCycleHour(u8),
    #[error("invalid UTC timestamp '{0}', expected YYYY-MM-DDTHH:MM:SSZ")]
    InvalidTimeStamp(String),
    #[error("invalid forecast hour {0}")]
    InvalidForecastHour(u16),
    #[error("pressure-level volume requires at least one level")]
    EmptyPressureLevels,
    #[error("invalid pressure level at index {index}: {value}")]
    InvalidPressureLevel { index: usize, value: f32 },
    #[error("hybrid-level volume requires at least one level")]
    EmptyHybridLevels,
    #[error("invalid hybrid level at index {index}: {value}")]
    InvalidHybridLevel { index: usize, value: u16 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GridShape {
    pub nx: usize,
    pub ny: usize,
}

impl GridShape {
    pub fn new(nx: usize, ny: usize) -> Result<Self, RustwxError> {
        if nx == 0 || ny == 0 {
            return Err(RustwxError::InvalidGridShape { nx, ny });
        }
        Ok(Self { nx, ny })
    }

    pub fn len(self) -> usize {
        self.nx * self.ny
    }
}

/// Native map-projection metadata carried alongside a lat/lon grid when the
/// upstream source knows the model's actual projection family.
///
/// This is intentionally lightweight: it captures the projection parameters
/// needed to project model footprints and overlays consistently, while keeping
/// the public core model independent of any GRIB-specific parser types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridProjection {
    Geographic,
    LambertConformal {
        standard_parallel_1_deg: f64,
        standard_parallel_2_deg: f64,
        central_meridian_deg: f64,
    },
    PolarStereographic {
        true_latitude_deg: f64,
        central_meridian_deg: f64,
        south_pole_on_projection_plane: bool,
    },
    Mercator {
        latitude_of_true_scale_deg: f64,
        central_meridian_deg: f64,
    },
    Other {
        template: u16,
    },
}

impl GridProjection {
    pub fn is_projected(&self) -> bool {
        !matches!(self, Self::Geographic)
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
    ) -> Result<Self, RustwxError> {
        if lat_deg.len() != shape.len() || lon_deg.len() != shape.len() {
            return Err(RustwxError::InvalidGridShape {
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat_deg: f64,
    pub lon_deg: f64,
}

impl GeoPoint {
    pub const fn new(lat_deg: f64, lon_deg: f64) -> Self {
        Self { lat_deg, lon_deg }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoBounds {
    pub west_lon_deg: f64,
    pub east_lon_deg: f64,
    pub south_lat_deg: f64,
    pub north_lat_deg: f64,
}

impl GeoBounds {
    pub const fn new(
        west_lon_deg: f64,
        east_lon_deg: f64,
        south_lat_deg: f64,
        north_lat_deg: f64,
    ) -> Self {
        Self {
            west_lon_deg,
            east_lon_deg,
            south_lat_deg,
            north_lat_deg,
        }
    }

    pub fn contains(self, point: GeoPoint) -> bool {
        point.lon_deg >= self.west_lon_deg
            && point.lon_deg <= self.east_lon_deg
            && point.lat_deg >= self.south_lat_deg
            && point.lat_deg <= self.north_lat_deg
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoPolygon {
    pub exterior: Vec<GeoPoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holes: Vec<Vec<GeoPoint>>,
}

impl GeoPolygon {
    pub fn new(exterior: Vec<GeoPoint>, holes: Vec<Vec<GeoPoint>>) -> Self {
        Self { exterior, holes }
    }

    pub fn bounds(&self) -> Option<GeoBounds> {
        let mut iter = self.exterior.iter().copied();
        let first = iter.next()?;
        let mut west = first.lon_deg;
        let mut east = first.lon_deg;
        let mut south = first.lat_deg;
        let mut north = first.lat_deg;
        for point in iter {
            west = west.min(point.lon_deg);
            east = east.max(point.lon_deg);
            south = south.min(point.lat_deg);
            north = north.max(point.lat_deg);
        }
        Some(GeoBounds::new(west, east, south, north))
    }

    pub fn contains(&self, point: GeoPoint) -> bool {
        if self.exterior.len() < 3 || !point_in_ring(point, &self.exterior) {
            return false;
        }
        !self.holes.iter().any(|ring| point_in_ring(point, ring))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldPointSampleMethod {
    Nearest,
    InverseDistance4,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldPointSampleContribution {
    pub grid_index: usize,
    pub location: GeoPoint,
    pub weight: f64,
    pub value: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldPointSample {
    pub point: GeoPoint,
    pub method: FieldPointSampleMethod,
    pub value: Option<f32>,
    pub contributors: Vec<FieldPointSampleContribution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldAreaSummaryMethod {
    CellCentersWithinPolygon,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldAreaSummary {
    pub method: FieldAreaSummaryMethod,
    pub included_cell_count: usize,
    pub valid_cell_count: usize,
    pub missing_cell_count: usize,
    pub min: Option<f32>,
    pub max: Option<f32>,
    pub mean: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeStamp {
    pub iso8601_utc: String,
}

impl TimeStamp {
    pub fn new<S: Into<String>>(iso8601_utc: S) -> Result<Self, RustwxError> {
        let iso8601_utc = iso8601_utc.into();
        validate_utc_timestamp(&iso8601_utc)?;
        Ok(Self { iso8601_utc })
    }

    pub fn as_str(&self) -> &str {
        self.iso8601_utc.as_str()
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

impl std::fmt::Display for ProductKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(name) => f.write_str(name),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CanonicalField {
    Pressure,
    GeopotentialHeight,
    Temperature,
    RelativeHumidity,
    Dewpoint,
    PressureReducedToMeanSeaLevel,
    AbsoluteVorticity,
    RelativeVorticity,
    UWind,
    VWind,
    WindGust,
    TotalCloudCover,
    LowCloudCover,
    MiddleCloudCover,
    HighCloudCover,
    PrecipitableWater,
    TotalPrecipitation,
    Visibility,
    SimulatedInfraredBrightnessTemperature,
    RadarReflectivity,
    LightningFlashDensity,
    CategoricalRain,
    CategoricalFreezingRain,
    CategoricalIcePellets,
    CategoricalSnow,
    LandSeaMask,
    CompositeReflectivity,
    UpdraftHelicity,
    SmokeMassDensity,
    ColumnIntegratedSmoke,
}

impl CanonicalField {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pressure => "pressure",
            Self::GeopotentialHeight => "geopotential_height",
            Self::Temperature => "temperature",
            Self::RelativeHumidity => "relative_humidity",
            Self::Dewpoint => "dewpoint",
            Self::PressureReducedToMeanSeaLevel => "pressure_reduced_to_mean_sea_level",
            Self::AbsoluteVorticity => "absolute_vorticity",
            Self::RelativeVorticity => "relative_vorticity",
            Self::UWind => "u_wind",
            Self::VWind => "v_wind",
            Self::WindGust => "wind_gust",
            Self::TotalCloudCover => "total_cloud_cover",
            Self::LowCloudCover => "low_cloud_cover",
            Self::MiddleCloudCover => "middle_cloud_cover",
            Self::HighCloudCover => "high_cloud_cover",
            Self::PrecipitableWater => "precipitable_water",
            Self::TotalPrecipitation => "total_precipitation",
            Self::Visibility => "visibility",
            Self::SimulatedInfraredBrightnessTemperature => {
                "simulated_infrared_brightness_temperature"
            }
            Self::RadarReflectivity => "radar_reflectivity",
            Self::LightningFlashDensity => "lightning_flash_density",
            Self::CategoricalRain => "categorical_rain",
            Self::CategoricalFreezingRain => "categorical_freezing_rain",
            Self::CategoricalIcePellets => "categorical_ice_pellets",
            Self::CategoricalSnow => "categorical_snow",
            Self::LandSeaMask => "land_sea_mask",
            Self::CompositeReflectivity => "composite_reflectivity",
            Self::UpdraftHelicity => "updraft_helicity",
            Self::SmokeMassDensity => "smoke_mass_density",
            Self::ColumnIntegratedSmoke => "column_integrated_smoke",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Pressure => "Pressure",
            Self::GeopotentialHeight => "Geopotential Height",
            Self::Temperature => "Temperature",
            Self::RelativeHumidity => "Relative Humidity",
            Self::Dewpoint => "Dewpoint",
            Self::PressureReducedToMeanSeaLevel => "Pressure Reduced to Mean Sea Level",
            Self::AbsoluteVorticity => "Absolute Vorticity",
            Self::RelativeVorticity => "Relative Vorticity",
            Self::UWind => "U Wind",
            Self::VWind => "V Wind",
            Self::WindGust => "Wind Gust",
            Self::TotalCloudCover => "Total Cloud Cover",
            Self::LowCloudCover => "Low Cloud Cover",
            Self::MiddleCloudCover => "Middle Cloud Cover",
            Self::HighCloudCover => "High Cloud Cover",
            Self::PrecipitableWater => "Precipitable Water",
            Self::TotalPrecipitation => "Total Precipitation",
            Self::Visibility => "Visibility",
            Self::SimulatedInfraredBrightnessTemperature => {
                "Simulated Infrared Brightness Temperature"
            }
            Self::RadarReflectivity => "Radar Reflectivity",
            Self::LightningFlashDensity => "Lightning Flash Density",
            Self::CategoricalRain => "Categorical Rain",
            Self::CategoricalFreezingRain => "Categorical Freezing Rain",
            Self::CategoricalIcePellets => "Categorical Ice Pellets",
            Self::CategoricalSnow => "Categorical Snow",
            Self::LandSeaMask => "Land-Sea Mask",
            Self::CompositeReflectivity => "Composite Reflectivity",
            Self::UpdraftHelicity => "Updraft Helicity",
            Self::SmokeMassDensity => "Smoke Mass Density",
            Self::ColumnIntegratedSmoke => "Column-Integrated Smoke",
        }
    }

    pub fn native_units(self) -> &'static str {
        match self {
            Self::Pressure => "Pa",
            Self::GeopotentialHeight => "gpm",
            Self::Temperature => "K",
            Self::RelativeHumidity => "%",
            Self::Dewpoint => "K",
            Self::PressureReducedToMeanSeaLevel => "Pa",
            Self::AbsoluteVorticity | Self::RelativeVorticity => "s^-1",
            Self::UWind | Self::VWind => "m/s",
            Self::WindGust => "m/s",
            Self::TotalCloudCover => "%",
            Self::LowCloudCover | Self::MiddleCloudCover | Self::HighCloudCover => "%",
            Self::PrecipitableWater => "kg/m^2",
            Self::TotalPrecipitation => "kg/m^2",
            Self::Visibility => "m",
            Self::SimulatedInfraredBrightnessTemperature => "K",
            Self::RadarReflectivity => "dBZ",
            Self::LightningFlashDensity => "km^-2 day^-1",
            Self::CategoricalRain
            | Self::CategoricalFreezingRain
            | Self::CategoricalIcePellets
            | Self::CategoricalSnow => "0/1",
            Self::LandSeaMask => "fraction",
            Self::CompositeReflectivity => "dBZ",
            Self::UpdraftHelicity => "m^2/s^2",
            Self::SmokeMassDensity => "kg/m^3",
            Self::ColumnIntegratedSmoke => "kg/m^2",
        }
    }
}

impl std::fmt::Display for CanonicalField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VerticalSelector {
    Surface,
    MeanSeaLevel,
    HeightAboveGroundMeters(u16),
    HeightAboveGroundLayerMeters { bottom_m: u16, top_m: u16 },
    HybridLevel(u16),
    IsobaricHpa(u16),
    EntireAtmosphere,
    NominalTop,
}

impl VerticalSelector {
    pub fn as_slug(self) -> String {
        match self {
            Self::Surface => "surface".to_string(),
            Self::MeanSeaLevel => "mean_sea_level".to_string(),
            Self::HeightAboveGroundMeters(height_m) => format!("{height_m}m_agl"),
            Self::HeightAboveGroundLayerMeters { bottom_m, top_m } => {
                format!("{bottom_m}m_to_{top_m}m_agl")
            }
            Self::HybridLevel(level) => format!("hybrid_level_{level}"),
            Self::IsobaricHpa(level_hpa) => format!("{level_hpa}hpa"),
            Self::EntireAtmosphere => "entire_atmosphere".to_string(),
            Self::NominalTop => "nominal_top".to_string(),
        }
    }
}

impl std::fmt::Display for VerticalSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Surface => f.write_str("surface"),
            Self::MeanSeaLevel => f.write_str("mean_sea_level"),
            Self::HeightAboveGroundMeters(height_m) => write!(f, "{height_m}m_agl"),
            Self::HeightAboveGroundLayerMeters { bottom_m, top_m } => {
                write!(f, "{bottom_m}-{top_m}m_agl")
            }
            Self::HybridLevel(level) => write!(f, "hybrid_level_{level}"),
            Self::IsobaricHpa(level_hpa) => write!(f, "{level_hpa}hpa"),
            Self::EntireAtmosphere => f.write_str("entire_atmosphere"),
            Self::NominalTop => f.write_str("nominal_top"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldSelector {
    pub field: CanonicalField,
    pub vertical: VerticalSelector,
}

impl FieldSelector {
    pub const fn new(field: CanonicalField, vertical: VerticalSelector) -> Self {
        Self { field, vertical }
    }

    pub const fn isobaric(field: CanonicalField, level_hpa: u16) -> Self {
        Self::new(field, VerticalSelector::IsobaricHpa(level_hpa))
    }

    pub const fn hybrid_level(field: CanonicalField, level: u16) -> Self {
        Self::new(field, VerticalSelector::HybridLevel(level))
    }

    pub const fn surface(field: CanonicalField) -> Self {
        Self::new(field, VerticalSelector::Surface)
    }

    pub const fn mean_sea_level(field: CanonicalField) -> Self {
        Self::new(field, VerticalSelector::MeanSeaLevel)
    }

    pub const fn height_agl(field: CanonicalField, height_m: u16) -> Self {
        Self::new(field, VerticalSelector::HeightAboveGroundMeters(height_m))
    }

    pub const fn entire_atmosphere(field: CanonicalField) -> Self {
        Self::new(field, VerticalSelector::EntireAtmosphere)
    }

    pub const fn nominal_top(field: CanonicalField) -> Self {
        Self::new(field, VerticalSelector::NominalTop)
    }

    pub const fn height_layer_agl(field: CanonicalField, bottom_m: u16, top_m: u16) -> Self {
        Self::new(
            field,
            VerticalSelector::HeightAboveGroundLayerMeters { bottom_m, top_m },
        )
    }

    pub fn key(self) -> String {
        format!("{}_{}", self.field.as_str(), self.vertical.as_slug())
    }

    pub fn product_key(self) -> ProductKey {
        ProductKey::named(self.key())
    }

    pub fn display_name(self) -> String {
        format!("{} ({})", self.field.display_name(), self.vertical)
    }

    pub fn native_units(self) -> &'static str {
        self.field.native_units()
    }
}

impl std::fmt::Display for FieldSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.field, self.vertical)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductLineage {
    Direct,
    Derived,
    Windowed,
    Bundled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductKind {
    Direct,
    Derived,
    Windowed,
    Bundled,
}

impl ProductKind {
    pub const fn lineage(self) -> ProductLineage {
        match self {
            Self::Direct => ProductLineage::Direct,
            Self::Derived => ProductLineage::Derived,
            Self::Windowed => ProductLineage::Windowed,
            Self::Bundled => ProductLineage::Bundled,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Derived => "derived",
            Self::Windowed => "windowed",
            Self::Bundled => "bundled",
        }
    }
}

impl std::fmt::Display for ProductKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductMaturity {
    Operational,
    Experimental,
    Proof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductSemanticFlag {
    Proxy,
    Composite,
    Alias,
    ProofOriented,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticalProcess {
    Instantaneous,
    Accumulation,
    Average,
    Maximum,
    Minimum,
    Range,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductWindowSpec {
    pub process: StatisticalProcess,
    pub duration_hours: Option<u16>,
}

impl ProductWindowSpec {
    pub fn instantaneous() -> Self {
        Self {
            process: StatisticalProcess::Instantaneous,
            duration_hours: None,
        }
    }

    pub fn accumulation(duration_hours: Option<u16>) -> Self {
        Self {
            process: StatisticalProcess::Accumulation,
            duration_hours,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProductId {
    pub kind: ProductKind,
    pub slug: String,
}

impl ProductId {
    pub fn new<S: Into<String>>(kind: ProductKind, slug: S) -> Self {
        Self {
            kind,
            slug: slug.into(),
        }
    }

    pub fn as_slug(&self) -> &str {
        self.slug.as_str()
    }

    pub fn product_key(&self) -> ProductKey {
        ProductKey::named(self.slug.clone())
    }
}

impl std::fmt::Display for ProductId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.kind, self.slug)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalProductIdentity {
    pub canonical: ProductId,
    pub alias_slugs: Vec<String>,
}

impl CanonicalProductIdentity {
    pub fn new(canonical: ProductId) -> Self {
        Self {
            canonical,
            alias_slugs: Vec::new(),
        }
    }

    pub fn with_alias_slug<S: Into<String>>(mut self, alias_slug: S) -> Self {
        let alias_slug = alias_slug.into();
        if alias_slug != self.canonical.slug && !self.alias_slugs.contains(&alias_slug) {
            self.alias_slugs.push(alias_slug);
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductProvenance {
    pub lineage: ProductLineage,
    pub maturity: ProductMaturity,
    pub flags: Vec<ProductSemanticFlag>,
    pub selector: Option<FieldSelector>,
    pub window: Option<ProductWindowSpec>,
}

impl ProductProvenance {
    pub fn new(lineage: ProductLineage, maturity: ProductMaturity) -> Self {
        Self {
            lineage,
            maturity,
            flags: Vec::new(),
            selector: None,
            window: None,
        }
    }

    pub fn selector_backed(selector: FieldSelector) -> Self {
        Self::new(ProductLineage::Direct, ProductMaturity::Operational).with_selector(selector)
    }

    pub fn with_flag(mut self, flag: ProductSemanticFlag) -> Self {
        if !self.flags.contains(&flag) {
            self.flags.push(flag);
        }
        self
    }

    pub fn with_selector(mut self, selector: FieldSelector) -> Self {
        self.selector = Some(selector);
        self
    }

    pub fn with_window(mut self, window: ProductWindowSpec) -> Self {
        self.window = Some(window);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectedField2D {
    pub selector: FieldSelector,
    pub units: String,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
    pub projection: Option<GridProjection>,
}

impl SelectedField2D {
    pub fn new<S: Into<String>>(
        selector: FieldSelector,
        units: S,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<Self, RustwxError> {
        let expected = grid.shape.len();
        if values.len() != expected {
            return Err(RustwxError::InvalidFieldDataLength {
                expected,
                actual: values.len(),
            });
        }
        Ok(Self {
            selector,
            units: units.into(),
            grid,
            values,
            projection: None,
        })
    }

    pub fn with_projection(mut self, projection: GridProjection) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn into_field2d(self) -> Field2D {
        Field2D {
            product: self.selector.product_key(),
            units: self.units,
            grid: self.grid,
            values: self.values,
        }
    }

    pub fn sample_point(
        &self,
        point: GeoPoint,
        method: FieldPointSampleMethod,
    ) -> FieldPointSample {
        sample_field_point(&self.grid, &self.values, point, method)
    }

    pub fn summarize_polygon(&self, polygon: &GeoPolygon) -> FieldAreaSummary {
        summarize_field_polygon(&self.grid, &self.values, polygon)
    }
}

impl From<SelectedField2D> for Field2D {
    fn from(value: SelectedField2D) -> Self {
        value.into_field2d()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectedHybridLevelVolume {
    pub field: CanonicalField,
    pub levels_hybrid: Vec<u16>,
    pub units: String,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
    pub projection: Option<GridProjection>,
}

impl SelectedHybridLevelVolume {
    pub fn new<S: Into<String>>(
        field: CanonicalField,
        levels_hybrid: Vec<u16>,
        units: S,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<Self, RustwxError> {
        validate_hybrid_levels(&levels_hybrid)?;
        let expected = levels_hybrid.len() * grid.shape.len();
        if values.len() != expected {
            return Err(RustwxError::InvalidFieldDataLength {
                expected,
                actual: values.len(),
            });
        }
        Ok(Self {
            field,
            levels_hybrid,
            units: units.into(),
            grid,
            values,
            projection: None,
        })
    }

    pub fn level_count(&self) -> usize {
        self.levels_hybrid.len()
    }

    pub fn level_slice(&self, level_index: usize) -> Option<&[f32]> {
        let layer_len = self.grid.shape.len();
        let start = level_index.checked_mul(layer_len)?;
        let end = start.checked_add(layer_len)?;
        self.values.get(start..end)
    }

    pub fn selector_at(&self, level_index: usize) -> Option<FieldSelector> {
        self.levels_hybrid
            .get(level_index)
            .copied()
            .map(|level| FieldSelector::hybrid_level(self.field, level))
    }

    pub fn with_projection(mut self, projection: GridProjection) -> Self {
        self.projection = Some(projection);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductKeyMetadata {
    pub display_name: String,
    pub description: Option<String>,
    pub native_units: Option<String>,
    pub category: Option<String>,
    pub identity: Option<CanonicalProductIdentity>,
    pub provenance: Option<ProductProvenance>,
}

impl ProductKeyMetadata {
    pub fn new<S: Into<String>>(display_name: S) -> Self {
        Self {
            display_name: display_name.into(),
            description: None,
            native_units: None,
            category: None,
            identity: None,
            provenance: None,
        }
    }

    pub fn with_description<S: Into<String>>(mut self, description: S) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_native_units<S: Into<String>>(mut self, native_units: S) -> Self {
        self.native_units = Some(native_units.into());
        self
    }

    pub fn with_category<S: Into<String>>(mut self, category: S) -> Self {
        self.category = Some(category.into());
        self
    }

    pub fn with_identity(mut self, identity: CanonicalProductIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn with_provenance(mut self, provenance: ProductProvenance) -> Self {
        self.provenance = Some(provenance);
        self
    }
}

impl FieldSelector {
    pub fn product_id(self) -> ProductId {
        ProductId::new(ProductKind::Direct, self.key())
    }

    pub fn product_provenance(self) -> ProductProvenance {
        ProductProvenance::selector_backed(self)
    }

    pub fn product_metadata(self) -> ProductKeyMetadata {
        ProductKeyMetadata::new(self.display_name())
            .with_native_units(self.native_units())
            .with_identity(CanonicalProductIdentity::new(self.product_id()))
            .with_provenance(self.product_provenance())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelTimestep {
    pub model: ModelId,
    pub cycle: CycleSpec,
    pub forecast_hour: u16,
    pub valid_time: TimeStamp,
    pub source: Option<SourceId>,
}

impl ModelTimestep {
    pub fn new(
        model: ModelId,
        cycle: CycleSpec,
        forecast_hour: u16,
        valid_time: TimeStamp,
    ) -> Result<Self, RustwxError> {
        Self::with_source(model, cycle, forecast_hour, valid_time, None)
    }

    pub fn with_source(
        model: ModelId,
        cycle: CycleSpec,
        forecast_hour: u16,
        valid_time: TimeStamp,
        source: Option<SourceId>,
    ) -> Result<Self, RustwxError> {
        if forecast_hour > 999 {
            return Err(RustwxError::InvalidForecastHour(forecast_hour));
        }
        Ok(Self {
            model,
            cycle,
            forecast_hour,
            valid_time,
            source,
        })
    }

    pub fn request<S: Into<String>>(&self, product: S) -> Result<ModelRunRequest, RustwxError> {
        ModelRunRequest::new(self.model, self.cycle.clone(), self.forecast_hour, product)
    }

    pub fn descriptor(&self) -> ForecastDescriptor {
        ForecastDescriptor::new(
            self.model.as_str(),
            self.cycle.clone(),
            self.valid_time.clone(),
            self.forecast_hour,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFieldMetadata {
    pub timestep: ModelTimestep,
    pub product: ProductKey,
    pub product_metadata: Option<ProductKeyMetadata>,
    pub units: String,
}

impl ModelFieldMetadata {
    pub fn new<S: Into<String>>(timestep: ModelTimestep, product: ProductKey, units: S) -> Self {
        Self {
            timestep,
            product,
            product_metadata: None,
            units: units.into(),
        }
    }

    pub fn with_product_metadata(mut self, product_metadata: ProductKeyMetadata) -> Self {
        self.product_metadata = Some(product_metadata);
        self
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
    ) -> Result<Self, RustwxError> {
        if values.len() != grid.shape.len() {
            return Err(RustwxError::InvalidGridShape {
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

    pub fn sample_point(
        &self,
        point: GeoPoint,
        method: FieldPointSampleMethod,
    ) -> FieldPointSample {
        sample_field_point(&self.grid, &self.values, point, method)
    }

    pub fn summarize_polygon(&self, polygon: &GeoPolygon) -> FieldAreaSummary {
        summarize_field_polygon(&self.grid, &self.values, polygon)
    }
}

fn sample_field_point(
    grid: &LatLonGrid,
    values: &[f32],
    point: GeoPoint,
    method: FieldPointSampleMethod,
) -> FieldPointSample {
    let keep = match method {
        FieldPointSampleMethod::Nearest => 1usize,
        FieldPointSampleMethod::InverseDistance4 => 4usize,
    };
    let mut nearest = Vec::<(usize, f64)>::new();
    for idx in 0..grid.shape.len() {
        let distance = geographic_distance_score(grid, idx, point);
        insert_best_sample_candidate(&mut nearest, keep, idx, distance);
    }
    if nearest.is_empty() {
        return FieldPointSample {
            point,
            method,
            value: None,
            contributors: Vec::new(),
        };
    }

    let exact_match = nearest[0].1 <= 1.0e-12;
    let mut contributions = if exact_match || matches!(method, FieldPointSampleMethod::Nearest) {
        vec![point_sample_contribution(grid, values, nearest[0].0, 1.0)]
    } else {
        let mut weights = nearest
            .iter()
            .map(|(_, distance)| 1.0 / distance.max(1.0e-12))
            .collect::<Vec<_>>();
        let weight_sum = weights.iter().sum::<f64>().max(1.0e-12);
        for weight in &mut weights {
            *weight /= weight_sum;
        }
        nearest
            .iter()
            .zip(weights.iter())
            .map(|((idx, _), weight)| point_sample_contribution(grid, values, *idx, *weight))
            .collect::<Vec<_>>()
    };

    let finite_weight_sum = contributions
        .iter()
        .filter(|entry| entry.value.map(|value| value.is_finite()).unwrap_or(false))
        .map(|entry| entry.weight)
        .sum::<f64>();
    let value = if finite_weight_sum <= 0.0 {
        None
    } else {
        for contribution in &mut contributions {
            if contribution
                .value
                .map(|sample| sample.is_finite())
                .unwrap_or(false)
            {
                contribution.weight /= finite_weight_sum;
            } else {
                contribution.weight = 0.0;
            }
        }
        Some(
            contributions
                .iter()
                .filter_map(|entry| entry.value.map(|sample| sample as f64 * entry.weight))
                .sum::<f64>() as f32,
        )
    };

    FieldPointSample {
        point,
        method,
        value,
        contributors: contributions,
    }
}

fn summarize_field_polygon(
    grid: &LatLonGrid,
    values: &[f32],
    polygon: &GeoPolygon,
) -> FieldAreaSummary {
    let Some(bounds) = polygon.bounds() else {
        return FieldAreaSummary {
            method: FieldAreaSummaryMethod::CellCentersWithinPolygon,
            included_cell_count: 0,
            valid_cell_count: 0,
            missing_cell_count: 0,
            min: None,
            max: None,
            mean: None,
        };
    };

    let mut included_cell_count = 0usize;
    let mut valid_cell_count = 0usize;
    let mut missing_cell_count = 0usize;
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    let mut sum = 0.0f64;

    for idx in 0..grid.shape.len() {
        let point = GeoPoint::new(grid.lat_deg[idx] as f64, grid.lon_deg[idx] as f64);
        if !bounds.contains(point) || !polygon.contains(point) {
            continue;
        }
        included_cell_count += 1;
        let value = values[idx];
        if value.is_finite() {
            valid_cell_count += 1;
            min = min.min(value);
            max = max.max(value);
            sum += value as f64;
        } else {
            missing_cell_count += 1;
        }
    }

    FieldAreaSummary {
        method: FieldAreaSummaryMethod::CellCentersWithinPolygon,
        included_cell_count,
        valid_cell_count,
        missing_cell_count,
        min: (valid_cell_count > 0).then_some(min),
        max: (valid_cell_count > 0).then_some(max),
        mean: (valid_cell_count > 0).then_some(sum / valid_cell_count as f64),
    }
}

fn point_sample_contribution(
    grid: &LatLonGrid,
    values: &[f32],
    idx: usize,
    weight: f64,
) -> FieldPointSampleContribution {
    FieldPointSampleContribution {
        grid_index: idx,
        location: GeoPoint::new(grid.lat_deg[idx] as f64, grid.lon_deg[idx] as f64),
        weight,
        value: values.get(idx).copied(),
    }
}

fn insert_best_sample_candidate(
    nearest: &mut Vec<(usize, f64)>,
    keep: usize,
    idx: usize,
    distance: f64,
) {
    let keep = keep.max(1);
    let insert_at = nearest
        .iter()
        .position(|&(existing_idx, existing_distance)| {
            distance < existing_distance
                || ((distance - existing_distance).abs() <= 1.0e-12 && idx < existing_idx)
        })
        .unwrap_or(nearest.len());
    if insert_at >= keep {
        return;
    }
    nearest.insert(insert_at, (idx, distance));
    if nearest.len() > keep {
        nearest.truncate(keep);
    }
}

fn geographic_distance_score(grid: &LatLonGrid, idx: usize, point: GeoPoint) -> f64 {
    let cos_lat = point.lat_deg.to_radians().cos().abs().max(0.2);
    let dlat = grid.lat_deg[idx] as f64 - point.lat_deg;
    let dlon = normalized_longitude_delta(grid.lon_deg[idx] as f64 - point.lon_deg) * cos_lat;
    dlat * dlat + dlon * dlon
}

fn point_in_ring(point: GeoPoint, ring: &[GeoPoint]) -> bool {
    if ring.len() < 3 {
        return false;
    }
    let point_x = 0.0f64;
    let point_y = point.lat_deg;
    let mut inside = false;
    let mut previous = *ring.last().expect("ring length checked");

    for current in ring {
        let current_x = normalized_longitude_delta(current.lon_deg - point.lon_deg);
        let current_y = current.lat_deg;
        let previous_x = normalized_longitude_delta(previous.lon_deg - point.lon_deg);
        let previous_y = previous.lat_deg;

        if point_on_segment(
            point_x, point_y, current_x, current_y, previous_x, previous_y,
        ) {
            return true;
        }

        let intersects = ((current_y > point_y) != (previous_y > point_y))
            && (point_x
                < (previous_x - current_x) * (point_y - current_y) / (previous_y - current_y)
                    + current_x);
        if intersects {
            inside = !inside;
        }
        previous = *current;
    }
    inside
}

fn point_on_segment(
    point_x: f64,
    point_y: f64,
    start_x: f64,
    start_y: f64,
    end_x: f64,
    end_y: f64,
) -> bool {
    let cross = (point_y - start_y) * (end_x - start_x) - (point_x - start_x) * (end_y - start_y);
    if cross.abs() > 1.0e-9 {
        return false;
    }
    let min_x = start_x.min(end_x) - 1.0e-9;
    let max_x = start_x.max(end_x) + 1.0e-9;
    let min_y = start_y.min(end_y) - 1.0e-9;
    let max_y = start_y.max(end_y) + 1.0e-9;
    point_x >= min_x && point_x <= max_x && point_y >= min_y && point_y <= max_y
}

fn normalized_longitude_delta(delta_deg: f64) -> f64 {
    let mut delta = delta_deg;
    while delta <= -180.0 {
        delta += 360.0;
    }
    while delta > 180.0 {
        delta -= 360.0;
    }
    delta
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field3D {
    pub product: ProductKey,
    pub units: String,
    pub levels: Vec<f32>,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
}

impl Field3D {
    pub fn new<S: Into<String>>(
        product: ProductKey,
        units: S,
        levels: Vec<f32>,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<Self, RustwxError> {
        let expected = levels.len() * grid.shape.len();
        if values.len() != expected {
            return Err(RustwxError::InvalidGridShape {
                nx: grid.shape.nx,
                ny: grid.shape.ny,
            });
        }
        Ok(Self {
            product,
            units: units.into(),
            levels,
            grid,
            values,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelField2D {
    pub metadata: ModelFieldMetadata,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
}

impl ModelField2D {
    pub fn new(
        metadata: ModelFieldMetadata,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<Self, RustwxError> {
        let expected = grid.shape.len();
        if values.len() != expected {
            return Err(RustwxError::InvalidFieldDataLength {
                expected,
                actual: values.len(),
            });
        }
        Ok(Self {
            metadata,
            grid,
            values,
        })
    }

    pub fn into_field2d(self) -> Field2D {
        Field2D {
            product: self.metadata.product,
            units: self.metadata.units,
            grid: self.grid,
            values: self.values,
        }
    }
}

impl From<ModelField2D> for Field2D {
    fn from(value: ModelField2D) -> Self {
        value.into_field2d()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PressureLevelVolume {
    pub metadata: ModelFieldMetadata,
    pub levels_hpa: Vec<f32>,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
}

impl PressureLevelVolume {
    pub fn new(
        metadata: ModelFieldMetadata,
        levels_hpa: Vec<f32>,
        grid: LatLonGrid,
        values: Vec<f32>,
    ) -> Result<Self, RustwxError> {
        validate_pressure_levels(&levels_hpa)?;
        let expected = levels_hpa.len() * grid.shape.len();
        if values.len() != expected {
            return Err(RustwxError::InvalidFieldDataLength {
                expected,
                actual: values.len(),
            });
        }
        Ok(Self {
            metadata,
            levels_hpa,
            grid,
            values,
        })
    }

    pub fn level_count(&self) -> usize {
        self.levels_hpa.len()
    }

    pub fn level_slice(&self, level_index: usize) -> Option<&[f32]> {
        let layer_len = self.grid.shape.len();
        let start = level_index.checked_mul(layer_len)?;
        let end = start.checked_add(layer_len)?;
        self.values.get(start..end)
    }

    pub fn into_field3d(self) -> Field3D {
        Field3D {
            product: self.metadata.product,
            units: self.metadata.units,
            levels: self.levels_hpa,
            grid: self.grid,
            values: self.values,
        }
    }
}

impl From<PressureLevelVolume> for Field3D {
    fn from(value: PressureLevelVolume) -> Self {
        value.into_field3d()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ModelId {
    Hrrr,
    Gfs,
    EcmwfOpenData,
    RrfsA,
    WrfGdex,
}

impl ModelId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hrrr => "hrrr",
            Self::Gfs => "gfs",
            Self::EcmwfOpenData => "ecmwf-open-data",
            Self::RrfsA => "rrfs-a",
            Self::WrfGdex => "wrf-gdex",
        }
    }
}

impl std::fmt::Display for ModelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ModelId {
    type Err = RustwxError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "hrrr" => Ok(Self::Hrrr),
            "gfs" => Ok(Self::Gfs),
            "ecmwf" | "ifs" | "ecmwf-open-data" | "ecmwf_open_data" => Ok(Self::EcmwfOpenData),
            "rrfs-a" | "rrfsa" | "rrfs_a" => Ok(Self::RrfsA),
            "wrf-gdex" | "wrf_gdex" | "wrfgdex" | "wrf" => Ok(Self::WrfGdex),
            other => Err(RustwxError::UnknownModel(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalDataFamily {
    Surface,
    Pressure,
    Native,
}

impl CanonicalDataFamily {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Pressure => "pressure",
            Self::Native => "native",
        }
    }
}

impl std::fmt::Display for CanonicalDataFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalBundleDescriptor {
    SurfaceAnalysis,
    PressureAnalysis,
    NativeAnalysis,
}

impl CanonicalBundleDescriptor {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SurfaceAnalysis => "surface_analysis",
            Self::PressureAnalysis => "pressure_analysis",
            Self::NativeAnalysis => "native_analysis",
        }
    }

    pub const fn family(self) -> CanonicalDataFamily {
        match self {
            Self::SurfaceAnalysis => CanonicalDataFamily::Surface,
            Self::PressureAnalysis => CanonicalDataFamily::Pressure,
            Self::NativeAnalysis => CanonicalDataFamily::Native,
        }
    }
}

impl std::fmt::Display for CanonicalBundleDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Typed identity for an executable input bundle: the unique key by which
/// the planner dedupes fetch+decode work across products. A
/// `CanonicalBundleId` resolves to exactly one fetched GRIB file (one
/// `(model, cycle, forecast_hour, source, native_product)` tuple); two
/// `BundleRequirement`s with the same id share the same load.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CanonicalBundleId {
    pub model: ModelId,
    pub cycle: CycleSpec,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub bundle: CanonicalBundleDescriptor,
    pub native_product: String,
}

impl CanonicalBundleId {
    pub fn new<S: Into<String>>(
        model: ModelId,
        cycle: CycleSpec,
        forecast_hour: u16,
        source: SourceId,
        bundle: CanonicalBundleDescriptor,
        native_product: S,
    ) -> Self {
        Self {
            model,
            cycle,
            forecast_hour,
            source,
            bundle,
            native_product: native_product.into(),
        }
    }

    pub fn family(&self) -> CanonicalDataFamily {
        self.bundle.family()
    }
}

impl std::fmt::Display for CanonicalBundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}{:02}z+f{:03}:{}",
            self.bundle,
            self.model,
            self.cycle.date_yyyymmdd,
            self.cycle.hour_utc,
            self.forecast_hour,
            self.native_product
        )
    }
}

/// What a product needs from the fetch layer. The planner translates each
/// requirement into a `CanonicalBundleId` for dedupe.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BundleRequirement {
    pub bundle: CanonicalBundleDescriptor,
    pub forecast_hour: u16,
    pub native_override: Option<String>,
}

impl BundleRequirement {
    pub fn new(bundle: CanonicalBundleDescriptor, forecast_hour: u16) -> Self {
        Self {
            bundle,
            forecast_hour,
            native_override: None,
        }
    }

    pub fn with_native_override<S: Into<String>>(mut self, native_product: S) -> Self {
        self.native_override = Some(native_product.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub enum SourceId {
    Aws,
    Nomads,
    Google,
    Azure,
    Ecmwf,
    Ncei,
    Gdex,
}

impl SourceId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Nomads => "nomads",
            Self::Google => "google",
            Self::Azure => "azure",
            Self::Ecmwf => "ecmwf",
            Self::Ncei => "ncei",
            Self::Gdex => "gdex",
        }
    }
}

impl std::fmt::Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SourceId {
    type Err = RustwxError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "aws" => Ok(Self::Aws),
            "nomads" => Ok(Self::Nomads),
            "google" => Ok(Self::Google),
            "azure" => Ok(Self::Azure),
            "ecmwf" => Ok(Self::Ecmwf),
            "ncei" => Ok(Self::Ncei),
            "gdex" => Ok(Self::Gdex),
            other => Err(RustwxError::UnknownSource(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CycleSpec {
    pub date_yyyymmdd: String,
    pub hour_utc: u8,
}

impl CycleSpec {
    pub fn new<S: Into<String>>(date_yyyymmdd: S, hour_utc: u8) -> Result<Self, RustwxError> {
        let date_yyyymmdd = date_yyyymmdd.into();
        if date_yyyymmdd.len() != 8 || !date_yyyymmdd.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(RustwxError::InvalidCycleDate(date_yyyymmdd));
        }
        validate_cycle_date(&date_yyyymmdd)?;
        if hour_utc > 23 {
            return Err(RustwxError::InvalidCycleHour(hour_utc));
        }
        Ok(Self {
            date_yyyymmdd,
            hour_utc,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRunRequest {
    pub model: ModelId,
    pub cycle: CycleSpec,
    pub forecast_hour: u16,
    pub product: String,
}

impl ModelRunRequest {
    pub fn new<S: Into<String>>(
        model: ModelId,
        cycle: CycleSpec,
        forecast_hour: u16,
        product: S,
    ) -> Result<Self, RustwxError> {
        if forecast_hour > 999 {
            return Err(RustwxError::InvalidForecastHour(forecast_hour));
        }
        Ok(Self {
            model,
            cycle,
            forecast_hour,
            product: product.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedUrl {
    pub source: SourceId,
    pub grib_url: String,
    pub idx_url: Option<String>,
}

impl ResolvedUrl {
    pub fn availability_probe_url(&self) -> &str {
        self.idx_url.as_deref().unwrap_or(&self.grib_url)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastDescriptor {
    pub model: String,
    pub cycle: CycleSpec,
    pub valid_time: TimeStamp,
    pub forecast_hour: u16,
}

impl ForecastDescriptor {
    pub fn new<S: Into<String>>(
        model: S,
        cycle: CycleSpec,
        valid_time: TimeStamp,
        forecast_hour: u16,
    ) -> Self {
        Self {
            model: model.into(),
            cycle,
            valid_time,
            forecast_hour,
        }
    }
}

fn validate_cycle_date(date_yyyymmdd: &str) -> Result<(), RustwxError> {
    let year = date_yyyymmdd[..4]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidCycleDate(date_yyyymmdd.to_string()))?;
    let month = date_yyyymmdd[4..6]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidCycleDate(date_yyyymmdd.to_string()))?;
    let day = date_yyyymmdd[6..8]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidCycleDate(date_yyyymmdd.to_string()))?;

    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => return Err(RustwxError::InvalidCycleDate(date_yyyymmdd.to_string())),
    };

    if day == 0 || day > max_day {
        return Err(RustwxError::InvalidCycleDate(date_yyyymmdd.to_string()));
    }

    Ok(())
}

fn validate_utc_timestamp(iso8601_utc: &str) -> Result<(), RustwxError> {
    let bytes = iso8601_utc.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return Err(RustwxError::InvalidTimeStamp(iso8601_utc.to_string()));
    }

    let year = iso8601_utc[..4]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;
    let month = iso8601_utc[5..7]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;
    let day = iso8601_utc[8..10]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;
    let hour = iso8601_utc[11..13]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;
    let minute = iso8601_utc[14..16]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;
    let second = iso8601_utc[17..19]
        .parse::<u32>()
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;

    validate_cycle_date(&format!("{year:04}{month:02}{day:02}"))
        .map_err(|_| RustwxError::InvalidTimeStamp(iso8601_utc.to_string()))?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(RustwxError::InvalidTimeStamp(iso8601_utc.to_string()));
    }

    Ok(())
}

fn is_leap_year(year: u32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn validate_pressure_levels(levels_hpa: &[f32]) -> Result<(), RustwxError> {
    if levels_hpa.is_empty() {
        return Err(RustwxError::EmptyPressureLevels);
    }

    for (index, value) in levels_hpa.iter().copied().enumerate() {
        if !value.is_finite() || value <= 0.0 {
            return Err(RustwxError::InvalidPressureLevel { index, value });
        }
    }

    Ok(())
}

fn validate_hybrid_levels(levels_hybrid: &[u16]) -> Result<(), RustwxError> {
    if levels_hybrid.is_empty() {
        return Err(RustwxError::EmptyHybridLevels);
    }

    for (index, value) in levels_hybrid.iter().copied().enumerate() {
        if value == 0 {
            return Err(RustwxError::InvalidHybridLevel { index, value });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_shape_len_matches() {
        let shape = GridShape::new(3, 2).unwrap();
        assert_eq!(shape.len(), 6);
    }

    #[test]
    fn model_id_aliases_round_trip() {
        assert_eq!("rrfs_a".parse::<ModelId>().unwrap(), ModelId::RrfsA);
        assert_eq!("ecmwf".parse::<ModelId>().unwrap(), ModelId::EcmwfOpenData);
        assert_eq!("wrf".parse::<ModelId>().unwrap(), ModelId::WrfGdex);
        assert_eq!(ModelId::Hrrr.to_string(), "hrrr");
        assert_eq!(ModelId::WrfGdex.to_string(), "wrf-gdex");
        assert_eq!("gdex".parse::<SourceId>().unwrap(), SourceId::Gdex);
        assert_eq!(SourceId::Gdex.to_string(), "gdex");
    }

    #[test]
    fn cycle_spec_validates_inputs() {
        assert!(CycleSpec::new("20260414", 20).is_ok());
        assert!(CycleSpec::new("20240229", 20).is_ok());
        assert!(matches!(
            CycleSpec::new("20260229", 20),
            Err(RustwxError::InvalidCycleDate(_))
        ));
        assert!(matches!(
            CycleSpec::new("2026-04-14", 20),
            Err(RustwxError::InvalidCycleDate(_))
        ));
        assert!(matches!(
            CycleSpec::new("20260414", 24),
            Err(RustwxError::InvalidCycleHour(24))
        ));
    }

    #[test]
    fn product_key_helpers_expose_name() {
        let key = ProductKey::named("cape_sfc");
        assert_eq!(key.as_named(), Some("cape_sfc"));
        assert_eq!(key.to_string(), "cape_sfc");
    }

    #[test]
    fn timestamp_validates_basic_utc_format() {
        assert!(TimeStamp::new("2026-04-15T00:00:00Z").is_ok());
        assert!(matches!(
            TimeStamp::new("2026-04-15 00:00:00Z"),
            Err(RustwxError::InvalidTimeStamp(_))
        ));
        assert!(matches!(
            TimeStamp::new("2026-02-29T00:00:00Z"),
            Err(RustwxError::InvalidTimeStamp(_))
        ));
    }

    #[test]
    fn field_selector_builds_keys_and_units() {
        let hybrid_pressure = FieldSelector::hybrid_level(CanonicalField::Pressure, 17);
        assert_eq!(hybrid_pressure.key(), "pressure_hybrid_level_17");
        assert_eq!(hybrid_pressure.native_units(), "Pa");

        let selector = FieldSelector::isobaric(CanonicalField::Temperature, 500);
        assert_eq!(selector.to_string(), "temperature@500hpa");
        assert_eq!(selector.key(), "temperature_500hpa");
        assert_eq!(
            selector.product_key().as_named(),
            Some("temperature_500hpa")
        );

        let temp_700 = FieldSelector::isobaric(CanonicalField::Temperature, 700);
        assert_eq!(temp_700.key(), "temperature_700hpa");

        let rh_700 = FieldSelector::isobaric(CanonicalField::RelativeHumidity, 700);
        assert_eq!(rh_700.key(), "relative_humidity_700hpa");
        assert_eq!(rh_700.native_units(), "%");

        let dewpoint_850 = FieldSelector::isobaric(CanonicalField::Dewpoint, 850);
        assert_eq!(dewpoint_850.key(), "dewpoint_850hpa");
        assert_eq!(dewpoint_850.native_units(), "K");

        let temp_2m = FieldSelector::height_agl(CanonicalField::Temperature, 2);
        assert_eq!(temp_2m.key(), "temperature_2m_agl");
        assert_eq!(temp_2m.native_units(), "K");

        let dewpoint_2m = FieldSelector::height_agl(CanonicalField::Dewpoint, 2);
        assert_eq!(dewpoint_2m.key(), "dewpoint_2m_agl");
        assert_eq!(dewpoint_2m.native_units(), "K");

        let rh_2m = FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2);
        assert_eq!(rh_2m.key(), "relative_humidity_2m_agl");
        assert_eq!(rh_2m.native_units(), "%");

        let wind_10m = FieldSelector::height_agl(CanonicalField::UWind, 10);
        assert_eq!(wind_10m.key(), "u_wind_10m_agl");
        assert_eq!(wind_10m.native_units(), "m/s");

        let wind_gust_10m = FieldSelector::height_agl(CanonicalField::WindGust, 10);
        assert_eq!(wind_gust_10m.key(), "wind_gust_10m_agl");
        assert_eq!(wind_gust_10m.native_units(), "m/s");

        let mslp = FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel);
        assert_eq!(
            mslp.key(),
            "pressure_reduced_to_mean_sea_level_mean_sea_level"
        );
        assert_eq!(mslp.native_units(), "Pa");

        let absolute_vorticity_500 =
            FieldSelector::isobaric(CanonicalField::AbsoluteVorticity, 500);
        assert_eq!(absolute_vorticity_500.key(), "absolute_vorticity_500hpa");
        assert_eq!(absolute_vorticity_500.native_units(), "s^-1");

        let relative_vorticity_500 =
            FieldSelector::isobaric(CanonicalField::RelativeVorticity, 500);
        assert_eq!(relative_vorticity_500.key(), "relative_vorticity_500hpa");
        assert_eq!(relative_vorticity_500.native_units(), "s^-1");

        let reflectivity = FieldSelector::entire_atmosphere(CanonicalField::CompositeReflectivity);
        assert_eq!(
            reflectivity.key(),
            "composite_reflectivity_entire_atmosphere"
        );

        let reflectivity_1km = FieldSelector::height_agl(CanonicalField::RadarReflectivity, 1000);
        assert_eq!(reflectivity_1km.key(), "radar_reflectivity_1000m_agl");
        assert_eq!(reflectivity_1km.native_units(), "dBZ");

        let pwat = FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater);
        assert_eq!(pwat.key(), "precipitable_water_entire_atmosphere");
        assert_eq!(pwat.native_units(), "kg/m^2");

        let cloud_cover = FieldSelector::entire_atmosphere(CanonicalField::TotalCloudCover);
        assert_eq!(cloud_cover.key(), "total_cloud_cover_entire_atmosphere");
        assert_eq!(cloud_cover.native_units(), "%");

        let simulated_ir =
            FieldSelector::nominal_top(CanonicalField::SimulatedInfraredBrightnessTemperature);
        assert_eq!(
            simulated_ir.key(),
            "simulated_infrared_brightness_temperature_nominal_top"
        );
        assert_eq!(simulated_ir.native_units(), "K");

        let visibility = FieldSelector::surface(CanonicalField::Visibility);
        assert_eq!(visibility.key(), "visibility_surface");
        assert_eq!(visibility.native_units(), "m");

        let lsm = FieldSelector::surface(CanonicalField::LandSeaMask);
        assert_eq!(lsm.key(), "land_sea_mask_surface");
        assert_eq!(lsm.native_units(), "fraction");

        let lightning = FieldSelector::height_agl(CanonicalField::LightningFlashDensity, 2);
        assert_eq!(lightning.key(), "lightning_flash_density_2m_agl");
        assert_eq!(lightning.native_units(), "km^-2 day^-1");

        let uh = FieldSelector::height_layer_agl(CanonicalField::UpdraftHelicity, 2000, 5000);
        assert_eq!(uh.key(), "updraft_helicity_2000m_to_5000m_agl");

        let smoke_8m = FieldSelector::height_agl(CanonicalField::SmokeMassDensity, 8);
        assert_eq!(smoke_8m.key(), "smoke_mass_density_8m_agl");
        assert_eq!(smoke_8m.native_units(), "kg/m^3");

        let smoke_hybrid = FieldSelector::hybrid_level(CanonicalField::SmokeMassDensity, 50);
        assert_eq!(smoke_hybrid.key(), "smoke_mass_density_hybrid_level_50");
        assert_eq!(smoke_hybrid.native_units(), "kg/m^3");

        let smoke_column = FieldSelector::entire_atmosphere(CanonicalField::ColumnIntegratedSmoke);
        assert_eq!(
            smoke_column.key(),
            "column_integrated_smoke_entire_atmosphere"
        );
        assert_eq!(smoke_column.native_units(), "kg/m^2");
    }

    #[test]
    fn model_timestep_builds_requests_and_descriptors() {
        let timestep = ModelTimestep::with_source(
            ModelId::RrfsA,
            CycleSpec::new("20260414", 18).unwrap(),
            6,
            TimeStamp::new("2026-04-15T00:00:00Z").unwrap(),
            Some(SourceId::Aws),
        )
        .unwrap();

        let request = timestep.request("prs-conus").unwrap();
        assert_eq!(request.model, ModelId::RrfsA);
        assert_eq!(request.forecast_hour, 6);
        assert_eq!(request.product, "prs-conus");
        assert_eq!(timestep.descriptor().cycle.date_yyyymmdd, "20260414");
        assert_eq!(timestep.descriptor().cycle.hour_utc, 18);
        assert_eq!(
            timestep.descriptor().valid_time.as_str(),
            "2026-04-15T00:00:00Z"
        );
        assert_eq!(timestep.source, Some(SourceId::Aws));
    }

    #[test]
    fn canonical_bundle_descriptors_are_typed() {
        assert_eq!(
            CanonicalBundleDescriptor::SurfaceAnalysis.as_str(),
            "surface_analysis"
        );
        assert_eq!(
            CanonicalBundleDescriptor::SurfaceAnalysis.family(),
            CanonicalDataFamily::Surface
        );
        assert_eq!(
            CanonicalBundleDescriptor::PressureAnalysis.as_str(),
            "pressure_analysis"
        );
        assert_eq!(
            CanonicalBundleDescriptor::PressureAnalysis.family(),
            CanonicalDataFamily::Pressure
        );
        assert_eq!(
            CanonicalBundleDescriptor::NativeAnalysis.as_str(),
            "native_analysis"
        );
        assert_eq!(
            CanonicalBundleDescriptor::NativeAnalysis.family(),
            CanonicalDataFamily::Native
        );
    }

    #[test]
    fn canonical_bundle_id_dedupes_by_full_identity() {
        let cycle = CycleSpec::new("20260415", 18).unwrap();
        let surface = CanonicalBundleId::new(
            ModelId::Hrrr,
            cycle.clone(),
            6,
            SourceId::Aws,
            CanonicalBundleDescriptor::SurfaceAnalysis,
            "sfc",
        );
        let surface_clone = CanonicalBundleId::new(
            ModelId::Hrrr,
            cycle.clone(),
            6,
            SourceId::Aws,
            CanonicalBundleDescriptor::SurfaceAnalysis,
            "sfc",
        );
        let surface_other_hour = CanonicalBundleId::new(
            ModelId::Hrrr,
            cycle.clone(),
            7,
            SourceId::Aws,
            CanonicalBundleDescriptor::SurfaceAnalysis,
            "sfc",
        );
        assert_eq!(surface, surface_clone);
        assert_ne!(surface, surface_other_hour);
        assert_eq!(surface.family(), CanonicalDataFamily::Surface);
        assert!(
            surface
                .to_string()
                .contains("surface_analysis@hrrr:2026041518z+f006:sfc")
        );
    }

    #[test]
    fn bundle_requirement_carries_native_override() {
        let plain = BundleRequirement::new(CanonicalBundleDescriptor::PressureAnalysis, 12);
        assert!(plain.native_override.is_none());
        let overridden = plain.clone().with_native_override("prs-na");
        assert_eq!(overridden.native_override.as_deref(), Some("prs-na"));
        assert_ne!(plain, overridden);
    }

    #[test]
    fn resolved_url_prefers_idx_when_probing_availability() {
        let with_idx = ResolvedUrl {
            source: SourceId::Aws,
            grib_url: "https://example.test/file.grib2".to_string(),
            idx_url: Some("https://example.test/file.grib2.idx".to_string()),
        };
        assert_eq!(
            with_idx.availability_probe_url(),
            "https://example.test/file.grib2.idx"
        );

        let without_idx = ResolvedUrl {
            source: SourceId::Azure,
            grib_url: "https://example.test/file.grib2".to_string(),
            idx_url: None,
        };
        assert_eq!(
            without_idx.availability_probe_url(),
            "https://example.test/file.grib2"
        );
    }

    #[test]
    fn model_field_2d_round_trips_to_legacy_field() {
        let shape = GridShape::new(2, 2).unwrap();
        let grid = LatLonGrid::new(
            shape,
            vec![35.0, 35.0, 36.0, 36.0],
            vec![-99.0, -98.0, -99.0, -98.0],
        )
        .unwrap();
        let metadata = ModelFieldMetadata::new(
            ModelTimestep::new(
                ModelId::Hrrr,
                CycleSpec::new("20260414", 18).unwrap(),
                1,
                TimeStamp::new("2026-04-14T19:00:00Z").unwrap(),
            )
            .unwrap(),
            ProductKey::named("sbcape"),
            "J/kg",
        )
        .with_product_metadata(ProductKeyMetadata::new("Surface-Based CAPE"));

        let field =
            ModelField2D::new(metadata.clone(), grid.clone(), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        let legacy: Field2D = field.into();

        assert_eq!(legacy.product, metadata.product);
        assert_eq!(legacy.units, "J/kg");
        assert_eq!(legacy.grid, grid);
        assert_eq!(legacy.values, vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(
            metadata.product_metadata.unwrap().display_name,
            "Surface-Based CAPE"
        );
    }

    #[test]
    fn selected_field_2d_round_trips_to_legacy_field() {
        let shape = GridShape::new(2, 1).unwrap();
        let grid = LatLonGrid::new(shape, vec![35.0, 35.0], vec![-99.0, -98.0]).unwrap();
        let selector = FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500);

        let selected = SelectedField2D::new(selector, "gpm", grid.clone(), vec![5700.0, 5712.0])
            .unwrap()
            .with_projection(GridProjection::LambertConformal {
                standard_parallel_1_deg: 38.5,
                standard_parallel_2_deg: 38.5,
                central_meridian_deg: -97.5,
            });
        let legacy: Field2D = selected.into();

        assert_eq!(
            legacy.product.as_named(),
            Some("geopotential_height_500hpa")
        );
        assert_eq!(legacy.units, "gpm");
        assert_eq!(legacy.grid, grid);
        assert_eq!(legacy.values, vec![5700.0, 5712.0]);
    }

    #[test]
    fn selected_field_keeps_projection_metadata() {
        let shape = GridShape::new(2, 1).unwrap();
        let grid = LatLonGrid::new(shape, vec![35.0, 35.0], vec![-99.0, -98.0]).unwrap();
        let selector = FieldSelector::surface(CanonicalField::Temperature);

        let selected = SelectedField2D::new(selector, "K", grid, vec![290.0, 291.0])
            .unwrap()
            .with_projection(GridProjection::Mercator {
                latitude_of_true_scale_deg: 20.0,
                central_meridian_deg: -95.0,
            });

        assert_eq!(
            selected.projection,
            Some(GridProjection::Mercator {
                latitude_of_true_scale_deg: 20.0,
                central_meridian_deg: -95.0,
            })
        );
    }

    #[test]
    fn selected_hybrid_level_volume_tracks_levels_slices_and_projection() {
        let shape = GridShape::new(2, 1).unwrap();
        let grid = LatLonGrid::new(shape, vec![35.0, 35.0], vec![-99.0, -98.0]).unwrap();
        let volume = SelectedHybridLevelVolume::new(
            CanonicalField::SmokeMassDensity,
            vec![1, 2],
            "kg/m^3",
            grid,
            vec![1.0, 2.0, 3.0, 4.0],
        )
        .unwrap()
        .with_projection(GridProjection::LambertConformal {
            standard_parallel_1_deg: 38.5,
            standard_parallel_2_deg: 38.5,
            central_meridian_deg: -97.5,
        });

        assert_eq!(volume.level_count(), 2);
        assert_eq!(volume.level_slice(0), Some(&[1.0, 2.0][..]));
        assert_eq!(volume.level_slice(1), Some(&[3.0, 4.0][..]));
        assert_eq!(
            volume.selector_at(1),
            Some(FieldSelector::hybrid_level(
                CanonicalField::SmokeMassDensity,
                2
            ))
        );
        assert_eq!(
            volume.projection,
            Some(GridProjection::LambertConformal {
                standard_parallel_1_deg: 38.5,
                standard_parallel_2_deg: 38.5,
                central_meridian_deg: -97.5,
            })
        );
    }

    #[test]
    fn grid_projection_bincode_round_trip_works() {
        let projection = GridProjection::PolarStereographic {
            true_latitude_deg: 60.0,
            central_meridian_deg: -105.0,
            south_pole_on_projection_plane: false,
        };

        let bytes = bincode::serialize(&projection).unwrap();
        let round_trip = bincode::deserialize::<GridProjection>(&bytes).unwrap();

        assert_eq!(round_trip, projection);
    }

    #[test]
    fn selector_product_metadata_carries_typed_provenance() {
        let selector = FieldSelector::isobaric(CanonicalField::Temperature, 500);
        let metadata = selector.product_metadata();

        assert_eq!(metadata.display_name, "Temperature (500hpa)");
        assert_eq!(metadata.native_units.as_deref(), Some("K"));
        assert_eq!(
            metadata
                .identity
                .as_ref()
                .expect("selector metadata should expose canonical identity")
                .canonical,
            ProductId::new(ProductKind::Direct, "temperature_500hpa")
        );
        let provenance = metadata
            .provenance
            .as_ref()
            .expect("selector metadata should carry provenance");
        assert_eq!(provenance.lineage, ProductLineage::Direct);
        assert_eq!(provenance.maturity, ProductMaturity::Operational);
        assert_eq!(provenance.selector, Some(selector));
        assert!(provenance.flags.is_empty());
        assert!(provenance.window.is_none());
    }

    #[test]
    fn product_key_metadata_builder_keeps_additive_provenance_fields() {
        let metadata = ProductKeyMetadata::new("Run-Max UH")
            .with_description("Trailing native hourly 2-5 km updraft-helicity maxima")
            .with_category("windowed")
            .with_native_units("m^2/s^2")
            .with_identity(
                CanonicalProductIdentity::new(ProductId::new(
                    ProductKind::Windowed,
                    "uh_2to5km_run_max",
                ))
                .with_alias_slug("run_max_uh_2to5km"),
            )
            .with_provenance(
                ProductProvenance::new(ProductLineage::Windowed, ProductMaturity::Operational)
                    .with_flag(ProductSemanticFlag::Composite)
                    .with_window(ProductWindowSpec::accumulation(Some(3))),
            );

        assert_eq!(metadata.category.as_deref(), Some("windowed"));
        let identity = metadata
            .identity
            .clone()
            .expect("builder should keep canonical identity");
        assert_eq!(
            identity.canonical,
            ProductId::new(ProductKind::Windowed, "uh_2to5km_run_max")
        );
        assert!(
            identity
                .alias_slugs
                .contains(&"run_max_uh_2to5km".to_string())
        );
        let provenance = metadata.provenance.expect("builder should keep provenance");
        assert_eq!(provenance.lineage, ProductLineage::Windowed);
        assert!(provenance.flags.contains(&ProductSemanticFlag::Composite));
        assert_eq!(
            provenance.window,
            Some(ProductWindowSpec::accumulation(Some(3)))
        );
    }

    #[test]
    fn pressure_level_volume_exposes_level_slices() {
        let shape = GridShape::new(2, 2).unwrap();
        let grid = LatLonGrid::new(
            shape,
            vec![35.0, 35.0, 36.0, 36.0],
            vec![-99.0, -98.0, -99.0, -98.0],
        )
        .unwrap();
        let metadata = ModelFieldMetadata::new(
            ModelTimestep::new(
                ModelId::Gfs,
                CycleSpec::new("20260414", 12).unwrap(),
                9,
                TimeStamp::new("2026-04-14T21:00:00Z").unwrap(),
            )
            .unwrap(),
            ProductKey::named("temperature"),
            "degC",
        );

        let volume = PressureLevelVolume::new(
            metadata.clone(),
            vec![850.0, 700.0],
            grid.clone(),
            vec![1.0, 2.0, 3.0, 4.0, -5.0, -4.0, -3.0, -2.0],
        )
        .unwrap();

        assert_eq!(volume.level_count(), 2);
        assert_eq!(volume.level_slice(0), Some(&[1.0, 2.0, 3.0, 4.0][..]));
        assert_eq!(volume.level_slice(1), Some(&[-5.0, -4.0, -3.0, -2.0][..]));

        let legacy: Field3D = volume.into();
        assert_eq!(legacy.product, metadata.product);
        assert_eq!(legacy.units, "degC");
        assert_eq!(legacy.levels, vec![850.0, 700.0]);
        assert_eq!(legacy.grid, grid);
    }

    #[test]
    fn pressure_level_volume_validates_levels_and_lengths() {
        let shape = GridShape::new(2, 1).unwrap();
        let grid = LatLonGrid::new(shape, vec![35.0, 35.0], vec![-99.0, -98.0]).unwrap();
        let metadata = ModelFieldMetadata::new(
            ModelTimestep::new(
                ModelId::EcmwfOpenData,
                CycleSpec::new("20260414", 0).unwrap(),
                12,
                TimeStamp::new("2026-04-14T12:00:00Z").unwrap(),
            )
            .unwrap(),
            ProductKey::named("rh"),
            "%",
        );

        assert!(matches!(
            PressureLevelVolume::new(metadata.clone(), Vec::new(), grid.clone(), vec![1.0, 2.0]),
            Err(RustwxError::EmptyPressureLevels)
        ));
        assert!(matches!(
            PressureLevelVolume::new(
                metadata.clone(),
                vec![850.0, -700.0],
                grid.clone(),
                vec![1.0, 2.0, 3.0, 4.0],
            ),
            Err(RustwxError::InvalidPressureLevel {
                index: 1,
                value: -700.0
            })
        ));
        assert!(matches!(
            PressureLevelVolume::new(metadata, vec![850.0, 700.0], grid, vec![1.0, 2.0, 3.0]),
            Err(RustwxError::InvalidFieldDataLength {
                expected: 4,
                actual: 3
            })
        ));
    }

    #[test]
    fn hybrid_level_volume_validates_levels_and_lengths() {
        let shape = GridShape::new(2, 1).unwrap();
        let grid = LatLonGrid::new(shape, vec![35.0, 35.0], vec![-99.0, -98.0]).unwrap();

        assert!(matches!(
            SelectedHybridLevelVolume::new(
                CanonicalField::SmokeMassDensity,
                Vec::new(),
                "kg/m^3",
                grid.clone(),
                vec![1.0, 2.0],
            ),
            Err(RustwxError::EmptyHybridLevels)
        ));
        assert!(matches!(
            SelectedHybridLevelVolume::new(
                CanonicalField::SmokeMassDensity,
                vec![1, 0],
                "kg/m^3",
                grid.clone(),
                vec![1.0, 2.0, 3.0, 4.0],
            ),
            Err(RustwxError::InvalidHybridLevel { index: 1, value: 0 })
        ));
        assert!(matches!(
            SelectedHybridLevelVolume::new(
                CanonicalField::Pressure,
                vec![1, 2],
                "Pa",
                grid,
                vec![1.0, 2.0, 3.0],
            ),
            Err(RustwxError::InvalidFieldDataLength {
                expected: 4,
                actual: 3
            })
        ));
    }

    #[test]
    fn field_point_sampling_uses_nearest_and_inverse_distance_modes() {
        let grid = LatLonGrid::new(
            GridShape::new(2, 2).unwrap(),
            vec![0.0, 0.0, 1.0, 1.0],
            vec![0.0, 1.0, 0.0, 1.0],
        )
        .unwrap();
        let field = Field2D::new(
            ProductKey::named("sample"),
            "unitless",
            grid,
            vec![0.0, 10.0, 20.0, 30.0],
        )
        .unwrap();

        let nearest =
            field.sample_point(GeoPoint::new(0.95, 0.95), FieldPointSampleMethod::Nearest);
        assert_eq!(nearest.value, Some(30.0));
        assert_eq!(nearest.contributors.len(), 1);
        assert_eq!(nearest.contributors[0].grid_index, 3);

        let blended = field.sample_point(
            GeoPoint::new(0.5, 0.5),
            FieldPointSampleMethod::InverseDistance4,
        );
        assert_eq!(blended.contributors.len(), 4);
        assert!((blended.value.expect("blended value") - 15.0).abs() < 1.0e-5);
        let total_weight = blended
            .contributors
            .iter()
            .map(|entry| entry.weight)
            .sum::<f64>();
        assert!((total_weight - 1.0).abs() < 1.0e-9);
    }

    #[test]
    fn field_polygon_summary_counts_finite_cells_inside_polygon() {
        let grid = LatLonGrid::new(
            GridShape::new(3, 2).unwrap(),
            vec![0.0, 0.0, 0.0, 1.0, 1.0, 1.0],
            vec![0.0, 1.0, 2.0, 0.0, 1.0, 2.0],
        )
        .unwrap();
        let field = Field2D::new(
            ProductKey::named("sample"),
            "unitless",
            grid,
            vec![1.0, 2.0, f32::NAN, 4.0, 5.0, 6.0],
        )
        .unwrap();
        let polygon = GeoPolygon::new(
            vec![
                GeoPoint::new(-0.5, -0.5),
                GeoPoint::new(-0.5, 1.5),
                GeoPoint::new(1.5, 1.5),
                GeoPoint::new(1.5, -0.5),
            ],
            Vec::new(),
        );

        let summary = field.summarize_polygon(&polygon);
        assert_eq!(summary.included_cell_count, 4);
        assert_eq!(summary.valid_cell_count, 4);
        assert_eq!(summary.missing_cell_count, 0);
        assert_eq!(summary.min, Some(1.0));
        assert_eq!(summary.max, Some(5.0));
        assert_eq!(summary.mean, Some(3.0));
    }

    #[test]
    fn polygon_holes_exclude_cells_from_area_summary() {
        let grid = LatLonGrid::new(
            GridShape::new(3, 1).unwrap(),
            vec![0.0, 0.0, 0.0],
            vec![0.0, 1.0, 2.0],
        )
        .unwrap();
        let field = Field2D::new(
            ProductKey::named("sample"),
            "unitless",
            grid,
            vec![10.0, 20.0, 30.0],
        )
        .unwrap();
        let polygon = GeoPolygon::new(
            vec![
                GeoPoint::new(-1.0, -1.0),
                GeoPoint::new(-1.0, 3.0),
                GeoPoint::new(1.0, 3.0),
                GeoPoint::new(1.0, -1.0),
            ],
            vec![vec![
                GeoPoint::new(-0.5, 0.5),
                GeoPoint::new(-0.5, 1.5),
                GeoPoint::new(0.5, 1.5),
                GeoPoint::new(0.5, 0.5),
            ]],
        );

        let summary = field.summarize_polygon(&polygon);
        assert_eq!(summary.included_cell_count, 2);
        assert_eq!(summary.valid_cell_count, 2);
        assert_eq!(summary.mean, Some(20.0));
    }
}
