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
    #[error("invalid forecast hour {0}")]
    InvalidForecastHour(u16),
    #[error("pressure-level volume requires at least one level")]
    EmptyPressureLevels,
    #[error("invalid pressure level at index {index}: {value}")]
    InvalidPressureLevel { index: usize, value: f32 },
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeStamp {
    pub iso8601_utc: String,
}

impl TimeStamp {
    pub fn new<S: Into<String>>(iso8601_utc: S) -> Self {
        Self {
            iso8601_utc: iso8601_utc.into(),
        }
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
    GeopotentialHeight,
    Temperature,
    RelativeHumidity,
    Dewpoint,
    AbsoluteVorticity,
    RelativeVorticity,
    UWind,
    VWind,
    LandSeaMask,
    CompositeReflectivity,
    UpdraftHelicity,
}

impl CanonicalField {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GeopotentialHeight => "geopotential_height",
            Self::Temperature => "temperature",
            Self::RelativeHumidity => "relative_humidity",
            Self::Dewpoint => "dewpoint",
            Self::AbsoluteVorticity => "absolute_vorticity",
            Self::RelativeVorticity => "relative_vorticity",
            Self::UWind => "u_wind",
            Self::VWind => "v_wind",
            Self::LandSeaMask => "land_sea_mask",
            Self::CompositeReflectivity => "composite_reflectivity",
            Self::UpdraftHelicity => "updraft_helicity",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::GeopotentialHeight => "Geopotential Height",
            Self::Temperature => "Temperature",
            Self::RelativeHumidity => "Relative Humidity",
            Self::Dewpoint => "Dewpoint",
            Self::AbsoluteVorticity => "Absolute Vorticity",
            Self::RelativeVorticity => "Relative Vorticity",
            Self::UWind => "U Wind",
            Self::VWind => "V Wind",
            Self::LandSeaMask => "Land-Sea Mask",
            Self::CompositeReflectivity => "Composite Reflectivity",
            Self::UpdraftHelicity => "Updraft Helicity",
        }
    }

    pub fn native_units(self) -> &'static str {
        match self {
            Self::GeopotentialHeight => "gpm",
            Self::Temperature => "K",
            Self::RelativeHumidity => "%",
            Self::Dewpoint => "K",
            Self::AbsoluteVorticity | Self::RelativeVorticity => "s^-1",
            Self::UWind | Self::VWind => "m/s",
            Self::LandSeaMask => "fraction",
            Self::CompositeReflectivity => "dBZ",
            Self::UpdraftHelicity => "m^2/s^2",
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
    IsobaricHpa(u16),
    EntireAtmosphere,
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
            Self::IsobaricHpa(level_hpa) => format!("{level_hpa}hpa"),
            Self::EntireAtmosphere => "entire_atmosphere".to_string(),
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
            Self::IsobaricHpa(level_hpa) => write!(f, "{level_hpa}hpa"),
            Self::EntireAtmosphere => f.write_str("entire_atmosphere"),
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

    pub const fn surface(field: CanonicalField) -> Self {
        Self::new(field, VerticalSelector::Surface)
    }

    pub const fn entire_atmosphere(field: CanonicalField) -> Self {
        Self::new(field, VerticalSelector::EntireAtmosphere)
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectedField2D {
    pub selector: FieldSelector,
    pub units: String,
    pub grid: LatLonGrid,
    pub values: Vec<f32>,
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
        })
    }

    pub fn into_field2d(self) -> Field2D {
        Field2D {
            product: self.selector.product_key(),
            units: self.units,
            grid: self.grid,
            values: self.values,
        }
    }
}

impl From<SelectedField2D> for Field2D {
    fn from(value: SelectedField2D) -> Self {
        value.into_field2d()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductKeyMetadata {
    pub display_name: String,
    pub description: Option<String>,
    pub native_units: Option<String>,
    pub category: Option<String>,
}

impl ProductKeyMetadata {
    pub fn new<S: Into<String>>(display_name: S) -> Self {
        Self {
            display_name: display_name.into(),
            description: None,
            native_units: None,
            category: None,
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ModelId {
    Hrrr,
    Gfs,
    EcmwfOpenData,
    RrfsA,
}

impl ModelId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hrrr => "hrrr",
            Self::Gfs => "gfs",
            Self::EcmwfOpenData => "ecmwf-open-data",
            Self::RrfsA => "rrfs-a",
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
            other => Err(RustwxError::UnknownModel(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceId {
    Aws,
    Nomads,
    Google,
    Azure,
    Ecmwf,
    Ncei,
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
            other => Err(RustwxError::UnknownSource(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub cycle: TimeStamp,
    pub forecast_hour: u16,
}

impl ForecastDescriptor {
    pub fn new<S: Into<String>>(model: S, cycle: TimeStamp, forecast_hour: u16) -> Self {
        Self {
            model: model.into(),
            cycle,
            forecast_hour,
        }
    }
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
        assert_eq!(ModelId::Hrrr.to_string(), "hrrr");
    }

    #[test]
    fn cycle_spec_validates_inputs() {
        assert!(CycleSpec::new("20260414", 20).is_ok());
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
    fn field_selector_builds_keys_and_units() {
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

        let lsm = FieldSelector::surface(CanonicalField::LandSeaMask);
        assert_eq!(lsm.key(), "land_sea_mask_surface");
        assert_eq!(lsm.native_units(), "fraction");

        let uh = FieldSelector::height_layer_agl(CanonicalField::UpdraftHelicity, 2000, 5000);
        assert_eq!(uh.key(), "updraft_helicity_2000m_to_5000m_agl");
    }

    #[test]
    fn model_timestep_builds_requests_and_descriptors() {
        let timestep = ModelTimestep::with_source(
            ModelId::RrfsA,
            CycleSpec::new("20260414", 18).unwrap(),
            6,
            TimeStamp::new("2026-04-15T00:00:00Z"),
            Some(SourceId::Aws),
        )
        .unwrap();

        let request = timestep.request("prs-conus").unwrap();
        assert_eq!(request.model, ModelId::RrfsA);
        assert_eq!(request.forecast_hour, 6);
        assert_eq!(request.product, "prs-conus");
        assert_eq!(timestep.descriptor().cycle.as_str(), "2026-04-15T00:00:00Z");
        assert_eq!(timestep.source, Some(SourceId::Aws));
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
                TimeStamp::new("2026-04-14T19:00:00Z"),
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

        let selected =
            SelectedField2D::new(selector, "gpm", grid.clone(), vec![5700.0, 5712.0]).unwrap();
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
                TimeStamp::new("2026-04-14T21:00:00Z"),
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
                TimeStamp::new("2026-04-14T12:00:00Z"),
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
}
