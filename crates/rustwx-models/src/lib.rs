use rustwx_core::{
    CanonicalBundleDescriptor, CanonicalDataFamily, CanonicalField, CycleSpec, FieldProduct,
    FieldSelector, ModelId, ModelRunRequest, ProbabilitySelection, ProductKeyMetadata,
    ProductLineage, ProductMaturity, ProductProvenance, ProductSemanticFlag, ProductWindowSpec,
    ResolvedUrl, RustwxError, SourceId, StatisticalProcess, VerticalSelector,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProductFamily {
    Surface,
    Pressure,
    Native,
    Subhourly,
}

/// Broad runtime family used by the model-compatibility layer. The
/// renderer still consumes canonical selectors, but this tells future
/// model integrations which adapter style to implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRuntimeFamily {
    Grib2Forecast,
    LocalNetcdfForecast,
    WrfNetcdfArchive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnsembleMode {
    Deterministic,
    MemberGribFiles,
    MemberDimensionNetcdf,
}

impl ProductFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Pressure => "pressure",
            Self::Native => "native",
            Self::Subhourly => "subhourly",
        }
    }

    pub fn default_lineage(self) -> ProductLineage {
        match self {
            Self::Surface | Self::Pressure | Self::Native => ProductLineage::Direct,
            Self::Subhourly => ProductLineage::Windowed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum GribLevelKind {
    Surface,
    MeanSeaLevel,
    HeightAboveGround,
    HeightAboveGroundLayer,
    IsobaricHpa,
    EntireAtmosphere,
    NominalTop,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RenderStyle {
    WeatherCape,
    WeatherCin,
    WeatherReflectivity,
    WeatherUh,
    WeatherTemperature,
    WeatherDewpoint,
    WeatherRh,
    WeatherProbability,
    WeatherWinds,
    WeatherHeight,
    WeatherPressure,
    WeatherWindGust,
    WeatherCloudCover,
    WeatherPrecipitableWater,
    WeatherQpf,
    WeatherCategorical,
    WeatherVisibility,
    WeatherRadarReflectivity,
    WeatherSatellite,
    WeatherLightning,
    WeatherVorticity,
    WeatherStp,
    WeatherScp,
    WeatherEhi,
}

fn recipe_lineage(slug: &str, family: ProductFamily) -> ProductLineage {
    match slug {
        "2m_theta_e_10m_winds" | "2m_heat_index" | "2m_wind_chill" => ProductLineage::Derived,
        "1h_qpf" => ProductLineage::Windowed,
        _ => family.default_lineage(),
    }
}

fn recipe_maturity(slug: &str) -> ProductMaturity {
    if slug.starts_with("nbm_qmd_") {
        return ProductMaturity::Experimental;
    }
    if slug.starts_with("sref_prob_") {
        return ProductMaturity::Experimental;
    }
    if slug.starts_with("aigefs_spr_") {
        return ProductMaturity::Experimental;
    }
    if slug.starts_with("hgefs_spr_") {
        return ProductMaturity::Experimental;
    }
    if slug.starts_with("href_sprd_") {
        return ProductMaturity::Experimental;
    }
    if slug.starts_with("gefs_avg_") || slug.starts_with("gefs_spr_") {
        return ProductMaturity::Experimental;
    }
    match slug {
        "simulated_ir_satellite" | "lightning_flash_density" => ProductMaturity::Experimental,
        _ => ProductMaturity::Operational,
    }
}

fn recipe_flags(slug: &str) -> Vec<ProductSemanticFlag> {
    if slug.starts_with("nbm_qmd_") {
        return vec![ProductSemanticFlag::ProofOriented];
    }
    if slug.starts_with("sref_prob_") {
        return vec![ProductSemanticFlag::ProofOriented];
    }
    if slug.starts_with("aigefs_spr_") {
        return vec![ProductSemanticFlag::ProofOriented];
    }
    if slug.starts_with("hgefs_spr_") {
        return vec![ProductSemanticFlag::ProofOriented];
    }
    if slug.starts_with("href_sprd_") {
        return vec![ProductSemanticFlag::ProofOriented];
    }
    if slug.starts_with("gefs_avg_") || slug.starts_with("gefs_spr_") {
        return vec![ProductSemanticFlag::ProofOriented];
    }
    match slug {
        "cloud_cover_levels" | "precipitation_type" | "composite_reflectivity_uh" => {
            vec![ProductSemanticFlag::Composite]
        }
        "1h_qpf" => vec![ProductSemanticFlag::Alias],
        _ => Vec::new(),
    }
}

fn recipe_window(slug: &str, lineage: ProductLineage) -> Option<ProductWindowSpec> {
    match slug {
        "1h_qpf" => Some(ProductWindowSpec::accumulation(Some(1))),
        _ if matches!(lineage, ProductLineage::Windowed) => {
            Some(ProductWindowSpec::accumulation(None))
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GribFieldSpec {
    pub key: &'static str,
    pub label: &'static str,
    pub family: ProductFamily,
    pub level_kind: GribLevelKind,
    pub level_value: Option<i32>,
    pub selector: Option<FieldSelector>,
    pub idx_fallback_patterns: &'static [&'static str],
}

impl GribFieldSpec {
    pub fn idx_patterns(&self) -> &'static [&'static str] {
        self.idx_fallback_patterns
    }

    pub fn provenance(&self) -> ProductProvenance {
        let mut provenance =
            ProductProvenance::new(self.family.default_lineage(), ProductMaturity::Operational);
        if let Some(selector) = self.selector {
            provenance = provenance.with_selector(selector);
        }
        if self.family == ProductFamily::Subhourly {
            provenance = provenance.with_window(ProductWindowSpec {
                process: StatisticalProcess::Accumulation,
                duration_hours: None,
            });
        }
        provenance
    }

    pub fn product_metadata(&self) -> ProductKeyMetadata {
        let mut metadata = ProductKeyMetadata::new(self.label).with_category(self.family.as_str());
        if let Some(selector) = self.selector {
            metadata = metadata.with_native_units(selector.native_units());
        }
        metadata.with_provenance(self.provenance())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlotRecipe {
    pub slug: &'static str,
    pub title: &'static str,
    pub filled: GribFieldSpec,
    pub contours: Option<GribFieldSpec>,
    pub barbs_u: Option<GribFieldSpec>,
    pub barbs_v: Option<GribFieldSpec>,
    pub style: RenderStyle,
}

impl PlotRecipe {
    pub fn provenance(&self) -> ProductProvenance {
        let lineage = recipe_lineage(self.slug, self.filled.family);
        let mut provenance = ProductProvenance::new(lineage, recipe_maturity(self.slug));
        if let Some(selector) = self.filled.selector {
            provenance = provenance.with_selector(selector);
        }
        if let Some(window) = recipe_window(self.slug, lineage) {
            provenance = provenance.with_window(window);
        }
        for flag in recipe_flags(self.slug) {
            provenance = provenance.with_flag(flag);
        }
        provenance
    }

    pub fn product_metadata(&self) -> ProductKeyMetadata {
        let mut metadata = ProductKeyMetadata::new(self.title)
            .with_category(self.filled.family.as_str())
            .with_provenance(self.provenance());
        if let Some(selector) = self.filled.selector {
            metadata = metadata.with_native_units(selector.native_units());
        }
        metadata
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlotRecipeFetchMode {
    IndexedSubset,
    WholeFileStructuredExtract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlotRecipeFetchPolicy {
    PreferIndexedSubset,
    WholeFile,
}

impl PlotRecipeFetchPolicy {
    pub fn fetch_mode(self) -> PlotRecipeFetchMode {
        match self {
            Self::PreferIndexedSubset => PlotRecipeFetchMode::IndexedSubset,
            Self::WholeFile => PlotRecipeFetchMode::WholeFileStructuredExtract,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlotRecipeBlocker {
    pub field_key: &'static str,
    pub field_label: &'static str,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlotRecipeFetchPlan {
    pub recipe_slug: &'static str,
    pub model: ModelId,
    pub product: &'static str,
    pub fetch_policy: PlotRecipeFetchPolicy,
    pub fetch_mode: PlotRecipeFetchMode,
    pub fields: Vec<&'static GribFieldSpec>,
}

impl PlotRecipeFetchPlan {
    pub fn idx_patterns(&self) -> Vec<&'static str> {
        dedupe_patterns(
            self.fields
                .iter()
                .flat_map(|field| field.idx_patterns().iter().copied()),
        )
    }

    pub fn selectors(&self) -> Vec<FieldSelector> {
        self.fields
            .iter()
            .map(|field| {
                field
                    .selector
                    .expect("plot recipe fetch plan only returns selector-backed fields")
            })
            .collect()
    }

    pub fn variable_patterns(&self) -> Vec<&'static str> {
        match self.fetch_mode {
            PlotRecipeFetchMode::IndexedSubset => self.idx_patterns(),
            PlotRecipeFetchMode::WholeFileStructuredExtract => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceDescriptor {
    pub id: SourceId,
    pub idx_available: bool,
    pub priority: u8,
    pub max_age_hours: Option<u32>,
    pub notes: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelSummary {
    pub id: ModelId,
    pub description: &'static str,
    pub default_product: &'static str,
    pub cycle_hours_utc: &'static [u8],
    pub max_forecast_hour: u16,
    pub sources: &'static [SourceDescriptor],
    pub runtime_family: ModelRuntimeFamily,
    pub ensemble_mode: EnsembleMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestRun {
    pub model: ModelId,
    pub cycle: CycleSpec,
    pub source: SourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedCanonicalBundleProduct {
    pub bundle: CanonicalBundleDescriptor,
    pub family: CanonicalDataFamily,
    pub native_product: String,
}

impl ResolvedCanonicalBundleProduct {
    pub fn new<S: Into<String>>(bundle: CanonicalBundleDescriptor, native_product: S) -> Self {
        Self {
            bundle,
            family: bundle.family(),
            native_product: native_product.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error(transparent)]
    Core(#[from] RustwxError),
    #[error("unsupported product '{product}' for model '{model}'")]
    UnsupportedProduct { model: ModelId, product: String },
    #[error(
        "unsupported forecast hour '{forecast_hour}' for model '{model}' cycle '{cycle_hour:02}z': {reason}"
    )]
    UnsupportedForecastHour {
        model: ModelId,
        cycle_hour: u8,
        forecast_hour: u16,
        reason: String,
    },
    #[error("unknown plot recipe '{slug}'")]
    UnknownPlotRecipe { slug: String },
    #[error("plot recipe '{recipe}' is not supported for model '{model}': {reason}")]
    UnsupportedPlotRecipeModel {
        recipe: &'static str,
        model: ModelId,
        reason: String,
    },
    #[error("no working source found for model '{model}' while probing latest availability")]
    NoAvailableRun { model: ModelId },
}

const HRRR_CYCLE_HOURS: &[u8] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];
const GFS_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const GDAS_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const GEFS_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const AI_MODEL_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const ECMWF_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const AIFS_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const AIFS_LOCAL_MAX_FORECAST_HOUR: u16 = 43_848;
const RAP_CYCLE_HOURS: &[u8] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];
const NAM_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const HIRESW_CYCLE_HOURS: &[u8] = &[0, 12];
const HREF_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const SREF_CYCLE_HOURS: &[u8] = &[3, 9, 15, 21];
const HOURLY_ANALYSIS_CYCLE_HOURS: &[u8] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];
const RRFS_A_CYCLE_HOURS: &[u8] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];
const RRFS_PUBLIC_CYCLE_HOURS: &[u8] = &[0, 3, 6, 9, 12, 15, 18, 21];
const RRFS_FIREWX_CYCLE_HOURS: &[u8] = &[0, 6, 12, 18];
const WRF_GDEX_CYCLE_HOURS: &[u8] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];
const WRF_GDEX_DEFAULT_SURFACE_PRODUCT: &str = "d612005-hist2d";
const WRF_GDEX_DEFAULT_PRESSURE_PRODUCT: &str = "d612005-hist3d";

const HRRR_SOURCES: &[SourceDescriptor] = &[
    SourceDescriptor {
        id: SourceId::Nomads,
        idx_available: true,
        priority: 1,
        max_age_hours: Some(48),
        notes: "Operational NOMADS feed",
    },
    SourceDescriptor {
        id: SourceId::Aws,
        idx_available: true,
        priority: 2,
        max_age_hours: None,
        notes: "AWS open data archive",
    },
    SourceDescriptor {
        id: SourceId::Google,
        idx_available: true,
        priority: 3,
        max_age_hours: None,
        notes: "Google mirror",
    },
    SourceDescriptor {
        id: SourceId::Azure,
        idx_available: false,
        priority: 4,
        max_age_hours: None,
        notes: "Azure mirror without .idx coverage",
    },
];

const NOMADS_ONLY_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Nomads,
    idx_available: true,
    priority: 1,
    max_age_hours: Some(72),
    notes: "Operational NOMADS GRIB2 feed",
}];

const NOMADS_AWS_SOURCES: &[SourceDescriptor] = &[
    SourceDescriptor {
        id: SourceId::Nomads,
        idx_available: true,
        priority: 1,
        max_age_hours: Some(72),
        notes: "Operational NOMADS GRIB2 feed",
    },
    SourceDescriptor {
        id: SourceId::Aws,
        idx_available: true,
        priority: 2,
        max_age_hours: None,
        notes: "NOAA public S3 open-data archive",
    },
];

const GFS_SOURCES: &[SourceDescriptor] = &[
    SourceDescriptor {
        id: SourceId::Nomads,
        idx_available: true,
        priority: 1,
        max_age_hours: Some(48),
        notes: "Operational NOMADS feed",
    },
    SourceDescriptor {
        id: SourceId::Aws,
        idx_available: true,
        priority: 2,
        max_age_hours: None,
        notes: "AWS open data archive",
    },
    SourceDescriptor {
        id: SourceId::Google,
        idx_available: true,
        priority: 3,
        max_age_hours: None,
        notes: "Google mirror",
    },
    SourceDescriptor {
        id: SourceId::Ncei,
        idx_available: false,
        priority: 4,
        max_age_hours: None,
        notes: "Historical NCEI archive",
    },
];

const GEFS_SOURCES: &[SourceDescriptor] = &[
    SourceDescriptor {
        id: SourceId::Nomads,
        idx_available: true,
        priority: 1,
        max_age_hours: Some(48),
        notes: "Operational NOAA GEFS feed",
    },
    SourceDescriptor {
        id: SourceId::Aws,
        idx_available: true,
        priority: 2,
        max_age_hours: None,
        notes: "NOAA GEFS AWS open-data archive",
    },
];

const ECMWF_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Ecmwf,
    // ECMWF open-data doesn't publish `.grib2.idx` companion files, so
    // HEAD-probing the `.idx` URL returns 404 and `latest_available_run`
    // falsely concludes the run is unavailable. Flagging this source
    // `idx_available=false` makes `availability_probe_url` fall back to
    // the grib URL itself, which HEAD 200s correctly. The tradeoff: no
    // idx-based range extraction, but ECMWF's open-data is served as
    // single grib2 files that the fetcher already pulls whole.
    idx_available: false,
    priority: 1,
    max_age_hours: None,
    notes: "ECMWF open data",
}];

const AIFS_SOURCES: &[SourceDescriptor] = &[
    SourceDescriptor {
        id: SourceId::Earth2Archive,
        idx_available: false,
        priority: 1,
        max_age_hours: None,
        notes: "Local NetCDF archive populated by Earth2Studio or another inference harness",
    },
    SourceDescriptor {
        id: SourceId::Ecmwf,
        idx_available: false,
        priority: 2,
        max_age_hours: None,
        notes: "ECMWF open data AIFS-Single GRIB2 feed",
    },
];

const RRFS_A_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Aws,
    idx_available: true,
    priority: 1,
    max_age_hours: None,
    notes: "NOAA RRFS AWS bucket",
}];

const RRFS_PUBLIC_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Aws,
    idx_available: true,
    priority: 1,
    max_age_hours: None,
    notes: "NOAA RRFS public prototype AWS bucket",
}];

const RRFS_FIREWX_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Aws,
    idx_available: true,
    priority: 1,
    max_age_hours: None,
    notes: "NOAA RRFS public fire-weather nest AWS bucket",
}];

const WRF_GDEX_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Gdex,
    idx_available: false,
    priority: 1,
    max_age_hours: None,
    notes: "UCAR GDEX THREDDS fileServer",
}];

const MODELS: &[ModelSummary] = &[
    ModelSummary {
        id: ModelId::Hrrr,
        description: "HRRR 3 km CONUS rapid-refresh forecast",
        default_product: "sfc",
        cycle_hours_utc: HRRR_CYCLE_HOURS,
        max_forecast_hour: 48,
        sources: HRRR_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::HrrrAk,
        description: "HRRR Alaska rapid-refresh forecast",
        default_product: "sfc",
        cycle_hours_utc: HRRR_CYCLE_HOURS,
        max_forecast_hour: 48,
        sources: HRRR_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Gfs,
        description: "GFS global 0.25 degree atmospheric grid",
        default_product: "pgrb2.0p25",
        cycle_hours_utc: GFS_CYCLE_HOURS,
        max_forecast_hour: 384,
        sources: GFS_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Gdas,
        description: "GDAS global data-assimilation analysis/forecast grids",
        default_product: "pgrb2.0p25",
        cycle_hours_utc: GDAS_CYCLE_HOURS,
        max_forecast_hour: 9,
        sources: NOMADS_AWS_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Gefs,
        description: "GEFS global 0.5 degree ensemble forecast",
        default_product: "pgrb2ap5/gec00",
        cycle_hours_utc: GEFS_CYCLE_HOURS,
        max_forecast_hour: 384,
        sources: GEFS_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::MemberGribFiles,
    },
    ModelSummary {
        id: ModelId::Aigfs,
        description: "NOAA AI-GFS global data-driven forecast",
        default_product: "sfc",
        cycle_hours_utc: AI_MODEL_CYCLE_HOURS,
        max_forecast_hour: 384,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Aigefs,
        description: "NOAA AI-GEFS ensemble-stat global data-driven forecast",
        default_product: "sfc/avg",
        cycle_hours_utc: AI_MODEL_CYCLE_HOURS,
        max_forecast_hour: 384,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::MemberGribFiles,
    },
    ModelSummary {
        id: ModelId::Hgefs,
        description: "NOAA HGEFS hybrid AI/GEFS ensemble-stat global forecast",
        default_product: "sfc/avg",
        cycle_hours_utc: AI_MODEL_CYCLE_HOURS,
        max_forecast_hour: 240,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::MemberGribFiles,
    },
    ModelSummary {
        id: ModelId::EcmwfOpenData,
        description: "ECMWF open data IFS 0.25 degree feed",
        default_product: "oper",
        cycle_hours_utc: ECMWF_CYCLE_HOURS,
        max_forecast_hour: 360,
        sources: ECMWF_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Aifs,
        description: "AIFS-Single / AIFSENS data-driven global forecast",
        default_product: "oper",
        cycle_hours_utc: AIFS_CYCLE_HOURS,
        max_forecast_hour: AIFS_LOCAL_MAX_FORECAST_HOUR,
        sources: AIFS_SOURCES,
        runtime_family: ModelRuntimeFamily::LocalNetcdfForecast,
        ensemble_mode: EnsembleMode::MemberDimensionNetcdf,
    },
    ModelSummary {
        id: ModelId::Rap,
        description: "RAP hourly North America forecast grids",
        default_product: "awp130pgrb",
        cycle_hours_utc: RAP_CYCLE_HOURS,
        max_forecast_hour: 51,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Nam,
        description: "NAM CONUS/North America/nest regional forecast grids",
        default_product: "awip12",
        cycle_hours_utc: NAM_CYCLE_HOURS,
        max_forecast_hour: 84,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Hiresw,
        description: "HIRESW high-resolution regional ARW/FV3 forecast grids",
        default_product: "arw_2p5km/conus",
        cycle_hours_utc: HIRESW_CYCLE_HOURS,
        max_forecast_hour: 48,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::MemberGribFiles,
    },
    ModelSummary {
        id: ModelId::Href,
        description: "HREF CONUS ensemble products",
        default_product: "ensprod/conus/sprd",
        cycle_hours_utc: HREF_CYCLE_HOURS,
        max_forecast_hour: 48,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::MemberGribFiles,
    },
    ModelSummary {
        id: ModelId::Sref,
        description: "SREF regional ensemble member/statistic forecast grids",
        default_product: "ensprod/pgrb212/mean_3hrly",
        cycle_hours_utc: SREF_CYCLE_HOURS,
        max_forecast_hour: 87,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::MemberGribFiles,
    },
    ModelSummary {
        id: ModelId::Rtma,
        description: "RTMA 2.5 km hourly surface analysis",
        default_product: "2dvaranl_ndfd",
        cycle_hours_utc: HOURLY_ANALYSIS_CYCLE_HOURS,
        max_forecast_hour: 0,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Urma,
        description: "URMA 2.5 km hourly surface analysis",
        default_product: "2dvaranl_ndfd",
        cycle_hours_utc: HOURLY_ANALYSIS_CYCLE_HOURS,
        max_forecast_hour: 0,
        sources: NOMADS_ONLY_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::Nbm,
        description: "National Blend of Models gridded core forecast",
        default_product: "core/co",
        cycle_hours_utc: HOURLY_ANALYSIS_CYCLE_HOURS,
        max_forecast_hour: 264,
        sources: NOMADS_AWS_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::RrfsA,
        description: "RRFS-A AWS open data feed with CONUS/NA/HI/PR variants",
        default_product: "prs-conus",
        cycle_hours_utc: RRFS_A_CYCLE_HOURS,
        max_forecast_hour: 60,
        sources: RRFS_A_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::RrfsPublic,
        description: "RRFS public prototype 3 km CONUS deterministic forecast",
        default_product: "prs-conus",
        cycle_hours_utc: RRFS_PUBLIC_CYCLE_HOURS,
        max_forecast_hour: 60,
        sources: RRFS_PUBLIC_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::RrfsFireWx,
        description: "RRFS public 1.5 km fire-weather nest deterministic forecast",
        default_product: "2dfld-firewx",
        cycle_hours_utc: RRFS_FIREWX_CYCLE_HOURS,
        max_forecast_hour: 36,
        sources: RRFS_FIREWX_SOURCES,
        runtime_family: ModelRuntimeFamily::Grib2Forecast,
        ensemble_mode: EnsembleMode::Deterministic,
    },
    ModelSummary {
        id: ModelId::WrfGdex,
        description: "WRF wrfout archive on UCAR GDEX THREDDS",
        default_product: WRF_GDEX_DEFAULT_SURFACE_PRODUCT,
        cycle_hours_utc: WRF_GDEX_CYCLE_HOURS,
        max_forecast_hour: 0,
        sources: WRF_GDEX_SOURCES,
        runtime_family: ModelRuntimeFamily::WrfNetcdfArchive,
        ensemble_mode: EnsembleMode::Deterministic,
    },
];

const fn field_spec(
    key: &'static str,
    label: &'static str,
    family: ProductFamily,
    level_kind: GribLevelKind,
    level_value: Option<i32>,
    selector: Option<FieldSelector>,
    idx_patterns: &'static [&'static str],
) -> GribFieldSpec {
    GribFieldSpec {
        key,
        label,
        family,
        level_kind,
        level_value,
        selector,
        idx_fallback_patterns: idx_patterns,
    }
}

const FIELD_500_HEIGHT: GribFieldSpec = field_spec(
    "height_500mb",
    "500mb Height",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(
        CanonicalField::GeopotentialHeight,
        500,
    )),
    &["HGT:500 mb"],
);

const FIELD_AIGEFS_SPR_500_HEIGHT_STDDEV: GribFieldSpec = field_spec(
    "aigefs_spread_height_500mb_stddev",
    "AI-GEFS 500mb Height Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
            .with_ensemble_standard_deviation(),
    ),
    &["HGT:500 mb"],
);

const FIELD_HGEFS_SPR_500_HEIGHT_STDDEV: GribFieldSpec = field_spec(
    "hgefs_spread_height_500mb_stddev",
    "HGEFS 500mb Height Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
            .with_ensemble_standard_deviation(),
    ),
    &["HGT:500 mb"],
);

const FIELD_HREF_SPRD_500_HEIGHT: GribFieldSpec = field_spec(
    "href_spread_height_500mb",
    "HREF 500mb Height Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["HGT:500 mb"],
);

const FIELD_GEFS_AVG_500_HEIGHT: GribFieldSpec = field_spec(
    "gefs_mean_height_500mb",
    "GEFS 500mb Height Mean",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500).with_ensemble_mean()),
    &["HGT:500 mb"],
);

const FIELD_GEFS_SPR_500_HEIGHT_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_height_500mb_stddev",
    "GEFS 500mb Height Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
            .with_ensemble_standard_deviation(),
    ),
    &["HGT:500 mb"],
);

const FIELD_700_HEIGHT: GribFieldSpec = field_spec(
    "height_700mb",
    "700mb Height",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(
        CanonicalField::GeopotentialHeight,
        700,
    )),
    &["HGT:700 mb"],
);

const FIELD_850_HEIGHT: GribFieldSpec = field_spec(
    "height_850mb",
    "850mb Height",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(
        CanonicalField::GeopotentialHeight,
        850,
    )),
    &["HGT:850 mb"],
);

const FIELD_500_TEMP: GribFieldSpec = field_spec(
    "temperature_500mb",
    "500mb Temperature",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 500)),
    &["TMP:500 mb"],
);

const FIELD_GEFS_AVG_500_TEMP: GribFieldSpec = field_spec(
    "gefs_mean_temperature_500mb",
    "GEFS 500mb Temperature Mean",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 500).with_ensemble_mean()),
    &["TMP:500 mb"],
);

const FIELD_GEFS_SPR_500_TEMP_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_temperature_500mb_stddev",
    "GEFS 500mb Temperature Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::Temperature, 500)
            .with_ensemble_standard_deviation(),
    ),
    &["TMP:500 mb"],
);

const FIELD_HREF_SPRD_500_TEMP: GribFieldSpec = field_spec(
    "href_spread_temperature_500mb",
    "HREF 500mb Temperature Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::Temperature, 500)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["TMP:500 mb"],
);

const FIELD_850_TEMP: GribFieldSpec = field_spec(
    "temperature_850mb",
    "850mb Temperature",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 850)),
    &["TMP:850 mb"],
);

const FIELD_700_TEMP: GribFieldSpec = field_spec(
    "temperature_700mb",
    "700mb Temperature",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 700)),
    &["TMP:700 mb"],
);

const FIELD_700_DEWPOINT: GribFieldSpec = field_spec(
    "dewpoint_700mb",
    "700mb Dewpoint",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(CanonicalField::Dewpoint, 700)),
    &["DPT:700 mb"],
);

const FIELD_850_DEWPOINT: GribFieldSpec = field_spec(
    "dewpoint_850mb",
    "850mb Dewpoint",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(CanonicalField::Dewpoint, 850)),
    &["DPT:850 mb"],
);

const FIELD_500_RH: GribFieldSpec = field_spec(
    "rh_500mb",
    "500mb Relative Humidity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(
        CanonicalField::RelativeHumidity,
        500,
    )),
    &["RH:500 mb"],
);

const FIELD_GEFS_AVG_500_RH: GribFieldSpec = field_spec(
    "gefs_mean_rh_500mb",
    "GEFS 500mb Relative Humidity Mean",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::RelativeHumidity, 500).with_ensemble_mean()),
    &["RH:500 mb"],
);

const FIELD_GEFS_SPR_500_RH_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_rh_500mb_stddev",
    "GEFS 500mb Relative Humidity Spread",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(
        FieldSelector::isobaric(CanonicalField::RelativeHumidity, 500)
            .with_ensemble_standard_deviation(),
    ),
    &["RH:500 mb"],
);

const FIELD_700_RH: GribFieldSpec = field_spec(
    "rh_700mb",
    "700mb Relative Humidity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(
        CanonicalField::RelativeHumidity,
        700,
    )),
    &["RH:700 mb"],
);

const FIELD_850_RH: GribFieldSpec = field_spec(
    "rh_850mb",
    "850mb Relative Humidity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(
        CanonicalField::RelativeHumidity,
        850,
    )),
    &["RH:850 mb"],
);

const FIELD_500_ABSOLUTE_VORTICITY: GribFieldSpec = field_spec(
    "absolute_vorticity_500mb",
    "500mb Absolute Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(
        CanonicalField::AbsoluteVorticity,
        500,
    )),
    &["ABSV:500 mb"],
);

const FIELD_700_ABSOLUTE_VORTICITY: GribFieldSpec = field_spec(
    "absolute_vorticity_700mb",
    "700mb Absolute Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(
        CanonicalField::AbsoluteVorticity,
        700,
    )),
    &["ABSV:700 mb"],
);

const FIELD_850_ABSOLUTE_VORTICITY: GribFieldSpec = field_spec(
    "absolute_vorticity_850mb",
    "850mb Absolute Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(
        CanonicalField::AbsoluteVorticity,
        850,
    )),
    &["ABSV:850 mb"],
);

const FIELD_500_U: GribFieldSpec = field_spec(
    "u_500mb",
    "500mb U Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 500)),
    &["UGRD:500 mb"],
);

const FIELD_500_V: GribFieldSpec = field_spec(
    "v_500mb",
    "500mb V Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 500)),
    &["VGRD:500 mb"],
);

const FIELD_GEFS_AVG_500_U: GribFieldSpec = field_spec(
    "gefs_mean_u_500mb",
    "GEFS 500mb U Wind Mean",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 500).with_ensemble_mean()),
    &["UGRD:500 mb"],
);

const FIELD_GEFS_AVG_500_V: GribFieldSpec = field_spec(
    "gefs_mean_v_500mb",
    "GEFS 500mb V Wind Mean",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 500).with_ensemble_mean()),
    &["VGRD:500 mb"],
);

const FIELD_700_U: GribFieldSpec = field_spec(
    "u_700mb",
    "700mb U Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 700)),
    &["UGRD:700 mb"],
);

const FIELD_700_V: GribFieldSpec = field_spec(
    "v_700mb",
    "700mb V Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 700)),
    &["VGRD:700 mb"],
);

const FIELD_850_U: GribFieldSpec = field_spec(
    "u_850mb",
    "850mb U Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 850)),
    &["UGRD:850 mb"],
);

const FIELD_850_V: GribFieldSpec = field_spec(
    "v_850mb",
    "850mb V Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 850)),
    &["VGRD:850 mb"],
);

const FIELD_200_HEIGHT: GribFieldSpec = field_spec(
    "height_200mb",
    "200mb Height",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(200),
    Some(FieldSelector::isobaric(
        CanonicalField::GeopotentialHeight,
        200,
    )),
    &["HGT:200 mb"],
);

const FIELD_300_HEIGHT: GribFieldSpec = field_spec(
    "height_300mb",
    "300mb Height",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(300),
    Some(FieldSelector::isobaric(
        CanonicalField::GeopotentialHeight,
        300,
    )),
    &["HGT:300 mb"],
);

const FIELD_250_HEIGHT: GribFieldSpec = field_spec(
    "height_250mb",
    "250mb Height",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(250),
    Some(FieldSelector::isobaric(
        CanonicalField::GeopotentialHeight,
        250,
    )),
    &["HGT:250 mb"],
);

const FIELD_200_TEMP: GribFieldSpec = field_spec(
    "temperature_200mb",
    "200mb Temperature",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(200),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 200)),
    &["TMP:200 mb"],
);

const FIELD_300_TEMP: GribFieldSpec = field_spec(
    "temperature_300mb",
    "300mb Temperature",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(300),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 300)),
    &["TMP:300 mb"],
);

const FIELD_250_TEMP: GribFieldSpec = field_spec(
    "temperature_250mb",
    "250mb Temperature",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(250),
    Some(FieldSelector::isobaric(CanonicalField::Temperature, 250)),
    &["TMP:250 mb"],
);

const FIELD_200_RH: GribFieldSpec = field_spec(
    "rh_200mb",
    "200mb Relative Humidity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(200),
    Some(FieldSelector::isobaric(
        CanonicalField::RelativeHumidity,
        200,
    )),
    &["RH:200 mb"],
);

const FIELD_300_RH: GribFieldSpec = field_spec(
    "rh_300mb",
    "300mb Relative Humidity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(300),
    Some(FieldSelector::isobaric(
        CanonicalField::RelativeHumidity,
        300,
    )),
    &["RH:300 mb"],
);

const FIELD_200_ABSOLUTE_VORTICITY: GribFieldSpec = field_spec(
    "absolute_vorticity_200mb",
    "200mb Absolute Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(200),
    Some(FieldSelector::isobaric(
        CanonicalField::AbsoluteVorticity,
        200,
    )),
    &["ABSV:200 mb"],
);

const FIELD_300_ABSOLUTE_VORTICITY: GribFieldSpec = field_spec(
    "absolute_vorticity_300mb",
    "300mb Absolute Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(300),
    Some(FieldSelector::isobaric(
        CanonicalField::AbsoluteVorticity,
        300,
    )),
    &["ABSV:300 mb"],
);

const FIELD_200_U: GribFieldSpec = field_spec(
    "u_200mb",
    "200mb U Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(200),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 200)),
    &["UGRD:200 mb"],
);

const FIELD_200_V: GribFieldSpec = field_spec(
    "v_200mb",
    "200mb V Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(200),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 200)),
    &["VGRD:200 mb"],
);

const FIELD_300_U: GribFieldSpec = field_spec(
    "u_300mb",
    "300mb U Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(300),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 300)),
    &["UGRD:300 mb"],
);

const FIELD_300_V: GribFieldSpec = field_spec(
    "v_300mb",
    "300mb V Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(300),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 300)),
    &["VGRD:300 mb"],
);

const FIELD_250_U: GribFieldSpec = field_spec(
    "u_250mb",
    "250mb U Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(250),
    Some(FieldSelector::isobaric(CanonicalField::UWind, 250)),
    &["UGRD:250 mb"],
);

const FIELD_250_V: GribFieldSpec = field_spec(
    "v_250mb",
    "250mb V Wind",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(250),
    Some(FieldSelector::isobaric(CanonicalField::VWind, 250)),
    &["VGRD:250 mb"],
);

const FIELD_2M_TEMP: GribFieldSpec = field_spec(
    "temperature_2m_agl",
    "2m AGL Temperature",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2)),
    &["TMP:2 m above ground"],
);

const FIELD_GEFS_AVG_2M_TEMP: GribFieldSpec = field_spec(
    "gefs_mean_temperature_2m_agl",
    "GEFS 2m AGL Temperature Mean",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2).with_ensemble_mean()),
    &["TMP:2 m above ground"],
);

const FIELD_GEFS_SPR_2M_TEMP_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_temperature_2m_agl_stddev",
    "GEFS 2m AGL Temperature Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_ensemble_standard_deviation(),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_GEFS_AVG_2M_RH: GribFieldSpec = field_spec(
    "gefs_mean_relative_humidity_2m_agl",
    "GEFS 2m AGL Relative Humidity Mean",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2).with_ensemble_mean()),
    &["RH:2 m above ground"],
);

const FIELD_GEFS_SPR_2M_RH_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_relative_humidity_2m_agl_stddev",
    "GEFS 2m AGL Relative Humidity Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2)
            .with_ensemble_standard_deviation(),
    ),
    &["RH:2 m above ground"],
);

const fn qmd_height_agl_stat_field_spec(
    key: &'static str,
    label: &'static str,
    field: CanonicalField,
    height_m: u16,
    product: FieldProduct,
    idx_patterns: &'static [&'static str],
) -> GribFieldSpec {
    field_spec(
        key,
        label,
        ProductFamily::Surface,
        GribLevelKind::HeightAboveGround,
        Some(height_m as i32),
        Some(FieldSelector::height_agl(field, height_m).with_product(product)),
        idx_patterns,
    )
}

const FIELD_QMD_2M_TEMP_MEAN: GribFieldSpec = field_spec(
    "qmd_temperature_2m_agl_mean",
    "NBM QMD 2m AGL Temperature Mean",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2).with_ensemble_mean()),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_2M_TEMP_STDDEV: GribFieldSpec = field_spec(
    "qmd_temperature_2m_agl_stddev",
    "NBM QMD 2m AGL Temperature Std Dev",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_ensemble_standard_deviation(),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_2M_TEMP_P10: GribFieldSpec = field_spec(
    "qmd_temperature_2m_agl_p10",
    "NBM QMD 2m AGL Temperature P10",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2).with_percentile(10)),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_2M_TEMP_P50: GribFieldSpec = field_spec(
    "qmd_temperature_2m_agl_p50",
    "NBM QMD 2m AGL Temperature P50",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2).with_percentile(50)),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_2M_TEMP_P90: GribFieldSpec = field_spec(
    "qmd_temperature_2m_agl_p90",
    "NBM QMD 2m AGL Temperature P90",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2).with_percentile(90)),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_PROB_2M_TEMP_BELOW_273P15K: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_probability_temperature_2m_agl_lt_273p15k",
    "NBM QMD Probability 2m AGL Temperature < 273.15 K",
    CanonicalField::Temperature,
    2,
    FieldProduct::Probability(ProbabilitySelection::below_milli(273_150)),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_PROB_2M_TEMP_ABOVE_305P372K: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_probability_temperature_2m_agl_gt_305p372k",
    "NBM QMD Probability 2m AGL Temperature > 305.372 K",
    CanonicalField::Temperature,
    2,
    FieldProduct::Probability(ProbabilitySelection::above_milli(305_372)),
    &["TMP:2 m above ground"],
);

const FIELD_QMD_2M_DEWPOINT_MEAN: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_dewpoint_2m_agl_mean",
    "NBM QMD 2m AGL Dewpoint Mean",
    CanonicalField::Dewpoint,
    2,
    FieldProduct::EnsembleMean,
    &["DPT:2 m above ground"],
);

const FIELD_QMD_2M_DEWPOINT_STDDEV: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_dewpoint_2m_agl_stddev",
    "NBM QMD 2m AGL Dewpoint Std Dev",
    CanonicalField::Dewpoint,
    2,
    FieldProduct::EnsembleStandardDeviation,
    &["DPT:2 m above ground"],
);

const FIELD_QMD_2M_DEWPOINT_P10: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_dewpoint_2m_agl_p10",
    "NBM QMD 2m AGL Dewpoint P10",
    CanonicalField::Dewpoint,
    2,
    FieldProduct::Percentile(10),
    &["DPT:2 m above ground"],
);

const FIELD_QMD_2M_DEWPOINT_P50: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_dewpoint_2m_agl_p50",
    "NBM QMD 2m AGL Dewpoint P50",
    CanonicalField::Dewpoint,
    2,
    FieldProduct::Percentile(50),
    &["DPT:2 m above ground"],
);

const FIELD_QMD_2M_DEWPOINT_P90: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_dewpoint_2m_agl_p90",
    "NBM QMD 2m AGL Dewpoint P90",
    CanonicalField::Dewpoint,
    2,
    FieldProduct::Percentile(90),
    &["DPT:2 m above ground"],
);

const FIELD_QMD_PROB_2M_DEWPOINT_ABOVE_291P483K: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_probability_dewpoint_2m_agl_gt_291p483k",
    "NBM QMD Probability 2m AGL Dewpoint > 291.483 K",
    CanonicalField::Dewpoint,
    2,
    FieldProduct::Probability(ProbabilitySelection::above_milli(291_483)),
    &["DPT:2 m above ground"],
);

const FIELD_QMD_2M_RH_P10: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_relative_humidity_2m_agl_p10",
    "NBM QMD 2m AGL Relative Humidity P10",
    CanonicalField::RelativeHumidity,
    2,
    FieldProduct::Percentile(10),
    &["RH:2 m above ground"],
);

const FIELD_QMD_2M_RH_P50: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_relative_humidity_2m_agl_p50",
    "NBM QMD 2m AGL Relative Humidity P50",
    CanonicalField::RelativeHumidity,
    2,
    FieldProduct::Percentile(50),
    &["RH:2 m above ground"],
);

const FIELD_QMD_2M_RH_P90: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_relative_humidity_2m_agl_p90",
    "NBM QMD 2m AGL Relative Humidity P90",
    CanonicalField::RelativeHumidity,
    2,
    FieldProduct::Percentile(90),
    &["RH:2 m above ground"],
);

const FIELD_AIGEFS_SPR_2M_TEMP_STDDEV: GribFieldSpec = field_spec(
    "aigefs_spread_temperature_2m_agl_stddev",
    "AI-GEFS 2m AGL Temperature Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_ensemble_standard_deviation(),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_HGEFS_SPR_2M_TEMP_STDDEV: GribFieldSpec = field_spec(
    "hgefs_spread_temperature_2m_agl_stddev",
    "HGEFS 2m AGL Temperature Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_ensemble_standard_deviation(),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_HREF_SPRD_2M_TEMP: GribFieldSpec = field_spec(
    "href_spread_temperature_2m_agl",
    "HREF 2m AGL Temperature Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_2M_DEWPOINT: GribFieldSpec = field_spec(
    "dewpoint_2m_agl",
    "2m AGL Dewpoint",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Dewpoint, 2)),
    &["DPT:2 m above ground"],
);

const FIELD_HREF_SPRD_2M_DEWPOINT: GribFieldSpec = field_spec(
    "href_spread_dewpoint_2m_agl",
    "HREF 2m AGL Dewpoint Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Dewpoint, 2)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["DPT:2 m above ground"],
);

const FIELD_2M_RH: GribFieldSpec = field_spec(
    "relative_humidity_2m_agl",
    "2m AGL Relative Humidity",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(
        CanonicalField::RelativeHumidity,
        2,
    )),
    &["RH:2 m above ground"],
);

const FIELD_10M_U: GribFieldSpec = field_spec(
    "u_10m_agl",
    "10m AGL U Wind",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(FieldSelector::height_agl(CanonicalField::UWind, 10)),
    &["UGRD:10 m above ground"],
);

const FIELD_10M_V: GribFieldSpec = field_spec(
    "v_10m_agl",
    "10m AGL V Wind",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(FieldSelector::height_agl(CanonicalField::VWind, 10)),
    &["VGRD:10 m above ground"],
);

const FIELD_GEFS_AVG_10M_U: GribFieldSpec = field_spec(
    "gefs_mean_u_10m_agl",
    "GEFS 10m AGL U Wind Mean",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(FieldSelector::height_agl(CanonicalField::UWind, 10).with_ensemble_mean()),
    &["UGRD:10 m above ground"],
);

const FIELD_GEFS_AVG_10M_V: GribFieldSpec = field_spec(
    "gefs_mean_v_10m_agl",
    "GEFS 10m AGL V Wind Mean",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(FieldSelector::height_agl(CanonicalField::VWind, 10).with_ensemble_mean()),
    &["VGRD:10 m above ground"],
);

const FIELD_10M_WIND_GUST: GribFieldSpec = field_spec(
    "wind_gust_10m_agl",
    "10m AGL Wind Gust",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(FieldSelector::height_agl(CanonicalField::WindGust, 10)),
    &["GUST:surface", "GUST:10 m above ground", "WSPD10MAX"],
);

const FIELD_QMD_10M_WIND_GUST_MEAN: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_gust_10m_agl_mean",
    "NBM QMD 10m AGL Wind Gust Mean",
    CanonicalField::WindGust,
    10,
    FieldProduct::EnsembleMean,
    &["GUST:10 m above ground"],
);

const FIELD_QMD_10M_WIND_GUST_STDDEV: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_gust_10m_agl_stddev",
    "NBM QMD 10m AGL Wind Gust Std Dev",
    CanonicalField::WindGust,
    10,
    FieldProduct::EnsembleStandardDeviation,
    &["GUST:10 m above ground"],
);

const FIELD_QMD_PROB_10M_WIND_GUST_ABOVE_24P6933MS: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_probability_wind_gust_10m_agl_gt_24p6933ms",
    "NBM QMD Probability 10m AGL Wind Gust > 24.6933 m/s",
    CanonicalField::WindGust,
    10,
    FieldProduct::Probability(ProbabilitySelection::above_milli(24_693)),
    &["GUST:10 m above ground"],
);

const FIELD_QMD_10M_WIND_GUST_P10: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_gust_10m_agl_p10",
    "NBM QMD 10m AGL Wind Gust P10",
    CanonicalField::WindGust,
    10,
    FieldProduct::Percentile(10),
    &["GUST:10 m above ground"],
);

const FIELD_QMD_10M_WIND_GUST_P50: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_gust_10m_agl_p50",
    "NBM QMD 10m AGL Wind Gust P50",
    CanonicalField::WindGust,
    10,
    FieldProduct::Percentile(50),
    &["GUST:10 m above ground"],
);

const FIELD_QMD_10M_WIND_GUST_P90: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_gust_10m_agl_p90",
    "NBM QMD 10m AGL Wind Gust P90",
    CanonicalField::WindGust,
    10,
    FieldProduct::Percentile(90),
    &["GUST:10 m above ground"],
);

const FIELD_QMD_10M_WIND_SPEED_MEAN: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_speed_10m_agl_mean",
    "NBM QMD 10m AGL Wind Speed Mean",
    CanonicalField::WindSpeed,
    10,
    FieldProduct::EnsembleMean,
    &["WIND:10 m above ground"],
);

const FIELD_QMD_10M_WIND_SPEED_STDDEV: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_speed_10m_agl_stddev",
    "NBM QMD 10m AGL Wind Speed Std Dev",
    CanonicalField::WindSpeed,
    10,
    FieldProduct::EnsembleStandardDeviation,
    &["WIND:10 m above ground"],
);

const FIELD_QMD_PROB_10M_WIND_SPEED_ABOVE_17P4911MS: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_probability_wind_speed_10m_agl_gt_17p4911ms",
    "NBM QMD Probability 10m AGL Wind Speed > 17.4911 m/s",
    CanonicalField::WindSpeed,
    10,
    FieldProduct::Probability(ProbabilitySelection::above_milli(17_491)),
    &["WIND:10 m above ground"],
);

const FIELD_QMD_10M_WIND_SPEED_P10: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_speed_10m_agl_p10",
    "NBM QMD 10m AGL Wind Speed P10",
    CanonicalField::WindSpeed,
    10,
    FieldProduct::Percentile(10),
    &["WIND:10 m above ground"],
);

const FIELD_QMD_10M_WIND_SPEED_P50: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_speed_10m_agl_p50",
    "NBM QMD 10m AGL Wind Speed P50",
    CanonicalField::WindSpeed,
    10,
    FieldProduct::Percentile(50),
    &["WIND:10 m above ground"],
);

const FIELD_QMD_10M_WIND_SPEED_P90: GribFieldSpec = qmd_height_agl_stat_field_spec(
    "qmd_wind_speed_10m_agl_p90",
    "NBM QMD 10m AGL Wind Speed P90",
    CanonicalField::WindSpeed,
    10,
    FieldProduct::Percentile(90),
    &["WIND:10 m above ground"],
);

const FIELD_HREF_SPRD_10M_WIND_SPEED: GribFieldSpec = field_spec(
    "href_spread_wind_speed_10m_agl",
    "HREF 10m AGL Wind Speed Spread",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(
        FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["WIND:10 m above ground"],
);

const FIELD_SREF_PROB_2M_TEMP_BELOW_FREEZING: GribFieldSpec = field_spec(
    "sref_probability_temperature_2m_agl_lt_273k",
    "SREF Probability 2m Temperature < 273 K",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_probability(ProbabilitySelection::below_milli(273_000)),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_SREF_PROB_2M_TEMP_ABOVE_298P8K: GribFieldSpec = field_spec(
    "sref_probability_temperature_2m_agl_gt_298p8k",
    "SREF Probability 2m Temperature > 298.8 K",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(
        FieldSelector::height_agl(CanonicalField::Temperature, 2)
            .with_probability(ProbabilitySelection::above_milli(298_800)),
    ),
    &["TMP:2 m above ground"],
);

const FIELD_SREF_PROB_850MB_TEMP_BELOW_FREEZING: GribFieldSpec = field_spec(
    "sref_probability_temperature_850mb_lt_273k",
    "SREF Probability 850mb Temperature < 273 K",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(
        FieldSelector::isobaric(CanonicalField::Temperature, 850)
            .with_probability(ProbabilitySelection::below_milli(273_000)),
    ),
    &["TMP:850 mb"],
);

const FIELD_SREF_PROB_VISIBILITY_BELOW_ONE_MILE: GribFieldSpec = field_spec(
    "sref_probability_visibility_surface_lt_1609m",
    "SREF Probability Visibility < 1609 m",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(
        FieldSelector::surface(CanonicalField::Visibility)
            .with_probability(ProbabilitySelection::below_milli(1_609_000)),
    ),
    &["VIS:surface"],
);

const FIELD_SREF_PROB_VISIBILITY_BELOW_402M: GribFieldSpec = field_spec(
    "sref_probability_visibility_surface_lt_402m",
    "SREF Probability Visibility < 402 m",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(
        FieldSelector::surface(CanonicalField::Visibility)
            .with_probability(ProbabilitySelection::below_milli(402_000)),
    ),
    &["VIS:surface"],
);

const FIELD_SREF_PROB_VISIBILITY_BELOW_804M: GribFieldSpec = field_spec(
    "sref_probability_visibility_surface_lt_804m",
    "SREF Probability Visibility < 804 m",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(
        FieldSelector::surface(CanonicalField::Visibility)
            .with_probability(ProbabilitySelection::below_milli(804_000)),
    ),
    &["VIS:surface"],
);

const FIELD_SREF_PROB_VISIBILITY_BELOW_3218M: GribFieldSpec = field_spec(
    "sref_probability_visibility_surface_lt_3218m",
    "SREF Probability Visibility < 3218 m",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(
        FieldSelector::surface(CanonicalField::Visibility)
            .with_probability(ProbabilitySelection::below_milli(3_218_000)),
    ),
    &["VIS:surface"],
);

const FIELD_SREF_PROB_10M_WIND_SPEED_ABOVE_12P89MS: GribFieldSpec = field_spec(
    "sref_probability_wind_speed_10m_agl_gt_12p89ms",
    "SREF Probability 10m Wind Speed > 12.89 m/s",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(
        FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
            .with_probability(ProbabilitySelection::above_milli(12_890)),
    ),
    &["WIND:10 m above ground"],
);

const FIELD_SREF_PROB_10M_WIND_SPEED_ABOVE_17P5MS: GribFieldSpec = field_spec(
    "sref_probability_wind_speed_10m_agl_gt_17p5ms",
    "SREF Probability 10m Wind Speed > 17.5 m/s",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(
        FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
            .with_probability(ProbabilitySelection::above_milli(17_500)),
    ),
    &["WIND:10 m above ground"],
);

const FIELD_SREF_PROB_10M_WIND_SPEED_ABOVE_25P78MS: GribFieldSpec = field_spec(
    "sref_probability_wind_speed_10m_agl_gt_25p78ms",
    "SREF Probability 10m Wind Speed > 25.78 m/s",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(
        FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
            .with_probability(ProbabilitySelection::above_milli(25_780)),
    ),
    &["WIND:10 m above ground"],
);

const FIELD_MSLP: GribFieldSpec = field_spec(
    "pressure_reduced_to_mean_sea_level",
    "MSLP",
    ProductFamily::Surface,
    GribLevelKind::MeanSeaLevel,
    None,
    Some(FieldSelector::mean_sea_level(
        CanonicalField::PressureReducedToMeanSeaLevel,
    )),
    &[
        "PRMSL:mean sea level",
        "MSLMA:mean sea level",
        "MSLET:mean sea level",
    ],
);

const FIELD_GEFS_AVG_MSLP: GribFieldSpec = field_spec(
    "gefs_mean_pressure_reduced_to_mean_sea_level",
    "GEFS MSLP Mean",
    ProductFamily::Surface,
    GribLevelKind::MeanSeaLevel,
    None,
    Some(
        FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
            .with_ensemble_mean(),
    ),
    &["PRMSL:mean sea level"],
);

const FIELD_GEFS_SPR_MSLP_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_pressure_reduced_to_mean_sea_level_stddev",
    "GEFS MSLP Spread",
    ProductFamily::Surface,
    GribLevelKind::MeanSeaLevel,
    None,
    Some(
        FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
            .with_ensemble_standard_deviation(),
    ),
    &["PRMSL:mean sea level"],
);

const FIELD_HREF_SPRD_MSLP: GribFieldSpec = field_spec(
    "href_spread_pressure_reduced_to_mean_sea_level",
    "HREF MSLP Spread",
    ProductFamily::Surface,
    GribLevelKind::MeanSeaLevel,
    None,
    Some(
        FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["MSLET:mean sea level"],
);

const FIELD_PWAT: GribFieldSpec = field_spec(
    "precipitable_water",
    "Precipitable Water",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::PrecipitableWater,
    )),
    &["PWAT:entire atmosphere", "PWAT:"],
);

const FIELD_GEFS_AVG_PWAT: GribFieldSpec = field_spec(
    "gefs_mean_precipitable_water",
    "GEFS Precipitable Water Mean",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater).with_ensemble_mean()),
    &["PWAT:entire atmosphere", "PWAT:"],
);

const FIELD_GEFS_SPR_PWAT_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_precipitable_water_stddev",
    "GEFS Precipitable Water Spread",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(
        FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater)
            .with_ensemble_standard_deviation(),
    ),
    &["PWAT:entire atmosphere", "PWAT:"],
);

const FIELD_HREF_SPRD_PWAT: GribFieldSpec = field_spec(
    "href_spread_precipitable_water",
    "HREF Precipitable Water Spread",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(
        FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater)
            .with_product(FieldProduct::EnsembleSpread),
    ),
    &["PWAT:entire atmosphere", "PWAT:"],
);

const FIELD_TOTAL_CLOUD_COVER: GribFieldSpec = field_spec(
    "total_cloud_cover",
    "Total Cloud Cover",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::TotalCloudCover,
    )),
    &["TCDC:entire atmosphere", "TCDC:"],
);

const FIELD_GEFS_AVG_TOTAL_CLOUD_COVER: GribFieldSpec = field_spec(
    "gefs_mean_total_cloud_cover",
    "GEFS Total Cloud Cover Mean",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(CanonicalField::TotalCloudCover).with_ensemble_mean()),
    &["TCDC:entire atmosphere", "TCDC:"],
);

const FIELD_GEFS_SPR_TOTAL_CLOUD_COVER_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_total_cloud_cover_stddev",
    "GEFS Total Cloud Cover Spread",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(
        FieldSelector::entire_atmosphere(CanonicalField::TotalCloudCover)
            .with_ensemble_standard_deviation(),
    ),
    &["TCDC:entire atmosphere", "TCDC:"],
);

const FIELD_LOW_CLOUD_COVER: GribFieldSpec = field_spec(
    "low_cloud_cover",
    "Low Cloud Cover",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::LowCloudCover,
    )),
    &["LCDC:low cloud layer", "LCDC:"],
);

const FIELD_MIDDLE_CLOUD_COVER: GribFieldSpec = field_spec(
    "middle_cloud_cover",
    "Middle Cloud Cover",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::MiddleCloudCover,
    )),
    &["MCDC:middle cloud layer", "MCDC:"],
);

const FIELD_HIGH_CLOUD_COVER: GribFieldSpec = field_spec(
    "high_cloud_cover",
    "High Cloud Cover",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::HighCloudCover,
    )),
    &["HCDC:high cloud layer", "HCDC:"],
);

const FIELD_TOTAL_QPF: GribFieldSpec = field_spec(
    "total_qpf",
    "Total QPF",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(CanonicalField::TotalPrecipitation)),
    &["APCP:surface"],
);

const FIELD_GEFS_AVG_6H_QPF: GribFieldSpec = field_spec(
    "gefs_mean_6h_qpf",
    "GEFS 6h QPF Mean",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(CanonicalField::TotalPrecipitation).with_ensemble_mean()),
    &["APCP:surface"],
);

const FIELD_GEFS_SPR_6H_QPF_STDDEV: GribFieldSpec = field_spec(
    "gefs_spread_6h_qpf_stddev",
    "GEFS 6h QPF Spread",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(
        FieldSelector::surface(CanonicalField::TotalPrecipitation)
            .with_ensemble_standard_deviation(),
    ),
    &["APCP:surface"],
);

const FIELD_POP: GribFieldSpec = field_spec(
    "probability_of_precipitation",
    "Probability of Precipitation",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(
        CanonicalField::ProbabilityOfPrecipitation,
    )),
    &["APCP:surface"],
);

const FIELD_CATEGORICAL_RAIN: GribFieldSpec = field_spec(
    "categorical_rain",
    "Categorical Rain",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(CanonicalField::CategoricalRain)),
    &["CRAIN:surface"],
);

const FIELD_CATEGORICAL_FREEZING_RAIN: GribFieldSpec = field_spec(
    "categorical_freezing_rain",
    "Categorical Freezing Rain",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(
        CanonicalField::CategoricalFreezingRain,
    )),
    &["CFRZR:surface", "FRZR:surface"],
);

const FIELD_CATEGORICAL_ICE_PELLETS: GribFieldSpec = field_spec(
    "categorical_ice_pellets",
    "Categorical Ice Pellets",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(
        CanonicalField::CategoricalIcePellets,
    )),
    &["CICEP:surface"],
);

const FIELD_CATEGORICAL_SNOW: GribFieldSpec = field_spec(
    "categorical_snow",
    "Categorical Snow",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(CanonicalField::CategoricalSnow)),
    &["CSNOW:surface"],
);

const FIELD_VISIBILITY: GribFieldSpec = field_spec(
    "visibility_surface",
    "Visibility",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    Some(FieldSelector::surface(CanonicalField::Visibility)),
    &["VIS:surface"],
);

const FIELD_SIMULATED_IR: GribFieldSpec = field_spec(
    "simulated_infrared_brightness_temperature",
    "Simulated IR Satellite",
    ProductFamily::Native,
    GribLevelKind::NominalTop,
    None,
    Some(FieldSelector::nominal_top(
        CanonicalField::SimulatedInfraredBrightnessTemperature,
    )),
    &["SBT113:top of atmosphere"],
);

const FIELD_2M_THETA_E: GribFieldSpec = field_spec(
    "theta_e_2m_agl",
    "2m AGL Theta-e",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    None,
    &[],
);

const FIELD_2M_HEAT_INDEX: GribFieldSpec = field_spec(
    "heat_index_2m_agl",
    "2m AGL Heat Index",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    None,
    &[],
);

const FIELD_2M_WIND_CHILL: GribFieldSpec = field_spec(
    "wind_chill_2m_agl",
    "2m AGL Wind Chill",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    None,
    &[],
);

const FIELD_LIGHTNING_FLASH_DENSITY: GribFieldSpec = field_spec(
    "lightning_flash_density",
    "Lightning Flash Density",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(1),
    None,
    &[
        "LTNGSD:1 m above ground",
        "LTNGSD:2 m above ground",
        "LTNG:entire atmosphere",
    ],
);

const FIELD_CLOUD_COVER_LEVELS: GribFieldSpec = field_spec(
    "cloud_cover_levels",
    "Cloud Cover Levels",
    ProductFamily::Surface,
    GribLevelKind::EntireAtmosphere,
    None,
    None,
    &[],
);

const FIELD_ONE_HOUR_QPF: GribFieldSpec = field_spec(
    "one_hour_qpf",
    "1h QPF",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    None,
    &["APCP:surface"],
);

const FIELD_PRECIPITATION_TYPE: GribFieldSpec = field_spec(
    "precipitation_type",
    "Precipitation Type",
    ProductFamily::Surface,
    GribLevelKind::Surface,
    None,
    None,
    &[
        "CRAIN:surface",
        "CFRZR:surface",
        "CICEP:surface",
        "CSNOW:surface",
    ],
);

const FIELD_1KM_REFLECTIVITY: GribFieldSpec = field_spec(
    "radar_reflectivity_1km_agl",
    "1km AGL Reflectivity",
    ProductFamily::Native,
    GribLevelKind::HeightAboveGround,
    Some(1000),
    Some(FieldSelector::height_agl(
        CanonicalField::RadarReflectivity,
        1000,
    )),
    &["REFD:1000 m above ground", "REFD:1 km above ground"],
);

const FIELD_COMPOSITE_REFLECTIVITY: GribFieldSpec = field_spec(
    "composite_reflectivity",
    "Composite Reflectivity",
    ProductFamily::Native,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::CompositeReflectivity,
    )),
    &["REFC:entire atmosphere", "REFC:"],
);

const FIELD_UH: GribFieldSpec = field_spec(
    "updraft_helicity",
    "Updraft Helicity",
    ProductFamily::Native,
    GribLevelKind::HeightAboveGroundLayer,
    None,
    Some(FieldSelector::height_layer_agl(
        CanonicalField::UpdraftHelicity,
        2000,
        5000,
    )),
    &["MXUPHL:5000-2000", "UPHL:5000-2000", "UHEL:"],
);

const FIELD_SMOKE_MASS_DENSITY_8M: GribFieldSpec = field_spec(
    "smoke_mass_density_8m_agl",
    "8m AGL Smoke Mass Density",
    ProductFamily::Native,
    GribLevelKind::HeightAboveGround,
    Some(8),
    Some(FieldSelector::height_agl(
        CanonicalField::SmokeMassDensity,
        8,
    )),
    &["MASSDEN:8 m above ground"],
);

const FIELD_COLUMN_INTEGRATED_SMOKE: GribFieldSpec = field_spec(
    "column_integrated_smoke",
    "Column-Integrated Smoke",
    ProductFamily::Native,
    GribLevelKind::EntireAtmosphere,
    None,
    Some(FieldSelector::entire_atmosphere(
        CanonicalField::ColumnIntegratedSmoke,
    )),
    &[
        "COLMD:entire atmosphere (considered as a single layer)",
        "COLMD:entire atmosphere",
    ],
);

const PLOT_RECIPES: &[PlotRecipe] = &[
    PlotRecipe {
        slug: "200mb_height_winds",
        title: "200mb Height / Winds",
        filled: FIELD_200_HEIGHT,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "300mb_height_winds",
        title: "300mb Height / Winds",
        filled: FIELD_300_HEIGHT,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "250mb_height_winds",
        title: "250mb Height / Winds",
        filled: FIELD_250_HEIGHT,
        contours: Some(FIELD_250_HEIGHT),
        barbs_u: Some(FIELD_250_U),
        barbs_v: Some(FIELD_250_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "500mb_height_winds",
        title: "500mb Height / Winds",
        filled: FIELD_500_HEIGHT,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "gefs_avg_500mb_height_winds",
        title: "GEFS 500mb Height / Winds Mean",
        filled: FIELD_GEFS_AVG_500_HEIGHT,
        contours: Some(FIELD_GEFS_AVG_500_HEIGHT),
        barbs_u: Some(FIELD_GEFS_AVG_500_U),
        barbs_v: Some(FIELD_GEFS_AVG_500_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "gefs_spr_500mb_height_stddev",
        title: "GEFS 500mb Height Spread",
        filled: FIELD_GEFS_SPR_500_HEIGHT_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "700mb_height_winds",
        title: "700mb Height / Winds",
        filled: FIELD_700_HEIGHT,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "850mb_height_winds",
        title: "850mb Height / Winds",
        filled: FIELD_850_HEIGHT,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "200mb_temperature_height_winds",
        title: "200mb Temperature / Height / Winds",
        filled: FIELD_200_TEMP,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "300mb_temperature_height_winds",
        title: "300mb Temperature / Height / Winds",
        filled: FIELD_300_TEMP,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "250mb_temperature_height_winds",
        title: "250mb Temperature / Height / Winds",
        filled: FIELD_250_TEMP,
        contours: Some(FIELD_250_HEIGHT),
        barbs_u: Some(FIELD_250_U),
        barbs_v: Some(FIELD_250_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "500mb_temperature_height_winds",
        title: "500mb Temperature / Height / Winds",
        filled: FIELD_500_TEMP,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "gefs_avg_500mb_temperature_height_winds",
        title: "GEFS 500mb Temperature / Height / Winds Mean",
        filled: FIELD_GEFS_AVG_500_TEMP,
        contours: Some(FIELD_GEFS_AVG_500_HEIGHT),
        barbs_u: Some(FIELD_GEFS_AVG_500_U),
        barbs_v: Some(FIELD_GEFS_AVG_500_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "gefs_spr_500mb_temperature_stddev",
        title: "GEFS 500mb Temperature Spread",
        filled: FIELD_GEFS_SPR_500_TEMP_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "850mb_temperature_height_winds",
        title: "850mb Temperature / Height / Winds",
        filled: FIELD_850_TEMP,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "700mb_temperature_height_winds",
        title: "700mb Temperature / Height / Winds",
        filled: FIELD_700_TEMP,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "2m_relative_humidity",
        title: "2m AGL Relative Humidity",
        filled: FIELD_2M_RH,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "2m_relative_humidity_10m_winds",
        title: "2m AGL Relative Humidity / 10m Winds",
        filled: FIELD_2M_RH,
        contours: Some(FIELD_MSLP),
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "2m_temperature",
        title: "2m AGL Temperature",
        filled: FIELD_2M_TEMP,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_temperature_mean",
        title: "NBM QMD 2m AGL Temperature Mean",
        filled: FIELD_QMD_2M_TEMP_MEAN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_temperature_stddev",
        title: "NBM QMD 2m AGL Temperature Std Dev",
        filled: FIELD_QMD_2M_TEMP_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_temperature_p10",
        title: "NBM QMD 2m AGL Temperature P10",
        filled: FIELD_QMD_2M_TEMP_P10,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_temperature_p50",
        title: "NBM QMD 2m AGL Temperature P50",
        filled: FIELD_QMD_2M_TEMP_P50,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_temperature_p90",
        title: "NBM QMD 2m AGL Temperature P90",
        filled: FIELD_QMD_2M_TEMP_P90,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "nbm_qmd_prob_2m_temperature_below_273p15k",
        title: "NBM QMD Probability 2m AGL Temperature < 273.15 K",
        filled: FIELD_QMD_PROB_2M_TEMP_BELOW_273P15K,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "nbm_qmd_prob_2m_temperature_above_305p372k",
        title: "NBM QMD Probability 2m AGL Temperature > 305.372 K",
        filled: FIELD_QMD_PROB_2M_TEMP_ABOVE_305P372K,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_dewpoint_mean",
        title: "NBM QMD 2m AGL Dewpoint Mean",
        filled: FIELD_QMD_2M_DEWPOINT_MEAN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_dewpoint_stddev",
        title: "NBM QMD 2m AGL Dewpoint Std Dev",
        filled: FIELD_QMD_2M_DEWPOINT_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_dewpoint_p10",
        title: "NBM QMD 2m AGL Dewpoint P10",
        filled: FIELD_QMD_2M_DEWPOINT_P10,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_dewpoint_p50",
        title: "NBM QMD 2m AGL Dewpoint P50",
        filled: FIELD_QMD_2M_DEWPOINT_P50,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_dewpoint_p90",
        title: "NBM QMD 2m AGL Dewpoint P90",
        filled: FIELD_QMD_2M_DEWPOINT_P90,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "nbm_qmd_prob_2m_dewpoint_above_291p483k",
        title: "NBM QMD Probability 2m AGL Dewpoint > 291.483 K",
        filled: FIELD_QMD_PROB_2M_DEWPOINT_ABOVE_291P483K,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_relative_humidity_p10",
        title: "NBM QMD 2m AGL Relative Humidity P10",
        filled: FIELD_QMD_2M_RH_P10,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_relative_humidity_p50",
        title: "NBM QMD 2m AGL Relative Humidity P50",
        filled: FIELD_QMD_2M_RH_P50,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "nbm_qmd_2m_relative_humidity_p90",
        title: "NBM QMD 2m AGL Relative Humidity P90",
        filled: FIELD_QMD_2M_RH_P90,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_gust_mean",
        title: "NBM QMD 10m AGL Wind Gust Mean",
        filled: FIELD_QMD_10M_WIND_GUST_MEAN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWindGust,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_gust_stddev",
        title: "NBM QMD 10m AGL Wind Gust Std Dev",
        filled: FIELD_QMD_10M_WIND_GUST_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWindGust,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_gust_p10",
        title: "NBM QMD 10m AGL Wind Gust P10",
        filled: FIELD_QMD_10M_WIND_GUST_P10,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWindGust,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_gust_p50",
        title: "NBM QMD 10m AGL Wind Gust P50",
        filled: FIELD_QMD_10M_WIND_GUST_P50,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWindGust,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_gust_p90",
        title: "NBM QMD 10m AGL Wind Gust P90",
        filled: FIELD_QMD_10M_WIND_GUST_P90,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWindGust,
    },
    PlotRecipe {
        slug: "nbm_qmd_prob_10m_wind_gust_above_24p6933ms",
        title: "NBM QMD Probability 10m AGL Wind Gust > 24.6933 m/s",
        filled: FIELD_QMD_PROB_10M_WIND_GUST_ABOVE_24P6933MS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_speed_mean",
        title: "NBM QMD 10m AGL Wind Speed Mean",
        filled: FIELD_QMD_10M_WIND_SPEED_MEAN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWinds,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_speed_stddev",
        title: "NBM QMD 10m AGL Wind Speed Std Dev",
        filled: FIELD_QMD_10M_WIND_SPEED_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWinds,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_speed_p10",
        title: "NBM QMD 10m AGL Wind Speed P10",
        filled: FIELD_QMD_10M_WIND_SPEED_P10,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWinds,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_speed_p50",
        title: "NBM QMD 10m AGL Wind Speed P50",
        filled: FIELD_QMD_10M_WIND_SPEED_P50,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWinds,
    },
    PlotRecipe {
        slug: "nbm_qmd_10m_wind_speed_p90",
        title: "NBM QMD 10m AGL Wind Speed P90",
        filled: FIELD_QMD_10M_WIND_SPEED_P90,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWinds,
    },
    PlotRecipe {
        slug: "nbm_qmd_prob_10m_wind_speed_above_17p4911ms",
        title: "NBM QMD Probability 10m AGL Wind Speed > 17.4911 m/s",
        filled: FIELD_QMD_PROB_10M_WIND_SPEED_ABOVE_17P4911MS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "aigefs_spr_2m_temperature_stddev",
        title: "AI-GEFS 2m AGL Temperature Spread",
        filled: FIELD_AIGEFS_SPR_2M_TEMP_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "aigefs_spr_500mb_height_stddev",
        title: "AI-GEFS 500mb Height Spread",
        filled: FIELD_AIGEFS_SPR_500_HEIGHT_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "hgefs_spr_2m_temperature_stddev",
        title: "HGEFS 2m AGL Temperature Spread",
        filled: FIELD_HGEFS_SPR_2M_TEMP_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "hgefs_spr_500mb_height_stddev",
        title: "HGEFS 500mb Height Spread",
        filled: FIELD_HGEFS_SPR_500_HEIGHT_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "href_sprd_2m_temperature",
        title: "HREF 2m AGL Temperature Spread",
        filled: FIELD_HREF_SPRD_2M_TEMP,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "href_sprd_2m_dewpoint",
        title: "HREF 2m AGL Dewpoint Spread",
        filled: FIELD_HREF_SPRD_2M_DEWPOINT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "href_sprd_10m_wind_speed",
        title: "HREF 10m AGL Wind Speed Spread",
        filled: FIELD_HREF_SPRD_10M_WIND_SPEED,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWinds,
    },
    PlotRecipe {
        slug: "href_sprd_mslp",
        title: "HREF MSLP Spread",
        filled: FIELD_HREF_SPRD_MSLP,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherPressure,
    },
    PlotRecipe {
        slug: "href_sprd_precipitable_water",
        title: "HREF Precipitable Water Spread",
        filled: FIELD_HREF_SPRD_PWAT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherPrecipitableWater,
    },
    PlotRecipe {
        slug: "href_sprd_500mb_height",
        title: "HREF 500mb Height Spread",
        filled: FIELD_HREF_SPRD_500_HEIGHT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherHeight,
    },
    PlotRecipe {
        slug: "href_sprd_500mb_temperature",
        title: "HREF 500mb Temperature Spread",
        filled: FIELD_HREF_SPRD_500_TEMP,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "sref_prob_2m_temperature_below_273k",
        title: "SREF Probability 2m Temperature < 273 K",
        filled: FIELD_SREF_PROB_2M_TEMP_BELOW_FREEZING,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_2m_temperature_above_298p8k",
        title: "SREF Probability 2m Temperature > 298.8 K",
        filled: FIELD_SREF_PROB_2M_TEMP_ABOVE_298P8K,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_850mb_temperature_below_273k",
        title: "SREF Probability 850mb Temperature < 273 K",
        filled: FIELD_SREF_PROB_850MB_TEMP_BELOW_FREEZING,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_visibility_below_1609m",
        title: "SREF Probability Visibility < 1609 m",
        filled: FIELD_SREF_PROB_VISIBILITY_BELOW_ONE_MILE,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_visibility_below_402m",
        title: "SREF Probability Visibility < 402 m",
        filled: FIELD_SREF_PROB_VISIBILITY_BELOW_402M,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_visibility_below_804m",
        title: "SREF Probability Visibility < 804 m",
        filled: FIELD_SREF_PROB_VISIBILITY_BELOW_804M,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_visibility_below_3218m",
        title: "SREF Probability Visibility < 3218 m",
        filled: FIELD_SREF_PROB_VISIBILITY_BELOW_3218M,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_10m_wind_speed_above_12p89ms",
        title: "SREF Probability 10m Wind Speed > 12.89 m/s",
        filled: FIELD_SREF_PROB_10M_WIND_SPEED_ABOVE_12P89MS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_10m_wind_speed_above_17p5ms",
        title: "SREF Probability 10m Wind Speed > 17.5 m/s",
        filled: FIELD_SREF_PROB_10M_WIND_SPEED_ABOVE_17P5MS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "sref_prob_10m_wind_speed_above_25p78ms",
        title: "SREF Probability 10m Wind Speed > 25.78 m/s",
        filled: FIELD_SREF_PROB_10M_WIND_SPEED_ABOVE_25P78MS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherProbability,
    },
    PlotRecipe {
        slug: "2m_temperature_10m_winds",
        title: "2m AGL Temperature / 10m Winds",
        filled: FIELD_2M_TEMP,
        contours: Some(FIELD_MSLP),
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "gefs_avg_2m_temperature_10m_winds",
        title: "GEFS 2m AGL Temperature / 10m Winds Mean",
        filled: FIELD_GEFS_AVG_2M_TEMP,
        contours: Some(FIELD_GEFS_AVG_MSLP),
        barbs_u: Some(FIELD_GEFS_AVG_10M_U),
        barbs_v: Some(FIELD_GEFS_AVG_10M_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "gefs_spr_2m_temperature_stddev",
        title: "GEFS 2m AGL Temperature Spread",
        filled: FIELD_GEFS_SPR_2M_TEMP_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "gefs_avg_2m_relative_humidity",
        title: "GEFS 2m AGL Relative Humidity Mean",
        filled: FIELD_GEFS_AVG_2M_RH,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "gefs_spr_2m_relative_humidity_stddev",
        title: "GEFS 2m AGL Relative Humidity Spread",
        filled: FIELD_GEFS_SPR_2M_RH_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "2m_dewpoint",
        title: "2m AGL Dewpoint",
        filled: FIELD_2M_DEWPOINT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "2m_dewpoint_10m_winds",
        title: "2m AGL Dewpoint / 10m Winds",
        filled: FIELD_2M_DEWPOINT,
        contours: Some(FIELD_MSLP),
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "mslp_10m_winds",
        title: "MSLP / 10m Winds",
        filled: FIELD_MSLP,
        contours: Some(FIELD_MSLP),
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::WeatherPressure,
    },
    PlotRecipe {
        slug: "gefs_avg_mslp_10m_winds",
        title: "GEFS MSLP / 10m Winds Mean",
        filled: FIELD_GEFS_AVG_MSLP,
        contours: Some(FIELD_GEFS_AVG_MSLP),
        barbs_u: Some(FIELD_GEFS_AVG_10M_U),
        barbs_v: Some(FIELD_GEFS_AVG_10M_V),
        style: RenderStyle::WeatherPressure,
    },
    PlotRecipe {
        slug: "gefs_spr_mslp_stddev",
        title: "GEFS MSLP Spread",
        filled: FIELD_GEFS_SPR_MSLP_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherPressure,
    },
    PlotRecipe {
        slug: "10m_wind_gusts",
        title: "10m AGL Wind Gusts",
        filled: FIELD_10M_WIND_GUST,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherWindGust,
    },
    PlotRecipe {
        slug: "precipitable_water",
        title: "Precipitable Water",
        filled: FIELD_PWAT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherPrecipitableWater,
    },
    PlotRecipe {
        slug: "gefs_avg_precipitable_water",
        title: "GEFS Precipitable Water Mean",
        filled: FIELD_GEFS_AVG_PWAT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherPrecipitableWater,
    },
    PlotRecipe {
        slug: "gefs_spr_precipitable_water_stddev",
        title: "GEFS Precipitable Water Spread",
        filled: FIELD_GEFS_SPR_PWAT_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherPrecipitableWater,
    },
    PlotRecipe {
        slug: "cloud_cover",
        title: "Cloud Cover",
        filled: FIELD_TOTAL_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "gefs_avg_cloud_cover",
        title: "GEFS Cloud Cover Mean",
        filled: FIELD_GEFS_AVG_TOTAL_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "gefs_spr_cloud_cover_stddev",
        title: "GEFS Cloud Cover Spread",
        filled: FIELD_GEFS_SPR_TOTAL_CLOUD_COVER_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "low_cloud_cover",
        title: "Low Cloud Cover",
        filled: FIELD_LOW_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "middle_cloud_cover",
        title: "Middle Cloud Cover",
        filled: FIELD_MIDDLE_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "high_cloud_cover",
        title: "High Cloud Cover",
        filled: FIELD_HIGH_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "cloud_cover_levels",
        title: "Cloud Cover, Levels",
        filled: FIELD_CLOUD_COVER_LEVELS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCloudCover,
    },
    PlotRecipe {
        slug: "visibility",
        title: "Visibility",
        filled: FIELD_VISIBILITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherVisibility,
    },
    PlotRecipe {
        slug: "simulated_ir_satellite",
        title: "Simulated IR Satellite",
        filled: FIELD_SIMULATED_IR,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherSatellite,
    },
    PlotRecipe {
        slug: "lightning_flash_density",
        title: "Lightning Flash Density",
        filled: FIELD_LIGHTNING_FLASH_DENSITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherLightning,
    },
    PlotRecipe {
        slug: "total_qpf",
        title: "Total QPF",
        filled: FIELD_TOTAL_QPF,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherQpf,
    },
    PlotRecipe {
        slug: "gefs_avg_6h_qpf",
        title: "GEFS 6h QPF Mean",
        filled: FIELD_GEFS_AVG_6H_QPF,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherQpf,
    },
    PlotRecipe {
        slug: "gefs_spr_6h_qpf_stddev",
        title: "GEFS 6h QPF Spread",
        filled: FIELD_GEFS_SPR_6H_QPF_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherQpf,
    },
    PlotRecipe {
        slug: "1h_qpf",
        title: "1h QPF",
        filled: FIELD_ONE_HOUR_QPF,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherQpf,
    },
    PlotRecipe {
        slug: "probability_of_precipitation",
        title: "Probability of Precipitation",
        filled: FIELD_POP,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "categorical_rain",
        title: "Categorical Rain",
        filled: FIELD_CATEGORICAL_RAIN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCategorical,
    },
    PlotRecipe {
        slug: "categorical_freezing_rain",
        title: "Categorical Freezing Rain",
        filled: FIELD_CATEGORICAL_FREEZING_RAIN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCategorical,
    },
    PlotRecipe {
        slug: "categorical_ice_pellets",
        title: "Categorical Ice Pellets",
        filled: FIELD_CATEGORICAL_ICE_PELLETS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCategorical,
    },
    PlotRecipe {
        slug: "categorical_snow",
        title: "Categorical Snow",
        filled: FIELD_CATEGORICAL_SNOW,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCategorical,
    },
    PlotRecipe {
        slug: "precipitation_type",
        title: "Precipitation Type",
        filled: FIELD_PRECIPITATION_TYPE,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherCategorical,
    },
    PlotRecipe {
        slug: "2m_theta_e_10m_winds",
        title: "2m AGL Theta-e / 10m Winds",
        filled: FIELD_2M_THETA_E,
        contours: None,
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "2m_heat_index",
        title: "2m AGL Heat Index",
        filled: FIELD_2M_HEAT_INDEX,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "2m_wind_chill",
        title: "2m AGL Wind Chill",
        filled: FIELD_2M_WIND_CHILL,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "700mb_dewpoint_height_winds",
        title: "700mb Dewpoint / Height / Winds",
        filled: FIELD_700_DEWPOINT,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "850mb_dewpoint_height_winds",
        title: "850mb Dewpoint / Height / Winds",
        filled: FIELD_850_DEWPOINT,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::WeatherDewpoint,
    },
    PlotRecipe {
        slug: "200mb_rh_height_winds",
        title: "200mb RH / Height / Winds",
        filled: FIELD_200_RH,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "300mb_rh_height_winds",
        title: "300mb RH / Height / Winds",
        filled: FIELD_300_RH,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "500mb_rh_height_winds",
        title: "500mb RH / Height / Winds",
        filled: FIELD_500_RH,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "gefs_avg_500mb_rh_height_winds",
        title: "GEFS 500mb RH / Height / Winds Mean",
        filled: FIELD_GEFS_AVG_500_RH,
        contours: Some(FIELD_GEFS_AVG_500_HEIGHT),
        barbs_u: Some(FIELD_GEFS_AVG_500_U),
        barbs_v: Some(FIELD_GEFS_AVG_500_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "gefs_spr_500mb_rh_stddev",
        title: "GEFS 500mb RH Spread",
        filled: FIELD_GEFS_SPR_500_RH_STDDEV,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "700mb_rh_height_winds",
        title: "700mb RH / Height / Winds",
        filled: FIELD_700_RH,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "850mb_rh_height_winds",
        title: "850mb RH / Height / Winds",
        filled: FIELD_850_RH,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::WeatherRh,
    },
    PlotRecipe {
        slug: "200mb_absolute_vorticity_height_winds",
        title: "200mb Absolute Vorticity / Height / Winds",
        filled: FIELD_200_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::WeatherVorticity,
    },
    PlotRecipe {
        slug: "300mb_absolute_vorticity_height_winds",
        title: "300mb Absolute Vorticity / Height / Winds",
        filled: FIELD_300_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::WeatherVorticity,
    },
    PlotRecipe {
        slug: "500mb_absolute_vorticity_height_winds",
        title: "500mb Absolute Vorticity / Height / Winds",
        filled: FIELD_500_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::WeatherVorticity,
    },
    PlotRecipe {
        slug: "700mb_absolute_vorticity_height_winds",
        title: "700mb Absolute Vorticity / Height / Winds",
        filled: FIELD_700_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::WeatherVorticity,
    },
    PlotRecipe {
        slug: "850mb_absolute_vorticity_height_winds",
        title: "850mb Absolute Vorticity / Height / Winds",
        filled: FIELD_850_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::WeatherVorticity,
    },
    PlotRecipe {
        slug: "1km_reflectivity",
        title: "1km AGL Reflectivity",
        filled: FIELD_1KM_REFLECTIVITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherRadarReflectivity,
    },
    PlotRecipe {
        slug: "composite_reflectivity",
        title: "Composite Reflectivity",
        filled: FIELD_COMPOSITE_REFLECTIVITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherReflectivity,
    },
    PlotRecipe {
        slug: "composite_reflectivity_uh",
        title: "Composite Reflectivity / UH",
        filled: FIELD_COMPOSITE_REFLECTIVITY,
        contours: Some(FIELD_UH),
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherReflectivity,
    },
    PlotRecipe {
        slug: "uh_2to5km",
        title: "Updraft Helicity 2-5 km",
        filled: FIELD_UH,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherUh,
    },
    PlotRecipe {
        slug: "smoke_pm25_native",
        title: "PM2.5 Smoke",
        filled: FIELD_SMOKE_MASS_DENSITY_8M,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
    PlotRecipe {
        slug: "smoke_column",
        title: "Column-Integrated Smoke",
        filled: FIELD_COLUMN_INTEGRATED_SMOKE,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::WeatherTemperature,
    },
];

pub fn built_in_models() -> &'static [ModelSummary] {
    MODELS
}

pub fn built_in_plot_recipes() -> &'static [PlotRecipe] {
    PLOT_RECIPES
}

pub fn plot_recipe(slug: &str) -> Option<&'static PlotRecipe> {
    let wanted = canonical_recipe_token(slug);
    PLOT_RECIPES
        .iter()
        .find(|recipe| normalize_token(recipe.slug) == wanted)
}

pub fn plot_recipe_fetch_plan(
    slug: &str,
    model: ModelId,
) -> Result<PlotRecipeFetchPlan, ModelError> {
    let recipe = plot_recipe(slug).ok_or_else(|| ModelError::UnknownPlotRecipe {
        slug: slug.to_string(),
    })?;
    plot_recipe_fetch_plan_for(recipe, model)
}

pub fn plot_recipe_fetch_blockers(
    slug: &str,
    model: ModelId,
) -> Result<Vec<PlotRecipeBlocker>, ModelError> {
    let recipe = plot_recipe(slug).ok_or_else(|| ModelError::UnknownPlotRecipe {
        slug: slug.to_string(),
    })?;
    Ok(plot_recipe_fetch_blockers_for(recipe, model))
}

pub fn selector_supported_for_model(selector: FieldSelector, model: ModelId) -> bool {
    if !selector.product.is_default() {
        match (model, selector.product) {
            (ModelId::Nbm, _) => {}
            (ModelId::Sref, FieldProduct::Probability(_)) => {}
            (
                ModelId::Gefs,
                FieldProduct::EnsembleMean
                | FieldProduct::EnsembleStandardDeviation
                | FieldProduct::EnsembleSpread,
            ) => {}
            (
                ModelId::Aigefs | ModelId::Hgefs,
                FieldProduct::EnsembleStandardDeviation | FieldProduct::EnsembleSpread,
            ) => {}
            (ModelId::Href, FieldProduct::EnsembleSpread) => {}
            _ => return false,
        }
    }
    if model == ModelId::Href && selector.product.is_default() {
        return false;
    }
    if matches!(
        (model, selector.vertical),
        (
            ModelId::Rtma | ModelId::Urma | ModelId::Nbm,
            VerticalSelector::IsobaricHpa(_)
        )
    ) {
        return false;
    }
    match (selector.field, selector.vertical) {
        (
            CanonicalField::GeopotentialHeight
            | CanonicalField::Temperature
            | CanonicalField::RelativeHumidity
            | CanonicalField::AbsoluteVorticity
            | CanonicalField::UWind
            | CanonicalField::VWind,
            VerticalSelector::IsobaricHpa(level_hpa),
        ) if is_supported_upper_air_level(level_hpa) => true,
        (CanonicalField::Dewpoint, VerticalSelector::IsobaricHpa(level_hpa))
            if matches!(level_hpa, 700 | 850) =>
        {
            true
        }
        (
            CanonicalField::Temperature
            | CanonicalField::Dewpoint
            | CanonicalField::RelativeHumidity,
            VerticalSelector::HeightAboveGroundMeters(2),
        ) => true,
        (
            CanonicalField::UWind | CanonicalField::VWind,
            VerticalSelector::HeightAboveGroundMeters(10),
        ) => true,
        (CanonicalField::WindSpeed, VerticalSelector::HeightAboveGroundMeters(10)) => true,
        (
            CanonicalField::Pressure | CanonicalField::SmokeMassDensity,
            VerticalSelector::HybridLevel(level),
        ) => {
            matches!(model, ModelId::Hrrr | ModelId::HrrrAk)
                && is_supported_hrrr_smoke_hybrid_level(level)
        }
        (CanonicalField::WindGust, VerticalSelector::HeightAboveGroundMeters(10)) => true,
        (CanonicalField::SmokeMassDensity, VerticalSelector::HeightAboveGroundMeters(8)) => {
            matches!(model, ModelId::Hrrr | ModelId::HrrrAk)
        }
        (CanonicalField::PressureReducedToMeanSeaLevel, VerticalSelector::MeanSeaLevel) => true,
        (
            CanonicalField::PrecipitableWater | CanonicalField::TotalCloudCover,
            VerticalSelector::EntireAtmosphere,
        ) => true,
        (
            CanonicalField::LowCloudCover
            | CanonicalField::MiddleCloudCover
            | CanonicalField::HighCloudCover,
            VerticalSelector::EntireAtmosphere,
        ) => true,
        (CanonicalField::ColumnIntegratedSmoke, VerticalSelector::EntireAtmosphere) => {
            matches!(model, ModelId::Hrrr | ModelId::HrrrAk)
        }
        (
            CanonicalField::TotalPrecipitation | CanonicalField::ProbabilityOfPrecipitation,
            VerticalSelector::Surface,
        ) => true,
        (CanonicalField::Visibility, VerticalSelector::Surface) => true,
        (
            CanonicalField::CategoricalRain
            | CanonicalField::CategoricalFreezingRain
            | CanonicalField::CategoricalIcePellets
            | CanonicalField::CategoricalSnow,
            VerticalSelector::Surface,
        ) => matches!(
            model,
            ModelId::Hrrr
                | ModelId::HrrrAk
                | ModelId::Gfs
                | ModelId::Gdas
                | ModelId::RrfsA
                | ModelId::RrfsPublic
                | ModelId::RrfsFireWx
        ),
        (CanonicalField::LandSeaMask, VerticalSelector::Surface) => {
            matches!(model, ModelId::EcmwfOpenData)
        }
        (CanonicalField::RadarReflectivity, VerticalSelector::HeightAboveGroundMeters(1000)) => {
            matches!(
                model,
                ModelId::Hrrr
                    | ModelId::HrrrAk
                    | ModelId::RrfsA
                    | ModelId::RrfsPublic
                    | ModelId::RrfsFireWx
                    | ModelId::WrfGdex
            )
        }
        (CanonicalField::CompositeReflectivity, VerticalSelector::EntireAtmosphere) => {
            matches!(
                model,
                ModelId::Hrrr
                    | ModelId::HrrrAk
                    | ModelId::RrfsA
                    | ModelId::RrfsPublic
                    | ModelId::RrfsFireWx
                    | ModelId::WrfGdex
            )
        }
        (
            CanonicalField::UpdraftHelicity,
            VerticalSelector::HeightAboveGroundLayerMeters {
                bottom_m: 2000,
                top_m: 5000,
            },
        ) => matches!(
            model,
            ModelId::Hrrr
                | ModelId::HrrrAk
                | ModelId::RrfsA
                | ModelId::RrfsPublic
                | ModelId::RrfsFireWx
                | ModelId::WrfGdex
        ),
        (CanonicalField::SimulatedInfraredBrightnessTemperature, VerticalSelector::NominalTop) => {
            matches!(model, ModelId::Hrrr | ModelId::HrrrAk)
        }
        _ => false,
    }
}

pub fn model_summary(model: ModelId) -> &'static ModelSummary {
    MODELS
        .iter()
        .find(|entry| entry.id == model)
        .expect("built-in model summary missing")
}

pub fn supported_forecast_hours(model: ModelId, cycle_hour_utc: u8) -> Vec<u16> {
    match model {
        ModelId::Hrrr | ModelId::HrrrAk => {
            if cycle_hour_utc % 6 == 0 {
                (0..=48).collect()
            } else {
                (0..=18).collect()
            }
        }
        ModelId::Gfs => {
            let mut hours = (0..=120).collect::<Vec<u16>>();
            hours.extend((123..=384).step_by(3));
            hours
        }
        ModelId::Gdas => (0..=9).collect(),
        ModelId::Gefs => {
            let mut hours = (0..=240).step_by(3).collect::<Vec<u16>>();
            hours.extend((246..=384).step_by(6));
            hours
        }
        ModelId::Aigfs | ModelId::Aigefs => (0..=384).step_by(6).collect(),
        ModelId::Hgefs => (0..=240).step_by(6).collect(),
        // ECMWF Open Data currently publishes four daily IFS runs. The 00/12z
        // deterministic/ensemble open-data stream carries 3-hourly steps to
        // 144h and then 6-hourly steps to 360h; 06/18z carries 3-hourly steps
        // to 144h only.
        ModelId::EcmwfOpenData => match cycle_hour_utc {
            0 | 12 => {
                let mut hours = (0..=144).step_by(3).collect::<Vec<u16>>();
                hours.extend((150..=360).step_by(6));
                hours
            }
            6 | 18 => (0..=144).step_by(3).collect(),
            _ => Vec::new(),
        },
        ModelId::Aifs => match cycle_hour_utc {
            0 | 6 | 12 | 18 => (0..=AIFS_LOCAL_MAX_FORECAST_HOUR).step_by(6).collect(),
            _ => Vec::new(),
        },
        ModelId::Rap => {
            if rap_extended_forecast_cycle(cycle_hour_utc) {
                (0..=51).collect()
            } else {
                (0..=21).collect()
            }
        }
        ModelId::Nam => {
            let mut hours = (0..=36).collect::<Vec<u16>>();
            hours.extend((39..=84).step_by(3));
            hours
        }
        ModelId::Hiresw => (0..=48).collect(),
        ModelId::Href => (1..=48).collect(),
        ModelId::Sref => (0..=87).step_by(3).collect(),
        ModelId::Rtma | ModelId::Urma => vec![0],
        ModelId::Nbm => (1..=264).collect(),
        ModelId::RrfsA => (0..=60).collect(),
        ModelId::RrfsPublic => (0..=60).collect(),
        ModelId::RrfsFireWx => (0..=36).collect(),
        ModelId::WrfGdex => (0..=23).collect(),
    }
}

pub fn forecast_hour_supported(model: ModelId, cycle_hour_utc: u8, forecast_hour: u16) -> bool {
    supported_forecast_hours(model, cycle_hour_utc)
        .into_iter()
        .any(|candidate| candidate == forecast_hour)
}

fn rap_extended_forecast_cycle(cycle_hour_utc: u8) -> bool {
    matches!(cycle_hour_utc, 3 | 9 | 15 | 21)
}

pub fn resolve_canonical_bundle_product(
    model: ModelId,
    bundle: CanonicalBundleDescriptor,
    native_override: Option<&str>,
) -> ResolvedCanonicalBundleProduct {
    let native_product = native_override
        .unwrap_or_else(|| default_canonical_bundle_product(model, bundle))
        .to_string();
    ResolvedCanonicalBundleProduct::new(bundle, native_product)
}

pub fn default_bundle_product(model: ModelId, bundle: CanonicalBundleDescriptor) -> &'static str {
    default_canonical_bundle_product(model, bundle)
}

fn default_canonical_bundle_product(
    model: ModelId,
    bundle: CanonicalBundleDescriptor,
) -> &'static str {
    match (model, bundle) {
        (ModelId::Hrrr, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc",
        (ModelId::Hrrr, CanonicalBundleDescriptor::PressureAnalysis) => "prs",
        // HRRR native (UH/composite reflectivity / sub-hourly bundles) live
        // in the `nat` GRIB; the direct lane reroutes them onto `sfc` when
        // the surface family already covers the requested fields, but the
        // canonical native bundle still maps to `nat` here.
        (ModelId::Hrrr, CanonicalBundleDescriptor::NativeAnalysis) => "nat",
        (ModelId::HrrrAk, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc",
        (ModelId::HrrrAk, CanonicalBundleDescriptor::PressureAnalysis) => "prs",
        (ModelId::HrrrAk, CanonicalBundleDescriptor::NativeAnalysis) => "nat",
        (ModelId::Gfs, _) => "pgrb2.0p25",
        (ModelId::Gdas, _) => "pgrb2.0p25",
        (ModelId::Gefs, _) => "pgrb2ap5/gec00",
        (ModelId::Aigfs, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc",
        (ModelId::Aigfs, CanonicalBundleDescriptor::PressureAnalysis) => "pres",
        (ModelId::Aigfs, CanonicalBundleDescriptor::NativeAnalysis) => "sfc",
        (ModelId::Aigefs, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc/avg",
        (ModelId::Aigefs, CanonicalBundleDescriptor::PressureAnalysis) => "pres/avg",
        (ModelId::Aigefs, CanonicalBundleDescriptor::NativeAnalysis) => "sfc/avg",
        (ModelId::Hgefs, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc/avg",
        (ModelId::Hgefs, CanonicalBundleDescriptor::PressureAnalysis) => "pres/avg",
        (ModelId::Hgefs, CanonicalBundleDescriptor::NativeAnalysis) => "sfc/avg",
        (ModelId::EcmwfOpenData, _) => "oper",
        (ModelId::Aifs, _) => "oper",
        (ModelId::Rap, _) => "awp130pgrb",
        (ModelId::Nam, _) => "awip12",
        (ModelId::Hiresw, _) => "arw_2p5km/conus",
        (ModelId::Href, _) => "ensprod/conus/sprd",
        (ModelId::Sref, _) => "ensprod/pgrb212/mean_3hrly",
        (ModelId::Rtma, _) => "2dvaranl_ndfd",
        (ModelId::Urma, _) => "2dvaranl_ndfd",
        (ModelId::Nbm, _) => "core/co",
        // RRFS-A keeps the faster CONUS direct lane on `prs-conus`, but the
        // shared thermo/severe kernels need the NA-domain pair that actually
        // carries both the surface bundle and matching pressure grid.
        (ModelId::RrfsA, CanonicalBundleDescriptor::SurfaceAnalysis) => "nat-na",
        (ModelId::RrfsA, CanonicalBundleDescriptor::PressureAnalysis) => "prs-na",
        (ModelId::RrfsA, CanonicalBundleDescriptor::NativeAnalysis) => "nat-na",
        (ModelId::RrfsPublic, CanonicalBundleDescriptor::SurfaceAnalysis) => "2dfld-conus",
        (ModelId::RrfsPublic, CanonicalBundleDescriptor::PressureAnalysis) => "prs-conus",
        (ModelId::RrfsPublic, CanonicalBundleDescriptor::NativeAnalysis) => "prs-conus",
        (ModelId::RrfsFireWx, CanonicalBundleDescriptor::SurfaceAnalysis) => "2dfld-firewx",
        (ModelId::RrfsFireWx, CanonicalBundleDescriptor::PressureAnalysis) => "prs-firewx",
        (ModelId::RrfsFireWx, CanonicalBundleDescriptor::NativeAnalysis) => "2dfld-firewx",
        (ModelId::WrfGdex, CanonicalBundleDescriptor::SurfaceAnalysis) => {
            WRF_GDEX_DEFAULT_SURFACE_PRODUCT
        }
        (ModelId::WrfGdex, CanonicalBundleDescriptor::PressureAnalysis) => {
            WRF_GDEX_DEFAULT_PRESSURE_PRODUCT
        }
        (ModelId::WrfGdex, CanonicalBundleDescriptor::NativeAnalysis) => {
            WRF_GDEX_DEFAULT_PRESSURE_PRODUCT
        }
    }
}

/// Build a typed `CanonicalBundleId` for `(model, cycle, fhour, source)`
/// given a typed `BundleRequirement`. This is the planner-side entry point
/// that converts requirements into deduplicated load identities.
pub fn resolve_canonical_bundle_id(
    model: ModelId,
    cycle: rustwx_core::CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    requirement: &rustwx_core::BundleRequirement,
) -> rustwx_core::CanonicalBundleId {
    let resolved = resolve_canonical_bundle_product(
        model,
        requirement.bundle,
        requirement.native_override.as_deref(),
    );
    rustwx_core::CanonicalBundleId::new(
        model,
        cycle,
        forecast_hour,
        source,
        requirement.bundle,
        resolved.native_product,
    )
}

pub fn resolve_urls(request: &ModelRunRequest) -> Result<Vec<ResolvedUrl>, ModelError> {
    let mut urls = Vec::new();
    let mut errors = Vec::new();
    for source in model_summary(request.model).sources {
        match build_grib_url(source.id, request) {
            Ok(grib_url) => {
                if grib_url.starts_with("unsupported://") {
                    continue;
                }
                let idx_url = if source.idx_available {
                    Some(format!("{grib_url}.idx"))
                } else {
                    None
                };
                urls.push(ResolvedUrl {
                    source: source.id,
                    grib_url,
                    idx_url,
                });
            }
            Err(err) => errors.push(err),
        }
    }
    if urls.is_empty() {
        if let Some(err) = errors.into_iter().next() {
            return Err(err);
        }
        return Err(ModelError::UnsupportedProduct {
            model: request.model,
            product: request.product.clone(),
        });
    }
    urls.sort_by_key(|entry| {
        model_summary(request.model)
            .sources
            .iter()
            .find(|source| source.id == entry.source)
            .map(|source| source.priority)
            .unwrap_or(u8::MAX)
    });
    Ok(urls)
}

pub fn latest_available_run(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
) -> Result<LatestRun, ModelError> {
    let agent = build_agent();
    latest_available_run_with_probe(model, source, date_yyyymmdd, |resolved| {
        availability_probe_ok(&agent, resolved)
    })
}

pub fn latest_available_run_at_forecast_hour(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
    forecast_hour: u16,
) -> Result<LatestRun, ModelError> {
    let agent = build_agent();
    latest_available_run_for_products_with_probe_at_forecast_hour(
        model,
        source,
        date_yyyymmdd,
        &[model_summary(model).default_product],
        forecast_hour,
        |resolved| availability_probe_ok(&agent, resolved),
    )
}

pub fn latest_available_run_for_products(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
    products: &[&str],
) -> Result<LatestRun, ModelError> {
    let agent = build_agent();
    latest_available_run_for_products_with_probe(
        model,
        source,
        date_yyyymmdd,
        products,
        |resolved| availability_probe_ok(&agent, resolved),
    )
}

pub fn latest_available_run_for_products_at_forecast_hour(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
    products: &[&str],
    forecast_hour: u16,
) -> Result<LatestRun, ModelError> {
    let agent = build_agent();
    latest_available_run_for_products_with_probe_at_forecast_hour(
        model,
        source,
        date_yyyymmdd,
        products,
        forecast_hour,
        |resolved| availability_probe_ok(&agent, resolved),
    )
}

fn latest_available_run_with_probe<F>(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
    probe_available: F,
) -> Result<LatestRun, ModelError>
where
    F: FnMut(&ResolvedUrl) -> bool,
{
    latest_available_run_for_products_with_probe_at_forecast_hour(
        model,
        source,
        date_yyyymmdd,
        &[model_summary(model).default_product],
        0,
        probe_available,
    )
}

fn latest_available_run_for_products_with_probe<F>(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
    products: &[&str],
    probe_available: F,
) -> Result<LatestRun, ModelError>
where
    F: FnMut(&ResolvedUrl) -> bool,
{
    latest_available_run_for_products_with_probe_at_forecast_hour(
        model,
        source,
        date_yyyymmdd,
        products,
        0,
        probe_available,
    )
}

fn latest_available_run_for_products_with_probe_at_forecast_hour<F>(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
    products: &[&str],
    forecast_hour: u16,
    mut probe_available: F,
) -> Result<LatestRun, ModelError>
where
    F: FnMut(&ResolvedUrl) -> bool,
{
    let summary = model_summary(model);
    let allowed_sources = summary
        .sources
        .iter()
        .filter(|candidate| source.map(|wanted| candidate.id == wanted).unwrap_or(true))
        .map(|candidate| candidate.id)
        .collect::<Vec<_>>();
    if allowed_sources.is_empty() {
        return Err(ModelError::NoAvailableRun { model });
    }
    let required_products = if products.is_empty() {
        vec![summary.default_product]
    } else {
        let mut deduped = Vec::new();
        for product in products {
            if !deduped.iter().any(|seen| seen == product) {
                deduped.push(*product);
            }
        }
        deduped
    };

    // Walk today, then (if nothing on today) yesterday. During the publishing
    // window between two cycles the newest cycle may not be available yet,
    // but the previous day's last cycle typically still is — without this
    // rollback, `latest_available_run` wrongly reports NoAvailableRun just
    // after UTC rollover or during a publication delay.
    let candidate_dates = cycle_date_rollback_candidates(date_yyyymmdd);
    for candidate_date in &candidate_dates {
        for hour_utc in summary.cycle_hours_utc.iter().rev().copied() {
            if !forecast_hour_supported(model, hour_utc, forecast_hour) {
                continue;
            }
            let cycle = match CycleSpec::new(candidate_date.clone(), hour_utc) {
                Ok(cycle) => cycle,
                Err(_) => continue,
            };
            let available_source = allowed_sources.iter().copied().find(|candidate_source| {
                required_products.iter().all(|product| {
                    let request =
                        match ModelRunRequest::new(model, cycle.clone(), forecast_hour, *product) {
                            Ok(request) => request,
                            Err(_) => return false,
                        };
                    resolve_urls(&request)
                        .map(|resolved_urls| {
                            resolved_urls.into_iter().any(|resolved| {
                                resolved.source == *candidate_source && probe_available(&resolved)
                            })
                        })
                        .unwrap_or(false)
                })
            });

            if let Some(source) = available_source {
                return Ok(LatestRun {
                    model,
                    cycle,
                    source,
                });
            }
        }
    }

    Err(ModelError::NoAvailableRun { model })
}

fn cycle_date_rollback_candidates(date_yyyymmdd: &str) -> Vec<String> {
    let mut dates = Vec::with_capacity(2);
    dates.push(date_yyyymmdd.to_string());
    if let Some(previous) = previous_day_yyyymmdd(date_yyyymmdd) {
        dates.push(previous);
    }
    dates
}

fn previous_day_yyyymmdd(date_yyyymmdd: &str) -> Option<String> {
    if date_yyyymmdd.len() != 8 || !date_yyyymmdd.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let year: i32 = date_yyyymmdd[..4].parse().ok()?;
    let month: u32 = date_yyyymmdd[4..6].parse().ok()?;
    let day: u32 = date_yyyymmdd[6..8].parse().ok()?;
    let (new_year, new_month, new_day) = if day > 1 {
        (year, month, day - 1)
    } else if month > 1 {
        (year, month - 1, days_in_month(year, month - 1))
    } else {
        (year - 1, 12, 31)
    };
    if new_year < 1 {
        return None;
    }
    Some(format!("{:04}{:02}{:02}", new_year, new_month, new_day))
}

fn next_day_yyyymmdd(date_yyyymmdd: &str) -> Option<String> {
    if date_yyyymmdd.len() != 8 || !date_yyyymmdd.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let year: i32 = date_yyyymmdd[..4].parse().ok()?;
    let month: u32 = date_yyyymmdd[4..6].parse().ok()?;
    let day: u32 = date_yyyymmdd[6..8].parse().ok()?;
    let month_days = days_in_month(year, month);
    let (new_year, new_month, new_day) = if day < month_days {
        (year, month, day + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    };
    Some(format!("{:04}{:02}{:02}", new_year, new_month, new_day))
}

fn advance_yyyymmddhh(
    date_yyyymmdd: &str,
    hour_utc: u8,
    forecast_hour: u16,
) -> Option<(String, u8)> {
    let mut date = date_yyyymmdd.to_string();
    let total_hours = u32::from(hour_utc) + u32::from(forecast_hour);
    let day_rollovers = total_hours / 24;
    let valid_hour = (total_hours % 24) as u8;
    for _ in 0..day_rollovers {
        date = next_day_yyyymmdd(&date)?;
    }
    Some((date, valid_hour))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            if is_leap { 29 } else { 28 }
        }
        _ => 30,
    }
}

fn build_agent() -> ureq::Agent {
    rustls::crypto::CryptoProvider::install_default(rustls_rustcrypto::provider()).ok();
    let crypto = std::sync::Arc::new(rustls_rustcrypto::provider());
    ureq::Agent::config_builder()
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .provider(ureq::tls::TlsProvider::Rustls)
                .root_certs(ureq::tls::RootCerts::WebPki)
                .unversioned_rustls_crypto_provider(crypto)
                .build(),
        )
        .build()
        .new_agent()
}

fn availability_probe_ok(agent: &ureq::Agent, resolved: &ResolvedUrl) -> bool {
    if should_use_range_probe(resolved.source) {
        return range_probe_ok(agent, &resolved.grib_url);
    }
    head_ok(agent, resolved.availability_probe_url())
}

fn should_use_range_probe(source: SourceId) -> bool {
    matches!(source, SourceId::Nomads)
}

fn head_ok(agent: &ureq::Agent, url: &str) -> bool {
    let response = if url.contains("nomads.ncep.noaa.gov") {
        agent.get(url).header("Range", "bytes=0-0").call()
    } else {
        agent.head(url).call()
    };
    match response {
        Ok(_) => true,
        Err(ureq::Error::StatusCode(code)) if code == 403 || code == 404 => false,
        Err(_) => false,
    }
}

fn range_probe_ok(agent: &ureq::Agent, url: &str) -> bool {
    match agent.get(url).header("Range", "bytes=0-0").call() {
        Ok(_) => true,
        Err(ureq::Error::StatusCode(code)) if code == 403 || code == 404 => false,
        Err(_) => false,
    }
}

fn build_grib_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    Ok(match request.model {
        ModelId::Hrrr => build_hrrr_url(source, request),
        ModelId::HrrrAk => build_hrrr_ak_url(source, request),
        ModelId::Gfs => build_gfs_url(source, request)?,
        ModelId::Gdas => build_gdas_url(source, request)?,
        ModelId::Gefs => build_gefs_url(source, request)?,
        ModelId::Aigfs => build_aigfs_url(source, request)?,
        ModelId::Aigefs => build_aigefs_url(source, request)?,
        ModelId::Hgefs => build_hgefs_url(source, request)?,
        ModelId::EcmwfOpenData => build_ecmwf_url(source, request)?,
        ModelId::Aifs => build_aifs_url(source, request)?,
        ModelId::Rap => build_rap_url(source, request)?,
        ModelId::Nam => build_nam_url(source, request)?,
        ModelId::Hiresw => build_hiresw_url(source, request)?,
        ModelId::Href => build_href_url(source, request)?,
        ModelId::Sref => build_sref_url(source, request)?,
        ModelId::Rtma => build_rtma_url(source, request)?,
        ModelId::Urma => build_urma_url(source, request)?,
        ModelId::Nbm => build_nbm_url(source, request)?,
        ModelId::RrfsA => build_rrfs_a_url(source, request)?,
        ModelId::RrfsPublic => build_rrfs_public_url(source, request)?,
        ModelId::RrfsFireWx => build_rrfs_firewx_url(source, request)?,
        ModelId::WrfGdex => build_wrf_gdex_url(source, request)?,
    })
}

enum WrfGdexProduct {
    LegacyWrfout {
        dataset: String,
        domain: String,
    },
    ClimateArchive {
        dataset: String,
        branch: &'static str,
        filename_prefix: &'static str,
        cadence_hours: Option<u8>,
    },
}

fn parse_wrf_gdex_product(product: &str) -> Option<WrfGdexProduct> {
    let token = normalize_token(product);
    if let Some((dataset, suffix)) = token.split_once('_') {
        if dataset.starts_with('d')
            && dataset.len() == 7
            && dataset[1..].chars().all(|ch| ch.is_ascii_digit())
        {
            if suffix.starts_with('d')
                && suffix.len() == 3
                && suffix[1..].chars().all(|ch| ch.is_ascii_digit())
            {
                return Some(WrfGdexProduct::LegacyWrfout {
                    dataset: dataset.to_string(),
                    domain: suffix.to_string(),
                });
            }
            let climate = match suffix {
                "hist2d" => Some(("hist2D", "wrf2d_d01", None)),
                "hist3d" => Some(("hist3D", "wrf3d_d01", Some(3))),
                "future2d" => Some(("future2D", "wrf2d_d01", None)),
                "future3d" => Some(("future3D", "wrf3d_d01", Some(3))),
                _ => None,
            };
            if let Some((branch, filename_prefix, cadence_hours)) = climate {
                return Some(WrfGdexProduct::ClimateArchive {
                    dataset: dataset.to_string(),
                    branch,
                    filename_prefix,
                    cadence_hours,
                });
            }
        }
    }
    if token == "d612005" {
        return Some(WrfGdexProduct::ClimateArchive {
            dataset: token,
            branch: "hist2D",
            filename_prefix: "wrf2d_d01",
            cadence_hours: None,
        });
    }
    if token.starts_with('d')
        && token.len() == 7
        && token[1..].chars().all(|ch| ch.is_ascii_digit())
    {
        return Some(WrfGdexProduct::LegacyWrfout {
            dataset: token,
            domain: "d01".to_string(),
        });
    }
    None
}

fn build_wrf_gdex_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Gdex {
        return Ok(unsupported_source(source, request.model));
    }
    let product =
        parse_wrf_gdex_product(&request.product).ok_or_else(|| ModelError::UnsupportedProduct {
            model: request.model,
            product: request.product.clone(),
        })?;
    let (valid_date, valid_hour) = advance_yyyymmddhh(
        &request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        request.forecast_hour,
    )
    .ok_or_else(|| ModelError::UnsupportedForecastHour {
        model: request.model,
        cycle_hour: request.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        reason: "wrfout archive timestamp rollover exceeded the supported date arithmetic path"
            .to_string(),
    })?;
    let year = &valid_date[..4];
    let month = &valid_date[4..6];
    let day = &valid_date[6..8];
    match product {
        WrfGdexProduct::LegacyWrfout { dataset, domain } => Ok(format!(
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/{dataset}/{year}{month}/wrfout_{domain}_{year}-{month}-{day}_{:02}:00:00.nc",
            valid_hour
        )),
        WrfGdexProduct::ClimateArchive {
            dataset,
            branch,
            filename_prefix,
            cadence_hours,
        } => {
            if let Some(cadence) = cadence_hours {
                if valid_hour % cadence != 0 {
                    return Err(ModelError::UnsupportedForecastHour {
                        model: request.model,
                        cycle_hour: request.cycle.hour_utc,
                        forecast_hour: request.forecast_hour,
                        reason: format!(
                            "WRF GDEX product '{branch}' is published every {cadence} hours; requested valid time {:02}Z is off cadence",
                            valid_hour
                        ),
                    });
                }
            }
            Ok(format!(
                "https://tds.gdex.ucar.edu/thredds/fileServer/files/g/{dataset}/{branch}/{year}{month}/{filename_prefix}_{year}-{month}-{day}_{:02}:00:00.nc",
                valid_hour
            ))
        }
    }
}

fn build_hrrr_url(source: SourceId, request: &ModelRunRequest) -> String {
    let product_code = match normalize_token(&request.product).as_str() {
        "sfc" | "surface" => "wrfsfc",
        "prs" | "pressure" => "wrfprs",
        "nat" | "native" => "wrfnat",
        "subh" | "subhourly" => "wrfsubh",
        _ => "wrfsfc",
    };

    match source {
        SourceId::Aws => format!(
            "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{}/conus/hrrr.t{:02}z.{}f{:02}.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{}/conus/hrrr.t{:02}z.{}f{:02}.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        SourceId::Google => format!(
            "https://storage.googleapis.com/high-resolution-rapid-refresh/hrrr.{}/conus/hrrr.t{:02}z.{}f{:02}.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        SourceId::Azure => format!(
            "https://noaahrrr.blob.core.windows.net/hrrr/hrrr.{}/conus/hrrr.t{:02}z.{}f{:02}.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        other => unsupported_source(other, request.model),
    }
}

fn build_hrrr_ak_url(source: SourceId, request: &ModelRunRequest) -> String {
    let product_code = match normalize_token(&request.product).as_str() {
        "sfc" | "surface" => "wrfsfc",
        "prs" | "pressure" => "wrfprs",
        "nat" | "native" => "wrfnat",
        "subh" | "subhourly" => "wrfsubh",
        _ => "wrfsfc",
    };

    match source {
        SourceId::Aws => format!(
            "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{}/alaska/hrrr.t{:02}z.{}f{:02}.ak.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{}/alaska/hrrr.t{:02}z.{}f{:02}.ak.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        SourceId::Google => format!(
            "https://storage.googleapis.com/high-resolution-rapid-refresh/hrrr.{}/alaska/hrrr.t{:02}z.{}f{:02}.ak.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        SourceId::Azure => format!(
            "https://noaahrrr.blob.core.windows.net/hrrr/hrrr.{}/alaska/hrrr.t{:02}z.{}f{:02}.ak.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product_code,
            request.forecast_hour
        ),
        other => unsupported_source(other, request.model),
    }
}

fn gfs_product_filename_part(product: &str) -> Result<GfsProductPart, ModelError> {
    let token = normalize_token(product);
    let part = match token.as_str() {
        "pgrb2_0p25" | "pgrb2_0_25" | "0p25" | "0_25" | "hourly" | "pgrb2" => {
            GfsProductPart::Forecast("pgrb2.0p25")
        }
        "pgrb2_0p50" | "pgrb2_0_50" | "0p50" | "0_50" => GfsProductPart::Forecast("pgrb2.0p50"),
        "pgrb2_1p00" | "pgrb2_1_00" | "1p00" | "1_00" => GfsProductPart::Forecast("pgrb2.1p00"),
        "pgrb2b_0p25" | "pgrb2b_0_25" | "secondary" | "secondary_params" => {
            GfsProductPart::Forecast("pgrb2b.0p25")
        }
        "sflux" | "sfluxgrb" | "sfluxgrbf" => GfsProductPart::Sflux,
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: ModelId::Gfs,
                product: other.to_string(),
            });
        }
    };
    Ok(part)
}

enum GfsProductPart {
    Forecast(&'static str),
    Sflux,
}

fn build_gfs_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    let filename = match gfs_product_filename_part(&request.product) {
        Ok(GfsProductPart::Forecast(part)) => format!(
            "gfs.t{:02}z.{}.f{:03}",
            request.cycle.hour_utc, part, request.forecast_hour
        ),
        Ok(GfsProductPart::Sflux) => format!(
            "gfs.t{:02}z.sfluxgrbf{:03}.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        Err(err) => return Err(err),
    };
    Ok(match source {
        SourceId::Aws => format!(
            "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{}/{:02}/atmos/{}",
            request.cycle.date_yyyymmdd, request.cycle.hour_utc, filename
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{}/{:02}/atmos/{}",
            request.cycle.date_yyyymmdd, request.cycle.hour_utc, filename
        ),
        SourceId::Google => format!(
            "https://storage.googleapis.com/global-forecast-system/gfs.{}/{:02}/atmos/{}",
            request.cycle.date_yyyymmdd, request.cycle.hour_utc, filename
        ),
        SourceId::Ncei => {
            let year = &request.cycle.date_yyyymmdd[..4];
            let month = &request.cycle.date_yyyymmdd[4..6];
            let day = &request.cycle.date_yyyymmdd[6..8];
            format!(
                "https://www.ncei.noaa.gov/data/global-forecast-system/access/grid-004-0.5-degree/analysis/{}{}/{}{}{}/gfs_4_{}{}{}_{}00_{:03}.grb2",
                year,
                month,
                year,
                month,
                day,
                year,
                month,
                day,
                format_args!("{:02}", request.cycle.hour_utc),
                request.forecast_hour
            )
        }
        other => unsupported_source(other, request.model),
    })
}

fn build_gdas_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if !forecast_hour_supported(request.model, request.cycle.hour_utc, request.forecast_hour) {
        return Err(ModelError::UnsupportedForecastHour {
            model: request.model,
            cycle_hour: request.cycle.hour_utc,
            forecast_hour: request.forecast_hour,
            reason: "GDAS pgrb2 fields are wired for f000 through f009 in this v0.5 path"
                .to_string(),
        });
    }
    let product = match normalize_token(&request.product).as_str() {
        "pgrb2_0p25" | "pgrb2_0_25" | "0p25" | "0_25" | "pgrb2" => "pgrb2.0p25",
        "pgrb2_1p00" | "pgrb2_1_00" | "1p00" | "1_00" => "pgrb2.1p00",
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(match source {
        SourceId::Aws => format!(
            "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gdas.{}/{:02}/atmos/gdas.t{:02}z.{}.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            request.cycle.hour_utc,
            product,
            request.forecast_hour
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gdas.{}/{:02}/atmos/gdas.t{:02}z.{}.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            request.cycle.hour_utc,
            product,
            request.forecast_hour
        ),
        other => unsupported_source(other, request.model),
    })
}

fn build_gefs_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if !forecast_hour_supported(request.model, request.cycle.hour_utc, request.forecast_hour) {
        return Err(ModelError::UnsupportedForecastHour {
            model: request.model,
            cycle_hour: request.cycle.hour_utc,
            forecast_hour: request.forecast_hour,
            reason: "GEFS pgrb2ap5 fields are expected every 3 hours through f384".to_string(),
        });
    }
    let product = gefs_product_from_product(&request.product)?;
    Ok(match source {
        SourceId::Aws => format!(
            "https://noaa-gefs-pds.s3.amazonaws.com/gefs.{}/{:02}/atmos/{}/{}.t{:02}z.{}.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product.directory,
            product.member_or_stat,
            request.cycle.hour_utc,
            product.file_product,
            request.forecast_hour
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.{}/{:02}/atmos/{}/{}.t{:02}z.{}.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            product.directory,
            product.member_or_stat,
            request.cycle.hour_utc,
            product.file_product,
            request.forecast_hour
        ),
        other => unsupported_source(other, request.model),
    })
}

struct GefsProduct {
    directory: &'static str,
    file_product: &'static str,
    member_or_stat: String,
}

fn gefs_product_from_product(product: &str) -> Result<GefsProduct, ModelError> {
    let token = normalize_token(product);
    let (directory, file_product) =
        if token.contains("pgrb2bp5") || token.contains("pgrb2b") || token.contains("secondary") {
            ("pgrb2bp5", "pgrb2b.0p50")
        } else if token.contains("pgrb2sp25")
            || token.contains("pgrb2s")
            || token.contains("0p25")
            || token.contains("0_25")
        {
            ("pgrb2sp25", "pgrb2s.0p25")
        } else {
            ("pgrb2ap5", "pgrb2a.0p50")
        };
    let candidate = token
        .split(['_', '/'])
        .find(|part| {
            *part == "gec00"
                || *part == "geavg"
                || *part == "gespr"
                || (part.len() == 5
                    && part.starts_with("gep")
                    && part[3..].chars().all(|ch| ch.is_ascii_digit()))
        })
        .unwrap_or(if directory == "pgrb2sp25" {
            "geavg"
        } else {
            "gec00"
        });
    Ok(GefsProduct {
        directory,
        file_product,
        member_or_stat: candidate.to_string(),
    })
}

fn build_ecmwf_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Ecmwf {
        return Ok(unsupported_source(source, request.model));
    }
    if !forecast_hour_supported(request.model, request.cycle.hour_utc, request.forecast_hour) {
        let reason = match request.cycle.hour_utc {
            0 | 12 => {
                "00/12z open-data runs expose 3-hourly steps to 144h, then 6-hourly steps to 360h"
            }
            6 | 18 => "06/18z open-data runs expose 3-hourly steps to 144h only",
            _ => "ECMWF open-data runs are currently expected at 00/06/12/18 UTC",
        };
        return Err(ModelError::UnsupportedForecastHour {
            model: request.model,
            cycle_hour: request.cycle.hour_utc,
            forecast_hour: request.forecast_hour,
            reason: reason.to_string(),
        });
    }
    let stream = match normalize_token(&request.product).as_str() {
        "oper" | "hres" => "oper",
        "ens" | "enfo" | "ensemble" => "enfo",
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(format!(
        "https://data.ecmwf.int/forecasts/{}/{:02}z/ifs/0p25/{}/{}{:02}0000-{}h-{}-fc.grib2",
        request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        stream,
        request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        request.forecast_hour,
        stream
    ))
}

fn ecmwf_open_data_forecast_hour_supported(cycle_hour_utc: u8, forecast_hour: u16) -> bool {
    match cycle_hour_utc {
        0 | 12 => {
            (forecast_hour <= 144 && forecast_hour % 3 == 0)
                || (forecast_hour > 144 && forecast_hour <= 360 && forecast_hour % 6 == 0)
        }
        6 | 18 => forecast_hour <= 144 && forecast_hour % 3 == 0,
        _ => false,
    }
}

fn build_aifs_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    match source {
        SourceId::Earth2Archive => Ok(format!(
            "earth2-archive://aifs/{}T{:02}Z/lead{:03}.nc",
            request.cycle.date_yyyymmdd, request.cycle.hour_utc, request.forecast_hour
        )),
        SourceId::Ecmwf => {
            if !ecmwf_open_data_forecast_hour_supported(
                request.cycle.hour_utc,
                request.forecast_hour,
            ) {
                return Err(ModelError::UnsupportedForecastHour {
                    model: request.model,
                    cycle_hour: request.cycle.hour_utc,
                    forecast_hour: request.forecast_hour,
                    reason: "AIFS-Single open data follows the ECMWF open-data step cadence; use Earth2Archive for experimental multi-year AIFS runs".to_string(),
                });
            }
            let stream = match normalize_token(&request.product).as_str() {
                "oper" | "hres" | "aifs" | "aifs_single" => "oper",
                other => {
                    return Err(ModelError::UnsupportedProduct {
                        model: request.model,
                        product: other.to_string(),
                    });
                }
            };
            Ok(format!(
                "https://data.ecmwf.int/forecasts/{}/{:02}z/aifs-single/0p25/{}/{}{:02}0000-{}h-{}-fc.grib2",
                request.cycle.date_yyyymmdd,
                request.cycle.hour_utc,
                stream,
                request.cycle.date_yyyymmdd,
                request.cycle.hour_utc,
                request.forecast_hour,
                stream
            ))
        }
        other => Ok(unsupported_source(other, request.model)),
    }
}

fn build_aigfs_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let family = match normalize_token(&request.product).as_str() {
        "sfc" | "surface" => "sfc",
        "pres" | "pressure" | "pgrb" | "pgrb2" => "pres",
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigfs/prod/aigfs.{}/{:02}/model/atmos/grib2/aigfs.t{:02}z.{}.f{:03}.grib2",
        request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        request.cycle.hour_utc,
        family,
        request.forecast_hour
    ))
}

fn build_aigefs_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let token = normalize_token(&request.product);
    let family = if token.contains("pres") || token.contains("pressure") {
        "pres"
    } else {
        "sfc"
    };
    let stat = token
        .split(['_', '/'])
        .find(|part| matches!(*part, "avg" | "spr" | "p10" | "p50" | "p90"))
        .unwrap_or("avg");
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigefs/prod/aigefs.{}/{:02}/ensstat/products/atmos/grib2/aigefs.t{:02}z.{}.{}.f{:03}.grib2",
        request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        request.cycle.hour_utc,
        family,
        stat,
        request.forecast_hour
    ))
}

fn build_hgefs_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let token = normalize_token(&request.product);
    let family = if token.contains("pres") || token.contains("pressure") {
        "pres"
    } else {
        "sfc"
    };
    let stat = token
        .split(['_', '/'])
        .find(|part| matches!(*part, "avg" | "spr" | "p10" | "p50" | "p90"))
        .unwrap_or("avg");
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hgefs/prod/hgefs.{}/{:02}/ensstat/products/atmos/grib2/hgefs.t{:02}z.{}.{}.f{:03}.grib2",
        request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        request.cycle.hour_utc,
        family,
        stat,
        request.forecast_hour
    ))
}

fn build_rap_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    if !forecast_hour_supported(request.model, request.cycle.hour_utc, request.forecast_hour) {
        return Err(ModelError::UnsupportedForecastHour {
            model: request.model,
            cycle_hour: request.cycle.hour_utc,
            forecast_hour: request.forecast_hour,
            reason: "RAP publishes f000 through f021 on most cycles and f000 through f051 on 03/09/15/21z cycles"
                .to_string(),
        });
    }
    let prefix = match normalize_token(&request.product).as_str() {
        "awp130pgrb" | "pgrb" | "prs" | "pressure" | "conus" => "awp130pgrb",
        "awp130bgrb" | "bgrb" | "secondary" => "awp130bgrb",
        "awip32" | "na32" | "north_america_32km" => "awip32",
        "wrfprs" | "wrf_prs" => "wrfprs",
        "wrfsfc" | "sfc" | "surface" => "wrfsfc",
        "wrfnat" | "nat" | "native" => "wrfnat",
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.{}/rap.t{:02}z.{}f{:02}.grib2",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, prefix, request.forecast_hour
    ))
}

fn build_nam_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let filename = match normalize_token(&request.product).as_str() {
        "awip12" | "conus" | "conus12" | "conus_12km" => format!(
            "nam.t{:02}z.awip12{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        "awip32" | "na32" | "north_america" | "north_america_32km" => format!(
            "nam.t{:02}z.awip32{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        "awip3d" | "3d" | "pressure" | "prs" => format!(
            "nam.t{:02}z.awip3d{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        "conusnest" | "nest_conus" | "namnest_conus" => format!(
            "nam.t{:02}z.conusnest.hiresf{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        "alaskanest" | "nest_alaska" | "aknest" => format!(
            "nam.t{:02}z.alaskanest.hiresf{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        "hawaiinest" | "nest_hawaii" | "hinest" => format!(
            "nam.t{:02}z.hawaiinest.hiresf{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        "priconest" | "nest_pr" | "prnest" => format!(
            "nam.t{:02}z.priconest.hiresf{:02}.tm00.grib2",
            request.cycle.hour_utc, request.forecast_hour
        ),
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.{}/{}",
        request.cycle.date_yyyymmdd, filename
    ))
}

fn build_hiresw_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let token = normalize_token(&request.product);
    let core = token
        .split('/')
        .next()
        .unwrap_or("arw_2p5km")
        .replace("__", "_");
    let core = match core.as_str() {
        "arw" | "arw_2p5km" | "arw_2_5km" => "arw_2p5km",
        "fv3" | "fv3_2p5km" | "fv3_2_5km" => "fv3_2p5km",
        "arw_mem2" | "mem2arw" | "arwmem2" => "arw_mem2_2p5km",
        other => other,
    };
    let domain = token
        .split(['_', '/'])
        .find(|part| {
            matches!(
                *part,
                "conus" | "ak" | "alaska" | "hi" | "hawaii" | "guam" | "pr"
            )
        })
        .unwrap_or("conus");
    let domain = match domain {
        "alaska" => "ak",
        "hawaii" => "hi",
        other => other,
    };
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hiresw/prod/hiresw.{}/hiresw.t{:02}z.{}.f{:02}.{}.grib2",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, core, request.forecast_hour, domain
    ))
}

fn build_href_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    if !forecast_hour_supported(request.model, request.cycle.hour_utc, request.forecast_hour) {
        return Err(ModelError::UnsupportedForecastHour {
            model: request.model,
            cycle_hour: request.cycle.hour_utc,
            forecast_hour: request.forecast_hour,
            reason: "HREF CONUS ensprod files are expected for f01 through f48".to_string(),
        });
    }
    let token = normalize_token(&request.product);
    let product = token
        .split(['_', '/'])
        .find(|part| {
            matches!(
                *part,
                "avrg" | "eas" | "ffri" | "lpmm" | "mean" | "pmmn" | "prob" | "sprd"
            )
        })
        .unwrap_or("sprd");
    let domain = token
        .split(['_', '/'])
        .find(|part| matches!(*part, "conus"))
        .unwrap_or("conus");
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/href/prod/href.{}/ensprod/href.t{:02}z.{}.{}.f{:02}.grib2",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, domain, product, request.forecast_hour
    ))
}

fn build_sref_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let token = normalize_token(&request.product);
    if token.starts_with("ensprod") || token.contains("mean") || token.contains("spread") {
        let grid = token
            .split(['_', '/'])
            .find(|part| part.starts_with("pgrb"))
            .unwrap_or("pgrb212");
        let stat = token
            .split(['_', '/'])
            .find(|part| {
                matches!(
                    *part,
                    "mean"
                        | "max"
                        | "min"
                        | "p10"
                        | "p25"
                        | "p50"
                        | "p75"
                        | "p90"
                        | "spread"
                        | "prob"
                )
            })
            .unwrap_or("mean");
        let cadence = if token.contains("1hrly") {
            "1hrly"
        } else {
            "3hrly"
        };
        return Ok(format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/sref/prod/sref.{}/{:02}/ensprod/sref.t{:02}z.{}.{}_{}.grib2",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            request.cycle.hour_utc,
            grid,
            stat,
            cadence
        ));
    }
    let member_family = token
        .split(['_', '/'])
        .find(|part| matches!(*part, "arw" | "nmb"))
        .unwrap_or("arw");
    let member = token
        .split(['_', '/'])
        .find(|part| *part == "ctl" || part.starts_with('p') || part.starts_with('n'))
        .unwrap_or("ctl");
    let grid = token
        .split(['_', '/'])
        .find(|part| part.starts_with("pgrb"))
        .unwrap_or("pgrb132");
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/sref/prod/sref.{}/{:02}/pgrb/sref_{}.t{:02}z.{}.{}.f{:02}.grib2",
        request.cycle.date_yyyymmdd,
        request.cycle.hour_utc,
        member_family,
        request.cycle.hour_utc,
        grid,
        member,
        request.forecast_hour
    ))
}

fn build_rtma_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let product = match normalize_token(&request.product).as_str() {
        "2dvaranl_ndfd" | "anl" | "analysis" => "2dvaranl_ndfd",
        "2dvarges_ndfd" | "ges" | "guess" => "2dvarges_ndfd",
        "2dvarerr_ndfd" | "err" | "error" => "2dvarerr_ndfd",
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.{}/rtma2p5.t{:02}z.{}.grb2_wexp",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, product
    ))
}

fn build_urma_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Nomads {
        return Ok(unsupported_source(source, request.model));
    }
    let product = match normalize_token(&request.product).as_str() {
        "2dvaranl_ndfd" | "anl" | "analysis" => "2dvaranl_ndfd",
        "2dvarges_ndfd" | "ges" | "guess" => "2dvarges_ndfd",
        "2dvarerr_ndfd" | "err" | "error" => "2dvarerr_ndfd",
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };
    Ok(format!(
        "https://nomads.ncep.noaa.gov/pub/data/nccf/com/urma/prod/urma2p5.{}/urma2p5.t{:02}z.{}.grb2_wexp",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, product
    ))
}

fn build_nbm_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    let token = normalize_token(&request.product);
    let stream = token
        .split(['_', '/'])
        .find(|part| {
            matches!(
                *part,
                "core" | "qmd" | "ak" | "co" | "hi" | "pr" | "gu" | "oc" | "global"
            )
        })
        .unwrap_or("core");
    let domain = token
        .split(['_', '/'])
        .find(|part| {
            matches!(
                *part,
                "ak" | "co" | "conus" | "hi" | "hawaii" | "pr" | "gu" | "guam" | "oc" | "global"
            )
        })
        .unwrap_or("co");
    let domain = match domain {
        "conus" => "co",
        "hawaii" => "hi",
        "guam" => "gu",
        other => other,
    };
    let filename = format!(
        "blend.t{:02}z.{}.f{:03}.{}.grib2",
        request.cycle.hour_utc, stream, request.forecast_hour, domain
    );
    Ok(match source {
        SourceId::Aws => format!(
            "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{}/{:02}/{}/{}",
            request.cycle.date_yyyymmdd, request.cycle.hour_utc, stream, filename
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.{}/{:02}/{}/{}",
            request.cycle.date_yyyymmdd, request.cycle.hour_utc, stream, filename
        ),
        other => unsupported_source(other, request.model),
    })
}

fn build_rrfs_a_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Aws {
        return Ok(unsupported_source(source, request.model));
    }

    let suffix = match normalize_token(&request.product).as_str() {
        "prs_conus" | "prslev_conus" | "conus" => {
            format!("prslev.3km.f{:03}.conus.grib2", request.forecast_hour)
        }
        "prs_na" | "prslev_na" | "na" => {
            format!("prslev.3km.f{:03}.na.grib2", request.forecast_hour)
        }
        "prs_ak" | "prslev_ak" | "ak" => {
            format!("prslev.3km.f{:03}.ak.grib2", request.forecast_hour)
        }
        "prs_hi" | "prslev_hi" | "hi" => {
            format!("prslev.2p5km.f{:03}.hi.grib2", request.forecast_hour)
        }
        "prs_pr" | "prslev_pr" | "pr" => {
            format!("prslev.2p5km.f{:03}.pr.grib2", request.forecast_hour)
        }
        "subh_hi" | "prs_subh_hi" | "prslev_subh_hi" => {
            format!("prslev.2p5km.subh.f{:03}.hi.grib2", request.forecast_hour)
        }
        "subh_pr" | "prs_subh_pr" | "prslev_subh_pr" => {
            format!("prslev.2p5km.subh.f{:03}.pr.grib2", request.forecast_hour)
        }
        "nat_na" | "natlev_na" => format!("natlev.3km.f{:03}.na.grib2", request.forecast_hour),
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };

    Ok(format!(
        "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.{}/{:02}/rrfs.t{:02}z.{}",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, request.cycle.hour_utc, suffix
    ))
}

fn build_rrfs_public_url(
    source: SourceId,
    request: &ModelRunRequest,
) -> Result<String, ModelError> {
    if source != SourceId::Aws {
        return Ok(unsupported_source(source, request.model));
    }

    let suffix = match normalize_token(&request.product).as_str() {
        "prs_conus" | "prslev_conus" | "conus" => {
            format!("prslev.3km.f{:03}.conus.grib2", request.forecast_hour)
        }
        "2dfld_conus" | "sfc_conus" | "surface_conus" | "surface" | "sfc" => {
            format!("2dfld.3km.f{:03}.conus.grib2", request.forecast_hour)
        }
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };

    Ok(format!(
        "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/rrfs.{}/{:02}/rrfs.t{:02}z.{}",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, request.cycle.hour_utc, suffix
    ))
}

fn build_rrfs_firewx_url(
    source: SourceId,
    request: &ModelRunRequest,
) -> Result<String, ModelError> {
    if source != SourceId::Aws {
        return Ok(unsupported_source(source, request.model));
    }

    let suffix = match normalize_token(&request.product).as_str() {
        "prs_firewx" | "prslev_firewx" | "firewx_prs" | "firewx_pressure" | "pressure" => {
            format!(
                "prslev.1p5km.f{:03}.firewx_lcc.grib2",
                request.forecast_hour
            )
        }
        "2dfld_firewx" | "firewx" | "firewx_sfc" | "firewx_surface" | "surface" | "sfc" => {
            format!("2dfld.1p5km.f{:03}.firewx_lcc.grib2", request.forecast_hour)
        }
        other => {
            return Err(ModelError::UnsupportedProduct {
                model: request.model,
                product: other.to_string(),
            });
        }
    };

    Ok(format!(
        "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/firewx.{}/{:02}/rrfs.t{:02}z.{}",
        request.cycle.date_yyyymmdd, request.cycle.hour_utc, request.cycle.hour_utc, suffix
    ))
}

fn normalize_token(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace(['-', ' ', '.'], "_")
}

fn canonical_recipe_token(value: &str) -> String {
    let normalized = normalize_token(value);
    match normalized.as_str() {
        "500mb_vorticity_height_winds" => "500mb_absolute_vorticity_height_winds".to_string(),
        "700mb_vorticity_height_winds" => "700mb_absolute_vorticity_height_winds".to_string(),
        "850mb_vorticity_height_winds" => "850mb_absolute_vorticity_height_winds".to_string(),
        _ => normalized,
    }
}

fn plot_recipe_fetch_plan_for(
    recipe: &'static PlotRecipe,
    model: ModelId,
) -> Result<PlotRecipeFetchPlan, ModelError> {
    let fields = collect_recipe_fields(recipe, model);
    let blockers = plot_recipe_fetch_blockers_for_fields(&fields, model);
    if !blockers.is_empty() {
        return Err(ModelError::UnsupportedPlotRecipeModel {
            recipe: recipe.slug,
            model,
            reason: summarize_plot_recipe_blockers(&blockers),
        });
    }

    let (product, fetch_policy) = plot_recipe_fetch_defaults(model, &fields);

    Ok(PlotRecipeFetchPlan {
        recipe_slug: recipe.slug,
        model,
        product,
        fetch_policy,
        fetch_mode: fetch_policy.fetch_mode(),
        fields,
    })
}

fn plot_recipe_fetch_blockers_for(
    recipe: &'static PlotRecipe,
    model: ModelId,
) -> Vec<PlotRecipeBlocker> {
    let fields = collect_recipe_fields(recipe, model);
    plot_recipe_fetch_blockers_for_fields(&fields, model)
}

fn plot_recipe_fetch_blockers_for_fields(
    fields: &[&'static GribFieldSpec],
    model: ModelId,
) -> Vec<PlotRecipeBlocker> {
    fields
        .iter()
        .copied()
        .filter_map(|field| plot_recipe_field_blocker(field, model))
        .collect()
}

fn plot_recipe_field_blocker(
    field: &'static GribFieldSpec,
    model: ModelId,
) -> Option<PlotRecipeBlocker> {
    if field.family == ProductFamily::Native {
        if let Some(reason) = native_field_gap_reason(field, model) {
            return Some(PlotRecipeBlocker {
                field_key: field.key,
                field_label: field.label,
                reason,
            });
        }

        let reason = match field.selector {
            Some(selector) if selector_supported_for_model(selector, model) => return None,
            Some(selector) => unsupported_selector_reason(selector, model),
            None => field_selector_gap_reason(field).to_string(),
        };

        return Some(PlotRecipeBlocker {
            field_key: field.key,
            field_label: field.label,
            reason,
        });
    }

    if field.family == ProductFamily::Pressure {
        if let Some(reason) = model_specific_pressure_field_gap(field, model) {
            return Some(PlotRecipeBlocker {
                field_key: field.key,
                field_label: field.label,
                reason,
            });
        }
    }

    if field.family == ProductFamily::Surface {
        if let Some(reason) = model_specific_surface_field_gap(field, model) {
            return Some(PlotRecipeBlocker {
                field_key: field.key,
                field_label: field.label,
                reason,
            });
        }
    }

    let reason = match field.selector {
        Some(selector) if selector_supported_for_model(selector, model) => return None,
        Some(selector) => unsupported_selector_reason(selector, model),
        None => field_selector_gap_reason(field).to_string(),
    };

    Some(PlotRecipeBlocker {
        field_key: field.key,
        field_label: field.label,
        reason,
    })
}

fn plot_recipe_fetch_defaults(
    model: ModelId,
    fields: &[&'static GribFieldSpec],
) -> (&'static str, PlotRecipeFetchPolicy) {
    if let Some(product) = wrf_gdex_recipe_product_override(model, fields) {
        return (product, PlotRecipeFetchPolicy::WholeFile);
    }
    let has_native = fields
        .iter()
        .any(|field| field.family == ProductFamily::Native);
    let has_surface = fields
        .iter()
        .any(|field| field.family == ProductFamily::Surface);
    let has_product_selector = fields.iter().any(|field| {
        field
            .selector
            .is_some_and(|selector| !selector.product.is_default())
    });
    let has_probability_selector = fields.iter().any(|field| {
        field
            .selector
            .is_some_and(|selector| matches!(selector.product, FieldProduct::Probability(_)))
    });
    let has_ensemble_spread_selector = fields.iter().any(|field| {
        field.selector.is_some_and(|selector| {
            matches!(
                selector.product,
                FieldProduct::EnsembleStandardDeviation | FieldProduct::EnsembleSpread
            )
        })
    });
    let has_ensemble_mean_selector = fields.iter().any(|field| {
        field
            .selector
            .is_some_and(|selector| matches!(selector.product, FieldProduct::EnsembleMean))
    });
    match (model, has_native, has_surface) {
        (ModelId::Hrrr, true, _) => ("nat", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Hrrr, false, true) => ("sfc", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Hrrr, false, false) => ("prs", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::HrrrAk, true, _) => ("nat", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::HrrrAk, false, true) => ("sfc", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::HrrrAk, false, false) => ("prs", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Gfs, _, _) => ("pgrb2.0p25", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Gdas, _, _) => ("pgrb2.0p25", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Gefs, _, _) if has_ensemble_spread_selector => {
            ("pgrb2ap5/gespr", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Gefs, _, _) if has_ensemble_mean_selector => {
            ("pgrb2ap5/geavg", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Gefs, _, _) => ("pgrb2ap5/gec00", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Aigfs, _, true) => ("sfc", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Aigfs, _, false) => ("pres", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Aigefs, _, true) if has_ensemble_spread_selector => {
            ("sfc/spr", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Aigefs, _, false) if has_ensemble_spread_selector => {
            ("pres/spr", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Aigefs, _, true) => ("sfc/avg", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Aigefs, _, false) => ("pres/avg", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Hgefs, _, true) if has_ensemble_spread_selector => {
            ("sfc/spr", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Hgefs, _, false) if has_ensemble_spread_selector => {
            ("pres/spr", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Hgefs, _, true) => ("sfc/avg", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Hgefs, _, false) => ("pres/avg", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Rap, _, _) => ("awp130pgrb", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Nam, _, _) => ("awip12", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Hiresw, _, _) => (
            "arw_2p5km/conus",
            PlotRecipeFetchPolicy::PreferIndexedSubset,
        ),
        (ModelId::Href, _, _) if has_ensemble_spread_selector => (
            "ensprod/conus/sprd",
            PlotRecipeFetchPolicy::PreferIndexedSubset,
        ),
        (ModelId::Href, _, _) => (
            "ensprod/conus/sprd",
            PlotRecipeFetchPolicy::PreferIndexedSubset,
        ),
        (ModelId::Sref, _, _) if has_probability_selector => (
            "ensprod/pgrb212/prob_3hrly",
            PlotRecipeFetchPolicy::PreferIndexedSubset,
        ),
        (ModelId::Sref, _, _) => (
            "arw/ctl/pgrb132",
            PlotRecipeFetchPolicy::PreferIndexedSubset,
        ),
        (ModelId::Rtma, _, _) => ("2dvaranl_ndfd", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Urma, _, _) => ("2dvaranl_ndfd", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::Nbm, _, _) if has_product_selector => {
            ("qmd/co", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::Nbm, _, _) => ("core/co", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::RrfsA, _, _) => ("prs-conus", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::RrfsPublic, true, _) => {
            ("2dfld-conus", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::RrfsPublic, false, true) => {
            ("2dfld-conus", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::RrfsPublic, false, false) => {
            ("prs-conus", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::RrfsFireWx, true, _) => {
            ("2dfld-firewx", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::RrfsFireWx, false, true) => {
            ("2dfld-firewx", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::RrfsFireWx, false, false) => {
            ("prs-firewx", PlotRecipeFetchPolicy::PreferIndexedSubset)
        }
        (ModelId::EcmwfOpenData, _, _) => ("oper", PlotRecipeFetchPolicy::WholeFile),
        (ModelId::Aifs, _, _) => ("oper", PlotRecipeFetchPolicy::WholeFile),
        (ModelId::WrfGdex, true, _) => (
            WRF_GDEX_DEFAULT_PRESSURE_PRODUCT,
            PlotRecipeFetchPolicy::WholeFile,
        ),
        (ModelId::WrfGdex, false, true) => (
            WRF_GDEX_DEFAULT_SURFACE_PRODUCT,
            PlotRecipeFetchPolicy::WholeFile,
        ),
        (ModelId::WrfGdex, false, false) => (
            WRF_GDEX_DEFAULT_PRESSURE_PRODUCT,
            PlotRecipeFetchPolicy::WholeFile,
        ),
    }
}

fn wrf_gdex_recipe_product_override(
    model: ModelId,
    fields: &[&'static GribFieldSpec],
) -> Option<&'static str> {
    if model != ModelId::WrfGdex {
        return None;
    }
    let all_native_surface_diagnostics = !fields.is_empty()
        && fields.iter().all(|field| {
            matches!(
                field.key,
                "composite_reflectivity" | "radar_reflectivity_1km_agl" | "updraft_helicity"
            )
        });
    if all_native_surface_diagnostics {
        return Some(WRF_GDEX_DEFAULT_SURFACE_PRODUCT);
    }
    let all_cloud_cover = !fields.is_empty()
        && fields.iter().all(|field| {
            matches!(
                field.key,
                "total_cloud_cover" | "low_cloud_cover" | "middle_cloud_cover" | "high_cloud_cover"
            )
        });
    if all_cloud_cover {
        return Some(WRF_GDEX_DEFAULT_PRESSURE_PRODUCT);
    }
    None
}

fn native_field_gap_reason(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (field.key, model) {
        (
            "composite_reflectivity" | "radar_reflectivity_1km_agl" | "updraft_helicity",
            ModelId::Gfs
            | ModelId::Gdas
            | ModelId::Gefs
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::EcmwfOpenData
            | ModelId::Aifs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Href
            | ModelId::Sref
            | ModelId::Rtma
            | ModelId::Urma
            | ModelId::Nbm,
        ) => Some(format!(
            "{} is not wired for model '{model}'; rustwx-models only has native convective product fetch planning for HRRR/RRFS-A right now",
            field.label
        )),
        (
            "smoke_mass_density_8m_agl" | "column_integrated_smoke",
            ModelId::Gfs
            | ModelId::Gdas
            | ModelId::Gefs
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::EcmwfOpenData
            | ModelId::Aifs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Href
            | ModelId::Sref
            | ModelId::Rtma
            | ModelId::Urma
            | ModelId::Nbm,
        ) => Some(format!(
            "{} is only verified and wired for HRRR wrfnat right now; the native smoke GRIB signature is not verified yet for model '{model}'",
            field.label
        )),
        (
            "simulated_infrared_brightness_temperature",
            ModelId::Gfs
            | ModelId::Gdas
            | ModelId::Gefs
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::EcmwfOpenData
            | ModelId::Aifs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Href
            | ModelId::Sref
            | ModelId::Rtma
            | ModelId::Urma
            | ModelId::Nbm
            | ModelId::RrfsA
            | ModelId::RrfsPublic
            | ModelId::RrfsFireWx,
        ) => Some(format!(
            "{} is only verified and wired for HRRR right now; the native GRIB signature is not verified yet for model '{model}'",
            field.label
        )),
        (
            "smoke_mass_density_8m_agl" | "column_integrated_smoke",
            ModelId::RrfsA | ModelId::RrfsPublic | ModelId::RrfsFireWx,
        ) => Some(format!(
            "{} is only verified and wired for HRRR wrfnat right now; the native GRIB signature is not verified yet for model '{model}'",
            field.label
        )),
        _ => None,
    }
}

fn model_specific_pressure_field_gap(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (model, field.key) {
        (ModelId::Href, key) if !key.starts_with("href_spread_") => Some(format!(
            "{} is not yet wired for HREF; HREF support is currently limited to explicit `href_sprd_*` ensprod spread recipes",
            field.label
        )),
        (model, key) if key.starts_with("href_spread_") && model != ModelId::Href => Some(format!(
            "{} is only verified for HREF ensprod spread fields right now; do not route this recipe through model '{model}'",
            field.label
        )),
        (model, key)
            if (key.starts_with("gefs_mean_") || key.starts_with("gefs_spread_"))
                && model != ModelId::Gefs =>
        {
            Some(format!(
                "{} is only verified for GEFS native geavg/gespr fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key) if key.starts_with("aigefs_spread_") && model != ModelId::Aigefs => {
            Some(format!(
                "{} is only verified for AI-GEFS ensstat spread fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key) if key.starts_with("hgefs_spread_") && model != ModelId::Hgefs => {
            Some(format!(
                "{} is only verified for HGEFS ensstat spread fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key) if key.starts_with("sref_probability_") && model != ModelId::Sref => {
            Some(format!(
                "{} is only verified for SREF ensprod probability fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (ModelId::Rtma | ModelId::Urma | ModelId::Nbm, _) => Some(format!(
            "{} requires an isobaric-pressure product; model '{model}' is currently wired only through surface/core grids in rustwx v0.5",
            field.label
        )),
        (ModelId::Gfs, "dewpoint_700mb" | "dewpoint_850mb") => Some(format!(
            "{} is not present in the GFS 0.25-degree pgrb2 file currently wired by rustwx-models; keep it blocked until a verified direct field or derived dewpoint path is implemented",
            field.label
        )),
        (
            ModelId::Gdas
            | ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Sref,
            "dewpoint_700mb" | "dewpoint_850mb",
        ) => Some(format!(
            "{} is not verified as a direct pressure-level dewpoint field for model '{model}'; use RH/TMP or add a model-specific derived dewpoint path",
            field.label
        )),
        (ModelId::Gefs, "dewpoint_700mb" | "dewpoint_850mb") => Some(format!(
            "{} is not present in the GEFS 0.5-degree pgrb2ap5 member files currently wired by rustwx-models; use RH/TMP or add derived dewpoint support for this model",
            field.label
        )),
        (ModelId::EcmwfOpenData, "dewpoint_700mb" | "dewpoint_850mb") => Some(format!(
            "{} is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models; use RH/TMP or add derived dewpoint support for this model",
            field.label
        )),
        (
            ModelId::EcmwfOpenData,
            "absolute_vorticity_200mb"
            | "absolute_vorticity_300mb"
            | "absolute_vorticity_500mb"
            | "absolute_vorticity_700mb"
            | "absolute_vorticity_850mb",
        ) => Some(format!(
            "{} is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models",
            field.label
        )),
        (
            ModelId::Aifs,
            "absolute_vorticity_200mb"
            | "absolute_vorticity_300mb"
            | "absolute_vorticity_500mb"
            | "absolute_vorticity_700mb"
            | "absolute_vorticity_850mb",
        ) => Some(format!(
            "{} is not present in the Earth2Archive AIFS NetCDF schema currently wired by rustwx-models",
            field.label
        )),
        (
            ModelId::Aigfs
            | ModelId::Aigefs
            | ModelId::Rap
            | ModelId::Nam
            | ModelId::Hiresw
            | ModelId::Sref,
            "absolute_vorticity_200mb"
            | "absolute_vorticity_300mb"
            | "absolute_vorticity_500mb"
            | "absolute_vorticity_700mb"
            | "absolute_vorticity_850mb",
        ) => Some(format!(
            "{} is not verified for model '{model}' in the v0.5 compatibility path; keep vorticity recipes conservative until the GRIB signature is checked",
            field.label
        )),
        _ => None,
    }
}

fn model_specific_surface_field_gap(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (model, field.key) {
        (ModelId::Href, key) if !key.starts_with("href_spread_") => Some(format!(
            "{} is not yet wired for HREF; HREF support is currently limited to explicit `href_sprd_*` ensprod spread recipes",
            field.label
        )),
        (model, key) if key.starts_with("href_spread_") && model != ModelId::Href => {
            Some(format!(
                "{} is only verified for HREF ensprod spread fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key)
            if (key.starts_with("gefs_mean_") || key.starts_with("gefs_spread_"))
                && model != ModelId::Gefs =>
        {
            Some(format!(
                "{} is only verified for GEFS native geavg/gespr fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key) if key.starts_with("aigefs_spread_") && model != ModelId::Aigefs => {
            Some(format!(
                "{} is only verified for AI-GEFS ensstat spread fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key) if key.starts_with("hgefs_spread_") && model != ModelId::Hgefs => {
            Some(format!(
                "{} is only verified for HGEFS ensstat spread fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (model, key) if key.starts_with("sref_probability_") && model != ModelId::Sref => {
            Some(format!(
                "{} is only verified for SREF ensprod probability fields right now; do not route this recipe through model '{model}'",
                field.label
            ))
        }
        (ModelId::Hrrr, "theta_e_2m_agl") => Some(
            "2m Theta-e is surface-derived rather than native; HRRR exposes it through the derived product 'theta_e_2m_10m_winds' (legacy plot-recipe slug '2m_theta_e_10m_winds'), not as a direct/native GRIB recipe.".to_string(),
        ),
        (_, "theta_e_2m_agl") => Some(
            "2m Theta-e is surface-derived rather than native; the direct/native recipe registry does not yet wire the required PSFC/T2/SPFH/U10/V10 dependency bundle into one renderable product".to_string(),
        ),
        (ModelId::Hrrr, "heat_index_2m_agl") => Some(
            "2m Heat Index is surface-derived rather than native; HRRR exposes it through the derived product 'heat_index_2m' (legacy plot-recipe slug '2m_heat_index'), not as a direct/native GRIB recipe.".to_string(),
        ),
        (_, "heat_index_2m_agl") => Some(
            "2m Heat Index is surface-derived rather than native; the direct/native recipe registry does not yet wire the required T2/SPFH/U10/V10 dependency bundle into one renderable product".to_string(),
        ),
        (ModelId::Hrrr, "wind_chill_2m_agl") => Some(
            "2m Wind Chill is surface-derived rather than native; HRRR exposes it through the derived product 'wind_chill_2m' (legacy plot-recipe slug '2m_wind_chill'), not as a direct/native GRIB recipe.".to_string(),
        ),
        (_, "wind_chill_2m_agl") => Some(
            "2m Wind Chill is surface-derived rather than native; the direct/native recipe registry does not yet wire the required T2/U10/V10 dependency bundle into one renderable product".to_string(),
        ),
        (ModelId::WrfGdex, "visibility_surface") => Some(
            "Visibility is not part of the current WRF/GDEX one-off path; no verified wrfout visibility field is wired yet".to_string(),
        ),
        (ModelId::Aifs, "wind_gust_10m_agl" | "visibility_surface") => Some(format!(
            "{} is not present in the current Earth2Archive AIFS NetCDF schema",
            field.label
        )),
        (
            ModelId::Aifs,
            "low_cloud_cover" | "middle_cloud_cover" | "high_cloud_cover",
        ) => Some(format!(
            "{} is not present in the current Earth2Archive AIFS NetCDF schema; total cloud cover is available as tcc",
            field.label
        )),
        (_, "cloud_cover_levels") => None,
        (ModelId::Hrrr, "one_hour_qpf") => Some(
            "1h QPF is handled honestly in the HRRR windowed lane as 'qpf_1h' (legacy plot-recipe slug '1h_qpf'); do not treat it as a native/direct APCP recipe.".to_string(),
        ),
        (_, "one_hour_qpf") => Some(
            "1h QPF is not yet exposed as a generic native recipe because APCP accumulation windows vary by model and forecast hour.".to_string(),
        ),
        (model, "probability_of_precipitation") if model != ModelId::Nbm => Some(format!(
            "{} is only verified for NBM core APCP probability fields right now; add a model-specific idx/GRIB signature test before exposing it for '{model}'",
            field.label
        )),
        (_, "precipitation_type") => None,
        (_, "lightning_flash_density") => Some(
            "Verified HRRR surface files expose LTNGSD at 1 m and 2 m AGL as discipline 0/category 17/number 0 Lightning Strike Density [m^-2 s^-1], plus LTNG as discipline 0/category 17/number 192 Lightning [non-dim]; HRRR does not expose the flash-density parameters 2/3/4, so wiring this slug would mislabel strike density or a lightning flag.".to_string(),
        ),
        (ModelId::EcmwfOpenData, "simulated_infrared_brightness_temperature") => Some(format!(
            "{} is still a placeholder in rustwx-models for model '{model}'; the GRIB signature is not verified yet",
            field.label
        )),
        _ => None,
    }
}

fn is_supported_upper_air_level(level_hpa: u16) -> bool {
    matches!(level_hpa, 200 | 250 | 300 | 500 | 700 | 850)
}

fn unsupported_selector_reason(selector: FieldSelector, model: ModelId) -> String {
    format!(
        "selector '{selector}' is not yet supported for model '{model}' by the rustwx registry/extractor path"
    )
}

fn field_selector_gap_reason(_field: &GribFieldSpec) -> &'static str {
    "recipe field does not yet have a rustwx-models FieldSelector binding"
}

fn summarize_plot_recipe_blockers(blockers: &[PlotRecipeBlocker]) -> String {
    let mut grouped = Vec::<(String, Vec<&'static str>)>::new();
    for blocker in blockers {
        if let Some((_, labels)) = grouped
            .iter_mut()
            .find(|(reason, _)| reason == &blocker.reason)
        {
            labels.push(blocker.field_label);
        } else {
            grouped.push((blocker.reason.clone(), vec![blocker.field_label]));
        }
    }

    grouped
        .into_iter()
        .map(|(reason, labels)| format!("{}: {}", labels.join(", "), reason))
        .collect::<Vec<_>>()
        .join("; ")
}

fn collect_recipe_fields(
    recipe: &'static PlotRecipe,
    _model: ModelId,
) -> Vec<&'static GribFieldSpec> {
    let mut fields = match recipe.slug {
        "cloud_cover_levels" => vec![
            &FIELD_LOW_CLOUD_COVER,
            &FIELD_MIDDLE_CLOUD_COVER,
            &FIELD_HIGH_CLOUD_COVER,
        ],
        "precipitation_type" => vec![
            &FIELD_CATEGORICAL_RAIN,
            &FIELD_CATEGORICAL_FREEZING_RAIN,
            &FIELD_CATEGORICAL_ICE_PELLETS,
            &FIELD_CATEGORICAL_SNOW,
        ],
        _ => vec![&recipe.filled],
    };
    if let Some(contours) = &recipe.contours {
        fields.push(contours);
    }
    if let Some(barbs_u) = &recipe.barbs_u {
        fields.push(barbs_u);
    }
    if let Some(barbs_v) = &recipe.barbs_v {
        fields.push(barbs_v);
    }

    let mut deduped = Vec::with_capacity(fields.len());
    for field in fields {
        if !deduped
            .iter()
            .any(|existing: &&GribFieldSpec| existing.key == field.key)
        {
            deduped.push(field);
        }
    }
    deduped
}

fn dedupe_patterns<I>(patterns: I) -> Vec<&'static str>
where
    I: IntoIterator<Item = &'static str>,
{
    let mut out = Vec::new();
    for pattern in patterns {
        if !out.contains(&pattern) {
            out.push(pattern);
        }
    }
    out
}

fn unsupported_source(source: SourceId, model: ModelId) -> String {
    format!("unsupported://{source}/{model}")
}

fn is_supported_hrrr_smoke_hybrid_level(level: u16) -> bool {
    (1..=50).contains(&level)
}

impl fmt::Display for ModelSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.id, self.description)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_models_are_real() {
        assert_eq!(built_in_models().len(), 22);
        assert_eq!(model_summary(ModelId::HrrrAk).default_product, "sfc");
        assert_eq!(model_summary(ModelId::Gdas).default_product, "pgrb2.0p25");
        assert_eq!(
            model_summary(ModelId::Gefs).default_product,
            "pgrb2ap5/gec00"
        );
        assert_eq!(model_summary(ModelId::Aigfs).default_product, "sfc");
        assert_eq!(model_summary(ModelId::Aigefs).default_product, "sfc/avg");
        assert_eq!(model_summary(ModelId::Hgefs).default_product, "sfc/avg");
        assert_eq!(model_summary(ModelId::Hgefs).max_forecast_hour, 240);
        assert_eq!(model_summary(ModelId::Aifs).max_forecast_hour, 43_848);
        assert_eq!(
            model_summary(ModelId::Href).default_product,
            "ensprod/conus/sprd"
        );
        assert_eq!(
            model_summary(ModelId::Href).cycle_hours_utc,
            HREF_CYCLE_HOURS
        );
        assert_eq!(model_summary(ModelId::Rtma).max_forecast_hour, 0);
        assert_eq!(model_summary(ModelId::Nbm).default_product, "core/co");
        assert_eq!(model_summary(ModelId::RrfsA).default_product, "prs-conus");
        assert_eq!(
            model_summary(ModelId::RrfsPublic).cycle_hours_utc,
            RRFS_PUBLIC_CYCLE_HOURS
        );
        assert_eq!(
            model_summary(ModelId::RrfsFireWx).cycle_hours_utc,
            RRFS_FIREWX_CYCLE_HOURS
        );
        assert_eq!(model_summary(ModelId::RrfsFireWx).max_forecast_hour, 36);
        assert_eq!(
            model_summary(ModelId::WrfGdex).default_product,
            WRF_GDEX_DEFAULT_SURFACE_PRODUCT
        );
    }

    #[test]
    fn canonical_bundle_products_resolve_through_model_adapter() {
        let hrrr_surface = resolve_canonical_bundle_product(
            ModelId::Hrrr,
            CanonicalBundleDescriptor::SurfaceAnalysis,
            None,
        );
        assert_eq!(hrrr_surface.family, CanonicalDataFamily::Surface);
        assert_eq!(hrrr_surface.native_product, "sfc");

        let hrrr_pressure = resolve_canonical_bundle_product(
            ModelId::Hrrr,
            CanonicalBundleDescriptor::PressureAnalysis,
            None,
        );
        assert_eq!(hrrr_pressure.family, CanonicalDataFamily::Pressure);
        assert_eq!(hrrr_pressure.native_product, "prs");

        let rrfs_pressure = resolve_canonical_bundle_product(
            ModelId::RrfsA,
            CanonicalBundleDescriptor::PressureAnalysis,
            Some("prs-na"),
        );
        assert_eq!(
            rrfs_pressure.bundle,
            CanonicalBundleDescriptor::PressureAnalysis
        );
        assert_eq!(rrfs_pressure.family, CanonicalDataFamily::Pressure);
        assert_eq!(rrfs_pressure.native_product, "prs-na");

        let wrf_surface = resolve_canonical_bundle_product(
            ModelId::WrfGdex,
            CanonicalBundleDescriptor::SurfaceAnalysis,
            None,
        );
        assert_eq!(wrf_surface.native_product, WRF_GDEX_DEFAULT_SURFACE_PRODUCT);

        let wrf_pressure = resolve_canonical_bundle_product(
            ModelId::WrfGdex,
            CanonicalBundleDescriptor::PressureAnalysis,
            None,
        );
        assert_eq!(
            wrf_pressure.native_product,
            WRF_GDEX_DEFAULT_PRESSURE_PRODUCT
        );
    }

    #[test]
    fn built_in_plot_recipes_cover_current_direct_atmos_surface_and_radar_maps() {
        assert!(plot_recipe("200mb_height_winds").is_some());
        assert!(plot_recipe("300mb_height_winds").is_some());
        assert!(plot_recipe("250mb_height_winds").is_some());
        assert!(plot_recipe("500mb_height_winds").is_some());
        assert!(plot_recipe("700mb_height_winds").is_some());
        assert!(plot_recipe("850mb_height_winds").is_some());
        assert!(plot_recipe("200mb_temperature_height_winds").is_some());
        assert!(plot_recipe("300mb_temperature_height_winds").is_some());
        assert!(plot_recipe("250mb_temperature_height_winds").is_some());
        assert!(plot_recipe("500mb_temperature_height_winds").is_some());
        assert!(plot_recipe("700mb_temperature_height_winds").is_some());
        assert!(plot_recipe("850mb_temperature_height_winds").is_some());
        assert!(plot_recipe("2m_relative_humidity").is_some());
        assert!(plot_recipe("2m_relative_humidity_10m_winds").is_some());
        assert!(plot_recipe("2m_temperature").is_some());
        assert!(plot_recipe("2m_temperature_10m_winds").is_some());
        assert!(plot_recipe("2m_dewpoint").is_some());
        assert!(plot_recipe("2m_dewpoint_10m_winds").is_some());
        assert!(plot_recipe("mslp_10m_winds").is_some());
        assert!(plot_recipe("10m_wind_gusts").is_some());
        assert!(plot_recipe("precipitable_water").is_some());
        assert!(plot_recipe("cloud_cover").is_some());
        assert!(plot_recipe("visibility").is_some());
        assert!(plot_recipe("simulated_ir_satellite").is_some());
        assert!(plot_recipe("700mb_dewpoint_height_winds").is_some());
        assert!(plot_recipe("850mb_dewpoint_height_winds").is_some());
        assert!(plot_recipe("200mb_rh_height_winds").is_some());
        assert!(plot_recipe("300mb_rh_height_winds").is_some());
        assert!(plot_recipe("500mb_absolute_vorticity_height_winds").is_some());
        assert!(plot_recipe("200mb_absolute_vorticity_height_winds").is_some());
        assert!(plot_recipe("300mb_absolute_vorticity_height_winds").is_some());
        assert!(plot_recipe("500mb_rh_height_winds").is_some());
        assert!(plot_recipe("700mb_rh_height_winds").is_some());
        assert!(plot_recipe("700mb_absolute_vorticity_height_winds").is_some());
        assert!(plot_recipe("1km_reflectivity").is_some());
        assert!(plot_recipe("composite_reflectivity").is_some());
        assert!(plot_recipe("composite_reflectivity_uh").is_some());
        assert!(plot_recipe("smoke_pm25_native").is_some());
        assert!(plot_recipe("smoke_column").is_some());
    }

    #[test]
    fn grib_field_spec_exposes_typed_product_metadata() {
        let metadata = FIELD_500_TEMP.product_metadata();
        assert_eq!(metadata.display_name, "500mb Temperature");
        assert_eq!(metadata.category.as_deref(), Some("pressure"));
        assert_eq!(metadata.native_units.as_deref(), Some("K"));
        let provenance = metadata
            .provenance
            .expect("field metadata should carry provenance");
        assert_eq!(provenance.lineage, ProductLineage::Direct);
        assert_eq!(provenance.maturity, ProductMaturity::Operational);
        assert_eq!(
            provenance.selector,
            Some(FieldSelector::isobaric(CanonicalField::Temperature, 500))
        );

        let smoke_metadata = FIELD_SMOKE_MASS_DENSITY_8M.product_metadata();
        assert_eq!(smoke_metadata.display_name, "8m AGL Smoke Mass Density");
        assert_eq!(smoke_metadata.category.as_deref(), Some("native"));
        assert_eq!(smoke_metadata.native_units.as_deref(), Some("kg/m^3"));

        let column_metadata = FIELD_COLUMN_INTEGRATED_SMOKE.product_metadata();
        assert_eq!(column_metadata.display_name, "Column-Integrated Smoke");
        assert_eq!(column_metadata.category.as_deref(), Some("native"));
        assert_eq!(column_metadata.native_units.as_deref(), Some("kg/m^2"));
    }

    #[test]
    fn plot_recipe_metadata_marks_derived_windowed_and_composite_routes() {
        let heat_index = plot_recipe("2m_heat_index").expect("heat index recipe should exist");
        assert_eq!(heat_index.provenance().lineage, ProductLineage::Derived);

        let qpf_1h = plot_recipe("1h_qpf").expect("1h qpf recipe should exist");
        let qpf_provenance = qpf_1h.provenance();
        assert_eq!(qpf_provenance.lineage, ProductLineage::Windowed);
        assert_eq!(
            qpf_provenance.window,
            Some(ProductWindowSpec::accumulation(Some(1)))
        );
        assert!(qpf_provenance.flags.contains(&ProductSemanticFlag::Alias));

        let refl_uh =
            plot_recipe("composite_reflectivity_uh").expect("reflectivity+UH recipe should exist");
        assert!(
            refl_uh
                .provenance()
                .flags
                .contains(&ProductSemanticFlag::Composite)
        );
    }

    #[test]
    fn experimental_recipe_metadata_is_explicit() {
        let simulated_ir =
            plot_recipe("simulated_ir_satellite").expect("simulated ir recipe should exist");
        assert_eq!(
            simulated_ir.product_metadata().provenance.unwrap().maturity,
            ProductMaturity::Experimental
        );

        let lightning =
            plot_recipe("lightning_flash_density").expect("lightning recipe should exist");
        assert_eq!(
            lightning.product_metadata().provenance.unwrap().maturity,
            ProductMaturity::Experimental
        );
    }

    #[test]
    fn plot_recipe_alias_lookup_normalizes_tokens() {
        let recipe = plot_recipe("500MB temperature height winds").unwrap();
        assert_eq!(recipe.slug, "500mb_temperature_height_winds");
        assert_eq!(recipe.filled.level_value, Some(500));
        assert_eq!(recipe.barbs_u.as_ref().unwrap().key, "u_500mb");
        assert_eq!(
            recipe.filled.selector,
            Some(FieldSelector::isobaric(CanonicalField::Temperature, 500))
        );

        let absolute_vorticity = plot_recipe("500MB vorticity height winds").unwrap();
        assert_eq!(
            absolute_vorticity.slug,
            "500mb_absolute_vorticity_height_winds"
        );
        assert_eq!(
            absolute_vorticity.filled.selector,
            Some(FieldSelector::isobaric(
                CanonicalField::AbsoluteVorticity,
                500,
            ))
        );

        let temp_2m = plot_recipe("2m temperature 10m winds").unwrap();
        assert_eq!(temp_2m.slug, "2m_temperature_10m_winds");
        assert_eq!(
            temp_2m.filled.selector,
            Some(FieldSelector::height_agl(CanonicalField::Temperature, 2))
        );

        let reflectivity_1km = plot_recipe("1km reflectivity").unwrap();
        assert_eq!(reflectivity_1km.slug, "1km_reflectivity");
        assert_eq!(
            reflectivity_1km.filled.selector,
            Some(FieldSelector::height_agl(
                CanonicalField::RadarReflectivity,
                1000,
            ))
        );
    }

    #[test]
    fn composite_reflectivity_uh_recipe_requires_native_reflectivity_and_uh() {
        let recipe = plot_recipe("composite_reflectivity_uh").unwrap();
        assert_eq!(recipe.filled.family, ProductFamily::Native);
        assert_eq!(recipe.filled.idx_patterns()[0], "REFC:entire atmosphere");
        assert_eq!(
            recipe.contours.as_ref().unwrap().idx_patterns()[0],
            "MXUPHL:5000-2000"
        );
        assert!(recipe.barbs_u.is_none());
        assert!(recipe.barbs_v.is_none());
    }

    #[test]
    fn surface_wind_recipes_include_mslp_contours() {
        for slug in [
            "2m_relative_humidity_10m_winds",
            "2m_temperature_10m_winds",
            "2m_dewpoint_10m_winds",
            "mslp_10m_winds",
        ] {
            let recipe = plot_recipe(slug).unwrap();
            assert_eq!(
                recipe.contours.as_ref().unwrap().selector,
                Some(FieldSelector::mean_sea_level(
                    CanonicalField::PressureReducedToMeanSeaLevel,
                ))
            );
            assert_eq!(
                recipe.barbs_u.as_ref().unwrap().selector,
                Some(FieldSelector::height_agl(CanonicalField::UWind, 10))
            );
            assert_eq!(
                recipe.barbs_v.as_ref().unwrap().selector,
                Some(FieldSelector::height_agl(CanonicalField::VWind, 10))
            );
        }
    }

    #[test]
    fn smoke_recipes_are_selector_backed_native_hrrr_products() {
        let smoke = plot_recipe("smoke_pm25_native").unwrap();
        assert_eq!(smoke.filled.family, ProductFamily::Native);
        assert_eq!(
            smoke.filled.selector,
            Some(FieldSelector::height_agl(
                CanonicalField::SmokeMassDensity,
                8
            ))
        );
        assert_eq!(smoke.filled.idx_patterns()[0], "MASSDEN:8 m above ground");

        let column = plot_recipe("smoke_column").unwrap();
        assert_eq!(column.filled.family, ProductFamily::Native);
        assert_eq!(
            column.filled.selector,
            Some(FieldSelector::entire_atmosphere(
                CanonicalField::ColumnIntegratedSmoke
            ))
        );
        assert_eq!(
            column.filled.idx_patterns()[0],
            "COLMD:entire atmosphere (considered as a single layer)"
        );
    }

    #[test]
    fn selector_backed_temperature_recipe_produces_gfs_fetch_plan() {
        let plan = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(plan.product, "pgrb2.0p25");
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert_eq!(plan.fields.len(), 4);
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::Temperature, 500),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
                FieldSelector::isobaric(CanonicalField::UWind, 500),
                FieldSelector::isobaric(CanonicalField::VWind, 500),
            ]
        );
        assert!(!plan.variable_patterns().is_empty());
    }

    #[test]
    fn selector_backed_200mb_temperature_recipe_produces_gfs_fetch_plan() {
        let plan = plot_recipe_fetch_plan("200mb_temperature_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(plan.product, "pgrb2.0p25");
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::Temperature, 200),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 200),
                FieldSelector::isobaric(CanonicalField::UWind, 200),
                FieldSelector::isobaric(CanonicalField::VWind, 200),
            ]
        );
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert!(!plan.variable_patterns().is_empty());
    }

    #[test]
    fn selector_backed_temperature_recipe_produces_ecmwf_whole_file_fetch_plan() {
        let plan = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::EcmwfOpenData)
            .unwrap();
        assert_eq!(plan.product, "oper");
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::Temperature, 500),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
                FieldSelector::isobaric(CanonicalField::UWind, 500),
                FieldSelector::isobaric(CanonicalField::VWind, 500),
            ]
        );
        assert!(plan.variable_patterns().is_empty());
    }

    #[test]
    fn rh_recipe_blocker_is_explicit_for_gfs() {
        let blockers = plot_recipe_fetch_blockers("500mb_rh_height_winds", ModelId::Gfs).unwrap();
        assert!(blockers.is_empty());
    }

    #[test]
    fn selector_gap_is_explicit_for_rh_recipe() {
        let plan = plot_recipe_fetch_plan("500mb_rh_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::RelativeHumidity, 500),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
                FieldSelector::isobaric(CanonicalField::UWind, 500),
                FieldSelector::isobaric(CanonicalField::VWind, 500),
            ]
        );
    }

    #[test]
    fn selector_backed_300mb_rh_recipe_produces_gfs_fetch_plan() {
        let plan = plot_recipe_fetch_plan("300mb_rh_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::RelativeHumidity, 300),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 300),
                FieldSelector::isobaric(CanonicalField::UWind, 300),
                FieldSelector::isobaric(CanonicalField::VWind, 300),
            ]
        );
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert!(!plan.variable_patterns().is_empty());
    }

    #[test]
    fn temperature_700_recipe_tracks_model_support() {
        let selectors = vec![
            FieldSelector::isobaric(CanonicalField::Temperature, 700),
            FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 700),
            FieldSelector::isobaric(CanonicalField::UWind, 700),
            FieldSelector::isobaric(CanonicalField::VWind, 700),
        ];

        for model in [
            ModelId::Hrrr,
            ModelId::HrrrAk,
            ModelId::Gfs,
            ModelId::Gdas,
            ModelId::Gefs,
            ModelId::Aigfs,
            ModelId::Aigefs,
            ModelId::EcmwfOpenData,
            ModelId::Aifs,
            ModelId::Rap,
            ModelId::Nam,
            ModelId::Hiresw,
            ModelId::Href,
            ModelId::Sref,
            ModelId::Rtma,
            ModelId::Urma,
            ModelId::Nbm,
            ModelId::RrfsA,
            ModelId::RrfsPublic,
            ModelId::WrfGdex,
        ] {
            if selectors
                .iter()
                .all(|selector| selector_supported_for_model(*selector, model))
            {
                let plan = plot_recipe_fetch_plan("700mb_temperature_height_winds", model).unwrap();
                assert_eq!(plan.selectors(), selectors);
                assert!(
                    plot_recipe_fetch_blockers("700mb_temperature_height_winds", model)
                        .unwrap()
                        .is_empty()
                );
            } else {
                let blockers =
                    plot_recipe_fetch_blockers("700mb_temperature_height_winds", model).unwrap();
                assert_eq!(
                    blockers
                        .iter()
                        .map(|blocker| blocker.field_key)
                        .collect::<Vec<_>>(),
                    vec!["temperature_700mb", "height_700mb", "u_700mb", "v_700mb"]
                );
                let reason = &blockers[0].reason;
                if matches!(model, ModelId::Rtma | ModelId::Urma | ModelId::Nbm) {
                    assert!(reason.contains("surface/core grids"));
                } else if model == ModelId::Href {
                    assert!(reason.contains("limited to explicit `href_sprd_*`"));
                } else {
                    assert!(reason.contains("700 hPa temperature/height/wind selectors"));
                }
                match model {
                    ModelId::EcmwfOpenData | ModelId::WrfGdex => {
                        assert!(reason.contains("whole-file structured extraction"));
                    }
                    ModelId::Hrrr
                    | ModelId::HrrrAk
                    | ModelId::Gfs
                    | ModelId::Gdas
                    | ModelId::Gefs
                    | ModelId::Aigfs
                    | ModelId::Aigefs
                    | ModelId::Hgefs
                    | ModelId::Rap
                    | ModelId::Nam
                    | ModelId::Hiresw
                    | ModelId::Sref
                    | ModelId::RrfsA
                    | ModelId::RrfsPublic
                    | ModelId::RrfsFireWx => {
                        assert!(reason.contains("idx subsetting can stage the GRIB messages"));
                    }
                    ModelId::Rtma | ModelId::Urma | ModelId::Nbm => {
                        assert!(reason.contains("surface/core grids"));
                    }
                    ModelId::Href => {
                        assert!(reason.contains("limited to explicit `href_sprd_*`"));
                    }
                    ModelId::Aifs => {
                        assert!(reason.contains("whole-file structured extraction"));
                    }
                }
            }
        }
    }

    #[test]
    fn dewpoint_and_700mb_recipe_blockers_are_explicit() {
        for model in [ModelId::Hrrr, ModelId::RrfsA] {
            let dewpoint_850 =
                plot_recipe_fetch_blockers("850mb_dewpoint_height_winds", model).unwrap();
            assert!(dewpoint_850.is_empty());

            let dewpoint_700 =
                plot_recipe_fetch_blockers("700mb_dewpoint_height_winds", model).unwrap();
            assert!(dewpoint_700.is_empty());
        }

        let gfs_dewpoint =
            plot_recipe_fetch_blockers("700mb_dewpoint_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(
            gfs_dewpoint,
            vec![PlotRecipeBlocker {
                field_key: "dewpoint_700mb",
                field_label: "700mb Dewpoint",
                reason: "700mb Dewpoint is not present in the GFS 0.25-degree pgrb2 file currently wired by rustwx-models; keep it blocked until a verified direct field or derived dewpoint path is implemented".to_string(),
            }]
        );
        let err = plot_recipe_fetch_plan("700mb_dewpoint_height_winds", ModelId::Gfs).unwrap_err();
        assert!(matches!(err, ModelError::UnsupportedPlotRecipeModel { .. }));

        let ecmwf_dewpoint =
            plot_recipe_fetch_blockers("700mb_dewpoint_height_winds", ModelId::EcmwfOpenData)
                .unwrap();
        assert_eq!(
            ecmwf_dewpoint,
            vec![PlotRecipeBlocker {
                field_key: "dewpoint_700mb",
                field_label: "700mb Dewpoint",
                reason: "700mb Dewpoint is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models; use RH/TMP or add derived dewpoint support for this model".to_string(),
            }]
        );
    }

    #[test]
    fn absolute_vorticity_recipe_blocker_is_explicit_for_gfs() {
        let blockers =
            plot_recipe_fetch_blockers("850mb_absolute_vorticity_height_winds", ModelId::Gfs)
                .unwrap();
        assert!(blockers.is_empty());
    }

    #[test]
    fn absolute_vorticity_recipe_retains_explicit_primary_blocker() {
        let plan =
            plot_recipe_fetch_plan("700mb_absolute_vorticity_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::AbsoluteVorticity, 700),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 700),
                FieldSelector::isobaric(CanonicalField::UWind, 700),
                FieldSelector::isobaric(CanonicalField::VWind, 700),
            ]
        );
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert!(!plan.variable_patterns().is_empty());

        let blockers =
            plot_recipe_fetch_blockers("700mb_absolute_vorticity_height_winds", ModelId::Gfs)
                .unwrap();
        assert!(blockers.is_empty());

        let err = plot_recipe_fetch_plan(
            "500mb_absolute_vorticity_height_winds",
            ModelId::EcmwfOpenData,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ModelError::UnsupportedPlotRecipeModel {
                recipe: "500mb_absolute_vorticity_height_winds",
                model: ModelId::EcmwfOpenData,
                reason,
            } if reason == "500mb Absolute Vorticity: 500mb Absolute Vorticity is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models"
        ));

        let ecmwf_blockers = plot_recipe_fetch_blockers(
            "500mb_absolute_vorticity_height_winds",
            ModelId::EcmwfOpenData,
        )
        .unwrap();
        assert_eq!(
            ecmwf_blockers,
            vec![PlotRecipeBlocker {
                field_key: "absolute_vorticity_500mb",
                field_label: "500mb Absolute Vorticity",
                reason: "500mb Absolute Vorticity is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models".to_string(),
            }]
        );

        let err = plot_recipe_fetch_plan(
            "300mb_absolute_vorticity_height_winds",
            ModelId::EcmwfOpenData,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ModelError::UnsupportedPlotRecipeModel {
                recipe: "300mb_absolute_vorticity_height_winds",
                model: ModelId::EcmwfOpenData,
                reason,
            } if reason == "300mb Absolute Vorticity: 300mb Absolute Vorticity is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models"
        ));
    }

    #[test]
    fn latest_available_run_prefers_newest_cycle_over_source_priority() {
        let latest = latest_available_run_with_probe(ModelId::Gfs, None, "20260414", |resolved| {
            resolved.source == SourceId::Aws
                && resolved
                    .availability_probe_url()
                    .contains("gfs.t18z.pgrb2.0p25.f000")
        })
        .unwrap();

        assert_eq!(latest.cycle.hour_utc, 18);
        assert_eq!(latest.source, SourceId::Aws);
    }

    #[test]
    fn latest_available_run_prefers_source_priority_within_same_cycle() {
        let latest = latest_available_run_with_probe(ModelId::Gfs, None, "20260414", |resolved| {
            resolved
                .availability_probe_url()
                .contains("gfs.t18z.pgrb2.0p25.f000")
        })
        .unwrap();

        assert_eq!(latest.cycle.hour_utc, 18);
        assert_eq!(latest.source, SourceId::Nomads);
    }

    #[test]
    fn ecmwf_summary_matches_current_open_data_cycles_and_horizon() {
        let summary = model_summary(ModelId::EcmwfOpenData);
        assert_eq!(summary.cycle_hours_utc, &[0, 6, 12, 18]);
        assert_eq!(summary.max_forecast_hour, 360);
    }

    #[test]
    fn hiresw_summary_matches_default_conus_arw_cycles_and_horizon() {
        let summary = model_summary(ModelId::Hiresw);
        assert_eq!(summary.default_product, "arw_2p5km/conus");
        assert_eq!(summary.cycle_hours_utc, &[0, 12]);
        assert_eq!(summary.max_forecast_hour, 48);
    }

    #[test]
    fn ecmwf_supported_forecast_hours_follow_open_data_cadence() {
        let hours_00z = supported_forecast_hours(ModelId::EcmwfOpenData, 0);
        assert!(hours_00z.contains(&0));
        assert!(hours_00z.contains(&144));
        assert!(hours_00z.contains(&150));
        assert!(hours_00z.contains(&360));
        assert!(!hours_00z.contains(&145));
        assert!(!hours_00z.contains(&147));

        let hours_06z = supported_forecast_hours(ModelId::EcmwfOpenData, 6);
        assert!(hours_06z.contains(&0));
        assert!(hours_06z.contains(&144));
        assert!(!hours_06z.contains(&145));
        assert!(!hours_06z.contains(&150));
        assert!(!hours_06z.contains(&360));
    }

    #[test]
    fn gefs_supported_forecast_hours_follow_operational_high_hour_cadence() {
        let hours = supported_forecast_hours(ModelId::Gefs, 12);
        assert!(hours.contains(&0));
        assert!(hours.contains(&240));
        assert!(!hours.contains(&243));
        assert!(hours.contains(&246));
        assert!(!hours.contains(&249));
        assert!(hours.contains(&384));
    }

    #[test]
    fn rap_supported_forecast_hours_are_cycle_aware() {
        let short_cycle = supported_forecast_hours(ModelId::Rap, 0);
        assert!(short_cycle.contains(&21));
        assert!(!short_cycle.contains(&22));
        assert!(!short_cycle.contains(&51));

        let extended_cycle = supported_forecast_hours(ModelId::Rap, 3);
        assert!(extended_cycle.contains(&21));
        assert!(extended_cycle.contains(&22));
        assert!(extended_cycle.contains(&51));
    }

    #[test]
    fn rap_url_builder_rejects_extended_hours_on_short_cycles() {
        let short_cycle = rustwx_core::CycleSpec::new("20260502", 0).unwrap();
        let request = ModelRunRequest::new(ModelId::Rap, short_cycle, 22, "awp130pgrb").unwrap();
        let err = build_grib_url(SourceId::Nomads, &request).unwrap_err();
        assert!(matches!(err, ModelError::UnsupportedForecastHour { .. }));

        let extended_cycle = rustwx_core::CycleSpec::new("20260502", 3).unwrap();
        let request = ModelRunRequest::new(ModelId::Rap, extended_cycle, 51, "awp130pgrb").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &request).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.20260502/rap.t03z.awp130pgrbf51.grib2"
        );
    }

    #[test]
    fn gefs_urls_use_member_product_and_operational_layout() {
        let request = ModelRunRequest::new(
            ModelId::Gefs,
            rustwx_core::CycleSpec::new("20260502", 0).unwrap(),
            24,
            "pgrb2ap5/gep03",
        )
        .unwrap();

        assert_eq!(
            build_grib_url(SourceId::Aws, &request).unwrap(),
            "https://noaa-gefs-pds.s3.amazonaws.com/gefs.20260502/00/atmos/pgrb2ap5/gep03.t00z.pgrb2a.0p50.f024"
        );
        assert_eq!(
            build_grib_url(SourceId::Nomads, &request).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.20260502/00/atmos/pgrb2ap5/gep03.t00z.pgrb2a.0p50.f024"
        );
    }

    #[test]
    fn easy_ncep_model_urls_use_current_operational_layouts() {
        let cycle = rustwx_core::CycleSpec::new("20260502", 0).unwrap();
        let hrrr_ak = ModelRunRequest::new(ModelId::HrrrAk, cycle.clone(), 24, "sfc").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &hrrr_ak).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.20260502/alaska/hrrr.t00z.wrfsfcf24.ak.grib2"
        );

        let gdas = ModelRunRequest::new(ModelId::Gdas, cycle.clone(), 3, "pgrb2.0p25").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Aws, &gdas).unwrap(),
            "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gdas.20260502/00/atmos/gdas.t00z.pgrb2.0p25.f003"
        );

        let aigfs = ModelRunRequest::new(ModelId::Aigfs, cycle.clone(), 24, "pres").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &aigfs).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigfs/prod/aigfs.20260502/00/model/atmos/grib2/aigfs.t00z.pres.f024.grib2"
        );

        let aigefs = ModelRunRequest::new(ModelId::Aigefs, cycle.clone(), 24, "sfc/avg").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &aigefs).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigefs/prod/aigefs.20260502/00/ensstat/products/atmos/grib2/aigefs.t00z.sfc.avg.f024.grib2"
        );

        let hgefs = ModelRunRequest::new(ModelId::Hgefs, cycle.clone(), 24, "pres/spr").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &hgefs).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hgefs/prod/hgefs.20260502/00/ensstat/products/atmos/grib2/hgefs.t00z.pres.spr.f024.grib2"
        );

        let rrfs_firewx =
            ModelRunRequest::new(ModelId::RrfsFireWx, cycle.clone(), 24, "2dfld-firewx").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Aws, &rrfs_firewx).unwrap(),
            "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/firewx.20260502/00/rrfs.t00z.2dfld.1p5km.f024.firewx_lcc.grib2"
        );

        let rap = ModelRunRequest::new(ModelId::Rap, cycle.clone(), 21, "awp130pgrb").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &rap).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.20260502/rap.t00z.awp130pgrbf21.grib2"
        );

        let nam = ModelRunRequest::new(ModelId::Nam, cycle.clone(), 24, "awip12").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &nam).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.20260502/nam.t00z.awip1224.tm00.grib2"
        );

        let hiresw =
            ModelRunRequest::new(ModelId::Hiresw, cycle.clone(), 24, "arw_2p5km/conus").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &hiresw).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hiresw/prod/hiresw.20260502/hiresw.t00z.arw_2p5km.f24.conus.grib2"
        );

        let href =
            ModelRunRequest::new(ModelId::Href, cycle.clone(), 24, "ensprod/conus/sprd").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &href).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/href/prod/href.20260502/ensprod/href.t00z.conus.sprd.f24.grib2"
        );

        let nbm = ModelRunRequest::new(ModelId::Nbm, cycle.clone(), 24, "core/co").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Aws, &nbm).unwrap(),
            "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20260502/00/core/blend.t00z.core.f024.co.grib2"
        );

        let rtma = ModelRunRequest::new(ModelId::Rtma, cycle.clone(), 0, "2dvaranl_ndfd").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &rtma).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.20260502/rtma2p5.t00z.2dvaranl_ndfd.grb2_wexp"
        );

        let sref_cycle = rustwx_core::CycleSpec::new("20260502", 3).unwrap();
        let sref = ModelRunRequest::new(ModelId::Sref, sref_cycle, 24, "arw/ctl/pgrb132").unwrap();
        assert_eq!(
            build_grib_url(SourceId::Nomads, &sref).unwrap(),
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/sref/prod/sref.20260502/03/pgrb/sref_arw.t03z.pgrb132.ctl.f24.grib2"
        );
    }

    #[test]
    fn aifs_supports_local_long_runs_and_ecmwf_open_data_urls() {
        let local = ModelRunRequest::new(
            ModelId::Aifs,
            rustwx_core::CycleSpec::new("20260502", 0).unwrap(),
            43_848,
            "oper",
        )
        .unwrap();
        assert_eq!(
            build_grib_url(SourceId::Earth2Archive, &local).unwrap(),
            "earth2-archive://aifs/20260502T00Z/lead43848.nc"
        );

        let open_data = ModelRunRequest::new(
            ModelId::Aifs,
            rustwx_core::CycleSpec::new("20260502", 0).unwrap(),
            24,
            "oper",
        )
        .unwrap();
        assert_eq!(
            build_grib_url(SourceId::Ecmwf, &open_data).unwrap(),
            "https://data.ecmwf.int/forecasts/20260502/00z/aifs-single/0p25/oper/20260502000000-24h-oper-fc.grib2"
        );

        assert!(matches!(
            build_grib_url(SourceId::Ecmwf, &local),
            Err(ModelError::UnsupportedForecastHour {
                model: ModelId::Aifs,
                forecast_hour: 43_848,
                ..
            })
        ));
    }

    #[test]
    fn latest_available_run_considers_ecmwf_six_hour_cycles() {
        let latest =
            latest_available_run_with_probe(ModelId::EcmwfOpenData, None, "20260414", |resolved| {
                resolved
                    .availability_probe_url()
                    .contains("/20260414/18z/ifs/0p25/oper/")
            })
            .unwrap();

        assert_eq!(latest.cycle.hour_utc, 18);
        assert_eq!(latest.source, SourceId::Ecmwf);
    }

    #[test]
    fn latest_available_run_at_forecast_hour_prefers_ecmwf_long_cycle_when_needed() {
        let latest = latest_available_run_for_products_with_probe_at_forecast_hour(
            ModelId::EcmwfOpenData,
            None,
            "20260414",
            &["oper"],
            150,
            |resolved| {
                let url = resolved.availability_probe_url();
                url.contains("/20260414/12z/ifs/0p25/oper/") && url.contains("150h-oper-fc.grib2")
            },
        )
        .unwrap();

        assert_eq!(latest.cycle.date_yyyymmdd, "20260414");
        assert_eq!(latest.cycle.hour_utc, 12);
    }

    #[test]
    fn latest_available_run_rolls_back_to_previous_day_when_today_is_unpublished() {
        // Simulate a UTC-rollover / publication window where no cycle of the
        // requested date has been published yet, but yesterday's 18z is still
        // serving. The probe returns true only for yesterday's 18z URL.
        let latest = latest_available_run_with_probe(ModelId::Gfs, None, "20260415", |resolved| {
            resolved
                .availability_probe_url()
                .contains("gfs.20260414/18/atmos/gfs.t18z.pgrb2.0p25.f000")
        })
        .unwrap();

        assert_eq!(latest.cycle.date_yyyymmdd, "20260414");
        assert_eq!(latest.cycle.hour_utc, 18);
    }

    #[test]
    fn latest_available_run_prefers_today_even_with_rollback_enabled() {
        // If both today and yesterday are available, today wins — the
        // rollback must not demote a published current-day run.
        let latest =
            latest_available_run_with_probe(ModelId::EcmwfOpenData, None, "20260414", |resolved| {
                let url = resolved.availability_probe_url();
                url.contains("/20260414/06z/") || url.contains("/20260413/18z/")
            })
            .unwrap();

        assert_eq!(latest.cycle.date_yyyymmdd, "20260414");
        assert_eq!(latest.cycle.hour_utc, 6);
    }

    #[test]
    fn latest_available_run_for_products_requires_all_products_on_same_cycle() {
        let latest = latest_available_run_for_products_with_probe(
            ModelId::RrfsA,
            Some(SourceId::Aws),
            "20260417",
            &["prs-conus", "nat-na", "prs-na"],
            |resolved| {
                let url = resolved.availability_probe_url();
                url.contains("20260417/22/")
                    || (url.contains("20260417/23/") && url.contains("prslev.3km.f000.conus"))
            },
        )
        .unwrap();

        assert_eq!(latest.cycle.date_yyyymmdd, "20260417");
        assert_eq!(latest.cycle.hour_utc, 22);
        assert_eq!(latest.source, SourceId::Aws);
    }

    #[test]
    fn previous_day_yyyymmdd_handles_month_and_year_boundaries() {
        assert_eq!(
            previous_day_yyyymmdd("20260415").as_deref(),
            Some("20260414")
        );
        assert_eq!(
            previous_day_yyyymmdd("20260401").as_deref(),
            Some("20260331")
        );
        assert_eq!(
            previous_day_yyyymmdd("20260101").as_deref(),
            Some("20251231")
        );
        // Leap-year awareness: 2028 is a leap year, so 2028-03-01 rolls back
        // to 2028-02-29, while 2027-03-01 rolls back to 2027-02-28.
        assert_eq!(
            previous_day_yyyymmdd("20280301").as_deref(),
            Some("20280229")
        );
        assert_eq!(
            previous_day_yyyymmdd("20270301").as_deref(),
            Some("20270228")
        );
        assert_eq!(previous_day_yyyymmdd("notadate"), None);
    }

    #[test]
    fn nomads_uses_range_probe_policy() {
        assert!(should_use_range_probe(SourceId::Nomads));
        assert!(!should_use_range_probe(SourceId::Aws));
    }

    #[test]
    fn hrrr_native_reflectivity_recipe_produces_nat_fetch_plan() {
        let plan = plot_recipe_fetch_plan("composite_reflectivity", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "nat");
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert_eq!(
            plan.selectors(),
            vec![FieldSelector::entire_atmosphere(
                CanonicalField::CompositeReflectivity
            )]
        );
        assert!(!plan.variable_patterns().is_empty());
    }

    #[test]
    fn native_reflectivity_uh_recipe_tracks_supported_models() {
        let hrrr_plan = plot_recipe_fetch_plan("composite_reflectivity_uh", ModelId::Hrrr).unwrap();
        assert_eq!(hrrr_plan.product, "nat");
        assert_eq!(
            hrrr_plan.selectors(),
            vec![
                FieldSelector::entire_atmosphere(CanonicalField::CompositeReflectivity),
                FieldSelector::height_layer_agl(CanonicalField::UpdraftHelicity, 2000, 5000),
            ]
        );

        let rrfs_plan =
            plot_recipe_fetch_plan("composite_reflectivity_uh", ModelId::RrfsA).unwrap();
        assert_eq!(rrfs_plan.product, "prs-conus");
        assert_eq!(
            rrfs_plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(rrfs_plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert_eq!(
            rrfs_plan.selectors(),
            vec![
                FieldSelector::entire_atmosphere(CanonicalField::CompositeReflectivity),
                FieldSelector::height_layer_agl(CanonicalField::UpdraftHelicity, 2000, 5000),
            ]
        );
        assert!(!rrfs_plan.variable_patterns().is_empty());

        let rrfs_public_plan =
            plot_recipe_fetch_plan("composite_reflectivity_uh", ModelId::RrfsPublic).unwrap();
        assert_eq!(rrfs_public_plan.product, "2dfld-conus");
        assert_eq!(
            rrfs_public_plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(
            rrfs_public_plan.fetch_mode,
            PlotRecipeFetchMode::IndexedSubset
        );
        assert_eq!(rrfs_public_plan.selectors(), rrfs_plan.selectors());
        assert!(!rrfs_public_plan.variable_patterns().is_empty());

        let rrfs_firewx_plan =
            plot_recipe_fetch_plan("composite_reflectivity_uh", ModelId::RrfsFireWx).unwrap();
        assert_eq!(rrfs_firewx_plan.product, "2dfld-firewx");
        assert_eq!(
            rrfs_firewx_plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(
            rrfs_firewx_plan.fetch_mode,
            PlotRecipeFetchMode::IndexedSubset
        );
        assert_eq!(rrfs_firewx_plan.selectors(), rrfs_plan.selectors());
        assert!(!rrfs_firewx_plan.variable_patterns().is_empty());
    }

    #[test]
    fn rrfs_public_variants_direct_fetches_use_matching_public_products() {
        let cases = [
            (ModelId::RrfsPublic, "2dfld-conus", "prs-conus"),
            (ModelId::RrfsFireWx, "2dfld-firewx", "prs-firewx"),
        ];
        for (model, surface_product, pressure_product) in cases {
            let surface = plot_recipe_fetch_plan("2m_temperature_10m_winds", model).unwrap();
            assert_eq!(surface.product, surface_product);
            assert_eq!(surface.fetch_mode, PlotRecipeFetchMode::IndexedSubset);

            let pressure = plot_recipe_fetch_plan("500mb_height_winds", model).unwrap();
            assert_eq!(pressure.product, pressure_product);
            assert_eq!(pressure.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        }
    }

    #[test]
    fn simulated_ir_recipe_is_supported_for_hrrr_native_fetch() {
        let plan = plot_recipe_fetch_plan("simulated_ir_satellite", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "nat");
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert_eq!(
            plan.selectors(),
            vec![FieldSelector::nominal_top(
                CanonicalField::SimulatedInfraredBrightnessTemperature
            )]
        );
        assert!(!plan.variable_patterns().is_empty());
        assert!(
            plot_recipe_fetch_blockers("simulated_ir_satellite", ModelId::Hrrr)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn smoke_recipes_use_hrrr_native_fetch_plan() {
        let smoke = plot_recipe_fetch_plan("smoke_pm25_native", ModelId::Hrrr).unwrap();
        assert_eq!(smoke.product, "nat");
        assert_eq!(
            smoke.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(smoke.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert_eq!(
            smoke.selectors(),
            vec![FieldSelector::height_agl(
                CanonicalField::SmokeMassDensity,
                8
            )]
        );
        assert!(!smoke.variable_patterns().is_empty());

        let column = plot_recipe_fetch_plan("smoke_column", ModelId::Hrrr).unwrap();
        assert_eq!(column.product, "nat");
        assert_eq!(
            column.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(column.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert_eq!(
            column.selectors(),
            vec![FieldSelector::entire_atmosphere(
                CanonicalField::ColumnIntegratedSmoke
            )]
        );
        assert!(!column.variable_patterns().is_empty());
    }

    #[test]
    fn supported_recipe_has_no_fetch_blockers() {
        let blockers =
            plot_recipe_fetch_blockers("850mb_temperature_height_winds", ModelId::EcmwfOpenData)
                .unwrap();
        assert!(blockers.is_empty());
    }

    #[test]
    fn supported_native_recipe_has_no_fetch_blockers() {
        assert!(
            plot_recipe_fetch_blockers("composite_reflectivity", ModelId::Hrrr)
                .unwrap()
                .is_empty()
        );
        assert!(
            plot_recipe_fetch_blockers("composite_reflectivity_uh", ModelId::RrfsA)
                .unwrap()
                .is_empty()
        );
        assert!(
            plot_recipe_fetch_blockers("composite_reflectivity_uh", ModelId::RrfsPublic)
                .unwrap()
                .is_empty()
        );
        assert!(
            plot_recipe_fetch_blockers("uh_2to5km", ModelId::Hrrr)
                .unwrap()
                .is_empty()
        );
        assert!(
            plot_recipe_fetch_blockers("smoke_pm25_native", ModelId::Hrrr)
                .unwrap()
                .is_empty()
        );
        assert!(
            plot_recipe_fetch_blockers("smoke_column", ModelId::Hrrr)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn global_models_get_explicit_native_recipe_blockers() {
        let blockers = plot_recipe_fetch_blockers("composite_reflectivity", ModelId::Gfs).unwrap();
        assert_eq!(
            blockers,
            vec![PlotRecipeBlocker {
                field_key: "composite_reflectivity",
                field_label: "Composite Reflectivity",
                reason: "Composite Reflectivity is not wired for model 'gfs'; rustwx-models only has native convective product fetch planning for HRRR/RRFS-A right now".to_string(),
            }]
        );

        let reflectivity_1km =
            plot_recipe_fetch_blockers("1km_reflectivity", ModelId::Gfs).unwrap();
        assert_eq!(
            reflectivity_1km,
            vec![PlotRecipeBlocker {
                field_key: "radar_reflectivity_1km_agl",
                field_label: "1km AGL Reflectivity",
                reason: "1km AGL Reflectivity is not wired for model 'gfs'; rustwx-models only has native convective product fetch planning for HRRR/RRFS-A right now".to_string(),
            }]
        );

        let uh = plot_recipe_fetch_blockers("uh_2to5km", ModelId::Gfs).unwrap();
        assert_eq!(
            uh,
            vec![PlotRecipeBlocker {
                field_key: "updraft_helicity",
                field_label: "Updraft Helicity",
                reason: "Updraft Helicity is not wired for model 'gfs'; rustwx-models only has native convective product fetch planning for HRRR/RRFS-A right now".to_string(),
            }]
        );
    }

    #[test]
    fn selector_support_policy_lives_in_models() {
        assert!(selector_supported_for_model(
            FieldSelector::isobaric(CanonicalField::Temperature, 500),
            ModelId::Gfs,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::isobaric(CanonicalField::Temperature, 200),
            ModelId::Gfs,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::isobaric(CanonicalField::Temperature, 250),
            ModelId::WrfGdex,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::isobaric(CanonicalField::Dewpoint, 700),
            ModelId::RrfsA,
        ));
        assert!(!selector_supported_for_model(
            FieldSelector::isobaric(CanonicalField::RelativeVorticity, 500),
            ModelId::Gfs,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::surface(CanonicalField::LandSeaMask),
            ModelId::EcmwfOpenData,
        ));
        assert!(!selector_supported_for_model(
            FieldSelector::surface(CanonicalField::LandSeaMask),
            ModelId::Hrrr,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::height_agl(CanonicalField::Temperature, 2),
            ModelId::Hrrr,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::height_agl(CanonicalField::RadarReflectivity, 1000),
            ModelId::RrfsA,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::height_agl(CanonicalField::RadarReflectivity, 1000),
            ModelId::WrfGdex,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel),
            ModelId::Hrrr,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::surface(CanonicalField::Visibility),
            ModelId::Gfs,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::nominal_top(CanonicalField::SimulatedInfraredBrightnessTemperature),
            ModelId::Hrrr,
        ));
        assert!(!selector_supported_for_model(
            FieldSelector::nominal_top(CanonicalField::SimulatedInfraredBrightnessTemperature),
            ModelId::Gfs,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::height_agl(CanonicalField::SmokeMassDensity, 8),
            ModelId::Hrrr,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::entire_atmosphere(CanonicalField::ColumnIntegratedSmoke),
            ModelId::Hrrr,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::hybrid_level(CanonicalField::SmokeMassDensity, 50),
            ModelId::Hrrr,
        ));
        assert!(selector_supported_for_model(
            FieldSelector::hybrid_level(CanonicalField::Pressure, 1),
            ModelId::Hrrr,
        ));
        assert!(!selector_supported_for_model(
            FieldSelector::height_agl(CanonicalField::SmokeMassDensity, 8),
            ModelId::RrfsA,
        ));
        assert!(!selector_supported_for_model(
            FieldSelector::hybrid_level(CanonicalField::SmokeMassDensity, 51),
            ModelId::Hrrr,
        ));
    }

    #[test]
    fn direct_surface_recipe_uses_surface_fetch_plan_when_supported() {
        let blockers = plot_recipe_fetch_blockers("2m_temperature", ModelId::Hrrr).unwrap();
        assert!(blockers.is_empty());

        let plan = plot_recipe_fetch_plan("2m_temperature_10m_winds", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "sfc");
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert!(!plan.variable_patterns().is_empty());
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::height_agl(CanonicalField::Temperature, 2),
                FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel),
                FieldSelector::height_agl(CanonicalField::UWind, 10),
                FieldSelector::height_agl(CanonicalField::VWind, 10),
            ]
        );
    }

    #[test]
    fn hrrr_pressure_recipe_prefers_indexed_subset_fetches() {
        let plan = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "prs");
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
        assert!(!plan.variable_patterns().is_empty());
    }

    #[test]
    fn wrf_gdex_pressure_recipe_prefers_whole_file_fetches() {
        let plan =
            plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::WrfGdex).unwrap();
        assert_eq!(plan.product, WRF_GDEX_DEFAULT_PRESSURE_PRODUCT);
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert!(plan.variable_patterns().is_empty());
    }

    #[test]
    fn wrf_gdex_native_reflectivity_recipe_uses_whole_file_structured_extract() {
        let plan = plot_recipe_fetch_plan("composite_reflectivity", ModelId::WrfGdex).unwrap();
        assert_eq!(plan.product, WRF_GDEX_DEFAULT_SURFACE_PRODUCT);
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert_eq!(
            plan.selectors(),
            vec![FieldSelector::entire_atmosphere(
                CanonicalField::CompositeReflectivity
            )]
        );
    }

    #[test]
    fn wrf_gdex_native_combo_and_uh_recipes_use_surface_fetch_plan() {
        for slug in ["1km_reflectivity", "uh_2to5km", "composite_reflectivity_uh"] {
            let plan = plot_recipe_fetch_plan(slug, ModelId::WrfGdex).unwrap();
            assert_eq!(plan.product, WRF_GDEX_DEFAULT_SURFACE_PRODUCT, "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::WholeFile,
                "{slug}"
            );
            assert_eq!(
                plan.fetch_mode,
                PlotRecipeFetchMode::WholeFileStructuredExtract,
                "{slug}"
            );
        }
    }

    #[test]
    fn wrf_gdex_wind_gust_recipe_is_unblocked_and_uses_surface_fetch_plan() {
        let blockers = plot_recipe_fetch_blockers("10m_wind_gusts", ModelId::WrfGdex).unwrap();
        assert!(blockers.is_empty());

        let recipe = plot_recipe("10m_wind_gusts").expect("gust recipe should exist");
        assert!(recipe.filled.idx_patterns().contains(&"WSPD10MAX"));

        let plan = plot_recipe_fetch_plan("10m_wind_gusts", ModelId::WrfGdex).unwrap();
        assert_eq!(plan.product, WRF_GDEX_DEFAULT_SURFACE_PRODUCT);
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.selectors(),
            vec![FieldSelector::height_agl(CanonicalField::WindGust, 10)]
        );
    }

    #[test]
    fn hrrr_indexed_subset_fetch_plans_cover_pressure_surface_and_native_lanes() {
        let pressure = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::Hrrr)
            .expect("pressure recipe should plan");
        let surface = plot_recipe_fetch_plan("2m_temperature_10m_winds", ModelId::Hrrr)
            .expect("surface recipe should plan");
        let native = plot_recipe_fetch_plan("composite_reflectivity_uh", ModelId::Hrrr)
            .expect("native recipe should plan");

        for plan in [pressure, surface, native] {
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset
            );
            assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
            assert!(
                !plan.variable_patterns().is_empty(),
                "indexed HRRR plans should carry idx variable patterns"
            );
        }
    }

    #[test]
    fn hrrr_direct_composite_layout_recipes_expand_to_selector_backed_components() {
        let cloud_levels = plot_recipe_fetch_plan("cloud_cover_levels", ModelId::Hrrr).unwrap();
        assert_eq!(cloud_levels.product, "sfc");
        assert_eq!(
            cloud_levels.selectors(),
            vec![
                FieldSelector::entire_atmosphere(CanonicalField::LowCloudCover),
                FieldSelector::entire_atmosphere(CanonicalField::MiddleCloudCover),
                FieldSelector::entire_atmosphere(CanonicalField::HighCloudCover),
            ]
        );

        let precipitation_type =
            plot_recipe_fetch_plan("precipitation_type", ModelId::Hrrr).unwrap();
        assert_eq!(precipitation_type.product, "sfc");
        assert_eq!(
            precipitation_type.selectors(),
            vec![
                FieldSelector::surface(CanonicalField::CategoricalRain),
                FieldSelector::surface(CanonicalField::CategoricalFreezingRain),
                FieldSelector::surface(CanonicalField::CategoricalIcePellets),
                FieldSelector::surface(CanonicalField::CategoricalSnow),
            ]
        );
    }

    #[test]
    fn hrrr_blockers_point_non_native_surface_products_to_honest_lanes() {
        let theta_e = plot_recipe_fetch_blockers("2m_theta_e_10m_winds", ModelId::Hrrr).unwrap();
        assert!(theta_e.iter().any(|blocker| {
            blocker.reason.contains("theta_e_2m_10m_winds")
                && blocker.reason.contains("derived product")
        }));

        let heat_index = plot_recipe_fetch_blockers("2m_heat_index", ModelId::Hrrr).unwrap();
        assert!(heat_index.iter().any(|blocker| {
            blocker.reason.contains("heat_index_2m") && blocker.reason.contains("derived product")
        }));

        let wind_chill = plot_recipe_fetch_blockers("2m_wind_chill", ModelId::Hrrr).unwrap();
        assert!(wind_chill.iter().any(|blocker| {
            blocker.reason.contains("wind_chill_2m") && blocker.reason.contains("derived product")
        }));

        let qpf = plot_recipe_fetch_blockers("1h_qpf", ModelId::Hrrr).unwrap();
        assert!(qpf.iter().any(|blocker| {
            blocker.reason.contains("qpf_1h") && blocker.reason.contains("windowed lane")
        }));
    }

    #[test]
    fn generic_direct_composite_layouts_follow_selector_support() {
        let cloud_levels = plot_recipe_fetch_blockers("cloud_cover_levels", ModelId::Gfs).unwrap();
        assert!(cloud_levels.is_empty());

        let precipitation_type =
            plot_recipe_fetch_blockers("precipitation_type", ModelId::EcmwfOpenData).unwrap();
        assert!(precipitation_type.iter().any(|blocker| {
            blocker.reason.contains("selector") || blocker.reason.contains("not yet supported")
        }));
    }

    #[test]
    fn nbm_probability_of_precipitation_recipe_uses_core_surface_plan() {
        let plan = plot_recipe_fetch_plan("probability_of_precipitation", ModelId::Nbm).unwrap();
        assert_eq!(plan.product, "core/co");
        assert_eq!(
            plan.fetch_policy,
            PlotRecipeFetchPolicy::PreferIndexedSubset
        );
        assert_eq!(
            plan.selectors(),
            vec![FieldSelector::surface(
                CanonicalField::ProbabilityOfPrecipitation
            )]
        );
        assert!(plan.variable_patterns().contains(&"APCP:surface"));
        assert!(
            plot_recipe_fetch_blockers("probability_of_precipitation", ModelId::Nbm)
                .unwrap()
                .is_empty()
        );

        let gfs_blockers =
            plot_recipe_fetch_blockers("probability_of_precipitation", ModelId::Gfs).unwrap();
        assert!(
            gfs_blockers
                .iter()
                .any(|blocker| blocker.reason.contains("only verified for NBM"))
        );
    }

    #[test]
    fn nbm_qmd_stat_recipes_use_qmd_product_and_exact_selectors() {
        let cases = [
            (
                "nbm_qmd_2m_temperature_mean",
                FieldSelector::height_agl(CanonicalField::Temperature, 2).with_ensemble_mean(),
            ),
            (
                "nbm_qmd_2m_temperature_stddev",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_ensemble_standard_deviation(),
            ),
            (
                "nbm_qmd_2m_temperature_p10",
                FieldSelector::height_agl(CanonicalField::Temperature, 2).with_percentile(10),
            ),
            (
                "nbm_qmd_2m_temperature_p50",
                FieldSelector::height_agl(CanonicalField::Temperature, 2).with_percentile(50),
            ),
            (
                "nbm_qmd_2m_temperature_p90",
                FieldSelector::height_agl(CanonicalField::Temperature, 2).with_percentile(90),
            ),
            (
                "nbm_qmd_prob_2m_temperature_below_273p15k",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_probability(ProbabilitySelection::below_milli(273_150)),
            ),
            (
                "nbm_qmd_prob_2m_temperature_above_305p372k",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_probability(ProbabilitySelection::above_milli(305_372)),
            ),
            (
                "nbm_qmd_2m_dewpoint_mean",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2).with_ensemble_mean(),
            ),
            (
                "nbm_qmd_2m_dewpoint_stddev",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2)
                    .with_ensemble_standard_deviation(),
            ),
            (
                "nbm_qmd_2m_dewpoint_p10",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2).with_percentile(10),
            ),
            (
                "nbm_qmd_2m_dewpoint_p50",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2).with_percentile(50),
            ),
            (
                "nbm_qmd_2m_dewpoint_p90",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2).with_percentile(90),
            ),
            (
                "nbm_qmd_prob_2m_dewpoint_above_291p483k",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2)
                    .with_probability(ProbabilitySelection::above_milli(291_483)),
            ),
            (
                "nbm_qmd_2m_relative_humidity_p10",
                FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2).with_percentile(10),
            ),
            (
                "nbm_qmd_2m_relative_humidity_p50",
                FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2).with_percentile(50),
            ),
            (
                "nbm_qmd_2m_relative_humidity_p90",
                FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2).with_percentile(90),
            ),
            (
                "nbm_qmd_10m_wind_gust_mean",
                FieldSelector::height_agl(CanonicalField::WindGust, 10).with_ensemble_mean(),
            ),
            (
                "nbm_qmd_10m_wind_gust_stddev",
                FieldSelector::height_agl(CanonicalField::WindGust, 10)
                    .with_ensemble_standard_deviation(),
            ),
            (
                "nbm_qmd_10m_wind_gust_p10",
                FieldSelector::height_agl(CanonicalField::WindGust, 10).with_percentile(10),
            ),
            (
                "nbm_qmd_10m_wind_gust_p50",
                FieldSelector::height_agl(CanonicalField::WindGust, 10).with_percentile(50),
            ),
            (
                "nbm_qmd_10m_wind_gust_p90",
                FieldSelector::height_agl(CanonicalField::WindGust, 10).with_percentile(90),
            ),
            (
                "nbm_qmd_prob_10m_wind_gust_above_24p6933ms",
                FieldSelector::height_agl(CanonicalField::WindGust, 10)
                    .with_probability(ProbabilitySelection::above_milli(24_693)),
            ),
            (
                "nbm_qmd_10m_wind_speed_mean",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10).with_ensemble_mean(),
            ),
            (
                "nbm_qmd_10m_wind_speed_stddev",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
                    .with_ensemble_standard_deviation(),
            ),
            (
                "nbm_qmd_10m_wind_speed_p10",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10).with_percentile(10),
            ),
            (
                "nbm_qmd_10m_wind_speed_p50",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10).with_percentile(50),
            ),
            (
                "nbm_qmd_10m_wind_speed_p90",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10).with_percentile(90),
            ),
            (
                "nbm_qmd_prob_10m_wind_speed_above_17p4911ms",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
                    .with_probability(ProbabilitySelection::above_milli(17_491)),
            ),
        ];

        for (slug, selector) in cases {
            let plan = plot_recipe_fetch_plan(slug, ModelId::Nbm).unwrap();
            assert_eq!(plan.product, "qmd/co", "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset,
                "{slug}"
            );
            assert_eq!(plan.selectors(), vec![selector], "{slug}");
        }
    }

    #[test]
    fn nbm_qmd_temperature_stat_recipes_block_for_non_nbm_models() {
        let blockers =
            plot_recipe_fetch_blockers("nbm_qmd_2m_temperature_p50", ModelId::Gfs).unwrap();
        assert!(
            blockers
                .iter()
                .any(|blocker| { blocker.reason.contains("not yet supported for model 'gfs'") })
        );
    }

    #[test]
    fn aigefs_spread_recipes_use_spr_products_and_stddev_selectors() {
        let cases = [
            (
                "aigefs_spr_2m_temperature_stddev",
                "sfc/spr",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_ensemble_standard_deviation(),
            ),
            (
                "aigefs_spr_500mb_height_stddev",
                "pres/spr",
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                    .with_ensemble_standard_deviation(),
            ),
        ];

        for (slug, product, selector) in cases {
            let plan = plot_recipe_fetch_plan(slug, ModelId::Aigefs).unwrap();
            assert_eq!(plan.product, product, "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset,
                "{slug}"
            );
            assert_eq!(plan.selectors(), vec![selector], "{slug}");

            let blockers = plot_recipe_fetch_blockers(slug, ModelId::Aigfs).unwrap();
            assert!(
                blockers
                    .iter()
                    .any(|blocker| blocker.reason.contains("model 'aigfs'")),
                "{slug}"
            );
        }
    }

    #[test]
    fn gefs_native_stat_recipes_use_geavg_gespr_products_and_exact_selectors() {
        let cases = [
            (
                "gefs_avg_2m_temperature_10m_winds",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::height_agl(CanonicalField::Temperature, 2).with_ensemble_mean(),
                    FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
                        .with_ensemble_mean(),
                    FieldSelector::height_agl(CanonicalField::UWind, 10).with_ensemble_mean(),
                    FieldSelector::height_agl(CanonicalField::VWind, 10).with_ensemble_mean(),
                ],
            ),
            (
                "gefs_avg_500mb_height_winds",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                        .with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::UWind, 500).with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::VWind, 500).with_ensemble_mean(),
                ],
            ),
            (
                "gefs_avg_500mb_temperature_height_winds",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::isobaric(CanonicalField::Temperature, 500).with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                        .with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::UWind, 500).with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::VWind, 500).with_ensemble_mean(),
                ],
            ),
            (
                "gefs_avg_500mb_rh_height_winds",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::isobaric(CanonicalField::RelativeHumidity, 500)
                        .with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                        .with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::UWind, 500).with_ensemble_mean(),
                    FieldSelector::isobaric(CanonicalField::VWind, 500).with_ensemble_mean(),
                ],
            ),
            (
                "gefs_avg_mslp_10m_winds",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
                        .with_ensemble_mean(),
                    FieldSelector::height_agl(CanonicalField::UWind, 10).with_ensemble_mean(),
                    FieldSelector::height_agl(CanonicalField::VWind, 10).with_ensemble_mean(),
                ],
            ),
            (
                "gefs_avg_precipitable_water",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater)
                        .with_ensemble_mean(),
                ],
            ),
            (
                "gefs_spr_2m_temperature_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::height_agl(CanonicalField::Temperature, 2)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_avg_2m_relative_humidity",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2)
                        .with_ensemble_mean(),
                ],
            ),
            (
                "gefs_spr_2m_relative_humidity_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::height_agl(CanonicalField::RelativeHumidity, 2)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_spr_500mb_height_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_spr_500mb_temperature_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::isobaric(CanonicalField::Temperature, 500)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_spr_500mb_rh_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::isobaric(CanonicalField::RelativeHumidity, 500)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_spr_mslp_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_spr_precipitable_water_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_avg_cloud_cover",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::entire_atmosphere(CanonicalField::TotalCloudCover)
                        .with_ensemble_mean(),
                ],
            ),
            (
                "gefs_spr_cloud_cover_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::entire_atmosphere(CanonicalField::TotalCloudCover)
                        .with_ensemble_standard_deviation(),
                ],
            ),
            (
                "gefs_avg_6h_qpf",
                "pgrb2ap5/geavg",
                vec![
                    FieldSelector::surface(CanonicalField::TotalPrecipitation).with_ensemble_mean(),
                ],
            ),
            (
                "gefs_spr_6h_qpf_stddev",
                "pgrb2ap5/gespr",
                vec![
                    FieldSelector::surface(CanonicalField::TotalPrecipitation)
                        .with_ensemble_standard_deviation(),
                ],
            ),
        ];

        for (slug, product, selectors) in cases {
            let plan = plot_recipe_fetch_plan(slug, ModelId::Gefs).unwrap();
            assert_eq!(plan.product, product, "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset,
                "{slug}"
            );
            assert_eq!(plan.selectors(), selectors, "{slug}");

            let blockers = plot_recipe_fetch_blockers(slug, ModelId::Gfs).unwrap();
            assert!(
                blockers
                    .iter()
                    .any(|blocker| blocker.reason.contains("model 'gfs'")),
                "{slug}"
            );
        }
    }

    #[test]
    fn hgefs_spread_recipes_use_spr_products_and_stddev_selectors() {
        let cases = [
            (
                "hgefs_spr_2m_temperature_stddev",
                "sfc/spr",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_ensemble_standard_deviation(),
            ),
            (
                "hgefs_spr_500mb_height_stddev",
                "pres/spr",
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                    .with_ensemble_standard_deviation(),
            ),
        ];

        for (slug, product, selector) in cases {
            let plan = plot_recipe_fetch_plan(slug, ModelId::Hgefs).unwrap();
            assert_eq!(plan.product, product, "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset,
                "{slug}"
            );
            assert_eq!(plan.selectors(), vec![selector], "{slug}");

            let blockers = plot_recipe_fetch_blockers(slug, ModelId::Aigefs).unwrap();
            assert!(
                blockers
                    .iter()
                    .any(|blocker| blocker.reason.contains("model 'aigefs'")),
                "{slug}"
            );
        }
    }

    #[test]
    fn href_spread_recipes_use_sprd_product_and_spread_selectors() {
        let cases = [
            (
                "href_sprd_2m_temperature",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
            (
                "href_sprd_2m_dewpoint",
                FieldSelector::height_agl(CanonicalField::Dewpoint, 2)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
            (
                "href_sprd_10m_wind_speed",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
            (
                "href_sprd_mslp",
                FieldSelector::mean_sea_level(CanonicalField::PressureReducedToMeanSeaLevel)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
            (
                "href_sprd_precipitable_water",
                FieldSelector::entire_atmosphere(CanonicalField::PrecipitableWater)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
            (
                "href_sprd_500mb_height",
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
            (
                "href_sprd_500mb_temperature",
                FieldSelector::isobaric(CanonicalField::Temperature, 500)
                    .with_product(FieldProduct::EnsembleSpread),
            ),
        ];

        for (slug, selector) in cases {
            let plan = plot_recipe_fetch_plan(slug, ModelId::Href).unwrap();
            assert_eq!(plan.product, "ensprod/conus/sprd", "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset,
                "{slug}"
            );
            assert_eq!(plan.selectors(), vec![selector], "{slug}");

            let blockers = plot_recipe_fetch_blockers(slug, ModelId::Gefs).unwrap();
            assert!(
                blockers
                    .iter()
                    .any(|blocker| blocker.reason.contains("model 'gefs'")),
                "{slug}"
            );
        }

        let generic = plot_recipe_fetch_blockers("2m_temperature", ModelId::Href).unwrap();
        assert!(
            generic
                .iter()
                .any(|blocker| { blocker.reason.contains("limited to explicit `href_sprd_*`") })
        );
    }

    #[test]
    fn sref_probability_recipes_use_prob_product_and_exact_selectors() {
        let cases = [
            (
                "sref_prob_2m_temperature_below_273k",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_probability(ProbabilitySelection::below_milli(273_000)),
            ),
            (
                "sref_prob_2m_temperature_above_298p8k",
                FieldSelector::height_agl(CanonicalField::Temperature, 2)
                    .with_probability(ProbabilitySelection::above_milli(298_800)),
            ),
            (
                "sref_prob_850mb_temperature_below_273k",
                FieldSelector::isobaric(CanonicalField::Temperature, 850)
                    .with_probability(ProbabilitySelection::below_milli(273_000)),
            ),
            (
                "sref_prob_visibility_below_1609m",
                FieldSelector::surface(CanonicalField::Visibility)
                    .with_probability(ProbabilitySelection::below_milli(1_609_000)),
            ),
            (
                "sref_prob_visibility_below_402m",
                FieldSelector::surface(CanonicalField::Visibility)
                    .with_probability(ProbabilitySelection::below_milli(402_000)),
            ),
            (
                "sref_prob_visibility_below_804m",
                FieldSelector::surface(CanonicalField::Visibility)
                    .with_probability(ProbabilitySelection::below_milli(804_000)),
            ),
            (
                "sref_prob_visibility_below_3218m",
                FieldSelector::surface(CanonicalField::Visibility)
                    .with_probability(ProbabilitySelection::below_milli(3_218_000)),
            ),
            (
                "sref_prob_10m_wind_speed_above_12p89ms",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
                    .with_probability(ProbabilitySelection::above_milli(12_890)),
            ),
            (
                "sref_prob_10m_wind_speed_above_17p5ms",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
                    .with_probability(ProbabilitySelection::above_milli(17_500)),
            ),
            (
                "sref_prob_10m_wind_speed_above_25p78ms",
                FieldSelector::height_agl(CanonicalField::WindSpeed, 10)
                    .with_probability(ProbabilitySelection::above_milli(25_780)),
            ),
        ];

        for (slug, selector) in cases {
            let plan = plot_recipe_fetch_plan(slug, ModelId::Sref).unwrap();
            assert_eq!(plan.product, "ensprod/pgrb212/prob_3hrly", "{slug}");
            assert_eq!(
                plan.fetch_policy,
                PlotRecipeFetchPolicy::PreferIndexedSubset,
                "{slug}"
            );
            assert_eq!(plan.selectors(), vec![selector], "{slug}");
        }
    }

    #[test]
    fn sref_probability_recipes_block_for_non_sref_models() {
        for model in [ModelId::Gfs, ModelId::Nbm] {
            let blockers =
                plot_recipe_fetch_blockers("sref_prob_2m_temperature_below_273k", model).unwrap();
            assert!(
                blockers
                    .iter()
                    .any(|blocker| blocker.reason.contains("only verified for SREF")),
                "{model:?}: {blockers:?}"
            );
        }
    }

    #[test]
    fn direct_upper_air_200mb_recipe_is_now_supported() {
        let blockers = plot_recipe_fetch_blockers("200mb_height_winds", ModelId::Gfs).unwrap();
        assert!(blockers.is_empty());

        let plan = plot_recipe_fetch_plan("200mb_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(plan.product, "pgrb2.0p25");
    }

    #[test]
    fn simulated_ir_recipe_remains_blocked_for_unverified_models() {
        let blockers =
            plot_recipe_fetch_blockers("simulated_ir_satellite", ModelId::EcmwfOpenData).unwrap();
        assert_eq!(blockers.len(), 1);
        assert!(
            blockers[0]
                .reason
                .contains("GRIB signature is not verified yet")
        );
    }

    #[test]
    fn smoke_recipes_remain_blocked_for_unverified_models() {
        let rrfs_smoke = plot_recipe_fetch_blockers("smoke_pm25_native", ModelId::RrfsA).unwrap();
        assert_eq!(rrfs_smoke.len(), 1);
        assert!(rrfs_smoke[0].reason.contains("HRRR wrfnat"));

        let gfs_column = plot_recipe_fetch_blockers("smoke_column", ModelId::Gfs).unwrap();
        assert_eq!(gfs_column.len(), 1);
        assert!(gfs_column[0].reason.contains("native smoke GRIB signature"));
    }

    #[test]
    fn lightning_flash_density_blocker_uses_verified_hrrr_message_evidence() {
        let blockers =
            plot_recipe_fetch_blockers("lightning_flash_density", ModelId::Hrrr).unwrap();
        assert_eq!(blockers.len(), 1);
        let reason = &blockers[0].reason;
        assert!(reason.contains("LTNGSD"));
        assert!(reason.contains("discipline 0/category 17/number 0"));
        assert!(reason.contains("m^-2 s^-1"));
        assert!(reason.contains("LTNG"));
        assert!(reason.contains("flash-density parameters 2/3/4"));
    }

    #[test]
    fn hrrr_urls_match_expected_operational_paths() {
        let request = ModelRunRequest::new(
            ModelId::Hrrr,
            CycleSpec::new("20260414", 19).unwrap(),
            2,
            "sfc",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.20260414/conus/hrrr.t19z.wrfsfcf02.grib2"
        );
    }

    #[test]
    fn gfs_urls_match_expected_operational_paths() {
        let request = ModelRunRequest::new(
            ModelId::Gfs,
            CycleSpec::new("20260414", 18).unwrap(),
            12,
            "pgrb2.0p25",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20260414/18/atmos/gfs.t18z.pgrb2.0p25.f012"
        );
    }

    #[test]
    fn ecmwf_urls_match_open_data_feed() {
        let request = ModelRunRequest::new(
            ModelId::EcmwfOpenData,
            CycleSpec::new("20260414", 12).unwrap(),
            6,
            "oper",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://data.ecmwf.int/forecasts/20260414/12z/ifs/0p25/oper/20260414120000-6h-oper-fc.grib2"
        );
    }

    #[test]
    fn ecmwf_urls_support_six_utc_cycles() {
        let request = ModelRunRequest::new(
            ModelId::EcmwfOpenData,
            CycleSpec::new("20260414", 6).unwrap(),
            144,
            "oper",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://data.ecmwf.int/forecasts/20260414/06z/ifs/0p25/oper/20260414060000-144h-oper-fc.grib2"
        );
    }

    #[test]
    fn ecmwf_urls_reject_unsupported_cycle_hour_cadence() {
        let request = ModelRunRequest::new(
            ModelId::EcmwfOpenData,
            CycleSpec::new("20260414", 6).unwrap(),
            150,
            "oper",
        )
        .unwrap();
        let err = resolve_urls(&request).unwrap_err();
        assert!(matches!(
            err,
            ModelError::UnsupportedForecastHour {
                model: ModelId::EcmwfOpenData,
                cycle_hour: 6,
                forecast_hour: 150,
                ..
            }
        ));
    }

    #[test]
    fn rrfs_a_urls_match_live_bucket_pattern() {
        let request = ModelRunRequest::new(
            ModelId::RrfsA,
            CycleSpec::new("20260414", 20).unwrap(),
            2,
            "prs-conus",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.20260414/20/rrfs.t20z.prslev.3km.f002.conus.grib2"
        );
        assert_eq!(
            urls[0].idx_url.as_deref(),
            Some(
                "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.20260414/20/rrfs.t20z.prslev.3km.f002.conus.grib2.idx"
            )
        );
    }

    #[test]
    fn rrfs_public_urls_match_public_bucket_pattern() {
        let request = ModelRunRequest::new(
            ModelId::RrfsPublic,
            CycleSpec::new("20260507", 0).unwrap(),
            24,
            "prs-conus",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/rrfs.20260507/00/rrfs.t00z.prslev.3km.f024.conus.grib2"
        );
        assert_eq!(
            urls[0].idx_url.as_deref(),
            Some(
                "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/rrfs.20260507/00/rrfs.t00z.prslev.3km.f024.conus.grib2.idx"
            )
        );
    }

    #[test]
    fn rrfs_firewx_urls_match_public_firewx_bucket_pattern() {
        let request = ModelRunRequest::new(
            ModelId::RrfsFireWx,
            CycleSpec::new("20260507", 6).unwrap(),
            24,
            "prs-firewx",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/firewx.20260507/06/rrfs.t06z.prslev.1p5km.f024.firewx_lcc.grib2"
        );
        assert_eq!(
            urls[0].idx_url.as_deref(),
            Some(
                "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_public/firewx.20260507/06/rrfs.t06z.prslev.1p5km.f024.firewx_lcc.grib2.idx"
            )
        );
    }

    #[test]
    fn rrfs_a_subhourly_hi_urls_match_live_bucket_pattern() {
        let request = ModelRunRequest::new(
            ModelId::RrfsA,
            CycleSpec::new("20260414", 20).unwrap(),
            2,
            "subh-hi",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.20260414/20/rrfs.t20z.prslev.2p5km.subh.f002.hi.grib2"
        );
    }

    #[test]
    fn wrf_gdex_urls_match_gdex_wrfout_pattern() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("20150101", 0).unwrap(),
            0,
            "d010047-d01",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/d010047/201501/wrfout_d01_2015-01-01_00:00:00.nc"
        );
        assert!(urls[0].idx_url.is_none());
    }

    #[test]
    fn wrf_gdex_urls_roll_forward_by_forecast_hour() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("20150101", 0).unwrap(),
            15,
            "d010047-d01",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/d010047/201501/wrfout_d01_2015-01-01_15:00:00.nc"
        );
    }

    #[test]
    fn wrf_gdex_hist2d_urls_match_osdf_pattern() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("19950101", 0).unwrap(),
            12,
            "d612005-hist2d",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/g/d612005/hist2D/199501/wrf2d_d01_1995-01-01_12:00:00.nc"
        );
    }

    #[test]
    fn wrf_gdex_hist3d_urls_match_osdf_pattern() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("19950101", 0).unwrap(),
            12,
            "d612005-hist3d",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/g/d612005/hist3D/199501/wrf3d_d01_1995-01-01_12:00:00.nc"
        );
    }

    #[test]
    fn wrf_gdex_future2d_urls_match_osdf_pattern() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("20800101", 0).unwrap(),
            0,
            "d612005-future2d",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/g/d612005/future2D/208001/wrf2d_d01_2080-01-01_00:00:00.nc"
        );
    }

    #[test]
    fn wrf_gdex_future3d_urls_match_osdf_pattern() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("20800101", 0).unwrap(),
            0,
            "d612005-future3d",
        )
        .unwrap();
        let urls = resolve_urls(&request).unwrap();
        assert_eq!(
            urls[0].grib_url,
            "https://tds.gdex.ucar.edu/thredds/fileServer/files/g/d612005/future3D/208001/wrf3d_d01_2080-01-01_00:00:00.nc"
        );
    }

    #[test]
    fn wrf_gdex_hist3d_rejects_off_cadence_valid_times() {
        let request = ModelRunRequest::new(
            ModelId::WrfGdex,
            CycleSpec::new("19950101", 0).unwrap(),
            1,
            "d612005-hist3d",
        )
        .unwrap();
        let err = resolve_urls(&request).unwrap_err();
        assert!(
            err.to_string().contains("every 3 hours"),
            "unexpected error: {err}"
        );
    }
}
