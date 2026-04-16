use rustwx_core::{
    CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, ProductKeyMetadata,
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
    Solar07Cape,
    Solar07Cin,
    Solar07Reflectivity,
    Solar07Uh,
    Solar07Temperature,
    Solar07Dewpoint,
    Solar07Rh,
    Solar07Winds,
    Solar07Height,
    Solar07Pressure,
    Solar07WindGust,
    Solar07CloudCover,
    Solar07PrecipitableWater,
    Solar07Qpf,
    Solar07Categorical,
    Solar07Visibility,
    Solar07RadarReflectivity,
    Solar07Satellite,
    Solar07Lightning,
    Solar07Vorticity,
    Solar07Stp,
    Solar07Scp,
    Solar07Ehi,
}

fn recipe_lineage(slug: &str, family: ProductFamily) -> ProductLineage {
    match slug {
        "2m_theta_e_10m_winds" | "2m_heat_index" | "2m_wind_chill" => ProductLineage::Derived,
        "1h_qpf" => ProductLineage::Windowed,
        _ => family.default_lineage(),
    }
}

fn recipe_maturity(slug: &str) -> ProductMaturity {
    match slug {
        "simulated_ir_satellite" | "lightning_flash_density" => ProductMaturity::Experimental,
        _ => ProductMaturity::Operational,
    }
}

fn recipe_flags(slug: &str) -> Vec<ProductSemanticFlag> {
    match slug {
        "cloud_cover_levels" | "precipitation_type" | "composite_reflectivity_uh" => {
            vec![ProductSemanticFlag::Composite]
        }
        "1h_qpf" => vec![ProductSemanticFlag::Alias],
        _ => Vec::new(),
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
        let mut provenance = ProductProvenance::new(
            recipe_lineage(self.slug, self.filled.family),
            recipe_maturity(self.slug),
        );
        if let Some(selector) = self.filled.selector {
            provenance = provenance.with_selector(selector);
        }
        if matches!(provenance.lineage, ProductLineage::Windowed) {
            provenance = provenance.with_window(ProductWindowSpec {
                process: StatisticalProcess::Accumulation,
                duration_hours: None,
            });
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestRun {
    pub model: ModelId,
    pub cycle: CycleSpec,
    pub source: SourceId,
}

#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error(transparent)]
    Core(#[from] RustwxError),
    #[error("unsupported product '{product}' for model '{model}'")]
    UnsupportedProduct { model: ModelId, product: String },
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
const ECMWF_CYCLE_HOURS: &[u8] = &[0, 12];
const RRFS_A_CYCLE_HOURS: &[u8] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];

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

const ECMWF_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Ecmwf,
    idx_available: true,
    priority: 1,
    max_age_hours: None,
    notes: "ECMWF open data",
}];

const RRFS_A_SOURCES: &[SourceDescriptor] = &[SourceDescriptor {
    id: SourceId::Aws,
    idx_available: true,
    priority: 1,
    max_age_hours: None,
    notes: "NOAA RRFS AWS bucket",
}];

const MODELS: &[ModelSummary] = &[
    ModelSummary {
        id: ModelId::Hrrr,
        description: "HRRR 3 km CONUS rapid-refresh forecast",
        default_product: "sfc",
        cycle_hours_utc: HRRR_CYCLE_HOURS,
        max_forecast_hour: 48,
        sources: HRRR_SOURCES,
    },
    ModelSummary {
        id: ModelId::Gfs,
        description: "GFS global 0.25 degree atmospheric grid",
        default_product: "pgrb2.0p25",
        cycle_hours_utc: GFS_CYCLE_HOURS,
        max_forecast_hour: 384,
        sources: GFS_SOURCES,
    },
    ModelSummary {
        id: ModelId::EcmwfOpenData,
        description: "ECMWF open data IFS 0.25 degree feed",
        default_product: "oper",
        cycle_hours_utc: ECMWF_CYCLE_HOURS,
        max_forecast_hour: 240,
        sources: ECMWF_SOURCES,
    },
    ModelSummary {
        id: ModelId::RrfsA,
        description: "RRFS-A AWS open data feed with CONUS/NA/HI/PR variants",
        default_product: "prs-conus",
        cycle_hours_utc: RRFS_A_CYCLE_HOURS,
        max_forecast_hour: 60,
        sources: RRFS_A_SOURCES,
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

const FIELD_2M_TEMP: GribFieldSpec = field_spec(
    "temperature_2m_agl",
    "2m AGL Temperature",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(2),
    Some(FieldSelector::height_agl(CanonicalField::Temperature, 2)),
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

const FIELD_10M_WIND_GUST: GribFieldSpec = field_spec(
    "wind_gust_10m_agl",
    "10m AGL Wind Gust",
    ProductFamily::Surface,
    GribLevelKind::HeightAboveGround,
    Some(10),
    Some(FieldSelector::height_agl(CanonicalField::WindGust, 10)),
    &["GUST:surface", "GUST:10 m above ground"],
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

const PLOT_RECIPES: &[PlotRecipe] = &[
    PlotRecipe {
        slug: "200mb_height_winds",
        title: "200mb Height / Winds",
        filled: FIELD_200_HEIGHT,
        contours: None,
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::Solar07Height,
    },
    PlotRecipe {
        slug: "300mb_height_winds",
        title: "300mb Height / Winds",
        filled: FIELD_300_HEIGHT,
        contours: None,
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::Solar07Height,
    },
    PlotRecipe {
        slug: "500mb_height_winds",
        title: "500mb Height / Winds",
        filled: FIELD_500_HEIGHT,
        contours: None,
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::Solar07Height,
    },
    PlotRecipe {
        slug: "700mb_height_winds",
        title: "700mb Height / Winds",
        filled: FIELD_700_HEIGHT,
        contours: None,
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::Solar07Height,
    },
    PlotRecipe {
        slug: "850mb_height_winds",
        title: "850mb Height / Winds",
        filled: FIELD_850_HEIGHT,
        contours: None,
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::Solar07Height,
    },
    PlotRecipe {
        slug: "200mb_temperature_height_winds",
        title: "200mb Temperature / Height / Winds",
        filled: FIELD_200_TEMP,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "300mb_temperature_height_winds",
        title: "300mb Temperature / Height / Winds",
        filled: FIELD_300_TEMP,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "500mb_temperature_height_winds",
        title: "500mb Temperature / Height / Winds",
        filled: FIELD_500_TEMP,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "850mb_temperature_height_winds",
        title: "850mb Temperature / Height / Winds",
        filled: FIELD_850_TEMP,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "700mb_temperature_height_winds",
        title: "700mb Temperature / Height / Winds",
        filled: FIELD_700_TEMP,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "2m_relative_humidity",
        title: "2m AGL Relative Humidity",
        filled: FIELD_2M_RH,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Rh,
    },
    PlotRecipe {
        slug: "2m_temperature",
        title: "2m AGL Temperature",
        filled: FIELD_2M_TEMP,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "2m_temperature_10m_winds",
        title: "2m AGL Temperature / 10m Winds",
        filled: FIELD_2M_TEMP,
        contours: None,
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "2m_dewpoint",
        title: "2m AGL Dewpoint",
        filled: FIELD_2M_DEWPOINT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Dewpoint,
    },
    PlotRecipe {
        slug: "2m_dewpoint_10m_winds",
        title: "2m AGL Dewpoint / 10m Winds",
        filled: FIELD_2M_DEWPOINT,
        contours: None,
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::Solar07Dewpoint,
    },
    PlotRecipe {
        slug: "mslp_10m_winds",
        title: "MSLP / 10m Winds",
        filled: FIELD_MSLP,
        contours: None,
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::Solar07Pressure,
    },
    PlotRecipe {
        slug: "10m_wind_gusts",
        title: "10m AGL Wind Gusts",
        filled: FIELD_10M_WIND_GUST,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07WindGust,
    },
    PlotRecipe {
        slug: "precipitable_water",
        title: "Precipitable Water",
        filled: FIELD_PWAT,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07PrecipitableWater,
    },
    PlotRecipe {
        slug: "cloud_cover",
        title: "Cloud Cover",
        filled: FIELD_TOTAL_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07CloudCover,
    },
    PlotRecipe {
        slug: "low_cloud_cover",
        title: "Low Cloud Cover",
        filled: FIELD_LOW_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07CloudCover,
    },
    PlotRecipe {
        slug: "middle_cloud_cover",
        title: "Middle Cloud Cover",
        filled: FIELD_MIDDLE_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07CloudCover,
    },
    PlotRecipe {
        slug: "high_cloud_cover",
        title: "High Cloud Cover",
        filled: FIELD_HIGH_CLOUD_COVER,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07CloudCover,
    },
    PlotRecipe {
        slug: "cloud_cover_levels",
        title: "Cloud Cover, Levels",
        filled: FIELD_CLOUD_COVER_LEVELS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07CloudCover,
    },
    PlotRecipe {
        slug: "visibility",
        title: "Visibility",
        filled: FIELD_VISIBILITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Visibility,
    },
    PlotRecipe {
        slug: "simulated_ir_satellite",
        title: "Simulated IR Satellite",
        filled: FIELD_SIMULATED_IR,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Satellite,
    },
    PlotRecipe {
        slug: "lightning_flash_density",
        title: "Lightning Flash Density",
        filled: FIELD_LIGHTNING_FLASH_DENSITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Lightning,
    },
    PlotRecipe {
        slug: "total_qpf",
        title: "Total QPF",
        filled: FIELD_TOTAL_QPF,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Qpf,
    },
    PlotRecipe {
        slug: "1h_qpf",
        title: "1h QPF",
        filled: FIELD_ONE_HOUR_QPF,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Qpf,
    },
    PlotRecipe {
        slug: "categorical_rain",
        title: "Categorical Rain",
        filled: FIELD_CATEGORICAL_RAIN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Categorical,
    },
    PlotRecipe {
        slug: "categorical_freezing_rain",
        title: "Categorical Freezing Rain",
        filled: FIELD_CATEGORICAL_FREEZING_RAIN,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Categorical,
    },
    PlotRecipe {
        slug: "categorical_ice_pellets",
        title: "Categorical Ice Pellets",
        filled: FIELD_CATEGORICAL_ICE_PELLETS,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Categorical,
    },
    PlotRecipe {
        slug: "categorical_snow",
        title: "Categorical Snow",
        filled: FIELD_CATEGORICAL_SNOW,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Categorical,
    },
    PlotRecipe {
        slug: "precipitation_type",
        title: "Precipitation Type",
        filled: FIELD_PRECIPITATION_TYPE,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Categorical,
    },
    PlotRecipe {
        slug: "2m_theta_e_10m_winds",
        title: "2m AGL Theta-e / 10m Winds",
        filled: FIELD_2M_THETA_E,
        contours: None,
        barbs_u: Some(FIELD_10M_U),
        barbs_v: Some(FIELD_10M_V),
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "2m_heat_index",
        title: "2m AGL Heat Index",
        filled: FIELD_2M_HEAT_INDEX,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "2m_wind_chill",
        title: "2m AGL Wind Chill",
        filled: FIELD_2M_WIND_CHILL,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Temperature,
    },
    PlotRecipe {
        slug: "700mb_dewpoint_height_winds",
        title: "700mb Dewpoint / Height / Winds",
        filled: FIELD_700_DEWPOINT,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::Solar07Dewpoint,
    },
    PlotRecipe {
        slug: "850mb_dewpoint_height_winds",
        title: "850mb Dewpoint / Height / Winds",
        filled: FIELD_850_DEWPOINT,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::Solar07Dewpoint,
    },
    PlotRecipe {
        slug: "200mb_rh_height_winds",
        title: "200mb RH / Height / Winds",
        filled: FIELD_200_RH,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::Solar07Rh,
    },
    PlotRecipe {
        slug: "300mb_rh_height_winds",
        title: "300mb RH / Height / Winds",
        filled: FIELD_300_RH,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::Solar07Rh,
    },
    PlotRecipe {
        slug: "500mb_rh_height_winds",
        title: "500mb RH / Height / Winds",
        filled: FIELD_500_RH,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::Solar07Rh,
    },
    PlotRecipe {
        slug: "700mb_rh_height_winds",
        title: "700mb RH / Height / Winds",
        filled: FIELD_700_RH,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::Solar07Rh,
    },
    PlotRecipe {
        slug: "850mb_rh_height_winds",
        title: "850mb RH / Height / Winds",
        filled: FIELD_850_RH,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::Solar07Rh,
    },
    PlotRecipe {
        slug: "200mb_absolute_vorticity_height_winds",
        title: "200mb Absolute Vorticity / Height / Winds",
        filled: FIELD_200_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_200_HEIGHT),
        barbs_u: Some(FIELD_200_U),
        barbs_v: Some(FIELD_200_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "300mb_absolute_vorticity_height_winds",
        title: "300mb Absolute Vorticity / Height / Winds",
        filled: FIELD_300_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_300_HEIGHT),
        barbs_u: Some(FIELD_300_U),
        barbs_v: Some(FIELD_300_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "500mb_absolute_vorticity_height_winds",
        title: "500mb Absolute Vorticity / Height / Winds",
        filled: FIELD_500_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "700mb_absolute_vorticity_height_winds",
        title: "700mb Absolute Vorticity / Height / Winds",
        filled: FIELD_700_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "850mb_absolute_vorticity_height_winds",
        title: "850mb Absolute Vorticity / Height / Winds",
        filled: FIELD_850_ABSOLUTE_VORTICITY,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "1km_reflectivity",
        title: "1km AGL Reflectivity",
        filled: FIELD_1KM_REFLECTIVITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07RadarReflectivity,
    },
    PlotRecipe {
        slug: "composite_reflectivity",
        title: "Composite Reflectivity",
        filled: FIELD_COMPOSITE_REFLECTIVITY,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Reflectivity,
    },
    PlotRecipe {
        slug: "composite_reflectivity_uh",
        title: "Composite Reflectivity / UH",
        filled: FIELD_COMPOSITE_REFLECTIVITY,
        contours: Some(FIELD_UH),
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Reflectivity,
    },
    PlotRecipe {
        slug: "uh_2to5km",
        title: "Updraft Helicity 2-5 km",
        filled: FIELD_UH,
        contours: None,
        barbs_u: None,
        barbs_v: None,
        style: RenderStyle::Solar07Uh,
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
        (CanonicalField::WindGust, VerticalSelector::HeightAboveGroundMeters(10)) => true,
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
        (CanonicalField::TotalPrecipitation, VerticalSelector::Surface) => true,
        (CanonicalField::Visibility, VerticalSelector::Surface) => true,
        (
            CanonicalField::CategoricalRain
            | CanonicalField::CategoricalFreezingRain
            | CanonicalField::CategoricalIcePellets
            | CanonicalField::CategoricalSnow,
            VerticalSelector::Surface,
        ) => matches!(model, ModelId::Hrrr | ModelId::Gfs | ModelId::RrfsA),
        (CanonicalField::LandSeaMask, VerticalSelector::Surface) => {
            matches!(model, ModelId::EcmwfOpenData)
        }
        (CanonicalField::RadarReflectivity, VerticalSelector::HeightAboveGroundMeters(1000)) => {
            matches!(model, ModelId::Hrrr | ModelId::RrfsA)
        }
        (CanonicalField::CompositeReflectivity, VerticalSelector::EntireAtmosphere) => {
            matches!(model, ModelId::Hrrr | ModelId::RrfsA)
        }
        (
            CanonicalField::UpdraftHelicity,
            VerticalSelector::HeightAboveGroundLayerMeters {
                bottom_m: 2000,
                top_m: 5000,
            },
        ) => matches!(model, ModelId::Hrrr | ModelId::RrfsA),
        (CanonicalField::SimulatedInfraredBrightnessTemperature, VerticalSelector::NominalTop) => {
            matches!(model, ModelId::Hrrr)
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

pub fn resolve_urls(request: &ModelRunRequest) -> Result<Vec<ResolvedUrl>, ModelError> {
    let mut urls = model_summary(request.model)
        .sources
        .iter()
        .map(|source| {
            let grib_url = build_grib_url(source.id, request)?;
            let idx_url = if source.idx_available {
                Some(format!("{grib_url}.idx"))
            } else {
                None
            };
            Ok(ResolvedUrl {
                source: source.id,
                grib_url,
                idx_url,
            })
        })
        .collect::<Result<Vec<_>, ModelError>>()?;
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

fn latest_available_run_with_probe<F>(
    model: ModelId,
    source: Option<SourceId>,
    date_yyyymmdd: &str,
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

    for hour_utc in summary.cycle_hours_utc.iter().rev().copied() {
        let cycle = CycleSpec::new(date_yyyymmdd, hour_utc)?;
        let request = ModelRunRequest::new(model, cycle.clone(), 0, summary.default_product)?;
        let available = resolve_urls(&request)?
            .into_iter()
            .filter(|resolved| allowed_sources.contains(&resolved.source))
            .find(|resolved| probe_available(resolved));

        if let Some(resolved) = available {
            return Ok(LatestRun {
                model,
                cycle,
                source: resolved.source,
            });
        }
    }

    Err(ModelError::NoAvailableRun { model })
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
        ModelId::Gfs => build_gfs_url(source, request),
        ModelId::EcmwfOpenData => build_ecmwf_url(source, request)?,
        ModelId::RrfsA => build_rrfs_a_url(source, request)?,
    })
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

fn build_gfs_url(source: SourceId, request: &ModelRunRequest) -> String {
    match source {
        SourceId::Aws => format!(
            "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{}/{:02}/atmos/gfs.t{:02}z.pgrb2.0p25.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            request.cycle.hour_utc,
            request.forecast_hour
        ),
        SourceId::Nomads => format!(
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{}/{:02}/atmos/gfs.t{:02}z.pgrb2.0p25.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            request.cycle.hour_utc,
            request.forecast_hour
        ),
        SourceId::Google => format!(
            "https://storage.googleapis.com/global-forecast-system/gfs.{}/{:02}/atmos/gfs.t{:02}z.pgrb2.0p25.f{:03}",
            request.cycle.date_yyyymmdd,
            request.cycle.hour_utc,
            request.cycle.hour_utc,
            request.forecast_hour
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
    }
}

fn build_ecmwf_url(source: SourceId, request: &ModelRunRequest) -> Result<String, ModelError> {
    if source != SourceId::Ecmwf {
        return Ok(unsupported_source(source, request.model));
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
    let has_native = fields
        .iter()
        .any(|field| field.family == ProductFamily::Native);
    let has_surface = fields
        .iter()
        .any(|field| field.family == ProductFamily::Surface);
    match (model, has_native, has_surface) {
        (ModelId::Hrrr, true, _) => ("nat", PlotRecipeFetchPolicy::WholeFile),
        (ModelId::Hrrr, false, true) => ("sfc", PlotRecipeFetchPolicy::WholeFile),
        (ModelId::Hrrr, false, false) => ("prs", PlotRecipeFetchPolicy::WholeFile),
        (ModelId::Gfs, _, _) => ("pgrb2.0p25", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::RrfsA, _, _) => ("prs-conus", PlotRecipeFetchPolicy::PreferIndexedSubset),
        (ModelId::EcmwfOpenData, _, _) => ("oper", PlotRecipeFetchPolicy::WholeFile),
    }
}

fn native_field_gap_reason(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (field.key, model) {
        (
            "composite_reflectivity" | "radar_reflectivity_1km_agl" | "updraft_helicity",
            ModelId::Gfs | ModelId::EcmwfOpenData,
        ) => Some(format!(
            "{} is not wired for model '{model}'; rustwx-models only has native convective product fetch planning for HRRR/RRFS-A right now",
            field.label
        )),
        (
            "simulated_infrared_brightness_temperature",
            ModelId::Gfs | ModelId::EcmwfOpenData | ModelId::RrfsA,
        ) => Some(format!(
            "{} is only verified and wired for HRRR right now; the native GRIB signature is not verified yet for model '{model}'",
            field.label
        )),
        _ => None,
    }
}

fn model_specific_pressure_field_gap(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (model, field.key) {
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
        _ => None,
    }
}

fn model_specific_surface_field_gap(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (model, field.key) {
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
        (ModelId::Hrrr, "cloud_cover_levels") => None,
        (_, "cloud_cover_levels") => Some(
            "Cloud Cover, Levels is currently wired only in the HRRR direct composite lane; other model runners still expose the honest native components separately as low_cloud_cover, middle_cloud_cover, and high_cloud_cover".to_string(),
        ),
        (ModelId::Hrrr, "one_hour_qpf") => Some(
            "1h QPF is handled honestly in the HRRR windowed lane as 'qpf_1h' (legacy plot-recipe slug '1h_qpf'); do not treat it as a native/direct APCP recipe.".to_string(),
        ),
        (_, "one_hour_qpf") => Some(
            "1h QPF is not yet exposed as a generic native recipe because APCP accumulation windows vary by model and forecast hour.".to_string(),
        ),
        (ModelId::Hrrr, "precipitation_type") => None,
        (_, "precipitation_type") => Some(
            "Precipitation Type is currently wired only in the HRRR direct composite lane; other model runners still expose the honest native phase flags separately as categorical_rain, categorical_freezing_rain, categorical_ice_pellets, and categorical_snow".to_string(),
        ),
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
    matches!(level_hpa, 200 | 300 | 500 | 700 | 850)
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
    model: ModelId,
) -> Vec<&'static GribFieldSpec> {
    let mut fields = match (model, recipe.slug) {
        (ModelId::Hrrr, "cloud_cover_levels") => vec![
            &FIELD_LOW_CLOUD_COVER,
            &FIELD_MIDDLE_CLOUD_COVER,
            &FIELD_HIGH_CLOUD_COVER,
        ],
        (ModelId::Hrrr, "precipitation_type") => vec![
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
        assert_eq!(built_in_models().len(), 4);
        assert_eq!(model_summary(ModelId::RrfsA).default_product, "prs-conus");
    }

    #[test]
    fn built_in_plot_recipes_cover_current_direct_atmos_surface_and_radar_maps() {
        assert!(plot_recipe("200mb_height_winds").is_some());
        assert!(plot_recipe("300mb_height_winds").is_some());
        assert!(plot_recipe("500mb_height_winds").is_some());
        assert!(plot_recipe("700mb_height_winds").is_some());
        assert!(plot_recipe("850mb_height_winds").is_some());
        assert!(plot_recipe("200mb_temperature_height_winds").is_some());
        assert!(plot_recipe("300mb_temperature_height_winds").is_some());
        assert!(plot_recipe("500mb_temperature_height_winds").is_some());
        assert!(plot_recipe("700mb_temperature_height_winds").is_some());
        assert!(plot_recipe("850mb_temperature_height_winds").is_some());
        assert!(plot_recipe("2m_relative_humidity").is_some());
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
            Some(ProductWindowSpec {
                process: StatisticalProcess::Accumulation,
                duration_hours: None,
            })
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
        assert_eq!(
            plan.variable_patterns(),
            vec!["TMP:500 mb", "HGT:500 mb", "UGRD:500 mb", "VGRD:500 mb"]
        );
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
            plan.variable_patterns(),
            vec!["TMP:200 mb", "HGT:200 mb", "UGRD:200 mb", "VGRD:200 mb"]
        );
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
            plan.variable_patterns(),
            vec!["RH:300 mb", "HGT:300 mb", "UGRD:300 mb", "VGRD:300 mb"]
        );
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
            ModelId::Gfs,
            ModelId::EcmwfOpenData,
            ModelId::RrfsA,
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
                assert!(reason.contains("700 hPa temperature/height/wind selectors"));
                match model {
                    ModelId::EcmwfOpenData => {
                        assert!(reason.contains("whole-file structured extraction"));
                    }
                    ModelId::Hrrr | ModelId::Gfs | ModelId::RrfsA => {
                        assert!(reason.contains("idx subsetting can stage the GRIB messages"));
                    }
                }
            }
        }
    }

    #[test]
    fn dewpoint_and_700mb_recipe_blockers_are_explicit() {
        for model in [ModelId::Hrrr, ModelId::Gfs, ModelId::RrfsA] {
            let dewpoint_850 =
                plot_recipe_fetch_blockers("850mb_dewpoint_height_winds", model).unwrap();
            assert!(dewpoint_850.is_empty());

            let dewpoint_700 =
                plot_recipe_fetch_blockers("700mb_dewpoint_height_winds", model).unwrap();
            assert!(dewpoint_700.is_empty());
        }

        let plan = plot_recipe_fetch_plan("700mb_dewpoint_height_winds", ModelId::Gfs).unwrap();
        assert_eq!(
            plan.selectors(),
            vec![
                FieldSelector::isobaric(CanonicalField::Dewpoint, 700),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 700),
                FieldSelector::isobaric(CanonicalField::UWind, 700),
                FieldSelector::isobaric(CanonicalField::VWind, 700),
            ]
        );
        assert_eq!(
            plan.variable_patterns(),
            vec!["DPT:700 mb", "HGT:700 mb", "UGRD:700 mb", "VGRD:700 mb"]
        );

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
            plan.variable_patterns(),
            vec!["ABSV:700 mb", "HGT:700 mb", "UGRD:700 mb", "VGRD:700 mb"]
        );

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
    fn nomads_uses_range_probe_policy() {
        assert!(should_use_range_probe(SourceId::Nomads));
        assert!(!should_use_range_probe(SourceId::Aws));
    }

    #[test]
    fn hrrr_native_reflectivity_recipe_produces_nat_fetch_plan() {
        let plan = plot_recipe_fetch_plan("composite_reflectivity", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "nat");
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
    }

    #[test]
    fn simulated_ir_recipe_is_supported_for_hrrr_native_fetch() {
        let plan = plot_recipe_fetch_plan("simulated_ir_satellite", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "nat");
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert_eq!(
            plan.selectors(),
            vec![FieldSelector::nominal_top(
                CanonicalField::SimulatedInfraredBrightnessTemperature
            )]
        );
        assert!(plan.variable_patterns().is_empty());
        assert!(
            plot_recipe_fetch_blockers("simulated_ir_satellite", ModelId::Hrrr)
                .unwrap()
                .is_empty()
        );
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
            plot_recipe_fetch_blockers("uh_2to5km", ModelId::Hrrr)
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
    }

    #[test]
    fn direct_surface_recipe_uses_surface_fetch_plan_when_supported() {
        let blockers = plot_recipe_fetch_blockers("2m_temperature", ModelId::Hrrr).unwrap();
        assert!(blockers.is_empty());

        let plan = plot_recipe_fetch_plan("2m_temperature_10m_winds", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "sfc");
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert!(plan.variable_patterns().is_empty());
    }

    #[test]
    fn hrrr_pressure_recipe_prefers_whole_file_fetches() {
        let plan = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "prs");
        assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
        assert_eq!(
            plan.fetch_mode,
            PlotRecipeFetchMode::WholeFileStructuredExtract
        );
        assert!(plan.variable_patterns().is_empty());
    }

    #[test]
    fn hrrr_full_file_fetch_plans_cover_pressure_surface_and_native_lanes() {
        let pressure = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::Hrrr)
            .expect("pressure recipe should plan");
        let surface = plot_recipe_fetch_plan("2m_temperature_10m_winds", ModelId::Hrrr)
            .expect("surface recipe should plan");
        let native = plot_recipe_fetch_plan("composite_reflectivity_uh", ModelId::Hrrr)
            .expect("native recipe should plan");

        for plan in [pressure, surface, native] {
            assert_eq!(plan.fetch_policy, PlotRecipeFetchPolicy::WholeFile);
            assert_eq!(
                plan.fetch_mode,
                PlotRecipeFetchMode::WholeFileStructuredExtract
            );
            assert!(
                plan.variable_patterns().is_empty(),
                "whole-file HRRR plans should not depend on idx variable patterns"
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
    fn non_hrrr_models_keep_direct_composite_layouts_blocked() {
        let cloud_levels = plot_recipe_fetch_blockers("cloud_cover_levels", ModelId::Gfs).unwrap();
        assert!(cloud_levels.iter().any(|blocker| {
            blocker.reason.contains("HRRR direct composite lane")
                && blocker.field_label.contains("Cloud Cover Levels")
        }));

        let precipitation_type =
            plot_recipe_fetch_blockers("precipitation_type", ModelId::EcmwfOpenData).unwrap();
        assert!(precipitation_type.iter().any(|blocker| {
            blocker.reason.contains("HRRR direct composite lane")
                && blocker.field_label.contains("Precipitation Type")
        }));
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
}
