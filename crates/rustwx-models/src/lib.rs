use rayon::prelude::*;
use rustwx_core::{
    CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, ResolvedUrl, RustwxError,
    SourceId,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum GribLevelKind {
    Surface,
    HeightAboveGround,
    IsobaricHpa,
    EntireAtmosphere,
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
    Solar07Vorticity,
    Solar07Stp,
    Solar07Scp,
    Solar07Ehi,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PlotRecipeFetchMode {
    IndexedSubset,
    WholeFileStructuredExtract,
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
    &["DPT:700 mb", "RH:700 mb", "TMP:700 mb", "SPFH:700 mb"],
);

const FIELD_850_DEWPOINT: GribFieldSpec = field_spec(
    "dewpoint_850mb",
    "850mb Dewpoint",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(CanonicalField::Dewpoint, 850)),
    &["DPT:850 mb", "RH:850 mb", "TMP:850 mb", "SPFH:850 mb"],
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

const FIELD_500_VORT: GribFieldSpec = field_spec(
    "vorticity_500mb",
    "500mb Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(500),
    Some(FieldSelector::isobaric(CanonicalField::Vorticity, 500)),
    &["ABSV:500 mb", "VORT:500 mb"],
);

const FIELD_700_VORT: GribFieldSpec = field_spec(
    "vorticity_700mb",
    "700mb Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(700),
    Some(FieldSelector::isobaric(CanonicalField::Vorticity, 700)),
    &["ABSV:700 mb", "VORT:700 mb"],
);

const FIELD_850_VORT: GribFieldSpec = field_spec(
    "vorticity_850mb",
    "850mb Vorticity",
    ProductFamily::Pressure,
    GribLevelKind::IsobaricHpa,
    Some(850),
    Some(FieldSelector::isobaric(CanonicalField::Vorticity, 850)),
    &["ABSV:850 mb", "VORT:850 mb"],
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
    GribLevelKind::EntireAtmosphere,
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
        slug: "500mb_vorticity_height_winds",
        title: "500mb Vorticity / Height / Winds",
        filled: FIELD_500_VORT,
        contours: Some(FIELD_500_HEIGHT),
        barbs_u: Some(FIELD_500_U),
        barbs_v: Some(FIELD_500_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "700mb_vorticity_height_winds",
        title: "700mb Vorticity / Height / Winds",
        filled: FIELD_700_VORT,
        contours: Some(FIELD_700_HEIGHT),
        barbs_u: Some(FIELD_700_U),
        barbs_v: Some(FIELD_700_V),
        style: RenderStyle::Solar07Vorticity,
    },
    PlotRecipe {
        slug: "850mb_vorticity_height_winds",
        title: "850mb Vorticity / Height / Winds",
        filled: FIELD_850_VORT,
        contours: Some(FIELD_850_HEIGHT),
        barbs_u: Some(FIELD_850_U),
        barbs_v: Some(FIELD_850_V),
        style: RenderStyle::Solar07Vorticity,
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
];

pub fn built_in_models() -> &'static [ModelSummary] {
    MODELS
}

pub fn built_in_plot_recipes() -> &'static [PlotRecipe] {
    PLOT_RECIPES
}

pub fn plot_recipe(slug: &str) -> Option<&'static PlotRecipe> {
    PLOT_RECIPES
        .iter()
        .find(|recipe| normalize_token(recipe.slug) == normalize_token(slug))
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
    let summary = model_summary(model);
    let sources = summary
        .sources
        .iter()
        .filter(|candidate| source.map(|wanted| candidate.id == wanted).unwrap_or(true))
        .collect::<Vec<_>>();
    if sources.is_empty() {
        return Err(ModelError::NoAvailableRun { model });
    }

    let probe_hours = summary
        .cycle_hours_utc
        .iter()
        .copied()
        .rev()
        .collect::<Vec<_>>();
    let agent = build_agent();

    for source in sources {
        let available = probe_hours
            .par_iter()
            .find_any(|&&hour| {
                let cycle = match CycleSpec::new(date_yyyymmdd, hour) {
                    Ok(cycle) => cycle,
                    Err(_) => return false,
                };
                let request = match ModelRunRequest::new(model, cycle, 0, summary.default_product) {
                    Ok(request) => request,
                    Err(_) => return false,
                };
                let idx_url = match build_grib_url(source.id, &request) {
                    Ok(url) => format!("{url}.idx"),
                    Err(_) => return false,
                };
                head_ok(&agent, &idx_url)
            })
            .copied();

        if let Some(hour_utc) = available {
            return Ok(LatestRun {
                model,
                cycle: CycleSpec::new(date_yyyymmdd, hour_utc)?,
                source: source.id,
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

fn head_ok(agent: &ureq::Agent, url: &str) -> bool {
    match agent.head(url).call() {
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

fn plot_recipe_fetch_plan_for(
    recipe: &'static PlotRecipe,
    model: ModelId,
) -> Result<PlotRecipeFetchPlan, ModelError> {
    let fields = collect_recipe_fields(recipe);
    let blockers = plot_recipe_fetch_blockers_for_fields(&fields, model);
    if !blockers.is_empty() {
        return Err(ModelError::UnsupportedPlotRecipeModel {
            recipe: recipe.slug,
            model,
            reason: summarize_plot_recipe_blockers(&blockers),
        });
    }

    let (product, fetch_mode) = plot_recipe_fetch_defaults(model, &fields);

    Ok(PlotRecipeFetchPlan {
        recipe_slug: recipe.slug,
        model,
        product,
        fetch_mode,
        fields,
    })
}

fn plot_recipe_fetch_blockers_for(
    recipe: &'static PlotRecipe,
    model: ModelId,
) -> Vec<PlotRecipeBlocker> {
    let fields = collect_recipe_fields(recipe);
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
            Some(selector) if selector.supports_model(model) => return None,
            Some(selector) => unsupported_selector_reason(selector, model),
            None => field_selector_gap_reason(field).to_string(),
        };

        return Some(PlotRecipeBlocker {
            field_key: field.key,
            field_label: field.label,
            reason,
        });
    }

    if field.family != ProductFamily::Pressure {
        return Some(PlotRecipeBlocker {
            field_key: field.key,
            field_label: field.label,
            reason: format!(
                "{} still requires model-specific product selection and subset-fetch glue for model '{model}'",
                field.label
            ),
        });
    }

    if let Some(reason) = model_specific_pressure_field_gap(field, model) {
        return Some(PlotRecipeBlocker {
            field_key: field.key,
            field_label: field.label,
            reason,
        });
    }

    let reason = match field.selector {
        Some(selector) if selector.supports_model(model) => return None,
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
) -> (&'static str, PlotRecipeFetchMode) {
    let has_native = fields
        .iter()
        .any(|field| field.family == ProductFamily::Native);
    match (model, has_native) {
        (ModelId::Hrrr, true) => ("nat", PlotRecipeFetchMode::IndexedSubset),
        (ModelId::Hrrr, false) => ("prs", PlotRecipeFetchMode::IndexedSubset),
        (ModelId::Gfs, _) => ("pgrb2.0p25", PlotRecipeFetchMode::IndexedSubset),
        (ModelId::RrfsA, _) => ("prs-conus", PlotRecipeFetchMode::IndexedSubset),
        (ModelId::EcmwfOpenData, _) => ("oper", PlotRecipeFetchMode::WholeFileStructuredExtract),
    }
}

fn native_field_gap_reason(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (field.key, model) {
        ("composite_reflectivity" | "updraft_helicity", ModelId::Gfs | ModelId::EcmwfOpenData) => {
            Some(format!(
                "{} is not wired for model '{model}'; rustwx-models only has native convective product fetch planning for HRRR/RRFS-A right now",
                field.label
            ))
        }
        _ => None,
    }
}

fn model_specific_pressure_field_gap(field: &GribFieldSpec, model: ModelId) -> Option<String> {
    match (model, field.key) {
        (ModelId::EcmwfOpenData, "dewpoint_700mb" | "dewpoint_850mb") => Some(format!(
            "{} is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models; use RH/TMP or add derived dewpoint support for this model",
            field.label
        )),
        (ModelId::EcmwfOpenData, "vorticity_500mb" | "vorticity_700mb" | "vorticity_850mb") => {
            Some(format!(
                "{} is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models",
                field.label
            ))
        }
        _ => None,
    }
}

fn unsupported_selector_reason(selector: FieldSelector, model: ModelId) -> String {
    format!(
        "selector '{selector}' is not yet supported for model '{model}' by rustwx-core/rustwx-io"
    )
}

fn field_selector_gap_reason(field: &GribFieldSpec) -> &'static str {
    match field.key {
        "dewpoint_700mb" | "dewpoint_850mb" => {
            "dewpoint lacks a canonical FieldSelector in rustwx-core, and rustwx-io has no direct or derived isobaric dewpoint extractor yet"
        }
        "rh_500mb" | "rh_700mb" | "rh_850mb" => {
            "relative humidity lacks a canonical FieldSelector in rustwx-core, and rustwx-io has no structured isobaric RH extractor yet"
        }
        "vorticity_500mb" | "vorticity_700mb" | "vorticity_850mb" => {
            "vorticity lacks a canonical FieldSelector in rustwx-core, and rustwx-io has no structured ABSV/VORT extractor yet"
        }
        _ => "recipe field does not yet have a rustwx-core FieldSelector binding",
    }
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

fn collect_recipe_fields(recipe: &'static PlotRecipe) -> Vec<&'static GribFieldSpec> {
    let mut fields = vec![&recipe.filled];
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
    fn built_in_plot_recipes_cover_core_upper_air_and_severe_maps() {
        assert!(plot_recipe("500mb_temperature_height_winds").is_some());
        assert!(plot_recipe("700mb_temperature_height_winds").is_some());
        assert!(plot_recipe("850mb_temperature_height_winds").is_some());
        assert!(plot_recipe("700mb_dewpoint_height_winds").is_some());
        assert!(plot_recipe("850mb_dewpoint_height_winds").is_some());
        assert!(plot_recipe("500mb_vorticity_height_winds").is_some());
        assert!(plot_recipe("500mb_rh_height_winds").is_some());
        assert!(plot_recipe("700mb_rh_height_winds").is_some());
        assert!(plot_recipe("700mb_vorticity_height_winds").is_some());
        assert!(plot_recipe("composite_reflectivity").is_some());
        assert!(plot_recipe("composite_reflectivity_uh").is_some());
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
    fn selector_backed_temperature_recipe_produces_ecmwf_whole_file_fetch_plan() {
        let plan = plot_recipe_fetch_plan("500mb_temperature_height_winds", ModelId::EcmwfOpenData)
            .unwrap();
        assert_eq!(plan.product, "oper");
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
                .all(|selector| selector.supports_model(model))
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
    fn vorticity_recipe_blocker_is_explicit_for_gfs() {
        let blockers =
            plot_recipe_fetch_blockers("850mb_vorticity_height_winds", ModelId::Gfs).unwrap();
        assert!(blockers.is_empty());
    }

    #[test]
    fn vorticity_700_recipe_retains_explicit_primary_blocker() {
        let blockers =
            plot_recipe_fetch_blockers("700mb_vorticity_height_winds", ModelId::Gfs).unwrap();
        assert!(blockers.is_empty());

        let err = plot_recipe_fetch_plan("500mb_vorticity_height_winds", ModelId::EcmwfOpenData)
            .unwrap_err();
        assert!(matches!(
            err,
            ModelError::UnsupportedPlotRecipeModel {
                recipe: "500mb_vorticity_height_winds",
                model: ModelId::EcmwfOpenData,
                reason,
            } if reason == "500mb Vorticity: 500mb Vorticity is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models"
        ));

        let ecmwf_blockers =
            plot_recipe_fetch_blockers("500mb_vorticity_height_winds", ModelId::EcmwfOpenData)
                .unwrap();
        assert_eq!(
            ecmwf_blockers,
            vec![PlotRecipeBlocker {
                field_key: "vorticity_500mb",
                field_label: "500mb Vorticity",
                reason: "500mb Vorticity is not present in the ECMWF open-data 'oper' pressure product currently wired by rustwx-models".to_string(),
            }]
        );
    }

    #[test]
    fn hrrr_native_reflectivity_recipe_produces_nat_fetch_plan() {
        let plan = plot_recipe_fetch_plan("composite_reflectivity", ModelId::Hrrr).unwrap();
        assert_eq!(plan.product, "nat");
        assert_eq!(plan.fetch_mode, PlotRecipeFetchMode::IndexedSubset);
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
