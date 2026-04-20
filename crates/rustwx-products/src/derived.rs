use rustwx_calc::{
    CalcError, EcapeVolumeInputs, FixedStpInputs, GridShape as CalcGridShape, SurfaceInputs,
    TemperatureAdvectionInputs, VolumeShape, WindGridInputs, compute_2m_apparent_temperature,
    compute_ehi_01km, compute_ehi_03km, compute_lapse_rate_0_3km, compute_lapse_rate_700_500,
    compute_lifted_index, compute_mlcape_cin, compute_mucape_cin, compute_sbcape_cin,
    compute_shear_01km, compute_shear_06km, compute_srh_01km, compute_srh_03km, compute_stp_fixed,
    compute_surface_thermo,
};
use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, Field2D, ModelId, ProductKey, SourceId,
};
use rustwx_render::{
    Color, DerivedProductStyle, DomainFrame, ExtendMode, MapRenderRequest, ProductVisualMode,
    PngCompressionMode, PngWriteOptions, ProjectedDomain, ProjectedExtent, ProjectedMap,
    RenderImageTiming, RenderStateTiming,
    Solar07Palette, Solar07Product, WindBarbLayer,
    build_projected_map as build_projected_map_from_latlon, map_frame_aspect_ratio,
    save_png_profile_with_options,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use crate::gridded::{
    PressureFields as GenericPressureFields, ProjectedGridIntersection,
    SharedTiming as GenericSharedTiming, SurfaceFields as GenericSurfaceFields,
    broadcast_levels_pa, classify_projected_grid_intersection, crop_latlon_grid,
    crop_values_f64, resolve_thermo_pair_run,
};
use crate::planner::{ExecutionPlanBuilder, PlannedBundle};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
};
use crate::runtime::{BundleLoaderConfig, LoadedBundleSet, load_execution_plan};
use crate::severe::{
    build_planned_input_fetches, build_severe_execution_plan, build_shared_timing_for_pair,
};
use crate::shared_context::DomainSpec;
use crate::source::{ProductSourceMode, ProductSourceRoute};
use crate::thermo_native::{
    NativeSemantics, NativeThermoCandidate, NativeThermoRecipe, crop_native_field,
    extract_native_thermo_field, native_candidate,
};
use rustwx_models::{
    latest_available_run_at_forecast_hour, latest_available_run_for_products_at_forecast_hour,
    resolve_canonical_bundle_product,
};

const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;
const KNOTS_PER_MS: f64 = 1.943_844_5;

fn default_output_width() -> u32 {
    OUTPUT_WIDTH
}

fn default_output_height() -> u32 {
    OUTPUT_HEIGHT
}

fn default_png_compression() -> PngCompressionMode {
    PngCompressionMode::Default
}

trait SurfaceFieldSet {
    fn lat(&self) -> &[f64];
    fn lon(&self) -> &[f64];
    fn nx(&self) -> usize;
    fn ny(&self) -> usize;
    fn orog_m(&self) -> &[f64];
    fn psfc_pa(&self) -> &[f64];
    fn t2_k(&self) -> &[f64];
    fn q2_kgkg(&self) -> &[f64];
    fn u10_ms(&self) -> &[f64];
    fn v10_ms(&self) -> &[f64];
}

trait PressureFieldSet {
    fn pressure_levels_hpa(&self) -> &[f64];
    fn temperature_c_3d(&self) -> &[f64];
    fn qvapor_kgkg_3d(&self) -> &[f64];
    fn u_ms_3d(&self) -> &[f64];
    fn v_ms_3d(&self) -> &[f64];
    fn gh_m_3d(&self) -> &[f64];
}

impl SurfaceFieldSet for GenericSurfaceFields {
    fn lat(&self) -> &[f64] {
        &self.lat
    }
    fn lon(&self) -> &[f64] {
        &self.lon
    }
    fn nx(&self) -> usize {
        self.nx
    }
    fn ny(&self) -> usize {
        self.ny
    }
    fn orog_m(&self) -> &[f64] {
        &self.orog_m
    }
    fn psfc_pa(&self) -> &[f64] {
        &self.psfc_pa
    }
    fn t2_k(&self) -> &[f64] {
        &self.t2_k
    }
    fn q2_kgkg(&self) -> &[f64] {
        &self.q2_kgkg
    }
    fn u10_ms(&self) -> &[f64] {
        &self.u10_ms
    }
    fn v10_ms(&self) -> &[f64] {
        &self.v10_ms
    }
}

impl PressureFieldSet for GenericPressureFields {
    fn pressure_levels_hpa(&self) -> &[f64] {
        &self.pressure_levels_hpa
    }
    fn temperature_c_3d(&self) -> &[f64] {
        &self.temperature_c_3d
    }
    fn qvapor_kgkg_3d(&self) -> &[f64] {
        &self.qvapor_kgkg_3d
    }
    fn u_ms_3d(&self) -> &[f64] {
        &self.u_ms_3d
    }
    fn v_ms_3d(&self) -> &[f64] {
        &self.v_ms_3d
    }
    fn gh_m_3d(&self) -> &[f64] {
        &self.gh_m_3d
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedRecipeInventoryEntry {
    pub slug: &'static str,
    pub title: &'static str,
    pub experimental: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedDerivedRecipeInventoryEntry {
    pub slug: &'static str,
    pub title: &'static str,
    pub reason: &'static str,
}

const SUPPORTED_DERIVED_RECIPE_INVENTORY: &[DerivedRecipeInventoryEntry] = &[
    DerivedRecipeInventoryEntry {
        slug: "sbcape",
        title: "SBCAPE",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "sbcin",
        title: "SBCIN",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "sblcl",
        title: "SBLCL",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mlcape",
        title: "MLCAPE",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mlcin",
        title: "MLCIN",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mucape",
        title: "MUCAPE",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mucin",
        title: "MUCIN",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "theta_e_2m_10m_winds",
        title: "2 m Theta-e, 10 m Wind Barbs",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "apparent_temperature_2m",
        title: "2 m Apparent Temperature",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "heat_index_2m",
        title: "2 m Heat Index",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "wind_chill_2m",
        title: "2 m Wind Chill",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "lifted_index",
        title: "Surface-Based Lifted Index",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "lapse_rate_700_500",
        title: "700-500 mb Lapse Rate",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "lapse_rate_0_3km",
        title: "0-3 km Lapse Rate",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "bulk_shear_0_1km",
        title: "0-1 km Bulk Shear",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "bulk_shear_0_6km",
        title: "0-6 km Bulk Shear",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "srh_0_1km",
        title: "0-1 km SRH",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "srh_0_3km",
        title: "0-3 km SRH",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "ehi_0_1km",
        title: "EHI 0-1 km",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "ehi_0_3km",
        title: "EHI 0-3 km",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "stp_fixed",
        title: "STP (FIXED)",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "scp_mu_0_3km_0_6km_proxy",
        title: "SCP (MU / 0-3 km / 0-6 km PROXY)",
        experimental: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "temperature_advection_700mb",
        title: "700 mb Temperature Advection",
        experimental: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "temperature_advection_850mb",
        title: "850 mb Temperature Advection",
        experimental: false,
    },
];

const BLOCKED_DERIVED_RECIPE_INVENTORY: &[BlockedDerivedRecipeInventoryEntry] = &[
    BlockedDerivedRecipeInventoryEntry {
        slug: "stp_effective",
        title: "STP (EFFECTIVE)",
        reason: "requires mixed-layer CAPE/CIN/LCL plus effective SRH and effective bulk wind difference; rustwx-products does not yet derive effective SRH or EBWD from HRRR profiles",
    },
    BlockedDerivedRecipeInventoryEntry {
        slug: "scp",
        title: "SCP",
        reason: "requires effective SRH and effective bulk wind difference; rustwx-products does not yet derive those effective-layer kinematics from HRRR profiles",
    },
    BlockedDerivedRecipeInventoryEntry {
        slug: "scp_effective",
        title: "SCP (EFFECTIVE)",
        reason: "requires effective SRH and effective bulk wind difference; rustwx-products does not yet derive those effective-layer kinematics from HRRR profiles",
    },
];

pub fn supported_derived_recipe_inventory() -> &'static [DerivedRecipeInventoryEntry] {
    SUPPORTED_DERIVED_RECIPE_INVENTORY
}

pub fn blocked_derived_recipe_inventory() -> &'static [BlockedDerivedRecipeInventoryEntry] {
    BLOCKED_DERIVED_RECIPE_INVENTORY
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedBatchRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub recipe_slugs: Vec<String>,
    pub surface_product_override: Option<String>,
    pub pressure_product_override: Option<String>,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDerivedBatchRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub recipe_slugs: Vec<String>,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedSharedTiming {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_decode: Option<GenericSharedTiming>,
    pub compute_ms: u128,
    pub project_ms: u128,
    #[serde(default)]
    pub native_extract_ms: u128,
    #[serde(default)]
    pub native_compare_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedRecipeTiming {
    pub render_state_prep_ms: u128,
    pub png_encode_ms: u128,
    pub file_write_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
    pub state_timing: RenderStateTiming,
    pub image_timing: RenderImageTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedRenderedRecipe {
    pub recipe_slug: String,
    pub title: String,
    pub source_route: ProductSourceRoute,
    pub output_path: PathBuf,
    pub content_identity: ArtifactContentIdentity,
    pub input_fetch_keys: Vec<String>,
    pub timing: DerivedRecipeTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedRecipeBlocker {
    pub recipe_slug: String,
    pub source_route: ProductSourceRoute,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeThermoArtifactReport {
    pub recipe_slug: String,
    pub source_route: ProductSourceRoute,
    pub semantics: NativeSemantics,
    pub auto_eligible: bool,
    pub native_label: String,
    pub native_detail: String,
    pub native_fetch_product: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedBatchReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub input_fetches: Vec<PublishedFetchIdentity>,
    pub shared_timing: DerivedSharedTiming,
    pub recipes: Vec<DerivedRenderedRecipe>,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<DerivedRecipeBlocker>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_thermo_artifacts: Vec<NativeThermoArtifactReport>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDerivedBatchReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub input_fetches: Vec<PublishedFetchIdentity>,
    pub shared_timing: DerivedSharedTiming,
    pub recipes: Vec<DerivedRenderedRecipe>,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<DerivedRecipeBlocker>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_thermo_artifacts: Vec<NativeThermoArtifactReport>,
    pub total_ms: u128,
}

pub type HrrrDerivedSharedTiming = DerivedSharedTiming;
pub type HrrrDerivedRecipeTiming = DerivedRecipeTiming;
pub type HrrrDerivedRenderedRecipe = DerivedRenderedRecipe;

#[derive(Debug, Clone)]
pub struct HrrrDerivedLiveArtifact {
    pub recipe_slug: String,
    pub title: String,
    pub field: Field2D,
    pub request: MapRenderRequest,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedSharedDerivedFields {
    grid: rustwx_core::LatLonGrid,
    computed: DerivedComputedFields,
    fetch_decode: Option<GenericSharedTiming>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedNativeThermoRoute {
    pub(crate) recipe: DerivedRecipe,
    pub(crate) native_recipe: NativeThermoRecipe,
    pub(crate) candidate: NativeThermoCandidate,
    pub(crate) source_route: ProductSourceRoute,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedDerivedSourceRoutes {
    pub(crate) output_recipes: Vec<DerivedRecipe>,
    pub(crate) compute_recipes: Vec<DerivedRecipe>,
    pub(crate) native_routes: Vec<PlannedNativeThermoRoute>,
    pub(crate) blockers: Vec<DerivedRecipeBlocker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DerivedRecipe {
    Sbcape,
    Sbcin,
    Sblcl,
    Mlcape,
    Mlcin,
    Mucape,
    Mucin,
    ThetaE2m10mWinds,
    ApparentTemperature2m,
    HeatIndex2m,
    WindChill2m,
    LiftedIndex,
    LapseRate700500,
    LapseRate03km,
    BulkShear01km,
    BulkShear06km,
    Srh01km,
    Srh03km,
    Ehi01km,
    Ehi03km,
    StpFixed,
    ScpMu03km06kmProxy,
    TemperatureAdvection700mb,
    TemperatureAdvection850mb,
}

impl DerivedRecipe {
    fn parse(slug: &str) -> Result<Self, String> {
        let normalized = normalize_slug(slug);
        match normalized.as_str() {
            "sbcape" => Ok(Self::Sbcape),
            "sbcin" => Ok(Self::Sbcin),
            "sblcl" => Ok(Self::Sblcl),
            "mlcape" => Ok(Self::Mlcape),
            "mlcin" => Ok(Self::Mlcin),
            "mucape" => Ok(Self::Mucape),
            "mucin" => Ok(Self::Mucin),
            "theta_e_2m_10m_winds" | "2m_theta_e_10m_winds" => Ok(Self::ThetaE2m10mWinds),
            "apparent_temperature_2m" | "2m_apparent_temperature" => {
                Ok(Self::ApparentTemperature2m)
            }
            "heat_index_2m" | "2m_heat_index" => Ok(Self::HeatIndex2m),
            "wind_chill_2m" | "2m_wind_chill" => Ok(Self::WindChill2m),
            "lifted_index" => Ok(Self::LiftedIndex),
            "lapse_rate_700_500" => Ok(Self::LapseRate700500),
            "lapse_rate_0_3km" => Ok(Self::LapseRate03km),
            "bulk_shear_0_1km" => Ok(Self::BulkShear01km),
            "bulk_shear_0_6km" => Ok(Self::BulkShear06km),
            "srh_0_1km" => Ok(Self::Srh01km),
            "srh_0_3km" => Ok(Self::Srh03km),
            "ehi_0_1km" | "ehi_sb_0_1km_proxy" => Ok(Self::Ehi01km),
            "ehi_0_3km" | "ehi_sb_0_3km_proxy" => Ok(Self::Ehi03km),
            "stp_fixed" => Ok(Self::StpFixed),
            "scp_mu_0_3km_0_6km_proxy" => Ok(Self::ScpMu03km06kmProxy),
            "temperature_advection_700mb" => Ok(Self::TemperatureAdvection700mb),
            "temperature_advection_850mb" => Ok(Self::TemperatureAdvection850mb),
            "stp_effective" => Err(
                "stp_effective requires mixed-layer CAPE/CIN/LCL plus effective SRH and effective bulk wind difference; rustwx-products does not yet derive effective SRH or EBWD from HRRR profiles".into(),
            ),
            "scp" | "scp_effective" => Err(
                "scp/scp_effective require effective SRH and effective bulk wind difference; rustwx-products does not yet derive those effective-layer kinematics from HRRR profiles".into(),
            ),
            other => Err(format!("unsupported derived recipe '{other}'")),
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Self::Sbcape => "sbcape",
            Self::Sbcin => "sbcin",
            Self::Sblcl => "sblcl",
            Self::Mlcape => "mlcape",
            Self::Mlcin => "mlcin",
            Self::Mucape => "mucape",
            Self::Mucin => "mucin",
            Self::ThetaE2m10mWinds => "theta_e_2m_10m_winds",
            Self::ApparentTemperature2m => "apparent_temperature_2m",
            Self::HeatIndex2m => "heat_index_2m",
            Self::WindChill2m => "wind_chill_2m",
            Self::LiftedIndex => "lifted_index",
            Self::LapseRate700500 => "lapse_rate_700_500",
            Self::LapseRate03km => "lapse_rate_0_3km",
            Self::BulkShear01km => "bulk_shear_0_1km",
            Self::BulkShear06km => "bulk_shear_0_6km",
            Self::Srh01km => "srh_0_1km",
            Self::Srh03km => "srh_0_3km",
            Self::Ehi01km => "ehi_0_1km",
            Self::Ehi03km => "ehi_0_3km",
            Self::StpFixed => "stp_fixed",
            Self::ScpMu03km06kmProxy => "scp_mu_0_3km_0_6km_proxy",
            Self::TemperatureAdvection700mb => "temperature_advection_700mb",
            Self::TemperatureAdvection850mb => "temperature_advection_850mb",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Sbcape => "SBCAPE",
            Self::Sbcin => "SBCIN",
            Self::Sblcl => "SBLCL",
            Self::Mlcape => "MLCAPE",
            Self::Mlcin => "MLCIN",
            Self::Mucape => "MUCAPE",
            Self::Mucin => "MUCIN",
            Self::ThetaE2m10mWinds => "2 m Theta-e, 10 m Wind",
            Self::ApparentTemperature2m => "2 m Apparent Temperature",
            Self::HeatIndex2m => "2 m Heat Index",
            Self::WindChill2m => "2 m Wind Chill",
            Self::LiftedIndex => "Surface-Based Lifted Index",
            Self::LapseRate700500 => "700-500 mb Lapse Rate",
            Self::LapseRate03km => "0-3 km Lapse Rate",
            Self::BulkShear01km => "0-1 km Bulk Shear",
            Self::BulkShear06km => "0-6 km Bulk Shear",
            Self::Srh01km => "0-1 km SRH",
            Self::Srh03km => "0-3 km SRH",
            Self::Ehi01km => "EHI 0-1 km",
            Self::Ehi03km => "EHI 0-3 km",
            Self::StpFixed => "STP (FIXED)",
            Self::ScpMu03km06kmProxy => "SCP (MU / 0-3 km / 0-6 km PROXY)",
            Self::TemperatureAdvection700mb => "700 mb Temperature Advection",
            Self::TemperatureAdvection850mb => "850 mb Temperature Advection",
        }
    }

    fn visual_mode(self) -> ProductVisualMode {
        match self {
            Self::ThetaE2m10mWinds
            | Self::TemperatureAdvection700mb
            | Self::TemperatureAdvection850mb => ProductVisualMode::UpperAirAnalysis,
            Self::ApparentTemperature2m | Self::HeatIndex2m | Self::WindChill2m => {
                ProductVisualMode::FilledMeteorology
            }
            _ => ProductVisualMode::SevereDiagnostic,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct DerivedComputedFields {
    sbcape_jkg: Option<Vec<f64>>,
    sbcin_jkg: Option<Vec<f64>>,
    sblcl_m: Option<Vec<f64>>,
    mlcape_jkg: Option<Vec<f64>>,
    mlcin_jkg: Option<Vec<f64>>,
    mucape_jkg: Option<Vec<f64>>,
    mucin_jkg: Option<Vec<f64>>,
    theta_e_2m_k: Option<Vec<f64>>,
    apparent_temperature_2m_c: Option<Vec<f64>>,
    heat_index_2m_c: Option<Vec<f64>>,
    wind_chill_2m_c: Option<Vec<f64>>,
    surface_u10_ms: Option<Vec<f64>>,
    surface_v10_ms: Option<Vec<f64>>,
    lifted_index_c: Option<Vec<f64>>,
    lapse_rate_700_500_cpkm: Option<Vec<f64>>,
    lapse_rate_0_3km_cpkm: Option<Vec<f64>>,
    shear_01km_kt: Option<Vec<f64>>,
    shear_06km_kt: Option<Vec<f64>>,
    srh_01km_m2s2: Option<Vec<f64>>,
    srh_03km_m2s2: Option<Vec<f64>>,
    ehi_01km: Option<Vec<f64>>,
    ehi_03km: Option<Vec<f64>>,
    stp_fixed: Option<Vec<f64>>,
    scp_mu_03km_06km_proxy: Option<Vec<f64>>,
    temperature_advection_700mb_cph: Option<Vec<f64>>,
    temperature_advection_850mb_cph: Option<Vec<f64>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct DerivedRequirements {
    sb: bool,
    ml: bool,
    mu: bool,
    surface_thermo: bool,
    surface_winds: bool,
    lifted_index: bool,
    lapse_rate_700_500: bool,
    lapse_rate_0_3km: bool,
    shear_01km: bool,
    shear_06km: bool,
    srh_01km: bool,
    srh_03km: bool,
    ehi_01km: bool,
    ehi_03km: bool,
    stp_fixed: bool,
    scp_mu_03km_06km_proxy: bool,
    temperature_advection_700mb: bool,
    temperature_advection_850mb: bool,
}

impl DerivedRequirements {
    fn from_recipes(recipes: &[DerivedRecipe]) -> Self {
        let mut requirements = Self::default();
        for &recipe in recipes {
            match recipe {
                DerivedRecipe::Sbcape | DerivedRecipe::Sbcin | DerivedRecipe::Sblcl => {
                    requirements.sb = true;
                }
                DerivedRecipe::Mlcape | DerivedRecipe::Mlcin => {
                    requirements.ml = true;
                }
                DerivedRecipe::Mucape | DerivedRecipe::Mucin => {
                    requirements.mu = true;
                }
                DerivedRecipe::ThetaE2m10mWinds => {
                    requirements.surface_thermo = true;
                    requirements.surface_winds = true;
                }
                DerivedRecipe::ApparentTemperature2m
                | DerivedRecipe::HeatIndex2m
                | DerivedRecipe::WindChill2m => {
                    requirements.surface_thermo = true;
                }
                DerivedRecipe::LiftedIndex => {
                    requirements.lifted_index = true;
                }
                DerivedRecipe::LapseRate700500 => {
                    requirements.lapse_rate_700_500 = true;
                }
                DerivedRecipe::LapseRate03km => {
                    requirements.lapse_rate_0_3km = true;
                }
                DerivedRecipe::BulkShear01km => {
                    requirements.shear_01km = true;
                }
                DerivedRecipe::BulkShear06km => {
                    requirements.shear_06km = true;
                }
                DerivedRecipe::Srh01km => {
                    requirements.srh_01km = true;
                }
                DerivedRecipe::Srh03km => {
                    requirements.srh_03km = true;
                }
                DerivedRecipe::Ehi01km => {
                    requirements.ehi_01km = true;
                    requirements.sb = true;
                    requirements.srh_01km = true;
                }
                DerivedRecipe::Ehi03km => {
                    requirements.ehi_03km = true;
                    requirements.sb = true;
                    requirements.srh_03km = true;
                }
                DerivedRecipe::StpFixed => {
                    requirements.stp_fixed = true;
                    requirements.sb = true;
                    requirements.srh_01km = true;
                    requirements.shear_06km = true;
                }
                DerivedRecipe::ScpMu03km06kmProxy => {
                    requirements.scp_mu_03km_06km_proxy = true;
                    requirements.mu = true;
                    requirements.srh_03km = true;
                    requirements.shear_06km = true;
                }
                DerivedRecipe::TemperatureAdvection700mb => {
                    requirements.temperature_advection_700mb = true;
                }
                DerivedRecipe::TemperatureAdvection850mb => {
                    requirements.temperature_advection_850mb = true;
                }
            }
        }
        requirements
    }

    fn needs_volume(self) -> bool {
        self.sb
            || self.ml
            || self.mu
            || self.lifted_index
            || self.lapse_rate_700_500
            || self.lapse_rate_0_3km
    }

    fn needs_height_agl(self) -> bool {
        self.needs_volume() || self.shear_01km || self.shear_06km || self.srh_01km || self.srh_03km
    }

    fn needs_grid_spacing(self) -> bool {
        self.temperature_advection_700mb || self.temperature_advection_850mb
    }
}

impl DerivedBatchRequest {
    pub(crate) fn from_hrrr(request: &HrrrDerivedBatchRequest) -> Self {
        Self {
            model: ModelId::Hrrr,
            date_yyyymmdd: request.date_yyyymmdd.clone(),
            cycle_override_utc: request.cycle_override_utc,
            forecast_hour: request.forecast_hour,
            source: request.source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: request.recipe_slugs.clone(),
            surface_product_override: None,
            pressure_product_override: None,
            source_mode: request.source_mode,
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
        }
    }

    fn png_write_options(&self) -> PngWriteOptions {
        PngWriteOptions {
            compression: self.png_compression,
        }
    }
}

pub fn supported_derived_recipe_slugs(model: ModelId) -> Vec<String> {
    match model {
        ModelId::Hrrr | ModelId::Gfs | ModelId::EcmwfOpenData | ModelId::RrfsA => {
            supported_derived_recipe_inventory()
                .iter()
                .map(|recipe| recipe.slug.to_string())
                .collect()
        }
    }
}

pub fn run_derived_batch(
    request: &DerivedBatchRequest,
) -> Result<DerivedBatchReport, Box<dyn std::error::Error>> {
    let recipes = plan_derived_recipes(&request.recipe_slugs)?;
    let planned_routes = plan_native_thermo_routes(request.model, &recipes, request.source_mode)?;
    let latest = resolve_derived_run(
        request,
        &planned_routes.compute_recipes,
        &planned_routes.native_routes,
    )?;
    if planned_routes.output_recipes.is_empty() {
        return Ok(empty_derived_report(
            request,
            &latest,
            planned_routes.blockers,
        ));
    }
    let plan = build_derived_execution_plan(
        &latest,
        request.forecast_hour,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
        !planned_routes.compute_recipes.is_empty(),
        &planned_routes.native_routes,
    );
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig {
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
        },
    )?;
    run_derived_batch_from_loaded_bundles(request, &recipes, &loaded)
}

pub fn run_hrrr_derived_batch(
    request: &HrrrDerivedBatchRequest,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    Ok(into_hrrr_report(run_derived_batch(
        &DerivedBatchRequest::from_hrrr(request),
    )?))
}

fn run_derived_batch_from_loaded_bundles(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
) -> Result<DerivedBatchReport, Box<dyn std::error::Error>> {
    run_derived_batch_from_loaded_bundles_with_precomputed(request, recipes, loaded, None)
}

fn run_derived_batch_from_loaded_bundles_with_precomputed(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
    shared_precomputed: Option<&PreparedSharedDerivedFields>,
) -> Result<DerivedBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }
    let total_start = Instant::now();
    let planned_routes = plan_native_thermo_routes(request.model, recipes, request.source_mode)?;
    if planned_routes.output_recipes.is_empty() {
        return Ok(empty_derived_report(
            request,
            &loaded.latest,
            planned_routes.blockers,
        ));
    }
    let mut computed = DerivedComputedFields::default();
    let mut fetch_decode = None;
    let mut compute_ms = 0u128;
    let mut project_ms = 0u128;
    let mut native_extract_ms = 0u128;
    let native_compare_ms = 0u128;
    let mut grid: Option<rustwx_core::LatLonGrid> = None;
    let mut projected: Option<ProjectedMap> = None;

    if !planned_routes.compute_recipes.is_empty() {
        let (surface_planned, surface_decode, pressure_planned, pressure_decode) = loaded
            .require_surface_pressure_pair()
            .map_err(|err| format!("derived surface/pressure pair unavailable: {err}"))?;
        let full_surface = &surface_decode.value;
        let full_pressure = &pressure_decode.value;
        let owned_full_grid = full_surface.core_grid()?;
        let project_start = Instant::now();
        let full_projected = build_projected_map_from_latlon(
            &owned_full_grid.lat_deg,
            &owned_full_grid.lon_deg,
            request.domain.bounds,
            map_frame_aspect_ratio(request.output_width, request.output_height, true, true),
        )?;
        match shared_precomputed {
            Some(shared) => {
                match classify_projected_grid_intersection(
                    shared.grid.shape.nx,
                    shared.grid.shape.ny,
                    &full_projected.projected_x,
                    &full_projected.projected_y,
                    &full_projected.extent,
                    2,
                )? {
                    ProjectedGridIntersection::Empty => {
                        return Err(format!(
                            "derived projected crop for domain '{}' produced an empty domain",
                            request.domain.slug
                        )
                        .into());
                    }
                    ProjectedGridIntersection::Full => {
                        grid = Some(shared.grid.clone());
                        projected = Some(full_projected);
                        computed = shared.computed.clone();
                    }
                    ProjectedGridIntersection::Crop(crop) => {
                        let derived_grid = crop_latlon_grid(&shared.grid, crop)?;
                        let derived_projected = build_projected_map_from_latlon(
                            &derived_grid.lat_deg,
                            &derived_grid.lon_deg,
                            request.domain.bounds,
                            map_frame_aspect_ratio(
                                request.output_width,
                                request.output_height,
                                true,
                                true,
                            ),
                        )?;
                        grid = Some(derived_grid);
                        projected = Some(derived_projected);
                        computed = crop_computed_fields(
                            &shared.computed,
                            shared.grid.shape.nx,
                            crop,
                        );
                    }
                }
                fetch_decode = shared.fetch_decode.clone();
            }
            None => {
                let cropped = crate::gridded::crop_heavy_domain_for_projected_extent(
                    full_surface,
                    full_pressure,
                    &full_projected.projected_x,
                    &full_projected.projected_y,
                    &full_projected.extent,
                    2,
                )?;
                let (surface, pressure, derived_grid) = match cropped.as_ref() {
                    Some(cropped) => (&cropped.surface, &cropped.pressure, cropped.grid.clone()),
                    None => (full_surface, full_pressure, owned_full_grid.clone()),
                };

                let derived_projected = if cropped.is_some() {
                    build_projected_map_from_latlon(
                        &derived_grid.lat_deg,
                        &derived_grid.lon_deg,
                        request.domain.bounds,
                        map_frame_aspect_ratio(
                            request.output_width,
                            request.output_height,
                            true,
                            true,
                        ),
                    )?
                } else {
                    full_projected
                };

                let compute_start = Instant::now();
                computed = compute_derived_fields_generic(
                    surface,
                    pressure,
                    &planned_routes.compute_recipes,
                )?;
                compute_ms += compute_start.elapsed().as_millis();
                fetch_decode = Some(build_shared_timing_for_pair(
                    loaded,
                    surface_planned,
                    pressure_planned,
                )?);
                grid = Some(derived_grid);
                projected = Some(derived_projected);
            }
        }
        project_ms += project_start.elapsed().as_millis();
    }

    let input_fetches = build_planned_input_fetches(loaded);
    let input_fetch_keys = unique_input_fetch_keys(&input_fetches);
    let date_yyyymmdd = request.date_yyyymmdd.as_str();
    let cycle_utc = loaded.latest.cycle.hour_utc;
    let forecast_hour = request.forecast_hour;
    let source = loaded.latest.source;
    let model = request.model;
    let computed = &computed;
    let mut rendered_by_recipe = HashMap::<DerivedRecipe, DerivedRenderedRecipe>::new();
    let mut native_thermo_artifacts = Vec::<NativeThermoArtifactReport>::new();

    for route in &planned_routes.native_routes {
        let native_planned = find_loaded_native_bundle(loaded, route.candidate.fetch_product)
            .ok_or_else(|| {
                format!(
                    "native thermo planner missed fetch for '{}' ({})",
                    route.recipe.slug(),
                    route.candidate.fetch_product
                )
            })?;
        let fetched = loaded
            .fetched_for(native_planned)
            .ok_or_else(|| format!("native thermo fetch missing for {}", route.recipe.slug()))?;
        let extract_start = Instant::now();
        let native_field =
            extract_native_thermo_field(request.model, route.native_recipe, &fetched.file.bytes)?
                .ok_or_else(|| {
                format!(
                    "native thermo field '{}' not found in {}",
                    route.recipe.slug(),
                    route.candidate.fetch_product
                )
            })?;
        let native_field = crop_native_field(&native_field, request.domain.bounds)?;
        native_extract_ms += extract_start.elapsed().as_millis();

        if grid.is_none() {
            let project_start = Instant::now();
            let native_projected = build_projected_map_from_latlon(
                &native_field.grid.lat_deg,
                &native_field.grid.lon_deg,
                request.domain.bounds,
                map_frame_aspect_ratio(request.output_width, request.output_height, true, true),
            )?;
            project_ms += project_start.elapsed().as_millis();
            grid = Some(native_field.grid.clone());
            projected = Some(native_projected);
        }
        let output_path = request.out_dir.join(format!(
            "rustwx_{}_{}_{}z_f{:03}_{}_{}.png",
            model.as_str().replace('-', "_"),
            request.date_yyyymmdd,
            cycle_utc,
            request.forecast_hour,
            request.domain.slug,
            route.recipe.slug()
        ));
        let render_start = Instant::now();
        let render_artifact = build_native_render_artifact(
            route.recipe,
            &native_field.grid,
            projected
                .as_ref()
                .ok_or("native thermo projection missing during main render")?,
            date_yyyymmdd,
            cycle_utc,
            forecast_hour,
            source,
            model,
            request.output_width,
            request.output_height,
            native_field.values.clone(),
        )?;
        let save_timing = save_png_profile_with_options(
            &render_artifact.request,
            &output_path,
            &request.png_write_options(),
        )?;
        let render_ms = render_start.elapsed().as_millis();
        let content_identity = artifact_identity_from_path(&output_path)?;
        rendered_by_recipe.insert(
            route.recipe,
            DerivedRenderedRecipe {
                recipe_slug: render_artifact.recipe_slug,
                title: render_artifact.title,
                source_route: route.source_route,
                output_path,
                content_identity,
                input_fetch_keys: input_fetch_keys.clone(),
                timing: DerivedRecipeTiming {
                    render_state_prep_ms: save_timing.state_timing.state_prep_ms,
                    png_encode_ms: save_timing.png_timing.png_encode_ms,
                    file_write_ms: save_timing.file_write_ms,
                    render_ms,
                    total_ms: render_ms,
                    state_timing: save_timing.state_timing,
                    image_timing: save_timing.png_timing.image_timing,
                },
            },
        );

        native_thermo_artifacts.push(NativeThermoArtifactReport {
            recipe_slug: route.recipe.slug().to_string(),
            source_route: route.source_route,
            semantics: route.candidate.semantics,
            auto_eligible: route.candidate.auto_eligible,
            native_label: route.candidate.label.to_string(),
            native_detail: route.candidate.detail.to_string(),
            native_fetch_product: route.candidate.fetch_product.to_string(),
        });
    }

    let derived_output_recipes = planned_routes
        .output_recipes
        .iter()
        .copied()
        .filter(|recipe| !rendered_by_recipe.contains_key(recipe))
        .collect::<Vec<_>>();
    if !derived_output_recipes.is_empty() {
        let render_parallelism = png_render_parallelism(derived_output_recipes.len());
        let grid_ref = grid
            .as_ref()
            .ok_or("derived render requested but no grid was prepared")?;
        let projected_ref = projected
            .as_ref()
            .ok_or("derived render requested but no projection was prepared")?;
        let rendered = thread::scope(|scope| -> Result<Vec<DerivedRenderedRecipe>, io::Error> {
            let mut rendered = Vec::with_capacity(derived_output_recipes.len());
            let mut pending = VecDeque::new();

            for recipe in derived_output_recipes.iter().copied() {
                let model_slug = request.model.as_str().replace('-', "_");
                let output_path = request.out_dir.join(format!(
                    "rustwx_{}_{}_{}z_f{:03}_{}_{}.png",
                    model_slug,
                    request.date_yyyymmdd,
                    cycle_utc,
                    request.forecast_hour,
                    request.domain.slug,
                    recipe.slug()
                ));
                let lane_fetch_keys = input_fetch_keys.clone();
                pending.push_back(scope.spawn(
                    move || -> Result<DerivedRenderedRecipe, io::Error> {
                        let render_start = Instant::now();
                        let render_artifact = build_render_artifact(
                            recipe,
                            grid_ref,
                            projected_ref,
                            date_yyyymmdd,
                            cycle_utc,
                            forecast_hour,
                            source,
                            model,
                            request.output_width,
                            request.output_height,
                            computed,
                        )
                        .map_err(thread_render_error)?;
                        let save_timing = save_png_profile_with_options(
                            &render_artifact.request,
                            &output_path,
                            &request.png_write_options(),
                        )
                        .map_err(thread_render_error)?;
                        let render_ms = render_start.elapsed().as_millis();
                        let content_identity = artifact_identity_from_path(&output_path)
                            .map_err(thread_render_error)?;
                        Ok(DerivedRenderedRecipe {
                            recipe_slug: render_artifact.recipe_slug,
                            title: render_artifact.title,
                            source_route: derived_compute_source_route(recipe, request.source_mode)
                                .ok_or_else(|| {
                                    io::Error::other(format!(
                                        "missing compute source route for '{}'",
                                        recipe.slug()
                                    ))
                                })?,
                            output_path,
                            content_identity,
                            input_fetch_keys: lane_fetch_keys,
                            timing: DerivedRecipeTiming {
                                render_state_prep_ms: save_timing.state_timing.state_prep_ms,
                                png_encode_ms: save_timing.png_timing.png_encode_ms,
                                file_write_ms: save_timing.file_write_ms,
                                render_ms,
                                total_ms: render_ms,
                                state_timing: save_timing.state_timing,
                                image_timing: save_timing.png_timing.image_timing,
                            },
                        })
                    },
                ));

                if pending.len() >= render_parallelism {
                    rendered.push(join_render_job(pending.pop_front().unwrap())?);
                }
            }

            while let Some(handle) = pending.pop_front() {
                rendered.push(join_render_job(handle)?);
            }

            Ok(rendered)
        })
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
        for recipe in rendered {
            let parsed = DerivedRecipe::parse(&recipe.recipe_slug).map_err(io::Error::other)?;
            rendered_by_recipe.insert(parsed, recipe);
        }
    }

    let rendered = planned_routes
        .output_recipes
        .iter()
        .map(|recipe| {
            rendered_by_recipe
                .remove(recipe)
                .ok_or_else(|| format!("derived renderer missed recipe '{}'", recipe.slug()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DerivedBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc,
        forecast_hour: request.forecast_hour,
        source: loaded.latest.source,
        domain: request.domain.clone(),
        input_fetches,
        shared_timing: DerivedSharedTiming {
            fetch_decode,
            compute_ms,
            project_ms,
            native_extract_ms,
            native_compare_ms,
        },
        recipes: rendered,
        source_mode: request.source_mode,
        blockers: planned_routes.blockers,
        native_thermo_artifacts,
        total_ms: total_start.elapsed().as_millis(),
    })
}

/// Run the HRRR derived lane consuming a planner-loaded bundle set.
/// Used by the unified `hrrr_non_ecape_hour` runner so direct + derived
/// + windowed all share one fetch+decode pass.
pub(crate) fn run_hrrr_derived_batch_from_loaded(
    request: &HrrrDerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let generic_request = DerivedBatchRequest::from_hrrr(request);
    let report = run_derived_batch_from_loaded_bundles(&generic_request, recipes, loaded)?;
    Ok(into_hrrr_report(report))
}

pub(crate) fn prepare_shared_derived_fields(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
) -> Result<Option<PreparedSharedDerivedFields>, Box<dyn std::error::Error>> {
    let planned_routes = plan_native_thermo_routes(request.model, recipes, request.source_mode)?;
    if planned_routes.compute_recipes.is_empty() {
        return Ok(None);
    }

    let (surface_planned, surface_decode, pressure_planned, pressure_decode) = loaded
        .require_surface_pressure_pair()
        .map_err(|err| format!("derived surface/pressure pair unavailable: {err}"))?;
    let computed = compute_derived_fields_generic(
        &surface_decode.value,
        &pressure_decode.value,
        &planned_routes.compute_recipes,
    )?;
    let fetch_decode = build_shared_timing_for_pair(loaded, surface_planned, pressure_planned)?;
    Ok(Some(PreparedSharedDerivedFields {
        grid: surface_decode.value.core_grid()?,
        computed,
        fetch_decode: Some(GenericSharedTiming {
            fetch_surface_ms: 0,
            fetch_pressure_ms: 0,
            decode_surface_ms: 0,
            decode_pressure_ms: 0,
            fetch_surface_cache_hit: fetch_decode.fetch_surface_cache_hit,
            fetch_pressure_cache_hit: fetch_decode.fetch_pressure_cache_hit,
            decode_surface_cache_hit: fetch_decode.decode_surface_cache_hit,
            decode_pressure_cache_hit: fetch_decode.decode_pressure_cache_hit,
            surface_fetch: fetch_decode.surface_fetch,
            pressure_fetch: fetch_decode.pressure_fetch,
        }),
    }))
}

pub(crate) fn run_hrrr_derived_batch_from_loaded_with_precomputed(
    request: &HrrrDerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
    prepared: &PreparedSharedDerivedFields,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let generic_request = DerivedBatchRequest::from_hrrr(request);
    let mut report = run_derived_batch_from_loaded_bundles_with_precomputed(
        &generic_request,
        recipes,
        loaded,
        Some(prepared),
    )?;
    report.shared_timing.compute_ms = 0;
    Ok(into_hrrr_report(report))
}

pub(crate) fn run_hrrr_derived_batch_without_loaded(
    request: &HrrrDerivedBatchRequest,
    recipes: &[DerivedRecipe],
    latest: &rustwx_models::LatestRun,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let generic_request = DerivedBatchRequest::from_hrrr(request);
    let planned_routes = plan_native_thermo_routes(ModelId::Hrrr, recipes, request.source_mode)?;
    let report = empty_derived_report(&generic_request, latest, planned_routes.blockers);
    Ok(into_hrrr_report(report))
}

fn into_hrrr_report(report: DerivedBatchReport) -> HrrrDerivedBatchReport {
    HrrrDerivedBatchReport {
        date_yyyymmdd: report.date_yyyymmdd,
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source,
        domain: report.domain,
        input_fetches: report.input_fetches,
        shared_timing: report.shared_timing,
        recipes: report.recipes,
        source_mode: report.source_mode,
        blockers: report.blockers,
        native_thermo_artifacts: report.native_thermo_artifacts,
        total_ms: report.total_ms,
    }
}

fn derived_compute_source_route(
    recipe: DerivedRecipe,
    mode: ProductSourceMode,
) -> Option<ProductSourceRoute> {
    match mode {
        ProductSourceMode::Canonical => Some(ProductSourceRoute::CanonicalDerived),
        ProductSourceMode::Fastest => cheap_fastest_route(recipe),
    }
}

fn empty_derived_report(
    request: &DerivedBatchRequest,
    latest: &rustwx_models::LatestRun,
    blockers: Vec<DerivedRecipeBlocker>,
) -> DerivedBatchReport {
    DerivedBatchReport {
        model: request.model,
        date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
        cycle_utc: latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: latest.source,
        domain: request.domain.clone(),
        input_fetches: Vec::new(),
        shared_timing: DerivedSharedTiming {
            fetch_decode: None,
            compute_ms: 0,
            project_ms: 0,
            native_extract_ms: 0,
            native_compare_ms: 0,
        },
        recipes: Vec::new(),
        source_mode: request.source_mode,
        blockers,
        native_thermo_artifacts: Vec::new(),
        total_ms: 0,
    }
}

fn unique_input_fetch_keys(fetches: &[PublishedFetchIdentity]) -> Vec<String> {
    let mut keys = Vec::with_capacity(fetches.len());
    for fetch in fetches {
        if !keys.contains(&fetch.fetch_key) {
            keys.push(fetch.fetch_key.clone());
        }
    }
    keys
}

fn find_loaded_native_bundle<'a>(
    loaded: &'a LoadedBundleSet,
    fetch_product: &str,
) -> Option<&'a PlannedBundle> {
    loaded.plan.bundles.iter().find(|bundle| {
        bundle.id.bundle == CanonicalBundleDescriptor::NativeAnalysis
            && bundle.fetch_key().native_product == fetch_product
    })
}

fn build_native_render_artifact(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    model: ModelId,
    output_width: u32,
    output_height: u32,
    values: Vec<f64>,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let (field, mut request) = match recipe {
        DerivedRecipe::Sbcape => {
            solar07_request(recipe, grid, "J/kg", values, Solar07Product::Sbcape)?
        }
        DerivedRecipe::Sbcin => {
            solar07_request(recipe, grid, "J/kg", values, Solar07Product::Sbcin)?
        }
        DerivedRecipe::Sblcl => solar07_request(recipe, grid, "m", values, Solar07Product::Lcl)?,
        DerivedRecipe::Mlcape => {
            solar07_request(recipe, grid, "J/kg", values, Solar07Product::Mlcape)?
        }
        DerivedRecipe::Mlcin => {
            solar07_request(recipe, grid, "J/kg", values, Solar07Product::Mlcin)?
        }
        DerivedRecipe::Mucape => {
            solar07_request(recipe, grid, "J/kg", values, Solar07Product::Mucape)?
        }
        DerivedRecipe::Mucin => {
            solar07_request(recipe, grid, "J/kg", values, Solar07Product::Mucin)?
        }
        DerivedRecipe::LiftedIndex => palette_request(
            recipe,
            grid,
            "degC",
            values,
            Solar07Palette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        _ => {
            return Err(format!(
                "recipe '{}' does not support native thermo rendering",
                recipe.slug()
            )
            .into());
        }
    };

    request.width = output_width;
    request.height = output_height;
    request.supersample_factor = 2;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.visual_mode = recipe.visual_mode();
    request.title = Some(recipe.title().to_string());
    request.subtitle_left = Some(format!(
        "{} {}Z F{:03}  {}",
        date_yyyymmdd, cycle_utc, forecast_hour, model
    ));
    request.subtitle_right = Some(format!("source: {}", source));
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x.clone(),
        y: projected.projected_y.clone(),
        extent: projected.extent.clone(),
    });
    request.projected_lines = projected.lines.clone();
    request.projected_polygons = projected.polygons.clone();
    Ok(HrrrDerivedLiveArtifact {
        recipe_slug: recipe.slug().to_string(),
        title: recipe.title().to_string(),
        field,
        request,
    })
}

/// Build a single derived render artifact for an HRRR live-preview
/// surface. Takes the planner-decoded generic surface/pressure types so
/// callers can reuse a `LoadedBundleSet` rather than re-decoding HRRR
/// natively. Reroutes through the same generic compute kernel as the
/// batched derived lane.
pub fn build_hrrr_live_derived_artifact(
    recipe_slug: &str,
    surface: &GenericSurfaceFields,
    pressure: &GenericPressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let recipe =
        DerivedRecipe::parse(recipe_slug).map_err(|err| format!("{recipe_slug}: {err}"))?;
    let computed = compute_derived_fields_generic(surface, pressure, &[recipe])?;
    build_render_artifact(
        recipe,
        grid,
        projected,
        date_yyyymmdd,
        cycle_utc,
        forecast_hour,
        source,
        ModelId::Hrrr,
        OUTPUT_WIDTH,
        OUTPUT_HEIGHT,
        &computed,
    )
}

pub(crate) fn plan_derived_recipes(
    recipe_slugs: &[String],
) -> Result<Vec<DerivedRecipe>, Box<dyn std::error::Error>> {
    let mut seen = HashSet::<DerivedRecipe>::new();
    let mut planned = Vec::new();
    for slug in recipe_slugs {
        let recipe = DerivedRecipe::parse(slug).map_err(|err| format!("{slug}: {err}"))?;
        if seen.insert(recipe) {
            planned.push(recipe);
        }
    }
    Ok(planned)
}

fn native_recipe_for_derived(recipe: DerivedRecipe) -> Option<NativeThermoRecipe> {
    match recipe {
        DerivedRecipe::Sbcape => Some(NativeThermoRecipe::Sbcape),
        DerivedRecipe::Sbcin => Some(NativeThermoRecipe::Sbcin),
        DerivedRecipe::Sblcl => Some(NativeThermoRecipe::Sblcl),
        DerivedRecipe::Mlcape => Some(NativeThermoRecipe::Mlcape),
        DerivedRecipe::Mlcin => Some(NativeThermoRecipe::Mlcin),
        DerivedRecipe::Mucape => Some(NativeThermoRecipe::Mucape),
        DerivedRecipe::Mucin => Some(NativeThermoRecipe::Mucin),
        DerivedRecipe::LiftedIndex => Some(NativeThermoRecipe::LiftedIndex),
        _ => None,
    }
}

pub(crate) fn plan_native_thermo_routes(
    model: ModelId,
    recipes: &[DerivedRecipe],
    mode: ProductSourceMode,
) -> Result<PlannedDerivedSourceRoutes, Box<dyn std::error::Error>> {
    let mut output_recipes = Vec::new();
    let mut compute_recipes = Vec::new();
    let mut native_routes = Vec::new();
    let mut blockers = Vec::new();

    for &recipe in recipes {
        let candidate = native_recipe_for_derived(recipe).and_then(|native_recipe| {
            native_candidate(model, native_recipe).map(|candidate| (native_recipe, candidate))
        });

        match mode {
            ProductSourceMode::Canonical => {
                output_recipes.push(recipe);
                compute_recipes.push(recipe);
            }
            ProductSourceMode::Fastest => {
                if let Some((native_recipe, candidate)) = candidate {
                    output_recipes.push(recipe);
                    native_routes.push(PlannedNativeThermoRoute {
                        recipe,
                        native_recipe,
                        source_route: native_source_route(candidate.semantics),
                        candidate,
                    });
                } else if let Some(source_route) = cheap_fastest_route(recipe) {
                    output_recipes.push(recipe);
                    if matches!(source_route, ProductSourceRoute::CheapDerived) {
                        compute_recipes.push(recipe);
                    }
                } else {
                    blockers.push(DerivedRecipeBlocker {
                        recipe_slug: recipe.slug().to_string(),
                        source_route: ProductSourceRoute::BlockedNoFastRoute,
                        reason: format!(
                            "recipe '{}' has no fast native/cheap route; fastest mode will not fall back to canonical-derived compute",
                            recipe.slug()
                        ),
                    });
                }
            }
        }
    }

    Ok(PlannedDerivedSourceRoutes {
        output_recipes,
        compute_recipes,
        native_routes,
        blockers,
    })
}

fn native_source_route(semantics: NativeSemantics) -> ProductSourceRoute {
    match semantics {
        NativeSemantics::ExactEquivalent => ProductSourceRoute::NativeExact,
        NativeSemantics::ProxyEquivalent => ProductSourceRoute::NativeProxy,
    }
}

fn cheap_fastest_route(_recipe: DerivedRecipe) -> Option<ProductSourceRoute> {
    // The current derived kernel still routes every non-native recipe
    // through the canonical surface+pressure pair compute path. Until a
    // recipe can be satisfied from already-loaded native/direct inputs
    // without forcing that pair, fastest mode blocks it explicitly.
    None
}

fn resolve_derived_run(
    request: &DerivedBatchRequest,
    derived_compute_recipes: &[DerivedRecipe],
    native_routes: &[PlannedNativeThermoRoute],
) -> Result<rustwx_models::LatestRun, Box<dyn std::error::Error>> {
    let needs_pair = !derived_compute_recipes.is_empty();
    if let Some(hour_utc) = request.cycle_override_utc {
        if !needs_pair {
            return Ok(rustwx_models::LatestRun {
                model: request.model,
                cycle: rustwx_core::CycleSpec::new(request.date_yyyymmdd.clone(), hour_utc)?,
                source: request.source,
            });
        }
        return resolve_thermo_pair_run(
            request.model,
            &request.date_yyyymmdd,
            Some(hour_utc),
            request.forecast_hour,
            request.source,
            request.surface_product_override.as_deref(),
            request.pressure_product_override.as_deref(),
        )
        .map_err(Into::into);
    }

    if !needs_pair && native_routes.is_empty() {
        return latest_available_run_at_forecast_hour(
            request.model,
            Some(request.source),
            &request.date_yyyymmdd,
            request.forecast_hour,
        )
        .map_err(Into::into);
    }

    let mut required_products = BTreeSet::<String>::new();
    if needs_pair {
        required_products.insert(
            resolve_canonical_bundle_product(
                request.model,
                CanonicalBundleDescriptor::SurfaceAnalysis,
                request.surface_product_override.as_deref(),
            )
            .native_product,
        );
        required_products.insert(
            resolve_canonical_bundle_product(
                request.model,
                CanonicalBundleDescriptor::PressureAnalysis,
                request.pressure_product_override.as_deref(),
            )
            .native_product,
        );
    }
    for route in native_routes {
        required_products.insert(route.candidate.fetch_product.to_string());
    }
    if required_products.is_empty() {
        return resolve_thermo_pair_run(
            request.model,
            &request.date_yyyymmdd,
            request.cycle_override_utc,
            request.forecast_hour,
            request.source,
            request.surface_product_override.as_deref(),
            request.pressure_product_override.as_deref(),
        )
        .map_err(Into::into);
    }

    let required_refs = required_products
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    latest_available_run_for_products_at_forecast_hour(
        request.model,
        Some(request.source),
        &request.date_yyyymmdd,
        &required_refs,
        request.forecast_hour,
    )
    .map_err(Into::into)
}

fn build_derived_execution_plan(
    latest: &rustwx_models::LatestRun,
    forecast_hour: u16,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
    include_pair: bool,
    native_routes: &[PlannedNativeThermoRoute],
) -> crate::planner::ExecutionPlan {
    let mut builder = ExecutionPlanBuilder::new(latest, forecast_hour);
    if include_pair {
        let pair_plan = build_severe_execution_plan(
            latest,
            forecast_hour,
            surface_product_override,
            pressure_product_override,
        );
        for bundle in &pair_plan.bundles {
            for alias in &bundle.aliases {
                let mut requirement = BundleRequirement::new(alias.bundle, bundle.id.forecast_hour);
                if let Some(ref over) = alias.native_override {
                    requirement = requirement.with_native_override(over.clone());
                }
                builder.require_with_logical_family(&requirement, alias.logical_family.as_deref());
            }
        }
    }
    let mut seen_native_products = BTreeSet::<String>::new();
    for route in native_routes {
        if seen_native_products.insert(route.candidate.fetch_product.to_string()) {
            let requirement =
                BundleRequirement::new(CanonicalBundleDescriptor::NativeAnalysis, forecast_hour)
                    .with_native_override(route.candidate.fetch_product);
            builder.require_with_logical_family(
                &requirement,
                Some(&format!("thermo-native:{}", route.candidate.fetch_product)),
            );
        }
    }
    builder.build()
}

fn compute_derived_fields_generic<S, P>(
    surface: &S,
    pressure: &P,
    recipes: &[DerivedRecipe],
) -> Result<DerivedComputedFields, Box<dyn std::error::Error>>
where
    S: SurfaceFieldSet,
    P: PressureFieldSet,
{
    fn missing_dependency(name: &str) -> std::io::Error {
        std::io::Error::other(format!(
            "derived compute missing required dependency: {name}"
        ))
    }

    fn require_option_ref<'a, T>(
        option: &'a Option<T>,
        name: &str,
    ) -> Result<&'a T, Box<dyn std::error::Error>> {
        option
            .as_ref()
            .ok_or_else(|| missing_dependency(name))
            .map_err(Into::into)
    }

    fn require_option_copy<T: Copy>(
        option: Option<T>,
        name: &str,
    ) -> Result<T, Box<dyn std::error::Error>> {
        option
            .ok_or_else(|| missing_dependency(name))
            .map_err(Into::into)
    }

    let requirements = DerivedRequirements::from_recipes(recipes);
    let grid = CalcGridShape::new(surface.nx(), surface.ny())?;
    let mut computed = DerivedComputedFields::default();

    let surface_inputs = SurfaceInputs {
        psfc_pa: surface.psfc_pa(),
        t2_k: surface.t2_k(),
        q2_kgkg: surface.q2_kgkg(),
        u10_ms: surface.u10_ms(),
        v10_ms: surface.v10_ms(),
    };

    let shape = if requirements.needs_height_agl() {
        Some(VolumeShape::new(
            grid,
            pressure.pressure_levels_hpa().len(),
        )?)
    } else {
        None
    };
    let pressure_3d_pa = if requirements.needs_volume() {
        Some(broadcast_levels_pa(
            pressure.pressure_levels_hpa(),
            grid.len(),
        ))
    } else {
        None
    };
    let height_agl_3d = if requirements.needs_height_agl() {
        Some(compute_height_agl_3d_generic(
            surface,
            pressure,
            grid,
            require_option_copy(shape, "volume shape for height_agl")?,
        ))
    } else {
        None
    };

    let make_volume = || -> Result<EcapeVolumeInputs<'_>, Box<dyn std::error::Error>> {
        Ok(EcapeVolumeInputs {
            pressure_pa: require_option_ref(
                &pressure_3d_pa,
                "pressure volume for derived thermodynamics",
            )?,
            temperature_c: pressure.temperature_c_3d(),
            qvapor_kgkg: pressure.qvapor_kgkg_3d(),
            height_agl_m: require_option_ref(
                &height_agl_3d,
                "height_agl for derived thermodynamics",
            )?,
            u_ms: pressure.u_ms_3d(),
            v_ms: pressure.v_ms_3d(),
            nz: require_option_copy(shape, "volume shape for derived thermodynamics")?.nz,
        })
    };
    let make_wind = || -> Result<WindGridInputs<'_>, Box<dyn std::error::Error>> {
        Ok(WindGridInputs {
            shape: require_option_copy(shape, "volume shape for wind diagnostics")?,
            u_3d_ms: pressure.u_ms_3d(),
            v_3d_ms: pressure.v_ms_3d(),
            height_agl_3d_m: require_option_ref(&height_agl_3d, "height_agl for wind diagnostics")?,
        })
    };

    let sb = if requirements.sb {
        Some(compute_sbcape_cin(
            grid,
            make_volume()?,
            surface_inputs,
            None,
        )?)
    } else {
        None
    };
    let ml = if requirements.ml {
        Some(compute_mlcape_cin(
            grid,
            make_volume()?,
            surface_inputs,
            None,
        )?)
    } else {
        None
    };
    let mu = if requirements.mu {
        Some(compute_mucape_cin(
            grid,
            make_volume()?,
            surface_inputs,
            None,
        )?)
    } else {
        None
    };

    if let Some(sb) = sb.as_ref() {
        computed.sbcape_jkg = Some(sb.cape_jkg.clone());
        computed.sbcin_jkg = Some(sb.cin_jkg.clone());
        computed.sblcl_m = Some(sb.lcl_m.clone());
    }
    if let Some(ml) = ml.as_ref() {
        computed.mlcape_jkg = Some(ml.cape_jkg.clone());
        computed.mlcin_jkg = Some(ml.cin_jkg.clone());
    }
    if let Some(mu) = mu.as_ref() {
        computed.mucape_jkg = Some(mu.cape_jkg.clone());
        computed.mucin_jkg = Some(mu.cin_jkg.clone());
    }

    if requirements.surface_thermo {
        let surface_thermo = compute_surface_thermo(grid, surface_inputs)?;
        if recipes.contains(&DerivedRecipe::ThetaE2m10mWinds) {
            computed.theta_e_2m_k = Some(surface_thermo.theta_e_2m_k);
            computed.surface_u10_ms = Some(surface.u10_ms().to_vec());
            computed.surface_v10_ms = Some(surface.v10_ms().to_vec());
        }
        if recipes.contains(&DerivedRecipe::ApparentTemperature2m) {
            computed.apparent_temperature_2m_c =
                Some(compute_2m_apparent_temperature(grid, surface_inputs)?);
        }
        if recipes.contains(&DerivedRecipe::HeatIndex2m) {
            computed.heat_index_2m_c = Some(surface_thermo.heat_index_2m_c);
        }
        if recipes.contains(&DerivedRecipe::WindChill2m) {
            computed.wind_chill_2m_c = Some(surface_thermo.wind_chill_2m_c);
        }
    }

    if requirements.lifted_index {
        computed.lifted_index_c = Some(compute_lifted_index(grid, make_volume()?, surface_inputs)?);
    }
    if requirements.lapse_rate_700_500 {
        computed.lapse_rate_700_500_cpkm = Some(compute_lapse_rate_700_500(grid, make_volume()?)?);
    }
    if requirements.lapse_rate_0_3km {
        computed.lapse_rate_0_3km_cpkm = Some(compute_lapse_rate_0_3km(
            grid,
            make_volume()?,
            surface_inputs,
        )?);
    }

    let shear_01km_ms = if requirements.shear_01km {
        Some(compute_shear_01km(make_wind()?)?)
    } else {
        None
    };
    let shear_06km_ms = if requirements.shear_06km {
        Some(compute_shear_06km(make_wind()?)?)
    } else {
        None
    };
    let srh_01km_m2s2 = if requirements.srh_01km {
        Some(compute_srh_01km(make_wind()?)?)
    } else {
        None
    };
    let srh_03km_m2s2 = if requirements.srh_03km {
        Some(compute_srh_03km(make_wind()?)?)
    } else {
        None
    };

    if let Some(values) = shear_01km_ms {
        computed.shear_01km_kt = Some(
            values
                .into_iter()
                .map(|value| value * KNOTS_PER_MS)
                .collect(),
        );
    }
    if let Some(values) = shear_06km_ms.as_ref() {
        computed.shear_06km_kt = Some(
            values
                .iter()
                .copied()
                .map(|value| value * KNOTS_PER_MS)
                .collect(),
        );
    }
    if let Some(values) = srh_01km_m2s2.as_ref() {
        computed.srh_01km_m2s2 = Some(values.clone());
    }
    if let Some(values) = srh_03km_m2s2.as_ref() {
        computed.srh_03km_m2s2 = Some(values.clone());
    }

    if requirements.ehi_01km {
        let sb = require_option_ref(&sb, "surface-based CAPE/CIN outputs for EHI 0-1 km")?;
        let srh_01km = require_option_ref(&srh_01km_m2s2, "0-1 km SRH for EHI 0-1 km")?;
        computed.ehi_01km = Some(compute_ehi_01km(grid, &sb.cape_jkg, srh_01km)?);
    }
    if requirements.ehi_03km {
        let sb = require_option_ref(&sb, "surface-based CAPE/CIN outputs for EHI 0-3 km")?;
        let srh_03km = require_option_ref(&srh_03km_m2s2, "0-3 km SRH for EHI 0-3 km")?;
        computed.ehi_03km = Some(compute_ehi_03km(grid, &sb.cape_jkg, srh_03km)?);
    }
    if requirements.stp_fixed {
        let sb = require_option_ref(&sb, "surface-based CAPE/CIN outputs for STP fixed")?;
        let srh_01km = require_option_ref(&srh_01km_m2s2, "0-1 km SRH for STP fixed")?;
        let shear_06km = require_option_ref(&shear_06km_ms, "0-6 km shear for STP fixed")?;
        computed.stp_fixed = Some(compute_stp_fixed(FixedStpInputs {
            grid,
            sbcape_jkg: &sb.cape_jkg,
            lcl_m: &sb.lcl_m,
            srh_1km_m2s2: srh_01km,
            shear_6km_ms: shear_06km,
        })?);
    }
    if requirements.scp_mu_03km_06km_proxy {
        let mu = require_option_ref(&mu, "most-unstable CAPE/CIN outputs for SCP proxy")?;
        let srh_03km = require_option_ref(&srh_03km_m2s2, "0-3 km SRH for SCP proxy")?;
        let shear_06km = require_option_ref(&shear_06km_ms, "0-6 km shear for SCP proxy")?;
        computed.scp_mu_03km_06km_proxy = Some(rustwx_calc::compute_scp(
            grid,
            &mu.cape_jkg,
            srh_03km,
            shear_06km,
        )?);
    }

    if requirements.needs_grid_spacing() {
        let (dx_m, dy_m) = estimate_grid_spacing_m(surface)?;
        if requirements.temperature_advection_700mb {
            let t700 = level_slice(
                pressure.temperature_c_3d(),
                pressure.pressure_levels_hpa(),
                700.0,
                grid.len(),
            )
            .ok_or("missing 700 mb temperature slice in HRRR pressure bundle")?;
            let u700 = level_slice(
                pressure.u_ms_3d(),
                pressure.pressure_levels_hpa(),
                700.0,
                grid.len(),
            )
            .ok_or("missing 700 mb u-wind slice in HRRR pressure bundle")?;
            let v700 = level_slice(
                pressure.v_ms_3d(),
                pressure.pressure_levels_hpa(),
                700.0,
                grid.len(),
            )
            .ok_or("missing 700 mb v-wind slice in HRRR pressure bundle")?;
            computed.temperature_advection_700mb_cph = Some(
                rustwx_calc::compute_temperature_advection_700mb(TemperatureAdvectionInputs {
                    grid,
                    temperature_2d: t700,
                    u_2d_ms: u700,
                    v_2d_ms: v700,
                    dx_m,
                    dy_m,
                })?
                .into_iter()
                .map(|value| value * 3600.0)
                .collect(),
            );
        }
        if requirements.temperature_advection_850mb {
            let t850 = level_slice(
                pressure.temperature_c_3d(),
                pressure.pressure_levels_hpa(),
                850.0,
                grid.len(),
            )
            .ok_or("missing 850 mb temperature slice in HRRR pressure bundle")?;
            let u850 = level_slice(
                pressure.u_ms_3d(),
                pressure.pressure_levels_hpa(),
                850.0,
                grid.len(),
            )
            .ok_or("missing 850 mb u-wind slice in HRRR pressure bundle")?;
            let v850 = level_slice(
                pressure.v_ms_3d(),
                pressure.pressure_levels_hpa(),
                850.0,
                grid.len(),
            )
            .ok_or("missing 850 mb v-wind slice in HRRR pressure bundle")?;
            computed.temperature_advection_850mb_cph = Some(
                rustwx_calc::compute_temperature_advection_850mb(TemperatureAdvectionInputs {
                    grid,
                    temperature_2d: t850,
                    u_2d_ms: u850,
                    v_2d_ms: v850,
                    dx_m,
                    dy_m,
                })?
                .into_iter()
                .map(|value| value * 3600.0)
                .collect(),
            );
        }
    }

    Ok(computed)
}

fn build_render_artifact(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    model: ModelId,
    output_width: u32,
    output_height: u32,
    computed: &DerivedComputedFields,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let (field, mut request) = match recipe {
        DerivedRecipe::Sbcape => solar07_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.sbcape_jkg, recipe, "sbcape_jkg")?.clone(),
            Solar07Product::Sbcape,
        )?,
        DerivedRecipe::Sbcin => solar07_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.sbcin_jkg, recipe, "sbcin_jkg")?.clone(),
            Solar07Product::Sbcin,
        )?,
        DerivedRecipe::Sblcl => solar07_request(
            recipe,
            grid,
            "m",
            required_values(&computed.sblcl_m, recipe, "sblcl_m")?.clone(),
            Solar07Product::Lcl,
        )?,
        DerivedRecipe::Mlcape => solar07_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mlcape_jkg, recipe, "mlcape_jkg")?.clone(),
            Solar07Product::Mlcape,
        )?,
        DerivedRecipe::Mlcin => solar07_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mlcin_jkg, recipe, "mlcin_jkg")?.clone(),
            Solar07Product::Mlcin,
        )?,
        DerivedRecipe::Mucape => solar07_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mucape_jkg, recipe, "mucape_jkg")?.clone(),
            Solar07Product::Mucape,
        )?,
        DerivedRecipe::Mucin => solar07_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mucin_jkg, recipe, "mucin_jkg")?.clone(),
            Solar07Product::Mucin,
        )?,
        DerivedRecipe::ThetaE2m10mWinds => palette_request(
            recipe,
            grid,
            "K",
            required_values(&computed.theta_e_2m_k, recipe, "theta_e_2m_k")?.clone(),
            Solar07Palette::Temperature,
            range_step(280.0, 381.0, 4.0),
            ExtendMode::Both,
            Some(8.0),
        )?,
        DerivedRecipe::ApparentTemperature2m => derived_style_request(
            recipe,
            grid,
            "degC",
            required_values(
                &computed.apparent_temperature_2m_c,
                recipe,
                "apparent_temperature_2m_c",
            )?
            .clone(),
            DerivedProductStyle::ApparentTemperature,
        )?,
        DerivedRecipe::HeatIndex2m => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.heat_index_2m_c, recipe, "heat_index_2m_c")?.clone(),
            Solar07Palette::Temperature,
            range_step(-30.0, 51.0, 5.0),
            ExtendMode::Both,
            Some(5.0),
        )?,
        DerivedRecipe::WindChill2m => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.wind_chill_2m_c, recipe, "wind_chill_2m_c")?.clone(),
            Solar07Palette::Temperature,
            range_step(-40.0, 31.0, 5.0),
            ExtendMode::Both,
            Some(5.0),
        )?,
        DerivedRecipe::LiftedIndex => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.lifted_index_c, recipe, "lifted_index_c")?.clone(),
            Solar07Palette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        DerivedRecipe::LapseRate700500 => solar07_lapse_request(
            recipe,
            grid,
            required_values(
                &computed.lapse_rate_700_500_cpkm,
                recipe,
                "lapse_rate_700_500_cpkm",
            )?
            .clone(),
        )?,
        DerivedRecipe::LapseRate03km => solar07_lapse_request(
            recipe,
            grid,
            required_values(
                &computed.lapse_rate_0_3km_cpkm,
                recipe,
                "lapse_rate_0_3km_cpkm",
            )?
            .clone(),
        )?,
        DerivedRecipe::BulkShear01km => palette_request(
            recipe,
            grid,
            "kt",
            required_values(&computed.shear_01km_kt, recipe, "shear_01km_kt")?.clone(),
            Solar07Palette::Winds,
            range_step(0.0, 85.0, 5.0),
            ExtendMode::Max,
            Some(5.0),
        )?,
        DerivedRecipe::BulkShear06km => palette_request(
            recipe,
            grid,
            "kt",
            required_values(&computed.shear_06km_kt, recipe, "shear_06km_kt")?.clone(),
            Solar07Palette::Winds,
            range_step(0.0, 85.0, 5.0),
            ExtendMode::Max,
            Some(5.0),
        )?,
        DerivedRecipe::Srh01km => solar07_request(
            recipe,
            grid,
            "m^2/s^2",
            required_values(&computed.srh_01km_m2s2, recipe, "srh_01km_m2s2")?.clone(),
            Solar07Product::Srh01km,
        )?,
        DerivedRecipe::Srh03km => solar07_request(
            recipe,
            grid,
            "m^2/s^2",
            required_values(&computed.srh_03km_m2s2, recipe, "srh_03km_m2s2")?.clone(),
            Solar07Product::Srh03km,
        )?,
        DerivedRecipe::Ehi01km => solar07_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.ehi_01km, recipe, "ehi_01km")?.clone(),
            Solar07Product::Ehi,
        )?,
        DerivedRecipe::Ehi03km => solar07_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.ehi_03km, recipe, "ehi_03km")?.clone(),
            Solar07Product::Ehi,
        )?,
        DerivedRecipe::StpFixed => solar07_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.stp_fixed, recipe, "stp_fixed")?.clone(),
            Solar07Product::StpFixed,
        )?,
        DerivedRecipe::ScpMu03km06kmProxy => solar07_request(
            recipe,
            grid,
            "dimensionless",
            required_values(
                &computed.scp_mu_03km_06km_proxy,
                recipe,
                "scp_mu_03km_06km_proxy",
            )?
            .clone(),
            Solar07Product::Scp,
        )?,
        DerivedRecipe::TemperatureAdvection700mb => palette_request(
            recipe,
            grid,
            "degC/hr",
            required_values(
                &computed.temperature_advection_700mb_cph,
                recipe,
                "temperature_advection_700mb_cph",
            )?
            .clone(),
            Solar07Palette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        DerivedRecipe::TemperatureAdvection850mb => palette_request(
            recipe,
            grid,
            "degC/hr",
            required_values(
                &computed.temperature_advection_850mb_cph,
                recipe,
                "temperature_advection_850mb_cph",
            )?
            .clone(),
            Solar07Palette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
    };

    request.width = output_width;
    request.height = output_height;
    request.supersample_factor = 2;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.title = Some(recipe.title().to_string());
    request.subtitle_left = Some(format!(
        "{} {}Z F{:03}  {}",
        date_yyyymmdd, cycle_utc, forecast_hour, model
    ));
    request.subtitle_right = Some(format!("source: {}", source));
    request.projected_domain = Some(ProjectedDomain {
        x: projected.projected_x.clone(),
        y: projected.projected_y.clone(),
        extent: projected.extent.clone(),
    });
    request.projected_lines = projected.lines.clone();
    request.projected_polygons = projected.polygons.clone();
    if matches!(recipe, DerivedRecipe::ThetaE2m10mWinds) {
        let u_kt = computed_surface_u10(computed, recipe)?;
        let v_kt = computed_surface_v10(computed, recipe)?;
        request.wind_barbs.push(surface_wind_barb_layer(
            grid,
            &projected.extent,
            &projected.projected_x,
            &projected.projected_y,
            &u_kt,
            &v_kt,
        ));
    }
    Ok(HrrrDerivedLiveArtifact {
        recipe_slug: recipe.slug().to_string(),
        title: recipe.title().to_string(),
        field,
        request,
    })
}

fn required_values<'a>(
    values: &'a Option<Vec<f64>>,
    recipe: DerivedRecipe,
    field_name: &str,
) -> Result<&'a Vec<f64>, Box<dyn std::error::Error>> {
    values.as_ref().ok_or_else(|| {
        format!(
            "derived field '{field_name}' was not computed for requested recipe '{}'",
            recipe.slug()
        )
        .into()
    })
}

fn crop_optional_values(
    values: &Option<Vec<f64>>,
    source_nx: usize,
    crop: crate::gridded::GridCrop,
) -> Option<Vec<f64>> {
    values
        .as_ref()
        .map(|values| crop_values_f64(values, source_nx, crop))
}

fn crop_computed_fields(
    computed: &DerivedComputedFields,
    source_nx: usize,
    crop: crate::gridded::GridCrop,
) -> DerivedComputedFields {
    DerivedComputedFields {
        sbcape_jkg: crop_optional_values(&computed.sbcape_jkg, source_nx, crop),
        sbcin_jkg: crop_optional_values(&computed.sbcin_jkg, source_nx, crop),
        sblcl_m: crop_optional_values(&computed.sblcl_m, source_nx, crop),
        mlcape_jkg: crop_optional_values(&computed.mlcape_jkg, source_nx, crop),
        mlcin_jkg: crop_optional_values(&computed.mlcin_jkg, source_nx, crop),
        mucape_jkg: crop_optional_values(&computed.mucape_jkg, source_nx, crop),
        mucin_jkg: crop_optional_values(&computed.mucin_jkg, source_nx, crop),
        theta_e_2m_k: crop_optional_values(&computed.theta_e_2m_k, source_nx, crop),
        apparent_temperature_2m_c: crop_optional_values(
            &computed.apparent_temperature_2m_c,
            source_nx,
            crop,
        ),
        heat_index_2m_c: crop_optional_values(&computed.heat_index_2m_c, source_nx, crop),
        wind_chill_2m_c: crop_optional_values(&computed.wind_chill_2m_c, source_nx, crop),
        surface_u10_ms: crop_optional_values(&computed.surface_u10_ms, source_nx, crop),
        surface_v10_ms: crop_optional_values(&computed.surface_v10_ms, source_nx, crop),
        lifted_index_c: crop_optional_values(&computed.lifted_index_c, source_nx, crop),
        lapse_rate_700_500_cpkm: crop_optional_values(
            &computed.lapse_rate_700_500_cpkm,
            source_nx,
            crop,
        ),
        lapse_rate_0_3km_cpkm: crop_optional_values(
            &computed.lapse_rate_0_3km_cpkm,
            source_nx,
            crop,
        ),
        shear_01km_kt: crop_optional_values(&computed.shear_01km_kt, source_nx, crop),
        shear_06km_kt: crop_optional_values(&computed.shear_06km_kt, source_nx, crop),
        srh_01km_m2s2: crop_optional_values(&computed.srh_01km_m2s2, source_nx, crop),
        srh_03km_m2s2: crop_optional_values(&computed.srh_03km_m2s2, source_nx, crop),
        ehi_01km: crop_optional_values(&computed.ehi_01km, source_nx, crop),
        ehi_03km: crop_optional_values(&computed.ehi_03km, source_nx, crop),
        stp_fixed: crop_optional_values(&computed.stp_fixed, source_nx, crop),
        scp_mu_03km_06km_proxy: crop_optional_values(
            &computed.scp_mu_03km_06km_proxy,
            source_nx,
            crop,
        ),
        temperature_advection_700mb_cph: crop_optional_values(
            &computed.temperature_advection_700mb_cph,
            source_nx,
            crop,
        ),
        temperature_advection_850mb_cph: crop_optional_values(
            &computed.temperature_advection_850mb_cph,
            source_nx,
            crop,
        ),
    }
}

fn computed_surface_u10(
    computed: &DerivedComputedFields,
    recipe: DerivedRecipe,
) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    Ok(
        required_values(&computed.surface_u10_ms, recipe, "surface_u10_ms")?
            .iter()
            .map(|value| (*value * KNOTS_PER_MS) as f32)
            .collect(),
    )
}

fn computed_surface_v10(
    computed: &DerivedComputedFields,
    recipe: DerivedRecipe,
) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    Ok(
        required_values(&computed.surface_v10_ms, recipe, "surface_v10_ms")?
            .iter()
            .map(|value| (*value * KNOTS_PER_MS) as f32)
            .collect(),
    )
}

fn surface_wind_barb_layer(
    grid: &rustwx_core::LatLonGrid,
    extent: &ProjectedExtent,
    projected_x: &[f64],
    projected_y: &[f64],
    u_kt: &[f32],
    v_kt: &[f32],
) -> WindBarbLayer {
    let (visible_nx, visible_ny) = visible_projected_grid_span(
        grid.shape.nx,
        grid.shape.ny,
        projected_x,
        projected_y,
        extent,
    );
    let stride_x = ((visible_nx as f64 / 24.0).round() as usize).clamp(3, 128);
    let stride_y = ((visible_ny as f64 / 14.0).round() as usize).clamp(3, 96);
    WindBarbLayer {
        u: u_kt.to_vec(),
        v: v_kt.to_vec(),
        stride_x,
        stride_y,
        color: Color::BLACK,
        width: 1,
        length_px: 20.0,
    }
}

fn visible_projected_grid_span(
    nx: usize,
    ny: usize,
    projected_x: &[f64],
    projected_y: &[f64],
    extent: &ProjectedExtent,
) -> (usize, usize) {
    let mut min_i = usize::MAX;
    let mut max_i = 0usize;
    let mut min_j = usize::MAX;
    let mut max_j = 0usize;

    for j in 0..ny {
        for i in 0..nx {
            let idx = j * nx + i;
            let x = projected_x[idx];
            let y = projected_y[idx];
            if x >= extent.x_min && x <= extent.x_max && y >= extent.y_min && y <= extent.y_max {
                min_i = min_i.min(i);
                max_i = max_i.max(i);
                min_j = min_j.min(j);
                max_j = max_j.max(j);
            }
        }
    }

    if min_i == usize::MAX || min_j == usize::MAX {
        return (nx.max(1), ny.max(1));
    }

    (max_i - min_i + 1, max_j - min_j + 1)
}

fn solar07_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    product: Solar07Product,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, units, grid, values)?;
    let request = MapRenderRequest::for_core_solar07_product(field.clone(), product)
        .with_visual_mode(recipe.visual_mode());
    Ok((field, request))
}

fn solar07_lapse_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    values: Vec<f64>,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, "degC/km", grid, values)?;
    let mut request = MapRenderRequest::for_palette_fill(
        field.clone().into(),
        Solar07Palette::LapseRate,
        range_step(4.0, 10.5, 0.5),
        ExtendMode::Both,
    )
    .with_visual_mode(recipe.visual_mode());
    request.cbar_tick_step = Some(0.5);
    Ok((field, request))
}

fn palette_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    palette: Solar07Palette,
    levels: Vec<f64>,
    extend: ExtendMode,
    tick_step: Option<f64>,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, units, grid, values)?;
    let mut request =
        MapRenderRequest::for_palette_fill(field.clone().into(), palette, levels, extend)
            .with_visual_mode(recipe.visual_mode());
    request.cbar_tick_step = tick_step;
    Ok((field, request))
}

fn derived_style_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    style: DerivedProductStyle,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, units, grid, values)?;
    let request = MapRenderRequest::for_derived_product(field.clone().into(), style)
        .with_visual_mode(recipe.visual_mode());
    Ok((field, request))
}

fn core_field(
    recipe: DerivedRecipe,
    units: &str,
    grid: &rustwx_core::LatLonGrid,
    values: Vec<f64>,
) -> Result<Field2D, Box<dyn std::error::Error>> {
    Ok(Field2D::new(
        ProductKey::named(recipe.slug()),
        units,
        grid.clone(),
        values.into_iter().map(|value| value as f32).collect(),
    )?)
}

fn level_slice<'a>(
    values_3d: &'a [f64],
    levels_hpa: &[f64],
    target_hpa: f64,
    nxy: usize,
) -> Option<&'a [f64]> {
    let level_idx = levels_hpa
        .iter()
        .position(|level| (level - target_hpa).abs() < 0.25)?;
    let start = level_idx * nxy;
    let end = start + nxy;
    values_3d.get(start..end)
}

fn compute_height_agl_3d_generic<S, P>(
    surface: &S,
    pressure: &P,
    grid: CalcGridShape,
    shape: VolumeShape,
) -> Vec<f64>
where
    S: SurfaceFieldSet,
    P: PressureFieldSet,
{
    let mut height_agl_3d = pressure
        .gh_m_3d()
        .iter()
        .enumerate()
        .map(|(idx, &value)| {
            let ij = idx % grid.len();
            (value - surface.orog_m()[ij]).max(0.0)
        })
        .collect::<Vec<_>>();

    for k in 1..shape.nz {
        let level_offset = k * grid.len();
        let prev_offset = (k - 1) * grid.len();
        for ij in 0..grid.len() {
            let min_height = height_agl_3d[prev_offset + ij] + 1.0;
            if height_agl_3d[level_offset + ij] < min_height {
                height_agl_3d[level_offset + ij] = min_height;
            }
        }
    }

    height_agl_3d
}

fn estimate_grid_spacing_m<S>(surface: &S) -> Result<(f64, f64), CalcError>
where
    S: SurfaceFieldSet,
{
    if surface.nx() < 2 || surface.ny() < 2 {
        return Err(CalcError::LengthMismatch {
            field: "grid_spacing",
            expected: 4,
            actual: surface.nx() * surface.ny(),
        });
    }

    let mut dx_sum = 0.0;
    let mut dx_count = 0usize;
    for y in 0..surface.ny() {
        let row_offset = y * surface.nx();
        for x in 0..(surface.nx() - 1) {
            let left = row_offset + x;
            let right = left + 1;
            let distance = haversine_m(
                surface.lat()[left],
                surface.lon()[left],
                surface.lat()[right],
                surface.lon()[right],
            );
            if distance.is_finite() && distance > 0.0 {
                dx_sum += distance;
                dx_count += 1;
            }
        }
    }

    let mut dy_sum = 0.0;
    let mut dy_count = 0usize;
    for y in 0..(surface.ny() - 1) {
        let row_offset = y * surface.nx();
        let next_row_offset = (y + 1) * surface.nx();
        for x in 0..surface.nx() {
            let top = row_offset + x;
            let bottom = next_row_offset + x;
            let distance = haversine_m(
                surface.lat()[top],
                surface.lon()[top],
                surface.lat()[bottom],
                surface.lon()[bottom],
            );
            if distance.is_finite() && distance > 0.0 {
                dy_sum += distance;
                dy_count += 1;
            }
        }
    }

    if dx_count == 0 || dy_count == 0 {
        return Err(CalcError::LengthMismatch {
            field: "grid_spacing",
            expected: 2,
            actual: 0,
        });
    }

    Ok((dx_sum / dx_count as f64, dy_sum / dy_count as f64))
}

fn haversine_m(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let lat1 = lat1_deg.to_radians();
    let lon1 = lon1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let lon2 = lon2_deg.to_radians();
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let a = (dlat * 0.5).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon * 0.5).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    6_371_000.0 * c
}

fn normalize_slug(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', ' '], "_")
}

fn png_render_parallelism(job_count: usize) -> usize {
    let override_threads = std::env::var("RUSTWX_RENDER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0);

    thread::available_parallelism()
        .map(|parallelism| override_threads.unwrap_or((parallelism.get() / 2).max(1)))
        .unwrap_or(1)
        .min(job_count.max(1))
}

fn thread_render_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

fn join_render_job<T>(
    handle: thread::ScopedJoinHandle<'_, Result<T, io::Error>>,
) -> Result<T, io::Error> {
    match handle.join() {
        Ok(result) => result,
        Err(panic) => Err(io::Error::other(format!(
            "render worker panicked: {}",
            panic_message(panic)
        ))),
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn range_step(start: f64, stop: f64, step: f64) -> Vec<f64> {
    let mut values = Vec::new();
    let mut current = start;
    while current < stop - step * 1.0e-9 {
        values.push(current);
        current += step;
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_depth_ehi_slugs_are_supported_and_legacy_aliases_canonicalize() {
        assert_eq!(
            DerivedRecipe::parse("scp_mu_0_3km_0_6km_proxy").unwrap(),
            DerivedRecipe::ScpMu03km06kmProxy
        );
        assert_eq!(
            DerivedRecipe::parse("apparent_temperature_2m").unwrap(),
            DerivedRecipe::ApparentTemperature2m
        );
        assert_eq!(
            DerivedRecipe::parse("2m_apparent_temperature").unwrap(),
            DerivedRecipe::ApparentTemperature2m
        );
        assert_eq!(
            DerivedRecipe::parse("ehi_0_1km").unwrap(),
            DerivedRecipe::Ehi01km
        );
        assert_eq!(
            DerivedRecipe::parse("ehi_sb_0_1km_proxy").unwrap(),
            DerivedRecipe::Ehi01km
        );
        assert_eq!(
            DerivedRecipe::parse("ehi_0_3km").unwrap(),
            DerivedRecipe::Ehi03km
        );
        assert_eq!(
            DerivedRecipe::parse("ehi_sb_0_3km_proxy").unwrap(),
            DerivedRecipe::Ehi03km
        );
        assert!(DerivedRecipe::parse("scp").is_err());
        assert!(DerivedRecipe::parse("stp_effective").is_err());
    }

    #[test]
    fn haversine_is_reasonable_for_one_degree_latitude() {
        let distance = haversine_m(35.0, -97.0, 36.0, -97.0);
        assert!(distance > 100_000.0);
        assert!(distance < 120_000.0);
    }

    #[test]
    fn derived_recipe_dedupe_preserves_first_seen_order() {
        let recipes = plan_derived_recipes(&[
            "mlcape".to_string(),
            "sbcape".to_string(),
            "mlcape".to_string(),
        ])
        .unwrap();
        assert_eq!(recipes, vec![DerivedRecipe::Mlcape, DerivedRecipe::Sbcape]);
    }

    #[test]
    fn derived_inventory_stays_in_sync_with_slug_parser() {
        for recipe in supported_derived_recipe_inventory() {
            assert!(
                DerivedRecipe::parse(recipe.slug).is_ok(),
                "supported inventory slug '{}' should parse",
                recipe.slug
            );
        }
        for recipe in blocked_derived_recipe_inventory() {
            assert!(
                DerivedRecipe::parse(recipe.slug).is_err(),
                "blocked inventory slug '{}' should stay blocked",
                recipe.slug
            );
        }
    }

    #[test]
    fn blocked_inventory_is_narrowed_to_effective_layer_products() {
        let blocked = blocked_derived_recipe_inventory()
            .iter()
            .map(|recipe| recipe.slug)
            .collect::<Vec<_>>();
        assert_eq!(blocked, vec!["stp_effective", "scp", "scp_effective"]);
    }

    #[test]
    fn derived_requirements_stay_narrow_for_surface_only_requests() {
        let requirements = DerivedRequirements::from_recipes(&[DerivedRecipe::HeatIndex2m]);
        assert!(requirements.surface_thermo);
        assert!(!requirements.needs_volume());
        assert!(!requirements.needs_height_agl());
        assert!(!requirements.needs_grid_spacing());
    }

    #[test]
    fn apparent_temperature_is_supported_surface_only_inventory_entry() {
        let recipe = supported_derived_recipe_inventory()
            .iter()
            .find(|recipe| recipe.slug == "apparent_temperature_2m")
            .expect("apparent temperature inventory entry should exist");
        assert_eq!(recipe.title, "2 m Apparent Temperature");
        assert!(!recipe.experimental);

        let requirements =
            DerivedRequirements::from_recipes(&[DerivedRecipe::ApparentTemperature2m]);
        assert!(requirements.surface_thermo);
        assert!(!requirements.surface_winds);
        assert!(!requirements.needs_volume());
        assert!(!requirements.needs_height_agl());
        assert!(!requirements.needs_grid_spacing());
    }

    #[test]
    fn canonical_mode_keeps_all_supported_recipes_on_canonical_path() {
        let recipes = vec![
            DerivedRecipe::Sbcape,
            DerivedRecipe::LiftedIndex,
            DerivedRecipe::BulkShear06km,
        ];
        let planned =
            plan_native_thermo_routes(ModelId::Hrrr, &recipes, ProductSourceMode::Canonical)
                .unwrap();
        assert_eq!(planned.output_recipes, recipes);
        assert_eq!(planned.compute_recipes, recipes);
        assert!(planned.native_routes.is_empty());
        assert!(planned.blockers.is_empty());
    }

    #[test]
    fn fastest_mode_uses_native_exact_and_blocks_non_fast_recipes() {
        let recipes = vec![DerivedRecipe::Sbcape, DerivedRecipe::BulkShear06km];
        let planned =
            plan_native_thermo_routes(ModelId::Hrrr, &recipes, ProductSourceMode::Fastest).unwrap();
        assert_eq!(planned.output_recipes, vec![DerivedRecipe::Sbcape]);
        assert!(planned.compute_recipes.is_empty());
        assert_eq!(planned.native_routes.len(), 1);
        assert_eq!(planned.native_routes[0].recipe, DerivedRecipe::Sbcape);
        assert_eq!(
            planned.native_routes[0].source_route,
            ProductSourceRoute::NativeExact
        );
        assert_eq!(planned.blockers.len(), 1);
        assert_eq!(planned.blockers[0].recipe_slug, "bulk_shear_0_6km");
        assert_eq!(
            planned.blockers[0].source_route,
            ProductSourceRoute::BlockedNoFastRoute
        );
    }

    #[test]
    fn fastest_mode_keeps_proxy_native_routes_when_labeled() {
        let planned = plan_native_thermo_routes(
            ModelId::Gfs,
            &[DerivedRecipe::Mlcape],
            ProductSourceMode::Fastest,
        )
        .unwrap();
        assert_eq!(planned.output_recipes, vec![DerivedRecipe::Mlcape]);
        assert_eq!(planned.native_routes.len(), 1);
        assert_eq!(
            planned.native_routes[0].source_route,
            ProductSourceRoute::NativeProxy
        );
        assert!(planned.blockers.is_empty());
    }

    #[test]
    fn fastest_mode_blocks_surface_only_canonical_shortcuts_until_a_true_fast_path_exists() {
        let planned = plan_native_thermo_routes(
            ModelId::Hrrr,
            &[DerivedRecipe::HeatIndex2m],
            ProductSourceMode::Fastest,
        )
        .unwrap();
        assert!(planned.output_recipes.is_empty());
        assert!(planned.native_routes.is_empty());
        assert_eq!(planned.blockers.len(), 1);
        assert!(
            planned.blockers[0]
                .reason
                .contains("will not fall back to canonical-derived compute")
        );
    }

    #[test]
    fn cycle_pinned_fastest_native_only_run_skips_pair_resolution() {
        let request = DerivedBatchRequest {
            model: ModelId::Hrrr,
            date_yyyymmdd: "20260418".to_string(),
            cycle_override_utc: Some(12),
            forecast_hour: 0,
            source: SourceId::Aws,
            domain: DomainSpec::new("midwest", (-104.0, -80.0, 34.0, 49.0)),
            out_dir: PathBuf::from("target\\test-out"),
            cache_root: PathBuf::from("target\\test-cache"),
            use_cache: true,
            recipe_slugs: vec!["sbcape".to_string()],
            surface_product_override: None,
            pressure_product_override: None,
            source_mode: ProductSourceMode::Fastest,
            output_width: OUTPUT_WIDTH,
            output_height: OUTPUT_HEIGHT,
            png_compression: PngCompressionMode::Default,
        };
        let planned =
            plan_native_thermo_routes(request.model, &[DerivedRecipe::Sbcape], request.source_mode)
                .unwrap();

        let latest =
            resolve_derived_run(&request, &planned.compute_recipes, &planned.native_routes)
                .unwrap();

        assert_eq!(latest.model, ModelId::Hrrr);
        assert_eq!(latest.cycle.date_yyyymmdd, "20260418");
        assert_eq!(latest.cycle.hour_utc, 12);
        assert_eq!(latest.source, SourceId::Aws);
    }
}
