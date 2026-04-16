use rustwx_calc::{
    CalcError, EcapeVolumeInputs, FixedStpInputs, GridShape as CalcGridShape, SurfaceInputs,
    TemperatureAdvectionInputs, VolumeShape, WindGridInputs, compute_2m_apparent_temperature,
    compute_ehi_01km, compute_ehi_03km, compute_lapse_rate_0_3km, compute_lapse_rate_700_500,
    compute_lifted_index, compute_mlcape_cin, compute_mucape_cin, compute_sbcape_cin,
    compute_shear_01km, compute_shear_06km, compute_srh_01km, compute_srh_03km, compute_stp_fixed,
    compute_surface_thermo,
};
use rustwx_core::{Field2D, ModelId, ProductKey, SourceId};
use rustwx_render::{
    Color, DerivedProductStyle, ExtendMode, MapRenderRequest, ProjectedDomain, ProjectedExtent,
    Solar07Palette, Solar07Product, WindBarbLayer, save_png,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use crate::direct::build_projected_map as build_projected_map_from_latlon;
use crate::gridded::{
    LoadedModelTimestep, PressureFields as GenericPressureFields,
    SurfaceFields as GenericSurfaceFields, load_model_timestep_from_parts,
};
use crate::hrrr::{
    DomainSpec, HrrrSharedTiming, HrrrSurfaceFields, PreparedHrrrHourContext, broadcast_levels_pa,
    build_projected_map as build_hrrr_projected_map,
};

const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;
const KNOTS_PER_MS: f64 = 1.943_844_5;

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

impl SurfaceFieldSet for HrrrSurfaceFields {
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

impl PressureFieldSet for crate::hrrr::HrrrPressureFields {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDerivedSharedTiming {
    pub fetch_decode: HrrrSharedTiming,
    pub compute_ms: u128,
    pub project_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDerivedRecipeTiming {
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDerivedRenderedRecipe {
    pub recipe_slug: String,
    pub title: String,
    pub output_path: PathBuf,
    pub timing: HrrrDerivedRecipeTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedBatchReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub shared_timing: HrrrDerivedSharedTiming,
    pub recipes: Vec<HrrrDerivedRenderedRecipe>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrDerivedBatchReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub shared_timing: HrrrDerivedSharedTiming,
    pub recipes: Vec<HrrrDerivedRenderedRecipe>,
    pub total_ms: u128,
}

#[derive(Debug, Clone)]
pub struct HrrrDerivedLiveArtifact {
    pub recipe_slug: String,
    pub title: String,
    pub field: Field2D,
    pub request: MapRenderRequest,
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
    fn from_hrrr(request: &HrrrDerivedBatchRequest) -> Self {
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
    let timestep = load_model_timestep_from_parts(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
        &request.cache_root,
        request.use_cache,
    )?;
    run_derived_batch_from_loaded(request, &recipes, &timestep)
}

pub fn run_hrrr_derived_batch(
    request: &HrrrDerivedBatchRequest,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    Ok(into_hrrr_report(run_derived_batch(
        &DerivedBatchRequest::from_hrrr(request),
    )?))
}

fn run_derived_batch_from_loaded(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    timestep: &LoadedModelTimestep,
) -> Result<DerivedBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }
    let total_start = Instant::now();

    let project_start = Instant::now();
    let projected = build_projected_map_from_latlon(
        &timestep.grid.lat_deg,
        &timestep.grid.lon_deg,
        request.domain.bounds,
        wrf_render::render::map_frame_aspect_ratio(OUTPUT_WIDTH, OUTPUT_HEIGHT, true, true),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let computed = compute_derived_fields_generic(
        &timestep.surface_decode.value,
        &timestep.pressure_decode.value,
        recipes,
    )?;
    let compute_ms = compute_start.elapsed().as_millis();

    let render_parallelism = png_render_parallelism(recipes.len());
    let grid = timestep.grid();
    let projected = &projected;
    let date_yyyymmdd = request.date_yyyymmdd.as_str();
    let cycle_utc = timestep.latest.cycle.hour_utc;
    let forecast_hour = request.forecast_hour;
    let source = timestep.latest.source;
    let model = request.model;
    let computed = &computed;
    let rendered = thread::scope(
        |scope| -> Result<Vec<HrrrDerivedRenderedRecipe>, io::Error> {
            let mut rendered = Vec::with_capacity(recipes.len());
            let mut pending = VecDeque::new();

            for &recipe in recipes {
                let model_slug = request.model.as_str().replace('-', "_");
                let output_path = request.out_dir.join(format!(
                    "rustwx_{}_{}_{}z_f{:03}_{}_{}.png",
                    model_slug,
                    request.date_yyyymmdd,
                    timestep.latest.cycle.hour_utc,
                    request.forecast_hour,
                    request.domain.slug,
                    recipe.slug()
                ));
                pending.push_back(scope.spawn(
                    move || -> Result<HrrrDerivedRenderedRecipe, io::Error> {
                        let render_start = Instant::now();
                        let render_artifact = build_render_artifact(
                            recipe,
                            grid,
                            projected,
                            date_yyyymmdd,
                            cycle_utc,
                            forecast_hour,
                            source,
                            model,
                            computed,
                        )
                        .map_err(thread_render_error)?;
                        save_png(&render_artifact.request, &output_path)
                            .map_err(thread_render_error)?;
                        let render_ms = render_start.elapsed().as_millis();
                        Ok(HrrrDerivedRenderedRecipe {
                            recipe_slug: render_artifact.recipe_slug,
                            title: render_artifact.title,
                            output_path,
                            timing: HrrrDerivedRecipeTiming {
                                render_ms,
                                total_ms: render_ms,
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
        },
    )
    .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    Ok(DerivedBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: timestep.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: timestep.latest.source,
        domain: request.domain.clone(),
        shared_timing: HrrrDerivedSharedTiming {
            fetch_decode: HrrrSharedTiming {
                fetch_surface_ms: timestep.shared_timing.fetch_surface_ms,
                fetch_pressure_ms: timestep.shared_timing.fetch_pressure_ms,
                decode_surface_ms: timestep.shared_timing.decode_surface_ms,
                decode_pressure_ms: timestep.shared_timing.decode_pressure_ms,
                fetch_surface_cache_hit: timestep.shared_timing.fetch_surface_cache_hit,
                fetch_pressure_cache_hit: timestep.shared_timing.fetch_pressure_cache_hit,
                decode_surface_cache_hit: timestep.shared_timing.decode_surface_cache_hit,
                decode_pressure_cache_hit: timestep.shared_timing.decode_pressure_cache_hit,
                surface_fetch: crate::hrrr::HrrrFetchRuntimeInfo {
                    planned_product: timestep.shared_timing.surface_fetch.planned_product.clone(),
                    fetched_product: timestep.shared_timing.surface_fetch.fetched_product.clone(),
                    requested_source: timestep.shared_timing.surface_fetch.requested_source,
                    resolved_source: timestep.shared_timing.surface_fetch.resolved_source,
                    resolved_url: timestep.shared_timing.surface_fetch.resolved_url.clone(),
                },
                pressure_fetch: crate::hrrr::HrrrFetchRuntimeInfo {
                    planned_product: timestep
                        .shared_timing
                        .pressure_fetch
                        .planned_product
                        .clone(),
                    fetched_product: timestep
                        .shared_timing
                        .pressure_fetch
                        .fetched_product
                        .clone(),
                    requested_source: timestep.shared_timing.pressure_fetch.requested_source,
                    resolved_source: timestep.shared_timing.pressure_fetch.resolved_source,
                    resolved_url: timestep.shared_timing.pressure_fetch.resolved_url.clone(),
                },
            },
            compute_ms,
            project_ms,
        },
        recipes: rendered,
        total_ms: total_start.elapsed().as_millis(),
    })
}

pub(crate) fn run_hrrr_derived_batch_with_context(
    request: &HrrrDerivedBatchRequest,
    recipes: &[DerivedRecipe],
    timestep: &crate::hrrr::LoadedHrrrTimestep,
    shared_context: Option<&PreparedHrrrHourContext>,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }
    let total_start = Instant::now();

    let project_start = Instant::now();
    let projected = if let Some(projected) =
        shared_context.and_then(|ctx| ctx.projected_map(OUTPUT_WIDTH, OUTPUT_HEIGHT).cloned())
    {
        projected
    } else {
        build_hrrr_projected_map(
            &timestep.surface_decode().value,
            request.domain.bounds,
            wrf_render::render::map_frame_aspect_ratio(OUTPUT_WIDTH, OUTPUT_HEIGHT, true, true),
        )?
    };
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let computed = compute_derived_fields_generic(
        &timestep.surface_decode().value,
        &timestep.pressure_decode().value,
        recipes,
    )?;
    let compute_ms = compute_start.elapsed().as_millis();

    let render_parallelism = png_render_parallelism(recipes.len());
    let grid = timestep.grid();
    let projected = &projected;
    let date_yyyymmdd = request.date_yyyymmdd.as_str();
    let cycle_utc = timestep.latest().cycle.hour_utc;
    let forecast_hour = request.forecast_hour;
    let source = timestep.latest().source;
    let model = ModelId::Hrrr;
    let computed = &computed;
    let rendered = thread::scope(
        |scope| -> Result<Vec<HrrrDerivedRenderedRecipe>, io::Error> {
            let mut rendered = Vec::with_capacity(recipes.len());
            let mut pending = VecDeque::new();

            for &recipe in recipes {
                let output_path = request.out_dir.join(format!(
                    "rustwx_hrrr_{}_{}z_f{:03}_{}_{}.png",
                    request.date_yyyymmdd,
                    timestep.latest().cycle.hour_utc,
                    request.forecast_hour,
                    request.domain.slug,
                    recipe.slug()
                ));
                pending.push_back(scope.spawn(
                    move || -> Result<HrrrDerivedRenderedRecipe, io::Error> {
                        let render_start = Instant::now();
                        let render_artifact = build_render_artifact(
                            recipe,
                            grid,
                            projected,
                            date_yyyymmdd,
                            cycle_utc,
                            forecast_hour,
                            source,
                            model,
                            computed,
                        )
                        .map_err(thread_render_error)?;
                        save_png(&render_artifact.request, &output_path)
                            .map_err(thread_render_error)?;
                        let render_ms = render_start.elapsed().as_millis();
                        Ok(HrrrDerivedRenderedRecipe {
                            recipe_slug: render_artifact.recipe_slug,
                            title: render_artifact.title,
                            output_path,
                            timing: HrrrDerivedRecipeTiming {
                                render_ms,
                                total_ms: render_ms,
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
        },
    )
    .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    Ok(HrrrDerivedBatchReport {
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: timestep.latest().cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: timestep.latest().source,
        domain: request.domain.clone(),
        shared_timing: HrrrDerivedSharedTiming {
            fetch_decode: timestep.shared_timing().clone(),
            compute_ms,
            project_ms,
        },
        recipes: rendered,
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn into_hrrr_report(report: DerivedBatchReport) -> HrrrDerivedBatchReport {
    HrrrDerivedBatchReport {
        date_yyyymmdd: report.date_yyyymmdd,
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source,
        domain: report.domain,
        shared_timing: report.shared_timing,
        recipes: report.recipes,
        total_ms: report.total_ms,
    }
}

pub fn build_hrrr_live_derived_artifact(
    recipe_slug: &str,
    surface: &HrrrSurfaceFields,
    pressure: &crate::hrrr::HrrrPressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &crate::hrrr::ProjectedMap,
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
    projected: &crate::hrrr::ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    model: ModelId,
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

    request.width = OUTPUT_WIDTH;
    request.height = OUTPUT_HEIGHT;
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
    let request = MapRenderRequest::for_core_solar07_product(field.clone(), product);
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
    );
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
        MapRenderRequest::for_palette_fill(field.clone().into(), palette, levels, extend);
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
    let request = MapRenderRequest::for_derived_product(field.clone().into(), style);
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
    thread::available_parallelism()
        .map(|parallelism| parallelism.get())
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
}
