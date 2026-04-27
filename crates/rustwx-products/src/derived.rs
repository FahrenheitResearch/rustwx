use crate::custom_poi::{CustomPoiOverlay, apply_custom_poi_overlay};
use crate::direct::{build_projected_map, build_projected_map_with_projection};
use rayon::prelude::*;
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
    ChromeScale, Color, ColorScale, DerivedProductStyle, DiscreteColorScale, DomainFrame,
    ExtendMode, LevelDensity, MapRenderRequest, PngCompressionMode, PngWriteOptions,
    ProductVisualMode, ProjectedContourLineStyle, ProjectedDomain, ProjectedExtent, ProjectedMap,
    RenderImageTiming, RenderStateTiming, WeatherPalette, WeatherProduct, WindBarbLayer,
    build_projected_contour_geometry_profile, densify_discrete_scale, map_frame_aspect_ratio,
    save_png_profile_with_options, weather::temperature_palette_cropped_f,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use crate::ecape::compute_ecape_map_fields_with_prepared_volume;
use crate::gridded::{
    GridCrop, PressureFields as GenericPressureFields, ProjectedGridIntersection,
    SharedTiming as GenericSharedTiming, SurfaceFields as GenericSurfaceFields,
    broadcast_levels_pa, classify_projected_grid_intersection, crop_latlon_grid, crop_values_f64,
    decode_cache_path, decode_surface_grid, fetch_family_file,
    load_or_decode_pressure_cropped_with_shape, load_or_decode_surface_cropped,
    prepare_heavy_volume_timed, resolve_thermo_pair_run,
};
use crate::heavy::{HeavyComputeTiming, crop_and_guard_heavy_domain};
use crate::places::PlaceLabelOverlay;
use crate::planner::{ExecutionPlanBuilder, PlannedBundle};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
};
use crate::runtime::{
    BundleLoaderConfig, CroppedDecodeProfile, FetchedBundleBytes, LoadedBundleSet,
    LoadedBundleTiming, load_execution_plan,
};
use crate::severe::{
    build_planned_input_fetches, build_severe_execution_plan, build_shared_timing_for_pair,
};
use crate::shared_context::{DomainSpec, WeatherPanelField, build_weather_map_request};
use crate::source::{ProductSourceMode, ProductSourceRoute};
use crate::thermo_native::{
    NativeSemantics, NativeThermoRecipe, extract_native_thermo_field, native_candidate,
};
use rustwx_models::{
    LatestRun, latest_available_run_at_forecast_hour,
    latest_available_run_for_products_at_forecast_hour, resolve_canonical_bundle_product,
};
#[cfg(feature = "wrf")]
use rustwx_wrf::{WrfFile, looks_like_wrf};

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

fn default_native_fill_level_multiplier() -> usize {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeContourRenderMode {
    #[default]
    Automatic,
    Signature,
    LegacyRaster,
    ExperimentalAllProjected,
}

trait SurfaceFieldSet {
    fn lat(&self) -> &[f64];
    fn lon(&self) -> &[f64];
    fn nx(&self) -> usize;
    fn ny(&self) -> usize;
    fn projection(&self) -> Option<&rustwx_core::GridProjection>;
    fn orog_m(&self) -> &[f64];
    fn psfc_pa(&self) -> &[f64];
    fn t2_k(&self) -> &[f64];
    fn q2_kgkg(&self) -> &[f64];
    fn u10_ms(&self) -> &[f64];
    fn v10_ms(&self) -> &[f64];
}

trait PressureFieldSet {
    fn pressure_levels_hpa(&self) -> &[f64];
    fn pressure_3d_pa(&self) -> Option<&[f64]>;
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
    fn projection(&self) -> Option<&rustwx_core::GridProjection> {
        self.projection.as_ref()
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
    fn pressure_3d_pa(&self) -> Option<&[f64]> {
        self.pressure_3d_pa.as_deref()
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
    pub heavy: bool,
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
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "sbcin",
        title: "SBCIN",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "sblcl",
        title: "SBLCL",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mlcape",
        title: "MLCAPE",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mlcin",
        title: "MLCIN",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mucape",
        title: "MUCAPE",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "mucin",
        title: "MUCIN",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "sbecape",
        title: "SBECAPE",
        experimental: false,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "mlecape",
        title: "MLECAPE",
        experimental: false,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "muecape",
        title: "MUECAPE",
        experimental: false,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "sb_ecape_derived_cape_ratio",
        title: "SB ECAPE / Derived CAPE Ratio (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "ml_ecape_derived_cape_ratio",
        title: "ML ECAPE / Derived CAPE Ratio (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "mu_ecape_derived_cape_ratio",
        title: "MU ECAPE / Derived CAPE Ratio (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "sb_ecape_native_cape_ratio",
        title: "SB ECAPE / Native CAPE Ratio (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "ml_ecape_native_cape_ratio",
        title: "ML ECAPE / Native CAPE Ratio (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "mu_ecape_native_cape_ratio",
        title: "MU ECAPE / Native CAPE Ratio (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "sbncape",
        title: "SBNCAPE",
        experimental: false,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "sbecin",
        title: "SBECIN",
        experimental: false,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "mlecin",
        title: "MLECIN",
        experimental: false,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "ecape_scp",
        title: "ECAPE SCP (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "ecape_ehi_0_1km",
        title: "ECAPE EHI 0-1 km (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "ecape_ehi_0_3km",
        title: "ECAPE EHI 0-3 km (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "ecape_stp",
        title: "ECAPE STP (EXP)",
        experimental: true,
        heavy: true,
    },
    DerivedRecipeInventoryEntry {
        slug: "theta_e_2m_10m_winds",
        title: "2 m Theta-e, 10 m Wind Barbs",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "vpd_2m",
        title: "2 m Vapor Pressure Deficit",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "dewpoint_depression_2m",
        title: "2 m Dewpoint Depression",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "wetbulb_2m",
        title: "2 m Wet-Bulb Temperature",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "fire_weather_composite",
        title: "Fire Weather Composite",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "apparent_temperature_2m",
        title: "2 m Apparent Temperature",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "heat_index_2m",
        title: "2 m Heat Index",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "wind_chill_2m",
        title: "2 m Wind Chill",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "lifted_index",
        title: "Surface-Based Lifted Index",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "lapse_rate_700_500",
        title: "700-500 mb Virtual Temperature Lapse Rate",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "lapse_rate_0_3km",
        title: "0-3 km Lapse Rate",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "bulk_shear_0_1km",
        title: "0-1 km Bulk Shear",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "bulk_shear_0_6km",
        title: "0-6 km Bulk Shear",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "srh_0_1km",
        title: "0-1 km SRH",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "srh_0_3km",
        title: "0-3 km SRH",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "ehi_0_1km",
        title: "EHI 0-1 km",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "ehi_0_3km",
        title: "EHI 0-3 km",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "stp_fixed",
        title: "STP (FIXED)",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "scp_mu_0_3km_0_6km_proxy",
        title: "SCP (MU / 0-3 km / 0-6 km PROXY)",
        experimental: true,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "temperature_advection_700mb",
        title: "700 mb Temperature Advection",
        experimental: false,
        heavy: false,
    },
    DerivedRecipeInventoryEntry {
        slug: "temperature_advection_850mb",
        title: "850 mb Temperature Advection",
        experimental: false,
        heavy: false,
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

pub fn is_heavy_derived_recipe_slug(slug: &str) -> bool {
    DerivedRecipe::parse(slug)
        .map(|recipe| recipe.is_heavy())
        .unwrap_or(false)
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
    #[serde(default)]
    pub allow_large_heavy_domain: bool,
    #[serde(default)]
    pub contour_mode: NativeContourRenderMode,
    #[serde(default = "default_native_fill_level_multiplier")]
    pub native_fill_level_multiplier: usize,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
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
    #[serde(default)]
    pub allow_large_heavy_domain: bool,
    #[serde(default)]
    pub contour_mode: NativeContourRenderMode,
    #[serde(default = "default_native_fill_level_multiplier")]
    pub native_fill_level_multiplier: usize,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_profile: Option<DerivedMemoryProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heavy_timing: Option<HeavyComputeTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedMemoryProfile {
    pub source_grid_nx: usize,
    pub source_grid_ny: usize,
    pub cropped_grid_nx: usize,
    pub cropped_grid_ny: usize,
    pub crop_x_start: usize,
    pub crop_x_end: usize,
    pub crop_y_start: usize,
    pub crop_y_end: usize,
    pub surface_fetch_bytes_len: usize,
    pub pressure_fetch_bytes_len: usize,
    pub cropped_surface_decoded_bytes_estimate: usize,
    pub cropped_pressure_decoded_bytes_estimate: usize,
    pub cropped_decoded_total_bytes_estimate: usize,
    pub pressure_level_count: usize,
    pub thermo_volume_points: usize,
    pub compute_recipe_count: usize,
    pub needs_volume: bool,
    pub needs_height_agl: bool,
    pub canonical_pressure_3d_pa_bytes_estimate: usize,
    pub canonical_height_agl_3d_bytes_estimate: usize,
    pub canonical_shared_volume_work_bytes_estimate: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedRecipeTiming {
    #[serde(default)]
    pub render_to_image_ms: u128,
    #[serde(default)]
    pub data_layer_draw_ms: u128,
    #[serde(default)]
    pub overlay_draw_ms: u128,
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

fn derived_data_layer_draw_ms(image_timing: &RenderImageTiming) -> u128 {
    image_timing.polygon_fill_ms
        + image_timing.projected_pixel_ms
        + image_timing.rasterize_ms
        + image_timing.raster_blit_ms
}

fn derived_overlay_draw_ms(image_timing: &RenderImageTiming) -> u128 {
    image_timing.linework_ms + image_timing.contour_ms + image_timing.barb_ms
}

#[derive(Debug, Clone)]
pub struct HrrrDerivedLiveArtifact {
    pub recipe_slug: String,
    pub title: String,
    pub field: Field2D,
    pub request: MapRenderRequest,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct DerivedLiveArtifactBuildTiming {
    pub compute_fields_ms: u128,
    pub request_base_build_ms: u128,
    pub native_contour_fill_ms: u128,
    #[serde(default)]
    pub native_contour_projected_points_ms: u128,
    #[serde(default)]
    pub native_contour_scalar_field_ms: u128,
    #[serde(default)]
    pub native_contour_fill_topology_ms: u128,
    #[serde(default)]
    pub native_contour_fill_geometry_ms: u128,
    #[serde(default)]
    pub native_contour_line_topology_ms: u128,
    #[serde(default)]
    pub native_contour_line_geometry_ms: u128,
    pub wind_overlay_build_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Copy, Default)]
struct NativeContourBuildTiming {
    total_ms: u128,
    projected_points_ms: u128,
    scalar_field_ms: u128,
    fill_topology_ms: u128,
    fill_geometry_ms: u128,
    line_topology_ms: u128,
    line_geometry_ms: u128,
}

#[derive(Debug, Clone)]
pub struct ProfiledHrrrDerivedLiveArtifact {
    pub artifact: HrrrDerivedLiveArtifact,
    pub timing: DerivedLiveArtifactBuildTiming,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedSharedDerivedFields {
    grid: rustwx_core::LatLonGrid,
    projection: Option<rustwx_core::GridProjection>,
    computed: DerivedComputedFields,
    fetch_decode: Option<GenericSharedTiming>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum NativeDerivedRecipe {
    Thermo(NativeThermoRecipe),
    WrfGdexScalar {
        variable: &'static str,
    },
    WrfGdexVectorMagnitude {
        u_variable: &'static str,
        v_variable: &'static str,
        scale: f64,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedNativeDerivedCandidate {
    pub(crate) label: String,
    pub(crate) semantics: NativeSemantics,
    pub(crate) auto_eligible: bool,
    pub(crate) detail: String,
    pub(crate) fetch_product: &'static str,
}

#[derive(Debug, Clone)]
struct NativeDerivedField {
    grid: rustwx_core::LatLonGrid,
    values: Vec<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedNativeThermoRoute {
    pub(crate) recipe: DerivedRecipe,
    pub(crate) native_recipe: NativeDerivedRecipe,
    pub(crate) candidate: PlannedNativeDerivedCandidate,
    pub(crate) source_route: ProductSourceRoute,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedDerivedSourceRoutes {
    pub(crate) output_recipes: Vec<DerivedRecipe>,
    pub(crate) compute_recipes: Vec<DerivedRecipe>,
    pub(crate) heavy_recipes: Vec<DerivedRecipe>,
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
    Sbecape,
    Mlecape,
    Muecape,
    SbEcapeDerivedCapeRatio,
    MlEcapeDerivedCapeRatio,
    MuEcapeDerivedCapeRatio,
    SbEcapeNativeCapeRatio,
    MlEcapeNativeCapeRatio,
    MuEcapeNativeCapeRatio,
    Sbncape,
    Sbecin,
    Mlecin,
    EcapeScp,
    EcapeEhi01km,
    EcapeEhi03km,
    EcapeStp,
    ThetaE2m10mWinds,
    Vpd2m,
    DewpointDepression2m,
    Wetbulb2m,
    FireWeatherComposite,
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
            "sbecape" => Ok(Self::Sbecape),
            "mlecape" => Ok(Self::Mlecape),
            "muecape" => Ok(Self::Muecape),
            "sb_ecape_derived_cape_ratio" | "sbecape_derived_cape_ratio" => {
                Ok(Self::SbEcapeDerivedCapeRatio)
            }
            "ml_ecape_derived_cape_ratio" | "mlecape_derived_cape_ratio" => {
                Ok(Self::MlEcapeDerivedCapeRatio)
            }
            "mu_ecape_derived_cape_ratio" | "muecape_derived_cape_ratio" => {
                Ok(Self::MuEcapeDerivedCapeRatio)
            }
            "sb_ecape_native_cape_ratio" | "sbecape_native_cape_ratio" => {
                Ok(Self::SbEcapeNativeCapeRatio)
            }
            "ml_ecape_native_cape_ratio" | "mlecape_native_cape_ratio" => {
                Ok(Self::MlEcapeNativeCapeRatio)
            }
            "mu_ecape_native_cape_ratio" | "muecape_native_cape_ratio" => {
                Ok(Self::MuEcapeNativeCapeRatio)
            }
            "sbncape" => Ok(Self::Sbncape),
            "sbecin" => Ok(Self::Sbecin),
            "mlecin" => Ok(Self::Mlecin),
            "ecape_scp" => Ok(Self::EcapeScp),
            "ecape_ehi" | "ecape_ehi_0_1km" | "ecape_ehi_01km" => Ok(Self::EcapeEhi01km),
            "ecape_ehi_0_3km" | "ecape_ehi_03km" => Ok(Self::EcapeEhi03km),
            "ecape_stp" => Ok(Self::EcapeStp),
            "theta_e_2m_10m_winds" | "2m_theta_e_10m_winds" => Ok(Self::ThetaE2m10mWinds),
            "vpd_2m" | "2m_vpd" | "vapor_pressure_deficit_2m" | "2m_vapor_pressure_deficit" => {
                Ok(Self::Vpd2m)
            }
            "dewpoint_depression_2m" | "2m_dewpoint_depression" => {
                Ok(Self::DewpointDepression2m)
            }
            "wetbulb_2m" | "wet_bulb_2m" | "2m_wetbulb" | "2m_wet_bulb" => {
                Ok(Self::Wetbulb2m)
            }
            "fire_weather_composite" | "fire_weather" | "fire_wx" => {
                Ok(Self::FireWeatherComposite)
            }
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
            Self::Sbecape => "sbecape",
            Self::Mlecape => "mlecape",
            Self::Muecape => "muecape",
            Self::SbEcapeDerivedCapeRatio => "sb_ecape_derived_cape_ratio",
            Self::MlEcapeDerivedCapeRatio => "ml_ecape_derived_cape_ratio",
            Self::MuEcapeDerivedCapeRatio => "mu_ecape_derived_cape_ratio",
            Self::SbEcapeNativeCapeRatio => "sb_ecape_native_cape_ratio",
            Self::MlEcapeNativeCapeRatio => "ml_ecape_native_cape_ratio",
            Self::MuEcapeNativeCapeRatio => "mu_ecape_native_cape_ratio",
            Self::Sbncape => "sbncape",
            Self::Sbecin => "sbecin",
            Self::Mlecin => "mlecin",
            Self::EcapeScp => "ecape_scp",
            Self::EcapeEhi01km => "ecape_ehi_0_1km",
            Self::EcapeEhi03km => "ecape_ehi_0_3km",
            Self::EcapeStp => "ecape_stp",
            Self::ThetaE2m10mWinds => "theta_e_2m_10m_winds",
            Self::Vpd2m => "vpd_2m",
            Self::DewpointDepression2m => "dewpoint_depression_2m",
            Self::Wetbulb2m => "wetbulb_2m",
            Self::FireWeatherComposite => "fire_weather_composite",
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
            Self::Sbecape => "SBECAPE",
            Self::Mlecape => "MLECAPE",
            Self::Muecape => "MUECAPE",
            Self::SbEcapeDerivedCapeRatio => "SB ECAPE / Derived CAPE Ratio (EXP)",
            Self::MlEcapeDerivedCapeRatio => "ML ECAPE / Derived CAPE Ratio (EXP)",
            Self::MuEcapeDerivedCapeRatio => "MU ECAPE / Derived CAPE Ratio (EXP)",
            Self::SbEcapeNativeCapeRatio => "SB ECAPE / Native CAPE Ratio (EXP)",
            Self::MlEcapeNativeCapeRatio => "ML ECAPE / Native CAPE Ratio (EXP)",
            Self::MuEcapeNativeCapeRatio => "MU ECAPE / Native CAPE Ratio (EXP)",
            Self::Sbncape => "SBNCAPE",
            Self::Sbecin => "SBECIN",
            Self::Mlecin => "MLECIN",
            Self::EcapeScp => "ECAPE SCP (EXP)",
            Self::EcapeEhi01km => "ECAPE EHI 0-1 km (EXP)",
            Self::EcapeEhi03km => "ECAPE EHI 0-3 km (EXP)",
            Self::EcapeStp => "ECAPE STP (EXP)",
            Self::ThetaE2m10mWinds => "2 m Theta-e, 10 m Wind",
            Self::Vpd2m => "2 m Vapor Pressure Deficit",
            Self::DewpointDepression2m => "2 m Dewpoint Depression",
            Self::Wetbulb2m => "2 m Wet-Bulb Temperature",
            Self::FireWeatherComposite => "Fire Weather Composite",
            Self::ApparentTemperature2m => "2 m Apparent Temperature",
            Self::HeatIndex2m => "2 m Heat Index",
            Self::WindChill2m => "2 m Wind Chill",
            Self::LiftedIndex => "Surface-Based Lifted Index",
            Self::LapseRate700500 => "700-500 mb Virtual Temperature Lapse Rate",
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
            Self::Vpd2m
            | Self::DewpointDepression2m
            | Self::Wetbulb2m
            | Self::ApparentTemperature2m
            | Self::HeatIndex2m
            | Self::WindChill2m => ProductVisualMode::FilledMeteorology,
            _ => ProductVisualMode::SevereDiagnostic,
        }
    }

    fn is_heavy(self) -> bool {
        matches!(
            self,
            Self::Sbecape
                | Self::Mlecape
                | Self::Muecape
                | Self::SbEcapeDerivedCapeRatio
                | Self::MlEcapeDerivedCapeRatio
                | Self::MuEcapeDerivedCapeRatio
                | Self::SbEcapeNativeCapeRatio
                | Self::MlEcapeNativeCapeRatio
                | Self::MuEcapeNativeCapeRatio
                | Self::Sbncape
                | Self::Sbecin
                | Self::Mlecin
                | Self::EcapeScp
                | Self::EcapeEhi01km
                | Self::EcapeEhi03km
                | Self::EcapeStp
        )
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
    vpd_2m_hpa: Option<Vec<f64>>,
    dewpoint_depression_2m_c: Option<Vec<f64>>,
    wetbulb_2m_c: Option<Vec<f64>>,
    fire_weather_composite: Option<Vec<f64>>,
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
                DerivedRecipe::Vpd2m
                | DerivedRecipe::DewpointDepression2m
                | DerivedRecipe::Wetbulb2m
                | DerivedRecipe::FireWeatherComposite
                | DerivedRecipe::ApparentTemperature2m
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
                DerivedRecipe::Sbecape
                | DerivedRecipe::Mlecape
                | DerivedRecipe::Muecape
                | DerivedRecipe::SbEcapeDerivedCapeRatio
                | DerivedRecipe::MlEcapeDerivedCapeRatio
                | DerivedRecipe::MuEcapeDerivedCapeRatio
                | DerivedRecipe::SbEcapeNativeCapeRatio
                | DerivedRecipe::MlEcapeNativeCapeRatio
                | DerivedRecipe::MuEcapeNativeCapeRatio
                | DerivedRecipe::Sbncape
                | DerivedRecipe::Sbecin
                | DerivedRecipe::Mlecin
                | DerivedRecipe::EcapeScp
                | DerivedRecipe::EcapeEhi01km
                | DerivedRecipe::EcapeEhi03km
                | DerivedRecipe::EcapeStp => {}
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
            allow_large_heavy_domain: request.allow_large_heavy_domain,
            contour_mode: request.contour_mode,
            native_fill_level_multiplier: request.native_fill_level_multiplier,
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
            custom_poi_overlay: request.custom_poi_overlay.clone(),
            place_label_overlay: request.place_label_overlay.clone(),
        }
    }

    fn png_write_options(&self) -> PngWriteOptions {
        PngWriteOptions {
            compression: self.png_compression,
        }
    }
}

fn dataset_token_from_product(product: &str) -> Option<&str> {
    let token = product.split(['-', '_']).next().unwrap_or(product);
    if is_gdex_dataset_token(token) {
        Some(token)
    } else {
        None
    }
}

fn derived_title_for_model(model: ModelId, base_title: &str) -> String {
    if model == ModelId::WrfGdex {
        let dataset = dataset_token_from_product("d612005-hist2d").unwrap_or("d612005");
        format!("{base_title} ({dataset})")
    } else {
        base_title.to_string()
    }
}

fn derived_title_for_request(request: &DerivedBatchRequest, base_title: &str) -> String {
    if request.model != ModelId::WrfGdex {
        return base_title.to_string();
    }

    let dataset = request
        .surface_product_override
        .as_deref()
        .and_then(dataset_token_from_product)
        .or_else(|| {
            request
                .pressure_product_override
                .as_deref()
                .and_then(dataset_token_from_product)
        })
        .unwrap_or("d612005");
    format!("{base_title} ({dataset})")
}

fn is_gdex_dataset_token(token: &str) -> bool {
    token.len() > 1 && token.starts_with('d') && token[1..].chars().all(|ch| ch.is_ascii_digit())
}

pub fn supported_derived_recipe_slugs(model: ModelId) -> Vec<String> {
    match model {
        ModelId::Hrrr
        | ModelId::Gfs
        | ModelId::EcmwfOpenData
        | ModelId::RrfsA
        | ModelId::WrfGdex => supported_derived_recipe_inventory()
            .iter()
            .map(|recipe| recipe.slug.to_string())
            .collect(),
    }
}

pub fn run_derived_batch(
    request: &DerivedBatchRequest,
) -> Result<DerivedBatchReport, Box<dyn std::error::Error>> {
    let recipes = plan_derived_recipes(&request.recipe_slugs)?;
    let planned_routes = plan_native_thermo_routes_with_surface_product(
        request.model,
        &recipes,
        request.source_mode,
        request.surface_product_override.as_deref(),
    )?;
    let latest = resolve_derived_run(
        request,
        &planned_routes.compute_recipes,
        &planned_routes.heavy_recipes,
        &planned_routes.native_routes,
    )?;
    if planned_routes.output_recipes.is_empty() {
        return Ok(empty_derived_report(
            request,
            &latest,
            planned_routes.blockers,
        ));
    }
    if let Some(loaded) =
        maybe_load_rrfs_cropped_pair_for_derived(request, &latest, &planned_routes)?
    {
        return run_derived_batch_from_loaded_bundles(request, &recipes, &loaded);
    }
    let plan = build_derived_execution_plan(
        &latest,
        request.forecast_hour,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
        !planned_routes.compute_recipes.is_empty() || !planned_routes.heavy_recipes.is_empty(),
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

fn maybe_load_rrfs_cropped_pair_for_derived(
    request: &DerivedBatchRequest,
    latest: &rustwx_models::LatestRun,
    planned_routes: &PlannedDerivedSourceRoutes,
) -> Result<Option<LoadedBundleSet>, Box<dyn std::error::Error>> {
    if request.model != ModelId::RrfsA
        || planned_routes.compute_recipes.is_empty()
        || !planned_routes.native_routes.is_empty()
    {
        return Ok(None);
    }

    let plan = build_derived_execution_plan(
        latest,
        request.forecast_hour,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
        true,
        &[],
    );
    let surface_planned = plan
        .bundle_for(
            CanonicalBundleDescriptor::SurfaceAnalysis,
            request.forecast_hour,
        )
        .ok_or("rrfs derived crop path missing surface bundle")?;
    let pressure_planned = plan
        .bundle_for(
            CanonicalBundleDescriptor::PressureAnalysis,
            request.forecast_hour,
        )
        .ok_or("rrfs derived crop path missing pressure bundle")?;

    let surface_fetch_start = Instant::now();
    let mut surface_file = fetch_family_file(
        request.model,
        latest.cycle.clone(),
        request.forecast_hour,
        latest.source,
        &surface_planned.resolved,
        &request.cache_root,
        request.use_cache,
    )?;
    let fetch_surface_ms = surface_fetch_start.elapsed().as_millis();

    let pressure_fetch_start = Instant::now();
    let mut pressure_file = fetch_family_file(
        request.model,
        latest.cycle.clone(),
        request.forecast_hour,
        latest.source,
        &pressure_planned.resolved,
        &request.cache_root,
        request.use_cache,
    )?;
    let fetch_pressure_ms = pressure_fetch_start.elapsed().as_millis();

    let surface_grid = decode_surface_grid(surface_file.bytes.as_slice())?;
    let projected = build_projected_map_with_projection(
        &surface_grid
            .lat
            .iter()
            .copied()
            .map(|value| value as f32)
            .collect::<Vec<_>>(),
        &surface_grid
            .lon
            .iter()
            .copied()
            .map(|value| value as f32)
            .collect::<Vec<_>>(),
        surface_grid.projection.as_ref(),
        request.domain.bounds,
        map_frame_aspect_ratio(request.output_width, request.output_height, true, true),
    )?;

    let crop = match classify_projected_grid_intersection(
        surface_grid.nx,
        surface_grid.ny,
        &projected.projected_x,
        &projected.projected_y,
        &projected.extent,
        2,
    )? {
        ProjectedGridIntersection::Empty => {
            return Err(format!(
                "rrfs derived projected crop for domain '{}' produced an empty domain",
                request.domain.slug
            )
            .into());
        }
        ProjectedGridIntersection::Full => return Ok(None),
        ProjectedGridIntersection::Crop(crop) => crop,
    };

    let surface_decode_start = Instant::now();
    let surface_decode = load_or_decode_surface_cropped(
        &cropped_decode_cache_path(&request.cache_root, &surface_file.request, "surface", crop),
        surface_file.bytes.as_slice(),
        request.use_cache,
        crop,
    )?;
    let decode_surface_ms = surface_decode_start.elapsed().as_millis();

    let pressure_decode_start = Instant::now();
    let (pressure_decode, pressure_shape) = load_or_decode_pressure_cropped_with_shape(
        &cropped_decode_cache_path(
            &request.cache_root,
            &pressure_file.request,
            "pressure",
            crop,
        ),
        pressure_file.bytes.as_slice(),
        request.use_cache,
        crop,
    )?;
    let decode_pressure_ms = pressure_decode_start.elapsed().as_millis();

    crate::gridded::validate_pressure_decode_against_surface(
        &pressure_decode,
        pressure_shape,
        surface_decode.value.nx,
        surface_decode.value.ny,
    )?;

    surface_file.bytes.clear();
    surface_file.bytes.shrink_to_fit();
    pressure_file.bytes.clear();
    pressure_file.bytes.shrink_to_fit();
    let surface_fetch_bytes_len = surface_file.fetched.result.bytes.len();
    let pressure_fetch_bytes_len = pressure_file.fetched.result.bytes.len();

    let mut fetched = BTreeMap::new();
    fetched.insert(
        surface_planned.fetch_key(),
        FetchedBundleBytes {
            key: surface_planned.fetch_key(),
            file: surface_file,
            fetch_ms: fetch_surface_ms,
        },
    );
    fetched.insert(
        pressure_planned.fetch_key(),
        FetchedBundleBytes {
            key: pressure_planned.fetch_key(),
            file: pressure_file,
            fetch_ms: fetch_pressure_ms,
        },
    );

    let mut surface_decodes = BTreeMap::new();
    surface_decodes.insert(surface_planned.id.clone(), surface_decode);
    let mut pressure_decodes = BTreeMap::new();
    pressure_decodes.insert(pressure_planned.id.clone(), pressure_decode);

    Ok(Some(LoadedBundleSet {
        plan,
        latest: latest.clone(),
        forecast_hour: request.forecast_hour,
        fetched,
        fetch_failures: BTreeMap::new(),
        surface_decodes,
        pressure_decodes,
        bundle_failures: BTreeMap::new(),
        timing: LoadedBundleTiming {
            fetch_ms_total: fetch_surface_ms + fetch_pressure_ms,
            decode_surface_ms_total: decode_surface_ms,
            decode_pressure_ms_total: decode_pressure_ms,
            cropped_decode_profile: Some(CroppedDecodeProfile {
                source_grid_nx: surface_grid.nx,
                source_grid_ny: surface_grid.ny,
                crop_x_start: crop.x_start,
                crop_x_end: crop.x_end,
                crop_y_start: crop.y_start,
                crop_y_end: crop.y_end,
                cropped_grid_nx: crop.width(),
                cropped_grid_ny: crop.height(),
                surface_fetch_bytes_len,
                pressure_fetch_bytes_len,
            }),
        },
    }))
}

pub(crate) fn maybe_load_special_pair_for_derived(
    request: &DerivedBatchRequest,
    latest: &rustwx_models::LatestRun,
    planned_routes: &PlannedDerivedSourceRoutes,
) -> Result<Option<LoadedBundleSet>, Box<dyn std::error::Error>> {
    maybe_load_rrfs_cropped_pair_for_derived(request, latest, planned_routes)
}

fn cropped_decode_cache_path(
    cache_root: &std::path::Path,
    fetch: &rustwx_io::FetchRequest,
    name: &str,
    crop: crate::gridded::GridCrop,
) -> PathBuf {
    let mut path = decode_cache_path(cache_root, fetch, name);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name)
        .to_string();
    let suffix = format!(
        "{stem}_crop_{}_{}_{}_{}",
        crop.x_start, crop.x_end, crop.y_start, crop.y_end
    );
    path.set_file_name(format!("{suffix}.bin"));
    path
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
    let planned_routes = plan_native_thermo_routes_with_surface_product(
        request.model,
        recipes,
        request.source_mode,
        request.surface_product_override.as_deref(),
    )?;
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
    let mut heavy_timing = None;
    let mut memory_profile = None;
    let mut grid: Option<rustwx_core::LatLonGrid> = None;
    let mut grid_projection: Option<rustwx_core::GridProjection> = None;
    let mut projected: Option<ProjectedMap> = None;
    let input_fetches = build_planned_input_fetches(loaded);
    let input_fetch_keys = unique_input_fetch_keys(&input_fetches);
    let date_yyyymmdd = request.date_yyyymmdd.as_str();
    let cycle_utc = loaded.latest.cycle.hour_utc;
    let forecast_hour = request.forecast_hour;
    let source = loaded.latest.source;
    let model = request.model;
    let mut rendered_by_recipe = HashMap::<DerivedRecipe, DerivedRenderedRecipe>::new();
    let needs_pair =
        !planned_routes.compute_recipes.is_empty() || !planned_routes.heavy_recipes.is_empty();

    if needs_pair {
        let (surface_planned, surface_decode, pressure_planned, pressure_decode) = loaded
            .require_surface_pressure_pair()
            .map_err(|err| format!("derived surface/pressure pair unavailable: {err}"))?;
        let full_surface = &surface_decode.value;
        let full_pressure = &pressure_decode.value;
        if !planned_routes.compute_recipes.is_empty() {
            memory_profile = build_derived_memory_profile(
                request.model,
                &planned_routes.compute_recipes,
                full_surface,
                full_pressure,
                loaded.timing.cropped_decode_profile,
            );
        }
        let owned_full_grid = full_surface.core_grid()?;
        let project_start = Instant::now();
        let full_projected = build_projected_map_with_projection(
            &owned_full_grid.lat_deg,
            &owned_full_grid.lon_deg,
            full_surface.projection.as_ref(),
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
                        grid_projection = shared.projection.clone();
                        projected = Some(full_projected.clone());
                        computed = shared.computed.clone();
                    }
                    ProjectedGridIntersection::Crop(crop) => {
                        let derived_grid = crop_latlon_grid(&shared.grid, crop)?;
                        let derived_projected = build_projected_map_with_projection(
                            &derived_grid.lat_deg,
                            &derived_grid.lon_deg,
                            full_surface.projection.as_ref(),
                            request.domain.bounds,
                            map_frame_aspect_ratio(
                                request.output_width,
                                request.output_height,
                                true,
                                true,
                            ),
                        )?;
                        grid = Some(derived_grid);
                        grid_projection = shared.projection.clone();
                        projected = Some(derived_projected);
                        computed =
                            crop_computed_fields(&shared.computed, shared.grid.shape.nx, crop);
                    }
                }
                fetch_decode = shared.fetch_decode.clone();
            }
            None => {
                if !planned_routes.compute_recipes.is_empty() {
                    let cropped = crate::gridded::crop_heavy_domain_for_projected_extent(
                        full_surface,
                        full_pressure,
                        &full_projected.projected_x,
                        &full_projected.projected_y,
                        &full_projected.extent,
                        2,
                    )?;
                    let (surface, pressure, derived_grid) = match cropped.as_ref() {
                        Some(cropped) => {
                            (&cropped.surface, &cropped.pressure, cropped.grid.clone())
                        }
                        None => (full_surface, full_pressure, owned_full_grid.clone()),
                    };

                    let derived_projected = if cropped.is_some() {
                        build_projected_map_with_projection(
                            &derived_grid.lat_deg,
                            &derived_grid.lon_deg,
                            surface.projection.as_ref(),
                            request.domain.bounds,
                            map_frame_aspect_ratio(
                                request.output_width,
                                request.output_height,
                                true,
                                true,
                            ),
                        )?
                    } else {
                        full_projected.clone()
                    };

                    let compute_start = Instant::now();
                    computed = compute_derived_fields_generic(
                        surface,
                        pressure,
                        &planned_routes.compute_recipes,
                    )?;
                    compute_ms += compute_start.elapsed().as_millis();
                    grid = Some(derived_grid);
                    grid_projection = surface.projection.clone();
                    projected = Some(derived_projected);
                }
                fetch_decode = Some(build_shared_timing_for_pair(
                    loaded,
                    surface_planned,
                    pressure_planned,
                )?);
            }
        }
        if !planned_routes.heavy_recipes.is_empty() {
            let (heavy_rendered, timing) = render_derived_heavy_recipes(
                request,
                &planned_routes.heavy_recipes,
                full_surface,
                full_pressure,
                &owned_full_grid,
                &full_projected,
                date_yyyymmdd,
                cycle_utc,
                forecast_hour,
                source,
                model,
                input_fetch_keys.clone(),
            )?;
            heavy_timing = Some(timing);
            for recipe in heavy_rendered {
                let parsed = DerivedRecipe::parse(&recipe.recipe_slug).map_err(io::Error::other)?;
                rendered_by_recipe.insert(parsed, recipe);
            }
        }
        project_ms += project_start.elapsed().as_millis();
    }

    let computed = &computed;
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
            extract_native_derived_field(request.model, route.native_recipe, fetched)?.ok_or_else(
                || {
                    format!(
                        "native derived field '{}' not found in {}",
                        route.recipe.slug(),
                        route.candidate.fetch_product
                    )
                },
            )?;
        let native_field = crop_native_derived_field(&native_field, request.domain.bounds)?;
        native_extract_ms += extract_start.elapsed().as_millis();

        let needs_native_projection = projected
            .as_ref()
            .map(|existing| existing.projected_x.len() != native_field.grid.shape.len())
            .unwrap_or(true);
        let native_projected = if needs_native_projection {
            let project_start = Instant::now();
            let native_projected = build_projected_map(
                &native_field.grid.lat_deg,
                &native_field.grid.lon_deg,
                request.domain.bounds,
                map_frame_aspect_ratio(request.output_width, request.output_height, true, true),
            )?;
            project_ms += project_start.elapsed().as_millis();
            if grid.is_none() {
                grid = Some(native_field.grid.clone());
                projected = Some(native_projected.clone());
            }
            native_projected
        } else {
            projected
                .as_ref()
                .ok_or("native thermo projection missing during main render")?
                .clone()
        };
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
            &native_projected,
            date_yyyymmdd,
            cycle_utc,
            forecast_hour,
            source,
            model,
            request.output_width,
            request.output_height,
            native_field.values.clone(),
            request.contour_mode,
            request.native_fill_level_multiplier,
        )?;
        let HrrrDerivedLiveArtifact {
            recipe_slug,
            title: _,
            field: _,
            request: mut render_request,
        } = render_artifact;
        let title = derived_title_for_request(request, route.recipe.title());
        render_request.title = Some(title.clone());
        if let Some(overlay) = request.custom_poi_overlay.as_ref() {
            apply_custom_poi_overlay(
                &mut render_request,
                overlay,
                request.domain.bounds,
                &native_field.grid.lat_deg,
                &native_field.grid.lon_deg,
                None,
            )?;
        }
        if let Some(overlay) = request.place_label_overlay.as_ref() {
            crate::apply_place_label_overlay_with_density_styling(
                &mut render_request,
                overlay,
                &request.domain,
                &native_field.grid.lat_deg,
                &native_field.grid.lon_deg,
                None,
            )?;
        }
        let save_timing = save_png_profile_with_options(
            &render_request,
            &output_path,
            &request.png_write_options(),
        )?;
        let render_ms = render_start.elapsed().as_millis();
        let content_identity = artifact_identity_from_path(&output_path)?;
        rendered_by_recipe.insert(
            route.recipe,
            DerivedRenderedRecipe {
                recipe_slug,
                title,
                source_route: route.source_route,
                output_path,
                content_identity,
                input_fetch_keys: input_fetch_keys.clone(),
                timing: DerivedRecipeTiming {
                    render_to_image_ms: save_timing.png_timing.render_to_image_ms,
                    data_layer_draw_ms: derived_data_layer_draw_ms(
                        &save_timing.png_timing.image_timing,
                    ),
                    overlay_draw_ms: derived_overlay_draw_ms(&save_timing.png_timing.image_timing),
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
        let projection_ref = grid_projection.as_ref();
        let projected_ref = projected
            .as_ref()
            .ok_or("derived render requested but no projection was prepared")?;
        let rendered = if render_parallelism <= 1 {
            let mut rendered = Vec::with_capacity(derived_output_recipes.len());
            for recipe in derived_output_recipes.iter().copied() {
                rendered.push(render_derived_output_recipe(
                    request,
                    recipe,
                    grid_ref,
                    projection_ref,
                    projected_ref,
                    date_yyyymmdd,
                    cycle_utc,
                    forecast_hour,
                    source,
                    model,
                    computed,
                    input_fetch_keys.clone(),
                )?);
            }
            rendered
        } else {
            thread::scope(|scope| -> Result<Vec<DerivedRenderedRecipe>, io::Error> {
                let mut rendered = Vec::with_capacity(derived_output_recipes.len());
                let mut pending = VecDeque::new();

                for recipe in derived_output_recipes.iter().copied() {
                    let lane_fetch_keys = input_fetch_keys.clone();
                    let lane_projection = grid_projection.clone();
                    pending.push_back(scope.spawn(move || {
                        render_derived_output_recipe(
                            request,
                            recipe,
                            grid_ref,
                            lane_projection.as_ref(),
                            projected_ref,
                            date_yyyymmdd,
                            cycle_utc,
                            forecast_hour,
                            source,
                            model,
                            computed,
                            lane_fetch_keys,
                        )
                    }));

                    if pending.len() >= render_parallelism {
                        rendered.push(join_render_job(pending.pop_front().unwrap())?);
                    }
                }

                while let Some(handle) = pending.pop_front() {
                    rendered.push(join_render_job(handle)?);
                }

                Ok(rendered)
            })
            .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?
        };
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
            memory_profile,
            heavy_timing,
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
pub(crate) fn run_model_derived_batch_from_loaded(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let report = run_derived_batch_from_loaded_bundles(request, recipes, loaded)?;
    Ok(into_hrrr_report(report))
}

pub(crate) fn run_model_derived_batch_from_loaded_with_precomputed(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
    prepared: &PreparedSharedDerivedFields,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let mut report = run_derived_batch_from_loaded_bundles_with_precomputed(
        request,
        recipes,
        loaded,
        Some(prepared),
    )?;
    report.shared_timing.compute_ms = 0;
    Ok(into_hrrr_report(report))
}

pub(crate) fn run_model_derived_batch_without_loaded(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    latest: &rustwx_models::LatestRun,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let planned_routes = plan_native_thermo_routes_with_surface_product(
        request.model,
        recipes,
        request.source_mode,
        request.surface_product_override.as_deref(),
    )?;
    let report = empty_derived_report(request, latest, planned_routes.blockers);
    Ok(into_hrrr_report(report))
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
    run_model_derived_batch_from_loaded(&generic_request, recipes, loaded)
}

pub(crate) fn prepare_shared_derived_fields(
    request: &DerivedBatchRequest,
    recipes: &[DerivedRecipe],
    loaded: &LoadedBundleSet,
) -> Result<Option<PreparedSharedDerivedFields>, Box<dyn std::error::Error>> {
    let planned_routes = plan_native_thermo_routes_with_surface_product(
        request.model,
        recipes,
        request.source_mode,
        request.surface_product_override.as_deref(),
    )?;
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
        projection: surface_decode.value.projection.clone(),
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
    run_model_derived_batch_from_loaded_with_precomputed(
        &generic_request,
        recipes,
        loaded,
        prepared,
    )
}

pub(crate) fn run_hrrr_derived_batch_without_loaded(
    request: &HrrrDerivedBatchRequest,
    recipes: &[DerivedRecipe],
    latest: &rustwx_models::LatestRun,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let generic_request = DerivedBatchRequest::from_hrrr(request);
    run_model_derived_batch_without_loaded(&generic_request, recipes, latest)
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

fn build_derived_memory_profile(
    model: ModelId,
    compute_recipes: &[DerivedRecipe],
    surface: &GenericSurfaceFields,
    pressure: &GenericPressureFields,
    cropped_profile: Option<CroppedDecodeProfile>,
) -> Option<DerivedMemoryProfile> {
    if model != ModelId::RrfsA {
        return None;
    }
    let cropped = cropped_profile?;
    let requirements = DerivedRequirements::from_recipes(compute_recipes);
    let pressure_level_count = pressure.pressure_levels_hpa.len();
    let thermo_volume_points = surface.nx * surface.ny * pressure_level_count;
    let canonical_pressure_3d_pa_bytes_estimate = if requirements.needs_volume() {
        thermo_volume_points * std::mem::size_of::<f64>()
    } else {
        0
    };
    let canonical_height_agl_3d_bytes_estimate = if requirements.needs_height_agl() {
        thermo_volume_points * std::mem::size_of::<f64>()
    } else {
        0
    };
    Some(DerivedMemoryProfile {
        source_grid_nx: cropped.source_grid_nx,
        source_grid_ny: cropped.source_grid_ny,
        cropped_grid_nx: cropped.cropped_grid_nx,
        cropped_grid_ny: cropped.cropped_grid_ny,
        crop_x_start: cropped.crop_x_start,
        crop_x_end: cropped.crop_x_end,
        crop_y_start: cropped.crop_y_start,
        crop_y_end: cropped.crop_y_end,
        surface_fetch_bytes_len: cropped.surface_fetch_bytes_len,
        pressure_fetch_bytes_len: cropped.pressure_fetch_bytes_len,
        cropped_surface_decoded_bytes_estimate: surface.decoded_bytes_estimate(),
        cropped_pressure_decoded_bytes_estimate: pressure.decoded_bytes_estimate(),
        cropped_decoded_total_bytes_estimate: surface.decoded_bytes_estimate()
            + pressure.decoded_bytes_estimate(),
        pressure_level_count,
        thermo_volume_points,
        compute_recipe_count: compute_recipes.len(),
        needs_volume: requirements.needs_volume(),
        needs_height_agl: requirements.needs_height_agl(),
        canonical_pressure_3d_pa_bytes_estimate,
        canonical_height_agl_3d_bytes_estimate,
        canonical_shared_volume_work_bytes_estimate: canonical_pressure_3d_pa_bytes_estimate
            + canonical_height_agl_3d_bytes_estimate,
    })
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
            memory_profile: None,
            heavy_timing: None,
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

fn extract_native_derived_field(
    model: ModelId,
    native_recipe: NativeDerivedRecipe,
    fetched: &FetchedBundleBytes,
) -> Result<Option<NativeDerivedField>, Box<dyn std::error::Error>> {
    match native_recipe {
        NativeDerivedRecipe::Thermo(recipe) => {
            let Some(field) = extract_native_thermo_field(model, recipe, &fetched.file.bytes)?
            else {
                return Ok(None);
            };
            Ok(Some(NativeDerivedField {
                grid: field.grid,
                values: field.values,
            }))
        }
        #[cfg(feature = "wrf")]
        NativeDerivedRecipe::WrfGdexScalar { variable } => {
            if model != ModelId::WrfGdex {
                return Ok(None);
            }
            let file = open_wrf_gdex_native_file(fetched)?;
            let grid = wrf_latlon_grid(&file)?;
            let values = file.read_var(variable)?;
            validate_native_wrf_values(variable, file.nxy(), &values)?;
            Ok(Some(NativeDerivedField { grid, values }))
        }
        #[cfg(not(feature = "wrf"))]
        NativeDerivedRecipe::WrfGdexScalar { .. } => {
            if model == ModelId::WrfGdex {
                return Err(
                    "WRF/GDEX NetCDF support is not compiled; rebuild with --features wrf".into(),
                );
            }
            Ok(None)
        }
        #[cfg(feature = "wrf")]
        NativeDerivedRecipe::WrfGdexVectorMagnitude {
            u_variable,
            v_variable,
            scale,
        } => {
            if model != ModelId::WrfGdex {
                return Ok(None);
            }
            let file = open_wrf_gdex_native_file(fetched)?;
            let grid = wrf_latlon_grid(&file)?;
            let u = file.read_var(u_variable)?;
            let v = file.read_var(v_variable)?;
            validate_native_wrf_values(u_variable, file.nxy(), &u)?;
            validate_native_wrf_values(v_variable, file.nxy(), &v)?;
            let values = u
                .iter()
                .zip(v.iter())
                .map(|(u, v)| u.hypot(*v) * scale)
                .collect();
            Ok(Some(NativeDerivedField { grid, values }))
        }
        #[cfg(not(feature = "wrf"))]
        NativeDerivedRecipe::WrfGdexVectorMagnitude { .. } => {
            if model == ModelId::WrfGdex {
                return Err(
                    "WRF/GDEX NetCDF support is not compiled; rebuild with --features wrf".into(),
                );
            }
            Ok(None)
        }
    }
}

#[cfg(feature = "wrf")]
fn open_wrf_gdex_native_file(
    fetched: &FetchedBundleBytes,
) -> Result<WrfFile, Box<dyn std::error::Error>> {
    let cached_path = fetched.file.fetched.bytes_path.as_path();
    if cached_path.exists() {
        return Ok(WrfFile::open(cached_path)?);
    }
    if !looks_like_wrf(&fetched.file.bytes) {
        return Err("WRF/GDEX native fetch was not a NetCDF/HDF5 payload".into());
    }
    let materialized = materialize_wrf_native_bytes(&fetched.file.bytes)?;
    Ok(WrfFile::open(&materialized)?)
}

#[cfg(feature = "wrf")]
fn materialize_wrf_native_bytes(bytes: &[u8]) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    let hash = hasher.finish();
    let path = std::env::temp_dir().join(format!("rustwx-products-wrf-native-{hash:016x}.nc"));
    if !path.exists() {
        fs::write(&path, bytes)?;
    }
    Ok(path)
}

#[cfg(feature = "wrf")]
fn wrf_latlon_grid(file: &WrfFile) -> Result<rustwx_core::LatLonGrid, Box<dyn std::error::Error>> {
    Ok(rustwx_core::LatLonGrid::new(
        rustwx_core::GridShape::new(file.nx, file.ny)?,
        file.lat()?.iter().map(|value| *value as f32).collect(),
        file.lon()?.iter().map(|value| *value as f32).collect(),
    )?)
}

#[cfg(feature = "wrf")]
fn validate_native_wrf_values(
    variable: &str,
    expected_len: usize,
    values: &[f64],
) -> Result<(), Box<dyn std::error::Error>> {
    if values.len() != expected_len {
        return Err(format!(
            "WRF/GDEX native variable '{variable}' length mismatch: expected {expected_len}, got {}",
            values.len()
        )
        .into());
    }
    Ok(())
}

fn crop_native_derived_field(
    field: &NativeDerivedField,
    bounds: (f64, f64, f64, f64),
) -> Result<NativeDerivedField, Box<dyn std::error::Error>> {
    let nx = field.grid.shape.nx;
    let ny = field.grid.shape.ny;
    let mut min_x = nx;
    let mut max_x = 0usize;
    let mut min_y = ny;
    let mut max_y = 0usize;
    let mut found = false;

    for y in 0..ny {
        let row_offset = y * nx;
        for x in 0..nx {
            let idx = row_offset + x;
            let lat = f64::from(field.grid.lat_deg[idx]);
            let lon = f64::from(field.grid.lon_deg[idx]);
            if point_in_geographic_bounds(lon, lat, bounds) {
                min_x = min_x.min(x);
                max_x = max_x.max(x);
                min_y = min_y.min(y);
                max_y = max_y.max(y);
                found = true;
            }
        }
    }

    if !found {
        return Err("requested native derived crop produced an empty domain".into());
    }

    if min_x == 0 && max_x + 1 == nx && min_y == 0 && max_y + 1 == ny {
        return Ok(field.clone());
    }

    let crop = GridCrop {
        x_start: min_x,
        x_end: max_x + 1,
        y_start: min_y,
        y_end: max_y + 1,
    };

    Ok(NativeDerivedField {
        grid: crop_latlon_grid(&field.grid, crop)?,
        values: crop_values_f64(&field.values, field.grid.shape.nx, crop),
    })
}

fn point_in_geographic_bounds(lon: f64, lat: f64, bounds: (f64, f64, f64, f64)) -> bool {
    if !lon.is_finite() || !lat.is_finite() || lat < bounds.2 || lat > bounds.3 {
        return false;
    }
    let west = normalize_longitude_for_bounds(bounds.0);
    let east = normalize_longitude_for_bounds(bounds.1);
    let lon = normalize_longitude_for_bounds(lon);
    if west <= east {
        lon >= west && lon <= east
    } else {
        lon >= west || lon <= east
    }
}

fn normalize_longitude_for_bounds(lon: f64) -> f64 {
    let mut lon = lon % 360.0;
    if lon > 180.0 {
        lon -= 360.0;
    } else if lon <= -180.0 {
        lon += 360.0;
    }
    lon
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
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let computed = computed_from_native_values(recipe, values)?;
    build_render_artifact(
        recipe,
        grid,
        projected,
        date_yyyymmdd,
        cycle_utc,
        forecast_hour,
        source,
        model,
        output_width,
        output_height,
        &computed,
        contour_mode,
        native_fill_level_multiplier,
    )
}

fn computed_from_native_values(
    recipe: DerivedRecipe,
    values: Vec<f64>,
) -> Result<DerivedComputedFields, Box<dyn std::error::Error>> {
    let mut computed = DerivedComputedFields::default();
    match recipe {
        DerivedRecipe::Sbcape => computed.sbcape_jkg = Some(values),
        DerivedRecipe::Sbcin => computed.sbcin_jkg = Some(values),
        DerivedRecipe::Sblcl => computed.sblcl_m = Some(values),
        DerivedRecipe::Mlcape => computed.mlcape_jkg = Some(values),
        DerivedRecipe::Mlcin => computed.mlcin_jkg = Some(values),
        DerivedRecipe::Mucape => computed.mucape_jkg = Some(values),
        DerivedRecipe::Mucin => computed.mucin_jkg = Some(values),
        DerivedRecipe::LiftedIndex => computed.lifted_index_c = Some(values),
        DerivedRecipe::BulkShear01km => computed.shear_01km_kt = Some(values),
        DerivedRecipe::BulkShear06km => computed.shear_06km_kt = Some(values),
        DerivedRecipe::Srh01km => computed.srh_01km_m2s2 = Some(values),
        DerivedRecipe::Srh03km => computed.srh_03km_m2s2 = Some(values),
        _ => {
            return Err(format!(
                "recipe '{}' does not support native derived rendering",
                recipe.slug()
            )
            .into());
        }
    }
    Ok(computed)
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
    domain_bounds: (f64, f64, f64, f64),
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    build_hrrr_live_derived_artifact_with_render_mode(
        recipe_slug,
        surface,
        pressure,
        grid,
        projected,
        domain_bounds,
        date_yyyymmdd,
        cycle_utc,
        forecast_hour,
        source,
        NativeContourRenderMode::Automatic,
        1,
    )
}

pub fn build_hrrr_live_derived_artifact_with_render_mode(
    recipe_slug: &str,
    surface: &GenericSurfaceFields,
    pressure: &GenericPressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    domain_bounds: (f64, f64, f64, f64),
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let recipe =
        DerivedRecipe::parse(recipe_slug).map_err(|err| format!("{recipe_slug}: {err}"))?;
    with_prepared_live_derived_domain(
        surface,
        pressure,
        grid,
        projected,
        domain_bounds,
        OUTPUT_WIDTH,
        OUTPUT_HEIGHT,
        |surface, pressure, grid, projected| {
            let computed = compute_derived_fields_generic(surface, pressure, &[recipe])?;
            build_render_artifact_with_contour_mode(
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
                contour_mode,
                native_fill_level_multiplier,
            )
        },
    )
}

pub fn build_hrrr_live_derived_artifact_profiled(
    recipe_slug: &str,
    surface: &GenericSurfaceFields,
    pressure: &GenericPressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    domain_bounds: (f64, f64, f64, f64),
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    contour_mode: NativeContourRenderMode,
) -> Result<ProfiledHrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let recipe =
        DerivedRecipe::parse(recipe_slug).map_err(|err| format!("{recipe_slug}: {err}"))?;
    with_prepared_live_derived_domain(
        surface,
        pressure,
        grid,
        projected,
        domain_bounds,
        OUTPUT_WIDTH,
        OUTPUT_HEIGHT,
        |surface, pressure, grid, projected| {
            let compute_start = Instant::now();
            let computed = compute_derived_fields_generic(surface, pressure, &[recipe])?;
            let compute_fields_ms = compute_start.elapsed().as_millis();
            let (artifact, mut timing) = build_render_artifact_with_contour_mode_profiled(
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
                contour_mode,
                1,
            )?;
            timing.compute_fields_ms = compute_fields_ms;
            timing.total_ms = total_start.elapsed().as_millis();
            Ok(ProfiledHrrrDerivedLiveArtifact { artifact, timing })
        },
    )
}

fn with_prepared_live_derived_domain<T>(
    surface: &GenericSurfaceFields,
    pressure: &GenericPressureFields,
    grid: &rustwx_core::LatLonGrid,
    projected: &ProjectedMap,
    domain_bounds: (f64, f64, f64, f64),
    output_width: u32,
    output_height: u32,
    build: impl FnOnce(
        &GenericSurfaceFields,
        &GenericPressureFields,
        &rustwx_core::LatLonGrid,
        &ProjectedMap,
    ) -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let cropped = crate::gridded::crop_heavy_domain_for_projected_extent(
        surface,
        pressure,
        &projected.projected_x,
        &projected.projected_y,
        &projected.extent,
        2,
    )?;
    if let Some(cropped) = cropped {
        let cropped_projected = build_projected_map_with_projection(
            &cropped.grid.lat_deg,
            &cropped.grid.lon_deg,
            cropped.surface.projection.as_ref(),
            domain_bounds,
            map_frame_aspect_ratio(output_width, output_height, true, true),
        )?;
        build(
            &cropped.surface,
            &cropped.pressure,
            &cropped.grid,
            &cropped_projected,
        )
    } else {
        build(surface, pressure, grid, projected)
    }
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

fn planned_candidate_from_native(
    model: ModelId,
    recipe: DerivedRecipe,
    surface_product_override: Option<&str>,
) -> Option<(NativeDerivedRecipe, PlannedNativeDerivedCandidate)> {
    if model == ModelId::WrfGdex {
        if let Some(candidate) = wrf_gdex_native_candidate(recipe, surface_product_override) {
            return Some(candidate);
        }
    }

    let native_recipe = native_recipe_for_derived(recipe)?;
    let candidate = native_candidate(model, native_recipe)?;
    Some((
        NativeDerivedRecipe::Thermo(native_recipe),
        PlannedNativeDerivedCandidate {
            label: candidate.label.to_string(),
            semantics: candidate.semantics,
            auto_eligible: candidate.auto_eligible,
            detail: candidate.detail.to_string(),
            fetch_product: candidate.fetch_product,
        },
    ))
}

fn wrf_gdex_native_candidate(
    recipe: DerivedRecipe,
    surface_product_override: Option<&str>,
) -> Option<(NativeDerivedRecipe, PlannedNativeDerivedCandidate)> {
    let fetch_product = resolve_canonical_bundle_product(
        ModelId::WrfGdex,
        CanonicalBundleDescriptor::SurfaceAnalysis,
        surface_product_override,
    )
    .native_product;
    if !wrf_gdex_native_surface_product(&fetch_product) {
        return None;
    }
    let fetch_product = leak_static_str(fetch_product);

    let (native_recipe, label, detail) = match recipe {
        DerivedRecipe::Sbcape => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "SBCAPE" },
            "surface CAPE",
            "WRF/GDEX native SBCAPE from model diagnostics",
        ),
        DerivedRecipe::Sbcin => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "SBCINH" },
            "surface CIN",
            "WRF/GDEX native SBCINH from model diagnostics",
        ),
        DerivedRecipe::Sblcl => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "SBLCL" },
            "surface LCL height",
            "WRF/GDEX native SBLCL from model diagnostics",
        ),
        DerivedRecipe::Mlcape => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "MLCAPE" },
            "mixed-layer CAPE",
            "WRF/GDEX native MLCAPE from model diagnostics",
        ),
        DerivedRecipe::Mlcin => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "MLCINH" },
            "mixed-layer CIN",
            "WRF/GDEX native MLCINH from model diagnostics",
        ),
        DerivedRecipe::Mucape => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "MUCAPE" },
            "most-unstable CAPE",
            "WRF/GDEX native MUCAPE from model diagnostics",
        ),
        DerivedRecipe::Mucin => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "MUCINH" },
            "most-unstable CIN",
            "WRF/GDEX native MUCINH from model diagnostics",
        ),
        DerivedRecipe::Srh01km => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "SRH01" },
            "0-1 km SRH",
            "WRF/GDEX native SRH01 from model diagnostics",
        ),
        DerivedRecipe::Srh03km => (
            NativeDerivedRecipe::WrfGdexScalar { variable: "SRH03" },
            "0-3 km SRH",
            "WRF/GDEX native SRH03 from model diagnostics",
        ),
        DerivedRecipe::BulkShear01km => (
            NativeDerivedRecipe::WrfGdexVectorMagnitude {
                u_variable: "USHR1",
                v_variable: "VSHR1",
                scale: KNOTS_PER_MS,
            },
            "0-1 km bulk shear",
            "WRF/GDEX native 0-1 km shear magnitude from model diagnostics",
        ),
        DerivedRecipe::BulkShear06km => (
            NativeDerivedRecipe::WrfGdexVectorMagnitude {
                u_variable: "USHR6",
                v_variable: "VSHR6",
                scale: KNOTS_PER_MS,
            },
            "0-6 km bulk shear",
            "WRF/GDEX native 0-6 km shear magnitude from model diagnostics",
        ),
        _ => return None,
    };

    Some((
        native_recipe,
        PlannedNativeDerivedCandidate {
            label: label.to_string(),
            semantics: NativeSemantics::ExactEquivalent,
            auto_eligible: true,
            detail: detail.to_string(),
            fetch_product,
        },
    ))
}

fn wrf_gdex_native_surface_product(product: &str) -> bool {
    let normalized = product.replace('_', "-").to_ascii_lowercase();
    let Some((dataset, suffix)) = normalized.split_once('-') else {
        return false;
    };
    is_gdex_dataset_token(dataset)
        && (matches!(suffix, "hist2d" | "future2d")
            || (suffix.starts_with('d')
                && suffix.len() == 3
                && suffix[1..].chars().all(|ch| ch.is_ascii_digit())))
}

pub(crate) fn plan_native_thermo_routes(
    model: ModelId,
    recipes: &[DerivedRecipe],
    mode: ProductSourceMode,
) -> Result<PlannedDerivedSourceRoutes, Box<dyn std::error::Error>> {
    plan_native_thermo_routes_with_surface_product(model, recipes, mode, None)
}

pub(crate) fn plan_native_thermo_routes_with_surface_product(
    model: ModelId,
    recipes: &[DerivedRecipe],
    mode: ProductSourceMode,
    surface_product_override: Option<&str>,
) -> Result<PlannedDerivedSourceRoutes, Box<dyn std::error::Error>> {
    let mut output_recipes = Vec::new();
    let mut compute_recipes = Vec::new();
    let mut heavy_recipes = Vec::new();
    let mut native_routes = Vec::new();
    let mut blockers = Vec::new();

    for &recipe in recipes {
        if recipe.is_heavy() {
            match mode {
                ProductSourceMode::Canonical => {
                    output_recipes.push(recipe);
                    heavy_recipes.push(recipe);
                }
                ProductSourceMode::Fastest => blockers.push(DerivedRecipeBlocker {
                    recipe_slug: recipe.slug().to_string(),
                    source_route: ProductSourceRoute::BlockedNoFastRoute,
                    reason: format!(
                        "recipe '{}' uses the cropped heavy ECAPE path; fastest mode will not fall back to canonical-derived compute",
                        recipe.slug()
                    ),
                }),
            }
            continue;
        }

        let candidate = planned_candidate_from_native(model, recipe, surface_product_override);

        match mode {
            ProductSourceMode::Canonical => {
                if let Some((native_recipe, candidate)) = candidate {
                    if use_native_route_in_canonical_mode(model, &candidate) {
                        output_recipes.push(recipe);
                        native_routes.push(PlannedNativeThermoRoute {
                            recipe,
                            native_recipe,
                            source_route: native_source_route(candidate.semantics),
                            candidate,
                        });
                        continue;
                    }
                }
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
        heavy_recipes,
        native_routes,
        blockers,
    })
}

fn leak_static_str(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

fn native_source_route(semantics: NativeSemantics) -> ProductSourceRoute {
    match semantics {
        NativeSemantics::ExactEquivalent => ProductSourceRoute::NativeExact,
        NativeSemantics::ProxyEquivalent => ProductSourceRoute::NativeProxy,
    }
}

fn use_native_route_in_canonical_mode(
    model: ModelId,
    candidate: &PlannedNativeDerivedCandidate,
) -> bool {
    model == ModelId::WrfGdex
        || (model == ModelId::Gfs
            && matches!(candidate.semantics, NativeSemantics::ExactEquivalent))
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
    heavy_recipes: &[DerivedRecipe],
    native_routes: &[PlannedNativeThermoRoute],
) -> Result<rustwx_models::LatestRun, Box<dyn std::error::Error>> {
    let needs_pair = !derived_compute_recipes.is_empty() || !heavy_recipes.is_empty();
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
        Some(
            pressure
                .pressure_3d_pa()
                .map(|values| values.to_vec())
                .unwrap_or_else(|| broadcast_levels_pa(pressure.pressure_levels_hpa(), grid.len())),
        )
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
        if recipes.contains(&DerivedRecipe::Vpd2m) {
            computed.vpd_2m_hpa = Some(surface_thermo.vpd_2m_hpa);
        }
        if recipes.contains(&DerivedRecipe::DewpointDepression2m) {
            computed.dewpoint_depression_2m_c = Some(surface_thermo.dewpoint_depression_2m_c);
        }
        if recipes.contains(&DerivedRecipe::Wetbulb2m) {
            computed.wetbulb_2m_c = Some(surface_thermo.wetbulb_2m_c);
        }
        if recipes.contains(&DerivedRecipe::FireWeatherComposite) {
            computed.fire_weather_composite = Some(surface_thermo.fire_weather_composite);
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
            let t700 = pressure_level_slice_or_interp(
                pressure,
                pressure.temperature_c_3d(),
                700.0,
                grid.len(),
            )
            .ok_or("missing 700 mb temperature slice in HRRR pressure bundle")?;
            let u700 =
                pressure_level_slice_or_interp(pressure, pressure.u_ms_3d(), 700.0, grid.len())
                    .ok_or("missing 700 mb u-wind slice in HRRR pressure bundle")?;
            let v700 =
                pressure_level_slice_or_interp(pressure, pressure.v_ms_3d(), 700.0, grid.len())
                    .ok_or("missing 700 mb v-wind slice in HRRR pressure bundle")?;
            computed.temperature_advection_700mb_cph = Some(
                rustwx_calc::compute_temperature_advection_700mb(TemperatureAdvectionInputs {
                    grid,
                    temperature_2d: &t700,
                    u_2d_ms: &u700,
                    v_2d_ms: &v700,
                    dx_m,
                    dy_m,
                })?
                .into_iter()
                .map(|value| value * 3600.0)
                .collect(),
            );
        }
        if requirements.temperature_advection_850mb {
            let t850 = pressure_level_slice_or_interp(
                pressure,
                pressure.temperature_c_3d(),
                850.0,
                grid.len(),
            )
            .ok_or("missing 850 mb temperature slice in HRRR pressure bundle")?;
            let u850 =
                pressure_level_slice_or_interp(pressure, pressure.u_ms_3d(), 850.0, grid.len())
                    .ok_or("missing 850 mb u-wind slice in HRRR pressure bundle")?;
            let v850 =
                pressure_level_slice_or_interp(pressure, pressure.v_ms_3d(), 850.0, grid.len())
                    .ok_or("missing 850 mb v-wind slice in HRRR pressure bundle")?;
            computed.temperature_advection_850mb_cph = Some(
                rustwx_calc::compute_temperature_advection_850mb(TemperatureAdvectionInputs {
                    grid,
                    temperature_2d: &t850,
                    u_2d_ms: &u850,
                    v_2d_ms: &v850,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DerivedQueryField {
    pub recipe_slug: String,
    pub title: String,
    pub units: String,
    pub values: Vec<f64>,
    pub nx: usize,
    pub ny: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct DerivedSampledProductField {
    pub recipe_slug: String,
    pub source_route: ProductSourceRoute,
    pub field: Field2D,
    pub input_fetches: Vec<PublishedFetchIdentity>,
}

#[derive(Debug, Clone)]
pub(crate) struct DerivedSampledProductSet {
    pub fields: Vec<DerivedSampledProductField>,
    pub blockers: Vec<DerivedRecipeBlocker>,
}

pub(crate) fn required_derived_fetch_products(
    model: ModelId,
    recipe_slugs: &[String],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let recipes = plan_derived_recipes(recipe_slugs)?;
    if recipes.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![
        resolve_canonical_bundle_product(model, CanonicalBundleDescriptor::SurfaceAnalysis, None)
            .native_product,
        resolve_canonical_bundle_product(model, CanonicalBundleDescriptor::PressureAnalysis, None)
            .native_product,
    ])
}

pub(crate) fn load_derived_sampled_fields_from_latest(
    latest: &LatestRun,
    forecast_hour: u16,
    cache_root: &std::path::Path,
    use_cache: bool,
    recipe_slugs: &[String],
) -> Result<DerivedSampledProductSet, Box<dyn std::error::Error>> {
    let recipes = plan_derived_recipes(recipe_slugs)?;
    if recipes.is_empty() {
        return Ok(DerivedSampledProductSet {
            fields: Vec::new(),
            blockers: Vec::new(),
        });
    }

    let plan = build_derived_execution_plan(latest, forecast_hour, None, None, true, &Vec::new());
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig::new(cache_root.to_path_buf(), use_cache),
    )?;
    let (_, surface_decode, _, pressure_decode) = loaded
        .require_surface_pressure_pair()
        .map_err(|err| format!("derived sampling surface/pressure pair unavailable: {err}"))?;
    let input_fetches = build_planned_input_fetches(&loaded);
    let mut fields = Vec::new();
    let mut blockers = Vec::new();

    for recipe in recipes {
        match compute_derived_query_field(
            &surface_decode.value,
            &pressure_decode.value,
            recipe.slug(),
        ) {
            Ok(query) => {
                let field = Field2D::new(
                    ProductKey::named(query.recipe_slug.clone()),
                    query.units.clone(),
                    surface_decode.value.core_grid()?,
                    query.values.into_iter().map(|value| value as f32).collect(),
                )?;
                fields.push(DerivedSampledProductField {
                    recipe_slug: query.recipe_slug,
                    source_route: ProductSourceRoute::CanonicalDerived,
                    field,
                    input_fetches: input_fetches.clone(),
                });
            }
            Err(err) => blockers.push(DerivedRecipeBlocker {
                recipe_slug: recipe.slug().to_string(),
                source_route: ProductSourceRoute::CanonicalDerived,
                reason: err.to_string(),
            }),
        }
    }

    Ok(DerivedSampledProductSet { fields, blockers })
}

pub(crate) fn compute_derived_query_field(
    surface: &GenericSurfaceFields,
    pressure: &GenericPressureFields,
    recipe_slug: &str,
) -> Result<DerivedQueryField, Box<dyn std::error::Error>> {
    fn take_values(
        values: &Option<Vec<f64>>,
        recipe: DerivedRecipe,
        field_name: &str,
    ) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        values.clone().ok_or_else(|| {
            format!(
                "derived field '{field_name}' was not computed for requested recipe '{}'",
                recipe.slug()
            )
            .into()
        })
    }

    let recipe = DerivedRecipe::parse(recipe_slug).map_err(std::io::Error::other)?;
    if recipe.is_heavy() {
        return Err(format!(
            "heavy derived recipe '{}' is not exposed through the lightweight query path",
            recipe.slug()
        )
        .into());
    }

    let computed = compute_derived_fields_generic(surface, pressure, &[recipe])?;
    let (values, units) = match recipe {
        DerivedRecipe::Sbcape => (
            take_values(&computed.sbcape_jkg, recipe, "sbcape_jkg")?,
            "J/kg",
        ),
        DerivedRecipe::Sbcin => (
            take_values(&computed.sbcin_jkg, recipe, "sbcin_jkg")?,
            "J/kg",
        ),
        DerivedRecipe::Sblcl => (take_values(&computed.sblcl_m, recipe, "sblcl_m")?, "m"),
        DerivedRecipe::Mlcape => (
            take_values(&computed.mlcape_jkg, recipe, "mlcape_jkg")?,
            "J/kg",
        ),
        DerivedRecipe::Mlcin => (
            take_values(&computed.mlcin_jkg, recipe, "mlcin_jkg")?,
            "J/kg",
        ),
        DerivedRecipe::Mucape => (
            take_values(&computed.mucape_jkg, recipe, "mucape_jkg")?,
            "J/kg",
        ),
        DerivedRecipe::Mucin => (
            take_values(&computed.mucin_jkg, recipe, "mucin_jkg")?,
            "J/kg",
        ),
        DerivedRecipe::ThetaE2m10mWinds => (
            take_values(&computed.theta_e_2m_k, recipe, "theta_e_2m_k")?,
            "K",
        ),
        DerivedRecipe::Vpd2m => (
            take_values(&computed.vpd_2m_hpa, recipe, "vpd_2m_hpa")?,
            "hPa",
        ),
        DerivedRecipe::DewpointDepression2m => (
            take_values(
                &computed.dewpoint_depression_2m_c,
                recipe,
                "dewpoint_depression_2m_c",
            )?,
            "degC",
        ),
        DerivedRecipe::Wetbulb2m => (
            take_values(&computed.wetbulb_2m_c, recipe, "wetbulb_2m_c")?,
            "degC",
        ),
        DerivedRecipe::FireWeatherComposite => (
            take_values(
                &computed.fire_weather_composite,
                recipe,
                "fire_weather_composite",
            )?,
            "index",
        ),
        DerivedRecipe::ApparentTemperature2m => (
            take_values(
                &computed.apparent_temperature_2m_c,
                recipe,
                "apparent_temperature_2m_c",
            )?,
            "degC",
        ),
        DerivedRecipe::HeatIndex2m => (
            take_values(&computed.heat_index_2m_c, recipe, "heat_index_2m_c")?,
            "degC",
        ),
        DerivedRecipe::WindChill2m => (
            take_values(&computed.wind_chill_2m_c, recipe, "wind_chill_2m_c")?,
            "degC",
        ),
        DerivedRecipe::LiftedIndex => (
            take_values(&computed.lifted_index_c, recipe, "lifted_index_c")?,
            "degC",
        ),
        DerivedRecipe::LapseRate700500 => (
            take_values(
                &computed.lapse_rate_700_500_cpkm,
                recipe,
                "lapse_rate_700_500_cpkm",
            )?,
            "degC/km",
        ),
        DerivedRecipe::LapseRate03km => (
            take_values(
                &computed.lapse_rate_0_3km_cpkm,
                recipe,
                "lapse_rate_0_3km_cpkm",
            )?,
            "degC/km",
        ),
        DerivedRecipe::BulkShear01km => (
            take_values(&computed.shear_01km_kt, recipe, "shear_01km_kt")?,
            "kt",
        ),
        DerivedRecipe::BulkShear06km => (
            take_values(&computed.shear_06km_kt, recipe, "shear_06km_kt")?,
            "kt",
        ),
        DerivedRecipe::Srh01km => (
            take_values(&computed.srh_01km_m2s2, recipe, "srh_01km_m2s2")?,
            "m^2/s^2",
        ),
        DerivedRecipe::Srh03km => (
            take_values(&computed.srh_03km_m2s2, recipe, "srh_03km_m2s2")?,
            "m^2/s^2",
        ),
        DerivedRecipe::Ehi01km => (
            take_values(&computed.ehi_01km, recipe, "ehi_01km")?,
            "dimensionless",
        ),
        DerivedRecipe::Ehi03km => (
            take_values(&computed.ehi_03km, recipe, "ehi_03km")?,
            "dimensionless",
        ),
        DerivedRecipe::StpFixed => (
            take_values(&computed.stp_fixed, recipe, "stp_fixed")?,
            "dimensionless",
        ),
        DerivedRecipe::ScpMu03km06kmProxy => (
            take_values(
                &computed.scp_mu_03km_06km_proxy,
                recipe,
                "scp_mu_03km_06km_proxy",
            )?,
            "dimensionless",
        ),
        DerivedRecipe::TemperatureAdvection700mb => (
            take_values(
                &computed.temperature_advection_700mb_cph,
                recipe,
                "temperature_advection_700mb_cph",
            )?,
            "degC/hr",
        ),
        DerivedRecipe::TemperatureAdvection850mb => (
            take_values(
                &computed.temperature_advection_850mb_cph,
                recipe,
                "temperature_advection_850mb_cph",
            )?,
            "degC/hr",
        ),
        DerivedRecipe::Sbecape
        | DerivedRecipe::Mlecape
        | DerivedRecipe::Muecape
        | DerivedRecipe::SbEcapeDerivedCapeRatio
        | DerivedRecipe::MlEcapeDerivedCapeRatio
        | DerivedRecipe::MuEcapeDerivedCapeRatio
        | DerivedRecipe::SbEcapeNativeCapeRatio
        | DerivedRecipe::MlEcapeNativeCapeRatio
        | DerivedRecipe::MuEcapeNativeCapeRatio
        | DerivedRecipe::Sbncape
        | DerivedRecipe::Sbecin
        | DerivedRecipe::Mlecin
        | DerivedRecipe::EcapeScp
        | DerivedRecipe::EcapeEhi01km
        | DerivedRecipe::EcapeEhi03km
        | DerivedRecipe::EcapeStp => unreachable!("heavy recipes are blocked above"),
    };

    Ok(DerivedQueryField {
        recipe_slug: recipe.slug().to_string(),
        title: recipe.title().to_string(),
        units: units.to_string(),
        values,
        nx: surface.nx,
        ny: surface.ny,
    })
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
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    build_render_artifact_with_contour_mode(
        recipe,
        grid,
        projected,
        date_yyyymmdd,
        cycle_utc,
        forecast_hour,
        source,
        model,
        output_width,
        output_height,
        computed,
        contour_mode,
        native_fill_level_multiplier,
    )
}

fn build_render_artifact_with_contour_mode(
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
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<HrrrDerivedLiveArtifact, Box<dyn std::error::Error>> {
    let (field, mut request) = match recipe {
        DerivedRecipe::Sbcape => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.sbcape_jkg, recipe, "sbcape_jkg")?.clone(),
            WeatherProduct::Sbcape,
        )?,
        DerivedRecipe::Sbcin => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.sbcin_jkg, recipe, "sbcin_jkg")?.clone(),
            WeatherProduct::Sbcin,
        )?,
        DerivedRecipe::Sblcl => weather_request(
            recipe,
            grid,
            "m",
            required_values(&computed.sblcl_m, recipe, "sblcl_m")?.clone(),
            WeatherProduct::Lcl,
        )?,
        DerivedRecipe::Mlcape => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mlcape_jkg, recipe, "mlcape_jkg")?.clone(),
            WeatherProduct::Mlcape,
        )?,
        DerivedRecipe::Mlcin => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mlcin_jkg, recipe, "mlcin_jkg")?.clone(),
            WeatherProduct::Mlcin,
        )?,
        DerivedRecipe::Mucape => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mucape_jkg, recipe, "mucape_jkg")?.clone(),
            WeatherProduct::Mucape,
        )?,
        DerivedRecipe::Mucin => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mucin_jkg, recipe, "mucin_jkg")?.clone(),
            WeatherProduct::Mucin,
        )?,
        DerivedRecipe::ThetaE2m10mWinds => palette_request(
            recipe,
            grid,
            "K",
            required_values(&computed.theta_e_2m_k, recipe, "theta_e_2m_k")?.clone(),
            WeatherPalette::Temperature,
            range_step(280.0, 381.0, 4.0),
            ExtendMode::Both,
            Some(8.0),
        )?,
        DerivedRecipe::Vpd2m => custom_scale_request(
            recipe,
            grid,
            "hPa",
            required_values(&computed.vpd_2m_hpa, recipe, "vpd_2m_hpa")?.clone(),
            range_step(0.0, 11.0, 1.0),
            vpd_scale_colors(),
            ExtendMode::Max,
            Some(2.0),
        )?,
        DerivedRecipe::DewpointDepression2m => custom_scale_request(
            recipe,
            grid,
            "degC",
            required_values(
                &computed.dewpoint_depression_2m_c,
                recipe,
                "dewpoint_depression_2m_c",
            )?
            .clone(),
            range_step(0.0, 41.0, 4.0),
            dewpoint_depression_scale_colors(),
            ExtendMode::Max,
            Some(8.0),
        )?,
        DerivedRecipe::Wetbulb2m => scale_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.wetbulb_2m_c, recipe, "wetbulb_2m_c")?.clone(),
            surface_temperature_scale_c(0.5),
            Some(5.0),
        )?,
        DerivedRecipe::FireWeatherComposite => custom_scale_request(
            recipe,
            grid,
            "index",
            required_values(
                &computed.fire_weather_composite,
                recipe,
                "fire_weather_composite",
            )?
            .clone(),
            range_step(0.0, 101.0, 10.0),
            fire_weather_composite_scale_colors(),
            ExtendMode::Neither,
            Some(20.0),
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
            WeatherPalette::Temperature,
            range_step(-30.0, 51.0, 5.0),
            ExtendMode::Both,
            Some(5.0),
        )?,
        DerivedRecipe::WindChill2m => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.wind_chill_2m_c, recipe, "wind_chill_2m_c")?.clone(),
            WeatherPalette::Temperature,
            range_step(-40.0, 31.0, 5.0),
            ExtendMode::Both,
            Some(5.0),
        )?,
        DerivedRecipe::LiftedIndex => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.lifted_index_c, recipe, "lifted_index_c")?.clone(),
            WeatherPalette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        DerivedRecipe::LapseRate700500 => weather_lapse_request(
            recipe,
            grid,
            required_values(
                &computed.lapse_rate_700_500_cpkm,
                recipe,
                "lapse_rate_700_500_cpkm",
            )?
            .clone(),
        )?,
        DerivedRecipe::LapseRate03km => weather_lapse_request(
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
            WeatherPalette::Winds,
            range_step(0.0, 85.0, 5.0),
            ExtendMode::Max,
            Some(5.0),
        )?,
        DerivedRecipe::BulkShear06km => palette_request(
            recipe,
            grid,
            "kt",
            required_values(&computed.shear_06km_kt, recipe, "shear_06km_kt")?.clone(),
            WeatherPalette::Winds,
            range_step(0.0, 85.0, 5.0),
            ExtendMode::Max,
            Some(5.0),
        )?,
        DerivedRecipe::Srh01km => weather_request(
            recipe,
            grid,
            "m^2/s^2",
            required_values(&computed.srh_01km_m2s2, recipe, "srh_01km_m2s2")?.clone(),
            WeatherProduct::Srh01km,
        )?,
        DerivedRecipe::Srh03km => weather_request(
            recipe,
            grid,
            "m^2/s^2",
            required_values(&computed.srh_03km_m2s2, recipe, "srh_03km_m2s2")?.clone(),
            WeatherProduct::Srh03km,
        )?,
        DerivedRecipe::Ehi01km => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.ehi_01km, recipe, "ehi_01km")?.clone(),
            WeatherProduct::Ehi,
        )?,
        DerivedRecipe::Ehi03km => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.ehi_03km, recipe, "ehi_03km")?.clone(),
            WeatherProduct::Ehi,
        )?,
        DerivedRecipe::StpFixed => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.stp_fixed, recipe, "stp_fixed")?.clone(),
            WeatherProduct::StpFixed,
        )?,
        DerivedRecipe::ScpMu03km06kmProxy => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(
                &computed.scp_mu_03km_06km_proxy,
                recipe,
                "scp_mu_03km_06km_proxy",
            )?
            .clone(),
            WeatherProduct::Scp,
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
            WeatherPalette::Temperature,
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
            WeatherPalette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        DerivedRecipe::Sbecape
        | DerivedRecipe::Mlecape
        | DerivedRecipe::Muecape
        | DerivedRecipe::SbEcapeDerivedCapeRatio
        | DerivedRecipe::MlEcapeDerivedCapeRatio
        | DerivedRecipe::MuEcapeDerivedCapeRatio
        | DerivedRecipe::SbEcapeNativeCapeRatio
        | DerivedRecipe::MlEcapeNativeCapeRatio
        | DerivedRecipe::MuEcapeNativeCapeRatio
        | DerivedRecipe::Sbncape
        | DerivedRecipe::Sbecin
        | DerivedRecipe::Mlecin
        | DerivedRecipe::EcapeScp
        | DerivedRecipe::EcapeEhi01km
        | DerivedRecipe::EcapeEhi03km
        | DerivedRecipe::EcapeStp => {
            return Err(format!(
                "heavy derived recipe '{}' must render through the cropped ECAPE path",
                recipe.slug()
            )
            .into());
        }
    };

    request.width = output_width;
    request.height = output_height;
    request.chrome_scale = ChromeScale::Fixed(1.5);
    request.supersample_factor = 2;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.title = Some(derived_title_for_model(model, recipe.title()));
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
    maybe_apply_native_contour_fill_for_mode(
        recipe,
        &mut request,
        contour_mode,
        native_fill_level_multiplier,
    )?;
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

fn build_render_artifact_with_contour_mode_profiled(
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
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<(HrrrDerivedLiveArtifact, DerivedLiveArtifactBuildTiming), Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let request_base_build_start = Instant::now();
    let (field, mut request) = match recipe {
        DerivedRecipe::Sbcape => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.sbcape_jkg, recipe, "sbcape_jkg")?.clone(),
            WeatherProduct::Sbcape,
        )?,
        DerivedRecipe::Sbcin => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.sbcin_jkg, recipe, "sbcin_jkg")?.clone(),
            WeatherProduct::Sbcin,
        )?,
        DerivedRecipe::Sblcl => weather_request(
            recipe,
            grid,
            "m",
            required_values(&computed.sblcl_m, recipe, "sblcl_m")?.clone(),
            WeatherProduct::Lcl,
        )?,
        DerivedRecipe::Mlcape => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mlcape_jkg, recipe, "mlcape_jkg")?.clone(),
            WeatherProduct::Mlcape,
        )?,
        DerivedRecipe::Mlcin => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mlcin_jkg, recipe, "mlcin_jkg")?.clone(),
            WeatherProduct::Mlcin,
        )?,
        DerivedRecipe::Mucape => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mucape_jkg, recipe, "mucape_jkg")?.clone(),
            WeatherProduct::Mucape,
        )?,
        DerivedRecipe::Mucin => weather_request(
            recipe,
            grid,
            "J/kg",
            required_values(&computed.mucin_jkg, recipe, "mucin_jkg")?.clone(),
            WeatherProduct::Mucin,
        )?,
        DerivedRecipe::ThetaE2m10mWinds => palette_request(
            recipe,
            grid,
            "K",
            required_values(&computed.theta_e_2m_k, recipe, "theta_e_2m_k")?.clone(),
            WeatherPalette::Temperature,
            range_step(280.0, 381.0, 4.0),
            ExtendMode::Both,
            Some(8.0),
        )?,
        DerivedRecipe::Vpd2m => custom_scale_request(
            recipe,
            grid,
            "hPa",
            required_values(&computed.vpd_2m_hpa, recipe, "vpd_2m_hpa")?.clone(),
            range_step(0.0, 11.0, 1.0),
            vpd_scale_colors(),
            ExtendMode::Max,
            Some(2.0),
        )?,
        DerivedRecipe::DewpointDepression2m => custom_scale_request(
            recipe,
            grid,
            "degC",
            required_values(
                &computed.dewpoint_depression_2m_c,
                recipe,
                "dewpoint_depression_2m_c",
            )?
            .clone(),
            range_step(0.0, 41.0, 4.0),
            dewpoint_depression_scale_colors(),
            ExtendMode::Max,
            Some(8.0),
        )?,
        DerivedRecipe::Wetbulb2m => scale_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.wetbulb_2m_c, recipe, "wetbulb_2m_c")?.clone(),
            surface_temperature_scale_c(0.5),
            Some(5.0),
        )?,
        DerivedRecipe::FireWeatherComposite => custom_scale_request(
            recipe,
            grid,
            "index",
            required_values(
                &computed.fire_weather_composite,
                recipe,
                "fire_weather_composite",
            )?
            .clone(),
            range_step(0.0, 101.0, 10.0),
            fire_weather_composite_scale_colors(),
            ExtendMode::Neither,
            Some(20.0),
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
            WeatherPalette::Temperature,
            range_step(-30.0, 51.0, 5.0),
            ExtendMode::Both,
            Some(5.0),
        )?,
        DerivedRecipe::WindChill2m => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.wind_chill_2m_c, recipe, "wind_chill_2m_c")?.clone(),
            WeatherPalette::Temperature,
            range_step(-40.0, 31.0, 5.0),
            ExtendMode::Both,
            Some(5.0),
        )?,
        DerivedRecipe::LiftedIndex => palette_request(
            recipe,
            grid,
            "degC",
            required_values(&computed.lifted_index_c, recipe, "lifted_index_c")?.clone(),
            WeatherPalette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        DerivedRecipe::LapseRate700500 => weather_lapse_request(
            recipe,
            grid,
            required_values(
                &computed.lapse_rate_700_500_cpkm,
                recipe,
                "lapse_rate_700_500_cpkm",
            )?
            .clone(),
        )?,
        DerivedRecipe::LapseRate03km => weather_lapse_request(
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
            WeatherPalette::Winds,
            range_step(0.0, 85.0, 5.0),
            ExtendMode::Max,
            Some(5.0),
        )?,
        DerivedRecipe::BulkShear06km => palette_request(
            recipe,
            grid,
            "kt",
            required_values(&computed.shear_06km_kt, recipe, "shear_06km_kt")?.clone(),
            WeatherPalette::Winds,
            range_step(0.0, 85.0, 5.0),
            ExtendMode::Max,
            Some(5.0),
        )?,
        DerivedRecipe::Srh01km => weather_request(
            recipe,
            grid,
            "m^2/s^2",
            required_values(&computed.srh_01km_m2s2, recipe, "srh_01km_m2s2")?.clone(),
            WeatherProduct::Srh01km,
        )?,
        DerivedRecipe::Srh03km => weather_request(
            recipe,
            grid,
            "m^2/s^2",
            required_values(&computed.srh_03km_m2s2, recipe, "srh_03km_m2s2")?.clone(),
            WeatherProduct::Srh03km,
        )?,
        DerivedRecipe::Ehi01km => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.ehi_01km, recipe, "ehi_01km")?.clone(),
            WeatherProduct::Ehi,
        )?,
        DerivedRecipe::Ehi03km => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.ehi_03km, recipe, "ehi_03km")?.clone(),
            WeatherProduct::Ehi,
        )?,
        DerivedRecipe::StpFixed => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(&computed.stp_fixed, recipe, "stp_fixed")?.clone(),
            WeatherProduct::StpFixed,
        )?,
        DerivedRecipe::ScpMu03km06kmProxy => weather_request(
            recipe,
            grid,
            "dimensionless",
            required_values(
                &computed.scp_mu_03km_06km_proxy,
                recipe,
                "scp_mu_03km_06km_proxy",
            )?
            .clone(),
            WeatherProduct::Scp,
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
            WeatherPalette::Temperature,
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
            WeatherPalette::Temperature,
            range_step(-12.0, 13.0, 1.0),
            ExtendMode::Both,
            Some(1.0),
        )?,
        DerivedRecipe::Sbecape
        | DerivedRecipe::Mlecape
        | DerivedRecipe::Muecape
        | DerivedRecipe::SbEcapeDerivedCapeRatio
        | DerivedRecipe::MlEcapeDerivedCapeRatio
        | DerivedRecipe::MuEcapeDerivedCapeRatio
        | DerivedRecipe::SbEcapeNativeCapeRatio
        | DerivedRecipe::MlEcapeNativeCapeRatio
        | DerivedRecipe::MuEcapeNativeCapeRatio
        | DerivedRecipe::Sbncape
        | DerivedRecipe::Sbecin
        | DerivedRecipe::Mlecin
        | DerivedRecipe::EcapeScp
        | DerivedRecipe::EcapeEhi01km
        | DerivedRecipe::EcapeEhi03km
        | DerivedRecipe::EcapeStp => {
            return Err(format!(
                "heavy derived recipe '{}' must render through the cropped ECAPE path",
                recipe.slug()
            )
            .into());
        }
    };

    request.width = output_width;
    request.height = output_height;
    request.chrome_scale = ChromeScale::Fixed(1.5);
    request.supersample_factor = 2;
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.title = Some(derived_title_for_model(model, recipe.title()));
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
    let request_base_build_ms = request_base_build_start.elapsed().as_millis();

    let native_contour_timing = maybe_apply_native_contour_fill_for_mode_profiled(
        recipe,
        &mut request,
        contour_mode,
        native_fill_level_multiplier,
    )?;

    let mut wind_overlay_build_ms = 0;
    if matches!(recipe, DerivedRecipe::ThetaE2m10mWinds) {
        let wind_overlay_start = Instant::now();
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
        wind_overlay_build_ms = wind_overlay_start.elapsed().as_millis();
    }

    Ok((
        HrrrDerivedLiveArtifact {
            recipe_slug: recipe.slug().to_string(),
            title: recipe.title().to_string(),
            field,
            request,
        },
        DerivedLiveArtifactBuildTiming {
            compute_fields_ms: 0,
            request_base_build_ms,
            native_contour_fill_ms: native_contour_timing.total_ms,
            native_contour_projected_points_ms: native_contour_timing.projected_points_ms,
            native_contour_scalar_field_ms: native_contour_timing.scalar_field_ms,
            native_contour_fill_topology_ms: native_contour_timing.fill_topology_ms,
            native_contour_fill_geometry_ms: native_contour_timing.fill_geometry_ms,
            native_contour_line_topology_ms: native_contour_timing.line_topology_ms,
            native_contour_line_geometry_ms: native_contour_timing.line_geometry_ms,
            wind_overlay_build_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    ))
}

struct NativeContourProductConfig {
    scale: rustwx_render::ColorScale,
    line_levels: &'static [f64],
    line_style: ProjectedContourLineStyle,
    tick_step: Option<f64>,
}

const STP_NATIVE_LINE_LEVELS: &[f64] = &[1.0, 3.0, 5.0];
const CAPE_NATIVE_LINE_LEVELS: &[f64] = &[500.0, 1000.0, 2000.0, 3000.0, 4000.0];
const SRH_NATIVE_LINE_LEVELS: &[f64] = &[150.0, 250.0, 350.0, 450.0];
const EHI_NATIVE_LINE_LEVELS: &[f64] = &[1.0, 2.0, 3.0, 5.0];

fn native_contour_product_config(recipe: DerivedRecipe) -> Option<NativeContourProductConfig> {
    match recipe {
        DerivedRecipe::StpFixed => Some(NativeContourProductConfig {
            scale: rustwx_render::ColorScale::Discrete(rustwx_render::palette_scale(
                WeatherPalette::Stp,
                vec![1.0, 2.0, 3.0, 5.0, 8.0, 11.0],
                ExtendMode::Max,
                None,
            )),
            line_levels: STP_NATIVE_LINE_LEVELS,
            line_style: ProjectedContourLineStyle {
                color: Color::rgba(55, 16, 16, 210),
                width: 2,
            },
            tick_step: Some(1.0),
        }),
        DerivedRecipe::Sbcape | DerivedRecipe::Mlcape => Some(NativeContourProductConfig {
            scale: rustwx_render::ColorScale::Discrete(rustwx_render::palette_scale(
                WeatherPalette::Cape,
                vec![250.0, 500.0, 1000.0, 1500.0, 2000.0, 3000.0, 4000.0, 5000.0],
                ExtendMode::Max,
                None,
            )),
            line_levels: CAPE_NATIVE_LINE_LEVELS,
            line_style: ProjectedContourLineStyle {
                color: Color::rgba(84, 44, 18, 215),
                width: 2,
            },
            tick_step: Some(500.0),
        }),
        DerivedRecipe::Srh01km | DerivedRecipe::Srh03km => Some(NativeContourProductConfig {
            scale: rustwx_render::ColorScale::Discrete(rustwx_render::palette_scale(
                WeatherPalette::Srh,
                vec![100.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0],
                ExtendMode::Max,
                None,
            )),
            line_levels: SRH_NATIVE_LINE_LEVELS,
            line_style: ProjectedContourLineStyle {
                color: Color::rgba(15, 35, 56, 220),
                width: 2,
            },
            tick_step: Some(50.0),
        }),
        DerivedRecipe::Ehi01km | DerivedRecipe::Ehi03km => Some(NativeContourProductConfig {
            scale: rustwx_render::ColorScale::Discrete(rustwx_render::palette_scale(
                WeatherPalette::Ehi,
                vec![0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0],
                ExtendMode::Max,
                None,
            )),
            line_levels: EHI_NATIVE_LINE_LEVELS,
            line_style: ProjectedContourLineStyle {
                color: Color::rgba(44, 18, 66, 220),
                width: 2,
            },
            tick_step: Some(0.5),
        }),
        _ => None,
    }
}

pub fn native_contour_line_levels_for_recipe_slug(
    recipe_slug: &str,
) -> Result<Option<Vec<f64>>, String> {
    let recipe = DerivedRecipe::parse(recipe_slug)?;
    Ok(native_contour_product_config(recipe).map(|config| config.line_levels.to_vec()))
}

fn maybe_apply_native_contour_fill_for_mode(
    recipe: DerivedRecipe,
    request: &mut MapRenderRequest,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    maybe_apply_native_contour_fill_for_mode_profiled(
        recipe,
        request,
        contour_mode,
        native_fill_level_multiplier,
    )
    .map(|_| ())
}

fn maybe_apply_native_contour_fill_for_mode_profiled(
    recipe: DerivedRecipe,
    request: &mut MapRenderRequest,
    contour_mode: NativeContourRenderMode,
    native_fill_level_multiplier: usize,
) -> Result<NativeContourBuildTiming, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    if matches!(contour_mode, NativeContourRenderMode::LegacyRaster) {
        return Ok(NativeContourBuildTiming::default());
    }
    let Some(projected_domain) = request.projected_domain.as_ref() else {
        return Ok(NativeContourBuildTiming::default());
    };
    let config = match contour_mode {
        NativeContourRenderMode::Automatic | NativeContourRenderMode::Signature => {
            return Ok(NativeContourBuildTiming::default());
        }
        NativeContourRenderMode::ExperimentalAllProjected => native_contour_product_config(recipe)
            .unwrap_or_else(|| NativeContourProductConfig {
                scale: request.scale.clone(),
                line_levels: &[],
                line_style: ProjectedContourLineStyle::default(),
                tick_step: request.cbar_tick_step,
            }),
        NativeContourRenderMode::LegacyRaster => unreachable!(),
    };
    request.scale = densify_native_contour_scale(config.scale, native_fill_level_multiplier);
    if config.tick_step.is_some() {
        request.cbar_tick_step = config.tick_step;
    }
    let (geometry, geometry_timing) = build_projected_contour_geometry_profile(
        &request.field,
        projected_domain,
        &request.scale,
        config.line_levels,
        config.line_style,
    )?;
    request.projected_data_polygons.extend(geometry.fills);
    request.projected_lines.extend(geometry.lines);
    request.field.values.fill(f32::NAN);
    Ok(NativeContourBuildTiming {
        total_ms: total_start.elapsed().as_millis(),
        projected_points_ms: geometry_timing.projected_points_ms,
        scalar_field_ms: geometry_timing.scalar_field_ms,
        fill_topology_ms: geometry_timing.fill_topology_ms,
        fill_geometry_ms: geometry_timing.fill_geometry_ms,
        line_topology_ms: geometry_timing.line_topology_ms,
        line_geometry_ms: geometry_timing.line_geometry_ms,
    })
}

fn densify_native_contour_scale(
    scale: rustwx_render::ColorScale,
    native_fill_level_multiplier: usize,
) -> rustwx_render::ColorScale {
    if native_fill_level_multiplier <= 1 {
        return scale;
    }
    let discrete = scale.resolved_discrete();
    rustwx_render::ColorScale::Discrete(densify_discrete_scale(
        &discrete,
        LevelDensity {
            multiplier: native_fill_level_multiplier,
            min_source_level_count: 2,
        },
    ))
}

fn heavy_ecape_subtitle_right(recipe: DerivedRecipe, source: SourceId) -> String {
    let source_label = format!("source: {}", source);
    match recipe {
        DerivedRecipe::SbEcapeDerivedCapeRatio
        | DerivedRecipe::MlEcapeDerivedCapeRatio
        | DerivedRecipe::MuEcapeDerivedCapeRatio => {
            format!("{source_label} | EXP | derived")
        }
        DerivedRecipe::SbEcapeNativeCapeRatio
        | DerivedRecipe::MlEcapeNativeCapeRatio
        | DerivedRecipe::MuEcapeNativeCapeRatio => {
            format!("{source_label} | EXP | native")
        }
        DerivedRecipe::EcapeScp
        | DerivedRecipe::EcapeEhi01km
        | DerivedRecipe::EcapeEhi03km
        | DerivedRecipe::EcapeStp => {
            format!("{source_label} | experimental")
        }
        _ => source_label,
    }
}

fn render_derived_heavy_recipe(
    request: &DerivedBatchRequest,
    recipe: DerivedRecipe,
    field: &WeatherPanelField,
    grid: &rustwx_core::LatLonGrid,
    projection: Option<&rustwx_core::GridProjection>,
    projected: &ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    model: ModelId,
    input_fetch_keys: Vec<String>,
) -> Result<DerivedRenderedRecipe, Box<dyn std::error::Error>> {
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}.png",
        model.as_str().replace('-', "_"),
        date_yyyymmdd,
        cycle_utc,
        forecast_hour,
        request.domain.slug,
        recipe.slug()
    ));
    let subtitle_left = format!(
        "{} {}Z F{:03}  {}",
        date_yyyymmdd, cycle_utc, forecast_hour, model
    );
    let render_start = Instant::now();
    let mut render_request = build_weather_map_request(
        grid,
        projected,
        field,
        request.output_width,
        request.output_height,
        Some(subtitle_left),
        Some(heavy_ecape_subtitle_right(recipe, source)),
    )?;
    render_request.chrome_scale = ChromeScale::Fixed(1.5);
    render_request.title = Some(derived_title_for_request(request, recipe.title()));
    maybe_apply_native_contour_fill_for_mode(
        recipe,
        &mut render_request,
        request.contour_mode,
        request.native_fill_level_multiplier,
    )?;
    if let Some(overlay) = request.custom_poi_overlay.as_ref() {
        apply_custom_poi_overlay(
            &mut render_request,
            overlay,
            request.domain.bounds,
            &grid.lat_deg,
            &grid.lon_deg,
            projection,
        )?;
    }
    if let Some(overlay) = request.place_label_overlay.as_ref() {
        crate::apply_place_label_overlay_with_density_styling(
            &mut render_request,
            overlay,
            &request.domain,
            &grid.lat_deg,
            &grid.lon_deg,
            projection,
        )?;
    }
    let save_timing =
        save_png_profile_with_options(&render_request, &output_path, &request.png_write_options())?;
    let render_ms = render_start.elapsed().as_millis();
    let content_identity = artifact_identity_from_path(&output_path)?;
    Ok(DerivedRenderedRecipe {
        recipe_slug: recipe.slug().to_string(),
        title: recipe.title().to_string(),
        source_route: ProductSourceRoute::CanonicalDerived,
        output_path,
        content_identity,
        input_fetch_keys,
        timing: DerivedRecipeTiming {
            render_to_image_ms: save_timing.png_timing.render_to_image_ms,
            data_layer_draw_ms: derived_data_layer_draw_ms(&save_timing.png_timing.image_timing),
            overlay_draw_ms: derived_overlay_draw_ms(&save_timing.png_timing.image_timing),
            render_state_prep_ms: save_timing.state_timing.state_prep_ms,
            png_encode_ms: save_timing.png_timing.png_encode_ms,
            file_write_ms: save_timing.file_write_ms,
            render_ms,
            total_ms: render_ms,
            state_timing: save_timing.state_timing,
            image_timing: save_timing.png_timing.image_timing,
        },
    })
}

fn render_derived_heavy_recipes(
    request: &DerivedBatchRequest,
    heavy_recipes: &[DerivedRecipe],
    full_surface: &GenericSurfaceFields,
    full_pressure: &GenericPressureFields,
    full_grid: &rustwx_core::LatLonGrid,
    full_projected: &ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    model: ModelId,
    input_fetch_keys: Vec<String>,
) -> Result<(Vec<DerivedRenderedRecipe>, HeavyComputeTiming), Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let heavy_domain = crop_and_guard_heavy_domain(
        full_surface,
        full_pressure,
        full_projected,
        &request.domain,
        2,
        request.allow_large_heavy_domain,
    )?;
    let (surface, pressure, grid) = heavy_domain.bind(full_surface, full_pressure, full_grid);
    let projected = if heavy_domain.cropped.is_some() {
        build_projected_map_with_projection(
            &grid.lat_deg,
            &grid.lon_deg,
            surface.projection.as_ref(),
            request.domain.bounds,
            map_frame_aspect_ratio(request.output_width, request.output_height, true, true),
        )?
    } else {
        full_projected.clone()
    };

    let (prepared, prep_timing) = prepare_heavy_volume_timed(surface, pressure, false)?;
    let ecape_start = Instant::now();
    let (ecape_fields, _failure_count) =
        compute_ecape_map_fields_with_prepared_volume(surface, pressure, &prepared)?;
    let ecape_triplet_ms = ecape_start.elapsed().as_millis();

    let mut rendered = Vec::with_capacity(heavy_recipes.len());
    let mut render_ms = 0u128;
    for recipe in heavy_recipes {
        let field = ecape_fields
            .iter()
            .find(|field| field.artifact_slug() == recipe.slug())
            .ok_or_else(|| {
                format!(
                    "heavy derived ECAPE renderer missing field for recipe '{}'",
                    recipe.slug()
                )
            })?;
        let artifact = render_derived_heavy_recipe(
            request,
            *recipe,
            field,
            &grid,
            surface.projection(),
            &projected,
            date_yyyymmdd,
            cycle_utc,
            forecast_hour,
            source,
            model,
            input_fetch_keys.clone(),
        )?;
        render_ms += artifact.timing.render_ms;
        rendered.push(artifact);
    }

    Ok((
        rendered,
        HeavyComputeTiming {
            full_cells: heavy_domain.stats.full_cells,
            cropped_cells: heavy_domain.stats.cropped_cells,
            pressure_levels: heavy_domain.stats.pressure_levels,
            crop_kind: heavy_domain.stats.crop_kind,
            crop_ms: heavy_domain.crop_ms,
            prepare_height_agl_ms: prep_timing.prepare_height_agl_ms,
            broadcast_pressure_ms: prep_timing.broadcast_pressure_ms,
            pressure_3d_bytes: prep_timing.pressure_3d_bytes,
            ecape_triplet_ms,
            severe_fields_ms: 0,
            render_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    ))
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
        vpd_2m_hpa: crop_optional_values(&computed.vpd_2m_hpa, source_nx, crop),
        dewpoint_depression_2m_c: crop_optional_values(
            &computed.dewpoint_depression_2m_c,
            source_nx,
            crop,
        ),
        wetbulb_2m_c: crop_optional_values(&computed.wetbulb_2m_c, source_nx, crop),
        fire_weather_composite: crop_optional_values(
            &computed.fire_weather_composite,
            source_nx,
            crop,
        ),
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

fn weather_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    product: WeatherProduct,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, units, grid, values)?;
    let request = MapRenderRequest::for_core_weather_product(field.clone(), product)
        .with_visual_mode(recipe.visual_mode());
    Ok((field, request))
}

fn weather_lapse_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    values: Vec<f64>,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, "degC/km", grid, values)?;
    let mut request = MapRenderRequest::for_palette_fill(
        field.clone().into(),
        WeatherPalette::LapseRate,
        range_step(2.0, 10.1, 0.1),
        ExtendMode::Both,
    )
    .with_visual_mode(recipe.visual_mode());
    request.cbar_tick_step = Some(1.0);
    Ok((field, request))
}

fn palette_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    palette: WeatherPalette,
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

fn scale_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    scale: ColorScale,
    tick_step: Option<f64>,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, units, grid, values)?;
    let mut request =
        MapRenderRequest::new(field.clone().into(), scale).with_visual_mode(recipe.visual_mode());
    request.cbar_tick_step = tick_step;
    Ok((field, request))
}

fn custom_scale_request(
    recipe: DerivedRecipe,
    grid: &rustwx_core::LatLonGrid,
    units: &str,
    values: Vec<f64>,
    levels: Vec<f64>,
    colors: Vec<Color>,
    extend: ExtendMode,
    tick_step: Option<f64>,
) -> Result<(Field2D, MapRenderRequest), Box<dyn std::error::Error>> {
    let field = core_field(recipe, units, grid, values)?;
    let mut request = MapRenderRequest::new(
        field.clone().into(),
        rustwx_render::ColorScale::Discrete(rustwx_render::DiscreteColorScale {
            levels,
            colors,
            extend,
            mask_below: None,
        }),
    )
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

fn surface_temperature_scale_c(level_step_c: f64) -> ColorScale {
    let lo = -50.0;
    let hi = 50.5;
    ColorScale::Discrete(DiscreteColorScale {
        levels: range_step(lo, hi, level_step_c),
        colors: temperature_palette_cropped_f(
            Some((-40.0, 120.0)),
            (((hi - lo) / level_step_c).round() as usize).max(2),
        ),
        extend: ExtendMode::Both,
        mask_below: None,
    })
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

fn vpd_scale_colors() -> Vec<Color> {
    vec![
        Color::rgba(26, 152, 80, 255),
        Color::rgba(85, 180, 95, 255),
        Color::rgba(120, 198, 102, 255),
        Color::rgba(166, 217, 106, 255),
        Color::rgba(217, 239, 139, 255),
        Color::rgba(254, 224, 139, 255),
        Color::rgba(253, 174, 97, 255),
        Color::rgba(244, 109, 67, 255),
        Color::rgba(215, 48, 39, 255),
        Color::rgba(165, 0, 38, 255),
    ]
}

fn dewpoint_depression_scale_colors() -> Vec<Color> {
    vec![
        Color::rgba(0, 104, 55, 255),
        Color::rgba(26, 152, 80, 255),
        Color::rgba(102, 189, 99, 255),
        Color::rgba(166, 217, 106, 255),
        Color::rgba(217, 239, 139, 255),
        Color::rgba(254, 224, 139, 255),
        Color::rgba(253, 174, 97, 255),
        Color::rgba(244, 109, 67, 255),
        Color::rgba(215, 48, 39, 255),
        Color::rgba(165, 0, 38, 255),
    ]
}

fn fire_weather_composite_scale_colors() -> Vec<Color> {
    vec![
        Color::rgba(34, 139, 34, 255),
        Color::rgba(50, 205, 50, 255),
        Color::rgba(120, 230, 60, 255),
        Color::rgba(173, 255, 47, 255),
        Color::rgba(255, 215, 0, 255),
        Color::rgba(255, 170, 0, 255),
        Color::rgba(255, 140, 0, 255),
        Color::rgba(255, 69, 0, 255),
        Color::rgba(204, 0, 0, 255),
        Color::rgba(139, 0, 0, 255),
    ]
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

fn pressure_level_slice_or_interp<P>(
    pressure: &P,
    values_3d: &[f64],
    target_hpa: f64,
    nxy: usize,
) -> Option<Vec<f64>>
where
    P: PressureFieldSet,
{
    if let Some(slice) = level_slice(values_3d, pressure.pressure_levels_hpa(), target_hpa, nxy) {
        return Some(slice.to_vec());
    }

    let pressure_3d_pa = pressure.pressure_3d_pa()?;
    let nz = pressure.pressure_levels_hpa().len();
    if nz == 0 || values_3d.len() != pressure_3d_pa.len() || values_3d.len() != nxy * nz {
        return None;
    }

    let log_target = target_hpa.ln();
    Some(
        (0..nxy)
            .into_par_iter()
            .map(|ij| {
                for k in 0..nz.saturating_sub(1) {
                    let idx0 = k * nxy + ij;
                    let idx1 = (k + 1) * nxy + ij;
                    let p0 = pressure_3d_pa[idx0] / 100.0;
                    let p1 = pressure_3d_pa[idx1] / 100.0;
                    if !p0.is_finite() || !p1.is_finite() || p0 <= 0.0 || p1 <= 0.0 {
                        continue;
                    }
                    if (p0 >= target_hpa && p1 <= target_hpa)
                        || (p0 <= target_hpa && p1 >= target_hpa)
                    {
                        let v0 = values_3d[idx0];
                        let v1 = values_3d[idx1];
                        let log0 = p0.ln();
                        let log1 = p1.ln();
                        let denom = log1 - log0;
                        if denom.abs() < 1.0e-12 {
                            return 0.5 * (v0 + v1);
                        }
                        let frac = (log_target - log0) / denom;
                        return v0 + frac * (v1 - v0);
                    }
                }
                f64::NAN
            })
            .collect(),
    )
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

fn render_derived_output_recipe(
    request: &DerivedBatchRequest,
    recipe: DerivedRecipe,
    grid_ref: &rustwx_core::LatLonGrid,
    projection: Option<&rustwx_core::GridProjection>,
    projected_ref: &ProjectedMap,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    model: ModelId,
    computed: &DerivedComputedFields,
    lane_fetch_keys: Vec<String>,
) -> Result<DerivedRenderedRecipe, io::Error> {
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
        request.contour_mode,
        request.native_fill_level_multiplier,
    )
    .map_err(thread_render_error)?;
    let HrrrDerivedLiveArtifact {
        recipe_slug,
        title: _,
        field: _,
        request: mut render_request,
    } = render_artifact;
    let title = derived_title_for_request(request, recipe.title());
    render_request.title = Some(title.clone());
    if let Some(overlay) = request.custom_poi_overlay.as_ref() {
        apply_custom_poi_overlay(
            &mut render_request,
            overlay,
            request.domain.bounds,
            &grid_ref.lat_deg,
            &grid_ref.lon_deg,
            projection,
        )
        .map_err(thread_render_error)?;
    }
    if let Some(overlay) = request.place_label_overlay.as_ref() {
        crate::apply_place_label_overlay_with_density_styling(
            &mut render_request,
            overlay,
            &request.domain,
            &grid_ref.lat_deg,
            &grid_ref.lon_deg,
            projection,
        )
        .map_err(thread_render_error)?;
    }
    let save_timing =
        save_png_profile_with_options(&render_request, &output_path, &request.png_write_options())
            .map_err(thread_render_error)?;
    let render_ms = render_start.elapsed().as_millis();
    let content_identity =
        artifact_identity_from_path(&output_path).map_err(thread_render_error)?;
    Ok(DerivedRenderedRecipe {
        recipe_slug,
        title,
        source_route: derived_compute_source_route(recipe, request.source_mode).ok_or_else(
            || {
                io::Error::other(format!(
                    "missing compute source route for '{}'",
                    recipe.slug()
                ))
            },
        )?,
        output_path,
        content_identity,
        input_fetch_keys: lane_fetch_keys,
        timing: DerivedRecipeTiming {
            render_to_image_ms: save_timing.png_timing.render_to_image_ms,
            data_layer_draw_ms: derived_data_layer_draw_ms(&save_timing.png_timing.image_timing),
            overlay_draw_ms: derived_overlay_draw_ms(&save_timing.png_timing.image_timing),
            render_state_prep_ms: save_timing.state_timing.state_prep_ms,
            png_encode_ms: save_timing.png_timing.png_encode_ms,
            file_write_ms: save_timing.file_write_ms,
            render_ms,
            total_ms: render_ms,
            state_timing: save_timing.state_timing,
            image_timing: save_timing.png_timing.image_timing,
        },
    })
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

    struct TestPressureFields {
        pressure_levels_hpa: Vec<f64>,
        pressure_3d_pa: Option<Vec<f64>>,
        temperature_c_3d: Vec<f64>,
    }

    impl PressureFieldSet for TestPressureFields {
        fn pressure_levels_hpa(&self) -> &[f64] {
            &self.pressure_levels_hpa
        }

        fn pressure_3d_pa(&self) -> Option<&[f64]> {
            self.pressure_3d_pa.as_deref()
        }

        fn temperature_c_3d(&self) -> &[f64] {
            &self.temperature_c_3d
        }

        fn qvapor_kgkg_3d(&self) -> &[f64] {
            &[]
        }

        fn u_ms_3d(&self) -> &[f64] {
            &[]
        }

        fn v_ms_3d(&self) -> &[f64] {
            &[]
        }

        fn gh_m_3d(&self) -> &[f64] {
            &[]
        }
    }

    fn sample_native_contour_grid() -> rustwx_core::LatLonGrid {
        rustwx_core::LatLonGrid::new(
            rustwx_core::GridShape::new(3, 3).unwrap(),
            vec![35.0, 35.0, 35.0, 36.0, 36.0, 36.0, 37.0, 37.0, 37.0],
            vec![
                -99.0, -98.0, -97.0, -99.0, -98.0, -97.0, -99.0, -98.0, -97.0,
            ],
        )
        .unwrap()
    }

    fn sample_projected_map() -> ProjectedMap {
        ProjectedMap {
            projected_x: vec![-1.0, 0.0, 1.0, -1.0, 0.0, 1.0, -1.0, 0.0, 1.0],
            projected_y: vec![0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
            extent: ProjectedExtent {
                x_min: -1.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 2.0,
            },
            lines: Vec::new(),
            polygons: Vec::new(),
        }
    }

    fn sample_fire_weather_computed_fields() -> DerivedComputedFields {
        DerivedComputedFields {
            vpd_2m_hpa: Some(vec![0.5, 1.5, 3.0, 2.0, 4.0, 6.0, 5.0, 8.0, 10.0]),
            dewpoint_depression_2m_c: Some(vec![1.0, 3.0, 6.0, 4.0, 8.0, 12.0, 10.0, 16.0, 20.0]),
            wetbulb_2m_c: Some(vec![-6.0, -3.0, 0.0, 2.0, 5.0, 8.0, 11.0, 15.0, 19.0]),
            fire_weather_composite: Some(vec![8.0, 15.0, 25.0, 20.0, 35.0, 55.0, 50.0, 75.0, 92.0]),
            ..DerivedComputedFields::default()
        }
    }

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
            DerivedRecipe::parse("2m_vpd").unwrap(),
            DerivedRecipe::Vpd2m
        );
        assert_eq!(
            DerivedRecipe::parse("vapor_pressure_deficit_2m").unwrap(),
            DerivedRecipe::Vpd2m
        );
        assert_eq!(
            DerivedRecipe::parse("2m_dewpoint_depression").unwrap(),
            DerivedRecipe::DewpointDepression2m
        );
        assert_eq!(
            DerivedRecipe::parse("wet_bulb_2m").unwrap(),
            DerivedRecipe::Wetbulb2m
        );
        assert_eq!(
            DerivedRecipe::parse("fire_weather").unwrap(),
            DerivedRecipe::FireWeatherComposite
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
    fn fire_weather_family_is_supported_surface_only_inventory() {
        let expected = [
            ("vpd_2m", "2 m Vapor Pressure Deficit", DerivedRecipe::Vpd2m),
            (
                "dewpoint_depression_2m",
                "2 m Dewpoint Depression",
                DerivedRecipe::DewpointDepression2m,
            ),
            (
                "wetbulb_2m",
                "2 m Wet-Bulb Temperature",
                DerivedRecipe::Wetbulb2m,
            ),
            (
                "fire_weather_composite",
                "Fire Weather Composite",
                DerivedRecipe::FireWeatherComposite,
            ),
        ];

        for (slug, title, parsed) in expected {
            let recipe = supported_derived_recipe_inventory()
                .iter()
                .find(|recipe| recipe.slug == slug)
                .unwrap_or_else(|| panic!("{slug} inventory entry should exist"));
            assert_eq!(recipe.title, title);
            assert!(!recipe.experimental);
            assert!(!recipe.heavy);
            assert_eq!(DerivedRecipe::parse(slug).unwrap(), parsed);
        }

        let requirements = DerivedRequirements::from_recipes(&[
            DerivedRecipe::Vpd2m,
            DerivedRecipe::DewpointDepression2m,
            DerivedRecipe::Wetbulb2m,
            DerivedRecipe::FireWeatherComposite,
        ]);
        assert!(requirements.surface_thermo);
        assert!(!requirements.surface_winds);
        assert!(!requirements.needs_volume());
        assert!(!requirements.needs_height_agl());
        assert!(!requirements.needs_grid_spacing());
    }

    #[test]
    fn native_contour_config_covers_multiple_real_products() {
        for recipe in [
            DerivedRecipe::StpFixed,
            DerivedRecipe::Sbcape,
            DerivedRecipe::Mlcape,
            DerivedRecipe::Srh01km,
            DerivedRecipe::Srh03km,
            DerivedRecipe::Ehi01km,
            DerivedRecipe::Ehi03km,
        ] {
            let config = native_contour_product_config(recipe)
                .unwrap_or_else(|| panic!("expected native contour config for {}", recipe.slug()));
            assert!(
                !config.line_levels.is_empty(),
                "{} should define contour lines",
                recipe.slug()
            );
        }
        assert!(native_contour_product_config(DerivedRecipe::LiftedIndex).is_none());
    }

    #[test]
    fn wetbulb_uses_raster_temperature_scale_without_contour_promotion() {
        assert!(native_contour_product_config(DerivedRecipe::Wetbulb2m).is_none());

        let grid = sample_native_contour_grid();
        let projected = sample_projected_map();
        let computed = sample_fire_weather_computed_fields();
        let artifact = build_render_artifact_with_contour_mode(
            DerivedRecipe::Wetbulb2m,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            &computed,
            NativeContourRenderMode::Automatic,
            1,
        )
        .unwrap();

        assert!(artifact.request.projected_data_polygons.is_empty());
        assert!(
            artifact
                .request
                .field
                .values
                .iter()
                .any(|value| value.is_finite())
        );
        let ColorScale::Discrete(scale) = artifact.request.scale else {
            panic!("wet-bulb scale should be discrete");
        };
        assert_eq!(scale.extend, ExtendMode::Both);
        assert_eq!(scale.levels[0], -50.0);
        assert_eq!(scale.levels[1] - scale.levels[0], 0.5);
        assert!(scale.levels.contains(&0.0));
        assert!(scale.levels.contains(&40.0));
        assert_ne!(scale.levels[1] - scale.levels[0], 5.0);
    }

    #[test]
    fn automatic_contour_mode_keeps_native_products_rasterized() {
        let grid = sample_native_contour_grid();
        let projected = sample_projected_map();
        let values = vec![
            0.0, 500.0, 1000.0, 250.0, 1250.0, 2250.0, 750.0, 2000.0, 3500.0,
        ];

        let automatic = build_native_render_artifact(
            DerivedRecipe::Sbcape,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            values.clone(),
            NativeContourRenderMode::Automatic,
            1,
        )
        .unwrap();
        assert!(automatic.request.projected_data_polygons.is_empty());
        assert!(
            automatic
                .request
                .field
                .values
                .iter()
                .any(|value| value.is_finite())
        );

        let legacy = build_native_render_artifact(
            DerivedRecipe::Sbcape,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            values,
            NativeContourRenderMode::LegacyRaster,
            1,
        )
        .unwrap();
        assert!(legacy.request.projected_data_polygons.is_empty());
        assert!(
            legacy
                .request
                .field
                .values
                .iter()
                .any(|value| value.is_finite())
        );
    }

    #[test]
    fn experimental_contour_mode_can_promote_nonconfigured_derived_products() {
        let grid = sample_native_contour_grid();
        let projected = sample_projected_map();
        let values = vec![-9.0, -6.0, -3.0, -2.0, 0.0, 2.0, 4.0, 7.0, 10.0];

        let automatic = build_native_render_artifact(
            DerivedRecipe::LiftedIndex,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            values.clone(),
            NativeContourRenderMode::Automatic,
            1,
        )
        .unwrap();
        assert!(automatic.request.projected_data_polygons.is_empty());

        let experimental = build_native_render_artifact(
            DerivedRecipe::LiftedIndex,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            values,
            NativeContourRenderMode::ExperimentalAllProjected,
            1,
        )
        .unwrap();
        assert!(!experimental.request.projected_data_polygons.is_empty());
        assert!(
            experimental
                .request
                .field
                .values
                .iter()
                .all(|value| value.is_nan())
        );
    }

    #[test]
    fn signature_contour_mode_keeps_selected_products_rasterized() {
        let grid = sample_native_contour_grid();
        let projected = sample_projected_map();
        let values = vec![-9.0, -6.0, -3.0, -2.0, 0.0, 2.0, 4.0, 7.0, 10.0];

        let signature = build_native_render_artifact(
            DerivedRecipe::LiftedIndex,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            values,
            NativeContourRenderMode::Signature,
            1,
        )
        .unwrap();
        assert!(signature.request.projected_data_polygons.is_empty());
        assert!(
            signature
                .request
                .field
                .values
                .iter()
                .any(|value| value.is_finite())
        );
    }

    #[test]
    fn signature_contour_mode_keeps_non_signature_products_rasterized() {
        let grid = sample_native_contour_grid();
        let projected = sample_projected_map();
        let values = vec![-2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 1.0, 0.0, -1.0];

        let signature = build_native_render_artifact(
            DerivedRecipe::Mucin,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            values,
            NativeContourRenderMode::Signature,
            1,
        )
        .unwrap();
        assert!(signature.request.projected_data_polygons.is_empty());
        assert!(
            signature
                .request
                .field
                .values
                .iter()
                .any(|value| value.is_finite())
        );
    }

    #[test]
    fn fire_weather_family_render_artifacts_build_and_stay_rasterized() {
        let grid = sample_native_contour_grid();
        let projected = sample_projected_map();
        let computed = sample_fire_weather_computed_fields();

        for recipe in [
            DerivedRecipe::Vpd2m,
            DerivedRecipe::DewpointDepression2m,
            DerivedRecipe::Wetbulb2m,
            DerivedRecipe::FireWeatherComposite,
        ] {
            let artifact = build_render_artifact_with_contour_mode(
                recipe,
                &grid,
                &projected,
                "20260414",
                23,
                0,
                SourceId::Nomads,
                ModelId::Hrrr,
                1200,
                900,
                &computed,
                NativeContourRenderMode::LegacyRaster,
                1,
            )
            .unwrap();
            assert_eq!(artifact.request.title.as_deref(), Some(recipe.title()));
            assert!(artifact.request.projected_data_polygons.is_empty());
            assert!(artifact.field.values.iter().any(|value| value.is_finite()));
        }

        let signature = build_render_artifact_with_contour_mode(
            DerivedRecipe::FireWeatherComposite,
            &grid,
            &projected,
            "20260414",
            23,
            0,
            SourceId::Nomads,
            ModelId::Hrrr,
            1200,
            900,
            &computed,
            NativeContourRenderMode::Signature,
            1,
        )
        .unwrap();
        assert!(signature.request.projected_data_polygons.is_empty());
        assert!(
            signature
                .request
                .field
                .values
                .iter()
                .any(|value| value.is_finite())
        );
    }

    #[test]
    fn ecape_inventory_entries_are_marked_heavy() {
        let sbecape = supported_derived_recipe_inventory()
            .iter()
            .find(|recipe| recipe.slug == "sbecape")
            .expect("sbecape inventory entry should exist");
        assert!(sbecape.heavy);
        assert!(!sbecape.experimental);

        let ecape_scp = supported_derived_recipe_inventory()
            .iter()
            .find(|recipe| recipe.slug == "ecape_scp")
            .expect("ecape_scp inventory entry should exist");
        assert!(ecape_scp.heavy);
        assert!(ecape_scp.experimental);
        assert_eq!(
            DerivedRecipe::parse("ecape_scp").unwrap(),
            DerivedRecipe::EcapeScp
        );

        let native_ratio = supported_derived_recipe_inventory()
            .iter()
            .find(|recipe| recipe.slug == "sb_ecape_native_cape_ratio")
            .expect("native ECAPE/CAPE ratio inventory entry should exist");
        assert!(native_ratio.heavy);
        assert!(native_ratio.experimental);
        assert_eq!(
            DerivedRecipe::parse("sb_ecape_native_cape_ratio").unwrap(),
            DerivedRecipe::SbEcapeNativeCapeRatio
        );
    }

    #[test]
    fn canonical_mode_keeps_all_supported_recipes_on_canonical_path() {
        let recipes = vec![
            DerivedRecipe::Sbcape,
            DerivedRecipe::LiftedIndex,
            DerivedRecipe::BulkShear06km,
        ];
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Hrrr,
            &recipes,
            ProductSourceMode::Canonical,
            None,
        )
        .unwrap();
        assert_eq!(planned.output_recipes, recipes);
        assert_eq!(planned.compute_recipes, recipes);
        assert!(planned.native_routes.is_empty());
        assert!(planned.blockers.is_empty());
    }

    #[test]
    fn gfs_canonical_mode_uses_exact_native_thermo_routes() {
        let recipes = vec![DerivedRecipe::Sbcape, DerivedRecipe::Mlcape];
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Gfs,
            &recipes,
            ProductSourceMode::Canonical,
            None,
        )
        .unwrap();

        assert_eq!(planned.output_recipes, recipes);
        assert_eq!(planned.compute_recipes, vec![DerivedRecipe::Mlcape]);
        assert_eq!(planned.native_routes.len(), 1);
        assert_eq!(planned.native_routes[0].recipe, DerivedRecipe::Sbcape);
        assert_eq!(
            planned.native_routes[0].source_route,
            ProductSourceRoute::NativeExact
        );
    }

    #[test]
    fn canonical_mode_routes_ecape_recipes_through_heavy_path() {
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Hrrr,
            &[DerivedRecipe::Sbecape, DerivedRecipe::EcapeScp],
            ProductSourceMode::Canonical,
            None,
        )
        .unwrap();
        assert_eq!(
            planned.output_recipes,
            vec![DerivedRecipe::Sbecape, DerivedRecipe::EcapeScp]
        );
        assert!(planned.compute_recipes.is_empty());
        assert_eq!(
            planned.heavy_recipes,
            vec![DerivedRecipe::Sbecape, DerivedRecipe::EcapeScp]
        );
        assert!(planned.native_routes.is_empty());
        assert!(planned.blockers.is_empty());
    }

    #[test]
    fn fastest_mode_uses_native_exact_and_blocks_non_fast_recipes() {
        let recipes = vec![DerivedRecipe::Sbcape, DerivedRecipe::BulkShear06km];
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Hrrr,
            &recipes,
            ProductSourceMode::Fastest,
            None,
        )
        .unwrap();
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
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Gfs,
            &[DerivedRecipe::Mlcape],
            ProductSourceMode::Fastest,
            None,
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
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Hrrr,
            &[DerivedRecipe::HeatIndex2m],
            ProductSourceMode::Fastest,
            None,
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
    fn fastest_mode_blocks_heavy_ecape_recipes() {
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::Hrrr,
            &[DerivedRecipe::Sbecape],
            ProductSourceMode::Fastest,
            None,
        )
        .unwrap();
        assert!(planned.output_recipes.is_empty());
        assert!(planned.compute_recipes.is_empty());
        assert!(planned.heavy_recipes.is_empty());
        assert_eq!(planned.blockers.len(), 1);
        assert!(
            planned.blockers[0]
                .reason
                .contains("cropped heavy ECAPE path")
        );
    }

    #[test]
    fn wrf_gdex_canonical_mode_prefers_native_d612005_2d_recipes() {
        let recipes = vec![
            DerivedRecipe::Sbcape,
            DerivedRecipe::BulkShear06km,
            DerivedRecipe::Srh03km,
            DerivedRecipe::LiftedIndex,
        ];
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::WrfGdex,
            &recipes,
            ProductSourceMode::Canonical,
            Some("d612005-future2d"),
        )
        .unwrap();

        assert_eq!(planned.output_recipes, recipes);
        assert_eq!(planned.compute_recipes, vec![DerivedRecipe::LiftedIndex]);
        assert_eq!(planned.native_routes.len(), 3);
        assert_eq!(
            planned.native_routes[0].candidate.fetch_product,
            "d612005-future2d"
        );
        assert_eq!(
            planned
                .native_routes
                .iter()
                .map(|route| route.recipe)
                .collect::<Vec<_>>(),
            vec![
                DerivedRecipe::Sbcape,
                DerivedRecipe::BulkShear06km,
                DerivedRecipe::Srh03km,
            ]
        );
    }

    #[test]
    fn wrf_gdex_non_d612005_products_fall_back_to_compute() {
        let recipes = vec![DerivedRecipe::Sbcape, DerivedRecipe::BulkShear06km];
        let planned = plan_native_thermo_routes_with_surface_product(
            ModelId::WrfGdex,
            &recipes,
            ProductSourceMode::Canonical,
            Some("d010047"),
        )
        .unwrap();

        assert_eq!(planned.output_recipes, recipes);
        assert_eq!(planned.compute_recipes, recipes);
        assert!(planned.native_routes.is_empty());
        assert!(planned.blockers.is_empty());
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
            allow_large_heavy_domain: false,
            contour_mode: NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: OUTPUT_WIDTH,
            output_height: OUTPUT_HEIGHT,
            png_compression: PngCompressionMode::Default,
            custom_poi_overlay: None,
            place_label_overlay: None,
        };
        let planned = plan_native_thermo_routes_with_surface_product(
            request.model,
            &[DerivedRecipe::Sbcape],
            request.source_mode,
            request.surface_product_override.as_deref(),
        )
        .unwrap();

        let latest = resolve_derived_run(
            &request,
            &planned.compute_recipes,
            &planned.heavy_recipes,
            &planned.native_routes,
        )
        .unwrap();

        assert_eq!(latest.model, ModelId::Hrrr);
        assert_eq!(latest.cycle.date_yyyymmdd, "20260418");
        assert_eq!(latest.cycle.hour_utc, 12);
        assert_eq!(latest.source, SourceId::Aws);
    }

    #[test]
    fn pressure_level_slice_or_interp_prefers_exact_isobaric_slice() {
        let pressure = TestPressureFields {
            pressure_levels_hpa: vec![850.0, 700.0],
            pressure_3d_pa: None,
            temperature_c_3d: vec![12.0, 13.0, 1.0, 2.0],
        };

        let slice = pressure_level_slice_or_interp(&pressure, &pressure.temperature_c_3d, 700.0, 2)
            .expect("exact 700 mb slice should be available");

        assert_eq!(slice, vec![1.0, 2.0]);
    }

    #[test]
    fn pressure_level_slice_or_interp_interpolates_native_pressure_columns() {
        let pressure = TestPressureFields {
            pressure_levels_hpa: vec![900.0, 600.0],
            pressure_3d_pa: Some(vec![90000.0, 90000.0, 60000.0, 60000.0]),
            temperature_c_3d: vec![20.0, 24.0, 0.0, 4.0],
        };

        let slice = pressure_level_slice_or_interp(&pressure, &pressure.temperature_c_3d, 700.0, 2)
            .expect("native-pressure interpolation should succeed");

        let log_frac = (700.0_f64.ln() - 900.0_f64.ln()) / (600.0_f64.ln() - 900.0_f64.ln());
        let expected0 = 20.0 + log_frac * (0.0 - 20.0);
        let expected1 = 24.0 + log_frac * (4.0 - 24.0);

        assert!((slice[0] - expected0).abs() < 1.0e-6);
        assert!((slice[1] - expected1).abs() < 1.0e-6);
    }
}
