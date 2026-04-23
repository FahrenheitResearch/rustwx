use crate::cache::ensure_dir;
use crate::derived::compute_derived_query_field;
use crate::direct::load_single_direct_sampled_field_from_latest;
use crate::ecape::compute_ecape8_panel_fields_with_prepared_volume;
use crate::gridded::{
    LoadedModelTimestep, load_model_timestep_from_parts, load_model_timestep_from_parts_cropped,
    prepare_heavy_volume,
};
use crate::publication::{atomic_write_bytes, atomic_write_json, fetch_key};
use chrono::{Duration, NaiveDate, Utc};
use rustwx_calc::{
    GridShape as CalcGridShape, SurfaceInputs, compute_2m_relative_humidity,
    compute_relative_humidity_from_pressure_temperature_and_mixing_ratio, compute_surface_thermo,
};
use rustwx_core::{GridProjection, ModelId, SourceId};
use rustwx_render::WeatherProduct;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const MESOCONVECTIVE_V1_CHANNELS: &[MlExportChannel] = &[
    MlExportChannel::T2m,
    MlExportChannel::D2m,
    MlExportChannel::Q2m,
    MlExportChannel::U10,
    MlExportChannel::V10,
    MlExportChannel::WindSpeed,
    MlExportChannel::WindDirection,
    MlExportChannel::RelativeHumidity2m,
    MlExportChannel::CapeCompat,
    MlExportChannel::Refc,
];

const HYBRID_COLUMN_V1_CHANNELS: &[MlExportChannel] = &[
    MlExportChannel::T2m,
    MlExportChannel::D2m,
    MlExportChannel::Q2m,
    MlExportChannel::U10,
    MlExportChannel::V10,
    MlExportChannel::WindSpeed,
    MlExportChannel::WindDirection,
    MlExportChannel::RelativeHumidity2m,
    MlExportChannel::Mslp,
    MlExportChannel::Terrain,
    MlExportChannel::Refc,
    MlExportChannel::T925,
    MlExportChannel::T850,
    MlExportChannel::T700,
    MlExportChannel::Rh925,
    MlExportChannel::Rh850,
    MlExportChannel::Rh700,
    MlExportChannel::Z925,
    MlExportChannel::Z850,
    MlExportChannel::Z700,
    MlExportChannel::U925,
    MlExportChannel::U850,
    MlExportChannel::U700,
    MlExportChannel::V925,
    MlExportChannel::V850,
    MlExportChannel::V700,
    MlExportChannel::Sbcape,
    MlExportChannel::Sbcin,
    MlExportChannel::Mlcape,
    MlExportChannel::Mlcin,
    MlExportChannel::Mucape,
    MlExportChannel::Srh01,
    MlExportChannel::Srh03,
    MlExportChannel::Shear06,
    MlExportChannel::Sblcl,
    MlExportChannel::Sbecape,
    MlExportChannel::Mlecape,
    MlExportChannel::Muecape,
];

const HYBRID_COLUMN_V1_RRFS_CHANNELS: &[MlExportChannel] = &[
    MlExportChannel::T2m,
    MlExportChannel::D2m,
    MlExportChannel::Q2m,
    MlExportChannel::U10,
    MlExportChannel::V10,
    MlExportChannel::WindSpeed,
    MlExportChannel::WindDirection,
    MlExportChannel::RelativeHumidity2m,
    MlExportChannel::Terrain,
    MlExportChannel::T925,
    MlExportChannel::T850,
    MlExportChannel::T700,
    MlExportChannel::Rh925,
    MlExportChannel::Rh850,
    MlExportChannel::Rh700,
    MlExportChannel::Z925,
    MlExportChannel::Z850,
    MlExportChannel::Z700,
    MlExportChannel::U925,
    MlExportChannel::U850,
    MlExportChannel::U700,
    MlExportChannel::V925,
    MlExportChannel::V850,
    MlExportChannel::V700,
    MlExportChannel::Sbcape,
    MlExportChannel::Sbcin,
    MlExportChannel::Mlcape,
    MlExportChannel::Mlcin,
    MlExportChannel::Mucape,
    MlExportChannel::Srh01,
    MlExportChannel::Srh03,
    MlExportChannel::Shear06,
    MlExportChannel::Sblcl,
    MlExportChannel::Sbecape,
    MlExportChannel::Mlecape,
    MlExportChannel::Muecape,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MlDatasetExportFormat {
    NpyDirectory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MlDatasetSplit {
    #[default]
    Train,
    Validation,
    Test,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MlChannelPreset {
    MesoconvectiveV1,
    HybridColumnV1,
}

impl MlChannelPreset {
    pub fn channels_for_model(self, model: ModelId) -> Vec<MlExportChannel> {
        match self {
            Self::MesoconvectiveV1 => MESOCONVECTIVE_V1_CHANNELS.to_vec(),
            Self::HybridColumnV1 => match model {
                ModelId::Hrrr => HYBRID_COLUMN_V1_CHANNELS.to_vec(),
                ModelId::RrfsA => HYBRID_COLUMN_V1_RRFS_CHANNELS.to_vec(),
                _ => Vec::new(),
            },
        }
    }

    pub fn manifest_name(self) -> &'static str {
        match self {
            Self::MesoconvectiveV1 => "mesoconvective_v1",
            Self::HybridColumnV1 => "hybrid_column_v1",
        }
    }

    pub fn compatibility_mode(self) -> Option<&'static str> {
        match self {
            Self::MesoconvectiveV1 => Some("wxtrain_legacy_cape_alias"),
            Self::HybridColumnV1 => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MlOptionalChannelGroup {
    Ecape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MlChannelKind {
    DecodedSurface,
    DecodedPressureLevel,
    DirectField,
    Derived,
    HeavyDerived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MlExportChannel {
    T2m,
    D2m,
    Q2m,
    U10,
    V10,
    WindSpeed,
    WindDirection,
    RelativeHumidity2m,
    CapeCompat,
    Refc,
    Mslp,
    Terrain,
    T925,
    T850,
    T700,
    Rh925,
    Rh850,
    Rh700,
    Z925,
    Z850,
    Z700,
    U925,
    U850,
    U700,
    V925,
    V850,
    V700,
    Sbcape,
    Sbcin,
    Mlcape,
    Mlcin,
    Mucape,
    Srh01,
    Srh03,
    Shear06,
    Sblcl,
    Sbecape,
    Mlecape,
    Muecape,
}

impl MlExportChannel {
    pub fn name(self) -> &'static str {
        match self {
            Self::T2m => "t2m",
            Self::D2m => "d2m",
            Self::Q2m => "q2m",
            Self::U10 => "u10",
            Self::V10 => "v10",
            Self::WindSpeed => "wind_speed",
            Self::WindDirection => "wind_direction",
            Self::RelativeHumidity2m => "relative_humidity",
            Self::CapeCompat => "cape",
            Self::Refc => "refc",
            Self::Mslp => "mslp",
            Self::Terrain => "terrain",
            Self::T925 => "t925",
            Self::T850 => "t850",
            Self::T700 => "t700",
            Self::Rh925 => "rh925",
            Self::Rh850 => "rh850",
            Self::Rh700 => "rh700",
            Self::Z925 => "z925",
            Self::Z850 => "z850",
            Self::Z700 => "z700",
            Self::U925 => "u925",
            Self::U850 => "u850",
            Self::U700 => "u700",
            Self::V925 => "v925",
            Self::V850 => "v850",
            Self::V700 => "v700",
            Self::Sbcape => "sbcape",
            Self::Sbcin => "sbcin",
            Self::Mlcape => "mlcape",
            Self::Mlcin => "mlcin",
            Self::Mucape => "mucape",
            Self::Srh01 => "srh01",
            Self::Srh03 => "srh03",
            Self::Shear06 => "shear06",
            Self::Sblcl => "sblcl",
            Self::Sbecape => "sbecape",
            Self::Mlecape => "mlecape",
            Self::Muecape => "muecape",
        }
    }

    pub fn units(self) -> &'static str {
        match self {
            Self::T2m | Self::D2m | Self::T925 | Self::T850 | Self::T700 => "K",
            Self::Q2m => "kg/kg",
            Self::WindSpeed | Self::U10 | Self::V10 => "m/s",
            Self::Terrain | Self::Z925 | Self::Z850 | Self::Z700 | Self::Sblcl => "m",
            Self::U925 | Self::U850 | Self::U700 | Self::V925 | Self::V850 | Self::V700 => "m/s",
            Self::WindDirection => "deg",
            Self::RelativeHumidity2m | Self::Rh925 | Self::Rh850 | Self::Rh700 => "%",
            Self::CapeCompat
            | Self::Sbcape
            | Self::Sbcin
            | Self::Mlcape
            | Self::Mlcin
            | Self::Mucape
            | Self::Sbecape
            | Self::Mlecape
            | Self::Muecape => "J/kg",
            Self::Refc => "dBZ",
            Self::Mslp => "Pa",
            Self::Srh01 | Self::Srh03 => "m^2/s^2",
            Self::Shear06 => "kt",
        }
    }

    pub fn level_label(self) -> &'static str {
        match self {
            Self::T2m
            | Self::D2m
            | Self::Q2m
            | Self::RelativeHumidity2m
            | Self::U10
            | Self::V10
            | Self::WindSpeed
            | Self::WindDirection
            | Self::Mslp
            | Self::Terrain => "surface",
            Self::Refc => "entire_atmosphere",
            Self::CapeCompat
            | Self::Sbcape
            | Self::Sbcin
            | Self::Sblcl
            | Self::Mlcape
            | Self::Mlcin
            | Self::Mucape
            | Self::Srh01
            | Self::Srh03
            | Self::Shear06 => "derived_surface_based",
            Self::Sbecape | Self::Mlecape | Self::Muecape => "heavy_derived_surface_based",
            Self::T925 | Self::Rh925 | Self::Z925 | Self::U925 | Self::V925 => "925 hPa",
            Self::T850 | Self::Rh850 | Self::Z850 | Self::U850 | Self::V850 => "850 hPa",
            Self::T700 | Self::Rh700 | Self::Z700 | Self::U700 | Self::V700 => "700 hPa",
        }
    }

    pub fn kind(self) -> MlChannelKind {
        match self {
            Self::T2m | Self::Q2m | Self::U10 | Self::V10 | Self::Terrain => {
                MlChannelKind::DecodedSurface
            }
            Self::D2m | Self::WindSpeed | Self::WindDirection | Self::RelativeHumidity2m => {
                MlChannelKind::Derived
            }
            Self::Refc | Self::Mslp => MlChannelKind::DirectField,
            Self::T925
            | Self::T850
            | Self::T700
            | Self::Rh925
            | Self::Rh850
            | Self::Rh700
            | Self::Z925
            | Self::Z850
            | Self::Z700
            | Self::U925
            | Self::U850
            | Self::U700
            | Self::V925
            | Self::V850
            | Self::V700 => MlChannelKind::DecodedPressureLevel,
            Self::CapeCompat
            | Self::Sbcape
            | Self::Sbcin
            | Self::Mlcape
            | Self::Mlcin
            | Self::Mucape
            | Self::Srh01
            | Self::Srh03
            | Self::Shear06
            | Self::Sblcl => MlChannelKind::Derived,
            Self::Sbecape | Self::Mlecape | Self::Muecape => MlChannelKind::HeavyDerived,
        }
    }

    pub fn product_identity(self) -> &'static str {
        match self {
            Self::T2m => "surface:t2_k",
            Self::D2m => "surface_derived:dewpoint_2m_k",
            Self::Q2m => "surface:q2_kgkg",
            Self::U10 => "surface:u10_ms",
            Self::V10 => "surface:v10_ms",
            Self::WindSpeed => "surface_derived:wind_speed_10m_ms",
            Self::WindDirection => "surface_derived:wind_direction_10m_deg",
            Self::RelativeHumidity2m => "surface_derived:relative_humidity_2m_pct",
            Self::CapeCompat | Self::Sbcape => "derived_recipe:sbcape",
            Self::Sbcin => "derived_recipe:sbcin",
            Self::Mlcape => "derived_recipe:mlcape",
            Self::Mlcin => "derived_recipe:mlcin",
            Self::Mucape => "derived_recipe:mucape",
            Self::Srh01 => "derived_recipe:srh_0_1km",
            Self::Srh03 => "derived_recipe:srh_0_3km",
            Self::Shear06 => "derived_recipe:bulk_shear_0_6km",
            Self::Sblcl => "derived_recipe:sblcl",
            Self::Refc => "direct_recipe:composite_reflectivity",
            Self::Mslp => "direct_recipe:mslp_10m_winds",
            Self::Terrain => "surface:terrain_orography_m",
            Self::T925 => "pressure_level:temperature_925_k",
            Self::T850 => "pressure_level:temperature_850_k",
            Self::T700 => "pressure_level:temperature_700_k",
            Self::Rh925 => "pressure_level:relative_humidity_925_pct",
            Self::Rh850 => "pressure_level:relative_humidity_850_pct",
            Self::Rh700 => "pressure_level:relative_humidity_700_pct",
            Self::Z925 => "pressure_level:geopotential_height_925_m",
            Self::Z850 => "pressure_level:geopotential_height_850_m",
            Self::Z700 => "pressure_level:geopotential_height_700_m",
            Self::U925 => "pressure_level:u_wind_925_ms",
            Self::U850 => "pressure_level:u_wind_850_ms",
            Self::U700 => "pressure_level:u_wind_700_ms",
            Self::V925 => "pressure_level:v_wind_925_ms",
            Self::V850 => "pressure_level:v_wind_850_ms",
            Self::V700 => "pressure_level:v_wind_700_ms",
            Self::Sbecape => "heavy_derived:sbecape",
            Self::Mlecape => "heavy_derived:mlecape",
            Self::Muecape => "heavy_derived:muecape",
        }
    }

    pub fn canonical_name(self) -> &'static str {
        match self {
            Self::CapeCompat => "sbcape",
            _ => self.name(),
        }
    }

    pub fn level_hpa(self) -> Option<f64> {
        match self {
            Self::T925 | Self::Rh925 | Self::Z925 | Self::U925 | Self::V925 => Some(925.0),
            Self::T850 | Self::Rh850 | Self::Z850 | Self::U850 | Self::V850 => Some(850.0),
            Self::T700 | Self::Rh700 | Self::Z700 | Self::U700 | Self::V700 => Some(700.0),
            _ => None,
        }
    }

    pub fn height_m_agl(self) -> Option<f64> {
        match self {
            Self::D2m | Self::T2m | Self::Q2m | Self::RelativeHumidity2m => Some(2.0),
            Self::U10 | Self::V10 | Self::WindSpeed | Self::WindDirection => Some(10.0),
            _ => None,
        }
    }

    pub fn compatibility_alias_of(self) -> Option<&'static str> {
        match self {
            Self::CapeCompat => Some("sbcape"),
            _ => None,
        }
    }

    pub fn experimental(self) -> bool {
        false
    }

    pub fn proxy(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlChannelCatalogEntry {
    pub name: String,
    pub canonical_name: String,
    pub units: String,
    pub shape: Vec<usize>,
    pub level: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level_hpa: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height_m_agl: Option<f64>,
    pub kind: MlChannelKind,
    pub experimental: bool,
    pub proxy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_alias_of: Option<String>,
    pub provenance_product_identity: String,
    pub provenance_route: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlDatasetManifest {
    pub dataset_name: String,
    pub generated_at: String,
    pub format: MlDatasetExportFormat,
    pub shard_count: usize,
    pub channels: Vec<String>,
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channel_metadata: Vec<MlChannelCatalogEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub excluded_optional_groups: Vec<MlOptionalChannelGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_domain_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MlDatasetSampleRef {
    pub sample_id: String,
    pub source: String,
    pub relative_dir: String,
    pub channel_count: usize,
    pub shard_index: usize,
    pub split: MlDatasetSplit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MlDatasetSplitCounts {
    pub train: usize,
    pub validation: usize,
    pub test: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlDatasetBuildManifest {
    pub dataset_name: String,
    pub generated_at: String,
    pub format: MlDatasetExportFormat,
    pub shard_count: usize,
    pub sample_count: usize,
    pub total_channel_count: usize,
    pub split_counts: MlDatasetSplitCounts,
    pub samples: Vec<MlDatasetSampleRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlChannelStats {
    pub min: f64,
    pub mean: f64,
    pub max: f64,
    #[serde(default)]
    pub std: f64,
    #[serde(default)]
    pub count: usize,
    #[serde(default)]
    pub nan_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MlChannelProvenance {
    pub route: String,
    pub product_identity: String,
    pub kind: MlChannelKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_selector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_bundle: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_fetch_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_fetch_urls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlSampleChannelArtifact {
    pub message_no: u64,
    pub name: String,
    pub canonical_name: String,
    pub level: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level_hpa: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height_m_agl: Option<f64>,
    pub units: String,
    pub width: usize,
    pub height: usize,
    pub missing_count: usize,
    pub data_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<MlChannelStats>,
    pub shape: Vec<usize>,
    pub experimental: bool,
    pub proxy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_alias_of: Option<String>,
    pub provenance: MlChannelProvenance,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlGridMetadata {
    pub shape: Vec<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<GridProjection>,
    pub grid_domain_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approx_dx_km: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approx_dy_km: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlSampleManifest {
    pub dataset_name: String,
    pub sample_id: String,
    pub generated_at: String,
    pub format: MlDatasetExportFormat,
    pub source: String,
    pub channel_count: usize,
    pub channels: Vec<MlSampleChannelArtifact>,
    pub valid_time_utc: String,
    pub cycle_init_utc: String,
    pub forecast_hour: u16,
    pub model: String,
    pub split: MlDatasetSplit,
    pub grid: MlGridMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_preset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub excluded_optional_groups: Vec<MlOptionalChannelGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_domain_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MlDatasetExportRequest {
    pub model: ModelId,
    pub dataset_name: String,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hours: Vec<u16>,
    pub source: SourceId,
    pub split: MlDatasetSplit,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub preset: MlChannelPreset,
    pub include_ecape: bool,
    pub requested_domain_id: Option<String>,
    pub crop_bounds: Option<(f64, f64, f64, f64)>,
}

pub type HrrrDatasetExportRequest = MlDatasetExportRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportedSampleReport {
    pub sample_id: String,
    pub sample_dir: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MlDatasetExportReport {
    pub dataset_manifest_path: PathBuf,
    pub dataset_build_manifest_path: PathBuf,
    pub samples: Vec<ExportedSampleReport>,
}

pub type HrrrDatasetExportReport = MlDatasetExportReport;

#[derive(Debug, Clone)]
struct ExportedSampleBundle {
    sample_dir: PathBuf,
    manifest_path: PathBuf,
    manifest: MlSampleManifest,
}

#[derive(Debug, Clone)]
struct DirectResolvedField {
    width: usize,
    height: usize,
    values_f64: Vec<f64>,
    units: String,
    title: String,
    field_selector: Option<String>,
    input_fetch_keys: Vec<String>,
    resolved_fetch_urls: Vec<String>,
}

pub fn export_hrrr_dataset_bundle(
    request: &HrrrDatasetExportRequest,
) -> Result<HrrrDatasetExportReport, Box<dyn std::error::Error>> {
    export_model_dataset_bundle(request)
}

pub fn export_model_dataset_bundle(
    request: &MlDatasetExportRequest,
) -> Result<MlDatasetExportReport, Box<dyn std::error::Error>> {
    validate_request(request)?;
    ensure_dir(&request.out_dir)?;
    ensure_dir(&request.cache_root)?;
    let generated_at = Utc::now().to_rfc3339();
    let channels_for_model = resolved_channels_for_request(request);
    let mut sample_refs = Vec::new();
    let mut sample_reports = Vec::new();
    let mut split_counts = MlDatasetSplitCounts::default();
    let mut static_channel_metadata = Vec::new();
    let mut total_channel_count = 0usize;

    for &forecast_hour in &request.forecast_hours {
        let exported = export_model_sample_bundle(request, forecast_hour, &generated_at)?;
        if static_channel_metadata.is_empty() {
            static_channel_metadata = exported
                .manifest
                .channels
                .iter()
                .map(channel_catalog_entry_from_sample)
                .collect();
        }
        total_channel_count += exported.manifest.channel_count;
        sample_refs.push(MlDatasetSampleRef {
            sample_id: exported.manifest.sample_id.clone(),
            source: exported.manifest.source.clone(),
            relative_dir: path_relative_to(&exported.sample_dir, &request.out_dir),
            channel_count: exported.manifest.channel_count,
            shard_index: 0,
            split: request.split,
        });
        increment_split_count(&mut split_counts, request.split);
        sample_reports.push(ExportedSampleReport {
            sample_id: exported.manifest.sample_id,
            sample_dir: exported.sample_dir,
            manifest_path: exported.manifest_path,
        });
    }

    let dataset_manifest = MlDatasetManifest {
        dataset_name: request.dataset_name.clone(),
        generated_at: generated_at.clone(),
        format: MlDatasetExportFormat::NpyDirectory,
        shard_count: 1,
        channels: channels_for_model
            .iter()
            .map(|channel| channel.name().to_string())
            .collect(),
        labels: Vec::new(),
        channel_metadata: static_channel_metadata,
        notes: preset_notes(request.preset, request.model),
        compatibility_mode: request
            .preset
            .compatibility_mode()
            .map(|value| value.to_string()),
        excluded_optional_groups: excluded_optional_groups(request),
        requested_domain_id: request.requested_domain_id.clone(),
    };
    let build_manifest = MlDatasetBuildManifest {
        dataset_name: request.dataset_name.clone(),
        generated_at,
        format: MlDatasetExportFormat::NpyDirectory,
        shard_count: 1,
        sample_count: sample_refs.len(),
        total_channel_count,
        split_counts,
        samples: sample_refs,
    };
    let dataset_manifest_path = request.out_dir.join("dataset_manifest.json");
    let dataset_build_manifest_path = request.out_dir.join("dataset_build_manifest.json");
    atomic_write_json(&dataset_manifest_path, &dataset_manifest)?;
    atomic_write_json(&dataset_build_manifest_path, &build_manifest)?;

    Ok(MlDatasetExportReport {
        dataset_manifest_path,
        dataset_build_manifest_path,
        samples: sample_reports,
    })
}

fn validate_request(request: &MlDatasetExportRequest) -> Result<(), Box<dyn std::error::Error>> {
    if request.forecast_hours.is_empty() {
        return Err("dataset export requires at least one forecast hour".into());
    }
    let supported = resolved_channels_for_request(request);
    if supported.is_empty() {
        return Err(format!(
            "preset '{}' is not currently verified for model '{}'",
            request.preset.manifest_name(),
            request.model
        )
        .into());
    }
    Ok(())
}

fn export_model_sample_bundle(
    request: &MlDatasetExportRequest,
    forecast_hour: u16,
    generated_at: &str,
) -> Result<ExportedSampleBundle, Box<dyn std::error::Error>> {
    let loaded = if let Some(bounds) = request.crop_bounds {
        load_model_timestep_from_parts_cropped(
            request.model,
            &request.date_yyyymmdd,
            Some(request.cycle_utc),
            forecast_hour,
            request.source,
            None,
            None,
            &request.cache_root,
            request.use_cache,
            bounds,
        )?
    } else {
        load_model_timestep_from_parts(
            request.model,
            &request.date_yyyymmdd,
            Some(request.cycle_utc),
            forecast_hour,
            request.source,
            None,
            None,
            &request.cache_root,
            request.use_cache,
        )?
    };
    let surface = &loaded.surface_decode.value;
    let pressure = &loaded.pressure_decode.value;
    let calc_grid = CalcGridShape::new(surface.nx, surface.ny)?;
    let surface_inputs = SurfaceInputs {
        psfc_pa: &surface.psfc_pa,
        t2_k: &surface.t2_k,
        q2_kgkg: &surface.q2_kgkg,
        u10_ms: &surface.u10_ms,
        v10_ms: &surface.v10_ms,
    };
    let surface_thermo = compute_surface_thermo(calc_grid, surface_inputs)?;
    let relative_humidity_pct = compute_2m_relative_humidity(calc_grid, surface_inputs)?;
    let requested_channels = resolved_channels_for_request(request);
    let rh_925 = if requested_channels.contains(&MlExportChannel::Rh925) {
        Some(compute_pressure_level_relative_humidity_slice(
            pressure, 925.0, surface.nx, surface.ny,
        )?)
    } else {
        None
    };
    let rh_850 = if requested_channels.contains(&MlExportChannel::Rh850) {
        Some(compute_pressure_level_relative_humidity_slice(
            pressure, 850.0, surface.nx, surface.ny,
        )?)
    } else {
        None
    };
    let rh_700 = if requested_channels.contains(&MlExportChannel::Rh700) {
        Some(compute_pressure_level_relative_humidity_slice(
            pressure, 700.0, surface.nx, surface.ny,
        )?)
    } else {
        None
    };

    let sample_id = build_sample_id(
        request.model,
        request.requested_domain_id.as_deref(),
        &request.date_yyyymmdd,
        request.cycle_utc,
        forecast_hour,
    );
    let sample_dir = request.out_dir.join("samples").join(&sample_id);
    ensure_dir(&sample_dir)?;

    let cycle_init_utc = cycle_init_utc(&request.date_yyyymmdd, request.cycle_utc)?;
    let valid_time_utc = valid_time_utc(&request.date_yyyymmdd, request.cycle_utc, forecast_hour)?;
    let grid_metadata = build_grid_metadata(surface);
    let common_ctx = CommonExportContext::from_loaded(&loaded);

    let mut direct_refc: Option<DirectResolvedField> = None;
    let mut direct_mslp: Option<DirectResolvedField> = None;
    let ecape_triplet = if requested_channels.iter().any(|channel| {
        matches!(
            channel,
            MlExportChannel::Sbecape | MlExportChannel::Mlecape | MlExportChannel::Muecape
        )
    }) {
        Some(compute_ecape_triplet_fields(surface, pressure)?)
    } else {
        None
    };

    let mut channels = Vec::new();
    for channel in requested_channels {
        let artifact = match channel {
            MlExportChannel::T2m => export_channel_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &surface.t2_k,
                surface_bundle_provenance(channel, "2 m temperature", &common_ctx),
            )?,
            MlExportChannel::D2m => {
                let dewpoint_k = surface_thermo
                    .dewpoint_2m_c
                    .iter()
                    .map(|value| *value + 273.15)
                    .collect::<Vec<_>>();
                export_channel_values(
                    &sample_dir,
                    channel,
                    surface.nx,
                    surface.ny,
                    &dewpoint_k,
                    derived_surface_provenance(channel, "2 m dewpoint", &common_ctx),
                )?
            }
            MlExportChannel::Q2m => export_channel_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &surface.q2_kgkg,
                surface_bundle_provenance(channel, "2 m specific humidity", &common_ctx),
            )?,
            MlExportChannel::U10 => export_channel_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &surface.u10_ms,
                surface_bundle_provenance(channel, "10 m U wind", &common_ctx),
            )?,
            MlExportChannel::V10 => export_channel_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &surface.v10_ms,
                surface_bundle_provenance(channel, "10 m V wind", &common_ctx),
            )?,
            MlExportChannel::WindSpeed => {
                let values = compute_wind_speed_ms(&surface.u10_ms, &surface.v10_ms);
                export_channel_values(
                    &sample_dir,
                    channel,
                    surface.nx,
                    surface.ny,
                    &values,
                    derived_surface_provenance(channel, "10 m wind speed", &common_ctx),
                )?
            }
            MlExportChannel::WindDirection => {
                let values = compute_wind_direction_deg(&surface.u10_ms, &surface.v10_ms);
                export_channel_values(
                    &sample_dir,
                    channel,
                    surface.nx,
                    surface.ny,
                    &values,
                    derived_surface_provenance(channel, "10 m wind direction", &common_ctx),
                )?
            }
            MlExportChannel::RelativeHumidity2m => export_channel_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &relative_humidity_pct,
                derived_surface_provenance(channel, "2 m relative humidity", &common_ctx),
            )?,
            MlExportChannel::CapeCompat | MlExportChannel::Sbcape => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "sbcape",
                "Surface-based CAPE",
                &common_ctx,
            )?,
            MlExportChannel::Sbcin => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "sbcin",
                "Surface-based CIN",
                &common_ctx,
            )?,
            MlExportChannel::Mlcape => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "mlcape",
                "Mixed-layer CAPE",
                &common_ctx,
            )?,
            MlExportChannel::Mlcin => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "mlcin",
                "Mixed-layer CIN",
                &common_ctx,
            )?,
            MlExportChannel::Mucape => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "mucape",
                "Most-unstable CAPE",
                &common_ctx,
            )?,
            MlExportChannel::Srh01 => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "srh_0_1km",
                "0-1 km SRH",
                &common_ctx,
            )?,
            MlExportChannel::Srh03 => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "srh_0_3km",
                "0-3 km SRH",
                &common_ctx,
            )?,
            MlExportChannel::Shear06 => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "bulk_shear_0_6km",
                "0-6 km bulk shear",
                &common_ctx,
            )?,
            MlExportChannel::Sblcl => export_derived_query_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                surface,
                pressure,
                "sblcl",
                "Surface-based LCL height",
                &common_ctx,
            )?,
            MlExportChannel::Refc => {
                let direct_refc = if let Some(field) = &direct_refc {
                    field
                } else {
                    direct_refc = Some(resolve_direct_field(
                        &loaded,
                        forecast_hour,
                        &request.cache_root,
                        request.use_cache,
                        "composite_reflectivity",
                        false,
                    )?);
                    direct_refc
                        .as_ref()
                        .expect("direct reflectivity just resolved")
                };
                export_direct_field_artifact(&sample_dir, channel, direct_refc)?
            }
            MlExportChannel::Mslp => {
                let direct_mslp = if let Some(field) = &direct_mslp {
                    field
                } else {
                    direct_mslp = Some(resolve_direct_field(
                        &loaded,
                        forecast_hour,
                        &request.cache_root,
                        request.use_cache,
                        "mslp_10m_winds",
                        true,
                    )?);
                    direct_mslp.as_ref().expect("direct MSLP just resolved")
                };
                export_direct_field_artifact(&sample_dir, channel, direct_mslp)?
            }
            MlExportChannel::Terrain => export_channel_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &surface.orog_m,
                surface_bundle_provenance(channel, "Terrain orography", &common_ctx),
            )?,
            MlExportChannel::T925 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.temperature_c_3d,
                pressure_level_index(pressure, 925.0)?,
                |values| values.iter().map(|value| *value + 273.15).collect(),
                &common_ctx,
                "925 hPa temperature",
            )?,
            MlExportChannel::T850 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.temperature_c_3d,
                pressure_level_index(pressure, 850.0)?,
                |values| values.iter().map(|value| *value + 273.15).collect(),
                &common_ctx,
                "850 hPa temperature",
            )?,
            MlExportChannel::T700 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.temperature_c_3d,
                pressure_level_index(pressure, 700.0)?,
                |values| values.iter().map(|value| *value + 273.15).collect(),
                &common_ctx,
                "700 hPa temperature",
            )?,
            MlExportChannel::Rh925 => export_pressure_level_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                rh_925
                    .as_deref()
                    .ok_or("925 hPa RH requested without a prepared slice")?,
                &common_ctx,
                "925 hPa relative humidity",
            )?,
            MlExportChannel::Rh850 => export_pressure_level_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                rh_850
                    .as_deref()
                    .ok_or("850 hPa RH requested without a prepared slice")?,
                &common_ctx,
                "850 hPa relative humidity",
            )?,
            MlExportChannel::Rh700 => export_pressure_level_values(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                rh_700
                    .as_deref()
                    .ok_or("700 hPa RH requested without a prepared slice")?,
                &common_ctx,
                "700 hPa relative humidity",
            )?,
            MlExportChannel::Z925 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.gh_m_3d,
                pressure_level_index(pressure, 925.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "925 hPa geopotential height",
            )?,
            MlExportChannel::Z850 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.gh_m_3d,
                pressure_level_index(pressure, 850.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "850 hPa geopotential height",
            )?,
            MlExportChannel::Z700 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.gh_m_3d,
                pressure_level_index(pressure, 700.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "700 hPa geopotential height",
            )?,
            MlExportChannel::U925 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.u_ms_3d,
                pressure_level_index(pressure, 925.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "925 hPa U wind",
            )?,
            MlExportChannel::U850 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.u_ms_3d,
                pressure_level_index(pressure, 850.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "850 hPa U wind",
            )?,
            MlExportChannel::U700 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.u_ms_3d,
                pressure_level_index(pressure, 700.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "700 hPa U wind",
            )?,
            MlExportChannel::V925 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.v_ms_3d,
                pressure_level_index(pressure, 925.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "925 hPa V wind",
            )?,
            MlExportChannel::V850 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.v_ms_3d,
                pressure_level_index(pressure, 850.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "850 hPa V wind",
            )?,
            MlExportChannel::V700 => export_pressure_level_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                &pressure.v_ms_3d,
                pressure_level_index(pressure, 700.0)?,
                |values| values.to_vec(),
                &common_ctx,
                "700 hPa V wind",
            )?,
            MlExportChannel::Sbecape => export_ecape_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                ecape_triplet
                    .as_ref()
                    .ok_or("SBECAPE requested without ECAPE triplet preparation")?
                    .sbecape_jkg
                    .as_slice(),
                &common_ctx,
                "Surface-based ECAPE",
            )?,
            MlExportChannel::Mlecape => export_ecape_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                ecape_triplet
                    .as_ref()
                    .ok_or("MLECAPE requested without ECAPE triplet preparation")?
                    .mlecape_jkg
                    .as_slice(),
                &common_ctx,
                "Mixed-layer ECAPE",
            )?,
            MlExportChannel::Muecape => export_ecape_channel(
                &sample_dir,
                channel,
                surface.nx,
                surface.ny,
                ecape_triplet
                    .as_ref()
                    .ok_or("MUECAPE requested without ECAPE triplet preparation")?
                    .muecape_jkg
                    .as_slice(),
                &common_ctx,
                "Most-unstable ECAPE",
            )?,
        };
        channels.push(artifact);
    }

    let manifest = MlSampleManifest {
        dataset_name: request.dataset_name.clone(),
        sample_id: sample_id.clone(),
        generated_at: generated_at.to_string(),
        format: MlDatasetExportFormat::NpyDirectory,
        source: request.source.to_string(),
        channel_count: channels.len(),
        channels,
        valid_time_utc,
        cycle_init_utc,
        forecast_hour,
        model: request.model.to_string(),
        split: request.split,
        grid: grid_metadata,
        channel_preset: Some(request.preset.manifest_name().to_string()),
        compatibility_mode: request
            .preset
            .compatibility_mode()
            .map(|value| value.to_string()),
        excluded_optional_groups: excluded_optional_groups(request),
        requested_domain_id: request.requested_domain_id.clone(),
    };
    let manifest_path = sample_dir.join("sample_manifest.json");
    atomic_write_json(&manifest_path, &manifest)?;
    Ok(ExportedSampleBundle {
        sample_dir,
        manifest_path,
        manifest,
    })
}

#[derive(Debug, Clone)]
struct CommonExportContext {
    surface_fetch_key: String,
    pressure_fetch_key: String,
    surface_fetch_url: String,
    pressure_fetch_url: String,
}

impl CommonExportContext {
    fn from_loaded(loaded: &LoadedModelTimestep) -> Self {
        Self {
            surface_fetch_key: fetch_key(
                &loaded
                    .shared_timing
                    .surface_fetch
                    .planned_family
                    .to_string(),
                &loaded.surface_file.request.request,
            ),
            pressure_fetch_key: fetch_key(
                &loaded
                    .shared_timing
                    .pressure_fetch
                    .planned_family
                    .to_string(),
                &loaded.pressure_file.request.request,
            ),
            surface_fetch_url: loaded.shared_timing.surface_fetch.resolved_url.clone(),
            pressure_fetch_url: loaded.shared_timing.pressure_fetch.resolved_url.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct EcapeTripletExport {
    sbecape_jkg: Vec<f64>,
    mlecape_jkg: Vec<f64>,
    muecape_jkg: Vec<f64>,
}

fn compute_ecape_triplet_fields(
    surface: &crate::gridded::SurfaceFields,
    pressure: &crate::gridded::PressureFields,
) -> Result<EcapeTripletExport, Box<dyn std::error::Error>> {
    let prepared = prepare_heavy_volume(surface, pressure, false)?;
    let fields = compute_ecape8_panel_fields_with_prepared_volume(surface, pressure, &prepared)?.0;
    let mut sbecape_jkg = None;
    let mut mlecape_jkg = None;
    let mut muecape_jkg = None;
    for field in fields {
        match field.product {
            WeatherProduct::Sbecape => sbecape_jkg = Some(field.values),
            WeatherProduct::Mlecape => mlecape_jkg = Some(field.values),
            WeatherProduct::Muecape => muecape_jkg = Some(field.values),
            _ => {}
        }
    }
    Ok(EcapeTripletExport {
        sbecape_jkg: sbecape_jkg.ok_or("SBECAPE field missing from ECAPE triplet output")?,
        mlecape_jkg: mlecape_jkg.ok_or("MLECAPE field missing from ECAPE triplet output")?,
        muecape_jkg: muecape_jkg.ok_or("MUECAPE field missing from ECAPE triplet output")?,
    })
}

fn resolve_direct_field(
    loaded: &LoadedModelTimestep,
    forecast_hour: u16,
    cache_root: &Path,
    use_cache: bool,
    recipe_slug: &str,
    allow_composite_filled_field: bool,
) -> Result<DirectResolvedField, Box<dyn std::error::Error>> {
    let field = load_single_direct_sampled_field_from_latest(
        &loaded.latest,
        forecast_hour,
        cache_root,
        use_cache,
        recipe_slug,
        allow_composite_filled_field,
    )?;
    Ok(DirectResolvedField {
        width: field.field.grid.shape.nx,
        height: field.field.grid.shape.ny,
        values_f64: field
            .field
            .values
            .iter()
            .map(|&value| value as f64)
            .collect(),
        units: field.field.units.clone(),
        title: field.title,
        field_selector: field.field_selector.map(|selector| selector.to_string()),
        input_fetch_keys: field
            .input_fetches
            .iter()
            .map(|fetch| fetch.fetch_key.clone())
            .collect(),
        resolved_fetch_urls: field
            .input_fetches
            .iter()
            .map(|fetch| fetch.resolved_url.clone())
            .collect(),
    })
}

fn export_channel_values(
    sample_dir: &Path,
    channel: MlExportChannel,
    width: usize,
    height: usize,
    values: &[f64],
    provenance: MlChannelProvenance,
) -> Result<MlSampleChannelArtifact, Box<dyn std::error::Error>> {
    let npy_path = sample_dir.join(format!("{}.npy", channel.name()));
    let npy_bytes = build_npy_f32_grid_bytes(height, width, values)?;
    atomic_write_bytes(&npy_path, &npy_bytes)?;
    let stats = compute_channel_stats(values);
    let missing_count = values.iter().filter(|value| !value.is_finite()).count();
    Ok(MlSampleChannelArtifact {
        message_no: 0,
        name: channel.name().to_string(),
        canonical_name: channel.canonical_name().to_string(),
        level: channel.level_label().to_string(),
        level_hpa: channel.level_hpa(),
        height_m_agl: channel.height_m_agl(),
        units: channel.units().to_string(),
        width,
        height,
        missing_count,
        data_file: format!("{}.npy", channel.name()),
        preview_file: None,
        stats,
        shape: vec![height, width],
        experimental: channel.experimental(),
        proxy: channel.proxy(),
        compatibility_alias_of: channel
            .compatibility_alias_of()
            .map(|value| value.to_string()),
        provenance,
    })
}

fn export_direct_field_artifact(
    sample_dir: &Path,
    channel: MlExportChannel,
    field: &DirectResolvedField,
) -> Result<MlSampleChannelArtifact, Box<dyn std::error::Error>> {
    let npy_path = sample_dir.join(format!("{}.npy", channel.name()));
    let npy_bytes = build_npy_f32_grid_bytes(field.height, field.width, &field.values_f64)?;
    atomic_write_bytes(&npy_path, &npy_bytes)?;
    let stats = compute_channel_stats(&field.values_f64);
    let missing_count = field
        .values_f64
        .iter()
        .filter(|value| !value.is_finite())
        .count();
    Ok(MlSampleChannelArtifact {
        message_no: 0,
        name: channel.name().to_string(),
        canonical_name: channel.canonical_name().to_string(),
        level: channel.level_label().to_string(),
        level_hpa: channel.level_hpa(),
        height_m_agl: channel.height_m_agl(),
        units: field.units.clone(),
        width: field.width,
        height: field.height,
        missing_count,
        data_file: format!("{}.npy", channel.name()),
        preview_file: None,
        stats,
        shape: vec![field.height, field.width],
        experimental: channel.experimental(),
        proxy: channel.proxy(),
        compatibility_alias_of: channel
            .compatibility_alias_of()
            .map(|value| value.to_string()),
        provenance: MlChannelProvenance {
            route: "direct_recipe".to_string(),
            product_identity: channel.product_identity().to_string(),
            kind: channel.kind(),
            product_title: Some(field.title.clone()),
            field_selector: field.field_selector.clone(),
            source_bundle: Some("direct".to_string()),
            input_fetch_keys: field.input_fetch_keys.clone(),
            resolved_fetch_urls: field.resolved_fetch_urls.clone(),
        },
    })
}

fn export_derived_query_channel(
    sample_dir: &Path,
    channel: MlExportChannel,
    width: usize,
    height: usize,
    surface: &crate::gridded::SurfaceFields,
    pressure: &crate::gridded::PressureFields,
    recipe_slug: &str,
    title: &str,
    common_ctx: &CommonExportContext,
) -> Result<MlSampleChannelArtifact, Box<dyn std::error::Error>> {
    let query = compute_derived_query_field(surface, pressure, recipe_slug)?;
    export_channel_values(
        sample_dir,
        channel,
        width,
        height,
        &query.values,
        MlChannelProvenance {
            route: "derived_recipe".to_string(),
            product_identity: channel.product_identity().to_string(),
            kind: channel.kind(),
            product_title: Some(title.to_string()),
            field_selector: None,
            source_bundle: Some("surface+pressure".to_string()),
            input_fetch_keys: vec![
                common_ctx.surface_fetch_key.clone(),
                common_ctx.pressure_fetch_key.clone(),
            ],
            resolved_fetch_urls: vec![
                common_ctx.surface_fetch_url.clone(),
                common_ctx.pressure_fetch_url.clone(),
            ],
        },
    )
}

fn export_ecape_channel(
    sample_dir: &Path,
    channel: MlExportChannel,
    width: usize,
    height: usize,
    values: &[f64],
    common_ctx: &CommonExportContext,
    title: &str,
) -> Result<MlSampleChannelArtifact, Box<dyn std::error::Error>> {
    export_channel_values(
        sample_dir,
        channel,
        width,
        height,
        values,
        MlChannelProvenance {
            route: "heavy_derived".to_string(),
            product_identity: channel.product_identity().to_string(),
            kind: channel.kind(),
            product_title: Some(title.to_string()),
            field_selector: None,
            source_bundle: Some("surface+pressure".to_string()),
            input_fetch_keys: vec![
                common_ctx.surface_fetch_key.clone(),
                common_ctx.pressure_fetch_key.clone(),
            ],
            resolved_fetch_urls: vec![
                common_ctx.surface_fetch_url.clone(),
                common_ctx.pressure_fetch_url.clone(),
            ],
        },
    )
}

fn export_pressure_level_channel<F>(
    sample_dir: &Path,
    channel: MlExportChannel,
    width: usize,
    height: usize,
    volume: &[f64],
    level_index: usize,
    transform: F,
    common_ctx: &CommonExportContext,
    title: &str,
) -> Result<MlSampleChannelArtifact, Box<dyn std::error::Error>>
where
    F: Fn(&[f64]) -> Vec<f64>,
{
    let layer = level_slice(volume, width * height, level_index)?;
    let transformed = transform(layer);
    export_channel_values(
        sample_dir,
        channel,
        width,
        height,
        &transformed,
        pressure_level_provenance(channel, title, common_ctx),
    )
}

fn export_pressure_level_values(
    sample_dir: &Path,
    channel: MlExportChannel,
    width: usize,
    height: usize,
    values: &[f64],
    common_ctx: &CommonExportContext,
    title: &str,
) -> Result<MlSampleChannelArtifact, Box<dyn std::error::Error>> {
    export_channel_values(
        sample_dir,
        channel,
        width,
        height,
        values,
        pressure_level_provenance(channel, title, common_ctx),
    )
}

fn surface_bundle_provenance(
    channel: MlExportChannel,
    title: &str,
    common_ctx: &CommonExportContext,
) -> MlChannelProvenance {
    MlChannelProvenance {
        route: "surface_decode".to_string(),
        product_identity: channel.product_identity().to_string(),
        kind: channel.kind(),
        product_title: Some(title.to_string()),
        field_selector: None,
        source_bundle: Some("surface".to_string()),
        input_fetch_keys: vec![common_ctx.surface_fetch_key.clone()],
        resolved_fetch_urls: vec![common_ctx.surface_fetch_url.clone()],
    }
}

fn derived_surface_provenance(
    channel: MlExportChannel,
    title: &str,
    common_ctx: &CommonExportContext,
) -> MlChannelProvenance {
    MlChannelProvenance {
        route: "surface_derived".to_string(),
        product_identity: channel.product_identity().to_string(),
        kind: channel.kind(),
        product_title: Some(title.to_string()),
        field_selector: None,
        source_bundle: Some("surface".to_string()),
        input_fetch_keys: vec![common_ctx.surface_fetch_key.clone()],
        resolved_fetch_urls: vec![common_ctx.surface_fetch_url.clone()],
    }
}

fn pressure_level_provenance(
    channel: MlExportChannel,
    title: &str,
    common_ctx: &CommonExportContext,
) -> MlChannelProvenance {
    MlChannelProvenance {
        route: "pressure_decode".to_string(),
        product_identity: channel.product_identity().to_string(),
        kind: channel.kind(),
        product_title: Some(title.to_string()),
        field_selector: None,
        source_bundle: Some("pressure".to_string()),
        input_fetch_keys: vec![common_ctx.pressure_fetch_key.clone()],
        resolved_fetch_urls: vec![common_ctx.pressure_fetch_url.clone()],
    }
}

fn channel_catalog_entry_from_sample(channel: &MlSampleChannelArtifact) -> MlChannelCatalogEntry {
    MlChannelCatalogEntry {
        name: channel.name.clone(),
        canonical_name: channel.canonical_name.clone(),
        units: channel.units.clone(),
        shape: channel.shape.clone(),
        level: channel.level.clone(),
        level_hpa: channel.level_hpa,
        height_m_agl: channel.height_m_agl,
        kind: channel.provenance.kind,
        experimental: channel.experimental,
        proxy: channel.proxy,
        compatibility_alias_of: channel.compatibility_alias_of.clone(),
        provenance_product_identity: channel.provenance.product_identity.clone(),
        provenance_route: channel.provenance.route.clone(),
    }
}

fn preset_notes(preset: MlChannelPreset, model: ModelId) -> Vec<String> {
    match preset {
        MlChannelPreset::MesoconvectiveV1 => vec![
            "Bundle format is generic and does not encode ML input/target roles.".to_string(),
            "This preset is an explicit compatibility mode for legacy wxtrain Julia readers: channel 'cape' is exported as an alias of exact channel 'sbcape'.".to_string(),
            "Use hybrid_column_v1 or exact canonical channel names for new hybrid training work.".to_string(),
            format!("Model '{}' export still reuses rustwx as the meteorological source of truth.", model),
        ],
        MlChannelPreset::HybridColumnV1 => {
            let mut notes = vec![
                "Bundle format is generic and does not encode ML input/target roles.".to_string(),
                "hybrid_column_v1 uses exact channel names for severe ingredients and pressure-level state.".to_string(),
                "Vertical motion (w700/omega700) is intentionally omitted until a verified public rustwx field path is wired; current hybrid_column_v1 stays honest instead of exporting a proxy.".to_string(),
                "WRF/WRF-GDEX export is not implemented yet; this preset is designed so future NetCDF adapters can plug in without changing the NPY bundle contract.".to_string(),
            ];
            if model == ModelId::RrfsA {
                notes.push("RRFS-A hybrid_column_v1 only uses fields decoded or derived directly from verified surface/pressure semantics; no thermo-native candidate/proxy mappings are used.".to_string());
                notes.push("RRFS-A currently excludes direct-field channels 'mslp' and 'refc' from hybrid_column_v1 until those public rustwx mappings are verified.".to_string());
            }
            notes
        }
    }
}

fn resolved_channels_for_request(request: &MlDatasetExportRequest) -> Vec<MlExportChannel> {
    let mut channels = request.preset.channels_for_model(request.model);
    if !request.include_ecape {
        channels.retain(|channel| {
            !matches!(
                channel,
                MlExportChannel::Sbecape | MlExportChannel::Mlecape | MlExportChannel::Muecape
            )
        });
    }
    channels
}

fn excluded_optional_groups(request: &MlDatasetExportRequest) -> Vec<MlOptionalChannelGroup> {
    if request.preset == MlChannelPreset::HybridColumnV1 && !request.include_ecape {
        vec![MlOptionalChannelGroup::Ecape]
    } else {
        Vec::new()
    }
}

fn pressure_level_index(
    pressure: &crate::gridded::PressureFields,
    target_hpa: f64,
) -> Result<usize, Box<dyn std::error::Error>> {
    pressure
        .pressure_levels_hpa
        .iter()
        .position(|level| (*level - target_hpa).abs() <= 0.25)
        .ok_or_else(|| {
            format!("pressure level {target_hpa} hPa not present in decoded pressure bundle").into()
        })
}

fn level_slice<'a>(
    volume: &'a [f64],
    plane_len: usize,
    level_index: usize,
) -> Result<&'a [f64], Box<dyn std::error::Error>> {
    let start = level_index
        .checked_mul(plane_len)
        .ok_or("volume indexing overflow")?;
    let end = start
        .checked_add(plane_len)
        .ok_or("volume indexing overflow")?;
    volume
        .get(start..end)
        .ok_or_else(|| "volume level slice was out of bounds".into())
}

fn compute_pressure_level_relative_humidity_slice(
    pressure: &crate::gridded::PressureFields,
    target_hpa: f64,
    nx: usize,
    ny: usize,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let plane_len = nx
        .checked_mul(ny)
        .ok_or("pressure-level RH plane length overflow")?;
    let level_index = pressure_level_index(pressure, target_hpa)?;
    let temperature_slice = level_slice(&pressure.temperature_c_3d, plane_len, level_index)?;
    let qvapor_slice = level_slice(&pressure.qvapor_kgkg_3d, plane_len, level_index)?;
    let pressure_plane_hpa = vec![target_hpa; plane_len];
    Ok(
        compute_relative_humidity_from_pressure_temperature_and_mixing_ratio(
            &pressure_plane_hpa,
            temperature_slice,
            qvapor_slice,
        )?,
    )
}

fn build_grid_metadata(surface: &crate::gridded::SurfaceFields) -> MlGridMetadata {
    MlGridMetadata {
        shape: vec![surface.ny, surface.nx],
        projection: surface.projection.clone(),
        grid_domain_id: format!(
            "{}x{}_{}",
            surface.nx,
            surface.ny,
            projection_slug(surface.projection.as_ref())
        ),
        approx_dx_km: approximate_grid_spacing_km(
            &surface.lat,
            &surface.lon,
            surface.nx,
            surface.ny,
            true,
        ),
        approx_dy_km: approximate_grid_spacing_km(
            &surface.lat,
            &surface.lon,
            surface.nx,
            surface.ny,
            false,
        ),
    }
}

fn projection_slug(projection: Option<&GridProjection>) -> &'static str {
    match projection {
        None | Some(GridProjection::Geographic) => "geographic",
        Some(GridProjection::LambertConformal { .. }) => "lambert_conformal",
        Some(GridProjection::PolarStereographic { .. }) => "polar_stereographic",
        Some(GridProjection::Mercator { .. }) => "mercator",
        Some(GridProjection::Other { .. }) => "other",
    }
}

fn approximate_grid_spacing_km(
    lat: &[f64],
    lon: &[f64],
    nx: usize,
    ny: usize,
    east_west: bool,
) -> Option<f64> {
    if nx < 2 || ny < 2 {
        return None;
    }
    let y = ny / 2;
    let x = nx / 2;
    let idx = y * nx + x;
    let neighbor = if east_west {
        if x + 1 >= nx {
            return None;
        }
        idx + 1
    } else {
        if y + 1 >= ny {
            return None;
        }
        idx + nx
    };
    Some(haversine_km(
        lat[idx],
        lon[idx],
        lat[neighbor],
        lon[neighbor],
    ))
}

fn haversine_km(lat0_deg: f64, lon0_deg: f64, lat1_deg: f64, lon1_deg: f64) -> f64 {
    let r_km = 6_371.0;
    let lat0 = lat0_deg.to_radians();
    let lat1 = lat1_deg.to_radians();
    let dlat = lat1 - lat0;
    let dlon = (lon1_deg - lon0_deg).to_radians();
    let sin_dlat = (dlat * 0.5).sin();
    let sin_dlon = (dlon * 0.5).sin();
    let a = sin_dlat * sin_dlat + lat0.cos() * lat1.cos() * sin_dlon * sin_dlon;
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r_km * c
}

fn compute_wind_speed_ms(u_ms: &[f64], v_ms: &[f64]) -> Vec<f64> {
    u_ms.iter()
        .zip(v_ms.iter())
        .map(|(&u, &v)| {
            if u.is_finite() && v.is_finite() {
                (u * u + v * v).sqrt()
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn compute_wind_direction_deg(u_ms: &[f64], v_ms: &[f64]) -> Vec<f64> {
    u_ms.iter()
        .zip(v_ms.iter())
        .map(|(&u, &v)| {
            if !(u.is_finite() && v.is_finite()) {
                return f64::NAN;
            }
            let mut direction = (-u).atan2(-v).to_degrees();
            if direction < 0.0 {
                direction += 360.0;
            }
            direction
        })
        .collect()
}

fn compute_channel_stats(values: &[f64]) -> Option<MlChannelStats> {
    let finite = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return None;
    }
    let min = finite.iter().copied().fold(f64::INFINITY, f64::min);
    let max = finite.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let count = finite.len();
    let mean = finite.iter().sum::<f64>() / count as f64;
    let variance = finite
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / count as f64;
    Some(MlChannelStats {
        min,
        mean,
        max,
        std: variance.sqrt(),
        count,
        nan_count: values.len().saturating_sub(count),
    })
}

fn build_npy_f32_grid_bytes(
    ny: usize,
    nx: usize,
    values: &[f64],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if values.len() != nx * ny {
        return Err(format!(
            "channel payload length {} does not match grid shape ({ny}, {nx})",
            values.len()
        )
        .into());
    }
    let mut bytes = build_npy_f32_header(ny, nx)?;
    for &value in values {
        bytes.extend_from_slice(&(value as f32).to_le_bytes());
    }
    Ok(bytes)
}

fn build_npy_f32_header(ny: usize, nx: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut header = format!("{{'descr': '<f4', 'fortran_order': False, 'shape': ({ny}, {nx}), }}");
    let preamble_len = 10usize;
    let total_without_padding = preamble_len + header.len() + 1;
    let padding = (16 - (total_without_padding % 16)) % 16;
    header.push_str(&" ".repeat(padding));
    header.push('\n');
    let header_len: u16 = header
        .len()
        .try_into()
        .map_err(|_| "npy header exceeds version 1.0 size limit")?;

    let mut bytes = Vec::with_capacity(preamble_len + header.len());
    bytes.extend_from_slice(b"\x93NUMPY");
    bytes.push(1);
    bytes.push(0);
    bytes.extend_from_slice(&header_len.to_le_bytes());
    bytes.extend_from_slice(header.as_bytes());
    Ok(bytes)
}

fn increment_split_count(counts: &mut MlDatasetSplitCounts, split: MlDatasetSplit) {
    match split {
        MlDatasetSplit::Train => counts.train += 1,
        MlDatasetSplit::Validation => counts.validation += 1,
        MlDatasetSplit::Test => counts.test += 1,
    }
}

fn build_sample_id(
    model: ModelId,
    requested_domain_id: Option<&str>,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
) -> String {
    let model_token = model.as_str().replace('-', "_");
    match requested_domain_id {
        Some(domain_id) => format!(
            "{}_{}_{}{:02}_f{:03}",
            model_token,
            sanitize_sample_token(domain_id),
            date_yyyymmdd,
            cycle_utc,
            forecast_hour
        ),
        None => format!(
            "{}_{}{:02}_f{:03}",
            model_token, date_yyyymmdd, cycle_utc, forecast_hour
        ),
    }
}

fn sanitize_sample_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

fn cycle_init_utc(
    date_yyyymmdd: &str,
    cycle_utc: u8,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(cycle_datetime(date_yyyymmdd, cycle_utc)?
        .and_utc()
        .to_rfc3339())
}

fn valid_time_utc(
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(
        (cycle_datetime(date_yyyymmdd, cycle_utc)? + Duration::hours(i64::from(forecast_hour)))
            .and_utc()
            .to_rfc3339(),
    )
}

fn cycle_datetime(
    date_yyyymmdd: &str,
    cycle_utc: u8,
) -> Result<chrono::NaiveDateTime, Box<dyn std::error::Error>> {
    let date = NaiveDate::parse_from_str(date_yyyymmdd, "%Y%m%d")?;
    date.and_hms_opt(u32::from(cycle_utc), 0, 0)
        .ok_or_else(|| format!("invalid cycle hour {cycle_utc}").into())
}

fn path_relative_to(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mesoconvective_v1_channel_order_matches_julia_expectation() {
        let channels = MlChannelPreset::MesoconvectiveV1
            .channels_for_model(ModelId::Hrrr)
            .iter()
            .map(|channel| channel.name())
            .collect::<Vec<_>>();
        assert_eq!(
            channels,
            vec![
                "t2m",
                "d2m",
                "q2m",
                "u10",
                "v10",
                "wind_speed",
                "wind_direction",
                "relative_humidity",
                "cape",
                "refc"
            ]
        );
    }

    #[test]
    fn hybrid_column_v1_uses_exact_channel_names() {
        let channels = MlChannelPreset::HybridColumnV1
            .channels_for_model(ModelId::Hrrr)
            .iter()
            .map(|channel| channel.name())
            .collect::<Vec<_>>();
        assert!(channels.contains(&"mslp"));
        assert!(channels.contains(&"refc"));
        assert!(channels.contains(&"sbcape"));
        assert!(channels.contains(&"mlcape"));
        assert!(channels.contains(&"mucape"));
        assert!(!channels.contains(&"cape"));
    }

    #[test]
    fn hybrid_column_v1_can_exclude_ecape_channels() {
        let request = MlDatasetExportRequest {
            model: ModelId::Hrrr,
            dataset_name: "test".to_string(),
            date_yyyymmdd: "20260422".to_string(),
            cycle_utc: 7,
            forecast_hours: vec![0],
            source: SourceId::Nomads,
            split: MlDatasetSplit::Train,
            out_dir: PathBuf::from("target/test"),
            cache_root: PathBuf::from("target/test-cache"),
            use_cache: true,
            preset: MlChannelPreset::HybridColumnV1,
            include_ecape: false,
            requested_domain_id: None,
            crop_bounds: None,
        };
        let channels = resolved_channels_for_request(&request)
            .iter()
            .map(|channel| channel.name())
            .collect::<Vec<_>>();
        assert!(!channels.contains(&"sbecape"));
        assert!(!channels.contains(&"mlecape"));
        assert!(!channels.contains(&"muecape"));
        assert_eq!(
            excluded_optional_groups(&request),
            vec![MlOptionalChannelGroup::Ecape]
        );
    }

    #[test]
    fn mesoconvective_v1_declares_explicit_compatibility_mode() {
        assert_eq!(
            MlChannelPreset::MesoconvectiveV1.compatibility_mode(),
            Some("wxtrain_legacy_cape_alias")
        );
        assert_eq!(
            MlExportChannel::CapeCompat.compatibility_alias_of(),
            Some("sbcape")
        );
        assert_eq!(MlExportChannel::CapeCompat.canonical_name(), "sbcape");
    }

    #[test]
    fn npy_header_uses_standard_shape_contract() {
        let bytes = build_npy_f32_grid_bytes(2, 3, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).unwrap();
        assert!(bytes.starts_with(b"\x93NUMPY"));
        let header_len = u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
        let header = String::from_utf8(bytes[10..10 + header_len].to_vec()).unwrap();
        assert!(header.contains("'shape': (2, 3)"));
        assert_eq!(
            bytes.len(),
            10 + header_len + 6 * std::mem::size_of::<f32>()
        );
    }

    #[test]
    fn cycle_and_valid_time_metadata_are_explicit() {
        assert_eq!(
            cycle_init_utc("20260422", 7).unwrap(),
            "2026-04-22T07:00:00+00:00"
        );
        assert_eq!(
            valid_time_utc("20260422", 7, 3).unwrap(),
            "2026-04-22T10:00:00+00:00"
        );
    }

    #[test]
    fn split_counts_accumulate_expected_bucket() {
        let mut counts = MlDatasetSplitCounts::default();
        increment_split_count(&mut counts, MlDatasetSplit::Train);
        increment_split_count(&mut counts, MlDatasetSplit::Validation);
        increment_split_count(&mut counts, MlDatasetSplit::Validation);
        increment_split_count(&mut counts, MlDatasetSplit::Test);
        assert_eq!(counts.train, 1);
        assert_eq!(counts.validation, 2);
        assert_eq!(counts.test, 1);
    }

    #[test]
    fn wind_direction_matches_meteorological_from_direction_convention() {
        let directions =
            compute_wind_direction_deg(&[-10.0, 0.0, 10.0, 0.0], &[0.0, -10.0, 0.0, 10.0]);
        assert_eq!(directions, vec![90.0, 0.0, 270.0, 180.0]);
    }

    #[test]
    fn rrfs_hybrid_profile_is_enabled_but_gfs_is_not() {
        assert!(
            !MlChannelPreset::HybridColumnV1
                .channels_for_model(ModelId::Gfs)
                .is_empty()
                == false
        );
        assert!(
            !MlChannelPreset::HybridColumnV1
                .channels_for_model(ModelId::RrfsA)
                .is_empty()
        );
    }

    #[test]
    fn rrfs_hybrid_profile_excludes_unverified_direct_channels() {
        let channels = MlChannelPreset::HybridColumnV1
            .channels_for_model(ModelId::RrfsA)
            .iter()
            .map(|channel| channel.name())
            .collect::<Vec<_>>();
        assert!(!channels.contains(&"mslp"));
        assert!(!channels.contains(&"refc"));
        assert!(channels.contains(&"terrain"));
        assert!(channels.contains(&"sbcape"));
    }

    #[test]
    fn pressure_level_metadata_is_attached_to_hybrid_channels() {
        assert_eq!(MlExportChannel::T925.level_hpa(), Some(925.0));
        assert_eq!(MlExportChannel::Rh850.level_hpa(), Some(850.0));
        assert_eq!(MlExportChannel::V700.level_hpa(), Some(700.0));
    }
}
