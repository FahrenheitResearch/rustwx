use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use thiserror::Error;

use rustwx_core::ModelId;

const EARTH_METERS_PER_DEG_LAT: f64 = 110_574.0;
const EARTH_METERS_PER_DEG_LON_AT_EQUATOR: f64 = 111_320.0;
const DEFAULT_OUTER_PADDING_FRACTION: f64 = 0.40;
const DEFAULT_TIME_STEP_SECONDS_PER_KM: f64 = 5.0;
const DEFAULT_ETA_LEVELS: &[f64] = &[
    1.00000, 0.99780, 0.99519, 0.99212, 0.98849, 0.98422, 0.97918, 0.97325, 0.96627, 0.95808,
    0.94846, 0.93719, 0.92402, 0.90866, 0.89079, 0.87006, 0.84612, 0.81857, 0.78706, 0.75124,
    0.71080, 0.66556, 0.61547, 0.56067, 0.50519, 0.45474, 0.40886, 0.36713, 0.32918, 0.29466,
    0.26328, 0.23473, 0.20877, 0.18516, 0.16369, 0.14417, 0.12641, 0.11026, 0.09557, 0.08222,
    0.07007, 0.05902, 0.04898, 0.03984, 0.03153, 0.02398, 0.01710, 0.01085, 0.00517, 0.00000,
];

#[derive(Debug, Error, PartialEq)]
pub enum WrfOpsError {
    #[error("invalid domain bounds; expected finite west<east and south<north")]
    InvalidBounds,
    #[error("invalid grid spacing {0}; expected a finite positive meter value")]
    InvalidDxMeters(f64),
    #[error("invalid parent grid ratio {0}; expected an integer >= 2")]
    InvalidParentRatio(u16),
    #[error("invalid time range; end must be after start")]
    InvalidTimeRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WrfDomainBounds {
    pub west_lon_deg: f64,
    pub east_lon_deg: f64,
    pub south_lat_deg: f64,
    pub north_lat_deg: f64,
}

impl WrfDomainBounds {
    pub fn new(
        west_lon_deg: f64,
        east_lon_deg: f64,
        south_lat_deg: f64,
        north_lat_deg: f64,
    ) -> Result<Self, WrfOpsError> {
        let bounds = Self {
            west_lon_deg,
            east_lon_deg,
            south_lat_deg,
            north_lat_deg,
        };
        bounds.validate()?;
        Ok(bounds)
    }

    pub fn from_center_km(
        center_lat_deg: f64,
        center_lon_deg: f64,
        width_km: f64,
        height_km: f64,
    ) -> Result<Self, WrfOpsError> {
        if !center_lat_deg.is_finite()
            || !center_lon_deg.is_finite()
            || !width_km.is_finite()
            || !height_km.is_finite()
            || width_km <= 0.0
            || height_km <= 0.0
        {
            return Err(WrfOpsError::InvalidBounds);
        }
        let meters_per_deg_lon =
            EARTH_METERS_PER_DEG_LON_AT_EQUATOR * center_lat_deg.to_radians().cos().abs().max(0.05);
        let half_width_deg = (width_km * 1_000.0) / meters_per_deg_lon / 2.0;
        let half_height_deg = (height_km * 1_000.0) / EARTH_METERS_PER_DEG_LAT / 2.0;
        Self::new(
            center_lon_deg - half_width_deg,
            center_lon_deg + half_width_deg,
            center_lat_deg - half_height_deg,
            center_lat_deg + half_height_deg,
        )
    }

    pub fn center_lat(self) -> f64 {
        0.5 * (self.south_lat_deg + self.north_lat_deg)
    }

    pub fn center_lon(self) -> f64 {
        0.5 * (self.west_lon_deg + self.east_lon_deg)
    }

    pub fn width_m(self) -> f64 {
        let meters_per_deg = EARTH_METERS_PER_DEG_LON_AT_EQUATOR
            * self.center_lat().to_radians().cos().abs().max(0.05);
        (self.east_lon_deg - self.west_lon_deg) * meters_per_deg
    }

    pub fn height_m(self) -> f64 {
        (self.north_lat_deg - self.south_lat_deg) * EARTH_METERS_PER_DEG_LAT
    }

    fn validate(self) -> Result<(), WrfOpsError> {
        if self.west_lon_deg.is_finite()
            && self.east_lon_deg.is_finite()
            && self.south_lat_deg.is_finite()
            && self.north_lat_deg.is_finite()
            && self.west_lon_deg < self.east_lon_deg
            && self.south_lat_deg < self.north_lat_deg
        {
            Ok(())
        } else {
            Err(WrfOpsError::InvalidBounds)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WrfNestedResolution {
    Default3Km,
    Extra1p5Km,
    Special500M,
    Custom { inner_dx_m: u32, parent_ratio: u16 },
}

impl WrfNestedResolution {
    pub fn inner_dx_m(self) -> f64 {
        match self {
            Self::Default3Km => 3_000.0,
            Self::Extra1p5Km => 1_500.0,
            Self::Special500M => 500.0,
            Self::Custom { inner_dx_m, .. } => inner_dx_m as f64,
        }
    }

    pub fn parent_ratio(self) -> u16 {
        match self {
            Self::Custom { parent_ratio, .. } => parent_ratio,
            _ => 3,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Default3Km => "3km-default",
            Self::Extra1p5Km => "1.5km-extra",
            Self::Special500M => "500m-special",
            Self::Custom { .. } => "custom",
        }
    }

    pub fn custom(inner_dx_m: u32, parent_ratio: u16) -> Result<Self, WrfOpsError> {
        if inner_dx_m == 0 {
            return Err(WrfOpsError::InvalidDxMeters(f64::from(inner_dx_m)));
        }
        if parent_ratio < 2 {
            return Err(WrfOpsError::InvalidParentRatio(parent_ratio));
        }
        Ok(Self::Custom {
            inner_dx_m,
            parent_ratio,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WrfInitSource {
    Hrrr,
    Rap,
    Gfs,
    Gefs,
    EcmwfOpenData,
    AifsV2,
    Era5,
    Nam,
    Rrfs,
    Icon,
}

impl WrfInitSource {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Hrrr => "hrrr",
            Self::Rap => "rap",
            Self::Gfs => "gfs",
            Self::Gefs => "gefs",
            Self::EcmwfOpenData => "ecmwf-open-data",
            Self::AifsV2 => "aifs-v2",
            Self::Era5 => "era5",
            Self::Nam => "nam",
            Self::Rrfs => "rrfs",
            Self::Icon => "icon",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Hrrr => "HRRR",
            Self::Rap => "RAP",
            Self::Gfs => "GFS",
            Self::Gefs => "GEFS",
            Self::EcmwfOpenData => "ECMWF open data",
            Self::AifsV2 => "AIFS v2",
            Self::Era5 => "ERA5",
            Self::Nam => "NAM",
            Self::Rrfs => "RRFS",
            Self::Icon => "ICON",
        }
    }

    pub fn default_interval_seconds(self) -> u32 {
        match self {
            Self::Hrrr | Self::Rap | Self::Era5 => 3_600,
            Self::Gfs
            | Self::Gefs
            | Self::EcmwfOpenData
            | Self::AifsV2
            | Self::Nam
            | Self::Rrfs => 10_800,
            Self::Icon => 10_800,
        }
    }

    pub fn default_vtable(self) -> &'static str {
        match self {
            Self::Hrrr | Self::Rap | Self::Gfs | Self::Gefs | Self::Nam | Self::Rrfs => {
                "Vtable.GFS"
            }
            Self::EcmwfOpenData | Self::AifsV2 | Self::Era5 => "Vtable.ECMWF",
            Self::Icon => "Vtable.GFS",
        }
    }

    pub fn rustwx_model_id(self) -> Option<ModelId> {
        match self {
            Self::Hrrr => Some(ModelId::Hrrr),
            Self::Rap => Some(ModelId::Rap),
            Self::Gfs => Some(ModelId::Gfs),
            Self::Gefs => Some(ModelId::Gefs),
            Self::EcmwfOpenData => Some(ModelId::EcmwfOpenData),
            Self::AifsV2 => Some(ModelId::Aifs),
            Self::Nam => Some(ModelId::Nam),
            Self::Rrfs => Some(ModelId::RrfsPublic),
            Self::Era5 | Self::Icon => None,
        }
    }

    pub fn default_wps_products(self) -> &'static [&'static str] {
        match self {
            Self::Hrrr => &["prs"],
            Self::Rap => &["awp130pgrb"],
            Self::Gfs => &["pgrb2.0p25"],
            Self::Gefs => &["pgrb2ap5/gec00"],
            Self::EcmwfOpenData | Self::AifsV2 => &["oper"],
            Self::Era5 => &["pressure", "surface"],
            Self::Nam => &["awip12"],
            Self::Rrfs => &["prs-conus", "2dfld-conus"],
            Self::Icon => &["global"],
        }
    }

    pub fn default_num_metgrid_levels(self) -> u16 {
        match self {
            Self::Hrrr | Self::Rap => 41,
            Self::Era5 => 30,
            _ => 34,
        }
    }

    pub fn default_num_metgrid_soil_levels(self) -> u16 {
        match self {
            Self::Hrrr | Self::Rap => 9,
            _ => 4,
        }
    }

    pub fn requires_surface_gribs(self) -> bool {
        matches!(
            self,
            Self::Gfs | Self::Gefs | Self::EcmwfOpenData | Self::AifsV2 | Self::Era5
        )
    }

    pub fn supported_sources() -> Vec<WrfInitSourceInfo> {
        [
            Self::Hrrr,
            Self::Rap,
            Self::Gfs,
            Self::Gefs,
            Self::EcmwfOpenData,
            Self::AifsV2,
            Self::Era5,
            Self::Nam,
            Self::Rrfs,
            Self::Icon,
        ]
        .into_iter()
        .map(WrfInitSourceInfo::from)
        .collect()
    }
}

impl std::str::FromStr for WrfInitSource {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let key = value.trim().to_ascii_lowercase().replace('_', "-");
        match key.as_str() {
            "hrrr" => Ok(Self::Hrrr),
            "rap" => Ok(Self::Rap),
            "gfs" => Ok(Self::Gfs),
            "gefs" => Ok(Self::Gefs),
            "ecmwf" | "euro" | "ecmwf-open-data" => Ok(Self::EcmwfOpenData),
            "aifs" | "aifs-v2" | "aifsv2" => Ok(Self::AifsV2),
            "era5" => Ok(Self::Era5),
            "nam" => Ok(Self::Nam),
            "rrfs" | "rrfs-a" | "rrfs-public" => Ok(Self::Rrfs),
            "icon" => Ok(Self::Icon),
            _ => Err(format!("unknown WRF init source `{value}`")),
        }
    }
}

impl std::fmt::Display for WrfInitSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.slug())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WrfInitStagingMode {
    RustwxDirect,
    ExternalGribs,
}

impl WrfInitStagingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RustwxDirect => "rustwx-direct",
            Self::ExternalGribs => "external-gribs",
        }
    }
}

impl std::fmt::Display for WrfInitStagingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrfInitSourceInfo {
    pub slug: String,
    pub display_name: String,
    pub default_interval_seconds: u32,
    pub default_vtable: String,
    pub rustwx_model: Option<ModelId>,
    pub staging_mode: WrfInitStagingMode,
    pub direct_stage_available: bool,
    pub default_wps_products: Vec<String>,
    pub default_num_metgrid_levels: u16,
    pub default_num_metgrid_soil_levels: u16,
    pub requires_surface_gribs: bool,
}

impl From<WrfInitSource> for WrfInitSourceInfo {
    fn from(source: WrfInitSource) -> Self {
        let rustwx_model = source.rustwx_model_id();
        Self {
            slug: source.slug().to_string(),
            display_name: source.display_name().to_string(),
            default_interval_seconds: source.default_interval_seconds(),
            default_vtable: source.default_vtable().to_string(),
            rustwx_model,
            staging_mode: if rustwx_model.is_some() {
                WrfInitStagingMode::RustwxDirect
            } else {
                WrfInitStagingMode::ExternalGribs
            },
            direct_stage_available: rustwx_model.is_some(),
            default_wps_products: source
                .default_wps_products()
                .iter()
                .map(|product| (*product).to_string())
                .collect(),
            default_num_metgrid_levels: source.default_num_metgrid_levels(),
            default_num_metgrid_soil_levels: source.default_num_metgrid_soil_levels(),
            requires_surface_gribs: source.requires_surface_gribs(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WrfPhysicsPreset {
    SevereConvection,
    SevereConvectionNoahMp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WrfOpsRequest {
    pub project_name: String,
    pub init_source: WrfInitSource,
    pub start_utc: String,
    pub end_utc: String,
    pub bounds: WrfDomainBounds,
    pub nested: bool,
    pub nested_resolution: WrfNestedResolution,
    pub history_interval_minutes: u32,
    pub num_cores: u32,
    pub output_3d_interval_minutes: Option<u32>,
    pub physics: WrfPhysicsPreset,
    #[serde(default)]
    pub num_metgrid_levels: Option<u16>,
    #[serde(default)]
    pub num_metgrid_soil_levels: Option<u16>,
    #[serde(default)]
    pub wps_products: Option<Vec<String>>,
}

impl WrfOpsRequest {
    pub fn severe_default(
        project_name: impl Into<String>,
        init_source: WrfInitSource,
        start_utc: impl Into<String>,
        end_utc: impl Into<String>,
        bounds: WrfDomainBounds,
    ) -> Self {
        Self {
            project_name: project_name.into(),
            init_source,
            start_utc: start_utc.into(),
            end_utc: end_utc.into(),
            bounds,
            nested: true,
            nested_resolution: WrfNestedResolution::Default3Km,
            history_interval_minutes: 6,
            num_cores: 20,
            output_3d_interval_minutes: Some(6),
            physics: WrfPhysicsPreset::SevereConvection,
            num_metgrid_levels: None,
            num_metgrid_soil_levels: None,
            wps_products: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WrfDomain {
    pub id: u8,
    pub parent_id: u8,
    pub e_we: u32,
    pub e_sn: u32,
    pub dx_m: f64,
    pub dy_m: f64,
    pub i_parent_start: u32,
    pub j_parent_start: u32,
    pub parent_grid_ratio: u16,
    pub parent_time_step_ratio: u16,
    pub ref_lat: f64,
    pub ref_lon: f64,
    pub truelat1: f64,
    pub truelat2: f64,
    pub stand_lon: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WrfOpsPlan {
    pub request: WrfOpsRequest,
    pub max_dom: u8,
    pub domains: Vec<WrfDomain>,
    pub time_step_seconds: u32,
    pub run_hours: u32,
    pub run_minutes: u32,
    pub interval_seconds: u32,
    pub vtable: String,
    #[serde(default = "default_plan_num_metgrid_levels")]
    pub num_metgrid_levels: u16,
    #[serde(default = "default_plan_num_metgrid_soil_levels")]
    pub num_metgrid_soil_levels: u16,
    #[serde(default)]
    pub wps_products: Vec<String>,
}

fn default_plan_num_metgrid_levels() -> u16 {
    34
}

fn default_plan_num_metgrid_soil_levels() -> u16 {
    4
}

impl WrfOpsPlan {
    pub fn plan(request: WrfOpsRequest) -> Result<Self, WrfOpsError> {
        request.bounds.validate()?;
        let inner_dx = request.nested_resolution.inner_dx_m();
        if !inner_dx.is_finite() || inner_dx <= 0.0 {
            return Err(WrfOpsError::InvalidDxMeters(inner_dx));
        }
        let ratio = request.nested_resolution.parent_ratio();
        if ratio < 2 {
            return Err(WrfOpsError::InvalidParentRatio(ratio));
        }

        let inner_e_we = nested_dim_for_span(request.bounds.width_m(), inner_dx, ratio);
        let inner_e_sn = nested_dim_for_span(request.bounds.height_m(), inner_dx, ratio);
        let ref_lat = request.bounds.center_lat();
        let ref_lon = request.bounds.center_lon();
        let truelat1 = request.bounds.south_lat_deg.clamp(-80.0, 80.0);
        let truelat2 = request.bounds.north_lat_deg.clamp(-80.0, 80.0);

        let domains = if request.nested {
            let inner_parent_span_we = (inner_e_we - 1) / ratio as u32;
            let inner_parent_span_sn = (inner_e_sn - 1) / ratio as u32;
            let pad_we = padded_parent_cells(inner_parent_span_we);
            let pad_sn = padded_parent_cells(inner_parent_span_sn);
            let parent_e_we = odd_dim(inner_parent_span_we + pad_we * 2 + 1);
            let parent_e_sn = odd_dim(inner_parent_span_sn + pad_sn * 2 + 1);
            let parent_dx = inner_dx * f64::from(ratio);
            vec![
                WrfDomain {
                    id: 1,
                    parent_id: 0,
                    e_we: parent_e_we,
                    e_sn: parent_e_sn,
                    dx_m: parent_dx,
                    dy_m: parent_dx,
                    i_parent_start: 1,
                    j_parent_start: 1,
                    parent_grid_ratio: 1,
                    parent_time_step_ratio: 1,
                    ref_lat,
                    ref_lon,
                    truelat1,
                    truelat2,
                    stand_lon: ref_lon,
                },
                WrfDomain {
                    id: 2,
                    parent_id: 1,
                    e_we: inner_e_we,
                    e_sn: inner_e_sn,
                    dx_m: inner_dx,
                    dy_m: inner_dx,
                    i_parent_start: pad_we + 1,
                    j_parent_start: pad_sn + 1,
                    parent_grid_ratio: ratio,
                    parent_time_step_ratio: ratio,
                    ref_lat,
                    ref_lon,
                    truelat1,
                    truelat2,
                    stand_lon: ref_lon,
                },
            ]
        } else {
            vec![WrfDomain {
                id: 1,
                parent_id: 0,
                e_we: odd_dim(cells_for_span(request.bounds.width_m(), inner_dx) + 1),
                e_sn: odd_dim(cells_for_span(request.bounds.height_m(), inner_dx) + 1),
                dx_m: inner_dx,
                dy_m: inner_dx,
                i_parent_start: 1,
                j_parent_start: 1,
                parent_grid_ratio: 1,
                parent_time_step_ratio: 1,
                ref_lat,
                ref_lon,
                truelat1,
                truelat2,
                stand_lon: ref_lon,
            }]
        };

        let coarse_dx = domains[0].dx_m;
        let time_step_seconds =
            (coarse_dx / 1000.0 * DEFAULT_TIME_STEP_SECONDS_PER_KM).round() as u32;
        let (run_hours, run_minutes) = parse_run_duration(&request.start_utc, &request.end_utc)?;
        let num_metgrid_levels = request
            .num_metgrid_levels
            .unwrap_or_else(|| request.init_source.default_num_metgrid_levels());
        let num_metgrid_soil_levels = request
            .num_metgrid_soil_levels
            .unwrap_or_else(|| request.init_source.default_num_metgrid_soil_levels());
        let wps_products = request.wps_products.clone().unwrap_or_else(|| {
            request
                .init_source
                .default_wps_products()
                .iter()
                .map(|product| (*product).to_string())
                .collect()
        });

        Ok(Self {
            max_dom: domains.len() as u8,
            time_step_seconds,
            run_hours,
            run_minutes,
            interval_seconds: request.init_source.default_interval_seconds(),
            vtable: request.init_source.default_vtable().to_string(),
            num_metgrid_levels,
            num_metgrid_soil_levels,
            wps_products,
            request,
            domains,
        })
    }

    pub fn inner_domain(&self) -> &WrfDomain {
        self.domains.last().expect("WRF plan always has a domain")
    }

    pub fn render_namelist_wps(&self, geog_data_path: &str) -> String {
        let max_dom = self.max_dom;
        let start_dates = repeat_quoted(&wrf_time(&self.request.start_utc), max_dom);
        let end_dates = repeat_quoted(&wrf_time(&self.request.end_utc), max_dom);
        let d01 = &self.domains[0];
        format!(
            "&share\n wrf_core               = 'ARW',\n max_dom                = {max_dom},\n start_date             = {start_dates}\n end_date               = {end_dates}\n interval_seconds       = {},\n io_form_geogrid        = 2,\n nocolons               = .true.,\n/\n&geogrid\n parent_id              = {}\n parent_grid_ratio      = {}\n i_parent_start         = {}\n j_parent_start         = {}\n e_we                   = {}\n e_sn                   = {}\n geog_data_res          = {}\n dx                     = {:.1},\n dy                     = {:.1},\n map_proj               = 'lambert',\n ref_lat                = {:.4},\n ref_lon                = {:.4},\n truelat1               = {:.4},\n truelat2               = {:.4},\n stand_lon              = {:.4},\n geog_data_path         = '{}'\n/\n&ungrib\n out_format             = 'WPS',\n prefix                 = 'FILE',\n/\n&metgrid\n fg_name                = 'FILE',\n io_form_metgrid        = 2,\n/\n",
            self.interval_seconds,
            join_domain_values(&self.domains, |d| d.parent_id),
            join_domain_values(&self.domains, |d| d.parent_grid_ratio),
            join_domain_values(&self.domains, |d| d.i_parent_start),
            join_domain_values(&self.domains, |d| d.j_parent_start),
            join_domain_values(&self.domains, |d| d.e_we),
            join_domain_values(&self.domains, |d| d.e_sn),
            "'default', ".repeat(max_dom as usize),
            d01.dx_m,
            d01.dy_m,
            d01.ref_lat,
            d01.ref_lon,
            d01.truelat1,
            d01.truelat2,
            d01.stand_lon,
            geog_data_path,
        )
    }

    pub fn render_namelist_input(&self) -> String {
        let max_dom = self.max_dom;
        let nested = if self.max_dom > 1 {
            ".true."
        } else {
            ".false."
        };
        let history_interval = if self.max_dom > 1 {
            format!("0, {},", self.request.history_interval_minutes)
        } else {
            format!("{},", self.request.history_interval_minutes)
        };
        let frames_per_outfile = if self.max_dom > 1 { "0, 1," } else { "1," };
        let cu = if self.max_dom > 1 {
            "3, 0,".to_string()
        } else if self.inner_domain().dx_m > 5_000.0 {
            "3,".to_string()
        } else {
            "0,".to_string()
        };
        let surface_physics = match self.request.physics {
            WrfPhysicsPreset::SevereConvection
                if matches!(
                    self.request.init_source,
                    WrfInitSource::Hrrr | WrfInitSource::Rap
                ) =>
            {
                3
            }
            WrfPhysicsPreset::SevereConvection => 2,
            WrfPhysicsPreset::SevereConvectionNoahMp => 4,
        };
        let soil_layers = if surface_physics == 3 { 9 } else { 4 };

        let mut eta = String::new();
        for (idx, level) in DEFAULT_ETA_LEVELS.iter().enumerate() {
            if idx == 0 {
                let _ = write!(eta, "{level:.5}");
            } else if idx % 5 == 0 {
                let _ = write!(eta, ",\n              {level:.5}");
            } else {
                let _ = write!(eta, ", {level:.5}");
            }
        }

        format!(
            "&time_control\n run_hours                           = {},\n run_minutes                         = {},\n start_year                          = {}\n start_month                         = {}\n start_day                           = {}\n start_hour                          = {}\n start_minute                        = {}\n end_year                            = {}\n end_month                           = {}\n end_day                             = {}\n end_hour                            = {}\n end_minute                          = {}\n interval_seconds                    = {},\n input_from_file                     = {}\n history_interval                    = {history_interval}\n frames_per_outfile                  = {frames_per_outfile}\n restart                             = .false.,\n restart_interval                    = 60,\n nwp_diagnostics                     = 1,\n nocolons                            = .true.,\n/\n&domains\n time_step                           = {}\n max_dom                             = {},\n e_we                                = {}\n e_sn                                = {}\n e_vert                              = {}\n eta_levels = {eta}\n p_top_requested                     = 5000,\n num_metgrid_levels                  = {},\n num_metgrid_soil_levels             = {},\n dx                                  = {}\n dy                                  = {}\n grid_id                             = {}\n parent_id                           = {}\n i_parent_start                      = {}\n j_parent_start                      = {}\n parent_grid_ratio                   = {}\n parent_time_step_ratio              = {}\n feedback                            = 1,\n smooth_option                       = 0,\n sfcp_to_sfcp                        = .true.,\n/\n&physics\n mp_physics                          = {}\n ra_lw_physics                       = {}\n ra_sw_physics                       = {}\n radt                                = {}\n sf_sfclay_physics                   = {}\n sf_surface_physics                  = {}\n bl_pbl_physics                      = {}\n bldt                                = {}\n cu_physics                          = {cu}\n cudt                                = 5,\n isfflx                              = 1,\n ifsnow                              = 1,\n icloud                              = 1,\n surface_input_source                = 1,\n do_radar_ref                        = 1,\n num_soil_layers                     = {soil_layers},\n usemonalb                           = .true.,\n rdlai2d                             = .true.,\n sf_urban_physics                    = 0,\n/\n&fdda\n/\n&dynamics\n w_damping                           = 1,\n epssm                               = 0.2,\n diff_opt                            = 1,\n km_opt                              = 4,\n diff_6th_opt                        = 0,\n diff_6th_factor                     = 0.12,\n base_temp                           = 290.,\n damp_opt                            = 3,\n zdamp                               = 5000.,\n khdif                               = 0,\n kvdif                               = 0,\n non_hydrostatic                     = .true.,\n moist_adv_opt                       = 1,\n scalar_adv_opt                      = 1,\n/\n&bdy_control\n spec_bdy_width                      = 5,\n spec_zone                           = 1,\n relax_zone                          = 4,\n specified                           = .true.,\n nested                              = {nested}\n/\n&grib2\n/\n&namelist_quilt\n nio_tasks_per_group = 0,\n nio_groups = 1,\n/\n",
            self.run_hours,
            self.run_minutes,
            repeat_time_part(&self.request.start_utc, max_dom, 0),
            repeat_time_part(&self.request.start_utc, max_dom, 1),
            repeat_time_part(&self.request.start_utc, max_dom, 2),
            repeat_time_part(&self.request.start_utc, max_dom, 3),
            repeat_time_part(&self.request.start_utc, max_dom, 4),
            repeat_time_part(&self.request.end_utc, max_dom, 0),
            repeat_time_part(&self.request.end_utc, max_dom, 1),
            repeat_time_part(&self.request.end_utc, max_dom, 2),
            repeat_time_part(&self.request.end_utc, max_dom, 3),
            repeat_time_part(&self.request.end_utc, max_dom, 4),
            self.interval_seconds,
            " .true.,".repeat(max_dom as usize),
            self.time_step_seconds,
            max_dom,
            join_domain_values(&self.domains, |d| d.e_we),
            join_domain_values(&self.domains, |d| d.e_sn),
            "50,".repeat(max_dom as usize),
            self.num_metgrid_levels,
            self.num_metgrid_soil_levels,
            join_domain_values_f64(&self.domains, |d| d.dx_m),
            join_domain_values_f64(&self.domains, |d| d.dy_m),
            join_domain_values(&self.domains, |d| d.id),
            join_domain_values(&self.domains, |d| d.parent_id),
            join_domain_values(&self.domains, |d| d.i_parent_start),
            join_domain_values(&self.domains, |d| d.j_parent_start),
            join_domain_values(&self.domains, |d| d.parent_grid_ratio),
            join_domain_values(&self.domains, |d| d.parent_time_step_ratio),
            repeat_i32(8, max_dom),
            repeat_i32(4, max_dom),
            repeat_i32(4, max_dom),
            join_domain_values(&self.domains, |d| (d.dx_m / 1000.0).round() as u32),
            repeat_i32(5, max_dom),
            repeat_i32(surface_physics, max_dom),
            repeat_i32(5, max_dom),
            repeat_i32(0, max_dom),
        )
    }
}

fn cells_for_span(span_m: f64, dx_m: f64) -> u32 {
    (span_m / dx_m).ceil().max(1.0) as u32
}

fn nested_dim_for_span(span_m: f64, dx_m: f64, ratio: u16) -> u32 {
    let cells = cells_for_span(span_m, dx_m);
    let ratio = u32::from(ratio);
    let corrected_cells = cells.div_ceil(ratio) * ratio;
    corrected_cells + 1
}

fn padded_parent_cells(parent_span: u32) -> u32 {
    (f64::from(parent_span) * DEFAULT_OUTER_PADDING_FRACTION / 2.0).ceil() as u32
}

fn odd_dim(value: u32) -> u32 {
    if value % 2 == 0 { value + 1 } else { value }
}

fn join_domain_values<T: std::fmt::Display>(
    domains: &[WrfDomain],
    f: impl Fn(&WrfDomain) -> T,
) -> String {
    domains
        .iter()
        .map(|d| format!("{},", f(d)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn join_domain_values_f64(domains: &[WrfDomain], f: impl Fn(&WrfDomain) -> f64) -> String {
    domains
        .iter()
        .map(|d| format!("{:.1},", f(d)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn repeat_i32(value: i32, domains: u8) -> String {
    (0..domains)
        .map(|_| format!("{value},"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn repeat_quoted(value: &str, domains: u8) -> String {
    (0..domains)
        .map(|_| format!("'{value}',"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn wrf_time(value: &str) -> String {
    value
        .replace('T', "_")
        .trim_end_matches('Z')
        .replace('-', "-")
}

fn repeat_time_part(value: &str, domains: u8, part: usize) -> String {
    let parts = time_parts(value);
    (0..domains)
        .map(|_| format!(" {},", parts[part]))
        .collect::<Vec<_>>()
        .join("")
}

fn time_parts(value: &str) -> [String; 5] {
    let clean = value
        .trim_end_matches('Z')
        .replace('T', "-")
        .replace(':', "-");
    let parts = clean.split('-').collect::<Vec<_>>();
    [
        parts.first().unwrap_or(&"1970").to_string(),
        parts.get(1).unwrap_or(&"01").to_string(),
        parts.get(2).unwrap_or(&"01").to_string(),
        parts.get(3).unwrap_or(&"00").to_string(),
        parts.get(4).unwrap_or(&"00").to_string(),
    ]
}

fn parse_run_duration(start: &str, end: &str) -> Result<(u32, u32), WrfOpsError> {
    let start = parse_utc_minutes(start);
    let end = parse_utc_minutes(end);
    if end <= start {
        return Err(WrfOpsError::InvalidTimeRange);
    }
    let minutes = end - start;
    Ok(((minutes / 60) as u32, (minutes % 60) as u32))
}

fn parse_utc_minutes(value: &str) -> i64 {
    let parts = time_parts(value);
    let year = parts[0].parse::<i64>().unwrap_or(1970);
    let month = parts[1].parse::<i64>().unwrap_or(1);
    let day = parts[2].parse::<i64>().unwrap_or(1);
    let hour = parts[3].parse::<i64>().unwrap_or(0);
    let minute = parts[4].parse::<i64>().unwrap_or(0);
    days_from_civil(year, month, day) * 24 * 60 + hour * 60 + minute
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outbreak_bounds() -> WrfDomainBounds {
        WrfDomainBounds::new(-99.0, -78.0, 30.0, 43.0).unwrap()
    }

    #[test]
    fn bounds_can_be_built_from_center_and_size() {
        let bounds = WrfDomainBounds::from_center_km(40.1, -95.6, 700.0, 500.0).unwrap();

        assert!((bounds.center_lat() - 40.1).abs() < 1e-9);
        assert!((bounds.center_lon() + 95.6).abs() < 1e-9);
        assert!((bounds.width_m() / 1_000.0 - 700.0).abs() < 0.5);
        assert!((bounds.height_m() / 1_000.0 - 500.0).abs() < 0.5);
    }

    fn request(resolution: WrfNestedResolution) -> WrfOpsRequest {
        let mut request = WrfOpsRequest::severe_default(
            "test",
            WrfInitSource::Hrrr,
            "1974-04-03T09:00:00Z",
            "1974-04-04T06:00:00Z",
            outbreak_bounds(),
        );
        request.nested_resolution = resolution;
        request
    }

    #[test]
    fn supports_obvious_wrf_init_sources() {
        let slugs = WrfInitSource::supported_sources()
            .into_iter()
            .map(|source| source.slug)
            .collect::<Vec<_>>();
        for expected in [
            "hrrr",
            "rap",
            "gfs",
            "gefs",
            "ecmwf-open-data",
            "aifs-v2",
            "era5",
            "nam",
            "rrfs",
        ] {
            assert!(slugs.contains(&expected.to_string()));
        }
        assert_eq!(
            "euro".parse::<WrfInitSource>().unwrap(),
            WrfInitSource::EcmwfOpenData
        );
        assert_eq!(
            "aifs".parse::<WrfInitSource>().unwrap(),
            WrfInitSource::AifsV2
        );
        let hrrr = WrfInitSourceInfo::from(WrfInitSource::Hrrr);
        assert_eq!(hrrr.rustwx_model, Some(ModelId::Hrrr));
        assert!(hrrr.direct_stage_available);
        assert_eq!(hrrr.staging_mode, WrfInitStagingMode::RustwxDirect);
        assert_eq!(hrrr.default_vtable, "Vtable.GFS");
        assert_eq!(hrrr.default_wps_products, vec!["prs"]);
        assert_eq!(hrrr.default_num_metgrid_levels, 41);
        assert_eq!(hrrr.default_num_metgrid_soil_levels, 9);
        let era5 = WrfInitSourceInfo::from(WrfInitSource::Era5);
        assert_eq!(era5.default_num_metgrid_levels, 30);
        assert_eq!(era5.default_num_metgrid_soil_levels, 4);
        assert_eq!(era5.rustwx_model, None);
        assert!(!era5.direct_stage_available);
        assert_eq!(era5.staging_mode, WrfInitStagingMode::ExternalGribs);
    }

    #[test]
    fn default_three_km_nested_plan_satisfies_wrf_ratio_rules() {
        let plan = WrfOpsPlan::plan(request(WrfNestedResolution::Default3Km)).unwrap();
        assert_eq!(plan.max_dom, 2);
        assert_eq!(plan.domains[1].dx_m, 3_000.0);
        assert_nested_rules(&plan);
        assert!(plan.time_step_seconds > 0);
        assert_eq!((plan.run_hours, plan.run_minutes), (21, 0));
    }

    #[test]
    fn extra_resolution_one_point_five_km_nested_plan_satisfies_wrf_ratio_rules() {
        let plan = WrfOpsPlan::plan(request(WrfNestedResolution::Extra1p5Km)).unwrap();
        assert_eq!(plan.domains[1].dx_m, 1_500.0);
        assert_nested_rules(&plan);
    }

    #[test]
    fn special_five_hundred_meter_nested_plan_satisfies_wrf_ratio_rules() {
        let plan = WrfOpsPlan::plan(request(WrfNestedResolution::Special500M)).unwrap();
        assert_eq!(plan.domains[1].dx_m, 500.0);
        assert_nested_rules(&plan);
    }

    #[test]
    fn custom_nested_resolution_satisfies_wrf_ratio_rules() {
        let resolution = WrfNestedResolution::custom(750, 3).unwrap();
        let plan = WrfOpsPlan::plan(request(resolution)).unwrap();
        assert_eq!(plan.domains[1].dx_m, 750.0);
        assert_eq!(plan.domains[1].parent_grid_ratio, 3);
        assert_nested_rules(&plan);
        assert_eq!(
            WrfNestedResolution::custom(0, 3),
            Err(WrfOpsError::InvalidDxMeters(0.0))
        );
        assert_eq!(
            WrfNestedResolution::custom(750, 1),
            Err(WrfOpsError::InvalidParentRatio(1))
        );
    }

    #[test]
    fn rectangular_domain_shapes_still_satisfy_nested_rules() {
        let wide_bounds = WrfDomainBounds::new(-110.0, -72.0, 31.0, 38.0).unwrap();
        let tall_bounds = WrfDomainBounds::new(-93.0, -84.0, 24.0, 49.0).unwrap();
        let wide = WrfOpsPlan::plan(WrfOpsRequest::severe_default(
            "wide",
            WrfInitSource::Gfs,
            "2026-05-17T06:00:00Z",
            "2026-05-17T12:00:00Z",
            wide_bounds,
        ))
        .unwrap();
        let tall = WrfOpsPlan::plan(WrfOpsRequest::severe_default(
            "tall",
            WrfInitSource::Gfs,
            "2026-05-17T06:00:00Z",
            "2026-05-17T12:00:00Z",
            tall_bounds,
        ))
        .unwrap();

        assert_nested_rules(&wide);
        assert_nested_rules(&tall);
        assert!(wide.domains[1].e_we > wide.domains[1].e_sn);
        assert!(tall.domains[1].e_sn > tall.domains[1].e_we);
    }

    #[test]
    fn namelists_include_nested_and_hourly_init_contract() {
        let plan = WrfOpsPlan::plan(request(WrfNestedResolution::Default3Km)).unwrap();
        let wps = plan.render_namelist_wps("/home/drew/weather/wrf/WRF_BUILD/WPS_GEOG");
        let input = plan.render_namelist_input();
        assert!(wps.contains("max_dom                = 2"));
        assert!(wps.contains("interval_seconds       = 3600"));
        assert!(wps.contains("nocolons               = .true."));
        assert!(input.contains("history_interval                    = 0, 6,"));
        assert!(input.contains("nwp_diagnostics                     = 1,"));
        assert!(input.contains("do_radar_ref                        = 1,"));
        assert!(input.contains("num_metgrid_levels                  = 41,"));
        assert!(input.contains("num_metgrid_soil_levels             = 9,"));
        assert!(input.contains("sf_surface_physics                  = 3, 3,"));
        assert!(input.contains("num_soil_layers                     = 9,"));
        assert!(input.contains("nested                              = .true."));
        assert!(input.contains("sfcp_to_sfcp                        = .true."));
    }

    fn assert_nested_rules(plan: &WrfOpsPlan) {
        let parent = &plan.domains[0];
        let child = &plan.domains[1];
        let ratio = u32::from(child.parent_grid_ratio);
        assert_eq!((child.e_we - 1) % ratio, 0);
        assert_eq!((child.e_sn - 1) % ratio, 0);
        assert_eq!(parent.e_we % 2, 1);
        assert_eq!(parent.e_sn % 2, 1);
        assert!(child.i_parent_start > 1);
        assert!(child.j_parent_start > 1);
        assert!(child.i_parent_start + (child.e_we - 1) / ratio < parent.e_we);
        assert!(child.j_parent_start + (child.e_sn - 1) / ratio < parent.e_sn);
    }
}
