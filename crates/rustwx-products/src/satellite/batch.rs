use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use rustwx_render::{PngCompressionMode, PngWriteOptions, save_png_profile_with_options};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

use crate::publication::atomic_write_json;

use super::abi::read_goes_abi_scene;
use super::goes::{GoesSatellite, parse_goes_abi_filename};
use super::render::{GoesAbiBandMapRequest, build_goes_abi_band_render_request};
use super::rgb::{
    GoesAbiRgbCompositeRequest, GoesAbiRgbCompositeStyle,
    build_goes_abi_rgb_composite_render_request,
};

const DEFAULT_ABI_PRODUCT: &str = "ABI-L2-CMIPC";
const DEFAULT_DOMAIN_SLUG: &str = "pacific_southwest";
const DEFAULT_DOMAIN_LABEL: &str = "Pacific Southwest";
const DEFAULT_WIDTH: u32 = 1400;
const DEFAULT_HEIGHT: u32 = 1100;
const DEFAULT_SCAN_LOOKBACK_HOURS: u32 = 6;
const DEFAULT_DISCOVERY_RETRIES: u32 = 2;
const DEFAULT_RETRY_SLEEP_MS: u64 = 20_000;
const DEFAULT_GLM_FETCH_COUNT: usize = 90;
const DEFAULT_GLM_LOOKBACK_HOURS: u32 = 3;
const DEFAULT_GLM_MAX_AGE_MIN: f64 = 30.0;
const DEFAULT_AUTO_BOUNDS_SAMPLE_AXIS: usize = 96;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesSatelliteBatchRequest {
    #[serde(default = "default_satellite")]
    pub satellite: String,
    #[serde(default = "default_abi_product")]
    pub abi_product: String,
    #[serde(default, alias = "sector")]
    pub abi_sector: Option<String>,
    #[serde(default = "default_domain_slug")]
    pub domain_slug: String,
    #[serde(default = "default_domain_label")]
    pub domain_label: String,
    #[serde(default = "default_psw_bounds")]
    pub bounds: (f64, f64, f64, f64),
    pub out_dir: PathBuf,
    pub cache_dir: PathBuf,
    #[serde(default = "default_satellite_products")]
    pub products: Vec<String>,
    #[serde(default = "default_width")]
    pub width: u32,
    #[serde(default = "default_height")]
    pub height: u32,
    #[serde(default = "default_scan_lookback_hours")]
    pub scan_lookback_hours: u32,
    #[serde(default = "default_discovery_retries")]
    pub discovery_retries: u32,
    #[serde(default = "default_retry_sleep_ms")]
    pub retry_sleep_ms: u64,
    #[serde(default = "default_true")]
    pub use_cache: bool,
    #[serde(default = "default_true")]
    pub download_glm: bool,
    #[serde(default = "default_glm_fetch_count")]
    pub glm_fetch_count: usize,
    #[serde(default = "default_glm_lookback_hours")]
    pub glm_lookback_hours: u32,
    #[serde(default = "default_glm_max_age_min")]
    pub glm_max_age_min: f64,
    #[serde(default)]
    pub png_compression: PngCompressionMode,
    #[serde(default)]
    pub skip_scan_id: Option<String>,
    #[serde(default)]
    pub auto_bounds: bool,
    #[serde(default)]
    pub allow_high_resolution_full_disk: bool,
}

impl GoesSatelliteBatchRequest {
    pub fn pacific_southwest(out_dir: impl Into<PathBuf>, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            satellite: default_satellite(),
            abi_product: default_abi_product(),
            abi_sector: None,
            domain_slug: default_domain_slug(),
            domain_label: default_domain_label(),
            bounds: default_psw_bounds(),
            out_dir: out_dir.into(),
            cache_dir: cache_dir.into(),
            products: default_satellite_products(),
            width: DEFAULT_WIDTH,
            height: DEFAULT_HEIGHT,
            scan_lookback_hours: DEFAULT_SCAN_LOOKBACK_HOURS,
            discovery_retries: DEFAULT_DISCOVERY_RETRIES,
            retry_sleep_ms: DEFAULT_RETRY_SLEEP_MS,
            use_cache: true,
            download_glm: true,
            glm_fetch_count: DEFAULT_GLM_FETCH_COUNT,
            glm_lookback_hours: DEFAULT_GLM_LOOKBACK_HOURS,
            glm_max_age_min: DEFAULT_GLM_MAX_AGE_MIN,
            png_compression: PngCompressionMode::Fast,
            skip_scan_id: None,
            auto_bounds: false,
            allow_high_resolution_full_disk: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum GoesSatelliteProduct {
    GeoColor,
    GlmFedGeoColor,
    AirMassRgb,
    SandwichRgb,
    DayNightCloudMicroComboRgb,
    FireTemperatureRgb,
    DustRgb,
    AbiBand(u8),
}

impl GoesSatelliteProduct {
    pub fn parse(raw: &str) -> Result<Self, Box<dyn Error>> {
        let normalized = raw
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .replace(' ', "_");
        match normalized.as_str() {
            "geocolor"
            | "geo_color"
            | "goes_geocolor"
            | "natural_color"
            | "goes_natural_color_rgb" => Ok(Self::GeoColor),
            "glm_fed_geocolor" | "goes_glm_fed_geocolor" | "glm_geocolor" => {
                Ok(Self::GlmFedGeoColor)
            }
            "airmass" | "air_mass" | "airmass_rgb" | "goes_airmass_rgb" => Ok(Self::AirMassRgb),
            "sandwich" | "sandwich_rgb" | "goes_sandwich_rgb" => Ok(Self::SandwichRgb),
            "day_night_cloud_micro_combo"
            | "day_night_cloud_micro_combo_rgb"
            | "goes_day_night_cloud_micro_combo_rgb"
            | "day_cloud_phase"
            | "goes_day_cloud_phase_rgb" => Ok(Self::DayNightCloudMicroComboRgb),
            "fire_temperature"
            | "fire_temperature_rgb"
            | "goes_fire_temperature_rgb"
            | "fire_temp" => Ok(Self::FireTemperatureRgb),
            "dust" | "dust_rgb" | "goes_dust_rgb" => Ok(Self::DustRgb),
            _ => parse_band_product(&normalized)
                .map(Self::AbiBand)
                .ok_or_else(|| boxed_error(format!("unknown GOES satellite product: {raw}"))),
        }
    }

    pub fn slug(&self) -> String {
        match self {
            Self::GeoColor => "goes_geocolor".to_string(),
            Self::GlmFedGeoColor => "goes_glm_fed_geocolor".to_string(),
            Self::AirMassRgb => "goes_airmass_rgb".to_string(),
            Self::SandwichRgb => "goes_sandwich_rgb".to_string(),
            Self::DayNightCloudMicroComboRgb => "goes_day_night_cloud_micro_combo_rgb".to_string(),
            Self::FireTemperatureRgb => "goes_fire_temperature_rgb".to_string(),
            Self::DustRgb => "goes_dust_rgb".to_string(),
            Self::AbiBand(channel) => format!("goes_abi_band_{channel:02}"),
        }
    }

    pub fn title(&self) -> String {
        match self {
            Self::GeoColor => "GeoColor".to_string(),
            Self::GlmFedGeoColor => "GLM FED3+GeoColor".to_string(),
            Self::AirMassRgb => "AirMass RGB".to_string(),
            Self::SandwichRgb => "Sandwich RGB".to_string(),
            Self::DayNightCloudMicroComboRgb => "Day Night Cloud Micro Combo RGB".to_string(),
            Self::FireTemperatureRgb => "Fire Temperature".to_string(),
            Self::DustRgb => "Dust RGB".to_string(),
            Self::AbiBand(channel) => format!("Band {channel}"),
        }
    }

    pub fn required_channels(&self) -> Vec<u8> {
        match self {
            Self::GeoColor | Self::GlmFedGeoColor => GoesAbiRgbCompositeStyle::GeoColor
                .required_channels()
                .to_vec(),
            Self::AirMassRgb => GoesAbiRgbCompositeStyle::AirMass
                .required_channels()
                .to_vec(),
            Self::SandwichRgb => GoesAbiRgbCompositeStyle::Sandwich
                .required_channels()
                .to_vec(),
            Self::DayNightCloudMicroComboRgb => GoesAbiRgbCompositeStyle::DayNightCloudMicroCombo
                .required_channels()
                .to_vec(),
            Self::FireTemperatureRgb => GoesAbiRgbCompositeStyle::FireTemperature
                .required_channels()
                .to_vec(),
            Self::DustRgb => GoesAbiRgbCompositeStyle::Dust.required_channels().to_vec(),
            Self::AbiBand(channel) => vec![*channel],
        }
    }

    fn rgb_style(&self) -> Option<GoesAbiRgbCompositeStyle> {
        match self {
            Self::GeoColor | Self::GlmFedGeoColor => Some(GoesAbiRgbCompositeStyle::GeoColor),
            Self::AirMassRgb => Some(GoesAbiRgbCompositeStyle::AirMass),
            Self::SandwichRgb => Some(GoesAbiRgbCompositeStyle::Sandwich),
            Self::DayNightCloudMicroComboRgb => {
                Some(GoesAbiRgbCompositeStyle::DayNightCloudMicroCombo)
            }
            Self::FireTemperatureRgb => Some(GoesAbiRgbCompositeStyle::FireTemperature),
            Self::DustRgb => Some(GoesAbiRgbCompositeStyle::Dust),
            Self::AbiBand(_) => None,
        }
    }

    fn uses_glm(&self) -> bool {
        matches!(self, Self::GlmFedGeoColor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesSatelliteBatchReport {
    pub ok: bool,
    #[serde(default)]
    pub skipped: bool,
    pub generated_at_utc: DateTime<Utc>,
    pub satellite: String,
    pub source_bucket: String,
    pub abi_product: String,
    pub abi_sector: String,
    pub scan_id: String,
    pub scan_time_utc: DateTime<Utc>,
    pub scan_end_time_utc: DateTime<Utc>,
    pub domain: String,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub width: u32,
    pub height: u32,
    pub products: Vec<String>,
    pub source_keys: Vec<String>,
    pub glm_source_keys: Vec<String>,
    pub channel_files: BTreeMap<u8, GoesSourceFile>,
    pub artifacts: Vec<GoesSatelliteArtifact>,
    pub report_path: Option<PathBuf>,
    pub timing: GoesSatelliteBatchTiming,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GoesSatelliteBatchTiming {
    pub discovery_ms: u128,
    pub abi_download_ms: u128,
    pub glm_download_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesSatelliteArtifact {
    pub product: String,
    pub title: String,
    pub kind: String,
    pub band: Option<u8>,
    pub satellite: String,
    pub scan_time_utc: DateTime<Utc>,
    pub product_time_utc: DateTime<Utc>,
    pub domain: String,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub resolution: String,
    pub png_path: PathBuf,
    pub source_keys: Vec<String>,
    pub generated_at_utc: DateTime<Utc>,
    pub mapbox_overlay: MapboxOverlayMetadata,
    pub timing_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapboxOverlayMetadata {
    pub overlay_type: String,
    pub bounds: (f64, f64, f64, f64),
    pub coordinates: [[f64; 2]; 4],
    pub opacity: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesSourceFile {
    pub key: String,
    pub url: String,
    pub size_bytes: u64,
    pub last_modified: String,
    pub local_path: PathBuf,
    pub cache_hit: bool,
}

#[derive(Debug, Clone)]
struct S3Object {
    key: String,
    size_bytes: u64,
    last_modified: String,
}

#[derive(Debug, Clone)]
struct AbiScan {
    scan_id: String,
    start_time_utc: DateTime<Utc>,
    end_time_utc: DateTime<Utc>,
    channel_objects: BTreeMap<u8, S3Object>,
}

#[derive(Debug, Clone)]
struct DownloadedObject {
    object: S3Object,
    path: PathBuf,
    cache_hit: bool,
}

pub fn run_goes_satellite_batch(
    request: &GoesSatelliteBatchRequest,
) -> Result<GoesSatelliteBatchReport, Box<dyn Error>> {
    let total_start = Instant::now();
    validate_bounds(request.bounds)?;
    let abi_product = resolve_abi_product(&request.abi_product, request.abi_sector.as_deref())?;
    let abi_sector = sector_slug_for_abi_product(&abi_product);
    let product_inputs = product_inputs_for_request(
        &request.products,
        &abi_product,
        request.allow_high_resolution_full_disk,
    );
    let products = requested_products(&product_inputs)?;
    let product_slugs = products
        .iter()
        .map(GoesSatelliteProduct::slug)
        .collect::<Vec<_>>();
    let required_channels = required_channels(&products);
    validate_requested_channels_for_product(
        &abi_product,
        &required_channels,
        request.allow_high_resolution_full_disk,
    )?;
    let satellite = GoesSatellite::parse(&request.satellite);
    let satellite_slug = satellite.as_str().to_ascii_lowercase();
    let bucket = bucket_for_satellite(&request.satellite)?;
    fs::create_dir_all(&request.cache_dir)?;
    fs::create_dir_all(&request.out_dir)?;

    let agent = build_agent();
    let discovery_start = Instant::now();
    let scan = discover_latest_complete_scan(
        &agent,
        &bucket,
        &abi_product,
        &required_channels,
        request.scan_lookback_hours,
        request.discovery_retries,
        request.retry_sleep_ms,
    )?;
    let discovery_ms = discovery_start.elapsed().as_millis();
    let source_keys = scan
        .channel_objects
        .values()
        .map(|object| object.key.clone())
        .collect::<Vec<_>>();

    if request
        .skip_scan_id
        .as_deref()
        .is_some_and(|skip| skip == scan.scan_id)
    {
        return Ok(GoesSatelliteBatchReport {
            ok: true,
            skipped: true,
            generated_at_utc: Utc::now(),
            satellite: satellite.as_str().to_string(),
            source_bucket: bucket,
            abi_product,
            abi_sector,
            scan_id: scan.scan_id,
            scan_time_utc: scan.start_time_utc,
            scan_end_time_utc: scan.end_time_utc,
            domain: request.domain_slug.clone(),
            domain_label: request.domain_label.clone(),
            bounds: request.bounds,
            width: request.width,
            height: request.height,
            products: product_slugs,
            source_keys,
            glm_source_keys: Vec::new(),
            channel_files: BTreeMap::new(),
            artifacts: Vec::new(),
            report_path: None,
            timing: GoesSatelliteBatchTiming {
                discovery_ms,
                total_ms: total_start.elapsed().as_millis(),
                ..GoesSatelliteBatchTiming::default()
            },
        });
    }

    let abi_download_start = Instant::now();
    let channel_downloads = download_abi_channels(
        &agent,
        &bucket,
        &request.cache_dir,
        &scan,
        &required_channels,
        request.use_cache,
    )?;
    let abi_download_ms = abi_download_start.elapsed().as_millis();

    let include_glm = products.iter().any(GoesSatelliteProduct::uses_glm);
    let glm_download_start = Instant::now();
    let (glm_dir, glm_downloads) = if include_glm && request.download_glm {
        download_recent_glm(
            &agent,
            &bucket,
            &request.cache_dir,
            scan.end_time_utc,
            request.glm_lookback_hours,
            request.glm_fetch_count,
            request.use_cache,
        )?
    } else {
        (None, Vec::new())
    };
    let glm_download_ms = glm_download_start.elapsed().as_millis();
    let glm_source_keys = glm_downloads
        .iter()
        .map(|item| item.object.key.clone())
        .collect::<Vec<_>>();

    let run_dir = request
        .out_dir
        .join("satellite")
        .join(&satellite_slug)
        .join(sanitize_component(&request.domain_slug))
        .join(scan.start_time_utc.format("%Y%m%dT%H%M%SZ").to_string());
    fs::create_dir_all(&run_dir)?;

    let render_start = Instant::now();
    let mut artifacts = Vec::new();
    for product in &products {
        let artifact = render_product(
            product,
            request,
            &run_dir,
            &channel_downloads,
            glm_dir.as_ref(),
            &satellite,
            &scan,
        )?;
        artifacts.push(artifact);
    }
    let render_ms = render_start.elapsed().as_millis();

    let channel_files = channel_downloads
        .iter()
        .map(|(&channel, download)| {
            (
                channel,
                GoesSourceFile {
                    key: download.object.key.clone(),
                    url: object_url(&bucket, &download.object.key),
                    size_bytes: download.object.size_bytes,
                    last_modified: download.object.last_modified.clone(),
                    local_path: download.path.clone(),
                    cache_hit: download.cache_hit,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let report_path = run_dir.join("rustwx_satellite_report.json");
    let report = GoesSatelliteBatchReport {
        ok: true,
        skipped: false,
        generated_at_utc: Utc::now(),
        satellite: satellite.as_str().to_string(),
        source_bucket: bucket,
        abi_product,
        abi_sector: abi_sector.clone(),
        scan_id: scan.scan_id,
        scan_time_utc: scan.start_time_utc,
        scan_end_time_utc: scan.end_time_utc,
        domain: request.domain_slug.clone(),
        domain_label: request.domain_label.clone(),
        bounds: request.bounds,
        width: request.width,
        height: request.height,
        products: product_slugs,
        source_keys,
        glm_source_keys,
        channel_files,
        artifacts,
        report_path: Some(report_path.clone()),
        timing: GoesSatelliteBatchTiming {
            discovery_ms,
            abi_download_ms,
            glm_download_ms,
            render_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    };
    atomic_write_json(&report_path, &report)?;
    Ok(report)
}

fn render_product(
    product: &GoesSatelliteProduct,
    request: &GoesSatelliteBatchRequest,
    run_dir: &Path,
    channel_downloads: &BTreeMap<u8, DownloadedObject>,
    glm_dir: Option<&PathBuf>,
    satellite: &GoesSatellite,
    scan: &AbiScan,
) -> Result<GoesSatelliteArtifact, Box<dyn Error>> {
    let started = Instant::now();
    let slug = product.slug();
    let png_path = run_dir.join(format!("{slug}.png"));
    let product_channels = product.required_channels();
    let source_keys = product_channels
        .iter()
        .filter_map(|channel| channel_downloads.get(channel))
        .map(|download| download.object.key.clone())
        .collect::<Vec<_>>();

    if let Some(style) = product.rgb_style() {
        let channel_paths = product_channels
            .iter()
            .map(|channel| {
                channel_downloads
                    .get(channel)
                    .map(|download| (*channel, download.path.clone()))
                    .ok_or_else(|| boxed_error(format!("missing downloaded ABI C{channel:02}")))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let base_channel = style.base_channel();
        let base_path = channel_paths.get(&base_channel).ok_or_else(|| {
            boxed_error(format!(
                "missing GOES ABI base channel C{base_channel:02} for {}",
                product.slug()
            ))
        })?;
        let render_bounds = render_bounds_for_path(request, base_path)?;
        let render_request =
            build_goes_abi_rgb_composite_render_request(&GoesAbiRgbCompositeRequest {
                channel_paths,
                composite_style: style,
                domain_label: request.domain_label.clone(),
                bounds: render_bounds,
                width: request.width,
                height: request.height,
                glm_data_dir: product.uses_glm().then(|| glm_dir.cloned()).flatten(),
                glm_max_age_min: request.glm_max_age_min,
            })?;
        save_png_profile_with_options(
            &render_request,
            &png_path,
            &PngWriteOptions {
                compression: request.png_compression,
            },
        )?;
    } else if let GoesSatelliteProduct::AbiBand(channel) = product {
        let download = channel_downloads
            .get(channel)
            .ok_or_else(|| boxed_error(format!("missing downloaded ABI C{channel:02}")))?;
        let render_bounds = render_bounds_for_path(request, &download.path)?;
        let render_request = build_goes_abi_band_render_request(&GoesAbiBandMapRequest {
            abi_path: download.path.clone(),
            variable_name: "CMI".to_string(),
            channel: *channel,
            domain_label: request.domain_label.clone(),
            bounds: render_bounds,
            width: request.width,
            height: request.height,
        })?;
        save_png_profile_with_options(
            &render_request,
            &png_path,
            &PngWriteOptions {
                compression: request.png_compression,
            },
        )?;
    }

    let artifact_bounds = artifact_bounds_for_product(product, request, channel_downloads)?;

    Ok(GoesSatelliteArtifact {
        product: slug,
        title: product.title(),
        kind: if product.uses_glm() {
            "glm_overlay_rgb".to_string()
        } else if matches!(product, GoesSatelliteProduct::AbiBand(_)) {
            "abi_single_band".to_string()
        } else {
            "abi_rgb_composite".to_string()
        },
        band: match product {
            GoesSatelliteProduct::AbiBand(channel) => Some(*channel),
            _ => None,
        },
        satellite: satellite.as_str().to_string(),
        scan_time_utc: scan.start_time_utc,
        product_time_utc: scan.start_time_utc,
        domain: request.domain_slug.clone(),
        domain_label: request.domain_label.clone(),
        bounds: artifact_bounds,
        resolution: product_resolution(product),
        png_path,
        source_keys,
        generated_at_utc: Utc::now(),
        mapbox_overlay: mapbox_overlay(artifact_bounds, scan.start_time_utc),
        timing_ms: started.elapsed().as_millis(),
    })
}

fn discover_latest_complete_scan(
    agent: &ureq::Agent,
    bucket: &str,
    abi_product: &str,
    required_channels: &[u8],
    lookback_hours: u32,
    retries: u32,
    retry_sleep_ms: u64,
) -> Result<AbiScan, Box<dyn Error>> {
    let mut last_error = None;
    for attempt in 0..=retries {
        match try_discover_latest_complete_scan(
            agent,
            bucket,
            abi_product,
            required_channels,
            lookback_hours,
        ) {
            Ok(scan) => return Ok(scan),
            Err(err) => {
                last_error = Some(err.to_string());
                if attempt < retries && retry_sleep_ms > 0 {
                    thread::sleep(std::time::Duration::from_millis(retry_sleep_ms));
                }
            }
        }
    }
    Err(boxed_error(format!(
        "no complete GOES ABI scan found for {abi_product} channels {:?}: {}",
        required_channels,
        last_error.unwrap_or_else(|| "no matching objects".to_string())
    )))
}

fn try_discover_latest_complete_scan(
    agent: &ureq::Agent,
    bucket: &str,
    abi_product: &str,
    required_channels: &[u8],
    lookback_hours: u32,
) -> Result<AbiScan, Box<dyn Error>> {
    let now = Utc::now();
    let mut groups = BTreeMap::<DateTime<Utc>, BTreeMap<u8, S3Object>>::new();
    for offset in 0..lookback_hours.max(1) {
        let hour = now - Duration::hours(i64::from(offset));
        let prefix = goes_hour_prefix(abi_product, hour);
        for object in list_s3_objects(agent, bucket, &prefix)? {
            if !object.key.ends_with(".nc") {
                continue;
            }
            let parsed = match parse_goes_abi_filename(object_filename(&object.key)) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            if !abi_filename_product_matches_request(&parsed.product, abi_product) {
                continue;
            }
            let Some(channel) = parsed.channel else {
                continue;
            };
            if !required_channels.contains(&channel) {
                continue;
            }
            groups
                .entry(parsed.start_time_utc)
                .or_default()
                .insert(channel, object);
        }
    }

    for (start_time, channel_objects) in groups.into_iter().rev() {
        if required_channels
            .iter()
            .all(|channel| channel_objects.contains_key(channel))
        {
            let mut end_time = start_time;
            for object in channel_objects.values() {
                if let Ok(parsed) = parse_goes_abi_filename(object_filename(&object.key)) {
                    end_time = end_time.max(parsed.end_time_utc);
                }
            }
            let satellite = channel_objects
                .values()
                .find_map(|object| parse_goes_abi_filename(object_filename(&object.key)).ok())
                .map(|parsed| parsed.satellite.as_str().to_string())
                .unwrap_or_else(|| "GOES".to_string());
            return Ok(AbiScan {
                scan_id: format!(
                    "{}_{}_{}",
                    satellite,
                    abi_product,
                    start_time.format("%Y%m%dT%H%M%SZ")
                ),
                start_time_utc: start_time,
                end_time_utc: end_time,
                channel_objects,
            });
        }
    }
    Err(boxed_error(
        "listed recent ABI hours but did not find a complete channel set",
    ))
}

fn download_abi_channels(
    agent: &ureq::Agent,
    bucket: &str,
    cache_dir: &Path,
    scan: &AbiScan,
    required_channels: &[u8],
    use_cache: bool,
) -> Result<BTreeMap<u8, DownloadedObject>, Box<dyn Error>> {
    let mut downloads = BTreeMap::new();
    for channel in required_channels {
        let object = scan
            .channel_objects
            .get(channel)
            .ok_or_else(|| boxed_error(format!("missing ABI C{channel:02} in discovered scan")))?;
        let downloaded = download_object(agent, bucket, cache_dir, object, use_cache)?;
        downloads.insert(*channel, downloaded);
    }
    Ok(downloads)
}

fn download_recent_glm(
    agent: &ureq::Agent,
    bucket: &str,
    cache_dir: &Path,
    reference_time: DateTime<Utc>,
    lookback_hours: u32,
    fetch_count: usize,
    use_cache: bool,
) -> Result<(Option<PathBuf>, Vec<DownloadedObject>), Box<dyn Error>> {
    let mut objects = BTreeMap::<String, S3Object>::new();
    for offset in 0..lookback_hours.max(1) {
        let hour = reference_time - Duration::hours(i64::from(offset));
        let prefix = goes_hour_prefix("GLM-L2-LCFA", hour);
        for object in list_s3_objects(agent, bucket, &prefix)? {
            if !object.key.ends_with(".nc") {
                continue;
            }
            if glm_object_time(&object)
                .is_some_and(|time| time > reference_time + Duration::minutes(5))
            {
                continue;
            }
            objects.insert(object.key.clone(), object);
        }
    }

    let selected = objects
        .into_values()
        .rev()
        .take(fetch_count.max(1))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok((None, Vec::new()));
    }

    let glm_dir = cache_dir
        .join("satellite")
        .join(bucket)
        .join("GLM-L2-LCFA")
        .join("latest");
    fs::create_dir_all(&glm_dir)?;
    let selected_names = selected
        .iter()
        .map(|object| object_filename(&object.key).to_string())
        .collect::<BTreeSet<_>>();
    let mut downloads = Vec::new();
    for object in &selected {
        downloads.push(download_object_to_dir(
            agent, bucket, &glm_dir, object, use_cache,
        )?);
    }
    prune_unselected_nc_files(&glm_dir, &selected_names)?;
    Ok((Some(glm_dir), downloads))
}

fn download_object(
    agent: &ureq::Agent,
    bucket: &str,
    cache_dir: &Path,
    object: &S3Object,
    use_cache: bool,
) -> Result<DownloadedObject, Box<dyn Error>> {
    let target = cache_dir.join("satellite").join(bucket).join(&object.key);
    download_object_to_path(agent, bucket, &target, object, use_cache)
}

fn download_object_to_dir(
    agent: &ureq::Agent,
    bucket: &str,
    dir: &Path,
    object: &S3Object,
    use_cache: bool,
) -> Result<DownloadedObject, Box<dyn Error>> {
    download_object_to_path(
        agent,
        bucket,
        &dir.join(object_filename(&object.key)),
        object,
        use_cache,
    )
}

fn download_object_to_path(
    agent: &ureq::Agent,
    bucket: &str,
    target: &Path,
    object: &S3Object,
    use_cache: bool,
) -> Result<DownloadedObject, Box<dyn Error>> {
    if use_cache && target.exists() && target.metadata()?.len() == object.size_bytes {
        return Ok(DownloadedObject {
            object: object.clone(),
            path: target.to_path_buf(),
            cache_hit: true,
        });
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let url = object_url(bucket, &object.key);
    let mut response = agent.get(&url).call()?;
    let limit = object
        .size_bytes
        .saturating_add(16 * 1024 * 1024)
        .max(32 * 1024 * 1024);
    let bytes = response
        .body_mut()
        .with_config()
        .limit(limit)
        .read_to_vec()?;
    if object.size_bytes > 0 && bytes.len() as u64 != object.size_bytes {
        return Err(boxed_error(format!(
            "downloaded byte count mismatch for {}: expected {}, got {}",
            object.key,
            object.size_bytes,
            bytes.len()
        )));
    }
    crate::publication::atomic_write_bytes(target, &bytes)?;
    Ok(DownloadedObject {
        object: object.clone(),
        path: target.to_path_buf(),
        cache_hit: false,
    })
}

fn list_s3_objects(
    agent: &ureq::Agent,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<S3Object>, Box<dyn Error>> {
    let url =
        format!("https://{bucket}.s3.amazonaws.com/?list-type=2&prefix={prefix}&max-keys=1000");
    let mut response = agent.get(&url).call()?;
    let xml = response.body_mut().read_to_string()?;
    Ok(parse_s3_list_xml(&xml))
}

fn parse_s3_list_xml(xml: &str) -> Vec<S3Object> {
    let mut objects = Vec::new();
    for contents in xml.split("<Contents>").skip(1) {
        let end = contents.find("</Contents>").unwrap_or(contents.len());
        let block = &contents[..end];
        let key = extract_xml_tag(block, "Key").unwrap_or_default();
        if key.is_empty() {
            continue;
        }
        let size_bytes = extract_xml_tag(block, "Size")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let last_modified = extract_xml_tag(block, "LastModified").unwrap_or_default();
        objects.push(S3Object {
            key,
            size_bytes,
            last_modified,
        });
    }
    objects
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn requested_products(
    raw_products: &[String],
) -> Result<Vec<GoesSatelliteProduct>, Box<dyn Error>> {
    let products = if raw_products.is_empty() {
        default_satellite_products()
    } else {
        raw_products.to_vec()
    };
    let mut parsed = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in products {
        let product = GoesSatelliteProduct::parse(&raw)?;
        if seen.insert(product.slug()) {
            parsed.push(product);
        }
    }
    Ok(parsed)
}

fn resolve_abi_product(product: &str, sector: Option<&str>) -> Result<String, Box<dyn Error>> {
    let Some(raw_sector) = sector.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(product.trim().to_string());
    };
    let normalized = raw_sector
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_");
    let suffix = match normalized.as_str() {
        "conus" | "continental_us" | "continental_united_states" | "c" => "C",
        "full" | "full_disk" | "fulldisk" | "full_disc" | "fulldisc" | "fd" | "f" => "F",
        "meso" | "mesoscale" => "M",
        "meso1" | "mesoscale1" | "mesoscale_1" | "m1" => "M1",
        "meso2" | "mesoscale2" | "mesoscale_2" | "m2" => "M2",
        _ => {
            return Err(boxed_error(format!(
                "unsupported GOES ABI sector '{raw_sector}', expected conus, full_disk, meso1, or meso2"
            )));
        }
    };
    Ok(format!("ABI-L2-CMIP{suffix}"))
}

fn sector_slug_for_abi_product(product: &str) -> String {
    let upper = product.trim().to_ascii_uppercase();
    if upper.ends_with("M1") {
        "mesoscale_1".to_string()
    } else if upper.ends_with("M2") {
        "mesoscale_2".to_string()
    } else if upper.ends_with('M') {
        "mesoscale".to_string()
    } else if upper.ends_with('F') {
        "full_disk".to_string()
    } else if upper.ends_with('C') {
        "conus".to_string()
    } else {
        "unknown".to_string()
    }
}

fn product_inputs_for_request(
    raw_products: &[String],
    abi_product: &str,
    allow_high_resolution_full_disk: bool,
) -> Vec<String> {
    let defaults = default_satellite_products();
    let products = if raw_products.is_empty() {
        defaults.clone()
    } else {
        raw_products.to_vec()
    };
    if is_full_disk_product(abi_product) && !allow_high_resolution_full_disk && products == defaults
    {
        return default_full_disk_satellite_products();
    }
    products
}

fn default_full_disk_satellite_products() -> Vec<String> {
    vec![
        "goes_abi_band_13".to_string(),
        "goes_airmass_rgb".to_string(),
        "goes_dust_rgb".to_string(),
    ]
}

fn validate_requested_channels_for_product(
    abi_product: &str,
    channels: &[u8],
    allow_high_resolution_full_disk: bool,
) -> Result<(), Box<dyn Error>> {
    if !is_full_disk_product(abi_product) || allow_high_resolution_full_disk {
        return Ok(());
    }
    let restricted = channels
        .iter()
        .copied()
        .filter(|channel| matches!(channel, 1 | 2 | 3 | 5))
        .collect::<Vec<_>>();
    if restricted.is_empty() {
        return Ok(());
    }
    Err(boxed_error(format!(
        "full-disk GOES channels C{} are high-resolution and can require very large memory; request lower-resolution IR/RGB products or set allow_high_resolution_full_disk=true",
        restricted
            .iter()
            .map(|channel| format!("{channel:02}"))
            .collect::<Vec<_>>()
            .join(",C")
    )))
}

fn is_full_disk_product(abi_product: &str) -> bool {
    abi_product.trim().to_ascii_uppercase().ends_with('F')
}

fn render_bounds_for_path(
    request: &GoesSatelliteBatchRequest,
    path: &Path,
) -> Result<(f64, f64, f64, f64), Box<dyn Error>> {
    if !request.auto_bounds {
        return Ok(request.bounds);
    }
    let scene = read_goes_abi_scene(path)?;
    let bounds = scene
        .approximate_lat_lon_bounds(DEFAULT_AUTO_BOUNDS_SAMPLE_AXIS)
        .ok_or_else(|| {
            boxed_error(format!(
                "could not infer GOES scene bounds from {}",
                path.display()
            ))
        })?;
    Ok(pad_bounds(bounds, 0.02))
}

fn artifact_bounds_for_product(
    product: &GoesSatelliteProduct,
    request: &GoesSatelliteBatchRequest,
    channel_downloads: &BTreeMap<u8, DownloadedObject>,
) -> Result<(f64, f64, f64, f64), Box<dyn Error>> {
    if !request.auto_bounds {
        return Ok(request.bounds);
    }
    let channel = product
        .rgb_style()
        .map(GoesAbiRgbCompositeStyle::base_channel)
        .or_else(|| match product {
            GoesSatelliteProduct::AbiBand(channel) => Some(*channel),
            _ => None,
        })
        .ok_or_else(|| {
            boxed_error(format!(
                "cannot infer artifact bounds for {}",
                product.slug()
            ))
        })?;
    let download = channel_downloads
        .get(&channel)
        .ok_or_else(|| boxed_error(format!("missing downloaded ABI C{channel:02}")))?;
    render_bounds_for_path(request, &download.path)
}

fn pad_bounds(bounds: (f64, f64, f64, f64), fraction: f64) -> (f64, f64, f64, f64) {
    let (west, east, south, north) = bounds;
    let lon_pad = ((east - west) * fraction).max(0.1);
    let lat_pad = ((north - south) * fraction).max(0.1);
    (
        (west - lon_pad).clamp(-180.0, 180.0),
        (east + lon_pad).clamp(-180.0, 180.0),
        (south - lat_pad).clamp(-90.0, 90.0),
        (north + lat_pad).clamp(-90.0, 90.0),
    )
}

fn required_channels(products: &[GoesSatelliteProduct]) -> Vec<u8> {
    let mut channels = BTreeSet::new();
    for product in products {
        channels.extend(product.required_channels());
    }
    channels.into_iter().collect()
}

fn parse_band_product(normalized: &str) -> Option<u8> {
    let raw = normalized
        .strip_prefix("goes_abi_band_")
        .or_else(|| normalized.strip_prefix("abi_band_"))
        .or_else(|| normalized.strip_prefix("band_"))
        .or_else(|| normalized.strip_prefix("band"))
        .or_else(|| normalized.strip_prefix('c'))?;
    let channel = raw.parse::<u8>().ok()?;
    (1..=16).contains(&channel).then_some(channel)
}

fn bucket_for_satellite(satellite: &str) -> Result<String, Box<dyn Error>> {
    let normalized = satellite.trim().to_ascii_lowercase().replace('-', "");
    match normalized.as_str() {
        "g16" | "goes16" => Ok("noaa-goes16".to_string()),
        "g17" | "goes17" => Ok("noaa-goes17".to_string()),
        "g18" | "goes18" => Ok("noaa-goes18".to_string()),
        "g19" | "goes19" => Ok("noaa-goes19".to_string()),
        value if value.starts_with("noaagoes") => Ok(value.replacen("noaagoes", "noaa-goes", 1)),
        value if value.starts_with("noaa-goes") => Ok(value.to_string()),
        _ => Err(boxed_error(format!(
            "unsupported GOES satellite: {satellite}"
        ))),
    }
}

fn goes_hour_prefix(product: &str, hour: DateTime<Utc>) -> String {
    let product = goes_s3_prefix_product(product);
    format!(
        "{}/{:04}/{:03}/{:02}/",
        product,
        hour.year(),
        hour.ordinal(),
        hour.hour()
    )
}

fn goes_s3_prefix_product(product: &str) -> String {
    let trimmed = product.trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.ends_with("M1") || upper.ends_with("M2") {
        trimmed[..trimmed.len().saturating_sub(1)].to_string()
    } else {
        trimmed.to_string()
    }
}

fn abi_filename_product_matches_request(actual_product: &str, requested_product: &str) -> bool {
    let actual = actual_product.trim().to_ascii_uppercase();
    let requested = requested_product.trim().to_ascii_uppercase();
    actual == requested
        || (requested.ends_with('M')
            && (actual == format!("{requested}1") || actual == format!("{requested}2")))
}

fn object_url(bucket: &str, key: &str) -> String {
    format!("https://{bucket}.s3.amazonaws.com/{key}")
}

fn object_filename(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

fn glm_object_time(object: &S3Object) -> Option<DateTime<Utc>> {
    parse_goes_abi_filename(object_filename(&object.key))
        .ok()
        .map(|parsed| parsed.start_time_utc)
}

fn product_resolution(product: &GoesSatelliteProduct) -> String {
    match product {
        GoesSatelliteProduct::AbiBand(channel) => {
            format!("{} km ABI native", abi_nominal_resolution_km(*channel))
        }
        _ => "mixed ABI native channels".to_string(),
    }
}

fn abi_nominal_resolution_km(channel: u8) -> &'static str {
    match channel {
        2 => "0.5",
        1 | 3 | 5 => "1",
        _ => "2",
    }
}

fn mapbox_overlay(bounds: (f64, f64, f64, f64), timestamp: DateTime<Utc>) -> MapboxOverlayMetadata {
    let (west, east, south, north) = bounds;
    MapboxOverlayMetadata {
        overlay_type: "image".to_string(),
        bounds,
        coordinates: [[west, north], [east, north], [east, south], [west, south]],
        opacity: 0.78,
        timestamp,
    }
}

fn validate_bounds(bounds: (f64, f64, f64, f64)) -> Result<(), Box<dyn Error>> {
    let (west, east, south, north) = bounds;
    if !west.is_finite()
        || !east.is_finite()
        || !south.is_finite()
        || !north.is_finite()
        || !(-90.0..=90.0).contains(&south)
        || !(-90.0..=90.0).contains(&north)
        || south >= north
    {
        return Err(boxed_error(
            "bounds must be finite [west,east,south,north] with valid latitudes and south < north",
        ));
    }
    Ok(())
}

fn prune_unselected_nc_files(
    dir: &Path,
    selected_names: &BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("nc"))
        {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if !selected_names.contains(name) {
                let _ = fs::remove_file(path);
            }
        }
    }
    Ok(())
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

fn sanitize_component(value: &str) -> String {
    let mut out = String::new();
    let mut last_sep = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_sep = false;
        } else if !last_sep {
            out.push('_');
            last_sep = true;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

fn default_satellite() -> String {
    "goes18".to_string()
}

fn default_abi_product() -> String {
    DEFAULT_ABI_PRODUCT.to_string()
}

fn default_domain_slug() -> String {
    DEFAULT_DOMAIN_SLUG.to_string()
}

fn default_domain_label() -> String {
    DEFAULT_DOMAIN_LABEL.to_string()
}

fn default_psw_bounds() -> (f64, f64, f64, f64) {
    (-127.0, -111.0, 30.0, 44.5)
}

fn default_satellite_products() -> Vec<String> {
    let mut products = vec![
        "goes_geocolor".to_string(),
        "goes_glm_fed_geocolor".to_string(),
        "goes_airmass_rgb".to_string(),
        "goes_sandwich_rgb".to_string(),
        "goes_day_night_cloud_micro_combo_rgb".to_string(),
        "goes_fire_temperature_rgb".to_string(),
        "goes_dust_rgb".to_string(),
    ];
    products.extend((1..=16).map(|channel| format!("goes_abi_band_{channel:02}")));
    products
}

fn default_width() -> u32 {
    DEFAULT_WIDTH
}

fn default_height() -> u32 {
    DEFAULT_HEIGHT
}

fn default_scan_lookback_hours() -> u32 {
    DEFAULT_SCAN_LOOKBACK_HOURS
}

fn default_discovery_retries() -> u32 {
    DEFAULT_DISCOVERY_RETRIES
}

fn default_retry_sleep_ms() -> u64 {
    DEFAULT_RETRY_SLEEP_MS
}

fn default_glm_fetch_count() -> usize {
    DEFAULT_GLM_FETCH_COUNT
}

fn default_glm_lookback_hours() -> u32 {
    DEFAULT_GLM_LOOKBACK_HOURS
}

fn default_glm_max_age_min() -> f64 {
    DEFAULT_GLM_MAX_AGE_MIN
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_satellite_product_slugs() {
        assert_eq!(
            GoesSatelliteProduct::parse("goes_geocolor").unwrap(),
            GoesSatelliteProduct::GeoColor
        );
        assert_eq!(
            GoesSatelliteProduct::parse("goes_abi_band_13").unwrap(),
            GoesSatelliteProduct::AbiBand(13)
        );
        assert_eq!(
            GoesSatelliteProduct::parse("C02").unwrap(),
            GoesSatelliteProduct::AbiBand(2)
        );
    }

    #[test]
    fn required_channels_are_deduped_and_sorted() {
        let products = vec![
            GoesSatelliteProduct::FireTemperatureRgb,
            GoesSatelliteProduct::AbiBand(13),
            GoesSatelliteProduct::DustRgb,
        ];
        assert_eq!(required_channels(&products), vec![5, 6, 7, 11, 13, 14, 15]);
    }

    #[test]
    fn parses_s3_list_objects() {
        let xml = r#"
        <ListBucketResult>
          <Contents>
            <Key>ABI-L2-CMIPC/2026/118/06/OR_ABI-L2-CMIPC-M6C13_G18_s20261180646171_e20261180648556_c20261180649033.nc</Key>
            <LastModified>2026-04-28T06:49:04.000Z</LastModified>
            <Size>123</Size>
          </Contents>
        </ListBucketResult>
        "#;
        let objects = parse_s3_list_xml(xml);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].size_bytes, 123);
        assert!(objects[0].key.ends_with(".nc"));
    }

    #[test]
    fn bucket_parser_accepts_goes_west_aliases() {
        assert_eq!(bucket_for_satellite("G18").unwrap(), "noaa-goes18");
        assert_eq!(bucket_for_satellite("goes-18").unwrap(), "noaa-goes18");
        assert_eq!(bucket_for_satellite("noaa-goes18").unwrap(), "noaa-goes18");
    }

    #[test]
    fn sector_aliases_resolve_to_abi_products() {
        assert_eq!(
            resolve_abi_product("ABI-L2-CMIPC", Some("full-disk")).unwrap(),
            "ABI-L2-CMIPF"
        );
        assert_eq!(
            resolve_abi_product("ABI-L2-CMIPC", Some("meso1")).unwrap(),
            "ABI-L2-CMIPM1"
        );
        assert_eq!(
            resolve_abi_product("ABI-L2-CMIPC", Some("m2")).unwrap(),
            "ABI-L2-CMIPM2"
        );
        assert_eq!(sector_slug_for_abi_product("ABI-L2-CMIPF"), "full_disk");
        assert_eq!(sector_slug_for_abi_product("ABI-L2-CMIPM1"), "mesoscale_1");
        assert_eq!(goes_s3_prefix_product("ABI-L2-CMIPM1"), "ABI-L2-CMIPM");
        assert!(abi_filename_product_matches_request(
            "ABI-L2-CMIPM1",
            "ABI-L2-CMIPM"
        ));
        assert!(abi_filename_product_matches_request(
            "ABI-L2-CMIPM1",
            "ABI-L2-CMIPM1"
        ));
        assert!(!abi_filename_product_matches_request(
            "ABI-L2-CMIPM2",
            "ABI-L2-CMIPM1"
        ));
    }

    #[test]
    fn full_disk_defaults_avoid_high_resolution_visible_channels() {
        let products =
            product_inputs_for_request(&default_satellite_products(), "ABI-L2-CMIPF", false);
        let parsed = requested_products(&products).unwrap();
        assert_eq!(required_channels(&parsed), vec![8, 10, 11, 12, 13, 14, 15]);
    }

    #[test]
    fn full_disk_rejects_high_resolution_visible_channels_without_opt_in() {
        let err =
            validate_requested_channels_for_product("ABI-L2-CMIPF", &[2, 13], false).unwrap_err();
        assert!(
            err.to_string()
                .contains("full-disk GOES channels C02 are high-resolution")
        );
        validate_requested_channels_for_product("ABI-L2-CMIPF", &[2, 13], true).unwrap();
    }
}
