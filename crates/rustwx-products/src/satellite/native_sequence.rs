use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use image::RgbaImage;
use rayon::prelude::*;
use rustwx_render::{
    Color, PngCompressionMode, PngWriteOptions, save_rgba_png_profile_with_options,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

use crate::publication::{atomic_write_bytes, atomic_write_json};

use super::abi::{GoesAbiField, GoesAbiScene, read_goes_abi_field_window, read_goes_abi_scene};
use super::batch::{GoesSatelliteProduct, GoesSourceFile};
use super::geostationary::lat_lon_to_scan_angles_fast;
use super::goes::{GoesSatellite, parse_goes_abi_filename};
use super::rgb::{GoesAbiRgbCompositeStyle, compose_goes_abi_rgb_pixel};

const DEFAULT_ABI_PRODUCT: &str = "ABI-L2-CMIPC";
const DEFAULT_LOOKBACK_HOURS: u32 = 6;
const DEFAULT_DISCOVERY_RETRIES: u32 = 1;
const DEFAULT_RETRY_SLEEP_MS: u64 = 10_000;
const DEFAULT_DOWNLOAD_WORKERS: usize = 8;
const DEFAULT_RENDER_WORKERS: usize = 0;
const DEFAULT_DOMAIN_SAMPLE_POINTS: usize = 48;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesNativeSequenceRequest {
    #[serde(default = "default_satellite")]
    pub satellite: String,
    #[serde(default = "default_abi_product")]
    pub abi_product: String,
    #[serde(default, alias = "sector")]
    pub abi_sector: Option<String>,
    #[serde(default = "default_product")]
    pub product: String,
    #[serde(default = "default_domain_slug")]
    pub domain_slug: String,
    #[serde(default = "default_domain_label")]
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub out_dir: PathBuf,
    pub cache_dir: PathBuf,
    #[serde(default)]
    pub start_time_utc: Option<DateTime<Utc>>,
    #[serde(default)]
    pub end_time_utc: Option<DateTime<Utc>>,
    #[serde(default = "default_latest_count")]
    pub latest_count: usize,
    #[serde(default = "default_lookback_hours")]
    pub scan_lookback_hours: u32,
    #[serde(default)]
    pub min_step_minutes: Option<u32>,
    #[serde(default = "default_true")]
    pub use_cache: bool,
    #[serde(default = "default_downsample")]
    pub downsample: f64,
    #[serde(default)]
    pub max_width: Option<u32>,
    #[serde(default)]
    pub max_height: Option<u32>,
    #[serde(default = "default_download_workers")]
    pub download_workers: usize,
    #[serde(default = "default_render_workers")]
    pub render_workers: usize,
    #[serde(default = "default_discovery_retries")]
    pub discovery_retries: u32,
    #[serde(default = "default_retry_sleep_ms")]
    pub retry_sleep_ms: u64,
    #[serde(default)]
    pub png_compression: PngCompressionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesNativeSequenceReport {
    pub ok: bool,
    pub generated_at_utc: DateTime<Utc>,
    pub satellite: String,
    pub source_bucket: String,
    pub abi_product: String,
    pub product: String,
    pub domain: String,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub downsample: f64,
    pub max_width: Option<u32>,
    pub max_height: Option<u32>,
    pub scan_count: usize,
    pub frames: Vec<GoesNativeSequenceFrame>,
    pub report_path: PathBuf,
    pub timing: GoesNativeSequenceTiming,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GoesNativeSequenceTiming {
    pub discovery_ms: u128,
    pub download_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoesNativeSequenceFrame {
    pub product: String,
    pub kind: String,
    pub band: Option<u8>,
    pub satellite: String,
    pub scan_id: String,
    pub scan_time_utc: DateTime<Utc>,
    pub scan_end_time_utc: DateTime<Utc>,
    pub domain: String,
    pub domain_label: String,
    pub bounds: (f64, f64, f64, f64),
    pub png_path: PathBuf,
    pub width: u32,
    pub height: u32,
    pub source_crop_width_px: usize,
    pub source_crop_height_px: usize,
    pub source_keys: Vec<String>,
    pub channel_files: BTreeMap<u8, GoesSourceFile>,
    pub download_ms: u128,
    pub render_ms: u128,
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

#[derive(Debug, Clone, Copy)]
struct AxisBracket {
    lo: usize,
    hi: usize,
    t: f32,
}

struct FieldSampler {
    nx: usize,
    values: Vec<f32>,
    x_map: Vec<Option<AxisBracket>>,
    y_map: Vec<Option<AxisBracket>>,
}

pub fn run_goes_native_sequence(
    request: &GoesNativeSequenceRequest,
) -> Result<GoesNativeSequenceReport, Box<dyn Error>> {
    let total_start = Instant::now();
    validate_bounds(request.bounds)?;
    validate_scale(request)?;
    let abi_product = resolve_abi_product(&request.abi_product, request.abi_sector.as_deref())?;
    let product = GoesSatelliteProduct::parse(&request.product)?;
    let required_channels = product.required_channels();
    let satellite = GoesSatellite::parse(&request.satellite);
    let satellite_slug = satellite.as_str().to_ascii_lowercase();
    let bucket = bucket_for_satellite(&request.satellite)?;
    fs::create_dir_all(&request.cache_dir)?;
    fs::create_dir_all(&request.out_dir)?;

    let agent = build_agent();
    let discovery_start = Instant::now();
    let scans = discover_scans(&agent, &bucket, &abi_product, &required_channels, request)?;
    let discovery_ms = discovery_start.elapsed().as_millis();

    let run_dir = request
        .out_dir
        .join("satellite")
        .join(&satellite_slug)
        .join("native_sequence")
        .join(sanitize_component(&request.domain_slug))
        .join(sequence_run_slug(&scans));
    fs::create_dir_all(&run_dir)?;

    let mut frames = Vec::with_capacity(scans.len());
    let mut total_download_ms = 0;
    let mut total_render_ms = 0;
    for scan in &scans {
        let download_start = Instant::now();
        let downloads = download_scan_channels(
            &bucket,
            &request.cache_dir,
            scan,
            &required_channels,
            request.use_cache,
            request.download_workers,
        )?;
        let download_ms = download_start.elapsed().as_millis();
        total_download_ms += download_ms;

        let frame_dir = if scans.len() > 1 {
            let dir = run_dir.join(scan.start_time_utc.format("%Y%m%dT%H%M%SZ").to_string());
            fs::create_dir_all(&dir)?;
            dir
        } else {
            run_dir.clone()
        };
        let render_start = Instant::now();
        let mut frame = render_native_crop_frame(
            request, &product, &satellite, &bucket, scan, &downloads, &frame_dir,
        )?;
        let render_ms = render_start.elapsed().as_millis();
        total_render_ms += render_ms;
        frame.download_ms = download_ms;
        frame.render_ms = render_ms;
        frames.push(frame);
    }

    let report_path = run_dir.join("rustwx_goes_native_sequence_report.json");
    let report = GoesNativeSequenceReport {
        ok: true,
        generated_at_utc: Utc::now(),
        satellite: satellite.as_str().to_string(),
        source_bucket: bucket,
        abi_product,
        product: product.slug(),
        domain: request.domain_slug.clone(),
        domain_label: request.domain_label.clone(),
        bounds: request.bounds,
        downsample: request.downsample,
        max_width: request.max_width,
        max_height: request.max_height,
        scan_count: scans.len(),
        frames,
        report_path: report_path.clone(),
        timing: GoesNativeSequenceTiming {
            discovery_ms,
            download_ms: total_download_ms,
            render_ms: total_render_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    };
    atomic_write_json(&report_path, &report)?;
    Ok(report)
}

fn render_native_crop_frame(
    request: &GoesNativeSequenceRequest,
    product: &GoesSatelliteProduct,
    satellite: &GoesSatellite,
    bucket: &str,
    scan: &AbiScan,
    downloads: &BTreeMap<u8, DownloadedObject>,
    frame_dir: &Path,
) -> Result<GoesNativeSequenceFrame, Box<dyn Error>> {
    let product_channels = product.required_channels();
    let base_channel = product
        .rgb_style()
        .map(GoesAbiRgbCompositeStyle::base_channel)
        .or(match product {
            GoesSatelliteProduct::AbiBand(channel) => Some(*channel),
            _ => None,
        })
        .ok_or_else(|| boxed_error(format!("cannot choose base channel for {}", product.slug())))?;
    let base_path = downloads
        .get(&base_channel)
        .ok_or_else(|| boxed_error(format!("missing downloaded C{base_channel:02}")))?
        .path
        .clone();
    let base_scene = read_goes_abi_scene(&base_path)?;
    let crop = crop_indices_for_bounds(&base_scene, request.bounds, DEFAULT_DOMAIN_SAMPLE_POINTS)?;
    let (width, height) = output_dimensions(
        crop.source_width(),
        crop.source_height(),
        request.downsample,
        request.max_width,
        request.max_height,
    )?;
    let out_x = output_scan_axis(
        &base_scene.fixed_grid.x_scan_rad,
        crop.x0,
        crop.x1,
        width as usize,
    );
    let out_y = output_scan_axis(
        &base_scene.fixed_grid.y_scan_rad,
        crop.y0,
        crop.y1,
        height as usize,
    );

    let mut samplers = HashMap::<u8, FieldSampler>::new();
    for channel in &product_channels {
        let path = downloads
            .get(channel)
            .ok_or_else(|| boxed_error(format!("missing downloaded C{channel:02}")))?
            .path
            .clone();
        let channel_scene = read_goes_abi_scene(&path)?;
        let channel_crop = crop_indices_for_output_axes(&channel_scene, &out_x, &out_y, 2)
            .ok_or_else(|| {
                boxed_error(format!(
                    "output crop for {} falls outside C{channel:02} source grid",
                    product.slug()
                ))
            })?;
        let field = read_goes_abi_field_window(
            &path,
            "CMI",
            channel_crop.x0,
            channel_crop.source_width(),
            channel_crop.y0,
            channel_crop.source_height(),
        )?;
        samplers.insert(*channel, build_sampler(field, &out_x, &out_y));
    }

    let slug = product.slug();
    let png_path = frame_dir.join(format!(
        "{}_{}.png",
        slug,
        scan.start_time_utc.format("%Y%m%dT%H%M%SZ")
    ));
    let mut rgba = vec![0u8; width as usize * height as usize * 4];
    let render_threads = request.render_workers;
    let style = product.rgb_style();
    if render_threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(render_threads)
            .build()?
            .install(|| fill_native_pixels(product, style, &samplers, width, &mut rgba));
    } else {
        fill_native_pixels(product, style, &samplers, width, &mut rgba);
    }
    let image = RgbaImage::from_vec(width, height, rgba).ok_or_else(|| {
        boxed_error(format!(
            "failed to create GOES native crop image {width}x{height}"
        ))
    })?;
    save_rgba_png_profile_with_options(
        &image,
        &png_path,
        &PngWriteOptions {
            compression: request.png_compression,
        },
    )?;

    let source_keys = product_channels
        .iter()
        .filter_map(|channel| downloads.get(channel))
        .map(|download| download.object.key.clone())
        .collect::<Vec<_>>();
    let channel_files = product_channels
        .iter()
        .filter_map(|channel| downloads.get(channel).map(|download| (*channel, download)))
        .map(|(channel, download)| {
            (
                channel,
                GoesSourceFile {
                    key: download.object.key.clone(),
                    url: object_url(bucket, &download.object.key),
                    size_bytes: download.object.size_bytes,
                    last_modified: download.object.last_modified.clone(),
                    local_path: download.path.clone(),
                    cache_hit: download.cache_hit,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(GoesNativeSequenceFrame {
        product: slug,
        kind: if product.rgb_style().is_some() {
            "abi_native_fixed_grid_rgb_crop".to_string()
        } else {
            "abi_native_fixed_grid_band_crop".to_string()
        },
        band: match product {
            GoesSatelliteProduct::AbiBand(channel) => Some(*channel),
            _ => None,
        },
        satellite: satellite.as_str().to_string(),
        scan_id: scan.scan_id.clone(),
        scan_time_utc: scan.start_time_utc,
        scan_end_time_utc: scan.end_time_utc,
        domain: request.domain_slug.clone(),
        domain_label: request.domain_label.clone(),
        bounds: request.bounds,
        png_path,
        width,
        height,
        source_crop_width_px: crop.source_width(),
        source_crop_height_px: crop.source_height(),
        source_keys,
        channel_files,
        download_ms: 0,
        render_ms: 0,
    })
}

fn fill_native_pixels(
    product: &GoesSatelliteProduct,
    style: Option<GoesAbiRgbCompositeStyle>,
    samplers: &HashMap<u8, FieldSampler>,
    width: u32,
    rgba: &mut [u8],
) {
    let width = width as usize;
    rgba.par_chunks_mut(4).enumerate().for_each(|(idx, pixel)| {
        let x = idx % width;
        let y = idx / width;
        let color = if let Some(style) = style {
            compose_goes_abi_rgb_pixel(style, |channel| {
                Ok(samplers
                    .get(&channel)
                    .map(|sampler| sampler.sample(x, y))
                    .unwrap_or(f32::NAN))
            })
            .unwrap_or(Color::TRANSPARENT)
        } else if let GoesSatelliteProduct::AbiBand(channel) = product {
            let value = samplers
                .get(channel)
                .map(|sampler| sampler.sample(x, y))
                .unwrap_or(f32::NAN);
            single_band_color(*channel, value)
        } else {
            Color::TRANSPARENT
        };
        pixel.copy_from_slice(&[color.r, color.g, color.b, color.a]);
    });
}

fn build_sampler(field: GoesAbiField, out_x: &[f64], out_y: &[f64]) -> FieldSampler {
    let x_map = out_x
        .iter()
        .map(|&value| bracket_axis(&field.scene.fixed_grid.x_scan_rad, value))
        .collect();
    let y_map = out_y
        .iter()
        .map(|&value| bracket_axis(&field.scene.fixed_grid.y_scan_rad, value))
        .collect();
    FieldSampler {
        nx: field.scene.fixed_grid.nx,
        values: field.values,
        x_map,
        y_map,
    }
}

impl FieldSampler {
    fn sample(&self, x: usize, y: usize) -> f32 {
        let Some(xb) = self.x_map.get(x).copied().flatten() else {
            return f32::NAN;
        };
        let Some(yb) = self.y_map.get(y).copied().flatten() else {
            return f32::NAN;
        };
        let idx = |yy: usize, xx: usize| yy.saturating_mul(self.nx).saturating_add(xx);
        bilinear_f32(
            self.values
                .get(idx(yb.lo, xb.lo))
                .copied()
                .unwrap_or(f32::NAN),
            self.values
                .get(idx(yb.lo, xb.hi))
                .copied()
                .unwrap_or(f32::NAN),
            self.values
                .get(idx(yb.hi, xb.lo))
                .copied()
                .unwrap_or(f32::NAN),
            self.values
                .get(idx(yb.hi, xb.hi))
                .copied()
                .unwrap_or(f32::NAN),
            xb.t,
            yb.t,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct CropIndices {
    x0: usize,
    x1: usize,
    y0: usize,
    y1: usize,
}

impl CropIndices {
    fn source_width(self) -> usize {
        self.x1.saturating_sub(self.x0).saturating_add(1)
    }

    fn source_height(self) -> usize {
        self.y1.saturating_sub(self.y0).saturating_add(1)
    }
}

fn crop_indices_for_bounds(
    scene: &GoesAbiScene,
    bounds: (f64, f64, f64, f64),
    samples_per_edge: usize,
) -> Result<CropIndices, Box<dyn Error>> {
    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    let n = samples_per_edge.max(4);
    let (west, east, south, north) = bounds;
    for idx in 0..=n {
        let t = idx as f64 / n as f64;
        let lon = interpolate_lon(west, east, t);
        for lat in [south, north] {
            accumulate_scan_bounds(
                scene, lat, lon, &mut x_min, &mut x_max, &mut y_min, &mut y_max,
            );
        }
        let lat = south + (north - south) * t;
        for lon in [west, east] {
            accumulate_scan_bounds(
                scene, lat, lon, &mut x_min, &mut x_max, &mut y_min, &mut y_max,
            );
        }
    }
    if !(x_min.is_finite() && x_max.is_finite() && y_min.is_finite() && y_max.is_finite()) {
        return Err(boxed_error(
            "requested bounds are not visible in the GOES fixed grid scene",
        ));
    }
    let (x0, x1) = index_range_for_axis(&scene.fixed_grid.x_scan_rad, x_min, x_max, 2)
        .ok_or_else(|| boxed_error("requested longitude bounds miss GOES x scan axis"))?;
    let (y0, y1) = index_range_for_axis(&scene.fixed_grid.y_scan_rad, y_min, y_max, 2)
        .ok_or_else(|| boxed_error("requested latitude bounds miss GOES y scan axis"))?;
    Ok(CropIndices { x0, x1, y0, y1 })
}

fn accumulate_scan_bounds(
    scene: &GoesAbiScene,
    lat: f64,
    lon: f64,
    x_min: &mut f64,
    x_max: &mut f64,
    y_min: &mut f64,
    y_max: &mut f64,
) {
    let projection = &scene.projection;
    if let Some((x, y)) = lat_lon_to_scan_angles_fast(
        projection.perspective_point_height_m,
        projection.semi_major_axis_m,
        projection.semi_minor_axis_m,
        projection.longitude_of_projection_origin_deg,
        projection.sweep_angle_axis,
        lat,
        lon,
    ) {
        *x_min = x_min.min(x);
        *x_max = x_max.max(x);
        *y_min = y_min.min(y);
        *y_max = y_max.max(y);
    }
}

fn index_range_for_axis(
    axis: &[f64],
    min_value: f64,
    max_value: f64,
    pad: usize,
) -> Option<(usize, usize)> {
    if axis.is_empty() {
        return None;
    }
    let lo = min_value.min(max_value);
    let hi = min_value.max(max_value);
    let mut first = None;
    let mut last = None;
    for (idx, &value) in axis.iter().enumerate() {
        if value >= lo && value <= hi {
            first.get_or_insert(idx);
            last = Some(idx);
        }
    }
    let first = first?;
    let last = last?;
    Some((
        first.saturating_sub(pad),
        (last + pad).min(axis.len().saturating_sub(1)),
    ))
}

fn crop_indices_for_output_axes(
    scene: &GoesAbiScene,
    out_x: &[f64],
    out_y: &[f64],
    pad: usize,
) -> Option<CropIndices> {
    let (x_min, x_max) = min_max_finite(out_x)?;
    let (y_min, y_max) = min_max_finite(out_y)?;
    let (x0, x1) = index_range_for_axis(&scene.fixed_grid.x_scan_rad, x_min, x_max, pad)?;
    let (y0, y1) = index_range_for_axis(&scene.fixed_grid.y_scan_rad, y_min, y_max, pad)?;
    Some(CropIndices { x0, x1, y0, y1 })
}

fn min_max_finite(values: &[f64]) -> Option<(f64, f64)> {
    let mut min_value = f64::INFINITY;
    let mut max_value = f64::NEG_INFINITY;
    let mut seen = false;
    for &value in values {
        if value.is_finite() {
            min_value = min_value.min(value);
            max_value = max_value.max(value);
            seen = true;
        }
    }
    seen.then_some((min_value, max_value))
}

fn output_scan_axis(axis: &[f64], start: usize, end: usize, out_len: usize) -> Vec<f64> {
    if out_len == 0 {
        return Vec::new();
    }
    let source_len = end.saturating_sub(start).saturating_add(1);
    (0..out_len)
        .map(|idx| {
            let pos = start as f64
                + ((idx as f64 + 0.5) * source_len as f64 / out_len as f64 - 0.5)
                    .clamp(0.0, source_len.saturating_sub(1) as f64);
            let lo = pos.floor() as usize;
            let hi = pos.ceil() as usize;
            let t = pos - lo as f64;
            let lo = lo.min(axis.len().saturating_sub(1));
            let hi = hi.min(axis.len().saturating_sub(1));
            axis[lo] * (1.0 - t) + axis[hi] * t
        })
        .collect()
}

fn output_dimensions(
    crop_width: usize,
    crop_height: usize,
    downsample: f64,
    max_width: Option<u32>,
    max_height: Option<u32>,
) -> Result<(u32, u32), Box<dyn Error>> {
    let downsample = downsample.max(1.0);
    let mut width = (crop_width as f64 / downsample).ceil().max(1.0);
    let mut height = (crop_height as f64 / downsample).ceil().max(1.0);
    let cap_scale_w = max_width
        .filter(|value| *value > 0)
        .map(|value| width / f64::from(value))
        .unwrap_or(1.0);
    let cap_scale_h = max_height
        .filter(|value| *value > 0)
        .map(|value| height / f64::from(value))
        .unwrap_or(1.0);
    let cap_scale = cap_scale_w.max(cap_scale_h).max(1.0);
    width = (width / cap_scale).round().max(1.0);
    height = (height / cap_scale).round().max(1.0);
    if width > f64::from(u32::MAX) || height > f64::from(u32::MAX) {
        return Err(boxed_error("GOES native crop output is too large"));
    }
    Ok((width as u32, height as u32))
}

fn bracket_axis(axis: &[f64], value: f64) -> Option<AxisBracket> {
    if axis.is_empty() || !value.is_finite() {
        return None;
    }
    if axis.len() == 1 {
        return ((value - axis[0]).abs() <= 1.0e-10).then_some(AxisBracket {
            lo: 0,
            hi: 0,
            t: 0.0,
        });
    }
    let ascending = axis[axis.len() - 1] >= axis[0];
    let first = axis[0];
    let last = axis[axis.len() - 1];
    if ascending {
        if value < first || value > last {
            return None;
        }
    } else if value > first || value < last {
        return None;
    }

    let mut lo = 0usize;
    let mut hi = axis.len() - 1;
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        let mid_value = axis[mid];
        if (ascending && mid_value <= value) || (!ascending && mid_value >= value) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let a = axis[lo];
    let b = axis[hi];
    let t = if (b - a).abs() <= 1.0e-15 {
        0.0
    } else {
        ((value - a) / (b - a)).clamp(0.0, 1.0)
    };
    Some(AxisBracket {
        lo,
        hi,
        t: t as f32,
    })
}

fn bilinear_f32(v00: f32, v10: f32, v01: f32, v11: f32, fx: f32, fy: f32) -> f32 {
    if v00.is_finite() && v10.is_finite() && v01.is_finite() && v11.is_finite() {
        let south = v00 * (1.0 - fx) + v10 * fx;
        let north = v01 * (1.0 - fx) + v11 * fx;
        south * (1.0 - fy) + north * fy
    } else {
        [v00, v10, v01, v11]
            .into_iter()
            .find(|value| value.is_finite())
            .unwrap_or(f32::NAN)
    }
}

fn single_band_color(channel: u8, value: f32) -> Color {
    if !value.is_finite() {
        return Color::rgba(0, 0, 0, 0);
    }
    if (1..=6).contains(&channel) {
        let shade = (f64::from(value).clamp(0.0, 1.0) * 255.0).round() as u8;
        return Color::rgba(shade, shade, shade, 255);
    }
    let anchors = [
        (188.0, [255, 255, 255]),
        (202.0, [218, 239, 254]),
        (216.0, [143, 204, 235]),
        (230.0, [83, 146, 202]),
        (244.0, [67, 91, 154]),
        (258.0, [87, 76, 122]),
        (272.0, [99, 95, 102]),
        (288.0, [72, 72, 72]),
        (306.0, [36, 36, 36]),
        (328.0, [4, 4, 4]),
    ];
    let [r, g, b, a] = interpolate_rgb_anchors(f64::from(value), &anchors);
    Color::rgba(r, g, b, a)
}

fn interpolate_rgb_anchors(value: f64, anchors: &[(f64, [u8; 3])]) -> [u8; 4] {
    if value <= anchors[0].0 {
        let [r, g, b] = anchors[0].1;
        return [r, g, b, 255];
    }
    for window in anchors.windows(2) {
        let (lo, lo_color) = window[0];
        let (hi, hi_color) = window[1];
        if value <= hi {
            let t = ((value - lo) / (hi - lo)).clamp(0.0, 1.0);
            let channel =
                |a: u8, b: u8| -> u8 { (a as f64 + (b as f64 - a as f64) * t).round() as u8 };
            return [
                channel(lo_color[0], hi_color[0]),
                channel(lo_color[1], hi_color[1]),
                channel(lo_color[2], hi_color[2]),
                255,
            ];
        }
    }
    let [r, g, b] = anchors[anchors.len() - 1].1;
    [r, g, b, 255]
}

fn discover_scans(
    agent: &ureq::Agent,
    bucket: &str,
    abi_product: &str,
    required_channels: &[u8],
    request: &GoesNativeSequenceRequest,
) -> Result<Vec<AbiScan>, Box<dyn Error>> {
    let mut last_error = None;
    for attempt in 0..=request.discovery_retries {
        match try_discover_scans(agent, bucket, abi_product, required_channels, request) {
            Ok(scans) => return Ok(scans),
            Err(err) => {
                last_error = Some(err.to_string());
                if attempt < request.discovery_retries && request.retry_sleep_ms > 0 {
                    thread::sleep(std::time::Duration::from_millis(request.retry_sleep_ms));
                }
            }
        }
    }
    Err(boxed_error(format!(
        "no complete GOES ABI scans found for {abi_product} channels {:?}: {}",
        required_channels,
        last_error.unwrap_or_else(|| "no matching objects".to_string())
    )))
}

fn try_discover_scans(
    agent: &ureq::Agent,
    bucket: &str,
    abi_product: &str,
    required_channels: &[u8],
    request: &GoesNativeSequenceRequest,
) -> Result<Vec<AbiScan>, Box<dyn Error>> {
    let (start, end, latest_mode) =
        if let (Some(start), Some(end)) = (request.start_time_utc, request.end_time_utc) {
            if end < start {
                return Err(boxed_error("--end must be after --start"));
            }
            (start, end, false)
        } else if request.start_time_utc.is_none() && request.end_time_utc.is_none() {
            let end = Utc::now();
            (
                end - Duration::hours(i64::from(request.scan_lookback_hours.max(1))),
                end,
                true,
            )
        } else {
            return Err(boxed_error(
                "provide both start_time_utc and end_time_utc, or neither",
            ));
        };

    let mut groups = BTreeMap::<DateTime<Utc>, BTreeMap<u8, S3Object>>::new();
    let mut hour = floor_to_hour(start);
    let end_hour = floor_to_hour(end);
    while hour <= end_hour {
        let prefix = goes_hour_prefix(abi_product, hour);
        for object in list_s3_objects(agent, bucket, &prefix)? {
            if !object.key.ends_with(".nc") {
                continue;
            }
            let parsed = match parse_goes_abi_filename(object_filename(&object.key)) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            if parsed.start_time_utc < start || parsed.start_time_utc > end {
                continue;
            }
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
        hour += Duration::hours(1);
    }

    let mut scans = groups
        .into_iter()
        .filter_map(|(start_time, channel_objects)| {
            if !required_channels
                .iter()
                .all(|channel| channel_objects.contains_key(channel))
            {
                return None;
            }
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
            Some(AbiScan {
                scan_id: format!(
                    "{}_{}_{}",
                    satellite,
                    abi_product,
                    start_time.format("%Y%m%dT%H%M%SZ")
                ),
                start_time_utc: start_time,
                end_time_utc: end_time,
                channel_objects,
            })
        })
        .collect::<Vec<_>>();
    scans.sort_by_key(|scan| scan.start_time_utc);
    if let Some(step_minutes) = request.min_step_minutes.filter(|value| *value > 0) {
        let step = Duration::minutes(i64::from(step_minutes));
        let mut last_kept = None;
        scans.retain(|scan| {
            if last_kept.is_none_or(|last| scan.start_time_utc >= last + step) {
                last_kept = Some(scan.start_time_utc);
                true
            } else {
                false
            }
        });
    }
    if latest_mode {
        let count = request.latest_count.max(1);
        if scans.len() > count {
            scans = scans.split_off(scans.len() - count);
        }
    }
    if scans.is_empty() {
        return Err(boxed_error(
            "listed ABI hours but found no complete scan/channel groups",
        ));
    }
    Ok(scans)
}

fn download_scan_channels(
    bucket: &str,
    cache_dir: &Path,
    scan: &AbiScan,
    required_channels: &[u8],
    use_cache: bool,
    workers: usize,
) -> Result<BTreeMap<u8, DownloadedObject>, Box<dyn Error>> {
    let run_download = || {
        required_channels
            .par_iter()
            .map(|channel| {
                let object = scan
                    .channel_objects
                    .get(channel)
                    .ok_or_else(|| format!("missing ABI C{channel:02} in discovered scan"))?;
                let agent = build_agent();
                download_object(&agent, bucket, cache_dir, object, use_cache)
                    .map(|download| (*channel, download))
                    .map_err(|err| err.to_string())
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let pairs = if workers > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()?
            .install(run_download)
    } else {
        run_download()
    }
    .map_err(boxed_error)?;
    Ok(pairs.into_iter().collect())
}

fn download_object(
    agent: &ureq::Agent,
    bucket: &str,
    cache_dir: &Path,
    object: &S3Object,
    use_cache: bool,
) -> Result<DownloadedObject, Box<dyn Error>> {
    let target = cache_dir.join("satellite").join(bucket).join(&object.key);
    if use_cache && target.exists() && target.metadata()?.len() == object.size_bytes {
        return Ok(DownloadedObject {
            object: object.clone(),
            path: target,
            cache_hit: true,
        });
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
    atomic_write_bytes(&target, &bytes)?;
    Ok(DownloadedObject {
        object: object.clone(),
        path: target,
        cache_hit: false,
    })
}

fn list_s3_objects(
    agent: &ureq::Agent,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<S3Object>, Box<dyn Error>> {
    let mut objects = Vec::new();
    let mut token = None::<String>;
    loop {
        let mut url = format!(
            "https://{bucket}.s3.amazonaws.com/?list-type=2&prefix={}&max-keys=1000",
            url_query_encode(prefix)
        );
        if let Some(token) = &token {
            url.push_str("&continuation-token=");
            url.push_str(&url_query_encode(token));
        }
        let mut response = agent.get(&url).call()?;
        let xml = response.body_mut().read_to_string()?;
        let page = parse_s3_list_xml(&xml);
        objects.extend(page.objects);
        token = page.next_continuation_token;
        if token.is_none() {
            break;
        }
    }
    Ok(objects)
}

struct S3ListPage {
    objects: Vec<S3Object>,
    next_continuation_token: Option<String>,
}

fn parse_s3_list_xml(xml: &str) -> S3ListPage {
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
    S3ListPage {
        objects,
        next_continuation_token: extract_xml_tag(xml, "NextContinuationToken"),
    }
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml_unescape(&xml[start..end]))
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn validate_bounds(bounds: (f64, f64, f64, f64)) -> Result<(), Box<dyn Error>> {
    let (west, east, south, north) = bounds;
    if !(west.is_finite() && east.is_finite() && south.is_finite() && north.is_finite()) {
        return Err(boxed_error("GOES native sequence bounds must be finite"));
    }
    if !(-90.0..=90.0).contains(&south) || !(-90.0..=90.0).contains(&north) || south >= north {
        return Err(boxed_error(
            "GOES native sequence latitude bounds are invalid",
        ));
    }
    Ok(())
}

fn validate_scale(request: &GoesNativeSequenceRequest) -> Result<(), Box<dyn Error>> {
    if !request.downsample.is_finite() || request.downsample < 1.0 {
        return Err(boxed_error("--downsample must be finite and >= 1"));
    }
    Ok(())
}

fn floor_to_hour(time: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(time.year(), time.month(), time.day(), time.hour(), 0, 0)
        .single()
        .unwrap_or(time)
}

fn interpolate_lon(west: f64, east: f64, t: f64) -> f64 {
    let mut east = east;
    if east < west {
        east += 360.0;
    }
    normalize_longitude_deg(west + (east - west) * t)
}

fn normalize_longitude_deg(lon: f64) -> f64 {
    let mut value = (lon + 180.0).rem_euclid(360.0) - 180.0;
    if value == -180.0 {
        value = 180.0;
    }
    value
}

fn sequence_run_slug(scans: &[AbiScan]) -> String {
    match (scans.first(), scans.last()) {
        (Some(first), Some(last)) if scans.len() > 1 => format!(
            "{}_to_{}",
            first.start_time_utc.format("%Y%m%dT%H%M%SZ"),
            last.start_time_utc.format("%Y%m%dT%H%M%SZ")
        ),
        (Some(scan), _) => scan.start_time_utc.format("%Y%m%dT%H%M%SZ").to_string(),
        _ => Utc::now().format("%Y%m%dT%H%M%SZ").to_string(),
    }
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

fn url_query_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "domain".to_string()
    } else {
        trimmed.to_string()
    }
}

fn build_agent() -> ureq::Agent {
    static CRYPTO_PROVIDER: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    CRYPTO_PROVIDER.get_or_init(|| {
        rustls::crypto::CryptoProvider::install_default(rustls_rustcrypto::provider()).ok();
    });
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

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

fn default_satellite() -> String {
    "goes18".to_string()
}

fn default_abi_product() -> String {
    DEFAULT_ABI_PRODUCT.to_string()
}

fn default_product() -> String {
    "geocolor".to_string()
}

fn default_domain_slug() -> String {
    "native_crop".to_string()
}

fn default_domain_label() -> String {
    "Native Crop".to_string()
}

fn default_latest_count() -> usize {
    1
}

fn default_lookback_hours() -> u32 {
    DEFAULT_LOOKBACK_HOURS
}

fn default_discovery_retries() -> u32 {
    DEFAULT_DISCOVERY_RETRIES
}

fn default_retry_sleep_ms() -> u64 {
    DEFAULT_RETRY_SLEEP_MS
}

fn default_download_workers() -> usize {
    DEFAULT_DOWNLOAD_WORKERS
}

fn default_render_workers() -> usize {
    DEFAULT_RENDER_WORKERS
}

fn default_true() -> bool {
    true
}

fn default_downsample() -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_dimensions_respects_downsample_and_caps() {
        assert_eq!(
            output_dimensions(1000, 500, 2.0, None, None).unwrap(),
            (500, 250)
        );
        assert_eq!(
            output_dimensions(1000, 500, 1.0, Some(250), None).unwrap(),
            (250, 125)
        );
    }

    #[test]
    fn s3_xml_parser_reads_continuation_token() {
        let xml = "<ListBucketResult><Contents><Key>a.nc</Key><Size>42</Size><LastModified>x</LastModified></Contents><NextContinuationToken>abc&amp;123</NextContinuationToken></ListBucketResult>";
        let page = parse_s3_list_xml(xml);
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "a.nc");
        assert_eq!(page.objects[0].size_bytes, 42);
        assert_eq!(page.next_continuation_token.as_deref(), Some("abc&123"));
    }

    #[test]
    fn longitude_interpolation_handles_dateline_wrap() {
        assert!(interpolate_lon(170.0, -170.0, 0.5).abs() >= 179.9);
    }

    #[test]
    fn output_axis_crop_uses_source_window_with_padding() {
        let scene = GoesAbiScene {
            path: PathBuf::from("synthetic.nc"),
            product: "ABI-L2-CMIPC".to_string(),
            sector: super::super::abi::AbiSector::Conus,
            channel: Some(2),
            satellite: GoesSatellite::G18,
            start_time_utc: Utc.timestamp_opt(2026, 0).unwrap(),
            end_time_utc: Utc.timestamp_opt(2026, 60).unwrap(),
            projection: super::super::abi::GoesImagerProjection {
                perspective_point_height_m: 35_786_023.0,
                semi_major_axis_m: 6_378_137.0,
                semi_minor_axis_m: 6_356_752.31414,
                longitude_of_projection_origin_deg: -137.0,
                sweep_angle_axis: super::super::geostationary::SweepAngleAxis::X,
            },
            fixed_grid: super::super::abi::AbiFixedGrid {
                nx: 10,
                ny: 10,
                x_scan_rad: (0..10).map(|idx| idx as f64).collect(),
                y_scan_rad: (0..10).map(|idx| idx as f64).collect(),
            },
        };

        let crop = crop_indices_for_output_axes(&scene, &[3.2, 3.8, 4.8], &[5.1, 5.9, 6.1], 1)
            .expect("crop should intersect source axes");

        assert_eq!((crop.x0, crop.x1, crop.y0, crop.y1), (3, 5, 5, 7));
        assert_eq!(crop.source_width(), 3);
        assert_eq!(crop.source_height(), 3);
    }
}
