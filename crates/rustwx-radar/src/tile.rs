use std::f64::consts::PI;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use image::codecs::png::{CompressionType, FilterType as PngFilterType, PngEncoder};
use image::{ExtendedColorType, ImageEncoder};
#[cfg(not(target_arch = "wasm32"))]
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::dealias::VelocityQualityMaskReport;
use crate::dealias::{
    DealiasAcceptancePolicy, DealiasMethod, DealiasReport, dealias_velocity_sweep,
    dealias_velocity_sweep_with_policy, effective_nyquist, mask_velocity_sweep_quality,
};
use crate::nexrad::derived::DerivedProducts;
use crate::nexrad::level2::{MomentData, RadialData};
use crate::nexrad::srv::SRVComputer;
use crate::nexrad::{Level2File, Level2Sweep, RadarProduct, RadarSite};
use crate::png::{
    RadarSweepSelection, select_sweep_with_hca_inputs, select_sweep_with_product,
    sweep_contains_product,
};
use crate::render::{ColorTable, ColorTablePreset};
use crate::sidecar::{
    RadarPolarSidecarOptions, RadarPolarSidecarRecord, radar_lat_lon_to_polar, write_polar_sidecar,
};

const WEB_MERCATOR_LIMIT: f64 = 85.051_128_78;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RadarTilePngCompression {
    #[default]
    Default,
    Fast,
    Fastest,
}

#[derive(Debug, Clone)]
pub struct RadarTileOptions {
    pub name: Option<String>,
    pub source_key_or_url: Option<String>,
    pub base_url: Option<String>,
    pub bounds: Option<[f64; 4]>,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub tile_size: u32,
    pub opacity: f64,
    pub min_value: Option<f32>,
    pub color_table_preset: ColorTablePreset,
    pub sample_factor: u8,
    pub png_compression: RadarTilePngCompression,
    pub skip_empty_tiles: bool,
    pub clip_to_bounds: bool,
    pub sweep: RadarSweepSelection,
    pub dealias_velocity: bool,
    pub dealias_method: DealiasMethod,
    pub force_rejected_dealias: bool,
    pub velocity_quality_filter: bool,
    pub reflectivity_despeckle: bool,
    pub reflectivity_despeckle_min_neighbors: u8,
    pub emit_numeric_sidecar: bool,
}

impl Default for RadarTileOptions {
    fn default() -> Self {
        Self {
            name: None,
            source_key_or_url: None,
            base_url: None,
            bounds: None,
            min_zoom: 2,
            max_zoom: 9,
            tile_size: 256,
            opacity: 1.0,
            min_value: None,
            color_table_preset: ColorTablePreset::Default,
            sample_factor: 1,
            png_compression: RadarTilePngCompression::Fast,
            skip_empty_tiles: true,
            clip_to_bounds: false,
            sweep: RadarSweepSelection::Lowest,
            dealias_velocity: false,
            dealias_method: DealiasMethod::SweepContinuity,
            force_rejected_dealias: false,
            velocity_quality_filter: false,
            reflectivity_despeckle: false,
            reflectivity_despeckle_min_neighbors: 2,
            emit_numeric_sidecar: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarTileManifest {
    pub ok: bool,
    pub name: String,
    pub out_dir: PathBuf,
    pub site: RadarTileSiteRecord,
    pub product: String,
    pub product_name: String,
    pub product_provenance: RadarProductProvenance,
    pub product_qc: Option<RadarProductQcSummary>,
    pub source_key_or_url: Option<String>,
    pub scan_time_utc: String,
    pub sweep_index: usize,
    pub elevation_deg: f32,
    pub bounds: [f64; 4],
    pub minzoom: u8,
    pub maxzoom: u8,
    pub tile_size: u32,
    pub opacity: f64,
    pub color_table: String,
    pub sample_factor: u8,
    pub clip_to_bounds: bool,
    pub sampling_bounds: [f64; 4],
    pub native_gate_size_m: Option<u16>,
    pub native_azimuth_spacing_deg: Option<f64>,
    pub maxzoom_site_meters_per_pixel: f64,
    pub dealias_velocity: bool,
    pub dealias_method: String,
    pub force_rejected_dealias: bool,
    pub dealias_applied: bool,
    pub dealias_qc: Option<DealiasReport>,
    pub velocity_quality_filter: bool,
    pub velocity_quality_qc: Option<VelocityQualityMaskReport>,
    pub reflectivity_despeckle: bool,
    pub reflectivity_qc: Option<RadarReflectivityQcSummary>,
    pub velocity_qc: Option<RadarVelocityQcSummary>,
    pub candidate_tile_count: usize,
    pub rendered_pixel_count: u64,
    pub tiles_per_second: f64,
    pub resolve_ms: u128,
    pub prepare_ms: u128,
    pub render_ms: u128,
    pub tile_count: usize,
    pub skipped_empty_tiles: usize,
    pub total_ms: u128,
    pub tilejson_path: PathBuf,
    pub numeric_sidecar: Option<RadarPolarSidecarRecord>,
    pub tiles: Vec<RadarTileRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarTileSiteRecord {
    pub id: String,
    pub name: String,
    pub state: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarTileRecord {
    pub z: u8,
    pub x: u32,
    pub y: u32,
    pub path: PathBuf,
    pub nontransparent_pixels: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarProductQcSummary {
    pub product: String,
    pub finite_gate_count: usize,
    pub min_value: f32,
    pub max_value: f32,
    pub mean_value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarProductProvenance {
    pub source: String,
    pub derived: bool,
    pub inputs: Vec<String>,
    pub method: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarVelocityQcSummary {
    pub product: String,
    pub nyquist_ms: f32,
    pub finite_gate_count: usize,
    pub radial_pair_count: usize,
    pub azimuth_pair_count: usize,
    pub fold_like_jump_count: usize,
    pub severe_jump_count: usize,
    pub fold_like_jump_fraction: f64,
    pub max_abs_jump_ms: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RadarReflectivityQcSummary {
    pub product: String,
    pub despeckle_applied: bool,
    pub min_neighbor_count: u8,
    pub finite_gate_count: usize,
    pub removed_gate_count: usize,
    pub removed_gate_fraction: f64,
}

#[derive(Debug, Clone, Serialize)]
struct RadarTileJson {
    tilejson: String,
    name: String,
    version: String,
    scheme: String,
    tiles: Vec<String>,
    minzoom: u8,
    maxzoom: u8,
    bounds: [f64; 4],
}

pub fn render_product_web_tiles(
    file: &Level2File,
    site: &RadarSite,
    product: RadarProduct,
    out_dir: impl AsRef<Path>,
    options: RadarTileOptions,
) -> anyhow::Result<RadarTileManifest> {
    validate_options(&options)?;
    let started = Instant::now();
    let out_dir = out_dir.as_ref();
    fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let resolve_started = Instant::now();
    let resolved = resolve_tile_sweep(
        file,
        product,
        options.sweep,
        options.dealias_velocity,
        options.dealias_method,
        options.force_rejected_dealias,
        options.velocity_quality_filter,
        options.reflectivity_despeckle,
        options.reflectivity_despeckle_min_neighbors,
    )?;
    let resolve_ms = resolve_started.elapsed().as_millis();
    let product_provenance = radar_product_provenance(file, product, options.sweep);
    let scan_time = scan_time_utc(file);

    let prepare_started = Instant::now();
    let prepared = PreparedSweep::new(
        resolved.sweep(),
        product,
        options.min_value,
        options.color_table_preset,
        options.opacity,
    )?;
    let velocity_qc = radar_velocity_qc_summary(resolved.sweep(), product);
    let product_qc = radar_product_qc_summary(resolved.sweep(), product);
    let processing_state =
        radar_processing_state(product, &product_provenance, &options, &resolved);
    let coverage_bounds = radar_coverage_bounds(site, prepared.max_ground_range_m);
    let tile_bounds = match options.bounds {
        Some(bounds) => intersect_bounds(bounds, coverage_bounds).ok_or_else(|| {
            anyhow::anyhow!(
                "requested bounds do not intersect {} radar coverage",
                site.id
            )
        })?,
        None => coverage_bounds,
    };
    let sampling_bounds =
        radar_sampling_bounds(tile_bounds, coverage_bounds, options.clip_to_bounds);

    let jobs = tile_jobs(tile_bounds, options.min_zoom, options.max_zoom)?;
    let candidate_tile_count = jobs.len();
    let rendered_pixel_count =
        candidate_tile_count as u64 * u64::from(options.tile_size) * u64::from(options.tile_size);
    let prepare_ms = prepare_started.elapsed().as_millis();
    let render_started = Instant::now();
    #[cfg(not(target_arch = "wasm32"))]
    let rendered = jobs
        .par_iter()
        .map(|&(z, x, y)| {
            render_tile(
                &prepared,
                site,
                sampling_bounds,
                z,
                x,
                y,
                options.tile_size,
                options.sample_factor,
                out_dir,
                options.png_compression,
                options.skip_empty_tiles,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    #[cfg(target_arch = "wasm32")]
    let rendered = jobs
        .iter()
        .map(|&(z, x, y)| {
            render_tile(
                &prepared,
                site,
                sampling_bounds,
                z,
                x,
                y,
                options.tile_size,
                options.sample_factor,
                out_dir,
                options.png_compression,
                options.skip_empty_tiles,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let render_ms = render_started.elapsed().as_millis();

    let skipped_empty_tiles = rendered.iter().filter(|record| record.is_none()).count();
    let tiles = rendered.into_iter().flatten().collect::<Vec<_>>();
    let name = options.name.unwrap_or_else(|| {
        format!(
            "{}_{}",
            site.id.to_ascii_lowercase(),
            product.short_name().to_ascii_lowercase()
        )
    });
    let tilejson_path = out_dir.join("tilejson.json");
    let tile_url = options
        .base_url
        .as_deref()
        .map(|base| {
            format!(
                "{}/{}/{}/{}.png",
                base.trim_end_matches('/'),
                "{z}",
                "{x}",
                "{y}"
            )
        })
        .unwrap_or_else(|| "{z}/{x}/{y}.png".to_string());
    let tilejson = RadarTileJson {
        tilejson: "3.0.0".to_string(),
        name: name.clone(),
        version: "1.0.0".to_string(),
        scheme: "xyz".to_string(),
        tiles: vec![tile_url],
        minzoom: options.min_zoom,
        maxzoom: options.max_zoom,
        bounds: tile_bounds,
    };
    atomic_write_json(&tilejson_path, &tilejson)?;
    let numeric_sidecar = if options.emit_numeric_sidecar {
        Some(write_polar_sidecar(
            resolved.sweep(),
            site,
            product,
            out_dir,
            RadarPolarSidecarOptions {
                name: name.clone(),
                source_key_or_url: options.source_key_or_url.clone(),
                scan_time_utc: scan_time.clone(),
                site_lat: file.site_metadata.map(|metadata| metadata.lat),
                site_lon: file.site_metadata.map(|metadata| metadata.lon),
                site_elevation_m: file.site_metadata.map(|metadata| metadata.elevation_m),
                site_feedhorn_height_m: file
                    .site_metadata
                    .and_then(|metadata| metadata.feedhorn_height_m),
                sweep_index: resolved.sweep_index(),
                processing_state: processing_state.clone(),
                product_provenance: serde_json::to_value(&product_provenance)?,
                product_qc: product_qc.as_ref().map(serde_json::to_value).transpose()?,
                velocity_qc: velocity_qc.as_ref().map(serde_json::to_value).transpose()?,
                dealias_qc: resolved
                    .dealias_report()
                    .map(serde_json::to_value)
                    .transpose()?,
                velocity_quality_qc: resolved
                    .velocity_quality_qc()
                    .map(serde_json::to_value)
                    .transpose()?,
                reflectivity_qc: resolved
                    .reflectivity_qc()
                    .map(serde_json::to_value)
                    .transpose()?,
            },
        )?)
    } else {
        None
    };

    let total_ms = started.elapsed().as_millis();
    let tiles_per_second = if total_ms > 0 {
        candidate_tile_count as f64 / (total_ms as f64 / 1000.0)
    } else {
        candidate_tile_count as f64
    };

    let manifest = RadarTileManifest {
        ok: true,
        name,
        out_dir: out_dir.to_path_buf(),
        site: RadarTileSiteRecord {
            id: site.id.to_string(),
            name: site.name.to_string(),
            state: site.state.to_string(),
            lat: site.lat,
            lon: site.lon,
        },
        product: product.short_name().to_ascii_lowercase(),
        product_name: product.display_name().to_string(),
        product_provenance,
        product_qc,
        source_key_or_url: options.source_key_or_url,
        scan_time_utc: scan_time,
        sweep_index: resolved.sweep_index(),
        elevation_deg: resolved.sweep().elevation_angle,
        bounds: tile_bounds,
        minzoom: options.min_zoom,
        maxzoom: options.max_zoom,
        tile_size: options.tile_size,
        opacity: options.opacity,
        color_table: prepared.color_table.name.clone(),
        sample_factor: options.sample_factor,
        clip_to_bounds: options.clip_to_bounds,
        sampling_bounds,
        native_gate_size_m: prepared.native_gate_size_m(),
        native_azimuth_spacing_deg: prepared.native_azimuth_spacing_deg(),
        maxzoom_site_meters_per_pixel: web_mercator_meters_per_pixel(site.lat, options.max_zoom),
        dealias_velocity: options.dealias_velocity,
        dealias_method: if options.dealias_velocity {
            options.dealias_method.as_str().to_string()
        } else {
            DealiasMethod::Off.as_str().to_string()
        },
        force_rejected_dealias: options.force_rejected_dealias,
        dealias_applied: resolved.dealias_applied(),
        dealias_qc: resolved.dealias_report().cloned(),
        velocity_quality_filter: options.velocity_quality_filter,
        velocity_quality_qc: resolved.velocity_quality_qc().cloned(),
        reflectivity_despeckle: options.reflectivity_despeckle,
        reflectivity_qc: resolved.reflectivity_qc().cloned(),
        velocity_qc,
        candidate_tile_count,
        rendered_pixel_count,
        tiles_per_second,
        resolve_ms,
        prepare_ms,
        render_ms,
        tile_count: tiles.len(),
        skipped_empty_tiles,
        total_ms,
        tilejson_path,
        numeric_sidecar,
        tiles,
    };
    atomic_write_json(out_dir.join("tiles_manifest.json"), &manifest)?;
    Ok(manifest)
}

fn validate_options(options: &RadarTileOptions) -> anyhow::Result<()> {
    if options.max_zoom < options.min_zoom {
        bail!("max_zoom must be >= min_zoom");
    }
    if options.tile_size == 0 || options.tile_size > 2048 {
        bail!("tile_size must be in 1..=2048");
    }
    if !(1..=4).contains(&options.sample_factor) {
        bail!("sample_factor must be in 1..=4");
    }
    if !(0.0..=1.0).contains(&options.opacity) {
        bail!("opacity must be in 0..=1");
    }
    if options.reflectivity_despeckle_min_neighbors > 8 {
        bail!("reflectivity_despeckle_min_neighbors must be in 0..=8");
    }
    if let Some(bounds) = options.bounds {
        validate_bounds(bounds, "bounds")?;
    }
    Ok(())
}

fn validate_bounds(bounds: [f64; 4], label: &str) -> anyhow::Result<()> {
    let [west, south, east, north] = bounds;
    if !(west.is_finite() && south.is_finite() && east.is_finite() && north.is_finite()) {
        bail!("{label} must contain finite west,south,east,north values");
    }
    if west < -180.0 || east > 180.0 || west >= east {
        bail!("{label} longitude bounds must satisfy -180 <= west < east <= 180");
    }
    if south < -WEB_MERCATOR_LIMIT || north > WEB_MERCATOR_LIMIT || south >= north {
        bail!("{label} latitude bounds must satisfy -85.05112878 <= south < north <= 85.05112878");
    }
    Ok(())
}

enum ResolvedTileSweep<'a> {
    Borrowed {
        sweep_index: usize,
        sweep: &'a Level2Sweep,
        dealias_applied: bool,
        dealias_report: Option<DealiasReport>,
        velocity_quality_qc: Option<VelocityQualityMaskReport>,
        reflectivity_qc: Option<RadarReflectivityQcSummary>,
    },
    Owned {
        sweep_index: usize,
        sweep: Level2Sweep,
        dealias_applied: bool,
        dealias_report: Option<DealiasReport>,
        velocity_quality_qc: Option<VelocityQualityMaskReport>,
        reflectivity_qc: Option<RadarReflectivityQcSummary>,
    },
}

impl ResolvedTileSweep<'_> {
    fn sweep(&self) -> &Level2Sweep {
        match self {
            Self::Borrowed { sweep, .. } => sweep,
            Self::Owned { sweep, .. } => sweep,
        }
    }

    fn sweep_index(&self) -> usize {
        match self {
            Self::Borrowed { sweep_index, .. } | Self::Owned { sweep_index, .. } => *sweep_index,
        }
    }

    fn dealias_applied(&self) -> bool {
        match self {
            Self::Borrowed {
                dealias_applied, ..
            }
            | Self::Owned {
                dealias_applied, ..
            } => *dealias_applied,
        }
    }

    fn dealias_report(&self) -> Option<&DealiasReport> {
        match self {
            Self::Borrowed { dealias_report, .. } | Self::Owned { dealias_report, .. } => {
                dealias_report.as_ref()
            }
        }
    }

    fn reflectivity_qc(&self) -> Option<&RadarReflectivityQcSummary> {
        match self {
            Self::Borrowed {
                reflectivity_qc, ..
            }
            | Self::Owned {
                reflectivity_qc, ..
            } => reflectivity_qc.as_ref(),
        }
    }

    fn velocity_quality_qc(&self) -> Option<&VelocityQualityMaskReport> {
        match self {
            Self::Borrowed {
                velocity_quality_qc,
                ..
            }
            | Self::Owned {
                velocity_quality_qc,
                ..
            } => velocity_quality_qc.as_ref(),
        }
    }
}

fn resolve_tile_sweep(
    file: &Level2File,
    product: RadarProduct,
    selection: RadarSweepSelection,
    dealias_velocity: bool,
    dealias_method: DealiasMethod,
    force_rejected_dealias: bool,
    velocity_quality_filter: bool,
    reflectivity_despeckle: bool,
    reflectivity_despeckle_min_neighbors: u8,
) -> anyhow::Result<ResolvedTileSweep<'_>> {
    let sample_product = product.base_product();
    if sample_product == RadarProduct::Velocity && dealias_velocity {
        if let Some((sweep_index, sweep)) =
            select_sweep_with_product(file, sample_product, selection)
        {
            let policy = if force_rejected_dealias {
                DealiasAcceptancePolicy::ForceCandidate
            } else {
                DealiasAcceptancePolicy::Safe
            };
            let (dealiased, report) =
                dealias_velocity_sweep_with_policy(sweep, dealias_method, policy);
            let dealias_applied = !sweep_product_data_equal(sweep, &dealiased, sample_product);
            let (dealiased, velocity_quality_qc) = if velocity_quality_filter {
                let (filtered, report) = mask_velocity_sweep_quality(&dealiased);
                (filtered, Some(report))
            } else {
                (dealiased, None)
            };
            return Ok(ResolvedTileSweep::Owned {
                sweep_index,
                sweep: dealiased,
                dealias_applied,
                dealias_report: Some(report),
                velocity_quality_qc,
                reflectivity_qc: None,
            });
        }
    }

    if let Some((sweep_index, sweep)) = select_sweep_with_product(file, sample_product, selection) {
        if sample_product == RadarProduct::Reflectivity && reflectivity_despeckle {
            let (filtered, reflectivity_qc) = despeckle_reflectivity_sweep(
                sweep,
                sample_product,
                reflectivity_despeckle_min_neighbors,
            );
            return Ok(ResolvedTileSweep::Owned {
                sweep_index,
                sweep: filtered,
                dealias_applied: false,
                dealias_report: None,
                velocity_quality_qc: None,
                reflectivity_qc: Some(reflectivity_qc),
            });
        }

        if sample_product == RadarProduct::Velocity && velocity_quality_filter {
            let (filtered, velocity_quality_qc) = mask_velocity_sweep_quality(sweep);
            return Ok(ResolvedTileSweep::Owned {
                sweep_index,
                sweep: filtered,
                dealias_applied: false,
                dealias_report: None,
                velocity_quality_qc: Some(velocity_quality_qc),
                reflectivity_qc: None,
            });
        }

        return Ok(ResolvedTileSweep::Borrowed {
            sweep_index,
            sweep,
            dealias_applied: false,
            dealias_report: None,
            velocity_quality_qc: None,
            reflectivity_qc: None,
        });
    }

    match product {
        RadarProduct::StormRelativeVelocity => {
            let velocity_sweeps_owned = file
                .sweeps
                .iter()
                .filter(|sweep| sweep_contains_product(sweep, RadarProduct::Velocity))
                .map(|sweep| dealias_velocity_sweep(sweep, dealias_method))
                .collect::<Vec<_>>();
            let velocity_sweeps = velocity_sweeps_owned.iter().collect::<Vec<_>>();
            let (sweep_index, velocity_sweep) =
                select_sweep_with_product(file, RadarProduct::Velocity, selection).ok_or_else(
                    || anyhow::anyhow!("cannot derive SRV because the volume has no velocity"),
                )?;
            let (storm_dir_deg, storm_speed_kts) =
                SRVComputer::estimate_storm_motion(&velocity_sweeps);
            let velocity_sweep = dealias_velocity_sweep(velocity_sweep, dealias_method);
            Ok(ResolvedTileSweep::Owned {
                sweep_index,
                sweep: SRVComputer::compute(&velocity_sweep, storm_dir_deg, storm_speed_kts),
                dealias_applied: false,
                dealias_report: None,
                velocity_quality_qc: None,
                reflectivity_qc: None,
            })
        }
        RadarProduct::VIL => {
            let sweep = DerivedProducts::compute_vil(file);
            ensure_nonempty_derived(product, sweep)
        }
        RadarProduct::EchoTops => {
            let sweep = DerivedProducts::compute_echo_tops(file, 18.0);
            ensure_nonempty_derived(product, sweep)
        }
        RadarProduct::SpecificDiffPhase => {
            let (sweep_index, phi_sweep) =
                select_sweep_with_product(file, RadarProduct::DifferentialPhase, selection)
                    .ok_or_else(|| {
                        anyhow::anyhow!("cannot derive KDP because the volume has no PHI")
                    })?;
            let sweep = DerivedProducts::compute_kdp_from_phi_sweep(phi_sweep)
                .ok_or_else(|| anyhow::anyhow!("cannot derive KDP from PHI"))?;
            Ok(ResolvedTileSweep::Owned {
                sweep_index,
                sweep,
                dealias_applied: false,
                dealias_report: None,
                velocity_quality_qc: None,
                reflectivity_qc: None,
            })
        }
        RadarProduct::HydrometeorClass => {
            let (sweep_index, dual_pol_sweep) = select_sweep_with_hca_inputs(file, selection)
                .ok_or_else(|| {
                    anyhow::anyhow!("cannot derive HCA because the volume has no dual-pol inputs")
                })?;
            let sweep = DerivedProducts::compute_hca_from_dual_pol_sweep(dual_pol_sweep)
                .ok_or_else(|| anyhow::anyhow!("cannot derive HCA from dual-pol inputs"))?;
            Ok(ResolvedTileSweep::Owned {
                sweep_index,
                sweep,
                dealias_applied: false,
                dealias_report: None,
                velocity_quality_qc: None,
                reflectivity_qc: None,
            })
        }
        _ => Err(anyhow::anyhow!(
            "volume does not contain product {}",
            product.short_name()
        )),
    }
}

fn sweep_product_data_equal(a: &Level2Sweep, b: &Level2Sweep, product: RadarProduct) -> bool {
    a.radials.len() == b.radials.len()
        && a.radials.iter().zip(b.radials.iter()).all(|(a, b)| {
            match (
                radial_moment_for_product(a, product),
                radial_moment_for_product(b, product),
            ) {
                (Some(a), Some(b)) => moment_data_equal(&a.data, &b.data),
                (None, None) => true,
                _ => false,
            }
        })
}

fn radial_moment_for_product(radial: &RadialData, product: RadarProduct) -> Option<&MomentData> {
    radial
        .moments
        .iter()
        .find(|moment| moment.product == product)
}

fn moment_data_equal(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(a, b)| a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()))
}

fn ensure_nonempty_derived(
    product: RadarProduct,
    sweep: Level2Sweep,
) -> anyhow::Result<ResolvedTileSweep<'static>> {
    if sweep.radials.is_empty() {
        bail!("cannot derive {} from this volume", product.short_name());
    }
    Ok(ResolvedTileSweep::Owned {
        sweep_index: usize::MAX,
        sweep,
        dealias_applied: false,
        dealias_report: None,
        velocity_quality_qc: None,
        reflectivity_qc: None,
    })
}

fn despeckle_reflectivity_sweep(
    sweep: &Level2Sweep,
    product: RadarProduct,
    min_neighbor_count: u8,
) -> (Level2Sweep, RadarReflectivityQcSummary) {
    let mut radials = sweep
        .radials
        .iter()
        .enumerate()
        .filter_map(|(radial_index, radial)| {
            radial
                .moments
                .iter()
                .find(|moment| moment.product == product)
                .map(|moment| (normalize_azimuth(radial.azimuth), radial_index, moment))
        })
        .collect::<Vec<_>>();
    radials.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let max_gates = radials
        .iter()
        .map(|(_, _, moment)| moment.data.len())
        .max()
        .unwrap_or(0);
    let mut finite = vec![vec![false; max_gates]; radials.len()];
    let mut finite_gate_count = 0usize;
    for (row, (_, _, moment)) in radials.iter().enumerate() {
        for gate in 0..moment.data.len() {
            if moment.data[gate].is_finite() {
                finite[row][gate] = true;
                finite_gate_count += 1;
            }
        }
    }

    let mut remove = vec![vec![false; max_gates]; radials.len()];
    let min_neighbors = usize::from(min_neighbor_count);
    for row in 0..finite.len() {
        for gate in 0..max_gates {
            if !finite[row][gate] {
                continue;
            }
            if reflectivity_neighbor_count(&finite, row, gate) < min_neighbors {
                remove[row][gate] = true;
            }
        }
    }

    let mut out = sweep.clone();
    let mut removed_gate_count = 0usize;
    for (row, (_, radial_index, _)) in radials.iter().enumerate() {
        let Some(moment) = out.radials[*radial_index]
            .moments
            .iter_mut()
            .find(|moment| moment.product == product)
        else {
            continue;
        };
        for gate in 0..moment.data.len() {
            if remove[row][gate] {
                moment.data[gate] = f32::NAN;
                removed_gate_count += 1;
            }
        }
    }

    let removed_gate_fraction = if finite_gate_count > 0 {
        removed_gate_count as f64 / finite_gate_count as f64
    } else {
        0.0
    };
    (
        out,
        RadarReflectivityQcSummary {
            product: product.short_name().to_ascii_lowercase(),
            despeckle_applied: true,
            min_neighbor_count,
            finite_gate_count,
            removed_gate_count,
            removed_gate_fraction,
        },
    )
}

fn reflectivity_neighbor_count(finite: &[Vec<bool>], row: usize, gate: usize) -> usize {
    if finite.is_empty() {
        return 0;
    }

    let mut count = 0usize;
    for neighbor_row in reflectivity_neighbor_rows(row, finite.len()) {
        let row_values = &finite[neighbor_row];
        for neighbor_gate in gate.saturating_sub(1)..=gate.saturating_add(1) {
            if neighbor_row == row && neighbor_gate == gate {
                continue;
            }
            if row_values.get(neighbor_gate).copied().unwrap_or(false) {
                count += 1;
            }
        }
    }
    count
}

fn reflectivity_neighbor_rows(row: usize, rows: usize) -> Vec<usize> {
    match rows {
        0 => Vec::new(),
        1 => vec![row],
        2 => vec![row, 1 - row],
        _ => vec![
            if row == 0 { rows - 1 } else { row - 1 },
            row,
            if row + 1 == rows { 0 } else { row + 1 },
        ],
    }
}

pub fn radar_velocity_qc_summary(
    sweep: &Level2Sweep,
    product: RadarProduct,
) -> Option<RadarVelocityQcSummary> {
    let qc_product = velocity_qc_product(product)?;
    let nyquist = effective_nyquist(sweep);
    if !nyquist.is_finite() || nyquist <= 0.0 {
        return None;
    }

    let mut radials = sweep
        .radials
        .iter()
        .filter_map(|radial| {
            radial
                .moments
                .iter()
                .find(|moment| moment.product == qc_product)
                .map(|moment| (normalize_azimuth(radial.azimuth), moment))
        })
        .collect::<Vec<_>>();
    if radials.is_empty() {
        return None;
    }
    radials.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut finite_gate_count = 0usize;
    let mut radial_pair_count = 0usize;
    let mut azimuth_pair_count = 0usize;
    let mut fold_like_jump_count = 0usize;
    let mut severe_jump_count = 0usize;
    let mut max_abs_jump_ms = 0.0f32;
    let fold_threshold = nyquist;
    let severe_threshold = nyquist * 1.5;

    for (_, moment) in &radials {
        finite_gate_count += moment.data.iter().filter(|value| value.is_finite()).count();
        for pair in moment.data.windows(2) {
            let [a, b] = pair else {
                continue;
            };
            if a.is_finite() && b.is_finite() {
                radial_pair_count += 1;
                update_velocity_jump_stats(
                    (*a - *b).abs(),
                    fold_threshold,
                    severe_threshold,
                    &mut fold_like_jump_count,
                    &mut severe_jump_count,
                    &mut max_abs_jump_ms,
                );
            }
        }
    }

    for index in 0..radials.len() {
        let next = if index + 1 == radials.len() {
            0
        } else {
            index + 1
        };
        let az_span = azimuth_span(radials[index].0, radials[next].0);
        if az_span > 10.0 {
            continue;
        }
        let a = &radials[index].1.data;
        let b = &radials[next].1.data;
        for gate in 0..a.len().min(b.len()) {
            let av = a[gate];
            let bv = b[gate];
            if av.is_finite() && bv.is_finite() {
                azimuth_pair_count += 1;
                update_velocity_jump_stats(
                    (av - bv).abs(),
                    fold_threshold,
                    severe_threshold,
                    &mut fold_like_jump_count,
                    &mut severe_jump_count,
                    &mut max_abs_jump_ms,
                );
            }
        }
    }

    let pair_count = radial_pair_count + azimuth_pair_count;
    Some(RadarVelocityQcSummary {
        product: qc_product.short_name().to_ascii_lowercase(),
        nyquist_ms: nyquist,
        finite_gate_count,
        radial_pair_count,
        azimuth_pair_count,
        fold_like_jump_count,
        severe_jump_count,
        fold_like_jump_fraction: if pair_count > 0 {
            fold_like_jump_count as f64 / pair_count as f64
        } else {
            0.0
        },
        max_abs_jump_ms,
    })
}

pub fn radar_product_qc_summary(
    sweep: &Level2Sweep,
    product: RadarProduct,
) -> Option<RadarProductQcSummary> {
    let qc_product = product_qc_product(product)?;
    let mut finite_gate_count = 0usize;
    let mut min_value = f32::INFINITY;
    let mut max_value = f32::NEG_INFINITY;
    let mut sum = 0.0f64;

    for radial in &sweep.radials {
        for moment in radial
            .moments
            .iter()
            .filter(|moment| moment.product == qc_product)
        {
            for value in moment
                .data
                .iter()
                .copied()
                .filter(|value| value.is_finite())
            {
                finite_gate_count += 1;
                min_value = min_value.min(value);
                max_value = max_value.max(value);
                sum += f64::from(value);
            }
        }
    }

    if finite_gate_count == 0 {
        return None;
    }

    Some(RadarProductQcSummary {
        product: qc_product.short_name().to_ascii_lowercase(),
        finite_gate_count,
        min_value,
        max_value,
        mean_value: sum / finite_gate_count as f64,
    })
}

fn velocity_qc_product(product: RadarProduct) -> Option<RadarProduct> {
    match product {
        RadarProduct::StormRelativeVelocity => Some(RadarProduct::StormRelativeVelocity),
        _ => match product.base_product() {
            RadarProduct::Velocity | RadarProduct::SuperResVelocity => Some(product.base_product()),
            _ => None,
        },
    }
}

fn product_qc_product(product: RadarProduct) -> Option<RadarProduct> {
    match product {
        RadarProduct::VIL
        | RadarProduct::EchoTops
        | RadarProduct::StormRelativeVelocity
        | RadarProduct::Unknown => None,
        other => Some(other.base_product()),
    }
}

fn radar_product_provenance(
    file: &Level2File,
    product: RadarProduct,
    selection: RadarSweepSelection,
) -> RadarProductProvenance {
    if select_sweep_with_product(file, product.base_product(), selection).is_some() {
        return native_product_provenance();
    }

    if product == RadarProduct::SpecificDiffPhase
        && select_sweep_with_product(file, RadarProduct::DifferentialPhase, selection).is_some()
    {
        return RadarProductProvenance {
            source: "derived".to_string(),
            derived: true,
            inputs: vec![
                RadarProduct::DifferentialPhase
                    .short_name()
                    .to_ascii_lowercase(),
            ],
            method: Some("centered_phi_range_derivative".to_string()),
        };
    }

    if product == RadarProduct::HydrometeorClass {
        if let Some((_, sweep)) = select_sweep_with_hca_inputs(file, selection) {
            return RadarProductProvenance {
                source: "derived".to_string(),
                derived: true,
                inputs: hca_product_provenance_inputs(sweep),
                method: Some("dual_pol_rule_hca_v1".to_string()),
            };
        }
    }

    if product == RadarProduct::StormRelativeVelocity
        && select_sweep_with_product(file, RadarProduct::Velocity, selection).is_some()
    {
        return RadarProductProvenance {
            source: "derived".to_string(),
            derived: true,
            inputs: vec![RadarProduct::Velocity.short_name().to_ascii_lowercase()],
            method: Some("storm_motion_subtraction".to_string()),
        };
    }

    if product == RadarProduct::VIL && lowest_reflectivity_sweep_exists(file) {
        return RadarProductProvenance {
            source: "derived".to_string(),
            derived: true,
            inputs: vec![RadarProduct::Reflectivity.short_name().to_ascii_lowercase()],
            method: Some("vertical_reflectivity_integration".to_string()),
        };
    }

    if product == RadarProduct::EchoTops && lowest_reflectivity_sweep_exists(file) {
        return RadarProductProvenance {
            source: "derived".to_string(),
            derived: true,
            inputs: vec![RadarProduct::Reflectivity.short_name().to_ascii_lowercase()],
            method: Some("reflectivity_threshold_beam_height".to_string()),
        };
    }

    native_product_provenance()
}

fn hca_product_provenance_inputs(sweep: &Level2Sweep) -> Vec<String> {
    let mut inputs = vec!["ref".to_string(), "zdr".to_string(), "cc".to_string()];
    if sweep_contains_product(sweep, RadarProduct::SpecificDiffPhase) {
        inputs.push("kdp".to_string());
    } else {
        inputs.push("phi".to_string());
    }
    inputs
}

fn native_product_provenance() -> RadarProductProvenance {
    RadarProductProvenance {
        source: "native".to_string(),
        derived: false,
        inputs: Vec::new(),
        method: None,
    }
}

fn radar_processing_state(
    product: RadarProduct,
    provenance: &RadarProductProvenance,
    options: &RadarTileOptions,
    resolved: &ResolvedTileSweep<'_>,
) -> String {
    let mut parts = Vec::new();
    if provenance.derived {
        parts.push("derived");
    }
    let sample_product = product.base_product();
    if sample_product == RadarProduct::Velocity
        && options.dealias_velocity
        && resolved.dealias_applied()
    {
        parts.push("dealiased");
    }
    if (sample_product == RadarProduct::Velocity && options.velocity_quality_filter)
        || (sample_product == RadarProduct::Reflectivity && options.reflectivity_despeckle)
    {
        parts.push("filtered");
    }
    if parts.is_empty() {
        "raw".to_string()
    } else {
        parts.join("_")
    }
}

fn lowest_reflectivity_sweep_exists(file: &Level2File) -> bool {
    select_sweep_with_product(
        file,
        RadarProduct::Reflectivity,
        RadarSweepSelection::Lowest,
    )
    .is_some()
}

fn azimuth_span(lo: f32, hi: f32) -> f32 {
    let mut span = hi - lo;
    if span < 0.0 {
        span += 360.0;
    }
    span
}

fn update_velocity_jump_stats(
    jump: f32,
    fold_threshold: f32,
    severe_threshold: f32,
    fold_like_jump_count: &mut usize,
    severe_jump_count: &mut usize,
    max_abs_jump_ms: &mut f32,
) {
    *max_abs_jump_ms = (*max_abs_jump_ms).max(jump);
    if jump > fold_threshold {
        *fold_like_jump_count += 1;
    }
    if jump > severe_threshold {
        *severe_jump_count += 1;
    }
}

struct PreparedSweep<'a> {
    color_table: ColorTable,
    cos_elev: f64,
    max_ground_range_m: f64,
    azimuths: Vec<f32>,
    moments: Vec<&'a MomentData>,
    opacity: f64,
}

impl<'a> PreparedSweep<'a> {
    fn new(
        sweep: &'a Level2Sweep,
        product: RadarProduct,
        min_value: Option<f32>,
        color_table_preset: ColorTablePreset,
        opacity: f64,
    ) -> anyhow::Result<Self> {
        let sample_product = product.base_product();
        let elev_rad = f64::from(sweep.elevation_angle).to_radians();
        let cos_elev = elev_rad.cos().max(0.1);
        let max_slant_range_m = sweep
            .radials
            .iter()
            .filter_map(|radial| {
                radial
                    .moments
                    .iter()
                    .find(|moment| moment.product == sample_product)
                    .map(|moment| {
                        f64::from(moment.first_gate_range)
                            + f64::from(moment.gate_count) * f64::from(moment.gate_size)
                    })
            })
            .fold(0.0f64, f64::max);
        if max_slant_range_m <= 0.0 {
            bail!("sweep has no range data for {}", product.short_name());
        }

        let mut sorted = sweep
            .radials
            .iter()
            .filter_map(|radial| {
                radial
                    .moments
                    .iter()
                    .find(|moment| moment.product == sample_product)
                    .map(|moment| (normalize_azimuth(radial.azimuth), moment))
            })
            .collect::<Vec<_>>();
        if sorted.is_empty() {
            bail!("sweep has no radials for {}", product.short_name());
        }
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let azimuths: Vec<f32> = sorted.iter().map(|(azimuth, _)| *azimuth).collect();
        let moments: Vec<&'a MomentData> = sorted.iter().map(|(_, moment)| *moment).collect();
        let color_table = match min_value {
            Some(min_value) => ColorTable::for_product_preset(product, color_table_preset)
                .with_min_value(min_value),
            None => ColorTable::for_product_preset(product, color_table_preset),
        };

        Ok(Self {
            color_table,
            cos_elev,
            max_ground_range_m: max_slant_range_m * cos_elev,
            azimuths,
            moments,
            opacity,
        })
    }

    fn color_at_azimuth_range(&self, azimuth_deg: f32, ground_range_m: f64) -> Option<[u8; 4]> {
        if ground_range_m <= 0.0 || ground_range_m > self.max_ground_range_m {
            return None;
        }
        let range_m = ground_range_m / self.cos_elev;
        let value = self.sample(azimuth_deg, range_m)?;
        let mut color = self.color_table.color_for_value(value);
        if color[3] == 0 {
            return None;
        }
        color[3] = (f64::from(color[3]) * self.opacity)
            .round()
            .clamp(0.0, 255.0) as u8;
        (color[3] > 0).then_some(color)
    }

    fn native_gate_size_m(&self) -> Option<u16> {
        self.moments
            .iter()
            .filter_map(|moment| (moment.gate_size > 0).then_some(moment.gate_size))
            .min()
    }

    fn native_azimuth_spacing_deg(&self) -> Option<f64> {
        if self.azimuths.len() < 2 {
            return None;
        }
        let mut spans = (0..self.azimuths.len())
            .map(|index| {
                let next = if index + 1 == self.azimuths.len() {
                    0
                } else {
                    index + 1
                };
                f64::from(azimuth_span(self.azimuths[index], self.azimuths[next]))
            })
            .filter(|span| *span > 0.0 && *span <= 10.0)
            .collect::<Vec<_>>();
        if spans.is_empty() {
            return None;
        }
        spans.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = spans.len() / 2;
        if spans.len() % 2 == 0 {
            Some((spans[mid - 1] + spans[mid]) / 2.0)
        } else {
            Some(spans[mid])
        }
    }

    fn sample(&self, azimuth_deg: f32, range_m: f64) -> Option<f32> {
        if self.azimuths.len() == 1 {
            return sample_moment_interp(self.moments[0], range_m);
        }

        let azimuth = normalize_azimuth(azimuth_deg);
        let n_az = self.azimuths.len();
        let insert_pos = match self.azimuths.binary_search_by(|candidate| {
            candidate
                .partial_cmp(&azimuth)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            Ok(index) => index,
            Err(index) => index,
        };
        let lo_sorted = if insert_pos == 0 {
            n_az - 1
        } else {
            insert_pos - 1
        };
        let hi_sorted = if insert_pos >= n_az { 0 } else { insert_pos };
        let az_lo = self.azimuths[lo_sorted];
        let az_hi = self.azimuths[hi_sorted];
        let mut az_span = az_hi - az_lo;
        if az_span < 0.0 {
            az_span += 360.0;
        }
        if az_span > 10.0 {
            return None;
        }
        let mut az_off = azimuth - az_lo;
        if az_off < 0.0 {
            az_off += 360.0;
        }
        let az_t = if az_span > 0.001 {
            (az_off / az_span).min(1.0)
        } else {
            0.0
        };

        let lo = sample_moment_interp(self.moments[lo_sorted], range_m);
        let hi = sample_moment_interp(self.moments[hi_sorted], range_m);
        match (lo, hi) {
            (Some(a), Some(b)) => Some(a + (b - a) * az_t),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }
        .filter(|value| value.is_finite())
    }
}

#[allow(clippy::too_many_arguments)]
fn render_tile(
    prepared: &PreparedSweep,
    site: &RadarSite,
    bounds: [f64; 4],
    z: u8,
    x: u32,
    y: u32,
    tile_size: u32,
    sample_factor: u8,
    out_dir: &Path,
    compression: RadarTilePngCompression,
    skip_empty_tiles: bool,
) -> anyhow::Result<Option<RadarTileRecord>> {
    let mut pixels = vec![0u8; tile_size as usize * tile_size as usize * 4];
    let mut nontransparent_pixels = 0u32;
    let sample_factor = u32::from(sample_factor.max(1));
    let sample_grid_size = tile_size * sample_factor;
    let lon_by_sample = tile_column_longitudes(z, x, sample_grid_size);
    let lat_by_sample = tile_row_latitudes(z, y, sample_grid_size);
    let sample_count = f64::from(sample_factor * sample_factor);
    for py in 0..tile_size {
        for px in 0..tile_size {
            let mut alpha_sum = 0.0;
            let mut red_sum = 0.0;
            let mut green_sum = 0.0;
            let mut blue_sum = 0.0;
            for sy in 0..sample_factor {
                let sample_y = py * sample_factor + sy;
                let lat = lat_by_sample[sample_y as usize];
                if lat < bounds[1] || lat > bounds[3] {
                    continue;
                }
                for sx in 0..sample_factor {
                    let sample_x = px * sample_factor + sx;
                    let lon = lon_by_sample[sample_x as usize];
                    if lon < bounds[0] || lon > bounds[2] {
                        continue;
                    }
                    let polar = radar_lat_lon_to_polar(site.lat, site.lon, lat, lon);
                    let Some(color) =
                        prepared.color_at_azimuth_range(polar.azimuth_deg, polar.ground_range_m)
                    else {
                        continue;
                    };
                    let alpha = f64::from(color[3]) / 255.0;
                    alpha_sum += alpha;
                    red_sum += f64::from(color[0]) * alpha;
                    green_sum += f64::from(color[1]) * alpha;
                    blue_sum += f64::from(color[2]) * alpha;
                }
            }
            if alpha_sum <= 0.0 {
                continue;
            }

            let idx = ((py * tile_size + px) * 4) as usize;
            pixels[idx] = (red_sum / alpha_sum).round().clamp(0.0, 255.0) as u8;
            pixels[idx + 1] = (green_sum / alpha_sum).round().clamp(0.0, 255.0) as u8;
            pixels[idx + 2] = (blue_sum / alpha_sum).round().clamp(0.0, 255.0) as u8;
            pixels[idx + 3] = ((alpha_sum / sample_count) * 255.0)
                .round()
                .clamp(0.0, 255.0) as u8;
            nontransparent_pixels = nontransparent_pixels.saturating_add(1);
        }
    }
    if nontransparent_pixels == 0 && skip_empty_tiles {
        return Ok(None);
    }
    let path = out_dir.join(z.to_string()).join(x.to_string());
    fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
    let path = path.join(format!("{y}.png"));
    write_png_rgba(&path, &pixels, tile_size, tile_size, compression)?;
    Ok(Some(RadarTileRecord {
        z,
        x,
        y,
        path,
        nontransparent_pixels,
    }))
}

fn write_png_rgba(
    path: &Path,
    pixels: &[u8],
    width: u32,
    height: u32,
    compression: RadarTilePngCompression,
) -> anyhow::Result<()> {
    let (compression, filter) = match compression {
        RadarTilePngCompression::Default => (CompressionType::Default, PngFilterType::Up),
        RadarTilePngCompression::Fast => (CompressionType::Fast, PngFilterType::Up),
        RadarTilePngCompression::Fastest => (CompressionType::Fast, PngFilterType::NoFilter),
    };
    let mut out = Vec::new();
    let encoder = PngEncoder::new_with_quality(&mut out, compression, filter);
    encoder.write_image(pixels, width, height, ExtendedColorType::Rgba8)?;
    fs::write(path, out).with_context(|| format!("write {}", path.display()))
}

fn sample_moment_interp(moment: &MomentData, range_m: f64) -> Option<f32> {
    if moment.gate_size == 0 {
        return None;
    }
    let gate_offset = range_m - f64::from(moment.first_gate_range);
    if gate_offset < 0.0 {
        return None;
    }
    let gate_f = gate_offset / f64::from(moment.gate_size);
    let gate_lo = gate_f as usize;
    if gate_lo >= moment.data.len() {
        return None;
    }
    let v0 = moment.data[gate_lo];
    if !v0.is_finite() {
        return None;
    }
    let gate_hi = gate_lo + 1;
    if gate_hi < moment.data.len() {
        let v1 = moment.data[gate_hi];
        if v1.is_finite() {
            let t = (gate_f - gate_lo as f64) as f32;
            return Some(v0 + (v1 - v0) * t);
        }
    }
    Some(v0)
}

fn tile_jobs(bounds: [f64; 4], min_zoom: u8, max_zoom: u8) -> anyhow::Result<Vec<(u8, u32, u32)>> {
    let mut jobs = Vec::new();
    for z in min_zoom..=max_zoom {
        let n = 1u32
            .checked_shl(u32::from(z))
            .ok_or_else(|| anyhow::anyhow!("zoom too large"))?;
        let x0 = lon_to_tile_x(bounds[0], z).min(n.saturating_sub(1));
        let x1 = lon_to_tile_x(bounds[2], z).min(n.saturating_sub(1));
        let y0 = lat_to_tile_y(bounds[3], z).min(n.saturating_sub(1));
        let y1 = lat_to_tile_y(bounds[1], z).min(n.saturating_sub(1));
        for y in y0..=y1 {
            for x in x0..=x1 {
                jobs.push((z, x, y));
            }
        }
    }
    Ok(jobs)
}

fn radar_coverage_bounds(site: &RadarSite, max_ground_range_m: f64) -> [f64; 4] {
    let lat_radius = max_ground_range_m / 111_139.0;
    let cos_lat = site.lat.to_radians().cos().abs().max(0.01);
    let lon_radius = max_ground_range_m / (111_139.0 * cos_lat);
    [
        (site.lon - lon_radius).max(-180.0),
        (site.lat - lat_radius).max(-WEB_MERCATOR_LIMIT),
        (site.lon + lon_radius).min(180.0),
        (site.lat + lat_radius).min(WEB_MERCATOR_LIMIT),
    ]
}

fn web_mercator_meters_per_pixel(lat: f64, z: u8) -> f64 {
    const EQUATOR_METERS_PER_PIXEL_Z0: f64 = 156_543.033_928_040_97;
    EQUATOR_METERS_PER_PIXEL_Z0 * lat.to_radians().cos().abs() / 2.0_f64.powi(i32::from(z))
}

fn intersect_bounds(a: [f64; 4], b: [f64; 4]) -> Option<[f64; 4]> {
    let bounds = [
        a[0].max(b[0]),
        a[1].max(b[1]),
        a[2].min(b[2]),
        a[3].min(b[3]),
    ];
    (bounds[0] < bounds[2] && bounds[1] < bounds[3]).then_some(bounds)
}

fn radar_sampling_bounds(
    tile_bounds: [f64; 4],
    coverage_bounds: [f64; 4],
    clip_to_bounds: bool,
) -> [f64; 4] {
    if clip_to_bounds {
        tile_bounds
    } else {
        coverage_bounds
    }
}

fn lon_to_tile_x(lon: f64, z: u8) -> u32 {
    let n = 2.0_f64.powi(i32::from(z));
    (((lon + 180.0) / 360.0) * n).floor().max(0.0) as u32
}

fn lat_to_tile_y(lat: f64, z: u8) -> u32 {
    let lat = lat.clamp(-WEB_MERCATOR_LIMIT, WEB_MERCATOR_LIMIT);
    let n = 2.0_f64.powi(i32::from(z));
    let lat_rad = lat.to_radians();
    ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n)
        .floor()
        .max(0.0) as u32
}

#[cfg(test)]
fn tile_pixel_lon_lat(z: u8, x: u32, y: u32, px: u32, py: u32, tile_size: u32) -> (f64, f64) {
    let n = 2.0_f64.powi(i32::from(z));
    let xf = (f64::from(x) + (f64::from(px) + 0.5) / f64::from(tile_size)) / n;
    let yf = (f64::from(y) + (f64::from(py) + 0.5) / f64::from(tile_size)) / n;
    let lon = xf * 360.0 - 180.0;
    let merc_y = PI * (1.0 - 2.0 * yf);
    let lat = merc_y.sinh().atan().to_degrees();
    (lon, lat)
}

fn tile_column_longitudes(z: u8, x: u32, tile_size: u32) -> Vec<f64> {
    let n = 2.0_f64.powi(i32::from(z));
    let scale = 360.0 / (n * f64::from(tile_size));
    let first = ((f64::from(x) / n) * 360.0 - 180.0) + 0.5 * scale;
    (0..tile_size)
        .map(|px| first + f64::from(px) * scale)
        .collect()
}

fn tile_row_latitudes(z: u8, y: u32, tile_size: u32) -> Vec<f64> {
    let n = 2.0_f64.powi(i32::from(z));
    (0..tile_size)
        .map(|py| {
            let yf = (f64::from(y) + (f64::from(py) + 0.5) / f64::from(tile_size)) / n;
            let merc_y = PI * (1.0 - 2.0 * yf);
            merc_y.sinh().atan().to_degrees()
        })
        .collect()
}

fn normalize_azimuth(value: f32) -> f32 {
    value.rem_euclid(360.0)
}

fn scan_time_utc(file: &Level2File) -> String {
    DateTime::<Utc>::from_timestamp_millis(file.unix_timestamp_ms())
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn atomic_write_json(path: impl AsRef<Path>, value: &impl Serialize) -> anyhow::Result<()> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("move {} to {}", tmp_path.display(), path.display()))
}

#[cfg(test)]
mod tests;
