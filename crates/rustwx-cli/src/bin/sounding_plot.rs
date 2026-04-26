use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::gridded::{
    PressureFields, SurfaceFields, load_model_timestep_from_parts_cropped,
};
use rustwx_sounding::{SoundingColumn, SoundingMetadata, write_full_sounding_png};
use serde::Serialize;

const SCHEMA_VERSION: u32 = 1;
const INTERPOLATED_NEIGHBOR_COUNT: usize = 4;

#[derive(Debug, Parser)]
#[command(
    name = "sounding-plot",
    about = "Render a native Rust SHARPpy-style model sounding at a latitude/longitude"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long)]
    date: String,
    #[arg(long)]
    cycle: u8,
    #[arg(long, default_value_t = 1)]
    forecast_hour: u16,
    #[arg(long, default_value = "aws")]
    source: SourceId,
    #[arg(long, allow_hyphen_values = true)]
    lat: f64,
    #[arg(long, allow_hyphen_values = true)]
    lon: f64,
    #[arg(long, default_value_t = 1.0)]
    crop_radius_deg: f64,
    #[arg(long, value_enum, default_value_t = SoundingSampleMethod::Nearest)]
    sample_method: SoundingSampleMethod,
    #[arg(long)]
    box_radius_km: Option<f64>,
    #[arg(long, allow_hyphen_values = true)]
    box_radius_deg: Option<f64>,
    #[arg(long)]
    station_id: Option<String>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    manifest: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    include_column: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum SoundingSampleMethod {
    Nearest,
    InverseDistance4,
    BoxMean,
}

#[derive(Debug, Serialize)]
struct SoundingPlotReport {
    schema_version: u32,
    renderer: &'static str,
    request: SoundingPlotRequest,
    shared_timing: rustwx_products::gridded::SharedTiming,
    nearest_grid_point: GridPointSummary,
    sampled_point: SampledPointSummary,
    profile: SoundingProfileSummary,
    output: SoundingOutput,
    timing: SoundingTiming,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    column: Option<SoundingColumn>,
}

#[derive(Debug, Serialize)]
struct SoundingPlotRequest {
    model: ModelId,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    requested_lat: f64,
    requested_lon: f64,
    crop_bounds: (f64, f64, f64, f64),
    sample_method: SoundingSampleMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    box_radius_km: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    box_radius_deg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_box_radius_deg: Option<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize)]
struct GridPointSummary {
    index: usize,
    i: usize,
    j: usize,
    lat: f64,
    lon: f64,
    distance_deg: f64,
}

#[derive(Debug, Serialize)]
struct SampledPointSummary {
    lat: f64,
    lon: f64,
    surface_pressure_hpa: f64,
    surface_height_m_msl: f64,
    contributing_points: Vec<WeightedGridPoint>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct WeightedGridPoint {
    index: usize,
    i: usize,
    j: usize,
    lat: f64,
    lon: f64,
    weight: f64,
}

#[derive(Debug, Serialize)]
struct SoundingProfileSummary {
    station_id: String,
    valid_time: String,
    levels: usize,
    pressure_bottom_hpa: f64,
    pressure_top_hpa: f64,
    height_bottom_m_msl: f64,
    height_top_m_msl: f64,
    temperature_surface_c: f64,
    dewpoint_surface_c: f64,
}

#[derive(Debug, Serialize)]
struct SoundingOutput {
    png: String,
    manifest: String,
}

#[derive(Debug, Default, Serialize)]
struct SoundingTiming {
    load_ms: u128,
    build_column_ms: u128,
    render_ms: u128,
    total_ms: u128,
}

#[derive(Debug, Clone)]
struct SampleStencil {
    points: Vec<WeightedGridPoint>,
}

#[derive(Debug, Clone, Copy)]
struct BoxRadiusDeg {
    lat: f64,
    lon: f64,
}

#[derive(Debug, Clone, Copy)]
struct SoundingLevel {
    pressure_hpa: f64,
    height_m_msl: f64,
    temperature_c: f64,
    dewpoint_c: f64,
    u_ms: f64,
    v_ms: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let total_start = Instant::now();

    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("proof").join("soundings"));
    ensure_dir(&out_dir)?;

    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(out_dir.as_path()));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let box_radius = box_radius_deg_for_args(&args)?;
    let effective_crop_radius_deg = effective_crop_radius_deg(&args, box_radius);
    let bounds = (
        args.lon - effective_crop_radius_deg,
        args.lon + effective_crop_radius_deg,
        args.lat - effective_crop_radius_deg,
        args.lat + effective_crop_radius_deg,
    );

    let load_start = Instant::now();
    let loaded = load_model_timestep_from_parts_cropped(
        args.model,
        &args.date,
        Some(args.cycle),
        args.forecast_hour,
        args.source,
        None,
        None,
        &cache_root,
        !args.no_cache,
        bounds,
    )?;
    let load_ms = load_start.elapsed().as_millis();

    let build_start = Instant::now();
    let nearest =
        nearest_point(&loaded.surface_decode.value, args.lat, args.lon)?.ok_or("empty grid")?;
    let stencil = sample_stencil(
        &loaded.surface_decode.value,
        args.lat,
        args.lon,
        args.sample_method,
        box_radius,
    )?;
    let (column, sampled_point) = build_sounding_column(
        &loaded.surface_decode.value,
        &loaded.pressure_decode.value,
        &stencil,
        build_metadata(&args, box_radius),
    )?;
    let build_column_ms = build_start.elapsed().as_millis();

    let output_path = args
        .output
        .clone()
        .unwrap_or_else(|| out_dir.join(format!("{}.png", default_artifact_stem(&args))));
    let manifest_path = args
        .manifest
        .clone()
        .unwrap_or_else(|| out_dir.join(format!("{}_manifest.json", default_artifact_stem(&args))));
    ensure_parent(&output_path)?;
    ensure_parent(&manifest_path)?;

    let render_start = Instant::now();
    write_full_sounding_png(&column, &output_path)?;
    let render_ms = render_start.elapsed().as_millis();

    let summary = profile_summary(&column);
    let report = SoundingPlotReport {
        schema_version: SCHEMA_VERSION,
        renderer: "rustwx-sounding native Rust SHARPpy-style renderer",
        request: SoundingPlotRequest {
            model: args.model,
            date_yyyymmdd: args.date.clone(),
            cycle_utc: args.cycle,
            forecast_hour: args.forecast_hour,
            source: args.source,
            requested_lat: args.lat,
            requested_lon: args.lon,
            crop_bounds: bounds,
            sample_method: args.sample_method,
            box_radius_km: args.box_radius_km,
            box_radius_deg: args.box_radius_deg,
            effective_box_radius_deg: box_radius.map(|radius| (radius.lat, radius.lon)),
        },
        shared_timing: loaded.shared_timing.clone(),
        nearest_grid_point: nearest,
        sampled_point,
        profile: summary,
        output: SoundingOutput {
            png: output_path.display().to_string(),
            manifest: manifest_path.display().to_string(),
        },
        timing: SoundingTiming {
            load_ms,
            build_column_ms,
            render_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
        column: args.include_column.then_some(column),
    };

    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(&manifest_path, json.as_bytes())?;
    println!("{json}");

    Ok(())
}

fn build_metadata(args: &Args, box_radius: Option<BoxRadiusDeg>) -> SoundingMetadata {
    let station_id = args
        .station_id
        .clone()
        .unwrap_or_else(|| format!("{} {:.2},{:.2}", args.model, args.lat, args.lon));
    SoundingMetadata {
        station_id,
        valid_time: format!(
            "{} {:02}Z F{:03}",
            args.date, args.cycle, args.forecast_hour
        ),
        latitude_deg: Some(args.lat),
        longitude_deg: Some(args.lon),
        elevation_m: None,
        sample_method: Some(sample_method_metadata_name(args.sample_method).to_string()),
        box_radius_lat_deg: box_radius.map(|radius| radius.lat),
        box_radius_lon_deg: box_radius.map(|radius| radius.lon),
    }
}

fn sample_method_metadata_name(method: SoundingSampleMethod) -> &'static str {
    match method {
        SoundingSampleMethod::Nearest => "nearest",
        SoundingSampleMethod::InverseDistance4 => "inverse_distance_4",
        SoundingSampleMethod::BoxMean => "box_mean",
    }
}

fn build_sounding_column(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    stencil: &SampleStencil,
    mut metadata: SoundingMetadata,
) -> Result<(SoundingColumn, SampledPointSummary), Box<dyn std::error::Error>> {
    let psfc_hpa = sample_2d(&surface.psfc_pa, stencil) / 100.0;
    let surface_height_m_msl = surface_height_m_msl(surface, pressure, stencil);
    let t2_k = sample_2d(&surface.t2_k, stencil);
    let q2_kgkg = sample_2d(&surface.q2_kgkg, stencil);
    let surface_level = SoundingLevel {
        pressure_hpa: psfc_hpa,
        height_m_msl: surface_height_m_msl,
        temperature_c: t2_k - 273.15,
        dewpoint_c: dewpoint_c_from_q(q2_kgkg, psfc_hpa * 100.0, t2_k),
        u_ms: sample_2d(&surface.u10_ms, stencil),
        v_ms: sample_2d(&surface.v10_ms, stencil),
    };

    metadata.elevation_m = Some(surface_level.height_m_msl);

    let nxy = surface.nx * surface.ny;
    let mut levels = Vec::with_capacity(pressure.pressure_levels_hpa.len() + 1);
    levels.push(surface_level);

    for k in 0..pressure.pressure_levels_hpa.len() {
        let level_offset = k * nxy;
        let pressure_hpa = pressure
            .pressure_3d_pa
            .as_ref()
            .map(|values| sample_level(values, level_offset, stencil) / 100.0)
            .unwrap_or(pressure.pressure_levels_hpa[k]);
        let height_m_msl = sample_level(&pressure.gh_m_3d, level_offset, stencil);
        let temperature_c = sample_level(&pressure.temperature_c_3d, level_offset, stencil);
        let q_kgkg = sample_level(&pressure.qvapor_kgkg_3d, level_offset, stencil);
        let temperature_k = temperature_c + 273.15;
        if pressure_hpa >= psfc_hpa - 0.1 || height_m_msl <= surface_level.height_m_msl + 1.0 {
            continue;
        }
        levels.push(SoundingLevel {
            pressure_hpa,
            height_m_msl,
            temperature_c,
            dewpoint_c: dewpoint_c_from_q(q_kgkg, pressure_hpa * 100.0, temperature_k),
            u_ms: sample_level(&pressure.u_ms_3d, level_offset, stencil),
            v_ms: sample_level(&pressure.v_ms_3d, level_offset, stencil),
        });
    }

    levels.retain(|level| {
        level.pressure_hpa.is_finite()
            && level.height_m_msl.is_finite()
            && level.temperature_c.is_finite()
            && level.dewpoint_c.is_finite()
            && level.u_ms.is_finite()
            && level.v_ms.is_finite()
            && level.pressure_hpa > 0.0
    });
    levels.sort_by(|a, b| {
        b.pressure_hpa
            .partial_cmp(&a.pressure_hpa)
            .unwrap_or(Ordering::Equal)
    });

    let mut column = SoundingColumn {
        pressure_hpa: Vec::with_capacity(levels.len()),
        height_m_msl: Vec::with_capacity(levels.len()),
        temperature_c: Vec::with_capacity(levels.len()),
        dewpoint_c: Vec::with_capacity(levels.len()),
        u_ms: Vec::with_capacity(levels.len()),
        v_ms: Vec::with_capacity(levels.len()),
        omega_pa_s: Vec::new(),
        metadata,
    };

    for level in levels {
        push_sounding_level(&mut column, level);
    }
    column.validate()?;

    let sampled = SampledPointSummary {
        lat: weighted_mean(stencil.points.iter().map(|point| (point.lat, point.weight))),
        lon: weighted_mean(stencil.points.iter().map(|point| (point.lon, point.weight))),
        surface_pressure_hpa: psfc_hpa,
        surface_height_m_msl: surface_level.height_m_msl,
        contributing_points: stencil.points.clone(),
    };

    Ok((column, sampled))
}

fn push_sounding_level(column: &mut SoundingColumn, level: SoundingLevel) {
    if let (Some(&last_p), Some(&last_z)) = (column.pressure_hpa.last(), column.height_m_msl.last())
    {
        if level.pressure_hpa >= last_p - 1.0e-6 || level.height_m_msl <= last_z + 1.0e-6 {
            return;
        }
    }

    column.pressure_hpa.push(level.pressure_hpa);
    column.height_m_msl.push(level.height_m_msl);
    column.temperature_c.push(level.temperature_c);
    column
        .dewpoint_c
        .push(level.dewpoint_c.min(level.temperature_c));
    column.u_ms.push(level.u_ms);
    column.v_ms.push(level.v_ms);
}

fn surface_height_m_msl(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    stencil: &SampleStencil,
) -> f64 {
    let sampled_orog = sample_2d(&surface.orog_m, stencil);
    if sampled_orog.is_finite() && !surface.orog_is_proxy {
        return sampled_orog;
    }

    let nxy = surface.nx * surface.ny;
    let mut min_height = f64::INFINITY;
    for k in 0..pressure.pressure_levels_hpa.len() {
        let height = sample_level(&pressure.gh_m_3d, k * nxy, stencil);
        if height.is_finite() {
            min_height = min_height.min(height);
        }
    }
    if min_height.is_finite() {
        min_height
    } else {
        sampled_orog
    }
}

fn profile_summary(column: &SoundingColumn) -> SoundingProfileSummary {
    SoundingProfileSummary {
        station_id: column.metadata.station_id.clone(),
        valid_time: column.metadata.valid_time.clone(),
        levels: column.len(),
        pressure_bottom_hpa: *column.pressure_hpa.first().unwrap_or(&f64::NAN),
        pressure_top_hpa: *column.pressure_hpa.last().unwrap_or(&f64::NAN),
        height_bottom_m_msl: *column.height_m_msl.first().unwrap_or(&f64::NAN),
        height_top_m_msl: *column.height_m_msl.last().unwrap_or(&f64::NAN),
        temperature_surface_c: *column.temperature_c.first().unwrap_or(&f64::NAN),
        dewpoint_surface_c: *column.dewpoint_c.first().unwrap_or(&f64::NAN),
    }
}

fn sample_stencil(
    surface: &SurfaceFields,
    lat: f64,
    lon: f64,
    method: SoundingSampleMethod,
    box_radius: Option<BoxRadiusDeg>,
) -> Result<SampleStencil, Box<dyn std::error::Error>> {
    if surface.lat.is_empty() || surface.lon.is_empty() {
        return Err("surface grid is empty".into());
    }
    if matches!(method, SoundingSampleMethod::BoxMean) {
        return Ok(SampleStencil {
            points: box_mean_points(
                surface,
                lat,
                lon,
                box_radius.unwrap_or_else(|| box_radius_deg_from_km(lat, DEFAULT_BOX_RADIUS_KM)),
            ),
        });
    }
    let keep = match method {
        SoundingSampleMethod::Nearest => 1,
        SoundingSampleMethod::InverseDistance4 => INTERPOLATED_NEIGHBOR_COUNT,
        SoundingSampleMethod::BoxMean => unreachable!("box mean is handled above"),
    };

    let mut nearest = Vec::<(usize, f64)>::new();
    for idx in 0..surface.lat.len() {
        let score = geographic_distance_score(surface, idx, lat, lon);
        insert_nearest(&mut nearest, keep, idx, score);
    }

    if nearest.is_empty() {
        return Err("surface grid is empty".into());
    }

    let points = if nearest[0].1 <= 1.0e-12 || matches!(method, SoundingSampleMethod::Nearest) {
        vec![weighted_point(surface, nearest[0].0, 1.0)]
    } else {
        let mut raw = nearest
            .iter()
            .map(|&(idx, distance)| (idx, 1.0 / distance.max(1.0e-12)))
            .collect::<Vec<_>>();
        let total = raw.iter().map(|(_, weight)| *weight).sum::<f64>();
        raw.iter_mut()
            .map(|(idx, weight)| weighted_point(surface, *idx, *weight / total.max(1.0e-12)))
            .collect()
    };

    Ok(SampleStencil { points })
}

const DEFAULT_BOX_RADIUS_KM: f64 = 25.0;

fn box_radius_deg_for_args(
    args: &Args,
) -> Result<Option<BoxRadiusDeg>, Box<dyn std::error::Error>> {
    if let Some(radius_deg) = args.box_radius_deg {
        if radius_deg < 0.0 || !radius_deg.is_finite() {
            return Err("--box-radius-deg must be a finite non-negative value".into());
        }
        return Ok(Some(BoxRadiusDeg {
            lat: radius_deg,
            lon: radius_deg,
        }));
    }
    if let Some(radius_km) = args.box_radius_km {
        if radius_km < 0.0 || !radius_km.is_finite() {
            return Err("--box-radius-km must be a finite non-negative value".into());
        }
        return Ok(Some(box_radius_deg_from_km(args.lat, radius_km)));
    }
    if matches!(args.sample_method, SoundingSampleMethod::BoxMean) {
        return Ok(Some(box_radius_deg_from_km(
            args.lat,
            DEFAULT_BOX_RADIUS_KM,
        )));
    }
    Ok(None)
}

fn box_radius_deg_from_km(lat: f64, radius_km: f64) -> BoxRadiusDeg {
    let lat_radius = radius_km / 111.0;
    let cos_lat = lat.to_radians().cos().abs().max(0.2);
    BoxRadiusDeg {
        lat: lat_radius,
        lon: lat_radius / cos_lat,
    }
}

fn effective_crop_radius_deg(args: &Args, box_radius: Option<BoxRadiusDeg>) -> f64 {
    let box_radius_deg = box_radius
        .map(|radius| radius.lat.max(radius.lon))
        .unwrap_or(0.0);
    args.crop_radius_deg.max(box_radius_deg + 0.1)
}

fn box_mean_points(
    surface: &SurfaceFields,
    lat: f64,
    lon: f64,
    radius: BoxRadiusDeg,
) -> Vec<WeightedGridPoint> {
    let mut selected = Vec::<usize>::new();
    let mut nearest = Vec::<(usize, f64)>::new();
    for idx in 0..surface.lat.len() {
        let dlat = (surface.lat[idx] - lat).abs();
        let dlon = normalized_longitude_delta(surface.lon[idx] - lon).abs();
        if dlat <= radius.lat && dlon <= radius.lon {
            selected.push(idx);
        }
        insert_nearest(
            &mut nearest,
            1,
            idx,
            geographic_distance_score(surface, idx, lat, lon),
        );
    }

    if selected.is_empty() {
        return nearest
            .first()
            .map(|(idx, _)| vec![weighted_point(surface, *idx, 1.0)])
            .unwrap_or_default();
    }

    selected.sort_unstable();
    let weight = 1.0 / selected.len() as f64;
    selected
        .into_iter()
        .map(|idx| weighted_point(surface, idx, weight))
        .collect()
}

fn insert_nearest(nearest: &mut Vec<(usize, f64)>, keep: usize, idx: usize, distance: f64) {
    let insert_at = nearest
        .iter()
        .position(|&(other_idx, other_distance)| {
            distance < other_distance || (distance == other_distance && idx < other_idx)
        })
        .unwrap_or(nearest.len());
    if insert_at >= keep {
        return;
    }
    nearest.insert(insert_at, (idx, distance));
    nearest.truncate(keep);
}

fn weighted_point(surface: &SurfaceFields, idx: usize, weight: f64) -> WeightedGridPoint {
    WeightedGridPoint {
        index: idx,
        i: idx % surface.nx,
        j: idx / surface.nx,
        lat: surface.lat[idx],
        lon: surface.lon[idx],
        weight,
    }
}

fn nearest_point(
    surface: &SurfaceFields,
    lat: f64,
    lon: f64,
) -> Result<Option<GridPointSummary>, Box<dyn std::error::Error>> {
    let mut best = None::<(usize, f64)>;
    for idx in 0..surface.lat.len() {
        let score = geographic_distance_score(surface, idx, lat, lon);
        if best
            .map(|(best_idx, best_score)| {
                score < best_score || (score == best_score && idx < best_idx)
            })
            .unwrap_or(true)
        {
            best = Some((idx, score));
        }
    }
    Ok(best.map(|(idx, score)| GridPointSummary {
        index: idx,
        i: idx % surface.nx,
        j: idx / surface.nx,
        lat: surface.lat[idx],
        lon: surface.lon[idx],
        distance_deg: score.sqrt(),
    }))
}

fn geographic_distance_score(surface: &SurfaceFields, idx: usize, lat: f64, lon: f64) -> f64 {
    let cos_lat = lat.to_radians().cos().abs().max(0.2);
    let dlat = surface.lat[idx] - lat;
    let dlon = normalized_longitude_delta(surface.lon[idx] - lon) * cos_lat;
    dlat * dlat + dlon * dlon
}

fn normalized_longitude_delta(delta_deg: f64) -> f64 {
    let mut delta = delta_deg;
    while delta <= -180.0 {
        delta += 360.0;
    }
    while delta > 180.0 {
        delta -= 360.0;
    }
    delta
}

fn sample_2d(values: &[f64], stencil: &SampleStencil) -> f64 {
    sample_weighted(values, 0, stencil)
}

fn sample_level(values: &[f64], level_offset: usize, stencil: &SampleStencil) -> f64 {
    sample_weighted(values, level_offset, stencil)
}

fn sample_weighted(values: &[f64], level_offset: usize, stencil: &SampleStencil) -> f64 {
    let mut weighted_sum = 0.0;
    let mut weight_sum = 0.0;
    for point in &stencil.points {
        let value = values[level_offset + point.index];
        if value.is_finite() {
            weighted_sum += value * point.weight;
            weight_sum += point.weight;
        }
    }
    if weight_sum <= 0.0 {
        f64::NAN
    } else {
        weighted_sum / weight_sum
    }
}

fn weighted_mean(values: impl Iterator<Item = (f64, f64)>) -> f64 {
    let mut weighted_sum = 0.0;
    let mut weight_sum = 0.0;
    for (value, weight) in values {
        if value.is_finite() && weight.is_finite() {
            weighted_sum += value * weight;
            weight_sum += weight;
        }
    }
    if weight_sum > 0.0 {
        weighted_sum / weight_sum
    } else {
        f64::NAN
    }
}

fn dewpoint_c_from_q(q_kgkg: f64, pressure_pa: f64, temperature_k: f64) -> f64 {
    let q = q_kgkg.max(1.0e-10);
    let p_hpa = pressure_pa / 100.0;
    let e = (q * p_hpa / (0.622 + q)).max(1.0e-10);
    let ln_e = (e / 6.112).ln();
    let td_c = (243.5 * ln_e) / (17.67 - ln_e);
    td_c.min(temperature_k - 273.15)
}

fn default_artifact_stem(args: &Args) -> String {
    format!(
        "rustwx_{}_{}_{:02}z_f{:03}_{:.3}_{:.3}_sounding",
        sanitize_component(&args.model.to_string()),
        args.date,
        args.cycle,
        args.forecast_hour,
        args.lat,
        args.lon
    )
}

fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn ensure_parent(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        ensure_dir(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_surface() -> SurfaceFields {
        SurfaceFields {
            lat: vec![35.0],
            lon: vec![-97.0],
            nx: 1,
            ny: 1,
            projection: None,
            psfc_pa: vec![96_490.0],
            orog_m: vec![367.5],
            orog_is_proxy: false,
            t2_k: vec![295.0],
            q2_kgkg: vec![0.012],
            u10_ms: vec![6.0],
            v10_ms: vec![2.0],
            native_sbcape_jkg: None,
            native_mlcape_jkg: None,
            native_mucape_jkg: None,
        }
    }

    fn sample_pressure() -> PressureFields {
        PressureFields {
            pressure_levels_hpa: vec![1013.2, 1000.0, 950.0, 900.0],
            pressure_3d_pa: None,
            temperature_c_3d: vec![24.0, 23.0, 20.0, 16.0],
            qvapor_kgkg_3d: vec![0.013, 0.012, 0.011, 0.009],
            u_ms_3d: vec![8.0, 9.0, 12.0, 15.0],
            v_ms_3d: vec![1.0, 2.0, 3.0, 4.0],
            gh_m_3d: vec![-60.0, 100.0, 520.0, 960.0],
        }
    }

    #[test]
    fn column_builder_filters_pressure_levels_below_the_sampled_surface() {
        let surface = sample_surface();
        let pressure = sample_pressure();
        let stencil = SampleStencil {
            points: vec![WeightedGridPoint {
                index: 0,
                i: 0,
                j: 0,
                lat: 35.0,
                lon: -97.0,
                weight: 1.0,
            }],
        };

        let metadata = SoundingMetadata {
            station_id: "TEST".to_string(),
            valid_time: "20260424 22Z F001".to_string(),
            latitude_deg: Some(35.0),
            longitude_deg: Some(-97.0),
            elevation_m: None,
            sample_method: Some("nearest".to_string()),
            box_radius_lat_deg: None,
            box_radius_lon_deg: None,
        };
        let (column, sampled) =
            build_sounding_column(&surface, &pressure, &stencil, metadata).unwrap();

        assert_eq!(sampled.surface_pressure_hpa, 964.9);
        assert_eq!(column.pressure_hpa, vec![964.9, 950.0, 900.0]);
        assert_eq!(column.height_m_msl[0], 367.5);
        assert!(column.height_m_msl[1] > column.height_m_msl[0]);
    }

    #[test]
    fn box_mean_stencil_uses_every_grid_point_inside_radius() {
        let mut surface = sample_surface();
        surface.lat = vec![35.0, 35.2, 34.8, 36.0];
        surface.lon = vec![-97.0, -96.8, -97.2, -98.0];
        surface.nx = 2;
        surface.ny = 2;
        surface.psfc_pa = vec![96_000.0; 4];
        surface.orog_m = vec![300.0; 4];
        surface.t2_k = vec![290.0; 4];
        surface.q2_kgkg = vec![0.01; 4];
        surface.u10_ms = vec![1.0; 4];
        surface.v10_ms = vec![1.0; 4];

        let stencil = sample_stencil(
            &surface,
            35.0,
            -97.0,
            SoundingSampleMethod::BoxMean,
            Some(BoxRadiusDeg {
                lat: 0.25,
                lon: 0.25,
            }),
        )
        .unwrap();

        assert_eq!(stencil.points.len(), 3);
        assert!(
            stencil
                .points
                .iter()
                .all(|point| (point.weight - 1.0 / 3.0).abs() < 1.0e-12)
        );
        assert!((sample_2d(&[3.0, 6.0, 9.0, 100.0], &stencil) - 6.0).abs() < 1.0e-12);
    }
}
