use anyhow::{Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use rustwx_products::wxstore_profile::{WxProfileBoxSummary, WxProfileStore, WxSurfacePoint};
use rustwx_sounding::{SoundingColumn, SoundingMetadata, write_full_sounding_png};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "wxprofile-sounding-render",
    about = "Render a sounding PNG from a native WxStore .wxp profile store"
)]
struct Args {
    #[arg(long)]
    store: PathBuf,
    #[arg(long, default_value = "proof/wxprofile_soundings")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 0)]
    hour: u16,
    #[arg(long, allow_hyphen_values = true)]
    lat: f64,
    #[arg(long, allow_hyphen_values = true)]
    lon: f64,
    #[arg(long, value_enum, default_value_t = SoundingSampleMethod::Nearest)]
    sample_method: SoundingSampleMethod,
    #[arg(long, default_value_t = 0.1)]
    box_radius_lat_deg: f64,
    #[arg(long, default_value_t = 0.1)]
    box_radius_lon_deg: f64,
    #[arg(long)]
    station_id: Option<String>,
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
    BoxMean,
}

#[derive(Debug, Serialize)]
struct WxProfileSoundingReport {
    renderer: &'static str,
    request: SoundingRequest,
    store: StoreSummary,
    sampled_point: SampledPointSummary,
    profile: SoundingProfileSummary,
    output: SoundingOutput,
    timing: SoundingTiming,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    column: Option<SoundingColumn>,
}

#[derive(Debug, Serialize)]
struct SoundingRequest {
    store: String,
    hour: u16,
    requested_lat: f64,
    requested_lon: f64,
    sample_method: SoundingSampleMethod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    box_radius_lat_deg: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    box_radius_lon_deg: Option<f64>,
}

#[derive(Debug, Serialize)]
struct StoreSummary {
    model: String,
    domain: String,
    cycle: String,
    forecast_hours: Vec<u16>,
    levels_hpa: Vec<u16>,
    variables: Vec<String>,
    grid_cells: usize,
}

#[derive(Debug, Serialize)]
struct SampledPointSummary {
    lat: f64,
    lon: f64,
    grid_x: usize,
    grid_y: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sample_point_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    box_summary: Option<WxProfileBoxSummary>,
    surface_pressure_hpa: f64,
    surface_height_m_msl: f64,
    pressure_level_count: usize,
    pressure_levels_used: Vec<f64>,
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
    temperature_bottom_c: f64,
    dewpoint_bottom_c: f64,
}

#[derive(Debug, Serialize)]
struct SoundingOutput {
    png: String,
    manifest: String,
}

#[derive(Debug, Serialize)]
struct SoundingTiming {
    open_ms: u128,
    sample_ms: u128,
    build_column_ms: u128,
    render_ms: u128,
    total_ms: u128,
}

#[derive(Debug, Clone, Copy)]
struct PressureLevel {
    pressure_hpa: f64,
    height_m_msl: f64,
    temperature_c: f64,
    dewpoint_c: f64,
    u_ms: f64,
    v_ms: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.lat.is_finite() || !args.lon.is_finite() {
        bail!("lat/lon must be finite");
    }
    fs::create_dir_all(&args.out_dir)?;
    let total_start = Instant::now();

    let open_start = Instant::now();
    let store = WxProfileStore::open(&args.store).map_err(|err| anyhow!(err.to_string()))?;
    let open_ms = open_start.elapsed().as_millis();

    let sample_start = Instant::now();
    let point = store
        .locate_nearest_grid_point(args.lat, args.lon)
        .map_err(|err| anyhow!(err.to_string()))?;
    let mut box_summary = None;
    let (surface, profile) = match args.sample_method {
        SoundingSampleMethod::Nearest => {
            let surface = store
                .read_surface_point(args.hour, &point)
                .map_err(|err| anyhow!(err.to_string()))?;
            let profile = ["TMP", "SPFH", "UGRD", "VGRD", "HGT"]
                .into_iter()
                .map(|variable| {
                    store
                        .read_variable_point(variable, args.hour, &point)
                        .map(|values| (variable.to_string(), values))
                        .map_err(|err| anyhow!(err.to_string()))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            (surface, profile)
        }
        SoundingSampleMethod::BoxMean => {
            let sample = store
                .read_box_mean(
                    args.hour,
                    args.lat,
                    args.lon,
                    args.box_radius_lat_deg,
                    args.box_radius_lon_deg,
                    &["TMP", "SPFH", "UGRD", "VGRD", "HGT"],
                )
                .map_err(|err| anyhow!(err.to_string()))?;
            box_summary = Some(sample.summary.clone());
            (sample.surface, sample.variables.into_iter().collect())
        }
    };
    let sample_ms = sample_start.elapsed().as_millis();

    let build_start = Instant::now();
    let (mut column, mut sampled_point) =
        build_sounding_column(&args, &store, &point, surface, profile)?;
    if let Some(summary) = box_summary {
        column.metadata.latitude_deg = Some(summary.mean_lat);
        column.metadata.longitude_deg = Some(summary.mean_lon);
        column.metadata.sample_method = Some("wxprofile_box_mean".to_string());
        column.metadata.box_radius_lat_deg = Some(summary.radius_lat_deg);
        column.metadata.box_radius_lon_deg = Some(summary.radius_lon_deg);
        sampled_point.lat = summary.mean_lat;
        sampled_point.lon = summary.mean_lon;
        sampled_point.sample_point_count = Some(summary.point_count);
        sampled_point.box_summary = Some(summary);
    }
    let build_column_ms = build_start.elapsed().as_millis();

    let output_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.out_dir.join(default_artifact_stem(&args)));
    let manifest_path = args.manifest.clone().unwrap_or_else(|| {
        args.out_dir
            .join(default_artifact_stem(&args).replace(".png", "_manifest.json"))
    });
    ensure_parent(&output_path)?;
    ensure_parent(&manifest_path)?;

    let render_start = Instant::now();
    write_full_sounding_png(&column, &output_path)?;
    let render_ms = render_start.elapsed().as_millis();

    let summary = profile_summary(&column);
    let report = WxProfileSoundingReport {
        renderer: "rustwx wxprofile sounding renderer",
        request: SoundingRequest {
            store: args.store.display().to_string(),
            hour: args.hour,
            requested_lat: args.lat,
            requested_lon: normalize_lon(args.lon),
            sample_method: args.sample_method,
            box_radius_lat_deg: (args.sample_method == SoundingSampleMethod::BoxMean)
                .then_some(args.box_radius_lat_deg),
            box_radius_lon_deg: (args.sample_method == SoundingSampleMethod::BoxMean)
                .then_some(args.box_radius_lon_deg),
        },
        store: StoreSummary {
            model: store.manifest().model.clone(),
            domain: store.manifest().domain.clone(),
            cycle: store.manifest().cycle.clone(),
            forecast_hours: store.manifest().forecast_hours.clone(),
            levels_hpa: store.manifest().levels_hpa.clone(),
            variables: store.variable_names(),
            grid_cells: store.manifest().nx * store.manifest().ny,
        },
        sampled_point,
        profile: summary,
        output: SoundingOutput {
            png: output_path.display().to_string(),
            manifest: manifest_path.display().to_string(),
        },
        timing: SoundingTiming {
            open_ms,
            sample_ms,
            build_column_ms,
            render_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
        column: args.include_column.then_some(column),
    };
    let json = serde_json::to_string_pretty(&report)?;
    fs::write(&manifest_path, json.as_bytes())?;
    println!("{json}");
    Ok(())
}

fn build_sounding_column(
    args: &Args,
    store: &WxProfileStore,
    point: &rustwx_products::wxstore_profile::WxProfileGridPoint,
    surface: WxSurfacePoint,
    values: BTreeMap<String, Vec<Option<f64>>>,
) -> Result<(SoundingColumn, SampledPointSummary)> {
    let mut levels = Vec::with_capacity(store.manifest().levels_hpa.len() + 1);
    levels.push(PressureLevel {
        pressure_hpa: surface.psfc_hpa,
        height_m_msl: surface.orog_m,
        temperature_c: surface.t2_c,
        dewpoint_c: dewpoint_c_from_q(surface.q2_kgkg, surface.psfc_hpa * 100.0, surface.t2_c),
        u_ms: surface.u10_ms,
        v_ms: surface.v10_ms,
    });
    for (level_index, level_hpa) in store.manifest().levels_hpa.iter().enumerate() {
        let pressure_hpa = f64::from(*level_hpa);
        let temperature_c = required_value(&values, "TMP", level_index)?;
        let q_kgkg = required_value(&values, "SPFH", level_index)?;
        let u_ms = required_value(&values, "UGRD", level_index)?;
        let v_ms = required_value(&values, "VGRD", level_index)?;
        let height_m_msl = required_value(&values, "HGT", level_index)?;
        if pressure_hpa >= surface.psfc_hpa - 0.1 || height_m_msl <= surface.orog_m + 1.0 {
            continue;
        }
        levels.push(PressureLevel {
            pressure_hpa,
            height_m_msl,
            temperature_c,
            dewpoint_c: dewpoint_c_from_q(q_kgkg, pressure_hpa * 100.0, temperature_c),
            u_ms,
            v_ms,
        });
    }
    levels.retain(|level| {
        level.pressure_hpa.is_finite()
            && level.height_m_msl.is_finite()
            && level.temperature_c.is_finite()
            && level.dewpoint_c.is_finite()
            && level.u_ms.is_finite()
            && level.v_ms.is_finite()
    });
    levels.sort_by(|a, b| {
        b.pressure_hpa
            .partial_cmp(&a.pressure_hpa)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let metadata = SoundingMetadata {
        station_id: args.station_id.clone().unwrap_or_else(|| {
            format!(
                "{} {:.2},{:.2}",
                store.manifest().model,
                args.lat,
                normalize_lon(args.lon)
            )
        }),
        valid_time: format!("{} F{:03}", store.manifest().cycle, args.hour),
        latitude_deg: Some(args.lat),
        longitude_deg: Some(normalize_lon(args.lon)),
        elevation_m: Some(surface.orog_m),
        sample_method: Some("wxprofile_nearest".to_string()),
        box_radius_lat_deg: None,
        box_radius_lon_deg: None,
    };
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
    for level in &levels {
        push_sounding_level(&mut column, *level);
    }
    column.validate()?;
    let used_levels = column.pressure_hpa.clone();
    Ok((
        column,
        SampledPointSummary {
            lat: point.lat,
            lon: normalize_lon(point.lon),
            grid_x: point.x,
            grid_y: point.y,
            sample_point_count: Some(1),
            box_summary: None,
            surface_pressure_hpa: surface.psfc_hpa,
            surface_height_m_msl: surface.orog_m,
            pressure_level_count: used_levels.len(),
            pressure_levels_used: used_levels,
        },
    ))
}

fn required_value(
    values: &BTreeMap<String, Vec<Option<f64>>>,
    variable: &str,
    level_index: usize,
) -> Result<f64> {
    values
        .get(variable)
        .and_then(|series| series.get(level_index))
        .and_then(|value| *value)
        .ok_or_else(|| anyhow!("missing {variable} at level index {level_index}"))
}

fn push_sounding_level(column: &mut SoundingColumn, level: PressureLevel) {
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

fn profile_summary(column: &SoundingColumn) -> SoundingProfileSummary {
    SoundingProfileSummary {
        station_id: column.metadata.station_id.clone(),
        valid_time: column.metadata.valid_time.clone(),
        levels: column.len(),
        pressure_bottom_hpa: *column.pressure_hpa.first().unwrap_or(&f64::NAN),
        pressure_top_hpa: *column.pressure_hpa.last().unwrap_or(&f64::NAN),
        height_bottom_m_msl: *column.height_m_msl.first().unwrap_or(&f64::NAN),
        height_top_m_msl: *column.height_m_msl.last().unwrap_or(&f64::NAN),
        temperature_bottom_c: *column.temperature_c.first().unwrap_or(&f64::NAN),
        dewpoint_bottom_c: *column.dewpoint_c.first().unwrap_or(&f64::NAN),
    }
}

fn dewpoint_c_from_q(q_kgkg: f64, pressure_pa: f64, temperature_c: f64) -> f64 {
    let q = q_kgkg.max(1.0e-10);
    let p_hpa = pressure_pa / 100.0;
    let e = (q * p_hpa / (0.622 + q)).max(1.0e-10);
    let ln_e = (e / 6.112).ln();
    let td_c = (243.5 * ln_e) / (17.67 - ln_e);
    td_c.min(temperature_c)
}

fn normalize_lon(lon: f64) -> f64 {
    ((lon + 180.0).rem_euclid(360.0)) - 180.0
}

fn default_artifact_stem(args: &Args) -> String {
    format!(
        "wxprofile_f{:03}_{:.3}_{:.3}_sounding.png",
        args.hour,
        args.lat,
        normalize_lon(args.lon)
    )
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}
