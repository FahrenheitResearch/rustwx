use anyhow::{Result, anyhow, bail};
use clap::Parser;
use rustwx_products::wxstore_profile::{WxProfileStore, WxSurfacePoint};
use rustwx_sounding::{NativeSounding, SoundingColumn, SoundingMetadata, VerifiedEcapeParcels};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "wxprofile-ecape-probe",
    about = "Compute verified sounding ECAPE numbers from a native WxProfile store column"
)]
struct Args {
    #[arg(long)]
    store: PathBuf,
    #[arg(long, default_value_t = 0)]
    hour: u16,
    #[arg(long, allow_hyphen_values = true)]
    lat: f64,
    #[arg(long, allow_hyphen_values = true)]
    lon: f64,
    #[arg(long, default_value_t = false)]
    include_column: bool,
}

#[derive(Debug, Serialize)]
struct ProbeReport {
    request: ProbeRequest,
    store: StoreSummary,
    sampled_point: SampledPointSummary,
    profile: ProfileSummary,
    verified_ecape: VerifiedEcapeParcels,
    timing: Timing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    column: Option<SoundingColumn>,
}

#[derive(Debug, Serialize)]
struct ProbeRequest {
    store: String,
    hour: u16,
    requested_lat: f64,
    requested_lon: f64,
}

#[derive(Debug, Serialize)]
struct StoreSummary {
    model: String,
    domain: String,
    cycle: String,
    forecast_hours: Vec<u16>,
    levels_hpa: Vec<u16>,
    variables: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SampledPointSummary {
    lat: f64,
    lon: f64,
    grid_x: usize,
    grid_y: usize,
    surface_pressure_hpa: f64,
    surface_height_m_msl: f64,
}

#[derive(Debug, Serialize)]
struct ProfileSummary {
    levels: usize,
    pressure_bottom_hpa: f64,
    pressure_top_hpa: f64,
    height_bottom_m_msl: f64,
    height_top_m_msl: f64,
    temperature_bottom_c: f64,
    dewpoint_bottom_c: f64,
}

#[derive(Debug, Serialize)]
struct Timing {
    open_ms: u128,
    sample_ms: u128,
    build_column_ms: u128,
    ecape_ms: u128,
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

    let total_start = Instant::now();
    let open_start = Instant::now();
    let store = WxProfileStore::open(&args.store).map_err(|err| anyhow!(err.to_string()))?;
    let open_ms = open_start.elapsed().as_millis();

    let sample_start = Instant::now();
    let point = store
        .locate_nearest_grid_point(args.lat, args.lon)
        .map_err(|err| anyhow!(err.to_string()))?;
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
    let sample_ms = sample_start.elapsed().as_millis();

    let build_start = Instant::now();
    let column = build_sounding_column(&args, &store, &point, surface, profile)?;
    let build_column_ms = build_start.elapsed().as_millis();

    let ecape_start = Instant::now();
    let native = NativeSounding::from_column(&column)?;
    let ecape_ms = ecape_start.elapsed().as_millis();

    let manifest = store.manifest();
    let report = ProbeReport {
        request: ProbeRequest {
            store: args.store.display().to_string(),
            hour: args.hour,
            requested_lat: args.lat,
            requested_lon: normalize_lon(args.lon),
        },
        store: StoreSummary {
            model: manifest.model.clone(),
            domain: manifest.domain.clone(),
            cycle: manifest.cycle.clone(),
            forecast_hours: manifest.forecast_hours.clone(),
            levels_hpa: manifest.levels_hpa.clone(),
            variables: manifest
                .variables
                .iter()
                .map(|var| var.name.clone())
                .collect(),
        },
        sampled_point: SampledPointSummary {
            lat: point.lat,
            lon: normalize_lon(point.lon),
            grid_x: point.x,
            grid_y: point.y,
            surface_pressure_hpa: *column.pressure_hpa.first().unwrap_or(&f64::NAN),
            surface_height_m_msl: *column.height_m_msl.first().unwrap_or(&f64::NAN),
        },
        profile: profile_summary(&column),
        verified_ecape: native.verified_ecape,
        timing: Timing {
            open_ms,
            sample_ms,
            build_column_ms,
            ecape_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
        column: args.include_column.then_some(column),
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn build_sounding_column(
    args: &Args,
    store: &WxProfileStore,
    point: &rustwx_products::wxstore_profile::WxProfileGridPoint,
    surface: WxSurfacePoint,
    values: BTreeMap<String, Vec<Option<f64>>>,
) -> Result<SoundingColumn> {
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

    let mut column = SoundingColumn {
        pressure_hpa: Vec::with_capacity(levels.len()),
        height_m_msl: Vec::with_capacity(levels.len()),
        temperature_c: Vec::with_capacity(levels.len()),
        dewpoint_c: Vec::with_capacity(levels.len()),
        u_ms: Vec::with_capacity(levels.len()),
        v_ms: Vec::with_capacity(levels.len()),
        omega_pa_s: Vec::new(),
        metadata: SoundingMetadata {
            station_id: format!(
                "{} {:.2},{:.2}",
                store.manifest().model,
                args.lat,
                normalize_lon(args.lon)
            ),
            valid_time: format!("{} F{:03}", store.manifest().cycle, args.hour),
            latitude_deg: Some(args.lat),
            longitude_deg: Some(normalize_lon(args.lon)),
            elevation_m: Some(surface.orog_m),
            sample_method: Some("wxprofile_nearest".to_string()),
            box_radius_lat_deg: None,
            box_radius_lon_deg: None,
        },
    };
    for level in &levels {
        push_sounding_level(&mut column, *level);
    }
    column.validate()?;
    if point.lat.is_nan() {
        bail!("invalid sampled point");
    }
    Ok(column)
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

fn profile_summary(column: &SoundingColumn) -> ProfileSummary {
    ProfileSummary {
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
