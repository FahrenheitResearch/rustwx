use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use rustwx_products::volume_store::{BoxProfile, PointProfile, SurfaceTerrainStore, VolumeStore};
use rustwx_sounding::{SoundingColumn, SoundingMetadata, write_full_sounding_png};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Parser)]
#[command(
    name = "volume-store-sounding-render",
    about = "Render a rustwx sounding PNG directly from a pressure VolumeStore"
)]
struct Args {
    #[arg(long)]
    store: PathBuf,
    #[arg(long, default_value = "proof/volume_store_soundings")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 0)]
    hour: u16,
    #[arg(long, allow_hyphen_values = true)]
    lat: f64,
    #[arg(long, allow_hyphen_values = true)]
    lon: f64,
    #[arg(long, value_enum, default_value_t = SoundingSampleMethod::Nearest)]
    sample_method: SoundingSampleMethod,
    #[arg(long, default_value_t = 0.0)]
    box_radius_lat_deg: f64,
    #[arg(long, default_value_t = 0.0)]
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
struct VolumeStoreSoundingReport {
    schema_version: u32,
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
    box_radius_lat_deg: Option<f64>,
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
    grid_x: f32,
    grid_y: f32,
    sample_method: SoundingSampleMethod,
    box_min_lat_deg: Option<f64>,
    box_max_lat_deg: Option<f64>,
    box_min_lon_deg: Option<f64>,
    box_max_lon_deg: Option<f64>,
    box_grid_x0: Option<usize>,
    box_grid_x1: Option<usize>,
    box_grid_y0: Option<usize>,
    box_grid_y1: Option<usize>,
    box_cell_count: Option<usize>,
    surface_pressure_hpa: Option<f64>,
    surface_height_m_msl: Option<f64>,
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
    omega_pa_s: Option<f64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.lat.is_finite() || !args.lon.is_finite() {
        bail!("lat/lon must be finite");
    }
    if !(-90.0..=90.0).contains(&args.lat) {
        bail!("latitude {} is outside [-90, 90]", args.lat);
    }
    let total_start = Instant::now();
    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create {}", args.out_dir.display()))?;

    let open_start = Instant::now();
    let store = VolumeStore::open(&args.store).map_err(|err| anyhow!(err.to_string()))?;
    let terrain =
        SurfaceTerrainStore::open_optional(&args.store).map_err(|err| anyhow!(err.to_string()))?;
    let open_ms = open_start.elapsed().as_millis();

    let available_hours = store
        .manifest()
        .forecast_hours
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if !available_hours.contains(&args.hour) {
        bail!(
            "requested f{:03}, but store supports {}",
            args.hour,
            store
                .manifest()
                .forecast_hours
                .iter()
                .map(|hour| format!("f{hour:03}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let sample_start = Instant::now();
    let variables = available_profile_variables(&store)?;
    let variable_refs = variables.iter().map(String::as_str).collect::<Vec<_>>();
    let levels = store.manifest().levels_hpa.clone();
    let (profile, box_profile) = match args.sample_method {
        SoundingSampleMethod::Nearest => (
            store
                .sample_point_3d(args.lat, args.lon, &variable_refs, &[args.hour], &levels)
                .map_err(|err| anyhow!(err.to_string()))?,
            None,
        ),
        SoundingSampleMethod::BoxMean => {
            if args.box_radius_lat_deg <= 0.0 || args.box_radius_lon_deg <= 0.0 {
                bail!(
                    "box-mean sampling requires positive --box-radius-lat-deg and --box-radius-lon-deg"
                );
            }
            let box_profile = store
                .sample_box_3d(
                    args.lat,
                    args.lon,
                    args.box_radius_lat_deg,
                    args.box_radius_lon_deg,
                    &variable_refs,
                    &[args.hour],
                    &levels,
                )
                .map_err(|err| anyhow!(err.to_string()))?;
            let profile = PointProfile {
                lat_deg: box_profile.center_lat_deg,
                lon_deg: box_profile.center_lon_deg,
                samples: box_profile.samples.clone(),
            };
            (profile, Some(box_profile))
        }
    };
    let (grid_x, grid_y) = store
        .manifest()
        .grid
        .grid_xy(args.lat, args.lon)
        .map_err(|err| anyhow!(err.to_string()))?;
    let terrain_point = match (&terrain, &box_profile) {
        (Some(terrain), Some(profile)) => Some(
            terrain
                .sample_grid_box(
                    args.hour,
                    profile.x0,
                    profile.x1,
                    profile.y0,
                    profile.y1,
                    store.manifest().grid.nx(),
                    store.manifest().grid.ny(),
                )
                .map_err(|err| anyhow!(err.to_string()))?,
        ),
        (Some(terrain), None) => Some(
            terrain
                .sample_grid_point(
                    args.hour,
                    grid_x,
                    grid_y,
                    store.manifest().grid.nx(),
                    store.manifest().grid.ny(),
                )
                .map_err(|err| anyhow!(err.to_string()))?,
        ),
        (None, _) => None,
    };
    let sample_ms = sample_start.elapsed().as_millis();

    let build_start = Instant::now();
    let (column, sampled_point) =
        build_sounding_column(&args, &store, &profile, terrain_point, box_profile.as_ref())?;
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
    let report = VolumeStoreSoundingReport {
        schema_version: SCHEMA_VERSION,
        renderer: "rustwx volume-store sounding renderer",
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
            variables: store
                .manifest()
                .variables
                .iter()
                .map(|variable| variable.name.clone())
                .collect(),
            grid_cells: store.manifest().grid.grid_len(),
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

fn available_profile_variables(store: &VolumeStore) -> Result<Vec<String>> {
    let available = store
        .manifest()
        .variables
        .iter()
        .map(|variable| variable.name.as_str())
        .collect::<BTreeSet<_>>();
    let required = ["TMP", "SPFH", "UGRD", "VGRD", "HGT"];
    let missing = required
        .iter()
        .copied()
        .filter(|name| !available.contains(name))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "pressure VolumeStore is missing required sounding variables: {}",
            missing.join(", ")
        );
    }
    let mut variables = required
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    if available.contains("VVEL") {
        variables.push("VVEL".to_string());
    }
    Ok(variables)
}

fn build_sounding_column(
    args: &Args,
    store: &VolumeStore,
    profile: &PointProfile,
    terrain_point: Option<rustwx_products::volume_store::SurfaceTerrainPoint>,
    box_profile: Option<&BoxProfile>,
) -> Result<(SoundingColumn, SampledPointSummary)> {
    let values = profile
        .samples
        .iter()
        .map(|sample| {
            (
                (sample.level_hpa, sample.variable.as_str()),
                f64::from(sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut levels = Vec::new();
    for &level_hpa in &store.manifest().levels_hpa {
        let pressure_hpa = f64::from(level_hpa);
        let temperature_c = required_value(&values, level_hpa, "TMP")?;
        let q_kgkg = required_value(&values, level_hpa, "SPFH")?;
        let u_ms = required_value(&values, level_hpa, "UGRD")?;
        let v_ms = required_value(&values, level_hpa, "VGRD")?;
        let height_m_msl = required_value(&values, level_hpa, "HGT")?;
        let omega_pa_s = values.get(&(level_hpa, "VVEL")).copied();
        let dewpoint_c = dewpoint_c_from_q(q_kgkg, pressure_hpa * 100.0, temperature_c + 273.15);
        levels.push(PressureLevel {
            pressure_hpa,
            height_m_msl,
            temperature_c,
            dewpoint_c,
            u_ms,
            v_ms,
            omega_pa_s,
        });
    }

    let surface_pressure_hpa = terrain_point
        .as_ref()
        .map(|point| point.surface_pressure_hpa)
        .filter(|value| value.is_finite() && *value > 0.0);
    let surface_height_m_msl = terrain_point
        .as_ref()
        .map(|point| point.surface_height_m_msl)
        .filter(|value| value.is_finite());

    levels.retain(|level| {
        if !level.pressure_hpa.is_finite()
            || !level.height_m_msl.is_finite()
            || !level.temperature_c.is_finite()
            || !level.dewpoint_c.is_finite()
            || !level.u_ms.is_finite()
            || !level.v_ms.is_finite()
        {
            return false;
        }
        if let Some(psfc) = surface_pressure_hpa {
            if level.pressure_hpa >= psfc - 0.1 {
                return false;
            }
        }
        if let Some(orog) = surface_height_m_msl {
            if level.height_m_msl <= orog + 1.0 {
                return false;
            }
        }
        true
    });
    levels.sort_by(|a, b| {
        b.pressure_hpa
            .partial_cmp(&a.pressure_hpa)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let metadata = SoundingMetadata {
        station_id: args.station_id.clone().unwrap_or_else(|| {
            let prefix = if args.sample_method == SoundingSampleMethod::BoxMean {
                "box mean"
            } else {
                store.manifest().model.as_str()
            };
            format!("{prefix} {:.2},{:.2}", args.lat, normalize_lon(args.lon))
        }),
        valid_time: format!("{} F{:03}", store.manifest().cycle, args.hour),
        latitude_deg: Some(args.lat),
        longitude_deg: Some(normalize_lon(args.lon)),
        elevation_m: surface_height_m_msl,
        sample_method: Some(match args.sample_method {
            SoundingSampleMethod::Nearest => "volume_store_nearest".to_string(),
            SoundingSampleMethod::BoxMean => "box_mean".to_string(),
        }),
        box_radius_lat_deg: box_profile.map(|_| args.box_radius_lat_deg),
        box_radius_lon_deg: box_profile.map(|_| args.box_radius_lon_deg),
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

    let include_omega = levels
        .iter()
        .all(|level| level.omega_pa_s.is_some_and(|omega| omega.is_finite()));
    for level in &levels {
        push_sounding_level(&mut column, *level, include_omega);
    }
    column.validate()?;

    let used_levels = column.pressure_hpa.clone();
    let sampled = SampledPointSummary {
        lat: profile.lat_deg,
        lon: normalize_lon(profile.lon_deg),
        grid_x: store
            .manifest()
            .grid
            .grid_xy(args.lat, args.lon)
            .map_err(|err| anyhow!(err.to_string()))?
            .0,
        grid_y: store
            .manifest()
            .grid
            .grid_xy(args.lat, args.lon)
            .map_err(|err| anyhow!(err.to_string()))?
            .1,
        sample_method: args.sample_method,
        box_min_lat_deg: box_profile.map(|profile| profile.min_lat_deg),
        box_max_lat_deg: box_profile.map(|profile| profile.max_lat_deg),
        box_min_lon_deg: box_profile.map(|profile| normalize_lon(profile.min_lon_deg)),
        box_max_lon_deg: box_profile.map(|profile| normalize_lon(profile.max_lon_deg)),
        box_grid_x0: box_profile.map(|profile| profile.x0),
        box_grid_x1: box_profile.map(|profile| profile.x1),
        box_grid_y0: box_profile.map(|profile| profile.y0),
        box_grid_y1: box_profile.map(|profile| profile.y1),
        box_cell_count: box_profile.map(|profile| profile.cell_count),
        surface_pressure_hpa,
        surface_height_m_msl,
        pressure_level_count: column.len(),
        pressure_levels_used: used_levels,
    };

    Ok((column, sampled))
}

fn required_value(
    values: &BTreeMap<(u16, &str), f64>,
    level_hpa: u16,
    var: &'static str,
) -> Result<f64> {
    values
        .get(&(level_hpa, var))
        .copied()
        .ok_or_else(|| anyhow!("missing {var} at {level_hpa} hPa in sampled profile"))
}

fn push_sounding_level(column: &mut SoundingColumn, level: PressureLevel, include_omega: bool) {
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
    if include_omega {
        column.omega_pa_s.push(level.omega_pa_s.unwrap_or(f64::NAN));
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
        temperature_bottom_c: *column.temperature_c.first().unwrap_or(&f64::NAN),
        dewpoint_bottom_c: *column.dewpoint_c.first().unwrap_or(&f64::NAN),
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

fn normalize_lon(lon: f64) -> f64 {
    if !lon.is_finite() {
        return lon;
    }
    ((lon + 180.0).rem_euclid(360.0)) - 180.0
}

fn default_artifact_stem(args: &Args) -> String {
    format!(
        "volume_store_f{:03}_{:.3}_{:.3}_sounding.png",
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
