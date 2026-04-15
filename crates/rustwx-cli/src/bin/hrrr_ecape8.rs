use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use grib_core::grib2::{
    Grib2File, Grib2Message, flip_rows, grid_latlon, unpack_message_normalized,
};
use image::DynamicImage;
use region::RegionPreset;
use rustwx_calc::{
    EcapeGridInputs, EcapeTripletOptions, GridShape, ScpEhiInputs, VolumeShape, WindGridInputs,
    compute_ecape_triplet_with_failure_mask, compute_scp_ehi, compute_shear, compute_srh,
};
use rustwx_cli::proof_cache::{default_proof_cache_dir, ensure_dir, load_bincode, store_bincode};
use rustwx_core::{CycleSpec, Field2D, LatLonGrid, ModelId, ModelRunRequest, ProductKey, SourceId};
use rustwx_io::{FetchRequest, artifact_cache_dir, fetch_bytes_with_cache};
use rustwx_models::latest_available_run;
use rustwx_render::{
    Color, MapRenderRequest, PanelGridLayout, PanelPadding, ProjectedDomain, ProjectedExtent,
    ProjectedLineOverlay, Solar07Product, render_panel_grid,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use wrf_render::features::load_styled_conus_features;
use wrf_render::projection::LambertConformal;
use wrf_render::render::map_frame_aspect_ratio;
use wrf_render::text;

const SURFACE_PATTERNS: &[&str] = &[
    "PRES:surface",
    "HGT:surface",
    "TMP:2 m above ground",
    "SPFH:2 m above ground",
    "UGRD:10 m above ground",
    "VGRD:10 m above ground",
];

const PRESSURE_PATTERNS: &[&str] = &["HGT:", "TMP:", "SPFH:", "UGRD:", "VGRD:"];

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-ecape8",
    about = "Generate a RustWX HRRR ECAPE 8-panel proof plot"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::Midwest)]
    region: RegionPreset,
    #[arg(long, default_value = "C:\\Users\\drew\\rustwx\\proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

#[derive(Debug, Clone)]
struct Timing {
    fetch_surface_ms: u128,
    fetch_pressure_ms: u128,
    decode_surface_ms: u128,
    decode_pressure_ms: u128,
    project_ms: u128,
    compute_ms: u128,
    render_ms: u128,
    total_ms: u128,
    fetch_surface_cache_hit: bool,
    fetch_pressure_cache_hit: bool,
    decode_surface_cache_hit: bool,
    decode_pressure_cache_hit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SurfaceFields {
    lat: Vec<f64>,
    lon: Vec<f64>,
    nx: usize,
    ny: usize,
    psfc_pa: Vec<f64>,
    orog_m: Vec<f64>,
    t2_k: Vec<f64>,
    q2_kgkg: Vec<f64>,
    u10_ms: Vec<f64>,
    v10_ms: Vec<f64>,
    lambert_latin1: f64,
    lambert_latin2: f64,
    lambert_lov: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PressureFields {
    pressure_levels_hpa: Vec<f64>,
    temperature_c_3d: Vec<f64>,
    qvapor_kgkg_3d: Vec<f64>,
    u_ms_3d: Vec<f64>,
    v_ms_3d: Vec<f64>,
    gh_m_3d: Vec<f64>,
}

#[derive(Debug, Clone)]
struct ProjectedMap {
    projected_x: Vec<f64>,
    projected_y: Vec<f64>,
    extent: ProjectedExtent,
    lines: Vec<ProjectedLineOverlay>,
}

#[derive(Debug, Clone)]
struct FetchedSubset {
    request: FetchRequest,
    fetched: rustwx_io::CachedFetchResult,
    bytes: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let total_start = Instant::now();
    let latest = match args.cycle {
        Some(hour) => rustwx_models::LatestRun {
            model: ModelId::Hrrr,
            cycle: CycleSpec::new(&args.date, hour)?,
            source: args.source,
        },
        None => latest_available_run(ModelId::Hrrr, Some(args.source), &args.date)?,
    };
    let cycle = latest.cycle.hour_utc;

    let fetch_surface_start = Instant::now();
    let surface_subset = fetch_subset(
        latest.cycle.clone(),
        args.forecast_hour,
        args.source,
        "sfc",
        SURFACE_PATTERNS,
        &cache_root,
        !args.no_cache,
    )?;
    let fetch_surface_ms = fetch_surface_start.elapsed().as_millis();

    let fetch_pressure_start = Instant::now();
    let pressure_subset = fetch_subset(
        latest.cycle.clone(),
        args.forecast_hour,
        args.source,
        "prs",
        PRESSURE_PATTERNS,
        &cache_root,
        !args.no_cache,
    )?;
    let fetch_pressure_ms = fetch_pressure_start.elapsed().as_millis();

    let sfc_subset_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_sfc_subset.grib2",
        args.date, cycle, args.forecast_hour
    ));
    let prs_subset_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_prs_subset.grib2",
        args.date, cycle, args.forecast_hour
    ));
    fs::write(&sfc_subset_path, &surface_subset.bytes)?;
    fs::write(&prs_subset_path, &pressure_subset.bytes)?;

    let decode_surface_start = Instant::now();
    let surface_decode_path = artifact_cache_dir(&cache_root, &surface_subset.request)
        .join("decoded")
        .join("surface.bin");
    let (surface, decode_surface_cache_hit) = if args.no_cache {
        (decode_surface(&surface_subset.bytes)?, false)
    } else if let Some(cached) = load_bincode::<SurfaceFields>(&surface_decode_path)? {
        (cached, true)
    } else {
        let decoded = decode_surface(&surface_subset.bytes)?;
        store_bincode(&surface_decode_path, &decoded)?;
        (decoded, false)
    };
    let decode_surface_ms = decode_surface_start.elapsed().as_millis();

    let decode_pressure_start = Instant::now();
    let pressure_decode_path = artifact_cache_dir(&cache_root, &pressure_subset.request)
        .join("decoded")
        .join("pressure.bin");
    let (pressure, decode_pressure_cache_hit) = if args.no_cache {
        (
            decode_pressure(&pressure_subset.bytes, surface.nx, surface.ny)?,
            false,
        )
    } else if let Some(cached) = load_bincode::<PressureFields>(&pressure_decode_path)? {
        (cached, true)
    } else {
        let decoded = decode_pressure(&pressure_subset.bytes, surface.nx, surface.ny)?;
        store_bincode(&pressure_decode_path, &decoded)?;
        (decoded, false)
    };
    let decode_stats_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_decode_stats.json",
        args.date, cycle, args.forecast_hour
    ));
    fs::write(
        &decode_stats_path,
        serde_json::to_vec_pretty(&json!({
            "surface": surface_stats(&surface),
            "pressure": pressure_stats(&pressure, surface.nx, surface.ny),
        }))?,
    )?;
    let decode_pressure_ms = decode_pressure_start.elapsed().as_millis();

    let project_start = Instant::now();
    let projected = build_projected_map(&surface, args.region)?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let (fields, failure_count) = compute_panel_fields(&surface, &pressure)?;
    let compute_ms = compute_start.elapsed().as_millis();

    let render_start = Instant::now();
    let panel_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_ecape8_panel.png",
        args.date,
        cycle,
        args.forecast_hour,
        args.region.slug()
    ));
    render_panel(
        &panel_path,
        &surface,
        &projected,
        &fields,
        &args.date,
        cycle,
        args.forecast_hour,
        failure_count,
    )?;
    let render_ms = render_start.elapsed().as_millis();

    let timing = Timing {
        fetch_surface_ms,
        fetch_pressure_ms,
        decode_surface_ms,
        decode_pressure_ms,
        project_ms,
        compute_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
        fetch_surface_cache_hit: surface_subset.fetched.cache_hit,
        fetch_pressure_cache_hit: pressure_subset.fetched.cache_hit,
        decode_surface_cache_hit,
        decode_pressure_cache_hit,
    };
    let timing_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_ecape8_timing.json",
        args.date,
        cycle,
        args.forecast_hour,
        args.region.slug()
    ));
    fs::write(
        &timing_path,
        serde_json::to_vec_pretty(&json!({
            "date": args.date,
            "cycle_utc": cycle,
            "forecast_hour": args.forecast_hour,
            "region": args.region.slug(),
            "source": args.source.as_str(),
            "failure_count": failure_count,
            "cache": {
                "root": cache_root,
                "surface_fetch_hit": timing.fetch_surface_cache_hit,
                "pressure_fetch_hit": timing.fetch_pressure_cache_hit,
                "surface_decode_hit": timing.decode_surface_cache_hit,
                "pressure_decode_hit": timing.decode_pressure_cache_hit,
                "surface_fetch_bytes_path": surface_subset.fetched.bytes_path,
                "surface_fetch_meta_path": surface_subset.fetched.metadata_path,
                "pressure_fetch_bytes_path": pressure_subset.fetched.bytes_path,
                "pressure_fetch_meta_path": pressure_subset.fetched.metadata_path,
                "surface_decode_path": surface_decode_path,
                "pressure_decode_path": pressure_decode_path,
            },
            "surface_subset_path": sfc_subset_path,
            "pressure_subset_path": prs_subset_path,
            "decode_stats_path": decode_stats_path,
            "panel_path": panel_path,
            "timing_ms": {
                "fetch_surface": timing.fetch_surface_ms,
                "fetch_pressure": timing.fetch_pressure_ms,
                "decode_surface": timing.decode_surface_ms,
                "decode_pressure": timing.decode_pressure_ms,
                "project": timing.project_ms,
                "compute": timing.compute_ms,
                "render": timing.render_ms,
                "total": timing.total_ms,
            }
        }))?,
    )?;

    println!("{}", panel_path.display());
    println!("{}", timing_path.display());
    Ok(())
}

fn surface_stats(surface: &SurfaceFields) -> serde_json::Value {
    json!({
        "grid": { "nx": surface.nx, "ny": surface.ny },
        "lat_range": range_stats(&surface.lat),
        "lon_range": range_stats(&surface.lon),
        "psfc_pa": range_stats(&surface.psfc_pa),
        "orog_m": range_stats(&surface.orog_m),
        "t2_k": range_stats(&surface.t2_k),
        "q2_kgkg": range_stats(&surface.q2_kgkg),
        "u10_ms": range_stats(&surface.u10_ms),
        "v10_ms": range_stats(&surface.v10_ms),
    })
}

fn pressure_stats(pressure: &PressureFields, nx: usize, ny: usize) -> serde_json::Value {
    let n2d = nx * ny;
    let center = (ny / 2) * nx + (nx / 2);
    let sample_profile = |values: &[f64]| -> Vec<f64> {
        (0..pressure.pressure_levels_hpa.len())
            .map(|k| values[k * n2d + center])
            .take(6)
            .collect()
    };
    json!({
        "levels_hpa_first_10": pressure.pressure_levels_hpa.iter().copied().take(10).collect::<Vec<_>>(),
        "temperature_c_3d": range_stats(&pressure.temperature_c_3d),
        "qvapor_kgkg_3d": range_stats(&pressure.qvapor_kgkg_3d),
        "u_ms_3d": range_stats(&pressure.u_ms_3d),
        "v_ms_3d": range_stats(&pressure.v_ms_3d),
        "gh_m_3d": range_stats(&pressure.gh_m_3d),
        "center_profile_temperature_first_6": sample_profile(&pressure.temperature_c_3d),
        "center_profile_qvapor_first_6": sample_profile(&pressure.qvapor_kgkg_3d),
        "center_profile_gh_first_6": sample_profile(&pressure.gh_m_3d),
    })
}

fn range_stats(values: &[f64]) -> serde_json::Value {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for &value in values {
        if value.is_finite() {
            min = min.min(value);
            max = max.max(value);
        }
    }
    json!({ "min": min, "max": max })
}

fn fetch_subset(
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    product: &str,
    patterns: &[&str],
    cache_root: &Path,
    use_cache: bool,
) -> Result<FetchedSubset, Box<dyn std::error::Error>> {
    let request = ModelRunRequest::new(ModelId::Hrrr, cycle, forecast_hour, product)?;
    let fetch_request = FetchRequest {
        request,
        source_override: Some(source),
        variable_patterns: patterns.iter().map(|s| s.to_string()).collect(),
    };
    let fetched = fetch_bytes_with_cache(&fetch_request, cache_root, use_cache)?;
    let bytes = fetched.result.bytes.clone();
    Ok(FetchedSubset {
        request: fetch_request,
        fetched,
        bytes,
    })
}

fn decode_surface(bytes: &[u8]) -> Result<SurfaceFields, Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let sample = file
        .messages
        .first()
        .ok_or("surface subset had no GRIB messages")?;
    let (mut lat, mut lon_raw) = grid_latlon(&sample.grid);
    if sample.grid.scan_mode & 0x40 != 0 {
        flip_rows(&mut lat, sample.grid.nx as usize, sample.grid.ny as usize);
        flip_rows(
            &mut lon_raw,
            sample.grid.nx as usize,
            sample.grid.ny as usize,
        );
    }
    let lon = lon_raw
        .into_iter()
        .map(normalize_longitude)
        .collect::<Vec<_>>();
    let nx = sample.grid.nx as usize;
    let ny = sample.grid.ny as usize;

    let psfc_pa = unpack_message_normalized(find_message(&file.messages, 0, 3, 0, 1, Some(0.0))?)?;
    let orog_m = unpack_message_normalized(find_message(&file.messages, 0, 3, 5, 1, Some(0.0))?)?;
    let t2_k = unpack_message_normalized(find_message(&file.messages, 0, 0, 0, 103, Some(2.0))?)?;
    let q2_specific =
        unpack_message_normalized(find_message(&file.messages, 0, 1, 0, 103, Some(2.0))?)?;
    let u10_ms =
        unpack_message_normalized(find_message(&file.messages, 0, 2, 2, 103, Some(10.0))?)?;
    let v10_ms =
        unpack_message_normalized(find_message(&file.messages, 0, 2, 3, 103, Some(10.0))?)?;

    Ok(SurfaceFields {
        lat,
        lon,
        nx,
        ny,
        psfc_pa,
        orog_m,
        t2_k,
        q2_kgkg: q_to_mixing_ratio(&q2_specific),
        u10_ms,
        v10_ms,
        lambert_latin1: sample.grid.latin1,
        lambert_latin2: sample.grid.latin2,
        lambert_lov: sample.grid.lov,
    })
}

fn decode_pressure(
    bytes: &[u8],
    nx: usize,
    ny: usize,
) -> Result<PressureFields, Box<dyn std::error::Error>> {
    let file = Grib2File::from_bytes(bytes)?;
    let temperature = collect_levels(&file.messages, 0, 0, 0, 100)?;
    let specific_humidity = collect_levels(&file.messages, 0, 1, 0, 100)?;
    let u_wind = collect_levels(&file.messages, 0, 2, 2, 100)?;
    let v_wind = collect_levels(&file.messages, 0, 2, 3, 100)?;
    let gh = collect_levels(&file.messages, 0, 3, 5, 100)?;

    let levels = temperature
        .iter()
        .map(|(level, _)| *level)
        .collect::<Vec<_>>();
    for dataset in [&specific_humidity, &u_wind, &v_wind, &gh] {
        let candidate = dataset.iter().map(|(level, _)| *level).collect::<Vec<_>>();
        if candidate != levels {
            return Err("pressure subset levels did not line up across variables".into());
        }
    }

    let expected = nx * ny;
    let flatten = |records: &Vec<(f64, Vec<f64>)>| -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        let mut out = Vec::with_capacity(records.len() * expected);
        for (_, values) in records {
            if values.len() != expected {
                return Err("decoded pressure field had unexpected grid size".into());
            }
            out.extend_from_slice(values);
        }
        Ok(out)
    };

    Ok(PressureFields {
        pressure_levels_hpa: levels
            .into_iter()
            .map(normalize_pressure_level_hpa)
            .collect(),
        temperature_c_3d: flatten(&temperature)?
            .into_iter()
            .map(|value| value - 273.15)
            .collect(),
        qvapor_kgkg_3d: q_to_mixing_ratio(&flatten(&specific_humidity)?),
        u_ms_3d: flatten(&u_wind)?,
        v_ms_3d: flatten(&v_wind)?,
        gh_m_3d: flatten(&gh)?,
    })
}

fn collect_levels(
    messages: &[Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
) -> Result<Vec<(f64, Vec<f64>)>, Box<dyn std::error::Error>> {
    let mut records = messages
        .iter()
        .filter(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
        })
        .map(|msg| Ok((msg.product.level_value, unpack_message_normalized(msg)?)))
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    records.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    Ok(records)
}

fn find_message<'a>(
    messages: &'a [Grib2Message],
    discipline: u8,
    category: u8,
    number: u8,
    level_type: u8,
    level_value: Option<f64>,
) -> Result<&'a Grib2Message, Box<dyn std::error::Error>> {
    messages
        .iter()
        .find(|msg| {
            msg.discipline == discipline
                && msg.product.parameter_category == category
                && msg.product.parameter_number == number
                && msg.product.level_type == level_type
                && level_value
                    .map(|level| (msg.product.level_value - level).abs() < 0.25)
                    .unwrap_or(true)
        })
        .ok_or_else(|| {
            format!(
                "missing GRIB message for discipline={discipline} category={category} number={number} level_type={level_type} level={level_value:?}"
            )
            .into()
        })
}

fn q_to_mixing_ratio(values: &[f64]) -> Vec<f64> {
    values
        .iter()
        .map(|&q| (q / (1.0 - q).max(1.0e-12)).max(1.0e-10))
        .collect()
}

fn normalize_pressure_level_hpa(level: f64) -> f64 {
    if level > 2_000.0 {
        level / 100.0
    } else {
        level
    }
}

fn normalize_longitude(lon: f64) -> f64 {
    if lon > 180.0 { lon - 360.0 } else { lon }
}

fn compute_panel_fields(
    surface: &SurfaceFields,
    pressure: &PressureFields,
) -> Result<(Vec<(Solar07Product, Vec<f64>)>, usize), Box<dyn std::error::Error>> {
    let grid = GridShape::new(surface.nx, surface.ny)?;
    let shape = VolumeShape::new(grid, pressure.pressure_levels_hpa.len())?;

    let mut height_agl_3d = pressure
        .gh_m_3d
        .iter()
        .enumerate()
        .map(|(idx, &value)| {
            let ij = idx % grid.len();
            (value - surface.orog_m[ij]).max(0.0)
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

    let pressure_3d_pa = broadcast_levels_pa(&pressure.pressure_levels_hpa, grid.len());
    let common = EcapeGridInputs {
        shape,
        pressure_3d_pa: &pressure_3d_pa,
        temperature_3d_c: &pressure.temperature_c_3d,
        qvapor_3d_kgkg: &pressure.qvapor_kgkg_3d,
        height_agl_3d_m: &height_agl_3d,
        u_3d_ms: &pressure.u_ms_3d,
        v_3d_ms: &pressure.v_ms_3d,
        psfc_pa: &surface.psfc_pa,
        t2_k: &surface.t2_k,
        q2_kgkg: &surface.q2_kgkg,
        u10_ms: &surface.u10_ms,
        v10_ms: &surface.v10_ms,
    };

    let triplet =
        compute_ecape_triplet_with_failure_mask(common, &EcapeTripletOptions::new("right_moving"))?;

    let wind = WindGridInputs {
        shape,
        u_3d_ms: &pressure.u_ms_3d,
        v_3d_ms: &pressure.v_ms_3d,
        height_agl_3d_m: &height_agl_3d,
    };
    let srh_1km = compute_srh(wind, 1000.0)?;
    let srh_3km = compute_srh(wind, 3000.0)?;
    let shear_6km = compute_shear(wind, 0.0, 6000.0)?;
    let experimental = compute_scp_ehi(ScpEhiInputs {
        grid,
        scp_cape_jkg: &triplet.mu.fields.ecape_jkg,
        scp_srh_m2s2: &srh_3km,
        scp_bulk_wind_difference_ms: &shear_6km,
        ehi_cape_jkg: &triplet.sb.fields.ecape_jkg,
        ehi_srh_m2s2: &srh_1km,
    })?;

    let failure_count = triplet.total_failure_count();
    let fields = vec![
        (Solar07Product::Sbecape, triplet.sb.fields.ecape_jkg),
        (Solar07Product::Mlecape, triplet.ml.fields.ecape_jkg),
        (Solar07Product::Muecape, triplet.mu.fields.ecape_jkg),
        (Solar07Product::Sbncape, triplet.sb.fields.ncape_jkg),
        (Solar07Product::Sbecin, triplet.sb.fields.cin_jkg),
        (Solar07Product::Mlecin, triplet.ml.fields.cin_jkg),
        (Solar07Product::EcapeScpExperimental, experimental.scp),
        (Solar07Product::EcapeEhiExperimental, experimental.ehi),
    ];
    Ok((fields, failure_count))
}

fn broadcast_levels_pa(levels_hpa: &[f64], n2d: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(levels_hpa.len() * n2d);
    for level in levels_hpa {
        out.extend(std::iter::repeat(*level * 100.0).take(n2d));
    }
    out
}

fn build_projected_map(
    surface: &SurfaceFields,
    region: RegionPreset,
) -> Result<ProjectedMap, Box<dyn std::error::Error>> {
    let (lon_min, lon_max, lat_min, lat_max) = region.bounds();

    let center_lat = surface
        .lat
        .iter()
        .zip(surface.lon.iter())
        .filter(|(_, lon)| lon.is_finite())
        .map(|(lat, _)| *lat)
        .sum::<f64>()
        / surface.lat.len() as f64;
    let proj = LambertConformal::new(
        surface.lambert_latin1,
        surface.lambert_latin2,
        normalize_longitude(surface.lambert_lov),
        center_lat,
    );

    let mut projected_x = Vec::with_capacity(surface.lat.len());
    let mut projected_y = Vec::with_capacity(surface.lat.len());
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    for (&lat, &lon) in surface.lat.iter().zip(surface.lon.iter()) {
        let (x, y) = proj.project(lat, lon);
        projected_x.push(x);
        projected_y.push(y);
        if lon >= lon_min && lon <= lon_max && lat >= lat_min && lat <= lat_max {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    }

    if !min_x.is_finite() || !max_x.is_finite() || !min_y.is_finite() || !max_y.is_finite() {
        return Err("midwest crop produced an empty projected extent".into());
    }

    let extent = wrf_render::overlay::MapExtent::from_bounds(
        min_x,
        max_x,
        min_y,
        max_y,
        map_frame_aspect_ratio(700, 520, true, true),
    );
    let mut lines = Vec::new();
    for layer in load_styled_conus_features() {
        for line in layer.lines {
            lines.push(ProjectedLineOverlay {
                points: line
                    .into_iter()
                    .map(|(lon, lat)| proj.project(lat, lon))
                    .collect(),
                color: Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a),
                width: layer.width,
            });
        }
    }

    Ok(ProjectedMap {
        projected_x,
        projected_y,
        extent: ProjectedExtent {
            x_min: extent.x_min,
            x_max: extent.x_max,
            y_min: extent.y_min,
            y_max: extent.y_max,
        },
        lines,
    })
}

fn render_panel(
    output_path: &Path,
    surface: &SurfaceFields,
    projected: &ProjectedMap,
    fields: &[(Solar07Product, Vec<f64>)],
    date: &str,
    cycle: u8,
    forecast_hour: u16,
    failure_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let nx = surface.nx;
    let ny = surface.ny;
    let core_grid = LatLonGrid::new(
        GridShape::new(nx, ny)?,
        surface.lat.iter().map(|&v| v as f32).collect(),
        surface.lon.iter().map(|&v| v as f32).collect(),
    )?;

    let panel_w = 700u32;
    let panel_h = 520u32;
    let header_h = 70u32;
    let layout = PanelGridLayout::two_by_four(panel_w, panel_h)?.with_padding(PanelPadding {
        top: header_h,
        ..Default::default()
    });
    let mut requests = Vec::with_capacity(fields.len());

    let run_title = format!(
        "HRRR ECAPE Product Panel  Run: {date} {cycle:02}:00 UTC  Forecast Hour: F{forecast_hour:02}  zero-fill columns: {failure_count}"
    );
    let subtitle = "Parcel-specific ECAPE shown for SB, ML, and MU. Single NCAPE context plus SBECIN and MLECIN. Experimental SCP/EHI shown.";
    for (product, values) in fields.iter() {
        let field = Field2D::new(
            ProductKey::named(product.slug()),
            if matches!(
                product,
                Solar07Product::EcapeScpExperimental | Solar07Product::EcapeEhiExperimental
            ) {
                "dimensionless"
            } else {
                "J/kg"
            },
            core_grid.clone(),
            values.iter().map(|&v| v as f32).collect(),
        )?;
        let mut request = MapRenderRequest::for_core_solar07_product(field, *product);
        request.width = panel_w;
        request.height = panel_h;
        request.projected_domain = Some(ProjectedDomain {
            x: projected.projected_x.clone(),
            y: projected.projected_y.clone(),
            extent: projected.extent.clone(),
        });
        request.projected_lines = projected.lines.clone();
        requests.push(request);
    }

    let mut canvas = render_panel_grid(&layout, &requests)?;
    text::draw_text_centered(&mut canvas, &run_title, 10, wrf_render::Rgba::BLACK, 2);
    text::draw_text_centered(&mut canvas, subtitle, 35, wrf_render::Rgba::BLACK, 1);

    DynamicImage::ImageRgba8(canvas).save(output_path)?;
    Ok(())
}
