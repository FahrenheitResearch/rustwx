use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_calc::{
    EcapeGridInputs, EcapeTripletOptions, GridShape, ScpEhiInputs, VolumeShape, WindGridInputs,
    compute_ecape_triplet_with_failure_mask, compute_scp_ehi, compute_shear, compute_srh,
};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::hrrr::{
    HrrrPressureFields, HrrrSurfaceFields, PRESSURE_PATTERNS, SURFACE_PATTERNS, Solar07PanelField,
    Solar07PanelHeader, Solar07PanelLayout, broadcast_levels_pa, build_projected_map,
    decode_cache_path, fetch_hrrr_subset, load_or_decode_pressure, load_or_decode_surface,
    render_two_by_four_solar07_panel, resolve_hrrr_run,
};
use rustwx_render::Solar07Product;
use serde_json::json;

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
    source: rustwx_core::SourceId,
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
    let latest = resolve_hrrr_run(&args.date, args.cycle, args.source)?;
    let cycle = latest.cycle.hour_utc;

    let fetch_surface_start = Instant::now();
    let surface_subset = fetch_hrrr_subset(
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
    let pressure_subset = fetch_hrrr_subset(
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
    let surface_decode = load_or_decode_surface(
        &decode_cache_path(&cache_root, &surface_subset.request, "surface"),
        &surface_subset.bytes,
        !args.no_cache,
    )?;
    let decode_surface_ms = decode_surface_start.elapsed().as_millis();

    let decode_pressure_start = Instant::now();
    let pressure_decode = load_or_decode_pressure(
        &decode_cache_path(&cache_root, &pressure_subset.request, "pressure"),
        &pressure_subset.bytes,
        surface_decode.value.nx,
        surface_decode.value.ny,
        !args.no_cache,
    )?;
    let decode_stats_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_decode_stats.json",
        args.date, cycle, args.forecast_hour
    ));
    fs::write(
        &decode_stats_path,
        serde_json::to_vec_pretty(&json!({
            "surface": surface_stats(&surface_decode.value),
            "pressure": pressure_stats(
                &pressure_decode.value,
                surface_decode.value.nx,
                surface_decode.value.ny
            ),
        }))?,
    )?;
    let decode_pressure_ms = decode_pressure_start.elapsed().as_millis();

    let layout = Solar07PanelLayout::default();
    let project_start = Instant::now();
    let projected = build_projected_map(
        &surface_decode.value,
        args.region.bounds(),
        layout.target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let (fields, failure_count) =
        compute_panel_fields(&surface_decode.value, &pressure_decode.value)?;
    let compute_ms = compute_start.elapsed().as_millis();

    let render_start = Instant::now();
    let panel_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:02}_{}_ecape8_panel.png",
        args.date,
        cycle,
        args.forecast_hour,
        args.region.slug()
    ));
    let header = Solar07PanelHeader::new(format!(
        "HRRR ECAPE Product Panel  Run: {} {:02}:00 UTC  Forecast Hour: F{:02}  zero-fill columns: {}",
        args.date, cycle, args.forecast_hour, failure_count
    ))
    .with_subtitle_line(
        "Parcel-specific ECAPE shown for SB, ML, and MU. Single NCAPE context plus SBECIN and MLECIN. Experimental SCP/EHI shown.",
    );
    render_two_by_four_solar07_panel(
        &panel_path,
        &surface_decode.value.core_grid()?,
        &projected,
        &fields,
        &header,
        layout,
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
        decode_surface_cache_hit: surface_decode.cache_hit,
        decode_pressure_cache_hit: pressure_decode.cache_hit,
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
                "surface_decode_path": surface_decode.path,
                "pressure_decode_path": pressure_decode.path,
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

fn surface_stats(surface: &HrrrSurfaceFields) -> serde_json::Value {
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

fn pressure_stats(pressure: &HrrrPressureFields, nx: usize, ny: usize) -> serde_json::Value {
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

fn compute_panel_fields(
    surface: &HrrrSurfaceFields,
    pressure: &HrrrPressureFields,
) -> Result<(Vec<Solar07PanelField>, usize), Box<dyn std::error::Error>> {
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
        Solar07PanelField::new(Solar07Product::Sbecape, "J/kg", triplet.sb.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Mlecape, "J/kg", triplet.ml.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Muecape, "J/kg", triplet.mu.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Sbncape, "J/kg", triplet.sb.fields.ncape_jkg),
        Solar07PanelField::new(Solar07Product::Sbecin, "J/kg", triplet.sb.fields.cin_jkg),
        Solar07PanelField::new(Solar07Product::Mlecin, "J/kg", triplet.ml.fields.cin_jkg),
        Solar07PanelField::new(
            Solar07Product::EcapeScpExperimental,
            "dimensionless",
            experimental.scp,
        ),
        Solar07PanelField::new(
            Solar07Product::EcapeEhiExperimental,
            "dimensionless",
            experimental.ehi,
        ),
    ];
    Ok((fields, failure_count))
}
