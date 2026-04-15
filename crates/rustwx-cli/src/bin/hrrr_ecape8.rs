use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::hrrr::{
    HrrrPressureFields, HrrrSurfaceFields, Solar07PanelHeader, Solar07PanelLayout,
    build_projected_map, compute_ecape8_panel_fields, load_hrrr_timestep_from_parts,
    render_two_by_four_solar07_panel,
};
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
    #[arg(long, default_value_t = false)]
    write_proof_artifacts: bool,
}

#[derive(Debug, Clone)]
struct Timing {
    prepare_ms: u128,
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
    let load_start = Instant::now();
    let timestep = load_hrrr_timestep_from_parts(
        &args.date,
        args.cycle,
        args.forecast_hour,
        args.source,
        &cache_root,
        !args.no_cache,
    )?;
    let load_ms = load_start.elapsed().as_millis();
    let cycle = timestep.latest().cycle.hour_utc;
    let shared_timing = timestep.shared_timing().clone();

    let sfc_subset_path = args.write_proof_artifacts.then(|| {
        args.out_dir.join(format!(
            "rustwx_hrrr_{}_{}z_f{:02}_sfc_subset.grib2",
            args.date, cycle, args.forecast_hour
        ))
    });
    let prs_subset_path = args.write_proof_artifacts.then(|| {
        args.out_dir.join(format!(
            "rustwx_hrrr_{}_{}z_f{:02}_prs_subset.grib2",
            args.date, cycle, args.forecast_hour
        ))
    });
    if let Some(path) = &sfc_subset_path {
        fs::write(path, &timestep.surface_subset().bytes)?;
    }
    if let Some(path) = &prs_subset_path {
        fs::write(path, &timestep.pressure_subset().bytes)?;
    }

    let decode_stats_path = args.write_proof_artifacts.then(|| {
        args.out_dir.join(format!(
            "rustwx_hrrr_{}_{}z_f{:02}_decode_stats.json",
            args.date, cycle, args.forecast_hour
        ))
    });
    if let Some(path) = &decode_stats_path {
        fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "surface": surface_stats(&timestep.surface_decode().value),
                "pressure": pressure_stats(
                    &timestep.pressure_decode().value,
                    timestep.surface_decode().value.nx,
                    timestep.surface_decode().value.ny
                ),
            }))?,
        )?;
    }

    let layout = Solar07PanelLayout::default();
    let project_start = Instant::now();
    let projected = build_projected_map(
        &timestep.surface_decode().value,
        args.region.bounds(),
        layout.target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let (fields, failure_count) = compute_ecape8_panel_fields(
        &timestep.surface_decode().value,
        &timestep.pressure_decode().value,
    )?;
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
        timestep.grid(),
        &projected,
        &fields,
        &header,
        layout,
    )?;
    let render_ms = render_start.elapsed().as_millis();

    let timing = Timing {
        prepare_ms: load_ms,
        fetch_surface_ms: shared_timing.fetch_surface_ms,
        fetch_pressure_ms: shared_timing.fetch_pressure_ms,
        decode_surface_ms: shared_timing.decode_surface_ms,
        decode_pressure_ms: shared_timing.decode_pressure_ms,
        project_ms,
        compute_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
        fetch_surface_cache_hit: shared_timing.fetch_surface_cache_hit,
        fetch_pressure_cache_hit: shared_timing.fetch_pressure_cache_hit,
        decode_surface_cache_hit: shared_timing.decode_surface_cache_hit,
        decode_pressure_cache_hit: shared_timing.decode_pressure_cache_hit,
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
                "surface_fetch_bytes_path": timestep.surface_subset().fetched.bytes_path,
                "surface_fetch_meta_path": timestep.surface_subset().fetched.metadata_path,
                "pressure_fetch_bytes_path": timestep.pressure_subset().fetched.bytes_path,
                "pressure_fetch_meta_path": timestep.pressure_subset().fetched.metadata_path,
                "surface_decode_path": timestep.surface_decode().path,
                "pressure_decode_path": timestep.pressure_decode().path,
            },
            "surface_subset_path": sfc_subset_path,
            "pressure_subset_path": prs_subset_path,
            "decode_stats_path": decode_stats_path,
            "panel_path": panel_path,
            "timing_ms": {
                "prepare": timing.prepare_ms,
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
