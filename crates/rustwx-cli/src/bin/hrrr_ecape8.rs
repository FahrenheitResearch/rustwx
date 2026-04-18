use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[path = "../region.rs"]
mod region;

use clap::Parser;
use region::RegionPreset;
use rustwx_core::{CanonicalBundleDescriptor, ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::direct::build_projected_map;
use rustwx_products::ecape::compute_ecape8_panel_fields;
use rustwx_products::gridded::{PressureFields, SurfaceFields};
use rustwx_products::hrrr::resolve_hrrr_run;
use rustwx_products::runtime::{BundleLoaderConfig, load_execution_plan};
use rustwx_products::severe::build_severe_execution_plan;
use rustwx_products::shared_context::{
    Solar07PanelHeader, Solar07PanelLayout, render_two_by_four_solar07_panel,
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
    source: SourceId,
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
    project_ms: u128,
    compute_ms: u128,
    render_ms: u128,
    total_ms: u128,
    surface_fetch_cache_hit: bool,
    pressure_fetch_cache_hit: bool,
    surface_decode_cache_hit: bool,
    pressure_decode_cache_hit: bool,
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
    // HRRR ECAPE 8-panel runs through the same planner as `ecape_batch`;
    // this bin only adds optional proof-artifact dumps so a developer
    // can inspect the raw fetched bytes and decode statistics for one
    // hour without invoking the full operational manifest pipeline.
    let latest = resolve_hrrr_run(&args.date, args.cycle, args.forecast_hour, args.source)?;
    let plan = build_severe_execution_plan(&latest, args.forecast_hour, None, None);
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig {
            cache_root: cache_root.clone(),
            use_cache: !args.no_cache,
        },
    )?;
    let load_ms = load_start.elapsed().as_millis();

    let cycle = loaded.latest.cycle.hour_utc;
    let surface_planned = loaded
        .plan
        .bundle_for(
            CanonicalBundleDescriptor::SurfaceAnalysis,
            args.forecast_hour,
        )
        .ok_or("planner missed HRRR surface analysis bundle")?;
    let pressure_planned = loaded
        .plan
        .bundle_for(
            CanonicalBundleDescriptor::PressureAnalysis,
            args.forecast_hour,
        )
        .ok_or("planner missed HRRR pressure analysis bundle")?;
    let surface_decode = loaded
        .surface_decode_for(
            CanonicalBundleDescriptor::SurfaceAnalysis,
            args.forecast_hour,
        )
        .ok_or("loader missing surface decode")?;
    let pressure_decode = loaded
        .pressure_decode_for(
            CanonicalBundleDescriptor::PressureAnalysis,
            args.forecast_hour,
        )
        .ok_or("loader missing pressure decode")?;
    let surface_fetched = loaded
        .fetched_for(surface_planned)
        .ok_or("loader missing surface fetch")?;
    let pressure_fetched = loaded
        .fetched_for(pressure_planned)
        .ok_or("loader missing pressure fetch")?;

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
        fs::write(path, &surface_fetched.file.bytes)?;
    }
    if let Some(path) = &prs_subset_path {
        fs::write(path, &pressure_fetched.file.bytes)?;
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
                "surface": surface_stats(&surface_decode.value),
                "pressure": pressure_stats(
                    &pressure_decode.value,
                    surface_decode.value.nx,
                    surface_decode.value.ny
                ),
            }))?,
        )?;
    }

    let layout = Solar07PanelLayout::default();
    let grid = surface_decode.value.core_grid()?;
    let project_start = Instant::now();
    let projected = build_projected_map(
        &grid.lat_deg,
        &grid.lon_deg,
        args.region.bounds(),
        layout.target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let (fields, failure_count) =
        compute_ecape8_panel_fields(&surface_decode.value, &pressure_decode.value)?;
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
    render_two_by_four_solar07_panel(&panel_path, &grid, &projected, &fields, &header, layout)?;
    let render_ms = render_start.elapsed().as_millis();

    let _ = ModelId::Hrrr;
    let timing = Timing {
        prepare_ms: load_ms,
        project_ms,
        compute_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
        surface_fetch_cache_hit: surface_fetched.file.fetched.cache_hit,
        pressure_fetch_cache_hit: pressure_fetched.file.fetched.cache_hit,
        surface_decode_cache_hit: surface_decode.cache_hit,
        pressure_decode_cache_hit: pressure_decode.cache_hit,
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
                "surface_fetch_hit": timing.surface_fetch_cache_hit,
                "pressure_fetch_hit": timing.pressure_fetch_cache_hit,
                "surface_decode_hit": timing.surface_decode_cache_hit,
                "pressure_decode_hit": timing.pressure_decode_cache_hit,
                "surface_fetch_bytes_path": surface_fetched.file.fetched.bytes_path,
                "surface_fetch_meta_path": surface_fetched.file.fetched.metadata_path,
                "pressure_fetch_bytes_path": pressure_fetched.file.fetched.bytes_path,
                "pressure_fetch_meta_path": pressure_fetched.file.fetched.metadata_path,
            },
            "surface_subset_path": sfc_subset_path,
            "pressure_subset_path": prs_subset_path,
            "decode_stats_path": decode_stats_path,
            "panel_path": panel_path,
            "timing_ms": {
                "prepare": timing.prepare_ms,
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
