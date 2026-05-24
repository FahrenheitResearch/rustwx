use anyhow::{Context, Result, bail};
use clap::Parser;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::ensure_dir;
use rustwx_products::gridded::load_model_timestep_from_parts_cropped;
use rustwx_products::wxstore_profile::{WxProfileTimestep, write_wx_profile_store_from_timesteps};
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "model-wxprofile-store",
    about = "Build a native WxStore .wxp profile store directly from decoded model pressure fields"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, default_value = "conus")]
    domain: String,
    #[arg(long)]
    date: String,
    #[arg(long)]
    cycle: u8,
    #[arg(long, default_value = "0")]
    hours: String,
    #[arg(long, default_value = "aws")]
    source: SourceId,
    #[arg(long, default_value_t = -125.0)]
    west: f64,
    #[arg(long, default_value_t = -66.0)]
    east: f64,
    #[arg(long, default_value_t = 24.0)]
    south: f64,
    #[arg(long, default_value_t = 50.0)]
    north: f64,
    #[arg(long, default_value = "proof/cache")]
    cache_dir: PathBuf,
    #[arg(long, default_value = "proof/model_wxprofile_store")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = 4096)]
    chunk_x: usize,
    #[arg(long, default_value_t = 4)]
    chunk_y: usize,
    #[arg(long, default_value_t = false)]
    include_vvel: bool,
}

#[derive(Debug, Serialize)]
struct LoadedHourReport {
    forecast_hour: u16,
    nx: usize,
    ny: usize,
    levels: usize,
    total_ms: u128,
    fetch_surface_cache_hit: bool,
    fetch_pressure_cache_hit: bool,
    decode_surface_cache_hit: bool,
    decode_pressure_cache_hit: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    model: ModelId,
    date: String,
    cycle: u8,
    source: SourceId,
    hours: Vec<u16>,
    bounds: [f64; 4],
    store_path: String,
    loaded_hours: Vec<LoadedHourReport>,
    build: rustwx_products::wxstore_profile::BuildWxProfileReport,
    total_ms: u128,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let started = Instant::now();
    let hours = parse_hours(&args.hours)?;
    if hours.is_empty() {
        bail!("--hours produced no forecast hours");
    }
    if !args.no_cache {
        ensure_dir(&args.cache_dir).map_err(|err| anyhow::anyhow!(err.to_string()))?;
    }
    ensure_dir(&args.out_dir).map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let store_path = args.out_dir.join("store");
    if store_path.exists() {
        fs::remove_dir_all(&store_path)
            .with_context(|| format!("remove old store {}", store_path.display()))?;
    }

    let mut loaded = Vec::new();
    let mut hour_reports = Vec::new();
    for hour in &hours {
        let hour_started = Instant::now();
        let timestep = load_model_timestep_from_parts_cropped(
            args.model,
            &args.date,
            Some(args.cycle),
            *hour,
            args.source,
            None,
            None,
            &args.cache_dir,
            !args.no_cache,
            (args.west, args.east, args.south, args.north),
        )
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        let surface = timestep.surface_decode.value;
        let pressure = timestep.pressure_decode.value;
        hour_reports.push(LoadedHourReport {
            forecast_hour: *hour,
            nx: surface.nx,
            ny: surface.ny,
            levels: pressure.pressure_levels_hpa.len(),
            total_ms: hour_started.elapsed().as_millis(),
            fetch_surface_cache_hit: timestep.shared_timing.fetch_surface_cache_hit,
            fetch_pressure_cache_hit: timestep.shared_timing.fetch_pressure_cache_hit,
            decode_surface_cache_hit: timestep.shared_timing.decode_surface_cache_hit,
            decode_pressure_cache_hit: timestep.shared_timing.decode_pressure_cache_hit,
        });
        loaded.push((*hour, surface, pressure));
    }

    let timesteps = loaded
        .iter()
        .map(|(forecast_hour, surface, pressure)| WxProfileTimestep {
            forecast_hour: *forecast_hour,
            pressure,
            surface,
        })
        .collect::<Vec<_>>();
    let build = write_wx_profile_store_from_timesteps(
        &store_path,
        args.model.as_str(),
        args.domain.clone(),
        cycle_iso(&args.date, args.cycle),
        run_id(args.model, &args.date, args.cycle),
        &timesteps,
        args.chunk_x,
        args.chunk_y,
        args.include_vvel,
    )
    .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let report = Report {
        model: args.model,
        date: args.date,
        cycle: args.cycle,
        source: args.source,
        hours,
        bounds: [args.west, args.east, args.south, args.north],
        store_path: store_path.display().to_string(),
        loaded_hours: hour_reports,
        build,
        total_ms: started.elapsed().as_millis(),
    };
    fs::write(
        args.out_dir.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn parse_hours(value: &str) -> Result<Vec<u16>> {
    let mut hours = Vec::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if let Some((start, end)) = part.split_once('-') {
            let start = start.trim().parse::<u16>()?;
            let end = end.trim().parse::<u16>()?;
            if end < start {
                bail!("invalid descending hour range {part}");
            }
            hours.extend(start..=end);
        } else {
            hours.push(part.parse::<u16>()?);
        }
    }
    hours.sort_unstable();
    hours.dedup();
    Ok(hours)
}

fn cycle_iso(date: &str, cycle: u8) -> String {
    format!(
        "{}-{}-{}T{cycle:02}:00:00Z",
        &date[0..4],
        &date[4..6],
        &date[6..8]
    )
}

fn run_id(model: ModelId, date: &str, cycle: u8) -> String {
    format!("{}_{}_{cycle:02}z", date, model.as_str().replace('-', "_"))
}
