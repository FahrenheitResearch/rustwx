#[cfg(not(feature = "wrf"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("wrf_local_pressure_volume_store requires `cargo build --release --features wrf`")
}

#[cfg(feature = "wrf")]
fn main() -> anyhow::Result<()> {
    app::run()
}

#[cfg(feature = "wrf")]
mod app {
    use anyhow::{Context, Result, bail};
    use clap::Parser;
    use rustwx_products::gridded::PressureFields;
    use rustwx_products::volume_store::{
        ChunkShape, GridSpec, PressureVolumeTimestep, write_pressure_volume_from_timesteps,
    };
    use serde::Serialize;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    #[derive(Debug, Parser)]
    #[command(
        name = "wrf-local-pressure-volume-store",
        about = "Export local WRF wrfout pressure columns into RustWX pressure VolumeStore artifacts"
    )]
    struct Args {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        #[arg(long, default_value = "wrf-local")]
        model: String,
        #[arg(long, default_value = "super_outbreak_1974_hourly_d02")]
        domain: String,
        #[arg(long, default_value = "1974-04-03T09:00:00Z")]
        cycle: String,
        #[arg(long)]
        lead_minutes: Option<u16>,
        #[arg(long, default_value_t = 4)]
        chunk_z: usize,
        #[arg(long, default_value_t = 64)]
        chunk_y: usize,
        #[arg(long, default_value_t = 64)]
        chunk_x: usize,
    }

    #[derive(Debug, Serialize)]
    struct Report {
        input: PathBuf,
        store_dir: PathBuf,
        model: String,
        domain: String,
        cycle: String,
        lead_minutes: u16,
        nx: usize,
        ny: usize,
        levels: usize,
        elapsed_ms: u128,
        build_stats_path: PathBuf,
    }

    pub fn run() -> Result<()> {
        let args = Args::parse();
        let started = Instant::now();
        let lead_minutes = match args.lead_minutes {
            Some(value) => value,
            None => lead_minutes_from_filename(&args.input)?,
        };
        let store_dir = args
            .out_dir
            .join(format!("lead_{lead_minutes:05}m"))
            .join("store");
        if store_dir.exists() {
            fs::remove_dir_all(&store_dir)
                .with_context(|| format!("remove old store {}", store_dir.display()))?;
        }
        fs::create_dir_all(&store_dir)?;

        let surface = rustwx_wrf::decode_surface_from_path(&args.input)
            .with_context(|| format!("decode WRF surface {}", args.input.display()))?;
        let pressure = rustwx_wrf::decode_pressure_from_path(&args.input)
            .with_context(|| format!("decode WRF pressure {}", args.input.display()))?;
        if surface.nx != pressure.nx || surface.ny != pressure.ny {
            bail!(
                "surface grid {}x{} did not match pressure grid {}x{}",
                surface.nx,
                surface.ny,
                pressure.nx,
                pressure.ny
            );
        }
        let grid = GridSpec::CurvilinearLatLon {
            nx: surface.nx,
            ny: surface.ny,
            lat_deg: surface.lat.iter().map(|value| *value as f32).collect(),
            lon_deg: surface.lon.iter().map(|value| *value as f32).collect(),
            description: format!("local WRF {} pressure grid", args.domain),
        };
        let pressure = PressureFields {
            pressure_levels_hpa: pressure.pressure_levels_hpa,
            pressure_3d_pa: Some(pressure.pressure_3d_pa),
            temperature_c_3d: pressure.temperature_c_3d,
            qvapor_kgkg_3d: pressure.qvapor_kgkg_3d,
            u_ms_3d: pressure.u_ms_3d,
            v_ms_3d: pressure.v_ms_3d,
            gh_m_3d: pressure.gh_m_3d,
            omega_pa_s_3d: None,
            absolute_vorticity_s_3d: None,
            cloud_liquid_kgkg_3d: None,
            cloud_ice_kgkg_3d: None,
            rain_kgkg_3d: None,
            snow_kgkg_3d: None,
            graupel_kgkg_3d: None,
        };
        let nx = grid.nx();
        let ny = grid.ny();
        let levels = pressure.pressure_levels_hpa.len();
        let timestep = PressureVolumeTimestep {
            forecast_hour: lead_minutes,
            pressure: &pressure,
        };
        let build = write_pressure_volume_from_timesteps(
            &store_dir,
            args.model.clone(),
            args.domain.clone(),
            args.cycle.clone(),
            grid,
            ChunkShape {
                t: 1,
                z: args.chunk_z,
                y: args.chunk_y,
                x: args.chunk_x,
            },
            &[timestep],
        )?;
        let report = Report {
            input: args.input,
            store_dir: store_dir.clone(),
            model: args.model,
            domain: args.domain,
            cycle: args.cycle,
            lead_minutes,
            nx,
            ny,
            levels,
            elapsed_ms: started.elapsed().as_millis(),
            build_stats_path: store_dir.join("build_stats.json"),
        };
        let report_path = store_dir
            .parent()
            .expect("lead directory")
            .join("wrf_local_pressure_volume_report.json");
        fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "report": report_path,
                "build": build,
            }))?
        );
        Ok(())
    }

    fn lead_minutes_from_filename(path: &Path) -> Result<u16> {
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow::anyhow!("input path has no UTF-8 filename"))?;
        let stamp = name
            .strip_prefix("wrfout_d02_")
            .or_else(|| name.strip_prefix("wrfout_d01_"))
            .ok_or_else(|| anyhow::anyhow!("expected wrfout_d??_YYYY-MM-DD_HH_MM_SS filename"))?;
        if stamp.len() < 19 {
            bail!("WRF timestamp too short in filename '{name}'");
        }
        let year = stamp[0..4].parse::<i32>()?;
        let month = stamp[5..7].parse::<u32>()?;
        let day = stamp[8..10].parse::<u32>()?;
        let hour = stamp[11..13].parse::<u32>()?;
        let minute = stamp[14..16].parse::<u32>()?;
        let second = stamp[17..19].parse::<u32>()?;
        let valid = chrono::NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_opt(hour, minute, second))
            .ok_or_else(|| anyhow::anyhow!("invalid timestamp in filename '{name}'"))?;
        let init = chrono::NaiveDate::from_ymd_opt(1974, 4, 3)
            .and_then(|date| date.and_hms_opt(9, 0, 0))
            .expect("fixed init is valid");
        let minutes = valid.signed_duration_since(init).num_minutes();
        if !(0..=u16::MAX as i64).contains(&minutes) {
            bail!("lead minutes {minutes} outside u16 range for '{name}'");
        }
        Ok(minutes as u16)
    }
}
