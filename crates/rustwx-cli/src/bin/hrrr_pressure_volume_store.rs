use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::ensure_dir;
use rustwx_products::gridded::{
    PressureFields, SurfaceFields, load_model_timestep_from_parts_cropped,
};
use rustwx_products::volume_store::{
    ChunkShape, GridSpec, PressureTimestepProvider, SurfaceTerrainTimestep, VolumeResult,
    VolumeStore, pressure_volume_variables_for_fields, write_pressure_volume_from_provider,
    write_surface_terrain_store,
};
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{Receiver, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "pressure-volume-store",
    about = "Build and smoke-profile a cropped pressure VolumeStore for a supported model"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, default_value = "regional")]
    domain: String,
    #[arg(long)]
    date: String,
    #[arg(long)]
    cycle: u8,
    #[arg(long, default_value_t = 0)]
    start_hour: u16,
    #[arg(long, default_value_t = 1)]
    end_hour: u16,
    #[arg(
        long,
        help = "Forecast hours to build, e.g. 0,3,6 or 0-18. Overrides start/end."
    )]
    hours: Option<String>,
    #[arg(long, default_value = "aws")]
    source: SourceId,
    #[arg(long, default_value_t = -125.0)]
    west: f64,
    #[arg(long, default_value_t = -113.5)]
    east: f64,
    #[arg(long, default_value_t = 31.5)]
    south: f64,
    #[arg(long, default_value_t = 42.5)]
    north: f64,
    #[arg(long, default_value = "proof/cache")]
    cache_dir: PathBuf,
    #[arg(long, default_value = "proof/hrrr_pressure_volume_store")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long)]
    load_parallelism: Option<usize>,
    #[arg(long, default_value_t = 1)]
    chunk_t: usize,
    #[arg(long, default_value_t = 4)]
    chunk_z: usize,
    #[arg(long, default_value_t = 64)]
    chunk_y: usize,
    #[arg(long, default_value_t = 64)]
    chunk_x: usize,
    #[arg(long, default_value_t = 40.5865)]
    sample_lat: f64,
    #[arg(long, default_value_t = -122.3917)]
    sample_lon: f64,
    #[arg(long, default_value_t = 34.05)]
    route_start_lat: f64,
    #[arg(long, default_value_t = -118.25)]
    route_start_lon: f64,
    #[arg(long, default_value_t = 39.32)]
    route_end_lat: f64,
    #[arg(long, default_value_t = -120.18)]
    route_end_lon: f64,
    #[arg(long, default_value_t = 20.0)]
    route_spacing_km: f32,
}

#[derive(Debug, Clone, Serialize)]
struct HourLoadReport {
    forecast_hour: u8,
    nx: usize,
    ny: usize,
    levels: usize,
    total_ms: u128,
    fetch_surface_ms: u128,
    fetch_pressure_ms: u128,
    decode_surface_ms: u128,
    decode_pressure_ms: u128,
    fetch_surface_cache_hit: bool,
    fetch_pressure_cache_hit: bool,
    decode_surface_cache_hit: bool,
    decode_pressure_cache_hit: bool,
}

#[derive(Debug, Serialize)]
struct SmokeProfile {
    open_ms: u128,
    point_sample_ms: u128,
    point_values: usize,
    route_sample_ms: u128,
    route_samples: usize,
    route_values: usize,
}

#[derive(Debug, Serialize)]
struct FileSizes {
    manifest_bytes: u64,
    index_bytes: u64,
    chunks_bytes: u64,
    build_stats_bytes: u64,
    total_bytes: u64,
}

#[derive(Debug, Serialize)]
struct Report {
    request: RequestReport,
    files: FileSizes,
    build: rustwx_products::volume_store::BuildStats,
    loaded_hours: Vec<HourLoadReport>,
    smoke_profile: SmokeProfile,
    artifacts: ArtifactReport,
}

#[derive(Debug, Serialize)]
struct RequestReport {
    model: ModelId,
    date: String,
    cycle: u8,
    source: SourceId,
    forecast_hours: Vec<u8>,
    bounds: (f64, f64, f64, f64),
    chunk_shape: ChunkShape,
    load_parallelism: usize,
    grid_cells: usize,
    levels: usize,
}

#[derive(Debug, Serialize)]
struct ArtifactReport {
    store_dir: String,
    report_json: String,
}

struct LoadedHour {
    surface: SurfaceFields,
    pressure: PressureFields,
    grid: GridSpec,
    report: HourLoadReport,
}

struct HrrrPressureProvider {
    args: Args,
    first_hours: BTreeMap<u8, PressureFields>,
    prefetch: Option<HourPrefetch>,
    surface_terrain: Rc<RefCell<BTreeMap<u8, SurfaceTerrainTimestep>>>,
    reports: Rc<RefCell<Vec<HourLoadReport>>>,
}

impl PressureTimestepProvider for HrrrPressureProvider {
    fn pressure_fields(&mut self, forecast_hour: u8) -> VolumeResult<PressureFields> {
        if let Some(pressure) = self.first_hours.remove(&forecast_hour) {
            return Ok(pressure);
        }
        if let Some(prefetch) = self.prefetch.as_mut() {
            return prefetch
                .take(forecast_hour)
                .map(|loaded| {
                    self.reports.borrow_mut().push(loaded.report);
                    self.surface_terrain.borrow_mut().insert(
                        forecast_hour,
                        SurfaceTerrainTimestep::from_surface(forecast_hour, &loaded.surface),
                    );
                    loaded.pressure
                })
                .map_err(|err| {
                    rustwx_products::volume_store::VolumeStoreError::Provider(err.to_string())
                });
        }
        load_hour(&self.args, forecast_hour)
            .map(|loaded| {
                self.reports.borrow_mut().push(loaded.report);
                self.surface_terrain.borrow_mut().insert(
                    forecast_hour,
                    SurfaceTerrainTimestep::from_surface(forecast_hour, &loaded.surface),
                );
                loaded.pressure
            })
            .map_err(|err| {
                rustwx_products::volume_store::VolumeStoreError::Provider(err.to_string())
            })
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.end_hour < args.start_hour {
        bail!("--end-hour must be >= --start-hour");
    }
    if args.end_hour > u16::from(u8::MAX) {
        bail!(
            "VolumeStore forecast hours are currently u8, got {}",
            args.end_hour
        );
    }
    if args.chunk_t != 1 {
        bail!("live pressure VolumeStore currently requires --chunk-t 1");
    }
    let forecast_hours = forecast_hours_from_args(&args)?;
    let load_parallelism = effective_load_parallelism(&args)?;
    if !args.no_cache {
        ensure_dir(&args.cache_dir).map_err(|err| anyhow!(err.to_string()))?;
    }
    ensure_dir(&args.out_dir).map_err(|err| anyhow!(err.to_string()))?;
    let store_dir = args.out_dir.join("store");
    if store_dir.exists() {
        fs::remove_dir_all(&store_dir)
            .with_context(|| format!("remove old store {}", store_dir.display()))?;
    }

    let request_args = args.clone();
    let first_hour = *forecast_hours.first().expect("validated non-empty hours");
    let first = load_hour(&args, first_hour)?;
    let levels_hpa = levels_from_pressure(&first.pressure)?;
    let volume_variables = pressure_volume_variables_for_fields(&first.pressure);
    let grid_cells = first.grid.grid_len();
    let level_count = levels_hpa.len();

    let reports = Rc::new(RefCell::new(vec![first.report.clone()]));
    let surface_terrain = Rc::new(RefCell::new(BTreeMap::from([(
        first_hour,
        SurfaceTerrainTimestep::from_surface(first_hour, &first.surface),
    )])));
    let mut first_hours = BTreeMap::new();
    first_hours.insert(first_hour, first.pressure);
    let prefetch_hours = forecast_hours
        .iter()
        .copied()
        .filter(|hour| *hour != first_hour)
        .collect::<Vec<_>>();
    let prefetch = (load_parallelism > 1 && !prefetch_hours.is_empty()).then(|| {
        eprintln!(
            "prefetching {} pressure hours with {} workers",
            prefetch_hours.len(),
            load_parallelism
        );
        HourPrefetch::new(args.clone(), prefetch_hours, load_parallelism)
    });
    let provider = HrrrPressureProvider {
        args,
        first_hours,
        prefetch,
        surface_terrain: surface_terrain.clone(),
        reports: reports.clone(),
    };

    let build = write_pressure_volume_from_provider(
        &store_dir,
        provider.args.model.to_string(),
        provider.args.domain.clone(),
        cycle_iso(&provider.args.date, provider.args.cycle),
        first.grid,
        ChunkShape {
            t: provider.args.chunk_t,
            z: provider.args.chunk_z,
            y: provider.args.chunk_y,
            x: provider.args.chunk_x,
        },
        forecast_hours.clone(),
        levels_hpa.clone(),
        volume_variables,
        provider,
    )?;
    let terrain_timesteps = surface_terrain
        .borrow()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let _terrain_build = write_surface_terrain_store(&store_dir, terrain_timesteps, grid_cells)?;

    let smoke_profile =
        smoke_profile_store(&store_dir, &forecast_hours, &levels_hpa, &request_args)
            .unwrap_or_else(|_| SmokeProfile {
                open_ms: 0,
                point_sample_ms: 0,
                point_values: 0,
                route_sample_ms: 0,
                route_samples: 0,
                route_values: 0,
            });

    let report_json = request_args.out_dir.join("report.json");
    let files = file_sizes(&store_dir)?;
    let report = Report {
        request: RequestReport {
            model: request_args.model,
            date: request_args.date.clone(),
            cycle: request_args.cycle,
            source: request_args.source,
            forecast_hours,
            bounds: (
                request_args.west,
                request_args.east,
                request_args.south,
                request_args.north,
            ),
            chunk_shape: ChunkShape {
                t: request_args.chunk_t,
                z: request_args.chunk_z,
                y: request_args.chunk_y,
                x: request_args.chunk_x,
            },
            load_parallelism,
            grid_cells,
            levels: level_count,
        },
        files,
        build,
        loaded_hours: reports.borrow().clone(),
        smoke_profile,
        artifacts: ArtifactReport {
            store_dir: store_dir.display().to_string(),
            report_json: report_json.display().to_string(),
        },
    };
    fs::write(&report_json, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

struct PrefetchResult {
    forecast_hour: u8,
    loaded: Result<LoadedHour, String>,
}

struct HourPrefetch {
    receiver: Receiver<PrefetchResult>,
    pending: BTreeMap<u8, Result<LoadedHour, String>>,
    _workers: Vec<thread::JoinHandle<()>>,
}

impl HourPrefetch {
    fn new(args: Args, hours: Vec<u8>, load_parallelism: usize) -> Self {
        let worker_count = load_parallelism.max(1).min(hours.len());
        let queue = Arc::new(Mutex::new(VecDeque::from(hours)));
        let (sender, receiver) = sync_channel(worker_count);
        let mut workers = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let args = args.clone();
            let queue = queue.clone();
            let sender = sender.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let forecast_hour = match queue.lock() {
                        Ok(mut queue) => queue.pop_front(),
                        Err(_) => None,
                    };
                    let Some(forecast_hour) = forecast_hour else {
                        break;
                    };
                    eprintln!("prefetch worker {worker_id}: loading f{forecast_hour:03}");
                    let loaded = load_hour(&args, forecast_hour).map_err(|err| err.to_string());
                    if sender
                        .send(PrefetchResult {
                            forecast_hour,
                            loaded,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }));
        }
        drop(sender);

        Self {
            receiver,
            pending: BTreeMap::new(),
            _workers: workers,
        }
    }

    fn take(&mut self, forecast_hour: u8) -> Result<LoadedHour> {
        if let Some(result) = self.pending.remove(&forecast_hour) {
            return result.map_err(|err| anyhow!(err));
        }
        loop {
            let result = self.receiver.recv().map_err(|_| {
                anyhow!(
                    "pressure hour prefetch workers exited before f{forecast_hour:03} was loaded"
                )
            })?;
            if result.forecast_hour == forecast_hour {
                return result.loaded.map_err(|err| anyhow!(err));
            }
            self.pending.insert(result.forecast_hour, result.loaded);
        }
    }
}

fn effective_load_parallelism(args: &Args) -> Result<usize> {
    let value = match args.load_parallelism {
        Some(value) => value,
        None => env::var("RUSTWX_VOLUME_STORE_LOAD_PARALLELISM")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1),
    };
    if value == 0 {
        bail!("--load-parallelism must be >= 1");
    }
    Ok(value)
}

fn forecast_hours_from_args(args: &Args) -> Result<Vec<u8>> {
    let hours = if let Some(spec) = args.hours.as_deref() {
        parse_hour_spec(spec)?
    } else {
        if args.end_hour < args.start_hour {
            bail!("--end-hour must be >= --start-hour");
        }
        (args.start_hour..=args.end_hour).collect::<Vec<_>>()
    };
    let mut out = Vec::with_capacity(hours.len());
    for hour in hours {
        if hour > u16::from(u8::MAX) {
            bail!("VolumeStore forecast hours are currently u8, got {hour}");
        }
        out.push(hour as u8);
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        bail!("at least one forecast hour is required");
    }
    Ok(out)
}

fn parse_hour_spec(spec: &str) -> Result<Vec<u16>> {
    let mut hours = Vec::new();
    for token in spec
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        if let Some((start, end)) = token.split_once('-') {
            let start = start
                .trim()
                .parse::<u16>()
                .with_context(|| format!("invalid start hour '{start}'"))?;
            let end = end
                .trim()
                .parse::<u16>()
                .with_context(|| format!("invalid end hour '{end}'"))?;
            if end < start {
                bail!("invalid hour range '{token}'");
            }
            hours.extend(start..=end);
        } else {
            hours.push(
                token
                    .parse::<u16>()
                    .with_context(|| format!("invalid forecast hour '{token}'"))?,
            );
        }
    }
    Ok(hours)
}

fn load_hour(args: &Args, forecast_hour: u8) -> Result<LoadedHour> {
    let started = Instant::now();
    let loaded = load_model_timestep_from_parts_cropped(
        args.model,
        &args.date,
        Some(args.cycle),
        u16::from(forecast_hour),
        args.source,
        None,
        None,
        &args.cache_dir,
        !args.no_cache,
        (args.west, args.east, args.south, args.north),
    )
    .map_err(|err| anyhow!(err.to_string()))?;
    let surface = loaded.surface_decode.value;
    let pressure = loaded.pressure_decode.value;
    let grid = GridSpec::CurvilinearLatLon {
        nx: surface.nx,
        ny: surface.ny,
        lat_deg: surface.lat.iter().map(|value| *value as f32).collect(),
        lon_deg: surface.lon.iter().map(|value| *value as f32).collect(),
        description: format!(
            "cropped {} f{forecast_hour:03} pressure grid from {:.3},{:.3},{:.3},{:.3}",
            args.model, args.west, args.east, args.south, args.north
        ),
    };
    let report = HourLoadReport {
        forecast_hour,
        nx: surface.nx,
        ny: surface.ny,
        levels: pressure.pressure_levels_hpa.len(),
        total_ms: started.elapsed().as_millis(),
        fetch_surface_ms: loaded.shared_timing.fetch_surface_ms,
        fetch_pressure_ms: loaded.shared_timing.fetch_pressure_ms,
        decode_surface_ms: loaded.shared_timing.decode_surface_ms,
        decode_pressure_ms: loaded.shared_timing.decode_pressure_ms,
        fetch_surface_cache_hit: loaded.shared_timing.fetch_surface_cache_hit,
        fetch_pressure_cache_hit: loaded.shared_timing.fetch_pressure_cache_hit,
        decode_surface_cache_hit: loaded.shared_timing.decode_surface_cache_hit,
        decode_pressure_cache_hit: loaded.shared_timing.decode_pressure_cache_hit,
    };
    Ok(LoadedHour {
        surface,
        pressure,
        grid,
        report,
    })
}

fn levels_from_pressure(pressure: &PressureFields) -> Result<Vec<u16>> {
    let mut levels = Vec::with_capacity(pressure.pressure_levels_hpa.len());
    for level in &pressure.pressure_levels_hpa {
        if !level.is_finite() || *level <= 0.0 || *level > f64::from(u16::MAX) {
            bail!("invalid pressure level {level}");
        }
        let rounded = level.round() as u16;
        if rounded > 0 {
            levels.push(rounded);
        }
    }
    levels.sort_unstable_by(|left, right| right.cmp(left));
    levels.dedup();
    if levels.is_empty() {
        bail!("no integer pressure levels remain after rounding");
    }
    Ok(levels)
}

fn smoke_profile_store(
    store_dir: &Path,
    forecast_hours: &[u8],
    levels_hpa: &[u16],
    args: &Args,
) -> Result<SmokeProfile> {
    let open_start = Instant::now();
    let store = VolumeStore::open(store_dir)?;
    let open_ms = open_start.elapsed().as_millis();

    let point_start = Instant::now();
    let profile = store.sample_point_3d(
        args.sample_lat,
        args.sample_lon,
        &["TMP", "SPFH", "UGRD", "VGRD", "HGT"],
        forecast_hours,
        levels_hpa,
    )?;
    let point_sample_ms = point_start.elapsed().as_millis();

    let route = rustwx_products::volume_store::RouteDef {
        id: "ca_sw_to_sierra".to_string(),
        name: "CA southwest to Sierra".to_string(),
        points: vec![
            (args.route_start_lat, args.route_start_lon),
            (args.route_end_lat, args.route_end_lon),
        ],
        sample_spacing_km: args.route_spacing_km,
    };
    let route_hour = forecast_hours[forecast_hours.len() / 2];
    let route_start = Instant::now();
    let section = store.sample_route_3d(
        &route,
        &["TMP", "SPFH", "UGRD", "VGRD", "HGT"],
        route_hour,
        levels_hpa,
    )?;
    let route_sample_ms = route_start.elapsed().as_millis();

    Ok(SmokeProfile {
        open_ms,
        point_sample_ms,
        point_values: profile.samples.len(),
        route_sample_ms,
        route_samples: section.route_samples.len(),
        route_values: section.values.len(),
    })
}

fn file_sizes(root: &Path) -> Result<FileSizes> {
    let manifest = fs::metadata(root.join("manifest.json"))?.len();
    let index = fs::metadata(root.join("index.bin"))?.len();
    let chunks = fs::metadata(root.join("chunks.bin"))?.len();
    let build_stats = fs::metadata(root.join("build_stats.json"))?.len();
    Ok(FileSizes {
        manifest_bytes: manifest,
        index_bytes: index,
        chunks_bytes: chunks,
        build_stats_bytes: build_stats,
        total_bytes: manifest + index + chunks + build_stats,
    })
}

fn cycle_iso(date: &str, cycle: u8) -> String {
    if date.len() == 8 {
        format!(
            "{}-{}-{}T{cycle:02}:00:00Z",
            &date[0..4],
            &date[4..6],
            &date[6..8]
        )
    } else {
        format!("{date}T{cycle:02}:00:00Z")
    }
}
