use anyhow::{Context, Result, anyhow, bail};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use clap::{Parser, ValueEnum};
use rustwx_core::SourceId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

const HRRR_V3_DATE: &str = "2018-07-12";

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-mdt-archive-ingest",
    about = "Build event-scoped HRRR archive metadata and pressure-volume stores for SPC MDT+/HIGH days"
)]
struct Args {
    #[arg(long, default_value = "C:/Users/drew/tor-sound-arch/dataset")]
    dataset_root: PathBuf,
    #[arg(long, default_value = "/data/weather-api/archive")]
    archive_root: PathBuf,
    #[arg(long, default_value = "/data/weather-api/pressure_volume")]
    pressure_volume_root: PathBuf,
    #[arg(long, default_value = "/data/weather-api/cache")]
    cache_dir: PathBuf,
    #[arg(long)]
    pressure_volume_bin: Option<PathBuf>,
    #[arg(long, default_value_t = SourceId::Aws)]
    source: SourceId,
    #[arg(long, value_enum, default_value_t = RankFilter::High)]
    rank: RankFilter,
    #[arg(long, help = "Limit to one convective day, YYYY-MM-DD")]
    event: Option<String>,
    #[arg(long, default_value_t = 0)]
    max_events: usize,
    #[arg(long, default_value_t = 0)]
    max_runs: usize,
    #[arg(long, value_enum, default_value_t = RunKindFilter::Any)]
    kind: RunKindFilter,
    #[arg(long, value_enum, default_value_t = HourSet::Sounding)]
    pressure_hours: HourSet,
    #[arg(
        long,
        help = "Explicit forecast-hour list/ranges for pressure stores, e.g. 0-18 or 0,3,6. Overrides --pressure-hours."
    )]
    pressure_hour_spec: Option<String>,
    #[arg(long, default_value_t = 0.10)]
    buffer_fraction: f64,
    #[arg(long, default_value_t = 150.0)]
    buffer_km: f64,
    #[arg(long, default_value_t = 2)]
    load_parallelism: usize,
    #[arg(long, default_value_t = 4)]
    chunk_z: usize,
    #[arg(long, default_value_t = 64)]
    chunk_y: usize,
    #[arg(long, default_value_t = 64)]
    chunk_x: usize,
    #[arg(
        long,
        help = "Actually build pressure-volume stores. Without this, only metadata/check plan is written."
    )]
    execute: bool,
    #[arg(long)]
    overwrite: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RankFilter {
    High,
    MdtPlus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum HourSet {
    Sounding,
    Plot,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RunKindFilter {
    Any,
    Synoptic,
    Near,
}

#[derive(Debug, Deserialize)]
struct ArchivePlanEntry {
    convective_day: String,
    max_outlook: String,
    peak_iso: String,
    peak_unix: i64,
    tornado_count: usize,
    max_ef: i32,
    sounding_window_iso: Vec<String>,
    synoptic_init_iso: Option<String>,
    synoptic_peak_fhour: Option<i32>,
    synoptic_window_fhours: Option<Vec<i32>>,
    near_init_iso: Option<String>,
    near_peak_fhour: Option<i32>,
    near_window_fhours: Option<Vec<i32>>,
}

#[derive(Debug, Deserialize)]
struct MrglAreaEntry {
    issuance: String,
    issuance_label: String,
    cycle: u8,
    polygon_geojson: Value,
    area_km2: f64,
    centroid: [f64; 2],
}

#[derive(Debug, Clone, Serialize)]
struct Bounds {
    west: f64,
    east: f64,
    south: f64,
    north: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ArchiveRunPlan {
    kind: String,
    init_iso: String,
    date_yyyymmdd: String,
    cycle_utc: u8,
    run_id: String,
    peak_fhour: Option<i32>,
    hrrr_max_fhour: u16,
    pressure_fhours: Vec<u8>,
    plot_fhours: Vec<u16>,
    store_path: String,
    complete: bool,
}

#[derive(Debug, Serialize)]
struct EventMetadata {
    schema: String,
    convective_day: String,
    max_outlook: String,
    peak_iso: String,
    peak_unix: i64,
    tornado_count: usize,
    max_ef: i32,
    sounding_window_iso: Vec<String>,
    mrgl: MrglMetadata,
    bounds: EventBounds,
    runs: Vec<ArchiveRunPlan>,
}

#[derive(Debug, Serialize)]
struct MrglMetadata {
    issuance: String,
    issuance_label: String,
    cycle: u8,
    area_km2: f64,
    centroid: [f64; 2],
}

#[derive(Debug, Serialize)]
struct EventBounds {
    polygon_bounds: Bounds,
    volume_bounds: Bounds,
    buffer_fraction: f64,
    buffer_km: f64,
}

#[derive(Debug, Serialize)]
struct IngestSummary {
    schema: String,
    generated_at_unix_ms: u128,
    rank: String,
    execute: bool,
    event_count: usize,
    run_count: usize,
    built_count: usize,
    skipped_complete_count: usize,
    failed_count: usize,
    events: Vec<EventSummary>,
}

#[derive(Debug, Serialize)]
struct EventSummary {
    convective_day: String,
    max_outlook: String,
    runs: Vec<RunSummary>,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    run_id: String,
    kind: String,
    pressure_fhours: Vec<u8>,
    status: String,
    elapsed_ms: u128,
    store_path: String,
    error: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.buffer_fraction < 0.0 || !args.buffer_fraction.is_finite() {
        bail!("--buffer-fraction must be finite and >= 0");
    }
    if args.buffer_km < 0.0 || !args.buffer_km.is_finite() {
        bail!("--buffer-km must be finite and >= 0");
    }
    if args.load_parallelism == 0 {
        bail!("--load-parallelism must be >= 1");
    }

    let mdt_dates = read_mdt_dates(&args.dataset_root)?;
    let plan = read_archive_plan(&args.dataset_root)?;
    let mrgl = read_mrgl_areas(&args.dataset_root)?;
    let pressure_bin = resolve_pressure_volume_bin(args.pressure_volume_bin.as_ref())?;

    fs::create_dir_all(&args.archive_root)
        .with_context(|| format!("create archive root {}", args.archive_root.display()))?;
    fs::create_dir_all(args.archive_root.join("events"))?;
    fs::create_dir_all(args.archive_root.join("ingest").join("checkpoints"))?;
    fs::create_dir_all(&args.pressure_volume_root).with_context(|| {
        format!(
            "create pressure-volume root {}",
            args.pressure_volume_root.display()
        )
    })?;

    let mut selected = plan
        .iter()
        .filter(|entry| rank_matches(args.rank, &entry.convective_day, &mdt_dates))
        .filter(|entry| {
            args.event
                .as_ref()
                .map(|day| day == &entry.convective_day)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    selected.sort_by(|a, b| a.convective_day.cmp(&b.convective_day));
    if args.max_events > 0 {
        selected.truncate(args.max_events);
    }

    let mut events = Vec::new();
    let mut run_seen = BTreeSet::new();
    let mut run_count = 0usize;
    let mut built_count = 0usize;
    let mut skipped_complete_count = 0usize;
    let mut failed_count = 0usize;

    for event in selected {
        let mrgl_entry = mrgl
            .get(&event.convective_day)
            .ok_or_else(|| anyhow!("missing MRGL polygon for {}", event.convective_day))?;
        let polygon_bounds = polygon_bounds(&mrgl_entry.polygon_geojson)
            .with_context(|| format!("compute MRGL bounds for {}", event.convective_day))?;
        let volume_bounds = expand_bounds(&polygon_bounds, args.buffer_fraction, args.buffer_km);
        let mut runs = build_event_runs(event, &args, &volume_bounds)?;
        let event_dir = args.archive_root.join("events").join(&event.convective_day);
        fs::create_dir_all(&event_dir)?;
        fs::write(
            event_dir.join("mrgl.geojson"),
            serde_json::to_vec_pretty(&mrgl_entry.polygon_geojson)?,
        )?;
        fs::write(
            event_dir.join("volume_mask.geojson"),
            serde_json::to_vec_pretty(&json!({
                "type": "Feature",
                "properties": {
                    "kind": "buffered_mrgl_bbox_v0",
                    "convective_day": event.convective_day,
                    "buffer_fraction": args.buffer_fraction,
                    "buffer_km": args.buffer_km
                },
                "geometry": bbox_polygon(&volume_bounds)
            }))?,
        )?;
        let metadata = EventMetadata {
            schema: "rustwx.hrrr_mdt_archive.event.v1".to_string(),
            convective_day: event.convective_day.clone(),
            max_outlook: event.max_outlook.clone(),
            peak_iso: event.peak_iso.clone(),
            peak_unix: event.peak_unix,
            tornado_count: event.tornado_count,
            max_ef: event.max_ef,
            sounding_window_iso: event.sounding_window_iso.clone(),
            mrgl: MrglMetadata {
                issuance: mrgl_entry.issuance.clone(),
                issuance_label: mrgl_entry.issuance_label.clone(),
                cycle: mrgl_entry.cycle,
                area_km2: mrgl_entry.area_km2,
                centroid: mrgl_entry.centroid,
            },
            bounds: EventBounds {
                polygon_bounds: polygon_bounds.clone(),
                volume_bounds: volume_bounds.clone(),
                buffer_fraction: args.buffer_fraction,
                buffer_km: args.buffer_km,
            },
            runs: runs.clone(),
        };
        fs::write(
            event_dir.join("event.json"),
            serde_json::to_vec_pretty(&metadata)?,
        )?;
        fs::write(
            event_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&metadata)?,
        )?;

        let mut run_summaries = Vec::new();
        for run in runs.drain(..) {
            if args.max_runs > 0 && run_count >= args.max_runs {
                continue;
            }
            if !run_seen.insert(run.run_id.clone()) {
                run_summaries.push(RunSummary {
                    run_id: run.run_id.clone(),
                    kind: run.kind.clone(),
                    pressure_fhours: run.pressure_fhours.clone(),
                    status: "duplicate_cycle_skipped".to_string(),
                    elapsed_ms: 0,
                    store_path: run.store_path.clone(),
                    error: None,
                });
                continue;
            }
            run_count += 1;
            let started = Instant::now();
            let result = ingest_run(&args, &pressure_bin, event, &run, &volume_bounds);
            let elapsed_ms = started.elapsed().as_millis();
            match result {
                Ok(status) => {
                    if status == "complete_skipped" {
                        skipped_complete_count += 1;
                    } else if status == "built" {
                        built_count += 1;
                    }
                    run_summaries.push(RunSummary {
                        run_id: run.run_id.clone(),
                        kind: run.kind.clone(),
                        pressure_fhours: run.pressure_fhours.clone(),
                        status,
                        elapsed_ms,
                        store_path: run.store_path.clone(),
                        error: None,
                    });
                }
                Err(err) => {
                    failed_count += 1;
                    run_summaries.push(RunSummary {
                        run_id: run.run_id.clone(),
                        kind: run.kind.clone(),
                        pressure_fhours: run.pressure_fhours.clone(),
                        status: "failed".to_string(),
                        elapsed_ms,
                        store_path: run.store_path.clone(),
                        error: Some(err.to_string()),
                    });
                }
            }
        }
        events.push(EventSummary {
            convective_day: event.convective_day.clone(),
            max_outlook: event.max_outlook.clone(),
            runs: run_summaries,
        });
    }

    let summary = IngestSummary {
        schema: "rustwx.hrrr_mdt_archive.ingest_summary.v1".to_string(),
        generated_at_unix_ms: chrono::Utc::now().timestamp_millis() as u128,
        rank: format!("{:?}", args.rank).to_ascii_lowercase(),
        execute: args.execute,
        event_count: events.len(),
        run_count,
        built_count,
        skipped_complete_count,
        failed_count,
        events,
    };
    let summary_path = args.archive_root.join("ingest").join(if args.execute {
        "last_execute_summary.json"
    } else {
        "last_plan_summary.json"
    });
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if failed_count > 0 {
        bail!("{failed_count} archive run(s) failed");
    }
    Ok(())
}

fn ingest_run(
    args: &Args,
    pressure_bin: &Path,
    event: &ArchivePlanEntry,
    run: &ArchiveRunPlan,
    bounds: &Bounds,
) -> Result<String> {
    let run_root = args
        .pressure_volume_root
        .join("hrrr_archive")
        .join(&run.run_id);
    let store = run_root.join("store");
    if pressure_store_complete(&store) && !args.overwrite {
        write_checkpoint(args, event, run, "complete_skipped", None)?;
        return Ok("complete_skipped".to_string());
    }
    if !args.execute {
        write_checkpoint(args, event, run, "planned", None)?;
        return Ok("planned".to_string());
    }
    fs::create_dir_all(&run_root)?;
    let hours = run
        .pressure_fhours
        .iter()
        .map(|hour| hour.to_string())
        .collect::<Vec<_>>()
        .join(",");
    if hours.is_empty() {
        write_checkpoint(args, event, run, "no_hours", None)?;
        return Ok("no_hours".to_string());
    }

    let status = Command::new(pressure_bin)
        .arg("--model")
        .arg("hrrr")
        .arg("--domain")
        .arg(format!(
            "hrrr_archive_{}_{}",
            event.convective_day, run.kind
        ))
        .arg("--date")
        .arg(&run.date_yyyymmdd)
        .arg("--cycle")
        .arg(run.cycle_utc.to_string())
        .arg("--hours")
        .arg(hours)
        .arg("--source")
        .arg(args.source.as_str())
        .arg(format!("--west={}", bounds.west))
        .arg(format!("--east={}", bounds.east))
        .arg(format!("--south={}", bounds.south))
        .arg(format!("--north={}", bounds.north))
        .arg("--cache-dir")
        .arg(&args.cache_dir)
        .arg("--out-dir")
        .arg(&run_root)
        .arg("--load-parallelism")
        .arg(args.load_parallelism.to_string())
        .arg("--chunk-z")
        .arg(args.chunk_z.to_string())
        .arg("--chunk-y")
        .arg(args.chunk_y.to_string())
        .arg("--chunk-x")
        .arg(args.chunk_x.to_string())
        .status()
        .with_context(|| format!("spawn {}", pressure_bin.display()))?;
    if !status.success() {
        let message = format!("pressure-volume build exited with {status}");
        write_checkpoint(args, event, run, "failed", Some(&message))?;
        bail!(message);
    }
    write_checkpoint(args, event, run, "built", None)?;
    Ok("built".to_string())
}

fn write_checkpoint(
    args: &Args,
    event: &ArchivePlanEntry,
    run: &ArchiveRunPlan,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    let dir = args
        .archive_root
        .join("ingest")
        .join("checkpoints")
        .join(&event.convective_day);
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join(format!("{}.json", run.run_id)),
        serde_json::to_vec_pretty(&json!({
            "schema": "rustwx.hrrr_mdt_archive.checkpoint.v1",
            "convective_day": event.convective_day,
            "run_id": run.run_id,
            "kind": run.kind,
            "init_iso": run.init_iso,
            "pressure_fhours": run.pressure_fhours,
            "status": status,
            "error": error,
            "updated_at": chrono::Utc::now().to_rfc3339(),
        }))?,
    )?;
    Ok(())
}

fn build_event_runs(
    event: &ArchivePlanEntry,
    args: &Args,
    _bounds: &Bounds,
) -> Result<Vec<ArchiveRunPlan>> {
    let mut runs = Vec::new();
    if matches!(args.kind, RunKindFilter::Any | RunKindFilter::Synoptic) {
        if let Some(init) = event.synoptic_init_iso.as_deref() {
            let max = synoptic_max_fhour(init);
            runs.push(build_run_plan(
                event,
                "synoptic",
                init,
                event.synoptic_peak_fhour,
                event.synoptic_window_fhours.as_deref().unwrap_or(&[]),
                max,
                args,
            )?);
        }
    }
    if matches!(args.kind, RunKindFilter::Any | RunKindFilter::Near) {
        if let Some(init) = event.near_init_iso.as_deref() {
            let max = near_max_fhour(init);
            runs.push(build_run_plan(
                event,
                "near",
                init,
                event.near_peak_fhour,
                event.near_window_fhours.as_deref().unwrap_or(&[]),
                max,
                args,
            )?);
        }
    }
    Ok(runs)
}

fn build_run_plan(
    event: &ArchivePlanEntry,
    kind: &str,
    init_iso: &str,
    peak_fhour: Option<i32>,
    sounding_fhours: &[i32],
    hrrr_max_fhour: u16,
    args: &Args,
) -> Result<ArchiveRunPlan> {
    let (date_yyyymmdd, cycle_utc) = parse_init_iso(init_iso)?;
    let plot_fhours = match kind {
        "synoptic" => (0..=hrrr_max_fhour).collect::<Vec<_>>(),
        _ => (0..=hrrr_max_fhour.min(24)).collect::<Vec<_>>(),
    };
    let mut pressure_fhours = BTreeSet::new();
    if let Some(spec) = args.pressure_hour_spec.as_deref() {
        pressure_fhours.extend(parse_hour_spec(spec, hrrr_max_fhour)?);
    } else if matches!(args.pressure_hours, HourSet::Sounding | HourSet::Both) {
        pressure_fhours.extend(
            sounding_fhours
                .iter()
                .copied()
                .filter(|hour| *hour >= 0 && *hour <= i32::from(hrrr_max_fhour))
                .map(|hour| hour as u8),
        );
    }
    if args.pressure_hour_spec.is_none()
        && matches!(args.pressure_hours, HourSet::Plot | HourSet::Both)
    {
        pressure_fhours.extend(
            plot_fhours
                .iter()
                .copied()
                .filter(|hour| *hour <= hrrr_max_fhour)
                .map(|hour| hour as u8),
        );
    }
    let run_id = format!(
        "{}_{}_{}_{:02}z",
        event.convective_day, kind, date_yyyymmdd, cycle_utc
    );
    let store_path = args
        .pressure_volume_root
        .join("hrrr_archive")
        .join(&run_id)
        .join("store");
    Ok(ArchiveRunPlan {
        kind: kind.to_string(),
        init_iso: init_iso.to_string(),
        date_yyyymmdd,
        cycle_utc,
        run_id,
        peak_fhour,
        hrrr_max_fhour,
        pressure_fhours: pressure_fhours.into_iter().collect(),
        plot_fhours,
        complete: pressure_store_complete(&store_path),
        store_path: store_path.display().to_string(),
    })
}

fn rank_matches(filter: RankFilter, day: &str, ranks: &BTreeMap<String, u8>) -> bool {
    let Some(rank) = ranks.get(day).copied() else {
        return false;
    };
    match filter {
        RankFilter::High => rank >= 6,
        RankFilter::MdtPlus => rank >= 5,
    }
}

fn read_mdt_dates(root: &Path) -> Result<BTreeMap<String, u8>> {
    let path = root.join("mdt_plus_dates.json");
    let value: Map<String, Value> = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )?;
    value
        .into_iter()
        .map(|(day, rank)| {
            let rank = rank
                .as_u64()
                .ok_or_else(|| anyhow!("invalid rank for {day}"))?;
            Ok((day, rank as u8))
        })
        .collect()
}

fn read_archive_plan(root: &Path) -> Result<Vec<ArchivePlanEntry>> {
    let path = root.join("archive_plan_v2.json");
    serde_json::from_slice(&fs::read(&path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

fn read_mrgl_areas(root: &Path) -> Result<BTreeMap<String, MrglAreaEntry>> {
    let path = root.join("mrgl_areas.json");
    serde_json::from_slice(&fs::read(&path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

fn parse_init_iso(init_iso: &str) -> Result<(String, u8)> {
    let trimmed = init_iso.trim_end_matches('Z');
    let dt = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M")
        .with_context(|| format!("parse init_iso {init_iso}"))?;
    Ok((
        format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day()),
        dt.hour() as u8,
    ))
}

fn synoptic_max_fhour(init_iso: &str) -> u16 {
    if init_iso.get(..10).unwrap_or("") >= HRRR_V3_DATE {
        48
    } else {
        36
    }
}

fn near_max_fhour(init_iso: &str) -> u16 {
    let Ok((date, cycle)) = parse_init_iso(init_iso) else {
        return 18;
    };
    if date.as_str() < "20201202" || (date.as_str() == "20201202" && cycle < 12) {
        18
    } else {
        24
    }
}

fn parse_hour_spec(spec: &str, max_fhour: u16) -> Result<Vec<u8>> {
    let mut hours = BTreeSet::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = part.split_once('-') {
            let start = start
                .trim()
                .parse::<u16>()
                .with_context(|| format!("parse pressure-hour range start in {part}"))?;
            let end = end
                .trim()
                .parse::<u16>()
                .with_context(|| format!("parse pressure-hour range end in {part}"))?;
            if end < start {
                bail!("invalid descending pressure-hour range {part}");
            }
            hours.extend(start..=end);
        } else {
            hours.insert(
                part.parse::<u16>()
                    .with_context(|| format!("parse pressure hour {part}"))?,
            );
        }
    }
    let hours = hours
        .into_iter()
        .filter(|hour| *hour <= max_fhour)
        .map(|hour| hour as u8)
        .collect::<Vec<_>>();
    if hours.is_empty() {
        bail!("pressure-hour spec {spec:?} produced no hours <= f{max_fhour:03}");
    }
    Ok(hours)
}

fn polygon_bounds(geometry: &Value) -> Result<Bounds> {
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    collect_positions(geometry, &mut |lon, lat| {
        if lon.is_finite() && lat.is_finite() {
            west = west.min(lon);
            east = east.max(lon);
            south = south.min(lat);
            north = north.max(lat);
        }
    });
    if !west.is_finite() || !east.is_finite() || !south.is_finite() || !north.is_finite() {
        bail!("polygon contained no finite coordinates");
    }
    Ok(Bounds {
        west,
        east,
        south,
        north,
    })
}

fn collect_positions(value: &Value, f: &mut impl FnMut(f64, f64)) {
    match value {
        Value::Array(values) => {
            if values.len() >= 2 && values[0].as_f64().is_some() && values[1].as_f64().is_some() {
                f(values[0].as_f64().unwrap(), values[1].as_f64().unwrap());
            } else {
                for child in values {
                    collect_positions(child, f);
                }
            }
        }
        Value::Object(map) => {
            if let Some(coordinates) = map.get("coordinates") {
                collect_positions(coordinates, f);
            } else if let Some(geometry) = map.get("geometry") {
                collect_positions(geometry, f);
            }
        }
        _ => {}
    }
}

fn expand_bounds(bounds: &Bounds, fraction: f64, buffer_km: f64) -> Bounds {
    let width = (bounds.east - bounds.west).abs().max(0.1);
    let height = (bounds.north - bounds.south).abs().max(0.1);
    let center_lat = ((bounds.north + bounds.south) * 0.5).to_radians();
    let lat_buffer_deg = buffer_km / 111.0;
    let lon_buffer_deg = buffer_km / (111.0 * center_lat.cos().abs().max(0.25));
    let xpad = (width * fraction).max(lon_buffer_deg);
    let ypad = (height * fraction).max(lat_buffer_deg);
    Bounds {
        west: (bounds.west - xpad).max(-130.0),
        east: (bounds.east + xpad).min(-60.0),
        south: (bounds.south - ypad).max(20.0),
        north: (bounds.north + ypad).min(55.0),
    }
}

fn bbox_polygon(bounds: &Bounds) -> Value {
    json!({
        "type": "Polygon",
        "coordinates": [[
            [bounds.west, bounds.south],
            [bounds.east, bounds.south],
            [bounds.east, bounds.north],
            [bounds.west, bounds.north],
            [bounds.west, bounds.south],
        ]]
    })
}

fn pressure_store_complete(store: &Path) -> bool {
    store.join("manifest.json").is_file()
        && store.join("index.bin").is_file()
        && store.join("chunks.bin").is_file()
}

fn resolve_pressure_volume_bin(explicit: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.clone());
    }
    let exe = std::env::current_exe()?;
    let dir = exe
        .parent()
        .ok_or_else(|| anyhow!("current executable has no parent directory"))?;
    let stem = if cfg!(windows) {
        "hrrr_pressure_volume_store.exe"
    } else {
        "hrrr_pressure_volume_store"
    };
    Ok(dir.join(stem))
}

#[allow(dead_code)]
fn parse_day(day: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(day, "%Y-%m-%d").with_context(|| format!("parse day {day}"))
}
