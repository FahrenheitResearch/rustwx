use anyhow::{Context, Result};
use clap::Parser;
use rustwx_products::volume_store::{
    ChunkCodec, ChunkShape, GridSpec, RouteDef, VolumeFieldProvider, VolumeManifest, VolumeStore,
    VolumeVariable, write_volume_store,
};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "volume-store-profile",
    about = "Profile the synthetic rustwx VolumeStore read/write path"
)]
struct Args {
    #[arg(long, default_value = "proof/volume_store_profile")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 240)]
    nx: usize,
    #[arg(long, default_value_t = 160)]
    ny: usize,
    #[arg(long, default_value_t = 13)]
    hours: u8,
    #[arg(long, default_value_t = 10)]
    levels: usize,
    #[arg(long, default_value_t = 1)]
    chunk_t: usize,
    #[arg(long, default_value_t = 4)]
    chunk_z: usize,
    #[arg(long, default_value_t = 64)]
    chunk_y: usize,
    #[arg(long, default_value_t = 64)]
    chunk_x: usize,
    #[arg(long, default_value_t = 250)]
    point_iterations: usize,
    #[arg(long, default_value_t = 4)]
    route_iterations: usize,
    #[arg(long, default_value_t = 12.0)]
    route_spacing_km: f32,
}

struct SyntheticProvider<'a> {
    levels_hpa: &'a [u16],
    nx: usize,
    ny: usize,
}

impl VolumeFieldProvider for SyntheticProvider<'_> {
    fn field_plane(
        &mut self,
        variable: &str,
        forecast_hour: u8,
        level_hpa: u16,
    ) -> rustwx_products::volume_store::VolumeResult<Vec<f32>> {
        synthetic_plane(
            variable,
            forecast_hour,
            level_hpa,
            self.levels_hpa,
            self.nx,
            self.ny,
        )
    }
}

#[derive(Debug, Serialize)]
struct ProfileReport {
    profile: ProfileConfig,
    files: FileSizes,
    build: BuildProfile,
    open: TimedCount,
    point_sampling: TimedCount,
    route_sampling: TimedCount,
    route: RouteProfile,
    estimates: Estimates,
    artifacts: Artifacts,
}

#[derive(Debug, Serialize)]
struct ProfileConfig {
    nx: usize,
    ny: usize,
    grid_cells: usize,
    variables: Vec<String>,
    forecast_hours: Vec<u8>,
    levels_hpa: Vec<u16>,
    chunk_shape: ChunkShape,
    codec: String,
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
struct BuildProfile {
    elapsed_ms_wall: u128,
    elapsed_ms_writer: u64,
    logical_values: u64,
    raw_f32_bytes: u64,
    raw_i16_bytes: u64,
    payload_bytes: u64,
    chunk_count: usize,
    values_per_second: f64,
}

#[derive(Debug, Serialize)]
struct TimedCount {
    elapsed_ms: u128,
    values: usize,
    values_per_second: f64,
}

#[derive(Debug, Serialize)]
struct RouteProfile {
    sample_count: usize,
    values_per_route: usize,
}

#[derive(Debug, Serialize)]
struct Estimates {
    writer_working_slab_bytes: u64,
    writer_working_slab_mib: f64,
    ca_220k_49h_25lev_5var_raw_f32_gib: f64,
    ca_220k_49h_25lev_5var_raw_i16_gib: f64,
    ca_220k_49h_25lev_5var_payload_gib_at_observed_ratio: f64,
}

#[derive(Debug, Serialize)]
struct Artifacts {
    store_dir: String,
    summary_json: String,
    summary_md: String,
    flow_svg: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;
    let store_dir = args.out_dir.join("store");
    if store_dir.exists() {
        fs::remove_dir_all(&store_dir)
            .with_context(|| format!("remove old store at {}", store_dir.display()))?;
    }
    fs::create_dir_all(&store_dir)?;

    let levels_hpa = pressure_levels(args.levels);
    let forecast_hours = (0..args.hours).collect::<Vec<_>>();
    let variables = ["TMP", "SPFH", "UGRD", "VGRD", "HGT"]
        .into_iter()
        .map(|name| VolumeVariable::new(name, label_for_var(name), units_for_var(name)))
        .collect::<Vec<_>>();
    let chunk_shape = ChunkShape {
        t: args.chunk_t,
        z: args.chunk_z,
        y: args.chunk_y,
        x: args.chunk_x,
    };
    let grid = GridSpec::RegularLatLon {
        nx: args.nx,
        ny: args.ny,
        west_lon_deg: -124.5,
        east_lon_deg: -113.5,
        south_lat_deg: 31.5,
        north_lat_deg: 42.5,
    };
    let manifest = VolumeManifest {
        format: "rustwx-volume-store-v0".to_string(),
        model: "synthetic-hrrr-ca-proxy".to_string(),
        domain: "california-proxy".to_string(),
        product: "pressure".to_string(),
        cycle: "2026-04-28T00:00:00Z".to_string(),
        forecast_hours: forecast_hours.clone(),
        variables: variables.clone(),
        levels_hpa: levels_hpa.clone(),
        chunk_shape,
        codec: ChunkCodec::AffineI16RawV0.name().to_string(),
        grid,
    };

    let build_start = Instant::now();
    let provider = SyntheticProvider {
        levels_hpa: &levels_hpa,
        nx: args.nx,
        ny: args.ny,
    };
    let stats = write_volume_store(&store_dir, &manifest, provider)?;
    let build_elapsed = build_start.elapsed();

    let open_start = Instant::now();
    let store = VolumeStore::open(&store_dir)?;
    let open_elapsed = open_start.elapsed();

    let point_start = Instant::now();
    let mut point_values = 0usize;
    for iter in 0..args.point_iterations {
        let frac = if args.point_iterations <= 1 {
            0.5
        } else {
            iter as f64 / (args.point_iterations - 1) as f64
        };
        let lat = 32.5 + 8.7 * frac;
        let lon = -123.5
            + 8.9
                * ((iter * 37 % args.point_iterations.max(1)) as f64
                    / args.point_iterations.max(1) as f64);
        let profile = store.sample_point_3d(
            lat,
            lon,
            &["TMP", "SPFH", "UGRD", "VGRD", "HGT"],
            &forecast_hours,
            &levels_hpa,
        )?;
        point_values += profile.samples.len();
    }
    let point_elapsed = point_start.elapsed();

    let route = RouteDef {
        id: "ca_proxy_sw_ne".to_string(),
        name: "CA proxy southwest-northeast route".to_string(),
        points: vec![(32.3, -123.7), (41.5, -114.2)],
        sample_spacing_km: args.route_spacing_km,
    };
    let route_start = Instant::now();
    let mut route_values = 0usize;
    let mut route_sample_count = 0usize;
    for _ in 0..args.route_iterations {
        let section = store.sample_route_3d(
            &route,
            &["TMP", "SPFH", "UGRD", "VGRD", "HGT"],
            6,
            &levels_hpa,
        )?;
        route_sample_count = section.route_samples.len();
        route_values += section.values.len();
    }
    let route_elapsed = route_start.elapsed();

    let files = file_sizes(&store_dir)?;
    let logical_values =
        (variables.len() * forecast_hours.len() * levels_hpa.len() * args.nx * args.ny) as u64;
    let observed_payload_ratio = stats.payload_bytes as f64 / stats.raw_i16_bytes.max(1) as f64;
    let ca_raw_i16 = 220_000.0 * 49.0 * 25.0 * 5.0 * 2.0;
    let ca_raw_f32 = ca_raw_i16 * 2.0;

    let summary_json = args.out_dir.join("summary.json");
    let summary_md = args.out_dir.join("summary.md");
    let flow_svg = args.out_dir.join("volume_store_flow.svg");
    let report = ProfileReport {
        profile: ProfileConfig {
            nx: args.nx,
            ny: args.ny,
            grid_cells: args.nx * args.ny,
            variables: variables.iter().map(|var| var.name.clone()).collect(),
            forecast_hours,
            levels_hpa,
            chunk_shape,
            codec: manifest.codec.clone(),
        },
        files,
        build: BuildProfile {
            elapsed_ms_wall: build_elapsed.as_millis(),
            elapsed_ms_writer: stats.elapsed_ms,
            logical_values,
            raw_f32_bytes: stats.raw_f32_bytes,
            raw_i16_bytes: stats.raw_i16_bytes,
            payload_bytes: stats.payload_bytes,
            chunk_count: stats.chunk_count,
            values_per_second: per_second(logical_values as usize, build_elapsed.as_secs_f64()),
        },
        open: TimedCount {
            elapsed_ms: open_elapsed.as_millis(),
            values: 1,
            values_per_second: per_second(1, open_elapsed.as_secs_f64()),
        },
        point_sampling: TimedCount {
            elapsed_ms: point_elapsed.as_millis(),
            values: point_values,
            values_per_second: per_second(point_values, point_elapsed.as_secs_f64()),
        },
        route_sampling: TimedCount {
            elapsed_ms: route_elapsed.as_millis(),
            values: route_values,
            values_per_second: per_second(route_values, route_elapsed.as_secs_f64()),
        },
        route: RouteProfile {
            sample_count: route_sample_count,
            values_per_route: if args.route_iterations == 0 {
                0
            } else {
                route_values / args.route_iterations
            },
        },
        estimates: Estimates {
            writer_working_slab_bytes: (args.chunk_t
                * args.chunk_z
                * args.nx
                * args.ny
                * std::mem::size_of::<f32>()) as u64,
            writer_working_slab_mib: (args.chunk_t
                * args.chunk_z
                * args.nx
                * args.ny
                * std::mem::size_of::<f32>()) as f64
                / 1024.0
                / 1024.0,
            ca_220k_49h_25lev_5var_raw_f32_gib: ca_raw_f32 / 1024.0 / 1024.0 / 1024.0,
            ca_220k_49h_25lev_5var_raw_i16_gib: ca_raw_i16 / 1024.0 / 1024.0 / 1024.0,
            ca_220k_49h_25lev_5var_payload_gib_at_observed_ratio: ca_raw_i16
                * observed_payload_ratio
                / 1024.0
                / 1024.0
                / 1024.0,
        },
        artifacts: Artifacts {
            store_dir: store_dir.display().to_string(),
            summary_json: summary_json.display().to_string(),
            summary_md: summary_md.display().to_string(),
            flow_svg: flow_svg.display().to_string(),
        },
    };

    fs::write(&summary_json, serde_json::to_vec_pretty(&report)?)?;
    fs::write(&summary_md, markdown_summary(&report))?;
    fs::write(&flow_svg, flow_svg_text())?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn pressure_levels(count: usize) -> Vec<u16> {
    let canonical = [
        1000, 975, 950, 925, 900, 875, 850, 800, 750, 700, 650, 600, 550, 500, 450, 400, 350, 300,
        250, 200, 150, 100, 75, 50, 25,
    ];
    canonical
        .into_iter()
        .take(count.min(canonical.len()))
        .collect()
}

fn synthetic_plane(
    var: &str,
    hour: u8,
    level: u16,
    levels: &[u16],
    nx: usize,
    ny: usize,
) -> rustwx_products::volume_store::VolumeResult<Vec<f32>> {
    let level_index = levels
        .iter()
        .position(|candidate| *candidate == level)
        .unwrap_or(0) as f32;
    let mut values = Vec::with_capacity(nx * ny);
    for y in 0..ny {
        let yf = y as f32 / (ny.saturating_sub(1).max(1)) as f32;
        for x in 0..nx {
            let xf = x as f32 / (nx.saturating_sub(1).max(1)) as f32;
            let wave = ((xf * 9.0 + yf * 5.0 + f32::from(hour) * 0.35).sin()
                + (xf * 3.0 - yf * 7.0).cos())
                * 0.5;
            let value = match var {
                "TMP" => 34.0 - level_index * 5.8 - f32::from(hour) * 0.18 - yf * 11.0 + wave * 3.0,
                "SPFH" => {
                    let base = 0.016 * (-level_index * 0.19).exp();
                    (base * (1.0 + 0.22 * wave + 0.12 * yf)).max(0.00001)
                }
                "UGRD" => 3.5 + level_index * 0.6 + xf * 18.0 + wave * 2.0,
                "VGRD" => -4.0 + yf * 14.0 - level_index * 0.35 + wave,
                "HGT" => 120.0 + level_index * 610.0 + yf * 460.0 + xf * 130.0 + wave * 18.0,
                _ => f32::NAN,
            };
            values.push(value);
        }
    }
    Ok(values)
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

fn per_second(count: usize, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        0.0
    } else {
        count as f64 / seconds
    }
}

fn label_for_var(name: &str) -> &'static str {
    match name {
        "TMP" => "Temperature",
        "SPFH" => "Specific humidity",
        "UGRD" => "U wind",
        "VGRD" => "V wind",
        "HGT" => "Geopotential height",
        _ => "Unknown",
    }
}

fn units_for_var(name: &str) -> &'static str {
    match name {
        "TMP" => "degC",
        "SPFH" => "kg/kg",
        "UGRD" | "VGRD" => "m/s",
        "HGT" => "m",
        _ => "unknown",
    }
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

fn markdown_summary(report: &ProfileReport) -> String {
    format!(
        r#"# VolumeStore Synthetic Profile

## Shape

- Grid: `{nx} x {ny}` = `{cells}` cells
- Variables: `{vars}`
- Forecast hours: `{hours}`
- Levels: `{levels}`
- Chunk shape: `[t={ct}, z={cz}, y={cy}, x={cx}]`
- Codec: `{codec}`

## Build

- Wall time: `{build_ms}` ms
- Logical values: `{logical_values}`
- Raw f32: `{raw_f32:.1}` MiB
- Raw i16: `{raw_i16:.1}` MiB
- Payload: `{payload:.1}` MiB
- Index: `{index:.2}` MiB
- Chunks: `{chunks:.1}` MiB
- Build throughput: `{build_vps:.0}` values/s

## Sampling

- Open store: `{open_ms}` ms
- Point sampling: `{point_values}` values in `{point_ms}` ms (`{point_vps:.0}` values/s)
- Route samples: `{route_samples}`
- Route sampling: `{route_values}` values in `{route_ms}` ms (`{route_vps:.0}` values/s)

## Resource Estimates

- Writer working slab estimate: `{slab_mib:.2}` MiB
- CA 220k cells, 49h, 25 levels, 5 vars raw f32: `{ca_f32:.2}` GiB
- Same raw i16: `{ca_i16:.2}` GiB
- Same payload at this raw-v0 observed ratio: `{ca_payload:.2}` GiB

Note: this run uses `affine_i16_raw_v0`, not zstd compression yet.
"#,
        nx = report.profile.nx,
        ny = report.profile.ny,
        cells = report.profile.grid_cells,
        vars = report.profile.variables.join(", "),
        hours = report.profile.forecast_hours.len(),
        levels = report.profile.levels_hpa.len(),
        ct = report.profile.chunk_shape.t,
        cz = report.profile.chunk_shape.z,
        cy = report.profile.chunk_shape.y,
        cx = report.profile.chunk_shape.x,
        codec = report.profile.codec,
        build_ms = report.build.elapsed_ms_wall,
        logical_values = report.build.logical_values,
        raw_f32 = mb(report.build.raw_f32_bytes),
        raw_i16 = mb(report.build.raw_i16_bytes),
        payload = mb(report.build.payload_bytes),
        index = mb(report.files.index_bytes),
        chunks = mb(report.files.chunks_bytes),
        build_vps = report.build.values_per_second,
        open_ms = report.open.elapsed_ms,
        point_values = report.point_sampling.values,
        point_ms = report.point_sampling.elapsed_ms,
        point_vps = report.point_sampling.values_per_second,
        route_samples = report.route.sample_count,
        route_values = report.route_sampling.values,
        route_ms = report.route_sampling.elapsed_ms,
        route_vps = report.route_sampling.values_per_second,
        slab_mib = report.estimates.writer_working_slab_mib,
        ca_f32 = report.estimates.ca_220k_49h_25lev_5var_raw_f32_gib,
        ca_i16 = report.estimates.ca_220k_49h_25lev_5var_raw_i16_gib,
        ca_payload = report
            .estimates
            .ca_220k_49h_25lev_5var_payload_gib_at_observed_ratio,
    )
}

fn flow_svg_text() -> &'static str {
    r##"<svg xmlns="http://www.w3.org/2000/svg" width="1180" height="360" viewBox="0 0 1180 360">
  <rect width="1180" height="360" fill="#11110f"/>
  <style>
    text { font-family: Inter, Segoe UI, Arial, sans-serif; fill: #f5efe2; }
    .box { fill: #1e1c18; stroke: #8b6f45; stroke-width: 2; rx: 8; }
    .accent { fill: #2b1812; stroke: #df7f45; }
    .small { fill: #c9bdab; font-size: 15px; }
    .title { font-size: 22px; font-weight: 700; }
    .label { font-size: 17px; font-weight: 650; }
    .arrow { stroke: #d9ad62; stroke-width: 3; marker-end: url(#arrow); }
  </style>
  <defs>
    <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
      <path d="M0,0 L0,6 L9,3 z" fill="#d9ad62"/>
    </marker>
  </defs>
  <text x="40" y="44" class="title">rustwx VolumeStore v0: decode once, serve many products</text>
  <rect x="40" y="90" width="180" height="120" class="box"/>
  <text x="62" y="128" class="label">Primitive Fields</text>
  <text x="62" y="158" class="small">TMP / SPFH</text>
  <text x="62" y="180" class="small">UGRD / VGRD / HGT</text>
  <line x1="230" y1="150" x2="315" y2="150" class="arrow"/>
  <rect x="330" y="90" width="200" height="120" class="box accent"/>
  <text x="354" y="128" class="label">Chunk Writer</text>
  <text x="354" y="158" class="small">[hour, level, y, x]</text>
  <text x="354" y="180" class="small">affine i16 per chunk</text>
  <line x1="540" y1="150" x2="625" y2="150" class="arrow"/>
  <rect x="640" y="66" width="210" height="74" class="box"/>
  <text x="665" y="106" class="label">index.bin</text>
  <text x="665" y="128" class="small">direct chunk lookup</text>
  <rect x="640" y="166" width="210" height="74" class="box"/>
  <text x="665" y="206" class="label">chunks.bin</text>
  <text x="665" y="228" class="small">quantized payloads</text>
  <line x1="860" y1="150" x2="945" y2="150" class="arrow"/>
  <rect x="960" y="90" width="180" height="120" class="box accent"/>
  <text x="984" y="128" class="label">Reader/Sampler</text>
  <text x="984" y="158" class="small">point profiles</text>
  <text x="984" y="180" class="small">route sections</text>
  <text x="68" y="285" class="small">Derived products stay lazy: RH, VPD, theta-e, wet bulb, wind speed, moisture transport.</text>
  <text x="68" y="312" class="small">Current profile uses raw affine i16; zstd1 is the next codec bolt-on.</text>
</svg>
"##
}
