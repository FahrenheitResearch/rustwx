use clap::{Parser, ValueEnum};
use image::{Rgba, RgbaImage};
use rayon::prelude::*;
use rustwx_products::satellite::{
    compose_goes_abi_rgb_pixel, lat_lon_to_scan_angles_fast, read_goes_abi_field, GoesAbiField,
    GoesAbiRgbCompositeStyle,
};
use rustwx_render::{save_rgba_png_profile_with_options, PngCompressionMode, PngWriteOptions};
use serde::Serialize;
use std::error::Error;
use std::f64::consts::PI;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(about = "Render GOES ABI GeoColor as transparent XYZ Web Mercator tiles")]
struct Args {
    #[arg(long)]
    channel1: PathBuf,
    #[arg(long)]
    channel2: PathBuf,
    #[arg(long)]
    channel3: PathBuf,
    #[arg(long)]
    channel13: Option<PathBuf>,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value = "goes_geocolor_webmercator")]
    name: String,
    #[arg(long, default_value_t = -165.0)]
    west: f64,
    #[arg(long, default_value_t = -5.0)]
    east: f64,
    #[arg(long, default_value_t = -70.0)]
    south: f64,
    #[arg(long, default_value_t = 70.0)]
    north: f64,
    #[arg(long, default_value_t = 2)]
    min_zoom: u8,
    #[arg(long, default_value_t = 5)]
    max_zoom: u8,
    #[arg(long, default_value_t = 256)]
    tile_size: u32,
    #[arg(long, default_value_t = 0.82)]
    opacity: f64,
    /// For cloud overlays, make retained cloud pixels fully opaque before layer opacity is applied.
    #[arg(long)]
    opaque_clouds: bool,
    #[arg(long, value_enum, default_value_t = LayerModeArg::Geocolor)]
    layer: LayerModeArg,
    #[arg(long)]
    base_url: Option<String>,
    #[arg(long, value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LayerModeArg {
    Geocolor,
    Clouds,
}

impl From<PngCompressionArg> for PngCompressionMode {
    fn from(value: PngCompressionArg) -> Self {
        match value {
            PngCompressionArg::Default => Self::Default,
            PngCompressionArg::Fast => Self::Fast,
            PngCompressionArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Bounds {
    west: f64,
    east: f64,
    south: f64,
    north: f64,
}

#[derive(Debug)]
struct ChannelSampler {
    field: GoesAbiField,
}

#[derive(Debug)]
struct Samplers {
    c01: ChannelSampler,
    c02: ChannelSampler,
    c03: ChannelSampler,
    c13: Option<ChannelSampler>,
}

#[derive(Debug, Serialize)]
struct TileManifest {
    ok: bool,
    name: String,
    out_dir: PathBuf,
    bounds: [f64; 4],
    minzoom: u8,
    maxzoom: u8,
    tile_size: u32,
    opacity: f64,
    opaque_clouds: bool,
    tile_count: usize,
    skipped_empty_tiles: usize,
    total_ms: u128,
    source_files: SourceFiles,
    scan_time_utc: String,
    tilejson_path: PathBuf,
    demo_path: PathBuf,
    tiles: Vec<TileRecord>,
}

#[derive(Debug, Serialize)]
struct SourceFiles {
    channel1: PathBuf,
    channel2: PathBuf,
    channel3: PathBuf,
}

#[derive(Debug, Serialize)]
struct TileRecord {
    z: u8,
    x: u32,
    y: u32,
    path: PathBuf,
    nontransparent_pixels: u32,
}

#[derive(Debug, Serialize)]
struct TileJson {
    tilejson: String,
    name: String,
    version: String,
    scheme: String,
    tiles: Vec<String>,
    minzoom: u8,
    maxzoom: u8,
    bounds: [f64; 4],
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let started = Instant::now();
    validate_args(&args)?;
    fs::create_dir_all(&args.out_dir)?;

    let bounds = Bounds {
        west: args.west,
        east: args.east,
        south: args.south,
        north: args.north,
    };
    let samplers = Samplers {
        c01: ChannelSampler {
            field: read_goes_abi_field(&args.channel1, "CMI")?,
        },
        c02: ChannelSampler {
            field: read_goes_abi_field(&args.channel2, "CMI")?,
        },
        c03: ChannelSampler {
            field: read_goes_abi_field(&args.channel3, "CMI")?,
        },
        c13: args
            .channel13
            .as_ref()
            .map(|path| {
                read_goes_abi_field(path, "CMI").map(|field| ChannelSampler { field })
            })
            .transpose()?,
    };
    let compression = args.png_compression.into();
    let jobs = tile_jobs(bounds, args.min_zoom, args.max_zoom)?;
    let records = jobs
        .par_iter()
        .map(|&(z, x, y)| {
            render_tile(
                &samplers,
                bounds,
                z,
                x,
                y,
                args.tile_size,
                args.opacity,
                args.opaque_clouds,
                args.layer,
                &args.out_dir,
                compression,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| boxed_error(err.to_string()))?;

    let skipped_empty_tiles = records.iter().filter(|record| record.is_none()).count();
    let tiles = records.into_iter().flatten().collect::<Vec<_>>();
    let tilejson_path = args.out_dir.join("tilejson.json");
    let tile_url = args
        .base_url
        .as_deref()
        .map(|base| {
            format!(
                "{}/{}/{}/{}.png",
                base.trim_end_matches('/'),
                "{z}",
                "{x}",
                "{y}"
            )
        })
        .unwrap_or_else(|| "{z}/{x}/{y}.png".to_string());
    let tilejson = TileJson {
        tilejson: "3.0.0".to_string(),
        name: args.name.clone(),
        version: "1.0.0".to_string(),
        scheme: "xyz".to_string(),
        tiles: vec![tile_url],
        minzoom: args.min_zoom,
        maxzoom: args.max_zoom,
        bounds: [bounds.west, bounds.south, bounds.east, bounds.north],
    };
    atomic_write_json(&tilejson_path, &tilejson)?;

    let demo_path = args.out_dir.join("index.html");
    write_demo_html(&demo_path, &args.name, bounds, args.min_zoom, args.max_zoom)?;

    let manifest = TileManifest {
        ok: true,
        name: args.name,
        out_dir: args.out_dir.clone(),
        bounds: [bounds.west, bounds.south, bounds.east, bounds.north],
        minzoom: args.min_zoom,
        maxzoom: args.max_zoom,
        tile_size: args.tile_size,
        opacity: args.opacity,
        opaque_clouds: args.opaque_clouds,
        tile_count: tiles.len(),
        skipped_empty_tiles,
        total_ms: started.elapsed().as_millis(),
        source_files: SourceFiles {
            channel1: args.channel1,
            channel2: args.channel2,
            channel3: args.channel3,
        },
        scan_time_utc: samplers.c02.field.scene.start_time_utc.to_rfc3339(),
        tilejson_path,
        demo_path,
        tiles,
    };
    let manifest_path = args.out_dir.join("tiles_manifest.json");
    atomic_write_json(&manifest_path, &manifest)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn Error>> {
    if args.max_zoom < args.min_zoom {
        return Err(boxed_error("--max-zoom must be >= --min-zoom"));
    }
    if args.tile_size == 0 || args.tile_size > 2048 {
        return Err(boxed_error("--tile-size must be in 1..=2048"));
    }
    if !(args.west.is_finite()
        && args.east.is_finite()
        && args.south.is_finite()
        && args.north.is_finite()
        && args.west < args.east
        && args.south < args.north
        && args.south >= -85.051_128_78
        && args.north <= 85.051_128_78)
    {
        return Err(boxed_error(
            "bounds must be finite west<east south<north within Web Mercator latitude limits",
        ));
    }
    if !(0.0..=1.0).contains(&args.opacity) {
        return Err(boxed_error("--opacity must be in 0..=1"));
    }
    Ok(())
}

fn tile_jobs(
    bounds: Bounds,
    min_zoom: u8,
    max_zoom: u8,
) -> Result<Vec<(u8, u32, u32)>, Box<dyn Error>> {
    let mut jobs = Vec::new();
    for z in min_zoom..=max_zoom {
        let n = 1u32
            .checked_shl(u32::from(z))
            .ok_or_else(|| boxed_error("zoom too large"))?;
        let x0 = lon_to_tile_x(bounds.west, z).min(n.saturating_sub(1));
        let x1 = lon_to_tile_x(bounds.east, z).min(n.saturating_sub(1));
        let y0 = lat_to_tile_y(bounds.north, z).min(n.saturating_sub(1));
        let y1 = lat_to_tile_y(bounds.south, z).min(n.saturating_sub(1));
        for y in y0..=y1 {
            for x in x0..=x1 {
                jobs.push((z, x, y));
            }
        }
    }
    Ok(jobs)
}

#[allow(clippy::too_many_arguments)]
fn render_tile(
    samplers: &Samplers,
    bounds: Bounds,
    z: u8,
    x: u32,
    y: u32,
    tile_size: u32,
    opacity: f64,
    opaque_clouds: bool,
    layer: LayerModeArg,
    out_dir: &Path,
    compression: PngCompressionMode,
) -> Result<Option<TileRecord>, Box<dyn Error + Send + Sync>> {
    let mut tile = RgbaImage::from_pixel(tile_size, tile_size, Rgba([0, 0, 0, 0]));
    let mut nontransparent_pixels = 0u32;
    for py in 0..tile_size {
        for px in 0..tile_size {
            let (lon, lat) = tile_pixel_lon_lat(z, x, y, px, py, tile_size);
            if lon < bounds.west || lon > bounds.east || lat < bounds.south || lat > bounds.north {
                continue;
            }
            let c02 = samplers.c02.sample(lat, lon);
            let c13 = samplers.c13.as_ref().map(|sampler| sampler.sample(lat, lon));
            let color = compose_goes_abi_rgb_pixel(GoesAbiRgbCompositeStyle::GeoColor, |channel| {
                Ok(match channel {
                    1 => samplers.c01.sample(lat, lon),
                    2 => c02,
                    3 => samplers.c03.sample(lat, lon),
                    _ => f32::NAN,
                })
            })
            .unwrap_or(rustwx_render::Color::TRANSPARENT);
            if color.a == 0 {
                continue;
            }
            let (r, g, b, source_alpha) = match layer {
                LayerModeArg::Geocolor => (color.r, color.g, color.b, f64::from(color.a) / 255.0),
                LayerModeArg::Clouds => {
                    let Some((r, g, b, alpha)) = cloud_overlay_pixel(color, c02, c13) else {
                        continue;
                    };
                    (r, g, b, alpha)
                }
            };
            let source_alpha = if opaque_clouds && layer == LayerModeArg::Clouds {
                1.0
            } else {
                source_alpha
            };
            let alpha = (source_alpha * opacity * 255.0).round().clamp(0.0, 255.0) as u8;
            if alpha > 0 {
                nontransparent_pixels = nontransparent_pixels.saturating_add(1);
                tile.put_pixel(px, py, Rgba([r, g, b, alpha]));
            }
        }
    }
    if nontransparent_pixels == 0 {
        return Ok(None);
    }
    let path = out_dir.join(z.to_string()).join(x.to_string());
    fs::create_dir_all(&path)?;
    let path = path.join(format!("{y}.png"));
    save_rgba_png_profile_with_options(&tile, &path, &PngWriteOptions { compression })?;
    Ok(Some(TileRecord {
        z,
        x,
        y,
        path,
        nontransparent_pixels,
    }))
}

fn cloud_overlay_pixel(
    color: rustwx_render::Color,
    c02_reflectance: f32,
    c13_brightness_temp_k: Option<f32>,
) -> Option<(u8, u8, u8, f64)> {
    let r = f64::from(color.r);
    let g = f64::from(color.g);
    let b = f64::from(color.b);
    let bright = 0.299 * r + 0.587 * g + 0.114 * b;
    let max_rgb = r.max(g).max(b);
    let min_rgb = r.min(g).min(b);
    let chroma = max_rgb - min_rgb;
    let neutral = (1.0 - chroma / 95.0).clamp(0.0, 1.0);

    let visible_score = if c02_reflectance.is_finite() {
        smoothstep(0.16, 0.62, f64::from(c02_reflectance)).max(smoothstep(95.0, 225.0, bright))
            * (0.36 + 0.64 * neutral)
    } else {
        0.0
    };
    let cold_score = c13_brightness_temp_k
        .filter(|value| value.is_finite())
        .map(|value| smoothstep(268.0, 218.0, f64::from(value)) * 0.92)
        .unwrap_or(0.0);
    let score = visible_score.max(cold_score);
    if score <= 0.035 {
        return None;
    }

    if cold_score > visible_score + 0.18 && bright < 82.0 {
        let gray = (140.0 + cold_score * 105.0).round().clamp(0.0, 255.0) as u8;
        let blue = (f64::from(gray) + 10.0).round().clamp(0.0, 255.0) as u8;
        Some((gray, gray, blue, score.powf(1.05).min(0.90)))
    } else {
        let lift = 10.0 + 10.0 * score;
        let rr = (r * 1.055 + lift).round().clamp(0.0, 255.0) as u8;
        let gg = (g * 1.055 + lift).round().clamp(0.0, 255.0) as u8;
        let bb = (b * 1.055 + lift).round().clamp(0.0, 255.0) as u8;
        Some((rr, gg, bb, score.powf(1.08).min(0.88)))
    }
}

fn smoothstep(edge0: f64, edge1: f64, value: f64) -> f64 {
    if (edge1 - edge0).abs() <= f64::EPSILON {
        return if value >= edge1 { 1.0 } else { 0.0 };
    }
    let t = ((value - edge0) / (edge1 - edge0)).clamp(0.0, 1.0);
    t * t * (3.0 - 2.0 * t)
}

impl ChannelSampler {
    fn sample(&self, lat: f64, lon: f64) -> f32 {
        let scene = &self.field.scene;
        let Some((scan_x, scan_y)) = lat_lon_to_scan_angles_fast(
            scene.projection.perspective_point_height_m,
            scene.projection.semi_major_axis_m,
            scene.projection.semi_minor_axis_m,
            scene.projection.longitude_of_projection_origin_deg,
            scene.projection.sweep_angle_axis,
            lat,
            lon,
        ) else {
            return f32::NAN;
        };
        let Some((x0, x1, fx)) = bracket_axis(&scene.fixed_grid.x_scan_rad, scan_x) else {
            return f32::NAN;
        };
        let Some((y0, y1, fy)) = bracket_axis(&scene.fixed_grid.y_scan_rad, scan_y) else {
            return f32::NAN;
        };
        bilinear_f32(&self.field.values, scene.fixed_grid.nx, x0, x1, y0, y1, fx, fy)
    }
}

fn bracket_axis(axis: &[f64], value: f64) -> Option<(usize, usize, f64)> {
    if axis.is_empty() || !value.is_finite() {
        return None;
    }
    if axis.len() == 1 {
        return (value == axis[0]).then_some((0, 0, 0.0));
    }
    let ascending = axis[axis.len() - 1] >= axis[0];
    let first = axis[0];
    let last = axis[axis.len() - 1];
    if ascending {
        if value < first || value > last {
            return None;
        }
        let upper = axis.partition_point(|probe| *probe < value);
        let x1 = upper.min(axis.len() - 1);
        let x0 = x1.saturating_sub(1);
        let denom = axis[x1] - axis[x0];
        let f = if denom.abs() > f64::EPSILON {
            (value - axis[x0]) / denom
        } else {
            0.0
        };
        Some((x0, x1, f.clamp(0.0, 1.0)))
    } else {
        if value > first || value < last {
            return None;
        }
        let upper = axis.partition_point(|probe| *probe > value);
        let x1 = upper.min(axis.len() - 1);
        let x0 = x1.saturating_sub(1);
        let denom = axis[x1] - axis[x0];
        let f = if denom.abs() > f64::EPSILON {
            (value - axis[x0]) / denom
        } else {
            0.0
        };
        Some((x0, x1, f.clamp(0.0, 1.0)))
    }
}

#[allow(clippy::too_many_arguments)]
fn bilinear_f32(
    values: &[f32],
    nx: usize,
    x0: usize,
    x1: usize,
    y0: usize,
    y1: usize,
    fx: f64,
    fy: f64,
) -> f32 {
    let idx = |yy: usize, xx: usize| yy.saturating_mul(nx).saturating_add(xx);
    let p00 = values.get(idx(y0, x0)).copied().unwrap_or(f32::NAN);
    let p10 = values.get(idx(y0, x1)).copied().unwrap_or(f32::NAN);
    let p01 = values.get(idx(y1, x0)).copied().unwrap_or(f32::NAN);
    let p11 = values.get(idx(y1, x1)).copied().unwrap_or(f32::NAN);
    if !(p00.is_finite() && p10.is_finite() && p01.is_finite() && p11.is_finite()) {
        return f32::NAN;
    }
    let south = f64::from(p00) * (1.0 - fx) + f64::from(p10) * fx;
    let north = f64::from(p01) * (1.0 - fx) + f64::from(p11) * fx;
    (south * (1.0 - fy) + north * fy) as f32
}

fn lon_to_tile_x(lon: f64, z: u8) -> u32 {
    let n = 2.0_f64.powi(i32::from(z));
    (((lon + 180.0) / 360.0 * n).floor().max(0.0)) as u32
}

fn lat_to_tile_y(lat: f64, z: u8) -> u32 {
    let lat_rad = lat.to_radians();
    let n = 2.0_f64.powi(i32::from(z));
    (((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0) * n)
        .floor()
        .max(0.0) as u32
}

fn tile_pixel_lon_lat(z: u8, x: u32, y: u32, px: u32, py: u32, tile_size: u32) -> (f64, f64) {
    let n = 2.0_f64.powi(i32::from(z));
    let xf = f64::from(x) + (f64::from(px) + 0.5) / f64::from(tile_size);
    let yf = f64::from(y) + (f64::from(py) + 0.5) / f64::from(tile_size);
    let lon = xf / n * 360.0 - 180.0;
    let lat_rad = (PI * (1.0 - 2.0 * yf / n)).sinh().atan();
    (lon, lat_rad.to_degrees())
}

fn write_demo_html(
    path: &Path,
    name: &str,
    bounds: Bounds,
    min_zoom: u8,
    max_zoom: u8,
) -> Result<(), Box<dyn Error>> {
    let center_lat = (bounds.south + bounds.north) * 0.5;
    let center_lon = (bounds.west + bounds.east) * 0.5;
    let html = format!(
        r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{name}</title>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.11.0/mapbox-gl.css" rel="stylesheet">
  <style>
    html, body, #map {{ height: 100%; margin: 0; background: #071018; }}
    .panel {{
      position: absolute; z-index: 2; top: 12px; left: 12px;
      width: min(360px, calc(100vw - 24px));
      color: #e7edf5; background: rgba(10, 16, 23, 0.86);
      font: 13px/1.35 system-ui, -apple-system, Segoe UI, sans-serif;
      padding: 10px 12px; border-radius: 6px; box-sizing: border-box;
      box-shadow: 0 8px 24px rgba(0,0,0,.28); backdrop-filter: blur(8px);
    }}
    .panel b {{ color: #fff; font-size: 14px; }}
    .row {{ display: flex; gap: 8px; align-items: center; margin-top: 8px; }}
    input[type="range"], select {{ flex: 1; min-width: 120px; }}
    select {{
      color: #fff; background: rgba(255,255,255,.08);
      border: 1px solid rgba(255,255,255,.18);
      border-radius: 4px; padding: 5px 7px; font: inherit;
    }}
    button {{
      border: 1px solid rgba(255,255,255,.18);
      background: rgba(255,255,255,.08);
      color: #fff; border-radius: 4px; padding: 5px 8px;
      cursor: pointer; font: inherit;
    }}
    button:hover {{ background: rgba(255,255,255,.14); }}
    .status {{ margin-top: 6px; color: #adbac7; font-size: 12px; }}
  </style>
</head>
<body>
<div id="map"></div>
<div class="panel">
  <b>GOES GeoColor Web Mercator Tiles</b><br>
  Transparent rustwx tiles over Mapbox.
  <div class="row">
    <span>Map</span>
    <select id="style">
      <option value="dark">Dark</option>
      <option value="light">Light</option>
      <option value="streets">Streets</option>
      <option value="outdoors">Outdoors</option>
      <option value="satellite">Satellite</option>
      <option value="satellite_streets">Satellite Streets</option>
      <option value="navigation_day">Navigation Day</option>
      <option value="navigation_night">Navigation Night</option>
    </select>
  </div>
  <div class="row">
    <span>Opacity</span>
    <input id="opacity" type="range" min="0" max="1" step="0.01" value="0.82">
    <button id="toggle">Hide</button>
  </div>
  <div class="status" id="status">Paste token with ?token=... or create mapbox_config.js.</div>
</div>
<script src="mapbox_config.js"></script>
<script src="https://api.mapbox.com/mapbox-gl-js/v3.11.0/mapbox-gl.js"></script>
<script>
const tokenFromUrl = new URLSearchParams(window.location.search).get("token");
const token = tokenFromUrl || window.MAPBOX_TOKEN || localStorage.getItem("MAPBOX_TOKEN");
if (!token) {{
  document.getElementById("status").textContent = "Missing Mapbox token. Add ?token=... or mapbox_config.js.";
  throw new Error("Missing Mapbox token");
}}
if (tokenFromUrl) localStorage.setItem("MAPBOX_TOKEN", tokenFromUrl);

mapboxgl.accessToken = token;
const bounds = [{west}, {south}, {east}, {north}];
const styles = {{
  dark: "mapbox://styles/mapbox/dark-v11",
  light: "mapbox://styles/mapbox/light-v11",
  streets: "mapbox://styles/mapbox/streets-v12",
  outdoors: "mapbox://styles/mapbox/outdoors-v12",
  satellite: "mapbox://styles/mapbox/satellite-v9",
  satellite_streets: "mapbox://styles/mapbox/satellite-streets-v12",
  navigation_day: "mapbox://styles/mapbox/navigation-day-v1",
  navigation_night: "mapbox://styles/mapbox/navigation-night-v1"
}};
let overlayVisible = true;
let overlayOpacity = 0.82;
let fittedOnce = false;
const map = new mapboxgl.Map({{
  container: "map",
  style: styles.dark,
  center: [{center_lon}, {center_lat}],
  zoom: 2.45,
  projection: "mercator",
  attributionControl: true
}});
map.addControl(new mapboxgl.NavigationControl({{ visualizePitch: false }}), "top-right");

function firstSymbolLayerId() {{
  const layers = map.getStyle()?.layers || [];
  const layer = layers.find(item => item.type === "symbol");
  return layer ? layer.id : undefined;
}}

function addGoesLayers() {{
  const beforeId = firstSymbolLayerId();
  if (!map.getSource("goes-geocolor")) {{
    map.addSource("goes-geocolor", {{
    type: "raster",
    tiles: [`${{window.location.origin}}${{window.location.pathname.replace(/\/[^/]*$/, "")}}/{{z}}/{{x}}/{{y}}.png`],
    tileSize: 256,
    minzoom: {min_zoom},
    maxzoom: {max_zoom},
    bounds
    }});
  }}
  if (!map.getLayer("goes-geocolor")) {{
    map.addLayer({{
    id: "goes-geocolor",
    type: "raster",
    source: "goes-geocolor",
    paint: {{
      "raster-opacity": overlayOpacity,
      "raster-fade-duration": 0,
      "raster-resampling": "linear"
    }}
    }}, beforeId);
  }}
  map.setPaintProperty("goes-geocolor", "raster-opacity", overlayOpacity);
  map.setLayoutProperty("goes-geocolor", "visibility", overlayVisible ? "visible" : "none");

  if (!map.getSource("goes-bounds")) {{
    map.addSource("goes-bounds", {{
    type: "geojson",
    data: {{
      type: "Feature",
      geometry: {{
        type: "Polygon",
        coordinates: [[
          [bounds[0], bounds[1]], [bounds[2], bounds[1]],
          [bounds[2], bounds[3]], [bounds[0], bounds[3]],
          [bounds[0], bounds[1]]
        ]]
      }}
    }}
    }});
  }}
  if (!map.getLayer("goes-bounds")) {{
    map.addLayer({{
    id: "goes-bounds",
    type: "line",
    source: "goes-bounds",
    paint: {{ "line-color": "#8ec5ff", "line-opacity": 0.55, "line-width": 1 }}
    }}, beforeId);
  }}
  if (!fittedOnce) {{
    map.fitBounds([[bounds[0], bounds[1]], [bounds[2], bounds[3]]], {{ padding: 30, duration: 0 }});
    fittedOnce = true;
  }}
  document.getElementById("status").textContent = "GOES tiles z{min_zoom}-z{max_zoom} - generated by rustwx";
}}

map.on("load", addGoesLayers);
map.on("style.load", addGoesLayers);

document.getElementById("style").addEventListener("change", event => {{
  map.setStyle(styles[event.target.value]);
}});

document.getElementById("opacity").addEventListener("input", event => {{
  overlayOpacity = Number(event.target.value);
  if (map.getLayer("goes-geocolor")) {{
    map.setPaintProperty("goes-geocolor", "raster-opacity", overlayOpacity);
  }}
}});
document.getElementById("toggle").addEventListener("click", event => {{
  const visible = map.getLayoutProperty("goes-geocolor", "visibility") !== "none";
  overlayVisible = !visible;
  map.setLayoutProperty("goes-geocolor", "visibility", overlayVisible ? "visible" : "none");
  event.target.textContent = overlayVisible ? "Hide" : "Show";
}});
</script>
</body>
</html>
"##,
        name = html_escape(name),
        west = bounds.west,
        east = bounds.east,
        south = bounds.south,
        north = bounds.north,
        center_lat = center_lat,
        center_lon = center_lon,
        min_zoom = min_zoom,
        max_zoom = max_zoom,
    );
    fs::write(path, html)?;
    Ok(())
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<(), Box<dyn Error>> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}
