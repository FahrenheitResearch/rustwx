use clap::{Parser, ValueEnum};
use image::{Rgba, RgbaImage};
use rayon::prelude::*;
use rustwx_render::{save_rgba_png_profile_with_options, PngCompressionMode, PngWriteOptions};
use serde::Serialize;
use std::error::Error;
use std::f64::consts::PI;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(about = "Cut a geographic PNG into transparent XYZ web-map tiles")]
struct Args {
    #[arg(long)]
    input_png: PathBuf,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long)]
    name: String,
    #[arg(long)]
    west: f64,
    #[arg(long)]
    east: f64,
    #[arg(long)]
    south: f64,
    #[arg(long)]
    north: f64,
    #[arg(long, default_value_t = 5)]
    min_zoom: u8,
    #[arg(long, default_value_t = 7)]
    max_zoom: u8,
    #[arg(long, default_value_t = 256)]
    tile_size: u32,
    #[arg(long)]
    base_url: Option<String>,
    #[arg(long, value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
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

#[derive(Debug, Serialize)]
struct TileManifest {
    ok: bool,
    name: String,
    input_png: PathBuf,
    out_dir: PathBuf,
    bounds: [f64; 4],
    minzoom: u8,
    maxzoom: u8,
    tile_size: u32,
    tile_count: usize,
    skipped_empty_tiles: usize,
    total_ms: u128,
    tiles: Vec<TileRecord>,
    tilejson_path: PathBuf,
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

#[derive(Debug, Clone, Copy)]
struct Bounds {
    west: f64,
    east: f64,
    south: f64,
    north: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let started = Instant::now();
    validate_args(&args)?;
    let bounds = Bounds {
        west: args.west,
        east: args.east,
        south: args.south,
        north: args.north,
    };
    fs::create_dir_all(&args.out_dir)?;
    let source = image::open(&args.input_png)?.to_rgba8();
    let (src_w, src_h) = source.dimensions();
    let compression = args.png_compression.into();

    let jobs = tile_jobs(bounds, args.min_zoom, args.max_zoom)?;
    let records = jobs
        .par_iter()
        .map(|&(z, x, y)| {
            render_tile(
                &source,
                src_w,
                src_h,
                bounds,
                z,
                x,
                y,
                args.tile_size,
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
        bounds: [args.west, args.south, args.east, args.north],
    };
    atomic_write_json(&tilejson_path, &tilejson)?;
    let manifest_path = args.out_dir.join("tiles_manifest.json");
    let manifest = TileManifest {
        ok: true,
        name: args.name,
        input_png: args.input_png,
        out_dir: args.out_dir.clone(),
        bounds: [args.west, args.south, args.east, args.north],
        minzoom: args.min_zoom,
        maxzoom: args.max_zoom,
        tile_size: args.tile_size,
        tile_count: tiles.len(),
        skipped_empty_tiles,
        total_ms: started.elapsed().as_millis(),
        tiles,
        tilejson_path,
    };
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
    source: &RgbaImage,
    src_w: u32,
    src_h: u32,
    bounds: Bounds,
    z: u8,
    x: u32,
    y: u32,
    tile_size: u32,
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
            let src_x = (lon - bounds.west) / (bounds.east - bounds.west) * f64::from(src_w - 1);
            let src_y = (bounds.north - lat) / (bounds.north - bounds.south) * f64::from(src_h - 1);
            let pixel = bilinear_rgba(source, src_x, src_y);
            if pixel[3] > 0 {
                nontransparent_pixels = nontransparent_pixels.saturating_add(1);
            }
            tile.put_pixel(px, py, Rgba(pixel));
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

fn bilinear_rgba(image: &RgbaImage, x: f64, y: f64) -> [u8; 4] {
    let w = image.width();
    let h = image.height();
    let x = x.clamp(0.0, f64::from(w.saturating_sub(1)));
    let y = y.clamp(0.0, f64::from(h.saturating_sub(1)));
    let x0 = x.floor() as u32;
    let y0 = y.floor() as u32;
    let x1 = (x0 + 1).min(w.saturating_sub(1));
    let y1 = (y0 + 1).min(h.saturating_sub(1));
    let fx = x - f64::from(x0);
    let fy = y - f64::from(y0);
    let p00 = image.get_pixel(x0, y0).0;
    let p10 = image.get_pixel(x1, y0).0;
    let p01 = image.get_pixel(x0, y1).0;
    let p11 = image.get_pixel(x1, y1).0;
    let mut out = [0u8; 4];
    for c in 0..4 {
        let south = f64::from(p00[c]) * (1.0 - fx) + f64::from(p10[c]) * fx;
        let north = f64::from(p01[c]) * (1.0 - fx) + f64::from(p11[c]) * fx;
        out[c] = (south * (1.0 - fy) + north * fy).round().clamp(0.0, 255.0) as u8;
    }
    out
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
