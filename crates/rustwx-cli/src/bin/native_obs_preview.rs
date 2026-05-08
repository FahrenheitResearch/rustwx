use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use image::{Rgb, RgbImage};
use rustwx_products::native_dataset::NativeDatasetBounds;
use rustwx_products::native_dataset_obs::{
    GOES_MCMIPC_CHANNELS, GoesAbiChannelSpec, NativeObsTileGrid, read_goes_multiband_hour,
    read_mrms_product_hour, remap_goes_hour_to_tile, remap_mrms_hour_to_tile,
};
use rustwx_radar::Level2File;
use rustwx_radar::batch::{
    CartesianGridSpec, Level2CartesianTensorBuildOptions, Level2TensorProduct,
    build_level2_cartesian_tensors_with_options,
};
use rustwx_radar::dealias::DealiasMethod;
use rustwx_radar::nexrad::RadarProduct;
use rustwx_radar::nexrad::sites::{find_nearest_site, find_site};
use rustwx_radar::render::ColorTable;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PreviewKind {
    Goes,
    Mrms,
    Level2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DealiasArg {
    Auto,
    Off,
    Radial,
    Sweep,
}

#[derive(Debug, Parser)]
#[command(
    name = "native-obs-preview",
    about = "Render quicklook PNGs from GOES, MRMS, or NEXRAD Level-II files"
)]
struct Args {
    #[arg(long, value_enum)]
    kind: PreviewKind,
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = 512)]
    size: usize,
    #[arg(long, value_name = "WEST,EAST,SOUTH,NORTH", allow_hyphen_values = true)]
    bounds: Option<String>,
    #[arg(long, help = "GOES channel, e.g. C13")]
    channel: Option<String>,
    #[arg(
        long,
        help = "Level-II product, e.g. reflectivity, velocity, cc, zdr, kdp"
    )]
    product: Option<String>,
    #[arg(long, help = "Radar site id for Level-II, e.g. KTLX")]
    radar_site: Option<String>,
    #[arg(long)]
    center_lat: Option<f64>,
    #[arg(long)]
    center_lon: Option<f64>,
    #[arg(long, default_value_t = 512.0)]
    span_km: f64,
    #[arg(long)]
    min: Option<f32>,
    #[arg(long)]
    max: Option<f32>,
    #[arg(
        long,
        value_enum,
        default_value = "auto",
        help = "Velocity dealiasing for Level-II quicklooks"
    )]
    dealias: DealiasArg,
}

#[derive(Debug, Serialize)]
struct PreviewReport {
    schema_version: &'static str,
    kind: String,
    input: String,
    output: String,
    field: String,
    width: usize,
    height: usize,
    min: f32,
    max: f32,
    finite_count: usize,
    palette: String,
    dealias_method: Option<String>,
}

struct PreviewData {
    values: Vec<f32>,
    field: String,
    default_min: f32,
    default_max: f32,
    palette_product: Option<RadarProduct>,
    dealias_method: Option<DealiasMethod>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.size == 0 {
        bail!("--size must be >= 1");
    }
    let preview = match args.kind {
        PreviewKind::Goes => render_goes_values(&args)?,
        PreviewKind::Mrms => render_mrms_values(&args)?,
        PreviewKind::Level2 => render_level2_values(&args)?,
    };
    let min = args.min.unwrap_or(preview.default_min);
    let max = args.max.unwrap_or(preview.default_max);
    let color_table = preview
        .palette_product
        .map(|product| ColorTable::for_product(product).with_min_value(min));
    let palette_name = color_table
        .as_ref()
        .map(|table| table.name.clone())
        .unwrap_or_else(|| {
            if args.kind == PreviewKind::Goes {
                "goes_grayscale".to_string()
            } else {
                "turbo_fallback".to_string()
            }
        });
    write_scalar_png(
        &preview.values,
        args.size,
        args.size,
        min,
        max,
        args.kind,
        color_table.as_ref(),
        &args.out,
    )?;
    let finite = preview
        .values
        .iter()
        .filter(|value| value.is_finite())
        .count();
    let report = PreviewReport {
        schema_version: "rustwx.native_obs_preview.v1",
        kind: format!("{:?}", args.kind).to_ascii_lowercase(),
        input: args.input.display().to_string(),
        output: args.out.display().to_string(),
        field: preview.field,
        width: args.size,
        height: args.size,
        min,
        max,
        finite_count: finite,
        palette: palette_name,
        dealias_method: preview
            .dealias_method
            .map(|method| format!("{method:?}").to_ascii_lowercase()),
    };
    fs::write(
        args.out.with_extension("json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn render_goes_values(args: &Args) -> anyhow::Result<PreviewData> {
    let channel = args
        .channel
        .as_deref()
        .unwrap_or("C13")
        .to_ascii_uppercase();
    let spec = GOES_MCMIPC_CHANNELS
        .iter()
        .find(|spec| spec.id.eq_ignore_ascii_case(&channel))
        .copied()
        .unwrap_or_else(|| {
            let number = channel
                .trim_start_matches('C')
                .parse::<u8>()
                .unwrap_or(13)
                .clamp(1, 16);
            GoesAbiChannelSpec::new(
                Box::leak(format!("C{number:02}").into_boxed_str()),
                number,
                "CMI",
            )
        });
    let hour = convert_box_error(read_goes_multiband_hour(&args.input, &[spec]))?;
    let bounds = parse_bounds_or_default(args.bounds.as_deref())?;
    let tile = convert_box_error(NativeObsTileGrid::new(bounds, args.size, args.size))?;
    let remapped = convert_box_error(remap_goes_hour_to_tile(&hour, tile))?;
    let band = remapped
        .bands
        .first()
        .ok_or_else(|| anyhow::anyhow!("GOES file produced no bands"))?;
    let is_ir = spec.channel >= 7;
    let (min, max) = if is_ir { (190.0, 310.0) } else { (0.0, 1.0) };
    Ok(PreviewData {
        values: band.values.clone(),
        field: spec.id.to_string(),
        default_min: min,
        default_max: max,
        palette_product: None,
        dealias_method: None,
    })
}

fn render_mrms_values(args: &Args) -> anyhow::Result<PreviewData> {
    let hour = convert_box_error(read_mrms_product_hour(&args.input))?;
    let bounds = parse_bounds_or_default(args.bounds.as_deref())?;
    let tile = convert_box_error(NativeObsTileGrid::new(bounds, args.size, args.size))?;
    let remapped = convert_box_error(remap_mrms_hour_to_tile(&hour, tile))?;
    let field = remapped.product_id.clone();
    let lower = field.to_ascii_lowercase();
    let (min, max) = if lower.contains("reflect") {
        (15.0, 75.0)
    } else if lower.contains("precip") {
        (0.0, 100.0)
    } else if lower.contains("mesh") || lower.contains("azshear") || lower.contains("rotation") {
        (
            0.0,
            finite_percentile(&remapped.values, 99.5)
                .unwrap_or(1.0)
                .max(1.0),
        )
    } else {
        auto_range(&remapped.values).unwrap_or((0.0, 1.0))
    };
    let palette_product = if lower.contains("reflect") {
        Some(RadarProduct::Reflectivity)
    } else if lower.contains("mesh") || lower.contains("vil") {
        Some(RadarProduct::VIL)
    } else {
        None
    };
    Ok(PreviewData {
        values: remapped.values,
        field,
        default_min: min,
        default_max: max,
        palette_product,
        dealias_method: None,
    })
}

fn render_level2_values(args: &Args) -> anyhow::Result<PreviewData> {
    let product_name = args.product.as_deref().unwrap_or("reflectivity");
    let product = parse_level2_product(product_name)?;
    let bytes = fs::read(&args.input)
        .with_context(|| format!("failed reading Level-II file {}", args.input.display()))?;
    let file = Level2File::parse(&bytes).context("failed parsing Level-II file")?;
    let site = if let Some(site_id) = args.radar_site.as_deref() {
        find_site(site_id).ok_or_else(|| anyhow::anyhow!("unknown radar site {site_id}"))?
    } else {
        let lat = args.center_lat.unwrap_or(35.0);
        let lon = args.center_lon.unwrap_or(-97.0);
        find_nearest_site(lat, lon).ok_or_else(|| anyhow::anyhow!("no nearest radar site found"))?
    };
    let center_lat = args.center_lat.unwrap_or(site.lat);
    let center_lon = args.center_lon.unwrap_or(site.lon);
    let resolution_m = args.span_km * 1000.0 / args.size.saturating_sub(1).max(1) as f64;
    let origin = -0.5 * resolution_m * args.size.saturating_sub(1) as f64;
    let grid_spec = CartesianGridSpec {
        nx: args.size as u32,
        ny: args.size as u32,
        center_lat,
        center_lon,
        resolution_m,
        x_origin_m: origin,
        y_origin_m: origin,
        projection: "local_tangent_cartesian_m".to_string(),
    };
    let dealias_method = level2_dealias_method(args.dealias, product);
    let tensors = build_level2_cartesian_tensors_with_options(
        &file,
        site,
        args.input.display().to_string(),
        &[product],
        &grid_spec,
        Level2CartesianTensorBuildOptions {
            dealias_method: dealias_method.unwrap_or(DealiasMethod::Off),
        },
    );
    let tensor = tensors
        .first()
        .ok_or_else(|| anyhow::anyhow!("Level-II tensor builder produced no tensors"))?;
    let (min, max) = match product {
        Level2TensorProduct::Reflectivity => (15.0, 75.0),
        Level2TensorProduct::Velocity | Level2TensorProduct::StormRelativeVelocity => (-45.0, 45.0),
        Level2TensorProduct::CorrelationCoefficient => (0.0, 1.05),
        Level2TensorProduct::DifferentialReflectivity => (-2.0, 8.0),
        Level2TensorProduct::SpecificDiffPhase => (-2.0, 8.0),
        Level2TensorProduct::DifferentialPhase => (0.0, 180.0),
        _ => auto_range(&tensor.values).unwrap_or((0.0, 1.0)),
    };
    Ok(PreviewData {
        values: tensor.values.clone(),
        field: product.short_name().to_string(),
        default_min: min,
        default_max: max,
        palette_product: Some(product.radar_product()),
        dealias_method,
    })
}

fn parse_bounds_or_default(value: Option<&str>) -> anyhow::Result<NativeDatasetBounds> {
    match value {
        Some(value) => {
            let parts = value.split(',').collect::<Vec<_>>();
            if parts.len() != 4 {
                bail!("--bounds must be WEST,EAST,SOUTH,NORTH");
            }
            Ok(NativeDatasetBounds::new(
                parts[0].parse().context("invalid west bound")?,
                parts[1].parse().context("invalid east bound")?,
                parts[2].parse().context("invalid south bound")?,
                parts[3].parse().context("invalid north bound")?,
            ))
        }
        None => Ok(NativeDatasetBounds::new(-125.0, -66.0, 24.0, 50.0)),
    }
}

fn parse_level2_product(value: &str) -> anyhow::Result<Level2TensorProduct> {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_");
    Ok(match normalized.as_str() {
        "reflectivity" | "ref" | "refl" | "dbz" => Level2TensorProduct::Reflectivity,
        "velocity" | "vel" => Level2TensorProduct::Velocity,
        "spectrum_width" | "sw" => Level2TensorProduct::SpectrumWidth,
        "differential_reflectivity" | "zdr" => Level2TensorProduct::DifferentialReflectivity,
        "correlation_coefficient" | "cc" | "rho" | "rhohv" => {
            Level2TensorProduct::CorrelationCoefficient
        }
        "differential_phase" | "phi" | "phidp" => Level2TensorProduct::DifferentialPhase,
        "specific_diff_phase" | "kdp" => Level2TensorProduct::SpecificDiffPhase,
        "hydrometeor_class" | "hca" | "hhc" => Level2TensorProduct::HydrometeorClass,
        "storm_relative_velocity" | "srv" => Level2TensorProduct::StormRelativeVelocity,
        "vil" => Level2TensorProduct::Vil,
        "echo_tops" | "et" => Level2TensorProduct::EchoTops,
        _ => bail!("unknown Level-II product: {value}"),
    })
}

fn level2_dealias_method(
    dealias: DealiasArg,
    product: Level2TensorProduct,
) -> Option<DealiasMethod> {
    let velocity_product = matches!(
        product.radar_product().base_product(),
        RadarProduct::Velocity | RadarProduct::SuperResVelocity
    );
    match dealias {
        DealiasArg::Off => None,
        DealiasArg::Radial => velocity_product.then_some(DealiasMethod::RadialContinuity),
        DealiasArg::Sweep | DealiasArg::Auto => {
            velocity_product.then_some(DealiasMethod::SweepContinuity)
        }
    }
}

fn write_scalar_png(
    values: &[f32],
    width: usize,
    height: usize,
    min: f32,
    max: f32,
    kind: PreviewKind,
    color_table: Option<&ColorTable>,
    out: &PathBuf,
) -> anyhow::Result<()> {
    if values.len() != width.saturating_mul(height) {
        bail!(
            "value count {} does not match image size {}x{}",
            values.len(),
            width,
            height
        );
    }
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut image = RgbImage::new(width as u32, height as u32);
    for row in 0..height {
        for col in 0..width {
            let value = values[row * width + col];
            let rgb = if value.is_finite() {
                if let Some(table) = color_table {
                    let rgba = table.color_for_value(value);
                    if rgba[3] == 0 {
                        Rgb([8, 9, 14])
                    } else {
                        Rgb([rgba[0], rgba[1], rgba[2]])
                    }
                } else {
                    let t = ((value - min) / (max - min).max(1.0e-6)).clamp(0.0, 1.0);
                    match kind {
                        PreviewKind::Goes => grayscale(1.0 - t),
                        PreviewKind::Mrms | PreviewKind::Level2 => turbo(t),
                    }
                }
            } else {
                Rgb([8, 9, 14])
            };
            image.put_pixel(col as u32, row as u32, rgb);
        }
    }
    image.save(out)?;
    Ok(())
}

fn auto_range(values: &[f32]) -> Option<(f32, f32)> {
    let min = finite_percentile(values, 1.0)?;
    let max = finite_percentile(values, 99.0)?;
    Some((min, max.max(min + 1.0e-3)))
}

fn finite_percentile(values: &[f32], percentile: f32) -> Option<f32> {
    let mut finite = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return None;
    }
    finite.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let index = ((percentile / 100.0).clamp(0.0, 1.0) * (finite.len() - 1) as f32).round();
    finite.get(index as usize).copied()
}

fn grayscale(t: f32) -> Rgb<u8> {
    let value = (255.0 * t.clamp(0.0, 1.0)).round() as u8;
    Rgb([value, value, value])
}

fn turbo(t: f32) -> Rgb<u8> {
    let t = t.clamp(0.0, 1.0);
    let r =
        34.61 + t * (1172.33 + t * (-10793.56 + t * (33300.12 + t * (-38394.49 + t * 14825.05))));
    let g = 23.31 + t * (557.33 + t * (1225.33 + t * (-3574.96 + t * (1073.77 + t * 707.56))));
    let b = 27.2 + t * (3211.1 + t * (-15327.97 + t * (27814.0 + t * (-22569.18 + t * 6838.66))));
    Rgb([to_u8(r), to_u8(g), to_u8(b)])
}

fn to_u8(value: f32) -> u8 {
    value.clamp(0.0, 255.0).round() as u8
}

fn convert_box_error<T>(result: Result<T, Box<dyn std::error::Error>>) -> anyhow::Result<T> {
    result.map_err(|err| anyhow::anyhow!("{err}"))
}
