use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use rustwx_products::satellite::{
    GoesAbiLayerStyle, GoesAbiMapRequest, build_goes_abi_map_render_request,
};
use rustwx_render::{PngCompressionMode, PngWriteOptions, save_png_profile_with_options};

#[derive(Debug, Parser)]
#[command(
    name = "goes-satellite-preview",
    about = "Render a GOES ABI NetCDF file through rustwx's native map plot pipeline"
)]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_enum)]
    style: SatelliteStyleArg,
    #[arg(long, default_value = "CMI")]
    variable: String,
    #[arg(long, default_value = "California")]
    domain_label: String,
    #[arg(long, default_value_t = -126.0)]
    west: f64,
    #[arg(long, default_value_t = -113.0)]
    east: f64,
    #[arg(long, default_value_t = 31.0)]
    south: f64,
    #[arg(long, default_value_t = 43.0)]
    north: f64,
    #[arg(long, default_value_t = 1400)]
    width: u32,
    #[arg(long, default_value_t = 1100)]
    height: u32,
    #[arg(long, value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SatelliteStyleArg {
    VisibleRed,
    CleanIr,
    ShortwaveIr,
    WaterVapor,
}

impl From<SatelliteStyleArg> for GoesAbiLayerStyle {
    fn from(value: SatelliteStyleArg) -> Self {
        match value {
            SatelliteStyleArg::VisibleRed => Self::VisibleRed,
            SatelliteStyleArg::CleanIr => Self::CleanIr,
            SatelliteStyleArg::ShortwaveIr => Self::ShortwaveIr,
            SatelliteStyleArg::WaterVapor => Self::WaterVapor,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let args = Args::parse();
    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let request = GoesAbiMapRequest {
        abi_path: args.input,
        variable_name: args.variable,
        layer_style: args.style.into(),
        domain_label: args.domain_label,
        bounds: (args.west, args.east, args.south, args.north),
        width: args.width,
        height: args.height,
    };
    let build_start = Instant::now();
    let render_request = build_goes_abi_map_render_request(&request)?;
    let build_ms = build_start.elapsed().as_millis();
    let timing = save_png_profile_with_options(
        &render_request,
        &args.output,
        &PngWriteOptions {
            compression: args.png_compression.into(),
        },
    )?;
    println!(
        "wrote {} in {} ms (build {} ms, save {} ms, render {} ms, png {} ms)",
        args.output.display(),
        total_start.elapsed().as_millis(),
        build_ms,
        timing.total_ms,
        timing.state_timing.state_prep_ms,
        timing.png_timing.total_ms
    );
    Ok(())
}
