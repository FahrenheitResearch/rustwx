use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use rustwx_products::satellite::{
    GoesAbiRgbCompositeRequest, GoesAbiRgbCompositeStyle,
    build_goes_abi_rgb_composite_render_request,
};
use rustwx_render::{PngCompressionMode, PngWriteOptions, save_png_profile_with_options};

#[derive(Debug, Parser)]
#[command(
    name = "goes-satellite-rgb-preview",
    about = "Render raw GOES ABI multi-channel RGB composites through rustwx's native map plot pipeline"
)]
struct Args {
    #[arg(long, value_enum)]
    style: RgbStyleArg,
    #[arg(long = "channel", value_parser = parse_channel_path)]
    channels: Vec<(u8, PathBuf)>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    glm_data_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 30.0)]
    glm_max_age_min: f64,
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
enum RgbStyleArg {
    FireTemperature,
    AirMass,
    Dust,
    Sandwich,
    DayCloudPhase,
    NaturalColor,
}

impl From<RgbStyleArg> for GoesAbiRgbCompositeStyle {
    fn from(value: RgbStyleArg) -> Self {
        match value {
            RgbStyleArg::FireTemperature => Self::FireTemperature,
            RgbStyleArg::AirMass => Self::AirMass,
            RgbStyleArg::Dust => Self::Dust,
            RgbStyleArg::Sandwich => Self::Sandwich,
            RgbStyleArg::DayCloudPhase => Self::DayCloudPhase,
            RgbStyleArg::NaturalColor => Self::NaturalColor,
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
    let style: GoesAbiRgbCompositeStyle = args.style.into();
    let channel_paths = args.channels.into_iter().collect::<BTreeMap<_, _>>();
    for &channel in style.required_channels() {
        if !channel_paths.contains_key(&channel) {
            return Err(format!(
                "{} requires --channel {channel:02}=<path>",
                style.product_slug()
            )
            .into());
        }
    }

    let request = GoesAbiRgbCompositeRequest {
        channel_paths,
        composite_style: style,
        domain_label: args.domain_label,
        bounds: (args.west, args.east, args.south, args.north),
        width: args.width,
        height: args.height,
        glm_data_dir: args.glm_data_dir,
        glm_max_age_min: args.glm_max_age_min,
    };
    let build_start = Instant::now();
    let render_request = build_goes_abi_rgb_composite_render_request(&request)?;
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

fn parse_channel_path(raw: &str) -> Result<(u8, PathBuf), String> {
    let (channel, path) = raw.split_once('=').ok_or_else(|| {
        "channel must be formatted as NN=path, for example 13=file.nc".to_string()
    })?;
    let channel = channel
        .trim_start_matches('C')
        .trim_start_matches('c')
        .parse::<u8>()
        .map_err(|err| format!("invalid channel number in {raw}: {err}"))?;
    if !(1..=16).contains(&channel) {
        return Err(format!("GOES ABI channel out of range in {raw}"));
    }
    Ok((channel, PathBuf::from(path)))
}
