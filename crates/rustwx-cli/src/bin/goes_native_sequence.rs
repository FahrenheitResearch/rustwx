use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use rustwx_products::satellite::{GoesNativeSequenceRequest, run_goes_native_sequence};
use rustwx_render::PngCompressionMode;

#[derive(Debug, Parser)]
#[command(
    name = "goes-native-sequence",
    about = "Discover, cache, and render fast native-grid GOES ABI crops for any area/time window"
)]
struct Args {
    #[arg(long, default_value = "goes18")]
    satellite: String,
    #[arg(long, default_value = "ABI-L2-CMIPC")]
    abi_product: String,
    #[arg(
        long,
        help = "ABI sector shortcut: conus, full_disk, meso1, or meso2. Overrides --abi-product."
    )]
    sector: Option<String>,
    #[arg(
        long,
        default_value = "geocolor",
        help = "Product: geocolor, airmass, dust, fire_temperature, sandwich, day_night_cloud_micro_combo, or band_13/C13"
    )]
    product: String,
    #[arg(long, default_value = "native_crop")]
    domain: String,
    #[arg(long, default_value = "Native Crop")]
    label: String,
    #[arg(long, allow_hyphen_values = true)]
    west: f64,
    #[arg(long, allow_hyphen_values = true)]
    east: f64,
    #[arg(long, allow_hyphen_values = true)]
    south: f64,
    #[arg(long, allow_hyphen_values = true)]
    north: f64,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: PathBuf,
    #[arg(
        long,
        help = "Inclusive start time, e.g. 2026-05-08T21:00:00Z. If omitted, latest mode is used."
    )]
    start: Option<String>,
    #[arg(
        long,
        help = "Inclusive end time, e.g. 2026-05-08T22:00:00Z. If omitted, latest mode is used."
    )]
    end: Option<String>,
    #[arg(
        long,
        default_value_t = 1,
        help = "Latest complete scans to render when --start/--end are omitted"
    )]
    latest_count: usize,
    #[arg(long, default_value_t = 6)]
    scan_lookback_hours: u32,
    #[arg(
        long,
        help = "Minimum spacing between kept scans, useful for 1-min mesoscale throttling"
    )]
    min_step_minutes: Option<u32>,
    #[arg(long)]
    no_cache: bool,
    #[arg(
        long,
        default_value_t = 1.0,
        help = "1.0 keeps native crop pixels; 2.0 halves each dimension"
    )]
    downsample: f64,
    #[arg(long, help = "Cap output width while preserving aspect ratio")]
    max_width: Option<u32>,
    #[arg(long, help = "Cap output height while preserving aspect ratio")]
    max_height: Option<u32>,
    #[arg(long, default_value_t = 8)]
    download_workers: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "Pixel render threads. 0 uses the global Rayon pool."
    )]
    render_workers: usize,
    #[arg(long, default_value_t = 1)]
    discovery_retries: u32,
    #[arg(long, default_value_t = 10_000)]
    retry_sleep_ms: u64,
    #[arg(long, value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
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
    let args = Args::parse();
    let request = GoesNativeSequenceRequest {
        satellite: args.satellite,
        abi_product: args.abi_product,
        abi_sector: args.sector,
        product: args.product,
        domain_slug: args.domain,
        domain_label: args.label,
        bounds: (args.west, args.east, args.south, args.north),
        out_dir: args.out_dir,
        cache_dir: args.cache_dir,
        start_time_utc: parse_optional_time(args.start.as_deref())?,
        end_time_utc: parse_optional_time(args.end.as_deref())?,
        latest_count: args.latest_count,
        scan_lookback_hours: args.scan_lookback_hours,
        min_step_minutes: args.min_step_minutes,
        use_cache: !args.no_cache,
        downsample: args.downsample,
        max_width: args.max_width,
        max_height: args.max_height,
        download_workers: args.download_workers,
        render_workers: args.render_workers,
        discovery_retries: args.discovery_retries,
        retry_sleep_ms: args.retry_sleep_ms,
        png_compression: args.png_compression.into(),
    };
    let report = run_goes_native_sequence(&request)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn parse_optional_time(
    raw: Option<&str>,
) -> Result<Option<DateTime<Utc>>, Box<dyn std::error::Error>> {
    raw.map(|value| {
        DateTime::parse_from_rfc3339(value)
            .map(|time| time.with_timezone(&Utc))
            .map_err(|err| err.into())
    })
    .transpose()
}
