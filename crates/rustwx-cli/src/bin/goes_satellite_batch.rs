use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use rustwx_products::satellite::{GoesSatelliteBatchRequest, run_goes_satellite_batch};
use rustwx_render::PngCompressionMode;

#[derive(Debug, Parser)]
#[command(
    name = "goes-satellite-batch",
    about = "Discover, cache, and render raw NOAA GOES ABI/GLM satellite products"
)]
struct Args {
    #[arg(long, default_value = "goes18")]
    satellite: String,
    #[arg(long, default_value = "ABI-L2-CMIPC")]
    abi_product: String,
    #[arg(
        long,
        help = "ABI sector shortcut: conus, full_disk, meso1, or meso2. Overrides --abi-product with the matching CMIP product."
    )]
    sector: Option<String>,
    #[arg(long, default_value = "pacific_southwest")]
    domain: String,
    #[arg(long, default_value = "Pacific Southwest")]
    label: String,
    #[arg(long, default_value_t = -127.0)]
    west: f64,
    #[arg(long, default_value_t = -111.0)]
    east: f64,
    #[arg(long, default_value_t = 30.0)]
    south: f64,
    #[arg(long, default_value_t = 44.5)]
    north: f64,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: PathBuf,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    #[arg(long, default_value_t = 1400)]
    width: u32,
    #[arg(long, default_value_t = 1100)]
    height: u32,
    #[arg(long, default_value_t = 6)]
    scan_lookback_hours: u32,
    #[arg(long, default_value_t = 2)]
    discovery_retries: u32,
    #[arg(long, default_value_t = 20_000)]
    retry_sleep_ms: u64,
    #[arg(long)]
    no_cache: bool,
    #[arg(long)]
    no_glm: bool,
    #[arg(long, default_value_t = 90)]
    glm_fetch_count: usize,
    #[arg(long, default_value_t = 3)]
    glm_lookback_hours: u32,
    #[arg(long, default_value_t = 30.0)]
    glm_max_age_min: f64,
    #[arg(long, value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
    #[arg(long)]
    skip_scan_id: Option<String>,
    #[arg(long, help = "Infer render bounds from the ABI fixed grid scene")]
    auto_bounds: bool,
    #[arg(
        long,
        help = "Allow full-disk high-resolution visible channels such as C02"
    )]
    allow_high_resolution_full_disk: bool,
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
    let request = GoesSatelliteBatchRequest {
        satellite: args.satellite,
        abi_product: args.abi_product,
        abi_sector: args.sector,
        domain_slug: args.domain,
        domain_label: args.label,
        bounds: (args.west, args.east, args.south, args.north),
        out_dir: args.out_dir,
        cache_dir: args.cache_dir,
        products: args.products,
        width: args.width,
        height: args.height,
        scan_lookback_hours: args.scan_lookback_hours,
        discovery_retries: args.discovery_retries,
        retry_sleep_ms: args.retry_sleep_ms,
        use_cache: !args.no_cache,
        download_glm: !args.no_glm,
        glm_fetch_count: args.glm_fetch_count,
        glm_lookback_hours: args.glm_lookback_hours,
        glm_max_age_min: args.glm_max_age_min,
        png_compression: args.png_compression.into(),
        skip_scan_id: args.skip_scan_id,
        auto_bounds: args.auto_bounds,
        allow_high_resolution_full_disk: args.allow_high_resolution_full_disk,
    };
    let report = run_goes_satellite_batch(&request)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
