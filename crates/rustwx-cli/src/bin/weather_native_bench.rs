use clap::Parser;

#[path = "../region.rs"]
#[allow(dead_code)]
mod region;

use region::RegionPreset;
use rustwx_cli::benchmark::{
    WeatherNativeBenchmarkRequest, default_benchmark_products, run_weather_native_benchmark,
};
use rustwx_core::SourceId;
use rustwx_products::cache::default_proof_cache_dir;
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "weather-native-bench",
    about = "Benchmark native contour weather maps against legacy raster and matplotlib/cartopy equivalents"
)]
struct Args {
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long, default_value_t = 23)]
    cycle: u8,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long, value_enum, default_value_t = RegionPreset::SouthernPlains)]
    region: RegionPreset,
    #[arg(long = "product", value_delimiter = ',', num_args = 1..)]
    products: Vec<String>,
    #[arg(long, default_value_t = 5)]
    rust_runs: usize,
    #[arg(long, default_value_t = 5)]
    python_runs: usize,
    #[arg(long, default_value = "python")]
    python: String,
    #[arg(long, default_value = "proof")]
    out_dir: std::path::PathBuf,
    #[arg(long)]
    cache_dir: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let products = if args.products.is_empty() {
        default_benchmark_products()
    } else {
        args.products
    };
    let out_dir = args.out_dir;
    let cache_root = args
        .cache_dir
        .unwrap_or_else(|| default_proof_cache_dir(&out_dir));
    let request = WeatherNativeBenchmarkRequest {
        date_yyyymmdd: args.date,
        cycle_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir,
        cache_root,
        use_cache: !args.no_cache,
        product_slugs: products,
        rust_runs: args.rust_runs,
        python_runs: args.python_runs,
        python_executable: args.python,
        output_width: 1200,
        output_height: 900,
        png_compression: rustwx_render::PngCompressionMode::Default,
    };
    let summary = run_weather_native_benchmark(&request)?;
    println!("{}", summary.summary_json.display());
    println!("{}", summary.summary_markdown.display());
    Ok(())
}
