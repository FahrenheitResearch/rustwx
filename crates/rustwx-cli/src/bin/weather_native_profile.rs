use std::path::PathBuf;

use clap::Parser;

#[path = "../region.rs"]
#[allow(dead_code)]
mod region;

use region::RegionPreset;
use rustwx_cli::cross_section_proof::{PressureCrossSectionRequest, RoutePresetArg, resolve_route};
use rustwx_cli::profile::{
    WeatherNativeProfileRequest, default_cross_section_profile_products, run_weather_native_profile,
};
use rustwx_core::SourceId;
use rustwx_cross_section::CrossSectionProduct;
use rustwx_products::cache::default_proof_cache_dir;
use rustwx_products::cross_section::supports_pressure_cross_section_product;
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Parser)]
#[command(
    name = "weather-native-profile",
    about = "Profile rustwx map and cross-section rendering components using real HRRR proof plots"
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
    #[arg(long = "section-product", value_delimiter = ',', num_args = 1..)]
    section_products: Vec<String>,
    #[arg(long, value_enum, default_value_t = RoutePresetArg::AmarilloChicago)]
    route: RoutePresetArg,
    #[arg(long)]
    start_lat: Option<f64>,
    #[arg(long)]
    start_lon: Option<f64>,
    #[arg(long)]
    end_lat: Option<f64>,
    #[arg(long)]
    end_lon: Option<f64>,
    #[arg(long, default_value_t = 181)]
    sample_count: usize,
    #[arg(long, default_value_t = 5)]
    runs: usize,
    #[arg(long, default_value = "proof")]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = false)]
    no_wind_overlay: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let route = resolve_route(
        args.route,
        args.start_lat,
        args.start_lon,
        args.end_lat,
        args.end_lon,
    )?;
    let map_products = args.products;
    let section_products = if args.section_products.is_empty() {
        default_cross_section_profile_products()
    } else {
        args.section_products
    };
    let cross_section_requests = section_products
        .into_iter()
        .map(|name| {
            let product = CrossSectionProduct::from_name(&name)
                .ok_or_else(|| format!("unknown cross-section product '{name}'"))?;
            if !supports_pressure_cross_section_product(product) {
                return Err(format!(
                    "cross-section product '{}' is not wired yet for gridded pressure sections",
                    product.slug()
                )
                .into());
            }
            Ok(PressureCrossSectionRequest {
                model: rustwx_core::ModelId::Hrrr,
                date: args.date.clone(),
                cycle: args.cycle,
                forecast_hour: args.forecast_hour,
                source: args.source,
                route: route.clone(),
                product,
                palette: None,
                sample_count: args.sample_count,
                out_dir: args.out_dir.clone(),
                cache_dir: args.cache_dir.clone(),
                use_cache: !args.no_cache,
                show_wind_overlay: !args.no_wind_overlay,
                surface_product_override: None,
                pressure_product_override: None,
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let out_dir = args.out_dir;
    let cache_root = args
        .cache_dir
        .unwrap_or_else(|| default_proof_cache_dir(&out_dir));
    let request = WeatherNativeProfileRequest {
        date_yyyymmdd: args.date,
        cycle_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domain: DomainSpec::new(args.region.slug(), args.region.bounds()),
        out_dir,
        cache_root,
        use_cache: !args.no_cache,
        map_products,
        cross_section_requests,
        runs: args.runs,
        output_width: 1200,
        output_height: 900,
        png_compression: rustwx_render::PngCompressionMode::Default,
    };
    let summary = run_weather_native_profile(&request)?;
    println!("{}", summary.summary_json.display());
    println!("{}", summary.summary_markdown.display());
    println!("{}", summary.bundle_manifest.display());
    Ok(())
}
