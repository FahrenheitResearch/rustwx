use std::path::PathBuf;

use clap::Parser;
use rustwx_cli::cross_section_proof::{
    PressureCrossSectionRequest, ProofProductArg, RoutePresetArg, resolve_route,
    run_pressure_cross_section,
};
use rustwx_core::{ModelId, SourceId};
use rustwx_cross_section::CrossSectionPalette;

#[derive(Debug, Parser)]
#[command(
    name = "cross_section_proof",
    about = "Generate projected cross-section proofs for any supported rustwx model"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long, value_enum, default_value_t = RoutePresetArg::AmarilloChicago)]
    route: RoutePresetArg,
    #[arg(long, value_enum, default_value_t = ProofProductArg::Temperature)]
    product: ProofProductArg,
    #[arg(long)]
    palette: Option<String>,
    #[arg(long, default_value = "20260414")]
    date: String,
    #[arg(long, default_value_t = 23)]
    cycle: u8,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: SourceId,
    #[arg(long)]
    surface_product: Option<String>,
    #[arg(long)]
    pressure_product: Option<String>,
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
    let palette = match args.palette {
        Some(name) => Some(
            CrossSectionPalette::from_name(&name)
                .ok_or_else(|| format!("unknown cross-section palette '{name}'"))?,
        ),
        None => None,
    };
    let request = PressureCrossSectionRequest {
        model: args.model,
        date: args.date,
        cycle: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        route,
        product: args.product.product(),
        palette,
        sample_count: args.sample_count,
        out_dir: args.out_dir,
        cache_dir: args.cache_dir,
        use_cache: !args.no_cache,
        show_wind_overlay: !args.no_wind_overlay,
        surface_product_override: args.surface_product,
        pressure_product_override: args.pressure_product,
    };
    let rendered = run_pressure_cross_section(&request)?;
    println!("{}", rendered.output_path.display());
    println!("{}", rendered.summary_path.display());
    Ok(())
}
