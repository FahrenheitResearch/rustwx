use std::path::PathBuf;

use clap::Parser;
use rustwx_radar::nexrad::{Level2File, RadarProduct, RadarSite, sites};
use rustwx_radar::{AiExportOptions, build_ai_frame, render_product_frame};

#[derive(Parser)]
#[command(
    name = "radar_export",
    about = "Render NEXRAD Level-II radar PNGs and AI-consumable feature JSON"
)]
struct Cli {
    #[arg(long)]
    site: Option<String>,

    #[arg(long)]
    lat: Option<f64>,

    #[arg(long)]
    lon: Option<f64>,

    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long, default_value = "ref")]
    product: String,

    #[arg(long, default_value_t = 1024)]
    size: u32,

    #[arg(long)]
    min_value: Option<f32>,

    #[arg(long)]
    png: Option<PathBuf>,

    #[arg(long)]
    json: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    include_tensor: bool,

    #[arg(long, default_value_t = 800)]
    max_tensor_gates: usize,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let site = resolve_site(&cli)?;
    let product = parse_product(&cli.product)?;

    let raw = if let Some(input) = &cli.input {
        eprintln!("loading local Level-II volume: {}", input.display());
        std::fs::read(input)?
    } else {
        eprintln!(
            "fetching latest public NEXRAD Level-II volume for {}",
            site.id
        );
        let download = rustwx_radar::aws::fetch_latest(site.id)?;
        eprintln!(
            "downloaded {} ({} bytes)",
            download.object.display_name,
            download.bytes.len()
        );
        download.bytes
    };

    let file = Level2File::parse(&raw)?;
    eprintln!(
        "parsed {} sweeps from {} at {}",
        file.sweeps.len(),
        file.station_id,
        file.timestamp_string()
    );

    let out_dir = PathBuf::from("target").join("radar_export");
    std::fs::create_dir_all(&out_dir)?;
    let stem = format!(
        "{}_{}_{}",
        site.id.to_lowercase(),
        product.short_name().to_lowercase(),
        file.timestamp_string()
            .replace(" UTC", "")
            .replace([':', '-', ' '], "")
    );
    let png_path = cli
        .png
        .unwrap_or_else(|| out_dir.join(format!("{stem}.png")));
    let json_path = cli
        .json
        .unwrap_or_else(|| out_dir.join(format!("{stem}.json")));

    let rendered = render_product_frame(
        &file,
        site,
        product,
        rustwx_radar::png::RadarPngOptions {
            size: cli.size,
            min_value: cli.min_value.or_else(|| default_min_value(product)),
            ..Default::default()
        },
    )?;
    std::fs::write(&png_path, &rendered.png)?;

    let ai_frame = build_ai_frame(
        &file,
        site,
        AiExportOptions {
            include_tensor: cli.include_tensor,
            tensor_product: product,
            max_tensor_gates: cli.max_tensor_gates,
        },
    );
    std::fs::write(&json_path, serde_json::to_vec_pretty(&ai_frame)?)?;

    eprintln!("wrote PNG: {}", png_path.display());
    eprintln!("wrote JSON: {}", json_path.display());
    eprintln!(
        "features: {} cells, {} mesos, {} tvs, {} hail, {} tds candidates",
        ai_frame.storm_cells.len(),
        ai_frame.mesocyclones.len(),
        ai_frame.tvs.len(),
        ai_frame.hail.len(),
        ai_frame.tds_candidates.len()
    );

    Ok(())
}

fn default_min_value(product: RadarProduct) -> Option<f32> {
    match product {
        RadarProduct::Reflectivity | RadarProduct::SuperResReflectivity => Some(10.0),
        _ => None,
    }
}

fn resolve_site(cli: &Cli) -> anyhow::Result<&'static RadarSite> {
    if let Some(site) = &cli.site {
        return sites::find_site(site)
            .ok_or_else(|| anyhow::anyhow!("unknown NEXRAD site {}", site));
    }
    if let (Some(lat), Some(lon)) = (cli.lat, cli.lon) {
        return sites::find_nearest_site(lat, lon)
            .ok_or_else(|| anyhow::anyhow!("no radar sites are available"));
    }
    anyhow::bail!("provide --site or both --lat and --lon")
}

fn parse_product(value: &str) -> anyhow::Result<RadarProduct> {
    match value.to_ascii_lowercase().as_str() {
        "ref" | "reflectivity" => Ok(RadarProduct::Reflectivity),
        "vel" | "velocity" => Ok(RadarProduct::Velocity),
        "sw" | "spectrum_width" => Ok(RadarProduct::SpectrumWidth),
        "zdr" => Ok(RadarProduct::DifferentialReflectivity),
        "cc" | "rho" => Ok(RadarProduct::CorrelationCoefficient),
        "phi" => Ok(RadarProduct::DifferentialPhase),
        "kdp" => Ok(RadarProduct::SpecificDiffPhase),
        "srv" => Ok(RadarProduct::StormRelativeVelocity),
        "vil" => Ok(RadarProduct::VIL),
        other => anyhow::bail!(
            "unknown product {other}; use ref, vel, sw, zdr, cc, phi, kdp, srv, or vil"
        ),
    }
}
