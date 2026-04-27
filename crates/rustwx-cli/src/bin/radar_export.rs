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

    #[arg(long)]
    products: Option<String>,

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
    let requested = cli.products.as_deref().unwrap_or(&cli.product);
    let products = parse_products(requested, &file)?;
    if products.len() > 1
        && cli.png.as_ref().is_some_and(|path| {
            path.extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("png"))
        })
    {
        anyhow::bail!("--png must be a directory when rendering multiple products");
    }
    let timestamp = file
        .timestamp_string()
        .replace(" UTC", "")
        .replace([':', '-', ' '], "");
    let png_dir = cli.png.clone().unwrap_or_else(|| out_dir.clone());
    let json_path = cli.json.unwrap_or_else(|| {
        out_dir.join(format!(
            "{}_{}_radar.json",
            site.id.to_lowercase(),
            timestamp
        ))
    });

    let mut written_pngs = Vec::new();
    for product in &products {
        let rendered = render_product_frame(
            &file,
            site,
            *product,
            rustwx_radar::png::RadarPngOptions {
                size: cli.size,
                min_value: cli.min_value.or_else(|| default_min_value(*product)),
                ..Default::default()
            },
        )?;
        let png_path = if products.len() == 1 {
            cli.png
                .clone()
                .filter(|path| {
                    path.extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("png"))
                })
                .unwrap_or_else(|| {
                    out_dir.join(format!(
                        "{}_{}_{}.png",
                        site.id.to_lowercase(),
                        product.short_name().to_lowercase(),
                        timestamp
                    ))
                })
        } else {
            std::fs::create_dir_all(&png_dir)?;
            png_dir.join(format!(
                "{}_{}_{}.png",
                site.id.to_lowercase(),
                product.short_name().to_lowercase(),
                timestamp
            ))
        };
        std::fs::write(&png_path, &rendered.png)?;
        written_pngs.push((product.short_name().to_string(), png_path));
    }

    let ai_frame = build_ai_frame(
        &file,
        site,
        AiExportOptions {
            include_tensor: cli.include_tensor,
            tensor_product: products[0],
            max_tensor_gates: cli.max_tensor_gates,
        },
    );
    std::fs::write(&json_path, serde_json::to_vec_pretty(&ai_frame)?)?;

    for (product, path) in &written_pngs {
        eprintln!("wrote {product} PNG: {}", path.display());
    }
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
        "hca" | "hhc" => Ok(RadarProduct::HydrometeorClass),
        "srv" => Ok(RadarProduct::StormRelativeVelocity),
        "vil" => Ok(RadarProduct::VIL),
        "et" | "echo_tops" | "echotops" => Ok(RadarProduct::EchoTops),
        other => anyhow::bail!(
            "unknown product {other}; use ref, vel, sw, zdr, cc, phi, kdp, hca, srv, vil, et, or all"
        ),
    }
}

fn parse_products(value: &str, file: &Level2File) -> anyhow::Result<Vec<RadarProduct>> {
    if value.eq_ignore_ascii_case("all") {
        let products = rustwx_radar::png::renderable_products(file);
        if products.is_empty() {
            anyhow::bail!("no renderable radar products found in this volume");
        }
        return Ok(products);
    }

    let mut products = Vec::new();
    for part in value.split(',') {
        let product = parse_product(part.trim())?;
        if !products.contains(&product) {
            products.push(product);
        }
    }
    if products.is_empty() {
        anyhow::bail!("no radar products requested");
    }
    Ok(products)
}
