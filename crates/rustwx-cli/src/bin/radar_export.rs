use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use rustwx_radar::nexrad::{sites, Level2File, RadarProduct, RadarSite};
use rustwx_radar::render::RenderMode;
use rustwx_radar::{
    build_ai_frame, render_product_frame, sweeps_with_hca_inputs, sweeps_with_product,
    AiExportOptions, RadarSweepSelection,
};

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

    #[arg(long, value_enum, default_value_t = RenderModeArg::Classic)]
    render_mode: RenderModeArg,

    #[arg(long, default_value_t = false)]
    dealias: bool,

    #[arg(long)]
    min_value: Option<f32>,

    #[arg(long)]
    sweep_index: Option<usize>,

    #[arg(long)]
    elevation_deg: Option<f32>,

    #[arg(long, default_value_t = false)]
    all_tilts: bool,

    #[arg(long, default_value_t = false)]
    list_sweeps: bool,

    #[arg(long)]
    png: Option<PathBuf>,

    #[arg(long)]
    json: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    include_tensor: bool,

    #[arg(long, default_value_t = 800)]
    max_tensor_gates: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RenderModeArg {
    Classic,
    Smooth,
}

impl From<RenderModeArg> for RenderMode {
    fn from(value: RenderModeArg) -> Self {
        match value {
            RenderModeArg::Classic => Self::Classic,
            RenderModeArg::Smooth => Self::Smooth,
        }
    }
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

    if cli.list_sweeps {
        print_sweeps(&file);
        return Ok(());
    }

    let out_dir = PathBuf::from("target").join("radar_export");
    std::fs::create_dir_all(&out_dir)?;
    let requested = cli.products.as_deref().unwrap_or(&cli.product);
    let products = parse_products(requested, &file)?;
    let render_requests = build_render_requests(&file, &products, &cli)?;
    if render_requests.len() > 1
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
    for request in &render_requests {
        let rendered = render_product_frame(
            &file,
            site,
            request.product,
            rustwx_radar::png::RadarPngOptions {
                size: cli.size,
                min_value: cli.min_value.or_else(|| default_min_value(request.product)),
                sweep: request.selection,
                render_mode: cli.render_mode.into(),
                dealias_velocity: cli.dealias,
                ..Default::default()
            },
        )?;
        let png_path = if render_requests.len() == 1 {
            cli.png
                .clone()
                .filter(|path| {
                    path.extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("png"))
                })
                .unwrap_or_else(|| {
                    out_dir.join(format!(
                        "{}_{}_{}_sweep{:02}_el{}.png",
                        site.id.to_lowercase(),
                        request.product.short_name().to_lowercase(),
                        timestamp,
                        rendered.sweep_index,
                        elevation_slug(rendered.elevation_deg)
                    ))
                })
        } else {
            std::fs::create_dir_all(&png_dir)?;
            png_dir.join(format!(
                "{}_{}_{}_sweep{:02}_el{}.png",
                site.id.to_lowercase(),
                request.product.short_name().to_lowercase(),
                timestamp,
                rendered.sweep_index,
                elevation_slug(rendered.elevation_deg)
            ))
        };
        std::fs::write(&png_path, &rendered.png)?;
        written_pngs.push((
            format!(
                "{} sweep={} elevation={:.2}",
                request.product.short_name(),
                rendered.sweep_index,
                rendered.elevation_deg
            ),
            png_path,
        ));
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

    for (label, path) in &written_pngs {
        eprintln!("wrote {label} PNG: {}", path.display());
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

#[derive(Debug, Clone, Copy)]
struct RenderRequest {
    product: RadarProduct,
    selection: RadarSweepSelection,
}

fn build_render_requests(
    file: &Level2File,
    products: &[RadarProduct],
    cli: &Cli,
) -> anyhow::Result<Vec<RenderRequest>> {
    let selection = parse_sweep_selection(cli)?;
    if cli.all_tilts && selection != RadarSweepSelection::Lowest {
        anyhow::bail!("--all-tilts cannot be combined with --sweep-index or --elevation-deg");
    }
    let mut requests = Vec::new();
    for product in products {
        if cli.all_tilts {
            let before = requests.len();
            let sweeps = if *product == RadarProduct::HydrometeorClass {
                sweeps_with_hca_inputs(file)
            } else {
                let sample_product = sweep_sample_product(*product)?;
                sweeps_with_product(file, sample_product)
            };
            for (sweep_index, _) in sweeps {
                requests.push(RenderRequest {
                    product: *product,
                    selection: RadarSweepSelection::Index(sweep_index),
                });
            }
            if requests.len() == before {
                anyhow::bail!("no sweeps contain {}", product.short_name());
            }
        } else {
            requests.push(RenderRequest {
                product: *product,
                selection,
            });
        }
    }
    Ok(requests)
}

fn parse_sweep_selection(cli: &Cli) -> anyhow::Result<RadarSweepSelection> {
    match (cli.sweep_index, cli.elevation_deg) {
        (Some(_), Some(_)) => {
            anyhow::bail!("use either --sweep-index or --elevation-deg, not both")
        }
        (Some(index), None) => Ok(RadarSweepSelection::Index(index)),
        (None, Some(elevation)) => Ok(RadarSweepSelection::NearestElevation(elevation)),
        (None, None) => Ok(RadarSweepSelection::Lowest),
    }
}

fn sweep_sample_product(product: RadarProduct) -> anyhow::Result<RadarProduct> {
    match product {
        RadarProduct::StormRelativeVelocity => Ok(RadarProduct::Velocity),
        RadarProduct::SpecificDiffPhase => Ok(RadarProduct::DifferentialPhase),
        RadarProduct::VIL | RadarProduct::EchoTops => anyhow::bail!(
            "{} is a volume-derived product and does not have per-tilt sweeps",
            product.short_name()
        ),
        _ => Ok(product.base_product()),
    }
}

fn print_sweeps(file: &Level2File) {
    println!(
        "sweeps for {} at {}",
        file.station_id,
        file.timestamp_string()
    );
    let renderable = rustwx_radar::png::renderable_products(file);
    println!("renderable products={}", product_names(&renderable));
    for (index, sweep) in file.sweeps.iter().enumerate() {
        let products = sweep
            .radials
            .iter()
            .flat_map(|radial| radial.moments.iter().map(|moment| moment.product))
            .fold(Vec::<RadarProduct>::new(), |mut products, product| {
                if !products.contains(&product) && product != RadarProduct::Unknown {
                    products.push(product);
                }
                products
            });
        println!(
            "  sweep={index:02} elevation={:.2} radials={} products={}",
            sweep.elevation_angle,
            sweep.radials.len(),
            product_names(&products)
        );
    }
}

fn product_names(products: &[RadarProduct]) -> String {
    products
        .iter()
        .map(RadarProduct::short_name)
        .collect::<Vec<_>>()
        .join(",")
}

fn elevation_slug(elevation_deg: f32) -> String {
    format!("{elevation_deg:.2}")
        .replace('-', "m")
        .replace('.', "p")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_tilts_samples_phi_for_derived_kdp() {
        assert_eq!(
            sweep_sample_product(RadarProduct::SpecificDiffPhase).unwrap(),
            RadarProduct::DifferentialPhase
        );
    }

    #[test]
    fn product_names_uses_short_names() {
        assert_eq!(
            product_names(&[
                RadarProduct::DifferentialPhase,
                RadarProduct::SpecificDiffPhase
            ]),
            "PHI,KDP"
        );
    }
}
