use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use rustwx_radar::nexrad::{sites, Level2File, RadarProduct, RadarSite};
use rustwx_radar::{
    render_product_web_tiles, sweeps_with_hca_inputs, sweeps_with_product, ColorTablePreset,
    DealiasMethod, RadarSweepSelection, RadarTileManifest, RadarTileOptions,
    RadarTilePngCompression,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Render NEXRAD Level-II radar as transparent XYZ Web Mercator tiles")]
struct Cli {
    #[arg(long)]
    site: Option<String>,

    #[arg(long)]
    lat: Option<f64>,

    #[arg(long)]
    lon: Option<f64>,

    #[arg(long)]
    input: Option<PathBuf>,

    #[arg(long)]
    out_dir: PathBuf,

    #[arg(long)]
    cache_dir: Option<PathBuf>,

    #[arg(long, default_value = "ref")]
    product: String,

    #[arg(long)]
    name: Option<String>,

    #[arg(long)]
    west: Option<f64>,

    #[arg(long)]
    east: Option<f64>,

    #[arg(long)]
    south: Option<f64>,

    #[arg(long)]
    north: Option<f64>,

    #[arg(long, default_value_t = 2)]
    min_zoom: u8,

    #[arg(long, default_value_t = 9)]
    max_zoom: u8,

    #[arg(long, default_value_t = 256)]
    tile_size: u32,

    #[arg(long, default_value_t = 1.0)]
    opacity: f64,

    #[arg(long)]
    min_value: Option<f32>,

    #[arg(long, value_enum, default_value_t = ColorTableArg::Default)]
    color_table: ColorTableArg,

    #[arg(long, default_value_t = 1)]
    supersample: u8,

    #[arg(long)]
    benchmark_supersamples: Option<String>,

    #[arg(long)]
    sweep_index: Option<usize>,

    #[arg(long)]
    elevation_deg: Option<f32>,

    #[arg(long, default_value_t = false)]
    all_tilts: bool,

    #[arg(long)]
    base_url: Option<String>,

    #[arg(long, value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,

    #[arg(long, default_value_t = false)]
    keep_empty_tiles: bool,

    /// Treat --west/--south/--east/--north as a hard pixel crop instead of only a tile-selection window.
    #[arg(long, default_value_t = false)]
    clip_to_bounds: bool,

    #[arg(long, default_value_t = false)]
    dealias: bool,

    #[arg(long, value_enum, default_value_t = DealiasMethodArg::Sweep)]
    dealias_method: DealiasMethodArg,

    /// Render the rejected dealias candidate for research/debugging.
    #[arg(long, default_value_t = false)]
    force_rejected_dealias: bool,

    /// Mask velocity gates that fail reflectivity/spectrum-width quality checks before rendering.
    #[arg(long, default_value_t = false)]
    velocity_quality_filter: bool,

    /// Remove isolated reflectivity gates before rendering.
    #[arg(long, default_value_t = false)]
    reflectivity_despeckle: bool,

    #[arg(long, default_value_t = 2)]
    reflectivity_despeckle_min_neighbors: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PngCompressionArg {
    Default,
    Fast,
    Fastest,
}

impl From<PngCompressionArg> for RadarTilePngCompression {
    fn from(value: PngCompressionArg) -> Self {
        match value {
            PngCompressionArg::Default => Self::Default,
            PngCompressionArg::Fast => Self::Fast,
            PngCompressionArg::Fastest => Self::Fastest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ColorTableArg {
    Default,
    #[value(name = "gr2analyst")]
    Gr2Analyst,
    Nssl,
    Classic,
    Dark,
    Colorblind,
}

impl From<ColorTableArg> for ColorTablePreset {
    fn from(value: ColorTableArg) -> Self {
        match value {
            ColorTableArg::Default => Self::Default,
            ColorTableArg::Gr2Analyst => Self::GR2Analyst,
            ColorTableArg::Nssl => Self::NSSL,
            ColorTableArg::Classic => Self::Classic,
            ColorTableArg::Dark => Self::Dark,
            ColorTableArg::Colorblind => Self::Colorblind,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DealiasMethodArg {
    Radial,
    Sweep,
    Staged,
}

impl From<DealiasMethodArg> for DealiasMethod {
    fn from(value: DealiasMethodArg) -> Self {
        match value {
            DealiasMethodArg::Radial => Self::RadialContinuity,
            DealiasMethodArg::Sweep => Self::SweepContinuity,
            DealiasMethodArg::Staged => Self::StagedContinuity,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let site = resolve_site(&cli)?;
    let product = parse_product(&cli.product)?;
    let bounds = parse_optional_bounds(&cli)?;
    let min_value = cli.min_value.or_else(|| default_min_value(product));

    let (raw, source_key_or_url) = load_volume(&cli, site)?;
    let file = Level2File::parse(&raw)?;
    eprintln!(
        "parsed {} sweeps from {} at {}",
        file.sweeps.len(),
        file.station_id,
        file.timestamp_string()
    );

    if let Some(spec) = cli.benchmark_supersamples.as_deref() {
        let manifest = render_supersample_benchmark(
            &cli,
            &file,
            site,
            product,
            bounds,
            min_value,
            &source_key_or_url,
            spec,
        )?;
        println!("{}", serde_json::to_string_pretty(&manifest)?);
        return Ok(());
    }

    if cli.all_tilts {
        let manifest = render_all_tilts(
            &cli,
            &cli.out_dir,
            &file,
            site,
            product,
            bounds,
            min_value,
            &source_key_or_url,
            cli.supersample,
        )?;
        println!("{}", serde_json::to_string_pretty(&manifest)?);
        return Ok(());
    }

    let sweep = parse_sweep_selection(&cli)?;
    let manifest = render_product_web_tiles(
        &file,
        site,
        product,
        &cli.out_dir,
        RadarTileOptions {
            name: cli.name,
            source_key_or_url: Some(source_key_or_url),
            base_url: cli.base_url,
            bounds,
            min_zoom: cli.min_zoom,
            max_zoom: cli.max_zoom,
            tile_size: cli.tile_size,
            opacity: cli.opacity,
            min_value,
            color_table_preset: cli.color_table.into(),
            sample_factor: cli.supersample,
            png_compression: cli.png_compression.into(),
            skip_empty_tiles: !cli.keep_empty_tiles,
            clip_to_bounds: cli.clip_to_bounds,
            sweep,
            dealias_velocity: cli.dealias,
            dealias_method: cli.dealias_method.into(),
            force_rejected_dealias: cli.force_rejected_dealias,
            velocity_quality_filter: cli.velocity_quality_filter,
            reflectivity_despeckle: cli.reflectivity_despeckle,
            reflectivity_despeckle_min_neighbors: cli.reflectivity_despeckle_min_neighbors,
            emit_numeric_sidecar: true,
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

#[derive(Debug, Serialize)]
struct RadarAllTiltsManifest {
    ok: bool,
    site: String,
    product: String,
    source_key_or_url: String,
    scan_time_utc: String,
    tilt_count: usize,
    total_tile_count: usize,
    total_candidate_tile_count: usize,
    total_rendered_pixel_count: u64,
    total_ms: u128,
    sample_factor: u8,
    manifests: Vec<RadarTileManifest>,
}

#[derive(Debug, Serialize)]
struct RadarSupersampleBenchmarkManifest {
    ok: bool,
    site: String,
    product: String,
    source_key_or_url: String,
    scan_time_utc: String,
    all_tilts: bool,
    benchmark_count: usize,
    entries: Vec<RadarSupersampleBenchmarkEntry>,
}

#[derive(Debug, Serialize)]
struct RadarSupersampleBenchmarkEntry {
    sample_factor: u8,
    out_dir: PathBuf,
    manifest_path: PathBuf,
    tilt_count: usize,
    tile_count: usize,
    candidate_tile_count: usize,
    rendered_pixel_count: u64,
    total_ms: u128,
    tiles_per_second: f64,
    png_count: usize,
    png_bytes: u64,
}

fn render_supersample_benchmark(
    cli: &Cli,
    file: &Level2File,
    site: &RadarSite,
    product: RadarProduct,
    bounds: Option<[f64; 4]>,
    min_value: Option<f32>,
    source_key_or_url: &str,
    spec: &str,
) -> anyhow::Result<RadarSupersampleBenchmarkManifest> {
    let factors = parse_supersample_list(spec)?;
    std::fs::create_dir_all(&cli.out_dir)?;
    let sweep = if cli.all_tilts {
        None
    } else {
        Some(parse_sweep_selection(cli)?)
    };

    let mut entries = Vec::with_capacity(factors.len());
    let mut scan_time_utc = String::new();
    for sample_factor in factors {
        let out_dir = cli.out_dir.join(format!("ss{sample_factor}"));
        let base_url = cli
            .base_url
            .as_ref()
            .map(|base| format!("{}/ss{sample_factor}", base.trim_end_matches('/')));
        if cli.all_tilts {
            let manifest = render_all_tilts(
                cli,
                &out_dir,
                file,
                site,
                product,
                bounds,
                min_value,
                source_key_or_url,
                sample_factor,
            )?;
            if scan_time_utc.is_empty() {
                scan_time_utc = manifest.scan_time_utc.clone();
            }
            let (png_count, png_bytes) = png_count_and_bytes(&out_dir)?;
            entries.push(RadarSupersampleBenchmarkEntry {
                sample_factor,
                out_dir: out_dir.clone(),
                manifest_path: out_dir.join("all_tilts_manifest.json"),
                tilt_count: manifest.tilt_count,
                tile_count: manifest.total_tile_count,
                candidate_tile_count: manifest.total_candidate_tile_count,
                rendered_pixel_count: manifest.total_rendered_pixel_count,
                total_ms: manifest.total_ms,
                tiles_per_second: if manifest.total_ms > 0 {
                    manifest.total_candidate_tile_count as f64 / (manifest.total_ms as f64 / 1000.0)
                } else {
                    manifest.total_candidate_tile_count as f64
                },
                png_count,
                png_bytes,
            });
        } else {
            let manifest = render_product_web_tiles(
                file,
                site,
                product,
                &out_dir,
                RadarTileOptions {
                    name: cli
                        .name
                        .as_ref()
                        .map(|name| format!("{name}_ss{sample_factor}")),
                    source_key_or_url: Some(source_key_or_url.to_string()),
                    base_url,
                    bounds,
                    min_zoom: cli.min_zoom,
                    max_zoom: cli.max_zoom,
                    tile_size: cli.tile_size,
                    opacity: cli.opacity,
                    min_value,
                    color_table_preset: cli.color_table.into(),
                    sample_factor,
                    png_compression: cli.png_compression.into(),
                    skip_empty_tiles: !cli.keep_empty_tiles,
                    clip_to_bounds: cli.clip_to_bounds,
                    sweep: sweep.expect("single-sweep benchmark selection"),
                    dealias_velocity: cli.dealias,
                    dealias_method: cli.dealias_method.into(),
                    force_rejected_dealias: cli.force_rejected_dealias,
                    velocity_quality_filter: cli.velocity_quality_filter,
                    reflectivity_despeckle: cli.reflectivity_despeckle,
                    reflectivity_despeckle_min_neighbors: cli.reflectivity_despeckle_min_neighbors,
                    emit_numeric_sidecar: true,
                },
            )?;
            if scan_time_utc.is_empty() {
                scan_time_utc = manifest.scan_time_utc.clone();
            }
            let (png_count, png_bytes) = png_count_and_bytes(&out_dir)?;
            entries.push(RadarSupersampleBenchmarkEntry {
                sample_factor,
                out_dir: out_dir.clone(),
                manifest_path: out_dir.join("tiles_manifest.json"),
                tilt_count: 1,
                tile_count: manifest.tile_count,
                candidate_tile_count: manifest.candidate_tile_count,
                rendered_pixel_count: manifest.rendered_pixel_count,
                total_ms: manifest.total_ms,
                tiles_per_second: manifest.tiles_per_second,
                png_count,
                png_bytes,
            });
        }
    }

    let summary = RadarSupersampleBenchmarkManifest {
        ok: true,
        site: site.id.to_string(),
        product: product.short_name().to_ascii_lowercase(),
        source_key_or_url: source_key_or_url.to_string(),
        scan_time_utc,
        all_tilts: cli.all_tilts,
        benchmark_count: entries.len(),
        entries,
    };
    std::fs::write(
        cli.out_dir.join("supersample_benchmark.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

fn render_all_tilts(
    cli: &Cli,
    out_dir: &Path,
    file: &Level2File,
    site: &RadarSite,
    product: RadarProduct,
    bounds: Option<[f64; 4]>,
    min_value: Option<f32>,
    source_key_or_url: &str,
    sample_factor: u8,
) -> anyhow::Result<RadarAllTiltsManifest> {
    if cli.sweep_index.is_some() || cli.elevation_deg.is_some() {
        anyhow::bail!("--all-tilts cannot be combined with --sweep-index or --elevation-deg");
    }
    let sweep_entries = if product == RadarProduct::HydrometeorClass {
        sweeps_with_hca_inputs(file)
    } else {
        let sample_product = sweep_sample_product(product)?;
        sweeps_with_product(file, sample_product)
    };
    if sweep_entries.is_empty() {
        anyhow::bail!("no sweeps can render {}", product.short_name());
    }

    std::fs::create_dir_all(out_dir)?;
    let mut manifests = Vec::with_capacity(sweep_entries.len());
    for (sweep_index, sweep) in sweep_entries {
        let slug = sweep_slug(sweep_index, sweep.elevation_angle);
        let out_dir = out_dir.join(&slug);
        let base_url = cli
            .base_url
            .as_ref()
            .map(|base| format!("{}/{}", base.trim_end_matches('/'), slug));
        let name = cli
            .name
            .as_ref()
            .map(|name| format!("{name}_{slug}"))
            .or_else(|| Some(format!("{}_{}", default_layer_name(site, product), slug)));
        let manifest = render_product_web_tiles(
            file,
            site,
            product,
            &out_dir,
            RadarTileOptions {
                name,
                source_key_or_url: Some(source_key_or_url.to_string()),
                base_url,
                bounds,
                min_zoom: cli.min_zoom,
                max_zoom: cli.max_zoom,
                tile_size: cli.tile_size,
                opacity: cli.opacity,
                min_value,
                color_table_preset: cli.color_table.into(),
                sample_factor,
                png_compression: cli.png_compression.into(),
                skip_empty_tiles: !cli.keep_empty_tiles,
                clip_to_bounds: cli.clip_to_bounds,
                sweep: RadarSweepSelection::Index(sweep_index),
                dealias_velocity: cli.dealias,
                dealias_method: cli.dealias_method.into(),
                force_rejected_dealias: cli.force_rejected_dealias,
                velocity_quality_filter: cli.velocity_quality_filter,
                reflectivity_despeckle: cli.reflectivity_despeckle,
                reflectivity_despeckle_min_neighbors: cli.reflectivity_despeckle_min_neighbors,
                emit_numeric_sidecar: true,
            },
        )?;
        manifests.push(manifest);
    }

    let total_tile_count = manifests.iter().map(|manifest| manifest.tile_count).sum();
    let total_candidate_tile_count = manifests
        .iter()
        .map(|manifest| manifest.candidate_tile_count)
        .sum();
    let total_rendered_pixel_count = manifests
        .iter()
        .map(|manifest| manifest.rendered_pixel_count)
        .sum();
    let total_ms = manifests.iter().map(|manifest| manifest.total_ms).sum();
    let scan_time_utc = manifests
        .first()
        .map(|manifest| manifest.scan_time_utc.clone())
        .unwrap_or_default();
    let summary = RadarAllTiltsManifest {
        ok: true,
        site: site.id.to_string(),
        product: product.short_name().to_ascii_lowercase(),
        source_key_or_url: source_key_or_url.to_string(),
        scan_time_utc,
        tilt_count: manifests.len(),
        total_tile_count,
        total_candidate_tile_count,
        total_rendered_pixel_count,
        total_ms,
        sample_factor,
        manifests,
    };
    std::fs::write(
        out_dir.join("all_tilts_manifest.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
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

fn load_volume(cli: &Cli, site: &RadarSite) -> anyhow::Result<(Vec<u8>, String)> {
    if let Some(input) = &cli.input {
        eprintln!("loading local Level-II volume: {}", input.display());
        return Ok((std::fs::read(input)?, input.display().to_string()));
    }

    eprintln!(
        "resolving latest public NEXRAD Level-II volume for {}",
        site.id
    );
    let object = rustwx_radar::aws::latest_object(site.id)?;
    let cache_path = cli
        .cache_dir
        .as_ref()
        .map(|cache_dir| radar_cache_path(cache_dir, &object.key));
    if let Some(cache_path) = cache_path.as_ref() {
        if cache_path.is_file() {
            eprintln!("using cached {}", cache_path.display());
            return Ok((std::fs::read(cache_path)?, object.key));
        }
    }

    eprintln!(
        "downloading {} ({} bytes)",
        object.display_name, object.size
    );
    let bytes = rustwx_radar::aws::fetch_object(&object.key)?;
    if let Some(cache_path) = cache_path.as_ref() {
        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(cache_path, &bytes)?;
    }
    Ok((bytes, object.key))
}

fn radar_cache_path(cache_dir: &Path, key: &str) -> PathBuf {
    let safe_key = key
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' => '_',
            other => other,
        })
        .collect::<String>();
    cache_dir.join("radar_level2").join(safe_key)
}

fn parse_optional_bounds(cli: &Cli) -> anyhow::Result<Option<[f64; 4]>> {
    let values = [cli.west, cli.south, cli.east, cli.north];
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    if !values.iter().all(Option::is_some) {
        anyhow::bail!("provide all of --west, --south, --east, and --north, or none");
    }
    Ok(Some([
        cli.west.unwrap(),
        cli.south.unwrap(),
        cli.east.unwrap(),
        cli.north.unwrap(),
    ]))
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

fn parse_supersample_list(spec: &str) -> anyhow::Result<Vec<u8>> {
    let mut factors = Vec::new();
    for item in spec.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let factor = item
            .parse::<u8>()
            .map_err(|_| anyhow::anyhow!("invalid supersample factor {item:?}"))?;
        if !(1..=4).contains(&factor) {
            anyhow::bail!("supersample factors must be in 1..=4");
        }
        if !factors.contains(&factor) {
            factors.push(factor);
        }
    }
    if factors.is_empty() {
        anyhow::bail!("--benchmark-supersamples must include at least one factor");
    }
    Ok(factors)
}

fn png_count_and_bytes(root: &Path) -> anyhow::Result<(usize, u64)> {
    let mut stack = vec![root.to_path_buf()];
    let mut count = 0usize;
    let mut bytes = 0u64;
    while let Some(dir) = stack.pop() {
        if !dir.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("png"))
            {
                count += 1;
                bytes += entry.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            }
        }
    }
    Ok((count, bytes))
}

fn sweep_sample_product(product: RadarProduct) -> anyhow::Result<RadarProduct> {
    match product {
        RadarProduct::StormRelativeVelocity => Ok(RadarProduct::Velocity),
        RadarProduct::SpecificDiffPhase => Ok(RadarProduct::DifferentialPhase),
        RadarProduct::VIL | RadarProduct::EchoTops => anyhow::bail!(
            "{} is volume-derived and does not have per-tilt sweeps",
            product.short_name()
        ),
        _ => Ok(product.base_product()),
    }
}

fn default_layer_name(site: &RadarSite, product: RadarProduct) -> String {
    format!(
        "{}_{}",
        site.id.to_ascii_lowercase(),
        product.short_name().to_ascii_lowercase()
    )
}

fn sweep_slug(sweep_index: usize, elevation_deg: f32) -> String {
    format!("sweep{sweep_index:02}_el{}", elevation_slug(elevation_deg))
}

fn elevation_slug(elevation_deg: f32) -> String {
    format!("{elevation_deg:.2}")
        .replace('-', "m")
        .replace('.', "p")
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
            "unknown product {other}; use ref, vel, sw, zdr, cc, phi, kdp, hca, srv, vil, or et"
        ),
    }
}

fn default_min_value(product: RadarProduct) -> Option<f32> {
    match product {
        RadarProduct::Reflectivity | RadarProduct::SuperResReflectivity => Some(10.0),
        _ => None,
    }
}
