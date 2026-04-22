use std::fs;
use std::path::PathBuf;

#[path = "../metro.rs"]
mod metro;
#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use metro::{MAJOR_US_CITY_PRESETS, major_us_city_domains};
use region::{RegionPreset, conus_plus_us_split_region_domains};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::non_ecape::{
    HrrrNonEcapeMultiDomainRequest, run_hrrr_non_ecape_hour_multi_domain,
};
use rustwx_products::places::{PlaceLabelDensityTier, PlaceLabelOverlay};
use rustwx_products::publication::atomic_write_json;
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use rustwx_render::PngCompressionMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ScopeArg {
    Sample,
    AllRegions,
    AllCities,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[value(rename_all = "kebab-case")]
enum PlaceLabelDensityArg {
    /// Disable place labels.
    #[value(alias("0"), alias("off"))]
    None,
    /// Major anchor labels only.
    #[default]
    #[value(alias("1"))]
    Major,
    /// Major anchors plus nearby auxiliary labels.
    #[value(alias("2"))]
    MajorAndAux,
    /// The densest supported label set.
    #[value(alias("3"), alias("full"))]
    Dense,
}

impl From<PlaceLabelDensityArg> for PlaceLabelDensityTier {
    fn from(value: PlaceLabelDensityArg) -> Self {
        match value {
            PlaceLabelDensityArg::None => Self::None,
            PlaceLabelDensityArg::Major => Self::Major,
            PlaceLabelDensityArg::MajorAndAux => Self::MajorAndAux,
            PlaceLabelDensityArg::Dense => Self::Dense,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-place-label-proof",
    about = "Generate a tight HRRR proof set for region and metro crop place labels"
)]
struct Args {
    #[arg(long, default_value = "20260422")]
    date: String,
    #[arg(long, default_value_t = 7)]
    cycle: u8,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: rustwx_core::SourceId,
    #[arg(
        long,
        default_value = "C:\\Users\\drew\\rustwx-next\\rustwx\\proof\\place_labels"
    )]
    out_dir: PathBuf,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long, default_value_t = 1400)]
    width: u32,
    #[arg(long, default_value_t = 1000)]
    height: u32,
    #[arg(long, value_enum, default_value_t = ScopeArg::Sample)]
    scope: ScopeArg,
    #[arg(
        long = "place-label-density",
        value_enum,
        default_value_t = PlaceLabelDensityArg::Major,
        help = "Place-label density: none, major, major-and-aux, or dense. Numeric aliases 0-3 also work."
    )]
    place_label_density: PlaceLabelDensityArg,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run(&args)
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(&args.out_dir)?;
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    unsafe {
        std::env::set_var("RUSTWX_RENDER_THREADS", "1");
    }

    let domains = domains_for_scope(args.scope)?;

    let request = HrrrNonEcapeMultiDomainRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: Some(args.cycle),
        forecast_hour: args.forecast_hour,
        source: args.source,
        domains,
        out_dir: args.out_dir.clone(),
        cache_root,
        use_cache: !args.no_cache,
        source_mode: ProductSourceMode::Fastest,
        direct_recipe_slugs: vec![
            "visibility".to_string(),
            "500mb_temperature_height_winds".to_string(),
        ],
        derived_recipe_slugs: Vec::new(),
        windowed_products: Vec::new(),
        output_width: args.width,
        output_height: args.height,
        png_compression: PngCompressionMode::Fast,
        custom_poi_overlay: None,
        place_label_overlay: Some(
            PlaceLabelOverlay::major_us_cities().with_density(args.place_label_density.into()),
        ),
        domain_jobs: Some(4),
    };

    let report = run_hrrr_non_ecape_hour_multi_domain(&request)?;
    let report_path = args.out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_place_label_proof_report.json",
        report.date_yyyymmdd, report.cycle_utc, report.forecast_hour
    ));
    atomic_write_json(&report_path, &report)?;
    println!("{}", report_path.display());
    Ok(())
}

fn city_domain(slug: &str) -> Result<DomainSpec, Box<dyn std::error::Error>> {
    let preset = MAJOR_US_CITY_PRESETS
        .iter()
        .find(|preset| preset.slug == slug)
        .ok_or_else(|| format!("missing city preset {slug}"))?;
    Ok(preset.domain())
}

fn domains_for_scope(scope: ScopeArg) -> Result<Vec<DomainSpec>, Box<dyn std::error::Error>> {
    match scope {
        ScopeArg::Sample => Ok(vec![
            DomainSpec::new(
                RegionPreset::CaliforniaSquare.slug(),
                RegionPreset::CaliforniaSquare.bounds(),
            ),
            DomainSpec::new(
                RegionPreset::SouthernPlains.slug(),
                RegionPreset::SouthernPlains.bounds(),
            ),
            city_domain("ca_los_angeles")?,
            city_domain("ca_san_francisco_bay")?,
            city_domain("ca_sacramento")?,
            city_domain("ca_san_diego")?,
        ]),
        ScopeArg::AllRegions => Ok(conus_plus_us_split_region_domains()),
        ScopeArg::AllCities => Ok(major_us_city_domains()),
        ScopeArg::All => {
            let mut domains = conus_plus_us_split_region_domains();
            domains.extend(major_us_city_domains());
            Ok(domains)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Args, PlaceLabelDensityArg};
    use clap::Parser;

    #[test]
    fn place_label_density_accepts_named_and_numeric_values() {
        let named = Args::try_parse_from([
            "hrrr-place-label-proof",
            "--place-label-density",
            "major-and-aux",
        ])
        .expect("named density should parse");
        assert_eq!(named.place_label_density, PlaceLabelDensityArg::MajorAndAux);

        let numeric =
            Args::try_parse_from(["hrrr-place-label-proof", "--place-label-density", "2"])
                .expect("numeric density alias should parse");
        assert_eq!(
            numeric.place_label_density,
            PlaceLabelDensityArg::MajorAndAux
        );
    }
}
