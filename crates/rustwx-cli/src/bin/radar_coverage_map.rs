use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use rustwx_radar::nexrad::{
    RadarSite,
    sites::{RADAR_SITES, haversine_km},
};
use rustwx_radar::radar_polar_to_lat_lon;
use rustwx_render::{
    BasemapDetail, BasemapStyle, Color, ColorScale, DiscreteColorScale, DomainFrame, ExtendMode,
    Field2D, GridShape, LambertConformal, LatLonGrid, MapRenderRequest, PolygonRole, ProductKey,
    ProjectedLabelPlacement, ProjectedLineOverlay, ProjectedMapBuildOptions, ProjectedMarkerShape,
    ProjectedPlaceLabel, ProjectedPlaceLabelPriority, ProjectedPlaceLabelStyle,
    ProjectedPointOverlay, RasterSampleMode, build_projected_map_with_options,
    load_styled_basemap_polygons_for, save_png,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Render first-order NEXRAD radar coverage gaps over a US region")]
struct Cli {
    #[arg(long, value_enum, default_value_t = RegionArg::Conus)]
    region: RegionArg,

    #[arg(long)]
    west: Option<f64>,

    #[arg(long)]
    east: Option<f64>,

    #[arg(long)]
    south: Option<f64>,

    #[arg(long)]
    north: Option<f64>,

    #[arg(long, default_value_t = 230.0)]
    range_km: f64,

    #[arg(long, default_value_t = 560)]
    nx: usize,

    #[arg(long, default_value_t = 340)]
    ny: usize,

    #[arg(long, default_value_t = 1400)]
    width: u32,

    #[arg(long, default_value_t = 620)]
    height: u32,

    #[arg(long)]
    out: Option<PathBuf>,

    #[arg(long)]
    summary_json: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    no_rings: bool,

    #[arg(long, default_value_t = false)]
    label_sites: bool,

    #[arg(long, default_value_t = false)]
    gpu_datacenters: bool,

    #[arg(long, default_value_t = false)]
    label_gpu_datacenters: bool,

    #[arg(long, default_value_t = false)]
    include_water: bool,

    #[arg(long, default_value_t = 10)]
    hotspots: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RegionArg {
    Conus,
    Alaska,
    Hawaii,
    PuertoRico,
    Guam,
}

#[derive(Debug, Clone, Copy)]
struct RegionSpec {
    slug: &'static str,
    label: &'static str,
    bounds: Bounds,
    projection: LambertConformal,
    land_clip: Option<&'static [(f64, f64)]>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct Bounds {
    west: f64,
    east: f64,
    south: f64,
    north: f64,
}

const CONUS_APPROX_CLIP: &[(f64, f64)] = &[
    (-124.9, 49.1),
    (-124.6, 45.6),
    (-124.4, 42.0),
    (-123.2, 39.0),
    (-122.0, 37.0),
    (-119.9, 34.7),
    (-117.2, 32.5),
    (-114.7, 32.5),
    (-111.0, 31.3),
    (-108.2, 31.3),
    (-106.5, 31.8),
    (-104.9, 30.1),
    (-102.0, 29.8),
    (-99.0, 26.0),
    (-97.0, 25.8),
    (-94.0, 28.8),
    (-90.0, 29.0),
    (-85.0, 29.5),
    (-82.0, 24.4),
    (-80.0, 25.0),
    (-80.0, 31.0),
    (-77.0, 34.5),
    (-74.0, 39.5),
    (-67.0, 44.7),
    (-68.8, 47.5),
    (-70.8, 45.6),
    (-73.4, 45.0),
    (-76.5, 44.0),
    (-79.0, 42.8),
    (-82.5, 43.3),
    (-83.3, 46.2),
    (-86.5, 46.7),
    (-89.0, 48.1),
    (-95.0, 49.1),
    (-110.0, 49.1),
    (-124.9, 49.1),
];

#[derive(Debug, Clone)]
struct GridSample {
    lat: f64,
    lon: f64,
    nearest_distance_km: f64,
    coverage_gap_km: f64,
    nearest_site: &'static RadarSite,
}

#[derive(Debug, Clone)]
struct LandMask {
    polygons: Vec<LandMaskPolygon>,
    clip: Option<&'static [(f64, f64)]>,
}

#[derive(Debug, Clone)]
struct LandMaskPolygon {
    rings: Vec<Vec<(f64, f64)>>,
    bbox: Bounds,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ComputeCampusCategory {
    AiGpu,
    Hyperscale,
}

#[derive(Debug, Clone, Copy)]
struct GpuDataCenterSite {
    category: ComputeCampusCategory,
    label: &'static str,
    operator: &'static str,
    approximate_location: &'static str,
    lat: f64,
    lon: f64,
    status: &'static str,
    evidence: &'static str,
    source_url: &'static str,
    label_placement: ProjectedLabelPlacement,
}

const GPU_DATACENTER_SITES: &[GpuDataCenterSite] = &[
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "xAI Colossus",
        operator: "xAI",
        approximate_location: "Memphis, TN",
        lat: 35.1495,
        lon: -90.0490,
        status: "operational / expanding",
        evidence: "NVIDIA reported a 100,000 Hopper GPU Colossus cluster in Memphis, with xAI in the process of doubling to 200,000 GPUs.",
        source_url: "https://nvidianews.nvidia.com/news/spectrum-x-ethernet-networking-xai-colossus",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "Stargate Abilene",
        operator: "OpenAI / Oracle / SoftBank",
        approximate_location: "Abilene, TX",
        lat: 32.4487,
        lon: -99.7331,
        status: "operational / expanding",
        evidence: "OpenAI described Abilene as Stargate's flagship campus, already up and running on OCI, with NVIDIA GB200 racks delivered.",
        source_url: "https://openai.com/index/five-new-stargate-sites/",
        label_placement: ProjectedLabelPlacement::AboveLeft,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "CoreWeave Lancaster",
        operator: "CoreWeave",
        approximate_location: "Lancaster, PA",
        lat: 40.0379,
        lon: -76.3055,
        status: "announced / buildout",
        evidence: "CoreWeave announced up to $6 billion to equip a Lancaster AI data center, initially 100 MW with potential expansion to 300 MW.",
        source_url: "https://investors.coreweave.com/news/news-details/2025/CoreWeave-Announces-Multi-Billion-Dollar-Commitment-to-AI-Infrastructure-in-Pennsylvania/default.aspx",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "Microsoft Fairwater",
        operator: "Microsoft",
        approximate_location: "Mount Pleasant, WI",
        lat: 42.7261,
        lon: -87.7829,
        status: "final build / operationalizing",
        evidence: "Microsoft identified Fairwater in Mount Pleasant as a Wisconsin AI datacenter built as one massive supercomputer with hundreds of thousands of latest NVIDIA GPUs.",
        source_url: "https://blogs.microsoft.com/blog/2025/09/18/inside-the-worlds-most-powerful-ai-datacenter/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "Meta Richland",
        operator: "Meta",
        approximate_location: "Richland Parish, LA",
        lat: 32.4500,
        lon: -91.7400,
        status: "under construction",
        evidence: "Louisiana Economic Development announced Meta's $10 billion AI-optimized Richland Parish data center on a 2,250 acre site.",
        source_url: "https://www.opportunitylouisiana.gov/news/meta-selects-northeast-louisiana-as-site-of-10-billion-artificial-intelligence-optimized-data-center-governor-jeff-landry-calls-investment-a-new-chapter-for-state",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "CoreWeave Ellendale",
        operator: "CoreWeave / Applied Digital",
        approximate_location: "Ellendale, ND",
        lat: 46.0027,
        lon: -98.5270,
        status: "leased / phased buildout",
        evidence: "Applied Digital's Ellendale campus is designed for large-scale AI/HPC workloads, with CoreWeave leasing 250 MW of capacity.",
        source_url: "https://ir.applieddigital.com/news-events/press-releases/detail/123/applied-digital-announces-250mw-ai-data-center-lease-with",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::AiGpu,
        label: "CoreWeave Plano",
        operator: "CoreWeave",
        approximate_location: "Plano, TX",
        lat: 33.0198,
        lon: -96.6989,
        status: "operational",
        evidence: "CoreWeave announced a $1.6 billion Plano data center for large-scale GPU-accelerated workloads and AI/ML demand.",
        source_url: "https://www.prnewswire.com/news-releases/coreweave-opens-new-texas-data-center-to-expand-access-to-high-performance-gpus-301884897.html",
        label_placement: ProjectedLabelPlacement::AboveLeft,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Meta Prineville",
        operator: "Meta",
        approximate_location: "Prineville, OR",
        lat: 44.2998,
        lon: -120.8345,
        status: "operational",
        evidence: "Meta's Prineville info sheet describes the Prineville Data Center as part of Meta's global infrastructure, with more than $2B invested in Oregon.",
        source_url: "https://datacenters.atmeta.com/asset/prineville-data-center-info-sheet/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google The Dalles",
        operator: "Google",
        approximate_location: "The Dalles, OR",
        lat: 45.5946,
        lon: -121.1787,
        status: "official Google data center location",
        evidence: "Google lists The Dalles, Oregon as the first site where it began building and operating its own data centers.",
        source_url: "https://datacenters.google/locations/oregon/",
        label_placement: ProjectedLabelPlacement::AboveLeft,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google IA/NE",
        operator: "Google",
        approximate_location: "Council Bluffs, IA / Omaha and Papillion, NE",
        lat: 41.2200,
        lon: -95.9250,
        status: "official Google data center locations",
        evidence: "Google's official data center locations list includes Council Bluffs, Iowa plus Omaha and Papillion, Nebraska; grouped here as one metro-scale map marker.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Douglas Co.",
        operator: "Google",
        approximate_location: "Douglas County, GA",
        lat: 33.7515,
        lon: -84.7477,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Douglas County, Georgia.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Henderson",
        operator: "Google",
        approximate_location: "Henderson, NV",
        lat: 36.0395,
        lon: -114.9817,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Henderson, Nevada.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Jackson Co.",
        operator: "Google",
        approximate_location: "Jackson County, AL",
        lat: 34.6723,
        lon: -85.7147,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Jackson County, Alabama.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Lenoir",
        operator: "Google",
        approximate_location: "Lenoir, NC",
        lat: 35.9140,
        lon: -81.5390,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Lenoir, North Carolina.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Lowcountry",
        operator: "Google",
        approximate_location: "Lowcountry, SC",
        lat: 33.1586,
        lon: -80.0132,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes the Lowcountry, South Carolina.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Ohio",
        operator: "Google",
        approximate_location: "New Albany / Lancaster / Columbus, OH",
        lat: 40.0812,
        lon: -82.8088,
        status: "official Google data center communities",
        evidence: "Google's Ohio page identifies New Albany, Lancaster, and Columbus as data center communities.",
        source_url: "https://datacenters.google/locations/ohio/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Indiana",
        operator: "Google",
        approximate_location: "Fort Wayne / Morgan County, IN",
        lat: 41.0793,
        lon: -85.1394,
        status: "in development",
        evidence: "Google's Indiana page says its data centers are in development, with Fort Wayne groundbreaking in 2024 and a Morgan County location announced in 2025.",
        source_url: "https://datacenters.google/locations/indiana/",
        label_placement: ProjectedLabelPlacement::AboveLeft,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Mayes Co.",
        operator: "Google",
        approximate_location: "Mayes County, OK",
        lat: 36.3070,
        lon: -95.3169,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Mayes County, Oklahoma.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Ellis Co.",
        operator: "Google",
        approximate_location: "Midlothian / Red Oak, TX",
        lat: 32.4965,
        lon: -96.9074,
        status: "official Google data center locations",
        evidence: "Google's Texas page says the state is home to two Google data center locations in Midlothian and Red Oak in Ellis County; grouped here as one county-scale map marker.",
        source_url: "https://datacenters.google/locations/texas/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Montgomery Co.",
        operator: "Google",
        approximate_location: "Montgomery County, TN",
        lat: 36.5298,
        lon: -87.3595,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Montgomery County, Tennessee.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Northern VA",
        operator: "Google",
        approximate_location: "Northern Virginia",
        lat: 38.8047,
        lon: -77.8000,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Northern Virginia.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::BelowRight,
    },
    GpuDataCenterSite {
        category: ComputeCampusCategory::Hyperscale,
        label: "Google Storey Co.",
        operator: "Google",
        approximate_location: "Storey County, NV",
        lat: 39.5296,
        lon: -119.4457,
        status: "official Google data center location",
        evidence: "Google's official data center locations list includes Storey County, Nevada.",
        source_url: "https://datacenters.google/locations/",
        label_placement: ProjectedLabelPlacement::AboveRight,
    },
];

#[derive(Debug, Serialize)]
struct CoverageSummary {
    schema: &'static str,
    region: String,
    bounds: Bounds,
    radar_range_km: f64,
    radar_sites_used: usize,
    grid_points: usize,
    covered_grid_points: usize,
    uncovered_grid_points: usize,
    covered_fraction: f64,
    max_gap_km: f64,
    mean_gap_km: Option<f64>,
    hotspots: Vec<HoleHotspot>,
    compute_campuses: Vec<ComputeCampusSummary>,
    limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct ComputeCampusSummary {
    category: ComputeCampusCategory,
    label: &'static str,
    operator: &'static str,
    approximate_location: &'static str,
    lat: f64,
    lon: f64,
    status: &'static str,
    evidence: &'static str,
    source_url: &'static str,
}

#[derive(Debug, Serialize)]
struct HoleHotspot {
    lat: f64,
    lon: f64,
    coverage_gap_km: f64,
    nearest_distance_km: f64,
    nearest_site_id: &'static str,
    nearest_site_name: &'static str,
    nearest_site_state: &'static str,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    validate_cli(&cli)?;

    let mut spec = cli.region.spec();
    if let Some(bounds) = custom_bounds(&cli)? {
        spec = RegionSpec {
            slug: "custom",
            label: "Custom bounds",
            bounds,
            projection: lambert_for_bounds(bounds),
            land_clip: None,
        };
    }

    let output_path = cli.out.clone().unwrap_or_else(|| {
        PathBuf::from("target").join("radar_coverage").join(format!(
            "{}_{}km.png",
            spec.slug,
            rounded_range_slug(cli.range_km)
        ))
    });
    let summary_path = cli
        .summary_json
        .clone()
        .unwrap_or_else(|| output_path.with_extension("json"));

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    if let Some(parent) = summary_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create summary directory {}", parent.display()))?;
    }

    let candidate_sites = candidate_sites(spec.bounds, cli.range_km);
    if candidate_sites.is_empty() {
        bail!("no radar sites found in or near requested bounds");
    }
    let gpu_sites = if cli.gpu_datacenters || cli.label_gpu_datacenters {
        gpu_datacenter_sites_in_bounds(spec.bounds)
    } else {
        Vec::new()
    };

    let land_mask = if cli.include_water {
        None
    } else {
        Some(LandMask::load(spec))
    };
    let (field, samples) = build_coverage_field(
        spec.bounds,
        cli.nx,
        cli.ny,
        cli.range_km,
        &candidate_sites,
        land_mask.as_ref(),
    )?;
    let projected = build_projected_map(&field, spec, cli.width, cli.height)?;
    let mut request = MapRenderRequest::new(field, coverage_margin_scale());
    request.width = cli.width;
    request.height = cli.height;
    request.title = Some(format!("NEXRAD Coverage Margin - {}", spec.label));
    let mut subtitle_left = format!(
        "Range-only radius: {:.0} km | Sites: {} | {}",
        cli.range_km,
        candidate_sites.len(),
        if cli.include_water {
            "full frame"
        } else {
            "land mask"
        }
    );
    if !gpu_sites.is_empty() {
        let ai_gpu_count = gpu_sites
            .iter()
            .filter(|site| matches!(site.category, ComputeCampusCategory::AiGpu))
            .count();
        let hyperscale_count = gpu_sites.len() - ai_gpu_count;
        subtitle_left.push_str(&format!(
            " | Compute dots: {} AI/GPU + {} hyperscale",
            ai_gpu_count, hyperscale_count
        ));
    }
    request.subtitle_left = Some(subtitle_left);
    request.subtitle_right = Some(if gpu_sites.is_empty() {
        "Positive values mark first-order radar holes".to_string()
    } else {
        "Magenta=AI/GPU, blue=hyperscale | dots use approximate public locations".to_string()
    });
    request.cbar_tick_step = Some(50.0);
    request.domain_frame = Some(DomainFrame::model_data_default());
    request.raster_sample_mode = RasterSampleMode::Linear;
    request.apply_projected_map(&projected);
    add_site_overlays(
        &mut request,
        spec.projection,
        &candidate_sites,
        cli.range_km,
        !cli.no_rings,
        cli.label_sites,
    );
    add_gpu_datacenter_overlays(
        &mut request,
        spec.projection,
        &gpu_sites,
        cli.label_gpu_datacenters,
    );

    save_png(&request, &output_path)
        .with_context(|| format!("write coverage map {}", output_path.display()))?;

    let summary = coverage_summary(
        spec,
        cli.range_km,
        candidate_sites.len(),
        &samples,
        cli.hotspots,
        cli.include_water,
        &gpu_sites,
    );
    std::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("write coverage summary {}", summary_path.display()))?;

    println!("{}\n{}", output_path.display(), summary_path.display());

    Ok(())
}

fn validate_cli(cli: &Cli) -> anyhow::Result<()> {
    if !cli.range_km.is_finite() || cli.range_km <= 0.0 {
        bail!("--range-km must be a positive finite value");
    }
    if cli.nx < 2 || cli.ny < 2 {
        bail!("--nx and --ny must both be at least 2");
    }
    if cli.width < 320 || cli.height < 240 {
        bail!("--width must be at least 320 and --height at least 240");
    }
    Ok(())
}

fn custom_bounds(cli: &Cli) -> anyhow::Result<Option<Bounds>> {
    match (cli.west, cli.east, cli.south, cli.north) {
        (None, None, None, None) => Ok(None),
        (Some(west), Some(east), Some(south), Some(north)) => {
            if ![west, east, south, north]
                .iter()
                .all(|value| value.is_finite())
            {
                bail!("custom bounds must be finite");
            }
            if west == east || south == north {
                bail!("custom bounds must span nonzero latitude and longitude");
            }
            Ok(Some(Bounds {
                west,
                east,
                south: south.min(north),
                north: south.max(north),
            }))
        }
        _ => bail!("provide all four custom bounds: --west --east --south --north"),
    }
}

impl RegionArg {
    fn spec(self) -> RegionSpec {
        match self {
            Self::Conus => RegionSpec {
                slug: "conus",
                label: "Lower 48",
                bounds: Bounds {
                    west: -125.0,
                    east: -66.0,
                    south: 24.0,
                    north: 50.0,
                },
                projection: LambertConformal::new(33.0, 45.0, -98.0, 38.5),
                land_clip: Some(CONUS_APPROX_CLIP),
            },
            Self::Alaska => RegionSpec {
                slug: "alaska",
                label: "Alaska",
                bounds: Bounds {
                    west: -171.0,
                    east: -129.0,
                    south: 51.0,
                    north: 72.0,
                },
                projection: LambertConformal::new(55.0, 65.0, -150.0, 62.0),
                land_clip: None,
            },
            Self::Hawaii => RegionSpec {
                slug: "hawaii",
                label: "Hawaii",
                bounds: Bounds {
                    west: -161.5,
                    east: -154.0,
                    south: 18.5,
                    north: 23.0,
                },
                projection: LambertConformal::new(18.0, 23.0, -157.5, 20.5),
                land_clip: None,
            },
            Self::PuertoRico => RegionSpec {
                slug: "puerto_rico",
                label: "Puerto Rico and Virgin Islands",
                bounds: Bounds {
                    west: -68.5,
                    east: -64.0,
                    south: 17.4,
                    north: 18.9,
                },
                projection: LambertConformal::new(17.5, 19.0, -66.2, 18.2),
                land_clip: None,
            },
            Self::Guam => RegionSpec {
                slug: "guam",
                label: "Guam",
                bounds: Bounds {
                    west: 143.5,
                    east: 146.0,
                    south: 12.5,
                    north: 14.5,
                },
                projection: LambertConformal::new(12.5, 14.5, 144.8, 13.5),
                land_clip: None,
            },
        }
    }
}

fn lambert_for_bounds(bounds: Bounds) -> LambertConformal {
    let lat_span = (bounds.north - bounds.south).abs();
    let lon_center = midpoint_longitude(bounds.west, bounds.east);
    LambertConformal::new(
        bounds.south + lat_span * 0.25,
        bounds.south + lat_span * 0.75,
        lon_center,
        (bounds.south + bounds.north) / 2.0,
    )
}

fn midpoint_longitude(west: f64, east: f64) -> f64 {
    if west <= east {
        return (west + east) / 2.0;
    }
    let midpoint = (west + east + 360.0) / 2.0;
    normalize_lon(midpoint)
}

fn candidate_sites(bounds: Bounds, range_km: f64) -> Vec<&'static RadarSite> {
    let lat_pad = (range_km / 111.0).max(1.0);
    let lon_pad = (range_km / longitude_km_per_degree(bounds.south, bounds.north)).max(1.0);
    RADAR_SITES
        .iter()
        .filter(|site| {
            site.lat >= bounds.south - lat_pad
                && site.lat <= bounds.north + lat_pad
                && lon_in_bounds(site.lon, bounds.west - lon_pad, bounds.east + lon_pad)
        })
        .collect()
}

fn gpu_datacenter_sites_in_bounds(bounds: Bounds) -> Vec<&'static GpuDataCenterSite> {
    GPU_DATACENTER_SITES
        .iter()
        .filter(|site| {
            site.lat >= bounds.south
                && site.lat <= bounds.north
                && lon_in_bounds(site.lon, bounds.west, bounds.east)
        })
        .collect()
}

fn longitude_km_per_degree(south: f64, north: f64) -> f64 {
    let max_abs_lat = south.abs().max(north.abs()).min(89.0).to_radians();
    (111.0 * max_abs_lat.cos()).max(20.0)
}

impl LandMask {
    fn load(region: RegionSpec) -> Self {
        let polygons = load_styled_basemap_polygons_for(BasemapStyle::Filled)
            .into_iter()
            .filter(|layer| layer.role == PolygonRole::Land)
            .flat_map(|layer| layer.polygons.into_iter())
            .filter_map(LandMaskPolygon::new)
            .filter(|polygon| bounds_intersect(polygon.bbox, region.bounds))
            .collect();
        Self {
            polygons,
            clip: region.land_clip,
        }
    }

    fn contains(&self, lat: f64, lon: f64) -> bool {
        if self
            .clip
            .map(|ring| !point_in_ring(lon, lat, ring))
            .unwrap_or(false)
        {
            return false;
        }
        self.polygons
            .iter()
            .any(|polygon| polygon.contains(lat, lon))
    }
}

impl LandMaskPolygon {
    fn new(rings: Vec<Vec<(f64, f64)>>) -> Option<Self> {
        if rings.first().map(|ring| ring.len()).unwrap_or(0) < 3 {
            return None;
        }
        let bbox = polygon_bounds(&rings)?;
        Some(Self { rings, bbox })
    }

    fn contains(&self, lat: f64, lon: f64) -> bool {
        if !bounds_contains(self.bbox, lat, lon) {
            return false;
        }
        self.rings
            .iter()
            .filter(|ring| point_in_ring(lon, lat, ring))
            .count()
            % 2
            == 1
    }
}

fn polygon_bounds(rings: &[Vec<(f64, f64)>]) -> Option<Bounds> {
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for ring in rings {
        for &(lon, lat) in ring {
            if lon.is_finite() && lat.is_finite() {
                west = west.min(lon);
                east = east.max(lon);
                south = south.min(lat);
                north = north.max(lat);
            }
        }
    }
    west.is_finite().then_some(Bounds {
        west,
        east,
        south,
        north,
    })
}

fn bounds_intersect(a: Bounds, b: Bounds) -> bool {
    !(a.east < b.west || a.west > b.east || a.north < b.south || a.south > b.north)
}

fn bounds_contains(bounds: Bounds, lat: f64, lon: f64) -> bool {
    lat >= bounds.south && lat <= bounds.north && lon >= bounds.west && lon <= bounds.east
}

fn point_in_ring(x: f64, y: f64, ring: &[(f64, f64)]) -> bool {
    if ring.len() < 3 {
        return false;
    }
    let mut inside = false;
    let mut previous = ring[ring.len() - 1];
    for &current in ring {
        let (xi, yi) = current;
        let (xj, yj) = previous;
        if ((yi > y) != (yj > y)) && (x < (xj - xi) * (y - yi) / ((yj - yi) + f64::EPSILON) + xi) {
            inside = !inside;
        }
        previous = current;
    }
    inside
}

fn build_coverage_field(
    bounds: Bounds,
    nx: usize,
    ny: usize,
    range_km: f64,
    sites: &[&'static RadarSite],
    land_mask: Option<&LandMask>,
) -> anyhow::Result<(Field2D, Vec<GridSample>)> {
    let shape = GridShape::new(nx, ny).context("valid coverage grid shape")?;
    let len = shape.len();
    let mut lat = Vec::with_capacity(len);
    let mut lon = Vec::with_capacity(len);
    let mut values = Vec::with_capacity(len);
    let mut samples = Vec::with_capacity(len);

    for j in 0..ny {
        let fy = j as f64 / (ny - 1) as f64;
        let sample_lat = bounds.south + fy * (bounds.north - bounds.south);
        for i in 0..nx {
            let fx = i as f64 / (nx - 1) as f64;
            let sample_lon = interpolate_longitude(bounds.west, bounds.east, fx);
            if land_mask
                .map(|mask| !mask.contains(sample_lat, sample_lon))
                .unwrap_or(false)
            {
                lat.push(sample_lat as f32);
                lon.push(sample_lon as f32);
                values.push(f32::NAN);
                continue;
            }
            let nearest = nearest_site(sample_lat, sample_lon, sites)
                .context("site list must not be empty")?;
            let gap = nearest.distance_km - range_km;
            lat.push(sample_lat as f32);
            lon.push(sample_lon as f32);
            values.push(gap as f32);
            samples.push(GridSample {
                lat: sample_lat,
                lon: sample_lon,
                nearest_distance_km: nearest.distance_km,
                coverage_gap_km: gap,
                nearest_site: nearest.site,
            });
        }
    }
    if samples.is_empty() {
        bail!("land mask excluded every grid point in requested bounds");
    }

    let grid = LatLonGrid::new(shape, lat, lon).context("coverage lat/lon grid")?;
    let field = Field2D::new(
        ProductKey::named("NEXRAD_COVERAGE_MARGIN"),
        "km",
        grid,
        values,
    )
    .context("coverage field")?;
    Ok((field, samples))
}

#[derive(Debug, Clone, Copy)]
struct NearestSite {
    site: &'static RadarSite,
    distance_km: f64,
}

fn nearest_site(lat: f64, lon: f64, sites: &[&'static RadarSite]) -> Option<NearestSite> {
    sites
        .iter()
        .map(|&site| NearestSite {
            site,
            distance_km: haversine_km(lat, lon, site.lat, site.lon),
        })
        .min_by(|a, b| a.distance_km.total_cmp(&b.distance_km))
}

fn build_projected_map(
    field: &Field2D,
    spec: RegionSpec,
    width: u32,
    height: u32,
) -> anyhow::Result<rustwx_render::ProjectedMap> {
    let mut options = ProjectedMapBuildOptions::from_bounds(
        (
            spec.bounds.west,
            spec.bounds.east,
            spec.bounds.south,
            spec.bounds.north,
        ),
        width as f64 / height as f64,
    )
    .with_projection(spec.projection.spec())
    .with_basemap_style(BasemapStyle::Filled)
    .with_basemap_detail(BasemapDetail::Regional);
    options.domain.reference_latitude_deg = Some(spec.projection.reference_latitude_deg());
    build_projected_map_with_options(&field.grid.lat_deg, &field.grid.lon_deg, &options)
        .map_err(|err| anyhow::anyhow!("build projected coverage basemap: {err}"))
}

fn add_site_overlays(
    request: &mut MapRenderRequest,
    projection: LambertConformal,
    sites: &[&'static RadarSite],
    range_km: f64,
    draw_rings: bool,
    label_sites: bool,
) {
    let ring_color = Color::rgba(28, 38, 48, 58);
    let site_color = Color::rgba(18, 24, 32, 230);

    if draw_rings {
        request.projected_lines.extend(
            sites
                .iter()
                .map(|site| coverage_ring(site, projection, range_km, ring_color)),
        );
    }

    for site in sites {
        let (x, y) = projection.project(site.lat, site.lon);
        request.projected_points.push(ProjectedPointOverlay {
            x,
            y,
            color: site_color,
            radius_px: 3,
            width_px: 2,
            shape: ProjectedMarkerShape::Plus,
        });
        if label_sites {
            request.projected_place_labels.push(
                ProjectedPlaceLabel::new(x, y)
                    .with_label(site.id)
                    .with_priority(ProjectedPlaceLabelPriority::Micro)
                    .with_style(site_label_style()),
            );
        }
    }
}

fn add_gpu_datacenter_overlays(
    request: &mut MapRenderRequest,
    projection: LambertConformal,
    sites: &[&GpuDataCenterSite],
    label_sites: bool,
) {
    for site in sites {
        let (x, y) = projection.project(site.lat, site.lon);
        let mut label = ProjectedPlaceLabel::new(x, y)
            .with_priority(ProjectedPlaceLabelPriority::Primary)
            .with_style(gpu_datacenter_label_style(
                site.category,
                site.label_placement,
            ));
        if label_sites {
            label = label.with_label(site.label);
        }
        request.projected_place_labels.push(label);
    }
}

fn coverage_ring(
    site: &RadarSite,
    projection: LambertConformal,
    range_km: f64,
    color: Color,
) -> ProjectedLineOverlay {
    let mut points = Vec::with_capacity(121);
    for azimuth in (0..=360).step_by(3) {
        let (lat, lon) =
            radar_polar_to_lat_lon(site.lat, site.lon, azimuth as f32, range_km * 1000.0);
        points.push(projection.project(lat, lon));
    }
    ProjectedLineOverlay {
        points,
        color,
        width: 1,
        role: rustwx_render::LineworkRole::Generic,
    }
}

fn site_label_style() -> ProjectedPlaceLabelStyle {
    ProjectedPlaceLabelStyle {
        marker_radius_px: 0,
        marker_fill: Color::rgba(0, 0, 0, 0),
        marker_outline: Color::rgba(0, 0, 0, 0),
        marker_outline_width: 0,
        label_color: Color::rgba(18, 24, 32, 220),
        label_halo: Color::rgba(255, 255, 255, 220),
        label_halo_width_px: 2,
        label_scale: 1,
        label_offset_x_px: 5,
        label_offset_y_px: -3,
        label_placement: ProjectedLabelPlacement::AboveRight,
        label_bold: true,
    }
}

fn gpu_datacenter_label_style(
    category: ComputeCampusCategory,
    label_placement: ProjectedLabelPlacement,
) -> ProjectedPlaceLabelStyle {
    let (marker_radius_px, marker_fill, label_color) = match category {
        ComputeCampusCategory::AiGpu => (
            7,
            Color::rgba(214, 24, 146, 245),
            Color::rgba(98, 20, 92, 255),
        ),
        ComputeCampusCategory::Hyperscale => (
            5,
            Color::rgba(18, 111, 191, 235),
            Color::rgba(14, 64, 112, 255),
        ),
    };
    ProjectedPlaceLabelStyle {
        marker_radius_px,
        marker_fill,
        marker_outline: Color::rgba(255, 255, 255, 245),
        marker_outline_width: 2,
        label_color,
        label_halo: Color::rgba(255, 255, 255, 245),
        label_halo_width_px: 3,
        label_scale: 1,
        label_offset_x_px: 9,
        label_offset_y_px: -4,
        label_placement,
        label_bold: true,
    }
}

fn coverage_margin_scale() -> ColorScale {
    ColorScale::Discrete(DiscreteColorScale {
        levels: vec![
            -200.0, -150.0, -100.0, -50.0, -25.0, 0.0, 25.0, 50.0, 75.0, 100.0, 150.0, 200.0, 300.0,
        ],
        colors: vec![
            Color::rgba(24, 98, 70, 132),
            Color::rgba(45, 132, 92, 132),
            Color::rgba(83, 165, 111, 128),
            Color::rgba(134, 190, 138, 122),
            Color::rgba(204, 221, 176, 118),
            Color::rgba(245, 214, 112, 172),
            Color::rgba(237, 163, 75, 186),
            Color::rgba(220, 105, 58, 202),
            Color::rgba(190, 62, 51, 214),
            Color::rgba(142, 41, 58, 224),
            Color::rgba(92, 28, 55, 232),
            Color::rgba(56, 21, 48, 238),
        ],
        extend: ExtendMode::Both,
        mask_below: None,
    })
}

fn coverage_summary(
    spec: RegionSpec,
    range_km: f64,
    radar_sites_used: usize,
    samples: &[GridSample],
    hotspot_count: usize,
    include_water: bool,
    gpu_sites: &[&GpuDataCenterSite],
) -> CoverageSummary {
    let covered_grid_points = samples
        .iter()
        .filter(|sample| sample.coverage_gap_km <= 0.0)
        .count();
    let uncovered_grid_points = samples.len() - covered_grid_points;
    let gap_sum: f64 = samples
        .iter()
        .filter(|sample| sample.coverage_gap_km > 0.0)
        .map(|sample| sample.coverage_gap_km)
        .sum();
    let max_gap_km = samples
        .iter()
        .map(|sample| sample.coverage_gap_km)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(0.0);
    CoverageSummary {
        schema: "rustwx.radar.coverage_summary.v2",
        region: spec.label.to_string(),
        bounds: spec.bounds,
        radar_range_km: rounded(range_km),
        radar_sites_used,
        grid_points: samples.len(),
        covered_grid_points,
        uncovered_grid_points,
        covered_fraction: rounded_fraction(covered_grid_points as f64 / samples.len() as f64),
        max_gap_km: rounded(max_gap_km),
        mean_gap_km: (uncovered_grid_points > 0)
            .then(|| rounded(gap_sum / uncovered_grid_points as f64)),
        hotspots: select_hotspots(samples, hotspot_count),
        compute_campuses: gpu_sites
            .iter()
            .map(|site| ComputeCampusSummary {
                category: site.category,
                label: site.label,
                operator: site.operator,
                approximate_location: site.approximate_location,
                lat: rounded_coord(site.lat),
                lon: rounded_coord(site.lon),
                status: site.status,
                evidence: site.evidence,
                source_url: site.source_url,
            })
            .collect(),
        limitations: coverage_limitations(include_water),
    }
}

fn coverage_limitations(include_water: bool) -> Vec<&'static str> {
    if include_water {
        vec![
            "Range-only first-order network view; beam height, terrain blockage, outages, and scan strategy are not modeled.",
            "Coverage fraction is based on the full rendered latitude/longitude frame, including water.",
            "Compute campus dots use approximate public city/county coordinates, not facility addresses.",
        ]
    } else {
        vec![
            "Range-only first-order network view; beam height, terrain blockage, outages, and scan strategy are not modeled.",
            "Coverage fraction uses a Natural Earth land mask plus a rough Lower-48 clip, not a population, terrain, or exact jurisdiction mask.",
            "Compute campus dots use approximate public city/county coordinates, not facility addresses.",
        ]
    }
}

fn select_hotspots(samples: &[GridSample], count: usize) -> Vec<HoleHotspot> {
    let mut candidates: Vec<&GridSample> = samples
        .iter()
        .filter(|sample| sample.coverage_gap_km > 0.0)
        .collect();
    candidates.sort_by(|a, b| b.coverage_gap_km.total_cmp(&a.coverage_gap_km));

    let mut selected: Vec<&GridSample> = Vec::new();
    for candidate in candidates {
        if selected.iter().all(|existing| {
            haversine_km(candidate.lat, candidate.lon, existing.lat, existing.lon) >= 125.0
        }) {
            selected.push(candidate);
        }
        if selected.len() >= count {
            break;
        }
    }

    selected
        .into_iter()
        .map(|sample| HoleHotspot {
            lat: rounded_coord(sample.lat),
            lon: rounded_coord(sample.lon),
            coverage_gap_km: rounded(sample.coverage_gap_km),
            nearest_distance_km: rounded(sample.nearest_distance_km),
            nearest_site_id: sample.nearest_site.id,
            nearest_site_name: sample.nearest_site.name,
            nearest_site_state: sample.nearest_site.state,
        })
        .collect()
}

fn interpolate_longitude(west: f64, east: f64, fraction: f64) -> f64 {
    if west <= east {
        west + (east - west) * fraction
    } else {
        normalize_lon(west + (east + 360.0 - west) * fraction)
    }
}

fn lon_in_bounds(lon: f64, west: f64, east: f64) -> bool {
    let lon = normalize_lon(lon);
    let west = normalize_lon(west);
    let east = normalize_lon(east);
    if west <= east {
        lon >= west && lon <= east
    } else {
        lon >= west || lon <= east
    }
}

fn normalize_lon(lon: f64) -> f64 {
    let mut out = lon % 360.0;
    if out > 180.0 {
        out -= 360.0;
    } else if out <= -180.0 {
        out += 360.0;
    }
    out
}

fn rounded(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn rounded_coord(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

fn rounded_fraction(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

fn rounded_range_slug(value: f64) -> String {
    if (value - value.round()).abs() < 0.05 {
        format!("{:.0}", value)
    } else {
        format!("{:.1}", value).replace('.', "p")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_site_finds_kcri_for_oklahoma_city() {
        let sites = candidate_sites(RegionArg::Conus.spec().bounds, 230.0);
        let nearest = nearest_site(35.4676, -97.5164, &sites).expect("nearest site");
        assert_eq!(nearest.site.id, "KCRI");
        assert!(nearest.distance_km < 35.0);
    }

    #[test]
    fn coverage_gap_is_negative_at_radar_site() {
        let sites = candidate_sites(RegionArg::Conus.spec().bounds, 230.0);
        let nearest = nearest_site(35.3331, -97.2775, &sites).expect("nearest site");
        assert_eq!(nearest.site.id, "KTLX");
        assert!(nearest.distance_km - 230.0 < 0.0);
    }

    #[test]
    fn custom_longitude_interpolation_wraps_antimeridian() {
        let lon = interpolate_longitude(170.0, -170.0, 0.75);
        assert!(lon < -170.0 || lon > 170.0);
    }
}
