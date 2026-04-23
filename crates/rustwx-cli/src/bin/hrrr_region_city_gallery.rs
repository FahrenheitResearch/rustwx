use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "../metro.rs"]
mod metro;
#[allow(dead_code)]
#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::derived::HrrrDerivedBatchReport;
use rustwx_products::direct::HrrrDirectBatchReport;
use rustwx_products::gallery::{
    GalleryDirectBatchReport, GalleryDirectRenderedRecipe, GalleryDirectTiming, ProofGalleryIndex,
    ProofManifest, ProofManifestRecord, ProofRunKind, build_proof_gallery_index,
    load_gallery_catalog, render_gallery_html,
};
use rustwx_products::non_ecape::{
    HrrrNonEcapeDomainReport, HrrrNonEcapeFanoutTiming, HrrrNonEcapeMultiDomainReport,
    HrrrNonEcapeMultiDomainRequest, HrrrNonEcapeSharedTiming, run_hrrr_non_ecape_hour_multi_domain,
};
use rustwx_products::places::{PlaceLabelDensityTier, PlaceLabelOverlay};
use rustwx_products::publication::atomic_write_json;
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
use rustwx_products::windowed::HrrrWindowedBatchReport;
use rustwx_render::PngCompressionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RunModeArg {
    Run,
    Summarize,
}

impl RunModeArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Summarize => "summarize",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ValidationScopeArg {
    All,
    Regions,
    Cities,
}

impl ValidationScopeArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Regions => "regions",
            Self::Cities => "cities",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceModeArg {
    Canonical,
    Fastest,
}

impl From<SourceModeArg> for ProductSourceMode {
    fn from(value: SourceModeArg) -> Self {
        match value {
            SourceModeArg::Canonical => Self::Canonical,
            SourceModeArg::Fastest => Self::Fastest,
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DomainKind {
    Region,
    City,
}

impl DomainKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Region => "region",
            Self::City => "city",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DomainClass {
    Region,
    City,
    Unknown,
}

impl DomainClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Region => "region",
            Self::City => "city",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GallerySection {
    All,
    Regions,
    Cities,
}

impl GallerySection {
    fn slug(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Regions => "regions",
            Self::Cities => "cities",
        }
    }

    fn title_suffix(self) -> &'static str {
        match self {
            Self::All => "All Domains",
            Self::Regions => "Regions",
            Self::Cities => "Cities",
        }
    }

    fn includes(self, class: DomainClass) -> bool {
        match self {
            Self::All => true,
            Self::Regions => class == DomainClass::Region,
            Self::Cities => class == DomainClass::City,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-region-city-gallery",
    about = "Generate or summarize full HRRR region+city crop validation galleries"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = RunModeArg::Run)]
    mode: RunModeArg,
    #[arg(long, value_enum, default_value_t = ValidationScopeArg::All)]
    scope: ValidationScopeArg,
    #[arg(long)]
    report: Option<PathBuf>,
    #[arg(long, default_value = "20260422")]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(long, default_value_t = 0)]
    forecast_hour: u16,
    #[arg(long, default_value = "nomads")]
    source: rustwx_core::SourceId,
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long = "source-mode", value_enum, default_value_t = SourceModeArg::Fastest)]
    source_mode: SourceModeArg,
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(long, default_value_t = 1200)]
    width: u32,
    #[arg(long, default_value_t = 900)]
    height: u32,
    #[arg(long, default_value_t = 8)]
    domain_jobs: usize,
    #[arg(
        long = "place-label-density",
        value_enum,
        default_value_t = PlaceLabelDensityArg::Major,
        help = "Place-label density: none, major, major-and-aux, or dense. Numeric aliases 0-3 also work."
    )]
    place_label_density: PlaceLabelDensityArg,
    #[arg(long)]
    render_threads: Option<usize>,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
    #[arg(long)]
    catalog: Option<PathBuf>,
    #[arg(long, default_value = "RustWX Region + City Validation")]
    title: String,
}

#[derive(Debug, Clone)]
struct DomainCatalogEntry {
    kind: DomainKind,
    slug: String,
    label: String,
    domain: DomainSpec,
    order: usize,
}

#[derive(Debug, Clone)]
struct DomainCatalog {
    entries: Vec<DomainCatalogEntry>,
    lookup: HashMap<String, DomainCatalogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainArtifactSummary {
    lane: String,
    slug: String,
    title: String,
    output_path: PathBuf,
    exists: bool,
    timing_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainValidationSummary {
    domain_type: String,
    slug: String,
    label: String,
    expected: bool,
    publication_manifest_path: PathBuf,
    attempt_manifest_path: Option<PathBuf>,
    output_count: usize,
    direct_rendered_count: usize,
    derived_rendered_count: usize,
    windowed_rendered_count: usize,
    windowed_blocker_count: usize,
    missing_image_count: usize,
    outputs: Vec<DomainArtifactSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GalleryBuildSummary {
    slug: String,
    title: String,
    index_html_path: PathBuf,
    index_json_path: PathBuf,
    run_count: usize,
    image_count: usize,
    missing_image_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegionCityGallerySummary {
    runner: &'static str,
    mode: String,
    scope: String,
    title: String,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: rustwx_core::SourceId,
    out_dir: PathBuf,
    report_path: PathBuf,
    cache_root: PathBuf,
    use_cache: bool,
    source_mode: ProductSourceMode,
    requested_direct_recipe_slugs: Vec<String>,
    requested_derived_recipe_slugs: Vec<String>,
    expected_domain_count: usize,
    expected_region_count: usize,
    expected_city_count: usize,
    rendered_domain_count: usize,
    rendered_region_count: usize,
    rendered_city_count: usize,
    unexpected_domain_count: usize,
    total_output_count: usize,
    missing_image_count: usize,
    missing_expected_domain_slugs: Vec<String>,
    unexpected_domain_slugs: Vec<String>,
    shared_timing: HrrrNonEcapeSharedTiming,
    fanout_timing: HrrrNonEcapeFanoutTiming,
    total_ms: u128,
    galleries: Vec<GalleryBuildSummary>,
    domains: Vec<DomainValidationSummary>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run(&args)
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    validate_args(args)?;

    let domain_catalog = build_domain_catalog();
    let out_dir = resolve_out_dir(args);
    fs::create_dir_all(&out_dir)?;

    let (report_path, report) = match args.mode {
        RunModeArg::Run => run_validation(args, &out_dir, &domain_catalog)?,
        RunModeArg::Summarize => {
            let report_path = args
                .report
                .clone()
                .ok_or("`--report` is required in summarize mode")?;
            (report_path.clone(), load_multi_domain_report(&report_path)?)
        }
    };

    let galleries = build_galleries(args, &out_dir, &report, &domain_catalog)?;
    let summary = build_summary(
        args,
        &out_dir,
        &report_path,
        &report,
        &domain_catalog,
        galleries,
    );
    let summary_path = summary_path(&out_dir, &report, args.scope);
    atomic_write_json(&summary_path, &summary)?;

    println!(
        "summary domains={} outputs={} missing_images={}",
        summary.rendered_domain_count, summary.total_output_count, summary.missing_image_count
    );
    println!("{}", report_path.display());
    for gallery in &summary.galleries {
        println!("{}", gallery.index_html_path.display());
        println!("{}", gallery.index_json_path.display());
    }
    println!("{}", summary_path.display());
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.mode == RunModeArg::Summarize && args.report.is_none() {
        return Err("`--report` is required in summarize mode".into());
    }
    Ok(())
}

fn resolve_out_dir(args: &Args) -> PathBuf {
    if let Some(out_dir) = &args.out_dir {
        return out_dir.clone();
    }
    match (&args.mode, &args.report) {
        (RunModeArg::Summarize, Some(report)) => report
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(default_out_dir),
        _ => default_out_dir(),
    }
}

fn default_out_dir() -> PathBuf {
    PathBuf::from("proof").join("region_city_validation")
}

fn default_direct_recipes() -> Vec<String> {
    vec!["visibility", "500mb_temperature_height_winds"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn build_domain_catalog() -> DomainCatalog {
    let mut entries = Vec::<DomainCatalogEntry>::new();
    let mut seen = HashSet::<String>::new();

    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::Conus.slug(),
        "CONUS",
        region::RegionPreset::Conus.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::Midwest.slug(),
        "Midwest",
        region::RegionPreset::Midwest.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::California.slug(),
        "California",
        region::RegionPreset::California.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::CaliforniaSquare.slug(),
        "California Square",
        region::RegionPreset::CaliforniaSquare.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::RenoSquare.slug(),
        "Reno Square",
        region::RegionPreset::RenoSquare.bounds(),
    );

    for split in region::US_SPLIT_REGION_PRESETS {
        if matches!(
            split.slug,
            "pacific_northwest" | "california_southwest" | "rockies_high_plains"
        ) {
            push_region_entry(
                &mut entries,
                &mut seen,
                split.slug,
                split.label,
                split.bounds,
            );
        }
    }

    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::Southeast.slug(),
        "Southeast",
        region::RegionPreset::Southeast.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::SouthernPlains.slug(),
        "Southern Plains",
        region::RegionPreset::SouthernPlains.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::Northeast.slug(),
        "Northeast",
        region::RegionPreset::Northeast.bounds(),
    );
    push_region_entry(
        &mut entries,
        &mut seen,
        region::RegionPreset::GreatLakes.slug(),
        "Great Lakes",
        region::RegionPreset::GreatLakes.bounds(),
    );

    for preset in metro::MAJOR_US_CITY_PRESETS {
        if !seen.insert(preset.slug.to_string()) {
            continue;
        }
        entries.push(DomainCatalogEntry {
            kind: DomainKind::City,
            slug: preset.slug.to_string(),
            label: preset.label.to_string(),
            domain: preset.domain(),
            order: entries.len(),
        });
    }

    let lookup = entries
        .iter()
        .cloned()
        .map(|entry| (entry.slug.clone(), entry))
        .collect();
    DomainCatalog { entries, lookup }
}

fn push_region_entry(
    entries: &mut Vec<DomainCatalogEntry>,
    seen: &mut HashSet<String>,
    slug: &str,
    label: &str,
    bounds: (f64, f64, f64, f64),
) {
    if !seen.insert(slug.to_string()) {
        return;
    }
    entries.push(DomainCatalogEntry {
        kind: DomainKind::Region,
        slug: slug.to_string(),
        label: label.to_string(),
        domain: DomainSpec::new(slug, bounds),
        order: entries.len(),
    });
}

impl DomainCatalog {
    fn selected_entries(&self, scope: ValidationScopeArg) -> Vec<DomainCatalogEntry> {
        self.entries
            .iter()
            .filter(|entry| match scope {
                ValidationScopeArg::All => true,
                ValidationScopeArg::Regions => entry.kind == DomainKind::Region,
                ValidationScopeArg::Cities => entry.kind == DomainKind::City,
            })
            .cloned()
            .collect()
    }

    fn classify(&self, slug: &str) -> DomainClass {
        match self.lookup.get(slug).map(|entry| entry.kind) {
            Some(DomainKind::Region) => DomainClass::Region,
            Some(DomainKind::City) => DomainClass::City,
            None => DomainClass::Unknown,
        }
    }

    fn label(&self, slug: &str) -> String {
        self.lookup
            .get(slug)
            .map(|entry| entry.label.clone())
            .unwrap_or_else(|| slug.to_string())
    }

    fn order(&self, slug: &str) -> usize {
        self.lookup
            .get(slug)
            .map(|entry| entry.order)
            .unwrap_or(usize::MAX)
    }
}

fn run_validation(
    args: &Args,
    out_dir: &Path,
    domain_catalog: &DomainCatalog,
) -> Result<(PathBuf, HrrrNonEcapeMultiDomainReport), Box<dyn std::error::Error>> {
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let domains = domain_catalog
        .selected_entries(args.scope)
        .into_iter()
        .map(|entry| entry.domain)
        .collect::<Vec<_>>();
    if domains.is_empty() {
        return Err("validation scope resolved to zero domains".into());
    }

    let (direct_recipe_slugs, derived_recipe_slugs) = resolve_recipe_lists(args);
    let actual_render_threads =
        configure_render_threads(args.render_threads, args.domain_jobs, domains.len());
    let request = HrrrNonEcapeMultiDomainRequest {
        date_yyyymmdd: args.date.clone(),
        cycle_override_utc: args.cycle,
        forecast_hour: args.forecast_hour,
        source: args.source,
        domains,
        out_dir: out_dir.to_path_buf(),
        cache_root,
        use_cache: !args.no_cache,
        source_mode: args.source_mode.into(),
        direct_recipe_slugs,
        derived_recipe_slugs,
        windowed_products: Vec::new(),
        output_width: args.width,
        output_height: args.height,
        png_compression: args.png_compression.into(),
        custom_poi_overlay: None,
        place_label_overlay: Some(
            PlaceLabelOverlay::major_us_cities().with_density(args.place_label_density.into()),
        ),
        domain_jobs: Some(args.domain_jobs.max(1)),
    };

    let report = run_hrrr_non_ecape_hour_multi_domain(&request)?;
    let report_path = report_path(out_dir, &report, args.scope);
    atomic_write_json(&report_path, &report)?;

    if actual_render_threads.is_none() {
        unsafe {
            std::env::remove_var("RUSTWX_RENDER_THREADS");
        }
    }

    Ok((report_path, report))
}

fn resolve_recipe_lists(args: &Args) -> (Vec<String>, Vec<String>) {
    if args.direct_recipes.is_empty() && args.derived_recipes.is_empty() {
        (default_direct_recipes(), Vec::new())
    } else {
        (args.direct_recipes.clone(), args.derived_recipes.clone())
    }
}

fn configure_render_threads(
    requested: Option<usize>,
    domain_jobs: usize,
    domain_count: usize,
) -> Option<usize> {
    let render_threads = requested.or_else(|| {
        if domain_jobs > 1 && domain_count > 1 {
            Some(1)
        } else {
            None
        }
    });
    match render_threads {
        Some(value) if value > 0 => unsafe {
            std::env::set_var("RUSTWX_RENDER_THREADS", value.to_string());
        },
        _ => unsafe {
            std::env::remove_var("RUSTWX_RENDER_THREADS");
        },
    }
    render_threads.filter(|value| *value > 0)
}

fn load_multi_domain_report(
    report_path: &Path,
) -> Result<HrrrNonEcapeMultiDomainReport, Box<dyn std::error::Error>> {
    let bytes = fs::read(report_path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn build_galleries(
    args: &Args,
    out_dir: &Path,
    report: &HrrrNonEcapeMultiDomainReport,
    domain_catalog: &DomainCatalog,
) -> Result<Vec<GalleryBuildSummary>, Box<dyn std::error::Error>> {
    let gallery_root = out_dir.join("gallery");
    fs::create_dir_all(&gallery_root)?;

    let gallery_catalog = match &args.catalog {
        Some(path) => Some(load_gallery_catalog(path)?),
        None => None,
    };
    let proof_root = absolutize_path(&report.out_dir)?;
    let mut outputs = Vec::new();

    for section in gallery_sections(args.scope) {
        let records = collect_gallery_records(report, domain_catalog, section)?;
        let viewer_dir = gallery_root.join(section.slug());
        fs::create_dir_all(&viewer_dir)?;
        let title = gallery_title(&args.title, report, section);
        let mut index = build_proof_gallery_index(
            &title,
            &proof_root,
            &viewer_dir,
            gallery_catalog.as_ref(),
            &records,
        );
        decorate_and_sort_index(&mut index, domain_catalog);

        let index_json_path = viewer_dir.join("index.json");
        let index_html_path = viewer_dir.join("index.html");
        atomic_write_json(&index_json_path, &index)?;
        fs::write(&index_html_path, render_gallery_html(&index))?;

        outputs.push(GalleryBuildSummary {
            slug: section.slug().to_string(),
            title,
            index_html_path,
            index_json_path,
            run_count: index.summary.run_count,
            image_count: index.summary.image_count,
            missing_image_count: index.summary.missing_image_count,
        });
    }

    Ok(outputs)
}

fn gallery_sections(scope: ValidationScopeArg) -> Vec<GallerySection> {
    match scope {
        ValidationScopeArg::All => vec![
            GallerySection::All,
            GallerySection::Regions,
            GallerySection::Cities,
        ],
        ValidationScopeArg::Regions => vec![GallerySection::Regions],
        ValidationScopeArg::Cities => vec![GallerySection::Cities],
    }
}

fn gallery_title(
    base_title: &str,
    report: &HrrrNonEcapeMultiDomainReport,
    section: GallerySection,
) -> String {
    format!(
        "{base_title} | HRRR {} {:02}Z F{:03} | {}",
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        section.title_suffix()
    )
}

fn collect_gallery_records(
    report: &HrrrNonEcapeMultiDomainReport,
    domain_catalog: &DomainCatalog,
    section: GallerySection,
) -> Result<Vec<ProofManifestRecord>, Box<dyn std::error::Error>> {
    let mut records = Vec::new();
    for domain_report in &report.domains {
        let class = domain_catalog.classify(&domain_report.domain.slug);
        if !section.includes(class) {
            continue;
        }
        if let Some(direct) = &domain_report.direct {
            records.push(ProofManifestRecord {
                path: absolutize_path(&domain_report.publication_manifest_path)?,
                manifest: ProofManifest::Direct(gallery_direct_report(direct)?),
            });
        }
        if let Some(derived) = &domain_report.derived {
            records.push(ProofManifestRecord {
                path: absolutize_path(&domain_report.publication_manifest_path)?,
                manifest: ProofManifest::Derived(absolutize_derived_report(derived)?),
            });
        }
        if let Some(windowed) = &domain_report.windowed {
            records.push(ProofManifestRecord {
                path: absolutize_path(&domain_report.publication_manifest_path)?,
                manifest: ProofManifest::Windowed(absolutize_windowed_report(windowed)?),
            });
        }
    }
    Ok(records)
}

fn gallery_direct_report(
    report: &HrrrDirectBatchReport,
) -> Result<GalleryDirectBatchReport, Box<dyn std::error::Error>> {
    Ok(GalleryDirectBatchReport {
        date_yyyymmdd: report.date_yyyymmdd.clone(),
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source.as_str().to_string(),
        domain: report.domain.clone(),
        recipes: report
            .recipes
            .iter()
            .map(|recipe| {
                Ok(GalleryDirectRenderedRecipe {
                    recipe_slug: recipe.recipe_slug.clone(),
                    title: recipe.title.clone(),
                    output_path: absolutize_path(&recipe.output_path)?,
                    timing: GalleryDirectTiming {
                        total_ms: recipe.timing.total_ms,
                    },
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?,
        total_ms: report.total_ms,
    })
}

fn absolutize_derived_report(
    report: &HrrrDerivedBatchReport,
) -> Result<HrrrDerivedBatchReport, Box<dyn std::error::Error>> {
    let mut cloned = report.clone();
    for recipe in &mut cloned.recipes {
        recipe.output_path = absolutize_path(&recipe.output_path)?;
    }
    Ok(cloned)
}

fn absolutize_windowed_report(
    report: &HrrrWindowedBatchReport,
) -> Result<HrrrWindowedBatchReport, Box<dyn std::error::Error>> {
    let mut cloned = report.clone();
    for product in &mut cloned.products {
        product.output_path = absolutize_path(&product.output_path)?;
    }
    Ok(cloned)
}

fn decorate_and_sort_index(index: &mut ProofGalleryIndex, domain_catalog: &DomainCatalog) {
    for run in &mut index.runs {
        let domain_label = domain_catalog.label(&run.domain_slug);
        run.title = format!("{domain_label} - {}", run_kind_title(run.kind));
    }
    index.runs.sort_by(|left, right| {
        domain_catalog
            .order(&left.domain_slug)
            .cmp(&domain_catalog.order(&right.domain_slug))
            .then_with(|| proof_run_kind_order(left.kind).cmp(&proof_run_kind_order(right.kind)))
            .then_with(|| left.forecast_hour.cmp(&right.forecast_hour))
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn run_kind_title(kind: ProofRunKind) -> &'static str {
    match kind {
        ProofRunKind::Direct => "Direct",
        ProofRunKind::Derived => "Derived",
        ProofRunKind::Heavy => "Heavy",
        ProofRunKind::Windowed => "Windowed",
    }
}

fn proof_run_kind_order(kind: ProofRunKind) -> u8 {
    match kind {
        ProofRunKind::Direct => 0,
        ProofRunKind::Derived => 1,
        ProofRunKind::Windowed => 2,
        ProofRunKind::Heavy => 3,
    }
}

fn build_summary(
    args: &Args,
    out_dir: &Path,
    report_path: &Path,
    report: &HrrrNonEcapeMultiDomainReport,
    domain_catalog: &DomainCatalog,
    galleries: Vec<GalleryBuildSummary>,
) -> RegionCityGallerySummary {
    let expected_entries = domain_catalog.selected_entries(args.scope);
    let expected_slugs = expected_entries
        .iter()
        .map(|entry| entry.slug.clone())
        .collect::<HashSet<_>>();
    let actual_slugs = report
        .domains
        .iter()
        .map(|domain| domain.domain.slug.clone())
        .collect::<HashSet<_>>();
    let mut missing_expected_domain_slugs = expected_entries
        .iter()
        .filter(|entry| !actual_slugs.contains(&entry.slug))
        .map(|entry| entry.slug.clone())
        .collect::<Vec<_>>();
    missing_expected_domain_slugs.sort();

    let mut unexpected_domain_slugs = report
        .domains
        .iter()
        .map(|domain| domain.domain.slug.clone())
        .filter(|slug| !expected_slugs.contains(slug))
        .collect::<Vec<_>>();
    unexpected_domain_slugs.sort();
    unexpected_domain_slugs.dedup();

    let domains = build_domain_summaries(report, domain_catalog, &expected_slugs);
    let total_output_count = domains.iter().map(|domain| domain.output_count).sum();
    let missing_image_count = domains
        .iter()
        .map(|domain| domain.missing_image_count)
        .sum();
    let rendered_region_count = domains
        .iter()
        .filter(|domain| domain.domain_type == DomainKind::Region.as_str())
        .count();
    let rendered_city_count = domains
        .iter()
        .filter(|domain| domain.domain_type == DomainKind::City.as_str())
        .count();

    RegionCityGallerySummary {
        runner: "hrrr_region_city_gallery",
        mode: args.mode.as_str().to_string(),
        scope: args.scope.as_str().to_string(),
        title: args.title.clone(),
        date_yyyymmdd: report.date_yyyymmdd.clone(),
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source,
        out_dir: out_dir.to_path_buf(),
        report_path: report_path.to_path_buf(),
        cache_root: report.cache_root.clone(),
        use_cache: report.use_cache,
        source_mode: report.source_mode,
        requested_direct_recipe_slugs: report.requested.direct_recipe_slugs.clone(),
        requested_derived_recipe_slugs: report.requested.derived_recipe_slugs.clone(),
        expected_domain_count: expected_entries.len(),
        expected_region_count: expected_entries
            .iter()
            .filter(|entry| entry.kind == DomainKind::Region)
            .count(),
        expected_city_count: expected_entries
            .iter()
            .filter(|entry| entry.kind == DomainKind::City)
            .count(),
        rendered_domain_count: domains.len(),
        rendered_region_count,
        rendered_city_count,
        unexpected_domain_count: unexpected_domain_slugs.len(),
        total_output_count,
        missing_image_count,
        missing_expected_domain_slugs,
        unexpected_domain_slugs,
        shared_timing: report.shared_timing.clone(),
        fanout_timing: report.fanout_timing.clone(),
        total_ms: report.total_ms,
        galleries,
        domains,
    }
}

fn build_domain_summaries(
    report: &HrrrNonEcapeMultiDomainReport,
    domain_catalog: &DomainCatalog,
    expected_slugs: &HashSet<String>,
) -> Vec<DomainValidationSummary> {
    report
        .domains
        .iter()
        .map(|domain_report| {
            let outputs = collect_domain_outputs(domain_report);
            let class = domain_catalog.classify(&domain_report.domain.slug);
            DomainValidationSummary {
                domain_type: class.as_str().to_string(),
                slug: domain_report.domain.slug.clone(),
                label: domain_catalog.label(&domain_report.domain.slug),
                expected: expected_slugs.contains(&domain_report.domain.slug),
                publication_manifest_path: domain_report.publication_manifest_path.clone(),
                attempt_manifest_path: domain_report.attempt_manifest_path.clone(),
                output_count: domain_report.summary.output_count,
                direct_rendered_count: domain_report.summary.direct_rendered_count,
                derived_rendered_count: domain_report.summary.derived_rendered_count,
                windowed_rendered_count: domain_report.summary.windowed_rendered_count,
                windowed_blocker_count: domain_report.summary.windowed_blocker_count,
                missing_image_count: outputs.iter().filter(|output| !output.exists).count(),
                outputs,
            }
        })
        .collect()
}

fn collect_domain_outputs(domain_report: &HrrrNonEcapeDomainReport) -> Vec<DomainArtifactSummary> {
    let mut outputs = Vec::new();
    if let Some(direct) = &domain_report.direct {
        outputs.extend(direct.recipes.iter().map(|recipe| DomainArtifactSummary {
            lane: "direct".to_string(),
            slug: recipe.recipe_slug.clone(),
            title: recipe.title.clone(),
            output_path: recipe.output_path.clone(),
            exists: path_exists(&recipe.output_path),
            timing_ms: Some(recipe.timing.total_ms),
        }));
    }
    if let Some(derived) = &domain_report.derived {
        outputs.extend(derived.recipes.iter().map(|recipe| DomainArtifactSummary {
            lane: "derived".to_string(),
            slug: recipe.recipe_slug.clone(),
            title: recipe.title.clone(),
            output_path: recipe.output_path.clone(),
            exists: path_exists(&recipe.output_path),
            timing_ms: Some(recipe.timing.total_ms),
        }));
    }
    if let Some(windowed) = &domain_report.windowed {
        outputs.extend(
            windowed
                .products
                .iter()
                .map(|product| DomainArtifactSummary {
                    lane: "windowed".to_string(),
                    slug: product.product.slug().to_string(),
                    title: product.product.title().to_string(),
                    output_path: product.output_path.clone(),
                    exists: path_exists(&product.output_path),
                    timing_ms: Some(product.timing.total_ms),
                }),
        );
    }
    outputs
}

fn path_exists(path: &Path) -> bool {
    absolutize_path(path)
        .map(|resolved| resolved.exists())
        .unwrap_or(false)
}

fn absolutize_path(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn report_path(
    out_dir: &Path,
    report: &HrrrNonEcapeMultiDomainReport,
    scope: ValidationScopeArg,
) -> PathBuf {
    out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_region_city_gallery_report.json",
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        scope.as_str()
    ))
}

fn summary_path(
    out_dir: &Path,
    report: &HrrrNonEcapeMultiDomainReport,
    scope: ValidationScopeArg,
) -> PathBuf {
    out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_region_city_gallery_summary.json",
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        scope.as_str()
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        Args, DomainCatalog, DomainKind, GallerySection, PlaceLabelDensityArg, ProofGalleryIndex,
        ValidationScopeArg, build_domain_catalog, decorate_and_sort_index, gallery_sections,
        proof_run_kind_order, run_kind_title,
    };
    use clap::Parser;
    use rustwx_products::gallery::{
        ProofGalleryImage, ProofGalleryRun, ProofGallerySummary, ProofRunKind,
    };
    use std::path::PathBuf;

    fn sample_run(domain_slug: &str, kind: ProofRunKind) -> ProofGalleryRun {
        ProofGalleryRun {
            kind,
            title: "placeholder".to_string(),
            manifest_path: PathBuf::from(format!("{domain_slug}.json")),
            manifest_href: format!("{domain_slug}.json"),
            run_state: Some("complete".to_string()),
            date_yyyymmdd: "20260422".to_string(),
            cycle_utc: 7,
            forecast_hour: 0,
            source: "Nomads".to_string(),
            domain_slug: domain_slug.to_string(),
            total_ms: 10,
            blockers: Vec::new(),
            images: vec![ProofGalleryImage {
                slug: "visibility".to_string(),
                title: "Visibility".to_string(),
                image_path: PathBuf::from(format!("{domain_slug}.png")),
                image_href: format!("{domain_slug}.png"),
                exists: true,
                artifact_state: Some("complete".to_string()),
                timing_ms: Some(10),
                catalog_kind: Some("direct".to_string()),
                catalog_status: Some("supported".to_string()),
                experimental: false,
                notes: Vec::new(),
            }],
        }
    }

    fn decorate(index: &mut ProofGalleryIndex, catalog: &DomainCatalog) {
        decorate_and_sort_index(index, catalog);
    }

    #[test]
    fn domain_catalog_keeps_region_and_city_slugs_unique() {
        let catalog = build_domain_catalog();
        let mut seen = std::collections::HashSet::new();
        for entry in &catalog.entries {
            assert!(
                seen.insert(entry.slug.clone()),
                "duplicate slug {}",
                entry.slug
            );
        }
        assert_eq!(catalog.classify("conus"), super::DomainClass::Region);
        assert_eq!(catalog.classify("ca_los_angeles"), super::DomainClass::City);
    }

    #[test]
    fn selected_scope_filters_regions_and_cities() {
        let catalog = build_domain_catalog();
        let regions = catalog.selected_entries(ValidationScopeArg::Regions);
        let cities = catalog.selected_entries(ValidationScopeArg::Cities);
        let all = catalog.selected_entries(ValidationScopeArg::All);
        assert!(!regions.is_empty());
        assert!(!cities.is_empty());
        assert_eq!(all.len(), regions.len() + cities.len());
        assert!(regions.iter().all(|entry| entry.kind == DomainKind::Region));
        assert!(cities.iter().all(|entry| entry.kind == DomainKind::City));
    }

    #[test]
    fn gallery_sections_follow_requested_scope() {
        assert_eq!(
            gallery_sections(ValidationScopeArg::All),
            vec![
                GallerySection::All,
                GallerySection::Regions,
                GallerySection::Cities
            ]
        );
        assert_eq!(
            gallery_sections(ValidationScopeArg::Regions),
            vec![GallerySection::Regions]
        );
        assert_eq!(
            gallery_sections(ValidationScopeArg::Cities),
            vec![GallerySection::Cities]
        );
    }

    #[test]
    fn decorate_and_sort_index_prefers_catalog_order_and_domain_labels() {
        let catalog = build_domain_catalog();
        let mut index = ProofGalleryIndex {
            title: "test".to_string(),
            proof_root: PathBuf::from("proof"),
            catalog: None,
            summary: ProofGallerySummary {
                run_count: 2,
                image_count: 2,
                missing_image_count: 0,
                direct_run_count: 2,
                derived_run_count: 0,
                heavy_run_count: 0,
                windowed_run_count: 0,
            },
            runs: vec![
                sample_run("ca_los_angeles", ProofRunKind::Direct),
                sample_run("conus", ProofRunKind::Direct),
            ],
        };

        decorate(&mut index, &catalog);

        assert_eq!(index.runs[0].domain_slug, "conus");
        assert!(index.runs[0].title.contains("CONUS"));
        assert!(index.runs[1].title.contains("Los Angeles, CA"));
    }

    #[test]
    fn run_kind_titles_and_order_are_stable() {
        assert_eq!(run_kind_title(ProofRunKind::Direct), "Direct");
        assert_eq!(run_kind_title(ProofRunKind::Derived), "Derived");
        assert!(
            proof_run_kind_order(ProofRunKind::Direct)
                < proof_run_kind_order(ProofRunKind::Derived)
        );
        assert!(
            proof_run_kind_order(ProofRunKind::Derived)
                < proof_run_kind_order(ProofRunKind::Windowed)
        );
    }

    #[test]
    fn place_label_density_accepts_named_and_numeric_values() {
        let named =
            Args::try_parse_from(["hrrr-region-city-gallery", "--place-label-density", "dense"])
                .expect("named density should parse");
        assert_eq!(named.place_label_density, PlaceLabelDensityArg::Dense);

        let numeric =
            Args::try_parse_from(["hrrr-region-city-gallery", "--place-label-density", "3"])
                .expect("numeric density alias should parse");
        assert_eq!(numeric.place_label_density, PlaceLabelDensityArg::Dense);
    }
}
