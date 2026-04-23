use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};

#[allow(dead_code)]
#[path = "../metro.rs"]
mod metro;
#[allow(dead_code)]
#[path = "../region.rs"]
mod region;

use clap::{Parser, ValueEnum};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::non_ecape::{
    HrrrNonEcapeDomainReport, HrrrNonEcapeMultiDomainReport, HrrrNonEcapeMultiDomainRequest,
    run_hrrr_non_ecape_hour_multi_domain,
};
use rustwx_products::places::{PlaceLabelDensityTier, PlaceLabelOverlay};
use rustwx_products::publication::atomic_write_json;
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::source::ProductSourceMode;
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
enum ScopeArg {
    Sample,
    AllRegions,
    AllCities,
    All,
}

impl ScopeArg {
    fn slug(self) -> &'static str {
        match self {
            Self::Sample => "sample",
            Self::AllRegions => "all_regions",
            Self::AllCities => "all_cities",
            Self::All => "all",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DomainKind {
    Region,
    City,
    Unknown,
}

impl DomainKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Region => "region",
            Self::City => "city",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-place-label-density-compare",
    about = "Generate or summarize bounded 0/1/2/3 place-label density comparisons"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = RunModeArg::Run)]
    mode: RunModeArg,
    #[arg(long, value_enum, default_value_t = ScopeArg::Sample)]
    scope: ScopeArg,
    #[arg(long)]
    report_root: Option<PathBuf>,
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
        default_value = "C:\\Users\\drew\\rustwx-next\\rustwx\\proof\\place_label_density_tiers"
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
    #[arg(long = "direct-recipe", value_delimiter = ',', num_args = 1..)]
    direct_recipes: Vec<String>,
    #[arg(long = "derived-recipe", value_delimiter = ',', num_args = 1..)]
    derived_recipes: Vec<String>,
    #[arg(long = "tier", value_delimiter = ',', num_args = 1.., value_parser = clap::value_parser!(u8).range(0..=3))]
    tiers: Vec<u8>,
    #[arg(long, default_value_t = 4)]
    domain_jobs: usize,
    #[arg(long)]
    render_threads: Option<usize>,
    #[arg(long = "png-compression", value_enum, default_value_t = PngCompressionArg::Fast)]
    png_compression: PngCompressionArg,
    #[arg(long, default_value = "RustWX Place-Label Density Compare")]
    title: String,
}

#[derive(Debug, Clone)]
struct ScopeDomainEntry {
    kind: DomainKind,
    slug: String,
    label: String,
    domain: DomainSpec,
    order: usize,
}

#[derive(Debug, Clone)]
struct ScopeCatalog {
    entries: Vec<ScopeDomainEntry>,
    lookup: HashMap<String, ScopeDomainEntry>,
}

#[derive(Debug, Clone)]
struct TierReportRecord {
    tier: u8,
    report_path: PathBuf,
    report: HrrrNonEcapeMultiDomainReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TierReportSummary {
    tier: u8,
    density_slug: String,
    density_label: String,
    report_path: PathBuf,
    domain_count: usize,
    output_count: usize,
    total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompareTierSlot {
    tier: u8,
    density_slug: String,
    density_label: String,
    image_path: Option<PathBuf>,
    exists: bool,
    timing_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompareEntrySummary {
    domain_slug: String,
    domain_label: String,
    domain_type: DomainKind,
    lane: String,
    artifact_slug: String,
    artifact_title: String,
    present_tier_count: usize,
    missing_tiers: Vec<u8>,
    tiers: Vec<CompareTierSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompareIndexSummary {
    runner: &'static str,
    mode: String,
    scope: String,
    title: String,
    out_dir: PathBuf,
    compare_dir: PathBuf,
    report_root: PathBuf,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: rustwx_core::SourceId,
    selected_tiers: Vec<u8>,
    tier_reports: Vec<TierReportSummary>,
    expected_domain_count: usize,
    rendered_domain_count: usize,
    compare_entry_count: usize,
    missing_artifact_count: usize,
    entries: Vec<CompareEntrySummary>,
}

#[derive(Debug, Clone)]
struct CompareEntryBuilder {
    domain_slug: String,
    domain_label: String,
    domain_type: DomainKind,
    lane: String,
    artifact_slug: String,
    artifact_title: String,
    tiers: BTreeMap<u8, CompareTierSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CompareKey {
    domain_slug: String,
    lane: String,
    artifact_slug: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    run(&args)
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let selected_tiers = resolve_tiers(&args.tiers);
    let scope_catalog = build_scope_catalog(args.scope)?;
    fs::create_dir_all(&args.out_dir)?;

    let tier_reports = match args.mode {
        RunModeArg::Run => run_tier_reports(args, &scope_catalog, &selected_tiers)?,
        RunModeArg::Summarize => load_tier_reports(args, &selected_tiers)?,
    };
    validate_report_family(&tier_reports)?;

    let compare_dir = args.out_dir.join("compare");
    fs::create_dir_all(&compare_dir)?;
    let summary = build_compare_index(
        args,
        &compare_dir,
        &scope_catalog,
        &tier_reports,
        &selected_tiers,
    );
    let index_json_path = compare_dir.join("index.json");
    let index_html_path = compare_dir.join("index.html");
    atomic_write_json(&index_json_path, &summary)?;
    fs::write(&index_html_path, render_compare_html(&summary))?;

    for tier in &summary.tier_reports {
        println!("{}", tier.report_path.display());
    }
    println!("{}", index_html_path.display());
    println!("{}", index_json_path.display());
    Ok(())
}

fn resolve_tiers(requested: &[u8]) -> Vec<u8> {
    let tiers = if requested.is_empty() {
        vec![0, 1, 2, 3]
    } else {
        requested.to_vec()
    };
    BTreeSet::from_iter(tiers).into_iter().collect()
}

fn run_tier_reports(
    args: &Args,
    scope_catalog: &ScopeCatalog,
    selected_tiers: &[u8],
) -> Result<Vec<TierReportRecord>, Box<dyn std::error::Error>> {
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&args.out_dir));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }

    let domains = scope_catalog
        .entries
        .iter()
        .map(|entry| entry.domain.clone())
        .collect::<Vec<_>>();
    if domains.is_empty() {
        return Err("density compare scope resolved to zero domains".into());
    }

    let render_threads =
        configure_render_threads(args.render_threads, args.domain_jobs, domains.len());
    let direct_recipe_slugs = if args.direct_recipes.is_empty() {
        default_direct_recipes()
    } else {
        args.direct_recipes.clone()
    };

    let mut reports = Vec::with_capacity(selected_tiers.len());
    for tier in selected_tiers {
        let tier_out_dir = args.out_dir.join(format!("tier{tier}"));
        fs::create_dir_all(&tier_out_dir)?;
        let request = HrrrNonEcapeMultiDomainRequest {
            date_yyyymmdd: args.date.clone(),
            cycle_override_utc: Some(args.cycle),
            forecast_hour: args.forecast_hour,
            source: args.source,
            domains: domains.clone(),
            out_dir: tier_out_dir.clone(),
            cache_root: cache_root.clone(),
            use_cache: !args.no_cache,
            source_mode: ProductSourceMode::Fastest,
            direct_recipe_slugs: direct_recipe_slugs.clone(),
            derived_recipe_slugs: args.derived_recipes.clone(),
            windowed_products: Vec::new(),
            output_width: args.width,
            output_height: args.height,
            png_compression: args.png_compression.into(),
            custom_poi_overlay: None,
            place_label_overlay: Some(
                PlaceLabelOverlay::major_us_cities()
                    .with_density(PlaceLabelDensityTier::from_numeric(*tier)),
            ),
            domain_jobs: Some(args.domain_jobs.max(1)),
        };
        let report = run_hrrr_non_ecape_hour_multi_domain(&request)?;
        let report_path = tier_report_path(&tier_out_dir, &report, args.scope, *tier);
        atomic_write_json(&report_path, &report)?;
        reports.push(TierReportRecord {
            tier: *tier,
            report_path,
            report,
        });
    }

    if render_threads.is_none() {
        unsafe {
            std::env::remove_var("RUSTWX_RENDER_THREADS");
        }
    }

    Ok(reports)
}

fn load_tier_reports(
    args: &Args,
    selected_tiers: &[u8],
) -> Result<Vec<TierReportRecord>, Box<dyn std::error::Error>> {
    let report_root = args
        .report_root
        .clone()
        .unwrap_or_else(|| args.out_dir.clone());
    let mut reports = Vec::with_capacity(selected_tiers.len());
    for tier in selected_tiers {
        let report_path = discover_tier_report_path(&report_root, *tier)?;
        let bytes = fs::read(&report_path)?;
        let report = serde_json::from_slice::<HrrrNonEcapeMultiDomainReport>(&bytes)?;
        reports.push(TierReportRecord {
            tier: *tier,
            report_path,
            report,
        });
    }
    Ok(reports)
}

fn discover_tier_report_path(
    report_root: &Path,
    tier: u8,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let tier_dir = report_root.join(format!("tier{tier}"));
    if !tier_dir.is_dir() {
        return Err(format!("missing tier directory {}", tier_dir.display()).into());
    }

    let mut preferred = Vec::new();
    let mut legacy = Vec::new();
    for entry in fs::read_dir(&tier_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.ends_with(&format!("_place_label_density_tier{tier}_report.json")) {
            preferred.push(path);
        } else if name.ends_with("_place_label_proof_report.json") {
            legacy.push(path);
        }
    }

    select_unique_report_path(preferred, legacy, tier)
}

fn select_unique_report_path(
    preferred: Vec<PathBuf>,
    fallback: Vec<PathBuf>,
    tier: u8,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let selected = if !preferred.is_empty() {
        preferred
    } else {
        fallback
    };
    if selected.len() != 1 {
        return Err(format!(
            "expected exactly one tier {tier} report, found {}",
            selected.len()
        )
        .into());
    }
    Ok(selected.into_iter().next().expect("one selected report"))
}

fn validate_report_family(reports: &[TierReportRecord]) -> Result<(), Box<dyn std::error::Error>> {
    let Some(first) = reports.first() else {
        return Err("density compare needs at least one tier report".into());
    };
    let expected = (
        first.report.date_yyyymmdd.as_str(),
        first.report.cycle_utc,
        first.report.forecast_hour,
        first.report.source,
    );
    for report in reports.iter().skip(1) {
        let candidate = (
            report.report.date_yyyymmdd.as_str(),
            report.report.cycle_utc,
            report.report.forecast_hour,
            report.report.source,
        );
        if candidate != expected {
            return Err("tier reports do not belong to the same HRRR run family".into());
        }
    }
    Ok(())
}

fn build_compare_index(
    args: &Args,
    compare_dir: &Path,
    scope_catalog: &ScopeCatalog,
    tier_reports: &[TierReportRecord],
    selected_tiers: &[u8],
) -> CompareIndexSummary {
    let mut builders = HashMap::<CompareKey, CompareEntryBuilder>::new();
    let mut rendered_domain_slugs = BTreeSet::<String>::new();

    for tier_report in tier_reports {
        for domain_report in &tier_report.report.domains {
            rendered_domain_slugs.insert(domain_report.domain.slug.clone());
            let (domain_label, domain_type) = scope_catalog
                .lookup
                .get(&domain_report.domain.slug)
                .map(|entry| (entry.label.clone(), entry.kind))
                .unwrap_or_else(|| (domain_report.domain.slug.clone(), DomainKind::Unknown));
            for artifact in collect_compare_artifacts(domain_report) {
                let key = CompareKey {
                    domain_slug: domain_report.domain.slug.clone(),
                    lane: artifact.lane.clone(),
                    artifact_slug: artifact.artifact_slug.clone(),
                };
                let entry = builders.entry(key).or_insert_with(|| CompareEntryBuilder {
                    domain_slug: domain_report.domain.slug.clone(),
                    domain_label: domain_label.clone(),
                    domain_type,
                    lane: artifact.lane.clone(),
                    artifact_slug: artifact.artifact_slug.clone(),
                    artifact_title: artifact.artifact_title.clone(),
                    tiers: BTreeMap::new(),
                });
                entry.tiers.insert(
                    tier_report.tier,
                    CompareTierSlot {
                        tier: tier_report.tier,
                        density_slug: density_tier_slug(tier_report.tier).to_string(),
                        density_label: density_tier_label(tier_report.tier).to_string(),
                        image_path: Some(artifact.output_path.clone()),
                        exists: path_exists(&artifact.output_path),
                        timing_ms: artifact.timing_ms,
                    },
                );
            }
        }
    }

    let mut entries = builders
        .into_values()
        .map(|builder| {
            let tiers = selected_tiers
                .iter()
                .map(|tier| {
                    builder.tiers.get(tier).cloned().unwrap_or(CompareTierSlot {
                        tier: *tier,
                        density_slug: density_tier_slug(*tier).to_string(),
                        density_label: density_tier_label(*tier).to_string(),
                        image_path: None,
                        exists: false,
                        timing_ms: None,
                    })
                })
                .collect::<Vec<_>>();
            let missing_tiers = tiers
                .iter()
                .filter(|tier| tier.image_path.is_none() || !tier.exists)
                .map(|tier| tier.tier)
                .collect::<Vec<_>>();
            let present_tier_count = tiers
                .iter()
                .filter(|tier| tier.image_path.is_some() && tier.exists)
                .count();
            CompareEntrySummary {
                domain_slug: builder.domain_slug,
                domain_label: builder.domain_label,
                domain_type: builder.domain_type,
                lane: builder.lane,
                artifact_slug: builder.artifact_slug,
                artifact_title: builder.artifact_title,
                present_tier_count,
                missing_tiers,
                tiers,
            }
        })
        .collect::<Vec<_>>();

    entries.sort_by(|left, right| {
        scope_catalog
            .order(&left.domain_slug)
            .cmp(&scope_catalog.order(&right.domain_slug))
            .then_with(|| lane_order(&left.lane).cmp(&lane_order(&right.lane)))
            .then_with(|| left.artifact_title.cmp(&right.artifact_title))
            .then_with(|| left.artifact_slug.cmp(&right.artifact_slug))
    });

    let tier_summaries = tier_reports
        .iter()
        .map(|record| TierReportSummary {
            tier: record.tier,
            density_slug: density_tier_slug(record.tier).to_string(),
            density_label: density_tier_label(record.tier).to_string(),
            report_path: record.report_path.clone(),
            domain_count: record.report.domains.len(),
            output_count: record
                .report
                .domains
                .iter()
                .map(|domain| domain.summary.output_count)
                .sum(),
            total_ms: record.report.total_ms,
        })
        .collect::<Vec<_>>();

    let first_report = &tier_reports[0].report;
    CompareIndexSummary {
        runner: "hrrr_place_label_density_compare",
        mode: args.mode.as_str().to_string(),
        scope: args.scope.slug().to_string(),
        title: args.title.clone(),
        out_dir: args.out_dir.clone(),
        compare_dir: compare_dir.to_path_buf(),
        report_root: args
            .report_root
            .clone()
            .unwrap_or_else(|| args.out_dir.clone()),
        date_yyyymmdd: first_report.date_yyyymmdd.clone(),
        cycle_utc: first_report.cycle_utc,
        forecast_hour: first_report.forecast_hour,
        source: first_report.source,
        selected_tiers: selected_tiers.to_vec(),
        tier_reports: tier_summaries,
        expected_domain_count: scope_catalog.entries.len(),
        rendered_domain_count: rendered_domain_slugs.len(),
        compare_entry_count: entries.len(),
        missing_artifact_count: entries.iter().map(|entry| entry.missing_tiers.len()).sum(),
        entries,
    }
}

fn render_compare_html(summary: &CompareIndexSummary) -> String {
    let compare_dir = &summary.compare_dir;
    let mut html = String::new();
    html.push_str(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
    );
    html.push_str(&format!("<title>{}</title>", escape_html(&summary.title)));
    html.push_str(
        "<style>\
body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f3f4f6;color:#111827}\
header{padding:20px 24px;border-bottom:1px solid #d1d5db;background:#ffffff;position:sticky;top:0;z-index:10}\
h1,h2,h3,p,figure{margin:0}\
.summary{display:flex;gap:12px;flex-wrap:wrap;margin-top:12px;font-size:14px;color:#374151}\
.summary span{background:#eef2ff;border:1px solid #c7d2fe;padding:6px 10px;border-radius:999px}\
.toolbar{display:flex;gap:12px;flex-wrap:wrap;margin-top:14px}\
.toolbar input{background:#ffffff;color:#111827;border:1px solid #cbd5e1;border-radius:8px;padding:8px 10px;min-width:260px}\
main{padding:24px;display:flex;flex-direction:column;gap:20px}\
.reports{display:flex;gap:10px;flex-wrap:wrap;font-size:13px}\
.reports a{background:#ffffff;border:1px solid #cbd5e1;padding:8px 10px;border-radius:8px;text-decoration:none;color:#1d4ed8}\
.entry{background:#ffffff;border:1px solid #d1d5db;border-radius:12px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,0.04)}\
.entry-header{padding:16px 18px;border-bottom:1px solid #e5e7eb;display:flex;flex-direction:column;gap:8px}\
.entry-meta{display:flex;gap:8px;flex-wrap:wrap;font-size:12px;color:#4b5563}\
.pill{background:#f3f4f6;border:1px solid #d1d5db;padding:3px 8px;border-radius:999px}\
.pill.warn{background:#fff7ed;border-color:#fdba74;color:#9a3412}\
.tier-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:16px;padding:16px}\
.tier-card{background:#f8fafc;border:1px solid #dbe4ee;border-radius:10px;overflow:hidden}\
.tier-card header{position:static;padding:10px 12px;border-bottom:1px solid #dbe4ee;background:#f8fafc}\
.tier-card img{display:block;width:100%;height:auto;background:#e5e7eb}\
.tier-card .missing{padding:36px 12px;text-align:center;color:#991b1b;background:#fef2f2}\
.tier-card .meta{padding:10px 12px;font-size:12px;color:#4b5563;display:flex;gap:8px;flex-wrap:wrap}\
a{color:#1d4ed8}a:hover{text-decoration:underline}\
</style>",
    );
    html.push_str(
        "<script>\
function applyFilter(){\
 const needle=(document.getElementById('search').value||'').toLowerCase();\
 document.querySelectorAll('[data-entry]').forEach(function(entry){\
   const hay=(entry.dataset.search||'').toLowerCase();\
   entry.style.display=!needle||hay.indexOf(needle)!==-1?'block':'none';\
 });\
}\
</script>",
    );
    html.push_str("</head><body><header>");
    html.push_str(&format!("<h1>{}</h1>", escape_html(&summary.title)));
    html.push_str("<div class=\"summary\">");
    html.push_str(&format!(
        "<span>{}</span><span>{} compare entries</span><span>{} domains</span><span>{} missing artifacts</span><span>run {} {:02}Z F{:03}</span>",
        escape_html(&summary.scope),
        summary.compare_entry_count,
        summary.rendered_domain_count,
        summary.missing_artifact_count,
        escape_html(&summary.date_yyyymmdd),
        summary.cycle_utc,
        summary.forecast_hour,
    ));
    html.push_str("</div>");
    html.push_str(
        "<div class=\"toolbar\"><input id=\"search\" type=\"search\" placeholder=\"Filter by domain, lane, or recipe\" oninput=\"applyFilter()\"></div>",
    );
    html.push_str("<div class=\"reports\">");
    for report in &summary.tier_reports {
        let href = relative_href(compare_dir, &report.report_path);
        html.push_str(&format!(
            "<a href=\"{}\">Tier {} report</a>",
            escape_html(&href),
            report.tier
        ));
    }
    html.push_str("</div>");
    html.push_str("</header><main>");

    for entry in &summary.entries {
        let search = format!(
            "{} {} {} {}",
            entry.domain_label, entry.domain_slug, entry.lane, entry.artifact_title
        );
        html.push_str(&format!(
            "<section class=\"entry\" data-entry data-search=\"{}\">",
            escape_html(&search)
        ));
        html.push_str("<div class=\"entry-header\">");
        html.push_str(&format!(
            "<h2>{} - {}</h2>",
            escape_html(&entry.domain_label),
            escape_html(&entry.artifact_title)
        ));
        html.push_str("<div class=\"entry-meta\">");
        html.push_str(&format!(
            "<span class=\"pill\">{}</span><span class=\"pill\">{}</span><span class=\"pill\">{}</span>",
            escape_html(entry.domain_type.as_str()),
            escape_html(&entry.lane),
            escape_html(&entry.artifact_slug)
        ));
        if !entry.missing_tiers.is_empty() {
            html.push_str(&format!(
                "<span class=\"pill warn\">missing tiers: {}</span>",
                entry
                    .missing_tiers
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        html.push_str("</div></div>");
        html.push_str("<div class=\"tier-grid\">");
        for tier in &entry.tiers {
            html.push_str("<article class=\"tier-card\">");
            html.push_str(&format!(
                "<header><h3>Tier {}: {}</h3></header>",
                tier.tier,
                escape_html(&tier.density_label)
            ));
            match (&tier.image_path, tier.exists) {
                (Some(path), true) => {
                    let href = relative_href(compare_dir, path);
                    html.push_str(&format!(
                        "<a href=\"{}\"><img loading=\"lazy\" src=\"{}\" alt=\"{} tier {}\"></a>",
                        escape_html(&href),
                        escape_html(&href),
                        escape_html(&entry.artifact_title),
                        tier.tier
                    ));
                }
                _ => {
                    html.push_str("<div class=\"missing\">Missing artifact for this tier</div>");
                }
            }
            html.push_str("<div class=\"meta\">");
            if let Some(path) = &tier.image_path {
                html.push_str(&format!(
                    "<span>{}</span>",
                    escape_html(&path.display().to_string())
                ));
            }
            if let Some(ms) = tier.timing_ms {
                html.push_str(&format!("<span>{} ms</span>", ms));
            }
            html.push_str("</div></article>");
        }
        html.push_str("</div></section>");
    }

    html.push_str("</main></body></html>");
    html
}

#[derive(Debug, Clone)]
struct CompareArtifactRecord {
    lane: String,
    artifact_slug: String,
    artifact_title: String,
    output_path: PathBuf,
    timing_ms: Option<u128>,
}

fn collect_compare_artifacts(
    domain_report: &HrrrNonEcapeDomainReport,
) -> Vec<CompareArtifactRecord> {
    let mut artifacts = Vec::new();
    if let Some(direct) = &domain_report.direct {
        artifacts.extend(direct.recipes.iter().map(|recipe| CompareArtifactRecord {
            lane: "direct".to_string(),
            artifact_slug: recipe.recipe_slug.clone(),
            artifact_title: recipe.title.clone(),
            output_path: recipe.output_path.clone(),
            timing_ms: Some(recipe.timing.total_ms),
        }));
    }
    if let Some(derived) = &domain_report.derived {
        artifacts.extend(derived.recipes.iter().map(|recipe| CompareArtifactRecord {
            lane: "derived".to_string(),
            artifact_slug: recipe.recipe_slug.clone(),
            artifact_title: recipe.title.clone(),
            output_path: recipe.output_path.clone(),
            timing_ms: Some(recipe.timing.total_ms),
        }));
    }
    if let Some(windowed) = &domain_report.windowed {
        artifacts.extend(
            windowed
                .products
                .iter()
                .map(|product| CompareArtifactRecord {
                    lane: "windowed".to_string(),
                    artifact_slug: product.product.slug().to_string(),
                    artifact_title: product.product.title().to_string(),
                    output_path: product.output_path.clone(),
                    timing_ms: Some(product.timing.total_ms),
                }),
        );
    }
    artifacts
}

fn build_scope_catalog(scope: ScopeArg) -> Result<ScopeCatalog, Box<dyn std::error::Error>> {
    let mut entries = Vec::<ScopeDomainEntry>::new();
    let mut seen = BTreeSet::<String>::new();

    match scope {
        ScopeArg::Sample => {
            push_region(
                &mut entries,
                &mut seen,
                region::RegionPreset::CaliforniaSquare.slug(),
                "California Square",
                region::RegionPreset::CaliforniaSquare.bounds(),
            );
            push_region(
                &mut entries,
                &mut seen,
                region::RegionPreset::SouthernPlains.slug(),
                "Southern Plains",
                region::RegionPreset::SouthernPlains.bounds(),
            );
            push_city(&mut entries, &mut seen, "ca_los_angeles")?;
            push_city(&mut entries, &mut seen, "ca_san_francisco_bay")?;
            push_city(&mut entries, &mut seen, "ca_sacramento")?;
            push_city(&mut entries, &mut seen, "ca_san_diego")?;
        }
        ScopeArg::AllRegions => {
            push_region(
                &mut entries,
                &mut seen,
                region::RegionPreset::Conus.slug(),
                "CONUS",
                region::RegionPreset::Conus.bounds(),
            );
            push_split_regions(&mut entries, &mut seen);
        }
        ScopeArg::AllCities => {
            push_all_cities(&mut entries, &mut seen);
        }
        ScopeArg::All => {
            push_region(
                &mut entries,
                &mut seen,
                region::RegionPreset::Conus.slug(),
                "CONUS",
                region::RegionPreset::Conus.bounds(),
            );
            push_split_regions(&mut entries, &mut seen);
            push_all_cities(&mut entries, &mut seen);
        }
    }

    let lookup = entries
        .iter()
        .cloned()
        .map(|entry| (entry.slug.clone(), entry))
        .collect();
    Ok(ScopeCatalog { entries, lookup })
}

fn push_split_regions(entries: &mut Vec<ScopeDomainEntry>, seen: &mut BTreeSet<String>) {
    for split in region::US_SPLIT_REGION_PRESETS {
        push_region(entries, seen, split.slug, split.label, split.bounds);
    }
}

fn push_all_cities(entries: &mut Vec<ScopeDomainEntry>, seen: &mut BTreeSet<String>) {
    for preset in metro::MAJOR_US_CITY_PRESETS {
        if !seen.insert(preset.slug.to_string()) {
            continue;
        }
        entries.push(ScopeDomainEntry {
            kind: DomainKind::City,
            slug: preset.slug.to_string(),
            label: preset.label.to_string(),
            domain: preset.domain(),
            order: entries.len(),
        });
    }
}

fn push_region(
    entries: &mut Vec<ScopeDomainEntry>,
    seen: &mut BTreeSet<String>,
    slug: &str,
    label: &str,
    bounds: (f64, f64, f64, f64),
) {
    if !seen.insert(slug.to_string()) {
        return;
    }
    entries.push(ScopeDomainEntry {
        kind: DomainKind::Region,
        slug: slug.to_string(),
        label: label.to_string(),
        domain: DomainSpec::new(slug, bounds),
        order: entries.len(),
    });
}

fn push_city(
    entries: &mut Vec<ScopeDomainEntry>,
    seen: &mut BTreeSet<String>,
    slug: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !seen.insert(slug.to_string()) {
        return Ok(());
    }
    let preset = metro::MAJOR_US_CITY_PRESETS
        .iter()
        .find(|preset| preset.slug == slug)
        .ok_or_else(|| format!("missing city preset {slug}"))?;
    entries.push(ScopeDomainEntry {
        kind: DomainKind::City,
        slug: preset.slug.to_string(),
        label: preset.label.to_string(),
        domain: preset.domain(),
        order: entries.len(),
    });
    Ok(())
}

impl ScopeCatalog {
    fn order(&self, slug: &str) -> usize {
        self.lookup
            .get(slug)
            .map(|entry| entry.order)
            .unwrap_or(usize::MAX)
    }
}

fn tier_report_path(
    tier_out_dir: &Path,
    report: &HrrrNonEcapeMultiDomainReport,
    scope: ScopeArg,
    tier: u8,
) -> PathBuf {
    tier_out_dir.join(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_place_label_density_tier{}_report.json",
        report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        scope.slug(),
        tier
    ))
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

fn default_direct_recipes() -> Vec<String> {
    vec!["visibility", "500mb_temperature_height_winds"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn density_tier_slug(tier: u8) -> &'static str {
    match PlaceLabelDensityTier::from_numeric(tier) {
        PlaceLabelDensityTier::None => "none",
        PlaceLabelDensityTier::Major => "major",
        PlaceLabelDensityTier::MajorAndAux => "major_and_aux",
        PlaceLabelDensityTier::Dense => "dense",
    }
}

fn density_tier_label(tier: u8) -> &'static str {
    match PlaceLabelDensityTier::from_numeric(tier) {
        PlaceLabelDensityTier::None => "none",
        PlaceLabelDensityTier::Major => "major",
        PlaceLabelDensityTier::MajorAndAux => "major + aux",
        PlaceLabelDensityTier::Dense => "dense x4",
    }
}

fn lane_order(lane: &str) -> u8 {
    match lane {
        "direct" => 0,
        "derived" => 1,
        "windowed" => 2,
        _ => 3,
    }
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

fn relative_href(from_dir: &Path, to_path: &Path) -> String {
    let base = absolutize_path(from_dir).unwrap_or_else(|_| from_dir.to_path_buf());
    let target = absolutize_path(to_path).unwrap_or_else(|_| to_path.to_path_buf());
    let base_components = base.components().collect::<Vec<_>>();
    let target_components = target.components().collect::<Vec<_>>();
    let mut shared = 0usize;
    while shared < base_components.len()
        && shared < target_components.len()
        && base_components[shared] == target_components[shared]
    {
        shared += 1;
    }

    let mut parts = Vec::new();
    for _ in shared..base_components.len() {
        parts.push("..".to_string());
    }
    for component in target_components.iter().skip(shared) {
        match component {
            Component::Normal(value) => parts.push(value.to_string_lossy().replace('\\', "/")),
            Component::CurDir => parts.push(".".to_string()),
            Component::ParentDir => parts.push("..".to_string()),
            Component::Prefix(prefix) => {
                parts.push(prefix.as_os_str().to_string_lossy().replace('\\', "/"))
            }
            Component::RootDir => {}
        }
    }

    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::{
        ScopeArg, build_scope_catalog, density_tier_label, density_tier_slug,
        discover_tier_report_path, resolve_tiers,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rustwx_{name}_{unique}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn resolve_tiers_defaults_and_dedupes() {
        assert_eq!(resolve_tiers(&[]), vec![0, 1, 2, 3]);
        assert_eq!(resolve_tiers(&[3, 1, 1, 0]), vec![0, 1, 3]);
    }

    #[test]
    fn density_tier_labels_are_stable() {
        assert_eq!(density_tier_slug(0), "none");
        assert_eq!(density_tier_slug(1), "major");
        assert_eq!(density_tier_label(2), "major + aux");
        assert_eq!(density_tier_label(3), "dense x4");
    }

    #[test]
    fn sample_scope_keeps_expected_domains() {
        let catalog = build_scope_catalog(ScopeArg::Sample).expect("sample catalog");
        let slugs = catalog
            .entries
            .iter()
            .map(|entry| entry.slug.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            slugs,
            vec![
                "california_square",
                "southern_plains",
                "ca_los_angeles",
                "ca_san_francisco_bay",
                "ca_sacramento",
                "ca_san_diego"
            ]
        );
    }

    #[test]
    fn all_scope_contains_regions_and_cities() {
        let catalog = build_scope_catalog(ScopeArg::All).expect("all catalog");
        assert!(catalog.entries.iter().any(|entry| entry.slug == "conus"));
        assert!(
            catalog
                .entries
                .iter()
                .any(|entry| entry.slug == "ca_los_angeles")
        );
    }

    #[test]
    fn summarize_discovers_new_and_legacy_report_names() {
        let root = temp_dir("place_label_density_compare");
        let tier0 = root.join("tier0");
        let tier1 = root.join("tier1");
        fs::create_dir_all(&tier0).expect("tier0");
        fs::create_dir_all(&tier1).expect("tier1");
        let new_name = tier0
            .join("rustwx_hrrr_20260422_07z_f000_sample_place_label_density_tier0_report.json");
        let legacy_name = tier1.join("rustwx_hrrr_20260422_7z_f000_place_label_proof_report.json");
        fs::write(&new_name, b"{}").expect("write new");
        fs::write(&legacy_name, b"{}").expect("write legacy");

        assert_eq!(
            discover_tier_report_path(&root, 0).expect("discover new"),
            new_name
        );
        assert_eq!(
            discover_tier_report_path(&root, 1).expect("discover legacy"),
            legacy_name
        );

        let _ = fs::remove_dir_all(root);
    }
}
