use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::derived::HrrrDerivedBatchReport;
use crate::hrrr::{DomainSpec, HrrrBatchProduct, HrrrBatchReport};
use crate::publication::{ArtifactPublicationState, RunPublicationManifest, RunPublicationState};
use crate::windowed::HrrrWindowedBatchReport;

#[derive(Debug)]
pub enum GalleryError {
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedManifest(PathBuf),
}

impl fmt::Display for GalleryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Json(err) => write!(f, "{err}"),
            Self::UnsupportedManifest(path) => {
                write!(f, "unsupported proof manifest: {}", path.display())
            }
        }
    }
}

impl std::error::Error for GalleryError {}

impl From<std::io::Error> for GalleryError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for GalleryError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProofRunKind {
    Direct,
    Derived,
    Heavy,
    Windowed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofManifestRecord {
    pub path: PathBuf,
    pub manifest: ProofManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "manifest_kind", rename_all = "snake_case")]
pub enum ProofManifest {
    Run(RunPublicationManifest),
    Direct(GalleryDirectBatchReport),
    Derived(HrrrDerivedBatchReport),
    Heavy(HrrrBatchReport),
    Windowed(HrrrWindowedBatchReport),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalleryDirectBatchReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: String,
    pub domain: DomainSpec,
    pub recipes: Vec<GalleryDirectRenderedRecipe>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalleryDirectRenderedRecipe {
    pub recipe_slug: String,
    pub title: String,
    pub output_path: PathBuf,
    pub timing: GalleryDirectTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalleryDirectTiming {
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofGallerySummary {
    pub run_count: usize,
    pub image_count: usize,
    pub missing_image_count: usize,
    pub direct_run_count: usize,
    pub derived_run_count: usize,
    pub heavy_run_count: usize,
    pub windowed_run_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofCatalogSnapshot {
    pub total_entries: usize,
    pub supported_entries: usize,
    pub partial_entries: usize,
    pub blocked_entries: usize,
    pub experimental_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofGalleryIndex {
    pub title: String,
    pub proof_root: PathBuf,
    pub catalog: Option<ProofCatalogSnapshot>,
    pub summary: ProofGallerySummary,
    pub runs: Vec<ProofGalleryRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofGalleryRun {
    pub kind: ProofRunKind,
    pub title: String,
    pub manifest_path: PathBuf,
    pub manifest_href: String,
    pub run_state: Option<String>,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: String,
    pub domain_slug: String,
    pub total_ms: u128,
    pub blockers: Vec<String>,
    pub images: Vec<ProofGalleryImage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofGalleryImage {
    pub slug: String,
    pub title: String,
    pub image_path: PathBuf,
    pub image_href: String,
    pub exists: bool,
    pub artifact_state: Option<String>,
    pub timing_ms: Option<u128>,
    pub catalog_kind: Option<String>,
    pub catalog_status: Option<String>,
    pub experimental: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct GalleryCatalog {
    pub summary: ProofCatalogSnapshot,
    entries: HashMap<String, GalleryCatalogEntry>,
}

#[derive(Debug, Clone)]
struct GalleryCatalogEntry {
    title: String,
    kind: String,
    status: String,
    experimental: bool,
    notes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ParsedCatalogFile {
    summary: ParsedCatalogSummary,
    direct: Vec<ParsedCatalogEntry>,
    derived: Vec<ParsedCatalogEntry>,
    heavy: Vec<ParsedCatalogEntry>,
    windowed: Vec<ParsedCatalogEntry>,
}

#[derive(Debug, Deserialize)]
struct ParsedCatalogSummary {
    total_entries: usize,
    supported_entries: usize,
    partial_entries: usize,
    blocked_entries: usize,
    experimental_entries: usize,
}

#[derive(Debug, Deserialize)]
struct ParsedCatalogEntry {
    slug: String,
    title: String,
    kind: String,
    status: String,
    experimental: bool,
    notes: Vec<String>,
}

pub fn load_gallery_catalog(path: &Path) -> Result<GalleryCatalog, GalleryError> {
    let bytes = fs::read(path)?;
    let parsed: ParsedCatalogFile = serde_json::from_slice(&bytes)?;
    let mut entries = HashMap::new();
    for entry in parsed
        .direct
        .into_iter()
        .chain(parsed.derived.into_iter())
        .chain(parsed.heavy.into_iter())
        .chain(parsed.windowed.into_iter())
    {
        entries.insert(
            entry.slug,
            GalleryCatalogEntry {
                title: entry.title,
                kind: entry.kind,
                status: entry.status,
                experimental: entry.experimental,
                notes: entry.notes,
            },
        );
    }
    Ok(GalleryCatalog {
        summary: ProofCatalogSnapshot {
            total_entries: parsed.summary.total_entries,
            supported_entries: parsed.summary.supported_entries,
            partial_entries: parsed.summary.partial_entries,
            blocked_entries: parsed.summary.blocked_entries,
            experimental_entries: parsed.summary.experimental_entries,
        },
        entries,
    })
}

pub fn load_proof_manifest(path: &Path) -> Result<ProofManifestRecord, GalleryError> {
    let bytes = fs::read(path)?;
    if let Ok(manifest) = serde_json::from_slice::<RunPublicationManifest>(&bytes) {
        return Ok(ProofManifestRecord {
            path: path.to_path_buf(),
            manifest: ProofManifest::Run(manifest),
        });
    }
    if let Ok(report) = serde_json::from_slice::<HrrrDerivedBatchReport>(&bytes) {
        return Ok(ProofManifestRecord {
            path: path.to_path_buf(),
            manifest: ProofManifest::Derived(report),
        });
    }
    if let Ok(report) = serde_json::from_slice::<GalleryDirectBatchReport>(&bytes) {
        return Ok(ProofManifestRecord {
            path: path.to_path_buf(),
            manifest: ProofManifest::Direct(report),
        });
    }
    if let Ok(report) = serde_json::from_slice::<HrrrBatchReport>(&bytes) {
        return Ok(ProofManifestRecord {
            path: path.to_path_buf(),
            manifest: ProofManifest::Heavy(report),
        });
    }
    if let Ok(report) = serde_json::from_slice::<HrrrWindowedBatchReport>(&bytes) {
        return Ok(ProofManifestRecord {
            path: path.to_path_buf(),
            manifest: ProofManifest::Windowed(report),
        });
    }
    Err(GalleryError::UnsupportedManifest(path.to_path_buf()))
}

pub fn build_proof_gallery_index(
    title: &str,
    proof_root: &Path,
    viewer_dir: &Path,
    catalog: Option<&GalleryCatalog>,
    manifests: &[ProofManifestRecord],
) -> ProofGalleryIndex {
    let mut runs = manifests
        .iter()
        .map(|record| build_run(record, viewer_dir, catalog))
        .collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        right
            .date_yyyymmdd
            .cmp(&left.date_yyyymmdd)
            .then_with(|| right.cycle_utc.cmp(&left.cycle_utc))
            .then_with(|| left.forecast_hour.cmp(&right.forecast_hour))
            .then_with(|| left.kind_sort_key().cmp(&right.kind_sort_key()))
            .then_with(|| left.title.cmp(&right.title))
    });

    let mut summary = ProofGallerySummary {
        run_count: runs.len(),
        image_count: 0,
        missing_image_count: 0,
        direct_run_count: 0,
        derived_run_count: 0,
        heavy_run_count: 0,
        windowed_run_count: 0,
    };
    for run in &runs {
        match run.kind {
            ProofRunKind::Direct => summary.direct_run_count += 1,
            ProofRunKind::Derived => summary.derived_run_count += 1,
            ProofRunKind::Heavy => summary.heavy_run_count += 1,
            ProofRunKind::Windowed => summary.windowed_run_count += 1,
        }
        summary.image_count += run.images.len();
        summary.missing_image_count += run.images.iter().filter(|image| !image.exists).count();
    }

    ProofGalleryIndex {
        title: title.to_string(),
        proof_root: proof_root.to_path_buf(),
        catalog: catalog.map(|value| value.summary.clone()),
        summary,
        runs,
    }
}

pub fn render_gallery_html(index: &ProofGalleryIndex) -> String {
    let mut html = String::new();
    html.push_str(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
    );
    html.push_str(&format!("<title>{}</title>", escape_html(&index.title)));
    html.push_str(
        "<style>\
body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#111827;color:#e5e7eb}\
header{padding:20px 24px;border-bottom:1px solid #374151;background:#0f172a;position:sticky;top:0;z-index:10}\
h1,h2,p{margin:0}\
.summary{display:flex;gap:16px;flex-wrap:wrap;margin-top:12px;font-size:14px;color:#cbd5e1}\
.summary span{background:#1f2937;border:1px solid #374151;padding:6px 10px;border-radius:6px}\
.toolbar{display:flex;gap:12px;flex-wrap:wrap;margin-top:14px}\
.toolbar input,.toolbar select{background:#111827;color:#e5e7eb;border:1px solid #4b5563;border-radius:6px;padding:8px 10px}\
main{padding:24px;display:flex;flex-direction:column;gap:24px}\
.run{border:1px solid #374151;border-radius:8px;background:#0b1220;overflow:hidden}\
.run-header{padding:16px 18px;border-bottom:1px solid #374151;display:flex;flex-direction:column;gap:8px}\
.run-meta{display:flex;gap:10px;flex-wrap:wrap;font-size:13px;color:#cbd5e1}\
.run-meta span{background:#111827;border:1px solid #374151;padding:4px 8px;border-radius:999px}\
.blockers{margin-top:8px;color:#fca5a5;font-size:13px}\
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;padding:16px}\
.card{background:#111827;border:1px solid #374151;border-radius:8px;overflow:hidden}\
.card img{display:block;width:100%;height:auto;background:#0b1220}\
.card-body{padding:12px;display:flex;flex-direction:column;gap:8px}\
.card-title{font-weight:600}\
.card-meta{display:flex;gap:8px;flex-wrap:wrap;font-size:12px;color:#cbd5e1}\
.pill{background:#1f2937;border:1px solid #4b5563;padding:3px 6px;border-radius:999px}\
.pill.exp{color:#fde68a;border-color:#b45309}\
.missing{color:#fca5a5;padding:16px}\
a{color:#93c5fd;text-decoration:none}a:hover{text-decoration:underline}.links{display:flex;gap:12px;flex-wrap:wrap;font-size:13px}\
</style>",
    );
    html.push_str(
        "<script>\
function applyFilters(){\
 const needle=(document.getElementById('search').value||'').toLowerCase();\
 const kind=document.getElementById('kind').value;\
 document.querySelectorAll('[data-run]').forEach(function(run){\
   const runKind=run.dataset.kind;\
   let runVisible=false;\
   run.querySelectorAll('[data-card]').forEach(function(card){\
     const hay=(card.dataset.search||'').toLowerCase();\
     const visible=(!needle||hay.indexOf(needle)!==-1)&&(!kind||runKind===kind);\
     card.style.display=visible?'block':'none';\
     if(visible){runVisible=true;}\
   });\
   run.style.display=runVisible?'block':'none';\
 });\
}\
</script>",
    );
    html.push_str("</head><body><header>");
    html.push_str(&format!("<h1>{}</h1>", escape_html(&index.title)));
    html.push_str("<div class=\"summary\">");
    html.push_str(&format!(
        "<span>{} runs</span><span>{} images</span><span>{} missing</span><span>{} direct</span><span>{} derived</span><span>{} heavy</span><span>{} windowed</span>",
        index.summary.run_count,
        index.summary.image_count,
        index.summary.missing_image_count,
        index.summary.direct_run_count,
        index.summary.derived_run_count,
        index.summary.heavy_run_count,
        index.summary.windowed_run_count
    ));
    if let Some(catalog) = &index.catalog {
        html.push_str(&format!(
            "<span>catalog: {} total / {} supported / {} partial / {} blocked / {} experimental</span>",
            catalog.total_entries,
            catalog.supported_entries,
            catalog.partial_entries,
            catalog.blocked_entries,
            catalog.experimental_entries
        ));
    }
    html.push_str("</div>");
    html.push_str(
        "<div class=\"toolbar\"><input id=\"search\" type=\"search\" placeholder=\"Filter by slug or title\" oninput=\"applyFilters()\">\
         <select id=\"kind\" onchange=\"applyFilters()\">\
         <option value=\"\">All run kinds</option>\
         <option value=\"direct\">Direct</option><option value=\"derived\">Derived</option>\
         <option value=\"heavy\">Heavy</option><option value=\"windowed\">Windowed</option>\
         </select></div></header><main>",
    );
    for run in &index.runs {
        html.push_str(&format!(
            "<section class=\"run\" data-run data-kind=\"{}\">",
            run.kind_slug()
        ));
        html.push_str("<div class=\"run-header\">");
        html.push_str(&format!("<h2>{}</h2>", escape_html(&run.title)));
        html.push_str(&format!(
            "<div class=\"run-meta\"><span>{}</span><span>{}Z</span><span>F{:03}</span><span>{}</span><span>{}</span><span>{} ms</span>",
            escape_html(&run.date_yyyymmdd),
            run.cycle_utc,
            run.forecast_hour,
            escape_html(&run.source),
            escape_html(&run.domain_slug),
            run.total_ms
        ));
        if let Some(run_state) = &run.run_state {
            html.push_str(&format!("<span>{}</span>", escape_html(run_state)));
        }
        html.push_str("</div>");
        html.push_str(&format!(
            "<div class=\"links\"><a href=\"{}\">manifest</a></div>",
            escape_html(&run.manifest_href)
        ));
        if !run.blockers.is_empty() {
            html.push_str("<div class=\"blockers\"><strong>Blockers:</strong><ul>");
            for blocker in &run.blockers {
                html.push_str(&format!("<li>{}</li>", escape_html(blocker)));
            }
            html.push_str("</ul></div>");
        }
        html.push_str("</div><div class=\"cards\">");
        for image in &run.images {
            let search = format!("{} {}", image.slug, image.title);
            html.push_str(&format!(
                "<article class=\"card\" data-card data-search=\"{}\">",
                escape_html(&search)
            ));
            if image.exists {
                html.push_str(&format!(
                    "<a href=\"{}\"><img loading=\"lazy\" src=\"{}\" alt=\"{}\"></a>",
                    escape_html(&image.image_href),
                    escape_html(&image.image_href),
                    escape_html(&image.title)
                ));
            } else {
                html.push_str("<div class=\"missing\">Missing PNG on disk</div>");
            }
            html.push_str("<div class=\"card-body\">");
            html.push_str(&format!(
                "<div class=\"card-title\">{}</div>",
                escape_html(&image.title)
            ));
            html.push_str("<div class=\"card-meta\">");
            html.push_str(&format!(
                "<span class=\"pill\">{}</span>",
                escape_html(&image.slug)
            ));
            if let Some(kind) = &image.catalog_kind {
                html.push_str(&format!(
                    "<span class=\"pill\">{}</span>",
                    escape_html(kind)
                ));
            }
            if let Some(status) = &image.catalog_status {
                html.push_str(&format!(
                    "<span class=\"pill\">{}</span>",
                    escape_html(status)
                ));
            }
            if let Some(state) = &image.artifact_state {
                html.push_str(&format!(
                    "<span class=\"pill\">{}</span>",
                    escape_html(state)
                ));
            }
            if image.experimental {
                html.push_str("<span class=\"pill exp\">experimental</span>");
            }
            if let Some(timing_ms) = image.timing_ms {
                html.push_str(&format!("<span class=\"pill\">{} ms</span>", timing_ms));
            }
            html.push_str("</div>");
            if !image.notes.is_empty() {
                html.push_str("<div class=\"card-meta\">");
                for note in &image.notes {
                    html.push_str(&format!("<span>{}</span>", escape_html(note)));
                }
                html.push_str("</div>");
            }
            if image.exists {
                html.push_str(&format!(
                    "<div class=\"links\"><a href=\"{}\">open png</a></div>",
                    escape_html(&image.image_href)
                ));
            }
            html.push_str("</div></article>");
        }
        html.push_str("</div></section>");
    }
    html.push_str("</main></body></html>");
    html
}

fn build_run(
    record: &ProofManifestRecord,
    viewer_dir: &Path,
    catalog: Option<&GalleryCatalog>,
) -> ProofGalleryRun {
    match &record.manifest {
        ProofManifest::Run(manifest) => {
            let parsed = parsed_run_metadata(manifest);
            let source = manifest.source.clone().unwrap_or_else(|| {
                infer_manifest_source(manifest).unwrap_or_else(|| "unknown".to_string())
            });
            let blockers = manifest
                .artifacts
                .iter()
                .filter(|artifact| artifact.state == ArtifactPublicationState::Blocked)
                .map(|artifact| match &artifact.detail {
                    Some(detail) => format!("{}: {}", artifact.artifact_key, detail),
                    None => artifact.artifact_key.clone(),
                })
                .collect::<Vec<_>>();
            let title = format_run_title(manifest, parsed.model_slug.as_deref());
            let images = manifest
                .artifacts
                .iter()
                .map(|artifact| {
                    let image_path = materialize_artifact_path(&manifest.output_root, artifact);
                    gallery_image(
                        &artifact.artifact_key,
                        &artifact.artifact_key,
                        &image_path,
                        None,
                        Some(artifact.state),
                        artifact.detail.as_deref(),
                        viewer_dir,
                        catalog,
                    )
                })
                .collect();

            ProofGalleryRun {
                kind: classify_manifest_kind(manifest),
                title,
                manifest_path: record.path.clone(),
                manifest_href: relative_href(viewer_dir, &record.path),
                run_state: Some(run_state_slug(manifest.state).to_string()),
                date_yyyymmdd: parsed
                    .date_yyyymmdd
                    .unwrap_or_else(|| "unknown".to_string()),
                cycle_utc: parsed.cycle_utc.unwrap_or(0),
                forecast_hour: parsed.forecast_hour.unwrap_or(0),
                source,
                domain_slug: parsed.domain_slug.unwrap_or_else(|| "unknown".to_string()),
                total_ms: manifest
                    .finished_unix_ms
                    .map(|finished| finished.saturating_sub(manifest.started_unix_ms))
                    .unwrap_or_default(),
                blockers,
                images,
            }
        }
        ProofManifest::Direct(report) => ProofGalleryRun {
            kind: ProofRunKind::Direct,
            title: "HRRR Direct Batch".to_string(),
            manifest_path: record.path.clone(),
            manifest_href: relative_href(viewer_dir, &record.path),
            run_state: None,
            date_yyyymmdd: report.date_yyyymmdd.clone(),
            cycle_utc: report.cycle_utc,
            forecast_hour: report.forecast_hour,
            source: report.source.clone(),
            domain_slug: report.domain.slug.clone(),
            total_ms: report.total_ms,
            blockers: Vec::new(),
            images: report
                .recipes
                .iter()
                .map(|recipe| {
                    gallery_image(
                        &recipe.recipe_slug,
                        &recipe.title,
                        &recipe.output_path,
                        Some(recipe.timing.total_ms),
                        None,
                        None,
                        viewer_dir,
                        catalog,
                    )
                })
                .collect(),
        },
        ProofManifest::Derived(report) => ProofGalleryRun {
            kind: ProofRunKind::Derived,
            title: "HRRR Derived Batch".to_string(),
            manifest_path: record.path.clone(),
            manifest_href: relative_href(viewer_dir, &record.path),
            run_state: None,
            date_yyyymmdd: report.date_yyyymmdd.clone(),
            cycle_utc: report.cycle_utc,
            forecast_hour: report.forecast_hour,
            source: format!("{:?}", report.source),
            domain_slug: report.domain.slug.clone(),
            total_ms: report.total_ms,
            blockers: Vec::new(),
            images: report
                .recipes
                .iter()
                .map(|recipe| {
                    gallery_image(
                        &recipe.recipe_slug,
                        &recipe.title,
                        &recipe.output_path,
                        Some(recipe.timing.total_ms),
                        None,
                        None,
                        viewer_dir,
                        catalog,
                    )
                })
                .collect(),
        },
        ProofManifest::Heavy(report) => ProofGalleryRun {
            kind: ProofRunKind::Heavy,
            title: "HRRR Heavy Batch".to_string(),
            manifest_path: record.path.clone(),
            manifest_href: relative_href(viewer_dir, &record.path),
            run_state: None,
            date_yyyymmdd: report.date_yyyymmdd.clone(),
            cycle_utc: report.cycle_utc,
            forecast_hour: report.forecast_hour,
            source: format!("{:?}", report.source),
            domain_slug: report.domain.slug.clone(),
            total_ms: report.total_ms,
            blockers: Vec::new(),
            images: report
                .products
                .iter()
                .map(|product| {
                    let (slug, title) = match product.product {
                        HrrrBatchProduct::SevereProofPanel => {
                            ("severe_proof_panel", "HRRR Severe Proof Panel")
                        }
                    };
                    gallery_image(
                        slug,
                        title,
                        &product.output_path,
                        Some(product.timing.total_ms),
                        None,
                        None,
                        viewer_dir,
                        catalog,
                    )
                })
                .collect(),
        },
        ProofManifest::Windowed(report) => ProofGalleryRun {
            kind: ProofRunKind::Windowed,
            title: "HRRR Windowed Batch".to_string(),
            manifest_path: record.path.clone(),
            manifest_href: relative_href(viewer_dir, &record.path),
            run_state: None,
            date_yyyymmdd: report.date_yyyymmdd.clone(),
            cycle_utc: report.cycle_utc,
            forecast_hour: report.forecast_hour,
            source: format!("{:?}", report.source),
            domain_slug: report.domain.slug.clone(),
            total_ms: report.total_ms,
            blockers: report
                .blockers
                .iter()
                .map(|blocker| format!("{}: {}", blocker.product.slug(), blocker.reason))
                .collect(),
            images: report
                .products
                .iter()
                .map(|product| {
                    gallery_image(
                        product.product.slug(),
                        product.product.title(),
                        &product.output_path,
                        Some(product.timing.total_ms),
                        None,
                        None,
                        viewer_dir,
                        catalog,
                    )
                })
                .collect(),
        },
    }
}

fn gallery_image(
    slug: &str,
    title: &str,
    image_path: &Path,
    timing_ms: Option<u128>,
    artifact_state: Option<ArtifactPublicationState>,
    detail: Option<&str>,
    viewer_dir: &Path,
    catalog: Option<&GalleryCatalog>,
) -> ProofGalleryImage {
    let catalog_entry = catalog.and_then(|value| value.entries.get(slug));
    let resolved_title = catalog_entry
        .map(|entry| entry.title.clone())
        .unwrap_or_else(|| title.to_string());
    let mut notes = catalog_entry
        .map(|entry| entry.notes.clone())
        .unwrap_or_default();
    if let Some(detail) = detail {
        notes.push(detail.to_string());
    }
    ProofGalleryImage {
        slug: slug.to_string(),
        title: resolved_title,
        image_path: image_path.to_path_buf(),
        image_href: relative_href(viewer_dir, image_path),
        exists: image_path.exists(),
        artifact_state: artifact_state.map(artifact_state_slug).map(str::to_string),
        timing_ms,
        catalog_kind: catalog_entry.map(|entry| entry.kind.clone()),
        catalog_status: catalog_entry.map(|entry| entry.status.clone()),
        experimental: catalog_entry
            .map(|entry| entry.experimental)
            .unwrap_or(false),
        notes,
    }
}

#[derive(Debug, Clone)]
struct ParsedRunMetadata {
    model_slug: Option<String>,
    date_yyyymmdd: Option<String>,
    cycle_utc: Option<u8>,
    forecast_hour: Option<u16>,
    domain_slug: Option<String>,
}

fn parsed_run_metadata(manifest: &RunPublicationManifest) -> ParsedRunMetadata {
    let mut parsed = ParsedRunMetadata {
        model_slug: manifest.model.clone(),
        date_yyyymmdd: manifest.date_yyyymmdd.clone(),
        cycle_utc: manifest.cycle_utc,
        forecast_hour: manifest.forecast_hour,
        domain_slug: manifest.domain_slug.clone(),
    };
    if parsed.date_yyyymmdd.is_some()
        && parsed.cycle_utc.is_some()
        && parsed.forecast_hour.is_some()
        && parsed.domain_slug.is_some()
    {
        return parsed;
    }

    let tokens = manifest.run_label.split('_').collect::<Vec<_>>();
    if tokens.len() < 6 || tokens.first().copied() != Some("rustwx") {
        return parsed;
    }
    let Some(date_index) = tokens
        .iter()
        .position(|token| token.len() == 8 && token.chars().all(|ch| ch.is_ascii_digit()))
    else {
        return parsed;
    };
    if parsed.model_slug.is_none() && date_index > 1 {
        parsed.model_slug = Some(tokens[1..date_index].join("_"));
    }
    if parsed.date_yyyymmdd.is_none() {
        parsed.date_yyyymmdd = Some(tokens[date_index].to_string());
    }
    if parsed.cycle_utc.is_none() {
        parsed.cycle_utc = tokens
            .get(date_index + 1)
            .and_then(|token| token.strip_suffix('z'))
            .and_then(|value| value.parse::<u8>().ok());
    }
    if parsed.forecast_hour.is_none() {
        parsed.forecast_hour = tokens
            .get(date_index + 2)
            .and_then(|token| token.strip_prefix('f'))
            .and_then(|value| value.parse::<u16>().ok());
    }
    if parsed.domain_slug.is_none() {
        parsed.domain_slug = tokens.get(date_index + 3).map(|value| (*value).to_string());
    }
    parsed
}

fn format_run_title(manifest: &RunPublicationManifest, model_slug: Option<&str>) -> String {
    let model = model_slug.unwrap_or("unknown").replace('_', " ");
    let kind = match classify_manifest_kind(manifest) {
        ProofRunKind::Direct => "Direct Batch",
        ProofRunKind::Derived => "Derived Batch",
        ProofRunKind::Heavy => "Heavy Batch",
        ProofRunKind::Windowed => "Windowed Batch",
    };
    format!("{} {}", model.to_uppercase(), kind)
}

fn classify_manifest_kind(manifest: &RunPublicationManifest) -> ProofRunKind {
    let kind = manifest.run_kind.as_str();
    if kind.contains("windowed") {
        ProofRunKind::Windowed
    } else if kind.contains("derived") {
        ProofRunKind::Derived
    } else if kind.contains("ecape")
        || kind.contains("batch")
            && manifest
                .artifacts
                .iter()
                .any(|artifact| artifact.artifact_key == "severe_proof_panel")
    {
        ProofRunKind::Heavy
    } else {
        ProofRunKind::Direct
    }
}

fn infer_manifest_source(manifest: &RunPublicationManifest) -> Option<String> {
    let mut unique = manifest
        .input_fetches
        .iter()
        .map(|fetch| format!("{:?}", fetch.resolved_source))
        .collect::<Vec<_>>();
    unique.sort();
    unique.dedup();
    match unique.len() {
        0 => None,
        1 => unique.into_iter().next(),
        _ => Some("mixed".to_string()),
    }
}

fn materialize_artifact_path(
    output_root: &Path,
    artifact: &crate::publication::PublishedArtifactRecord,
) -> PathBuf {
    if artifact.relative_path.is_absolute() {
        artifact.relative_path.clone()
    } else {
        output_root.join(&artifact.relative_path)
    }
}

fn run_state_slug(state: RunPublicationState) -> &'static str {
    match state {
        RunPublicationState::Planned => "planned",
        RunPublicationState::Running => "running",
        RunPublicationState::Complete => "complete",
        RunPublicationState::Partial => "partial",
        RunPublicationState::Failed => "failed",
    }
}

fn artifact_state_slug(state: ArtifactPublicationState) -> &'static str {
    match state {
        ArtifactPublicationState::Planned => "planned",
        ArtifactPublicationState::Running => "running",
        ArtifactPublicationState::Complete => "complete",
        ArtifactPublicationState::Failed => "failed",
        ArtifactPublicationState::Blocked => "blocked",
        ArtifactPublicationState::CacheHit => "cache_hit",
    }
}

fn relative_href(from_dir: &Path, to_path: &Path) -> String {
    let base = from_dir.components().collect::<Vec<_>>();
    let target = to_path.components().collect::<Vec<_>>();
    let mut shared = 0usize;
    while shared < base.len() && shared < target.len() && base[shared] == target[shared] {
        shared += 1;
    }
    let mut parts = Vec::new();
    for _ in shared..base.len() {
        parts.push("..".to_string());
    }
    for component in target.iter().skip(shared) {
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

impl ProofGalleryRun {
    fn kind_sort_key(&self) -> u8 {
        match self.kind {
            ProofRunKind::Heavy => 0,
            ProofRunKind::Derived => 1,
            ProofRunKind::Direct => 2,
            ProofRunKind::Windowed => 3,
        }
    }

    fn kind_slug(&self) -> &'static str {
        match self.kind {
            ProofRunKind::Direct => "direct",
            ProofRunKind::Derived => "derived",
            ProofRunKind::Heavy => "heavy",
            ProofRunKind::Windowed => "windowed",
        }
    }
}
