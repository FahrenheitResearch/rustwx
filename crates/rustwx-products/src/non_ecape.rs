use crate::custom_poi::CustomPoiOverlay;
use crate::derived::{
    DerivedBatchRequest, HrrrDerivedBatchReport, PlannedDerivedSourceRoutes,
    is_heavy_derived_recipe_slug, maybe_load_special_pair_for_derived, plan_derived_recipes,
    plan_native_thermo_routes_with_surface_product, prepare_shared_derived_fields,
    run_model_derived_batch_from_loaded, run_model_derived_batch_from_loaded_with_precomputed,
    run_model_derived_batch_without_loaded,
};
use crate::direct::{
    DirectBatchRequest, FetchGroup, HrrrDirectBatchReport, run_direct_batch_from_loaded,
};
use crate::hrrr::{DomainSpec, resolve_hrrr_run};
use crate::orchestrator::{lane, run_fanout3};
use crate::places::PlaceLabelOverlay;
use crate::planner::ExecutionPlanBuilder;
use crate::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, PublishedFetchIdentity,
    RunPublicationManifest, artifact_identity_from_path, default_run_manifest_path,
    finalize_and_publish_run_manifest, publish_run_manifest_with_attempt,
};
use crate::publication_provenance::capture_default_build_provenance;
use crate::runtime::{BundleLoaderConfig, load_execution_plan};
use crate::severe::build_severe_execution_plan;
use crate::source::ProductSourceMode;
use crate::windowed::{
    HrrrWindowedBatchReport, HrrrWindowedBatchRequest, HrrrWindowedProduct,
    HrrrWindowedRenderedProduct, collect_windowed_input_fetches,
    run_hrrr_windowed_batch_with_context, windowed_product_input_fetch_keys,
};
use rustwx_core::{BundleRequirement, CanonicalBundleDescriptor, ModelId, SourceId};
use rustwx_models::{LatestRun, latest_available_run_at_forecast_hour, plot_recipe};
use rustwx_render::PngCompressionMode;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;

fn default_output_width() -> u32 {
    1200
}

fn default_output_height() -> u32 {
    900
}

fn default_png_compression() -> PngCompressionMode {
    PngCompressionMode::Default
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeHourRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub direct_recipe_slugs: Vec<String>,
    pub derived_recipe_slugs: Vec<String>,
    pub windowed_products: Vec<HrrrWindowedProduct>,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeMultiDomainRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domains: Vec<DomainSpec>,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub direct_recipe_slugs: Vec<String>,
    pub derived_recipe_slugs: Vec<String>,
    pub windowed_products: Vec<HrrrWindowedProduct>,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_jobs: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HrrrNonEcapeSharedTiming {
    pub resolve_run_ms: u128,
    pub shared_load_decode_ms: u128,
    pub shared_derived_prepare_ms: u128,
    pub total_prepare_ms: u128,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HrrrNonEcapeFanoutTiming {
    pub domain_context_build_ms: u128,
    pub domain_fanout_wall_ms: u128,
    pub domain_render_sum_ms: u128,
    pub domain_render_max_ms: u128,
    pub conus_wall_ms: u128,
    pub city_domains_sum_ms: u128,
    pub city_domains_max_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeHourRequestedProducts {
    pub direct_recipe_slugs: Vec<String>,
    pub derived_recipe_slugs: Vec<String>,
    pub windowed_products: Vec<HrrrWindowedProduct>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeHourSummary {
    pub runner_count: usize,
    pub direct_rendered_count: usize,
    pub derived_rendered_count: usize,
    pub windowed_rendered_count: usize,
    pub windowed_blocker_count: usize,
    pub output_count: usize,
    pub output_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeHourReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    /// Canonical (latest-attempt) run manifest path — stable across
    /// reruns and therefore clobberable.
    pub publication_manifest_path: PathBuf,
    /// Immutable attempt-stamped sibling manifest path. Always present
    /// on completed runs; paired with [`publication_manifest_path`] it
    /// forms the `(current truth, immutable attempt)` contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_manifest_path: Option<PathBuf>,
    pub requested: HrrrNonEcapeHourRequestedProducts,
    #[serde(default)]
    pub shared_timing: HrrrNonEcapeSharedTiming,
    pub summary: HrrrNonEcapeHourSummary,
    pub direct: Option<HrrrDirectBatchReport>,
    pub derived: Option<HrrrDerivedBatchReport>,
    pub windowed: Option<HrrrWindowedBatchReport>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeDomainReport {
    pub domain: DomainSpec,
    pub publication_manifest_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_manifest_path: Option<PathBuf>,
    pub summary: HrrrNonEcapeHourSummary,
    pub direct: Option<HrrrDirectBatchReport>,
    pub derived: Option<HrrrDerivedBatchReport>,
    pub windowed: Option<HrrrWindowedBatchReport>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrNonEcapeMultiDomainReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub requested: HrrrNonEcapeHourRequestedProducts,
    #[serde(default)]
    pub shared_timing: HrrrNonEcapeSharedTiming,
    #[serde(default)]
    pub fanout_timing: HrrrNonEcapeFanoutTiming,
    pub domains: Vec<HrrrNonEcapeDomainReport>,
    pub total_ms: u128,
}

pub type NonEcapeSharedTiming = HrrrNonEcapeSharedTiming;
pub type NonEcapeFanoutTiming = HrrrNonEcapeFanoutTiming;
pub type NonEcapeRequestedProducts = HrrrNonEcapeHourRequestedProducts;
pub type NonEcapeHourSummary = HrrrNonEcapeHourSummary;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonEcapeHourRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub direct_recipe_slugs: Vec<String>,
    pub derived_recipe_slugs: Vec<String>,
    #[serde(default)]
    pub direct_product_overrides: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_product_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pressure_product_override: Option<String>,
    #[serde(default)]
    pub allow_large_heavy_domain: bool,
    #[serde(default)]
    pub windowed_products: Vec<HrrrWindowedProduct>,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonEcapeMultiDomainRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domains: Vec<DomainSpec>,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub direct_recipe_slugs: Vec<String>,
    pub derived_recipe_slugs: Vec<String>,
    #[serde(default)]
    pub direct_product_overrides: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_product_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pressure_product_override: Option<String>,
    #[serde(default)]
    pub allow_large_heavy_domain: bool,
    #[serde(default)]
    pub windowed_products: Vec<HrrrWindowedProduct>,
    #[serde(default = "default_output_width")]
    pub output_width: u32,
    #[serde(default = "default_output_height")]
    pub output_height: u32,
    #[serde(default = "default_png_compression")]
    pub png_compression: PngCompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_poi_overlay: Option<CustomPoiOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub place_label_overlay: Option<PlaceLabelOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_jobs: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonEcapeHourReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub publication_manifest_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_manifest_path: Option<PathBuf>,
    pub requested: NonEcapeRequestedProducts,
    #[serde(default)]
    pub shared_timing: NonEcapeSharedTiming,
    pub summary: NonEcapeHourSummary,
    pub direct: Option<HrrrDirectBatchReport>,
    pub derived: Option<HrrrDerivedBatchReport>,
    pub windowed: Option<HrrrWindowedBatchReport>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonEcapeDomainReport {
    pub domain: DomainSpec,
    pub publication_manifest_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_manifest_path: Option<PathBuf>,
    pub summary: NonEcapeHourSummary,
    pub direct: Option<HrrrDirectBatchReport>,
    pub derived: Option<HrrrDerivedBatchReport>,
    pub windowed: Option<HrrrWindowedBatchReport>,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonEcapeMultiDomainReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default)]
    pub source_mode: ProductSourceMode,
    pub requested: NonEcapeRequestedProducts,
    #[serde(default)]
    pub shared_timing: NonEcapeSharedTiming,
    #[serde(default)]
    pub fanout_timing: NonEcapeFanoutTiming,
    pub domains: Vec<NonEcapeDomainReport>,
    pub total_ms: u128,
}

struct PreparedNonEcapeHour {
    normalized: NonEcapeRequestedProducts,
    latest: LatestRun,
    derived_recipes: Vec<crate::derived::DerivedRecipe>,
    precomputed_derived: Option<crate::derived::PreparedSharedDerivedFields>,
    direct_loaded: Option<Arc<crate::runtime::LoadedBundleSet>>,
    derived_loaded: Option<Arc<crate::runtime::LoadedBundleSet>>,
    timing: NonEcapeSharedTiming,
}

fn non_ecape_request_from_hrrr(request: &HrrrNonEcapeHourRequest) -> NonEcapeHourRequest {
    NonEcapeHourRequest {
        model: ModelId::Hrrr,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_override_utc: request.cycle_override_utc,
        forecast_hour: request.forecast_hour,
        source: request.source,
        domain: request.domain.clone(),
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        source_mode: request.source_mode,
        direct_recipe_slugs: request.direct_recipe_slugs.clone(),
        derived_recipe_slugs: request.derived_recipe_slugs.clone(),
        direct_product_overrides: HashMap::new(),
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: false,
        windowed_products: request.windowed_products.clone(),
        output_width: request.output_width,
        output_height: request.output_height,
        png_compression: request.png_compression,
        custom_poi_overlay: request.custom_poi_overlay.clone(),
        place_label_overlay: request.place_label_overlay.clone(),
    }
}

fn non_ecape_multi_request_from_hrrr(
    request: &HrrrNonEcapeMultiDomainRequest,
) -> NonEcapeMultiDomainRequest {
    NonEcapeMultiDomainRequest {
        model: ModelId::Hrrr,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_override_utc: request.cycle_override_utc,
        forecast_hour: request.forecast_hour,
        source: request.source,
        domains: request.domains.clone(),
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        source_mode: request.source_mode,
        direct_recipe_slugs: request.direct_recipe_slugs.clone(),
        derived_recipe_slugs: request.derived_recipe_slugs.clone(),
        direct_product_overrides: HashMap::new(),
        surface_product_override: None,
        pressure_product_override: None,
        allow_large_heavy_domain: false,
        windowed_products: request.windowed_products.clone(),
        output_width: request.output_width,
        output_height: request.output_height,
        png_compression: request.png_compression,
        custom_poi_overlay: request.custom_poi_overlay.clone(),
        place_label_overlay: request.place_label_overlay.clone(),
        domain_jobs: request.domain_jobs,
    }
}

fn hrrr_hour_report_from_generic(report: NonEcapeHourReport) -> HrrrNonEcapeHourReport {
    HrrrNonEcapeHourReport {
        date_yyyymmdd: report.date_yyyymmdd,
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source,
        domain: report.domain,
        out_dir: report.out_dir,
        cache_root: report.cache_root,
        use_cache: report.use_cache,
        source_mode: report.source_mode,
        publication_manifest_path: report.publication_manifest_path,
        attempt_manifest_path: report.attempt_manifest_path,
        requested: report.requested,
        shared_timing: report.shared_timing,
        summary: report.summary,
        direct: report.direct,
        derived: report.derived,
        windowed: report.windowed,
        total_ms: report.total_ms,
    }
}

fn hrrr_multi_domain_report_from_generic(
    report: NonEcapeMultiDomainReport,
) -> HrrrNonEcapeMultiDomainReport {
    HrrrNonEcapeMultiDomainReport {
        date_yyyymmdd: report.date_yyyymmdd,
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source,
        out_dir: report.out_dir,
        cache_root: report.cache_root,
        use_cache: report.use_cache,
        source_mode: report.source_mode,
        requested: report.requested,
        shared_timing: report.shared_timing,
        fanout_timing: report.fanout_timing,
        domains: report
            .domains
            .into_iter()
            .map(|domain| HrrrNonEcapeDomainReport {
                domain: domain.domain,
                publication_manifest_path: domain.publication_manifest_path,
                attempt_manifest_path: domain.attempt_manifest_path,
                summary: domain.summary,
                direct: domain.direct,
                derived: domain.derived,
                windowed: domain.windowed,
                total_ms: domain.total_ms,
            })
            .collect(),
        total_ms: report.total_ms,
    }
}

pub fn run_model_non_ecape_hour(
    request: &NonEcapeHourRequest,
) -> Result<NonEcapeHourReport, Box<dyn std::error::Error>> {
    let multi_request = NonEcapeMultiDomainRequest {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_override_utc: request.cycle_override_utc,
        forecast_hour: request.forecast_hour,
        source: request.source,
        domains: vec![request.domain.clone()],
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        source_mode: request.source_mode,
        direct_recipe_slugs: request.direct_recipe_slugs.clone(),
        derived_recipe_slugs: request.derived_recipe_slugs.clone(),
        direct_product_overrides: request.direct_product_overrides.clone(),
        surface_product_override: request.surface_product_override.clone(),
        pressure_product_override: request.pressure_product_override.clone(),
        allow_large_heavy_domain: request.allow_large_heavy_domain,
        windowed_products: request.windowed_products.clone(),
        output_width: request.output_width,
        output_height: request.output_height,
        png_compression: request.png_compression,
        custom_poi_overlay: request.custom_poi_overlay.clone(),
        place_label_overlay: request.place_label_overlay.clone(),
        domain_jobs: None,
    };
    let report = run_model_non_ecape_hour_multi_domain(&multi_request)?;
    let domain_report = report
        .domains
        .into_iter()
        .next()
        .ok_or("multi-domain runner returned no domain reports for single-domain request")?;
    Ok(NonEcapeHourReport {
        model: report.model,
        date_yyyymmdd: report.date_yyyymmdd,
        cycle_utc: report.cycle_utc,
        forecast_hour: report.forecast_hour,
        source: report.source,
        domain: domain_report.domain,
        out_dir: report.out_dir,
        cache_root: report.cache_root,
        use_cache: report.use_cache,
        source_mode: report.source_mode,
        publication_manifest_path: domain_report.publication_manifest_path,
        attempt_manifest_path: domain_report.attempt_manifest_path,
        requested: report.requested,
        shared_timing: report.shared_timing,
        summary: domain_report.summary,
        direct: domain_report.direct,
        derived: domain_report.derived,
        windowed: domain_report.windowed,
        total_ms: domain_report.total_ms,
    })
}

pub fn run_hrrr_non_ecape_hour(
    request: &HrrrNonEcapeHourRequest,
) -> Result<HrrrNonEcapeHourReport, Box<dyn std::error::Error>> {
    let report = run_model_non_ecape_hour(&non_ecape_request_from_hrrr(request))?;
    Ok(hrrr_hour_report_from_generic(report))
}

pub fn run_model_non_ecape_hour_multi_domain(
    request: &NonEcapeMultiDomainRequest,
) -> Result<NonEcapeMultiDomainReport, Box<dyn std::error::Error>> {
    validate_requested_domains(&request.domains)?;
    let total_start = Instant::now();
    let prepared = prepare_non_ecape_hour(request)?;
    let worker_count = domain_worker_count(request.domain_jobs, request.domains.len());
    let domain_context_build_ms = 0;
    let domain_fanout_start = Instant::now();
    let mut domain_reports = Vec::with_capacity(request.domains.len());
    if worker_count <= 1 || request.domains.len() <= 1 {
        for domain in &request.domains {
            domain_reports.push(run_prepared_non_ecape_domain(request, &prepared, domain)?);
        }
    } else {
        let queue = Arc::new(Mutex::new(
            (0..request.domains.len()).collect::<VecDeque<usize>>(),
        ));
        let (tx, rx) = mpsc::channel::<(usize, Result<NonEcapeDomainReport, String>)>();
        let mut ordered = vec![None; request.domains.len()];
        let request_ref = request;
        let prepared_ref = &prepared;
        let domains_ref = &request.domains;
        thread::scope(|scope| {
            for _ in 0..worker_count {
                let queue = Arc::clone(&queue);
                let tx = tx.clone();
                let request_ref = request_ref;
                let prepared_ref = prepared_ref;
                let domains_ref = domains_ref;
                scope.spawn(move || {
                    loop {
                        let next = {
                            let mut queue = queue.lock().expect("domain queue poisoned");
                            queue.pop_front()
                        };
                        let Some(index) = next else {
                            break;
                        };
                        let result = run_prepared_non_ecape_domain(
                            request_ref,
                            prepared_ref,
                            &domains_ref[index],
                        )
                        .map_err(|err| err.to_string());
                        if tx.send((index, result)).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(tx);
            for (index, result) in rx {
                ordered[index] = Some(result);
            }
        });
        for result in ordered {
            let report = result
                .ok_or("domain worker dropped a result")?
                .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
            domain_reports.push(report);
        }
    }
    let domain_fanout_wall_ms = domain_fanout_start.elapsed().as_millis();
    let domain_render_sum_ms = domain_reports.iter().map(|report| report.total_ms).sum();
    let domain_render_max_ms = domain_reports
        .iter()
        .map(|report| report.total_ms)
        .max()
        .unwrap_or(0);
    let conus_wall_ms = domain_reports
        .iter()
        .find(|report| report.domain.slug == "conus")
        .map(|report| report.total_ms)
        .unwrap_or(0);
    let city_domains_sum_ms = domain_reports
        .iter()
        .filter(|report| report.domain.slug != "conus")
        .map(|report| report.total_ms)
        .sum();
    let city_domains_max_ms = domain_reports
        .iter()
        .filter(|report| report.domain.slug != "conus")
        .map(|report| report.total_ms)
        .max()
        .unwrap_or(0);
    Ok(NonEcapeMultiDomainReport {
        model: request.model,
        date_yyyymmdd: prepared.latest.cycle.date_yyyymmdd.clone(),
        cycle_utc: prepared.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: prepared.latest.source,
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        source_mode: request.source_mode,
        requested: prepared.normalized.clone(),
        shared_timing: prepared.timing.clone(),
        fanout_timing: HrrrNonEcapeFanoutTiming {
            domain_context_build_ms,
            domain_fanout_wall_ms,
            domain_render_sum_ms,
            domain_render_max_ms,
            conus_wall_ms,
            city_domains_sum_ms,
            city_domains_max_ms,
        },
        domains: domain_reports,
        total_ms: total_start.elapsed().as_millis(),
    })
}

pub fn run_hrrr_non_ecape_hour_multi_domain(
    request: &HrrrNonEcapeMultiDomainRequest,
) -> Result<HrrrNonEcapeMultiDomainReport, Box<dyn std::error::Error>> {
    let report =
        run_model_non_ecape_hour_multi_domain(&non_ecape_multi_request_from_hrrr(request))?;
    Ok(hrrr_multi_domain_report_from_generic(report))
}

fn resolve_model_run(
    model: ModelId,
    date: &str,
    cycle_override: Option<u8>,
    forecast_hour: u16,
    source: SourceId,
) -> Result<LatestRun, Box<dyn std::error::Error>> {
    if model == ModelId::Hrrr {
        return resolve_hrrr_run(date, cycle_override, forecast_hour, source);
    }
    match cycle_override {
        Some(hour) => Ok(LatestRun {
            model,
            cycle: rustwx_core::CycleSpec::new(date, hour)?,
            source,
        }),
        None => Ok(latest_available_run_at_forecast_hour(
            model,
            Some(source),
            date,
            forecast_hour,
        )?),
    }
}

fn prepare_non_ecape_hour(
    request: &NonEcapeMultiDomainRequest,
) -> Result<PreparedNonEcapeHour, Box<dyn std::error::Error>> {
    let total_prepare_start = Instant::now();
    let normalized = normalize_requested_products_from_parts(
        request.model,
        &request.direct_recipe_slugs,
        &request.derived_recipe_slugs,
        &request.windowed_products,
    );
    validate_requested_work(request.model, &normalized)?;

    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let resolve_start = Instant::now();
    let latest = resolve_model_run(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
    )?;
    let resolve_run_ms = resolve_start.elapsed().as_millis();
    let pinned_date = latest.cycle.date_yyyymmdd.clone();
    let pinned_cycle = Some(latest.cycle.hour_utc);
    let pinned_source = latest.source;
    let planning_domain = request
        .domains
        .first()
        .cloned()
        .ok_or("multi-domain HRRR hour runner needs at least one domain")?;

    let direct_groups = if normalized.direct_recipe_slugs.is_empty() {
        Vec::new()
    } else {
        let direct_request = DirectBatchRequest {
            model: request.model,
            date_yyyymmdd: pinned_date,
            cycle_override_utc: pinned_cycle,
            forecast_hour: request.forecast_hour,
            source: pinned_source,
            domain: planning_domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: normalized.direct_recipe_slugs.clone(),
            product_overrides: request.direct_product_overrides.clone(),
            contour_mode: crate::derived::NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
            custom_poi_overlay: request.custom_poi_overlay.clone(),
            place_label_overlay: request.place_label_overlay.clone(),
        };
        crate::direct::plan_direct_fetch_groups(&direct_request)?
    };
    let derived_recipes = if normalized.derived_recipe_slugs.is_empty() {
        Vec::new()
    } else {
        plan_derived_recipes(&normalized.derived_recipe_slugs)?
    };
    let derived_routes = if derived_recipes.is_empty() {
        None
    } else {
        Some(plan_native_thermo_routes_with_surface_product(
            request.model,
            &derived_recipes,
            request.source_mode,
            request.surface_product_override.as_deref(),
        )?)
    };

    let derived_request = (!derived_recipes.is_empty()).then(|| DerivedBatchRequest {
        model: request.model,
        date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
        cycle_override_utc: Some(latest.cycle.hour_utc),
        forecast_hour: request.forecast_hour,
        source: latest.source,
        domain: planning_domain.clone(),
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        recipe_slugs: normalized.derived_recipe_slugs.clone(),
        surface_product_override: request.surface_product_override.clone(),
        pressure_product_override: request.pressure_product_override.clone(),
        source_mode: request.source_mode,
        allow_large_heavy_domain: request.allow_large_heavy_domain,
        contour_mode: crate::derived::NativeContourRenderMode::Automatic,
        native_fill_level_multiplier: 1,
        output_width: request.output_width,
        output_height: request.output_height,
        png_compression: request.png_compression,
        custom_poi_overlay: request.custom_poi_overlay.clone(),
        place_label_overlay: request.place_label_overlay.clone(),
    });

    let mut shared_load_decode_ms = 0u128;
    let mut derived_loaded_override: Option<Arc<crate::runtime::LoadedBundleSet>> = None;
    if let (Some(derived_request), Some(routes)) =
        (derived_request.as_ref(), derived_routes.as_ref())
    {
        let special_load_start = Instant::now();
        if let Some(loaded) = maybe_load_special_pair_for_derived(derived_request, &latest, routes)?
        {
            shared_load_decode_ms += special_load_start.elapsed().as_millis();
            derived_loaded_override = Some(Arc::new(loaded));
        }
    }

    let mut main_loaded: Option<Arc<crate::runtime::LoadedBundleSet>> = None;
    let mut direct_loaded: Option<Arc<crate::runtime::LoadedBundleSet>> = None;

    let build_shared_loaded = request.model == ModelId::Hrrr || derived_loaded_override.is_none();
    if build_shared_loaded {
        let plan = build_shared_non_ecape_execution_plan(
            &latest,
            request.forecast_hour,
            &direct_groups,
            derived_routes.as_ref(),
            derived_loaded_override.is_none(),
            request.surface_product_override.as_deref(),
            request.pressure_product_override.as_deref(),
        );
        let load_start = Instant::now();
        main_loaded = if plan.bundles.is_empty() {
            None
        } else {
            Some(Arc::new(load_execution_plan(
                plan,
                &BundleLoaderConfig {
                    cache_root: request.cache_root.clone(),
                    use_cache: request.use_cache,
                },
            )?))
        };
        shared_load_decode_ms += load_start.elapsed().as_millis();
        direct_loaded = main_loaded.clone();
    } else {
        if !direct_groups.is_empty() {
            let mut direct_plan_builder = ExecutionPlanBuilder::new(&latest, request.forecast_hour);
            for group in &direct_groups {
                let requirement = rustwx_core::BundleRequirement::new(
                    rustwx_core::CanonicalBundleDescriptor::NativeAnalysis,
                    request.forecast_hour,
                )
                .with_native_override(group.product.clone());
                for alias in &group.planned_family_aliases {
                    direct_plan_builder.require_with_logical_family(&requirement, Some(alias));
                }
            }
            let direct_plan = direct_plan_builder.build();
            let direct_load_start = Instant::now();
            direct_loaded = if direct_plan.bundles.is_empty() {
                None
            } else {
                Some(Arc::new(load_execution_plan(
                    direct_plan,
                    &BundleLoaderConfig {
                        cache_root: request.cache_root.clone(),
                        use_cache: request.use_cache,
                    },
                )?))
            };
            shared_load_decode_ms += direct_load_start.elapsed().as_millis();
        }
    }

    let derived_loaded = if derived_recipes.is_empty() {
        None
    } else if let Some(loaded) = derived_loaded_override {
        Some(loaded)
    } else {
        main_loaded.clone()
    };

    let shared_derived_prepare_start = Instant::now();
    let precomputed_derived = if derived_recipes.is_empty() || request.domains.len() <= 1 {
        None
    } else if let (Some(derived_request), Some(derived_loaded_ref)) =
        (derived_request.as_ref(), derived_loaded.as_ref())
    {
        prepare_shared_derived_fields(derived_request, &derived_recipes, derived_loaded_ref)?
    } else {
        None
    };
    let shared_derived_prepare_ms = shared_derived_prepare_start.elapsed().as_millis();

    Ok(PreparedNonEcapeHour {
        normalized,
        latest,
        derived_recipes,
        precomputed_derived,
        direct_loaded,
        derived_loaded,
        timing: HrrrNonEcapeSharedTiming {
            resolve_run_ms,
            shared_load_decode_ms,
            shared_derived_prepare_ms,
            total_prepare_ms: total_prepare_start.elapsed().as_millis(),
        },
    })
}

fn build_shared_non_ecape_execution_plan(
    latest: &LatestRun,
    forecast_hour: u16,
    direct_groups: &[FetchGroup],
    derived_routes: Option<&PlannedDerivedSourceRoutes>,
    include_pair_compute: bool,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
) -> crate::planner::ExecutionPlan {
    let mut plan_builder = ExecutionPlanBuilder::new(latest, forecast_hour);
    if include_pair_compute
        && derived_routes
            .map(|routes| !routes.compute_recipes.is_empty())
            .unwrap_or(false)
    {
        add_pair_requirements(
            &mut plan_builder,
            latest,
            forecast_hour,
            surface_product_override,
            pressure_product_override,
        );
    }
    if let Some(routes) = derived_routes {
        add_native_route_requirements(&mut plan_builder, forecast_hour, routes);
    }
    add_direct_fetch_group_requirements(&mut plan_builder, forecast_hour, direct_groups);
    plan_builder.build()
}

fn add_pair_requirements(
    plan_builder: &mut ExecutionPlanBuilder,
    latest: &LatestRun,
    forecast_hour: u16,
    surface_product_override: Option<&str>,
    pressure_product_override: Option<&str>,
) {
    let pair_plan = build_severe_execution_plan(
        latest,
        forecast_hour,
        surface_product_override,
        pressure_product_override,
    );
    for bundle in &pair_plan.bundles {
        for alias in &bundle.aliases {
            let mut requirement =
                rustwx_core::BundleRequirement::new(alias.bundle, bundle.id.forecast_hour);
            if let Some(ref over) = alias.native_override {
                requirement = requirement.with_native_override(over.clone());
            }
            plan_builder.require_with_logical_family(&requirement, alias.logical_family.as_deref());
        }
    }
}

fn add_native_route_requirements(
    plan_builder: &mut ExecutionPlanBuilder,
    forecast_hour: u16,
    routes: &PlannedDerivedSourceRoutes,
) {
    let mut native_products = std::collections::BTreeSet::<String>::new();
    for route in &routes.native_routes {
        if native_products.insert(route.candidate.fetch_product.to_string()) {
            let requirement =
                BundleRequirement::new(CanonicalBundleDescriptor::NativeAnalysis, forecast_hour)
                    .with_native_override(route.candidate.fetch_product);
            plan_builder.require_with_logical_family(
                &requirement,
                Some(&format!("thermo-native:{}", route.candidate.fetch_product)),
            );
        }
    }
}

fn add_direct_fetch_group_requirements(
    plan_builder: &mut ExecutionPlanBuilder,
    forecast_hour: u16,
    direct_groups: &[FetchGroup],
) {
    for group in direct_groups {
        let requirement =
            BundleRequirement::new(CanonicalBundleDescriptor::NativeAnalysis, forecast_hour)
                .with_native_override(group.product.clone());
        for alias in &group.planned_family_aliases {
            plan_builder.require_with_logical_family(&requirement, Some(alias));
        }
    }
}

fn run_prepared_non_ecape_domain(
    request: &NonEcapeMultiDomainRequest,
    prepared: &PreparedNonEcapeHour,
    domain: &DomainSpec,
) -> Result<NonEcapeDomainReport, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    let domain_out_dir = request.out_dir.join(&domain.slug);
    fs::create_dir_all(&domain_out_dir)?;
    let pinned_date = prepared.latest.cycle.date_yyyymmdd.clone();
    let pinned_cycle = Some(prepared.latest.cycle.hour_utc);
    let pinned_source = prepared.latest.source;
    let pinned_cycle_utc = prepared.latest.cycle.hour_utc;
    let run_slug = format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_non_ecape_hour",
        request.model.as_str().replace('-', "_"),
        pinned_date,
        pinned_cycle_utc,
        request.forecast_hour,
        domain.slug
    );
    let manifest_path = default_run_manifest_path(&domain_out_dir, &run_slug);
    let mut manifest = build_run_manifest(
        request.model,
        &prepared.normalized,
        &domain_out_dir,
        &run_slug,
        &pinned_date,
        pinned_cycle_utc,
        request.forecast_hour,
        &domain.slug,
    );
    manifest.build_provenance = Some(capture_default_build_provenance());
    manifest.mark_running();
    crate::publication::publish_run_manifest(&manifest_path, &manifest)?;

    let direct_loaded_ref = prepared.direct_loaded.as_deref();
    let derived_loaded_ref = prepared.derived_loaded.as_deref();
    let direct_request =
        (!prepared.normalized.direct_recipe_slugs.is_empty()).then(|| DirectBatchRequest {
            model: request.model,
            date_yyyymmdd: pinned_date.clone(),
            cycle_override_utc: pinned_cycle,
            forecast_hour: request.forecast_hour,
            source: pinned_source,
            domain: domain.clone(),
            out_dir: domain_out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: prepared.normalized.direct_recipe_slugs.clone(),
            product_overrides: request.direct_product_overrides.clone(),
            contour_mode: crate::derived::NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: request.output_width,
            output_height: request.output_height,
            png_compression: request.png_compression,
            custom_poi_overlay: request.custom_poi_overlay.clone(),
            place_label_overlay: request.place_label_overlay.clone(),
        });

    let derived_request = (!prepared.normalized.derived_recipe_slugs.is_empty()).then(|| {
        (
            DerivedBatchRequest {
                model: request.model,
                date_yyyymmdd: pinned_date.clone(),
                cycle_override_utc: pinned_cycle,
                forecast_hour: request.forecast_hour,
                source: pinned_source,
                domain: domain.clone(),
                out_dir: domain_out_dir.clone(),
                cache_root: request.cache_root.clone(),
                use_cache: request.use_cache,
                recipe_slugs: prepared.normalized.derived_recipe_slugs.clone(),
                surface_product_override: request.surface_product_override.clone(),
                pressure_product_override: request.pressure_product_override.clone(),
                source_mode: request.source_mode,
                allow_large_heavy_domain: request.allow_large_heavy_domain,
                contour_mode: crate::derived::NativeContourRenderMode::Automatic,
                native_fill_level_multiplier: 1,
                output_width: request.output_width,
                output_height: request.output_height,
                png_compression: request.png_compression,
                custom_poi_overlay: request.custom_poi_overlay.clone(),
                place_label_overlay: request.place_label_overlay.clone(),
            },
            prepared.derived_recipes.clone(),
        )
    });
    let derived_latest = prepared.latest.clone();
    let precomputed_derived = prepared.precomputed_derived.as_ref();

    let windowed_request = (request.model == ModelId::Hrrr
        && !prepared.normalized.windowed_products.is_empty())
    .then(|| HrrrWindowedBatchRequest {
        date_yyyymmdd: pinned_date.clone(),
        cycle_override_utc: pinned_cycle,
        forecast_hour: request.forecast_hour,
        source: pinned_source,
        domain: domain.clone(),
        out_dir: domain_out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        products: prepared.normalized.windowed_products.clone(),
        output_width: request.output_width,
        output_height: request.output_height,
        png_compression: request.png_compression,
        place_label_overlay: request.place_label_overlay.clone(),
    });

    let lane_result = run_fanout3(
        should_run_lanes_concurrently(request.model, pinned_source),
        direct_request.as_ref().map(|lane_request| {
            lane("direct", move || {
                run_direct_batch_from_loaded(
                    lane_request,
                    direct_loaded_ref.expect("planner must load bundles when direct is requested"),
                    &lane_request.cache_root,
                    lane_request.use_cache,
                    None,
                )
            })
        }),
        derived_request.as_ref().map(|(lane_request, recipes)| {
            lane("derived", move || {
                if let Some(loaded) = derived_loaded_ref {
                    if let Some(precomputed) = precomputed_derived {
                        run_model_derived_batch_from_loaded_with_precomputed(
                            lane_request,
                            recipes,
                            loaded,
                            precomputed,
                        )
                    } else {
                        run_model_derived_batch_from_loaded(lane_request, recipes, loaded)
                    }
                } else {
                    run_model_derived_batch_without_loaded(lane_request, recipes, &derived_latest)
                }
            })
        }),
        windowed_request.as_ref().map(|lane_request| {
            let windowed_latest = prepared.latest.clone();
            lane("windowed", move || {
                run_hrrr_windowed_batch_with_context(lane_request, &windowed_latest)
            })
        }),
    );

    let (direct, derived, windowed) = match lane_result {
        Ok(reports) => reports,
        Err(err) => {
            manifest.mark_failed(err.to_string());
            let _ = publish_run_manifest_with_attempt(
                &manifest_path,
                &domain_out_dir,
                &run_slug,
                &manifest,
            );
            return Err(err);
        }
    };

    let summary = build_summary(&direct, &derived, &windowed);
    manifest.input_fetches = collect_input_fetches(&direct, &derived, &windowed);
    apply_direct_manifest_updates(&mut manifest, &direct);
    apply_derived_manifest_updates(&mut manifest, &derived);
    apply_windowed_manifest_updates(&mut manifest, &windowed);
    let (canonical_manifest_path, attempt_manifest_path) =
        finalize_and_publish_run_manifest(&mut manifest, &domain_out_dir, &run_slug)?;

    Ok(NonEcapeDomainReport {
        domain: domain.clone(),
        publication_manifest_path: canonical_manifest_path,
        attempt_manifest_path: Some(attempt_manifest_path),
        summary,
        direct,
        derived,
        windowed,
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn validate_requested_work(
    model: ModelId,
    request: &NonEcapeRequestedProducts,
) -> Result<(), Box<dyn std::error::Error>> {
    if request.direct_recipe_slugs.is_empty()
        && request.derived_recipe_slugs.is_empty()
        && request.windowed_products.is_empty()
    {
        return Err(
            "unified non-ECAPE hour runner needs at least one direct recipe, derived recipe, or windowed product"
                .into(),
        );
    }
    if model != ModelId::Hrrr && !request.windowed_products.is_empty() {
        return Err(format!(
            "windowed products are only supported by the HRRR non-ECAPE runner, not {}",
            model
        )
        .into());
    }
    if let Some(heavy_slug) = request
        .derived_recipe_slugs
        .iter()
        .find(|slug| is_heavy_derived_recipe_slug(slug))
    {
        return Err(format!(
            "derived recipe '{}' is a heavy ECAPE product; use derived_batch or a heavy runner instead of non_ecape_hour",
            heavy_slug
        )
        .into());
    }
    Ok(())
}

fn validate_requested_domains(domains: &[DomainSpec]) -> Result<(), Box<dyn std::error::Error>> {
    if domains.is_empty() {
        return Err("multi-domain non-ECAPE hour runner needs at least one domain".into());
    }
    let mut seen = HashSet::<&str>::new();
    for domain in domains {
        if !seen.insert(domain.slug.as_str()) {
            return Err(format!("duplicate multi-domain slug '{}'", domain.slug).into());
        }
    }
    Ok(())
}

#[cfg(test)]
fn normalize_requested_products(
    request: &HrrrNonEcapeHourRequest,
) -> HrrrNonEcapeHourRequestedProducts {
    normalize_requested_products_from_parts(
        ModelId::Hrrr,
        &request.direct_recipe_slugs,
        &request.derived_recipe_slugs,
        &request.windowed_products,
    )
}

fn normalize_requested_products_from_parts(
    model: ModelId,
    direct_recipe_slugs: &[String],
    derived_recipe_slugs: &[String],
    windowed_products: &[HrrrWindowedProduct],
) -> HrrrNonEcapeHourRequestedProducts {
    let mut normalized_direct_recipe_slugs = Vec::new();
    let mut normalized_windowed_products = windowed_products.to_vec();

    for slug in direct_recipe_slugs {
        let normalized_slug = plot_recipe(slug)
            .map(|recipe| recipe.slug)
            .unwrap_or(slug.as_str());
        if model == ModelId::Hrrr && normalized_slug == "1h_qpf" {
            if !normalized_windowed_products.contains(&HrrrWindowedProduct::Qpf1h) {
                normalized_windowed_products.push(HrrrWindowedProduct::Qpf1h);
            }
            continue;
        }
        normalized_direct_recipe_slugs.push(slug.clone());
    }

    HrrrNonEcapeHourRequestedProducts {
        direct_recipe_slugs: normalized_direct_recipe_slugs,
        derived_recipe_slugs: derived_recipe_slugs.to_vec(),
        windowed_products: normalized_windowed_products,
    }
}

fn should_run_lanes_concurrently(model: ModelId, source: SourceId) -> bool {
    matches!(model, ModelId::Hrrr | ModelId::WrfGdex) && !matches!(source, SourceId::Nomads)
}

fn domain_worker_count(requested_jobs: Option<usize>, domain_count: usize) -> usize {
    if domain_count <= 1 {
        return 1;
    }

    let env_override = std::env::var("RUSTWX_DOMAIN_JOBS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0);
    let requested = requested_jobs.or(env_override).filter(|&value| value > 0);
    let default_jobs = 1;
    requested.unwrap_or(default_jobs).clamp(1, domain_count)
}

fn build_summary(
    direct: &Option<HrrrDirectBatchReport>,
    derived: &Option<HrrrDerivedBatchReport>,
    windowed: &Option<HrrrWindowedBatchReport>,
) -> HrrrNonEcapeHourSummary {
    let mut output_paths = Vec::new();
    let mut runner_count = 0usize;
    let mut direct_rendered_count = 0usize;
    let mut derived_rendered_count = 0usize;
    let mut windowed_rendered_count = 0usize;
    let mut windowed_blocker_count = 0usize;

    if let Some(report) = direct {
        runner_count += 1;
        direct_rendered_count = report.recipes.len();
        output_paths.extend(
            report
                .recipes
                .iter()
                .map(|recipe| recipe.output_path.clone()),
        );
    }

    if let Some(report) = derived {
        runner_count += 1;
        derived_rendered_count = report.recipes.len();
        output_paths.extend(
            report
                .recipes
                .iter()
                .map(|recipe| recipe.output_path.clone()),
        );
    }

    if let Some(report) = windowed {
        runner_count += 1;
        windowed_rendered_count = report.products.len();
        windowed_blocker_count = report.blockers.len();
        output_paths.extend(
            report
                .products
                .iter()
                .map(|product| product.output_path.clone()),
        );
    }

    HrrrNonEcapeHourSummary {
        runner_count,
        direct_rendered_count,
        derived_rendered_count,
        windowed_rendered_count,
        windowed_blocker_count,
        output_count: output_paths.len(),
        output_paths,
    }
}

fn build_run_manifest(
    model: ModelId,
    request: &NonEcapeRequestedProducts,
    out_dir: &std::path::Path,
    run_slug: &str,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    domain_slug: &str,
) -> RunPublicationManifest {
    let mut seen = HashSet::new();
    let mut artifacts = Vec::new();

    for slug in &request.direct_recipe_slugs {
        let key = direct_artifact_key(slug);
        if seen.insert(key.clone()) {
            artifacts.push(PublishedArtifactRecord::planned(
                key,
                expected_output_relative_path(
                    model,
                    date_yyyymmdd,
                    cycle_utc,
                    forecast_hour,
                    domain_slug,
                    slug,
                ),
            ));
        }
    }

    for slug in &request.derived_recipe_slugs {
        let key = derived_artifact_key(slug);
        if seen.insert(key.clone()) {
            artifacts.push(PublishedArtifactRecord::planned(
                key,
                expected_output_relative_path(
                    model,
                    date_yyyymmdd,
                    cycle_utc,
                    forecast_hour,
                    domain_slug,
                    slug,
                ),
            ));
        }
    }

    for product in &request.windowed_products {
        let slug = product.slug();
        let key = windowed_artifact_key(slug);
        if seen.insert(key.clone()) {
            artifacts.push(PublishedArtifactRecord::planned(
                key,
                expected_output_relative_path(
                    model,
                    date_yyyymmdd,
                    cycle_utc,
                    forecast_hour,
                    domain_slug,
                    slug,
                ),
            ));
        }
    }

    let runner_name = if model == ModelId::Hrrr {
        "hrrr_non_ecape_hour".to_string()
    } else {
        format!("{}_non_ecape_hour", model.as_str().replace('-', "_"))
    };
    RunPublicationManifest::new(&runner_name, run_slug.to_string(), out_dir.to_path_buf())
        .with_artifacts(artifacts)
}

fn expected_output_relative_path(
    model: ModelId,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    domain_slug: &str,
    product_slug: &str,
) -> PathBuf {
    PathBuf::from(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_{}.png",
        model.as_str().replace('-', "_"),
        date_yyyymmdd,
        cycle_utc,
        forecast_hour,
        domain_slug,
        product_slug
    ))
}

fn direct_artifact_key(slug: &str) -> String {
    format!("direct:{slug}")
}

fn derived_artifact_key(slug: &str) -> String {
    format!("derived:{slug}")
}

fn windowed_artifact_key(slug: &str) -> String {
    format!("windowed:{slug}")
}

fn apply_direct_manifest_updates(
    manifest: &mut RunPublicationManifest,
    direct: &Option<HrrrDirectBatchReport>,
) {
    let Some(report) = direct else {
        return;
    };
    for recipe in &report.recipes {
        manifest.update_artifact_state(
            &direct_artifact_key(&recipe.recipe_slug),
            ArtifactPublicationState::Complete,
            Some(format!(
                "source_route={} planned_family={} fetched_family={} resolved_source={} resolved_url={}",
                recipe.source_route.as_str(),
                recipe.grib_product,
                recipe.fetched_grib_product,
                recipe.resolved_source,
                recipe.resolved_url
            )),
        );
        manifest.update_artifact_identity(
            &direct_artifact_key(&recipe.recipe_slug),
            recipe.content_identity.clone(),
        );
        manifest.update_artifact_input_fetch_keys(
            &direct_artifact_key(&recipe.recipe_slug),
            recipe.input_fetch_keys.clone(),
        );
    }
}

fn apply_derived_manifest_updates(
    manifest: &mut RunPublicationManifest,
    derived: &Option<HrrrDerivedBatchReport>,
) {
    let Some(report) = derived else {
        return;
    };
    for recipe in &report.recipes {
        let detail = if let Some(fetch_decode) = &report.shared_timing.fetch_decode {
            format!(
                "source_mode={} source_route={} shared_surface planned_family={} fetched_family={} resolved_source={}; shared_pressure planned_family={} fetched_family={} resolved_source={}",
                report.source_mode.as_str(),
                recipe.source_route.as_str(),
                fetch_decode.surface_fetch.planned_product,
                fetch_decode.surface_fetch.fetched_product,
                fetch_decode.surface_fetch.resolved_source,
                fetch_decode.pressure_fetch.planned_product,
                fetch_decode.pressure_fetch.fetched_product,
                fetch_decode.pressure_fetch.resolved_source
            )
        } else {
            format!(
                "source_mode={} source_route={} native_thermo_only native_extract_ms={} native_compare_ms={}",
                report.source_mode.as_str(),
                recipe.source_route.as_str(),
                report.shared_timing.native_extract_ms,
                report.shared_timing.native_compare_ms
            )
        };
        manifest.update_artifact_state(
            &derived_artifact_key(&recipe.recipe_slug),
            ArtifactPublicationState::Complete,
            Some(detail),
        );
        manifest.update_artifact_identity(
            &derived_artifact_key(&recipe.recipe_slug),
            recipe.content_identity.clone(),
        );
        manifest.update_artifact_input_fetch_keys(
            &derived_artifact_key(&recipe.recipe_slug),
            recipe.input_fetch_keys.clone(),
        );
    }
    for blocker in &report.blockers {
        manifest.update_artifact_state(
            &derived_artifact_key(&blocker.recipe_slug),
            ArtifactPublicationState::Blocked,
            Some(format!(
                "source_mode={} source_route={} {}",
                report.source_mode.as_str(),
                blocker.source_route.as_str(),
                blocker.reason
            )),
        );
    }
}

fn apply_windowed_manifest_updates(
    manifest: &mut RunPublicationManifest,
    windowed: &Option<HrrrWindowedBatchReport>,
) {
    let Some(report) = windowed else {
        return;
    };
    for product in &report.products {
        let detail = windowed_artifact_detail(product, &report.shared_timing);
        manifest.update_artifact_state(
            &windowed_artifact_key(product.product.slug()),
            ArtifactPublicationState::Complete,
            Some(detail),
        );
        if let Ok(identity) = artifact_identity_from_path(&product.output_path) {
            manifest
                .update_artifact_identity(&windowed_artifact_key(product.product.slug()), identity);
        }
        let input_fetch_keys = windowed_product_input_fetch_keys(product, &report.shared_timing);
        if !input_fetch_keys.is_empty() {
            manifest.update_artifact_input_fetch_keys(
                &windowed_artifact_key(product.product.slug()),
                input_fetch_keys,
            );
        }
    }
    for blocker in &report.blockers {
        manifest.update_artifact_state(
            &windowed_artifact_key(blocker.product.slug()),
            ArtifactPublicationState::Blocked,
            Some(blocker.reason.clone()),
        );
    }
}

fn windowed_artifact_detail(
    product: &HrrrWindowedRenderedProduct,
    shared_timing: &crate::windowed::HrrrWindowedSharedTiming,
) -> String {
    let is_qpf = matches!(
        product.product,
        HrrrWindowedProduct::Qpf1h
            | HrrrWindowedProduct::Qpf6h
            | HrrrWindowedProduct::Qpf12h
            | HrrrWindowedProduct::Qpf24h
            | HrrrWindowedProduct::QpfTotal
    );
    let is_wind = matches!(
        product.product,
        HrrrWindowedProduct::Wind10m1hMax
            | HrrrWindowedProduct::Wind10mRunMax
            | HrrrWindowedProduct::Wind10m0to24hMax
            | HrrrWindowedProduct::Wind10m24to48hMax
            | HrrrWindowedProduct::Wind10m0to48hMax
    );
    let fetches = windowed_runtime_fetches_for_product(product, shared_timing);
    let planned_family = fetches
        .first()
        .map(|fetch| fetch.planned_product.as_str())
        .unwrap_or(if is_qpf || is_wind { "sfc" } else { "nat" });
    let fetched_families = unique_join(fetches.iter().map(|fetch| fetch.fetched_product.as_str()));
    let resolved_sources = unique_join(fetches.iter().map(|fetch| fetch.resolved_source.as_str()));
    let hours = fetches
        .iter()
        .map(|fetch| fetch.hour.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "planned_family={} fetched_families={} resolved_sources={} contributing_fetch_hours=[{}]",
        planned_family, fetched_families, resolved_sources, hours
    )
}

fn unique_join<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut unique = Vec::<&'a str>::new();
    for value in values {
        if !unique.contains(&value) {
            unique.push(value);
        }
    }
    unique.join(",")
}

#[cfg(test)]
fn count_blocked_artifacts(manifest: &RunPublicationManifest) -> usize {
    manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.state == ArtifactPublicationState::Blocked)
        .count()
}

fn collect_input_fetches(
    direct: &Option<HrrrDirectBatchReport>,
    derived: &Option<HrrrDerivedBatchReport>,
    windowed: &Option<HrrrWindowedBatchReport>,
) -> Vec<PublishedFetchIdentity> {
    let mut by_key = HashMap::<String, PublishedFetchIdentity>::new();

    if let Some(report) = direct {
        for fetch in &report.fetches {
            by_key
                .entry(fetch.input_fetch.fetch_key.clone())
                .or_insert_with(|| fetch.input_fetch.clone());
        }
    }

    if let Some(report) = derived {
        for fetch in &report.input_fetches {
            by_key
                .entry(fetch.fetch_key.clone())
                .or_insert_with(|| fetch.clone());
        }
    }

    if let Some(report) = windowed {
        for identity in collect_windowed_input_fetches(report) {
            by_key.entry(identity.fetch_key.clone()).or_insert(identity);
        }
    }

    let mut fetches = by_key.into_values().collect::<Vec<_>>();
    fetches.sort_by(|left, right| left.fetch_key.cmp(&right.fetch_key));
    fetches
}

fn windowed_runtime_fetches_for_product<'a>(
    product: &HrrrWindowedRenderedProduct,
    shared_timing: &'a crate::windowed::HrrrWindowedSharedTiming,
) -> Vec<&'a crate::windowed::HrrrWindowedHourFetchInfo> {
    let is_qpf = matches!(
        product.product,
        HrrrWindowedProduct::Qpf1h
            | HrrrWindowedProduct::Qpf6h
            | HrrrWindowedProduct::Qpf12h
            | HrrrWindowedProduct::Qpf24h
            | HrrrWindowedProduct::QpfTotal
    );
    let is_wind = matches!(
        product.product,
        HrrrWindowedProduct::Wind10m1hMax
            | HrrrWindowedProduct::Wind10mRunMax
            | HrrrWindowedProduct::Wind10m0to24hMax
            | HrrrWindowedProduct::Wind10m24to48hMax
            | HrrrWindowedProduct::Wind10m0to48hMax
    );
    let contributing_hours = &product.metadata.contributing_forecast_hours;
    let fetches = if is_qpf {
        &shared_timing.surface_hour_fetches
    } else if is_wind {
        &shared_timing.wind_hour_fetches
    } else {
        &shared_timing.uh_hour_fetches
    };
    fetches
        .iter()
        .filter(|fetch| contributing_hours.contains(&fetch.hour))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derived::{
        HrrrDerivedRecipeTiming, HrrrDerivedRenderedRecipe, HrrrDerivedSharedTiming,
    };
    use crate::direct::{
        DirectBatchRequest, HrrrDirectRecipeTiming, HrrrDirectRenderedRecipe,
        plan_direct_fetch_groups,
    };
    use crate::hrrr::HrrrFetchRuntimeInfo;
    use crate::windowed::{
        HrrrWindowedBlocker, HrrrWindowedHourFetchInfo, HrrrWindowedProductMetadata,
        HrrrWindowedProductTiming, HrrrWindowedRenderedProduct, HrrrWindowedSharedTiming,
    };

    fn domain() -> DomainSpec {
        DomainSpec::new("conus", (-127.0, -66.0, 23.0, 51.5))
    }

    fn empty_request() -> HrrrNonEcapeHourRequest {
        HrrrNonEcapeHourRequest {
            date_yyyymmdd: "20260415".into(),
            cycle_override_utc: Some(12),
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            out_dir: PathBuf::from("C:\\temp\\proof"),
            cache_root: PathBuf::from("C:\\temp\\proof\\cache"),
            use_cache: true,
            source_mode: ProductSourceMode::Canonical,
            direct_recipe_slugs: Vec::new(),
            derived_recipe_slugs: Vec::new(),
            windowed_products: Vec::new(),
            output_width: 1200,
            output_height: 900,
            png_compression: PngCompressionMode::Default,
            custom_poi_overlay: None,
            place_label_overlay: None,
        }
    }

    fn latest_global(model: ModelId) -> LatestRun {
        LatestRun {
            model,
            cycle: rustwx_core::CycleSpec::new("20260415", 12).unwrap(),
            source: SourceId::Aws,
        }
    }

    #[test]
    fn duplicate_multi_domain_slugs_are_rejected() {
        let err = validate_requested_domains(&[domain(), domain()]).unwrap_err();
        assert!(err.to_string().contains("duplicate multi-domain slug"));
    }

    fn windowed_fetch_identity(
        planned_family: &str,
        fetched_product: &str,
        hour: u16,
    ) -> PublishedFetchIdentity {
        let request = rustwx_core::ModelRunRequest::new(
            rustwx_core::ModelId::Hrrr,
            rustwx_core::CycleSpec::new("20260415", 12).unwrap(),
            hour,
            fetched_product,
        )
        .unwrap();
        PublishedFetchIdentity {
            fetch_key: crate::publication::fetch_key(planned_family, &request),
            planned_family: planned_family.to_string(),
            planned_family_aliases: Vec::new(),
            request,
            source_override: Some(SourceId::Aws),
            resolved_source: SourceId::Aws,
            resolved_url: format!(
                "https://example.test/hrrr.t12z.wrf{}f{:02}.grib2",
                fetched_product, hour
            ),
            resolved_family: fetched_product.to_string(),
            bytes_len: 3,
            bytes_sha256: "abc123".into(),
        }
    }

    #[test]
    fn validation_rejects_empty_requests() {
        let err = validate_requested_work(
            ModelId::Hrrr,
            &normalize_requested_products(&empty_request()),
        )
        .expect_err("empty request should be rejected")
        .to_string();
        assert!(err.contains("at least one direct recipe"));
    }

    #[test]
    fn validation_rejects_heavy_derived_recipes() {
        let mut request = empty_request();
        request.derived_recipe_slugs = vec!["sbecape".into()];
        let err = validate_requested_work(ModelId::Hrrr, &normalize_requested_products(&request))
            .expect_err("heavy derived recipes should be rejected by non_ecape_hour")
            .to_string();
        assert!(err.contains("heavy ECAPE product"));
    }

    #[test]
    fn normalization_routes_legacy_one_hour_qpf_to_windowed_lane() {
        let mut request = empty_request();
        request.direct_recipe_slugs = vec!["1h_qpf".into(), "cloud_cover".into()];
        let normalized = normalize_requested_products(&request);
        assert_eq!(
            normalized.direct_recipe_slugs,
            vec!["cloud_cover".to_string()]
        );
        assert_eq!(
            normalized.windowed_products,
            vec![HrrrWindowedProduct::Qpf1h]
        );
    }

    #[test]
    fn nomads_runs_lanes_sequentially() {
        assert!(!should_run_lanes_concurrently(
            ModelId::Hrrr,
            SourceId::Nomads
        ));
        assert!(should_run_lanes_concurrently(ModelId::Hrrr, SourceId::Aws));
        assert!(should_run_lanes_concurrently(
            ModelId::WrfGdex,
            SourceId::Gdex
        ));
    }

    #[test]
    fn shared_non_ecape_plan_collapses_gfs_direct_and_pair_to_one_fetch_key() {
        let latest = latest_global(ModelId::Gfs);
        let direct_request = DirectBatchRequest {
            model: ModelId::Gfs,
            date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
            cycle_override_utc: Some(latest.cycle.hour_utc),
            forecast_hour: 12,
            source: latest.source,
            domain: domain(),
            out_dir: PathBuf::from("C:\\temp\\proof"),
            cache_root: PathBuf::from("C:\\temp\\proof\\cache"),
            use_cache: true,
            recipe_slugs: vec!["mslp_10m_winds".into()],
            product_overrides: HashMap::new(),
            contour_mode: crate::derived::NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: 1200,
            output_height: 900,
            png_compression: PngCompressionMode::Default,
            custom_poi_overlay: None,
            place_label_overlay: None,
        };
        let direct_groups = plan_direct_fetch_groups(&direct_request).unwrap();
        let derived_recipes = plan_derived_recipes(&["sbcape".to_string()]).unwrap();
        let derived_routes = plan_native_thermo_routes_with_surface_product(
            ModelId::Gfs,
            &derived_recipes,
            ProductSourceMode::Canonical,
            None,
        )
        .unwrap();

        let plan = build_shared_non_ecape_execution_plan(
            &latest,
            12,
            &direct_groups,
            Some(&derived_routes),
            true,
            None,
            None,
        );

        assert_eq!(plan.fetch_keys().len(), 1);
        assert_eq!(plan.fetch_keys()[0].native_product, "pgrb2.0p25");
    }

    #[test]
    fn shared_non_ecape_plan_collapses_ecmwf_direct_and_pair_to_one_fetch_key() {
        let latest = latest_global(ModelId::EcmwfOpenData);
        let direct_request = DirectBatchRequest {
            model: ModelId::EcmwfOpenData,
            date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
            cycle_override_utc: Some(latest.cycle.hour_utc),
            forecast_hour: 6,
            source: latest.source,
            domain: domain(),
            out_dir: PathBuf::from("C:\\temp\\proof"),
            cache_root: PathBuf::from("C:\\temp\\proof\\cache"),
            use_cache: true,
            recipe_slugs: vec!["500mb_height_winds".into()],
            product_overrides: HashMap::new(),
            contour_mode: crate::derived::NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: 1200,
            output_height: 900,
            png_compression: PngCompressionMode::Default,
            custom_poi_overlay: None,
            place_label_overlay: None,
        };
        let direct_groups = plan_direct_fetch_groups(&direct_request).unwrap();
        let derived_recipes = plan_derived_recipes(&["sbcape".to_string()]).unwrap();
        let derived_routes = plan_native_thermo_routes_with_surface_product(
            ModelId::EcmwfOpenData,
            &derived_recipes,
            ProductSourceMode::Canonical,
            None,
        )
        .unwrap();

        let plan = build_shared_non_ecape_execution_plan(
            &latest,
            6,
            &direct_groups,
            Some(&derived_routes),
            true,
            None,
            None,
        );

        assert_eq!(plan.fetch_keys().len(), 1);
        assert_eq!(plan.fetch_keys()[0].native_product, "oper");
    }

    #[test]
    fn summary_flattens_outputs_across_all_runners() {
        let direct = HrrrDirectBatchReport {
            model: rustwx_core::ModelId::Hrrr,
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            fetches: Vec::new(),
            recipes: vec![HrrrDirectRenderedRecipe {
                recipe_slug: "composite_reflectivity".into(),
                title: "Composite Reflectivity".into(),
                source_route: crate::source::ProductSourceRoute::DirectNativeExact,
                grib_product: "nat".into(),
                fetched_grib_product: "sfc".into(),
                resolved_source: SourceId::Aws,
                resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                output_path: PathBuf::from("C:\\proof\\direct.png"),
                content_identity: crate::publication::artifact_identity_from_bytes(b"direct"),
                input_fetch_keys: vec!["direct:nat->sfc".into()],
                timing: HrrrDirectRecipeTiming {
                    project_ms: 1,
                    field_prepare_ms: 0,
                    contour_prepare_ms: 0,
                    barb_prepare_ms: 0,
                    render_to_image_ms: 0,
                    data_layer_draw_ms: 0,
                    overlay_draw_ms: 0,
                    panel_compose_ms: 0,
                    request_build_ms: 0,
                    render_state_prep_ms: 0,
                    png_encode_ms: 0,
                    file_write_ms: 0,
                    render_ms: 2,
                    total_ms: 3,
                    state_timing: Default::default(),
                    image_timing: Default::default(),
                },
            }],
            blockers: Vec::new(),
            total_ms: 10,
        };
        let derived = HrrrDerivedBatchReport {
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            input_fetches: Vec::new(),
            shared_timing: HrrrDerivedSharedTiming {
                fetch_decode: Some(crate::gridded::SharedTiming {
                    fetch_surface_ms: 0,
                    fetch_pressure_ms: 0,
                    decode_surface_ms: 0,
                    decode_pressure_ms: 0,
                    fetch_surface_cache_hit: false,
                    fetch_pressure_cache_hit: false,
                    decode_surface_cache_hit: false,
                    decode_pressure_cache_hit: false,
                    surface_fetch: crate::gridded::FetchRuntimeInfo {
                        planned_bundle: rustwx_core::CanonicalBundleDescriptor::SurfaceAnalysis,
                        planned_family: rustwx_core::CanonicalDataFamily::Surface,
                        planned_product: "sfc".into(),
                        resolved_native_product: "sfc".into(),
                        fetched_product: "sfc".into(),
                        requested_source: SourceId::Aws,
                        resolved_source: SourceId::Aws,
                        resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    },
                    pressure_fetch: crate::gridded::FetchRuntimeInfo {
                        planned_bundle: rustwx_core::CanonicalBundleDescriptor::PressureAnalysis,
                        planned_family: rustwx_core::CanonicalDataFamily::Pressure,
                        planned_product: "prs".into(),
                        resolved_native_product: "prs".into(),
                        fetched_product: "prs".into(),
                        requested_source: SourceId::Aws,
                        resolved_source: SourceId::Aws,
                        resolved_url: "https://example.test/hrrr.t12z.wrfprsf06.grib2".into(),
                    },
                }),
                compute_ms: 4,
                project_ms: 5,
                native_extract_ms: 0,
                native_compare_ms: 0,
                memory_profile: None,
                heavy_timing: None,
            },
            recipes: vec![HrrrDerivedRenderedRecipe {
                recipe_slug: "sbcape".into(),
                title: "SBCAPE".into(),
                source_route: crate::source::ProductSourceRoute::CanonicalDerived,
                output_path: PathBuf::from("C:\\proof\\derived.png"),
                content_identity: crate::publication::artifact_identity_from_bytes(b"derived"),
                input_fetch_keys: vec!["derived:sfc".into(), "derived:prs".into()],
                timing: HrrrDerivedRecipeTiming {
                    render_to_image_ms: 0,
                    data_layer_draw_ms: 0,
                    overlay_draw_ms: 0,
                    render_state_prep_ms: 0,
                    png_encode_ms: 0,
                    file_write_ms: 0,
                    render_ms: 6,
                    total_ms: 6,
                    state_timing: Default::default(),
                    image_timing: Default::default(),
                },
            }],
            source_mode: ProductSourceMode::Canonical,
            blockers: Vec::new(),
            native_thermo_artifacts: Vec::new(),
            total_ms: 11,
        };
        let windowed = HrrrWindowedBatchReport {
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            shared_timing: HrrrWindowedSharedTiming {
                fetch_geometry_ms: 0,
                decode_geometry_ms: 0,
                project_ms: 0,
                fetch_surface_ms: 0,
                decode_surface_ms: 0,
                fetch_nat_ms: 0,
                decode_nat_ms: 0,
                fetch_wind_ms: 0,
                decode_wind_ms: 0,
                geometry_fetch_cache_hit: false,
                geometry_decode_cache_hit: false,
                surface_hours_loaded: vec![6],
                nat_hours_loaded: vec![6],
                wind_hours_loaded: Vec::new(),
                geometry_fetch: Some(HrrrFetchRuntimeInfo {
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                }),
                geometry_input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                surface_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                }],
                uh_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "nat".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("nat", "sfc", 6)),
                }],
                wind_hour_fetches: Vec::new(),
            },
            products: vec![HrrrWindowedRenderedProduct {
                product: HrrrWindowedProduct::Qpf6h,
                output_path: PathBuf::from("C:\\proof\\windowed.png"),
                timing: HrrrWindowedProductTiming {
                    compute_ms: 7,
                    render_ms: 8,
                    total_ms: 15,
                },
                metadata: HrrrWindowedProductMetadata {
                    strategy: "direct APCP 6h accumulation".into(),
                    contributing_forecast_hours: vec![1, 2, 3, 4, 5, 6],
                    window_hours: Some(6),
                },
            }],
            blockers: vec![HrrrWindowedBlocker {
                product: HrrrWindowedProduct::Uh25kmRunMax,
                reason: "demo blocker".into(),
            }],
            total_ms: 12,
        };

        let summary = build_summary(&Some(direct), &Some(derived), &Some(windowed));
        assert_eq!(summary.runner_count, 3);
        assert_eq!(summary.direct_rendered_count, 1);
        assert_eq!(summary.derived_rendered_count, 1);
        assert_eq!(summary.windowed_rendered_count, 1);
        assert_eq!(summary.windowed_blocker_count, 1);
        assert_eq!(summary.output_count, 3);
        assert_eq!(
            summary.output_paths,
            vec![
                PathBuf::from("C:\\proof\\direct.png"),
                PathBuf::from("C:\\proof\\derived.png"),
                PathBuf::from("C:\\proof\\windowed.png"),
            ]
        );
    }

    #[test]
    fn run_manifest_tracks_planned_complete_and_blocked_artifacts() {
        let requested = HrrrNonEcapeHourRequestedProducts {
            direct_recipe_slugs: vec!["500mb_height_winds".into()],
            derived_recipe_slugs: vec!["sbcape".into()],
            windowed_products: vec![HrrrWindowedProduct::Qpf6h, HrrrWindowedProduct::Qpf12h],
        };
        let mut manifest = build_run_manifest(
            ModelId::Hrrr,
            &requested,
            std::path::Path::new("C:\\proof\\run"),
            "rustwx_hrrr_20260415_12z_f006_conus_non_ecape_hour",
            "20260415",
            12,
            6,
            "conus",
        );
        manifest.mark_running();

        let direct = HrrrDirectBatchReport {
            model: rustwx_core::ModelId::Hrrr,
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            fetches: Vec::new(),
            recipes: vec![HrrrDirectRenderedRecipe {
                recipe_slug: "500mb_height_winds".into(),
                title: "500mb Height / Winds".into(),
                source_route: crate::source::ProductSourceRoute::DirectNativeExact,
                grib_product: "prs".into(),
                fetched_grib_product: "prs".into(),
                resolved_source: SourceId::Aws,
                resolved_url: "https://example.test/hrrr.t12z.wrfprsf06.grib2".into(),
                output_path: PathBuf::from(
                    "C:\\proof\\run\\rustwx_hrrr_20260415_12z_f006_conus_500mb_height_winds.png",
                ),
                content_identity: crate::publication::artifact_identity_from_bytes(b"direct-run"),
                input_fetch_keys: vec!["direct:prs".into()],
                timing: HrrrDirectRecipeTiming {
                    project_ms: 1,
                    field_prepare_ms: 0,
                    contour_prepare_ms: 0,
                    barb_prepare_ms: 0,
                    render_to_image_ms: 0,
                    data_layer_draw_ms: 0,
                    overlay_draw_ms: 0,
                    panel_compose_ms: 0,
                    request_build_ms: 0,
                    render_state_prep_ms: 0,
                    png_encode_ms: 0,
                    file_write_ms: 0,
                    render_ms: 2,
                    total_ms: 3,
                    state_timing: Default::default(),
                    image_timing: Default::default(),
                },
            }],
            blockers: Vec::new(),
            total_ms: 10,
        };
        let derived = HrrrDerivedBatchReport {
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            input_fetches: Vec::new(),
            shared_timing: HrrrDerivedSharedTiming {
                fetch_decode: Some(crate::gridded::SharedTiming {
                    fetch_surface_ms: 0,
                    fetch_pressure_ms: 0,
                    decode_surface_ms: 0,
                    decode_pressure_ms: 0,
                    fetch_surface_cache_hit: false,
                    fetch_pressure_cache_hit: false,
                    decode_surface_cache_hit: false,
                    decode_pressure_cache_hit: false,
                    surface_fetch: crate::gridded::FetchRuntimeInfo {
                        planned_bundle: rustwx_core::CanonicalBundleDescriptor::SurfaceAnalysis,
                        planned_family: rustwx_core::CanonicalDataFamily::Surface,
                        planned_product: "sfc".into(),
                        resolved_native_product: "sfc".into(),
                        fetched_product: "sfc".into(),
                        requested_source: SourceId::Aws,
                        resolved_source: SourceId::Aws,
                        resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    },
                    pressure_fetch: crate::gridded::FetchRuntimeInfo {
                        planned_bundle: rustwx_core::CanonicalBundleDescriptor::PressureAnalysis,
                        planned_family: rustwx_core::CanonicalDataFamily::Pressure,
                        planned_product: "prs".into(),
                        resolved_native_product: "prs".into(),
                        fetched_product: "prs".into(),
                        requested_source: SourceId::Aws,
                        resolved_source: SourceId::Aws,
                        resolved_url: "https://example.test/hrrr.t12z.wrfprsf06.grib2".into(),
                    },
                }),
                compute_ms: 1,
                project_ms: 1,
                native_extract_ms: 0,
                native_compare_ms: 0,
                memory_profile: None,
                heavy_timing: None,
            },
            recipes: vec![HrrrDerivedRenderedRecipe {
                recipe_slug: "sbcape".into(),
                title: "SBCAPE".into(),
                source_route: crate::source::ProductSourceRoute::CanonicalDerived,
                output_path: PathBuf::from(
                    "C:\\proof\\run\\rustwx_hrrr_20260415_12z_f006_conus_sbcape.png",
                ),
                content_identity: crate::publication::artifact_identity_from_bytes(b"derived-run"),
                input_fetch_keys: vec!["derived:sfc".into(), "derived:prs".into()],
                timing: HrrrDerivedRecipeTiming {
                    render_to_image_ms: 0,
                    data_layer_draw_ms: 0,
                    overlay_draw_ms: 0,
                    render_state_prep_ms: 0,
                    png_encode_ms: 0,
                    file_write_ms: 0,
                    render_ms: 1,
                    total_ms: 1,
                    state_timing: Default::default(),
                    image_timing: Default::default(),
                },
            }],
            source_mode: ProductSourceMode::Canonical,
            blockers: Vec::new(),
            native_thermo_artifacts: Vec::new(),
            total_ms: 5,
        };
        let windowed = HrrrWindowedBatchReport {
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            shared_timing: HrrrWindowedSharedTiming {
                fetch_geometry_ms: 0,
                decode_geometry_ms: 0,
                project_ms: 0,
                fetch_surface_ms: 0,
                decode_surface_ms: 0,
                fetch_nat_ms: 0,
                decode_nat_ms: 0,
                fetch_wind_ms: 0,
                decode_wind_ms: 0,
                geometry_fetch_cache_hit: false,
                geometry_decode_cache_hit: false,
                surface_hours_loaded: vec![6],
                nat_hours_loaded: vec![6],
                wind_hours_loaded: Vec::new(),
                geometry_fetch: Some(HrrrFetchRuntimeInfo {
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                }),
                geometry_input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                surface_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                }],
                uh_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "nat".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("nat", "sfc", 6)),
                }],
                wind_hour_fetches: Vec::new(),
            },
            products: vec![HrrrWindowedRenderedProduct {
                product: HrrrWindowedProduct::Qpf6h,
                output_path: PathBuf::from(
                    "C:\\proof\\run\\rustwx_hrrr_20260415_12z_f006_conus_qpf_6h.png",
                ),
                timing: HrrrWindowedProductTiming {
                    compute_ms: 1,
                    render_ms: 1,
                    total_ms: 2,
                },
                metadata: HrrrWindowedProductMetadata {
                    strategy: "test".into(),
                    contributing_forecast_hours: vec![1, 2, 3, 4, 5, 6],
                    window_hours: Some(6),
                },
            }],
            blockers: vec![HrrrWindowedBlocker {
                product: HrrrWindowedProduct::Qpf12h,
                reason: "not enough hours".into(),
            }],
            total_ms: 2,
        };

        apply_direct_manifest_updates(&mut manifest, &Some(direct));
        apply_derived_manifest_updates(&mut manifest, &Some(derived));
        apply_windowed_manifest_updates(&mut manifest, &Some(windowed));
        assert_eq!(count_blocked_artifacts(&manifest), 1);

        let direct_record = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_key == "direct:500mb_height_winds")
            .unwrap();
        assert_eq!(direct_record.state, ArtifactPublicationState::Complete);
        assert!(
            direct_record
                .detail
                .as_deref()
                .unwrap()
                .contains("planned_family=prs fetched_family=prs resolved_source=aws")
        );

        let derived_record = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_key == "derived:sbcape")
            .unwrap();
        assert_eq!(derived_record.state, ArtifactPublicationState::Complete);
        assert!(
            derived_record.detail.as_deref().unwrap().contains(
                "shared_surface planned_family=sfc fetched_family=sfc resolved_source=aws"
            )
        );

        let blocked_record = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_key == "windowed:qpf_12h")
            .unwrap();
        assert_eq!(blocked_record.state, ArtifactPublicationState::Blocked);
        assert_eq!(blocked_record.detail.as_deref(), Some("not enough hours"));
    }

    #[test]
    fn windowed_input_fetch_keys_follow_contributing_hours_without_cache() {
        let product = HrrrWindowedRenderedProduct {
            product: HrrrWindowedProduct::Qpf1h,
            output_path: PathBuf::from("C:\\proof\\qpf_1h.png"),
            timing: HrrrWindowedProductTiming {
                compute_ms: 1,
                render_ms: 1,
                total_ms: 2,
            },
            metadata: HrrrWindowedProductMetadata {
                strategy: "direct APCP 1h accumulation".into(),
                contributing_forecast_hours: vec![6],
                window_hours: Some(1),
            },
        };
        let shared_timing = HrrrWindowedSharedTiming {
            fetch_geometry_ms: 0,
            decode_geometry_ms: 0,
            project_ms: 0,
            fetch_surface_ms: 0,
            decode_surface_ms: 0,
            fetch_nat_ms: 0,
            decode_nat_ms: 0,
            fetch_wind_ms: 0,
            decode_wind_ms: 0,
            geometry_fetch_cache_hit: false,
            geometry_decode_cache_hit: false,
            surface_hours_loaded: vec![5, 6],
            nat_hours_loaded: Vec::new(),
            wind_hours_loaded: Vec::new(),
            geometry_fetch: None,
            geometry_input_fetch: None,
            surface_hour_fetches: vec![
                HrrrWindowedHourFetchInfo {
                    hour: 5,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf05.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 5)),
                },
                HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                },
            ],
            uh_hour_fetches: Vec::new(),
            wind_hour_fetches: Vec::new(),
        };

        let keys = windowed_product_input_fetch_keys(&product, &shared_timing);
        assert_eq!(
            keys,
            vec![windowed_fetch_identity("sfc", "sfc", 6).fetch_key]
        );
    }

    #[test]
    fn collect_input_fetches_keeps_windowed_lineage_when_cache_is_off() {
        let report = HrrrWindowedBatchReport {
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            shared_timing: HrrrWindowedSharedTiming {
                fetch_geometry_ms: 0,
                decode_geometry_ms: 0,
                project_ms: 0,
                fetch_surface_ms: 0,
                decode_surface_ms: 0,
                fetch_nat_ms: 0,
                decode_nat_ms: 0,
                fetch_wind_ms: 0,
                decode_wind_ms: 0,
                geometry_fetch_cache_hit: false,
                geometry_decode_cache_hit: false,
                surface_hours_loaded: vec![6],
                nat_hours_loaded: vec![6],
                wind_hours_loaded: Vec::new(),
                geometry_fetch: None,
                geometry_input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                surface_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("sfc", "sfc", 6)),
                }],
                uh_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "nat".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                    input_fetch: Some(windowed_fetch_identity("nat", "sfc", 6)),
                }],
                wind_hour_fetches: Vec::new(),
            },
            products: Vec::new(),
            blockers: Vec::new(),
            total_ms: 1,
        };

        let fetches = collect_input_fetches(&None, &None, &Some(report));
        let keys = fetches
            .into_iter()
            .map(|fetch| fetch.fetch_key)
            .collect::<Vec<_>>();
        assert!(keys.contains(&windowed_fetch_identity("sfc", "sfc", 6).fetch_key));
        assert!(keys.contains(&windowed_fetch_identity("nat", "sfc", 6).fetch_key));
    }

    #[test]
    fn non_ecape_report_serialization_keeps_cache_mode_for_benchmarks() {
        let report = HrrrNonEcapeHourReport {
            date_yyyymmdd: "20260415".into(),
            cycle_utc: 12,
            forecast_hour: 6,
            source: SourceId::Aws,
            domain: domain(),
            out_dir: PathBuf::from("C:\\proof\\bench"),
            cache_root: PathBuf::from("C:\\proof\\bench\\cache"),
            use_cache: false,
            source_mode: ProductSourceMode::Canonical,
            publication_manifest_path: PathBuf::from("C:\\proof\\bench\\run_manifest.json"),
            attempt_manifest_path: None,
            requested: HrrrNonEcapeHourRequestedProducts {
                direct_recipe_slugs: vec!["500mb_height_winds".into()],
                derived_recipe_slugs: vec!["sbcape".into()],
                windowed_products: vec![HrrrWindowedProduct::Qpf6h],
            },
            shared_timing: HrrrNonEcapeSharedTiming::default(),
            summary: HrrrNonEcapeHourSummary {
                runner_count: 1,
                direct_rendered_count: 1,
                derived_rendered_count: 0,
                windowed_rendered_count: 0,
                windowed_blocker_count: 0,
                output_count: 1,
                output_paths: vec![PathBuf::from("C:\\proof\\bench\\out.png")],
            },
            direct: None,
            derived: None,
            windowed: None,
            total_ms: 1234,
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(
            json.contains("\"use_cache\":false"),
            "cold benchmark reports should serialize cache mode explicitly"
        );
    }
}
