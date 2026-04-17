use crate::gridded::{
    decode_cache_path, load_surface_geometry_from_latest, resolve_model_run, FetchRuntimeInfo,
};
use crate::hrrr::HrrrFetchRuntimeInfo;
use crate::planner::ExecutionPlanBuilder;
use crate::publication::{fetch_identity_from_cached_result, PublishedFetchIdentity};
use crate::runtime::{BundleLoaderConfig, FetchedBundleBytes, LoadedBundleSet, load_execution_plan};
use crate::shared_context::{DomainSpec, ProjectedMap};
use crate::windowed_decoder::{
    compute_qpf_product, compute_uh_product, load_or_decode_apcp, load_or_decode_uh25,
    HrrrApcpDecode, HrrrUhDecode,
};
use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, ModelId, SourceId,
};
use rustwx_models::LatestRun;
use rustwx_render::{save_png, MapRenderRequest, Solar07Product};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

const OUTPUT_WIDTH: u32 = 1200;
const OUTPUT_HEIGHT: u32 = 900;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HrrrWindowedProduct {
    Qpf1h,
    Qpf6h,
    Qpf12h,
    Qpf24h,
    QpfTotal,
    Uh25km1h,
    Uh25km3h,
    Uh25kmRunMax,
}

impl HrrrWindowedProduct {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Qpf1h => "qpf_1h",
            Self::Qpf6h => "qpf_6h",
            Self::Qpf12h => "qpf_12h",
            Self::Qpf24h => "qpf_24h",
            Self::QpfTotal => "qpf_total",
            Self::Uh25km1h => "uh_2to5km_1h_max",
            Self::Uh25km3h => "uh_2to5km_3h_max",
            Self::Uh25kmRunMax => "uh_2to5km_run_max",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Qpf1h => "1-h QPF",
            Self::Qpf6h => "6-h QPF",
            Self::Qpf12h => "12-h QPF",
            Self::Qpf24h => "24-h QPF",
            Self::QpfTotal => "Total QPF",
            Self::Uh25km1h => "Updraft Helicity: 2-5 km AGL (1 h max)",
            Self::Uh25km3h => "Updraft Helicity: 2-5 km AGL (3 h max)",
            Self::Uh25kmRunMax => "Updraft Helicity: 2-5 km AGL (run max)",
        }
    }

    fn is_qpf(self) -> bool {
        matches!(
            self,
            Self::Qpf1h | Self::Qpf6h | Self::Qpf12h | Self::Qpf24h | Self::QpfTotal
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedBatchRequest {
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub products: Vec<HrrrWindowedProduct>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedHourFetchInfo {
    pub hour: u16,
    pub planned_product: String,
    pub fetched_product: String,
    pub requested_source: SourceId,
    pub resolved_source: SourceId,
    pub resolved_url: String,
    pub fetch_cache_hit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_fetch: Option<PublishedFetchIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedSharedTiming {
    pub fetch_geometry_ms: u128,
    pub decode_geometry_ms: u128,
    pub project_ms: u128,
    pub fetch_surface_ms: u128,
    pub decode_surface_ms: u128,
    pub fetch_nat_ms: u128,
    pub decode_nat_ms: u128,
    pub geometry_fetch_cache_hit: bool,
    pub geometry_decode_cache_hit: bool,
    pub surface_hours_loaded: Vec<u16>,
    pub nat_hours_loaded: Vec<u16>,
    pub geometry_fetch: Option<HrrrFetchRuntimeInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geometry_input_fetch: Option<PublishedFetchIdentity>,
    pub surface_hour_fetches: Vec<HrrrWindowedHourFetchInfo>,
    pub uh_hour_fetches: Vec<HrrrWindowedHourFetchInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedProductTiming {
    pub compute_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedProductMetadata {
    pub strategy: String,
    pub contributing_forecast_hours: Vec<u16>,
    pub window_hours: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedRenderedProduct {
    pub product: HrrrWindowedProduct,
    pub output_path: PathBuf,
    pub timing: HrrrWindowedProductTiming,
    pub metadata: HrrrWindowedProductMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedBlocker {
    pub product: HrrrWindowedProduct,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrrrWindowedBatchReport {
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub shared_timing: HrrrWindowedSharedTiming,
    pub products: Vec<HrrrWindowedRenderedProduct>,
    pub blockers: Vec<HrrrWindowedBlocker>,
    pub total_ms: u128,
}

#[derive(Debug, Clone)]
struct PreparedWindowedGeometryContext {
    fetch_geometry_ms: u128,
    decode_geometry_ms: u128,
    geometry_fetch_cache_hit: bool,
    geometry_decode_cache_hit: bool,
    geometry_fetch: Option<HrrrFetchRuntimeInfo>,
    geometry_input_fetch: Option<PublishedFetchIdentity>,
    projected: ProjectedMap,
    project_ms: u128,
    grid: rustwx_core::LatLonGrid,
}

#[derive(Debug)]
enum WindowedProductOutcome {
    Rendered {
        index: usize,
        rendered: HrrrWindowedRenderedProduct,
    },
    Blocker {
        index: usize,
        blocker: HrrrWindowedBlocker,
    },
}

fn prepare_windowed_geometry_context(
    request: &HrrrWindowedBatchRequest,
    latest: &LatestRun,
) -> Result<PreparedWindowedGeometryContext, Box<dyn std::error::Error>> {
    let geometry = load_surface_geometry_from_latest(
        latest.clone(),
        request.forecast_hour,
        None,
        &request.cache_root,
        request.use_cache,
    )?;
    let project_start = Instant::now();
    let projected_maps = crate::gridded::build_projected_maps_for_sizes(
        &geometry.surface_decode.value,
        request.domain.bounds,
        &[(OUTPUT_WIDTH, OUTPUT_HEIGHT)],
    )?;
    let project_ms = project_start.elapsed().as_millis();
    let projected = projected_maps
        .projected_map(OUTPUT_WIDTH, OUTPUT_HEIGHT)
        .cloned()
        .ok_or("missing projected map for windowed batch")?;

    Ok(PreparedWindowedGeometryContext {
        fetch_geometry_ms: geometry.fetch_ms,
        decode_geometry_ms: geometry.decode_ms,
        geometry_fetch_cache_hit: geometry.surface_file.fetched.cache_hit,
        geometry_decode_cache_hit: geometry.surface_decode.cache_hit,
        geometry_fetch: Some(hrrr_fetch_runtime_info_from_bundle(
            &geometry.surface_file.runtime_info(&geometry.surface_bundle),
        )),
        geometry_input_fetch: Some(fetch_identity_from_cached_result(
            &geometry.surface_bundle.native_product,
            &geometry.surface_file.request,
            &geometry.surface_file.fetched,
        )),
        projected,
        project_ms,
        grid: geometry.grid,
    })
}

fn hrrr_fetch_runtime_info_from_bundle(fetch: &FetchRuntimeInfo) -> HrrrFetchRuntimeInfo {
    HrrrFetchRuntimeInfo {
        planned_product: fetch.planned_product.clone(),
        fetched_product: fetch.fetched_product.clone(),
        requested_source: fetch.requested_source,
        resolved_source: fetch.resolved_source,
        resolved_url: fetch.resolved_url.clone(),
    }
}

/// All `PublishedFetchIdentity` values that contributed to a windowed
/// batch, deduplicated by fetch key. Extracted so standalone runners
/// (`hrrr_windowed_batch`) and the unified runner (`hrrr_non_ecape_hour`)
/// publish the same input-fetch set.
pub fn collect_windowed_input_fetches(
    report: &HrrrWindowedBatchReport,
) -> Vec<PublishedFetchIdentity> {
    let mut by_key = std::collections::BTreeMap::<String, PublishedFetchIdentity>::new();
    if let Some(identity) = &report.shared_timing.geometry_input_fetch {
        by_key
            .entry(identity.fetch_key.clone())
            .or_insert_with(|| identity.clone());
    }
    for fetch in report
        .shared_timing
        .surface_hour_fetches
        .iter()
        .chain(report.shared_timing.uh_hour_fetches.iter())
    {
        if let Some(identity) = &fetch.input_fetch {
            by_key
                .entry(identity.fetch_key.clone())
                .or_insert_with(|| identity.clone());
        }
    }
    by_key.into_values().collect()
}

/// Fetch keys that cited this product as an input, in contributing-hour
/// order. Mirrors the runtime identity the rendered product actually
/// depended on (QPF products consume `sfc` hourly fetches; UH products
/// consume `nat` hourly fetches).
pub fn windowed_product_input_fetch_keys(
    product: &HrrrWindowedRenderedProduct,
    shared_timing: &HrrrWindowedSharedTiming,
) -> Vec<String> {
    let is_qpf = matches!(
        product.product,
        HrrrWindowedProduct::Qpf1h
            | HrrrWindowedProduct::Qpf6h
            | HrrrWindowedProduct::Qpf12h
            | HrrrWindowedProduct::Qpf24h
            | HrrrWindowedProduct::QpfTotal
    );
    let contributing_hours = &product.metadata.contributing_forecast_hours;
    let fetches = if is_qpf {
        &shared_timing.surface_hour_fetches
    } else {
        &shared_timing.uh_hour_fetches
    };
    let mut keys = Vec::new();
    for fetch in fetches
        .iter()
        .filter(|fetch| contributing_hours.contains(&fetch.hour))
    {
        if let Some(identity) = &fetch.input_fetch {
            if !keys.contains(&identity.fetch_key) {
                keys.push(identity.fetch_key.clone());
            }
        }
    }
    keys
}

pub fn run_hrrr_windowed_batch(
    request: &HrrrWindowedBatchRequest,
) -> Result<HrrrWindowedBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let latest = resolve_model_run(
        ModelId::Hrrr,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.source,
    )?;
    run_hrrr_windowed_batch_with_context(request, &latest)
}

pub(crate) fn run_hrrr_windowed_batch_with_context(
    request: &HrrrWindowedBatchRequest,
    latest: &rustwx_models::LatestRun,
) -> Result<HrrrWindowedBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let total_start = Instant::now();
    let geometry_context = prepare_windowed_geometry_context(request, latest)?;
    let fetch_geometry_ms = geometry_context.fetch_geometry_ms;
    let decode_geometry_ms = geometry_context.decode_geometry_ms;
    let geometry_fetch_cache_hit = geometry_context.geometry_fetch_cache_hit;
    let geometry_decode_cache_hit = geometry_context.geometry_decode_cache_hit;
    let geometry_fetch = geometry_context.geometry_fetch;
    let geometry_input_fetch = geometry_context.geometry_input_fetch;
    let projected = geometry_context.projected;
    let project_ms = geometry_context.project_ms;
    let grid = geometry_context.grid;

    let (planned_products, mut blockers, surface_hours, nat_hours) =
        plan_windowed_products(&request.products, request.forecast_hour);

    // Build a planner execution plan for every contributing forecast
    // hour the windowed lane needs. APCP and native UH both live in the
    // wrfsfc file, so the planner dedupes when QPF and UH products at
    // the same hour share a fetch — and the loader's parallel-fetch
    // path (off for NOMADS) keeps multi-hour runs reasonable.
    let mut all_hours: BTreeSet<u16> = surface_hours.iter().copied().collect();
    all_hours.extend(nat_hours.iter().copied());

    let mut plan_builder = ExecutionPlanBuilder::new(latest, request.forecast_hour);
    for &hour in &all_hours {
        let requirement = BundleRequirement::new(
            CanonicalBundleDescriptor::NativeAnalysis,
            hour,
        )
        .with_native_override("sfc");
        // Preserve the logical alias names manifests have always
        // surfaced for windowed: QPF hours show up as "sfc"; UH hours
        // show up as "nat" because the windowed lane historically
        // logged them as native-family fetches even though both decode
        // out of wrfsfc.
        if surface_hours.contains(&hour) {
            plan_builder.require_with_logical_family(&requirement, Some("sfc"));
        }
        if nat_hours.contains(&hour) {
            plan_builder.require_with_logical_family(&requirement, Some("nat"));
        }
    }
    let plan = plan_builder.build();
    let loaded = if plan.bundles.is_empty() {
        None
    } else {
        Some(load_execution_plan(
            plan,
            &BundleLoaderConfig::new(request.cache_root.clone(), request.use_cache),
        )?)
    };

    let (apcp_by_hour, surface_hour_fetches, fetch_surface_ms, decode_surface_ms) =
        load_apcp_hours_from_plan(loaded.as_ref(), request, &surface_hours)?;
    let (uh_by_hour, uh_hour_fetches, fetch_nat_ms, decode_nat_ms) =
        load_uh_hours_from_plan(loaded.as_ref(), request, &nat_hours)?;

    let product_parallelism = windowed_parallelism(request.source, planned_products.len());
    let date_yyyymmdd = request.date_yyyymmdd.as_str();
    let cycle_utc = latest.cycle.hour_utc;
    let forecast_hour = request.forecast_hour;
    let domain_slug = request.domain.slug.as_str();
    let out_dir = &request.out_dir;
    let model = latest.model;
    let source = latest.source;
    let projected = &projected;
    let grid = &grid;
    let apcp_by_hour = &apcp_by_hour;
    let uh_by_hour = &uh_by_hour;
    let mut outcomes = thread::scope(|scope| -> Result<Vec<WindowedProductOutcome>, io::Error> {
        let mut done = Vec::with_capacity(planned_products.len());
        let mut pending = std::collections::VecDeque::new();

        for (index, &product) in planned_products.iter().enumerate() {
            pending.push_back(
                scope.spawn(move || -> Result<WindowedProductOutcome, io::Error> {
                    let compute_start = Instant::now();
                    let computed = if product.is_qpf() {
                        compute_qpf_product(product, forecast_hour, grid, apcp_by_hour)
                    } else {
                        compute_uh_product(product, forecast_hour, grid, uh_by_hour)
                    };
                    let compute_ms = compute_start.elapsed().as_millis();

                    let computed = match computed {
                        Ok(value) => value,
                        Err(reason) => {
                            return Ok(WindowedProductOutcome::Blocker {
                                index,
                                blocker: HrrrWindowedBlocker { product, reason },
                            });
                        }
                    };

                    let output_path = out_dir.join(format!(
                        "rustwx_hrrr_{}_{}z_f{:03}_{}_{}.png",
                        date_yyyymmdd,
                        cycle_utc,
                        forecast_hour,
                        domain_slug,
                        product.slug()
                    ));
                    let render_start = Instant::now();
                    let mut render_request = if matches!(
                        product,
                        HrrrWindowedProduct::Uh25km1h
                            | HrrrWindowedProduct::Uh25km3h
                            | HrrrWindowedProduct::Uh25kmRunMax
                    ) {
                        MapRenderRequest::for_core_solar07_product(
                            computed.field.clone(),
                            Solar07Product::Uh,
                        )
                    } else {
                        MapRenderRequest::from_core_field(
                            computed.field.clone(),
                            computed.scale.clone(),
                        )
                    };
                    render_request.width = OUTPUT_WIDTH;
                    render_request.height = OUTPUT_HEIGHT;
                    render_request.title = Some(computed.title.clone());
                    render_request.subtitle_left = Some(format!(
                        "{} {}Z F{:03}  {}",
                        date_yyyymmdd, cycle_utc, forecast_hour, model
                    ));
                    render_request.subtitle_right = Some(format!(
                        "source: {} | {}",
                        source, computed.metadata.strategy
                    ));
                    render_request.projected_domain = Some(rustwx_render::ProjectedDomain {
                        x: projected.projected_x.clone(),
                        y: projected.projected_y.clone(),
                        extent: projected.extent.clone(),
                    });
                    render_request.projected_lines = projected.lines.clone();
                    render_request.projected_polygons = projected.polygons.clone();
                    save_png(&render_request, &output_path).map_err(thread_windowed_error)?;
                    let render_ms = render_start.elapsed().as_millis();

                    Ok(WindowedProductOutcome::Rendered {
                        index,
                        rendered: HrrrWindowedRenderedProduct {
                            product,
                            output_path,
                            timing: HrrrWindowedProductTiming {
                                compute_ms,
                                render_ms,
                                total_ms: compute_ms + render_ms,
                            },
                            metadata: computed.metadata,
                        },
                    })
                }),
            );

            if pending.len() >= product_parallelism {
                done.push(join_windowed_job(pending.pop_front().unwrap())?);
            }
        }

        while let Some(handle) = pending.pop_front() {
            done.push(join_windowed_job(handle)?);
        }

        Ok(done)
    })?;
    outcomes.sort_by_key(|outcome| match outcome {
        WindowedProductOutcome::Rendered { index, .. } => *index,
        WindowedProductOutcome::Blocker { index, .. } => *index,
    });
    let mut rendered = Vec::new();
    for outcome in outcomes {
        match outcome {
            WindowedProductOutcome::Rendered { rendered: item, .. } => rendered.push(item),
            WindowedProductOutcome::Blocker { blocker, .. } => blockers.push(blocker),
        }
    }

    Ok(HrrrWindowedBatchReport {
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: latest.source,
        domain: request.domain.clone(),
        shared_timing: HrrrWindowedSharedTiming {
            fetch_geometry_ms,
            decode_geometry_ms,
            project_ms,
            fetch_surface_ms,
            decode_surface_ms,
            fetch_nat_ms,
            decode_nat_ms,
            geometry_fetch_cache_hit,
            geometry_decode_cache_hit,
            surface_hours_loaded: surface_hours.into_iter().collect(),
            nat_hours_loaded: nat_hours.into_iter().collect(),
            geometry_fetch,
            geometry_input_fetch,
            surface_hour_fetches,
            uh_hour_fetches,
        },
        products: rendered,
        blockers,
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn plan_windowed_products(
    products: &[HrrrWindowedProduct],
    forecast_hour: u16,
) -> (
    Vec<HrrrWindowedProduct>,
    Vec<HrrrWindowedBlocker>,
    BTreeSet<u16>,
    BTreeSet<u16>,
) {
    let mut seen = BTreeSet::new();
    let mut planned = Vec::new();
    let mut blockers = Vec::new();
    let mut surface_hours = BTreeSet::new();
    let mut nat_hours = BTreeSet::new();

    for &product in products {
        if !seen.insert(product.slug().to_string()) {
            continue;
        }

        match product {
            HrrrWindowedProduct::Qpf1h => {
                if forecast_hour < 1 {
                    blockers.push(blocker(
                        product,
                        "1-h QPF requires forecast hour >= 1 because HRRR APCP windows start at 0-1 h",
                    ));
                    continue;
                }
                surface_hours.insert(forecast_hour);
            }
            HrrrWindowedProduct::Qpf6h => {
                if forecast_hour < 6 {
                    blockers.push(blocker(product, "6-h QPF requires forecast hour >= 6"));
                    continue;
                }
                surface_hours.extend((forecast_hour - 5)..=forecast_hour);
            }
            HrrrWindowedProduct::Qpf12h => {
                if forecast_hour < 12 {
                    blockers.push(blocker(product, "12-h QPF requires forecast hour >= 12"));
                    continue;
                }
                surface_hours.extend((forecast_hour - 11)..=forecast_hour);
            }
            HrrrWindowedProduct::Qpf24h => {
                if forecast_hour < 24 {
                    blockers.push(blocker(product, "24-h QPF requires forecast hour >= 24"));
                    continue;
                }
                surface_hours.extend((forecast_hour - 23)..=forecast_hour);
            }
            HrrrWindowedProduct::QpfTotal => {
                if forecast_hour < 1 {
                    blockers.push(blocker(product, "total QPF requires forecast hour >= 1"));
                    continue;
                }
                surface_hours.extend(1..=forecast_hour);
            }
            HrrrWindowedProduct::Uh25km1h => {
                if forecast_hour < 1 {
                    blockers.push(blocker(
                        product,
                        "1-h UH max requires forecast hour >= 1 because native UH windows start at 0-1 h",
                    ));
                    continue;
                }
                nat_hours.insert(forecast_hour);
            }
            HrrrWindowedProduct::Uh25km3h => {
                if forecast_hour < 3 {
                    blockers.push(blocker(product, "3-h UH max requires forecast hour >= 3"));
                    continue;
                }
                nat_hours.extend((forecast_hour - 2)..=forecast_hour);
            }
            HrrrWindowedProduct::Uh25kmRunMax => {
                if forecast_hour < 1 {
                    blockers.push(blocker(product, "run-max UH requires forecast hour >= 1"));
                    continue;
                }
                nat_hours.extend(1..=forecast_hour);
            }
        }

        planned.push(product);
    }

    (planned, blockers, surface_hours, nat_hours)
}

fn blocker(product: HrrrWindowedProduct, reason: impl Into<String>) -> HrrrWindowedBlocker {
    HrrrWindowedBlocker {
        product,
        reason: reason.into(),
    }
}

/// Planner-loaded APCP hour decode. The bytes were already fetched by
/// the runtime's `load_execution_plan`; this just wraps the decode +
/// hour-info bookkeeping.
fn load_apcp_hours_from_plan(
    loaded: Option<&LoadedBundleSet>,
    request: &HrrrWindowedBatchRequest,
    hours: &BTreeSet<u16>,
) -> Result<
    (
        BTreeMap<u16, Result<HrrrApcpDecode, String>>,
        Vec<HrrrWindowedHourFetchInfo>,
        u128,
        u128,
    ),
    Box<dyn std::error::Error>,
> {
    let mut out = BTreeMap::new();
    let mut fetches = Vec::new();
    let mut total_fetch_ms = 0u128;
    let mut total_decode_ms = 0u128;

    for &hour in hours {
        let Some(loaded) = loaded else {
            return Err(format!("planner produced no bundles for APCP hour {hour}").into());
        };
        let fetched = find_planner_bundle_for_hour(loaded, hour)?;
        total_fetch_ms += fetched.fetch_ms;
        let decode_path = decode_cache_path(
            &request.cache_root,
            &fetched.file.request,
            "windowed_apcp",
        );
        let decode_start = Instant::now();
        let decode_result = load_or_decode_apcp(
            &decode_path,
            &fetched.file.bytes,
            request.use_cache,
        )
        .map_err(|err| err.to_string());
        total_decode_ms += decode_start.elapsed().as_millis();
        fetches.push(HrrrWindowedHourFetchInfo {
            hour,
            planned_product: "sfc".into(),
            fetched_product: fetched.file.request.request.product.clone(),
            requested_source: fetched
                .file
                .request
                .source_override
                .unwrap_or(fetched.file.fetched.result.source),
            resolved_source: fetched.file.fetched.result.source,
            resolved_url: fetched.file.fetched.result.url.clone(),
            fetch_cache_hit: fetched.file.fetched.cache_hit,
            input_fetch: Some(fetch_identity_from_cached_result(
                "sfc",
                &fetched.file.request,
                &fetched.file.fetched,
            )),
        });
        out.insert(hour, decode_result);
    }
    Ok((out, fetches, total_fetch_ms, total_decode_ms))
}

/// Planner-loaded native UH hour decode. UH messages live in the same
/// wrfsfc file the QPF lane already pulled, so the planner's dedupe
/// means we only fetch each hour once even when both QPF and UH ask for
/// it.
fn load_uh_hours_from_plan(
    loaded: Option<&LoadedBundleSet>,
    request: &HrrrWindowedBatchRequest,
    hours: &BTreeSet<u16>,
) -> Result<
    (
        BTreeMap<u16, Result<HrrrUhDecode, String>>,
        Vec<HrrrWindowedHourFetchInfo>,
        u128,
        u128,
    ),
    Box<dyn std::error::Error>,
> {
    let mut out = BTreeMap::new();
    let mut fetches = Vec::new();
    let mut total_fetch_ms = 0u128;
    let mut total_decode_ms = 0u128;

    for &hour in hours {
        let Some(loaded) = loaded else {
            return Err(format!("planner produced no bundles for UH hour {hour}").into());
        };
        let fetched = find_planner_bundle_for_hour(loaded, hour)?;
        total_fetch_ms += fetched.fetch_ms;
        let decode_path = decode_cache_path(
            &request.cache_root,
            &fetched.file.request,
            "windowed_uh25",
        );
        let decode_start = Instant::now();
        let decode_result = load_or_decode_uh25(
            &decode_path,
            &fetched.file.bytes,
            request.use_cache,
        )
        .map_err(|err| err.to_string());
        total_decode_ms += decode_start.elapsed().as_millis();
        fetches.push(HrrrWindowedHourFetchInfo {
            hour,
            planned_product: "nat".into(),
            fetched_product: fetched.file.request.request.product.clone(),
            requested_source: fetched
                .file
                .request
                .source_override
                .unwrap_or(fetched.file.fetched.result.source),
            resolved_source: fetched.file.fetched.result.source,
            resolved_url: fetched.file.fetched.result.url.clone(),
            fetch_cache_hit: fetched.file.fetched.cache_hit,
            input_fetch: Some(fetch_identity_from_cached_result(
                "nat",
                &fetched.file.request,
                &fetched.file.fetched,
            )),
        });
        out.insert(hour, decode_result);
    }
    Ok((out, fetches, total_fetch_ms, total_decode_ms))
}

fn find_planner_bundle_for_hour<'a>(
    loaded: &'a LoadedBundleSet,
    hour: u16,
) -> Result<&'a FetchedBundleBytes, Box<dyn std::error::Error>> {
    loaded
        .fetched
        .values()
        .find(|bundle| bundle.key.forecast_hour == hour && bundle.key.native_product == "sfc")
        .ok_or_else(|| format!("planner missed windowed hour {hour}").into())
}

fn windowed_parallelism(source: SourceId, job_count: usize) -> usize {
    if matches!(source, SourceId::Nomads) {
        return 1;
    }
    thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(job_count.max(1))
}

fn thread_windowed_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

fn join_windowed_job<T>(
    handle: thread::ScopedJoinHandle<'_, Result<T, io::Error>>,
) -> Result<T, io::Error> {
    match handle.join() {
        Ok(result) => result,
        Err(panic) => Err(io::Error::other(format!(
            "windowed worker panicked: {}",
            panic_message(panic)
        ))),
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_windowed_products_blocks_short_forecast_hours() {
        let (planned, blockers, surface_hours, nat_hours) = plan_windowed_products(
            &[HrrrWindowedProduct::Qpf24h, HrrrWindowedProduct::Uh25km3h],
            2,
        );
        assert!(planned.is_empty());
        assert_eq!(blockers.len(), 2);
        assert!(surface_hours.is_empty());
        assert!(nat_hours.is_empty());
    }

    #[test]
    fn windowed_fetch_truth_can_show_nat_planned_but_sfc_fetched() {
        let fetch = HrrrWindowedHourFetchInfo {
            hour: 1,
            planned_product: "nat".into(),
            fetched_product: "sfc".into(),
            requested_source: SourceId::Nomads,
            resolved_source: SourceId::Nomads,
            resolved_url: "https://example.test/hrrr.t23z.wrfsfcf01.grib2".into(),
            fetch_cache_hit: false,
            input_fetch: None,
        };
        assert_eq!(fetch.planned_product, "nat");
        assert_eq!(fetch.fetched_product, "sfc");
        assert_eq!(fetch.resolved_source, SourceId::Nomads);
        assert!(fetch.resolved_url.contains("wrfsfc"));
    }
}
