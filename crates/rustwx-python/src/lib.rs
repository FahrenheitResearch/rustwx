pub fn python_bindings_enabled() -> bool {
    cfg!(feature = "python")
}

#[cfg(feature = "python")]
mod wrf_render;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};
#[cfg(feature = "python")]
use rustwx_io::{FetchRequest, available_forecast_hours, probe_sources};
#[cfg(feature = "python")]
use rustwx_products::{
    cache::default_proof_cache_dir,
    derived::{
        DerivedBatchReport, DerivedBatchRequest, NativeContourRenderMode,
        is_heavy_derived_recipe_slug, run_derived_batch, supported_derived_recipe_inventory,
        supported_derived_recipe_slugs,
    },
    direct::supported_direct_recipe_slugs,
    named_geometry::{
        NamedGeometryCatalog, NamedGeometryKind, find_built_in_country_domain,
        find_built_in_named_geometry,
    },
    non_ecape::{NonEcapeMultiDomainRequest, run_model_non_ecape_hour_multi_domain},
    places::{PlaceLabelDensityTier, default_place_label_overlay_for_domain},
    shared_context::DomainSpec,
    source::ProductSourceMode,
    windowed::HrrrWindowedProduct,
};
#[cfg(feature = "python")]
use rustwx_render::PngCompressionMode;
#[cfg(feature = "python")]
use rustwx_sounding::{SoundingColumn, write_full_sounding_png};
#[cfg(feature = "python")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use std::collections::HashMap;
#[cfg(feature = "python")]
use std::fs;
#[cfg(feature = "python")]
use std::path::{Path, PathBuf};
#[cfg(feature = "python")]
use std::time::Instant;
#[cfg(feature = "python")]
use wrf_render::{
    build_projected_basemap_overlays, build_projected_basemap_overlays_json,
    describe_projected_geometry, describe_projected_geometry_json, describe_projected_projection,
    describe_projected_projection_json, normalize_cross_section_request,
    normalize_cross_section_request_json, render_projected_map, render_projected_map_json,
    render_wrf_map, render_wrf_map_json,
};

#[cfg(feature = "python")]
#[pyfunction]
fn workspace_name() -> &'static str {
    "rustwx"
}

#[cfg(feature = "python")]
#[pyfunction]
fn list_models_json() -> PyResult<String> {
    serde_json::to_string_pretty(rustwx_models::built_in_models())
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, hour, forecast_hour, product=None))]
fn resolve_urls_json(
    model: &str,
    date: &str,
    hour: u8,
    forecast_hour: u16,
    product: Option<&str>,
) -> PyResult<String> {
    let model: ModelId = model.parse().map_err(|err: rustwx_core::RustwxError| {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    })?;
    let cycle = CycleSpec::new(date, hour)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let default_product = rustwx_models::model_summary(model).default_product;
    let request = ModelRunRequest::new(
        model,
        cycle,
        forecast_hour,
        product.unwrap_or(default_product),
    )
    .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let urls = rustwx_models::resolve_urls(&request)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&urls)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, source=None))]
fn latest_run_json(model: &str, date: &str, source: Option<&str>) -> PyResult<String> {
    let model: ModelId = parse_model(model)?;
    let source = parse_optional_source(source)?;
    let latest = rustwx_models::latest_available_run(model, source, date)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&latest)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, hour, product=None, source=None))]
fn available_forecast_hours_json(
    model: &str,
    date: &str,
    hour: u8,
    product: Option<&str>,
    source: Option<&str>,
) -> PyResult<String> {
    let model: ModelId = parse_model(model)?;
    let source = parse_optional_source(source)?;
    let product = resolve_product(model, product);
    let hours = available_forecast_hours(model, date, hour, &product, source)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&hours)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, hour, forecast_hour, product=None, source=None, variable_patterns=None))]
fn probe_sources_json(
    model: &str,
    date: &str,
    hour: u8,
    forecast_hour: u16,
    product: Option<&str>,
    source: Option<&str>,
    variable_patterns: Option<Vec<String>>,
) -> PyResult<String> {
    let model: ModelId = parse_model(model)?;
    let source = parse_optional_source(source)?;
    let product = resolve_product(model, product);
    let cycle = CycleSpec::new(date, hour)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let request = ModelRunRequest::new(model, cycle, forecast_hour, product)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let fetch_request = FetchRequest {
        request,
        source_override: source,
        variable_patterns: variable_patterns.unwrap_or_default(),
    };
    let probe = probe_sources(&fetch_request)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&probe)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
const AGENT_API_VERSION: &str = "rustwx-agent-v1";
#[cfg(feature = "python")]
const BUILT_IN_MODELS: &[ModelId] = &[
    ModelId::Hrrr,
    ModelId::Gfs,
    ModelId::EcmwfOpenData,
    ModelId::RrfsA,
    ModelId::WrfGdex,
];

#[cfg(feature = "python")]
#[pyfunction]
fn agent_capabilities_json() -> PyResult<String> {
    agent_capabilities_json_impl()
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (request_json))]
fn render_maps_json(request_json: &str) -> PyResult<String> {
    let request: RenderMapsRequestJson = serde_json::from_str(request_json).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid render-maps request: {err}"))
    })?;
    render_maps_json_impl(request)
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (kind=None, limit=None))]
fn list_domains_json(kind: Option<&str>, limit: Option<usize>) -> PyResult<String> {
    let kind = kind.map(parse_named_geometry_kind).transpose()?;
    let mut assets = NamedGeometryCatalog::built_in()
        .iter()
        .filter(|asset| kind.is_none_or(|wanted| asset.kind == wanted))
        .filter(|asset| asset.domain_spec().is_some())
        .map(|asset| {
            serde_json::json!({
                "slug": asset.slug,
                "label": asset.label,
                "kind": named_geometry_kind_slug(asset.kind),
                "groups": asset.groups,
                "tags": asset.tags,
                "bounds": asset.domain_spec().map(|domain| domain.bounds),
            })
        })
        .collect::<Vec<_>>();
    assets.sort_by(|left, right| {
        left["slug"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["slug"].as_str().unwrap_or_default())
    });
    if let Some(limit) = limit {
        assets.truncate(limit);
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "count": assets.len(),
        "domains": assets
    }))
    .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
fn cli_main(py: Python<'_>) -> PyResult<i32> {
    let argv = py
        .import("sys")?
        .getattr("argv")?
        .extract::<Vec<String>>()?;
    match run_agent_cli(&argv) {
        Ok(code) => Ok(code),
        Err(err) => {
            eprintln!("rustwx: {err}");
            Ok(2)
        }
    }
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, Default, Deserialize)]
struct RenderMapsRequestJson {
    #[serde(default)]
    model: Option<String>,
    #[serde(default, alias = "date")]
    date_yyyymmdd: Option<String>,
    #[serde(default, alias = "cycle")]
    cycle_utc: Option<u8>,
    #[serde(default, alias = "forecastHour")]
    forecast_hour: Option<u16>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    domains: Option<Vec<String>>,
    #[serde(default)]
    bounds: Option<Vec<f64>>,
    #[serde(default, alias = "out")]
    out_dir: Option<PathBuf>,
    #[serde(default)]
    cache_dir: Option<PathBuf>,
    #[serde(default)]
    use_cache: Option<bool>,
    #[serde(default)]
    no_cache: Option<bool>,
    #[serde(default)]
    source_mode: Option<String>,
    #[serde(default)]
    products: Option<Vec<String>>,
    #[serde(default)]
    direct_recipes: Option<Vec<String>>,
    #[serde(default)]
    derived_recipes: Option<Vec<String>>,
    #[serde(default, alias = "width")]
    output_width: Option<u32>,
    #[serde(default, alias = "height")]
    output_height: Option<u32>,
    #[serde(default)]
    place_label_density: Option<String>,
    #[serde(default)]
    direct_product_overrides: HashMap<String, String>,
    #[serde(default)]
    surface_product_override: Option<String>,
    #[serde(default)]
    pressure_product_override: Option<String>,
    #[serde(default)]
    allow_large_heavy_domain: Option<bool>,
    #[serde(default)]
    windowed_products: Option<Vec<String>>,
    #[serde(default)]
    domain_jobs: Option<usize>,
}

#[cfg(feature = "python")]
#[derive(Debug, Clone)]
struct RenderMapsPlan {
    request: NonEcapeMultiDomainRequest,
    heavy_derived_recipe_slugs: Vec<String>,
    place_density: PlaceLabelDensityTier,
}

#[cfg(feature = "python")]
#[derive(Debug, Clone)]
struct RoutedRenderProducts {
    direct_recipe_slugs: Vec<String>,
    derived_recipe_slugs: Vec<String>,
    heavy_derived_recipe_slugs: Vec<String>,
    windowed_products: Vec<HrrrWindowedProduct>,
}

#[cfg(feature = "python")]
fn agent_capabilities_json_impl() -> PyResult<String> {
    let models = BUILT_IN_MODELS
        .iter()
        .copied()
        .map(|model| {
            let derived_inventory = supported_derived_recipe_inventory();
            serde_json::json!({
                "id": model.as_str(),
                "default_product": rustwx_models::model_summary(model).default_product,
                "default_render_product": default_render_product(model),
                "direct_recipes": supported_direct_recipe_slugs(model),
                "derived_recipes": supported_derived_recipe_slugs(model),
                "light_derived_recipes": derived_inventory
                    .iter()
                    .filter(|recipe| !recipe.heavy)
                    .map(|recipe| recipe.slug)
                    .collect::<Vec<_>>(),
                "heavy_derived_recipes": derived_inventory
                    .iter()
                    .filter(|recipe| recipe.heavy)
                    .map(|recipe| recipe.slug)
                    .collect::<Vec<_>>(),
                "windowed_products": if model == ModelId::Hrrr {
                    supported_windowed_product_slugs()
                } else {
                    Vec::<String>::new()
                },
            })
        })
        .collect::<Vec<_>>();
    let domains = NamedGeometryCatalog::built_in();
    let payload = serde_json::json!({
        "package": "rustwx",
        "version": env!("CARGO_PKG_VERSION"),
        "agent_api": AGENT_API_VERSION,
        "entrypoints": {
            "python": [
                "rustwx.agent_capabilities_json()",
                "rustwx.list_domains_json(kind=None, limit=None)",
                "rustwx.render_maps_json(request_json)"
            ],
            "console_scripts": [
                {
                    "name": "rustwx",
                    "commands": ["capabilities", "list-domains", "render-maps"]
                }
            ]
        },
        "models": models,
        "domains": {
            "count": domains.len(),
            "kinds": ["country", "region", "metro", "watch_area"],
            "lookup": "pass domain=<slug> for any built-in bounded asset, or country=<iso/name> via domain"
        },
        "render_maps_request_schema": {
            "model": "optional model id; default hrrr",
            "date_yyyymmdd": "YYYYMMDD, required",
            "cycle_utc": "optional integer UTC cycle; omitted means latest available for date/forecast_hour/source",
            "forecast_hour": "optional integer forecast hour; default 0",
            "source": "optional source id; default nomads",
            "domain": "optional built-in domain/country/metro slug; default conus",
            "domains": "optional list of built-in domain/country/metro slugs",
            "bounds": "optional [west,east,south,north] custom domain override",
            "products": "optional mixed product slugs; rustwx routes to direct, derived, or HRRR windowed products",
            "direct_recipes": "optional explicit direct product slugs",
            "derived_recipes": "optional explicit derived product slugs; heavy ECAPE recipes route through the canonical derived_batch ECAPE path",
            "windowed_products": "optional explicit HRRR windowed product slugs",
            "out_dir": "optional output directory",
            "place_label_density": "none, major, major-and-aux, or dense"
        }
    });
    serde_json::to_string_pretty(&payload)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
fn render_maps_json_impl(request: RenderMapsRequestJson) -> PyResult<String> {
    let plan = build_render_maps_plan(request)?;
    let report = run_render_maps_plan(plan)?;
    serde_json::to_string_pretty(&report)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
fn run_render_maps_plan(plan: RenderMapsPlan) -> PyResult<serde_json::Value> {
    let non_ecape_work = !plan.request.direct_recipe_slugs.is_empty()
        || !plan.request.derived_recipe_slugs.is_empty()
        || !plan.request.windowed_products.is_empty();
    if plan.heavy_derived_recipe_slugs.is_empty() {
        let report = run_model_non_ecape_hour_multi_domain(&plan.request)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        return serde_json::to_value(&report)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
    }

    let total_start = Instant::now();
    let non_ecape_report = if non_ecape_work {
        Some(
            run_model_non_ecape_hour_multi_domain(&plan.request)
                .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?,
        )
    } else {
        None
    };
    let pinned = non_ecape_report.as_ref().map(|report| {
        (
            report.date_yyyymmdd.clone(),
            Some(report.cycle_utc),
            report.source,
        )
    });
    let heavy_reports = run_heavy_derived_domains(&plan, pinned)?;
    let total_ms = total_start.elapsed().as_millis();

    if let Some(report) = non_ecape_report {
        let mut value = serde_json::to_value(&report)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        attach_heavy_derived_reports(
            &mut value,
            &plan.heavy_derived_recipe_slugs,
            &heavy_reports,
            total_ms,
        )?;
        Ok(value)
    } else {
        heavy_only_render_maps_report(&plan, &heavy_reports, total_ms)
    }
}

#[cfg(feature = "python")]
fn build_render_maps_plan(request: RenderMapsRequestJson) -> PyResult<RenderMapsPlan> {
    let model = request.model.as_deref().unwrap_or("hrrr").parse().map_err(
        |err: rustwx_core::RustwxError| pyo3::exceptions::PyValueError::new_err(err.to_string()),
    )?;
    let date_yyyymmdd = request.date_yyyymmdd.clone().ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("render-maps request requires date_yyyymmdd")
    })?;
    let source = request
        .source
        .as_deref()
        .unwrap_or("nomads")
        .parse()
        .map_err(|err: rustwx_core::RustwxError| {
            pyo3::exceptions::PyValueError::new_err(err.to_string())
        })?;
    let domains = render_domains(&request)?;
    let out_dir = request
        .out_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("rustwx_outputs").join("maps"));
    let cache_root = request
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(&out_dir));
    let use_cache = request.use_cache.unwrap_or(true) && !request.no_cache.unwrap_or(false);
    let source_mode = parse_product_source_mode(request.source_mode.as_deref())?;
    let place_density = parse_place_label_density(request.place_label_density.as_deref())?;
    let routed = route_requested_products(model, &request)?;
    let place_label_overlay = domains
        .first()
        .and_then(|domain| default_place_label_overlay_for_domain(domain, place_density));

    Ok(RenderMapsPlan {
        heavy_derived_recipe_slugs: routed.heavy_derived_recipe_slugs,
        place_density,
        request: NonEcapeMultiDomainRequest {
            model,
            date_yyyymmdd,
            cycle_override_utc: request.cycle_utc,
            forecast_hour: request.forecast_hour.unwrap_or(0),
            source,
            domains,
            out_dir,
            cache_root,
            use_cache,
            source_mode,
            direct_recipe_slugs: routed.direct_recipe_slugs,
            derived_recipe_slugs: routed.derived_recipe_slugs,
            direct_product_overrides: request.direct_product_overrides.clone(),
            surface_product_override: request.surface_product_override.clone(),
            pressure_product_override: request.pressure_product_override.clone(),
            allow_large_heavy_domain: request.allow_large_heavy_domain.unwrap_or(false),
            windowed_products: routed.windowed_products,
            output_width: request.output_width.unwrap_or(1400),
            output_height: request.output_height.unwrap_or(1100),
            png_compression: PngCompressionMode::Fast,
            custom_poi_overlay: None,
            place_label_overlay,
            domain_jobs: request.domain_jobs,
        },
    })
}

#[cfg(feature = "python")]
fn run_heavy_derived_domains(
    plan: &RenderMapsPlan,
    initial_pin: Option<(String, Option<u8>, SourceId)>,
) -> PyResult<Vec<DerivedBatchReport>> {
    let mut pinned = initial_pin;
    let mut reports = Vec::with_capacity(plan.request.domains.len());
    for domain in &plan.request.domains {
        let (date_yyyymmdd, cycle_override_utc, source) = pinned.clone().unwrap_or_else(|| {
            (
                plan.request.date_yyyymmdd.clone(),
                plan.request.cycle_override_utc,
                plan.request.source,
            )
        });
        let request = DerivedBatchRequest {
            model: plan.request.model,
            date_yyyymmdd,
            cycle_override_utc,
            forecast_hour: plan.request.forecast_hour,
            source,
            domain: domain.clone(),
            out_dir: plan.request.out_dir.join(&domain.slug),
            cache_root: plan.request.cache_root.clone(),
            use_cache: plan.request.use_cache,
            recipe_slugs: plan.heavy_derived_recipe_slugs.clone(),
            surface_product_override: plan.request.surface_product_override.clone(),
            pressure_product_override: plan.request.pressure_product_override.clone(),
            source_mode: plan.request.source_mode,
            allow_large_heavy_domain: plan.request.allow_large_heavy_domain,
            contour_mode: NativeContourRenderMode::Automatic,
            native_fill_level_multiplier: 1,
            output_width: plan.request.output_width,
            output_height: plan.request.output_height,
            png_compression: plan.request.png_compression,
            custom_poi_overlay: plan.request.custom_poi_overlay.clone(),
            place_label_overlay: default_place_label_overlay_for_domain(domain, plan.place_density),
        };
        let report = run_derived_batch(&request)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        if pinned.is_none() {
            pinned = Some((
                report.date_yyyymmdd.clone(),
                Some(report.cycle_utc),
                report.source,
            ));
        }
        reports.push(report);
    }
    Ok(reports)
}

#[cfg(feature = "python")]
fn attach_heavy_derived_reports(
    value: &mut serde_json::Value,
    heavy_recipe_slugs: &[String],
    heavy_reports: &[DerivedBatchReport],
    total_ms: u128,
) -> PyResult<()> {
    let heavy_summary = heavy_derived_summary_value(heavy_recipe_slugs, heavy_reports, total_ms)?;
    let reports_by_domain = heavy_reports
        .iter()
        .map(|report| {
            serde_json::to_value(report)
                .map(|value| (report.domain.slug.clone(), value))
                .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
        })
        .collect::<PyResult<HashMap<_, _>>>()?;

    let Some(object) = value.as_object_mut() else {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "render-maps report was not a JSON object",
        ));
    };
    object.insert(
        "agent_api".to_string(),
        serde_json::json!(AGENT_API_VERSION),
    );
    object.insert(
        "run_kind".to_string(),
        serde_json::json!("render_maps_mixed"),
    );
    object.insert("agent_total_ms".to_string(), serde_json::json!(total_ms));
    object.insert("heavy_derived".to_string(), heavy_summary);

    if let Some(domains) = object
        .get_mut("domains")
        .and_then(serde_json::Value::as_array_mut)
    {
        for domain_value in domains {
            let Some(domain_object) = domain_value.as_object_mut() else {
                continue;
            };
            let slug = domain_object
                .get("domain")
                .and_then(|domain| domain.get("slug"))
                .and_then(serde_json::Value::as_str);
            if let Some(report) = slug.and_then(|slug| reports_by_domain.get(slug)) {
                domain_object.insert("heavy_derived".to_string(), report.clone());
            }
        }
    }
    Ok(())
}

#[cfg(feature = "python")]
fn heavy_only_render_maps_report(
    plan: &RenderMapsPlan,
    heavy_reports: &[DerivedBatchReport],
    total_ms: u128,
) -> PyResult<serde_json::Value> {
    let first = heavy_reports.first();
    let domains = heavy_reports
        .iter()
        .map(|report| {
            serde_json::to_value(report).map(|report_value| {
                serde_json::json!({
                    "domain": report.domain,
                    "summary": {
                        "png_count": report.recipes.len(),
                        "blocker_count": report.blockers.len(),
                    },
                    "derived": report_value.clone(),
                    "heavy_derived": report_value,
                    "total_ms": report.total_ms,
                })
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;

    let value = serde_json::json!({
        "agent_api": AGENT_API_VERSION,
        "run_kind": "render_maps_heavy_derived",
        "model": plan.request.model,
        "date_yyyymmdd": first
            .map(|report| report.date_yyyymmdd.as_str())
            .unwrap_or(plan.request.date_yyyymmdd.as_str()),
        "cycle_utc": first
            .map(|report| report.cycle_utc)
            .or(plan.request.cycle_override_utc),
        "forecast_hour": plan.request.forecast_hour,
        "source": first
            .map(|report| report.source)
            .unwrap_or(plan.request.source),
        "out_dir": plan.request.out_dir,
        "cache_root": plan.request.cache_root,
        "use_cache": plan.request.use_cache,
        "source_mode": plan.request.source_mode,
        "requested": {
            "direct_recipe_slugs": Vec::<String>::new(),
            "derived_recipe_slugs": plan.heavy_derived_recipe_slugs,
            "windowed_products": Vec::<String>::new(),
        },
        "domains": domains,
        "heavy_derived": heavy_derived_summary_value(
            &plan.heavy_derived_recipe_slugs,
            heavy_reports,
            total_ms,
        )?,
        "agent_total_ms": total_ms,
        "total_ms": total_ms,
    });
    Ok(value)
}

#[cfg(feature = "python")]
fn heavy_derived_summary_value(
    heavy_recipe_slugs: &[String],
    heavy_reports: &[DerivedBatchReport],
    total_ms: u128,
) -> PyResult<serde_json::Value> {
    let domains = heavy_reports
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    let output_count = heavy_reports
        .iter()
        .map(|report| report.recipes.len())
        .sum::<usize>();
    let blocker_count = heavy_reports
        .iter()
        .map(|report| report.blockers.len())
        .sum::<usize>();
    Ok(serde_json::json!({
        "runner": "derived_batch",
        "route": "heavy_ecape",
        "source_mode": heavy_reports
            .first()
            .map(|report| report.source_mode)
            .unwrap_or(ProductSourceMode::Canonical),
        "requested_recipe_slugs": heavy_recipe_slugs,
        "domain_count": heavy_reports.len(),
        "output_count": output_count,
        "blocker_count": blocker_count,
        "domains": domains,
        "total_ms": total_ms,
    }))
}

#[cfg(feature = "python")]
fn render_domains(request: &RenderMapsRequestJson) -> PyResult<Vec<DomainSpec>> {
    if let Some(bounds) = &request.bounds {
        let slug = request
            .domain
            .as_deref()
            .or(request.region.as_deref())
            .unwrap_or("custom");
        return Ok(vec![bounds_domain(slug, bounds.as_slice())?]);
    }
    if let Some(domains) = &request.domains {
        let resolved = domains
            .iter()
            .map(|domain| resolve_named_domain(domain))
            .collect::<PyResult<Vec<_>>>()?;
        if resolved.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "domains must not be empty",
            ));
        }
        return Ok(resolved);
    }
    let region = request
        .domain
        .as_deref()
        .or(request.region.as_deref())
        .unwrap_or("conus");
    Ok(vec![resolve_named_domain(region)?])
}

#[cfg(feature = "python")]
fn resolve_named_domain(value: &str) -> PyResult<DomainSpec> {
    let slug = normalize_slug(value);
    if let Some(asset) = find_built_in_named_geometry(&slug) {
        if let Some(domain) = asset.domain_spec() {
            return Ok(domain);
        }
    }
    if let Some(domain) =
        find_built_in_country_domain(value).or_else(|| find_built_in_country_domain(&slug))
    {
        return Ok(domain);
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "unknown bounded domain '{value}'; use rustwx list-domains or pass bounds=[west,east,south,north]"
    )))
}

#[cfg(feature = "python")]
fn route_requested_products(
    model: ModelId,
    request: &RenderMapsRequestJson,
) -> PyResult<RoutedRenderProducts> {
    let mut direct = request.direct_recipes.clone().unwrap_or_default();
    let mut derived = request.derived_recipes.clone().unwrap_or_default();
    let mut windowed = request
        .windowed_products
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|slug| parse_windowed_product(slug))
        .collect::<PyResult<Vec<_>>>()?;

    if let Some(products) = &request.products {
        let supported_direct = supported_direct_recipe_slugs(model);
        let supported_derived = supported_derived_recipe_slugs(model);
        for product in products {
            let normalized = normalize_slug(product);
            if supported_direct.iter().any(|slug| slug == &normalized) {
                push_unique(&mut direct, normalized);
            } else if supported_derived.iter().any(|slug| slug == &normalized) {
                push_unique(&mut derived, normalized);
            } else if let Ok(windowed_product) = parse_windowed_product(&normalized) {
                push_unique_windowed(&mut windowed, windowed_product);
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown or unsupported product '{product}' for model {model}"
                )));
            }
        }
    }

    if direct.is_empty() && derived.is_empty() && windowed.is_empty() {
        direct.push(default_render_product(model));
    }
    if !windowed.is_empty() && model != ModelId::Hrrr {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "windowed_products are currently HRRR-only",
        ));
    }
    let (derived, heavy_derived) = split_heavy_derived_recipes(derived);
    Ok(RoutedRenderProducts {
        direct_recipe_slugs: direct,
        derived_recipe_slugs: derived,
        heavy_derived_recipe_slugs: heavy_derived,
        windowed_products: windowed,
    })
}

#[cfg(feature = "python")]
fn split_heavy_derived_recipes(recipe_slugs: Vec<String>) -> (Vec<String>, Vec<String>) {
    let mut light = Vec::new();
    let mut heavy = Vec::new();
    for slug in recipe_slugs {
        if is_heavy_derived_recipe_slug(&slug) {
            push_unique(&mut heavy, slug);
        } else {
            push_unique(&mut light, slug);
        }
    }
    (light, heavy)
}

#[cfg(feature = "python")]
fn default_render_product(model: ModelId) -> String {
    let direct = supported_direct_recipe_slugs(model);
    for preferred in [
        "2m_temperature_10m_winds",
        "500mb_height_winds",
        "mslp_10m_winds",
    ] {
        if direct.iter().any(|slug| slug == preferred) {
            return preferred.to_string();
        }
    }
    direct
        .into_iter()
        .next()
        .unwrap_or_else(|| "2m_temperature_10m_winds".to_string())
}

#[cfg(feature = "python")]
fn bounds_domain(slug: &str, bounds: &[f64]) -> PyResult<DomainSpec> {
    if bounds.len() != 4 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "bounds must be [west,east,south,north]",
        ));
    }
    let west = bounds[0];
    let east = bounds[1];
    let south = bounds[2];
    let north = bounds[3];
    if !west.is_finite()
        || !east.is_finite()
        || !south.is_finite()
        || !north.is_finite()
        || south >= north
        || south < -90.0
        || north > 90.0
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "bounds must be finite [west,east,south,north] values with south < north and valid latitudes",
        ));
    }
    Ok(DomainSpec::new(
        normalize_slug(slug),
        (west, east, south, north),
    ))
}

#[cfg(feature = "python")]
fn normalize_slug(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

#[cfg(feature = "python")]
fn parse_product_source_mode(value: Option<&str>) -> PyResult<ProductSourceMode> {
    match value.unwrap_or("canonical").to_ascii_lowercase().as_str() {
        "canonical" => Ok(ProductSourceMode::Canonical),
        "fastest" => Ok(ProductSourceMode::Fastest),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported source_mode '{other}'; expected canonical or fastest"
        ))),
    }
}

#[cfg(feature = "python")]
fn parse_place_label_density(value: Option<&str>) -> PyResult<PlaceLabelDensityTier> {
    match value
        .unwrap_or("major-and-aux")
        .to_ascii_lowercase()
        .replace('_', "-")
        .as_str()
    {
        "none" | "off" | "0" => Ok(PlaceLabelDensityTier::None),
        "major" | "1" => Ok(PlaceLabelDensityTier::Major),
        "major-and-aux" | "major+aux" | "2" => Ok(PlaceLabelDensityTier::MajorAndAux),
        "dense" | "full" | "3" => Ok(PlaceLabelDensityTier::Dense),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported place_label_density '{other}'"
        ))),
    }
}

#[cfg(feature = "python")]
fn parse_named_geometry_kind(value: &str) -> PyResult<NamedGeometryKind> {
    match normalize_slug(value).as_str() {
        "country" => Ok(NamedGeometryKind::Country),
        "metro" | "city" => Ok(NamedGeometryKind::Metro),
        "region" => Ok(NamedGeometryKind::Region),
        "watch_area" | "watch" => Ok(NamedGeometryKind::WatchArea),
        "route" => Ok(NamedGeometryKind::Route),
        "other" => Ok(NamedGeometryKind::Other),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported domain kind '{other}'"
        ))),
    }
}

#[cfg(feature = "python")]
fn named_geometry_kind_slug(kind: NamedGeometryKind) -> &'static str {
    match kind {
        NamedGeometryKind::Country => "country",
        NamedGeometryKind::Metro => "metro",
        NamedGeometryKind::Region => "region",
        NamedGeometryKind::WatchArea => "watch_area",
        NamedGeometryKind::Route => "route",
        NamedGeometryKind::Other => "other",
    }
}

#[cfg(feature = "python")]
fn parse_windowed_product(value: &str) -> PyResult<HrrrWindowedProduct> {
    match normalize_slug(value).as_str() {
        "qpf_1h" | "qpf1h" => Ok(HrrrWindowedProduct::Qpf1h),
        "qpf_6h" | "qpf6h" => Ok(HrrrWindowedProduct::Qpf6h),
        "qpf_12h" | "qpf12h" => Ok(HrrrWindowedProduct::Qpf12h),
        "qpf_24h" | "qpf24h" => Ok(HrrrWindowedProduct::Qpf24h),
        "qpf_total" => Ok(HrrrWindowedProduct::QpfTotal),
        "uh_2to5km_1h_max" | "uh25km_1h" | "uh_1h" => Ok(HrrrWindowedProduct::Uh25km1h),
        "uh_2to5km_3h_max" | "uh25km_3h" | "uh_3h" => Ok(HrrrWindowedProduct::Uh25km3h),
        "uh_2to5km_run_max" | "uh25km_run_max" | "uh_run_max" => {
            Ok(HrrrWindowedProduct::Uh25kmRunMax)
        }
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported windowed product '{other}'"
        ))),
    }
}

#[cfg(feature = "python")]
fn supported_windowed_product_slugs() -> Vec<String> {
    [
        HrrrWindowedProduct::Qpf1h,
        HrrrWindowedProduct::Qpf6h,
        HrrrWindowedProduct::Qpf12h,
        HrrrWindowedProduct::Qpf24h,
        HrrrWindowedProduct::QpfTotal,
        HrrrWindowedProduct::Uh25km1h,
        HrrrWindowedProduct::Uh25km3h,
        HrrrWindowedProduct::Uh25kmRunMax,
    ]
    .into_iter()
    .map(|product| product.slug().to_string())
    .collect()
}

#[cfg(feature = "python")]
fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

#[cfg(feature = "python")]
fn push_unique_windowed(values: &mut Vec<HrrrWindowedProduct>, value: HrrrWindowedProduct) {
    if !values.contains(&value) {
        values.push(value);
    }
}

#[cfg(feature = "python")]
fn run_agent_cli(argv: &[String]) -> Result<i32, String> {
    let args = argv.get(1..).unwrap_or(&[]);
    let Some(command) = args.first().map(String::as_str) else {
        print_agent_help();
        return Ok(0);
    };
    match command {
        "-h" | "--help" | "help" => {
            print_agent_help();
            Ok(0)
        }
        "-V" | "--version" | "version" => {
            println!("rustwx {}", env!("CARGO_PKG_VERSION"));
            Ok(0)
        }
        "capabilities" => {
            println!(
                "{}",
                agent_capabilities_json_impl().map_err(|err| err.to_string())?
            );
            Ok(0)
        }
        "list-domains" | "domains" => {
            if args[1..]
                .iter()
                .any(|arg| matches!(arg.as_str(), "-h" | "--help"))
            {
                print_list_domains_help();
                return Ok(0);
            }
            let (kind, limit) = list_domains_args_from_cli(&args[1..])?;
            println!(
                "{}",
                list_domains_json(kind.as_deref(), limit).map_err(|err| err.to_string())?
            );
            Ok(0)
        }
        "render-maps" | "render-map" => {
            if args[1..]
                .iter()
                .any(|arg| matches!(arg.as_str(), "-h" | "--help"))
            {
                print_render_maps_help();
                return Ok(0);
            }
            let request = render_maps_request_from_cli(&args[1..])?;
            println!(
                "{}",
                render_maps_json_impl(request).map_err(|err| err.to_string())?
            );
            Ok(0)
        }
        other => Err(format!("unknown command '{other}'")),
    }
}

#[cfg(feature = "python")]
fn print_agent_help() {
    println!(
        "rustwx {}\n\nUSAGE:\n  rustwx capabilities\n  rustwx list-domains [--kind country|region|metro|watch-area] [--limit N]\n  rustwx render-maps --date YYYYMMDD [--model hrrr] [--cycle H] [--forecast-hour H] [--domain conus] [--product PRODUCT] [--out-dir DIR]\n  rustwx render-maps --request request.json\n\nPython API: rustwx.agent_capabilities_json(), rustwx.list_domains_json(), rustwx.render_maps_json(request_json).",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(feature = "python")]
fn print_list_domains_help() {
    println!("USAGE:\n  rustwx list-domains [--kind country|region|metro|watch-area] [--limit N]");
}

#[cfg(feature = "python")]
fn print_render_maps_help() {
    println!(
        "USAGE:\n  rustwx render-maps --date YYYYMMDD [--model hrrr] [--cycle H] [--forecast-hour H] [--domain conus] [--product PRODUCT] [--out-dir DIR]\n  rustwx render-maps --request request.json\n\nOptions include --source, --bounds west,east,south,north, --direct-recipe, --derived-recipe, --windowed-product, --place-label-density, --width, --height, --cache-dir, --no-cache."
    );
}

#[cfg(feature = "python")]
fn list_domains_args_from_cli(args: &[String]) -> Result<(Option<String>, Option<usize>), String> {
    let mut kind = None;
    let mut limit = None;
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "-h" | "--help" => {
                print_list_domains_help();
                return Err("help requested".to_string());
            }
            "--kind" => kind = Some(next_cli_value(args, &mut index, arg)?),
            "--limit" => limit = Some(parse_cli_value(args, &mut index, arg)?),
            other => return Err(format!("unknown list-domains option '{other}'")),
        }
        index += 1;
    }
    Ok((kind, limit))
}

#[cfg(feature = "python")]
fn render_maps_request_from_cli(args: &[String]) -> Result<RenderMapsRequestJson, String> {
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "-h" | "--help"))
    {
        print_render_maps_help();
        return Err("help requested".to_string());
    }

    let mut request = RenderMapsRequestJson::default();
    let mut products = Vec::<String>::new();
    let mut direct_recipes = Vec::<String>::new();
    let mut derived_recipes = Vec::<String>::new();
    let mut windowed_products = Vec::<String>::new();
    let mut domains = Vec::<String>::new();
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--request" => {
                let path = next_cli_value(args, &mut index, arg)?;
                let payload = fs::read_to_string(&path)
                    .map_err(|err| format!("failed to read request file '{path}': {err}"))?;
                return serde_json::from_str(&payload)
                    .map_err(|err| format!("invalid request JSON in '{path}': {err}"));
            }
            "--request-json" => {
                let payload = next_cli_value(args, &mut index, arg)?;
                return serde_json::from_str(&payload)
                    .map_err(|err| format!("invalid request JSON: {err}"));
            }
            "--model" => request.model = Some(next_cli_value(args, &mut index, arg)?),
            "--date" | "--date-yyyymmdd" => {
                request.date_yyyymmdd = Some(next_cli_value(args, &mut index, arg)?);
            }
            "--cycle" | "--cycle-utc" => {
                request.cycle_utc = Some(parse_cli_value(args, &mut index, arg)?)
            }
            "--forecast-hour" | "--hour" => {
                request.forecast_hour = Some(parse_cli_value(args, &mut index, arg)?);
            }
            "--source" => request.source = Some(next_cli_value(args, &mut index, arg)?),
            "--domain" | "--region" => {
                extend_comma_values(&mut domains, &next_cli_value(args, &mut index, arg)?);
            }
            "--bounds" => {
                let raw = next_cli_value(args, &mut index, arg)?;
                request.bounds = Some(parse_comma_f64s(&raw, "--bounds")?);
            }
            "--product" | "--products" => {
                extend_comma_values(&mut products, &next_cli_value(args, &mut index, arg)?);
            }
            "--direct-recipe" | "--direct-recipes" => {
                extend_comma_values(&mut direct_recipes, &next_cli_value(args, &mut index, arg)?);
            }
            "--derived-recipe" | "--derived-recipes" => {
                extend_comma_values(
                    &mut derived_recipes,
                    &next_cli_value(args, &mut index, arg)?,
                );
            }
            "--windowed-product" | "--windowed-products" => {
                extend_comma_values(
                    &mut windowed_products,
                    &next_cli_value(args, &mut index, arg)?,
                );
            }
            "--out-dir" | "--out" => {
                request.out_dir = Some(PathBuf::from(next_cli_value(args, &mut index, arg)?))
            }
            "--cache-dir" => {
                request.cache_dir = Some(PathBuf::from(next_cli_value(args, &mut index, arg)?))
            }
            "--no-cache" => request.no_cache = Some(true),
            "--source-mode" => request.source_mode = Some(next_cli_value(args, &mut index, arg)?),
            "--width" => request.output_width = Some(parse_cli_value(args, &mut index, arg)?),
            "--height" => request.output_height = Some(parse_cli_value(args, &mut index, arg)?),
            "--place-label-density" => {
                request.place_label_density = Some(next_cli_value(args, &mut index, arg)?);
            }
            "--allow-large-heavy-domain" => request.allow_large_heavy_domain = Some(true),
            "--domain-jobs" => request.domain_jobs = Some(parse_cli_value(args, &mut index, arg)?),
            other => return Err(format!("unknown render-maps option '{other}'")),
        }
        index += 1;
    }

    if domains.len() == 1 {
        request.domain = domains.pop();
    } else if !domains.is_empty() {
        request.domains = Some(domains);
    }
    if !products.is_empty() {
        request.products = Some(products);
    }
    if !direct_recipes.is_empty() {
        request.direct_recipes = Some(direct_recipes);
    }
    if !derived_recipes.is_empty() {
        request.derived_recipes = Some(derived_recipes);
    }
    if !windowed_products.is_empty() {
        request.windowed_products = Some(windowed_products);
    }
    Ok(request)
}

#[cfg(feature = "python")]
fn next_cli_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

#[cfg(feature = "python")]
fn parse_cli_value<T>(args: &[String], index: &mut usize, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = next_cli_value(args, index, flag)?;
    value
        .parse::<T>()
        .map_err(|err| format!("invalid {flag} value '{value}': {err}"))
}

#[cfg(feature = "python")]
fn parse_comma_f64s(raw: &str, flag: &str) -> Result<Vec<f64>, String> {
    raw.split(',')
        .map(|part| {
            part.trim()
                .parse::<f64>()
                .map_err(|err| format!("invalid {flag} component '{part}': {err}"))
        })
        .collect()
}

#[cfg(feature = "python")]
fn extend_comma_values(values: &mut Vec<String>, raw: &str) {
    values.extend(
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
    );
}

#[cfg(feature = "python")]
#[derive(Debug, Serialize)]
struct SoundingRenderResult {
    renderer: &'static str,
    output_path: String,
    levels: usize,
    station_id: String,
    valid_time: String,
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (column_json, output_path))]
fn render_sounding_column_json(column_json: &str, output_path: &str) -> PyResult<String> {
    let result =
        render_sounding_column_impl(parse_sounding_column_json(column_json)?, output_path)?;
    serde_json::to_string_pretty(&result)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (column, output_path))]
fn render_sounding_column(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    output_path: &str,
) -> PyResult<Py<PyAny>> {
    let result = render_sounding_column_impl(
        parse_sounding_column_json(&python_value_to_json(column)?)?,
        output_path,
    )?;
    json_to_python(
        py,
        &serde_json::to_string_pretty(&result)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?,
    )
}

#[cfg(feature = "python")]
fn render_sounding_column_impl(
    column: SoundingColumn,
    output_path: &str,
) -> PyResult<SoundingRenderResult> {
    if let Some(parent) = Path::new(output_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    }
    write_full_sounding_png(&column, output_path)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    Ok(SoundingRenderResult {
        renderer: "rustwx-sounding native Rust SHARPpy-style renderer",
        output_path: output_path.to_string(),
        levels: column.len(),
        station_id: column.metadata.station_id,
        valid_time: column.metadata.valid_time,
    })
}

#[cfg(feature = "python")]
fn parse_sounding_column_json(payload: &str) -> PyResult<SoundingColumn> {
    serde_json::from_str(payload).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid sounding column: {err}"))
    })
}

#[cfg(feature = "python")]
fn python_value_to_json(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(text) = value.extract::<String>() {
        return Ok(text);
    }
    let json_module = pyo3::types::PyModule::import(value.py(), "json")?;
    json_module
        .getattr("dumps")?
        .call1((value,))?
        .extract::<String>()
}

#[cfg(feature = "python")]
fn json_to_python(py: Python<'_>, payload: &str) -> PyResult<Py<PyAny>> {
    let json_module = pyo3::types::PyModule::import(py, "json")?;
    Ok(json_module.getattr("loads")?.call1((payload,))?.unbind())
}

#[cfg(feature = "python")]
fn parse_model(model: &str) -> PyResult<ModelId> {
    model.parse().map_err(|err: rustwx_core::RustwxError| {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    })
}

#[cfg(feature = "python")]
fn parse_optional_source(source: Option<&str>) -> PyResult<Option<SourceId>> {
    source
        .map(|value| {
            value.parse().map_err(|err: rustwx_core::RustwxError| {
                pyo3::exceptions::PyValueError::new_err(err.to_string())
            })
        })
        .transpose()
}

#[cfg(feature = "python")]
fn resolve_product(model: ModelId, product: Option<&str>) -> String {
    product
        .unwrap_or(rustwx_models::model_summary(model).default_product)
        .to_string()
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use super::*;

    #[test]
    fn render_maps_router_splits_heavy_ecape_from_non_ecape_work() {
        let request = RenderMapsRequestJson {
            products: Some(vec![
                "mlecape".to_string(),
                "mlcape".to_string(),
                "srh_0_3km".to_string(),
                "qpf_1h".to_string(),
            ]),
            ..RenderMapsRequestJson::default()
        };

        let routed = route_requested_products(ModelId::Hrrr, &request).unwrap();

        assert_eq!(routed.heavy_derived_recipe_slugs, vec!["mlecape"]);
        assert!(routed.direct_recipe_slugs.is_empty());
        assert_eq!(routed.derived_recipe_slugs, vec!["mlcape", "srh_0_3km"]);
        assert_eq!(routed.windowed_products, vec![HrrrWindowedProduct::Qpf1h]);
    }

    #[test]
    fn render_maps_plan_keeps_heavy_only_requests_out_of_non_ecape_runner() {
        let request = RenderMapsRequestJson {
            date_yyyymmdd: Some("20260424".to_string()),
            cycle_utc: Some(22),
            forecast_hour: Some(1),
            domain: Some("heavy-smoke".to_string()),
            bounds: Some(vec![-102.0, -94.0, 33.0, 38.0]),
            products: Some(vec![
                "sbecape".to_string(),
                "mlecape".to_string(),
                "muecape".to_string(),
            ]),
            ..RenderMapsRequestJson::default()
        };

        let plan = build_render_maps_plan(request).unwrap();

        assert!(plan.request.direct_recipe_slugs.is_empty());
        assert!(plan.request.derived_recipe_slugs.is_empty());
        assert!(plan.request.windowed_products.is_empty());
        assert_eq!(
            plan.heavy_derived_recipe_slugs,
            vec!["sbecape", "mlecape", "muecape"]
        );
        assert_eq!(plan.request.domains[0].slug, "heavy_smoke");
    }
}

#[cfg(feature = "python")]
#[pymodule]
fn rustwx(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(workspace_name, module)?)?;
    module.add_function(wrap_pyfunction!(list_models_json, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_urls_json, module)?)?;
    module.add_function(wrap_pyfunction!(latest_run_json, module)?)?;
    module.add_function(wrap_pyfunction!(available_forecast_hours_json, module)?)?;
    module.add_function(wrap_pyfunction!(probe_sources_json, module)?)?;
    module.add_function(wrap_pyfunction!(agent_capabilities_json, module)?)?;
    module.add_function(wrap_pyfunction!(list_domains_json, module)?)?;
    module.add_function(wrap_pyfunction!(render_maps_json, module)?)?;
    module.add_function(wrap_pyfunction!(cli_main, module)?)?;
    module.add_function(wrap_pyfunction!(
        describe_projected_projection_json,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(describe_projected_projection, module)?)?;
    module.add_function(wrap_pyfunction!(describe_projected_geometry_json, module)?)?;
    module.add_function(wrap_pyfunction!(describe_projected_geometry, module)?)?;
    module.add_function(wrap_pyfunction!(
        build_projected_basemap_overlays_json,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(build_projected_basemap_overlays, module)?)?;
    module.add_function(wrap_pyfunction!(
        normalize_cross_section_request_json,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(normalize_cross_section_request, module)?)?;
    module.add_function(wrap_pyfunction!(render_projected_map, module)?)?;
    module.add_function(wrap_pyfunction!(render_projected_map_json, module)?)?;
    module.add_function(wrap_pyfunction!(render_wrf_map, module)?)?;
    module.add_function(wrap_pyfunction!(render_wrf_map_json, module)?)?;
    module.add_function(wrap_pyfunction!(render_sounding_column, module)?)?;
    module.add_function(wrap_pyfunction!(render_sounding_column_json, module)?)?;
    Ok(())
}
