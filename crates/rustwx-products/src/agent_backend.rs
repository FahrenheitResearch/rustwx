use rustwx_core::ModelId;
use rustwx_models::PlotRecipeFetchMode;
use rustwx_render::{ProductMaturity, ProductSemanticFlag};
use serde::{Deserialize, Serialize};

use crate::catalog::{
    ProductCatalogAlias, ProductCatalogEntry, ProductCatalogKind, ProductCatalogStatus,
    ProductTargetStatus, SupportedProductsCatalog, build_supported_products_catalog,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentUserTier {
    Basic,
    Intermediate,
    Advanced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentExecutionLane {
    Instant,
    Interactive,
    Background,
    Precompute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentPreflightStatus {
    Complete,
    Partial,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCallableSurface {
    CatalogMetadata,
    ProductSampling,
    DirectMap,
    DerivedMap,
    WindowedMap,
    HeavyBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPreflightRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelId>,
    #[serde(default)]
    pub products: Vec<String>,
    #[serde(default)]
    pub include_all_catalog_entries: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPreflightReport {
    pub schema_version: u16,
    pub model: Option<String>,
    pub summary: AgentPreflightSummary,
    pub products: Vec<AgentProductPreflight>,
    pub unknown_products: Vec<AgentUnknownProduct>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPreflightSummary {
    pub requested_products: usize,
    pub reported_products: usize,
    pub unknown_products: usize,
    pub complete_products: usize,
    pub partial_products: usize,
    pub blocked_products: usize,
    pub instant_lanes: usize,
    pub interactive_lanes: usize,
    pub background_lanes: usize,
    pub precompute_lanes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProductPreflight {
    pub slug: String,
    pub title: String,
    pub kind: ProductCatalogKind,
    pub catalog_status: ProductCatalogStatus,
    pub preflight_status: AgentPreflightStatus,
    pub tier: AgentUserTier,
    pub primary_lane: AgentExecutionLane,
    pub maturity: ProductMaturity,
    pub flags: Vec<ProductSemanticFlag>,
    pub runners: Vec<String>,
    pub callable_surfaces: Vec<AgentSurfacePlan>,
    pub cost: AgentCostEstimate,
    pub supported_targets: Vec<String>,
    pub blocked_targets: Vec<String>,
    pub blockers: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSurfacePlan {
    pub surface: AgentCallableSurface,
    pub lane: AgentExecutionLane,
    pub runner: Option<String>,
    pub output_contract: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCostEstimate {
    pub fetch_strategy: String,
    pub expected_fetch_groups: Option<usize>,
    pub expected_artifacts: Option<usize>,
    pub cache_value: String,
    pub preflight_confidence: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentUnknownProduct {
    pub requested: String,
    pub status: AgentPreflightStatus,
    pub blockers: Vec<String>,
}

pub fn build_agent_preflight(request: AgentPreflightRequest) -> AgentPreflightReport {
    build_agent_preflight_from_catalog(request, &build_supported_products_catalog())
}

pub fn build_agent_preflight_from_catalog(
    request: AgentPreflightRequest,
    catalog: &SupportedProductsCatalog,
) -> AgentPreflightReport {
    let mut products = Vec::new();
    let mut unknown_products = Vec::new();

    if request.include_all_catalog_entries || request.products.is_empty() {
        for entry in all_catalog_entries(catalog) {
            products.push(preflight_entry(entry, request.model, None));
        }
    } else {
        for requested in &request.products {
            match find_catalog_entry(catalog, requested) {
                Some((entry, alias)) => products.push(preflight_entry(entry, request.model, alias)),
                None => unknown_products.push(AgentUnknownProduct {
                    requested: requested.clone(),
                    status: AgentPreflightStatus::Blocked,
                    blockers: vec![format!(
                        "unknown product slug '{requested}'; ask product_catalog or agent_preflight without filters for the reachable surface"
                    )],
                }),
            }
        }
    }

    products.sort_by(|left, right| left.slug.cmp(&right.slug));
    let summary = summarize_preflight(&request, &products, &unknown_products);
    let recommendations = build_recommendations(&products, &unknown_products);

    AgentPreflightReport {
        schema_version: 1,
        model: request.model.map(|model| model.to_string()),
        summary,
        products,
        unknown_products,
        recommendations,
    }
}

fn all_catalog_entries(catalog: &SupportedProductsCatalog) -> Vec<&ProductCatalogEntry> {
    catalog
        .direct
        .iter()
        .chain(catalog.derived.iter())
        .chain(catalog.heavy.iter())
        .chain(catalog.windowed.iter())
        .collect()
}

fn find_catalog_entry<'a>(
    catalog: &'a SupportedProductsCatalog,
    requested: &str,
) -> Option<(&'a ProductCatalogEntry, Option<&'a ProductCatalogAlias>)> {
    let normalized = normalize_slug(requested);
    all_catalog_entries(catalog).into_iter().find_map(|entry| {
        if normalize_slug(&entry.slug) == normalized {
            return Some((entry, None));
        }
        entry
            .aliases
            .iter()
            .find(|alias| normalize_slug(&alias.slug) == normalized)
            .map(|alias| (entry, Some(alias)))
    })
}

fn normalize_slug(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

fn preflight_entry(
    entry: &ProductCatalogEntry,
    model: Option<ModelId>,
    alias: Option<&ProductCatalogAlias>,
) -> AgentProductPreflight {
    let preflight_status = preflight_status(entry, model);
    let tier = classify_tier(entry);
    let primary_lane = classify_primary_lane(entry);
    let target_support = target_support(entry, model);
    let supported_targets = target_support
        .iter()
        .filter(|target| matches!(target.status, ProductTargetStatus::Supported))
        .map(|target| target.target.clone())
        .collect::<Vec<_>>();
    let blocked_targets = target_support
        .iter()
        .filter(|target| matches!(target.status, ProductTargetStatus::Blocked))
        .map(|target| target.target.clone())
        .collect::<Vec<_>>();
    let blockers = target_support
        .iter()
        .flat_map(|target| target.blockers.iter().cloned())
        .collect::<Vec<_>>();
    let mut notes = entry.notes.clone();
    if let Some(alias) = alias {
        notes.push(format!(
            "requested alias '{}' resolves to canonical product '{}': {}",
            alias.slug, entry.slug, alias.note
        ));
    }
    if entry.experimental {
        notes.push(
            "non-operational product; require explicit agent wording before launch".to_string(),
        );
    }
    if preflight_status == AgentPreflightStatus::Partial {
        notes.push("some model targets are supported and some are blocked".to_string());
    }

    AgentProductPreflight {
        slug: entry.slug.clone(),
        title: entry.title.clone(),
        kind: entry.kind,
        catalog_status: entry.status,
        preflight_status,
        tier,
        primary_lane,
        maturity: entry.maturity,
        flags: entry.flags.clone(),
        runners: entry.runners.clone(),
        callable_surfaces: surface_plans(entry),
        cost: estimate_cost(entry, model),
        supported_targets,
        blocked_targets,
        blockers,
        notes,
    }
}

fn target_support(
    entry: &ProductCatalogEntry,
    model: Option<ModelId>,
) -> Vec<crate::catalog::ProductTargetSupport> {
    match model {
        Some(model) => entry
            .support
            .iter()
            .filter(|target| target.model == Some(model))
            .cloned()
            .collect(),
        None => entry.support.clone(),
    }
}

fn preflight_status(entry: &ProductCatalogEntry, model: Option<ModelId>) -> AgentPreflightStatus {
    if let Some(model) = model {
        return match entry
            .support
            .iter()
            .find(|target| target.model == Some(model))
        {
            Some(target) if matches!(target.status, ProductTargetStatus::Supported) => {
                AgentPreflightStatus::Complete
            }
            Some(_) => AgentPreflightStatus::Blocked,
            None => AgentPreflightStatus::Blocked,
        };
    }

    match entry.status {
        ProductCatalogStatus::Supported => AgentPreflightStatus::Complete,
        ProductCatalogStatus::Partial => AgentPreflightStatus::Partial,
        ProductCatalogStatus::Blocked => AgentPreflightStatus::Blocked,
    }
}

fn classify_tier(entry: &ProductCatalogEntry) -> AgentUserTier {
    if entry.experimental || entry.maturity != ProductMaturity::Operational {
        return AgentUserTier::Advanced;
    }
    match entry.kind {
        ProductCatalogKind::Heavy | ProductCatalogKind::Windowed => AgentUserTier::Advanced,
        ProductCatalogKind::Derived => AgentUserTier::Advanced,
        ProductCatalogKind::Direct => {
            let style = entry.render_style.as_deref().unwrap_or_default();
            if matches!(
                style,
                "weather_temperature"
                    | "weather_dewpoint"
                    | "weather_rh"
                    | "weather_winds"
                    | "weather_wind_gust"
                    | "weather_qpf"
                    | "weather_probability"
            ) {
                AgentUserTier::Basic
            } else {
                AgentUserTier::Intermediate
            }
        }
    }
}

fn classify_primary_lane(entry: &ProductCatalogEntry) -> AgentExecutionLane {
    match entry.kind {
        ProductCatalogKind::Direct => AgentExecutionLane::Background,
        ProductCatalogKind::Derived | ProductCatalogKind::Windowed => {
            AgentExecutionLane::Background
        }
        ProductCatalogKind::Heavy => AgentExecutionLane::Precompute,
    }
}

fn surface_plans(entry: &ProductCatalogEntry) -> Vec<AgentSurfacePlan> {
    let mut plans = vec![AgentSurfacePlan {
        surface: AgentCallableSurface::CatalogMetadata,
        lane: AgentExecutionLane::Instant,
        runner: Some("agent_preflight".to_string()),
        output_contract: "json".to_string(),
        notes: vec!["safe to preload and keep hot in an agent service".to_string()],
    }];

    match entry.kind {
        ProductCatalogKind::Direct => {
            plans.push(AgentSurfacePlan {
                surface: AgentCallableSurface::ProductSampling,
                lane: AgentExecutionLane::Interactive,
                runner: Some("product_sampling".to_string()),
                output_contract: "json".to_string(),
                notes: vec![
                    "good first live path for point or area answers when model support is complete"
                        .to_string(),
                ],
            });
            plans.push(AgentSurfacePlan {
                surface: AgentCallableSurface::DirectMap,
                lane: AgentExecutionLane::Background,
                runner: Some("direct_batch".to_string()),
                output_contract: "png_and_manifest_json".to_string(),
                notes: vec![
                    "stream progress and artifact completion rather than blocking the chat turn"
                        .to_string(),
                ],
            });
        }
        ProductCatalogKind::Derived => plans.push(AgentSurfacePlan {
            surface: AgentCallableSurface::DerivedMap,
            lane: AgentExecutionLane::Background,
            runner: Some("derived_batch".to_string()),
            output_contract: "png_and_manifest_json".to_string(),
            notes: vec![
                "preflight blockers and native-route availability before launching work"
                    .to_string(),
            ],
        }),
        ProductCatalogKind::Windowed => plans.push(AgentSurfacePlan {
            surface: AgentCallableSurface::WindowedMap,
            lane: AgentExecutionLane::Background,
            runner: Some("hrrr_windowed_batch".to_string()),
            output_contract: "png_and_manifest_json".to_string(),
            notes: vec![
                "multi-hour products should be queued or prewarmed for expected domains"
                    .to_string(),
            ],
        }),
        ProductCatalogKind::Heavy => plans.push(AgentSurfacePlan {
            surface: AgentCallableSurface::HeavyBundle,
            lane: AgentExecutionLane::Precompute,
            runner: Some("severe_batch".to_string()),
            output_contract: "png_bundle_and_manifest_json".to_string(),
            notes: vec!["requires region-size gating before fetch/decode/compute".to_string()],
        }),
    }

    plans
}

fn estimate_cost(entry: &ProductCatalogEntry, model: Option<ModelId>) -> AgentCostEstimate {
    let support = target_support(entry, model);
    let first_supported = support
        .iter()
        .find(|target| matches!(target.status, ProductTargetStatus::Supported));
    let fetch_strategy = match entry.kind {
        ProductCatalogKind::Direct => first_supported
            .and_then(|target| target.fetch_mode)
            .map(fetch_mode_label)
            .unwrap_or("model_dependent_direct_fetch"),
        ProductCatalogKind::Derived => "derived_field_bundle",
        ProductCatalogKind::Windowed => "multi_hour_windowed_bundle",
        ProductCatalogKind::Heavy => "heavy_volume_compute",
    }
    .to_string();
    let expected_fetch_groups = match entry.kind {
        ProductCatalogKind::Direct => first_supported.map(|_| 1),
        ProductCatalogKind::Derived => Some(2),
        ProductCatalogKind::Windowed => Some(3),
        ProductCatalogKind::Heavy => Some(2),
    };
    let expected_artifacts = match entry.kind {
        ProductCatalogKind::Heavy => Some(10),
        _ => Some(1),
    };
    let cache_value = match entry.kind {
        ProductCatalogKind::Direct => "medium",
        ProductCatalogKind::Derived | ProductCatalogKind::Windowed | ProductCatalogKind::Heavy => {
            "high"
        }
    };
    let preflight_confidence = match entry.kind {
        ProductCatalogKind::Direct => "high",
        ProductCatalogKind::Derived | ProductCatalogKind::Windowed => "medium",
        ProductCatalogKind::Heavy => "low",
    };
    let mut notes = Vec::new();
    if matches!(entry.kind, ProductCatalogKind::Heavy) {
        notes.push("ask RustWx for a region/cell cost before launching heavy compute".to_string());
    }
    if matches!(entry.status, ProductCatalogStatus::Partial) {
        notes.push(
            "model-specific support can change this from partial to complete or blocked"
                .to_string(),
        );
    }
    if support.iter().any(|target| {
        matches!(
            target.fetch_mode,
            Some(PlotRecipeFetchMode::WholeFileStructuredExtract)
        )
    }) {
        notes.push(
            "whole-file fetches may beat repeated subsets when many products share the same file"
                .to_string(),
        );
    }

    AgentCostEstimate {
        fetch_strategy,
        expected_fetch_groups,
        expected_artifacts,
        cache_value: cache_value.to_string(),
        preflight_confidence: preflight_confidence.to_string(),
        notes,
    }
}

fn fetch_mode_label(fetch_mode: PlotRecipeFetchMode) -> &'static str {
    match fetch_mode {
        PlotRecipeFetchMode::IndexedSubset => "indexed_subset",
        PlotRecipeFetchMode::WholeFileStructuredExtract => "whole_file_structured_extract",
    }
}

fn summarize_preflight(
    request: &AgentPreflightRequest,
    products: &[AgentProductPreflight],
    unknown_products: &[AgentUnknownProduct],
) -> AgentPreflightSummary {
    let mut summary = AgentPreflightSummary {
        requested_products: request.products.len(),
        reported_products: products.len(),
        unknown_products: unknown_products.len(),
        complete_products: 0,
        partial_products: 0,
        blocked_products: unknown_products.len(),
        instant_lanes: 0,
        interactive_lanes: 0,
        background_lanes: 0,
        precompute_lanes: 0,
    };

    for product in products {
        match product.preflight_status {
            AgentPreflightStatus::Complete => summary.complete_products += 1,
            AgentPreflightStatus::Partial => summary.partial_products += 1,
            AgentPreflightStatus::Blocked => summary.blocked_products += 1,
        }
        match product.primary_lane {
            AgentExecutionLane::Instant => summary.instant_lanes += 1,
            AgentExecutionLane::Interactive => summary.interactive_lanes += 1,
            AgentExecutionLane::Background => summary.background_lanes += 1,
            AgentExecutionLane::Precompute => summary.precompute_lanes += 1,
        }
    }

    summary
}

fn build_recommendations(
    products: &[AgentProductPreflight],
    unknown_products: &[AgentUnknownProduct],
) -> Vec<String> {
    let mut recommendations = vec![
        "preload this preflight report and the product catalog in the app service".to_string(),
        "return job IDs for background/precompute lanes and stream artifact updates".to_string(),
        "treat blocked and partial statuses as user-visible planning facts, not process failures"
            .to_string(),
    ];
    if products
        .iter()
        .any(|product| product.preflight_status == AgentPreflightStatus::Partial)
    {
        recommendations.push(
            "resolve partial products with an explicit model before launching work".to_string(),
        );
    }
    if products
        .iter()
        .any(|product| product.primary_lane == AgentExecutionLane::Precompute)
    {
        recommendations.push(
            "ask for a domain/cell-size estimate before launching heavy products".to_string(),
        );
    }
    if !unknown_products.is_empty() {
        recommendations.push(
            "run agent_preflight without product filters to refresh the reachable product surface"
                .to_string(),
        );
    }
    recommendations
}

#[cfg(test)]
mod tests;
