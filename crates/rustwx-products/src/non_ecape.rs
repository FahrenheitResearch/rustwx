use crate::derived::{
    plan_derived_recipes, run_hrrr_derived_batch_from_loaded, HrrrDerivedBatchReport,
    HrrrDerivedBatchRequest,
};
use crate::direct::{
    run_hrrr_direct_batch_from_loaded, HrrrDirectBatchReport, HrrrDirectBatchRequest,
};
use crate::hrrr::{resolve_hrrr_run, DomainSpec};
use crate::orchestrator::{lane, run_fanout3};
use crate::planner::ExecutionPlanBuilder;
use crate::publication::{
    artifact_identity_from_path, default_run_manifest_path, finalize_and_publish_run_manifest,
    publish_run_manifest_with_attempt, ArtifactPublicationState, PublishedArtifactRecord,
    PublishedFetchIdentity, RunPublicationManifest,
};
use crate::publication_provenance::capture_default_build_provenance;
use crate::runtime::{BundleLoaderConfig, load_execution_plan};
use crate::severe::build_severe_execution_plan;
use crate::windowed::{
    collect_windowed_input_fetches, run_hrrr_windowed_batch_with_context,
    windowed_product_input_fetch_keys, HrrrWindowedBatchReport, HrrrWindowedBatchRequest,
    HrrrWindowedProduct, HrrrWindowedRenderedProduct,
};
use rustwx_core::SourceId;
use rustwx_models::plot_recipe;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

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
    pub direct_recipe_slugs: Vec<String>,
    pub derived_recipe_slugs: Vec<String>,
    pub windowed_products: Vec<HrrrWindowedProduct>,
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
    /// Canonical (latest-attempt) run manifest path — stable across
    /// reruns and therefore clobberable.
    pub publication_manifest_path: PathBuf,
    /// Immutable attempt-stamped sibling manifest path. Always present
    /// on completed runs; paired with [`publication_manifest_path`] it
    /// forms the `(current truth, immutable attempt)` contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_manifest_path: Option<PathBuf>,
    pub requested: HrrrNonEcapeHourRequestedProducts,
    pub summary: HrrrNonEcapeHourSummary,
    pub direct: Option<HrrrDirectBatchReport>,
    pub derived: Option<HrrrDerivedBatchReport>,
    pub windowed: Option<HrrrWindowedBatchReport>,
    pub total_ms: u128,
}

pub fn run_hrrr_non_ecape_hour(
    request: &HrrrNonEcapeHourRequest,
) -> Result<HrrrNonEcapeHourReport, Box<dyn std::error::Error>> {
    let normalized = normalize_requested_products(request);
    validate_requested_work(&normalized)?;

    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let total_start = Instant::now();
    let latest = resolve_hrrr_run(
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.source,
    )?;
    let pinned_date = latest.cycle.date_yyyymmdd.clone();
    let pinned_cycle = Some(latest.cycle.hour_utc);
    let pinned_source = latest.source;
    let pinned_cycle_utc = latest.cycle.hour_utc;

    // Build a single planner-level execution plan that covers every
    // bundle every requested lane needs at this hour. Direct and derived
    // (including severe/ECAPE-style surface+pressure pairs) all flow
    // through the loader once; the planner dedupes when direct's
    // `nat`-planned recipes route onto the same `sfc` fetch the derived
    // surface lane already needs.
    let direct_groups = if normalized.direct_recipe_slugs.is_empty() {
        Vec::new()
    } else {
        let direct_request = HrrrDirectBatchRequest {
            date_yyyymmdd: pinned_date.clone(),
            cycle_override_utc: pinned_cycle,
            forecast_hour: request.forecast_hour,
            source: pinned_source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: normalized.direct_recipe_slugs.clone(),
        };
        let generic_direct = crate::direct::DirectBatchRequest::from_hrrr_for_planner(&direct_request);
        crate::direct::plan_direct_fetch_groups(&generic_direct)?
    };
    let derived_recipes = if normalized.derived_recipe_slugs.is_empty() {
        Vec::new()
    } else {
        plan_derived_recipes(&normalized.derived_recipe_slugs)?
    };

    // Combine derived (surface+pressure pair) and direct (per-group
    // NativeAnalysis) requirements into one execution plan.
    let mut plan_builder = ExecutionPlanBuilder::new(&latest, request.forecast_hour);
    if !derived_recipes.is_empty() {
        // Reuse the severe/ECAPE pair builder; planner dedupes when
        // direct requirements collapse onto the same sfc/prs fetches.
        let pair_plan = build_severe_execution_plan(&latest, request.forecast_hour, None, None);
        for bundle in &pair_plan.bundles {
            for alias in &bundle.aliases {
                let mut requirement = rustwx_core::BundleRequirement::new(
                    alias.bundle,
                    bundle.id.forecast_hour,
                );
                if let Some(ref over) = alias.native_override {
                    requirement = requirement.with_native_override(over.clone());
                }
                plan_builder.require_with_logical_family(
                    &requirement,
                    alias.logical_family.as_deref(),
                );
            }
        }
    }
    for group in &direct_groups {
        let requirement = rustwx_core::BundleRequirement::new(
            rustwx_core::CanonicalBundleDescriptor::NativeAnalysis,
            request.forecast_hour,
        )
        .with_native_override(group.product.clone());
        for alias in &group.planned_family_aliases {
            plan_builder.require_with_logical_family(&requirement, Some(alias));
        }
    }
    let plan = plan_builder.build();
    let needs_load = !plan.bundles.is_empty();
    let loaded = if needs_load {
        Some(load_execution_plan(
            plan,
            &BundleLoaderConfig {
                cache_root: request.cache_root.clone(),
                use_cache: request.use_cache,
            },
        )?)
    } else {
        None
    };
    let loaded_ref = loaded.as_ref();

    let run_slug = format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_non_ecape_hour",
        pinned_date, pinned_cycle_utc, request.forecast_hour, request.domain.slug
    );
    let manifest_path = default_run_manifest_path(&request.out_dir, &run_slug);
    let mut manifest = build_run_manifest(
        &normalized,
        &request.out_dir,
        &run_slug,
        &pinned_date,
        pinned_cycle_utc,
        request.forecast_hour,
        &request.domain.slug,
    );
    manifest.build_provenance = Some(capture_default_build_provenance());
    manifest.mark_running();
    crate::publication::publish_run_manifest(&manifest_path, &manifest)?;

    let direct_request =
        (!normalized.direct_recipe_slugs.is_empty()).then(|| HrrrDirectBatchRequest {
            date_yyyymmdd: pinned_date.clone(),
            cycle_override_utc: pinned_cycle,
            forecast_hour: request.forecast_hour,
            source: pinned_source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            recipe_slugs: normalized.direct_recipe_slugs.clone(),
        });

    let derived_request = (!normalized.derived_recipe_slugs.is_empty()).then(|| {
        (
            HrrrDerivedBatchRequest {
                date_yyyymmdd: pinned_date.clone(),
                cycle_override_utc: pinned_cycle,
                forecast_hour: request.forecast_hour,
                source: pinned_source,
                domain: request.domain.clone(),
                out_dir: request.out_dir.clone(),
                cache_root: request.cache_root.clone(),
                use_cache: request.use_cache,
                recipe_slugs: normalized.derived_recipe_slugs.clone(),
            },
            derived_recipes.clone(),
        )
    });

    let windowed_request =
        (!normalized.windowed_products.is_empty()).then(|| HrrrWindowedBatchRequest {
            date_yyyymmdd: pinned_date.clone(),
            cycle_override_utc: pinned_cycle,
            forecast_hour: request.forecast_hour,
            source: pinned_source,
            domain: request.domain.clone(),
            out_dir: request.out_dir.clone(),
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
            products: normalized.windowed_products.clone(),
        });

    let lane_result = run_fanout3(
        should_run_lanes_concurrently(pinned_source),
        direct_request.as_ref().map(|lane_request| {
            lane("direct", move || {
                run_hrrr_direct_batch_from_loaded(
                    lane_request,
                    loaded_ref.expect("planner must load bundles when direct is requested"),
                )
            })
        }),
        derived_request.as_ref().map(|(lane_request, recipes)| {
            lane("derived", move || {
                run_hrrr_derived_batch_from_loaded(
                    lane_request,
                    recipes,
                    loaded_ref.expect("planner must load bundles when derived is requested"),
                )
            })
        }),
        windowed_request.as_ref().map(|lane_request| {
            // Windowed builds its own planner execution plan across
            // contributing hours and loads it through load_execution_plan,
            // so the lane itself is planner-driven. What's missing here
            // is cross-lane dedupe: windowed's plan is separate from the
            // direct+derived plan we just built, so a same-hour wrfsfc
            // fetch that both sides need is fetched twice. Unifying the
            // two plans is the next step for true cross-lane dedupe.
            let windowed_latest = latest.clone();
            lane("windowed", move || {
                run_hrrr_windowed_batch_with_context(lane_request, &windowed_latest)
            })
        }),
    );

    let (direct, derived, windowed) = match lane_result {
        Ok(reports) => reports,
        Err(err) => {
            manifest.mark_failed(err.to_string());
            // On hard failure still publish both canonical and
            // attempt-stamped manifests so the failing run is auditable.
            let _ = publish_run_manifest_with_attempt(
                &manifest_path,
                &request.out_dir,
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
        finalize_and_publish_run_manifest(&mut manifest, &request.out_dir, &run_slug)?;
    Ok(HrrrNonEcapeHourReport {
        date_yyyymmdd: pinned_date,
        cycle_utc: latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: pinned_source,
        domain: request.domain.clone(),
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        publication_manifest_path: canonical_manifest_path,
        attempt_manifest_path: Some(attempt_manifest_path),
        requested: normalized,
        summary,
        direct,
        derived,
        windowed,
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn validate_requested_work(
    request: &HrrrNonEcapeHourRequestedProducts,
) -> Result<(), Box<dyn std::error::Error>> {
    if request.direct_recipe_slugs.is_empty()
        && request.derived_recipe_slugs.is_empty()
        && request.windowed_products.is_empty()
    {
        return Err(
            "unified HRRR non-ECAPE hour runner needs at least one direct recipe, derived recipe, or windowed product"
                .into(),
        );
    }
    Ok(())
}

fn normalize_requested_products(
    request: &HrrrNonEcapeHourRequest,
) -> HrrrNonEcapeHourRequestedProducts {
    let mut direct_recipe_slugs = Vec::new();
    let mut windowed_products = request.windowed_products.clone();

    for slug in &request.direct_recipe_slugs {
        let normalized_slug = plot_recipe(slug)
            .map(|recipe| recipe.slug)
            .unwrap_or(slug.as_str());
        if normalized_slug == "1h_qpf" {
            if !windowed_products.contains(&HrrrWindowedProduct::Qpf1h) {
                windowed_products.push(HrrrWindowedProduct::Qpf1h);
            }
            continue;
        }
        direct_recipe_slugs.push(slug.clone());
    }

    HrrrNonEcapeHourRequestedProducts {
        direct_recipe_slugs,
        derived_recipe_slugs: request.derived_recipe_slugs.clone(),
        windowed_products,
    }
}

fn should_run_lanes_concurrently(source: SourceId) -> bool {
    !matches!(source, SourceId::Nomads)
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
    request: &HrrrNonEcapeHourRequestedProducts,
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
                    date_yyyymmdd,
                    cycle_utc,
                    forecast_hour,
                    domain_slug,
                    slug,
                ),
            ));
        }
    }

    RunPublicationManifest::new(
        "hrrr_non_ecape_hour",
        run_slug.to_string(),
        out_dir.to_path_buf(),
    )
    .with_artifacts(artifacts)
}

fn expected_output_relative_path(
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    domain_slug: &str,
    product_slug: &str,
) -> PathBuf {
    PathBuf::from(format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_{}.png",
        date_yyyymmdd, cycle_utc, forecast_hour, domain_slug, product_slug
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
                "planned_family={} fetched_family={} resolved_source={} resolved_url={}",
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
        manifest.update_artifact_state(
            &derived_artifact_key(&recipe.recipe_slug),
            ArtifactPublicationState::Complete,
            Some(format!(
                "shared_surface planned_family={} fetched_family={} resolved_source={}; shared_pressure planned_family={} fetched_family={} resolved_source={}",
                report.shared_timing.fetch_decode.surface_fetch.planned_product,
                report.shared_timing.fetch_decode.surface_fetch.fetched_product,
                report.shared_timing.fetch_decode.surface_fetch.resolved_source,
                report.shared_timing.fetch_decode.pressure_fetch.planned_product,
                report.shared_timing.fetch_decode.pressure_fetch.fetched_product,
                report.shared_timing.fetch_decode.pressure_fetch.resolved_source
            )),
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
    let fetches = windowed_runtime_fetches_for_product(product, shared_timing);
    let planned_family = fetches
        .first()
        .map(|fetch| fetch.planned_product.as_str())
        .unwrap_or(if is_qpf { "sfc" } else { "nat" });
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
            by_key
                .entry(identity.fetch_key.clone())
                .or_insert(identity);
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
    let contributing_hours = &product.metadata.contributing_forecast_hours;
    let fetches = if is_qpf {
        &shared_timing.surface_hour_fetches
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
    use crate::direct::{HrrrDirectRecipeTiming, HrrrDirectRenderedRecipe};
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
            direct_recipe_slugs: Vec::new(),
            derived_recipe_slugs: Vec::new(),
            windowed_products: Vec::new(),
        }
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
        let err = validate_requested_work(&normalize_requested_products(&empty_request()))
            .expect_err("empty request should be rejected")
            .to_string();
        assert!(err.contains("at least one direct recipe"));
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
        assert!(!should_run_lanes_concurrently(SourceId::Nomads));
        assert!(should_run_lanes_concurrently(SourceId::Aws));
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
                grib_product: "nat".into(),
                fetched_grib_product: "sfc".into(),
                resolved_source: SourceId::Aws,
                resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                output_path: PathBuf::from("C:\\proof\\direct.png"),
                content_identity: crate::publication::artifact_identity_from_bytes(b"direct"),
                input_fetch_keys: vec!["direct:nat->sfc".into()],
                timing: HrrrDirectRecipeTiming {
                    project_ms: 1,
                    render_ms: 2,
                    total_ms: 3,
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
                fetch_decode: crate::gridded::SharedTiming {
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
                },
                compute_ms: 4,
                project_ms: 5,
            },
            recipes: vec![HrrrDerivedRenderedRecipe {
                recipe_slug: "sbcape".into(),
                title: "SBCAPE".into(),
                output_path: PathBuf::from("C:\\proof\\derived.png"),
                content_identity: crate::publication::artifact_identity_from_bytes(b"derived"),
                input_fetch_keys: vec!["derived:sfc".into(), "derived:prs".into()],
                timing: HrrrDerivedRecipeTiming {
                    render_ms: 6,
                    total_ms: 6,
                },
            }],
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
                geometry_fetch_cache_hit: false,
                geometry_decode_cache_hit: false,
                surface_hours_loaded: vec![6],
                nat_hours_loaded: vec![6],
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
                    render_ms: 2,
                    total_ms: 3,
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
                fetch_decode: crate::gridded::SharedTiming {
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
                },
                compute_ms: 1,
                project_ms: 1,
            },
            recipes: vec![HrrrDerivedRenderedRecipe {
                recipe_slug: "sbcape".into(),
                title: "SBCAPE".into(),
                output_path: PathBuf::from(
                    "C:\\proof\\run\\rustwx_hrrr_20260415_12z_f006_conus_sbcape.png",
                ),
                content_identity: crate::publication::artifact_identity_from_bytes(b"derived-run"),
                input_fetch_keys: vec!["derived:sfc".into(), "derived:prs".into()],
                timing: HrrrDerivedRecipeTiming {
                    render_ms: 1,
                    total_ms: 1,
                },
            }],
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
                geometry_fetch_cache_hit: false,
                geometry_decode_cache_hit: false,
                surface_hours_loaded: vec![6],
                nat_hours_loaded: vec![6],
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
        assert!(direct_record
            .detail
            .as_deref()
            .unwrap()
            .contains("planned_family=prs fetched_family=prs resolved_source=aws"));

        let derived_record = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_key == "derived:sbcape")
            .unwrap();
        assert_eq!(derived_record.state, ArtifactPublicationState::Complete);
        assert!(derived_record
            .detail
            .as_deref()
            .unwrap()
            .contains("shared_surface planned_family=sfc fetched_family=sfc resolved_source=aws"));

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
            geometry_fetch_cache_hit: false,
            geometry_decode_cache_hit: false,
            surface_hours_loaded: vec![5, 6],
            nat_hours_loaded: Vec::new(),
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
                geometry_fetch_cache_hit: false,
                geometry_decode_cache_hit: false,
                surface_hours_loaded: vec![6],
                nat_hours_loaded: vec![6],
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
            publication_manifest_path: PathBuf::from("C:\\proof\\bench\\run_manifest.json"),
            attempt_manifest_path: None,
            requested: HrrrNonEcapeHourRequestedProducts {
                direct_recipe_slugs: vec!["500mb_height_winds".into()],
                derived_recipe_slugs: vec!["sbcape".into()],
                windowed_products: vec![HrrrWindowedProduct::Qpf6h],
            },
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
