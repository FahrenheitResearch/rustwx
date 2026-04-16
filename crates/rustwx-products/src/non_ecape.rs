use crate::derived::{
    HrrrDerivedBatchReport, HrrrDerivedBatchRequest, plan_derived_recipes,
    run_hrrr_derived_batch_with_context,
};
use crate::direct::{
    HrrrDirectBatchReport, HrrrDirectBatchRequest, required_direct_projection_sizes,
    run_hrrr_direct_batch_with_context,
};
use crate::hrrr::{DomainSpec, prepare_hrrr_hour_context};
use crate::orchestrator::{PreparedRunContext, PreparedRunMetadata, lane, run_fanout3};
use crate::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, PublishedFetchIdentity,
    RunPublicationManifest, artifact_identity_from_path, default_run_manifest_path,
    fetch_identity_from_cached_result, publish_run_manifest,
};
use crate::windowed::{
    HrrrWindowedBatchReport, HrrrWindowedBatchRequest, HrrrWindowedProduct,
    HrrrWindowedRenderedProduct, run_hrrr_windowed_batch_with_context,
};
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};
use rustwx_io::{FetchRequest, load_cached_fetch};
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
    pub publication_manifest_path: PathBuf,
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
    let projection_sizes = required_projection_sizes(&normalized);
    let context = prepare_hrrr_hour_context(
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
        request.domain.bounds,
        &projection_sizes,
        &request.cache_root,
        request.use_cache,
    )?;
    let prepared = PreparedRunContext::new(
        PreparedRunMetadata::from_latest(context.timestep().latest(), request.forecast_hour),
        context,
    );
    let metadata = prepared.metadata().clone();
    let latest = prepared.context().timestep().latest();
    let timestep = prepared.context().timestep();
    let context_ref = prepared.context();
    let pinned_date = metadata.date_yyyymmdd.clone();
    let pinned_cycle = Some(metadata.cycle_utc);
    let pinned_source = metadata.source;
    let run_slug = format!(
        "rustwx_hrrr_{}_{}z_f{:03}_{}_non_ecape_hour",
        pinned_date, metadata.cycle_utc, metadata.forecast_hour, request.domain.slug
    );
    let manifest_path = default_run_manifest_path(&request.out_dir, &run_slug);
    let mut manifest = build_run_manifest(
        &normalized,
        &request.out_dir,
        &run_slug,
        &pinned_date,
        metadata.cycle_utc,
        metadata.forecast_hour,
        &request.domain.slug,
    );
    manifest.mark_running();
    publish_run_manifest(&manifest_path, &manifest)?;

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

    let derived_request = (!normalized.derived_recipe_slugs.is_empty())
        .then(|| {
            let recipes = plan_derived_recipes(&normalized.derived_recipe_slugs)?;
            Ok::<_, Box<dyn std::error::Error>>((
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
                recipes,
            ))
        })
        .transpose()?;

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
                run_hrrr_direct_batch_with_context(lane_request, latest, Some(context_ref))
            })
        }),
        derived_request.as_ref().map(|(lane_request, recipes)| {
            lane("derived", move || {
                run_hrrr_derived_batch_with_context(
                    lane_request,
                    recipes,
                    timestep,
                    Some(context_ref),
                )
            })
        }),
        windowed_request.as_ref().map(|lane_request| {
            lane("windowed", move || {
                run_hrrr_windowed_batch_with_context(lane_request, latest, Some(context_ref))
            })
        }),
    );

    let (direct, derived, windowed) = match lane_result {
        Ok(reports) => reports,
        Err(err) => {
            manifest.mark_failed(err.to_string());
            publish_run_manifest(&manifest_path, &manifest)?;
            return Err(err);
        }
    };

    let summary = build_summary(&direct, &derived, &windowed);
    manifest.input_fetches = collect_input_fetches(
        request,
        &pinned_date,
        metadata.cycle_utc,
        &direct,
        &derived,
        &windowed,
    )?;
    apply_direct_manifest_updates(&mut manifest, &direct);
    apply_derived_manifest_updates(&mut manifest, &derived);
    apply_windowed_manifest_updates(
        &mut manifest,
        &windowed,
        &request.cache_root,
        request.use_cache,
        &pinned_date,
        metadata.cycle_utc,
        pinned_source,
    );
    let blocked_count = count_blocked_artifacts(&manifest);
    if blocked_count > 0 {
        manifest.mark_partial(format!("{blocked_count} artifact(s) blocked"));
    } else {
        manifest.mark_complete();
    }
    publish_run_manifest(&manifest_path, &manifest)?;
    Ok(HrrrNonEcapeHourReport {
        date_yyyymmdd: pinned_date,
        cycle_utc: latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: pinned_source,
        domain: request.domain.clone(),
        out_dir: request.out_dir.clone(),
        cache_root: request.cache_root.clone(),
        use_cache: request.use_cache,
        publication_manifest_path: manifest_path,
        requested: normalized,
        summary,
        direct,
        derived,
        windowed,
        total_ms: total_start.elapsed().as_millis(),
    })
}

fn required_projection_sizes(request: &HrrrNonEcapeHourRequestedProducts) -> Vec<(u32, u32)> {
    let mut sizes = required_direct_projection_sizes(&request.direct_recipe_slugs);
    let default_size = (1200_u32, 900_u32);
    if !sizes.contains(&default_size) {
        sizes.push(default_size);
    }
    sizes
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
    cache_root: &std::path::Path,
    use_cache: bool,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    source: SourceId,
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
        let input_fetch_keys = collect_windowed_input_fetch_keys(
            product,
            &report.shared_timing,
            cache_root,
            use_cache,
            date_yyyymmdd,
            cycle_utc,
            source,
        );
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
    let fetches = if is_qpf {
        &shared_timing.surface_hour_fetches
    } else {
        &shared_timing.uh_hour_fetches
    };
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

fn count_blocked_artifacts(manifest: &RunPublicationManifest) -> usize {
    manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.state == ArtifactPublicationState::Blocked)
        .count()
}

fn collect_input_fetches(
    request: &HrrrNonEcapeHourRequest,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    direct: &Option<HrrrDirectBatchReport>,
    derived: &Option<HrrrDerivedBatchReport>,
    windowed: &Option<HrrrWindowedBatchReport>,
) -> Result<Vec<PublishedFetchIdentity>, Box<dyn std::error::Error>> {
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
        collect_windowed_fetch_identities(report, request, date_yyyymmdd, cycle_utc, &mut by_key)?;
    }

    let mut fetches = by_key.into_values().collect::<Vec<_>>();
    fetches.sort_by(|left, right| left.fetch_key.cmp(&right.fetch_key));
    Ok(fetches)
}

fn collect_windowed_fetch_identities(
    report: &HrrrWindowedBatchReport,
    request: &HrrrNonEcapeHourRequest,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    by_key: &mut HashMap<String, PublishedFetchIdentity>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !request.use_cache {
        return Ok(());
    }

    let cycle = CycleSpec::new(date_yyyymmdd, cycle_utc)?;
    if let Some(geometry_fetch) = &report.shared_timing.geometry_fetch {
        if let Some(identity) = load_windowed_fetch_identity(
            cycle.clone(),
            report.forecast_hour,
            request.source,
            geometry_fetch,
            &request.cache_root,
        )? {
            by_key.entry(identity.fetch_key.clone()).or_insert(identity);
        }
    }

    for fetch in report
        .shared_timing
        .surface_hour_fetches
        .iter()
        .chain(report.shared_timing.uh_hour_fetches.iter())
    {
        if let Some(identity) = load_windowed_hour_fetch_identity(
            cycle.clone(),
            request.source,
            fetch,
            &request.cache_root,
        )? {
            by_key.entry(identity.fetch_key.clone()).or_insert(identity);
        }
    }

    Ok(())
}

fn collect_windowed_input_fetch_keys(
    product: &HrrrWindowedRenderedProduct,
    shared_timing: &crate::windowed::HrrrWindowedSharedTiming,
    cache_root: &std::path::Path,
    use_cache: bool,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    source: SourceId,
) -> Vec<String> {
    if !use_cache {
        return Vec::new();
    }
    let cycle = match CycleSpec::new(date_yyyymmdd, cycle_utc) {
        Ok(cycle) => cycle,
        Err(_) => return Vec::new(),
    };
    let is_qpf = matches!(
        product.product,
        HrrrWindowedProduct::Qpf1h
            | HrrrWindowedProduct::Qpf6h
            | HrrrWindowedProduct::Qpf12h
            | HrrrWindowedProduct::Qpf24h
            | HrrrWindowedProduct::QpfTotal
    );
    let fetches = if is_qpf {
        &shared_timing.surface_hour_fetches
    } else {
        &shared_timing.uh_hour_fetches
    };
    let mut keys = Vec::new();
    for fetch in fetches {
        if let Ok(Some(identity)) =
            load_windowed_hour_fetch_identity(cycle.clone(), source, fetch, cache_root)
        {
            if !keys.contains(&identity.fetch_key) {
                keys.push(identity.fetch_key);
            }
        }
    }
    keys
}

fn load_windowed_fetch_identity(
    cycle: CycleSpec,
    forecast_hour: u16,
    source: SourceId,
    fetch: &crate::hrrr::HrrrFetchRuntimeInfo,
    cache_root: &std::path::Path,
) -> Result<Option<PublishedFetchIdentity>, Box<dyn std::error::Error>> {
    let fetch_request = FetchRequest {
        request: ModelRunRequest::new(
            ModelId::Hrrr,
            cycle,
            forecast_hour,
            fetch.fetched_product.as_str(),
        )?,
        source_override: Some(source),
        variable_patterns: Vec::new(),
    };
    Ok(
        load_cached_fetch(cache_root, &fetch_request)?.map(|cached| {
            fetch_identity_from_cached_result(
                fetch.planned_product.as_str(),
                &fetch_request,
                &cached,
            )
        }),
    )
}

fn load_windowed_hour_fetch_identity(
    cycle: CycleSpec,
    source: SourceId,
    fetch: &crate::windowed::HrrrWindowedHourFetchInfo,
    cache_root: &std::path::Path,
) -> Result<Option<PublishedFetchIdentity>, Box<dyn std::error::Error>> {
    let fetch_request = FetchRequest {
        request: ModelRunRequest::new(
            ModelId::Hrrr,
            cycle,
            fetch.hour,
            fetch.fetched_product.as_str(),
        )?,
        source_override: Some(source),
        variable_patterns: Vec::new(),
    };
    Ok(
        load_cached_fetch(cache_root, &fetch_request)?.map(|cached| {
            fetch_identity_from_cached_result(
                fetch.planned_product.as_str(),
                &fetch_request,
                &cached,
            )
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derived::{
        HrrrDerivedRecipeTiming, HrrrDerivedRenderedRecipe, HrrrDerivedSharedTiming,
    };
    use crate::direct::{HrrrDirectRecipeTiming, HrrrDirectRenderedRecipe};
    use crate::hrrr::{HrrrFetchRuntimeInfo, HrrrSharedTiming};
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
                fetch_decode: HrrrSharedTiming {
                    fetch_surface_ms: 0,
                    fetch_pressure_ms: 0,
                    decode_surface_ms: 0,
                    decode_pressure_ms: 0,
                    fetch_surface_cache_hit: false,
                    fetch_pressure_cache_hit: false,
                    decode_surface_cache_hit: false,
                    decode_pressure_cache_hit: false,
                    surface_fetch: HrrrFetchRuntimeInfo {
                        planned_product: "sfc".into(),
                        fetched_product: "sfc".into(),
                        requested_source: SourceId::Aws,
                        resolved_source: SourceId::Aws,
                        resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    },
                    pressure_fetch: HrrrFetchRuntimeInfo {
                        planned_product: "prs".into(),
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
                surface_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                }],
                uh_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "nat".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
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
                fetch_decode: HrrrSharedTiming {
                    fetch_surface_ms: 0,
                    fetch_pressure_ms: 0,
                    decode_surface_ms: 0,
                    decode_pressure_ms: 0,
                    fetch_surface_cache_hit: false,
                    fetch_pressure_cache_hit: false,
                    decode_surface_cache_hit: false,
                    decode_pressure_cache_hit: false,
                    surface_fetch: HrrrFetchRuntimeInfo {
                        planned_product: "sfc".into(),
                        fetched_product: "sfc".into(),
                        requested_source: SourceId::Aws,
                        resolved_source: SourceId::Aws,
                        resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    },
                    pressure_fetch: HrrrFetchRuntimeInfo {
                        planned_product: "prs".into(),
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
                surface_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "sfc".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
                }],
                uh_hour_fetches: vec![HrrrWindowedHourFetchInfo {
                    hour: 6,
                    planned_product: "nat".into(),
                    fetched_product: "sfc".into(),
                    requested_source: SourceId::Aws,
                    resolved_source: SourceId::Aws,
                    resolved_url: "https://example.test/hrrr.t12z.wrfsfcf06.grib2".into(),
                    fetch_cache_hit: false,
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
        apply_windowed_manifest_updates(
            &mut manifest,
            &Some(windowed),
            std::path::Path::new("C:\\proof\\run\\cache"),
            false,
            "20260415",
            12,
            SourceId::Aws,
        );
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
