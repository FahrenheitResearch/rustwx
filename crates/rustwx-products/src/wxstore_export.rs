use crate::catalog::{
    ProductCatalogEntry, ProductCatalogKind, ProductTargetStatus, build_supported_products_catalog,
};
use crate::derived::{
    DerivedRecipeBlocker, build_derived_sampled_execution_plan, is_heavy_derived_recipe_slug,
    load_derived_sampled_fields_from_loaded,
};
use crate::direct::{
    DirectRecipeBlocker, build_direct_sampled_execution_plan,
    load_direct_sampled_fields_from_loaded,
};
use crate::gridded::resolve_model_run;
use crate::planner::{ExecutionPlan, ExecutionPlanBuilder};
use crate::publication::PublishedFetchIdentity;
use crate::publication::atomic_write_json;
use crate::runtime::{BundleLoaderConfig, LoadedBundleSet, load_execution_plan};
use crate::shared_context::DomainSpec;
use crate::windowed::{
    HrrrWindowedBlocker, HrrrWindowedProduct, load_windowed_sampled_fields_for_hours_from_latest,
};
use crate::wxstore_wxa::{
    WxaDense2dWriteGrid, WxaGridCrop, write_wxa_dense2d_grids, write_wxa_spatial_run_manifest,
    wxa_grid_meta_from_latlon,
};
use chrono::{Duration, NaiveDate, Utc};
use rustwx_core::{BundleRequirement, Field2D, ModelId, SourceId};
use rustwx_models::{LatestRun, plot_recipe, plot_recipe_fetch_blockers, plot_recipe_fetch_plan};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxStoreGridExportRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hours: Vec<u16>,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub product_slugs: Vec<String>,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_wxa_root: Option<PathBuf>,
    #[serde(default)]
    pub publish_wxa_latest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxStoreGridExportReport {
    pub schema: String,
    pub model: String,
    pub run_id: String,
    pub member: String,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub source: String,
    pub domain: DomainSpec,
    pub forecast_hours: Vec<u16>,
    pub generated_at: String,
    pub manifest_path: PathBuf,
    pub fields: Vec<WxStoreGridExportRecord>,
    pub blockers: Vec<WxStoreGridExportBlocker>,
    pub timing: WxStoreGridExportTiming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxStoreGridExportRecord {
    pub product_slug: String,
    pub title: String,
    pub units: String,
    pub model: String,
    pub run_id: String,
    pub member: String,
    pub forecast_hour: u16,
    pub valid_time: String,
    pub nx: usize,
    pub ny: usize,
    pub crop: Option<WxStoreGridExportCrop>,
    pub bounds: Option<[f64; 4]>,
    pub values_path: PathBuf,
    pub lat_path: PathBuf,
    pub lon_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wxa_path: Option<PathBuf>,
    pub grid_geometry: WxStoreGridGeometrySummary,
    pub no_data: WxStoreNoDataInfo,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_fetches: Vec<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct WxStoreGridExportCrop {
    pub x_start: usize,
    pub x_end: usize,
    pub y_start: usize,
    pub y_end: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxStoreGridGeometrySummary {
    pub kind: String,
    pub source_nx: usize,
    pub source_ny: usize,
    pub exported_nx: usize,
    pub exported_ny: usize,
    pub bounds: [f64; 4],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxStoreNoDataInfo {
    pub encoding: String,
    pub finite_count: usize,
    pub nan_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WxStoreGridExportBlocker {
    pub product_slug: String,
    pub forecast_hour: Option<u16>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct WxStoreGridExportTiming {
    pub total_ms: u128,
    pub load_ms: u128,
    pub write_ms: u128,
}

struct CroppedField {
    values: Vec<f32>,
    lat: Vec<f32>,
    lon: Vec<f32>,
    nx: usize,
    ny: usize,
    crop: Option<WxStoreGridExportCrop>,
    bounds: [f64; 4],
}

#[derive(Default)]
struct GeometryLatLonCache {
    entries: BTreeMap<String, GeometryLatLonEntry>,
}

#[derive(Debug, Clone)]
struct GeometryLatLonEntry {
    lat_path: PathBuf,
    lon_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct ClassifiedWxStoreProducts {
    direct_slugs: Vec<String>,
    derived_slugs: Vec<String>,
    windowed_products: Vec<HrrrWindowedProduct>,
    titles: BTreeMap<String, String>,
    blockers: Vec<WxStoreGridExportBlocker>,
}

struct ExportableSampledField {
    product_slug: String,
    title: String,
    field: Field2D,
    input_fetches: Vec<PublishedFetchIdentity>,
}

pub fn default_wxstore_export_product_slugs(model: ModelId) -> Vec<String> {
    let catalog = build_supported_products_catalog();
    let mut products = Vec::new();
    let mut seen = BTreeSet::<String>::new();

    for entry in catalog
        .direct
        .iter()
        .chain(catalog.derived.iter())
        .chain(catalog.windowed.iter())
    {
        if excluded_from_wxstore_grid_export(entry)
            || catalog_model_blocker_reason(entry, model).is_some()
        {
            continue;
        }
        if matches!(entry.kind, ProductCatalogKind::Direct)
            && !direct_recipe_exposes_sampled_grid(&entry.slug)
        {
            continue;
        }
        if seen.insert(entry.slug.clone()) {
            products.push(entry.slug.clone());
        }
    }

    products
}

pub fn export_wxstore_grid_bundle(
    request: &WxStoreGridExportRequest,
) -> Result<WxStoreGridExportReport, Box<dyn std::error::Error>> {
    if request.forecast_hours.is_empty() {
        return Err("wxstore grid export requires at least one forecast hour".into());
    }
    if request.product_slugs.is_empty() {
        return Err("wxstore grid export requires at least one product slug".into());
    }

    let total_start = Instant::now();
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let latest = resolve_model_run(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hours[0],
        request.source,
    )?;
    let run_id = run_id_for_latest(&latest);
    let manifest_dir = request.out_dir.join(&run_id).join(format!(
        "{}_{}",
        safe_slug(&request.domain.slug),
        hour_range_slug(&request.forecast_hours)
    ));
    fs::create_dir_all(&manifest_dir)?;

    let classified_products =
        classify_wxstore_export_products(request.model, &request.product_slugs);
    let mut blockers = classified_products.blockers.clone();
    let mut fields = Vec::new();
    let mut load_ms = 0u128;
    let mut write_ms = 0u128;
    let mut geometry_cache = GeometryLatLonCache::default();
    let mut direct_wxa_grids = BTreeMap::<String, Vec<WxaDense2dWriteGrid>>::new();

    for &forecast_hour in &request.forecast_hours {
        let non_windowed_loaded = if !classified_products.direct_slugs.is_empty()
            || !classified_products.derived_slugs.is_empty()
        {
            let load_start = Instant::now();
            match load_non_windowed_wxstore_bundles(
                request,
                &latest,
                forecast_hour,
                &classified_products,
            ) {
                Ok(loaded) => {
                    load_ms += load_start.elapsed().as_millis();
                    Some(loaded)
                }
                Err(err) => {
                    load_ms += load_start.elapsed().as_millis();
                    if !classified_products.direct_slugs.is_empty() {
                        blockers.extend(lane_failure_blockers(
                            "direct",
                            &classified_products.direct_slugs,
                            forecast_hour,
                            err.as_ref(),
                        ));
                    }
                    if !classified_products.derived_slugs.is_empty() {
                        blockers.extend(lane_failure_blockers(
                            "derived",
                            &classified_products.derived_slugs,
                            forecast_hour,
                            err.as_ref(),
                        ));
                    }
                    None
                }
            }
        } else {
            None
        };

        if !classified_products.direct_slugs.is_empty() {
            let load_start = Instant::now();
            if let Some(loaded) = non_windowed_loaded.as_ref() {
                match load_direct_sampled_fields_from_loaded(
                    &latest,
                    forecast_hour,
                    &request.cache_root,
                    request.use_cache,
                    &classified_products.direct_slugs,
                    loaded,
                ) {
                    Ok(sampled) => {
                        load_ms += load_start.elapsed().as_millis();

                        blockers.extend(
                            sampled
                                .blockers
                                .into_iter()
                                .map(|blocker| export_blocker_from_direct(blocker, forecast_hour)),
                        );

                        for sampled_field in sampled.fields {
                            let components = sampled_field.components;
                            let (record, elapsed_ms) = write_export_field(
                                &manifest_dir,
                                &latest,
                                request,
                                &run_id,
                                forecast_hour,
                                &mut geometry_cache,
                                &mut direct_wxa_grids,
                                ExportableSampledField {
                                    product_slug: sampled_field.recipe_slug,
                                    title: sampled_field.title,
                                    field: sampled_field.field,
                                    input_fetches: sampled_field.input_fetches,
                                },
                            )?;
                            write_ms += elapsed_ms;
                            fields.push(record);
                            for component in components {
                                let (record, elapsed_ms) = write_export_field(
                                    &manifest_dir,
                                    &latest,
                                    request,
                                    &run_id,
                                    forecast_hour,
                                    &mut geometry_cache,
                                    &mut direct_wxa_grids,
                                    ExportableSampledField {
                                        product_slug: component.product_slug,
                                        title: component.title,
                                        field: component.field,
                                        input_fetches: component.input_fetches,
                                    },
                                )?;
                                write_ms += elapsed_ms;
                                fields.push(record);
                            }
                        }
                    }
                    Err(err) => {
                        load_ms += load_start.elapsed().as_millis();
                        blockers.extend(lane_failure_blockers(
                            "direct",
                            &classified_products.direct_slugs,
                            forecast_hour,
                            err.as_ref(),
                        ));
                    }
                }
            }
        }

        if !classified_products.derived_slugs.is_empty() {
            let load_start = Instant::now();
            if let Some(loaded) = non_windowed_loaded.as_ref() {
                match load_derived_sampled_fields_from_loaded(
                    &classified_products.derived_slugs,
                    loaded,
                ) {
                    Ok(sampled) => {
                        load_ms += load_start.elapsed().as_millis();

                        blockers.extend(
                            sampled
                                .blockers
                                .into_iter()
                                .map(|blocker| export_blocker_from_derived(blocker, forecast_hour)),
                        );

                        for sampled_field in sampled.fields {
                            let title = classified_products
                                .titles
                                .get(&sampled_field.recipe_slug)
                                .cloned()
                                .unwrap_or_else(|| humanize_slug(&sampled_field.recipe_slug));
                            let (record, elapsed_ms) = write_export_field(
                                &manifest_dir,
                                &latest,
                                request,
                                &run_id,
                                forecast_hour,
                                &mut geometry_cache,
                                &mut direct_wxa_grids,
                                ExportableSampledField {
                                    product_slug: sampled_field.recipe_slug,
                                    title,
                                    field: sampled_field.field,
                                    input_fetches: sampled_field.input_fetches,
                                },
                            )?;
                            write_ms += elapsed_ms;
                            fields.push(record);
                        }
                    }
                    Err(err) => {
                        load_ms += load_start.elapsed().as_millis();
                        blockers.extend(lane_failure_blockers(
                            "derived",
                            &classified_products.derived_slugs,
                            forecast_hour,
                            err.as_ref(),
                        ));
                    }
                }
            }
        }
    }

    if !classified_products.windowed_products.is_empty() {
        let load_start = Instant::now();
        match load_windowed_sampled_fields_for_hours_from_latest(
            &latest,
            &request.forecast_hours,
            &request.cache_root,
            request.use_cache,
            &classified_products.windowed_products,
        ) {
            Ok(sampled) => {
                load_ms += load_start.elapsed().as_millis();

                blockers.extend(
                    sampled.blockers.into_iter().map(|entry| {
                        export_blocker_from_windowed(entry.blocker, entry.forecast_hour)
                    }),
                );

                for sampled_field in sampled.fields {
                    let (record, elapsed_ms) = write_export_field(
                        &manifest_dir,
                        &latest,
                        request,
                        &run_id,
                        sampled_field.forecast_hour,
                        &mut geometry_cache,
                        &mut direct_wxa_grids,
                        ExportableSampledField {
                            product_slug: sampled_field.product.slug().to_string(),
                            title: sampled_field.product.title().to_string(),
                            field: sampled_field.field,
                            input_fetches: sampled_field.input_fetches,
                        },
                    )?;
                    write_ms += elapsed_ms;
                    fields.push(record);
                }
            }
            Err(err) => {
                load_ms += load_start.elapsed().as_millis();
                let product_slugs = classified_products
                    .windowed_products
                    .iter()
                    .map(|product| product.slug().to_string())
                    .collect::<Vec<_>>();
                for &forecast_hour in &request.forecast_hours {
                    blockers.extend(lane_failure_blockers(
                        "windowed",
                        &product_slugs,
                        forecast_hour,
                        err.as_ref(),
                    ));
                }
            }
        }
    }

    fields.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
    });
    blockers.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
            .then(a.reason.cmp(&b.reason))
    });

    write_ms += write_direct_wxa_products(request, &run_id, direct_wxa_grids)?;

    let manifest_path = manifest_dir.join("manifest.json");
    let report = WxStoreGridExportReport {
        schema: "rustwx.wxstore_grid_export.v1".to_string(),
        model: request.model.as_str().to_string(),
        run_id,
        member: "control".to_string(),
        date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
        cycle_utc: latest.cycle.hour_utc,
        source: latest.source.as_str().to_string(),
        domain: request.domain.clone(),
        forecast_hours: request.forecast_hours.clone(),
        generated_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        manifest_path: manifest_path.clone(),
        fields,
        blockers,
        timing: WxStoreGridExportTiming {
            total_ms: total_start.elapsed().as_millis(),
            load_ms,
            write_ms,
        },
    };
    atomic_write_json(&manifest_path, &report)?;
    publish_direct_wxa_run_manifest(request, &report, &manifest_path)?;
    Ok(report)
}

pub(crate) fn export_wxstore_grid_bundle_from_loaded(
    request: &WxStoreGridExportRequest,
    loaded: &LoadedBundleSet,
) -> Result<WxStoreGridExportReport, Box<dyn std::error::Error>> {
    export_wxstore_grid_bundle_from_loaded_parts(request, Some(loaded), Some(loaded))
}

pub(crate) fn export_wxstore_grid_bundle_from_loaded_parts(
    request: &WxStoreGridExportRequest,
    direct_loaded: Option<&LoadedBundleSet>,
    derived_loaded: Option<&LoadedBundleSet>,
) -> Result<WxStoreGridExportReport, Box<dyn std::error::Error>> {
    if request.forecast_hours.len() != 1 {
        return Err("loaded WxStore export currently requires exactly one forecast hour".into());
    }
    let forecast_hour = request.forecast_hours[0];
    let latest = direct_loaded
        .or(derived_loaded)
        .map(|loaded| &loaded.latest)
        .ok_or("loaded WxStore export requires at least one loaded bundle set")?;
    for loaded in direct_loaded.into_iter().chain(derived_loaded.into_iter()) {
        if loaded.forecast_hour != forecast_hour {
            return Err(format!(
                "loaded WxStore export forecast-hour mismatch: request f{forecast_hour:03}, loaded f{:03}",
                loaded.forecast_hour
            )
            .into());
        }
        if loaded.latest.model != latest.model
            || loaded.latest.cycle != latest.cycle
            || loaded.latest.source != latest.source
        {
            return Err(
                "loaded WxStore export got mismatched direct/derived run identities".into(),
            );
        }
    }

    let total_start = Instant::now();
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let run_id = run_id_for_latest(latest);
    let manifest_dir = request.out_dir.join(&run_id).join(format!(
        "{}_{}",
        safe_slug(&request.domain.slug),
        hour_range_slug(&request.forecast_hours)
    ));
    fs::create_dir_all(&manifest_dir)?;

    let classified_products =
        classify_wxstore_export_products(request.model, &request.product_slugs);
    let mut blockers = classified_products.blockers.clone();
    let mut fields = Vec::new();
    let mut load_ms = 0u128;
    let mut write_ms = 0u128;
    let mut geometry_cache = GeometryLatLonCache::default();
    let mut direct_wxa_grids = BTreeMap::<String, Vec<WxaDense2dWriteGrid>>::new();

    if !classified_products.direct_slugs.is_empty() {
        let load_start = Instant::now();
        if let Some(loaded) = direct_loaded {
            match load_direct_sampled_fields_from_loaded(
                latest,
                forecast_hour,
                &request.cache_root,
                request.use_cache,
                &classified_products.direct_slugs,
                loaded,
            ) {
                Ok(sampled) => {
                    load_ms += load_start.elapsed().as_millis();
                    blockers.extend(
                        sampled
                            .blockers
                            .into_iter()
                            .map(|blocker| export_blocker_from_direct(blocker, forecast_hour)),
                    );
                    for sampled_field in sampled.fields {
                        let components = sampled_field.components;
                        let (record, elapsed_ms) = write_export_field(
                            &manifest_dir,
                            latest,
                            request,
                            &run_id,
                            forecast_hour,
                            &mut geometry_cache,
                            &mut direct_wxa_grids,
                            ExportableSampledField {
                                product_slug: sampled_field.recipe_slug,
                                title: sampled_field.title,
                                field: sampled_field.field,
                                input_fetches: sampled_field.input_fetches,
                            },
                        )?;
                        write_ms += elapsed_ms;
                        fields.push(record);
                        for component in components {
                            let (record, elapsed_ms) = write_export_field(
                                &manifest_dir,
                                latest,
                                request,
                                &run_id,
                                forecast_hour,
                                &mut geometry_cache,
                                &mut direct_wxa_grids,
                                ExportableSampledField {
                                    product_slug: component.product_slug,
                                    title: component.title,
                                    field: component.field,
                                    input_fetches: component.input_fetches,
                                },
                            )?;
                            write_ms += elapsed_ms;
                            fields.push(record);
                        }
                    }
                }
                Err(err) => {
                    load_ms += load_start.elapsed().as_millis();
                    blockers.extend(lane_failure_blockers(
                        "direct",
                        &classified_products.direct_slugs,
                        forecast_hour,
                        err.as_ref(),
                    ));
                }
            }
        } else {
            load_ms += load_start.elapsed().as_millis();
            blockers.extend(lane_failure_blockers(
                "direct",
                &classified_products.direct_slugs,
                forecast_hour,
                &std::io::Error::other("no direct bundle set was prepared"),
            ));
        }
    }

    if !classified_products.derived_slugs.is_empty() {
        let load_start = Instant::now();
        if let Some(loaded) = derived_loaded {
            match load_derived_sampled_fields_from_loaded(
                &classified_products.derived_slugs,
                loaded,
            ) {
                Ok(sampled) => {
                    load_ms += load_start.elapsed().as_millis();
                    blockers.extend(
                        sampled
                            .blockers
                            .into_iter()
                            .map(|blocker| export_blocker_from_derived(blocker, forecast_hour)),
                    );
                    for sampled_field in sampled.fields {
                        let title = classified_products
                            .titles
                            .get(&sampled_field.recipe_slug)
                            .cloned()
                            .unwrap_or_else(|| humanize_slug(&sampled_field.recipe_slug));
                        let (record, elapsed_ms) = write_export_field(
                            &manifest_dir,
                            latest,
                            request,
                            &run_id,
                            forecast_hour,
                            &mut geometry_cache,
                            &mut direct_wxa_grids,
                            ExportableSampledField {
                                product_slug: sampled_field.recipe_slug,
                                title,
                                field: sampled_field.field,
                                input_fetches: sampled_field.input_fetches,
                            },
                        )?;
                        write_ms += elapsed_ms;
                        fields.push(record);
                    }
                }
                Err(err) => {
                    load_ms += load_start.elapsed().as_millis();
                    blockers.extend(lane_failure_blockers(
                        "derived",
                        &classified_products.derived_slugs,
                        forecast_hour,
                        err.as_ref(),
                    ));
                }
            }
        } else {
            load_ms += load_start.elapsed().as_millis();
            blockers.extend(lane_failure_blockers(
                "derived",
                &classified_products.derived_slugs,
                forecast_hour,
                &std::io::Error::other("no derived bundle set was prepared"),
            ));
        }
    }

    if !classified_products.windowed_products.is_empty() {
        let load_start = Instant::now();
        match load_windowed_sampled_fields_for_hours_from_latest(
            latest,
            &request.forecast_hours,
            &request.cache_root,
            request.use_cache,
            &classified_products.windowed_products,
        ) {
            Ok(sampled) => {
                load_ms += load_start.elapsed().as_millis();
                blockers.extend(
                    sampled.blockers.into_iter().map(|entry| {
                        export_blocker_from_windowed(entry.blocker, entry.forecast_hour)
                    }),
                );
                for sampled_field in sampled.fields {
                    let (record, elapsed_ms) = write_export_field(
                        &manifest_dir,
                        latest,
                        request,
                        &run_id,
                        sampled_field.forecast_hour,
                        &mut geometry_cache,
                        &mut direct_wxa_grids,
                        ExportableSampledField {
                            product_slug: sampled_field.product.slug().to_string(),
                            title: sampled_field.product.title().to_string(),
                            field: sampled_field.field,
                            input_fetches: sampled_field.input_fetches,
                        },
                    )?;
                    write_ms += elapsed_ms;
                    fields.push(record);
                }
            }
            Err(err) => {
                load_ms += load_start.elapsed().as_millis();
                let product_slugs = classified_products
                    .windowed_products
                    .iter()
                    .map(|product| product.slug().to_string())
                    .collect::<Vec<_>>();
                blockers.extend(lane_failure_blockers(
                    "windowed",
                    &product_slugs,
                    forecast_hour,
                    err.as_ref(),
                ));
            }
        }
    }

    fields.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
    });
    blockers.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
            .then(a.reason.cmp(&b.reason))
    });

    write_ms += write_direct_wxa_products(request, &run_id, direct_wxa_grids)?;

    let manifest_path = manifest_dir.join("manifest.json");
    let report = WxStoreGridExportReport {
        schema: "rustwx.wxstore_grid_export.v1".to_string(),
        model: request.model.as_str().to_string(),
        run_id,
        member: "control".to_string(),
        date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
        cycle_utc: latest.cycle.hour_utc,
        source: latest.source.as_str().to_string(),
        domain: request.domain.clone(),
        forecast_hours: request.forecast_hours.clone(),
        generated_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        manifest_path: manifest_path.clone(),
        fields,
        blockers,
        timing: WxStoreGridExportTiming {
            total_ms: total_start.elapsed().as_millis(),
            load_ms,
            write_ms,
        },
    };
    atomic_write_json(&manifest_path, &report)?;
    publish_direct_wxa_run_manifest(request, &report, &manifest_path)?;
    Ok(report)
}

fn write_direct_wxa_products(
    request: &WxStoreGridExportRequest,
    run_id: &str,
    direct_wxa_grids: BTreeMap<String, Vec<WxaDense2dWriteGrid>>,
) -> Result<u128, Box<dyn std::error::Error>> {
    let Some(spatial_root) = request.direct_wxa_root.as_ref() else {
        return Ok(0);
    };
    if direct_wxa_grids.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    for (product, grids) in direct_wxa_grids {
        write_wxa_dense2d_grids(
            spatial_root,
            request.model.as_str(),
            run_id,
            Some("control"),
            &product,
            &grids,
        )?;
    }
    Ok(started.elapsed().as_millis())
}

fn publish_direct_wxa_run_manifest(
    request: &WxStoreGridExportRequest,
    report: &WxStoreGridExportReport,
    manifest_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(spatial_root) = request.direct_wxa_root.as_ref() else {
        return Ok(());
    };
    if report.fields.is_empty() {
        return Ok(());
    }
    let blocker_values = report
        .blockers
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    write_wxa_spatial_run_manifest(
        spatial_root,
        &report.model,
        &report.run_id,
        &report.source,
        Some(manifest_path),
        &blocker_values,
        report.timing.total_ms,
        request.publish_wxa_latest,
    )?;
    Ok(())
}

fn load_non_windowed_wxstore_bundles(
    request: &WxStoreGridExportRequest,
    latest: &LatestRun,
    forecast_hour: u16,
    classified_products: &ClassifiedWxStoreProducts,
) -> Result<LoadedBundleSet, Box<dyn std::error::Error>> {
    let mut builder = ExecutionPlanBuilder::new(latest, forecast_hour);

    if !classified_products.direct_slugs.is_empty() {
        let direct_plan = build_direct_sampled_execution_plan(
            latest,
            forecast_hour,
            &request.cache_root,
            request.use_cache,
            &classified_products.direct_slugs,
        )?;
        merge_execution_plan(&mut builder, &direct_plan);
    }

    if !classified_products.derived_slugs.is_empty() {
        let derived_plan = build_derived_sampled_execution_plan(
            latest,
            forecast_hour,
            &classified_products.derived_slugs,
        )?;
        merge_execution_plan(&mut builder, &derived_plan);
    }

    load_execution_plan(
        builder.build(),
        &BundleLoaderConfig::new(request.cache_root.clone(), request.use_cache),
    )
}

fn merge_execution_plan(builder: &mut ExecutionPlanBuilder, plan: &ExecutionPlan) {
    for bundle in &plan.bundles {
        for alias in &bundle.aliases {
            let mut requirement = BundleRequirement::new(alias.bundle, bundle.id.forecast_hour);
            if let Some(native_override) = &alias.native_override {
                requirement = requirement.with_native_override(native_override.clone());
            }
            builder.require_with_logical_family_and_patterns(
                &requirement,
                alias.logical_family.as_deref(),
                alias.variable_patterns.clone(),
            );
        }
    }
}

fn classify_wxstore_export_products(
    model: ModelId,
    requested: &[String],
) -> ClassifiedWxStoreProducts {
    let catalog = build_supported_products_catalog();
    let mut classified = ClassifiedWxStoreProducts::default();
    let mut seen_direct = BTreeSet::<String>::new();
    let mut seen_derived = BTreeSet::<String>::new();
    let mut seen_windowed = BTreeSet::<String>::new();

    for slug in requested {
        let normalized = normalize_product_slug(slug);
        if normalized.contains("ecape") || is_heavy_derived_recipe_slug(&normalized) {
            classified.blockers.push(WxStoreGridExportBlocker {
                product_slug: slug.clone(),
                forecast_hour: None,
                reason: "ECAPE/heavy products are excluded from WxStore grid export".to_string(),
            });
            continue;
        };

        let Some(entry) = find_catalog_entry(&catalog, slug) else {
            classify_direct_fallback(model, slug, &mut classified, &mut seen_direct);
            continue;
        };

        if excluded_from_wxstore_grid_export(entry) {
            classified.blockers.push(WxStoreGridExportBlocker {
                product_slug: entry.slug.clone(),
                forecast_hour: None,
                reason: "ECAPE/heavy products are excluded from WxStore grid export".to_string(),
            });
            continue;
        }

        if let Some(reason) = catalog_model_blocker_reason(entry, model) {
            classified.blockers.push(WxStoreGridExportBlocker {
                product_slug: entry.slug.clone(),
                forecast_hour: None,
                reason,
            });
            continue;
        }

        classified
            .titles
            .insert(entry.slug.clone(), entry.title.clone());

        match entry.kind {
            ProductCatalogKind::Direct => {
                classify_direct_catalog_entry(model, entry, &mut classified, &mut seen_direct);
            }
            ProductCatalogKind::Derived => {
                if seen_derived.insert(entry.slug.clone()) {
                    classified.derived_slugs.push(entry.slug.clone());
                }
            }
            ProductCatalogKind::Windowed => match windowed_product_from_slug(&entry.slug) {
                Some(product) => {
                    if seen_windowed.insert(product.slug().to_string()) {
                        classified.windowed_products.push(product);
                    }
                }
                None => classified.blockers.push(WxStoreGridExportBlocker {
                    product_slug: entry.slug.clone(),
                    forecast_hour: None,
                    reason: "catalog windowed product is not wired to the windowed sampler"
                        .to_string(),
                }),
            },
            ProductCatalogKind::Heavy => {
                classified.blockers.push(WxStoreGridExportBlocker {
                    product_slug: entry.slug.clone(),
                    forecast_hour: None,
                    reason: "ECAPE/heavy products are excluded from WxStore grid export"
                        .to_string(),
                });
            }
        }
    }

    classified
}

fn classify_direct_catalog_entry(
    model: ModelId,
    entry: &ProductCatalogEntry,
    classified: &mut ClassifiedWxStoreProducts,
    seen_direct: &mut BTreeSet<String>,
) {
    let Some(recipe) = plot_recipe(&entry.slug) else {
        classified.blockers.push(WxStoreGridExportBlocker {
            product_slug: entry.slug.clone(),
            forecast_hour: None,
            reason: "catalog direct product is not wired to a rustwx plot recipe".to_string(),
        });
        return;
    };
    if recipe.filled.selector.is_none() {
        classified.blockers.push(WxStoreGridExportBlocker {
            product_slug: entry.slug.clone(),
            forecast_hour: None,
            reason: "direct recipe does not expose a single sampled filled field".to_string(),
        });
        return;
    }
    match direct_fetch_plan_blocker_reason(recipe.slug, model) {
        Some(reason) => classified.blockers.push(WxStoreGridExportBlocker {
            product_slug: recipe.slug.to_string(),
            forecast_hour: None,
            reason,
        }),
        None => {
            if seen_direct.insert(recipe.slug.to_string()) {
                classified.direct_slugs.push(recipe.slug.to_string());
            }
        }
    }
}

fn classify_direct_fallback(
    model: ModelId,
    requested_slug: &str,
    classified: &mut ClassifiedWxStoreProducts,
    seen_direct: &mut BTreeSet<String>,
) {
    let Some(recipe) = plot_recipe(requested_slug) else {
        classified.blockers.push(WxStoreGridExportBlocker {
            product_slug: requested_slug.to_string(),
            forecast_hour: None,
            reason: "unknown rustwx product slug".to_string(),
        });
        return;
    };
    if recipe.filled.selector.is_none() {
        classified.blockers.push(WxStoreGridExportBlocker {
            product_slug: recipe.slug.to_string(),
            forecast_hour: None,
            reason: "direct recipe does not expose a single sampled filled field".to_string(),
        });
        return;
    }
    match direct_fetch_plan_blocker_reason(recipe.slug, model) {
        Some(reason) => classified.blockers.push(WxStoreGridExportBlocker {
            product_slug: recipe.slug.to_string(),
            forecast_hour: None,
            reason,
        }),
        None => {
            classified
                .titles
                .insert(recipe.slug.to_string(), recipe.title.to_string());
            if seen_direct.insert(recipe.slug.to_string()) {
                classified.direct_slugs.push(recipe.slug.to_string());
            }
        }
    }
}

fn find_catalog_entry<'a>(
    catalog: &'a crate::catalog::SupportedProductsCatalog,
    requested_slug: &str,
) -> Option<&'a ProductCatalogEntry> {
    let wanted = normalize_product_slug(requested_slug);
    catalog
        .direct
        .iter()
        .chain(catalog.derived.iter())
        .chain(catalog.heavy.iter())
        .chain(catalog.windowed.iter())
        .find(|entry| {
            normalize_product_slug(&entry.slug) == wanted
                || entry
                    .aliases
                    .iter()
                    .any(|alias| normalize_product_slug(&alias.slug) == wanted)
        })
}

fn excluded_from_wxstore_grid_export(entry: &ProductCatalogEntry) -> bool {
    matches!(entry.kind, ProductCatalogKind::Heavy)
        || normalize_product_slug(&entry.slug).contains("ecape")
        || is_heavy_derived_recipe_slug(&entry.slug)
}

fn catalog_model_blocker_reason(entry: &ProductCatalogEntry, model: ModelId) -> Option<String> {
    let model_slug = model.as_str();
    let target = entry
        .support
        .iter()
        .find(|target| target.model == Some(model) || target.target == model_slug);
    match target {
        Some(target) if matches!(target.status, ProductTargetStatus::Supported) => None,
        Some(target) => Some(if target.blockers.is_empty() {
            format!(
                "product '{}' is blocked for model {}",
                entry.slug,
                model.as_str()
            )
        } else {
            target.blockers.join("; ")
        }),
        None => Some(format!(
            "product '{}' is not listed as supported for model {}",
            entry.slug,
            model.as_str()
        )),
    }
}

fn direct_fetch_plan_blocker_reason(recipe_slug: &str, model: ModelId) -> Option<String> {
    match plot_recipe_fetch_plan(recipe_slug, model) {
        Ok(_) => None,
        Err(err) => Some(
            plot_recipe_fetch_blockers(recipe_slug, model)
                .map(|blockers| {
                    if blockers.is_empty() {
                        err.to_string()
                    } else {
                        blockers
                            .into_iter()
                            .map(|blocker| format!("{}: {}", blocker.field_label, blocker.reason))
                            .collect::<Vec<_>>()
                            .join("; ")
                    }
                })
                .unwrap_or_else(|_| err.to_string()),
        ),
    }
}

fn direct_recipe_exposes_sampled_grid(slug: &str) -> bool {
    plot_recipe(slug)
        .map(|recipe| recipe.filled.selector.is_some())
        .unwrap_or(false)
}

fn windowed_product_from_slug(slug: &str) -> Option<HrrrWindowedProduct> {
    let wanted = normalize_product_slug(slug);
    HrrrWindowedProduct::supported_products()
        .iter()
        .copied()
        .find(|product| normalize_product_slug(product.slug()) == wanted)
}

fn normalize_product_slug(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_was_separator = false;
    for ch in value.trim().chars() {
        let normalized = ch.to_ascii_lowercase();
        if normalized.is_ascii_alphanumeric() {
            out.push(normalized);
            last_was_separator = false;
        } else if !last_was_separator {
            out.push('_');
            last_was_separator = true;
        }
    }
    out.trim_matches('_').to_string()
}

fn humanize_slug(value: &str) -> String {
    value.replace('_', " ")
}

fn export_blocker_from_direct(
    blocker: DirectRecipeBlocker,
    forecast_hour: u16,
) -> WxStoreGridExportBlocker {
    WxStoreGridExportBlocker {
        product_slug: blocker.recipe_slug,
        forecast_hour: Some(forecast_hour),
        reason: blocker.reason,
    }
}

fn export_blocker_from_derived(
    blocker: DerivedRecipeBlocker,
    forecast_hour: u16,
) -> WxStoreGridExportBlocker {
    WxStoreGridExportBlocker {
        product_slug: blocker.recipe_slug,
        forecast_hour: Some(forecast_hour),
        reason: blocker.reason,
    }
}

fn export_blocker_from_windowed(
    blocker: HrrrWindowedBlocker,
    forecast_hour: u16,
) -> WxStoreGridExportBlocker {
    WxStoreGridExportBlocker {
        product_slug: blocker.product.slug().to_string(),
        forecast_hour: Some(forecast_hour),
        reason: blocker.reason,
    }
}

fn lane_failure_blockers(
    lane: &str,
    product_slugs: &[String],
    forecast_hour: u16,
    err: &dyn std::error::Error,
) -> Vec<WxStoreGridExportBlocker> {
    let reason = format!("{lane} lane failed: {err}");
    product_slugs
        .iter()
        .map(|product_slug| WxStoreGridExportBlocker {
            product_slug: product_slug.clone(),
            forecast_hour: Some(forecast_hour),
            reason: reason.clone(),
        })
        .collect()
}

fn write_export_field(
    manifest_dir: &Path,
    latest: &LatestRun,
    request: &WxStoreGridExportRequest,
    run_id: &str,
    forecast_hour: u16,
    geometry_cache: &mut GeometryLatLonCache,
    direct_wxa_grids: &mut BTreeMap<String, Vec<WxaDense2dWriteGrid>>,
    sampled_field: ExportableSampledField,
) -> Result<(WxStoreGridExportRecord, u128), Box<dyn std::error::Error>> {
    let write_start = Instant::now();
    let source_nx = sampled_field.field.grid.shape.nx;
    let source_ny = sampled_field.field.grid.shape.ny;
    let units = sampled_field.field.units.clone();
    let cropped = crop_field_to_domain(&sampled_field.field, request.domain.bounds)?;
    let prefix = format!(
        "{}_f{:03}",
        safe_slug(&sampled_field.product_slug),
        forecast_hour
    );
    let (values_path, lat_path, lon_path, wxa_path) =
        if let Some(spatial_root) = request.direct_wxa_root.as_ref() {
            let member = Some("control");
            let wxa_path = crate::wxstore_wxa::wxa_product_path(
                spatial_root,
                request.model.as_str(),
                run_id,
                member,
                &sampled_field.product_slug,
            );
            let grid_meta = wxa_grid_meta_from_latlon(
                request.model.as_str(),
                cropped.nx,
                cropped.ny,
                &cropped.lat,
                &cropped.lon,
                cropped.crop.map(|crop| WxaGridCrop {
                    x_start: crop.x_start,
                    x_end: crop.x_end,
                    y_start: crop.y_start,
                    y_end: crop.y_end,
                }),
                Some(cropped.bounds),
            );
            direct_wxa_grids
                .entry(sampled_field.product_slug.clone())
                .or_default()
                .push(WxaDense2dWriteGrid {
                    model: request.model.as_str().to_string(),
                    run_id: run_id.to_string(),
                    member: member.map(str::to_string),
                    product_slug: sampled_field.product_slug.clone(),
                    units: units.clone(),
                    forecast_hour: u32::from(forecast_hour),
                    nx: cropped.nx,
                    ny: cropped.ny,
                    grid_meta,
                    values: cropped.values.clone(),
                });
            (
                PathBuf::new(),
                PathBuf::new(),
                PathBuf::new(),
                Some(wxa_path),
            )
        } else {
            let values_path = PathBuf::from(format!("{prefix}_values.f32"));
            write_f32_file(&manifest_dir.join(&values_path), &cropped.values)?;
            let (lat_path, lon_path) =
                geometry_cache.latlon_paths(manifest_dir, source_nx, source_ny, &cropped)?;
            (values_path, lat_path, lon_path, None)
        };
    let elapsed_ms = write_start.elapsed().as_millis();

    let no_data = no_data_info(&cropped.values);
    Ok((
        WxStoreGridExportRecord {
            product_slug: sampled_field.product_slug,
            title: sampled_field.title,
            units,
            model: request.model.as_str().to_string(),
            run_id: run_id.to_string(),
            member: "control".to_string(),
            forecast_hour,
            valid_time: valid_time_utc(latest, forecast_hour)?,
            nx: cropped.nx,
            ny: cropped.ny,
            crop: cropped.crop,
            bounds: Some(cropped.bounds),
            values_path,
            lat_path,
            lon_path,
            wxa_path,
            grid_geometry: WxStoreGridGeometrySummary {
                kind: "lat_lon_arrays".to_string(),
                source_nx,
                source_ny,
                exported_nx: cropped.nx,
                exported_ny: cropped.ny,
                bounds: cropped.bounds,
            },
            no_data,
            input_fetches: sampled_field
                .input_fetches
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        },
        elapsed_ms,
    ))
}

fn crop_field_to_domain(
    field: &Field2D,
    bounds: (f64, f64, f64, f64),
) -> Result<CroppedField, Box<dyn std::error::Error>> {
    let nx = field.grid.shape.nx;
    let ny = field.grid.shape.ny;
    if nx == 0 || ny == 0 {
        return Err("cannot export an empty grid".into());
    }
    let len = nx * ny;
    if field.values.len() != len
        || field.grid.lat_deg.len() != len
        || field.grid.lon_deg.len() != len
    {
        return Err("field values and lat/lon arrays have inconsistent dimensions".into());
    }

    let mut x_min = usize::MAX;
    let mut y_min = usize::MAX;
    let mut x_max = 0usize;
    let mut y_max = 0usize;
    let mut matched = 0usize;

    for y in 0..ny {
        for x in 0..nx {
            let index = y * nx + x;
            let lat = field.grid.lat_deg[index] as f64;
            let lon = field.grid.lon_deg[index] as f64;
            if point_in_bounds(lat, lon, bounds) {
                x_min = x_min.min(x);
                y_min = y_min.min(y);
                x_max = x_max.max(x);
                y_max = y_max.max(y);
                matched += 1;
            }
        }
    }

    let (x_start, y_start, x_end, y_end, crop) = if matched == 0 {
        return Err(format!("domain '{}' did not intersect field grid", field.product).into());
    } else if x_min == 0 && y_min == 0 && x_max + 1 == nx && y_max + 1 == ny {
        (0, 0, nx, ny, None)
    } else {
        let crop = WxStoreGridExportCrop {
            x_start: x_min,
            x_end: x_max + 1,
            y_start: y_min,
            y_end: y_max + 1,
        };
        (
            crop.x_start,
            crop.y_start,
            crop.x_end,
            crop.y_end,
            Some(crop),
        )
    };

    let out_nx = x_end - x_start;
    let out_ny = y_end - y_start;
    let mut values = Vec::with_capacity(out_nx * out_ny);
    let mut lat = Vec::with_capacity(out_nx * out_ny);
    let mut lon = Vec::with_capacity(out_nx * out_ny);
    for y in y_start..y_end {
        let row_start = y * nx;
        for x in x_start..x_end {
            let index = row_start + x;
            values.push(field.values[index]);
            lat.push(field.grid.lat_deg[index]);
            lon.push(normalize_lon(field.grid.lon_deg[index] as f64) as f32);
        }
    }

    let exported_bounds = bounds_from_latlon(&lat, &lon).unwrap_or([
        bounds.0.min(bounds.1),
        bounds.2.min(bounds.3),
        bounds.0.max(bounds.1),
        bounds.2.max(bounds.3),
    ]);
    Ok(CroppedField {
        values,
        lat,
        lon,
        nx: out_nx,
        ny: out_ny,
        crop,
        bounds: exported_bounds,
    })
}

impl GeometryLatLonCache {
    fn latlon_paths(
        &mut self,
        manifest_dir: &Path,
        source_nx: usize,
        source_ny: usize,
        cropped: &CroppedField,
    ) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let key = geometry_cache_key(source_nx, source_ny, cropped);
        if let Some(entry) = self.entries.get(&key) {
            return Ok((entry.lat_path.clone(), entry.lon_path.clone()));
        }

        let prefix = format!(
            "grid_{:03}_{}x{}_{}x{}",
            self.entries.len(),
            source_nx,
            source_ny,
            cropped.nx,
            cropped.ny
        );
        let lat_path = PathBuf::from(format!("{prefix}_lat.f32"));
        let lon_path = PathBuf::from(format!("{prefix}_lon.f32"));
        write_f32_file(&manifest_dir.join(&lat_path), &cropped.lat)?;
        write_f32_file(&manifest_dir.join(&lon_path), &cropped.lon)?;
        self.entries.insert(
            key,
            GeometryLatLonEntry {
                lat_path: lat_path.clone(),
                lon_path: lon_path.clone(),
            },
        );
        Ok((lat_path, lon_path))
    }
}

fn geometry_cache_key(source_nx: usize, source_ny: usize, cropped: &CroppedField) -> String {
    let crop = cropped
        .crop
        .map(|crop| {
            format!(
                "{}:{}:{}:{}",
                crop.x_start, crop.x_end, crop.y_start, crop.y_end
            )
        })
        .unwrap_or_else(|| "full".to_string());
    let bounds = cropped
        .bounds
        .iter()
        .map(|value| format!("{:016x}", value.to_bits()))
        .collect::<Vec<_>>()
        .join(":");
    format!(
        "{source_nx}x{source_ny}|{}x{}|{crop}|{bounds}",
        cropped.nx, cropped.ny
    )
}

fn write_f32_file(path: &Path, values: &[f32]) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = BufWriter::new(File::create(path)?);
    let mut bytes = Vec::with_capacity(64 * 1024 * 4);
    for chunk in values.chunks(64 * 1024) {
        bytes.clear();
        for value in chunk {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        file.write_all(&bytes)?;
    }
    file.flush()?;
    Ok(())
}

fn no_data_info(values: &[f32]) -> WxStoreNoDataInfo {
    let finite_count = values.iter().filter(|value| value.is_finite()).count();
    WxStoreNoDataInfo {
        encoding: "nan".to_string(),
        finite_count,
        nan_count: values.len().saturating_sub(finite_count),
    }
}

fn bounds_from_latlon(lat: &[f32], lon: &[f32]) -> Option<[f64; 4]> {
    if lat.len() != lon.len() || lat.is_empty() {
        return None;
    }
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for (&lat, &lon) in lat.iter().zip(lon.iter()) {
        let lat = lat as f64;
        let lon = normalize_lon(lon as f64);
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        west = west.min(lon);
        east = east.max(lon);
        south = south.min(lat);
        north = north.max(lat);
    }
    west.is_finite().then_some([west, south, east, north])
}

fn point_in_bounds(lat: f64, lon: f64, bounds: (f64, f64, f64, f64)) -> bool {
    if !lat.is_finite() || !lon.is_finite() || lat < bounds.2 || lat > bounds.3 {
        return false;
    }
    let west = normalize_lon(bounds.0);
    let east = normalize_lon(bounds.1);
    let lon = normalize_lon(lon);
    if west <= east {
        lon >= west && lon <= east
    } else {
        lon >= west || lon <= east
    }
}

fn normalize_lon(lon: f64) -> f64 {
    let mut lon = lon;
    while lon > 180.0 {
        lon -= 360.0;
    }
    while lon <= -180.0 {
        lon += 360.0;
    }
    lon
}

fn valid_time_utc(
    latest: &LatestRun,
    forecast_hour: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    let date = NaiveDate::parse_from_str(&latest.cycle.date_yyyymmdd, "%Y%m%d")?;
    let cycle = date
        .and_hms_opt(u32::from(latest.cycle.hour_utc), 0, 0)
        .ok_or("invalid cycle hour")?;
    let valid = cycle + Duration::hours(i64::from(forecast_hour));
    Ok(valid.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn run_id_for_latest(latest: &LatestRun) -> String {
    format!(
        "{}_{}_{:02}z",
        latest.cycle.date_yyyymmdd,
        latest.model.as_str().replace('-', "_"),
        latest.cycle.hour_utc
    )
}

fn hour_range_slug(hours: &[u16]) -> String {
    if let (Some(first), Some(last)) = (hours.first(), hours.last()) {
        if hours.windows(2).all(|pair| pair[1] == pair[0] + 1) {
            if first == last {
                return format!("f{first:03}");
            }
            return format!("f{first:03}_f{last:03}");
        }
        if hours.len() > 2 {
            let step = hours[1].saturating_sub(hours[0]);
            if step > 0 && hours.windows(2).all(|pair| pair[1] == pair[0] + step) {
                return format!("f{first:03}_f{last:03}_step{step:03}");
            }
            if hours.len() > 12 {
                return format!("f{first:03}_f{last:03}_n{}", hours.len());
            }
        }
    }
    hours
        .iter()
        .map(|hour| format!("f{hour:03}"))
        .collect::<Vec<_>>()
        .join("_")
}

fn safe_slug(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{GridShape, LatLonGrid, ProductKey};

    fn sample_field() -> Field2D {
        let grid = LatLonGrid::new(
            GridShape::new(4, 3).unwrap(),
            vec![
                20.0, 20.0, 20.0, 20.0, 30.0, 30.0, 30.0, 30.0, 40.0, 40.0, 40.0, 40.0,
            ],
            vec![
                -130.0, -120.0, -110.0, -100.0, -130.0, -120.0, -110.0, -100.0, -130.0, -120.0,
                -110.0, -100.0,
            ],
        )
        .unwrap();
        Field2D::new(
            ProductKey::named("sample"),
            "unit",
            grid,
            (0..12).map(|value| value as f32).collect(),
        )
        .unwrap()
    }

    #[test]
    fn crop_field_to_domain_uses_exclusive_crop_bounds() {
        let cropped = crop_field_to_domain(&sample_field(), (-121.0, -109.0, 25.0, 41.0)).unwrap();
        assert_eq!(cropped.nx, 2);
        assert_eq!(cropped.ny, 2);
        assert_eq!(
            cropped.crop,
            Some(WxStoreGridExportCrop {
                x_start: 1,
                x_end: 3,
                y_start: 1,
                y_end: 3
            })
        );
        assert_eq!(cropped.values, vec![5.0, 6.0, 9.0, 10.0]);
    }

    #[test]
    fn valid_time_uses_cycle_plus_forecast_hour() {
        let latest = LatestRun {
            model: ModelId::Gfs,
            cycle: rustwx_core::CycleSpec::new("20260430", 18).unwrap(),
            source: SourceId::Aws,
        };
        assert_eq!(valid_time_utc(&latest, 9).unwrap(), "2026-05-01T03:00:00Z");
    }

    #[test]
    fn default_products_include_all_supported_non_ecape_grid_lanes() {
        let products = default_wxstore_export_product_slugs(ModelId::Hrrr);
        assert!(products.contains(&"2m_temperature".to_string()));
        assert!(products.contains(&"mlcape".to_string()));
        assert!(products.contains(&"qpf_1h".to_string()));
        assert!(products.contains(&"total_qpf".to_string()));
        assert!(products.contains(&"scp_mu_0_3km_0_6km_proxy".to_string()));
        assert!(!products.contains(&"sbecape".to_string()));
        assert!(!products.contains(&"severe_proof_panel".to_string()));
        assert!(!products.contains(&"cloud_cover_levels".to_string()));
        assert!(products.iter().all(|slug| !slug.contains("ecape")));
    }

    #[test]
    fn classification_routes_aliases_and_blocks_ecape() {
        let products = classify_wxstore_export_products(
            ModelId::Hrrr,
            &[
                "1h_qpf".to_string(),
                "2m_theta_e_10m_winds".to_string(),
                "sbecape".to_string(),
            ],
        );
        assert!(products.direct_slugs.is_empty());
        assert_eq!(products.derived_slugs, vec!["theta_e_2m_10m_winds"]);
        assert_eq!(
            products
                .windowed_products
                .iter()
                .map(|product| product.slug())
                .collect::<Vec<_>>(),
            vec!["qpf_1h"]
        );
        assert!(
            products
                .blockers
                .iter()
                .any(|blocker| blocker.product_slug == "sbecape")
        );
    }

    #[test]
    fn hour_range_slug_stays_short_for_full_runs() {
        let hrrr = (0..=48).collect::<Vec<_>>();
        assert_eq!(hour_range_slug(&hrrr), "f000_f048");

        let mut gefs = (0..=240).step_by(3).collect::<Vec<_>>();
        gefs.extend((246..=384).step_by(6));
        assert_eq!(hour_range_slug(&gefs), "f000_f384_n105");

        let mut gfs = (0..=120).collect::<Vec<_>>();
        gfs.extend((123..=384).step_by(3));
        assert_eq!(hour_range_slug(&gfs), "f000_f384_n209");
    }
}
