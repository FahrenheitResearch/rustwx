use crate::direct::build_projected_map;
use crate::gridded::{
    PreparedHeavyVolume, PressureFields, SharedTiming, SurfaceFields, prepare_heavy_volume,
    resolve_model_run,
};
use crate::planner::{BundleFetchKey, ExecutionPlan, ExecutionPlanBuilder, PlannedBundle};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
    fetch_identity_from_cached_result_with_aliases,
};
use crate::runtime::{BundleLoaderConfig, FetchedBundleBytes, LoadedBundleSet, load_execution_plan};
use crate::shared_context::{
    DomainSpec, Solar07PanelField, Solar07PanelHeader, Solar07PanelLayout,
    render_two_by_four_solar07_panel,
};
use rustwx_calc::{
    EcapeVolumeInputs, SupportedSevereFields, SurfaceInputs, compute_supported_severe_fields,
};
use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, ModelId, SourceId,
};
use rustwx_models::LatestRun;
use rustwx_render::Solar07Product;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SevereBatchRequest {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_override_utc: Option<u8>,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub out_dir: PathBuf,
    pub cache_root: PathBuf,
    pub use_cache: bool,
    pub surface_product_override: Option<String>,
    pub pressure_product_override: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SevereBatchReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub output_path: PathBuf,
    pub output_identity: ArtifactContentIdentity,
    pub input_fetches: Vec<PublishedFetchIdentity>,
    pub shared_timing: SharedTiming,
    pub project_ms: u128,
    pub compute_ms: u128,
    pub render_ms: u128,
    pub total_ms: u128,
}

pub fn run_severe_batch(
    request: &SevereBatchRequest,
) -> Result<SevereBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let total_start = Instant::now();
    let latest = resolve_model_run(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.source,
    )?;
    let plan = build_severe_execution_plan(
        &latest,
        request.forecast_hour,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
    );
    let loaded = load_execution_plan(
        plan,
        &BundleLoaderConfig {
            cache_root: request.cache_root.clone(),
            use_cache: request.use_cache,
        },
    )?;

    let (surface_planned, surface_decode, pressure_planned, pressure_decode) = loaded
        .surface_pressure_pair()
        .ok_or("severe planner missed surface or pressure bundle")?;
    let grid = loaded.surface_grid()?;

    let layout = Solar07PanelLayout {
        top_padding: 86,
        ..Default::default()
    };
    let project_start = Instant::now();
    let projected = build_projected_map(
        &grid.lat_deg,
        &grid.lon_deg,
        request.domain.bounds,
        layout.target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let fields = compute_severe_panel_fields(&surface_decode.value, &pressure_decode.value)?;
    let compute_ms = compute_start.elapsed().as_millis();

    let model_slug = request.model.as_str().replace('-', "_");
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_severe_proof_panel.png",
        model_slug,
        request.date_yyyymmdd,
        loaded.latest.cycle.hour_utc,
        request.forecast_hour,
        request.domain.slug
    ));
    let render_start = Instant::now();
    let header = Solar07PanelHeader::new(format!("{} Severe Proof Panel", request.model))
        .with_subtitle_line(
            "STP is fixed-layer only: sbCAPE + sbLCL + 0-1 km SRH + 0-6 km bulk shear.",
        )
        .with_subtitle_line(
            "SCP stays a fixed-depth proxy here: muCAPE + 0-3 km SRH + 0-6 km shear. EHI 0-1 km uses sbCAPE + 0-1 km SRH. Effective-layer derivation is not wired yet.",
        );
    render_two_by_four_solar07_panel(
        &output_path,
        &grid,
        &projected,
        &fields,
        &header,
        layout,
    )?;
    let render_ms = render_start.elapsed().as_millis();
    let output_identity = artifact_identity_from_path(&output_path)?;
    let shared_timing = build_shared_timing_for_pair(
        &loaded,
        surface_planned,
        pressure_planned,
    )?;
    let input_fetches = build_planned_input_fetches(&loaded);

    Ok(SevereBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: loaded.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: loaded.latest.source,
        domain: request.domain.clone(),
        output_path,
        output_identity,
        input_fetches,
        shared_timing,
        project_ms,
        compute_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
    })
}

/// Build the typed execution plan for a severe-panel run: surface +
/// pressure analyses at the requested forecast hour, with optional
/// native-product overrides.
pub fn build_severe_execution_plan(
    latest: &LatestRun,
    forecast_hour: u16,
    surface_override: Option<&str>,
    pressure_override: Option<&str>,
) -> ExecutionPlan {
    let mut builder = ExecutionPlanBuilder::new(latest, forecast_hour);
    let mut surface = BundleRequirement::new(CanonicalBundleDescriptor::SurfaceAnalysis, forecast_hour);
    if let Some(value) = surface_override {
        surface = surface.with_native_override(value.to_string());
    }
    let mut pressure = BundleRequirement::new(
        CanonicalBundleDescriptor::PressureAnalysis,
        forecast_hour,
    );
    if let Some(value) = pressure_override {
        pressure = pressure.with_native_override(value.to_string());
    }
    builder.require_with_logical_family(
        &surface,
        Some(default_logical_family(latest.model, CanonicalBundleDescriptor::SurfaceAnalysis)),
    );
    builder.require_with_logical_family(
        &pressure,
        Some(default_logical_family(latest.model, CanonicalBundleDescriptor::PressureAnalysis)),
    );
    builder.build()
}

fn default_logical_family(
    model: ModelId,
    bundle: CanonicalBundleDescriptor,
) -> &'static str {
    match (model, bundle) {
        (ModelId::Hrrr, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc",
        (ModelId::Hrrr, CanonicalBundleDescriptor::PressureAnalysis) => "prs",
        (ModelId::Hrrr, CanonicalBundleDescriptor::NativeAnalysis) => "nat",
        (ModelId::Gfs, _) => "pgrb2.0p25",
        (ModelId::EcmwfOpenData, _) => "oper",
        (ModelId::RrfsA, _) => "prs-conus",
    }
}

/// Reconstruct the legacy `SharedTiming` block from the loader output so
/// existing consumers keep working unchanged.
pub(crate) fn build_shared_timing_for_pair(
    loaded: &LoadedBundleSet,
    surface_planned: &PlannedBundle,
    pressure_planned: &PlannedBundle,
) -> Result<SharedTiming, Box<dyn std::error::Error>> {
    let surface_fetched = loaded
        .fetched_for(surface_planned)
        .ok_or("loader missing surface fetch for severe report")?;
    let pressure_fetched = loaded
        .fetched_for(pressure_planned)
        .ok_or("loader missing pressure fetch for severe report")?;
    let surface_decode = loaded
        .surface_decode_for(CanonicalBundleDescriptor::SurfaceAnalysis, loaded.forecast_hour)
        .ok_or("loader missing surface decode for severe report")?;
    let pressure_decode = loaded
        .pressure_decode_for(
            CanonicalBundleDescriptor::PressureAnalysis,
            loaded.forecast_hour,
        )
        .ok_or("loader missing pressure decode for severe report")?;

    Ok(SharedTiming {
        fetch_surface_ms: surface_fetched.fetch_ms,
        fetch_pressure_ms: pressure_fetched.fetch_ms,
        decode_surface_ms: 0,
        decode_pressure_ms: 0,
        fetch_surface_cache_hit: surface_fetched.file.fetched.cache_hit,
        fetch_pressure_cache_hit: pressure_fetched.file.fetched.cache_hit,
        decode_surface_cache_hit: surface_decode.cache_hit,
        decode_pressure_cache_hit: pressure_decode.cache_hit,
        surface_fetch: surface_fetched
            .file
            .runtime_info(&surface_planned.resolved),
        pressure_fetch: pressure_fetched
            .file
            .runtime_info(&pressure_planned.resolved),
    })
}

/// Build a deduplicated `PublishedFetchIdentity` list from the planner's
/// loaded bundles, preserving any logical family aliases the planner
/// recorded (e.g., HRRR `nat` planned-family that merged onto `sfc`).
///
/// Dedupe is keyed by `BundleFetchKey`, not `CanonicalBundleId`: on
/// global models (GFS / ECMWF / RRFS-A) a single physical GRIB file
/// serves both the surface and pressure canonical bundles, so grouping
/// by canonical id would publish one file twice. Aliases from every
/// canonical bundle sharing the fetch key are unioned onto the single
/// identity that represents the physical file.
pub(crate) fn build_planned_input_fetches(loaded: &LoadedBundleSet) -> Vec<PublishedFetchIdentity> {
    struct FetchGroup<'a> {
        bundle: &'a PlannedBundle,
        fetched: &'a FetchedBundleBytes,
        aliases: std::collections::BTreeSet<String>,
    }

    let mut by_fetch: std::collections::BTreeMap<BundleFetchKey, FetchGroup<'_>> =
        std::collections::BTreeMap::new();
    for bundle in &loaded.plan.bundles {
        let Some(fetched) = loaded.fetched_for(bundle) else {
            continue;
        };
        let key = bundle.fetch_key();
        let canonical = bundle.resolved.native_product.as_str();
        let entry = by_fetch.entry(key).or_insert_with(|| FetchGroup {
            bundle,
            fetched,
            aliases: std::collections::BTreeSet::new(),
        });
        for slug in bundle.planned_family_slugs() {
            // Drop the canonical planned product so it doesn't appear
            // duplicated in the manifest alongside the `planned_family`.
            if slug != canonical {
                entry.aliases.insert(slug);
            }
        }
    }
    by_fetch
        .into_values()
        .map(|group| {
            fetch_identity_from_cached_result_with_aliases(
                group.bundle.resolved.native_product.as_str(),
                group.aliases.into_iter().collect(),
                &group.fetched.file.request,
                &group.fetched.file.fetched,
            )
        })
        .collect()
}

pub fn severe_panel_fields_from_supported(fields: SupportedSevereFields) -> Vec<Solar07PanelField> {
    vec![
        Solar07PanelField::new(Solar07Product::Sbcape, "J/kg", fields.sbcape_jkg),
        Solar07PanelField::new(Solar07Product::Mlcin, "J/kg", fields.mlcin_jkg),
        Solar07PanelField::new(Solar07Product::Mucape, "J/kg", fields.mucape_jkg),
        Solar07PanelField::new(Solar07Product::Srh01km, "m^2/s^2", fields.srh_01km_m2s2),
        Solar07PanelField::new(Solar07Product::Srh03km, "m^2/s^2", fields.srh_03km_m2s2),
        Solar07PanelField::new(Solar07Product::StpFixed, "dimensionless", fields.stp_fixed),
        Solar07PanelField::new(
            Solar07Product::Scp,
            "dimensionless",
            fields.scp_mu_03km_06km_proxy,
        )
        .with_title_override("SCP (MU / 0-3 KM / 0-6 KM PROXY)"),
        Solar07PanelField::new(
            Solar07Product::Ehi,
            "dimensionless",
            fields.ehi_sb_01km_proxy,
        )
        .with_title_override("EHI 0-1 KM"),
    ]
}

pub fn compute_severe_panel_fields(
    surface: &SurfaceFields,
    pressure: &PressureFields,
) -> Result<Vec<Solar07PanelField>, Box<dyn std::error::Error>> {
    let prepared = prepare_heavy_volume(surface, pressure, true)?;
    compute_severe_panel_fields_with_prepared_volume(surface, pressure, &prepared)
}

#[cfg(test)]
mod planned_input_fetches_tests {
    use super::*;
    use crate::gridded::FetchedModelFile;
    use crate::planner::ExecutionPlanBuilder;
    use crate::runtime::{LoadedBundleSet, LoadedBundleTiming};
    use rustwx_core::{CycleSpec, ModelRunRequest};
    use rustwx_io::{CachedFetchResult, FetchRequest, FetchResult};
    use rustwx_models::LatestRun;
    use std::path::PathBuf;

    fn synthetic_fetched(key: &BundleFetchKey) -> FetchedBundleBytes {
        let request = FetchRequest {
            request: ModelRunRequest::new(
                key.model,
                key.cycle.clone(),
                key.forecast_hour,
                key.native_product.as_str(),
            )
            .unwrap(),
            source_override: Some(key.source),
            variable_patterns: Vec::new(),
        };
        let bytes = b"synthetic-grib".to_vec();
        let fetched = CachedFetchResult {
            result: FetchResult {
                source: key.source,
                url: format!("https://example.test/{}", key.native_product),
                bytes: bytes.clone(),
            },
            cache_hit: true,
            bytes_path: PathBuf::from("synthetic"),
            metadata_path: PathBuf::from("synthetic.json"),
        };
        FetchedBundleBytes {
            key: key.clone(),
            file: FetchedModelFile {
                request,
                fetched,
                bytes,
            },
            fetch_ms: 0,
        }
    }

    fn build_loaded(latest: LatestRun, forecast_hour: u16) -> LoadedBundleSet {
        let mut builder = ExecutionPlanBuilder::new(&latest, forecast_hour);
        builder.require(&BundleRequirement::new(
            CanonicalBundleDescriptor::SurfaceAnalysis,
            forecast_hour,
        ));
        builder.require(&BundleRequirement::new(
            CanonicalBundleDescriptor::PressureAnalysis,
            forecast_hour,
        ));
        let plan = builder.build();
        let mut fetched = std::collections::BTreeMap::new();
        for key in plan.fetch_keys() {
            fetched.insert(key.clone(), synthetic_fetched(&key));
        }
        LoadedBundleSet {
            plan,
            latest,
            forecast_hour,
            fetched,
            surface_decodes: std::collections::BTreeMap::new(),
            pressure_decodes: std::collections::BTreeMap::new(),
            timing: LoadedBundleTiming::default(),
        }
    }

    #[test]
    fn gfs_shared_fetch_publishes_one_identity_per_physical_file() {
        // Regression: prior code keyed dedupe by CanonicalBundleId, so GFS
        // surface + pressure (two canonicals, one pgrb2.0p25 fetch) would
        // publish the same physical file twice in manifests.
        let loaded = build_loaded(
            LatestRun {
                model: ModelId::Gfs,
                cycle: CycleSpec::new("20260415", 18).unwrap(),
                source: SourceId::Nomads,
            },
            12,
        );
        assert_eq!(loaded.plan.bundles.len(), 2);
        assert_eq!(loaded.plan.fetch_keys().len(), 1);

        let identities = build_planned_input_fetches(&loaded);
        assert_eq!(identities.len(), 1);
        assert_eq!(identities[0].planned_family, "pgrb2.0p25");
    }

    #[test]
    fn hrrr_distinct_fetches_publish_one_identity_each() {
        // HRRR genuinely has two physical files (sfc + prs); we must
        // still emit one identity per fetch.
        let loaded = build_loaded(
            LatestRun {
                model: ModelId::Hrrr,
                cycle: CycleSpec::new("20260415", 18).unwrap(),
                source: SourceId::Aws,
            },
            6,
        );
        assert_eq!(loaded.plan.fetch_keys().len(), 2);

        let identities = build_planned_input_fetches(&loaded);
        assert_eq!(identities.len(), 2);
        let families: std::collections::BTreeSet<_> = identities
            .iter()
            .map(|id| id.planned_family.clone())
            .collect();
        assert!(families.contains("sfc"));
        assert!(families.contains("prs"));
    }
}

pub fn compute_severe_panel_fields_with_prepared_volume(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    prepared: &PreparedHeavyVolume,
) -> Result<Vec<Solar07PanelField>, Box<dyn std::error::Error>> {
    let pressure_3d_pa = prepared
        .pressure_3d_pa
        .as_deref()
        .ok_or("prepared severe volume was missing broadcast pressure data")?;
    let fields = compute_supported_severe_fields(
        prepared.grid,
        EcapeVolumeInputs {
            pressure_pa: pressure_3d_pa,
            temperature_c: &pressure.temperature_c_3d,
            qvapor_kgkg: &pressure.qvapor_kgkg_3d,
            height_agl_m: &prepared.height_agl_3d,
            u_ms: &pressure.u_ms_3d,
            v_ms: &pressure.v_ms_3d,
            nz: prepared.shape.nz,
        },
        SurfaceInputs {
            psfc_pa: &surface.psfc_pa,
            t2_k: &surface.t2_k,
            q2_kgkg: &surface.q2_kgkg,
            u10_ms: &surface.u10_ms,
            v10_ms: &surface.v10_ms,
        },
    )?;
    Ok(severe_panel_fields_from_supported(fields))
}
