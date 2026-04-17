//! Planner-backed runtime: takes an [`ExecutionPlan`] and materializes
//! every bundle into fetched bytes plus, where applicable, decoded
//! surface and pressure fields. Heavy/derived/severe/ECAPE/direct kernels
//! consume this `LoadedBundleSet` instead of running their own ad hoc
//! fetch wiring.
//!
//! The loader honors the planner's two-level identity:
//! - Each `BundleFetchKey` is fetched once even when several
//!   `CanonicalBundleId`s decode out of the same physical file (GFS /
//!   ECMWF / RRFS-A serve both surface + pressure from one file).
//! - Each surface or pressure `CanonicalBundleId` records a typed
//!   decode that the kernels can borrow without re-parsing GRIB bytes.

use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, CanonicalBundleId, CycleSpec, LatLonGrid,
    ModelRunRequest, RustwxError, SourceId,
};
use rustwx_io::{FetchRequest, fetch_bytes_with_cache};
use rustwx_models::LatestRun;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::gridded::{
    CachedDecode, FetchedModelFile, PressureFields, SurfaceFields, decode_cache_path,
    load_or_decode_pressure_with_shape, load_or_decode_surface,
    validate_pressure_decode_against_surface,
};
use crate::planner::{BundleFetchKey, ExecutionPlan, PlannedBundle};

/// Outcome of running a fetch+decode pass over an `ExecutionPlan`.
#[derive(Debug)]
pub struct LoadedBundleSet {
    pub plan: ExecutionPlan,
    pub latest: LatestRun,
    pub forecast_hour: u16,
    pub fetched: BTreeMap<BundleFetchKey, FetchedBundleBytes>,
    pub surface_decodes: BTreeMap<CanonicalBundleId, CachedDecode<SurfaceFields>>,
    pub pressure_decodes: BTreeMap<CanonicalBundleId, CachedDecode<PressureFields>>,
    pub timing: LoadedBundleTiming,
}

/// Aggregated timing surfaced into per-lane reports.
///
/// Note on `fetch_ms_total`: this is the **sum of per-worker elapsed
/// time across fetches**, not the wall-clock cost of the fetch phase.
/// When fetches run in parallel (non-NOMADS), wall-clock is roughly
/// `max(per_fetch_ms)` while this field is the sum across workers and
/// will be larger. Callers that want wall-clock fetch cost should
/// measure around `load_execution_plan` directly.
#[derive(Debug, Default, Clone, Copy)]
pub struct LoadedBundleTiming {
    /// Summed worker-elapsed fetch time across all distinct fetch keys.
    /// See the struct-level note: this is not wall-clock time when the
    /// loader fetches in parallel.
    pub fetch_ms_total: u128,
    pub decode_surface_ms_total: u128,
    pub decode_pressure_ms_total: u128,
}

/// Raw fetched bytes for a single physical fetch key, plus the original
/// `FetchRequest`/`CachedFetchResult` so manifest/provenance code can
/// build `PublishedFetchIdentity` records.
#[derive(Debug, Clone)]
pub struct FetchedBundleBytes {
    pub key: BundleFetchKey,
    pub file: FetchedModelFile,
    pub fetch_ms: u128,
}

/// Configuration for the loader.
#[derive(Debug, Clone)]
pub struct BundleLoaderConfig {
    pub cache_root: PathBuf,
    pub use_cache: bool,
}

impl BundleLoaderConfig {
    pub fn new(cache_root: PathBuf, use_cache: bool) -> Self {
        Self {
            cache_root,
            use_cache,
        }
    }
}

impl LoadedBundleSet {
    pub fn fetched_for(&self, bundle: &PlannedBundle) -> Option<&FetchedBundleBytes> {
        self.fetched.get(&bundle.fetch_key())
    }

    /// Convenience for kernels that want a (surface, pressure) pair at
    /// the run's nominal forecast hour. Returns the decoded fields and
    /// the matching planned bundles in one call.
    pub fn surface_pressure_pair(
        &self,
    ) -> Option<(&PlannedBundle, &CachedDecode<SurfaceFields>, &PlannedBundle, &CachedDecode<PressureFields>)>
    {
        let surface = self
            .plan
            .bundle_for(CanonicalBundleDescriptor::SurfaceAnalysis, self.forecast_hour)?;
        let pressure = self
            .plan
            .bundle_for(CanonicalBundleDescriptor::PressureAnalysis, self.forecast_hour)?;
        let surface_decode = self.surface_decodes.get(&surface.id)?;
        let pressure_decode = self.pressure_decodes.get(&pressure.id)?;
        Some((surface, surface_decode, pressure, pressure_decode))
    }

    pub fn surface_decode_for(
        &self,
        bundle: CanonicalBundleDescriptor,
        forecast_hour: u16,
    ) -> Option<&CachedDecode<SurfaceFields>> {
        let planned = self.plan.bundle_for(bundle, forecast_hour)?;
        self.surface_decodes.get(&planned.id)
    }

    pub fn pressure_decode_for(
        &self,
        bundle: CanonicalBundleDescriptor,
        forecast_hour: u16,
    ) -> Option<&CachedDecode<PressureFields>> {
        let planned = self.plan.bundle_for(bundle, forecast_hour)?;
        self.pressure_decodes.get(&planned.id)
    }

    /// Convenience for derived/severe/ECAPE: returns the decoded surface
    /// grid (uses the surface bundle at the run's nominal forecast hour).
    pub fn surface_grid(&self) -> Result<LatLonGrid, RustwxError> {
        let surface_decode = self
            .surface_decode_for(CanonicalBundleDescriptor::SurfaceAnalysis, self.forecast_hour)
            .expect("surface bundle missing from loaded plan");
        surface_decode.value.core_grid()
    }
}

/// Materialize the plan: fetch each unique fetch key once, then decode
/// surface and pressure bundles. Other bundle types (e.g. NativeAnalysis
/// at extra forecast hours used by windowed) are surfaced as raw bytes
/// only — kernels that need them call into `fetched_for` to access the
/// `FetchedModelFile`.
///
/// The fetch phase runs in parallel across distinct fetch keys, except
/// for NOMADS-sourced runs (which serialize to avoid the well-known
/// rate-limiting that the windowed lane has historically guarded
/// against).
pub fn load_execution_plan(
    plan: ExecutionPlan,
    config: &BundleLoaderConfig,
) -> Result<LoadedBundleSet, Box<dyn std::error::Error>> {
    let latest = plan.latest();
    let forecast_hour = plan.forecast_hour;
    let parallel_fetches = !matches!(plan.source, SourceId::Nomads);

    // Phase 1: fetch each unique physical file. Parallel for non-NOMADS
    // sources; the planner already deduped, so each spawn corresponds
    // to one distinct GRIB file.
    let fetch_keys = plan.fetch_keys();
    let cache_root = config.cache_root.clone();
    let use_cache = config.use_cache;
    let fetch_results: Vec<Result<FetchedBundleBytes, Box<dyn std::error::Error + Send + Sync>>> =
        if parallel_fetches && fetch_keys.len() > 1 {
            std::thread::scope(|scope| -> Vec<_> {
                let handles: Vec<_> = fetch_keys
                    .iter()
                    .cloned()
                    .map(|key| {
                        let cache_root = cache_root.clone();
                        let use_cache = use_cache;
                        scope.spawn(move || fetch_one(key, &cache_root, use_cache))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle.join().unwrap_or_else(|_| {
                            Err(Box::<dyn std::error::Error + Send + Sync>::from(
                                "planner fetch worker panicked",
                            ))
                        })
                    })
                    .collect()
            })
        } else {
            fetch_keys
                .iter()
                .cloned()
                .map(|key| fetch_one(key, &cache_root, use_cache))
                .collect()
        };

    let mut fetched: BTreeMap<BundleFetchKey, FetchedBundleBytes> = BTreeMap::new();
    let mut total_fetch_ms = 0u128;
    for entry in fetch_results {
        let bundle = entry.map_err(|err| -> Box<dyn std::error::Error> {
            // Re-box from Send + Sync into the lane's looser Box<dyn Error>.
            Box::<dyn std::error::Error>::from(err.to_string())
        })?;
        total_fetch_ms += bundle.fetch_ms;
        fetched.insert(bundle.key.clone(), bundle);
    }

    // Phase 2: decode surface + pressure bundles.
    let mut surface_decodes: BTreeMap<CanonicalBundleId, CachedDecode<SurfaceFields>> =
        BTreeMap::new();
    let mut pressure_decodes: BTreeMap<CanonicalBundleId, CachedDecode<PressureFields>> =
        BTreeMap::new();
    let mut decode_surface_ms_total = 0u128;
    let mut decode_pressure_ms_total = 0u128;

    for bundle in &plan.bundles {
        let fetched_bytes = fetched
            .get(&bundle.fetch_key())
            .ok_or_else(|| format!("planner missed fetch for bundle {}", bundle.id))?;
        match bundle.id.bundle {
            CanonicalBundleDescriptor::SurfaceAnalysis => {
                let cache_path = decode_cache_path(
                    &config.cache_root,
                    &fetched_bytes.file.request,
                    "surface",
                );
                let start = Instant::now();
                let decoded = load_or_decode_surface(
                    &cache_path,
                    fetched_bytes.file.bytes.as_slice(),
                    config.use_cache,
                )?;
                decode_surface_ms_total += start.elapsed().as_millis();
                surface_decodes.insert(bundle.id.clone(), decoded);
            }
            CanonicalBundleDescriptor::PressureAnalysis => {
                let cache_path = decode_cache_path(
                    &config.cache_root,
                    &fetched_bytes.file.request,
                    "pressure",
                );
                let start = Instant::now();
                let (decoded, shape) = load_or_decode_pressure_with_shape(
                    &cache_path,
                    fetched_bytes.file.bytes.as_slice(),
                    config.use_cache,
                )?;
                decode_pressure_ms_total += start.elapsed().as_millis();
                if let Some(matching_surface) = plan.bundle_for(
                    CanonicalBundleDescriptor::SurfaceAnalysis,
                    bundle.id.forecast_hour,
                ) {
                    if let Some(matching) = surface_decodes.get(&matching_surface.id) {
                        validate_pressure_decode_against_surface(
                            &decoded,
                            shape,
                            matching.value.nx,
                            matching.value.ny,
                        )?;
                    }
                }
                pressure_decodes.insert(bundle.id.clone(), decoded);
            }
            CanonicalBundleDescriptor::NativeAnalysis => {
                // Native bundles surface as raw bytes only; kernels
                // (windowed UH/QPF, native composite-direct decode) walk
                // the GRIB messages on demand.
            }
        }
    }

    Ok(LoadedBundleSet {
        plan,
        latest,
        forecast_hour,
        fetched,
        surface_decodes,
        pressure_decodes,
        timing: LoadedBundleTiming {
            fetch_ms_total: total_fetch_ms,
            decode_surface_ms_total,
            decode_pressure_ms_total,
        },
    })
}

fn build_fetch_request(key: &BundleFetchKey) -> Result<FetchRequest, RustwxError> {
    Ok(FetchRequest {
        request: ModelRunRequest::new(
            key.model,
            key.cycle.clone(),
            key.forecast_hour,
            key.native_product.as_str(),
        )?,
        source_override: Some(key.source),
        variable_patterns: Vec::new(),
    })
}

/// Worker used by `load_execution_plan` to fetch a single bundle's
/// physical bytes. Returns a Send + Sync error so it composes with
/// `std::thread::scope`.
fn fetch_one(
    key: BundleFetchKey,
    cache_root: &Path,
    use_cache: bool,
) -> Result<FetchedBundleBytes, Box<dyn std::error::Error + Send + Sync>> {
    let request = build_fetch_request(&key).map_err(|err| {
        Box::<dyn std::error::Error + Send + Sync>::from(err.to_string())
    })?;
    let start = Instant::now();
    let cached = fetch_bytes_with_cache(&request, cache_root, use_cache)
        .map_err(|err| Box::<dyn std::error::Error + Send + Sync>::from(err.to_string()))?;
    let fetch_ms = start.elapsed().as_millis();
    let bytes = cached.result.bytes.clone();
    Ok(FetchedBundleBytes {
        key,
        file: FetchedModelFile {
            request,
            bytes,
            fetched: cached,
        },
        fetch_ms,
    })
}

/// Helper used by the per-lane reports: build a deduped list of
/// `(planned_family, BundleFetchKey)` pairs that captures every alias
/// that asked for each fetch.
pub fn planned_family_aliases_for(bundle: &PlannedBundle) -> Vec<String> {
    bundle.planned_family_slugs()
}

/// Convenience for callers that want to build a one-product plan with a
/// single requirement set (used by severe / ECAPE / single-direct
/// runners). The latest run is supplied externally because resolving the
/// "latest" sometimes requires network probes that the lane batches
/// already perform.
pub fn build_single_pair_plan(
    latest: &LatestRun,
    forecast_hour: u16,
    surface_override: Option<String>,
    pressure_override: Option<String>,
) -> ExecutionPlan {
    let mut builder = crate::planner::ExecutionPlanBuilder::new(latest, forecast_hour);
    let mut surface = BundleRequirement::new(CanonicalBundleDescriptor::SurfaceAnalysis, forecast_hour);
    if let Some(value) = surface_override {
        surface = surface.with_native_override(value);
    }
    let mut pressure = BundleRequirement::new(CanonicalBundleDescriptor::PressureAnalysis, forecast_hour);
    if let Some(value) = pressure_override {
        pressure = pressure.with_native_override(value);
    }
    builder.require_with_logical_family(
        &surface,
        Some(default_planned_family_slug(latest.model, CanonicalBundleDescriptor::SurfaceAnalysis)),
    );
    builder.require_with_logical_family(
        &pressure,
        Some(default_planned_family_slug(latest.model, CanonicalBundleDescriptor::PressureAnalysis)),
    );
    builder.build()
}

fn default_planned_family_slug(
    model: rustwx_core::ModelId,
    bundle: CanonicalBundleDescriptor,
) -> &'static str {
    use rustwx_core::ModelId;
    match (model, bundle) {
        (ModelId::Hrrr, CanonicalBundleDescriptor::SurfaceAnalysis) => "sfc",
        (ModelId::Hrrr, CanonicalBundleDescriptor::PressureAnalysis) => "prs",
        (ModelId::Hrrr, CanonicalBundleDescriptor::NativeAnalysis) => "nat",
        (ModelId::Gfs, _) => "pgrb2.0p25",
        (ModelId::EcmwfOpenData, _) => "oper",
        (ModelId::RrfsA, _) => "prs-conus",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::CycleSpec;

    fn latest() -> LatestRun {
        LatestRun {
            model: rustwx_core::ModelId::Gfs,
            cycle: CycleSpec::new("20260415", 18).unwrap(),
            source: SourceId::Nomads,
        }
    }

    #[test]
    fn build_single_pair_plan_emits_one_fetch_key_for_global_models() {
        let plan = build_single_pair_plan(&latest(), 12, None, None);
        assert_eq!(plan.bundles.len(), 2);
        assert_eq!(plan.fetch_keys().len(), 1);
        assert_eq!(plan.fetch_keys()[0].native_product, "pgrb2.0p25");
    }

    #[test]
    fn build_single_pair_plan_emits_two_fetch_keys_for_hrrr() {
        let plan = build_single_pair_plan(
            &LatestRun {
                model: rustwx_core::ModelId::Hrrr,
                cycle: CycleSpec::new("20260415", 18).unwrap(),
                source: SourceId::Aws,
            },
            6,
            None,
            None,
        );
        assert_eq!(plan.bundles.len(), 2);
        assert_eq!(plan.fetch_keys().len(), 2);
        let products: Vec<_> = plan.fetch_keys().iter().map(|k| k.native_product.clone()).collect();
        assert!(products.contains(&"sfc".to_string()));
        assert!(products.contains(&"prs".to_string()));
    }
}

// Re-export the path helper so tests in other modules don't need to
// dive into gridded.rs internals.
#[doc(hidden)]
pub fn cache_root_decode_path(cache_root: &Path, fetch: &FetchRequest, name: &str) -> PathBuf {
    decode_cache_path(cache_root, fetch, name)
}

#[doc(hidden)]
#[allow(unused)]
fn _force_use_unused_imports(_: SourceId, _: CycleSpec) {}
