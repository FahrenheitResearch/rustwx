use crate::direct::build_projected_map;
use crate::gridded::{
    PressureFields, SharedTiming, SurfaceFields, prepare_heavy_volume, resolve_thermo_pair_run,
};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
};
use crate::runtime::{BundleLoaderConfig, load_execution_plan};
use crate::severe::{
    build_planned_input_fetches, build_severe_execution_plan, build_shared_timing_for_pair,
};
use crate::shared_context::{
    DomainSpec, Solar07PanelField, Solar07PanelHeader, Solar07PanelLayout,
    render_two_by_four_solar07_panel,
};
use rustwx_calc::{
    EcapeTripletOptions, EcapeVolumeInputs, ScpEhiInputs, SurfaceInputs, WindGridInputs,
    compute_ecape_triplet_with_failure_mask_from_parts, compute_scp_ehi,
    compute_wind_diagnostics_bundle,
};
use rustwx_core::{ModelId, SourceId};
use rustwx_render::Solar07Product;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcapeBatchRequest {
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
pub struct EcapeBatchReport {
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
    pub failure_count: usize,
}

pub fn run_ecape_batch(
    request: &EcapeBatchRequest,
) -> Result<EcapeBatchReport, Box<dyn std::error::Error>> {
    fs::create_dir_all(&request.out_dir)?;
    if request.use_cache {
        fs::create_dir_all(&request.cache_root)?;
    }

    let total_start = Instant::now();
    let latest = resolve_thermo_pair_run(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
    )?;
    // ECAPE consumes the same surface+pressure pair as the severe panel,
    // so we reuse the same execution-plan builder; the planner dedupes if
    // both products run in the same pass.
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
        .require_surface_pressure_pair()
        .map_err(|err| format!("ECAPE surface/pressure pair unavailable: {err}"))?;
    let full_surface = &surface_decode.value;
    let full_pressure = &pressure_decode.value;

    // Same rationale as severe_batch: crop before compute so ECAPE's
    // per-cell parcel ascent runs on ~300×300 midwest cells instead of
    // ~1800×1000 CONUS.
    let cropped =
        crate::gridded::crop_heavy_domain(full_surface, full_pressure, request.domain.bounds)?;
    let owned_full_grid;
    let (surface, pressure, grid) = match cropped.as_ref() {
        Some(cropped) => (&cropped.surface, &cropped.pressure, cropped.grid.clone()),
        None => {
            owned_full_grid = full_surface.core_grid()?;
            (full_surface, full_pressure, owned_full_grid)
        }
    };

    let project_start = Instant::now();
    let projected = build_projected_map(
        &grid.lat_deg,
        &grid.lon_deg,
        request.domain.bounds,
        Solar07PanelLayout::default().target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let (fields, failure_count) = compute_ecape8_panel_fields(surface, pressure)?;
    let compute_ms = compute_start.elapsed().as_millis();

    let model_slug = request.model.as_str().replace('-', "_");
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_ecape8_panel.png",
        model_slug,
        request.date_yyyymmdd,
        loaded.latest.cycle.hour_utc,
        request.forecast_hour,
        request.domain.slug
    ));
    let render_start = Instant::now();
    render_two_by_four_solar07_panel(
        &output_path,
        &grid,
        &projected,
        &fields,
        &Solar07PanelHeader::new(format!("{} ECAPE 8-Panel", request.model)),
        Solar07PanelLayout::default(),
    )?;
    let render_ms = render_start.elapsed().as_millis();
    let output_identity = artifact_identity_from_path(&output_path)?;
    let shared_timing = build_shared_timing_for_pair(&loaded, surface_planned, pressure_planned)?;
    let input_fetches = build_planned_input_fetches(&loaded);

    Ok(EcapeBatchReport {
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
        failure_count,
    })
}

pub fn compute_ecape8_panel_fields(
    surface: &SurfaceFields,
    pressure: &PressureFields,
) -> Result<(Vec<Solar07PanelField>, usize), Box<dyn std::error::Error>> {
    let prepared = prepare_heavy_volume(surface, pressure, false)?;
    compute_ecape8_panel_fields_with_prepared_volume(surface, pressure, &prepared)
}

pub fn compute_ecape8_panel_fields_with_prepared_volume(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    prepared: &crate::gridded::PreparedHeavyVolume,
) -> Result<(Vec<Solar07PanelField>, usize), Box<dyn std::error::Error>> {
    let triplet = compute_ecape_triplet_with_failure_mask_from_parts(
        prepared.grid,
        EcapeVolumeInputs {
            pressure_pa: &prepared.pressure_levels_pa,
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
        EcapeTripletOptions::new("right_moving"),
    )?;
    let wind = WindGridInputs {
        shape: prepared.shape,
        u_3d_ms: &pressure.u_ms_3d,
        v_3d_ms: &pressure.v_ms_3d,
        height_agl_3d_m: &prepared.height_agl_3d,
    };
    let wind_diagnostics = compute_wind_diagnostics_bundle(wind)?;
    let experimental = compute_scp_ehi(ScpEhiInputs {
        grid: prepared.grid,
        scp_cape_jkg: &triplet.mu.fields.ecape_jkg,
        scp_srh_m2s2: &wind_diagnostics.srh_03km_m2s2,
        scp_bulk_wind_difference_ms: &wind_diagnostics.shear_06km_ms,
        ehi_cape_jkg: &triplet.sb.fields.ecape_jkg,
        ehi_srh_m2s2: &wind_diagnostics.srh_01km_m2s2,
    })?;
    let failure_count = triplet.total_failure_count();

    let fields = vec![
        Solar07PanelField::new(Solar07Product::Sbecape, "J/kg", triplet.sb.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Mlecape, "J/kg", triplet.ml.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Muecape, "J/kg", triplet.mu.fields.ecape_jkg),
        Solar07PanelField::new(Solar07Product::Sbncape, "J/kg", triplet.sb.fields.ncape_jkg),
        Solar07PanelField::new(Solar07Product::Sbecin, "J/kg", triplet.sb.fields.cin_jkg),
        Solar07PanelField::new(Solar07Product::Mlecin, "J/kg", triplet.ml.fields.cin_jkg),
        Solar07PanelField::new(
            Solar07Product::EcapeScpExperimental,
            "dimensionless",
            experimental.scp,
        ),
        Solar07PanelField::new(
            Solar07Product::EcapeEhiExperimental,
            "dimensionless",
            experimental.ehi,
        ),
    ];
    Ok((fields, failure_count))
}
