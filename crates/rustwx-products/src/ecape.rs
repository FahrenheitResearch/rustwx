use crate::direct::build_projected_map_with_projection;
use crate::gridded::{
    PressureFields, SharedTiming, SurfaceFields, prepare_heavy_volume, prepare_heavy_volume_timed,
    resolve_thermo_pair_run,
};
use crate::heavy::{
    HeavyComputeTiming, HeavyRenderedArtifact, crop_and_guard_heavy_domain,
    heavy_map_target_aspect_ratio, render_heavy_map_group,
};
use crate::publication::PublishedFetchIdentity;
use crate::runtime::{BundleLoaderConfig, load_execution_plan};
use crate::severe::{
    build_planned_input_fetches, build_severe_execution_plan, build_shared_timing_for_pair,
};
use crate::shared_context::{DomainSpec, WeatherPanelField};
use rustwx_calc::{
    EcapeTripletOptions, EcapeVolumeInputs, ScpEhiInputs, SurfaceInputs, WindGridInputs,
    compute_ecape_triplet_with_failure_mask_from_parts, compute_scp_ehi,
    compute_wind_diagnostics_bundle,
};
use rustwx_core::{ModelId, SourceId};
use rustwx_render::WeatherProduct;
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
    pub allow_large_heavy_domain: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcapeBatchReport {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
    pub domain: DomainSpec,
    pub outputs: Vec<HeavyRenderedArtifact>,
    pub input_fetches: Vec<PublishedFetchIdentity>,
    pub shared_timing: SharedTiming,
    pub heavy_timing: HeavyComputeTiming,
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
    let owned_full_grid = full_surface.core_grid()?;
    let project_start = Instant::now();
    let full_projected = build_projected_map_with_projection(
        &owned_full_grid.lat_deg,
        &owned_full_grid.lon_deg,
        full_surface.projection.as_ref(),
        request.domain.bounds,
        heavy_map_target_aspect_ratio(),
    )?;

    // Same rationale as severe_batch: crop before compute so ECAPE's
    // per-cell parcel ascent runs on ~300×300 midwest cells instead of
    // ~1800×1000 CONUS.
    let heavy_domain = crop_and_guard_heavy_domain(
        full_surface,
        full_pressure,
        &full_projected,
        &request.domain,
        2,
        request.allow_large_heavy_domain,
    )?;
    let (surface, pressure, grid) =
        heavy_domain.bind(full_surface, full_pressure, &owned_full_grid);

    let projected = if heavy_domain.cropped.is_some() {
        build_projected_map_with_projection(
            &grid.lat_deg,
            &grid.lon_deg,
            surface.projection.as_ref(),
            request.domain.bounds,
            heavy_map_target_aspect_ratio(),
        )?
    } else {
        full_projected
    };
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let (prepared, prep_timing) = prepare_heavy_volume_timed(surface, pressure, false)?;
    let ecape_triplet_start = Instant::now();
    let (fields, failure_count) =
        compute_ecape8_panel_fields_with_prepared_volume(surface, pressure, &prepared)?;
    let ecape_triplet_ms = ecape_triplet_start.elapsed().as_millis();
    let compute_ms = compute_start.elapsed().as_millis();

    let model_slug = request.model.as_str().replace('-', "_");
    let subtitle_left = format!(
        "{} {}Z F{:03}  {}",
        request.date_yyyymmdd, loaded.latest.cycle.hour_utc, request.forecast_hour, request.model
    );
    let source_label = format!("source: {}", loaded.latest.source.as_str());
    let (outputs, render_ms) = render_heavy_map_group(
        &request.out_dir,
        &model_slug,
        &request.date_yyyymmdd,
        loaded.latest.cycle.hour_utc,
        request.forecast_hour,
        &request.domain.slug,
        "ecape",
        &grid,
        &projected,
        &fields,
        &subtitle_left,
        |field| match field.artifact_slug() {
            "ecape_scp" | "ecape_ehi" => Some(format!("{source_label} | experimental")),
            _ => Some(source_label.clone()),
        },
    )?;
    let shared_timing = build_shared_timing_for_pair(&loaded, surface_planned, pressure_planned)?;
    let input_fetches = build_planned_input_fetches(&loaded);
    let total_ms = total_start.elapsed().as_millis();
    let heavy_timing = HeavyComputeTiming {
        full_cells: heavy_domain.stats.full_cells,
        cropped_cells: heavy_domain.stats.cropped_cells,
        pressure_levels: heavy_domain.stats.pressure_levels,
        crop_kind: heavy_domain.stats.crop_kind,
        crop_ms: heavy_domain.crop_ms,
        prepare_height_agl_ms: prep_timing.prepare_height_agl_ms,
        broadcast_pressure_ms: prep_timing.broadcast_pressure_ms,
        pressure_3d_bytes: prep_timing.pressure_3d_bytes,
        ecape_triplet_ms,
        severe_fields_ms: 0,
        render_ms,
        total_ms,
    };

    Ok(EcapeBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: loaded.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: loaded.latest.source,
        domain: request.domain.clone(),
        outputs,
        input_fetches,
        shared_timing,
        heavy_timing,
        project_ms,
        compute_ms,
        render_ms,
        total_ms,
        failure_count,
    })
}

pub fn compute_ecape8_panel_fields(
    surface: &SurfaceFields,
    pressure: &PressureFields,
) -> Result<(Vec<WeatherPanelField>, usize), Box<dyn std::error::Error>> {
    let prepared = prepare_heavy_volume(surface, pressure, false)?;
    compute_ecape8_panel_fields_with_prepared_volume(surface, pressure, &prepared)
}

pub fn compute_ecape8_panel_fields_with_prepared_volume(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    prepared: &crate::gridded::PreparedHeavyVolume,
) -> Result<(Vec<WeatherPanelField>, usize), Box<dyn std::error::Error>> {
    let triplet = compute_ecape_triplet_with_failure_mask_from_parts(
        prepared.grid,
        EcapeVolumeInputs {
            pressure_pa: prepared
                .pressure_3d_pa
                .as_deref()
                .unwrap_or(&prepared.pressure_levels_pa),
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
        WeatherPanelField::new(WeatherProduct::Sbecape, "J/kg", triplet.sb.fields.ecape_jkg),
        WeatherPanelField::new(WeatherProduct::Mlecape, "J/kg", triplet.ml.fields.ecape_jkg),
        WeatherPanelField::new(WeatherProduct::Muecape, "J/kg", triplet.mu.fields.ecape_jkg),
        WeatherPanelField::new(WeatherProduct::Sbncape, "J/kg", triplet.sb.fields.ncape_jkg),
        WeatherPanelField::new(WeatherProduct::Sbecin, "J/kg", triplet.sb.fields.cin_jkg),
        WeatherPanelField::new(WeatherProduct::Mlecin, "J/kg", triplet.ml.fields.cin_jkg),
        WeatherPanelField::new(
            WeatherProduct::EcapeScpExperimental,
            "dimensionless",
            experimental.scp,
        ),
        WeatherPanelField::new(
            WeatherProduct::EcapeEhiExperimental,
            "dimensionless",
            experimental.ehi,
        ),
    ];
    Ok((fields, failure_count))
}
