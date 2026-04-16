use crate::direct::build_projected_map;
use crate::gridded::{
    PreparedHeavyVolume, PressureFields, SharedTiming, SurfaceFields,
    load_model_timestep_from_parts, prepare_heavy_volume,
};
use crate::publication::{
    ArtifactContentIdentity, PublishedFetchIdentity, artifact_identity_from_path,
    fetch_identity_from_cached_result,
};
use crate::shared_context::{
    DomainSpec, Solar07PanelField, Solar07PanelHeader, Solar07PanelLayout,
    render_two_by_four_solar07_panel,
};
use rustwx_calc::{
    EcapeVolumeInputs, SupportedSevereFields, SurfaceInputs, compute_supported_severe_fields,
};
use rustwx_core::{ModelId, SourceId};
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
    let timestep = load_model_timestep_from_parts(
        request.model,
        &request.date_yyyymmdd,
        request.cycle_override_utc,
        request.forecast_hour,
        request.source,
        request.surface_product_override.as_deref(),
        request.pressure_product_override.as_deref(),
        &request.cache_root,
        request.use_cache,
    )?;

    let layout = Solar07PanelLayout {
        top_padding: 86,
        ..Default::default()
    };
    let project_start = Instant::now();
    let projected = build_projected_map(
        &timestep.grid.lat_deg,
        &timestep.grid.lon_deg,
        request.domain.bounds,
        layout.target_aspect_ratio(),
    )?;
    let project_ms = project_start.elapsed().as_millis();

    let compute_start = Instant::now();
    let fields = compute_severe_panel_fields(
        &timestep.surface_decode.value,
        &timestep.pressure_decode.value,
    )?;
    let compute_ms = compute_start.elapsed().as_millis();

    let model_slug = request.model.as_str().replace('-', "_");
    let output_path = request.out_dir.join(format!(
        "rustwx_{}_{}_{}z_f{:03}_{}_severe_proof_panel.png",
        model_slug,
        request.date_yyyymmdd,
        timestep.latest.cycle.hour_utc,
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
        &timestep.grid,
        &projected,
        &fields,
        &header,
        layout,
    )?;
    let render_ms = render_start.elapsed().as_millis();
    let output_identity = artifact_identity_from_path(&output_path)?;
    let input_fetches = vec![
        fetch_identity_from_cached_result(
            timestep
                .shared_timing
                .surface_fetch
                .planned_product
                .as_str(),
            &timestep.surface_file.request,
            &timestep.surface_file.fetched,
        ),
        fetch_identity_from_cached_result(
            timestep
                .shared_timing
                .pressure_fetch
                .planned_product
                .as_str(),
            &timestep.pressure_file.request,
            &timestep.pressure_file.fetched,
        ),
    ];

    Ok(SevereBatchReport {
        model: request.model,
        date_yyyymmdd: request.date_yyyymmdd.clone(),
        cycle_utc: timestep.latest.cycle.hour_utc,
        forecast_hour: request.forecast_hour,
        source: timestep.latest.source,
        domain: request.domain.clone(),
        output_path,
        output_identity,
        input_fetches,
        shared_timing: timestep.shared_timing,
        project_ms,
        compute_ms,
        render_ms,
        total_ms: total_start.elapsed().as_millis(),
    })
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
