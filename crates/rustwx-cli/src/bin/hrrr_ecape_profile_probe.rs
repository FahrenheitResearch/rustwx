use clap::Parser;
use ecape_rs::{CapeType, ParcelOptions, StormMotionType, calc_ecape_ncape, calc_ecape_parcel};
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::gridded::{
    load_model_timestep_from_parts_cropped, prepare_heavy_volume_timed,
};
use serde::Serialize;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-ecape-profile-probe",
    about = "Extract one HRRR column and compute full-parcel ECAPE layer diagnostics"
)]
struct Args {
    #[arg(long, default_value = "hrrr")]
    model: ModelId,
    #[arg(long)]
    date: String,
    #[arg(long)]
    cycle: u8,
    #[arg(long, default_value_t = 1)]
    forecast_hour: u16,
    #[arg(long, default_value = "aws")]
    source: SourceId,
    #[arg(long)]
    lat: f64,
    #[arg(long)]
    lon: f64,
    #[arg(long, default_value_t = 1.0)]
    crop_radius_deg: f64,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    include_input_column: bool,
}

#[derive(Debug, Serialize)]
struct ProbeReport {
    request: ProbeRequest,
    nearest_grid_point: NearestPoint,
    timing: ProbeTiming,
    input_profile: InputProfileSummary,
    input_column: Option<InputColumn>,
    kinematics: KinematicDiagnostics,
    parcels: Vec<ParcelDiagnostics>,
}

#[derive(Debug, Serialize)]
struct ProbeRequest {
    model: ModelId,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    requested_lat: f64,
    requested_lon: f64,
    crop_bounds: (f64, f64, f64, f64),
}

#[derive(Debug, Clone, Serialize)]
struct NearestPoint {
    index: usize,
    i: usize,
    j: usize,
    lat: f64,
    lon: f64,
    distance_deg: f64,
}

#[derive(Debug, Default, Serialize)]
struct ProbeTiming {
    load_ms: u128,
    prepare_ms: u128,
    parcel_ms: u128,
    total_ms: u128,
}

#[derive(Debug, Serialize)]
struct InputProfileSummary {
    levels: usize,
    pressure_top_pa: f64,
    pressure_bottom_pa: f64,
    height_top_m: f64,
    t2_k: f64,
    q2_kgkg: f64,
    psfc_pa: f64,
}

#[derive(Debug, Serialize)]
struct InputColumn {
    pressure_pa: Vec<f64>,
    height_m: Vec<f64>,
    temperature_k: Vec<f64>,
    dewpoint_k: Vec<f64>,
    u_wind_ms: Vec<f64>,
    v_wind_ms: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct KinematicDiagnostics {
    bulk_shear_0_1km_ms: Option<f64>,
    bulk_shear_0_3km_ms: Option<f64>,
    bulk_shear_0_6km_ms: Option<f64>,
    bulk_shear_0_8km_ms: Option<f64>,
    mean_wind_0_6km_u_ms: Option<f64>,
    mean_wind_0_6km_v_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ParcelDiagnostics {
    parcel_type: String,
    analytic_reference: AnalyticReferenceSummary,
    entraining: ParcelRunSummary,
    undiluted: ParcelRunSummary,
    ratio_ecape_to_undiluted_cape: Option<f64>,
    ratio_entraining_cape_to_undiluted_cape: Option<f64>,
    entraining_cape_minus_analytic_ecape_jkg: f64,
    post_entrainment_ecape_minus_analytic_ecape_jkg: f64,
    storm_relative_layers: Vec<StormRelativeLayerDiagnostics>,
    layers: Vec<LayerDiagnostics>,
    max_buoyancy_difference: ExtremeDifference,
}

#[derive(Debug, Serialize)]
struct AnalyticReferenceSummary {
    ecape_jkg: f64,
    ncape_jkg: f64,
    cape_jkg: f64,
    lfc_m: Option<f64>,
    el_m: Option<f64>,
    storm_motion_u_ms: f64,
    storm_motion_v_ms: f64,
    storm_relative_wind_ms: f64,
    psi: f64,
}

#[derive(Debug, Serialize)]
struct ParcelRunSummary {
    ecape_jkg: f64,
    ncape_jkg: f64,
    cape_jkg: f64,
    cin_jkg: f64,
    lfc_m: Option<f64>,
    el_m: Option<f64>,
    storm_motion_u_ms: f64,
    storm_motion_v_ms: f64,
    profile_levels: usize,
    max_buoyancy_ms2: f64,
    max_buoyancy_height_m: f64,
}

#[derive(Debug, Serialize)]
struct LayerDiagnostics {
    layer_m: String,
    entraining_positive_jkg: f64,
    undiluted_positive_jkg: f64,
    positive_buoyancy_delta_jkg: f64,
    efficiency_ratio: Option<f64>,
    mean_buoyancy_delta_ms2: f64,
}

#[derive(Debug, Serialize)]
struct StormRelativeLayerDiagnostics {
    layer_m: String,
    storm_motion_u_ms: f64,
    storm_motion_v_ms: f64,
    srh_m2s2: Option<f64>,
    experimental_ecape_ehi: Option<f64>,
    undiluted_cape_ehi: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ExtremeDifference {
    height_m: f64,
    entraining_buoyancy_ms2: f64,
    undiluted_buoyancy_ms2: f64,
    delta_ms2: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let total_start = Instant::now();
    let cache_root = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_proof_cache_dir(PathBuf::from("proof").as_path()));
    if !args.no_cache {
        ensure_dir(&cache_root)?;
    }
    let bounds = (
        args.lon - args.crop_radius_deg,
        args.lon + args.crop_radius_deg,
        args.lat - args.crop_radius_deg,
        args.lat + args.crop_radius_deg,
    );

    let load_start = Instant::now();
    let loaded = load_model_timestep_from_parts_cropped(
        args.model,
        &args.date,
        Some(args.cycle),
        args.forecast_hour,
        args.source,
        None,
        None,
        &cache_root,
        !args.no_cache,
        bounds,
    )?;
    let load_ms = load_start.elapsed().as_millis();

    let surface = &loaded.surface_decode.value;
    let pressure = &loaded.pressure_decode.value;
    let nearest = nearest_point(surface, args.lat, args.lon)?;

    let prepare_start = Instant::now();
    let (prepared, _prep_timing) = prepare_heavy_volume_timed(surface, pressure, false)?;
    let prepare_ms = prepare_start.elapsed().as_millis();

    let nxy = surface.nx * surface.ny;
    let nz = pressure.pressure_levels_hpa.len();
    let pressure_levels_pa = pressure
        .pressure_levels_hpa
        .iter()
        .map(|level| level * 100.0)
        .collect::<Vec<_>>();
    let model_bottom_up = if nz >= 2 {
        prepared.height_agl_3d[nearest.index] < prepared.height_agl_3d[nxy + nearest.index]
    } else {
        true
    };
    let (pressure_pa, height_m, temp_k, dewpoint_k, u_ms, v_ms) =
        build_surface_augmented_column_levels(
            &pressure_levels_pa,
            &pressure.temperature_c_3d,
            &pressure.qvapor_kgkg_3d,
            &prepared.height_agl_3d,
            &pressure.u_ms_3d,
            &pressure.v_ms_3d,
            surface.psfc_pa[nearest.index],
            surface.t2_k[nearest.index],
            surface.q2_kgkg[nearest.index],
            surface.u10_ms[nearest.index],
            surface.v10_ms[nearest.index],
            nz,
            nxy,
            nearest.index,
            model_bottom_up,
        );

    let parcel_start = Instant::now();
    let parcels = ["surface_based", "mixed_layer", "most_unstable"]
        .into_iter()
        .map(|parcel_type| {
            parcel_diagnostics(
                parcel_type,
                &height_m,
                &pressure_pa,
                &temp_k,
                &dewpoint_k,
                &u_ms,
                &v_ms,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let parcel_ms = parcel_start.elapsed().as_millis();

    let nearest_index = nearest.index;
    let report = ProbeReport {
        request: ProbeRequest {
            model: args.model,
            date_yyyymmdd: args.date.clone(),
            cycle_utc: args.cycle,
            forecast_hour: args.forecast_hour,
            source: args.source,
            requested_lat: args.lat,
            requested_lon: args.lon,
            crop_bounds: bounds,
        },
        nearest_grid_point: nearest,
        timing: ProbeTiming {
            load_ms,
            prepare_ms,
            parcel_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
        input_profile: InputProfileSummary {
            levels: pressure_pa.len(),
            pressure_top_pa: *pressure_pa.last().unwrap_or(&f64::NAN),
            pressure_bottom_pa: *pressure_pa.first().unwrap_or(&f64::NAN),
            height_top_m: *height_m.last().unwrap_or(&f64::NAN),
            t2_k: surface.t2_k[nearest_index],
            q2_kgkg: surface.q2_kgkg[nearest_index],
            psfc_pa: surface.psfc_pa[nearest_index],
        },
        input_column: args.include_input_column.then(|| InputColumn {
            pressure_pa: pressure_pa.clone(),
            height_m: height_m.clone(),
            temperature_k: temp_k.clone(),
            dewpoint_k: dewpoint_k.clone(),
            u_wind_ms: u_ms.clone(),
            v_wind_ms: v_ms.clone(),
        }),
        kinematics: kinematic_diagnostics(&height_m, &u_ms, &v_ms),
        parcels,
    };

    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn nearest_point(
    surface: &rustwx_products::gridded::SurfaceFields,
    lat: f64,
    lon: f64,
) -> Result<NearestPoint, Box<dyn std::error::Error>> {
    let mut best = None::<NearestPoint>;
    for idx in 0..surface.lat.len() {
        let dlat = surface.lat[idx] - lat;
        let dlon = surface.lon[idx] - lon;
        let dist2 = dlat * dlat + dlon * dlon;
        if best
            .as_ref()
            .map(|point| dist2 < point.distance_deg * point.distance_deg)
            .unwrap_or(true)
        {
            best = Some(NearestPoint {
                index: idx,
                i: idx % surface.nx,
                j: idx / surface.nx,
                lat: surface.lat[idx],
                lon: surface.lon[idx],
                distance_deg: dist2.sqrt(),
            });
        }
    }
    best.ok_or_else(|| "surface grid is empty".into())
}

fn parcel_diagnostics(
    parcel_type: &str,
    height_m: &[f64],
    pressure_pa: &[f64],
    temp_k: &[f64],
    dewpoint_k: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
) -> Result<ParcelDiagnostics, Box<dyn std::error::Error>> {
    let cape_type = CapeType::parse_normalized(parcel_type)?;
    let base_options = ParcelOptions {
        cape_type,
        storm_motion_type: StormMotionType::RightMoving,
        pseudoadiabatic: Some(true),
        ..ParcelOptions::default()
    };
    let qv_kgkg = pressure_pa
        .iter()
        .zip(dewpoint_k.iter())
        .map(|(&p, &td)| specific_humidity_from_dewpoint_k(p, td))
        .collect::<Vec<_>>();
    let analytic_reference =
        calc_ecape_ncape(height_m, pressure_pa, temp_k, &qv_kgkg, u_ms, v_ms, &base_options)?;
    let entraining = calc_ecape_parcel(
        height_m,
        pressure_pa,
        temp_k,
        dewpoint_k,
        u_ms,
        v_ms,
        &base_options,
    )?;
    let undiluted_options = ParcelOptions {
        entrainment_rate: Some(0.0),
        ..base_options
    };
    let undiluted = calc_ecape_parcel(
        height_m,
        pressure_pa,
        temp_k,
        dewpoint_k,
        u_ms,
        v_ms,
        &undiluted_options,
    )?;
    let layers = [
        (0.0, 1000.0),
        (1000.0, 3000.0),
        (3000.0, 6000.0),
        (6000.0, 10000.0),
    ]
    .into_iter()
    .map(|(bottom, top)| layer_diagnostics(bottom, top, &entraining, &undiluted))
    .collect();
    let storm_relative_layers = [(0.0, 1000.0), (0.0, 3000.0)]
        .into_iter()
        .map(|(bottom, top)| {
            storm_relative_layer_diagnostics(
                bottom,
                top,
                height_m,
                u_ms,
                v_ms,
                &entraining,
                &undiluted,
            )
        })
        .collect();

    Ok(ParcelDiagnostics {
        parcel_type: parcel_type.to_string(),
        ratio_ecape_to_undiluted_cape: positive_ratio(entraining.ecape_jkg, undiluted.cape_jkg),
        ratio_entraining_cape_to_undiluted_cape: positive_ratio(
            entraining.cape_jkg,
            undiluted.cape_jkg,
        ),
        entraining_cape_minus_analytic_ecape_jkg: entraining.cape_jkg
            - analytic_reference.ecape_jkg,
        post_entrainment_ecape_minus_analytic_ecape_jkg: entraining.ecape_jkg
            - analytic_reference.ecape_jkg,
        analytic_reference: summarize_analytic_reference(&analytic_reference),
        entraining: summarize_run(&entraining),
        undiluted: summarize_run(&undiluted),
        storm_relative_layers,
        layers,
        max_buoyancy_difference: max_difference(&entraining, &undiluted),
    })
}

fn specific_humidity_from_dewpoint_k(pressure_pa: f64, dewpoint_k: f64) -> f64 {
    let dewpoint_c = dewpoint_k - 273.15;
    let vapor_pressure_pa = 611.2 * ((17.67 * dewpoint_c) / (dewpoint_c + 243.5)).exp();
    0.62197 * vapor_pressure_pa / (pressure_pa - 0.37803 * vapor_pressure_pa)
}

fn summarize_analytic_reference(result: &ecape_rs::EcapeNcape) -> AnalyticReferenceSummary {
    AnalyticReferenceSummary {
        ecape_jkg: result.ecape_jkg,
        ncape_jkg: result.ncape_jkg,
        cape_jkg: result.cape_jkg,
        lfc_m: result.lfc_m,
        el_m: result.el_m,
        storm_motion_u_ms: result.storm_motion_u_ms,
        storm_motion_v_ms: result.storm_motion_v_ms,
        storm_relative_wind_ms: result.storm_relative_wind_ms,
        psi: result.psi,
    }
}

fn kinematic_diagnostics(height_m: &[f64], u_ms: &[f64], v_ms: &[f64]) -> KinematicDiagnostics {
    let mean_0_6 = mean_wind_layer(height_m, u_ms, v_ms, 0.0, 6000.0);
    KinematicDiagnostics {
        bulk_shear_0_1km_ms: bulk_shear_layer(height_m, u_ms, v_ms, 0.0, 1000.0),
        bulk_shear_0_3km_ms: bulk_shear_layer(height_m, u_ms, v_ms, 0.0, 3000.0),
        bulk_shear_0_6km_ms: bulk_shear_layer(height_m, u_ms, v_ms, 0.0, 6000.0),
        bulk_shear_0_8km_ms: bulk_shear_layer(height_m, u_ms, v_ms, 0.0, 8000.0),
        mean_wind_0_6km_u_ms: mean_0_6.map(|(u, _)| u),
        mean_wind_0_6km_v_ms: mean_0_6.map(|(_, v)| v),
    }
}

fn storm_relative_layer_diagnostics(
    bottom: f64,
    top: f64,
    height_m: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
    entraining: &ecape_rs::EcapeParcelResult,
    undiluted: &ecape_rs::EcapeParcelResult,
) -> StormRelativeLayerDiagnostics {
    let storm_u = entraining.storm_motion_u_ms;
    let storm_v = entraining.storm_motion_v_ms;
    let srh = storm_relative_helicity_layer(height_m, u_ms, v_ms, bottom, top, storm_u, storm_v);
    StormRelativeLayerDiagnostics {
        layer_m: format!("{bottom:.0}-{top:.0}"),
        storm_motion_u_ms: storm_u,
        storm_motion_v_ms: storm_v,
        srh_m2s2: srh,
        experimental_ecape_ehi: srh.and_then(|value| ehi(entraining.ecape_jkg, value)),
        undiluted_cape_ehi: srh.and_then(|value| ehi(undiluted.cape_jkg, value)),
    }
}

fn summarize_run(result: &ecape_rs::EcapeParcelResult) -> ParcelRunSummary {
    let mut max_b = f64::NEG_INFINITY;
    let mut max_h = f64::NAN;
    for (&b, &h) in result
        .parcel_profile
        .buoyancy_ms2
        .iter()
        .zip(result.parcel_profile.height_m.iter())
    {
        if b > max_b {
            max_b = b;
            max_h = h;
        }
    }
    ParcelRunSummary {
        ecape_jkg: result.ecape_jkg,
        ncape_jkg: result.ncape_jkg,
        cape_jkg: result.cape_jkg,
        cin_jkg: result.cin_jkg,
        lfc_m: result.lfc_m,
        el_m: result.el_m,
        storm_motion_u_ms: result.storm_motion_u_ms,
        storm_motion_v_ms: result.storm_motion_v_ms,
        profile_levels: result.parcel_profile.height_m.len(),
        max_buoyancy_ms2: max_b,
        max_buoyancy_height_m: max_h,
    }
}

fn layer_diagnostics(
    bottom: f64,
    top: f64,
    entraining: &ecape_rs::EcapeParcelResult,
    undiluted: &ecape_rs::EcapeParcelResult,
) -> LayerDiagnostics {
    let ent_pos = integrate_layer_positive(
        &entraining.parcel_profile.height_m,
        &entraining.parcel_profile.buoyancy_ms2,
        bottom,
        top,
    );
    let und_pos = integrate_layer_positive(
        &undiluted.parcel_profile.height_m,
        &undiluted.parcel_profile.buoyancy_ms2,
        bottom,
        top,
    );
    let mean_delta = mean_layer_delta(
        &entraining.parcel_profile.height_m,
        &entraining.parcel_profile.buoyancy_ms2,
        &undiluted.parcel_profile.buoyancy_ms2,
        bottom,
        top,
    );
    LayerDiagnostics {
        layer_m: format!("{bottom:.0}-{top:.0}"),
        entraining_positive_jkg: ent_pos,
        undiluted_positive_jkg: und_pos,
        positive_buoyancy_delta_jkg: ent_pos - und_pos,
        efficiency_ratio: positive_ratio(ent_pos, und_pos),
        mean_buoyancy_delta_ms2: mean_delta,
    }
}

fn integrate_layer_positive(heights: &[f64], buoyancy: &[f64], bottom: f64, top: f64) -> f64 {
    let mut total = 0.0;
    for idx in 0..heights.len().saturating_sub(1) {
        let z0 = heights[idx];
        let z1 = heights[idx + 1];
        if z1 <= bottom || z0 >= top {
            continue;
        }
        let lo = z0.max(bottom);
        let hi = z1.min(top);
        if hi <= lo {
            continue;
        }
        let b = 0.5 * (buoyancy[idx].max(0.0) + buoyancy[idx + 1].max(0.0));
        total += b * (hi - lo);
    }
    total
}

fn mean_layer_delta(
    heights: &[f64],
    entraining_b: &[f64],
    undiluted_b: &[f64],
    bottom: f64,
    top: f64,
) -> f64 {
    let mut total = 0.0;
    let mut dz_sum = 0.0;
    let n = heights
        .len()
        .min(entraining_b.len())
        .min(undiluted_b.len())
        .saturating_sub(1);
    for idx in 0..n {
        let z0 = heights[idx];
        let z1 = heights[idx + 1];
        if z1 <= bottom || z0 >= top {
            continue;
        }
        let lo = z0.max(bottom);
        let hi = z1.min(top);
        if hi <= lo {
            continue;
        }
        let delta0 = entraining_b[idx] - undiluted_b[idx];
        let delta1 = entraining_b[idx + 1] - undiluted_b[idx + 1];
        let dz = hi - lo;
        total += 0.5 * (delta0 + delta1) * dz;
        dz_sum += dz;
    }
    if dz_sum > 0.0 {
        total / dz_sum
    } else {
        f64::NAN
    }
}

fn max_difference(
    entraining: &ecape_rs::EcapeParcelResult,
    undiluted: &ecape_rs::EcapeParcelResult,
) -> ExtremeDifference {
    let n = entraining
        .parcel_profile
        .height_m
        .len()
        .min(entraining.parcel_profile.buoyancy_ms2.len())
        .min(undiluted.parcel_profile.buoyancy_ms2.len());
    let mut best = ExtremeDifference {
        height_m: f64::NAN,
        entraining_buoyancy_ms2: f64::NAN,
        undiluted_buoyancy_ms2: f64::NAN,
        delta_ms2: f64::NAN,
    };
    let mut best_abs = f64::NEG_INFINITY;
    for idx in 0..n {
        let delta = entraining.parcel_profile.buoyancy_ms2[idx]
            - undiluted.parcel_profile.buoyancy_ms2[idx];
        if delta.abs() > best_abs {
            best_abs = delta.abs();
            best = ExtremeDifference {
                height_m: entraining.parcel_profile.height_m[idx],
                entraining_buoyancy_ms2: entraining.parcel_profile.buoyancy_ms2[idx],
                undiluted_buoyancy_ms2: undiluted.parcel_profile.buoyancy_ms2[idx],
                delta_ms2: delta,
            };
        }
    }
    best
}

fn bulk_shear_layer(
    height_m: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
    bottom: f64,
    top: f64,
) -> Option<f64> {
    let (u0, v0) = interpolate_wind(height_m, u_ms, v_ms, bottom)?;
    let (u1, v1) = interpolate_wind(height_m, u_ms, v_ms, top)?;
    Some(((u1 - u0).powi(2) + (v1 - v0).powi(2)).sqrt())
}

fn mean_wind_layer(
    height_m: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
    bottom: f64,
    top: f64,
) -> Option<(f64, f64)> {
    let profile = clipped_wind_profile(height_m, u_ms, v_ms, bottom, top)?;
    if profile.len() < 2 {
        return None;
    }
    let mut u_total = 0.0;
    let mut v_total = 0.0;
    let mut dz_total = 0.0;
    for window in profile.windows(2) {
        let (z0, u0, v0) = window[0];
        let (z1, u1, v1) = window[1];
        let dz = z1 - z0;
        if dz <= 0.0 {
            continue;
        }
        u_total += 0.5 * (u0 + u1) * dz;
        v_total += 0.5 * (v0 + v1) * dz;
        dz_total += dz;
    }
    if dz_total > 0.0 {
        Some((u_total / dz_total, v_total / dz_total))
    } else {
        None
    }
}

fn storm_relative_helicity_layer(
    height_m: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
    bottom: f64,
    top: f64,
    storm_u: f64,
    storm_v: f64,
) -> Option<f64> {
    let profile = clipped_wind_profile(height_m, u_ms, v_ms, bottom, top)?;
    if profile.len() < 2 {
        return None;
    }
    let mut srh = 0.0;
    for window in profile.windows(2) {
        let (_, u0, v0) = window[0];
        let (_, u1, v1) = window[1];
        srh += (u1 - storm_u) * (v0 - storm_v) - (u0 - storm_u) * (v1 - storm_v);
    }
    if srh.is_finite() { Some(srh) } else { None }
}

fn clipped_wind_profile(
    height_m: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
    bottom: f64,
    top: f64,
) -> Option<Vec<(f64, f64, f64)>> {
    if top <= bottom {
        return None;
    }
    let (bottom_u, bottom_v) = interpolate_wind(height_m, u_ms, v_ms, bottom)?;
    let (top_u, top_v) = interpolate_wind(height_m, u_ms, v_ms, top)?;
    let mut profile = vec![(bottom, bottom_u, bottom_v)];
    for ((&z, &u), &v) in height_m.iter().zip(u_ms.iter()).zip(v_ms.iter()) {
        if z > bottom && z < top && z.is_finite() && u.is_finite() && v.is_finite() {
            if profile
                .last()
                .map(|(last_z, _, _)| (z - *last_z).abs() > 1.0e-6)
                .unwrap_or(true)
            {
                profile.push((z, u, v));
            }
        }
    }
    if profile
        .last()
        .map(|(last_z, _, _)| (top - *last_z).abs() > 1.0e-6)
        .unwrap_or(true)
    {
        profile.push((top, top_u, top_v));
    }
    Some(profile)
}

fn interpolate_wind(
    height_m: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
    target_m: f64,
) -> Option<(f64, f64)> {
    let n = height_m.len().min(u_ms.len()).min(v_ms.len());
    if n == 0 || !target_m.is_finite() {
        return None;
    }
    if (target_m - height_m[0]).abs() <= 1.0e-6 {
        return Some((u_ms[0], v_ms[0]));
    }
    for idx in 0..n.saturating_sub(1) {
        let z0 = height_m[idx];
        let z1 = height_m[idx + 1];
        if !z0.is_finite() || !z1.is_finite() || z1 <= z0 {
            continue;
        }
        if target_m >= z0 && target_m <= z1 {
            let frac = (target_m - z0) / (z1 - z0);
            let u = u_ms[idx] + frac * (u_ms[idx + 1] - u_ms[idx]);
            let v = v_ms[idx] + frac * (v_ms[idx + 1] - v_ms[idx]);
            if u.is_finite() && v.is_finite() {
                return Some((u, v));
            }
            return None;
        }
    }
    None
}

fn ehi(cape_jkg: f64, srh_m2s2: f64) -> Option<f64> {
    if cape_jkg.is_finite() && srh_m2s2.is_finite() {
        Some(cape_jkg * srh_m2s2 / 160000.0)
    } else {
        None
    }
}

fn positive_ratio(numerator: f64, denominator: f64) -> Option<f64> {
    if denominator.abs() > 1.0e-6 && numerator.is_finite() && denominator.is_finite() {
        Some(numerator / denominator)
    } else {
        None
    }
}

fn build_surface_augmented_column_levels(
    pressure_levels_pa: &[f64],
    temperature_c_3d: &[f64],
    qvapor_3d: &[f64],
    height_agl_3d: &[f64],
    u_3d: &[f64],
    v_3d: &[f64],
    psfc_pa: f64,
    t2_k: f64,
    q2_kgkg: f64,
    u10_ms: f64,
    v10_ms: f64,
    nz: usize,
    nxy: usize,
    ij: usize,
    model_bottom_up: bool,
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut pressure_pa = Vec::with_capacity(nz + 1);
    let mut height_m = Vec::with_capacity(nz + 1);
    let mut temp_k = Vec::with_capacity(nz + 1);
    let mut dewpoint_k = Vec::with_capacity(nz + 1);
    let mut u_ms = Vec::with_capacity(nz + 1);
    let mut v_ms = Vec::with_capacity(nz + 1);

    push_ecape_level(
        &mut pressure_pa,
        &mut height_m,
        &mut temp_k,
        &mut dewpoint_k,
        &mut u_ms,
        &mut v_ms,
        psfc_pa,
        0.0,
        t2_k,
        dewpoint_k_from_q(q2_kgkg, psfc_pa, t2_k),
        u10_ms,
        v10_ms,
    );

    let mut push_model_level = |k: usize| {
        let idx = k * nxy + ij;
        let pressure_k = pressure_levels_pa[k];
        let tk = temperature_c_3d[idx] + 273.15;
        push_ecape_level(
            &mut pressure_pa,
            &mut height_m,
            &mut temp_k,
            &mut dewpoint_k,
            &mut u_ms,
            &mut v_ms,
            pressure_k,
            height_agl_3d[idx],
            tk,
            dewpoint_k_from_q(qvapor_3d[idx], pressure_k, tk),
            u_3d[idx],
            v_3d[idx],
        );
    };

    if model_bottom_up {
        for k in 0..nz {
            push_model_level(k);
        }
    } else {
        for k in (0..nz).rev() {
            push_model_level(k);
        }
    }

    (pressure_pa, height_m, temp_k, dewpoint_k, u_ms, v_ms)
}

#[allow(clippy::too_many_arguments)]
fn push_ecape_level(
    pressure_pa: &mut Vec<f64>,
    height_m: &mut Vec<f64>,
    temp_k: &mut Vec<f64>,
    dewpoint_k: &mut Vec<f64>,
    u_ms: &mut Vec<f64>,
    v_ms: &mut Vec<f64>,
    p: f64,
    z: f64,
    t: f64,
    td: f64,
    u: f64,
    v: f64,
) {
    if !p.is_finite()
        || !z.is_finite()
        || !t.is_finite()
        || !td.is_finite()
        || !u.is_finite()
        || !v.is_finite()
    {
        return;
    }

    if let (Some(&last_p), Some(&last_z)) = (pressure_pa.last(), height_m.last()) {
        if p >= last_p || z <= last_z {
            return;
        }
    }

    pressure_pa.push(p);
    height_m.push(z);
    temp_k.push(t);
    dewpoint_k.push(td.min(t));
    u_ms.push(u);
    v_ms.push(v);
}

fn dewpoint_k_from_q(q_kgkg: f64, p_pa: f64, temp_k: f64) -> f64 {
    let q = q_kgkg.max(1.0e-10);
    let p_hpa = p_pa / 100.0;
    let e = (q * p_hpa / (0.622 + q)).max(1.0e-10);
    let ln_e = (e / 6.112).ln();
    let td_c = (243.5 * ln_e) / (17.67 - ln_e);
    (td_c + 273.15).min(temp_k)
}
