use clap::Parser;
use rustwx_calc::{
    EcapeTripletOptions, EcapeVolumeInputs, SurfaceInputs, WindGridInputs,
    compute_analytic_ecape_triplet_with_failure_mask_from_parts,
    compute_ecape_triplet_with_failure_mask_from_parts, compute_ehi,
    compute_wind_diagnostics_bundle,
};
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::gridded::{
    load_model_timestep_from_parts_cropped, prepare_heavy_volume_timed,
};
use serde::Serialize;
use std::collections::{BTreeSet, VecDeque};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "hrrr-ecape-grid-research",
    about = "Compute full-grid HRRR ECAPE research statistics for a lat/lon swath"
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
    west: f64,
    #[arg(long)]
    east: f64,
    #[arg(long)]
    south: f64,
    #[arg(long)]
    north: f64,
    #[arg(long, default_value = "custom_swath")]
    domain_slug: String,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    components_csv: Option<PathBuf>,
    #[arg(long)]
    reports_csv: Option<PathBuf>,
    #[arg(long)]
    report_overlap_csv: Option<PathBuf>,
    #[arg(long)]
    field_grid_csv: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct GridResearchReport {
    request: RequestSummary,
    grid: GridSummary,
    timing: TimingSummary,
    failure_count: usize,
    analytic_failure_count: usize,
    masks: Vec<MaskSummary>,
    fields: Vec<FieldSummary>,
}

#[derive(Debug, Serialize)]
struct RequestSummary {
    model: ModelId,
    date_yyyymmdd: String,
    cycle_utc: u8,
    forecast_hour: u16,
    source: SourceId,
    domain_slug: String,
    bounds: (f64, f64, f64, f64),
}

#[derive(Debug, Serialize)]
struct GridSummary {
    nx: usize,
    ny: usize,
    cells: usize,
    cell_area_km2_approx: f64,
    domain_area_km2_approx: f64,
    lat_min: f64,
    lat_max: f64,
    lon_min: f64,
    lon_max: f64,
}

#[derive(Debug, Default, Serialize)]
struct TimingSummary {
    load_ms: u128,
    prepare_ms: u128,
    ecape_ms: u128,
    analytic_ecape_ms: u128,
    wind_ms: u128,
    stats_ms: u128,
    total_ms: u128,
}

#[derive(Debug, Serialize)]
struct MaskSummary {
    name: String,
    description: String,
    count: usize,
    fraction: f64,
    area_km2_approx: f64,
    centroid_lat: Option<f64>,
    centroid_lon: Option<f64>,
    bounding_box: Option<(f64, f64, f64, f64)>,
    largest_component: ComponentSummary,
}

#[derive(Debug, Serialize)]
struct ComponentSummary {
    count: usize,
    fraction: f64,
    fraction_of_mask: Option<f64>,
    area_km2_approx: f64,
    centroid_lat: Option<f64>,
    centroid_lon: Option<f64>,
    bounding_box: Option<(f64, f64, f64, f64)>,
}

#[derive(Debug, Serialize)]
struct FieldSummary {
    name: String,
    units: String,
    all: BasicStats,
    by_mask: Vec<MaskedFieldStats>,
}

#[derive(Debug, Serialize)]
struct MaskedFieldStats {
    mask: String,
    stats: BasicStats,
}

#[derive(Debug, Clone, Serialize)]
struct BasicStats {
    count: usize,
    min: Option<f64>,
    p10: Option<f64>,
    p25: Option<f64>,
    median: Option<f64>,
    mean: Option<f64>,
    p75: Option<f64>,
    p90: Option<f64>,
    p95: Option<f64>,
    p99: Option<f64>,
    max: Option<f64>,
}

struct NamedMask {
    name: &'static str,
    description: &'static str,
    values: Vec<bool>,
}

struct NamedField {
    name: &'static str,
    units: &'static str,
    values: Vec<f64>,
}

#[derive(Debug, Clone)]
struct ReportPoint {
    report_id: String,
    report_type: String,
    lat: f64,
    lon: f64,
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

    let bounds = (args.west, args.east, args.south, args.north);
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
    let prepare_start = Instant::now();
    let (prepared, _prep_timing) = prepare_heavy_volume_timed(surface, pressure, false)?;
    let prepare_ms = prepare_start.elapsed().as_millis();

    let volume = EcapeVolumeInputs {
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
    };
    let surface_inputs = SurfaceInputs {
        psfc_pa: &surface.psfc_pa,
        t2_k: &surface.t2_k,
        q2_kgkg: &surface.q2_kgkg,
        u10_ms: &surface.u10_ms,
        v10_ms: &surface.v10_ms,
    };

    let ecape_start = Instant::now();
    let triplet = compute_ecape_triplet_with_failure_mask_from_parts(
        prepared.grid,
        volume,
        surface_inputs,
        EcapeTripletOptions::new("right_moving"),
    )?;
    let ecape_ms = ecape_start.elapsed().as_millis();

    let analytic_ecape_start = Instant::now();
    let analytic_triplet = compute_analytic_ecape_triplet_with_failure_mask_from_parts(
        prepared.grid,
        volume,
        surface_inputs,
        EcapeTripletOptions::new("right_moving"),
    )?;
    let analytic_ecape_ms = analytic_ecape_start.elapsed().as_millis();

    let wind_start = Instant::now();
    let wind = compute_wind_diagnostics_bundle(WindGridInputs {
        shape: prepared.shape,
        u_3d_ms: &pressure.u_ms_3d,
        v_3d_ms: &pressure.v_ms_3d,
        height_agl_3d_m: &prepared.height_agl_3d,
    })?;
    let wind_ms = wind_start.elapsed().as_millis();

    let sb_ehi_01 = compute_ehi(
        prepared.grid,
        &triplet.sb.fields.ecape_jkg,
        &wind.srh_01km_m2s2,
    )?;
    let sb_ehi_03 = compute_ehi(
        prepared.grid,
        &triplet.sb.fields.ecape_jkg,
        &wind.srh_03km_m2s2,
    )?;
    let ml_ehi_01 = compute_ehi(
        prepared.grid,
        &triplet.ml.fields.ecape_jkg,
        &wind.srh_01km_m2s2,
    )?;
    let ml_ehi_03 = compute_ehi(
        prepared.grid,
        &triplet.ml.fields.ecape_jkg,
        &wind.srh_03km_m2s2,
    )?;
    let ml_cape_ehi_03 = compute_ehi(
        prepared.grid,
        &triplet.ml.fields.cape_jkg,
        &wind.srh_03km_m2s2,
    )?;
    let ml_analytic_ehi_03 = compute_ehi(
        prepared.grid,
        &analytic_triplet.ml.fields.ecape_jkg,
        &wind.srh_03km_m2s2,
    )?;
    let mu_ehi_03 = compute_ehi(
        prepared.grid,
        &triplet.mu.fields.ecape_jkg,
        &wind.srh_03km_m2s2,
    )?;

    let ml_ratio = ratio(
        &triplet.ml.fields.ecape_jkg,
        &triplet.ml.fields.cape_jkg,
        100.0,
    );
    let mu_ratio = ratio(
        &triplet.mu.fields.ecape_jkg,
        &triplet.mu.fields.cape_jkg,
        100.0,
    );
    let sb_ratio = ratio(
        &triplet.sb.fields.ecape_jkg,
        &triplet.sb.fields.cape_jkg,
        100.0,
    );
    let mut fields = vec![
        NamedField::new("sb_ecape", "J/kg", triplet.sb.fields.ecape_jkg.clone()),
        NamedField::new("ml_ecape", "J/kg", triplet.ml.fields.ecape_jkg.clone()),
        NamedField::new("mu_ecape", "J/kg", triplet.mu.fields.ecape_jkg.clone()),
        NamedField::new(
            "sb_cape_undiluted",
            "J/kg",
            triplet.sb.fields.cape_jkg.clone(),
        ),
        NamedField::new(
            "ml_cape_undiluted",
            "J/kg",
            triplet.ml.fields.cape_jkg.clone(),
        ),
        NamedField::new(
            "mu_cape_undiluted",
            "J/kg",
            triplet.mu.fields.cape_jkg.clone(),
        ),
        NamedField::new("sb_ecape_cape_ratio", "ratio", sb_ratio.clone()),
        NamedField::new("ml_ecape_cape_ratio", "ratio", ml_ratio.clone()),
        NamedField::new("mu_ecape_cape_ratio", "ratio", mu_ratio.clone()),
        NamedField::new(
            "ml_cape_minus_ecape",
            "J/kg",
            diff(&triplet.ml.fields.cape_jkg, &triplet.ml.fields.ecape_jkg),
        ),
        NamedField::new("srh_0_1km", "m2/s2", wind.srh_01km_m2s2.clone()),
        NamedField::new("srh_0_3km", "m2/s2", wind.srh_03km_m2s2.clone()),
        NamedField::new("shear_0_6km", "m/s", wind.shear_06km_ms.clone()),
        NamedField::new(
            "ml_analytic_ecape",
            "J/kg",
            analytic_triplet.ml.fields.ecape_jkg.clone(),
        ),
        NamedField::new(
            "ml_ecape_minus_analytic_ecape",
            "J/kg",
            diff(
                &triplet.ml.fields.ecape_jkg,
                &analytic_triplet.ml.fields.ecape_jkg,
            ),
        ),
        NamedField::new("sb_ecape_ehi_0_1km", "dimensionless", sb_ehi_01),
        NamedField::new("sb_ecape_ehi_0_3km", "dimensionless", sb_ehi_03),
        NamedField::new("ml_ecape_ehi_0_1km", "dimensionless", ml_ehi_01),
        NamedField::new("ml_ecape_ehi_0_3km", "dimensionless", ml_ehi_03.clone()),
        NamedField::new("ml_cape_ehi_0_3km", "dimensionless", ml_cape_ehi_03.clone()),
        NamedField::new(
            "ml_analytic_ecape_ehi_0_3km",
            "dimensionless",
            ml_analytic_ehi_03.clone(),
        ),
        NamedField::new("mu_ecape_ehi_0_3km", "dimensionless", mu_ehi_03),
    ];
    if let Some(native_mlcape) = surface.native_mlcape_jkg.as_ref() {
        fields.push(NamedField::new(
            "native_mlcape",
            "J/kg",
            native_mlcape.clone(),
        ));
        fields.push(NamedField::new(
            "ml_ecape_native_cape_ratio",
            "ratio",
            ratio(&triplet.ml.fields.ecape_jkg, native_mlcape, 100.0),
        ));
    }

    let masks = build_masks(
        &triplet.ml.fields.cape_jkg,
        &triplet.ml.fields.ecape_jkg,
        &analytic_triplet.ml.fields.ecape_jkg,
        &ml_ratio,
        &ml_ehi_03,
        &ml_cape_ehi_03,
        &ml_analytic_ehi_03,
        &wind.srh_03km_m2s2,
        &wind.shear_06km_ms,
    );
    let stats_start = Instant::now();
    let cell_area_km2 =
        estimate_cell_area_km2(surface.nx, surface.ny, &surface.lat, &surface.lon).unwrap_or(9.0);
    let mask_summaries = masks
        .iter()
        .map(|mask| {
            summarize_mask(
                mask,
                surface.nx,
                surface.ny,
                &surface.lat,
                &surface.lon,
                cell_area_km2,
            )
        })
        .collect::<Vec<_>>();
    let field_summaries = fields
        .iter()
        .map(|field| summarize_field(field, &masks))
        .collect::<Vec<_>>();

    if let Some(path) = args.components_csv.as_ref() {
        write_components_csv(
            path,
            &args,
            &masks,
            surface.nx,
            surface.ny,
            &surface.lat,
            &surface.lon,
            cell_area_km2,
        )?;
    }

    if let (Some(reports_path), Some(overlap_path)) =
        (args.reports_csv.as_ref(), args.report_overlap_csv.as_ref())
    {
        let reports = read_report_points_csv(reports_path)?;
        write_report_overlap_csv(
            overlap_path,
            &args,
            &masks,
            surface.nx,
            surface.ny,
            &surface.lat,
            &surface.lon,
            cell_area_km2,
            &reports,
        )?;
    }

    if let Some(path) = args.field_grid_csv.as_ref() {
        write_field_grid_csv(
            path,
            surface.nx,
            surface.ny,
            &surface.lat,
            &surface.lon,
            &fields,
        )?;
    }

    let stats_ms = stats_start.elapsed().as_millis();

    let report = GridResearchReport {
        request: RequestSummary {
            model: args.model,
            date_yyyymmdd: args.date,
            cycle_utc: args.cycle,
            forecast_hour: args.forecast_hour,
            source: args.source,
            domain_slug: args.domain_slug,
            bounds,
        },
        grid: GridSummary {
            nx: surface.nx,
            ny: surface.ny,
            cells: surface.nx * surface.ny,
            cell_area_km2_approx: cell_area_km2,
            domain_area_km2_approx: (surface.nx * surface.ny) as f64 * cell_area_km2,
            lat_min: finite_min(&surface.lat).unwrap_or(f64::NAN),
            lat_max: finite_max(&surface.lat).unwrap_or(f64::NAN),
            lon_min: finite_min(&surface.lon).unwrap_or(f64::NAN),
            lon_max: finite_max(&surface.lon).unwrap_or(f64::NAN),
        },
        timing: TimingSummary {
            load_ms,
            prepare_ms,
            ecape_ms,
            analytic_ecape_ms,
            wind_ms,
            stats_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
        failure_count: triplet.total_failure_count(),
        analytic_failure_count: analytic_triplet.total_failure_count(),
        masks: mask_summaries,
        fields: field_summaries,
    };

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(args.output, serde_json::to_string_pretty(&report)?)?;
    Ok(())
}

impl NamedField {
    fn new(name: &'static str, units: &'static str, values: Vec<f64>) -> Self {
        Self {
            name,
            units,
            values,
        }
    }
}

fn build_masks(
    ml_cape: &[f64],
    ml_ecape: &[f64],
    ml_analytic_ecape: &[f64],
    ml_ratio: &[f64],
    ml_ehi_03: &[f64],
    ml_cape_ehi_03: &[f64],
    ml_analytic_ehi_03: &[f64],
    srh_03: &[f64],
    shear_06: &[f64],
) -> Vec<NamedMask> {
    let mut masks = Vec::new();
    for threshold in CAPE_ECAPE_THRESHOLDS {
        push_threshold_mask(
            &mut masks,
            "ml_cape_ge",
            "ML undiluted CAPE",
            "J/kg",
            ml_cape,
            *threshold,
        );
        push_threshold_mask(
            &mut masks,
            "ml_ecape_ge",
            "ML ECAPE",
            "J/kg",
            ml_ecape,
            *threshold,
        );
        push_threshold_mask(
            &mut masks,
            "ml_analytic_ecape_ge",
            "ML analytic ECAPE",
            "J/kg",
            ml_analytic_ecape,
            *threshold,
        );
    }
    for threshold in EHI_THRESHOLDS {
        push_threshold_mask(
            &mut masks,
            "ml_ecape_ehi03_ge",
            "ML ECAPE-EHI 0-3 km",
            "",
            ml_ehi_03,
            *threshold,
        );
        push_threshold_mask(
            &mut masks,
            "ml_cape_ehi03_ge",
            "Traditional ML CAPE-EHI 0-3 km",
            "",
            ml_cape_ehi_03,
            *threshold,
        );
        push_threshold_mask(
            &mut masks,
            "ml_analytic_ecape_ehi03_ge",
            "Analytic ML ECAPE-EHI 0-3 km",
            "",
            ml_analytic_ehi_03,
            *threshold,
        );
    }

    masks.extend([
        NamedMask {
            name: "warm_sector_combo",
            description: "ML CAPE >= 500 J/kg, 0-3 km SRH >= 100 m2/s2, 0-6 km shear >= 20 m/s",
            values: combine3(ml_cape, 500.0, srh_03, 100.0, shear_06, 20.0),
        },
        NamedMask {
            name: "high_end_ecape_ehi_combo",
            description: "ML ECAPE >= 1000 J/kg, 0-3 km SRH >= 150 m2/s2, 0-6 km shear >= 20 m/s",
            values: combine3(ml_ecape, 1000.0, srh_03, 150.0, shear_06, 20.0),
        },
        NamedMask {
            name: "ratio_low_cape_plume",
            description: "ML ECAPE/CAPE ratio < 0.75 where ML undiluted CAPE >= 500 J/kg",
            values: ml_ratio
                .iter()
                .zip(ml_cape.iter())
                .map(|(&ratio, &cape)| ratio.is_finite() && cape >= 500.0 && ratio < 0.75)
                .collect(),
        },
        NamedMask {
            name: "ratio_low_cape250_plume",
            description: "ML ECAPE/CAPE ratio < 0.75 where ML undiluted CAPE >= 250 J/kg",
            values: ml_ratio
                .iter()
                .zip(ml_cape.iter())
                .map(|(&ratio, &cape)| ratio.is_finite() && cape >= 250.0 && ratio < 0.75)
                .collect(),
        },
        NamedMask {
            name: "ratio_high_cape_plume",
            description: "ML ECAPE/CAPE ratio >= 1.0 where ML undiluted CAPE >= 500 J/kg",
            values: ml_ratio
                .iter()
                .zip(ml_cape.iter())
                .map(|(&ratio, &cape)| ratio.is_finite() && cape >= 500.0 && ratio >= 1.0)
                .collect(),
        },
        NamedMask {
            name: "ratio_high_cape250_plume",
            description: "ML ECAPE/CAPE ratio >= 1.0 where ML undiluted CAPE >= 250 J/kg",
            values: ml_ratio
                .iter()
                .zip(ml_cape.iter())
                .map(|(&ratio, &cape)| ratio.is_finite() && cape >= 250.0 && ratio >= 1.0)
                .collect(),
        },
        NamedMask {
            name: "ratio_high_ecape500_plume",
            description: "ML ECAPE/CAPE ratio >= 1.0 where ML ECAPE >= 500 J/kg",
            values: ml_ratio
                .iter()
                .zip(ml_ecape.iter())
                .map(|(&ratio, &ecape)| ratio.is_finite() && ecape >= 500.0 && ratio >= 1.0)
                .collect(),
        },
    ]);

    masks
}

const CAPE_ECAPE_THRESHOLDS: &[f64] = &[
    100.0, 250.0, 400.0, 500.0, 600.0, 750.0, 900.0, 1000.0, 1250.0, 1500.0, 1750.0, 2000.0, 2500.0,
];

const EHI_THRESHOLDS: &[f64] = &[
    0.10, 0.25, 0.40, 0.50, 0.65, 0.75, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00, 4.00,
];

fn threshold_label(threshold: f64) -> String {
    if threshold.fract().abs() < 1.0e-9 {
        format!("{threshold:.0}")
    } else {
        let text = format!("{threshold:.2}");
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

fn threshold_token(threshold: f64) -> String {
    threshold_label(threshold).replace('.', "p")
}

fn leaked(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

fn push_threshold_mask(
    masks: &mut Vec<NamedMask>,
    prefix: &str,
    label: &str,
    units: &str,
    values: &[f64],
    threshold: f64,
) {
    let threshold_text = threshold_label(threshold);
    let unit_text = if units.is_empty() {
        String::new()
    } else {
        format!(" {units}")
    };
    masks.push(mask_threshold(
        leaked(format!("{}_{}", prefix, threshold_token(threshold))),
        leaked(format!("{label} >= {threshold_text}{unit_text}")),
        values,
        threshold,
    ));
}

fn mask_threshold(
    name: &'static str,
    description: &'static str,
    values: &[f64],
    threshold: f64,
) -> NamedMask {
    NamedMask {
        name,
        description,
        values: values
            .iter()
            .map(|&value| value.is_finite() && value >= threshold)
            .collect(),
    }
}

fn combine3(a: &[f64], a_min: f64, b: &[f64], b_min: f64, c: &[f64], c_min: f64) -> Vec<bool> {
    a.iter()
        .zip(b.iter())
        .zip(c.iter())
        .map(|((&av, &bv), &cv)| {
            av.is_finite()
                && bv.is_finite()
                && cv.is_finite()
                && av >= a_min
                && bv >= b_min
                && cv >= c_min
        })
        .collect()
}

fn summarize_mask(
    mask: &NamedMask,
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    cell_area_km2: f64,
) -> MaskSummary {
    let mut count = 0usize;
    let mut lat_sum = 0.0;
    let mut lon_sum = 0.0;
    let mut lat_min = f64::INFINITY;
    let mut lat_max = f64::NEG_INFINITY;
    let mut lon_min = f64::INFINITY;
    let mut lon_max = f64::NEG_INFINITY;
    for ((&included, &la), &lo) in mask.values.iter().zip(lat.iter()).zip(lon.iter()) {
        if included && la.is_finite() && lo.is_finite() {
            count += 1;
            lat_sum += la;
            lon_sum += lo;
            lat_min = lat_min.min(la);
            lat_max = lat_max.max(la);
            lon_min = lon_min.min(lo);
            lon_max = lon_max.max(lo);
        }
    }
    MaskSummary {
        name: mask.name.to_string(),
        description: mask.description.to_string(),
        count,
        fraction: if mask.values.is_empty() {
            0.0
        } else {
            count as f64 / mask.values.len() as f64
        },
        area_km2_approx: count as f64 * cell_area_km2,
        centroid_lat: (count > 0).then_some(lat_sum / count as f64),
        centroid_lon: (count > 0).then_some(lon_sum / count as f64),
        bounding_box: (count > 0).then_some((lon_min, lon_max, lat_min, lat_max)),
        largest_component: largest_component_summary(mask, nx, ny, lat, lon, count, cell_area_km2),
    }
}

fn largest_component_summary(
    mask: &NamedMask,
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    mask_count: usize,
    cell_area_km2: f64,
) -> ComponentSummary {
    if nx == 0 || ny == 0 || mask.values.len() != nx * ny {
        return ComponentSummary {
            count: 0,
            fraction: 0.0,
            fraction_of_mask: None,
            area_km2_approx: 0.0,
            centroid_lat: None,
            centroid_lon: None,
            bounding_box: None,
        };
    }

    let mut visited = vec![false; mask.values.len()];
    let mut best = ComponentAccumulator::default();
    let mut queue = VecDeque::new();

    for start in 0..mask.values.len() {
        if visited[start] || !mask.values[start] {
            continue;
        }
        visited[start] = true;
        queue.push_back(start);
        let mut current = ComponentAccumulator::default();

        while let Some(idx) = queue.pop_front() {
            current.add(idx, lat, lon);
            let x = idx % nx;
            let y = idx / nx;
            let x_start = x.saturating_sub(1);
            let y_start = y.saturating_sub(1);
            let x_end = (x + 1).min(nx - 1);
            let y_end = (y + 1).min(ny - 1);

            for yy in y_start..=y_end {
                for xx in x_start..=x_end {
                    if xx == x && yy == y {
                        continue;
                    }
                    let neighbor = yy * nx + xx;
                    if !visited[neighbor] && mask.values[neighbor] {
                        visited[neighbor] = true;
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        if current.count > best.count {
            best = current;
        }
    }

    best.into_summary(mask.values.len(), mask_count, cell_area_km2)
}

#[derive(Debug, Default)]
struct ComponentDetail {
    indices: Vec<usize>,
    accumulator: ComponentAccumulator,
}

impl ComponentDetail {
    fn add(&mut self, idx: usize, lat: &[f64], lon: &[f64]) {
        self.indices.push(idx);
        self.accumulator.add(idx, lat, lon);
    }

    fn summary(
        &self,
        grid_count: usize,
        mask_count: usize,
        cell_area_km2: f64,
    ) -> ComponentSummary {
        self.accumulator
            .to_summary(grid_count, mask_count, cell_area_km2)
    }
}

fn collect_components(
    mask: &NamedMask,
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
) -> Vec<ComponentDetail> {
    if nx == 0 || ny == 0 || mask.values.len() != nx * ny {
        return Vec::new();
    }

    let mut visited = vec![false; mask.values.len()];
    let mut components = Vec::new();
    let mut queue = VecDeque::new();

    for start in 0..mask.values.len() {
        if visited[start] || !mask.values[start] {
            continue;
        }
        visited[start] = true;
        queue.push_back(start);
        let mut current = ComponentDetail::default();

        while let Some(idx) = queue.pop_front() {
            current.add(idx, lat, lon);
            let x = idx % nx;
            let y = idx / nx;
            let x_start = x.saturating_sub(1);
            let y_start = y.saturating_sub(1);
            let x_end = (x + 1).min(nx - 1);
            let y_end = (y + 1).min(ny - 1);

            for yy in y_start..=y_end {
                for xx in x_start..=x_end {
                    if xx == x && yy == y {
                        continue;
                    }
                    let neighbor = yy * nx + xx;
                    if !visited[neighbor] && mask.values[neighbor] {
                        visited[neighbor] = true;
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        components.push(current);
    }

    components.sort_by(|a, b| b.accumulator.count.cmp(&a.accumulator.count));
    components
}

#[derive(Debug, Default)]
struct ComponentAccumulator {
    count: usize,
    lat_sum: f64,
    lon_sum: f64,
    lat_min: f64,
    lat_max: f64,
    lon_min: f64,
    lon_max: f64,
}

impl ComponentAccumulator {
    fn add(&mut self, idx: usize, lat: &[f64], lon: &[f64]) {
        let Some((&la, &lo)) = lat.get(idx).zip(lon.get(idx)) else {
            return;
        };
        if !la.is_finite() || !lo.is_finite() {
            return;
        }
        if self.count == 0 {
            self.lat_min = la;
            self.lat_max = la;
            self.lon_min = lo;
            self.lon_max = lo;
        } else {
            self.lat_min = self.lat_min.min(la);
            self.lat_max = self.lat_max.max(la);
            self.lon_min = self.lon_min.min(lo);
            self.lon_max = self.lon_max.max(lo);
        }
        self.count += 1;
        self.lat_sum += la;
        self.lon_sum += lo;
    }

    fn into_summary(
        self,
        grid_count: usize,
        mask_count: usize,
        cell_area_km2: f64,
    ) -> ComponentSummary {
        self.to_summary(grid_count, mask_count, cell_area_km2)
    }

    fn to_summary(
        &self,
        grid_count: usize,
        mask_count: usize,
        cell_area_km2: f64,
    ) -> ComponentSummary {
        ComponentSummary {
            count: self.count,
            fraction: if grid_count == 0 {
                0.0
            } else {
                self.count as f64 / grid_count as f64
            },
            fraction_of_mask: (mask_count > 0).then_some(self.count as f64 / mask_count as f64),
            area_km2_approx: self.count as f64 * cell_area_km2,
            centroid_lat: (self.count > 0).then_some(self.lat_sum / self.count as f64),
            centroid_lon: (self.count > 0).then_some(self.lon_sum / self.count as f64),
            bounding_box: (self.count > 0).then_some((
                self.lon_min,
                self.lon_max,
                self.lat_min,
                self.lat_max,
            )),
        }
    }
}

fn write_components_csv(
    path: &PathBuf,
    args: &Args,
    masks: &[NamedMask],
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    cell_area_km2: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(path)?;
    writeln!(
        file,
        "case_id,date,cycle,forecast_hour,domain_slug,mask,component_rank,cell_count,area_km2,fraction_grid,fraction_mask,centroid_lat,centroid_lon,lon_min,lon_max,lat_min,lat_max"
    )?;
    let case_id = format!("{}_{}z_{}", args.date, args.cycle, args.domain_slug);
    let grid_count = nx * ny;
    for mask in masks {
        let mask_count = mask.values.iter().filter(|&&value| value).count();
        let components = collect_components(mask, nx, ny, lat, lon);
        for (rank, component) in components.iter().enumerate() {
            let summary = component.summary(grid_count, mask_count, cell_area_km2);
            let bbox = summary.bounding_box;
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{:.6},{:.10},{},{},{},{},{},{},{}",
                csv_cell(&case_id),
                args.date,
                args.cycle,
                args.forecast_hour,
                csv_cell(&args.domain_slug),
                csv_cell(mask.name),
                rank + 1,
                summary.count,
                summary.area_km2_approx,
                summary.fraction,
                opt_f64(summary.fraction_of_mask),
                opt_f64(summary.centroid_lat),
                opt_f64(summary.centroid_lon),
                opt_f64(bbox.map(|value| value.0)),
                opt_f64(bbox.map(|value| value.1)),
                opt_f64(bbox.map(|value| value.2)),
                opt_f64(bbox.map(|value| value.3)),
            )?;
        }
    }
    Ok(())
}

fn read_report_points_csv(path: &PathBuf) -> Result<Vec<ReportPoint>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut lines = reader.lines();
    let Some(header_line) = lines.next() else {
        return Ok(Vec::new());
    };
    let header = split_simple_csv(&header_line?);
    let lat_idx = find_column(&header, &["lat", "latitude"])?;
    let lon_idx = find_column(&header, &["lon", "longitude"])?;
    let type_idx = find_column(&header, &["report_type", "type"])?;
    let id_idx = find_column_optional(&header, &["report_id", "id"]);
    let mut reports = Vec::new();

    for (row_idx, line) in lines.enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let cols = split_simple_csv(&line);
        let Some(lat) = cols
            .get(lat_idx)
            .and_then(|value| value.parse::<f64>().ok())
        else {
            continue;
        };
        let Some(lon) = cols
            .get(lon_idx)
            .and_then(|value| value.parse::<f64>().ok())
        else {
            continue;
        };
        if !lat.is_finite() || !lon.is_finite() {
            continue;
        }
        let report_type = cols
            .get(type_idx)
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "unknown".to_string());
        let report_id = id_idx
            .and_then(|idx| cols.get(idx).cloned())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("report_{}", row_idx + 1));
        reports.push(ReportPoint {
            report_id,
            report_type,
            lat,
            lon,
        });
    }

    Ok(reports)
}

fn write_field_grid_csv(
    path: &PathBuf,
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    fields: &[NamedField],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let wanted = [
        "ml_cape_undiluted",
        "ml_ecape",
        "ml_analytic_ecape",
        "ml_cape_minus_ecape",
        "ml_ecape_minus_analytic_ecape",
        "ml_ecape_ehi_0_3km",
        "ml_cape_ehi_0_3km",
        "ml_analytic_ecape_ehi_0_3km",
        "srh_0_3km",
        "shear_0_6km",
    ];
    let selected = wanted
        .iter()
        .filter_map(|name| {
            fields
                .iter()
                .find(|field| field.name == *name)
                .map(|field| (*name, field.values.as_slice()))
        })
        .collect::<Vec<_>>();
    let grid_count = nx.saturating_mul(ny);
    if lat.len() != grid_count || lon.len() != grid_count {
        return Err(format!(
            "lat/lon length mismatch: nx={nx} ny={ny} lat={} lon={}",
            lat.len(),
            lon.len()
        )
        .into());
    }
    for (name, values) in &selected {
        if values.len() != grid_count {
            return Err(format!(
                "field {name} length mismatch: expected {grid_count}, got {}",
                values.len()
            )
            .into());
        }
    }

    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);
    write!(writer, "i,j,lat,lon")?;
    for (name, _values) in &selected {
        write!(writer, ",{}", csv_cell(name))?;
    }
    writeln!(writer)?;

    for idx in 0..grid_count {
        let i = idx % nx;
        let j = idx / nx;
        write!(writer, "{i},{j},{:.6},{:.6}", lat[idx], lon[idx])?;
        for (_name, values) in &selected {
            write!(writer, ",{}", values[idx])?;
        }
        writeln!(writer)?;
    }
    Ok(())
}

fn write_report_overlap_csv(
    path: &PathBuf,
    args: &Args,
    masks: &[NamedMask],
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    cell_area_km2: f64,
    reports: &[ReportPoint],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(path)?;
    writeln!(
        file,
        "case_id,date,cycle,forecast_hour,domain_slug,mask,report_type,num_reports,coverage_fraction,coverage_area_km2,largest_object_fraction,largest_object_area_km2,reports_inside_any,reports_within_25km_any,reports_within_40km_any,reports_within_80km_any,median_distance_any_km,p90_distance_any_km,reports_inside_largest,reports_within_25km_largest,reports_within_40km_largest,reports_within_80km_largest,median_distance_largest_km,p90_distance_largest_km,object_efficiency_40km_any,object_efficiency_40km_largest,search_radius_km"
    )?;

    let case_id = format!("{}_{}z_{}", args.date, args.cycle, args.domain_slug);
    let grid_count = nx * ny;
    let search_radius_km = 90.0;
    let search_radius_cells = ((search_radius_km / cell_area_km2.sqrt().max(1.0)).ceil() as usize)
        .saturating_add(6)
        .max(35);
    let nearest_indices = reports
        .iter()
        .map(|report| nearest_grid_index(report.lat, report.lon, nx, ny, lat, lon))
        .collect::<Vec<_>>();
    let mut report_types = BTreeSet::new();
    report_types.insert("all".to_string());
    for report in reports {
        report_types.insert(report.report_type.clone());
    }

    for mask in masks {
        let mask_count = mask.values.iter().filter(|&&value| value).count();
        let coverage_fraction = if grid_count == 0 {
            0.0
        } else {
            mask_count as f64 / grid_count as f64
        };
        let coverage_area_km2 = mask_count as f64 * cell_area_km2;
        let components = collect_components(mask, nx, ny, lat, lon);
        let mut largest_values = vec![false; mask.values.len()];
        if let Some(largest) = components.first() {
            for &idx in &largest.indices {
                largest_values[idx] = true;
            }
        }
        let largest_count = components
            .first()
            .map(|component| component.indices.len())
            .unwrap_or(0);
        let largest_fraction = if grid_count == 0 {
            0.0
        } else {
            largest_count as f64 / grid_count as f64
        };
        let largest_area_km2 = largest_count as f64 * cell_area_km2;

        for report_type in &report_types {
            let report_indices = reports
                .iter()
                .enumerate()
                .filter_map(|(idx, report)| {
                    (report_type == "all" || &report.report_type == report_type).then_some(idx)
                })
                .collect::<Vec<_>>();
            let stats_any = overlap_stats_for_reports(
                &report_indices,
                reports,
                &nearest_indices,
                &mask.values,
                nx,
                ny,
                lat,
                lon,
                search_radius_cells,
            );
            let stats_largest = overlap_stats_for_reports(
                &report_indices,
                reports,
                &nearest_indices,
                &largest_values,
                nx,
                ny,
                lat,
                lon,
                search_radius_cells,
            );
            let n = report_indices.len();
            let object_eff_any = efficiency(stats_any.within_40km, n, coverage_fraction);
            let object_eff_largest = efficiency(stats_largest.within_40km, n, largest_fraction);
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{:.10},{:.6},{:.10},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.1}",
                csv_cell(&case_id),
                args.date,
                args.cycle,
                args.forecast_hour,
                csv_cell(&args.domain_slug),
                csv_cell(mask.name),
                csv_cell(report_type),
                n,
                coverage_fraction,
                coverage_area_km2,
                largest_fraction,
                largest_area_km2,
                stats_any.inside,
                stats_any.within_25km,
                stats_any.within_40km,
                stats_any.within_80km,
                fmt_f64(stats_any.median_distance_km),
                fmt_f64(stats_any.p90_distance_km),
                stats_largest.inside,
                stats_largest.within_25km,
                stats_largest.within_40km,
                stats_largest.within_80km,
                fmt_f64(stats_largest.median_distance_km),
                fmt_f64(stats_largest.p90_distance_km),
                fmt_f64(object_eff_any),
                fmt_f64(object_eff_largest),
                search_radius_km,
            )?;
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct OverlapStats {
    inside: usize,
    within_25km: usize,
    within_40km: usize,
    within_80km: usize,
    median_distance_km: Option<f64>,
    p90_distance_km: Option<f64>,
}

fn overlap_stats_for_reports(
    report_indices: &[usize],
    reports: &[ReportPoint],
    nearest_indices: &[Option<usize>],
    mask_values: &[bool],
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    search_radius_cells: usize,
) -> OverlapStats {
    let mut distances = Vec::new();
    let mut stats = OverlapStats::default();
    for &report_idx in report_indices {
        let Some(report) = reports.get(report_idx) else {
            continue;
        };
        let Some(nearest_idx) = nearest_indices.get(report_idx).and_then(|value| *value) else {
            continue;
        };
        let distance = distance_to_mask_near_index(
            mask_values,
            nx,
            ny,
            lat,
            lon,
            nearest_idx,
            report.lat,
            report.lon,
            search_radius_cells,
        )
        .unwrap_or(999.0);
        if distance <= 0.01 {
            stats.inside += 1;
        }
        if distance <= 25.0 {
            stats.within_25km += 1;
        }
        if distance <= 40.0 {
            stats.within_40km += 1;
        }
        if distance <= 80.0 {
            stats.within_80km += 1;
        }
        distances.push(distance);
    }
    distances.sort_by(|a, b| a.total_cmp(b));
    stats.median_distance_km = percentile(&distances, 0.50);
    stats.p90_distance_km = percentile(&distances, 0.90);
    stats
}

fn nearest_grid_index(
    report_lat: f64,
    report_lon: f64,
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
) -> Option<usize> {
    if nx == 0 || ny == 0 || lat.len() != nx * ny || lon.len() != nx * ny {
        return None;
    }
    let coarse_step = 16usize;
    let mut best_idx = None;
    let mut best_dist = f64::INFINITY;
    for y in (0..ny).step_by(coarse_step) {
        for x in (0..nx).step_by(coarse_step) {
            let idx = y * nx + x;
            let dist = haversine_km(report_lat, report_lon, lat[idx], lon[idx]);
            if dist.is_finite() && dist < best_dist {
                best_dist = dist;
                best_idx = Some(idx);
            }
        }
    }
    let coarse_idx = best_idx?;
    let mut refined_idx = coarse_idx;
    let best_x = coarse_idx % nx;
    let best_y = coarse_idx / nx;
    let refine_radius = coarse_step * 2;
    let y0 = best_y.saturating_sub(refine_radius);
    let y1 = (best_y + refine_radius).min(ny - 1);
    let x0 = best_x.saturating_sub(refine_radius);
    let x1 = (best_x + refine_radius).min(nx - 1);
    for y in y0..=y1 {
        for x in x0..=x1 {
            let idx = y * nx + x;
            let dist = haversine_km(report_lat, report_lon, lat[idx], lon[idx]);
            if dist.is_finite() && dist < best_dist {
                best_dist = dist;
                refined_idx = idx;
            }
        }
    }
    Some(refined_idx)
}

fn distance_to_mask_near_index(
    mask_values: &[bool],
    nx: usize,
    ny: usize,
    lat: &[f64],
    lon: &[f64],
    nearest_idx: usize,
    report_lat: f64,
    report_lon: f64,
    radius_cells: usize,
) -> Option<f64> {
    if nx == 0
        || ny == 0
        || nearest_idx >= nx * ny
        || mask_values.len() != nx * ny
        || lat.len() != nx * ny
        || lon.len() != nx * ny
    {
        return None;
    }
    if mask_values[nearest_idx] {
        return Some(0.0);
    }
    let x = nearest_idx % nx;
    let y = nearest_idx / nx;
    let y0 = y.saturating_sub(radius_cells);
    let y1 = (y + radius_cells).min(ny - 1);
    let x0 = x.saturating_sub(radius_cells);
    let x1 = (x + radius_cells).min(nx - 1);
    let mut best = f64::INFINITY;
    for yy in y0..=y1 {
        for xx in x0..=x1 {
            let idx = yy * nx + xx;
            if !mask_values[idx] {
                continue;
            }
            let dist = haversine_km(report_lat, report_lon, lat[idx], lon[idx]);
            if dist.is_finite() && dist < best {
                best = dist;
            }
        }
    }
    best.is_finite().then_some(best)
}

fn efficiency(captured: usize, total: usize, coverage_fraction: f64) -> Option<f64> {
    if total == 0 || coverage_fraction <= 0.0 {
        None
    } else {
        Some((captured as f64 / total as f64) / coverage_fraction)
    }
}

fn split_simple_csv(line: &str) -> Vec<String> {
    let mut cols = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                cols.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    cols.push(current.trim().to_string());
    cols
}

fn find_column(header: &[String], names: &[&str]) -> Result<usize, Box<dyn std::error::Error>> {
    find_column_optional(header, names).ok_or_else(|| {
        format!(
            "missing required CSV column; expected one of: {}",
            names.join(", ")
        )
        .into()
    })
}

fn find_column_optional(header: &[String], names: &[&str]) -> Option<usize> {
    header.iter().position(|column| {
        names
            .iter()
            .any(|name| column.trim().eq_ignore_ascii_case(name))
    })
}

fn csv_cell(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn fmt_f64(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{value:.6}"))
        .unwrap_or_default()
}

fn opt_f64(value: Option<f64>) -> String {
    fmt_f64(value)
}

fn estimate_cell_area_km2(nx: usize, ny: usize, lat: &[f64], lon: &[f64]) -> Option<f64> {
    if nx < 2 || ny < 2 || lat.len() != nx * ny || lon.len() != nx * ny {
        return None;
    }
    let x_step = (nx / 80).max(1);
    let y_step = (ny / 80).max(1);
    let mut areas = Vec::new();
    for y in (0..ny - 1).step_by(y_step) {
        for x in (0..nx - 1).step_by(x_step) {
            let idx = y * nx + x;
            let east = idx + 1;
            let north = idx + nx;
            let dx = haversine_km(lat[idx], lon[idx], lat[east], lon[east]);
            let dy = haversine_km(lat[idx], lon[idx], lat[north], lon[north]);
            let area = dx * dy;
            if area.is_finite() && area > 0.0 {
                areas.push(area);
            }
        }
    }
    areas.sort_by(|a, b| a.total_cmp(b));
    percentile(&areas, 0.50)
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_KM * a.sqrt().asin()
}

fn summarize_field(field: &NamedField, masks: &[NamedMask]) -> FieldSummary {
    FieldSummary {
        name: field.name.to_string(),
        units: field.units.to_string(),
        all: stats_for_values(field.values.iter().copied()),
        by_mask: masks
            .iter()
            .map(|mask| MaskedFieldStats {
                mask: mask.name.to_string(),
                stats: stats_for_values(
                    field
                        .values
                        .iter()
                        .zip(mask.values.iter())
                        .filter_map(|(&value, &included)| included.then_some(value)),
                ),
            })
            .collect(),
    }
}

fn stats_for_values(values: impl Iterator<Item = f64>) -> BasicStats {
    let mut finite = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    finite.sort_by(|a, b| a.total_cmp(b));
    if finite.is_empty() {
        return BasicStats {
            count: 0,
            min: None,
            p10: None,
            p25: None,
            median: None,
            mean: None,
            p75: None,
            p90: None,
            p95: None,
            p99: None,
            max: None,
        };
    }
    let sum = finite.iter().sum::<f64>();
    BasicStats {
        count: finite.len(),
        min: finite.first().copied(),
        p10: percentile(&finite, 0.10),
        p25: percentile(&finite, 0.25),
        median: percentile(&finite, 0.50),
        mean: Some(sum / finite.len() as f64),
        p75: percentile(&finite, 0.75),
        p90: percentile(&finite, 0.90),
        p95: percentile(&finite, 0.95),
        p99: percentile(&finite, 0.99),
        max: finite.last().copied(),
    }
}

fn percentile(sorted: &[f64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let pos = (sorted.len() - 1) as f64 * q.clamp(0.0, 1.0);
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        Some(sorted[lo])
    } else {
        let frac = pos - lo as f64;
        Some(sorted[lo] * (1.0 - frac) + sorted[hi] * frac)
    }
}

fn ratio(numerator: &[f64], denominator: &[f64], min_denominator: f64) -> Vec<f64> {
    numerator
        .iter()
        .zip(denominator.iter())
        .map(|(&n, &d)| {
            if n.is_finite() && d.is_finite() && d >= min_denominator {
                n / d
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn diff(left: &[f64], right: &[f64]) -> Vec<f64> {
    left.iter()
        .zip(right.iter())
        .map(|(&l, &r)| {
            if l.is_finite() && r.is_finite() {
                l - r
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn finite_min(values: &[f64]) -> Option<f64> {
    values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .min_by(|a, b| a.total_cmp(b))
}

fn finite_max(values: &[f64]) -> Option<f64> {
    values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .max_by(|a, b| a.total_cmp(b))
}
