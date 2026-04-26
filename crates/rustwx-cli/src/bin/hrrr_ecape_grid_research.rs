use clap::Parser;
use rustwx_calc::{
    compute_ecape_triplet_with_failure_mask_from_parts, compute_ehi,
    compute_wind_diagnostics_bundle, EcapeTripletOptions, EcapeVolumeInputs, SurfaceInputs,
    WindGridInputs,
};
use rustwx_core::{ModelId, SourceId};
use rustwx_products::cache::{default_proof_cache_dir, ensure_dir};
use rustwx_products::gridded::{
    load_model_timestep_from_parts_cropped, prepare_heavy_volume_timed,
};
use serde::Serialize;
use std::collections::VecDeque;
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
}

#[derive(Debug, Serialize)]
struct GridResearchReport {
    request: RequestSummary,
    grid: GridSummary,
    timing: TimingSummary,
    failure_count: usize,
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
        NamedField::new("sb_ecape_ehi_0_1km", "dimensionless", sb_ehi_01),
        NamedField::new("sb_ecape_ehi_0_3km", "dimensionless", sb_ehi_03),
        NamedField::new("ml_ecape_ehi_0_1km", "dimensionless", ml_ehi_01),
        NamedField::new("ml_ecape_ehi_0_3km", "dimensionless", ml_ehi_03.clone()),
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
        &ml_ratio,
        &ml_ehi_03,
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
            wind_ms,
            stats_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
        failure_count: triplet.total_failure_count(),
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
    ml_ratio: &[f64],
    ml_ehi_03: &[f64],
    srh_03: &[f64],
    shear_06: &[f64],
) -> Vec<NamedMask> {
    vec![
        mask_threshold(
            "ml_cape_ge_500",
            "ML undiluted CAPE >= 500 J/kg",
            ml_cape,
            500.0,
        ),
        mask_threshold(
            "ml_cape_ge_1000",
            "ML undiluted CAPE >= 1000 J/kg",
            ml_cape,
            1000.0,
        ),
        mask_threshold(
            "ml_cape_ge_2000",
            "ML undiluted CAPE >= 2000 J/kg",
            ml_cape,
            2000.0,
        ),
        mask_threshold("ml_ecape_ge_500", "ML ECAPE >= 500 J/kg", ml_ecape, 500.0),
        mask_threshold(
            "ml_ecape_ge_1000",
            "ML ECAPE >= 1000 J/kg",
            ml_ecape,
            1000.0,
        ),
        mask_threshold(
            "ml_ecape_ge_2000",
            "ML ECAPE >= 2000 J/kg",
            ml_ecape,
            2000.0,
        ),
        mask_threshold(
            "ml_ecape_ehi03_ge_1",
            "ML ECAPE-EHI 0-3 km >= 1",
            ml_ehi_03,
            1.0,
        ),
        mask_threshold(
            "ml_ecape_ehi03_ge_2",
            "ML ECAPE-EHI 0-3 km >= 2",
            ml_ehi_03,
            2.0,
        ),
        mask_threshold(
            "ml_ecape_ehi03_ge_3",
            "ML ECAPE-EHI 0-3 km >= 3",
            ml_ehi_03,
            3.0,
        ),
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
            name: "ratio_high_cape_plume",
            description: "ML ECAPE/CAPE ratio >= 1.0 where ML undiluted CAPE >= 500 J/kg",
            values: ml_ratio
                .iter()
                .zip(ml_cape.iter())
                .map(|(&ratio, &cape)| ratio.is_finite() && cape >= 500.0 && ratio >= 1.0)
                .collect(),
        },
    ]
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
