use serde::{Deserialize, Serialize};

use crate::cells::{StormCell, identify_cells};
use crate::dealias::{DealiasMethod, dealias_velocity_file};
use crate::nexrad::detection::{HailDetection, MesocycloneDetection, TVSDetection};
use crate::nexrad::level2::{MomentData, RadialData};
use crate::nexrad::{Level2File, Level2Sweep, RadarProduct, RadarSite};
use crate::png::{lowest_sweep_with_product, sweep_contains_product};

#[derive(Debug, Clone, Copy)]
pub struct AiExportOptions {
    pub include_tensor: bool,
    pub tensor_product: RadarProduct,
    pub max_tensor_gates: usize,
}

impl Default for AiExportOptions {
    fn default() -> Self {
        Self {
            include_tensor: false,
            tensor_product: RadarProduct::Reflectivity,
            max_tensor_gates: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarAiFrame {
    pub schema_version: &'static str,
    pub station: RadarStationExport,
    pub scan_time_utc: String,
    pub volume: VolumeExport,
    pub available_products: Vec<RadarProduct>,
    pub storm_cells: Vec<StormCellExport>,
    pub mesocyclones: Vec<MesocycloneExport>,
    pub tvs: Vec<TvsExport>,
    pub hail: Vec<HailExport>,
    pub tds_candidates: Vec<TdsCandidateExport>,
    pub tensor: Option<PolarTensorExport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarStationExport {
    pub id: String,
    pub name: String,
    pub state: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeExport {
    pub station_id: String,
    pub sweep_count: usize,
    pub partial: bool,
    pub vcp: Option<u16>,
    pub sweeps: Vec<SweepExport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepExport {
    pub sweep_index: usize,
    pub elevation_deg: f32,
    pub radial_count: usize,
    pub products: Vec<RadarProduct>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StormCellExport {
    pub label: usize,
    pub lat: f64,
    pub lon: f64,
    pub centroid_azimuth_deg: f32,
    pub centroid_range_km: f32,
    pub max_reflectivity_dbz: f32,
    pub mean_reflectivity_dbz: f32,
    pub area_km2: f32,
    pub gate_count: usize,
    pub core_threshold_dbz: f32,
    pub azimuth_extent_deg: f32,
    pub range_extent_km: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MesocycloneExport {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub max_shear_s1: f32,
    pub max_delta_v_ms: f32,
    pub strength: String,
    pub base_height_km: f32,
    pub diameter_km: f32,
    pub rotation_sense: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TvsExport {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub max_delta_v_ms: f32,
    pub gate_to_gate_shear_s1: f32,
    pub elevation_deg: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HailExport {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub max_reflectivity_dbz: f32,
    pub height_km: f32,
    pub indicator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TdsCandidateExport {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub reflectivity_dbz: f32,
    pub differential_reflectivity_db: f32,
    pub correlation_coefficient: f32,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolarTensorExport {
    pub product: RadarProduct,
    pub sweep_index: usize,
    pub elevation_deg: f32,
    pub radial_count: usize,
    pub gate_count: usize,
    pub first_gate_km: f32,
    pub gate_spacing_km: f32,
    pub azimuth_deg: Vec<f32>,
    pub values_row_major_radial_gate: Vec<Option<f32>>,
}

pub fn build_ai_frame(
    file: &Level2File,
    site: &RadarSite,
    options: AiExportOptions,
) -> RadarAiFrame {
    let storm_cells = lowest_sweep_with_product(file, RadarProduct::Reflectivity)
        .map(|(_, sweep)| identify_cells(sweep, Some(site.lat), Some(site.lon)))
        .unwrap_or_default()
        .into_iter()
        .map(StormCellExport::from)
        .collect();

    let dealiased_file = file_has_velocity(file)
        .then(|| dealias_velocity_file(file, DealiasMethod::SweepContinuity));
    let detection_file = dealiased_file.as_ref().unwrap_or(file);
    let (mesos, tvs, hail) =
        crate::nexrad::detection::RotationDetector::detect(detection_file, site);
    let tensor_file = if options.tensor_product.base_product() == RadarProduct::Velocity {
        detection_file
    } else {
        file
    };
    let tensor = options
        .include_tensor
        .then(|| {
            build_polar_tensor(
                tensor_file,
                options.tensor_product,
                options.max_tensor_gates,
            )
        })
        .flatten();

    RadarAiFrame {
        schema_version: "rustwx.radar.ai.v1",
        station: RadarStationExport {
            id: site.id.to_string(),
            name: site.name.to_string(),
            state: site.state.to_string(),
            lat: site.lat,
            lon: site.lon,
        },
        scan_time_utc: file.timestamp_string(),
        volume: VolumeExport {
            station_id: file.station_id.clone(),
            sweep_count: file.sweeps.len(),
            partial: file.partial,
            vcp: file.vcp,
            sweeps: file
                .sweeps
                .iter()
                .enumerate()
                .map(|(sweep_index, sweep)| SweepExport {
                    sweep_index,
                    elevation_deg: sweep.elevation_angle,
                    radial_count: sweep.radials.len(),
                    products: products_in_sweep(sweep),
                })
                .collect(),
        },
        available_products: file.available_products(),
        storm_cells,
        mesocyclones: mesos.into_iter().map(MesocycloneExport::from).collect(),
        tvs: tvs.into_iter().map(TvsExport::from).collect(),
        hail: hail.into_iter().map(HailExport::from).collect(),
        tds_candidates: detect_tds_candidates(file, site),
        tensor,
    }
}

fn file_has_velocity(file: &Level2File) -> bool {
    file.sweeps
        .iter()
        .any(|sweep| sweep_contains_product(sweep, RadarProduct::Velocity))
}

fn products_in_sweep(sweep: &Level2Sweep) -> Vec<RadarProduct> {
    let mut products = Vec::new();
    for product in RadarProduct::all_products() {
        if sweep_contains_product(sweep, *product) {
            products.push(*product);
        }
    }
    products
}

fn build_polar_tensor(
    file: &Level2File,
    product: RadarProduct,
    max_gates: usize,
) -> Option<PolarTensorExport> {
    let (sweep_index, sweep) = lowest_sweep_with_product(file, product)?;
    let mut moments = Vec::<(&RadialData, &MomentData)>::new();
    for radial in &sweep.radials {
        if let Some(moment) = radial.moments.iter().find(|m| m.product == product) {
            moments.push((radial, moment));
        }
    }
    let (_, first_moment) = moments.first().copied()?;
    let gate_count = moments
        .iter()
        .map(|(_, moment)| moment.data.len())
        .max()
        .unwrap_or(0)
        .min(max_gates);
    if gate_count == 0 {
        return None;
    }

    let mut azimuth_deg = Vec::with_capacity(moments.len());
    let mut values = Vec::with_capacity(moments.len() * gate_count);
    for (radial, moment) in &moments {
        azimuth_deg.push(radial.azimuth);
        for gate in 0..gate_count {
            values.push(
                moment
                    .data
                    .get(gate)
                    .copied()
                    .filter(|value| value.is_finite()),
            );
        }
    }

    Some(PolarTensorExport {
        product,
        sweep_index,
        elevation_deg: sweep.elevation_angle,
        radial_count: moments.len(),
        gate_count,
        first_gate_km: first_moment.first_gate_range as f32 / 1000.0,
        gate_spacing_km: first_moment.gate_size as f32 / 1000.0,
        azimuth_deg,
        values_row_major_radial_gate: values,
    })
}

fn detect_tds_candidates(file: &Level2File, site: &RadarSite) -> Vec<TdsCandidateExport> {
    let mut candidates = Vec::new();
    for sweep in &file.sweeps {
        if !sweep_contains_product(sweep, RadarProduct::Reflectivity)
            || !sweep_contains_product(sweep, RadarProduct::DifferentialReflectivity)
            || !sweep_contains_product(sweep, RadarProduct::CorrelationCoefficient)
        {
            continue;
        }

        for radial in &sweep.radials {
            let Some(ref_m) = radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Reflectivity)
            else {
                continue;
            };
            let Some(zdr_m) = radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::DifferentialReflectivity)
            else {
                continue;
            };
            let Some(cc_m) = radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::CorrelationCoefficient)
            else {
                continue;
            };

            let n = ref_m.data.len().min(zdr_m.data.len()).min(cc_m.data.len());
            let gate_size_km = ref_m.gate_size as f32 / 1000.0;
            let first_gate_km = ref_m.first_gate_range as f32 / 1000.0;
            for gate in (0..n).step_by(2) {
                let range_km = first_gate_km + gate as f32 * gate_size_km;
                if !(15.0..=120.0).contains(&range_km) {
                    continue;
                }
                let ref_dbz = ref_m.data[gate];
                let zdr = zdr_m.data[gate];
                let cc = cc_m.data[gate];
                if !ref_dbz.is_finite() || !zdr.is_finite() || !cc.is_finite() {
                    continue;
                }
                if ref_dbz < 40.0 || zdr.abs() > 1.25 || !(0.55..=0.82).contains(&cc) {
                    continue;
                }

                let (lat, lon) = azimuth_range_to_latlon(site, radial.azimuth, range_km);
                let score =
                    (ref_dbz - 40.0).max(0.0) * 0.02 + (0.82 - cc).max(0.0) * 2.0 - zdr.abs() * 0.1;
                candidates.push(TdsCandidateExport {
                    lat,
                    lon,
                    azimuth_deg: radial.azimuth,
                    range_km,
                    reflectivity_dbz: ref_dbz,
                    differential_reflectivity_db: zdr,
                    correlation_coefficient: cc,
                    score,
                });
            }
        }
    }

    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    cluster_tds(candidates).into_iter().take(64).collect()
}

fn cluster_tds(candidates: Vec<TdsCandidateExport>) -> Vec<TdsCandidateExport> {
    let mut kept: Vec<TdsCandidateExport> = Vec::new();
    'candidate: for candidate in candidates {
        for existing in &kept {
            if azimuth_diff(candidate.azimuth_deg, existing.azimuth_deg) <= 3.0
                && (candidate.range_km - existing.range_km).abs() <= 3.0
            {
                continue 'candidate;
            }
        }
        kept.push(candidate);
    }
    kept
}

fn azimuth_range_to_latlon(site: &RadarSite, azimuth_deg: f32, range_km: f32) -> (f64, f64) {
    let az_rad = (azimuth_deg as f64).to_radians();
    let lat = site.lat + (range_km as f64 * az_rad.cos()) / 111.139;
    let lon = site.lon + (range_km as f64 * az_rad.sin()) / (111.139 * site.lat.to_radians().cos());
    (lat, lon)
}

fn azimuth_diff(a: f32, b: f32) -> f32 {
    let d = (a - b).abs();
    if d > 180.0 { 360.0 - d } else { d }
}

impl From<StormCell> for StormCellExport {
    fn from(cell: StormCell) -> Self {
        Self {
            label: cell.label,
            lat: cell.lat,
            lon: cell.lon,
            centroid_azimuth_deg: cell.centroid_azimuth,
            centroid_range_km: cell.centroid_range_km,
            max_reflectivity_dbz: cell.max_reflectivity,
            mean_reflectivity_dbz: cell.mean_reflectivity,
            area_km2: cell.area_km2,
            gate_count: cell.gate_count,
            core_threshold_dbz: cell.core_threshold,
            azimuth_extent_deg: cell.az_extent,
            range_extent_km: cell.range_extent_km,
        }
    }
}

impl From<MesocycloneDetection> for MesocycloneExport {
    fn from(meso: MesocycloneDetection) -> Self {
        Self {
            lat: meso.lat,
            lon: meso.lon,
            azimuth_deg: meso.azimuth_deg,
            range_km: meso.range_km,
            max_shear_s1: meso.max_shear,
            max_delta_v_ms: meso.max_delta_v,
            strength: meso.strength.to_string(),
            base_height_km: meso.base_height_km,
            diameter_km: meso.diameter_km,
            rotation_sense: meso.rotation_sense.to_string(),
        }
    }
}

impl From<TVSDetection> for TvsExport {
    fn from(tvs: TVSDetection) -> Self {
        Self {
            lat: tvs.lat,
            lon: tvs.lon,
            azimuth_deg: tvs.azimuth_deg,
            range_km: tvs.range_km,
            max_delta_v_ms: tvs.max_delta_v,
            gate_to_gate_shear_s1: tvs.gate_to_gate_shear,
            elevation_deg: tvs.elevation_angle,
        }
    }
}

impl From<HailDetection> for HailExport {
    fn from(hail: HailDetection) -> Self {
        Self {
            lat: hail.lat,
            lon: hail.lon,
            azimuth_deg: hail.azimuth_deg,
            range_km: hail.range_km,
            max_reflectivity_dbz: hail.max_reflectivity_dbz,
            height_km: hail.height_km,
            indicator: hail.indicator.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexrad::level2::{MomentData, RadialData};

    #[test]
    fn ai_frame_includes_storm_cell_and_tensor() {
        let file = Level2File {
            station_id: "KTLX".to_string(),
            volume_date: 20_000,
            volume_time: 0,
            vcp: Some(212),
            sweeps: vec![synthetic_reflectivity_sweep()],
            partial: false,
        };
        let site = RadarSite {
            id: "KTLX",
            name: "Oklahoma City",
            lat: 35.333,
            lon: -97.277,
            state: "OK",
        };
        let frame = build_ai_frame(
            &file,
            &site,
            AiExportOptions {
                include_tensor: true,
                max_tensor_gates: 80,
                ..AiExportOptions::default()
            },
        );
        assert_eq!(frame.schema_version, "rustwx.radar.ai.v1");
        assert!(!frame.storm_cells.is_empty());
        assert_eq!(frame.tensor.as_ref().unwrap().gate_count, 80);
    }

    fn synthetic_reflectivity_sweep() -> Level2Sweep {
        let mut radials = Vec::new();
        for az in 0..360 {
            let mut data = vec![f32::NAN; 180];
            for gate in 45..75 {
                let az_dist = ((az as f32 - 210.0 + 540.0) % 360.0 - 180.0).abs();
                if az_dist < 8.0 {
                    data[gate] = 60.0 - az_dist - (gate as f32 - 60.0).abs() * 0.3;
                }
            }
            radials.push(RadialData {
                azimuth: az as f32,
                elevation: 0.5,
                azimuth_spacing: 1.0,
                nyquist_velocity: None,
                radial_status: 1,
                moments: vec![MomentData {
                    product: RadarProduct::Reflectivity,
                    gate_count: data.len() as u16,
                    first_gate_range: 0,
                    gate_size: 1_000,
                    data,
                }],
            });
        }
        Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: None,
            radials,
        }
    }
}
