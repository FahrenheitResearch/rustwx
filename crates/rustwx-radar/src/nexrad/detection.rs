use super::{Level2File, Level2Sweep, RadarProduct, RadarSite};

/// Detected mesocyclone signature from velocity data.
pub struct MesocycloneDetection {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub max_shear: f32,   // s^-1
    pub max_delta_v: f32, // m/s
    pub strength: RotationStrength,
    pub base_height_km: f32,
    pub diameter_km: f32,
    pub rotation_sense: RotationSense,
}

/// Detected Tornadic Vortex Signature (gate-to-gate shear).
#[derive(Clone)]
pub struct TVSDetection {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub max_delta_v: f32,        // m/s
    pub gate_to_gate_shear: f32, // s^-1
    pub elevation_angle: f32,
}

/// Detected hail indicator from reflectivity and dual-pol data.
pub struct HailDetection {
    pub lat: f64,
    pub lon: f64,
    pub azimuth_deg: f32,
    pub range_km: f32,
    pub max_reflectivity_dbz: f32,
    pub height_km: f32,
    pub indicator: HailIndicator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HailIndicator {
    /// Reflectivity >= 50 dBZ at significant height
    HighReflectivity,
    /// Three-Body Scatter Spike detected downrange of a high-Z core
    TBSS,
}

impl std::fmt::Display for HailIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HailIndicator::HighReflectivity => write!(f, "High-Z Hail"),
            HailIndicator::TBSS => write!(f, "TBSS"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationSense {
    Cyclonic,
    Anticyclonic,
}

impl std::fmt::Display for RotationSense {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RotationSense::Cyclonic => write!(f, "Cyclonic"),
            RotationSense::Anticyclonic => write!(f, "Anticyclonic"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationStrength {
    Weak,
    Moderate,
    Strong,
}

impl RotationStrength {
    /// Classify rotation strength from azimuthal shear (s^-1).
    /// Thresholds based on NSSL mesocyclone detection algorithm:
    ///   Weak:     0.004 - 0.008 s^-1
    ///   Moderate: 0.008 - 0.012 s^-1
    ///   Strong:   >= 0.012 s^-1
    fn from_shear(shear: f32) -> Self {
        if shear >= 0.012 {
            RotationStrength::Strong
        } else if shear >= 0.008 {
            RotationStrength::Moderate
        } else {
            RotationStrength::Weak
        }
    }
}

impl std::fmt::Display for RotationStrength {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RotationStrength::Weak => write!(f, "Weak"),
            RotationStrength::Moderate => write!(f, "Moderate"),
            RotationStrength::Strong => write!(f, "Strong"),
        }
    }
}

// ---- Algorithm constants ----

/// Default Nyquist velocity estimate (m/s) when not available from data.
const DEFAULT_NYQUIST: f32 = 30.0;

/// Minimum azimuthal shear threshold for mesocyclone candidacy (s^-1).
/// GR2Analyst-comparable value: ~10 m/s over ~2 km => 0.005 s^-1.
/// We use 0.004 s^-1 as the floor, then require cluster coherence to filter noise.
const MESO_SHEAR_THRESHOLD: f32 = 0.004;

/// Minimum delta-V (m/s) for a gate pair to be a mesocyclone candidate.
/// This prevents extremely low-delta-V gates from clustering at close range
/// where small angular distances produce inflated shear values.
const MESO_MIN_DELTA_V: f32 = 10.0;

/// Minimum gate-to-gate delta-V for TVS detection at close range (m/s).
/// GR2Analyst uses ~30 m/s at close range. We use a base of 30 m/s and
/// apply range-dependent scaling.
const TVS_DELTA_V_BASE: f32 = 30.0;

/// TVS delta-V threshold at maximum detection range.
/// At longer range, beam broadening reduces observable shear, so we lower
/// the threshold to ~20 m/s at TVS_MAX_RANGE_KM.
const TVS_DELTA_V_FAR: f32 = 20.0;

/// Maximum range from radar for TVS detection (km).
const TVS_MAX_RANGE_KM: f32 = 100.0;

/// Maximum range from radar for mesocyclone detection (km).
const MESO_MAX_RANGE_KM: f32 = 150.0;

/// Maximum number of lowest unique elevation tilts to check for TVS.
const TVS_MAX_TILTS: usize = 3;

/// Minimum reflectivity (dBZ) at the candidate location to report a mesocyclone.
const MIN_REFLECTIVITY_DBZ: f32 = 20.0;

/// Number of lowest tilts to consider for vertical continuity check.
const VERTICAL_CONTINUITY_TILTS: usize = 4;

/// Maximum horizontal offset (km) for matching meso detections across tilts.
const VERTICAL_MATCH_DISTANCE_KM: f32 = 10.0;

/// Minimum number of tilts a mesocyclone must appear on to be reported.
const MIN_TILT_COUNT: usize = 2;

/// Minimum elevation angle separation (degrees) for SAILS tilt deduplication.
const SAILS_DEDUP_THRESHOLD: f32 = 0.3;

/// Minimum cluster size (gates) to form a mesocyclone candidate.
const MIN_CLUSTER_SIZE: usize = 4;

/// Minimum diameter for a reported mesocyclone (km).
/// Real mesocyclones are typically 2-10 km in diameter. Sub-1 km features
/// are more likely shear zones or noise.
const MIN_MESO_DIAMETER_KM: f32 = 1.0;

/// Maximum diameter for a reported mesocyclone (km).
/// Features larger than 20 km are almost certainly not mesocyclones.
const MAX_MESO_DIAMETER_KM: f32 = 20.0;

/// Reflectivity threshold (dBZ) for hail core identification.
const HAIL_REFLECTIVITY_THRESHOLD: f32 = 50.0;

/// Reflectivity threshold (dBZ) for severe hail / TBSS source.
const TBSS_SOURCE_THRESHOLD: f32 = 60.0;

/// TBSS flare echo: reflectivity drops to this range downrange of the core.
const TBSS_FLARE_MIN_DBZ: f32 = 10.0;
const TBSS_FLARE_MAX_DBZ: f32 = 30.0;

/// TBSS flare offset range: 10-30 km downrange of a 60+ dBZ core.
const TBSS_FLARE_MIN_OFFSET_KM: f32 = 8.0;
const TBSS_FLARE_MAX_OFFSET_KM: f32 = 35.0;

/// Minimum height (km AGL) for a hail reflectivity signature to be meaningful.
/// Ground-level 50 dBZ is heavy rain; 50 dBZ at height suggests hail.
const HAIL_MIN_HEIGHT_KM: f32 = 3.0;

/// Convert azimuth/range relative to a radar site into lat/lon.
fn azimuth_range_to_latlon(site: &RadarSite, azimuth_deg: f32, range_km: f32) -> (f64, f64) {
    let az_rad = (azimuth_deg as f64).to_radians();
    let lat = site.lat + (range_km as f64 * az_rad.cos()) / 111.139;
    let lon = site.lon + (range_km as f64 * az_rad.sin()) / (111.139 * site.lat.to_radians().cos());
    (lat, lon)
}

/// Dealias a velocity difference using the Nyquist interval.
/// Applies iterative correction until the value is within [-nyquist, nyquist].
fn dealias(delta_v: f32, nyquist: f32) -> f32 {
    let interval = 2.0 * nyquist;
    let mut dv = delta_v;
    while dv > nyquist {
        dv -= interval;
    }
    while dv < -nyquist {
        dv += interval;
    }
    dv
}

/// Get the effective Nyquist velocity for a sweep.
fn sweep_nyquist(sweep: &Level2Sweep) -> f32 {
    // Prefer the Nyquist parsed from the radial 'R' data block.
    if let Some(nv) = sweep.nyquist_velocity {
        if nv > 0.0 {
            return nv;
        }
    }
    // Fallback: check individual radials.
    for radial in &sweep.radials {
        if let Some(nv) = radial.nyquist_velocity {
            if nv > 0.0 {
                return nv;
            }
        }
    }
    DEFAULT_NYQUIST
}

/// Compute range-dependent TVS delta-V threshold.
/// At close range (0 km), uses TVS_DELTA_V_BASE (30 m/s).
/// At TVS_MAX_RANGE_KM, uses TVS_DELTA_V_FAR (20 m/s).
/// Linear interpolation between.
fn tvs_threshold_at_range(range_km: f32) -> f32 {
    let t = (range_km / TVS_MAX_RANGE_KM).clamp(0.0, 1.0);
    TVS_DELTA_V_BASE + t * (TVS_DELTA_V_FAR - TVS_DELTA_V_BASE)
}

/// A flagged rotation candidate gate before grouping.
struct RotationCandidate {
    azimuth_idx: usize, // index into sorted radials array
    gate_idx: usize,    // gate index
    azimuth_deg: f32,
    range_km: f32,
    shear: f32,   // absolute shear value (s^-1)
    delta_v: f32, // signed delta-V (positive = cyclonic in NH)
}

/// A single-tilt mesocyclone detection before vertical continuity filtering.
struct SingleTiltMeso {
    lat: f64,
    lon: f64,
    azimuth_deg: f32,
    range_km: f32,
    max_shear: f32,
    max_delta_v: f32,
    elevation_angle: f32,
    diameter_km: f32,
    tilt_index: usize,
    rotation_sense: RotationSense,
}

/// Effective earth radius factor for beam height (4/3 model).
const RE_PRIME: f64 = 8495.0;

/// Compute beam height above radar level in km.
fn beam_height_km(range_km: f64, elevation_rad: f64) -> f64 {
    let r = range_km;
    (r * r + RE_PRIME * RE_PRIME + 2.0 * r * RE_PRIME * elevation_rad.sin()).sqrt() - RE_PRIME
}

/// Automated rotation detection from NEXRAD Level 2 velocity data.
pub struct RotationDetector;

impl RotationDetector {
    /// Detect mesocyclones, TVS, and hail from Level 2 data.
    ///
    /// Returns `(mesocyclones, tvs_detections, hail_detections)`.
    pub fn detect(
        file: &Level2File,
        site: &RadarSite,
    ) -> (
        Vec<MesocycloneDetection>,
        Vec<TVSDetection>,
        Vec<HailDetection>,
    ) {
        let velocity_sweeps: Vec<&Level2Sweep> = file
            .sweeps
            .iter()
            .filter(|s| {
                s.radials.iter().any(|r| {
                    r.moments
                        .iter()
                        .any(|m| m.product == RadarProduct::Velocity)
                })
            })
            .collect();

        // Also collect reflectivity sweeps for cross-checking.
        let reflectivity_sweeps: Vec<&Level2Sweep> = file
            .sweeps
            .iter()
            .filter(|s| {
                s.radials.iter().any(|r| {
                    r.moments
                        .iter()
                        .any(|m| m.product == RadarProduct::Reflectivity)
                })
            })
            .collect();

        let mut tvs_detections: Vec<TVSDetection> = Vec::new();
        let mut single_tilt_mesos: Vec<SingleTiltMeso> = Vec::new();

        // Sort all velocity sweeps by elevation angle ascending.
        let mut sorted_sweeps = velocity_sweeps.clone();
        sorted_sweeps.sort_by(|a, b| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Single pass: TVS scans ALL sweeps (including SAILS repeats) within
        // the lowest unique elevations; meso uses only one sweep per unique
        // elevation for vertical continuity.
        let mut unique_elev_count: usize = 0;
        let mut last_unique_elev: Option<f32> = None;
        let mut deduped_tilt_idx: usize = 0;
        let mut deduped_sweeps: Vec<&Level2Sweep> = Vec::new();

        for sweep in sorted_sweeps.iter() {
            let nyquist = sweep_nyquist(sweep);

            let is_new_unique = last_unique_elev.map_or(true, |prev| {
                (sweep.elevation_angle - prev).abs() >= SAILS_DEDUP_THRESHOLD
            });
            if is_new_unique {
                unique_elev_count += 1;
                last_unique_elev = Some(sweep.elevation_angle);
            }

            // TVS: eligible on all sweeps within lowest TVS_MAX_TILTS unique elevations
            let is_tvs_eligible = unique_elev_count <= TVS_MAX_TILTS;
            let (candidates, tvs) = Self::scan_sweep(sweep, site, is_tvs_eligible, nyquist);
            tvs_detections.extend(tvs);

            // Meso: process only one sweep per unique elevation (skip SAILS repeats)
            if is_new_unique {
                let grouped = Self::group_candidates(
                    &candidates,
                    site,
                    sweep.elevation_angle,
                    deduped_tilt_idx,
                );
                single_tilt_mesos.extend(grouped);
                deduped_sweeps.push(sweep);
                deduped_tilt_idx += 1;
            }
        }

        // --- TVS clustering ---
        tvs_detections =
            Self::cluster_tvs(tvs_detections, &reflectivity_sweeps, &deduped_sweeps, site);

        // --- Reflectivity filter ---
        let single_tilt_mesos =
            Self::filter_by_reflectivity(single_tilt_mesos, &reflectivity_sweeps, &deduped_sweeps);

        // --- Vertical continuity filter ---
        let meso_detections = Self::apply_vertical_continuity(single_tilt_mesos, site);

        // --- Hail detection ---
        let hail_detections = Self::detect_hail(&reflectivity_sweeps, site);

        (meso_detections, tvs_detections, hail_detections)
    }

    /// Scan a single sweep for rotation candidates and TVS signatures.
    ///
    /// Uses both azimuthal shear (adjacent radials, same gate) and radial
    /// gate-to-gate shear (same radial, adjacent gates) to detect rotation.
    /// GR2Analyst primarily uses gate-to-gate on adjacent azimuths, but
    /// radial gate-to-gate is important for TVS detection.
    fn scan_sweep(
        sweep: &Level2Sweep,
        site: &RadarSite,
        check_tvs: bool,
        nyquist: f32,
    ) -> (Vec<RotationCandidate>, Vec<TVSDetection>) {
        let mut candidates = Vec::new();
        let mut tvs_list = Vec::new();

        if sweep.radials.is_empty() {
            return (candidates, tvs_list);
        }

        // Sort radials by azimuth.
        let mut radials: Vec<&super::level2::RadialData> = sweep.radials.iter().collect();
        radials.sort_by(|a, b| {
            a.azimuth
                .partial_cmp(&b.azimuth)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // --- Azimuthal shear: adjacent radials, same gate ---
        for i in 0..radials.len() {
            let next_i = (i + 1) % radials.len();
            let rad_a = radials[i];
            let rad_b = radials[next_i];

            // Find velocity moment data for each radial.
            let vel_a = match rad_a
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Velocity)
            {
                Some(v) => v,
                None => continue,
            };
            let vel_b = match rad_b
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Velocity)
            {
                Some(v) => v,
                None => continue,
            };

            // Compute angular distance between the two radials.
            let mut delta_az = (rad_b.azimuth - rad_a.azimuth).abs();
            if delta_az > 180.0 {
                delta_az = 360.0 - delta_az;
            }
            // Skip if azimuth gap is too large (missing radials)
            if delta_az > 3.0 {
                continue;
            }
            let delta_az_rad = (delta_az as f64).to_radians();

            // Use the smaller gate count so we stay in bounds.
            let gate_count = vel_a.data.len().min(vel_b.data.len());
            let gate_size_km = vel_a.gate_size as f32 / 1000.0;
            let first_gate_km = vel_a.first_gate_range as f32 / 1000.0;

            let mid_azimuth = if (rad_b.azimuth - rad_a.azimuth).abs() > 180.0 {
                // Wrap around 0/360.
                let sum = rad_a.azimuth + rad_b.azimuth + 360.0;
                (sum / 2.0) % 360.0
            } else {
                (rad_a.azimuth + rad_b.azimuth) / 2.0
            };

            for g in 0..gate_count {
                let va = vel_a.data[g];
                let vb = vel_b.data[g];

                // Skip missing/range-folded gates.
                if va.is_nan() || vb.is_nan() {
                    continue;
                }

                // Skip near-zero velocities (likely ground clutter / clear air).
                if va.abs() < 1.0 && vb.abs() < 1.0 {
                    continue;
                }

                let range_km = first_gate_km + g as f32 * gate_size_km;
                if range_km < 2.0 {
                    continue; // skip unreasonably close gates
                }

                // --- Range filter for mesocyclone candidates ---
                if range_km > MESO_MAX_RANGE_KM {
                    continue;
                }

                let raw_delta = vb - va;
                let delta_v = dealias(raw_delta, nyquist);

                // Minimum delta-V filter: prevent noise at close range
                if delta_v.abs() < MESO_MIN_DELTA_V {
                    continue;
                }

                // Azimuthal shear: delta_v / angular_distance_km.
                let angular_distance_km = range_km * delta_az_rad as f32;
                if angular_distance_km < 0.01 {
                    continue;
                }
                let shear = delta_v.abs() / angular_distance_km;

                // Mesocyclone candidate check.
                if shear >= MESO_SHEAR_THRESHOLD {
                    candidates.push(RotationCandidate {
                        azimuth_idx: i,
                        gate_idx: g,
                        azimuth_deg: mid_azimuth,
                        range_km,
                        shear,
                        delta_v, // preserve sign for rotation sense
                    });
                }

                // TVS check: azimuthal gate-to-gate on adjacent radials.
                if check_tvs && range_km <= TVS_MAX_RANGE_KM {
                    let threshold = tvs_threshold_at_range(range_km);
                    if delta_v.abs() >= threshold {
                        let (lat, lon) = azimuth_range_to_latlon(site, mid_azimuth, range_km);
                        tvs_list.push(TVSDetection {
                            lat,
                            lon,
                            azimuth_deg: mid_azimuth,
                            range_km,
                            max_delta_v: delta_v.abs(),
                            gate_to_gate_shear: shear,
                            elevation_angle: sweep.elevation_angle,
                        });
                    }
                }
            }
        }

        // --- Radial gate-to-gate shear: same radial, adjacent gates ---
        // This catches TVS signatures where the inbound/outbound couplet
        // is along the radial direction rather than across azimuths.
        if check_tvs {
            for radial in &radials {
                let vel_moment = match radial
                    .moments
                    .iter()
                    .find(|m| m.product == RadarProduct::Velocity)
                {
                    Some(v) => v,
                    None => continue,
                };

                let gate_size_km = vel_moment.gate_size as f32 / 1000.0;
                let first_gate_km = vel_moment.first_gate_range as f32 / 1000.0;

                // Check pairs of gates 1-3 gates apart (covers 0.25 to 0.75 km at
                // standard 250m gate spacing, appropriate for TVS scale).
                for gap in 1..=3usize {
                    if vel_moment.data.len() < gap + 1 {
                        continue;
                    }
                    for g in 0..(vel_moment.data.len() - gap) {
                        let v_near = vel_moment.data[g];
                        let v_far = vel_moment.data[g + gap];

                        if v_near.is_nan() || v_far.is_nan() {
                            continue;
                        }

                        let range_km = first_gate_km + (g as f32 + gap as f32 / 2.0) * gate_size_km;
                        if range_km < 2.0 || range_km > TVS_MAX_RANGE_KM {
                            continue;
                        }

                        let raw_delta = v_far - v_near;
                        let delta_v = dealias(raw_delta, nyquist);
                        let distance_km = gap as f32 * gate_size_km;

                        if distance_km < 0.01 {
                            continue;
                        }

                        let threshold = tvs_threshold_at_range(range_km);
                        if delta_v.abs() >= threshold {
                            let radial_shear = delta_v.abs() / distance_km;
                            let (lat, lon) =
                                azimuth_range_to_latlon(site, radial.azimuth, range_km);
                            tvs_list.push(TVSDetection {
                                lat,
                                lon,
                                azimuth_deg: radial.azimuth,
                                range_km,
                                max_delta_v: delta_v.abs(),
                                gate_to_gate_shear: radial_shear,
                                elevation_angle: sweep.elevation_angle,
                            });
                        }
                    }
                }
            }
        }

        (candidates, tvs_list)
    }

    /// Group adjacent flagged gates into contiguous mesocyclone regions using
    /// connected-components clustering.
    ///
    /// Two gates are considered connected if they are within 2 range gates and
    /// 2 azimuth bins of each other.
    fn group_candidates(
        candidates: &[RotationCandidate],
        site: &RadarSite,
        elevation_angle: f32,
        tilt_index: usize,
    ) -> Vec<SingleTiltMeso> {
        if candidates.is_empty() {
            return Vec::new();
        }

        // Build a spatial index: (azimuth_idx, gate_idx) -> candidate index.
        // Use connected-components via union-find.
        let n = candidates.len();
        let mut parent: Vec<usize> = (0..n).collect();
        let mut rank: Vec<usize> = vec![0; n];

        fn find(parent: &mut [usize], x: usize) -> usize {
            if parent[x] != x {
                parent[x] = find(parent, parent[x]);
            }
            parent[x]
        }

        fn union(parent: &mut [usize], rank: &mut [usize], a: usize, b: usize) {
            let ra = find(parent, a);
            let rb = find(parent, b);
            if ra == rb {
                return;
            }
            if rank[ra] < rank[rb] {
                parent[ra] = rb;
            } else if rank[ra] > rank[rb] {
                parent[rb] = ra;
            } else {
                parent[rb] = ra;
                rank[ra] += 1;
            }
        }

        // For efficient neighbor lookup, build a hashmap from (azimuth_idx, gate_idx).
        use std::collections::HashMap;
        let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
        for (ci, c) in candidates.iter().enumerate() {
            let key = (c.azimuth_idx as i32, c.gate_idx as i32);
            grid.entry(key).or_default().push(ci);
        }

        // For each candidate, check neighbors within 2 azimuth bins and 2 range gates.
        for (ci, c) in candidates.iter().enumerate() {
            let ai = c.azimuth_idx as i32;
            let gi = c.gate_idx as i32;
            for da in -2i32..=2 {
                for dg in -2i32..=2 {
                    if da == 0 && dg == 0 {
                        continue;
                    }
                    let key = (ai + da, gi + dg);
                    if let Some(neighbors) = grid.get(&key) {
                        for &ni in neighbors {
                            union(&mut parent, &mut rank, ci, ni);
                        }
                    }
                }
            }
        }

        // Collect clusters.
        let mut clusters: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = find(&mut parent, i);
            clusters.entry(root).or_default().push(i);
        }

        // Convert qualifying clusters into SingleTiltMeso detections.
        clusters
            .values()
            .filter(|group| group.len() >= MIN_CLUSTER_SIZE)
            .filter_map(|group| {
                let mut sum_az = 0.0_f64;
                let mut sum_range = 0.0_f64;
                let mut max_shear: f32 = 0.0;
                let mut max_dv: f32 = 0.0;
                let mut min_range: f32 = f32::MAX;
                let mut max_range: f32 = f32::MIN;
                let mut min_az: f32 = f32::MAX;
                let mut max_az: f32 = f32::MIN;

                // Count positive and negative delta-V to determine rotation sense.
                let mut cyclonic_count: usize = 0;
                let mut anticyclonic_count: usize = 0;

                for &idx in group {
                    let c = &candidates[idx];
                    sum_az += c.azimuth_deg as f64;
                    sum_range += c.range_km as f64;
                    if c.shear > max_shear {
                        max_shear = c.shear;
                    }
                    if c.delta_v.abs() > max_dv {
                        max_dv = c.delta_v.abs();
                    }
                    if c.range_km < min_range {
                        min_range = c.range_km;
                    }
                    if c.range_km > max_range {
                        max_range = c.range_km;
                    }
                    if c.azimuth_deg < min_az {
                        min_az = c.azimuth_deg;
                    }
                    if c.azimuth_deg > max_az {
                        max_az = c.azimuth_deg;
                    }

                    // In the Northern Hemisphere, cyclonic rotation (counterclockwise
                    // from above) produces positive azimuthal shear: radar sees
                    // inbound velocities on the right and outbound on the left
                    // (looking from the radar), which means vb > va when scanning
                    // clockwise in azimuth, hence positive delta_v.
                    if c.delta_v > 0.0 {
                        cyclonic_count += 1;
                    } else {
                        anticyclonic_count += 1;
                    }
                }

                let count = group.len() as f64;
                let center_az = (sum_az / count) as f32;
                let center_range = (sum_range / count) as f32;

                // Compute diameter from both range spread and azimuthal spread.
                let radial_extent = max_range - min_range;
                let az_spread = {
                    let d = max_az - min_az;
                    if d > 180.0 { 360.0 - d } else { d }
                };
                let azimuthal_extent = center_range * (az_spread as f64).to_radians() as f32;
                let diameter_km = radial_extent.max(azimuthal_extent);

                // Filter by diameter constraints.
                if diameter_km < MIN_MESO_DIAMETER_KM || diameter_km > MAX_MESO_DIAMETER_KM {
                    return None;
                }

                let (lat, lon) = azimuth_range_to_latlon(site, center_az, center_range);

                let rotation_sense = if cyclonic_count >= anticyclonic_count {
                    RotationSense::Cyclonic
                } else {
                    RotationSense::Anticyclonic
                };

                Some(SingleTiltMeso {
                    lat,
                    lon,
                    azimuth_deg: center_az,
                    range_km: center_range,
                    max_shear,
                    max_delta_v: max_dv,
                    elevation_angle,
                    diameter_km,
                    tilt_index,
                    rotation_sense,
                })
            })
            .collect()
    }

    /// Cluster raw TVS detections spatially, keep strongest per cluster,
    /// then require co-located reflectivity >= 30 dBZ.
    fn cluster_tvs(
        raw: Vec<TVSDetection>,
        reflectivity_sweeps: &[&Level2Sweep],
        _velocity_sweeps: &[&Level2Sweep],
        _site: &RadarSite,
    ) -> Vec<TVSDetection> {
        if raw.is_empty() {
            return Vec::new();
        }

        // Spatial clustering: merge TVS hits within 2° azimuth and 3km range
        // Uses spatial bucketing to avoid O(n²) all-pairs comparison.
        let n = raw.len();
        let mut parent: Vec<usize> = (0..n).collect();
        fn find(parent: &mut [usize], i: usize) -> usize {
            if parent[i] != i {
                parent[i] = find(parent, parent[i]);
            }
            parent[i]
        }

        const AZ_BUCKET_SIZE: f32 = 2.0;
        const RANGE_BUCKET_SIZE: f32 = 2.0;

        // Build spatial grid: bucket each detection by (azimuth_bucket, range_bucket)
        let mut buckets: std::collections::HashMap<(i32, i32), Vec<usize>> =
            std::collections::HashMap::new();
        for i in 0..n {
            let az_bucket = (raw[i].azimuth_deg / AZ_BUCKET_SIZE).floor() as i32;
            let range_bucket = (raw[i].range_km / RANGE_BUCKET_SIZE).floor() as i32;
            buckets
                .entry((az_bucket, range_bucket))
                .or_default()
                .push(i);
        }

        // Total number of azimuth buckets (for wrapping)
        let num_az_buckets = (360.0 / AZ_BUCKET_SIZE).ceil() as i32;

        // For each detection, only compare against detections in same + adjacent buckets
        for i in 0..n {
            let az_bucket = (raw[i].azimuth_deg / AZ_BUCKET_SIZE).floor() as i32;
            let range_bucket = (raw[i].range_km / RANGE_BUCKET_SIZE).floor() as i32;

            for daz in -1..=1_i32 {
                for dr in -1..=1_i32 {
                    let neighbor_az = (az_bucket + daz).rem_euclid(num_az_buckets);
                    let neighbor_range = range_bucket + dr;
                    if let Some(indices) = buckets.get(&(neighbor_az, neighbor_range)) {
                        for &j in indices {
                            if j <= i {
                                continue;
                            }
                            let az_diff = (raw[i].azimuth_deg - raw[j].azimuth_deg).abs();
                            let az_diff = if az_diff > 180.0 {
                                360.0 - az_diff
                            } else {
                                az_diff
                            };
                            let range_diff = (raw[i].range_km - raw[j].range_km).abs();
                            if az_diff < 2.0 && range_diff < 3.0 {
                                let ri = find(&mut parent, i);
                                let rj = find(&mut parent, j);
                                parent[ri] = rj;
                            }
                        }
                    }
                }
            }
        }

        // Keep strongest per cluster
        let mut clusters: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();
        for i in 0..n {
            clusters.entry(find(&mut parent, i)).or_default().push(i);
        }

        let mut clustered: Vec<TVSDetection> = Vec::new();
        for members in clusters.values() {
            if let Some(&best_idx) = members.iter().max_by(|&&a, &&b| {
                raw[a]
                    .max_delta_v
                    .partial_cmp(&raw[b].max_delta_v)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                clustered.push(raw[best_idx].clone());
            }
        }

        // Reflectivity filter: require >= 30 dBZ at TVS location.
        // This is critical for filtering out noise — a real TVS should always
        // be embedded in a thunderstorm.
        let min_ref_tvs = 30.0_f32;
        clustered.retain(|tvs| {
            // Find the reflectivity sweep closest in elevation to this TVS
            let ref_sweep = match reflectivity_sweeps.iter().min_by(|a, b| {
                let da = (a.elevation_angle - tvs.elevation_angle).abs();
                let db = (b.elevation_angle - tvs.elevation_angle).abs();
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            }) {
                Some(s) => s,
                None => return false,
            };
            // Find nearest radial
            let radial = ref_sweep.radials.iter().min_by(|a, b| {
                let da = {
                    let d = (a.azimuth - tvs.azimuth_deg).abs();
                    if d > 180.0 { 360.0 - d } else { d }
                };
                let db = {
                    let d = (b.azimuth - tvs.azimuth_deg).abs();
                    if d > 180.0 { 360.0 - d } else { d }
                };
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            });
            let radial = match radial {
                Some(r) => r,
                None => return false,
            };
            let ref_moment = match radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Reflectivity)
            {
                Some(m) => m,
                None => return false,
            };
            let gate_size_km = ref_moment.gate_size as f32 / 1000.0;
            let first_gate_km = ref_moment.first_gate_range as f32 / 1000.0;
            if gate_size_km <= 0.0 {
                return false;
            }
            let gate_idx = ((tvs.range_km - first_gate_km) / gate_size_km).round() as i32;
            if gate_idx < 0 || gate_idx as usize >= ref_moment.data.len() {
                return false;
            }
            let ref_val = ref_moment.data[gate_idx as usize];
            !ref_val.is_nan() && ref_val >= min_ref_tvs
        });

        clustered
    }

    /// Filter out mesocyclone candidates where the coincident reflectivity is
    /// below MIN_REFLECTIVITY_DBZ. Finds the reflectivity sweep closest in
    /// elevation to the velocity sweep and samples the reflectivity at the
    /// candidate's azimuth/range.
    fn filter_by_reflectivity(
        mesos: Vec<SingleTiltMeso>,
        reflectivity_sweeps: &[&Level2Sweep],
        _velocity_sweeps: &[&Level2Sweep],
    ) -> Vec<SingleTiltMeso> {
        if reflectivity_sweeps.is_empty() {
            // Can't filter without reflectivity data -- let them through.
            return mesos;
        }

        mesos
            .into_iter()
            .filter(|m| {
                // Find the closest reflectivity sweep by elevation angle.
                let ref_sweep = reflectivity_sweeps.iter().min_by(|a, b| {
                    let da = (a.elevation_angle - m.elevation_angle).abs();
                    let db = (b.elevation_angle - m.elevation_angle).abs();
                    da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                });
                let ref_sweep = match ref_sweep {
                    Some(s) => s,
                    None => return true, // no ref sweep, keep it
                };

                // Find the radial closest in azimuth.
                let radial = ref_sweep.radials.iter().min_by(|a, b| {
                    let da = azimuth_diff(a.azimuth, m.azimuth_deg);
                    let db = azimuth_diff(b.azimuth, m.azimuth_deg);
                    da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                });
                let radial = match radial {
                    Some(r) => r,
                    None => return true,
                };

                // Find the reflectivity moment.
                let ref_moment = match radial
                    .moments
                    .iter()
                    .find(|mo| mo.product == RadarProduct::Reflectivity)
                {
                    Some(mo) => mo,
                    None => return true,
                };

                // Compute gate index for the candidate's range.
                let gate_size_km = ref_moment.gate_size as f32 / 1000.0;
                let first_gate_km = ref_moment.first_gate_range as f32 / 1000.0;
                if gate_size_km <= 0.0 {
                    return true;
                }
                let gate_idx = ((m.range_km - first_gate_km) / gate_size_km).round() as i32;
                if gate_idx < 0 || gate_idx as usize >= ref_moment.data.len() {
                    return false; // out of range
                }
                let ref_val = ref_moment.data[gate_idx as usize];
                if ref_val.is_nan() {
                    return false; // no reflectivity = likely no precip
                }
                ref_val >= MIN_REFLECTIVITY_DBZ
            })
            .collect()
    }

    /// Apply vertical continuity: only keep mesocyclones detected on at least
    /// MIN_TILT_COUNT of the lowest VERTICAL_CONTINUITY_TILTS tilts.
    /// Matching is done by horizontal distance < VERTICAL_MATCH_DISTANCE_KM.
    fn apply_vertical_continuity(
        single_tilt_mesos: Vec<SingleTiltMeso>,
        _site: &RadarSite,
    ) -> Vec<MesocycloneDetection> {
        if single_tilt_mesos.is_empty() {
            return Vec::new();
        }

        // Only consider mesos from the lowest VERTICAL_CONTINUITY_TILTS tilts.
        let relevant: Vec<&SingleTiltMeso> = single_tilt_mesos
            .iter()
            .filter(|m| m.tilt_index < VERTICAL_CONTINUITY_TILTS)
            .collect();

        if relevant.is_empty() {
            return Vec::new();
        }

        // Greedy clustering across tilts: for each meso on tilt 0, find matches
        // on other tilts within VERTICAL_MATCH_DISTANCE_KM.
        let mut used: Vec<bool> = vec![false; relevant.len()];
        let mut results: Vec<MesocycloneDetection> = Vec::new();

        for (i, m) in relevant.iter().enumerate() {
            if used[i] {
                continue;
            }

            // Collect matching detections across tilts.
            let mut matched_indices = vec![i];
            let mut matched_tilts = std::collections::HashSet::new();
            matched_tilts.insert(m.tilt_index);

            for (j, other) in relevant.iter().enumerate() {
                if j == i || used[j] {
                    continue;
                }
                if matched_tilts.contains(&other.tilt_index) {
                    continue; // already have a match for this tilt
                }
                let dist = horizontal_distance_km(m.lat, m.lon, other.lat, other.lon);
                if dist < VERTICAL_MATCH_DISTANCE_KM {
                    matched_indices.push(j);
                    matched_tilts.insert(other.tilt_index);
                }
            }

            if matched_tilts.len() < MIN_TILT_COUNT {
                continue; // Fails vertical continuity
            }

            // Mark all matched as used.
            for &idx in &matched_indices {
                used[idx] = true;
            }

            // Build the final detection from the strongest match.
            let best = matched_indices
                .iter()
                .map(|&idx| &relevant[idx])
                .max_by(|a, b| {
                    a.max_shear
                        .partial_cmp(&b.max_shear)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap();

            // Use the lowest-tilt detection for position (most representative).
            let lowest = matched_indices
                .iter()
                .map(|&idx| &relevant[idx])
                .min_by_key(|m| m.tilt_index)
                .unwrap();

            // Estimate base height using standard beam propagation (4/3 earth model).
            let elev_rad = (lowest.elevation_angle as f64).to_radians();
            let base_height_km = beam_height_km(lowest.range_km as f64, elev_rad) as f32;

            results.push(MesocycloneDetection {
                lat: lowest.lat,
                lon: lowest.lon,
                azimuth_deg: lowest.azimuth_deg,
                range_km: lowest.range_km,
                max_shear: best.max_shear,
                max_delta_v: best.max_delta_v,
                strength: RotationStrength::from_shear(best.max_shear),
                base_height_km,
                diameter_km: best.diameter_km,
                rotation_sense: best.rotation_sense,
            });
        }

        results
    }

    /// Detect hail signatures from reflectivity data.
    ///
    /// Two detection methods:
    /// 1. High reflectivity at height: >= 50 dBZ at >= 3 km AGL suggests hail.
    /// 2. TBSS (Three-Body Scatter Spike): a "flare" echo of 10-30 dBZ
    ///    appearing 8-35 km downrange of a 60+ dBZ core on the lowest tilt.
    fn detect_hail(reflectivity_sweeps: &[&Level2Sweep], site: &RadarSite) -> Vec<HailDetection> {
        if reflectivity_sweeps.is_empty() {
            return Vec::new();
        }

        let mut detections = Vec::new();

        // Sort by elevation ascending.
        let mut sorted: Vec<&Level2Sweep> = reflectivity_sweeps.to_vec();
        sorted.sort_by(|a, b| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // --- Method 1: High reflectivity at height ---
        // Check upper tilts (skip the lowest tilt to focus on elevated returns).
        for sweep in sorted.iter().skip(1) {
            let elev_rad = (sweep.elevation_angle as f64).to_radians();

            for radial in &sweep.radials {
                let ref_moment = match radial
                    .moments
                    .iter()
                    .find(|m| m.product == RadarProduct::Reflectivity)
                {
                    Some(m) => m,
                    None => continue,
                };

                let gate_size_km = ref_moment.gate_size as f64 / 1000.0;
                let first_gate_km = ref_moment.first_gate_range as f64 / 1000.0;

                for (g, &dbz) in ref_moment.data.iter().enumerate() {
                    if dbz.is_nan() || dbz < HAIL_REFLECTIVITY_THRESHOLD {
                        continue;
                    }

                    let range_km = first_gate_km + g as f64 * gate_size_km;
                    if range_km < 5.0 || range_km > 200.0 {
                        continue;
                    }

                    let height_km = beam_height_km(range_km, elev_rad) as f32;
                    if height_km < HAIL_MIN_HEIGHT_KM {
                        continue;
                    }

                    let (lat, lon) = azimuth_range_to_latlon(site, radial.azimuth, range_km as f32);
                    detections.push(HailDetection {
                        lat,
                        lon,
                        azimuth_deg: radial.azimuth,
                        range_km: range_km as f32,
                        max_reflectivity_dbz: dbz,
                        height_km,
                        indicator: HailIndicator::HighReflectivity,
                    });
                }
            }
        }

        // Cluster high-Z hail detections: keep strongest per 5 km / 3 deg cluster
        detections = Self::cluster_hail(detections);

        // --- Method 2: TBSS detection on the lowest tilt ---
        if let Some(lowest) = sorted.first() {
            let tbss = Self::detect_tbss(lowest, site);
            detections.extend(tbss);
        }

        detections
    }

    /// Detect Three-Body Scatter Spike on a single (lowest) reflectivity sweep.
    ///
    /// A TBSS appears as a radially-elongated flare echo of 10-30 dBZ
    /// located 8-35 km downrange from a 60+ dBZ hail core.
    fn detect_tbss(sweep: &Level2Sweep, site: &RadarSite) -> Vec<HailDetection> {
        let mut detections = Vec::new();

        for radial in &sweep.radials {
            let ref_moment = match radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Reflectivity)
            {
                Some(m) => m,
                None => continue,
            };

            let gate_size_km = ref_moment.gate_size as f32 / 1000.0;
            let first_gate_km = ref_moment.first_gate_range as f32 / 1000.0;
            if gate_size_km <= 0.0 {
                continue;
            }

            // Find gates with reflectivity >= 60 dBZ (potential hail cores).
            for (g, &dbz) in ref_moment.data.iter().enumerate() {
                if dbz.is_nan() || dbz < TBSS_SOURCE_THRESHOLD {
                    continue;
                }

                let core_range_km = first_gate_km + g as f32 * gate_size_km;
                if core_range_km < 5.0 || core_range_km > 150.0 {
                    continue;
                }

                // Look for a flare echo 8-35 km downrange from this core.
                let min_flare_range = core_range_km + TBSS_FLARE_MIN_OFFSET_KM;
                let max_flare_range = core_range_km + TBSS_FLARE_MAX_OFFSET_KM;

                let min_flare_gate =
                    ((min_flare_range - first_gate_km) / gate_size_km).ceil() as usize;
                let max_flare_gate =
                    ((max_flare_range - first_gate_km) / gate_size_km).floor() as usize;

                let max_gate = ref_moment.data.len().min(max_flare_gate + 1);
                if min_flare_gate >= max_gate {
                    continue;
                }

                // Count consecutive gates in the flare range that match TBSS criteria.
                let mut flare_count = 0usize;
                let mut max_flare_range_km: f32 = 0.0;

                for fg in min_flare_gate..max_gate {
                    let fdbz = ref_moment.data[fg];
                    if !fdbz.is_nan() && fdbz >= TBSS_FLARE_MIN_DBZ && fdbz <= TBSS_FLARE_MAX_DBZ {
                        flare_count += 1;
                        let fr = first_gate_km + fg as f32 * gate_size_km;
                        if fr > max_flare_range_km {
                            max_flare_range_km = fr;
                        }
                    }
                }

                // Require at least 5 gates of flare echo (about 1.25 km at 250m spacing).
                if flare_count >= 5 {
                    // Also verify there is a gap or significant reflectivity drop
                    // between the core and the flare (not just continuous heavy rain).
                    let gap_start = g + 1;
                    let gap_end = min_flare_gate.min(ref_moment.data.len());
                    let mut has_gap = false;
                    if gap_start < gap_end {
                        // Check if there is at least one gate in the gap region
                        // with reflectivity < 40 dBZ (significant drop from 60+ core).
                        for gg in gap_start..gap_end {
                            let gdbz = ref_moment.data[gg];
                            if gdbz.is_nan() || gdbz < 40.0 {
                                has_gap = true;
                                break;
                            }
                        }
                    } else {
                        has_gap = true; // no gap region to check
                    }

                    if has_gap {
                        let flare_center_range = (min_flare_range + max_flare_range_km) / 2.0;
                        let (lat, lon) =
                            azimuth_range_to_latlon(site, radial.azimuth, flare_center_range);
                        detections.push(HailDetection {
                            lat,
                            lon,
                            azimuth_deg: radial.azimuth,
                            range_km: core_range_km,
                            max_reflectivity_dbz: dbz,
                            height_km: 0.0, // lowest tilt
                            indicator: HailIndicator::TBSS,
                        });
                    }
                }
            }
        }

        // Cluster TBSS detections (same core can trigger multiple adjacent gates).
        Self::cluster_hail(detections)
    }

    /// Cluster hail detections: keep the strongest per 5 deg / 5 km spatial cluster.
    fn cluster_hail(detections: Vec<HailDetection>) -> Vec<HailDetection> {
        if detections.len() <= 1 {
            return detections;
        }

        let n = detections.len();
        let mut parent: Vec<usize> = (0..n).collect();
        fn find(parent: &mut [usize], i: usize) -> usize {
            if parent[i] != i {
                parent[i] = find(parent, parent[i]);
            }
            parent[i]
        }

        for i in 0..n {
            for j in (i + 1)..n {
                let az_diff = azimuth_diff(detections[i].azimuth_deg, detections[j].azimuth_deg);
                let range_diff = (detections[i].range_km - detections[j].range_km).abs();
                if az_diff < 5.0 && range_diff < 5.0 {
                    let ri = find(&mut parent, i);
                    let rj = find(&mut parent, j);
                    parent[ri] = rj;
                }
            }
        }

        let mut clusters: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();
        for i in 0..n {
            clusters.entry(find(&mut parent, i)).or_default().push(i);
        }

        let mut result = Vec::new();
        for members in clusters.values() {
            if let Some(&best) = members.iter().max_by(|&&a, &&b| {
                detections[a]
                    .max_reflectivity_dbz
                    .partial_cmp(&detections[b].max_reflectivity_dbz)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                result.push(HailDetection {
                    lat: detections[best].lat,
                    lon: detections[best].lon,
                    azimuth_deg: detections[best].azimuth_deg,
                    range_km: detections[best].range_km,
                    max_reflectivity_dbz: detections[best].max_reflectivity_dbz,
                    height_km: detections[best].height_km,
                    indicator: detections[best].indicator,
                });
            }
        }

        result
    }
}

/// Compute the absolute azimuth difference in degrees, handling 0/360 wrap.
fn azimuth_diff(a: f32, b: f32) -> f32 {
    let d = (a - b).abs();
    if d > 180.0 { 360.0 - d } else { d }
}

/// Approximate horizontal distance (km) between two lat/lon points.
fn horizontal_distance_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f32 {
    let dlat = (lat2 - lat1) * 111.139;
    let dlon = (lon2 - lon1) * 111.139 * lat1.to_radians().cos();
    ((dlat * dlat + dlon * dlon).sqrt()) as f32
}
