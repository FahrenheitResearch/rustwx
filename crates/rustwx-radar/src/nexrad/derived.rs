use crate::nexrad::RadarProduct;
use crate::nexrad::level2::{MomentData, RadialData};
use crate::nexrad::{Level2File, Level2Sweep};

/// Effective earth radius for beam height calculations (4/3 model), in km.
const RE_PRIME: f64 = 8495.0;

pub struct DerivedProducts;

impl DerivedProducts {
    /// Compute VIL (Vertically Integrated Liquid) by integrating reflectivity
    /// across all elevation tilts using the standard NWS algorithm.
    ///
    /// Returns a single sweep whose data values are VIL in kg/m^2.
    pub fn compute_vil(file: &Level2File) -> Level2Sweep {
        // Collect all sweeps that contain reflectivity data, sorted by elevation
        let mut ref_sweeps: Vec<&Level2Sweep> = file
            .sweeps
            .iter()
            .filter(|s| {
                s.radials
                    .first()
                    .map(|r| {
                        r.moments
                            .iter()
                            .any(|m| m.product == RadarProduct::Reflectivity)
                    })
                    .unwrap_or(false)
            })
            .collect();

        ref_sweeps.sort_by(|a, b| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Use the lowest sweep as the template
        let template = match ref_sweeps.first() {
            Some(t) => t,
            None => {
                return Level2Sweep {
                    elevation_number: 0,
                    elevation_angle: 0.0,
                    nyquist_velocity: None,
                    radials: Vec::new(),
                };
            }
        };
        let template_radials = &template.radials;

        let mut out_radials: Vec<RadialData> = Vec::with_capacity(template_radials.len());

        for radial in template_radials {
            let ref_moment = match radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Reflectivity)
            {
                Some(m) => m,
                None => continue,
            };

            let num_gates = ref_moment.gate_count as usize;
            let mut vil_data = vec![f32::NAN; num_gates];

            for gate_idx in 0..num_gates {
                let range_m = ref_moment.first_gate_range as f64
                    + gate_idx as f64 * ref_moment.gate_size as f64;
                let range_km = range_m / 1000.0;

                // Collect (elevation_angle_rad, dbz) pairs for this azimuth/range bin
                let mut tilt_values: Vec<(f64, f64)> = Vec::with_capacity(ref_sweeps.len());

                for sweep in &ref_sweeps {
                    let nearest = find_nearest_radial(sweep, radial.azimuth);
                    if let Some(nr) = nearest {
                        if let Some(ref_m) = nr
                            .moments
                            .iter()
                            .find(|m| m.product == RadarProduct::Reflectivity)
                        {
                            // Map gate index using this sweep's gate geometry
                            let gi = range_to_gate_index(
                                range_m,
                                ref_m.first_gate_range,
                                ref_m.gate_size,
                                ref_m.gate_count,
                            );
                            if let Some(&dbz) = gi.and_then(|i| ref_m.data.get(i)) {
                                if !dbz.is_nan() && dbz >= 0.0 {
                                    let elev_rad = (nr.elevation as f64).to_radians();
                                    tilt_values.push((elev_rad, dbz as f64));
                                }
                            }
                        }
                    }
                }

                if tilt_values.len() < 2 {
                    vil_data[gate_idx] = if tilt_values.is_empty() {
                        f32::NAN
                    } else {
                        0.0
                    };
                    continue;
                }

                let mut vil_total: f64 = 0.0;

                for i in 0..tilt_values.len() - 1 {
                    let (elev1, dbz1) = tilt_values[i];
                    let (elev2, dbz2) = tilt_values[i + 1];

                    let z1 = 10.0_f64.powf(dbz1 / 10.0);
                    let z2 = 10.0_f64.powf(dbz2 / 10.0);

                    let h1 = beam_height_km(range_km, elev1);
                    let h2 = beam_height_km(range_km, elev2);
                    let dh = h2 - h1;

                    if dh > 0.0 {
                        let z_avg = (z1 + z2) / 2.0;
                        let vil_layer = 3.44e-6 * z_avg.powf(4.0 / 7.0) * dh * 1000.0;
                        vil_total += vil_layer;
                    }
                }

                vil_data[gate_idx] = vil_total.min(80.0) as f32;
            }

            out_radials.push(RadialData {
                azimuth: radial.azimuth,
                elevation: radial.elevation,
                azimuth_spacing: radial.azimuth_spacing,
                nyquist_velocity: radial.nyquist_velocity,
                radial_status: radial.radial_status,
                moments: vec![MomentData {
                    product: RadarProduct::VIL,
                    gate_count: num_gates as u16,
                    first_gate_range: ref_moment.first_gate_range,
                    gate_size: ref_moment.gate_size,
                    data: vil_data,
                }],
            });
        }

        Level2Sweep {
            elevation_number: 0,
            elevation_angle: 0.0,
            nyquist_velocity: None,
            radials: out_radials,
        }
    }

    /// Compute Echo Tops -- the highest altitude (km AGL) where reflectivity
    /// meets or exceeds the given threshold (typically 18 dBZ).
    pub fn compute_echo_tops(file: &Level2File, threshold_dbz: f32) -> Level2Sweep {
        let mut ref_sweeps: Vec<&Level2Sweep> = file
            .sweeps
            .iter()
            .filter(|s| {
                s.radials
                    .first()
                    .map(|r| {
                        r.moments
                            .iter()
                            .any(|m| m.product == RadarProduct::Reflectivity)
                    })
                    .unwrap_or(false)
            })
            .collect();

        // Sort ascending so we can scan from highest down
        ref_sweeps.sort_by(|a, b| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let template = match ref_sweeps.first() {
            Some(t) => t,
            None => {
                return Level2Sweep {
                    elevation_number: 0,
                    elevation_angle: 0.0,
                    nyquist_velocity: None,
                    radials: Vec::new(),
                };
            }
        };
        let template_radials = &template.radials;

        let mut out_radials: Vec<RadialData> = Vec::with_capacity(template_radials.len());

        for radial in template_radials {
            let ref_moment = match radial
                .moments
                .iter()
                .find(|m| m.product == RadarProduct::Reflectivity)
            {
                Some(m) => m,
                None => continue,
            };

            let num_gates = ref_moment.gate_count as usize;
            let mut et_data = vec![f32::NAN; num_gates];

            for gate_idx in 0..num_gates {
                let range_m = ref_moment.first_gate_range as f64
                    + gate_idx as f64 * ref_moment.gate_size as f64;
                let range_km = range_m / 1000.0;

                // Scan from highest tilt down to find the first that meets threshold
                let mut echo_top: f32 = f32::NAN;

                for sweep in ref_sweeps.iter().rev() {
                    let nearest = find_nearest_radial(sweep, radial.azimuth);
                    if let Some(nr) = nearest {
                        if let Some(ref_m) = nr
                            .moments
                            .iter()
                            .find(|m| m.product == RadarProduct::Reflectivity)
                        {
                            let gi = range_to_gate_index(
                                range_m,
                                ref_m.first_gate_range,
                                ref_m.gate_size,
                                ref_m.gate_count,
                            );
                            if let Some(&dbz) = gi.and_then(|i| ref_m.data.get(i)) {
                                if !dbz.is_nan() && dbz >= threshold_dbz {
                                    let elev_rad = (nr.elevation as f64).to_radians();
                                    echo_top = beam_height_km(range_km, elev_rad) as f32;
                                    break;
                                }
                            }
                        }
                    }
                }

                et_data[gate_idx] = echo_top;
            }

            out_radials.push(RadialData {
                azimuth: radial.azimuth,
                elevation: radial.elevation,
                azimuth_spacing: radial.azimuth_spacing,
                nyquist_velocity: radial.nyquist_velocity,
                radial_status: radial.radial_status,
                moments: vec![MomentData {
                    product: RadarProduct::EchoTops,
                    gate_count: num_gates as u16,
                    first_gate_range: ref_moment.first_gate_range,
                    gate_size: ref_moment.gate_size,
                    data: et_data,
                }],
            });
        }

        Level2Sweep {
            elevation_number: 0,
            elevation_angle: 0.0,
            nyquist_velocity: None,
            radials: out_radials,
        }
    }
}

/// Compute beam height above radar in km using the 4/3 earth radius model.
fn beam_height_km(range_km: f64, elevation_rad: f64) -> f64 {
    let r = range_km;
    (r * r + RE_PRIME * RE_PRIME + 2.0 * r * RE_PRIME * elevation_rad.sin()).sqrt() - RE_PRIME
}

/// Find the radial in `sweep` closest in azimuth to `target_az`.
fn find_nearest_radial<'a>(sweep: &'a Level2Sweep, target_az: f32) -> Option<&'a RadialData> {
    if sweep.radials.is_empty() {
        return None;
    }

    let mut best: Option<&RadialData> = None;
    let mut best_diff = f32::MAX;

    for r in &sweep.radials {
        let mut diff = (r.azimuth - target_az).abs();
        if diff > 180.0 {
            diff = 360.0 - diff;
        }
        if diff < best_diff {
            best_diff = diff;
            best = Some(r);
        }
    }

    best
}

/// Convert a range in meters to a gate index for a given moment geometry.
/// Returns None if the range falls outside the data.
fn range_to_gate_index(
    range_m: f64,
    first_gate_range: u16,
    gate_size: u16,
    gate_count: u16,
) -> Option<usize> {
    if gate_size == 0 {
        return None;
    }
    let offset = range_m - first_gate_range as f64;
    if offset < 0.0 {
        return None;
    }
    let idx = (offset / gate_size as f64).round() as usize;
    if idx >= gate_count as usize {
        return None;
    }
    Some(idx)
}
