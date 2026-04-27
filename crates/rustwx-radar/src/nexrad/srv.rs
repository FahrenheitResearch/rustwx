use crate::nexrad::level2::{MomentData, RadialData};
use crate::nexrad::{Level2Sweep, RadarProduct};

pub struct SRVComputer;

impl SRVComputer {
    /// Compute Storm Relative Velocity
    /// storm_dir_deg: direction storm is moving FROM (meteorological convention)
    /// storm_speed_kts: storm motion speed in knots
    pub fn compute(
        velocity_sweep: &Level2Sweep,
        storm_dir_deg: f32,
        storm_speed_kts: f32,
    ) -> Level2Sweep {
        let storm_speed_ms = storm_speed_kts * 0.51444;
        let storm_dir_rad = storm_dir_deg.to_radians();

        // Storm motion components (meteorological "from" convention):
        // u = east-west component, v = north-south component
        let storm_u = -storm_speed_ms * storm_dir_rad.sin();
        let storm_v = -storm_speed_ms * storm_dir_rad.cos();

        let radials = velocity_sweep
            .radials
            .iter()
            .map(|radial| {
                let az_rad = radial.azimuth.to_radians();

                // Project storm motion onto this radial direction
                // Positive radial = away from radar
                let storm_component = storm_u * az_rad.sin() + storm_v * az_rad.cos();

                let moments = radial
                    .moments
                    .iter()
                    .map(|moment| {
                        if moment.product == RadarProduct::Velocity {
                            let srv_data: Vec<f32> = moment
                                .data
                                .iter()
                                .map(|&vel| {
                                    if vel.is_nan() {
                                        f32::NAN
                                    } else {
                                        vel - storm_component
                                    }
                                })
                                .collect();

                            MomentData {
                                product: RadarProduct::StormRelativeVelocity,
                                gate_count: moment.gate_count,
                                first_gate_range: moment.first_gate_range,
                                gate_size: moment.gate_size,
                                data: srv_data,
                            }
                        } else {
                            moment.clone()
                        }
                    })
                    .collect();

                RadialData {
                    azimuth: radial.azimuth,
                    elevation: radial.elevation,
                    azimuth_spacing: radial.azimuth_spacing,
                    nyquist_velocity: radial.nyquist_velocity,
                    radial_status: radial.radial_status,
                    moments,
                }
            })
            .collect();

        Level2Sweep {
            elevation_number: velocity_sweep.elevation_number,
            elevation_angle: velocity_sweep.elevation_angle,
            nyquist_velocity: velocity_sweep.nyquist_velocity,
            radials,
        }
    }

    /// Estimate storm motion from velocity data across multiple elevation sweeps.
    /// Returns (direction_from_deg, speed_kts).
    ///
    /// Uses the Bunkers right-mover method when multiple elevations are available:
    /// 1. Compute mean wind at each elevation (range-weighted, filtering near-radar noise)
    /// 2. Compute 0-6km mean wind and 0-6km shear vector
    /// 3. Apply Bunkers deviation (7.5 m/s perpendicular right of shear)
    ///
    /// Falls back to range-weighted mean wind from the lowest sweep if only one
    /// elevation is available.
    pub fn estimate_storm_motion(velocity_sweeps: &[&Level2Sweep]) -> (f32, f32) {
        if velocity_sweeps.is_empty() {
            return (240.0, 30.0);
        }

        // Compute mean wind (u, v) in m/s for a single sweep.
        // NEXRAD velocity data is in m/s. The mean wind from radial velocity
        // represents the radial projection of the true wind; by summing the
        // u/v components across all azimuths, we recover the full wind vector.
        //
        // Filter gates < 20km (near-radar noise) and > 100km (too far for
        // reliable environmental sampling). Weight by range to give more
        // influence to gates that sample the broader environment.
        let mean_wind_for_sweep = |sweep: &Level2Sweep| -> Option<(f64, f64)> {
            let mut sum_u: f64 = 0.0;
            let mut sum_v: f64 = 0.0;
            let mut total_weight: f64 = 0.0;

            for radial in &sweep.radials {
                let az_rad = (radial.azimuth as f64).to_radians();
                for moment in &radial.moments {
                    if moment.product == RadarProduct::Velocity {
                        let first_range_m = moment.first_gate_range as f64;
                        let gate_size_m = moment.gate_size as f64;
                        for (gi, &vel) in moment.data.iter().enumerate() {
                            if vel.is_nan() {
                                continue;
                            }
                            let range_m = first_range_m + (gi as f64) * gate_size_m;
                            // Skip gates within 20km to avoid near-radar noise
                            if range_m < 20_000.0 {
                                continue;
                            }
                            // Skip gates beyond 100km to focus on the near environment
                            if range_m > 100_000.0 {
                                continue;
                            }
                            // Weight by range: gates farther out sample more of
                            // the environment and less of storm-scale circulation
                            let weight = range_m / 1000.0; // weight in km
                            let vel_f64 = vel as f64;
                            // Radial velocity is the projection of the wind onto
                            // the radial direction. Decompose back into u/v:
                            // v_radial = u*sin(az) + v*cos(az)
                            // With full 360 deg coverage, the sum recovers u and v.
                            sum_u += vel_f64 * az_rad.sin() * weight;
                            sum_v += vel_f64 * az_rad.cos() * weight;
                            total_weight += weight;
                        }
                    }
                }
            }

            if total_weight < 1.0 {
                return None;
            }

            Some((sum_u / total_weight, sum_v / total_weight))
        };

        // Sort sweeps by elevation angle
        let mut sorted_sweeps: Vec<&Level2Sweep> = velocity_sweeps.to_vec();
        sorted_sweeps.sort_by(|a, b| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Collect (elevation_angle_deg, u, v) for each sweep that has data.
        // All values are in m/s.
        let mut level_winds: Vec<(f64, f64, f64)> = Vec::new();
        for sweep in &sorted_sweeps {
            if let Some((u, v)) = mean_wind_for_sweep(sweep) {
                level_winds.push((sweep.elevation_angle as f64, u, v));
            }
        }

        if level_winds.is_empty() {
            return (240.0, 30.0);
        }

        // If we have multiple elevation levels, use Bunkers right-mover method
        if level_winds.len() >= 3 {
            // Use all available levels as a proxy for the wind profile.
            // Low-level mean: lowest third of available levels
            // Mid-to-upper: upper two-thirds
            let n = level_winds.len();
            let low_count = (n / 3).max(1);

            // Mean wind across all levels (proxy for 0-6 km mean wind)
            let mean_u_all: f64 = level_winds.iter().map(|(_, u, _)| u).sum::<f64>() / n as f64;
            let mean_v_all: f64 = level_winds.iter().map(|(_, _, v)| v).sum::<f64>() / n as f64;

            // Shear vector: difference between upper and lower level winds
            let low_u: f64 = level_winds[..low_count]
                .iter()
                .map(|(_, u, _)| u)
                .sum::<f64>()
                / low_count as f64;
            let low_v: f64 = level_winds[..low_count]
                .iter()
                .map(|(_, _, v)| v)
                .sum::<f64>()
                / low_count as f64;
            let high_u: f64 = level_winds[n - low_count..]
                .iter()
                .map(|(_, u, _)| u)
                .sum::<f64>()
                / low_count as f64;
            let high_v: f64 = level_winds[n - low_count..]
                .iter()
                .map(|(_, _, v)| v)
                .sum::<f64>()
                / low_count as f64;

            let shear_u = high_u - low_u;
            let shear_v = high_v - low_v;
            let shear_mag = (shear_u * shear_u + shear_v * shear_v).sqrt();

            if shear_mag > 0.5 {
                // Bunkers deviation: 7.5 m/s perpendicular to the right of the shear vector
                // Right-perpendicular of (shear_u, shear_v) is (shear_v, -shear_u)
                let dev_magnitude = 7.5; // m/s
                let rm_u = mean_u_all + dev_magnitude * (shear_v / shear_mag);
                let rm_v = mean_v_all + dev_magnitude * (-shear_u / shear_mag);

                // Speed in m/s, then convert to knots for the return value.
                let speed_ms = (rm_u * rm_u + rm_v * rm_v).sqrt();
                let speed_kts = speed_ms / 0.51444;

                // Direction: storm is MOVING TOWARD atan2(u,v), convert to FROM.
                let dir_rad = rm_u.atan2(rm_v);
                let dir_from = (dir_rad.to_degrees() + 180.0).rem_euclid(360.0);

                return (dir_from as f32, speed_kts as f32);
            }
        }

        // Fallback: use range-weighted mean wind from lowest sweep.
        // The mean wind values are in m/s; convert speed to knots.
        let (_, mean_u, mean_v) = level_winds[0];

        let speed_ms = (mean_u * mean_u + mean_v * mean_v).sqrt();
        let speed_kts = speed_ms / 0.51444;
        let dir_rad = mean_u.atan2(mean_v);
        let dir_from = (dir_rad.to_degrees() + 180.0).rem_euclid(360.0);

        (dir_from as f32, speed_kts as f32)
    }
}
