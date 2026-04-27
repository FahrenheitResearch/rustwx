use crate::nexrad::level2::RadialData;
use crate::nexrad::{Level2File, Level2Sweep, RadarProduct};

const DEFAULT_NYQUIST_MS: f32 = 30.0;
const MAX_ABS_FOLD: i32 = 8;

/// Velocity dealiasing strategy for NEXRAD velocity moments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DealiasMethod {
    Off,
    /// One-dimensional continuity along each radial.
    RadialContinuity,
    /// Radial continuity plus a sweep-neighborhood refinement pass.
    SweepContinuity,
}

impl Default for DealiasMethod {
    fn default() -> Self {
        Self::SweepContinuity
    }
}

/// Return a cloned Level 2 file with velocity moments dealiased.
pub fn dealias_velocity_file(file: &Level2File, method: DealiasMethod) -> Level2File {
    if method == DealiasMethod::Off {
        return file.clone();
    }

    let mut out = file.clone();
    out.sweeps = file
        .sweeps
        .iter()
        .map(|sweep| dealias_velocity_sweep(sweep, method))
        .collect();
    out
}

/// Return a cloned sweep with velocity moments dealiased.
///
/// The implementation is rustwx-owned, with the same practical shape as the
/// ES90 radial-continuity and ZW06 sweep-neighbor families used in
/// FahrenheitResearch/open-dealiasing-algorithms: unfold each observed gate to
/// the nearest plausible reference, then use neighboring gates/radials to
/// repair local fold inconsistencies.
pub fn dealias_velocity_sweep(sweep: &Level2Sweep, method: DealiasMethod) -> Level2Sweep {
    if method == DealiasMethod::Off || !contains_velocity(sweep) {
        return sweep.clone();
    }

    let nyquist = effective_nyquist(sweep);
    if nyquist <= 0.0 || !nyquist.is_finite() {
        return sweep.clone();
    }

    let Some((observed, radial_to_row)) = velocity_grid(sweep) else {
        return sweep.clone();
    };

    let mut corrected = radial_continuity_grid(&observed, nyquist);
    if method == DealiasMethod::SweepContinuity {
        sweep_refine_grid(&observed, &mut corrected, nyquist, 3);
    }

    let mut out = sweep.clone();
    for (radial_index, radial) in out.radials.iter_mut().enumerate() {
        let Some(row) = radial_to_row.get(radial_index).and_then(|row| *row) else {
            continue;
        };
        replace_velocity_moment(radial, &corrected[row]);
    }

    out
}

/// Effective Nyquist velocity for a sweep, in m/s.
pub fn effective_nyquist(sweep: &Level2Sweep) -> f32 {
    if let Some(nyquist) = sweep.nyquist_velocity {
        if nyquist.is_finite() && nyquist > 0.0 {
            return nyquist;
        }
    }

    sweep
        .radials
        .iter()
        .filter_map(|radial| radial.nyquist_velocity)
        .find(|nyquist| nyquist.is_finite() && *nyquist > 0.0)
        .unwrap_or(DEFAULT_NYQUIST_MS)
}

fn contains_velocity(sweep: &Level2Sweep) -> bool {
    sweep.radials.iter().any(|radial| {
        radial
            .moments
            .iter()
            .any(|moment| is_velocity_product(moment.product))
    })
}

fn is_velocity_product(product: RadarProduct) -> bool {
    matches!(
        product.base_product(),
        RadarProduct::Velocity | RadarProduct::SuperResVelocity
    )
}

fn velocity_grid(sweep: &Level2Sweep) -> Option<(Vec<Vec<f32>>, Vec<Option<usize>>)> {
    let mut radial_indices: Vec<(f32, usize)> = sweep
        .radials
        .iter()
        .enumerate()
        .filter(|(_, radial)| velocity_moment(radial).is_some())
        .map(|(index, radial)| (radial.azimuth, index))
        .collect();
    if radial_indices.is_empty() {
        return None;
    }
    radial_indices.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let max_gates = radial_indices
        .iter()
        .filter_map(|(_, index)| velocity_moment(&sweep.radials[*index]))
        .map(|moment| moment.data.len())
        .max()
        .unwrap_or(0);
    if max_gates == 0 {
        return None;
    }

    let mut grid = vec![vec![f32::NAN; max_gates]; radial_indices.len()];
    let mut radial_to_row = vec![None; sweep.radials.len()];

    for (row, (_, radial_index)) in radial_indices.iter().enumerate() {
        if let Some(moment) = velocity_moment(&sweep.radials[*radial_index]) {
            grid[row][..moment.data.len()].copy_from_slice(&moment.data);
            radial_to_row[*radial_index] = Some(row);
        }
    }

    Some((grid, radial_to_row))
}

fn velocity_moment(radial: &RadialData) -> Option<&crate::nexrad::level2::MomentData> {
    radial
        .moments
        .iter()
        .find(|moment| is_velocity_product(moment.product))
}

fn replace_velocity_moment(radial: &mut RadialData, corrected: &[f32]) {
    for moment in &mut radial.moments {
        if !is_velocity_product(moment.product) {
            continue;
        }
        for (gate, value) in moment.data.iter_mut().enumerate() {
            if let Some(dealiased) = corrected.get(gate).copied() {
                if dealiased.is_finite() {
                    *value = dealiased;
                }
            }
        }
    }
}

fn radial_continuity_grid(observed: &[Vec<f32>], nyquist: f32) -> Vec<Vec<f32>> {
    let rows = observed.len();
    let mut corrected = Vec::with_capacity(rows);
    let mut previous: Option<Vec<f32>> = None;

    for row in observed {
        let row_corrected = dealias_radial(row, nyquist, previous.as_deref());
        previous = Some(row_corrected.clone());
        corrected.push(row_corrected);
    }

    corrected
}

fn dealias_radial(observed: &[f32], nyquist: f32, reference: Option<&[f32]>) -> Vec<f32> {
    let mut corrected = vec![f32::NAN; observed.len()];
    let Some(seed) = pick_seed(observed, reference) else {
        return corrected;
    };

    let seed_observed = observed[seed];
    corrected[seed] = reference
        .and_then(|reference| reference.get(seed).copied())
        .filter(|value| value.is_finite())
        .map(|reference| unfold_to_reference(seed_observed, reference, nyquist))
        .unwrap_or(seed_observed);

    walk_radial(observed, &mut corrected, reference, seed, nyquist, 1);
    walk_radial(observed, &mut corrected, reference, seed, nyquist, -1);
    corrected
}

fn pick_seed(observed: &[f32], reference: Option<&[f32]>) -> Option<usize> {
    if let Some(reference) = reference {
        let overlap: Vec<usize> = observed
            .iter()
            .zip(reference.iter())
            .enumerate()
            .filter_map(|(index, (observed, reference))| {
                (observed.is_finite() && reference.is_finite()).then_some(index)
            })
            .collect();
        if !overlap.is_empty() {
            let center = overlap.iter().sum::<usize>() as f32 / overlap.len() as f32;
            return overlap.into_iter().min_by(|a, b| {
                ((*a as f32 - center).abs())
                    .partial_cmp(&(*b as f32 - center).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
    }

    observed.iter().position(|value| value.is_finite())
}

fn walk_radial(
    observed: &[f32],
    corrected: &mut [f32],
    reference: Option<&[f32]>,
    seed: usize,
    nyquist: f32,
    direction: isize,
) {
    let mut index = seed as isize + direction;
    let mut last_valid = Some(seed);
    let mut last_valid_two: Option<usize> = None;

    while (0..observed.len() as isize).contains(&index) {
        let gate = index as usize;
        let observed_value = observed[gate];
        if !observed_value.is_finite() {
            index += direction;
            continue;
        }

        let mut refs = Vec::with_capacity(3);
        if let Some(last) = last_valid {
            if corrected[last].is_finite() {
                refs.push(corrected[last]);
                if let Some(previous) = last_valid_two {
                    if corrected[previous].is_finite() {
                        let slope = corrected[last] - corrected[previous];
                        refs.push(corrected[last] + slope);
                    }
                }
            }
        }
        if let Some(reference) = reference {
            if let Some(reference_value) = reference.get(gate).copied() {
                if reference_value.is_finite() {
                    refs.push(reference_value);
                }
            }
        }

        let reference_value = median(&mut refs).unwrap_or(observed_value);
        corrected[gate] = unfold_to_reference(observed_value, reference_value, nyquist);
        last_valid_two = last_valid;
        last_valid = Some(gate);
        index += direction;
    }
}

fn sweep_refine_grid(
    observed: &[Vec<f32>],
    corrected: &mut [Vec<f32>],
    nyquist: f32,
    passes: usize,
) {
    if observed.is_empty() || observed[0].is_empty() {
        return;
    }

    let rows = observed.len();
    let cols = observed[0].len();
    for _ in 0..passes {
        let current = corrected.to_vec();
        let mut changed = 0usize;

        for row in 0..rows {
            let row_prev = if row == 0 { rows - 1 } else { row - 1 };
            let row_next = if row + 1 == rows { 0 } else { row + 1 };
            for col in 0..cols {
                let observed_value = observed[row][col];
                if !observed_value.is_finite() {
                    continue;
                }

                let mut refs = Vec::with_capacity(8);
                push_neighbor(&mut refs, &current[row_prev], col);
                push_neighbor(&mut refs, &current[row_next], col);
                if col > 0 {
                    push_neighbor(&mut refs, &current[row], col - 1);
                    push_neighbor(&mut refs, &current[row_prev], col - 1);
                    push_neighbor(&mut refs, &current[row_next], col - 1);
                }
                if col + 1 < cols {
                    push_neighbor(&mut refs, &current[row], col + 1);
                    push_neighbor(&mut refs, &current[row_prev], col + 1);
                    push_neighbor(&mut refs, &current[row_next], col + 1);
                }

                if refs.len() < 2 {
                    continue;
                }
                let Some(reference_value) = median(&mut refs) else {
                    continue;
                };

                let candidate = unfold_to_reference(observed_value, reference_value, nyquist);
                let current_value = current[row][col];
                if !current_value.is_finite()
                    || (candidate - reference_value).abs() + 0.15 * nyquist
                        < (current_value - reference_value).abs()
                {
                    corrected[row][col] = candidate;
                    changed += 1;
                }
            }
        }

        if changed == 0 {
            break;
        }
    }
}

fn push_neighbor(values: &mut Vec<f32>, row: &[f32], col: usize) {
    if let Some(value) = row.get(col).copied() {
        if value.is_finite() {
            values.push(value);
        }
    }
}

fn unfold_to_reference(observed: f32, reference: f32, nyquist: f32) -> f32 {
    let interval = 2.0 * nyquist;
    let fold = ((reference - observed) / interval)
        .round()
        .clamp(-(MAX_ABS_FOLD as f32), MAX_ABS_FOLD as f32);
    observed + fold * interval
}

fn median(values: &mut Vec<f32>) -> Option<f32> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) * 0.5)
    } else {
        Some(values[mid])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexrad::level2::{MomentData, RadialData};

    #[test]
    fn radial_continuity_unfolds_outward_gate_sequence() {
        let nyquist = 15.0;
        let observed = vec![10.0, 12.0, 14.0, -14.0, -12.0, -10.0];
        let corrected = dealias_radial(&observed, nyquist, None);

        assert_close(corrected[0], 10.0);
        assert_close(corrected[2], 14.0);
        assert_close(corrected[3], 16.0);
        assert_close(corrected[5], 20.0);
    }

    #[test]
    fn sweep_continuity_repairs_neighbor_fold() {
        let nyquist = 15.0;
        let observed = vec![
            vec![9.0, 11.0, 13.0],
            vec![10.0, 12.0, -14.0],
            vec![11.0, 13.0, 15.0],
        ];
        let mut corrected = observed.clone();
        sweep_refine_grid(&observed, &mut corrected, nyquist, 2);

        assert_close(corrected[1][2], 16.0);
    }

    #[test]
    fn velocity_sweep_replaces_velocity_moment_only() {
        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: Some(15.0),
            radials: vec![synthetic_radial(0.0), synthetic_radial(1.0)],
        };

        let out = dealias_velocity_sweep(&sweep, DealiasMethod::SweepContinuity);
        let velocity = out.radials[0]
            .moments
            .iter()
            .find(|moment| moment.product == RadarProduct::Velocity)
            .unwrap();
        assert_close(velocity.data[3], 16.0);
        let reflectivity = out.radials[0]
            .moments
            .iter()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap();
        assert_close(reflectivity.data[0], 45.0);
    }

    fn synthetic_radial(azimuth: f32) -> RadialData {
        RadialData {
            azimuth,
            elevation: 0.5,
            azimuth_spacing: 1.0,
            nyquist_velocity: Some(15.0),
            radial_status: 1,
            moments: vec![
                MomentData {
                    product: RadarProduct::Velocity,
                    gate_count: 6,
                    first_gate_range: 0,
                    gate_size: 250,
                    data: vec![10.0, 12.0, 14.0, -14.0, -12.0, -10.0],
                },
                MomentData {
                    product: RadarProduct::Reflectivity,
                    gate_count: 1,
                    first_gate_range: 0,
                    gate_size: 1_000,
                    data: vec![45.0],
                },
            ],
        }
    }

    fn assert_close(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() < 1e-4,
            "expected {expected}, got {actual}"
        );
    }
}
