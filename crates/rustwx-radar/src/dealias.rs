use crate::nexrad::level2::RadialData;
use crate::nexrad::{Level2File, Level2Sweep, RadarProduct};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};

const DEFAULT_NYQUIST_MS: f32 = 30.0;
const MAX_ABS_FOLD: i32 = 8;
const MIN_DEALIAS_REF_DBZ: f32 = 0.0;
const MAX_DEALIAS_REF_DBZ: f32 = 80.0;
const HIGH_QUALITY_SW_MS: f32 = 5.0;
const MODERATE_QUALITY_SW_MS: f32 = 15.0;
const MAX_DEALIAS_SW_MS: f32 = 25.0;
const MAX_ABS_DEALIASED_VELOCITY_MS: f32 = 120.0;
const LOW_ALIAS_MAX_FOLD_LIKE_JUMPS: usize = 256;
const LOW_ALIAS_MAX_SEVERE_JUMPS: usize = 32;
const LOW_ALIAS_MAX_FOLD_FRACTION: f64 = 0.002;
const LOW_ALIAS_MAX_JUMP_NYQUIST_MULTIPLE: f32 = 2.0;
const NETWORK_INTERVAL_SPLITS: usize = 4;
const IMPROVED_CANDIDATE_MAX_JUMP_REGRESSION_NYQUIST_MULTIPLE: f32 = 2.0;
const STAGED_EXTREME_JUMP_CLEANUP_NYQUIST_MULTIPLE: f32 = 4.0;
const STAGED_EXTREME_JUMP_CLEANUP_MAX_MS: f32 = 100.0;
const STAGED_EXTREME_JUMP_CLEANUP_PASSES: usize = 2;

/// Velocity dealiasing strategy for NEXRAD velocity moments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DealiasMethod {
    Off,
    /// One-dimensional continuity along each radial.
    RadialContinuity,
    /// Two-dimensional sweep region growing plus neighborhood refinement.
    SweepContinuity,
    /// Staged radial reference plus sweep refinement for production comparison.
    StagedContinuity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DealiasAcceptancePolicy {
    Safe,
    ForceCandidate,
}

impl Default for DealiasMethod {
    fn default() -> Self {
        Self::SweepContinuity
    }
}

impl DealiasMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::RadialContinuity => "radial",
            Self::SweepContinuity => "sweep",
            Self::StagedContinuity => "staged",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DealiasDecision {
    Disabled,
    NoVelocityMoment,
    InvalidNyquist,
    NoVelocityGrid,
    CandidateAccepted,
    CandidateUnchanged,
    CandidateRejectedWorseContinuity,
    SkippedLowAliasBurden,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct DealiasContinuityScore {
    pub fold_like_jumps: usize,
    pub severe_jumps: usize,
    pub max_abs_jump_ms: f32,
}

impl DealiasContinuityScore {
    fn is_no_worse_than(self, baseline: Self) -> bool {
        self.severe_jumps <= baseline.severe_jumps
            && self.fold_like_jumps <= baseline.fold_like_jumps
            && self.max_abs_jump_ms <= baseline.max_abs_jump_ms + f32::EPSILON
    }

    fn is_acceptable_candidate(self, baseline: Self, nyquist: f32) -> bool {
        if self.is_no_worse_than(baseline) {
            return true;
        }
        if !nyquist.is_finite() || nyquist <= 0.0 {
            return false;
        }

        let fold_improved_substantially =
            baseline.fold_like_jumps > 0 && self.fold_like_jumps * 2 <= baseline.fold_like_jumps;
        let severe_improved_substantially =
            baseline.severe_jumps > 0 && self.severe_jumps * 2 <= baseline.severe_jumps;
        let max_regression_limit = baseline.max_abs_jump_ms
            + nyquist * IMPROVED_CANDIDATE_MAX_JUMP_REGRESSION_NYQUIST_MULTIPLE;
        let max_regression_is_limited = self.max_abs_jump_ms <= max_regression_limit + f32::EPSILON;

        fold_improved_substantially && severe_improved_substantially && max_regression_is_limited
    }
}

fn continuity_score_is_better(
    candidate: DealiasContinuityScore,
    baseline: DealiasContinuityScore,
) -> bool {
    candidate
        .severe_jumps
        .cmp(&baseline.severe_jumps)
        .then_with(|| candidate.fold_like_jumps.cmp(&baseline.fold_like_jumps))
        .then_with(|| {
            candidate
                .max_abs_jump_ms
                .partial_cmp(&baseline.max_abs_jump_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .is_lt()
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DealiasReport {
    pub method: String,
    pub attempted: bool,
    pub accepted: bool,
    pub forced: bool,
    pub decision: DealiasDecision,
    pub nyquist_ms: Option<f32>,
    pub quality_gate_count: usize,
    pub changed_gate_count: usize,
    pub original_score: Option<DealiasContinuityScore>,
    pub candidate_score: Option<DealiasContinuityScore>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VelocityQualityMaskReport {
    pub finite_gate_count: usize,
    pub masked_gate_count: usize,
    pub masked_gate_fraction: f64,
}

impl DealiasReport {
    fn skipped(method: DealiasMethod, decision: DealiasDecision, nyquist_ms: Option<f32>) -> Self {
        Self {
            method: method.as_str().to_string(),
            attempted: false,
            accepted: false,
            forced: false,
            decision,
            nyquist_ms,
            quality_gate_count: 0,
            changed_gate_count: 0,
            original_score: None,
            candidate_score: None,
        }
    }
}

pub fn mask_velocity_sweep_quality(
    sweep: &Level2Sweep,
) -> (Level2Sweep, VelocityQualityMaskReport) {
    let mut out = sweep.clone();
    let mut finite_gate_count = 0usize;
    let mut masked_gate_count = 0usize;

    for (radial_index, radial) in sweep.radials.iter().enumerate() {
        for (moment_index, moment) in radial.moments.iter().enumerate() {
            if !is_velocity_product(moment.product) {
                continue;
            }
            for (gate, value) in moment.data.iter().enumerate() {
                if !value.is_finite() {
                    continue;
                }
                finite_gate_count += 1;
                if gate_quality(radial, moment, gate) <= 0.0 {
                    if let Some(output_value) = out
                        .radials
                        .get_mut(radial_index)
                        .and_then(|radial| radial.moments.get_mut(moment_index))
                        .and_then(|moment| moment.data.get_mut(gate))
                    {
                        *output_value = f32::NAN;
                        masked_gate_count += 1;
                    }
                }
            }
        }
    }

    let masked_gate_fraction = if finite_gate_count == 0 {
        0.0
    } else {
        masked_gate_count as f64 / finite_gate_count as f64
    };

    (
        out,
        VelocityQualityMaskReport {
            finite_gate_count,
            masked_gate_count,
            masked_gate_fraction,
        },
    )
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
    dealias_velocity_sweep_with_report(sweep, method).0
}

/// Return a cloned sweep with velocity moments dealiased plus QC diagnostics.
pub fn dealias_velocity_sweep_with_report(
    sweep: &Level2Sweep,
    method: DealiasMethod,
) -> (Level2Sweep, DealiasReport) {
    dealias_velocity_sweep_with_policy(sweep, method, DealiasAcceptancePolicy::Safe)
}

/// Return a cloned sweep with velocity moments dealiased plus QC diagnostics.
///
/// `ForceCandidate` is intended for research screenshots and algorithm
/// development only. Normal operational rendering should use the safe policy.
pub fn dealias_velocity_sweep_with_policy(
    sweep: &Level2Sweep,
    method: DealiasMethod,
    acceptance_policy: DealiasAcceptancePolicy,
) -> (Level2Sweep, DealiasReport) {
    if method == DealiasMethod::Off || !contains_velocity(sweep) {
        let decision = if method == DealiasMethod::Off {
            DealiasDecision::Disabled
        } else {
            DealiasDecision::NoVelocityMoment
        };
        return (
            sweep.clone(),
            DealiasReport::skipped(method, decision, None),
        );
    }

    let nyquist = effective_nyquist(sweep);
    if nyquist <= 0.0 || !nyquist.is_finite() {
        return (
            sweep.clone(),
            DealiasReport::skipped(method, DealiasDecision::InvalidNyquist, Some(nyquist)),
        );
    }

    let Some(grid) = velocity_grid(sweep) else {
        return (
            sweep.clone(),
            DealiasReport::skipped(method, DealiasDecision::NoVelocityGrid, Some(nyquist)),
        );
    };

    let original_score = continuity_score(&grid.observed, &grid.weights, nyquist);
    let quality_gate_count = quality_gate_count(&grid.observed, &grid.weights);
    if acceptance_policy != DealiasAcceptancePolicy::ForceCandidate
        && is_low_alias_burden(original_score, quality_gate_count, nyquist)
    {
        return (
            sweep.clone(),
            DealiasReport {
                method: method.as_str().to_string(),
                attempted: true,
                accepted: false,
                forced: false,
                decision: DealiasDecision::SkippedLowAliasBurden,
                nyquist_ms: Some(nyquist),
                quality_gate_count,
                changed_gate_count: 0,
                original_score: Some(original_score),
                candidate_score: None,
            },
        );
    }

    let corrected = match method {
        DealiasMethod::Off => unreachable!("off is returned above"),
        DealiasMethod::RadialContinuity => {
            radial_continuity_grid(&grid.observed, &grid.weights, nyquist)
        }
        DealiasMethod::SweepContinuity => {
            let mut corrected = region_continuity_grid(&grid.observed, &grid.weights, nyquist);
            sweep_refine_grid(&grid.observed, &grid.weights, &mut corrected, nyquist, 3);
            corrected
        }
        DealiasMethod::StagedContinuity => {
            staged_continuity_grid(&grid.observed, &grid.weights, nyquist)
        }
    };

    let corrected_score = continuity_score(&corrected, &grid.weights, nyquist);
    let changed_gate_count = output_changed_gate_count(sweep, &grid, &corrected);
    if !corrected_score.is_acceptable_candidate(original_score, nyquist) {
        if acceptance_policy == DealiasAcceptancePolicy::ForceCandidate && changed_gate_count > 0 {
            let out = corrected_sweep(sweep, &grid, &corrected);
            return (
                out,
                DealiasReport {
                    method: method.as_str().to_string(),
                    attempted: true,
                    accepted: false,
                    forced: true,
                    decision: DealiasDecision::CandidateRejectedWorseContinuity,
                    nyquist_ms: Some(nyquist),
                    quality_gate_count,
                    changed_gate_count,
                    original_score: Some(original_score),
                    candidate_score: Some(corrected_score),
                },
            );
        }

        return (
            sweep.clone(),
            DealiasReport {
                method: method.as_str().to_string(),
                attempted: true,
                accepted: false,
                forced: false,
                decision: DealiasDecision::CandidateRejectedWorseContinuity,
                nyquist_ms: Some(nyquist),
                quality_gate_count,
                changed_gate_count,
                original_score: Some(original_score),
                candidate_score: Some(corrected_score),
            },
        );
    }

    if changed_gate_count == 0 {
        return (
            sweep.clone(),
            DealiasReport {
                method: method.as_str().to_string(),
                attempted: true,
                accepted: false,
                forced: false,
                decision: DealiasDecision::CandidateUnchanged,
                nyquist_ms: Some(nyquist),
                quality_gate_count,
                changed_gate_count,
                original_score: Some(original_score),
                candidate_score: Some(corrected_score),
            },
        );
    }

    let out = corrected_sweep(sweep, &grid, &corrected);

    (
        out,
        DealiasReport {
            method: method.as_str().to_string(),
            attempted: true,
            accepted: true,
            forced: false,
            decision: DealiasDecision::CandidateAccepted,
            nyquist_ms: Some(nyquist),
            quality_gate_count,
            changed_gate_count,
            original_score: Some(original_score),
            candidate_score: Some(corrected_score),
        },
    )
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

struct VelocityGrid {
    observed: Vec<Vec<f32>>,
    weights: Vec<Vec<f32>>,
    radial_to_row: Vec<Option<usize>>,
}

fn velocity_grid(sweep: &Level2Sweep) -> Option<VelocityGrid> {
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
    let mut weights = vec![vec![0.0; max_gates]; radial_indices.len()];
    let mut radial_to_row = vec![None; sweep.radials.len()];

    for (row, (_, radial_index)) in radial_indices.iter().enumerate() {
        let radial = &sweep.radials[*radial_index];
        if let Some(moment) = velocity_moment(radial) {
            for gate in 0..moment.data.len() {
                let velocity = moment.data[gate];
                let quality = gate_quality(radial, moment, gate);
                if velocity.is_finite() && quality > 0.0 {
                    grid[row][gate] = velocity;
                    weights[row][gate] = quality;
                }
            }
            radial_to_row[*radial_index] = Some(row);
        }
    }

    Some(VelocityGrid {
        observed: grid,
        weights,
        radial_to_row,
    })
}

fn velocity_moment(radial: &RadialData) -> Option<&crate::nexrad::level2::MomentData> {
    radial
        .moments
        .iter()
        .find(|moment| is_velocity_product(moment.product))
}

fn gate_quality(
    radial: &RadialData,
    velocity: &crate::nexrad::level2::MomentData,
    gate: usize,
) -> f32 {
    let range_m = velocity.first_gate_range as f64 + gate as f64 * velocity.gate_size as f64;
    let mut quality = 1.0;

    if let Some(reflectivity) = sample_product(radial, RadarProduct::Reflectivity, range_m) {
        if !(MIN_DEALIAS_REF_DBZ..=MAX_DEALIAS_REF_DBZ).contains(&reflectivity) {
            return 0.0;
        }
        if reflectivity < 10.0 {
            quality *= 0.6;
        }
    }

    if let Some(spectrum_width) = sample_product(radial, RadarProduct::SpectrumWidth, range_m) {
        if spectrum_width > MAX_DEALIAS_SW_MS {
            return 0.0;
        }
        quality *= if spectrum_width <= HIGH_QUALITY_SW_MS {
            1.0
        } else if spectrum_width <= MODERATE_QUALITY_SW_MS {
            0.6
        } else {
            0.25
        };
    }

    quality
}

fn sample_product(radial: &RadialData, product: RadarProduct, range_m: f64) -> Option<f32> {
    let moment = radial
        .moments
        .iter()
        .find(|moment| moment.product == product)?;
    let gate_offset = range_m - moment.first_gate_range as f64;
    if gate_offset < 0.0 || moment.gate_size == 0 {
        return None;
    }
    let gate = (gate_offset / moment.gate_size as f64).floor() as usize;
    moment
        .data
        .get(gate)
        .copied()
        .filter(|value| value.is_finite())
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
                } else {
                    *value = f32::NAN;
                }
            }
        }
    }
}

fn corrected_sweep(
    sweep: &Level2Sweep,
    grid: &VelocityGrid,
    corrected: &[Vec<f32>],
) -> Level2Sweep {
    let mut out = sweep.clone();
    for (radial_index, radial) in out.radials.iter_mut().enumerate() {
        let Some(row) = grid.radial_to_row.get(radial_index).and_then(|row| *row) else {
            continue;
        };
        replace_velocity_moment(radial, &corrected[row]);
    }
    out
}

fn radial_continuity_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    nyquist: f32,
) -> Vec<Vec<f32>> {
    let rows = observed.len();
    let mut corrected = Vec::with_capacity(rows);
    let mut previous: Option<Vec<f32>> = None;
    let mut previous_weights: Option<Vec<f32>> = None;

    for (row, row_weights) in observed.iter().zip(weights.iter()) {
        let row_corrected = dealias_radial(
            row,
            row_weights,
            nyquist,
            previous.as_deref(),
            previous_weights.as_deref(),
        );
        previous = Some(row_corrected.clone());
        previous_weights = Some(row_weights.clone());
        corrected.push(row_corrected);
    }

    corrected
}

fn region_continuity_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    nyquist: f32,
) -> Vec<Vec<f32>> {
    let rows = observed.len();
    let mut corrected: Vec<Vec<f32>> = observed
        .iter()
        .map(|row| vec![f32::NAN; row.len()])
        .collect();
    if rows == 0 {
        return corrected;
    }

    let mut seeds = Vec::new();
    for (row, values) in observed.iter().enumerate() {
        for (col, value) in values.iter().enumerate() {
            let weight = weight_at(weights.get(row).map(Vec::as_slice).unwrap_or(&[]), col);
            if value.is_finite() && weight > 0.0 {
                seeds.push((row, col, weight));
            }
        }
    }
    seeds.sort_by(|a, b| {
        b.2.partial_cmp(&a.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| a.1.cmp(&b.1))
    });

    let mut queue = VecDeque::new();
    for (seed_row, seed_col, _) in seeds {
        if corrected[seed_row][seed_col].is_finite() {
            continue;
        }

        corrected[seed_row][seed_col] = observed[seed_row][seed_col];
        queue.push_back((seed_row, seed_col));
        while let Some((row, col)) = queue.pop_front() {
            enqueue_region_neighbors(
                observed,
                weights,
                &mut corrected,
                &mut queue,
                row,
                col,
                nyquist,
            );
        }
    }

    corrected
}

fn staged_continuity_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    nyquist: f32,
) -> Vec<Vec<f32>> {
    let radial = radial_continuity_grid(observed, weights, nyquist);
    let mut sweep = region_continuity_grid(observed, weights, nyquist);
    sweep_refine_grid(observed, weights, &mut sweep, nyquist, 2);
    let mut refined = sweep.clone();
    reference_refine_grid(observed, weights, &radial, &mut refined, nyquist, 2);

    let partial_radial = partial_accept_grid(observed, weights, &radial, nyquist);
    let partial_sweep = partial_accept_grid(observed, weights, &sweep, nyquist);
    let partial_refined = partial_accept_grid(observed, weights, &refined, nyquist);

    let mut best = radial.clone();
    let mut best_score = continuity_score(&best, weights, nyquist);
    for candidate in [
        sweep,
        refined,
        partial_radial,
        partial_sweep,
        partial_refined,
    ] {
        let candidate_score = continuity_score(&candidate, weights, nyquist);
        if continuity_score_is_better(candidate_score, best_score) {
            best = candidate;
            best_score = candidate_score;
        }
    }

    if best_score.fold_like_jumps == 0
        && best_score.severe_jumps == 0
        && best_score.max_abs_jump_ms <= nyquist
    {
        return best;
    }

    let mut network = network_region_continuity_grid(observed, weights, nyquist);
    sweep_refine_grid(observed, weights, &mut network, nyquist, 2);
    let mut network_refined = network.clone();
    reference_refine_grid(observed, weights, &radial, &mut network_refined, nyquist, 2);
    let partial_network = partial_accept_grid(observed, weights, &network, nyquist);
    let partial_network_refined = partial_accept_grid(observed, weights, &network_refined, nyquist);

    for candidate in [
        network,
        network_refined,
        partial_network,
        partial_network_refined,
    ] {
        let candidate_score = continuity_score(&candidate, weights, nyquist);
        if continuity_score_is_better(candidate_score, best_score) {
            best = candidate;
            best_score = candidate_score;
        }
    }
    mask_staged_extreme_jump_pairs(&mut best, weights, nyquist);
    best
}

fn mask_staged_extreme_jump_pairs(values: &mut [Vec<f32>], weights: &[Vec<f32>], nyquist: f32) {
    if !nyquist.is_finite() || nyquist <= 0.0 || values.is_empty() {
        return;
    }
    let jump_limit = (nyquist * STAGED_EXTREME_JUMP_CLEANUP_NYQUIST_MULTIPLE)
        .min(STAGED_EXTREME_JUMP_CLEANUP_MAX_MS);

    for _ in 0..STAGED_EXTREME_JUMP_CLEANUP_PASSES {
        let mut mask = values
            .iter()
            .map(|row| vec![false; row.len()])
            .collect::<Vec<_>>();
        let mut marked = 0usize;

        for row in 0..values.len() {
            for col in 1..values[row].len() {
                if should_mask_extreme_jump_pair(
                    values[row][col - 1],
                    weight_at(weights.get(row).map(Vec::as_slice).unwrap_or(&[]), col - 1),
                    values[row][col],
                    weight_at(weights.get(row).map(Vec::as_slice).unwrap_or(&[]), col),
                    jump_limit,
                ) {
                    mark_extreme_jump_gate(&mut mask, row, col - 1, &mut marked);
                    mark_extreme_jump_gate(&mut mask, row, col, &mut marked);
                }
            }
        }

        for row in 0..values.len() {
            let next_row = if row + 1 == values.len() { 0 } else { row + 1 };
            if row == next_row {
                continue;
            }
            let cols = values[row].len().min(values[next_row].len());
            let row_weights = weights.get(row).map(Vec::as_slice).unwrap_or(&[]);
            let next_weights = weights.get(next_row).map(Vec::as_slice).unwrap_or(&[]);
            for col in 0..cols {
                if should_mask_extreme_jump_pair(
                    values[row][col],
                    weight_at(row_weights, col),
                    values[next_row][col],
                    weight_at(next_weights, col),
                    jump_limit,
                ) {
                    mark_extreme_jump_gate(&mut mask, row, col, &mut marked);
                    mark_extreme_jump_gate(&mut mask, next_row, col, &mut marked);
                }
            }
        }

        if marked == 0 {
            break;
        }

        for (row, row_mask) in mask.into_iter().enumerate() {
            for (col, should_mask) in row_mask.into_iter().enumerate() {
                if should_mask {
                    values[row][col] = f32::NAN;
                }
            }
        }
    }
}

fn should_mask_extreme_jump_pair(
    a: f32,
    a_weight: f32,
    b: f32,
    b_weight: f32,
    jump_limit: f32,
) -> bool {
    a.is_finite() && b.is_finite() && a_weight > 0.0 && b_weight > 0.0 && (a - b).abs() > jump_limit
}

fn mark_extreme_jump_gate(mask: &mut [Vec<bool>], row: usize, col: usize, marked: &mut usize) {
    if let Some(value) = mask.get_mut(row).and_then(|row| row.get_mut(col)) {
        if !*value {
            *value = true;
            *marked += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct NetworkEdgeAccumulator {
    weighted_delta_sum: f32,
    weighted_residual_sum: f32,
    weight_sum: f32,
    count: usize,
}

#[derive(Debug, Clone, Copy)]
struct NetworkEdge {
    a: usize,
    b: usize,
    delta_b_from_a: i32,
    cost: f32,
    count: usize,
}

fn network_region_continuity_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    nyquist: f32,
) -> Vec<Vec<f32>> {
    let rows = observed.len();
    let mut corrected: Vec<Vec<f32>> = observed
        .iter()
        .map(|row| vec![f32::NAN; row.len()])
        .collect();
    if rows == 0 || !nyquist.is_finite() || nyquist <= 0.0 {
        return corrected;
    }

    let mut labels = observed
        .iter()
        .map(|row| vec![0usize; row.len()])
        .collect::<Vec<_>>();
    let mut region_sizes = Vec::new();
    let mut next_label = 1usize;
    for row in 0..rows {
        for col in 0..observed[row].len() {
            if labels[row][col] != 0 || !network_gate_is_valid(observed, weights, row, col) {
                continue;
            }
            let bin = network_velocity_bin(observed[row][col], nyquist);
            let size = label_network_region(
                observed,
                weights,
                &mut labels,
                row,
                col,
                bin,
                next_label,
                nyquist,
            );
            if size > 0 {
                region_sizes.push(size);
                next_label += 1;
            }
        }
    }

    if region_sizes.is_empty() {
        return corrected;
    }
    if region_sizes.len() == 1 {
        copy_valid_network_gates(observed, weights, &mut corrected);
        return corrected;
    }

    let edges = network_region_edges(observed, weights, &labels, nyquist);
    let offsets = network_region_offsets(region_sizes.as_slice(), &edges);
    let interval = 2.0 * nyquist;
    for row in 0..rows {
        for col in 0..observed[row].len() {
            let label = labels[row][col];
            if label == 0 {
                continue;
            }
            let Some(offset) = offsets.get(label - 1).and_then(|offset| *offset) else {
                continue;
            };
            let candidate = observed[row][col] + offset as f32 * interval;
            corrected[row][col] = if candidate.abs() <= MAX_ABS_DEALIASED_VELOCITY_MS {
                candidate
            } else {
                observed[row][col]
            };
        }
    }

    corrected
}

fn network_gate_is_valid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    row: usize,
    col: usize,
) -> bool {
    observed
        .get(row)
        .and_then(|values| values.get(col))
        .copied()
        .is_some_and(f32::is_finite)
        && weights
            .get(row)
            .map(|row_weights| weight_at(row_weights, col) > 0.0)
            .unwrap_or(false)
}

fn network_velocity_bin(value: f32, nyquist: f32) -> usize {
    let interval = 2.0 * nyquist;
    let normalized = ((value + nyquist) / interval).clamp(0.0, 0.999_999);
    (normalized * NETWORK_INTERVAL_SPLITS as f32).floor() as usize
}

fn label_network_region(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    labels: &mut [Vec<usize>],
    seed_row: usize,
    seed_col: usize,
    bin: usize,
    label: usize,
    nyquist: f32,
) -> usize {
    let mut queue = VecDeque::new();
    labels[seed_row][seed_col] = label;
    queue.push_back((seed_row, seed_col));
    let mut size = 0usize;

    while let Some((row, col)) = queue.pop_front() {
        size += 1;
        let rows = observed.len();
        if rows > 1 {
            let prev = if row == 0 { rows - 1 } else { row - 1 };
            try_label_network_neighbor(
                observed, weights, labels, &mut queue, prev, col, bin, label, nyquist,
            );
            let next = if row + 1 == rows { 0 } else { row + 1 };
            if next != prev {
                try_label_network_neighbor(
                    observed, weights, labels, &mut queue, next, col, bin, label, nyquist,
                );
            }
        }
        if col > 0 {
            try_label_network_neighbor(
                observed,
                weights,
                labels,
                &mut queue,
                row,
                col - 1,
                bin,
                label,
                nyquist,
            );
        }
        try_label_network_neighbor(
            observed,
            weights,
            labels,
            &mut queue,
            row,
            col + 1,
            bin,
            label,
            nyquist,
        );
    }

    size
}

#[allow(clippy::too_many_arguments)]
fn try_label_network_neighbor(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    labels: &mut [Vec<usize>],
    queue: &mut VecDeque<(usize, usize)>,
    row: usize,
    col: usize,
    bin: usize,
    label: usize,
    nyquist: f32,
) {
    let Some(row_labels) = labels.get(row) else {
        return;
    };
    if col >= row_labels.len()
        || row_labels[col] != 0
        || !network_gate_is_valid(observed, weights, row, col)
        || network_velocity_bin(observed[row][col], nyquist) != bin
    {
        return;
    }

    if let Some(label_slot) = labels.get_mut(row).and_then(|row| row.get_mut(col)) {
        *label_slot = label;
        queue.push_back((row, col));
    }
}

fn copy_valid_network_gates(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    corrected: &mut [Vec<f32>],
) {
    for row in 0..observed.len() {
        for col in 0..observed[row].len() {
            if network_gate_is_valid(observed, weights, row, col) {
                corrected[row][col] = observed[row][col];
            }
        }
    }
}

fn network_region_edges(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    labels: &[Vec<usize>],
    nyquist: f32,
) -> Vec<NetworkEdge> {
    let rows = observed.len();
    let interval = 2.0 * nyquist;
    let mut accumulators = HashMap::<(usize, usize), NetworkEdgeAccumulator>::new();

    for row in 0..rows {
        for col in 0..observed[row].len() {
            add_network_edge_pair(
                observed,
                weights,
                labels,
                nyquist,
                interval,
                &mut accumulators,
                row,
                col,
                row,
                col + 1,
            );
            if rows > 1 {
                let next_row = if row + 1 == rows { 0 } else { row + 1 };
                if row < next_row || row + 1 == rows {
                    add_network_edge_pair(
                        observed,
                        weights,
                        labels,
                        nyquist,
                        interval,
                        &mut accumulators,
                        row,
                        col,
                        next_row,
                        col,
                    );
                }
            }
        }
    }

    let mut edges = accumulators
        .into_iter()
        .filter_map(|((a, b), acc)| {
            if acc.weight_sum <= 0.0 || acc.count == 0 {
                return None;
            }
            let delta_b_from_a = (acc.weighted_delta_sum / acc.weight_sum).round() as i32;
            let cost = acc.weighted_residual_sum / acc.weight_sum;
            Some(NetworkEdge {
                a,
                b,
                delta_b_from_a,
                cost,
                count: acc.count,
            })
        })
        .collect::<Vec<_>>();
    edges.sort_by(|a, b| {
        a.cost
            .partial_cmp(&b.cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| a.a.cmp(&b.a))
            .then_with(|| a.b.cmp(&b.b))
    });
    edges
}

#[allow(clippy::too_many_arguments)]
fn add_network_edge_pair(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    labels: &[Vec<usize>],
    nyquist: f32,
    interval: f32,
    accumulators: &mut HashMap<(usize, usize), NetworkEdgeAccumulator>,
    row_a: usize,
    col_a: usize,
    row_b: usize,
    col_b: usize,
) {
    let Some(label_a) = labels.get(row_a).and_then(|row| row.get(col_a)).copied() else {
        return;
    };
    let Some(label_b) = labels.get(row_b).and_then(|row| row.get(col_b)).copied() else {
        return;
    };
    if label_a == 0 || label_b == 0 || label_a == label_b {
        return;
    }
    let Some(value_a) = observed.get(row_a).and_then(|row| row.get(col_a)).copied() else {
        return;
    };
    let Some(value_b) = observed.get(row_b).and_then(|row| row.get(col_b)).copied() else {
        return;
    };
    if !value_a.is_finite() || !value_b.is_finite() {
        return;
    }
    let weight = weight_at(weights.get(row_a).map(Vec::as_slice).unwrap_or(&[]), col_a).min(
        weight_at(weights.get(row_b).map(Vec::as_slice).unwrap_or(&[]), col_b),
    );
    if weight <= 0.0 {
        return;
    }

    let (lo, hi, value_lo, value_hi) = if label_a < label_b {
        (label_a - 1, label_b - 1, value_a, value_b)
    } else {
        (label_b - 1, label_a - 1, value_b, value_a)
    };
    let delta_hi_from_lo = ((value_lo - value_hi) / interval)
        .round()
        .clamp(-(MAX_ABS_FOLD as f32), MAX_ABS_FOLD as f32);
    let residual = (value_lo - (value_hi + delta_hi_from_lo * interval)).abs();
    if residual > nyquist * 1.5 {
        return;
    }

    let entry = accumulators
        .entry((lo, hi))
        .or_insert(NetworkEdgeAccumulator {
            weighted_delta_sum: 0.0,
            weighted_residual_sum: 0.0,
            weight_sum: 0.0,
            count: 0,
        });
    entry.weighted_delta_sum += delta_hi_from_lo * weight;
    entry.weighted_residual_sum += residual * weight;
    entry.weight_sum += weight;
    entry.count += 1;
}

fn network_region_offsets(region_sizes: &[usize], edges: &[NetworkEdge]) -> Vec<Option<i32>> {
    let mut adjacency = vec![Vec::<(usize, i32, f32, usize)>::new(); region_sizes.len()];
    for edge in edges {
        adjacency[edge.a].push((edge.b, edge.delta_b_from_a, edge.cost, edge.count));
        adjacency[edge.b].push((edge.a, -edge.delta_b_from_a, edge.cost, edge.count));
    }

    let mut offsets = vec![None; region_sizes.len()];
    let mut assigned = 0usize;
    while assigned < region_sizes.len() {
        let Some(seed) = largest_unassigned_region(region_sizes, &offsets) else {
            break;
        };
        offsets[seed] = Some(0);
        assigned += 1;

        let mut heap = BinaryHeap::<Reverse<(i64, i64, usize, usize, i32)>>::new();
        push_network_edges(&adjacency, &offsets, seed, &mut heap);
        while let Some(Reverse((_, _, from, to, delta_to_from_from))) = heap.pop() {
            let Some(from_offset) = offsets[from] else {
                continue;
            };
            if offsets[to].is_some() {
                continue;
            }
            offsets[to] =
                Some((from_offset + delta_to_from_from).clamp(-MAX_ABS_FOLD, MAX_ABS_FOLD));
            assigned += 1;
            push_network_edges(&adjacency, &offsets, to, &mut heap);
        }
    }

    center_network_offsets(region_sizes, &mut offsets);
    offsets
}

fn largest_unassigned_region(region_sizes: &[usize], offsets: &[Option<i32>]) -> Option<usize> {
    region_sizes
        .iter()
        .enumerate()
        .filter(|(index, _)| offsets[*index].is_none())
        .max_by_key(|(_, size)| **size)
        .map(|(index, _)| index)
}

fn push_network_edges(
    adjacency: &[Vec<(usize, i32, f32, usize)>],
    offsets: &[Option<i32>],
    from: usize,
    heap: &mut BinaryHeap<Reverse<(i64, i64, usize, usize, i32)>>,
) {
    for (to, delta, cost, count) in &adjacency[from] {
        if offsets[*to].is_some() {
            continue;
        }
        let cost_milli = (cost.max(0.0) * 1000.0).round() as i64;
        heap.push(Reverse((cost_milli, -(*count as i64), from, *to, *delta)));
    }
}

fn center_network_offsets(region_sizes: &[usize], offsets: &mut [Option<i32>]) {
    let mut weighted_sum = 0i64;
    let mut weight_sum = 0i64;
    for (size, offset) in region_sizes.iter().zip(offsets.iter()) {
        if let Some(offset) = offset {
            weighted_sum += *size as i64 * *offset as i64;
            weight_sum += *size as i64;
        }
    }
    if weight_sum == 0 {
        return;
    }
    let average = weighted_sum as f32 / weight_sum as f32;
    let center = if average.abs() >= 0.75 {
        average.round() as i32
    } else {
        0
    };
    if center == 0 {
        return;
    }
    for offset in offsets.iter_mut().flatten() {
        *offset = (*offset - center).clamp(-MAX_ABS_FOLD, MAX_ABS_FOLD);
    }
}

fn partial_accept_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    candidate: &[Vec<f32>],
    nyquist: f32,
) -> Vec<Vec<f32>> {
    let rows = observed.len();
    let mut merged = observed.to_vec();
    if rows == 0 {
        return merged;
    }

    for row in 0..rows {
        let cols = observed[row]
            .len()
            .min(candidate.get(row).map(Vec::len).unwrap_or(0));
        for col in 0..cols {
            let observed_value = observed[row][col];
            let candidate_value = candidate[row][col];
            if !observed_value.is_finite()
                || !candidate_value.is_finite()
                || weight_at(&weights[row], col) <= 0.0
                || (observed_value - candidate_value).abs() <= 0.001
            {
                continue;
            }

            let baseline_score =
                local_continuity_score(observed, weights, row, col, observed_value, nyquist);
            let candidate_score =
                local_continuity_score(candidate, weights, row, col, candidate_value, nyquist);
            if candidate_score.is_acceptable_candidate(baseline_score, nyquist)
                && continuity_score_is_better(candidate_score, baseline_score)
            {
                merged[row][col] = candidate_value;
            }
        }
    }

    merged
}

fn local_continuity_score(
    values: &[Vec<f32>],
    weights: &[Vec<f32>],
    row: usize,
    col: usize,
    value: f32,
    nyquist: f32,
) -> DealiasContinuityScore {
    let mut score = DealiasContinuityScore {
        fold_like_jumps: 0,
        severe_jumps: 0,
        max_abs_jump_ms: 0.0,
    };
    let rows = values.len();
    if rows == 0 {
        return score;
    }

    let fold_threshold = nyquist;
    let severe_threshold = nyquist * 1.5;
    let row_prev = if row == 0 { rows - 1 } else { row - 1 };
    let row_next = if row + 1 == rows { 0 } else { row + 1 };
    for neighbor_row in [row_prev, row, row_next] {
        let Some(neighbor_values) = values.get(neighbor_row) else {
            continue;
        };
        if neighbor_values.is_empty() {
            continue;
        }
        let neighbor_weights = weights.get(neighbor_row).map(Vec::as_slice).unwrap_or(&[]);
        let start_col = col.saturating_sub(1);
        let end_col = (col + 1).min(neighbor_values.len().saturating_sub(1));
        for neighbor_col in start_col..=end_col {
            if neighbor_row == row && neighbor_col == col {
                continue;
            }
            update_continuity_score(
                value,
                weight_at(&weights[row], col),
                neighbor_values[neighbor_col],
                weight_at(neighbor_weights, neighbor_col),
                fold_threshold,
                severe_threshold,
                &mut score,
            );
        }
    }
    score
}

fn reference_refine_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    reference: &[Vec<f32>],
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
                if !observed_value.is_finite() || weight_at(&weights[row], col) <= 0.0 {
                    continue;
                }

                let mut refs = Vec::with_capacity(11);
                push_neighbor(&mut refs, &current[row_prev], &weights[row_prev], col);
                push_neighbor(&mut refs, &current[row_next], &weights[row_next], col);
                if col > 0 {
                    push_neighbor(&mut refs, &current[row], &weights[row], col - 1);
                    push_neighbor(&mut refs, &current[row_prev], &weights[row_prev], col - 1);
                    push_neighbor(&mut refs, &current[row_next], &weights[row_next], col - 1);
                }
                if col + 1 < cols {
                    push_neighbor(&mut refs, &current[row], &weights[row], col + 1);
                    push_neighbor(&mut refs, &current[row_prev], &weights[row_prev], col + 1);
                    push_neighbor(&mut refs, &current[row_next], &weights[row_next], col + 1);
                }
                push_reference(&mut refs, reference, weights, row, col, 1.5);
                if col > 0 {
                    push_reference(&mut refs, reference, weights, row, col - 1, 0.5);
                }
                if col + 1 < cols {
                    push_reference(&mut refs, reference, weights, row, col + 1, 0.5);
                }

                let Some(reference_value) = weighted_median(&mut refs) else {
                    continue;
                };
                let candidate = unfold_to_reference(observed_value, reference_value, nyquist);
                let current_value = current[row][col];
                if !current_value.is_finite()
                    || (candidate - reference_value).abs() + 0.1 * nyquist
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

fn push_reference(
    values: &mut Vec<(f32, f32)>,
    reference: &[Vec<f32>],
    weights: &[Vec<f32>],
    row: usize,
    col: usize,
    weight_multiplier: f32,
) {
    let Some(row_values) = reference.get(row) else {
        return;
    };
    let Some(value) = row_values.get(col).copied() else {
        return;
    };
    let row_weights = weights.get(row).map(Vec::as_slice).unwrap_or(&[]);
    let weight = weight_at(row_weights, col) * weight_multiplier;
    if value.is_finite() && weight > 0.0 {
        values.push((value, weight));
    }
}

fn enqueue_region_neighbors(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    corrected: &mut [Vec<f32>],
    queue: &mut VecDeque<(usize, usize)>,
    row: usize,
    col: usize,
    nyquist: f32,
) {
    let rows = observed.len();
    if rows == 0 {
        return;
    }

    let mut enqueue = |neighbor_row: usize, neighbor_col: usize| {
        unfold_region_neighbor(
            observed,
            weights,
            corrected,
            queue,
            neighbor_row,
            neighbor_col,
            nyquist,
        );
    };

    if rows > 1 {
        let row_prev = if row == 0 { rows - 1 } else { row - 1 };
        enqueue_row_neighbors(&mut enqueue, row_prev, col, true);
    }

    enqueue_row_neighbors(&mut enqueue, row, col, false);

    if rows > 2 {
        let row_next = if row + 1 == rows { 0 } else { row + 1 };
        enqueue_row_neighbors(&mut enqueue, row_next, col, true);
    }
}

fn enqueue_row_neighbors<F>(enqueue: &mut F, row: usize, col: usize, include_center: bool)
where
    F: FnMut(usize, usize),
{
    if col > 0 {
        enqueue(row, col - 1);
    }
    if include_center {
        enqueue(row, col);
    }
    enqueue(row, col + 1);
}

fn unfold_region_neighbor(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
    corrected: &mut [Vec<f32>],
    queue: &mut VecDeque<(usize, usize)>,
    row: usize,
    col: usize,
    nyquist: f32,
) {
    let Some(row_values) = observed.get(row) else {
        return;
    };
    let Some(observed_value) = row_values.get(col).copied() else {
        return;
    };
    if !observed_value.is_finite() || corrected[row][col].is_finite() {
        return;
    }

    let row_weights = weights.get(row).map(Vec::as_slice).unwrap_or(&[]);
    if weight_at(row_weights, col) <= 0.0 {
        return;
    }

    let reference =
        corrected_neighbor_reference(corrected, weights, row, col).unwrap_or(observed_value);
    corrected[row][col] = unfold_to_reference(observed_value, reference, nyquist);
    queue.push_back((row, col));
}

fn corrected_neighbor_reference(
    corrected: &[Vec<f32>],
    weights: &[Vec<f32>],
    row: usize,
    col: usize,
) -> Option<f32> {
    let rows = corrected.len();
    if rows == 0 {
        return None;
    }

    let mut refs = Vec::with_capacity(8);
    if rows > 1 {
        let row_prev = if row == 0 { rows - 1 } else { row - 1 };
        push_neighbor(&mut refs, &corrected[row_prev], &weights[row_prev], col);
        if col > 0 {
            push_neighbor(&mut refs, &corrected[row_prev], &weights[row_prev], col - 1);
        }
        push_neighbor(&mut refs, &corrected[row_prev], &weights[row_prev], col + 1);
    }

    if col > 0 {
        push_neighbor(&mut refs, &corrected[row], &weights[row], col - 1);
    }
    push_neighbor(&mut refs, &corrected[row], &weights[row], col + 1);

    if rows > 2 {
        let row_next = if row + 1 == rows { 0 } else { row + 1 };
        push_neighbor(&mut refs, &corrected[row_next], &weights[row_next], col);
        if col > 0 {
            push_neighbor(&mut refs, &corrected[row_next], &weights[row_next], col - 1);
        }
        push_neighbor(&mut refs, &corrected[row_next], &weights[row_next], col + 1);
    }

    weighted_median(&mut refs)
}

fn dealias_radial(
    observed: &[f32],
    weights: &[f32],
    nyquist: f32,
    reference: Option<&[f32]>,
    reference_weights: Option<&[f32]>,
) -> Vec<f32> {
    let mut corrected = vec![f32::NAN; observed.len()];
    let Some(seed) = pick_seed(observed, weights, reference, reference_weights) else {
        return corrected;
    };

    let seed_observed = observed[seed];
    corrected[seed] = reference
        .and_then(|reference| reference.get(seed).copied())
        .filter(|value| value.is_finite())
        .map(|reference| unfold_to_reference(seed_observed, reference, nyquist))
        .unwrap_or(seed_observed);

    walk_radial(
        observed,
        weights,
        &mut corrected,
        reference,
        reference_weights,
        seed,
        nyquist,
        1,
    );
    walk_radial(
        observed,
        weights,
        &mut corrected,
        reference,
        reference_weights,
        seed,
        nyquist,
        -1,
    );
    corrected
}

fn pick_seed(
    observed: &[f32],
    weights: &[f32],
    reference: Option<&[f32]>,
    reference_weights: Option<&[f32]>,
) -> Option<usize> {
    if let Some(reference) = reference {
        let overlap: Vec<usize> = observed
            .iter()
            .zip(reference.iter())
            .enumerate()
            .filter_map(|(index, (observed, reference))| {
                (observed.is_finite() && reference.is_finite() && weight_at(weights, index) > 0.0)
                    .then_some(index)
            })
            .collect();
        if !overlap.is_empty() {
            let center = overlap.iter().sum::<usize>() as f32 / overlap.len() as f32;
            return overlap.into_iter().max_by(|a, b| {
                let aw = weight_at(weights, *a)
                    + reference_weights.map(|w| weight_at(w, *a)).unwrap_or(0.0);
                let bw = weight_at(weights, *b)
                    + reference_weights.map(|w| weight_at(w, *b)).unwrap_or(0.0);
                aw.partial_cmp(&bw)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        let ad = (*a as f32 - center).abs();
                        let bd = (*b as f32 - center).abs();
                        bd.partial_cmp(&ad).unwrap_or(std::cmp::Ordering::Equal)
                    })
            });
        }
    }

    observed
        .iter()
        .enumerate()
        .filter(|(index, value)| value.is_finite() && weight_at(weights, *index) > 0.0)
        .next()
        .map(|(index, _)| index)
}

fn walk_radial(
    observed: &[f32],
    weights: &[f32],
    corrected: &mut [f32],
    reference: Option<&[f32]>,
    reference_weights: Option<&[f32]>,
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
        if !observed_value.is_finite() || weight_at(weights, gate) <= 0.0 {
            index += direction;
            continue;
        }

        let mut refs = Vec::with_capacity(3);
        if let Some(last) = last_valid {
            if corrected[last].is_finite() {
                refs.push((corrected[last], weight_at(weights, last)));
                if let Some(previous) = last_valid_two {
                    if corrected[previous].is_finite() {
                        let slope = corrected[last] - corrected[previous];
                        refs.push((
                            corrected[last] + slope,
                            weight_at(weights, last).min(weight_at(weights, previous)) * 0.75,
                        ));
                    }
                }
            }
        }
        if let Some(reference) = reference {
            if let Some(reference_value) = reference.get(gate).copied() {
                if reference_value.is_finite() {
                    refs.push((
                        reference_value,
                        reference_weights.map(|w| weight_at(w, gate)).unwrap_or(0.5),
                    ));
                }
            }
        }

        let reference_value = weighted_median(&mut refs).unwrap_or(observed_value);
        corrected[gate] = unfold_to_reference(observed_value, reference_value, nyquist);
        last_valid_two = last_valid;
        last_valid = Some(gate);
        index += direction;
    }
}

fn sweep_refine_grid(
    observed: &[Vec<f32>],
    weights: &[Vec<f32>],
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
                if !observed_value.is_finite() || weight_at(&weights[row], col) <= 0.0 {
                    continue;
                }

                let mut refs = Vec::with_capacity(8);
                push_neighbor(&mut refs, &current[row_prev], &weights[row_prev], col);
                push_neighbor(&mut refs, &current[row_next], &weights[row_next], col);
                if col > 0 {
                    push_neighbor(&mut refs, &current[row], &weights[row], col - 1);
                    push_neighbor(&mut refs, &current[row_prev], &weights[row_prev], col - 1);
                    push_neighbor(&mut refs, &current[row_next], &weights[row_next], col - 1);
                }
                if col + 1 < cols {
                    push_neighbor(&mut refs, &current[row], &weights[row], col + 1);
                    push_neighbor(&mut refs, &current[row_prev], &weights[row_prev], col + 1);
                    push_neighbor(&mut refs, &current[row_next], &weights[row_next], col + 1);
                }

                if refs.len() < 2 {
                    continue;
                }
                let Some(reference_value) = weighted_median(&mut refs) else {
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

fn push_neighbor(values: &mut Vec<(f32, f32)>, row: &[f32], weights: &[f32], col: usize) {
    if let Some(value) = row.get(col).copied() {
        let weight = weight_at(weights, col);
        if value.is_finite() && weight > 0.0 {
            values.push((value, weight));
        }
    }
}

fn weight_at(weights: &[f32], index: usize) -> f32 {
    weights.get(index).copied().unwrap_or(0.0).max(0.0)
}

fn unfold_to_reference(observed: f32, reference: f32, nyquist: f32) -> f32 {
    let interval = 2.0 * nyquist;
    let target_fold = ((reference - observed) / interval)
        .round()
        .clamp(-(MAX_ABS_FOLD as f32), MAX_ABS_FOLD as f32);
    let target_fold = target_fold as i32;

    (-MAX_ABS_FOLD..=MAX_ABS_FOLD)
        .min_by(|a, b| {
            let a_candidate = observed + (*a as f32) * interval;
            let b_candidate = observed + (*b as f32) * interval;
            let a_valid = a_candidate.abs() <= MAX_ABS_DEALIASED_VELOCITY_MS;
            let b_valid = b_candidate.abs() <= MAX_ABS_DEALIASED_VELOCITY_MS;
            match (a_valid, b_valid) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => (a_candidate - reference)
                    .abs()
                    .partial_cmp(&(b_candidate - reference).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| (a - target_fold).abs().cmp(&(b - target_fold).abs())),
            }
        })
        .map(|fold| observed + (fold as f32) * interval)
        .filter(|candidate| candidate.abs() <= MAX_ABS_DEALIASED_VELOCITY_MS)
        .unwrap_or(observed)
}

fn weighted_median(values: &mut Vec<(f32, f32)>) -> Option<f32> {
    values.retain(|(value, weight)| value.is_finite() && weight.is_finite() && *weight > 0.0);
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let total = values.iter().map(|(_, weight)| *weight).sum::<f32>();
    if total <= 0.0 {
        return None;
    }
    let midpoint = total * 0.5;
    let mut cumulative = 0.0;
    for (value, weight) in values.iter() {
        cumulative += *weight;
        if cumulative >= midpoint {
            return Some(*value);
        }
    }
    values.last().map(|(value, _)| *value)
}

fn quality_gate_count(values: &[Vec<f32>], weights: &[Vec<f32>]) -> usize {
    values
        .iter()
        .enumerate()
        .map(|(row, row_values)| {
            let row_weights = weights.get(row).map(Vec::as_slice).unwrap_or(&[]);
            row_values
                .iter()
                .enumerate()
                .filter(|(col, value)| value.is_finite() && weight_at(row_weights, *col) > 0.0)
                .count()
        })
        .sum()
}

fn is_low_alias_burden(
    score: DealiasContinuityScore,
    quality_gate_count: usize,
    nyquist: f32,
) -> bool {
    if quality_gate_count == 0 || !nyquist.is_finite() || nyquist <= 0.0 {
        return false;
    }

    let fold_fraction = score.fold_like_jumps as f64 / quality_gate_count as f64;
    score.fold_like_jumps <= LOW_ALIAS_MAX_FOLD_LIKE_JUMPS
        && score.severe_jumps <= LOW_ALIAS_MAX_SEVERE_JUMPS
        && fold_fraction <= LOW_ALIAS_MAX_FOLD_FRACTION
        && score.max_abs_jump_ms <= nyquist * LOW_ALIAS_MAX_JUMP_NYQUIST_MULTIPLE + f32::EPSILON
}

fn output_changed_gate_count(
    sweep: &Level2Sweep,
    grid: &VelocityGrid,
    corrected: &[Vec<f32>],
) -> usize {
    sweep
        .radials
        .iter()
        .enumerate()
        .filter_map(|(radial_index, radial)| {
            let row = grid.radial_to_row.get(radial_index).copied().flatten()?;
            let corrected_row = corrected.get(row)?;
            let moment = velocity_moment(radial)?;
            Some(
                moment
                    .data
                    .iter()
                    .enumerate()
                    .filter(|(gate, observed)| {
                        let corrected = corrected_row.get(*gate).copied().unwrap_or(f32::NAN);
                        observed.is_finite()
                            && (!corrected.is_finite() || (*observed - corrected).abs() > 0.001)
                    })
                    .count(),
            )
        })
        .sum()
}

fn continuity_score(
    values: &[Vec<f32>],
    weights: &[Vec<f32>],
    nyquist: f32,
) -> DealiasContinuityScore {
    let mut score = DealiasContinuityScore {
        fold_like_jumps: 0,
        severe_jumps: 0,
        max_abs_jump_ms: 0.0,
    };
    let fold_threshold = nyquist;
    let severe_threshold = nyquist * 1.5;

    for (row, row_values) in values.iter().enumerate() {
        let row_weights = weights.get(row).map(Vec::as_slice).unwrap_or(&[]);
        for col in 1..row_values.len() {
            update_continuity_score(
                row_values[col - 1],
                weight_at(row_weights, col - 1),
                row_values[col],
                weight_at(row_weights, col),
                fold_threshold,
                severe_threshold,
                &mut score,
            );
        }
    }

    for row in 0..values.len() {
        let next_row = if row + 1 == values.len() { 0 } else { row + 1 };
        if row == next_row {
            continue;
        }
        let current_weights = weights.get(row).map(Vec::as_slice).unwrap_or(&[]);
        let next_weights = weights.get(next_row).map(Vec::as_slice).unwrap_or(&[]);
        let cols = values[row].len().min(values[next_row].len());
        for col in 0..cols {
            update_continuity_score(
                values[row][col],
                weight_at(current_weights, col),
                values[next_row][col],
                weight_at(next_weights, col),
                fold_threshold,
                severe_threshold,
                &mut score,
            );
        }
    }

    score
}

fn update_continuity_score(
    a: f32,
    a_weight: f32,
    b: f32,
    b_weight: f32,
    fold_threshold: f32,
    severe_threshold: f32,
    score: &mut DealiasContinuityScore,
) {
    if !a.is_finite() || !b.is_finite() || a_weight <= 0.0 || b_weight <= 0.0 {
        return;
    }

    let jump = (a - b).abs();
    score.max_abs_jump_ms = score.max_abs_jump_ms.max(jump);
    if jump > fold_threshold {
        score.fold_like_jumps += 1;
    }
    if jump > severe_threshold {
        score.severe_jumps += 1;
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
        let weights = vec![1.0; observed.len()];
        let corrected = dealias_radial(&observed, &weights, nyquist, None, None);

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
        let weights = vec![vec![1.0; 3]; 3];
        let mut corrected = observed.clone();
        sweep_refine_grid(&observed, &weights, &mut corrected, nyquist, 2);

        assert_close(corrected[1][2], 16.0);
    }

    #[test]
    fn unfold_to_reference_limits_runaway_velocity() {
        let corrected = unfold_to_reference(-20.0, 320.0, 26.28);

        assert!(corrected.abs() <= MAX_ABS_DEALIASED_VELOCITY_MS);
        assert_close(corrected, 85.12);
    }

    #[test]
    fn continuity_score_rejects_worse_output() {
        let baseline = DealiasContinuityScore {
            fold_like_jumps: 3,
            severe_jumps: 1,
            max_abs_jump_ms: 52.0,
        };
        let worse = DealiasContinuityScore {
            fold_like_jumps: 4,
            severe_jumps: 2,
            max_abs_jump_ms: 80.0,
        };

        assert!(!worse.is_no_worse_than(baseline));
    }

    #[test]
    fn continuity_score_allows_large_improvement_with_limited_max_regression() {
        let baseline = DealiasContinuityScore {
            fold_like_jumps: 1345,
            severe_jumps: 1087,
            max_abs_jump_ms: 52.0,
        };
        let high_shear_candidate = DealiasContinuityScore {
            fold_like_jumps: 154,
            severe_jumps: 90,
            max_abs_jump_ms: 77.24,
        };
        let runaway_candidate = DealiasContinuityScore {
            fold_like_jumps: 2710,
            severe_jumps: 2607,
            max_abs_jump_ms: 167.72,
        };

        assert!(high_shear_candidate.is_acceptable_candidate(baseline, 26.12));
        assert!(!runaway_candidate.is_acceptable_candidate(baseline, 26.12));
    }

    #[test]
    fn continuity_score_allows_ksjt_low_tilt_candidate_below_gate_limit() {
        let baseline = DealiasContinuityScore {
            fold_like_jumps: 5580,
            severe_jumps: 4258,
            max_abs_jump_ms: 48.0,
        };
        let ksjt_low_tilt_candidate = DealiasContinuityScore {
            fold_like_jumps: 589,
            severe_jumps: 56,
            max_abs_jump_ms: 95.5,
        };
        let runaway_candidate = DealiasContinuityScore {
            fold_like_jumps: 589,
            severe_jumps: 56,
            max_abs_jump_ms: 120.5,
        };

        assert!(ksjt_low_tilt_candidate.is_acceptable_candidate(baseline, 24.0));
        assert!(!runaway_candidate.is_acceptable_candidate(baseline, 24.0));
    }

    #[test]
    fn staged_extreme_jump_cleanup_masks_unresolved_gate_pairs() {
        let mut candidate = vec![
            vec![10.0, 12.0, 14.0],
            vec![11.0, 135.0, 15.0],
            vec![12.0, 13.0, 16.0],
        ];
        let weights = vec![vec![1.0; 3]; 3];

        mask_staged_extreme_jump_pairs(&mut candidate, &weights, 24.0);
        let score = continuity_score(&candidate, &weights, 24.0);

        assert!(!candidate[1][1].is_finite());
        assert!(score.max_abs_jump_ms <= 5.0);
        assert_eq!(score.severe_jumps, 0);
    }

    #[test]
    fn low_alias_burden_skips_full_candidate_generation() {
        let quiet_baseline = DealiasContinuityScore {
            fold_like_jumps: 222,
            severe_jumps: 23,
            max_abs_jump_ms: 62.5,
        };
        let active_baseline = DealiasContinuityScore {
            fold_like_jumps: 1345,
            severe_jumps: 1087,
            max_abs_jump_ms: 52.0,
        };

        assert!(is_low_alias_burden(quiet_baseline, 222_777, 33.04));
        assert!(!is_low_alias_burden(active_baseline, 130_064, 26.12));
    }

    #[test]
    fn region_continuity_uses_neighbor_seed_to_unfold_folded_radial() {
        let nyquist = 15.0;
        let observed = vec![
            vec![10.0, 12.0, 14.0],
            vec![-14.0, -12.0, -10.0],
            vec![11.0, 13.0, 15.0],
        ];
        let weights = vec![vec![1.0; 3]; 3];
        let corrected = region_continuity_grid(&observed, &weights, nyquist);

        assert_close(corrected[1][0], 16.0);
        assert_close(corrected[1][1], 18.0);
        assert_close(corrected[1][2], 20.0);
    }

    #[test]
    fn staged_continuity_keeps_best_candidate_score() {
        let nyquist = 15.0;
        let observed = vec![
            vec![10.0, 12.0, 14.0],
            vec![-14.0, -12.0, -10.0],
            vec![11.0, 13.0, 15.0],
        ];
        let weights = vec![vec![1.0; 3]; 3];
        let radial = radial_continuity_grid(&observed, &weights, nyquist);
        let staged = staged_continuity_grid(&observed, &weights, nyquist);

        assert_close(staged[1][0], 16.0);
        assert_close(staged[1][1], 18.0);
        assert_close(staged[1][2], 20.0);
        assert!(
            continuity_score_is_better(
                continuity_score(&staged, &weights, nyquist),
                continuity_score(&radial, &weights, nyquist)
            ) || continuity_score(&staged, &weights, nyquist)
                == continuity_score(&radial, &weights, nyquist)
        );
    }

    #[test]
    fn velocity_sweep_reports_staged_method() {
        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: Some(15.0),
            radials: vec![synthetic_radial(0.0), synthetic_radial(1.0)],
        };

        let (_, report) =
            dealias_velocity_sweep_with_report(&sweep, DealiasMethod::StagedContinuity);

        assert_eq!(report.method, "staged");
        assert!(report.attempted);
        assert!(report.changed_gate_count > 0);
    }

    #[test]
    fn velocity_sweep_replaces_velocity_moment_only() {
        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: Some(15.0),
            radials: vec![synthetic_radial(0.0), synthetic_radial(1.0)],
        };

        let (out, _) = dealias_velocity_sweep_with_policy(
            &sweep,
            DealiasMethod::SweepContinuity,
            DealiasAcceptancePolicy::ForceCandidate,
        );
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

    #[test]
    fn velocity_sweep_reports_candidate_diagnostics() {
        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: Some(15.0),
            radials: vec![synthetic_radial(0.0), synthetic_radial(1.0)],
        };

        let (_, report) =
            dealias_velocity_sweep_with_report(&sweep, DealiasMethod::SweepContinuity);

        assert!(report.attempted);
        assert!(report.accepted);
        assert_eq!(report.decision, DealiasDecision::CandidateAccepted);
        assert_eq!(report.nyquist_ms, Some(15.0));
        assert!(report.quality_gate_count > 0);
        assert!(report.changed_gate_count > 0);
        assert!(report.original_score.is_some());
        assert!(report.candidate_score.is_some());
    }

    #[test]
    fn velocity_sweep_masks_low_quality_dealias_gates() {
        let mut radial = synthetic_radial(0.0);
        radial.moments.push(MomentData {
            product: RadarProduct::SpectrumWidth,
            gate_count: 6,
            first_gate_range: 0,
            gate_size: 250,
            data_word_size: None,
            scale: None,
            offset: None,
            raw_data: None,
            data: vec![1.0, 1.0, 30.0, 1.0, 1.0, 1.0],
        });
        radial
            .moments
            .iter_mut()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap()
            .data = vec![45.0, 45.0, 45.0, -5.0, 45.0, 45.0];
        radial
            .moments
            .iter_mut()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap()
            .gate_size = 250;
        radial
            .moments
            .iter_mut()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap()
            .gate_count = 6;

        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: Some(15.0),
            radials: vec![radial],
        };

        let (out, _) = dealias_velocity_sweep_with_policy(
            &sweep,
            DealiasMethod::SweepContinuity,
            DealiasAcceptancePolicy::ForceCandidate,
        );
        let velocity = out.radials[0]
            .moments
            .iter()
            .find(|moment| moment.product == RadarProduct::Velocity)
            .unwrap();

        assert!(
            velocity.data[2].is_nan(),
            "high spectrum width gate should be masked"
        );
        assert!(
            velocity.data[3].is_nan(),
            "low reflectivity gate should be masked"
        );
        assert_close(velocity.data[0], 10.0);
    }

    #[test]
    fn velocity_quality_mask_filters_bad_gates_without_dealiasing() {
        let mut radial = synthetic_radial(0.0);
        radial.moments.push(MomentData {
            product: RadarProduct::SpectrumWidth,
            gate_count: 6,
            first_gate_range: 0,
            gate_size: 250,
            data_word_size: None,
            scale: None,
            offset: None,
            raw_data: None,
            data: vec![1.0, 1.0, 30.0, 1.0, 1.0, 1.0],
        });
        radial
            .moments
            .iter_mut()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap()
            .data = vec![45.0, 45.0, 45.0, -5.0, 45.0, 45.0];
        radial
            .moments
            .iter_mut()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap()
            .gate_size = 250;
        radial
            .moments
            .iter_mut()
            .find(|moment| moment.product == RadarProduct::Reflectivity)
            .unwrap()
            .gate_count = 6;

        let sweep = Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: Some(15.0),
            radials: vec![radial],
        };

        let (out, report) = mask_velocity_sweep_quality(&sweep);
        let velocity = out.radials[0]
            .moments
            .iter()
            .find(|moment| moment.product == RadarProduct::Velocity)
            .unwrap();

        assert_eq!(report.finite_gate_count, 6);
        assert_eq!(report.masked_gate_count, 2);
        assert!(velocity.data[2].is_nan());
        assert!(velocity.data[3].is_nan());
        assert_close(velocity.data[0], 10.0);
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
                    data_word_size: None,
                    scale: None,
                    offset: None,
                    raw_data: None,
                    data: vec![10.0, 12.0, 14.0, -14.0, -12.0, -10.0],
                },
                MomentData {
                    product: RadarProduct::Reflectivity,
                    gate_count: 1,
                    first_gate_range: 0,
                    gate_size: 1_000,
                    data_word_size: None,
                    scale: None,
                    offset: None,
                    raw_data: None,
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
