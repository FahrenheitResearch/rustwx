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

    let (_, report) = dealias_velocity_sweep_with_report(&sweep, DealiasMethod::StagedContinuity);

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

    let (_, report) = dealias_velocity_sweep_with_report(&sweep, DealiasMethod::SweepContinuity);

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
