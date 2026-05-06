//! Smoke test: CUDA `bunkers_storm_motion` launches and produces finite results.
//!
//! DEFER: the kernel uses height-weighted 0-6 km mean wind and a simple
//! (top - bottom) bulk shear, while `metrust::calc::wind::bunkers_storm_motion`
//! uses a pressure-weighted mean wind plus mean(5.5-6 km) - mean(0-0.5 km)
//! shear. The vectors disagree by O(m/s), so a 1e-10 parity test is
//! infeasible. See `DIVERGENT_KERNELS.md`.

use rustwx_cuda_core::global;
use rustwx_cuda_wind::bunkers_storm_motion;

const NCOLS: usize = 256;
const NLEVELS: usize = 30;

fn synthetic_profile() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut u = vec![0.0; NCOLS * NLEVELS];
    let mut v = vec![0.0; NCOLS * NLEVELS];
    let mut h = vec![0.0; NCOLS * NLEVELS];
    for c in 0..NCOLS {
        let phase = (c as f64) * 0.017;
        for k in 0..NLEVELS {
            let f = (k as f64) / ((NLEVELS - 1) as f64);
            let height = f * 15000.0;
            let z_km = height / 1000.0;
            let ui = 2.0 + 4.0 * z_km + 1.5 * (z_km + phase).sin();
            let vi = -3.0 + 0.8 * z_km + 1.2 * ((z_km + phase) * 0.7).cos();
            let idx = c * NLEVELS + k;
            u[idx] = ui;
            v[idx] = vi;
            h[idx] = height;
        }
    }
    (u, v, h)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn smoke() {
    let ctx = global().expect("init CUDA context");
    let (u, v, heights) = synthetic_profile();
    let (rm_u, rm_v, lm_u, lm_v, mw_u, mw_v) =
        bunkers_storm_motion::host(&ctx, &u, &v, &heights, NCOLS, NLEVELS).expect("kernel");
    assert_eq!(rm_u.len(), NCOLS);
    assert_eq!(rm_v.len(), NCOLS);
    assert_eq!(lm_u.len(), NCOLS);
    assert_eq!(lm_v.len(), NCOLS);
    assert_eq!(mw_u.len(), NCOLS);
    assert_eq!(mw_v.len(), NCOLS);

    // The right and left movers are mirror images about the mean wind:
    // (rm + lm) / 2 == mw, exactly (no rounding from the kernel side).
    for c in 0..NCOLS {
        for &x in &[rm_u[c], rm_v[c], lm_u[c], lm_v[c], mw_u[c], mw_v[c]] {
            assert!(x.is_finite(), "col={c} non-finite");
        }
        let mid_u = 0.5 * (rm_u[c] + lm_u[c]);
        let mid_v = 0.5 * (rm_v[c] + lm_v[c]);
        assert!(
            (mid_u - mw_u[c]).abs() < 1e-12 && (mid_v - mw_v[c]).abs() < 1e-12,
            "col={c}: midpoint of RM/LM should equal mean wind"
        );
    }
}
