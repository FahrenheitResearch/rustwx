//! Numerical agreement test: CUDA `bulk_shear` vs `metrust::calc::wind::bulk_shear`.
//!
//! Run with `cargo test -p rustwx-cuda-wind --release` on a CUDA-capable host.
//! `#[ignore]`d by default so CI without a GPU stays green.

use metrust::calc::wind::bulk_shear as cpu_bulk_shear;
use rustwx_cuda_core::global;
use rustwx_cuda_wind::bulk_shear;

const TOL: f64 = 1e-10;

const NCOLS: usize = 256;
const NLEVELS: usize = 30;

/// Build a synthetic 256-column x 30-level (u, v, height) profile, row-major.
/// Heights span 0..15000 m AGL identically per column; each column gets a
/// slightly different sinusoidal hodograph.
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
            // Hodograph: u shears positively with height, v turns sinusoidally.
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
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v, heights) = synthetic_profile();
    let bottom = 0.0;
    let top = 6000.0;

    let (su_gpu, sv_gpu) =
        bulk_shear::host(&ctx, &u, &v, &heights, NCOLS, NLEVELS, bottom, top).expect("kernel");
    assert_eq!(su_gpu.len(), NCOLS);
    assert_eq!(sv_gpu.len(), NCOLS);

    let mut max_abs = 0.0f64;
    for c in 0..NCOLS {
        let s = c * NLEVELS;
        let e = s + NLEVELS;
        let (su_cpu, sv_cpu) = cpu_bulk_shear(&u[s..e], &v[s..e], &heights[s..e], bottom, top);
        let du = (su_gpu[c] - su_cpu).abs();
        let dv = (sv_gpu[c] - sv_cpu).abs();
        let m = du.max(dv);
        if m > max_abs {
            max_abs = m;
        }
        assert!(
            du < TOL && dv < TOL,
            "col={c} gpu=({},{}) cpu=({},{}) du={du:e} dv={dv:e}",
            su_gpu[c],
            sv_gpu[c],
            su_cpu,
            sv_cpu
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
