//! Numerical agreement test: CUDA `dry_lapse` (elementwise) vs an inline
//! CPU reference matching the formula. wx-math has a slice-based
//! `dry_lapse(p_profile, t_surface)`; the kernel is its per-element form.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::dry_lapse;

const ZEROCNK: f64 = 273.15;
const ROCP: f64 = 0.2857142857142857;
const TOL: f64 = 1e-10;

fn cpu_t(p: f64, p_ref: f64, t_sfc_c: f64) -> f64 {
    let t_k = t_sfc_c + ZEROCNK;
    t_k * (p / p_ref).powf(ROCP) - ZEROCNK
}

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut p_ref = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(200.0 + f * 800.0);
        p_ref.push(950.0 + (1.0 - f) * 100.0); // 950..1050
        ts.push(-10.0 + f * 40.0); // -10 .. 30 C
    }
    (p, p_ref, ts)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, pr, ts) = synthetic_profile(8192);
    let gpu = dry_lapse::host(&ctx, &p, &pr, &ts).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_t(p[i], pr[i], ts[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
