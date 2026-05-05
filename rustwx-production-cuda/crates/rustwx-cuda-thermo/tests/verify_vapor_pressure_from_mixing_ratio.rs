//! Numerical agreement test: CUDA `vapor_pressure_from_mixing_ratio` vs
//! an inline CPU reference (no wx-math equivalent for this exact form).

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::vapor_pressure_from_mixing_ratio;

const EPS: f64 = 0.6219569100577033;
const TOL: f64 = 1e-10;

fn cpu_e(w: f64, p: f64) -> f64 {
    w * p / (EPS + w)
}

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut w = Vec::with_capacity(n);
    let mut p = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        w.push(0.0001 + f * 0.024);
        p.push(100.0 + f * 950.0);
    }
    (w, p)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (w, p) = synthetic_profile(8192);
    let gpu = vapor_pressure_from_mixing_ratio::host(&ctx, &w, &p)
        .expect("kernel");
    assert_eq!(gpu.len(), w.len());

    let mut max_abs = 0.0;
    for i in 0..w.len() {
        let cpu = cpu_e(w[i], p[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
