//! Numerical agreement: CUDA `vertical_velocity_pressure` vs metrust.

use metrust::calc::thermo::vertical_velocity_pressure as cpu_omega;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::vertical_velocity_pressure;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut w = Vec::with_capacity(n);
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        w.push(-3.0 + f * 6.0);
        p.push(200.0 + f * 800.0);
        t.push(-50.0 + f * 80.0);
    }
    (w, p, t)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (w, p, t) = synthetic(8192);
    let gpu = vertical_velocity_pressure::host(&ctx, &w, &p, &t).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..w.len() {
        let cpu = cpu_omega(w[i], p[i], t[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
