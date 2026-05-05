//! Numerical agreement: CUDA `relative_humidity_from_mixing_ratio` vs metrust.
//! Kernel input mixing ratio is kg/kg; metrust takes g/kg.

use metrust::calc::thermo::relative_humidity_from_mixing_ratio as cpu_rh;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::relative_humidity_from_mixing_ratio;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(400.0 + f * 600.0);
        t.push(-20.0 + f * 50.0);
        w.push(0.0001 + f * 0.018);
    }
    (p, t, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t, w) = synthetic(8192);
    let gpu = relative_humidity_from_mixing_ratio::host(&ctx, &p, &t, &w).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        // metrust takes g/kg
        let cpu = cpu_rh(p[i], t[i], w[i] * 1000.0);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} p={} t={} w={}", gpu[i], cpu, abs, p[i], t[i], w[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
