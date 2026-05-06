//! Numerical agreement test: CUDA `windchill` vs
//! `metrust::calc::atmo::windchill` (Celsius / m/s, formula always evaluated).

use metrust::calc::atmo::windchill as cpu_wc;
use rustwx_cuda_core::global;
use rustwx_cuda_thermo::windchill;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut t = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(-40.0 + f * 50.0); // -40 .. 10 C
        w.push(0.5 + f * 25.0); // 0.5 .. 25.5 m/s
    }
    (t, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, w) = synthetic_profile(8192);
    let gpu = windchill::host(&ctx, &t, &w).expect("kernel");
    assert_eq!(gpu.len(), t.len());

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_wc(t[i], w[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
