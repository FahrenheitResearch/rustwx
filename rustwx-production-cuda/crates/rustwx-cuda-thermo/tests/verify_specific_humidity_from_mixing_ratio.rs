//! Numerical agreement test: CUDA `specific_humidity_from_mixing_ratio` vs
//! `wx_math::thermo::specific_humidity` (which takes w in g/kg).

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::specific_humidity_from_mixing_ratio;
use wx_math::thermo::specific_humidity as cpu_q;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        w.push(0.0001 + f * 0.024); // kg/kg
    }
    w
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let w = synthetic_profile(8192);
    let gpu = specific_humidity_from_mixing_ratio::host(&ctx, &w).expect("kernel");
    assert_eq!(gpu.len(), w.len());

    let mut max_abs = 0.0;
    for i in 0..w.len() {
        let cpu = cpu_q(1000.0, w[i] * 1000.0); // p unused; pass w in g/kg
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
