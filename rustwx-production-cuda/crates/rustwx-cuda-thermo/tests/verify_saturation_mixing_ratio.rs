//! Numerical agreement test: CUDA `saturation_mixing_ratio` (kg/kg) vs
//! `wx_math::thermo::saturation_mixing_ratio` (g/kg).

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::saturation_mixing_ratio;
use wx_math::thermo::saturation_mixing_ratio as cpu_ws_gkg;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(150.0 + f * 900.0);
        t.push(-50.0 + f * 90.0);
    }
    (p, t)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t) = synthetic_profile(8192);
    let gpu = saturation_mixing_ratio::host(&ctx, &p, &t).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_ws_gkg(p[i], t[i]) / 1000.0;
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i} (p={}, t={})",
            gpu[i],
            cpu,
            abs,
            p[i],
            t[i]
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
