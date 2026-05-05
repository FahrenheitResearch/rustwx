//! Numerical agreement test: CUDA `saturation_vapor_pressure` vs
//! `wx_math::thermo::saturation_vapor_pressure`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::saturation_vapor_pressure;
use wx_math::thermo::saturation_vapor_pressure as cpu_es;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(-70.0 + f * 110.0); // -70 .. 40 C
    }
    t
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let t = synthetic_profile(8192);
    let gpu = saturation_vapor_pressure::host(&ctx, &t).expect("kernel");
    assert_eq!(gpu.len(), t.len());

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_es(t[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} (t={})", gpu[i], cpu, abs, t[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
