//! Numerical agreement test: CUDA `dewpoint` vs the wx-math CPU reference
//! (`dewpoint_from_vapor_pressure`).

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::dewpoint;
use wx_math::thermo::dewpoint_from_vapor_pressure as cpu_td;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut e = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        // Vapor pressures spanning 0.1 hPa (very dry / cold) to 60 hPa (hot wet).
        e.push(0.1 + f * 59.9);
    }
    e
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let e = synthetic_profile(8192);
    let gpu = dewpoint::host(&ctx, &e).expect("kernel");
    assert_eq!(gpu.len(), e.len());

    let mut max_abs = 0.0;
    for i in 0..e.len() {
        let cpu = cpu_td(e[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} (e={})", gpu[i], cpu, abs, e[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
