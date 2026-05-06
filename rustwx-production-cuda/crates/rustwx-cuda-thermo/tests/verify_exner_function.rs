//! Numerical agreement test: CUDA `exner_function` vs
//! `wx_math::thermo::exner_function`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::exner_function;
use wx_math::thermo::exner_function as cpu_pi;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut p = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(50.0 + f * 1050.0);
    }
    p
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let p = synthetic_profile(8192);
    let gpu = exner_function::host(&ctx, &p).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_pi(p[i]);
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
