//! Numerical agreement test: CUDA `potential_temperature` vs the metrust CPU reference.
//!
//! Run with `cargo test -p rustwx-cuda-thermo --release` on a CUDA-capable host.
//! `#[ignore]`d by default so CI without a GPU stays green.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::potential_temperature;
use wx_math::thermo::potential_temperature as cpu_theta;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    // Pressures from 100 hPa (high troposphere) to 1050 hPa (deep low).
    // Temperatures from -75 C (tropopause) to +35 C (surface).
    let mut p = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        p.push(100.0 + f * 950.0);
        t.push(-75.0 + f * 110.0);
    }
    (p, t)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (p, t) = synthetic_profile(8192);
    let gpu = potential_temperature::host(&ctx, &p, &t).expect("kernel");
    assert_eq!(gpu.len(), p.len());

    let mut max_abs = 0.0;
    let mut max_rel = 0.0;
    for i in 0..p.len() {
        let cpu = cpu_theta(p[i], t[i]);
        let abs = (gpu[i] - cpu).abs();
        let rel = abs / cpu.abs().max(1.0);
        if abs > max_abs {
            max_abs = abs;
        }
        if rel > max_rel {
            max_rel = rel;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} rel_diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs,
            rel
        );
    }
    eprintln!("max_abs={max_abs:e} max_rel={max_rel:e}");
}
