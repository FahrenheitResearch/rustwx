//! Numerical agreement test: CUDA boyden_index vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::boyden_index;
use wx_math::composite::boyden_index as cpu_bi;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut z1000 = Vec::with_capacity(n);
    let mut z700 = Vec::with_capacity(n);
    let mut t700 = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        z1000.push(0.0 + f * 200.0);          // 0..200 m
        z700.push(2800.0 + f * 400.0);        // 2800..3200 m
        t700.push(-10.0 + f * 20.0);          // -10..10 C
    }
    (z1000, z700, t700)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (z1000, z700, t700) = synthetic(8192);
    let gpu = boyden_index::host(&ctx, &z1000, &z700, &t700).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..z1000.len() {
        let cpu = cpu_bi(z1000[i], z700[i], t700[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i], cpu, abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
