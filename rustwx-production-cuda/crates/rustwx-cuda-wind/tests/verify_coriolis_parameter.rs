//! CUDA `coriolis_parameter` vs the wx-math CPU reference (per-element).

use rustwx_cuda_core::global;
use rustwx_cuda_wind::coriolis_parameter;
use wx_math::dynamics::coriolis_parameter as cpu_coriolis;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| {
            let f = (i as f64) / ((n.max(2) - 1) as f64);
            -90.0 + f * 180.0
        })
        .collect()
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let lats = synthetic(4096);
    let gpu = coriolis_parameter::host(&ctx, &lats).expect("kernel");
    assert_eq!(gpu.len(), lats.len());

    let mut max_abs = 0.0;
    for i in 0..lats.len() {
        let cpu = cpu_coriolis(lats[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "abs={abs:e} at i={i} lat={} gpu={} cpu={}",
            lats[i],
            gpu[i],
            cpu
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
