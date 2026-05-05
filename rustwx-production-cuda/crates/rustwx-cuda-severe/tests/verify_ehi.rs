//! Numerical agreement test: CUDA EHI vs wx_math grid CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::ehi;
use wx_math::composite::compute_ehi as cpu_ehi;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut cape = Vec::with_capacity(n);
    let mut srh = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        cape.push(0.0 + f * 5000.0);
        srh.push(0.0 + f * 800.0);
    }
    (cape, srh)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (cape, srh) = synthetic(8192);
    let gpu = ehi::host(&ctx, &cape, &srh).expect("kernel");
    let cpu = cpu_ehi(&cape, &srh);

    let mut max_abs = 0.0;
    for i in 0..cape.len() {
        let abs = (gpu[i] - cpu[i]).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i], cpu[i], abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
