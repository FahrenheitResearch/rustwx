//! Numerical agreement test: CUDA BRN vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::brn;
use wx_math::composite::bulk_richardson_number as cpu_brn;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut cape = Vec::with_capacity(n);
    let mut shear = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        cape.push(100.0 + f * 4900.0); // 100..5000 J/kg
                                       // 0.5 * shear^2 must be >= 0.1 for finite output (matches CPU NaN branch).
                                       // shear in [1.5, 40] m/s keeps denom >= 1.125.
        shear.push(1.5 + f * 38.5);
    }
    (cape, shear)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (cape, shear) = synthetic(8192);
    let gpu = brn::host(&ctx, &cape, &shear).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..cape.len() {
        let cpu = cpu_brn(cape[i], shear[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
