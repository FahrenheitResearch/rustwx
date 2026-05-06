//! Numerical agreement test: CUDA STP vs metrust CPU reference.

use metrust::calc::severe::significant_tornado_parameter as cpu_stp;
use rustwx_cuda_core::global;
use rustwx_cuda_severe::stp;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut cape = Vec::with_capacity(n);
    let mut lcl = Vec::with_capacity(n);
    let mut srh = Vec::with_capacity(n);
    let mut shear = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        cape.push(0.0 + f * 5000.0); // 0..5000 J/kg
        lcl.push(500.0 + f * 2500.0); // 500..3000 m
        srh.push(0.0 + f * 600.0); // 0..600
        shear.push(0.0 + f * 40.0); // 0..40 m/s (covers <12.5 and >30 branches)
    }
    (cape, lcl, srh, shear)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (cape, lcl, srh, shear) = synthetic(8192);
    let gpu = stp::host(&ctx, &cape, &lcl, &srh, &shear).expect("kernel");
    assert_eq!(gpu.len(), cape.len());

    let mut max_abs = 0.0;
    for i in 0..cape.len() {
        let cpu = cpu_stp(cape[i], lcl[i], srh[i], shear[i]);
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
