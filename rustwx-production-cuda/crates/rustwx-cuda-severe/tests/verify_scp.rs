//! Numerical agreement test: CUDA SCP vs metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::scp;
use metrust::calc::severe::supercell_composite_parameter as cpu_scp;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut mucape = Vec::with_capacity(n);
    let mut srh = Vec::with_capacity(n);
    let mut shear = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        mucape.push(0.0 + f * 5000.0);   // 0..5000 J/kg
        srh.push(0.0 + f * 800.0);       // 0..800
        shear.push(0.0 + f * 30.0);      // 0..30 m/s, covers <10 and >20 branches
    }
    (mucape, srh, shear)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (mucape, srh, shear) = synthetic(8192);
    let gpu = scp::host(&ctx, &mucape, &srh, &shear).expect("kernel");
    assert_eq!(gpu.len(), mucape.len());

    let mut max_abs = 0.0;
    for i in 0..mucape.len() {
        let cpu = cpu_scp(mucape[i], srh[i], shear[i]);
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
