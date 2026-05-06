//! Numerical agreement test: CUDA SHIP vs wx_math grid CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::ship;
use wx_math::composite::significant_hail_parameter as cpu_ship;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut cape = Vec::with_capacity(n);
    let mut shear = Vec::with_capacity(n);
    let mut t500 = Vec::with_capacity(n);
    let mut lr = Vec::with_capacity(n);
    let mut mr = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        cape.push(0.0 + f * 5000.0); // 0..5000 J/kg, covers <1300 branch
        shear.push(5.0 + f * 35.0); // 5..40 m/s
        t500.push(-30.0 + f * 25.0); // -30..-5 C (negative as expected)
        lr.push(4.0 + f * 5.0); // 4..9 C/km
        mr.push(2.0 + f * 16.0); // 2..18 g/kg
    }
    (cape, shear, t500, lr, mr)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (cape, shear, t500, lr, mr) = synthetic(8192);
    let gpu = ship::host(&ctx, &cape, &shear, &t500, &lr, &mr).expect("kernel");
    assert_eq!(gpu.len(), cape.len());

    // wx-math grid version takes (cape, shear06, t500, lr_700_500, mr, nx, ny)
    let cpu = cpu_ship(&cape, &shear, &t500, &lr, &mr, cape.len(), 1);

    let mut max_abs = 0.0;
    for i in 0..cape.len() {
        let abs = (gpu[i] - cpu[i]).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i],
            cpu[i],
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
