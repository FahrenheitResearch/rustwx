//! CUDA `wind_speed` vs the wx-math CPU reference.
//! `#[ignore]`d by default so CI without a GPU stays green.

use rustwx_cuda_core::global;
use rustwx_cuda_wind::wind_speed;
use wx_math::dynamics::wind_speed as cpu_wind_speed;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut u = Vec::with_capacity(n);
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        u.push(-25.0 + f * 50.0);
        v.push(-30.0 + f * 60.0);
    }
    (u, v)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v) = synthetic(8192);
    let gpu = wind_speed::host(&ctx, &u, &v).expect("kernel");
    assert_eq!(gpu.len(), u.len());

    let cpu = cpu_wind_speed(&u, &v);
    let mut max_abs = 0.0;
    for i in 0..u.len() {
        let abs = (gpu[i] - cpu[i]).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "abs={abs:e} at i={i} gpu={} cpu={}",
            gpu[i],
            cpu[i]
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
