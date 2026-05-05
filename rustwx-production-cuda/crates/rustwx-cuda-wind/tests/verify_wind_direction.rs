//! CUDA `wind_direction` vs the wx-math CPU reference.
//!
//! Inputs avoid the calm-wind boundary (|wind| > 0.1 m/s) where the two
//! references use different thresholds (CUDA `u==0 && v==0`, CPU `|wind|<1e-10`).

use rustwx_cuda_core::global;
use rustwx_cuda_wind::wind_direction;
use wx_math::dynamics::wind_direction as cpu_wind_direction;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut u = Vec::with_capacity(n);
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        // Always away from (0, 0): radius in [1, 30] m/s, angle sweeps full circle.
        let r = 1.0 + f * 29.0;
        let theta = f * std::f64::consts::TAU;
        u.push(r * theta.cos());
        v.push(r * theta.sin());
    }
    (u, v)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v) = synthetic(8192);
    let gpu = wind_direction::host(&ctx, &u, &v).expect("kernel");
    assert_eq!(gpu.len(), u.len());

    let cpu = cpu_wind_direction(&u, &v);
    let mut max_abs = 0.0;
    for i in 0..u.len() {
        // Direction is on a circle: handle wraparound at 0/360.
        let a = gpu[i];
        let b = cpu[i];
        let mut d = (a - b).abs();
        if d > 180.0 {
            d = 360.0 - d;
        }
        if d > max_abs {
            max_abs = d;
        }
        assert!(d < TOL, "d={d:e} at i={i} gpu={a} cpu={b}");
    }
    eprintln!("max_abs={max_abs:e}");
}
