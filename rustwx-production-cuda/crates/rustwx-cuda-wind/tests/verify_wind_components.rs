//! CUDA `wind_components` vs the wx-math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_wind::wind_components;
use wx_math::dynamics::wind_components as cpu_wind_components;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut s = Vec::with_capacity(n);
    let mut d = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        s.push(0.5 + f * 39.5);
        d.push(f * 360.0);
    }
    (s, d)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (s, d) = synthetic(8192);
    let (gpu_u, gpu_v) = wind_components::host(&ctx, &s, &d).expect("kernel");
    let (cpu_u, cpu_v) = cpu_wind_components(&s, &d);
    assert_eq!(gpu_u.len(), s.len());
    assert_eq!(gpu_v.len(), s.len());

    let mut max_abs = 0.0;
    for i in 0..s.len() {
        let du = (gpu_u[i] - cpu_u[i]).abs();
        let dv = (gpu_v[i] - cpu_v[i]).abs();
        let m = du.max(dv);
        if m > max_abs {
            max_abs = m;
        }
        assert!(du < TOL && dv < TOL, "du={du:e} dv={dv:e} at i={i}");
    }
    eprintln!("max_abs={max_abs:e}");
}
