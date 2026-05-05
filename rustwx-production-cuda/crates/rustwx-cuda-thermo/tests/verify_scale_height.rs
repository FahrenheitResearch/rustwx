//! Numerical agreement test: CUDA `scale_height` vs
//! `wx_math::thermo::scale_height`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::scale_height;
use wx_math::thermo::scale_height as cpu_h;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(200.0 + f * 110.0);
    }
    t
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let t = synthetic_profile(8192);
    let gpu = scale_height::host(&ctx, &t).expect("kernel");
    assert_eq!(gpu.len(), t.len());

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_h(t[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
