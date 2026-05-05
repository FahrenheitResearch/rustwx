//! CUDA `tangential_component` vs metrust's `kinematics::tangential_component`.

use metrust::calc::kinematics::{
    tangential_component as cpu_tangential, unit_vectors_from_cross_section,
};
use rustwx_cuda_core::global;
use rustwx_cuda_wind::tangential_component;

const TOL: f64 = 1e-10;

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");

    let n = 8192;
    let mut u = Vec::with_capacity(n);
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        u.push(-25.0 + f * 50.0);
        v.push(-30.0 + f * 60.0);
    }
    let start = (35.0, -100.0);
    let end = (45.0, -85.0);
    let ((te, tn), _) = unit_vectors_from_cross_section(start, end);
    let tx = vec![te; n];
    let ty = vec![tn; n];

    let gpu = tangential_component::host(&ctx, &u, &v, &tx, &ty).expect("kernel");
    let cpu = cpu_tangential(&u, &v, start, end);

    let mut max_abs = 0.0;
    for i in 0..n {
        let abs = (gpu[i] - cpu[i]).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(abs < TOL, "abs={abs:e} at i={i}");
    }
    eprintln!("max_abs={max_abs:e}");
}
