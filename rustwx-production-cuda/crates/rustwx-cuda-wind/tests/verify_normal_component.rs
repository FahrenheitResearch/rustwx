//! CUDA `normal_component` vs metrust's `kinematics::normal_component`.
//!
//! The metrust API takes (start, end) lat/lon endpoints and computes a single
//! unit normal applied to every (u, v) pair. We mirror that by computing the
//! same unit normal once on the host and broadcasting it as the (nx, ny)
//! per-element inputs to the CUDA kernel.

use metrust::calc::kinematics::{normal_component as cpu_normal, unit_vectors_from_cross_section};
use rustwx_cuda_core::global;
use rustwx_cuda_wind::normal_component;

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
    let (_, (ne, nn)) = unit_vectors_from_cross_section(start, end);
    let nx = vec![ne; n];
    let ny = vec![nn; n];

    let gpu = normal_component::host(&ctx, &u, &v, &nx, &ny).expect("kernel");
    let cpu = cpu_normal(&u, &v, start, end);

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
