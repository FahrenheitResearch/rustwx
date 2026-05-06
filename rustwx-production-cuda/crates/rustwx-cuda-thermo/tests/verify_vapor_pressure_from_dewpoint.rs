//! Numerical agreement test: CUDA `vapor_pressure_from_dewpoint` vs
//! `wx_math::thermo::vapor_pressure_from_dewpoint`.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::vapor_pressure_from_dewpoint;
use wx_math::thermo::vapor_pressure_from_dewpoint as cpu_vp;

const TOL: f64 = 1e-10;

fn synthetic_profile(n: usize) -> Vec<f64> {
    let mut td = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        td.push(-70.0 + f * 100.0);
    }
    td
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let td = synthetic_profile(8192);
    let gpu = vapor_pressure_from_dewpoint::host(&ctx, &td).expect("kernel");
    assert_eq!(gpu.len(), td.len());

    let mut max_abs = 0.0;
    for i in 0..td.len() {
        let cpu = cpu_vp(td[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} diff={:e} at i={i}",
            gpu[i],
            cpu,
            abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
