//! Numerical agreement test: CUDA hot_dry_windy vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::hot_dry_windy;
use wx_math::composite::hot_dry_windy as cpu_hdw;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut t_c = Vec::with_capacity(n);
    let mut rh = Vec::with_capacity(n);
    let mut wspd_ms = Vec::with_capacity(n);
    let mut vpd = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t_c.push(0.0 + f * 45.0); // 0..45 C
        rh.push(5.0 + f * 90.0); // 5..95 %
        wspd_ms.push(1.0 + f * 25.0);
        // Alternate user-supplied VPD vs internal computation.
        vpd.push(if i % 2 == 0 { 0.0 } else { 5.0 + f * 30.0 });
    }
    (t_c, rh, wspd_ms, vpd)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t_c, rh, wspd_ms, vpd) = synthetic(8192);
    let gpu = hot_dry_windy::host(&ctx, &t_c, &rh, &wspd_ms, &vpd).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..t_c.len() {
        let cpu = cpu_hdw(t_c[i], rh[i], wspd_ms[i], vpd[i]);
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
