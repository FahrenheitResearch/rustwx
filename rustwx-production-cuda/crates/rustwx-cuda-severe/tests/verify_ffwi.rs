//! Numerical agreement test: CUDA FFWI vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::ffwi;
use wx_math::composite::fosberg_fire_weather_index as cpu_ffwi;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut t_f = Vec::with_capacity(n);
    let mut rh = Vec::with_capacity(n);
    let mut wspd_mph = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t_f.push(40.0 + f * 60.0); // 40..100 F
        rh.push(5.0 + f * 90.0); // 5..95 % (covers all three EMC branches)
        wspd_mph.push(2.0 + f * 40.0); // 2..42 mph
    }
    (t_f, rh, wspd_mph)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t_f, rh, wspd_mph) = synthetic(8192);
    let gpu = ffwi::host(&ctx, &t_f, &rh, &wspd_mph).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..t_f.len() {
        let cpu = cpu_ffwi(t_f[i], rh[i], wspd_mph[i]);
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
