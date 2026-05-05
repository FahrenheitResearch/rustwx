//! Numerical agreement test: CUDA sweat_index vs wx_math CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_severe::sweat_index;
use wx_math::composite::sweat_index as cpu_sweat;

const TOL: f64 = 1e-10;

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut tt = Vec::with_capacity(n);
    let mut td850 = Vec::with_capacity(n);
    let mut wspd850 = Vec::with_capacity(n);
    let mut wdir850 = Vec::with_capacity(n);
    let mut wspd500 = Vec::with_capacity(n);
    let mut wdir500 = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        tt.push(35.0 + f * 30.0);             // 35..65 covers <49 and >49
        td850.push(-5.0 + f * 25.0);          // -5..20 covers <0 and >0
        // Sweep through both branches of the directional shear test.
        wspd850.push(5.0 + f * 35.0);         // 5..40 kts (covers <15)
        wdir850.push(120.0 + f * 140.0);      // 120..260 (covers in/out of [130,250])
        wspd500.push(10.0 + f * 40.0);        // 10..50 kts
        wdir500.push(200.0 + f * 120.0);      // 200..320 (covers in/out of [210,310])
    }
    (tt, td850, wspd850, wdir850, wspd500, wdir500)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (tt, td850, wspd850, wdir850, wspd500, wdir500) = synthetic(8192);
    let gpu = sweat_index::host(
        &ctx, &tt, &td850, &wspd850, &wdir850, &wspd500, &wdir500,
    )
    .expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..tt.len() {
        let cpu = cpu_sweat(tt[i], td850[i], wspd850[i], wdir850[i], wspd500[i], wdir500[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "gpu={} cpu={} abs_diff={:e} at i={i}",
            gpu[i], cpu, abs
        );
    }
    eprintln!("max_abs={max_abs:e}");
}
