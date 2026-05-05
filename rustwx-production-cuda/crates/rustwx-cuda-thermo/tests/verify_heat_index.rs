//! Numerical agreement test: CUDA `heat_index` vs an inline CPU reference
//! that mirrors the kernel formula verbatim.
//!
//! `metrust::calc::atmo::heat_index` uses a different threshold rule near
//! 80 F (averages Steadman with T_F before deciding), so it does not match
//! the kernel to 1e-10. The kernel is retained verbatim from met-cu and
//! validated against its own definition here. See DIVERGENT_KERNELS.md.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::heat_index;

const TOL: f64 = 1e-10;

fn cpu_hi(t_c: f64, rh: f64) -> f64 {
    let t_f = t_c * 9.0 / 5.0 + 32.0;
    let steadman = 0.5 * (t_f + 61.0 + (t_f - 68.0) * 1.2 + rh * 0.094);
    let hi_f = if t_f < 80.0 {
        steadman
    } else {
        let mut hi = -42.379
            + 2.04901523 * t_f
            + 10.14333127 * rh
            - 0.22475541 * t_f * rh
            - 0.00683783 * t_f * t_f
            - 0.05481717 * rh * rh
            + 0.00122874 * t_f * t_f * rh
            + 0.00085282 * t_f * rh * rh
            - 0.00000199 * t_f * t_f * rh * rh;
        if rh < 13.0 && t_f >= 80.0 && t_f <= 112.0 {
            hi -= ((13.0 - rh) / 4.0) * ((17.0 - (t_f - 95.0).abs()) / 17.0).sqrt();
        } else if rh > 85.0 && t_f >= 80.0 && t_f <= 87.0 {
            hi += ((rh - 85.0) / 10.0) * ((87.0 - t_f) / 5.0);
        }
        hi
    };
    (hi_f - 32.0) * 5.0 / 9.0
}

fn synthetic_profile(n: usize) -> (Vec<f64>, Vec<f64>) {
    let mut t = Vec::with_capacity(n);
    let mut rh = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(15.0 + f * 30.0); // 15 .. 45 C
        rh.push(5.0 + f * 90.0); // 5 .. 95 %
    }
    (t, rh)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, rh) = synthetic_profile(8192);
    let gpu = heat_index::host(&ctx, &t, &rh).expect("kernel");
    assert_eq!(gpu.len(), t.len());

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_hi(t[i], rh[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i} (t={}, rh={})",
                gpu[i], cpu, abs, t[i], rh[i]);
    }
    eprintln!("max_abs={max_abs:e}");
}
