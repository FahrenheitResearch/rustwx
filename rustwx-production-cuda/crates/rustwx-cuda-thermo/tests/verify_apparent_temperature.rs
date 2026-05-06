//! Numerical agreement: CUDA `apparent_temperature` vs an inline reference
//! that mirrors the kernel formula verbatim.
//!
//! `metrust::calc::atmo::apparent_temperature` calls `metrust::calc::atmo::heat_index`
//! which uses a Steadman-vs-T_F average — the kernel does not — so metrust does
//! not match to 1e-10. See DIVERGENT_KERNELS.md.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::apparent_temperature;

const TOL: f64 = 1e-10;

fn cpu_at(t_c: f64, rh: f64, ws_ms: f64) -> f64 {
    let t_f = t_c * 9.0 / 5.0 + 32.0;
    let wind_mph = ws_ms * 2.23694;
    if t_f >= 80.0 {
        let mut hi_f = -42.379 + 2.04901523 * t_f + 10.14333127 * rh
            - 0.22475541 * t_f * rh
            - 0.00683783 * t_f * t_f
            - 0.05481717 * rh * rh
            + 0.00122874 * t_f * t_f * rh
            + 0.00085282 * t_f * rh * rh
            - 0.00000199 * t_f * t_f * rh * rh;
        if rh < 13.0 && t_f >= 80.0 && t_f <= 112.0 {
            hi_f -= ((13.0 - rh) / 4.0) * ((17.0 - (t_f - 95.0).abs()) / 17.0).sqrt();
        } else if rh > 85.0 && t_f >= 80.0 && t_f <= 87.0 {
            hi_f += ((rh - 85.0) / 10.0) * ((87.0 - t_f) / 5.0);
        }
        (hi_f - 32.0) * 5.0 / 9.0
    } else if t_f <= 50.0 && wind_mph > 3.0 {
        let wind_kmh = ws_ms * 3.6;
        let spf = wind_kmh.powf(0.16);
        (0.6215 + 0.3965 * spf) * t_c - 11.37 * spf + 13.12
    } else {
        t_c
    }
}

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut t = Vec::with_capacity(n);
    let mut rh = Vec::with_capacity(n);
    let mut w = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        t.push(-25.0 + f * 60.0);
        rh.push(5.0 + f * 90.0);
        w.push(f * 20.0);
    }
    (t, rh, w)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (t, rh, w) = synthetic(8192);
    let gpu = apparent_temperature::host(&ctx, &t, &rh, &w).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..t.len() {
        let cpu = cpu_at(t[i], rh[i], w[i]);
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
