//! Numerical agreement test for the met-cu altimeter -> SLP kernel.
//!
//! Compared against an inline reference that mirrors the kernel formula
//! verbatim. metrust's `altimeter_to_sea_level_pressure` uses the
//! Smithsonian +0.3 inverse for step 1 and so does not match to 1e-10. See
//! DIVERGENT_KERNELS.md.

use rustwx_cuda_core::global;
use rustwx_cuda_thermo::altimeter_to_sea_level_pressure;

const TOL: f64 = 1e-10;

fn cpu_slp(alt: f64, elev: f64, t_c: f64) -> f64 {
    let lapse = 0.0065_f64;
    let t0 = 288.15_f64;
    let g = 9.80665_f64;
    let rd = 287.058_f64;
    let rocp = 0.2857142857142857_f64;
    let ratio = 1.0 - (lapse * elev) / (t0 + lapse * elev);
    let p_stn = alt * ratio.powf(1.0 / rocp) + 0.3;
    let t_sfc_k = t_c + 273.15;
    let t_mean_k = t_sfc_k + 0.5 * lapse * elev;
    p_stn * (g * elev / (rd * t_mean_k)).exp()
}

fn synthetic(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut alt = Vec::with_capacity(n);
    let mut elev = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    for i in 0..n {
        let f = (i as f64) / ((n.max(2) - 1) as f64);
        alt.push(950.0 + f * 100.0);
        elev.push(f * 3000.0);
        t.push(-10.0 + f * 35.0);
    }
    (alt, elev, t)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (alt, elev, t) = synthetic(8192);
    let gpu = altimeter_to_sea_level_pressure::host(&ctx, &alt, &elev, &t).expect("kernel");

    let mut max_abs = 0.0;
    for i in 0..alt.len() {
        let cpu = cpu_slp(alt[i], elev[i], t[i]);
        let abs = (gpu[i] - cpu).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "gpu={} cpu={} diff={:e} at i={i}", gpu[i], cpu, abs);
    }
    eprintln!("max_abs={max_abs:e}");
}
