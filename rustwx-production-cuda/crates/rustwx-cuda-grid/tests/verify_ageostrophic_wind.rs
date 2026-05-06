//! Numerical agreement test: CUDA `ageostrophic_wind` vs metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::ageostrophic_wind;
use wx_math::dynamics::{ageostrophic_wind as cpu_aw, geostrophic_wind as cpu_gw};

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 100_000.0;
const DY: f64 = 100_000.0;

fn synthetic_fields() -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut u = vec![0.0; n];
    let mut v = vec![0.0; n];
    let mut height = vec![0.0; n];
    let mut lats = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            u[j * NX + i] = 12.0 + 3.0 * (x * 1e-6).sin();
            v[j * NX + i] = -2.0 + 1.5 * (y * 1e-6).cos();
            height[j * NX + i] = 5500.0 + 50.0 * (x * 1e-6).sin() + 30.0 * (y * 1e-6).cos();
            lats[j * NX + i] = 35.0 + 0.1 * j as f64;
        }
    }
    (u, v, height, lats)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (u, v, height, lats) = synthetic_fields();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let (gpu_ua, gpu_va) =
        ageostrophic_wind::host(&ctx, &u, &v, &height, &lats, &dx, &dy, NX, NY).expect("kernel");

    // CPU: derive geostrophic wind from height + lats, then subtract.
    let (cpu_ug, cpu_vg) = cpu_gw(&height, &lats, NX, NY, DX, DY);
    let (cpu_ua, cpu_va) = cpu_aw(&u, &v, &cpu_ug, &cpu_vg);

    let mut max_u = 0.0_f64;
    let mut max_v = 0.0_f64;
    for k in 0..n {
        let au = (gpu_ua[k] - cpu_ua[k]).abs();
        let av = (gpu_va[k] - cpu_va[k]).abs();
        if au > max_u {
            max_u = au;
        }
        if av > max_v {
            max_v = av;
        }
        assert!(
            au < TOL,
            "ua k={k} gpu={} cpu={} abs={:e}",
            gpu_ua[k],
            cpu_ua[k],
            au
        );
        assert!(
            av < TOL,
            "va k={k} gpu={} cpu={} abs={:e}",
            gpu_va[k],
            cpu_va[k],
            av
        );
    }
    eprintln!("ageostrophic_wind max_abs_u={max_u:e} max_abs_v={max_v:e}");
}
