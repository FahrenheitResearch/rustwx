//! Numerical agreement test: CUDA `geostrophic_wind` vs metrust CPU reference.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::geostrophic_wind;
use wx_math::dynamics::geostrophic_wind as cpu_gw;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const DX: f64 = 100_000.0; // 100 km grid for synoptic-scale heights
const DY: f64 = 100_000.0;

fn synthetic_fields() -> (Vec<f64>, Vec<f64>) {
    let n = NX * NY;
    let mut height = vec![0.0; n];
    let mut lats = vec![0.0; n];
    for j in 0..NY {
        for i in 0..NX {
            let x = i as f64 * DX;
            let y = j as f64 * DY;
            // Geopotential height (m), magnitude similar to 500 hPa surface.
            height[j * NX + i] = 5500.0 + 50.0 * (x * 1e-6).sin() + 30.0 * (y * 1e-6).cos();
            // Mid-latitude lats far from equator (35-45N) — no near-equator branch.
            lats[j * NX + i] = 35.0 + 0.1 * j as f64;
        }
    }
    (height, lats)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (height, lats) = synthetic_fields();
    let n = NX * NY;
    let dx = vec![DX; n];
    let dy = vec![DY; n];

    let (gpu_ug, gpu_vg) =
        geostrophic_wind::host(&ctx, &height, &lats, &dx, &dy, NX, NY).expect("kernel");
    let (cpu_ug, cpu_vg) = cpu_gw(&height, &lats, NX, NY, DX, DY);

    let mut max_u = 0.0_f64;
    let mut max_v = 0.0_f64;
    for k in 0..n {
        let au = (gpu_ug[k] - cpu_ug[k]).abs();
        let av = (gpu_vg[k] - cpu_vg[k]).abs();
        if au > max_u { max_u = au; }
        if av > max_v { max_v = av; }
        assert!(au < TOL, "u k={k} gpu={} cpu={} abs={:e}", gpu_ug[k], cpu_ug[k], au);
        assert!(av < TOL, "v k={k} gpu={} cpu={} abs={:e}", gpu_vg[k], cpu_vg[k], av);
    }
    eprintln!("geostrophic_wind max_abs_u={max_u:e} max_abs_v={max_v:e}");
}
