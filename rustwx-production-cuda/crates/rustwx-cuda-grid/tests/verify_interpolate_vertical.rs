//! GPU `interpolate_vertical` ↔ `wx-math::regrid::interpolate_vertical`.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::interpolate_vertical;
use wx_math::regrid::interpolate_vertical as cpu_interp;

const TOL: f64 = 1e-10;

fn synthetic_volume(nx: usize, ny: usize, nz: usize) -> (Vec<f64>, Vec<f64>) {
    // Pressure descends from surface (1000) up to 100 hPa.
    let levels: Vec<f64> = (0..nz)
        .map(|k| 1000.0 - (k as f64) * (900.0 / (nz - 1) as f64))
        .collect();

    let mut values = Vec::with_capacity(nx * ny * nz);
    for k in 0..nz {
        let layer_t = -50.0 + (levels[k] - 100.0) * (75.0 / 900.0);
        for j in 0..ny {
            for i in 0..nx {
                let dx = i as f64 * 0.13 - (nx as f64) * 0.05;
                let dy = j as f64 * 0.07;
                values.push(layer_t + 0.01 * dx + 0.005 * dy);
            }
        }
    }
    (values, levels)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_linear() {
    let ctx = global().expect("init CUDA");
    let (nx, ny, nz) = (32usize, 24usize, 13usize);
    let (vals, levels) = synthetic_volume(nx, ny, nz);

    for &target in &[850.0_f64, 700.0, 500.0, 250.0] {
        let cpu = cpu_interp(&vals, &levels, target, nx, ny, nz, false);
        let gpu =
            interpolate_vertical::host(&ctx, &vals, &levels, target, nx, ny, nz, false)
                .expect("kernel");
        assert_eq!(gpu.len(), cpu.len());
        for i in 0..cpu.len() {
            let abs = (gpu[i] - cpu[i]).abs();
            assert!(abs < TOL, "linear target={target} idx={i} abs={abs:e}");
        }
    }
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_log() {
    let ctx = global().expect("init CUDA");
    let (nx, ny, nz) = (32usize, 24usize, 13usize);
    let (vals, levels) = synthetic_volume(nx, ny, nz);

    for &target in &[850.0_f64, 700.0, 500.0, 250.0] {
        let cpu = cpu_interp(&vals, &levels, target, nx, ny, nz, true);
        let gpu =
            interpolate_vertical::host(&ctx, &vals, &levels, target, nx, ny, nz, true)
                .expect("kernel");
        for i in 0..cpu.len() {
            let abs = (gpu[i] - cpu[i]).abs();
            assert!(abs < TOL, "log target={target} idx={i} abs={abs:e}");
        }
    }
}
