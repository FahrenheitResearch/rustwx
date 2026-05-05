//! Numerical agreement test: CUDA `log_interpolate_1d` vs metrust's per-column
//! `log_interpolate_1d`.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::log_interpolate_1d;
use metrust::interpolate::log_interpolate_1d as cpu_logp;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const NZ_IN: usize = 6;

fn synthetic() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NZ_IN * NY * NX;
    let mut field = vec![0.0; n];
    let mut pressure = vec![0.0; n];

    // Standard pressure levels (Pa), descending with k.
    let base = [100_000.0_f64, 85_000.0, 70_000.0, 50_000.0, 30_000.0, 20_000.0];

    let nxy = NY * NX;
    for k in 0..NZ_IN {
        for j in 0..NY {
            for i in 0..NX {
                let idx = k * nxy + j * NX + i;
                // Pressure: base level slightly modulated by a small smooth
                // perturbation. Stays strictly monotone (descending) in k.
                let perturb = 1.0 * ((i as f64 * 0.05).sin() + (j as f64 * 0.05).cos());
                pressure[idx] = base[k] + perturb;
                // Field: smooth function of level + position (e.g. T in K).
                field[idx] = 250.0 + 0.5 * (k as f64) + 2.0 * (i as f64 * 0.1).sin()
                    + 1.5 * (j as f64 * 0.1).cos();
            }
        }
    }

    // Target pressures inside the input range.
    let p_target = vec![92_500.0, 60_000.0, 25_000.0];
    (field, pressure, p_target)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (field, pressure, p_target) = synthetic();
    let nz_out = p_target.len();

    let gpu = log_interpolate_1d::host(&ctx, &field, &pressure, &p_target, NX, NY, NZ_IN)
        .expect("kernel");

    let nxy = NY * NX;
    let mut cpu = vec![0.0_f64; nz_out * nxy];
    let mut col_xp = vec![0.0_f64; NZ_IN];
    let mut col_fp = vec![0.0_f64; NZ_IN];
    for j in 0..NY {
        for i in 0..NX {
            let idx2d = j * NX + i;
            for k in 0..NZ_IN {
                col_xp[k] = pressure[k * nxy + idx2d];
                col_fp[k] = field[k * nxy + idx2d];
            }
            let interp = cpu_logp(&p_target, &col_xp, &col_fp);
            for ko in 0..nz_out {
                cpu[ko * nxy + idx2d] = interp[ko];
            }
        }
    }

    let mut max_abs = 0.0_f64;
    for k in 0..gpu.len() {
        if cpu[k].is_nan() && gpu[k].is_nan() { continue; }
        let abs = (gpu[k] - cpu[k]).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "k={k} gpu={} cpu={} abs={:e}", gpu[k], cpu[k], abs);
    }
    eprintln!("log_interpolate_1d max_abs={max_abs:e}");
}
