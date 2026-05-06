//! Numerical agreement test: CUDA `interpolate_1d` vs metrust's per-column
//! `interpolate_1d` (1-D linear interpolation across input levels to target
//! levels, applied independently to each `(j, i)` column).

use metrust::interpolate::interpolate_1d as cpu_1d;
use rustwx_cuda_core::global;
use rustwx_cuda_grid::interpolate_1d;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const NZ_IN: usize = 8;

fn synthetic() -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = NZ_IN * NY * NX;
    let mut field = vec![0.0; n];
    let mut levels_in = vec![0.0; n];

    // Ascending coordinates: per-column slight perturbation of base [0..nz)
    let nxy = NY * NX;
    for k in 0..NZ_IN {
        for j in 0..NY {
            for i in 0..NX {
                let idx = k * nxy + j * NX + i;
                // levels_in: ascending in k, varies smoothly across (j, i) but
                // remains strictly monotone along z.
                let base = k as f64;
                let perturb = 0.01 * ((i as f64 * 0.1).sin() + (j as f64 * 0.1).cos());
                levels_in[idx] = base + perturb;
                // field: smooth function of level + position
                field[idx] = 10.0
                    + base * 2.0
                    + 0.5 * (i as f64 * 0.2).sin()
                    + 0.3 * (j as f64 * 0.15).cos();
            }
        }
    }

    // Target levels strictly inside the input range (no NaN expected).
    let levels_out = vec![1.5, 3.5, 5.5];
    (field, levels_in, levels_out)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let (field, levels_in, levels_out) = synthetic();
    let nz_out = levels_out.len();

    let gpu = interpolate_1d::host(&ctx, &field, &levels_in, &levels_out, NX, NY, NZ_IN, true)
        .expect("kernel");

    // Build CPU reference per column.
    let nxy = NY * NX;
    let mut cpu = vec![0.0_f64; nz_out * nxy];
    let mut col_xp = vec![0.0_f64; NZ_IN];
    let mut col_fp = vec![0.0_f64; NZ_IN];
    for j in 0..NY {
        for i in 0..NX {
            let idx2d = j * NX + i;
            for k in 0..NZ_IN {
                col_xp[k] = levels_in[k * nxy + idx2d];
                col_fp[k] = field[k * nxy + idx2d];
            }
            let interp = cpu_1d(&levels_out, &col_xp, &col_fp);
            for ko in 0..nz_out {
                cpu[ko * nxy + idx2d] = interp[ko];
            }
        }
    }

    let mut max_abs = 0.0_f64;
    for k in 0..gpu.len() {
        if cpu[k].is_nan() && gpu[k].is_nan() {
            continue;
        }
        let abs = (gpu[k] - cpu[k]).abs();
        if abs > max_abs {
            max_abs = abs;
        }
        assert!(
            abs < TOL,
            "k={k} gpu={} cpu={} abs={:e}",
            gpu[k],
            cpu[k],
            abs
        );
    }
    eprintln!("interpolate_1d max_abs={max_abs:e}");
}
