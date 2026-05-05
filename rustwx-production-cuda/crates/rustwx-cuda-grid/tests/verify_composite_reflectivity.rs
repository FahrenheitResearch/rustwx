//! Numerical agreement test: CUDA `composite_reflectivity` vs metrust CPU
//! reference. Test inputs are constructed so every column max exceeds the
//! `-30 dBZ` floor that the CPU reference clamps at, eliminating that
//! divergence at the boundary.

use rustwx_cuda_core::global;
use rustwx_cuda_grid::composite_reflectivity;
use wx_math::composite::composite_reflectivity_from_refl as cpu_cr;

const TOL: f64 = 1e-10;
const NX: usize = 64;
const NY: usize = 64;
const NZ: usize = 8;

fn synthetic_field() -> Vec<f64> {
    let n = NZ * NY * NX;
    let mut f = vec![0.0; n];
    let nxy = NY * NX;
    for k in 0..NZ {
        for j in 0..NY {
            for i in 0..NX {
                let idx = k * nxy + j * NX + i;
                // dBZ values in [0, 60]: ensures every column max > -30,
                // matching the CPU reference's lower-bound clamp.
                f[idx] = 10.0
                    + (k as f64) * 4.0
                    + 5.0 * ((i as f64 * 0.07).sin() + (j as f64 * 0.05).cos());
            }
        }
    }
    f
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn matches_cpu_reference() {
    let ctx = global().expect("init CUDA context");
    let f = synthetic_field();

    let gpu = composite_reflectivity::host(&ctx, &f, NX, NY, NZ).expect("kernel");
    let cpu = cpu_cr(&f, NX, NY, NZ);
    assert_eq!(gpu.len(), cpu.len());

    let mut max_abs = 0.0_f64;
    for k in 0..gpu.len() {
        let abs = (gpu[k] - cpu[k]).abs();
        if abs > max_abs { max_abs = abs; }
        assert!(abs < TOL, "k={k} gpu={} cpu={} abs={:e}", gpu[k], cpu[k], abs);
    }
    eprintln!("composite_reflectivity max_abs={max_abs:e}");
}
