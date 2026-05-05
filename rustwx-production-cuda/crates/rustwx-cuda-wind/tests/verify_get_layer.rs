//! Smoke test: CUDA `get_layer` launches and selects expected pressure rows.
//!
//! DEFER: the kernel performs pure index selection. `wx_math::thermo::get_layer`
//! also interpolates new endpoints in log-pressure at the layer boundaries,
//! so the two implementations differ at the layer edges. See
//! `DIVERGENT_KERNELS.md`.

use rustwx_cuda_core::global;
use rustwx_cuda_wind::get_layer;

const NCOLS: usize = 256;
const NLEVELS: usize = 30;

fn synthetic_profile() -> (Vec<f64>, Vec<f64>) {
    // Pressure descends from 1000 hPa (surface) up to 100 hPa (high), shared
    // across all columns. Values: column-distinct linear ramp in level index.
    let mut p = vec![0.0; NCOLS * NLEVELS];
    let mut vals = vec![0.0; NCOLS * NLEVELS];
    for c in 0..NCOLS {
        for k in 0..NLEVELS {
            let f = (k as f64) / ((NLEVELS - 1) as f64);
            let pressure = 1000.0 - f * 900.0;
            let idx = c * NLEVELS + k;
            p[idx] = pressure;
            vals[idx] = (c as f64) + (k as f64) * 0.1;
        }
    }
    (p, vals)
}

#[test]
#[ignore = "requires NVIDIA GPU + CUDA driver"]
fn smoke() {
    let ctx = global().expect("init CUDA context");
    let (p, vals) = synthetic_profile();
    let p_bottom = 850.0;
    let p_top = 500.0;

    let (p_out, v_out, cnt) = get_layer::host(
        &ctx, &p, &vals, NCOLS, NLEVELS, p_bottom, p_top,
    )
    .expect("kernel");
    assert_eq!(p_out.len(), NCOLS * NLEVELS);
    assert_eq!(v_out.len(), NCOLS * NLEVELS);
    assert_eq!(cnt.len(), NCOLS);

    // Independent CPU selection mirroring the kernel semantics.
    for c in 0..NCOLS {
        let s = c * NLEVELS;
        let mut expected_p: Vec<f64> = Vec::new();
        let mut expected_v: Vec<f64> = Vec::new();
        for k in 0..NLEVELS {
            let pp = p[s + k];
            if pp <= p_bottom && pp >= p_top {
                expected_p.push(pp);
                expected_v.push(vals[s + k]);
            }
        }
        assert_eq!(cnt[c] as usize, expected_p.len(), "col={c} count");
        for i in 0..expected_p.len() {
            assert!(
                (p_out[s + i] - expected_p[i]).abs() < 1e-12,
                "col={c} i={i} p_out={} expected={}",
                p_out[s + i],
                expected_p[i]
            );
            assert!(
                (v_out[s + i] - expected_v[i]).abs() < 1e-12,
                "col={c} i={i} v_out={} expected={}",
                v_out[s + i],
                expected_v[i]
            );
        }
        // Padding past `cnt[c]` must be NaN.
        for i in (cnt[c] as usize)..NLEVELS {
            assert!(
                p_out[s + i].is_nan() && v_out[s + i].is_nan(),
                "col={c} i={i} expected NaN padding"
            );
        }
    }
}
