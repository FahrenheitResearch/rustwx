//! 3 HRRR forecast hours × CONUS + 7 US split regions × full non-ECAPE
//! product set, with three production-shaped optimizations:
//!
//!   1. Single CONUS upload per hour (regions are device-side crops).
//!   2. Pinned host memory for the H2D upload.
//!   3. Skip the 3D T-volume upload for products that don't need it
//!      (severe / wind / thermo); only vertical interpolation consumes it.
//!
//! Per-product timings captured for both CPU (rayon) and GPU. PNGs of
//! every GPU output go to `bench_output/h<hour>/<region>/<product>.png`
//! for visual sanity checking.
//!
//!     cargo run -p rustwx-cuda-bench --bin three_hours --release

mod viz;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use rustwx_cuda_core::cudarc::driver::CudaSlice;
use rustwx_cuda_core::{global, ContextHandle, Result};
use wx_math::composite as cpu_comp;
use wx_math::dynamics as cpu_dyn;
use wx_math::regrid as cpu_regrid;
use wx_math::thermo as cpu_thermo;

const NZ: usize = 40;
const HOURS: usize = 3;
const HRRR_DX_M: f64 = 3000.0;
const CONUS_NX: usize = 1799;
const CONUS_NY: usize = 1059;

/// Plottable region — `(off_x, off_y)` is the top-left corner inside CONUS,
/// `(nx, ny)` is the crop size in HRRR cells.
#[derive(Clone, Copy)]
struct Region {
    name: &'static str,
    off_x: usize,
    off_y: usize,
    nx: usize,
    ny: usize,
}

fn regions() -> [Region; 8] {
    [
        Region {
            name: "conus",
            off_x: 0,
            off_y: 0,
            nx: CONUS_NX,
            ny: CONUS_NY,
        },
        Region {
            name: "pacific_northwest",
            off_x: 0,
            off_y: 700,
            nx: 442,
            ny: 316,
        },
        Region {
            name: "california_southwest",
            off_x: 0,
            off_y: 200,
            nx: 501,
            ny: 390,
        },
        Region {
            name: "rockies_high_plains",
            off_x: 400,
            off_y: 350,
            nx: 472,
            ny: 464,
        },
        Region {
            name: "southern_plains",
            off_x: 550,
            off_y: 80,
            nx: 560,
            ny: 576,
        },
        Region {
            name: "great_lakes",
            off_x: 900,
            off_y: 580,
            nx: 752,
            ny: 427,
        },
        Region {
            name: "southeast",
            off_x: 900,
            off_y: 80,
            nx: 708,
            ny: 539,
        },
        Region {
            name: "northeast",
            off_x: 1200,
            off_y: 550,
            nx: 575,
            ny: 464,
        },
    ]
}

// ---------------------------------------------------------------------------
// Synthetic CONUS-shaped source data
// ---------------------------------------------------------------------------

struct ConusVolume {
    nx: usize,
    ny: usize,
    nz: usize,
    pressure: Vec<f64>,
    t: Vec<f64>,
}

struct ConusSlab {
    nx: usize,
    ny: usize,
    t850: Vec<f64>,
    t700: Vec<f64>,
    t500: Vec<f64>,
    td850: Vec<f64>,
    td700: Vec<f64>,
    u500: Vec<f64>,
    v500: Vec<f64>,
    height500: Vec<f64>,
    sbcape: Vec<f64>,
    mucape: Vec<f64>,
    sblcl: Vec<f64>,
    srh_1km: Vec<f64>,
    srh_3km: Vec<f64>,
    shear_06: Vec<f64>,
    z1000: Vec<f64>,
    z700: Vec<f64>,
    dx: Vec<f64>,
    dy: Vec<f64>,
    lat: Vec<f64>,
}

fn build_conus_volume(hour: usize) -> ConusVolume {
    let nz = NZ;
    let pressure: Vec<f64> = (0..nz)
        .map(|k| 1000.0 - (k as f64) * (975.0 / (nz - 1) as f64))
        .collect();
    let n = CONUS_NX * CONUS_NY;
    let mut t = vec![0.0; n * nz];
    let phase = (hour as f64) * 0.13;
    for k in 0..nz {
        let p = pressure[k];
        let layer_t = -55.0 + (p - 100.0) * (75.0 / 900.0);
        for j in 0..CONUS_NY {
            for i in 0..CONUS_NX {
                let off = k * n + j * CONUS_NX + i;
                t[off] = layer_t
                    + ((i as f64) * 0.013 + phase).sin() * 1.2
                    + ((j as f64) * 0.011 + phase).cos() * 0.6;
            }
        }
    }
    ConusVolume {
        nx: CONUS_NX,
        ny: CONUS_NY,
        nz,
        pressure,
        t,
    }
}

fn build_conus_slab(hour: usize) -> ConusSlab {
    let n = CONUS_NX * CONUS_NY;
    let phase = (hour as f64) * 0.17;
    let mk = |seed: f64, amp: f64, base: f64| -> Vec<f64> {
        (0..n)
            .map(|i| base + amp * (((i as f64) * seed + phase).sin()))
            .collect()
    };
    ConusSlab {
        nx: CONUS_NX,
        ny: CONUS_NY,
        t850: mk(0.013, 4.0, 16.0),
        t700: mk(0.011, 3.0, 3.0),
        t500: mk(0.009, 4.0, -10.0),
        td850: mk(0.014, 5.0, 10.0),
        td700: mk(0.012, 4.0, -3.0),
        u500: mk(0.017, 8.0, 20.0),
        v500: mk(0.019, 6.0, 2.0),
        height500: mk(0.005, 50.0, 5700.0),
        sbcape: mk(0.020, 800.0, 1500.0),
        mucape: mk(0.022, 900.0, 1700.0),
        sblcl: mk(0.018, 400.0, 1200.0),
        srh_1km: mk(0.015, 80.0, 120.0),
        srh_3km: mk(0.013, 120.0, 200.0),
        shear_06: mk(0.011, 6.0, 18.0),
        z1000: mk(0.007, 30.0, 100.0),
        z700: mk(0.006, 60.0, 3100.0),
        dx: vec![HRRR_DX_M; n],
        dy: vec![HRRR_DX_M; n],
        lat: (0..n)
            .map(|i| 25.0 + ((i / CONUS_NX) as f64) * 0.025)
            .collect(),
    }
}

/// Pull a contiguous CPU-side region crop out of a CONUS-sized field. Used
/// only for CPU-baseline calls, which take per-region inputs.
fn cpu_crop_2d(src: &[f64], r: &Region) -> Vec<f64> {
    let mut out = Vec::with_capacity(r.nx * r.ny);
    for j in 0..r.ny {
        let row = (j + r.off_y) * CONUS_NX + r.off_x;
        out.extend_from_slice(&src[row..row + r.nx]);
    }
    out
}

// ---------------------------------------------------------------------------
// Per-product timing record
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Cat {
    VInterp,
    Severe,
    Stencil,
    Thermo,
    Wind,
    Transport,
}
impl Cat {
    fn name(self) -> &'static str {
        match self {
            Cat::VInterp => "vertical_interp",
            Cat::Severe => "severe",
            Cat::Stencil => "stencil",
            Cat::Thermo => "thermo",
            Cat::Wind => "wind",
            Cat::Transport => "pcie_transport",
        }
    }
}

struct Record {
    product: &'static str,
    category: Cat,
    cpu: Duration,
    gpu: Duration,
}

fn time_cpu<R>(f: impl FnOnce() -> R) -> (Duration, R) {
    let t = Instant::now();
    let r = f();
    (t.elapsed(), r)
}

// ---------------------------------------------------------------------------
// Device-side state held for the whole hour: CONUS-sized DeviceVecs
// ---------------------------------------------------------------------------

struct ConusDevice {
    pressure: CudaSlice<f64>,
    t_3d: CudaSlice<f64>,
    t850: CudaSlice<f64>,
    t700: CudaSlice<f64>,
    t500: CudaSlice<f64>,
    td850: CudaSlice<f64>,
    td700: CudaSlice<f64>,
    u500: CudaSlice<f64>,
    v500: CudaSlice<f64>,
    height500: CudaSlice<f64>,
    sbcape: CudaSlice<f64>,
    mucape: CudaSlice<f64>,
    sblcl: CudaSlice<f64>,
    srh_1km: CudaSlice<f64>,
    srh_3km: CudaSlice<f64>,
    shear_06: CudaSlice<f64>,
    z1000: CudaSlice<f64>,
    z700: CudaSlice<f64>,
    dx: CudaSlice<f64>,
    dy: CudaSlice<f64>,
    lat: CudaSlice<f64>,
    p500_const: CudaSlice<f64>,
    p850_const: CudaSlice<f64>,
    mr_const: CudaSlice<f64>,
}

/// Synchronous H2D upload using pageable memory. Allocating pinned per call
/// is too slow (page-locking 608 MB for the 3D volume costs ~600ms by itself),
/// and CONUS-sized buffers don't repeat. Pinned helps when the same buffer
/// is reused many times — we don't have that pattern here.
fn upload_pinned(ctx: &ContextHandle, src: &[f64]) -> Result<CudaSlice<f64>> {
    Ok(ctx.stream().memcpy_stod(src)?)
}

include!("main_part2.rs");
