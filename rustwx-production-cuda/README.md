# rustwx-production-cuda

CUDA-accelerated meteorology kernels for [rustwx-production], built by porting
the verified kernels from [met-cu] into Rust + cudarc bindings. Numerical
agreement with the metrust CPU reference is asserted to ~1e-10 in tests.

ECAPE is **not** ported here — the vendored `ecape-rs` solver
(Cloutier & Flannigan) is the authoritative path and stays on CPU.

## Layout

```
crates/
  rustwx-cuda-core/     GPU context, NVRTC PTX compile + disk cache, error types
  rustwx-cuda-thermo/   thermodynamics (theta, dewpoint, mixing ratio, ...)
  rustwx-cuda-wind/     wind components, shear, SRH, storm motion
  rustwx-cuda-grid/     2D stencils (vorticity, divergence, frontogenesis, ...)
  rustwx-cuda-severe/   composite indices (STP, SCP, SHIP, EHI, BRN, ...)
  rustwx-cuda/          umbrella, re-exports everything
kernels/
  common/               shared device helpers (.cuh) — included by every kernel
  thermo/ wind/ grid/ severe/    raw .cu source per kernel
vendor/
  metrust/ wx-math/ ...           CPU reference, used by verification tests
```

## Build & test

Requires:
- NVIDIA driver + CUDA toolkit (tested with CUDA 13.0)
- Rust 1.75+
- A CUDA-capable GPU for the `#[ignore]`d numerical-verification tests

```
cargo check -p rustwx-cuda             # compile-only, no GPU needed
cargo test  -p rustwx-cuda-thermo --release -- --ignored   # numerical verify
```

The first run compiles each kernel via NVRTC and caches the PTX under
`$RUSTWX_CUDA_CACHE` (default: `~/.cache/rustwx-cuda/ptx`). Subsequent runs
load PTX directly.

## Numerical contract

- All kernels run with `--use_fast_math=false`. Parcel-ascent reproducibility
  matters more than cycles.
- Inputs must be C-contiguous on the FFI boundary. F-order input would index
  garbage; the wrappers refuse it.
- Each kernel has a paired test under `crates/<crate>/tests/verify_*.rs`
  comparing GPU output against the matching `metrust` / `wx-math` function on
  a synthetic profile of ~8K points, with a tolerance of `1e-10` absolute.

## Deferred kernels

See [DIVERGENT_KERNELS.md](./DIVERGENT_KERNELS.md). Four met-cu kernels
disagree numerically with metrust and are not ported until the underlying
formula choice is reconciled:

- `smooth_n_point` (different convolution weights)
- `showalter_index` (RK4 step size mismatch)
- `cin` (metrust v0.3.x has an unbounded-integration bug; met-cu is correct)
- `dcp` (4th-term formula difference, source location TBD)

Everything else from the earlier `REVIEW.md` divergence list (STP, SCP,
Haines, hot_dry_windy) was re-audited and confirmed byte-identical to
metrust — those port as-is.

[rustwx-production]: https://github.com/FahrenheitResearch/rustwx-production
[met-cu]: https://github.com/FahrenheitResearch/met-cu
