# Codex handoff — rustwx CUDA acceleration

This document is a complete handoff to whoever picks up next on the
rustwx CUDA work. Read it end-to-end before touching any code.

## What you're inheriting

Two parallel Cargo workspaces:

- **`rustwx-production/`** — the production rustwx tree (HRRR/GFS/etc.
  ingest, calc, render, CLI). Cloned from
  `github.com/FahrenheitResearch/rustwx-production`. Has a `cuda` feature
  that, when enabled, swaps GPU code into hot CPU paths.
- **`rustwx-production-cuda/`** — separate workspace housing all CUDA
  kernels and Rust wrappers. Vendors the relevant `wx-*`/`metrust` crates
  for verification harnesses. No git.

The `rustwx-production` crate `rustwx-render` (and `rustwx-calc`) take an
optional `path` dep on `rustwx-production-cuda/crates/rustwx-cuda` and
gate GPU paths behind `#[cfg(feature = "cuda")]`. Default builds are
unaffected.

## Production-scale context

The user's target workload: **~200,000 plots per cycle across 5 models ×
all GEFS members × all continents.** Every millisecond saved per plot
multiplies by 200,000. A 50 ms per-plot win = 10,000 seconds of aggregate
compute saved per cycle = 5 minutes wall at 32-thread parallelism. Frame
all optimization decisions against that scale, not the 91-plot test pass
we use for verification.

The 91-plot pipeline used for benchmarking throughout this work is
`hrrr_non_ecape_hour --date 20260504 --cycle 18 --forecast-hour 1
--region conus`. It runs 52 direct + 28 derived + 11 windowed recipes;
each produces one PNG.

## What's shipped and verified working

### CUDA kernels (in `rustwx-production-cuda/crates/rustwx-cuda-render/`)

All built, all tested, all integrated with strict numerical agreement
verified against an inlined CPU reference in each test module.

| Kernel | File | CPU baseline | GPU per-call |
|---|---|---|---|
| `rasterize_grid` (regular bilinear) | `kernels/render/rasterize_grid.cu` | n/a | <1 ms (untested in prod — HRRR only uses projected) |
| `rasterize_projected_grid` (triangle fill) | `kernels/render/rasterize_projected_grid.cu` | ~110 ms/call | ~17 ms/call |
| `downsample_lanczos3` | `kernels/render/downsample_lanczos3.cu` | ~90 ms/call | ~5 ms/call |
| `sharpen_3x3` | `kernels/render/sharpen_3x3.cu` | (folded into downsample CPU) | ~0.1 ms/call |
| `raster_blit` (alpha composite) | `kernels/render/raster_blit.cu` | n/a | tested only |
| `polygon_fill` (scanline) | `kernels/render/polygon_fill.cu` | n/a | tested only |
| `linework` (anti-aliased thick polylines) | `kernels/render/linework.cu` | n/a | tested only |
| `contour_lines` (marching squares + AA line) | `kernels/render/contour_lines.cu` | n/a | tested only |

The four "tested only" kernels have working Rust wrappers and passing
unit tests but are NOT wired into the production render path. See
[Why they're not wired](#why-the-other-four-kernels-arent-wired) below.

### Pipeline-level changes that are wired and shipping

Verified on real HRRR data (`hrrr_non_ecape_hour --features cuda`):

1. **GPU rasterize_projected_grid swap** in
   `rustwx-production/crates/rustwx-render/src/rasterize.rs`. 91 of 91
   calls hit GPU; counter-verified with `RUSTWX_CUDA_RASTERIZE_DEBUG=1`.
2. **Per-thread CUDA streams** via thread-local cache in
   `rasterize.rs::with_thread_stream`. Each rayon worker gets its own
   non-default stream so kernels actually run concurrently on the
   device instead of queuing on the shared default stream.
3. **Global mesh cache** in
   `rustwx-cuda-render/src/rasterize_projected_grid.rs::MESH_CACHE`.
   Same `pixel_points` slice across recipes hits cache; eliminates
   redundant 30 MB upload + 1.9 M-iteration `Option<>` flatten per call.
   Currently 44% hit rate (single-pipeline test), expected >99% at
   production scale where many plots share each unique mesh.
4. **CUDA `bind_to_thread()` retry** in `with_thread_stream` —
   eliminates the 7-9 of 91 `CUDA_ERROR_NOT_INITIALIZED` failures
   we saw on first run from newly-spawned rayon workers.
5. **GPU downsample + sharpen** in `render.rs::render_to_image_profile`.
   Replaces `image::imageops::resize(Lanczos3) + sharpen_downsampled_image`.
   91 of 91 calls hit GPU. f32 vs CPU's f64 produces ≤6/255 channel
   delta on data pixels and up to ~50/255 on text glyphs (sub-pixel
   positioned). Visually identical.
6. **PNG `Filter::Up` instead of `Filter::Adaptive`** in
   `encode_rgba_png_profile_with_options`. `Adaptive` tries all 5
   filters per scanline; `Up` is the next-best single filter and
   compresses within ~5% of adaptive on map images. CPU-only
   optimization, ~12% PNG encode reduction.

### Concrete results vs CPU baseline

```
Pipeline total_ms:    42,636 → 38,271  (-10.2%)
render_to_image_ms:   50,583 → 37,541  (-25.8%)
rasterize aggregate:  15,981 → 11,033  (-31%)
downsample aggregate:  7,987 →    643  (-92%)
png_encode aggregate:  6,954 →  6,118  (-12%)
Peak SM utilization:     20% →    39% (sustained), 84% briefly
```

At 200k plots/cycle: ~143 ms aggregate render time saved per plot →
~28,600 seconds aggregate saved per cycle → 7.5 minutes wall at
64-thread parallelism. ~183 hours/year per worker pool.

## Why the other four kernels aren't wired

`raster_blit`, `polygon_fill`, `linework`, and `contour_lines` all have
working kernels and passing tests (max channel delta ≤ 2 in all cases).
They're not wired because each phase swap costs two PCIe round-trips of
the full canvas (~3.7 MB up + 3.7 MB down per call). With 5 phases that
adds up to ~37 MB of redundant PCIe per plot.

Concrete proof point: I tried wiring linework as a per-call swap (see
git diff history of `render.rs::draw_projected_lines`). Result was a
**100× regression** — the linework_ms aggregate went from 2,378 ms (CPU)
to 248,617 ms (GPU). Cause: the kernel needs to launch sequentially per
polyline to preserve CPU draw order, and each launch carries
sync overhead, plus the canvas round-trip per call. For ~5-10 polyline
groups × 91 plots = 450-900 launch-syncs all paying full canvas PCIe.

The fix is architectural, not per-kernel. See next section.

## The next big work item: canvas-resident render pipeline

This is the unlock to push the kernels from "exists" to "actually
saving wall time". Currently `img: &mut RgbaImage` is owned by CPU and
each GPU phase uploads/processes/downloads. The plan: keep the canvas
GPU-resident for its whole lifetime.

### Concrete changes

1. **Define a `Canvas` enum** in `rustwx-render/src/canvas.rs` (new file):
   ```rust
   pub enum Canvas {
       Cpu(RgbaImage),
       Gpu {
           buf: Arc<parking_lot::Mutex<DeviceVec<u32>>>,
           width: u32,
           height: u32,
       },
   }

   impl Canvas {
       pub fn width(&self) -> u32 { ... }
       pub fn height(&self) -> u32 { ... }
       /// Force CPU residency. No-op if already CPU; downloads if GPU.
       pub fn to_cpu(&mut self) -> &mut RgbaImage { ... }
       /// Force GPU residency. Uploads if CPU; no-op if already GPU.
       #[cfg(feature = "cuda")]
       pub fn to_gpu(&mut self, ctx: &ContextHandle, stream: &Arc<CudaStream>) { ... }
       /// Final extraction for PNG encode.
       pub fn into_rgba_image(self) -> RgbaImage { ... }
   }
   ```

2. **Change `&mut RgbaImage` → `&mut Canvas`** through the call chain:
   - `cached_static_base_image` returns `Canvas` instead of `RgbaImage`
   - `draw_variable_layers` takes `&mut Canvas`
   - `draw_chrome_and_colorbar` takes `&mut Canvas`
   - `render_to_image_profile_inner` returns `Canvas`
   - `render_to_image_profile` calls `canvas.into_rgba_image()` after
     downsample

3. **Each `draw_*` function dispatches on canvas variant**:
   ```rust
   fn draw_projected_lines(canvas: &mut Canvas, ...) {
       match canvas {
           Canvas::Cpu(img) => draw_projected_lines_cpu(img, ...),
           #[cfg(feature = "cuda")]
           Canvas::Gpu { buf, .. } => {
               // collect polylines, launch linework kernel directly on
               // the device buffer — no upload/download
               draw_projected_lines_gpu(buf, ...);
           }
       }
   }
   ```

4. **The static-base cache stays GPU-aware**: `cached_static_base_image`
   returns a `Canvas::Gpu` when feature `cuda` is on. This avoids the
   first upload entirely for the basemap layer.

5. **Downsample becomes the natural DL boundary**: `downsample` reads
   from device, writes to a smaller device buffer, then we DL once
   before PNG encode (or never, if we add a GPU PNG encoder).

### Phase order on canvas-resident path

```
Canvas::Gpu created (zeros + bg fill via fast kernel)
  ↓ polygon_fill kernel (basemap)
  ↓ rasterize_projected_grid kernel (data layer)
  ↓ raster_blit kernel (alpha composite)
  ↓ linework kernel (coastlines, borders)
  ↓ contour_lines kernel (contour lines)
  ↓ [text/barb/labels — see "Phases that should stay CPU"]
  ↓ chrome + colorbar (mostly text — likely CPU still)
  ↓ downsample + sharpen kernel (2x → 1x)
  ↓ DL once
  ↓ PNG encode (CPU, with Filter::Up)
```

Critical: text rendering (place labels, chrome titles, colorbar tick
labels) stays CPU. Font glyph rasterization is intricate and the time
budget is small. We DL the canvas, draw text on CPU, then either
re-upload for downsample OR push the downsample to a third buffer
pre-text. The cleanest split is: GPU does the data + linework + contour
into a pre-text canvas, then DL → CPU draws text + chrome → run
downsample on the final composited image.

Or — more aggressive — port text glyph rasterization to a GPU kernel
(takes glyph atlas + per-glyph positions, splats onto canvas). That's a
~1-week subproject by itself.

### Phases that should stay CPU

- **Text rendering** (`text::draw_text`, `draw_projected_place_labels`,
  chrome/colorbar): glyph atlas rasterization is awkward on GPU and the
  time budget is small.
- **Wind barbs** (`draw_barbs`): glyph splatting; CPU is fine.
- **Anti-aliased points/markers**: tiny work.
- **PNG encode**: already on `fdeflate` Fast path; `Filter::Up` swap
  shipped. NVJPEG only does JPEG. nvCOMP DEFLATE is heavyweight to
  integrate. Current 6 s aggregate is acceptable; revisit if it's the
  remaining bottleneck after canvas-resident lands.

### Estimated impact

After canvas-resident integration of all already-built kernels:
- **Pipeline total_ms: ~12-15 seconds** (vs 38 s today, vs 43 s CPU)
- **Peak SM utilization: 60-80% sustained**
- **At 200k plots/cycle: 25-30 hours aggregate compute saved per cycle**
  (vs 8 hours today)

## Critical conventions you MUST follow

These are baked into all existing kernels and the cudarc setup. Breaking
any of them silently corrupts numerics or causes runtime failures.

### NVRTC compile options

`rustwx-cuda-core/src/kernel.rs` sets:
```rust
CompileOptions {
    use_fast_math: Some(false),
    fmad: Some(false),
    ..Default::default()
}
```

- **`use_fast_math: false`** — reproducibility against the CPU metrust
  reference at ~1e-10 tolerance. Don't change.
- **`fmad: false`** — disables fused multiply-add. NVRTC defaults to
  `fmad: true` which fuses `a*b + c` into one rounding step on device,
  but Rust/LLVM doesn't fuse by default, so leaving FMAD on causes
  0.5-1 ULP drift on energy-magnitude composites
  (`moist_static_energy`, `montgomery_streamfunction`, anything of the
  form `a*b + c*d + e*f`). Don't change.

If you change either, **clear the PTX cache first**:
```
rm -rf ~/.cache/rustwx-cuda/ptx/
```
otherwise stale cached PTX hides your change.

### Kernel writing rules (NVRTC quirks)

- **No C++ lambdas in `__device__` code.** They sometimes work, sometimes
  miscompile. Use plain `__device__ __forceinline__` helper functions.
- **No fast-math intrinsics** like `__sinf`, `__cosf`, `__fdividef`.
  Use the standard libm names (`sinf`, `cosf`, `/`). `use_fast_math` is
  off so the intrinsics either don't exist or don't get the speedup.
- **Add `M_PI` manually** — NVRTC doesn't expose `<math.h>` macros.
  See `kernels/common/constants.cuh`.
- **`extern "C" __global__`** for kernel entry points — NVRTC respects
  C linkage so cudarc can find the symbol by name.
- **Don't `#include <math.h>` or other host headers** — NVRTC has a
  limited subset.

### Numerical contracts

| Kernel category | Tolerance |
|---|---|
| Calc kernels (severe, thermo, wind, grid) | byte-for-byte / ≤ 1e-10 vs metrust |
| `rasterize_grid` | byte-for-byte vs CPU rasterize_grid |
| `rasterize_projected_grid` | byte-for-byte (real HRRR shows ≤ 0.003% pixels diff with ≤ 38 LSB max from triangle-edge race on smooth fields — accepted) |
| `downsample_lanczos3` | ≤ 6/255 channel delta on data pixels (f32 vs f64 Lanczos3); text glyphs may diverge up to 50/255 from sub-pixel f32 vs f64 positioning. Visually identical. |
| `sharpen_3x3` | byte-for-byte against same kernel in f32 |
| `raster_blit` | byte-for-byte (forces dst alpha = 255 like CPU `blend_pixel`) |
| `polygon_fill` | byte-for-byte (same scanline algorithm) |
| `linework` | ≤ 2/255 channel delta (race on segment overlap within a polyline) |
| `contour_lines` | ≤ 2/255 channel delta (race on inter-level overlap) |

### Packed RGBA8 byte layout

All canvas buffers are `unsigned int*` per-pixel with this packing:
```
u32 = R | (G << 8) | (B << 16) | (A << 24)
```
which on little-endian hosts (x86_64, ARM64) reinterprets to bytes
`[R, G, B, A]` — matches `image::Rgba<u8>` exactly. This means a
`Vec<u32>` of GPU output can be cast to `Vec<u8>` and passed straight
to `RgbaImage::from_raw`. Helper: `u32_vec_to_rgba_bytes` in each
wrapper module.

### Per-thread streams API

In `rustwx-cuda-core::Context`:
- `ctx.stream()` returns the **shared default stream**. Multiple threads
  using this serialize on the GPU. Avoid for hot paths.
- `ctx.new_stream()` creates a **non-default stream**. Multiple
  non-default streams run concurrently on the GPU. Use this for
  per-rayon-worker streams.

In `rustwx-cuda-core::DeviceVec`:
- `from_host` / `zeros` / `copy_to_host` use the default stream.
- `from_host_on(stream)` / `zeros_on(stream)` / `copy_to_host_on(stream)`
  take a caller-supplied stream. **Always use the `_on` variants in new
  kernel wrappers** — they're the path to concurrency.

The thread-local stream cache pattern is in
`rustwx-render/src/rasterize.rs::with_thread_stream`. Copy it (don't
re-implement) for any new GPU swap site.

### NEVER touch from a CUDA wrapper

- `rustwx-render/Cargo.toml` workspace member list (already wired)
- `cudarc::driver::CudaContext::new` — keep the global init in
  `rustwx-cuda-core::global()`
- The PTX cache directory layout — `~/.cache/rustwx-cuda/ptx/`

## File map

```
rustwx-production-cuda/
├── kernels/
│   ├── common/
│   │   ├── constants.cuh         # physical constants, M_PI fix
│   │   ├── thermo_helpers.cuh
│   │   └── grid_helpers.cuh
│   ├── render/                   # new — render pipeline
│   │   ├── rasterize_grid.cu
│   │   ├── rasterize_projected_grid.cu
│   │   ├── downsample_lanczos3.cu
│   │   ├── sharpen_3x3.cu
│   │   ├── raster_blit.cu        # built, not wired
│   │   ├── polygon_fill.cu       # built, not wired
│   │   ├── linework.cu           # built, not wired
│   │   └── contour_lines.cu      # built, not wired
│   ├── thermo/                   # 70 calc kernels (earlier work)
│   ├── wind/
│   ├── grid/
│   └── severe/
├── crates/
│   ├── rustwx-cuda-core/         # shared infra (ctx, kernel cache, buffer)
│   ├── rustwx-cuda-render/       # render kernels — Rust wrappers
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── colormap.rs
│   │       ├── sources.rs
│   │       ├── rasterize_grid.rs
│   │       ├── rasterize_projected_grid.rs
│   │       ├── downsample.rs     # downsample + sharpen, fused
│   │       ├── raster_blit.rs    # not wired
│   │       ├── polygon_fill.rs   # not wired
│   │       ├── linework.rs       # not wired
│   │       └── contour_lines.rs  # not wired
│   ├── rustwx-cuda-thermo/       # 70 ported calc kernels
│   ├── rustwx-cuda-wind/         # 40 ported calc kernels
│   ├── rustwx-cuda-grid/         # 35 ported calc kernels
│   ├── rustwx-cuda-severe/       # 14 ported severe kernels
│   ├── rustwx-cuda/              # umbrella re-exports
│   └── rustwx-cuda-bench/        # synthetic-data benchmark binary
├── vendor/                       # vendored wx-* + metrust for verification
└── DIVERGENT_KERNELS.md          # 18 deferred kernels (numerical mismatch)

rustwx-production/                # production tree
├── crates/
│   ├── rustwx-render/
│   │   ├── Cargo.toml            # has optional rustwx-cuda dep + cuda feature
│   │   └── src/
│   │       ├── rasterize.rs      # GPU swap + per-thread stream + counters
│   │       └── render.rs         # downsample swap + linework helpers
│   ├── rustwx-calc/
│   │   ├── Cargo.toml            # cuda feature wires severe.rs swap
│   │   └── src/
│   │       └── severe.rs         # CUDA STP/EHI/SCP swaps (earlier work)
│   └── rustwx-cli/
│       └── Cargo.toml            # cuda feature propagates to render+calc
└── target/release/
    ├── hrrr_non_ecape_hour_cpu_v2.exe   # CPU baseline reference
    └── hrrr_non_ecape_hour_gpu_v11.exe  # current GPU best
```

## Build & test commands

```bash
# Verify CUDA kernels (in cuda workspace)
cd C:/Users/drew/claude-rustwx-prod/rustwx-production-cuda
cargo test -p rustwx-cuda-render --release -- --nocapture

# Build CPU baseline
cd C:/Users/drew/claude-rustwx-prod/rustwx-production
cargo build --release --bin hrrr_non_ecape_hour
cp target/release/hrrr_non_ecape_hour.exe target/release/hrrr_non_ecape_hour_cpu.exe

# Build GPU
cargo build --release --bin hrrr_non_ecape_hour --features cuda
cp target/release/hrrr_non_ecape_hour.exe target/release/hrrr_non_ecape_hour_gpu.exe

# Run with CUDA counters + per-phase timing
RUSTWX_CUDA_RASTERIZE_DEBUG=1 \
RUSTWX_CUDA_RASTERIZE_TIMING=1 \
target/release/hrrr_non_ecape_hour_gpu.exe \
  --date 20260504 --cycle 18 --forecast-hour 1 --region conus \
  --out-dir /c/Users/drew/rustwx/proof_gpu_test

# Watch GPU live during a run
nvidia-smi dmon -s u -c 60 -d 1
```

The 91-plot HRRR pipeline takes 60-90 s wall depending on network jitter
on NOMADS download. Internal `total_ms` in the report.json is the
reliable metric. Reports land at `<out-dir>/rustwx_hrrr_*_report.json`.

## Where to look first

If you're picking this up cold:

1. Read `rustwx-render/src/rasterize.rs` from the top — it shows the
   feature-gating pattern, per-thread stream cache, counters, and the
   rasterize_projected_grid swap.
2. Read `rustwx-render/src/render.rs::render_to_image_profile` and
   `cuda_downsample_then_sharpen` — the downsample swap pattern.
3. Read `rustwx-cuda-render/src/rasterize_projected_grid.rs` end-to-end
   — gold standard for kernel wrapper structure including the global
   mesh cache.
4. Read `rustwx-cuda-render/src/downsample.rs` — example of fused
   two-pass kernel and a synthetic correctness test.
5. Read `kernels/render/rasterize_projected_grid.cu` — gold standard
   kernel showing the conventions.

## Known issues / gotchas

- **Mesh cache is by `(slice_ptr, len, fingerprint)` not by content
  hash.** Slice pointer changes if the upstream LRU evicts and
  reallocates. Hit rate is currently 44% in the 91-plot test. At
  production scale (200k plots, ~few unique meshes) hit rate should
  be near 100%. If it isn't, switch to a content hash key — `xxhash`
  on the first/middle/last few KB is enough.
- **`use_fast_math: false` + `fmad: false` are load-bearing.** Don't
  enable them for any kernel without checking calc reproducibility.
  Render kernels could safely use fast math but the cost-benefit isn't
  there for current bottlenecks.
- **The PNG `Filter::Up` swap is global** — every caller of
  `encode_rgba_png_profile_with_options` gets it. If a caller depends
  on the old `Adaptive` behavior for max compression, they need a new
  `PngCompressionMode` variant.
- **Linework kernel has a per-polyline launch loop** for ordering
  preservation. This is correct but slow when called naively. Bulk-batch
  multiple polylines into one launch only if you accept ≤ 2 LSB race
  noise on segment intersections. The CPU code's last-write-wins
  semantics within a polyline is preserved.
- **GPU outputs are NOT byte-identical PNGs to CPU baseline** after
  the downsample swap — Lanczos3 in f32 vs f64 produces sub-LSB rounding
  differences on most pixels (≤ 6 channel delta) and larger differences
  (up to ~50) on AA text edges. Visually identical. If byte-identicality
  is a hard requirement, port Lanczos3 to f64 in the kernel — easy
  change, ~2× kernel time, still fast.
- **`stp_fixed` PNG showed catastrophic 12% pixel diff** at one point —
  that was a half-pixel bug in the Lanczos3 source center formula.
  Fixed; the formula is `(out + 0.5) * sratio - 0.5` matching
  `image::imageops::resize`. Documented in
  `kernels/render/downsample_lanczos3.cu`.
- **9 of 91 calls failed with `CUDA_ERROR_NOT_INITIALIZED`** on
  newly-spawned rayon threads. Fix shipped: explicit `bind_to_thread()`
  + retry in `with_thread_stream`. If it recurs, consider preheating
  rayon thread pool with a no-op CUDA call before the pipeline starts.

## Deferred work catalog (in DIVERGENT_KERNELS.md)

18 calc kernels are deferred because their CPU reference disagrees with
the metrust math we vendored. List in `DIVERGENT_KERNELS.md`. These are
NOT integration blockers; they're separate calc work for a future pass.

Examples: `smooth_n_point`, `showalter_index`, `cin`, `dcp`,
`heat_index`, `friction_velocity`, `bunkers_storm_motion`,
`corfidi_storm_motion`, `lat_lon_grid_deltas`, `apparent_temperature`,
altimeter conversions.

## Recommended next steps in order

1. **Canvas-resident pipeline** (1-2 weeks). The biggest single win
   left. Unwires the per-phase PCIe round-trip, lets the four built
   kernels (raster_blit, polygon_fill, linework, contour_lines)
   actually save wall time.
2. **GPU-resident calc → render handoff** (1 week after #1). Today
   `rasterize_projected_grid` uploads a 15 MB f64 grid per call. If
   the calc layer (which is already on GPU for severe composites)
   keeps the grids on device, the rasterize upload disappears entirely.
   Saves ~700 ms aggregate per pipeline (100% of `image_timing.upload`
   for the data field).
3. **Glyph-atlas text kernel** (1 week). Text rendering is the only
   reason to DL the canvas before downsample. Port it and the canvas
   stays GPU-resident through PNG encode.
4. **GPU PNG encoder via nvCOMP DEFLATE** (~3 days research + 1 week
   integration). Saves ~6 s aggregate. Not worth doing before #1-3.
5. **Pinned host memory for the data field upload** (1 day). Probably
   diminishing returns once #2 ships, but worth measuring before
   committing.
6. **Daemon mode for production** (~1 week). One process serves many
   cycles, keeping CUDA context + PTX modules + mesh cache + per-thread
   streams warm. Crucial at 200k plots/cycle scale where startup cost
   per process is real.

## Don't waste time on these

- **Pinned memory per-call** — already tried, was net negative because
  per-allocation page-locking dominates. Re-evaluate only if pinned
  buffer is reused across many calls.
- **Wiring `polygon_fill`** — basemap is already CPU-cached after the
  first call (`cached_static_base_image`). Aggregate is 1.1 s, mostly
  spent on first-call upload that GPU can't beat by enough to matter.
- **NVJPEG** — outputs JPEG only. Useless for our PNG outputs.
- **mtpng** — single-threaded performance is on par with `fdeflate`,
  multi-threaded helps only on > 128 KiB files and we already parallel
  per-recipe via rayon.
- **libdeflate as a `flate2` swap** — `image` 0.25 already uses
  `fdeflate` which beats libdeflate on the workload.

## Verification harness

Every CUDA kernel module has a `#[cfg(test)] mod tests` with at least
one test that:
1. Builds a synthetic input (or uses real grid for calc kernels)
2. Computes a CPU reference inline (drift detector — fails if upstream
   changes)
3. Runs the GPU path
4. Compares with documented tolerance

Pattern: test passes the `Result` of `rustwx_cuda_core::global()` and
returns early with `eprintln!("skip: ...")` if no GPU. Never panic on
missing GPU.

## How to claim a win

Always quote two metrics:
1. `image_timing.<phase>_ms` aggregate from report.json — that's what
   the swap directly affects
2. Pipeline `total_ms` from report.json — that's what the user feels

OS wall clock from `time` is dominated by network jitter on NOMADS;
don't trust it for compute-bound comparisons. Always do at least
two runs of each variant (warm cache) and report internal `total_ms`.

## Contact

User: `fahrenheitagi@gmail.com` — Fahrenheit Research. Their target
workload is 200k plots per cycle across 5 models/all GEFS members/all
continents. Frame all work against that scale.
