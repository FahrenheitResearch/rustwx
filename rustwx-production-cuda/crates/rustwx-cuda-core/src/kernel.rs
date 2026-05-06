use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use cudarc::driver::{CudaFunction, CudaModule, LaunchConfig};
use cudarc::nvrtc::{compile_ptx_with_opts, CompileOptions, Ptx};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;

use crate::{ContextHandle, Result};

/// Process-wide cache of compiled+loaded modules, keyed by stable string id.
struct ModuleCache {
    modules: RwLock<HashMap<&'static str, Arc<CudaModule>>>,
}

static CACHE: OnceCell<ModuleCache> = OnceCell::new();

fn cache() -> &'static ModuleCache {
    CACHE.get_or_init(|| ModuleCache {
        modules: RwLock::new(HashMap::new()),
    })
}

/// Compile (or load from disk cache) the given `.cu` source string into PTX,
/// load it onto the device, and stash the resulting module under `cache_key`.
///
/// Subsequent calls with the same key are no-ops.
pub fn compile_or_load_ptx(
    ctx: &ContextHandle,
    cache_key: &'static str,
    cu_source: &str,
) -> Result<Arc<CudaModule>> {
    {
        let r = cache().modules.read();
        if let Some(m) = r.get(cache_key) {
            return Ok(Arc::clone(m));
        }
    }
    let ptx = load_or_compile(cache_key, cu_source)?;
    let module = ctx.cuda().load_module(ptx)?;
    let mut w = cache().modules.write();
    let entry = w.entry(cache_key).or_insert_with(|| Arc::clone(&module));
    Ok(Arc::clone(entry))
}

/// Look up a function by name on a previously-loaded module.
pub fn function(module: &Arc<CudaModule>, name: &str) -> Result<CudaFunction> {
    Ok(module.load_function(name)?)
}

fn load_or_compile(cache_key: &str, cu_source: &str) -> Result<Ptx> {
    let cache_dir = ptx_cache_dir();
    let _ = fs::create_dir_all(&cache_dir);

    let mut hasher = DefaultHasher::new();
    cu_source.hash(&mut hasher);
    let src_hash = hasher.finish();
    let cache_file = cache_dir.join(format!("{cache_key}-{src_hash:016x}.ptx"));

    if let Ok(bytes) = fs::read(&cache_file) {
        if let Ok(s) = String::from_utf8(bytes) {
            return Ok(Ptx::from_src(s));
        }
    }

    let opts = CompileOptions {
        // No --use_fast_math: parcel-ascent reproducibility matters more
        // than cycles. Match metrust to ~1e-10.
        use_fast_math: Some(false),
        // Disable FMAD too. NVRTC defaults `--fmad=true`, which fuses
        // multiply-add into a single rounding step on device. CPU code
        // (rustc/LLVM) does NOT fuse by default, so leaving FMAD on causes
        // 0.5–1 ULP disagreement on energy-magnitude composites
        // (moist_static_energy, montgomery_streamfunction, anything of the
        // form `a*b + c*d + e*f`). Cost is one extra round per FMA — small.
        fmad: Some(false),
        ..Default::default()
    };
    let ptx = compile_ptx_with_opts(cu_source, opts)?;
    let _ = fs::write(&cache_file, ptx.to_src());
    Ok(ptx)
}

fn ptx_cache_dir() -> PathBuf {
    if let Ok(p) = std::env::var("RUSTWX_CUDA_CACHE") {
        return PathBuf::from(p);
    }
    if let Some(home) = dirs_home() {
        return home.join(".cache").join("rustwx-cuda").join("ptx");
    }
    std::env::temp_dir().join("rustwx-cuda-ptx")
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Loaded-module + key, returned by `KernelModule::load`.
pub struct KernelModule {
    pub module: Arc<CudaModule>,
    pub module_key: &'static str,
}

impl KernelModule {
    pub fn load(ctx: &ContextHandle, module_key: &'static str, cu_source: &str) -> Result<Self> {
        let module = compile_or_load_ptx(ctx, module_key, cu_source)?;
        Ok(Self { module, module_key })
    }

    pub fn function(&self, name: &str) -> Result<CudaFunction> {
        function(&self.module, name)
    }
}

/// 1D launch geometry helper.
pub fn launch_cfg_1d(n: usize, block: u32) -> LaunchConfig {
    let block = block.max(1);
    let n = n as u32;
    let grid = (n.saturating_add(block - 1)) / block;
    LaunchConfig {
        grid_dim: (grid.max(1), 1, 1),
        block_dim: (block, 1, 1),
        shared_mem_bytes: 0,
    }
}

/// 2D launch geometry helper for `nx * ny` grids.
pub fn launch_cfg_2d(nx: u32, ny: u32, block_x: u32, block_y: u32) -> LaunchConfig {
    let bx = block_x.max(1);
    let by = block_y.max(1);
    LaunchConfig {
        grid_dim: ((nx + bx - 1) / bx, (ny + by - 1) / by, 1),
        block_dim: (bx, by, 1),
        shared_mem_bytes: 0,
    }
}

pub use cudarc::driver::LaunchConfig as LaunchCfg;
