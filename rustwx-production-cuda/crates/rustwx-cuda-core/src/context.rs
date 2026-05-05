use std::sync::Arc;

use cudarc::driver::{CudaContext, CudaStream};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::Result;

/// Owning handle to a CUDA primary context + its default stream.
///
/// Every kernel module in this workspace shares a single global context per
/// process. That keeps the primary context warm, lets all kernels reuse the
/// PTX module cache, and amortizes PCIe setup.
pub struct Context {
    pub(crate) ctx: Arc<CudaContext>,
    pub(crate) stream: Arc<CudaStream>,
}

impl Context {
    pub fn new(ordinal: usize) -> Result<Self> {
        let ctx = CudaContext::new(ordinal)?;
        let stream = ctx.default_stream();
        Ok(Self { ctx, stream })
    }

    pub fn cuda(&self) -> &Arc<CudaContext> {
        &self.ctx
    }

    /// The shared default stream. Use this only for one-shot ops that don't
    /// need to run concurrently with other CPU threads' kernels. Most callers
    /// should prefer `Context::new_stream()` so different rayon workers
    /// actually overlap on the GPU instead of serializing through one queue.
    pub fn stream(&self) -> &Arc<CudaStream> {
        &self.stream
    }

    /// Create a fresh non-default CUDA stream. Multiple non-default streams
    /// run concurrently on the device, so callers running kernels from many
    /// CPU threads in parallel should give each thread its own stream
    /// (typically via a `thread_local!` cache).
    pub fn new_stream(&self) -> Result<Arc<CudaStream>> {
        Ok(self.ctx.new_stream()?)
    }

    pub fn synchronize(&self) -> Result<()> {
        self.stream.synchronize()?;
        Ok(())
    }
}

/// Cheap-clone shared handle. Kernel APIs in this workspace take `ContextHandle`.
pub type ContextHandle = Arc<Context>;

static GLOBAL: OnceCell<Mutex<Option<ContextHandle>>> = OnceCell::new();

/// Get-or-init the workspace-wide GPU context (device 0).
pub fn global() -> Result<ContextHandle> {
    let cell = GLOBAL.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock();
    if let Some(ctx) = guard.as_ref() {
        return Ok(Arc::clone(ctx));
    }
    let ctx = Arc::new(Context::new(0)?);
    *guard = Some(Arc::clone(&ctx));
    Ok(ctx)
}
