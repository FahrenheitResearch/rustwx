use std::sync::Arc;

use cudarc::driver::{CudaSlice, CudaStream, DeviceRepr, ValidAsZeroBits};

use crate::{ContextHandle, Error, Result};

/// Thin wrapper around `CudaSlice<T>`. The reason this exists at all is to
/// give callers a length they can read without going through `cudarc`'s
/// `len()` impl on a private type, and to keep the API symmetric with
/// `HostVec`.
pub struct DeviceVec<T: DeviceRepr> {
    pub(crate) inner: CudaSlice<T>,
    pub len: usize,
}

impl<T: DeviceRepr + ValidAsZeroBits> DeviceVec<T> {
    pub fn zeros(ctx: &ContextHandle, len: usize) -> Result<Self> {
        let inner = ctx.stream().alloc_zeros::<T>(len)?;
        Ok(Self { inner, len })
    }

    /// Allocate `len` zero-initialised elements on a caller-supplied stream.
    /// Use this when you need device-side concurrency across CPU threads —
    /// each thread runs its own stream so allocations + kernels overlap on
    /// the GPU instead of queuing through the default stream.
    pub fn zeros_on(stream: &Arc<CudaStream>, len: usize) -> Result<Self> {
        let inner = stream.alloc_zeros::<T>(len)?;
        Ok(Self { inner, len })
    }
}

impl<T: DeviceRepr> DeviceVec<T> {
    /// Upload host slice -> device, synchronously on the default stream.
    pub fn from_host(ctx: &ContextHandle, data: &[T]) -> Result<Self>
    where
        T: Copy,
    {
        let inner = ctx.stream().clone_htod(data)?;
        Ok(Self {
            inner,
            len: data.len(),
        })
    }

    /// Upload host slice -> device on a caller-supplied stream.
    pub fn from_host_on(stream: &Arc<CudaStream>, data: &[T]) -> Result<Self>
    where
        T: Copy,
    {
        let inner = stream.clone_htod(data)?;
        Ok(Self {
            inner,
            len: data.len(),
        })
    }

    /// Synchronously copy device -> new `Vec<T>`.
    pub fn copy_to_host(&self, ctx: &ContextHandle) -> Result<Vec<T>>
    where
        T: Copy + Default,
    {
        Ok(ctx.stream().clone_dtoh(&self.inner)?)
    }

    /// Copy device -> new `Vec<T>` on a caller-supplied stream.
    pub fn copy_to_host_on(&self, stream: &Arc<CudaStream>) -> Result<Vec<T>>
    where
        T: Copy + Default,
    {
        Ok(stream.clone_dtoh(&self.inner)?)
    }

    pub fn slice(&self) -> &CudaSlice<T> {
        &self.inner
    }
    pub fn slice_mut(&mut self) -> &mut CudaSlice<T> {
        &mut self.inner
    }
}

/// Marker — kept so kernel modules can wrap host vectors in a uniform type.
pub struct HostVec<T> {
    pub data: Vec<T>,
}

impl<T> HostVec<T> {
    pub fn new(data: Vec<T>) -> Self {
        Self { data }
    }
}

pub fn require_eq(what: &'static str, expected: usize, got: usize) -> Result<()> {
    if expected != got {
        return Err(Error::LengthMismatch {
            what,
            expected,
            got,
        });
    }
    Ok(())
}
