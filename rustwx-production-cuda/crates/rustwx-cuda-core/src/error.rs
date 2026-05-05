use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("CUDA driver error: {0}")]
    Driver(#[from] cudarc::driver::DriverError),

    #[error("NVRTC compile error: {0}")]
    Nvrtc(#[from] cudarc::nvrtc::CompileError),

    #[error("kernel `{kernel}` not found in module `{module}`")]
    KernelNotFound { module: String, kernel: String },

    #[error("input array length mismatch: {what} (expected {expected}, got {got})")]
    LengthMismatch {
        what: &'static str,
        expected: usize,
        got: usize,
    },

    #[error("array must be C-contiguous on the FFI boundary; refusing F-order layout")]
    NonContiguous,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
