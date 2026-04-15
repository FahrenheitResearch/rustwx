use thiserror::Error;

#[derive(Debug, Error)]
pub enum CalcError {
    #[error("length mismatch for {field}: expected {expected}, got {actual}")]
    LengthMismatch {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("storm_u and storm_v must either both be provided or both be omitted")]
    InvalidStormMotionPair,
    #[error("metrust error: {0}")]
    Metrust(String),
}
