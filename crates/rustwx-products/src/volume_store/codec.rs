use super::index::{
    ChunkIndexRecord, FLAG_CONSTANT, FLAG_DENSE_I16, FLAG_EMPTY, FLAG_HAS_MISSING_SENTINEL,
};
use super::{VolumeResult, VolumeStoreError};

pub const MISSING_Q: i16 = i16::MIN;
pub const Q_MIN: i16 = i16::MIN + 1;
pub const Q_MAX: i16 = i16::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkCodec {
    AffineI16RawV0,
}

impl ChunkCodec {
    pub fn name(self) -> &'static str {
        match self {
            Self::AffineI16RawV0 => "affine_i16_raw_v0",
        }
    }

    pub fn from_name(name: &str) -> VolumeResult<Self> {
        match name {
            "affine_i16_raw_v0" => Ok(Self::AffineI16RawV0),
            other => Err(VolumeStoreError::InvalidManifest(format!(
                "unsupported volume codec '{other}'"
            ))),
        }
    }

    pub fn encode(self, values: &[f32]) -> VolumeResult<EncodedChunk> {
        match self {
            Self::AffineI16RawV0 => encode_affine_i16_raw(values),
        }
    }

    pub fn decode(
        self,
        record: ChunkIndexRecord,
        payload: &[u8],
        value_count: usize,
    ) -> VolumeResult<DecodedChunk> {
        match self {
            Self::AffineI16RawV0 => decode_affine_i16_raw(record, payload, value_count),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedChunk {
    pub record: ChunkIndexRecord,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedChunk {
    pub values: Vec<f32>,
}

fn encode_affine_i16_raw(values: &[f32]) -> VolumeResult<EncodedChunk> {
    if values.is_empty() {
        return Err(VolumeStoreError::InvalidChunk(
            "cannot encode empty chunk".to_string(),
        ));
    }

    let mut valid_min = f32::INFINITY;
    let mut valid_max = f32::NEG_INFINITY;
    let mut valid_count = 0usize;
    let mut has_missing = false;
    for value in values {
        if value.is_finite() {
            valid_min = valid_min.min(*value);
            valid_max = valid_max.max(*value);
            valid_count += 1;
        } else {
            has_missing = true;
        }
    }

    if valid_count == 0 {
        return Ok(EncodedChunk {
            record: ChunkIndexRecord::empty(),
            payload: Vec::new(),
        });
    }

    if (valid_max - valid_min).abs() <= f32::EPSILON {
        let mut record = ChunkIndexRecord::constant(valid_min);
        if has_missing {
            record.flags |= FLAG_HAS_MISSING_SENTINEL;
            let mut payload = Vec::with_capacity(values.len() * 2);
            for value in values {
                let q = if value.is_finite() { 0i16 } else { MISSING_Q };
                payload.extend_from_slice(&q.to_le_bytes());
            }
            record.flags |= FLAG_DENSE_I16;
            record.compressed_len = payload.len() as u32;
            record.uncompressed_len = payload.len() as u32;
            return Ok(EncodedChunk { record, payload });
        }
        return Ok(EncodedChunk {
            record,
            payload: Vec::new(),
        });
    }

    let center = 0.5 * (valid_min + valid_max);
    let scale = (valid_max - valid_min) / (2.0 * f32::from(Q_MAX));
    if !scale.is_finite() || scale <= 0.0 {
        return Err(VolumeStoreError::InvalidChunk(
            "invalid affine quantization scale".to_string(),
        ));
    }

    let mut payload = Vec::with_capacity(values.len() * 2);
    for value in values {
        let q = if value.is_finite() {
            ((*value - center) / scale)
                .round()
                .clamp(f32::from(Q_MIN), f32::from(Q_MAX)) as i16
        } else {
            MISSING_Q
        };
        payload.extend_from_slice(&q.to_le_bytes());
    }

    let mut flags = FLAG_DENSE_I16;
    if has_missing {
        flags |= FLAG_HAS_MISSING_SENTINEL;
    }
    Ok(EncodedChunk {
        record: ChunkIndexRecord {
            offset: 0,
            compressed_len: payload.len() as u32,
            uncompressed_len: payload.len() as u32,
            center,
            scale,
            valid_min,
            valid_max,
            flags,
        },
        payload,
    })
}

fn decode_affine_i16_raw(
    record: ChunkIndexRecord,
    payload: &[u8],
    value_count: usize,
) -> VolumeResult<DecodedChunk> {
    if record.flags & FLAG_EMPTY != 0 {
        return Ok(DecodedChunk {
            values: vec![f32::NAN; value_count],
        });
    }
    if record.flags & FLAG_CONSTANT != 0 && record.flags & FLAG_DENSE_I16 == 0 {
        return Ok(DecodedChunk {
            values: vec![record.center; value_count],
        });
    }
    if record.flags & FLAG_DENSE_I16 == 0 {
        return Err(VolumeStoreError::InvalidChunk(format!(
            "chunk flags {} do not include a dense i16 payload",
            record.flags
        )));
    }
    let expected_len = value_count * 2;
    if payload.len() != expected_len {
        return Err(VolumeStoreError::InvalidChunk(format!(
            "dense i16 payload has {} bytes, expected {}",
            payload.len(),
            expected_len
        )));
    }

    let mut values = Vec::with_capacity(value_count);
    for pair in payload.chunks_exact(2) {
        let q = i16::from_le_bytes(pair.try_into().unwrap());
        if q == MISSING_Q {
            values.push(f32::NAN);
        } else if record.flags & FLAG_CONSTANT != 0 {
            values.push(record.center);
        } else {
            values.push(record.center + record.scale * f32::from(q));
        }
    }
    Ok(DecodedChunk { values })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn affine_i16_round_trip_keeps_error_bounded() {
        let values = vec![250.0, 252.5, 255.0, f32::NAN, 260.0, 262.0];
        let encoded = ChunkCodec::AffineI16RawV0.encode(&values).unwrap();
        assert!(encoded.record.flags & FLAG_DENSE_I16 != 0);
        assert!(encoded.record.flags & FLAG_HAS_MISSING_SENTINEL != 0);
        let decoded = ChunkCodec::AffineI16RawV0
            .decode(encoded.record, &encoded.payload, values.len())
            .unwrap();
        for (source, round_trip) in values.iter().zip(decoded.values.iter()) {
            if source.is_finite() {
                assert!((source - round_trip).abs() <= encoded.record.scale);
            } else {
                assert!(round_trip.is_nan());
            }
        }
    }

    #[test]
    fn constant_chunk_uses_no_payload() {
        let values = vec![12.5; 16];
        let encoded = ChunkCodec::AffineI16RawV0.encode(&values).unwrap();
        assert_eq!(encoded.record.flags, FLAG_CONSTANT);
        assert!(encoded.payload.is_empty());
        let decoded = ChunkCodec::AffineI16RawV0
            .decode(encoded.record, &encoded.payload, values.len())
            .unwrap();
        assert_eq!(decoded.values, values);
    }

    #[test]
    fn all_missing_chunk_uses_empty_flag() {
        let values = vec![f32::NAN; 8];
        let encoded = ChunkCodec::AffineI16RawV0.encode(&values).unwrap();
        assert_eq!(encoded.record.flags, FLAG_EMPTY);
        let decoded = ChunkCodec::AffineI16RawV0
            .decode(encoded.record, &encoded.payload, values.len())
            .unwrap();
        assert!(decoded.values.iter().all(|value| value.is_nan()));
    }
}
