use super::{VolumeResult, VolumeStoreError};
use memmap2::{Mmap, MmapOptions};
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct ChunkPayload {
    len: u64,
    inner: Arc<ChunkPayloadInner>,
}

enum ChunkPayloadInner {
    Mmap(Mmap),
    File(Mutex<File>),
    Ram(Vec<u8>),
}

#[derive(Debug)]
pub(super) enum ChunkPayloadBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl ChunkPayload {
    pub(super) fn open(path: &Path) -> VolumeResult<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(Self {
                len,
                inner: Arc::new(ChunkPayloadInner::Ram(Vec::new())),
            });
        }

        let inner = if usize::try_from(len).is_ok() {
            // SAFETY: the map is read-only, the file handle is opened read-only,
            // and all consumers can only access bytes through checked ranges.
            match unsafe { MmapOptions::new().map(&file) } {
                Ok(mmap) => ChunkPayloadInner::Mmap(mmap),
                Err(_) => ChunkPayloadInner::File(Mutex::new(file)),
            }
        } else {
            ChunkPayloadInner::File(Mutex::new(file))
        };

        Ok(Self {
            len,
            inner: Arc::new(inner),
        })
    }

    pub(super) fn bytes(
        &self,
        chunk_id: usize,
        offset: u64,
        len: u32,
    ) -> VolumeResult<ChunkPayloadBytes<'_>> {
        let range = self.checked_range(chunk_id, offset, len)?;
        match self.inner.as_ref() {
            ChunkPayloadInner::Mmap(mmap) => {
                let (start, end) = range.usize_bounds(chunk_id)?;
                Ok(ChunkPayloadBytes::Borrowed(&mmap[start..end]))
            }
            ChunkPayloadInner::File(file) => {
                let mut bytes = vec![0u8; range.len];
                if !bytes.is_empty() {
                    let mut file = file.lock().map_err(|_| {
                        VolumeStoreError::InvalidChunk(
                            "chunks.bin file reader lock poisoned".to_string(),
                        )
                    })?;
                    file.seek(SeekFrom::Start(range.offset))?;
                    file.read_exact(&mut bytes)?;
                }
                Ok(ChunkPayloadBytes::Owned(bytes))
            }
            ChunkPayloadInner::Ram(bytes) => {
                let (start, end) = range.usize_bounds(chunk_id)?;
                Ok(ChunkPayloadBytes::Borrowed(&bytes[start..end]))
            }
        }
    }

    fn checked_range(&self, chunk_id: usize, offset: u64, len: u32) -> VolumeResult<CheckedRange> {
        let len_u64 = u64::from(len);
        let end = offset.checked_add(len_u64).ok_or_else(|| {
            VolumeStoreError::InvalidChunk(format!(
                "chunk {chunk_id} payload range offset {offset} len {len} overflows"
            ))
        })?;
        if end > self.len {
            return Err(VolumeStoreError::InvalidChunk(format!(
                "chunk {chunk_id} payload range {offset}..{end} exceeds chunks.bin length {}",
                self.len
            )));
        }
        let len = usize::try_from(len_u64).map_err(|_| {
            VolumeStoreError::InvalidChunk(format!(
                "chunk {chunk_id} payload length {len_u64} does not fit this platform"
            ))
        })?;
        Ok(CheckedRange { offset, len })
    }
}

impl AsRef<[u8]> for ChunkPayloadBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}

impl fmt::Debug for ChunkPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let backend = match self.inner.as_ref() {
            ChunkPayloadInner::Mmap(_) => "mmap",
            ChunkPayloadInner::File(_) => "file",
            ChunkPayloadInner::Ram(_) => "ram",
        };
        f.debug_struct("ChunkPayload")
            .field("len", &self.len)
            .field("backend", &backend)
            .finish()
    }
}

struct CheckedRange {
    offset: u64,
    len: usize,
}

impl CheckedRange {
    fn usize_bounds(&self, chunk_id: usize) -> VolumeResult<(usize, usize)> {
        let start = usize::try_from(self.offset).map_err(|_| {
            VolumeStoreError::InvalidChunk(format!(
                "chunk {chunk_id} payload offset {} does not fit this platform",
                self.offset
            ))
        })?;
        let end = start.checked_add(self.len).ok_or_else(|| {
            VolumeStoreError::InvalidChunk(format!(
                "chunk {chunk_id} payload range start {start} len {} overflows",
                self.len
            ))
        })?;
        Ok((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ram_payload(bytes: &[u8]) -> ChunkPayload {
        ChunkPayload {
            len: bytes.len() as u64,
            inner: Arc::new(ChunkPayloadInner::Ram(bytes.to_vec())),
        }
    }

    #[test]
    fn checked_range_returns_expected_bytes() {
        let payload = ram_payload(&[10, 11, 12, 13]);
        let bytes = payload.bytes(7, 1, 2).unwrap();
        assert_eq!(bytes.as_ref(), &[11, 12]);
    }

    #[test]
    fn checked_range_rejects_truncated_chunk_payload() {
        let payload = ram_payload(&[10, 11, 12, 13]);
        let err = payload.bytes(7, 3, 2).unwrap_err();
        assert!(err.to_string().contains("chunk 7 payload range 3..5"));
    }

    #[test]
    fn checked_range_rejects_offset_overflow() {
        let payload = ram_payload(&[10, 11, 12, 13]);
        let err = payload.bytes(7, u64::MAX, 1).unwrap_err();
        assert!(err.to_string().contains("overflows"));
    }

    #[test]
    fn empty_payload_allows_empty_range_at_start() {
        let payload = ram_payload(&[]);
        let bytes = payload.bytes(0, 0, 0).unwrap();
        assert!(bytes.as_ref().is_empty());
    }
}
