use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Component, Path, PathBuf};

pub const TRAINING_SHARD_FORMAT: &str = "rustwx-training-shard-v0";
pub const TRAINING_SHARD_MANIFEST_FILE: &str = "manifest.json";
pub const TRAINING_SHARD_INDEX_FILE: &str = "index.jsonl";
pub const TRAINING_SHARD_INDEX_FORMAT: &str = "jsonl";

pub type TrainingShardResult<T> = Result<T, TrainingShardError>;

#[derive(Debug)]
pub enum TrainingShardError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidManifest(String),
    InvalidIndex(String),
    InvalidSample(String),
    MissingTensor(String),
    MissingSourceGroup(String),
    UnsupportedEncoding(String),
}

impl std::fmt::Display for TrainingShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Json(err) => write!(f, "JSON error: {err}"),
            Self::InvalidManifest(message) => write!(f, "invalid shard manifest: {message}"),
            Self::InvalidIndex(message) => write!(f, "invalid shard index: {message}"),
            Self::InvalidSample(message) => write!(f, "invalid shard sample: {message}"),
            Self::MissingTensor(name) => write!(f, "missing tensor: {name}"),
            Self::MissingSourceGroup(group) => write!(f, "missing source group: {group}"),
            Self::UnsupportedEncoding(encoding) => {
                write!(f, "unsupported shard encoding: {encoding}")
            }
        }
    }
}

impl std::error::Error for TrainingShardError {}

impl From<std::io::Error> for TrainingShardError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for TrainingShardError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainingShardDType {
    F32,
    I16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrainingShardTensorEncoding {
    #[serde(rename = "f32_le_raw_v0")]
    F32LeRawV0,
    #[serde(rename = "affine_i16_raw_v0")]
    AffineI16RawV0,
}

impl TrainingShardTensorEncoding {
    pub fn name(self) -> &'static str {
        match self {
            Self::F32LeRawV0 => "f32_le_raw_v0",
            Self::AffineI16RawV0 => "affine_i16_raw_v0",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrainingShardQuantizationMode {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "affine_i16_raw_v0")]
    AffineI16RawV0,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingShardSourceGroupSpec {
    pub id: String,
    pub blob_path: String,
    pub encoding: TrainingShardTensorEncoding,
}

impl TrainingShardSourceGroupSpec {
    pub fn new(
        id: impl Into<String>,
        blob_path: impl Into<String>,
        encoding: TrainingShardTensorEncoding,
    ) -> Self {
        Self {
            id: id.into(),
            blob_path: blob_path.into(),
            encoding,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingShardTensorSpec {
    pub name: String,
    pub source_group: String,
    pub dtype: TrainingShardDType,
    pub encoding: TrainingShardTensorEncoding,
    pub shape: Vec<usize>,
    pub blob_path: String,
    pub per_sample_elements: u64,
    pub per_sample_bytes: u64,
}

impl TrainingShardTensorSpec {
    pub fn f32_raw(
        name: impl Into<String>,
        source_group: impl Into<String>,
        shape: impl Into<Vec<usize>>,
    ) -> TrainingShardResult<Self> {
        let name = name.into();
        let source_group = source_group.into();
        let shape = shape.into();
        let per_sample_elements = checked_shape_elements(&shape)?;
        let per_sample_bytes = per_sample_elements.checked_mul(4).ok_or_else(|| {
            TrainingShardError::InvalidManifest(format!(
                "tensor '{name}' per-sample byte count overflows"
            ))
        })?;
        Ok(Self {
            name,
            blob_path: format!("{source_group}_f32.bin"),
            source_group,
            dtype: TrainingShardDType::F32,
            encoding: TrainingShardTensorEncoding::F32LeRawV0,
            shape,
            per_sample_elements,
            per_sample_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingShardManifest {
    pub format: String,
    pub shard_id: String,
    pub sample_count: u64,
    pub index_path: String,
    pub index_format: String,
    pub quantization_mode: TrainingShardQuantizationMode,
    pub source_groups: Vec<TrainingShardSourceGroupSpec>,
    pub tensors: Vec<TrainingShardTensorSpec>,
    pub completed: bool,
}

impl TrainingShardManifest {
    pub fn new(
        shard_id: impl Into<String>,
        tensors: Vec<TrainingShardTensorSpec>,
    ) -> TrainingShardResult<Self> {
        let mut groups = BTreeMap::<String, TrainingShardSourceGroupSpec>::new();
        for tensor in &tensors {
            let group = TrainingShardSourceGroupSpec::new(
                tensor.source_group.clone(),
                tensor.blob_path.clone(),
                tensor.encoding,
            );
            if let Some(existing) = groups.insert(tensor.source_group.clone(), group.clone()) {
                if existing.blob_path != group.blob_path || existing.encoding != group.encoding {
                    return Err(TrainingShardError::InvalidManifest(format!(
                        "source group '{}' has conflicting blob definitions",
                        tensor.source_group
                    )));
                }
            }
        }
        let manifest = Self {
            format: TRAINING_SHARD_FORMAT.to_string(),
            shard_id: shard_id.into(),
            sample_count: 0,
            index_path: TRAINING_SHARD_INDEX_FILE.to_string(),
            index_format: TRAINING_SHARD_INDEX_FORMAT.to_string(),
            quantization_mode: TrainingShardQuantizationMode::None,
            source_groups: groups.into_values().collect(),
            tensors,
            completed: false,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> TrainingShardResult<()> {
        if self.format != TRAINING_SHARD_FORMAT {
            return Err(TrainingShardError::InvalidManifest(format!(
                "unsupported format '{}'",
                self.format
            )));
        }
        if self.shard_id.trim().is_empty() {
            return Err(TrainingShardError::InvalidManifest(
                "shard_id cannot be blank".to_string(),
            ));
        }
        validate_relative_file_path(&self.index_path, "index_path")?;
        if self.index_format != TRAINING_SHARD_INDEX_FORMAT {
            return Err(TrainingShardError::InvalidManifest(format!(
                "unsupported index format '{}'",
                self.index_format
            )));
        }
        if self.tensors.is_empty() {
            return Err(TrainingShardError::InvalidManifest(
                "at least one tensor is required".to_string(),
            ));
        }
        if self.source_groups.is_empty() {
            return Err(TrainingShardError::InvalidManifest(
                "at least one source group is required".to_string(),
            ));
        }

        let mut group_ids = BTreeSet::new();
        let mut group_paths = BTreeMap::<&str, (&str, TrainingShardTensorEncoding)>::new();
        for group in &self.source_groups {
            if group.id.trim().is_empty() {
                return Err(TrainingShardError::InvalidManifest(
                    "source group id cannot be blank".to_string(),
                ));
            }
            if !group_ids.insert(group.id.as_str()) {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "duplicate source group '{}'",
                    group.id
                )));
            }
            validate_relative_file_path(&group.blob_path, "source group blob_path")?;
            group_paths.insert(
                group.id.as_str(),
                (group.blob_path.as_str(), group.encoding),
            );
        }

        let mut tensor_names = BTreeSet::new();
        for tensor in &self.tensors {
            if tensor.name.trim().is_empty() {
                return Err(TrainingShardError::InvalidManifest(
                    "tensor name cannot be blank".to_string(),
                ));
            }
            if !tensor_names.insert(tensor.name.as_str()) {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "duplicate tensor '{}'",
                    tensor.name
                )));
            }
            if tensor.shape.is_empty() || tensor.shape.iter().any(|dim| *dim == 0) {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' shape dimensions must all be positive",
                    tensor.name
                )));
            }
            let elements = checked_shape_elements(&tensor.shape)?;
            if tensor.per_sample_elements != elements {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' per_sample_elements does not match shape",
                    tensor.name
                )));
            }
            let bytes_per_element = match tensor.dtype {
                TrainingShardDType::F32 => 4,
                TrainingShardDType::I16 => 2,
            };
            let expected_bytes = elements.checked_mul(bytes_per_element).ok_or_else(|| {
                TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' per-sample byte count overflows",
                    tensor.name
                ))
            })?;
            if tensor.per_sample_bytes != expected_bytes {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' per_sample_bytes does not match dtype and shape",
                    tensor.name
                )));
            }
            let Some((blob_path, group_encoding)) = group_paths.get(tensor.source_group.as_str())
            else {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' references unknown source group '{}'",
                    tensor.name, tensor.source_group
                )));
            };
            if tensor.blob_path != *blob_path {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' blob_path does not match source group",
                    tensor.name
                )));
            }
            if tensor.encoding != *group_encoding {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' encoding does not match source group",
                    tensor.name
                )));
            }
            if tensor.encoding == TrainingShardTensorEncoding::F32LeRawV0
                && tensor.dtype != TrainingShardDType::F32
            {
                return Err(TrainingShardError::InvalidManifest(format!(
                    "tensor '{}' f32 raw encoding requires f32 dtype",
                    tensor.name
                )));
            }
        }
        Ok(())
    }

    pub fn tensor(&self, name: &str) -> Option<&TrainingShardTensorSpec> {
        self.tensors.iter().find(|tensor| tensor.name == name)
    }

    pub fn source_group(&self, id: &str) -> Option<&TrainingShardSourceGroupSpec> {
        self.source_groups.iter().find(|group| group.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingShardTensorSlice {
    pub tensor: String,
    pub source_group: String,
    pub blob_path: String,
    pub offset_bytes: u64,
    pub byte_len: u64,
    pub element_count: u64,
    pub encoding: TrainingShardTensorEncoding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingShardSampleIndex {
    pub sample_id: String,
    pub sample_ordinal: u64,
    pub tensors: Vec<TrainingShardTensorSlice>,
}

impl TrainingShardSampleIndex {
    pub fn tensor(&self, name: &str) -> Option<&TrainingShardTensorSlice> {
        self.tensors.iter().find(|tensor| tensor.tensor == name)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TrainingShardSampleTensor<'a> {
    pub name: &'a str,
    pub values: &'a [f32],
}

pub struct TrainingShardWriter {
    root: PathBuf,
    manifest: TrainingShardManifest,
    blob_writers: BTreeMap<String, BufWriter<File>>,
    blob_offsets: BTreeMap<String, u64>,
    index_writer: BufWriter<File>,
}

impl TrainingShardWriter {
    pub fn create(
        root: impl AsRef<Path>,
        mut manifest: TrainingShardManifest,
    ) -> TrainingShardResult<Self> {
        manifest.sample_count = 0;
        manifest.completed = false;
        manifest.validate()?;

        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;

        let mut blob_writers = BTreeMap::new();
        let mut blob_offsets = BTreeMap::new();
        for group in &manifest.source_groups {
            let path = root.join(&group.blob_path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = File::create(&path)?;
            blob_writers.insert(group.id.clone(), BufWriter::new(file));
            blob_offsets.insert(group.id.clone(), 0);
        }

        let index_path = root.join(&manifest.index_path);
        let index_writer = BufWriter::new(File::create(&index_path)?);
        write_manifest_atomic(&root.join(TRAINING_SHARD_MANIFEST_FILE), &manifest)?;

        Ok(Self {
            root,
            manifest,
            blob_writers,
            blob_offsets,
            index_writer,
        })
    }

    pub fn manifest(&self) -> &TrainingShardManifest {
        &self.manifest
    }

    pub fn append_sample(
        &mut self,
        sample_id: impl Into<String>,
        tensors: &[TrainingShardSampleTensor<'_>],
    ) -> TrainingShardResult<TrainingShardSampleIndex> {
        let sample_id = sample_id.into();
        if sample_id.trim().is_empty() {
            return Err(TrainingShardError::InvalidSample(
                "sample_id cannot be blank".to_string(),
            ));
        }
        let mut provided = BTreeMap::<&str, &[f32]>::new();
        for tensor in tensors {
            if self.manifest.tensor(tensor.name).is_none() {
                return Err(TrainingShardError::InvalidSample(format!(
                    "unexpected tensor '{}'",
                    tensor.name
                )));
            }
            if provided.insert(tensor.name, tensor.values).is_some() {
                return Err(TrainingShardError::InvalidSample(format!(
                    "duplicate tensor '{}'",
                    tensor.name
                )));
            }
        }

        let mut slices = Vec::with_capacity(self.manifest.tensors.len());
        let specs = self.manifest.tensors.clone();
        for spec in &specs {
            if spec.encoding != TrainingShardTensorEncoding::F32LeRawV0 {
                return Err(TrainingShardError::UnsupportedEncoding(
                    spec.encoding.name().to_string(),
                ));
            }
            let values = provided
                .get(spec.name.as_str())
                .ok_or_else(|| TrainingShardError::MissingTensor(spec.name.clone()))?;
            if values.len() as u64 != spec.per_sample_elements {
                return Err(TrainingShardError::InvalidSample(format!(
                    "tensor '{}' expected {} f32 values, got {}",
                    spec.name,
                    spec.per_sample_elements,
                    values.len()
                )));
            }
            let offset = *self
                .blob_offsets
                .get(spec.source_group.as_str())
                .ok_or_else(|| TrainingShardError::MissingSourceGroup(spec.source_group.clone()))?;
            let writer = self
                .blob_writers
                .get_mut(spec.source_group.as_str())
                .ok_or_else(|| TrainingShardError::MissingSourceGroup(spec.source_group.clone()))?;
            write_f32_le(writer, values)?;
            let byte_len = spec.per_sample_bytes;
            self.blob_offsets
                .insert(spec.source_group.clone(), offset + byte_len);
            slices.push(TrainingShardTensorSlice {
                tensor: spec.name.clone(),
                source_group: spec.source_group.clone(),
                blob_path: spec.blob_path.clone(),
                offset_bytes: offset,
                byte_len,
                element_count: spec.per_sample_elements,
                encoding: spec.encoding,
            });
        }

        for writer in self.blob_writers.values_mut() {
            writer.flush()?;
        }

        let record = TrainingShardSampleIndex {
            sample_id,
            sample_ordinal: self.manifest.sample_count,
            tensors: slices,
        };
        serde_json::to_writer(&mut self.index_writer, &record)?;
        self.index_writer.write_all(b"\n")?;
        self.index_writer.flush()?;

        self.manifest.sample_count = self.manifest.sample_count.saturating_add(1);
        write_manifest_atomic(
            &self.root.join(TRAINING_SHARD_MANIFEST_FILE),
            &self.manifest,
        )?;
        Ok(record)
    }

    pub fn finish(mut self) -> TrainingShardResult<TrainingShardManifest> {
        for writer in self.blob_writers.values_mut() {
            writer.flush()?;
        }
        self.index_writer.flush()?;
        self.manifest.completed = true;
        write_manifest_atomic(
            &self.root.join(TRAINING_SHARD_MANIFEST_FILE),
            &self.manifest,
        )?;
        Ok(self.manifest)
    }
}

pub struct TrainingShardReader {
    root: PathBuf,
    manifest: TrainingShardManifest,
    index: Vec<TrainingShardSampleIndex>,
    blobs: BTreeMap<String, TrainingShardBlob>,
}

impl TrainingShardReader {
    pub fn open(root: impl AsRef<Path>) -> TrainingShardResult<Self> {
        let root = root.as_ref().to_path_buf();
        let manifest_bytes = fs::read(root.join(TRAINING_SHARD_MANIFEST_FILE))?;
        let manifest: TrainingShardManifest = serde_json::from_slice(&manifest_bytes)?;
        manifest.validate()?;

        let index = read_index_jsonl(&root.join(&manifest.index_path), &manifest)?;
        let mut blobs = BTreeMap::new();
        for group in &manifest.source_groups {
            let path = root.join(&group.blob_path);
            blobs.insert(group.id.clone(), TrainingShardBlob::open(&path)?);
        }
        Ok(Self {
            root,
            manifest,
            index,
            blobs,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifest(&self) -> &TrainingShardManifest {
        &self.manifest
    }

    pub fn index(&self) -> &[TrainingShardSampleIndex] {
        &self.index
    }

    pub fn sample(&self, sample_ordinal: u64) -> Option<&TrainingShardSampleIndex> {
        self.index
            .iter()
            .find(|sample| sample.sample_ordinal == sample_ordinal)
    }

    pub fn tensor_bytes(
        &self,
        sample_ordinal: u64,
        tensor_name: &str,
    ) -> TrainingShardResult<TrainingShardTensorBytes<'_>> {
        let sample = self.sample(sample_ordinal).ok_or_else(|| {
            TrainingShardError::InvalidIndex(format!("missing sample ordinal {sample_ordinal}"))
        })?;
        let slice = sample
            .tensor(tensor_name)
            .ok_or_else(|| TrainingShardError::MissingTensor(tensor_name.to_string()))?;
        let spec = self
            .manifest
            .tensor(tensor_name)
            .ok_or_else(|| TrainingShardError::MissingTensor(tensor_name.to_string()))?;
        let blob = self
            .blobs
            .get(slice.source_group.as_str())
            .ok_or_else(|| TrainingShardError::MissingSourceGroup(slice.source_group.clone()))?;
        let bytes = blob.bytes(slice.offset_bytes, slice.byte_len)?;
        Ok(TrainingShardTensorBytes { spec, slice, bytes })
    }

    pub fn read_tensor_f32_le(
        &self,
        sample_ordinal: u64,
        tensor_name: &str,
    ) -> TrainingShardResult<Vec<f32>> {
        let tensor = self.tensor_bytes(sample_ordinal, tensor_name)?;
        if tensor.slice.encoding != TrainingShardTensorEncoding::F32LeRawV0 {
            return Err(TrainingShardError::UnsupportedEncoding(
                tensor.slice.encoding.name().to_string(),
            ));
        }
        if tensor.bytes.len() % 4 != 0 {
            return Err(TrainingShardError::InvalidIndex(format!(
                "tensor '{}' byte length {} is not divisible by 4",
                tensor_name,
                tensor.bytes.len()
            )));
        }
        Ok(tensor
            .bytes
            .chunks_exact(4)
            .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect())
    }
}

pub struct TrainingShardTensorBytes<'a> {
    pub spec: &'a TrainingShardTensorSpec,
    pub slice: &'a TrainingShardTensorSlice,
    pub bytes: &'a [u8],
}

enum TrainingShardBlob {
    Mmap(Mmap),
    Empty,
}

impl TrainingShardBlob {
    fn open(path: &Path) -> TrainingShardResult<Self> {
        let file = File::open(path)?;
        if file.metadata()?.len() == 0 {
            return Ok(Self::Empty);
        }
        // SAFETY: the mmap is read-only, the file handle is opened read-only,
        // and readers only receive slices after explicit range checks.
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Ok(Self::Mmap(mmap))
    }

    fn bytes(&self, offset: u64, len: u64) -> TrainingShardResult<&[u8]> {
        let len = usize::try_from(len).map_err(|_| {
            TrainingShardError::InvalidIndex(format!("byte length {len} does not fit platform"))
        })?;
        let start = usize::try_from(offset).map_err(|_| {
            TrainingShardError::InvalidIndex(format!("byte offset {offset} does not fit platform"))
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            TrainingShardError::InvalidIndex(format!(
                "byte range offset {offset} len {len} overflows"
            ))
        })?;
        let bytes = match self {
            Self::Mmap(mmap) => mmap.as_ref(),
            Self::Empty => &[],
        };
        if end > bytes.len() {
            return Err(TrainingShardError::InvalidIndex(format!(
                "byte range {start}..{end} exceeds blob length {}",
                bytes.len()
            )));
        }
        Ok(&bytes[start..end])
    }
}

fn read_index_jsonl(
    path: &Path,
    manifest: &TrainingShardManifest,
) -> TrainingShardResult<Vec<TrainingShardSampleIndex>> {
    let file = File::open(path)?;
    let mut index = Vec::new();
    for (line_index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: TrainingShardSampleIndex = serde_json::from_str(&line)?;
        validate_index_record(&record, manifest, index.len() as u64, line_index + 1)?;
        index.push(record);
    }
    if manifest.sample_count != index.len() as u64 {
        return Err(TrainingShardError::InvalidIndex(format!(
            "manifest sample_count {} does not match {} index records",
            manifest.sample_count,
            index.len()
        )));
    }
    Ok(index)
}

fn validate_index_record(
    record: &TrainingShardSampleIndex,
    manifest: &TrainingShardManifest,
    expected_ordinal: u64,
    line_number: usize,
) -> TrainingShardResult<()> {
    if record.sample_id.trim().is_empty() {
        return Err(TrainingShardError::InvalidIndex(format!(
            "line {line_number} sample_id is blank"
        )));
    }
    if record.sample_ordinal != expected_ordinal {
        return Err(TrainingShardError::InvalidIndex(format!(
            "line {line_number} sample ordinal {} should be {expected_ordinal}",
            record.sample_ordinal
        )));
    }
    if record.tensors.len() != manifest.tensors.len() {
        return Err(TrainingShardError::InvalidIndex(format!(
            "line {line_number} has {} tensor slices, expected {}",
            record.tensors.len(),
            manifest.tensors.len()
        )));
    }
    for spec in &manifest.tensors {
        let Some(slice) = record.tensor(&spec.name) else {
            return Err(TrainingShardError::InvalidIndex(format!(
                "line {line_number} missing tensor '{}'",
                spec.name
            )));
        };
        if slice.source_group != spec.source_group
            || slice.blob_path != spec.blob_path
            || slice.byte_len != spec.per_sample_bytes
            || slice.element_count != spec.per_sample_elements
            || slice.encoding != spec.encoding
        {
            return Err(TrainingShardError::InvalidIndex(format!(
                "line {line_number} tensor '{}' metadata does not match manifest",
                spec.name
            )));
        }
    }
    Ok(())
}

fn write_f32_le<W: Write>(writer: &mut W, values: &[f32]) -> TrainingShardResult<()> {
    let mut bytes = Vec::with_capacity(64 * 1024 * 4);
    for chunk in values.chunks(64 * 1024) {
        bytes.clear();
        for value in chunk {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        writer.write_all(&bytes)?;
    }
    Ok(())
}

fn write_manifest_atomic(path: &Path, manifest: &TrainingShardManifest) -> TrainingShardResult<()> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(TRAINING_SHARD_MANIFEST_FILE);
    let tmp_path = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    let _ = fs::remove_file(&tmp_path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn checked_shape_elements(shape: &[usize]) -> TrainingShardResult<u64> {
    if shape.is_empty() {
        return Err(TrainingShardError::InvalidManifest(
            "tensor shape cannot be empty".to_string(),
        ));
    }
    let mut elements = 1u64;
    for &dim in shape {
        if dim == 0 {
            return Err(TrainingShardError::InvalidManifest(
                "tensor shape dimensions must be positive".to_string(),
            ));
        }
        elements = elements.checked_mul(dim as u64).ok_or_else(|| {
            TrainingShardError::InvalidManifest("tensor element count overflows".to_string())
        })?;
    }
    Ok(elements)
}

fn validate_relative_file_path(value: &str, label: &str) -> TrainingShardResult<()> {
    let path = Path::new(value);
    if value.trim().is_empty() || path.is_absolute() {
        return Err(TrainingShardError::InvalidManifest(format!(
            "{label} must be a non-empty relative file path"
        )));
    }
    let mut components = path.components();
    let Some(Component::Normal(_)) = components.next() else {
        return Err(TrainingShardError::InvalidManifest(format!(
            "{label} must be a relative file path"
        )));
    };
    if components.next().is_some() {
        return Err(TrainingShardError::InvalidManifest(format!(
            "{label} must stay in the shard root"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn temp_shard_dir(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rustwx_training_shard_{name}_{}_{}",
            std::process::id(),
            id
        ))
    }

    fn synthetic_manifest() -> TrainingShardManifest {
        TrainingShardManifest::new(
            "unit-shard-000",
            vec![
                TrainingShardTensorSpec::f32_raw("hrrr_fields", "hrrr", vec![1, 2, 2]).unwrap(),
                TrainingShardTensorSpec::f32_raw("mrms_fields", "mrms", vec![1, 2, 2]).unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn training_shard_writer_reader_round_trips_raw_f32_offsets() {
        let root = temp_shard_dir("round_trip");
        let manifest = synthetic_manifest();
        let mut writer = TrainingShardWriter::create(&root, manifest).unwrap();

        let first = writer
            .append_sample(
                "sample-000",
                &[
                    TrainingShardSampleTensor {
                        name: "hrrr_fields",
                        values: &[1.0, 2.0, 3.0, 4.0],
                    },
                    TrainingShardSampleTensor {
                        name: "mrms_fields",
                        values: &[10.0, 11.0, 12.0, 13.0],
                    },
                ],
            )
            .unwrap();
        let second = writer
            .append_sample(
                "sample-001",
                &[
                    TrainingShardSampleTensor {
                        name: "hrrr_fields",
                        values: &[5.0, 6.0, 7.0, 8.0],
                    },
                    TrainingShardSampleTensor {
                        name: "mrms_fields",
                        values: &[14.0, 15.0, 16.0, 17.0],
                    },
                ],
            )
            .unwrap();
        let finished = writer.finish().unwrap();

        assert_eq!(finished.sample_count, 2);
        assert!(finished.completed);
        assert_eq!(first.tensor("hrrr_fields").unwrap().offset_bytes, 0);
        assert_eq!(second.tensor("hrrr_fields").unwrap().offset_bytes, 16);
        assert_eq!(second.tensor("mrms_fields").unwrap().offset_bytes, 16);
        assert_eq!(fs::metadata(root.join("hrrr_f32.bin")).unwrap().len(), 32);
        assert_eq!(fs::metadata(root.join("mrms_f32.bin")).unwrap().len(), 32);

        let reader = TrainingShardReader::open(&root).unwrap();
        assert_eq!(reader.index().len(), 2);
        assert_eq!(reader.manifest().sample_count, 2);
        assert_eq!(
            reader.read_tensor_f32_le(1, "hrrr_fields").unwrap(),
            vec![5.0, 6.0, 7.0, 8.0]
        );
        let bytes = reader.tensor_bytes(1, "mrms_fields").unwrap();
        assert_eq!(bytes.slice.offset_bytes, 16);
        assert_eq!(bytes.bytes.len(), 16);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn training_shard_manifest_validates_dimensions_and_source_groups() {
        let manifest = synthetic_manifest();
        manifest.validate().unwrap();
        assert_eq!(manifest.source_groups.len(), 2);
        assert_eq!(
            manifest.source_group("hrrr").unwrap().blob_path,
            "hrrr_f32.bin"
        );
        let hrrr = manifest.tensor("hrrr_fields").unwrap();
        assert_eq!(hrrr.shape, vec![1, 2, 2]);
        assert_eq!(hrrr.per_sample_elements, 4);
        assert_eq!(hrrr.per_sample_bytes, 16);
    }

    #[test]
    fn training_shard_hot_path_does_not_emit_compressed_outputs() {
        let root = temp_shard_dir("uncompressed");
        let manifest = synthetic_manifest();
        let mut writer = TrainingShardWriter::create(&root, manifest).unwrap();
        writer
            .append_sample(
                "sample-000",
                &[
                    TrainingShardSampleTensor {
                        name: "hrrr_fields",
                        values: &[1.0, 2.0, 3.0, 4.0],
                    },
                    TrainingShardSampleTensor {
                        name: "mrms_fields",
                        values: &[10.0, 11.0, 12.0, 13.0],
                    },
                ],
            )
            .unwrap();
        writer.finish().unwrap();

        let files = fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<BTreeSet<_>>();
        assert!(files.contains(TRAINING_SHARD_MANIFEST_FILE));
        assert!(files.contains(TRAINING_SHARD_INDEX_FILE));
        assert!(files.contains("hrrr_f32.bin"));
        assert!(files.contains("mrms_f32.bin"));
        assert!(files.iter().all(|name| {
            !name.ends_with(".gz")
                && !name.ends_with(".npz")
                && !name.ends_with(".zip")
                && !name.ends_with(".zst")
        }));

        let _ = fs::remove_dir_all(root);
    }
}
