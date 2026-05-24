use super::{VolumeResult, VolumeStoreError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

pub const FORECAST_BUNDLE_FORMAT: &str = "rustwx-forecast-bundle-v0";
pub const FORECAST_GROUP_FORMAT: &str = "rustwx-forecast-group-v0";
pub const FORECAST_BUNDLE_FILE: &str = "bundle.json";
pub const FORECAST_GROUP_MANIFEST_FILE: &str = "group.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForecastBundle {
    pub format: String,
    pub model: String,
    pub domain: String,
    pub cycle: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forecast_hours: Vec<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_registry_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<ForecastGroupRef>,
}

impl ForecastBundle {
    pub fn new(
        model: impl Into<String>,
        domain: impl Into<String>,
        cycle: impl Into<String>,
        groups: Vec<ForecastGroupRef>,
    ) -> Self {
        Self {
            format: FORECAST_BUNDLE_FORMAT.to_string(),
            model: model.into(),
            domain: domain.into(),
            cycle: cycle.into(),
            forecast_hours: Vec::new(),
            grid_id: None,
            product_registry_version: None,
            label: None,
            metadata: BTreeMap::new(),
            groups,
        }
    }

    pub fn validate(&self) -> VolumeResult<()> {
        if self.format != FORECAST_BUNDLE_FORMAT {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "unsupported forecast bundle format '{}'",
                self.format
            )));
        }
        require_nonblank(&self.model, "bundle model")?;
        require_nonblank(&self.domain, "bundle domain")?;
        require_nonblank(&self.cycle, "bundle cycle")?;
        if self.groups.is_empty() {
            return Err(VolumeStoreError::InvalidManifest(
                "at least one group is required".to_string(),
            ));
        }

        let mut group_ids = HashSet::new();
        for group in &self.groups {
            group.validate()?;
            if !group_ids.insert(group.id.as_str()) {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "duplicate group '{}'",
                    group.id
                )));
            }
        }
        Ok(())
    }

    pub fn group_ref(&self, id: &str) -> VolumeResult<&ForecastGroupRef> {
        self.groups
            .iter()
            .find(|group| group.id == id)
            .ok_or_else(|| VolumeStoreError::InvalidManifest(format!("missing group '{id}'")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastGroupRef {
    pub id: String,
    pub kind: ForecastGroupKind,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_for: Vec<String>,
    #[serde(default)]
    pub lazy: bool,
    #[serde(default)]
    pub static_group: bool,
}

impl ForecastGroupRef {
    pub fn new(id: impl Into<String>, kind: ForecastGroupKind, path: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind,
            path: path.into(),
            manifest_path: None,
            label: None,
            required_for: Vec::new(),
            lazy: false,
            static_group: false,
        }
    }

    pub fn with_manifest_path(mut self, manifest_path: impl Into<String>) -> Self {
        self.manifest_path = Some(manifest_path.into());
        self
    }

    fn validate(&self) -> VolumeResult<()> {
        require_nonblank(&self.id, "group id")?;
        validate_relative_path(&self.path, "group path")?;
        if let Some(manifest_path) = &self.manifest_path {
            validate_relative_path(manifest_path, "group manifest path")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ForecastGroupKind {
    #[serde(rename = "pressure_3d")]
    Pressure3d,
    #[serde(rename = "planar_2d")]
    Planar2d,
    #[serde(rename = "hybrid_3d")]
    Hybrid3d,
    #[serde(rename = "static_terrain")]
    StaticTerrain,
    #[serde(rename = "point_timeseries")]
    PointTimeseries,
}

impl ForecastGroupKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pressure3d => "pressure_3d",
            Self::Planar2d => "planar_2d",
            Self::Hybrid3d => "hybrid_3d",
            Self::StaticTerrain => "static_terrain",
            Self::PointTimeseries => "point_timeseries",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForecastGroupManifest {
    pub format: String,
    pub id: String,
    pub kind: ForecastGroupKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub axes: Vec<ForecastAxis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variables: Vec<ForecastVariable>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<ForecastBlob>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<ForecastIndex>,
}

impl ForecastGroupManifest {
    pub fn new(
        id: impl Into<String>,
        kind: ForecastGroupKind,
        axes: Vec<ForecastAxis>,
        variables: Vec<ForecastVariable>,
        blobs: Vec<ForecastBlob>,
    ) -> Self {
        Self {
            format: FORECAST_GROUP_FORMAT.to_string(),
            id: id.into(),
            kind,
            grid_id: None,
            label: None,
            axes,
            variables,
            blobs,
            index: None,
        }
    }

    pub fn validate(&self) -> VolumeResult<()> {
        if self.format != FORECAST_GROUP_FORMAT {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "unsupported forecast group format '{}'",
                self.format
            )));
        }
        require_nonblank(&self.id, "group id")?;

        let mut axis_names = HashSet::new();
        for axis in &self.axes {
            axis.validate()?;
            if !axis_names.insert(axis.name.as_str()) {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "duplicate axis '{}'",
                    axis.name
                )));
            }
        }

        let mut blob_ids = HashSet::new();
        for blob in &self.blobs {
            blob.validate()?;
            if !blob_ids.insert(blob.id.as_str()) {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "duplicate blob '{}'",
                    blob.id
                )));
            }
        }
        if let Some(index) = &self.index {
            index.validate()?;
        }

        let mut variable_names = HashSet::new();
        for variable in &self.variables {
            variable.validate()?;
            if !variable_names.insert(variable.name.as_str()) {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "duplicate variable '{}'",
                    variable.name
                )));
            }
            for axis in &variable.axes {
                if !axis_names.contains(axis.as_str()) {
                    return Err(VolumeStoreError::InvalidManifest(format!(
                        "variable '{}' references missing axis '{}'",
                        variable.name, axis
                    )));
                }
            }
            if let Some(blob) = &variable.blob {
                if !blob_ids.contains(blob.as_str()) {
                    return Err(VolumeStoreError::InvalidManifest(format!(
                        "variable '{}' references missing blob '{}'",
                        variable.name, blob
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastIndex {
    pub path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contains: Vec<String>,
}

impl ForecastIndex {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            key: Vec::new(),
            contains: Vec::new(),
        }
    }

    fn validate(&self) -> VolumeResult<()> {
        validate_relative_path(&self.path, "index path")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForecastAxis {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ForecastAxisKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub units: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<ForecastAxisValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

impl ForecastAxis {
    pub fn new(name: impl Into<String>, kind: ForecastAxisKind) -> Self {
        Self {
            name: name.into(),
            kind: Some(kind),
            label: None,
            units: None,
            values: Vec::new(),
            blob: None,
        }
    }

    fn validate(&self) -> VolumeResult<()> {
        require_nonblank(&self.name, "axis name")?;
        if let Some(blob) = &self.blob {
            require_nonblank(blob, "axis blob")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastAxisKind {
    Time,
    ForecastHour,
    Pressure,
    HybridLevel,
    Height,
    Latitude,
    Longitude,
    X,
    Y,
    Station,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ForecastAxisValue {
    Integer(i64),
    Number(f64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastVariable {
    pub name: String,
    pub label: String,
    pub units: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standard_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub axes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

impl ForecastVariable {
    pub fn new(
        name: impl Into<String>,
        label: impl Into<String>,
        units: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            units: units.into(),
            standard_name: None,
            axes: Vec::new(),
            blob: None,
        }
    }

    pub fn with_axes(mut self, axes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.axes = axes.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_blob(mut self, blob: impl Into<String>) -> Self {
        self.blob = Some(blob.into());
        self
    }

    fn validate(&self) -> VolumeResult<()> {
        require_nonblank(&self.name, "variable name")?;
        require_nonblank(&self.label, "variable label")?;
        require_nonblank(&self.units, "variable units")?;
        for axis in &self.axes {
            require_nonblank(axis, "variable axis")?;
        }
        if let Some(blob) = &self.blob {
            require_nonblank(blob, "variable blob")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastBlob {
    pub id: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_len: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

impl ForecastBlob {
    pub fn new(id: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            path: path.into(),
            media_type: None,
            codec: None,
            byte_len: None,
            sha256: None,
        }
    }

    fn validate(&self) -> VolumeResult<()> {
        require_nonblank(&self.id, "blob id")?;
        validate_relative_path(&self.path, "blob path")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ForecastBundleReader {
    root: PathBuf,
    bundle: ForecastBundle,
}

impl ForecastBundleReader {
    pub fn open(root: impl AsRef<Path>) -> VolumeResult<Self> {
        let root = root.as_ref().to_path_buf();
        let bundle_bytes = fs::read(root.join(FORECAST_BUNDLE_FILE))?;
        let bundle: ForecastBundle = serde_json::from_slice(&bundle_bytes)?;
        bundle.validate()?;
        Ok(Self { root, bundle })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn bundle(&self) -> &ForecastBundle {
        &self.bundle
    }

    pub fn group_ref(&self, id: &str) -> VolumeResult<&ForecastGroupRef> {
        self.bundle.group_ref(id)
    }

    pub fn group_path(&self, id: &str) -> VolumeResult<PathBuf> {
        self.resolve_group_path(self.group_ref(id)?)
    }

    pub fn resolve_group_path(&self, group: &ForecastGroupRef) -> VolumeResult<PathBuf> {
        resolve_relative_path(&self.root, &group.path, "group path")
    }

    pub fn group_manifest_path(&self, id: &str) -> VolumeResult<PathBuf> {
        self.resolve_group_manifest_path(self.group_ref(id)?)
    }

    pub fn resolve_group_manifest_path(&self, group: &ForecastGroupRef) -> VolumeResult<PathBuf> {
        let group_root = self.resolve_group_path(group)?;
        let manifest_path = group
            .manifest_path
            .as_deref()
            .unwrap_or(FORECAST_GROUP_MANIFEST_FILE);
        resolve_relative_path(&group_root, manifest_path, "group manifest path")
    }

    pub fn open_group_manifest(&self, id: &str) -> VolumeResult<ForecastGroupManifest> {
        let manifest_path = self.group_manifest_path(id)?;
        let manifest_bytes = fs::read(manifest_path)?;
        let manifest: ForecastGroupManifest = serde_json::from_slice(&manifest_bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn resolve_blob_path(
        &self,
        group: &ForecastGroupRef,
        blob: &ForecastBlob,
    ) -> VolumeResult<PathBuf> {
        let group_root = self.resolve_group_path(group)?;
        resolve_relative_path(&group_root, &blob.path, "blob path")
    }
}

fn require_nonblank(value: &str, label: &str) -> VolumeResult<()> {
    if value.trim().is_empty() {
        return Err(VolumeStoreError::InvalidManifest(format!(
            "{label} cannot be blank"
        )));
    }
    Ok(())
}

fn validate_relative_path(raw_path: &str, label: &str) -> VolumeResult<()> {
    relative_path_components(raw_path, label).map(drop)
}

fn resolve_relative_path(root: &Path, raw_path: &str, label: &str) -> VolumeResult<PathBuf> {
    relative_path_components(raw_path, label)?;
    Ok(root.join(raw_path))
}

fn relative_path_components(raw_path: &str, label: &str) -> VolumeResult<()> {
    if raw_path.trim().is_empty() {
        return Err(VolumeStoreError::InvalidManifest(format!(
            "{label} cannot be blank"
        )));
    }
    let path = Path::new(raw_path);
    if path.is_absolute() {
        return Err(VolumeStoreError::InvalidManifest(format!(
            "{label} must be relative"
        )));
    }
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "{label} cannot escape the bundle root"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn temp_bundle_dir(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rustwx_forecast_bundle_{name}_{}_{}",
            std::process::id(),
            id
        ))
    }

    #[test]
    fn bundle_reader_opens_bundle_and_group_manifest() {
        let root = temp_bundle_dir("open");
        let pressure_root = root.join("pressure");
        fs::create_dir_all(&pressure_root).expect("create pressure group");

        let mut hour_axis = ForecastAxis::new("forecast_hour", ForecastAxisKind::ForecastHour);
        hour_axis.units = Some("h".to_string());
        hour_axis.values = vec![ForecastAxisValue::Integer(0), ForecastAxisValue::Integer(1)];
        let mut pressure_axis = ForecastAxis::new("pressure", ForecastAxisKind::Pressure);
        pressure_axis.units = Some("hPa".to_string());
        pressure_axis.values = vec![
            ForecastAxisValue::Integer(1000),
            ForecastAxisValue::Integer(850),
        ];

        let manifest = ForecastGroupManifest::new(
            "pressure",
            ForecastGroupKind::Pressure3d,
            vec![hour_axis, pressure_axis],
            vec![ForecastVariable::new("TMP", "Temperature", "K")
                .with_axes(["forecast_hour", "pressure"])
                .with_blob("chunks")],
            vec![ForecastBlob::new("chunks", "chunks.bin")],
        );
        manifest.validate().expect("valid group manifest");
        fs::write(
            pressure_root.join(FORECAST_GROUP_MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).expect("serialize group manifest"),
        )
        .expect("write group manifest");

        let bundle = ForecastBundle::new(
            "hrrr",
            "conus",
            "2026-04-28T00:00:00Z",
            vec![ForecastGroupRef::new(
                "pressure",
                ForecastGroupKind::Pressure3d,
                "pressure",
            )],
        );
        fs::write(
            root.join(FORECAST_BUNDLE_FILE),
            serde_json::to_vec_pretty(&bundle).expect("serialize bundle"),
        )
        .expect("write bundle");

        let reader = ForecastBundleReader::open(&root).expect("open bundle");
        assert_eq!(reader.group_path("pressure").unwrap(), pressure_root);
        assert_eq!(
            reader.group_manifest_path("pressure").unwrap(),
            pressure_root.join(FORECAST_GROUP_MANIFEST_FILE)
        );
        let loaded_manifest = reader
            .open_group_manifest("pressure")
            .expect("open group manifest");
        assert_eq!(loaded_manifest.kind, ForecastGroupKind::Pressure3d);
        assert_eq!(ForecastGroupKind::Hybrid3d.as_str(), "hybrid_3d");

        let encoded_kind =
            serde_json::to_string(&ForecastGroupKind::PointTimeseries).expect("serialize kind");
        assert_eq!(encoded_kind, "\"point_timeseries\"");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bundle_rejects_escaping_group_paths() {
        let bundle = ForecastBundle::new(
            "hrrr",
            "conus",
            "2026-04-28T00:00:00Z",
            vec![ForecastGroupRef::new(
                "pressure",
                ForecastGroupKind::Pressure3d,
                "../pressure",
            )],
        );

        assert!(bundle.validate().is_err());
    }
}
