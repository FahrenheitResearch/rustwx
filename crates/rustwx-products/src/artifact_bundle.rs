use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::publication::{ArtifactContentIdentity, artifact_identity_from_path, atomic_write_json};
use crate::publication_provenance::{BuildProvenance, new_attempt_id};

pub const ARTIFACT_BUNDLE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactBundleManifest {
    pub schema_version: u32,
    pub bundle_kind: String,
    pub bundle_label: String,
    pub output_root: PathBuf,
    #[serde(default = "default_bundle_id")]
    pub bundle_id: String,
    #[serde(default)]
    pub created_unix_ms: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_provenance: Option<BuildProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_context: Option<ArtifactBundleRunContext>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactBundleArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactBundleRunContext {
    pub runner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date_yyyymmdd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle_utc: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forecast_hour: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_slug: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactBundleRole {
    PrimaryImage,
    Metadata,
    Stats,
    Auxiliary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactBundleArtifact {
    pub artifact_key: String,
    pub role: ArtifactBundleRole,
    pub media_type: String,
    pub relative_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_identity: Option<ArtifactContentIdentity>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stats: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auxiliary_outputs: Vec<ArtifactBundleAuxiliaryOutput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactBundleAuxiliaryOutput {
    pub output_key: String,
    pub relative_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_identity: Option<ArtifactContentIdentity>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

fn default_bundle_id() -> String {
    new_attempt_id()
}

impl ArtifactBundleManifest {
    pub fn new(
        bundle_kind: impl Into<String>,
        bundle_label: impl Into<String>,
        output_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            schema_version: ARTIFACT_BUNDLE_SCHEMA_VERSION,
            bundle_kind: bundle_kind.into(),
            bundle_label: bundle_label.into(),
            output_root: output_root.into(),
            bundle_id: new_attempt_id(),
            created_unix_ms: unix_time_ms(),
            build_provenance: None,
            run_context: None,
            metadata: BTreeMap::new(),
            artifacts: Vec::new(),
        }
    }

    pub fn with_build_provenance(mut self, provenance: BuildProvenance) -> Self {
        self.build_provenance = Some(provenance);
        self
    }

    pub fn with_captured_build_provenance(self, workspace_root: &Path) -> Self {
        self.with_build_provenance(crate::publication_provenance::capture_build_provenance(
            workspace_root,
        ))
    }

    pub fn with_run_context(mut self, run_context: ArtifactBundleRunContext) -> Self {
        self.run_context = Some(run_context);
        self
    }

    pub fn insert_metadata_value(&mut self, key: impl Into<String>, value: Value) {
        self.metadata.insert(key.into(), value);
    }

    pub fn push_artifact(&mut self, artifact: ArtifactBundleArtifact) {
        self.artifacts.push(artifact);
    }
}

impl ArtifactBundleRunContext {
    pub fn new(runner: impl Into<String>) -> Self {
        Self {
            runner: runner.into(),
            model: None,
            date_yyyymmdd: None,
            cycle_utc: None,
            forecast_hour: None,
            source: None,
            domain_slug: None,
        }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_cycle_metadata(
        mut self,
        date_yyyymmdd: impl Into<String>,
        cycle_utc: u8,
        forecast_hour: u16,
    ) -> Self {
        self.date_yyyymmdd = Some(date_yyyymmdd.into());
        self.cycle_utc = Some(cycle_utc);
        self.forecast_hour = Some(forecast_hour);
        self
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_domain_slug(mut self, domain_slug: impl Into<String>) -> Self {
        self.domain_slug = Some(domain_slug.into());
        self
    }
}

impl ArtifactBundleArtifact {
    pub fn new(
        artifact_key: impl Into<String>,
        role: ArtifactBundleRole,
        media_type: impl Into<String>,
        relative_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            artifact_key: artifact_key.into(),
            role,
            media_type: media_type.into(),
            relative_path: relative_path.into(),
            content_identity: None,
            metadata: BTreeMap::new(),
            stats: BTreeMap::new(),
            auxiliary_outputs: Vec::new(),
        }
    }

    pub fn from_existing_path(
        artifact_key: impl Into<String>,
        role: ArtifactBundleRole,
        media_type: impl Into<String>,
        output_root: &Path,
        path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::new(
            artifact_key,
            role,
            media_type,
            relative_bundle_path(output_root, path),
        )
        .with_content_identity(artifact_identity_from_path(path)?))
    }

    pub fn with_content_identity(mut self, identity: ArtifactContentIdentity) -> Self {
        self.content_identity = Some(identity);
        self
    }

    pub fn insert_metadata_value(&mut self, key: impl Into<String>, value: Value) {
        self.metadata.insert(key.into(), value);
    }

    pub fn insert_stat_value(&mut self, key: impl Into<String>, value: Value) {
        self.stats.insert(key.into(), value);
    }

    pub fn push_auxiliary_output(&mut self, output: ArtifactBundleAuxiliaryOutput) {
        self.auxiliary_outputs.push(output);
    }
}

impl ArtifactBundleAuxiliaryOutput {
    pub fn new(output_key: impl Into<String>, relative_path: impl Into<PathBuf>) -> Self {
        Self {
            output_key: output_key.into(),
            relative_path: relative_path.into(),
            media_type: None,
            content_identity: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn from_existing_path(
        output_key: impl Into<String>,
        media_type: impl Into<String>,
        output_root: &Path,
        path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(
            Self::new(output_key, relative_bundle_path(output_root, path))
                .with_media_type(media_type)
                .with_content_identity(artifact_identity_from_path(path)?),
        )
    }

    pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }

    pub fn with_content_identity(mut self, identity: ArtifactContentIdentity) -> Self {
        self.content_identity = Some(identity);
        self
    }

    pub fn insert_metadata_value(&mut self, key: impl Into<String>, value: Value) {
        self.metadata.insert(key.into(), value);
    }
}

pub fn relative_bundle_path(output_root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(output_root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| path.to_path_buf())
}

pub fn default_artifact_bundle_manifest_path(output_root: &Path, bundle_slug: &str) -> PathBuf {
    output_root.join(format!("{bundle_slug}_artifact_bundle_manifest.json"))
}

pub fn publish_artifact_bundle_manifest(
    path: &Path,
    manifest: &ArtifactBundleManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    atomic_write_json(path, manifest)
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::process;

    #[test]
    fn existing_path_artifact_keeps_relative_path_and_hash() {
        let root = std::env::temp_dir().join(format!("rustwx_bundle_artifact_{}", process::id()));
        let artifact_path = root.join("profile/demo.png");
        fs::create_dir_all(artifact_path.parent().unwrap()).unwrap();
        fs::write(&artifact_path, b"png-bytes").unwrap();

        let artifact = ArtifactBundleArtifact::from_existing_path(
            "map:demo:native",
            ArtifactBundleRole::PrimaryImage,
            "image/png",
            &root,
            &artifact_path,
        )
        .unwrap();

        assert_eq!(artifact.relative_path, PathBuf::from("profile/demo.png"));
        assert_eq!(
            artifact.content_identity.as_ref().unwrap().sha256,
            crate::publication::sha256_hex(b"png-bytes")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn publish_bundle_manifest_writes_json() {
        let root = std::env::temp_dir().join(format!("rustwx_bundle_manifest_{}", process::id()));
        let path = default_artifact_bundle_manifest_path(&root, "demo");
        let manifest = ArtifactBundleManifest::new("weather_native_profile", "demo", &root)
            .with_run_context(
                ArtifactBundleRunContext::new("weather_native_profile")
                    .with_model("hrrr")
                    .with_cycle_metadata("20260422", 18, 6)
                    .with_source("nomads")
                    .with_domain_slug("southern_plains"),
            );

        publish_artifact_bundle_manifest(&path, &manifest).unwrap();

        let loaded: ArtifactBundleManifest =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(loaded.bundle_kind, "weather_native_profile");
        assert_eq!(
            loaded.run_context.as_ref().unwrap().domain_slug.as_deref(),
            Some("southern_plains")
        );

        let _ = fs::remove_dir_all(root);
    }
}
