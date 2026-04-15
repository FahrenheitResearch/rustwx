use serde::Serialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

pub const RUN_PUBLICATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPublicationState {
    Planned,
    Running,
    Complete,
    Partial,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPublicationState {
    Planned,
    Running,
    Complete,
    Failed,
    Blocked,
    CacheHit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct PublishedArtifactRecord {
    pub artifact_key: String,
    pub relative_path: PathBuf,
    pub state: ArtifactPublicationState,
    pub detail: Option<String>,
}

impl PublishedArtifactRecord {
    pub fn planned<K: Into<String>, P: Into<PathBuf>>(artifact_key: K, relative_path: P) -> Self {
        Self {
            artifact_key: artifact_key.into(),
            relative_path: relative_path.into(),
            state: ArtifactPublicationState::Planned,
            detail: None,
        }
    }

    pub fn with_state(mut self, state: ArtifactPublicationState) -> Self {
        self.state = state;
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct RunPublicationManifest {
    pub schema_version: u32,
    pub run_kind: String,
    pub run_label: String,
    pub output_root: PathBuf,
    pub state: RunPublicationState,
    pub started_unix_ms: u128,
    pub finished_unix_ms: Option<u128>,
    pub detail: Option<String>,
    pub artifacts: Vec<PublishedArtifactRecord>,
}

impl RunPublicationManifest {
    pub fn new(
        run_kind: impl Into<String>,
        run_label: impl Into<String>,
        output_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            schema_version: RUN_PUBLICATION_SCHEMA_VERSION,
            run_kind: run_kind.into(),
            run_label: run_label.into(),
            output_root: output_root.into(),
            state: RunPublicationState::Planned,
            started_unix_ms: unix_time_ms(),
            finished_unix_ms: None,
            detail: None,
            artifacts: Vec::new(),
        }
    }

    pub fn with_artifacts(mut self, artifacts: Vec<PublishedArtifactRecord>) -> Self {
        self.artifacts = artifacts;
        self
    }

    pub fn push_artifact(&mut self, artifact: PublishedArtifactRecord) {
        self.artifacts.push(artifact);
    }

    pub fn mark_running(&mut self) {
        self.state = RunPublicationState::Running;
        self.finished_unix_ms = None;
        self.detail = None;
    }

    pub fn mark_complete(&mut self) {
        self.state = RunPublicationState::Complete;
        self.finished_unix_ms = Some(unix_time_ms());
        self.detail = None;
    }

    pub fn mark_partial(&mut self, detail: impl Into<String>) {
        self.state = RunPublicationState::Partial;
        self.finished_unix_ms = Some(unix_time_ms());
        self.detail = Some(detail.into());
    }

    pub fn mark_failed(&mut self, detail: impl Into<String>) {
        self.state = RunPublicationState::Failed;
        self.finished_unix_ms = Some(unix_time_ms());
        self.detail = Some(detail.into());
    }

    pub fn update_artifact_state(
        &mut self,
        artifact_key: &str,
        state: ArtifactPublicationState,
        detail: Option<String>,
    ) -> bool {
        if let Some(artifact) = self
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.artifact_key == artifact_key)
        {
            artifact.state = state;
            artifact.detail = detail;
            return true;
        }
        false
    }
}

pub fn default_run_manifest_path(output_root: &Path, run_slug: &str) -> PathBuf {
    output_root.join(format!("{run_slug}_run_manifest.json"))
}

pub fn publish_run_manifest(
    path: &Path,
    manifest: &RunPublicationManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    atomic_write_json(path, manifest)
}

pub fn atomic_write_json<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec_pretty(value)?;
    atomic_write_bytes(path, &bytes)
}

pub fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = temp_path_for(path);
    let write_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    if let Err(err) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(Box::new(err));
    }
    Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    path.with_file_name(format!(
        ".{file_name}.tmp-{}-{}",
        process::id(),
        unix_time_ms()
    ))
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

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
    struct JsonFixture {
        name: String,
        value: u16,
    }

    #[test]
    fn atomic_write_json_publishes_readable_file() {
        let root =
            std::env::temp_dir().join(format!("rustwx_products_publication_{}", process::id()));
        let path = root.join("fixture.json");
        let fixture = JsonFixture {
            name: "demo".into(),
            value: 7,
        };

        atomic_write_json(&path, &fixture).unwrap();
        let loaded: JsonFixture = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(loaded, fixture);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manifest_tracks_artifact_and_run_states() {
        let mut manifest = RunPublicationManifest::new(
            "hrrr_direct_batch",
            "hrrr_20260414_23z_f006_conus",
            PathBuf::from("proof/demo"),
        )
        .with_artifacts(vec![
            PublishedArtifactRecord::planned("sbcape", "sbcape.png"),
            PublishedArtifactRecord::planned("mlcape", "mlcape.png"),
        ]);

        manifest.mark_running();
        assert_eq!(manifest.state, RunPublicationState::Running);
        assert!(manifest.update_artifact_state("sbcape", ArtifactPublicationState::Complete, None));
        assert!(manifest.update_artifact_state(
            "mlcape",
            ArtifactPublicationState::Blocked,
            Some("blocked in test".into())
        ));
        manifest.mark_partial("one artifact blocked");

        assert_eq!(manifest.state, RunPublicationState::Partial);
        assert!(manifest.finished_unix_ms.is_some());
        assert_eq!(
            manifest
                .artifacts
                .iter()
                .find(|artifact| artifact.artifact_key == "sbcape")
                .unwrap()
                .state,
            ArtifactPublicationState::Complete
        );
        assert_eq!(
            manifest
                .artifacts
                .iter()
                .find(|artifact| artifact.artifact_key == "mlcape")
                .unwrap()
                .state,
            ArtifactPublicationState::Blocked
        );
    }
}
