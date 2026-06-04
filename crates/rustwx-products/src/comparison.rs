use crate::derived::{DerivedBatchReport, HrrrDerivedBatchReport};
use crate::direct::DirectBatchReport;
use crate::ecape::EcapeBatchReport;
use crate::heavy::{HeavyPanelHourReport, HeavyRenderedArtifactGroup};
use crate::hrrr::HrrrBatchReport;
use crate::non_ecape::{
    HrrrNonEcapeHourReport, HrrrNonEcapeMultiDomainReport, NonEcapeHourReport,
    NonEcapeMultiDomainReport,
};
use crate::publication::{
    ArtifactContentIdentity, ArtifactPublicationState, PublishedFetchIdentity,
    RunPublicationManifest, artifact_identity_from_path,
};
use crate::severe::SevereBatchReport;
use crate::windowed::{
    HrrrWindowedBatchReport, collect_windowed_input_fetches, windowed_product_input_fetch_keys,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

pub const PRODUCT_COMPARISON_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonInputKind {
    RunManifest,
    DirectBatchReport,
    DerivedBatchReport,
    HrrrBatchReport,
    HeavyPanelHourReport,
    WindowedBatchReport,
    SevereBatchReport,
    EcapeBatchReport,
    HrrrNonEcapeHourReport,
    NonEcapeHourReport,
    HrrrNonEcapeMultiDomainReport,
    NonEcapeMultiDomainReport,
}

#[derive(Debug)]
pub enum ComparisonError {
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedInput(PathBuf),
}

impl fmt::Display for ComparisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Json(err) => write!(f, "{err}"),
            Self::UnsupportedInput(path) => {
                write!(
                    f,
                    "unsupported comparison input; expected a rustwx manifest/report: {}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ComparisonError {}

impl From<std::io::Error> for ComparisonError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ComparisonError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparableArtifactState {
    Planned,
    Running,
    Complete,
    Failed,
    Blocked,
    CacheHit,
}

impl From<ArtifactPublicationState> for ComparableArtifactState {
    fn from(value: ArtifactPublicationState) -> Self {
        match value {
            ArtifactPublicationState::Planned => Self::Planned,
            ArtifactPublicationState::Running => Self::Running,
            ArtifactPublicationState::Complete => Self::Complete,
            ArtifactPublicationState::Failed => Self::Failed,
            ArtifactPublicationState::Blocked => Self::Blocked,
            ArtifactPublicationState::CacheHit => Self::CacheHit,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparableArtifactRecord {
    pub artifact_key: String,
    pub lane: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    pub state: ComparableArtifactState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_identity: Option<ArtifactContentIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_fetch_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparableInputFetchRecord {
    pub logical_key: String,
    pub fetch_key: String,
    pub planned_family: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub planned_family_aliases: Vec<String>,
    pub request: rustwx_core::ModelRunRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_override: Option<rustwx_core::SourceId>,
    pub resolved_source: rustwx_core::SourceId,
    pub resolved_url: String,
    pub resolved_family: String,
    pub bytes_len: usize,
    pub bytes_sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProductRunSnapshotSummary {
    pub artifact_count: usize,
    pub blocker_count: usize,
    pub complete_artifact_count: usize,
    pub input_fetch_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductRunSnapshot {
    pub schema_version: u32,
    pub source_path: PathBuf,
    pub source_kind: ComparisonInputKind,
    pub run_kind: String,
    pub run_label: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_ms: Option<u128>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub linked_manifest_paths: Vec<PathBuf>,
    pub summary: ProductRunSnapshotSummary,
    pub artifacts: Vec<ComparableArtifactRecord>,
    pub input_fetches: Vec<ComparableInputFetchRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonChangeKind {
    Added,
    Removed,
    Changed,
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductRunRelationKind {
    SameRun,
    HourToHour,
    RunToRun,
    MixedTemporal,
    Arbitrary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductRunRelation {
    pub kind: ProductRunRelationKind,
    pub same_run_kind: bool,
    pub same_model: bool,
    pub same_source: bool,
    pub same_domain: bool,
    pub same_date: bool,
    pub same_cycle_utc: bool,
    pub same_forecast_hour: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle_delta_hours: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forecast_hour_delta: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactComparisonRecord {
    pub artifact_key: String,
    pub change: ComparisonChangeKind,
    pub content_changed: bool,
    pub state_changed: bool,
    pub detail_changed: bool,
    pub lane_changed: bool,
    pub title_changed: bool,
    pub domain_changed: bool,
    pub input_fetch_keys_changed: bool,
    pub timing_changed: bool,
    pub path_changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_delta_ms: Option<i128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<ComparableArtifactRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right: Option<ComparableArtifactRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputFetchComparisonRecord {
    pub logical_key: String,
    pub change: ComparisonChangeKind,
    pub fetch_key_changed: bool,
    pub bytes_changed: bool,
    pub route_changed: bool,
    pub source_changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<ComparableInputFetchRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right: Option<ComparableInputFetchRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProductComparisonSummary {
    pub left_artifact_count: usize,
    pub right_artifact_count: usize,
    pub left_blocker_count: usize,
    pub right_blocker_count: usize,
    pub artifact_added_count: usize,
    pub artifact_removed_count: usize,
    pub artifact_changed_count: usize,
    pub artifact_unchanged_count: usize,
    pub artifact_content_changed_count: usize,
    pub artifact_state_changed_count: usize,
    pub artifact_detail_changed_count: usize,
    pub artifact_input_fetch_key_changed_count: usize,
    pub artifact_timing_changed_count: usize,
    pub artifact_path_changed_count: usize,
    pub left_input_fetch_count: usize,
    pub right_input_fetch_count: usize,
    pub input_fetch_added_count: usize,
    pub input_fetch_removed_count: usize,
    pub input_fetch_changed_count: usize,
    pub input_fetch_unchanged_count: usize,
    pub input_fetch_bytes_changed_count: usize,
    pub input_fetch_route_changed_count: usize,
    pub run_state_changed: bool,
    pub run_detail_changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_ms_delta: Option<i128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductRunComparison {
    pub schema_version: u32,
    pub relation: ProductRunRelation,
    pub summary: ProductComparisonSummary,
    pub left: ProductRunSnapshot,
    pub right: ProductRunSnapshot,
    pub artifact_changes: Vec<ArtifactComparisonRecord>,
    pub input_fetch_changes: Vec<InputFetchComparisonRecord>,
}

pub fn load_and_compare_product_runs(
    left_path: &Path,
    right_path: &Path,
) -> Result<ProductRunComparison, ComparisonError> {
    let left = load_product_run_snapshot(left_path)?;
    let right = load_product_run_snapshot(right_path)?;
    Ok(compare_product_runs(&left, &right))
}

pub fn load_product_run_snapshot(path: &Path) -> Result<ProductRunSnapshot, ComparisonError> {
    let bytes = fs::read(path)?;
    let value: Value = serde_json::from_slice(&bytes)?;
    let kind = detect_input_kind(&value)
        .ok_or_else(|| ComparisonError::UnsupportedInput(path.to_path_buf()))?;

    let snapshot = match kind {
        ComparisonInputKind::RunManifest => snapshot_from_run_manifest(
            path,
            serde_json::from_slice::<RunPublicationManifest>(&bytes)?,
        ),
        ComparisonInputKind::DirectBatchReport => snapshot_from_direct_batch_report(
            path,
            serde_json::from_slice::<DirectBatchReport>(&bytes)?,
        ),
        ComparisonInputKind::DerivedBatchReport => snapshot_from_derived_batch_report(
            path,
            serde_json::from_slice::<DerivedBatchReport>(&bytes)?,
        ),
        ComparisonInputKind::HrrrBatchReport => snapshot_from_hrrr_batch_report(
            path,
            serde_json::from_slice::<HrrrBatchReport>(&bytes)?,
        ),
        ComparisonInputKind::HeavyPanelHourReport => snapshot_from_heavy_panel_hour_report(
            path,
            serde_json::from_slice::<HeavyPanelHourReport>(&bytes)?,
        ),
        ComparisonInputKind::WindowedBatchReport => snapshot_from_windowed_batch_report(
            path,
            serde_json::from_slice::<HrrrWindowedBatchReport>(&bytes)?,
        ),
        ComparisonInputKind::SevereBatchReport => snapshot_from_severe_batch_report(
            path,
            serde_json::from_slice::<SevereBatchReport>(&bytes)?,
        ),
        ComparisonInputKind::EcapeBatchReport => snapshot_from_ecape_batch_report(
            path,
            serde_json::from_slice::<EcapeBatchReport>(&bytes)?,
        ),
        ComparisonInputKind::HrrrNonEcapeHourReport => snapshot_from_hrrr_non_ecape_hour_report(
            path,
            serde_json::from_slice::<HrrrNonEcapeHourReport>(&bytes)?,
        ),
        ComparisonInputKind::NonEcapeHourReport => snapshot_from_non_ecape_hour_report(
            path,
            serde_json::from_slice::<NonEcapeHourReport>(&bytes)?,
        ),
        ComparisonInputKind::HrrrNonEcapeMultiDomainReport => {
            snapshot_from_hrrr_non_ecape_multi_domain_report(
                path,
                serde_json::from_slice::<HrrrNonEcapeMultiDomainReport>(&bytes)?,
            )
        }
        ComparisonInputKind::NonEcapeMultiDomainReport => {
            snapshot_from_non_ecape_multi_domain_report(
                path,
                serde_json::from_slice::<NonEcapeMultiDomainReport>(&bytes)?,
            )
        }
    };

    Ok(finalize_snapshot(snapshot))
}

pub fn compare_product_runs(
    left: &ProductRunSnapshot,
    right: &ProductRunSnapshot,
) -> ProductRunComparison {
    let relation = build_relation(left, right);
    let mut summary = ProductComparisonSummary {
        left_artifact_count: left.summary.artifact_count,
        right_artifact_count: right.summary.artifact_count,
        left_blocker_count: left.summary.blocker_count,
        right_blocker_count: right.summary.blocker_count,
        left_input_fetch_count: left.summary.input_fetch_count,
        right_input_fetch_count: right.summary.input_fetch_count,
        run_state_changed: left.run_state != right.run_state,
        run_detail_changed: left.run_detail != right.run_detail,
        total_ms_delta: signed_delta_opt(left.total_ms, right.total_ms),
        ..Default::default()
    };

    let left_artifacts = left
        .artifacts
        .iter()
        .map(|artifact| (artifact.artifact_key.clone(), artifact))
        .collect::<BTreeMap<_, _>>();
    let right_artifacts = right
        .artifacts
        .iter()
        .map(|artifact| (artifact.artifact_key.clone(), artifact))
        .collect::<BTreeMap<_, _>>();

    let mut artifact_changes = Vec::new();
    let mut artifact_keys = left_artifacts.keys().cloned().collect::<Vec<_>>();
    for key in right_artifacts.keys() {
        if !left_artifacts.contains_key(key) {
            artifact_keys.push(key.clone());
        }
    }
    artifact_keys.sort();

    for key in artifact_keys {
        let left_record = left_artifacts.get(&key).cloned().cloned();
        let right_record = right_artifacts.get(&key).cloned().cloned();
        let comparison = compare_artifact_records(key, left_record, right_record);
        match comparison.change {
            ComparisonChangeKind::Added => summary.artifact_added_count += 1,
            ComparisonChangeKind::Removed => summary.artifact_removed_count += 1,
            ComparisonChangeKind::Changed => summary.artifact_changed_count += 1,
            ComparisonChangeKind::Unchanged => summary.artifact_unchanged_count += 1,
        }
        if comparison.content_changed {
            summary.artifact_content_changed_count += 1;
        }
        if comparison.state_changed {
            summary.artifact_state_changed_count += 1;
        }
        if comparison.detail_changed {
            summary.artifact_detail_changed_count += 1;
        }
        if comparison.input_fetch_keys_changed {
            summary.artifact_input_fetch_key_changed_count += 1;
        }
        if comparison.timing_changed {
            summary.artifact_timing_changed_count += 1;
        }
        if comparison.path_changed {
            summary.artifact_path_changed_count += 1;
        }
        artifact_changes.push(comparison);
    }

    let left_fetches = left
        .input_fetches
        .iter()
        .map(|fetch| (fetch.logical_key.clone(), fetch))
        .collect::<BTreeMap<_, _>>();
    let right_fetches = right
        .input_fetches
        .iter()
        .map(|fetch| (fetch.logical_key.clone(), fetch))
        .collect::<BTreeMap<_, _>>();

    let mut input_fetch_changes = Vec::new();
    let mut logical_keys = left_fetches.keys().cloned().collect::<Vec<_>>();
    for key in right_fetches.keys() {
        if !left_fetches.contains_key(key) {
            logical_keys.push(key.clone());
        }
    }
    logical_keys.sort();

    for logical_key in logical_keys {
        let left_record = left_fetches.get(&logical_key).cloned().cloned();
        let right_record = right_fetches.get(&logical_key).cloned().cloned();
        let comparison = compare_input_fetch_records(logical_key, left_record, right_record);
        match comparison.change {
            ComparisonChangeKind::Added => summary.input_fetch_added_count += 1,
            ComparisonChangeKind::Removed => summary.input_fetch_removed_count += 1,
            ComparisonChangeKind::Changed => summary.input_fetch_changed_count += 1,
            ComparisonChangeKind::Unchanged => summary.input_fetch_unchanged_count += 1,
        }
        if comparison.bytes_changed {
            summary.input_fetch_bytes_changed_count += 1;
        }
        if comparison.route_changed {
            summary.input_fetch_route_changed_count += 1;
        }
        input_fetch_changes.push(comparison);
    }

    ProductRunComparison {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        relation,
        summary,
        left: left.clone(),
        right: right.clone(),
        artifact_changes,
        input_fetch_changes,
    }
}

fn detect_input_kind(value: &Value) -> Option<ComparisonInputKind> {
    let object = value.as_object()?;
    if object.contains_key("schema_version")
        && object.contains_key("run_kind")
        && object.contains_key("artifacts")
    {
        return Some(ComparisonInputKind::RunManifest);
    }
    if object.contains_key("domains") && object.contains_key("requested") {
        return if object.contains_key("model") {
            Some(ComparisonInputKind::NonEcapeMultiDomainReport)
        } else {
            Some(ComparisonInputKind::HrrrNonEcapeMultiDomainReport)
        };
    }
    if object.contains_key("summary")
        && object.contains_key("requested")
        && object.contains_key("publication_manifest_path")
    {
        return if object.contains_key("model") {
            Some(ComparisonInputKind::NonEcapeHourReport)
        } else {
            Some(ComparisonInputKind::HrrrNonEcapeHourReport)
        };
    }
    if object.contains_key("severe") && object.contains_key("ecape") {
        return Some(ComparisonInputKind::HeavyPanelHourReport);
    }
    if object.contains_key("fetches") && object.contains_key("recipes") {
        return Some(ComparisonInputKind::DirectBatchReport);
    }
    if object.contains_key("recipes")
        && object.contains_key("shared_timing")
        && !object.contains_key("fetches")
    {
        return Some(ComparisonInputKind::DerivedBatchReport);
    }
    if object.contains_key("products")
        && object.contains_key("shared_timing")
        && object.contains_key("blockers")
    {
        return Some(ComparisonInputKind::WindowedBatchReport);
    }
    if object.contains_key("products")
        && object.contains_key("shared_timing")
        && object.contains_key("input_fetches")
    {
        return Some(ComparisonInputKind::HrrrBatchReport);
    }
    if object.contains_key("outputs") && object.contains_key("heavy_timing") {
        return if object.contains_key("failure_count") {
            Some(ComparisonInputKind::EcapeBatchReport)
        } else {
            Some(ComparisonInputKind::SevereBatchReport)
        };
    }
    None
}

fn snapshot_from_run_manifest(path: &Path, manifest: RunPublicationManifest) -> ProductRunSnapshot {
    let mut artifacts = BTreeMap::new();
    for artifact in &manifest.artifacts {
        let normalized = normalize_manifest_artifact(
            &manifest.output_root,
            artifact.artifact_key.clone(),
            artifact,
            manifest.domain_slug.clone(),
            Some(manifest.run_kind.as_str()),
        );
        insert_artifact(&mut artifacts, normalized);
    }

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::RunManifest,
        run_kind: manifest.run_kind.clone(),
        run_label: manifest.run_label.clone(),
        model: manifest.model.clone(),
        date_yyyymmdd: manifest.date_yyyymmdd.clone(),
        cycle_utc: manifest.cycle_utc,
        forecast_hour: manifest.forecast_hour,
        source: manifest.source.clone(),
        domain_slug: manifest.domain_slug.clone(),
        run_state: Some(run_state_str(manifest.state).to_string()),
        run_detail: manifest.detail.clone(),
        total_ms: None,
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts: artifacts.into_values().collect(),
        input_fetches: normalize_input_fetches(manifest.input_fetches),
    }
}

fn snapshot_from_direct_batch_report(path: &Path, report: DirectBatchReport) -> ProductRunSnapshot {
    let mut artifacts = BTreeMap::new();
    for recipe in &report.recipes {
        insert_artifact(
            &mut artifacts,
            ComparableArtifactRecord {
                artifact_key: recipe.recipe_slug.clone(),
                lane: "direct".to_string(),
                domain_slug: Some(report.domain.slug.clone()),
                title: Some(recipe.title.clone()),
                path: Some(recipe.output_path.clone()),
                state: ComparableArtifactState::Complete,
                detail: Some(format!(
                    "source_route={} planned_family={} fetched_family={} resolved_source={} resolved_url={}",
                    recipe.source_route.as_str(),
                    recipe.grib_product,
                    recipe.fetched_grib_product,
                    recipe.resolved_source.as_str(),
                    recipe.resolved_url
                )),
                content_identity: Some(recipe.content_identity.clone()),
                input_fetch_keys: recipe.input_fetch_keys.clone(),
                timing_ms: Some(recipe.timing.total_ms),
            },
        );
    }
    for blocker in &report.blockers {
        insert_artifact(
            &mut artifacts,
            ComparableArtifactRecord {
                artifact_key: blocker.recipe_slug.clone(),
                lane: "direct".to_string(),
                domain_slug: Some(report.domain.slug.clone()),
                title: None,
                path: None,
                state: ComparableArtifactState::Blocked,
                detail: Some(blocker.reason.clone()),
                content_identity: None,
                input_fetch_keys: Vec::new(),
                timing_ms: None,
            },
        );
    }

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::DirectBatchReport,
        run_kind: "direct_batch".to_string(),
        run_label: format_run_label(
            Some(report.model.as_str()),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "direct_batch",
        ),
        model: Some(report.model.as_str().to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts: artifacts.into_values().collect(),
        input_fetches: normalize_input_fetches(
            report
                .fetches
                .into_iter()
                .map(|fetch| fetch.input_fetch)
                .collect(),
        ),
    }
}

fn snapshot_from_derived_batch_report(
    path: &Path,
    report: DerivedBatchReport,
) -> ProductRunSnapshot {
    let mut artifacts = BTreeMap::new();
    for recipe in &report.recipes {
        insert_artifact(
            &mut artifacts,
            ComparableArtifactRecord {
                artifact_key: recipe.recipe_slug.clone(),
                lane: "derived".to_string(),
                domain_slug: Some(report.domain.slug.clone()),
                title: Some(recipe.title.clone()),
                path: Some(recipe.output_path.clone()),
                state: ComparableArtifactState::Complete,
                detail: Some(format!(
                    "source_mode={} source_route={}",
                    report.source_mode.as_str(),
                    recipe.source_route.as_str()
                )),
                content_identity: Some(recipe.content_identity.clone()),
                input_fetch_keys: recipe.input_fetch_keys.clone(),
                timing_ms: Some(recipe.timing.total_ms),
            },
        );
    }
    for blocker in &report.blockers {
        insert_artifact(
            &mut artifacts,
            ComparableArtifactRecord {
                artifact_key: blocker.recipe_slug.clone(),
                lane: "derived".to_string(),
                domain_slug: Some(report.domain.slug.clone()),
                title: None,
                path: None,
                state: ComparableArtifactState::Blocked,
                detail: Some(format!(
                    "source_mode={} source_route={} {}",
                    report.source_mode.as_str(),
                    blocker.source_route.as_str(),
                    blocker.reason
                )),
                content_identity: None,
                input_fetch_keys: Vec::new(),
                timing_ms: None,
            },
        );
    }

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::DerivedBatchReport,
        run_kind: "derived_batch".to_string(),
        run_label: format_run_label(
            Some(report.model.as_str()),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "derived_batch",
        ),
        model: Some(report.model.as_str().to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts: artifacts.into_values().collect(),
        input_fetches: normalize_input_fetches(report.input_fetches),
    }
}

fn snapshot_from_windowed_batch_report(
    path: &Path,
    report: HrrrWindowedBatchReport,
) -> ProductRunSnapshot {
    let mut artifacts = BTreeMap::new();
    for product in &report.products {
        insert_artifact(
            &mut artifacts,
            ComparableArtifactRecord {
                artifact_key: product.product.slug().to_string(),
                lane: "windowed".to_string(),
                domain_slug: Some(report.domain.slug.clone()),
                title: Some(product.product.title().to_string()),
                path: Some(product.output_path.clone()),
                state: ComparableArtifactState::Complete,
                detail: Some(windowed_product_detail(product)),
                content_identity: maybe_artifact_identity(&product.output_path),
                input_fetch_keys: windowed_product_input_fetch_keys(product, &report.shared_timing),
                timing_ms: Some(product.timing.total_ms),
            },
        );
    }
    for blocker in &report.blockers {
        insert_artifact(
            &mut artifacts,
            ComparableArtifactRecord {
                artifact_key: blocker.product.slug().to_string(),
                lane: "windowed".to_string(),
                domain_slug: Some(report.domain.slug.clone()),
                title: Some(blocker.product.title().to_string()),
                path: None,
                state: ComparableArtifactState::Blocked,
                detail: Some(blocker.reason.clone()),
                content_identity: None,
                input_fetch_keys: Vec::new(),
                timing_ms: None,
            },
        );
    }

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::WindowedBatchReport,
        run_kind: "hrrr_windowed_batch".to_string(),
        run_label: format_run_label(
            Some("hrrr"),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "windowed_batch",
        ),
        model: Some("hrrr".to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts: artifacts.into_values().collect(),
        input_fetches: normalize_input_fetches(collect_windowed_input_fetches(&report)),
    }
}

fn snapshot_from_severe_batch_report(path: &Path, report: SevereBatchReport) -> ProductRunSnapshot {
    let input_fetch_keys = report
        .input_fetches
        .iter()
        .map(|fetch| fetch.fetch_key.clone())
        .collect::<Vec<_>>();
    let artifacts = report
        .outputs
        .iter()
        .map(|output| ComparableArtifactRecord {
            artifact_key: output.product.clone(),
            lane: "severe".to_string(),
            domain_slug: Some(report.domain.slug.clone()),
            title: Some(output.title.clone()),
            path: Some(output.output_path.clone()),
            state: ComparableArtifactState::Complete,
            detail: None,
            content_identity: Some(output.output_identity.clone()),
            input_fetch_keys: input_fetch_keys.clone(),
            timing_ms: None,
        })
        .collect();

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::SevereBatchReport,
        run_kind: "severe_batch".to_string(),
        run_label: format_run_label(
            Some(report.model.as_str()),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "severe_batch",
        ),
        model: Some(report.model.as_str().to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts,
        input_fetches: normalize_input_fetches(report.input_fetches),
    }
}

fn snapshot_from_ecape_batch_report(path: &Path, report: EcapeBatchReport) -> ProductRunSnapshot {
    let input_fetch_keys = report
        .input_fetches
        .iter()
        .map(|fetch| fetch.fetch_key.clone())
        .collect::<Vec<_>>();
    let artifacts = report
        .outputs
        .iter()
        .map(|output| ComparableArtifactRecord {
            artifact_key: output.product.clone(),
            lane: "ecape".to_string(),
            domain_slug: Some(report.domain.slug.clone()),
            title: Some(output.title.clone()),
            path: Some(output.output_path.clone()),
            state: ComparableArtifactState::Complete,
            detail: Some(format!("failure_count={}", report.failure_count)),
            content_identity: Some(output.output_identity.clone()),
            input_fetch_keys: input_fetch_keys.clone(),
            timing_ms: None,
        })
        .collect();

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::EcapeBatchReport,
        run_kind: "ecape_batch".to_string(),
        run_label: format_run_label(
            Some(report.model.as_str()),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "ecape_batch",
        ),
        model: Some(report.model.as_str().to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts,
        input_fetches: normalize_input_fetches(report.input_fetches),
    }
}

fn snapshot_from_heavy_panel_hour_report(
    path: &Path,
    report: HeavyPanelHourReport,
) -> ProductRunSnapshot {
    let input_fetch_keys = report
        .input_fetches
        .iter()
        .map(|fetch| fetch.fetch_key.clone())
        .collect::<Vec<_>>();
    let mut artifacts = BTreeMap::new();
    extend_heavy_group_artifacts(
        &mut artifacts,
        &report.domain.slug,
        &report.severe,
        "severe",
        &input_fetch_keys,
    );
    extend_heavy_group_artifacts(
        &mut artifacts,
        &report.domain.slug,
        &report.ecape,
        "ecape",
        &input_fetch_keys,
    );

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::HeavyPanelHourReport,
        run_kind: "heavy_panel_hour".to_string(),
        run_label: format_run_label(
            Some(report.model.as_str()),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "heavy_panel_hour",
        ),
        model: Some(report.model.as_str().to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts: artifacts.into_values().collect(),
        input_fetches: normalize_input_fetches(report.input_fetches),
    }
}

fn snapshot_from_hrrr_batch_report(path: &Path, report: HrrrBatchReport) -> ProductRunSnapshot {
    let artifacts = report
        .products
        .iter()
        .map(|product| ComparableArtifactRecord {
            artifact_key: product.product.slug().to_string(),
            lane: "heavy".to_string(),
            domain_slug: Some(report.domain.slug.clone()),
            title: Some(product.product.slug().to_string()),
            path: Some(product.output_path.clone()),
            state: ComparableArtifactState::Complete,
            detail: product
                .metadata
                .failure_count
                .map(|count| format!("failure_count={count}")),
            content_identity: product
                .content_identity
                .clone()
                .or_else(|| maybe_artifact_identity(&product.output_path)),
            input_fetch_keys: product.input_fetch_keys.clone(),
            timing_ms: Some(product.timing.total_ms),
        })
        .collect();

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind: ComparisonInputKind::HrrrBatchReport,
        run_kind: "hrrr_batch".to_string(),
        run_label: format_run_label(
            Some("hrrr"),
            &report.date_yyyymmdd,
            report.cycle_utc,
            report.forecast_hour,
            Some(&report.domain.slug),
            "hrrr_batch",
        ),
        model: Some("hrrr".to_string()),
        date_yyyymmdd: Some(report.date_yyyymmdd.clone()),
        cycle_utc: Some(report.cycle_utc),
        forecast_hour: Some(report.forecast_hour),
        source: Some(report.source.as_str().to_string()),
        domain_slug: Some(report.domain.slug.clone()),
        run_state: None,
        run_detail: None,
        total_ms: Some(report.total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts,
        input_fetches: normalize_input_fetches(report.input_fetches),
    }
}

fn snapshot_from_hrrr_non_ecape_hour_report(
    path: &Path,
    report: HrrrNonEcapeHourReport,
) -> ProductRunSnapshot {
    let mut snapshot = build_non_ecape_hour_snapshot(
        path,
        ComparisonInputKind::HrrrNonEcapeHourReport,
        "hrrr_non_ecape_hour",
        Some("hrrr".to_string()),
        &report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.source.as_str().to_string(),
        report.domain.slug.clone(),
        report.total_ms,
        &report.out_dir,
        Some(&report.publication_manifest_path),
        report.direct.as_ref(),
        report.derived.as_ref(),
        report.windowed.as_ref(),
    );
    if let Some(manifest) = load_linked_manifest(&report.publication_manifest_path) {
        apply_manifest_metadata(&mut snapshot, &manifest);
        merge_manifest_artifacts(&mut snapshot, &manifest, None);
    }
    snapshot
}

fn snapshot_from_non_ecape_hour_report(
    path: &Path,
    report: NonEcapeHourReport,
) -> ProductRunSnapshot {
    let runner_kind = format!("{}_non_ecape_hour", report.model.as_str().replace('-', "_"));
    let mut snapshot = build_non_ecape_hour_snapshot(
        path,
        ComparisonInputKind::NonEcapeHourReport,
        &runner_kind,
        Some(report.model.as_str().to_string()),
        &report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.source.as_str().to_string(),
        report.domain.slug.clone(),
        report.total_ms,
        &report.out_dir,
        Some(&report.publication_manifest_path),
        report.direct.as_ref(),
        report.derived.as_ref(),
        report.windowed.as_ref(),
    );
    if let Some(manifest) = load_linked_manifest(&report.publication_manifest_path) {
        apply_manifest_metadata(&mut snapshot, &manifest);
        merge_manifest_artifacts(&mut snapshot, &manifest, None);
    }
    snapshot
}

fn snapshot_from_hrrr_non_ecape_multi_domain_report(
    path: &Path,
    report: HrrrNonEcapeMultiDomainReport,
) -> ProductRunSnapshot {
    let mut snapshot = build_non_ecape_multi_domain_snapshot(
        path,
        ComparisonInputKind::HrrrNonEcapeMultiDomainReport,
        "hrrr_non_ecape_multi_domain",
        Some("hrrr".to_string()),
        &report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.source.as_str().to_string(),
        report.total_ms,
    );

    for domain in &report.domains {
        extend_non_ecape_domain_artifacts(
            &mut snapshot.artifacts,
            &mut snapshot.input_fetches,
            &domain.domain.slug,
            &report.out_dir,
            domain.direct.as_ref(),
            domain.derived.as_ref(),
            domain.windowed.as_ref(),
        );
        if let Some(manifest) = load_linked_manifest(&domain.publication_manifest_path) {
            snapshot
                .linked_manifest_paths
                .push(domain.publication_manifest_path.clone());
            merge_manifest_artifacts(&mut snapshot, &manifest, Some(&domain.domain.slug));
        }
    }

    dedupe_snapshot_inputs(&mut snapshot);
    snapshot
}

fn snapshot_from_non_ecape_multi_domain_report(
    path: &Path,
    report: NonEcapeMultiDomainReport,
) -> ProductRunSnapshot {
    let runner_kind = format!(
        "{}_non_ecape_multi_domain",
        report.model.as_str().replace('-', "_")
    );
    let mut snapshot = build_non_ecape_multi_domain_snapshot(
        path,
        ComparisonInputKind::NonEcapeMultiDomainReport,
        &runner_kind,
        Some(report.model.as_str().to_string()),
        &report.date_yyyymmdd,
        report.cycle_utc,
        report.forecast_hour,
        report.source.as_str().to_string(),
        report.total_ms,
    );

    for domain in &report.domains {
        extend_non_ecape_domain_artifacts(
            &mut snapshot.artifacts,
            &mut snapshot.input_fetches,
            &domain.domain.slug,
            &report.out_dir,
            domain.direct.as_ref(),
            domain.derived.as_ref(),
            domain.windowed.as_ref(),
        );
        if let Some(manifest) = load_linked_manifest(&domain.publication_manifest_path) {
            snapshot
                .linked_manifest_paths
                .push(domain.publication_manifest_path.clone());
            merge_manifest_artifacts(&mut snapshot, &manifest, Some(&domain.domain.slug));
        }
    }

    dedupe_snapshot_inputs(&mut snapshot);
    snapshot
}

fn build_non_ecape_hour_snapshot(
    path: &Path,
    source_kind: ComparisonInputKind,
    runner_kind: &str,
    model: Option<String>,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: String,
    domain_slug: String,
    total_ms: u128,
    out_dir: &Path,
    linked_manifest: Option<&Path>,
    direct: Option<&DirectBatchReport>,
    derived: Option<&HrrrDerivedBatchReport>,
    windowed: Option<&HrrrWindowedBatchReport>,
) -> ProductRunSnapshot {
    let mut artifacts = Vec::new();
    let mut input_fetches = Vec::new();
    extend_non_ecape_domain_artifacts(
        &mut artifacts,
        &mut input_fetches,
        &domain_slug,
        out_dir,
        direct,
        derived,
        windowed,
    );

    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind,
        run_kind: runner_kind.to_string(),
        run_label: format_run_label(
            model.as_deref(),
            date_yyyymmdd,
            cycle_utc,
            forecast_hour,
            Some(&domain_slug),
            runner_kind,
        ),
        model,
        date_yyyymmdd: Some(date_yyyymmdd.to_string()),
        cycle_utc: Some(cycle_utc),
        forecast_hour: Some(forecast_hour),
        source: Some(source),
        domain_slug: Some(domain_slug),
        run_state: None,
        run_detail: None,
        total_ms: Some(total_ms),
        linked_manifest_paths: linked_manifest
            .map(|value| vec![value.to_path_buf()])
            .unwrap_or_default(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts,
        input_fetches,
    }
}

fn build_non_ecape_multi_domain_snapshot(
    path: &Path,
    source_kind: ComparisonInputKind,
    runner_kind: &str,
    model: Option<String>,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    source: String,
    total_ms: u128,
) -> ProductRunSnapshot {
    ProductRunSnapshot {
        schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
        source_path: path.to_path_buf(),
        source_kind,
        run_kind: runner_kind.to_string(),
        run_label: format!(
            "{} {} {}Z F{:03}",
            runner_kind, date_yyyymmdd, cycle_utc, forecast_hour
        ),
        model,
        date_yyyymmdd: Some(date_yyyymmdd.to_string()),
        cycle_utc: Some(cycle_utc),
        forecast_hour: Some(forecast_hour),
        source: Some(source),
        domain_slug: None,
        run_state: None,
        run_detail: None,
        total_ms: Some(total_ms),
        linked_manifest_paths: Vec::new(),
        summary: ProductRunSnapshotSummary::default(),
        artifacts: Vec::new(),
        input_fetches: Vec::new(),
    }
}

fn extend_non_ecape_domain_artifacts(
    artifacts: &mut Vec<ComparableArtifactRecord>,
    input_fetches: &mut Vec<ComparableInputFetchRecord>,
    domain_slug: &str,
    out_dir: &Path,
    direct: Option<&DirectBatchReport>,
    derived: Option<&HrrrDerivedBatchReport>,
    windowed: Option<&HrrrWindowedBatchReport>,
) {
    let mut by_key = artifacts
        .drain(..)
        .map(|artifact| (artifact.artifact_key.clone(), artifact))
        .collect::<BTreeMap<_, _>>();

    if let Some(report) = direct {
        for recipe in &report.recipes {
            insert_artifact(
                &mut by_key,
                ComparableArtifactRecord {
                    artifact_key: scope_artifact_key(
                        domain_slug,
                        &format!("direct:{}", recipe.recipe_slug),
                    ),
                    lane: "direct".to_string(),
                    domain_slug: Some(domain_slug.to_string()),
                    title: Some(recipe.title.clone()),
                    path: Some(relative_to_root(out_dir, &recipe.output_path)),
                    state: ComparableArtifactState::Complete,
                    detail: Some(format!(
                        "source_route={} planned_family={} fetched_family={} resolved_source={} resolved_url={}",
                        recipe.source_route.as_str(),
                        recipe.grib_product,
                        recipe.fetched_grib_product,
                        recipe.resolved_source.as_str(),
                        recipe.resolved_url
                    )),
                    content_identity: Some(recipe.content_identity.clone()),
                    input_fetch_keys: recipe.input_fetch_keys.clone(),
                    timing_ms: Some(recipe.timing.total_ms),
                },
            );
        }
        for blocker in &report.blockers {
            insert_artifact(
                &mut by_key,
                ComparableArtifactRecord {
                    artifact_key: scope_artifact_key(
                        domain_slug,
                        &format!("direct:{}", blocker.recipe_slug),
                    ),
                    lane: "direct".to_string(),
                    domain_slug: Some(domain_slug.to_string()),
                    title: None,
                    path: None,
                    state: ComparableArtifactState::Blocked,
                    detail: Some(blocker.reason.clone()),
                    content_identity: None,
                    input_fetch_keys: Vec::new(),
                    timing_ms: None,
                },
            );
        }
        for fetch in &report.fetches {
            input_fetches.push(normalize_input_fetch(fetch.input_fetch.clone()));
        }
    }

    if let Some(report) = derived {
        for recipe in &report.recipes {
            insert_artifact(
                &mut by_key,
                ComparableArtifactRecord {
                    artifact_key: scope_artifact_key(
                        domain_slug,
                        &format!("derived:{}", recipe.recipe_slug),
                    ),
                    lane: "derived".to_string(),
                    domain_slug: Some(domain_slug.to_string()),
                    title: Some(recipe.title.clone()),
                    path: Some(relative_to_root(out_dir, &recipe.output_path)),
                    state: ComparableArtifactState::Complete,
                    detail: Some(format!(
                        "source_mode={} source_route={}",
                        report.source_mode.as_str(),
                        recipe.source_route.as_str()
                    )),
                    content_identity: Some(recipe.content_identity.clone()),
                    input_fetch_keys: recipe.input_fetch_keys.clone(),
                    timing_ms: Some(recipe.timing.total_ms),
                },
            );
        }
        for blocker in &report.blockers {
            insert_artifact(
                &mut by_key,
                ComparableArtifactRecord {
                    artifact_key: scope_artifact_key(
                        domain_slug,
                        &format!("derived:{}", blocker.recipe_slug),
                    ),
                    lane: "derived".to_string(),
                    domain_slug: Some(domain_slug.to_string()),
                    title: None,
                    path: None,
                    state: ComparableArtifactState::Blocked,
                    detail: Some(format!(
                        "source_mode={} source_route={} {}",
                        report.source_mode.as_str(),
                        blocker.source_route.as_str(),
                        blocker.reason
                    )),
                    content_identity: None,
                    input_fetch_keys: Vec::new(),
                    timing_ms: None,
                },
            );
        }
        input_fetches.extend(normalize_input_fetches(report.input_fetches.clone()));
    }

    if let Some(report) = windowed {
        for product in &report.products {
            insert_artifact(
                &mut by_key,
                ComparableArtifactRecord {
                    artifact_key: scope_artifact_key(
                        domain_slug,
                        &format!("windowed:{}", product.product.slug()),
                    ),
                    lane: "windowed".to_string(),
                    domain_slug: Some(domain_slug.to_string()),
                    title: Some(product.product.title().to_string()),
                    path: Some(relative_to_root(out_dir, &product.output_path)),
                    state: ComparableArtifactState::Complete,
                    detail: Some(windowed_product_detail(product)),
                    content_identity: maybe_artifact_identity(&product.output_path),
                    input_fetch_keys: windowed_product_input_fetch_keys(
                        product,
                        &report.shared_timing,
                    ),
                    timing_ms: Some(product.timing.total_ms),
                },
            );
        }
        for blocker in &report.blockers {
            insert_artifact(
                &mut by_key,
                ComparableArtifactRecord {
                    artifact_key: scope_artifact_key(
                        domain_slug,
                        &format!("windowed:{}", blocker.product.slug()),
                    ),
                    lane: "windowed".to_string(),
                    domain_slug: Some(domain_slug.to_string()),
                    title: Some(blocker.product.title().to_string()),
                    path: None,
                    state: ComparableArtifactState::Blocked,
                    detail: Some(blocker.reason.clone()),
                    content_identity: None,
                    input_fetch_keys: Vec::new(),
                    timing_ms: None,
                },
            );
        }
        input_fetches.extend(normalize_input_fetches(collect_windowed_input_fetches(
            report,
        )));
    }

    *artifacts = by_key.into_values().collect();
}

fn extend_heavy_group_artifacts(
    artifacts: &mut BTreeMap<String, ComparableArtifactRecord>,
    domain_slug: &str,
    group: &HeavyRenderedArtifactGroup,
    lane: &str,
    input_fetch_keys: &[String],
) {
    for output in &group.outputs {
        insert_artifact(
            artifacts,
            ComparableArtifactRecord {
                artifact_key: output.product.clone(),
                lane: lane.to_string(),
                domain_slug: Some(domain_slug.to_string()),
                title: Some(output.title.clone()),
                path: Some(output.output_path.clone()),
                state: ComparableArtifactState::Complete,
                detail: group
                    .failure_count
                    .map(|count| format!("failure_count={count}")),
                content_identity: Some(output.output_identity.clone()),
                input_fetch_keys: input_fetch_keys.to_vec(),
                timing_ms: None,
            },
        );
    }
}

fn finalize_snapshot(mut snapshot: ProductRunSnapshot) -> ProductRunSnapshot {
    snapshot.artifacts.sort_by(|left, right| {
        left.artifact_key
            .cmp(&right.artifact_key)
            .then_with(|| left.lane.cmp(&right.lane))
    });
    snapshot
        .input_fetches
        .sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
    snapshot.summary = ProductRunSnapshotSummary {
        artifact_count: snapshot.artifacts.len(),
        blocker_count: snapshot
            .artifacts
            .iter()
            .filter(|artifact| artifact.state == ComparableArtifactState::Blocked)
            .count(),
        complete_artifact_count: snapshot
            .artifacts
            .iter()
            .filter(|artifact| {
                matches!(
                    artifact.state,
                    ComparableArtifactState::Complete | ComparableArtifactState::CacheHit
                )
            })
            .count(),
        input_fetch_count: snapshot.input_fetches.len(),
    };
    snapshot
}

fn build_relation(left: &ProductRunSnapshot, right: &ProductRunSnapshot) -> ProductRunRelation {
    let same_run_kind = left.run_kind == right.run_kind;
    let same_model = left.model == right.model;
    let same_source = left.source == right.source;
    let same_domain = left.domain_slug == right.domain_slug;
    let same_date = left.date_yyyymmdd == right.date_yyyymmdd;
    let same_cycle_utc = left.cycle_utc == right.cycle_utc;
    let same_forecast_hour = left.forecast_hour == right.forecast_hour;

    let kind = if same_run_kind
        && same_model
        && same_source
        && same_domain
        && same_date
        && same_cycle_utc
        && same_forecast_hour
    {
        ProductRunRelationKind::SameRun
    } else if same_run_kind
        && same_model
        && same_source
        && same_domain
        && same_date
        && same_cycle_utc
        && !same_forecast_hour
    {
        ProductRunRelationKind::HourToHour
    } else if same_run_kind
        && same_model
        && same_source
        && same_domain
        && same_forecast_hour
        && (!same_date || !same_cycle_utc)
    {
        ProductRunRelationKind::RunToRun
    } else if same_run_kind && same_model && same_source && same_domain {
        ProductRunRelationKind::MixedTemporal
    } else {
        ProductRunRelationKind::Arbitrary
    };

    ProductRunRelation {
        kind,
        same_run_kind,
        same_model,
        same_source,
        same_domain,
        same_date,
        same_cycle_utc,
        same_forecast_hour,
        cycle_delta_hours: signed_delta_i32_opt(left.cycle_utc, right.cycle_utc),
        forecast_hour_delta: signed_delta_i32_opt(left.forecast_hour, right.forecast_hour),
    }
}

fn compare_artifact_records(
    artifact_key: String,
    left: Option<ComparableArtifactRecord>,
    right: Option<ComparableArtifactRecord>,
) -> ArtifactComparisonRecord {
    match (left, right) {
        (None, Some(right)) => ArtifactComparisonRecord {
            artifact_key,
            change: ComparisonChangeKind::Added,
            content_changed: false,
            state_changed: false,
            detail_changed: false,
            lane_changed: false,
            title_changed: false,
            domain_changed: false,
            input_fetch_keys_changed: false,
            timing_changed: false,
            path_changed: false,
            timing_delta_ms: None,
            left: None,
            right: Some(right),
        },
        (Some(left), None) => ArtifactComparisonRecord {
            artifact_key,
            change: ComparisonChangeKind::Removed,
            content_changed: false,
            state_changed: false,
            detail_changed: false,
            lane_changed: false,
            title_changed: false,
            domain_changed: false,
            input_fetch_keys_changed: false,
            timing_changed: false,
            path_changed: false,
            timing_delta_ms: None,
            left: Some(left),
            right: None,
        },
        (Some(left), Some(right)) => {
            let content_changed = left.content_identity != right.content_identity;
            let state_changed = left.state != right.state;
            let detail_changed = left.detail != right.detail;
            let lane_changed = left.lane != right.lane;
            let title_changed = left.title != right.title;
            let domain_changed = left.domain_slug != right.domain_slug;
            let input_fetch_keys_changed = left.input_fetch_keys != right.input_fetch_keys;
            let timing_changed = left.timing_ms != right.timing_ms;
            let path_changed = left.path != right.path;
            let materially_changed = content_changed
                || state_changed
                || detail_changed
                || lane_changed
                || title_changed
                || domain_changed
                || input_fetch_keys_changed
                || timing_changed;

            ArtifactComparisonRecord {
                artifact_key,
                change: if materially_changed {
                    ComparisonChangeKind::Changed
                } else {
                    ComparisonChangeKind::Unchanged
                },
                content_changed,
                state_changed,
                detail_changed,
                lane_changed,
                title_changed,
                domain_changed,
                input_fetch_keys_changed,
                timing_changed,
                path_changed,
                timing_delta_ms: signed_delta_opt(left.timing_ms, right.timing_ms),
                left: Some(left),
                right: Some(right),
            }
        }
        (None, None) => unreachable!(),
    }
}

fn compare_input_fetch_records(
    logical_key: String,
    left: Option<ComparableInputFetchRecord>,
    right: Option<ComparableInputFetchRecord>,
) -> InputFetchComparisonRecord {
    match (left, right) {
        (None, Some(right)) => InputFetchComparisonRecord {
            logical_key,
            change: ComparisonChangeKind::Added,
            fetch_key_changed: false,
            bytes_changed: false,
            route_changed: false,
            source_changed: false,
            left: None,
            right: Some(right),
        },
        (Some(left), None) => InputFetchComparisonRecord {
            logical_key,
            change: ComparisonChangeKind::Removed,
            fetch_key_changed: false,
            bytes_changed: false,
            route_changed: false,
            source_changed: false,
            left: Some(left),
            right: None,
        },
        (Some(left), Some(right)) => {
            let fetch_key_changed = left.fetch_key != right.fetch_key;
            let bytes_changed =
                left.bytes_len != right.bytes_len || left.bytes_sha256 != right.bytes_sha256;
            let route_changed = left.planned_family != right.planned_family
                || left.planned_family_aliases != right.planned_family_aliases
                || left.resolved_family != right.resolved_family
                || left.resolved_url != right.resolved_url;
            let source_changed = left.source_override != right.source_override
                || left.resolved_source != right.resolved_source;
            let changed = fetch_key_changed || bytes_changed || route_changed || source_changed;

            InputFetchComparisonRecord {
                logical_key,
                change: if changed {
                    ComparisonChangeKind::Changed
                } else {
                    ComparisonChangeKind::Unchanged
                },
                fetch_key_changed,
                bytes_changed,
                route_changed,
                source_changed,
                left: Some(left),
                right: Some(right),
            }
        }
        (None, None) => unreachable!(),
    }
}

fn normalize_manifest_artifact(
    output_root: &Path,
    artifact_key: String,
    artifact: &crate::publication::PublishedArtifactRecord,
    domain_slug: Option<String>,
    fallback_lane: Option<&str>,
) -> ComparableArtifactRecord {
    let path = Some(artifact.relative_path.clone());
    let lane = infer_lane_from_artifact_key(&artifact_key)
        .unwrap_or_else(|| fallback_lane.unwrap_or("artifact"))
        .to_string();
    let content_identity = artifact.content_identity.clone().or_else(|| {
        let candidate = absolute_from_root(output_root, &artifact.relative_path);
        maybe_artifact_identity(&candidate)
    });

    ComparableArtifactRecord {
        artifact_key,
        lane,
        domain_slug,
        title: None,
        path,
        state: artifact.state.into(),
        detail: artifact.detail.clone(),
        content_identity,
        input_fetch_keys: artifact.input_fetch_keys.clone(),
        timing_ms: None,
    }
}

fn normalize_input_fetches(
    fetches: Vec<PublishedFetchIdentity>,
) -> Vec<ComparableInputFetchRecord> {
    let mut by_key = BTreeMap::new();
    for fetch in fetches {
        let normalized = normalize_input_fetch(fetch);
        by_key
            .entry(normalized.logical_key.clone())
            .or_insert(normalized);
    }
    by_key.into_values().collect()
}

fn normalize_input_fetch(fetch: PublishedFetchIdentity) -> ComparableInputFetchRecord {
    ComparableInputFetchRecord {
        logical_key: logical_fetch_key(&fetch),
        fetch_key: fetch.fetch_key,
        planned_family: fetch.planned_family,
        planned_family_aliases: fetch.planned_family_aliases,
        request: fetch.request,
        source_override: fetch.source_override,
        resolved_source: fetch.resolved_source,
        resolved_url: fetch.resolved_url,
        resolved_family: fetch.resolved_family,
        bytes_len: fetch.bytes_len,
        bytes_sha256: fetch.bytes_sha256,
    }
}

fn logical_fetch_key(fetch: &PublishedFetchIdentity) -> String {
    format!(
        "{}:f{:03}:{}->{}",
        fetch.request.model.as_str(),
        fetch.request.forecast_hour,
        fetch.planned_family,
        fetch.resolved_family
    )
}

fn infer_lane_from_artifact_key(key: &str) -> Option<&str> {
    if key.contains("/direct:") || key.starts_with("direct:") {
        Some("direct")
    } else if key.contains("/derived:") || key.starts_with("derived:") {
        Some("derived")
    } else if key.contains("/windowed:") || key.starts_with("windowed:") {
        Some("windowed")
    } else {
        None
    }
}

fn load_linked_manifest(path: &Path) -> Option<RunPublicationManifest> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice::<RunPublicationManifest>(&bytes).ok()
}

fn apply_manifest_metadata(snapshot: &mut ProductRunSnapshot, manifest: &RunPublicationManifest) {
    snapshot.run_kind = manifest.run_kind.clone();
    snapshot.run_label = manifest.run_label.clone();
    snapshot.run_state = Some(run_state_str(manifest.state).to_string());
    snapshot.run_detail = manifest.detail.clone();
}

fn merge_manifest_artifacts(
    snapshot: &mut ProductRunSnapshot,
    manifest: &RunPublicationManifest,
    domain_prefix: Option<&str>,
) {
    let mut by_key = snapshot
        .artifacts
        .drain(..)
        .map(|artifact| (artifact.artifact_key.clone(), artifact))
        .collect::<BTreeMap<_, _>>();

    for artifact in &manifest.artifacts {
        let scoped_key = scope_artifact_key_opt(domain_prefix, &artifact.artifact_key);
        let normalized = normalize_manifest_artifact(
            &manifest.output_root,
            scoped_key.clone(),
            artifact,
            domain_prefix
                .map(str::to_string)
                .or_else(|| manifest.domain_slug.clone()),
            Some(manifest.run_kind.as_str()),
        );
        if let Some(existing) = by_key.get_mut(&scoped_key) {
            merge_artifact_record(existing, normalized);
        } else {
            by_key.insert(scoped_key, normalized);
        }
    }

    snapshot.artifacts = by_key.into_values().collect();
    snapshot
        .input_fetches
        .extend(normalize_input_fetches(manifest.input_fetches.clone()));
    dedupe_snapshot_inputs(snapshot);
}

fn merge_artifact_record(
    existing: &mut ComparableArtifactRecord,
    manifest_record: ComparableArtifactRecord,
) {
    existing.path = manifest_record.path.or(existing.path.clone());
    existing.state = manifest_record.state;
    existing.detail = manifest_record.detail.or(existing.detail.clone());
    existing.content_identity = manifest_record
        .content_identity
        .or(existing.content_identity.clone());
    if !manifest_record.input_fetch_keys.is_empty() {
        existing.input_fetch_keys = manifest_record.input_fetch_keys;
    }
    if existing.domain_slug.is_none() {
        existing.domain_slug = manifest_record.domain_slug;
    }
}

fn dedupe_snapshot_inputs(snapshot: &mut ProductRunSnapshot) {
    let mut by_key = BTreeMap::new();
    for fetch in snapshot.input_fetches.drain(..) {
        by_key.entry(fetch.logical_key.clone()).or_insert(fetch);
    }
    snapshot.input_fetches = by_key.into_values().collect();
}

fn maybe_artifact_identity(path: &Path) -> Option<ArtifactContentIdentity> {
    if !path.exists() {
        return None;
    }
    artifact_identity_from_path(path).ok()
}

fn insert_artifact(
    artifacts: &mut BTreeMap<String, ComparableArtifactRecord>,
    artifact: ComparableArtifactRecord,
) {
    artifacts.insert(artifact.artifact_key.clone(), artifact);
}

fn relative_to_root(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| path.to_path_buf())
}

fn absolute_from_root(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn scope_artifact_key(domain_slug: &str, artifact_key: &str) -> String {
    format!("{domain_slug}/{artifact_key}")
}

fn scope_artifact_key_opt(domain_slug: Option<&str>, artifact_key: &str) -> String {
    domain_slug
        .map(|domain| scope_artifact_key(domain, artifact_key))
        .unwrap_or_else(|| artifact_key.to_string())
}

fn format_run_label(
    model: Option<&str>,
    date_yyyymmdd: &str,
    cycle_utc: u8,
    forecast_hour: u16,
    domain_slug: Option<&str>,
    suffix: &str,
) -> String {
    let model = model.unwrap_or("rustwx");
    match domain_slug {
        Some(domain) => {
            format!("{model} {date_yyyymmdd} {cycle_utc:02}Z F{forecast_hour:03} {domain} {suffix}")
        }
        None => format!("{model} {date_yyyymmdd} {cycle_utc:02}Z F{forecast_hour:03} {suffix}"),
    }
}

fn run_state_str(state: crate::publication::RunPublicationState) -> &'static str {
    match state {
        crate::publication::RunPublicationState::Planned => "planned",
        crate::publication::RunPublicationState::Running => "running",
        crate::publication::RunPublicationState::Complete => "complete",
        crate::publication::RunPublicationState::Partial => "partial",
        crate::publication::RunPublicationState::Failed => "failed",
    }
}

fn signed_delta_opt(left: Option<u128>, right: Option<u128>) -> Option<i128> {
    match (left, right) {
        (Some(left), Some(right)) => Some(signed_delta(left, right)),
        _ => None,
    }
}

fn signed_delta(left: u128, right: u128) -> i128 {
    if right >= left {
        (right - left) as i128
    } else {
        -((left - right) as i128)
    }
}

fn signed_delta_i32_opt<T>(left: Option<T>, right: Option<T>) -> Option<i32>
where
    T: Into<i32> + Copy,
{
    match (left, right) {
        (Some(left), Some(right)) => Some(right.into() - left.into()),
        _ => None,
    }
}

fn windowed_product_detail(product: &crate::windowed::HrrrWindowedRenderedProduct) -> String {
    let hours = product
        .metadata
        .contributing_forecast_hours
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(",");
    match product.metadata.window_hours {
        Some(window_hours) => format!(
            "strategy={} contributing_forecast_hours=[{}] window_hours={}",
            product.metadata.strategy, hours, window_hours
        ),
        None => format!(
            "strategy={} contributing_forecast_hours=[{}]",
            product.metadata.strategy, hours
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publication::{ArtifactPublicationState, PublishedArtifactRecord};
    use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};

    fn sample_snapshot(path: &str) -> ProductRunSnapshot {
        finalize_snapshot(ProductRunSnapshot {
            schema_version: PRODUCT_COMPARISON_SCHEMA_VERSION,
            source_path: PathBuf::from(path),
            source_kind: ComparisonInputKind::RunManifest,
            run_kind: "hrrr_non_ecape_hour".to_string(),
            run_label: "sample".to_string(),
            model: Some("hrrr".to_string()),
            date_yyyymmdd: Some("20260422".to_string()),
            cycle_utc: Some(12),
            forecast_hour: Some(0),
            source: Some("nomads".to_string()),
            domain_slug: Some("conus".to_string()),
            run_state: Some("complete".to_string()),
            run_detail: None,
            total_ms: Some(100),
            linked_manifest_paths: Vec::new(),
            summary: ProductRunSnapshotSummary::default(),
            artifacts: vec![ComparableArtifactRecord {
                artifact_key: "direct:visibility".to_string(),
                lane: "direct".to_string(),
                domain_slug: Some("conus".to_string()),
                title: Some("Visibility".to_string()),
                path: Some(PathBuf::from("a.png")),
                state: ComparableArtifactState::Complete,
                detail: Some("ok".to_string()),
                content_identity: Some(ArtifactContentIdentity {
                    bytes_len: 10,
                    sha256: "abc".to_string(),
                }),
                input_fetch_keys: vec!["fetch-a".to_string()],
                timing_ms: Some(10),
            }],
            input_fetches: vec![ComparableInputFetchRecord {
                logical_key: "hrrr:f000:sfc->wrfsfcf00".to_string(),
                fetch_key: "full-fetch-key".to_string(),
                planned_family: "sfc".to_string(),
                planned_family_aliases: Vec::new(),
                request: ModelRunRequest::new(
                    ModelId::Hrrr,
                    CycleSpec::new("20260422", 12).unwrap(),
                    0,
                    "wrfsfcf00",
                )
                .unwrap(),
                source_override: None,
                resolved_source: SourceId::Nomads,
                resolved_url: "https://example.test/a".to_string(),
                resolved_family: "wrfsfcf00".to_string(),
                bytes_len: 100,
                bytes_sha256: "fetchsha".to_string(),
            }],
        })
    }

    #[test]
    fn comparison_ignores_path_only_changes_for_material_change_count() {
        let left = sample_snapshot("left.json");
        let mut right = sample_snapshot("right.json");
        right.artifacts[0].path = Some(PathBuf::from("b.png"));
        right = finalize_snapshot(right);

        let comparison = compare_product_runs(&left, &right);
        assert_eq!(comparison.summary.artifact_changed_count, 0);
        assert_eq!(comparison.summary.artifact_unchanged_count, 1);
        assert_eq!(comparison.summary.artifact_path_changed_count, 1);
        assert!(comparison.artifact_changes[0].path_changed);
        assert_eq!(
            comparison.artifact_changes[0].change,
            ComparisonChangeKind::Unchanged
        );
    }

    #[test]
    fn relation_classifies_run_to_run_and_hour_to_hour() {
        let left = sample_snapshot("left.json");

        let mut run_to_run = sample_snapshot("run_to_run.json");
        run_to_run.cycle_utc = Some(13);
        run_to_run = finalize_snapshot(run_to_run);
        assert_eq!(
            build_relation(&left, &run_to_run).kind,
            ProductRunRelationKind::RunToRun
        );

        let mut hour_to_hour = sample_snapshot("hour_to_hour.json");
        hour_to_hour.forecast_hour = Some(1);
        hour_to_hour = finalize_snapshot(hour_to_hour);
        assert_eq!(
            build_relation(&left, &hour_to_hour).kind,
            ProductRunRelationKind::HourToHour
        );
    }

    #[test]
    fn detect_input_kind_identifies_manifest_and_non_ecape_reports() {
        let manifest = serde_json::json!({
            "schema_version": 4,
            "run_kind": "hrrr_non_ecape_hour",
            "run_label": "label",
            "output_root": "proof",
            "state": "complete",
            "started_unix_ms": 1,
            "finished_unix_ms": 2,
            "detail": null,
            "input_fetches": [],
            "artifacts": []
        });
        assert_eq!(
            detect_input_kind(&manifest),
            Some(ComparisonInputKind::RunManifest)
        );

        let non_ecape = serde_json::json!({
            "model": "hrrr",
            "date_yyyymmdd": "20260422",
            "cycle_utc": 12,
            "forecast_hour": 0,
            "source": "nomads",
            "domain": { "slug": "conus", "bounds": [-127.0, -66.0, 23.0, 51.5] },
            "out_dir": "proof",
            "cache_root": "proof/cache",
            "use_cache": true,
            "publication_manifest_path": "proof/run_manifest.json",
            "requested": {
                "direct_recipe_slugs": [],
                "derived_recipe_slugs": [],
                "windowed_products": []
            },
            "shared_timing": {},
            "summary": {
                "runner_count": 0,
                "direct_rendered_count": 0,
                "derived_rendered_count": 0,
                "windowed_rendered_count": 0,
                "windowed_blocker_count": 0,
                "output_count": 0,
                "output_paths": []
            },
            "direct": null,
            "derived": null,
            "windowed": null,
            "total_ms": 0
        });
        assert_eq!(
            detect_input_kind(&non_ecape),
            Some(ComparisonInputKind::NonEcapeHourReport)
        );
    }

    #[test]
    fn logical_fetch_key_ignores_cycle_date_but_keeps_forecast_hour() {
        let base_request = ModelRunRequest::new(
            ModelId::Hrrr,
            CycleSpec::new("20260422", 12).unwrap(),
            0,
            "wrfsfcf00",
        )
        .unwrap();
        let other_request = ModelRunRequest::new(
            ModelId::Hrrr,
            CycleSpec::new("20260423", 0).unwrap(),
            0,
            "wrfsfcf00",
        )
        .unwrap();
        let left = PublishedFetchIdentity {
            fetch_key: "left".to_string(),
            planned_family: "sfc".to_string(),
            planned_family_aliases: Vec::new(),
            request: base_request,
            source_override: None,
            resolved_source: SourceId::Nomads,
            resolved_url: "https://example.test/left".to_string(),
            resolved_family: "wrfsfcf00".to_string(),
            bytes_len: 1,
            bytes_sha256: "a".to_string(),
        };
        let right = PublishedFetchIdentity {
            fetch_key: "right".to_string(),
            planned_family: "sfc".to_string(),
            planned_family_aliases: Vec::new(),
            request: other_request,
            source_override: None,
            resolved_source: SourceId::Nomads,
            resolved_url: "https://example.test/right".to_string(),
            resolved_family: "wrfsfcf00".to_string(),
            bytes_len: 2,
            bytes_sha256: "b".to_string(),
        };

        assert_eq!(logical_fetch_key(&left), logical_fetch_key(&right));
    }

    #[test]
    fn normalize_manifest_artifact_backfills_lane_from_key() {
        let record = PublishedArtifactRecord::planned("direct:visibility", "vis.png")
            .with_state(ArtifactPublicationState::Complete);
        let normalized = normalize_manifest_artifact(
            Path::new("proof"),
            "direct:visibility".to_string(),
            &record,
            Some("conus".to_string()),
            Some("hrrr_non_ecape_hour"),
        );
        assert_eq!(normalized.lane, "direct");
        assert_eq!(normalized.state, ComparableArtifactState::Complete);
    }
}
