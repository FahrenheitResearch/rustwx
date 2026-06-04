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
