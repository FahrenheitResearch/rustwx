use rustwx_core::{
    BundleRequirement, CanonicalBundleDescriptor, CanonicalBundleId, CanonicalDataFamily,
    CanonicalField, CycleSpec, FieldSelector, ModelId, ModelRunRequest, SourceId,
};
use rustwx_products::WeatherPanelField;
use rustwx_products::publication::{
    ArtifactPublicationState, PublishedArtifactRecord, PublishedFetchIdentity,
    RunPublicationManifest, RunPublicationState, artifact_identity_from_bytes,
};
use rustwx_render::WeatherProduct;

#[test]
fn structured_query_contract_keeps_selector_metadata_and_bundle_identity_stable() {
    let selector = FieldSelector::isobaric(CanonicalField::Temperature, 500);
    assert_eq!(selector.key(), "temperature_500hpa");
    assert_eq!(selector.display_name(), "Temperature (500hpa)");
    assert_eq!(selector.native_units(), "K");

    let metadata = selector.product_metadata();
    assert_eq!(metadata.display_name, "Temperature (500hpa)");
    assert_eq!(metadata.native_units.as_deref(), Some("K"));
    assert_eq!(
        metadata
            .provenance
            .as_ref()
            .and_then(|provenance| provenance.selector),
        Some(selector)
    );

    let requirement = BundleRequirement::new(CanonicalBundleDescriptor::PressureAnalysis, 12)
        .with_native_override("pgrb2.0p25");
    let id = CanonicalBundleId::new(
        ModelId::Gfs,
        CycleSpec::new("20260422", 18).unwrap(),
        requirement.forecast_hour,
        SourceId::Aws,
        requirement.bundle,
        requirement.native_override.clone().unwrap(),
    );
    assert_eq!(id.family(), CanonicalDataFamily::Pressure);
    assert!(id.to_string().contains("f012"));
    assert!(id.to_string().ends_with(":pgrb2.0p25"));
}

#[test]
fn artifact_bundle_manifest_round_trips_named_asset_fetch_links() {
    let request = ModelRunRequest::new(
        ModelId::Gfs,
        CycleSpec::new("20260422", 18).unwrap(),
        12,
        "pgrb2.0p25",
    )
    .unwrap();
    let field = WeatherPanelField::new(WeatherProduct::Scp, "dimensionless", vec![1.0])
        .with_title_override("SCP (ML proxy)")
        .with_artifact_slug("scp_ml_proxy");

    let mut manifest = RunPublicationManifest::new(
        "weather_intelligence_demo",
        "gfs_20260422_18z_f012_demo",
        "proof/demo",
    )
    .with_input_fetches(vec![PublishedFetchIdentity {
        fetch_key: "native:gfs:f012".into(),
        planned_family: "nat".into(),
        planned_family_aliases: vec!["surface".into(), "pressure".into()],
        request,
        source_override: Some(SourceId::Aws),
        resolved_source: SourceId::Aws,
        resolved_url: "https://example.test/gfs/f012/pgrb2.0p25.grib2".into(),
        resolved_family: "pressure".into(),
        bytes_len: 4096,
        bytes_sha256: "demo-bytes-sha256".into(),
    }])
    .with_artifacts(vec![
        PublishedArtifactRecord::planned(field.artifact_slug(), "named/scp_ml_proxy.png")
            .with_state(ArtifactPublicationState::Complete)
            .with_input_fetch_keys(vec!["native:gfs:f012".into()])
            .with_content_identity(artifact_identity_from_bytes(b"png")),
    ]);

    manifest.finalize_from_artifact_states();
    assert_eq!(manifest.state, RunPublicationState::Complete);

    let serialized = serde_json::to_string(&manifest).unwrap();
    let round_tripped: RunPublicationManifest = serde_json::from_str(&serialized).unwrap();
    assert_eq!(
        round_tripped.input_fetches[0].planned_family_aliases,
        vec!["surface".to_string(), "pressure".to_string()]
    );
    assert_eq!(round_tripped.artifacts[0].artifact_key, "scp_ml_proxy");
    assert_eq!(
        round_tripped.artifacts[0].relative_path,
        std::path::PathBuf::from("named/scp_ml_proxy.png")
    );
    assert_eq!(
        round_tripped.artifacts[0].input_fetch_keys,
        vec!["native:gfs:f012".to_string()]
    );
}
