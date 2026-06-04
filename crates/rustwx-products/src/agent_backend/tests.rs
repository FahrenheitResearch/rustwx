use rustwx_core::ModelId;

use super::*;

#[test]
fn preflight_reports_complete_direct_product_for_explicit_model() {
    let report = build_agent_preflight(AgentPreflightRequest {
        model: Some(ModelId::Hrrr),
        products: vec!["2m_temperature_10m_winds".to_string()],
        include_all_catalog_entries: false,
    });

    assert_eq!(report.summary.reported_products, 1);
    assert_eq!(report.summary.complete_products, 1);
    assert_eq!(report.summary.blocked_products, 0);
    let product = &report.products[0];
    assert_eq!(product.preflight_status, AgentPreflightStatus::Complete);
    assert_eq!(product.tier, AgentUserTier::Basic);
    assert_eq!(product.primary_lane, AgentExecutionLane::Background);
    assert!(product.callable_surfaces.iter().any(|surface| {
        surface.surface == AgentCallableSurface::ProductSampling
            && surface.lane == AgentExecutionLane::Interactive
    }));
}

#[test]
fn preflight_turns_partial_catalog_status_into_model_specific_blocker() {
    let report = build_agent_preflight(AgentPreflightRequest {
        model: Some(ModelId::Gfs),
        products: vec!["composite_reflectivity_uh".to_string()],
        include_all_catalog_entries: false,
    });

    assert_eq!(report.summary.reported_products, 1);
    assert_eq!(report.summary.blocked_products, 1);
    let product = &report.products[0];
    assert_eq!(product.preflight_status, AgentPreflightStatus::Blocked);
    assert!(product.blocked_targets.contains(&"gfs".to_string()));
    assert!(!product.blockers.is_empty());
}

#[test]
fn preflight_preserves_partial_status_when_model_is_unspecified() {
    let report = build_agent_preflight(AgentPreflightRequest {
        model: None,
        products: vec!["composite_reflectivity_uh".to_string()],
        include_all_catalog_entries: false,
    });

    assert_eq!(report.summary.partial_products, 1);
    let product = &report.products[0];
    assert_eq!(product.preflight_status, AgentPreflightStatus::Partial);
    assert!(
        product
            .notes
            .iter()
            .any(|note| note.contains("model targets"))
    );
}

#[test]
fn preflight_resolves_legacy_alias_to_canonical_product() {
    let report = build_agent_preflight(AgentPreflightRequest {
        model: Some(ModelId::Hrrr),
        products: vec!["1h_qpf".to_string()],
        include_all_catalog_entries: false,
    });

    assert_eq!(report.summary.complete_products, 1);
    let product = &report.products[0];
    assert_eq!(product.slug, "qpf_1h");
    assert!(product.notes.iter().any(|note| note.contains("alias")));
}

#[test]
fn preflight_reports_unknown_products_as_blocked_planning_facts() {
    let report = build_agent_preflight(AgentPreflightRequest {
        model: Some(ModelId::Hrrr),
        products: vec!["not_a_real_product".to_string()],
        include_all_catalog_entries: false,
    });

    assert_eq!(report.summary.reported_products, 0);
    assert_eq!(report.summary.unknown_products, 1);
    assert_eq!(report.summary.blocked_products, 1);
    assert_eq!(
        report.unknown_products[0].status,
        AgentPreflightStatus::Blocked
    );
}
