use rustwx_core::{ModelId, ProductId, ProductKeyMetadata, ProductKind};
use rustwx_models::{
    PlotRecipeFetchMode, built_in_models, plot_recipe_fetch_blockers, plot_recipe_fetch_plan,
};
use rustwx_render::{ProductMaturity, ProductSemanticFlag};
use serde::{Deserialize, Serialize};

use crate::spec::{
    ProductSpec, blocked_derived_product_specs, direct_product_specs, heavy_product_specs,
    supported_derived_product_specs, windowed_product_specs,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductCatalogKind {
    Direct,
    Derived,
    Heavy,
    Windowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductCatalogStatus {
    Supported,
    Partial,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductTargetStatus {
    Supported,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductCatalogSummary {
    pub total_entries: usize,
    pub direct_entries: usize,
    pub derived_entries: usize,
    pub heavy_entries: usize,
    pub windowed_entries: usize,
    pub supported_entries: usize,
    pub partial_entries: usize,
    pub blocked_entries: usize,
    pub experimental_entries: usize,
    pub proof_entries: usize,
    pub proxy_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductTargetSupport {
    pub target: String,
    pub model: Option<ModelId>,
    pub status: ProductTargetStatus,
    pub fetch_mode: Option<PlotRecipeFetchMode>,
    pub grib_product: Option<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductCatalogAlias {
    pub id: ProductId,
    pub slug: String,
    pub title: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductCatalogEntry {
    pub id: ProductId,
    pub slug: String,
    pub title: String,
    pub kind: ProductCatalogKind,
    pub status: ProductCatalogStatus,
    pub product_metadata: Option<ProductKeyMetadata>,
    pub maturity: ProductMaturity,
    pub flags: Vec<ProductSemanticFlag>,
    pub experimental: bool,
    pub render_style: Option<String>,
    pub runners: Vec<String>,
    pub aliases: Vec<ProductCatalogAlias>,
    pub notes: Vec<String>,
    pub support: Vec<ProductTargetSupport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupportedProductsCatalog {
    pub summary: ProductCatalogSummary,
    pub direct: Vec<ProductCatalogEntry>,
    pub derived: Vec<ProductCatalogEntry>,
    pub heavy: Vec<ProductCatalogEntry>,
    pub windowed: Vec<ProductCatalogEntry>,
}

pub fn build_supported_products_catalog() -> SupportedProductsCatalog {
    let direct = build_direct_entries();
    let derived = build_derived_entries();
    let heavy = build_heavy_entries();
    let windowed = build_windowed_entries();

    let all = direct
        .iter()
        .chain(derived.iter())
        .chain(heavy.iter())
        .chain(windowed.iter());

    let mut summary = ProductCatalogSummary {
        total_entries: direct.len() + derived.len() + heavy.len() + windowed.len(),
        direct_entries: direct.len(),
        derived_entries: derived.len(),
        heavy_entries: heavy.len(),
        windowed_entries: windowed.len(),
        supported_entries: 0,
        partial_entries: 0,
        blocked_entries: 0,
        experimental_entries: 0,
        proof_entries: 0,
        proxy_entries: 0,
    };
    for entry in all {
        match entry.status {
            ProductCatalogStatus::Supported => summary.supported_entries += 1,
            ProductCatalogStatus::Partial => summary.partial_entries += 1,
            ProductCatalogStatus::Blocked => summary.blocked_entries += 1,
        }
        match entry.maturity {
            ProductMaturity::Operational => {}
            ProductMaturity::Experimental => summary.experimental_entries += 1,
            ProductMaturity::Proof => summary.proof_entries += 1,
        }
        if entry.flags.contains(&ProductSemanticFlag::Proxy) {
            summary.proxy_entries += 1;
        }
    }

    SupportedProductsCatalog {
        summary,
        direct,
        derived,
        heavy,
        windowed,
    }
}

fn build_direct_entries() -> Vec<ProductCatalogEntry> {
    direct_product_specs()
        .into_iter()
        .map(|spec| {
            let support = built_in_models()
                .iter()
                .map(|model| match plot_recipe_fetch_plan(&spec.slug, model.id) {
                    Ok(plan) => ProductTargetSupport {
                        target: model.id.to_string(),
                        model: Some(model.id),
                        status: ProductTargetStatus::Supported,
                        fetch_mode: Some(plan.fetch_mode),
                        grib_product: Some(plan.product.to_string()),
                        blockers: Vec::new(),
                    },
                    Err(_) => ProductTargetSupport {
                        target: model.id.to_string(),
                        model: Some(model.id),
                        status: ProductTargetStatus::Blocked,
                        fetch_mode: None,
                        grib_product: None,
                        blockers: plot_recipe_fetch_blockers(&spec.slug, model.id)
                            .unwrap_or_default()
                            .into_iter()
                            .map(|blocker| format!("{}: {}", blocker.field_label, blocker.reason))
                            .collect(),
                    },
                })
                .collect::<Vec<_>>();

            let mut runners = vec!["plot_recipe_proof".to_string()];
            if support
                .iter()
                .any(|target| matches!(target.status, ProductTargetStatus::Supported))
            {
                runners.push("direct_batch".to_string());
            }
            if support.iter().any(|target| {
                target.model == Some(ModelId::Hrrr)
                    && matches!(target.status, ProductTargetStatus::Supported)
            }) {
                runners.push("hrrr_direct_batch".to_string());
                runners.push("hrrr_non_ecape_hour".to_string());
            }

            build_catalog_entry(spec, collapse_entry_status(&support), runners, support)
        })
        .collect()
}

fn build_derived_entries() -> Vec<ProductCatalogEntry> {
    let supported_models = built_in_models()
        .iter()
        .map(|model| ProductTargetSupport {
            target: model.id.to_string(),
            model: Some(model.id),
            status: ProductTargetStatus::Supported,
            fetch_mode: None,
            grib_product: None,
            blockers: Vec::new(),
        })
        .collect::<Vec<_>>();

    let mut entries = supported_derived_product_specs()
        .into_iter()
        .map(|spec| {
            let mut runners = vec!["derived_batch".to_string()];
            if supported_models
                .iter()
                .any(|target| target.model == Some(ModelId::Hrrr))
            {
                runners.push("hrrr_derived_batch".to_string());
                runners.push("hrrr_non_ecape_hour".to_string());
            }
            build_catalog_entry(
                spec,
                ProductCatalogStatus::Supported,
                runners,
                supported_models.clone(),
            )
        })
        .collect::<Vec<_>>();

    entries.extend(blocked_derived_product_specs().into_iter().map(|spec| {
        let blockers = spec.blocked_reasons.clone();
        let blocked_support = built_in_models()
            .iter()
            .map(|model| ProductTargetSupport {
                target: model.id.to_string(),
                model: Some(model.id),
                status: ProductTargetStatus::Blocked,
                fetch_mode: None,
                grib_product: None,
                blockers: blockers.clone(),
            })
            .collect::<Vec<_>>();
        build_catalog_entry(
            spec,
            ProductCatalogStatus::Blocked,
            vec![
                "derived_batch".to_string(),
                "hrrr_derived_batch".to_string(),
            ],
            blocked_support,
        )
    }));

    entries
}

fn build_heavy_entries() -> Vec<ProductCatalogEntry> {
    heavy_product_specs()
        .into_iter()
        .map(|spec| {
            let (runners, support) = match spec.slug.as_str() {
                "ecape8_panel" => {
                    let mut runners = vec!["ecape8_batch".to_string()];
                    let support = built_in_models()
                        .iter()
                        .map(|model| ProductTargetSupport {
                            target: model.id.to_string(),
                            model: Some(model.id),
                            status: ProductTargetStatus::Supported,
                            fetch_mode: None,
                            grib_product: None,
                            blockers: Vec::new(),
                        })
                        .collect::<Vec<_>>();
                    if support
                        .iter()
                        .any(|target| target.model == Some(ModelId::Hrrr))
                    {
                        runners.push("hrrr_batch".to_string());
                        runners.push("hrrr_ecape8".to_string());
                    }
                    (runners, support)
                }
                _ => (
                    vec!["hrrr_batch".to_string()],
                    vec![ProductTargetSupport {
                        target: ModelId::Hrrr.to_string(),
                        model: Some(ModelId::Hrrr),
                        status: ProductTargetStatus::Supported,
                        fetch_mode: None,
                        grib_product: None,
                        blockers: Vec::new(),
                    }],
                ),
            };
            build_catalog_entry(spec, ProductCatalogStatus::Supported, runners, support)
        })
        .collect()
}

fn build_windowed_entries() -> Vec<ProductCatalogEntry> {
    windowed_product_specs()
        .into_iter()
        .map(|spec| {
            build_catalog_entry(
                spec,
                ProductCatalogStatus::Supported,
                vec![
                    "hrrr_windowed_batch".to_string(),
                    "hrrr_non_ecape_hour".to_string(),
                ],
                vec![ProductTargetSupport {
                    target: ModelId::Hrrr.to_string(),
                    model: Some(ModelId::Hrrr),
                    status: ProductTargetStatus::Supported,
                    fetch_mode: None,
                    grib_product: None,
                    blockers: Vec::new(),
                }],
            )
        })
        .collect()
}

fn collapse_entry_status(support: &[ProductTargetSupport]) -> ProductCatalogStatus {
    let supported = support
        .iter()
        .filter(|target| matches!(target.status, ProductTargetStatus::Supported))
        .count();
    if supported == 0 {
        ProductCatalogStatus::Blocked
    } else if supported == support.len() {
        ProductCatalogStatus::Supported
    } else {
        ProductCatalogStatus::Partial
    }
}

fn build_catalog_entry(
    spec: ProductSpec,
    status: ProductCatalogStatus,
    runners: Vec<String>,
    support: Vec<ProductTargetSupport>,
) -> ProductCatalogEntry {
    let experimental = spec.experimental();
    ProductCatalogEntry {
        id: spec.id,
        slug: spec.slug,
        title: spec.title,
        kind: catalog_kind(spec.kind),
        status,
        product_metadata: spec.product_metadata,
        maturity: spec.maturity,
        flags: spec.flags,
        experimental,
        render_style: spec.render_style,
        runners,
        aliases: spec
            .aliases
            .into_iter()
            .map(|alias| ProductCatalogAlias {
                id: alias.id,
                slug: alias.slug,
                title: alias.title,
                note: alias.note,
            })
            .collect(),
        notes: spec.notes,
        support,
    }
}

fn catalog_kind(kind: ProductKind) -> ProductCatalogKind {
    match kind {
        ProductKind::Direct => ProductCatalogKind::Direct,
        ProductKind::Derived => ProductCatalogKind::Derived,
        ProductKind::Bundled => ProductCatalogKind::Heavy,
        ProductKind::Windowed => ProductCatalogKind::Windowed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_catalog_keeps_supported_and_blocked_matrix() {
        let catalog = build_supported_products_catalog();
        let entry = catalog
            .direct
            .iter()
            .find(|entry| entry.slug == "composite_reflectivity_uh")
            .expect("direct catalog should include native reflectivity + UH");
        assert_eq!(
            entry.id,
            ProductId::new(ProductKind::Direct, "composite_reflectivity_uh")
        );
        assert_eq!(entry.status, ProductCatalogStatus::Partial);
        assert!(
            entry.support.iter().any(|target| {
                target.model == Some(ModelId::Hrrr)
                    && matches!(target.status, ProductTargetStatus::Supported)
            }),
            "HRRR should support composite_reflectivity_uh"
        );
        assert!(
            entry.support.iter().any(|target| {
                target.model == Some(ModelId::Gfs)
                    && matches!(target.status, ProductTargetStatus::Blocked)
            }),
            "GFS should still report blockers for composite_reflectivity_uh"
        );
        assert_eq!(entry.maturity, ProductMaturity::Operational);
        assert!(entry.flags.is_empty());
        let provenance = entry
            .product_metadata
            .as_ref()
            .and_then(|metadata| metadata.provenance.as_ref())
            .expect("direct entry should expose typed product provenance");
        assert_eq!(provenance.lineage, rustwx_core::ProductLineage::Direct);
    }

    #[test]
    fn direct_catalog_marks_hrrr_layout_composites_as_panel_products() {
        let catalog = build_supported_products_catalog();
        let cloud_levels = catalog
            .direct
            .iter()
            .find(|entry| entry.slug == "cloud_cover_levels")
            .expect("cloud_cover_levels should stay in the direct lane");
        assert_eq!(
            cloud_levels.render_style.as_deref(),
            Some("solar07_panel_grid")
        );
        assert!(
            cloud_levels
                .notes
                .iter()
                .any(|note| note.contains("composite panel"))
        );
        assert!(cloud_levels.support.iter().any(|target| {
            target.model == Some(ModelId::Hrrr)
                && matches!(target.status, ProductTargetStatus::Supported)
        }));

        let precipitation_type = catalog
            .direct
            .iter()
            .find(|entry| entry.slug == "precipitation_type")
            .expect("precipitation_type should stay in the direct lane");
        assert_eq!(
            precipitation_type.render_style.as_deref(),
            Some("solar07_panel_grid")
        );
        assert!(
            precipitation_type
                .notes
                .iter()
                .any(|note| note.contains("freezing-rain"))
        );
        assert_eq!(precipitation_type.maturity, ProductMaturity::Operational);
        assert!(
            precipitation_type
                .product_metadata
                .as_ref()
                .and_then(|metadata| metadata.provenance.as_ref())
                .expect("direct composite should carry provenance")
                .flags
                .contains(&rustwx_core::ProductSemanticFlag::Composite)
        );
    }

    #[test]
    fn derived_catalog_includes_intentional_blockers() {
        let catalog = build_supported_products_catalog();
        let entry = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "stp_effective")
            .expect("catalog should include blocked stp_effective entry");
        assert_eq!(entry.status, ProductCatalogStatus::Blocked);
        assert_eq!(entry.maturity, ProductMaturity::Operational);
        assert!(entry.flags.is_empty());
        assert_eq!(
            entry
                .product_metadata
                .as_ref()
                .and_then(|metadata| metadata.provenance.as_ref())
                .expect("blocked derived entries should still expose typed provenance")
                .lineage,
            rustwx_core::ProductLineage::Derived
        );
        assert_eq!(entry.support.len(), built_in_models().len());
        assert!(
            entry
                .support
                .iter()
                .all(|target| matches!(target.status, ProductTargetStatus::Blocked))
        );
        assert!(
            entry
                .support
                .iter()
                .flat_map(|target| target.blockers.iter())
                .any(|reason| reason.contains("effective SRH") || reason.contains("EBWD")),
            "blocked derived entries should carry the current blocker text"
        );
    }

    #[test]
    fn derived_catalog_includes_depth_specific_ehi_products() {
        let catalog = build_supported_products_catalog();
        let entry = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "ehi_0_1km")
            .expect("catalog should include supported ehi_0_1km entry");
        assert_eq!(entry.status, ProductCatalogStatus::Supported);
        assert!(!entry.experimental);
        assert_eq!(entry.maturity, ProductMaturity::Operational);
        assert!(entry.notes.iter().any(|note| note.contains("0-1 km SRH")));
        assert_eq!(
            entry
                .product_metadata
                .as_ref()
                .and_then(|metadata| metadata.provenance.as_ref())
                .expect("derived entry should carry provenance")
                .lineage,
            rustwx_core::ProductLineage::Derived
        );
    }

    #[test]
    fn catalog_marks_proxy_and_proof_products_explicitly() {
        let catalog = build_supported_products_catalog();

        let proxy = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "scp_mu_0_3km_0_6km_proxy")
            .expect("catalog should expose proxy SCP entry");
        assert_eq!(proxy.maturity, ProductMaturity::Experimental);
        assert!(proxy.experimental);
        assert!(proxy.flags.contains(&ProductSemanticFlag::Proxy));

        let proof = catalog
            .heavy
            .iter()
            .find(|entry| entry.slug == "severe_proof_panel")
            .expect("catalog should expose proof heavy panel");
        assert_eq!(
            proof.id,
            ProductId::new(ProductKind::Bundled, "severe_proof_panel")
        );
        assert_eq!(proof.maturity, ProductMaturity::Proof);
        assert!(proof.experimental);
        assert!(proof.flags.contains(&ProductSemanticFlag::ProofOriented));
        assert!(proof.flags.contains(&ProductSemanticFlag::Proxy));
        assert_eq!(
            proof
                .product_metadata
                .as_ref()
                .and_then(|metadata| metadata.provenance.as_ref())
                .expect("heavy proof entry should expose provenance")
                .lineage,
            rustwx_core::ProductLineage::Bundled
        );
    }

    #[test]
    fn ecape_catalog_entry_is_supported_for_all_built_in_models() {
        let catalog = build_supported_products_catalog();
        let ecape = catalog
            .heavy
            .iter()
            .find(|entry| entry.slug == "ecape8_panel")
            .expect("catalog should expose ecape8 panel entry");
        assert_eq!(ecape.title, "ECAPE 8-Panel");
        assert!(ecape.runners.iter().any(|runner| runner == "ecape8_batch"));
        assert_eq!(ecape.support.len(), built_in_models().len());
        assert!(
            ecape
                .support
                .iter()
                .all(|target| matches!(target.status, ProductTargetStatus::Supported))
        );
    }

    #[test]
    fn catalog_reroutes_legacy_surface_aliases_into_derived_lane() {
        let catalog = build_supported_products_catalog();
        for slug in ["2m_theta_e_10m_winds", "2m_heat_index", "2m_wind_chill"] {
            assert!(
                catalog.direct.iter().all(|entry| entry.slug != slug),
                "{slug} should not remain in the direct/native lane"
            );
        }

        let theta_e = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "theta_e_2m_10m_winds")
            .expect("catalog should expose canonical theta-e product");
        assert_eq!(
            theta_e.id,
            ProductId::new(ProductKind::Derived, "theta_e_2m_10m_winds")
        );
        assert!(
            theta_e
                .aliases
                .iter()
                .any(|alias| alias.slug == "2m_theta_e_10m_winds")
        );
        assert!(
            theta_e
                .aliases
                .iter()
                .any(|alias| alias.id
                    == ProductId::new(ProductKind::Derived, "2m_theta_e_10m_winds"))
        );
        assert!(
            theta_e
                .notes
                .iter()
                .any(|note| note.contains("derived lane"))
        );
        let identity = theta_e
            .product_metadata
            .as_ref()
            .and_then(|metadata| metadata.identity.as_ref())
            .expect("catalog entry should expose canonical identity");
        assert_eq!(identity.canonical, theta_e.id);
        assert!(
            identity
                .alias_slugs
                .contains(&"2m_theta_e_10m_winds".to_string())
        );

        let heat_index = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "heat_index_2m")
            .expect("catalog should expose canonical heat index product");
        assert!(
            heat_index
                .aliases
                .iter()
                .any(|alias| alias.slug == "2m_heat_index")
        );

        let wind_chill = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "wind_chill_2m")
            .expect("catalog should expose canonical wind chill product");
        assert!(
            wind_chill
                .aliases
                .iter()
                .any(|alias| alias.slug == "2m_wind_chill")
        );
    }

    #[test]
    fn catalog_tracks_legacy_one_hour_qpf_name_in_windowed_lane() {
        let catalog = build_supported_products_catalog();
        assert!(
            catalog.direct.iter().all(|entry| entry.slug != "1h_qpf"),
            "1h_qpf should not remain in the direct/native lane"
        );
        let qpf_1h = catalog
            .windowed
            .iter()
            .find(|entry| entry.slug == "qpf_1h")
            .expect("catalog should expose canonical 1-hour QPF windowed product");
        assert_eq!(qpf_1h.id, ProductId::new(ProductKind::Windowed, "qpf_1h"));
        assert!(qpf_1h.aliases.iter().any(|alias| alias.slug == "1h_qpf"));
        assert!(
            qpf_1h
                .aliases
                .iter()
                .any(|alias| alias.id == ProductId::new(ProductKind::Windowed, "1h_qpf"))
        );
        assert!(
            qpf_1h
                .notes
                .iter()
                .any(|note| note.contains("windowed lane")),
            "catalog notes should keep 1h_qpf routed into the windowed story"
        );
        assert_eq!(
            qpf_1h
                .product_metadata
                .as_ref()
                .and_then(|metadata| metadata.provenance.as_ref())
                .expect("windowed entry should expose typed provenance")
                .window,
            Some(rustwx_core::ProductWindowSpec {
                process: rustwx_core::StatisticalProcess::Accumulation,
                duration_hours: Some(1),
            })
        );
        assert_eq!(
            qpf_1h
                .product_metadata
                .as_ref()
                .and_then(|metadata| metadata.identity.as_ref())
                .expect("windowed entry should expose canonical identity")
                .canonical,
            qpf_1h.id
        );
    }

    #[test]
    fn windowed_catalog_marks_hr_rr_windowed_products_supported() {
        let catalog = build_supported_products_catalog();
        assert_eq!(catalog.windowed.len(), 8);
        assert!(
            catalog
                .windowed
                .iter()
                .all(|entry| entry.status == ProductCatalogStatus::Supported)
        );
        assert!(
            catalog
                .windowed
                .iter()
                .all(|entry| entry.maturity == ProductMaturity::Operational)
        );
        assert!(catalog.windowed.iter().any(|entry| {
            entry.slug == "qpf_6h"
                && entry
                    .runners
                    .iter()
                    .any(|runner| runner == "hrrr_non_ecape_hour")
                && entry.support[0].blockers.is_empty()
        }));
    }

    #[test]
    fn summary_counts_proxy_and_proof_entries() {
        let catalog = build_supported_products_catalog();
        assert!(catalog.summary.experimental_entries >= 1);
        assert!(catalog.summary.proof_entries >= 2);
        assert!(catalog.summary.proxy_entries >= 2);
    }
}
