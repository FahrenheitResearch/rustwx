use rustwx_core::ModelId;
use rustwx_models::{
    built_in_models, built_in_plot_recipes, plot_recipe_fetch_blockers, plot_recipe_fetch_plan,
    PlotRecipeFetchMode, RenderStyle,
};
use serde::{Deserialize, Serialize};

use crate::derived::{blocked_derived_recipe_inventory, supported_derived_recipe_inventory};
use crate::hrrr::HrrrBatchProduct;
use crate::windowed::HrrrWindowedProduct;

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
    pub slug: String,
    pub title: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductCatalogEntry {
    pub slug: String,
    pub title: String,
    pub kind: ProductCatalogKind,
    pub status: ProductCatalogStatus,
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

#[derive(Debug, Clone, Copy)]
struct LegacyProductAliasRoute {
    alias_slug: &'static str,
    alias_title: &'static str,
    canonical_slug: &'static str,
    canonical_kind: ProductCatalogKind,
    note: &'static str,
}

const LEGACY_NON_ECAPE_ALIAS_ROUTES: &[LegacyProductAliasRoute] = &[
    LegacyProductAliasRoute {
        alias_slug: "2m_theta_e_10m_winds",
        alias_title: "2m AGL Theta-e / 10m Winds",
        canonical_slug: "theta_e_2m_10m_winds",
        canonical_kind: ProductCatalogKind::Derived,
        note: "Legacy plot-recipe slug from the big list. HRRR support lives in the derived lane, not as a native/direct GRIB recipe.",
    },
    LegacyProductAliasRoute {
        alias_slug: "2m_heat_index",
        alias_title: "2m AGL Heat Index",
        canonical_slug: "heat_index_2m",
        canonical_kind: ProductCatalogKind::Derived,
        note: "Legacy plot-recipe slug from the big list. HRRR support lives in the derived lane, not as a native/direct GRIB recipe.",
    },
    LegacyProductAliasRoute {
        alias_slug: "2m_wind_chill",
        alias_title: "2m AGL Wind Chill",
        canonical_slug: "wind_chill_2m",
        canonical_kind: ProductCatalogKind::Derived,
        note: "Legacy plot-recipe slug from the big list. HRRR support lives in the derived lane, not as a native/direct GRIB recipe.",
    },
    LegacyProductAliasRoute {
        alias_slug: "1h_qpf",
        alias_title: "1h QPF",
        canonical_slug: "qpf_1h",
        canonical_kind: ProductCatalogKind::Windowed,
        note: "Legacy plot-recipe slug from the big list. The honest HRRR implementation is the 1-hour windowed APCP product; alias wiring belongs in the windowed lane rather than a fake native/direct recipe.",
    },
];

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
    };
    for entry in all {
        match entry.status {
            ProductCatalogStatus::Supported => summary.supported_entries += 1,
            ProductCatalogStatus::Partial => summary.partial_entries += 1,
            ProductCatalogStatus::Blocked => summary.blocked_entries += 1,
        }
        if entry.experimental {
            summary.experimental_entries += 1;
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
    built_in_plot_recipes()
        .iter()
        .filter(|recipe| legacy_alias_route_for_direct_slug(recipe.slug).is_none())
        .map(|recipe| {
            let support = built_in_models()
                .iter()
                .map(
                    |model| match plot_recipe_fetch_plan(recipe.slug, model.id) {
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
                            blockers: plot_recipe_fetch_blockers(recipe.slug, model.id)
                                .unwrap_or_default()
                                .into_iter()
                                .map(|blocker| {
                                    format!("{}: {}", blocker.field_label, blocker.reason)
                                })
                                .collect(),
                        },
                    },
                )
                .collect::<Vec<_>>();

            let mut runners = vec!["plot_recipe_proof".to_string()];
            if support.iter().any(|target| {
                target.model == Some(ModelId::Hrrr)
                    && matches!(target.status, ProductTargetStatus::Supported)
            }) {
                runners.push("hrrr_direct_batch".to_string());
                runners.push("hrrr_non_ecape_hour".to_string());
            }

            ProductCatalogEntry {
                slug: recipe.slug.to_string(),
                title: recipe.title.to_string(),
                kind: ProductCatalogKind::Direct,
                status: collapse_entry_status(&support),
                experimental: false,
                render_style: Some(direct_render_style(recipe).to_string()),
                runners,
                aliases: Vec::new(),
                notes: direct_entry_notes(recipe.slug),
                support,
            }
        })
        .collect()
}

fn direct_render_style(recipe: &rustwx_models::PlotRecipe) -> &'static str {
    match recipe.slug {
        "cloud_cover_levels" | "precipitation_type" => "solar07_panel_grid",
        _ => render_style_name(recipe.style),
    }
}

fn direct_entry_notes(slug: &str) -> Vec<String> {
    match slug {
        "cloud_cover_levels" => vec![
            "Rendered as an honest HRRR direct composite panel over low, middle, and high cloud-cover component fields".to_string(),
        ],
        "precipitation_type" => vec![
            "Rendered as an honest HRRR direct composite panel over categorical rain, freezing-rain, ice-pellet, and snow phase flags".to_string(),
        ],
        _ => Vec::new(),
    }
}

fn build_derived_entries() -> Vec<ProductCatalogEntry> {
    let mut entries = supported_derived_recipe_inventory()
        .iter()
        .map(|recipe| ProductCatalogEntry {
            slug: recipe.slug.to_string(),
            title: recipe.title.to_string(),
            kind: ProductCatalogKind::Derived,
            status: ProductCatalogStatus::Supported,
            experimental: recipe.experimental,
            render_style: None,
            runners: vec![
                "hrrr_derived_batch".to_string(),
                "hrrr_non_ecape_hour".to_string(),
            ],
            aliases: legacy_aliases(ProductCatalogKind::Derived, recipe.slug),
            notes: derived_entry_notes(recipe.slug, recipe.experimental),
            support: vec![ProductTargetSupport {
                target: ModelId::Hrrr.to_string(),
                model: Some(ModelId::Hrrr),
                status: ProductTargetStatus::Supported,
                fetch_mode: None,
                grib_product: None,
                blockers: Vec::new(),
            }],
        })
        .collect::<Vec<_>>();

    entries.extend(
        blocked_derived_recipe_inventory()
            .iter()
            .map(|recipe| ProductCatalogEntry {
                slug: recipe.slug.to_string(),
                title: recipe.title.to_string(),
                kind: ProductCatalogKind::Derived,
                status: ProductCatalogStatus::Blocked,
                experimental: false,
                render_style: None,
                runners: vec!["hrrr_derived_batch".to_string()],
                aliases: Vec::new(),
                notes: Vec::new(),
                support: vec![ProductTargetSupport {
                    target: ModelId::Hrrr.to_string(),
                    model: Some(ModelId::Hrrr),
                    status: ProductTargetStatus::Blocked,
                    fetch_mode: None,
                    grib_product: None,
                    blockers: vec![recipe.reason.to_string()],
                }],
            }),
    );

    entries
}

fn derived_entry_notes(slug: &str, experimental: bool) -> Vec<String> {
    let mut notes = Vec::new();
    match slug {
        "ehi_0_1km" => notes.push(
            "Depth-specific EHI using sbCAPE with 0-1 km SRH; not an effective-layer diagnostic"
                .to_string(),
        ),
        "ehi_0_3km" => notes.push(
            "Depth-specific EHI using sbCAPE with 0-3 km SRH; not an effective-layer diagnostic"
                .to_string(),
        ),
        "scp_mu_0_3km_0_6km_proxy" => notes.push(
            "Uses muCAPE with 0-3 km SRH and 0-6 km bulk shear; kept explicit because effective-layer SCP is still blocked"
                .to_string(),
        ),
        _ => {}
    }
    notes.extend(legacy_alias_notes(ProductCatalogKind::Derived, slug));
    if experimental {
        notes.push(
            "Current proof/product runner labels this as a proxy or experimental diagnostic"
                .to_string(),
        );
    }
    notes
}

fn build_heavy_entries() -> Vec<ProductCatalogEntry> {
    [
        HrrrBatchProduct::SevereProofPanel,
        HrrrBatchProduct::Ecape8Panel,
    ]
    .into_iter()
    .map(|product| {
        let (title, experimental, notes) = match product {
            HrrrBatchProduct::SevereProofPanel => (
                "HRRR Severe Proof Panel",
                true,
                vec![
                    "Proof-oriented bundled panel".to_string(),
                    "Keeps fixed-depth SCP proxy diagnostics until effective-layer SRH and EBWD are wired"
                        .to_string(),
                ],
            ),
            HrrrBatchProduct::Ecape8Panel => (
                "HRRR ECAPE 8-Panel",
                true,
                vec![
                    "Proof-oriented bundled panel".to_string(),
                    "Contains experimental ECAPE SCP/EHI fields inside the panel set".to_string(),
                ],
            ),
        };

        ProductCatalogEntry {
            slug: product.slug().to_string(),
            title: title.to_string(),
            kind: ProductCatalogKind::Heavy,
            status: ProductCatalogStatus::Supported,
            experimental,
            render_style: Some("solar07_panel_grid".to_string()),
            runners: vec!["hrrr_batch".to_string()],
            aliases: Vec::new(),
            notes,
            support: vec![ProductTargetSupport {
                target: ModelId::Hrrr.to_string(),
                model: Some(ModelId::Hrrr),
                status: ProductTargetStatus::Supported,
                fetch_mode: None,
                grib_product: None,
                blockers: Vec::new(),
            }],
        }
    })
    .collect()
}

fn build_windowed_entries() -> Vec<ProductCatalogEntry> {
    [
        (
            HrrrWindowedProduct::Qpf1h,
            "1-h APCP accumulation ending at the requested forecast hour",
            "solar07_qpf",
        ),
        (
            HrrrWindowedProduct::Qpf6h,
            "Uses direct 6-hour APCP from the ending hour when present, else sums hourly APCP increments",
            "solar07_qpf",
        ),
        (
            HrrrWindowedProduct::Qpf12h,
            "Uses direct 12-hour APCP from the ending hour when present, else sums hourly APCP increments",
            "solar07_qpf",
        ),
        (
            HrrrWindowedProduct::Qpf24h,
            "Uses direct 24-hour APCP from the ending hour when present, else sums hourly APCP increments",
            "solar07_qpf",
        ),
        (
            HrrrWindowedProduct::QpfTotal,
            "Uses direct APCP from the ending hour when available, else sums all hourly APCP increments from F001..Fend",
            "solar07_qpf",
        ),
        (
            HrrrWindowedProduct::Uh25km1h,
            "Native 2-5 km UH 1-hour max from HRRR wrfnat",
            "solar07_uh",
        ),
        (
            HrrrWindowedProduct::Uh25km3h,
            "Max of trailing native hourly 2-5 km UH maxima",
            "solar07_uh",
        ),
        (
            HrrrWindowedProduct::Uh25kmRunMax,
            "Run max of native hourly 2-5 km UH maxima from F001..Fend",
            "solar07_uh",
        ),
    ]
    .into_iter()
    .map(|(product, note, render_style)| ProductCatalogEntry {
        slug: product.slug().to_string(),
        title: product.title().to_string(),
        kind: ProductCatalogKind::Windowed,
        status: ProductCatalogStatus::Supported,
        experimental: false,
        render_style: Some(render_style.to_string()),
        runners: vec![
            "hrrr_windowed_batch".to_string(),
            "hrrr_non_ecape_hour".to_string(),
        ],
        aliases: legacy_aliases(ProductCatalogKind::Windowed, product.slug()),
        notes: {
            let mut notes = vec![
                note.to_string(),
                "Backed by HRRR statistical time-window metadata surfaced through grib-core"
                    .to_string(),
            ];
            notes.extend(legacy_alias_notes(ProductCatalogKind::Windowed, product.slug()));
            notes
        },
        support: vec![ProductTargetSupport {
            target: ModelId::Hrrr.to_string(),
            model: Some(ModelId::Hrrr),
            status: ProductTargetStatus::Supported,
            fetch_mode: None,
            grib_product: None,
            blockers: Vec::new(),
        }],
    })
    .collect()
}

fn legacy_alias_route_for_direct_slug(slug: &str) -> Option<&'static LegacyProductAliasRoute> {
    LEGACY_NON_ECAPE_ALIAS_ROUTES
        .iter()
        .find(|route| route.alias_slug == slug)
}

fn legacy_aliases(kind: ProductCatalogKind, canonical_slug: &str) -> Vec<ProductCatalogAlias> {
    LEGACY_NON_ECAPE_ALIAS_ROUTES
        .iter()
        .filter(|route| route.canonical_kind == kind && route.canonical_slug == canonical_slug)
        .map(|route| ProductCatalogAlias {
            slug: route.alias_slug.to_string(),
            title: route.alias_title.to_string(),
            note: route.note.to_string(),
        })
        .collect()
}

fn legacy_alias_notes(kind: ProductCatalogKind, canonical_slug: &str) -> Vec<String> {
    LEGACY_NON_ECAPE_ALIAS_ROUTES
        .iter()
        .filter(|route| route.canonical_kind == kind && route.canonical_slug == canonical_slug)
        .map(|route| route.note.to_string())
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

fn render_style_name(style: RenderStyle) -> &'static str {
    match style {
        RenderStyle::Solar07Cape => "solar07_cape",
        RenderStyle::Solar07Cin => "solar07_cin",
        RenderStyle::Solar07Reflectivity => "solar07_reflectivity",
        RenderStyle::Solar07Uh => "solar07_uh",
        RenderStyle::Solar07Temperature => "solar07_temperature",
        RenderStyle::Solar07Dewpoint => "solar07_dewpoint",
        RenderStyle::Solar07Rh => "solar07_rh",
        RenderStyle::Solar07Winds => "solar07_winds",
        RenderStyle::Solar07Height => "solar07_height",
        RenderStyle::Solar07Pressure => "solar07_pressure",
        RenderStyle::Solar07WindGust => "solar07_wind_gust",
        RenderStyle::Solar07CloudCover => "solar07_cloud_cover",
        RenderStyle::Solar07PrecipitableWater => "solar07_precipitable_water",
        RenderStyle::Solar07Qpf => "solar07_qpf",
        RenderStyle::Solar07Categorical => "solar07_categorical",
        RenderStyle::Solar07Visibility => "solar07_visibility",
        RenderStyle::Solar07RadarReflectivity => "solar07_radar_reflectivity",
        RenderStyle::Solar07Satellite => "solar07_satellite",
        RenderStyle::Solar07Lightning => "solar07_lightning",
        RenderStyle::Solar07Vorticity => "solar07_vorticity",
        RenderStyle::Solar07Stp => "solar07_stp",
        RenderStyle::Solar07Scp => "solar07_scp",
        RenderStyle::Solar07Ehi => "solar07_ehi",
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
        assert!(cloud_levels
            .notes
            .iter()
            .any(|note| note.contains("composite panel")));
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
        assert!(precipitation_type
            .notes
            .iter()
            .any(|note| note.contains("freezing-rain")));
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
        assert_eq!(entry.support.len(), 1);
        assert!(
            entry.support[0]
                .blockers
                .iter()
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
        assert!(entry.notes.iter().any(|note| note.contains("0-1 km SRH")));
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
        assert!(theta_e
            .aliases
            .iter()
            .any(|alias| alias.slug == "2m_theta_e_10m_winds"));
        assert!(theta_e
            .notes
            .iter()
            .any(|note| note.contains("derived lane")));

        let heat_index = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "heat_index_2m")
            .expect("catalog should expose canonical heat index product");
        assert!(heat_index
            .aliases
            .iter()
            .any(|alias| alias.slug == "2m_heat_index"));

        let wind_chill = catalog
            .derived
            .iter()
            .find(|entry| entry.slug == "wind_chill_2m")
            .expect("catalog should expose canonical wind chill product");
        assert!(wind_chill
            .aliases
            .iter()
            .any(|alias| alias.slug == "2m_wind_chill"));
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
        assert!(qpf_1h.aliases.iter().any(|alias| alias.slug == "1h_qpf"));
        assert!(
            qpf_1h
                .notes
                .iter()
                .any(|note| note.contains("windowed lane")),
            "catalog notes should keep 1h_qpf routed into the windowed story"
        );
    }

    #[test]
    fn windowed_catalog_marks_hr_rr_windowed_products_supported() {
        let catalog = build_supported_products_catalog();
        assert_eq!(catalog.windowed.len(), 8);
        assert!(catalog
            .windowed
            .iter()
            .all(|entry| entry.status == ProductCatalogStatus::Supported));
        assert!(catalog.windowed.iter().any(|entry| {
            entry.slug == "qpf_6h"
                && entry
                    .runners
                    .iter()
                    .any(|runner| runner == "hrrr_non_ecape_hour")
                && entry.support[0].blockers.is_empty()
        }));
    }
}
