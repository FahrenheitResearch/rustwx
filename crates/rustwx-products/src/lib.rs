pub mod artifact_bundle;
pub mod cache;
pub mod catalog;
pub mod comparison;
pub mod cross_section;
pub mod custom_poi;
pub mod dataset_export;
pub mod derived;
pub mod direct;
pub mod ecape;
pub mod gallery;
pub mod gridded;
pub mod heavy;
pub mod hrrr;
pub mod intelligence;
pub mod named_geometry;
pub mod non_ecape;
pub mod orchestrator;
pub mod places;
pub mod planner;
pub mod publication;
pub mod publication_provenance;
pub mod runtime;
pub mod sampling;
pub mod severe;
pub mod shared_context;
pub mod source;
pub mod spec;
pub mod thermo_native;
pub mod windowed;
pub mod windowed_decoder;

pub use named_geometry::{
    NamedGeoBounds, NamedGeoPoint, NamedGeometry, NamedGeometryAsset, NamedGeometryCatalog,
    NamedGeometryKind, NamedGeometrySelector,
};
pub use shared_context::{
    DomainSpec, PreparedProjectedContext, ProjectedMap, ProjectedMapProvider, WeatherPanelField,
    WeatherPanelHeader, WeatherPanelLayout, layout_key, render_two_by_four_weather_panel,
};

pub(crate) fn apply_place_label_overlay_with_density_styling(
    render_request: &mut rustwx_render::MapRenderRequest,
    overlay: &crate::places::PlaceLabelOverlay,
    domain: &crate::shared_context::DomainSpec,
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projection: Option<&rustwx_core::GridProjection>,
) -> Result<(), Box<dyn std::error::Error>> {
    let selected = overlay.selected_places_for_domain(domain);
    let start = render_request.projected_place_labels.len();
    crate::places::apply_place_label_overlay(
        render_request,
        overlay,
        domain,
        grid_lat_deg,
        grid_lon_deg,
        projection,
    )?;
    style_added_place_labels_for_density(render_request, overlay, &selected, start);
    Ok(())
}

fn style_added_place_labels_for_density(
    render_request: &mut rustwx_render::MapRenderRequest,
    overlay: &crate::places::PlaceLabelOverlay,
    selected: &[crate::places::SelectedPlace],
    start: usize,
) {
    let aux_total = selected
        .iter()
        .filter(|place| !is_major_catalog_place(place.slug.as_str()))
        .count();
    let auxiliary_budget = if aux_total <= 2 {
        aux_total
    } else {
        aux_total.div_ceil(2).min(4)
    };
    let mut aux_seen = 0usize;

    for (place, label) in selected
        .iter()
        .zip(render_request.projected_place_labels.iter_mut().skip(start))
    {
        let is_auxiliary_catalog_place = !is_major_catalog_place(place.slug.as_str());
        label.priority = match overlay.density {
            crate::places::PlaceLabelDensityTier::None
            | crate::places::PlaceLabelDensityTier::Major => {
                rustwx_render::ProjectedPlaceLabelPriority::Primary
            }
            crate::places::PlaceLabelDensityTier::MajorAndAux => {
                if is_auxiliary_catalog_place {
                    rustwx_render::ProjectedPlaceLabelPriority::Auxiliary
                } else {
                    rustwx_render::ProjectedPlaceLabelPriority::Primary
                }
            }
            crate::places::PlaceLabelDensityTier::Dense => {
                if !is_auxiliary_catalog_place {
                    rustwx_render::ProjectedPlaceLabelPriority::Primary
                } else if aux_seen < auxiliary_budget {
                    rustwx_render::ProjectedPlaceLabelPriority::Auxiliary
                } else {
                    rustwx_render::ProjectedPlaceLabelPriority::Micro
                }
            }
        };

        if is_auxiliary_catalog_place {
            aux_seen = aux_seen.saturating_add(1);
        }
    }
}

fn is_major_catalog_place(slug: &str) -> bool {
    crate::places::major_us_city_places()
        .iter()
        .any(|preset| preset.slug == slug)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::places::{PlaceLabelDensityTier, PlaceLabelOverlay};

    const CALIFORNIA_SQUARE: (f64, f64, f64, f64) = (-124.9, -113.7, 31.8, 42.7);

    fn sample_place_label_request() -> rustwx_render::MapRenderRequest {
        let grid = rustwx_render::LatLonGrid::new(
            rustwx_render::GridShape::new(2, 2).unwrap(),
            vec![31.8, 31.8, 42.7, 42.7],
            vec![-124.9, -113.7, -124.9, -113.7],
        )
        .unwrap();
        let field = rustwx_render::Field2D::new(
            rustwx_render::ProductKey::named("place_label_density_style_test"),
            "unitless",
            grid,
            vec![0.0, 0.0, 0.0, 0.0],
        )
        .unwrap();
        let mut request = rustwx_render::MapRenderRequest::contour_only(field);
        request.projected_domain = Some(rustwx_render::ProjectedDomain {
            x: vec![0.0, 1.0, 0.0, 1.0],
            y: vec![0.0, 0.0, 1.0, 1.0],
            extent: rustwx_render::ProjectedExtent {
                x_min: 0.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 1.0,
            },
        });
        request
    }

    #[test]
    fn major_and_aux_place_label_glue_marks_auxiliary_catalog_entries_as_auxiliary() {
        let overlay = PlaceLabelOverlay::major_us_cities()
            .with_density(PlaceLabelDensityTier::MajorAndAux)
            .with_included_place_slugs([
                "ca_los_angeles",
                "ca_san_diego",
                "ca_bakersfield",
                "ca_santa_barbara",
            ]);
        let domain = DomainSpec::new("california_square", CALIFORNIA_SQUARE);
        let selected = overlay.selected_places_for_domain(&domain);

        assert!(
            selected
                .iter()
                .any(|place| !is_major_catalog_place(place.slug.as_str()))
        );

        let mut request = sample_place_label_request();
        let grid_lat_deg = request.field.grid.lat_deg.clone();
        let grid_lon_deg = request.field.grid.lon_deg.clone();
        apply_place_label_overlay_with_density_styling(
            &mut request,
            &overlay,
            &domain,
            &grid_lat_deg,
            &grid_lon_deg,
            None,
        )
        .expect("overlay should project");

        assert_eq!(request.projected_place_labels.len(), selected.len());
        for (place, label) in selected.iter().zip(request.projected_place_labels.iter()) {
            let expected = if is_major_catalog_place(place.slug.as_str()) {
                rustwx_render::ProjectedPlaceLabelPriority::Primary
            } else {
                rustwx_render::ProjectedPlaceLabelPriority::Auxiliary
            };
            assert_eq!(label.priority, expected);
        }
    }

    #[test]
    fn dense_place_label_glue_pushes_lower_rank_auxiliary_entries_to_micro() {
        let overlay = PlaceLabelOverlay::major_us_cities()
            .with_density(PlaceLabelDensityTier::Dense)
            .with_included_place_slugs([
                "ca_los_angeles",
                "ca_san_diego",
                "ca_bakersfield",
                "ca_fresno",
                "ca_san_jose",
                "ca_santa_barbara",
                "ca_san_luis_obispo",
            ]);
        let domain = DomainSpec::new("california_square", CALIFORNIA_SQUARE);
        let selected = overlay.selected_places_for_domain(&domain);
        let aux_total = selected
            .iter()
            .filter(|place| !is_major_catalog_place(place.slug.as_str()))
            .count();

        assert!(
            aux_total >= 3,
            "test should include enough auxiliary labels"
        );

        let mut request = sample_place_label_request();
        let grid_lat_deg = request.field.grid.lat_deg.clone();
        let grid_lon_deg = request.field.grid.lon_deg.clone();
        apply_place_label_overlay_with_density_styling(
            &mut request,
            &overlay,
            &domain,
            &grid_lat_deg,
            &grid_lon_deg,
            None,
        )
        .expect("overlay should project");

        let micro_count = request
            .projected_place_labels
            .iter()
            .filter(|label| label.priority == rustwx_render::ProjectedPlaceLabelPriority::Micro)
            .count();
        assert!(micro_count > 0);
        assert!(request.projected_place_labels.iter().any(|label| {
            label.priority == rustwx_render::ProjectedPlaceLabelPriority::Auxiliary
        }));
        assert!(
            selected
                .iter()
                .zip(request.projected_place_labels.iter())
                .filter(|(place, _)| is_major_catalog_place(place.slug.as_str()))
                .all(|(_, label)| {
                    label.priority == rustwx_render::ProjectedPlaceLabelPriority::Primary
                })
        );
    }
}
