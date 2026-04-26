use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Serialize;

use super::projection::{DEG_TO_RAD, WRF_EARTH_RADIUS_M};
use super::spec::{CrossSectionPointSpec, CrossSectionRequestSpec, CrossSectionVerticalCoordinate};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CrossSectionPathMetrics {
    pub(crate) sample_count: usize,
    pub(crate) great_circle_km: f64,
    pub(crate) initial_bearing_deg: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NormalizedCrossSectionRequest {
    pub(crate) kind: &'static str,
    pub(crate) schema_version: u8,
    pub(crate) status: &'static str,
    pub(crate) request: CrossSectionRequestSpec,
    pub(crate) path_metrics: CrossSectionPathMetrics,
}

fn validate_lat_lon_point(point: &CrossSectionPointSpec, label: &str) -> PyResult<()> {
    if !(-90.0..=90.0).contains(&point.lat) {
        return Err(PyValueError::new_err(format!(
            "{label}.lat must be between -90 and 90 degrees"
        )));
    }
    if !(-180.0..=180.0).contains(&point.lon) {
        return Err(PyValueError::new_err(format!(
            "{label}.lon must be between -180 and 180 degrees"
        )));
    }
    Ok(())
}

fn ensure_non_empty(value: &str, label: &str) -> PyResult<()> {
    if value.trim().is_empty() {
        return Err(PyValueError::new_err(format!("{label} must not be empty")));
    }
    Ok(())
}

fn haversine_distance_km(start: &CrossSectionPointSpec, end: &CrossSectionPointSpec) -> f64 {
    let lat1 = start.lat * DEG_TO_RAD;
    let lat2 = end.lat * DEG_TO_RAD;
    let dlat = (end.lat - start.lat) * DEG_TO_RAD;
    let dlon = (end.lon - start.lon) * DEG_TO_RAD;
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    (WRF_EARTH_RADIUS_M / 1000.0) * c
}

fn initial_bearing_deg(start: &CrossSectionPointSpec, end: &CrossSectionPointSpec) -> f64 {
    let lat1 = start.lat * DEG_TO_RAD;
    let lat2 = end.lat * DEG_TO_RAD;
    let dlon = (end.lon - start.lon) * DEG_TO_RAD;
    let y = dlon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    y.atan2(x).to_degrees().rem_euclid(360.0)
}

pub(crate) fn normalize_cross_section_request_impl(
    mut spec: CrossSectionRequestSpec,
) -> PyResult<NormalizedCrossSectionRequest> {
    validate_lat_lon_point(&spec.path.start, "path.start")?;
    validate_lat_lon_point(&spec.path.end, "path.end")?;
    if (spec.path.start.lat - spec.path.end.lat).abs() < 1.0e-9
        && (spec.path.start.lon - spec.path.end.lon).abs() < 1.0e-9
    {
        return Err(PyValueError::new_err(
            "path.start and path.end must describe two distinct points",
        ));
    }

    ensure_non_empty(&spec.field.product_key, "field.product_key")?;
    for (index, contour) in spec.contours.iter().enumerate() {
        ensure_non_empty(
            &contour.product_key,
            &format!("contours[{index}].product_key"),
        )?;
        if contour.levels.is_empty() {
            return Err(PyValueError::new_err(format!(
                "contours[{index}].levels must not be empty"
            )));
        }
    }
    if let Some(ref overlay) = spec.overlay {
        ensure_non_empty(&overlay.product_key, "overlay.product_key")?;
    }
    if let Some(ref wind) = spec.wind {
        ensure_non_empty(&wind.u_product_key, "wind.u_product_key")?;
        ensure_non_empty(&wind.v_product_key, "wind.v_product_key")?;
    }

    let sample_count = spec.path.sample_count.unwrap_or(200);
    if sample_count < 2 {
        return Err(PyValueError::new_err(
            "path.sample_count must be at least 2",
        ));
    }
    spec.path.sample_count = Some(sample_count);

    let (default_bottom, default_top, default_units) = match spec.axis.coordinate {
        CrossSectionVerticalCoordinate::Pressure => (1000.0, 100.0, "hPa"),
        CrossSectionVerticalCoordinate::Height => (0.0, 12000.0, "m"),
        CrossSectionVerticalCoordinate::Altitude => (0.0, 12000.0, "m"),
    };
    spec.axis.bottom.get_or_insert(default_bottom);
    spec.axis.top.get_or_insert(default_top);
    spec.axis
        .units
        .get_or_insert_with(|| default_units.to_string());
    let vertical_scale = spec.axis.vertical_scale.get_or_insert(1.0);
    if *vertical_scale <= 0.0 {
        return Err(PyValueError::new_err(
            "axis.vertical_scale must be greater than zero",
        ));
    }
    match spec.axis.coordinate {
        CrossSectionVerticalCoordinate::Pressure => {
            if spec.axis.top.unwrap_or(default_top) >= spec.axis.bottom.unwrap_or(default_bottom) {
                return Err(PyValueError::new_err(
                    "pressure cross-sections require axis.top < axis.bottom",
                ));
            }
        }
        CrossSectionVerticalCoordinate::Height | CrossSectionVerticalCoordinate::Altitude => {
            if spec.axis.top.unwrap_or(default_top) <= spec.axis.bottom.unwrap_or(default_bottom) {
                return Err(PyValueError::new_err(
                    "height/altitude cross-sections require axis.top > axis.bottom",
                ));
            }
        }
    }

    spec.render.width.get_or_insert(1400);
    spec.render.height.get_or_insert(900);

    Ok(NormalizedCrossSectionRequest {
        kind: "cross_section_request",
        schema_version: 1,
        status: "validated_only",
        path_metrics: CrossSectionPathMetrics {
            sample_count,
            great_circle_km: haversine_distance_km(&spec.path.start, &spec.path.end),
            initial_bearing_deg: initial_bearing_deg(&spec.path.start, &spec.path.end),
        },
        request: spec,
    })
}

#[cfg(test)]
mod tests {
    use super::normalize_cross_section_request_impl;
    use crate::wrf_render::spec::{
        CrossSectionFieldSpec, CrossSectionPathSpec, CrossSectionPointSpec, CrossSectionRequestSpec,
    };

    fn base_request() -> CrossSectionRequestSpec {
        CrossSectionRequestSpec {
            path: CrossSectionPathSpec {
                start: CrossSectionPointSpec {
                    lat: 39.74,
                    lon: -104.99,
                    label: Some("Denver".to_string()),
                },
                end: CrossSectionPointSpec {
                    lat: 41.88,
                    lon: -87.63,
                    label: Some("Chicago".to_string()),
                },
                sample_count: None,
            },
            field: CrossSectionFieldSpec {
                product_key: "temperature".to_string(),
                field_units: Some("degC".to_string()),
                scale: None,
            },
            contours: Vec::new(),
            overlay: None,
            wind: None,
            axis: Default::default(),
            render: Default::default(),
        }
    }

    #[test]
    fn normalize_cross_section_applies_defaults() {
        let normalized = normalize_cross_section_request_impl(base_request()).unwrap();

        assert_eq!(normalized.path_metrics.sample_count, 200);
        assert_eq!(normalized.request.axis.units.as_deref(), Some("hPa"));
        assert_eq!(normalized.request.render.width, Some(1400));
        assert_eq!(normalized.request.render.height, Some(900));
    }

    #[test]
    fn normalize_cross_section_rejects_degenerate_path() {
        pyo3::prepare_freethreaded_python();
        let mut request = base_request();
        request.path.end.lat = request.path.start.lat;
        request.path.end.lon = request.path.start.lon;

        let error = normalize_cross_section_request_impl(request).unwrap_err();
        assert!(error.to_string().contains("two distinct points"));
    }
}
