use numpy::PyReadonlyArray2;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::cross_section::normalize_cross_section_request_impl;
use super::projection::{
    extract_lat_lon_arrays, extract_projected_render_arrays, geometry_description,
    projected_overlay_description, projection_description,
};
use super::render::{render_projected_map_impl, serialize_pretty};
use super::spec::{ProjectedSurfaceSpec, ProjectionSpec, RenderSpec};

fn json_to_python(py: Python<'_>, payload: &str) -> PyResult<Py<PyAny>> {
    let json_module = pyo3::types::PyModule::import(py, "json")?;
    Ok(json_module.getattr("loads")?.call1((payload,))?.unbind())
}

fn python_value_to_json(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(text) = value.extract::<String>() {
        return Ok(text);
    }
    let json_module = pyo3::types::PyModule::import(value.py(), "json")?;
    json_module
        .getattr("dumps")?
        .call1((value,))?
        .extract::<String>()
}

fn parse_json_str<T: DeserializeOwned>(payload: &str, label: &str) -> PyResult<T> {
    serde_json::from_str(payload)
        .map_err(|error| PyValueError::new_err(format!("Invalid {label}: {error}")))
}

fn parse_json_like<T: DeserializeOwned>(value: &Bound<'_, PyAny>, label: &str) -> PyResult<T> {
    parse_json_str(&python_value_to_json(value)?, label)
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectionSpecHolder {
    projection: ProjectionSpec,
}

fn parse_projection_str(payload: &str) -> PyResult<ProjectionSpec> {
    serde_json::from_str(payload)
        .or_else(|_| {
            serde_json::from_str::<ProjectionSpecHolder>(payload).map(|holder| holder.projection)
        })
        .map_err(|error| PyValueError::new_err(format!("Invalid projection spec: {error}")))
}

fn parse_projection_like(value: &Bound<'_, PyAny>) -> PyResult<ProjectionSpec> {
    parse_projection_str(&python_value_to_json(value)?)
}

#[pyfunction]
#[pyo3(signature = (spec_json))]
pub fn describe_projected_projection_json(spec_json: &str) -> PyResult<String> {
    serialize_pretty(&projection_description(&parse_projection_str(spec_json)?)?)
}

#[pyfunction]
#[pyo3(signature = (spec))]
pub fn describe_projected_projection(
    py: Python<'_>,
    spec: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    json_to_python(
        py,
        &serialize_pretty(&projection_description(&parse_projection_like(spec)?)?)?,
    )
}

#[pyfunction]
#[pyo3(signature = (spec_json, lat, lon, *, include_projected_domain=false))]
pub fn describe_projected_geometry_json(
    spec_json: &str,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    include_projected_domain: bool,
) -> PyResult<String> {
    let spec: ProjectedSurfaceSpec = parse_json_str(spec_json, "projected surface spec")?;
    let (lat, lon) = extract_lat_lon_arrays(lat, lon)?;
    serialize_pretty(&geometry_description(
        &spec,
        &lat,
        &lon,
        include_projected_domain,
    )?)
}

#[pyfunction]
#[pyo3(signature = (spec, lat, lon, *, include_projected_domain=false))]
pub fn describe_projected_geometry(
    py: Python<'_>,
    spec: &Bound<'_, PyAny>,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    include_projected_domain: bool,
) -> PyResult<Py<PyAny>> {
    let spec: ProjectedSurfaceSpec = parse_json_like(spec, "projected surface spec")?;
    let (lat, lon) = extract_lat_lon_arrays(lat, lon)?;
    json_to_python(
        py,
        &serialize_pretty(&geometry_description(
            &spec,
            &lat,
            &lon,
            include_projected_domain,
        )?)?,
    )
}

#[pyfunction]
#[pyo3(signature = (spec_json, lat, lon, *, include_geometry=true))]
pub fn build_projected_basemap_overlays_json(
    spec_json: &str,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    include_geometry: bool,
) -> PyResult<String> {
    let spec: ProjectedSurfaceSpec = parse_json_str(spec_json, "projected surface spec")?;
    let (lat, lon) = extract_lat_lon_arrays(lat, lon)?;
    serialize_pretty(&projected_overlay_description(
        &spec,
        &lat,
        &lon,
        include_geometry,
    )?)
}

#[pyfunction]
#[pyo3(signature = (spec, lat, lon, *, include_geometry=true))]
pub fn build_projected_basemap_overlays(
    py: Python<'_>,
    spec: &Bound<'_, PyAny>,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    include_geometry: bool,
) -> PyResult<Py<PyAny>> {
    let spec: ProjectedSurfaceSpec = parse_json_like(spec, "projected surface spec")?;
    let (lat, lon) = extract_lat_lon_arrays(lat, lon)?;
    json_to_python(
        py,
        &serialize_pretty(&projected_overlay_description(
            &spec,
            &lat,
            &lon,
            include_geometry,
        )?)?,
    )
}

#[pyfunction]
#[pyo3(signature = (spec_json))]
pub fn normalize_cross_section_request_json(spec_json: &str) -> PyResult<String> {
    serialize_pretty(&normalize_cross_section_request_impl(parse_json_str(
        spec_json,
        "cross-section request spec",
    )?)?)
}

#[pyfunction]
#[pyo3(signature = (spec))]
pub fn normalize_cross_section_request(
    py: Python<'_>,
    spec: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    json_to_python(
        py,
        &serialize_pretty(&normalize_cross_section_request_impl(parse_json_like(
            spec,
            "cross-section request spec",
        )?)?)?,
    )
}

#[pyfunction]
#[pyo3(signature = (spec_json, lat, lon, field, contour_field=None, overlay_field=None, wind_u=None, wind_v=None))]
pub fn render_projected_map_json(
    spec_json: &str,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<String> {
    let spec: RenderSpec = parse_json_str(spec_json, "projected map render spec")?;
    let arrays = extract_projected_render_arrays(
        lat,
        lon,
        field,
        contour_field,
        overlay_field,
        wind_u,
        wind_v,
    )?;
    serialize_pretty(&render_projected_map_impl(&spec, arrays)?)
}

#[pyfunction]
#[pyo3(signature = (spec, lat, lon, field, contour_field=None, overlay_field=None, wind_u=None, wind_v=None))]
pub fn render_projected_map(
    py: Python<'_>,
    spec: &Bound<'_, PyAny>,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<Py<PyAny>> {
    let spec: RenderSpec = parse_json_like(spec, "projected map render spec")?;
    let arrays = extract_projected_render_arrays(
        lat,
        lon,
        field,
        contour_field,
        overlay_field,
        wind_u,
        wind_v,
    )?;
    json_to_python(
        py,
        &serialize_pretty(&render_projected_map_impl(&spec, arrays)?)?,
    )
}

#[pyfunction]
#[pyo3(signature = (spec_json, lat, lon, field, contour_field=None, overlay_field=None, wind_u=None, wind_v=None))]
pub fn render_wrf_map_json(
    spec_json: &str,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<String> {
    render_projected_map_json(
        spec_json,
        lat,
        lon,
        field,
        contour_field,
        overlay_field,
        wind_u,
        wind_v,
    )
}

#[pyfunction]
#[pyo3(signature = (spec, lat, lon, field, contour_field=None, overlay_field=None, wind_u=None, wind_v=None))]
pub fn render_wrf_map(
    py: Python<'_>,
    spec: &Bound<'_, PyAny>,
    lat: PyReadonlyArray2<'_, f64>,
    lon: PyReadonlyArray2<'_, f64>,
    field: PyReadonlyArray2<'_, f64>,
    contour_field: Option<PyReadonlyArray2<'_, f64>>,
    overlay_field: Option<PyReadonlyArray2<'_, f64>>,
    wind_u: Option<PyReadonlyArray2<'_, f64>>,
    wind_v: Option<PyReadonlyArray2<'_, f64>>,
) -> PyResult<Py<PyAny>> {
    render_projected_map(
        py,
        spec,
        lat,
        lon,
        field,
        contour_field,
        overlay_field,
        wind_u,
        wind_v,
    )
}
