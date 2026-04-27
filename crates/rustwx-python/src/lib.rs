pub fn python_bindings_enabled() -> bool {
    cfg!(feature = "python")
}

#[cfg(feature = "python")]
mod wrf_render;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};
#[cfg(feature = "python")]
use rustwx_io::{FetchRequest, available_forecast_hours, probe_sources};
#[cfg(feature = "python")]
use rustwx_sounding::{SoundingColumn, write_full_sounding_png};
#[cfg(feature = "python")]
use serde::Serialize;
#[cfg(feature = "python")]
use std::path::Path;
#[cfg(feature = "python")]
use wrf_render::{
    build_projected_basemap_overlays, build_projected_basemap_overlays_json,
    describe_projected_geometry, describe_projected_geometry_json, describe_projected_projection,
    describe_projected_projection_json, normalize_cross_section_request,
    normalize_cross_section_request_json, render_projected_map, render_projected_map_json,
    render_wrf_map, render_wrf_map_json,
};

#[cfg(feature = "python")]
#[pyfunction]
fn workspace_name() -> &'static str {
    "rustwx"
}

#[cfg(feature = "python")]
#[pyfunction]
fn list_models_json() -> PyResult<String> {
    serde_json::to_string_pretty(rustwx_models::built_in_models())
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, hour, forecast_hour, product=None))]
fn resolve_urls_json(
    model: &str,
    date: &str,
    hour: u8,
    forecast_hour: u16,
    product: Option<&str>,
) -> PyResult<String> {
    let model: ModelId = model.parse().map_err(|err: rustwx_core::RustwxError| {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    })?;
    let cycle = CycleSpec::new(date, hour)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let default_product = rustwx_models::model_summary(model).default_product;
    let request = ModelRunRequest::new(
        model,
        cycle,
        forecast_hour,
        product.unwrap_or(default_product),
    )
    .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let urls = rustwx_models::resolve_urls(&request)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&urls)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, source=None))]
fn latest_run_json(model: &str, date: &str, source: Option<&str>) -> PyResult<String> {
    let model: ModelId = parse_model(model)?;
    let source = parse_optional_source(source)?;
    let latest = rustwx_models::latest_available_run(model, source, date)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&latest)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, hour, product=None, source=None))]
fn available_forecast_hours_json(
    model: &str,
    date: &str,
    hour: u8,
    product: Option<&str>,
    source: Option<&str>,
) -> PyResult<String> {
    let model: ModelId = parse_model(model)?;
    let source = parse_optional_source(source)?;
    let product = resolve_product(model, product);
    let hours = available_forecast_hours(model, date, hour, &product, source)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&hours)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (model, date, hour, forecast_hour, product=None, source=None, variable_patterns=None))]
fn probe_sources_json(
    model: &str,
    date: &str,
    hour: u8,
    forecast_hour: u16,
    product: Option<&str>,
    source: Option<&str>,
    variable_patterns: Option<Vec<String>>,
) -> PyResult<String> {
    let model: ModelId = parse_model(model)?;
    let source = parse_optional_source(source)?;
    let product = resolve_product(model, product);
    let cycle = CycleSpec::new(date, hour)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let request = ModelRunRequest::new(model, cycle, forecast_hour, product)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let fetch_request = FetchRequest {
        request,
        source_override: source,
        variable_patterns: variable_patterns.unwrap_or_default(),
    };
    let probe = probe_sources(&fetch_request)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    serde_json::to_string_pretty(&probe)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[derive(Debug, Serialize)]
struct SoundingRenderResult {
    renderer: &'static str,
    output_path: String,
    levels: usize,
    station_id: String,
    valid_time: String,
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (column_json, output_path))]
fn render_sounding_column_json(column_json: &str, output_path: &str) -> PyResult<String> {
    let result =
        render_sounding_column_impl(parse_sounding_column_json(column_json)?, output_path)?;
    serde_json::to_string_pretty(&result)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (column, output_path))]
fn render_sounding_column(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    output_path: &str,
) -> PyResult<Py<PyAny>> {
    let result = render_sounding_column_impl(
        parse_sounding_column_json(&python_value_to_json(column)?)?,
        output_path,
    )?;
    json_to_python(
        py,
        &serde_json::to_string_pretty(&result)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?,
    )
}

#[cfg(feature = "python")]
fn render_sounding_column_impl(
    column: SoundingColumn,
    output_path: &str,
) -> PyResult<SoundingRenderResult> {
    if let Some(parent) = Path::new(output_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    }
    write_full_sounding_png(&column, output_path)
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    Ok(SoundingRenderResult {
        renderer: "rustwx-sounding native Rust SHARPpy-style renderer",
        output_path: output_path.to_string(),
        levels: column.len(),
        station_id: column.metadata.station_id,
        valid_time: column.metadata.valid_time,
    })
}

#[cfg(feature = "python")]
fn parse_sounding_column_json(payload: &str) -> PyResult<SoundingColumn> {
    serde_json::from_str(payload).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid sounding column: {err}"))
    })
}

#[cfg(feature = "python")]
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

#[cfg(feature = "python")]
fn json_to_python(py: Python<'_>, payload: &str) -> PyResult<Py<PyAny>> {
    let json_module = pyo3::types::PyModule::import(py, "json")?;
    Ok(json_module.getattr("loads")?.call1((payload,))?.unbind())
}

#[cfg(feature = "python")]
fn parse_model(model: &str) -> PyResult<ModelId> {
    model.parse().map_err(|err: rustwx_core::RustwxError| {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    })
}

#[cfg(feature = "python")]
fn parse_optional_source(source: Option<&str>) -> PyResult<Option<SourceId>> {
    source
        .map(|value| {
            value.parse().map_err(|err: rustwx_core::RustwxError| {
                pyo3::exceptions::PyValueError::new_err(err.to_string())
            })
        })
        .transpose()
}

#[cfg(feature = "python")]
fn resolve_product(model: ModelId, product: Option<&str>) -> String {
    product
        .unwrap_or(rustwx_models::model_summary(model).default_product)
        .to_string()
}

#[cfg(feature = "python")]
#[pymodule]
fn rustwx(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(workspace_name, module)?)?;
    module.add_function(wrap_pyfunction!(list_models_json, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_urls_json, module)?)?;
    module.add_function(wrap_pyfunction!(latest_run_json, module)?)?;
    module.add_function(wrap_pyfunction!(available_forecast_hours_json, module)?)?;
    module.add_function(wrap_pyfunction!(probe_sources_json, module)?)?;
    module.add_function(wrap_pyfunction!(
        describe_projected_projection_json,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(describe_projected_projection, module)?)?;
    module.add_function(wrap_pyfunction!(describe_projected_geometry_json, module)?)?;
    module.add_function(wrap_pyfunction!(describe_projected_geometry, module)?)?;
    module.add_function(wrap_pyfunction!(
        build_projected_basemap_overlays_json,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(build_projected_basemap_overlays, module)?)?;
    module.add_function(wrap_pyfunction!(
        normalize_cross_section_request_json,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(normalize_cross_section_request, module)?)?;
    module.add_function(wrap_pyfunction!(render_projected_map, module)?)?;
    module.add_function(wrap_pyfunction!(render_projected_map_json, module)?)?;
    module.add_function(wrap_pyfunction!(render_wrf_map, module)?)?;
    module.add_function(wrap_pyfunction!(render_wrf_map_json, module)?)?;
    module.add_function(wrap_pyfunction!(render_sounding_column, module)?)?;
    module.add_function(wrap_pyfunction!(render_sounding_column_json, module)?)?;
    Ok(())
}
