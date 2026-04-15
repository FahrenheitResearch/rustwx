pub fn python_bindings_enabled() -> bool {
    cfg!(feature = "python")
}

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest, SourceId};
#[cfg(feature = "python")]
use rustwx_io::{FetchRequest, available_forecast_hours, probe_sources};

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
fn rustwx_python(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(workspace_name, module)?)?;
    module.add_function(wrap_pyfunction!(list_models_json, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_urls_json, module)?)?;
    module.add_function(wrap_pyfunction!(latest_run_json, module)?)?;
    module.add_function(wrap_pyfunction!(available_forecast_hours_json, module)?)?;
    module.add_function(wrap_pyfunction!(probe_sources_json, module)?)?;
    Ok(())
}
