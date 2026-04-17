//! Decode + compute kernel for windowed products.
//!
//! This module owns the GRIB2 message decode for APCP and native UH
//! fields as well as the per-product window-compute kernels (QPF and
//! UH). It is deliberately separated from the batch orchestration in
//! [`crate::windowed`] so non-HRRR windowed products can plug in later
//! without dragging the HRRR-specific runner along.
//!
//! The orchestrator in `windowed.rs` fetches bytes through the planner
//! + runtime and then hands them here. Everything in this module is
//! pure given bytes (plus the cache path when the caller opts in) — it
//! does no I/O of its own beyond the optional bincode cache.
use crate::cache::{load_bincode, store_bincode};
use crate::windowed::{HrrrWindowedProduct, HrrrWindowedProductMetadata};
use grib_core::grib2::{unpack_message_normalized, Grib2File, Grib2Message};
use rustwx_calc::{max_window_fields, sum_window_fields};
use rustwx_core::{Field2D, ProductKey};
use rustwx_render::{
    palette_scale, ColorScale, ExtendMode, Solar07Palette, Solar07Product,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

const MM_PER_INCH: f64 = 25.4;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WindowedFieldRecord {
    pub(crate) hours: u16,
    pub(crate) values: Vec<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HrrrApcpDecode {
    pub(crate) windows: Vec<WindowedFieldRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HrrrUhDecode {
    pub(crate) windows: Vec<WindowedFieldRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct ComputedWindowedField {
    pub(crate) field: Field2D,
    pub(crate) title: String,
    pub(crate) metadata: HrrrWindowedProductMetadata,
    pub(crate) scale: ColorScale,
}

pub(crate) fn load_or_decode_apcp(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<HrrrApcpDecode, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<HrrrApcpDecode>(path)? {
            return Ok(cached);
        }
    }
    let decoded = decode_apcp(bytes)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok(decoded)
}

pub(crate) fn load_or_decode_uh25(
    path: &Path,
    bytes: &[u8],
    use_cache: bool,
) -> Result<HrrrUhDecode, Box<dyn std::error::Error>> {
    if use_cache {
        if let Some(cached) = load_bincode::<HrrrUhDecode>(path)? {
            return Ok(cached);
        }
    }
    let decoded = decode_uh25(bytes)?;
    if use_cache {
        store_bincode(path, &decoded)?;
    }
    Ok(decoded)
}

pub(crate) fn decode_apcp(bytes: &[u8]) -> Result<HrrrApcpDecode, Box<dyn std::error::Error>> {
    let grib = Grib2File::from_bytes(bytes)?;
    let mut windows = Vec::new();
    for message in &grib.messages {
        if message.discipline == 0
            && message.product.parameter_category == 1
            && message.product.parameter_number == 8
            && message.product.level_type == 1
        {
            let hours = time_range_hours(message)
                .ok_or("APCP message missing hourly time-range metadata")?;
            if windows
                .iter()
                .any(|record: &WindowedFieldRecord| record.hours == hours)
            {
                continue;
            }
            windows.push(WindowedFieldRecord {
                hours,
                values: unpack_message_normalized(message)?,
            });
        }
    }
    if windows.is_empty() {
        return Err("no APCP surface accumulation fields were found in subset".into());
    }
    windows.sort_by_key(|record| record.hours);
    Ok(HrrrApcpDecode { windows })
}

pub(crate) fn decode_uh25(bytes: &[u8]) -> Result<HrrrUhDecode, Box<dyn std::error::Error>> {
    let grib = Grib2File::from_bytes(bytes)?;
    let mut windows = Vec::new();
    for message in &grib.messages {
        if is_uh25_message(message) {
            let hours = time_range_hours(message)
                .ok_or("native UH message missing hourly max-window metadata")?;
            if windows
                .iter()
                .any(|record: &WindowedFieldRecord| record.hours == hours)
            {
                continue;
            }
            windows.push(WindowedFieldRecord {
                hours,
                values: unpack_message_normalized(message)?,
            });
        }
    }
    if windows.is_empty() {
        return Err("no native 2-5 km UH max fields were found in subset".into());
    }
    windows.sort_by_key(|record| record.hours);
    Ok(HrrrUhDecode { windows })
}

pub(crate) fn is_uh25_message(message: &Grib2Message) -> bool {
    matches!(
        (
            message.product.parameter_category,
            message.product.parameter_number
        ),
        (7, 199) | (7, 15)
    ) && matches!(message.product.level_type, 103 | 118)
        && (message.product.level_value - 5000.0).abs() < 0.25
}

pub(crate) fn time_range_hours(message: &Grib2Message) -> Option<u16> {
    message.product.statistical_time_range_hours()
}

pub(crate) fn compute_qpf_product(
    product: HrrrWindowedProduct,
    forecast_hour: u16,
    grid: &rustwx_core::LatLonGrid,
    apcp_by_hour: &BTreeMap<u16, Result<HrrrApcpDecode, String>>,
) -> Result<ComputedWindowedField, String> {
    let (window_hours, title) = match product {
        HrrrWindowedProduct::Qpf1h => (Some(1), "1-h QPF"),
        HrrrWindowedProduct::Qpf6h => (Some(6), "6-h QPF"),
        HrrrWindowedProduct::Qpf12h => (Some(12), "12-h QPF"),
        HrrrWindowedProduct::Qpf24h => (Some(24), "24-h QPF"),
        HrrrWindowedProduct::QpfTotal => (None, "Total QPF"),
        _ => return Err(format!("{} is not a QPF product", product.slug())),
    };

    let (values_mm, strategy, contributing_hours) = match window_hours {
        Some(window) => {
            let end = apcp_by_hour
                .get(&forecast_hour)
                .ok_or_else(|| format!("missing APCP fetch for F{:03}", forecast_hour))?
                .as_ref()
                .map_err(Clone::clone)?;
            if let Some(direct) = select_window(&end.windows, window) {
                (
                    direct.to_vec(),
                    format!("direct APCP {}h accumulation", window),
                    vec![forecast_hour],
                )
            } else {
                let start_hour = forecast_hour + 1 - window;
                let hours = (start_hour..=forecast_hour).collect::<Vec<_>>();
                let increments = collect_apcp_windows(apcp_by_hour, &hours, 1)?;
                (
                    sum_window_fields(grid.shape, &increments).map_err(|err| err.to_string())?,
                    format!("sum of {} hourly APCP increments", window),
                    hours,
                )
            }
        }
        None => {
            let end = apcp_by_hour
                .get(&forecast_hour)
                .ok_or_else(|| format!("missing APCP fetch for F{:03}", forecast_hour))?
                .as_ref()
                .map_err(Clone::clone)?;
            if let Some(direct) = select_window(&end.windows, forecast_hour) {
                (
                    direct.to_vec(),
                    format!("direct APCP {}h accumulation", forecast_hour),
                    vec![forecast_hour],
                )
            } else {
                let hours = (1..=forecast_hour).collect::<Vec<_>>();
                let increments = collect_apcp_windows(apcp_by_hour, &hours, 1)?;
                (
                    sum_window_fields(grid.shape, &increments).map_err(|err| err.to_string())?,
                    "sum of all available hourly APCP increments".to_string(),
                    hours,
                )
            }
        }
    };

    let values_in = values_mm
        .into_iter()
        .map(|value| value / MM_PER_INCH)
        .collect::<Vec<_>>();
    let field = Field2D::new(
        ProductKey::named(product.slug()),
        "in",
        grid.clone(),
        values_in.iter().map(|&value| value as f32).collect(),
    )
    .map_err(|err| err.to_string())?;

    Ok(ComputedWindowedField {
        field,
        title: title.to_string(),
        metadata: HrrrWindowedProductMetadata {
            strategy,
            contributing_forecast_hours: contributing_hours,
            window_hours,
        },
        scale: ColorScale::Discrete(qpf_scale()),
    })
}

pub(crate) fn compute_uh_product(
    product: HrrrWindowedProduct,
    forecast_hour: u16,
    grid: &rustwx_core::LatLonGrid,
    uh_by_hour: &BTreeMap<u16, Result<HrrrUhDecode, String>>,
) -> Result<ComputedWindowedField, String> {
    let (values, strategy, contributing_hours, window_hours) = match product {
        HrrrWindowedProduct::Uh25km1h => {
            let decoded = uh_by_hour
                .get(&forecast_hour)
                .ok_or_else(|| format!("missing native UH fetch for F{:03}", forecast_hour))?
                .as_ref()
                .map_err(Clone::clone)?;
            let values = select_window(&decoded.windows, 1)
                .ok_or_else(|| format!("native UH F{:03} missing 1-hour max field", forecast_hour))?
                .to_vec();
            (
                values,
                "direct native 1-hour UH max".to_string(),
                vec![forecast_hour],
                Some(1),
            )
        }
        HrrrWindowedProduct::Uh25km3h => {
            let hours = ((forecast_hour - 2)..=forecast_hour).collect::<Vec<_>>();
            let windows = collect_uh_windows(uh_by_hour, &hours, 1)?;
            (
                max_window_fields(grid.shape, &windows).map_err(|err| err.to_string())?,
                "max of native hourly UH maxima across trailing 3 hours".to_string(),
                hours,
                Some(3),
            )
        }
        HrrrWindowedProduct::Uh25kmRunMax => {
            let hours = (1..=forecast_hour).collect::<Vec<_>>();
            let windows = collect_uh_windows(uh_by_hour, &hours, 1)?;
            (
                max_window_fields(grid.shape, &windows).map_err(|err| err.to_string())?,
                "run max of native hourly UH maxima".to_string(),
                hours,
                None,
            )
        }
        _ => return Err(format!("{} is not a UH product", product.slug())),
    };

    let field = Field2D::new(
        ProductKey::named(product.slug()),
        "m^2/s^2",
        grid.clone(),
        values.iter().map(|&value| value as f32).collect(),
    )
    .map_err(|err| err.to_string())?;

    Ok(ComputedWindowedField {
        field,
        title: product.title().to_string(),
        metadata: HrrrWindowedProductMetadata {
            strategy,
            contributing_forecast_hours: contributing_hours,
            window_hours,
        },
        scale: ColorScale::Solar07(Solar07Product::Uh.scale_preset()),
    })
}

pub(crate) fn collect_apcp_windows<'a>(
    apcp_by_hour: &'a BTreeMap<u16, Result<HrrrApcpDecode, String>>,
    hours: &[u16],
    window_hours: u16,
) -> Result<Vec<&'a [f64]>, String> {
    let mut out = Vec::with_capacity(hours.len());
    for &hour in hours {
        let decoded = apcp_by_hour
            .get(&hour)
            .ok_or_else(|| format!("missing APCP fetch for F{:03}", hour))?
            .as_ref()
            .map_err(Clone::clone)?;
        let window = select_window(&decoded.windows, window_hours).ok_or_else(|| {
            format!(
                "APCP F{:03} missing {}-hour accumulation field",
                hour, window_hours
            )
        })?;
        out.push(window);
    }
    Ok(out)
}

pub(crate) fn collect_uh_windows<'a>(
    uh_by_hour: &'a BTreeMap<u16, Result<HrrrUhDecode, String>>,
    hours: &[u16],
    window_hours: u16,
) -> Result<Vec<&'a [f64]>, String> {
    let mut out = Vec::with_capacity(hours.len());
    for &hour in hours {
        let decoded = uh_by_hour
            .get(&hour)
            .ok_or_else(|| format!("missing native UH fetch for F{:03}", hour))?
            .as_ref()
            .map_err(Clone::clone)?;
        let window = select_window(&decoded.windows, window_hours).ok_or_else(|| {
            format!(
                "native UH F{:03} missing {}-hour max field",
                hour, window_hours
            )
        })?;
        out.push(window);
    }
    Ok(out)
}

pub(crate) fn select_window(records: &[WindowedFieldRecord], hours: u16) -> Option<&[f64]> {
    records
        .iter()
        .find(|record| record.hours == hours)
        .map(|record| record.values.as_slice())
}

pub(crate) fn qpf_scale() -> rustwx_render::DiscreteColorScale {
    palette_scale(
        Solar07Palette::Precip,
        vec![
            0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0,
        ],
        ExtendMode::Max,
        Some(0.01),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::{GridShape, LatLonGrid};

    fn tiny_grid() -> LatLonGrid {
        LatLonGrid::new(
            GridShape::new(2, 1).unwrap(),
            vec![40.0, 40.0],
            vec![-100.0, -99.0],
        )
        .unwrap()
    }

    #[test]
    fn compute_qpf_prefers_direct_window_when_available() {
        let mut apcp = BTreeMap::new();
        apcp.insert(
            6,
            Ok(HrrrApcpDecode {
                windows: vec![
                    WindowedFieldRecord {
                        hours: 1,
                        values: vec![0.5, 0.25],
                    },
                    WindowedFieldRecord {
                        hours: 6,
                        values: vec![12.7, 25.4],
                    },
                ],
            }),
        );
        let computed =
            compute_qpf_product(HrrrWindowedProduct::Qpf6h, 6, &tiny_grid(), &apcp).unwrap();
        assert_eq!(computed.metadata.strategy, "direct APCP 6h accumulation");
        assert_eq!(computed.metadata.contributing_forecast_hours, vec![6]);
        assert_eq!(computed.field.values, vec![0.5_f32, 1.0_f32]);
    }

    #[test]
    fn compute_qpf_window_blocks_when_a_contributing_hour_is_missing() {
        // Partial-success regression: if the planner couldn't fetch one
        // hour inside a windowed QPF product's window, the compute
        // kernel must emit a blocker for *that* product — not abort the
        // whole windowed lane. The windowed lane's loader inserts
        // Err(reason) for missing hours; compute_qpf_product surfaces
        // the reason through the normal per-product blocker path.
        let mut apcp = BTreeMap::new();
        // Hour 1 and 3 loaded fine; hour 2 failed upstream.
        for hour in [1u16, 3u16] {
            apcp.insert(
                hour,
                Ok(HrrrApcpDecode {
                    windows: vec![WindowedFieldRecord {
                        hours: 1,
                        values: vec![25.4, 12.7],
                    }],
                }),
            );
        }
        apcp.insert(
            2,
            Err("hour 2 fetch failed: 404 Not Found".to_string()),
        );

        // Qpf24h hitting forecast_hour 3 would want hours 1..=3 — the
        // missing hour 2 has to blocker this product. Use QpfTotal
        // (covers 1..=forecast_hour) which is more representative.
        let err = compute_qpf_product(HrrrWindowedProduct::QpfTotal, 3, &tiny_grid(), &apcp)
            .expect_err("compute must surface the missing-hour failure as a blocker");
        assert!(
            err.contains("hour 2") || err.contains("404"),
            "blocker should reference the missing hour or its upstream reason; got: {err}"
        );

        // Meanwhile a 1-hour QPF at forecast_hour 3 needs only hour 3 —
        // the missing hour 2 doesn't block it, and the product still
        // renders.
        let ok = compute_qpf_product(HrrrWindowedProduct::Qpf1h, 3, &tiny_grid(), &apcp)
            .expect("Qpf1h at f003 should render despite an unrelated missing hour");
        assert_eq!(ok.metadata.contributing_forecast_hours, vec![3]);
    }

    #[test]
    fn compute_qpf_total_falls_back_to_hourly_sum() {
        let mut apcp = BTreeMap::new();
        for hour in 1..=3 {
            apcp.insert(
                hour,
                Ok(HrrrApcpDecode {
                    windows: vec![WindowedFieldRecord {
                        hours: 1,
                        values: vec![25.4, 12.7],
                    }],
                }),
            );
        }
        let computed =
            compute_qpf_product(HrrrWindowedProduct::QpfTotal, 3, &tiny_grid(), &apcp).unwrap();
        assert_eq!(
            computed.metadata.strategy,
            "sum of all available hourly APCP increments"
        );
        assert_eq!(computed.field.values, vec![3.0_f32, 1.5_f32]);
    }

    #[test]
    fn compute_uh_run_max_takes_pointwise_maximum() {
        let mut uh = BTreeMap::new();
        uh.insert(
            1,
            Ok(HrrrUhDecode {
                windows: vec![WindowedFieldRecord {
                    hours: 1,
                    values: vec![50.0, 10.0],
                }],
            }),
        );
        uh.insert(
            2,
            Ok(HrrrUhDecode {
                windows: vec![WindowedFieldRecord {
                    hours: 1,
                    values: vec![25.0, 40.0],
                }],
            }),
        );
        let computed =
            compute_uh_product(HrrrWindowedProduct::Uh25kmRunMax, 2, &tiny_grid(), &uh).unwrap();
        assert_eq!(computed.field.values, vec![50.0_f32, 40.0_f32]);
        assert_eq!(
            computed.metadata.strategy,
            "run max of native hourly UH maxima"
        );
    }
}
