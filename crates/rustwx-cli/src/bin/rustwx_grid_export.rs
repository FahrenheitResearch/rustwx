use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

#[path = "../domain.rs"]
mod domain;
#[path = "../region.rs"]
mod region;

use anyhow::{Context, Result, bail};
use clap::Parser;
use domain::{domain_from_region_or_country, requested_domain_slug};
use region::RegionPreset;
use rustwx_core::{ModelId, SourceId};
use rustwx_models::{model_summary, supported_forecast_hours};
use rustwx_products::shared_context::DomainSpec;
use rustwx_products::windowed::HrrrWindowedProduct;
use rustwx_products::wxstore_export::{
    WxStoreGridExportReport, WxStoreGridExportRequest, WxStoreGridExportTiming,
    default_wxstore_export_product_slugs, export_wxstore_grid_bundle,
};

#[derive(Debug, Parser)]
#[command(
    name = "rustwx-grid-export",
    about = "Export rustwx product grids as WxStore-importable f32 manifests"
)]
struct Args {
    #[arg(long, default_value = "gfs")]
    model: ModelId,
    #[arg(long)]
    date: String,
    #[arg(long)]
    cycle: Option<u8>,
    #[arg(
        long,
        alias = "hours",
        default_value = "0",
        help = "Forecast hour list/ranges, e.g. 0,3,6 or 0-3"
    )]
    forecast_hour: String,
    #[arg(long)]
    source: Option<SourceId>,
    #[arg(long, value_enum, default_value_t = RegionPreset::Conus)]
    region: RegionPreset,
    #[arg(
        long,
        help = "Country crop by ISO alpha-2/alpha-3 code or normalized country name"
    )]
    country: Option<String>,
    #[arg(
        long,
        allow_hyphen_values = true,
        help = "Override region/country bounds as west,east,south,north"
    )]
    bounds: Option<String>,
    #[arg(long, help = "Slug used when --bounds overrides the named region")]
    domain_slug: Option<String>,
    #[arg(long = "product", value_delimiter = ',', num_args = 0..)]
    products: Vec<String>,
    #[arg(long, default_value = "H:\\weather-api-proof\\rustwx_grid_exports")]
    out_dir: PathBuf,
    #[arg(long, default_value = "H:\\weather-api-proof\\rustwx_cache")]
    cache_dir: PathBuf,
    #[arg(
        long,
        help = "Write native WxStore .wxa files directly under this spatial root, skipping f32 sidecars/import."
    )]
    direct_wxa_root: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "When --direct-wxa-root is set, publish this run as the model latest pointer."
    )]
    publish_wxa_latest: bool,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(
        long,
        default_value_t = 1,
        help = "Export hour chunks concurrently. Use a bounded value such as 4-8 on a server."
    )]
    jobs: usize,
    #[arg(
        long,
        help = "Forecast hours per worker chunk. Defaults to enough chunks to keep --jobs busy."
    )]
    hour_chunk_size: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let forecast_hours = parse_hours_for_model(args.model, args.cycle, &args.forecast_hour)?;
    let products = if args.products.is_empty() {
        default_wxstore_export_product_slugs(args.model)
    } else {
        args.products.clone()
    };
    if products.is_empty() {
        bail!(
            "no WxStore grid-export products are supported for model {} after ECAPE/heavy filtering",
            args.model
        );
    }
    let source = args
        .source
        .unwrap_or(model_summary(args.model).sources[0].id);
    let domain = if let Some(bounds) = args.bounds.as_deref() {
        DomainSpec::new(
            args.domain_slug
                .clone()
                .unwrap_or_else(|| requested_domain_slug(args.region, args.country.as_deref())),
            parse_bounds(bounds)?,
        )
    } else {
        domain_from_region_or_country(args.region, args.country.as_deref())
            .map_err(|err| anyhow::anyhow!(err.to_string()))?
    };

    let request = WxStoreGridExportRequest {
        model: args.model,
        date_yyyymmdd: args.date,
        cycle_override_utc: args.cycle,
        forecast_hours,
        source,
        domain,
        product_slugs: products,
        out_dir: args.out_dir,
        cache_root: args.cache_dir,
        use_cache: !args.no_cache,
        direct_wxa_root: args.direct_wxa_root,
        publish_wxa_latest: args.publish_wxa_latest,
    };
    let direct_wxa = request.direct_wxa_root.is_some();
    let report = if !direct_wxa
        && args.jobs > 1
        && request.forecast_hours.len() > 1
        && request.model == ModelId::Hrrr
        && hrrr_windowed_product_partition(&request.product_slugs)
            .0
            .len()
            > 0
    {
        export_parallel_hrrr_split_windowed(&request, args.jobs, args.hour_chunk_size)?
    } else if !direct_wxa && args.jobs > 1 && request.forecast_hours.len() > 1 {
        export_parallel_hour_chunks(&request, args.jobs, args.hour_chunk_size)?
    } else {
        export_wxstore_grid_bundle(&request).map_err(|err| anyhow::anyhow!(err.to_string()))?
    };
    println!("{}", report.manifest_path.display());
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema": report.schema,
            "model": report.model,
            "run_id": report.run_id,
            "requested_product_count": request.product_slugs.len(),
            "field_count": report.fields.len(),
            "blocker_count": report.blockers.len(),
            "manifest_path": report.manifest_path,
            "timing": report.timing
        }))?
    );
    Ok(())
}

fn export_parallel_hrrr_split_windowed(
    request: &WxStoreGridExportRequest,
    jobs: usize,
    hour_chunk_size: Option<usize>,
) -> Result<WxStoreGridExportReport> {
    let started = Instant::now();
    let (windowed_products, non_windowed_products) =
        hrrr_windowed_product_partition(&request.product_slugs);
    let mut reports = Vec::new();

    if !non_windowed_products.is_empty() {
        let mut non_windowed_request = request.clone();
        non_windowed_request.product_slugs = non_windowed_products;
        reports.push(export_parallel_hour_chunks(
            &non_windowed_request,
            jobs,
            hour_chunk_size,
        )?);
    }

    if !windowed_products.is_empty() {
        let mut windowed_request = request.clone();
        windowed_request.product_slugs = windowed_products;
        reports.push(
            export_wxstore_grid_bundle(&windowed_request)
                .map_err(|err| anyhow::anyhow!(err.to_string()))?,
        );
    }

    combine_parallel_reports(request, reports, started.elapsed().as_millis())
}

fn hrrr_windowed_product_partition(product_slugs: &[String]) -> (Vec<String>, Vec<String>) {
    let windowed_slugs = HrrrWindowedProduct::supported_products()
        .iter()
        .map(|product| product.slug())
        .collect::<std::collections::BTreeSet<_>>();
    let mut windowed = Vec::new();
    let mut rest = Vec::new();
    for slug in product_slugs {
        if windowed_slugs.contains(slug.as_str()) {
            windowed.push(slug.clone());
        } else {
            rest.push(slug.clone());
        }
    }
    (windowed, rest)
}

fn export_parallel_hour_chunks(
    request: &WxStoreGridExportRequest,
    jobs: usize,
    hour_chunk_size: Option<usize>,
) -> Result<WxStoreGridExportReport> {
    let started = Instant::now();
    let jobs = jobs.max(1).min(request.forecast_hours.len());
    let chunk_size = hour_chunk_size
        .unwrap_or_else(|| request.forecast_hours.len().div_ceil(jobs * 2).max(1))
        .max(1);
    let chunks = request
        .forecast_hours
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect::<VecDeque<_>>();
    let queue = Arc::new(Mutex::new(chunks));
    let reports = Arc::new(Mutex::new(Vec::<WxStoreGridExportReport>::new()));
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut handles = Vec::new();
    for _ in 0..jobs {
        let queue = Arc::clone(&queue);
        let reports = Arc::clone(&reports);
        let errors = Arc::clone(&errors);
        let base_request = request.clone();
        handles.push(thread::spawn(move || {
            loop {
                let Some(chunk) = queue.lock().expect("chunk queue poisoned").pop_front() else {
                    break;
                };
                let mut chunk_request = base_request.clone();
                chunk_request.forecast_hours = chunk;
                match export_wxstore_grid_bundle(&chunk_request) {
                    Ok(report) => reports
                        .lock()
                        .expect("report collection poisoned")
                        .push(report),
                    Err(err) => errors
                        .lock()
                        .expect("error collection poisoned")
                        .push(err.to_string()),
                }
            }
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("parallel export worker panicked"))?;
    }
    let errors = Arc::try_unwrap(errors)
        .unwrap_or_else(|_| Mutex::new(vec!["error collection still referenced".to_string()]))
        .into_inner()
        .unwrap_or_else(|_| vec!["error collection poisoned".to_string()]);
    if !errors.is_empty() {
        bail!("parallel export failed:\n{}", errors.join("\n"));
    }
    let mut reports = Arc::try_unwrap(reports)
        .unwrap_or_else(|_| Mutex::new(Vec::new()))
        .into_inner()
        .map_err(|_| anyhow::anyhow!("report collection poisoned"))?;
    reports.sort_by_key(|report| report.forecast_hours.first().copied().unwrap_or(0));
    combine_parallel_reports(request, reports, started.elapsed().as_millis())
}

fn combine_parallel_reports(
    request: &WxStoreGridExportRequest,
    reports: Vec<WxStoreGridExportReport>,
    wall_ms: u128,
) -> Result<WxStoreGridExportReport> {
    let first = reports
        .first()
        .ok_or_else(|| anyhow::anyhow!("parallel export produced no reports"))?;
    let run_id = first.run_id.clone();
    let model = first.model.clone();
    let member = first.member.clone();
    let date_yyyymmdd = first.date_yyyymmdd.clone();
    let cycle_utc = first.cycle_utc;
    let source = first.source.clone();
    let generated_at = first.generated_at.clone();
    for report in &reports {
        if report.run_id != run_id {
            bail!(
                "parallel chunks resolved different runs: {} and {}",
                run_id,
                report.run_id
            );
        }
    }

    let combined_dir = request.out_dir.join(&run_id).join(format!(
        "{}_{}",
        safe_slug(&request.domain.slug),
        hour_range_slug(&request.forecast_hours)
    ));
    fs::create_dir_all(&combined_dir)
        .with_context(|| format!("create {}", combined_dir.display()))?;
    let manifest_path = combined_dir.join("manifest.json");

    let mut fields = Vec::new();
    let mut compositions = Vec::new();
    let mut blockers = Vec::new();
    let mut load_ms = 0u128;
    let mut write_ms = 0u128;
    let mut load_bundle_ms = 0u128;
    let mut load_direct_ms = 0u128;
    let mut load_derived_ms = 0u128;
    let mut load_windowed_ms = 0u128;
    let mut write_fields_ms = 0u128;
    let mut write_wxa_ms = 0u128;
    for report in reports {
        let chunk_dir = report
            .manifest_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        load_ms += report.timing.load_ms;
        write_ms += report.timing.write_ms;
        load_bundle_ms += report.timing.load_bundle_ms;
        load_direct_ms += report.timing.load_direct_ms;
        load_derived_ms += report.timing.load_derived_ms;
        load_windowed_ms += report.timing.load_windowed_ms;
        write_fields_ms += report.timing.write_fields_ms;
        write_wxa_ms += report.timing.write_wxa_ms;
        blockers.extend(report.blockers);
        compositions.extend(report.compositions);
        fields.extend(report.fields.into_iter().map(|mut field| {
            absolutize_record_paths(&chunk_dir, &mut field);
            field
        }));
    }
    fields.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
    });
    blockers.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
            .then(a.reason.cmp(&b.reason))
    });
    compositions.sort_by(|a, b| {
        a.product_slug
            .cmp(&b.product_slug)
            .then(a.forecast_hour.cmp(&b.forecast_hour))
    });

    let report = WxStoreGridExportReport {
        schema: "rustwx.wxstore_grid_export.v1".to_string(),
        model,
        run_id,
        member,
        date_yyyymmdd,
        cycle_utc,
        source,
        domain: request.domain.clone(),
        forecast_hours: request.forecast_hours.clone(),
        generated_at,
        manifest_path: manifest_path.clone(),
        fields,
        compositions,
        blockers,
        timing: WxStoreGridExportTiming {
            total_ms: wall_ms,
            load_ms,
            write_ms,
            load_bundle_ms,
            load_direct_ms,
            load_derived_ms,
            load_windowed_ms,
            write_fields_ms,
            write_wxa_ms,
        },
    };
    fs::write(&manifest_path, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("write {}", manifest_path.display()))?;
    Ok(report)
}

fn absolutize_record_paths(
    chunk_dir: &Path,
    field: &mut rustwx_products::wxstore_export::WxStoreGridExportRecord,
) {
    if field.values_path.is_relative() {
        field.values_path = chunk_dir.join(&field.values_path);
    }
    if field.lat_path.is_relative() {
        field.lat_path = chunk_dir.join(&field.lat_path);
    }
    if field.lon_path.is_relative() {
        field.lon_path = chunk_dir.join(&field.lon_path);
    }
}

fn safe_slug(value: &str) -> String {
    let slug = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if slug.is_empty() {
        "domain".into()
    } else {
        slug
    }
}

fn hour_range_slug(hours: &[u16]) -> String {
    if let (Some(first), Some(last)) = (hours.first(), hours.last()) {
        if hours.windows(2).all(|pair| pair[1] == pair[0] + 1) {
            if first == last {
                return format!("f{first:03}");
            }
            return format!("f{first:03}_f{last:03}");
        }
        if hours.len() > 2 {
            let step = hours[1].saturating_sub(hours[0]);
            if step > 0 && hours.windows(2).all(|pair| pair[1] == pair[0] + step) {
                return format!("f{first:03}_f{last:03}_step{step:03}");
            }
            if hours.len() > 12 {
                return format!("f{first:03}_f{last:03}_n{}", hours.len());
            }
        }
    }
    hours
        .iter()
        .map(|hour| format!("f{hour:03}"))
        .collect::<Vec<_>>()
        .join("_")
}

fn parse_hours(value: &str) -> Result<Vec<u16>> {
    let mut hours = Vec::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let (range_part, step) = if let Some((range_part, step_part)) = part.split_once('/') {
            let step = step_part
                .parse::<u16>()
                .with_context(|| format!("invalid forecast-hour step in '{part}'"))?;
            if step == 0 {
                bail!("forecast-hour step in '{part}' must be greater than zero");
            }
            (range_part, step)
        } else {
            (part, 1)
        };
        if let Some((start, end)) = range_part
            .split_once('-')
            .or_else(|| range_part.split_once(':'))
        {
            let start = start
                .parse::<u16>()
                .with_context(|| format!("invalid forecast-hour range '{part}'"))?;
            let end = end
                .parse::<u16>()
                .with_context(|| format!("invalid forecast-hour range '{part}'"))?;
            if end < start {
                bail!("forecast-hour range '{part}' is reversed");
            }
            hours.extend((start..=end).step_by(usize::from(step)));
        } else {
            hours.push(
                part.parse::<u16>()
                    .with_context(|| format!("invalid forecast hour '{part}'"))?,
            );
        }
    }
    hours.sort_unstable();
    hours.dedup();
    if hours.is_empty() {
        bail!("at least one forecast hour is required");
    }
    Ok(hours)
}

fn parse_hours_for_model(model: ModelId, cycle: Option<u8>, value: &str) -> Result<Vec<u16>> {
    let normalized = value.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "all" | "model" | "supported") {
        let cycle = cycle
            .with_context(|| "--forecast-hour all requires --cycle so model cadence is known")?;
        let hours = supported_forecast_hours(model, cycle);
        if hours.is_empty() {
            bail!("model {model} cycle {cycle:02}z has no supported forecast hours");
        }
        return Ok(hours);
    }
    parse_hours(value)
}

fn parse_bounds(value: &str) -> Result<(f64, f64, f64, f64)> {
    let parts = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<f64>()
                .with_context(|| format!("invalid bounds coordinate '{part}'"))
        })
        .collect::<Result<Vec<_>>>()?;
    if parts.len() != 4 {
        bail!("--bounds expects west,east,south,north");
    }
    if parts[2] > parts[3] {
        bail!("--bounds south must be <= north");
    }
    Ok((parts[0], parts[1], parts[2], parts[3]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hours_accepts_lists_and_ranges() {
        assert_eq!(parse_hours("0,2-4,4").unwrap(), vec![0, 2, 3, 4]);
        assert_eq!(parse_hours("0-12/3").unwrap(), vec![0, 3, 6, 9, 12]);
    }

    #[test]
    fn parse_hours_for_model_accepts_supported_keyword() {
        let hours = parse_hours_for_model(ModelId::Gefs, Some(12), "all").unwrap();
        assert!(hours.contains(&240));
        assert!(!hours.contains(&243));
        assert!(hours.contains(&384));
    }

    #[test]
    fn parse_bounds_requires_four_values() {
        assert_eq!(
            parse_bounds("-125,-66,24,50").unwrap(),
            (-125.0, -66.0, 24.0, 50.0)
        );
        assert!(parse_bounds("-125,-66,24").is_err());
    }
}
