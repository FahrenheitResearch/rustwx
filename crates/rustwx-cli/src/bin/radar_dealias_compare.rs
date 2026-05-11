use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use clap::{Parser, ValueEnum};
use rustwx_radar::nexrad::{Level2File, RadarProduct, sites};
use rustwx_radar::{
    DealiasAcceptancePolicy, DealiasMethod, DealiasReport, RadarSweepSelection,
    RadarVelocityQcSummary, dealias_velocity_sweep_with_policy, radar_velocity_qc_summary,
    select_sweep_with_product, sweeps_with_product,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Compare NEXRAD velocity dealiasing methods on the same sweep")]
struct Cli {
    #[arg(long)]
    site: String,

    #[arg(long)]
    input: PathBuf,

    #[arg(long, default_value = "vel")]
    product: String,

    #[arg(long)]
    sweep_index: Option<usize>,

    #[arg(long)]
    elevation_deg: Option<f32>,

    #[arg(long, default_value_t = false)]
    all_sweeps: bool,

    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "raw,radial,sweep,staged"
    )]
    methods: Vec<MethodArg>,

    #[arg(long, value_enum, value_delimiter = ',')]
    require_method_pass: Vec<MethodArg>,

    #[arg(long, value_enum)]
    expect_best_method: Option<MethodArg>,

    #[arg(long, value_delimiter = ',')]
    max_method_ms: Vec<String>,

    #[arg(long, value_delimiter = ',')]
    max_total_method_ms: Vec<String>,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long, default_value_t = 0.005)]
    max_velocity_fold_fraction: f64,

    #[arg(long, default_value_t = 200)]
    max_velocity_severe_jumps: usize,

    #[arg(long, default_value_t = 100.0)]
    max_velocity_max_jump_ms: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum MethodArg {
    Raw,
    Radial,
    Sweep,
    Staged,
}

impl MethodArg {
    fn label(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Radial => "radial",
            Self::Sweep => "sweep",
            Self::Staged => "staged",
        }
    }

    fn dealias_method(self) -> Option<DealiasMethod> {
        match self {
            Self::Raw => None,
            Self::Radial => Some(DealiasMethod::RadialContinuity),
            Self::Sweep => Some(DealiasMethod::SweepContinuity),
            Self::Staged => Some(DealiasMethod::StagedContinuity),
        }
    }
}

#[derive(Debug, Serialize)]
struct CompareManifest {
    ok: bool,
    site: String,
    product: String,
    source: String,
    scan_time_utc: String,
    thresholds: CompareThresholds,
    required_method_passes: Vec<String>,
    expected_best_method: Option<String>,
    method_elapsed_limits: Vec<MethodElapsedLimitManifest>,
    method_total_elapsed_limits: Vec<MethodElapsedLimitManifest>,
    sweeps: Vec<SweepComparison>,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CompareThresholds {
    max_velocity_fold_fraction: f64,
    max_velocity_severe_jumps: usize,
    max_velocity_max_jump_ms: f32,
}

#[derive(Debug, Clone, Copy)]
struct MethodElapsedLimit {
    method: MethodArg,
    max_ms: u128,
}

#[derive(Debug, Serialize)]
struct MethodElapsedLimitManifest {
    method: String,
    max_ms: u128,
}

impl MethodElapsedLimitManifest {
    fn from_limit(limit: &MethodElapsedLimit) -> Self {
        Self {
            method: limit.method.label().to_string(),
            max_ms: limit.max_ms,
        }
    }
}

#[derive(Debug, Serialize)]
struct SweepComparison {
    sweep_index: usize,
    elevation_deg: f32,
    best_method: Option<String>,
    methods: Vec<MethodComparison>,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MethodComparison {
    method: String,
    elapsed_ms: u128,
    applied: bool,
    passes_gate: bool,
    velocity_qc: Option<RadarVelocityQcSummary>,
    dealias_qc: Option<DealiasReport>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let site = sites::find_site(&cli.site)
        .ok_or_else(|| anyhow::anyhow!("unknown NEXRAD site {}", cli.site))?;
    let product = parse_product(&cli.product)?;
    if !matches!(
        product.base_product(),
        RadarProduct::Velocity | RadarProduct::SuperResVelocity
    ) {
        anyhow::bail!("radar_dealias_compare requires a velocity product");
    }
    if cli.all_sweeps && (cli.sweep_index.is_some() || cli.elevation_deg.is_some()) {
        anyhow::bail!("use --all-sweeps or a single sweep selector, not both");
    }
    let method_elapsed_limits = parse_method_elapsed_limits(&cli.max_method_ms, "--max-method-ms")?;
    let method_total_elapsed_limits =
        parse_method_elapsed_limits(&cli.max_total_method_ms, "--max-total-method-ms")?;

    let raw = std::fs::read(&cli.input)
        .with_context(|| format!("read Level-II volume {}", cli.input.display()))?;
    let file = Level2File::parse(&raw)?;
    let sweeps = selected_sweeps(&file, product, &cli)?;
    let thresholds = CompareThresholds {
        max_velocity_fold_fraction: cli.max_velocity_fold_fraction,
        max_velocity_severe_jumps: cli.max_velocity_severe_jumps,
        max_velocity_max_jump_ms: cli.max_velocity_max_jump_ms,
    };

    let mut failures = Vec::new();
    let mut sweep_reports = Vec::new();
    for (sweep_index, sweep) in sweeps {
        let mut methods = Vec::new();
        for method in &cli.methods {
            let started = Instant::now();
            let (candidate, report) = match method.dealias_method() {
                Some(dealias_method) => {
                    let (candidate, report) = dealias_velocity_sweep_with_policy(
                        sweep,
                        dealias_method,
                        DealiasAcceptancePolicy::Safe,
                    );
                    (candidate, Some(report))
                }
                None => (sweep.clone(), None),
            };
            let elapsed_ms = started.elapsed().as_millis();
            let velocity_qc = radar_velocity_qc_summary(&candidate, product);
            let passes_gate = velocity_qc
                .as_ref()
                .is_some_and(|qc| velocity_passes_gate(qc, &thresholds));
            methods.push(MethodComparison {
                method: method.label().to_string(),
                elapsed_ms,
                applied: report.as_ref().is_some_and(|report| report.accepted),
                passes_gate,
                velocity_qc,
                dealias_qc: report,
            });
        }

        let best_method = best_method(&methods).map(ToString::to_string);
        let mut sweep_failures = Vec::new();
        if best_method.is_none() {
            sweep_failures.push(format!(
                "sweep {sweep_index} has no method passing velocity thresholds"
            ));
        }
        sweep_failures.extend(required_method_failures(
            sweep_index,
            &methods,
            &cli.require_method_pass,
        ));
        sweep_failures.extend(method_elapsed_failures(
            sweep_index,
            &methods,
            &method_elapsed_limits,
        ));
        if let Some(expected) = cli.expect_best_method {
            if best_method.as_deref() != Some(expected.label()) {
                sweep_failures.push(format!(
                    "sweep {sweep_index} best method was {}, expected {}",
                    best_method.as_deref().unwrap_or("none"),
                    expected.label()
                ));
            }
        }
        failures.extend(sweep_failures.iter().cloned());
        sweep_reports.push(SweepComparison {
            sweep_index,
            elevation_deg: sweep.elevation_angle,
            best_method,
            methods,
            failures: sweep_failures,
        });
    }
    failures.extend(total_method_elapsed_failures(
        &sweep_reports,
        &method_total_elapsed_limits,
    ));

    let manifest = CompareManifest {
        ok: failures.is_empty(),
        site: site.id.to_string(),
        product: product.short_name().to_string(),
        source: cli.input.display().to_string(),
        scan_time_utc: file.timestamp_string(),
        thresholds,
        required_method_passes: cli
            .require_method_pass
            .iter()
            .map(|method| method.label().to_string())
            .collect(),
        expected_best_method: cli
            .expect_best_method
            .map(|method| method.label().to_string()),
        method_elapsed_limits: method_elapsed_limits
            .iter()
            .map(MethodElapsedLimitManifest::from_limit)
            .collect(),
        method_total_elapsed_limits: method_total_elapsed_limits
            .iter()
            .map(MethodElapsedLimitManifest::from_limit)
            .collect(),
        sweeps: sweep_reports,
        failures,
    };

    let json = serde_json::to_vec_pretty(&manifest)?;
    if let Some(output) = cli.output.as_ref() {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output, &json)?;
    }
    println!("{}", String::from_utf8(json)?);

    if manifest.ok {
        Ok(())
    } else {
        anyhow::bail!(
            "radar dealias comparison found {} failure(s)",
            manifest.failures.len()
        )
    }
}

fn selected_sweeps<'a>(
    file: &'a Level2File,
    product: RadarProduct,
    cli: &Cli,
) -> anyhow::Result<Vec<(usize, &'a rustwx_radar::Level2Sweep)>> {
    if cli.all_sweeps {
        let sweeps = sweeps_with_product(file, product);
        if sweeps.is_empty() {
            anyhow::bail!("no sweeps contain {}", product.short_name());
        }
        return Ok(sweeps);
    }

    let selection = match (cli.sweep_index, cli.elevation_deg) {
        (Some(_), Some(_)) => {
            anyhow::bail!("use either --sweep-index or --elevation-deg, not both")
        }
        (Some(index), None) => RadarSweepSelection::Index(index),
        (None, Some(elevation)) => RadarSweepSelection::NearestElevation(elevation),
        (None, None) => RadarSweepSelection::Lowest,
    };

    select_sweep_with_product(file, product, selection)
        .map(|selected| vec![selected])
        .ok_or_else(|| anyhow::anyhow!("no sweep contains {}", product.short_name()))
}

fn parse_method_elapsed_limits(
    values: &[String],
    flag: &str,
) -> anyhow::Result<Vec<MethodElapsedLimit>> {
    values
        .iter()
        .map(|value| parse_method_elapsed_limit(value, flag))
        .collect()
}

fn parse_method_elapsed_limit(value: &str, flag: &str) -> anyhow::Result<MethodElapsedLimit> {
    let Some((method, max_ms)) = value.split_once('=') else {
        anyhow::bail!("{flag} expects METHOD=MS, got {value:?}");
    };
    let method = MethodArg::from_str(method.trim(), true)
        .map_err(|err| anyhow::anyhow!("{flag} has unsupported method {method:?}: {err}"))?;
    let max_ms = max_ms
        .trim()
        .parse::<u128>()
        .with_context(|| format!("{flag} expects an integer millisecond limit in {value:?}"))?;
    if max_ms == 0 {
        anyhow::bail!("{flag} limit must be greater than zero in {value:?}");
    }

    Ok(MethodElapsedLimit { method, max_ms })
}

fn velocity_passes_gate(qc: &RadarVelocityQcSummary, thresholds: &CompareThresholds) -> bool {
    qc.fold_like_jump_fraction <= thresholds.max_velocity_fold_fraction
        && qc.severe_jump_count <= thresholds.max_velocity_severe_jumps
        && qc.max_abs_jump_ms <= thresholds.max_velocity_max_jump_ms
}

fn best_method(methods: &[MethodComparison]) -> Option<&str> {
    methods
        .iter()
        .filter(|method| method.passes_gate)
        .min_by(|a, b| compare_method(a, b))
        .map(|method| method.method.as_str())
}

fn required_method_failures(
    sweep_index: usize,
    methods: &[MethodComparison],
    required_methods: &[MethodArg],
) -> Vec<String> {
    required_methods
        .iter()
        .filter_map(|required| {
            let required_label = required.label();
            let passes = methods
                .iter()
                .any(|method| method.method == required_label && method.passes_gate);
            (!passes).then(|| {
                format!("sweep {sweep_index} required method {required_label} did not pass velocity thresholds")
            })
        })
        .collect()
}

fn method_elapsed_failures(
    sweep_index: usize,
    methods: &[MethodComparison],
    limits: &[MethodElapsedLimit],
) -> Vec<String> {
    limits
        .iter()
        .filter_map(|limit| {
            let label = limit.method.label();
            let method = methods.iter().find(|method| method.method == label)?;
            (method.elapsed_ms > limit.max_ms).then(|| {
                format!(
                    "sweep {sweep_index} method {label} elapsed {}ms exceeds {}ms",
                    method.elapsed_ms, limit.max_ms
                )
            })
        })
        .collect()
}

fn total_method_elapsed_failures(
    sweeps: &[SweepComparison],
    limits: &[MethodElapsedLimit],
) -> Vec<String> {
    limits
        .iter()
        .filter_map(|limit| {
            let label = limit.method.label();
            let total_ms = sweeps
                .iter()
                .flat_map(|sweep| sweep.methods.iter())
                .filter(|method| method.method == label)
                .map(|method| method.elapsed_ms)
                .sum::<u128>();
            (total_ms > limit.max_ms).then(|| {
                format!(
                    "method {label} total elapsed {total_ms}ms exceeds {}ms",
                    limit.max_ms
                )
            })
        })
        .collect()
}

fn compare_method(a: &MethodComparison, b: &MethodComparison) -> std::cmp::Ordering {
    let Some(a_qc) = a.velocity_qc.as_ref() else {
        return std::cmp::Ordering::Greater;
    };
    let Some(b_qc) = b.velocity_qc.as_ref() else {
        return std::cmp::Ordering::Less;
    };

    a_qc.severe_jump_count
        .cmp(&b_qc.severe_jump_count)
        .then_with(|| a_qc.fold_like_jump_count.cmp(&b_qc.fold_like_jump_count))
        .then_with(|| {
            a_qc.max_abs_jump_ms
                .partial_cmp(&b_qc.max_abs_jump_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| a.elapsed_ms.cmp(&b.elapsed_ms))
}

fn parse_product(value: &str) -> anyhow::Result<RadarProduct> {
    match value.trim().to_ascii_lowercase().as_str() {
        "vel" | "velocity" => Ok(RadarProduct::Velocity),
        "srvel" | "srv" | "storm-relative-velocity" => Ok(RadarProduct::StormRelativeVelocity),
        other => anyhow::bail!("unsupported velocity product {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn method(
        method: &str,
        severe: usize,
        fold: usize,
        max_jump: f32,
        elapsed_ms: u128,
    ) -> MethodComparison {
        method_with_pass(method, true, severe, fold, max_jump, elapsed_ms)
    }

    fn method_with_pass(
        method: &str,
        passes_gate: bool,
        severe: usize,
        fold: usize,
        max_jump: f32,
        elapsed_ms: u128,
    ) -> MethodComparison {
        MethodComparison {
            method: method.to_string(),
            elapsed_ms,
            applied: false,
            passes_gate,
            velocity_qc: Some(RadarVelocityQcSummary {
                product: "vel".to_string(),
                nyquist_ms: 25.0,
                finite_gate_count: 100,
                radial_pair_count: 100,
                azimuth_pair_count: 100,
                fold_like_jump_count: fold,
                severe_jump_count: severe,
                fold_like_jump_fraction: fold as f64 / 100.0,
                max_abs_jump_ms: max_jump,
            }),
            dealias_qc: None,
        }
    }

    #[test]
    fn best_method_prefers_severe_then_fold_then_speed() {
        let methods = vec![
            method("raw", 10, 20, 50.0, 1),
            method("sweep", 4, 10, 70.0, 8),
            method("staged", 4, 9, 80.0, 12),
        ];

        assert_eq!(best_method(&methods), Some("staged"));
    }

    #[test]
    fn required_method_failures_require_named_method_to_pass() {
        let methods = vec![
            method_with_pass("raw", true, 10, 20, 50.0, 1),
            method_with_pass("staged", false, 4, 9, 80.0, 12),
        ];

        let failures = required_method_failures(2, &methods, &[MethodArg::Staged]);

        assert_eq!(
            failures,
            vec!["sweep 2 required method staged did not pass velocity thresholds"]
        );
    }

    #[test]
    fn method_elapsed_limits_parse_and_fail_slow_methods() {
        let limits =
            parse_method_elapsed_limits(&["staged=500".to_string()], "--max-method-ms").unwrap();
        let methods = vec![
            method_with_pass("raw", true, 10, 20, 50.0, 1),
            method_with_pass("staged", true, 4, 9, 80.0, 650),
        ];

        let failures = method_elapsed_failures(6, &methods, &limits);

        assert_eq!(
            failures,
            vec!["sweep 6 method staged elapsed 650ms exceeds 500ms"]
        );
    }

    #[test]
    fn total_elapsed_limits_sum_named_method() {
        let limits =
            parse_method_elapsed_limits(&["staged=20".to_string()], "--max-total-method-ms")
                .unwrap();
        let sweeps = vec![
            SweepComparison {
                sweep_index: 1,
                elevation_deg: 0.5,
                best_method: Some("staged".to_string()),
                methods: vec![method_with_pass("staged", true, 4, 9, 80.0, 12)],
                failures: Vec::new(),
            },
            SweepComparison {
                sweep_index: 2,
                elevation_deg: 0.9,
                best_method: Some("staged".to_string()),
                methods: vec![method_with_pass("staged", true, 5, 10, 85.0, 11)],
                failures: Vec::new(),
            },
        ];

        let failures = total_method_elapsed_failures(&sweeps, &limits);

        assert_eq!(
            failures,
            vec!["method staged total elapsed 23ms exceeds 20ms"]
        );
    }
}
