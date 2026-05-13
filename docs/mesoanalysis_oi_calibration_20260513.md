# Surface OI/Kriging Calibration Smoke - 2026-05-13 01Z

This note records the first real HRRR + live-observation smoke for the RustWX
surface OI correction layer.

## Case

- Model: HRRR `20260513 00Z`, forecast hour `1`, valid `2026-05-13T01:00Z`.
- Model source: NOMADS.
- Observations: METAR CONUS, Oklahoma Mesonet, Kansas Mesonet, Nebraska Mesonet,
  NDAWN.
- Observation profile: `surface_meso_conus`, 90 minute freshness gate.
- Holdout: deterministic 10 percent station/time split, seed `20260512`.
- Artifact: `target/surface_mesoanalysis_oi_default_smoke/run_report.json`.
- WxStore-style grid export:
  `target/surface_mesoanalysis_oi_default_smoke/wxstore_grid_export/manifest.json`.

## Default OI Result

Command:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --compare-barnes-baseline --out-dir target\surface_mesoanalysis_oi_default_smoke
```

Runtime:

- OI full-CONUS compute: `1393 ms`.
- Barnes baseline compute: `1773 ms`.
- Same-observation validation samples: `2933`.
- Holdout validation samples: `293`.
- Grid export fields: `15`.

Holdout MAE:

| field | raw HRRR | OI | Barnes |
| --- | ---: | ---: | ---: |
| 2 m temperature C | 0.896 | 0.870 | 0.978 |
| 2 m dewpoint C | 1.572 | 1.454 | 1.692 |
| 10 m wind speed m/s | 1.316 | 1.296 | 1.492 |

Holdout RMSE:

| field | raw HRRR | OI | Barnes |
| --- | ---: | ---: | ---: |
| 2 m temperature C | 1.180 | 1.145 | 1.311 |
| 2 m dewpoint C | 2.624 | 2.514 | 2.739 |
| 10 m wind speed m/s | 1.695 | 1.662 | 1.956 |

Same-observation validation still favors Barnes because Barnes strongly pulls
toward the observations it used to correct the grid. The holdout comparison is
the more meaningful skill check for this use case.

## Calibration Lesson

The initial aggressive OI configuration improved same-observation MAE but
worsened holdout skill and produced wind outliers. Two changes fixed the first
case:

- Limit each local OI correction to the local observed innovation envelope.
- Use conservative OI defaults for high-quality analysis backgrounds:
  exponential covariance, 15 km length scale, and smaller assumed background
  errors.

This should not be treated as universal tuning. It is a first real smoke showing
that the machinery can detect over-correction and choose a safer correction
regime for an analysis-time HRRR background.

## Repeated Holdout Smoke

The CLI also supports repeated deterministic holdouts:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --compare-barnes-baseline --holdout-repeat-count 3 --no-grid-export --out-dir target\surface_mesoanalysis_oi_repeated_holdout_smoke
```

Artifact:
`target/surface_mesoanalysis_oi_repeated_holdout_smoke/run_report.json`.

Three-fold repeated holdout MAE:

| field | raw HRRR | OI | Barnes | OI beats raw folds | OI beats Barnes folds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 m temperature C | 0.954 | 0.895 | 0.943 | 3/3 | 2/3 |
| 2 m dewpoint C | 1.499 | 1.417 | 1.569 | 3/3 | 3/3 |
| 10 m wind speed m/s | 1.296 | 1.247 | 1.411 | 3/3 | 3/3 |

## Stronger Holdout Strategies

RustWX now supports `--holdout-strategy station_hash`, `spatial_block`, and
`source_hash`.

Spatial-block smoke:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --compare-barnes-baseline --holdout-strategy spatial_block --holdout-repeat-count 2 --no-grid-export --out-dir target\surface_mesoanalysis_oi_spatial_holdout_smoke
```

Two-fold spatial-block holdout MAE:

| field | raw HRRR | OI | Barnes | OI beats raw folds | OI beats Barnes folds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 m temperature C | 0.899 | 0.858 | 0.936 | 2/2 | 2/2 |
| 2 m dewpoint C | 1.339 | 1.307 | 1.633 | 2/2 | 2/2 |
| 10 m wind speed m/s | 1.243 | 1.223 | 1.471 | 2/2 | 2/2 |

Source/provider holdout smoke:

```powershell
target\release\surface_mesoanalysis.exe --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --compare-barnes-baseline --holdout-strategy source_hash --holdout-repeat-count 2 --no-grid-export --out-dir target\surface_mesoanalysis_oi_source_holdout_smoke
```

The first source-hash fold withheld `aviation_weather_metar_conus` as a provider
group (`2378` observations), which is intentionally much harsher than random
station withholding.

Two-fold source-hash holdout MAE:

| field | raw HRRR | OI | Barnes | OI beats raw folds | OI beats Barnes folds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 m temperature C | 0.933 | 0.931 | 0.994 | 1/2 | 2/2 |
| 2 m dewpoint C | 1.400 | 1.344 | 1.393 | 2/2 | 2/2 |
| 10 m wind speed m/s | 1.334 | 1.308 | 1.355 | 1/2 | 2/2 |

## Compact Agent Packet

The CLI writes `mesoanalysis_agent_packet.json` beside the full run report. It
is intended for LLM/agent consumption and avoids inlining station-level
validation samples.

Packet smoke:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --compare-barnes-baseline --holdout-strategy spatial_block --holdout-repeat-count 2 --out-dir target\surface_mesoanalysis_agent_packet_smoke
```

Artifact:
`target/surface_mesoanalysis_agent_packet_smoke/mesoanalysis_agent_packet.json`.

Current packet confidence semantics:

- `confidence_semantics.grid_confidence_field_kind` is
  `oi_variance_reduction_support_proxy`.
- `confidence_semantics.skill_calibrated_by_default` is `false`.
- `validation.confidence_reliability` uses holdout validation when available,
  falling back only when stronger validation is absent.
- Each field contract carries ranked low/high confidence bucket counts, ranked
  high-minus-low MAE, bucket coverage sufficiency, status, and a semantic label.
- Agent consumers should treat confidence as calibrated uncertainty only when
  the contract status is `passed` and the semantic label is
  `calibrated_reliability`. Otherwise it is support metadata:
  `uncalibrated_support` for represented buckets that failed the ranked MAE
  check, or `support_index` when the case is untestable.

Packet audit by inspection:

- Packet size: `127260` bytes.
- Full run report size: `11077179` bytes.
- Schema: `rustwx.surface_mesoanalysis.agent_packet.v1`.
- Analysis kind: `surface_adjusted_diagnostic`.
- Grid manifest field count: `15`.
- Same-observation and holdout sections do not include station-level `samples`.
- Repeated holdout strategy: `spatial_block`, `2` folds.

## Calibration Matrix Harness

The calibration harness aggregates existing `surface_mesoanalysis` run reports
without re-fetching model data:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --reports-root target --out target\surface_mesoanalysis_calibration\calibration_matrix.json --min-case-count 1
```

Artifact:
`target/surface_mesoanalysis_calibration/calibration_matrix.json`.

Smoke result from the current target tree:

- Requested reports discovered: `17`.
- Loaded surface mesoanalysis cases: `15`.
- Skipped reports: `2` unrelated or legacy run-report schemas.
- Benchmark modes: `10` holdout, `4` repeated holdout, `1`
  same-observation.
- Quality flags: expected to include skipped reports, mixed benchmark modes,
  and same-observation content when run across the whole `target` tree.

Aggregate MAE deltas across these smoke/tuning artifacts:

| field | OI minus raw HRRR MAE | OI beats raw cases | OI minus Barnes MAE | OI beats Barnes cases |
| --- | ---: | ---: | ---: | ---: |
| 2 m temperature C | -0.010 | 12/15 | -0.034 | 12/15 |
| 2 m dewpoint C | -0.059 | 13/15 | -0.122 | 12/15 |
| 10 m wind speed m/s | +0.007 | 10/15 | -0.099 | 12/15 |

The matrix also carries per-source aggregates. In the same smoke artifact:

| source | T OI-raw MAE | wind OI-raw MAE | T OI-Barnes MAE | wind OI-Barnes MAE |
| --- | ---: | ---: | ---: | ---: |
| METAR CONUS | -0.025 | +0.029 | -0.058 | -0.156 |
| Oklahoma Mesonet | -0.026 | -0.169 | +0.129 | +0.042 |
| Nebraska Mesonet | +0.022 | -0.195 | +0.130 | -0.037 |
| NDAWN | +0.062 | +0.044 | +0.022 | +0.168 |

This is a harness smoke, not a multi-case climatology. Most reports are tuning
variants of the same HRRR valid time. The important result is that the matrix
exposes both wins and losses: some aggressive or experimental settings degraded
temperature or wind against raw HRRR, and source-level results differ sharply by
network. That is the guardrail needed before making defaults more assertive.

## Calibration Gate Smoke

The matrix can also carry an enforceable gate for CI/tuning sweeps. This command
checks the two repeated-holdout smokes only, requires repeated-holdout evidence,
requires OI to beat raw HRRR and Barnes domain-wide, and allows up to `0.75`
MAE degradation on individual source/variable slices:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_oi_spatial_holdout_smoke\run_report.json --run-report target\surface_mesoanalysis_oi_source_holdout_smoke\run_report.json --out target\surface_mesoanalysis_calibration\repeated_gate_matrix.json --min-case-count 2 --fail-on-calibration-gate --require-benchmark-mode repeated_holdout_validation --gate-max-domain-oi-minus-raw-mae 0.0 --gate-max-domain-oi-minus-barnes-mae 0.0 --gate-max-source-oi-minus-raw-mae 0.75 --gate-max-source-oi-minus-barnes-mae 0.75
```

Artifact:
`target/surface_mesoanalysis_calibration/repeated_gate_matrix.json`.

Gate smoke result:

- Schema: `rustwx.surface_mesoanalysis.calibration_gate.v1`.
- Loaded cases: `2`.
- Required benchmark mode: `repeated_holdout_validation`.
- Gate status: passed.
- Domain OI-minus-raw MAE:
  - temperature `-0.021`.
  - dewpoint `-0.044`.
  - wind speed `-0.023`.
- Domain OI-minus-Barnes MAE:
  - temperature `-0.070`.
  - dewpoint `-0.188`.
  - wind speed `-0.148`.

## External Reference Lane

The calibration matrix can now carry optional external-reference benchmarks for
RTMA/URMA or another analysis of record. `surface_mesoanalysis` can generate
these directly with `--external-reference-model rtma` or
`--external-reference-model rtma,urma`.

The run report includes:

```json
{
  "external_reference_comparison": {
    "reference_label": "rtma",
    "validation_mode": "holdout_validation",
    "temperature_c": {
      "candidate_observation_count": 120,
      "candidate_mean_abs_error": 0.86,
      "reference_mean_abs_error": 0.90,
      "candidate_rmse": 1.10,
      "reference_rmse": 1.20
    }
  }
}
```

The matrix aggregates that into `aggregate.external_references.rtma` and the gate
can enforce it with:

```powershell
--require-external-reference rtma --gate-max-domain-oi-minus-reference-mae 0.0
```

The first live RTMA smoke used the same HRRR `20260513 00Z f001` case and
matched RTMA to the valid `20260513 01Z` analysis hour:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 0 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --holdout-strategy spatial_block --external-reference-model rtma --no-grid-export --out-dir target\surface_mesoanalysis_rtma_reference_smoke
```

Artifact:
`target/surface_mesoanalysis_rtma_reference_smoke/run_report.json`.

RTMA reference holdout comparison, using the same withheld station samples:

| field | raw HRRR MAE | OI MAE | RTMA MAE | OI minus raw | OI minus RTMA |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 m temperature C | 0.936 | 0.900 | 0.576 | -0.037 | +0.324 |
| 2 m dewpoint C | 1.263 | 1.241 | 1.386 | -0.022 | -0.144 |
| 10 m wind speed m/s | 1.263 | 1.250 | 1.109 | -0.014 | +0.141 |

This is the right kind of uncomfortable result: the OI layer improved raw HRRR
on all three withheld variables and beat RTMA on dewpoint for this case, while
RTMA still beat OI on temperature and wind speed. That gives the tuning harness
a real target instead of only a Barnes baseline.

The calibration aggregate now accepts external-reference-only reports, so this
command loads the RTMA smoke even when a Barnes comparison was not requested:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_rtma_reference_smoke\run_report.json --out target\surface_mesoanalysis_calibration\rtma_reference_matrix.json --min-case-count 1 --require-external-reference rtma
```

Artifact:
`target/surface_mesoanalysis_calibration/rtma_reference_matrix.json`.

## Terrain/Flow Covariance Ablation

`surface_mesoanalysis` can now run a no-flow/no-terrain OI ablation with
`--compare-isotropic-oi-baseline`. The ablation keeps the same OI solver,
background errors, observation errors, kernel, length scale, and holdout split,
but sets:

- `oi_flow_anisotropy_ratio = 1.0`.
- `oi_terrain_pressure_scale_hpa = 1.0e9`.

This makes the comparison an explicit test of whether flow anisotropy and
terrain-pressure damping helped the case, rather than assuming the covariance
terms are useful because they exist.

Smoke command:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 2 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --holdout-strategy spatial_block --compare-isotropic-oi-baseline --no-grid-export --out-dir target\surface_mesoanalysis_covariance_ablation_smoke_03z
```

Artifact:
`target/surface_mesoanalysis_covariance_ablation_smoke_03z/run_report.json`.

Holdout MAE, same withheld station samples:

| field | terrain/flow OI | isotropic OI | terrain/flow minus isotropic |
| --- | ---: | ---: | ---: |
| 2 m temperature C | 1.101 | 1.118 | -0.017 |
| 2 m dewpoint C | 1.132 | 1.137 | -0.005 |
| 10 m wind speed m/s | 1.218 | 1.235 | -0.017 |

The first ablation smoke shows a small positive holdout signal for the
terrain/flow covariance terms on all three variables, with `260` withheld
samples. This is not enough to lock the defaults; it is enough to make the
covariance terms auditable case by case.

The calibration matrix now parses those ablation comparisons as first-class
evidence under `aggregate.covariance_ablations.<baseline_label>`. This lets a
tuning sweep require that the terrain/flow covariance terms do no worse than
the isotropic/no-terrain OI baseline:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_covariance_ablation_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\covariance_ablation_matrix.json --min-case-count 1 --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c,wind_speed_ms --gate-max-covariance-ablation-oi-minus-baseline-mae 0.0
```

Artifact:
`target/surface_mesoanalysis_calibration/covariance_ablation_matrix.json`.

Gate result for this smoke: passed. The gate observed candidate-minus-baseline
holdout MAE of `-0.017` C for temperature, `-0.005` C for dewpoint, and
`-0.017` m/s for wind speed.

## Spatial-Index Hot-Path Pass

The calc layer now scans spatial-bin candidates through a callback rather than
allocating a fresh candidate `Vec` for every target grid cell. This affects
nearest-grid lookup, Barnes passes, and OI target-neighbor selection. It also
wraps longitude bins across the dateline instead of silently missing candidates
there. Dense-network OI also uses partial top-K selection before the local
matrix solve, so capped neighborhoods no longer sort every candidate just to
retain the strongest `oi_max_observations_per_grid_cell` observations.

Verification:

```powershell
cargo test -p rustwx-calc mesoanalysis
cargo test -p rustwx-products mesoanalysis
cargo check -p rustwx-cli --bin surface_mesoanalysis --bin hrrr_mesoanalysis --bin surface_mesoanalysis_calibration
```

Live-style smoke after the change:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 2 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --holdout-strategy spatial_block --compare-isotropic-oi-baseline --no-grid-export --out-dir target\surface_mesoanalysis_spatial_index_smoke_03z
```

Artifact:
`target/surface_mesoanalysis_spatial_index_smoke_03z/run_report.json`.

The live-style report recorded `1178 ms` for terrain/flow OI and `1085 ms` for
the isotropic/no-terrain OI baseline, versus `1534 ms` and `1418 ms` in the
earlier 03Z smoke. The calibration aggregate now records
`mean_mesoanalysis_compute_ms` and `max_mesoanalysis_compute_ms`, and the gate
can enforce a per-case runtime ceiling with
`--gate-max-case-mesoanalysis-compute-ms`. The paired ablation plus speed gate
still passed:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_spatial_index_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\spatial_index_speed_gate_matrix.json --min-case-count 1 --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c,wind_speed_ms --gate-max-covariance-ablation-oi-minus-baseline-mae 0.0 --gate-max-case-mesoanalysis-compute-ms 2000
```

Gate artifact:
`target/surface_mesoanalysis_calibration/spatial_index_speed_gate_matrix.json`.

Observed candidate-minus-isotropic holdout MAE after the hot-path pass:
temperature `-0.015` C, dewpoint `-0.003` C, and wind speed `-0.017` m/s.
Observed max per-case compute time: `1178 ms` against a `2000 ms` gate.

## Gross-Error Buddy Rescue

The OI gross-error filter now has a buddy-observation rescue path. A large
innovation that exceeds the normalized gross-error threshold can still be kept
when nearby observations within `--oi-gross-error-buddy-radius-km` have the
same sign and agree within `--oi-gross-error-buddy-agreement-sigma` combined
error. The default requires at least one supporting neighbor.

Run diagnostics now include `gross_error_rescued_observations` per analyzed
variable, and the calibration matrix aggregates those diagnostics by variable
so tuning sweeps can spot configurations that either reject real mesoscale
features or rescue too many questionable extremes.

## Time-Representativeness Weighting

The runner-observation loader now separates hard freshness rejection from
within-window time representativeness. Observations outside
`--max-obs-age-minutes` are still rejected, but accepted observations are
down-weighted by age relative to the model valid time. The default half-life is
controlled by `--obs-time-weight-half-life-minutes`, and the maximum
observation-error inflation is controlled by
`--obs-max-time-error-inflation-factor`.

The run report and compact agent packet source summaries now include accepted
minimum/mean/maximum observation age plus mean/min time weight, so downstream
agents can distinguish fresh high-support corrections from older but still
usable context.

## Observation De-Duplication

Runner observations are now de-duplicated after source/profile/time filtering.
The loader keeps the best duplicate by source/time-adjusted quality weight,
field completeness, timestamp, and observation-error precision. This prevents a
station mirrored through multiple feeds from acting like multiple independent
measurements in the OI solve.

Source summaries now report `duplicate_filtered_count`, and
`accepted_for_mesoanalysis` reflects the post-dedup count that actually reaches
the correction layer.

## Validation Strata

Validation output now carries machine-readable strata in addition to raw source
summaries. Each validation sample includes source-quality metadata,
representativeness class, correction role, observation age bucket, and a
terrain-pressure class derived from the nearest background grid-cell surface
pressure.

Run reports and agent packets expose `strata_summaries` for:

- `source_quality_class`
- `representativeness_class`
- `correction_role`
- `terrain_pressure_class`
- `observation_age_bucket`

The calibration matrix also aggregates these under `aggregate.strata`. This is
the next guardrail against over-tuning: a case can pass overall while still
showing a weak bucket, such as old observations, road-weather microclimates, or
high-terrain pressure classes.

The calibration gate can now target those strata directly. Use
`--gate-stratum` with keys such as
`terrain_pressure_class=mountain_terrain` or
`representativeness_class=road_microclimate`, then apply stratum MAE or
confidence-reliability thresholds:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_strata_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\strata_gate_matrix.json --min-case-count 1 --gate-variable temperature_c,dewpoint_c,wind_speed_ms --gate-stratum terrain_pressure_class=mountain_terrain --gate-max-stratum-oi-minus-raw-mae 0.0
```

That gate shape is intentionally stricter than a domain average: it can fail a
high-terrain, stale-age, or questionable-representativeness bucket before the
system promotes a tuning change as generally trustworthy.

## Calibration Breadth Gates

The calibration matrix now records breadth metadata so a sweep can distinguish
real multi-case evidence from repeated variants of one valid time:

- `model_counts`.
- `model_source_counts`.
- `date_counts`.
- `cycle_counts`.
- `forecast_hour_counts`.
- `case_signature_counts`, keyed by model, source, date, cycle, and forecast
  hour.
- `case_tag_counts`, populated from run-level `--case-tag` labels such as
  `regime=dryline`, `hazard=severe`, or `domain=ok_plains`.

The calibration gate can require:

- `--require-holdout-strategy`.
- `--require-case-tag`.
- `--gate-min-unique-case-signatures`.
- `--gate-min-unique-dates`.
- `--gate-min-unique-cycles`.
- `--gate-min-unique-forecast-hours`.
- `--gate-min-unique-case-tags`.

If a matrix has more than one loaded report but only one unique case signature,
it receives the `single_case_signature_matrix` quality flag.

Real two-cycle smoke:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_rtma_reference_smoke\run_report.json --run-report target\surface_mesoanalysis_spatial_index_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\diverse_two_cycle_gate_matrix.json --min-case-count 2 --require-holdout-strategy spatial_block --gate-min-unique-case-signatures 2 --gate-min-unique-cycles 2 --gate-min-unique-dates 1 --gate-min-unique-forecast-hours 1 --gate-max-case-mesoanalysis-compute-ms 2000
```

Artifact:
`target/surface_mesoanalysis_calibration/diverse_two_cycle_gate_matrix.json`.

Gate result: passed. The matrix loaded `2` reports, found `2` unique case
signatures, `2` cycles, `1` date, `1` forecast hour, and a max per-case compute
time of `1453 ms`.

For future archive sweeps, tag each `surface_mesoanalysis` run with labels like
`--case-tag regime=dryline,hazard=severe,domain=ok_plains`. The calibration
matrix will preserve those tags per case and aggregate them under
`aggregate.case_tag_counts`; strict gates can then require particular regimes or
a minimum number of unique tags before accepting a tuning change.

## Station Innovation Aggregates

The matrix now also extracts compact station-level innovation summaries from
the selected validation mode's `samples` array:

- Repeated holdout: all completed fold validation samples.
- Holdout: `mesoanalysis.holdout_validation.validation.samples`.
- Same-observation fallback: `mesoanalysis.validation.samples`.

Those are aggregated under `aggregate.stations`, keyed as `source::station_id`.
Each station records sample count plus per-field mean signed background error,
mean signed analysis error, MAE, RMSE, mean absolute-error improvement, and max
absolute errors. This is the first calibration hook for rolling station/provider
innovation monitoring: a domain or source score can pass while one station is
persistently biased, stale, badly exposed, or over-corrected.

Real tagged smoke artifact:
`target/surface_mesoanalysis_calibration/station_innovation_matrix.json`.

For the `20260513 02Z f001` tagged station-hash smoke, the matrix had `10`
station aggregates from the withheld METAR sample. The largest temperature
analysis MAE in that small holdout was `KP69` at about `5.46 C`, which is exactly
the sort of station-level signal that should be inspected before broadening OI
trust.

The calibration gate can now make that inspection machine-enforceable instead
of advisory. Use `--gate-station` with the aggregate key plus station thresholds:

- `--gate-min-station-observation-count`.
- `--gate-max-station-oi-minus-raw-mae`.
- `--gate-max-station-analysis-mae`.
- `--gate-max-station-abs-analysis-bias`.

Passing station gate smoke:

```powershell
target\debug\surface_mesoanalysis_calibration.exe --run-report target\surface_mesoanalysis_case_tag_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\station_gate_pass_matrix.json --innovation-history-out target\surface_mesoanalysis_calibration\innovation_history_smoke.json --min-case-count 1 --require-case-tag regime=dryline --gate-min-unique-case-tags 3 --gate-variable temperature_c --gate-station aviation_weather_metar_conus::KP69 --gate-min-station-observation-count 1 --gate-max-station-analysis-mae 6.0 --gate-max-station-abs-analysis-bias 6.0 --fail-on-calibration-gate
```

Failing station gate smoke:

```powershell
target\debug\surface_mesoanalysis_calibration.exe --run-report target\surface_mesoanalysis_case_tag_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\station_gate_fail_matrix.json --min-case-count 1 --require-case-tag regime=dryline --gate-min-unique-case-tags 3 --gate-variable temperature_c --gate-station aviation_weather_metar_conus::KP69 --gate-max-station-analysis-mae 1.0
```

The pass artifact observed `KP69` temperature station MAE and absolute bias at
about `5.46 C` against a `6.0 C` threshold. The fail artifact kept the process
exit clean because `--fail-on-calibration-gate` was omitted, but wrote
`calibration_gate.passed = false` with a failed `station_analysis_mae` check. The
matching fail-on smoke returned a hard nonzero status with `surface mesoanalysis
calibration gate failed`.

The same pass smoke also wrote
`target/surface_mesoanalysis_calibration/innovation_history_smoke.json` with
schema `rustwx.surface_mesoanalysis.innovation_history.v1`. That sidecar keeps
station and source/provider series keyed by `source::station_id` and source name,
with per-case entries plus aggregate rollups. In the smoke artifact it contained
`10` station series, `1` source series, and the KP69 entry preserved the
`hrrr|nomads|20260513|02|f001` case signature and `5.455 C` temperature analysis
MAE.

Rolling history merge smoke:

```powershell
target\debug\surface_mesoanalysis_calibration.exe --run-report target\surface_mesoanalysis_case_tag_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\station_gate_rolling_matrix.json --innovation-history-in target\surface_mesoanalysis_calibration\innovation_history_smoke.json --innovation-history-out target\surface_mesoanalysis_calibration\innovation_history_rolling_smoke.json --innovation-history-max-entries-per-series 1 --min-case-count 1 --require-case-tag regime=dryline --gate-min-unique-case-tags 3 --gate-variable temperature_c --gate-station aviation_weather_metar_conus::KP69 --gate-min-station-observation-count 1 --gate-max-station-analysis-mae 6.0 --gate-max-station-abs-analysis-bias 6.0 --fail-on-calibration-gate
```

That command read the prior history, merged the current case back into it,
deduped by case identity, recomputed station/source aggregate rollups, and
retained only the newest entry per series. The resulting
`innovation_history_rolling_smoke.json` still had `case_count = 1`, `10` station
series, `1` source series, and one KP69 entry, proving the merge path does not
double-count a rerun of the same case.

The same history schema now carries operator/agent watchlists:

- `station_watchlist`: worst station-field combinations ranked by rolling
  analysis MAE, absolute bias, negative improvement versus background, and tail
  error.
- `source_watchlist`: worst source-field combinations ranked by rolling analysis
  MAE and whether OI is worse than the raw background.

In the rolling smoke, the top station watch item was
`aviation_weather_metar_conus::KP69` / `temperature_c` with about `5.455 C`
analysis MAE and reason `persistent_station_bias`. The source watchlist flagged
`aviation_weather_metar_conus` / `wind_speed_ms` as
`source_mean_worse_than_background` because its OI-minus-raw MAE was slightly
positive in this one-case smoke. This is intentionally a triage signal; broad
provider trust still needs multi-case evidence.

Query-only history contract:

```powershell
target\debug\surface_mesoanalysis_calibration.exe --innovation-query-history target\surface_mesoanalysis_calibration\innovation_history_rolling_smoke.json --innovation-query-station aviation_weather_metar_conus::KP69 --innovation-query-variable temperature_c --innovation-query-top 3 --innovation-query-out target\surface_mesoanalysis_calibration\innovation_query_kp69_temperature.json
```

This writes `rustwx.surface_mesoanalysis.innovation_query.v1`. The KP69 query
returned one station watch item, no source watch items, reason
`persistent_station_bias`, and temperature analysis MAE `5.455 C`. A source query
for `aviation_weather_metar_conus` / `wind_speed_ms` returned one source watch
item with reason `source_mean_worse_than_background` and OI-minus-raw MAE
`0.0138`.

WxStore-shaped innovation index smoke:

```powershell
target\debug\surface_mesoanalysis_calibration.exe --innovation-query-history target\surface_mesoanalysis_calibration\innovation_history_rolling_smoke.json --innovation-query-station aviation_weather_metar_conus::KP69 --innovation-query-variable temperature_c --innovation-query-out target\surface_mesoanalysis_calibration\innovation_query_kp69_temperature.json --innovation-wxstore-index-dir target\surface_mesoanalysis_calibration\innovation_wxstore_index_smoke
```

This wrote `target/surface_mesoanalysis_calibration/innovation_wxstore_index_smoke`
with schema `rustwx.surface_mesoanalysis.innovation_wxstore_index.v1` and files:

- `manifest.json`.
- `station_index.jsonl`.
- `source_index.jsonl`.
- `station_watchlist.json`.
- `source_watchlist.json`.

The manifest recorded `case_count = 1`, `10` station series, and `1` source
series. The `station_index.jsonl` KP69 temperature record carried
`mean_abs_analysis_error = 5.455 C` plus its denormalized watchlist reason
`persistent_station_bias`.

## Confidence Reliability Gates

The validation packet now carries confidence calibration summaries alongside
MAE/RMSE. Each variable summary can include:

- Mean confidence at sampled observations.
- Low/medium/high confidence bucket counts.
- Bucket MAE for low, medium, and high confidence samples.
- `high_minus_low_mean_abs_analysis_error` when both buckets are represented.
- Ranked low/high confidence tercile counts and MAE, which are useful when all
  held-out samples land in the same absolute confidence bucket.
- `confidence_abs_error_correlation`, where negative is the desired sign.

The calibration matrix preserves those metrics at domain and per-source scope.
It also records minimum low/medium/high confidence bucket counts across loaded
cases so a strict reliability gate can distinguish a bad confidence ranking from
an under-covered reliability test.

New gate knobs:

- `--gate-min-domain-low-confidence-observation-count`.
- `--gate-min-domain-high-confidence-observation-count`.
- `--gate-max-domain-high-minus-low-confidence-mae`.
- `--gate-max-domain-ranked-high-minus-low-confidence-mae`.
- `--gate-max-domain-confidence-abs-error-correlation`.
- `--gate-min-source-low-confidence-observation-count`.
- `--gate-min-source-high-confidence-observation-count`.
- `--gate-max-source-high-minus-low-confidence-mae`.
- `--gate-max-source-ranked-high-minus-low-confidence-mae`.
- `--gate-max-source-confidence-abs-error-correlation`.

Live-style confidence smoke:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 2 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --holdout-strategy spatial_block --compare-isotropic-oi-baseline --no-grid-export --out-dir target\surface_mesoanalysis_confidence_smoke_03z
```

Artifact:
`target/surface_mesoanalysis_confidence_smoke_03z/run_report.json`.

The smoke recorded `544 ms` mesoanalysis compute time, `467` sampled
observations in same-observation validation, and `51` withheld observations in
the spatial-block holdout. The same-observation validation had useful
confidence spread, but the spatial-block holdout deliberately removed the local
observation support around withheld stations. As a result, the calibration
matrix placed all withheld temperature/dewpoint samples in the low-confidence
bucket:

| field | low-count | high-count | confidence/error correlation |
| --- | ---: | ---: | ---: |
| 2 m temperature C | 51 | 0 | +0.015 |
| 2 m dewpoint C | 51 | 0 | +0.118 |

Strict bucket-coverage/reliability gate:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_confidence_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\confidence_bucket_coverage_gate_matrix.json --min-case-count 1 --require-holdout-strategy spatial_block --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c --gate-min-domain-low-confidence-observation-count 1 --gate-min-domain-high-confidence-observation-count 1 --gate-max-domain-high-minus-low-confidence-mae 0.0 --gate-max-domain-confidence-abs-error-correlation 0.0 --gate-max-case-mesoanalysis-compute-ms 2000
```

Artifact:
`target/surface_mesoanalysis_calibration/confidence_bucket_coverage_gate_matrix.json`.

Gate result: failed, as intended for this holdout geometry. The matrix passed
runtime, holdout-strategy, covariance-ablation, and low-confidence bucket
coverage checks, but failed because both temperature and dewpoint had
`0` high-confidence withheld samples against the explicit smoke threshold of
`1`, no high-minus-low MAE could be computed, and the confidence/error
correlation was slightly positive. This is a calibration warning, not a crash:
strict confidence reliability now requires a holdout design with enough
low- and high-confidence samples to satisfy the core ranked-bucket contract.

Softer smoke gate:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_confidence_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\confidence_presence_gate_matrix.json --min-case-count 1 --require-holdout-strategy spatial_block --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c --gate-max-domain-confidence-abs-error-correlation 0.2 --gate-max-case-mesoanalysis-compute-ms 2000
```

Artifact:
`target/surface_mesoanalysis_calibration/confidence_presence_gate_matrix.json`.

Gate result: passed. This weaker gate is useful as a smoke check that
confidence metrics are present, parsed, and bounded, while the stricter
bucket-coverage gate remains the professional calibration target.

Ranked station-hash confidence smoke:

```powershell
cargo run -p rustwx-cli --release --bin surface_mesoanalysis -- --model hrrr --date 20260513 --cycle 2 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --holdout-strategy station_hash --compare-isotropic-oi-baseline --no-grid-export --out-dir target\surface_mesoanalysis_confidence_ranked_station_hash_smoke_03z
```

Artifact:
`target/surface_mesoanalysis_confidence_ranked_station_hash_smoke_03z/run_report.json`.

This run produced a release-mode OI compute time of `293 ms` and passed the
operational validation gate with `36` sampled observations. The report now
contains ranked low/high confidence terciles. Same-observation validation had
useful-looking ranked confidence for temperature and dewpoint, but the stricter
station-hash holdout exposed a real calibration problem: higher-confidence
held-out stations had larger MAE in this case.

Holdout ranked confidence result from the calibration matrix:

| field | ranked low-count | ranked low MAE | ranked high-count | ranked high MAE | high-minus-low MAE |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 m temperature C | 3 | 0.293 | 3 | 2.557 | +2.265 |
| 2 m dewpoint C | 3 | 0.401 | 3 | 1.456 | +1.055 |
| 10 m wind speed m/s | 3 | 0.657 | 3 | 1.943 | +1.286 |

Strict ranked reliability gate:

```powershell
cargo run -p rustwx-cli --bin surface_mesoanalysis_calibration -- --run-report target\surface_mesoanalysis_confidence_ranked_station_hash_smoke_03z\run_report.json --out target\surface_mesoanalysis_calibration\confidence_ranked_station_hash_gate_matrix.json --min-case-count 1 --require-holdout-strategy station_hash --require-covariance-ablation IsotropicOiNoTerrain --gate-variable temperature_c,dewpoint_c,wind_speed_ms --gate-max-domain-ranked-high-minus-low-confidence-mae 0.0 --gate-max-case-mesoanalysis-compute-ms 2000
```

Artifact:
`target/surface_mesoanalysis_calibration/confidence_ranked_station_hash_gate_matrix.json`.

Gate result: failed, intentionally. Runtime, holdout strategy, and covariance
ablation checks passed, but all three variables failed the ranked confidence
skill threshold in this historical artifact. The core reliability contract has
since been tightened further: a field now needs at least `10` observations in
both the ranked low-confidence and ranked high-confidence buckets before the
gate is testable. Smaller ranked buckets may still be useful diagnostics, but
they remain `support_index` metadata rather than calibrated reliability. This
means the current grid `confidence` value should be read as OI
support/variance-reduction metadata, not yet as a calibrated forecast of lower
held-out error. That is exactly the distinction the gate is meant to surface
before the packet tells an agent to trust corrected fields.

Stronger min-10 confidence contract smoke:

```powershell
target\surface_mesoanalysis_release_refresh\release\surface_mesoanalysis.exe --model hrrr --date 20260513 --cycle 2 --forecast-hour 1 --model-source nomads --observations-root $env:RUSTWX_RUNNER_DATA\observations --obs-profile surface_meso_conus --obs-source aviation_weather_metar_conus,oklahoma_mesonet,kansas_mesonet,nebraska_mesonet,ndawn --analysis-method oi --holdout-strategy station_hash --compare-isotropic-oi-baseline --no-grid-export --out-dir target\surface_mesoanalysis_confidence_min10_smoke_03z
```

Artifacts:

- `target/surface_mesoanalysis_confidence_min10_smoke_03z/run_report.json`
- `target/surface_mesoanalysis_confidence_min10_smoke_03z/mesoanalysis_agent_packet.json`
- `target/surface_mesoanalysis_calibration/confidence_min10_station_hash_gate_matrix.json`

This release-mode smoke completed the OI solve in `317 ms` and wrote
`min_ranked_bucket_observation_count = 10` into the run report, compact agent
packet, and calibration matrix. The holdout had only `3` observations in each
ranked confidence tercile for temperature, dewpoint, and wind, so all three
fields are now correctly labeled `status = untestable`,
`semantic_label = support_index`, with `bucket_coverage_sufficient = false`.
The ranked-confidence calibration gate is also reliability-aware: its
comparator is now `<= and reliability=passed`, and the gate message reports the
reliability status, semantic label, bucket coverage, and passed/failed/
untestable case counts. That prevents an under-covered negative ranked delta
from being misread as calibrated reliability.
