from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
REPORT_DIR = ROOT / "target" / "reports"
ASSET_DIR = REPORT_DIR / "mesoanalysis_after_barnes_assets"

TIME_WEIGHT_DIR = ROOT / "target" / "surface_mesoanalysis_time_weight_smoke_03z"
CAL_DIR = ROOT / "target" / "surface_mesoanalysis_calibration"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def maybe_load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return load_json(path)


def tex_escape(value: object) -> str:
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(ch, ch) for ch in text)


def fmt(value: object, digits: int = 3) -> str:
    if value is None:
        return "--"
    if isinstance(value, float):
        if math.isnan(value):
            return "--"
        return f"{value:.{digits}f}"
    return str(value)


def short_source_name(source: str) -> str:
    mapping = {
        "aviation_weather_metar_conus": "METAR",
        "oklahoma_mesonet": "OK Mesonet",
        "kansas_mesonet": "KS Mesonet",
        "nebraska_mesonet": "NE Mesonet",
        "ndawn": "NDAWN",
    }
    return mapping.get(source, source.replace("_", " "))


def savefig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=180, bbox_inches="tight")
    plt.close()


def make_architecture_flow(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4.7))
    ax.axis("off")
    boxes = [
        ("Model background\nHRRR/RAP/RRFS/etc.", 0.06, 0.62, "#D7E7F7"),
        ("Observation feeds\nMETAR, mesonets,\nRAWS/RWIS/marine", 0.06, 0.20, "#DDF0D8"),
        ("Source QC\nfreshness, errors,\ntime weights, dedup", 0.28, 0.40, "#F7E7B5"),
        ("Local OI solve\ncovariance matrix,\nterrain/flow terms", 0.50, 0.40, "#F6D6D6"),
        ("Diagnostics\nincrements, residuals,\nconfidence/support", 0.71, 0.62, "#E6DDF7"),
        ("Validation\nholdout gates,\ncalibration matrix", 0.71, 0.20, "#F7DDE9"),
        ("Agent packet + maps\nfocused, typed,\nprovenance-rich", 0.90, 0.40, "#DDF3F7"),
    ]
    for label, x, y, color in boxes:
        ax.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            fontsize=10,
            bbox=dict(boxstyle="round,pad=0.45", facecolor=color, edgecolor="#36454F", linewidth=1.2),
        )

    arrows = [
        ((0.16, 0.62), (0.25, 0.47)),
        ((0.16, 0.20), (0.25, 0.34)),
        ((0.38, 0.40), (0.46, 0.40)),
        ((0.59, 0.40), (0.67, 0.58)),
        ((0.59, 0.40), (0.67, 0.25)),
        ((0.80, 0.62), (0.87, 0.47)),
        ((0.80, 0.20), (0.87, 0.34)),
    ]
    for start, end in arrows:
        ax.annotate(
            "",
            xy=end,
            xytext=start,
            arrowprops=dict(arrowstyle="->", lw=1.6, color="#36454F"),
        )

    ax.text(
        0.5,
        0.05,
        "Key idea: this is a model-agnostic diagnostic surface correction/reliability layer, not a 3D data-assimilation cycle.",
        ha="center",
        fontsize=10,
        color="#333333",
    )
    savefig(path)


def make_source_counts_chart(packet: dict, path: Path) -> list[dict]:
    summaries = packet.get("observations", {}).get("source_summaries", [])
    rows = []
    for item in summaries:
        rows.append(
            {
                "source": item.get("source", "unknown"),
                "raw": item.get("observation_count") or 0,
                "profile_filtered": item.get("profile_filtered_count") or 0,
                "time_filtered": item.get("time_filtered_count") or 0,
                "accepted": item.get("accepted_for_mesoanalysis") or 0,
                "mean_time_weight": item.get("mean_time_weight"),
                "max_age": item.get("accepted_max_observation_age_minutes"),
            }
        )
    labels = [short_source_name(row["source"]) for row in rows]
    x = range(len(rows))
    width = 0.2

    fig, ax = plt.subplots(figsize=(11, 5.2))
    ax.bar([i - width * 1.5 for i in x], [row["raw"] for row in rows], width, label="raw")
    ax.bar([i - width * 0.5 for i in x], [row["profile_filtered"] for row in rows], width, label="profile filtered")
    ax.bar([i + width * 0.5 for i in x], [row["time_filtered"] for row in rows], width, label="time filtered")
    ax.bar([i + width * 1.5 for i in x], [row["accepted"] for row in rows], width, label="accepted")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel("Observation records")
    ax.set_title("Observation funnel in the time-weight smoke packet")
    ax.legend(ncol=4, fontsize=9)
    ax.grid(axis="y", alpha=0.25)
    savefig(path)
    return rows


def make_holdout_mae_chart(matrix: dict, path: Path) -> list[dict]:
    variables = [
        ("temperature_c", "2 m T"),
        ("dewpoint_c", "2 m Td"),
        ("wind_speed_ms", "10 m wind"),
    ]
    rows = []
    for key, label in variables:
        stats = matrix.get("aggregate", {}).get("variables", {}).get(key, {})
        rows.append(
            {
                "field": label,
                "raw": stats.get("mean_background_mae"),
                "oi": stats.get("mean_candidate_mae"),
                "barnes": stats.get("mean_barnes_mae"),
                "oi_minus_raw": stats.get("mean_candidate_minus_background_mae"),
                "oi_minus_barnes": stats.get("mean_candidate_minus_barnes_mae"),
            }
        )

    x = range(len(rows))
    width = 0.25
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar([i - width for i in x], [row["raw"] for row in rows], width, label="raw background")
    ax.bar(list(x), [row["oi"] for row in rows], width, label="RustWX OI")
    ax.bar([i + width for i in x], [row["barnes"] for row in rows], width, label="Barnes")
    ax.set_xticks(list(x))
    ax.set_xticklabels([row["field"] for row in rows])
    ax.set_ylabel("MAE")
    ax.set_title("Repeated holdout gate: OI vs raw background vs Barnes")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    savefig(path)
    return rows


def make_confidence_chart(packet: dict, matrix: dict, path: Path) -> list[dict]:
    packet_fields = packet.get("validation", {}).get("confidence_reliability", {}).get("fields", {})
    matrix_vars = matrix.get("aggregate", {}).get("variables", {})
    labels = [
        ("temperature_c", "2 m T"),
        ("dewpoint_c", "2 m Td"),
        ("wind_speed_ms", "10 m wind"),
    ]
    rows = []
    for key, label in labels:
        pstats = packet_fields.get(key, {})
        cstats = matrix_vars.get(key, {}).get("confidence", {})
        rows.append(
            {
                "field": label,
                "low_count": pstats.get("ranked_low_confidence_observation_count"),
                "high_count": pstats.get("ranked_high_confidence_observation_count"),
                "low_mae": cstats.get("mean_ranked_low_confidence_mae"),
                "high_mae": cstats.get("mean_ranked_high_confidence_mae"),
                "high_minus_low": pstats.get("ranked_high_minus_low_mean_abs_analysis_error"),
                "status": pstats.get("status"),
                "semantic_label": pstats.get("semantic_label"),
            }
        )

    x = range(len(rows))
    width = 0.32
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar([i - width / 2 for i in x], [row["low_mae"] for row in rows], width, label="ranked low confidence")
    ax.bar([i + width / 2 for i in x], [row["high_mae"] for row in rows], width, label="ranked high confidence")
    ax.axhline(0, color="#444444", linewidth=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels([row["field"] for row in rows])
    ax.set_ylabel("Holdout MAE")
    ax.set_title("Confidence reliability failed: high-confidence buckets had higher MAE")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    savefig(path)
    return rows


def make_runtime_chart(path: Path) -> list[dict]:
    selected = [
        ("default OI + Barnes", ROOT / "target" / "surface_mesoanalysis_oi_default_smoke" / "run_report.json", "Barnes"),
        ("confidence gate", ROOT / "target" / "surface_mesoanalysis_confidence_ranked_station_hash_smoke_03z" / "run_report.json", "iso OI"),
        ("buddy rescue", ROOT / "target" / "surface_mesoanalysis_buddy_rescue_smoke_03z" / "run_report.json", "iso OI"),
        ("time weighting", ROOT / "target" / "surface_mesoanalysis_time_weight_smoke_03z" / "run_report.json", "iso OI"),
    ]
    rows = []
    for label, file_path, baseline_label in selected:
        report = maybe_load_json(file_path)
        if not report:
            continue
        baseline = report.get("barnes_baseline_comparison") or report.get("covariance_ablation_comparison") or {}
        rows.append(
            {
                "case": label,
                "oi_ms": report.get("mesoanalysis_compute_ms"),
                "baseline_ms": baseline.get("baseline_compute_ms"),
                "baseline_label": baseline_label,
            }
        )

    labels = [row["case"] for row in rows]
    x = range(len(rows))
    width = 0.32
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar([i - width / 2 for i in x], [row["oi_ms"] for row in rows], width, label="RustWX OI")
    ax.bar([i + width / 2 for i in x], [row["baseline_ms"] for row in rows], width, label="baseline")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.set_ylabel("Compute time, ms")
    ax.set_title("Smoke artifact runtimes (mixed debug/release, not a formal benchmark)")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    savefig(path)
    return rows


def table_rows(rows: list[list[object]]) -> str:
    return "\n".join(" & ".join(tex_escape(item) for item in row) + r" \\" for row in rows)


def artifact_table_rows(rows: list[list[object]]) -> str:
    rendered = []
    for label, path in rows:
        rendered.append(f"{tex_escape(label)} & \\path{{{path}}} \\\\")
    return "\n".join(rendered)


def rel_posix(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def build_report() -> tuple[Path, dict]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ASSET_DIR.mkdir(parents=True, exist_ok=True)

    packet_path = TIME_WEIGHT_DIR / "mesoanalysis_agent_packet.json"
    run_report_path = TIME_WEIGHT_DIR / "run_report.json"
    packet = load_json(packet_path)
    run_report = load_json(run_report_path)
    repeated_gate = load_json(CAL_DIR / "repeated_gate_matrix.json")
    confidence_matrix = load_json(CAL_DIR / "confidence_ranked_station_hash_gate_matrix.json")
    buddy_matrix = load_json(CAL_DIR / "buddy_rescue_diagnostics_matrix.json")

    architecture_png = ASSET_DIR / "architecture_flow.png"
    source_counts_png = ASSET_DIR / "source_counts.png"
    holdout_mae_png = ASSET_DIR / "holdout_mae.png"
    confidence_png = ASSET_DIR / "confidence_reliability.png"
    runtime_png = ASSET_DIR / "runtime_smokes.png"

    make_architecture_flow(architecture_png)
    source_rows = make_source_counts_chart(packet, source_counts_png)
    holdout_rows = make_holdout_mae_chart(repeated_gate, holdout_mae_png)
    confidence_rows = make_confidence_chart(packet, confidence_matrix, confidence_png)
    runtime_rows = make_runtime_chart(runtime_png)

    run = packet.get("run", {})
    validation = packet.get("validation", {})
    conf = validation.get("confidence_reliability", {})
    obs = packet.get("observations", {})
    method = packet.get("method", {})
    buddy_diag = buddy_matrix.get("aggregate", {}).get("diagnostics", {})

    artifact_rows = [
        ["Time-weight run report", rel_posix(run_report_path)],
        ["Time-weight agent packet", rel_posix(packet_path)],
        ["Repeated holdout gate matrix", "target/surface_mesoanalysis_calibration/repeated_gate_matrix.json"],
        ["Confidence ranked station-hash matrix", "target/surface_mesoanalysis_calibration/confidence_ranked_station_hash_gate_matrix.json"],
        ["Buddy rescue diagnostics matrix", "target/surface_mesoanalysis_calibration/buddy_rescue_diagnostics_matrix.json"],
        ["Completion audit", "docs/mesoanalysis_oi_completion_audit.md"],
        ["Calibration note", "docs/mesoanalysis_oi_calibration_20260513.md"],
    ]

    timeline_rows = [
        ["Barnes prototype", "Distance-weighted residual interpolation", "Good first visual correction; weak error model and validation semantics."],
        ["Full-matrix OI", "Local dense covariance solve with Cholesky", "Turns smoothing into an explicit background-plus-observation error problem."],
        ["Source error profiles", "METAR, mesonet, RAWS, RWIS, SNOTEL/SCAN, marine/coastal, generic", "Lets good networks correct more strongly while noisy/siting-sensitive sources are damped."],
        ["Terrain/flow covariance", "Wind-aligned anisotropy and pressure-terrain damping", "Reduces cross-flow and cross-terrain over-spreading."],
        ["Holdout validation", "Station-hash, spatial-block, source-provider, repeated holdouts", "Separates fit-to-used-stations from real independent skill."],
        ["Confidence contract", "Ranked low/high buckets and reliability status", "Prevents the packet from selling support as calibrated uncertainty."],
        ["Buddy rescue", "Gross-error candidate can survive if nearby stations agree", "Keeps real mesoscale extremes from being filtered as isolated bad obs."],
        ["Time weighting", "Observation age decays quality and inflates error", "Makes freshness a continuous trust input, not just a pass/fail gate."],
        ["Deduplication", "Cross-source duplicate station keys with best-observation retention", "Avoids double-counting the same station when sources overlap; products test passed."],
    ]

    confidence_semantic_rows = [
        ["support_index", "Untestable or insufficient ranked bucket coverage", "Local OI support metadata only."],
        ["uncalibrated_support", "Coverage exists but ranked high-confidence MAE is worse than low-confidence MAE", "Do not interpret confidence as uncertainty; use it as support/context."],
        ["calibrated_reliability", "High-confidence ranked bucket has lower or equal MAE and coverage is sufficient", "May be described as reliability-calibrated for that field/case."],
    ]

    source_table = []
    for row in source_rows:
        source_table.append(
            [
                short_source_name(row["source"]),
                row["raw"],
                row["profile_filtered"],
                row["time_filtered"],
                row["accepted"],
                fmt(row["mean_time_weight"]),
                fmt(row["max_age"], 1),
            ]
        )

    holdout_table = []
    for row in holdout_rows:
        holdout_table.append(
            [
                row["field"],
                fmt(row["raw"]),
                fmt(row["oi"]),
                fmt(row["barnes"]),
                fmt(row["oi_minus_raw"]),
                fmt(row["oi_minus_barnes"]),
            ]
        )

    confidence_table = []
    for row in confidence_rows:
        confidence_table.append(
            [
                row["field"],
                row["low_count"],
                row["high_count"],
                fmt(row["low_mae"]),
                fmt(row["high_mae"]),
                fmt(row["high_minus_low"]),
                row["status"],
                row["semantic_label"],
            ]
        )

    runtime_table = []
    for row in runtime_rows:
        runtime_table.append(
            [
                row["case"],
                row["oi_ms"],
                row["baseline_ms"],
                row["baseline_label"],
            ]
        )

    buddy_table = []
    for variable, stats in buddy_diag.items():
        buddy_table.append(
            [
                variable,
                fmt(stats.get("mean_candidate_observations"), 1),
                fmt(stats.get("mean_accepted_observations"), 1),
                stats.get("total_gross_error_rescued_observations"),
                stats.get("total_solver_failed_grid_cells"),
                stats.get("total_truncated_neighbor_grid_cells"),
            ]
        )

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_tex = DOCS_DIR / "mesoanalysis_after_barnes_report.tex"
    report_data = REPORT_DIR / "mesoanalysis_after_barnes_report_data.json"

    data_snapshot = {
        "generated_at": generated_at,
        "source_rows": source_rows,
        "holdout_rows": holdout_rows,
        "confidence_rows": confidence_rows,
        "runtime_rows": runtime_rows,
        "artifacts": artifact_rows,
        "run": run,
        "confidence_status": conf,
    }
    report_data.write_text(json.dumps(data_snapshot, indent=2), encoding="utf-8")

    tex = rf"""
\documentclass[11pt]{{article}}
\usepackage[margin=0.8in]{{geometry}}
\usepackage{{graphicx}}
\usepackage{{booktabs}}
\usepackage{{array}}
\usepackage{{longtable}}
\usepackage{{xcolor}}
\usepackage{{hyperref}}
\usepackage{{float}}
\usepackage{{caption}}
\hypersetup{{colorlinks=true,linkcolor=blue,urlcolor=blue}}
\newcolumntype{{L}}[1]{{>{{\raggedright\arraybackslash}}p{{#1}}}}
\setlength{{\parindent}}{{0pt}}
\setlength{{\parskip}}{{0.65em}}
\renewcommand{{\arraystretch}}{{1.18}}
\sloppy

\title{{\textbf{{From Barnes to Source-Aware OI}}\\RustWX Surface Mesoanalysis Research Report}}
\author{{Prepared for Drew / FahrenheitResearch}}
\date{{Generated {tex_escape(generated_at)}}}

\begin{{document}}
\maketitle

\begin{{abstract}}
This report explains what changed after the original Barnes interpolation prototype.
The short version: RustWX moved from a station-smoothing demo to a local, model-agnostic,
source-aware surface diagnostic correction and reliability layer. It now treats the
model as the dynamically assimilated background, applies bounded OI/kriging-like
increments to near-surface fields, and emits validation and confidence metadata for
agent packets. The remaining hard problem is not just faster math; it is proving,
case by case and source by source, when those corrections deserve trust.
\end{{abstract}}

\section{{Executive Summary}}

The first Barnes prototype answered: ``Can we spread model-minus-observation residuals
onto a grid?'' The current RustWX system asks a stricter question: ``Given a model
background, observation source quality, observation age, terrain/flow context, and
held-out validation, should this surface correction be trusted?'' That is the major
conceptual shift.

Implemented pieces now visible in local docs, code, tests, and smoke artifacts:
\begin{{itemize}}
  \item Barnes remains available as a baseline, but the main method is a local
  full-matrix optimal-interpolation solve with explicit background and observation
  errors.
  \item The correction layer is model-agnostic. HRRR, RAP, RRFS, GFS, or other grids
  are background providers; the product is not named around HRRR.
  \item Observation handling now includes source error profiles, freshness gates,
  time-representativeness weighting, and cross-source duplicate filtering.
  \item Covariance can be terrain/flow-aware: flow-aligned anisotropy and
  terrain-pressure damping reduce physically suspect spreading.
  \item Validation now includes station-hash, spatial-block, source-provider, and
  repeated holdout modes. Same-observation fit is no longer treated as proof of skill.
  \item Confidence is deliberately conservative. In the current packet, ranked
  confidence reliability failed for temperature, dewpoint, and wind, so confidence
  must be consumed as uncalibrated support metadata.
\end{{itemize}}

\section{{What This Layer Is, And Is Not}}

RustWX surface mesoanalysis is a \textbf{{post-processed near-surface diagnostic analysis}}.
It starts with a model background and applies bounded source-aware increments to fields
such as 2 m temperature, 2 m dewpoint, and 10 m wind. The layer can produce raw model
fields, residuals, corrected fields, confidence/support grids, validation summaries,
and compact agent packets.

It is \textbf{{not}} a full 3D data-assimilation system. Correcting 2 m temperature
after the model run does not automatically rebalance the PBL, pressure, soil state,
moisture profile, storm evolution, or derived severe-weather fields. Agent packets
should preserve that distinction.

\section{{RTMA / 3D-RTMA Distinction}}

NOAA's operational RTMA/URMA family is an operational analysis system. The EMC RTMA
overview describes RTMA/URMA as 2DVar analysis systems for NDFD parameters, with RTMA
run hourly for situational awareness and URMA run later to include late-arriving data.
The same EMC page describes RTMA as an hourly two-dimensional variational analysis of
surface sensible weather fields, generally at 2.5 km grid spacing, with operational
distribution latency. The NCO product inventory also lists RTMA/URMA GRIB2 product
paths and hourly/15-minute product families. The EMC graphics portal identifies
experimental 3D-RTMA/3D-URMA configurations that use HRRR or developmental RRFS first
guesses and additional observational components.

\begin{{table}}[H]
\centering
\caption{{RustWX surface layer vs. 3D-RTMA-style analysis}}
\begin{{tabular}}{{p{{0.28\linewidth}} p{{0.32\linewidth}} p{{0.32\linewidth}}}}
\toprule
Dimension & RustWX current layer & 3D-RTMA-style system \\
\midrule
Core role & Fast local diagnostic surface correction and reliability metadata & Full operational or experimental analysis cycle \\
Background & Model-agnostic: HRRR/RAP/RRFS/etc. & Specific operational/prototype first-guess systems \\
Vertical consistency & Near-surface post-processing only & 3D prototype aims at volumetric analysis behavior \\
Observation use & Source-aware correction layer with freshness, errors, validation & Mature operational DA/QC pipelines and monitored products \\
Strength & Local, reproducible, agent-readable, easy to run/tune by domain & Operational maturity, broad assimilation/QC, analysis-of-record lineage \\
Risk & Can double-count already assimilated obs or break physical consistency if over-trusted & Less local/custom/agent-native, constrained by operational products \\
\bottomrule
\end{{tabular}}
\end{{table}}

Official references checked: EMC RTMA/URMA overview
\url{{https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/rtma.php}},
NCO RTMA product inventory \url{{https://www.nco.ncep.noaa.gov/pmb/products/rtma/}},
and EMC RTMA/URMA graphics portal
\url{{https://www.emc.ncep.noaa.gov/mmb/rtma/rtma_graphics/main.php}}.

\section{{Architecture}}

\begin{{figure}}[H]
\centering
\includegraphics[width=\linewidth]{{target/reports/mesoanalysis_after_barnes_assets/architecture_flow.png}}
\caption{{High-level RustWX mesoanalysis flow. The product is a focused packet and artifact builder, not a giant raw dump.}}
\end{{figure}}

\section{{Feature Timeline}}

\begin{{longtable}}{{p{{0.18\linewidth}} p{{0.36\linewidth}} p{{0.36\linewidth}}}}
\caption{{What changed after Barnes interpolation}}\\
\toprule
Stage & What changed & Why it matters \\
\midrule
\endfirsthead
\toprule
Stage & What changed & Why it matters \\
\midrule
\endhead
{table_rows(timeline_rows)}
\bottomrule
\end{{longtable}}

\section{{Real Artifact Evidence}}

The following numbers come from local artifacts under \texttt{{target/}} and docs under
\texttt{{docs/}}. They are smoke/calibration evidence, not a multi-season climatology.

\subsection{{Observation Funnel}}

The time-weight packet used model \texttt{{{tex_escape(run_report.get("model"))}}},
cycle \texttt{{{tex_escape(run_report.get("model_cycle"))}}}, valid/reference time
\texttt{{{tex_escape(run_report.get("obs_reference_time"))}}}, max observation age
{tex_escape(run_report.get("max_obs_age_minutes"))} minutes, time half-life
{tex_escape(run_report.get("obs_time_weight_half_life_minutes"))} minutes, and max
time-error inflation factor {tex_escape(run_report.get("obs_max_time_error_inflation_factor"))}.

\begin{{figure}}[H]
\centering
\includegraphics[width=0.95\linewidth]{{target/reports/mesoanalysis_after_barnes_assets/source_counts.png}}
\caption{{Source counts from the time-weight smoke packet. This specific run accepted only the fresh METAR subset; the mesonet files were present but outside the time gate for that model valid time.}}
\end{{figure}}

\begin{{table}}[H]
\centering
\caption{{Observation source summary from packet}}
\footnotesize
\begin{{tabular}}{{lrrrrrr}}
\toprule
Source & Raw & Profile filt. & Time filt. & Accepted & Mean time wt. & Max age min \\
\midrule
{table_rows(source_table)}
\bottomrule
\end{{tabular}}
\end{{table}}

\subsection{{Holdout Skill}}

\begin{{figure}}[H]
\centering
\includegraphics[width=0.87\linewidth]{{target/reports/mesoanalysis_after_barnes_assets/holdout_mae.png}}
\caption{{Repeated holdout gate matrix. In this two-case smoke gate, RustWX OI beat both raw background and Barnes on the domain MAE checks for all three fields.}}
\end{{figure}}

\begin{{table}}[H]
\centering
\caption{{Repeated holdout MAE from \texttt{{repeated\_gate\_matrix.json}}}}
\begin{{tabular}}{{lrrrrr}}
\toprule
Field & Raw & OI & Barnes & OI-Raw & OI-Barnes \\
\midrule
{table_rows(holdout_table)}
\bottomrule
\end{{tabular}}
\end{{table}}

\subsection{{Confidence Reliability}}

The confidence contract is intentionally stricter than the grid confidence/support
field. A grid cell can have good local OI support and still fail held-out reliability.
For the current packet, the reliability summary is:
\textbf{{{tex_escape(conf.get("status"))}}}, semantic label
\texttt{{{tex_escape(conf.get("semantic_label"))}}}, passed fields
{tex_escape(conf.get("passed_field_count"))}/{tex_escape(conf.get("field_count"))}.

\begin{{figure}}[H]
\centering
\includegraphics[width=0.87\linewidth]{{target/reports/mesoanalysis_after_barnes_assets/confidence_reliability.png}}
\caption{{Ranked confidence reliability gate. The high-confidence bucket had higher MAE than the low-confidence bucket for the current station-hash holdout, so confidence is support metadata, not calibrated uncertainty.}}
\end{{figure}}

\begin{{table}}[H]
\centering
\caption{{Confidence reliability contract from packet and ranked station-hash matrix}}
\footnotesize
\begin{{tabular}}{{lrrrrlll}}
\toprule
Field & Low n & High n & Low MAE & High MAE & High-Low & Status & Label \\
\midrule
{table_rows(confidence_table)}
\bottomrule
\end{{tabular}}
\end{{table}}

\begin{{table}}[H]
\centering
\caption{{Confidence semantic labels}}
\begin{{tabular}}{{p{{0.22\linewidth}} p{{0.42\linewidth}} p{{0.26\linewidth}}}}
\toprule
Label & Gate meaning & Agent interpretation \\
\midrule
{table_rows(confidence_semantic_rows)}
\bottomrule
\end{{tabular}}
\end{{table}}

\subsection{{Runtime And Diagnostics}}

\begin{{figure}}[H]
\centering
\includegraphics[width=0.9\linewidth]{{target/reports/mesoanalysis_after_barnes_assets/runtime_smokes.png}}
\caption{{Selected smoke runtimes. These artifacts mix release and debug runs, so this is a sanity chart rather than a formal performance benchmark.}}
\end{{figure}}

\begin{{table}}[H]
\centering
\caption{{Selected smoke compute times}}
\begin{{tabular}}{{lrrl}}
\toprule
Case & OI ms & Baseline ms & Baseline \\
\midrule
{table_rows(runtime_table)}
\bottomrule
\end{{tabular}}
\end{{table}}

\begin{{table}}[H]
\centering
\caption{{Buddy-rescue diagnostics aggregate from live smoke}}
\footnotesize
\begin{{tabular}}{{lrrrrr}}
\toprule
Variable & Candidate obs & Accepted obs & Rescued & Solver failures & Truncated cells \\
\midrule
{table_rows(buddy_table)}
\bottomrule
\end{{tabular}}
\end{{table}}

The live buddy-rescue smoke did not need to rescue gross-error observations
(\texttt{{0}} rescues in the artifact), but the unit test covers the rescue path.
The point is to keep a supported mesoscale extreme from being rejected just because
it is large relative to the background.

\section{{What Is Real, Smoke-Tested, And Still Open}}

\textbf{{Implemented and tested locally:}} Barnes/OI method selection, local covariance
matrix solve, source-specific errors, time weighting, duplicate filtering, holdout
validation modes, confidence reliability contract, compact packet output, and the
buddy-rescue code path. The targeted products mesoanalysis suite passed in this
session.

\textbf{{Smoke-tested with real artifacts:}} HRRR/NOMADS background loading, real runner
observation files, station-hash/spatial/source holdouts, repeated holdout gates,
covariance ablation comparisons, time-weighted observation loading, and compact agent
packet output.

\textbf{{Still research/engineering gaps:}} multi-case climatological calibration,
per-source rolling innovation stats, external RTMA/URMA comparisons at scale,
source siting metadata, land/water/coastline barriers, advected boundary logic,
posterior uncertainty calibration, and full human map sets for domains like Oklahoma
or the Plains. This is why the packet should not call confidence calibrated uncertainty
unless the ranked holdout reliability gate actually passes.

\section{{Files And Artifacts Used}}

\begin{{longtable}}{{L{{0.27\linewidth}} L{{0.67\linewidth}}}}
\toprule
Artifact & Path \\
\midrule
\endfirsthead
\toprule
Artifact & Path \\
\midrule
\endhead
\footnotesize
{artifact_table_rows(artifact_rows)}
\bottomrule
\end{{longtable}}

\section{{Bottom Line For Drew}}

After Barnes, the project became less about drawing a smoother map and more about
earning trust. The system now has the bones of a professional surface diagnostic
analysis layer: explicit errors, source quality, covariance choices, validation gates,
and machine-readable reliability semantics. That is exactly the road toward a
DESI/RTMA-caliber sidekick packet. The next hill is calibration across many cases and
source regimes, not just adding another interpolation formula.

\end{{document}}
"""
    report_tex.write_text(tex, encoding="utf-8")
    return report_tex, data_snapshot


if __name__ == "__main__":
    tex_path, snapshot = build_report()
    print(f"Wrote {tex_path}")
    print(f"Wrote {REPORT_DIR / 'mesoanalysis_after_barnes_report_data.json'}")
    print(f"Wrote charts under {ASSET_DIR}")
    print(json.dumps({"generated_at": snapshot["generated_at"]}, indent=2))
