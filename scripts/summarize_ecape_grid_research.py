#!/usr/bin/env python3
"""Summarize full-grid HRRR ECAPE research JSON reports.

The Rust grid research binary writes one JSON report per HRRR hour. This helper
turns a directory of those reports into compact CSV and Markdown tables so the
research can compare broad plume coverage across many cases.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


DEFAULT_MASKS = [
    "ml_cape_ge_500",
    "ml_ecape_ge_500",
    "ml_cape_ge_1000",
    "ml_ecape_ge_1000",
    "ml_ecape_ehi03_ge_1",
    "ml_ecape_ehi03_ge_2",
    "ml_ecape_ehi03_ge_3",
    "warm_sector_combo",
    "high_end_ecape_ehi_combo",
    "ratio_low_cape_plume",
    "ratio_high_cape_plume",
]

DEFAULT_FIELDS = [
    "ml_ecape",
    "ml_cape_undiluted",
    "ml_ecape_cape_ratio",
    "srh_0_3km",
    "shear_0_6km",
    "ml_ecape_ehi_0_3km",
]


def fmt_pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{100.0 * value:.1f}"


def fmt_num(value: float | None) -> str:
    if value is None:
        return ""
    if abs(value) >= 1000:
        return f"{value:.0f}"
    if abs(value) >= 100:
        return f"{value:.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.3f}"


def case_id(report: dict[str, Any]) -> str:
    request = report["request"]
    return (
        f"{request['date_yyyymmdd']}_"
        f"{int(request['cycle_utc']):02d}z_"
        f"f{int(request['forecast_hour']):03d}_"
        f"{request['domain_slug']}"
    )


def load_reports(input_dir: Path) -> list[dict[str, Any]]:
    reports = []
    for path in sorted(input_dir.glob("gridstats_*.json")):
        report = json.loads(path.read_text(encoding="utf-8"))
        report["_path"] = str(path)
        reports.append(report)
    return reports


def mask_map(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {mask["name"]: mask for mask in report["masks"]}


def field_map(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {field["name"]: field for field in report["fields"]}


def write_case_csv(reports: list[dict[str, Any]], output: Path) -> None:
    headers = [
        "case",
        "date",
        "cycle_utc",
        "forecast_hour",
        "cells",
        "total_s",
        "failures",
    ]
    headers += [f"{name}_pct" for name in DEFAULT_MASKS]
    headers += [f"{name}_largest_component_pct" for name in DEFAULT_MASKS]
    headers += [f"{name}_largest_component_of_mask_pct" for name in DEFAULT_MASKS]
    for field in DEFAULT_FIELDS:
        headers += [
            f"{field}_median",
            f"{field}_p90",
            f"{field}_p95",
            f"{field}_max",
        ]

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for report in reports:
            request = report["request"]
            row: dict[str, Any] = {
                "case": case_id(report),
                "date": request["date_yyyymmdd"],
                "cycle_utc": request["cycle_utc"],
                "forecast_hour": request["forecast_hour"],
                "cells": report["grid"]["cells"],
                "total_s": report["timing"]["total_ms"] / 1000.0,
                "failures": report["failure_count"],
            }
            masks = mask_map(report)
            for name in DEFAULT_MASKS:
                row[f"{name}_pct"] = (
                    masks[name]["fraction"] * 100.0 if name in masks else ""
                )
                component = masks.get(name, {}).get("largest_component", {})
                row[f"{name}_largest_component_pct"] = (
                    component["fraction"] * 100.0 if "fraction" in component else ""
                )
                row[f"{name}_largest_component_of_mask_pct"] = (
                    component["fraction_of_mask"] * 100.0
                    if component.get("fraction_of_mask") is not None
                    else ""
                )
            fields = field_map(report)
            for name in DEFAULT_FIELDS:
                stats = fields[name]["all"] if name in fields else {}
                for stat_name in ["median", "p90", "p95", "max"]:
                    row[f"{name}_{stat_name}"] = stats.get(stat_name, "")
            writer.writerow(row)


def write_long_csv(reports: list[dict[str, Any]], output: Path) -> None:
    headers = [
        "case",
        "field",
        "mask",
        "units",
        "count",
        "min",
        "p10",
        "p25",
        "median",
        "mean",
        "p75",
        "p90",
        "p95",
        "p99",
        "max",
    ]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for report in reports:
            cid = case_id(report)
            for field in report["fields"]:
                for mask_name, stats in [("all", field["all"])] + [
                    (masked["mask"], masked["stats"])
                    for masked in field["by_mask"]
                ]:
                    row = {
                        "case": cid,
                        "field": field["name"],
                        "mask": mask_name,
                        "units": field["units"],
                    }
                    row.update(stats)
                    writer.writerow(row)


def write_markdown(reports: list[dict[str, Any]], output: Path) -> None:
    lines: list[str] = []
    lines.append("# Full-Grid ECAPE Research Summary")
    lines.append("")
    lines.append(
        "Each row is one HRRR forecast hour over the same broad Gulf-to-Kansas "
        "warm-sector domain. Mask columns are percent of the full cropped grid."
    )
    lines.append("")
    lines.append("## Plume Coverage")
    lines.append("")
    lines.append(
        "| Case | Cells | Total s | ML CAPE >=500 | ML ECAPE >=500 | "
        "ML ECAPE-EHI 0-3 >=1 | >=2 | >=3 | Warm sector combo | "
        "High-end combo | Ratio <0.75 | Ratio >=1 |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    for report in reports:
        masks = mask_map(report)
        values = [
            case_id(report),
            str(report["grid"]["cells"]),
            f"{report['timing']['total_ms'] / 1000.0:.1f}",
            fmt_pct(masks.get("ml_cape_ge_500", {}).get("fraction")),
            fmt_pct(masks.get("ml_ecape_ge_500", {}).get("fraction")),
            fmt_pct(masks.get("ml_ecape_ehi03_ge_1", {}).get("fraction")),
            fmt_pct(masks.get("ml_ecape_ehi03_ge_2", {}).get("fraction")),
            fmt_pct(masks.get("ml_ecape_ehi03_ge_3", {}).get("fraction")),
            fmt_pct(masks.get("warm_sector_combo", {}).get("fraction")),
            fmt_pct(masks.get("high_end_ecape_ehi_combo", {}).get("fraction")),
            fmt_pct(masks.get("ratio_low_cape_plume", {}).get("fraction")),
            fmt_pct(masks.get("ratio_high_cape_plume", {}).get("fraction")),
        ]
        lines.append("| " + " | ".join(values) + " |")

    lines.append("")
    lines.append("## Largest Continuous Plumes")
    lines.append("")
    lines.append(
        "| Case | ML CAPE >=500 largest | ML ECAPE >=500 largest | "
        "ML ECAPE-EHI 0-3 >=1 largest | >=2 largest | >=3 largest | "
        "High-end combo largest |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for report in reports:
        masks = mask_map(report)

        def comp_pct(name: str) -> str:
            component = masks.get(name, {}).get("largest_component", {})
            return fmt_pct(component.get("fraction"))

        lines.append(
            "| "
            + " | ".join(
                [
                    case_id(report),
                    comp_pct("ml_cape_ge_500"),
                    comp_pct("ml_ecape_ge_500"),
                    comp_pct("ml_ecape_ehi03_ge_1"),
                    comp_pct("ml_ecape_ehi03_ge_2"),
                    comp_pct("ml_ecape_ehi03_ge_3"),
                    comp_pct("high_end_ecape_ehi_combo"),
                ]
            )
            + " |"
        )

    lines.append("")
    lines.append("## Domain-Wide Percentiles")
    lines.append("")
    lines.append(
        "| Case | ML ECAPE med/p90 | ML CAPE med/p90 | ECAPE/CAPE ratio med/p90 | "
        "0-3 SRH med/p90 | 0-6 shear med/p90 | ML ECAPE-EHI 0-3 med/p90 |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for report in reports:
        fields = field_map(report)

        def med_p90(name: str) -> str:
            stats = fields[name]["all"]
            return f"{fmt_num(stats.get('median'))}/{fmt_num(stats.get('p90'))}"

        lines.append(
            "| "
            + " | ".join(
                [
                    case_id(report),
                    med_p90("ml_ecape"),
                    med_p90("ml_cape_undiluted"),
                    med_p90("ml_ecape_cape_ratio"),
                    med_p90("srh_0_3km"),
                    med_p90("shear_0_6km"),
                    med_p90("ml_ecape_ehi_0_3km"),
                ]
            )
            + " |"
        )

    lines.append("")
    lines.append("## Early Lessons")
    lines.append("")
    lines.append(
        "- The broad-plume view changes the question from whether a point is "
        "favorable to how much of the warm sector is favorable."
    )
    lines.append(
        "- ML ECAPE coverage can be close to ML CAPE coverage in rich warm-season "
        "plumes, but the ECAPE/CAPE ratio still highlights where entrainment or "
        "kinematic effects reshape the instability field."
    )
    lines.append(
        "- Cool-season/high-shear cases can show large ECAPE/CAPE ratios even when "
        "raw CAPE coverage is small, so ratio maps need to be interpreted with "
        "CAPE magnitude contours or a CAPE mask."
    )
    lines.append(
        "- Area fractions for ECAPE-EHI thresholds look promising as event-scale "
        "predictors because they encode plume size, not only local intensity."
    )
    lines.append(
        "- Largest-component fractions are the cleaner metric for continuous "
        "Gulf-to-Plains plumes because they separate one connected corridor from "
        "small disconnected favorable pockets."
    )
    lines.append("")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    reports = load_reports(args.input_dir)
    if not reports:
        raise SystemExit(f"no gridstats_*.json files found in {args.input_dir}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_case_csv(reports, args.out_dir / "grid_case_summary.csv")
    write_long_csv(reports, args.out_dir / "grid_field_mask_stats_long.csv")
    write_markdown(reports, args.out_dir / "grid_research_summary.md")


if __name__ == "__main__":
    main()
