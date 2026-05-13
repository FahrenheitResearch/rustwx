#!/usr/bin/env python3
"""Emit Py-ART velocity continuity baselines for pinned NEXRAD fixtures.

This script is intentionally optional: rustwx's radar implementation remains
Rust-native, while this provides an external open-science comparison artifact
when Py-ART is available in the local research environment.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from typing import Any

import numpy as np


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare raw and Py-ART region-based velocity continuity stats"
    )
    parser.add_argument("--input", required=True, type=pathlib.Path)
    parser.add_argument("--sweep-index", type=int)
    parser.add_argument(
        "--fixed-angle-deg",
        type=float,
        help="Select the finite velocity sweep nearest this fixed angle.",
    )
    parser.add_argument(
        "--min-finite-gates",
        type=int,
        default=1,
        help="Minimum finite gates required before a sweep is usable.",
    )
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument("--vel-field", default="velocity")
    parser.add_argument("--dealias-field", default="pyart_region_based_velocity")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        import pyart
    except ImportError as exc:
        raise SystemExit(
            "Py-ART is not installed; install arm_pyart to generate external radar baselines"
        ) from exc

    radar = pyart.io.read_nexrad_archive(str(args.input))
    sweep_resolution = resolve_sweep_index(
        radar,
        args.vel_field,
        args.sweep_index,
        args.fixed_angle_deg,
        args.min_finite_gates,
    )
    sweep_index = sweep_resolution["selected_sweep_index"]
    raw = radar.fields[args.vel_field]["data"]
    dealiased = pyart.correct.dealias_region_based(
        radar,
        vel_field=args.vel_field,
        keep_original=False,
    )["data"]

    start = int(radar.sweep_start_ray_index["data"][sweep_index])
    end = int(radar.sweep_end_ray_index["data"][sweep_index])
    fixed_angle = float(radar.fixed_angle["data"][sweep_index])
    nyquist = sweep_nyquist(radar, start, end)
    azimuths = np.asarray(radar.azimuth["data"][start : end + 1], dtype=np.float32)

    summary = {
        "ok": True,
        "tool": "pyart",
        "pyart_version": getattr(pyart, "__version__", "unknown"),
        "source": str(args.input),
        "requested_sweep_index": args.sweep_index,
        "requested_fixed_angle_deg": args.fixed_angle_deg,
        "sweep_index": sweep_index,
        "sweep_resolution": sweep_resolution,
        "fixed_angle_deg": fixed_angle,
        "nyquist_ms": nyquist,
        "methods": [
            method_summary("raw", raw[start : end + 1], azimuths, nyquist),
            method_summary(
                "pyart_region_based",
                dealiased[start : end + 1],
                azimuths,
                nyquist,
            ),
        ],
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


def resolve_sweep_index(
    radar: Any,
    field: str,
    requested_sweep_index: int | None,
    requested_fixed_angle: float | None,
    min_finite_gates: int,
) -> dict[str, Any]:
    if field not in radar.fields:
        raise SystemExit(
            f"Field {field!r} was not found. Available fields: {sorted(radar.fields)}"
        )

    sweeps = []
    for index in range(radar.nsweeps):
        sweeps.append(sweep_field_summary(radar, field, index))

    target_angle = requested_fixed_angle
    requested = None
    if requested_sweep_index is not None:
        if requested_sweep_index < 0 or requested_sweep_index >= radar.nsweeps:
            raise SystemExit(
                f"Requested sweep {requested_sweep_index} is outside 0..{radar.nsweeps - 1}"
            )
        requested = sweeps[requested_sweep_index]
        if requested["finite_gate_count"] >= min_finite_gates:
            return {
                "selected_sweep_index": requested_sweep_index,
                "reason": "requested_sweep_has_velocity",
                "min_finite_gates": min_finite_gates,
                "requested_sweep": requested,
            }
        if target_angle is None:
            target_angle = requested["fixed_angle_deg"]

    candidates = [
        sweep
        for sweep in sweeps
        if sweep["finite_gate_count"] >= min_finite_gates
    ]
    if not candidates:
        raise SystemExit(
            f"No sweep has at least {min_finite_gates} finite gates for field {field!r}"
        )

    if target_angle is None:
        selected = max(candidates, key=lambda sweep: sweep["finite_gate_count"])
        reason = "max_finite_gate_velocity_sweep"
    else:
        selected = min(
            candidates,
            key=lambda sweep: (
                abs(sweep["fixed_angle_deg"] - target_angle),
                -sweep["finite_gate_count"],
                sweep["sweep_index"],
            ),
        )
        reason = "nearest_fixed_angle_with_velocity"

    return {
        "selected_sweep_index": selected["sweep_index"],
        "reason": reason,
        "min_finite_gates": min_finite_gates,
        "target_fixed_angle_deg": target_angle,
        "requested_sweep": requested,
        "selected_sweep": selected,
        "velocity_sweeps": candidates,
    }


def sweep_field_summary(radar: Any, field: str, sweep_index: int) -> dict[str, Any]:
    start = int(radar.sweep_start_ray_index["data"][sweep_index])
    end = int(radar.sweep_end_ray_index["data"][sweep_index])
    values = radar.fields[field]["data"][start : end + 1]
    grid = masked_to_nan(values)
    return {
        "sweep_index": sweep_index,
        "fixed_angle_deg": float(radar.fixed_angle["data"][sweep_index]),
        "ray_count": end - start + 1,
        "finite_gate_count": int(np.isfinite(grid).sum()),
    }


def sweep_nyquist(radar: Any, start: int, end: int) -> float:
    try:
        values = np.asarray(
            radar.instrument_parameters["nyquist_velocity"]["data"][start : end + 1],
            dtype=np.float32,
        )
    except KeyError:
        return float("nan")
    values = values[np.isfinite(values)]
    if values.size == 0:
        return float("nan")
    return float(np.median(values))


def method_summary(
    label: str,
    values: np.ndarray,
    azimuths: np.ndarray,
    nyquist: float,
) -> dict[str, Any]:
    grid = masked_to_nan(values)
    if azimuths.shape[0] == grid.shape[0]:
        order = np.argsort(azimuths)
        grid = grid[order]
        azimuths = azimuths[order]

    finite_gate_count = int(np.isfinite(grid).sum())
    fold_threshold = nyquist
    severe_threshold = nyquist * 1.5
    radial_pairs = pair_stats_along_radials(grid, fold_threshold, severe_threshold)
    azimuth_pairs = pair_stats_along_azimuths(
        grid, azimuths, fold_threshold, severe_threshold
    )
    pair_count = radial_pairs["pair_count"] + azimuth_pairs["pair_count"]
    fold_like = radial_pairs["fold_like"] + azimuth_pairs["fold_like"]
    severe = radial_pairs["severe"] + azimuth_pairs["severe"]
    max_jump = max(radial_pairs["max_jump"], azimuth_pairs["max_jump"])

    return {
        "method": label,
        "finite_gate_count": finite_gate_count,
        "radial_pair_count": radial_pairs["pair_count"],
        "azimuth_pair_count": azimuth_pairs["pair_count"],
        "fold_like_jump_count": fold_like,
        "severe_jump_count": severe,
        "fold_like_jump_fraction": (fold_like / pair_count) if pair_count else 0.0,
        "max_abs_jump_ms": max_jump,
    }


def masked_to_nan(values: np.ndarray) -> np.ndarray:
    if np.ma.isMaskedArray(values):
        return values.astype(np.float32).filled(np.nan)
    return np.asarray(values, dtype=np.float32)


def pair_stats_along_radials(
    grid: np.ndarray, fold_threshold: float, severe_threshold: float
) -> dict[str, Any]:
    left = grid[:, :-1]
    right = grid[:, 1:]
    return pair_stats(left, right, fold_threshold, severe_threshold)


def pair_stats_along_azimuths(
    grid: np.ndarray,
    azimuths: np.ndarray,
    fold_threshold: float,
    severe_threshold: float,
) -> dict[str, Any]:
    total = {"pair_count": 0, "fold_like": 0, "severe": 0, "max_jump": 0.0}
    rows = grid.shape[0]
    if rows < 2:
        return total
    for row in range(rows):
        next_row = 0 if row + 1 == rows else row + 1
        span = azimuth_span(float(azimuths[row]), float(azimuths[next_row]))
        if span > 10.0:
            continue
        stats = pair_stats(
            grid[row : row + 1, :],
            grid[next_row : next_row + 1, :],
            fold_threshold,
            severe_threshold,
        )
        total["pair_count"] += stats["pair_count"]
        total["fold_like"] += stats["fold_like"]
        total["severe"] += stats["severe"]
        total["max_jump"] = max(total["max_jump"], stats["max_jump"])
    return total


def pair_stats(
    left: np.ndarray,
    right: np.ndarray,
    fold_threshold: float,
    severe_threshold: float,
) -> dict[str, Any]:
    finite = np.isfinite(left) & np.isfinite(right)
    if not finite.any():
        return {"pair_count": 0, "fold_like": 0, "severe": 0, "max_jump": 0.0}
    jumps = np.abs(left[finite] - right[finite])
    return {
        "pair_count": int(jumps.size),
        "fold_like": int((jumps > fold_threshold).sum()),
        "severe": int((jumps > severe_threshold).sum()),
        "max_jump": float(jumps.max()) if jumps.size else 0.0,
    }


def azimuth_span(lo: float, hi: float) -> float:
    span = hi - lo
    if span < 0:
        span += 360.0
    return span


if __name__ == "__main__":
    sys.exit(main())
