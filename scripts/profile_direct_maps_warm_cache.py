#!/usr/bin/env python3
"""Profile direct map products one at a time from a warm RustWx cache.

The script uses the current product catalog as source of truth, warms the exact
selected direct-product set once, then runs each product in a separate
`direct_batch` process. It records the CLI wall time plus the detailed
per-product timing JSON emitted by the Rust pipeline.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def today_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def slug_filename(slug: str, index: int) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in slug)
    return f"{index:03d}_{cleaned}"


def run_command(
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=stdout,
            stderr=stderr,
            text=True,
        )
    return {
        "returncode": proc.returncode,
        "wall_ms": round((time.perf_counter() - started) * 1000.0),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }


def write_status(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps({**payload, "updated_at": now_iso()}, indent=2), encoding="utf-8")


def build_binaries(repo: Path, cargo_profile: str, logs_dir: Path) -> None:
    cmd = [
        "cargo",
        "build",
        "-p",
        "rustwx-cli",
        "--bin",
        "direct_batch",
        "--bin",
        "product_catalog",
    ]
    if cargo_profile == "release":
        cmd.append("--release")
    result = run_command(
        cmd,
        cwd=repo,
        env=os.environ.copy(),
        stdout_path=logs_dir / "cargo_build.out.log",
        stderr_path=logs_dir / "cargo_build.err.log",
    )
    if result["returncode"] != 0:
        raise RuntimeError(
            "cargo build failed; see "
            f"{result['stdout_path']} and {result['stderr_path']}"
        )


def binary_path(repo: Path, cargo_profile: str, name: str) -> Path:
    exe = f"{name}.exe" if os.name == "nt" else name
    profile_dir = "release" if cargo_profile == "release" else "debug"
    return repo / "target" / profile_dir / exe


def load_catalog(repo: Path, product_catalog: Path, catalog_path: Path, logs_dir: Path) -> dict[str, Any]:
    result = run_command(
        [str(product_catalog), "--out", str(catalog_path)],
        cwd=repo,
        env=os.environ.copy(),
        stdout_path=logs_dir / "product_catalog.out.log",
        stderr_path=logs_dir / "product_catalog.err.log",
    )
    if result["returncode"] != 0:
        raise RuntimeError(
            "product_catalog failed; see "
            f"{result['stdout_path']} and {result['stderr_path']}"
        )
    return json.loads(catalog_path.read_text(encoding="utf-8"))


def selected_direct_products(
    catalog: dict[str, Any],
    *,
    model: str,
    only: list[str],
    skip: set[str],
    limit: int | None,
) -> list[dict[str, Any]]:
    wanted = {slug.strip() for slug in only if slug.strip()}
    rows: list[dict[str, Any]] = []
    for entry in catalog.get("direct", []):
        slug = entry.get("slug")
        if not isinstance(slug, str) or slug in skip:
            continue
        if wanted and slug not in wanted:
            continue
        support = None
        for target in entry.get("support", []):
            if target.get("target") == model and target.get("status") == "supported":
                support = target
                break
        if support is None:
            continue
        rows.append(
            {
                "slug": slug,
                "title": entry.get("title"),
                "status": entry.get("status"),
                "maturity": entry.get("maturity"),
                "render_style": entry.get("render_style"),
                "fetch_mode": support.get("fetch_mode"),
                "grib_product": support.get("grib_product"),
                "source_routes": support.get("source_routes", []),
            }
        )
    if wanted:
        missing = sorted(wanted - {row["slug"] for row in rows})
        if missing:
            raise RuntimeError(
                f"{len(missing)} requested products are not supported for {model}: {missing}"
            )
    if limit is not None:
        rows = rows[:limit]
    return rows


def direct_batch_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    env["RUSTWX_PLOT_STYLE"] = args.plot_style
    env["RUSTWX_STATIC_OUTPUT_WIDTH"] = str(args.width)
    env["RUSTWX_STATIC_OUTPUT_HEIGHT"] = str(args.height)
    env["RUSTWX_RENDER_THREADS"] = "1"
    env.setdefault("RUSTWX_PRESSURE_CONTOUR_SMOOTH_PASSES", "0")
    for name in ("RUSTWX_PROJECTED_FRAME_SOURCE", "RUSTWX_DOMAIN_CROP_PAD_CELLS"):
        env.pop(name, None)
    return env


def direct_batch_base_args(args: argparse.Namespace, out_dir: Path, cache_dir: Path) -> list[str]:
    cmd = [
        str(args.direct_batch_bin),
        "--model",
        args.model,
        "--date",
        args.date,
        "--forecast-hour",
        str(args.forecast_hour),
        "--source",
        args.source,
        "--region",
        args.region,
        "--out-dir",
        str(out_dir),
        "--cache-dir",
        str(cache_dir),
        "--png-compression",
        args.png_compression,
        "--place-label-density",
        str(args.place_label_density),
    ]
    if args.cycle is not None:
        cmd.extend(["--cycle", str(args.cycle)])
    if args.bounds:
        cmd.extend(["--bounds", args.bounds])
    if args.domain_slug:
        cmd.extend(["--domain-slug", args.domain_slug])
    if args.country:
        cmd.extend(["--country", args.country])
    if args.contour_mode:
        cmd.extend(["--contour-mode", args.contour_mode])
    if args.native_fill_level_multiplier != 1:
        cmd.extend(["--native-fill-level-multiplier", str(args.native_fill_level_multiplier)])
    return cmd


def find_single_timing(out_dir: Path) -> Path | None:
    timings = sorted(out_dir.glob("*_direct_timing.json"))
    return timings[-1] if timings else None


def read_timing(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        with open(filesystem_path(path), encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None


def filesystem_path(path: Path) -> str:
    """Return a path string that survives long Windows product filenames."""
    if os.name != "nt":
        return str(path)
    resolved = path.resolve()
    text = str(resolved)
    if text.startswith("\\\\?\\"):
        return text
    if text.startswith("\\\\"):
        return "\\\\?\\UNC\\" + text[2:]
    return "\\\\?\\" + text


def nested_get(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def first_recipe_record(timing: dict[str, Any] | None) -> dict[str, Any] | None:
    if not timing:
        return None
    recipes = timing.get("recipes")
    if not isinstance(recipes, list) or not recipes:
        return None
    first = recipes[0]
    return first if isinstance(first, dict) else None


def cache_warm_ok(timing: dict[str, Any] | None) -> bool:
    if not timing:
        return False
    fetches = timing.get("fetches")
    if not isinstance(fetches, list) or not fetches:
        return False
    for fetch in fetches:
        if not isinstance(fetch, dict):
            return False
        if not fetch.get("fetch_cache_hit"):
            return False
        if int(fetch.get("extract_cache_misses") or 0) != 0:
            return False
    return True


def summarize_fetches(timing: dict[str, Any] | None) -> dict[str, Any]:
    summary = {
        "fetch_count": 0,
        "fetch_cache_hits": 0,
        "extract_cache_hits": 0,
        "extract_cache_misses": 0,
        "fetch_ms": 0,
        "extract_ms": 0,
        "fetch_total_ms": 0,
        "fetch_bytes": 0,
    }
    if not timing:
        return summary
    for fetch in timing.get("fetches") or []:
        if not isinstance(fetch, dict):
            continue
        summary["fetch_count"] += 1
        if fetch.get("fetch_cache_hit"):
            summary["fetch_cache_hits"] += 1
        summary["extract_cache_hits"] += int(fetch.get("extract_cache_hits") or 0)
        summary["extract_cache_misses"] += int(fetch.get("extract_cache_misses") or 0)
        summary["fetch_ms"] += int(fetch.get("fetch_ms") or 0)
        summary["extract_ms"] += int(fetch.get("extract_ms") or 0)
        summary["fetch_total_ms"] += int(fetch.get("total_ms") or 0)
        input_fetch = fetch.get("input_fetch") if isinstance(fetch.get("input_fetch"), dict) else {}
        summary["fetch_bytes"] += int(input_fetch.get("bytes_len") or 0)
    return summary


TIMING_FIELDS = [
    "total_ms",
    "render_ms",
    "render_to_image_ms",
    "request_build_ms",
    "field_prepare_ms",
    "contour_prepare_ms",
    "barb_prepare_ms",
    "project_ms",
    "render_state_prep_ms",
    "data_layer_draw_ms",
    "overlay_draw_ms",
    "png_encode_ms",
    "file_write_ms",
    "image_timing.total_ms",
    "image_timing.background_ms",
    "image_timing.polygon_fill_ms",
    "image_timing.projected_pixel_ms",
    "image_timing.rasterize_ms",
    "image_timing.raster_blit_ms",
    "image_timing.linework_ms",
    "image_timing.contour_ms",
    "image_timing.contour_bucket_ms",
    "image_timing.contour_extrema_ms",
    "image_timing.contour_label_draw_ms",
    "image_timing.contour_segment_count",
    "image_timing.barb_ms",
    "image_timing.chrome_ms",
    "image_timing.postprocess_ms",
]


def product_record(
    *,
    product: dict[str, Any],
    index: int,
    command_result: dict[str, Any],
    timing_path: Path | None,
    out_dir: Path,
) -> dict[str, Any]:
    timing = read_timing(timing_path)
    recipe = first_recipe_record(timing)
    fetch_summary = summarize_fetches(timing)
    record: dict[str, Any] = {
        "index": index,
        "slug": product["slug"],
        "title": product.get("title"),
        "model": timing.get("model") if timing else None,
        "date": timing.get("date") if timing else None,
        "cycle_utc": timing.get("cycle_utc") if timing else None,
        "forecast_hour": timing.get("forecast_hour") if timing else None,
        "catalog_status": product.get("status"),
        "maturity": product.get("maturity"),
        "render_style": product.get("render_style"),
        "fetch_mode": product.get("fetch_mode"),
        "grib_product": product.get("grib_product"),
        "returncode": command_result["returncode"],
        "ok": bool(command_result["returncode"] == 0 and recipe),
        "warm_cache_ok": cache_warm_ok(timing),
        "wall_ms": command_result["wall_ms"],
        "out_dir": str(out_dir),
        "timing_path": str(timing_path) if timing_path else None,
        "stdout_path": command_result["stdout_path"],
        "stderr_path": command_result["stderr_path"],
        "output_path": recipe.get("output_path") if recipe else None,
        "blockers": timing.get("blockers", []) if timing else None,
    }
    record.update(fetch_summary)
    timing_ms = recipe.get("timing_ms") if isinstance(recipe, dict) else None
    if isinstance(timing_ms, dict):
        for field in TIMING_FIELDS:
            record[field.replace(".", "_")] = nested_get(timing_ms, field)
    else:
        for field in TIMING_FIELDS:
            record[field.replace(".", "_")] = None
    return record


def write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    fieldnames = [
        "index",
        "slug",
        "title",
        "ok",
        "warm_cache_ok",
        "returncode",
        "wall_ms",
        "total_ms",
        "render_ms",
        "render_to_image_ms",
        "request_build_ms",
        "fetch_total_ms",
        "fetch_ms",
        "extract_ms",
        "fetch_cache_hits",
        "extract_cache_hits",
        "extract_cache_misses",
        "data_layer_draw_ms",
        "overlay_draw_ms",
        "image_timing_contour_ms",
        "image_timing_contour_bucket_ms",
        "image_timing_contour_extrema_ms",
        "image_timing_contour_segment_count",
        "image_timing_rasterize_ms",
        "image_timing_projected_pixel_ms",
        "image_timing_barb_ms",
        "png_encode_ms",
        "fetch_mode",
        "grib_product",
        "render_style",
        "output_path",
        "timing_path",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def markdown_summary(summary: dict[str, Any]) -> str:
    records = summary["records"]
    ok_records = [row for row in records if row.get("ok")]
    failed = [row for row in records if not row.get("ok")]
    not_warm = [row for row in ok_records if not row.get("warm_cache_ok")]
    slowest = sorted(ok_records, key=lambda row: int(row.get("total_ms") or 0), reverse=True)[:20]
    contour_heavy = sorted(
        ok_records, key=lambda row: int(row.get("image_timing_contour_ms") or 0), reverse=True
    )[:20]

    lines = [
        "# Direct Map Warm-Cache Product Profile",
        "",
        f"- Generated: {summary['generated_at']}",
        f"- Model/source/run: `{summary['model']} {summary['source']} {summary['date']} {summary['cycle_label']} F{summary['forecast_hour']:03d}`",
        f"- Domain: `{summary['region']}`",
        f"- Products selected: {summary['product_count']}",
        f"- Successful product renders: {len(ok_records)}",
        f"- Failed or blocked product renders: {len(failed)}",
        f"- Warm-cache verified renders: {len(ok_records) - len(not_warm)} / {len(ok_records)}",
        f"- Output root: `{summary['out_root']}`",
        f"- CSV: `{summary['csv_path']}`",
        f"- JSON: `{summary['json_path']}`",
        "",
        "## Slowest Total Product Runs",
        "",
        "| Rank | Product | Total ms | Wall ms | Render ms | Fetch total ms | Warm |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(slowest, 1):
        lines.append(
            "| {rank} | `{slug}` | {total} | {wall} | {render} | {fetch} | {warm} |".format(
                rank=rank,
                slug=row["slug"],
                total=row.get("total_ms") or "-",
                wall=row.get("wall_ms") or "-",
                render=row.get("render_ms") or "-",
                fetch=row.get("fetch_total_ms") or "-",
                warm=str(row.get("warm_cache_ok")),
            )
        )
    lines.extend(
        [
            "",
            "## Slowest Contour Draws",
            "",
            "| Rank | Product | Contour ms | Extrema ms | Buckets ms | Segments | Total ms |",
            "|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for rank, row in enumerate(contour_heavy, 1):
        lines.append(
            "| {rank} | `{slug}` | {contour} | {extrema} | {bucket} | {segments} | {total} |".format(
                rank=rank,
                slug=row["slug"],
                contour=row.get("image_timing_contour_ms") or 0,
                extrema=row.get("image_timing_contour_extrema_ms") or 0,
                bucket=row.get("image_timing_contour_bucket_ms") or 0,
                segments=row.get("image_timing_contour_segment_count") or 0,
                total=row.get("total_ms") or "-",
            )
        )
    if failed:
        lines.extend(["", "## Failed Or Blocked", "", "| Product | Return | Blockers |", "|---|---:|---|"])
        for row in failed:
            blockers = row.get("blockers")
            lines.append(f"| `{row['slug']}` | {row.get('returncode')} | `{blockers}` |")
    if not_warm:
        lines.extend(["", "## Warm-Cache Misses", "", "| Product | Fetch hits | Extract misses |", "|---|---:|---:|"])
        for row in not_warm:
            lines.append(
                f"| `{row['slug']}` | {row.get('fetch_cache_hits')} | {row.get('extract_cache_misses')} |"
            )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--model", default="hrrr")
    parser.add_argument("--source", default="aws")
    parser.add_argument("--date", default=today_yyyymmdd())
    parser.add_argument("--cycle", type=int, default=0)
    parser.add_argument("--forecast-hour", type=int, default=0)
    parser.add_argument("--region", default="midwest")
    parser.add_argument("--bounds")
    parser.add_argument("--domain-slug")
    parser.add_argument("--country")
    parser.add_argument("--width", type=int, default=1600)
    parser.add_argument("--height", type=int, default=900)
    parser.add_argument("--plot-style", default="operational_fast")
    parser.add_argument("--png-compression", default="fast")
    parser.add_argument("--place-label-density", type=int, default=1)
    parser.add_argument("--contour-mode")
    parser.add_argument("--native-fill-level-multiplier", type=int, default=1)
    parser.add_argument("--cargo-profile", choices=["debug", "release"], default="debug")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--out-root")
    parser.add_argument("--cache-dir")
    parser.add_argument("--only", default="", help="Comma-separated product slugs to include")
    parser.add_argument("--skip", default="", help="Comma-separated product slugs to skip")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--no-global-warmup",
        action="store_true",
        help="Skip the initial all-selected-products warmup pass",
    )
    parser.add_argument(
        "--no-per-product-warmup",
        action="store_true",
        help="Skip the one-product warmup immediately before each measured run",
    )
    parser.add_argument("--stop-on-fail", action="store_true")
    args = parser.parse_args()
    args.only = [value.strip() for value in args.only.split(",") if value.strip()]
    args.skip = [value.strip() for value in args.skip.split(",") if value.strip()]
    return args


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    out_root = (
        Path(args.out_root).resolve()
        if args.out_root
        else repo / "proof" / "direct_map_warm_profile" / f"{args.model}_{args.date}_{utc_stamp()}"
    )
    cache_dir = Path(args.cache_dir).resolve() if args.cache_dir else out_root / "cache"
    logs_dir = out_root / "logs"
    product_dir = out_root / "products"
    warm_dir = out_root / "warmup"
    for path in (out_root, cache_dir, logs_dir, product_dir, warm_dir):
        path.mkdir(parents=True, exist_ok=True)

    status_path = out_root / "status.json"
    catalog_path = out_root / "catalog.json"
    json_path = out_root / "warm_cache_product_profile.json"
    csv_path = out_root / "warm_cache_product_profile.csv"
    md_path = out_root / "warm_cache_product_profile.md"

    try:
        write_status(status_path, {"phase": "building", "out_root": str(out_root)})
        if not args.skip_build:
            build_binaries(repo, args.cargo_profile, logs_dir)
        args.direct_batch_bin = binary_path(repo, args.cargo_profile, "direct_batch")
        product_catalog_bin = binary_path(repo, args.cargo_profile, "product_catalog")
        if not args.direct_batch_bin.exists() or not product_catalog_bin.exists():
            raise RuntimeError("required binaries are missing; rerun without --skip-build")

        write_status(status_path, {"phase": "catalog", "out_root": str(out_root)})
        catalog = load_catalog(repo, product_catalog_bin, catalog_path, logs_dir)
        products = selected_direct_products(
            catalog,
            model=args.model,
            only=args.only,
            skip={slug for slug in args.skip if slug},
            limit=args.limit,
        )
        if not products:
            raise RuntimeError(f"no supported direct products selected for model {args.model}")

        env = direct_batch_env(args)
        if not args.no_global_warmup:
            write_status(
                status_path,
                {
                    "phase": "warmup",
                    "product_count": len(products),
                    "out_root": str(out_root),
                },
            )
            warm_cmd = direct_batch_base_args(args, warm_dir, cache_dir)
            warm_cmd.extend(["--recipe", ",".join(product["slug"] for product in products)])
            warm_result = run_command(
                warm_cmd,
                cwd=repo,
                env=env,
                stdout_path=logs_dir / "warmup.out.log",
                stderr_path=logs_dir / "warmup.err.log",
            )
            if warm_result["returncode"] != 0:
                raise RuntimeError(
                    "warmup direct_batch failed; see "
                    f"{warm_result['stdout_path']} and {warm_result['stderr_path']}"
                )
        else:
            warm_result = {"returncode": None, "wall_ms": None}

        records: list[dict[str, Any]] = []
        for index, product in enumerate(products, 1):
            slug = product["slug"]
            write_status(
                status_path,
                {
                    "phase": "profiling",
                    "current_index": index,
                    "product_count": len(products),
                    "current_product": slug,
                    "out_root": str(out_root),
                },
            )
            run_dir = product_dir / slug_filename(slug, index)
            run_dir.mkdir(parents=True, exist_ok=True)
            product_warm_result = {"returncode": None, "wall_ms": None}
            if not args.no_per_product_warmup:
                product_warm_dir = warm_dir / slug_filename(slug, index)
                product_warm_dir.mkdir(parents=True, exist_ok=True)
                warm_cmd = direct_batch_base_args(args, product_warm_dir, cache_dir)
                warm_cmd.extend(["--recipe", slug])
                product_warm_result = run_command(
                    warm_cmd,
                    cwd=repo,
                    env=env,
                    stdout_path=logs_dir / f"{slug_filename(slug, index)}.warm.out.log",
                    stderr_path=logs_dir / f"{slug_filename(slug, index)}.warm.err.log",
                )
                if product_warm_result["returncode"] != 0 and args.stop_on_fail:
                    raise RuntimeError(f"per-product warmup failed for {slug}; stopping by request")
            cmd = direct_batch_base_args(args, run_dir, cache_dir)
            cmd.extend(["--recipe", slug])
            result = run_command(
                cmd,
                cwd=repo,
                env=env,
                stdout_path=logs_dir / f"{slug_filename(slug, index)}.out.log",
                stderr_path=logs_dir / f"{slug_filename(slug, index)}.err.log",
            )
            timing_path = find_single_timing(run_dir)
            record = product_record(
                product=product,
                index=index,
                command_result=result,
                timing_path=timing_path,
                out_dir=run_dir,
            )
            record["product_warmup"] = product_warm_result
            records.append(record)
            partial_summary = {
                "generated_at": now_iso(),
                "out_root": str(out_root),
                "cache_dir": str(cache_dir),
                "catalog_path": str(catalog_path),
                "json_path": str(json_path),
                "csv_path": str(csv_path),
                "markdown_path": str(md_path),
                "model": args.model,
                "source": args.source,
                "date": args.date,
                "cycle_label": "latest" if args.cycle is None else f"{args.cycle:02d}z",
                "forecast_hour": args.forecast_hour,
                "region": args.region,
                "product_count": len(products),
                "warmup": warm_result,
                "records": records,
            }
            json_path.write_text(json.dumps(partial_summary, indent=2), encoding="utf-8")
            write_csv(csv_path, records)
            md_path.write_text(markdown_summary(partial_summary), encoding="utf-8")
            if args.stop_on_fail and not record["ok"]:
                raise RuntimeError(f"profile failed for {slug}; stopping by request")

        summary = {
            "generated_at": now_iso(),
            "out_root": str(out_root),
            "cache_dir": str(cache_dir),
            "catalog_path": str(catalog_path),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
            "markdown_path": str(md_path),
            "model": args.model,
            "source": args.source,
            "date": args.date,
            "cycle_label": "latest" if args.cycle is None else f"{args.cycle:02d}z",
            "forecast_hour": args.forecast_hour,
            "region": args.region,
            "product_count": len(products),
            "warmup": warm_result,
            "records": records,
        }
        json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        write_csv(csv_path, records)
        md_path.write_text(markdown_summary(summary), encoding="utf-8")
        write_status(status_path, {"phase": "finished", **summary})
        print(json.dumps({"ok": True, "out_root": str(out_root), "json": str(json_path), "csv": str(csv_path), "markdown": str(md_path)}, indent=2))
        return 0
    except Exception as exc:
        payload = {"ok": False, "phase": "error", "out_root": str(out_root), "error": str(exc)}
        write_status(status_path, payload)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
