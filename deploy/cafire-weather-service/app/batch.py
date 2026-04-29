from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import parse_hour_spec, settings
from .rustwx_client import latest_full_hrrr_run, latest_run, render_maps
from .storage import ArtifactStore


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
        tmp_name = handle.name
    os.replace(tmp_name, path)


def read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def cycle_sort_key(date_yyyymmdd: str, cycle_utc: int) -> tuple[str, int]:
    return date_yyyymmdd, int(cycle_utc)


def should_update_latest_pointer(latest_path: Path, date_yyyymmdd: str, cycle_utc: int) -> bool:
    latest = read_json_if_exists(latest_path)
    if not latest:
        return True
    latest_date = str(latest.get("date_yyyymmdd", ""))
    latest_cycle = int(latest.get("cycle_utc", -1))
    return cycle_sort_key(date_yyyymmdd, cycle_utc) >= cycle_sort_key(latest_date, latest_cycle)


def brand_static_pngs(root: Path, text: str | None) -> int:
    if not text:
        return 0
    try:
        from PIL import Image, ImageDraw, ImageFont, PngImagePlugin
    except Exception:
        return 0

    branded = 0
    for path in sorted(root.rglob("*.png")):
        try:
            with Image.open(path) as image:
                if image.info.get("rustwx_cafire_brand") == "top_banner_v1":
                    continue
                canvas = image.convert("RGBA")
            font_size = max(30, min(42, canvas.width // 34))
            font = None
            for font_path in [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/local/lib/python3.12/site-packages/matplotlib/mpl-data/fonts/ttf/DejaVuSans-Bold.ttf",
                "/usr/local/lib/python3.11/site-packages/matplotlib/mpl-data/fonts/ttf/DejaVuSans-Bold.ttf",
            ]:
                if Path(font_path).exists():
                    try:
                        font = ImageFont.truetype(font_path, font_size)
                        break
                    except Exception:
                        pass
            if font is None:
                font = ImageFont.load_default()
            measure = ImageDraw.Draw(canvas)
            bbox = measure.textbbox((0, 0), text, font=font)
            text_w = bbox[2] - bbox[0]
            text_h = bbox[3] - bbox[1]
            banner_h = max(text_h + 22, 58)
            branded_canvas = Image.new("RGBA", (canvas.width, canvas.height + banner_h), (245, 246, 247, 255))
            branded_canvas.paste(canvas, (0, banner_h))
            draw = ImageDraw.Draw(branded_canvas)
            x = max(8, (branded_canvas.width - text_w) // 2)
            y = max(6, (banner_h - text_h) // 2 - 2)
            shadow = (255, 255, 255, 240)
            red = (190, 18, 46, 255)
            for dx, dy in [(-2, 0), (2, 0), (0, -2), (0, 2), (-1, -1), (1, -1), (-1, 1), (1, 1)]:
                draw.text((x + dx, y + dy), text, font=font, fill=shadow)
            draw.text((x, y), text, font=font, fill=red)
            metadata = PngImagePlugin.PngInfo()
            metadata.add_text("rustwx_cafire_brand", "top_banner_v1")
            branded_canvas.save(path, pnginfo=metadata)
            branded += 1
        except Exception:
            continue
    return branded


def write_static_webps(root: Path, *, enabled: bool, quality: int) -> dict[str, int]:
    if not enabled:
        return {"written": 0, "skipped": 0, "bytes": 0}
    try:
        from PIL import Image
    except Exception:
        return {"written": 0, "skipped": 0, "bytes": 0}

    written = 0
    skipped = 0
    total_bytes = 0
    for png_path in sorted(root.rglob("*.png")):
        webp_path = png_path.with_suffix(".webp")
        try:
            if webp_path.exists() and webp_path.stat().st_mtime >= png_path.stat().st_mtime:
                skipped += 1
                total_bytes += webp_path.stat().st_size
                continue
            with Image.open(png_path) as image:
                if image.mode in {"RGBA", "LA"}:
                    canvas = Image.new("RGB", image.size, (255, 255, 255))
                    alpha = image.getchannel("A") if "A" in image.getbands() else None
                    canvas.paste(image.convert("RGBA"), mask=alpha)
                else:
                    canvas = image.convert("RGB")
                canvas.save(webp_path, "WEBP", quality=quality, method=5, optimize=True)
            written += 1
            total_bytes += webp_path.stat().st_size
        except Exception:
            continue
    return {"written": written, "skipped": skipped, "bytes": total_bytes}


def _render_one_hour(task: dict[str, Any]) -> dict[str, Any]:
    forecast_hour = int(task["forecast_hour"])
    run_prefix = task["run_prefix"]
    domain = task["domain"]
    hour_dir = settings.artifact_root / run_prefix / f"f{forecast_hour:03d}" / domain
    hour_dir.mkdir(parents=True, exist_ok=True)
    report_path = hour_dir / "rustwx_render_report.json"
    start = time.perf_counter()
    if task.get("skip_existing") and report_path.exists():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        branded_pngs = brand_static_pngs(hour_dir, task.get("brand_text"))
        webps = write_static_webps(
            hour_dir,
            enabled=bool(task.get("webp_enabled")),
            quality=int(task.get("webp_quality") or 72),
        )
        uploads = ArtifactStore(settings).upload_tree(hour_dir, f"{run_prefix}/f{forecast_hour:03d}/{domain}")
        return {
            "forecast_hour": forecast_hour,
            "local_dir": str(hour_dir),
            "uploaded": uploads,
            "rustwx_report": report,
            "branded_pngs": branded_pngs,
            "webps": webps,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
            "skipped": True,
        }
    report = render_maps(
        settings=settings,
        date_yyyymmdd=task["date_yyyymmdd"],
        cycle_utc=int(task["cycle_utc"]),
        forecast_hour=forecast_hour,
        out_dir=hour_dir,
        products=list(task["products"]),
        domain=domain,
        source=task["source"],
        width=int(task["width"]),
        height=int(task["height"]),
        place_label_density=task["place_label_density"],
    )
    branded_pngs = brand_static_pngs(hour_dir, task.get("brand_text"))
    webps = write_static_webps(
        hour_dir,
        enabled=bool(task.get("webp_enabled")),
        quality=int(task.get("webp_quality") or 72),
    )
    atomic_write_json(report_path, report)
    uploads = ArtifactStore(settings).upload_tree(hour_dir, f"{run_prefix}/f{forecast_hour:03d}/{domain}")
    return {
        "forecast_hour": forecast_hour,
        "local_dir": str(hour_dir),
        "uploaded": uploads,
        "rustwx_report": report,
        "branded_pngs": branded_pngs,
        "webps": webps,
        "elapsed_ms": int((time.perf_counter() - start) * 1000),
    }


def _render_hours(
    *,
    date: str,
    cycle: int,
    source: str,
    products: list[str],
    hours: list[int],
    domain: str,
    run_prefix: str,
    width: int,
    height: int,
    place_label_density: str,
    parallelism: int,
    skip_existing: bool,
    brand_text: str | None,
) -> list[dict[str, Any]]:
    tasks = [
        {
            "forecast_hour": forecast_hour,
            "run_prefix": run_prefix,
            "domain": domain,
            "date_yyyymmdd": date,
            "cycle_utc": cycle,
            "source": source,
            "products": products,
            "width": width,
            "height": height,
            "place_label_density": place_label_density,
            "skip_existing": skip_existing,
            "brand_text": brand_text,
            "webp_enabled": settings.static_map_webp_enabled,
            "webp_quality": settings.static_map_webp_quality,
        }
        for forecast_hour in hours
    ]
    if parallelism <= 1 or len(tasks) <= 1:
        reports = []
        for task in tasks:
            print(f"rendering f{int(task['forecast_hour']):03d}", flush=True)
            report = _render_one_hour(task)
            print(
                f"{'skipped' if report.get('skipped') else 'completed'} "
                f"f{report['forecast_hour']:03d} in {report['elapsed_ms']} ms",
                flush=True,
            )
            reports.append(report)
        return reports

    reports: list[dict[str, Any]] = []
    workers = min(parallelism, len(tasks))
    print(f"rendering {len(tasks)} forecast hours with {workers} workers", flush=True)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_render_one_hour, task): int(task["forecast_hour"]) for task in tasks}
        for future in as_completed(futures):
            forecast_hour = futures[future]
            report = future.result()
            print(
                f"{'skipped' if report.get('skipped') else 'completed'} "
                f"f{forecast_hour:03d} in {report['elapsed_ms']} ms",
                flush=True,
            )
            reports.append(report)
    return sorted(reports, key=lambda item: item["forecast_hour"])


def render_run(args: argparse.Namespace) -> dict[str, Any]:
    settings.ensure_dirs()
    date = str(args.date_yyyymmdd)
    cycle = int(args.cycle_utc)
    source = args.source or settings.default_source
    products = args.products.split(",") if args.products else settings.default_products
    manifest_products = (
        args.manifest_products.split(",")
        if getattr(args, "manifest_products", None)
        else products
    )
    hours = parse_hour_spec(args.hours)
    domain = args.domain or settings.default_domain
    run_prefix = f"hrrr/runs/{date}/{cycle:02d}Z"
    local_run_root = settings.artifact_root / run_prefix
    store = ArtifactStore(settings)
    width = args.width or settings.default_width
    height = args.height or settings.default_height
    manifest_name = getattr(args, "manifest_name", None) or "manifest.json"
    latest_name = getattr(args, "latest_name", None) or "latest.json"
    latest_path = settings.artifact_root / "hrrr" / latest_name
    manifest_path = local_run_root / manifest_name
    existing_manifest = read_json_if_exists(manifest_path) or {}

    hour_reports = _render_hours(
        date=date,
        cycle=cycle,
        source=source,
        products=products,
        hours=hours,
        domain=domain,
        run_prefix=run_prefix,
        width=width,
        height=height,
        place_label_density=args.place_label_density,
        parallelism=getattr(args, "parallelism", None) or settings.static_map_worker_parallelism,
        skip_existing=not getattr(args, "force", False),
        brand_text=settings.static_map_brand_text,
    )

    merged_hours = {
        int(item["forecast_hour"]): item for item in existing_manifest.get("hours", []) if "forecast_hour" in item
    }
    for item in hour_reports:
        merged_hours[int(item["forecast_hour"])] = item
    manifest_hours = [merged_hours[hour] for hour in sorted(merged_hours)]
    rendered_forecast_hours = [int(item["forecast_hour"]) for item in manifest_hours]
    expected_hours = (
        parse_hour_spec(args.expected_hours)
        if getattr(args, "expected_hours", None)
        else sorted(set(rendered_forecast_hours + hours))
    )
    available_hours = (
        parse_hour_spec(args.available_hours)
        if getattr(args, "available_hours", None)
        else sorted(set(rendered_forecast_hours + hours))
    )

    manifest = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "model": settings.default_model,
        "source": source,
        "date_yyyymmdd": date,
        "cycle_utc": cycle,
        "domain": domain,
        "products": manifest_products,
        "forecast_hours": rendered_forecast_hours,
        "expected_forecast_hours": expected_hours,
        "available_forecast_hours": available_hours,
        "width": width,
        "height": height,
        "place_label_density": args.place_label_density,
        "brand_text": settings.static_map_brand_text,
        "webp_enabled": settings.static_map_webp_enabled,
        "webp_quality": settings.static_map_webp_quality,
        "artifact_prefix": run_prefix,
        "public_base_url": settings.public_artifact_base_url,
        "hours": manifest_hours,
    }
    atomic_write_json(manifest_path, manifest)
    latest_payload = {
        "schema_version": 1,
        "generated_at_utc": manifest["generated_at_utc"],
        "model": settings.default_model,
        "source": source,
        "date_yyyymmdd": date,
        "cycle_utc": cycle,
        "domain": domain,
        "products": manifest_products,
        "forecast_hours": rendered_forecast_hours,
        "expected_forecast_hours": expected_hours,
        "available_forecast_hours": available_hours,
        "width": width,
        "height": height,
        "place_label_density": args.place_label_density,
        "brand_text": settings.static_map_brand_text,
        "webp_enabled": settings.static_map_webp_enabled,
        "webp_quality": settings.static_map_webp_quality,
        "manifest_key": f"{run_prefix}/{manifest_name}",
        "manifest_url": (
            f"{settings.public_artifact_base_url.rstrip('/')}/{run_prefix}/{manifest_name}"
            if settings.public_artifact_base_url
            else None
        ),
    }
    update_latest = getattr(args, "update_latest", True)
    if update_latest and should_update_latest_pointer(latest_path, date, cycle):
        atomic_write_json(latest_path, latest_payload)

    if store.enabled():
        store.upload_file(manifest_path, f"{run_prefix}/{manifest_name}", immutable=False)
        if update_latest and should_update_latest_pointer(latest_path, date, cycle):
            store.upload_file(latest_path, f"hrrr/{latest_name}", immutable=False)

    return latest_payload


def render_latest(args: argparse.Namespace) -> dict[str, Any]:
    run = (
        latest_full_hrrr_run(settings, args.source)
        if getattr(args, "full_run", False)
        else latest_run(settings, settings.default_model, args.source)
    )
    args.date_yyyymmdd = run["cycle"]["date_yyyymmdd"]
    args.cycle_utc = int(run["cycle"]["hour_utc"])
    if not args.hours:
        args.hours = ",".join(str(hour) for hour in settings.default_forecast_hours)
    return render_run(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CA fire-weather batch worker")
    sub = parser.add_subparsers(dest="command", required=True)
    render = sub.add_parser("render-latest", help="render latest HRRR California products")
    render.add_argument("--source", default=None)
    render.add_argument("--domain", default=None)
    render.add_argument("--hours", default=None, help="Forecast hours, e.g. 0-48 or 1,2,3")
    render.add_argument("--products", default=None, help="Comma-separated rustwx product slugs")
    render.add_argument("--manifest-products", default=None, help="Comma-separated product slugs to publish in manifest")
    render.add_argument("--width", type=int, default=None)
    render.add_argument("--height", type=int, default=None)
    render.add_argument("--place-label-density", default="major")
    render.add_argument("--full-run", action="store_true", help="Use the latest 00/06/12/18Z run expected to have f048")
    render.add_argument("--parallelism", type=int, default=None)
    render.add_argument("--force", action="store_true", help="Regenerate existing forecast-hour artifacts")
    render.set_defaults(func=render_latest)
    run = sub.add_parser("render-run", help="render a specific HRRR run")
    run.add_argument("--date", dest="date_yyyymmdd", required=True)
    run.add_argument("--cycle", dest="cycle_utc", type=int, required=True)
    run.add_argument("--source", default=None)
    run.add_argument("--domain", default=None)
    run.add_argument("--hours", required=True, help="Forecast hours, e.g. 0-18 or 1,2,3")
    run.add_argument("--expected-hours", default=None)
    run.add_argument("--available-hours", default=None)
    run.add_argument("--products", default=None, help="Comma-separated rustwx product slugs")
    run.add_argument("--manifest-products", default=None, help="Comma-separated product slugs to publish in manifest")
    run.add_argument("--manifest-name", default="manifest.json", help="Run manifest filename")
    run.add_argument("--latest-name", default="latest.json", help="Latest pointer filename")
    run.add_argument("--width", type=int, default=None)
    run.add_argument("--height", type=int, default=None)
    run.add_argument("--place-label-density", default="major")
    run.add_argument("--parallelism", type=int, default=None)
    run.add_argument("--force", action="store_true", help="Regenerate existing forecast-hour artifacts")
    run.add_argument("--no-update-latest", dest="update_latest", action="store_false")
    run.set_defaults(func=render_run, update_latest=True)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = args.func(args)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
