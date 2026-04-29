from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .batch import atomic_write_json, write_static_webps
from .config import Settings, settings
from .rustwx_client import render_glm_lightning
from .storage import ArtifactStore


S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
GOES_BUCKETS = {
    "goes16": "noaa-goes16",
    "goes18": "noaa-goes18",
    "goes19": "noaa-goes19",
}


@dataclass(frozen=True)
class GlmObject:
    key: str
    size_bytes: int
    last_modified: str

    @property
    def filename(self) -> str:
        return self.key.rsplit("/", 1)[-1]


def _bucket_for_satellite(satellite: str) -> str:
    normalized = satellite.strip().lower().replace("-", "")
    if normalized in GOES_BUCKETS:
        return GOES_BUCKETS[normalized]
    if normalized.startswith("noaa-goes"):
        return normalized
    raise ValueError(f"unsupported GLM satellite: {satellite}")


def _list_glm_objects(bucket: str, prefix: str) -> list[GlmObject]:
    url = f"https://{bucket}.s3.amazonaws.com/?" + urllib.parse.urlencode(
        {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
    )
    with urllib.request.urlopen(url, timeout=30) as response:  # nosec - public NOAA S3 bucket
        root = ET.fromstring(response.read())

    objects: list[GlmObject] = []
    for item in root.findall("s3:Contents", S3_NS):
        key = item.findtext("s3:Key", default="", namespaces=S3_NS)
        if not key.endswith(".nc"):
            continue
        objects.append(
            GlmObject(
                key=key,
                size_bytes=int(item.findtext("s3:Size", default="0", namespaces=S3_NS)),
                last_modified=item.findtext("s3:LastModified", default="", namespaces=S3_NS),
            )
        )
    return objects


def _recent_glm_objects(config: Settings, now: datetime | None = None) -> tuple[str, list[GlmObject]]:
    now = now or datetime.now(UTC)
    bucket = _bucket_for_satellite(config.lightning_satellite)
    objects: dict[str, GlmObject] = {}
    for offset in range(config.lightning_lookback_hours):
        hour = now - timedelta(hours=offset)
        prefix = f"GLM-L2-LCFA/{hour:%Y}/{hour.timetuple().tm_yday:03d}/{hour:%H}/"
        for item in _list_glm_objects(bucket, prefix):
            objects[item.key] = item
    selected = sorted(objects.values(), key=lambda item: item.key)[-config.lightning_fetch_count :]
    return bucket, selected


def _download_glm_objects(config: Settings, bucket: str, objects: list[GlmObject]) -> dict[str, Any]:
    config.glm_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    reused = 0
    bytes_downloaded = 0
    selected_names = {item.filename for item in objects}
    for item in objects:
        target = config.glm_dir / item.filename
        if target.exists() and target.stat().st_size == item.size_bytes:
            reused += 1
            continue
        url = f"https://{bucket}.s3.amazonaws.com/{item.key}"
        with urllib.request.urlopen(url, timeout=60) as response:  # nosec - public NOAA S3 bucket
            with tempfile.NamedTemporaryFile("wb", delete=False, dir=config.glm_dir) as handle:
                tmp_name = handle.name
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    bytes_downloaded += len(chunk)
        os.replace(tmp_name, target)
        downloaded += 1

    pruned = 0
    for path in config.glm_dir.glob("OR_GLM-L2-LCFA_*.nc"):
        if path.name in selected_names:
            continue
        try:
            path.unlink()
            pruned += 1
        except OSError:
            continue

    return {
        **_glm_object_summary(objects),
        "selected_files": len(objects),
        "downloaded_files": downloaded,
        "reused_files": reused,
        "downloaded_bytes": bytes_downloaded,
        "pruned_files": pruned,
    }


def _glm_object_summary(objects: list[GlmObject]) -> dict[str, Any]:
    summary: dict[str, Any] = {"selected_files": len(objects)}
    if objects:
        first = objects[0]
        latest = objects[-1]
        summary.update(
            {
                "selected_first_key": first.key,
                "selected_first_last_modified": first.last_modified,
                "selected_latest_key": latest.key,
                "selected_latest_filename": latest.filename,
                "selected_latest_last_modified": latest.last_modified,
            }
        )
    return summary


def _read_latest_lightning_manifest(config: Settings) -> dict[str, Any] | None:
    latest_path = config.artifact_root / "lightning" / "latest.json"
    if not latest_path.exists():
        return None
    try:
        return json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _relative_key(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _uploaded(path: Path, key: str, url: str | None) -> dict[str, Any]:
    return {
        "path": str(path),
        "key": key,
        "url": url,
        "format": path.suffix.lower().lstrip("."),
        "size_bytes": path.stat().st_size,
    }


def _publish_lightning_manifest(
    *,
    config: Settings,
    report: dict[str, Any],
    fetch_report: dict[str, Any],
    bucket: str,
) -> dict[str, Any]:
    artifact_root = config.artifact_root
    png_path = Path(report["png_path"])
    data_json_path = Path(report["data_json_path"])
    webps = write_static_webps(png_path.parent, enabled=config.static_map_webp_enabled, quality=config.static_map_webp_quality)
    webp_path = png_path.with_suffix(".webp")

    png_key = _relative_key(artifact_root, png_path)
    data_json_key = _relative_key(artifact_root, data_json_path)
    webp_key = _relative_key(artifact_root, webp_path) if webp_path.exists() else None
    manifest_key = str(Path(png_key).parent / "manifest.json").replace("\\", "/")
    latest_key = "lightning/latest.json"

    uploaded: list[dict[str, Any]] = []
    store = ArtifactStore(config)
    png_url = store.upload_file(png_path, png_key, immutable=True) if store.enabled() else None
    uploaded.append(_uploaded(png_path, png_key, png_url or _public_url(config, png_key)))
    if webp_key:
        webp_url = store.upload_file(webp_path, webp_key, immutable=True) if store.enabled() else None
        uploaded.append(_uploaded(webp_path, webp_key, webp_url or _public_url(config, webp_key)))
    data_json_url = store.upload_file(data_json_path, data_json_key, immutable=True) if store.enabled() else None
    uploaded.append(_uploaded(data_json_path, data_json_key, data_json_url or _public_url(config, data_json_key)))

    time_window = report.get("time_window") or {}
    generated_at = datetime.now(UTC).isoformat()
    manifest = {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "kind": "glm_lightning",
        "model": "goes_glm",
        "source": bucket,
        "satellite": config.lightning_satellite,
        "domain": report.get("domain", config.lightning_domain),
        "domain_label": report.get("domain_label", config.lightning_label),
        "products": ["glm_lightning_flashes"],
        "forecast_hours": [0],
        "width": config.lightning_width,
        "height": config.lightning_height,
        "webp_enabled": config.static_map_webp_enabled,
        "webp_quality": config.static_map_webp_quality,
        "artifact_prefix": str(Path(manifest_key).parent).replace("\\", "/"),
        "public_base_url": config.public_artifact_base_url,
        "time_window": time_window,
        "flash_count_total": report.get("flash_count_total"),
        "flash_count_in_domain": report.get("flash_count_in_domain"),
        "flash_count_drawn": report.get("flash_count_drawn"),
        "n_files": report.get("n_files"),
        "latest_glm_key": fetch_report.get("selected_latest_key"),
        "latest_glm_last_modified": fetch_report.get("selected_latest_last_modified"),
        "fetch": fetch_report,
        "timing": report.get("timing"),
        "webps": webps,
        "hours": [
            {
                "forecast_hour": 0,
                "valid_time_utc": time_window.get("last"),
                "uploaded": uploaded,
            }
        ],
    }

    manifest_path = artifact_root / manifest_key
    latest_path = artifact_root / latest_key
    atomic_write_json(manifest_path, manifest)
    atomic_write_json(latest_path, manifest)
    if store.enabled():
        manifest_url = store.upload_file(manifest_path, manifest_key, immutable=False)
        latest_url = store.upload_file(latest_path, latest_key, immutable=False)
    else:
        manifest_url = None
        latest_url = None
    manifest["manifest_key"] = manifest_key
    manifest["manifest_url"] = manifest_url or _public_url(config, manifest_key)
    manifest["latest_key"] = latest_key
    manifest["latest_url"] = latest_url or _public_url(config, latest_key)
    atomic_write_json(manifest_path, manifest)
    atomic_write_json(latest_path, manifest)
    if store.enabled():
        store.upload_file(manifest_path, manifest_key, immutable=False)
        store.upload_file(latest_path, latest_key, immutable=False)
    return manifest


def _public_url(config: Settings, key: str) -> str:
    if config.public_artifact_base_url:
        return f"{config.public_artifact_base_url.rstrip('/')}/{key}"
    return f"/artifacts/{key}"


def run_lightning_once(
    config: Settings = settings,
    *,
    skip_fetch: bool = False,
    skip_unchanged: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    config.ensure_dirs()
    bucket, objects = _recent_glm_objects(config)
    if not objects:
        return {
            "ok": False,
            "reason": "no recent GLM LCFA objects found",
            "satellite": config.lightning_satellite,
            "bucket": bucket,
        }
    object_summary = _glm_object_summary(objects)
    latest_glm_key = object_summary.get("selected_latest_key")
    latest_manifest = _read_latest_lightning_manifest(config) if skip_unchanged else None
    if latest_manifest and latest_glm_key and latest_manifest.get("latest_glm_key") == latest_glm_key:
        return {
            "ok": True,
            "skipped": True,
            "reason": "latest GLM object unchanged",
            "satellite": config.lightning_satellite,
            "bucket": bucket,
            "manifest_url": latest_manifest.get("manifest_url"),
            "latest_url": latest_manifest.get("latest_url"),
            "flash_count_total": latest_manifest.get("flash_count_total"),
            "flash_count_in_domain": latest_manifest.get("flash_count_in_domain"),
            "n_files": latest_manifest.get("n_files"),
            "time_window": latest_manifest.get("time_window"),
            "latest_glm_key": latest_glm_key,
            "fetch": {
                **object_summary,
                "downloaded_files": 0,
                "reused_files": len(objects),
                "downloaded_bytes": 0,
                "pruned_files": 0,
                "skip_unchanged": True,
            },
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
        }
    fetch_report = (
        {
            **object_summary,
            "downloaded_files": 0,
            "reused_files": len(objects),
            "downloaded_bytes": 0,
            "pruned_files": 0,
            "skip_fetch": True,
        }
        if skip_fetch
        else _download_glm_objects(config, bucket, objects)
    )
    report = render_glm_lightning(
        settings=config,
        out_dir=config.artifact_root,
        domain=config.lightning_domain,
        label=config.lightning_label,
        width=config.lightning_width,
        height=config.lightning_height,
        max_age_min=config.lightning_max_age_min,
        high_speed_png=True,
    )
    manifest = _publish_lightning_manifest(
        config=config,
        report=report,
        fetch_report=fetch_report,
        bucket=bucket,
    )
    return {
        "ok": True,
        "satellite": config.lightning_satellite,
        "bucket": bucket,
        "manifest_url": manifest.get("manifest_url"),
        "latest_url": manifest.get("latest_url"),
        "flash_count_total": manifest.get("flash_count_total"),
        "flash_count_in_domain": manifest.get("flash_count_in_domain"),
        "n_files": manifest.get("n_files"),
        "time_window": manifest.get("time_window"),
        "latest_glm_key": manifest.get("latest_glm_key"),
        "fetch": fetch_report,
        "timing": report.get("timing"),
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
    }


def run_loop(config: Settings = settings, *, skip_fetch: bool = False) -> None:
    config.ensure_dirs()
    print(
        json.dumps(
            {
                "ok": True,
                "worker": "lightning-worker",
                "event": "starting",
                "interval_sec": config.lightning_interval_sec,
                "satellite": config.lightning_satellite,
                "domain": config.lightning_domain,
            }
        ),
        flush=True,
    )
    while True:
        loop_started = time.monotonic()
        try:
            result = run_lightning_once(config, skip_fetch=skip_fetch, skip_unchanged=True)
            result["worker"] = "lightning-worker"
            print(json.dumps(result, sort_keys=True), flush=True)
        except Exception as exc:  # pragma: no cover - live NOAA/GLM path
            print(
                json.dumps(
                    {
                        "ok": False,
                        "worker": "lightning-worker",
                        "error": str(exc),
                        "at_utc": datetime.now(UTC).isoformat(),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        elapsed = time.monotonic() - loop_started
        time.sleep(max(1.0, config.lightning_interval_sec - elapsed))


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch recent GOES GLM files and render a lightning map")
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-unchanged", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    if args.loop:
        run_loop(settings, skip_fetch=args.skip_fetch)
    else:
        print(
            json.dumps(
                run_lightning_once(settings, skip_fetch=args.skip_fetch, skip_unchanged=args.skip_unchanged),
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
