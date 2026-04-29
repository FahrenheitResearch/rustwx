from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .batch import atomic_write_json, write_static_webps
from .config import Settings, settings
from .rustwx_client import render_goes_satellite
from .satellite_catalog import product_catalog_for, product_metadata
from .storage import ArtifactStore


def _read_latest_satellite_manifest(config: Settings) -> dict[str, Any] | None:
    latest_path = config.artifact_root / "satellite" / "latest.json"
    if not latest_path.exists():
        return None
    try:
        return json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _relative_key(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _public_url(config: Settings, key: str) -> str:
    if config.public_artifact_base_url:
        return f"{config.public_artifact_base_url.rstrip('/')}/{key}"
    return f"/artifacts/{key}"


def _uploaded(path: Path, key: str, url: str | None) -> dict[str, Any]:
    return {
        "path": str(path),
        "key": key,
        "url": url,
        "format": path.suffix.lower().lstrip("."),
        "size_bytes": path.stat().st_size,
    }


def _image_size(path: Path) -> tuple[int, int] | None:
    try:
        from PIL import Image

        with Image.open(path) as image:
            return image.size
    except Exception:
        return None


def _image_record(
    *,
    config: Settings,
    path: Path,
    key: str,
    url: str,
    width: int | None = None,
    height: int | None = None,
    role: str = "still",
    native: bool = False,
) -> dict[str, Any]:
    if width is None or height is None:
        size = _image_size(path)
        if size:
            width, height = size
    return {
        "role": role,
        "format": path.suffix.lower().lstrip("."),
        "key": key,
        "url": url,
        "size_bytes": path.stat().st_size,
        "width": width,
        "height": height,
        "native": native,
        "public_base_url": config.public_artifact_base_url,
    }


def _rgb_canvas(image: Any) -> Any:
    from PIL import Image

    if image.mode in {"RGBA", "LA"}:
        canvas = Image.new("RGB", image.size, (255, 255, 255))
        alpha = image.getchannel("A") if "A" in image.getbands() else None
        canvas.paste(image.convert("RGBA"), mask=alpha)
        return canvas
    return image.convert("RGB")


def _write_satellite_still_variants(
    *,
    source_path: Path,
    product: str,
    run_dir: Path,
    config: Settings,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    try:
        from PIL import Image
    except Exception:
        return [], {"written": 0, "skipped": 0, "bytes": 0}

    variants: list[dict[str, Any]] = []
    stats = {"written": 0, "skipped": 0, "bytes": 0}
    formats = [fmt for fmt in config.satellite_still_formats if fmt in {"png", "webp"}]
    if not formats or not config.satellite_still_widths:
        return variants, stats

    with Image.open(source_path) as source_image:
        source = _rgb_canvas(source_image)
        source_width, source_height = source.size
        for target_width in config.satellite_still_widths:
            if target_width == source_width:
                continue
            if target_width > source_width and not config.satellite_still_allow_upscale:
                continue
            target_height = max(1, round(source_height * (target_width / source_width)))
            if target_width == source_width and target_height == source_height:
                resized = source.copy()
            else:
                resized = source.resize((target_width, target_height), Image.Resampling.LANCZOS)
            for fmt in formats:
                output_path = run_dir / "stills" / f"w{target_width}" / f"{product}.{fmt}"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if output_path.exists() and output_path.stat().st_mtime >= source_path.stat().st_mtime:
                    stats["skipped"] += 1
                    stats["bytes"] += output_path.stat().st_size
                else:
                    if fmt == "webp":
                        resized.save(output_path, "WEBP", quality=config.static_map_webp_quality, method=5, optimize=True)
                    else:
                        resized.save(output_path, "PNG", optimize=True)
                    stats["written"] += 1
                    stats["bytes"] += output_path.stat().st_size
                variants.append(
                    {
                        "path": output_path,
                        "format": fmt,
                        "width": target_width,
                        "height": target_height,
                    }
                )
    return variants, stats


def _with_product_metadata(artifact: dict[str, Any], products: list[str]) -> dict[str, Any]:
    metadata = product_metadata(str(artifact.get("product", "")), products)
    return {
        **artifact,
        "title": metadata["name"],
        "display_name": metadata["name"],
        "description": metadata.get("description", ""),
        "wavelength": metadata.get("wavelength"),
        "category": metadata.get("category"),
        "display_order": metadata.get("display_order"),
    }


def _artifact_uploads(
    config: Settings,
    report: dict[str, Any],
    store: ArtifactStore,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    artifacts = report.get("artifacts") or []
    if not artifacts:
        return [], [], {
            "webps": {"written": 0, "skipped": 0, "bytes": 0},
            "stills": {"written": 0, "skipped": 0, "bytes": 0},
        }
    run_dir = Path(artifacts[0]["png_path"]).parent
    webp_started = time.perf_counter()
    webps = write_static_webps(
        run_dir,
        enabled=config.static_map_webp_enabled,
        quality=config.static_map_webp_quality,
    )
    webp_elapsed_ms = int((time.perf_counter() - webp_started) * 1000)

    uploaded: list[dict[str, Any]] = []
    manifest_artifacts: list[dict[str, Any]] = []
    still_stats = {"written": 0, "skipped": 0, "bytes": 0}
    still_started = time.perf_counter()
    for artifact in artifacts:
        png_path = Path(artifact["png_path"])
        product = str(artifact.get("product") or png_path.stem)
        png_key = _relative_key(config.artifact_root, png_path)
        png_url = store.upload_file(png_path, png_key, immutable=True) if store.enabled() else None
        png_public_url = png_url or _public_url(config, png_key)
        uploaded.append(_uploaded(png_path, png_key, png_public_url))
        source_size = _image_size(png_path)
        native_width, native_height = source_size if source_size else (None, None)
        stills: list[dict[str, Any]] = [
            _image_record(
                config=config,
                path=png_path,
                key=png_key,
                url=png_public_url,
                width=native_width,
                height=native_height,
                native=True,
            )
        ]

        webp_path = png_path.with_suffix(".webp")
        webp_key = None
        webp_public_url = None
        if webp_path.exists():
            webp_key = _relative_key(config.artifact_root, webp_path)
            webp_url = store.upload_file(webp_path, webp_key, immutable=True) if store.enabled() else None
            webp_public_url = webp_url or _public_url(config, webp_key)
            uploaded.append(_uploaded(webp_path, webp_key, webp_public_url))
            stills.append(
                _image_record(
                    config=config,
                    path=webp_path,
                    key=webp_key,
                    url=webp_public_url,
                    width=native_width,
                    height=native_height,
                    native=True,
                )
            )

        variant_paths, product_still_stats = _write_satellite_still_variants(
            source_path=png_path,
            product=product,
            run_dir=run_dir,
            config=config,
        )
        for key, value in product_still_stats.items():
            still_stats[key] = still_stats.get(key, 0) + value
        for variant in variant_paths:
            variant_path = Path(variant["path"])
            if not variant_path.exists():
                continue
            variant_key = _relative_key(config.artifact_root, variant_path)
            variant_url = store.upload_file(variant_path, variant_key, immutable=True) if store.enabled() else None
            variant_public_url = variant_url or _public_url(config, variant_key)
            uploaded.append(_uploaded(variant_path, variant_key, variant_public_url))
            stills.append(
                _image_record(
                    config=config,
                    path=variant_path,
                    key=variant_key,
                    url=variant_public_url,
                    width=int(variant["width"]),
                    height=int(variant["height"]),
                )
            )

        overlay = dict(artifact.get("mapbox_overlay") or {})
        overlay["image_url"] = png_public_url
        overlay["image_key"] = png_key
        still_widths = sorted({int(item["width"]) for item in stills if item.get("width")})
        still_formats = sorted({str(item["format"]) for item in stills if item.get("format")})
        manifest_artifacts.append(
            {
                **_with_product_metadata(artifact, report.get("products") or config.satellite_products),
                "png_key": png_key,
                "png_url": png_public_url,
                "webp_key": webp_key,
                "webp_url": webp_public_url,
                "stills": stills,
                "still_widths": still_widths,
                "still_formats": still_formats,
                "mapbox_overlay": overlay,
            }
        )
    still_elapsed_ms = int((time.perf_counter() - still_started) * 1000)

    report_path_raw = report.get("report_path")
    report_path = Path(report_path_raw) if report_path_raw else None
    if report_path and report_path.exists():
        report_key = _relative_key(config.artifact_root, report_path)
        report_url = store.upload_file(report_path, report_key, immutable=True) if store.enabled() else None
        uploaded.append(_uploaded(report_path, report_key, report_url or _public_url(config, report_key)))

    return uploaded, manifest_artifacts, {
        "webps": webps,
        "stills": still_stats,
        "webp_elapsed_ms": webp_elapsed_ms,
        "still_elapsed_ms": still_elapsed_ms,
    }


def _manifest_artifact_for_product(manifest: dict[str, Any], product: str) -> dict[str, Any] | None:
    for artifact in manifest.get("artifacts") or []:
        if artifact.get("product") == product:
            return artifact
    return None


def _satellite_scan_root(config: Settings) -> Path:
    normalized = config.satellite_satellite.strip().lower().replace("-", "")
    if normalized in {"g18", "goes18", "noaagoes18"}:
        satellite = "g18"
    elif normalized in {"g17", "goes17", "noaagoes17"}:
        satellite = "g17"
    elif normalized in {"g16", "goes16", "noaagoes16"}:
        satellite = "g16"
    elif normalized.startswith("goes"):
        satellite = "g" + normalized.removeprefix("goes")
    else:
        satellite = normalized
    return config.artifact_root / "satellite" / satellite / config.satellite_domain


def _parse_satellite_time(value: str) -> datetime | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        pass
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _loop_frame_candidates(config: Settings, current_artifact: dict[str, Any]) -> list[tuple[str, Path]]:
    product = str(current_artifact.get("product") or "")
    frames: list[tuple[str, Path]] = []
    for manifest_path in _satellite_scan_root(config).glob("*/manifest.json"):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        artifact = _manifest_artifact_for_product(manifest, product)
        if not artifact:
            continue
        png_key = artifact.get("png_key")
        if not isinstance(png_key, str) or not png_key:
            continue
        png_path = config.artifact_root / png_key
        if png_path.exists():
            frames.append((str(manifest.get("scan_time_utc") or manifest_path.parent.name), png_path))

    current_key = current_artifact.get("png_key")
    if isinstance(current_key, str) and current_key:
        current_path = config.artifact_root / current_key
        if current_path.exists():
            frames.append((str(current_artifact.get("scan_time_utc") or current_path.parent.name), current_path))

    deduped: dict[str, tuple[str, Path]] = {}
    for scan_time, path in frames:
        deduped[str(path.resolve())] = (scan_time, path)
    return sorted(deduped.values(), key=lambda item: item[0])[-config.satellite_loop_max_frames :]


def _filter_loop_candidates(
    candidates: list[tuple[str, Path]],
    *,
    duration_min: int,
    max_frames: int,
) -> list[tuple[str, Path]]:
    if not candidates:
        return []
    parsed = [(scan_time, _parse_satellite_time(scan_time), path) for scan_time, path in candidates]
    dated = [(scan_time, when, path) for scan_time, when, path in parsed if when is not None]
    if not dated:
        return candidates[-max_frames:]
    newest = max(when for _, when, _ in dated)
    cutoff = newest - timedelta(minutes=duration_min)
    selected = [(scan_time, path) for scan_time, when, path in dated if when >= cutoff]
    return sorted(selected, key=lambda item: item[0])[-max_frames:]


def _load_loop_frames(frame_paths: list[Path], *, width: int | None) -> list[Any]:
    from PIL import Image

    frames = []
    for path in frame_paths:
        with Image.open(path) as source:
            frame = _rgb_canvas(source)
            if width and frame.width != width:
                height = max(1, round(frame.height * (width / frame.width)))
                frame = frame.resize((width, height), Image.Resampling.LANCZOS)
            frames.append(frame)
    return frames


def _write_satellite_loop(
    *,
    frame_paths: list[Path],
    output_path: Path,
    fmt: str,
    width: int | None,
    config: Settings,
) -> tuple[int, int] | None:
    frames = _load_loop_frames(frame_paths, width=width)
    if not frames:
        return None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame_width, frame_height = frames[0].size
    if fmt == "webp":
        frames[0].save(
            output_path,
            "WEBP",
            save_all=True,
            append_images=frames[1:],
            duration=config.satellite_loop_frame_ms,
            loop=0,
            quality=config.static_map_webp_quality,
            method=5,
            minimize_size=True,
        )
    elif fmt == "gif":
        from PIL import Image

        paletted = [frame.convert("P", palette=Image.Palette.ADAPTIVE, colors=256) for frame in frames]
        paletted[0].save(
            output_path,
            save_all=True,
            append_images=paletted[1:],
            duration=config.satellite_loop_frame_ms,
            loop=0,
            optimize=True,
            disposal=2,
        )
    else:
        raise ValueError(f"unsupported satellite loop format: {fmt}")
    return frame_width, frame_height


def _build_satellite_loops(
    *,
    config: Settings,
    manifest_artifacts: list[dict[str, Any]],
    run_dir: Path,
    store: ArtifactStore,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not config.satellite_loop_enabled:
        return [], []
    loops: list[dict[str, Any]] = []
    uploaded: list[dict[str, Any]] = []
    loop_dir = config.artifact_root / run_dir / "loops"
    formats = [fmt for fmt in config.satellite_loop_formats if fmt in {"webp", "gif"}]
    for artifact in manifest_artifacts:
        product = str(artifact.get("product") or "")
        if not product:
            continue
        candidates = _loop_frame_candidates(config, artifact)
        for duration_min in config.satellite_loop_durations_min:
            selected = _filter_loop_candidates(
                candidates,
                duration_min=duration_min,
                max_frames=config.satellite_loop_max_frames,
            )
            frame_paths = [path for _, path in selected]
            if not frame_paths:
                continue
            for width in config.satellite_loop_widths:
                for fmt in formats:
                    if fmt == "gif" and (
                        config.satellite_loop_gif_max_duration_min <= 0
                        or duration_min > config.satellite_loop_gif_max_duration_min
                    ):
                        continue
                    loop_path = loop_dir / f"{duration_min}min" / f"w{width}" / f"{product}.{fmt}"
                    try:
                        rendered_size = _write_satellite_loop(
                            frame_paths=frame_paths,
                            output_path=loop_path,
                            fmt=fmt,
                            width=width,
                            config=config,
                        )
                    except Exception as exc:
                        loops.append(
                            {
                                **product_metadata(product, config.satellite_products),
                                "ok": False,
                                "format": fmt,
                                "duration_min": duration_min,
                                "width": width,
                                "error": str(exc),
                            }
                        )
                        continue
                    if not loop_path.exists() or rendered_size is None:
                        continue
                    loop_key = _relative_key(config.artifact_root, loop_path)
                    loop_url = store.upload_file(loop_path, loop_key, immutable=True) if store.enabled() else None
                    public_url = loop_url or _public_url(config, loop_key)
                    upload = _uploaded(loop_path, loop_key, public_url)
                    uploaded.append(upload)
                    rendered_width, rendered_height = rendered_size
                    loops.append(
                        {
                            **product_metadata(product, config.satellite_products),
                            "ok": True,
                            "format": fmt,
                            "key": loop_key,
                            "url": public_url,
                            "size_bytes": loop_path.stat().st_size,
                            "frame_count": len(frame_paths),
                            "frame_ms": config.satellite_loop_frame_ms,
                            "duration_min": duration_min,
                            "duration_label": f"{duration_min // 60}h" if duration_min % 60 == 0 else f"{duration_min}m",
                            "width": rendered_width,
                            "height": rendered_height,
                            "requested_width": width,
                            "scan_times_utc": [scan_time for scan_time, _ in selected],
                        }
                    )
    return loops, uploaded


def _satellite_manifest_payload(
    *,
    config: Settings,
    report: dict[str, Any],
    run_dir: Path,
    uploaded: list[dict[str, Any]],
    manifest_artifacts: list[dict[str, Any]],
    loops: list[dict[str, Any]],
    loop_uploads: list[dict[str, Any]],
    postprocess: dict[str, Any],
    publish_stage: str,
    stills_published_at: str | None,
    loops_published_at: str | None,
) -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat()
    return {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "publish_stage": publish_stage,
        "stills_published_at_utc": stills_published_at,
        "loops_published_at_utc": loops_published_at,
        "kind": "goes_satellite",
        "model": "goes_satellite",
        "source": report.get("source_bucket"),
        "satellite": report.get("satellite", config.satellite_satellite),
        "abi_product": report.get("abi_product", config.satellite_abi_product),
        "scan_id": report.get("scan_id"),
        "scan_time_utc": report.get("scan_time_utc"),
        "scan_end_time_utc": report.get("scan_end_time_utc"),
        "domain": report.get("domain", config.satellite_domain),
        "domain_label": report.get("domain_label", config.satellite_label),
        "bounds": report.get("bounds"),
        "products": report.get("products", config.satellite_products),
        "product_catalog": product_catalog_for(report.get("products") or config.satellite_products),
        "forecast_hours": [0],
        "width": report.get("width", config.satellite_width),
        "height": report.get("height", config.satellite_height),
        "webp_enabled": config.static_map_webp_enabled,
        "webp_quality": config.static_map_webp_quality,
        "still_widths": config.satellite_still_widths,
        "still_formats": config.satellite_still_formats,
        "loop_enabled": config.satellite_loop_enabled,
        "artifact_prefix": str(run_dir).replace("\\", "/"),
        "public_base_url": config.public_artifact_base_url,
        "source_keys": report.get("source_keys", []),
        "glm_source_keys": report.get("glm_source_keys", []),
        "channel_files": report.get("channel_files", {}),
        "artifacts": manifest_artifacts,
        "loops": loops,
        "loop_policy": {
            "type": "per_product_recent_scan_sequence",
            "max_frames": config.satellite_loop_max_frames,
            "frame_ms": config.satellite_loop_frame_ms,
            "widths": config.satellite_loop_widths,
            "durations_min": config.satellite_loop_durations_min,
            "formats": config.satellite_loop_formats,
            "gif_max_duration_min": config.satellite_loop_gif_max_duration_min,
        },
        "timing": report.get("timing", {}),
        "postprocess": postprocess,
        "webps": postprocess.get("webps", {}),
        "hours": [
            {
                "forecast_hour": 0,
                "valid_time_utc": report.get("scan_time_utc"),
                "uploaded": uploaded + loop_uploads,
            }
        ],
    }


def _write_satellite_manifest(
    *,
    config: Settings,
    store: ArtifactStore,
    manifest: dict[str, Any],
    manifest_key: str,
    latest_key: str,
) -> dict[str, Any]:
    artifact_root = config.artifact_root
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


def _publish_satellite_manifest(*, config: Settings, report: dict[str, Any]) -> dict[str, Any]:
    store = ArtifactStore(config)
    postprocess_started = time.perf_counter()
    uploaded, manifest_artifacts, postprocess = _artifact_uploads(config, report, store)
    if manifest_artifacts:
        run_dir = Path(manifest_artifacts[0]["png_key"]).parent
    else:
        run_dir = Path("satellite") / "empty"
    manifest_key = str(run_dir / "manifest.json").replace("\\", "/")
    latest_key = "satellite/latest.json"

    postprocess["loop_elapsed_ms"] = 0
    postprocess["elapsed_ms"] = int((time.perf_counter() - postprocess_started) * 1000)
    stills_published_at = datetime.now(UTC).isoformat()
    still_manifest = _satellite_manifest_payload(
        config=config,
        report=report,
        run_dir=run_dir,
        uploaded=uploaded,
        manifest_artifacts=manifest_artifacts,
        loops=[],
        loop_uploads=[],
        postprocess=postprocess,
        publish_stage="stills",
        stills_published_at=stills_published_at,
        loops_published_at=None,
    )
    _write_satellite_manifest(
        config=config,
        store=store,
        manifest=still_manifest,
        manifest_key=manifest_key,
        latest_key=latest_key,
    )

    loop_started = time.perf_counter()
    loops, loop_uploads = _build_satellite_loops(
        config=config,
        manifest_artifacts=manifest_artifacts,
        run_dir=run_dir,
        store=store,
    )
    loop_elapsed_ms = int((time.perf_counter() - loop_started) * 1000)
    if loops:
        loops_by_product: dict[str, list[dict[str, Any]]] = {}
        for loop in loops:
            loops_by_product.setdefault(str(loop.get("product") or ""), []).append(loop)
        for artifact in manifest_artifacts:
            product_loops = loops_by_product.get(str(artifact.get("product") or ""), [])
            artifact["loops"] = [
                loop for loop in product_loops if loop.get("ok") is not False and loop.get("url")
            ]
    postprocess["loop_elapsed_ms"] = loop_elapsed_ms
    postprocess["elapsed_ms"] = int((time.perf_counter() - postprocess_started) * 1000)
    loops_published_at = datetime.now(UTC).isoformat()
    manifest = _satellite_manifest_payload(
        config=config,
        report=report,
        run_dir=run_dir,
        uploaded=uploaded,
        manifest_artifacts=manifest_artifacts,
        loops=loops,
        loop_uploads=loop_uploads,
        postprocess=postprocess,
        publish_stage="complete",
        stills_published_at=stills_published_at,
        loops_published_at=loops_published_at,
    )
    return _write_satellite_manifest(
        config=config,
        store=store,
        manifest=manifest,
        manifest_key=manifest_key,
        latest_key=latest_key,
    )


def run_satellite_once(
    config: Settings = settings,
    *,
    skip_unchanged: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    config.ensure_dirs()
    latest_manifest = _read_latest_satellite_manifest(config) if skip_unchanged else None
    report = render_goes_satellite(
        settings=config,
        out_dir=config.artifact_root,
        domain=config.satellite_domain,
        label=config.satellite_label,
        products=config.satellite_products,
        width=config.satellite_width,
        height=config.satellite_height,
        skip_scan_id=latest_manifest.get("scan_id") if latest_manifest else None,
        high_speed_png=True,
    )
    if report.get("skipped") and latest_manifest:
        return {
            "ok": True,
            "skipped": True,
            "reason": "latest GOES ABI scan unchanged",
            "satellite": config.satellite_satellite,
            "scan_id": latest_manifest.get("scan_id"),
            "scan_time_utc": latest_manifest.get("scan_time_utc"),
            "manifest_url": latest_manifest.get("manifest_url"),
            "latest_url": latest_manifest.get("latest_url"),
            "artifact_count": len(latest_manifest.get("artifacts") or []),
            "loop_count": len(latest_manifest.get("loops") or []),
            "timing": report.get("timing"),
            "postprocess": latest_manifest.get("postprocess"),
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
        }
    manifest = _publish_satellite_manifest(config=config, report=report)
    return {
        "ok": True,
        "satellite": manifest.get("satellite"),
        "scan_id": manifest.get("scan_id"),
        "scan_time_utc": manifest.get("scan_time_utc"),
        "manifest_url": manifest.get("manifest_url"),
        "latest_url": manifest.get("latest_url"),
        "artifact_count": len(manifest.get("artifacts") or []),
        "loop_count": len(manifest.get("loops") or []),
        "source_key_count": len(manifest.get("source_keys") or []),
        "glm_source_key_count": len(manifest.get("glm_source_keys") or []),
        "timing": manifest.get("timing"),
        "postprocess": manifest.get("postprocess"),
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
    }


def run_loop(config: Settings = settings) -> None:
    config.ensure_dirs()
    print(
        json.dumps(
            {
                "ok": True,
                "worker": "satellite-worker",
                "event": "starting",
                "interval_sec": config.satellite_interval_sec,
                "satellite": config.satellite_satellite,
                "domain": config.satellite_domain,
            }
        ),
        flush=True,
    )
    while True:
        loop_started = time.monotonic()
        try:
            if not config.satellite_enabled:
                result = {"ok": True, "skipped": True, "reason": "satellite worker disabled"}
            else:
                result = run_satellite_once(config, skip_unchanged=True)
            result["worker"] = "satellite-worker"
            print(json.dumps(result, sort_keys=True), flush=True)
        except Exception as exc:  # pragma: no cover - live NOAA path
            print(
                json.dumps(
                    {
                        "ok": False,
                        "worker": "satellite-worker",
                        "error": str(exc),
                        "at_utc": datetime.now(UTC).isoformat(),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        elapsed = time.monotonic() - loop_started
        time.sleep(max(1.0, config.satellite_interval_sec - elapsed))


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NOAA GOES ABI/GLM NetCDF and render satellite products")
    parser.add_argument("--skip-unchanged", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    if args.loop:
        run_loop(settings)
    else:
        print(json.dumps(run_satellite_once(settings, skip_unchanged=args.skip_unchanged), indent=2))


if __name__ == "__main__":
    main()
