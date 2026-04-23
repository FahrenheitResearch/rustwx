#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path


def sanitize_component(value: str) -> str:
    out = []
    last_sep = False
    for ch in value:
        if ch.isascii() and ch.isalnum():
            out.append(ch.lower())
            last_sep = False
        elif not last_sep:
            out.append("_")
            last_sep = True
    text = "".join(out).strip("_")
    return text or "default"


def sha256_hex(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alias one WRF GDEX cached fetch entry onto another product family"
    )
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cycle-hour", required=True, type=int)
    parser.add_argument("--forecast-hour", required=True, type=int)
    parser.add_argument("--source-product", required=True)
    parser.add_argument("--target-product", required=True)
    args = parser.parse_args()

    base = (
        Path(args.cache_root)
        / "wrf_gdex"
        / args.date
        / f"{args.cycle_hour:02d}z"
        / f"f{args.forecast_hour:03d}"
    )
    source_root = base / sanitize_component(args.source_product) / "gdex" / "full"
    target_root = base / sanitize_component(args.target_product) / "gdex" / "full"
    source_bytes = source_root / "fetch.grib2"
    source_meta = source_root / "fetch_meta.json"
    target_bytes = target_root / "fetch.grib2"
    target_meta = target_root / "fetch_meta.json"
    target_decoded = target_root / "decoded"

    if not source_bytes.exists() or not source_meta.exists():
        raise SystemExit(f"missing source cache entry: {source_root}")

    target_root.mkdir(parents=True, exist_ok=True)
    if target_bytes.exists():
        target_bytes.unlink()
    try:
        os.link(source_bytes, target_bytes)
    except OSError:
        shutil.copyfile(source_bytes, target_bytes)

    source_payload = json.loads(source_meta.read_text())
    payload = source_payload["payload"]
    payload["request"]["product"] = args.target_product
    payload["resolved_family"] = args.target_product
    payload["bytes_len"] = target_bytes.stat().st_size
    payload["bytes_sha256"] = sha256_hex(target_bytes)
    target_meta.write_text(json.dumps(source_payload, indent=2))

    if target_decoded.exists():
        for child in target_decoded.iterdir():
            if child.is_file():
                child.unlink()
            else:
                shutil.rmtree(child)

    print(f"{args.source_product} -> {args.target_product}")
    print(target_bytes)


if __name__ == "__main__":
    main()
