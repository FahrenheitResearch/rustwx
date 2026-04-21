#!/usr/bin/env python3
import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path


FAMILIES = {
    "hist2d": ("hist2D", "wrf2d_d01"),
    "hist3d": ("hist3D", "wrf3d_d01"),
    "future2d": ("future2D", "wrf2d_d01"),
    "future3d": ("future3D", "wrf3d_d01"),
}


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


def valid_time(date_yyyymmdd: str, cycle_hour: int, forecast_hour: int) -> datetime:
    base = datetime.strptime(date_yyyymmdd, "%Y%m%d").replace(tzinfo=timezone.utc)
    return base + timedelta(hours=cycle_hour + forecast_hour)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a WRF GDEX cache entry from OSDF direct")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--family", required=True, choices=sorted(FAMILIES))
    parser.add_argument("--date", required=True, help="Cycle date in YYYYMMDD")
    parser.add_argument("--cycle-hour", required=True, type=int)
    parser.add_argument("--forecast-hour", required=True, type=int)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    branch, filename_prefix = FAMILIES[args.family]
    product = f"{args.dataset}-{args.family}"
    when = valid_time(args.date, args.cycle_hour, args.forecast_hour)
    month = when.strftime("%Y%m")
    stamp = when.strftime("%Y-%m-%d_%H:00:00")
    filename = f"{filename_prefix}_{stamp}.nc"
    url = (
        f"https://osdf-director.osg-htc.org/ncar/gdex/"
        f"{args.dataset}/{branch}/{month}/{filename}"
    )

    root = (
        Path(args.cache_root)
        / "wrf_gdex"
        / args.date
        / f"{args.cycle_hour:02d}z"
        / f"f{args.forecast_hour:03d}"
        / sanitize_component(product)
        / "gdex"
        / "full"
    )
    root.mkdir(parents=True, exist_ok=True)
    bytes_path = root / "fetch.grib2"
    meta_path = root / "fetch_meta.json"

    if args.force and bytes_path.exists():
        bytes_path.unlink()
    if args.force and meta_path.exists():
        meta_path.unlink()

    if not bytes_path.exists():
        subprocess.run(
            ["curl", "-fL", "-C", "-", "-o", str(bytes_path), url],
            check=True,
        )

    metadata = {
        "schema_version": 2,
        "payload": {
            "request": {
                "model": "WrfGdex",
                "cycle": {
                    "date_yyyymmdd": args.date,
                    "hour_utc": args.cycle_hour,
                },
                "forecast_hour": args.forecast_hour,
                "product": product,
            },
            "source_override": "Gdex",
            "variable_patterns": [],
            "resolved_source": "Gdex",
            "resolved_url": url,
            "resolved_family": product,
            "bytes_len": bytes_path.stat().st_size,
            "bytes_sha256": sha256_hex(bytes_path),
        },
    }
    meta_path.write_text(json.dumps(metadata, indent=2))

    print(product)
    print(url)
    print(bytes_path)


if __name__ == "__main__":
    main()
