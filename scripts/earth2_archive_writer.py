#!/usr/bin/env python3
"""Place Earth2Studio NetCDF outputs into rustwx's local archive layout.

Canonical layout:
  {archive_root}/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc

The script intentionally only organizes already-produced NetCDF fields. Model
inference remains external so this writer can be reused for AIFS, AIFS-ENS,
Pangu, GraphCast, or future Earth2-produced fields without tying rustwx to one
inference package.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-netcdf", required=True, type=Path)
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=Path(os.environ["RUSTWX_EARTH2_ARCHIVE"])
        if os.environ.get("RUSTWX_EARTH2_ARCHIVE")
        else None,
        help="Archive root. Defaults to RUSTWX_EARTH2_ARCHIVE.",
    )
    parser.add_argument("--model", default="aifs")
    parser.add_argument(
        "--init-time",
        help="Initialization time, e.g. 2016-08-22T00:00:00Z or 20160822T00Z.",
    )
    parser.add_argument("--lead", "--lead-hours", dest="lead_hours", type=int)
    parser.add_argument(
        "--mode",
        choices=("copy", "move", "hardlink"),
        default="copy",
        help="How to materialize the archive file.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Optional manifest JSON path. Defaults next to the archived file.",
    )
    return parser.parse_args()


def normalize_init_time(value: str) -> tuple[str, int, str]:
    text = value.strip()
    compact = re.fullmatch(r"(\d{8})T(\d{2})Z", text)
    if compact:
        date, hour = compact.groups()
        return date, int(hour), f"{date}T{hour}Z"

    iso = text.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso).astimezone(timezone.utc)
    date = dt.strftime("%Y%m%d")
    hour = dt.hour
    return date, hour, f"{date}T{hour:02d}Z"


def infer_from_filename(path: Path) -> tuple[str | None, int | None]:
    match = re.search(r"(\d{8}T\d{2})(?:\d{4})?Z[_-]lead(\d{3})", path.name)
    if not match:
        return None, None
    init_text, lead = match.groups()
    return f"{init_text}Z", int(lead)


def copy_into_archive(src: Path, dst: Path, mode: str, overwrite: bool) -> None:
    if dst.exists():
        if not overwrite:
            raise FileExistsError(f"{dst} exists; pass --overwrite to replace it")
        dst.unlink()
    dst.parent.mkdir(parents=True, exist_ok=True)
    if mode == "copy":
        shutil.copy2(src, dst)
    elif mode == "move":
        shutil.move(str(src), str(dst))
    elif mode == "hardlink":
        os.link(src, dst)
    else:
        raise ValueError(f"unsupported mode {mode}")


def main() -> None:
    args = parse_args()
    if args.archive_root is None:
        raise SystemExit("--archive-root is required when RUSTWX_EARTH2_ARCHIVE is not set")
    src = args.input_netcdf.resolve()
    if not src.is_file():
        raise SystemExit(f"input NetCDF does not exist: {src}")

    inferred_init, inferred_lead = infer_from_filename(src)
    init_value = args.init_time or inferred_init
    lead_hours = args.lead_hours if args.lead_hours is not None else inferred_lead
    if init_value is None or lead_hours is None:
        raise SystemExit(
            "--init-time and --lead are required unless they can be inferred from the file name"
        )
    if lead_hours < 0 or lead_hours > 999:
        raise SystemExit("--lead must be between 0 and 999 hours")

    _, _, init_dir = normalize_init_time(init_value)
    dst = (
        args.archive_root.resolve()
        / args.model.lower()
        / init_dir
        / f"lead{lead_hours:03d}.nc"
    )
    copy_into_archive(src, dst, args.mode, args.overwrite)

    manifest = {
        "model": args.model.lower(),
        "archive_root": str(args.archive_root.resolve()),
        "source": str(src),
        "archived_path": str(dst),
        "init": init_dir,
        "lead_hours": lead_hours,
        "mode": args.mode,
        "layout": "{archive_root}/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc",
    }
    manifest_path = args.manifest or dst.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
