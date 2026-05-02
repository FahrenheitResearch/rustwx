#!/usr/bin/env python3
"""Build a release-review inventory of non-ECAPE GRIB map products.

Input is the JSON produced by:

    cargo run -q -p rustwx-cli --bin product_catalog -- --out product_catalog.json

The output intentionally treats indexed direct-map support as the release
contract. Derived/windowed products are listed as catalog support because they
can require broader thermodynamic bundles and should still be smoked per model
before public claims.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


INDEXED_GRIB_MODELS = [
    "hrrr",
    "hrrr-ak",
    "gfs",
    "gdas",
    "gefs",
    "aigfs",
    "aigefs",
    "rap",
    "nam",
    "hiresw",
    "sref",
    "rtma",
    "urma",
    "nbm",
    "rrfs-a",
]

WHOLE_FILE_GRIB_EXCEPTIONS = {
    "ecmwf-open-data": "GRIB source without a NOAA-style .idx sidecar in the current adapter.",
}


def is_ecape(slug: str, title: str) -> bool:
    needle = f"{slug} {title}".lower()
    return "ecape" in needle


def load_entries(catalog: dict) -> list[dict]:
    entries = []
    for kind in ["direct", "derived", "windowed", "heavy"]:
        for item in catalog.get(kind, []):
            slug = item.get("slug") or item.get("id", {}).get("slug", "")
            title = item.get("title") or item.get("product_metadata", {}).get("display_name", slug)
            if is_ecape(slug, title):
                continue
            for support in item.get("support", []):
                target = support.get("target", "")
                if target not in INDEXED_GRIB_MODELS:
                    continue
                if support.get("status") != "supported":
                    continue
                entries.append(
                    {
                        "model": target,
                        "kind": kind,
                        "slug": slug,
                        "title": title,
                        "fetch_mode": support.get("fetch_mode", ""),
                        "grib_product": support.get("grib_product", ""),
                        "routes": ",".join(support.get("source_routes", [])),
                    }
                )
    return entries


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "model",
                "kind",
                "slug",
                "title",
                "fetch_mode",
                "grib_product",
                "routes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_md(rows: list[dict], path: Path) -> None:
    by_model: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_model[row["model"]].append(row)

    lines = [
        "# GRIB Map Inventory, Non-ECAPE",
        "",
        "Generated from `product_catalog.json`. This inventory is limited to model/product",
        "support that currently uses NOAA-style indexed GRIB subset fetches in rustwx.",
        "",
        "ECAPE products are excluded. Direct maps are the strongest compatibility signal;",
        "derived/windowed rows are catalog-level support and still deserve live smoke tests",
        "before release claims for a specific model/date/product combination.",
        "",
        "## Summary",
        "",
        "| Model | Direct maps | Derived maps | Windowed maps | Heavy maps | Indexed direct maps |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model in INDEXED_GRIB_MODELS:
        model_rows = by_model.get(model, [])
        counts = defaultdict(int)
        indexed_direct = 0
        for row in model_rows:
            counts[row["kind"]] += 1
            if row["kind"] == "direct" and row["fetch_mode"] == "IndexedSubset":
                indexed_direct += 1
        lines.append(
            f"| `{model}` | {counts['direct']} | {counts['derived']} | {counts['windowed']} | {counts['heavy']} | {indexed_direct} |"
        )

    lines.extend(
        [
            "",
            "## Whole-File GRIB Exceptions",
            "",
            "| Model | Reason |",
            "| --- | --- |",
        ]
    )
    for model, reason in WHOLE_FILE_GRIB_EXCEPTIONS.items():
        lines.append(f"| `{model}` | {reason} |")

    lines.extend(["", "## Per-Model Product List", ""])
    for model in INDEXED_GRIB_MODELS:
        model_rows = sorted(
            by_model.get(model, []),
            key=lambda row: (row["kind"], row["slug"], row["grib_product"]),
        )
        lines.append(f"### `{model}`")
        lines.append("")
        lines.append("| Kind | Slug | Title | Fetch | Product |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in model_rows:
            lines.append(
                f"| {row['kind']} | `{row['slug']}` | {row['title']} | {row['fetch_mode']} | `{row['grib_product']}` |"
            )
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    rows = load_entries(catalog)
    rows.sort(key=lambda row: (INDEXED_GRIB_MODELS.index(row["model"]), row["kind"], row["slug"]))

    write_csv(rows, args.out_dir / "grib_map_inventory_non_ecape.csv")
    write_md(rows, args.out_dir / "GRIB_MAP_INVENTORY_NON_ECAPE.md")
    print(f"wrote {len(rows)} rows to {args.out_dir}")


if __name__ == "__main__":
    main()
