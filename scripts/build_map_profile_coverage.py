#!/usr/bin/env python3
"""Build a coverage report for map-product warm-cache profiling artifacts.

The report is intentionally evidence-oriented: the product catalog defines the
universe, and local profile artifacts prove which product slugs currently have
one-product warm-cache timing evidence.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_DIRECT_PROFILES = [
    ("hrrr", "proof/direct_map_warm_profile_hrrr_20260604_00z_f000/warm_cache_product_profile.json"),
    ("hrrr", "proof/warm_hrrr_f000_retry_failed/warm_cache_product_profile.json"),
    ("hrrr", "proof/warm_hrrr_f000_composites_fixed/warm_cache_product_profile.json"),
    (
        "nbm",
        "proof/direct_map_warm_profile_nbm_20260604_00z_f006_warm_verified/warm_cache_product_profile.json",
    ),
]


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def supported_targets(entry: dict[str, Any]) -> list[str]:
    targets = []
    for support in entry.get("support", []):
        if support.get("status") == "supported":
            target = support.get("target")
            if isinstance(target, str):
                targets.append(target)
    return targets


def first_supported_target(entry: dict[str, Any]) -> str | None:
    targets = supported_targets(entry)
    return targets[0] if targets else None


def product_key(kind: str, slug: str) -> str:
    return f"{kind}:{slug}"


def load_catalog_rows(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for kind in ("direct", "derived", "windowed", "heavy"):
        for entry in catalog.get(kind, []):
            slug = entry.get("slug")
            if not isinstance(slug, str):
                continue
            targets = supported_targets(entry)
            rows.append(
                {
                    "kind": kind,
                    "slug": slug,
                    "title": entry.get("title"),
                    "catalog_status": entry.get("status"),
                    "maturity": entry.get("maturity"),
                    "render_style": entry.get("render_style"),
                    "supported_targets": targets,
                    "preferred_target": first_supported_target(entry),
                }
            )
    return rows


def direct_evidence_from_profile(
    repo: Path,
    profile_path: Path,
    target: str,
) -> dict[str, dict[str, Any]]:
    data = read_json(profile_path)
    if not data:
        return {}
    evidence: dict[str, dict[str, Any]] = {}
    for record in data.get("records", []):
        slug = record.get("slug")
        if not isinstance(slug, str):
            continue
        key = product_key("direct", slug)
        candidate = {
            "kind": "direct",
            "target": target,
            "profile_path": str(profile_path),
            "ok": bool(record.get("ok")),
            "warm_cache_ok": bool(record.get("warm_cache_ok")),
            "total_ms": record.get("total_ms"),
            "wall_ms": record.get("wall_ms"),
            "fetch_total_ms": record.get("fetch_total_ms"),
            "extract_ms": record.get("extract_ms"),
            "render_ms": record.get("render_ms"),
            "contour_ms": record.get("image_timing_contour_ms"),
            "output_path": record.get("output_path"),
        }
        previous = evidence.get(key)
        if previous is None or evidence_rank(candidate) >= evidence_rank(previous):
            evidence[key] = candidate
    return evidence


def evidence_rank(evidence: dict[str, Any]) -> tuple[int, int]:
    return (
        1 if evidence.get("ok") else 0,
        1 if evidence.get("warm_cache_ok") else 0,
    )


def merge_evidence(items: list[dict[str, dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in items:
        for key, evidence in item.items():
            previous = merged.get(key)
            if previous is None or evidence_rank(evidence) >= evidence_rank(previous):
                merged[key] = evidence
    return merged


def direct_greedy_plan(rows: list[dict[str, Any]], covered: set[str]) -> list[dict[str, Any]]:
    direct_rows = [row for row in rows if row["kind"] == "direct" and row["supported_targets"]]
    all_direct = {row["slug"] for row in direct_rows}
    remaining = set(all_direct) - covered
    plan = []
    while remaining:
        counts: dict[str, list[str]] = defaultdict(list)
        for row in direct_rows:
            if row["slug"] not in remaining:
                continue
            for target in row["supported_targets"]:
                counts[target].append(row["slug"])
        if not counts:
            break
        target, slugs = max(counts.items(), key=lambda item: (len(item[1]), item[0]))
        slugs = sorted(slugs)
        plan.append({"target": target, "count": len(slugs), "slugs": slugs})
        remaining.difference_update(slugs)
    return plan


def build_report(repo: Path, catalog_path: Path, direct_profiles: list[tuple[str, Path]]) -> dict[str, Any]:
    catalog = read_json(catalog_path)
    if not catalog:
        raise RuntimeError(f"catalog not found or unreadable: {catalog_path}")
    rows = load_catalog_rows(catalog)
    evidence = merge_evidence(
        [direct_evidence_from_profile(repo, path, target) for target, path in direct_profiles]
    )
    coverage_rows = []
    for row in rows:
        key = product_key(row["kind"], row["slug"])
        proof = evidence.get(key)
        if proof:
            status = (
                "warm_profiled"
                if proof.get("ok") and proof.get("warm_cache_ok")
                else "profiled_not_warm"
                if proof.get("ok")
                else "profile_failed"
            )
        elif row["supported_targets"]:
            status = "missing_profile"
        else:
            status = "not_supported"
        coverage_rows.append({**row, "coverage_status": status, "evidence": proof})

    by_kind: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in coverage_rows:
        by_kind[row["kind"]]["total"] += 1
        by_kind[row["kind"]][row["coverage_status"]] += 1
    direct_covered = {
        row["slug"]
        for row in coverage_rows
        if row["kind"] == "direct" and row["coverage_status"] == "warm_profiled"
    }
    return {
        "generated_at": now_iso(),
        "catalog_path": str(catalog_path),
        "direct_profile_paths": [str(path) for _, path in direct_profiles],
        "summary_by_kind": {kind: dict(counts) for kind, counts in sorted(by_kind.items())},
        "direct_remaining_plan": direct_greedy_plan(rows, direct_covered),
        "rows": coverage_rows,
    }


def write_csv(path: Path, report: dict[str, Any]) -> None:
    fieldnames = [
        "kind",
        "slug",
        "title",
        "coverage_status",
        "preferred_target",
        "supported_targets",
        "evidence_target",
        "total_ms",
        "fetch_total_ms",
        "extract_ms",
        "render_ms",
        "contour_ms",
        "profile_path",
        "output_path",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report["rows"]:
            evidence = row.get("evidence") or {}
            writer.writerow(
                {
                    "kind": row["kind"],
                    "slug": row["slug"],
                    "title": row.get("title"),
                    "coverage_status": row["coverage_status"],
                    "preferred_target": row.get("preferred_target"),
                    "supported_targets": ",".join(row.get("supported_targets") or []),
                    "evidence_target": evidence.get("target"),
                    "total_ms": evidence.get("total_ms"),
                    "fetch_total_ms": evidence.get("fetch_total_ms"),
                    "extract_ms": evidence.get("extract_ms"),
                    "render_ms": evidence.get("render_ms"),
                    "contour_ms": evidence.get("contour_ms"),
                    "profile_path": evidence.get("profile_path"),
                    "output_path": evidence.get("output_path"),
                }
            )


def write_html(path: Path, report: dict[str, Any]) -> None:
    def esc(value: Any) -> str:
        return html.escape("" if value is None else str(value))

    def fmt_ms(value: Any) -> str:
        try:
            return f"{int(float(value)):,}"
        except (TypeError, ValueError):
            return "-"

    summary_cards = []
    for kind, counts in report["summary_by_kind"].items():
        warm = counts.get("warm_profiled", 0)
        total = counts.get("total", 0)
        summary_cards.append(
            f"<div class='metric'><span>{esc(kind)}</span><strong>{warm} / {total}</strong></div>"
        )

    missing_rows = []
    for row in report["rows"]:
        if row["coverage_status"] == "warm_profiled":
            continue
        evidence = row.get("evidence") or {}
        missing_rows.append(
            "<tr>"
            f"<td><code>{esc(row['kind'])}</code></td>"
            f"<td><code>{esc(row['slug'])}</code></td>"
            f"<td>{esc(row.get('title'))}</td>"
            f"<td>{esc(row['coverage_status'])}</td>"
            f"<td>{esc(','.join(row.get('supported_targets') or []))}</td>"
            f"<td>{esc(evidence.get('target'))}</td>"
            "</tr>"
        )

    proven_rows = []
    for row in report["rows"]:
        if row["coverage_status"] != "warm_profiled":
            continue
        evidence = row["evidence"] or {}
        proven_rows.append(
            "<tr>"
            f"<td><code>{esc(row['kind'])}</code></td>"
            f"<td><code>{esc(row['slug'])}</code></td>"
            f"<td>{esc(evidence.get('target'))}</td>"
            f"<td>{fmt_ms(evidence.get('total_ms'))}</td>"
            f"<td>{fmt_ms(evidence.get('fetch_total_ms'))}</td>"
            f"<td>{fmt_ms(evidence.get('render_ms'))}</td>"
            f"<td>{fmt_ms(evidence.get('contour_ms'))}</td>"
            "</tr>"
        )

    plan_rows = []
    for item in report["direct_remaining_plan"]:
        plan_rows.append(
            "<tr>"
            f"<td><code>{esc(item['target'])}</code></td>"
            f"<td>{item['count']}</td>"
            f"<td>{esc(','.join(item['slugs'][:24]))}"
            f"{'...' if len(item['slugs']) > 24 else ''}</td>"
            "</tr>"
        )

    path.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="20">
<title>RustWX Map Profile Coverage</title>
<style>
body {{ margin: 0; font: 14px/1.45 system-ui, Segoe UI, sans-serif; background: #f7f8fa; color: #151a20; }}
header {{ background: #101820; color: #fff; padding: 18px 22px; }}
main {{ max-width: 1500px; margin: auto; padding: 18px 22px 40px; }}
section {{ background: #fff; border: 1px solid #d8dde3; border-radius: 8px; margin-bottom: 16px; padding: 16px; }}
h1 {{ margin: 0 0 4px; font-size: 22px; }}
h2 {{ margin: 0 0 10px; font-size: 18px; }}
.grid {{ display: grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap: 10px; }}
.metric {{ border: 1px solid #d8dde3; border-radius: 8px; background: #fbfcfd; padding: 10px; }}
.metric span {{ display: block; color: #5c6670; font-size: 12px; }}
.metric strong {{ display: block; font-size: 22px; margin-top: 2px; }}
.table-wrap {{ max-height: 620px; overflow: auto; border: 1px solid #d8dde3; border-radius: 8px; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ border-bottom: 1px solid #d8dde3; padding: 7px 8px; text-align: left; vertical-align: top; }}
th {{ background: #eef2f5; position: sticky; top: 0; z-index: 1; }}
code {{ font-family: Consolas, ui-monospace, monospace; font-size: 12px; }}
.note {{ color: #5c6670; }}
</style>
</head>
<body>
<header>
<h1>RustWX Map Profile Coverage</h1>
<div>Generated {esc(report['generated_at'])}; auto-refreshes every 20 seconds</div>
</header>
<main>
<section>
<h2>Coverage By Catalog Lane</h2>
<div class="grid">{''.join(summary_cards)}</div>
<p class="note">Warm-profiled means a one-product direct profile record exists and has <code>warm_cache_ok=true</code>. Non-direct lanes are intentionally still missing until their profilers are added or wired into this report.</p>
</section>
<section>
<h2>Next Direct Lanes</h2>
<div class="table-wrap"><table><thead><tr><th>Target</th><th>Unique products</th><th>Slugs</th></tr></thead><tbody>{''.join(plan_rows)}</tbody></table></div>
</section>
<section>
<h2>Missing Or Unproven Products</h2>
<div class="table-wrap"><table><thead><tr><th>Kind</th><th>Slug</th><th>Title</th><th>Status</th><th>Supported targets</th><th>Evidence target</th></tr></thead><tbody>{''.join(missing_rows)}</tbody></table></div>
</section>
<section>
<h2>Warm-Profiled Products</h2>
<div class="table-wrap"><table><thead><tr><th>Kind</th><th>Slug</th><th>Target</th><th>Total ms</th><th>Fetch total</th><th>Render</th><th>Contour</th></tr></thead><tbody>{''.join(proven_rows)}</tbody></table></div>
</section>
</main>
</body>
</html>
""",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument(
        "--catalog",
        default="proof/direct_map_warm_profile_nbm_20260604_00z_f006_warm_verified/catalog.json",
    )
    parser.add_argument("--out-dir", default="../../outputs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    catalog_path = (repo / args.catalog).resolve()
    out_dir = (repo / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    direct_profiles = [(target, (repo / rel).resolve()) for target, rel in DEFAULT_DIRECT_PROFILES]
    report = build_report(repo, catalog_path, direct_profiles)
    json_path = out_dir / "rustwx_map_profile_coverage.json"
    csv_path = out_dir / "rustwx_map_profile_coverage.csv"
    html_path = out_dir / "rustwx_map_profile_coverage.html"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_csv(csv_path, report)
    write_html(html_path, report)
    print(
        json.dumps(
            {
                "ok": True,
                "json": str(json_path),
                "csv": str(csv_path),
                "html": str(html_path),
                "summary_by_kind": report["summary_by_kind"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
