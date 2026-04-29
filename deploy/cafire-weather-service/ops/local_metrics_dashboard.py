from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


DEFAULT_SSH_TARGET = "root@178.104.59.253"
DEFAULT_REMOTE_PATH = "/opt/cafire-weather-service"
SNAPSHOT_CACHE_TTL_SEC = 60
SNAPSHOT_CACHE_LOCK = threading.Lock()
SNAPSHOT_CACHE: tuple[float, bytes] | None = None


REMOTE_SNAPSHOT_SCRIPT = r"""
from __future__ import annotations

import json
import subprocess
import urllib.request
from datetime import UTC, datetime
from pathlib import Path


def run(args, timeout=20, check=False):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if check and result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"{args[0]} exited {result.returncode}").strip())
    return result


def read_env():
    env = {}
    path = Path(".env")
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def api_get(path, api_key=None, timeout=20):
    headers = {}
    if api_key:
        headers["x-api-key"] = api_key
    request = urllib.request.Request(f"http://127.0.0.1:8000{path}", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"ok": False, "error": str(exc), "path": path}


def parse_compose_ps():
    result = run(["docker", "compose", "ps", "--format", "json"], timeout=20)
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        rows.append(
            {
                "service": item.get("Service"),
                "name": item.get("Name") or item.get("Names"),
                "state": item.get("State"),
                "status": item.get("Status"),
                "ports": item.get("Ports"),
            }
        )
    return {"ok": result.returncode == 0, "services": rows, "error": result.stderr.strip() or None}


def parse_docker_stats():
    result = run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], timeout=20)
    rows = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        rows[item.get("Name")] = {
            "cpu": item.get("CPUPerc"),
            "mem": item.get("MemUsage"),
            "mem_pct": item.get("MemPerc"),
            "net_io": item.get("NetIO"),
            "block_io": item.get("BlockIO"),
        }
    return rows


def parse_df():
    result = run(["df", "-B1", "/"], timeout=10, check=True)
    line = result.stdout.splitlines()[1]
    parts = line.split()
    return {
        "filesystem": parts[0],
        "size_bytes": int(parts[1]),
        "used_bytes": int(parts[2]),
        "free_bytes": int(parts[3]),
        "used_pct": int(parts[4].rstrip("%")),
        "mount": parts[5],
    }


def path_size(path):
    result = run(["du", "-sb", path], timeout=30)
    if result.returncode != 0:
        return None
    try:
        return int(result.stdout.split()[0])
    except Exception:
        return None


def host_status():
    meminfo = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, value = line.split(":", 1)
        raw = value.strip().split()[0]
        meminfo[key] = int(raw) * 1024
    load = Path("/proc/loadavg").read_text(encoding="utf-8").split()[:3]
    return {
        "hostname": run(["hostname"], timeout=5).stdout.strip(),
        "loadavg": load,
        "mem_total_bytes": meminfo.get("MemTotal"),
        "mem_available_bytes": meminfo.get("MemAvailable"),
    }


def summarize_run_status(block):
    block = block or {}
    run = block.get("run") or {}
    cycle = run.get("cycle") or {}
    build_report = block.get("build_report") or {}
    return {
        "enabled": block.get("enabled"),
        "status": block.get("status"),
        "date_yyyymmdd": cycle.get("date_yyyymmdd") or run.get("date_yyyymmdd"),
        "hour_utc": cycle.get("hour_utc") or run.get("cycle_utc"),
        "source": run.get("source"),
        "started_at_utc": block.get("started_at_utc"),
        "finished_at_utc": block.get("finished_at_utc"),
        "error": block.get("error"),
        "forecast_hour_count": len(block.get("forecast_hours") or []),
        "variable_count": len(block.get("variables") or []),
        "bounds": block.get("bounds"),
        "grid_points": build_report.get("grid_points"),
        "memory_bytes_estimate": build_report.get("memory_bytes_estimate"),
        "total_ms": build_report.get("total_ms"),
    }


def summarize_health(health):
    return {
        "ok": bool(health.get("ok")),
        "service": health.get("service"),
        "rustwx_version": health.get("rustwx_version"),
        "r2_enabled": health.get("r2_enabled"),
        "satellite_enabled": health.get("satellite_enabled"),
        "meteogram_warm": summarize_run_status(health.get("meteogram_warm")),
        "fast_meteogram_store": summarize_run_status(health.get("fast_meteogram_store")),
        "pressure_volume": health.get("pressure_volume"),
        "pressure_volume_builder": health.get("pressure_volume_builder"),
        "pressure_cross_sections": health.get("pressure_cross_sections"),
        "error": health.get("error"),
    }


def summarize_health_with_fallback(health, warm_status):
    summary = summarize_health(health)
    if summary["ok"] or not warm_status or warm_status.get("error"):
        return summary
    if warm_status.get("ok", True):
        summary["ok"] = True
        summary["degraded"] = True
        summary["error"] = f"/health failed; using /api/v1/public/warm-status fallback: {summary.get('error') or 'unknown error'}"
        summary["meteogram_warm"] = summarize_run_status(warm_status.get("fetch_decode_cache") or warm_status.get("meteogram_warm"))
        summary["fast_meteogram_store"] = summarize_run_status(
            warm_status.get("fast_store") or warm_status.get("fast_meteogram_store")
        )
        summary["pressure_volume"] = warm_status.get("pressure_volume")
        summary["pressure_volume_builder"] = warm_status.get("pressure_volume_builder")
        summary["pressure_cross_sections"] = warm_status.get("pressure_cross_sections")
    return summary


def summarize_manifest(path, label):
    data = api_get(path)
    hours = data.get("hours") or []
    uploaded_count = 0
    for hour in hours:
        uploaded = hour.get("uploaded") or []
        if isinstance(uploaded, list):
            uploaded_count += len(uploaded)
    return {
        "ok": bool(data.get("ok", True)) and "error" not in data,
        "label": label,
        "path": path,
        "generated_at_utc": data.get("generated_at_utc"),
        "scan_time_utc": data.get("scan_time_utc"),
        "scan_id": data.get("scan_id"),
        "date_yyyymmdd": data.get("date_yyyymmdd"),
        "cycle_utc": data.get("cycle_utc"),
        "domain": data.get("domain"),
        "product_count": len(data.get("products") or []),
        "artifact_count": len(data.get("artifacts") or []),
        "loop_count": len(data.get("loops") or []),
        "forecast_hour_count": len(data.get("forecast_hours") or []),
        "hour_count": len(hours),
        "uploaded_count": uploaded_count,
        "flash_count_total": data.get("flash_count_total"),
        "flash_count_in_domain": data.get("flash_count_in_domain"),
        "flash_count_drawn": data.get("flash_count_drawn"),
        "n_files": data.get("n_files"),
        "time_window": data.get("time_window"),
        "latest_glm_key": data.get("latest_glm_key"),
        "latest_glm_last_modified": data.get("latest_glm_last_modified"),
        "error": data.get("error"),
    }


def summarize_lightning_geojson():
    data = api_get("/api/v1/public/latest-lightning.geojson")
    features = data.get("features") or []
    return {
        "ok": bool(data.get("ok", True)) and "error" not in data and data.get("type") == "FeatureCollection",
        "path": "/api/v1/public/latest-lightning.geojson",
        "type": data.get("type"),
        "feature_count": len(features),
        "generated_at_utc": data.get("generated_at_utc"),
        "time_window": data.get("time_window"),
        "flash_count_total": data.get("flash_count_total"),
        "flash_count_in_domain": data.get("flash_count_in_domain"),
        "flash_count_drawn": data.get("flash_count_drawn"),
        "n_files": data.get("n_files"),
        "error": data.get("error"),
    }


def config_summary(env):
    keys = [
        "CACHE_CLEANUP_ENABLED",
        "CACHE_CLEANUP_INTERVAL_SEC",
        "CACHE_CLEANUP_MAX_AGE_HOURS",
        "CACHE_CLEANUP_MAX_CACHE_GB",
        "CACHE_CLEANUP_TARGET_CACHE_GB",
        "CACHE_CLEANUP_MIN_FREE_GB",
        "CACHE_CLEANUP_TARGET_FREE_GB",
        "CACHE_CLEANUP_EMERGENCY_MIN_AGE_HOURS",
        "LIGHTNING_ENABLED",
        "LIGHTNING_INTERVAL_SEC",
        "LIGHTNING_FETCH_COUNT",
        "LIGHTNING_LOOKBACK_HOURS",
        "LIGHTNING_MAX_AGE_MIN",
        "FAST_METEOGRAM_STORE_ENABLED",
        "FAST_METEOGRAM_STORE_BOUNDS",
        "PRESSURE_VOLUME_ENABLED",
        "PRESSURE_VOLUME_BASE_URL",
        "PRESSURE_VOLUME_STORE_PATH",
        "PRESSURE_VOLUME_BUILDER_ENABLED",
        "PRESSURE_VOLUME_BUILDER_INTERVAL_SEC",
        "PRESSURE_VOLUME_BUILDER_LOAD_PARALLELISM",
        "PRESSURE_VOLUME_BUILDER_REQUIRE_STATIC_MANIFEST",
        "PRESSURE_CROSS_SECTION_RENDER_MAX_ACTIVE",
        "PRESSURE_CROSS_SECTION_LOOP_MAX_ACTIVE",
        "PRESSURE_CROSS_SECTION_DEFAULT_TOP_HPA",
        "SATELLITE_ENABLED",
        "SATELLITE_INTERVAL_SEC",
        "SATELLITE_LOOP_ENABLED",
    ]
    return {key: env.get(key) for key in keys if key in env}


env = read_env()
api_key = env.get("SERVICE_API_KEY")
health = api_get("/health", timeout=30)
warm_status = api_get("/api/v1/public/warm-status", timeout=10)
compose = parse_compose_ps()
stats = parse_docker_stats()
for service in compose["services"]:
    service["stats"] = stats.get(service.get("name")) or {}

snapshot = {
    "ok": True,
    "generated_at_utc": datetime.now(UTC).isoformat(),
    "host": host_status(),
    "compose": compose,
    "disk": parse_df(),
    "data_dirs": {
        "cache_bytes": path_size("/opt/cafire-weather-service/data/cache"),
        "artifacts_bytes": path_size("/opt/cafire-weather-service/data/artifacts"),
        "glm_bytes": path_size("/opt/cafire-weather-service/data/glm"),
        "volume_stores_bytes": path_size("/opt/cafire-weather-service/data/volume-stores"),
    },
    "health": summarize_health_with_fallback(health, warm_status),
    "warm_status": warm_status,
    "pressure_volume_status": api_get("/api/v1/public/pressure-volume/status", timeout=10),
    "pressure_volume_builder_status": api_get("/api/v1/public/pressure-volume-builder/status", timeout=10),
    "lightning_geojson": summarize_lightning_geojson(),
    "metrics": api_get("/api/v1/metrics", api_key=api_key),
    "manifests": [
        summarize_manifest("/api/v1/public/latest-artifacts", "Hourly static maps"),
        summarize_manifest("/api/v1/public/latest-diurnal-artifacts", "Diurnal static maps"),
        summarize_manifest("/api/v1/public/latest-lightning-artifacts", "Lightning"),
        summarize_manifest("/api/v1/public/latest-satellite-artifacts", "Satellite"),
    ],
    "config": config_summary(env),
}
print(json.dumps(snapshot, separators=(",", ":"), sort_keys=True))
"""


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CA Fire Weather Ops</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f6f7;
      --panel: #ffffff;
      --text: #18222d;
      --muted: #64717f;
      --line: #d9e0e7;
      --head: #edf1f5;
      --good: #146c3f;
      --warn: #9c5a00;
      --bad: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      padding: 16px 22px 12px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    main { max-width: 1560px; margin: 0 auto; padding: 16px 22px 36px; }
    h1 { margin: 0; font-size: 22px; letter-spacing: 0; }
    h2 { margin: 20px 0 10px; font-size: 15px; letter-spacing: 0; }
    .meta { display: flex; flex-wrap: wrap; gap: 14px; margin-top: 5px; color: var(--muted); font-size: 13px; }
    .grid { display: grid; grid-template-columns: repeat(6, minmax(145px, 1fr)); gap: 10px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      min-height: 82px;
    }
    .label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .04em; }
    .value { margin-top: 7px; font-size: 22px; line-height: 1.1; font-weight: 700; overflow-wrap: anywhere; }
    .sub { margin-top: 5px; font-size: 12px; color: var(--muted); overflow-wrap: anywhere; }
    .good { color: var(--good); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .two { display: grid; grid-template-columns: minmax(0, 1fr) minmax(420px, .58fr); gap: 14px; align-items: start; }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    th, td {
      padding: 8px 9px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      font-size: 13px;
      vertical-align: top;
    }
    th:first-child, td:first-child { text-align: left; }
    th { background: var(--head); color: #384654; font-weight: 650; }
    tr:last-child td { border-bottom: 0; }
    code { font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace; font-size: 12px; }
    .notes {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px 12px;
    }
    .notes div { padding: 5px 0; border-bottom: 1px solid var(--line); font-size: 13px; }
    .notes div:last-child { border-bottom: 0; }
    @media (max-width: 1150px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .two { grid-template-columns: 1fr; }
      header, main { padding-left: 14px; padding-right: 14px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>CA Fire Weather Ops</h1>
    <div class="meta">
      <span id="connection">Loading...</span>
      <span id="generated"></span>
      <span>SSH source: root@178.104.59.253:/opt/cafire-weather-service</span>
      <span>R2 image-download traffic is still Cloudflare/R2 analytics, not this API process.</span>
    </div>
  </header>
  <main>
    <section class="grid" id="overview"></section>

    <section class="two">
      <div>
        <h2>Services</h2>
        <table>
          <thead><tr><th>Service</th><th>State</th><th>Status</th><th>CPU</th><th>Memory</th><th>I/O</th></tr></thead>
          <tbody id="services"></tbody>
        </table>
      </div>
      <div>
        <h2>Weak Points</h2>
        <div class="notes" id="weak"></div>
      </div>
    </section>

    <section class="two">
      <div>
        <h2>Products</h2>
        <table>
          <thead><tr><th>Product</th><th>Generated</th><th>Age</th><th>Hours</th><th>Files</th><th>Detail</th></tr></thead>
          <tbody id="products"></tbody>
        </table>
      </div>
      <div>
        <h2>Model Stores</h2>
        <table>
          <thead><tr><th>Store</th><th>Status</th><th>Run</th><th>Hours</th><th>Build</th></tr></thead>
          <tbody id="stores"></tbody>
        </table>
      </div>
    </section>

    <section class="two">
      <div>
        <h2>Endpoint Traffic</h2>
        <table>
          <thead><tr><th>Route</th><th>Count</th><th>RPS</th><th>P95</th><th>P99</th><th>Max</th><th>Errors</th></tr></thead>
          <tbody id="routes"></tbody>
        </table>
      </div>
      <div>
        <h2>Meteograms</h2>
        <table>
          <thead><tr><th>Path</th><th>Count</th><th>Total P95</th><th>Sample P95</th><th>Render P95</th><th>Fast Store</th></tr></thead>
          <tbody id="meteogram"></tbody>
        </table>
      </div>
    </section>

    <section class="two">
      <div>
        <h2>Slowest Requests</h2>
        <table>
          <thead><tr><th>Route</th><th>Status</th><th>Total</th><th>Time</th></tr></thead>
          <tbody id="slow-requests"></tbody>
        </table>
      </div>
      <div>
        <h2>Slowest Meteograms</h2>
        <table>
          <thead><tr><th>Endpoint</th><th>Path</th><th>Total</th><th>Render</th><th>Fetches</th></tr></thead>
          <tbody id="slow-meteograms"></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    const fmt = (v, suffix = "") => (v === null || v === undefined || Number.isNaN(v) ? "--" : `${v}${suffix}`);
    const ms = (v) => fmt(v, " ms");
    const pct = (v) => fmt(v, "%");
    const gb = (v) => v === null || v === undefined ? "--" : `${(v / 1024 ** 3).toFixed(1)}G`;
    const clsHigh = (value, warn, bad) => value >= bad ? "bad" : value >= warn ? "warn" : "good";
    const clsLow = (value, warn, bad) => value <= bad ? "bad" : value <= warn ? "warn" : "good";

    function card(label, value, sub, klass = "") {
      return `<div class="card"><div class="label">${esc(label)}</div><div class="value ${klass}">${esc(value)}</div><div class="sub">${esc(sub)}</div></div>`;
    }

    function ageSeconds(ts, nowTs) {
      if (!ts) return null;
      const value = Date.parse(ts);
      const now = Date.parse(nowTs);
      if (!Number.isFinite(value) || !Number.isFinite(now)) return null;
      return Math.max(0, Math.round((now - value) / 1000));
    }

    function ageText(sec) {
      if (sec === null || sec === undefined) return "--";
      if (sec < 90) return `${sec}s`;
      if (sec < 5400) return `${Math.round(sec / 60)}m`;
      return `${(sec / 3600).toFixed(1)}h`;
    }

    function windowMetrics(data) {
      const emptyLatency = { p95: null, p99: null, max: null };
      return data.metrics?.windows?.["5m"] || {
        requests: { count: 0, rps: 0, error_count: 0, error_rate_pct: 0, latency_ms: emptyLatency, routes: [], slowest: [] },
        meteograms: { count: 0, rps: 0, error_count: 0, error_rate_pct: 0, total_ms: emptyLatency, sample_total_ms: emptyLatency, render_total_ms: emptyLatency, fetch_count: emptyLatency, sample_paths: {}, slowest: [] },
      };
    }

    function overallIssues(data) {
      const win = windowMetrics(data);
      const req = win.requests;
      const met = win.meteograms;
      const disk = data.disk || {};
      const dirs = data.data_dirs || {};
      const services = data.compose?.services || [];
      const lightning = (data.manifests || []).find((m) => m.label === "Lightning") || {};
      const satellite = (data.manifests || []).find((m) => m.label === "Satellite") || {};
      const lightningGeojson = data.lightning_geojson || {};
      const pressure = data.health?.pressure_cross_sections || {};
      const pressureBuilder = data.health?.pressure_volume_builder || data.pressure_volume_builder_status || {};
      const lightningAge = ageSeconds(lightning.time_window?.last || lightning.generated_at_utc, data.generated_at_utc);
      const satelliteAge = ageSeconds(satellite.scan_time_utc || satellite.generated_at_utc, data.generated_at_utc);
      const satellitePublishAge = ageSeconds(satellite.generated_at_utc, data.generated_at_utc);
      const cacheCap = Number(data.config?.CACHE_CLEANUP_MAX_CACHE_GB || 300);
      const notes = [];

      const down = services.filter((s) => s.state !== "running");
      if (down.length) notes.push(["bad", `Down services: ${down.map((s) => s.service).join(", ")}.`]);
      if (data.health?.degraded) notes.push(["warn", data.health?.error || "Health endpoint used fallback status."]);
      else if (!data.health?.ok) notes.push(["bad", `API health is not OK: ${data.health?.error || "unknown error"}.`]);
      if ((disk.free_bytes || 0) < 160 * 1024 ** 3) notes.push(["bad", `Root disk free is ${gb(disk.free_bytes)}; below the 160G floor.`]);
      else if ((disk.free_bytes || 0) < 220 * 1024 ** 3) notes.push(["warn", `Root disk free is ${gb(disk.free_bytes)}; janitor target is 220G.`]);
      if ((dirs.cache_bytes || 0) > cacheCap * 1024 ** 3) notes.push(["bad", `Cache is ${gb(dirs.cache_bytes)}; over the ${cacheCap}G cap.`]);
      else if ((dirs.cache_bytes || 0) > cacheCap * 0.9 * 1024 ** 3) notes.push(["warn", `Cache is ${gb(dirs.cache_bytes)}; close to the ${cacheCap}G cap.`]);
      if (lightningAge !== null && lightningAge > 180) notes.push(["warn", `Lightning latest age is ${ageText(lightningAge)}; target is roughly sub-2-minute when NOAA publishes.`]);
      if (satellite.ok === false) notes.push(["bad", `Satellite manifest is not serving: ${satellite.error || "unknown error"}.`]);
      else if (satelliteAge !== null && satelliteAge > 30 * 60) notes.push(["warn", `Satellite scan age is ${ageText(satelliteAge)}; published ${ageText(satellitePublishAge)} ago.`]);
      if (lightningGeojson.ok === false) notes.push(["bad", `Lightning GeoJSON is not serving: ${lightningGeojson.error || "unknown error"}.`]);
      if (pressure.enabled && pressure.status !== "ready") notes.push(["warn", `Pressure cross-section renderer is ${pressure.status || "unknown"}: ${pressure.detail || "no detail"}.`]);
      if (pressureBuilder.enabled && pressureBuilder.status === "error") notes.push(["bad", `Pressure VolumeStore builder failed: ${pressureBuilder.error || "unknown error"}.`]);
      if ((req.error_rate_pct || 0) > 1) notes.push(["bad", `API error rate is ${req.error_rate_pct}% over 5m.`]);
      if ((req.latency_ms?.p95 || 0) > 1000) notes.push(["warn", `API p95 is ${req.latency_ms.p95} ms over 5m.`]);
      if ((met.error_rate_pct || 0) > 1) notes.push(["bad", `Meteogram error rate is ${met.error_rate_pct}% over 5m.`]);
      if ((met.total_ms?.p95 || 0) > 3000) notes.push(["warn", `Meteogram total p95 is ${met.total_ms.p95} ms over 5m.`]);
      if ((met.count || 0) >= 5 && (met.fast_store_hit_rate_pct || 0) < 80) notes.push(["warn", `Fast-store hit rate is ${met.fast_store_hit_rate_pct}%.`]);
      if (!notes.length) notes.push(["good", "No obvious server, disk, API, lightning, or meteogram issue in the current snapshot."]);
      notes.push(["warn", "Static PNG/WebP download volume is outside this process; check Cloudflare/R2 analytics for bandwidth and CDN cache behavior."]);
      return notes;
    }

    function render(data) {
      const win = windowMetrics(data);
      const req = win.requests;
      const met = win.meteograms;
      const services = data.compose?.services || [];
      const running = services.filter((s) => s.state === "running").length;
      const disk = data.disk || {};
      const dirs = data.data_dirs || {};
      const fast = data.health?.fast_meteogram_store || {};
      const pressure = data.health?.pressure_cross_sections || data.pressure_volume_status || {};
      const pressureBuilder = data.health?.pressure_volume_builder || data.pressure_volume_builder_status || {};
      const lightning = (data.manifests || []).find((m) => m.label === "Lightning") || {};
      const satellite = (data.manifests || []).find((m) => m.label === "Satellite") || {};
      const lightningGeojson = data.lightning_geojson || {};
      const hourly = (data.manifests || []).find((m) => m.label === "Hourly static maps") || {};
      const lightningAge = ageSeconds(lightning.time_window?.last || lightning.generated_at_utc, data.generated_at_utc);
      const satelliteAge = ageSeconds(satellite.scan_time_utc || satellite.generated_at_utc, data.generated_at_utc);
      const satellitePublishAge = ageSeconds(satellite.generated_at_utc, data.generated_at_utc);
      const hourlyAge = ageSeconds(hourly.generated_at_utc, data.generated_at_utc);
      const cacheCap = Number(data.config?.CACHE_CLEANUP_MAX_CACHE_GB || 300);

      $("generated").textContent = `Snapshot ${data.generated_at_utc}`;
      $("overview").innerHTML = [
        card("Overall", overallIssues(data).some(([c]) => c === "bad") ? "Degraded" : "OK", `${running}/${services.length} services running`, overallIssues(data).some(([c]) => c === "bad") ? "bad" : overallIssues(data).some(([c]) => c === "warn") ? "warn" : "good"),
        card("Root disk free", gb(disk.free_bytes), `${disk.used_pct ?? "--"}% used`, clsLow(disk.free_bytes || 0, 220 * 1024 ** 3, 160 * 1024 ** 3)),
        card("Cache", gb(dirs.cache_bytes), `cap ${cacheCap}G`, clsHigh((dirs.cache_bytes || 0) / 1024 ** 3, cacheCap * 0.9, cacheCap)),
        card("Fast store", fast.status || "--", `${fast.date_yyyymmdd || "--"} ${fmt(fast.hour_utc)}Z`, fast.status === "ready" ? "good" : "warn"),
        card("VolumeStore", pressure.status || "--", `${gb(dirs.volume_stores_bytes)} on disk`, pressure.status === "ready" ? "good" : pressure.enabled ? "warn" : ""),
        card("Volume build", pressureBuilder.status || "--", `${pressureBuilder.date_yyyymmdd || "--"} ${fmt(pressureBuilder.cycle_utc)}Z`, pressureBuilder.status === "ready" ? "good" : pressureBuilder.status === "error" ? "bad" : "warn"),
        card("Cross sections", pressure.status || "--", `${fmt(pressure.product_count)} products; R ${fmt(pressure.render_slots?.active)}/${fmt(pressure.render_slots?.max)} L ${fmt(pressure.loop_slots?.active)}/${fmt(pressure.loop_slots?.max)}`, pressure.status === "ready" ? "good" : pressure.enabled ? "warn" : ""),
        card("Lightning age", ageText(lightningAge), `${fmt(lightning.flash_count_in_domain)} CA flashes; ${fmt(lightning.n_files)} files`, clsHigh(lightningAge || 0, 120, 300)),
        card("Lightning GeoJSON", fmt(lightningGeojson.feature_count), lightningGeojson.ok === false ? lightningGeojson.error || "error" : "Mapbox source", lightningGeojson.ok === false ? "bad" : "good"),
        card("Satellite scan", ageText(satelliteAge), `${fmt(satellite.product_count)} products; ${fmt(satellite.loop_count)} loops; published ${ageText(satellitePublishAge)}`, satellite.ok === false ? "bad" : clsHigh(satelliteAge || 0, 20 * 60, 45 * 60)),
        card("Static hours", `${fmt(hourly.hour_count)} hrs`, `${ageText(hourlyAge)} old; ${fmt(hourly.uploaded_count)} files`, clsHigh(hourlyAge || 0, 3 * 3600, 6 * 3600)),
        card("API p95", ms(req.latency_ms?.p95), `${req.count} req / 5m`, clsHigh(req.latency_ms?.p95 || 0, 500, 1500)),
        card("API errors", pct(req.error_rate_pct), `${req.error_count} errors / 5m`, clsHigh(req.error_rate_pct || 0, 1, 5)),
        card("Meteogram p95", ms(met.total_ms?.p95), `${met.count} req / 5m`, clsHigh(met.total_ms?.p95 || 0, 2500, 6000)),
        card("Fast-store hit", pct(met.fast_store_hit_rate_pct), "meteogram samples", clsLow(met.fast_store_hit_rate_pct ?? 100, 80, 50)),
        card("Host load", (data.host?.loadavg || []).join(" "), `${gb(data.host?.mem_available_bytes)} mem free`),
        card("Artifacts", gb(dirs.artifacts_bytes), `GLM ${gb(dirs.glm_bytes)}; volumes ${gb(dirs.volume_stores_bytes)}`),
      ].join("");

      $("services").innerHTML = services.map((s) => `
        <tr><td><code>${esc(s.service)}</code></td><td class="${s.state === "running" ? "good" : "bad"}">${esc(s.state)}</td><td>${esc(s.status)}</td><td>${esc(s.stats?.cpu || "--")}</td><td>${esc(s.stats?.mem || "--")} (${esc(s.stats?.mem_pct || "--")})</td><td>${esc(s.stats?.block_io || "--")}</td></tr>
      `).join("") || `<tr><td colspan="6">No Compose services returned.</td></tr>`;

      $("weak").innerHTML = overallIssues(data).map(([klass, text]) => `<div class="${klass}">${esc(text)}</div>`).join("");

      $("products").innerHTML = (data.manifests || []).map((m) => {
        const age = ageSeconds(m.time_window?.last || m.scan_time_utc || m.generated_at_utc, data.generated_at_utc);
        const detail = m.label === "Lightning"
          ? `${fmt(m.flash_count_in_domain)} CA / ${fmt(m.flash_count_total)} total flashes`
          : m.label === "Satellite"
          ? `${fmt(m.product_count)} products; ${fmt(m.artifact_count)} artifacts; ${fmt(m.loop_count)} loops`
          : `${fmt(m.product_count)} products`;
        return `<tr><td>${esc(m.label)}</td><td>${esc(m.generated_at_utc || "--")}</td><td>${ageText(age)}</td><td>${fmt(m.hour_count || m.forecast_hour_count)}</td><td>${fmt(m.uploaded_count)}</td><td>${esc(detail)}</td></tr>`;
      }).join("");

      const warm = data.health?.meteogram_warm || {};
      $("stores").innerHTML = [
        ["Fetch/decode warm", warm],
        ["Fast meteogram store", fast],
        ["Pressure store builder", pressureBuilder],
        ["Pressure cross-sections", pressure],
      ].map(([name, s]) => `<tr><td>${esc(name)}</td><td class="${s.status === "ready" ? "good" : "warn"}">${esc(s.status || "--")}</td><td>${esc((s.date_yyyymmdd || "--") + " " + fmt(s.hour_utc ?? s.cycle_utc) + "Z")}</td><td>${fmt(s.forecast_hour_count || s.expected_forecast_hours)}</td><td>${ms(s.total_ms || s.elapsed_ms)}</td></tr>`).join("");

      $("routes").innerHTML = (req.routes || []).map((r) => `
        <tr><td><code>${esc(r.route)}</code></td><td>${r.count}</td><td>${r.rps}</td><td>${ms(r.latency_ms?.p95)}</td><td>${ms(r.latency_ms?.p99)}</td><td>${ms(r.latency_ms?.max)}</td><td>${r.error_count}</td></tr>
      `).join("") || `<tr><td colspan="7">No request traffic in this window.</td></tr>`;

      const samplePaths = met.sample_paths || {};
      $("meteogram").innerHTML = Object.keys(samplePaths).map((path) => `
        <tr><td><code>${esc(path)}</code></td><td>${samplePaths[path]}</td><td>${ms(met.total_ms?.p95)}</td><td>${ms(met.sample_total_ms?.p95)}</td><td>${ms(met.render_total_ms?.p95)}</td><td>${pct(met.fast_store_hit_rate_pct)}</td></tr>
      `).join("") || `<tr><td colspan="6">No meteogram traffic in this window.</td></tr>`;

      $("slow-requests").innerHTML = (req.slowest || []).map((r) => `
        <tr><td><code>${esc(r.route)}</code></td><td>${r.status_code}</td><td>${ms(r.total_ms)}</td><td>${esc(r.at_utc)}</td></tr>
      `).join("") || `<tr><td colspan="4">No request traffic in this window.</td></tr>`;

      $("slow-meteograms").innerHTML = (met.slowest || []).map((m) => `
        <tr><td><code>${esc(m.endpoint)}</code></td><td><code>${esc(m.sample_path)}</code></td><td>${ms(m.total_ms)}</td><td>${ms(m.render_total_ms)}</td><td>${fmt(m.fetch_count)}</td></tr>
      `).join("") || `<tr><td colspan="5">No meteogram traffic in this window.</td></tr>`;
    }

    async function refresh() {
      try {
        const response = await fetch("/snapshot", { cache: "no-store" });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || response.statusText);
        $("connection").textContent = "Connected";
        $("connection").className = "good";
        render(data);
      } catch (err) {
        $("connection").textContent = `Error: ${err.message}`;
        $("connection").className = "bad";
      }
    }
    refresh();
    setInterval(refresh, 60000);
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    ssh_target = DEFAULT_SSH_TARGET
    remote_path = DEFAULT_REMOTE_PATH

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/":
            self._send(200, "text/html; charset=utf-8", HTML.encode("utf-8"))
            return
        if path in {"/snapshot", "/metrics"}:
            try:
                payload = get_snapshot_cached(self.ssh_target, self.remote_path)
                self._send(200, "application/json; charset=utf-8", payload)
            except Exception as exc:
                body = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
                self._send(502, "application/json; charset=utf-8", body)
            return
        self._send(404, "text/plain; charset=utf-8", b"not found")

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def get_snapshot_cached(ssh_target: str, remote_path: str) -> bytes:
    global SNAPSHOT_CACHE

    now = time.monotonic()
    cached = SNAPSHOT_CACHE
    if cached and now - cached[0] < SNAPSHOT_CACHE_TTL_SEC:
        return cached[1]

    if not SNAPSHOT_CACHE_LOCK.acquire(blocking=False):
        if cached:
            return with_dashboard_warning(cached[1], "refresh already running; returning cached snapshot")
        with SNAPSHOT_CACHE_LOCK:
            if SNAPSHOT_CACHE:
                return SNAPSHOT_CACHE[1]
            return fetch_remote_snapshot(ssh_target, remote_path)

    try:
        cached = SNAPSHOT_CACHE
        now = time.monotonic()
        if cached and now - cached[0] < SNAPSHOT_CACHE_TTL_SEC:
            return cached[1]
        payload = fetch_remote_snapshot(ssh_target, remote_path)
        SNAPSHOT_CACHE = (time.monotonic(), payload)
        return payload
    except Exception as exc:
        if cached:
            return with_dashboard_warning(cached[1], f"refresh failed; returning cached snapshot: {exc}")
        raise
    finally:
        SNAPSHOT_CACHE_LOCK.release()


def with_dashboard_warning(payload: bytes, warning: str) -> bytes:
    data = json.loads(payload)
    data["dashboard_warning"] = warning
    data["dashboard_stale_snapshot"] = True
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def fetch_remote_snapshot(ssh_target: str, remote_path: str) -> bytes:
    quoted_path = shlex.quote(remote_path)
    remote_command = f"cd {quoted_path} && python3 -"
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_target, remote_command],
        check=False,
        capture_output=True,
        input=REMOTE_SNAPSHOT_SCRIPT.encode("utf-8"),
        timeout=90,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(stderr or f"ssh exited {result.returncode}")
    json.loads(result.stdout)
    return result.stdout


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve a local dashboard for CA Fire Weather ops")
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--ssh-target", default=DEFAULT_SSH_TARGET)
    parser.add_argument("--remote-path", default=DEFAULT_REMOTE_PATH)
    args = parser.parse_args()

    DashboardHandler.ssh_target = args.ssh_target
    DashboardHandler.remote_path = args.remote_path
    server = ThreadingHTTPServer((args.bind, args.port), DashboardHandler)
    print(f"Dashboard: http://{args.bind}:{args.port}")
    print(f"Remote: {args.ssh_target}:{args.remote_path}")
    server.serve_forever()


if __name__ == "__main__":
    main()
