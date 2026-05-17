use anyhow::{Context, Result, anyhow};
use clap::Parser;
use rustwx_products::volume_store::{GridSpec, RouteDef, VolumeStore};
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(
    name = "volume-store-dashboard",
    about = "Serve a local OpenStreetMap dashboard for a rustwx VolumeStore"
)]
struct Args {
    #[arg(long, default_value = "proof/hrrr_pressure_volume_store_warm/store")]
    store: PathBuf,
    #[arg(long, default_value_t = 8787)]
    port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
}

#[derive(Debug, Serialize)]
struct DashboardMetadata {
    model: String,
    domain: String,
    product: String,
    cycle: String,
    variables: Vec<String>,
    forecast_hours: Vec<u16>,
    levels_hpa: Vec<u16>,
    grid: GridSummary,
}

#[derive(Debug, Serialize)]
struct GridSummary {
    kind: String,
    nx: usize,
    ny: usize,
    bounds: Option<(f64, f64, f64, f64)>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let store = Arc::new(
        VolumeStore::open(&args.store)
            .map_err(|err| anyhow!(err.to_string()))
            .with_context(|| format!("open VolumeStore at {}", args.store.display()))?,
    );
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).with_context(|| format!("bind {addr}"))?;
    println!("volume-store dashboard: http://{addr}/");
    println!("store: {}", args.store.display());

    for stream in listener.incoming() {
        let stream = stream?;
        let store = Arc::clone(&store);
        thread::spawn(move || {
            if let Err(err) = handle_connection(stream, store) {
                eprintln!("dashboard request failed: {err:#}");
            }
        });
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<VolumeStore>) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or("/");
    if method != "GET" {
        return write_response(
            &mut stream,
            405,
            "application/json",
            br#"{"error":"method not allowed"}"#,
        );
    }

    let (path, query) = split_target(target);
    match path {
        "/" => write_response(
            &mut stream,
            200,
            "text/html; charset=utf-8",
            DASHBOARD_HTML.as_bytes(),
        ),
        "/api/metadata" => write_json(&mut stream, &metadata(&store)?),
        "/api/point" => {
            let params = parse_query(query);
            let lat = parse_f64(&params, "lat")?;
            let lon = parse_f64(&params, "lon")?;
            let started = Instant::now();
            let manifest = store.manifest();
            let variables = manifest
                .variables
                .iter()
                .map(|variable| variable.name.as_str())
                .collect::<Vec<_>>();
            let profile = store
                .sample_point_3d(
                    lat,
                    lon,
                    &variables,
                    &manifest.forecast_hours,
                    &manifest.levels_hpa,
                )
                .map_err(|err| anyhow!(err.to_string()))?;
            write_json(
                &mut stream,
                &json!({
                    "elapsed_ms": started.elapsed().as_millis(),
                    "profile": profile
                }),
            )
        }
        "/api/cross-section" => {
            let params = parse_query(query);
            let lat1 = parse_f64(&params, "lat1")?;
            let lon1 = parse_f64(&params, "lon1")?;
            let lat2 = parse_f64(&params, "lat2")?;
            let lon2 = parse_f64(&params, "lon2")?;
            let hour =
                parse_u16(&params, "hour").unwrap_or_else(|_| store.manifest().forecast_hours[0]);
            let variable = params.get("variable").map(String::as_str).unwrap_or("TMP");
            let spacing_km = params
                .get("spacing_km")
                .and_then(|value| value.parse::<f32>().ok())
                .unwrap_or(20.0);
            let route = RouteDef {
                id: "dashboard_route".to_string(),
                name: "Dashboard route".to_string(),
                points: vec![(lat1, lon1), (lat2, lon2)],
                sample_spacing_km: spacing_km,
            };
            let started = Instant::now();
            let section = store
                .sample_route_3d(&route, &[variable], hour, &store.manifest().levels_hpa)
                .map_err(|err| anyhow!(err.to_string()))?;
            write_json(
                &mut stream,
                &json!({
                    "elapsed_ms": started.elapsed().as_millis(),
                    "section": section
                }),
            )
        }
        _ => write_response(
            &mut stream,
            404,
            "application/json",
            br#"{"error":"not found"}"#,
        ),
    }
}

fn metadata(store: &VolumeStore) -> Result<DashboardMetadata> {
    let manifest = store.manifest();
    Ok(DashboardMetadata {
        model: manifest.model.clone(),
        domain: manifest.domain.clone(),
        product: manifest.product.clone(),
        cycle: manifest.cycle.clone(),
        variables: manifest
            .variables
            .iter()
            .map(|variable| variable.name.clone())
            .collect(),
        forecast_hours: manifest.forecast_hours.clone(),
        levels_hpa: manifest.levels_hpa.clone(),
        grid: grid_summary(&manifest.grid),
    })
}

fn grid_summary(grid: &GridSpec) -> GridSummary {
    match grid {
        GridSpec::RegularLatLon {
            nx,
            ny,
            west_lon_deg,
            east_lon_deg,
            south_lat_deg,
            north_lat_deg,
        } => GridSummary {
            kind: "regular_lat_lon".to_string(),
            nx: *nx,
            ny: *ny,
            bounds: Some((*west_lon_deg, *east_lon_deg, *south_lat_deg, *north_lat_deg)),
        },
        GridSpec::LambertConformal { nx, ny, .. } => GridSummary {
            kind: "lambert_conformal".to_string(),
            nx: *nx,
            ny: *ny,
            bounds: None,
        },
        GridSpec::CurvilinearLatLon {
            nx,
            ny,
            lat_deg,
            lon_deg,
            ..
        } => {
            let bounds = lat_lon_bounds(lat_deg, lon_deg);
            GridSummary {
                kind: "curvilinear_lat_lon".to_string(),
                nx: *nx,
                ny: *ny,
                bounds,
            }
        }
    }
}

fn lat_lon_bounds(lat: &[f32], lon: &[f32]) -> Option<(f64, f64, f64, f64)> {
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for (&lat, &lon) in lat.iter().zip(lon.iter()) {
        if lat.is_finite() && lon.is_finite() {
            west = west.min(f64::from(lon));
            east = east.max(f64::from(lon));
            south = south.min(f64::from(lat));
            north = north.max(f64::from(lat));
        }
    }
    west.is_finite().then_some((west, east, south, north))
}

fn split_target(target: &str) -> (&str, &str) {
    target
        .split_once('?')
        .map(|(path, query)| (path, query))
        .unwrap_or((target, ""))
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    let mut params = BTreeMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(percent_decode(key), percent_decode(value));
    }
    params
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = &value[i + 1..i + 3];
                if let Ok(decoded) = u8::from_str_radix(hex, 16) {
                    out.push(decoded);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_f64(params: &BTreeMap<String, String>, key: &str) -> Result<f64> {
    params
        .get(key)
        .ok_or_else(|| anyhow!("missing query parameter '{key}'"))?
        .parse::<f64>()
        .with_context(|| format!("parse query parameter '{key}' as f64"))
}

fn parse_u16(params: &BTreeMap<String, String>, key: &str) -> Result<u16> {
    params
        .get(key)
        .ok_or_else(|| anyhow!("missing query parameter '{key}'"))?
        .parse::<u16>()
        .with_context(|| format!("parse query parameter '{key}' as u16"))
}

fn write_json<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    write_response(stream, 200, "application/json", &body)
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}

const DASHBOARD_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>rustwx VolumeStore Dashboard</title>
  <style>
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Inter, Segoe UI, Arial, sans-serif; color: #1d2329; background: #f4f2ee; }
    header { height: 52px; display: flex; align-items: center; gap: 16px; padding: 0 18px; background: #20262b; color: #f8f5ef; }
    header strong { font-size: 15px; letter-spacing: .02em; }
    header span { color: #cbd2d9; font-size: 13px; }
    main { display: grid; grid-template-columns: minmax(480px, 1.25fr) minmax(420px, .75fr); gap: 12px; padding: 12px; height: calc(100vh - 52px); }
    #mapPanel, #dataPanel { min-height: 0; background: white; border: 1px solid #d6d1c8; border-radius: 6px; overflow: hidden; }
    #mapToolbar { height: 42px; display: flex; align-items: center; gap: 8px; padding: 6px 8px; border-bottom: 1px solid #ded9d0; background: #faf8f4; }
    button, select { height: 30px; border: 1px solid #b9b2a7; background: white; color: #1d2329; border-radius: 4px; padding: 0 9px; font-size: 13px; }
    button.active { background: #2f5f7b; color: white; border-color: #2f5f7b; }
    #map { position: relative; height: calc(100% - 42px); overflow: hidden; background: #d6d2c8; cursor: crosshair; user-select: none; }
    .tile { position: absolute; width: 256px; height: 256px; image-rendering: auto; }
    #overlay { position: absolute; inset: 0; pointer-events: none; }
    #status { margin-left: auto; min-width: 180px; text-align: right; font-size: 12px; color: #5d6570; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    #dataPanel { display: grid; grid-template-rows: 42px 1fr 1fr 1fr; }
    .panelTitle { display: flex; align-items: center; gap: 8px; padding: 6px 10px; background: #faf8f4; border-bottom: 1px solid #ded9d0; font-size: 13px; }
    .chartBlock { min-height: 0; padding: 8px 10px; border-bottom: 1px solid #ded9d0; }
    .chartBlock:last-child { border-bottom: 0; }
    canvas { width: 100%; height: 100%; display: block; background: #fbfaf7; border: 1px solid #e0dbd1; }
    #metaLine { font-size: 12px; color: #67707a; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    @media (max-width: 980px) {
      main { grid-template-columns: 1fr; grid-template-rows: 58vh 1fr; height: auto; min-height: calc(100vh - 52px); }
      #dataPanel { min-height: 680px; }
    }
  </style>
</head>
<body>
  <header>
    <strong>rustwx VolumeStore Dashboard</strong>
    <span id="headerMeta">loading store...</span>
  </header>
  <main>
    <section id="mapPanel">
      <div id="mapToolbar">
        <button id="pointMode" class="active">Point</button>
        <button id="routeMode">Route</button>
        <button id="zoomOut">-</button>
        <button id="zoomIn">+</button>
        <select id="variable"></select>
        <select id="hour"></select>
        <select id="level"></select>
        <span id="status">click the map</span>
      </div>
      <div id="map">
        <div id="tiles"></div>
        <svg id="overlay"></svg>
      </div>
    </section>
    <section id="dataPanel">
      <div class="panelTitle">
        <strong>Point / Route Diagnostics</strong>
        <span id="metaLine"></span>
      </div>
      <div class="chartBlock"><canvas id="meteogram"></canvas></div>
      <div class="chartBlock"><canvas id="profile"></canvas></div>
      <div class="chartBlock"><canvas id="section"></canvas></div>
    </section>
  </main>
<script>
const state = {
  meta: null,
  center: { lat: 37.3, lon: -119.4 },
  zoom: 6,
  mode: "point",
  route: [],
  pointData: null,
  sectionData: null
};

const map = document.getElementById("map");
const tiles = document.getElementById("tiles");
const overlay = document.getElementById("overlay");
const statusEl = document.getElementById("status");
const variableSel = document.getElementById("variable");
const hourSel = document.getElementById("hour");
const levelSel = document.getElementById("level");

function lonToX(lon, z) { return (lon + 180) / 360 * 256 * Math.pow(2, z); }
function latToY(lat, z) {
  const rad = lat * Math.PI / 180;
  return (1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2 * 256 * Math.pow(2, z);
}
function xToLon(x, z) { return x / (256 * Math.pow(2, z)) * 360 - 180; }
function yToLat(y, z) {
  const n = Math.PI - 2 * Math.PI * y / (256 * Math.pow(2, z));
  return 180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
}
function project(lat, lon) {
  const cx = lonToX(state.center.lon, state.zoom);
  const cy = latToY(state.center.lat, state.zoom);
  return {
    x: lonToX(lon, state.zoom) - cx + map.clientWidth / 2,
    y: latToY(lat, state.zoom) - cy + map.clientHeight / 2
  };
}
function unproject(x, y) {
  const cx = lonToX(state.center.lon, state.zoom);
  const cy = latToY(state.center.lat, state.zoom);
  return {
    lat: yToLat(y - map.clientHeight / 2 + cy, state.zoom),
    lon: xToLon(x - map.clientWidth / 2 + cx, state.zoom)
  };
}
function renderMap() {
  tiles.innerHTML = "";
  const z = state.zoom;
  const cx = lonToX(state.center.lon, z);
  const cy = latToY(state.center.lat, z);
  const left = cx - map.clientWidth / 2;
  const top = cy - map.clientHeight / 2;
  const x0 = Math.floor(left / 256) - 1;
  const x1 = Math.floor((left + map.clientWidth) / 256) + 1;
  const y0 = Math.floor(top / 256) - 1;
  const y1 = Math.floor((top + map.clientHeight) / 256) + 1;
  const max = Math.pow(2, z);
  for (let x = x0; x <= x1; x++) {
    for (let y = y0; y <= y1; y++) {
      if (y < 0 || y >= max) continue;
      const wrapX = ((x % max) + max) % max;
      const img = document.createElement("img");
      img.className = "tile";
      img.src = `https://tile.openstreetmap.org/${z}/${wrapX}/${y}.png`;
      img.style.left = `${x * 256 - left}px`;
      img.style.top = `${y * 256 - top}px`;
      img.alt = "";
      tiles.appendChild(img);
    }
  }
  renderOverlay();
}
function renderOverlay() {
  overlay.setAttribute("width", map.clientWidth);
  overlay.setAttribute("height", map.clientHeight);
  overlay.innerHTML = "";
  if (state.pointData) {
    const p = project(state.pointData.profile.lat_deg, state.pointData.profile.lon_deg);
    overlay.insertAdjacentHTML("beforeend", `<circle cx="${p.x}" cy="${p.y}" r="7" fill="#d92222" stroke="white" stroke-width="2"/>`);
  }
  if (state.route.length) {
    const pts = state.route.map(p => project(p.lat, p.lon));
    for (const p of pts) overlay.insertAdjacentHTML("beforeend", `<circle cx="${p.x}" cy="${p.y}" r="6" fill="#1455d9" stroke="white" stroke-width="2"/>`);
    if (pts.length >= 2) overlay.insertAdjacentHTML("beforeend", `<line x1="${pts[0].x}" y1="${pts[0].y}" x2="${pts[1].x}" y2="${pts[1].y}" stroke="#1455d9" stroke-width="3"/>`);
  }
}
function setMode(mode) {
  state.mode = mode;
  document.getElementById("pointMode").classList.toggle("active", mode === "point");
  document.getElementById("routeMode").classList.toggle("active", mode === "route");
  statusEl.textContent = mode === "point" ? "click for profile" : "click start and end";
}
async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}
async function loadMeta() {
  state.meta = await fetchJson("/api/metadata");
  const b = state.meta.grid.bounds;
  if (b) state.center = { lat: (b[2] + b[3]) / 2, lon: (b[0] + b[1]) / 2 };
  document.getElementById("headerMeta").textContent = `${state.meta.model} ${state.meta.cycle} ${state.meta.domain} ${state.meta.grid.nx}x${state.meta.grid.ny}`;
  document.getElementById("metaLine").textContent = `${state.meta.variables.join(", ")} | ${state.meta.forecast_hours.length} hours | ${state.meta.levels_hpa.length} levels`;
  variableSel.innerHTML = state.meta.variables.map(v => `<option value="${v}">${v}</option>`).join("");
  hourSel.innerHTML = state.meta.forecast_hours.map(h => `<option value="${h}">f${String(h).padStart(3, "0")}</option>`).join("");
  levelSel.innerHTML = state.meta.levels_hpa.map(l => `<option value="${l}">${l} hPa</option>`).join("");
  renderMap();
  clearCharts();
}
async function handleMapClick(evt) {
  const rect = map.getBoundingClientRect();
  const p = unproject(evt.clientX - rect.left, evt.clientY - rect.top);
  if (state.mode === "point") {
    statusEl.textContent = `sampling ${p.lat.toFixed(3)}, ${p.lon.toFixed(3)}...`;
    state.pointData = await fetchJson(`/api/point?lat=${p.lat}&lon=${p.lon}`);
    statusEl.textContent = `point ${state.pointData.elapsed_ms} ms`;
    renderOverlay();
    drawPointCharts();
  } else {
    state.route.push(p);
    if (state.route.length > 2) state.route = [p];
    renderOverlay();
    if (state.route.length === 2) {
      await loadSection();
    } else {
      statusEl.textContent = "click route end";
    }
  }
}
async function loadSection() {
  const [a, b] = state.route;
  const hour = hourSel.value;
  const variable = variableSel.value;
  statusEl.textContent = "sampling cross-section...";
  state.sectionData = await fetchJson(`/api/cross-section?lat1=${a.lat}&lon1=${a.lon}&lat2=${b.lat}&lon2=${b.lon}&hour=${hour}&variable=${variable}&spacing_km=20`);
  statusEl.textContent = `section ${state.sectionData.elapsed_ms} ms`;
  drawSection();
}
function samplesFor(variable, hour, level) {
  if (!state.pointData) return [];
  return state.pointData.profile.samples.filter(s =>
    s.variable === variable &&
    (hour === null || s.forecast_hour === Number(hour)) &&
    (level === null || s.level_hpa === Number(level))
  );
}
function clearCanvas(canvas, title) {
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, canvas.clientWidth * dpr);
  canvas.height = Math.max(1, canvas.clientHeight * dpr);
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, canvas.clientWidth, canvas.clientHeight);
  ctx.fillStyle = "#fbfaf7";
  ctx.fillRect(0, 0, canvas.clientWidth, canvas.clientHeight);
  ctx.fillStyle = "#29323a";
  ctx.font = "13px Segoe UI, Arial";
  ctx.fillText(title, 12, 20);
  return ctx;
}
function drawAxes(ctx, w, h, left, top, right, bottom) {
  ctx.strokeStyle = "#8c938c";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(left, top);
  ctx.lineTo(left, h - bottom);
  ctx.lineTo(w - right, h - bottom);
  ctx.stroke();
}
function drawPointCharts() {
  const variable = variableSel.value;
  const hour = hourSel.value;
  const level = levelSel.value;
  drawMeteogram(variable, level);
  drawProfile(variable, hour);
}
function drawMeteogram(variable, level) {
  const canvas = document.getElementById("meteogram");
  const ctx = clearCanvas(canvas, `Point time series | ${variable} ${level} hPa`);
  const w = canvas.clientWidth, h = canvas.clientHeight;
  const data = samplesFor(variable, null, level).filter(s => Number.isFinite(s.value)).sort((a,b) => a.forecast_hour - b.forecast_hour);
  if (!data.length) return noData(ctx, w, h);
  const left = 44, right = 16, top = 34, bottom = 28;
  drawAxes(ctx, w, h, left, top, right, bottom);
  const minV = Math.min(...data.map(d => d.value));
  const maxV = Math.max(...data.map(d => d.value));
  const minH = Math.min(...data.map(d => d.forecast_hour));
  const maxH = Math.max(...data.map(d => d.forecast_hour));
  const sx = v => left + (maxH === minH ? .5 : (v - minH) / (maxH - minH)) * (w - left - right);
  const sy = v => h - bottom - (maxV === minV ? .5 : (v - minV) / (maxV - minV)) * (h - top - bottom);
  ctx.strokeStyle = "#b33b2e"; ctx.lineWidth = 2; ctx.beginPath();
  data.forEach((d,i) => { const x=sx(d.forecast_hour), y=sy(d.value); i ? ctx.lineTo(x,y) : ctx.moveTo(x,y); });
  ctx.stroke();
  ctx.fillStyle = "#b33b2e";
  data.forEach(d => { ctx.beginPath(); ctx.arc(sx(d.forecast_hour), sy(d.value), 3, 0, Math.PI*2); ctx.fill(); });
  ctx.fillStyle = "#4a535c"; ctx.font = "12px Segoe UI, Arial";
  ctx.fillText(`${minV.toFixed(2)} to ${maxV.toFixed(2)}`, left, h - 8);
}
function drawProfile(variable, hour) {
  const canvas = document.getElementById("profile");
  const ctx = clearCanvas(canvas, `Vertical profile | ${variable} f${String(hour).padStart(3, "0")}`);
  const w = canvas.clientWidth, h = canvas.clientHeight;
  const data = samplesFor(variable, hour, null).filter(s => Number.isFinite(s.value)).sort((a,b) => b.level_hpa - a.level_hpa);
  if (!data.length) return noData(ctx, w, h);
  const left = 48, right = 18, top = 34, bottom = 24;
  drawAxes(ctx, w, h, left, top, right, bottom);
  const minV = Math.min(...data.map(d => d.value));
  const maxV = Math.max(...data.map(d => d.value));
  const minP = Math.min(...data.map(d => d.level_hpa));
  const maxP = Math.max(...data.map(d => d.level_hpa));
  const sx = v => left + (maxV === minV ? .5 : (v - minV) / (maxV - minV)) * (w - left - right);
  const sy = p => top + (maxP - p) / (maxP - minP) * (h - top - bottom);
  ctx.strokeStyle = "#2f5f7b"; ctx.lineWidth = 2; ctx.beginPath();
  data.forEach((d,i) => { const x=sx(d.value), y=sy(d.level_hpa); i ? ctx.lineTo(x,y) : ctx.moveTo(x,y); });
  ctx.stroke();
  ctx.fillStyle = "#4a535c"; ctx.font = "12px Segoe UI, Arial";
  ctx.fillText(`${minV.toFixed(2)} to ${maxV.toFixed(2)}`, left, h - 7);
}
function drawSection() {
  const canvas = document.getElementById("section");
  const variable = variableSel.value;
  const ctx = clearCanvas(canvas, `Cross-section | ${variable}`);
  const w = canvas.clientWidth, h = canvas.clientHeight;
  if (!state.sectionData) return noData(ctx, w, h);
  const section = state.sectionData.section;
  const levels = state.meta.levels_hpa.slice();
  const samples = section.route_samples;
  const values = section.values.filter(v => v.variable === variable && Number.isFinite(v.value));
  if (!samples.length || !values.length) return noData(ctx, w, h);
  const byKey = new Map(values.map(v => [`${v.sample_index}:${v.level_hpa}`, v.value]));
  const minV = Math.min(...values.map(v => v.value));
  const maxV = Math.max(...values.map(v => v.value));
  const left = 44, right = 14, top = 34, bottom = 28;
  const plotW = w - left - right, plotH = h - top - bottom;
  for (let i = 0; i < samples.length; i++) {
    for (let j = 0; j < levels.length; j++) {
      const value = byKey.get(`${i}:${levels[j]}`);
      if (!Number.isFinite(value)) continue;
      ctx.fillStyle = ramp((value - minV) / Math.max(1e-6, maxV - minV));
      const x = left + i / samples.length * plotW;
      const y = top + j / levels.length * plotH;
      ctx.fillRect(x, y, Math.ceil(plotW / samples.length) + 1, Math.ceil(plotH / levels.length) + 1);
    }
  }
  drawAxes(ctx, w, h, left, top, right, bottom);
  ctx.fillStyle = "#29323a"; ctx.font = "12px Segoe UI, Arial";
  ctx.fillText(`${minV.toFixed(2)} to ${maxV.toFixed(2)} | ${state.sectionData.elapsed_ms} ms`, left, h - 7);
}
function ramp(t) {
  t = Math.max(0, Math.min(1, t));
  const r = Math.round(45 + 205 * t);
  const g = Math.round(80 + 115 * (1 - Math.abs(t - .5) * 2));
  const b = Math.round(155 - 120 * t);
  return `rgb(${r},${g},${b})`;
}
function noData(ctx, w, h) {
  ctx.fillStyle = "#7b838a";
  ctx.font = "13px Segoe UI, Arial";
  ctx.fillText("No data for this selection yet.", 14, Math.max(44, h / 2));
}
function clearCharts() {
  clearCanvas(document.getElementById("meteogram"), "Point time series");
  clearCanvas(document.getElementById("profile"), "Vertical profile");
  clearCanvas(document.getElementById("section"), "Cross-section");
}
map.addEventListener("click", evt => handleMapClick(evt).catch(err => statusEl.textContent = err.message));
document.getElementById("pointMode").onclick = () => setMode("point");
document.getElementById("routeMode").onclick = () => setMode("route");
document.getElementById("zoomIn").onclick = () => { state.zoom = Math.min(11, state.zoom + 1); renderMap(); };
document.getElementById("zoomOut").onclick = () => { state.zoom = Math.max(3, state.zoom - 1); renderMap(); };
variableSel.onchange = () => { drawPointCharts(); if (state.route.length === 2) loadSection().catch(err => statusEl.textContent = err.message); };
hourSel.onchange = () => { drawPointCharts(); if (state.route.length === 2) loadSection().catch(err => statusEl.textContent = err.message); };
levelSel.onchange = drawPointCharts;
window.addEventListener("resize", () => { renderMap(); drawPointCharts(); drawSection(); });
loadMeta().catch(err => statusEl.textContent = err.message);
</script>
</body>
</html>
"##;
