const CALIFORNIA_BOUNDS = [[31.0, -125.2], [43.0, -113.5]];

const state = {
  marker: null,
  point: null,
  manifests: {
    latest: { manifest: null, records: [], error: null },
    diurnal: { manifest: null, records: [], error: null },
    lightning: { manifest: null, records: [], error: null },
    satellite: { manifest: null, records: [], error: null },
  },
  activeManifest: "latest",
  productCatalog: null,
  crossSectionCatalog: null,
  pressureVolume: null,
  pressureOutputPinned: false,
  pressureRoutePoints: [],
  pressureRouteLine: null,
  pressureRouteMarkers: [],
  pressureDrawActive: false,
  pressureLoopFrames: [],
  pressureLoopFrameIndex: 0,
  pressureLoopTimer: null,
  pressureLoopPlaying: false,
};

const map = L.map("map", {
  zoomControl: true,
  maxBounds: [[28, -130], [46, -108]],
  maxBoundsViscosity: 0.6,
}).fitBounds(CALIFORNIA_BOUNDS);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 12,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

L.rectangle(CALIFORNIA_BOUNDS, {
  color: "#e2b84b",
  weight: 1,
  fillOpacity: 0.02,
}).addTo(map);

const pointReadout = document.getElementById("point-readout");
const renderButton = document.getElementById("render-point");
const statusEl = document.getElementById("meteogram-status");
const meteogramLink = document.getElementById("meteogram-link");
const meteogramImage = document.getElementById("meteogram-image");
const runtimeList = document.getElementById("runtime-list");
const runLabel = document.getElementById("run-label");
const warmLabel = document.getElementById("warm-label");
const gallerySubtitle = document.getElementById("gallery-subtitle");
const coverageSummary = document.getElementById("coverage-summary");
const productCoverage = document.getElementById("product-coverage");
const galleryGrid = document.getElementById("gallery-grid");
const galleryHour = document.getElementById("gallery-hour");
const galleryProduct = document.getElementById("gallery-product");
const galleryFormat = document.getElementById("gallery-format");
const gallerySize = document.getElementById("gallery-size");
const galleryLoop = document.getElementById("gallery-loop");
const gallerySearch = document.getElementById("gallery-search");
const manifestLink = document.getElementById("manifest-link");
const pressureProfileButton = document.getElementById("pressure-profile");
const pressureOutput = document.getElementById("pressure-output");
const pressureHour = document.getElementById("pressure-hour");
const pressureVariable = document.getElementById("pressure-variable");
const pressureProduct = document.getElementById("pressure-product");
const pressureSpacing = document.getElementById("pressure-spacing");
const pressureTop = document.getElementById("pressure-top");
const pressureDrawRoute = document.getElementById("pressure-draw-route");
const pressureClearRoute = document.getElementById("pressure-clear-route");
const pressureRender = document.getElementById("pressure-render");
const pressureLoop = document.getElementById("pressure-loop");
const pressureRenderLink = document.getElementById("pressure-render-link");
const pressureRenderImage = document.getElementById("pressure-render-image");
const pressureLoopControls = document.getElementById("pressure-loop-controls");
const pressureLoopPlay = document.getElementById("pressure-loop-play");
const pressureFrame = document.getElementById("pressure-frame");
const pressureFrameLabel = document.getElementById("pressure-frame-label");
const FORMAT_KEY = "cafire_gallery_preview_format";
const SIZE_KEY = "cafire_gallery_satellite_size";
const LOOP_KEY = "cafire_gallery_satellite_loop";

const PRESSURE_PRODUCTS = [
  { product: "temperature", label: "Temperature" },
  { product: "wind_speed", label: "Wind Speed" },
  { product: "theta_e", label: "Theta-e" },
  { product: "rh", label: "Relative Humidity" },
  { product: "q", label: "Specific Humidity" },
  { product: "omega", label: "Vertical Motion" },
  { product: "vorticity", label: "Absolute Vorticity" },
  { product: "shear", label: "Deep-Layer Shear" },
  { product: "lapse_rate", label: "Lapse Rate" },
  { product: "cloud", label: "Cloud Water/Ice" },
  { product: "cloud_total", label: "Total Hydrometeors" },
  { product: "wetbulb", label: "Wet Bulb" },
  { product: "icing", label: "Icing" },
  { product: "frontogenesis", label: "Frontogenesis" },
  { product: "vpd", label: "Vapor Pressure Deficit" },
  { product: "dewpoint_dep", label: "Dewpoint Depression" },
  { product: "moisture_transport", label: "Moisture Transport" },
  { product: "pv", label: "Potential Vorticity" },
  { product: "fire_wx", label: "Fire Weather" },
];

const PRESSURE_ROUTES = {
  "bay-sierra": {
    name: "Bay to Sierra",
    lat1: 37.7749,
    lon1: -122.4194,
    lat2: 38.5788,
    lon2: -119.7513,
  },
  "la-sierra": {
    name: "LA Basin to Southern Sierra",
    lat1: 34.0522,
    lon1: -118.2437,
    lat2: 36.5786,
    lon2: -118.2923,
  },
  "north-south": {
    name: "Northern CA to Southern CA",
    lat1: 40.5865,
    lon1: -122.3917,
    lat2: 34.4208,
    lon2: -119.6982,
  },
};

galleryFormat.value = localStorage.getItem(FORMAT_KEY) || "webp";
gallerySize.value = localStorage.getItem(SIZE_KEY) || "native";
galleryLoop.value = localStorage.getItem(LOOP_KEY) || "still";

function setStatus(message) {
  statusEl.textContent = message;
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      detail = body.detail || detail;
    } catch (_) {}
    throw new Error(detail);
  }
  return response.json();
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function satelliteCatalogItem(product, manifest = null) {
  const catalog = manifest?.product_catalog || [];
  return catalog.find((item) => item.product === product) || null;
}

function labelForProduct(product, manifest = null) {
  const satelliteItem = satelliteCatalogItem(product, manifest);
  if (satelliteItem?.name) return satelliteItem.name;
  if (product === "glm_lightning_flashes" || product === "glm_flashes") {
    return "GLM lightning flashes";
  }
  if (product === "goes_geocolor") return "GeoColor";
  if (product === "goes_glm_fed_geocolor") return "GLM FED3+GeoColor";
  if (product === "goes_airmass_rgb") return "AirMass RGB";
  if (product === "goes_sandwich_rgb") return "Sandwich RGB";
  if (product === "goes_day_night_cloud_micro_combo_rgb") return "Day Night Cloud Micro Combo RGB";
  if (product === "goes_fire_temperature_rgb") return "Fire Temperature";
  if (product === "goes_dust_rgb") return "Dust RGB";
  const bandMatch = product.match(/^goes_abi_band_(\d{2})$/);
  if (bandMatch) return `Band ${Number(bandMatch[1])}`;
  return product
    .replace(/^2m_/, "2 m ")
    .replace(/^10m_/, "10 m ")
    .replace(/_/g, " ")
    .replace(/\brh\b/g, "RH")
    .replace(/\bqpf\b/g, "QPF")
    .replace(/\bvpd\b/g, "VPD")
    .replace(/\bpm25\b/g, "PM2.5")
    .replace(/\buh\b/g, "UH");
}

function labelForPressureProduct(product) {
  const catalog = state.crossSectionCatalog?.products || PRESSURE_PRODUCTS;
  return catalog.find((item) => item.product === product)?.label || product.replace(/_/g, " ");
}

function descriptionForProduct(product, manifest = null) {
  const item = satelliteCatalogItem(product, manifest);
  if (!item) return "";
  return [item.wavelength, item.description].filter(Boolean).join(" - ");
}

function artifactUrl(url, manifest) {
  const version = encodeURIComponent(manifest.generated_at_utc || "");
  return version ? `${url}?v=${version}` : url;
}

function formatBytes(bytes) {
  if (!bytes) return "";
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(0)} KB`;
  return `${(kb / 1024).toFixed(1)} MB`;
}

function manifestUrl(manifest) {
  if (manifest.manifest_url) {
    return manifest.manifest_url;
  }
  if (manifest.public_base_url && manifest.artifact_prefix) {
    return `${manifest.public_base_url.replace(/\/$/, "")}/${manifest.artifact_prefix}/manifest.json`;
  }
  return "#";
}

function productSlugFromKey(key, manifest) {
  const leaf = (key.split("/").pop() || key).replace(/\.(png|webp)$/i, "");
  const domain = manifest.domain || "california";
  const domainIndex = leaf.indexOf(`_${domain}_`);
  if (domainIndex !== -1) {
    return leaf.slice(domainIndex + domain.length + 2);
  }
  const fallback = leaf.match(/_f\d{3}_.+?_(.+)$/);
  return fallback ? fallback[1] : leaf;
}

function preferredStill(stills, format, width) {
  const candidates = stills.filter((still) => still.url && still.format === format);
  if (!candidates.length) return null;
  if (width !== "native") {
    const selectedWidth = Number(width);
    const exact = candidates.find((still) => Number(still.width) === selectedWidth);
    if (exact) return exact;
  }
  return candidates.find((still) => still.native) || candidates[candidates.length - 1];
}

function collectSatelliteRecords(manifest) {
  const loopsByProduct = new Map();
  for (const loop of manifest.loops || []) {
    if (loop.ok === false || !loop.url) continue;
    const product = loop.product || "";
    const list = loopsByProduct.get(product) || [];
    list.push(loop);
    loopsByProduct.set(product, list);
  }

  const records = [];
  for (const artifact of manifest.artifacts || []) {
    const product = artifact.product || "";
    const stills = Array.isArray(artifact.stills) ? artifact.stills.filter((still) => still.url) : [];
    if (artifact.webp_url && !stills.some((still) => still.url === artifact.webp_url)) {
      stills.push({
        format: "webp",
        url: artifact.webp_url,
        key: artifact.webp_key,
        size_bytes: artifact.webp_size_bytes,
        width: artifact.width || manifest.width,
        height: artifact.height || manifest.height,
        native: true,
      });
    }
    if (artifact.png_url && !stills.some((still) => still.url === artifact.png_url)) {
      stills.push({
        format: "png",
        url: artifact.png_url,
        key: artifact.png_key,
        size_bytes: artifact.png_size_bytes,
        width: artifact.width || manifest.width,
        height: artifact.height || manifest.height,
        native: true,
      });
    }
    const png = preferredStill(stills, "png", "native");
    const webp = preferredStill(stills, "webp", "native");
    const loopVariants = loopsByProduct.get(product) || artifact.loops || [];
    const preferredLoop =
      loopVariants.find((loop) => loop.format === "webp" && Number(loop.duration_min) >= 360) ||
      loopVariants.find((loop) => loop.format === "webp") ||
      loopVariants[0] ||
      null;
    records.push({
      key: artifact.png_key || artifact.webp_key || product,
      hour: 0,
      product,
      label: labelForProduct(product, manifest),
      description: descriptionForProduct(product, manifest),
      stills,
      loopVariants,
      loop: preferredLoop,
      pngUrl: png?.url || null,
      webpUrl: webp?.url || null,
      pngBytes: png?.size_bytes || null,
      webpBytes: webp?.size_bytes || null,
      url: webp?.url || png?.url || stills[0]?.url || null,
    });
  }
  return records.filter((record) => record.url);
}

function collectPlotRecords(manifest) {
  if (manifest.kind === "goes_satellite" && Array.isArray(manifest.artifacts)) {
    return collectSatelliteRecords(manifest);
  }
  const byPlot = new Map();
  const loopsByProduct = new Map((manifest.loops || []).map((loop) => [loop.product, loop]));
  for (const hour of manifest.hours || []) {
    for (const uploaded of hour.uploaded || []) {
      const key = uploaded.key || uploaded.path || "";
      const url = uploaded.url || "";
      const lowerKey = key.toLowerCase();
      const ext = lowerKey.endsWith(".webp") ? "webp" : lowerKey.endsWith(".png") ? "png" : null;
      if (!ext || !url) continue;
      const product =
        manifest.kind === "glm_lightning"
          ? "glm_lightning_flashes"
          : productSlugFromKey(key, manifest);
      const hourNumber = Number(hour.forecast_hour || 0);
      const plotKey = `${hourNumber}:${product}`;
      const record = byPlot.get(plotKey) || {
        key: key.replace(/\.(png|webp)$/i, ""),
        hour: hourNumber,
        product,
        label: labelForProduct(product, manifest),
        description: descriptionForProduct(product, manifest),
        loop: loopsByProduct.get(product) || null,
        pngUrl: null,
        webpUrl: null,
        pngBytes: null,
        webpBytes: null,
      };
      if (ext === "png") {
        record.pngUrl = url;
        record.pngBytes = uploaded.size_bytes || null;
      } else {
        record.webpUrl = url;
        record.webpBytes = uploaded.size_bytes || null;
      }
      record.url = record.webpUrl || record.pngUrl;
      byPlot.set(plotKey, record);
    }
  }
  return [...byPlot.values()]
    .filter((record) => record.url)
    .sort((a, b) => a.hour - b.hour || a.product.localeCompare(b.product));
}

function uniqueSorted(values) {
  return [...new Set(values)].sort((a, b) => {
    if (typeof a === "number" && typeof b === "number") return a - b;
    return String(a).localeCompare(String(b));
  });
}

function setOptions(select, options, selected) {
  select.innerHTML = "";
  for (const option of options) {
    const el = document.createElement("option");
    el.value = String(option.value);
    el.textContent = option.label;
    select.appendChild(el);
  }
  select.value = options.some((option) => String(option.value) === String(selected))
    ? String(selected)
    : String(options[0]?.value || "");
}

function refreshSatelliteVariantControls(manifest, records, resetSelection = false) {
  const sizeValues = uniqueSorted(
    records.flatMap((record) => (record.stills || []).map((still) => Number(still.width)).filter(Boolean)),
  );
  const previousSize = resetSelection ? null : gallerySize.value;
  setOptions(
    gallerySize,
    [
      { value: "native", label: "Native" },
      ...sizeValues.map((width) => ({ value: width, label: `${width}px` })),
    ],
    previousSize || localStorage.getItem(SIZE_KEY) || "native",
  );

  const loopDurations = uniqueSorted(
    records.flatMap((record) => (record.loopVariants || []).map((loop) => Number(loop.duration_min)).filter(Boolean)),
  );
  const previousLoop = resetSelection ? null : galleryLoop.value;
  setOptions(
    galleryLoop,
    [
      { value: "still", label: "Still" },
      ...loopDurations.map((duration) => ({
        value: duration,
        label: duration % 60 === 0 ? `${duration / 60}h loop` : `${duration}m loop`,
      })),
    ],
    previousLoop || localStorage.getItem(LOOP_KEY) || "still",
  );
  gallerySize.disabled = false;
  galleryLoop.disabled = false;
}

function resetVariantControls() {
  setOptions(gallerySize, [{ value: "native", label: "Native" }], "native");
  setOptions(galleryLoop, [{ value: "still", label: "Still" }], "still");
  gallerySize.disabled = true;
  galleryLoop.disabled = true;
}

function activeData() {
  return state.manifests[state.activeManifest];
}

function activeIsLightning() {
  return state.activeManifest === "lightning";
}

function activeIsSatellite() {
  return state.activeManifest === "satellite";
}

function expectedProductsForHour(manifest, hour) {
  const products = manifest.products || [];
  if (hour === null || Number.isNaN(hour)) return products;
  if (state.activeManifest === "diurnal") {
    if (hour === 24) return products.filter((product) => product === "2m_temp_0_24h_range");
    if (hour === 48) return products;
  }
  if (activeIsLightning()) {
    return products;
  }
  if (hour === 0) {
    return products.filter((product) => product !== "qpf_1h" && product !== "10m_wind_1h_max");
  }
  return products;
}

function renderRunLabel(manifest) {
  if (!manifest) {
    runLabel.textContent = "Latest HRRR run unavailable";
    return;
  }
  const cycle = String(manifest.cycle_utc).padStart(2, "0");
  const hours = manifest.available_forecast_hours || manifest.forecast_hours || [];
  const hourText = hours.length
    ? `f${String(hours[0]).padStart(3, "0")}-f${String(hours[hours.length - 1]).padStart(3, "0")}`
    : "no hours";
  runLabel.textContent = `HRRR ${manifest.date_yyyymmdd} ${cycle}Z - ${manifest.domain} - ${hourText}`;
}

function refreshGalleryControls(resetSelection = false) {
  const { manifest, records } = activeData();
  if (!manifest) {
    galleryHour.innerHTML = "";
    galleryProduct.innerHTML = "";
    resetVariantControls();
    return;
  }

  const hours = uniqueSorted(records.map((record) => record.hour));
  const previousHour = resetSelection ? null : galleryHour.value;
  const defaultHour = activeIsLightning() ? "all" : hours.length ? String(hours[hours.length - 1]) : "all";
  setOptions(
    galleryHour,
    [
      { value: "all", label: "All hours" },
      ...hours.map((hour) => ({
        value: hour,
        label: `f${String(hour).padStart(3, "0")}`,
      })),
    ],
    previousHour || defaultHour,
  );

  const requested = manifest.products || [];
  const rendered = uniqueSorted(records.map((record) => record.product));
  const products = uniqueSorted([...requested, ...rendered]);
  const previousProduct = resetSelection ? null : galleryProduct.value;
  setOptions(
    galleryProduct,
    [
      { value: "all", label: "All products" },
      ...products.map((product) => ({
        value: product,
        label: labelForProduct(product, manifest),
      })),
    ],
    previousProduct || "all",
  );
  if (activeIsSatellite()) {
    refreshSatelliteVariantControls(manifest, records, resetSelection);
  } else {
    resetVariantControls();
  }
}

function renderCoverage() {
  const { manifest, records, error } = activeData();
  if (error) {
    gallerySubtitle.textContent = `${state.activeManifest} manifest unavailable`;
    coverageSummary.innerHTML = `<div class="muted">${escapeHtml(error.message)}</div>`;
    productCoverage.innerHTML = "";
    galleryGrid.innerHTML = "";
    manifestLink.href = "#";
    return;
  }
  if (!manifest) {
    gallerySubtitle.textContent = "Loading manifests...";
    coverageSummary.innerHTML = "<div class=\"muted\">Waiting for manifest...</div>";
    productCoverage.innerHTML = "";
    galleryGrid.innerHTML = "";
    manifestLink.href = "#";
    return;
  }

  const selectedHour = galleryHour.value === "all" ? null : Number(galleryHour.value);
  const scopedRecords =
    selectedHour === null ? records : records.filter((record) => record.hour === selectedHour);
  const requested = expectedProductsForHour(manifest, selectedHour);
  const renderedProducts = new Set(scopedRecords.map((record) => record.product));
  const missing = requested.filter((product) => !renderedProducts.has(product));
  const hours = uniqueSorted(records.map((record) => record.hour));
  const webpCount = records.filter((record) => record.webpUrl).length;
  const pngCount = records.filter((record) => record.pngUrl).length;
  const cycle = String(manifest.cycle_utc).padStart(2, "0");
  const sourceLabel =
    state.activeManifest === "diurnal"
      ? "extended-cycle diurnal ranges"
      : activeIsLightning()
        ? "latest GOES-West GLM lightning"
        : activeIsSatellite()
          ? "latest GOES-West satellite"
          : "latest hourly run";
  const productCatalog = state.productCatalog;
  const catalogText = productCatalog
    ? `${productCatalog.direct?.length || 0} direct / ${productCatalog.light_derived?.length || 0} derived / ${productCatalog.windowed?.length || 0} windowed`
    : "catalog loading";

  const windowLast = manifest.time_window?.last || manifest.generated_at_utc || "";
  gallerySubtitle.textContent =
    activeIsLightning()
      ? `${sourceLabel}: ${manifest.satellite || manifest.source || "GLM"}, ${manifest.domain_label || manifest.domain}, latest ${windowLast}`
      : activeIsSatellite()
        ? `${sourceLabel}: ${manifest.satellite || "GOES"}, ${manifest.domain_label || manifest.domain}, scan ${manifest.scan_time_utc || manifest.generated_at_utc || "latest"}`
      : `${sourceLabel}: HRRR ${manifest.date_yyyymmdd} ${cycle}Z, ${manifest.domain}`;
  manifestLink.href = manifestUrl(manifest);
  const scopeLabel = selectedHour === null ? "run products present" : `f${String(selectedHour).padStart(3, "0")} products present`;
  const missingLabel = selectedHour === null ? "missing in this manifest" : "missing at selected hour";
  if (activeIsLightning()) {
    coverageSummary.innerHTML = `
      <div class="metric"><strong>${records.length}</strong><span>lightning plot</span></div>
      <div class="metric"><strong>${manifest.flash_count_in_domain ?? "--"}</strong><span>CA flashes</span></div>
      <div class="metric"><strong>${manifest.flash_count_total ?? "--"}</strong><span>GLM flashes read</span></div>
      <div class="metric"><strong>${manifest.n_files ?? "--"}</strong><span>GLM files</span></div>
      <div class="metric"><strong>${webpCount}/${pngCount}</strong><span>WebP / PNG pairs</span></div>
      <div class="metric wide"><strong>${escapeHtml(windowLast || "latest")}</strong><span>latest GLM time</span></div>
    `;
  } else if (activeIsSatellite()) {
    const loopCount = (manifest.loops || []).filter((loop) => loop.ok !== false && loop.url).length;
    const stillVariantCount = records.reduce((total, record) => total + (record.stills || []).length, 0);
    coverageSummary.innerHTML = `
      <div class="metric"><strong>${records.length}</strong><span>satellite plots</span></div>
      <div class="metric"><strong>${renderedProducts.size}/${requested.length || renderedProducts.size}</strong><span>products present</span></div>
      <div class="metric"><strong>${stillVariantCount}</strong><span>still variants</span></div>
      <div class="metric"><strong>${loopCount}</strong><span>loop variants</span></div>
      <div class="metric"><strong>${(manifest.source_keys || []).length}</strong><span>ABI source files</span></div>
      <div class="metric"><strong>${(manifest.glm_source_keys || []).length}</strong><span>GLM source files</span></div>
      <div class="metric wide"><strong>${escapeHtml(manifest.scan_time_utc || "latest")}</strong><span>scan time</span></div>
    `;
  } else {
    coverageSummary.innerHTML = `
      <div class="metric"><strong>${records.length}</strong><span>plots</span></div>
      <div class="metric"><strong>${hours.length}</strong><span>hours rendered</span></div>
      <div class="metric"><strong>${renderedProducts.size}/${requested.length || renderedProducts.size}</strong><span>${escapeHtml(scopeLabel)}</span></div>
      <div class="metric"><strong>${webpCount}/${pngCount}</strong><span>WebP / PNG pairs</span></div>
      <div class="metric wide"><strong>${escapeHtml(catalogText)}</strong><span>rustwx product catalog</span></div>
      <div class="metric"><strong>${missing.length}</strong><span>${escapeHtml(missingLabel)}</span></div>
    `;
  }

  const products = uniqueSorted([...requested, ...renderedProducts]);
  productCoverage.innerHTML = "";
  for (const product of products) {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = `coverage-chip ${renderedProducts.has(product) ? "ok" : "missing"}`;
    chip.textContent = labelForProduct(product, manifest);
    const description = descriptionForProduct(product, manifest);
    chip.title = `${renderedProducts.has(product) ? "Rendered in this manifest" : "Configured but not rendered in this manifest"}${description ? `: ${description}` : ""}`;
    chip.addEventListener("click", () => {
      galleryProduct.value = product;
      renderGallery();
    });
    productCoverage.appendChild(chip);
  }
}

function selectedSatelliteStill(record) {
  const requestedFormat = galleryFormat.value === "png" ? "png" : "webp";
  const requestedSize = gallerySize.value || "native";
  return (
    preferredStill(record.stills || [], requestedFormat, requestedSize) ||
    preferredStill(record.stills || [], requestedFormat === "png" ? "webp" : "png", requestedSize) ||
    preferredStill(record.stills || [], requestedFormat, "native") ||
    preferredStill(record.stills || [], "png", "native")
  );
}

function selectedSatelliteLoop(record) {
  if (galleryLoop.value === "still") return null;
  const duration = Number(galleryLoop.value);
  const requestedFormat = galleryFormat.value === "png" ? "gif" : "webp";
  const requestedSize = gallerySize.value || "native";
  const loops = record.loopVariants || [];
  const matchingDuration = loops.filter((loop) => Number(loop.duration_min) === duration);
  const matchingFormat = matchingDuration.filter((loop) => loop.format === requestedFormat);
  const width = requestedSize === "native" ? null : Number(requestedSize);
  if (width) {
    const exact = matchingFormat.find((loop) => Number(loop.width || loop.requested_width) === width);
    if (exact) return exact;
  }
  return matchingFormat[0] || matchingDuration.find((loop) => loop.format === "webp") || matchingDuration[0] || null;
}

function renderGallery() {
  renderCoverage();
  const { manifest, records, error } = activeData();
  if (error || !manifest) return;

  const selectedHour = galleryHour.value;
  const selectedProduct = galleryProduct.value;
  const query = gallerySearch.value.trim().toLowerCase();
  const filtered = records.filter((record) => {
    if (selectedHour !== "all" && record.hour !== Number(selectedHour)) return false;
    if (selectedProduct !== "all" && record.product !== selectedProduct) return false;
    if (query && !`${record.product} ${record.label} ${record.description || ""}`.toLowerCase().includes(query)) return false;
    return true;
  });

  galleryGrid.innerHTML = "";
  if (!filtered.length) {
    galleryGrid.innerHTML = "<div class=\"muted\">No plots match this filter.</div>";
    return;
  }

  const fragment = document.createDocumentFragment();
  const preferWebp = galleryFormat.value !== "png";
  for (const record of filtered) {
    const selectedStill = activeIsSatellite() ? selectedSatelliteStill(record) : null;
    const selectedLoop = activeIsSatellite() ? selectedSatelliteLoop(record) : null;
    const selectedUrl = selectedLoop?.url || selectedStill?.url || (preferWebp
      ? record.webpUrl || record.pngUrl || record.url
      : record.pngUrl || record.webpUrl || record.url);
    const thumbUrl = artifactUrl(selectedUrl, manifest);
    const pngStill = activeIsSatellite()
      ? preferredStill(record.stills || [], "png", gallerySize.value || "native")
      : null;
    const webpStill = activeIsSatellite()
      ? preferredStill(record.stills || [], "webp", gallerySize.value || "native")
      : null;
    const pngUrl = pngStill?.url ? artifactUrl(pngStill.url, manifest) : record.pngUrl ? artifactUrl(record.pngUrl, manifest) : thumbUrl;
    const webpUrl = webpStill?.url ? artifactUrl(webpStill.url, manifest) : record.webpUrl ? artifactUrl(record.webpUrl, manifest) : null;
    const card = document.createElement("article");
    card.className = activeIsLightning() ? "gallery-card lightning-card" : "gallery-card";

    const link = document.createElement("a");
    link.href = thumbUrl;
    link.target = "_blank";
    link.rel = "noreferrer";
    link.className = "thumb-link";

    const img = document.createElement("img");
    img.src = thumbUrl;
    img.alt = activeIsLightning()
      ? `${record.label} latest GLM lightning`
      : activeIsSatellite()
        ? `${record.label} latest GOES satellite`
      : `${record.label} f${String(record.hour).padStart(3, "0")}`;
    img.loading = "lazy";
    link.appendChild(img);

    const meta = document.createElement("div");
    meta.className = "gallery-card-meta";
    const pngSize = formatBytes(pngStill?.size_bytes || record.pngBytes);
    const webpSize = formatBytes(webpStill?.size_bytes || record.webpBytes);
    const webpLink = webpUrl
      ? `<a href="${escapeHtml(webpUrl)}" target="_blank" rel="noreferrer">WebP${webpSize ? ` ${escapeHtml(webpSize)}` : ""}</a>`
      : "";
    const loopUrl = selectedLoop?.url ? artifactUrl(selectedLoop.url, manifest) : record.loop?.url ? artifactUrl(record.loop.url, manifest) : null;
    const loopLink = loopUrl
      ? `<a href="${escapeHtml(loopUrl)}" target="_blank" rel="noreferrer">Loop${(selectedLoop || record.loop).size_bytes ? ` ${escapeHtml(formatBytes((selectedLoop || record.loop).size_bytes))}` : ""}</a>`
      : "";
    const leftLabel = activeIsLightning() ? "GLM" : activeIsSatellite() ? "GOES" : `f${String(record.hour).padStart(3, "0")}`;
    const extra = activeIsLightning()
      ? `<em>${manifest.flash_count_in_domain ?? "--"} CA flashes / ${manifest.flash_count_total ?? "--"} full-disk flashes</em>`
      : activeIsSatellite()
        ? `<em>${escapeHtml(record.description || manifest.scan_time_utc || "latest scan")}</em>`
      : "";
    meta.innerHTML = `
      <span>${escapeHtml(leftLabel)}</span>
      <strong>${escapeHtml(record.label)}</strong>
      ${extra}
      <div class="format-links">
        ${loopLink}
        ${webpLink}
        <a href="${escapeHtml(pngUrl)}" target="_blank" rel="noreferrer">PNG${pngSize ? ` ${escapeHtml(pngSize)}` : ""}</a>
      </div>
    `;

    card.appendChild(link);
    card.appendChild(meta);
    fragment.appendChild(card);
  }
  galleryGrid.appendChild(fragment);
}

async function loadManifest(kind, url) {
  try {
    const manifest = await fetchJson(url);
    state.manifests[kind] = {
      manifest,
      records: collectPlotRecords(manifest),
      error: null,
    };
  } catch (error) {
    state.manifests[kind] = { manifest: null, records: [], error };
  }
}

async function loadProductCatalog() {
  try {
    state.productCatalog = await fetchJson("/api/v1/public/products");
  } catch (_) {
    state.productCatalog = null;
  }
}

async function loadCrossSectionCatalog() {
  try {
    state.crossSectionCatalog = await fetchJson("/api/v1/public/cross-section-products");
  } catch (_) {
    state.crossSectionCatalog = { products: PRESSURE_PRODUCTS };
  }
  const current = pressureProduct.value;
  const products = state.crossSectionCatalog.products?.length ? state.crossSectionCatalog.products : PRESSURE_PRODUCTS;
  pressureProduct.innerHTML = products
    .map((item) => `<option value="${escapeHtml(item.product)}">${escapeHtml(item.label)}</option>`)
    .join("");
  pressureProduct.value = products.some((item) => item.product === current) ? current : "wind_speed";
}

async function loadGallery() {
  coverageSummary.innerHTML = "<div class=\"muted\">Loading manifests...</div>";
  await Promise.all([
    loadManifest("latest", "/api/v1/public/latest-artifacts"),
    loadManifest("diurnal", "/api/v1/public/latest-diurnal-artifacts"),
    loadManifest("lightning", "/api/v1/public/latest-lightning-artifacts"),
    loadManifest("satellite", "/api/v1/public/latest-satellite-artifacts"),
    loadProductCatalog(),
    loadCrossSectionCatalog(),
  ]);
  if (state.activeManifest === "latest" && state.manifests.latest.error && state.manifests.satellite.manifest) {
    state.activeManifest = "satellite";
    document.querySelectorAll(".tab-button").forEach((button) => {
      button.classList.toggle("active", button.dataset.manifest === "satellite");
    });
  }
  renderRunLabel(state.manifests.latest.manifest);
  refreshGalleryControls(true);
  renderGallery();
}

async function loadWarmStatus() {
  try {
    const status = await fetchJson("/api/v1/public/warm-status");
    const fast = status.fast_store || {};
    const pressure = status.pressure_volume || {};
    const crossSections = status.pressure_cross_sections || {};
    state.pressureVolume = pressure;
    renderPressureVolumeState(pressure);
    const run = fast.run || fast.target_run || status.run || status.target_run;
    const runText = run ? `${run.cycle.date_yyyymmdd} ${String(run.cycle.hour_utc).padStart(2, "0")}Z` : "no run";
    warmLabel.textContent = `Meteogram fast store: ${fast.status || status.status} - ${runText}`;
    runtimeList.innerHTML = `
      <div><span>Fast store</span><strong>${escapeHtml(fast.status || "unknown")}</strong></div>
      <div><span>Meteogram run</span><strong>${escapeHtml(runText)}</strong></div>
      <div><span>Pressure volume</span><strong>${escapeHtml(pressure.status || "disabled")}</strong></div>
      <div><span>Cross-section renderer</span><strong>${escapeHtml(crossSections.status || "disabled")}</strong></div>
      <div><span>Store bounds</span><strong>California</strong></div>
      <div><span>Static maps</span><strong>${state.manifests.latest.records.length || "--"} plots in latest manifest</strong></div>
      <div><span>Lightning</span><strong>${state.manifests.lightning.records.length ? "available" : "pending"}</strong></div>
      <div><span>Satellite</span><strong>${state.manifests.satellite.records.length ? "available" : "pending"}</strong></div>
    `;
  } catch (error) {
    warmLabel.textContent = `Meteogram fast store: ${error.message}`;
    runtimeList.innerHTML = `<div class="muted">${escapeHtml(error.message)}</div>`;
  }
}

function renderPressureVolumeState(pressure) {
  const metadata = pressure.metadata || {};
  if (metadata.forecast_hours && metadata.forecast_hours.length) {
    const currentHour = pressureHour.value;
    pressureHour.innerHTML = metadata.forecast_hours
      .map((hour) => `<option value="${hour}">f${String(hour).padStart(3, "0")}</option>`)
      .join("");
    if (metadata.forecast_hours.map(String).includes(currentHour)) {
      pressureHour.value = currentHour;
    }
  }
  if (metadata.variables && metadata.variables.length) {
    const currentVariable = pressureVariable.value;
    pressureVariable.innerHTML = metadata.variables
      .map((variable) => `<option value="${escapeHtml(variable)}">${escapeHtml(variable)}</option>`)
      .join("");
    if (metadata.variables.includes(currentVariable)) {
      pressureVariable.value = currentVariable;
    }
  }
  if (pressure.status === "ready" && metadata.grid) {
    if (!state.pressureOutputPinned) {
      pressureOutput.textContent =
        `ready | ${metadata.model} ${metadata.cycle}\n` +
        `${metadata.domain} ${metadata.grid.nx}x${metadata.grid.ny} | ` +
        `${(metadata.forecast_hours || []).length} hours | ${(metadata.levels_hpa || []).length} levels`;
    }
  } else {
    if (!state.pressureOutputPinned) {
      pressureOutput.textContent = `${pressure.status || "disabled"}${pressure.detail ? ` | ${pressure.detail}` : ""}`;
    }
  }
}

function setPressureOutput(message) {
  state.pressureOutputPinned = true;
  pressureOutput.textContent = message;
}

function summarizeProfile(report) {
  const samples = (report.profile && report.profile.samples) || [];
  const variables = new Set(samples.map((sample) => sample.variable));
  const hours = new Set(samples.map((sample) => sample.forecast_hour));
  const levels = new Set(samples.map((sample) => sample.level_hpa));
  return [
    `profile ${report.elapsed_ms || "--"} ms sidecar | ${report.proxy_total_ms || "--"} ms API`,
    `${samples.length} values | ${variables.size} vars | ${hours.size} hours | ${levels.size} levels`,
    `${report.profile.lat_deg.toFixed(4)}, ${report.profile.lon_deg.toFixed(4)}`,
  ].join("\n");
}

function summarizeCrossSection(report, routeName) {
  const section = report.section || {};
  const samples = section.route_samples || [];
  const values = section.values || [];
  return [
    `${routeName} | ${pressureVariable.value} f${String(pressureHour.value).padStart(3, "0")}`,
    `section ${report.elapsed_ms || "--"} ms sidecar | ${report.proxy_total_ms || "--"} ms API`,
    `${samples.length} route samples | ${values.length} pressure values`,
  ].join("\n");
}

function stopPressureLoop() {
  if (state.pressureLoopTimer) {
    clearInterval(state.pressureLoopTimer);
  }
  state.pressureLoopTimer = null;
  state.pressureLoopPlaying = false;
  pressureLoopPlay.textContent = "Play";
}

function resetPressureRenderPreview() {
  stopPressureLoop();
  state.pressureLoopFrames = [];
  state.pressureLoopFrameIndex = 0;
  pressureRenderLink.classList.add("hidden");
  pressureRenderImage.removeAttribute("src");
  pressureLoopControls.classList.add("hidden");
}

function updatePressureRouteButtons() {
  const ready = state.pressureRoutePoints.length === 2;
  pressureRender.disabled = !ready;
  pressureLoop.disabled = !ready;
  pressureClearRoute.disabled = !state.pressureRoutePoints.length;
  pressureDrawRoute.classList.toggle("active", state.pressureDrawActive);
}

function clearPressureRoute() {
  state.pressureRoutePoints = [];
  if (state.pressureRouteLine) {
    map.removeLayer(state.pressureRouteLine);
  }
  for (const marker of state.pressureRouteMarkers) {
    map.removeLayer(marker);
  }
  state.pressureRouteLine = null;
  state.pressureRouteMarkers = [];
  state.pressureDrawActive = false;
  pointReadout.textContent = state.point
    ? `${state.point.lat.toFixed(3)}, ${state.point.lng.toFixed(3)}`
    : "Click a California point";
  updatePressureRouteButtons();
  resetPressureRenderPreview();
}

function drawPressureRoute() {
  if (state.pressureRouteLine) {
    map.removeLayer(state.pressureRouteLine);
  }
  for (const marker of state.pressureRouteMarkers) {
    map.removeLayer(marker);
  }
  state.pressureRouteMarkers = state.pressureRoutePoints.map((point) =>
    L.circleMarker([point.lat, point.lon], {
      radius: 5,
      color: "#f4b63f",
      weight: 2,
      fillColor: "#e4572e",
      fillOpacity: 0.95,
    }).addTo(map),
  );
  if (state.pressureRoutePoints.length === 2) {
    state.pressureRouteLine = L.polyline(
      state.pressureRoutePoints.map((point) => [point.lat, point.lon]),
      { color: "#f4b63f", weight: 3, opacity: 0.95 },
    ).addTo(map);
  } else {
    state.pressureRouteLine = null;
  }
  updatePressureRouteButtons();
}

function setPressureRoute(points, routeName) {
  state.pressureRoutePoints = points.map((point) => ({ lat: point.lat, lon: point.lon }));
  state.pressureDrawActive = false;
  drawPressureRoute();
  if (routeName) {
    setPressureOutput(`${routeName}\nready to render ${labelForPressureProduct(pressureProduct.value)} f${String(pressureHour.value).padStart(3, "0")}`);
  }
}

function addPressureRoutePoint(latlng) {
  if (!state.pressureDrawActive) return false;
  if (state.pressureRoutePoints.length >= 2) {
    state.pressureRoutePoints = [];
  }
  state.pressureRoutePoints.push({ lat: latlng.lat, lon: latlng.lng });
  drawPressureRoute();
  if (state.pressureRoutePoints.length === 2) {
    state.pressureDrawActive = false;
    updatePressureRouteButtons();
    setPressureOutput(
      `custom route\nready to render ${labelForPressureProduct(pressureProduct.value)} f${String(pressureHour.value).padStart(3, "0")}`,
    );
  } else {
    setPressureOutput("custom route\nselect the second endpoint");
  }
  return true;
}

function pressureRoutePayload() {
  if (state.pressureRoutePoints.length !== 2) return null;
  const [start, end] = state.pressureRoutePoints;
  return {
    lat1: start.lat,
    lon1: start.lon,
    lat2: end.lat,
    lon2: end.lon,
    spacing_km: Number(pressureSpacing.value || 5),
    top_pressure_hpa: Number(pressureTop.value || 100),
    width: 1400,
    height: 820,
    route_name: "Selected CA Cross-Section",
  };
}

function renderPressureArtifact(report) {
  const records = report.records || [];
  const first = records[0];
  if (!first) {
    resetPressureRenderPreview();
    setPressureOutput("renderer returned no frames");
    return;
  }
  const url = first.webp_url || first.png_url;
  pressureRenderLink.href = url;
  pressureRenderImage.src = artifactUrl(url, report);
  pressureRenderLink.classList.remove("hidden");
  pressureLoopControls.classList.add("hidden");
  const products = [...new Set(records.map((record) => record.product_label || record.product))].join(", ");
  const hours = [...new Set(records.map((record) => record.hour))].sort((a, b) => a - b);
  const hourText = hours.length > 1
    ? `f${String(hours[0]).padStart(3, "0")}-f${String(hours[hours.length - 1]).padStart(3, "0")}`
    : `f${String(first.hour || 0).padStart(3, "0")}`;
  setPressureOutput(
    `${products} ${hourText}\n` +
      `${records.length} artifact frame(s) | renderer ${report.renderer_total_ms || "--"} ms | API ${report.server_elapsed_ms || "--"} ms\n` +
      `${first.webp_url || first.png_url}`,
  );
}

function setPressureLoopFrame(index) {
  if (!state.pressureLoopFrames.length) return;
  const clamped = Math.max(0, Math.min(index, state.pressureLoopFrames.length - 1));
  state.pressureLoopFrameIndex = clamped;
  const frame = state.pressureLoopFrames[clamped];
  const url = frame.webp_url || frame.png_url;
  pressureRenderLink.href = url;
  pressureRenderImage.src = artifactUrl(url, { generated_at_utc: frame.generated_at_utc || "" });
  pressureFrame.value = String(clamped);
  pressureFrameLabel.textContent = `f${String(frame.hour || 0).padStart(3, "0")}`;
}

function startPressureLoop() {
  if (!state.pressureLoopFrames.length) return;
  stopPressureLoop();
  state.pressureLoopPlaying = true;
  pressureLoopPlay.textContent = "Pause";
  state.pressureLoopTimer = setInterval(() => {
    setPressureLoopFrame((state.pressureLoopFrameIndex + 1) % state.pressureLoopFrames.length);
  }, 350);
}

function renderPressureLoop(report) {
  const frames = (report.frames || report.records || [])
    .filter((frame) => frame.webp_url || frame.png_url)
    .sort((a, b) => Number(a.hour || 0) - Number(b.hour || 0));
  state.pressureLoopFrames = frames.map((frame) => ({ ...frame, generated_at_utc: report.generated_at_utc }));
  if (!frames.length) {
    resetPressureRenderPreview();
    setPressureOutput("renderer returned no loop frames");
    return;
  }
  pressureRenderLink.classList.remove("hidden");
  pressureLoopControls.classList.remove("hidden");
  pressureFrame.min = "0";
  pressureFrame.max = String(frames.length - 1);
  setPressureLoopFrame(0);
  startPressureLoop();
  setPressureOutput(
    `${labelForPressureProduct(report.products?.[0] || pressureProduct.value)} loop\n` +
      `${frames.length} WebP frame(s) | renderer ${report.renderer_total_ms || "--"} ms | API ${report.server_elapsed_ms || "--"} ms\n` +
      `${report.manifest_url || ""}`,
  );
}

async function runPressureProfile() {
  if (!state.point) return;
  pressureProfileButton.disabled = true;
  setPressureOutput("sampling pressure volume profile...");
  try {
    const report = await fetchJson("/api/v1/public/pressure-profile", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lat: state.point.lat, lon: state.point.lng }),
    });
    setPressureOutput(summarizeProfile(report));
  } catch (error) {
    setPressureOutput(error.message);
  } finally {
    pressureProfileButton.disabled = false;
  }
}

async function runPressureRoute(routeKey) {
  const route = PRESSURE_ROUTES[routeKey];
  if (!route) return;
  setPressureRoute(
    [
      { lat: route.lat1, lon: route.lon1 },
      { lat: route.lat2, lon: route.lon2 },
    ],
    route.name,
  );
  runPressureRenderedPlot(route.name);
}

async function runPressureJsonRoute(routeKey) {
  const route = PRESSURE_ROUTES[routeKey];
  if (!route) return;
  setPressureOutput(`sampling ${route.name}...`);
  try {
    const report = await fetchJson("/api/v1/public/cross-section", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...route,
        hour: Number(pressureHour.value),
        variable: pressureVariable.value,
        spacing_km: 20,
      }),
    });
    setPressureOutput(summarizeCrossSection(report, route.name));
  } catch (error) {
    setPressureOutput(error.message);
  }
}

async function runPressureRenderedPlot(routeName = "Selected CA Cross-Section") {
  const payload = pressureRoutePayload();
  if (!payload) return;
  pressureRender.disabled = true;
  pressureLoop.disabled = true;
  resetPressureRenderPreview();
  setPressureOutput(`rendering ${routeName}...`);
  try {
    const report = await fetchJson("/api/v1/public/cross-section-render", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...payload,
        route_name: routeName,
        hour: Number(pressureHour.value),
        products: pressureProduct.value,
      }),
    });
    renderPressureArtifact(report);
  } catch (error) {
    setPressureOutput(error.message);
  } finally {
    updatePressureRouteButtons();
  }
}

async function runPressureRenderedLoop(routeName = "Selected CA Cross-Section") {
  const payload = pressureRoutePayload();
  if (!payload) return;
  const selectedHour = Math.max(0, Number(pressureHour.value) || 0);
  const hoursSpec = `0-${selectedHour}`;
  pressureRender.disabled = true;
  pressureLoop.disabled = true;
  resetPressureRenderPreview();
  setPressureOutput(`rendering f000-f${String(selectedHour).padStart(3, "0")} ${routeName} loop...`);
  try {
    const report = await fetchJson("/api/v1/public/cross-section-loop", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...payload,
        route_name: routeName,
        product: pressureProduct.value,
        hours: hoursSpec,
      }),
    });
    renderPressureLoop(report);
  } catch (error) {
    setPressureOutput(error.message);
  } finally {
    updatePressureRouteButtons();
  }
}

function renderMeteogramPng(report) {
  meteogramLink.href = report.url;
  meteogramImage.src = report.url;
  meteogramLink.classList.remove("hidden");
  const hours = report.forecast_hours || [];
  const range = hours.length
    ? `f${String(hours[0]).padStart(3, "0")}-f${String(hours[hours.length - 1]).padStart(3, "0")}`
    : "requested hours";
  const cache = report.cache_hit ? "cached" : "rendered";
  setStatus(`${cache} PNG ${range} in ${report.render_total_ms || "--"} ms; sample ${report.sample_total_ms || "--"} ms`);
}

async function runMeteogram() {
  if (!state.point) return;
  const [start, end] = document.getElementById("hour-range").value.split("-").map(Number);
  renderButton.disabled = true;
  setStatus("Rendering HRRR six-panel meteogram PNG...");
  try {
    const report = await fetchJson("/api/v1/public/meteogram.png", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        lat: state.point.lat,
        lon: state.point.lng,
        forecast_hour_start: start,
        forecast_hour_end: end,
        label: "Selected point",
      }),
    });
    renderMeteogramPng(report);
  } catch (error) {
    setStatus(error.message);
  } finally {
    renderButton.disabled = false;
  }
}

function setActiveManifest(kind) {
  state.activeManifest = kind;
  gallerySearch.value = "";
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.manifest === kind);
  });
  refreshGalleryControls(true);
  renderGallery();
}

map.on("click", (event) => {
  const usedForRoute = addPressureRoutePoint(event.latlng);
  state.point = event.latlng;
  if (state.marker) {
    state.marker.setLatLng(event.latlng);
  } else {
    state.marker = L.marker(event.latlng).addTo(map);
  }
  pointReadout.textContent = `${event.latlng.lat.toFixed(3)}, ${event.latlng.lng.toFixed(3)}`;
  renderButton.disabled = false;
  pressureProfileButton.disabled = false;
  if (usedForRoute && state.pressureRoutePoints.length === 2) {
    pointReadout.textContent = "Cross-section route selected";
  }
});

renderButton.addEventListener("click", runMeteogram);
pressureProfileButton.addEventListener("click", runPressureProfile);
pressureDrawRoute.addEventListener("click", () => {
  state.pressureDrawActive = !state.pressureDrawActive;
  if (state.pressureDrawActive) {
    state.pressureRoutePoints = [];
    drawPressureRoute();
    setPressureOutput("custom route\nselect the first endpoint");
  }
  updatePressureRouteButtons();
});
pressureClearRoute.addEventListener("click", clearPressureRoute);
pressureRender.addEventListener("click", () => runPressureRenderedPlot());
pressureLoop.addEventListener("click", () => runPressureRenderedLoop());
pressureLoopPlay.addEventListener("click", () => {
  if (state.pressureLoopPlaying) {
    stopPressureLoop();
  } else {
    startPressureLoop();
  }
});
pressureFrame.addEventListener("input", () => {
  stopPressureLoop();
  setPressureLoopFrame(Number(pressureFrame.value));
});
document.querySelectorAll("[data-pressure-route]").forEach((button) => {
  button.addEventListener("click", () => runPressureRoute(button.dataset.pressureRoute));
});
document.getElementById("hour-range").addEventListener("change", () => {
  if (state.point) runMeteogram();
});
document.getElementById("refresh-gallery").addEventListener("click", loadGallery);
galleryHour.addEventListener("change", renderGallery);
galleryProduct.addEventListener("change", renderGallery);
galleryFormat.addEventListener("change", () => {
  localStorage.setItem(FORMAT_KEY, galleryFormat.value);
  renderGallery();
});
gallerySize.addEventListener("change", () => {
  localStorage.setItem(SIZE_KEY, gallerySize.value);
  renderGallery();
});
galleryLoop.addEventListener("change", () => {
  localStorage.setItem(LOOP_KEY, galleryLoop.value);
  renderGallery();
});
gallerySearch.addEventListener("input", renderGallery);
document.querySelectorAll(".tab-button").forEach((button) => {
  button.addEventListener("click", () => setActiveManifest(button.dataset.manifest));
});

loadGallery().then(loadWarmStatus);
setInterval(loadWarmStatus, 30000);
setInterval(loadGallery, 120000);
