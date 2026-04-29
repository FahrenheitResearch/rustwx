# Local CA Fire Volume Demo

This runs the CA Fire FastAPI/dashboard against a local rustwx pressure VolumeStore sidecar. It does not touch the Hetzner instance, R2, or production workers unless you set those env vars yourself.

## 1. Build the sidecar

From the repo root:

```powershell
cargo build -p rustwx-cli --release --bin volume_store_dashboard --bin hrrr_pressure_volume_store
```

## 2. Start the VolumeStore sidecar

This uses the existing real HRRR CA proof store if it is present:

```powershell
target\release\volume_store_dashboard.exe `
  --store proof\hrrr_pressure_volume_store_warm\store `
  --host 127.0.0.1 `
  --port 8797
```

Sidecar checks:

```powershell
Invoke-RestMethod http://127.0.0.1:8797/api/metadata
Invoke-RestMethod "http://127.0.0.1:8797/api/point?lat=40.5865&lon=-122.3917"
```

## 3. Start the local CA Fire API/site

In a second terminal:

```powershell
cd deploy\cafire-weather-service

$env:PUBLIC_SITE_ENABLED = "1"
$env:PRESSURE_VOLUME_ENABLED = "1"
$env:PRESSURE_VOLUME_BASE_URL = "http://127.0.0.1:8797"

# Keep this local proof isolated from production/background processing.
$env:ARTIFACT_ROOT = "..\..\proof\cafire_local_artifacts"
$env:RUSTWX_CACHE_DIR = "..\..\proof\cache"
$env:RUSTWX_GLM_DIR = "..\..\proof\glm"
$env:METEOGRAM_WARM_STATUS_PATH = "..\..\proof\cafire_local_artifacts\meteogram_warm_status.json"
$env:METEOGRAM_WARM_IN_API = "0"
$env:FAST_METEOGRAM_STORE_ENABLED = "0"
$env:STATIC_MAP_WORKER_ENABLED = "0"
$env:LIGHTNING_ENABLED = "0"
$env:CACHE_CLEANUP_ENABLED = "0"

python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open:

```text
http://127.0.0.1:8000/
```

Use the normal static gallery/meteogram UI, plus the new **Pressure Volume** card. The pressure card proxies to the Rust sidecar and should return point profiles in a few milliseconds and CA route cross sections in tens of milliseconds once the sidecar is open.

Shortcut launcher:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\cafire-weather-service\ops\run_local_volume_demo.ps1
```

## API endpoints

```http
GET  /api/v1/public/pressure-volume/status
POST /api/v1/public/pressure-profile
POST /api/v1/public/cross-section
```

Example:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v1/public/pressure-profile `
  -Method Post `
  -ContentType "application/json" `
  -Body '{"lat":40.5865,"lon":-122.3917}'
```

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v1/public/cross-section `
  -Method Post `
  -ContentType "application/json" `
  -Body '{"lat1":37.7749,"lon1":-122.4194,"lat2":38.5788,"lon2":-119.7513,"hour":0,"variable":"TMP","spacing_km":20}'
```
