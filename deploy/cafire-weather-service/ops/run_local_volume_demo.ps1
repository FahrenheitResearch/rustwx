param(
  [int]$ApiPort = 8000,
  [int]$SidecarPort = 8797,
  [string]$Store = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServiceDir = Resolve-Path (Join-Path $ScriptDir "..")
$RepoRoot = Resolve-Path (Join-Path $ServiceDir "..\..")

if (-not $Store) {
  $Store = Join-Path $RepoRoot "proof\hrrr_pressure_volume_store_warm\store"
}

$SidecarExe = Join-Path $RepoRoot "target\release\volume_store_dashboard.exe"
if (-not (Test-Path $SidecarExe)) {
  throw "Missing $SidecarExe. Run: cargo build -p rustwx-cli --release --bin volume_store_dashboard"
}
if (-not (Test-Path $Store)) {
  throw "Missing VolumeStore at $Store"
}

$env:PUBLIC_SITE_ENABLED = "1"
$env:PRESSURE_VOLUME_ENABLED = "1"
$env:PRESSURE_VOLUME_BASE_URL = "http://127.0.0.1:$SidecarPort"
$env:ARTIFACT_ROOT = Join-Path $RepoRoot "proof\cafire_local_artifacts"
$env:RUSTWX_CACHE_DIR = Join-Path $RepoRoot "proof\cache"
$env:RUSTWX_GLM_DIR = Join-Path $RepoRoot "proof\glm"
$env:METEOGRAM_WARM_STATUS_PATH = Join-Path $RepoRoot "proof\cafire_local_artifacts\meteogram_warm_status.json"
$env:METEOGRAM_WARM_IN_API = "0"
$env:FAST_METEOGRAM_STORE_ENABLED = "0"
$env:STATIC_MAP_WORKER_ENABLED = "0"
$env:LIGHTNING_ENABLED = "0"
$env:CACHE_CLEANUP_ENABLED = "0"

$SidecarCommand = "Set-Location '$RepoRoot'; & '$SidecarExe' --store '$Store' --host 127.0.0.1 --port $SidecarPort"
$ApiCommand = "Set-Location '$ServiceDir'; python -m uvicorn app.main:app --host 127.0.0.1 --port $ApiPort"

Start-Process powershell -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $SidecarCommand)
Start-Sleep -Seconds 1
Start-Process powershell -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $ApiCommand)
Start-Process "http://127.0.0.1:$ApiPort/"

Write-Host "Local CA Fire Volume demo launching at http://127.0.0.1:$ApiPort/"
