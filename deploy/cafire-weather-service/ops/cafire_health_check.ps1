param(
    [string]$RepoRoot = "C:\Users\drew\rustwx",
    [string]$SshTarget = "root@178.104.59.253",
    [string]$RemotePath = "/opt/cafire-weather-service",
    [string]$PublicBase = "https://cafire.wxsection.com"
)

$ErrorActionPreference = "Stop"

$outDir = Join-Path $RepoRoot "deploy\cafire-weather-service\ops\health_checks"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$now = Get-Date
$stamp = $now.ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$latestPath = Join-Path $outDir "latest.json"
$snapshotPath = Join-Path $outDir "$stamp.json"
$logPath = Join-Path $outDir "health_check.log"

function Add-Log {
    param([string]$Message)
    $line = "$(Get-Date -Format o) $Message"
    Add-Content -Path $logPath -Value $line
}

function Invoke-JsonGet {
    param([string]$Url, [int]$TimeoutSec = 20)
    try {
        return @{
            ok = $true
            value = Invoke-RestMethod -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSec
            error = $null
        }
    } catch {
        return @{
            ok = $false
            value = $null
            error = $_.Exception.Message
        }
    }
}

function Invoke-RemoteJson {
    $remoteScript = @'
from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime


def run(cmd: str, timeout: int = 25) -> dict:
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return {"ok": p.returncode == 0, "stdout": p.stdout, "stderr": p.stderr, "code": p.returncode}
    except subprocess.TimeoutExpired as exc:
        return {"ok": False, "stdout": exc.stdout or "", "stderr": "timeout", "code": 124}


def compose_services() -> list[dict]:
    result = run("docker compose ps --format json", 20)
    rows = []
    for line in result["stdout"].splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        rows.append({
            "service": item.get("Service"),
            "state": item.get("State"),
            "status": item.get("Status"),
        })
    return rows


def df_root() -> dict:
    result = run("df -B1 / | tail -1", 10)
    parts = result["stdout"].split()
    if len(parts) < 6:
        return {"ok": False, "error": result["stderr"] or result["stdout"]}
    return {
        "ok": True,
        "filesystem": parts[0],
        "size_bytes": int(parts[1]),
        "used_bytes": int(parts[2]),
        "free_bytes": int(parts[3]),
        "used_pct": int(parts[4].rstrip("%")),
        "mount": parts[5],
    }


def du_bytes(path: str) -> int | None:
    result = run(f"du -sb {path} 2>/dev/null | awk '{{print $1}}'", 20)
    try:
        return int(result["stdout"].strip().splitlines()[0])
    except Exception:
        return None


health = run("curl -fsS http://127.0.0.1:8000/health", 20)
try:
    health_json = json.loads(health["stdout"]) if health["ok"] else {"ok": False, "error": health["stderr"] or health["stdout"]}
except json.JSONDecodeError:
    health_json = {"ok": False, "error": "health returned non-json"}

print(json.dumps({
    "generated_at_utc": datetime.now(UTC).isoformat(),
    "compose_services": compose_services(),
    "health": health_json,
    "disk": df_root(),
    "data_dirs": {
        "cache_bytes": du_bytes("data/cache"),
        "artifacts_bytes": du_bytes("data/artifacts"),
        "glm_bytes": du_bytes("data/glm"),
    },
}, separators=(",", ":")))
'@

    try {
        $raw = $remoteScript | & ssh -o BatchMode=yes -o ConnectTimeout=10 $SshTarget "cd $RemotePath && python3 -"
        if ($LASTEXITCODE -ne 0) {
            return @{ ok = $false; error = "ssh exited $LASTEXITCODE"; raw = ($raw -join "`n") }
        }
        return @{ ok = $true; value = ($raw -join "`n" | ConvertFrom-Json); error = $null }
    } catch {
        return @{ ok = $false; error = $_.Exception.Message; raw = $null }
    }
}

function AgeSeconds {
    param($IsoTime)
    if (-not $IsoTime) { return $null }
    try {
        return [math]::Round(((Get-Date).ToUniversalTime() - ([datetime]::Parse($IsoTime).ToUniversalTime())).TotalSeconds)
    } catch {
        return $null
    }
}

$public = @{}
foreach ($name in @("latest-artifacts", "latest-diurnal-artifacts", "latest-lightning-artifacts", "warm-status")) {
    $public[$name] = Invoke-JsonGet "$PublicBase/api/v1/public/$name"
}

$remote = Invoke-RemoteJson
$issues = New-Object System.Collections.Generic.List[string]

if (-not $remote.ok) {
    $issues.Add("remote ssh check failed: $($remote.error)")
} else {
    $services = @($remote.value.compose_services)
    $down = @($services | Where-Object { $_.state -ne "running" })
    if ($down.Count -gt 0) {
        $issues.Add("down services: " + (($down | ForEach-Object { $_.service }) -join ","))
    }
    if (-not $remote.value.health.ok) {
        $issues.Add("api health failed")
    }
    $freeBytes = [double]($remote.value.disk.free_bytes)
    if ($freeBytes -lt 160GB) {
        $issues.Add("root disk free below 160G")
    }
    $cacheBytes = [double]($remote.value.data_dirs.cache_bytes)
    if ($cacheBytes -gt 220GB) {
        $issues.Add("cache above 220G")
    }
}

foreach ($name in @("latest-artifacts", "latest-diurnal-artifacts", "latest-lightning-artifacts", "warm-status")) {
    if (-not $public[$name].ok) {
        $issues.Add("public $name failed: $($public[$name].error)")
    }
}

if ($public["latest-lightning-artifacts"].ok) {
    $lightning = $public["latest-lightning-artifacts"].value
    $age = AgeSeconds $lightning.latest_glm_last_modified
    if ($age -ne $null -and $age -gt 600) {
        $issues.Add("lightning GLM age above 10 min")
    }
}

$summary = [ordered]@{
    ok = ($issues.Count -eq 0)
    generated_at_utc = $now.ToUniversalTime().ToString("o")
    issues = @($issues)
    public = $public
    remote = $remote
}

$json = $summary | ConvertTo-Json -Depth 40
Set-Content -Path $snapshotPath -Value $json -Encoding UTF8
Set-Content -Path $latestPath -Value $json -Encoding UTF8
Add-Log ("ok={0} issues={1}" -f $summary.ok, ($issues -join "; "))

# Keep roughly two days of 5-minute snapshots.
Get-ChildItem -Path $outDir -Filter "*.json" |
    Where-Object { $_.Name -ne "latest.json" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -Skip 600 |
    Remove-Item -Force -ErrorAction SilentlyContinue
