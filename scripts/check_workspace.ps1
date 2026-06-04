[CmdletBinding()]
param(
    [switch] $SkipFmt,
    [switch] $SkipKnownFailingPackageTests
)

$ErrorActionPreference = "Stop"

function Invoke-Check {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command
    )

    Write-Host ""
    Write-Host "==> $Name"
    $global:LASTEXITCODE = 0
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

if (-not $SkipFmt) {
    Invoke-Check "cargo fmt --all -- --check" {
        cargo fmt --all -- --check
    }
} else {
    Write-Host ""
    Write-Host "==> Skipping cargo fmt --all -- --check"
}

Invoke-Check "cargo check --workspace --all-targets" {
    cargo check --workspace --all-targets
}

$packages = @(
    "rustwx-models",
    "rustwx-io",
    "rustwx-products",
    "rustwx-render",
    "rustwx-radar",
    "rustwx-regrid"
)

if ($SkipKnownFailingPackageTests) {
    $packages = $packages | Where-Object { $_ -notin @("rustwx-products", "rustwx-render") }
    Write-Host ""
    Write-Host "==> Skipping currently known failing package tests: rustwx-products, rustwx-render"
}

foreach ($package in $packages) {
    Invoke-Check "cargo test -p $package --lib" {
        cargo test -p $package --lib
    }
}

Invoke-Check "cargo test -p rustwx-products --test product_catalog_inventory" {
    cargo test -p rustwx-products --test product_catalog_inventory
}

Invoke-Check "cargo test -p rustwx-cli --test bin_inventory" {
    cargo test -p rustwx-cli --test bin_inventory
}

Write-Host ""
Write-Host "Workspace checks passed."
