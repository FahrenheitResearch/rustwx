param(
    [string]$Version = "0.1.0-preview",
    [string]$OutputDir = "dist\model-maps-preview",
    [string]$WxStoreExe = "",
    [switch]$SkipBuild,
    [switch]$SkipPythonDownload,
    [switch]$SkipInstaller
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$OutputRoot = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $OutputDir))
$Stage = Join-Path $OutputRoot "RustWxModelMaps"
$WheelDir = Join-Path $OutputRoot "wheels"
$AppDir = Join-Path $Stage "app"
$BinDir = Join-Path $AppDir "bin"
$AssetsDir = Join-Path $AppDir "assets"
$PythonDir = Join-Path $AppDir "python"
$PythonLibDir = Join-Path $AppDir "python_lib"
$LauncherExe = "RustWx Model Maps.exe"

function Test-PathUnder($Path, $Root) {
    $Full = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
    $RootFull = [System.IO.Path]::GetFullPath($Root).TrimEnd('\')
    return $Full.Equals($RootFull, [System.StringComparison]::OrdinalIgnoreCase) -or $Full.StartsWith($RootFull + '\', [System.StringComparison]::OrdinalIgnoreCase)
}

function Assert-SafeCleanDir($Path) {
    $Full = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
    $RepoFull = [System.IO.Path]::GetFullPath($RepoRoot).TrimEnd('\')
    $DriveRoot = [System.IO.Path]::GetPathRoot($Full).TrimEnd('\')
    if ($Full.Equals($DriveRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clean filesystem root: $Full"
    }
    if ($Full.Equals($RepoFull, [System.StringComparison]::OrdinalIgnoreCase) -or $RepoFull.StartsWith($Full + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clean repo root or repo ancestor: $Full"
    }
    $AllowedRoots = @(
        (Join-Path $RepoRoot "dist"),
        (Join-Path $RepoRoot "release_dist")
    )
    foreach ($AllowedRoot in $AllowedRoots) {
        $AllowedFull = [System.IO.Path]::GetFullPath($AllowedRoot).TrimEnd('\')
        if ($Full.Equals($AllowedFull, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to clean top-level release directory directly: $Full"
        }
    }
    if (-not ($AllowedRoots | Where-Object { Test-PathUnder $Full $_ })) {
        throw "Refusing to clean outside repo dist/release_dist: $Full"
    }
}

function Clean-Dir($Path) {
    Assert-SafeCleanDir $Path
    if (Test-Path $Path) { Remove-Item -LiteralPath $Path -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Resolve-WxStoreExe {
    param([string]$Explicit)
    $Candidates = @()
    if ($Explicit) { $Candidates += $Explicit }
    if ($env:WXSTORE_EXE) { $Candidates += $env:WXSTORE_EXE }
    $Command = Get-Command wxstore -ErrorAction SilentlyContinue
    if ($Command) { $Candidates += $Command.Source }
    foreach ($Candidate in $Candidates) {
        if ($Candidate -and (Test-Path $Candidate)) {
            return (Resolve-Path $Candidate).Path
        }
    }
    return $null
}

Clean-Dir $OutputRoot
New-Item -ItemType Directory -Force -Path $BinDir, $WheelDir, $PythonLibDir, $AssetsDir | Out-Null

if (-not $SkipBuild) {
    Push-Location $RepoRoot
    cargo build --release --bin direct_batch --bin sounding_plot --bin hrrr_pressure_volume_store --bin volume_store_sounding_render --bin model_wxprofile_store --bin wxprofile_sounding_render --bin rustwx_grid_export --bin wxstore_wxa_showcase
    cargo build --release --package rustwx-model-maps-launcher
    Pop-Location

    Push-Location (Join-Path $RepoRoot "crates\rustwx-python")
    python -m maturin build --release --out $WheelDir
    Pop-Location
}

$RequiredBins = @(
    "direct_batch.exe",
    "sounding_plot.exe",
    "hrrr_pressure_volume_store.exe",
    "volume_store_sounding_render.exe",
    "model_wxprofile_store.exe",
    "wxprofile_sounding_render.exe",
    "rustwx_grid_export.exe",
    "wxstore_wxa_showcase.exe"
)
foreach ($Name in $RequiredBins) {
    $Source = Join-Path $RepoRoot "target\release\$Name"
    if (Test-Path $Source) {
        Copy-Item -LiteralPath $Source -Destination $BinDir -Force
    } else {
        throw "Missing required binary: $Name. Run without -SkipBuild or fix the release build."
    }
}

$ResolvedWxStore = Resolve-WxStoreExe -Explicit $WxStoreExe
if ($ResolvedWxStore) {
    Copy-Item -LiteralPath $ResolvedWxStore -Destination (Join-Path $BinDir "wxstore.exe") -Force
    Write-Host "Bundled WxStore: $ResolvedWxStore"
} else {
    throw "wxstore.exe is required for the Model Maps release package. Pass -WxStoreExe or set WXSTORE_EXE."
}

$BasemapSource = Join-Path $RepoRoot "assets\basemap"
if (-not (Test-Path $BasemapSource)) {
    throw "Basemap assets missing: $BasemapSource"
}
Copy-Item -LiteralPath $BasemapSource -Destination (Join-Path $AssetsDir "basemap") -Recurse -Force

$LauncherSource = Join-Path $RepoRoot "target\release\rustwx-model-maps-launcher.exe"
if (-not (Test-Path $LauncherSource)) {
    throw "Missing launcher binary: $LauncherSource"
}
Copy-Item -LiteralPath $LauncherSource -Destination (Join-Path $Stage $LauncherExe) -Force

$Wheel = Get-ChildItem -Path $WheelDir -Filter "rustwx-*.whl" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $Wheel) {
    throw "No rustwx wheel found in $WheelDir. Run without -SkipBuild or provide a built wheel."
}

$PyVersion = (& python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')").Trim()
$PyMinor = (& python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')").Trim()
$EmbedZip = Join-Path $OutputRoot "python-$PyVersion-embed-amd64.zip"
if (-not $SkipPythonDownload) {
    $Url = "https://www.python.org/ftp/python/$PyVersion/python-$PyVersion-embed-amd64.zip"
    Invoke-WebRequest -Uri $Url -OutFile $EmbedZip
    Expand-Archive -LiteralPath $EmbedZip -DestinationPath $PythonDir -Force
}
if (-not (Test-Path (Join-Path $PythonDir "python.exe"))) {
    throw "Embedded Python runtime missing. Expected $PythonDir\python.exe."
}

python -m pip install --upgrade --target $PythonLibDir $Wheel.FullName "numpy>=1.26,<2.3"

$Pth = Join-Path $PythonDir "python$PyMinor._pth"
if (Test-Path $Pth) {
    $Lines = Get-Content -LiteralPath $Pth | Where-Object { $_ -ne "#import site" -and $_ -ne "import site" -and $_ -ne "..\python_lib" }
    $Lines += "..\python_lib"
    $Lines += "import site"
    Set-Content -LiteralPath $Pth -Value $Lines -Encoding ASCII
}

$RunBat = @"
@echo off
setlocal
"%~dp0$LauncherExe" %*
if errorlevel 1 pause
"@
Set-Content -LiteralPath (Join-Path $Stage "Run RustWx Model Maps.bat") -Value $RunBat -Encoding ASCII

$InstallPs1 = @'
$ErrorActionPreference = "Stop"
$InstallDir = Join-Path $env:LOCALAPPDATA "RustWx\ModelMapsApp"
$Source = Split-Path -Parent $MyInvocation.MyCommand.Path
if (Test-Path $InstallDir) { Remove-Item -LiteralPath $InstallDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item -Path (Join-Path $Source "*") -Destination $InstallDir -Recurse -Force
$ShortcutPath = Join-Path ([Environment]::GetFolderPath("Desktop")) "RustWx Model Maps.lnk"
$Shell = New-Object -ComObject WScript.Shell
$Shortcut = $Shell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = Join-Path $InstallDir "RustWx Model Maps.exe"
$Shortcut.WorkingDirectory = $InstallDir
$Shortcut.IconLocation = $Shortcut.TargetPath
$Shortcut.Save()
$StartMenuDir = Join-Path ([Environment]::GetFolderPath("Programs")) "RustWx"
New-Item -ItemType Directory -Force -Path $StartMenuDir | Out-Null
$StartMenuShortcut = Join-Path $StartMenuDir "RustWx Model Maps.lnk"
$MenuLink = $Shell.CreateShortcut($StartMenuShortcut)
$MenuLink.TargetPath = Join-Path $InstallDir "RustWx Model Maps.exe"
$MenuLink.WorkingDirectory = $InstallDir
$MenuLink.IconLocation = $MenuLink.TargetPath
$MenuLink.Save()
Write-Host "Installed RustWx Model Maps to $InstallDir"
Write-Host "Desktop shortcut: $ShortcutPath"
Write-Host "Start menu shortcut: $StartMenuShortcut"
Read-Host "Press Enter to close"
'@
Set-Content -LiteralPath (Join-Path $Stage "Install RustWx Model Maps.ps1") -Value $InstallPs1 -Encoding ASCII

$InstallBat = @"
@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0Install RustWx Model Maps.ps1"
"@
Set-Content -LiteralPath (Join-Path $Stage "Install RustWx Model Maps.bat") -Value $InstallBat -Encoding ASCII

$Readme = @"
RustWx Model Maps $Version

Quick start:
1. Run "RustWx Model Maps.exe" directly for portable use.
2. Or run "Install RustWx Model Maps.bat" to copy the app to LocalAppData and create desktop/start-menu shortcuts.

Data locations:
- App install: %LOCALAPPDATA%\RustWx\ModelMapsApp
- Outputs/cache: %LOCALAPPDATA%\RustWx\ModelMaps
- Logs: %LOCALAPPDATA%\RustWx\ModelMaps\logs

The app is local-only. Normal use opens a browser pointed at 127.0.0.1.

Cache layers are visible in the Data panel and can be deleted independently:
raw GRIB cache, WxStore spatial map cache, WxProfile sounding stores, and PNG outputs.

Trust note:
This preview package is unsigned unless the release artifacts are Authenticode-signed before distribution.
Use the SHA256SUMS.txt hashes from the release page when sharing unsigned preview builds.
"@
Set-Content -LiteralPath (Join-Path $Stage "README.txt") -Value $Readme -Encoding ASCII

$Zip = Join-Path $OutputRoot "RustWxModelMaps-$Version-windows-x64.zip"
if (Test-Path $Zip) { Remove-Item -LiteralPath $Zip -Force }
Compress-Archive -Path (Join-Path $Stage "*") -DestinationPath $Zip -Force
Write-Host "Portable package: $Stage"
Write-Host "Zip: $Zip"

$Artifacts = @($Zip)
if (-not $SkipInstaller) {
    $IExpress = Get-Command iexpress -ErrorAction SilentlyContinue
    if ($IExpress) {
        $InstallerWork = Join-Path $OutputRoot "installer-work"
        Clean-Dir $InstallerWork
        $ZipName = Split-Path -Leaf $Zip
        Copy-Item -LiteralPath $Zip -Destination (Join-Path $InstallerWork $ZipName) -Force

        $InstallPayloadPs1 = @'
param([string]$ZipPath)
$ErrorActionPreference = "Stop"
if (-not $ZipPath) {
    $ZipPath = Join-Path $PSScriptRoot "__ZIP_NAME__"
}
$InstallDir = Join-Path $env:LOCALAPPDATA "RustWx\ModelMapsApp"
$TempDir = Join-Path $env:TEMP ("RustWxModelMaps-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null
try {
    Expand-Archive -LiteralPath $ZipPath -DestinationPath $TempDir -Force
    if (Test-Path $InstallDir) {
        Remove-Item -LiteralPath $InstallDir -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item -Path (Join-Path $TempDir "*") -Destination $InstallDir -Recurse -Force

    $Exe = Join-Path $InstallDir "RustWx Model Maps.exe"
    $Shell = New-Object -ComObject WScript.Shell
    $DesktopShortcut = Join-Path ([Environment]::GetFolderPath("Desktop")) "RustWx Model Maps.lnk"
    $Desktop = $Shell.CreateShortcut($DesktopShortcut)
    $Desktop.TargetPath = $Exe
    $Desktop.WorkingDirectory = $InstallDir
    $Desktop.IconLocation = $Exe
    $Desktop.Save()

    $StartMenuDir = Join-Path ([Environment]::GetFolderPath("Programs")) "RustWx"
    New-Item -ItemType Directory -Force -Path $StartMenuDir | Out-Null
    $StartMenuShortcut = Join-Path $StartMenuDir "RustWx Model Maps.lnk"
    $StartMenu = $Shell.CreateShortcut($StartMenuShortcut)
    $StartMenu.TargetPath = $Exe
    $StartMenu.WorkingDirectory = $InstallDir
    $StartMenu.IconLocation = $Exe
    $StartMenu.Save()

    Start-Process -FilePath $Exe -WorkingDirectory $InstallDir
} finally {
    if (Test-Path $TempDir) {
        Remove-Item -LiteralPath $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
'@.Replace("__ZIP_NAME__", $ZipName)
        Set-Content -LiteralPath (Join-Path $InstallerWork "install_payload.ps1") -Value $InstallPayloadPs1 -Encoding ASCII

        $InstallPayloadCmd = @"
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_payload.ps1" -ZipPath "%~dp0$ZipName"
if errorlevel 1 pause
"@
        Set-Content -LiteralPath (Join-Path $InstallerWork "install_payload.cmd") -Value $InstallPayloadCmd -Encoding ASCII

        $InstallerExe = Join-Path $OutputRoot "RustWxModelMaps-$Version-windows-x64-installer.exe"
        if (Test-Path $InstallerExe) { Remove-Item -LiteralPath $InstallerExe -Force }
        $SedPath = Join-Path $InstallerWork "rustwx_model_maps_installer.sed"
        $Sed = @"
[Version]
Class=IEXPRESS
SEDVersion=3
[Options]
PackagePurpose=InstallApp
ShowInstallProgramWindow=1
HideExtractAnimation=0
UseLongFileName=1
InsideCompressed=0
CAB_FixedSize=0
CAB_ResvCodeSigning=0
RebootMode=N
InstallPrompt=
DisplayLicense=
FinishMessage=RustWx Model Maps installed.
TargetName=$InstallerExe
FriendlyName=RustWx Model Maps $Version
AppLaunched=install_payload.cmd
PostInstallCmd=<None>
AdminQuietInstCmd=
UserQuietInstCmd=
SourceFiles=SourceFiles
[Strings]
FILE0="install_payload.cmd"
FILE1="install_payload.ps1"
FILE2="$ZipName"
[SourceFiles]
SourceFiles0=$InstallerWork
[SourceFiles0]
%FILE0%=
%FILE1%=
%FILE2%=
"@
        Set-Content -LiteralPath $SedPath -Value $Sed -Encoding ASCII
        & $IExpress.Source /N /Q $SedPath
        $WaitUntil = (Get-Date).AddSeconds(45)
        while (-not (Test-Path $InstallerExe) -and (Get-Date) -lt $WaitUntil) {
            Start-Sleep -Milliseconds 500
        }
        if (Test-Path $InstallerExe) {
            $Artifacts += $InstallerExe
            Write-Host "Installer EXE: $InstallerExe"
        } else {
            Write-Warning "IExpress finished but did not create $InstallerExe"
        }
    } else {
        Write-Warning "IExpress was not found; skipping single-file installer EXE."
    }
}

$HashFile = Join-Path $OutputRoot "SHA256SUMS.txt"
$HashLines = foreach ($Artifact in $Artifacts) {
    $Hash = Get-FileHash -Algorithm SHA256 -LiteralPath $Artifact
    "$($Hash.Hash.ToLowerInvariant())  $(Split-Path -Leaf $Artifact)"
}
Set-Content -LiteralPath $HashFile -Value $HashLines -Encoding ASCII
Write-Host "SHA256: $HashFile"
