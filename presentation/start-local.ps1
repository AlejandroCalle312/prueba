Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$presentationPath = Join-Path $repoRoot 'presentation'

if (-not (Test-Path $presentationPath)) {
    throw "Presentation path not found: $presentationPath"
}

Write-Host "Starting presentation server on http://localhost:3000"
Write-Host "Directory: $presentationPath"

Push-Location $presentationPath
try {
    python -m http.server 3000
}
finally {
    Pop-Location
}
