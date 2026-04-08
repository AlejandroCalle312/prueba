Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$servicePath = $PSScriptRoot
$repoRoot = Resolve-Path (Join-Path $servicePath '..\..')
$venvPython = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (Test-Path $venvPython) {
    $pythonExe = $venvPython
} else {
    $pythonExe = 'python'
}

Write-Host "Starting API server on http://localhost:8000"
Write-Host "Directory: $servicePath"
Write-Host "Python: $pythonExe"

Push-Location $servicePath
try {
    & $pythonExe -m uvicorn app:app --host 0.0.0.0 --port 8000
}
finally {
    Pop-Location
}
