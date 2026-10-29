$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runDir = Join-Path $root ".run"

foreach ($name in @("worker", "consumer", "outbox", "api")) {
    $pidPath = Join-Path $runDir "$name.pid"
    if (-not (Test-Path $pidPath)) { continue }
    $processId = [int](Get-Content $pidPath -Raw)
    if (Get-Process -Id $processId -ErrorAction SilentlyContinue) {
        Stop-Process -Id $processId
        Write-Host "Stopped $name (PID $processId)."
    }
    Remove-Item $pidPath
}
