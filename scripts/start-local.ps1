$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root
$python = Join-Path $root ".venv\Scripts\python.exe"
$runDir = Join-Path $root ".run"
$logDir = Join-Path $root "logs"

if (-not (Test-Path $python)) {
    throw "Python environment is missing. Run .\scripts\setup-local.ps1 first."
}
if (-not (Test-Path ".env")) {
    throw "Missing .env. Copy .env.example to .env, configure local services, then retry."
}
if (-not (Test-Path $runDir)) { New-Item -ItemType Directory -Path $runDir | Out-Null }
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

foreach ($name in @("api", "outbox", "consumer", "worker")) {
    $pidPath = Join-Path $runDir "$name.pid"
    if (Test-Path $pidPath) {
        $existingPid = [int](Get-Content $pidPath -Raw)
        if (Get-Process -Id $existingPid -ErrorAction SilentlyContinue) {
            throw "A managed $name process is already running (PID $existingPid). Use .\scripts\stop-local.ps1 first."
        }
        Remove-Item $pidPath
    }
}

& $python -m app.cli check
if ($LASTEXITCODE -ne 0) {
    throw "Local dependency check failed. Start PostgreSQL and RabbitMQ Windows services and verify .env."
}

& (Join-Path $root ".venv\Scripts\alembic.exe") upgrade head
if ($LASTEXITCODE -ne 0) { throw "Database migration failed; application processes were not started." }

$processes = @(
    @{ Name = "api"; Module = "app.processes.api" },
    @{ Name = "outbox"; Module = "app.processes.outbox_publisher" },
    @{ Name = "consumer"; Module = "app.processes.inbound_consumer" },
    @{ Name = "worker"; Module = "app.processes.worker_simulator" }
)
$started = @()
try {
    foreach ($item in $processes) {
        $stdout = Join-Path $logDir "$($item.Name).out.log"
        $stderr = Join-Path $logDir "$($item.Name).err.log"
        $process = Start-Process -FilePath $python `
            -ArgumentList @("-m", $item.Module) `
            -WorkingDirectory $root `
            -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr `
            -WindowStyle Hidden `
            -PassThru
        Set-Content -Path (Join-Path $runDir "$($item.Name).pid") -Value $process.Id
        $started += $process.Id
    }
    Start-Sleep -Seconds 2
    foreach ($processId in $started) {
        if (-not (Get-Process -Id $processId -ErrorAction SilentlyContinue)) {
            throw "A process exited during startup. Inspect logs\ for its error details."
        }
    }
    Write-Host "Started API, outbox publisher, inbound consumer, and worker simulator."
    Write-Host "API: http://127.0.0.1:8000/docs (check .env if API_HOST/API_PORT differ)"
    Write-Host "Logs: $logDir"
}
catch {
    foreach ($processId in $started) {
        if (Get-Process -Id $processId -ErrorAction SilentlyContinue) {
            Stop-Process -Id $processId
        }
    }
    foreach ($name in @("api", "outbox", "consumer", "worker")) {
        $pidPath = Join-Path $runDir "$name.pid"
        if (Test-Path $pidPath) { Remove-Item $pidPath }
    }
    throw
}
