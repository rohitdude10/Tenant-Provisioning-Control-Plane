$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root
$python = Join-Path $root ".venv\Scripts\python.exe"
$runDir = Join-Path $root ".run"
$logDir = Join-Path $root "logs"

if (-not (Test-Path $python)) {
    & (Join-Path $root "scripts\setup-local.ps1")
    if ($LASTEXITCODE -ne 0) { throw "Local Python setup failed." }
}
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    throw "Created .env from .env.example without overwriting user configuration. Set local credentials and rerun .\scripts\start-local.ps1."
}
if (-not (Test-Path $runDir)) { New-Item -ItemType Directory -Path $runDir | Out-Null }
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$processModules = @{
    api = "app.processes.api"
    outbox = "app.processes.outbox_publisher"
    consumer = "app.processes.inbound_consumer"
    worker = "app.processes.worker_simulator"
    monitor = "monitor-local.ps1"
}
function Get-ManagedProcess([int]$processId, [string]$module) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$processId" -ErrorAction SilentlyContinue
    if ($module -eq "monitor-local.ps1" -and $process -and
        $process.Name -match "^powershell\.exe$" -and
        $process.CommandLine -match [regex]::Escape("scripts\monitor-local.ps1")) {
        return $process
    }
    if ($process -and $process.Name -match "^python(w)?\.exe$" -and
        $process.CommandLine -match [regex]::Escape("-m $module")) {
        return $process
    }
    return $null
}

foreach ($name in @("api", "outbox", "consumer", "worker", "monitor")) {
    $pidPath = Join-Path $runDir "$name.pid"
    if (Test-Path $pidPath) {
        $existingPid = [int](Get-Content $pidPath -Raw)
        if (Get-ManagedProcess $existingPid $processModules[$name]) {
            throw "A managed $name process is already running (PID $existingPid). Use .\scripts\stop-local.ps1 first."
        }
        Remove-Item $pidPath
    }
}

Remove-Item (Join-Path $runDir "shutdown.request") -Force -ErrorAction SilentlyContinue
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
        $started += [pscustomobject]@{ Id = $process.Id; Module = $item.Module }
    }
    $settings = & $python -c "from app.core.config import get_settings; s=get_settings(); print(f'{s.api_host}|{s.api_port}')"
    if ($LASTEXITCODE -ne 0) { throw "Could not read API host/port from local configuration." }
    $apiAddress = $settings -split "\|"
    $apiUrl = "http://$($apiAddress[0]):$($apiAddress[1])/openapi.json"
    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        foreach ($process in $started) {
            if (-not (Get-ManagedProcess $process.Id $process.Module)) {
                throw "A process exited during startup. Inspect logs\ for its error details."
            }
        }
        try {
            Invoke-WebRequest -Uri $apiUrl -UseBasicParsing -TimeoutSec 2 | Out-Null
            $ready = $true
            break
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }
    if (-not $ready) { throw "API did not become ready at $apiUrl within 30 seconds. Inspect logs\." }
    $monitor = Start-Process -FilePath (Join-Path $PSHOME "powershell.exe") `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$(Join-Path $root 'scripts\monitor-local.ps1')`"") `
        -WorkingDirectory $root `
        -RedirectStandardOutput (Join-Path $logDir "monitor.out.log") `
        -RedirectStandardError (Join-Path $logDir "monitor.err.log") `
        -WindowStyle Hidden `
        -PassThru
    Set-Content -Path (Join-Path $runDir "monitor.pid") -Value $monitor.Id
    $started += [pscustomobject]@{ Id = $monitor.Id; Module = "monitor-local.ps1" }
    Start-Sleep -Milliseconds 250
    if (-not (Get-ManagedProcess $monitor.Id "monitor-local.ps1")) {
        throw "Process monitor exited during startup. Inspect logs\monitor.err.log."
    }
    Write-Host "Started API, outbox publisher, inbound consumer, and worker simulator."
    Write-Host "Process monitor is active; use .\scripts\stop-local.ps1 to stop the stack."
    Write-Host "API: http://$($apiAddress[0]):$($apiAddress[1])/docs"
    Write-Host "Logs: $logDir"
}
catch {
    Set-Content -Path (Join-Path $runDir "shutdown.request") -Value "startup-failed"
    Start-Sleep -Milliseconds 1000
    foreach ($process in $started) {
        if (Get-ManagedProcess $process.Id $process.Module) {
            Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
        }
    }
    foreach ($name in @("api", "outbox", "consumer", "worker", "monitor")) {
        $pidPath = Join-Path $runDir "$name.pid"
        if (Test-Path $pidPath) { Remove-Item $pidPath }
    }
    throw
}
