$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runDir = Join-Path $root ".run"
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

$knownProcesses = @()
foreach ($name in @("worker", "consumer", "outbox", "api", "monitor")) {
    $pidPath = Join-Path $runDir "$name.pid"
    if (-not (Test-Path $pidPath)) { continue }
    $processId = [int](Get-Content $pidPath -Raw)
    if (Get-ManagedProcess $processId $processModules[$name]) {
        $knownProcesses += [pscustomobject]@{ Name = $name; Id = $processId; Module = $processModules[$name] }
    }
}

if ($knownProcesses.Count -gt 0) {
    Set-Content -Path (Join-Path $runDir "shutdown.request") -Value "user-requested"
    $deadline = (Get-Date).AddSeconds(15)
    do {
        $running = @($knownProcesses | Where-Object { Get-ManagedProcess $_.Id $_.Module })
        if ($running.Count -eq 0) { break }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)

    foreach ($process in $knownProcesses) {
        if (Get-ManagedProcess $process.Id $process.Module) {
            Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
            Write-Warning "Graceful shutdown timed out; force-stopped $($process.Name) (PID $($process.Id))."
        }
        else {
            Write-Host "Stopped $($process.Name) (PID $($process.Id))."
        }
    }
}

foreach ($name in @("worker", "consumer", "outbox", "api", "monitor")) {
    $pidPath = Join-Path $runDir "$name.pid"
    if (Test-Path $pidPath) {
        Remove-Item $pidPath
    }
}
Remove-Item (Join-Path $runDir "shutdown.request") -Force -ErrorAction SilentlyContinue
