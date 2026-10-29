$ErrorActionPreference = "Continue"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runDir = Join-Path $root ".run"
$logPath = Join-Path $root "logs\monitor.events.log"
$processModules = @{
    api = "app.processes.api"
    outbox = "app.processes.outbox_publisher"
    consumer = "app.processes.inbound_consumer"
    worker = "app.processes.worker_simulator"
}

function Get-ManagedProcess([int]$processId, [string]$module) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$processId" -ErrorAction SilentlyContinue
    if ($process -and $process.Name -match "^python(w)?\.exe$" -and
        $process.CommandLine -match [regex]::Escape("-m $module")) {
        return $process
    }
    return $null
}

while (-not (Test-Path (Join-Path $runDir "shutdown.request"))) {
    foreach ($name in $processModules.Keys) {
        $pidPath = Join-Path $runDir "$name.pid"
        if (-not (Test-Path $pidPath)) { continue }
        $processId = [int](Get-Content $pidPath -Raw)
        if (-not (Get-ManagedProcess $processId $processModules[$name])) {
            Add-Content -Path $logPath -Value "$(Get-Date -Format o) Managed $name process (PID $processId) exited unexpectedly; requesting stack shutdown."
            Set-Content -Path (Join-Path $runDir "shutdown.request") -Value "process-exited:$name"
            break
        }
    }
    if (-not (Test-Path (Join-Path $runDir "shutdown.request"))) {
        Start-Sleep -Seconds 2
    }
}
