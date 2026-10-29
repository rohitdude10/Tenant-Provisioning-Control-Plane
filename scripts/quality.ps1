$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root
$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Python environment is missing. Run .\scripts\setup-local.ps1 or .\scripts\start-local.ps1 first."
}

if (Test-Path ".env") {
    foreach ($line in Get-Content ".env") {
        if ($line -match '^\s*([^#=]+?)\s*=\s*(.*?)\s*$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim().Trim('"').Trim("'")
            if (-not [Environment]::GetEnvironmentVariable($key, "Process")) {
                [Environment]::SetEnvironmentVariable($key, $value, "Process")
            }
        }
    }
}

if (-not $env:TEST_DATABASE_URL) {
    throw "Set TEST_DATABASE_URL in .env or this PowerShell session to a disposable PostgreSQL database ending in _test."
}
$testUri = $null
if (-not [uri]::TryCreate($env:TEST_DATABASE_URL, [UriKind]::Absolute, [ref]$testUri)) {
    throw "TEST_DATABASE_URL must be an absolute PostgreSQL URL."
}
$testDbName = [uri]::UnescapeDataString($testUri.AbsolutePath.Trim("/"))
if (-not $testDbName.EndsWith("_test")) {
    throw "TEST_DATABASE_URL must use a database name ending in _test."
}
if ($env:DATABASE_URL -and $env:TEST_DATABASE_URL -eq $env:DATABASE_URL) {
    throw "TEST_DATABASE_URL must not equal DATABASE_URL."
}
if (-not $env:TEST_RABBITMQ_URL) {
    throw "Set TEST_RABBITMQ_URL in .env or this PowerShell session to a dedicated local RabbitMQ test virtual host."
}
$testRabbitUri = $null
if (-not [uri]::TryCreate($env:TEST_RABBITMQ_URL, [UriKind]::Absolute, [ref]$testRabbitUri)) {
    throw "TEST_RABBITMQ_URL must be an absolute AMQP URL."
}
$testVhost = [uri]::UnescapeDataString($testRabbitUri.AbsolutePath.Trim("/"))
if (-not $testVhost.EndsWith("_test")) {
    throw "TEST_RABBITMQ_URL must use a virtual host whose name ends in _test."
}
if ($env:RABBITMQ_URL -and $env:TEST_RABBITMQ_URL -eq $env:RABBITMQ_URL) {
    throw "TEST_RABBITMQ_URL must not equal RABBITMQ_URL."
}

$env:DATABASE_URL = $env:TEST_DATABASE_URL
$env:RABBITMQ_URL = $env:TEST_RABBITMQ_URL
& $python -m app.cli check
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL/RabbitMQ test dependency check failed." }
& (Join-Path $root ".venv\Scripts\alembic.exe") upgrade head
if ($LASTEXITCODE -ne 0) { throw "Test database migration failed." }

& (Join-Path $root ".venv\Scripts\ruff.exe") format --check app tests alembic scripts
if ($LASTEXITCODE -ne 0) { throw "Ruff format check failed." }
& (Join-Path $root ".venv\Scripts\ruff.exe") check app tests alembic scripts
if ($LASTEXITCODE -ne 0) { throw "Ruff lint check failed." }
& (Join-Path $root ".venv\Scripts\bandit.exe") -q -r app scripts
if ($LASTEXITCODE -ne 0) { throw "Bandit security scan failed." }
& (Join-Path $root ".venv\Scripts\pip-audit.exe") --skip-editable
if ($LASTEXITCODE -ne 0) { throw "Python dependency vulnerability audit failed." }
& $python -m pytest
if ($LASTEXITCODE -ne 0) { throw "Pytest failed." }
