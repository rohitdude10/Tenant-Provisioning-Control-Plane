$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root
$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not $env:TEST_DATABASE_URL) {
    throw "Set TEST_DATABASE_URL to a disposable PostgreSQL database whose name ends in _test."
}
if (-not $env:TEST_RABBITMQ_URL) {
    throw "Set TEST_RABBITMQ_URL to a dedicated local RabbitMQ test virtual host."
}
$testDbName = ($env:TEST_DATABASE_URL -split "/")[-1] -split "\?" | Select-Object -First 1
if (-not $testDbName.EndsWith("_test")) {
    throw "TEST_DATABASE_URL must use a database name ending in _test."
}

$env:DATABASE_URL = $env:TEST_DATABASE_URL
$env:RABBITMQ_URL = $env:TEST_RABBITMQ_URL
& $python -m app.cli check
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL/RabbitMQ test dependency check failed." }
& (Join-Path $root ".venv\Scripts\alembic.exe") upgrade head
if ($LASTEXITCODE -ne 0) { throw "Test database migration failed." }

& (Join-Path $root ".venv\Scripts\ruff.exe") format --check app tests alembic
if ($LASTEXITCODE -ne 0) { throw "Ruff format check failed." }
& (Join-Path $root ".venv\Scripts\ruff.exe") check app tests alembic
if ($LASTEXITCODE -ne 0) { throw "Ruff lint check failed." }
& (Join-Path $root ".venv\Scripts\bandit.exe") -q -r app
if ($LASTEXITCODE -ne 0) { throw "Bandit security scan failed." }
& $python -m pytest
if ($LASTEXITCODE -ne 0) { throw "Pytest failed." }
