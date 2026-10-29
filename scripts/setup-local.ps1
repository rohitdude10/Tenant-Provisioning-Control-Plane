$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $root

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python Launcher (py) was not found. Install Python 3.11 and enable the launcher."
}

$pythonVersion = py -3.11 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ($LASTEXITCODE -ne 0 -or $pythonVersion -ne "3.11") {
    throw "Python 3.11 was not found. Install Python 3.11 and enable the Python Launcher."
}

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    py -3.11 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Could not create the local Python virtual environment." }
}

& ".venv\Scripts\python.exe" -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "Could not upgrade pip in .venv." }
& ".venv\Scripts\python.exe" -m pip install --upgrade "setuptools>=84"
if ($LASTEXITCODE -ne 0) { throw "Could not upgrade setuptools in .venv." }
& ".venv\Scripts\python.exe" -m pip install -e ".[dev]"
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Edit local database and RabbitMQ credentials before startup."
}

Write-Host "Local Python environment is ready. PostgreSQL and RabbitMQ must be installed separately."
