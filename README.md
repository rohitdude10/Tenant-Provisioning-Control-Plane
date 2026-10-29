# Tenant Provisioning Control Plane

A local-first Python service that provisions tenants asynchronously. FastAPI accepts tenant operations, PostgreSQL stores tenant/task state and an atomic transactional outbox, and RabbitMQ carries task and progress messages. The API, outbox publisher, inbound consumer, and worker simulator run as separate local Python processes.

**Windows is the supported development platform. Docker, Docker Compose, containers, SQLite, and in-memory broker substitutes are not used.** The assignment source in this workspace is `Control_Plane_CANDIDATE_BRIEF (1).md`; there is no separate `ASSIGNMENT.md` file.

## Architecture

```text
 Clients
    |
    v
 FastAPI API --------------------> PostgreSQL
    |                              tenants / tasks
    |                              outbox_events
    |                              processed_updates
    |                                   ^
    |                                   | atomic task, tenant,
    |                                   | idempotency transaction
    v                                   |
 Outbox publisher --> RabbitMQ --> Worker simulator
                         ^               |
                         |               | confirmed progress updates
                         +---------------+
                      inbound consumer
```

### Component responsibilities

- **API**: tenant CRUD and read-only task routes. Each accepted create/update/delete transaction writes the tenant mutation, task, and outbox row together. The HTTP handler does not publish to RabbitMQ.
- **PostgreSQL**: durable source of truth, optimistic tenant versions, state constraints, unique slugs, task records, durable publisher retry schedule, and idempotency keys.
- **Transactional outbox publisher**: claims eligible rows using PostgreSQL `FOR UPDATE SKIP LOCKED`, publishes persistent task events with RabbitMQ publisher confirms, and marks them published only after confirmation. Multiple instances do not concurrently claim a row.
- **RabbitMQ**: durable quorum task/update queues and dead-letter queues. Durable TTL retry queues hold delayed inbound retries.
- **Worker simulator**: independently consumes and validates accepted task events, publishes `in_progress` and then `done` or `failed`, and acknowledges the task only once both progress publications are confirmed.
- **Inbound consumer**: validates updates, applies task/tenant changes and the processed-update record in one database transaction, and acknowledges only after that transaction succeeds.

## State and delivery guarantees

Task transitions are strictly `accepted -> in_progress -> done|failed`. `done` and `failed` are terminal. Tenant transitions are:

```text
provisioning -> active       (deploy done)
provisioning -> failed       (deploy failed)
active       -> updating     (PATCH accepted)
updating     -> active       (update done)
updating     -> failed       (update failed)
active       -> destroying   (DELETE accepted)
failed       -> destroying   (DELETE accepted)
destroying   -> destroyed    (destroy done)
destroying   -> failed       (destroy failed)
```

Important invariants and trade-offs:

- A tenant slug is unique in PostgreSQL even after its tenant reaches `destroyed`. PATCH uses a conditional update on both the expected version and active status, so simultaneous requests with one version cannot both succeed.
- API task/outbox creation and inbound task/tenant/idempotency updates are atomic database transactions. A rollback leaves none of the coupled changes committed.
- Outbound broker publication and the `published_at` database update cannot share a distributed transaction. A crash after RabbitMQ confirmation but before the marker commits can publish the same event again. The task UUID is the event ID and broker message ID; consumers must be idempotent.
- Worker progress IDs are stable and deterministic (`<task-uuid>:in_progress`, followed by `<task-uuid>:done` or `<task-uuid>:failed`). A redelivered task reuses those IDs. Updates with an already-processed ID do not mutate state a second time.
- A terminal update received while the task is still `accepted` is invalid: it is not saved and is dead-lettered immediately. It cannot bypass the required `in_progress` state or enter an infinite retry loop. Correct the ordering and replay it with the same ID. Stale updates after a terminal task are recorded as handled no-ops; terminal state never regresses.
- Malformed payloads, unknown task IDs, and permanent database constraint errors are dead-lettered without stopping a consumer. Transient database failures use confirmed republishing to durable TTL queues with exponential delay: `INBOUND_RETRY_SECONDS` doubles to `INBOUND_RETRY_MAX_SECONDS`, up to `INBOUND_RETRY_MAX_ATTEMPTS`; another failure is dead-lettered. If RabbitMQ is unavailable while scheduling a retry, the original message is requeued. The outbox uses a persisted, bounded exponential retry schedule and keeps unpublished rows after restarts.
- There is no authentication/authorization layer, and the simulator models work rather than calling a real provisioning backend. Configure the local service and bind/API access accordingly. Windows service installation and database/broker account setup remain machine-specific one-time tasks.

## Windows prerequisites (one-time machine setup)

Use 64-bit Windows and **Python 3.11** (the project and full suite were verified with Python 3.11.7). PostgreSQL **14 or newer** is the supported local database target; this workspace's installed service is PostgreSQL 18. RabbitMQ must support durable quorum queues; this workspace's installed broker is RabbitMQ 4.4.0. Use a 64-bit Erlang/OTP version supported by the exact RabbitMQ release. RabbitMQ 4.4.0 supports Erlang/OTP 27.x–29.x according to the [official compatibility matrix](https://www.rabbitmq.com/docs/which-erlang). Do not mix arbitrary Erlang and RabbitMQ versions.

1. **Python**: install Python 3.11 for Windows from [python.org](https://www.python.org/downloads/windows/) and enable the Python Launcher. Verify in PowerShell:

   ```powershell
   py -3.11 --version
   ```

2. **PostgreSQL**: install a supported PostgreSQL Windows release and command-line tools using the [official Windows download page](https://www.postgresql.org/download/windows/). Keep its Windows service enabled/running. Verify:

   ```powershell
   psql --version
   pg_isready -h localhost -p 5432
   ```

3. **RabbitMQ and Erlang/OTP**: follow the [official RabbitMQ Windows installation guide](https://www.rabbitmq.com/docs/install-windows) and its [Erlang compatibility matrix](https://www.rabbitmq.com/docs/which-erlang). Install Erlang as an administrator as the RabbitMQ Windows service requires, then install/start RabbitMQ. Verify from a terminal where RabbitMQ's `sbin` directory is on `PATH`:

   ```powershell
   rabbitmq-diagnostics.bat -q ping
   rabbitmqctl.bat status
   ```

   If the commands are not on `PATH`, run the `.bat` files under the installed RabbitMQ `sbin` directory. The RabbitMQ Windows guide documents Erlang service discovery, CLI authentication, and the Erlang cookie.

4. **Create application identities and resources**. In `psql` as a PostgreSQL administrator, choose a strong local password and create separate development and disposable test databases:

   ```sql
   CREATE USER tenant_app WITH PASSWORD '<choose-a-local-password>';
   CREATE DATABASE tenant_control_plane OWNER tenant_app;
   CREATE DATABASE tenant_control_plane_test OWNER tenant_app;
   ```

   In an elevated PowerShell or Command Prompt with `rabbitmqctl.bat` available, create matching development/test virtual hosts and a dedicated user:

   ```powershell
   rabbitmqctl.bat add_user tenant_app "<choose-a-local-password>"
   rabbitmqctl.bat add_vhost tenant_control_plane
   rabbitmqctl.bat add_vhost tenant_control_plane_test
   rabbitmqctl.bat set_permissions -p tenant_control_plane tenant_app ".*" ".*" ".*"
   rabbitmqctl.bat set_permissions -p tenant_control_plane_test tenant_app ".*" ".*" ".*"
   ```

   PostgreSQL is expected on `localhost:5432` and RabbitMQ AMQP on `localhost:5672` by default. Change the `.env` URLs if your host, port, user, password, database, or vhost differs. URL-encode reserved characters in credentials. The test resources must remain separate from development resources.

Service installation, account/password choice, permissions, service startup, Erlang cookie setup, firewall decisions, and any administrator approval are intentionally not automated. The application scripts never install or reconfigure system services.

## Configuration and local startup

From the repository root in PowerShell, one command bootstraps the Python environment if necessary, preserves an existing `.env`, checks PostgreSQL/RabbitMQ, applies Alembic migrations, starts the four application processes, and waits for API readiness:

```powershell
.\scripts\start-local.ps1
```

On a first clone, the script creates `.venv` and copies `.env.example` to `.env` if missing. It never overwrites `.env`. Configure the local credentials in `.env` (or supply the corresponding environment variables) and run the same startup command again. Python package installation is performed in the project virtual environment; the script does not install PostgreSQL, RabbitMQ, Erlang, or Windows services.

Relevant `.env` entries (the checked-in example contains placeholders only):

```dotenv
DATABASE_URL=postgresql+psycopg://tenant_app:REPLACE_WITH_LOCAL_PASSWORD@localhost:5432/tenant_control_plane
RABBITMQ_URL=amqp://tenant_app:REPLACE_WITH_LOCAL_PASSWORD@localhost:5672/tenant_control_plane
API_HOST=127.0.0.1
API_PORT=8000
LOG_LEVEL=INFO
OUTBOX_POLL_SECONDS=1.0
OUTBOX_RETRY_BASE_SECONDS=1.0
OUTBOX_RETRY_MAX_SECONDS=30.0
INBOUND_RETRY_SECONDS=2.0
INBOUND_RETRY_MAX_SECONDS=30.0
INBOUND_RETRY_MAX_ATTEMPTS=5
WORKER_FAIL_RATE=0.0
WORKER_MIN_DELAY_MS=250
WORKER_MAX_DELAY_MS=1000
TEST_DATABASE_URL=postgresql+psycopg://tenant_app:REPLACE_WITH_LOCAL_PASSWORD@localhost:5432/tenant_control_plane_test
TEST_RABBITMQ_URL=amqp://tenant_app:REPLACE_WITH_LOCAL_PASSWORD@localhost:5672/tenant_control_plane_test
```

`.env` is ignored by Git. Never commit it, real credentials, or credential-bearing URLs. The startup script records per-process PIDs in `.run\` and logs stdout/stderr under `logs\`. A small PowerShell monitor watches the four application process identities and requests a stack shutdown if one exits unexpectedly. Startup validates process identity, detects early exit and API readiness/port failures, and tears down processes started by a failed boot. Shutdown uses a local request so Python processes can close HTTP/RabbitMQ connections and exit; it allows 15 seconds before force-stopping any process that does not respond. This is best-effort graceful shutdown on Windows, not a Windows Service wrapper.

Stop the managed application processes:

```powershell
.\scripts\stop-local.ps1
```

The application commands do not start or stop PostgreSQL/RabbitMQ Windows services. Use **Services** or the service's documented CLI commands to manage those prerequisites. API docs are at `http://127.0.0.1:8000/docs` with the default settings.

### Individual processes for debugging

After setup/configuration and (if needed) `.\.venv\Scripts\alembic.exe upgrade head`, each component can be run in a separate terminal from the project root:

```powershell
.\.venv\Scripts\python.exe -m app.processes.api
.\.venv\Scripts\python.exe -m app.processes.outbox_publisher
.\.venv\Scripts\python.exe -m app.processes.inbound_consumer
.\.venv\Scripts\python.exe -m app.processes.worker_simulator --fail-rate 0 --min-delay-ms 250 --max-delay-ms 1000
```

The simulator accepts `--fail-rate` in `[0,1]`, non-negative `--min-delay-ms`, and `--max-delay-ms` greater than or equal to the minimum. These also default from `.env` as `WORKER_FAIL_RATE`, `WORKER_MIN_DELAY_MS`, and `WORKER_MAX_DELAY_MS`.

## HTTP API

Timestamps are UTC ISO 8601 with millisecond precision and trailing `Z`. Lists return `{ "items": [...], "limit": n, "offset": n }`; limit defaults to 50 and is capped at 100.

| Method | Path | Behavior |
|---|---|---|
| `POST` | `/tenants` | Create a provisioning tenant and accepted deploy task; `202`. |
| `GET` | `/tenants?limit=50&offset=0` | List tenants; `200`. |
| `GET` | `/tenants/{tenant_id}` | Get a tenant; `200` or structured `tenant_not_found`/`404`. |
| `PATCH` | `/tenants/{tenant_id}` | Update an active tenant with optimistic version; `202`. |
| `DELETE` | `/tenants/{tenant_id}` | Start destruction from active/failed; `202`. Tenant row is retained. |
| `GET` | `/tasks?limit=50&offset=0` | List read-only tasks; `200`. |
| `GET` | `/tasks/{task_id}` | Get a task; `200` or `task_not_found`/`404`. |

Create:

```powershell
$tenant = Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/tenants `
  -ContentType application/json `
  -Body '{"slug":"acme-prod","name":"Acme Production"}'
$tenant
```

Example immediate response:

```json
{
  "id": "<tenant-uuid>",
  "slug": "acme-prod",
  "name": "Acme Production",
  "status": "provisioning",
  "version": 1,
  "created_at": "2026-10-29T06:00:00.123Z",
  "updated_at": "2026-10-29T06:00:00.123Z"
}
```

PATCH requires the current integer `version` in the JSON body, for example `{"version":2,"name":"Acme Updated"}`. Missing/invalid versions return `validation_error`/`422`; a stale version returns `tenant_version_conflict`/`409`. Duplicate slugs return `tenant_already_exists`/`409`; invalid lifecycle operations return `tenant_update_not_allowed`/`409`. Errors are structured as `{"error":{"code":"...","message":"..."}}`.

Read a task or list the most recent page:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/tasks/<task-uuid>"
Invoke-RestMethod "http://127.0.0.1:8000/tasks?limit=50&offset=0"
```

An accepted task detail has this shape:

```json
{
  "id": "<task-uuid>",
  "type": "deploy",
  "tenant_id": "<tenant-uuid>",
  "status": "accepted",
  "created_at": "2026-10-29T06:00:00.123Z",
  "updated_at": "2026-10-29T06:00:00.123Z"
}
```

For example, a duplicate slug returns HTTP `409`:

```json
{
  "error": {
    "code": "tenant_already_exists",
    "message": "Tenant slug 'acme-prod' already exists"
  }
}
```

## Reproducible end-to-end scenarios

These are local PowerShell examples. Use fresh slugs for create scenarios. Poll tenant/task detail until the asynchronous status changes.

### Provision, update, and destroy successfully

```powershell
$base = "http://127.0.0.1:8000"
function Wait-TenantStatus($tenantId, $expectedStatus) {
  for ($i=0; $i -lt 60; $i++) {
    $current = Invoke-RestMethod "$base/tenants/$tenantId"
    if ($current.status -eq $expectedStatus) { return $current }
    Start-Sleep -Milliseconds 250
  }
  throw "Tenant $tenantId did not reach $expectedStatus."
}
function Wait-TaskStatus($tenantId, $taskType, $expectedStatus) {
  for ($i=0; $i -lt 60; $i++) {
    $task = (Invoke-RestMethod "$base/tasks?limit=100").items |
      Where-Object { $_.tenant_id -eq $tenantId -and $_.type -eq $taskType } |
      Sort-Object created_at -Descending | Select-Object -First 1
    if ($task -and $task.status -eq $expectedStatus) { return $task }
    Start-Sleep -Milliseconds 250
  }
  throw "$taskType task for tenant $tenantId did not reach $expectedStatus."
}
$slug = "e2e-" + [guid]::NewGuid().ToString("N").Substring(0,10)
$tenant = Invoke-RestMethod -Method Post -Uri "$base/tenants" `
  -ContentType application/json -Body (@{slug=$slug;name="E2E Acme"} | ConvertTo-Json -Compress)
$deploy = Wait-TaskStatus $tenant.id "deploy" "done"
$active = Wait-TenantStatus $tenant.id "active"

$updated = Invoke-RestMethod -Method Patch -Uri "$base/tenants/$($tenant.id)" `
  -ContentType application/json `
  -Body (@{version=$active.version; name="E2E Acme Updated"} | ConvertTo-Json -Compress)
$updateTask = Wait-TaskStatus $tenant.id "update" "done"
$active = Wait-TenantStatus $tenant.id "active"

$destroying = Invoke-RestMethod -Method Delete -Uri "$base/tenants/$($tenant.id)"
$destroyTask = Wait-TaskStatus $tenant.id "destroy" "done"
$destroyed = Wait-TenantStatus $tenant.id "destroyed"
[pscustomobject]@{
  Deploy = $deploy.status
  Update = $updateTask.status
  Destroy = $destroyTask.status
  FinalTenantStatus = $destroyed.status
}
```

The API response to PATCH/DELETE is asynchronous; use tenant/task GET routes to verify final outcomes.

### Reproduce a 100% worker failure

The simplest managed-stack method is to set `WORKER_FAIL_RATE=1.0` in `.env` before startup. To restart only the managed worker with command-line options, stop its exact PID and start a replacement:

```powershell
$workerPid = [int](Get-Content .run\worker.pid -Raw)
Stop-Process -Id $workerPid
Remove-Item .run\worker.pid
$python = (Resolve-Path .\.venv\Scripts\python.exe).Path
$worker = Start-Process -FilePath $python `
  -ArgumentList @("-m","app.processes.worker_simulator","--fail-rate","1","--min-delay-ms","0","--max-delay-ms","0") `
  -WorkingDirectory (Get-Location).Path `
  -RedirectStandardOutput logs\worker.out.log -RedirectStandardError logs\worker.err.log -PassThru
Set-Content .run\worker.pid $worker.Id
```

Create a fresh tenant; its task should reach `failed`, and the tenant should reach `failed`. Restore normal operation by stopping this worker and rerunning `.\scripts\start-local.ps1` after confirming the `.env` fail rate is `0`. The targeted command-line restart above is intentionally explicit; the normal stack startup reads the `.env` defaults.

### Duplicate, stale, and out-of-order worker updates

To drive a task manually, first stop the worker so it does not race the test, create a tenant, and get its deploy task ID. Publish a valid progress update followed by a terminal update, repeat the terminal update with its **same** ID, then send a new stale progress ID:

```powershell
$taskId = "<deploy-task-uuid>"
$progressId = "${taskId}:in_progress"
$doneId = "${taskId}:done"
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id $taskId --status in_progress --update-id $progressId
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id $taskId --status done --update-id $doneId
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id $taskId --status done --update-id $doneId
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id $taskId --status in_progress --update-id "${taskId}:late-progress"
Invoke-RestMethod "http://127.0.0.1:8000/tasks/$taskId"
Invoke-RestMethod "http://127.0.0.1:8000/tenants/<tenant-uuid>"
```

The task remains `done`, the tenant remains `active`, and duplicate ID handling does not increment the tenant version twice. For an invalid early terminal, publish `done` before `in_progress`: the update is dead-lettered with no state change. Then publish `in_progress` and replay that same `done` update ID; it can now be applied. Inspect dead-letter queue depth with `rabbitmqctl.bat list_queues name messages` (run in the correct RabbitMQ node context).

### Concurrent PATCH requests using one expected version

First provision and activate a tenant, then run two requests with the same current version. This PowerShell example uses two background jobs:

```powershell
$url = "http://127.0.0.1:8000/tenants/<tenant-uuid>"
$version = 2
$jobs = @(
  Start-Job -ArgumentList $url,$version,"Concurrent A" -ScriptBlock {
    param($uri,$expectedVersion,$name)
    try {
      Invoke-RestMethod -Method Patch -Uri $uri -ContentType application/json `
        -Body (@{version=[int]$expectedVersion;name=$name} | ConvertTo-Json -Compress) | Out-Null
      "accepted"
    } catch { [int]$_.Exception.Response.StatusCode }
  }
  Start-Job -ArgumentList $url,$version,"Concurrent B" -ScriptBlock {
    param($uri,$expectedVersion,$name)
    try {
      Invoke-RestMethod -Method Patch -Uri $uri -ContentType application/json `
        -Body (@{version=[int]$expectedVersion;name=$name} | ConvertTo-Json -Compress) | Out-Null
      "accepted"
    } catch { [int]$_.Exception.Response.StatusCode }
  }
)
Receive-Job -Job $jobs -Wait -AutoRemoveJob
```

Expect one `accepted` and one `409`. PostgreSQL integration test `test_same_version_patch_has_only_one_winner` exercises the concurrent race directly.

### RabbitMQ outage and recovery

With appropriate Windows service permissions, stop RabbitMQ through **Services** or:

```powershell
Stop-Service -Name RabbitMQ
```

Create a tenant while RabbitMQ is unavailable. The API still commits its tenant/task/outbox transaction; no event is lost. The outbox publisher retains the unpublished row and backs off. Start the RabbitMQ service again:

```powershell
Start-Service -Name RabbitMQ
```

The publisher reconnects and retries the pending event, after which the worker and consumer process the lifecycle. Inspect `logs\outbox.err.log`, `logs\worker.err.log`, and `logs\consumer.err.log` if recovery does not proceed. Do not mark an event published manually.

### Restart and persistence

Save a tenant ID, then stop and restart the application:

```powershell
$tenantId = "<tenant-uuid>"
.\scripts\stop-local.ps1
.\scripts\start-local.ps1
Invoke-RestMethod "http://127.0.0.1:8000/tenants/$tenantId"
```

Tenant/task state remains in PostgreSQL across application restarts. Pending outbox records and inbound retry messages are also durable.

## Tests and quality/security checks

Full automated tests require Python dependencies, a disposable PostgreSQL database whose name ends `_test`, and a dedicated RabbitMQ virtual host whose name ends `_test`. Configure `TEST_DATABASE_URL` and `TEST_RABBITMQ_URL` in `.env` or the current PowerShell environment. The test command never starts the API or worker; HTTP tests use FastAPI's in-process test client, worker/consumer tests call message handlers directly, and integration tests use the real local PostgreSQL/RabbitMQ services. Integration fixtures run Alembic migrations. Tests create unique tenant slugs and clean up database records; RabbitMQ probe queues are exclusive, while declared durable retry/topology queues remain empty in the test vhost.

Run the one-command full quality gate:

```powershell
.\scripts\quality.ps1
```

It loads `.env` values only when the corresponding PowerShell environment variable is unset, requires distinct test resources, verifies service connectivity, migrates the test database, verifies Ruff formatting and lint, runs Bandit static security checks, audits installed dependency versions against the Python advisory database with `pip-audit`, and runs pytest.

Useful focused commands after `.\scripts\setup-local.ps1` and configuration:

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\ruff.exe format --check app tests alembic scripts
.\.venv\Scripts\ruff.exe check app tests alembic scripts
.\.venv\Scripts\bandit.exe -q -r app scripts
.\.venv\Scripts\pip-audit.exe --skip-editable
```

`pip-audit` uses the network advisory service; a network outage can block that check even when application tests work. For PostgreSQL/RabbitMQ failures, the scripts report which test URL or service check failed. Startup diagnostics are in the per-process logs under `logs\`. For migration failures, inspect the Alembic output and verify `DATABASE_URL`/`TEST_DATABASE_URL` point to the intended databases. If port 8000 is occupied, set an unused `API_PORT` in `.env`; startup checks the configured API endpoint and cleans up its own partially started processes.

### Troubleshooting

| Symptom | Checks and recovery |
|---|---|
| PostgreSQL connection refused | Confirm the `postgresql-x64-*` Windows service is running; verify `DATABASE_URL` host/port, then run `pg_isready -h localhost -p 5432`. |
| PostgreSQL authentication/database error | Verify the application role, password, database, and grants using `psql`; URL-encode special password characters in `.env`. |
| RabbitMQ connection refused | Confirm the RabbitMQ Windows service is running and listening on AMQP port 5672; run `rabbitmq-diagnostics.bat -q ping`. |
| RabbitMQ access refused or vhost missing | Verify the exact virtual host, user, permissions, and CLI/service Erlang cookie; use `rabbitmqctl.bat list_permissions -p <vhost>`. |
| Alembic migration fails | Read the migration error printed by startup. Confirm the URL points to the intended PostgreSQL database and the configured role can create/alter schema objects. Do not repair by using `create_all`; migrations are authoritative. |
| API port already occupied | Change `API_PORT` to a free port, then rerun startup. Logs under `logs\api.err.log` show API boot errors. |
| A process exits during startup | Startup checks each process and API readiness, then stops the processes it launched. Inspect the matching `logs\<api|outbox|consumer|worker>.err.log` and verify dependency configuration. An unexpected exit after startup is recorded in `logs\monitor.events.log`, and the monitor requests a clean stack shutdown. |
| `.env` has placeholders after first boot | Edit `.env` with the one-time local database and broker credentials. Startup never overwrites it; rerun `.\scripts\start-local.ps1`. |
| `pip-audit` cannot reach advisory data | Restore HTTPS/network access to PyPI/advisory endpoints and rerun `.\scripts\quality.ps1`; do not interpret an unavailable scan as a pass. |

## Scope and known limitations

The assignment's earlier setup description assumes dependencies can be started by one boot command. This Windows/local phase assumes PostgreSQL, RabbitMQ, and Erlang are already installed, configured, and running; those machine-level operations require administrator and credential choices. The application boot command initializes the Python environment and all application processes but deliberately does not install/start system services. No Dockerfile, Compose file, container setup, or Docker dependency is present.
