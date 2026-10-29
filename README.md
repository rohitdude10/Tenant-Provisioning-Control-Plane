# Tenant Provisioning Control Plane

A Python/FastAPI control plane that persists tenant and task state in PostgreSQL and exchanges durable task/progress messages with RabbitMQ. **Phases 1–5 run directly on Windows; no Docker or Compose is used or required.** PostgreSQL and RabbitMQ are explicit local prerequisites.

## Architecture

```text
                         PostgreSQL
                   tenants / tasks / outbox
                    ^                  |
                    |                  | committed outbox rows
           inbound consumer            v
                    |            outbox publisher
                    |                  |
                    +------ RabbitMQ --+
                           ^       |
                    progress       | accepted tasks
                           |       v
                    worker simulator

             FastAPI API <----> PostgreSQL
```

The API, outbox publisher, inbound consumer, and worker simulator are separate Python processes. PostgreSQL is the source of truth. API mutations update the tenant and create its task and outbox row within one transaction. The publisher claims pending rows with PostgreSQL `FOR UPDATE SKIP LOCKED`, keeps the claim locked through RabbitMQ publisher confirmation, and marks a row published only after confirmation. Transient broker failures persist an exponentially increasing retry time in PostgreSQL, capped by `OUTBOX_RETRY_MAX_SECONDS`; a publisher restart therefore preserves pending work and its retry schedule. If it crashes after broker confirmation but before the published marker commits, the event can be delivered again; the task UUID is used as both the event ID and RabbitMQ message ID, and consumers must be idempotent. This is at-least-once delivery, not exactly-once delivery.

The RabbitMQ work and update queues are durable quorum queues:

- `tenant.provisioning.tasks` — accepted task events sent to the worker.
- `tenant.provisioning.updates` — worker progress updates sent to the control plane.
- `tenant.provisioning.tasks.dead` and `tenant.provisioning.updates.dead` — rejected poison messages.

The worker validates each task event, publishes `in_progress` followed by `done` or `failed`, and waits for RabbitMQ publisher confirms for both updates before acknowledging the task. If publishing fails, it requeues the task; its deterministic `task-id:status` update IDs make republished progress idempotent. Malformed task events are rejected to the task dead-letter queue, and already-progressed task events are acknowledged without repeating work.

The inbound consumer commits the task mutation, tenant mutation, and unique `processed_updates` record in one PostgreSQL transaction, then acknowledges the RabbitMQ update. Duplicate update IDs are acknowledged without changing state. Stale updates after a terminal task are recorded and ignored. A terminal result received while its task is still `accepted` is invalid: it is not recorded or applied and is dead-lettered immediately; it must not skip the required `accepted -> in_progress -> terminal` progression. Operators can correct the ordering and replay that update, retaining its original update ID. This is a permanent rejection rather than an automatic requeue, so it cannot loop forever.

Transient database errors are republished with publisher confirms to durable TTL retry queues, then acknowledged from the original queue. Backoff doubles from `INBOUND_RETRY_SECONDS` up to `INBOUND_RETRY_MAX_SECONDS`; after `INBOUND_RETRY_MAX_ATTEMPTS` scheduled retries, a further database failure is dead-lettered. Malformed updates, unknown task IDs, invalid retry headers, and database constraint violations go directly to the dead-letter queue. If RabbitMQ is unavailable while a retry is being scheduled, the original message remains unacknowledged/requeued; the process-level broker reconnect loop backs off while preserving delivery.

## Phase 2 data model and transaction boundaries

The ORM defines `tenants`, `tasks`, `outbox_events`, and `processed_updates`; production schema changes are applied with Alembic (`alembic upgrade head`), not `Base.metadata.create_all()`. Tenant slug format and nonblank names are checked both at request validation and by PostgreSQL constraints. Slug uniqueness is a database unique constraint independent of tenant status. Timestamps are timezone-aware UTC in storage and serialize as ISO 8601 with millisecond precision and a trailing `Z`.

The domain transition tables are centralized in `app/domain.py`. Tasks may transition only `accepted -> in_progress -> done|failed`; terminal states have no outgoing transitions. Tenant transitions follow the assignment state machine.

`SessionLocal` is the configured SQLAlchemy session factory; `transaction_scope()` commits when its block succeeds and rolls back if an exception escapes. Operations that need application-specific handling use an explicit `session.begin()` transaction. Creating a tenant, its deploy task, and its outbox row is one atomic database transaction. PATCH/DELETE state/version changes, the corresponding task, and its outbox row are likewise atomic. Applying a worker update, recording its idempotency key, changing the task, and applying a terminal tenant mutation share one transaction. The outbox publisher’s broker publish and later `published_at` update cannot be one distributed transaction; duplicate delivery is possible and is handled through idempotency. Concurrent publisher instances use `FOR UPDATE SKIP LOCKED` so only one holds and publishes a given pending row at a time. Broker retry starts at `OUTBOX_RETRY_BASE_SECONDS` and doubles up to `OUTBOX_RETRY_MAX_SECONDS`; the attempt count and next attempt time are stored with the outbox row.

## Requirements and implementation choices

**Assignment requirements implemented:** tenant/task state machines; unique tenant slugs including destroyed tenants; optimistic version checks; tenant CRUD and read-only task routes; structured errors; PostgreSQL persistence; post-commit task publishing; idempotent/order-tolerant progress processing; configurable worker simulation; and local setup, tests, and quality checks.

**Implementation choices:** Python 3.11+, FastAPI, SQLAlchemy 2, Alembic, PostgreSQL, RabbitMQ via Pika; JSON error envelope; `POST/PATCH/DELETE` return `202 Accepted`; list endpoints use `limit`/`offset`; PostgreSQL outbox; RabbitMQ quorum queues and dead-letter queues; and PowerShell scripts for Windows setup and process management.

## Windows prerequisites (manual, once per machine)

### PostgreSQL

1. Install PostgreSQL for Windows from [the official PostgreSQL Windows download page](https://www.postgresql.org/download/windows/). Install the server and command-line tools. pgAdmin is optional.
2. Set a local administrator password during installation. Leave the PostgreSQL Windows service enabled and start it from **Services** or the PostgreSQL installer’s service controls.
3. Create an application user and two databases (the second is disposable and used only by tests). In `psql` as a PostgreSQL administrator:

   ```sql
   CREATE USER tenant_app WITH PASSWORD '<choose-a-local-password>';
   CREATE DATABASE tenant_control_plane OWNER tenant_app;
   CREATE DATABASE tenant_control_plane_test OWNER tenant_app;
   ```

### RabbitMQ

1. Install a RabbitMQ-supported Erlang/OTP version, then install RabbitMQ for Windows using [the official Windows installation guide](https://www.rabbitmq.com/docs/install-windows).
2. Start the RabbitMQ Windows service. Service names and installation locations depend on the installed versions.
3. Create an application user and two virtual hosts, then grant permissions. From an elevated PowerShell or Command Prompt with `rabbitmqctl` available (replace the password):

   ```powershell
   rabbitmqctl add_user tenant_app "<choose-a-local-password>"
   rabbitmqctl add_vhost tenant_control_plane
   rabbitmqctl add_vhost tenant_control_plane_test
   rabbitmqctl set_permissions -p tenant_control_plane tenant_app ".*" ".*" ".*"
   rabbitmqctl set_permissions -p tenant_control_plane_test tenant_app ".*" ".*" ".*"
   ```

   The test virtual host keeps test queues separate from development queues. The built-in `guest` account is restricted to localhost; this application uses a dedicated account instead.

These installers, service permissions, database creation, RabbitMQ users/virtual hosts, and firewall/security decisions require machine-specific setup and may need an administrator. The application scripts deliberately do not attempt to install packages, create users, change Windows services, or bypass those permissions. They check connectivity and report actionable failures.

## Python environment and configuration

From the repository root, in PowerShell:

```powershell
.\scripts\setup-local.ps1
```

The script requires Python 3.11 and creates `.venv`, installs the project and development dependencies, and copies `.env.example` to `.env` if needed. Edit `.env` with the local credentials you configured. Keep `.env` private; it is ignored by Git. `.env.example` contains placeholders only.

Example local URLs:

```dotenv
DATABASE_URL=postgresql+psycopg://tenant_app:your-password@localhost:5432/tenant_control_plane
RABBITMQ_URL=amqp://tenant_app:your-password@localhost:5672/tenant_control_plane
TEST_DATABASE_URL=postgresql+psycopg://tenant_app:your-password@localhost:5432/tenant_control_plane_test
TEST_RABBITMQ_URL=amqp://tenant_app:your-password@localhost:5672/tenant_control_plane_test
```

URL-encode any reserved characters in passwords. The application rejects non-PostgreSQL database URLs and does not provide SQLite or in-memory broker fallbacks.

## Start and stop all local processes

After the one-time Windows service and `.env` setup:

```powershell
.\scripts\start-local.ps1
```

This single application boot command verifies PostgreSQL and RabbitMQ connectivity, creates durable RabbitMQ queues, applies Alembic migrations, then starts the API, publisher, inbound consumer, and worker simulator as separate background Python processes. Logs are written to `logs\`; process IDs are tracked in `.run\`. To stop only the processes started by the script:

```powershell
.\scripts\stop-local.ps1
```

The local boot command does not install/start third-party services. If either service is stopped, start it using Windows **Services**, then rerun the application command. OpenAPI documentation is at `http://127.0.0.1:8000/docs` with the default settings. The API process also runs by itself with:

```powershell
.\.venv\Scripts\python.exe -m app.processes.api
```

The other independent processes can be run in separate PowerShell windows:

```powershell
.\.venv\Scripts\python.exe -m app.processes.outbox_publisher
.\.venv\Scripts\python.exe -m app.processes.inbound_consumer
.\.venv\Scripts\python.exe -m app.processes.worker_simulator --fail-rate 0 --min-delay-ms 250 --max-delay-ms 1000
```

Worker options also have `.env` defaults (`WORKER_FAIL_RATE`, `WORKER_MIN_DELAY_MS`, `WORKER_MAX_DELAY_MS`). Restart the worker with `--fail-rate 1` to reproduce failed provisioning; `--fail-rate 0` returns success. Delay values are milliseconds. A task is acknowledged only after both status updates have broker confirms; if a progress publish fails, the task is requeued and may be processed again with the same stable update IDs. Malformed task messages are dead-lettered.

Inbound database retry behavior is configurable with `INBOUND_RETRY_SECONDS`, `INBOUND_RETRY_MAX_SECONDS`, and `INBOUND_RETRY_MAX_ATTEMPTS` in `.env`. Retry queues are durable and TTL-based, so delayed updates survive consumer restarts. Terminal-before-progress messages are dead-lettered immediately rather than retried; stale progress after a terminal task is acknowledged as an idempotent no-op.

## HTTP API

All timestamps are UTC ISO 8601 strings with exactly millisecond precision and a `Z` suffix. List endpoints return `{ "items": [...], "limit": n, "offset": n }`. `limit` defaults to 50 and is capped at 100.

| Method | Path | Behavior |
|---|---|---|
| `POST` | `/tenants` | Create `provisioning` tenant and accepted `deploy` task; returns `202`. |
| `GET` | `/tenants?limit=50&offset=0` | List tenants. |
| `GET` | `/tenants/{tenant_id}` | Get a tenant. |
| `PATCH` | `/tenants/{tenant_id}` | Require `version` and `name`; only an `active` tenant can be updated. Creates an `update` task and returns `202`. |
| `DELETE` | `/tenants/{tenant_id}` | Only `active` or `failed` tenants can be deleted. Creates a `destroy` task and returns `202`. |
| `GET` | `/tasks?limit=50&offset=0` | List tasks. |
| `GET` | `/tasks/{task_id}` | Get a task. |

Example create:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/tenants `
  -ContentType application/json `
  -Body '{"slug":"acme-prod","name":"Acme Production"}'
```

Poll `GET /tenants/{id}` and `GET /tasks` to observe `provisioning` / `accepted`, then `active` / `done`. Once active, PATCH with the current version:

```powershell
Invoke-RestMethod -Method Patch `
  -Uri http://127.0.0.1:8000/tenants/<tenant-id> `
  -ContentType application/json `
  -Body '{"version":2,"name":"Acme Updated"}'
```

The deploy completion increments version from 1 to 2. PATCH requires that current version in its JSON body; accepting the update atomically changes the name, moves the tenant to `updating`, increments its version, and creates the update task and outbox row in the same PostgreSQL transaction. A later successful worker update moves the tenant back to `active`; failure moves it to `failed`.

The expected version format is a required JSON integer field named `version` in the PATCH body (for example `{"version":2,"name":"Acme Updated"}`). Missing, null, non-integer, or less-than-one versions return the structured `validation_error` response with HTTP 422. Stale versions return `tenant_version_conflict` with HTTP 409. This choice keeps the optimistic-lock precondition explicit in the request body rather than introducing an ETag/`If-Match` header.

The API persists the corresponding outbox record in the same database transaction; the tenant API handler does not publish to RabbitMQ. Errors use a stable structured response, for example:

```json
{
  "error": {
    "code": "tenant_version_conflict",
    "message": "Expected version 2, current version is 3"
  }
}
```

Contract codes: `tenant_not_found` (404), `task_not_found` (404), `tenant_already_exists` (409), `tenant_update_not_allowed` (409), and `tenant_version_conflict` (409). Request validation errors return `validation_error` with field details (422). A duplicate slug is enforced by PostgreSQL’s unique constraint, so simultaneous creates cannot both win.

## Reproducible messaging scenarios

**Worker failure:** stop only the managed worker (leave the API, publisher, and consumer running), then restart it separately:

```powershell
$workerPid = [int](Get-Content .run\worker.pid -Raw)
Stop-Process -Id $workerPid
Remove-Item .run\worker.pid
.\.venv\Scripts\python.exe -m app.processes.worker_simulator --fail-rate 1
```

Use the API to create a fresh tenant; its task should finish as failed and its tenant should become failed. Stop that worker and restart with `--fail-rate 0` for successful tasks. For the all-process script, set `WORKER_FAIL_RATE` in `.env` before running `start-local.ps1`.

**Duplicate and out-of-order progress:** create a tenant and copy its task ID from `GET /tasks`. The helper sends a caller-specified stable update ID to the real RabbitMQ updates queue:

```powershell
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id <task-id> --status in_progress --update-id manual-<task-id>-progress
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id <task-id> --status done --update-id manual-<task-id>-done
.\.venv\Scripts\python.exe .\scripts\publish-update.py --task-id <task-id> --status in_progress --update-id manual-<task-id>-late-progress
```

Repeat the second command with the same update ID to demonstrate deduplication. The terminal `done` result is applied, while the later `in_progress` is acknowledged but cannot regress the task or tenant. The helper publishes to RabbitMQ directly, so use a fresh tenant/task or stop the simulator when manually driving that task.

## Tests and quality command

Create the test database and RabbitMQ test virtual host as above, then set `TEST_DATABASE_URL` and `TEST_RABBITMQ_URL` in the current PowerShell session to their connection strings. The database name must end in `_test`. Run the full quality gate:

```powershell
.\scripts\quality.ps1
```

It verifies real PostgreSQL and RabbitMQ connectivity, applies migrations to the designated test database, checks formatting and lint with Ruff, scans the application with Bandit, and runs pytest. Unit tests cover schema, structured API errors, and state transitions. PostgreSQL integration tests exercise concurrent duplicate-slug creation, competing PATCH requests with one version, worker success/failure for deploy/update/destroy, duplicate and out-of-order updates, consumer recovery, bounded database retry, broker failure handling, and outbox publication/retry/restart behavior; they use unique slugs and clean up their created rows. RabbitMQ integration tests confirm persistent message round-trips and durable retry-queue declarations. Without the matching test URL, an integration test is explicitly reported as skipped by pytest. The quality script requires both test services rather than silently skipping service checks.

## Scope note

The assignment’s cold-start example expects all dependencies to start with one command, originally in containers. During local Phases 1–5, this project’s one-command boot starts all **application** processes but requires preinstalled, configured PostgreSQL and RabbitMQ Windows services. Fully automating their installation, Windows service setup, users, permissions, and firewall configuration is not reliable or appropriate without administrator-specific decisions. No Dockerfile, Compose file, Docker dependency, or container startup command is included in this local implementation.
