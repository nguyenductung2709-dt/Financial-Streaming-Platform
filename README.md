# Financial-Streaming-Platform

The [backend and dashboard setup](services/backend/README.md) explains how to
apply the live-notification migrations and serve the dashboard on port 8000.

## Development setup

After cloning, activate the git hooks once:

```bash
make setup-hooks
```

The pre-commit hook formats staged service files:

- `services/data-producer`: `gofmt -w` and `go vet`
- `services/materializer`: `gofmt -w` and recursive `go vet`
- `services/streaming-processor`: `scalafmtAll` and `scalafmtSbt` (slow: JVM startup)

Formatting runs on a snapshot of the staged files. Formatting changes are copied
back and re-staged only for fully staged files. If a file needs formatting and
also has unstaged changes, the hook stops without changing your files or staging
area. Run `make formatall`, review, and stage the intended changes before retrying.
Already formatted staged files can still have unstaged edits.

If a toolchain is missing, the hook skips that check with a warning —
CI still enforces the full format and lint suite.

## Run the complete local flow

Copy `.env.example` to `.env` once, and check the `POSTGRES_*` and
`MATERIALIZER_*` settings. Put the CSV dataset under `dataset/`, then run:

```sh
make run-flow
```

This builds the Go producer and materializer, prepares the backend's `.venv`,
starts PostgreSQL, Redpanda, the Redpanda console, and Flink, creates the topics,
applies missing database migrations, submits the processor, and supervises the
producer, materializer, and backend together. The dashboard is available at
<http://localhost:8000>; publication starts only after its backend is healthy.
Startup preserves stored results and complete existing migrations. A partially
installed migration causes startup to stop for manual repair.

For a limited producer run:

```sh
make run-flow PRODUCER_ARGS="-rate=1000 -maxEvents=100000"
```

The materializer and dashboard keep running after the producer finishes so they
can consume and display remaining and future results. Ctrl+C stops all managed
local processes.
Containers and the Flink job remain running. Before a fresh run, use:

```sh
make reset-flow
make run-flow
```

`reset-flow` stops the managed producer, materializer, and backend, removes the
Compose containers, and permanently deletes this project's local PostgreSQL,
broker data (including topics and consumer offsets), and Flink checkpoints.
The next `make run-flow` recreates the infrastructure and schema. Stop separately
launched producers and materializers before resetting. Reset works without a
`.env` and does not build or start services first.

To stop local processes and all Compose containers while keeping data:

```sh
make stop-flow
```

Use `BACKEND_PORT=8001` to change the dashboard port, or `BACKEND_VENV=/path/to/venv`
to choose its Python environment. `make run-backend` applies missing migrations
and starts only the dashboard when PostgreSQL is already available.

`run-flow` and the backend commands load `.env`; explicit `MATERIALIZER_*`
environment or Make command-line overrides take precedence. The complete flow uses local Compose
infrastructure, so keep the materializer URL and broker addresses pointed at
those services.

`validate-flow` uses its own internal data reset so containers and checkpoints
stay available during automated validation.

Run the flow lifecycle tests with:

```sh
python3 -m unittest discover -s scripts -p 'test_flow.py' -v
```

## Local PostgreSQL

Start the database and open its SQL shell from the repository root:

```sh
make run-postgres
make postgres-shell
```

In `psql`, use `\conninfo` to inspect the connection and `\q` to exit.
The Compose defaults are for local development:

| Setting | Default |
| --- | --- |
| Database | `financial_streaming` |
| User | `financial` |
| Password | `financial_dev` |
| Address from the host | `localhost:15432` |
| Address from another Compose service | `postgres:5432` |

For a host application or database GUI, the default connection URL is:

```text
postgresql://financial:financial_dev@localhost:15432/financial_streaming?sslmode=disable
```

Optionally copy `.env.example` to `.env` and edit the values before the first
startup. Compose reads `.env` automatically, and Git ignores it. Database name,
user, and password initialize a new database volume; changing these variables
later does not update an existing database's credentials. `POSTGRES_PORT` only
changes the port published on the host.

PostgreSQL 18 stores its files in the persistent `postgres-data` named volume,
mounted at `/var/lib/postgresql` as required by the official image. The server
uses UTC, and its published port is restricted to the local host. To stop just
the database while retaining its data, run `docker compose stop postgres`.
See the [official PostgreSQL image documentation](https://hub.docker.com/_/postgres)
for initialization and storage behavior.

Apply the [materializer's initial migration](services/materializer/README.md)
to create the symbols, EMA windows, and trade advisories tables.

## CI checks

Pushes and pull requests run checks based on the changed paths:

| Changes | Checks |
| --- | --- |
| Backend/frontend code (except Markdown) or materializer migrations | Dashboard CI: backend unit/PostgreSQL integration tests, frontend regression tests, and flow lifecycle/Make recipe tests |
| Flow startup/stop/reset scripts and their tests, including `scripts/csv_make_test.py` | Dashboard CI |
| `services/data-producer/**` (except Markdown) | Producer formatting, lint, tests, and build |
| `services/materializer/**` (except Markdown) | Materializer formatting, lint, race tests, PostgreSQL integration tests, and build |
| `services/streaming-processor/**` (except Markdown) | Processor formatting, lint, tests, and assembly verification |
| `schemas/**/*.proto` | Producer, materializer, processor, and Protobuf generation |
| `gen/go/**` | Producer, materializer, and Protobuf generation |
| `gen/java/**` | Processor and Protobuf generation |
| Other files under `gen/**` | Protobuf generation |
| `go.mod`, `go.sum` | Producer, materializer, and Protobuf generation |
| `Makefile` | Producer, materializer, processor, Protobuf generation, and dashboard checks |
| `.githooks/pre-commit` | Producer and materializer checks |
| `compose.yaml` | Infrastructure Compose configuration validation |
| A file in `.github/workflows/` | The workflow defined by that file |

Documentation-only changes do not trigger these checks. The flow scripts listed
above trigger Dashboard CI; other local scripts do not trigger these checks.
Changes spanning multiple areas run all matching workflows.
The dashboard, producer, materializer, and infrastructure workflows also support manual runs with
`workflow_dispatch`.

Dashboard CI uses PostgreSQL 18, Python 3.12, and Node.js 22. Database tests use
disposable schemas; lifecycle tests use local service fixtures. These checks do
not start the full Docker stack or exercise the dashboard in a browser.

Materializer CI uses a PostgreSQL 18 service container for the database integration
tests. Each test creates and removes its own schema. Unit tests run separately
from the database integration tests, and coverage is uploaded as an artifact.
Locally, use `make test-materializer`, `make format-materializer`, and
`MATERIALIZER_TEST_DATABASE_URL='<PostgreSQL URL>' make test-materializer-integration`.

Infrastructure CI runs `docker compose -f compose.yaml config --quiet` to validate
the Compose configuration without starting containers. This checks configuration
validity; it does not verify image availability or service startup. Run the same
command locally before pushing Compose changes.

GitHub leaves checks pending when an entire workflow is skipped by path filters.
If branch protection requires these checks, use job-level change detection instead
of workflow-level path filters before making them required. See
[GitHub's path filtering documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow#using-filters-to-target-specific-paths-for-pull-request-or-push-events).
