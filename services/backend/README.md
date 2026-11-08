# Backend database setup

The backend reads the materializer's PostgreSQL result tables. Its notification
and revision queries require migrations 001 through 004:

| Migration | Purpose |
| --- | --- |
| 001 | Symbols, EMA windows, and trade advisories tables |
| 002 | Window and advisory notifications |
| 003 | New-symbol notifications |
| 004 | Row revisions for EMA windows and advisories |

Revision values are returned as strings so JavaScript can compare them exactly.
An advisory's revision includes changes to its joined EMA window. Symbols have
no revision field because each row contains only its identifier. Migration 004
adds revision columns, a shared sequence, and update triggers.

## Apply missing migrations

Start PostgreSQL and prepare a Python environment from the repository root:

```sh
docker compose up -d --wait postgres
python3 -m venv .venv
.venv/bin/python -m pip install -r services/backend/requirements.txt
```

Set `MATERIALIZER_DATABASE_URL` in the environment or repository `.env`. An
exported environment value takes precedence. Then run:

```sh
.venv/bin/python services/backend/migrate.py
```

The helper checks the objects installed by each known migration. It skips
complete migrations, applies missing migrations in order, and stops on a
partially installed migration for manual repair. An advisory lock serializes
concurrent setup runs. All migrations applied during one run share a transaction,
including the final schema check; failures roll back that run's changes.
Existing result rows are preserved. The helper supports migrations 001–004.

Manual SQL application remains supported. Apply only missing `.up.sql` files
from `services/materializer/migrations/`, in ascending order. Roll back with the
matching `.down.sql` files in reverse order while the backend is stopped.
The materializer's `-init-db` flag only initializes migration 001.

`db.check_schema()` verifies that the required notification/update triggers are
enabled and that revision columns are non-null BIGINTs. An incomplete schema
raises `SchemaNotReady` with migration instructions. Revisions order versions
of a result row; they are not a commit-ordered event replay cursor.

## Tests

Use a disposable PostgreSQL test database whose user can create schemas:

```sh
BACKEND_TEST_DATABASE_URL='postgresql://user:password@localhost:15432/test_db' \
  .venv/bin/python -m unittest discover -s services/backend -p 'db_integration_test.py' -v
```

Each integration test creates and removes its own schema. Coverage includes
fresh setup, concurrent/repeated setup, legacy-data upgrades, partial-migration
rollback, readiness checks, exact revisions, and revisions on joined advisories.
The materializer's integration tests also check writes, reset, and migration
rollback with the revision columns installed.

## Backend readiness

Backend startup verifies the database schema before starting its notification
listener. A failed startup closes the pool. The `/api/health` endpoint reports
503 if the schema is incomplete or the notification listener is reconnecting.
A healthy response includes `status: "ok"` and the backend process `pid`.

The listener checks schema readiness, establishes PostgreSQL LISTEN, and then
marks itself ready. On a lost connection it clears readiness and sends SSE
`status` events with `state: "reconnecting"`. After reconnecting, it discards
queued stale records and requests a resync. Clients without symbol subscriptions
still receive symbol announcements and recovery events.

## SSE catch-up and recovery contract

Every connection to `/api/events?symbols=A,B` waits for listener readiness and
requests an authoritative REST snapshot using a `resync` event. Clients should
reload their applicable window, advisory, and signal snapshots on this event.
Listener reconnection and client queue overflow also request resync. Keep-alive
comments maintain idle connections, and disconnected clients are unsubscribed.

An optional `since` timestamp first streams matching history, separately for
windows and advisories, in 5,000-row pages. Each page continues after its last
`(window_start, symbol)` pair, including symbols that share a timestamp. The
stream reads every page before requesting resync and draining live updates.

`since` filters trading-window time. It is not a change cursor and cannot
identify later corrections to older windows or deletions; clients must still
honor `resync`. When an EMA window changes, the listener also refreshes its
joined advisory so clients receive its updated price and EMA values.

Run backend unit tests and all PostgreSQL regression tests with:

```sh
BACKEND_TEST_DATABASE_URL='postgresql://user:password@localhost:15432/test_db' \
  .venv/bin/python -m unittest discover -s services/backend -p '*_test.py' -v
```

PostgreSQL tests cover catch-up beyond 5,000 rows with timestamp ties, live
window/advisory updates, and recovery after terminating the listener connection.
Unit tests cover readiness, cancellation cleanup, queue overflow, and startup
failure cleanup. PostgreSQL tests are skipped when the test URL is unset.
