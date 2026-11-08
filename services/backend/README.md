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
