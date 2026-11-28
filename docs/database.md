# Database

We use PostgreSQL 18, running in Compose with the time zone set to UTC. Its data lives
in the `postgres-data` volume, and it's published only on `127.0.0.1:15432`. The
database name, user and password come from `.env`. They only take effect when the
volume is first created, so changing them later won't change an existing database.

```
make run-postgres
make postgres-shell
```

## Tables

`symbols` has one row per symbol seen in either result topic. `ema_windows` holds the
closing price, EMA 38 and EMA 100 for each symbol and window. `trade_advisories` holds
the action and the previous EMAs, and points at its EMA window for everything else.
Both result tables use `(symbol, window_start)` as their primary key, which is what
lets the materializer upsert.

Prices are `NUMERIC(20,0)`, limited to the `uint64` range. EMAs are plain `NUMERIC`, so
no digits are lost. Window times are `TIMESTAMPTZ`, and a check constraint requires every
window to be exactly five minutes long.

## Migrations

The migrations are plain SQL files in `services/materializer/migrations/`, each with an
`.up.sql` and a `.down.sql`. Migration 001 creates the three tables. Migrations 002 and
003 add the `NOTIFY` triggers the backend listens to. Migration 004 adds a `revision`
column, so clients can tell which version of a row is newer.

You usually don't need to think about migrations, because `make run-flow`,
`make run-backend` and `make migrate-backend` all run `services/backend/migrate.py`.
It checks which migrations are already installed and applies the missing ones in order,
in a single transaction, without losing existing data. If it finds a migration that's
only partly installed, it stops, and you'll have to fix the schema by hand. It takes an
advisory lock first, so two runs at the same time won't conflict.

Note that `materializer -init-db` only creates migration 001.

To apply or roll back a migration by hand, pipe the file into `psql`:

```
docker compose exec -T postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < services/materializer/migrations/001_create_read_models.up.sql
```

Roll back in reverse order, and stop the backend first.
`services/materializer/tests/schema_test.sql` can be run the same way. It checks the
constraints and rolls back its own test data.

## Starting over

`make reset-flow` deletes the whole volume, along with the broker data and Flink
checkpoints. The next `make run-flow` creates everything again. If you only want to
empty the tables, use `materializer -reset-db`.
