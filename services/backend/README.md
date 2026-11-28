# Backend

The backend is a Python application built with FastAPI. It reads EMA windows and BUY/SELL 
advisories stored in PostgreSQL by the materializer and pushes live updates to the frontend 
using REST API and Server-Sent Events (SSE).

The backend also serves the [dashboard](../frontend/README.md) from the same origin at http://
localhost:8000.

## Running the backend

The `make run-flow` command starts the full flow, backend included.

If PostgreSQL is already running, you can start just the backend:

```
make run-backend
```

This applies any missing database migrations and starts the backend on http://localhost:8000. 
Set `BACKEND_PORT` to use a different port. 

To apply migrations without starting the backend, run:

```
make migrate-backend 
```

Both commands install the packages from `requirements.txt` into `.venv` the first time they
run. They also read `MATERIALIZER_DATABASE_URL` from the environment or from `.env`.

## REST API

The backend has the following endpoints:

- `GET /api/health` checks whether the database schema is ready and the notification listener 
is connected. Returns the backend's process ID (`pid`) when both checks pass, or HTTP 503 if 
either check fails
- `GET /api/symbols` returns available symbols. Results can be filtered by `q` and `exchange`
- `GET /api/ema-windows?symbol=` returns historical EMA windows for a symbol. Accepts 
optional `start` and `end` parameters to filter by time, and a `limit`
- `GET /api/trade-advisories?symbol=` returns historical buy/sell advisories for a symbol in 
chronological order. Accepts optional `start` and `end` parameters to filter by time, and a 
`limit`
- `GET /api/quotes?symbols=A,B` returns the latest 24 windows for each requested symbol. Used 
by the watchlist
- `GET /api/signals?symbols=A,B` returns recent advisories for the requested symbols. With 
`include_counts=true`, it also returns the total number of requested signals stored in the 
`X-Signal-Total`, `X-Signal-Buy`, and `X-Signal-Sell` headers
- `GET /api/events?symbols=A,B` streams live updates through an SSE connection

Every row includes a `revision` value sent as a string. Higher revision is a newer version of 
the same row. This allows the frontend to compare revisions without losing precision.

## Live updates

The backend uses PostgreSQL notifications to detect changes in the database. Migrations 002 
and 003 add triggers that send a `NOTIFY` whenever a row or symbol changes. Migration 004 
adds the `revision` column that orders row versions.

The backend listens for these changes using PostgreSQL `LISTEN` and forwards the updated rows 
to connected SSE clients. Updates are sent only for the symbols each user follows.

When a new SSE connection is ready, the backend sends a `resync` event. The frontend can then 
reload its data through the REST API and apply incoming live updates in revision order.

The backend also sends another `resync` event when the database connection recovers after an 
outage or when a client falls too far behind.

During a database outage, connected clients receive `status` events with `state: 
"reconnecting"`.

The SSE endpoint also supports an optional `since` parameter, which sends stored historical 
data before live updates begin. It filters results by trading time rather than by when a row 
was changed. This means corrections to older data may be missed, so clients must still handle 
`resync` events.

## Tests

The backend tests can be run with:

```
BACKEND_TEST_DATABASE_URL='postgresql://user:password@localhost:15432/test_db' \
  .venv/bin/python -m unittest discover -s services/backend -p '*_test.py' -v
```

If BACKEND_TEST_DATABASE_URL is not set, database tests are skipped.

Each database test creates its own schema, so the database user must have permission to 
create schemas.

The flow lifecycle tests are located in scripts/ and can be run separately:

```
python3 -m unittest discover -s scripts -p test_flow.py -v
```
