# Backend

The backend is a small FastAPI app on top of the materializer's tables. It serves the
REST API, pushes live updates over Server-Sent Events, and serves the
[dashboard](../frontend/README.md) from the same origin.

`make run-flow` starts it. If PostgreSQL is already running, you can start just the
backend:

```
make run-backend
```

This applies any missing migrations and serves on http://localhost:8000. Set
`BACKEND_PORT` to use a different port. `make migrate-backend` only applies the
migrations. Both commands install `requirements.txt` into `.venv` the first time they
run, and they read `MATERIALIZER_DATABASE_URL` from your environment or from `.env`.

## API

- `GET /api/health` returns the backend's `pid`. It returns 503 while the schema is
  incomplete or the database listener is reconnecting.
- `GET /api/symbols` lists the symbols, filtered by `q` and `exchange`.
- `GET /api/ema-windows?symbol=` and `GET /api/trade-advisories?symbol=` return one
  symbol's history. Both accept `start`, `end` and `limit`.
- `GET /api/quotes?symbols=A,B` returns the last 24 windows of each symbol, for the
  watchlist.
- `GET /api/signals?symbols=A,B` returns recent advisories. With
  `include_counts=true`, it also returns the totals over all stored rows in the
  `X-Signal-Total`, `X-Signal-Buy` and `X-Signal-Sell` headers.
- `GET /api/events?symbols=A,B` is the live SSE stream.

Every row includes a `revision`, sent as a string so that JavaScript can compare it
exactly. A higher revision is a newer version of the same row.

## Live updates

Migrations 002 to 004 add triggers that `NOTIFY` whenever a row changes. The backend
`LISTEN`s for these and forwards the rows to SSE clients.

A new SSE connection gets a `resync` event as soon as the listener is ready. When a
client sees `resync`, it should reload its data over REST and then apply live rows on
top in revision order. The backend sends `resync` again when its database connection
comes back after an outage, or when a client falls too far behind. During the outage
itself, clients get `status` events with `state: "reconnecting"`.

The optional `since` parameter streams stored history first. It filters on trading
time, not on when a row changed, so it can miss corrections to older windows. Clients
still need to handle `resync`.

## Tests

```
BACKEND_TEST_DATABASE_URL='postgresql://user:password@localhost:15432/test_db' \
  .venv/bin/python -m unittest discover -s services/backend -p '*_test.py' -v
```

Without `BACKEND_TEST_DATABASE_URL`, the database tests are skipped. Each test creates
its own schema, so the user needs permission to create schemas. The flow lifecycle
tests live in `scripts/`:

```
python3 -m unittest discover -s scripts -p test_flow.py -v
```
