import os
from datetime import datetime, timezone
from pathlib import Path
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


#Read a value from the repository's .env file
def env_file_value(name):
    env = Path(__file__).resolve().parents[2] / ".env"

    if not env.is_file():
        return None

    for line in env.read_text().splitlines():
        line = line.strip()
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip().strip("'\"")

    return None


#Read the database connection string from the repository's .env
DSN = os.getenv("MATERIALIZER_DATABASE_URL") or env_file_value("MATERIALIZER_DATABASE_URL")
if not DSN:
    raise RuntimeError("Set MATERIALIZER_DATABASE_URL in the environment or repository .env")

pool = AsyncConnectionPool(DSN, min_size=1, max_size=10, timeout=5, open=False, kwargs={"autocommit": True, "row_factory": dict_row, "options": "-c timezone=UTC"})

#Send the exact price as a string because JavaScript cannot safely represent very large integers
#Also send the price and EMAs as numbers so they can be used in charts
WINDOW_COLS = """symbol, window_start, window_end, revision::text AS revision,
    closing_price_micros::text AS closing_price_micros,
    (closing_price_micros / 1000000)::float8 AS closing_price,
    ema38::float8 AS ema38, ema100::float8 AS ema100"""

ADVISORY_COLS = """a.symbol, a.window_start, w.window_end, a.action,
    greatest(a.revision, w.revision)::text AS revision,
    a.previous_ema38::float8 AS previous_ema38,
    a.previous_ema100::float8 AS previous_ema100,
    w.ema38::float8 AS ema38, w.ema100::float8 AS ema100,
    (w.closing_price_micros / 1000000)::float8 AS closing_price"""

ADVISORY_FROM = """trade_advisories a
    JOIN ema_windows w ON w.symbol = a.symbol AND w.window_start = a.window_start"""


#Convert a datetime to UTC
def as_utc(dt):
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


#Format a datetime as an ISO timestamp in UTC
def iso(dt):
    return as_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


#Convert datetime values in a database row to ISO timestamps
def shape(row):
    return {k: (iso(v) if isinstance(v, datetime) else v) for k, v in row.items()}


#Run a database query and return its rows
async def fetch(sql, params=None):
    async with pool.connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


class SchemaNotReady(RuntimeError):
    """The database objects required by the backend are missing or disabled."""


async def check_schema(query=None):
    query = query or fetch
    required = {
        ("symbols", "symbol_notify"),
        ("ema_windows", "ema_window_notify"),
        ("trade_advisories", "trade_advisory_notify"),
        ("ema_windows", "ema_window_revision"),
        ("trade_advisories", "trade_advisory_revision"),
    }
    rows = await query("""SELECT c.relname AS table_name, t.tgname AS trigger_name
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema() AND NOT t.tgisinternal
          AND t.tgenabled IN ('O', 'A')""")
    installed = {(r["table_name"], r["trigger_name"]) for r in rows}
    columns = await query("""SELECT table_name FROM information_schema.columns
        WHERE table_schema = current_schema() AND column_name = 'revision'
          AND data_type = 'bigint' AND is_nullable = 'NO'
          AND table_name IN ('ema_windows', 'trade_advisories')""")
    if required - installed or {r["table_name"] for r in columns} != {"ema_windows", "trade_advisories"}:
        raise SchemaNotReady(
            "Backend schema is incomplete: apply materializer migrations 001–004 in order. "
            "See services/backend/README.md; existing databases must apply only missing migrations.")


#Add start and end time conditions to a query
def time_filter(column, start, end, where, params):
    if start:
        where.append(f"{column} >= %s")
        params.append(start)
    if end:
        where.append(f"{column} < %s")
        params.append(end)


#Build the WHERE clause from the given conditions
def where_clause(where):
    return (" WHERE " + " AND ".join(where)) if where else ""


#Get available symbols, with optional search and exchange filters
async def get_symbols(q, exchange, limit):
    where, params = [], []
    if q:
        where.append("position(upper(%s) in upper(symbol)) > 0")
        params.append(q)
    if exchange:
        suffix = "." + exchange.upper()
        where.append("right(upper(symbol), %s) = %s")
        params += [len(suffix), suffix]
    rows = await fetch(f"SELECT symbol FROM symbols{where_clause(where)} ORDER BY symbol LIMIT %s", params + [limit])
    return [{"symbol": r["symbol"], "exchange": r["symbol"].rpartition(".")[2] if "." in r["symbol"] else None} for r in rows]


#Get EMA windows from oldest to newest, keeping the newest rows if the limit is reached
async def get_windows(symbol, start, end, limit):
    where, params = ["symbol = %s"], [symbol]
    time_filter("window_start", start, end, where, params)
    rows = await fetch(
        f"SELECT {WINDOW_COLS} FROM ema_windows{where_clause(where)} "
        "ORDER BY window_start DESC LIMIT %s", params + [limit])
    return [shape(r) for r in reversed(rows)]


#Get trade advisories from newest to oldest, including the related price and EMAs
async def get_advisories(symbols, start, end, limit):
    where, params = [], []
    if symbols:
        where.append("a.symbol = ANY(%s)")
        params.append(symbols)
    time_filter("a.window_start", start, end, where, params)
    rows = await fetch(
        f"SELECT {ADVISORY_COLS} FROM {ADVISORY_FROM}{where_clause(where)} "
        "ORDER BY a.window_start DESC, a.symbol LIMIT %s", params + [limit])
    return [shape(r) for r in rows]


#Get one EMA window using its symbol and start time
async def get_window_by_key(symbol, window_start):
    rows = await fetch(
        f"SELECT {WINDOW_COLS} FROM ema_windows "
        "WHERE symbol = %s AND window_start = %s",
        [symbol, window_start],
    )
    return shape(rows[0]) if rows else None


#Get one trade advisory using its symbol and start time
async def get_advisory_by_key(symbol, window_start):
    rows = await fetch(
        f"SELECT {ADVISORY_COLS} FROM {ADVISORY_FROM} "
        "WHERE a.symbol = %s AND a.window_start = %s",
        [symbol, window_start],
    )
    return shape(rows[0]) if rows else None


#Get the latest trade signals for the given symbols
async def get_signals(symbols, limit):
    return await get_advisories(symbols, None, None, limit)


# Stream history in deterministic pages, including ties across symbols.
# `since` filters trading-window time; it is not a cursor for row corrections.
async def iter_catchup(symbols, since, page_size=5000):
    if page_size < 1:
        raise ValueError("page_size must be positive")
    if not symbols:
        return
    for kind, cols, source, prefix in (
        ("window", WINDOW_COLS, "ema_windows", ""),
        ("advisory", ADVISORY_COLS, ADVISORY_FROM, "a."),
    ):
        cursor = None
        while True:
            params = [symbols, since]
            after = ""
            if cursor is not None:
                after = f" AND ({prefix}window_start, {prefix}symbol) > (%s, %s)"
                params.extend(cursor)
            rows = await fetch(
                f"SELECT {cols} FROM {source} WHERE {prefix}symbol = ANY(%s) "
                f"AND {prefix}window_start > %s{after} "
                f"ORDER BY {prefix}window_start, {prefix}symbol LIMIT %s",
                params + [page_size])
            for row in rows:
                yield kind, shape(row)
            if len(rows) < page_size:
                break
            cursor = (rows[-1]["window_start"], rows[-1]["symbol"])
