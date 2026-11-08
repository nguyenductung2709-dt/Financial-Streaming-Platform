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
DSN = env_file_value("MATERIALIZER_DATABASE_URL")
if not DSN:
    raise RuntimeError("MATERIALIZER_DATABASE_URL not found in repository .env")

pool = AsyncConnectionPool(DSN, min_size=1, max_size=10, timeout=5, open=False, kwargs={"autocommit": True, "row_factory": dict_row, "options": "-c timezone=UTC"})

#Send the exact price as a string because JavaScript cannot safely represent very large integers
#Also send the price and EMAs as numbers so they can be used in charts
WINDOW_COLS = """symbol, window_start, window_end,
    closing_price_micros::text AS closing_price_micros,
    (closing_price_micros / 1000000)::float8 AS closing_price,
    ema38::float8 AS ema38, ema100::float8 AS ema100"""

ADVISORY_COLS = """a.symbol, a.window_start, w.window_end, a.action,
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
        "ORDER BY a.window_start DESC LIMIT %s", params + [limit])
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


#Get EMA windows and advisories created after the given time, for SSE reconnections
async def get_catchup(symbols, since, limit=5000):
    windows = await fetch(
        f"SELECT {WINDOW_COLS} FROM ema_windows "
        "WHERE symbol = ANY(%s) AND window_start > %s ORDER BY window_start LIMIT %s",
        [symbols, since, limit])
    advisories = await fetch(
        f"SELECT {ADVISORY_COLS} FROM {ADVISORY_FROM} "
        "WHERE a.symbol = ANY(%s) AND a.window_start > %s ORDER BY a.window_start LIMIT %s",
        [symbols, since, limit])
    return [shape(r) for r in windows], [shape(r) for r in advisories]