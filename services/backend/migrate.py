"""Apply missing known migrations, preserving complete manual installations."""

import asyncio
from pathlib import Path
import sys

import psycopg

import db

MIGRATIONS = Path(__file__).resolve().parents[1] / "materializer/migrations"


async def markers(conn):
    """Inspect objects instead of blindly replaying manually applied SQL."""
    cursor = await conn.execute("""SELECT 'table:' || table_name FROM information_schema.tables
        WHERE table_schema = current_schema()
        UNION ALL SELECT 'column:' || table_name || '.' || column_name
        FROM information_schema.columns WHERE table_schema = current_schema()
        UNION ALL SELECT 'trigger:' || c.relname || '.' || t.tgname
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema() AND NOT t.tgisinternal
        UNION ALL SELECT 'function:' || p.proname FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = current_schema()
        UNION ALL SELECT 'sequence:' || sequencename FROM pg_sequences
        WHERE schemaname = current_schema()""")
    return {row[0] if not isinstance(row, dict) else next(iter(row.values()))
            for row in await cursor.fetchall()}


EXPECTED = {
    "001_create_read_models": {f"table:{name}" for name in ("symbols", "ema_windows", "trade_advisories")},
    "002_create_notifications": {
        "trigger:ema_windows.ema_window_notify", "trigger:trade_advisories.trade_advisory_notify"},
    "003_notify_new_symbols": {"trigger:symbols.symbol_notify"},
    "004_result_revisions": {
        "column:ema_windows.revision", "column:trade_advisories.revision",
        "sequence:result_revision_seq", "function:advance_result_revision",
        "trigger:ema_windows.ema_window_revision", "trigger:trade_advisories.trade_advisory_revision",
    },
}


async def apply_migrations(dsn=None):
    applied = []
    async with await psycopg.AsyncConnection.connect(dsn or db.DSN, autocommit=True) as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL lock_timeout = '10s'")
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext('financial-streaming-migrations'))")
            for name, required in EXPECTED.items():
                present = required & await markers(conn)
                if present == required:
                    continue
                if present:
                    raise RuntimeError(f"Migration {name} is partially installed; repair it before starting. No migrations were committed.")
                sql = (MIGRATIONS / f"{name}.up.sql").read_text().strip()
                if not sql.startswith("BEGIN;") or not sql.endswith("COMMIT;"):
                    raise RuntimeError(f"Unexpected transaction wrapper in {name}")
                await conn.execute(sql[len("BEGIN;"):-len("COMMIT;")])
                applied.append(name)
            # Reuse the same connection and transaction for readiness validation.
            async def fetch(sql, params=None):
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                    await cursor.execute(sql, params)
                    return await cursor.fetchall()

            # check_schema accepts an explicit query function for transactional setup.
            await db.check_schema(query=fetch)
    return applied


if __name__ == "__main__":
    try:
        applied = asyncio.run(apply_migrations())
        print("Applied migrations: " + ", ".join(applied) if applied else "Database migrations are current.")
    except (RuntimeError, psycopg.Error, OSError) as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        sys.exit(1)
