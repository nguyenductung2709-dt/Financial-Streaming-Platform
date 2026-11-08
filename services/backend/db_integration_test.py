"""Run with BACKEND_TEST_DATABASE_URL pointing at a disposable test database."""

import asyncio
import os
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

os.environ.setdefault("MATERIALIZER_DATABASE_URL", "postgresql://localhost/backend_test")
import db
import migrate

TEST_DSN = os.getenv("BACKEND_TEST_DATABASE_URL")
MIGRATIONS = Path(__file__).resolve().parents[1] / "materializer/migrations"


@unittest.skipUnless(TEST_DSN, "set BACKEND_TEST_DATABASE_URL for PostgreSQL integration tests")
class DatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "backend_test_" + uuid.uuid4().hex
        self.admin = await psycopg.AsyncConnection.connect(TEST_DSN, autocommit=True)
        await self.admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.addAsyncCleanup(self.cleanup_schema)
        self.dsn = make_conninfo(TEST_DSN, options=f"-c search_path={self.schema} -c timezone=UTC",
                                 application_name=self.schema)
        self.conn = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True, row_factory=dict_row)
        self.addAsyncCleanup(self.conn.close)
        for name in sorted(MIGRATIONS.glob("*.up.sql")):
            await self.conn.execute(name.read_text())
        self.pool = AsyncConnectionPool(self.dsn, open=False, kwargs={"autocommit": True, "row_factory": dict_row})
        await self.pool.open(wait=True)
        self.addAsyncCleanup(self.pool.close)
        self.patch_pool = patch.object(db, "pool", self.pool)
        self.patch_dsn = patch.object(db, "DSN", self.dsn)
        self.patch_pool.start()
        self.patch_dsn.start()
        self.addCleanup(self.patch_pool.stop)
        self.addCleanup(self.patch_dsn.stop)

    async def cleanup_schema(self):
        await self.admin.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        await self.admin.close()

    async def migration(self, filename):
        await self.conn.execute((MIGRATIONS / filename).read_text())

    async def rollback_to(self, version):
        for path in reversed(sorted(MIGRATIONS.glob("*.down.sql"))):
            if int(path.name[:3]) > version:
                await self.conn.execute(path.read_text())

    async def seed(self):
        await self.conn.execute("INSERT INTO symbols VALUES ('A')")
        await self.conn.execute("""INSERT INTO ema_windows
            (symbol, window_start, window_end, closing_price_micros, ema38, ema100)
            VALUES ('A', '2021-11-08 10:00Z', '2021-11-08 10:05Z', 15000000, 14, 13)""")
        await self.conn.execute("""INSERT INTO trade_advisories
            (symbol, window_start, action, previous_ema38, previous_ema100)
            VALUES ('A', '2021-11-08 10:00Z', 'BUY', 12, 13)""")


class DatabaseIntegrationTest(DatabaseTestCase):
    async def test_setup_initializes_fresh_database_and_serializes_repeated_runs(self):
        await self.rollback_to(0)
        results = await asyncio.gather(migrate.apply_migrations(self.dsn), migrate.apply_migrations(self.dsn))
        self.assertEqual(sorted(map(len, results)), [0, 4])
        await self.seed()
        before = await db.get_windows("A", None, None, 1)
        self.assertEqual(await migrate.apply_migrations(self.dsn), [])
        self.assertEqual(await db.get_windows("A", None, None, 1), before)

    async def test_setup_upgrades_legacy_database_without_erasing_results(self):
        await self.rollback_to(1)
        await self.seed()
        applied = await migrate.apply_migrations(self.dsn)
        self.assertEqual(len(applied), 3)
        self.assertEqual((await db.get_signals(["A"], 300))[0]["closing_price"], 15)

    async def test_setup_partial_migration_rolls_back_earlier_changes(self):
        await self.rollback_to(1)
        await self.seed()
        await self.conn.execute("CREATE FUNCTION advance_result_revision() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql")
        with self.assertRaisesRegex(RuntimeError, "partially installed"):
            await migrate.apply_migrations(self.dsn)
        installed = await migrate.markers(self.conn)
        self.assertNotIn("trigger:symbols.symbol_notify", installed)
        self.assertNotIn("trigger:ema_windows.ema_window_notify", installed)
        cursor = await self.conn.execute("SELECT count(*) AS count FROM ema_windows")
        self.assertEqual((await cursor.fetchone())["count"], 1)

    async def test_upgrade_rollback_and_readiness(self):
        await db.check_schema()
        await self.migration("004_result_revisions.down.sql")
        with self.assertRaisesRegex(db.SchemaNotReady, "001–004"):
            await db.check_schema()
        await self.seed()
        await self.migration("004_result_revisions.up.sql")
        await db.check_schema()
        window = await db.get_window_by_key("A", "2021-11-08 10:00Z")
        self.assertEqual(window["closing_price"], 15)
        self.assertTrue(window["revision"].isdigit())
        await self.conn.execute("ALTER TABLE symbols DISABLE TRIGGER symbol_notify")
        with self.assertRaises(db.SchemaNotReady):
            await db.check_schema()

    async def test_old_window_correction_has_new_revision_in_snapshot(self):
        await self.seed()
        before = await db.get_advisory_by_key("A", "2021-11-08 10:00Z")
        await self.conn.execute("UPDATE ema_windows SET closing_price_micros = 25000000 WHERE symbol = 'A'")
        after = (await db.get_signals(["A"], 300))[0]
        self.assertGreater(int(after["revision"]), int(before["revision"]))
        self.assertEqual(after["closing_price"], 25)
        # Row revisions remain exact strings even beyond JavaScript's safe integer.
        await self.conn.execute("SELECT setval('result_revision_seq', 9007199254740992)")
        await self.conn.execute("UPDATE ema_windows SET ema38 = 16 WHERE symbol = 'A'")
        self.assertGreater(int((await db.get_windows("A", None, None, 1))[0]["revision"]), 2**53)


if __name__ == "__main__":
    unittest.main()
