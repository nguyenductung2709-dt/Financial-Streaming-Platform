"""PostgreSQL integration tests for SSE history and reconnect recovery."""

import asyncio
import json
import unittest

from db_integration_test import DatabaseTestCase
import db
import events


class BackendIntegrationTest(DatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        events.listener_ready = asyncio.Event()

    async def test_catchup_pages_past_5000_with_equal_timestamp_symbols(self):
        await self.conn.execute("INSERT INTO symbols VALUES ('A'), ('B'), ('C')")
        await self.conn.execute("""INSERT INTO ema_windows
            (symbol, window_start, window_end, closing_price_micros, ema38, ema100)
            SELECT symbol, t, t + interval '5 minutes', 15000000, 14, 13
            FROM symbols CROSS JOIN generate_series('2021-11-08 00:00Z'::timestamptz,
                '2021-11-08 00:00Z'::timestamptz + 2000 * interval '5 minutes', interval '5 minutes') t""")
        await self.conn.execute("""INSERT INTO trade_advisories
            (symbol, window_start, action, previous_ema38, previous_ema100)
            SELECT symbol, window_start, 'BUY', 12, 13 FROM ema_windows""")
        seen = {"window": [], "advisory": []}
        async for kind, row in db.iter_catchup(["A", "B", "C"], "2021-11-07 00:00Z", page_size=1000):
            seen[kind].append((row["window_start"], row["symbol"]))
        for rows in seen.values():
            self.assertEqual(len(rows), 6003)
            self.assertEqual(len(set(rows)), 6003)
            self.assertEqual(rows, sorted(rows))

    async def next_kind(self, stream, kind):
        async with asyncio.timeout(5):
            while True:
                event = await anext(stream)
                if event.startswith(f"event: {kind}\n"):
                    return json.loads(event.split("data: ", 1)[1])

    async def test_notifications_include_window_corrections(self):
        await self.seed()
        stream = events.stream(["A"], None)
        await anext(stream)
        listener = asyncio.create_task(events.listen_for_notifications())
        try:
            await self.next_kind(stream, "resync")
            await self.conn.execute("UPDATE ema_windows SET closing_price_micros = 25000000 WHERE symbol = 'A'")
            advisory = await self.next_kind(stream, "advisory")
            self.assertEqual(advisory["closing_price"], 25)
        finally:
            await stream.aclose()
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
        self.assertFalse(events.listener_ready.is_set())
        self.assertFalse(events.all_subscribers)
        self.assertFalse(events.subscribers)

    async def test_listener_reconnect_resync_recovers_older_corrections(self):
        await self.seed()
        stream = events.stream(["A"], None)
        await anext(stream)
        listener = asyncio.create_task(events.listen_for_notifications())
        try:
            await self.next_kind(stream, "resync")
            await self.conn.execute("""SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE application_name = %s AND query = 'LISTEN results'""", [self.schema])
            await self.next_kind(stream, "status")
            self.assertFalse(events.listener_ready.is_set())
            await self.conn.execute("UPDATE ema_windows SET closing_price_micros = 35000000 WHERE symbol = 'A'")
            await self.next_kind(stream, "resync")
            self.assertTrue(events.listener_ready.is_set())
            self.assertEqual((await db.get_signals(["A"], 300))[0]["closing_price"], 35)
        finally:
            await stream.aclose()
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
