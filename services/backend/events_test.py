import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("MATERIALIZER_DATABASE_URL", "postgresql://localhost/backend_test")
import db
import events
import main


class EventsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        events.listener_ready = asyncio.Event()
        events.listener_ready.set()
        events.subscribers.clear()
        events.all_subscribers.clear()

    async def asyncTearDown(self):
        events.listener_ready.clear()
        self.assertFalse(events.subscribers)
        self.assertFalse(events.all_subscribers)

    async def test_empty_subscription_receives_reset_and_closes_cleanly(self):
        stream = events.stream([], None)
        await anext(stream)
        self.assertIn("event: resync", await anext(stream))
        events.broadcast_resync()
        self.assertIn("event: resync", await anext(stream))
        await stream.aclose()

    async def test_overflow_and_reset_discard_obsolete_queued_rows(self):
        stream = events.stream(["A"], None)
        await anext(stream)
        await anext(stream)
        queue = next(iter(events.all_subscribers))
        for i in range(events.QUEUE_SIZE + 1):
            events.offer(queue, {"kind": "window", "symbol": "A", "revision": str(i)})
        self.assertEqual(queue.qsize(), 1)
        self.assertIn("event: resync", await anext(stream))
        events.dispatch_msg({"kind": "window", "symbol": "A"})
        events.broadcast_resync()
        self.assertEqual(queue.qsize(), 1)
        self.assertIn("event: resync", await anext(stream))
        await stream.aclose()

    async def test_history_is_followed_by_authoritative_resync(self):
        async def history(symbols, since):
            for index in range(5001):
                yield "window", {"symbol": "A", "revision": str(index)}

        with patch.object(db, "iter_catchup", history):
            stream = events.stream(["A"], "2021-11-08T00:00Z")
            await anext(stream)
            for _ in range(5001):
                self.assertIn("event: window", await anext(stream))
            self.assertIn("event: resync", await anext(stream))
            await stream.aclose()

    async def test_resync_while_listener_is_down_does_not_claim_readiness(self):
        stream = events.stream(["A"], None)
        await anext(stream)
        await anext(stream)
        events.listener_ready.clear()
        events.broadcast_resync()
        self.assertIn("event: status", await anext(stream))
        events.listener_ready.set()
        events.broadcast_resync()
        self.assertIn("event: resync", await anext(stream))
        await stream.aclose()

    async def test_stream_waits_for_listener_readiness(self):
        events.listener_ready.clear()
        stream = events.stream(["A"], None)
        await anext(stream)
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        self.assertFalse(pending.done())
        events.listener_ready.set()
        self.assertIn("event: resync", await pending)
        await stream.aclose()

    async def test_cancelled_stream_releases_subscription(self):
        stream = events.stream(["A"], None)
        await anext(stream)
        await anext(stream)
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending

    async def test_health_reports_missing_migration(self):
        with patch.object(db, "check_schema", AsyncMock(side_effect=db.SchemaNotReady("apply 004"))):
            response = await main.health()
        self.assertEqual(response.status_code, 503)
        self.assertIn(b"apply 004", response.body)

    async def test_quotes_only_request_the_explicit_deduplicated_symbols(self):
        with patch.object(db, "get_quotes", AsyncMock(return_value=[])) as get_quotes:
            self.assertEqual(await main.quotes(" A, B, A, , "), [])
        get_quotes.assert_awaited_once_with(["A", "B"])

    async def test_empty_quotes_do_not_query_all_market_history(self):
        with patch.object(db, "fetch", AsyncMock()) as fetch:
            self.assertEqual(await db.get_quotes([]), [])
        fetch.assert_not_awaited()

    async def test_signal_counts_are_exposed_without_changing_the_array_response(self):
        items = [{"symbol": "A", "action": "BUY"}]
        snapshot = {"items": items, "counts": {"total": 1234, "buy": 800, "sell": 434}}
        response = main.Response()
        with patch.object(db, "get_signal_snapshot", AsyncMock(return_value=snapshot)) as get_snapshot:
            result = await main.signals(response, " A, B, A ", 100, True)
        self.assertEqual(result, items)
        get_snapshot.assert_awaited_once_with(["A", "B"], 100)
        self.assertEqual(response.headers["X-Signal-Total"], "1234")
        self.assertEqual(response.headers["X-Signal-Buy"], "800")
        self.assertEqual(response.headers["X-Signal-Sell"], "434")
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    async def test_signal_counts_are_optional_for_existing_clients(self):
        response = main.Response()
        with patch.object(db, "get_signals", AsyncMock(return_value=[])) as get_signals:
            result = await main.signals(response, "A", 100, False)
        self.assertEqual(result, [])
        get_signals.assert_awaited_once_with(["A"], 100)
        self.assertNotIn("X-Signal-Total", response.headers)

    async def test_health_requires_notification_listener(self):
        events.listener_ready.clear()
        with patch.object(db, "check_schema", AsyncMock()):
            self.assertEqual((await main.health()).status_code, 503)
            events.listener_ready.set()
            self.assertEqual((await main.health())["status"], "ok")

    async def test_startup_schema_failure_closes_pool(self):
        pool = AsyncMock()
        with patch.object(db, "pool", pool), patch.object(
            db, "check_schema", AsyncMock(side_effect=db.SchemaNotReady("apply 004"))
        ):
            with self.assertRaisesRegex(db.SchemaNotReady, "apply 004"):
                async with main.lifespan(main.app):
                    self.fail("startup must fail before serving requests")
        pool.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
