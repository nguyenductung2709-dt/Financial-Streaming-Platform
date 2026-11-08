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
