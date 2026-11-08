import asyncio
import json
import logging
import psycopg
import db

log = logging.getLogger("events")

CHANNEL = "results"
QUEUE_SIZE = 2000
KEEPALIVE_SECONDS = 15

#Store connected clients by symbol
subscribers = {}  # symbol -> set of asyncio.Queue
all_subscribers = set()
listener_ready = asyncio.Event()

#Add a message to a client's queue, requesting a resync if it is full
def offer(queue, msg):
    try:
        queue.put_nowait(msg)
    except asyncio.QueueFull:
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait({"kind": "resync"})


#Send updates to clients following the symbol, or all clients for new symbols
def dispatch_msg(msg):
    queues = set(subscribers.get(msg.get("symbol"), ()))

    if msg.get("kind") in {"symbol", "resync", "status"}:
        queues.update(all_subscribers)

    for queue in queues:
        offer(queue, msg)


#Tell all connected clients to reload data after a listener reconnects
def broadcast_resync():
    # Discard queued rows before clients reload authoritative snapshots.
    for queue in all_subscribers:
        while not queue.empty():
            queue.get_nowait()
        offer(queue, {"kind": "resync"})


#Listen for PostgreSQL notifications and retrieve the full updated rows
async def listen_for_notifications():
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(db.DSN, autocommit=True) as conn:
                await db.check_schema()
                await conn.execute(f"LISTEN {CHANNEL}")
                log.info("listening on channel %r", CHANNEL)

                #Reload data after connecting to avoid missing earlier updates
                listener_ready.set()
                broadcast_resync()

                async for note in conn.notifies():
                    try:
                        payload = json.loads(note.payload)
                        if not isinstance(payload, dict):
                            raise ValueError("notification must be an object")
                        kind = payload.get("kind")
                        symbol = payload.get("symbol")

                        if not kind or not symbol:
                            log.warning("incomplete PostgreSQL notification: %s", note.payload)
                            continue

                        if kind == "symbol":
                            dispatch_msg({
                                "kind": "symbol",
                                "symbol": symbol,
                                "exchange": symbol.rpartition(".")[2] if "." in symbol else None,
                            })
                            continue

                        if not subscribers.get(symbol):
                            continue

                        window_start = payload.get("window_start")
                        if not window_start:
                            log.warning("incomplete PostgreSQL notification: %s", note.payload)
                            continue

                        if kind == "window":
                            row = await db.get_window_by_key(symbol, window_start)
                        elif kind == "advisory":
                            row = await db.get_advisory_by_key(symbol, window_start)
                        else:
                            log.warning("unknown notification kind: %s", kind)
                            continue

                        if row is not None:
                            row["kind"] = kind
                            dispatch_msg(row)

                        # Advisories join their current price/EMAs from this window.
                        if kind == "window":
                            advisory = await db.get_advisory_by_key(symbol, window_start)
                            if advisory is not None:
                                advisory["kind"] = "advisory"
                                dispatch_msg(advisory)

                    except (ValueError, TypeError, KeyError):
                        log.warning(
                            "invalid PostgreSQL notification: %s",
                            note.payload,
                        )

        except asyncio.CancelledError:
            listener_ready.clear()
            raise
        except Exception as exc:
            listener_ready.clear()
            dispatch_msg({"kind": "status", "state": "reconnecting"})
            log.warning("listener lost (%s), retrying in 2s", exc)
            await asyncio.sleep(2)


#Format an update as a Server-Sent Event
def sse(event, data):
    return (
        f"event: {event}\n"
        f"data: {json.dumps(data, separators=(',', ':'))}\n\n"
    )


#Stream updates to a client and send missed data when it reconnects
async def stream(symbols, since):
    queue = asyncio.Queue(QUEUE_SIZE)
    all_subscribers.add(queue)

    #Register before catch-up so incoming updates are not missed
    for symbol in symbols:
        subscribers.setdefault(symbol, set()).add(queue)

    try:
        yield "retry: 3000\n\n"
        # Do not declare live delivery ready until LISTEN has been established.
        await listener_ready.wait()

        if since is not None:
            async for kind, row in db.iter_catchup(symbols, since):
                yield sse(kind, row)

        # A trading timestamp cannot detect older corrections or deletions.
        # Every connection must refresh its authoritative REST snapshots.
        if listener_ready.is_set():
            yield sse("resync", {})
        else:
            yield sse("status", {"state": "reconnecting"})

        while True:
            try:
                msg = await asyncio.wait_for(
                    queue.get(), KEEPALIVE_SECONDS
                )
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
                continue

            kind = msg.get("kind")

            if kind == "resync":
                if listener_ready.is_set():
                    yield sse("resync", {})
                else:
                    yield sse("status", {"state": "reconnecting"})
                continue

            yield sse(kind, msg)

    finally:
        #Remove the client when its connection closes
        all_subscribers.discard(queue)
        for symbol in symbols:
            queues = subscribers.get(symbol)

            if queues is not None:
                queues.discard(queue)

                if not queues:
                    del subscribers[symbol]
