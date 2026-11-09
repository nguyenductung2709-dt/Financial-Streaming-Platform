
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional
import psycopg
import psycopg_pool
import uvicorn
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import db
import events

#Configure logging and create the backend logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("backend")


#Open the database pool and start the PostgreSQL notification listener
@asynccontextmanager
async def lifespan(app):
    listener = None
    try:
        await db.pool.open(wait=True)
        await db.check_schema()
        events.listener_ready = asyncio.Event()
        listener = asyncio.create_task(events.listen_for_notifications())
        log.info("live update mode: notify")
        yield
    finally:
        if listener is not None:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
        await db.pool.close()


#Create the FastAPI application and allow frontend requests
app = FastAPI(title="Financial streaming backend", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   expose_headers=["X-Signal-Total", "X-Signal-Buy", "X-Signal-Sell"])


#Return a 503 response when the database is unavailable
@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(psycopg_pool.PoolTimeout)
async def database_unavailable(request, exc):
    return JSONResponse({"detail": "database unavailable"}, status_code=503)


#Split a comma-separated symbol list and remove duplicates
def parse_symbols(text):
    return list(dict.fromkeys(s.strip() for s in text.split(",") if s.strip()))


#Convert a datetime to UTC
def utc(dt):
    return db.as_utc(dt) if dt else None


#Check that the backend can connect to the database
@app.get("/api/health")
async def health():
    try:
        await db.check_schema()
    except db.SchemaNotReady as exc:
        return JSONResponse({"detail": str(exc)}, status_code=503)
    if not events.listener_ready.is_set():
        return JSONResponse({"detail": "notification listener reconnecting"}, status_code=503)
    return {"status": "ok", "pid": os.getpid()}


#Return available symbols with optional search and exchange filters
@app.get("/api/symbols")
async def symbols(q: Optional[str] = None, exchange: Optional[str] = Query(None, description="FR, NL or ETR"), limit: int = Query(10000, ge=1, le=10000)):
    return await db.get_symbols(q, exchange, limit)


#Return EMA windows for a symbol within the requested time range
@app.get("/api/ema-windows")
async def ema_windows(symbol: str = Query(..., min_length=1), start: Optional[datetime] = None, end: Optional[datetime] = None, limit: int = Query(5000, ge=1, le=50000)):
    return await db.get_windows(symbol, utc(start), utc(end), limit)


@app.get("/api/quotes")
async def quotes(symbols: str = Query(..., min_length=1, description="comma-separated followed symbols")):
    return await db.get_quotes(parse_symbols(symbols))


#Return trade advisories for a symbol in chronological order
@app.get("/api/trade-advisories")
async def trade_advisories(symbol: str = Query(..., min_length=1), start: Optional[datetime] = None, end: Optional[datetime] = None, limit: int = Query(5000, ge=1, le=50000)):
    rows = await db.get_advisories([symbol], utc(start), utc(end), limit)
    return rows[::-1]


#Return the latest signals for the requested symbols
@app.get("/api/signals")
async def signals(response: Response, symbols: Optional[str] = Query(None, description="comma list; default all"),
                  limit: int = Query(50, ge=1, le=1000), include_counts: bool = Query(False)):
    selected = parse_symbols(symbols) if symbols is not None else None
    if not include_counts:
        return await db.get_signals(selected, limit)
    snapshot = await db.get_signal_snapshot(selected, limit)
    for name, value in snapshot["counts"].items():
        response.headers["X-Signal-" + name.title()] = str(value)
    response.headers["Cache-Control"] = "no-store"
    return snapshot["items"]


#Stream live updates for the requested symbols using Server-Sent Events
@app.get("/api/events")
async def live_events(symbols: str = Query("", description="comma list of symbols to subscribe to"), since: Optional[datetime] = Query(None, description="optional window-start history filter; resync always requires reloading snapshots")):
    wanted = parse_symbols(symbols)
    return StreamingResponse(events.stream(wanted, utc(since)), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


#Serve the HTML and JavaScript frontend from this process
FRONTEND_DIR = Path(os.getenv("FRONTEND_DIR", Path(__file__).resolve().parent.parent / "frontend"))
if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
else:
    log.warning("frontend folder not found at %s", FRONTEND_DIR)


#Start the backend when this file is run directly
if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")),
                timeout_graceful_shutdown=2)
