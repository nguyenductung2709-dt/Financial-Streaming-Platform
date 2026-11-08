# Dashboard state and recovery

The backend serves this browser application and its REST/SSE endpoints from
the same origin. `app.js` loads saved history over REST and receives live rows
through an `EventSource` connected to `/api/events` for followed symbols.

## Snapshot loading

A `resync` event requests fresh symbol, window, advisory, and signal snapshots.
History requests run together and replace the displayed state after all succeed.
Incoming live rows are buffered while history is loading, then reconciled with
the snapshot using each row's revision. Revisions are compared as exact integers
using `BigInt`, so an older response cannot overwrite a newer row.
An authoritative snapshot replaces previously cached rows, including when a
database reset has restarted revision numbering.

Each refresh has a generation number. A new resync or selection change invalidates
earlier requests and queued messages, and their late responses are discarded.
Symbol-list requests also discard replies superseded by a newer request. Events
from a closed SSE connection are ignored.

The dashboard recovers through snapshots instead of a saved trading-time cursor,
which cannot identify changes to older windows. The backend's optional `since`
history stream remains available to other clients.

## Watchlists and cache limits

The saved watchlist accepts unique symbol strings. Invalid saved values fall
back to an empty watchlist so they cannot prevent the dashboard from opening.
Signals from unfollowed symbols are removed before applying the 300-record feed
limit, keeping older signals from currently followed symbols accessible.

Each chart keeps the latest 5,000 window rows and 5,000 advisory rows. Incoming
live results evict the oldest rows when these limits are reached. Removing a
symbol from both charts evicts its chart caches immediately. Unfollowing also
removes its signals, and highlight bookkeeping retains only current feed keys.

REST requests load the latest history initially. Older history is not fetched
automatically by zooming out; a date-range or load-older interface remains a
separate feature. These limits bound browser state while live updates continue.

## Failures and status

History requests time out after 15 seconds. A failure keeps the last complete
snapshot, shows `History unavailable; retrying…`, and schedules a retry starting
at one second with exponential backoff capped at 30 seconds. A successful refresh
resets the retry delay. Pending live updates are deduplicated by key and revision;
buffer overflow requests another snapshot refresh.

An open SSE transport alone does not set `Live`. That status requires a backend
resync signal indicating listener readiness and a successfully loaded snapshot.
Listener outages show `Reconnecting…`; browser connection failures retry after
three seconds. Outside snapshot loading, live updates are applied in 250 ms
batches to avoid redrawing the charts for each individual message.

## Tests

Run with Node.js 22:

```sh
node --check services/frontend/app.js
node --test services/frontend/app.test.cjs
```

The tests run the actual application code with controlled DOM, network, SSE,
and timer fixtures. They cover delayed responses, revision ordering, empty
snapshots, failed history loads and retries, selection changes, listener status,
obsolete connections, symbol-list races, and pending-buffer recovery.
They also cover watchlist changes, invalid saved watchlists, retention of the
newest rows, and immediate cache eviction after unfollowing.
