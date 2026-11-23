# Frontend

The dashboard is plain JavaScript (`index.html`, `app.js` and `style.css`) with no
build step. The [backend](../backend/README.md) serves it at http://localhost:8000.

You pick symbols from the watchlist on the left. The main chart shows the selected
symbol's closing price with both EMAs and markers for its advisories, over the last
day, the last three days, or everything loaded. The Compare view puts up to five
symbols on one chart, either as price return from a common starting point or as the
gap between EMA 38 and EMA 100 in percent. Recent BUY and SELL signals for the symbols
you follow are listed on the right.

Times are UTC. During a replay, the timestamps are historical market time, so the Live
badge only tells you the connection is working, not that the data is current.

The watchlist, the selected symbols, the comparison metric and the theme are saved in
`localStorage`.

## How it stays in sync

The page opens an `EventSource` on `/api/events`. Every time the backend sends
`resync`, the page reloads its data over REST, holds back live rows until the reload
finishes, and then merges them by revision. Revisions are compared as `BigInt`, so a
slow response can never overwrite newer data. Outside a reload, live rows are applied
every 250 ms rather than one at a time.

If loading history fails, the page keeps what it has and tries again, waiting
1 second at first and up to 30 seconds. To keep memory bounded, it holds at most 5,000
windows and 5,000 advisories for the main chart, 5,000 windows per symbol in Compare,
and 300 signals.

## Changing the code

`index.html` loads `app.js` and `style.css` with a version query string. When you
change either file, bump both versions together so that browsers don't mix old and new
files.

The tests run the real `app.js` against a fake DOM, network and timers. Use Node.js 22:

```
node --check services/frontend/app.js
node --test services/frontend/app.test.cjs
```
