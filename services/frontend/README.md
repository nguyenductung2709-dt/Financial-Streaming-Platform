# Frontend

The frontend is a web-based dashboard built with HTML, plain JavaScript, and CSS (`index.
html`, `app.js`, and `style.css`). It uses [Plotly.js](https://plotly.com/javascript/) to 
display charts and requires no separate build step. The [backend](../backend/README.md) 
serves it at http://localhost:8000.

## Dashboard

The dashboard is divided into three sections:

- **Watchlist** - lists all available symbols and lets users choose which ones to follow. A 
user can follow at most 100 symbols

- **Main chart** - shows the selected symbol's closing price, EMA values (EMA38 and EMA100), 
and markers for its buy/sell advisories

- **Signal feed** - displays recent BUY and SELL signals for the followed symbols

The dashboard also has a **Compare Performance** view that allows users to compare up to five 
symbols on the same chart. There are two comparison metrics available:

- **Price return** - shows the percentage change in price from a common starting point
- **EMA spread** - shows the percentage gap between EMA38 and EMA100

Both chart views support three time ranges: the last day, the last three days, or all loaded 
history.

All timestamps are displayed in UTC. During replay, timestamps represent historical market 
time. The Live badge only tells you that the connection is working, not that the displayed 
data is current.

The watchlist, selected symbols, comparison metric, and theme are saved in `localStorage` and 
restored when the page is reopened.

## Follow limit

The dashboard always shows every symbol, but only followed symbols receive prices, signals, 
and live updates. A user can follow at most 100 symbols (`FOLLOW_MAX` in `app.js`):

- When the limit is reached, the follow, chart, and compare buttons of unfollowed symbols are 
disabled until the user unfollows one
- "Follow up to 100" follows symbols until the limit is reached and keeps the existing follows

## Live updates and synchronization

The frontend receives live updates through a Server-Sent Events (SSE) connection using 
`EventSource` on `/api/events`. The backend sends updates for the symbols followed by the 
user.

When the frontend receives a `resync` event, it reloads the data using the REST API. While 
the data is loading, new live updates are temporarily stored. Once loading is complete, the 
stored updates are merged with the loaded data by revision. Revisions are compared using 
`BigInt`, so a slow response can never overwrite newer data. Outside of a reload, incoming 
updates are processed in batches every 250 ms rather than one at a time.

If loading historical data fails, the frontend keeps the data it already has and tries again. 
The retry delay starts at 1 second and increases to a maximum of 30 seconds.

To limit memory usage, the frontend stores a maximum of:

- 5,000 windows and 5,000 advisories for the main chart
- 5,000 windows per symbol in the Compare Performance view
- 24 windows per followed symbol for the watchlist prices
- 300 signals

## Changing the code

`index.html` loads `app.js` and `style.css` with version query strings. Whenever either file 
is changed, both version numbers should be increased together to prevent browsers from mixing 
old and new files.

The tests run the actual `app.js` with a fake DOM, network, and timers. Use Node.js 22 to run 
them:

```
node --check services/frontend/app.js
node --test services/frontend/app.test.cjs
```
