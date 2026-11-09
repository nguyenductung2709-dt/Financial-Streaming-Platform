# Dashboard state and recovery

## Market workspace

The primary chart fills the center of the workspace. On startup, the dashboard
restores the saved primary symbol, chooses an available followed symbol, or
follows the first available symbol. Selecting a symbol opens and follows it in
one action. Its + button adds it to the comparison view. The Single chart /
Compare switch moves between individual price analysis and a shared percentage
chart for up to five symbols. Comparison selections persist in browser storage.

Dark and light themes include the charts and persist in browser storage. Signals
can be collapsed on desktop. On smaller screens, charts appear first and the
watchlist and signals use tabs. New signals retain a yellow arrival highlight,
disabled when reduced motion is requested.

The watchlist loads the last 24 five-minute windows for each followed symbol
through `/api/quotes?symbols=A,B`, then reconciles live window updates with the
same exact revision ordering as chart history. It shows the latest available
close, a sparkline, and change against the previous available window. This is
not a daily return. Unfollowed symbols show placeholders until selected/followed.

Chart controls select 1D, 3D, or all **loaded** history, reset pan/zoom, and toggle
EMA and signal overlays. Gaps without observations are collapsed on the time
axis and identified in the chart footer. Hover timestamps remain actual UTC
times. Clicking a signal focuses a 65-minute range around its window. Manual
pan/zoom is preserved through live updates; selecting a range recenters the view.
Prices and timestamps describe stored market windows, including historical replay;
the Live badge describes connection readiness rather than current market time.

## Comparing symbols

Compare offers **Price return** and **EMA spread (%)**, with the selected metric
saved in browser storage. Switching metrics reuses loaded window rows, preserves
symbols and colors, and resets zoom. Raw EMA overlays remain in Single chart.

Price return uses `(close / starting_close - 1) * 100`. Every plotted symbol must
have actual closes at the same starting **and ending** timestamp. 1D and 3D
choose the first common close within that period, measured back from the latest
common end in loaded history. All uses the first common close in loaded history.
At least two distinct matching closes are required; a single observation never
produces a measured 0% return. If the selected period contains only one match,
the chart offers an action to use all available loaded history.

One symbol with insufficient or incompatible history does not blank the chart.
The dashboard finds the largest group with two matching closes (at least two
symbols when multiple symbols are selected), preferring the newest common end
when equally sized groups exist. A notice identifies how many symbols are
actually compared. The other selections remain visible in the table with their
available dates, observation counts, and reasons such as `Needs another price`.
Their returns and comparison prices are not fabricated or carried forward.
Users can remove an unavailable selection or keep only the comparable group
without unfollowing those symbols. If no group can be compared, an empty state
shows the available histories and a Change symbols action.

The starting close stays anchored during live updates. The shared ending close
advances only when every plotted symbol has a close at a newer common time.
Changing the selection or choosing a range resets the anchor. If bounded-history
eviction removes the anchor, the chart selects a new shared start and displays it.
The table shows returns and raw closing prices at the shared end, not newer
unmatched prices from the watchlist. These are price returns, not total returns.
Excluded symbols rejoin automatically when their data supports a larger matching
group; a change to the plotted group resets manual zoom and is reflected in the
displayed shared period. Sparse series use markers to distinguish observations
from missing windows, and the header reports the number of matching closes.

Missing five-minute observations insert null points so lines cannot interpolate
across an individual symbol's gaps. Time intervals with no observations for any
selected symbol collapse on the axis, as disclosed in the footer. Hover retains
actual closing timestamps and prices. Hiding a line does not alter the shared
period. Symbol colors remain stable as other symbols are added or removed.
Endpoint labels appear on wider charts; compact charts retain colored chips,
the shared hover display, and the table.

EMA spread uses `((EMA38 - EMA100) / EMA100) * 100` at each actual window close.
It keeps its real percentage level instead of rebasing at the first observation.
Above zero means EMA38 is above EMA100, below zero means it is below, and zero
means equal EMAs. The table describes these as bullish, bearish, and equal EMA
alignment, respectively. The chart always includes the zero reference line;
hover shows the two source EMAs alongside the spread.

Spread does not require a shared start/end timestamp or two matching prices.
Each selected symbol can plot its own valid observations within 1D, 3D, or all
loaded history. A single observation appears as a point. The table identifies
each symbol's observation time, and endpoint markers stay at that time, even
when a peer has newer observations. Earlier endpoint labels include a timestamp.
These EMA levels describe indicator alignment rather than price performance.

Missing or non-finite EMAs and EMA100 values at or below zero are omitted.
Invalid observations inside a series remain null gaps, never fabricated zero
spreads or carried-forward values. A symbol with no usable EMA values is labeled
in the availability table while valid peers remain visible. Spread ranges count
back from the newest valid observation across the selections. Color, visibility,
history limits, and revision reconciliation work the same way for both metrics.

Comparison collapses the desktop signal panel by default and remembers its
visibility independently from Single chart. Chip labels hide/show lines; ×
removes a symbol without unfollowing it. Selecting a table symbol or ↗ opens its
individual price/EMA chart. The Add symbol button focuses watchlist search.

The HTML versions its local stylesheet and application script together. Bump
both version query strings when releasing changes to avoid mixed cached assets.

The backend serves this browser application and its REST/SSE endpoints from
the same origin. `app.js` loads saved history over REST and receives live rows
through an `EventSource` connected to `/api/events` for followed symbols.

## Signal activity counts

The activity badge reports all stored signals for the followed symbols and the
selected All/Buy/Sell direction. The list displays the newest 100 matching rows
from a bounded 300-row recent-activity cache; the summary labels the displayed
rows separately from the full total. A direction with no rows in recent activity
does not imply its stored total is zero.

The dashboard requests `/api/signals?include_counts=true`. The backend returns
the normal row array and `X-Signal-Total`, `X-Signal-Buy`, and `X-Signal-Sell`
headers, counted independently of the row limit from the same SQL snapshot.
Live advisory updates schedule a throttled refresh after two seconds; totals
are refreshed from the backend rather than incremented for each event. This
avoids double-counting revisions or replayed messages. Late responses for an
old subscription are discarded, and clearing follows immediately shows zero.

Missing or invalid count headers (including from an older backend) are labeled
as `loaded` counts with `Total unavailable`, rather than presenting the display
limit as an exact total. Restart the Python backend after installing the count
API update. No database migration or Docker data reset is required.

Signal rows, counts, prices, and chart histories remain in memory. Saved
watchlists, chart selections, theme, and metric preferences remain unchanged.

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

Single chart keeps the latest 5,000 window rows and 5,000 advisory rows.
Comparison loads up to 5,000 window rows for each of its five selected symbols
without fetching advisory histories. Incoming live results evict the oldest
rows when these limits are reached. Leaving a view or removing a symbol releases
history no longer needed by the active view. Unfollowing also
removes its signals, and highlight bookkeeping retains only current feed keys.

REST requests load the latest history initially. Older history is not fetched
automatically by zooming out; a date-range or load-older interface remains a
separate feature. These limits bound browser state while live updates continue.

## Failures and status

Symbol-list requests retry after three seconds on failure, including when a
previous list is still displayed. Empty successful responses keep polling until
symbols become available. A successful refresh clears the warning, and superseded
requests cannot restore an older error. Retries are independent of SSE and keep
the current chart selection. The initial selection is made once symbols first
become available, including after an initial connection failure.

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
Comparison tests cover normalization, shared start/end alignment, missing and
non-overlapping history, gap preservation, anchored ranges, independent line
visibility, stable colors, saved selections, limits, and revision-ordered updates.
