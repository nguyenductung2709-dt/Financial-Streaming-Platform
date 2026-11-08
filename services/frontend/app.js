(() => {
  "use strict";
  const BUY = "#15803d", SELL = "#c62828";
  const CLOSE = "#1f2a44", EMA38 = "#e08a00", EMA100 = "#7c5cc4";
  const FLUSH_MS = 250, FEED_MAX = 300, MAX_ROWS = 300;
  const HISTORY_MAX = 5000, BUFFER_MAX = 4 * HISTORY_MAX + FEED_MAX;
  const API = location.protocol.startsWith("http") ? location.origin : "http://localhost:8000";

  document.documentElement.style.setProperty("--buy", BUY);
  document.documentElement.style.setProperty("--sell", SELL);

  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  const isBuy = (a) => String(a).toLowerCase() === "buy";
  const byTime = (a, b) => a.window_start < b.window_start ? -1 : a.window_start > b.window_start ? 1 : 0;
  const utcLabel = (iso) => iso.replace("T", " ").slice(0, 16) + " UTC";

  const fmt = (x) => {
    const n = Number(x);
    if (!Number.isFinite(n)) return "–";
    return n >= 1 ? n.toFixed(2) : n.toFixed(4);
  };

  //Load saved browser data and use the default if it is missing or invalid
  const load = (key, initial) => {
    try {
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : initial;
    } catch {
      return initial;
    }
  };

  //Save browser data as JSON so it can be restored after a page reload
  const save = (key, value) => {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      //Storage may be unavailable
    }
  };

  //Fetch data from the backend and return the JSON response
  async function get(path, params = {}) {
    const url = new URL(API + path);
    for (const [k, v] of Object.entries(params)) {
      if (v != null) url.searchParams.set(k, v);
    }

    const res = await fetch(url, { signal: AbortSignal.timeout(15000) });
    if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
    return res.json();
  }

  const savedWatchlist = load("watchlist", []);
  const state = {
    symbols: [],
    watchlist: [...new Set((Array.isArray(savedWatchlist) ? savedWatchlist : []).filter((s) => typeof s === "string"))], slot: {A: null, B: null},
    query: "", exchange: "", onlyWatched: false, feedFilter: "all", windows: {}, advisories: {}, feed: [],
  };

  //Keep the followed-symbol list saved between page reloads
  save("watchlist", state.watchlist);

  const charted = () => [...new Set([state.slot.A, state.slot.B].filter(Boolean))];

  //The watchlist determines which symbols receive live updates
  const subscribed = () => [...new Set(state.watchlist)];

  //Refresh the interface and update the backend subscription after a change
  function changed() {
    save("watchlist", state.watchlist);
    const charts = new Set(charted());
    for (const bucket of [state.windows, state.advisories]) {
      for (const symbol of Object.keys(bucket)) {
        if (!charts.has(symbol)) delete bucket[symbol];
      }
    }
    state.feed = state.feed.filter((row) => state.watchlist.includes(row.symbol));
    renderChips();
    renderList();
    renderFeed();
    updateCharts();
    applySubscription();
  }

  //Follow or unfollow a symbol. Unfollowing also removes it from the charts
  function toggleWatch(symbol) {
    if (state.watchlist.includes(symbol)) {
      state.watchlist = state.watchlist.filter((s) => s !== symbol);

      for (const slot of ["A", "B"]) {
        if (state.slot[slot] === symbol) {
          state.slot[slot] = null;
        }
      }
    } else {
      state.watchlist = [...state.watchlist, symbol];
    }

    changed();
  }

  //Assign a followed symbol to a chart or remove it from that chart
  function assign(slot, symbol) {
    if (!state.watchlist.includes(symbol)) return;

    if (state.slot[slot] === symbol) {
      state.slot[slot] = null;
    } else {
      state.slot[slot] = symbol;
    }

    changed();
  }

  function renderChips() {
    const exchanges = [...new Set(state.symbols.map((s) => s.exchange).filter(Boolean))].sort();
    const chip = (label, attrs, on) => `<button class="chip${on ? " on" : ""}" ${attrs}>${esc(label)}</button>`;

    $("chips").innerHTML =
      chip("All", 'data-ex=""', !state.exchange) +
      exchanges.map((ex) =>
        chip(ex, `data-ex="${esc(ex)}"`, state.exchange === ex)
      ).join("") +
      chip(
        `★ Following ${state.watchlist.length}`,
        'data-watched="1"',
        state.onlyWatched
      );
  }

  //Display symbols that match the selected exchange, search, and watchlist filters
  function renderList() {
    const subs = subscribed();
    const watching = new Set(state.watchlist);
    const q = state.query.trim().toUpperCase();

    $("hint").textContent =
      "★ follows a symbol's signals. Only followed symbols can be displayed in charts. " +
      `${subs.length} followed.`;

    const shown = state.symbols.filter((s) =>
      (!state.exchange || s.exchange === state.exchange) &&
      (!q || s.symbol.toUpperCase().includes(q)) &&
      (!state.onlyWatched || watching.has(s.symbol))
    );

    let html = shown.slice(0, MAX_ROWS).map((s) => {
      const on = watching.has(s.symbol);
      const chartBlocked = !on;

      const slotBtn = (slot) => {
        const active = state.slot[slot] === s.symbol;

        return `<button class="slot${active ? " on" : ""}" data-act="${slot}" aria-pressed="${active}"` +
          ` title="${active ? "Remove from" : "Show in"} chart ${slot}"` +
          `${chartBlocked && !active ? " disabled" : ""}>${slot}</button>`;
      };

      return `<li class="row" data-sym="${esc(s.symbol)}"><span class="sym">${esc(s.symbol)}</span>` +
        `<button class="star${on ? " on" : ""}" data-act="watch" aria-pressed="${on}"` +
        ` title="${on ? "Stop following" : "Follow signals"}"` +
        `${on ? "★" : "☆"}</button>${slotBtn("A")}${slotBtn("B")}</li>`;
    }).join("");

    if (!shown.length) {
      html = `<li class="empty">${state.symbols.length
        ? "No symbol matches."
        : "No symbols yet. They appear once the materializer has stored results."}</li>`;
    } else if (shown.length > MAX_ROWS) {
      html += `<li class="empty">Showing ${MAX_ROWS} of ${shown.length}. Narrow the search to see more.</li>`;
    }

    $("rows").innerHTML = html;
  }

  //Load the available symbols from the backend
  let symbolsRequest = 0;
  async function loadSymbols() {
    const token = ++symbolsRequest;
    try {
      const symbols = await get("/api/symbols");
      if (token !== symbolsRequest) return state.symbols.length;
      state.symbols = symbols;
      $("error").hidden = true;
    } catch {
      if (token !== symbolsRequest) return state.symbols.length;
      $("error").textContent =
        "Can't reach the backend. Is it running on port 8000?";
      $("error").hidden = false;
    }

    renderChips();
    renderList();

    return state.symbols.length;
  }

  $("search").addEventListener("input", (e) => {
    state.query = e.target.value;
    renderList();
  });

  $("chips").addEventListener("click", (e) => {
    const b = e.target.closest("button");
    if (!b) return;

    if (b.dataset.watched) {
      state.onlyWatched = !state.onlyWatched;
    } else {
      state.exchange = b.dataset.ex;
    }

    renderChips();
    renderList();
  });

  $("rows").addEventListener("click", (e) => {
    const b = e.target.closest("button[data-act]");
    const li = e.target.closest("li[data-sym]");

    if (!b || !li || b.disabled) return;

    if (b.dataset.act === "watch") {
      toggleWatch(li.dataset.sym);
    } else {
      assign(b.dataset.act, li.dataset.sym);
    }
  });

  $("followAll").addEventListener("click", () => {
    state.watchlist = [...new Set(state.symbols.map((s) => s.symbol))];
    changed();
  });

  $("unfollowAll").addEventListener("click", () => {
    state.watchlist = [];
    state.slot.A = null;
    state.slot.B = null;
    changed();
  });

  const ts = (iso) => iso;

  const PLOT_CONFIG = {
    responsive: true,
    scrollZoom: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"],
  };

  //Create background bands showing when EMA38 is above or below EMA100
  function bands(rows) {
    const out = [];
    let start = 0;

    for (let i = 1; i <= rows.length; i++) {
      const bull = (r) => r.ema38 > r.ema100;

      if (i === rows.length || bull(rows[i]) !== bull(rows[start])) {
        out.push({
          type: "rect",
          xref: "x",
          yref: "paper",
          y0: 0,
          y1: 1,
          x0: ts(rows[start].window_start),
          x1: ts(
            i === rows.length
              ? rows[i - 1].window_end
              : rows[i].window_start
          ),
          fillcolor: bull(rows[start]) ? BUY : SELL,
          opacity: 0.07,
          line: { width: 0 },
          layer: "below",
        });

        start = i;
      }
    }

    return out;
  }

  const lineTrace = (name, x, y, color, width) => ({
    type: "scatter",
    mode: "lines",
    name,
    x,
    y,
    line: { color, width },
    hovertemplate: `${name} %{y:.3f}<extra></extra>`,
  });

  const flagTrace = (name, points, symbol, color) => ({
    type: "scatter",
    mode: "markers",
    name,
    x: points.map((a) => ts(a.window_start)),
    y: points.map((a) => a.closing_price),
    marker: {
      symbol,
      size: 12,
      color,
      line: { color: "#ffffff", width: 1 },
    },
    hovertemplate: `${name} at %{y:.3f}<extra></extra>`,
  });

  class Pane {
    constructor(slot) {
      this.slot = slot;
      this.symbol = null;

      const root = $("pane" + slot);

      root.innerHTML = `
        <header class="chart-head">
          <div class="chart-title">
            <span class="slot-tag">${slot}</span>
            <strong data-r="name">Empty</strong>
            <button class="plain" data-r="clear" hidden>Clear</button>
          </div>
          <div class="chart-stats" data-r="stats"></div>
        </header>
        <div class="chart-box">
          <div class="chart-canvas" data-r="canvas"></div>
          <div class="chart-empty" data-r="empty"></div>
        </div>
        <footer class="legend">
          <span><i class="swatch" style="background:${CLOSE}"></i>Closing price</span>
          <span><i class="swatch" style="background:${EMA38}"></i>EMA38</span>
          <span><i class="swatch" style="background:${EMA100}"></i>EMA100</span>
          <span style="color:${BUY}">▲ Buy</span>
          <span style="color:${SELL}">▼ Sell</span>
          <span><i class="swatch" style="height:10px;background:${BUY};opacity:.25"></i>EMA38 above</span>
          <span><i class="swatch" style="height:10px;background:${SELL};opacity:.25"></i>EMA38 below</span>
          <span class="muted">Times in UTC</span>
        </footer>`;

      const r = (name) => root.querySelector(`[data-r="${name}"]`);

      this.plot = r("canvas");
      this.nameEl = r("name");
      this.clearEl = r("clear");
      this.statsEl = r("stats");
      this.emptyEl = r("empty");

      this.clearEl.addEventListener("click", () =>
        assign(slot, this.symbol)
      );
    }

    //Draw the selected symbol's price, EMAs and trade signals
    update() {
      const sym = state.slot[this.slot];
      this.symbol = sym;

      const rows = Object.values(state.windows[sym] || {}).sort(byTime);
      const advs = Object.values(state.advisories[sym] || {}).sort(byTime);

      const times = new Set(rows.map((r) => r.window_start));
      const shown = advs.filter((a) => times.has(a.window_start));
      const x = rows.map((r) => ts(r.window_start));

      const traces = [
        lineTrace("Close", x, rows.map((r) => r.closing_price), CLOSE, 2),
        lineTrace("EMA38", x, rows.map((r) => r.ema38), EMA38, 1.3),
        lineTrace("EMA100", x, rows.map((r) => r.ema100), EMA100, 1.3),
        flagTrace("Buy", shown.filter((a) => isBuy(a.action)), "triangle-up", BUY),
        flagTrace("Sell", shown.filter((a) => !isBuy(a.action)), "triangle-down", SELL),
      ];

      const axis = {
        gridcolor: "#eef1f4",
        linecolor: "#d9dee4",
        zeroline: false,
      };

      const layout = {
        uirevision: sym || "none",
        margin: { l: 52, r: 16, t: 36, b: 36 },
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#ffffff",
        font: {
          family: "system-ui, -apple-system, Segoe UI, sans-serif",
          size: 12,
          color: "#5b6573",
        },
        showlegend: false,
        hovermode: "x unified",
        xaxis: {
          ...axis,
          type: "date",
          rangebreaks: [{ bounds: ["sat", "mon"] }],
          rangeselector: {
            x: 0,
            y: 1.02,
            yanchor: "bottom",
            buttons: [
              { count: 1, step: "day", stepmode: "backward", label: "1 day" },
              { count: 3, step: "day", stepmode: "backward", label: "3 days" },
              { step: "all", label: "All" },
            ],
          },
        },
        yaxis: { ...axis, tickformat: ",.2f" },
        shapes: bands(rows),
      };

      Plotly.react(this.plot, traces, layout, PLOT_CONFIG);

      this.nameEl.textContent = sym || "Empty";
      this.clearEl.hidden = !sym;

      const last = rows[rows.length - 1];

      if (last) {
        const bull = last.ema38 > last.ema100;

        this.statsEl.innerHTML =
          `<span>Close ${fmt(last.closing_price)}</span>` +
          `<span style="color:${EMA38}">EMA38 ${fmt(last.ema38)}</span>` +
          `<span style="color:${EMA100}">EMA100 ${fmt(last.ema100)}</span>` +
          `<span class="trend" style="color:${bull ? BUY : SELL}">` +
          `${bull ? "▲ EMA38 above EMA100" : "▼ EMA38 below EMA100"}</span>`;
      } else {
        this.statsEl.innerHTML = "";
      }

      if (!sym) {
        this.emptyEl.innerHTML =
          `Choose a symbol in the list and press <b>${this.slot}</b> to chart it here.`;
        this.emptyEl.hidden = false;
      } else if (!rows.length) {
        this.emptyEl.textContent = `Loading ${sym}…`;
        this.emptyEl.hidden = false;
      } else {
        this.emptyEl.hidden = true;
      }
    }
  }

  const panes = [new Pane("A"), new Pane("B")];
  const updateCharts = () => panes.forEach((p) => p.update());
  const feedKey = (r) => `${r.symbol}|${r.window_start}`;
  let knownFeedKeys = new Set();
  let feedPrimed = false;

  // Revisions are decimal strings so comparisons stay exact beyond 2^53.
  const newer = (a, b) => !b || BigInt(a.revision || "0") >= BigInt(b.revision || "0");
  const bounded = (rows, limit) => [...rows].sort((a, b) =>
    b.window_start.localeCompare(a.window_start) || a.symbol.localeCompare(b.symbol)
  ).slice(0, limit);
  const history = (rows) => Object.fromEntries(
    bounded(rows, HISTORY_MAX).map((row) => [row.window_start, row])
  );

  //Merge new signals with existing ones and avoid duplicate entries
  function mergeFeed(rows) {
    const watching = new Set(subscribed());
    const byKey = new Map(state.feed.filter((r) => watching.has(r.symbol)).map((r) => [feedKey(r), r]));

    for (const row of rows) {
      if (watching.has(row.symbol) && newer(row, byKey.get(feedKey(row)))) {
        byKey.set(feedKey(row), row);
      }
    }

    const next = bounded(byKey.values(), FEED_MAX);

    const changed = next.length !== state.feed.length || next.some((row, i) => JSON.stringify(row) !== JSON.stringify(state.feed[i]));
    if (!changed) return false;
    state.feed = next;
    return true;
  }

  //Display signals for followed symbols using the selected filter
  function renderFeed() {
    const subSet = new Set(subscribed());

    const items = state.feed
      .filter((r) => subSet.has(r.symbol))
      .filter((r) =>
        state.feedFilter === "all" ||
        (state.feedFilter === "buy") === isBuy(r.action)
      )
      .slice(0, 100);

    let html = items.map((r) => {
      const buy = isBuy(r.action);
      const color = buy ? BUY : SELL;
      const fresh = feedPrimed && !knownFeedKeys.has(feedKey(r));

      return `<li class="feed-item${fresh ? " new" : ""}" style="border-left-color:${color}">` +
        `<div class="feed-top"><span class="feed-action" style="color:${color}">${buy ? "▲ Buy" : "▼ Sell"}</span>` +
        `<strong>${esc(r.symbol)}</strong><span class="feed-time">${esc(utcLabel(r.window_end))}</span></div>` +
        `<div class="feed-detail">Close ${fmt(r.closing_price)} · EMA38 ${fmt(r.ema38)} · EMA100 ${fmt(r.ema100)}</div></li>`;
    }).join("");

    if (!items.length) {
      html = `<li class="empty">${subSet.size === 0
        ? "Star symbols in the list to follow their buy and sell signals."
        : "No signals yet for the symbols you follow. New ones appear here as they arrive."}</li>`;
    }

    $("feedList").innerHTML = html;

    knownFeedKeys = new Set(state.feed.map(feedKey));

    if (items.length) feedPrimed = true;
  }

  $("feedChips").addEventListener("click", (e) => {
    const b = e.target.closest("button[data-f]");
    if (!b) return;

    state.feedFilter = b.dataset.f;

    document.querySelectorAll("#feedChips .chip").forEach((c) =>
      c.classList.toggle("on", c === b)
    );

    renderFeed();
  });

  let buffer = new Map();
  let syncPromise = null;
  let syncRequested = false;
  let generation = 0;
  let syncRetryTimer = null;
  let syncRetryDelay = 1000;
  let streamConnected = false;
  let listenerReady = false;

  const push = (kind, row) => {
    if (kind === "window" && !charted().includes(row.symbol)) return;
    if (kind === "advisory" && !subscribed().includes(row.symbol)) return;
    const key = `${kind}|${feedKey(row)}`;
    if (!buffer.has(key) && buffer.size >= BUFFER_MAX) sync(true);
    if (newer(row, buffer.get(key)?.row)) buffer.set(key, { kind, row });
  };

  //Batch incoming updates to avoid redrawing charts for every individual event
  function flushBuffer() {
    if (!buffer.size) return;

    const items = [...buffer.values()];
    buffer.clear();
    const chartSet = new Set(charted());
    const subSet = new Set(subscribed());
    let chartsDirty = false;
    const feedRows = [];

    for (const { kind, row } of items) {
      if (chartSet.has(row.symbol)) {
        const bucket =
          kind === "window" ? state.windows : state.advisories;

        const rows = bucket[row.symbol] = bucket[row.symbol] || {};
        if (newer(row, rows[row.window_start])) {
          rows[row.window_start] = row;
          chartsDirty = true;
        }
      }

      if (kind === "advisory" && subSet.has(row.symbol)) {
        feedRows.push(row);
      }
    }

    if (chartsDirty) {
      for (const bucket of [state.windows, state.advisories]) {
        for (const symbol of chartSet) {
          bucket[symbol] = history(Object.values(bucket[symbol] || {}));
        }
      }
      updateCharts();
    }

    if (feedRows.length && mergeFeed(feedRows)) {
      renderFeed();
    }
  }
  setInterval(() => { if (!syncPromise) flushBuffer(); }, FLUSH_MS);

  // Replace snapshots atomically, then apply buffered updates by row revision.
  // An invalidation discards queued messages from the previous refresh.
  function sync(invalidate = false) {
    if (invalidate) {
      generation++;
      buffer.clear();
    }
    syncRequested = true;
    clearTimeout(syncRetryTimer);
    if (!syncPromise) {
      syncPromise = refreshSnapshots().finally(() => { syncPromise = null; });
    }
    return syncPromise;
  }

  async function refreshSnapshots() {
    do {
      syncRequested = false;
      const token = generation;
      const subs = subscribed();
      const charts = charted();
      setStatus("refreshing");
      try {
        const [feed, windows, advisories] = await Promise.all([
          subs.length ? get("/api/signals", { symbols: subs.join(","), limit: FEED_MAX }) : [],
          Promise.all(charts.map((symbol) => get("/api/ema-windows", { symbol, limit: HISTORY_MAX }))),
          Promise.all(charts.map((symbol) => get("/api/trade-advisories", { symbol, limit: HISTORY_MAX }))),
        ]);
        if (token !== generation) continue;
        // The snapshot replaces the previous database state. Revisions can
        // restart after a full reset; compare only live rows buffered since
        // this refresh started against the incoming snapshot.
        state.feed = feed;
        state.windows = Object.fromEntries(charts.map((symbol, i) =>
          [symbol, history(windows[i])]));
        state.advisories = Object.fromEntries(charts.map((symbol, i) =>
          [symbol, history(advisories[i])]));
        flushBuffer();
        updateCharts();
        renderFeed();
        syncRetryDelay = 1000;
        setStatus(streamConnected && listenerReady ? "live" : "connecting");
      } catch {
        if (token !== generation) continue;
        setStatus("historyError");
        syncRetryTimer = setTimeout(() => sync(), syncRetryDelay);
        syncRetryDelay = Math.min(syncRetryDelay * 2, 30000);
        return false;
      }
    } while (syncRequested);
    return true;
  }

  const STATUS_TEXT = {
    idle: "Waiting for symbols…",
    connecting: "Connecting…",
    live: "Live",
    reconnecting: "Reconnecting…",
    refreshing: "Refreshing history…",
    historyError: "History unavailable; retrying…",
  };

  function setStatus(s) {
    const el = $("status");
    el.className = "status " + s;
    el.textContent = STATUS_TEXT[s];
  }

  let streamKey = null;
  let chartsKey = null;
  let source = null;
  let retryTimer = null;

  //Open an SSE connection to receive updates for the followed symbols
  function openStream(subs) {
    if (source) {
      source.close();
      source = null;
    }

    clearTimeout(retryTimer);
    streamConnected = false;
    listenerReady = false;

    function connect() {
      setStatus("connecting");

      const params = new URLSearchParams({
        symbols: subs.join(","),
      });

      const url = `${API}/api/events?${params.toString()}`;
      const es = new EventSource(url);

      source = es;

      es.onopen = () => {
        if (source !== es) return;

        streamConnected = true;
      };

      es.onerror = () => {
        if (source !== es) return;

        setStatus("reconnecting");
        streamConnected = false;
        listenerReady = false;
        es.close();
        source = null;

        clearTimeout(retryTimer);
        retryTimer = setTimeout(connect, 3000);
      };

      es.addEventListener("symbol", () => {
        if (source === es) loadSymbols();
      });

      es.addEventListener("window", (e) => {
        if (source === es) push("window", JSON.parse(e.data));
      });

      es.addEventListener("advisory", (e) => {
        if (source === es) push("advisory", JSON.parse(e.data));
      });

      es.addEventListener("resync", () => {
        if (source === es) {
          loadSymbols();
          listenerReady = true;
          sync(true);
        }
      });
      es.addEventListener("status", () => {
        if (source !== es) return;
        listenerReady = false;
        setStatus("reconnecting");
      });
    }

    connect();
  }

  //Reconnect when the followed-symbol list changes and reload history when chart selections change
  function applySubscription() {
    const subs = subscribed();
    const key = subs.join(",");

    const ck = charted().join(",");
    const selectionChanged = key !== streamKey || ck !== chartsKey;
    if (key !== streamKey) {
      streamKey = key;
      openStream(subs);
    }

    if (selectionChanged) {
      chartsKey = ck;
      sync(true);
    }
  }

  renderChips();
  renderList();
  renderFeed();
  updateCharts();
  applySubscription();

  //Load symbols when the page opens and retry while none are available
  (async function boot() {
    const n = await loadSymbols();

    if (n === 0) {
      setTimeout(boot, 3000);
    }
  })();
})();
