(() => {
  "use strict";
  let BUY = "#37d3a1", SELL = "#ff7f8b";
  let CLOSE = "#a7bfff", EMA38 = "#efb85c", EMA100 = "#b59aff";
  const QUOTE_MAX = 24, COMPARE_MAX = 5, WINDOW_MS = 5 * 60000;
  const SERIES_COLORS = {
    dark: ["#83aaff", "#b59aff", "#efb85c", "#48cdb0", "#f28db5"],
    light: ["#345fc5", "#7950c2", "#9e6500", "#087c68", "#b53577"],
  };
  const THEMES = {
    dark: { buy: BUY, sell: SELL, close: CLOSE, ema38: EMA38, ema100: EMA100,
      panel: "#111620", grid: "#1c2533", line: "#252e3e", text: "#93a0b5" },
    light: { buy: "#087c58", sell: "#c32e49", close: "#345fc5", ema38: "#9e6500", ema100: "#7950c2",
      panel: "#ffffff", grid: "#edf0f5", line: "#dbe2ec", text: "#59677d" },
  };
  const FLUSH_MS = 250, FEED_MAX = 300, MAX_ROWS = 300;
  const HISTORY_MAX = 5000, BUFFER_MAX = 2 * COMPARE_MAX * HISTORY_MAX + FEED_MAX;
  const API = location.protocol.startsWith("http") ? location.origin : "http://localhost:8000";

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
    if (!res.ok) {
      const error = new Error(`${path}: HTTP ${res.status}`);
      error.status = res.status;
      throw error;
    }
    return res.json();
  }

  const savedWatchlist = load("watchlist", []);
  const savedComparison = load("comparisonSymbols", []);
  const state = {
    symbols: [],
    watchlist: [...new Set((Array.isArray(savedWatchlist) ? savedWatchlist : []).filter((s) => typeof s === "string"))], slot: {A: null},
    query: "", exchange: "", onlyWatched: false, feedFilter: "all", windows: {}, advisories: {}, feed: [], quotes: {},
    theme: load("theme", "dark") === "light" ? "light" : "dark", compare: false, signalsVisible: true, comparisonSignalsVisible: false, mobilePanel: "watchlist",
    comparison: [...new Set((Array.isArray(savedComparison) ? savedComparison : []).filter((s) => typeof s === "string" && Array.isArray(savedWatchlist) && savedWatchlist.includes(s)))].slice(0, COMPARE_MAX),
  };

  //Keep the followed-symbol list saved between page reloads
  save("watchlist", state.watchlist);

  const charted = () => state.compare ? [...state.comparison] : [state.slot.A].filter(Boolean);
  const advisorySymbols = () => state.compare ? [] : [state.slot.A].filter(Boolean);

  //The watchlist determines which symbols receive live updates
  const subscribed = () => [...new Set(state.watchlist)];

  //Refresh the interface and update the backend subscription after a change
  function changed() {
    save("watchlist", state.watchlist);
    save("primarySymbol", state.slot.A);
    save("comparisonSymbols", state.comparison);
    const charts = new Set(charted());
    for (const symbol of Object.keys(state.quotes)) {
      if (!state.watchlist.includes(symbol)) delete state.quotes[symbol];
    }
    for (const symbol of Object.keys(state.windows)) {
      if (!charts.has(symbol)) delete state.windows[symbol];
    }
    const advisorySet = new Set(advisorySymbols());
    for (const symbol of Object.keys(state.advisories)) {
      if (!advisorySet.has(symbol)) delete state.advisories[symbol];
    }
    updateWorkspace();
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

      state.comparison = state.comparison.filter((s) => s !== symbol);
      for (const slot of ["A"]) {
        if (state.slot[slot] === symbol) {
          state.slot[slot] = null;
        }
      }
    } else {
      state.watchlist = [...state.watchlist, symbol];
    }

    changed();
  }

  // Opening a symbol also follows it so the chart continues receiving updates.
  function openSymbol(symbol) {
    if (!state.watchlist.includes(symbol)) state.watchlist.push(symbol);
    state.slot.A = symbol;
    state.compare = false;
    changed();
  }

  function toggleComparison(symbol) {
    if (state.comparison.includes(symbol)) {
      state.comparison = state.comparison.filter((s) => s !== symbol);
    } else {
      if (state.comparison.length >= COMPARE_MAX) {
        $("comparisonNotice").textContent = "Five symbols selected. Remove one before adding another.";
        $("comparisonNotice").hidden = false;
        return;
      }
      if (!state.watchlist.includes(symbol)) state.watchlist.push(symbol);
      comparison.colorIndex(symbol);
      state.comparison.push(symbol);
    }
    $("comparisonNotice").hidden = true;
    changed();
  }

  function addToComparison(symbol) {
    if (!state.compare) setCompare(true, symbol);
    else toggleComparison(symbol);
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
    const chip = (label, attrs, on) => `<button class="chip${on ? " on" : ""}" aria-pressed="${on}" ${attrs}>${esc(label)}</button>`;

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

  // Price changes compare adjacent available five-minute closes, never daily returns.
  function quoteInfo(rows) {
    const last = rows.at(-1), previous = rows.at(-2);
    const delta = previous && Number(previous.closing_price) > 0
      ? Number(last.closing_price) - Number(previous.closing_price) : null;
    return { last, delta, percent: delta === null ? null : delta / Number(previous.closing_price) * 100 };
  }

  const signed = (n, digits = 2) => `${n > 0 ? "+" : ""}${n.toFixed(digits)}`;

  function sparkline(rows) {
    if (rows.length < 2) return '<span class="spark-placeholder">—</span>';
    const values = rows.map((r) => Number(r.closing_price));
    const low = Math.min(...values), spread = Math.max(...values) - low;
    const points = values.map((v, i) => `${(i / (values.length - 1) * 60 + 1).toFixed(1)},${(spread ? 17 - (v - low) / spread * 16 : 9).toFixed(1)}`).join(" ");
    return `<svg class="sparkline ${values.at(-1) >= values[0] ? "up" : "down"}" viewBox="0 0 62 18" aria-hidden="true"><polyline points="${points}" fill="none" stroke="currentColor" stroke-width="1.4"/></svg>`;
  }

  function renderList() {
    const watching = new Set(state.watchlist);
    const q = state.query.trim().toUpperCase();
    $("hint").textContent = state.compare
      ? "Select symbols to compare. Use ↗ to inspect an individual chart."
      : `${watching.size} followed · Select a symbol to chart & follow. ★ toggles following.`;
    $("comparisonSelection").hidden = !state.compare;
    $("comparisonSelection").textContent = `IN COMPARISON  ${state.comparison.length} / ${COMPARE_MAX}`;
    $("symbolCount").textContent = state.symbols.length;
    const shown = state.symbols.filter((s) =>
      (!state.exchange || s.exchange === state.exchange) &&
      (!q || s.symbol.toUpperCase().includes(q)) &&
      (!state.onlyWatched || watching.has(s.symbol))
    );
    let html = shown.slice(0, MAX_ROWS).map((s) => {
      const on = watching.has(s.symbol);
      const rows = Object.values(state.quotes[s.symbol] || {}).sort(byTime);
      const { last, percent } = quoteInfo(rows);
      const selected = state.compare ? state.comparison.includes(s.symbol) : state.slot.A === s.symbol;
      const full = state.compare && !selected && state.comparison.length >= COMPARE_MAX;
      const color = selected && state.compare ? comparison.color(s.symbol) : "";
      const selection = state.compare
        ? `<button class="comparison-check${selected ? " on" : ""}" data-act="compare" aria-pressed="${selected}" aria-label="${selected ? "Remove" : "Add"} ${esc(s.symbol)} ${selected ? "from" : "to"} comparison" ${full ? "disabled" : ""} ${color ? `style="--series:${color}"` : ""}>${selected ? "✓" : ""}</button>`
        : `<button class="star${on ? " on" : ""}" data-act="watch" aria-pressed="${on}" aria-label="${on ? "Unfollow" : "Follow"} ${esc(s.symbol)}">${on ? "★" : "☆"}</button>`;
      return `<li class="row${selected ? " selected" : ""}" data-sym="${esc(s.symbol)}">${selection}` +
        `<button class="symbol-select" data-act="${state.compare ? "compare" : "open"}" aria-label="${state.compare ? selected ? "Remove from comparison" : "Add to comparison" : "Chart"} ${esc(s.symbol)}" aria-pressed="${selected}" ${full ? "disabled" : ""} title="${last ? `Latest close: ${esc(utcLabel(last.window_end))}` : "Select to follow and load prices"}">` +
        `<span class="sym">${esc(s.symbol)}</span><span class="quote-price">${last ? fmt(last.closing_price) : "—"}</span>` +
        `<span class="symbol-exchange">${esc(s.exchange || "Market")}${on ? sparkline(rows) : ""}</span>` +
        `<span class="quote-change ${percent === null ? "muted" : percent >= 0 ? "up" : "down"}">${percent === null ? on ? "Awaiting data" : "Follow for prices" : `${signed(percent)}%`}</span></button>` +
        `<button class="slot" data-act="${state.compare ? "detail" : "compare"}" aria-label="${state.compare ? "Open individual chart for" : "Compare"} ${esc(s.symbol)}" title="${state.compare ? "Open individual chart" : "Add to comparison"}">${state.compare ? "↗" : "+"}</button></li>`;
    }).join("");
    if (!shown.length) {
      html = `<li class="empty">${state.symbols.length ? "No symbols match your filters." : "Waiting for market symbols…"}</li>`;
    } else if (shown.length > MAX_ROWS) {
      html += `<li class="empty">Showing ${MAX_ROWS} of ${shown.length}. Narrow your search to see more.</li>`;
    }
    $("rows").innerHTML = html;
    let latest;
    for (const quote of Object.values(state.quotes)) {
      for (const row of Object.values(quote)) {
        if (!latest || row.window_end > latest.window_end) latest = row;
      }
    }
    $("dataTime").textContent = latest ? `Latest close ${utcLabel(latest.window_end)}` : "Awaiting market data";
  }

  //Load the available symbols from the backend
  let symbolsRequest = 0;
  let symbolsRetryTimer = null;
  let initialSymbolChosen = false;
  async function loadSymbols() {
    const token = ++symbolsRequest;
    clearTimeout(symbolsRetryTimer);
    symbolsRetryTimer = null;
    let retry = false;
    try {
      const symbols = await get("/api/symbols");
      if (token !== symbolsRequest) return state.symbols.length;
      if (!Array.isArray(symbols) || symbols.some((s) => !s || typeof s.symbol !== "string")) {
        throw new Error("Unexpected symbol response");
      }
      state.symbols = symbols;
      $("error").hidden = true;
      retry = symbols.length === 0;
      if (symbols.length && !initialSymbolChosen) {
        initialSymbolChosen = true;
        if (!state.slot.A && !state.compare) {
          const available = new Set(symbols.map((s) => s.symbol));
          const saved = load("primarySymbol", null);
          const symbol = available.has(saved) ? saved
            : state.watchlist.find((s) => available.has(s)) || symbols[0].symbol;
          openSymbol(symbol);
        }
      }
    } catch (error) {
      if (token !== symbolsRequest) return state.symbols.length;
      $("error").textContent = error.status
        ? `Market symbols unavailable (HTTP ${error.status}). Retrying…`
        : "Could not refresh market symbols. Retrying…";
      $("error").hidden = false;
      retry = true;
    }

    renderChips();
    renderList();
    if (retry) {
      symbolsRetryTimer = setTimeout(() => {
        symbolsRetryTimer = null;
        return loadSymbols();
      }, 3000);
    }
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
    } else if (b.dataset.act === "compare") {
      addToComparison(li.dataset.sym);
    } else {
      openSymbol(li.dataset.sym);
    }
  });

  $("followAll").addEventListener("click", () => {
    state.watchlist = [...new Set(state.symbols.map((s) => s.symbol))];
    changed();
  });

  $("unfollowAll").addEventListener("click", () => {
    state.watchlist = [];
    state.slot.A = null;
    state.comparison = [];
    changed();
  });

  const ts = (iso) => iso;

  const PLOT_CONFIG = {
    responsive: true,
    scrollZoom: true,
    displaylogo: false,
    displayModeBar: false,
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
    hovertemplate: `${name} %{y:,.4f}<extra></extra>`,
  });

  // Collapse intervals without observations, retaining real timestamps on hover.
  function dataBreaks(rows) {
    const breaks = [];
    for (let i = 1; i < rows.length; i++) {
      const gap = Date.parse(rows[i].window_start) - Date.parse(rows[i - 1].window_end);
      if (gap > 0) breaks.push({ values: [rows[i - 1].window_end], dvalue: gap });
    }
    return breaks;
  }

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
    hovertemplate: `${name} at %{y:,.4f}<extra></extra>`,
  });

  // Find the largest group with two actual matching closes; retain every selection in availability.
  function comparisonData(symbols, windows, range = "1D", anchor = null) {
    if (!symbols.length) return { series: [], availability: [], title: "Compare your market", message: "Choose up to five symbols from the watchlist to compare." };
    const rows = symbols.map((symbol) => Object.values(windows[symbol] || {})
      .filter((r) => Number.isFinite(Number(r.closing_price)) && Number(r.closing_price) > 0 && Number.isFinite(Date.parse(r.window_end)))
      .sort(byTime));
    const maps = rows.map((list) => new Map(list.map((r) => [Date.parse(r.window_end), r])));
    const availability = symbols.map((symbol, i) => ({ symbol, count: maps[i].size,
      first: rows[i][0], last: rows[i].at(-1), loading: !Object.hasOwn(windows, symbol), compared: false }));
    const ready = symbols.map((_, i) => i).filter((i) => maps[i].size >= 2);
    let selected = [], common = [];
    const minimum = symbols.length === 1 ? 1 : 2;
    // At most five selections means only 31 subsets. Prefer more symbols, then the newest shared period.
    for (let mask = 1; mask < (1 << ready.length); mask++) {
      const group = ready.filter((_, bit) => mask & (1 << bit));
      if (group.length < minimum || group.length < selected.length) continue;
      const times = [...maps[group[0]].keys()].filter((time) => group.every((i) => maps[i].has(time))).sort((a, b) => a - b);
      if (times.length < 2) continue;
      if (group.length > selected.length || times.at(-1) > common.at(-1) ||
          (times.at(-1) === common.at(-1) && times.length > common.length)) {
        selected = group; common = times;
      }
    }
    for (const item of availability) {
      item.compared = selected.some((i) => symbols[i] === item.symbol);
      item.reason = item.loading ? "Loading prices…" : item.count === 0 ? "No prices available yet"
        : item.count === 1 ? "Needs another price" : item.compared ? "Compared" : "No matching period";
    }
    if (!common.length) {
      const loading = availability.some((a) => a.loading);
      const sparse = availability.some((a) => a.count < 2);
      return { series: [], availability,
        title: loading ? "Loading price history" : sparse ? "More price history needed" : "These histories don’t line up yet",
        message: loading ? "Available dates will appear below as prices load."
          : sparse ? "A return needs at least two prices. See what is available for each symbol below."
            : "The available prices don’t include two matching times. Review their dates below or change your selection." };
    }
    const end = common.at(-1);
    const cutoff = range === "All" ? -Infinity : end - (range === "3D" ? 3 : 1) * 86400000;
    let start = anchor !== null && common.includes(anchor) && anchor < end ? anchor : common.find((t) => t >= cutoff);
    // Never present one common observation as a measured 0% return.
    if (start === end) {
      return { series: [], availability: availability.map((a) => ({ ...a, compared: false,
        reason: a.compared ? "Needs a longer period" : a.reason })),
        title: "This period has only one matching price",
        message: "Choose a longer period to measure a return.", suggestAll: true };
    }
    const observed = new Set();
    const series = selected.map((i) => {
      const symbol = symbols[i];
      const base = Number(maps[i].get(start).closing_price);
      const last = maps[i].get(end);
      const times = [], values = [], prices = [];
      let previous;
      for (const r of rows[i]) {
        const time = Date.parse(r.window_end);
        if (time < start || time > end) continue;
        if (previous !== undefined && time - previous > WINDOW_MS) {
          times.push(new Date(previous + WINDOW_MS).toISOString()); values.push(null); prices.push(null);
        }
        times.push(new Date(time).toISOString());
        values.push((Number(r.closing_price) / base - 1) * 100);
        prices.push(Number(r.closing_price)); observed.add(time); previous = time;
      }
      return { symbol, base, last, percent: (Number(last.closing_price) / base - 1) * 100, times, values, prices };
    });
    const allTimes = [...observed].sort((a, b) => a - b);
    const breaks = [];
    for (let i = 1; i < allTimes.length; i++) {
      const gap = allTimes[i] - allTimes[i - 1] - WINDOW_MS;
      if (gap > 0) breaks.push({ values: [new Date(allTimes[i - 1] + WINDOW_MS).toISOString()], dvalue: gap });
    }
    return { series, availability, start, end, breaks, commonCount: common.filter((t) => t >= start).length };
  }

  const historyDate = new Intl.DateTimeFormat("en-GB", {
    day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "UTC",
  });
  function historyLabel(item) {
    if (!item.count) return item.loading ? "Loading history…" : "No prices available yet";
    const first = new Date(item.first.window_end), last = new Date(item.last.window_end);
    const sameDay = first.toISOString().slice(0, 10) === last.toISOString().slice(0, 10);
    const end = sameDay ? last.toISOString().slice(11, 16) : historyDate.format(last);
    return `${historyDate.format(first)}${first.getTime() === last.getTime() ? "" : ` – ${end}`} UTC · ${item.count} ${item.count === 1 ? "price" : "prices"}`;
  }

  class ComparisonPane {
    constructor() {
      this.plot = $("comparisonCanvas");
      this.range = "1D";
      this.anchor = null;
      this.signature = null;
      this.cohortSignature = null;
      this.viewRevision = 0;
      this.colors = new Map();
      this.hidden = new Set();
      this.manualRange = null;
      this.eventsBound = false;
      $("comparisonRanges").addEventListener("click", (e) => {
        const button = e.target.closest("button[data-range]");
        if (!button) return;
        if (button.dataset.range !== "Reset") this.range = button.dataset.range;
        this.anchor = null; this.manualRange = null; this.viewRevision++;
        this.update();
      });
      $("comparisonSymbols").addEventListener("click", (e) => {
        const remove = e.target.closest("button[data-remove-symbol]");
        const toggle = e.target.closest("button[data-toggle-symbol]");
        if (remove) toggleComparison(remove.dataset.removeSymbol);
        else if (toggle) {
          const symbol = toggle.dataset.toggleSymbol;
          if (this.hidden.has(symbol)) this.hidden.delete(symbol); else this.hidden.add(symbol);
          this.update();
        } else if (e.target.closest("button[data-add-symbol]")) this.focusSymbols();
      });
      $("comparisonTable").addEventListener("click", (e) => {
        const remove = e.target.closest("button[data-remove-unavailable]");
        if (remove) { toggleComparison(remove.dataset.removeUnavailable); return; }
        const button = e.target.closest("button[data-detail-symbol]");
        if (button) openSymbol(button.dataset.detailSymbol);
      });
      $("comparisonChangeSymbols").addEventListener("click", () => this.focusSymbols());
      $("comparisonUseAvailable").addEventListener("click", () => {
        if (this.data.series.length < 2) return;
        state.comparison = this.data.series.map((s) => s.symbol);
        $("comparisonNotice").hidden = true;
        changed();
      });
      $("comparisonEmpty").addEventListener("click", (e) => {
        if (e.target.closest("button[data-use-all]")) {
          this.range = "All"; this.anchor = null; this.manualRange = null; this.viewRevision++;
          this.update();
        } else if (e.target.closest("button[data-change-symbols]")) this.focusSymbols();
      });
    }

    focusSymbols() {
      state.mobilePanel = "watchlist";
      $("workspace").classList.toggle("mobile-signals", false);
      $("watchlistTab").classList.toggle("on", true); $("watchlistTab").setAttribute("aria-pressed", true);
      $("signalsTab").classList.toggle("on", false); $("signalsTab").setAttribute("aria-pressed", false);
      $("search").focus(); $("search").scrollIntoView({ block: "nearest" });
    }

    colorIndex(symbol) {
      const others = new Set(state.comparison.filter((s) => s !== symbol).map((s) => this.colors.get(s)));
      const previous = this.colors.get(symbol);
      if (previous !== undefined && !others.has(previous)) return previous;
      const index = SERIES_COLORS.dark.findIndex((_, i) => !others.has(i));
      this.colors.set(symbol, index < 0 ? 0 : index);
      // Keep color bookkeeping bounded as symbols are replaced over a long session.
      for (const s of this.colors.keys()) {
        if (s !== symbol && !state.comparison.includes(s)) this.colors.delete(s);
      }
      return this.colors.get(symbol);
    }

    color(symbol) { return SERIES_COLORS[state.theme][this.colorIndex(symbol)]; }

    update() {
      const signature = state.comparison.join("|");
      if (signature !== this.signature) {
        this.anchor = null; this.manualRange = null; this.viewRevision++;
        this.signature = signature;
        for (const s of this.hidden) if (!state.comparison.includes(s)) this.hidden.delete(s);
      }
      this.data = comparisonData(state.comparison, state.windows, this.range, this.anchor);
      if (this.data.start !== undefined) this.anchor = this.data.start;
      else this.anchor = null;
      const data = this.data;
      const cohort = data.series.map((s) => s.symbol).join("|");
      if (cohort !== this.cohortSignature) {
        this.cohortSignature = cohort; this.manualRange = null; this.viewRevision++;
      }
      const theme = THEMES[state.theme];
      $("comparisonCount").textContent = `${state.comparison.length} / ${COMPARE_MAX}`;
      $("comparisonPeriod").textContent = data.series.length
        ? `Price return · Shared start ${utcLabel(new Date(data.start).toISOString())} · End ${utcLabel(new Date(data.end).toISOString())} · ${data.commonCount} matching closes`
        : "Price return · Available history is listed below";
      const excluded = data.availability.filter((a) => !a.compared);
      $("comparisonCoverage").hidden = !excluded.length;
      $("comparisonCoverageText").textContent = data.series.length
        ? `Showing ${data.series.length} of ${state.comparison.length} symbols. ${excluded.map((a) => `${a.symbol}: ${a.reason.toLowerCase()}`).join("; ")}.`
        : "Each return needs two matching prices. Review the available history below.";
      $("comparisonUseAvailable").hidden = data.series.length < 2 || !excluded.length;
      $("comparisonUseAvailable").textContent = `Keep these ${data.series.length} symbols`;
      $("comparisonSymbols").innerHTML = state.comparison.map((symbol) => {
        const visible = !this.hidden.has(symbol);
        return `<span class="comparison-chip${visible ? "" : " line-hidden"}" style="--series:${this.color(symbol)}">` +
          `<button data-toggle-symbol="${esc(symbol)}" aria-pressed="${visible}" title="${visible ? "Hide" : "Show"} line for ${esc(symbol)}"><i class="series-dot"></i>${esc(symbol)}</button>` +
          `<button data-remove-symbol="${esc(symbol)}" aria-label="Remove ${esc(symbol)} from comparison">×</button></span>`;
      }).join("") + `<button class="tool-button add-symbol" data-add-symbol ${state.comparison.length >= COMPARE_MAX ? "disabled" : ""}>+ Add symbol</button>`;
      $("comparisonRanges").querySelectorAll("[data-range]").forEach((b) => {
        const active = b.dataset.range === this.range;
        b.classList.toggle("on", active); b.setAttribute("aria-pressed", active);
      });
      $("comparisonHistoryHeading").textContent = excluded.length ? "Available history (UTC)" : "Data through (UTC)";
      $("comparisonTable").innerHTML = data.availability.map((item) => {
        const s = data.series.find((series) => series.symbol === item.symbol);
        const muted = this.hidden.has(item.symbol);
        const history = historyLabel(item);
        return `<tr class="${s ? muted ? "line-hidden" : "" : "comparison-unavailable"}"><th scope="row"><button class="table-symbol" data-detail-symbol="${esc(item.symbol)}" title="Open individual chart" style="--series:${this.color(item.symbol)}"><i class="series-dot"></i>${esc(item.symbol)}<span aria-hidden="true">↗</span></button>` +
          `${excluded.length ? `<small class="history-range">${esc(history)}</small>` : ""}</th>` +
          (s ? `<td class="${s.percent >= 0 ? "up" : "down"}">${signed(s.percent)}%</td><td>${fmt(s.last.closing_price)}</td>`
            : `<td><span class="history-status">${esc(item.reason)}</span><button class="plain remove-unavailable" data-remove-unavailable="${esc(item.symbol)}" aria-label="Remove ${esc(item.symbol)} from comparison">Remove</button></td><td class="muted">—</td>`) +
          `<td>${esc(excluded.length ? history : utcLabel(s.last.window_end))}</td></tr>`;
      }).join("");
      const available = typeof Plotly !== "undefined";
      const allHidden = data.series.length && data.series.every((s) => this.hidden.has(s.symbol));
      $("comparisonEmpty").hidden = !!(available && data.series.length && !allHidden);
      $("comparisonEmpty").innerHTML = !available
        ? '<strong>Chart library unavailable</strong><span>Reload to retry loading the comparison chart.</span>'
        : allHidden ? '<strong>All lines hidden</strong><span>Select a symbol chip above to show its line.</span>'
          : `<strong>${esc(data.title || "Compare your market")}</strong><span>${esc(data.message || "")}</span>` +
            `${state.comparison.length ? `<button class="tool-button" ${data.suggestAll ? "data-use-all" : "data-change-symbols"}>${data.suggestAll ? "Use all available history" : "Change symbols"}</button>` : ""}`;
      const traces = data.series.flatMap((s) => {
        const color = this.color(s.symbol), visible = !this.hidden.has(s.symbol);
        const sparse = s.prices.filter((p) => p !== null).length <= 12;
        return [{ type: "scatter", mode: sparse ? "lines+markers" : "lines", name: s.symbol, x: s.times, y: s.values, customdata: s.prices,
          line: { color, width: 1.8 }, marker: { color, size: 4 }, connectgaps: false, visible,
          hovertemplate: `${esc(s.symbol)} %{y:+.2f}%<br>Close %{customdata:,.4f}<extra></extra>` },
        { type: "scatter", mode: "markers", name: `${s.symbol} end`, x: [new Date(data.end).toISOString()], y: [s.percent],
          marker: { color, size: 6, line: { color: theme.panel, width: 1 } }, visible, hoverinfo: "skip" }];
      });
      const values = data.series.flatMap((s) => s.values.filter((v) => v !== null));
      const low = Math.min(0, ...values), high = Math.max(0, ...values);
      const pad = Math.max((high - low) * .12, .05);
      const ymin = low - pad, ymax = high + pad;
      const height = Math.max(120, (this.plot.clientHeight || 400) - 50);
      const labels = data.series.filter((s) => !this.hidden.has(s.symbol)).sort((a, b) => a.percent - b.percent);
      const positions = [];
      labels.forEach((s, i) => { positions[i] = Math.max((s.percent - ymin) / (ymax - ymin) * height, i ? positions[i - 1] + 19 : 8); });
      const overflow = Math.max(0, (positions.at(-1) || 0) - height + 8);
      const annotations = labels.map((s, i) => ({ xref: "paper", x: 1.01, xanchor: "left", yref: "y", y: s.percent,
        yshift: positions[i] - overflow - (s.percent - ymin) / (ymax - ymin) * height,
        xshift: 46, text: `${esc(s.symbol.length > 12 ? s.symbol.slice(0, 10) + "…" : s.symbol)} ${signed(s.percent)}%`, showarrow: false, bgcolor: theme.panel,
        font: { color: this.color(s.symbol), size: 10 }, borderpad: 3 }));
      const range = this.manualRange || (data.series.length
        ? [new Date(data.start - (data.start === data.end ? WINDOW_MS : 0)).toISOString(), new Date(data.end + WINDOW_MS).toISOString()] : null);
      const compact = this.plot.clientWidth !== undefined && this.plot.clientWidth < 650;
      const showEndpoints = !compact && (!range || Date.parse(range[1]) >= data.end);
      const axis = { gridcolor: theme.grid, linecolor: theme.line, zeroline: false, showspikes: true,
        spikecolor: theme.text, spikethickness: 1, spikemode: "across", spikesnap: "cursor" };
      const layout = {
        uirevision: `${this.signature}|${this.range}|${data.start}|${this.viewRevision}`,
        margin: { l: 14, r: compact ? 60 : 180, t: 18, b: 40 }, autosize: true,
        paper_bgcolor: theme.panel, plot_bgcolor: theme.panel,
        font: { family: "system-ui, -apple-system, Segoe UI, sans-serif", size: 11, color: theme.text },
        hovermode: "x unified", showlegend: false, dragmode: "pan",
        hoverlabel: { bgcolor: theme.panel, bordercolor: theme.line, font: { color: theme.text } },
        xaxis: { ...axis, type: "date", rangebreaks: data.breaks || [], ...(range ? { range, autorange: false } : {}) },
        yaxis: { ...axis, side: "right", ticksuffix: "%", tickformat: "+.2f", range: [ymin, ymax], fixedrange: true },
        shapes: [{ type: "line", xref: "paper", x0: 0, x1: 1, yref: "y", y0: 0, y1: 0,
          line: { color: theme.text, width: 1, dash: "dot" } }],
        annotations: showEndpoints ? annotations : [],
      };
      if (available) {
        Plotly.react(this.plot, traces, layout, PLOT_CONFIG);
        if (!this.eventsBound && typeof this.plot.on === "function") {
          this.eventsBound = true;
          this.plot.on("plotly_relayout", (event) => {
            if (event["xaxis.autorange"]) this.manualRange = null;
            const range = event["xaxis.range"];
            const start = range?.[0] || event["xaxis.range[0]"];
            const end = range?.[1] || event["xaxis.range[1]"];
            if (start && end) this.manualRange = [start, end];
          });
        }
      }
    }
  }

  class Pane {
    constructor(slot) {
      this.slot = slot;
      this.symbol = null;
      this.range = "1D";
      this.viewRevision = 0;
      this.focus = null;
      this.displayRange = null;
      this.axisRevision = null;
      this.displayYRange = null;
      this.indicators = { ema38: true, ema100: true, signals: true };
      const root = $("pane" + slot);
      root.innerHTML = `
        <header class="chart-head">
          <div class="chart-title"><span class="slot-tag">${slot === "A" ? "PRIMARY" : "COMPARE"}</span><strong data-r="name">Select a symbol</strong><span class="interval-tag">5m · Close</span><button class="plain" data-r="clear" hidden aria-label="Clear chart ${slot}">Clear</button></div>
          <div class="chart-stats" data-r="stats"></div>
        </header>
        <div class="chart-toolbar">
          <div class="range-controls" aria-label="Loaded history range" title="Ranges show loaded history; older records are not fetched automatically">
            ${["1D", "3D", "All"].map((range) => `<button class="range-button${range === this.range ? " on" : ""}" data-range="${range}" aria-pressed="${range === this.range}">${range}</button>`).join("")}
            <button class="range-button" data-range="Reset" title="Reset pan and zoom">Reset</button>
          </div>
          <div class="indicator-controls" aria-label="Chart overlays">
            <button class="indicator" data-indicator="ema38" aria-pressed="true"><i class="swatch" style="background:var(--ema38)"></i>EMA 38</button>
            <button class="indicator" data-indicator="ema100" aria-pressed="true"><i class="swatch" style="background:var(--ema100)"></i>EMA 100</button>
            <button class="indicator" data-indicator="signals" aria-pressed="true">Signals</button>
          </div>
        </div>
        <div class="chart-box"><div class="chart-canvas" data-r="canvas"></div><div class="chart-empty" data-r="empty"></div></div>
        <footer class="legend"><span><i class="swatch" style="background:var(--price)"></i>Close</span><span class="up">▲ Buy</span><span class="down">▼ Sell</span><span title="Intervals without observations are omitted from the time axis">Data gaps collapsed</span><span class="trend" data-r="trend">EMA trend</span></footer>`;
      const r = (name) => root.querySelector(`[data-r="${name}"]`);
      this.plot = r("canvas"); this.nameEl = r("name"); this.clearEl = r("clear");
      this.statsEl = r("stats"); this.emptyEl = r("empty"); this.trendEl = r("trend");
      this.clearEl.addEventListener("click", () => assign(slot, this.symbol));
      root.addEventListener("click", (e) => {
        const range = e.target.closest("button[data-range]");
        const indicator = e.target.closest("button[data-indicator]");
        if (range) {
          this.range = range.dataset.range === "Reset" ? "All" : range.dataset.range;
          this.focus = null; this.displayRange = null; this.viewRevision++;
        } else if (indicator) {
          const key = indicator.dataset.indicator;
          this.indicators[key] = !this.indicators[key];
          indicator.setAttribute("aria-pressed", this.indicators[key]);
        } else return;
        this.update();
      });
      this.root = root;
    }

    update() {
      const sym = state.slot[this.slot];
      if (sym !== this.symbol) { this.focus = null; this.displayRange = null; this.viewRevision++; }
      this.symbol = sym;
      const rows = Object.values(state.windows[sym] || {}).sort(byTime);
      const advs = Object.values(state.advisories[sym] || {}).sort(byTime);
      const times = new Set(rows.map((r) => r.window_start));
      const shown = advs.filter((a) => times.has(a.window_start));
      const x = rows.map((r) => ts(r.window_start));
      const traces = [
        lineTrace("Close", x, rows.map((r) => r.closing_price), CLOSE, 2),
        { ...lineTrace("EMA 38", x, rows.map((r) => r.ema38), EMA38, 1.2), visible: this.indicators.ema38 },
        { ...lineTrace("EMA 100", x, rows.map((r) => r.ema100), EMA100, 1.2), visible: this.indicators.ema100 },
        { ...flagTrace("Buy", shown.filter((a) => isBuy(a.action)), "triangle-up", BUY), visible: this.indicators.signals },
        { ...flagTrace("Sell", shown.filter((a) => !isBuy(a.action)), "triangle-down", SELL), visible: this.indicators.signals },
      ];
      const theme = THEMES[state.theme];
      const axis = { gridcolor: theme.grid, linecolor: theme.line, zeroline: false,
        showspikes: true, spikecolor: theme.text, spikethickness: 1, spikemode: "across", spikesnap: "cursor" };
      const last = rows.at(-1);
      let range;
      if (this.focus) {
        const center = Date.parse(this.focus);
        range = [new Date(center - 30 * 60000).toISOString(), new Date(center + 35 * 60000).toISOString()];
      } else if (last && this.range !== "All") {
        // Keep the input range stable so live updates preserve manual pan/zoom.
        if (!this.displayRange) {
          const end = Date.parse(last.window_end);
          const start = Math.max(Date.parse(rows[0].window_start), end - (this.range === "3D" ? 3 : 1) * 86400000);
          this.displayRange = [new Date(start).toISOString(), last.window_end];
        }
        range = this.displayRange;
      }
      if (rows.length && this.axisRevision !== this.viewRevision) {
        const visible = range ? rows.filter((r) => Date.parse(r.window_start) >= Date.parse(range[0]) && Date.parse(r.window_start) <= Date.parse(range[1])) : rows;
        const prices = (visible.length ? visible : rows).flatMap((r) => [Number(r.closing_price), Number(r.ema38), Number(r.ema100)]);
        const low = Math.min(...prices), high = Math.max(...prices);
        const pad = Math.max((high - low) * .12, high * .001);
        this.displayYRange = [low - pad, high + pad];
        this.axisRevision = this.viewRevision;
      }
      const layout = {
        uirevision: `${sym || "none"}|${this.viewRevision}`,
        margin: { l: 12, r: 76, t: 14, b: 36 }, autosize: true,
        paper_bgcolor: theme.panel, plot_bgcolor: theme.panel,
        font: { family: "system-ui, -apple-system, Segoe UI, sans-serif", size: 11, color: theme.text },
        showlegend: false, hovermode: "x unified", dragmode: "pan",
        hoverlabel: { bgcolor: theme.panel, bordercolor: theme.line, font: { color: theme.text } },
        xaxis: { ...axis, type: "date", rangebreaks: dataBreaks(rows), ...(range ? { range, autorange: false } : { autorange: true }) },
        yaxis: { ...axis, side: "right", tickformat: last && last.closing_price < 1 ? ",.4f" : ",.2f", fixedrange: false, automargin: true,
          ...(rows.length ? { range: this.displayYRange, autorange: false } : { autorange: true }) },
        shapes: this.indicators.ema38 && this.indicators.ema100 ? bands(rows) : [],
        annotations: last ? [{ xref: "paper", x: 1, xanchor: "left", yref: "y", y: last.closing_price,
          text: fmt(last.closing_price), showarrow: false, bgcolor: CLOSE, borderpad: 4, font: { color: theme.panel, size: 10 } }] : [],
      };
      if (typeof Plotly !== "undefined" && (this.slot === "A" || state.compare)) Plotly.react(this.plot, traces, layout, PLOT_CONFIG);
      this.nameEl.textContent = sym || "Select a symbol";
      this.clearEl.hidden = !sym;
      this.root.querySelectorAll("[data-range]").forEach((button) => {
        const active = !this.focus && button.dataset.range === this.range;
        button.classList.toggle("on", active); button.setAttribute("aria-pressed", active);
      });
      if (last) {
        const { delta, percent } = quoteInfo(rows);
        const bull = last.ema38 > last.ema100;
        const equal = last.ema38 === last.ema100;
        this.statsEl.innerHTML = `<strong class="main-price">${fmt(last.closing_price)}</strong>` +
          `<span class="price-change ${delta === null ? "muted" : delta >= 0 ? "up" : "down"}">${delta === null ? "—" : `${signed(delta, Math.abs(delta) < .01 ? 4 : 2)} (${signed(percent)}%)`}<small>vs previous available window</small></span>` +
          `<span class="quote-time">Latest close ${esc(utcLabel(last.window_end))} · EMA 38 ${fmt(last.ema38)} · EMA 100 ${fmt(last.ema100)}</span>`;
        this.trendEl.textContent = equal ? "EMA 38 = EMA 100" : `${bull ? "▲" : "▼"} EMA 38 ${bull ? "above" : "below"} EMA 100`;
        this.trendEl.className = `trend ${equal ? "muted" : bull ? "up" : "down"}`;
      } else {
        this.statsEl.innerHTML = '<span class="muted">Five-minute price history & EMA signals</span>';
        this.trendEl.textContent = "EMA trend"; this.trendEl.className = "trend";
      }
      this.emptyEl.hidden = !!(sym && rows.length && typeof Plotly !== "undefined");
      if (typeof Plotly === "undefined") {
        this.emptyEl.innerHTML = '<strong>Chart library unavailable</strong><span>Reload to retry loading charts. Watchlist prices and signals remain available.</span>';
      } else if (!sym) {
        this.emptyEl.innerHTML = '<span class="empty-icon" aria-hidden="true">⌁</span><strong>Your market, in focus</strong><span>Select a symbol from the watchlist to explore its price, EMA trends, and signals.</span>';
      } else {
        this.emptyEl.innerHTML = `<strong>No price history loaded</strong><span>Waiting for available windows for ${esc(sym)}. Connection and refresh status appear above.</span>`;
      }
    }
  }

  const panes = [new Pane("A")];
  const comparison = new ComparisonPane();
  const updateCharts = () => { if (state.compare) comparison.update(); else panes[0].update(); };
  function resizeCharts() {
    if (typeof Plotly !== "undefined" && Plotly.Plots) {
      if (state.compare) comparison.update();
      const plot = state.compare ? comparison.plot : panes[0].plot;
      if (plot.layout) Plotly.Plots.resize(plot);
    }
  }
  function updateWorkspace() {
    $("paneA").hidden = state.compare;
    $("comparisonPane").hidden = !state.compare;
    $("workspace").classList.toggle("comparison-mode", state.compare);
    $("compareToggle").setAttribute("aria-pressed", state.compare);
    $("singleToggle").setAttribute("aria-pressed", !state.compare);
    const signals = state.compare ? state.comparisonSignalsVisible : state.signalsVisible;
    $("workspace").classList.toggle("signals-hidden", !signals);
    $("signalsToggle").setAttribute("aria-expanded", signals);
    $("openIndividual").disabled = !state.comparison.length && !state.slot.A;
  }
  function setCompare(on, symbol = null) {
    state.compare = on;
    if (on && !state.comparison.length) {
      const initial = [state.slot.A, symbol].filter(Boolean);
      if (!symbol) initial.push(state.watchlist.find((s) => s !== state.slot.A));
      state.comparison = [...new Set(initial.filter(Boolean))].slice(0, COMPARE_MAX);
      for (const s of state.comparison) {
        if (!state.watchlist.includes(s)) state.watchlist.push(s);
        comparison.colorIndex(s);
      }
    } else if (on && symbol && !state.comparison.includes(symbol) && state.comparison.length < COMPARE_MAX) {
      if (!state.watchlist.includes(symbol)) state.watchlist.push(symbol);
      comparison.colorIndex(symbol);
      state.comparison.push(symbol);
    }
    $("comparisonNotice").hidden = true;
    changed(); resizeCharts();
  }
  function applyTheme() {
    const theme = THEMES[state.theme];
    BUY = theme.buy; SELL = theme.sell; CLOSE = theme.close; EMA38 = theme.ema38; EMA100 = theme.ema100;
    document.documentElement.setAttribute("data-theme", state.theme);
    $("themeToggle").textContent = state.theme === "dark" ? "Light theme" : "Dark theme";
    $("themeToggle").setAttribute("aria-label", `Switch to ${state.theme === "dark" ? "light" : "dark"} theme`);
    save("theme", state.theme); updateCharts(); renderFeed();
  }
  $("compareToggle").addEventListener("click", () => setCompare(true));
  $("singleToggle").addEventListener("click", () => setCompare(false));
  $("openIndividual").addEventListener("click", () => {
    const symbol = state.comparison[0] || state.slot.A;
    if (symbol) openSymbol(symbol);
  });
  $("signalsToggle").addEventListener("click", () => {
    if (state.compare) state.comparisonSignalsVisible = !state.comparisonSignalsVisible;
    else state.signalsVisible = !state.signalsVisible;
    updateWorkspace();
    resizeCharts();
  });
  $("themeToggle").addEventListener("click", () => { state.theme = state.theme === "dark" ? "light" : "dark"; applyTheme(); });
  for (const panel of ["watchlist", "signals"]) {
    $(panel + "Tab").addEventListener("click", () => {
      state.mobilePanel = panel;
      $("workspace").classList.toggle("mobile-signals", panel === "signals");
      for (const key of ["watchlist", "signals"]) {
        $(key + "Tab").classList.toggle("on", key === panel);
        $(key + "Tab").setAttribute("aria-pressed", key === panel);
      }
    });
  }
  if (typeof ResizeObserver !== "undefined") {
    const observer = new ResizeObserver(resizeCharts);
    observer.observe($("charts"));
  }
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

      return `<li class="feed-item${fresh ? " new" : ""}"><button class="signal-button" data-symbol="${esc(r.symbol)}" data-time="${esc(r.window_start)}" aria-label="Inspect ${buy ? "buy" : "sell"} signal for ${esc(r.symbol)} at ${esc(utcLabel(r.window_end))}">` +
        `<span class="feed-symbol">${esc(r.symbol)}</span><span class="feed-action" style="color:${color}">${buy ? "▲ BUY" : "▼ SELL"}</span>` +
        `<span class="feed-price">${fmt(r.closing_price)}</span><time class="feed-time" datetime="${esc(r.window_end)}">${esc(utcLabel(r.window_end))}</time>` +
        `<span class="feed-detail">EMA 38 ${fmt(r.ema38)} <span class="footer-dot">/</span> EMA 100 ${fmt(r.ema100)}</span></button></li>`;
    }).join("");

    if (!items.length) {
      html = `<li class="empty">${subSet.size === 0
        ? "Star symbols in the list to follow their buy and sell signals."
        : "No signals yet for the symbols you follow. New ones appear here as they arrive."}</li>`;
    }

    $("feedList").innerHTML = html;
    $("signalCount").textContent = items.length;

    knownFeedKeys = new Set(state.feed.map(feedKey));

    if (items.length) feedPrimed = true;
  }

  $("feedChips").addEventListener("click", (e) => {
    const b = e.target.closest("button[data-f]");
    if (!b) return;

    state.feedFilter = b.dataset.f;

    document.querySelectorAll("#feedChips .chip").forEach((c) => {
      c.classList.toggle("on", c === b);
      c.setAttribute("aria-pressed", c === b);
    });

    renderFeed();
  });

  $("feedList").addEventListener("click", (e) => {
    const button = e.target.closest("button[data-symbol]");
    if (!button || !state.watchlist.includes(button.dataset.symbol)) return;
    openSymbol(button.dataset.symbol);
    panes[0].focus = button.dataset.time; panes[0].viewRevision++;
    panes[0].update();
    $("paneA").scrollIntoView({ block: "nearest" });
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
    if (kind === "window" && !subscribed().includes(row.symbol)) return;
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
    let chartsDirty = false, quotesDirty = false;
    const feedRows = [];

    for (const { kind, row } of items) {
      if (kind === "window" && subSet.has(row.symbol)) {
        const quote = state.quotes[row.symbol] = state.quotes[row.symbol] || {};
        if (newer(row, quote[row.window_start])) {
          quote[row.window_start] = row;
          state.quotes[row.symbol] = Object.fromEntries(bounded(Object.values(quote), QUOTE_MAX).map((r) => [r.window_start, r]));
          quotesDirty = true;
        }
      }
      if (chartSet.has(row.symbol) && (kind === "window" || !state.compare)) {
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

    if (quotesDirty) renderList();

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
      const advisoryCharts = advisorySymbols();
      setStatus("refreshing");
      try {
        const [feed, windows, advisories, quotes] = await Promise.all([
          subs.length ? get("/api/signals", { symbols: subs.join(","), limit: FEED_MAX }) : [],
          Promise.all(charts.map((symbol) => get("/api/ema-windows", { symbol, limit: HISTORY_MAX }))),
          Promise.all(advisoryCharts.map((symbol) => get("/api/trade-advisories", { symbol, limit: HISTORY_MAX }))),
          subs.length ? get("/api/quotes", { symbols: subs.join(",") }) : [],
        ]);
        if (token !== generation) continue;
        // The snapshot replaces the previous database state. Revisions can
        // restart after a full reset; compare only live rows buffered since
        // this refresh started against the incoming snapshot.
        state.feed = feed;
        state.quotes = {};
        for (const row of quotes) {
          const bucket = state.quotes[row.symbol] = state.quotes[row.symbol] || {};
          bucket[row.window_start] = row;
        }
        state.windows = Object.fromEntries(charts.map((symbol, i) =>
          [symbol, history(windows[i])]));
        state.advisories = Object.fromEntries(advisoryCharts.map((symbol, i) =>
          [symbol, history(advisories[i])]));
        flushBuffer();
        updateCharts();
        renderList();
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

    const ck = `${state.compare ? "compare" : "single"}|${charted().join(",")}`;
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

  updateWorkspace();
  applyTheme();
  renderChips();
  renderList();
  renderFeed();
  updateCharts();
  applySubscription();

  // Symbol loading retries empty results and transient failures independently of SSE.
  loadSymbols();
})();
