const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Exercise the actual dashboard with deterministic DOM, network, and timers.
async function dashboard(storage = {}) {
  const nodes = new Map();
  const timers = new Map();
  const sources = [];
  let timerId = 0;
  function node(id) {
    if (!nodes.has(id)) nodes.set(id, {
      innerHTML: '', textContent: '', hidden: false, attrs: {}, handlers: {}, dataset: {},
      addEventListener(kind, handler) { this.handlers[kind] = handler; },
      setAttribute(key, value) { this.attrs[key] = String(value); },
      querySelector: (selector) => node(id + selector), querySelectorAll: () => [],
      scrollIntoView() {},
      style: { setProperty() {} }, classList: { toggle() {} },
    });
    return nodes.get(id);
  }
  const ctx = {
    URL, URLSearchParams, AbortSignal,
    location: { protocol: 'http:', origin: 'http://localhost:8000' },
    document: { documentElement: node('html'), getElementById: node, querySelectorAll: () => [] },
    localStorage: {
      getItem: (key) => Object.hasOwn(storage, key) ? JSON.stringify(storage[key]) : null,
      setItem(key, value) { storage[key] = JSON.parse(value); },
    },
    Plotly: { react(plot, traces, layout) { plot.traces = traces; plot.layout = layout; } },
    EventSource: class {
      constructor(url) { this.url = url; this.listeners = {}; sources.push(this); }
      close() { this.closed = true; }
      addEventListener(kind, handler) { this.listeners[kind] = handler; }
      send(kind, data = {}) { this.listeners[kind]?.({ data: JSON.stringify(data) }); }
    },
    fetch: async () => ({ ok: true, json: async () => [] }),
    setInterval: (callback) => { ctx.flush = callback; },
    setTimeout(callback, delay) { timers.set(++timerId, { callback, delay }); return timerId; },
    clearTimeout(id) { timers.delete(id); },
  };
  const original = fs.readFileSync(__dirname + '/app.js', 'utf8');
  const source = original.replace(/\}\)\(\);\s*$/, `
    globalThis.review = {state, mergeFeed, renderFeed, push, sync, changed, applySubscription, loadSymbols,
      panes, openSymbol, setCompare, quoteInfo, renderList, applyTheme, toggleWatch,
      comparison, comparisonData, toggleComparison, addToComparison, COMPARE_MAX,
      getPending: () => syncPromise, getBuffer: () => buffer,
      getKnown: () => knownFeedKeys, HISTORY_MAX, BUFFER_MAX};
  })();`);
  assert.notEqual(source, original, 'test harness must expose the dashboard');
  vm.runInNewContext(source, ctx);
  await ctx.review.sync();
  return { ctx, ...ctx.review, node, timers, sources };
}

function row(price = 15, revision = '1', index = 0, symbol = 'A') {
  const time = Date.parse('2021-11-08T10:00:00Z') + index * 300000;
  return {
    symbol, revision, window_start: new Date(time).toISOString(),
    window_end: new Date(time + 300000).toISOString(), action: 'BUY',
    closing_price: price, ema38: 14, ema100: 13,
  };
}

function select(app, symbol = 'A') {
  app.state.watchlist = [symbol];
  app.state.slot.A = symbol;
}

function snapshots(app, windows = [], feed = [], advisories = []) {
  app.ctx.fetch = async (url) => ({ ok: true, json: async () => ({
    '/api/ema-windows': windows, '/api/trade-advisories': advisories,
    '/api/signals': feed, '/api/symbols': [], '/api/quotes': windows,
  })[url.pathname] });
}

test('unfollowed symbols cannot crowd an older followed signal out of the feed', async () => {
  const app = await dashboard();
  select(app);
  app.mergeFeed(Array.from({ length: 300 }, (_, i) => row(15, '1', i)));
  app.state.watchlist = ['B'];
  assert.equal(app.mergeFeed([row(15, '1', -1, 'B')]), true);
  app.renderFeed();
  assert.equal(app.state.feed.length, 1);
  assert.equal(app.state.feed[0].symbol, 'B');
  assert.doesNotMatch(app.node('feedList').innerHTML, /No signals yet/);
});

test('invalid saved watchlists do not prevent the dashboard from opening', async () => {
  for (const watchlist of [null, 42, {}, 'A']) {
    const app = await dashboard({ watchlist });
    assert.equal(app.state.watchlist.length, 0);
    assert.equal(app.state.feed.length, 0);
  }
});

test('saved watchlists retain unique symbol strings', async () => {
  const app = await dashboard({ watchlist: ['A', 'A', 42, null, 'B'] });
  assert.deepEqual(Array.from(app.state.watchlist), ['A', 'B']);
});

test('authoritative empty snapshots remove deleted windows, advisories, and signals', async () => {
  const app = await dashboard();
  select(app);
  app.push('window', row());
  app.push('advisory', row());
  app.ctx.flush();
  assert.equal(app.state.feed.length, 1);
  snapshots(app);
  await app.sync(true);
  assert.equal(Object.keys(app.state.windows.A).length, 0);
  assert.equal(Object.keys(app.state.advisories.A).length, 0);
  assert.equal(app.state.feed.length, 0);
});

test('a recreated database snapshot replaces higher revisions cached before reset', async () => {
  const app = await dashboard();
  select(app);
  app.push('window', row(15, '100'));
  app.push('advisory', row(15, '100'));
  app.ctx.flush();
  snapshots(app, [row(25, '1')], [row(25, '2')], [row(25, '2')]);
  await app.sync(true);
  assert.equal(app.state.windows.A[row().window_start].closing_price, 25);
  assert.equal(app.state.advisories.A[row().window_start].closing_price, 25);
  assert.equal(app.state.feed[0].closing_price, 25);
});

test('a delayed REST snapshot cannot overwrite newer buffered SSE corrections', async () => {
  const app = await dashboard();
  select(app);
  let release;
  const response = new Promise((resolve) => { release = resolve; });
  app.ctx.fetch = async (url) => ({ ok: true, json: () =>
    url.pathname === '/api/ema-windows' ? response : Promise.resolve([]) });
  const pending = app.sync(true);
  app.push('window', row(25, '9007199254740993'));
  app.ctx.flush();
  release([row(15, '9007199254740992')]);
  await pending;
  assert.equal(app.state.windows.A[row().window_start].closing_price, 25);
});

test('older buffered events cannot overwrite a fresher snapshot', async () => {
  const app = await dashboard();
  select(app);
  snapshots(app, [row(25, '3')]);
  const pending = app.sync(true);
  app.push('window', row(15, '2'));
  await pending;
  assert.equal(app.state.windows.A[row().window_start].closing_price, 25);
});

test('resync discards pre-reset queued messages and in-flight snapshots', async () => {
  const app = await dashboard();
  select(app);
  let release;
  const old = new Promise((resolve) => { release = resolve; });
  app.ctx.fetch = async (url) => ({ ok: true, json: () =>
    url.pathname === '/api/ema-windows' ? old : Promise.resolve([]) });
  const pending = app.sync(true);
  app.push('window', row(15, '2'));
  snapshots(app);
  app.sync(true);
  release([row(15, '1')]);
  await pending;
  assert.equal(Object.keys(app.state.windows.A).length, 0);
});

test('a history 503 shows an error and retries without reconnecting SSE', async () => {
  const app = await dashboard();
  select(app);
  app.ctx.fetch = async (url) => ({
    ok: url.pathname !== '/api/ema-windows', status: 503, json: async () => [],
  });
  assert.equal(await app.sync(true), false);
  assert.match(app.node('status').textContent, /History unavailable/);
  const retry = [...app.timers.values()].find((timer) => timer.delay === 1000);
  assert.ok(retry, 'failed history must schedule a retry');
  snapshots(app, [row()]);
  retry.callback();
  await app.getPending();
  assert.equal(Object.keys(app.state.windows.A).length, 1);
});

test('a partial snapshot failure does not replace a successful cached chart', async () => {
  const app = await dashboard();
  select(app);
  snapshots(app, [row()]);
  await app.sync(true);
  app.ctx.fetch = async (url) => ({
    ok: url.pathname !== '/api/signals', status: 503, json: async () => [],
  });
  await app.sync(true);
  assert.equal(Object.keys(app.state.windows.A).length, 1);
});

test('late replies for a previous chart selection are discarded', async () => {
  const app = await dashboard();
  select(app);
  let release;
  const old = new Promise((resolve) => { release = resolve; });
  app.ctx.fetch = async (url) => ({ ok: true, json: () =>
    url.pathname === '/api/ema-windows' ? old : Promise.resolve([]) });
  const pending = app.sync(true);
  select(app, 'B');
  snapshots(app, [row(25, '2', 0, 'B')]);
  app.changed();
  release([row()]);
  await pending;
  assert.equal(app.state.windows.A, undefined);
  assert.equal(app.state.windows.B[row().window_start].symbol, 'B');
});

test('chart history and highlight keys are bounded; unfollow evicts caches', async () => {
  const app = await dashboard();
  select(app);
  for (let i = 0; i < app.HISTORY_MAX + 100; i++) {
    app.push('window', row(15, String(i + 1), i));
    app.push('advisory', row(15, String(i + 1), i));
  }
  app.ctx.flush();
  assert.equal(Object.keys(app.state.windows.A).length, app.HISTORY_MAX);
  assert.equal(Object.keys(app.state.advisories.A).length, app.HISTORY_MAX);
  assert.equal(app.state.windows.A[row().window_start], undefined);
  assert.ok(app.state.windows.A[row(15, '101', 100).window_start]);
  assert.ok(app.state.windows.A[row(15, '5100', app.HISTORY_MAX + 99).window_start]);
  assert.equal(app.state.feed.length, 300);
  assert.equal(app.state.feed[0].window_start, row(15, '5100', app.HISTORY_MAX + 99).window_start);
  assert.equal(app.getKnown().size, 300);
  app.state.watchlist = [];
  app.state.slot.A = null;
  app.changed();
  assert.equal(app.state.windows.A, undefined);
  assert.equal(app.state.advisories.A, undefined);
  assert.equal(app.state.feed.length, 0);
  assert.equal(app.getKnown().size, 0);
  await app.getPending();
});

test('closed EventSource messages cannot corrupt the current subscription', async () => {
  const app = await dashboard();
  select(app);
  app.changed();
  await app.getPending();
  const old = app.sources.at(-1);
  select(app, 'B');
  app.changed();
  await app.getPending();
  old.send('window', row(15, '1', 0, 'B'));
  assert.equal(app.getBuffer().size, 0);
});

test('transport open is not live until listener resync and snapshots succeed', async () => {
  const app = await dashboard();
  const es = app.sources.at(-1);
  es.onopen();
  assert.doesNotMatch(app.node('status').textContent, /^Live$/);
  es.send('resync');
  await app.getPending();
  assert.equal(app.node('status').textContent, 'Live');
  es.send('status', { state: 'reconnecting' });
  assert.equal(app.node('status').textContent, 'Reconnecting…');
});

test('a slow symbol response cannot restore a list cleared by a newer reset', async () => {
  const app = await dashboard();
  let release;
  const stale = new Promise((resolve) => { release = resolve; });
  app.ctx.fetch = async () => ({ ok: true, json: () => stale });
  const oldRequest = app.loadSymbols();
  snapshots(app);
  await app.loadSymbols();
  release([{ symbol: 'A', exchange: null }]);
  await oldRequest;
  assert.equal(app.state.symbols.length, 0);
});

test('client buffer overflow remains bounded and requests authoritative recovery', async () => {
  const app = await dashboard();
  select(app);
  let requests = 0;
  app.ctx.fetch = async () => {
    requests++;
    return { ok: true, json: async () => [] };
  };
  for (let i = 0; i < app.BUFFER_MAX + 1; i++) app.push('advisory', row(15, String(i + 1), i));
  assert.ok(app.getBuffer().size <= app.BUFFER_MAX);
  await app.getPending();
  assert.ok(requests > 0);
  assert.ok(app.state.feed.length <= 300);
});


test('followed uncharted symbols receive bounded quotes without full chart caches', async () => {
  const app = await dashboard();
  app.state.watchlist = ['A'];
  for (let i = 0; i < 30; i++) app.push('window', row(15 + i, String(i + 1), i));
  app.ctx.flush();
  assert.equal(Object.keys(app.state.quotes.A).length, 24);
  assert.equal(app.state.windows.A, undefined);
  assert.equal(app.state.quotes.A[row().window_start], undefined);
  const latest = row(50, '100', 29);
  app.push('window', latest);
  app.push('window', row(1, '99', 29));
  app.ctx.flush();
  assert.equal(app.state.quotes.A[latest.window_start].closing_price, 50);
  app.toggleWatch('A');
  assert.equal(app.state.quotes.A, undefined);
  await app.getPending();
});

test('watchlist quotes reconcile delayed snapshots and disappear on database reset', async () => {
  const app = await dashboard();
  app.state.watchlist = ['A'];
  let release;
  const stale = new Promise((resolve) => { release = resolve; });
  app.ctx.fetch = async (url) => ({ ok: true, json: () => url.pathname === '/api/quotes' ? stale : [] });
  const pending = app.sync(true);
  app.push('window', row(25, '9007199254740993'));
  release([row(15, '9007199254740992')]);
  await pending;
  assert.equal(app.state.quotes.A[row().window_start].closing_price, 25);
  snapshots(app);
  await app.sync(true);
  assert.equal(app.state.quotes.A, undefined);
});

test('price changes require a previous close and describe the available window comparison', async () => {
  const app = await dashboard();
  assert.equal(app.quoteInfo([row()]).percent, null);
  const info = app.quoteInfo([row(10), row(11, '2', 1)]);
  assert.equal(info.delta, 1);
  assert.equal(info.percent, 10);
  select(app);
  snapshots(app, [row(10), row(11, '2', 1)]);
  await app.sync(true);
  assert.match(app.panes[0].statsEl.innerHTML, /vs previous available window/);
  assert.match(app.panes[0].statsEl.innerHTML, /10.00%/);
});

test('opening a symbol follows it; closing comparison releases its chart cache', async () => {
  const app = await dashboard();
  snapshots(app);
  app.openSymbol('A');
  await app.getPending();
  assert.equal(app.state.slot.A, 'A');
  assert.ok(app.state.watchlist.includes('A'));
  app.addToComparison('B');
  await app.getPending();
  assert.equal(app.state.compare, true);
  assert.equal(app.node('comparisonPane').hidden, false);
  app.push('window', row(30, '1', 0, 'B'));
  app.ctx.flush();
  app.setCompare(false);
  assert.deepEqual([...app.state.comparison], ['A', 'B']);
  assert.equal(app.state.slot.A, 'A');
  assert.equal(app.state.windows.B, undefined);
  assert.ok(app.state.quotes.B);
  assert.equal(app.node('comparisonPane').hidden, true);
  await app.getPending();
});

test('theme preference updates chart colors and survives reload', async () => {
  const storage = { theme: 'light' };
  const app = await dashboard(storage);
  assert.equal(app.node('html').attrs['data-theme'], 'light');
  assert.equal(app.panes[0].plot.layout.paper_bgcolor, '#ffffff');
  app.node('themeToggle').handlers.click();
  assert.equal(storage.theme, 'dark');
  assert.equal(app.panes[0].plot.layout.paper_bgcolor, '#111620');
  const reloaded = await dashboard(storage);
  assert.equal(reloaded.state.theme, 'dark');
});

test('signal navigation focuses its window; toolbar reset and overlays work independently', async () => {
  const app = await dashboard();
  select(app);
  snapshots(app, [row(), row(16, '2', 1)]);
  await app.sync(true);
  const signal = { dataset: { symbol: 'A', time: row().window_start } };
  app.node('feedList').handlers.click({ target: { closest: () => signal } });
  await app.getPending();
  assert.equal(app.panes[0].focus, row().window_start);
  assert.equal(app.panes[0].plot.layout.xaxis.range[0], '2021-11-08T09:30:00.000Z');
  const revision = app.panes[0].plot.layout.uirevision;
  app.push('window', row(17, '3', 2)); app.ctx.flush();
  assert.equal(app.panes[0].plot.layout.uirevision, revision);
  const indicator = { dataset: { indicator: 'ema38' }, setAttribute() {} };
  app.node('paneA').handlers.click({ target: { closest: (selector) => selector.includes('indicator') ? indicator : null } });
  assert.equal(app.panes[0].plot.traces[1].visible, false);
  assert.equal(app.panes[0].plot.traces[2].visible, true);
  const reset = { dataset: { range: 'Reset' } };
  app.node('paneA').handlers.click({ target: { closest: (selector) => selector.includes('range') ? reset : null } });
  assert.equal(app.panes[0].focus, null);
  assert.equal(app.panes[0].plot.layout.xaxis.autorange, true);
});

test('mobile panels and signal collapse expose their selected state', async () => {
  const app = await dashboard();
  app.node('signalsTab').handlers.click();
  assert.equal(app.state.mobilePanel, 'signals');
  assert.equal(app.node('signalsTab').attrs['aria-pressed'], 'true');
  assert.equal(app.node('watchlistTab').attrs['aria-pressed'], 'false');
  app.node('signalsToggle').handlers.click();
  assert.equal(app.state.signalsVisible, false);
  assert.equal(app.node('signalsToggle').attrs['aria-expanded'], 'false');
});


test('initial startup opens a real symbol and restores the saved primary selection', async () => {
  const storage = { watchlist: ['A', 'B'], primarySymbol: 'B' };
  const app = await dashboard(storage);
  app.ctx.fetch = async (url) => ({ ok: true, json: async () => ({
    '/api/symbols': [{symbol:'A'}, {symbol:'B'}],
    '/api/quotes': [row(25, '1', 0, 'B')],
    '/api/ema-windows': [row(25, '1', 0, 'B')],
    '/api/trade-advisories': [], '/api/signals': [],
  })[url.pathname] });
  const bootRetry = [...app.timers.values()].find(timer => timer.delay === 3000);
  await bootRetry.callback();
  await app.getPending();
  assert.equal(app.state.slot.A, 'B');
  assert.equal(storage.primarySymbol, 'B');
  assert.match(app.panes[0].statsEl.innerHTML, /25.00/);
});

test('initial chart ranges collapse missing windows and remain stable through live updates', async () => {
  const app = await dashboard();
  select(app);
  snapshots(app, [row(10), row(11, '2', 100)]);
  await app.sync(true);
  const plot = app.panes[0].plot;
  assert.equal(plot.layout.xaxis.rangebreaks.length, 1);
  assert.equal(plot.layout.xaxis.rangebreaks[0].dvalue, 99 * 300000);
  const range = [...plot.layout.xaxis.range];
  const yRange = [...plot.layout.yaxis.range];
  app.push('window', row(12, '3', 101));
  app.ctx.flush();
  assert.deepEqual([...plot.layout.xaxis.range], range);
  assert.deepEqual([...plot.layout.yaxis.range], yRange);
});


function windowMap(...rows) {
  return Object.fromEntries(rows.map(r => [r.window_start, r]));
}
function near(actual, expected) { assert.ok(Math.abs(actual - expected) < 1e-8, `${actual} != ${expected}`); }

test('comparison normalizes differently priced symbols to the same shared baseline', async () => {
  const app = await dashboard();
  const data = app.comparisonData(['A', 'B'], {
    A: windowMap(row(10), row(11, '2', 1)),
    B: windowMap(row(100, '1', 0, 'B'), row(110, '2', 1, 'B')),
  });
  assert.equal(data.start, Date.parse(row().window_end));
  assert.equal(data.end, Date.parse(row(11, '2', 1).window_end));
  for (const s of data.series) { assert.equal(s.values[0], 0); near(s.percent, 10); }
});

test('comparison uses identical start and end closes despite unequal history', async () => {
  const app = await dashboard();
  const data = app.comparisonData(['A', 'B'], {
    A: windowMap(row(10), row(11, '2', 1), row(12, '3', 2), row(99, '4', 3)),
    B: windowMap(row(200, '1', 1, 'B'), row(220, '2', 2, 'B')),
  });
  assert.equal(data.start, Date.parse(row(11, '2', 1).window_end));
  assert.equal(data.end, Date.parse(row(12, '3', 2).window_end));
  near(data.series[0].percent, (12 / 11 - 1) * 100);
  near(data.series[1].percent, 10);
  assert.equal(data.series[0].last.closing_price, 12);
});

test('comparison explains missing or non-overlapping history without dropping selected symbols', async () => {
  const app = await dashboard();
  const missing = app.comparisonData(['A', 'B'], { A: windowMap(row()) });
  assert.equal(missing.series.length, 0);
  assert.match(missing.title, /Loading/);
  assert.equal(missing.availability.find(a => a.symbol === 'B').loading, true);
  const separate = app.comparisonData(['A', 'B'], {
    A: windowMap(row()), B: windowMap(row(25, '1', 1, 'B')),
  });
  assert.equal(separate.series.length, 0);
  assert.match(separate.title, /More price history/);
  assert.equal(separate.availability.length, 2);
});

test('individual missing closes remain null; only gaps shared by every symbol collapse', async () => {
  const app = await dashboard();
  const windows = {
    A: windowMap(row(10), row(11, '2', 1), row(12, '3', 2), row(13, '4', 3)),
    B: windowMap(row(100, '1', 0, 'B'), row(110, '2', 2, 'B'), row(120, '3', 3, 'B')),
  };
  const data = app.comparisonData(['A', 'B'], windows);
  assert.equal(data.series[1].values[1], null);
  assert.equal(data.series[1].prices[1], null);
  assert.equal(data.series[0].values.includes(null), false);
  assert.equal(data.breaks.length, 0);
  const shared = app.comparisonData(['A', 'B'], {
    A: windowMap(row(10), row(13, '2', 3)),
    B: windowMap(row(100, '1', 0, 'B'), row(120, '2', 3, 'B')),
  });
  assert.equal(shared.breaks.length, 1);
  assert.equal(shared.breaks[0].dvalue, 2 * 300000);
});

test('comparison ranges select a common baseline and keep it stable during live updates', async () => {
  const app = await dashboard();
  const windows = {
    A: windowMap(row(10), row(11, '2', 20), row(12, '3', 300)),
    B: windowMap(row(100, '1', 0, 'B'), row(110, '2', 20, 'B'), row(120, '3', 300, 'B')),
  };
  const day = app.comparisonData(['A', 'B'], windows, '1D');
  assert.equal(day.start, Date.parse(row(11, '2', 20).window_end));
  const all = app.comparisonData(['A', 'B'], windows, 'All');
  assert.equal(all.start, Date.parse(row().window_end));
  windows.A[row(13, '4', 400).window_start] = row(13, '4', 400);
  windows.B[row(130, '4', 400, 'B').window_start] = row(130, '4', 400, 'B');
  const anchored = app.comparisonData(['A', 'B'], windows, '1D', day.start);
  assert.equal(anchored.start, day.start);
  near(anchored.series[0].percent, (13 / 11 - 1) * 100);
});

test('zero or invalid prices cannot become a comparison baseline', async () => {
  const app = await dashboard();
  const data = app.comparisonData(['A', 'B'], {
    A: windowMap(row(0), row(10, '2', 1), row(11, '3', 2)),
    B: windowMap(row(100, '1', 0, 'B'), row(100, '2', 1, 'B'), row(110, '3', 2, 'B')),
  });
  assert.equal(data.start, Date.parse(row(10, '2', 1).window_end));
  near(data.series[0].percent, 10);
});

test('comparison limits selection to five, preserves colors, and never unfollows on removal', async () => {
  const app = await dashboard();
  snapshots(app);
  app.openSymbol('A');
  app.setCompare(true);
  for (const symbol of ['B', 'C', 'D', 'E']) app.toggleComparison(symbol);
  assert.equal(app.state.comparison.length, 5);
  const color = app.comparison.color('B');
  app.toggleComparison('F');
  assert.equal(app.state.comparison.includes('F'), false);
  assert.match(app.node('comparisonNotice').textContent, /Five symbols/);
  app.toggleComparison('A');
  app.toggleComparison('F');
  assert.equal(app.comparison.color('B'), color);
  assert.ok(app.state.watchlist.includes('A'));
  assert.ok(app.state.watchlist.includes('F'));
  app.toggleWatch('B');
  assert.equal(app.state.comparison.includes('B'), false);
  await app.getPending();
});

test('comparison requests only price windows and advances when all symbols share the new end', async () => {
  const app = await dashboard();
  app.state.watchlist = ['A', 'B'];
  app.state.slot.A = 'A';
  const requested = [];
  app.ctx.fetch = async (url) => {
    requested.push(url.pathname);
    const symbol = url.searchParams.get('symbol');
    return { ok: true, json: async () => url.pathname === '/api/ema-windows'
      ? [row(symbol === 'A' ? 10 : 100, '1', 0, symbol), row(symbol === 'A' ? 11 : 110, '2', 1, symbol)] : [] };
  };
  app.setCompare(true);
  await app.getPending();
  assert.equal(requested.includes('/api/trade-advisories'), false);
  assert.equal(app.comparison.data.series.length, 2);
  const before = app.comparison.data.end;
  app.push('window', row(12, '3', 2)); app.ctx.flush();
  assert.equal(app.comparison.data.end, before);
  app.push('window', row(120, '3', 2, 'B')); app.ctx.flush();
  assert.equal(app.comparison.data.end, Date.parse(row(12, '3', 2).window_end));
  near(app.comparison.data.series[0].percent, 20);
  app.push('window', row(1, '2', 2)); app.ctx.flush();
  near(app.comparison.data.series[0].percent, 20);
  assert.equal(app.comparison.plot.traces.filter(t => /EMA|Buy|Sell/.test(t.name)).length, 0);
  assert.match(app.node('comparisonTable').innerHTML, /20.00%/);
});

test('comparison can hide lines, restore them, and reset its range without changing selection', async () => {
  const app = await dashboard();
  select(app); snapshots(app, [row(10), row(11, '2', 1)]);
  app.setCompare(true); await app.getPending();
  const toggle = { dataset: { toggleSymbol: 'A' } };
  const click = { target: { closest: s => s.includes('toggle-symbol') ? toggle : null } };
  app.node('comparisonSymbols').handlers.click(click);
  assert.equal(app.comparison.plot.traces[0].visible, false);
  assert.equal(app.node('comparisonEmpty').hidden, false);
  app.node('comparisonSymbols').handlers.click(click);
  assert.equal(app.comparison.plot.traces[0].visible, true);
  app.comparison.manualRange = ['2021-11-08T10:00Z', '2021-11-08T10:05Z'];
  const range = { dataset: { range: '3D' } };
  app.node('comparisonRanges').handlers.click({ target: { closest: () => range } });
  assert.equal(app.comparison.range, '3D');
  assert.equal(app.comparison.manualRange, null);
  assert.deepEqual([...app.state.comparison], ['A']);
});

test('comparison selections survive reload and reject malformed saved preferences', async () => {
  const storage = { watchlist: ['A', 'B'], comparisonSymbols: ['A', 'B', 'A', null, 'MISSING'] };
  const app = await dashboard(storage);
  assert.deepEqual([...app.state.comparison], ['A', 'B']);
  app.setCompare(true); await app.getPending();
  assert.deepEqual(storage.comparisonSymbols, ['A', 'B']);
  const reloaded = await dashboard(storage);
  assert.deepEqual([...reloaded.state.comparison], ['A', 'B']);
  const invalid = await dashboard({ watchlist: 'A', comparisonSymbols: ['A'] });
  assert.equal(invalid.state.comparison.length, 0);
});


test('switching between views reloads the right histories even with the same single symbol', async () => {
  const app = await dashboard();
  select(app);
  const paths = [];
  app.ctx.fetch = async (url) => {
    paths.push(url.pathname);
    return { ok: true, json: async () => url.pathname === '/api/ema-windows' || url.pathname === '/api/trade-advisories' ? [row()] : [] };
  };
  app.setCompare(true); await app.getPending();
  assert.equal(paths.includes('/api/trade-advisories'), false);
  paths.length = 0;
  app.setCompare(false); await app.getPending();
  assert.equal(paths.includes('/api/trade-advisories'), true);
  assert.ok(app.state.advisories.A[row().window_start]);
  app.setCompare(true); await app.getPending();
  assert.equal(app.state.advisories.A, undefined);
});

test('removing a comparison symbol invalidates its delayed snapshot and evicts its history', async () => {
  const app = await dashboard();
  app.state.watchlist = ['A', 'B']; app.state.slot.A = 'A';
  let release;
  const delayed = new Promise(resolve => { release = resolve; });
  app.ctx.fetch = async (url) => ({ ok: true, json: () => url.pathname === '/api/ema-windows' ? delayed : [] });
  app.setCompare(true);
  const pending = app.getPending();
  snapshots(app, [row(12, '2'), row(13, '3', 1)]);
  app.toggleComparison('B');
  release([row(10)]);
  await pending;
  assert.equal(app.state.windows.B, undefined);
  assert.equal(app.comparison.data.series.length, 1);
  assert.equal(app.comparison.data.series[0].last.closing_price, 13);
  assert.ok(app.state.watchlist.includes('B'));
});




test('a one-price symbol cannot blank the comparison between two compatible symbols', async () => {
  const app = await dashboard();
  const data = app.comparisonData(['120071.ETR', '121806.ETR', '120470.ETR'], {
    '120071.ETR': windowMap(row(19.68, '1', 0, '120071.ETR'), row(19.64, '2', 1, '120071.ETR'), row(19.36, '3', 2, '120071.ETR')),
    '121806.ETR': windowMap(row(24.2, '1', 10, '121806.ETR')),
    '120470.ETR': windowMap(row(.3371, '1', 0, '120470.ETR'), row(.333, '2', 1, '120470.ETR')),
  });
  assert.deepEqual([...data.series.map(s => s.symbol)], ['120071.ETR', '120470.ETR']);
  const unavailable = data.availability.find(a => a.symbol === '121806.ETR');
  assert.equal(unavailable.count, 1);
  assert.equal(unavailable.compared, false);
  assert.equal(unavailable.reason, 'Needs another price');
  assert.equal(data.start, Date.parse(row().window_end));
  assert.equal(data.end, Date.parse(row(15, '2', 1).window_end));
});

test('largest compatible group wins; equal groups prefer the newest period', async () => {
  const app = await dashboard();
  const windows = {
    A: windowMap(row(10), row(11, '2', 1)),
    B: windowMap(row(10, '1', 0, 'B'), row(11, '2', 1, 'B')),
    C: windowMap(row(10, '1', 0, 'C'), row(11, '2', 1, 'C')),
    D: windowMap(row(10, '1', 10, 'D'), row(11, '2', 11, 'D')),
    E: windowMap(row(10, '1', 10, 'E'), row(11, '2', 11, 'E')),
  };
  const largest = app.comparisonData(['A', 'B', 'C', 'D', 'E'], windows);
  assert.deepEqual([...largest.series.map(s => s.symbol)], ['A', 'B', 'C']);
  const recent = app.comparisonData(['A', 'B', 'D', 'E'], windows);
  assert.deepEqual([...recent.series.map(s => s.symbol)], ['D', 'E']);
});

test('disjoint histories explain availability instead of claiming the chart is still waiting', async () => {
  const app = await dashboard();
  const data = app.comparisonData(['A', 'B'], {
    A: windowMap(row(10), row(11, '2', 1)),
    B: windowMap(row(10, '1', 10, 'B'), row(11, '2', 11, 'B')),
  });
  assert.equal(data.series.length, 0);
  assert.equal(data.availability.length, 2);
  assert.match(data.message, /available.*dates|dates.*below/i);
  assert.doesNotMatch(data.title, /Waiting|timestamp/);
});

test('a single shared observation never produces a fabricated 0% return', async () => {
  const app = await dashboard();
  const data = app.comparisonData(['A', 'B'], {
    A: windowMap(row(10), row(11, '2', 1)),
    B: windowMap(row(100, '1', 1, 'B'), row(110, '2', 2, 'B')),
  });
  assert.equal(data.series.length, 0);
  assert.equal(data.availability.every(a => !a.compared), true);
});

test('a selected period with one shared close offers all loaded history as a recovery action', async () => {
  const app = await dashboard();
  const windows = {
    A: windowMap(row(10), row(11, '2', 600)),
    B: windowMap(row(100, '1', 0, 'B'), row(110, '2', 600, 'B')),
  };
  const day = app.comparisonData(['A', 'B'], windows, '1D');
  assert.equal(day.series.length, 0);
  assert.equal(day.suggestAll, true);
  const all = app.comparisonData(['A', 'B'], windows, 'All');
  assert.equal(all.series.length, 2);
  near(all.series[0].percent, 10);
});

test('partial comparison lists excluded history, keeps selections, and can keep only comparable symbols', async () => {
  const app = await dashboard();
  app.state.watchlist = ['A', 'B', 'C']; app.state.comparison = ['A', 'B', 'C'];
  app.state.compare = true;
  app.state.windows = {
    A: windowMap(row(10), row(11, '2', 1)),
    B: windowMap(row(24.2, '1', 10, 'B')),
    C: windowMap(row(100, '1', 0, 'C'), row(110, '2', 1, 'C')),
  };
  app.comparison.update();
  assert.equal(app.node('comparisonEmpty').hidden, true);
  assert.match(app.node('comparisonCoverageText').textContent, /Showing 2 of 3/);
  assert.match(app.node('comparisonTable').innerHTML, /Needs another price/);
  assert.match(app.node('comparisonTable').innerHTML, /1 price/);
  assert.equal(app.state.comparison.length, 3);
  snapshots(app);
  app.node('comparisonUseAvailable').handlers.click();
  assert.deepEqual([...app.state.comparison], ['A', 'C']);
  assert.ok(app.state.watchlist.includes('B'));
  await app.getPending();
});

test('an excluded symbol rejoins automatically once two matching closes arrive', async () => {
  const app = await dashboard();
  app.state.watchlist = ['A', 'B', 'C']; app.state.comparison = ['A', 'B', 'C']; app.state.compare = true;
  app.state.windows = {
    A: windowMap(row(10), row(11, '2', 1)),
    B: windowMap(row(100, '1', 0, 'B')),
    C: windowMap(row(1000, '1', 0, 'C'), row(1100, '2', 1, 'C')),
  };
  app.comparison.update();
  assert.equal(app.comparison.data.series.length, 2);
  app.push('window', row(110, '2', 1, 'B')); app.ctx.flush();
  assert.equal(app.comparison.data.series.length, 3);
  assert.equal(app.node('comparisonCoverage').hidden, true);
  assert.equal(app.state.comparison.length, 3);
});
