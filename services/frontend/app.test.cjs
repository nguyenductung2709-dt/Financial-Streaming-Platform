const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Exercise the actual dashboard with deterministic DOM, network, and timers.
async function dashboard() {
  const nodes = new Map();
  const timers = new Map();
  const sources = [];
  let timerId = 0;
  function node(id) {
    if (!nodes.has(id)) nodes.set(id, {
      innerHTML: '', textContent: '', hidden: false,
      addEventListener() {}, querySelector: (selector) => node(id + selector),
      style: { setProperty() {} }, classList: { toggle() {} },
    });
    return nodes.get(id);
  }
  const ctx = {
    URL, URLSearchParams, AbortSignal,
    location: { protocol: 'http:', origin: 'http://localhost:8000' },
    document: { documentElement: node('html'), getElementById: node, querySelectorAll: () => [] },
    localStorage: { getItem: () => null, setItem() {} },
    Plotly: { react() {} },
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
      getPending: () => syncPromise, getBuffer: () => buffer,
      HISTORY_MAX, BUFFER_MAX};
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
    '/api/signals': feed, '/api/symbols': [],
  })[url.pathname] });
}

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
