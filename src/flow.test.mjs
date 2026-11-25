// engine/l3/flow.test.mjs — the load harness's order flow (engine/l3/miner/flow.js).
//
// A generator is a measuring instrument, so these tests are about whether the instrument reads true: that the
// books it builds are the books the live maker builds (same arithmetic, same tick, same skew), that the resting
// set is bounded by the grid rather than by luck, that fills come from the matcher and move the inventory that
// moves the next quote, and that two runs of one seed are the same run. The ratchet test is the one that caught a
// real bug: a maker handed its own best bid as "somebody else's" pulls a tick inside itself at every re-quote.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createFlow, flowsFor, rng, softmax, toLogits } from './miner/flow.js';

const E = 10n ** 18n;
const wei = (x) => BigInt(Math.round(x * 1e6)) * 10n ** 12n;

/// drive a flow for n calls, signing every quote the way the soak worker does
function drive(f, n, { t0 = 1700000000000, step = 20 } = {}) {
  let t = t0; const ops = [];
  for (let i = 0; i < n; i++) {
    const op = f.next(t);
    if (op) {
      ops.push(op);
      if (op.t === 'add' && op.kind !== 'take') { op.hash = '0x' + String(ops.length).padStart(64, '0'); f.acked(op.ref, op.hash); }
    }
    t += step;
  }
  return { ops, t };
}
/// the resting book the matcher would hold, from the quotes the flow placed (what sync() is handed)
function bookOf(ops, outcomes = 2) {
  const live = new Map();
  for (const op of ops) {
    if (op.t === 'cancel') live.delete(op.hash);
    else if (op.kind !== 'take') live.set(op.hash, op);
  }
  const books = Array.from({ length: outcomes }, () => []);
  let i = 0;
  for (const [hash, op] of live) books[op.outcome].push({ hash, price: wei(op.price), remaining: wei(op.size), buy: op.buy, seq: i++ });
  return books;
}

test('a maker quotes two sides around the fair, on the tick, and never crossed', () => {
  const f = createFlow({ market: '1', fair: [0.62, 0.38], makers: 1, levels: 1, takers: 0, seed: 3, tick: 0.01 });
  const { ops } = drive(f, 8);
  const quotes = ops.filter((o) => o.kind === 'quote');
  assert.ok(quotes.length >= 4, `two sides × two outcomes: ${quotes.length}`);
  for (const q of quotes) {
    assert.equal(Math.round(q.price * 1000) % 10, 0, `${q.price} is on the 1c tick`);
    assert.ok(q.price > 0 && q.price < 1, `${q.price} inside (0,1)`);
    assert.equal(q.postOnly, true, 'a maker never takes');
    assert.equal(q.ioc, false);
  }
  for (let o = 0; o < 2; o++) {
    const bid = Math.max(...quotes.filter((q) => q.outcome === o && q.buy).map((q) => q.price));
    const ask = Math.min(...quotes.filter((q) => q.outcome === o && !q.buy).map((q) => q.price));
    assert.ok(bid < ask, `outcome ${o}: bid ${bid} under ask ${ask}`);
    const fair = f.fairs()[o];
    assert.ok(bid < fair && ask > fair, `outcome ${o}: the fair ${fair} sits inside ${bid}/${ask}`);
  }
});

/// the cumulative resting book, the way the matcher holds it — see the note on bookOf: feeding sync() one round's
/// ops at a time makes every un-requoted order look filled
function roundsOf(f, rounds, { per = 6, step = 40, gap = 200, outcomes = 2, t0 = 1700000000000 } = {}) {
  let t = t0; const all = []; const seen = [];
  for (let round = 0; round < rounds; round++) {
    const mine = [];
    for (let i = 0; i < per; i++) {
      const op = f.next(t);
      if (op) { if (op.t === 'add' && op.kind !== 'take') { op.hash = `0x${round}_${i}`.padEnd(66, '0'); f.acked(op.ref, op.hash); } all.push(op); mine.push(op); }
      t += step;
    }
    const books = bookOf(all, outcomes);
    f.sync(books);
    seen.push({ round, ops: mine, books, live: books.reduce((n, rows) => n + rows.length, 0) });
    t += gap;
  }
  return seen;
}

test('the touch does not ratchet: a maker is not pulled inside its own resting quote', () => {
  // The bug this exists for: pass the whole book's best as `others` and quoteMath pulls the new bid one tick
  // under the maker's OWN best bid, every re-quote, until the ladder crosses. With a still fair the re-quoted
  // price must be the same price.
  const f = createFlow({ market: '1', fair: [0.5, 0.5], makers: 1, levels: 1, takers: 0, volPerSec: 0, requoteMs: 100, seed: 5 });
  const seenBids = new Set(), seenAsks = new Set();
  for (const r of roundsOf(f, 6)) for (const o of r.ops) if (o.kind === 'quote' && o.outcome === 0) (o.buy ? seenBids : seenAsks).add(o.price);
  assert.equal(seenBids.size, 1, `one bid price across six re-quotes, saw ${[...seenBids].join(',')}`);
  assert.equal(seenAsks.size, 1, `one ask price across six re-quotes, saw ${[...seenAsks].join(',')}`);
});

test('a fill moves the inventory, and the inventory skews the next quote down', () => {
  const f = createFlow({ market: '1', outcomes: 2, fair: [0.5, 0.5], makers: 1, levels: 1, size: 10, skewMax: 0.02,
    takers: 0, volPerSec: 0, requoteMs: 1, seed: 11, tick: 0.01 });
  const a = drive(f, 6);
  const first = a.ops.filter((o) => o.kind === 'quote' && o.outcome === 0);
  const bid0 = Math.max(...first.filter((o) => o.buy).map((o) => o.price));
  // the book comes back with the maker's outcome-0 bid gone — it was lifted
  const books = bookOf(a.ops);
  const kept = books.map((rows, o) => (o === 0 ? rows.filter((r) => !r.buy) : rows));
  f.sync(kept);
  const st = f.stat();
  assert.ok(st.fills >= 1, `the vanished quote is a fill: ${st.fills}`);
  assert.ok(st.inventoryMax > 0, `long after buying: ${JSON.stringify(st.inventory)}`);
  const b = drive(f, 6, { t0: a.t + 5000 });
  const bid1 = Math.max(...b.ops.filter((o) => o.kind === 'quote' && o.outcome === 0 && o.buy).map((o) => o.price));
  assert.ok(bid1 < bid0, `long → quote lower: ${bid1} < ${bid0}`);
});

test('partial fills shrink the tracked size and move the inventory by the delta only', () => {
  const f = createFlow({ market: '1', fair: [0.5, 0.5], makers: 1, levels: 1, size: 10, takers: 0, volPerSec: 0, seed: 2 });
  const { ops } = drive(f, 6);
  const books = bookOf(ops);
  for (const rows of books) for (const r of rows) if (r.buy) r.remaining = r.remaining / 2n;   // half of every bid taken
  f.sync(books);
  const st = f.stat();
  assert.ok(st.filledSize > 0 && st.filledSize < 40, `a partial fill, not the whole ladder: ${st.filledSize}`);
  const again = f.stat();
  f.sync(books);                                     // the same book twice must not fill twice
  assert.equal(f.stat().filledSize, again.filledSize, 'sync is idempotent on an unchanged book');
});

test('the resting set is bounded by the grid, whatever the rate', () => {
  const f = createFlow({ market: '1', outcomes: 3, fair: [0.5, 0.3, 0.2], makers: 3, levels: 2, takers: 0.5, seed: 9 });
  const grid = f.stat().grid;
  assert.equal(grid, 3 * 3 * 2 * 2);
  const rounds = roundsOf(f, 10, { per: 200, step: 5, gap: 0, outcomes: 3 });
  for (const r of rounds) assert.ok(r.live <= grid, `round ${r.round}: ${r.live} resting ≤ grid ${grid}`);
  assert.ok(rounds[rounds.length - 1].live > 0, 'and it is not empty either');
});

test('takers cross the touch, are IOC, and are not the makers', () => {
  const f = createFlow({ market: '1', fair: [0.5, 0.5], makers: 2, levels: 1, takers: 0.9, informed: 1, seed: 4 });
  const a = drive(f, 10);
  f.sync(bookOf(a.ops));
  const b = drive(f, 60, { t0: a.t });
  const takes = b.ops.filter((o) => o.kind === 'take');
  assert.ok(takes.length > 0, 'somebody took');
  const touch = { bid: Math.max(...bookOf(a.ops)[0].filter((r) => r.buy).map((r) => Number(r.price) / 1e18)), ask: Math.min(...bookOf(a.ops)[0].filter((r) => !r.buy).map((r) => Number(r.price) / 1e18)) };
  for (const tk of takes) {
    assert.equal(tk.ioc, true, 'a taker never rests');
    assert.equal(tk.postOnly, false);
    assert.ok(tk.userIndex >= f.makers, 'a taker is not one of the makers');
    if (tk.outcome === 0) {
      if (tk.buy) assert.ok(tk.price >= touch.ask - 1e-9, `a buy at ${tk.price} reaches the ask ${touch.ask}`);
      else assert.ok(tk.price <= touch.bid + 1e-9, `a sell at ${tk.price} reaches the bid ${touch.bid}`);
    }
  }
});

test('a fair move of a tick or more re-quotes; a smaller one does not', () => {
  const still = createFlow({ market: '1', fair: [0.5, 0.5], makers: 1, levels: 1, takers: 0, volPerSec: 0, requoteMs: 1e9, seed: 6 });
  const a = drive(still, 6);
  const b = drive(still, 6, { t0: a.t + 60000 });
  assert.equal(b.ops.filter((o) => o.kind === 'quote').length, 0, 'a still fair and no TTL: nothing to do');
  const moving = createFlow({ market: '1', fair: [0.5, 0.5], makers: 1, levels: 1, takers: 0, volPerSec: 0, requoteMs: 1e9, seed: 6 });
  drive(moving, 6);
  moving.setFair([0.62, 0.38]);
  const c = drive(moving, 10, { t0: 1700000060000 });
  assert.ok(c.ops.some((o) => o.t === 'cancel'), 'the old quote is pulled');
  assert.ok(c.ops.some((o) => o.kind === 'quote'), 'and replaced');
});

test('a cancel is followed by its own replacement, not by the next level', () => {
  const f = createFlow({ market: '1', fair: [0.5, 0.5], makers: 1, levels: 3, takers: 0, volPerSec: 0, requoteMs: 1e9, seed: 8 });
  drive(f, 12);
  f.setFair([0.7, 0.3]);
  const { ops } = drive(f, 2, { t0: 1700000060000 });
  assert.equal(ops[0].t, 'cancel');
  assert.equal(ops[1].t, 'add', 'the replacement comes next, so the level is never left empty for a whole grid walk');
});

test('an unpriceable market gets a floor quote, not a price claim; a closing one gets nothing', () => {
  const floor = createFlow({ market: '1', fair: [0.5, 0.5], confidence: 0.05, quoteMinConf: 0.12, floorHalf: 0.1,
    makers: 1, levels: 1, takers: 0, volPerSec: 0, seed: 1, tick: 0.01 });
  const { ops } = drive(floor, 6);
  const fl = ops.filter((o) => o.kind === 'floor');
  assert.ok(fl.length >= 2, `the floor quote is what a maker shows when it cannot price the market: ${JSON.stringify(ops.map((o) => o.kind))}`);
  const bid = Math.max(...fl.filter((o) => o.outcome === 0 && o.buy).map((o) => o.price));
  const ask = Math.min(...fl.filter((o) => o.outcome === 0 && !o.buy).map((o) => o.price));
  assert.ok(ask - bid >= 0.1, `and it is wide: ${bid}/${ask}`);
  assert.ok(floor.stat().floors >= 2);

  const closing = createFlow({ market: '1', fair: [0.5, 0.5], endsAt: 1700000030000, closeGuardMs: 60000,
    makers: 1, levels: 1, takers: 0, seed: 1 });
  const c = drive(closing, 6);
  assert.equal(c.ops.filter((o) => o.t === 'add').length, 0, 'inside the close guard a maker shows nothing at all');
});

test('the fair walks but stays a probability distribution', () => {
  const f = createFlow({ market: '1', outcomes: 3, fair: [0.5, 0.3, 0.2], volPerSec: 0.5, makers: 1, levels: 1, seed: 21 });
  let t = 1700000000000;
  for (let i = 0; i < 2000; i++) { f.next(t); t += 100; }
  const ps = f.fairs();
  assert.equal(ps.length, 3);
  for (const p of ps) assert.ok(p > 0 && p < 1, `${p} is a probability`);
  assert.ok(Math.abs(ps.reduce((a, b) => a + b, 0) - 1) < 1e-3, `the outcomes sum to 1: ${ps.join('+')}`);
  assert.notDeepEqual(ps, [0.5, 0.3, 0.2], 'and it actually moved');
});

test('one seed is one run', () => {
  const sig = (seed) => {
    const f = createFlow({ market: '1', fair: [0.55, 0.45], makers: 2, levels: 2, takers: 0.4, volPerSec: 0.3, seed });
    const { ops } = drive(f, 120, { step: 200 });
    return ops.map((o) => `${o.t}${o.kind}${o.outcome}${o.buy ? 1 : 0}${o.price ?? ''}${o.size ?? ''}`).join('|') + '#' + f.fairs().join(',');
  };
  assert.equal(sig(42), sig(42), 'same seed, same ops');
  assert.notEqual(sig(42), sig(43), 'different seed, different ops');
});

test('stat reports the book, not only the pipeline', () => {
  const f = createFlow({ market: '1', fair: [0.6, 0.4], makers: 2, levels: 2, takers: 0, volPerSec: 0, seed: 13, tick: 0.01 });
  const { ops } = drive(f, 20);
  f.sync(bookOf(ops));
  const st = f.stat();
  assert.ok(st.spreadCents > 0 && st.spreadCents < 20, `a real touch spread: ${st.spreadCents}c`);
  assert.ok(st.depthAtTouch > 0, `depth at the touch: ${st.depthAtTouch}`);
  assert.ok(st.fairDevCents != null && st.fairDevCents < 5, `the mid sits near the fair: ${st.fairDevCents}c`);
  assert.equal(st.grid, 2 * 2 * 2 * 2);
  assert.ok(st.placed >= 8);
});

test('boost scales the grid and keeps it two-sided', () => {
  const one = createFlow({ market: '1', makers: 2, levels: 2, boost: 1 }).stat().grid;
  const four = createFlow({ market: '1', makers: 2, levels: 2, boost: 4 }).stat().grid;
  assert.equal(one, 16);
  assert.equal(four, 64, 'boost 4 → makers ×2 and levels ×2');
});

test('flowsFor maps the snapshot: one flow a market, quoted around ITS fair', () => {
  const fs = flowsFor({ markets: ['19085', '21249'], mids: [52, 7], makers: 1, levels: 1, seed: 1 });
  assert.equal(fs.length, 2);
  assert.ok(Math.abs(fs[0].fairs()[0] - 0.52) < 0.01, `52c → ${fs[0].fairs()[0]}`);
  assert.ok(Math.abs(fs[1].fairs()[0] - 0.07) < 0.01, `7c → ${fs[1].fairs()[0]}`);
  const a = drive(fs[0], 4).ops.filter((o) => o.kind === 'quote');
  const b = drive(fs[1], 4).ops.filter((o) => o.kind === 'quote');
  assert.notEqual(a[0].price, b[0].price, 'two markets at different prices quote at different prices');
  // a cheap outcome must still clear the book's minimum notional
  for (const q of b) assert.ok(q.price * q.size >= 0.01 - 1e-9, `${q.size} @ ${q.price} clears the 0.01 minimum`);
});

test('missing mids are reported by being 1/n, not by looking like a price', () => {
  const [f] = flowsFor({ markets: ['1'], mids: [], outcomes: 2, makers: 1, levels: 1 });
  assert.deepEqual(f.fairs(), [0.5, 0.5]);
});

test('the primitives: a seeded stream, a simplex, and logits that round-trip', () => {
  const r1 = rng(7), r2 = rng(7);
  assert.equal(r1(), r2());
  const ps = softmax(toLogits([0.2, 0.3, 0.5]));
  assert.ok(Math.abs(ps[0] - 0.2) < 1e-3 && Math.abs(ps[2] - 0.5) < 1e-3, ps.join(','));
  assert.ok(Math.abs(softmax([5, 5]).reduce((a, b) => a + b, 0) - 1) < 1e-6);
  assert.ok(softmax([-1e9, 0])[0] > 0, 'clamped off zero, so a logit can always come back');
});

test('the generator does not walk the grid for nothing: a settled book costs one taker a call', () => {
  // If the full grid were walked on every call, this would be 400 × makers × outcomes × 2 × levels runs of
  // quoteMath inside the load generator — error in the instrument, reported as the venue being slow.
  const f = createFlow({ market: '1', fair: [0.5, 0.5], makers: 4, levels: 3, takers: 0.5, volPerSec: 0,
    requoteMs: 1e9, seed: 17 });
  const a = roundsOf(f, 1, { per: 60, step: 1, gap: 0 });
  assert.ok(a[0].live > 0, 'quoted first');
  const before = f.stat().gridWalks;
  let t = 1700000100000;
  for (let i = 0; i < 400; i++) { f.next(t); t += 1; }
  const st = f.stat();
  assert.ok(st.gridWalks - before <= 2, `a settled book: ${st.gridWalks - before} grid passes in 400 calls`);
  // the other calls took the taker branch before ever reaching the grid, so they are not counted as skips:
  // what matters is that no call did the full pass, and that the gate is the reason
  assert.ok(st.gridSkips > 150, `the gate refused the pass on the calls that reached it: ${st.gridSkips}`);
  assert.ok(st.taken > 150, `and those calls traded instead: ${st.taken}`);
  f.setFair([0.7, 0.3]);
  f.next(t + 1);
  assert.ok(f.stat().gridWalks > st.gridWalks, 'and a moved fair wakes it up again');
});
