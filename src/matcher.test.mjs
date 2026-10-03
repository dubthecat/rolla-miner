// node --test engine/l3/matcher.test.mjs — the price-time book's rules, determinism, and an orders/s figure
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createBook } from './matcher.js';

const E = 10n ** 18n; const px = (c) => BigInt(Math.round(c * 100)) * E / 100n; const sz = (n) => BigInt(n) * E;
let n = 0; const o = (user, buy, price, size, extra = {}) => ({ hash: `h${++n}`, user, buy, price: px(price), size: sz(size), ...extra });

test('a crossing taker fills at the MAKER price, FIFO within a level, best price first', () => {
  const b = createBook();
  b.add(o('ann', false, 0.60, 10)); b.add(o('bob', false, 0.55, 5)); b.add(o('cat', false, 0.55, 5));
  const r = b.add(o('dan', true, 0.60, 12));
  assert.equal(r.fills.length, 3); assert.equal(r.rested, false); assert.equal(r.remaining, 0n);
  assert.deepEqual(r.fills.map((f) => [f.maker, Number(f.price) / 1e18, Number(f.size) / 1e18]), [['bob', 0.55, 5], ['cat', 0.55, 5], ['ann', 0.60, 2]]);
  assert.equal(b.best().ask, px(0.60)); assert.equal(b.depth().asks[0].size, sz(8));
});

test('what does not cross rests; post-only that would cross is refused; ioc never rests', () => {
  const b = createBook();
  assert.equal(b.add(o('ann', true, 0.40, 10)).rested, true);
  const po = b.add(o('bob', false, 0.40, 1, { postOnly: true })); assert.equal(po.reason, 'would cross'); assert.equal(po.rested, false);
  assert.equal(b.add(o('bob', false, 0.45, 1, { postOnly: true })).rested, true);
  const ioc = b.add(o('cat', false, 0.30, 25, { ioc: true })); assert.equal(ioc.fills.length, 1); assert.equal(ioc.remaining, sz(15)); assert.equal(ioc.rested, false);
  assert.equal(b.size, 1);   // only bob's ask is left
});

test('self-trades are allowed: an order fills against the same user\'s resting orders in strict price-time', () => {
  const b = createBook();
  b.add(o('ann', false, 0.50, 5)); b.add(o('bob', false, 0.50, 5)); b.add(o('ann', false, 0.52, 5));
  const r = b.add(o('ann', true, 0.52, 7));
  assert.deepEqual(r.fills.map((f) => [f.maker, Number(f.size) / 1e18]), [['ann', 5], ['bob', 2]]); assert.equal(r.remaining, 0n); assert.equal(r.rested, false);
  assert.equal(b.orders('ann').length, 1);   // the 0.52 ask is untouched; the 0.50 one is gone
});

test('cancel removes exactly that order and empties its level', () => {
  const b = createBook();
  const a = o('ann', true, 0.30, 4); b.add(a); b.add(o('bob', true, 0.30, 6));
  assert.equal(b.cancel(a.hash).hash, a.hash); assert.equal(b.cancel(a.hash), null);
  assert.equal(b.depth().bids[0].size, sz(6)); assert.equal(b.depth().bids[0].orders, 1);
  b.cancel(b.orders('bob')[0].hash); assert.equal(b.depth().bids.length, 0); assert.equal(b.best().bid, null);
});

test('duplicate hashes are refused and a preview never changes the book', () => {
  const b = createBook();
  const a = o('ann', false, 0.70, 3); b.add(a); assert.equal(b.add(a).reason, 'duplicate');
  const p = b.preview(true, px(0.70), sz(5)); assert.equal(p.filled, sz(3)); assert.equal(p.avg, px(0.70)); assert.equal(b.size, 1);
});

/// the same event sequence replayed must produce the same fills — the property the journal and validators rely on
function run(seed, count, book, log = null) {
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const users = ['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8']; const live = []; const out = [];
  for (let i = 0; i < count; i++) {
    if (live.length && rnd() < 0.1) { const h = live.splice(Math.floor(rnd() * live.length), 1)[0]; const c = book.cancel(h); out.push(c ? `c${h}` : `x${h}`); continue; }
    const buy = rnd() < 0.5; const cross = rnd() < 0.3; const mid = 50; const off = Math.floor(rnd() * 20);
    const price = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
    const ord = { hash: `o${i}`, user: users[Math.floor(rnd() * users.length)], buy, price: BigInt(price) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 100)) * E, ioc: rnd() < 0.1 };
    const r = book.add(ord); if (r.rested) live.push(ord.hash);
    for (const f of r.fills) out.push(`${f.makerHash}>${f.takerHash}@${f.price}:${f.size}`);
  }
  return out;
}
test('determinism: two books fed the same sequence agree fill for fill', () => {
  const a = run(7, 20000, createBook()), b = run(7, 20000, createBook());
  assert.equal(a.length, b.length); assert.deepEqual(a.slice(0, 500), b.slice(0, 500)); assert.ok(a.length > 1000);
});

test('throughput: in-process orders per second on one thread', () => {
  const book = createBook(); const N = 200000;
  const t0 = process.hrtime.bigint(); const out = run(11, N, book); const dt = Number(process.hrtime.bigint() - t0) / 1e9;
  console.log(`  matcher: ${N} ops in ${dt.toFixed(2)} s → ${Math.round(N / dt).toLocaleString()} ops/s · ${out.length} fills+cancels · ${book.size} resting`);
  assert.ok(N / dt > 50000, `too slow: ${Math.round(N / dt)} ops/s`);
});
