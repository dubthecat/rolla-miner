// node --test engine/l3/restore.test.mjs — a book rebuilt from a snapshot must be the SAME book.
//
// A miner catching up from the log re-verifies every signature in it: 19,098 orders on one live shard, ~6.9
// minutes across 73 of them, and hours once the log holds millions. A checkpoint is how a miner — or a brand-new
// one, which has no journal at all — gets back to a book it already had without paying that again.
//
// "Same" is a strong word here and it is the whole test. The state commitment's leaves are keyed on (side, price,
// seq), so a book holding identical orders with fresh sequence numbers commits to a DIFFERENT root, and the miner
// would dissent on the next epoch boundary over a book that is actually correct. So: same depth, same FIFO order,
// same bookHash, and matching from there produces the same fills.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createBook } from './matcher.js';
import { bookHashOf } from './miner/merkle.js';

const P = (x) => BigInt(Math.round(x * 1e18));
const mk = (i, buy, price, size = 5) => ({ hash: '0x' + String(i).padStart(64, '0'), user: '0x' + String(i % 3).repeat(40), buy, price: P(price), size: P(size) });

/// a book with a mix of levels, partial fills and cancels — not a tidy one
function busy() {
  const b = createBook();
  let i = 1;
  for (const [buy, price] of [[true, 0.40], [true, 0.40], [true, 0.39], [true, 0.41], [false, 0.60], [false, 0.60], [false, 0.61]]) b.add(mk(i++, buy, price));
  b.add(mk(i++, false, 0.40, 3));                 // crosses the 0.40 bids: one fills out, one is left partial
  const live = b.orders();
  b.cancel(live.find((o) => o.price === P(0.39)).hash);
  b.add(mk(i++, true, 0.42));
  return b;
}

test('a restored book has the same depth, the same FIFO and the same commitment', () => {
  const a = busy();
  const snap = a.orders();
  const b = createBook();
  assert.equal(b.restore(snap), snap.length);
  assert.equal(b.size, a.size);
  assert.deepEqual(b.orders(), a.orders(), 'the orders differ, field for field');
  assert.deepEqual(b.depth(10), a.depth(10), 'the ladder differs');
  assert.deepEqual(b.best(), a.best());
  assert.equal(bookHashOf(b.orders()), bookHashOf(a.orders()), 'the state commitment differs');
  // and the FIFO inside a level is price-time, not insertion-of-the-snapshot
  const lvl = b.orders().filter((o) => o.buy && o.price === P(0.40)).map((o) => o.seq);
  assert.deepEqual(lvl, [...lvl].sort((x, y) => x - y));
});

test('matching on from a restored book gives the same fills as on the original', () => {
  const a = busy(), b = createBook();
  b.restore(a.orders());
  const taker = (bk, i) => bk.add(mk(900 + i, false, 0.40, 12));
  const ra = taker(a, 1), rb = taker(b, 1);
  assert.equal(rb.fills.length, ra.fills.length);
  assert.deepEqual(rb.fills.map((f) => [f.makerHash, String(f.size), String(f.price)]), ra.fills.map((f) => [f.makerHash, String(f.size), String(f.price)]));
  assert.equal(String(rb.remaining), String(ra.remaining));
  assert.equal(bookHashOf(b.orders()), bookHashOf(a.orders()), 'the books diverged after one more trade');
});

test('a restored book numbers its next order after the snapshot, never on top of it', () => {
  const a = busy(), b = createBook();
  const top = Math.max(...a.orders().map((o) => o.seq));
  b.restore(a.orders());
  const r = b.add(mk(777, true, 0.30));
  assert.ok(r.seq > top, `the next order took seq ${r.seq}, at or below the snapshot's ${top}`);
});

test('a partially filled order keeps what is left of it, and its size', () => {
  const a = busy();
  const partial = a.orders().find((o) => o.remaining !== o.size);
  assert.ok(partial, 'the fixture did not leave a partial fill to test');
  const b = createBook(); b.restore(a.orders());
  const got = b.orders().find((o) => o.hash === partial.hash);
  assert.equal(String(got.remaining), String(partial.remaining));
  assert.equal(String(got.size), String(partial.size));
});

test('an empty snapshot restores an empty book', () => {
  const b = createBook();
  assert.equal(b.restore([]), 0);
  assert.equal(b.size, 0);
  assert.equal(b.restore(null), 0);
});

test('a corrupt snapshot is refused, so the caller falls back to the log', () => {
  const good = busy().orders();
  const cases = [
    [good.map((o) => ({ ...o, seq: undefined })), /no sequence number/],
    [good.map((o) => ({ ...o, seq: 0 })), /no sequence number/],
    [[...good, good[0]], /appears twice/],
    [good.map((o) => ({ ...o, price: 0n })), /no price/],
    [good.map((o) => ({ ...o, remaining: 0n })), /resting/],
    [good.map((o) => ({ ...o, remaining: P(999) })), /resting/],
    [[{ seq: 1, price: P(0.4), size: P(1), remaining: P(1) }], /without a hash/],
  ];
  for (const [snap, why] of cases) {
    const b = createBook();
    assert.throws(() => b.restore(snap), why, `this snapshot was accepted: ${why}`);
  }
});

test('restoring over a book that already holds orders is refused', () => {
  const a = busy();
  const b = createBook(); b.add(mk(1, true, 0.5));
  assert.throws(() => b.restore(a.orders()), /already holds orders/);
});

test('a big book round-trips, and restoring is far cheaper than replaying it', () => {
  const a = createBook();
  const N = 20000;
  for (let i = 1; i <= N; i++) a.add(mk(i, i % 2 === 0, 0.30 + (i % 50) * 0.001));
  const snap = a.orders();
  const t0 = process.hrtime.bigint();
  const b = createBook(); b.restore(snap);
  const restoreMs = Number(process.hrtime.bigint() - t0) / 1e6;
  assert.equal(b.size, a.size);
  assert.equal(bookHashOf(b.orders()), bookHashOf(a.orders()));
  const t1 = process.hrtime.bigint();
  const c = createBook();
  for (let i = 1; i <= N; i++) c.add(mk(i, i % 2 === 0, 0.30 + (i % 50) * 0.001));
  const replayMs = Number(process.hrtime.bigint() - t1) / 1e6;
  console.log(`  restore ${a.size} resting: ${restoreMs.toFixed(0)} ms · re-applying the same adds: ${replayMs.toFixed(0)} ms (and that is WITHOUT the signature checks a log replay pays)`);
  assert.ok(restoreMs < replayMs, `restore (${restoreMs.toFixed(0)} ms) was not cheaper than replay (${replayMs.toFixed(0)} ms)`);
});
