// node --test engine/l3/native.test.mjs — the native book behind createBook()'s surface: the same rules as
// matcher.test.mjs, a differential against the JS matcher in-process, journal replay, and batched add.
// Skipped unless native/book/bookd has been built (make -C native/book).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBook } from './matcher.js';
import { createNativeBook } from './native.js';

const BIN = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../native/book/bookd');
const skip = fs.existsSync(BIN) ? false : `native/book/bookd not built (${BIN})`;
const E = 10n ** 18n; const px = (c) => BigInt(Math.round(c * 100)) * E / 100n; const sz = (n) => BigInt(n) * E;
let n = 0; const o = (user, buy, price, size, extra = {}) => ({ hash: `h${++n}`, user, buy, price: px(price), size: sz(size), ...extra });
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'rollbook-test-'));

test('the native book matches at the maker price, FIFO, best price first', { skip }, () => {
  const b = createNativeBook();
  try {
    b.add(o('ann', false, 0.60, 10)); b.add(o('bob', false, 0.55, 5)); b.add(o('cat', false, 0.55, 5));
    const r = b.add(o('dan', true, 0.60, 12));
    assert.equal(r.fills.length, 3); assert.equal(r.rested, false); assert.equal(r.remaining, 0n);
    assert.deepEqual(r.fills.map((f) => [f.maker, Number(f.price) / 1e18, Number(f.size) / 1e18]), [['bob', 0.55, 5], ['cat', 0.55, 5], ['ann', 0.60, 2]]);
    assert.equal(b.best().ask, px(0.60)); assert.equal(b.depth().asks[0].size, sz(8)); assert.equal(b.size, 1);
  } finally { b.close(); }
});

test('post-only, ioc, duplicate, cancel and self-trade behave as the JS matcher does', { skip }, () => {
  const b = createNativeBook();
  try {
    const a = o('ann', true, 0.40, 10);
    assert.equal(b.add(a).rested, true);
    assert.equal(b.add(a).reason, 'duplicate');
    assert.equal(b.add(o('bob', false, 0.40, 1, { postOnly: true })).reason, 'would cross');
    assert.equal(b.add(o('bob', false, 0.45, 1, { postOnly: true })).rested, true);
    const ioc = b.add(o('cat', false, 0.30, 25, { ioc: true }));
    assert.equal(ioc.fills.length, 1); assert.equal(ioc.remaining, sz(15)); assert.equal(ioc.rested, false);
    // self-trades are allowed: bob's own 0.45 ask is taken by bob's own bid
    const self = b.add(o('bob', true, 0.45, 1));
    assert.equal(self.fills.length, 1); assert.equal(self.fills[0].maker, 'bob'); assert.equal(self.fills[0].taker, 'bob');
    assert.equal(b.size, 0);
    assert.equal(b.cancel(a.hash), null);                 // it was filled out by the ioc
    assert.equal(b.best().bid, null); assert.equal(b.best().ask, null);
  } finally { b.close(); }
});

test('a price off the tick grid is refused rather than rounded', { skip }, () => {
  const b = createNativeBook();
  try {
    const r = b.add({ hash: 'x1', user: 'ann', buy: true, price: px(0.40) + 1n, size: sz(1) });
    assert.equal(r.reason, 'off tick'); assert.equal(r.rested, false); assert.equal(b.size, 0);
    assert.equal(b.add({ hash: 'x2', user: 'ann', buy: true, price: 10n ** 14n, size: sz(1) }).rested, true);
  } finally { b.close(); }
});

// fsync off here only: this test is about matching semantics, and one fdatasync per single-order ack costs
// ~2 ms on an ordinary disk (the measured figure is in L3-NATIVE-BOOK.md). Durability has its own test below.
test('differential in-process: 20k mixed ops agree with engine/l3/matcher.js fill for fill', { skip }, () => {
  const a = createBook(), b = createNativeBook({ fsync: false });
  try {
    let s = 2024; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
    const users = ['u1', 'u2', 'u3', 'u4']; const live = [];
    const fmt = (r) => r.fills.map((f) => `${f.makerHash}>${f.takerHash}@${f.price}:${f.size}:${f.maker}:${f.taker}:${f.takerBuys}`).join('|') + `#${r.rested}:${r.remaining}:${r.reason}`;
    for (let i = 0; i < 20000; i++) {
      if (live.length && rnd() < 0.15) {
        const h = live.splice(Math.floor(rnd() * live.length), 1)[0];
        const x = a.cancel(h), y = b.cancel(h);
        assert.equal(!!x, !!y, `cancel ${h} at op ${i}`);
        if (x) assert.equal(x.remaining, y.remaining, `cancel remaining ${h}`);
        continue;
      }
      const buy = rnd() < 0.5, cross = rnd() < 0.35;
      const tick = BigInt(buy ? (cross ? 5000 + Math.floor(rnd() * 40) : 4999 - Math.floor(rnd() * 40))
                              : (cross ? 5000 - Math.floor(rnd() * 40) : 5001 + Math.floor(rnd() * 40)));
      const ord = { hash: `o${i}`, user: users[Math.floor(rnd() * 4)], buy, price: tick * 10n ** 14n,
                    size: BigInt(1 + Math.floor(rnd() * 50)) * E, ioc: rnd() < 0.08, postOnly: rnd() < 0.08 };
      const ra = a.add(ord), rb = b.add(ord);
      assert.equal(fmt(ra), fmt(rb), `op ${i}`);
      if (ra.rested) live.push(ord.hash);
    }
    assert.equal(a.size, b.size); assert.equal(a.seq, b.seq);
    assert.deepEqual(a.depth(10).bids.map((l) => [l.price, l.size, l.orders]), b.depth(10).bids.map((l) => [l.price, l.size, l.orders]));
    assert.deepEqual(a.depth(10).asks.map((l) => [l.price, l.size, l.orders]), b.depth(10).asks.map((l) => [l.price, l.size, l.orders]));
    assert.deepEqual(a.best(), b.best());
    const pa = a.preview(true, px(0.52), sz(30)), pb = b.preview(true, px(0.52), sz(30));
    assert.equal(pa.filled, pb.filled); assert.equal(pa.cost, pb.cost); assert.equal(pa.fills, pb.fills); assert.equal(pa.avg, pb.avg);
    assert.deepEqual(a.orders('u1').map((x) => [x.hash, x.remaining, x.price]), b.orders('u1').map((x) => [x.hash, x.remaining, x.price]));
    assert.ok(a.size > 100);
  } finally { b.close(); }
});

test('addMany pipelines a batch and returns exactly what one-at-a-time would', { skip }, () => {
  const one = createNativeBook({ fsync: false }), many = createNativeBook({ fsync: false });
  try {
    const batch = [];
    for (let i = 0; i < 200; i++) batch.push({ hash: `b${i}`, user: `u${i % 5}`, buy: i % 2 === 0, price: px(0.50 + ((i % 7) - 3) / 100), size: sz(1 + (i % 9)) });
    const a = batch.map((x) => one.add(x)), b = many.addMany(batch);
    const key = (r) => `${r.rested}:${r.remaining}:${r.reason}:${r.fills.map((f) => `${f.makerHash}@${f.price}:${f.size}`).join(',')}`;
    assert.deepEqual(a.map(key), b.map(key));
    assert.equal(one.stat().stateHash, many.stat().stateHash);
    assert.equal(one.stat().bookHash, many.stat().bookHash);
  } finally { one.close(); many.close(); }
});

test('the journal is durable before the ack: a restart replays to the same book, seq and hashes', { skip }, () => {
  const dir = tmp(); const journal = path.join(dir, 'book.journal');
  const first = createNativeBook({ dir, journal });
  let before;
  try {
    for (let i = 0; i < 500; i++) first.add({ hash: `j${i}`, user: `u${i % 6}`, buy: i % 2 === 0, price: px(0.45 + ((i % 11) - 5) / 100), size: sz(1 + (i % 5)) });
    for (let i = 0; i < 100; i += 3) first.cancel(`j${i}`);
    before = first.stat();
    assert.ok(before.journal >= 500);
  } finally { first.close(); }
  const again = createNativeBook({ dir, journal });
  try {
    const after = again.stat();
    assert.equal(after.seq, before.seq);
    assert.equal(after.resting, before.resting);
    assert.equal(after.stateHash, before.stateHash);         // the transcript hash of a replay is the transcript
    assert.equal(after.bookHash, before.bookHash);
  } finally { again.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});
