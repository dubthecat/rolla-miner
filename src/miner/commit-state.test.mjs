// node --test engine/l3/miner/commit-state.test.mjs — the epoch book commitment adapter (commit-state.js):
// bookhash is bookHashOf; tree is the Merkle-treap fed from the book's own events through createShardState,
// equal to a tree rebuilt from scratch over the resting orders at every checkpoint, whatever the flush cadence,
// whichever book (matcher.js or the native book) feeds it, and whichever tree (commit.js or bookd) keeps it.
// Prints the measured cost of feeding the tree per op and per 2,000-op batch, JS and native.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createShardState } from './miner.js';
import { createCommitState } from './commit-state.js';
import { createCommitTree, verifyCommitProof } from './commit.js';
import { EMPTY_BOOK_HASH } from './commit-state.js';
import { bookHashOf, keccak, ZERO32 } from './merkle.js';
import { createNativeBook } from '../native.js';
import { l3OrderFor, serializeL3Order } from '../desk.js';

const BIN = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../native/book/bookd');
const skipNative = fs.existsSync(BIN) ? false : `native/book/bookd not built (${BIN})`;
const E = 10n ** 18n, MARKET = 4242, TOKEN = '0x' + 'c0'.repeat(20);
const utf8 = (s) => new TextEncoder().encode(s);

/// the shape of the cluster test's workload (10% cancels, 30% crossing, 10% IOC, 1..100 shares within 20¢ of
/// 50¢), unsigned: createShardState does not verify, and a hash only has to be a 32-byte word here
function workload(seed, n, { ioc = 0.1, cancels = 0.1 } = {}) {
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const hexn = (k) => { let h = '0x'; for (let i = 0; i < k; i++) h += ((rnd() * 256) | 0).toString(16).padStart(2, '0'); return h; };
  const users = Array.from({ length: 8 }, () => hexn(20));
  const live = [], ops = [];
  for (let i = 0; i < n; i++) {
    if (live.length && rnd() < cancels) { const hash = live.splice((rnd() * live.length) | 0, 1)[0]; ops.push({ t: 'cancel', hash, market: MARKET, outcome: 0 }); continue; }
    const buy = rnd() < 0.5, cross = rnd() < 0.3, off = (rnd() * 20) | 0;
    const cents = buy ? (cross ? 50 + off : 49 - off) : (cross ? 50 - off : 51 + off);
    const order = l3OrderFor({ user: users[(rnd() * users.length) | 0], marketId: MARKET, outcome: 0, token: TOKEN, buy, price: BigInt(cents) * E / 100n, size: BigInt(1 + ((rnd() * 100) | 0)) * E, ioc: rnd() < ioc });
    order.nonce = BigInt(1700000000000 + i); order.salt = BigInt(i) + 1n;
    const hash = keccak(utf8(`${seed}:${i}`));
    ops.push({ t: 'add', hash, market: MARKET, outcome: 0, order: serializeL3Order(order), at: 1700000000000 });
    if (!order.ioc) live.push(hash);
  }
  return ops;
}
/// the commitment rebuilt from nothing over the resting orders: what any tree must equal
const fromScratch = (orders) => { const t = createCommitTree(); for (const o of orders) assert.equal(t.insert(o), true); return t.root(); };
const ENV = {};   // no L3_COMMIT: the default

test('bookhash is the default: the root is bookHashOf(resting) and the events cost nothing', () => {
  const st = createShardState({ env: ENV });
  for (const op of workload(1, 400)) st.apply(op);
  assert.equal(st.commit.mode, 'bookhash'); assert.equal(st.commit.impl, 'bookhash');
  assert.ok(st.size > 50);
  assert.equal(st.bookHash(), bookHashOf(st.resting()));
  assert.equal(st.commit.stats().ops, 0);
  assert.equal(st.commit.proof(st.resting()[0].hash), null);
  assert.equal(st.flush().applied, 0);
  const empty = createShardState({ env: ENV }); assert.equal(empty.bookHash(), bookHashOf([]));
  st.close();
});

test('tree: the shard root is the treap over its resting orders at every checkpoint, whatever the flush cadence', () => {
  const each = createShardState({ env: ENV, commit: { mode: 'tree', native: false, flushAt: 1 } });        // every event through the tree, in order
  const batched = createShardState({ env: ENV, commit: { mode: 'tree', native: false, flushAt: 100000 } });  // coalesced until the root is asked
  assert.equal(each.commit.mode, 'tree'); assert.equal(each.commit.impl, 'js'); assert.equal(batched.commit.impl, 'js');
  // an empty book commits to the same non-zero word as bookhash mode: the zero word means "no commitment"
  assert.equal(each.bookHash(), EMPTY_BOOK_HASH); assert.equal(EMPTY_BOOK_HASH, bookHashOf([])); assert.notEqual(EMPTY_BOOK_HASH, ZERO32);
  const ops = workload(7, 6000);
  for (let i = 0; i < ops.length; i++) {
    each.apply(ops[i]); batched.apply(ops[i]);
    if (i % 500 === 499) {
      const want = fromScratch(each.resting());
      assert.equal(each.bookHash(), want, `unbatched tree diverged at op ${i}`);
      assert.equal(batched.bookHash(), want, `batched tree diverged at op ${i}`);
      assert.equal(each.commit.size(), each.size);
    }
  }
  assert.ok(each.size > 500, `only ${each.size} resting`);
  assert.notEqual(each.bookHash(), bookHashOf(each.resting()), 'a treap root is not a sorted-list root: the mode is a protocol change');
  const a = each.commit.stats(), b = batched.commit.stats();
  assert.equal(a.refused, 0); assert.equal(b.refused, 0); assert.equal(a.conflicts, 0); assert.equal(b.conflicts, 0);
  assert.equal(a.ops, b.ops, 'the same events were seen');
  assert.ok(b.applied < a.applied, `coalescing applied ${b.applied} of ${b.ops} events (unbatched: ${a.applied})`);
  // a proof of a resting order verifies against the root, and names that order's remaining
  const o = each.resting()[17];
  const p = each.commit.proof(o.hash);
  assert.ok(p); assert.equal(verifyCommitProof(p, each.bookHash(), o), true);
  assert.equal(verifyCommitProof(p, each.bookHash(), { ...o, remaining: o.remaining + 1n }), false);
  assert.deepEqual(batched.commit.proof(o.hash), p);
  // a cancel of something that is not there, and a fill of an unknown maker, are refused and counted, not fatal
  each.commit.cancelled('0x' + 'ee'.repeat(32)); each.commit.flush();
  assert.equal(each.commit.stats().refused, 1);
  // emptied again (every resting order cancelled), the book commits to the empty word, not to "no commitment"
  for (const o of each.resting()) each.apply({ t: 'cancel', hash: o.hash, market: MARKET, outcome: 0 });
  assert.equal(each.size, 0); assert.equal(each.bookHash(), EMPTY_BOOK_HASH); assert.equal(each.commit.size(), 0);
  each.close(); batched.close();
});

test('tree, native: bookd agrees root for root and proof for proof, fed by either book', { skip: skipNative }, () => {
  const jj = createShardState({ env: ENV, commit: { mode: 'tree', native: false } });                                         // matcher.js + commit.js
  const jn = createShardState({ env: ENV, commit: { mode: 'tree', native: true } });                                          // matcher.js + bookd's tree
  const nn = createShardState({ env: ENV, commit: { mode: 'tree', native: true }, newBook: () => createNativeBook({ fsync: false }) });   // the native book + bookd's tree
  try {
    assert.equal(jn.commit.impl, 'native'); assert.equal(nn.commit.impl, 'native');
    assert.equal(jn.bookHash(), EMPTY_BOOK_HASH);
    const ops = workload(11, 6000);
    let checks = 0;
    for (let i = 0; i < ops.length; i++) {
      const a = jj.apply(ops[i]), b = jn.apply(ops[i]), c = nn.apply(ops[i]);
      assert.equal(a.rested, c.rested); assert.equal(a.seq, c.seq, `the native book numbers op ${i} differently`);
      if (a.fills?.length) assert.deepEqual(a.fills.map((f) => f.makerRemaining), c.fills.map((f) => f.makerRemaining), `maker remaining at op ${i}`);
      assert.equal(a.rested, b.rested);
      if (i % 500 === 499) {
        const want = fromScratch(jj.resting());
        assert.equal(jj.bookHash(), want, `js tree diverged at op ${i}`);
        assert.equal(jn.bookHash(), want, `native tree diverged at op ${i}`);
        assert.equal(nn.bookHash(), want, `native book + native tree diverged at op ${i}`);
        assert.equal(jn.commit.size(), jj.size);
        const o = jj.resting()[(i * 7) % jj.size];
        const p = jj.commit.proof(o.hash);
        assert.deepEqual(jn.commit.proof(o.hash), p, `native proof differs at op ${i}`);
        assert.deepEqual(nn.commit.proof(o.hash), p);
        assert.equal(verifyCommitProof(p, want, o), true);
        checks++;
      }
    }
    assert.ok(checks >= 10);
    assert.equal(jn.commit.proof('0x' + 'ee'.repeat(32)), null);
    for (const s of [jj, jn, nn]) { const x = s.commit.stats(); assert.equal(x.refused, 0, `${x.impl}: refused ${x.refused}`); assert.equal(x.conflicts, 0); }
  } finally { jj.close(); jn.close(); nn.close(); }
});

test('measured: feeding the tree per op and per 2,000-op batch, JS and native', { skip: skipNative }, () => {
  const N = 20000, BATCH = 2000;
  const ops = workload(5, N);
  const run = (commit) => {
    const st = createShardState({ env: ENV, commit });
    const t0 = process.hrtime.bigint();
    let flushMs = 0, batches = 0;
    for (let i = 0; i < ops.length; i++) {
      st.apply(ops[i]);
      if (i % BATCH === BATCH - 1) { const t = process.hrtime.bigint(); st.flush(); flushMs += Number(process.hrtime.bigint() - t) / 1e6; batches++; }
    }
    const t1 = process.hrtime.bigint(); const root = st.bookHash(); const rootUs = Number(process.hrtime.bigint() - t1) / 1e3;
    const total = Number(process.hrtime.bigint() - t0) / 1e6;
    const x = st.commit.stats(); const resting = st.size;
    st.close();
    return { root, total, flushMs, batches, rootUs, x, resting };
  };
  const js = run({ mode: 'tree', native: false }), nat = run({ mode: 'tree', native: true }), bh = run({ mode: 'bookhash' });
  assert.equal(js.root, nat.root);
  assert.equal(nat.x.impl, 'native');
  console.log(`\n  commitment over ${N} ops → ${js.resting} resting, ${js.x.ops} tree events, ${js.x.applied} applied after coalescing, ${js.batches} batches of ${BATCH}`);
  console.log(`  JS tree      ${(js.x.ms * 1000 / js.x.ops).toFixed(1)} µs per event · ${(js.flushMs / js.batches).toFixed(1)} ms per ${BATCH}-op batch (${(js.flushMs * 1000 / js.x.applied).toFixed(0)} µs per applied update) · root ${js.rootUs.toFixed(0)} µs · whole run ${js.total.toFixed(0)} ms`);
  console.log(`  native tree  ${(nat.x.ms * 1000 / nat.x.ops).toFixed(1)} µs per event · ${(nat.flushMs / nat.batches).toFixed(1)} ms per ${BATCH}-op batch (${(nat.flushMs * 1000 / nat.x.applied).toFixed(0)} µs per applied update, one round trip each) · root ${nat.rootUs.toFixed(0)} µs · whole run ${nat.total.toFixed(0)} ms`);
  console.log(`  bookhash     root over ${bh.resting} resting: ${(bh.rootUs / 1000).toFixed(1)} ms (${(bh.rootUs / bh.resting).toFixed(0)} µs a leaf) — what every epoch boundary cost before`);
});
