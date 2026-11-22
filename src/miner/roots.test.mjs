// node --test engine/l3/miner/roots.test.mjs — the orders root, optionally off the event loop.
//
// The root is what every miner votes on, so the only acceptable property is that the pooled path and the inline
// path produce the SAME word, always, including when a worker dies mid-batch. The measurement that decided the
// default is in docs/L3-MINERS.md §6: across 20 shards sealing a 200-op batch each, workers gave no throughput
// (17,117 → 20,892 → 16,960 orders/s for 0 → 2 → 4) and made the loop LESS responsive (0 ms late inline against
// 25–59 ms), because the work is CPU-bound and the workers contend for the same cores. So it is off by default,
// and it exists for a machine with cores to spare — the correctness tests stand either way.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRootPool } from './roots.js';
import { ordersRootOf } from './merkle.js';

const op = (i, kind = 'add') => ({
  t: kind, hash: '0x' + String(i).padStart(64, '0'), market: 1, outcome: 0,
  signer: '0x' + '55'.repeat(20), sig: '0x' + 'cd'.repeat(65), at: i,
  order: { user: '0x' + '77'.repeat(20), marketId: '1', outcome: '0', token: '0x' + 'c0'.repeat(20), buy: i % 2 === 0,
           price: String(BigInt(i % 97 + 1) * 10n ** 16n), size: String(10n ** 18n * BigInt(1 + i % 7)), deadline: '9', nonce: String(i), salt: String(i), postOnly: i % 5 === 0, ioc: i % 11 === 0 } });
const batch = (n) => Array.from({ length: n }, (_, i) => op(i + 1, i % 13 === 0 ? 'cancel' : 'add'));

test('with no workers it is ordersRootOf, and says so', async () => {
  const pool = createRootPool({ workers: 0 });
  try {
    for (const n of [0, 1, 2, 7, 64, 200]) {
      const ops = batch(n);
      assert.equal(await pool.ordersRoot(ops), ordersRootOf(ops), `n=${n}`);
    }
    assert.equal(pool.workers, 0);
    assert.equal(pool.stats().parallel, 0);
    assert.ok(pool.stats().inline > 0);
  } finally { await pool.close(); }
});

test('the pooled root equals the inline root, for every batch shape', async () => {
  const pool = createRootPool({ workers: 2, minOps: 1 });
  try {
    for (const n of [1, 2, 3, 5, 8, 13, 64, 101, 200, 501]) {
      const ops = batch(n);
      const got = await pool.ordersRoot(ops);
      assert.equal(got, ordersRootOf(ops), `n=${n}: the pool and the inline path disagree, which is a fork`);
    }
    assert.ok(pool.stats().parallel > 0, 'nothing actually went to a worker');
  } finally { await pool.close(); }
});

test('an odd split across workers still gives the leaves in order', async () => {
  // the tree is order-sensitive: a root built from leaves in the wrong order is a different root, and the slices
  // come back from several workers. 7 ops over 4 workers is 2/2/2/1.
  for (const workers of [1, 2, 3, 4, 8]) {
    const pool = createRootPool({ workers, minOps: 1 });
    try {
      const ops = batch(7);
      assert.equal(await pool.ordersRoot(ops), ordersRootOf(ops), `${workers} worker(s)`);
      // and a batch smaller than the worker count
      assert.equal(await pool.ordersRoot(batch(2)), ordersRootOf(batch(2)), `${workers} worker(s), 2 ops`);
    } finally { await pool.close(); }
  }
});

test('below minOps it stays inline, because the hand-off is the whole cost there', async () => {
  const pool = createRootPool({ workers: 2, minOps: 100 });
  try {
    const small = batch(10);
    assert.equal(await pool.ordersRoot(small), ordersRootOf(small));
    assert.equal(pool.stats().parallel, 0, 'a small batch was sent to a worker');
    const big = batch(200);
    assert.equal(await pool.ordersRoot(big), ordersRootOf(big));
    assert.equal(pool.stats().parallel, 1);
  } finally { await pool.close(); }
});

test('a dead worker does not fail a seal: its slice is hashed here instead', async () => {
  // a sequencer that cannot seal is worse than a slow one
  const pool = createRootPool({ workers: 2, minOps: 1, logger: () => { } });
  try {
    const ops = batch(120);
    assert.equal(await pool.ordersRoot(ops), ordersRootOf(ops));
    // kill them both, the way an OOM or a crash would
    await pool.close();
    const got = await pool.ordersRoot(ops);
    assert.equal(got, ordersRootOf(ops), 'the root changed when the workers went away');
    assert.ok(pool.stats().inline > 0, 'it did not fall back inline');
  } finally { await pool.close(); }
});

test('the same batch rooted twice gives the same word, pooled or not', async () => {
  const ops = batch(150);
  const a = createRootPool({ workers: 0 });
  const b = createRootPool({ workers: 3, minOps: 1 });
  try {
    const r1 = await a.ordersRoot(ops), r2 = await b.ordersRoot(ops), r3 = await b.ordersRoot(ops);
    assert.equal(r1, r2); assert.equal(r2, r3);
  } finally { await a.close(); await b.close(); }
});
