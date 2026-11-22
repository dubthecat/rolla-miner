// engine/l3/miner/roots.js — the batch's orders root, computed off the event loop.
//
// This is the venue's throughput wall, measured on the production path (docs/L3-MINERS.md §6):
//
//   per 500-op batch   apply 7.4 ms · ordersRoot 57.8 ms · batchRoot 0.21 ms   →  7,652 ops/s per shard
//   and ordersRootOf   94 % leaves, 6 % tree
//
// It is not the hashing algorithm's fault and there is nothing left to shave. Each leaf is two keccaks, and a
// keccak through the native binding is ~7 µs for 289 bytes — which IS the floor here: the raw addon calls cost
// 7.17 µs against the package wrapper's 7.01, a fresh instance 17.7, and pure-JS noble 77. Four N-API crossings
// per hash, ~4 µs of them fixed cost. The packer and the hex round trips were already squeezed (41.97 → 38.21 µs
// a leaf).
//
// What is left is that it all happens on ONE event loop. The engine sequences every shard in a single process, so
// a 19.6 ms root is 19.6 ms during which no other shard is sequenced, no order is accepted and no route answers —
// which is why the venue-wide ceiling is ~7,600 orders/s and not 7,652 × 73 shards.
//
// The leaves are embarrassingly parallel: a batch's ops are fixed the moment it closes, and `orderLeaf` is pure.
// So the leaves go to workers and the tree — 6 %, and inherently a reduction — stays here. The hand-off is cheap
// enough to be worth it: structuredClone of 500 ops is 1.78 ms against 19.6 ms of hashing, and the log append
// already pays a comparable 1.94 ms to JSON.stringify the same batch.
//
// `L3_ROOT_WORKERS=0` (the default) computes inline, exactly as before. Below `L3_ROOT_MIN_OPS` it also computes
// inline, because for a small batch the hand-off is the whole cost. A worker that dies is not fatal: that batch's
// root is computed inline and the pool carries on, because a sequencer that cannot seal is worse than a slow one.
import { isMainThread, parentPort, workerData, Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { orderLeaf, ordersRootOf, merkleRoot } from './merkle.js';

const ROLE = 'l3-roots';

/**
 * createRootPool({ workers, minOps, logger, env })
 *   ordersRoot(ops) → Promise<hex>   identical to ordersRootOf(ops), by construction: the workers run the same
 *                                    orderLeaf and this builds the same tree over the leaves they return
 */
export function createRootPool({ workers = null, minOps = null, logger = null, env = process.env } = {}) {
  const n = Math.max(0, workers == null ? Number(env.L3_ROOT_WORKERS ?? 0) : Number(workers) || 0);
  const MIN = Math.max(1, minOps == null ? Number(env.L3_ROOT_MIN_OPS ?? 64) : Number(minOps) || 1);
  const stats = { calls: 0, inline: 0, parallel: 0, ops: 0, ms: 0, failures: 0 };

  if (!n) {
    return {
      workers: 0, stats: () => ({ ...stats }),
      async ordersRoot(ops) { const t0 = process.hrtime.bigint(); stats.calls++; stats.inline++; stats.ops += ops.length; const r = ordersRootOf(ops); stats.ms += Number(process.hrtime.bigint() - t0) / 1e6; return r; },
      async close() { },
    };
  }

  const pool = [];
  for (let i = 0; i < n; i++) {
    const w = new Worker(fileURLToPath(import.meta.url), { workerData: { role: ROLE } });
    const slot = { w, pending: new Map(), busy: 0, next: 1, dead: false };
    w.on('message', (m) => { const r = slot.pending.get(m.id); if (!r) return; slot.pending.delete(m.id); slot.busy--; if (!slot.busy) w.unref(); r(m.out); });
    w.on('error', (e) => { slot.dead = true; stats.failures++; logger && logger(`[l3roots] worker died: ${e.message}`); for (const r of slot.pending.values()) r(null); slot.pending.clear(); slot.busy = 0; });
    w.unref();
    pool.push(slot);
  }
  const ask = (slot, ops) => new Promise((resolve) => {
    const id = slot.next++;
    slot.pending.set(id, resolve);
    if (!slot.busy) slot.w.ref();
    slot.busy++;
    slot.w.postMessage({ id, ops });
  });

  return {
    workers: n, stats: () => ({ ...stats }),
    async ordersRoot(ops) {
      const t0 = process.hrtime.bigint();
      stats.calls++; stats.ops += ops.length;
      const live = pool.filter((s) => !s.dead);
      if (ops.length < MIN || !live.length) {
        stats.inline++;
        const r = ordersRootOf(ops);
        stats.ms += Number(process.hrtime.bigint() - t0) / 1e6;
        return r;
      }
      const per = Math.ceil(ops.length / live.length);
      const jobs = [];
      for (let i = 0, k = 0; i < ops.length; i += per, k++) jobs.push({ at: i, slice: ops.slice(i, i + per), p: ask(live[k % live.length], ops.slice(i, i + per)) });
      const leaves = new Array(ops.length);
      let fellBack = false;
      for (const j of jobs) {
        const res = await j.p;
        // a dead worker is not a reason to fail a seal: hash its slice here instead
        if (!res) { fellBack = true; for (let i = 0; i < j.slice.length; i++) leaves[j.at + i] = orderLeaf(j.slice[i]); continue; }
        for (let i = 0; i < res.length; i++) leaves[j.at + i] = res[i];
      }
      if (fellBack) stats.inline++; else stats.parallel++;
      const root = merkleRoot(leaves);
      stats.ms += Number(process.hrtime.bigint() - t0) / 1e6;
      return root;
    },
    async close() { for (const s of pool) { try { await s.w.terminate(); } catch { } } pool.length = 0; },
  };
}

// --------------------------------------------------------------------------------------------- the worker
// This file IS the worker: re-entered with workerData.role, it turns ops into leaves and nothing else. No state,
// no clock, no I/O — `orderLeaf` is pure, which is the only reason this is safe to move off the main thread.
if (!isMainThread && workerData && workerData.role === ROLE) {
  parentPort.on('message', ({ id, ops }) => {
    let out = null;
    try { out = ops.map(orderLeaf); } catch (e) { out = null; }
    parentPort.postMessage({ id, out });
  });
}
