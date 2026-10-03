// node --test engine/l3/miner/sequencer.test.mjs — a SLOW log (every append waits) makes full batches queue behind a seal;
// the epoch commitments must still be the ones the batch's own ops produce, so a miner replaying the log agrees on every
// batch (the RunPod cluster dissented on every epoch boundary before this was fixed)
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createFileLog, ordersTopic, votesTopic } from './log.js';
import { createSequencer } from './sequencer.js';
import { createMiner, createShardState } from './miner.js';
import { createQuorum } from './quorum.js';
import { orderHash } from './verify.js';
import { BOOK_DOMAIN, L3_ORDER_TYPES, l3OrderFor, serializeL3Order } from '../desk.js';

test('queued batches carry the commitment of their own ops: one miner, slow log, no dissent', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'l3seq-')); const SHARD = '77', E = 10n ** 18n;
  const domain = BOOK_DOMAIN(46630, '0x' + 'b0'.repeat(20)); const token = '0x' + 'c0'.repeat(20);
  const slow = createFileLog({ dir: path.join(dir, 'log'), pollMs: 2 }); const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const log = { ...slow, append: async (t, v) => { await sleep(25); return slow.append(t, v); } };
  const traders = Array.from({ length: 4 }, () => privateKeyToAccount(generatePrivateKey()));
  const ops = []; let s = 5 >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  for (let i = 0; i < 1200; i++) {
    const acct = traders[i % 4]; const buy = rnd() < 0.5, cross = rnd() < 0.4, off = Math.floor(rnd() * 10); const cents = buy ? (cross ? 50 + off : 49 - off) : (cross ? 50 - off : 51 + off);
    const order = l3OrderFor({ user: acct.address, marketId: 77, outcome: 0, token, buy, price: BigInt(cents) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 20)) * E });
    order.nonce = BigInt(1700000000000 + i); order.salt = BigInt(i) + 1n;
    const ser = serializeL3Order(order); const sig = await acct.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: order });
    ops.push({ t: 'add', hash: orderHash(domain, ser), market: 77, outcome: 0, order: ser, sig, signer: acct.address.toLowerCase(), at: Date.now() });
  }
  const seqAcct = privateKeyToAccount(generatePrivateKey()); const state = createShardState({}); const sealed = [];
  const seq = createSequencer({ shard: SHARD, log, account: seqAcct, state, batchMs: 30, batchMax: 50, epochBatches: 3, onSealed: (b) => sealed.push(b), logger: () => {} });
  await seq.resume();
  const votes = []; const miner = createMiner({ shard: SHARD, log: slow, account: privateKeyToAccount(generatePrivateKey()), domain, dir: path.join(dir, 'm'), workers: 1, sequencers: [seqAcct.address], threshold: 1, watchVotes: false, logger: () => {}, onVote: (v) => votes.push(v) });
  await miner.start();
  const quorum = createQuorum({ threshold: 1, logger: () => {} });
  await slow.subscribe(ordersTopic(SHARD), 0, async ({ value }) => { quorum.announce(value); });
  await slow.subscribe(votesTopic(SHARD), 0, async ({ value }) => { await quorum.vote(value); });
  for (let i = 0; i < ops.length; i++) { seq.submit(ops[i]); if (i % 10 === 0) await sleep(1); }   // fast submits against a 25 ms append → batches queue up
  await seq.flush(); const last = seq.index - 1;
  assert.ok(sealed.length >= 20, `only ${sealed.length} batches`); assert.ok(seq.status().queued === 0);
  const deadline = Date.now() + 60000; while (Date.now() < deadline && quorum.finalIndex < last) await sleep(10);
  assert.equal(quorum.finalIndex, last, `finality stopped at ${quorum.finalIndex} of ${last}`); assert.equal(quorum.halted, false);
  assert.equal(votes.filter((v) => !v.ok).length, 0, `dissents: ${votes.filter((v) => !v.ok).map((v) => v.index + ':' + v.why).join(' | ')}`);
  const epochEnds = sealed.filter((b) => b.bookHash !== '0x' + '0'.repeat(64)); assert.ok(epochEnds.length >= 5, 'no epoch commitments');
  assert.equal(miner.state.bookHash(), state.bookHash());
  for (let i = 1; i < sealed.length; i++) assert.equal(sealed[i].seqFrom, sealed[i - 1].seqTo + 1, `seq range gap at ${i}`);
  await miner.stop?.(); await seq.stop();
});

test('the append pipeline: seals overlap the log round trip, the log stays in index order, and a refusing log is retried in order', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'l3seq-pipe-')); const base = createFileLog({ dir, logger: () => {} });
  const SHARD = 90, E = 10n ** 18n, domain = BOOK_DOMAIN(46630, '0x' + 'b0'.repeat(20)), token = '0x' + 'c0'.repeat(20);
  const traders = Array.from({ length: 4 }, () => privateKeyToAccount(generatePrivateKey()));
  async function signedOps(n, from) {
    const out = []; let s = (7 + from) >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
    for (let i = from; i < from + n; i++) {
      const acct = traders[i % 4]; const buy = rnd() < 0.5, cross = rnd() < 0.4, off = Math.floor(rnd() * 10); const cents = buy ? (cross ? 50 + off : 49 - off) : (cross ? 50 - off : 51 + off);
      const order = l3OrderFor({ user: acct.address, marketId: SHARD + 1, outcome: 0, token, buy, price: BigInt(cents) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 20)) * E });
      order.nonce = BigInt(1700000000000 + i); order.salt = BigInt(i) + 1n;
      const ser = serializeL3Order(order); const sig = await acct.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: order });
      out.push({ t: 'add', hash: orderHash(domain, ser), market: SHARD + 1, outcome: 0, order: ser, sig, signer: acct.address.toLowerCase(), at: Date.now() });
    }
    return out;
  }
  let refuse = false; const appended = [];
  const RTT = 40;
  const log = {
    kind: 'slow', offset: (t) => base.offset(t), read: (t, f, l) => base.read(t, f, l),
    async append(t, v) { await new Promise((r) => setTimeout(r, RTT)); if (refuse) throw new Error('broker down'); appended.push(v.index); return base.append(t, v); },
    async appendMany(t, vs) { await new Promise((r) => setTimeout(r, RTT)); if (refuse) throw new Error('broker down'); for (const v of vs) appended.push(v.index); return base.appendMany(t, vs); },
  };
  const seqAcct = privateKeyToAccount(generatePrivateKey()); const state = createShardState({}); const sealed = [];
  const seq = createSequencer({ shard: SHARD + 1, log, account: seqAcct, state, batchMs: 5, batchMax: 20, epochBatches: 4, onSealed: (b) => sealed.push(b.index), logger: () => {} });
  await seq.resume();
  const ops = await signedOps(400, 0);
  // 400 ops = 20 batches. Serial appends would take ≥ 20 × RTT = 800 ms; the pipeline overlaps them
  const t0 = Date.now(); for (const op of ops) seq.submit(op); await seq.flush(); const ms = Date.now() - t0;
  assert.equal(seq.index, 20); assert.equal(seq.status().pending, 0); assert.equal(seq.status().retry, 0);
  assert.ok(ms < 20 * RTT, `20 batches took ${ms} ms, serial would be ≥ ${20 * RTT}`);
  assert.deepEqual(appended, [...Array(20).keys()], 'log order');
  assert.deepEqual(sealed, [...Array(20).keys()], 'onSealed order');
  const rows = await base.read(ordersTopic(SHARD + 1), 0, 100); assert.deepEqual(rows.map((r) => r.value.index), [...Array(20).keys()]);
  // now the broker refuses for a while: batches wait in order and land in order once it is back
  refuse = true; const more = await signedOps(100, 400);
  for (const op of more) seq.submit(op); await seq.flush();
  assert.ok(seq.status().retry >= 1, `nothing waiting: ${JSON.stringify(seq.status())}`); assert.equal(seq.index, 25);
  refuse = false; const tail = await signedOps(40, 500); for (const op of tail) seq.submit(op); await seq.flush();
  for (let i = 0; i < 10 && seq.status().retry; i++) { await seq.flush(); await new Promise((r) => setTimeout(r, RTT)); }
  assert.equal(seq.status().retry, 0, 'retry drained');
  const all = await base.read(ordersTopic(SHARD + 1), 0, 100); assert.deepEqual(all.map((r) => r.value.index), [...Array(27).keys()], 'log order after the outage');
  await seq.stop(); await base.close?.();
});
