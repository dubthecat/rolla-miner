// node --test engine/l3/miner/checkpoint.test.mjs — a miner resuming from a checkpoint must land on the SAME
// book as one that replayed the log, and must refuse any checkpoint it cannot prove.
//
// Why this exists: `handleBatch` verifies every order signature, so a miner catching up from the log pays
// secp256k1 for the whole of history. One live shard holds 19,098 orders; across the 73 this fleet follows that
// is ~6.9 minutes per restart at 3,350 orders/s per core, and it scales with the log — hours at a million orders
// a shard. Every restart paid it, and every NEW miner pays it to join. That is the thing that stopped the system
// taking millions of orders, and no number of cores fixes it.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { parseEther } from 'viem';
import { createFileLog, ordersTopic } from './log.js';
import { createSequencer } from './sequencer.js';
import { createMiner, createShardState } from './miner.js';
import { orderHash } from './verify.js';
import { ZERO32 } from './merkle.js';
import * as checkpoint from './checkpoint.js';
import { BOOK_DOMAIN, L3_ORDER_TYPES, serializeL3Order } from '../desk.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'l3cp-'));
const SHARD = '909';
const BOOK = '0x' + '11'.repeat(20);
const until = async (f, ms = 30000, what = 'condition') => { const t = Date.now(); while (!f()) { if (Date.now() - t > ms) throw new Error(`timed out waiting for ${what}`); await new Promise((r) => setTimeout(r, 5)); } };

/// a sequencer on a file log, and signed orders that leave a real book behind (crossing flow and resting flow)
async function rig(dir, { orders = 60, batchMax = 5, epochBatches = 4 } = {}) {
  const log = createFileLog({ dir: path.join(dir, 'log'), pollMs: 2 });
  const domain = BOOK_DOMAIN(46630, BOOK);
  const seqAcct = privateKeyToAccount(generatePrivateKey());
  const state = createShardState({});
  const sealed = [];
  const seq = createSequencer({ shard: SHARD, log, account: seqAcct, state, batchMs: 20, batchMax, epochBatches, onSealed: (b) => sealed.push(b), logger: () => { } });
  await seq.resume();
  const users = [privateKeyToAccount(generatePrivateKey()), privateKeyToAccount(generatePrivateKey())];
  let salt = 0n;
  for (let i = 0; i < orders; i++) {
    const acct = users[i % 2];
    const kind = i % 3;                                  // a third crosses, a third rests far away
    const o = { user: acct.address, marketId: BigInt(SHARD), outcome: 0n, token: '0x' + 'c0'.repeat(20), buy: kind !== 1,
      price: parseEther(String(kind === 0 ? 0.55 : kind === 1 ? 0.45 : 0.30)), size: parseEther('5'),
      deadline: BigInt(Math.floor(Date.now() / 1000) + 3600), nonce: BigInt(Date.now()), salt: ++salt, postOnly: false, ioc: false };
    const ser = serializeL3Order(o);
    const sig = await acct.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: o });
    seq.submit({ t: 'add', hash: orderHash(domain, ser), market: Number(SHARD), outcome: 0, order: ser, sig, signer: acct.address.toLowerCase(), at: Date.now() });
    if (i % 10 === 0) await new Promise((r) => setImmediate(r));
  }
  await seq.flush();
  return { log, domain, seqAcct, state, sealed, seq };
}

const minerOn = (dir, { log, domain, seqAcct }, env = {}, said = []) => createMiner({
  shard: SHARD, log, account: privateKeyToAccount(generatePrivateKey()), domain, dir: path.join(dir, 'miner'), workers: 0,
  sequencers: [seqAcct.address], threshold: 1, watchVotes: false, env: { CHAIN_ID: '46630', ...env }, logger: (m) => said.push(String(m)),
});

test('a miner resumed from a checkpoint holds the same book as one that replayed the log', async () => {
  const dir = tmp();
  const r = await rig(dir);
  const last = r.sealed.at(-1).index;
  // first run: catch up from the log and write checkpoints as it goes
  const said1 = [];
  const a = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '5' }, said1);
  await a.start();
  await until(() => a.index === last, 30000, 'the first miner to catch up');
  const want = { index: a.index, epoch: a.status().epoch, book: a.state.bookHash(), resting: a.state.size, seq: a.state.stat().seq };
  assert.ok(a.status().checkpoint.written >= 1, `no checkpoint was written: ${JSON.stringify(a.status().checkpoint)}`);
  await a.stop();

  // second run: same journal dir, so the checkpoint is there. It must NOT replay the log.
  const said2 = [];
  const b = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '5' }, said2);
  await b.start();
  const resumed = said2.find((l) => /resumed from a checkpoint/.test(l));
  assert.ok(resumed, `it did not resume from the checkpoint: ${said2.join(' | ')}`);
  assert.match(resumed, /no history replayed/);
  assert.equal(b.status().checkpoint.loaded, 1);
  assert.equal(b.status().replayed, 0, 'it replayed batches it did not need to');
  await until(() => b.index === last, 30000, 'the resumed miner to reach the head');
  assert.equal(b.state.bookHash(), want.book, 'the resumed miner holds a different book');
  assert.equal(b.state.size, want.resting);
  assert.equal(b.state.stat().seq, want.seq, 'the shard op counter restarted');
  assert.equal(b.status().dissents, 0, 'it dissented after resuming');
  await b.stop();
});

test('a checkpoint that does not reproduce its own bookHash is refused, and the log is replayed instead', async () => {
  const dir = tmp();
  const r = await rig(dir);
  const last = r.sealed.at(-1).index;
  const a = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '5' });
  await a.start(); await until(() => a.index === last, 30000, 'catch-up'); await a.stop();

  // tamper: drop one resting order but leave the hash the file claims
  const file = checkpoint.fileOf(path.join(dir, 'miner'), SHARD);
  const snap = JSON.parse(fs.readFileSync(file, 'utf8'));
  const key = Object.keys(snap.books)[0];
  assert.ok(snap.books[key].length >= 1, 'the fixture left no resting orders to tamper with');
  snap.books[key] = snap.books[key].slice(1); snap.resting -= 1;
  fs.writeFileSync(file, JSON.stringify(snap));
  try { fs.unlinkSync(file + '.bak'); } catch { }

  const said = [];
  const b = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '5' }, said);
  await b.start();
  const why = said.find((l) => /ignoring the checkpoint/.test(l));
  assert.ok(why, `the tampered checkpoint was accepted: ${said.join(' | ')}`);
  assert.match(why, /commits to|resting order/);
  assert.equal(b.status().checkpoint.refused, 1);
  // and it still gets to the right book the slow way
  await until(() => b.index === last, 30000, 'the fallback to catch up');
  assert.equal(b.state.bookHash(), r.state.bookHash(), 'the fallback produced a different book than the sequencer');
  await b.stop();
});

test('a checkpoint written under a different commitment mode is refused by name', async () => {
  const dir = tmp();
  const r = await rig(dir);
  const last = r.sealed.at(-1).index;
  const a = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '5' });
  await a.start(); await until(() => a.index === last, 30000, 'catch-up'); await a.stop();
  const file = checkpoint.fileOf(path.join(dir, 'miner'), SHARD);
  const snap = JSON.parse(fs.readFileSync(file, 'utf8'));
  // the tree root and the bookhash root are different words for the same book, so a mode change invalidates it
  snap.commit = 'tree';
  fs.writeFileSync(file, JSON.stringify(snap));
  try { fs.unlinkSync(file + '.bak'); } catch { }
  const said = [];
  const b = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '5' }, said);
  await b.start();
  assert.ok(said.some((l) => /written under L3_COMMIT=tree/.test(l)), `not refused by name: ${said.join(' | ')}`);
  await b.stop();
});

test('L3_CHECKPOINT_BATCHES=0 writes nothing and resumes the old way', async () => {
  const dir = tmp();
  const r = await rig(dir);
  const last = r.sealed.at(-1).index;
  const a = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '0' });
  await a.start(); await until(() => a.index === last, 30000, 'catch-up');
  assert.equal(a.status().checkpoint.written, 0);
  assert.equal(fs.existsSync(checkpoint.fileOf(path.join(dir, 'miner'), SHARD)), false);
  await a.stop();
});

test('the snapshot round-trips through the file, bigints and all', () => {
  const dir = tmp();
  const state = createShardState({});
  const o = (i, buy, price) => ({ t: 'add', hash: '0x' + String(i).padStart(64, '0'), market: 7, outcome: 0,
    order: { user: '0x' + '55'.repeat(20), marketId: '7', outcome: '0', token: '0x' + 'c0'.repeat(20), buy, price: String(parseEther(String(price))), size: String(parseEther('5')), deadline: '99999999999', nonce: '1', salt: String(i), postOnly: false, ioc: false } });
  for (let i = 1; i <= 6; i++) state.apply(o(i, i % 2 === 0, 0.30 + i * 0.01));
  const snap = checkpoint.snapshot(state, { shard: '7', index: 12, epoch: 3, prevRoot: '0x' + 'ab'.repeat(32), offset: 40 });
  checkpoint.write(dir, '7', snap);
  const back = checkpoint.read(dir, '7');
  assert.ok(back, 'the file did not read back');
  const fresh = createShardState({});
  const got = checkpoint.restore(fresh, back, { commitMode: fresh.commit.mode });
  assert.equal(got.index, 12); assert.equal(got.epoch, 3); assert.equal(got.offset, 40);
  assert.equal(fresh.bookHash(), state.bookHash(), 'the restored shard commits differently');
  assert.equal(fresh.size, state.size);
  assert.equal(fresh.stat().seq, state.stat().seq);
  assert.deepEqual(fresh.resting().map((x) => [x.hash, String(x.price), String(x.remaining), x.seq]),
                   state.resting().map((x) => [x.hash, String(x.price), String(x.remaining), x.seq]));
  // a file for another shard, or a future version, is simply not ours
  assert.equal(checkpoint.read(dir, '8'), null);
  const f = checkpoint.fileOf(dir, '7');
  fs.writeFileSync(f, JSON.stringify({ ...back, v: 99 })); try { fs.unlinkSync(f + '.bak'); } catch { }
  assert.equal(checkpoint.read(dir, '7'), null);
});

test('what it buys: a restart over a log of real size', async () => {
  // The point of the whole file. `handleBatch` verifies every order signature, so catching up from the log costs
  // secp256k1 for all of history; restoring a checkpoint costs O(resting). This drives a log big enough for the
  // difference to be a fact rather than an argument, then times both routes to the same book.
  const dir = tmp();
  const ORDERS = Number(process.env.L3_CP_BENCH_ORDERS || 600);
  const r = await rig(dir, { orders: ORDERS, batchMax: 10, epochBatches: 5 });
  const last = r.sealed.at(-1).index;

  // route 1: the log, which is what a brand-new miner and every miner without a checkpoint does
  const cold = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '0' });
  const t0 = Date.now();
  await cold.start();
  await until(() => cold.index === last, 120000, 'the cold miner to catch up');
  const coldMs = Date.now() - t0;
  const want = cold.state.bookHash();
  const verified = cold.status().orders;
  await cold.stop();

  // route 2: write a checkpoint, then start again from it
  const warm = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '20' });
  await warm.start();
  await until(() => warm.index === last, 120000, 'the warm miner to catch up');
  assert.ok(warm.status().checkpoint.written >= 1);
  await warm.stop();

  const resumed = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '20' });
  const t1 = Date.now();
  await resumed.start();
  const loadMs = Date.now() - t1;
  assert.equal(resumed.status().checkpoint.loaded, 1, 'it did not resume from the checkpoint');
  const from = resumed.index;
  await until(() => resumed.index === last, 120000, 'the resumed miner to reach the head');
  const warmMs = Date.now() - t1;
  assert.equal(resumed.state.bookHash(), want, 'the resumed miner reached a different book');
  console.log(`  ${ORDERS} orders, ${last + 1} batches, ${verified} signatures in the log`);
  console.log(`    from the log        ${String(coldMs).padStart(6)} ms  (every signature re-verified)`);
  console.log(`    from a checkpoint   ${String(loadMs).padStart(6)} ms to load at batch ${from} of ${last}, ${warmMs} ms to the head`);
  console.log(`    → ${(coldMs / Math.max(1, warmMs)).toFixed(1)}x less work for the same book, and it does not grow with the log`);
  assert.ok(warmMs < coldMs, `resuming (${warmMs} ms) was not cheaper than the log (${coldMs} ms)`);
  await resumed.stop();
});
