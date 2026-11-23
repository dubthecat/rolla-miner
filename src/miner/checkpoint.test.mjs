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

// ---------------------------------------------------------------------------- a checkpoint from the venue
// A miner's own file is self-checking, which proves it is intact — not that the book is the one the venue agreed
// on. A file handed over by the engine needs more than the engine's word, and it does not need a new signature:
// the sequencer already signed the batch at that index, the batch carries the bookHash of the book as of itself,
// and `threshold` miners signing agreement on that batch is what finality IS. So the log settles it.
import * as cpmod from './checkpoint.js';
import { votesTopic } from './log.js';

/// the venue's own view: a sequencer, a miner that votes, and the batch/vote records in the log
async function venue(dir, { orders = 80, batchMax = 5, epochBatches = 4 } = {}) {
  const r = await rig(dir, { orders, batchMax, epochBatches });
  const voter = minerOn(dir, r, { L3_CHECKPOINT_BATCHES: '0' });
  await voter.start();
  const last = r.sealed.at(-1).index;
  await until(() => voter.index === last, 30000, 'the voting miner to catch up');
  // a vote has to be IN the log for the verification to find it
  await until(() => voter.metrics().votes >= 1, 30000, 'a vote in the log');
  return { ...r, voter, last };
}

/// the snapshot the engine would serve: the sequencer's own books at the last epoch boundary
function served(v) {
  const epochBatch = [...v.sealed].reverse().find((b) => b.bookHash && b.bookHash !== ZERO32);
  assert.ok(epochBatch, 'the fixture sealed no epoch boundary');
  const books = new Set([...v.state.books].map(([key, m]) => ({ market: Number(key.split(':')[0]), outcome: Number(key.split(':')[1]), m })));
  return { snap: cpmod.snapshotOfBooks(books, { shard: SHARD, index: Number(epochBatch.index), epoch: Number(epochBatch.epoch), prevRoot: epochBatch.prevRoot, offset: Number(epochBatch.index), seq: Number(epochBatch.seqTo), bookHash: epochBatch.bookHash, commit: v.state.commit.mode }), epochBatch };
}

test('a checkpoint the log confirms is accepted; the venue is never taken at its word', async () => {
  const dir = tmp();
  const v = await venue(dir);
  const { snap, epochBatch } = served(v);
  const ok = await cpmod.verifyAgainstLog(v.log, SHARD, snap, { threshold: 1, ordersTopic, votesTopic });
  assert.equal(ok.ok, true, `the honest checkpoint was refused: ${ok.why}`);
  assert.ok(ok.votes >= 1);
  assert.equal(ok.batchRoot, epochBatch.batchRoot);
  await v.voter.stop();
});

test('a forged checkpoint is refused: the book it claims is not the book the batch committed to', async () => {
  const dir = tmp();
  const v = await venue(dir);
  const { snap } = served(v);
  const cases = [
    // the classic attack: a real batch index, a book of someone's choosing
    [{ ...snap, bookHash: '0x' + 'de'.repeat(32) }, /commits to|the checkpoint says/],
    // a book that never had a commitment: not an epoch boundary
    [{ ...snap, index: snap.index + 1 }, /no batch|carries no book commitment|commits to/],
    // a batch that does not exist
    [{ ...snap, index: 99999 }, /no batch 99999/],
    [{ ...snap, index: -1 }, /index -1/],
    // another shard's batch
    [{ ...snap, shard: '404' }, /no batch/],
  ];
  for (const [bad, why] of cases) {
    const r = await cpmod.verifyAgainstLog(v.log, bad.shard === '404' ? '404' : SHARD, bad, { threshold: 1, ordersTopic, votesTopic });
    assert.equal(r.ok, false, `this forgery was accepted: ${JSON.stringify({ index: bad.index, bookHash: String(bad.bookHash).slice(0, 10) })}`);
    assert.match(r.why, why);
  }
  await v.voter.stop();
});

test('a checkpoint the quorum has not reached the threshold on is refused', async () => {
  const dir = tmp();
  const v = await venue(dir);
  const { snap } = served(v);
  // one miner voted; ask for three
  const r = await cpmod.verifyAgainstLog(v.log, SHARD, snap, { threshold: 3, ordersTopic, votesTopic });
  assert.equal(r.ok, false);
  assert.match(r.why, /agreeing vote\(s\), threshold is 3/);
  await v.voter.stop();
});

test('a joining miner with no journal and no checkpoint of its own starts from the venue\'s', async () => {
  // The whole point: a NEW miner. No journal, no checkpoint, nothing but the log and an engine to ask.
  const dir = tmp();
  const v = await venue(dir);
  const { snap } = served(v);
  // stand up the engine's route, exactly as server.js answers it
  const http = await import('node:http');
  const srv = http.createServer((req, res) => {
    if (req.url.startsWith('/v1/l3/checkpoint')) { res.writeHead(200, { 'content-type': 'application/json' }); return res.end(JSON.stringify(snap)); }
    res.writeHead(404); res.end();
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const url = `http://127.0.0.1:${srv.address().port}`;
  const said = [];
  const fresh = createMiner({
    shard: SHARD, log: v.log, account: privateKeyToAccount(generatePrivateKey()), domain: v.domain,
    dir: path.join(dir, 'joiner'), workers: 0, sequencers: [v.seqAcct.address], threshold: 1, watchVotes: false,
    env: { CHAIN_ID: '46630', L3_CHECKPOINT_BATCHES: '20', L3_CHECKPOINT_URL: url }, logger: (m) => said.push(String(m)),
  });
  try {
    await fresh.start();
    const took = said.find((l) => /took a checkpoint at batch/.test(l));
    assert.ok(took, `it did not take the venue's checkpoint: ${said.join(' | ')}`);
    assert.match(took, /the log confirms its book commitment/);
    assert.equal(fresh.status().checkpoint.fetched, 1);
    assert.equal(fresh.status().replayed, 0, 'it replayed history it was given a shortcut past');
    await until(() => fresh.index === v.last, 30000, 'the joining miner to reach the head');
    assert.equal(fresh.state.bookHash(), v.state.bookHash(), 'the joining miner reached a different book');
    assert.equal(fresh.status().dissents, 0, 'it dissented after joining from a checkpoint');
  } finally { await fresh.stop(); srv.close(); await v.voter.stop(); }
});

test('an engine serving a forged checkpoint gets ignored, and the miner catches up anyway', async () => {
  const dir = tmp();
  const v = await venue(dir);
  const { snap } = served(v);
  const http = await import('node:http');
  const srv = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ...snap, bookHash: '0x' + 'ff'.repeat(32) }));   // a book of its own choosing
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const said = [];
  const fresh = createMiner({
    shard: SHARD, log: v.log, account: privateKeyToAccount(generatePrivateKey()), domain: v.domain,
    dir: path.join(dir, 'joiner2'), workers: 0, sequencers: [v.seqAcct.address], threshold: 1, watchVotes: false,
    env: { CHAIN_ID: '46630', L3_CHECKPOINT_BATCHES: '20', L3_CHECKPOINT_URL: `http://127.0.0.1:${srv.address().port}` }, logger: (m) => said.push(String(m)),
  });
  try {
    await fresh.start();
    assert.ok(said.some((l) => /no usable checkpoint from the venue/.test(l)), `the forgery was accepted: ${said.join(' | ')}`);
    assert.equal(fresh.status().checkpoint.fetched, 0);
    assert.equal(fresh.status().checkpoint.fetchRefused, 1);
    // and it still gets to the right book the honest way
    await until(() => fresh.index === v.last, 60000, 'the fallback to catch up');
    assert.equal(fresh.state.bookHash(), v.state.bookHash());
  } finally { await fresh.stop(); srv.close(); await v.voter.stop(); }
});

// --------------------------------------------------------------------------------- the counter a checkpoint forgot
// Found on the live venue, 2026-11-22. Six of 46 shards sat halted with `finalIndex: -1`, dissenting at 173 of
// 1,071 epoch boundaries — and ONLY on `bookHash`; ordersRoot and fillsRoot agreed on every batch in the log
// (scripts/ops/l3-divergence.mjs). The cause: retention had trimmed the front of those shards' logs, so a miner
// replaying what was left started each book's sequence counter at zero while the engine's was in the thousands,
// and `restingLeaf` is keyed on side/price/SEQ.
//
// Checkpoints are the answer to that — except they had the same hole one step further along. `matcher.restore`
// inferred the counter as `max(resting seq)`, and the counter is spent by `add`, by every fill AND by `cancel`,
// which leaves nothing behind in the resting set. So a restored book's counter came back too low, the next order
// took a sequence number the sequencer had already spent, and the shard dissented at the following boundary.
//
// Nothing at restore time can catch it: the restored book's own root is correct, which is why these tests check
// the NEXT boundary rather than the restore itself.
const E = 10n ** 18n;
const opAdd = (hash, { buy = true, price = 50n, market = 909, outcome = 0 } = {}) => ({
  t: 'add', hash, market, outcome, at: 0,
  order: { user: '0x' + '77'.repeat(20), marketId: String(market), outcome: String(outcome), token: '0x' + 'c0'.repeat(20),
           buy, price: String(price * E / 100n), size: String(5n * E), deadline: '9999999999', nonce: hash, salt: hash, postOnly: false, ioc: false },
});
const opCancel = (hash, { market = 909, outcome = 0 } = {}) => ({ t: 'cancel', hash, market, outcome });
const snapOf = (state) => checkpoint.snapshot(state, { shard: SHARD, index: 7, epoch: 1, prevRoot: ZERO32, offset: 7 });

test('a cancel spends a sequence number, and a checkpoint has to remember it', () => {
  const a = createShardState({});
  a.apply(opAdd('0x' + 'a1'.repeat(32)));                       // seq 1, rests
  a.apply(opAdd('0x' + 'a2'.repeat(32), { buy: false, price: 60n }));  // seq 2, rests
  a.apply(opCancel('0x' + 'a1'.repeat(32)));                    // seq 3, and leaves NO trace in the resting set
  const snap = snapOf(a);
  assert.equal(snap.resting, 1, 'one order left resting');
  assert.equal(snap.counters['909:0'], 3, 'the counter the cancel spent is in the file');

  const b = createShardState({});
  checkpoint.restore(b, snap);
  assert.equal(b.bookHash(), a.bookHash(), 'the restored book commits to the same root');

  // the same next order on both: this is where an inferred counter diverges
  const next = opAdd('0x' + 'a3'.repeat(32), { buy: true, price: 40n });
  a.apply(next); b.apply(next);
  assert.equal(b.bookHash(), a.bookHash(), 'and still does after the next order');
});

test('a book with nothing resting still remembers what it spent', () => {
  const a = createShardState({});
  a.apply(opAdd('0x' + 'b1'.repeat(32)));                       // seq 1
  a.apply(opCancel('0x' + 'b1'.repeat(32)));                    // seq 2 — the book is now empty
  const snap = snapOf(a);
  assert.equal(snap.resting, 0, 'nothing resting');
  assert.equal(snap.counters['909:0'], 2, 'but the counter is kept anyway');

  const b = createShardState({});
  checkpoint.restore(b, snap);
  const next = opAdd('0x' + 'b2'.repeat(32));
  a.apply(next); b.apply(next);
  assert.equal(b.bookHash(), a.bookHash(), 'an empty book restored with its counter stays in agreement');
});

test('fills spend sequence numbers too', () => {
  const a = createShardState({});
  a.apply(opAdd('0x' + 'c1'.repeat(32), { buy: false, price: 40n }));   // seq 1, rests on the ask
  const r = a.apply(opAdd('0x' + 'c2'.repeat(32), { buy: true, price: 50n }));  // seq 2, crosses → a fill bumps it again
  assert.ok(r.fills.length > 0, 'the taker crossed');
  const snap = snapOf(a);

  const b = createShardState({});
  checkpoint.restore(b, snap);
  const next = opAdd('0x' + 'c3'.repeat(32), { buy: true, price: 20n });
  a.apply(next); b.apply(next);
  assert.equal(b.bookHash(), a.bookHash(), 'the fill\'s sequence number is not lost');
});

test('a counter below the newest order it restored is refused', () => {
  const a = createShardState({});
  a.apply(opAdd('0x' + 'd1'.repeat(32)));
  a.apply(opAdd('0x' + 'd2'.repeat(32), { buy: false, price: 60n }));
  const snap = snapOf(a);
  snap.counters['909:0'] = 1;                                   // below the second order's seq 2
  const b = createShardState({});
  assert.throws(() => checkpoint.restore(b, snap), /below the newest restored order/);
});

test('a v1 checkpoint is refused rather than restored with a counter it does not carry', () => {
  const a = createShardState({});
  a.apply(opAdd('0x' + 'e1'.repeat(32)));
  const snap = snapOf(a);
  snap.v = 1;
  const dir = tmp();
  checkpoint.write(dir, SHARD, snap);
  assert.equal(checkpoint.read(dir, SHARD), null, 'read() refuses the older format');
});

test('bootstrap: a shard no vote can settle is recovered on the sequencer signature, not the engine word', async () => {
  const dir = tmp();
  const r = await rig(dir, { orders: 40, batchMax: 5, epochBatches: 4 });
  // a checkpoint is only meaningful at an epoch boundary: that is the only batch that commits to a book
  const at = [...r.sealed].reverse().find((b) => b.bookHash && b.bookHash !== ZERO32);
  assert.ok(at, 'the rig sealed at least one epoch boundary');
  const snap = checkpoint.snapshotOfBooks([{ market: Number(SHARD), outcome: 0, m: r.state.books.get(`${SHARD}:0`) }], {
    shard: SHARD, index: at.index, epoch: at.epoch, prevRoot: at.prevRoot,
    offset: at.index, seq: r.state.stat().seq, bookHash: at.bookHash, commit: null,
  });
  const opts = { threshold: 2, ordersTopic, votesTopic: (sh) => `votes.${sh}` };
  // no votes exist at all: this is the state the live shards were in
  const plain = await checkpoint.verifyAgainstLog(r.log, SHARD, snap, opts);
  assert.equal(plain.ok, false, 'without votes it is refused, as before');
  assert.match(plain.why, /agreeing vote/);

  const boot = await checkpoint.verifyAgainstLog(r.log, SHARD, snap, { ...opts, bootstrap: true, sequencers: [r.seqAcct.address] });
  assert.equal(boot.ok, true, 'and accepted on the sequencer signature when bootstrapping');
  assert.equal(boot.bootstrap, true);
  assert.equal(String(boot.sequencer).toLowerCase(), r.seqAcct.address.toLowerCase());

  // bootstrap is still not a free pass: a different sequencer is refused
  const wrong = await checkpoint.verifyAgainstLog(r.log, SHARD, snap, { ...opts, bootstrap: true, sequencers: ['0x' + '99'.repeat(20)] });
  assert.equal(wrong.ok, false, 'a batch not signed by an allowed sequencer is refused even bootstrapping');
  assert.match(wrong.why, /not signed by a known sequencer/);

  // and a forged book is still refused, because the batch commits to the real one
  const forged = { ...snap, bookHash: '0x' + 'ab'.repeat(32) };
  const bad = await checkpoint.verifyAgainstLog(r.log, SHARD, forged, { ...opts, bootstrap: true, sequencers: [r.seqAcct.address] });
  assert.equal(bad.ok, false, 'bootstrap does not weaken the book-commitment check');
  try { await r.seq.stop(); } catch { }
});

test('a batch is found by its index even when the log offsets have drifted from it', async () => {
  const dir = tmp();
  // enough batches that a skew of 40 does not simply clamp to the start of the log
  const r = await rig(dir, { orders: 300, batchMax: 5, epochBatches: 4 });
  const topic = ordersTopic(SHARD);
  const head = await r.log.offset(topic);
  const all = await r.log.read(topic, 0, 1000);
  assert.ok(all.length > 50, `the rig sealed ${all.length} batches, need > 50`);
  const pickRow = all[all.length - 10];
  const pick = pickRow.value, pickOffset = Number(pickRow.offset);
  // the honest case first
  const direct = await checkpoint.locateBatch(r.log, topic, Number(pick.index), { head });
  assert.ok(direct, 'found it');
  assert.equal(Number(direct.batch.index), Number(pick.index));

  // now a log whose offsets are SHIFTED from its indices, which is what the live venue looks like after a restart
  // that re-appended: reading at offset === index lands on the wrong batch entirely.
  const SKEW = 40;                       // larger than the eight-row window, which is the whole point
  const shifted = {
    offset: async () => head + SKEW,
    read: async (t, from, limit) => {
      const at = Math.max(0, Number(from) - SKEW);
      return (await r.log.read(t, at, limit)).map((row) => ({ offset: Number(row.offset) + SKEW, value: row.value }));
    },
  };
  const naive = await shifted.read(topic, Number(pick.index), 8);
  assert.ok(!naive.map((x) => x.value).some((v) => Number(v.index) === Number(pick.index)),
    'the fixed window at offset === index does NOT contain the batch — this is the bug');
  const found = await checkpoint.locateBatch(shifted, topic, Number(pick.index));
  assert.ok(found, 'bisection finds it anyway');
  assert.equal(Number(found.batch.index), Number(pick.index));
  assert.equal(found.offset, pickOffset + SKEW, 'and reports where it really sits');
  try { await r.seq.stop(); } catch { }
});

test('a checkpoint written while this miner was dissenting carries that on its face', () => {
  const a = createShardState({});
  a.apply(opAdd('0x' + 'f1'.repeat(32)));
  const clean = snapOf(a);
  assert.equal(clean.dissents, undefined, 'snapshot() itself does not invent the field');
  // the miner stamps it (miner.js writeCheckpoint), and `read` must carry it back so start() can see it
  const dir = tmp();
  checkpoint.write(dir, SHARD, { ...clean, dissents: 7 });
  const back = checkpoint.read(dir, SHARD);
  assert.ok(back, 'the file still reads');
  assert.equal(Number(back.dissents), 7, 'and says it was written with dissents on record');
  // and one written in agreement is unmarked, so it is used first
  checkpoint.write(dir, SHARD, { ...clean, dissents: 0 });
  assert.equal(Number(checkpoint.read(dir, SHARD).dissents), 0);
});
