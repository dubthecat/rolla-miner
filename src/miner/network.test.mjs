// node --test engine/l3/miner/network.test.mjs — a SMALL NETWORK, end to end: several shards, a sequencer each,
// three independent miner fleets following a moving engine listing, one replicated log, and an engine-side quorum
// per shard. It prints benchmarks and then breaks things.
//
// The existing cluster test (miner.test.mjs) proves one shard: three books fed the same log agree on every root.
// This one proves the VENUE — the shape that actually broke on 2026-11-11 and 2026-11-19, where no single-shard
// test could see it:
//
//   * the fleet discovers its shards from the engine and reconciles when the listing moves (a resolved market
//     leaves, a new one arrives), and reclaims the journals of shards it no longer mines;
//   * a miner whose journal was truncated by a full disk must refuse it and re-derive from the log — the headless
//     journal replayed onto an empty book was 2,811 forks across 8 halted shards;
//   * a journal that reaches its cap must close without the miner losing a beat;
//   * one Byzantine miner must not be able to stop the venue — the opposite was true until 2026-11-19, when
//     `fork()` on the first dissent plus `haltAfter: 1` gave any single miner a halt switch for any shard;
//   * a restart that re-reads an outage's dissents from the votes topic must not halt a shard whose miners now
//     agree, and must not pay for a signature recovery per historical vote (1,028,595 of them against 18 accepted
//     on the live engine, which was 51 % of all its CPU).
//
// Every number printed here is from this machine and this harness: an in-process FileLog has none of the broker's
// latency, so treat them as a floor for the code path, not as venue capacity. docs/L3-MINERS.md §6 has the
// cross-machine measurements.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { parseEther } from 'viem';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createFileLog, ordersTopic, votesTopic } from './log.js';
import { createSequencer } from './sequencer.js';
import { createMiner, createShardState } from './miner.js';
import { createFleet } from './fleet.js';
import { createQuorum } from './quorum.js';
import { orderHash } from './verify.js';
import { ZERO32 } from './merkle.js';
import { BOOK_DOMAIN, L3_ORDER_TYPES, serializeL3Order } from '../desk.js';

const tmp = (tag) => fs.mkdtempSync(path.join(os.tmpdir(), `l3net-${tag}-`));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const until = async (f, ms = 30000, what = 'condition') => {
  const t = Date.now();
  while (!f()) { if (Date.now() - t > ms) throw new Error(`timed out waiting for ${what}`); await sleep(5); }
};
const quant = (xs, q) => (xs.length ? [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * q))] : null);
const BOOK = '0x' + '11'.repeat(20);

/**
 * A network. Returns the pieces plus `drive(n)`, which signs and sequences n orders per shard, and `settled()`,
 * which waits until every shard's quorum has finalized every batch its sequencer sealed.
 */
async function network({ shards = ['101', '102', '103'], miners = 3, threshold = 2, batchMs = 25, batchMax = 10, epochBatches = 4, env = {}, tamperOn = null, retainDays = 0 } = {}) {
  const dir = tmp('n');
  const log = createFileLog({ dir: path.join(dir, 'log'), pollMs: 2 });
  const domain = BOOK_DOMAIN(46630, BOOK);
  const seqAcct = privateKeyToAccount(generatePrivateKey());

  // one sequencer and one book per shard, as the engine's rig runs them
  const cell = new Map();
  for (const s of shards) {
    const state = createShardState({});
    const sealed = [];
    // small batches on purpose: the epoch boundary is where the book commitment is published and where every
    // divergence between the sequencer's book and a miner's shows up, so a test wants several of them
    const seq = createSequencer({ shard: s, log, account: seqAcct, state, batchMs, batchMax, epochBatches, onSealed: (b) => sealed.push({ ...b, sealedAt: Date.now() }), logger: () => { } });
    await seq.resume();
    cell.set(s, { seq, state, sealed });
  }

  // the engine side: a quorum per shard, reading both topics from the beginning
  const quorums = new Map(); const finals = new Map(); const subs = [];
  for (const s of shards) {
    const got = []; finals.set(s, got);
    const q = createQuorum({ threshold, onFinal: (f) => got.push({ ...f, at: Date.now() }), logger: () => { } });
    quorums.set(s, q);
    subs.push(await log.subscribe(ordersTopic(s), 0, async ({ value }) => { q.announce(value); }));
    subs.push(await log.subscribe(votesTopic(s), 0, async ({ value }) => { await q.vote(value); }));
  }

  // the miners, each a real fleet following the engine's listing
  let listed = [...shards];
  const setListing = (xs) => { listed = [...xs]; };
  const fleets = [];
  for (let i = 0; i < miners; i++) {
    const account = privateKeyToAccount(generatePrivateKey());
    const jdir = path.join(dir, `miner${i + 1}`);
    const tamper = tamperOn === i ? (batch, fills) => (Number(batch.index) >= 2 && fills.length ? fills.slice(1) : fills) : null;
    const fleet = createFleet({
      engineUrl: 'http://engine', pollMs: 0, logger: () => { }, address: account.address,
      journalDir: jdir, journalRetainDays: retainDays, journalSweepMs: 0,
      fetchImpl: async () => ({ ok: true, json: async () => ({ markets: listed.map((m) => ({ market: Number(m) })) }) }),
      newMiner: (s) => createMiner({
        shard: s, log, account, domain, dir: jdir, workers: 0, tamper,
        sequencers: [seqAcct.address], threshold, watchVotes: false,
        env: { CHAIN_ID: '46630', ...env }, logger: () => { },
      }),
    });
    await fleet.start();
    fleets.push({ fleet, account, jdir, i });
  }

  let salt = 0n;
  const users = [privateKeyToAccount(generatePrivateKey()), privateKeyToAccount(generatePrivateKey())];
  /// sign and sequence `n` orders on every shard; returns how long signing and sequencing took
  async function drive(n) {
    const ops = [];
    const tSign = process.hrtime.bigint();
    for (const s of shards) {
      for (let i = 0; i < n; i++) {
        // A third of the flow crosses, so the batches carry FILLS — without them there is no fillsRoot to differ
        // over and a Byzantine miner has nothing to lie about — and a third rests far from the market, so the
        // epoch boundary has a non-empty book to commit to. The two users alternate so the matcher is not
        // self-trading the whole way.
        const acct = users[i % users.length];
        const kind = i % 3;
        const buy = kind !== 1;
        const price = kind === 0 ? 0.55 : kind === 1 ? 0.45 : 0.30;
        const order = {
          user: acct.address, marketId: BigInt(s), outcome: 0n, token: '0x' + 'c0'.repeat(20), buy,
          price: parseEther(String(price)),
          size: parseEther('5'), deadline: BigInt(Math.floor(Date.now() / 1000) + 3600),
          nonce: BigInt(Date.now()), salt: ++salt, postOnly: false, ioc: false,
        };
        const ser = serializeL3Order(order);
        const sig = await acct.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: order });
        ops.push({ shard: s, op: { t: 'add', hash: orderHash(domain, ser), market: Number(s), outcome: 0, order: ser, sig, signer: acct.address.toLowerCase(), at: Date.now() } });
      }
    }
    const signMs = Number(process.hrtime.bigint() - tSign) / 1e6;
    const tSeq = process.hrtime.bigint();
    for (let i = 0; i < ops.length; i++) {
      cell.get(ops[i].shard).seq.submit(ops[i].op);
      if (i % 25 === 0) await new Promise((r) => setImmediate(r));
    }
    for (const s of shards) await cell.get(s).seq.flush();
    const seqMs = Number(process.hrtime.bigint() - tSeq) / 1e6;
    return { orders: ops.length, signMs, seqMs };
  }

  /// wait until every shard's quorum has finalized every sealed batch
  async function settled(ms = 30000) {
    await until(() => shards.every((s) => {
      const { sealed } = cell.get(s); const q = quorums.get(s);
      return sealed.length > 0 && q.finalIndex >= sealed[sealed.length - 1].index;
    }), ms, 'every shard to finalize every batch');
  }

  async function stop() {
    for (const f of fleets) { try { await f.fleet.stop(); } catch { } }
    for (const s of shards) { try { await cell.get(s).seq.stop?.(); } catch { } }
    for (const su of subs) { try { su.close && su.close(); } catch { } }
  }

  return { dir, log, domain, seqAcct, shards, cell, quorums, finals, fleets, drive, settled, stop, setListing,
           reconcileAll: async () => { for (const f of fleets) await f.fleet.reconcile(); } };
}

/// the journal bytes a miner holds, per shard
const journalBytes = (jdir) => {
  let n = 0;
  try { for (const f of fs.readdirSync(jdir)) n += fs.statSync(path.join(jdir, f)).size; } catch { }
  return n;
};

test('a network of 3 shards and 3 miners finalizes every batch, and every book is the same book', async () => {
  const net = await network({ shards: ['101', '102', '103'], miners: 3, threshold: 2 });
  try {
    const bench = await net.drive(40);
    await net.settled();

    let batches = 0, votes = 0;
    const lat = [];
    for (const s of net.shards) {
      const { sealed, state } = net.cell.get(s);
      const q = net.quorums.get(s);
      batches += sealed.length;
      votes += q.status().votes;
      assert.ok(sealed.length >= 1, `shard ${s} sealed nothing`);
      assert.equal(q.finalIndex, sealed[sealed.length - 1].index, `shard ${s} stopped at ${q.finalIndex} of ${sealed[sealed.length - 1].index}`);
      assert.equal(q.halted, false, `shard ${s} halted`);
      assert.equal(q.status().forks, 0, `shard ${s} forked`);
      // every miner of the fleet holds the sequencer's book for this shard
      const want = state.bookHash();
      for (const f of net.fleets) {
        const m = f.fleet.minerOf(s);
        assert.ok(m, `miner ${f.i + 1} is not mining ${s}`);
        assert.equal(m.state.bookHash(), want, `miner ${f.i + 1} has a different book on ${s}`);
        assert.equal(m.status().dissents, 0, `miner ${f.i + 1} dissented on ${s}`);
        assert.equal(m.status().stalled, null);
      }
      // seal → final, per batch
      const got = net.finals.get(s);
      for (const b of sealed) { const fin = got.find((x) => x.index === b.index); if (fin) lat.push(fin.at - b.sealedAt); }
      // an epoch boundary carried a book commitment, which is the root the miners had to reproduce
      assert.ok(sealed.some((b) => b.bookHash !== ZERO32), `shard ${s} never closed an epoch`);
    }
    const bytes = net.fleets.reduce((n, f) => n + journalBytes(f.jdir), 0);
    console.log(`  network: ${net.shards.length} shards x ${net.fleets.length} miners · ${bench.orders} orders`);
    console.log(`    sign      ${(bench.orders / (bench.signMs / 1000)).toFixed(0)}/s (${(bench.signMs / bench.orders).toFixed(2)} ms each, viem EIP-712)`);
    console.log(`    sequence  ${(bench.orders / (bench.seqMs / 1000)).toFixed(0)}/s venue-wide into ${batches} batches`);
    console.log(`    finality  p50 ${quant(lat, 0.5)} ms · p90 ${quant(lat, 0.9)} ms · ${lat.length} batches measured (in-process log)`);
    console.log(`    votes     ${votes} verified by the engine-side quorums · journals ${(bytes / 1024).toFixed(0)} KB over ${net.fleets.length} miners`);
  } finally { await net.stop(); }
});

test('the fleet follows the engine: a resolved market leaves, its journal is reclaimed, a new market is picked up', async () => {
  // This is the loop that mines the venue, and the reason the disks filled: the engine used to list every book it
  // held — 927 shards for 107 open markets — and the sweep keeps the journal of every shard the list names.
  const net = await network({ shards: ['201', '202'], miners: 2, threshold: 2, retainDays: 0 });
  try {
    await net.drive(8);
    await net.settled();
    for (const f of net.fleets) assert.deepEqual(f.fleet.shards().sort(), ['201', '202']);
    assert.ok(fs.existsSync(path.join(net.fleets[0].jdir, '201.batches.jsonl')), 'a mined shard has a journal');

    // 201 resolves: the engine stops listing it
    net.setListing(['202']);
    await net.reconcileAll();
    for (const f of net.fleets) assert.deepEqual(f.fleet.shards(), ['202'], 'the miner for the resolved market was stopped');
    // and with the retention window at zero its journal goes on the next sweep
    for (const f of net.fleets) {
      const swept = f.fleet.sweepJournals();
      assert.ok(swept.removed >= 1, `nothing reclaimed: ${JSON.stringify(swept)}`);
      assert.equal(fs.existsSync(path.join(f.jdir, '201.batches.jsonl')), false);
      assert.ok(fs.existsSync(path.join(f.jdir, '202.batches.jsonl')), 'the live shard keeps its journal');
    }

    // a new market opens: the next poll picks it up and it finalizes like the rest
    const state = createShardState({});
    const sealed = [];
    const seq = createSequencer({ shard: '203', log: net.log, account: net.seqAcct, state, batchMs: 25, batchMax: 200, epochBatches: 4, onSealed: (b) => sealed.push({ ...b, sealedAt: Date.now() }), logger: () => { } });
    await seq.resume();
    net.cell.set('203', { seq, state, sealed });
    const q = createQuorum({ threshold: 2, logger: () => { } });
    net.quorums.set('203', q); net.finals.set('203', []);
    await net.log.subscribe(ordersTopic('203'), 0, async ({ value }) => { q.announce(value); });
    await net.log.subscribe(votesTopic('203'), 0, async ({ value }) => { await q.vote(value); });
    net.setListing(['202', '203']);
    await net.reconcileAll();
    for (const f of net.fleets) assert.deepEqual(f.fleet.shards().sort(), ['202', '203']);
    net.shards.push('203');
    await net.drive(6);
    await until(() => q.finalIndex >= sealed[sealed.length - 1].index, 30000, 'the new shard to finalize');
    assert.equal(q.halted, false);
  } finally { await net.stop(); }
});

test('a full disk: one miner loses the head of its journal, refuses it, and re-derives the same book from the log', async () => {
  // The exact 2026-11-11 shape. Every journal append failed with ENOSPC, and the miners came back replaying
  // journals whose first batches were gone: replay rebuilds the book by applying every op from the start, so a
  // headless journal replays from the middle onto an empty book and the miner then computes a different root for
  // every batch after it — 2,811 forks across 8 halted shards, every one `minerWhy: null`. The journal's own
  // self-consistency check cannot catch it, because the root it compares against is read from the same file.
  const net = await network({ shards: ['301'], miners: 3, threshold: 2 });
  try {
    await net.drive(30);
    await net.settled();
    const want = net.cell.get('301').state.bookHash();
    const victim = net.fleets[2];
    const before = victim.fleet.minerOf('301').state.bookHash();
    assert.equal(before, want);

    // the disk filled: stop that miner and cut the head off its journal
    await victim.fleet.stop();
    const jf = path.join(victim.jdir, '301.batches.jsonl');
    const lines = fs.readFileSync(jf, 'utf8').split('\n').filter(Boolean);
    assert.ok(lines.length >= 4, `only ${lines.length} journalled batches to cut`);
    fs.writeFileSync(jf, lines.slice(2).join('\n') + '\n');

    // it comes back, refuses the headless journal by name, and re-derives from the log
    const said = [];
    const account = victim.account;
    const revived = createMiner({ shard: '301', log: net.log, account, domain: net.domain, dir: victim.jdir, workers: 0,
      sequencers: [net.seqAcct.address], threshold: 2, watchVotes: false, env: { CHAIN_ID: '46630' }, logger: (m) => said.push(String(m)) });
    await revived.start();
    try {
      assert.ok(said.some((l) => /head was lost/.test(l)), `the refusal is named: ${said.join(' | ')}`);
      await until(() => revived.index === net.cell.get('301').seq.index - 1, 30000, 'the revived miner to catch up');
      assert.equal(revived.state.bookHash(), want, 'the revived miner rebuilt a different book');
      assert.equal(revived.status().dissents, 0, 'it dissented while catching up');
      // and the shard was never halted: the other two carried finality throughout
      assert.equal(net.quorums.get('301').halted, false);
      assert.equal(net.quorums.get('301').status().forks, 0);
    } finally { await revived.stop(); }
  } finally { await net.stop(); }
});

test('a journal that reaches its cap closes, and the miner does not miss a batch', async () => {
  // A hot shard's journal pair reached 78 MB on a 3 GB volume while every append was failing. The cap closes the
  // journal at a batch boundary instead of rotating it, because replayJournal() refuses a journal that does not
  // start at batch 0 — so a closed one still replays to its last batch and the log supplies the rest.
  const net = await network({ shards: ['401'], miners: 2, threshold: 2, env: { L3_JOURNAL_MAX_BYTES: '4096' } });
  try {
    await net.drive(60);
    await net.settled();
    const m = net.fleets[0].fleet.minerOf('401');
    const st = m.status();
    assert.equal(st.journalCapped, true, `the journal never reached the 4 KB cap (${st.journalBytes} B)`);
    assert.ok(st.journalBytes <= 4096 * 3, `the cap did not hold: ${st.journalBytes} B`);
    // it kept mining regardless, and the shard stayed final
    assert.equal(st.index, net.cell.get('401').seq.index - 1, 'the miner fell behind after its journal closed');
    assert.equal(st.dissents, 0);
    assert.equal(net.quorums.get('401').halted, false);
    assert.equal(net.quorums.get('401').finalIndex, net.cell.get('401').sealed.at(-1).index);
    // the journal it does hold starts at batch 0, so it is still replayable
    const first = JSON.parse(fs.readFileSync(path.join(net.fleets[0].jdir, '401.batches.jsonl'), 'utf8').split('\n')[0]);
    assert.equal(first.index, 0, 'a capped journal must keep its head or it is worth nothing');
  } finally { await net.stop(); }
});

test('one Byzantine miner cannot stop the venue: it is named, earns nothing, and finality carries on', async () => {
  // Until 2026-11-19 it could: classify() called fork() on the first dissent and haltAfter is 1, so any single
  // miner had a halt switch for any shard — the opposite of what a 2-of-3 quorum is for.
  const net = await network({ shards: ['501'], miners: 3, threshold: 2, tamperOn: 2 });
  try {
    await net.drive(40);
    const q = net.quorums.get('501');
    const { sealed } = net.cell.get('501');
    await until(() => q.finalIndex >= sealed.at(-1).index, 30000, 'the honest two to finalize everything');
    assert.equal(q.halted, false, 'one miner of three halted the shard');
    assert.deepEqual(q.forks(), [], 'a dissent the quorum outvoted is not a fork');

    const bad = net.fleets[2].account.address.toLowerCase();
    const liar = net.fleets[2].fleet.minerOf('501');
    assert.ok(liar.status().dissents >= 1, 'the tampered miner never dissented, so nothing was tested');
    // it is named, with the evidence, on every batch it lied about
    const dis = q.dissents().filter((d) => d.miner === bad);
    assert.ok(dis.length >= 1, `the tamper was not recorded: ${JSON.stringify(q.dissents().map((d) => d.miner))}`);
    assert.notEqual(dis[0].got, dis[0].claimed);
    // and it is not in the agreeing set of those batches, so it earns nothing for them
    for (const d of dis) assert.equal(q.at(d.index).dissent.includes(bad), true);
    // the honest two hold the sequencer's book; the liar does not
    const want = net.cell.get('501').state.bookHash();
    assert.equal(net.fleets[0].fleet.minerOf('501').state.bookHash(), want);
    assert.equal(net.fleets[1].fleet.minerOf('501').state.bookHash(), want);
  } finally { await net.stop(); }
});

test('a restart re-reads an outage\'s dissents and neither halts nor pays for them', async () => {
  // Both halves of the 2026-11-19 fix, in the shape that produced them. A miner's quorum re-reads the votes topic
  // from offset 0 at every start (votes are keyed by miner, so re-reading one is harmless and a restart needs it),
  // so every historical dissent arrives again. It must not raise a live fork — four of miner-1's six shards sat
  // halted at finalIndex -1 with 7,709 of 8,116 votes agreeing — and it must not spend a signature recovery each,
  // which was 1,028,595 recoveries against 18 accepted votes, 51 % of all the engine's CPU.
  const net = await network({ shards: ['601'], miners: 3, threshold: 2, tamperOn: 2 });
  try {
    await net.drive(40);
    const { sealed } = net.cell.get('601');
    await until(() => net.quorums.get('601').finalIndex >= sealed.at(-1).index, 30000, 'finality');
    const dissented = net.quorums.get('601').dissents().length;
    assert.ok(dissented >= 1, 'the outage left no dissents in the log to replay');

    // A fresh quorum over the same log, built the way a restart builds one: the floor is the log's tail (sequencer.js
    // resume() → `quorum.start(from)`), and only the batches from that floor onward are re-announced — which is why
    // every vote below it is history. The votes topic, though, is always re-read from offset 0.
    let recoveries = 0;
    const fresh = createQuorum({ threshold: 2, logger: () => { }, verify: async (v) => { recoveries++; return String(v.miner).toLowerCase(); } });
    fresh.start(sealed.at(-1).index + 1);
    const v = await net.log.subscribe(votesTopic('601'), 0, async ({ value }) => { await fresh.vote(value); });
    await sleep(600);
    try {
      assert.equal(fresh.halted, false, 'a restart halted on history');
      assert.deepEqual(fresh.forks(), []);
      assert.ok(fresh.status().stale > 0, 'nothing was recognised as history');
      assert.equal(fresh.status().votes, 0, 'a settled vote was still counted');
      assert.equal(recoveries, 0, `${recoveries} signature(s) recovered for votes already settled`);
    } finally { v.close && v.close(); }
  } finally { await net.stop(); }
});

test('scale: 8 shards, 3 miners, every batch final — and what it cost', async () => {
  // The venue's shape at the top of its range on one machine. The point is not the number (an in-process FileLog
  // has none of the broker's latency) but that nothing degrades as shards multiply: the fleet shares one log
  // connection and one verifier per process, so N shards cost N books and 2N subscriptions, not N worker pools.
  // `L3_NET_SHARDS=32 L3_NET_ORDERS=200 L3_NET_MINERS=3 node --test engine/l3/miner/network.test.mjs` turns this
  // into a benchmark run without changing the default suite's cost.
  const S = Math.max(1, Number(process.env.L3_NET_SHARDS || 8));
  const N = Math.max(1, Number(process.env.L3_NET_ORDERS || 25));
  const M = Math.max(1, Number(process.env.L3_NET_MINERS || 3));
  const shards = Array.from({ length: S }, (_, i) => String(701 + i));
  const net = await network({ shards, miners: M, threshold: Math.min(2, M), batchMs: 20, epochBatches: 5 });
  try {
    const rss0 = process.memoryUsage().rss;
    const bench = await net.drive(N);
    await net.settled(Math.max(60000, S * N * 40));
    const rss = process.memoryUsage().rss;
    let batches = 0, votes = 0, dissents = 0; const lat = [];
    for (const s of shards) {
      const { sealed, state } = net.cell.get(s); const q = net.quorums.get(s);
      batches += sealed.length; votes += q.status().votes;
      assert.equal(q.finalIndex, sealed.at(-1).index, `shard ${s} stalled at ${q.finalIndex} of ${sealed.at(-1).index}`);
      assert.equal(q.halted, false); assert.equal(q.status().forks, 0);
      const want = state.bookHash();
      for (const f of net.fleets) { const m = f.fleet.minerOf(s); assert.equal(m.state.bookHash(), want, `miner ${f.i + 1} differs on ${s}`); dissents += m.status().dissents; }
      const got = net.finals.get(s);
      for (const b of sealed) { const fin = got.find((x) => x.index === b.index); if (fin) lat.push(fin.at - b.sealedAt); }
    }
    assert.equal(dissents, 0, 'a miner dissented with nothing wrong');
    for (const f of net.fleets) assert.equal(f.fleet.healthy(), true, `miner ${f.i + 1} is not healthy`);
    console.log(`  scale: ${shards.length} shards x ${net.fleets.length} miners · ${bench.orders} orders · ${batches} batches`);
    console.log(`    sequence  ${(bench.orders / (bench.seqMs / 1000)).toFixed(0)}/s venue-wide · finality p50 ${quant(lat, 0.5)} ms · p90 ${quant(lat, 0.9)} ms`);
    console.log(`    ${votes} votes verified · rss +${((rss - rss0) / 1048576).toFixed(0)} MB for ${shards.length * net.fleets.length} miner-shards · journals ${(net.fleets.reduce((n, f) => n + journalBytes(f.jdir), 0) / 1024).toFixed(0)} KB`);
  } finally { await net.stop(); }
});
