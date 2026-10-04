// node --test engine/l3/miner/fleet.test.mjs — the multi-shard fleet member: the shard list behind /v1/l3/markets,
// reconciling miners against a static list plus a moving engine list, a failed poll that changes nothing, the
// combined health, and a real miner per shard over a FileLog (two shards sequenced, both mined by one fleet).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createFleet, parseShards, shardsOfMarkets } from './fleet.js';
import { createFileLog, ordersTopic } from './log.js';
import { createMiner } from './miner.js';
import { createVerifier } from './verify.js';
import { batchRootOf, ZERO32, ordersRootOf, fillsRootOf } from './merkle.js';
import { signDigest } from './verify.js';
import { BOOK_DOMAIN } from '../desk.js';

const tmp = (tag) => fs.mkdtempSync(path.join(os.tmpdir(), `l3-fleet-${tag}-`));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const until = async (f, ms = 3000) => { const t = Date.now(); while (!f()) { if (Date.now() - t > ms) throw new Error('timeout'); await sleep(10); } };

test('parseShards and shardsOfMarkets name shards the way the sequencer does', () => {
  assert.deepEqual(parseShards(' 13805, 13839,,13805 '), ['13805', '13839']);
  assert.deepEqual(parseShards(''), []); assert.deepEqual(parseShards(undefined), []);
  const markets = [{ market: 13884, outcome: 0 }, { market: 9784, outcome: 0 }, { market: 13884, outcome: 1 }, { market: 'x' }, null, { id: 7 }];
  assert.deepEqual(shardsOfMarkets(markets), ['7', '9784', '13884']);                                       // a market is one shard whatever its books
  assert.deepEqual(shardsOfMarkets(markets, { byOutcome: true }), ['7-0', '9784-0', '13884-0', '13884-1']);  // L3_SHARD_BY=outcome: a shard per book
  assert.deepEqual(shardsOfMarkets(undefined), []); assert.deepEqual(shardsOfMarkets('nope'), []);
});

/// a fake miner that records its life and answers status() like miner.js does
const fakeMiners = () => {
  const log = [];
  const make = (shard, st = {}) => {
    const m = { shard, started: false, stopped: false, s: { index: -1, epoch: 0, offset: -1, stalled: null, batches: 0, votes: 0, dissents: 0, resting: 0, lagSeconds: -1, votesPending: 0, voteFailures: 0, voteRetried: 0, errors: 0, ...st },
      async start() { m.started = true; log.push(`start ${shard}`); }, async stop() { m.stopped = true; log.push(`stop ${shard}`); }, status: () => ({ shard, ...m.s }), quorum: { finalIndex: -1, halted: false } };
    return m;
  };
  return { log, make };
};

test('the fleet mines the static list plus what the engine lists, stops what disappears, and keeps the last answer on a failed poll', async () => {
  const { log, make } = fakeMiners(); const made = new Map();
  let answer = { markets: [{ market: 100, outcome: 0 }, { market: 100, outcome: 1 }, { market: 200, outcome: 0 }] }; let fail = false; let polls = 0;
  const fetchImpl = async (url) => { polls++; assert.match(url, /\/v1\/l3\/markets$/); if (fail) throw new Error('ECONNREFUSED'); return { ok: true, json: async () => answer }; };
  const fleet = createFleet({ newMiner: (s) => { const m = make(s); made.set(s, m); return m; }, static: ['7'], engineUrl: 'http://engine.test/', pollMs: 0, fetchImpl, logger: () => {} });
  await fleet.start();
  assert.deepEqual(fleet.shards().sort(), ['100', '200', '7']);
  assert.equal(fleet.status().engine.url, 'http://engine.test');
  assert.deepEqual(fleet.status().engine.listed, ['100', '200']);
  // the engine drops 200 and adds 300: 200's miner stops, 300's starts, the static 7 stays
  answer = { markets: [{ market: 100, outcome: 0 }, { market: 300, outcome: 0 }] };
  await fleet.reconcile();
  assert.deepEqual(fleet.shards().sort(), ['100', '300', '7']);
  assert.ok(made.get('200').stopped && made.get('300').started);
  // the engine is unreachable: nothing changes, the error is counted and named
  fail = true; await fleet.reconcile();
  assert.deepEqual(fleet.shards().sort(), ['100', '300', '7']);
  assert.equal(fleet.status().engine.pollErrors, 1); assert.match(fleet.status().engine.lastPollError, /ECONNREFUSED/);
  // the engine answers an empty list: only the static shard remains
  fail = false; answer = { markets: [] }; await fleet.reconcile();
  assert.deepEqual(fleet.shards(), ['7']);
  assert.equal(fleet.status().engine.pollErrors, 1);
  await fleet.stop();
  assert.ok(made.get('7').stopped && made.get('100').stopped && made.get('300').stopped);
  assert.equal(polls, 4);
  assert.deepEqual(log.filter((l) => l.startsWith('start')).sort(), ['start 100', 'start 200', 'start 300', 'start 7']);
});

test('health: an idle shard is fine, a stalled one, a dissent or a vote backlog is not; a miner that fails to start is retried', async () => {
  const { make } = fakeMiners(); const miners = new Map(); let boom = true;
  const fleet = createFleet({ newMiner: (s) => { if (s === 'bad' && boom) throw new Error('no leader yet'); const m = make(s); miners.set(s, m); return m; }, static: ['a', 'b', 'bad'], logger: () => {} });
  assert.equal(fleet.healthy(), false);                                   // nothing reconciled yet: not ready
  await fleet.start();
  assert.deepEqual(fleet.shards().sort(), ['a', 'b']);
  assert.equal(fleet.stats.startFailures, 1);
  assert.equal(fleet.healthy(), true);                                    // idle (lagSeconds -1, no batches) is healthy
  boom = false; await fleet.reconcile();                                  // the next pass picks the failed shard up
  assert.deepEqual(fleet.shards().sort(), ['a', 'b', 'bad']);
  miners.get('a').s.lagSeconds = 7200; assert.equal(fleet.healthy(), true, 'a quiet shard is not ill health');
  miners.get('a').s.dissents = 1; assert.equal(fleet.healthy(), false); assert.equal(fleet.status().shards.a.ok, false); miners.get('a').s.dissents = 0;
  miners.get('b').s.stalled = 'the log skipped from 3 to 5'; assert.equal(fleet.healthy(), false); miners.get('b').s.stalled = null;
  miners.get('b').s.voteFailures = 3; miners.get('b').s.voteRetried = 1; assert.equal(fleet.healthy(), false); assert.equal(fleet.status().shards.b.voteBacklog, 2);
  miners.get('b').s.voteRetried = 3; assert.equal(fleet.healthy(), true);
  const text = fleet.metricsText();
  assert.match(text, /rolla_l3_fleet_shards\{miner=""\} 3/); assert.match(text, /rolla_l3_index\{shard="a",miner=""\} -1/); assert.match(text, /rolla_l3_fleet_start_failures_total\{miner=""\} 1/);
  await fleet.stop();
  assert.equal(fleet.shards().length, 0);
});

test('one fleet, two shards, real miners over a FileLog: each shard is replayed by its own book and voted on, both healthy', async () => {
  const dir = tmp('two'); const log = createFileLog({ dir: path.join(dir, 'log') });
  const seqKey = generatePrivateKey(); const seq = privateKeyToAccount(seqKey);
  const account = privateKeyToAccount(generatePrivateKey());
  const domain = BOOK_DOMAIN(46630, '0x' + '11'.repeat(20));
  const verifier = createVerifier({ domain, workers: 0 });
  const fleet = createFleet({
    static: ['41', '42'], address: account.address, logger: () => {},
    newMiner: (shard) => createMiner({ shard, log, account, domain, dir: path.join(dir, 'journal'), verifier, sequencers: [seq.address], threshold: 1, watchVotes: true, env: { CHAIN_ID: '46630' }, logger: () => {} }),
  });
  await fleet.start();
  assert.deepEqual(fleet.shards().sort(), ['41', '42']);
  // an empty batch per shard, sealed by the allowed sequencer (no orders: nothing to verify, the roots are of the empty lists)
  const last = {};   // shard → the previous batchRoot: a batch chains to it (the miner dissents on a broken chain)
  const seal = async (shard, index) => {
    const b = { shard, epoch: 0, index, seqFrom: 0, seqTo: 0, prevRoot: last[shard] || ZERO32, ordersRoot: ordersRootOf([]), fillsRoot: fillsRootOf([]), bookHash: ZERO32, ops: [] };
    b.batchRoot = batchRootOf(b); b.sig = await signDigest(seq, b.batchRoot); last[shard] = b.batchRoot;
    await log.append(ordersTopic(shard), b);
  };
  await seal('41', 0); await seal('42', 0); await seal('42', 1);
  await until(() => fleet.status().shards['41']?.index === 0 && fleet.status().shards['42']?.index === 1, 5000);
  const s = fleet.status();
  assert.equal(s.ok, true, JSON.stringify(s.shards)); assert.equal(s.shards['41'].votes, 1); assert.equal(s.shards['42'].votes, 2); assert.equal(s.shards['42'].dissents, 0);
  await until(() => fleet.status().shards['42']?.finalIndex === 1, 5000);   // its own votes reach its own quorum (threshold 1)
  // a batch from a stranger stalls that shard only, and the fleet says so
  const stranger = privateKeyToAccount(generatePrivateKey());
  const bad = { shard: '41', epoch: 0, index: 1, seqFrom: 0, seqTo: 0, prevRoot: last['41'], ordersRoot: ordersRootOf([]), fillsRoot: fillsRootOf([]), bookHash: ZERO32, ops: [] };
  bad.batchRoot = batchRootOf(bad); bad.sig = await signDigest(stranger, bad.batchRoot);
  await log.append(ordersTopic('41'), bad);
  await until(() => !!fleet.status().shards['41'].stalled, 5000);
  assert.equal(fleet.healthy(), false); assert.equal(fleet.status().shards['42'].ok, true);
  assert.match(fleet.metricsText(), /rolla_l3_stalled\{shard="41",miner="0x[0-9a-f]+"\} 1/);
  await fleet.stop(); await log.close(); await verifier.close();
  // the journals are per shard, under one directory
  assert.ok(fs.existsSync(path.join(dir, 'journal', '41.batches.jsonl')) && fs.existsSync(path.join(dir, 'journal', '42.batches.jsonl')));
});
