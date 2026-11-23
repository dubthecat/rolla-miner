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

test('health: starting is not broken, an idle shard is fine, a dissent it recovered from is fine; disagreeing now, a stall or a vote backlog is not; a miner that fails to start is retried', async () => {
  const { make } = fakeMiners(); const miners = new Map(); let boom = true;
  const fleet = createFleet({ newMiner: (s) => { if (s === 'bad' && boom) throw new Error('no leader yet'); const m = make(s); miners.set(s, m); return m; }, static: ['a', 'b', 'bad'], logger: () => {} });
  assert.equal(fleet.healthy(), false);                                   // not started: we do not know what to mine yet
  await fleet.start();
  assert.deepEqual(fleet.shards().sort(), ['a', 'b']);
  assert.equal(fleet.stats.startFailures, 1);
  assert.equal(fleet.healthy(), true);                                    // idle (lagSeconds -1, no batches) is healthy
  boom = false; await fleet.reconcile();                                  // the next pass picks the failed shard up
  assert.deepEqual(fleet.shards().sort(), ['a', 'b', 'bad']);
  miners.get('a').s.lagSeconds = 7200; assert.equal(fleet.healthy(), true, 'a quiet shard is not ill health');
  // A dissent in the PAST is not ill health. The test used to be the cumulative counter, so one dissent ever marked
  // the shard unhealthy for the life of the process — and after the volumes filled on 2026-11-11 every shard that
  // had replayed a truncated journal carried one, so the fleet could never report healthy again however well it ran.
  miners.get('a').s.dissents = 1; assert.equal(fleet.healthy(), true, 'a dissent it has since recovered from');
  assert.equal(fleet.status().shards.a.dissents, 1, 'still reported, just not a verdict');
  // disagreeing with the sequencer about the batch it just processed IS
  miners.get('a').s.lastVote = { index: 9, ok: false };
  assert.equal(fleet.healthy(), false); assert.equal(fleet.status().shards.a.ok, false); assert.equal(fleet.status().shards.a.disagreeing, true);
  miners.get('a').s.lastVote = { index: 10, ok: true }; miners.get('a').s.dissents = 0;
  assert.equal(fleet.healthy(), true, 'it agrees again');
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

// ---- the journal sweep -----------------------------------------------------------------------------------------------
// A miner journals every batch it signs and every vote it casts, per shard, and nothing ever removed those files: 458
// of them holding 958 MB on a 1 GB volume by 2026-11-11, all three fleet machines at 100 %, so every append failed
// with ENOSPC and no miner could record what it had signed. 798 MB of it belonged to shards whose markets had left
// the engine's snapshot entirely.
//
// A journal is read in exactly one place — replayJournal(), when a miner for THAT shard starts — so a shard the fleet
// does not mine has no reader. These pin that the sweep removes only those, and only once they are old.
test('the journal sweep removes the journals of unmined shards, keeps the mined ones, and respects the age window', async () => {
  const dir = tmp('sweep'); const jdir = path.join(dir, 'journal'); fs.mkdirSync(jdir, { recursive: true });
  const old = Date.now() - 30 * 86400000, fresh = Date.now() - 60000;
  const put = (name, when) => { const f = path.join(jdir, name); fs.writeFileSync(f, '{}\n'); fs.utimesSync(f, when / 1000, when / 1000); return f; };
  // two shards the fleet mines, two it does not, one of those only just dropped, plus a corrupt spill-over
  put('41.batches.jsonl', old); put('41.votes.jsonl', old);
  put('42.batches.jsonl', old); put('42.votes.jsonl', old);
  put('900.batches.jsonl', old); put('900.votes.jsonl', old);
  put('901.batches.jsonl', fresh);
  put('902.batches.jsonl.corrupt.123', old);
  put('notajournal.txt', old);

  const fleet = createFleet({
    static: ['41', '42'], address: '0x' + '11'.repeat(20), logger: () => {},
    journalDir: jdir, journalRetainDays: 7,
    newMiner: (shard) => ({ start: async () => {}, stop: async () => {}, status: () => ({ shard, ok: true, index: 0, stalled: null, voteFailures: 0, voteRetried: 0 }) }),
  });
  await fleet.start();
  const r = fleet.sweepJournals();
  const left = fs.readdirSync(jdir).sort();
  // mined shards keep theirs, however old
  assert.ok(left.includes('41.batches.jsonl') && left.includes('41.votes.jsonl'), 'a mined shard keeps its journal');
  assert.ok(left.includes('42.batches.jsonl'), 'and the second mined shard');
  // an unmined, old shard loses both files, and its corrupt spill-over too
  assert.equal(left.includes('900.batches.jsonl'), false);
  assert.equal(left.includes('900.votes.jsonl'), false);
  assert.equal(left.includes('902.batches.jsonl.corrupt.123'), false, 'a corrupt copy is journal data too');
  // an unmined shard dropped only moments ago keeps its journal: it may be re-listed and resume by replay
  assert.ok(left.includes('901.batches.jsonl'), 'inside the age window it is kept');
  // and nothing that is not a journal is touched
  assert.ok(left.includes('notajournal.txt'));
  assert.equal(r.removed, 3); assert.ok(r.freedBytes > 0);
  assert.equal(fleet.status().journals.removed, 3);
  await fleet.stop();
});

test('the sweep is inert without a journal dir, and survives a dir that is not there', async () => {
  const mk = (journalDir) => createFleet({
    static: ['41'], address: '0x' + '22'.repeat(20), logger: () => {}, journalDir,
    newMiner: (shard) => ({ start: async () => {}, stop: async () => {}, status: () => ({ shard, ok: true, index: 0, stalled: null, voteFailures: 0, voteRetried: 0 }) }),
  });
  const a = mk(null); await a.start();
  assert.equal(a.status().journals, null, 'no dir, nothing reported');
  assert.equal(a.sweepJournals().removed, 0);
  await a.stop();
  const b = mk(path.join(tmp('gone'), 'nope')); await b.start();
  const r = b.sweepJournals();
  assert.equal(r.removed, 0); assert.ok(r.error, 'it reports rather than throwing');
  await b.stop();
});

test('the sweep waits for the engine listing, and keeps a shard it is about to mine', async () => {
  const dir = tmp('intent'); const jdir = path.join(dir, 'journal'); fs.mkdirSync(jdir, { recursive: true });
  const old = Date.now() - 30 * 86400000;
  const put = (name) => { const f = path.join(jdir, name); fs.writeFileSync(f, '{}\n'); fs.utimesSync(f, old / 1000, old / 1000); };
  put('700.batches.jsonl'); put('800.batches.jsonl');
  // an engine that lists 700 but is slow to answer: before the first poll nothing may be deleted, because the target
  // set is unknown and 700's journal is about to be needed
  let answer = null;
  const fetchImpl = async () => { if (!answer) throw new Error('engine down'); return { ok: true, json: async () => answer }; };
  const fleet = createFleet({
    engineUrl: 'http://engine.invalid', pollMs: 0, address: '0x' + '33'.repeat(20), logger: () => {},
    journalDir: jdir, journalRetainDays: 7, journalSweepMs: 0, fetchImpl,
    newMiner: (shard) => ({ start: async () => {}, stop: async () => {}, status: () => ({ shard, ok: true, index: 0, stalled: null, voteFailures: 0, voteRetried: 0 }) }),
  });
  await fleet.start();
  assert.equal(fleet.sweepJournals().removed, 0, 'no successful poll yet: nothing is judged');
  assert.deepEqual(fs.readdirSync(jdir).sort(), ['700.batches.jsonl', '800.batches.jsonl']);
  // now the engine answers with 700 only
  answer = { markets: [{ id: 700, outcomes: 2 }] };
  await fleet.reconcile();
  const r = fleet.sweepJournals();
  assert.equal(r.removed, 1, 'only the shard the engine does not list');
  assert.deepEqual(fs.readdirSync(jdir), ['700.batches.jsonl'], '700 is kept because it is intended, started or not');
  await fleet.stop();
});

test('a fleet still bringing its shards up answers 200, not 503', async () => {
  // `ok` used to be `ready`, which only turns true after the FIRST reconcile — and a reconcile starts shards one at a
  // time, each replaying its journal op by op, so on the live fleet's 101 shards it runs for over an hour. Every
  // `fly deploy` of a miner therefore failed its health check ("timeout reached waiting for health checks to pass")
  // while the machine underneath was perfectly fine, which is how the three miners ended up on three images.
  const { make } = fakeMiners();
  let release; const gate = new Promise((r) => { release = r; });
  const fleet = createFleet({
    // a shard that takes its time to come up: on the live fleet that is a journal replayed op by op, ~1 min each
    newMiner: (s) => { const m = make(s); const real = m.start; m.start = async () => { await gate; return real(); }; return m; },
    engineUrl: 'http://engine', pollMs: 0, logger: () => {},
    fetchImpl: async () => ({ ok: true, json: async () => ({ markets: [{ market: 1 }, { market: 2 }] }) }),
  });
  const starting = fleet.start();
  await new Promise((r) => setTimeout(r, 20));
  const s = fleet.status();
  assert.equal(s.ok, true, 'a fleet mid-reconcile is healthy');
  assert.equal(s.starting, true, 'and says so');
  assert.equal(s.ready, false);
  release(); await starting;
  assert.equal(fleet.status().starting, false, 'once every shard is up it is no longer starting');
  assert.equal(fleet.healthy(), true);
  await fleet.stop();
});

test('an engine the fleet has never reached is still a 503: it does not know what to mine', async () => {
  const { make } = fakeMiners();
  const fleet = createFleet({
    newMiner: (s) => make(s), engineUrl: 'http://engine', pollMs: 0, logger: () => {},
    fetchImpl: async () => { throw new Error('ECONNREFUSED'); },
  });
  await fleet.start();
  assert.equal(fleet.healthy(), false, 'no listing has ever arrived');
  assert.equal(fleet.status().starting, false);
  await fleet.stop();
});

// ------------------------------------------------------------------ dividing the shards between the fleet
// Why this matters more than it looks: with every miner running every shard, the replication factor IS the miner
// count, so venue capacity — (cores x per-core verify rate) / replication — does not move when a miner is added.
// A fourth miner verifies everything a fourth time and the venue goes no faster. Holding R fixed and dividing the
// shards is what turns a new machine into throughput.
const ADDRS = Array.from({ length: 9 }, (_, i) => '0x' + String(i + 1).padStart(2, '0').repeat(20));

/// a fleet that only pretends to mine, so the shard SET is what is under test
function listFleet({ me, roster, replicas, shards, assign, fixed = [] }) {
  const made = new Map();
  const fetchImpl = async () => ({ ok: true, json: async () => ({ markets: shards.map((s) => ({ market: Number(s), outcome: 0 })), miners: roster, replicas }) });
  const fleet = createFleet({
    newMiner: (s) => { const m = { start: async () => m, stop: async () => {}, status: () => ({ index: -1, votes: 0, dissents: 0, resting: 0, lagSeconds: 0, votesPending: 0, batches: 0, stalled: null, errors: 0 }), metrics: () => ({}), quorum: null };
      made.set(s, m); return m; },
    static: fixed, engineUrl: 'http://engine.test/', pollMs: 0, address: me, fetchImpl, logger: () => {},
    env: { L3_SHARD_ASSIGN: assign ? '1' : '0', L3_SHARD_REPLICAS: String(replicas) },
  });
  return fleet;
}

test('assignment off: the fleet wants every shard the engine lists, as it always has', async () => {
  const shards = Array.from({ length: 30 }, (_, i) => String(19000 + i));
  const fleet = listFleet({ me: ADDRS[0], roster: ADDRS, replicas: 3, shards, assign: false });
  await fleet.reconcile();
  assert.equal(fleet.shards().length, 30);
  assert.equal(fleet.status().assign.on, false);
  await fleet.stop();
});

test('assignment on with a roster of 9 and R=3: this miner takes about a third of the shards', async () => {
  const shards = Array.from({ length: 30 }, (_, i) => String(19000 + i));
  const fleet = listFleet({ me: ADDRS[0], roster: ADDRS, replicas: 3, shards, assign: true });
  await fleet.reconcile();
  const mine = fleet.shards().length;
  const a = fleet.status().assign;
  assert.equal(a.on, true);
  assert.equal(a.roster, 9);
  assert.equal(a.everyone, false, 'a roster bigger than R must divide');
  // ideal is 3*30/9 = 10, capped a little above
  assert.ok(mine >= 5 && mine <= 14, `took ${mine} of 30, expected around 10`);
  assert.equal(a.under, 0, 'every shard is still covered three times');
  await fleet.stop();
});

test('every shard is covered exactly R times across the whole roster, and none is orphaned', async () => {
  const shards = Array.from({ length: 30 }, (_, i) => String(19000 + i));
  const counts = new Map(shards.map((s) => [s, 0]));
  for (const me of ADDRS) {
    const fleet = listFleet({ me, roster: ADDRS, replicas: 3, shards, assign: true });
    await fleet.reconcile();
    for (const s of fleet.shards()) counts.set(s, counts.get(s) + 1);
    await fleet.stop();
  }
  for (const [s, n] of counts) assert.equal(n, 3, `shard ${s} is covered ${n} time(s), not 3`);
  // 90 placements over 9 miners, and the venue verifies each order 3 times rather than 9
});

test('a roster no bigger than R divides nothing: three miners is exactly today', async () => {
  const shards = Array.from({ length: 29 }, (_, i) => String(19000 + i));
  const three = ADDRS.slice(0, 3);
  for (const me of three) {
    const fleet = listFleet({ me, roster: three, replicas: 3, shards, assign: true });
    await fleet.reconcile();
    assert.equal(fleet.shards().length, 29, 'every shard, as the live fleet does now');
    assert.equal(fleet.status().assign.everyone, true);
    await fleet.stop();
  }
});

test('a miner not on the roster keeps mining everything rather than nothing', async () => {
  const shards = Array.from({ length: 20 }, (_, i) => String(19000 + i));
  const fleet = listFleet({ me: '0x' + 'ee'.repeat(20), roster: ADDRS, replicas: 3, shards, assign: true });
  await fleet.reconcile();
  assert.equal(fleet.shards().length, 20, 'an unknown miner must not silently stop working');
  await fleet.stop();
});

test('L3_SHARDS is never divided away: an explicitly named shard stays', async () => {
  const shards = Array.from({ length: 30 }, (_, i) => String(19000 + i));
  // '7' is not in the engine's list and is named statically; it must survive the assignment
  const fleet = listFleet({ me: ADDRS[0], roster: ADDRS, replicas: 3, shards, assign: true, fixed: ['7'] });
  await fleet.reconcile();
  assert.ok(fleet.shards().includes('7'), 'the operator asked for this one by name');
  await fleet.stop();
});
