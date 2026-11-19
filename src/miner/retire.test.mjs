// The broker keeps `orders.<market>` and `votes.<market>` for every market the venue has ever sequenced and
// reclaimed neither: 2,045 topics and 26 GB by 2026-11-16. Each topic is a raft group, and on one node that crowd
// stopped being a storage bill and became an outage — 190 of 237 shards could not finish booting their sequencer,
// every order on them was refused with "the L3 sequencer is not ready", and the board read "no book yet".
//
// A shard's log protects one thing: the miners' ability to agree on a batch and, if challenged, to prove it. Once
// the market has resolved and the quorum has finalised every index there is no batch left to agree and no challenge
// left to serve. These tests pin every clause of that proof, because the default must be to refuse.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { generatePrivateKey } from 'viem/accounts';
import { createFileLog, ordersTopic, votesTopic } from './log.js';
import { createSequencerRig } from './sequencer.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'l3retire-rig-'));

/// a rig over a file log, with topics planted for the shards named
async function rigWith({ dir, shards = [], resolved = new Set(), retire = '1' }) {
  const logDir = path.join(dir, 'log');
  const log = createFileLog({ dir: logDir, pollMs: 5 });
  for (const s of shards) { await log.ensureTopic(ordersTopic(s)); await log.ensureTopic(votesTopic(s)); }
  const said = [];
  const rig = createSequencerRig({
    env: { OPERATOR_KEY: generatePrivateKey(), L3_LOG: 'file', L3_DIR: logDir, L3_RETIRE: retire, L3_RETIRE_MS: '600000', CHAIN_ID: '46630' },
    dir: logDir, logger: (m) => said.push(String(m)),
    retirableOf: (id) => resolved.has(Number(id)),
  });
  return { rig, log, logDir, said };
}
const files = (d) => fs.readdirSync(d).filter((f) => f.endsWith('.jsonl')).sort();

test('a resolved market the rig never loaded has its topics dropped', async () => {
  const dir = tmp();
  const { rig, log, logDir } = await rigWith({ dir, shards: ['100', '200'], resolved: new Set([100]) });
  try {
    // the rig has to have a log open before it can sweep: the file log is opened on demand
    rig.log; // eslint-disable-line no-unused-expressions
    const before = files(logDir);
    assert.deepEqual(before, ['orders.100.jsonl', 'orders.200.jsonl', 'votes.100.jsonl', 'votes.200.jsonl']);
    await rig.retire();
    const after = files(logDir);
    assert.equal(after.includes('orders.100.jsonl'), false, 'the resolved market went');
    assert.equal(after.includes('votes.100.jsonl'), false, 'both of its topics');
    assert.ok(after.includes('orders.200.jsonl'), 'the open market is untouched');
    assert.ok(after.includes('votes.200.jsonl'));
    assert.equal(rig.status().retirement.retired, 1);
    assert.equal(rig.status().retirement.dropped, 2);
  } finally { await rig.stop(); await log.close?.(); }
});

test('an unknown market id is refused, because "not read" is not "finished"', async () => {
  const dir = tmp();
  // nothing is resolved as far as the engine is concerned
  const { rig, log, logDir } = await rigWith({ dir, shards: ['300'], resolved: new Set() });
  try {
    await rig.retire();
    assert.deepEqual(files(logDir), ['orders.300.jsonl', 'votes.300.jsonl'], 'kept');
    const r = rig.status().retirement;
    assert.equal(r.retired, 0);
    assert.ok(r.refused > 0);
    assert.ok(r.byWhy['the chain says it is not finished'] > 0, JSON.stringify(r.byWhy));
  } finally { await rig.stop(); await log.close?.(); }
});

test('"not known yet" is its own answer, and a sweep that learned nothing comes back soon', async () => {
  // The engine's cache takes minutes to load 21,000 markets and the first sweep runs a minute after boot. Reading
  // "I have not read that id" as "it is not finished" made the first live sweep refuse all 572 candidates and then
  // sleep for an hour, so the backlog only moved by luck of timing.
  const dir = tmp();
  const logDir = path.join(dir, 'log');
  const log = createFileLog({ dir: logDir, pollMs: 5 });
  await log.ensureTopic(ordersTopic('310')); await log.ensureTopic(votesTopic('310'));
  let loaded = false;
  const rig = createSequencerRig({
    env: { OPERATOR_KEY: generatePrivateKey(), L3_LOG: 'file', L3_DIR: logDir, L3_RETIRE_RETRY_MS: '60', CHAIN_ID: '46630' },
    dir: logDir, logger: () => {},
    retirableOf: () => (loaded ? true : null),      // null = the cache has not loaded
  });
  try {
    await rig.retire();
    const r1 = rig.status().retirement;
    assert.equal(r1.retired, 0, 'nothing judged, nothing dropped');
    assert.ok(r1.byWhy['not known yet (the market cache is still loading)'] > 0, JSON.stringify(r1.byWhy));
    assert.equal(r1.lastUnknown, 1);
    assert.deepEqual(files(logDir), ['orders.310.jsonl', 'votes.310.jsonl'], 'and the log is kept');
    // the cache loads, and the scheduled retry does the work without waiting the full interval
    loaded = true;
    await new Promise((r) => setTimeout(r, 250));
    assert.deepEqual(files(logDir), [], 'the retry retired it');
  } finally { await rig.stop(); }
});

test('retirement off leaves everything alone', async () => {
  const dir = tmp();
  const { rig, log, logDir } = await rigWith({ dir, shards: ['400'], resolved: new Set([400]), retire: '0' });
  try {
    await rig.retire();
    assert.deepEqual(files(logDir), ['orders.400.jsonl', 'votes.400.jsonl']);
    assert.equal(rig.status().retirement.enabled, false);
  } finally { await rig.stop(); await log.close?.(); }
});

test('without a retirableOf the rig can never drop anything', async () => {
  const dir = tmp();
  const logDir = path.join(dir, 'log');
  const log = createFileLog({ dir: logDir, pollMs: 5 });
  await log.ensureTopic(ordersTopic('500')); await log.ensureTopic(votesTopic('500'));
  const rig = createSequencerRig({ env: { OPERATOR_KEY: generatePrivateKey(), L3_LOG: 'file', L3_DIR: logDir, CHAIN_ID: '46630' }, dir: logDir, logger: () => {} });
  try {
    await rig.retire();
    assert.deepEqual(files(logDir), ['orders.500.jsonl', 'votes.500.jsonl'], 'no proof available, nothing dropped');
    assert.equal(rig.status().retirement.enabled, false);
  } finally { await rig.stop(); }
});

test('a run is bounded, so one sweep cannot stall the engine on thousands of topics', async () => {
  const dir = tmp();
  const many = Array.from({ length: 12 }, (_, i) => String(600 + i));
  const logDir = path.join(dir, 'log');
  const log = createFileLog({ dir: logDir, pollMs: 5 });
  for (const s of many) { await log.ensureTopic(ordersTopic(s)); await log.ensureTopic(votesTopic(s)); }
  const rig = createSequencerRig({
    env: { OPERATOR_KEY: generatePrivateKey(), L3_LOG: 'file', L3_DIR: logDir, L3_RETIRE_PER_RUN: '5', CHAIN_ID: '46630' },
    dir: logDir, logger: () => {}, retirableOf: () => true,
  });
  try {
    await rig.retire();
    assert.equal(rig.status().retirement.lastRetired, 5, 'stopped at the per-run cap');
    assert.equal(files(logDir).length, (12 - 5) * 2, 'the rest wait for the next sweep');
    await rig.retire();
    assert.equal(files(logDir).length, (12 - 10) * 2);
  } finally { await rig.stop(); }
});

test('a LIVE shard is refused on every local clause, even when its market has resolved', async () => {
  // the clauses that protect a shard the rig is actually running: ops not yet sequenced, a halted quorum, forks
  // outstanding, a tail the quorum has not reached. Each one alone must stop the drop.
  const dir = tmp();
  const logDir = path.join(dir, 'log');
  const log = createFileLog({ dir: logDir, pollMs: 5 });
  await log.ensureTopic(ordersTopic('900')); await log.ensureTopic(votesTopic('900'));
  const rig = createSequencerRig({
    env: { OPERATOR_KEY: generatePrivateKey(), L3_LOG: 'file', L3_DIR: logDir, CHAIN_ID: '46630' },
    dir: logDir, logger: () => {}, retirableOf: () => true,
  });
  try {
    await rig.retire();   // opens the log and drops it: nothing is loaded, the market is resolved
    assert.deepEqual(files(logDir), [], 'with no local shard the chain alone decides');

    // now plant a loaded shard whose state forbids it, one clause at a time
    const mk = (over) => ({ booting: false, deferred: [], sub: null, state: { close() {} }, closedEpoch: 0, reconcile: null,
                            seq: { index: 10, stop: async () => {}, status: () => ({ shard: '901', index: 10, ready: true }) },
                            quorum: { status: () => ({ halted: false, forks: 0, finalIndex: 9, pending: [] }) }, ...over });
    const cases = [
      ['booting', mk({ booting: true })],
      ['ops not yet sequenced', mk({ deferred: [{}] })],
      ['quorum halted', mk({ quorum: { status: () => ({ halted: true, forks: 0, finalIndex: 9, pending: [] }) } })],
      ['forks outstanding', mk({ quorum: { status: () => ({ halted: false, forks: 3, finalIndex: 9, pending: [] }) } })],
      ['tail not finalised', mk({ quorum: { status: () => ({ halted: false, forks: 0, finalIndex: 4, pending: [] }) } })],
      ['batches pending a vote', mk({ quorum: { status: () => ({ halted: false, forks: 0, finalIndex: 9, pending: [{ index: 10 }] }) } })],
    ];
    for (const [why, shard] of cases) {
      await log.ensureTopic(ordersTopic('901')); await log.ensureTopic(votesTopic('901'));
      rig.shardsForTest().set('901', shard);
      await rig.retire();
      assert.deepEqual(files(logDir).sort(), ['orders.901.jsonl', 'votes.901.jsonl'], `${why}: kept`);
      assert.ok(rig.status().retirement.byWhy[why] > 0, `${why} was the reason: ${JSON.stringify(rig.status().retirement.byWhy)}`);
      rig.shardsForTest().delete('901');
      // clean up THROUGH the log, not behind it: the file log caches an open descriptor per topic, so unlinking the
      // file leaves that cache believing the topic exists and the next ensureTopic becomes a no-op
      await log.drop(ordersTopic('901')); await log.drop(votesTopic('901'));
    }
    // and a shard whose every clause holds IS retired
    await log.ensureTopic(ordersTopic('902')); await log.ensureTopic(votesTopic('902'));
    rig.shardsForTest().set('902', mk({}));
    await rig.retire();
    assert.deepEqual(files(logDir), [], 'finalised to its head, nothing deferred, no forks → dropped');
  } finally { await rig.stop(); }
});
