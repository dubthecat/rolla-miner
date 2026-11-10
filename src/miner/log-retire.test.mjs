// A shard's log is deleted once its market has resolved and its quorum has finalised every index, which is the only
// way the broker stops growing: it keeps one orders topic and one votes topic PER MARKET and never reclaimed either,
// 1,970 topics for 985 markets by 2026-11-10, 23 GB of a 40 GB volume with a 4.3 MB floor each.
//
// Deleting them by hand never held, and these tests pin why. Every reader — subscribe, offset, read — used to create
// the topic it was asked about, so the next miner sweep brought a retired shard's log straight back, empty and still
// holding the floor. A reader is now pure; only the shard's owner creates.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { createFileLog, ordersTopic, votesTopic } from './log.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'l3retire-'));
const files = (dir) => fs.readdirSync(dir).filter((f) => f.endsWith('.jsonl')).sort();

test('a reader never brings a topic into being', async () => {
  const dir = tmp(); const log = createFileLog({ dir, pollMs: 2 });
  const t = ordersTopic('19337');
  // nothing exists yet, and none of the three read paths may change that
  assert.equal(log.exists(t), false);
  assert.equal(await log.offset(t), 0);
  assert.deepEqual(await log.read(t, 0, 10), []);
  // a subscribe WAITS rather than creating or dying: the engine's quorum subscribes to a shard's votes topic before
  // any miner has voted, and a dead handle there would starve finality for good
  const seen = [];
  const sub = await log.subscribe(t, 0, async ({ value }) => { seen.push(value); });
  assert.equal(sub.waiting, true, 'it reports that it is waiting');
  assert.equal(sub.topic, t);
  assert.deepEqual(files(dir), [], 'not one file was created by reading');
  assert.equal(log.exists(t), false);

  // and it attaches by itself the moment the owner creates the topic and writes to it
  await log.ensureTopic(t);
  await log.append(t, { index: 0, shard: '19337' });
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(seen.map((v) => v.index), [0], 'the waiting reader picked it up');
  sub.close();
});

test('the owner creates, and then the reader works normally', async () => {
  const dir = tmp(); const log = createFileLog({ dir, pollMs: 2 });
  const t = ordersTopic('19337');
  await log.ensureTopic(t);
  assert.equal(log.exists(t), true);
  assert.deepEqual(files(dir), ['orders.19337.jsonl'], 'the owner made exactly one');
  // and a reader on it is an ordinary subscription, not a `missing` stub
  const seen = [];
  const sub = await log.subscribe(t, 0, async ({ value }) => { seen.push(value); });
  assert.equal(sub.missing, undefined);
  await log.append(t, { index: 0, shard: '19337' });
  await new Promise((r) => setTimeout(r, 40));
  assert.equal(seen.length, 1);
  sub.close();
});

test('a write still creates its own topic — the sequencer must not need a ceremony to start a shard', async () => {
  const dir = tmp(); const log = createFileLog({ dir, pollMs: 2 });
  const t = votesTopic('19338');
  await log.append(t, { index: 0, shard: '19338' });
  assert.equal(log.exists(t), true);
  assert.equal(await log.offset(t), 1);
  assert.deepEqual(await log.read(t, 0, 10).then((r) => r.map((x) => x.value.index)), [0]);
});

test('a retired shard stays retired: reading it after deletion does not resurrect it', async () => {
  const dir = tmp(); const log = createFileLog({ dir, pollMs: 2 });
  const o = ordersTopic('19339'), v = votesTopic('19339');
  await log.append(o, { index: 0, shard: '19339' });
  await log.append(v, { index: 0, miner: '0xabc' });
  assert.deepEqual(files(dir), ['orders.19339.jsonl', 'votes.19339.jsonl']);
  // the market resolved and its quorum finalised everything, so the owner drops both
  for (const f of files(dir)) fs.unlinkSync(path.join(dir, f));
  // a fresh process reads them — this is the miner sweep that used to undo the deletion
  const log2 = createFileLog({ dir, pollMs: 2 });
  assert.equal(log2.exists(o), false);
  const so = await log2.subscribe(o, 0, async () => {});
  const sv = await log2.subscribe(v, 0, async () => {});
  assert.equal(so.waiting, true); assert.equal(sv.waiting, true);
  assert.equal(await log2.offset(o), 0);
  // the whole point: several polls later the topics are still gone. A reader that created them — which is what
  // subscribe used to do, and what kafkajs's consumer did too — undid the deletion on the next miner sweep.
  await new Promise((r) => setTimeout(r, 60));
  assert.deepEqual(files(dir), [], 'still gone after the readers have polled');
  assert.equal(log2.exists(o), false);
  so.close(); sv.close();
});
