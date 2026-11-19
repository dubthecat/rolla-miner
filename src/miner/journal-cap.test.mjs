// node --test engine/l3/miner/journal-cap.test.mjs — a shard's journal is bounded, and it is closed on a BATCH
// BOUNDARY so it always ends on a complete batch/vote pair.
//
// The journal exists so a restart replays locally instead of re-reading the log, and nothing bounded it. One hot
// shard's pair reached 78 MB while all three of the fleet's 3 GB volumes sat at 100 %: after that every append
// failed with ENOSPC and the miners came back replaying truncated journals — 2,811 forks across 8 halted shards.
//
// The cap CLOSES the journal rather than rotating it, because replayJournal() refuses a journal that does not start
// at batch 0: rotating would throw away the whole thing, while a closed journal still rebuilds the book up to its
// last batch and `from = offset + 1` has the log supply the rest, which is the ordinary restart path.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createFileLog, ordersTopic } from './log.js';
import { BOOK_DOMAIN } from '../desk.js';
import { createMiner } from './miner.js';
import { ZERO32 } from './merkle.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'l3cap-'));
const SHARD = '777';
const lines = (f) => { try { return fs.readFileSync(f, 'utf8').split('\n').filter(Boolean).length; } catch { return 0; } };

/// a miner on its own file log, with the cap we want to test; returns a feed() that appends one batch and waits
async function rig(dir, env = {}) {
  const jdir = path.join(dir, 'journal');
  const log = createFileLog({ dir: path.join(dir, 'log'), pollMs: 2 });
  const said = [];
  const m = createMiner({
    shard: SHARD, log, account: privateKeyToAccount(generatePrivateKey()),
    domain: BOOK_DOMAIN(46630, '0x' + '11'.repeat(20)), dir: jdir, workers: 0,
    sequencers: null, threshold: 1, watchVotes: false, env: { CHAIN_ID: '46630', ...env },
    logger: (s) => said.push(String(s)),
  });
  await m.start();
  // the roots are fabricated, so every batch is a DISSENT — which still travels the journal's write path, which is
  // what this test is about
  const feed = async (index) => {
    await log.append(ordersTopic(SHARD), {
      shard: SHARD, epoch: 0, index, seqFrom: 0, seqTo: 0, prevRoot: ZERO32,
      ordersRoot: '0x' + '33'.repeat(32), fillsRoot: '0x' + '44'.repeat(32), bookHash: ZERO32,
      batchRoot: '0x' + '22'.repeat(32), ops: [], sig: '0x',
    });
    for (let i = 0; i < 400 && m.status().index < index; i++) await new Promise((r) => setTimeout(r, 5));
  };
  return { m, said, jdir, feed, batches: path.join(jdir, `${SHARD}.batches.jsonl`), votes: path.join(jdir, `${SHARD}.votes.jsonl`) };
}

test('under the cap both halves are written for every batch', async () => {
  const { m, feed, batches, votes } = await rig(tmp());
  try {
    await feed(0); await feed(1); await feed(2);
    assert.equal(m.status().index, 2, 'all three batches applied');
    assert.equal(lines(batches), 3);
    assert.equal(lines(votes), 3);
    assert.equal(m.status().journalCapped, false);
    assert.ok(m.status().journalBytes > 0, 'the size is reported');
  } finally { await m.stop(); }
});

test('at the cap the journal is closed, on a batch boundary, and the log keeps feeding the miner', async () => {
  // a cap of one byte: the first pair is written (nothing was there yet), everything after it is refused
  const { m, said, feed, batches, votes } = await rig(tmp(), { L3_JOURNAL_MAX_BYTES: '1' });
  try {
    await feed(0); await feed(1); await feed(2);
    assert.equal(m.status().index, 2, 'the miner kept applying batches after its journal closed');
    assert.equal(lines(batches), 1, 'one batch line');
    assert.equal(lines(votes), 1, 'and its vote: the journal ends on a COMPLETE pair');
    assert.equal(m.status().journalCapped, true);
    const why = said.find((l) => /journal is /.test(l));
    assert.ok(why, `the close is announced: ${said.join(' | ')}`);
    assert.match(why, /closing it before batch 1/, 'it names the batch it refused, not the one it holds');
    assert.match(why, /reads the rest from the log/);
  } finally { await m.stop(); }
});

test('a cap of 0 means unbounded', async () => {
  const { m, feed, batches } = await rig(tmp(), { L3_JOURNAL_MAX_BYTES: '0' });
  try {
    await feed(0); await feed(1);
    assert.equal(lines(batches), 2);
    assert.equal(m.status().journalCapped, false);
  } finally { await m.stop(); }
});

test('a discarded journal takes its votes half with it, and only one forensic copy per half is kept', async () => {
  // Both halves move together: the votes file is only a fallback for batch lines written before 2026-10-07, so
  // leaving it behind meant a shard that discarded its journal kept appending to a file nothing would read again
  // — 349 MB across 45 orphaned copies on one machine, two of them for each hot shard.
  const dir = tmp(); const jdir = path.join(dir, 'journal'); fs.mkdirSync(jdir, { recursive: true });
  const headless = { offset: 9, index: 9, epoch: 0, root: '0x' + '22'.repeat(32),
    batch: { shard: SHARD, epoch: 0, index: 9, seqFrom: 0, seqTo: 0, prevRoot: ZERO32, ordersRoot: '0x' + '33'.repeat(32), fillsRoot: '0x' + '44'.repeat(32), bookHash: ZERO32, batchRoot: '0x' + '22'.repeat(32), ops: [] } };
  fs.writeFileSync(path.join(jdir, `${SHARD}.batches.jsonl`), JSON.stringify(headless) + '\n');
  fs.writeFileSync(path.join(jdir, `${SHARD}.votes.jsonl`), JSON.stringify({ index: 9, batchRoot: '0x' + '22'.repeat(32) }) + '\n');
  fs.writeFileSync(path.join(jdir, `${SHARD}.batches.jsonl.corrupt.1`), 'stale\n');
  fs.writeFileSync(path.join(jdir, `${SHARD}.votes.jsonl.corrupt.1`), 'stale\n');
  const { m, said } = await rig(dir);
  try {
    assert.ok(said.some((l) => /head was lost/.test(l)), `refused: ${said.join(' | ')}`);
    const aside = fs.readdirSync(jdir).filter((f) => /\.corrupt\./.test(f));
    assert.equal(aside.length, 2, `one copy of each half, not four: ${aside.join(', ')}`);
    assert.equal(aside.filter((f) => /\.corrupt\.1$/.test(f)).length, 0, 'the stale copies are gone');
    assert.equal(fs.existsSync(path.join(jdir, `${SHARD}.votes.jsonl`)), false, 'the votes half moved too');
    assert.equal(m.status().journalBytes, 0, 'the byte count restarts with the journal');
  } finally { await m.stop(); }
});
