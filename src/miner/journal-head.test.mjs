// A miner's journal is replayed from its FIRST batch: replay rebuilds the book by applying every operation in order,
// so a journal that has lost its head replays from the middle onto an empty book. The resulting state is wrong in a
// way nothing downstream can see — the miner computes a different batchRoot from the sequencer for every batch after
// it, dissents on each one, and halts its own shard's finality for good.
//
// That happened. The fleet's volumes filled on 2026-11-11, every journal append failed with ENOSPC, and the miners
// came back replaying truncated journals: 2,811 forks across 8 halted shards, every one of them `minerWhy: null`,
// which is the signature of a state divergence and not of a batch anyone believes is invalid. The journal's own
// self-consistency check cannot catch it, because the root it compares against is read from the same truncated
// journal — a journal missing only its head agrees with itself perfectly.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createFileLog, ordersTopic } from './log.js';
import { BOOK_DOMAIN } from '../desk.js';
import { createMiner } from './miner.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'l3head-'));
const SHARD = '4242';

/// a miner over an empty log, with whatever journal lines we plant
async function minerOver(dir, lines) {
  const jdir = path.join(dir, 'journal'); fs.mkdirSync(jdir, { recursive: true });
  if (lines) fs.writeFileSync(path.join(jdir, `${SHARD}.batches.jsonl`), lines.map((l) => JSON.stringify(l)).join('\n') + '\n');
  const log = createFileLog({ dir: path.join(dir, 'log'), pollMs: 2 });
  const said = [];
  const m = createMiner({
    shard: SHARD, log, account: privateKeyToAccount(generatePrivateKey()),
    domain: BOOK_DOMAIN(46630, '0x' + '11'.repeat(20)), dir: jdir, workers: 0,
    sequencers: [], threshold: 1, watchVotes: false, env: { CHAIN_ID: '46630' }, logger: (s) => said.push(String(s)),
  });
  await m.start();
  return { m, said, jdir };
}

const batchLine = (index) => ({
  offset: index, index, epoch: 0, root: '0x' + '22'.repeat(32),
  batch: { shard: SHARD, epoch: 0, index, seqFrom: 0, seqTo: 0, prevRoot: '0x' + '00'.repeat(32), ordersRoot: '0x' + '33'.repeat(32), fillsRoot: '0x' + '44'.repeat(32), bookHash: '0x' + '00'.repeat(32), batchRoot: '0x' + '22'.repeat(32), ops: [] },
});

test('a journal that lost its head is refused and set aside, not replayed onto an empty book', async () => {
  const dir = tmp();
  // the shard's first batches are gone: this journal starts at 7
  const { m, said, jdir } = await minerOver(dir, [batchLine(7), batchLine(8), batchLine(9)]);
  try {
    const why = said.find((l) => /journal starts at batch 7/.test(l));
    assert.ok(why, `the refusal is named: ${said.join(' | ')}`);
    assert.match(why, /head was lost/);
    assert.match(why, /replaying 4242 from the log/);
    // it starts from nothing rather than from a wrong book, and the bad journal is kept for diagnosis
    assert.equal(m.status().index, -1, 'no batch was replayed');
    const aside = fs.readdirSync(jdir).filter((f) => /\.corrupt\./.test(f));
    assert.equal(aside.length, 1, 'set aside');
  } finally { await m.stop(); }
});

test('a journal that starts at 0 passes the head check and is judged on its roots instead', async () => {
  // these fixtures carry fabricated roots, so replay still refuses this one — but for the RIGHT reason. What matters
  // is that the head check does not fire on a journal that begins where it should, so an honest journal is never
  // thrown away by the new rule.
  const dir = tmp();
  const { m, said } = await minerOver(dir, [batchLine(0)]);
  try {
    assert.equal(said.some((l) => /head was lost/.test(l)), false, 'the head check did not fire');
    assert.ok(said.some((l) => /no longer reproduces the root/.test(l)), `judged on its roots: ${said.join(' | ')}`);
  } finally { await m.stop(); }
});

test('no journal at all is not a refusal: a fresh shard starts from the log', async () => {
  const dir = tmp();
  const { m, said } = await minerOver(dir, null);
  try {
    assert.equal(said.some((l) => /head was lost|discarding/.test(l)), false);
    assert.equal(m.status().index, -1);
  } finally { await m.stop(); }
});
