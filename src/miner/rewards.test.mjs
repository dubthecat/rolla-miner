// node --test engine/l3/miner/rewards.test.mjs — the µROLLA accounting of docs/L3-MINERS.md §6a: what a
// finalized batch credits, what a proven dissent earns, what a losing vote costs, and the epoch root a miner
// claims against. Everything here is integer and pure: two processes fed the same finalizations must produce the
// same rewardsRoot, or a miner's balance is not provable.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { encodeAbiParameters, keccak256, concat } from 'viem';
import { createRewards, rewardsFromEnv, claimLeaf, MICRO_PER_ROLLA } from './rewards.js';
import { verifyProof, merkleRoot, ZERO32 } from './merkle.js';
import { createQuorum } from './quorum.js';
import { batchRootOf, fillLeaf } from './merkle.js';

const A = '0x' + 'a1'.repeat(20), B = '0x' + 'b2'.repeat(20), C = '0x' + 'c3'.repeat(20);

test('a claim leaf is byte-identical to keccak256(0x00 ‖ abi.encode(uint64,address,uint256))', () => {
  const viem = keccak256(concat(['0x00', encodeAbiParameters(
    [{ type: 'uint64' }, { type: 'address' }, { type: 'uint256' }], [7n, A, 12345n])]));
  assert.equal(claimLeaf({ epoch: 7, miner: A, amount: 12345n }), viem);
  assert.equal(claimLeaf({ epoch: 7, miner: A.toUpperCase().replace('0X', '0x'), amount: 12345n }), viem);
  assert.equal(MICRO_PER_ROLLA, 1000000n);
});

test('a finalized batch credits every agreeing miner: base + perFill × fills', () => {
  const r = createRewards({ base: 100n, perFill: 1n });
  const out = r.creditBatch({ epoch: 0, index: 0, fills: 50, miners: [A, B] });
  assert.deepEqual(out.map((x) => x.amount), [150n, 150n]);
  assert.equal(r.accruedOf(A), 150n);
  r.creditBatch({ epoch: 0, index: 1, fills: 0, miners: [A] });
  assert.equal(r.accruedOf(A), 250n, 'a batch with no fills still pays the base');
  assert.equal(r.accruedOf(C), 0n, 'a miner that did not vote was paid');
  const st = r.state();
  assert.equal(st.lifetime[A.toLowerCase()].batches, 2);
  assert.equal(st.lifetime[A.toLowerCase()].fills, 50);
  assert.equal(st.openTotal, '400');
});

test('weights scale the credit, and uptime is clamped', () => {
  const w = { [A.toLowerCase()]: { stake: 3, uptime: 1 }, [B.toLowerCase()]: { stake: 1, uptime: 0.5 }, [C.toLowerCase()]: { stake: 1, uptime: 9 } };
  const r = createRewards({ base: 100n, perFill: 0n, weightOf: (m) => w[m] });
  const out = r.creditBatch({ fills: 0, miners: [A, B, C] });
  assert.deepEqual(out.map((x) => x.amount), [300n, 50n, 100n]);   // 3×, 0.5×, uptime clamped to 1
});

test('a dissent proven right earns the bounty; a losing vote is slashed and the shortfall becomes debt', () => {
  const r = createRewards({ base: 100n, perFill: 1n, bountyMultiple: 10n, slashNum: 1n, slashDen: 4n, minSlash: 1000n });
  r.creditBatch({ fills: 100, miners: [A, B] });                   // 200 each
  const bounty = r.bounty({ epoch: 0, index: 3, fills: 100, miner: C });
  assert.equal(bounty.amount, 2000n);                              // 10 × (100 + 100)
  assert.equal(r.accruedOf(C), 2000n);
  const cut = r.slash({ miner: A, fills: 100 });
  assert.equal(cut.amount, 1000n, 'the floor did not apply when a quarter of 200 is less than it');
  assert.equal(cut.taken, 200n);
  assert.equal(cut.carried, 800n);
  assert.equal(r.accruedOf(A), 0n);
  assert.equal(r.debtOf(A), 800n);
  // a miner in debt earns nothing until it is paid off
  r.creditBatch({ fills: 100, miners: [A] });                      // 200 → all of it to the debt
  assert.equal(r.debtOf(A), 600n);
  assert.equal(r.accruedOf(A), 0n);
  for (let i = 0; i < 4; i++) r.creditBatch({ fills: 100, miners: [A] });
  assert.equal(r.debtOf(A), 0n);
  assert.equal(r.accruedOf(A), 200n, 'the credit that overshot the debt was lost');
  // a quarter of a big balance beats the floor
  const r2 = createRewards({ base: 100n, perFill: 1n, minSlash: 10n });
  for (let i = 0; i < 10; i++) r2.creditBatch({ fills: 100, miners: [A] });   // 2000
  assert.equal(r2.slash({ miner: A }).amount, 500n);
});

test('judging a fork: the sequencer was bad, so the dissenters are paid and the agreers cut', () => {
  const r = createRewards({ base: 100n, perFill: 1n, bountyMultiple: 10n, minSlash: 50n });
  r.creditBatch({ fills: 10, miners: [A, B, C] });                 // 110 each
  const j = r.judge({ epoch: 1, index: 9, fills: 10, agreers: [A, B], dissenters: [C], dissenterWasRight: true });
  assert.equal(j.paid.length, 1); assert.equal(j.paid[0].amount, 1100n);
  assert.equal(j.cut.length, 2);
  assert.equal(r.accruedOf(C), 1210n);
  assert.equal(r.accruedOf(A), 60n);                               // 110 − 50 (the floor)
  assert.equal(r.judge({ index: 9, dissenters: [C] }), null, 'a fork was judged twice');
  // and the other way round
  const r2 = createRewards({ base: 100n, perFill: 0n, minSlash: 40n });
  r2.creditBatch({ fills: 0, miners: [A, B, C] });
  const j2 = r2.judge({ index: 4, agreers: [A, B], dissenters: [C], dissenterWasRight: false });
  assert.equal(j2.paid.length, 0);
  assert.equal(r2.accruedOf(C), 60n);
  assert.equal(r2.accruedOf(A), 100n, 'an agreer was slashed although the dissenter was wrong');
});

test('an epoch closes into one canonical root, with a proof per miner', () => {
  const r = createRewards({ base: 100n, perFill: 1n });
  r.creditBatch({ epoch: 2, fills: 10, miners: [C, A, B] });       // credited in a scrambled order
  const rec = r.closeEpoch(2);
  assert.equal(rec.miners, 3);
  assert.equal(rec.total, 330n);
  assert.deepEqual(rec.rows.map((x) => x.miner), [A, B, C].map((x) => x.toLowerCase()));   // sorted by address
  assert.equal(rec.rewardsRoot, merkleRoot([A, B, C].map((m) => claimLeaf({ epoch: 2, miner: m, amount: 110n }))));
  assert.equal(r.epochRootOf(2), rec.rewardsRoot);
  assert.equal(r.accruedOf(A), 0n, 'closing did not clear the open epoch');
  assert.equal(r.closeEpoch(2).rewardsRoot, rec.rewardsRoot, 'closing twice changed the root');

  for (const m of [A, B, C]) {
    const p = r.proofFor(2, m);
    assert.equal(p.amount, 110n);
    assert.ok(verifyProof(p, rec.rewardsRoot), `${m} cannot prove its claim`);
    assert.ok(r.verifyClaim(p));
    assert.ok(!r.verifyClaim({ ...p, amount: 1110n }), 'a claim for more than the root says verified');
    assert.ok(!r.verifyClaim({ ...p, epoch: 3 }), 'a claim against another epoch verified');
  }
  assert.equal(r.proofFor(2, '0x' + 'dd'.repeat(20)), null);
  assert.equal(r.proofFor(99, A), null);
  assert.equal(r.epochRootOf(99), ZERO32);
});

test('two independent accountants fed the same finalizations agree on the root', () => {
  const run = () => {
    const r = createRewards({ base: 100n, perFill: 1n });
    for (let i = 0; i < 20; i++) r.creditBatch({ epoch: 0, index: i, fills: (i * 7) % 13, miners: i % 3 ? [A, B, C] : [B, A] });
    return r.closeEpoch(0);
  };
  const a = run(), b = run();
  assert.equal(a.rewardsRoot, b.rewardsRoot);
  assert.equal(a.total, b.total);
  const scrambled = createRewards({ base: 100n, perFill: 1n });
  for (let i = 19; i >= 0; i--) scrambled.creditBatch({ epoch: 0, index: i, fills: (i * 7) % 13, miners: i % 3 ? [C, B, A] : [A, B] });
  assert.equal(scrambled.closeEpoch(0).rewardsRoot, a.rewardsRoot, 'the root depends on the order of arrival');
});

test('the env knobs are read', () => {
  const r = rewardsFromEnv({ L3_REWARD_BASE: '7', L3_REWARD_PER_FILL: '2', L3_REWARD_BOUNTY: '3', L3_SLASH_NUM: '1', L3_SLASH_DEN: '2' });
  assert.equal(r.creditBatch({ fills: 4, miners: [A] })[0].amount, 15n);   // 7 + 2×4
  assert.equal(r.bounty({ fills: 4, miner: B }).amount, 45n);
});

// ---------------------------------------------------------------- through the quorum, where it actually happens
const fake = (index, prevRoot, salt = 0) => {
  const b = { shard: 'r', epoch: Math.floor(index / 4), index, seqFrom: index, seqTo: index, prevRoot, ordersRoot: fillLeaf({ seq: index, makerHash: ZERO32, takerHash: ZERO32, maker: A, taker: B, price: 1n, size: 1n, takerBuys: true }), fillsRoot: fillLeaf({ seq: 1000 + index + salt, makerHash: ZERO32, takerHash: ZERO32, maker: A, taker: B, price: 1n, size: 1n, takerBuys: true }), bookHash: ZERO32 };
  b.batchRoot = batchRootOf(b);
  return b;
};
const vote = (miner, b, fills, root = null) => ({ shard: b.shard, epoch: b.epoch, index: b.index, claimed: b.batchRoot, batchRoot: root || b.batchRoot, ordersRoot: b.ordersRoot, fillsRoot: b.fillsRoot, bookHash: b.bookHash, fills, ok: !root, miner, sig: '0x' });

test('the quorum pays only the agreeing set, and judging a fork pays the dissenter', async () => {
  const rewards = createRewards({ base: 100n, perFill: 1n, bountyMultiple: 10n, minSlash: 50n });
  const finals = [];
  const q = createQuorum({ threshold: 2, rewards, haltAfter: 99, verify: async (v) => v.miner, onFinal: (f) => finals.push(f) });
  const b0 = fake(0, ZERO32);
  q.announce(b0);
  await q.vote(vote(A, b0, 20));
  await q.vote(vote(B, b0, 20));
  assert.equal(finals.length, 1);
  assert.equal(finals[0].fills, 20, 'the fill count came from the votes, not the batch');
  assert.deepEqual(finals[0].credit.map((c) => [c.miner, c.amount]), [[A, 120n], [B, 120n]]);
  assert.equal(rewards.accruedOf(A), 120n);
  assert.equal(rewards.accruedOf(C), 0n);
  // C dissents on the next batch and is later proven right
  const b1 = fake(1, b0.batchRoot);
  q.announce(b1);
  await q.vote(vote(A, b1, 5)); await q.vote(vote(B, b1, 5));
  await q.vote(vote(C, b1, 5, fake(1, b0.batchRoot, 9).batchRoot));
  // No fork: A and B already met the threshold, so C's dissent is absorbed — one miner disagreeing is what a quorum
  // exists to absorb, and the two conditions that DO halt (the dissenters contradicting each other, or too few
  // miners left to reach the threshold) are neither of them here. The arbiter path must not depend on a fork having
  // been raised, which is what `judge` below proves: the evidence is the votes themselves.
  assert.deepEqual(q.forks(), []);
  assert.deepEqual(q.dissents().map((f) => [f.index, f.miner]), [[1, C]]);
  assert.equal(q.status().rewards.accrued[A], '225');             // 120 + 105
  const judged = q.judge(1, { dissenterWasRight: true });
  assert.equal(judged.paid[0].miner, C);
  assert.equal(judged.paid[0].amount, 1050n);                     // 10 × (100 + 5)
  assert.equal(rewards.accruedOf(C), 1050n);
  assert.equal(rewards.accruedOf(A), 169n);                       // 225 − 225/4 (a quarter beats the 50 floor)
  assert.equal(q.judge(1, { dissenterWasRight: true }), null);
  assert.ok(q.status().rewards.epochs.length === 0, 'the quorum closed an epoch by itself');
});

test('a fork resolves when its batch later reaches the threshold; equivocation never does', async () => {
  // This is what a restart needs. A miner whose journal is gone re-reads the orders log from the beginning and its
  // quorum re-reads the votes topic from offset 0, so an outage's historical dissents all arrive again — in
  // arbitrary order. The dissent for index i can land BEFORE the two agreements that make i final, so the fork is
  // raised, and being sticky it halted the shard for good: on 2026-11-19 four of miner-1's six shards sat halted at
  // `finalIndex -1` while 7,709 of their 8,116 votes agreed and no miner was currently disagreeing.
  const q = createQuorum({ threshold: 2, verify: async (v) => v.miner, miners: [A, B, C] });
  const b0 = fake(0, ZERO32);
  q.announce(b0);
  // two dissenters, holding DIFFERENT wrong roots, arrive first: that is a real fork and it halts
  await q.vote(vote(B, b0, 1, fake(0, ZERO32, 8).batchRoot));
  await q.vote(vote(C, b0, 1, fake(0, ZERO32, 9).batchRoot));
  assert.equal(q.halted, true);
  assert.equal(q.forks().length >= 1, true);
  // then the agreements that settle it. With B and C dissenting there is only one miner left, so finality still
  // cannot be reached here — the fork stands, which is correct.
  await q.vote(vote(A, b0, 1));
  assert.equal(q.halted, true, 'one agreeing miner of three is not a quorum');

  // the next batch: the dissents land first, the agreements after — the replay order that caused the outage. Four
  // miners here, so two can still reach a threshold of two after two have dissented.
  const D = '0xd';
  const q2 = createQuorum({ threshold: 2, verify: async (v) => v.miner, miners: [A, B, C, D] });
  const b1 = fake(0, ZERO32);
  q2.announce(b1);
  await q2.vote(vote(C, b1, 1, fake(0, ZERO32, 9).batchRoot));
  await q2.vote(vote(B, b1, 1, fake(0, ZERO32, 8).batchRoot));
  assert.equal(q2.halted, true, 'two miners holding two different roots is a fork');
  await q2.vote(vote(A, b1, 1));
  assert.equal(q2.halted, true, 'still only one agreeing');
  // the fourth miner agrees: now the sequencer's root has the threshold and the dispute is settled
  await q2.vote(vote(D, b1, 1));
  assert.equal(q2.finalIndex, 0, 'the batch is final on the two that agree');
  assert.equal(q2.halted, false, 'and the fork it raised is resolved');
  assert.deepEqual(q2.forks(), []);
  assert.equal(q2.resolvedForks().length >= 1, true, 'kept for the record');

  // equivocation is never resolved this way: that miner has provably misbehaved whatever the set decides
  const q3 = createQuorum({ threshold: 2, verify: async (v) => v.miner, miners: [A, B, C] });
  const b2 = fake(0, ZERO32);
  q3.announce(b2);
  await q3.vote(vote(C, b2, 1));
  await q3.vote(vote(C, b2, 1, fake(0, ZERO32, 7).batchRoot));   // the same miner, a second root
  assert.equal(q3.halted, true);
  await q3.vote(vote(A, b2, 1)); await q3.vote(vote(B, b2, 1));
  assert.equal(q3.at(0).final, true, 'the honest two do agree on the batch');
  assert.equal(q3.halted, true, 'but the equivocation stands, and a halt holds finality back');
  assert.equal(q3.finalIndex, -1, 'nothing settles behind an unresolved dispute — that is the point of halting');
  assert.equal(q3.forks()[0].sticky, true);
  assert.deepEqual(q3.resolvedForks(), [], 'provable misbehaviour is never resolved by a later vote');
});

test('a stale vote costs an integer compare, not a signature recovery', async () => {
  // Recovering the signer is the single most expensive thing this process does — 254 µs of secp256k1 — and every
  // miner's votes topic is re-read from offset 0 at every start (the quorum keys votes by miner, so re-reading one
  // is harmless and a restart needs it). The engine sets a real finality floor on resume (`quorum.start(tail)`),
  // so almost all of that log is history; the stale test just sat BEHIND the recovery, so each historical vote was
  // fully verified and then thrown away. On the live engine that was 51 % of ALL its CPU across ~95 shards,
  // blocking the event loop in ~100 ms bursts: a 21 KB page took 1.2 s to serve while a route with no body took 20.
  let recoveries = 0;
  const q = createQuorum({ threshold: 2, verify: async (v) => { recoveries++; return v.miner; } });
  q.start(500);                                      // the log's tail after a restart: everything below is settled
  assert.equal(q.finalIndex, 499);
  const old = fake(12, ZERO32);
  assert.deepEqual(await q.vote(vote(A, old, 1)), { ok: false, why: 'stale (that batch is pruned)' });
  assert.equal(recoveries, 0, 'the history of the log is skipped without a single recovery');
  assert.equal(q.status().stale, 1);
  // a vote at the live head is verified as it always was
  const live = fake(500, ZERO32);
  q.announce(live);
  await q.vote(vote(A, live, 1));
  assert.equal(recoveries, 1);
  // and so is a late vote for a batch still held in memory, finalized or not
  await q.vote(vote(B, live, 1));
  assert.equal(recoveries, 2);
  assert.equal(q.finalIndex, 500);
  await q.vote(vote(C, live, 1));
  assert.equal(recoveries, 3, 'a batch still in `batches` is never stale, even below finalIndex');
});
