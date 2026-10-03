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
  // two distinct conditions fire on one dissent: "a miner disagrees with the sequencer" and, because the vote
  // set now holds two roots, "miners disagree with each other"
  assert.deepEqual(q.forks().map((f) => f.why), ['a miner disagrees with the sequencer', 'miners disagree with each other']);
  assert.equal(q.status().rewards.accrued[A], '225');             // 120 + 105
  const judged = q.judge(1, { dissenterWasRight: true });
  assert.equal(judged.paid[0].miner, C);
  assert.equal(judged.paid[0].amount, 1050n);                     // 10 × (100 + 5)
  assert.equal(rewards.accruedOf(C), 1050n);
  assert.equal(rewards.accruedOf(A), 169n);                       // 225 − 225/4 (a quarter beats the 50 floor)
  assert.equal(q.judge(1, { dissenterWasRight: true }), null);
  assert.ok(q.status().rewards.epochs.length === 0, 'the quorum closed an epoch by itself');
});
