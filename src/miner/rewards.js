// engine/l3/miner/rewards.js — what a miner earns, in microRolla (µROLLA), and the Merkle root that makes the
// balance provable without trusting the operator. PURE: no I/O, no clock, no chain, bigint arithmetic only, so
// two processes fed the same finalizations compute the same rewardsRoot (docs/L3-MINERS.md §6a).
//
// Earning, per FINALIZED batch, for each miner whose vote matched the finalized fillsRoot and bookHash:
//
//     credit(m) = (base + perFill × fills) × weight(m) / 1000
//
// `base` pays for showing up and replaying (the cost of a batch is mostly its signatures, which a miner pays
// whether the batch had one fill or a hundred); `perFill` pays for the work that scales. `weight(m)` is 1000
// (= 1.0) unless the caller supplies stake and uptime, in which case it is `round(1000 × uptime × stakeFactor)`
// — the same local-incentive shape the rest of the venue uses: paid for the markets you actually serve.
//
// A dissent that is later PROVEN RIGHT earns `bountyMultiple ×` a batch's credit: finding a bad batch is worth
// far more to the venue than validating a good one, and the bounty is what makes it rational to dissent rather
// than to follow the sequencer. A vote on the LOSING side of a judged fork is slashed from the miner's
// unredeemed balance; if the open epoch's accrual does not cover it, the remainder becomes a debt carried into
// the next epochs, so a miner cannot escape a slash by having just closed an epoch.
//
// Accounting: each epoch closes into rows [{ miner, amount }] sorted by address and a
//     rewardsRoot = merkleRoot(claimLeaf(epoch, miner, amount))
// which the sequencer carries in the epoch-boundary batch and the validators attest together with the
// finalized batch root (merkle.js epochDigestOf). A miner then claims on chain with a Merkle proof against
// that root — the operator cannot pay less than the attested root says, and cannot pay a miner who is not in it.
//
// Units: µROLLA is an integer. One $ROLLA is MICRO_PER_ROLLA µROLLA at parity, but the redemption RATE is not
// parity and is not set here: it is read from on-chain fundamentals by the value oracle described in
// docs/L3-MINERS.md §6c. This module only ever counts µROLLA.
import { merkleRoot, merkleProof, verifyProof, AbiWords, tagged, TAG, hex, ZERO32 } from './merkle.js';

export const MICRO_PER_ROLLA = 1_000_000n;

/// the claim leaf: keccak256(0x00 ‖ abi.encode(uint64 epoch, address miner, uint256 amount)), abi-identical so a
/// Solidity claim() verifies the proof with abi.encode and keccak256 alone
export function claimLeaf({ epoch, miner, amount }) {
  const w = new AbiWords(3).uint(Number(epoch), 8).address(miner).uint(amount);
  return hex(tagged(TAG.LEAF, w.done()));
}

const lower = (a) => String(a || '').toLowerCase();
const big = (x) => (typeof x === 'bigint' ? x : BigInt(x || 0));

/**
 * createRewards({ ... }) — every parameter is meant to be tuned; these are starting values, in µROLLA.
 *   base             per finalized batch, per agreeing miner                     (default 100)
 *   perFill          per fill in that batch, per agreeing miner                  (default 1)
 *   bountyMultiple   a proven-right dissent earns this × a batch's credit        (default 10)
 *   slashNum/Den     a losing vote costs this fraction of the unredeemed balance (default 1/4)
 *   minSlash         the floor of a slash, so a miner with nothing accrued still owes (default 100 × base)
 *   weightOf         (miner) => { stake = 1, uptime = 1 } → weight 1000 × uptime × stake; null means 1.0
 */
export function createRewards({
  base = 100n, perFill = 1n, bountyMultiple = 10n, slashNum = 1n, slashDen = 4n, minSlash = null, weightOf = null,
} = {}) {
  const BASE = big(base), PER_FILL = big(perFill), BOUNTY = big(bountyMultiple);
  const SNUM = big(slashNum), SDEN = big(slashDen) || 1n;
  const MIN_SLASH = minSlash == null ? BASE * 100n : big(minSlash);
  const open = new Map();        // miner → µROLLA accrued in the epoch that has not closed
  const debt = new Map();        // miner → µROLLA owed from a slash the open epoch could not cover
  const life = new Map();        // miner → { credited, bounty, slashed, batches, fills, dissents }
  const epochs = new Map();      // epoch → { epoch, rows, leaves, rewardsRoot, total, miners }
  const judged = new Set();      // fork indices already judged, so a judgement is idempotent

  const stat = (m) => { let x = life.get(m); if (!x) { x = { credited: 0n, bounty: 0n, slashed: 0n, batches: 0, fills: 0, dissents: 0 }; life.set(m, x); } return x; };
  const accrued = (m) => open.get(m) || 0n;
  const owed = (m) => debt.get(m) || 0n;

  /// weight in thousandths. Integer throughout: a reward must be reproducible, and floats are not.
  function weight(miner) {
    if (!weightOf) return 1000n;
    const w = weightOf(miner) || {};
    const uptime = Number.isFinite(w.uptime) ? Math.max(0, Math.min(1, w.uptime)) : 1;
    const stake = Number.isFinite(w.stake) && w.stake > 0 ? w.stake : 1;
    return BigInt(Math.max(0, Math.round(1000 * uptime * stake)));
  }
  /// credit nets off any carried slash first: a miner in debt earns nothing until it is paid
  function add(miner, amount, kind = 'credited') {
    const m = lower(miner); const s = stat(m);
    let left = amount;
    const d = owed(m);
    if (d > 0n) { const pay = d < left ? d : left; if (pay >= d) debt.delete(m); else debt.set(m, d - pay); left -= pay; }
    if (left > 0n) open.set(m, accrued(m) + left);
    s[kind] += amount;
    return left;
  }
  function take(miner, amount) {
    const m = lower(miner); const s = stat(m);
    const have = accrued(m);
    const hit = have < amount ? have : amount;
    if (hit >= have) open.delete(m); else open.set(m, have - hit);
    if (amount > hit) debt.set(m, owed(m) + (amount - hit));
    s.slashed += amount;
    return { taken: hit, carried: amount - hit };
  }

  /// the credit of one batch, before weighting — the quantity a bounty is a multiple of
  const batchCredit = (fills) => BASE + PER_FILL * BigInt(Math.max(0, Number(fills) || 0));

  return {
    params: { base: BASE, perFill: PER_FILL, bountyMultiple: BOUNTY, slashNum: SNUM, slashDen: SDEN, minSlash: MIN_SLASH, weighted: !!weightOf },
    /// one finalized batch: credit every miner that agreed. Returns [{ miner, amount, net }].
    creditBatch({ epoch = 0, index = 0, fills = 0, miners = [] } = {}) {
      const unit = batchCredit(fills);
      const out = [];
      for (const raw of miners) {
        const m = lower(raw);
        const amount = (unit * weight(m)) / 1000n;
        if (amount <= 0n) continue;
        const net = add(m, amount, 'credited');
        const s = stat(m); s.batches++; s.fills += Number(fills) || 0;
        out.push({ miner: m, amount, net, epoch: Number(epoch), index: Number(index) });
      }
      return out;
    },
    /// a dissent that was proven right
    bounty({ epoch = 0, index = 0, fills = 0, miner } = {}) {
      const amount = batchCredit(fills) * BOUNTY * weight(lower(miner)) / 1000n;
      add(miner, amount, 'bounty');
      stat(lower(miner)).dissents++;
      return { miner: lower(miner), amount, epoch: Number(epoch), index: Number(index) };
    },
    /// a vote on the losing side of a judged fork
    slash({ miner, fills = 0 } = {}) {
      const m = lower(miner);
      const frac = (accrued(m) * SNUM) / SDEN;
      const floor = MIN_SLASH > 0n ? MIN_SLASH : batchCredit(fills);
      const amount = frac > floor ? frac : floor;
      const r = take(m, amount);
      return { miner: m, amount, ...r };
    },
    /**
     * judge one fork: who was right. `agreers` voted with the sequencer, `dissenters` against it.
     *   dissenterWasRight = true   → the sequencer's batch was bad: bounty the dissenters, slash the agreers
     *   dissenterWasRight = false  → the dissenters were wrong: slash them
     * Idempotent by index, because a fork is judged once.
     */
    judge({ epoch = 0, index = 0, fills = 0, agreers = [], dissenters = [], dissenterWasRight = false } = {}) {
      if (judged.has(Number(index))) return null;
      judged.add(Number(index));
      const paid = [], cut = [];
      if (dissenterWasRight) {
        for (const d of dissenters) paid.push(this.bounty({ epoch, index, fills, miner: d }));
        for (const a of agreers) cut.push(this.slash({ miner: a, fills }));
      } else {
        for (const d of dissenters) cut.push(this.slash({ miner: d, fills }));
      }
      return { index: Number(index), epoch: Number(epoch), dissenterWasRight, paid, cut };
    },
    /// close an epoch: the rows, the root a validator attests, and the leaves a claim proves against.
    /// Closing is idempotent — a second call returns the same epoch unchanged.
    closeEpoch(epoch) {
      const e = Number(epoch);
      if (epochs.has(e)) return epochs.get(e);
      const rows = [...open.entries()].filter(([, v]) => v > 0n).map(([miner, amount]) => ({ miner, amount }))
        .sort((a, b) => (a.miner < b.miner ? -1 : a.miner > b.miner ? 1 : 0));   // by address: one canonical order
      const leaves = rows.map((r) => claimLeaf({ epoch: e, miner: r.miner, amount: r.amount }));
      const rec = { epoch: e, rows, leaves, rewardsRoot: merkleRoot(leaves), total: rows.reduce((a, r) => a + r.amount, 0n), miners: rows.length };
      epochs.set(e, rec);
      open.clear();
      return rec;
    },
    /// the proof a miner takes to claim(epoch, amount, proof)
    proofFor(epoch, miner) {
      const rec = epochs.get(Number(epoch)); if (!rec) return null;
      const m = lower(miner);
      const i = rec.rows.findIndex((r) => r.miner === m);
      if (i < 0) return null;
      return { epoch: rec.epoch, miner: m, amount: rec.rows[i].amount, rewardsRoot: rec.rewardsRoot, ...merkleProof(rec.leaves, i) };
    },
    /// what the claim contract would do
    verifyClaim(claim, root = null) {
      if (!claim) return false;
      const leaf = claimLeaf({ epoch: claim.epoch, miner: claim.miner, amount: claim.amount });
      if (leaf !== claim.leaf) return false;
      return verifyProof(claim, root || claim.rewardsRoot);
    },
    epochRootOf(epoch) { return epochs.get(Number(epoch))?.rewardsRoot || ZERO32; },
    epoch(epoch) { const r = epochs.get(Number(epoch)); return r ? { epoch: r.epoch, rewardsRoot: r.rewardsRoot, total: r.total, miners: r.miners, rows: r.rows } : null; },
    accruedOf(miner) { return accrued(lower(miner)); },
    debtOf(miner) { return owed(lower(miner)); },
    /// everything, with bigints as strings so it can go straight into JSON (/metrics, /v1/l3/status)
    state() {
      const accruedOut = {}, debtOut = {}, lifeOut = {};
      for (const [m, v] of open) accruedOut[m] = v.toString();
      for (const [m, v] of debt) debtOut[m] = v.toString();
      for (const [m, v] of life) lifeOut[m] = { credited: v.credited.toString(), bounty: v.bounty.toString(), slashed: v.slashed.toString(), batches: v.batches, fills: v.fills, dissents: v.dissents };
      return {
        unit: 'uROLLA', microPerRolla: MICRO_PER_ROLLA.toString(),
        params: { base: BASE.toString(), perFill: PER_FILL.toString(), bountyMultiple: BOUNTY.toString(), slash: `${SNUM}/${SDEN}`, minSlash: MIN_SLASH.toString(), weighted: !!weightOf },
        accrued: accruedOut, debt: debtOut, lifetime: lifeOut,
        epochs: [...epochs.values()].slice(-16).map((r) => ({ epoch: r.epoch, rewardsRoot: r.rewardsRoot, total: r.total.toString(), miners: r.miners })),
        openTotal: [...open.values()].reduce((a, v) => a + v, 0n).toString(),
        issuedTotal: [...life.values()].reduce((a, v) => a + v.credited + v.bounty, 0n).toString(),
        slashedTotal: [...life.values()].reduce((a, v) => a + v.slashed, 0n).toString(),
      };
    },
  };
}

/// the env knobs, in one place
export function rewardsFromEnv(env = process.env, extra = {}) {
  return createRewards({
    base: BigInt(env.L3_REWARD_BASE || 100),
    perFill: BigInt(env.L3_REWARD_PER_FILL || 1),
    bountyMultiple: BigInt(env.L3_REWARD_BOUNTY || 10),
    slashNum: BigInt(env.L3_SLASH_NUM || 1), slashDen: BigInt(env.L3_SLASH_DEN || 4),
    ...extra,
  });
}
