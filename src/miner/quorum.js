// engine/l3/miner/quorum.js — votes in, finality out, and the fork detector that stops settlement.
//
// A batch is FINAL when at least `threshold` distinct miners have published a vote whose batchRoot equals the
// sequencer's. Because batchRoot commits to shard, epoch, index, the sequence range, prevRoot, ordersRoot,
// fillsRoot and bookHash (merkle.js), that one comparison means they agree about every order, every fill and —
// at epoch boundaries — the book those fills left behind.
//
// Finality is IN ORDER: `finalIndex` advances only through contiguous finalized batches. The batch chain is a
// hash chain, so settling batch n while n−1 is disputed would settle fills whose preconditions are in doubt.
// One stuck batch stalls settlement behind it, by design.
//
// Five fork conditions, all detected without asking anyone (docs/L3-MINERS.md §3):
//   * a vote's batchRoot differs from the sequencer's                       → dissent
//   * two miners disagree with each other                                  → fork
//   * one miner votes twice for one index with different roots              → equivocation; its votes stop counting
//   * a batch's prevRoot is not the previous batch's batchRoot              → the log was spliced or rewritten
//   * forks reach `haltAfter` (default 1)                                   → HALT: finality stops, nothing settles
//
// Halting, not slashing, is the first response: nothing is lost by stopping (funds are in the desk, the AMM book
// keeps quoting), everything is lost by settling a disputed fill. The slashing evidence is kept here — both
// signed claims — for the on-chain arbiter that does not exist yet (§9.2).
import { voteDigestOf, batchRootOf, ZERO32 } from './merkle.js';
import { recoverDigestSigner } from './verify.js';

/// miners: an allowlist (array/Set of addresses) or null for "anyone who signs". threshold: distinct agreeing
/// miners needed. verify: (vote) => Promise<address|null>, overridable for tests and for a registry check.
export function createQuorum({ threshold = 2, miners = null, haltAfter = 1, keep = 4096, verify = null, rewards = null, onFinal = null, onFork = null, logger = null } = {}) {
  const allow = miners ? new Set([...miners].map((a) => String(a).toLowerCase())) : null;
  const batches = new Map();        // index → { batch, root, votes: Map<miner, vote>, agree: Set, dissent: Map, final, announced }
  const equivocators = new Set();
  const forks = [];
  const stats = { batches: 0, votes: 0, dissents: 0, stale: 0, unknown: 0, bad: 0, finalized: 0 };
  let finalIndex = -1, head = -1, halted = false, prevRoot = ZERO32, prevIndex = -1;

  const recover = verify || (async (v) => recoverDigestSigner(voteDigestOf(v), v.sig));
  const slotOf = (index) => {
    let s = batches.get(index);
    if (!s) { s = { index, batch: null, root: null, votes: new Map(), agree: new Set(), dissent: new Map(), final: false, at: Date.now() }; batches.set(index, s); }
    return s;
  };
  function fork(index, why, extra = {}) {
    const f = { ...extra, index, why, at: Date.now() };   // `why` is the CONDITION; extras never shadow it
    forks.push(f);
    logger && logger(`[l3quorum] FORK at ${index}: ${why}`);
    onFork && onFork(f);
    if (forks.length >= haltAfter) halted = true;
    return f;
  }
  /// drop finalized history we no longer need, oldest first (a miner's late vote for a pruned batch is 'stale')
  function prune() {
    if (batches.size <= keep) return;
    const idx = [...batches.keys()].sort((a, b) => a - b);
    for (const i of idx) { if (batches.size <= keep) break; if (i <= finalIndex) batches.delete(i); }
  }

  /// the sequencer's batch, as read back from the log. Returns { ok, slot } — ok false means a fork was raised.
  function announce(batch) {
    const index = Number(batch.index);
    const root = batch.batchRoot || batchRootOf(batch);
    if (batch.batchRoot && batchRootOf(batch) !== batch.batchRoot) {
      fork(index, 'the batch does not hash to the batchRoot it carries', { claimed: batch.batchRoot, computed: batchRootOf(batch) });
      return { ok: false, slot: slotOf(index) };
    }
    const s = slotOf(index);
    if (s.batch && s.root !== root) { fork(index, 'two different batches at the same index (sequencer equivocation)', { had: s.root, got: root }); return { ok: false, slot: s }; }
    s.batch = batch; s.root = root;
    stats.batches++;
    if (index > head) head = index;
    // the hash chain: this batch must follow the previous one we saw
    if (prevIndex >= 0 && index === prevIndex + 1 && (batch.prevRoot || ZERO32) !== prevRoot) {
      fork(index, 'prevRoot does not chain to the previous batch', { expected: prevRoot, got: batch.prevRoot });
      return { ok: false, slot: s };
    }
    if (index > prevIndex) { prevRoot = root; prevIndex = index; }
    // votes may have arrived before the batch did
    for (const v of s.votes.values()) classify(s, v);
    settleFinality();
    prune();
    return { ok: true, slot: s };
  }

  function classify(s, v) {
    if (!s.batch) return;                                   // cannot judge a vote without the batch
    const miner = v.miner;
    if (equivocators.has(miner)) return;
    // A vote counts as agreement only if the roots match AND the miner did not flag the batch. The second half
    // matters: a batch whose order signature is forged hashes to the SAME roots (the ops are what they are), so
    // a miner that caught it would otherwise be counted towards finality for the batch it just refused.
    if (v.batchRoot === s.root && v.ok !== false) { s.dissent.delete(miner); s.agree.add(miner); }
    else { s.agree.delete(miner); if (!s.dissent.has(miner)) { s.dissent.set(miner, v); stats.dissents++; fork(s.index, 'a miner disagrees with the sequencer', { miner, claimed: s.root, got: v.batchRoot, minerWhy: v.why || null, fillsRoot: v.fillsRoot, ordersRoot: v.ordersRoot }); } }
    // two miners that disagree with each other, both having voted
    // the miners' OWN roots, dissents included: more than one value means they disagree with each other, which is a
    // different (and worse) story than all of them disagreeing with the sequencer. A dissent whose root matches —
    // the forged-signature case — leaves this set at one, so it raises only the dissent above.
    const roots = new Set([...s.votes.values()].filter((x) => !equivocators.has(x.miner)).map((x) => x.batchRoot));
    if (roots.size > 1 && !forks.some((f) => f.index === s.index && f.why === 'miners disagree with each other')) fork(s.index, 'miners disagree with each other', { roots: [...roots] });
  }

  /// one signed vote. Returns { ok, why?, final?, agree?, dissent? }.
  async function vote(v) {
    if (!v || typeof v !== 'object' || !Number.isFinite(Number(v.index))) { stats.bad++; return { ok: false, why: 'malformed' }; }
    const index = Number(v.index);
    const signer = await recover(v);
    if (!signer) { stats.bad++; return { ok: false, why: 'bad signature' }; }
    if (v.miner && String(v.miner).toLowerCase() !== signer) { stats.bad++; return { ok: false, why: 'signature is not the miner it claims' }; }
    if (allow && !allow.has(signer)) { stats.unknown++; return { ok: false, why: 'not a known miner' }; }
    const vv = { ...v, miner: signer };
    if (index <= finalIndex && !batches.has(index)) { stats.stale++; return { ok: false, why: 'stale (that batch is pruned)' }; }
    const s = slotOf(index);
    const had = s.votes.get(signer);
    if (had && had.batchRoot !== vv.batchRoot) {
      equivocators.add(signer); s.agree.delete(signer);
      fork(index, 'a miner signed two different roots for one batch (equivocation)', { miner: signer, a: had, b: vv });
      return { ok: false, why: 'equivocation', evidence: [had, vv] };
    }
    s.votes.set(signer, vv); stats.votes++;
    classify(s, vv);
    settleFinality();
    return { ok: true, final: s.final, agree: s.agree.size, dissent: s.dissent.size, halted };
  }

  /// how many fills the agreeing miners say that batch produced. They agree on fillsRoot, so they agree on this;
  /// the batch itself does not carry it (a batch carries no fills, by design), so the votes are where it lives.
  function fillsOf(s) { for (const m of s.agree) { const v = s.votes.get(m); if (v && Number.isFinite(Number(v.fills))) return Number(v.fills); } return 0; }

  /// advance finality through contiguous finalized batches, firing onFinal in order
  function settleFinality() {
    for (const s of batches.values()) if (!s.final && s.batch && s.agree.size >= threshold) { s.final = true; stats.finalized++; }
    if (halted) return;
    for (;;) {
      const next = batches.get(finalIndex + 1);
      if (!next || !next.final) break;
      finalIndex++;
      const epoch = Number(next.batch.epoch), fills = fillsOf(next);
      // the batch is final: every miner that agreed earns its µROLLA for it (rewards.js). Paying only the agreeing
      // set is the whole incentive — a miner that did not replay, or replayed differently, is not paid for it.
      const credit = rewards ? rewards.creditBatch({ epoch, index: next.index, fills, miners: [...next.agree] }) : null;
      try { onFinal && onFinal({ index: next.index, epoch, fills, batch: next.batch, batchRoot: next.root, votes: next.agree.size, miners: [...next.agree], credit }); }
      catch (e) { logger && logger(`[l3quorum] onFinal failed at ${next.index}: ${e.message}`); }
    }
  }

  return {
    announce, vote,
    get finalIndex() { return finalIndex; },
    get head() { return head; },
    get halted() { return halted; },
    /// a batch's standing, for /v1/l3/status and the miner's /metrics
    at(index) { const s = batches.get(Number(index)); return s ? { index: s.index, root: s.root, final: s.final, agree: [...s.agree], dissent: [...s.dissent.keys()], votes: s.votes.size } : null; },
    forks: () => forks.slice(-32),
    rewards,
    /**
     * judge a fork, off chain for now (docs/L3-MINERS.md §9.2 wants an on-chain arbiter). The evidence is already
     * here: the sequencer's signed batch and every miner's signed vote. `dissenterWasRight` decides who pays.
     */
    judge(index, { dissenterWasRight = false } = {}) {
      const s = batches.get(Number(index)); if (!s || !rewards) return null;
      const r = rewards.judge({ epoch: Number(s.batch?.epoch || 0), index: Number(index), fills: fillsOf(s),
                                agreers: [...s.agree], dissenters: [...s.dissent.keys()], dissenterWasRight });
      logger && logger(`[l3quorum] fork ${index} judged: ${dissenterWasRight ? 'the dissenter was right' : 'the dissenter was wrong'}`);
      return r;
    },
    equivocators: () => [...equivocators],
    threshold,
    /// resume after a fork has been judged off chain. Deliberately explicit: nothing clears a halt by itself.
    resume({ forget = false } = {}) { if (forget) forks.length = 0; halted = false; settleFinality(); return { halted, forks: forks.length }; },
    status() {
      const pending = [];
      for (const s of batches.values()) if (!s.final) pending.push({ index: s.index, agree: s.agree.size, dissent: s.dissent.size, has: !!s.batch });
      pending.sort((a, b) => a.index - b.index);
      return { threshold, finalIndex, head, halted, forks: forks.length, lastFork: forks.length ? forks[forks.length - 1] : null,
               equivocators: [...equivocators], tracked: batches.size, pending: pending.slice(0, 16), ...stats,
               rewards: rewards ? rewards.state() : null };
    },
  };
}
