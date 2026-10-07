// engine/l3/miner/sequencer.js — the shard sequencer, and the rig that wires it into the engine.
//
// The sequencer's only privilege is ORDER (docs/L3-MINERS.md §1). It admits signed orders — the engine already
// checks the signature, the session grant, the desk balance, the nonce floor and the deadline in
// engine/l3/book.js — decides the sequence, applies them to its book, and every `batchMs` (or `batchMax` orders)
// seals a batch: the ops in order, the Merkle root of the ops, the Merkle root of the fills they produced, the
// book commitment at epoch boundaries, the previous batch's root, and its own signature. Then it appends that
// to `orders.<shard>` and the miners take over.
//
// What it cannot do, and this is the whole design: change a fill after sealing (batchRoot is signed and
// chained), hide an order (its absence changes ordersRoot, and the trader holds a receipt), reorder silently
// (the ops are in the batch, in order, signed), or settle a batch no quorum accepted (the rig stages the fills
// until quorum.js finalizes the batch).
//
// The batch carries the ops and the roots but NEVER the fills, so a miner cannot vote without matching.
//
// Two shapes, one implementation:
//   submit(op)        — the sequencer applies the op to its own book and records it (a standalone sequencer,
//                       and what the tests drive).
//   record(op, fills) — the caller already matched (engine/l3/book.js has to: it answers the HTTP request with
//                       the fills) and hands over the op and the fills it produced. Returns the batch slot the
//                       fills belong to, which is what the engine stages them under until finality.
import path from 'node:path';
import { privateKeyToAccount } from 'viem/accounts';
import { fillsRootOf, ordersRootOf, batchRootOf, epochDigestOf, ZERO32 } from './merkle.js';
import { signDigest } from './verify.js';
import { createShardState } from './miner.js';
import { createCommitState } from './commit-state.js';
import { createQuorum } from './quorum.js';
import { createLog, ordersTopic, votesTopic } from './log.js';
import { rewardsFromEnv } from './rewards.js';
import { serializeL3Order } from '../desk.js';

const lower = (a) => String(a || '').toLowerCase();

/**
 * createSequencer({ shard, log, account, ... })
 *   shard         the shard id (the market id as a string, by default)
 *   log           a Log (log.js) — the replicated log, NOT a printer (`logger` is the printer)
 *   account       a viem account — the operator key. Only its address is ever printed.
 *   state         what the batch commits to: anything with `seq` (the shard's op counter) and `bookHash()`.
 *                 createShardState() from miner.js is the standalone case — the same state machine every miner
 *                 runs — and the rig passes an adapter over the engine's OWN books instead, because the engine
 *                 matched the orders and its book is what the commitment has to describe. `apply()` is needed
 *                 only if submit() is used.
 *   batchMs       seal interval, 50–100 ms in production (L3-SIDECHAIN.md §3)
 *   batchMax      seal early at this many ops
 *   epochBatches  batches per epoch; the last batch of an epoch carries the book commitment (§6.6)
 *   sealEmpty     seal a batch with no ops (a liveness heartbeat). Off by default: the log stays meaningful.
 *   onSealed      (batch, fills) => void, after the batch is in the log
 *   rewardsRoot   () => { root, epoch } — the µROLLA rewards root of the last epoch whose batches are all
 *                 final (rewards.js). Carried in the epoch-boundary batch and attested with it; deliberately
 *                 NOT part of batchRoot, because rewards depend on votes that arrive after the batch is sealed
 *                 (docs/L3-MINERS.md §6a).
 *   onEpoch       (epochDigest, batch) => void, at an epoch boundary — what a validator attests
 */
export function createSequencer({
  shard, log, account, state = null, batchMs = 60, batchMax = 500, epochBatches = 20,
  sealEmpty = false, onSealed = null, onEpoch = null, rewardsRoot = null, logger: logFn = console.log, newBook = null,
  appendBytes = 6 << 20,   // one append request carries as many sealed batches as fit under this (the broker's limit is 8 MiB on rolla-l3-broker)
} = {}) {
  if (!shard) throw new Error('a sequencer needs a shard');
  if (!log) throw new Error('a sequencer needs a log');
  if (!account?.address) throw new Error('a sequencer needs an account');
  const st = state || createShardState({ newBook, logger: logFn });
  const ownState = !state;    // a state the sequencer made is the sequencer's to close (its books, its commit tree)
  if (typeof st.bookHash !== 'function') throw new Error('a sequencer state needs bookHash()');
  const topic = ordersTopic(shard);
  const m = { batches: 0, ops: 0, fills: 0, sealed: 0, failed: 0, retried: 0, sealMs: 0, rootMs: 0, lastSealAt: 0 };
  let index = 0, epoch = 0, prevRoot = ZERO32, seqFrom = 1;
  let open = { ops: [], fills: [] };
  const queued = [];   // full batches (batchMax ops) waiting for their seal, in order — a seal in flight must not let the next batch grow past batchMax
  let timer = null, chain = Promise.resolve(), stopped = false, ready = false;
  const retry = [];
  const pending = [];   // sealed, signed batches waiting for the log, in order — the seal never waits for the append (see pump)
  let pumping = null;

  const slotOf = (i) => `${shard}#${i}`;
  /// where an op recorded right now will land. Known before the batch is sealed, which is what lets the engine
  /// stage a fill under its batch the moment it happens.
  const slot = () => slotOf(index + queued.length);

  function arm() {
    if (timer || stopped) return;
    timer = setTimeout(() => { timer = null; sealSoon(); }, batchMs);
    timer.unref?.();
  }
  const sealSoon = () => { chain = chain.then(() => seal()).catch((e) => { m.failed++; logFn(`[l3seq] seal failed: ${e.message}`); }); return chain; };

  /// a full batch closes NOW: its seq range and, at an epoch end, its book commitment are taken from the state as it
  /// is at this moment — later ops keep being applied to the state while this batch waits for its seal, and a hash
  /// taken at seal time would describe a book the batch's own ops never produced (the miners dissented on exactly
  /// the epoch-boundary batches when a remote log made batches queue up)
  function closeOpen() {
    const idx = index + queued.length;
    open.seqTo = st.seq;
    if (epochBatches > 0 && (idx + 1) % epochBatches === 0) open.bookHash = st.bookHash();
    queued.push(open); open = { ops: [], fills: [] };
  }
  /// record an op the caller has already applied to the shard's book
  function record(op, fills = []) {
    if (stopped) return null;
    open.ops.push(op);
    if (fills.length) open.fills.push(...fills);
    m.ops++; m.fills += fills.length;
    const here = slot();
    if (open.ops.length >= batchMax) { closeOpen(); sealSoon(); } else arm();
    return here;
  }
  /// apply an op to the sequencer's own book and record it
  function submit(op) {
    if (typeof st.apply !== 'function') throw new Error('this sequencer records only (no state to apply to)');
    const r = st.apply(op);
    const where = record(op, r.fills || []);
    return { ...r, slot: where, index };
  }

  /// seal the open batch. Everything up to the signature is synchronous, so an op that arrives mid-seal lands in
  /// the NEXT batch and nothing is ever counted twice or dropped.
  async function seal() {
    if (stopped) return null;
    if (!queued.length && !open.ops.length && !sealEmpty) return null;
    const t0 = process.hrtime.bigint();
    const take = queued.length ? queued.shift() : open;   // oldest full batch first; the open one only when nothing is queued
    if (take === open) { open = { ops: [], fills: [] }; take.seqTo = st.seq; }
    const ops = take.ops, fills = take.fills;
    const atEpochEnd = epochBatches > 0 && (index + 1) % epochBatches === 0;
    const t1 = process.hrtime.bigint();
    const ordersRoot = ordersRootOf(ops);
    const fillsRoot = fillsRootOf(fills);
    const bookHash = atEpochEnd ? (take.bookHash || st.bookHash()) : ZERO32;
    m.rootMs += Number(process.hrtime.bigint() - t1) / 1e6;
    const rw = atEpochEnd && rewardsRoot ? (rewardsRoot() || null) : null;
    const batch = {
      shard: String(shard), epoch, index, seqFrom, seqTo: take.seqTo,
      prevRoot, ordersRoot, fillsRoot, bookHash,
      rewardsRoot: rw?.root || ZERO32, rewardsEpoch: rw && rw.epoch >= 0 ? rw.epoch : -1,
      ops, at: Date.now(), sequencer: account.address, batchRoot: ZERO32, sig: '0x',
    };
    batch.batchRoot = batchRootOf(batch);
    // advance the chain BEFORE awaiting anything: the next batch's prevRoot is this one's root, whatever
    // happens to the append
    const thisIndex = index, thisEpoch = epoch;
    prevRoot = batch.batchRoot; index++; seqFrom = take.seqTo + 1;
    if (atEpochEnd) epoch++;
    batch.sig = await signDigest(account, batch.batchRoot);
    // the seal is done: the batch joins the append pipeline and the NEXT seal starts now, while this one is in
    // flight. The log's order is the pipeline's order, and onSealed/onEpoch fire once the log has answered.
    pending.push({ batch, fills, bytes: Buffer.byteLength(JSON.stringify(batch)), thisIndex, thisEpoch, atEpochEnd, bookHash, fillsTotal: m.fills });
    m.batches++; m.lastSealAt = Date.now(); m.sealMs += Number(process.hrtime.bigint() - t0) / 1e6;
    pump();
    if (queued.length) sealSoon(); else if (open.ops.length) arm();
    return batch;
  }
  /// the append pipeline. Seals never wait for the log: pending batches go out in order, as many per request as
  /// fit under appendBytes (one produce = one round trip whatever it carries, and a single-partition record batch
  /// is accepted or refused whole), and a seal in progress overlaps the request in flight. From outside the
  /// broker's region the round trip (260–300 ms to Amsterdam) WAS the sequencing budget (L3-MINERS.md §6).
  function pump() {
    if (!pumping) pumping = (async () => { try { await pumpLoop(); } catch (e) { logFn(`[l3seq] append pipeline: ${e.message}`); } finally { pumping = null; if (pending.length) pump(); } })();
    return pumping;
  }
  async function pumpLoop() {
    while (pending.length) {
      if (retry.length) await drain();
      const group = [pending[0]]; let bytes = pending[0].bytes;
      while (pending.length > group.length && bytes + pending[group.length].bytes <= appendBytes) { bytes += pending[group.length].bytes; group.push(pending[group.length]); }
      pending.splice(0, group.length);
      if (retry.length) {
        // the log is still refusing: these wait behind the batches already waiting, in order
        for (const p of group) refused(p.batch, 'the log is refusing earlier batches');
      } else if (group.length > 1 && typeof log.appendMany === 'function') {
        try { await log.appendMany(topic, group.map((p) => p.batch)); m.sealed += group.length; }
        catch (e) { for (const p of group) refused(p.batch, e.message); }
      } else {
        for (let i = 0; i < group.length; i++) {
          try { await log.append(topic, group[i].batch); m.sealed++; }
          catch (e) { for (let j = i; j < group.length; j++) refused(group[j].batch, e.message); break; }
        }
      }
      for (const p of group) finish(p);
    }
  }
  /// the log refused a batch. The chain is already advanced, so the batch is kept and retried: a batch that
  /// never reaches the log can never finalize, and its fills therefore never settle — which is the correct
  /// failure (docs/L3-MINERS.md §7, "Kafka partition").
  function refused(batch, why) {
    retry.push(batch); m.failed++;
    logFn(`[l3seq] ${shard}#${batch.index} could not be logged (${why}); ${retry.length} batch(es) waiting`);
  }
  function finish(p) {
    try { onSealed && onSealed(p.batch, p.fills); } catch (e) { logFn(`[l3seq] onSealed failed: ${e.message}`); }
    if (p.atEpochEnd && onEpoch) {
      const e = { shard: String(shard), epoch: p.thisEpoch, index: p.thisIndex, batchRoot: p.batch.batchRoot, bookHash: p.bookHash, fills: p.fillsTotal, rewardsRoot: p.batch.rewardsRoot, rewardsEpoch: p.batch.rewardsEpoch };
      try { onEpoch({ ...e, digest: epochDigestOf(e) }, p.batch); } catch (err) { logFn(`[l3seq] onEpoch failed: ${err.message}`); }
    }
  }
  const settled = async () => { while (pending.length || pumping) await (pumping || pump()); };
  /// re-append batches the log refused, oldest first; stops at the first failure so the log stays in order
  async function drain() {
    while (retry.length) {
      const b = retry[0];
      try { await log.append(topic, b); retry.shift(); m.retried++; m.sealed++; }
      catch { return; }
    }
  }

  /// resume from the log: the last batch there fixes index, epoch and prevRoot, so a restarted sequencer
  /// continues the same chain instead of forking it. Offsets and indices coincide for a single-sequencer shard
  /// (one append per batch), which is why a miner can subscribe at `index + 1`.
  /// `known`: the log's last record when the caller has already read the log (the rig replays the whole shard
  /// before resuming — see createSequencerRig's reconcile); undefined reads the tail here, null is an empty log
  async function resume(known = undefined) {
    let last = known;
    if (last === undefined) {
      last = null;
      const n = await log.offset(topic);
      if (n > 0) { const tail = await log.read(topic, Math.max(0, n - 1), 1); last = tail.length ? tail[tail.length - 1].value : null; }
    }
    if (last && String(last.shard) === String(shard)) {
      // an epoch-boundary batch (the one carrying a book commitment) closes its epoch
      index = Number(last.index) + 1; prevRoot = last.batchRoot; seqFrom = Number(last.seqTo) + 1;
      epoch = Number(last.epoch) + (last.bookHash && last.bookHash !== ZERO32 ? 1 : 0);
      logFn(`[l3seq] ${shard}: resuming at index ${index}, epoch ${epoch}, prevRoot ${prevRoot.slice(0, 12)}…`);
    }
    ready = true;
    return { index, epoch, prevRoot };
  }

  return {
    shard: String(shard), topic, state: st, submit, record, seal, resume, slot,
    get index() { return index; }, get epoch() { return epoch; }, get prevRoot() { return prevRoot; },
    get ready() { return ready; }, get open() { return open.ops.length + queued.reduce((n, b) => n + b.ops.length, 0); },
    async flush() { await sealSoon(); await chain; await settled(); return chain; },
    status: () => ({ shard: String(shard), index, epoch, prevRoot, open: open.ops.length + queued.reduce((n, b) => n + b.ops.length, 0), queued: queued.length, pending: pending.length, retry: retry.length, ready,
                     batchMs, batchMax, epochBatches, ...m, perBatchMs: m.batches ? Number((m.sealMs / m.batches).toFixed(3)) : 0,
                     state: st.stat ? st.stat() : { seq: st.seq } }),      // the rig's adapter has no books of its own to report
    async stop() { stopped = true; if (timer) { clearTimeout(timer); timer = null; } try { await chain; await settled(); } catch {} if (ownState) { try { st.close?.(); } catch {} } },
  };
}

// --------------------------------------------------------------------------------------------------- the rig
/**
 * createSequencerRig — everything the engine needs behind L3_MINERS=1, in one object with three hooks.
 *
 *   record(book, rec, fills) → slot | null   the order (or cancel) is sequenced into the log; its fills are
 *                                            the caller's to stage under the returned slot. null means "not
 *                                            sequenced" (the log is not ready yet) and the caller settles as
 *                                            it does today.
 *   status()                                 for /v1/l3/status
 *   stop()
 *
 * Finality arrives through `onFinal({ slot, index, shard, batch, votes, finalSigs })`, and a fork through
 * `onFork(f)`. One sequencer and one quorum per shard; a shard is one market by default (L3_SHARD_BY=outcome
 * gives a shard per book instead, for a market hot enough to deserve it).
 *
 * `resumeFrom(shard)` → index | null (async allowed): after a restart, the first batch index whose finality the
 * caller still needs — the engine answers with the oldest batch it still has fills staged under, and, settling
 * from roots, the chain's own `nextIndex` (docs/L3-MINERS.md §4). The rig re-announces the log's batches from
 * there and replays the votes, so those batches finalize again instead of the quorum waiting forever for index 0.
 * Without it the quorum starts at the log's tail: nothing before the restart is finalized again.
 *
 * `booksOf()` → every engine book ({ key, market, outcome, m }), `recOf(hash)` → the engine's order record ({ hash,
 * o, sig, signer, at }) and `swapBook(book, matcher)` are what the boot-time RECONCILE needs (see reconcile() below):
 * the engine's books of a shard are compared with the log's own replay, whatever the log never got is sequenced,
 * and the engine's matchers are replaced by the log's so the sequence counters agree with every miner's. Without
 * booksOf only the books the rig has seen are reconciled; without swapBook the matchers are kept (native books).
 */
export function createSequencerRig({
  env = process.env, dir = null, account = null, logger: logFn = console.log,
  onFinal = null, onFork = null, onEpoch = null, newBook = null, resumeFrom = null,
  booksOf = null, recOf = null, swapBook = null,
} = {}) {
  const acct = account || (env.OPERATOR_KEY ? privateKeyToAccount(env.OPERATOR_KEY.startsWith('0x') ? env.OPERATOR_KEY : '0x' + env.OPERATOR_KEY) : null);
  if (!acct) throw new Error('the L3 sequencer needs OPERATOR_KEY');
  const logDir = dir || path.join(env.L3_DIR || (env.DATA_DIR ? path.join(env.DATA_DIR, 'l3') : '.l3'), 'log');
  const threshold = Number(env.L3_THRESHOLD || 2);
  const allow = (env.L3_MINERS_ALLOW || '').split(',').map((s) => s.trim()).filter(Boolean);
  const byOutcome = env.L3_SHARD_BY === 'outcome';
  const batchMs = Number(env.L3_BATCH_MS || 60), batchMax = Number(env.L3_BATCH_MAX || 500), epochBatches = Number(env.L3_EPOCH_BATCHES || 20);
  const shardOf = (market, outcome) => (byOutcome ? `${Number(market)}-${Number(outcome)}` : String(Number(market)));

  const shards = new Map();     // shard → { seq, quorum, sub, books, state, rewards, closedEpoch, deferred, reconcile }
  let logImpl = null, started = null, stopping = false;
  const stats = { finalized: 0, forks: 0, records: 0, skipped: 0, deferred: 0, reconciled: 0, unreconciled: 0, rebuilt: 0, epochsClosed: 0 };
  const PAGE = Math.max(1, Number(env.L3_RECONCILE_PAGE || 1000));

  /// the log's view of a shard: every batch replayed through the same state machine a miner runs (miner.js's
  /// createShardState, bookhash mode — the resting SET and the sequence counters are what matter here). What the
  /// miners hold is exactly this, so it is what the engine's books have to be reconciled with before anything
  /// new is sequenced. Paged, so a long shard does not need its whole log in memory at once.
  async function replayLog(impl, topic, shard) {
    const state = createShardState({ commit: 'bookhash', env, logger: logFn });
    const n = await impl.offset(topic);
    let last = null, batches = 0, ops = 0, bad = 0;
    for (let from = 0; from < n; from += PAGE) {
      const rows = await impl.read(topic, from, Math.min(PAGE, n - from));
      for (const { value } of rows) {
        if (!value || String(value.shard) !== String(shard)) continue;
        for (const op of value.ops || []) { try { state.apply(op); ops++; } catch { bad++; } }
        last = value; batches++;
      }
    }
    if (bad) logFn(`[l3rig] ${shard}: ${bad} op(s) in the log could not be replayed`);
    return { state, last, batches, ops, n };
  }
  /// the same resting orders, with the same remaining sizes
  function sameResting(a, b) {
    if (a.length !== b.length) return false;
    const m = new Map(a.map((o) => [lower(o.hash), BigInt(o.remaining)]));
    for (const o of b) { const r = m.get(lower(o.hash)); if (r === undefined || r !== BigInt(o.remaining)) return false; }
    return true;
  }
  /**
   * reconcile the engine's books of a shard with the log, at the shard's boot and before anything new is sequenced.
   *
   * The engine's book is rebuilt from its own journal at a restart; the miners' from the log. The two diverge
   * whenever the engine applied an op the log never got: a cancel recorded within batchMs of the process exiting
   * (the market maker's shutdown cancel-all, 2026-10-07: the batch timer never fired), an order the replay expired,
   * an op applied while the shard's log was still being resumed (deferred below), or — the other way — an add
   * whose batch was sealed but never appended. The epoch commitment then differs for good, and since the matcher's
   * sequence counter (part of every resting leaf AND every fill leaf) advances on cancels too, so would every
   * later fill's root: the fleet dissented on every epoch boundary of four shards after the deploys of 10-07.
   *
   * So: (1) an order the log holds that the engine no longer does is cancelled in the log (`by: 'reconcile'`) —
   * first, so that nothing the engine already dropped is still resting when (2) an order resting here that the log
   * never got is re-sequenced (an untouched one only — a partly filled order cannot be replayed; and a post-only
   * order re-sequenced over a stale orphan would be refused as crossing: the live dry run of 18104 showed it);
   * (3) the ops deferred during the resume follow, in their order; (4) the engine's matchers are REPLACED by the
   * log's replay plus those ops — the same resting orders, the miners' sequence numbers — and the commitment is
   * re-seeded from them. A book that still differs afterwards keeps its matcher and is reported: the shard will
   * dissent at epoch boundaries until that order leaves the book.
   */
  function reconcile(s, shadow) {
    const engineBooks = (booksOf ? booksOf() : [...s.books]).filter((b) => b && b.m && shardOf(b.market, b.outcome) === s.shard);
    for (const b of engineBooks) s.state.track(b);       // every book of the shard is in the commitment, resting since before this boot included
    const out = { at: Date.now(), logBatches: shadow.batches, logOps: shadow.ops, deferred: s.deferred.length, cancelled: 0, resequenced: 0, unreconciled: 0, rebuilt: 0, kept: 0, books: engineBooks.length };
    // the deferred ops are in the engine's book already and will be sequenced below (step 3): what they added is
    // not "unlogged", what they cancelled is not "orphaned". The shadow gets every op in the order the miners will.
    const deferredAdds = new Set(), deferredCancels = new Set();
    for (const d of s.deferred) (d.op.t === 'cancel' ? deferredCancels : deferredAdds).add(lower(d.op.hash));
    const mine = new Map(); for (const b of engineBooks) for (const o of b.m.orders()) mine.set(lower(o.hash), { o, b });
    const theirs = new Map(); for (const [key, sb] of shadow.state.books) { const [market, outcome] = key.split(':').map(Number); for (const o of sb.orders()) theirs.set(lower(o.hash), { o, market, outcome }); }
    const sequence = (op, fills = []) => { s.state.ops++; stats.records++; try { shadow.state.apply(op); } catch {} return s.seq.record(op, fills); };
    for (const { o, market, outcome } of theirs.values()) if (!mine.has(lower(o.hash)) && !deferredCancels.has(lower(o.hash))) { out.cancelled++; sequence({ t: 'cancel', hash: o.hash, market, outcome, user: lower(o.user), by: 'reconcile', at: Date.now() }); }
    const unlogged = [...mine.values()].filter(({ o }) => !theirs.has(lower(o.hash)) && !deferredAdds.has(lower(o.hash))).sort((a, b) => Number(a.o.seq) - Number(b.o.seq));
    for (const { o, b } of unlogged) {
      const rec = recOf ? recOf(o.hash) : null;
      if (rec && rec.o && rec.sig && BigInt(o.remaining) === BigInt(rec.o.size)) { out.resequenced++; sequence({ t: 'add', hash: rec.hash, market: Number(b.market), outcome: Number(b.outcome), order: serializeL3Order(rec.o), sig: rec.sig, signer: lower(rec.signer || rec.o.user), at: rec.at || Date.now() }); }
      else { out.unreconciled++; logFn(`[l3rig] ${s.shard}: resting order ${String(o.hash).slice(0, 12)}… is not in the log and cannot be re-sequenced (${rec ? 'partly filled' : 'no record'}); the miners will not hold it`); }
    }
    for (const d of s.deferred.splice(0)) sequence(d.op, d.fills);
    if (swapBook) for (const b of engineBooks) {
      const sb = shadow.state.books.get(`${Number(b.market)}:${Number(b.outcome)}`);
      if (!sb) continue;                                   // the log never saw this book: nothing to take the counter from
      if (!sameResting(b.m.orders(), sb.orders())) { out.kept++; logFn(`[l3rig] ${s.shard}: book ${b.key || `${b.market}:${b.outcome}`} still differs from the log after the reconcile; its matcher is kept`); continue; }
      const old = b.m.orders();
      swapBook(b, sb);
      for (const o of old) s.state.commit.cancelled(o.hash);   // the tree's leaves carry sequence numbers: re-seed (bookhash mode walks the books)
      s.state.commit.seed(b.m.orders());
      out.rebuilt++;
    }
    s.reconcile = out;
    stats.deferred += out.deferred; stats.reconciled += out.cancelled + out.resequenced; stats.unreconciled += out.unreconciled; stats.rebuilt += out.rebuilt;
    if (out.cancelled || out.resequenced || out.unreconciled || out.deferred || out.kept) logFn(`[l3rig] ${s.shard}: reconciled with the log (${shadow.batches} batch(es), ${shadow.ops} op(s)): ${out.cancelled} cancel(s) the log never got, ${out.resequenced} order(s) re-sequenced, ${out.deferred} op(s) deferred during the resume, ${out.unreconciled} not reconcilable, ${out.rebuilt} book(s) now numbered as the log${out.kept ? `, ${out.kept} kept` : ''}`);
    return out;
  }
  // the log down (a broker unreachable at boot, or gone): the first attempt pays kafkajs's bounded connect retry,
  // every call in the next L3_LOG_RETRY_MS fails at once with the same reason — an order never waits on a dead
  // broker twice — and a shard whose boot failed is re-armed on its next prepare() once that window has passed.
  // The engine keeps running either way: fills simply settle as they do without miners (null slots).
  const retryMs = Number(env.L3_LOG_RETRY_MS || 30000);
  const logDown = { at: 0, error: null, attempts: 0 };

  async function boot() {
    logDown.attempts++;
    logImpl = await createLog({ env, dir: logDir, clientId: `rolla-seq-${process.pid}`, logger: logFn });
    logFn(`[l3rig] sequencing into ${logImpl.kind} log${logImpl.dir ? ' at ' + logImpl.dir : ''} · threshold ${threshold}${allow.length ? ` · ${allow.length} allowed miner(s)` : ' · any signed vote counts'}`);
    logDown.at = 0; logDown.error = null;
    return logImpl;
  }
  const ready = () => {
    if (started) return started;
    if (logDown.at && Date.now() - logDown.at < retryMs) return Promise.reject(new Error(`the L3 log is unavailable (${logDown.error}); next attempt in ${Math.ceil((retryMs - (Date.now() - logDown.at)) / 1000)} s`));
    return (started = boot().catch((e) => { logFn(`[l3rig] log unavailable: ${e.message} — the book keeps running, fills settle without miners; retry in ${Math.round(retryMs / 1000)} s`); started = null; logImpl = null; logDown.at = Date.now(); logDown.error = e.message; throw e; }));
  };

  function shardFor(shard) {
    let s = shards.get(shard);
    if (s) return s;
    const rewards = rewardsFromEnv(env);
    const s0 = { closedEpoch: -1 };
    const quorum = createQuorum({
      threshold, miners: allow.length ? allow : null, logger: logFn, rewards,
      onFinal: (f) => {
        stats.finalized++;
        // an epoch's µROLLA can only be closed once every batch in it is final: epoch E covers indices
        // [E·epochBatches, (E+1)·epochBatches − 1], so this index closes everything up to here
        const doneEpoch = epochBatches > 0 ? Math.floor((f.index + 1) / epochBatches) - 1 : -1;
        while (s0.closedEpoch < doneEpoch) {
          const e = ++s0.closedEpoch; const rec = rewards.closeEpoch(e); stats.epochsClosed++;
          logFn(`[l3rig] ${shard} epoch ${e} rewards closed: ${rec.miners} miner(s), ${rec.total} µROLLA, root ${rec.rewardsRoot.slice(0, 12)}…`);
        }
        onFinal && onFinal({ slot: `${shard}#${f.index}`, shard, index: f.index, epoch: f.epoch, fills: f.fills, batch: f.batch, batchRoot: f.batchRoot, votes: f.votes, miners: f.miners, finalSigs: f.finalSigs || [], credit: f.credit });
      },
      onFork: (f) => { stats.forks++; onFork && onFork({ shard, ...f }); },
    });
    // the state the batch commits to is the ENGINE's books, not a shadow of them: the engine did the matching,
    // and a commitment to anything else would be a commitment to a book nobody traded on. `ops` is the shard's
    // op counter, defined exactly as createShardState defines it, so every miner's counter agrees.
    //
    // The commitment is commit-state.js's, in the mode L3_COMMIT names, and in tree mode it is fed by the SAME
    // events a miner's createShardState feeds its tree with — the engine's add result and its cancel — so the
    // sequencer's root is the root a miner replaying the log arrives at. A book joining the shard seeds the tree
    // with its resting orders as they are at that moment (after a restart the engine's replay rebuilt them before
    // anything was sequenced); from then on every mutation of the engine's book is mirrored, sequenced or not.
    const books = new Set();
    const cs = createCommitState({ env, resting: () => [...books].flatMap((b) => b.m.orders()), logger: logFn });
    const state = {
      ops: 0, get seq() { return this.ops; }, bookHash: () => cs.root(), commit: cs,
      /// true when the book was new to the shard (its resting orders were just seeded, this op included)
      track(book) { if (books.has(book)) return false; books.add(book); cs.seed(book.m.orders()); return true; },
      /// one order the engine matched: `result` is the book's add result; without it the taker is looked up
      applied(book, rec, fills, result) {
        if (state.track(book)) return;
        const o = rec.o, user = lower(o.user), me = { hash: rec.hash, user, buy: o.buy, price: o.price };
        if (result) return cs.applied({ fills, rested: result.rested, remaining: result.remaining, seq: result.seq }, me);
        for (const f of fills) cs.filled(f);
        const live = book.m.get(rec.hash); if (live) cs.rested({ ...me, remaining: live.remaining, seq: live.seq });
      },
      /// one cancel: `o` is what the book's cancel() returned (null: it hit nothing); unknown → let the tree say
      cancelled(book, rec, o) { if (state.track(book)) return; if (o !== null) cs.cancelled(rec.hash); },
      close() { cs.close(); },
    };
    const seq = createSequencer({
      shard, state,
      log: { kind: 'deferred', append: async (t, v) => (await ready()).append(t, v), appendMany: async (t, vs) => (await ready()).appendMany(t, vs), offset: async (t) => (await ready()).offset(t), read: async (t, f, l) => (await ready()).read(t, f, l) },
      appendBytes: Number(env.L3_APPEND_BYTES || 6 << 20),
      account: acct, batchMs, batchMax, epochBatches, logger: logFn, newBook,
      onSealed: (batch) => { quorum.announce(batch); },
      rewardsRoot: () => ({ root: rewards.epochRootOf(s0.closedEpoch), epoch: s0.closedEpoch }),
      onEpoch,
    });
    s = { shard, seq, quorum, sub: null, books, state, rewards, ready: null, failedAt: 0, booting: false, deferred: [], reconcile: null, get closedEpoch() { return s0.closedEpoch; } };
    shards.set(shard, s);
    // resume the chain from the log — the tail fixes index, epoch and prevRoot — then listen for the miners'
    // votes. The promise is kept so a caller can await the first order of a new market instead of having it
    // settle unsequenced: a batch index cannot be handed out before the log says where the chain is. A boot that
    // fails (the log down) marks the shard and prepare() re-arms it after the retry window.
    s.boot = async () => {
      s.booting = true;
      try {
        const impl = await ready();
        // the log's own replay of the shard, then the chain's tail from it; the engine's books are reconciled
        // with it — and renumbered as it — before the first new op is sequenced (see reconcile)
        const shadow = await replayLog(impl, seq.topic, shard);
        await seq.resume(shadow.last);
        s.state.ops = shadow.state.seq;      // the shard's op counter continues the log's: every miner counts the same ops
        reconcile(s, shadow);
        // where finality resumes: the log's tail unless the caller still needs earlier batches (see resumeFrom)
        let from = seq.index;
        if (resumeFrom) { try { const r = await resumeFrom(shard); if (r != null && Number.isFinite(Number(r))) from = Math.max(0, Math.min(from, Number(r))); } catch (e) { logFn(`[l3rig] resumeFrom(${shard}) failed: ${e.message}`); } }
        quorum.start(from);
        if (from < seq.index) {
          // offsets and indices coincide for a single-sequencer shard (one append per batch — see resume())
          const rows = await impl.read(seq.topic, from, seq.index - from);
          let n = 0; for (const { value } of rows) if (value && String(value.shard) === String(shard) && Number(value.index) >= from) { quorum.announce(value); n++; }
          logFn(`[l3rig] ${shard}: re-announced ${n} batch(es) from index ${from} (log at ${seq.index}) for finality after restart`);
        }
        s.sub = await impl.subscribe(votesTopic(shard), 0, async ({ value }) => { try { await quorum.vote(value); } catch (e) { logFn(`[l3rig] vote rejected: ${e.message}`); } });
        s.failedAt = 0;
      } catch (e) { logFn(`[l3rig] shard ${shard} not ready: ${e.message}`); s.failedAt = Date.now(); }
      finally { s.booting = false; }
      return s;
    };
    s.ready = s.boot();
    return s;
  }

  return {
    get log() { return logImpl; },
    threshold, ready,
    /// make the shard for this book exist and be resumed. The engine awaits this once per market, before its
    /// first order is matched, so no fill is ever matched into a shard that cannot tell it which batch it is in.
    async prepare(book) {
      if (stopping) return null;
      const s = shardFor(shardOf(book.market, book.outcome));
      s.state.track(book);
      if (!s.seq.ready && !s.booting && s.failedAt && Date.now() - s.failedAt >= retryMs) s.ready = s.boot();   // the log may be back
      try { await s.ready; } catch {}
      return s.seq.ready ? s.shard : null;
    },
    /// the engine's hooks: one matched order, or one cancel, goes into the log. `rec` is book.js's order record,
    /// `fills` are the fills the engine's own book produced and `result` its add result (what rested, and under
    /// which sequence number — the commitment needs it; without it the book is asked). Returns the slot the fills
    /// belong to, or null when the log is not up yet (the caller then settles them as it does today).
    record(book, rec, fills = [], result = undefined) {
      if (stopping) { stats.skipped++; return null; }
      const s = shardFor(shardOf(book.market, book.outcome));
      s.state.applied(book, rec, fills, result);                 // the commitment mirrors the engine's book, sequenced or not
      const op = { t: 'add', hash: rec.hash, market: book.market, outcome: book.outcome, order: serializeL3Order(rec.o), sig: rec.sig, signer: lower(rec.signer || rec.o.user), at: rec.at || Date.now() };
      // the log not resumed yet: the op is already in the engine's book, so it is kept and sequenced — in order —
      // the moment the shard is up (reconcile); its fills settle as today (null slot). An op dropped here used to
      // leave the engine's book and the miners' apart for good.
      if (!s.seq.ready || s.deferred.length) { s.deferred.push({ op, fills }); return null; }
      s.state.ops++;
      stats.records++;
      return s.seq.record(op, fills);
    },
    recordCancel(book, rec, by = 'user', cancelled = undefined) {
      if (stopping) { stats.skipped++; return null; }
      const s = shardFor(shardOf(book.market, book.outcome));
      s.state.cancelled(book, rec, cancelled);
      const op = { t: 'cancel', hash: rec.hash, market: book.market, outcome: book.outcome, user: lower(rec.o.user), by, at: Date.now() };
      if (!s.seq.ready || s.deferred.length) { s.deferred.push({ op, fills: [] }); return null; }
      s.state.ops++;
      stats.records++;
      return s.seq.record(op, []);
    },
    /// force a seal on every shard (used by tests and by a graceful shutdown)
    async flush() { for (const s of shards.values()) await s.seq.flush(); },
    shard(shard) { return shards.get(shard) || null; },
    /// the sorted finality signatures a finalized batch has now (quorum.js finalSigsOf), or null
    finalSigsOf(shard, index) { const s = shards.get(String(shard)); return s ? s.quorum.finalSigsOf(index) : null; },
    status() {
      const out = { log: logImpl ? logImpl.kind : logDown.at ? 'unavailable' : 'starting', logError: logDown.error, logAttempts: logDown.attempts, dir: logImpl?.dir || null, threshold, ...stats, shards: {} };
      for (const [k, s] of shards) out.shards[k] = { sequencer: s.seq.status(), quorum: s.quorum.status(), rewardsEpochClosed: s.closedEpoch, deferred: s.deferred.length, reconcile: s.reconcile };
      return out;
    },
    async stop() {
      stopping = true;
      for (const s of shards.values()) { try { await s.seq.stop(); } catch {} try { s.sub && (await s.sub.close()); } catch {} try { s.state.close(); } catch {} }
      shards.clear();
      if (logImpl) { try { await logImpl.close(); } catch {} }
    },
  };
}
