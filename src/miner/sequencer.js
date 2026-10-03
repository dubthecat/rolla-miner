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
import { fillsRootOf, ordersRootOf, batchRootOf, epochDigestOf, bookHashOf, ZERO32 } from './merkle.js';
import { signDigest } from './verify.js';
import { createShardState } from './miner.js';
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
} = {}) {
  if (!shard) throw new Error('a sequencer needs a shard');
  if (!log) throw new Error('a sequencer needs a log');
  if (!account?.address) throw new Error('a sequencer needs an account');
  const st = state || createShardState({ newBook, logger: logFn });
  if (typeof st.bookHash !== 'function') throw new Error('a sequencer state needs bookHash()');
  const topic = ordersTopic(shard);
  const m = { batches: 0, ops: 0, fills: 0, sealed: 0, failed: 0, retried: 0, sealMs: 0, rootMs: 0, lastSealAt: 0 };
  let index = 0, epoch = 0, prevRoot = ZERO32, seqFrom = 1;
  let open = { ops: [], fills: [] };
  let timer = null, chain = Promise.resolve(), stopped = false, ready = false;
  const retry = [];

  const slotOf = (i) => `${shard}#${i}`;
  /// where an op recorded right now will land. Known before the batch is sealed, which is what lets the engine
  /// stage a fill under its batch the moment it happens.
  const slot = () => slotOf(index);

  function arm() {
    if (timer || stopped) return;
    timer = setTimeout(() => { timer = null; sealSoon(); }, batchMs);
    timer.unref?.();
  }
  const sealSoon = () => { chain = chain.then(() => seal()).catch((e) => { m.failed++; logFn(`[l3seq] seal failed: ${e.message}`); }); return chain; };

  /// record an op the caller has already applied to the shard's book
  function record(op, fills = []) {
    if (stopped) return null;
    open.ops.push(op);
    if (fills.length) open.fills.push(...fills);
    m.ops++; m.fills += fills.length;
    const here = slot();
    if (open.ops.length >= batchMax) sealSoon(); else arm();
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
    if (!open.ops.length && !sealEmpty) return null;
    const t0 = process.hrtime.bigint();
    const ops = open.ops, fills = open.fills;
    open = { ops: [], fills: [] };
    const atEpochEnd = epochBatches > 0 && (index + 1) % epochBatches === 0;
    const t1 = process.hrtime.bigint();
    const ordersRoot = ordersRootOf(ops);
    const fillsRoot = fillsRootOf(fills);
    const bookHash = atEpochEnd ? st.bookHash() : ZERO32;
    m.rootMs += Number(process.hrtime.bigint() - t1) / 1e6;
    const rw = atEpochEnd && rewardsRoot ? (rewardsRoot() || null) : null;
    const batch = {
      shard: String(shard), epoch, index, seqFrom, seqTo: st.seq,
      prevRoot, ordersRoot, fillsRoot, bookHash,
      rewardsRoot: rw?.root || ZERO32, rewardsEpoch: rw && rw.epoch >= 0 ? rw.epoch : -1,
      ops, at: Date.now(), sequencer: account.address, batchRoot: ZERO32, sig: '0x',
    };
    batch.batchRoot = batchRootOf(batch);
    // advance the chain BEFORE awaiting anything: the next batch's prevRoot is this one's root, whatever
    // happens to the append
    const thisIndex = index, thisEpoch = epoch;
    prevRoot = batch.batchRoot; index++; seqFrom = st.seq + 1;
    if (atEpochEnd) epoch++;
    batch.sig = await signDigest(account, batch.batchRoot);
    try {
      await log.append(topic, batch);
      m.sealed++;
    } catch (e) {
      // the log refused the batch. The chain is already advanced, so the batch is kept and retried: a batch
      // that never reaches the log can never finalize, and its fills therefore never settle — which is the
      // correct failure (docs/L3-MINERS.md §7, "Kafka partition").
      retry.push(batch); m.failed++;
      logFn(`[l3seq] ${shard}#${thisIndex} could not be logged (${e.message}); ${retry.length} batch(es) waiting`);
    }
    m.batches++; m.lastSealAt = Date.now(); m.sealMs += Number(process.hrtime.bigint() - t0) / 1e6;
    try { onSealed && onSealed(batch, fills); } catch (e) { logFn(`[l3seq] onSealed failed: ${e.message}`); }
    if (atEpochEnd && onEpoch) {
      const e = { shard: String(shard), epoch: thisEpoch, index: thisIndex, batchRoot: batch.batchRoot, bookHash, fills: m.fills, rewardsRoot: batch.rewardsRoot, rewardsEpoch: batch.rewardsEpoch };
      try { onEpoch({ ...e, digest: epochDigestOf(e) }, batch); } catch (err) { logFn(`[l3seq] onEpoch failed: ${err.message}`); }
    }
    if (retry.length) drain();
    if (open.ops.length) arm();
    return batch;
  }
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
  async function resume() {
    const n = await log.offset(topic);
    if (n > 0) {
      const tail = await log.read(topic, Math.max(0, n - 1), 1);
      const last = tail.length ? tail[tail.length - 1].value : null;
      if (last && String(last.shard) === String(shard)) {
        // an epoch-boundary batch (the one carrying a book commitment) closes its epoch
        index = Number(last.index) + 1; prevRoot = last.batchRoot; seqFrom = Number(last.seqTo) + 1;
        epoch = Number(last.epoch) + (last.bookHash && last.bookHash !== ZERO32 ? 1 : 0);
        logFn(`[l3seq] ${shard}: resuming at index ${index}, epoch ${epoch}, prevRoot ${prevRoot.slice(0, 12)}…`);
      }
    }
    ready = true;
    return { index, epoch, prevRoot };
  }

  return {
    shard: String(shard), topic, state: st, submit, record, seal, resume, slot,
    get index() { return index; }, get epoch() { return epoch; }, get prevRoot() { return prevRoot; },
    get ready() { return ready; }, get open() { return open.ops.length; },
    async flush() { await sealSoon(); return chain; },
    status: () => ({ shard: String(shard), index, epoch, prevRoot, open: open.ops.length, retry: retry.length, ready,
                     batchMs, batchMax, epochBatches, ...m, perBatchMs: m.batches ? Number((m.sealMs / m.batches).toFixed(3)) : 0,
                     state: st.stat ? st.stat() : { seq: st.seq } }),      // the rig's adapter has no books of its own to report
    async stop() { stopped = true; if (timer) { clearTimeout(timer); timer = null; } try { await chain; } catch {} },
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
 * Finality arrives through `onFinal({ slot, index, shard, batch, votes })`, and a fork through `onFork(f)`.
 * One sequencer and one quorum per shard; a shard is one market by default (L3_SHARD_BY=outcome gives a shard
 * per book instead, for a market hot enough to deserve it).
 */
export function createSequencerRig({
  env = process.env, dir = null, account = null, logger: logFn = console.log,
  onFinal = null, onFork = null, onEpoch = null, newBook = null,
} = {}) {
  const acct = account || (env.OPERATOR_KEY ? privateKeyToAccount(env.OPERATOR_KEY.startsWith('0x') ? env.OPERATOR_KEY : '0x' + env.OPERATOR_KEY) : null);
  if (!acct) throw new Error('the L3 sequencer needs OPERATOR_KEY');
  const logDir = dir || path.join(env.L3_DIR || (env.DATA_DIR ? path.join(env.DATA_DIR, 'l3') : '.l3'), 'log');
  const threshold = Number(env.L3_THRESHOLD || 2);
  const allow = (env.L3_MINERS_ALLOW || '').split(',').map((s) => s.trim()).filter(Boolean);
  const byOutcome = env.L3_SHARD_BY === 'outcome';
  const batchMs = Number(env.L3_BATCH_MS || 60), batchMax = Number(env.L3_BATCH_MAX || 500), epochBatches = Number(env.L3_EPOCH_BATCHES || 20);
  const shardOf = (market, outcome) => (byOutcome ? `${Number(market)}-${Number(outcome)}` : String(Number(market)));

  const shards = new Map();     // shard → { seq, quorum, sub, books, state, rewards, closedEpoch }
  let logImpl = null, started = null, stopping = false;
  const stats = { finalized: 0, forks: 0, records: 0, skipped: 0, epochsClosed: 0 };

  async function boot() {
    logImpl = await createLog({ env, dir: logDir, clientId: `rolla-seq-${process.pid}`, logger: logFn });
    logFn(`[l3rig] sequencing into ${logImpl.kind} log${logImpl.dir ? ' at ' + logImpl.dir : ''} · threshold ${threshold}${allow.length ? ` · ${allow.length} allowed miner(s)` : ' · any signed vote counts'}`);
    return logImpl;
  }
  const ready = () => (started || (started = boot().catch((e) => { logFn(`[l3rig] log unavailable: ${e.message}`); started = null; logImpl = null; throw e; })));

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
        onFinal && onFinal({ slot: `${shard}#${f.index}`, shard, index: f.index, epoch: f.epoch, fills: f.fills, batch: f.batch, batchRoot: f.batchRoot, votes: f.votes, miners: f.miners, credit: f.credit });
      },
      onFork: (f) => { stats.forks++; onFork && onFork({ shard, ...f }); },
    });
    // the state the batch commits to is the ENGINE's books, not a shadow of them: the engine did the matching,
    // and a commitment to anything else would be a commitment to a book nobody traded on. `ops` is the shard's
    // op counter, defined exactly as createShardState defines it, so every miner's counter agrees.
    const books = new Set();
    const state = { ops: 0, get seq() { return this.ops; }, bookHash: () => bookHashOf([...books].flatMap((b) => b.m.orders())) };
    const seq = createSequencer({
      shard, state,
      log: { kind: 'deferred', append: async (t, v) => (await ready()).append(t, v), offset: async (t) => (await ready()).offset(t), read: async (t, f, l) => (await ready()).read(t, f, l) },
      account: acct, batchMs, batchMax, epochBatches, logger: logFn, newBook,
      onSealed: (batch) => { quorum.announce(batch); },
      rewardsRoot: () => ({ root: rewards.epochRootOf(s0.closedEpoch), epoch: s0.closedEpoch }),
      onEpoch,
    });
    s = { shard, seq, quorum, sub: null, books, state, rewards, ready: null, get closedEpoch() { return s0.closedEpoch; } };
    shards.set(shard, s);
    // resume the chain from the log — the tail fixes index, epoch and prevRoot — then listen for the miners'
    // votes. The promise is kept so a caller can await the first order of a new market instead of having it
    // settle unsequenced: a batch index cannot be handed out before the log says where the chain is.
    s.ready = (async () => {
      const impl = await ready();
      await seq.resume();
      s.sub = await impl.subscribe(votesTopic(shard), 0, async ({ value }) => { try { await quorum.vote(value); } catch (e) { logFn(`[l3rig] vote rejected: ${e.message}`); } });
      return s;
    })().catch((e) => { logFn(`[l3rig] shard ${shard} not ready: ${e.message}`); return s; });
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
      s.books.add(book);
      try { await s.ready; } catch {}
      return s.seq.ready ? s.shard : null;
    },
    /// the engine's hooks: one matched order, or one cancel, goes into the log. `rec` is book.js's order record
    /// and `fills` are the fills the engine's own book produced. Returns the slot the fills belong to, or null
    /// when the log is not up yet (the caller then settles them as it does today).
    record(book, rec, fills = []) {
      if (stopping) return null;
      const s = shardFor(shardOf(book.market, book.outcome));
      if (!s.seq.ready) { stats.skipped++; return null; }         // before the log is up: settle as today
      s.books.add(book); s.state.ops++;
      stats.records++;
      return s.seq.record({ t: 'add', hash: rec.hash, market: book.market, outcome: book.outcome, order: serializeL3Order(rec.o), sig: rec.sig, signer: lower(rec.signer || rec.o.user), at: rec.at || Date.now() }, fills);
    },
    recordCancel(book, rec, by = 'user') {
      if (stopping) return null;
      const s = shardFor(shardOf(book.market, book.outcome));
      if (!s.seq.ready) { stats.skipped++; return null; }
      s.books.add(book); s.state.ops++;
      stats.records++;
      return s.seq.record({ t: 'cancel', hash: rec.hash, market: book.market, outcome: book.outcome, user: lower(rec.o.user), by, at: Date.now() }, []);
    },
    /// force a seal on every shard (used by tests and by a graceful shutdown)
    async flush() { for (const s of shards.values()) await s.seq.flush(); },
    shard(shard) { return shards.get(shard) || null; },
    status() {
      const out = { log: logImpl ? logImpl.kind : 'starting', dir: logImpl?.dir || null, threshold, ...stats, shards: {} };
      for (const [k, s] of shards) out.shards[k] = { sequencer: s.seq.status(), quorum: s.quorum.status(), rewardsEpochClosed: s.closedEpoch };
      return out;
    },
    async stop() {
      stopping = true;
      for (const s of shards.values()) { try { await s.seq.stop(); } catch {} try { s.sub && (await s.sub.close()); } catch {} }
      shards.clear();
      if (logImpl) { try { await logImpl.close(); } catch {} }
    },
  };
}
