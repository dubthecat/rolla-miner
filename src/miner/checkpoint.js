// engine/l3/miner/checkpoint.js — a shard's book, written down, so a miner does not have to earn it again.
//
// A miner catching up from the log re-verifies every signature in it (`handleBatch` → `verifyOps`). On one live
// shard that is 19,098 orders; across the 73 this fleet follows it is about **6.9 minutes of pure secp256k1 per
// restart** at 3,350 orders/s per core, and it grows with the log. At a million orders a shard it is hours, which
// is the plain reason the system could not "take millions of orders" however many cores were thrown at it: every
// restart, and every NEW miner, starts by re-verifying history.
//
// A checkpoint is the book as of a known batch. Restoring it is O(resting) — 12 ms for 800 resting orders — and
// then the miner subscribes from the offset after it. Nothing historical is verified again, because nothing
// historical is replayed.
//
// What makes this safe to trust is that it is SELF-CHECKING: the file carries the `bookHash` the restored book
// must produce, and the commitment is a function of the resting set including every order's sequence number
// (restingLeaf is keyed on side/price/seq, which is why matcher.restore preserves seq rather than reassigning
// it). A truncated file, a hand-edited one, or one written under a different `L3_COMMIT` mode fails that check
// and is refused — and the fallback, replaying the log, is always available and always correct.
//
// It is a performance optimisation, not a trust assumption. A miner that somehow loaded a wrong book would
// dissent and be outvoted; it cannot make the venue agree with it. That is the quorum's job and it still does it.
import fs from 'node:fs';
import path from 'node:path';
import { writeState, readState } from '../state-file.js';
import { ZERO32 } from './merkle.js';
import { recoverDigestSigner } from './verify.js';

export const CHECKPOINT_VERSION = 2;   // 2 carries each book's seq COUNTER; a v1 file restores it too low
export const fileOf = (dir, shard) => path.join(dir, `${shard}.checkpoint.json`);

/// bigints and the matcher's node fields do not belong in JSON; this is the wire form of one resting order
const rowOf = (o) => ({ h: o.hash, u: o.user, b: !!o.buy, p: String(o.price), s: String(o.size), r: String(o.remaining), po: o.postOnly ? 1 : undefined, io: o.ioc ? 1 : undefined, t: o.ts || undefined, q: o.seq });
const orderOf = (r) => ({ hash: r.h, user: r.u, buy: !!r.b, price: BigInt(r.p), size: BigInt(r.s), remaining: BigInt(r.r), postOnly: !!r.po, ioc: !!r.io, ts: Number(r.t) || 0, seq: Number(r.q) });

/**
 * snapshot(state, { shard, index, epoch, prevRoot, offset, bookHash })
 * The caller passes the batch co-ordinates it is consistent with — a checkpoint is only meaningful at a batch
 * boundary, after the batch has been applied in full.
 */
export function snapshot(state, { shard, index, epoch, prevRoot, offset, bookHash = null }) {
  const books = {};
  const counters = {};
  let resting = 0;
  for (const [key, book] of state.books) {
    // EVERY book's counter, resting or not. A book with nothing left in it has still spent every sequence number
    // its adds, fills and cancels consumed, and the next order must carry on from there or the commitment moves.
    if (typeof book.seq === 'number') counters[key] = book.seq;
    const rows = book.orders();
    if (!rows.length) continue;                       // nothing resting: the counter above is all there is to keep
    books[key] = rows.map(rowOf);
    resting += rows.length;
  }
  return {
    v: CHECKPOINT_VERSION, shard: String(shard), index: Number(index), epoch: Number(epoch),
    prevRoot, offset: Number(offset), seq: Number(state.stat().seq) || 0,
    commit: state.commit ? state.commit.mode : null,
    // the root the restored book has to reproduce. Taken from the caller when it already has it for this batch,
    // so the file commits to the same word the miner voted on rather than one computed a moment later.
    bookHash: bookHash || state.bookHash(),
    resting, books, counters, at: Date.now(),
  };
}

/// write it atomically (lib/state-file.js: temp, fsync, rename, one .bak behind)
export function write(dir, shard, snap) {
  fs.mkdirSync(dir, { recursive: true });
  writeState(fileOf(dir, shard), snap);
  return snap;
}

/// read it back, or null when there is none. A file that is not ours, or not this version, is not an error: the
/// caller replays the log, which is what it would have done anyway.
export function read(dir, shard) {
  // readState answers { data, from } and falls back to the .bak copy, which is exactly what we want here: a
  // checkpoint truncated mid-write is replaced by the one before it rather than dropping us back to the log.
  const { data: snap } = readState(fileOf(dir, shard), null);
  if (!snap || typeof snap !== 'object') return null;
  if (Number(snap.v) !== CHECKPOINT_VERSION) return null;
  if (String(snap.shard) !== String(shard)) return null;
  if (!Number.isInteger(Number(snap.index)) || Number(snap.index) < 0) return null;
  if (!Number.isInteger(Number(snap.offset)) || Number(snap.offset) < 0) return null;
  if (typeof snap.bookHash !== 'string') return null;
  return snap;
}

export function remove(dir, shard) {
  for (const f of [fileOf(dir, shard), fileOf(dir, shard) + '.bak']) { try { fs.unlinkSync(f); } catch { } }
}

/**
 * restore(state, snap, { commitMode }) → { resting, index, epoch, prevRoot, offset }
 * Throws when the snapshot does not reproduce its own bookHash, which is the whole point of writing it down.
 */
export function restore(state, snap, { commitMode = null } = {}) {
  if (!snap) throw new Error('checkpoint: nothing to restore');
  // A tree root and a bookhash root are different words for the same book (commit-state.js), so a checkpoint
  // written under one mode cannot be checked under the other. Refuse rather than fail the hash check with a
  // misleading message — this is exactly what a cluster-wide L3_COMMIT change looks like from here.
  if (commitMode && snap.commit && snap.commit !== commitMode) throw new Error(`checkpoint: written under L3_COMMIT=${snap.commit}, this miner runs ${commitMode}`);
  let resting = 0;
  const counters = snap.counters || {};
  // the union: a book may appear only in `counters` (nothing resting, a counter worth keeping) or only in
  // `books` (a v1 file, which has no counters at all — then the matcher infers, as it used to)
  const keys = new Set([...Object.keys(snap.books || {}), ...Object.keys(counters)]);
  for (const key of keys) {
    const rows = (snap.books || {})[key] || [];
    const [market, outcome] = String(key).split(':').map(Number);
    if (!Number.isFinite(market) || !Number.isFinite(outcome)) throw new Error(`checkpoint: book key ${key}`);
    const book = state.bookFor(market, outcome);
    if (typeof book.restore !== 'function') throw new Error('checkpoint: this book backend has no restore (the native book does not yet)');
    const counter = Object.prototype.hasOwnProperty.call(counters, key) ? Number(counters[key]) : null;
    resting += book.restore(rows.map(orderOf), { seq: counter });
  }
  state.restoreSeq(Number(snap.seq) || 0);
  state.seedCommit();
  const got = state.bookHash();
  if (got !== snap.bookHash) throw new Error(`checkpoint: the restored book commits to ${String(got).slice(0, 12)}…, the file says ${String(snap.bookHash).slice(0, 12)}…`);
  if (resting !== Number(snap.resting)) throw new Error(`checkpoint: restored ${resting} resting order(s), the file says ${snap.resting}`);
  return { resting, index: Number(snap.index), epoch: Number(snap.epoch), prevRoot: snap.prevRoot, offset: Number(snap.offset) };
}

/**
 * snapshotOfBooks(books, { … }) — the same file, built from the ENGINE's books rather than a miner's shard state.
 * The engine wraps each matcher as `.m` on a `{ market, outcome, token, m, trades }` record and keeps the shard's
 * op counter on the sequencer's state, so the pieces arrive separately; the format is identical, which is the
 * point — a miner cannot tell (and must not care) whose book it restored.
 */
export function snapshotOfBooks(books, { shard, index, epoch, prevRoot, offset, seq, bookHash, commit = null }) {
  const out = {};
  const counters = {};
  let resting = 0;
  for (const b of books) {
    const key = `${Number(b.market)}:${Number(b.outcome)}`;
    if (typeof b.m.seq === 'number') counters[key] = b.m.seq;
    const rows = b.m.orders();
    if (!rows.length) continue;
    out[key] = rows.map(rowOf);
    resting += rows.length;
  }
  return {
    v: CHECKPOINT_VERSION, shard: String(shard), index: Number(index), epoch: Number(epoch),
    prevRoot, offset: Number(offset), seq: Number(seq) || 0, commit,
    bookHash, resting, books: out, counters, at: Date.now(),
  };
}

/**
 * verifyAgainstLog(log, shard, snap, { threshold }) → { ok, why, votes, batchRoot }
 *
 * What makes a checkpoint from SOMEONE ELSE safe to start from. A miner's own file is self-checking — it carries
 * the bookHash the restored book must reproduce — but that only proves the file is intact, not that the book is
 * the one the venue agreed on. A file handed over by the engine needs more than the engine's word for it.
 *
 * It does not need a new signature, because the two that matter already exist in the log:
 *
 *   the SEQUENCER signed the batch at `index`, and that batch carries the bookHash of the book as of itself;
 *   the MINERS signed votes for that batch, and `threshold` of them agreeing on its root is what finality IS.
 *
 * So: read the batch, check the checkpoint commits to the same book the batch does, and count the agreeing votes.
 * A checkpoint that passes is a book the venue finalised — and one that fails is simply not used, which costs a
 * log replay and nothing else. The engine is never trusted here; the log is.
 *
 * BOOTSTRAP. There is one state where counting votes cannot work, and the live venue was in it: a shard whose log
 * has been trimmed at the front by retention cannot be replayed into the engine's book at all (the per-book
 * sequence counters are gone with the prefix), so every miner dissents at every epoch boundary that has anything
 * resting, no batch ever reaches the threshold, and the checkpoint that would fix it is refused for want of the
 * votes that it is the cure for. On 2026-11-22 that was six of 46 live shards, halted with finalIndex -1.
 *
 * `bootstrap` breaks it with the other signature in the log rather than with trust in the engine: the SEQUENCER
 * signed `batchRoot`, and batchRoot commits to the bookHash this checkpoint has to match. Accepting that is
 * strictly less than the miner already does — it applies every batch that signature covers — and it is not a
 * substitute for the quorum, which starts counting again at the very next batch. It is allowed only when the
 * shard has no finality to lose (`finalIndex < 0`) and only against an explicit sequencer allowlist.
 */
/**
 * locateBatch(log, topic, index) → { batch, offset } | null
 *
 * Where a batch actually SITS, rather than where it ought to. Everything here used to assume that a batch's index
 * and its log offset are the same number — one append per batch from offset zero, which is true of a shard that
 * has only ever been sequenced once. The live venue is not that: on shard 19085 batch 24,479 sits at offset
 * ~25,007, a drift of 528 from a restart that re-appended, and the checkpoint written for it recorded 24,479 as
 * its offset because the sequencer had nothing better to hand. A fixed eight-row window at the wrong place then
 * contains none of the batch it is looking for, so EVERY checkpoint was refused with "the log has no batch N" —
 * a true statement about where it looked, and false about the log.
 *
 * Indices are monotonic in a single-sequencer log, so the offset can be found by bisection: ~15 reads for 25,000
 * batches, against a replay of the whole shard. The obvious place is tried first, because it is usually right.
 */
export async function locateBatch(log, topic, index, { head = null } = {}) {
  const want = Number(index);
  const indexAt = (rows) => { const r = rows.find((x) => x.value && Number(x.value.index) === want); return r ? { batch: r.value, offset: Number(r.offset) } : null; };
  let hi;
  try { hi = head == null ? Number(await log.offset(topic)) : Number(head); } catch { return null; }
  if (!Number.isFinite(hi) || hi <= 0) return null;
  // 1. where it would be if nothing had ever drifted
  try {
    const direct = await log.read(topic, Math.max(0, Math.min(want, hi - 1)), 8);
    const got = indexAt(direct);
    if (got) return got;
  } catch { }
  // 2. bisect on the index carried by whatever sits at each offset
  let lo = 0, steps = 0;
  while (lo <= hi && steps++ < 48) {
    const mid = Math.floor((lo + hi) / 2);
    let rows = [];
    try { rows = await log.read(topic, mid, 4); } catch { return null; }
    if (!rows.length) { hi = mid - 1; continue; }          // past the end, or a gap
    const got = indexAt(rows);
    if (got) return got;
    const here = Number(rows[0].value && rows[0].value.index);
    if (!Number.isFinite(here)) return null;
    if (here < want) lo = mid + 1; else hi = mid - 1;
  }
  return null;
}

export async function verifyAgainstLog(log, shard, snap, { threshold = 2, ordersTopic, votesTopic, sequencers = null, bootstrap = false } = {}) {
  if (!snap) return { ok: false, why: 'nothing to verify' };
  const index = Number(snap.index);
  if (!Number.isInteger(index) || index < 0) return { ok: false, why: `index ${snap.index}` };
  // Offsets and indices coincide for a single-sequencer shard — one append per batch (sequencer.js resume()) —
  // but the batch is matched by its own `index`, not by where it sat, so a log that ever differs is caught here
  // rather than trusted.
  let found = null;
  try { found = await locateBatch(log, ordersTopic(shard), index); } catch (e) { return { ok: false, why: `cannot read the log: ${e.message}` }; }
  const batch = found && String(found.batch.shard) === String(shard) ? found.batch : null;
  if (!batch) return { ok: false, why: `the log has no batch ${index} for shard ${shard}` };
  if (String(batch.bookHash || '') !== String(snap.bookHash || '')) {
    return { ok: false, why: `batch ${index} commits to ${String(batch.bookHash).slice(0, 12)}…, the checkpoint says ${String(snap.bookHash).slice(0, 12)}…` };
  }
  if (!batch.bookHash || batch.bookHash === ZERO32) return { ok: false, why: `batch ${index} carries no book commitment (not an epoch boundary)` };
  // and the quorum's own answer about that batch
  let votes = [];
  try { votes = await log.read(votesTopic(shard), 0, 100000); } catch (e) { return { ok: false, why: `cannot read the votes: ${e.message}` }; }
  const agreed = new Set();
  for (const { value: v } of votes) {
    if (!v || Number(v.index) !== index) continue;
    if (v.ok === false) continue;
    if (String(v.batchRoot) !== String(batch.batchRoot)) continue;
    if (v.miner) agreed.add(String(v.miner).toLowerCase());
  }
  // the offset the batch ACTUALLY sits at, so a restoring miner resumes from the right place rather than from
  // whatever the writer guessed (see locateBatch)
  const at = found.offset;
  if (agreed.size >= threshold) return { ok: true, why: null, votes: agreed.size, batchRoot: batch.batchRoot, offset: at };
  if (bootstrap) {
    // the sequencer's own signature over the root that commits to this book
    const allow = new Set([...(sequencers || [])].map((a) => String(a).toLowerCase()));
    if (!allow.size) return { ok: false, why: `batch ${index} has ${agreed.size} agreeing vote(s) and bootstrap has no sequencer allowlist to check against`, votes: agreed.size };
    const who = batch.sig ? recoverDigestSigner(batch.batchRoot, batch.sig) : null;
    if (who && allow.has(String(who).toLowerCase())) {
      return { ok: true, why: `bootstrap: ${agreed.size} agreeing vote(s), below the threshold of ${threshold}, accepted on the sequencer's signature over batch ${index}`, votes: agreed.size, batchRoot: batch.batchRoot, bootstrap: true, sequencer: who, offset: at };
    }
    return { ok: false, why: `batch ${index} has ${agreed.size} agreeing vote(s) and is not signed by a known sequencer (${who || 'no signature'})`, votes: agreed.size };
  }
  return { ok: false, why: `batch ${index} has ${agreed.size} agreeing vote(s), threshold is ${threshold}`, votes: agreed.size };
}
