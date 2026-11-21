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

export const CHECKPOINT_VERSION = 1;
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
  let resting = 0;
  for (const [key, book] of state.books) {
    const rows = book.orders();
    if (!rows.length) continue;                       // an empty book restores to nothing
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
    resting, books, at: Date.now(),
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
  for (const [key, rows] of Object.entries(snap.books || {})) {
    const [market, outcome] = String(key).split(':').map(Number);
    if (!Number.isFinite(market) || !Number.isFinite(outcome)) throw new Error(`checkpoint: book key ${key}`);
    const book = state.bookFor(market, outcome);
    if (typeof book.restore !== 'function') throw new Error('checkpoint: this book backend has no restore (the native book does not yet)');
    resting += book.restore(rows.map(orderOf));
  }
  state.restoreSeq(Number(snap.seq) || 0);
  state.seedCommit();
  const got = state.bookHash();
  if (got !== snap.bookHash) throw new Error(`checkpoint: the restored book commits to ${String(got).slice(0, 12)}…, the file says ${String(snap.bookHash).slice(0, 12)}…`);
  if (resting !== Number(snap.resting)) throw new Error(`checkpoint: restored ${resting} resting order(s), the file says ${snap.resting}`);
  return { resting, index: Number(snap.index), epoch: Number(snap.epoch), prevRoot: snap.prevRoot, offset: Number(snap.offset) };
}
