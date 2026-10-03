// engine/l3/miner/commit-state.js — the epoch book commitment behind one interface, two implementations.
//
// Every batch that closes an epoch carries `bookHash`, a commitment to every resting order in the shard, and the
// sequencer and every miner compute it (docs/L3-MINERS.md §6.6). How it is computed is `L3_COMMIT`:
//
//   bookhash   (default) merkle.js's bookHashOf: a Merkle root rebuilt over EVERY resting order at the boundary —
//              48 µs a leaf, 2.4 s for a 50,000-order book, paid by the sequencer and by every miner, and on
//              RunPod it was the whole finality lag. The events below are no-ops; root() walks the books.
//   tree       the incremental state commitment of L3-NATIVE-BOOK.md §9: a Merkle-treap maintained alongside the
//              book from the book's own events — an order that RESTS is inserted, a partial maker fill rewrites
//              one leaf, a maker filled out or a cancel that hits is removed — O(log n) keccaks each, and the
//              boundary is a root READ. In-process (commit.js) by default; NATIVE (native/book/commit.hpp
//              behind bookd, native.js's createNativeCommit) with L3_NATIVE=1 or L3_COMMIT_NATIVE=1, falling
//              back to the JS tree if the binary is not built.
//
// The two modes produce DIFFERENT words for the same book (a treap root is not a sorted-list root), so the mode
// is an epoch-boundary protocol: the sequencer and every miner of a shard switch together.
//
// ------------------------------------------------------------------ the events
//
//   rested(e)              e: { hash, user, buy, price, remaining, seq } — the fields of restingLeaf
//   filled(f)              a fill record with `makerRemaining` (matcher.js and native.js both carry it): 0n → the
//                          maker left the book, else its leaf changes
//   cancelled(hash)        a cancel that hit
//   applied(r, o)          all of the above for one add result r = { fills, rested, remaining, seq } of order o
//   seed(orders)           a book's resting orders as they are now (the engine's rig, whose books were rebuilt by
//                          replay before any of them was sequenced)
//
// Events are BUFFERED and coalesced per order hash until flush() — which root(), proof() and size() do first — or
// until `flushAt` of them are waiting: the tree is a function of the resting SET, so an order that rests and is
// cancelled within the same batch costs nothing, a maker filled three times in a batch is one leaf rewrite, and
// the native tree sees one request per batch instead of one per event (the round trip is what the process
// boundary costs). A combination the buffer cannot express (a hash removed and inserted again in one batch) is
// flushed through in order, so the result is always what applying the events one by one would give.
import { bookHashOf, merkleRoot, ZERO32 } from './merkle.js';
import { createCommitTree } from './commit.js';
import { createNativeCommit } from '../native.js';      // imports node built-ins only; spawns nothing until called

export const COMMIT_MODES = ['bookhash', 'tree'];
const lower = (h) => String(h).toLowerCase();
/// what an EMPTY book commits to, in both modes: bookHashOf([]) = keccak256(0x02 ‖ uint32(0) ‖ 0^32). A treap with
/// no nodes has the zero root by commit.hpp's definition, and the zero word is the protocol's "this batch carries
/// no commitment" (the sequencer's resume(), the miner, the quorum all test `bookHash !== ZERO32`), so an epoch
/// boundary over an empty book must never produce it — the shard would silently lose its epoch end.
export const EMPTY_BOOK_HASH = merkleRoot([]);

/// what the environment asks for
export function commitOptionsFromEnv(env = process.env) {
  const mode = String(env.L3_COMMIT || 'bookhash').toLowerCase();
  if (!COMMIT_MODES.includes(mode)) throw new Error(`L3_COMMIT=${env.L3_COMMIT}: expected one of ${COMMIT_MODES.join(', ')}`);
  return { mode, native: env.L3_NATIVE === '1' || env.L3_COMMIT_NATIVE === '1', flushAt: Number(env.L3_COMMIT_FLUSH || 4000) };
}

/**
 * createCommitState({ mode?, native?, resting, env?, logger?, dir?, flushAt? })
 *   mode      'bookhash' | 'tree' (default from env.L3_COMMIT)
 *   native    tree mode only: the bookd-served tree (default L3_NATIVE=1 || L3_COMMIT_NATIVE=1)
 *   resting   () => every resting order of the shard — what bookhash mode hashes; unused by tree mode
 *   dir       where the native client's FIFOs live (a temp dir by default)
 */
export function createCommitState({ mode = null, native = null, resting = null, env = process.env, logger = null, dir = null, flushAt = null } = {}) {
  const fromEnv = commitOptionsFromEnv(env);
  mode = mode || fromEnv.mode; native = native == null ? fromEnv.native : !!native; flushAt = flushAt || fromEnv.flushAt;
  if (!COMMIT_MODES.includes(mode)) throw new Error(`commit mode ${mode}: expected one of ${COMMIT_MODES.join(', ')}`);
  if (mode === 'bookhash' && typeof resting !== 'function') throw new Error('bookhash commitment needs resting()');
  const log = logger || (() => {});
  const tree = mode === 'tree';

  let impl = 'bookhash', js = null, nat = null;
  if (tree) {
    if (native) {
      try { nat = createNativeCommit({ dir, log, flushAt }); impl = 'native'; }
      catch (e) { log(`[l3commit] native commit tree unavailable (${e.message}); using the in-process tree`); }
    }
    if (!nat) { js = createCommitTree(); impl = 'js'; }
  }

  const st = { ops: 0, applied: 0, refused: 0, conflicts: 0, flushes: 0, roots: 0, ms: 0, size: -1 };
  const pending = new Map();                  // hash → { k: 'i' | 's' | 'r', e }
  const now = () => Number(process.hrtime.bigint()) / 1e6;
  let warned = 0;

  /// apply the buffer to the tree: one request for the native one, one call per entry for the JS one
  function flush() {
    if (!tree) return { applied: 0, refused: 0 };
    const t0 = now();
    let applied = 0, refused = 0;
    if (nat) {
      for (const [hash, p] of pending) { if (p.k === 'i') nat.insert(p.e); else if (p.k === 's') nat.setRemaining(hash, p.e.remaining); else nat.remove(hash); }
      pending.clear();
      const r = nat.flush(); applied = r.applied; refused = r.refused; st.size = r.size;
    } else {
      for (const [hash, p] of pending) {
        const ok = p.k === 'i' ? js.insert(p.e) : p.k === 's' ? js.setRemaining(hash, p.e.remaining) : js.remove(hash);
        if (ok) applied++; else refused++;
      }
      pending.clear(); st.size = js.size;
    }
    st.applied += applied; st.refused += refused; st.flushes++; st.ms += now() - t0;
    if (refused && warned++ < 3) log(`[l3commit] ${refused} commitment update(s) refused by the tree: the book and the commitment disagree (${impl})`);
    return { applied, refused };
  }
  function push(hash, k, e) {
    st.ops++;
    const p = pending.get(hash);
    if (p) {
      if (k === 's' && (p.k === 'i' || p.k === 's')) { p.e.remaining = e.remaining; return; }     // the later remaining wins
      if (k === 'r' && p.k === 'i') { pending.delete(hash); return; }                               // rested and gone within the batch
      if (k === 'r' && p.k === 's') { p.k = 'r'; p.e = null; return; }
      st.conflicts++; flush();                                                                      // insert after remove, or a double: in order, through the tree
    }
    pending.set(hash, { k, e });
    if (pending.size >= flushAt) flush();
  }
  const entry = (e) => {
    if (!(Number.isSafeInteger(e.seq) && e.seq > 0) && typeof e.seq !== 'bigint') throw new Error(`commit-state: a resting order without a sequence number (${e.hash})`);
    return { hash: lower(e.hash), user: lower(e.user), buy: !!e.buy, price: BigInt(e.price), remaining: BigInt(e.remaining), seq: e.seq };
  };

  const api = {
    mode, impl, get native() { return impl === 'native'; }, get tree() { return tree; },
    rested(e) { if (tree) push(lower(e.hash), 'i', entry(e)); },
    filled(f) {
      if (!tree) return;
      if (f.makerRemaining === undefined) throw new Error('commit-state: a fill without makerRemaining — the book must report what is left of the maker');
      const rem = BigInt(f.makerRemaining);
      if (rem === 0n) push(lower(f.makerHash), 'r', null); else push(lower(f.makerHash), 's', { remaining: rem });
    },
    cancelled(hash) { if (tree) push(lower(hash), 'r', null); },
    /// one applied add: the makers it hit, then the taker if it rests
    applied(r, o) {
      if (!tree || !r) return;
      for (const f of r.fills || []) api.filled(f);
      if (r.rested) api.rested({ hash: o.hash, user: o.user, buy: o.buy, price: o.price, remaining: r.remaining, seq: r.seq });
    },
    seed(orders) { if (tree) for (const o of orders) api.rested(o); },
    flush,
    /// the commitment. bookhash: O(resting). tree: a read (after the buffer is flushed).
    root() {
      const t0 = now(); st.roots++;
      let h;
      if (!tree) h = bookHashOf(resting());
      else if (nat) { if (pending.size) flush(); h = nat.root(); }
      else { if (pending.size) flush(); h = js.root(); }
      if (h === ZERO32) h = EMPTY_BOOK_HASH;        // the empty tree: never the "no commitment" word
      st.ms += now() - t0;
      return h;
    },
    /// the inclusion proof of one resting order against root() (commit.js's shape), tree mode only
    proof(hash) { if (!tree) return null; if (pending.size) flush(); return nat ? nat.proof(hash) : js.proof(hash); },
    size() { if (!tree) return resting().length; if (pending.size) flush(); return nat ? nat.size() : js.size; },
    get pending() { return pending.size; },
    stats() { return { mode, impl, ...st, pending: pending.size, size: tree ? (js ? js.size : st.size) : -1 }; },
    close() { if (nat) { try { nat.close(); } catch {} nat = null; } pending.clear(); },
  };
  return api;
}

export { ZERO32 };
