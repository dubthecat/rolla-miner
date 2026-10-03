// engine/l3/miner/commit.js — the incremental state commitment: a Merkle-treap over the resting book.
//
// docs/L3-MINERS.md §6.6 / §9.5a: `bookHashOf` (merkle.js) rebuilds a Merkle root over EVERY resting order
// at each epoch boundary — 48 µs a leaf, 2.4 s for a 50,000-order book, paid by the sequencer and by every
// miner. This tree is maintained alongside the book instead: O(log n) keccaks per insert, fill and cancel,
// the root a read, and an inclusion proof for any resting order. It is the JavaScript reference of
// native/book/commit.hpp — same keys, same priorities, same bytes under every hash — and commit_diff.mjs
// checks the two agree root for root and proof for proof over 100,000 random operations.
//
// ------------------------------------------------------------------ the structure
//
// A treap: a binary search tree over the key (side, price, seq) — side 0 = bid, 1 = ask; price as a 32-byte
// big-endian unsigned; seq the book's sequence number — that is at the same time a max-heap over a priority
// derived from the key alone: priority = the first 8 bytes (big-endian) of keccak256(side ‖ price ‖ seq).
// Both the ordering and the priorities are functions of the KEY SET, so the shape of the tree is too: any
// history of inserts and removes that reaches the same set of resting orders reaches the same tree and the
// same root. A canonical commitment, like the sorted list bookHashOf hashes, but O(log n) to maintain.
//
// A shard holds several books and each numbers its own sequence, so (side, price, seq) is not unique across a
// shard (bookHashOf has the same note): the order hash is the final tiebreaker of the key order, and of a
// priority tie. With unique (side, price, seq) the order is exactly ascending (side, price, seq).
//
// ------------------------------------------------------------------ the hashing
//
//   leaf(n)  = restingLeaf(n)  = keccak256(0x00 ‖ abi.encode(bytes32 hash, address user, bool buy,
//                                                              uint256 price, uint256 remaining, uint256 seq))
//   node(n)  = keccak256(0x03 ‖ leaf(n) ‖ node(n.left) ‖ node(n.right))        32 zero bytes for no child
//   root     = node(top), or 0x00…0 for an empty tree
//
// 0x00 / 0x03 are merkle.js's LEAF / TREAP tags: a 193-byte leaf preimage can never be re-read as a 97-byte
// node, nor either as a 0x01 pair or 0x02 apex of the batch trees. Every node hash is cached on the node, so
// an operation rehashes the path from the touched node to the root and nothing else.
//
// A proof is { hash, leaf, left, right, path: [{ leaf, sibling, dir }, …] } — the order's own leaf and child
// hashes, then for each ancestor from the parent up to the root: that ancestor's leaf, the hash of the
// subtree on the other side, and dir = 0 if the proven subtree was its LEFT child, 1 for RIGHT.
// verifyCommitProof recomputes the root from those alone. encodeCommitProof/decodeCommitProof are the wire
// form the C ABI uses: leaf ‖ left ‖ right ‖ n × (dir ‖ leaf ‖ sibling).
//
// Everything in and out is what merkle.js speaks: 0x hex strings for hashes and addresses, bigint (or a safe
// integer) for price, remaining and seq.
import { keccak256, restingLeafBytes, restingLeaf, tagged, TAG, hex, bytes, ZERO32 } from './merkle.js';

const TREAP = TAG.TREAP;
const Z32 = new Uint8Array(32);
const MAX256 = (1n << 256n) - 1n, MAX64 = (1n << 64n) - 1n;

/// normalise a 0x hex word to lower-case 0x hex of exactly `n` bytes (bytes() validates the digits)
function word(h, n, what) {
  const u = bytes(h);
  if (u.length !== n) throw new Error(`${what}: expected ${n} bytes, got ${u.length}`);
  return hex(u);
}
function uint(v, max, what) {
  const x = typeof v === 'bigint' ? v : BigInt(v);
  if (x < 0n || x > max) throw new Error(`${what} out of range`);
  return x;
}
/// the first 8 bytes of keccak256(side ‖ price ‖ seq), as two uint32 halves (cheaper to compare than a bigint)
export function commitPriority(buy, price, seq) {
  const b = new Uint8Array(41); b[0] = buy ? 0 : 1;
  let p = uint(price, MAX256, 'price'); for (let k = 32; k >= 1; k--) { b[k] = Number(p & 0xffn); p >>= 8n; }
  let s = uint(seq, MAX64, 'seq'); for (let k = 40; k >= 33; k--) { b[k] = Number(s & 0xffn); s >>= 8n; }
  const h = keccak256(b);
  return { hi: ((h[0] << 24) | (h[1] << 16) | (h[2] << 8) | h[3]) >>> 0, lo: ((h[4] << 24) | (h[5] << 16) | (h[6] << 8) | h[7]) >>> 0 };
}
/// key order: side, price, seq, then the hash (lower-case hex compares like the bytes do)
function cmpKey(a, b) {
  if (a.side !== b.side) return a.side - b.side;
  if (a.price !== b.price) return a.price < b.price ? -1 : 1;
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1;
  return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
}
/// heap order: the priority, then the key as the tiebreaker (so a priority tie is still canonical)
function higher(a, b) {
  if (a.pHi !== b.pHi) return a.pHi > b.pHi;
  if (a.pLo !== b.pLo) return a.pLo > b.pLo;
  return cmpKey(a, b) < 0;
}
const nodeHash = (leaf, l, r) => tagged(TREAP, leaf, l ? l.nh : Z32, r ? r.nh : Z32);

export function createCommitTree() {
  const byHash = new Map();      // hash → node
  let root = null, count = 0, keccaks = 0;

  const pull = (n) => { n.nh = nodeHash(n.leaf, n.left, n.right); keccaks++; };
  const ins = (t, n) => {
    if (!t) { pull(n); return n; }
    if (cmpKey(n, t) < 0) {
      t.left = ins(t.left, n);
      if (higher(t.left, t)) { const l = t.left; t.left = l.right; l.right = t; pull(t); t = l; }     // rotate right
    } else {
      t.right = ins(t.right, n);
      if (higher(t.right, t)) { const r = t.right; t.right = r.left; r.left = t; pull(t); t = r; }   // rotate left
    }
    pull(t); return t;
  };
  const merge = (a, b) => {                 // every key of a is below every key of b
    if (!a) return b; if (!b) return a;
    if (higher(a, b)) { a.right = merge(a.right, b); pull(a); return a; }
    b.left = merge(a, b.left); pull(b); return b;
  };
  const del = (t, n) => {
    if (!t) return null;
    if (t === n) return merge(t.left, t.right);
    if (cmpKey(n, t) < 0) t.left = del(t.left, n); else t.right = del(t.right, n);
    pull(t); return t;
  };
  const repull = (t, n) => {                // after n's leaf changed: rehash the path root → n
    if (!t) return;
    if (t !== n) repull(cmpKey(n, t) < 0 ? t.left : t.right, n);
    pull(t);
  };
  const entryOf = (n) => ({ hash: n.hash, user: n.user, buy: n.buy, price: n.price, remaining: n.remaining, seq: n.seq });
  const walk = (t, out) => { if (!t) return; walk(t.left, out); out.push(entryOf(t)); walk(t.right, out); };

  return {
    /// add a resting order { hash, user, buy, price, remaining, seq }; false if an order with this hash is in the tree
    insert(o) {
      const hash = word(o.hash, 32, 'hash');
      if (byHash.has(hash)) return false;
      const n = {
        hash, user: word(o.user, 20, 'user'), buy: !!o.buy, side: o.buy ? 0 : 1,
        price: uint(o.price, MAX256, 'price'), remaining: uint(o.remaining, MAX256, 'remaining'), seq: uint(o.seq, MAX64, 'seq'),
        pHi: 0, pLo: 0, leaf: null, nh: null, left: null, right: null,
      };
      const p = commitPriority(n.buy, n.price, n.seq); n.pHi = p.hi; n.pLo = p.lo;
      n.leaf = restingLeafBytes(n); keccaks += 2;
      root = ins(root, n);
      byHash.set(hash, n); count++;
      return true;
    },
    /// drop a resting order (a cancel, or a fill that emptied it); false if unknown
    remove(hash) {
      const n = byHash.get(word(hash, 32, 'hash'));
      if (!n) return false;
      root = del(root, n);
      byHash.delete(n.hash); count--;
      return true;
    },
    /// a partial fill: the order stays, its leaf changes; false if unknown
    setRemaining(hash, remaining) {
      const n = byHash.get(word(hash, 32, 'hash'));
      if (!n) return false;
      n.remaining = uint(remaining, MAX256, 'remaining');
      n.leaf = restingLeafBytes(n); keccaks++;
      repull(root, n);
      return true;
    },
    /// the commitment: O(1)
    root() { return root ? hex(root.nh) : ZERO32; },
    get size() { return count; },
    get keccaks() { return keccaks; },
    has(hash) { return byHash.has(word(hash, 32, 'hash')); },
    get(hash) { const n = byHash.get(word(hash, 32, 'hash')); return n ? entryOf(n) : null; },
    /// every resting order in key order (side, price, seq, hash): the canonical list the root commits to
    entries() { const out = []; walk(root, out); return out; },
    /// the inclusion proof of one resting order against root(), or null if unknown
    proof(hash) {
      const n = byHash.get(word(hash, 32, 'hash'));
      if (!n) return null;
      const down = []; let t = root;
      while (t !== n) {
        if (!t) return null;                                   // cannot happen: n is in the tree
        if (cmpKey(n, t) < 0) { down.push({ leaf: hex(t.leaf), sibling: t.right ? hex(t.right.nh) : ZERO32, dir: 0 }); t = t.left; }
        else { down.push({ leaf: hex(t.leaf), sibling: t.left ? hex(t.left.nh) : ZERO32, dir: 1 }); t = t.right; }
      }
      return { hash: n.hash, leaf: hex(n.leaf), left: n.left ? hex(n.left.nh) : ZERO32, right: n.right ? hex(n.right.nh) : ZERO32, path: down.reverse() };
    },
  };
}

/// verify a proof against a root alone. With `order` given, the proof's leaf must also be restingLeaf(order) —
/// i.e. the proof is about THAT order with THAT remaining, not merely about some leaf.
export function verifyCommitProof(proof, root, order) {
  if (!proof || typeof proof !== 'object' || !Array.isArray(proof.path)) return false;
  try {
    const w = (h) => { const u = bytes(h); if (u.length !== 32) throw new Error('not a word'); return u; };
    let h = tagged(TREAP, w(proof.leaf), w(proof.left), w(proof.right));
    for (const s of proof.path) {
      if (!s || (s.dir !== 0 && s.dir !== 1)) return false;
      h = s.dir === 0 ? tagged(TREAP, w(s.leaf), h, w(s.sibling)) : tagged(TREAP, w(s.leaf), w(s.sibling), h);
    }
    if (hex(h) !== root) return false;
    return order ? restingLeaf(order) === proof.leaf : true;
  } catch { return false; }
}

/// the wire form (the C ABI's): leaf ‖ left ‖ right ‖ n × (dir ‖ leaf ‖ sibling)
export function encodeCommitProof(proof) {
  const out = new Uint8Array(96 + 65 * proof.path.length);
  out.set(bytes(proof.leaf), 0); out.set(bytes(proof.left), 32); out.set(bytes(proof.right), 64);
  let o = 96;
  for (const s of proof.path) { out[o] = s.dir; out.set(bytes(s.leaf), o + 1); out.set(bytes(s.sibling), o + 33); o += 65; }
  return out;
}
export function decodeCommitProof(u, hash) {
  if (!(u instanceof Uint8Array) || u.length < 96 || (u.length - 96) % 65) throw new Error(`commit proof: bad length ${u?.length}`);
  const path = [];
  for (let o = 96; o < u.length; o += 65) path.push({ dir: u[o], leaf: hex(u.subarray(o + 1, o + 33)), sibling: hex(u.subarray(o + 33, o + 65)) });
  return { hash: hash ?? null, leaf: hex(u.subarray(0, 32)), left: hex(u.subarray(32, 64)), right: hex(u.subarray(64, 96)), path };
}
