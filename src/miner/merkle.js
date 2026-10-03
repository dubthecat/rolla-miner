// engine/l3/miner/merkle.js — keccak256 Merkle trees over an L3 batch, and the hashes a vote is about.
//
// What the L3 miner log needs that the native book cannot give it (L3-NATIVE-BOOK.md §8): the book's
// `book_state_hash()` is a running CHAIN over the request transcript, so it proves "these requests, in this
// order" in one word but cannot prove one fill against it. A challenge needs a TREE. This module is that tree,
// plus the two commitments a batch carries and the two digests that are signed.
//
// ------------------------------------------------------------------ the tree
//
//   leaf  = keccak256(0x00 ‖ payload)
//   node  = keccak256(0x01 ‖ left ‖ right)       an odd node is PROMOTED unchanged, never duplicated
//   root  = keccak256(0x02 ‖ uint32be(count) ‖ apex)
//
// Three deliberate choices, each closing a real hole:
//   1. the 0x00/0x01/0x02 prefixes domain-separate leaves from internal nodes from the root, so a leaf can
//      never be re-read as a node (the classic second-preimage attack on naive Merkle trees);
//   2. promoting the odd node instead of duplicating it — Bitcoin's duplicate makes two different leaf lists
//      hash to the same root;
//   3. binding the leaf COUNT into the root, because promotion alone would make a one-leaf tree's root equal
//      to its leaf and would let a proof be replayed at a different level. verifyProof() walks the layer sizes
//      from `count`, so it knows exactly where a promotion happened and must consume the whole path.
//
// A proof is { leaf, index, count, path } and is verified against a root alone — no tree, no leaf list.
//
// ------------------------------------------------------------------ the leaves
//
// Both payloads are `abi.encode(...)` of static types, so a Solidity verifier (the RollaBookL3 of
// docs/L3-MINERS.md §4) checks a proof with abi.encode + keccak256 and nothing else. They are hand-packed into
// 32-byte words here rather than built with viem's encodeAbiParameters, because for static types the two are
// byte-identical (miner.test.mjs pins that against viem) and hand-packing is 4× faster — 27 µs against 109 µs
// per leaf on the bench machine, which matters at ten batches a second.
//
//   fill leaf:   abi.encode(uint256 seq, bytes32 makerHash, bytes32 takerHash, address maker, address taker,
//                           uint256 price, uint256 size, bool takerBuys)
//   order leaf:  abi.encode(uint8 kind, bytes32 orderHash, address user, address signer, bool buy,
//                           uint256 price, uint256 size, uint8 flags, bytes32 sigHash)
//                kind 1 = add, 2 = cancel · flags = postOnly | ioc<<1 · sigHash = keccak256(signature)
//
// Everything in and out is a 0x-prefixed lower-case hex string, because that is what viem, the journal, the
// JSON on the wire and Solidity all speak.
import { keccak_256 } from '@noble/hashes/sha3';

export const ZERO32 = '0x' + '00'.repeat(32);
const LEAF = 0x00, NODE = 0x01, ROOT = 0x02;
export const TAG = { LEAF, NODE, ROOT };

// Hex conversion is on the hot path twice per field of every leaf, so both directions are table-driven: a
// 256-entry byte→"xx" table, and a 128-entry char→nibble table that validates while it parses. The obvious
// versions (`parseInt(s.substr(i*2,2),16)` behind a `/[^0-9a-f]/` regex) cost more than the keccak they feed —
// the same lesson the sequencer's seal time taught about walking the tree in hex.
const BYTE_HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'));
const NIB = new Int8Array(128).fill(-1);
for (let c = 48; c <= 57; c++) NIB[c] = c - 48;          // 0-9
for (let c = 97; c <= 102; c++) NIB[c] = c - 87;         // a-f
for (let c = 65; c <= 70; c++) NIB[c] = c - 55;          // A-F
/// bytes → 0x hex, without going through Buffer
export function hex(b) {
  let s = '0x';
  for (let i = 0; i < b.length; i++) s += BYTE_HEX[b[i]];
  return s;
}
/// 0x hex → bytes. Throws on anything that is not an even-length hex string, because a silently truncated
/// hash would produce a root nobody can reproduce.
export function bytes(h) {
  const s = typeof h === 'string' ? ((h.charCodeAt(0) === 48 && (h.charCodeAt(1) | 32) === 120) ? h.slice(2) : h) : String(h ?? '');
  const n = s.length;
  if (n & 1) throw new Error(`not hex: ${String(h).slice(0, 20)}`);
  const out = new Uint8Array(n >> 1);
  for (let i = 0, j = 0; i < n; i += 2) {
    const hi = NIB[s.charCodeAt(i) & 127], lo = NIB[s.charCodeAt(i + 1) & 127];
    if (hi < 0 || lo < 0) throw new Error(`not hex: ${String(h).slice(0, 20)}`);
    out[j++] = (hi << 4) | lo;
  }
  return out;
}
export const keccak = (b) => hex(keccak_256(b));

// ------------------------------------------------------------------ abi.encode of static types, by hand
/// a growable 32-byte-word writer. `word(bytes)` left-pads like abi.encode does for every static type.
class Words {
  constructor(n = 8) { this.b = new Uint8Array(n * 32); this.i = 0; }
  need(n) { if (this.i + n > this.b.length) { const b = new Uint8Array(Math.max(this.b.length * 2, this.i + n)); b.set(this.b.subarray(0, this.i)); this.b = b; } }
  /// a right-aligned value of `len` bytes in a 32-byte word (uint*, address, bool, bytesN<32 are LEFT-aligned —
  /// see bytes32() — so this is the uint/address/bool case)
  uint(v, len = 32) {
    this.need(32); const off = this.i + 32 - len;
    if (typeof v === 'number' && Number.isSafeInteger(v) && v >= 0) {            // seq, epoch, index, flags
      let x = v;
      for (let k = len - 1; k >= 0 && x > 0; k--) { this.b[off + k] = x & 255; x = Math.floor(x / 256); }
    } else {
      let x = BigInt(v);                                                         // prices and sizes, 1e18-scaled
      for (let k = len - 1; k >= 0; k--) { this.b[off + k] = Number(x & 0xffn); x >>= 8n; }
    }
    this.i += 32; return this;
  }
  bool(v) { return this.uint(v ? 1 : 0, 1); }
  address(a) { const u = bytes(a); if (u.length !== 20) throw new Error(`address: ${a}`); this.need(32); this.b.set(u, this.i + 12); this.i += 32; return this; }
  /// bytes32 occupies the word left-aligned; a shorter bytesN is right-padded. Anything longer is a bug.
  bytes32(h) { const u = bytes(h); if (u.length > 32) throw new Error(`bytes32 too long: ${u.length}`); this.need(32); this.b.set(u, this.i); this.i += 32; return this; }
  done() { return this.b.subarray(0, this.i); }
}
export { Words as AbiWords };            // rewards.js builds its claim leaves with the same packer
/// keccak256(prefix ‖ payload) — the leaf/node/root constructor
export function tagged(tag, ...parts) {
  let n = 1; for (const p of parts) n += p.length;
  const b = new Uint8Array(n); b[0] = tag; let o = 1;
  for (const p of parts) { b.set(p, o); o += p.length; }
  return keccak_256(b);
}

// ------------------------------------------------------------------ leaves
/// the canonical leaf of one fill. `f` is the matcher's fill record (bigint price/size, 0x hashes, addresses).
export function fillLeaf(f) {
  const w = new Words(8)
    .uint(f.seq).bytes32(f.makerHash).bytes32(f.takerHash)
    .address(f.maker).address(f.taker)
    .uint(f.price).uint(f.size).bool(f.takerBuys);
  return hex(tagged(LEAF, w.done()));
}
/// the canonical leaf of one sequenced op. `op` is { t: 'add'|'cancel', hash, order?, sig?, signer? }.
/// A cancel has no price, size or flags of its own: it names the order it kills and is signed by the user, so
/// the leaf carries the cancel's own signature hash (or the zero word when the engine cancelled it).
export function orderLeaf(op) {
  const add = op.t !== 'cancel';
  const o = op.order || {};
  const w = new Words(9)
    .uint(add ? 1 : 2, 1)
    .bytes32(op.hash)
    .address(add ? o.user : (op.user || o.user || '0x' + '00'.repeat(20)))
    .address(op.signer || '0x' + '00'.repeat(20))
    .bool(add ? o.buy : false)
    .uint(add ? o.price : 0).uint(add ? o.size : 0)
    .uint(add ? ((o.postOnly ? 1 : 0) | (o.ioc ? 2 : 0)) : 0, 1)
    .bytes32(op.sig ? keccak(bytes(op.sig)) : ZERO32);
  return hex(tagged(LEAF, w.done()));
}
/// the leaf of one RESTING order, for the book-state commitment. Price-time identity only: who, which side,
/// what price, how much is left, and the sequence number that fixes its place in the FIFO.
export function restingLeaf(o) {
  const w = new Words(6)
    .bytes32(o.hash).address(o.user).bool(o.buy)
    .uint(o.price).uint(o.remaining).uint(o.seq);
  return hex(tagged(LEAF, w.done()));
}

// ------------------------------------------------------------------ tree
const u32 = (n) => { const b = new Uint8Array(4); b[0] = (n >>> 24) & 255; b[1] = (n >>> 16) & 255; b[2] = (n >>> 8) & 255; b[3] = n & 255; return b; };
/// the tree is walked in BYTES, not hex: a batch of 250 orders is ~500 nodes, and converting each one to a
/// string and back cost more than the hashing did (it was most of the sequencer's seal time). Hex appears only
/// at the boundary — the leaves coming in and the root going out.
const NODE_BUF = new Uint8Array(65); NODE_BUF[0] = NODE;
const pairB = (l, r) => { NODE_BUF.set(l, 1); NODE_BUF.set(r, 33); return keccak_256(NODE_BUF); };
const apexB = (apex, count) => { const b = new Uint8Array(37); b[0] = ROOT; b.set(u32(count), 1); b.set(apex, 5); return hex(keccak_256(b)); };
const apexRoot = (apexHex, count) => apexB(bytes(apexHex), count);
const pair = (l, r) => hex(pairB(bytes(l), bytes(r)));

/// the Merkle root of a leaf list (already-hashed leaves, as fillLeaf/orderLeaf return them)
export function merkleRoot(leaves) {
  const count = leaves.length;
  if (!count) return apexRoot(ZERO32, 0);
  let layer = leaves.map(bytes);
  while (layer.length > 1) {
    const next = [];
    for (let i = 0; i + 1 < layer.length; i += 2) next.push(pairB(layer[i], layer[i + 1]));
    if (layer.length & 1) next.push(layer[layer.length - 1]);   // promoted unchanged
    layer = next;
  }
  return apexB(layer[0], count);
}
/// the proof of leaves[index] against merkleRoot(leaves)
export function merkleProof(leaves, index) {
  const count = leaves.length;
  if (!Number.isInteger(index) || index < 0 || index >= count) throw new Error(`index ${index} out of ${count}`);
  const path = []; let layer = leaves.map(bytes), i = index;
  while (layer.length > 1) {
    const odd = layer.length & 1, last = layer.length - 1;
    if (!(i === last && odd)) path.push(hex(layer[i ^ 1]));     // a promoted node has no sibling
    const next = [];
    for (let k = 0; k + 1 < layer.length; k += 2) next.push(pairB(layer[k], layer[k + 1]));
    if (odd) next.push(layer[last]);
    layer = next; i >>= 1;
  }
  return { leaf: leaves[index], index, count, path };
}
/// verify a proof against a root alone. The layer sizes come from `count`, so promotions are known and the
/// whole path must be consumed — a proof with a spare element, or one short, fails.
export function verifyProof(proof, root) {
  if (!proof || typeof proof !== 'object' || !Array.isArray(proof.path)) return false;
  const { leaf, index, count, path } = proof;
  if (!Number.isInteger(count) || !Number.isInteger(index) || index < 0 || index >= count) return false;
  let i = index, n = count, p = 0;
  try {
    let h = bytes(leaf);
    while (n > 1) {
      const odd = n & 1;
      if (!(i === n - 1 && odd)) {
        if (p >= path.length) return false;
        const sib = bytes(path[p++]);
        h = (i & 1) ? pairB(sib, h) : pairB(h, sib);
      }
      i >>= 1; n = (n + 1) >> 1;
    }
    if (p !== path.length) return false;
    return apexB(h, count) === root;
  } catch { return false; }
}

// ------------------------------------------------------------------ the batch's commitments
export const fillsRootOf = (fills) => merkleRoot(fills.map(fillLeaf));
export const ordersRootOf = (ops) => merkleRoot(ops.map(orderLeaf));
/// the state commitment at an epoch boundary: a root over every resting order in a canonical order
/// (asks before bids, price ascending, then sequence number, then order hash — so two books that arrived at the
/// same state by different routes commit to the same word). The hash is the final tiebreaker because a shard
/// holds several books and each book numbers its own sequence, so (side, price, seq) is not unique across a
/// shard — without it the sort would be unstable and two honest miners could commit to different roots.
/// O(resting): docs/L3-MINERS.md §6.6 explains why this is an epoch-boundary operation and not a per-batch one.
export function bookHashOf(orders) {
  const rows = [...orders].sort((a, b) =>
    (a.buy === b.buy
      ? (a.price === b.price ? (Number(a.seq) - Number(b.seq) || (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0)) : (a.price < b.price ? -1 : 1))
      : (a.buy ? 1 : -1)));
  return merkleRoot(rows.map(restingLeaf));
}

/// batchRoot = keccak256(abi.encode(bytes32 shardId, uint64 epoch, uint64 index, uint64 seqFrom, uint64 seqTo,
///                                  bytes32 prevRoot, bytes32 ordersRoot, bytes32 fillsRoot, bytes32 bookHash))
/// The one word a vote is about: it commits to the shard, the position in the chain, and everything the batch
/// claims happened. `shardId = keccak256(utf8(shard))`.
export const shardId = (shard) => keccak(new TextEncoder().encode(String(shard)));
export function batchRootOf(b) {
  const w = new Words(9)
    .bytes32(shardId(b.shard))
    .uint(b.epoch, 8).uint(b.index, 8).uint(b.seqFrom, 8).uint(b.seqTo, 8)
    .bytes32(b.prevRoot || ZERO32).bytes32(b.ordersRoot).bytes32(b.fillsRoot).bytes32(b.bookHash || ZERO32);
  return keccak(w.done());
}
/// what a miner signs. It covers the miner's OWN roots and the sequencer's claimed root, so a vote cannot be
/// replayed against a different batch, a different shard or a different claim.
export function voteDigestOf(v) {
  const w = new Words(8)
    .bytes32(shardId(v.shard))
    .uint(v.epoch, 8).uint(v.index, 8)
    .bytes32(v.batchRoot).bytes32(v.claimed || ZERO32)
    .bytes32(v.fillsRoot).bytes32(v.bookHash || ZERO32)
    .bool(v.ok);
  return keccak(w.done());
}
/// what a validator attests at an epoch boundary: the shard, the epoch, its last batch root, the book hash, the
/// fill count, and the epoch's rewardsRoot (rewards.js). The rewards root travels with the finalized root and is
/// attested with it, which is what makes a miner's µROLLA balance provable without trusting the operator —
/// but it is deliberately NOT part of batchRootOf: see docs/L3-MINERS.md §6a, rewards depend on VOTES, which
/// arrive after the batch is sealed, so making them consensus-critical for the fills would invent forks.
export function epochDigestOf(e) {
  const w = new Words(7)
    .bytes32(shardId(e.shard)).uint(e.epoch, 8).uint(e.index, 8)
    .bytes32(e.batchRoot).bytes32(e.bookHash || ZERO32).uint(e.fills, 8)
    .bytes32(e.rewardsRoot || ZERO32);
  return keccak(w.done());
}
