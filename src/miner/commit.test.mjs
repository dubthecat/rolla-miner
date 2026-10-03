// engine/l3/miner/commit.test.mjs — the incremental state commitment (commit.js): determinism, remove/update
// against a tree rebuilt from scratch, proofs, and the pinned vectors native/book/commit_test.cpp also pins.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createCommitTree, verifyCommitProof, encodeCommitProof, decodeCommitProof, commitPriority } from './commit.js';
import { restingLeaf, ZERO32 } from './merkle.js';

// a deterministic fixture generator (LCG, like the rest of the l3 tests)
function gen(seed) {
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const hexw = (n) => { let h = '0x'; for (let i = 0; i < n; i++) h += ((rnd() * 256) | 0).toString(16).padStart(2, '0'); return h; };
  let seq = 0;
  const order = () => ({ hash: hexw(32), user: hexw(20), buy: rnd() < 0.5, price: BigInt(1 + ((rnd() * 10000) | 0)) * 10n ** 14n, remaining: BigInt(1 + ((rnd() * 100) | 0)) * 10n ** 18n, seq: ++seq });
  return { rnd, order, hexw };
}
const build = (orders) => { const t = createCommitTree(); for (const o of orders) assert.equal(t.insert(o), true); return t; };
const shuffle = (a, rnd) => { const b = a.slice(); for (let i = b.length; i > 1; i--) { const j = (rnd() * i) | 0; [b[i - 1], b[j]] = [b[j], b[i - 1]]; } return b; };
const PINNED = { hash: '0x' + 'ab'.repeat(32), user: '0x' + '11'.repeat(20), buy: true, price: 550000000000000000n, remaining: 3000000000000000000n, seq: 7 };

test('the pinned vectors: restingLeaf, the one-node root and the priority (shared with commit_test.cpp)', () => {
  assert.equal(restingLeaf(PINNED), '0xda812cb1a3ca38a5c6ae55087ae14e7734a4278ba5a0688e07d79e223bdc516a');
  const t = createCommitTree();
  assert.equal(t.root(), ZERO32); assert.equal(t.size, 0);
  assert.equal(t.insert(PINNED), true);
  assert.equal(t.root(), '0x4bbea421ef8c5a4519eee2d3ebd2cc228cebe798d4b3f6249502421c06be83d5');
  assert.deepEqual(commitPriority(true, PINNED.price, 7), { hi: 0xce320c10, lo: 0xacce9837 });
  const p = t.proof(PINNED.hash);
  assert.equal(p.leaf, restingLeaf(PINNED)); assert.equal(p.left, ZERO32); assert.equal(p.right, ZERO32); assert.deepEqual(p.path, []);
  assert.equal(verifyCommitProof(p, t.root(), PINNED), true);
  assert.equal(t.remove(PINNED.hash), true); assert.equal(t.root(), ZERO32); assert.equal(t.size, 0);
});

test('determinism: three permuted insertion orders give the same root; a different set does not', () => {
  const g = gen(42); const orders = Array.from({ length: 1500 }, g.order);
  const a = build(orders).root();
  assert.equal(build(orders.slice().reverse()).root(), a);
  assert.equal(build(shuffle(orders, g.rnd)).root(), a);
  assert.notEqual(build(orders.slice(1)).root(), a);
  // the in-order walk is the canonical (side, price, seq) list
  const w = build(shuffle(orders, g.rnd)).entries();
  assert.equal(w.length, orders.length);
  for (let i = 1; i < w.length; i++) {
    const x = w[i - 1], y = w[i];
    const sx = x.buy ? 0 : 1, sy = y.buy ? 0 : 1;
    assert.ok(sx < sy || (sx === sy && (x.price < y.price || (x.price === y.price && x.seq < y.seq))), `out of order at ${i}`);
  }
});

test('remove and setRemaining agree with a tree rebuilt from scratch, through 3,000 random operations', () => {
  const g = gen(7); const t = createCommitTree(); const model = [];
  for (let i = 0; i < 3000; i++) {
    const d = g.rnd();
    if (model.length && d < 0.25) { const k = (g.rnd() * model.length) | 0; assert.equal(t.remove(model[k].hash), true); model.splice(k, 1); }
    else if (model.length && d < 0.5) { const k = (g.rnd() * model.length) | 0; model[k].remaining = BigInt(1 + ((g.rnd() * 1e6) | 0)); assert.equal(t.setRemaining(model[k].hash, model[k].remaining), true); }
    else { const o = g.order(); assert.equal(t.insert(o), true); model.push(o); }
    if (i % 250 === 249) { assert.equal(t.root(), build(shuffle(model, g.rnd)).root(), `diverged at op ${i}`); assert.equal(t.size, model.length); }
  }
  assert.ok(t.size > 500);
  assert.deepEqual(t.entries().map((e) => e.hash).sort(), model.map((e) => e.hash).sort());
});

test('proofs verify against the root, tampering fails, and the wire form round-trips', () => {
  const g = gen(11); const orders = Array.from({ length: 400 }, g.order); const t = build(orders); const root = t.root();
  let maxDepth = 0;
  for (const o of orders) {
    const p = t.proof(o.hash);
    assert.equal(verifyCommitProof(p, root, o), true, `proof of ${o.hash}`);
    assert.equal(p.leaf, restingLeaf(o));
    maxDepth = Math.max(maxDepth, p.path.length);
    const enc = encodeCommitProof(p);
    assert.equal(enc.length, 96 + 65 * p.path.length);
    assert.deepEqual(decodeCommitProof(enc, o.hash), p);
  }
  assert.ok(maxDepth >= 8 && maxDepth <= 40, `depth ${maxDepth}`);
  const o = orders[123]; const p = t.proof(o.hash); assert.ok(p.path.length > 0);
  const clone = () => JSON.parse(JSON.stringify(p));
  assert.equal(verifyCommitProof(p, root), true);
  assert.equal(verifyCommitProof(p, root, { ...o, remaining: o.remaining + 1n }), false);   // a proof is about one remaining
  let q = clone(); q.leaf = '0x' + '00'.repeat(32); assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.right = q.left; assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.path[0].dir ^= 1; assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.path[0].dir = 2; assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.path[0].sibling = ZERO32; assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.path.pop(); assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.path.push(q.path[q.path.length - 1]); assert.equal(verifyCommitProof(q, root), false);
  q = clone(); q.leaf = '0x1234'; assert.equal(verifyCommitProof(q, root), false);
  assert.equal(verifyCommitProof(p, '0x' + 'ff'.repeat(32)), false);
  assert.equal(verifyCommitProof(null, root), false);
  assert.equal(t.proof('0x' + 'ee'.repeat(32)), null);
  assert.throws(() => decodeCommitProof(new Uint8Array(97)));
});

test("an order's root changes when its remaining changes, and changes back", () => {
  const g = gen(5); const orders = Array.from({ length: 300 }, g.order); const t = build(orders); const before = t.root();
  const o = orders[77];
  assert.equal(t.setRemaining(o.hash, 1n), true);
  const after = t.root(); assert.notEqual(after, before);
  assert.equal(t.get(o.hash).remaining, 1n);
  assert.equal(verifyCommitProof(t.proof(o.hash), after, { ...o, remaining: 1n }), true);
  assert.equal(verifyCommitProof(t.proof(o.hash), after, o), false);
  assert.equal(t.setRemaining(o.hash, o.remaining), true); assert.equal(t.root(), before);
  assert.equal(t.setRemaining('0x' + 'ee'.repeat(32), 1n), false); assert.equal(t.root(), before);
  assert.equal(t.remove('0x' + 'ee'.repeat(32)), false); assert.equal(t.size, 300);
});

test('a duplicate hash is refused; a duplicate (side, price, seq) under another hash is ordered by hash, canonically', () => {
  const g = gen(9); const a = g.order(); const t = createCommitTree();
  assert.equal(t.insert(a), true); assert.equal(t.insert(a), false); assert.equal(t.size, 1);
  assert.equal(t.insert({ ...a, hash: a.hash.toUpperCase().replace('0X', '0x') }), false);   // the same hash, spelled differently
  const b = { ...a, hash: g.hexw(32) }, c = { ...a, hash: g.hexw(32) };                      // another book of the shard: same side, price, seq
  const x = build([a, b, c]), y = build([c, a, b]);
  assert.equal(x.size, 3); assert.equal(x.root(), y.root());
  const w = x.entries().map((e) => e.hash); assert.deepEqual(w, w.slice().sort());
  assert.equal(verifyCommitProof(x.proof(b.hash), x.root(), b), true);
  assert.throws(() => t.insert({ ...a, hash: '0x1234' }));
  assert.throws(() => t.insert({ ...g.order(), price: -1n }));
  assert.throws(() => t.insert({ ...g.order(), seq: 1n << 64n }));
});
