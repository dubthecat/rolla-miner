// node --test engine/l3/miner/words.test.mjs — the abi word packer, at the boundaries of its fast path.
//
// `uint()` writes a 32-byte word. It used to cost 32 mask-and-shift pairs on a BigInt, and `orderLeaf` does two
// of them — 64 BigInt operations a leaf, each allocating — while a leaf is 94 % of `ordersRootOf` and
// `ordersRootOf` is 87 % of what the sequencer spends per batch. A 1e18-scaled price or size is under 2^64, so
// the common case now writes eight bytes and zeroes the twenty-four above them.
//
// The bytes have to be IDENTICAL either way: this word goes into a leaf, the leaf into the batch root, and the
// batch root is what every miner votes on. A packer that differed above 2^64 would produce a root the sequencer
// and the miners disagree about — a fork, not a bug report. So the test is the two paths against each other,
// across the boundary and past it.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { AbiWords, hex } from './merkle.js';

/// the long way round: one byte at a time over the whole word, which is what this used to do always
const longWay = (v, len = 32) => {
  const b = new Uint8Array(32);
  let x = BigInt(v);
  for (let k = len - 1; k >= 0; k--) { b[32 - len + k] = Number(x & 0xffn); x >>= 8n; }
  return hex(b);
};
const packed = (v, len) => hex(new AbiWords(1).uint(v, len).done());

test('the fast path and the long way agree at and around 2^64', () => {
  const B = 0xffffffffffffffffn;
  for (const v of [0n, 1n, 255n, 256n, 65535n, B - 1n, B, B + 1n, B * 2n, 1n << 100n, (1n << 255n) - 1n]) {
    assert.equal(packed(v), longWay(v), `uint(${v})`);
  }
});

test('the values this venue actually packs: 1e18-scaled prices and sizes', () => {
  for (const v of [10n ** 18n, 10n ** 17n * 5n, 10n ** 18n * 5n, 10n ** 18n * 12n, 10n ** 21n, 10n ** 24n]) {
    assert.equal(packed(v), longWay(v), `uint(${v})`);
    assert.equal(packed(String(v)), longWay(v), `uint("${v}") — the log carries them as decimal strings`);
  }
});

test('a decimal string, a bigint and a number give the same word', () => {
  for (const v of [0, 1, 7, 1000, 2 ** 31, Number.MAX_SAFE_INTEGER]) {
    assert.equal(packed(v), longWay(BigInt(v)), `number ${v}`);
    assert.equal(packed(BigInt(v)), longWay(BigInt(v)), `bigint ${v}`);
    assert.equal(packed(String(v)), longWay(BigInt(v)), `string ${v}`);
  }
});

test('a short word (uint8, uint64) is right-aligned and untouched by the fast path', () => {
  for (const [v, len] of [[1, 1], [3, 1], [255, 1], [65535, 8], [2 ** 31, 8]]) {
    assert.equal(packed(v, len), longWay(BigInt(v), len), `uint(${v}, ${len})`);
  }
  assert.equal(packed(1, 1), '0x' + '00'.repeat(31) + '01');
});

test('a word is not polluted by the one before it', () => {
  // the fast path zeroes the high 24 bytes rather than trusting them, so a reused or grown buffer cannot leak
  const a = new AbiWords(1);
  a.uint(1n << 200n);                       // the slow path writes high bytes
  a.uint(5n);                               // the fast path must not inherit them
  const words = hex(a.done()).slice(2);
  assert.equal(words.length, 128);
  assert.equal(words.slice(64), longWay(5n).slice(2), 'the second word kept bytes from the first');
});

test('growing the buffer mid-write keeps every word it already held', () => {
  const w = new AbiWords(1);                 // room for one word; the second forces a grow
  const vals = [10n ** 18n, 1n << 100n, 7n, 0n, 0xffffffffffffffffn];
  for (const v of vals) w.uint(v);
  const got = hex(w.done()).slice(2);
  assert.equal(got.length, vals.length * 64);
  vals.forEach((v, i) => assert.equal('0x' + got.slice(i * 64, (i + 1) * 64), longWay(v), `word ${i}`));
});

// ---------------------------------------------------------------------------- the reused writer
// `orderLeaf` and `fillLeaf` build into a module-scoped scratch writer rather than allocating one per call:
// allocation, not arithmetic, is what the sequencer spends its time on at scale. That makes every word's
// CLEANLINESS load-bearing, because a word that only partly overwrites what was there produces a leaf that
// depends on the leaf before it — the same orders hashing to different roots on the sequencer and on a miner,
// which is a fork and not a bug report.
//
// This is not hypothetical: `uint()`'s number path stops as soon as the value is exhausted, so a `false` or a 0
// wrote NOTHING, and interleaving the old and new implementations in one process matched on 251 of 500 leaves.
import { orderLeaf } from './merkle.js';

const op = (i, over = {}) => ({
  t: 'add', hash: '0x' + String(i).padStart(64, '0'), signer: '0x' + '55'.repeat(20), sig: '0x' + 'cd'.repeat(65), at: 0,
  order: { user: '0x' + '77'.repeat(20), marketId: '1', outcome: '0', token: '0x' + 'c0'.repeat(20),
           buy: true, price: String(10n ** 17n * 5n), size: String(10n ** 18n * 5n), deadline: '9',
           nonce: String(i), salt: String(i), postOnly: false, ioc: false, ...over },
});

test('a leaf does not depend on the leaf built before it', () => {
  // the shapes that write the FEWEST bytes, after shapes that write the most: a false flag, a zero-ish price,
  // and a cancel (which zeroes most of the word set). Each must come out the same whatever preceded it.
  const lean = [
    op(1, { buy: false }),
    op(2, { postOnly: false, ioc: false }),
    op(3, { price: '1', size: '1' }),
    { t: 'cancel', hash: '0x' + '0c'.repeat(32), user: '0x' + '88'.repeat(20), signer: '0x' + '55'.repeat(20), sig: '0x' + 'cd'.repeat(65), at: 0 },
  ];
  const fat = op(99, { buy: true, postOnly: true, ioc: true, price: String((1n << 200n)), size: String((1n << 190n)) });
  for (const one of lean) {
    const alone = orderLeaf(one);                      // the scratch holds whatever the last test left
    orderLeaf(fat);                                    // fill every byte of every word
    const after = orderLeaf(one);
    assert.equal(after, alone, 'the leaf changed because of the leaf before it');
    // and again, the other way round
    orderLeaf(op(5, { buy: false, price: '3', size: '3' }));
    assert.equal(orderLeaf(one), alone);
  }
});

test('the same op leafed a thousand times gives one answer', () => {
  const ops = [op(1, { buy: false }), op(2, { buy: true, postOnly: true }), op(3, { price: '1' }),
    { t: 'cancel', hash: '0x' + 'aa'.repeat(32), user: '0x' + '11'.repeat(20), signer: '0x' + '22'.repeat(20), sig: '0x' + 'ee'.repeat(65) }];
  const want = ops.map(orderLeaf);
  for (let r = 0; r < 250; r++) {
    // a different order each round, so any carry-over between words would show
    for (let i = ops.length - 1; i >= 0; i--) assert.equal(orderLeaf(ops[i]), want[i], `round ${r}, op ${i}`);
  }
});
