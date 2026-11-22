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
