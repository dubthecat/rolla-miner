// node --test engine/l3/miner/miner.test.mjs — the Merkle tree, the log, the verifier, the quorum, and an
// in-process cluster of one sequencer and three miners over 2,000 signed orders.
//
// The cluster test is the real one: it proves that three independent books fed the same log agree on every
// root, that a tampered miner is caught, and that a restarted miner reaches the same book hash. It prints the
// cluster's throughput and the per-order signature cost, which is the number docs/L3-MINERS.md §6 is about.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { encodeAbiParameters, keccak256, concat } from 'viem';
import { merkleRoot, merkleProof, verifyProof, fillLeaf, orderLeaf, bookHashOf, batchRootOf, voteDigestOf, ZERO32 } from './merkle.js';
import { createFileLog, ordersTopic, votesTopic } from './log.js';
import { createVerifier, recoverSigner, orderHash } from './verify.js';
import { createQuorum } from './quorum.js';

const tmp = (tag) => fs.mkdtempSync(path.join(os.tmpdir(), `l3-${tag}-`));
const FILL = { seq: 42, makerHash: '0x' + 'ab'.repeat(32), takerHash: '0x' + 'cd'.repeat(32), maker: '0x' + '11'.repeat(20), taker: '0x' + '22'.repeat(20), price: 520000000000000000n, size: 3000000000000000000n, takerBuys: true };

// ------------------------------------------------------------------------------------------------- merkle
test('a fill leaf is byte-identical to keccak256(0x00 ‖ abi.encode(...))', () => {
  const viem = keccak256(concat(['0x00', encodeAbiParameters(
    [{ type: 'uint256' }, { type: 'bytes32' }, { type: 'bytes32' }, { type: 'address' }, { type: 'address' }, { type: 'uint256' }, { type: 'uint256' }, { type: 'bool' }],
    [BigInt(FILL.seq), FILL.makerHash, FILL.takerHash, FILL.maker, FILL.taker, FILL.price, FILL.size, FILL.takerBuys])]));
  assert.equal(fillLeaf(FILL), viem);   // a Solidity verifier can check a proof with abi.encode and nothing else
});

test('an order leaf is byte-identical to keccak256(0x00 ‖ abi.encode(...))', () => {
  const o = { user: '0x' + '33'.repeat(20), marketId: '7', outcome: '1', token: '0x' + '44'.repeat(20), buy: true, price: '400000000000000000', size: '2000000000000000000', deadline: '99', nonce: '1', salt: '2', postOnly: true, ioc: false };
  const op = { t: 'add', hash: '0x' + 'ee'.repeat(32), order: o, sig: '0x' + '55'.repeat(65), signer: '0x' + '66'.repeat(20) };
  const viem = keccak256(concat(['0x00', encodeAbiParameters(
    [{ type: 'uint8' }, { type: 'bytes32' }, { type: 'address' }, { type: 'address' }, { type: 'bool' }, { type: 'uint256' }, { type: 'uint256' }, { type: 'uint8' }, { type: 'bytes32' }],
    [1, op.hash, o.user, op.signer, true, BigInt(o.price), BigInt(o.size), 1, keccak256(op.sig)])]));
  assert.equal(orderLeaf(op), viem);
});

test('every leaf proves against the root, and nothing else does', () => {
  for (const n of [1, 2, 3, 4, 5, 7, 8, 9, 16, 17, 100, 101, 257]) {
    const leaves = Array.from({ length: n }, (_, i) => fillLeaf({ ...FILL, seq: i }));
    const root = merkleRoot(leaves);
    for (let i = 0; i < n; i++) {
      const p = merkleProof(leaves, i);
      assert.ok(verifyProof(p, root), `n=${n} i=${i} does not verify`);
      assert.ok(p.path.length <= Math.ceil(Math.log2(Math.max(2, n))), `n=${n} i=${i} path is ${p.path.length} long`);
      assert.ok(!verifyProof({ ...p, leaf: fillLeaf({ ...FILL, seq: 10 ** 6 }) }, root), 'a forged leaf verified');
      if (n > 1) assert.ok(!verifyProof({ ...p, index: (i + 1) % n }, root), 'a shifted index verified');
      assert.ok(!verifyProof({ ...p, count: n + 1 }, root), 'a wrong count verified');
      assert.ok(!verifyProof({ ...p, path: [...p.path, ZERO32] }, root), 'a padded path verified');
      if (p.path.length) assert.ok(!verifyProof({ ...p, path: p.path.slice(0, -1) }, root), 'a truncated path verified');
    }
  }
});

test('the empty tree has a root of its own, and the one-leaf root is not the leaf', () => {
  assert.notEqual(merkleRoot([]), ZERO32);
  assert.notEqual(merkleRoot([fillLeaf(FILL)]), fillLeaf(FILL));   // the count is bound into the root
  assert.notEqual(merkleRoot([]), merkleRoot([fillLeaf(FILL)]));
  assert.throws(() => merkleProof([], 0));
});

test('reordering leaves changes the root; promotion does not collide with duplication', () => {
  const a = fillLeaf({ ...FILL, seq: 1 }), b = fillLeaf({ ...FILL, seq: 2 }), c = fillLeaf({ ...FILL, seq: 3 });
  assert.notEqual(merkleRoot([a, b, c]), merkleRoot([a, c, b]));
  assert.notEqual(merkleRoot([a, b, c]), merkleRoot([a, b, c, c]));   // Bitcoin's bug, absent
});

test('the book hash is order-independent in arrival and canonical in price-time', () => {
  const mk = (i, buy, price, seq) => ({ hash: '0x' + String(i).padStart(64, '0'), user: '0x' + '77'.repeat(20), buy, price: BigInt(price), remaining: 10n ** 18n, seq });
  const rows = [mk(1, true, 400, 5), mk(2, false, 600, 3), mk(3, true, 400, 9), mk(4, false, 610, 7)];
  assert.equal(bookHashOf(rows), bookHashOf([...rows].reverse()));
  assert.notEqual(bookHashOf(rows), bookHashOf(rows.slice(1)));
  const moved = rows.map((r) => (r.seq === 9 ? { ...r, remaining: 5n * 10n ** 17n } : r));
  assert.notEqual(bookHashOf(rows), bookHashOf(moved));   // size matters
});

test('batchRoot and voteDigest cover every field they claim to', () => {
  const b = { shard: '42', epoch: 1, index: 2, seqFrom: 3, seqTo: 4, prevRoot: ZERO32, ordersRoot: fillLeaf(FILL), fillsRoot: ZERO32, bookHash: ZERO32 };
  const base = batchRootOf(b);
  for (const k of ['shard', 'epoch', 'index', 'seqFrom', 'seqTo', 'prevRoot', 'ordersRoot', 'fillsRoot', 'bookHash']) {
    const bump = typeof b[k] === 'number' ? b[k] + 1 : (k === 'shard' ? '43' : fillLeaf({ ...FILL, seq: 99 }));
    assert.notEqual(batchRootOf({ ...b, [k]: bump }), base, `batchRoot ignores ${k}`);
  }
  const v = { shard: '42', epoch: 1, index: 2, batchRoot: base, claimed: base, fillsRoot: ZERO32, bookHash: ZERO32, ok: true };
  assert.notEqual(voteDigestOf(v), voteDigestOf({ ...v, ok: false }));
  assert.notEqual(voteDigestOf(v), voteDigestOf({ ...v, claimed: ZERO32 }));
});

// ---------------------------------------------------------------------------------------------- FileLog
test('FileLog: append, offsets, read, subscribe from an offset, tail, and reopen', async () => {
  const dir = tmp('log');
  const log = createFileLog({ dir, pollMs: 2 });
  const topic = ordersTopic('m1');
  assert.equal(await log.offset(topic), 0);
  for (let i = 0; i < 5; i++) assert.equal(await log.append(topic, { i, s: 'x'.repeat(i) }), i);
  assert.equal(await log.offset(topic), 5);
  assert.equal(await log.appendMany(topic, [{ i: 5 }, { i: 6 }]), 5);
  assert.equal(await log.offset(topic), 7);

  const all = await log.read(topic, 0);
  assert.deepEqual(all.map((r) => r.offset), [0, 1, 2, 3, 4, 5, 6]);
  assert.deepEqual(all.map((r) => r.value.i), [0, 1, 2, 3, 4, 5, 6]);
  assert.deepEqual((await log.read(topic, 5)).map((r) => r.value.i), [5, 6]);
  assert.equal((await log.read(topic, 0, 2)).length, 2);

  const seen = [];
  const sub = await log.subscribe(topic, 3, async (r) => { seen.push([r.offset, r.value.i]); });
  assert.deepEqual(seen, [[3, 3], [4, 4], [5, 5], [6, 6]]);        // history, in order, from the asked offset
  await log.append(topic, { i: 7 });
  await new Promise((r) => setTimeout(r, 40));
  assert.deepEqual(seen[seen.length - 1], [7, 7]);                 // and the tail woke up
  sub.close();
  await log.append(topic, { i: 8 });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(seen.length, 5, 'a closed subscription kept delivering');

  await log.close();
  const again = createFileLog({ dir });                            // the log is the file: a new process sees it all
  assert.equal(await again.offset(topic), 9);
  assert.deepEqual((await again.read(topic, 8)).map((r) => r.value.i), [8]);
  await again.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('FileLog: two topics do not see each other, and a bad line is skipped not fatal', async () => {
  const dir = tmp('log2');
  const log = createFileLog({ dir });
  await log.append(ordersTopic('a'), { x: 1 });
  await log.append(votesTopic('a'), { y: 2 });
  assert.equal(await log.offset(ordersTopic('a')), 1);
  assert.equal(await log.offset(votesTopic('a')), 1);
  assert.equal((await log.read(ordersTopic('a')))[0].value.x, 1);
  await log.close();
  fs.appendFileSync(path.join(dir, 'orders.a.jsonl'), '{not json\n');
  const l2 = createFileLog({ dir });
  const got = [];
  const sub = await l2.subscribe(ordersTopic('a'), 0, async (r) => got.push(r.value.x));
  assert.deepEqual(got, [1]);
  sub.close(); await l2.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------------------------- verifier
test('the verifier recovers a signer, in-thread and in a pool, and refuses what the chain would refuse', async () => {
  const { privateKeyToAccount, generatePrivateKey } = await import('viem/accounts');
  const { BOOK_DOMAIN, l3OrderFor, serializeL3Order } = await import('../desk.js');
  const domain = BOOK_DOMAIN(46630, '0x' + '99'.repeat(20));
  const acct = privateKeyToAccount(generatePrivateKey());
  const order = l3OrderFor({ user: acct.address, marketId: 1, outcome: 0, token: '0x' + '88'.repeat(20), buy: true, price: 5n * 10n ** 17n, size: 10n ** 18n });
  const hash = orderHash(domain, serializeL3Order(order));
  const sig = await acct.signTypedData({ domain, types: (await import('../desk.js')).L3_ORDER_TYPES, primaryType: 'L3Order', message: order });
  assert.equal(recoverSigner(hash, sig), acct.address.toLowerCase());
  assert.equal(recoverSigner(hash, '0x' + '00'.repeat(65)), null);
  assert.equal(recoverSigner(hash, sig.slice(0, -2) + '09'), null, 'an impossible v recovered');
  assert.equal(recoverSigner('0xdead', sig), null);

  const items = [{ order: serializeL3Order(order), signature: sig }, { order: serializeL3Order(order), signature: '0x' + '11'.repeat(65) }];
  const inThread = createVerifier({ domain, workers: 0 });
  assert.deepEqual(await inThread.verifyOrders(items), [{ hash, signer: acct.address.toLowerCase() }, null]);
  await inThread.close();
  const pooled = createVerifier({ domain, workers: 2 });
  assert.deepEqual(await pooled.verifyOrders(items), [{ hash, signer: acct.address.toLowerCase() }, null]);
  assert.deepEqual(await pooled.verifyOrders([]), []);
  await pooled.close();
});

// ------------------------------------------------------------------------------------------------ quorum
function fakeBatch(index, prevRoot, salt = 0) {
  const b = { shard: 'q', epoch: 0, index, seqFrom: index * 10, seqTo: index * 10 + 9, prevRoot, ordersRoot: fillLeaf({ ...FILL, seq: index }), fillsRoot: fillLeaf({ ...FILL, seq: 1000 + index + salt }), bookHash: ZERO32 };
  b.batchRoot = batchRootOf(b);
  return b;
}
const voteFor = (miner, b, root = null) => ({ shard: b.shard, epoch: b.epoch, index: b.index, claimed: b.batchRoot, batchRoot: root || b.batchRoot, ordersRoot: b.ordersRoot, fillsRoot: b.fillsRoot, bookHash: b.bookHash, ok: !root || root === b.batchRoot, miner, sig: '0x' });

test('quorum: finality at the threshold, in order, and only for batches it has', async () => {
  const final = [];
  const q = createQuorum({ threshold: 2, verify: async (v) => v.miner, onFinal: (f) => final.push(f.index) });
  const b0 = fakeBatch(0, ZERO32), b1 = fakeBatch(1, b0.batchRoot), b2 = fakeBatch(2, b1.batchRoot);
  for (const b of [b0, b1, b2]) assert.equal(q.announce(b).ok, true);
  await q.vote(voteFor('0xa', b1)); await q.vote(voteFor('0xb', b1));
  assert.deepEqual(final, [], 'batch 1 finalized before batch 0');
  assert.equal(q.finalIndex, -1);
  await q.vote(voteFor('0xa', b0));
  assert.equal(q.finalIndex, -1, 'one vote was enough');
  await q.vote(voteFor('0xa', b0));                       // the same miner twice is still one vote
  assert.equal(q.finalIndex, -1);
  await q.vote(voteFor('0xc', b0));
  assert.deepEqual(final, [0, 1], 'finality did not cascade through the contiguous run');
  assert.equal(q.finalIndex, 1);
  await q.vote(voteFor('0xa', b2)); await q.vote(voteFor('0xb', b2));
  assert.equal(q.finalIndex, 2);
  assert.equal(q.halted, false);
  assert.equal(q.status().forks, 0);
});

test('quorum: a dissent is a fork and halts finality', async () => {
  const forks = [];
  const q = createQuorum({ threshold: 2, verify: async (v) => v.miner, onFork: (f) => forks.push(f) });
  const b0 = fakeBatch(0, ZERO32);
  q.announce(b0);
  await q.vote(voteFor('0xa', b0));
  const other = fakeBatch(0, ZERO32, 7).batchRoot;
  const r = await q.vote(voteFor('0xb', b0, other));
  assert.equal(r.dissent, 1);
  assert.equal(q.halted, true);
  assert.equal(forks.length >= 1, true);
  assert.equal(forks[0].why, 'a miner disagrees with the sequencer');
  assert.equal(forks[0].miner, '0xb');
  await q.vote(voteFor('0xc', b0));
  assert.equal(q.finalIndex, -1, 'a halted quorum still advanced finality');
  assert.equal(q.at(0).agree.length, 2);
  q.resume({ forget: true });
  assert.equal(q.finalIndex, 0, 'resume did not release the finalized batch');
});

test('quorum: equivocation, a spliced chain and an unknown miner', async () => {
  const q = createQuorum({ threshold: 2, miners: ['0xA', '0xb'], verify: async (v) => v.miner, haltAfter: 99 });
  const b0 = fakeBatch(0, ZERO32);
  q.announce(b0);
  assert.equal((await q.vote(voteFor('0xzz', b0))).why, 'not a known miner');
  assert.equal((await q.vote(voteFor('0xa', b0))).ok, true, 'the allowlist is case-sensitive');
  const bad = await q.vote(voteFor('0xa', b0, fakeBatch(0, ZERO32, 3).batchRoot));
  assert.equal(bad.why, 'equivocation');
  assert.equal(bad.evidence.length, 2);
  assert.deepEqual(q.equivocators(), ['0xa']);
  const spliced = fakeBatch(1, fillLeaf({ ...FILL, seq: 5 }));
  assert.equal(q.announce(spliced).ok, false);
  assert.equal(q.forks().some((f) => f.why === 'prevRoot does not chain to the previous batch'), true);
  // a batch whose body does not hash to the root it carries
  const lying = fakeBatch(2, spliced.batchRoot); lying.batchRoot = fakeBatch(2, spliced.batchRoot, 9).batchRoot;
  assert.equal(q.announce(lying).ok, false);
  assert.equal(q.forks().some((f) => f.why === 'the batch does not hash to the batchRoot it carries'), true);
});

test('quorum: a vote that flags the batch does not count, even when its roots match', async () => {
  // the case a forged order signature produces: the ops hash to the same roots, so only the miner's `ok` flag
  // distinguishes "I agree" from "I refuse this batch"
  const q = createQuorum({ threshold: 2, verify: async (v) => v.miner, haltAfter: 99, onFinal: () => {} });
  const b0 = fakeBatch(0, ZERO32);
  q.announce(b0);
  await q.vote(voteFor('0xa', b0));
  const flagged = { ...voteFor('0xb', b0), ok: false, why: 'op 3: signature does not recover' };
  const r = await q.vote(flagged);
  assert.equal(r.agree, 1);
  assert.equal(r.dissent, 1);
  assert.equal(q.finalIndex, -1, 'a flagged vote was counted towards finality');
  assert.equal(q.forks()[0].why, 'a miner disagrees with the sequencer');
  assert.equal(q.forks()[0].got, q.forks()[0].claimed, 'the roots were supposed to be identical here');
  assert.equal(q.forks()[0].minerWhy, 'op 3: signature does not recover');   // the miner's reason, kept beside the condition
  assert.equal(q.at(0).dissent.length, 1);
  await q.vote(voteFor('0xc', b0));
  assert.equal(q.finalIndex, 0, 'two clean votes did not finalize it');
});

test('quorum: votes that arrive before the batch are judged when it does', async () => {
  const final = [];
  const q = createQuorum({ threshold: 2, verify: async (v) => v.miner, onFinal: (f) => final.push(f.index) });
  const b0 = fakeBatch(0, ZERO32);
  await q.vote(voteFor('0xa', b0)); await q.vote(voteFor('0xb', b0));
  assert.equal(q.finalIndex, -1);
  q.announce(b0);
  assert.deepEqual(final, [0]);
});

// ------------------------------------------------------------------------------------- the cluster
// One sequencer and three miners over one FileLog, 2,000 signed orders from the workload generator of
// matcher.test.mjs (same LCG, same 10% cancels / 30% crossing / 10% IOC mix). Everything here is in one
// process, but nothing in the code knows that: the miners talk to the sequencer only through the log.
test('a cluster of 1 sequencer and 3 miners finalizes every batch, catches a tampered miner, and recovers from a journal', async () => {
  const { privateKeyToAccount, generatePrivateKey } = await import('viem/accounts');
  const { BOOK_DOMAIN, L3_ORDER_TYPES, l3OrderFor, serializeL3Order } = await import('../desk.js');
  const { createSequencer } = await import('./sequencer.js');
  const { createMiner, createShardState } = await import('./miner.js');

  const SHARD = '4242', MARKET = 4242, OUTCOME = 0, N = 2000;
  const E = 10n ** 18n;
  const dir = tmp('cluster');
  const domain = BOOK_DOMAIN(46630, '0x' + 'b0'.repeat(20));
  const token = '0x' + 'c0'.repeat(20);
  const traders = Array.from({ length: 8 }, () => privateKeyToAccount(generatePrivateKey()));

  // ---- the workload: the shape of matcher.test.mjs's run(seed, count), signed ----
  // Built in two passes because a cancel names an order by its EIP-712 hash, which only exists once the order
  // is signed: pass one draws the sequence (10% cancels of something that should still be resting, 30% crossing,
  // 10% IOC, sizes 1..100, prices within 20¢ of a 50¢ mid) and remembers plan POSITIONS; pass two signs; pass
  // three turns the remembered positions into hashes.
  let s = 11 >>> 0;
  const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const live = [];
  const plan = [];
  for (let i = 0; i < N; i++) {
    if (live.length && rnd() < 0.1) { plan.push({ t: 'cancel', target: live.splice(Math.floor(rnd() * live.length), 1)[0] }); continue; }
    const buy = rnd() < 0.5, cross = rnd() < 0.3, mid = 50, off = Math.floor(rnd() * 20);
    const cents = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
    const acct = traders[Math.floor(rnd() * traders.length)];
    const order = l3OrderFor({ user: acct.address, marketId: MARKET, outcome: OUTCOME, token, buy,
      price: BigInt(cents) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 100)) * E, ioc: rnd() < 0.1 });
    order.nonce = BigInt(1700000000000 + i); order.salt = BigInt(i) * 7919n + 1n;   // deterministic, so reruns match
    plan.push({ t: 'add', acct, order });
    if (!order.ioc) live.push(plan.length - 1);
  }

  // sign, and measure what a signature costs
  const tSign = process.hrtime.bigint();
  for (const p of plan) {
    if (p.t === 'cancel') continue;
    const order = serializeL3Order(p.order);
    const sig = await p.acct.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: p.order });
    p.op = { t: 'add', hash: orderHash(domain, order), market: MARKET, outcome: OUTCOME, order, sig, signer: p.acct.address.toLowerCase(), at: 1700000000000 };
  }
  const signMs = Number(process.hrtime.bigint() - tSign) / 1e6;
  for (const p of plan) if (p.t === 'cancel') p.op = { t: 'cancel', hash: plan[p.target].op.hash, market: MARKET, outcome: OUTCOME };
  const cancels = plan.filter((p) => p.t === 'cancel').length;

  // ---- the log, the sequencer, the three miners, and an engine-side quorum reading both topics ----
  const log = createFileLog({ dir: path.join(dir, 'log'), pollMs: 2 });
  const seqAcct = privateKeyToAccount(generatePrivateKey());
  const state = createShardState({});
  const sealed = [];
  const seq = createSequencer({ shard: SHARD, log, account: seqAcct, state, batchMs: 40, batchMax: 250, epochBatches: 4,
    onSealed: (b) => sealed.push(b), logger: () => {} });
  await seq.resume();

  const finals = [];
  const quorum = createQuorum({ threshold: 3, onFinal: (f) => finals.push(f), logger: () => {} });
  const qOrders = await log.subscribe(ordersTopic(SHARD), 0, async ({ value }) => { quorum.announce(value); });
  const qVotes = await log.subscribe(votesTopic(SHARD), 0, async ({ value }) => { await quorum.vote(value); });

  const keys = [generatePrivateKey(), generatePrivateKey(), generatePrivateKey()];
  const votesSeen = new Map();   // miner → [vote]
  const miners = [];
  for (let i = 0; i < 3; i++) {
    const account = privateKeyToAccount(keys[i]);
    votesSeen.set(account.address.toLowerCase(), []);
    const m = createMiner({ shard: SHARD, log, account, domain, dir: path.join(dir, `miner${i + 1}`), workers: 2,
      sequencers: [seqAcct.address], threshold: 3, watchVotes: false, logger: () => {},
      onVote: (v) => votesSeen.get(account.address.toLowerCase()).push(v) });
    await m.start();
    miners.push(m);
  }

  // ---- drive it. An await between orders is what the engine has naturally (one HTTP request each) ----
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < plan.length; i++) {
    seq.submit(plan[i].op);
    if (i % 25 === 0) await new Promise((r) => setImmediate(r));
  }
  await seq.flush();
  const seqMs = Number(process.hrtime.bigint() - t0) / 1e6;
  const lastIndex = seq.index - 1;
  assert.ok(sealed.length >= 6, `only ${sealed.length} batches sealed`);

  // wait for the miners to catch up and the quorum to finalize everything
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline && quorum.finalIndex < lastIndex) await new Promise((r) => setTimeout(r, 10));
  const clusterMs = Number(process.hrtime.bigint() - t0) / 1e6;
  const mainBatches = sealed.length;

  // ---- every batch finalized with three matching votes, on the sequencer's roots ----
  assert.equal(quorum.finalIndex, lastIndex, `finality stopped at ${quorum.finalIndex} of ${lastIndex}`);
  assert.equal(quorum.halted, false);
  assert.equal(quorum.status().forks, 0);
  assert.deepEqual(finals.map((f) => f.index), sealed.map((b) => b.index));
  for (const f of finals) assert.equal(f.votes, 3, `batch ${f.index} finalized on ${f.votes} votes`);
  for (const b of sealed) {
    const st = quorum.at(b.index);
    assert.equal(st.agree.length, 3);
    assert.equal(st.dissent.length, 0);
    for (const [, vs] of votesSeen) {
      const v = vs.find((x) => x.index === b.index);
      assert.ok(v, `a miner never voted on ${b.index}`);
      assert.equal(v.ok, true, `a miner dissented on ${b.index}: ${v.why}`);
      assert.equal(v.batchRoot, b.batchRoot);
      assert.equal(v.fillsRoot, b.fillsRoot);
      assert.equal(v.ordersRoot, b.ordersRoot);
      assert.equal(v.bookHash, b.bookHash);
    }
  }
  // and the four books are the same book
  const seqBook = state.bookHash();
  for (const m of miners) {
    assert.equal(m.state.bookHash(), seqBook, `miner ${m.address} has a different book`);
    assert.equal(m.index, lastIndex);
    assert.equal(m.stalled, null);
    assert.equal(m.metrics().dissents, 0);
    assert.equal(m.metrics().badSigs, 0);
  }
  const epochs = sealed.filter((b) => b.bookHash !== ZERO32).length;
  assert.ok(epochs >= 1, 'no epoch boundary carried a book commitment');

  // ---- a challenge proof: one fill of the last batch, against that batch's fillsRoot ----
  const proof = miners[0].proveFill(lastIndex, () => true);
  assert.ok(proof, 'the miner could not produce a fill proof');
  assert.equal(verifyProof(proof.proof, proof.fillsRoot), true);
  assert.equal(proof.fillsRoot, sealed[sealed.length - 1].fillsRoot);
  assert.equal(verifyProof({ ...proof.proof, leaf: fillLeaf({ ...FILL, seq: 123456 }) }, proof.fillsRoot), false);

  // ---- a tampered miner dissents, and an observer sees the fork ----
  const badAcct = privateKeyToAccount(generatePrivateKey());
  const observed = [];
  const observer = createQuorum({ threshold: 3, onFork: (f) => observed.push(f), logger: () => {} });
  const oOrders = await log.subscribe(ordersTopic(SHARD), 0, async ({ value }) => { observer.announce(value); });
  // a stub verifier: the Byzantine miner's lie is in the MATCHING, and paying 2,000 more recoveries to prove it
  // would only make the test slower. Every other miner here does the real work.
  const stub = { workers: 0, stats: () => ({}), close: async () => {},
    verifyOrders: async (items) => items.map((it) => ({ hash: orderHash(domain, it.order), signer: String(it.order.user).toLowerCase() })) };
  const bad = createMiner({ shard: SHARD, log, account: badAcct, domain, dir: path.join(dir, 'miner-bad'), verifier: stub,
    sequencers: [seqAcct.address], threshold: 3, watchVotes: false, logger: () => {},
    tamper: (batch, fills) => (Number(batch.index) === 2 && fills.length ? fills.slice(1) : fills) });
  await bad.start();
  const oVotes = await log.subscribe(votesTopic(SHARD), 0, async ({ value }) => { await observer.vote(value); });
  const dl2 = Date.now() + 120000;
  while (Date.now() < dl2 && bad.index < lastIndex) await new Promise((r) => setTimeout(r, 10));
  assert.equal(bad.index, lastIndex);
  assert.equal(bad.metrics().dissents, 1, 'the tampered miner did not dissent exactly once');
  assert.equal(bad.lastVote.ok, true, 'the tamper leaked past its own batch');
  const dis = observed.find((f) => f.index === 2 && f.why === 'a miner disagrees with the sequencer');
  assert.ok(dis, `no dissent fork was raised: ${JSON.stringify(observed.map((f) => f.why))}`);
  assert.equal(dis.miner, badAcct.address.toLowerCase());
  assert.notEqual(dis.got, dis.claimed);
  assert.equal(observer.halted, true, 'the observer did not halt on the fork');
  assert.equal(observer.at(2).dissent.length, 1);
  assert.equal(observer.at(2).agree.length, 3, 'the honest three were not still counted');
  await bad.stop(); oOrders.close(); oVotes.close();

  // ---- a miner restarted from its journal catches up to the same book hash ----
  const m2 = miners[1];
  const before = { index: m2.index, book: m2.state.bookHash(), votes: m2.metrics().votes };
  await m2.stop();
  const revived = createMiner({ shard: SHARD, log, account: privateKeyToAccount(keys[1]), domain,
    dir: path.join(dir, 'miner2'), workers: 0, sequencers: [seqAcct.address], threshold: 3, watchVotes: false, logger: () => {} });
  await revived.start();
  assert.equal(revived.index, before.index, 'the revived miner is at a different index');
  assert.equal(revived.state.bookHash(), before.book, 'the revived miner rebuilt a different book');
  assert.equal(revived.state.bookHash(), seqBook);
  assert.equal(revived.metrics().replayed, sealed.length, `replayed ${revived.metrics().replayed} of ${sealed.length}`);
  assert.equal(revived.metrics().votes, 0, 'the revived miner re-voted on history');
  assert.equal(revived.stalled, null);
  // it keeps going: one more signed order, one more batch, one more vote on the same chain
  const extra = l3OrderFor({ user: traders[0].address, marketId: MARKET, outcome: OUTCOME, token, buy: true, price: 41n * E / 100n, size: 3n * E });
  const extraSig = await traders[0].signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: extra });
  seq.submit({ t: 'add', hash: orderHash(domain, serializeL3Order(extra)), market: MARKET, outcome: OUTCOME, order: serializeL3Order(extra), sig: extraSig, signer: traders[0].address.toLowerCase(), at: Date.now() });
  await seq.flush();
  const dl3 = Date.now() + 60000;
  while (Date.now() < dl3 && revived.index < seq.index - 1) await new Promise((r) => setTimeout(r, 10));
  assert.equal(revived.index, seq.index - 1, 'the revived miner did not follow the live chain');
  assert.equal(revived.metrics().votes + revived.metrics().dissents, 1);
  await revived.stop();

  // ---- the numbers ----
  const verifyMs = miners.reduce((a, m) => a + m.metrics().verifyMs, 0);
  const verified = miners.reduce((a, m) => a + m.metrics().orders, 0);
  const fills = miners[0].metrics().fills;
  console.log(`\n  cluster: ${N} ops (${N - cancels} signed orders, ${cancels} cancels) · ${mainBatches} batches · ${fills} fills · 1 sequencer + 3 miners over a FileLog`);
  console.log(`  sequencing      ${seqMs.toFixed(0)} ms → ${Math.round(N / (seqMs / 1000)).toLocaleString()} orders/s (match + roots + sign + append; the engine verified the signatures before this)`);
  console.log(`  whole cluster   ${clusterMs.toFixed(0)} ms → ${Math.round(N / (clusterMs / 1000)).toLocaleString()} orders/s through sequencing AND 3 independent replays + votes`);
  console.log(`  signing (viem)  ${(signMs / N).toFixed(2)} ms/order → ${Math.round(N / (signMs / 1000)).toLocaleString()} signatures/s on one thread`);
  console.log(`  verification    ${(verifyMs / verified).toFixed(2)} ms/order of wall time per miner (${miners[0].verifier.workers} workers each), ${verified} recoveries in ${(clusterMs / 1000).toFixed(1)} s → ${Math.round(verified / (clusterMs / 1000)).toLocaleString()}/s across the cluster`);
  console.log(`                  one recovery costs 5.6 ms of a core here (docs/L3-MINERS.md §6), so ${verified} of them are ${(verified * 5.6 / 1000).toFixed(0)} core-seconds: the pool is the only reason this finishes in ${(clusterMs / 1000).toFixed(0)} s`);
  for (const m of miners) { const x = m.metrics(); console.log(`    ${m.address.slice(0, 10)}… ${x.batches} batches · verify ${x.verifyMs.toFixed(0)} ms · match ${x.applyMs.toFixed(0)} ms · roots ${x.rootMs.toFixed(0)} ms · ${x.fills} fills`); }
  const ss = seq.status();
  console.log(`  sequencer       seal ${ss.perBatchMs} ms/batch, of which roots ${(ss.rootMs / ss.batches).toFixed(0)} ms · ${epochs} epoch boundaries carried a book commitment`);

  for (const m of miners) await m.stop();
  qOrders.close(); qVotes.close();
  await seq.stop(); await log.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

// --------------------------------------------------------------------------- the engine side of L3_MINERS=1
// book.js's whole contribution is a flag, two hooks and a finality callback. This is those hooks: the rig
// sequences what the engine already matched, a real miner votes, and the fills the engine staged under that
// batch are released by the quorum — the path a trader's money actually takes with L3_MINERS=1.
test('the rig sequences the engine\'s own fills and releases them on finality', async () => {
  const { privateKeyToAccount, generatePrivateKey } = await import('viem/accounts');
  const { BOOK_DOMAIN, L3_ORDER_TYPES, l3OrderFor, serializeL3Order, parseL3Order } = await import('../desk.js');
  const { createSequencerRig } = await import('./sequencer.js');
  const { createMiner } = await import('./miner.js');
  const { createBook } = await import('../matcher.js');
  const { createLog } = await import('./log.js');

  const dir = tmp('rig');
  const E = 10n ** 18n, MARKET = 91, SHARD = '91';
  const operator = privateKeyToAccount(generatePrivateKey());
  const minerAcct = privateKeyToAccount(generatePrivateKey());
  const env = { L3_LOG: 'file', L3_LOG_DIR: path.join(dir, 'log'), L3_THRESHOLD: '1', L3_BATCH_MS: '20', L3_BATCH_MAX: '4', L3_EPOCH_BATCHES: '2', CHAIN_ID: '46630' };
  const domain = BOOK_DOMAIN(46630, '0x' + 'b0'.repeat(20));
  const token = '0x' + 'c0'.repeat(20);
  const traders = [privateKeyToAccount(generatePrivateKey()), privateKeyToAccount(generatePrivateKey())];

  // the engine's own book and the staging map, exactly as book.js holds them
  const b = { key: `${MARKET}:0`, market: MARKET, outcome: 0, token, m: createBook() };
  const staged = new Map(); const released = [];
  const rig = createSequencerRig({ env, dir: path.join(dir, 'log'), account: operator, logger: () => {},
    onFinal: (f) => { const a = staged.get(f.slot) || []; staged.delete(f.slot); released.push({ slot: f.slot, fills: a.length, votes: f.votes, credit: f.credit }); },
    onFork: () => {} });
  await rig.ready();
  assert.equal(await rig.prepare(b), SHARD, 'the shard did not come up');   // what book.js awaits once per market

  const log = await createLog({ env, dir: path.join(dir, 'log'), logger: () => {} });
  const miner = createMiner({ shard: SHARD, log, account: minerAcct, domain, dir: path.join(dir, 'miner'), workers: 0,
    sequencers: [operator.address], threshold: 1, logger: () => {}, env: { ...env, L3_REWARD_BASE: '100', L3_REWARD_PER_FILL: '1' } });
  await miner.start();

  // six orders through the hooks: three bids, three crossing asks
  for (let i = 0; i < 6; i++) {
    const t = traders[i % 2];
    const o = l3OrderFor({ user: t.address, marketId: MARKET, outcome: 0, token, buy: i < 3, price: (i < 3 ? 51n : 50n) * E / 100n, size: 2n * E });
    const sig = await t.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: o });
    const hash = orderHash(domain, serializeL3Order(o));
    const rec = { hash, o: parseL3Order(serializeL3Order(o)), sig, signer: t.address.toLowerCase(), at: Date.now() };
    const r = b.m.add({ hash, user: o.user.toLowerCase(), buy: o.buy, price: o.price, size: o.size });
    const slot = rig.record(b, rec, r.fills);
    assert.ok(slot && slot.startsWith(`${SHARD}#`), `no slot for order ${i}: ${slot}`);
    if (r.fills.length) { const a = staged.get(slot) || []; a.push(...r.fills); staged.set(slot, a); }
    await new Promise((r2) => setImmediate(r2));
  }
  await rig.flush();

  const deadline = Date.now() + 60000;
  while (Date.now() < deadline && (!released.length || staged.size)) await new Promise((r) => setTimeout(r, 10));
  assert.equal(staged.size, 0, 'fills were left staged after finality');
  assert.ok(released.length >= 1);
  assert.equal(released.reduce((n, r) => n + r.fills, 0), 3, 'the released fills are not the three the book made');
  for (const r of released) assert.equal(r.votes, 1);
  // the µROLLA the quorum credited that one miner, and the epoch root it can claim against
  const st = rig.status();
  const q = st.shards[SHARD].quorum;
  assert.equal(q.halted, false);
  assert.equal(q.forks, 0);
  assert.ok(q.finalIndex >= 0);
  const earned = BigInt(q.rewards.accrued[minerAcct.address.toLowerCase()] || '0') + BigInt(q.rewards.epochs.reduce((a, e) => a + Number(e.total), 0));
  assert.ok(earned > 0n, 'the miner earned no µROLLA for the batches it finalized');
  assert.equal([...rig.shard(SHARD).books][0], b, 'the rig did not commit to the engine\'s own book');
  assert.equal(miner.metrics().dissents, 0);
  assert.equal(miner.metrics().badSigs, 0);
  // the engine's book and the miner's book are the same book
  const { bookHashOf } = await import('./merkle.js');
  assert.equal(miner.state.bookHash(), bookHashOf(b.m.orders()), 'the miner replayed a different book than the engine matched');

  await miner.stop(); await log.close(); await rig.stop();
  fs.rmSync(dir, { recursive: true, force: true });
});
