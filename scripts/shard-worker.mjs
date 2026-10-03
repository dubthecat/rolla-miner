// scripts/shard-worker.mjs — one shard of a multi-shard run, in its own thread: a sequencer against the broker,
// an observer quorum on the shard's votes, and a signed workload. Spawned by runpod-shards.mjs; reports over
// parentPort: { t: 'signed' } → waits for 'go' → { t: 'result', … }.
import { parentPort, workerData } from 'node:worker_threads';
import { createRequire } from 'node:module';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createKafkaLog, ordersTopic, votesTopic } from '../src/miner/log.js';
import { createSequencer } from '../src/miner/sequencer.js';
import { createShardState } from '../src/miner/miner.js';
import { createQuorum } from '../src/miner/quorum.js';
import { orderHash } from '../src/miner/verify.js';
import { BOOK_DOMAIN, l3OrderFor, serializeL3Order } from '../src/desk.js';

const { shard, brokers, orders: N, batch, epoch, threshold, seqKey, book, chainId, seed } = workerData;
const SECP = createRequire(import.meta.url)('secp256k1');
const h2b = (h) => Uint8Array.from(Buffer.from(h.slice(2), 'hex'));
const fastSign = (keyHex, digestHex) => { const { signature, recid } = SECP.ecdsaSign(h2b(digestHex), h2b(keyHex)); return '0x' + Buffer.from(signature).toString('hex') + (27 + recid).toString(16).padStart(2, '0'); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (m) => parentPort.postMessage({ shard, ...m });

const seqAcct = privateKeyToAccount(seqKey); const state = createShardState({});
const sealed = [], finals = [], forks = [], sealedAt = new Map(), finalAt = new Map();
let log = null, seq = null, quorum = null;
// the setup touches freshly created topics; a broker that has not elected their leaders yet refuses the first
// calls, so the whole setup is retried a few times before the shard gives up
for (let attempt = 1; ; attempt++) {
  try {
    log = await createKafkaLog({ brokers: [brokers], clientId: `shard-${shard}`, logger: () => {} });
    seq = createSequencer({ shard, log, account: seqAcct, state, batchMs: 60, batchMax: batch, epochBatches: epoch, onSealed: (b) => { sealed.push(b); sealedAt.set(b.index, Date.now()); }, logger: () => {} });
    await seq.resume();
    quorum = createQuorum({ threshold, onFinal: (f) => { finals.push(f); finalAt.set(f.index, Date.now()); }, onFork: (f) => forks.push(f), logger: () => {} });
    await log.subscribe(ordersTopic(shard), 0, async ({ value }) => { quorum.announce(value); });
    await log.subscribe(votesTopic(shard), 0, async ({ value }) => { await quorum.vote(value); });
    break;
  } catch (e) {
    if (attempt >= 6) throw e;
    post({ t: 'retry', attempt, error: e.message.slice(0, 100) }); try { await log?.close?.(); } catch {}
    await sleep(3000);
  }
}

// the workload: matcher.test.mjs's shape, eight traders, signed with libsecp256k1
const domain = BOOK_DOMAIN(chainId, book); const E = 10n ** 18n; const token = '0x' + 'c0'.repeat(20); const MARKET = Number(shard);
const traders = Array.from({ length: 8 }, () => { const key = generatePrivateKey(); return { key, address: privateKeyToAccount(key).address }; });
let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296; const live = [], plan = [];
for (let i = 0; i < N; i++) {
  if (live.length && rnd() < 0.1) { plan.push({ t: 'cancel', target: live.splice(Math.floor(rnd() * live.length), 1)[0] }); continue; }
  const buy = rnd() < 0.5, cross = rnd() < 0.3, mid = 50, off = Math.floor(rnd() * 20);
  const cents = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
  const tr = traders[Math.floor(rnd() * traders.length)];
  const order = l3OrderFor({ user: tr.address, marketId: MARKET, outcome: 0, token, buy, price: BigInt(cents) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 100)) * E, ioc: rnd() < 0.1 });
  order.nonce = BigInt(1700000000000 + i); order.salt = BigInt(i) * 7919n + 1n;
  plan.push({ t: 'add', key: tr.key, signer: tr.address.toLowerCase(), order }); if (!order.ioc) live.push(plan.length - 1);
}
const t0 = Date.now();
for (const p of plan) if (p.t === 'add') { const order = serializeL3Order(p.order); const hash = orderHash(domain, order); p.op = { t: 'add', hash, market: MARKET, outcome: 0, order, sig: fastSign(p.key, hash), signer: p.signer, at: Date.now() }; }
for (const p of plan) if (p.t === 'cancel') p.op = { t: 'cancel', hash: plan[p.target].op.hash, market: MARKET, outcome: 0 };
const ops = plan.map((p) => p.op);
post({ t: 'signed', ops: ops.length, ms: Date.now() - t0 });
await new Promise((r) => parentPort.on('message', (m) => m === 'go' && r()));

const a = Date.now(); const first = seq.index;
for (let i = 0; i < ops.length; i++) { seq.submit(ops[i]); if (i % 25 === 0) await new Promise((r) => setImmediate(r)); }
await seq.flush(); const seqMs = Date.now() - a; const last = seq.index - 1;
const deadline = Date.now() + 300000; while (Date.now() < deadline && quorum.finalIndex < last) await sleep(50);
const clusterMs = Date.now() - a;
const lat = sealed.filter((b) => b.index >= first && b.index <= last).map((b) => (finalAt.get(b.index) || 0) - sealedAt.get(b.index)).filter((x) => x > 0).sort((x, y) => x - y);
const p = (q) => (lat.length ? lat[Math.min(lat.length - 1, Math.floor(q * lat.length))] : null);
const st = seq.status();
post({ t: 'result', ops: ops.length, batches: last - first + 1, seqMs, clusterMs, finalIndex: quorum.finalIndex, last, p50: p(0.5), p90: p(0.9), max: lat[lat.length - 1] ?? null, forks: forks.length, halted: !!quorum.halted,
       perBatchMs: st.perBatchMs, rootMsPerBatch: st.batches ? Number((st.rootMs / st.batches).toFixed(1)) : 0, failed: st.failed, retry: st.retry, bookHash: state.bookHash() });
await seq.stop(); try { await log.close?.(); } catch {}
process.exit(0);
