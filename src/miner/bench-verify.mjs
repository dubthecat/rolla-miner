// engine/l3/miner/bench-verify.mjs — the verification ceiling of ONE machine: how many signed L3 orders per
// second a miner on this box can check, as a function of worker threads. This is the number that sizes a venue
// (docs/L3-MINERS.md §6): every miner re-verifies every order, so orders/s per shard ≤ this, and cores per
// million orders/s = 1e6 / (per-core rate). Runs anywhere the miner image runs:
//
//   node engine/l3/miner/bench-verify.mjs                       # ladder 1,2,4,… up to cpus-1 workers
//   BENCH_ORDERS=50000 BENCH_WORKERS=8,16,32 node …             # your own ladder
//   PORT=8080 node …                                             # also serve /results (JSON) for an orchestrator
//   BENCH_PROCS=4 BENCH_WORKERS=8,16 node …                      # 4 processes at once, summed per ladder step —
//                                                                  tells a per-process cap (the worker hand-off) from the cores
//
// Orders are signed with libsecp256k1 (RFC 6979, byte-identical to viem) so 100k take seconds; the ed25519 line
// is node:crypto (OpenSSL) on one thread — the per-signature cost ratio secp256k1-recover : ed25519-verify is
// what §9's session-key plan buys.
import os from 'node:os';
import http from 'node:http';
import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createVerifier, orderHash, NATIVE } from './verify.js';
import { BOOK_DOMAIN, l3OrderFor, serializeL3Order } from '../desk.js';

const env = process.env;
const N = Number(env.BENCH_ORDERS || 20000);
const cpus = os.cpus()?.length || 1;
const ladder = env.BENCH_WORKERS ? env.BENCH_WORKERS.split(',').map(Number) : (() => { const l = [1]; for (let w = 2; w <= cpus - 1; w *= 2) l.push(w); if (!l.includes(cpus - 1) && cpus - 1 > 1) l.push(cpus - 1); return l; })();
const PROCS = Number(env.BENCH_PROCS || 1), CHILD = env.BENCH_CHILD === '1';
const results = { machine: `${cpus} vCPU · ${os.cpus()?.[0]?.model || '?'} · node ${process.version}`, native: NATIVE, orders: N, procs: PROCS, ladder: [], multi: [], ed25519: null, done: false, startedAt: Date.now() };
const say = (...a) => { if (!CHILD) console.log(...a); };

if (env.PORT && !CHILD) http.createServer((req, res) => { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(req.url === '/results' ? results : { ok: true, done: results.done })); }).listen(Number(env.PORT), () => say(`serving /results on ${env.PORT}`));

// ---- several processes at once: fork children, release them together, sum each ladder step ----
if (PROCS > 1 && !CHILD) {
  say(results.machine, '· native', JSON.stringify(NATIVE), `· ${PROCS} processes × ${N} orders each`);
  const kids = Array.from({ length: PROCS }, () => fork(fileURLToPath(import.meta.url), [], { env: { ...env, BENCH_CHILD: '1', BENCH_PROCS: '1', PORT: '' }, stdio: ['ignore', 'ignore', 'inherit', 'ipc'] }));
  const rows = new Map(); let signed = 0, finished = 0;
  await new Promise((resolve) => {
    for (const k of kids) k.on('message', (msg) => {
      if (msg.t === 'signed') { if (++signed === kids.length) for (const x of kids) x.send('go'); }
      if (msg.t === 'row') { const r = rows.get(msg.row.workers) || { workers: msg.row.workers, procs: 0, ordersPerSec: 0, ok: 0, ms: 0 }; r.procs++; r.ordersPerSec += msg.row.ordersPerSec; r.ok += msg.row.ok; r.ms = Math.max(r.ms, msg.row.ms); rows.set(msg.row.workers, r); }
      if (msg.t === 'done') { if (++finished === kids.length) resolve(); }
    });
  });
  for (const r of [...rows.values()].sort((a, b) => a.workers - b.workers)) { results.multi.push(r); say(`${r.procs} procs × ${String(r.workers).padStart(2)} workers · ${String(r.ordersPerSec).padStart(7)} orders/s total · ${Math.round(r.ordersPerSec / r.procs)} per process · ok ${r.ok}/${N * r.procs}`); }
  results.done = true; results.ms = Date.now() - results.startedAt;
  say('RESULT', JSON.stringify(results));
  if (!env.PORT) process.exit(0);
} else {

const SECP = createRequire(import.meta.url)('secp256k1');
const h2b = (h) => Uint8Array.from(Buffer.from(h.slice(2), 'hex'));
const fastSign = (keyHex, digestHex) => { const { signature, recid } = SECP.ecdsaSign(h2b(digestHex), h2b(keyHex)); return '0x' + Buffer.from(signature).toString('hex') + (27 + recid).toString(16).padStart(2, '0'); };

say(results.machine, '· native', JSON.stringify(NATIVE));
const domain = BOOK_DOMAIN(46630, '0x7197A5160562516F6f8C4503dF03CD836a524D66'); const E = 10n ** 18n;
const keys = Array.from({ length: 16 }, () => { const key = generatePrivateKey(); return { key, address: privateKeyToAccount(key).address }; });
let t = Date.now(); const items = [];
for (let i = 0; i < N; i++) {
  const k = keys[i % keys.length];
  const o = l3OrderFor({ user: k.address, marketId: 1, outcome: 0, token: '0x' + 'c0'.repeat(20), buy: i % 2 === 0, price: BigInt(40 + (i % 20)) * E / 100n, size: BigInt(1 + (i % 100)) * E, ioc: i % 10 === 0 });
  o.nonce = BigInt(1700000000000 + i); o.salt = BigInt(i) * 7919n + 1n;
  const order = serializeL3Order(o); items.push({ order, signature: fastSign(k.key, orderHash(domain, order)), expect: k.address.toLowerCase() });
}
say(`signed ${N} orders in ${Date.now() - t} ms (${Math.round(N / (Date.now() - t) * 1000)}/s, one thread, libsecp256k1)`);
if (CHILD) { process.send({ t: 'signed' }); await new Promise((r) => process.on('message', (m) => m === 'go' && r())); }

for (const w of ladder) {
  const v = createVerifier({ domain, workers: w, logger: () => {} });
  await v.verifyOrders(items.slice(0, Math.min(500, N)));   // warm the workers (thread start, JIT)
  const t0 = process.hrtime.bigint();
  const out = await v.verifyOrders(items);
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  let ok = 0; for (let i = 0; i < out.length; i++) if (out[i] && out[i].signer === items[i].expect) ok++;
  await v.close();
  const row = { workers: v.workers, ms: Math.round(ms), ordersPerSec: Math.round(N / ms * 1000), perWorker: Math.round(N / ms * 1000 / Math.max(1, v.workers)), usPerOrder: Number((ms * 1000 / N).toFixed(1)), ok };
  results.ladder.push(row); if (CHILD) process.send({ t: 'row', row });
  say(`workers ${String(row.workers).padStart(2)} · ${row.ordersPerSec.toString().padStart(7)} orders/s · ${row.perWorker}/s per worker · ${row.usPerOrder} µs/order wall · ${ok}/${N} ok`);
  if (ok !== N) { say('VERIFICATION MISMATCH'); process.exitCode = 1; }
}

// ed25519 on one thread, for the ratio
{
  const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
  const msgs = []; for (let i = 0; i < 20000; i++) msgs.push(crypto.randomBytes(32));
  const sigs = msgs.map((m) => crypto.sign(null, m, privateKey));
  const t0 = process.hrtime.bigint(); let ok = 0; for (let i = 0; i < msgs.length; i++) if (crypto.verify(null, msgs[i], publicKey, sigs[i])) ok++;
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  const secp1 = results.ladder.find((r) => r.workers === 1) || results.ladder[0];
  results.ed25519 = { perCore: Math.round(msgs.length / ms * 1000), usPerVerify: Number((ms * 1000 / msgs.length).toFixed(1)), ok, ratioVsSecp: secp1 ? Number((secp1.usPerOrder / (ms * 1000 / msgs.length)).toFixed(1)) : null };
  say(`ed25519 (node:crypto, one thread): ${results.ed25519.perCore} verifies/s · ${results.ed25519.usPerVerify} µs each · ${results.ed25519.ratioVsSecp}× cheaper than secp256k1 recover+hash here`);
}
results.done = true; results.ms = Date.now() - results.startedAt;
say('RESULT', JSON.stringify(results));
if (CHILD) { process.send({ t: 'done' }); process.exit(process.exitCode || 0); }
if (!env.PORT) process.exit(process.exitCode || 0);
}
