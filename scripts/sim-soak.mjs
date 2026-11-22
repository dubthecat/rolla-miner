// scripts/sim-soak.mjs — a venue cell under SUSTAINED load, for as long as you like.
//
// Every benchmark before this one was a burst: sign N orders, release them together, measure the wall. That
// answers "how fast can it go" and says nothing about "can it stay there", which is the only question that
// matters for a venue that is supposed to run. A burst hides everything that accumulates — the resting book, the
// miners' journals, the broker's log, the heap, the finality backlog — and all of those are what actually ends a
// venue's day.
//
// So this one runs for SOAK_SECONDS and keeps the order flow coming, and it does one more thing: it FINDS the
// sustainable rate instead of being told it. Each shard watches the gap between the last batch it sealed and the
// last one the quorum finalised and steers its own submit rate by AIMD — up a little while finality keeps pace,
// down hard when it falls behind. Left alone for half an hour the rate settles at what the cell can actually
// hold, which is the number worth having. `SOAK_RATE` pins it instead, for measuring a known load.
//
// The book is kept BOUNDED on purpose. Under L3_COMMIT=bookhash every epoch boundary hashes the whole resting
// set, so a workload that only ever adds orders turns the epoch commitment into the wall and measures that
// instead of the venue — 9 million orders in half an hour would leave a resting set no commitment scheme
// survives. The generator therefore steers towards SOAK_RESTING: past it, it cancels and crosses rather than
// rests, which is also what a liquid market looks like.
//
//   SOAK_SECONDS=1800 SOAK_SHARDS=2 L3_KAFKA_BROKERS=broker.internal:9092 SOAK_REPLICAS=3 node scripts/sim-soak.mjs
//   GET /status · GET /results · GET /metrics · GET /log · GET /series
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { createRequire } from 'node:module';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createLog, ordersTopic, votesTopic } from '../src/miner/log.js';
import { createSequencer } from '../src/miner/sequencer.js';
import { createShardState } from '../src/miner/miner.js';
import { createQuorum } from '../src/miner/quorum.js';
import { orderHash } from '../src/miner/verify.js';
import { BOOK_DOMAIN, l3OrderFor, serializeL3Order } from '../src/desk.js';

const ROLE = 'l3-soak';
const env = process.env;
const here = path.dirname(fileURLToPath(import.meta.url));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pct = (a, q) => (a.length ? a[Math.min(a.length - 1, Math.floor(q * a.length))] : null);

// ------------------------------------------------------------------------------------------------ the worker
// One shard: a sequencer against the broker, an observer quorum on the shard's votes, and a generator that signs
// its own order flow and steers its rate by how finality is keeping up.
if (!isMainThread && workerData && workerData.role === ROLE) {
  const { shard, brokers, batch, epoch, threshold, seqKey, book, chainId, seed, seconds, rate, restingTarget, rateMin, rateMax } = workerData;
  const SECP = createRequire(import.meta.url)('secp256k1');
  const h2b = (h) => Uint8Array.from(Buffer.from(h.slice(2), 'hex'));
  const fastSign = (keyHex, digestHex) => { const { signature, recid } = SECP.ecdsaSign(h2b(digestHex), h2b(keyHex)); return '0x' + Buffer.from(signature).toString('hex') + (27 + recid).toString(16).padStart(2, '0'); };
  const post = (m) => parentPort.postMessage({ shard, ...m });

  const seqAcct = privateKeyToAccount(seqKey);
  const state = createShardState({});
  const sealedAt = new Map(), finalAt = new Map();
  const lat = [];                                  // finality latencies, this sample window
  let sealedIndex = -1, forks = 0, dissents = 0;
  let log = null, seq = null, quorum = null;

  for (let attempt = 1; ; attempt++) {
    try {
      // env-driven so the same harness runs on a file log locally (L3_LOG=file) and on the broker in a cell
      log = await createLog({ env: { ...env, L3_KAFKA_BROKERS: brokers }, clientId: `soak-${shard}`, logger: () => { } });
      seq = createSequencer({ shard, log, account: seqAcct, state, batchMs: 60, batchMax: batch, epochBatches: epoch,
        onSealed: (b) => { sealedIndex = b.index; sealedAt.set(b.index, Date.now()); }, logger: () => { } });
      await seq.resume();
      quorum = createQuorum({ threshold, onFinal: (f) => { finalAt.set(f.index, Date.now()); const s = sealedAt.get(f.index); if (s) { lat.push(Date.now() - s); sealedAt.delete(f.index); } }, onFork: () => { forks++; }, logger: () => { } });
      await log.subscribe(ordersTopic(shard), 0, async ({ value }) => { quorum.announce(value); });
      await log.subscribe(votesTopic(shard), 0, async ({ value }) => { if (value && value.ok === false) dissents++; await quorum.vote(value); });
      break;
    } catch (e) {
      if (attempt >= 6) throw e;
      post({ t: 'retry', attempt, error: e.message.slice(0, 120) });
      try { await log?.close?.(); } catch { }
      await sleep(3000);
    }
  }

  // the traders and the EIP-712 domain their orders are signed under
  const domain = BOOK_DOMAIN(chainId, book), E = 10n ** 18n, token = '0x' + 'c0'.repeat(20), MARKET = Number(shard);
  const traders = Array.from({ length: 8 }, () => { const key = generatePrivateKey(); return { key, address: privateKeyToAccount(key).address }; });
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const live = [];                                 // hashes we believe are resting, for cancels
  let nonce = 1700000000000, made = 0;

  /// one signed op. The mix is steered by how much is resting: at or over the target it cancels and crosses, so
  /// the resting set — and therefore the epoch commitment — stays the size we asked for.
  function nextOp() {
    const resting = state.stat().resting;
    const over = resting >= restingTarget;
    if (live.length && (over ? rnd() < 0.45 : rnd() < 0.10)) {
      const i = Math.floor(rnd() * live.length), hash = live[i];
      live[i] = live[live.length - 1]; live.pop();
      return { t: 'cancel', hash, market: MARKET, outcome: 0 };
    }
    const buy = rnd() < 0.5;
    const cross = over ? rnd() < 0.85 : rnd() < 0.35;      // over the target, mostly take liquidity away
    const mid = 50, off = Math.floor(rnd() * 20);
    const cents = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
    const tr = traders[Math.floor(rnd() * traders.length)];
    const order = l3OrderFor({ user: tr.address, marketId: MARKET, outcome: 0, token, buy,
      price: BigInt(Math.max(1, Math.min(99, cents))) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 50)) * E, ioc: rnd() < 0.05 });
    order.nonce = BigInt(++nonce); order.salt = BigInt(++made) * 7919n + 1n;
    const ser = serializeL3Order(order);
    const hash = orderHash(domain, ser);
    if (!order.ioc && !cross) live.push(hash);
    if (live.length > 200000) live.splice(0, 100000);       // the cancel pool is a sample, not a ledger
    return { t: 'add', hash, market: MARKET, outcome: 0, order: ser, sig: fastSign(tr.key, hash), signer: tr.address.toLowerCase(), at: Date.now() };
  }

  // ---- the loop. A tick every 20 ms submits its share of the current rate; the rate itself is steered by the
  // finality backlog, because a venue that seals faster than it finalises is not running, it is falling behind.
  const TICK = 20;
  let perSec = rate > 0 ? rate : rateMin;
  const fixed = rate > 0;
  let submitted = 0, carry = 0;
  const t0 = Date.now(), until = t0 + seconds * 1000;
  let lastSample = t0, sampleOps = 0;

  while (Date.now() < until) {
    const want = perSec * (TICK / 1000) + carry;
    const n = Math.floor(want); carry = want - n;
    for (let i = 0; i < n; i++) { seq.submit(nextOp()); submitted++; sampleOps++; }
    await sleep(TICK);

    const now = Date.now();
    if (now - lastSample >= 1000) {
      const backlog = Math.max(0, sealedIndex - quorum.finalIndex);
      if (!fixed) {
        // AIMD: a backlog of a couple of batches is normal pipelining; beyond that finality is losing and the
        // rate comes down hard. Below it, climb slowly — the point is to settle, not to oscillate.
        if (backlog > 8) perSec = Math.max(rateMin, Math.floor(perSec * 0.75));
        else if (backlog <= 3) perSec = Math.min(rateMax, perSec + Math.max(50, Math.floor(perSec * 0.05)));
      }
      const l = [...lat].sort((a, b) => a - b); lat.length = 0;
      post({ t: 'sample', at: now, elapsed: (now - t0) / 1000, ops: sampleOps, rate: Math.round(sampleOps / ((now - lastSample) / 1000)),
             target: perSec, submitted, sealedIndex, finalIndex: quorum.finalIndex, backlog, resting: state.stat().resting,
             p50: pct(l, 0.5), p90: pct(l, 0.9), finals: l.length, forks, dissents, halted: !!quorum.halted, rss: Math.round(process.memoryUsage().rss / 1e6) });
      sampleOps = 0; lastSample = now;
    }
  }

  // drain: stop submitting and give finality a chance to catch up, so the run ends with a true tail
  await seq.flush();
  const drainUntil = Date.now() + 120000;
  while (Date.now() < drainUntil && quorum.finalIndex < sealedIndex) await sleep(100);
  const st = seq.status();
  post({ t: 'done', submitted, sealedIndex, finalIndex: quorum.finalIndex, backlog: Math.max(0, sealedIndex - quorum.finalIndex),
         resting: state.stat().resting, forks, dissents, halted: !!quorum.halted, batches: st.batches,
         perBatchMs: st.perBatchMs, rootMsPerBatch: st.batches ? Number((st.rootMs / st.batches).toFixed(1)) : 0, failed: st.failed, retry: st.retry,
         applied: state.stat().applied, fills: state.stat().fills, cancels: state.stat().cancels, bookHash: state.bookHash() });
  try { await seq.stop(); } catch { }
  try { await log.close?.(); } catch { }
  process.exit(0);
}

// -------------------------------------------------------------------------------------------------- the host
const { spawnMiners, minersHealth, minersMetrics } = await import('./sim-miners.mjs');

const SECONDS = Number(env.SOAK_SECONDS || 1800);
const S = Number(env.SOAK_SHARDS || 2), BASE = Number(env.SOAK_BASE || (900000 + Math.floor(Math.random() * 90000)));
const SHARDS = env.SOAK_SHARD_LIST ? env.SOAK_SHARD_LIST.split(',').filter(Boolean) : Array.from({ length: S }, (_, i) => String(BASE + i));
const BATCH = Number(env.SOAK_BATCH || 500), EPOCH = Number(env.SOAK_EPOCH || 20);
const R = Number(env.SOAK_REPLICAS || 3), THRESHOLD = Number(env.SOAK_THRESHOLD || Math.max(1, R - 1)), WORKERS = Number(env.SOAK_WORKERS || 1);
const RATE = Number(env.SOAK_RATE || 0);                       // 0 = find it
const RATE_MIN = Number(env.SOAK_RATE_MIN || 200), RATE_MAX = Number(env.SOAK_RATE_MAX || 40000);
const RESTING = Number(env.SOAK_RESTING || 4000);
const brokers = env.L3_KAFKA_BROKERS || '127.0.0.1:9092';
const seqKey = env.SOAK_SEQ_KEY || generatePrivateKey(), seqAddr = privateKeyToAccount(seqKey).address;
const BOOK = env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66', CHAIN_ID = Number(env.CHAIN_ID || 46630);

const startedAt = Date.now();
const state = { seqAddress: seqAddr, shards: SHARDS, seconds: SECONDS, replicas: R, threshold: THRESHOLD, restingTarget: RESTING,
                rate: RATE || 'adaptive', running: false, done: false, error: null, log: [], series: [], latest: {}, results: {}, miners: null };
const say = (...a) => { const l = `+${((Date.now() - startedAt) / 1000).toFixed(0)}s ${a.join(' ')}`; console.log(l); state.log.push(l); if (state.log.length > 400) state.log.shift(); };
let localMiners = [], workers = [];

/// every shard's latest sample, rolled into one line a second — the shape a soak is actually read in
function rollup() {
  const rows = Object.values(state.latest);
  if (!rows.length) return null;
  const sum = (k) => rows.reduce((a, r) => a + (Number(r[k]) || 0), 0);
  const p50s = rows.map((r) => r.p50).filter((x) => x != null).sort((a, b) => a - b);
  const p90s = rows.map((r) => r.p90).filter((x) => x != null).sort((a, b) => a - b);
  return { at: Date.now(), elapsed: Math.round((Date.now() - startedAt) / 1000), shards: rows.length,
           rate: sum('rate'), target: sum('target'), submitted: sum('submitted'), backlog: sum('backlog'),
           resting: sum('resting'), finals: sum('finals'), forks: sum('forks'), dissents: sum('dissents'),
           halted: rows.filter((r) => r.halted).length, p50: pct(p50s, 0.5), p90: p90s.length ? p90s[p90s.length - 1] : null,
           rssMax: Math.max(...rows.map((r) => r.rss || 0)) };
}

async function main() {
  say(`[soak] ${SHARDS.length} shard(s) ${SHARDS[0]}…${SHARDS[SHARDS.length - 1]} · ${SECONDS}s · ${R} miner(s)/shard threshold ${THRESHOLD} · rate ${RATE || 'adaptive'} · resting target ${RESTING} · broker ${brokers}`);
  say(`[soak] sequencer ${seqAddr}`);
  for (let r = 0; r < R; r++) {
    const keys = SHARDS.map(() => generatePrivateKey());
    // a file log is how this runs on one box with no broker at all; on a cell it is kafka, the default
    const extra = (env.L3_LOG || '').toLowerCase() === 'file' ? { L3_LOG: 'file', L3_LOG_DIR: env.L3_LOG_DIR || '' } : {};
    const ms = spawnMiners({ shards: SHARDS, keys, brokers, sequencers: seqAddr, threshold: THRESHOLD, workers: WORKERS, extra,
      basePort: 9100 + r * 100, dataDir: path.join(env.DATA_DIR || '/data', `r${r + 1}`), onLog: (l) => say(l) });
    localMiners.push(...ms.map((m) => ({ ...m, replica: r + 1 })));
  }
  if (localMiners.length) {
    say(`[soak] ${localMiners.length} miner process(es) · ${WORKERS} verify worker(s) each — waiting for health`);
    const deadline = Date.now() + 300000; let ok = false, last = null;
    while (Date.now() < deadline) { const h = await minersHealth(localMiners, { timing: false }); last = h; if (h.length && h.every((m) => m.ok)) { ok = true; break; } await sleep(4000); }
    if (!ok) throw new Error(`miners never all healthy: ${(last || []).filter((m) => !m.ok).slice(0, 3).map((m) => `${m.shard}: ${m.error || m.stalled || 'not ok'}`).join('; ')}`);
    say('[soak] all miners healthy');
  }
  state.running = true; state.goAt = Date.now();
  workers = SHARDS.map((shard, i) => new Worker(fileURLToPath(import.meta.url), {
    workerData: { role: ROLE, shard, brokers, batch: BATCH, epoch: EPOCH, threshold: THRESHOLD, seqKey, book: BOOK, chainId: CHAIN_ID,
                  seed: 101 + i, seconds: SECONDS, rate: RATE, restingTarget: RESTING, rateMin: RATE_MIN, rateMax: RATE_MAX },
  }));
  for (const w of workers) {
    w.on('message', (m) => {
      if (m.t === 'sample') { state.latest[m.shard] = m; }
      if (m.t === 'retry') say(`[soak] shard ${m.shard} retrying setup (${m.attempt}): ${m.error}`);
      if (m.t === 'done') { state.results[m.shard] = m; if (Object.keys(state.results).length === SHARDS.length) finish(); }
    });
    w.on('error', (e) => { say(`[soak] shard thread error: ${e.message}`); state.error = e.message; });
  }
  // one rolled-up line a second into the series, and a readable one every 30
  setInterval(() => {
    const r = rollup(); if (!r) return;
    state.series.push(r); if (state.series.length > 4000) state.series.shift();
    if (r.elapsed % 30 === 0) say(`[soak] ${r.elapsed}s · ${r.rate.toLocaleString()}/s (target ${r.target.toLocaleString()}) · backlog ${r.backlog} · final p50 ${r.p50}ms p90 ${r.p90}ms · resting ${r.resting.toLocaleString()} · forks ${r.forks} · dissents ${r.dissents} · halted ${r.halted} · rss ${r.rssMax}MB`);
  }, 1000);
}

/// the numbers the run is for: what it HELD, not what it peaked at
function summary() {
  const ser = state.series.filter((r) => r.elapsed > 60);        // drop the first minute: miners are still warming
  if (!ser.length) return null;
  const rates = ser.map((r) => r.rate).sort((a, b) => a - b);
  const rs = Object.values(state.results);
  const p50s = ser.map((r) => r.p50).filter((x) => x != null).sort((a, b) => a - b);
  const submitted = rs.length ? rs.reduce((a, r) => a + r.submitted, 0) : (ser[ser.length - 1].submitted || 0);
  return {
    seconds: ser[ser.length - 1].elapsed, shards: SHARDS.length, replicas: R, threshold: THRESHOLD,
    submitted, sustainedMedian: pct(rates, 0.5), sustainedP10: pct(rates, 0.1), peak: rates[rates.length - 1],
    finalityP50: pct(p50s, 0.5), finalityP90: pct(p50s, 0.9),
    backlogMax: Math.max(...ser.map((r) => r.backlog)), backlogEnd: ser[ser.length - 1].backlog,
    restingMax: Math.max(...ser.map((r) => r.resting)), restingEnd: ser[ser.length - 1].resting,
    rssMax: Math.max(...ser.map((r) => r.rssMax)),
    forks: rs.reduce((a, r) => a + r.forks, 0), dissents: rs.reduce((a, r) => a + r.dissents, 0),
    halted: rs.filter((r) => r.halted).length,
    allCaughtUp: rs.length === SHARDS.length && rs.every((r) => r.finalIndex >= r.sealedIndex && !r.halted && r.forks === 0),
    perBatchMs: rs.length ? Number((rs.reduce((a, r) => a + (r.perBatchMs || 0), 0) / rs.length).toFixed(1)) : null,
    rootMsPerBatch: rs.length ? Number((rs.reduce((a, r) => a + (r.rootMsPerBatch || 0), 0) / rs.length).toFixed(1)) : null,
  };
}

async function finish() {
  state.running = false; state.done = true;
  state.summary = summary();
  try { state.miners = await minersHealth(localMiners); } catch { }
  const a = state.summary;
  if (a) {
    say(`[soak] DONE ${a.seconds}s · ${a.submitted.toLocaleString()} orders · HELD ${a.sustainedMedian.toLocaleString()}/s (p10 ${a.sustainedP10.toLocaleString()}, peak ${a.peak.toLocaleString()})`);
    say(`[soak] finality p50 ${a.finalityP50}ms p90 ${a.finalityP90}ms · backlog max ${a.backlogMax} end ${a.backlogEnd} · resting max ${a.restingMax.toLocaleString()}`);
    say(`[soak] forks ${a.forks} · dissents ${a.dissents} · halted ${a.halted} · all caught up ${a.allCaughtUp} · rss max ${a.rssMax}MB · seal ${a.perBatchMs}ms (roots ${a.rootMsPerBatch})`);
  }
  for (const m of localMiners) { m.stopped = true; try { m.child && m.child.kill('SIGTERM'); } catch { } }
}

http.createServer(async (req, res) => {
  const j = (code, o) => { res.statusCode = code; res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(o)); };
  try {
    if (req.url === '/results') return j(200, { done: state.done, summary: state.done ? state.summary : summary(), results: state.results, miners: state.miners, error: state.error, now: rollup() });
    if (req.url === '/series') return j(200, { series: state.series });
    if (req.url === '/metrics') { res.setHeader('content-type', 'text/plain; version=0.0.4'); return res.end(await minersMetrics(localMiners)); }
    if (req.url === '/log') { res.setHeader('content-type', 'text/plain'); return res.end(state.log.join('\n')); }
    return j(200, { ok: !state.error, seqAddress: seqAddr, shards: SHARDS, running: state.running, done: state.done, error: state.error,
                    uptime: (Date.now() - startedAt) / 1000, now: rollup(), miners: localMiners.length ? (state.miners || null) : null });
  } catch (e) { j(500, { error: e.message }); }
}).listen(Number(env.PORT || 8080), () => say(`[soak] serving on ${env.PORT || 8080}`));

main().catch((e) => { state.error = e.message; say(`[soak] FAILED ${e.stack || e.message}`); });
