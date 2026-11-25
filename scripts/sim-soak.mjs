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
// SOAK_FLOW picks what the order flow IS, and it is the difference between a load test and a venue test.
//
//   SOAK_FLOW=makers (default)  the venue's own books: M maker clients per market quoting with src/quote-math.js —
//                               the same arithmetic engine/mm/strategies/l3-quoter.js runs live — around a fair
//                               that moves, re-quoting when it moves a tick, skewed by the inventory the matcher
//                               actually gave them, with takers (some informed) crossing the touch. The resting
//                               set is the makers' ladders: makers × outcomes × 2 × levels, bounded by design.
//   SOAK_FLOW=noise             the original random generator around a fixed mid, kept so every number measured
//                               before 2026-11-25 is still comparable.
//
// `SOAK_BOOST` is the one dial for more volume: it scales makers and levels together, because more takers alone
// empties the book and more makers alone never trade.
//
//   SOAK_SECONDS=1800 SOAK_SHARDS=2 L3_KAFKA_BROKERS=broker.internal:9092 SOAK_REPLICAS=3 node scripts/sim-soak.mjs
//   SOAK_FLOW=makers SOAK_BOOST=4 SOAK_MAKERS=3 SOAK_LEVELS=3 SOAK_TAKERS=0.4 node scripts/sim-soak.mjs
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
import { createFlow } from '../src/miner/flow.js';
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
  const { shard, brokers, batch, epoch, threshold, seqKey, book, chainId, seed, seconds, rate, restingTarget, rateMin, rateMax, mid: MID, spread: SPREAD, flow: FLOWCFG } = workerData;
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

  // The flow. In makers mode each maker gets its own key, because the inventory that skews a maker's quote is
  // per-user in the book and would be meaningless pooled; the takers get four more.
  const flow = FLOWCFG && FLOWCFG.kind === 'makers' ? createFlow({
    market: MARKET, outcomes: FLOWCFG.outcomes, fair: FLOWCFG.outcomes === 2 ? [MID / 100, 1 - MID / 100] : null,
    confidence: FLOWCFG.confidence, makers: FLOWCFG.makers, levels: FLOWCFG.levels, boost: FLOWCFG.boost,
    size: FLOWCFG.size, takeSize: FLOWCFG.takeSize, takers: FLOWCFG.takers, informed: FLOWCFG.informed,
    volPerSec: FLOWCFG.vol, tick: FLOWCFG.tick, requoteMs: FLOWCFG.requoteMs, seed,
  }) : null;
  const traders = Array.from({ length: flow ? flow.makers + 4 : 8 }, () => { const key = generatePrivateKey(); return { key, address: privateKeyToAccount(key).address }; });
  const units = (x) => BigInt(Math.max(0, Math.round(Number(x) * 1e6))) * 10n ** 12n;
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
    // Quote AROUND THIS MARKET'S FAIR PRICE rather than around 50. A workload centred on 50 produces books that
    // look nothing like the venue's — every market priced the same, half of them nowhere near their own fair —
    // and it hides the thing that actually costs: a book whose resting set clusters a cent or two from the mid,
    // which is what the maker builds and what the epoch commitment has to hash.
    const mid = MID, half = Math.max(1, SPREAD);
    const off = 1 + Math.floor(rnd() * half * 3);          // depth: most of it near the touch, a tail further out
    const cents = buy ? (cross ? mid + Math.ceil(off / 3) : mid - off) : (cross ? mid - Math.ceil(off / 3) : mid + off);
    const tr = traders[Math.floor(rnd() * traders.length)];
    const order = l3OrderFor({ user: tr.address, marketId: MARKET, outcome: 0, token, buy,
      price: BigInt(Math.max(1, Math.min(99, Math.round(cents)))) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 50)) * E, ioc: rnd() < 0.05 });
    order.nonce = BigInt(++nonce); order.salt = BigInt(++made) * 7919n + 1n;
    const ser = serializeL3Order(order);
    const hash = orderHash(domain, ser);
    if (!order.ioc && !cross) live.push(hash);
    if (live.length > 200000) live.splice(0, 100000);       // the cancel pool is a sample, not a ledger
    return { t: 'add', hash, market: MARKET, outcome: 0, order: ser, sig: fastSign(tr.key, hash), signer: tr.address.toLowerCase(), at: Date.now() };
  }

  /// one op from the maker/taker flow: the intent decides everything about the order, this signs it. The hash is
  /// handed back with acked() so the flow can cancel its own quote later — it cannot know it before the signer.
  function makerOp() {
    const it = flow.next(Date.now());
    if (!it) return null;
    if (it.t === 'cancel') return { t: 'cancel', hash: it.hash, market: MARKET, outcome: it.outcome };
    const tr = traders[it.userIndex % traders.length];
    const order = l3OrderFor({ user: tr.address, marketId: MARKET, outcome: it.outcome, token, buy: it.buy,
      price: units(it.price), size: units(it.size), ioc: it.ioc, postOnly: it.postOnly });
    order.nonce = BigInt(++nonce); order.salt = BigInt(++made) * 7919n + 1n;
    const ser = serializeL3Order(order);
    const hash = orderHash(domain, ser);
    flow.acked(it.ref, hash);
    return { t: 'add', hash, market: MARKET, outcome: it.outcome, order: ser, sig: fastSign(tr.key, hash), signer: tr.address.toLowerCase(), at: Date.now() };
  }
  const anOp = flow ? makerOp : nextOp;

  /// reconcile the flow against the shard's real books, once a second. This is where fills enter the maker's
  /// inventory — from the matcher, not from a model of it.
  function syncFlow() {
    if (!flow) return;
    const books = [];
    for (let o = 0; o < flow.outcomes; o++) books.push(state.bookFor(MARKET, o).orders());
    flow.sync(books);
  }

  // ---- the loop. A tick every 20 ms submits its share of the current rate; the rate itself is steered by the
  // finality backlog, because a venue that seals faster than it finalises is not running, it is falling behind.
  const TICK = 20;
  let perSec = rate > 0 ? rate : rateMin;
  const fixed = rate > 0;
  let submitted = 0, carry = 0, lastBacklog = 0, lastFinal = -1;
  const t0 = Date.now(), until = t0 + seconds * 1000;
  let lastSample = t0, sampleOps = 0;

  while (Date.now() < until) {
    const want = perSec * (TICK / 1000) + carry;
    const n = Math.floor(want); carry = want - n;
    for (let i = 0; i < n; i++) { const op = anOp(); if (!op) break; seq.submit(op); submitted++; sampleOps++; }
    await sleep(TICK);

    const now = Date.now();
    if (now - lastSample >= 1000) {
      const backlog = Math.max(0, sealedIndex - quorum.finalIndex);
      // what finality actually achieved since the last sample — the only honest ceiling for the submit rate
      const finalRate = lastFinal >= 0 ? Math.max(0, (quorum.finalIndex - lastFinal)) * batch / ((now - lastSample) / 1000) : 0;
      if (!fixed) {
        // Steer towards the FINALITY rate, not away from a backlog threshold. The first version of this was a
        // plain AIMD on the backlog and it oscillated hard: it climbed until finality broke, then cut the rate
        // multiplicatively while the backlog it had already built kept the condition true, so it sat on the floor
        // for minutes and reported a rate the venue never had trouble with. Submitting faster than the venue
        // finalises is what builds a queue, so the queue's DERIVATIVE says whether we are over, and the measured
        // finality rate says by how much.
        const growing = backlog > lastBacklog;
        if (backlog <= 3) perSec = Math.min(rateMax, perSec + Math.max(50, Math.floor(perSec * 0.05)));
        else if (growing) perSec = Math.max(rateMin, Math.min(Math.floor(perSec * 0.9), Math.floor((finalRate || perSec) * 0.95)));
        // a backlog that is large but shrinking needs no change: it is already draining at the right rate
      }
      lastBacklog = backlog; lastFinal = quorum.finalIndex;
      syncFlow();
      // What the venue kept up with, not only whether it kept up: the touch spread, the depth at it, and how far
      // the mid sits from the fair the makers quote around. A fast book that is not a market is not a result.
      const fs = flow ? flow.stat() : null;
      const l = [...lat].sort((a, b) => a - b); lat.length = 0;
      post({ t: 'sample', at: now, elapsed: (now - t0) / 1000, ops: sampleOps, rate: Math.round(sampleOps / ((now - lastSample) / 1000)),
             target: perSec, submitted, sealedIndex, finalIndex: quorum.finalIndex, backlog, finalRate: Math.round(finalRate), resting: state.stat().resting,
             p50: pct(l, 0.5), p90: pct(l, 0.9), finals: l.length, forks, dissents, halted: !!quorum.halted, rss: Math.round(process.memoryUsage().rss / 1e6),
             spreadCents: fs ? fs.spreadCents : null, fairDevCents: fs ? fs.fairDevCents : null, depthAtTouch: fs ? fs.depthAtTouch : null,
             quotes: fs ? fs.quotes : null, requoted: fs ? fs.requoted : null, takes: fs ? fs.taken : null,
             makerFills: fs ? fs.fills : null, inventoryMax: fs ? fs.inventoryMax : null, fairP0: fs ? fs.fair[0] : null });
      sampleOps = 0; lastSample = now;
    }
  }

  // drain: stop submitting and give finality a chance to catch up, so the run ends with a true tail
  await seq.flush();
  const drainUntil = Date.now() + 120000;
  while (Date.now() < drainUntil && quorum.finalIndex < sealedIndex) await sleep(100);
  syncFlow();
  const st = seq.status();
  post({ t: 'done', submitted, sealedIndex, finalIndex: quorum.finalIndex, backlog: Math.max(0, sealedIndex - quorum.finalIndex),
         flow: flow ? flow.stat() : null,
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
// SOAK_MIDS is one fair price in cents per shard, in the same order as the shard list — scripts/market-snapshot.mjs
// produces both from a live engine, so a soak can be run against the real open markets at their real fair prices.
// A single value applies to every shard; missing or unparseable falls back to 50.
const MIDS = String(env.SOAK_MIDS || '').split(',').map((x) => Number(x)).filter((x) => Number.isFinite(x) && x > 0 && x < 100);
const SPREAD = Number(env.SOAK_SPREAD || 3);
const midFor = (i) => (MIDS.length === 0 ? 50 : MIDS.length === 1 ? MIDS[0] : (MIDS[i] ?? MIDS[MIDS.length - 1]));
// the order flow. Defaults are the LIVE maker's: tick 1c, half-spread floor 1.5c, quote size 12, re-quote at
// 45 s — engine/mm/strategies/l3-quoter.js's own configuration, so a soak book is the venue's book.
const FLOW = String(env.SOAK_FLOW || 'makers').toLowerCase() === 'noise' ? null : {
  kind: 'makers',
  outcomes: Number(env.SOAK_OUTCOMES || 2),
  makers: Number(env.SOAK_MAKERS || 3),
  levels: Number(env.SOAK_LEVELS || 3),
  boost: Number(env.SOAK_BOOST || 1),
  size: Number(env.SOAK_SIZE || 12),
  takeSize: Number(env.SOAK_TAKE_SIZE || 3),
  takers: Number(env.SOAK_TAKERS || 0.35),
  informed: Number(env.SOAK_INFORMED || 0.2),
  vol: Number(env.SOAK_VOL || 0.02),
  tick: Number(env.SOAK_TICK || 0.01),
  requoteMs: Number(env.SOAK_REQUOTE_MS || 45000),
  confidence: Number(env.SOAK_CONF || 0.8),
};
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
  const med = (k) => { const v = rows.map((r) => Number(r[k])).filter((x) => Number.isFinite(x)).sort((a, b) => a - b); return v.length ? Number(pct(v, 0.5).toFixed(2)) : null; };
  const p50s = rows.map((r) => r.p50).filter((x) => x != null).sort((a, b) => a - b);
  const p90s = rows.map((r) => r.p90).filter((x) => x != null).sort((a, b) => a - b);
  return { at: Date.now(), elapsed: Math.round((Date.now() - startedAt) / 1000), shards: rows.length,
           rate: sum('rate'), target: sum('target'), submitted: sum('submitted'), backlog: sum('backlog'),
           resting: sum('resting'), finals: sum('finals'), forks: sum('forks'), dissents: sum('dissents'),
           halted: rows.filter((r) => r.halted).length, p50: pct(p50s, 0.5), p90: p90s.length ? p90s[p90s.length - 1] : null,
           rssMax: Math.max(...rows.map((r) => r.rss || 0)),
           // the book, across the venue: a median rather than a sum, because a spread is not additive
           spreadCents: med('spreadCents'), fairDevCents: med('fairDevCents'), depthAtTouch: med('depthAtTouch'),
           quotes: sum('quotes'), requoted: sum('requoted'), takes: sum('takes'), makerFills: sum('makerFills'),
           inventoryMax: Math.max(0, ...rows.map((r) => Number(r.inventoryMax) || 0)) };
}

async function main() {
  say(`[soak] ${SHARDS.length} shard(s) ${SHARDS[0]}…${SHARDS[SHARDS.length - 1]} · ${SECONDS}s · ${R} miner(s)/shard threshold ${THRESHOLD} · rate ${RATE || 'adaptive'} · resting target ${RESTING} · broker ${brokers}`);
  say(`[soak] books quoted around ${MIDS.length ? (MIDS.length === 1 ? MIDS[0] + 'c for every shard' : MIDS.length + ' per-shard fair price(s), ' + midFor(0) + 'c…' + midFor(SHARDS.length - 1) + 'c') : '50c (no SOAK_MIDS given)'} ± ${SPREAD}c`);
  say(FLOW
    ? `[soak] flow MAKERS · ${FLOW.makers}×${FLOW.levels} grid boost ${FLOW.boost} → ${Math.max(1, Math.round(FLOW.makers * Math.sqrt(FLOW.boost)))} makers × ${Math.max(1, Math.round(FLOW.levels * Math.sqrt(FLOW.boost)))} levels × ${FLOW.outcomes} outcomes × 2 sides = ${Math.max(1, Math.round(FLOW.makers * Math.sqrt(FLOW.boost))) * Math.max(1, Math.round(FLOW.levels * Math.sqrt(FLOW.boost))) * FLOW.outcomes * 2} resting/shard · takers ${FLOW.takers} (${FLOW.informed} informed) · size ${FLOW.size} · tick ${FLOW.tick} · vol ${FLOW.vol}/s · requote ${FLOW.requoteMs}ms · quoted with the live maker's own arithmetic`
    : '[soak] flow NOISE · the original random generator (SOAK_FLOW=makers for the venue\'s own books)');
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
                  seed: 101 + i, seconds: SECONDS, rate: RATE, restingTarget: RESTING, rateMin: RATE_MIN, rateMax: RATE_MAX,
                  mid: midFor(i), spread: SPREAD, flow: FLOW },
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
    if (r.elapsed % 30 === 0) say(`[soak] ${r.elapsed}s · ${r.rate.toLocaleString()}/s (target ${r.target.toLocaleString()}) · backlog ${r.backlog} · final p50 ${r.p50}ms p90 ${r.p90}ms · resting ${r.resting.toLocaleString()} · forks ${r.forks} · dissents ${r.dissents} · halted ${r.halted} · rss ${r.rssMax}MB${r.spreadCents != null ? ` · book ${r.spreadCents}c wide, ${r.depthAtTouch} at the touch, ${r.fairDevCents}c off fair · ${r.takes.toLocaleString()} takes, ${r.makerFills.toLocaleString()} maker fills` : ''}`);
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
    // THE BOOK. Without these a soak reports how fast the venue went and not what it was: the touch spread, the
    // depth at it and the distance from fair are what say a market was there to trade, and the fills say somebody
    // did. A run with a perfect rate and no fills has measured a sequencer, not an exchange.
    flow: FLOW ? {
      mode: 'makers', grid: rs.length ? rs[0].flow?.grid ?? null : null,
      spreadCentsP50: med(ser, 'spreadCents'), spreadCentsP90: pct(ser.map((r) => Number(r.spreadCents)).filter(Number.isFinite).sort((a, b) => a - b), 0.9) ?? null,
      fairDevCentsP50: med(ser, 'fairDevCents'), depthAtTouchP50: med(ser, 'depthAtTouch'),
      quotesPlaced: rs.reduce((a, r) => a + (r.flow?.placed || 0), 0),
      requotes: rs.reduce((a, r) => a + (r.flow?.requoted || 0), 0),
      cancels: rs.reduce((a, r) => a + (r.flow?.cancelled || 0), 0),
      floorQuotes: rs.reduce((a, r) => a + (r.flow?.floors || 0), 0),
      takes: rs.reduce((a, r) => a + (r.flow?.taken || 0), 0),
      makerFills: rs.reduce((a, r) => a + (r.flow?.fills || 0), 0),
      filledShares: Number(rs.reduce((a, r) => a + (r.flow?.filledSize || 0), 0).toFixed(2)),
      inventoryMax: Math.max(0, ...rs.map((r) => Number(r.flow?.inventoryMax) || 0)),
      gridWalks: rs.reduce((a, r) => a + (r.flow?.gridWalks || 0), 0),
      gridSkips: rs.reduce((a, r) => a + (r.flow?.gridSkips || 0), 0),
    } : { mode: 'noise' },
  };
}
/// the median of a series field — a spread or a depth is not additive, so it is never summed
function med(rows, k) {
  const v = rows.map((r) => Number(r[k])).filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  return v.length ? Number(pct(v, 0.5).toFixed(2)) : null;
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
    if (a.flow?.mode === 'makers') {
      say(`[soak] BOOK touch ${a.flow.spreadCentsP50}c (p90 ${a.flow.spreadCentsP90}c) · depth ${a.flow.depthAtTouchP50} · ${a.flow.fairDevCentsP50}c off fair · ${a.flow.grid} resting/shard by design`);
      say(`[soak] FLOW ${a.flow.quotesPlaced.toLocaleString()} quotes (${a.flow.requotes.toLocaleString()} re-quotes, ${a.flow.floorQuotes.toLocaleString()} floor) · ${a.flow.takes.toLocaleString()} takes · ${a.flow.makerFills.toLocaleString()} maker fills, ${a.flow.filledShares.toLocaleString()} shares · max inventory ${a.flow.inventoryMax} · generator grid passes ${a.flow.gridWalks.toLocaleString()} of ${(a.flow.gridWalks + a.flow.gridSkips).toLocaleString()}`);
    }
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
