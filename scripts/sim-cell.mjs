// scripts/sim-cell.mjs — a SEQUENCER HOST, or a whole venue cell, in one machine:
//   S shards (one sequencer thread each, scripts/shard-worker.mjs) sequencing into a broker — this machine's own
//   Redpanda (SIM_LOCAL_BROKER=1, the rolla-miner-sim image) or a remote one (L3_KAFKA_BROKERS) — and, with
//   SIM_REPLICAS=R, R miner processes per shard here too (sim-miners.mjs). Every shard signs its own workload,
//   then all shards are released together by POST /go (or SIM_AUTOGO=1 once the local miners are healthy).
//   GET /status · POST /go · GET /results · GET /metrics (local miners) · GET /log
// env: SIM_SHARDS (count) + SIM_BASE, or SIM_SHARD_LIST=a,b,c · SIM_ORDERS per shard · SIM_BATCH · SIM_EPOCH ·
//      SIM_REPLICAS · SIM_THRESHOLD · SIM_WORKERS (verify workers per local miner) · SIM_SEQ_KEY · PORT ·
//      SIM_BROKER_SMP / SIM_BROKER_MEM · PREDICT_BOOK / CHAIN_ID
import { Worker } from 'node:worker_threads';
import { spawn, spawnSync } from 'node:child_process';
import http from 'node:http';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { spawnMiners, minersHealth, minersMetrics } from './sim-miners.mjs';
import { createKafkaLog } from '../src/miner/log.js';

const env = process.env; const here = path.dirname(fileURLToPath(import.meta.url));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = Number(env.SIM_SHARDS || 8), BASE = Number(env.SIM_BASE || (700000 + Math.floor(Math.random() * 90000)));
const SHARDS = env.SIM_SHARD_LIST ? env.SIM_SHARD_LIST.split(',').filter(Boolean) : Array.from({ length: S }, (_, i) => String(BASE + i));
const N = Number(env.SIM_ORDERS || 50000), BATCH = Number(env.SIM_BATCH || 2000), EPOCH = Number(env.SIM_EPOCH || 50);
const R = Number(env.SIM_REPLICAS || 0), THRESHOLD = Number(env.SIM_THRESHOLD || Math.max(1, R - 1)), WORKERS = Number(env.SIM_WORKERS || 1);
const seqKey = env.SIM_SEQ_KEY || generatePrivateKey(); const seqAddr = privateKeyToAccount(seqKey).address;
const BOOK = env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66', CHAIN_ID = Number(env.CHAIN_ID || 46630);
let brokers = env.L3_KAFKA_BROKERS || '127.0.0.1:9092';
const state = { seqAddress: seqAddr, shards: SHARDS, orders: N, batch: BATCH, epoch: EPOCH, replicas: R, threshold: THRESHOLD, signed: 0, running: false, done: false, broker: null, results: {}, startedAt: Date.now(), log: [], error: null };
const say = (...a) => { const line = `+${((Date.now() - state.startedAt) / 1000).toFixed(0)}s ${a.join(' ')}`; console.log(line); state.log.push(line); if (state.log.length > 300) state.log.shift(); };
let localMiners = [], workers = [];

const portOpen = (port) => new Promise((r) => { const s = net.connect(port, '127.0.0.1'); s.once('connect', () => { s.destroy(); r(true); }); s.once('error', () => r(false)); });
async function startBroker() {
  const dir = env.SIM_BROKER_DIR || '/data/redpanda'; fs.mkdirSync(dir, { recursive: true });
  const hasUser = spawnSync('id', ['-u', 'redpanda']).status === 0;
  if (hasUser) spawnSync('chown', ['-R', 'redpanda:redpanda', dir]);
  const smp = Number(env.SIM_BROKER_SMP || 2), mem = env.SIM_BROKER_MEM || '3G';
  const args = ['redpanda', 'start', '--overprovisioned', '--smp', String(smp), '--memory', mem, '--reserve-memory', '0M', '--node-id', '0', '--check=false',
    '--kafka-addr', 'PLAINTEXT://127.0.0.1:9092', '--advertise-kafka-addr', 'PLAINTEXT://127.0.0.1:9092', '--rpc-addr', '127.0.0.1:33145',
    '--set', 'redpanda.kafka_batch_max_bytes=8388608', '--set', `redpanda.data_directory=${dir}`];
  const cmd = hasUser ? ['runuser', '-u', 'redpanda', '--', 'rpk', ...args] : ['rpk', ...args];
  say('[cell] starting broker:', cmd.slice(0, 6).join(' '), '…');
  const p = spawn(cmd[0], cmd.slice(1), { stdio: ['ignore', 'inherit', 'inherit'] }); p.on('exit', (c, sg) => say('[cell] broker exited', c ?? sg));
  for (let i = 0; i < 180; i++) { if (await portOpen(9092)) break; await sleep(1000); }
  if (!(await portOpen(9092))) throw new Error('the broker did not open 9092 in 180 s');
  // the port opens before the broker serves: prove it with a real client (create a topic, append, read back)
  for (let i = 0; i < 60; i++) {
    try {
      const log = await createKafkaLog({ brokers: ['127.0.0.1:9092'], clientId: 'cell-probe', logger: () => {} });
      const topic = 'probe.cell'; await log.append(topic, { i, at: Date.now() }); const n = await log.offset(topic); try { await log.close?.(); } catch {}
      if (n > 0) { state.broker = 'up'; say(`[cell] broker serving on 127.0.0.1:9092 (probe offset ${n})`); return; }
    } catch (e) { if (i % 10 === 0) say('[cell] broker not serving yet:', e.message.slice(0, 80)); }
    await sleep(2000);
  }
  throw new Error('the broker opened 9092 but never served a produce/fetch');
}

async function main() {
  say(`[cell] ${SHARDS.length} shards (${SHARDS[0]}…${SHARDS[SHARDS.length - 1]}) · ${N} orders each · batches of ${BATCH}, epochs of ${EPOCH} · ${R} local replica(s), threshold ${THRESHOLD} · sequencer ${seqAddr}`);
  if (env.SIM_LOCAL_BROKER === '1') { await startBroker(); brokers = '127.0.0.1:9092'; } else say('[cell] broker', brokers);
  // local miners: one process per (shard, replica)
  for (let r = 0; r < R; r++) {
    const keys = SHARDS.map(() => generatePrivateKey());
    const ms = spawnMiners({ shards: SHARDS, keys, brokers, sequencers: seqAddr, threshold: THRESHOLD, workers: WORKERS, basePort: 9100 + r * 100, dataDir: path.join(env.DATA_DIR || '/data', `r${r + 1}`), onLog: (l) => say(l) });
    localMiners.push(...ms.map((m) => ({ ...m, replica: r + 1 })));
  }
  if (localMiners.length) {
    say(`[cell] ${localMiners.length} miner processes · ${WORKERS} verify worker(s) each`);
    const deadline = Date.now() + 300000; let ok = false;
    let lastH = null;
    while (Date.now() < deadline) { const h = await minersHealth(localMiners, { timing: false }); lastH = h; if (h.length && h.every((m) => m.ok)) { ok = true; break; } await sleep(4000); }
    if (!ok) { const bad = (lastH || []).filter((m) => !m.ok); throw new Error(`local miners never all healthy: ${bad.length} unhealthy — ${bad.slice(0, 4).map((m) => `${m.shard}: ${m.stalled || m.error || 'not ok'}`).join('; ')}`); }
    say('[cell] all local miners healthy');
  }
  // the shards: a thread each; they sign, then wait for go
  workers = SHARDS.map((shard, i) => new Worker(path.join(here, 'shard-worker.mjs'), { workerData: { shard, brokers, orders: N, batch: BATCH, epoch: EPOCH, threshold: THRESHOLD, seqKey, book: BOOK, chainId: CHAIN_ID, seed: 11 + i } }));
  for (const w of workers) {
    w.on('message', (m) => {
      if (m.t === 'signed') { state.signed++; say(`[cell] shard ${m.shard} signed ${m.ops} ops in ${m.ms} ms (${state.signed}/${SHARDS.length})`); if (env.SIM_AUTOGO === '1' && state.signed === SHARDS.length) go(); }
      if (m.t === 'result') {
        state.results[m.shard] = m;
        say(`[cell] shard ${m.shard}: ${m.ops} ops · ${m.batches} batches · sequenced ${m.seqMs} ms (${Math.round(m.ops / m.seqMs * 1000)}/s) · final ${m.finalIndex}/${m.last} after ${m.clusterMs} ms (${Math.round(m.ops / m.clusterMs * 1000)}/s) · finality p50 ${m.p50} p90 ${m.p90} max ${m.max} · seal ${m.perBatchMs} ms (roots ${m.rootMsPerBatch}) · forks ${m.forks}${m.halted ? ' HALTED' : ''}`);
        if (Object.keys(state.results).length === SHARDS.length) finish();
      }
    });
    w.on('error', (e) => { say('[cell] shard thread error', e.message); state.error = e.message; });
  }
}
function go() {
  if (state.running || state.signed < SHARDS.length) return false;
  state.running = true; state.goAt = Date.now(); say('[cell] GO — all shards released together');
  for (const w of workers) w.postMessage('go');
  return true;
}
function aggregate() {
  const rs = Object.values(state.results); if (!rs.length) return null;
  const totalOps = rs.reduce((a, r) => a + r.ops, 0), wallSeqMs = Math.max(...rs.map((r) => r.seqMs)), wallFinalMs = Math.max(...rs.map((r) => r.clusterMs));
  const allFinal = rs.every((r) => r.finalIndex >= r.last && !r.halted && r.forks === 0);
  const sorted = (k) => rs.map((r) => r[k]).filter((x) => x != null).sort((a, b) => a - b);
  return { shards: rs.length, totalOps, batches: rs.reduce((a, r) => a + r.batches, 0), wallSeqMs, wallFinalMs, seqRate: Math.round(totalOps / wallSeqMs * 1000), finalRate: Math.round(totalOps / wallFinalMs * 1000), allFinal, forks: rs.reduce((a, r) => a + r.forks, 0), p50Median: sorted('p50')[Math.floor(sorted('p50').length / 2)] ?? null, p90Max: Math.max(...sorted('p90'), 0) };
}
async function finish() {
  state.done = true; state.aggregate = aggregate(); state.miners = localMiners.length ? await minersHealth(localMiners) : null;
  const a = state.aggregate;
  say(`[cell] DONE ${a.shards} shards · ${a.totalOps} ops · sequenced ${a.seqRate} orders/s · finalized ${a.finalRate} orders/s (wall ${a.wallFinalMs} ms) · all final ${a.allFinal} · forks ${a.forks} · finality p50(median shard) ${a.p50Median} ms · p90(max) ${a.p90Max} ms`);
}
http.createServer(async (req, res) => {
  const j = (code, o) => { res.statusCode = code; res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(o)); };
  try {
    if (req.url === '/go' && req.method === 'POST') return j(200, { started: go(), signed: state.signed, running: state.running });
    if (req.url === '/results') return j(200, { done: state.done, aggregate: state.done ? state.aggregate : aggregate(), results: state.results, miners: state.miners, error: state.error, broker: state.broker, seqAddress: seqAddr, shards: SHARDS });
    if (req.url === '/metrics') { res.setHeader('content-type', 'text/plain; version=0.0.4'); return res.end(await minersMetrics(localMiners)); }
    if (req.url === '/log') { res.setHeader('content-type', 'text/plain'); return res.end(state.log.join('\n')); }
    return j(200, { ok: !state.error, seqAddress: seqAddr, shards: SHARDS, signed: state.signed, running: state.running, done: state.done, broker: state.broker, miners: localMiners.length ? await minersHealth(localMiners) : null, error: state.error, uptime: (Date.now() - state.startedAt) / 1000 });
  } catch (e) { j(500, { error: e.message }); }
}).listen(Number(env.PORT || 8080), () => say(`[cell] serving on ${env.PORT || 8080}`));
main().catch((e) => { state.error = e.message; say('[cell] FAILED', e.stack || e.message); });
