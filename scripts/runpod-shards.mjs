// scripts/runpod-shards.mjs — S shards at once: S sequencers here (one worker thread each, against the Fly
// broker), S × R miner pods on RunPod (a miner per shard per replica), one signed workload per shard, all shards
// released together. The venue's rate is the sum of its shards; this measures how linearly it adds up.
//   RUNPOD_API_KEY=… node scripts/runpod-shards.mjs [--shards 4] [--replicas 3] [--orders 50000] [--vcpu 8] [--batch 2000] [--epoch 10] [--keep]
import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createPod, waitRunning, endpoint, terminatePod, getJson } from './runpod.mjs';

const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const flag = (k) => process.argv.includes(k);
const S = Number(arg('--shards', 4)), R = Number(arg('--replicas', 3)), N = Number(arg('--orders', 50000)), VCPU = Number(arg('--vcpu', 8)), FLAVOR = arg('--flavor', 'cpu3c');
const BATCH = Number(arg('--batch', 2000)), EPOCH = Number(arg('--epoch', 10));
const IMAGE = arg('--image', 'ghcr.io/dubthecat/rolla-miner:latest');
const BROKERS = arg('--brokers', process.env.L3_KAFKA_BROKERS || 'rolla-l3-broker.fly.dev:9092');
const BOOK = process.env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66', CHAIN_ID = Number(process.env.CHAIN_ID || 46630);
const THRESHOLD = Math.max(1, R - 1);
const base = 800000 + Math.floor(Math.random() * 90000); const SHARDS = Array.from({ length: S }, (_, i) => String(base + i));
const t0 = Date.now(); const say = (...a) => console.log(`+${((Date.now() - t0) / 1000).toFixed(0)}s`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FLAVORS = ['cpu3c', 'cpu5c', 'cpu3g', 'cpu5g', 'cpu3m', 'cpu5m'];
const GPUS = ['NVIDIA GeForce RTX 3090', 'NVIDIA RTX A4500', 'NVIDIA RTX A5000', 'NVIDIA GeForce RTX 4090', 'NVIDIA GeForce RTX 3070'];
async function createPodOnLadder(opts) {
  let lastErr = null; const tried = [];
  for (let round = 0; round < 2; round++) {
    for (const cloud of ['SECURE', 'COMMUNITY']) {
      for (const flavor of [opts.flavor, ...FLAVORS.filter((f) => f !== opts.flavor)]) { try { const p = await createPod({ ...opts, flavor, cloud }); return { ...p, vcpu: opts.vcpu, flavor, cloud }; } catch (e) { lastErr = e; tried.push(`${cloud} ${flavor}`); } }
      for (const gpu of GPUS) { try { const p = await createPod({ ...opts, cloud, gpu }); return { ...p, vcpu: `gpu≥${opts.vcpu}`, flavor: gpu, cloud }; } catch (e) { lastErr = e; tried.push(`${cloud} ${gpu}`); } }
    }
    say(`no instance on ${tried.length} rungs (${lastErr.message.slice(0, 80)}) — second round in 30 s`); await sleep(30000);
  }
  throw lastErr;
}
const pods = [];
async function teardown(why) {
  if (flag('--keep')) { say('keeping pods (--keep):', pods.map((p) => p.id).join(' ')); return; }
  for (const p of pods) { try { await terminatePod(p.id); } catch (e) { say('terminate failed', p.id, e.message); } }
  say(`terminated ${pods.length} pods${why ? ' — ' + why : ''}`);
}
process.on('SIGINT', async () => { await teardown('interrupted'); process.exit(130); });

try {
  say(`shards: ${S} × ${R} replicas = ${S * R} miner pods × ${VCPU} vCPU · ${N} orders per shard (${S * N} total) · batches of ${BATCH}, epochs of ${EPOCH} · threshold ${THRESHOLD} · broker ${BROKERS}`);
  const seqKey = generatePrivateKey(); const seqAddr = privateKeyToAccount(seqKey).address;
  // ---- the miners: one pod per (shard, replica) ----
  const miners = [];
  for (const shard of SHARDS) for (let r = 0; r < R; r++) {
    const key = generatePrivateKey();
    const p = await createPodOnLadder({ name: `miner-${shard}-${r + 1}`, image: IMAGE, ports: ['8080/http'], vcpu: VCPU, flavor: FLAVOR, diskGb: 10,
      env: { L3_LOG: 'kafka', L3_KAFKA_BROKERS: BROKERS, L3_SHARD: shard, L3_MINER_KEY: key, PREDICT_BOOK: BOOK, CHAIN_ID: String(CHAIN_ID), L3_SEQUENCERS: seqAddr, L3_THRESHOLD: String(THRESHOLD), L3_VERIFY_WORKERS: String(Math.max(1, VCPU - 1)), PORT: '8080', DATA_DIR: '/data' } });
    pods.push(p); miners.push({ ...p, shard, replica: r + 1 }); say('pod', p.name, p.id, `${p.cloud} ${p.flavor} × ${p.vcpu}`, `$${p.costPerHr}/h`);
  }
  const cost = pods.reduce((a, p) => a + (p.costPerHr || 0), 0); say(`fleet $${cost.toFixed(2)}/h`);
  for (const m of miners) {
    const info = await waitRunning(m.id, { timeoutMs: 420000 }); m.url = endpoint(info, 8080);
    const deadline = Date.now() + 240000; let ok = false;
    while (Date.now() < deadline) { try { const h = await getJson(m.url + '/healthz'); if (h && h.miner) { ok = true; break; } } catch {} await sleep(4000); }
    say(m.name, ok ? 'healthy' : 'NOT HEALTHY', m.url);
  }
  // ---- the shards: a worker thread each; all sign, then all go at once ----
  const dir = path.dirname(fileURLToPath(import.meta.url));
  const workers = SHARDS.map((shard, i) => new Worker(path.join(dir, 'shard-worker.mjs'), { workerData: { shard, brokers: BROKERS, orders: N, batch: BATCH, epoch: EPOCH, threshold: THRESHOLD, seqKey, book: BOOK, chainId: CHAIN_ID, seed: 11 + i } }));
  const results = new Map(); let signed = 0;
  const done = new Promise((resolve, reject) => {
    for (const w of workers) {
      w.on('message', (m) => {
        if (m.t === 'signed') { signed++; say(`shard ${m.shard} signed ${m.ops} ops in ${m.ms} ms`); if (signed === workers.length) { say('GO — all shards released together'); for (const x of workers) x.postMessage('go'); } }
        if (m.t === 'result') { results.set(m.shard, m); say(`shard ${m.shard}: ${m.ops} ops · ${m.batches} batches · sequenced ${m.seqMs} ms (${Math.round(m.ops / m.seqMs * 1000)}/s) · final at ${m.finalIndex}/${m.last} after ${m.clusterMs} ms (${Math.round(m.ops / m.clusterMs * 1000)}/s) · finality p50 ${m.p50} p90 ${m.p90} max ${m.max} ms · seal ${m.perBatchMs} ms (roots ${m.rootMsPerBatch}) · forks ${m.forks}${m.halted ? ' HALTED' : ''}${m.failed ? ` failed ${m.failed}` : ''}`); if (results.size === workers.length) resolve(); }
      });
      w.on('error', (e) => { say('worker error', e.message); reject(e); });
    }
  });
  await done;
  const rs = [...results.values()]; const totalOps = rs.reduce((a, r) => a + r.ops, 0);
  const wallSeq = Math.max(...rs.map((r) => r.seqMs)), wallFinal = Math.max(...rs.map((r) => r.clusterMs));
  const allFinal = rs.every((r) => r.finalIndex >= r.last && !r.halted && r.forks === 0);
  say(`AGGREGATE ${S} shards: ${totalOps} ops · sequenced ${Math.round(totalOps / wallSeq * 1000)} orders/s · finalized ${Math.round(totalOps / wallFinal * 1000)} orders/s across the venue (wall ${wallFinal} ms) · all final ${allFinal} · fleet $${cost.toFixed(2)}/h`);
  // the miners' own counters
  const prom = (txt) => { const o = {}; for (const line of String(txt).split('\n')) { const mm = line.match(/^rolla_l3_([a-z_]+)\{[^}]*\}\s+([-0-9.eE+]+)/); if (mm) o[mm[1]] = Number(mm[2]); } return o; };
  for (const m of miners) { try { const h = await getJson(m.url + '/healthz'); const x = prom((await getJson(m.url + '/metrics')).raw || ''); say(m.name, JSON.stringify({ index: h.index, stalled: h.stalled, lag: h.lagSeconds, votes: x.votes_total, dissents: x.dissents_total, badSigs: x.bad_signatures_total, orders: x.orders_total, verifyMs: x.verify_ms_per_order, workers: x.verify_workers })); } catch (e) { say(m.name, 'metrics failed', e.message.slice(0, 60)); } }
  say(`RESULT ${allFinal ? 'ALL SHARDS FINAL' : 'INCOMPLETE'} · ${rs.reduce((a, r) => a + r.batches, 0)} batches · ${rs.reduce((a, r) => a + r.forks, 0)} forks`);
} catch (e) { say('FAILED', e.stack || e.message); process.exitCode = 1; }
finally { await teardown(); }
process.exit(process.exitCode || 0);
