// scripts/runpod-sim.mjs — the testnet simulation on RunPod, two shapes:
//   --mode cross  (default)  one SEQUENCER HOST pod (S shard threads) + R MINER HOST pods (one miner process per shard
//                            each) against the Fly broker — the real cross-machine venue, 8 shards × 32 vCPU
//   --mode cells             C self-contained cells (sim image: own Redpanda + S shards + S×R miners in one machine),
//                            run at once and summed — the way to push the aggregate without a bigger broker
//   RUNPOD_API_KEY=… node scripts/runpod-sim.mjs [--shards 8] [--replicas 3] [--orders 50000] [--batch 2000] [--epoch 50]
//      [--seq-vcpu 32] [--miner-vcpu 32] [--workers 3] [--brokers host:9092] | [--mode cells --cells 4 --cell-vcpu 64 --workers 1]
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createPod, waitRunning, endpoint, terminatePod, getJson } from './runpod.mjs';
const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const flag = (k) => process.argv.includes(k);
const MODE = arg('--mode', 'cross'), S = Number(arg('--shards', 8)), R = Number(arg('--replicas', 3)), N = Number(arg('--orders', 50000));
const BATCH = Number(arg('--batch', 2000)), EPOCH = Number(arg('--epoch', 50)), WORKERS = Number(arg('--workers', MODE === 'cells' ? 1 : 3));
const SEQ_VCPU = Number(arg('--seq-vcpu', 32)), MINER_VCPU = Number(arg('--miner-vcpu', 32)), CELLS = Number(arg('--cells', 1)), CELL_VCPU = Number(arg('--cell-vcpu', 64));
const IMAGE = arg('--image', 'ghcr.io/dubthecat/rolla-miner:latest'), SIM_IMAGE = arg('--sim-image', 'ghcr.io/dubthecat/rolla-miner-sim:latest');
const BROKERS = arg('--brokers', process.env.L3_KAFKA_BROKERS || 'rolla-l3-broker.fly.dev:9092');
const BOOK = process.env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66', CHAIN_ID = String(process.env.CHAIN_ID || 46630);
const THRESHOLD = Math.max(1, R - 1);
const t0 = Date.now(); const say = (...a) => console.log(`+${((Date.now() - t0) / 1000).toFixed(0)}s`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FLAVORS = ['cpu3c', 'cpu5c', 'cpu3g', 'cpu5g', 'cpu3m', 'cpu5m'];
const GPUS = ['NVIDIA GeForce RTX 4090', 'NVIDIA RTX A6000', 'NVIDIA L40S', 'NVIDIA A100 80GB PCIe', 'NVIDIA H100 PCIe', 'NVIDIA GeForce RTX 3090', 'NVIDIA RTX A5000'];
async function rent(opts) {
  let last = null; const tried = [];
  for (let round = 0; round < (opts.rounds ?? 2); round++) {
    for (const cloud of ['SECURE', 'COMMUNITY']) {
      for (const flavor of FLAVORS) { try { const p = await createPod({ ...opts, flavor, cloud }); return { ...p, kind: `${cloud} ${flavor} × ${opts.vcpu}` }; } catch (e) { last = e; tried.push(`${cloud} ${flavor}`); } }
      for (const gpu of GPUS) { try { const p = await createPod({ ...opts, cloud, gpu }); return { ...p, kind: `${cloud} ${gpu} ≥${opts.vcpu} vCPU` }; } catch (e) { last = e; tried.push(`${cloud} ${gpu}`); } }
    }
    say(`no instance on ${tried.length} rungs for ${opts.name} (${last?.message?.slice(0, 80)}) — second round in 30 s`); await sleep(30000);
  }
  throw last;
}
const pods = [];
async function teardown() { if (flag('--keep')) { say('keeping pods (--keep):', pods.map((p) => p.id).join(' ')); return; } for (const p of pods) { try { await terminatePod(p.id); } catch (e) { say('terminate failed', p.id, e.message); } } say(`terminated ${pods.length} pods`); }
process.on('SIGINT', async () => { await teardown(); process.exit(130); });
const post = async (url) => { const r = await fetch(url, { method: 'POST' }); return r.json(); };
async function up(p, timeoutMs = 420000) { const info = await waitRunning(p.id, { timeoutMs }); p.url = endpoint(info, 8080); return p; }
async function waitFor(p, pred, label, timeoutMs = 600000) {
  const deadline = Date.now() + timeoutMs; let last = null;
  while (Date.now() < deadline) { try { const s = await getJson(p.url + '/'); last = s; if (pred(s)) return s; if (s.error) throw new Error(`${p.name}: ${s.error}`); } catch (e) { if (/: /.test(e.message) && !/fetch|abort|JSON|ECONN/i.test(e.message)) throw e; } await sleep(5000); }
  throw new Error(`${p.name} not ${label} in time: ${JSON.stringify(last).slice(0, 200)}`);
}
function printCell(name, r) {
  for (const m of Object.values(r.results || {})) say(`  ${name} shard ${m.shard}: ${m.ops} ops · ${m.batches} batches · sequenced ${Math.round(m.ops / m.seqMs * 1000)}/s · finalized ${Math.round(m.ops / m.clusterMs * 1000)}/s · finality p50 ${m.p50} p90 ${m.p90} max ${m.max} ms · seal ${m.perBatchMs} ms (roots ${m.rootMsPerBatch}) · forks ${m.forks}${m.halted ? ' HALTED' : ''}`);
  const a = r.aggregate; if (a) say(`  ${name} AGGREGATE ${a.shards} shards · ${a.totalOps} ops · sequenced ${a.seqRate} orders/s · finalized ${a.finalRate} orders/s (wall ${a.wallFinalMs} ms) · all final ${a.allFinal} · forks ${a.forks} · finality p50 ${a.p50Median} ms (median shard) · p90 ${a.p90Max} ms (max)`);
  if (r.miners) { const bad = r.miners.filter((m) => !m.ok); say(`  ${name} miners ${r.miners.length} · ${bad.length} unhealthy${bad.length ? ': ' + bad.map((m) => `${m.shard}:${m.stalled || m.error || 'lag'}`).join(' ') : ''}`); for (const m of r.miners.slice(0, 8)) say(`    shard ${m.shard} index ${m.index} · verify ${m.verifyMsPerOrder} ms/order (${m.workers} w) · roots ${m.rootMsPerBatch} ms/batch · replay ${m.applyMsPerBatch} ms/batch · resting ${m.resting}`); }
}
try {
  if (MODE === 'cross') {
    const base = 600000 + Math.floor(Math.random() * 90000); const SHARDS = Array.from({ length: S }, (_, i) => String(base + i));
    const seqKey = generatePrivateKey(), seqAddr = privateKeyToAccount(seqKey).address;
    say(`cross: ${S} shards · sequencer host ${SEQ_VCPU} vCPU · ${R} miner hosts × ${MINER_VCPU} vCPU (${S} miner processes × ${WORKERS} workers each) · ${N} orders/shard (${S * N}) · batches ${BATCH} · epochs ${EPOCH} · threshold ${THRESHOLD} · broker ${BROKERS}`);
    const seqPod = await rent({ name: `sim-seq-${base}`, image: IMAGE, ports: ['8080/http'], vcpu: SEQ_VCPU, diskGb: 20, cmd: ['node', 'scripts/sim-cell.mjs'],
      env: { SIM_SHARD_LIST: SHARDS.join(','), SIM_ORDERS: String(N), SIM_BATCH: String(BATCH), SIM_EPOCH: String(EPOCH), SIM_REPLICAS: '0', SIM_THRESHOLD: String(THRESHOLD), SIM_SEQ_KEY: seqKey, SIM_LOCAL_BROKER: '0', L3_KAFKA_BROKERS: BROKERS, PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID } });
    pods.push(seqPod); say('sequencer host', seqPod.id, seqPod.kind, `$${seqPod.costPerHr}/h`);
    const hosts = [];
    for (let r = 0; r < R; r++) {
      const keys = SHARDS.map(() => generatePrivateKey());
      const p = await rent({ name: `sim-miners-${base}-${r + 1}`, image: IMAGE, ports: ['8080/http'], vcpu: MINER_VCPU, diskGb: 40, cmd: ['node', 'scripts/sim-miners.mjs'],
        env: { SIM_SHARDS: SHARDS.join(','), SIM_KEYS: keys.join(','), L3_KAFKA_BROKERS: BROKERS, L3_SEQUENCERS: seqAddr, L3_THRESHOLD: String(THRESHOLD), SIM_WORKERS: String(WORKERS), PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID } });
      pods.push(p); hosts.push(p); say('miner host', r + 1, p.id, p.kind, `$${p.costPerHr}/h`);
    }
    say(`fleet $${pods.reduce((a, p) => a + (p.costPerHr || 0), 0).toFixed(2)}/h`);
    await up(seqPod); say('sequencer host running', seqPod.url);
    for (const h of hosts) { await up(h); say(h.name, 'running', h.url); }
    for (const h of hosts) { await waitFor(h, (s) => s.ok && s.count === S, 'all miners healthy', 420000); say(h.name, `${S} miners healthy`); }
    await waitFor(seqPod, (s) => s.signed === S, 'signed', 600000); say('all shards signed');
    const g = await post(seqPod.url + '/go'); say('go', JSON.stringify(g));
    const deadline = Date.now() + 900000; let r = null;
    while (Date.now() < deadline) { try { r = await getJson(seqPod.url + '/results'); if (r.done || r.error) break; } catch {} await sleep(5000); }
    if (!r) throw new Error('no results'); if (r.error) say('sequencer host error:', r.error);
    printCell('venue', r);
    for (const h of hosts) { try { const s = await getJson(h.url + '/healthz'); say(`${h.name}: ${s.miners.filter((m) => m.ok).length}/${s.count} ok`); for (const m of s.miners) say(`    shard ${m.shard} index ${m.index}${m.stalled ? ' STALLED ' + m.stalled : ''} · verify ${m.verifyMsPerOrder} ms/order (${m.workers} workers) · roots ${m.rootMsPerBatch} ms/batch · replay ${m.applyMsPerBatch} ms/batch · resting ${m.resting} · votes pending ${m.votesPending}`); } catch (e) { say(h.name, 'health failed', e.message.slice(0, 60)); } }
    say(`RESULT ${r.done && r.aggregate?.allFinal ? 'ALL SHARDS FINAL' : 'INCOMPLETE'} · sequenced ${r.aggregate?.seqRate} orders/s · finalized ${r.aggregate?.finalRate} orders/s`);
  } else {
    say(`cells: ${CELLS} × (${S} shards × ${R} replicas, ${S * R} miner processes × ${WORKERS} workers, own broker) on ${CELL_VCPU} vCPU · ${N} orders/shard · batches ${BATCH} · epochs ${EPOCH}`);
    // capacity is what it is: rent as many cells as RunPod has machines for (the requested size, then the fallback
    // size), and run with those rather than throwing away the ones already rented
    const cells = []; const FALLBACK_VCPU = Number(arg('--cell-vcpu-fallback', 32));
    for (let c = 0; c < CELLS; c++) {
      const base = 500000 + c * 1000 + Math.floor(Math.random() * 900);
      const env = { SIM_SHARDS: String(S), SIM_BASE: String(base), SIM_ORDERS: String(N), SIM_BATCH: String(BATCH), SIM_EPOCH: String(EPOCH), SIM_REPLICAS: String(R), SIM_THRESHOLD: String(THRESHOLD), SIM_WORKERS: String(WORKERS), SIM_AUTOGO: '1', SIM_LOCAL_BROKER: '1', SIM_BROKER_SMP: arg('--broker-smp', '2'), SIM_BROKER_MEM: arg('--broker-mem', '3G'), PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID };
      let p = null;
      for (const vcpu of [CELL_VCPU, FALLBACK_VCPU].filter((v, i, a) => v > 0 && a.indexOf(v) === i)) {
        try { p = await rent({ rounds: 1, name: `sim-cell-${base}`, image: SIM_IMAGE, ports: ['8080/http'], vcpu, diskGb: 80, cmd: ['node', 'scripts/sim-cell.mjs'], env: { ...env, SIM_WORKERS: vcpu < CELL_VCPU ? '1' : String(WORKERS) } }); p.vcpu = vcpu; break; }
        catch (e) { say(`cell ${c + 1}: no ${vcpu}-vCPU machine (${e.message.slice(0, 60)})`); }
      }
      if (!p) { say(`cell ${c + 1}: no capacity — running with the ${cells.length} rented`); break; }
      pods.push(p); cells.push(p); say('cell', c + 1, p.id, p.kind, `$${p.costPerHr}/h`);
    }
    if (!cells.length) throw new Error('no capacity for a single cell');
    say(`fleet $${pods.reduce((a, p) => a + (p.costPerHr || 0), 0).toFixed(2)}/h`);
    for (const c of cells) { try { await up(c); say(c.name, 'running', c.url); } catch (e) { say(c.name, 'did not start:', e.message.slice(0, 80)); c.dead = true; } }
    const results = new Map(); const deadline = Date.now() + 1500000;
    while (Date.now() < deadline && results.size < cells.filter((c) => !c.dead).length) {
      for (const c of cells) { if (c.dead || results.has(c.id)) continue; try { const r = await getJson(c.url + '/results'); if (r.done || r.error) { results.set(c.id, r); say(c.name, r.error ? `ERROR ${r.error}` : 'done'); printCell(c.name, r); } } catch {} }
      await sleep(5000);
    }
    const rs = [...results.values()].filter((r) => r.aggregate);
    const totalOps = rs.reduce((a, r) => a + r.aggregate.totalOps, 0), sumSeq = rs.reduce((a, r) => a + r.aggregate.seqRate, 0), sumFinal = rs.reduce((a, r) => a + r.aggregate.finalRate, 0);
    say(`RESULT ${rs.length}/${cells.length} cells · ${rs.reduce((a, r) => a + r.aggregate.shards, 0)} shards · ${totalOps} ops · sequenced ${sumSeq} orders/s · finalized ${sumFinal} orders/s summed across cells (each cell's own wall clock) · all final ${rs.every((r) => r.aggregate.allFinal)} · forks ${rs.reduce((a, r) => a + r.aggregate.forks, 0)}`);
  }
} catch (e) { say('FAILED', e.stack || e.message); process.exitCode = 1; }
finally { await teardown(); }
process.exit(process.exitCode || 0);
