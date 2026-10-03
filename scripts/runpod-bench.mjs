// scripts/runpod-bench.mjs — the verification ceiling of real machines: rent pods of several sizes on RunPod, run
// src/miner/bench-verify.mjs on each (the miner image, a different start command), collect /results, terminate.
//   RUNPOD_API_KEY=… node scripts/runpod-bench.mjs [--sizes 8,16,32,64] [--orders 50000] [--flavor cpu5c] [--gpu]
// --gpu rents GPU hosts instead (minVCPUPerGPU = size) for when the CPU fleet is empty. Pods are named bench-* so
// `node scripts/runpod.mjs sweep` kills strays.
import { createPod, waitRunning, podInfo, endpoint, terminatePod, getJson } from './runpod.mjs';
const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const flag = (k) => process.argv.includes(k);
const SIZES = String(arg('--sizes', '8,16,32,64')).split(',').map(Number), ORDERS = Number(arg('--orders', 50000)), FLAVOR = arg('--flavor', 'cpu5c');
const IMAGE = arg('--image', 'ghcr.io/dubthecat/rolla-miner:latest');
const FLAVORS = [FLAVOR, 'cpu3c', 'cpu5c', 'cpu3g', 'cpu5g', 'cpu3m', 'cpu5m'].filter((f, i, a) => a.indexOf(f) === i);
const GPUS = ['NVIDIA GeForce RTX 4090', 'NVIDIA RTX A6000', 'NVIDIA A100 80GB PCIe', 'NVIDIA H100 PCIe', 'NVIDIA L40S', 'NVIDIA GeForce RTX 3090'];
const t0 = Date.now(); const say = (...a) => console.log(`+${((Date.now() - t0) / 1000).toFixed(0)}s`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pods = [];
async function rent(size) {
  const base = { name: `bench-${size}-${Math.floor(Math.random() * 1e5)}`, image: IMAGE, ports: ['8080/http'], diskGb: 10, cmd: ['node', 'src/miner/bench-verify.mjs'], env: { PORT: '8080', BENCH_ORDERS: String(ORDERS) } };
  const tried = []; let last = null;
  if (!flag('--gpu')) for (const cloud of ['SECURE', 'COMMUNITY']) for (const flavor of FLAVORS) { try { const p = await createPod({ ...base, vcpu: size, flavor, cloud }); return { ...p, size, kind: `${cloud} ${flavor} × ${size}` }; } catch (e) { last = e; tried.push(`${cloud} ${flavor}`); } }
  for (const cloud of ['SECURE', 'COMMUNITY']) for (const gpu of GPUS) { try { const p = await createPod({ ...base, vcpu: size, cloud, gpu }); return { ...p, size, kind: `${cloud} ${gpu} ≥${size} vCPU` }; } catch (e) { last = e; tried.push(`${cloud} ${gpu}`); } }
  throw new Error(`no capacity for ${size} vCPU (tried ${tried.length}): ${last?.message?.slice(0, 120)}`);
}
try {
  for (const size of SIZES) { try { const p = await rent(size); pods.push(p); say('pod', p.name, p.id, p.kind, `$${p.costPerHr}/h`); } catch (e) { say('skip', size, 'vCPU:', e.message); } }
  const out = [];
  for (const p of pods) {
    try {
      const info = await waitRunning(p.id, { timeoutMs: 420000 }); p.url = endpoint(info, 8080); say(p.name, 'running', p.url);
      const deadline = Date.now() + 900000; let r = null;
      while (Date.now() < deadline) { try { r = await getJson(p.url + '/results'); if (r && r.done) break; } catch {} await sleep(5000); }
      if (!r || !r.done) { say(p.name, 'no result in time'); continue; }
      say(p.name, r.machine, 'native', JSON.stringify(r.native));
      for (const row of r.ladder) say(`  workers ${String(row.workers).padStart(3)} · ${String(row.ordersPerSec).padStart(7)} orders/s · ${row.perWorker}/s per worker · ${row.usPerOrder} µs/order wall · ok ${row.ok}/${r.orders}`);
      if (r.ed25519) say(`  ed25519 one thread: ${r.ed25519.perCore}/s · ${r.ed25519.usPerVerify} µs · ${r.ed25519.ratioVsSecp}× vs secp256k1`);
      out.push({ size: p.size, kind: p.kind, costPerHr: p.costPerHr, machine: r.machine, best: r.ladder.reduce((a, b) => (b.ordersPerSec > (a?.ordersPerSec || 0) ? b : a), null), ed25519: r.ed25519 });
    } catch (e) { say(p.name, 'failed:', e.message.slice(0, 160)); }
    finally { try { await terminatePod(p.id); say('terminated', p.name); } catch {} }
  }
  say('SUMMARY'); for (const o of out) say(`  ${o.kind} · $${o.costPerHr}/h · best ${o.best?.ordersPerSec} orders/s at ${o.best?.workers} workers (${o.best?.perWorker}/s per worker) · ${o.best ? Math.round(o.best.ordersPerSec / Math.max(0.01, o.costPerHr)) : '?'} orders/s per $/h · 1M/s ≈ ${o.best ? Math.ceil(1e6 / o.best.ordersPerSec) : '?'} such machines per replica`);
} finally { for (const p of pods) { try { await terminatePod(p.id); } catch {} } }
process.exit(0);
